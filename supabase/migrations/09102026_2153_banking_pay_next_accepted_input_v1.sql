-- F-25: additive immutable evidence for the existing PREPARE agency cutoff.
-- No backfill, fixture, second clock, financial ledger, current-pointer rewrite,
-- browser grant, owner transition or external action. Existing money owners
-- retain all realised/held/funded/paid counters and conservation authority.

\set ON_ERROR_STOP on

begin;

-- Both identities are needed to reject a receipt for a different run/command.
alter table private.bpay_next_run_command
  add constraint bpay_next_run_command_run_command_key unique(run_id,command_id);
alter table private.bpay_next_command
  add constraint bpay_next_command_sequence_epoch_key unique(id,agency_sequence,module_epoch);

create table private.bpay_next_accepted_input (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_id uuid not null,
  prepare_command_id uuid not null,
  candidate_id uuid not null,
  module_epoch bigint not null check(module_epoch>0),
  accepted_sequence bigint not null check(accepted_sequence>0),
  target_pay_channel text not null check(target_pay_channel in ('PAYE','UMBRELLA')),
  target_umbrella_id uuid references public.umbrellas(id) on delete restrict,
  umbrella_vat_chargeable boolean,
  umbrella_enabled boolean,
  candidate_floor_raw numeric(12,2),
  floor_state text not null check(floor_state in ('BOUND','UNBOUND')),
  candidate_floor private.bpay_next_penny_amount check(candidate_floor>=0),
  pay_date date not null check(pg_catalog.isfinite(pay_date)),
  pay_week_start date not null check(pg_catalog.isfinite(pay_week_start)
    and extract(isodow from pay_week_start)=1),
  valuation_policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
  case_scope text not null check(case_scope in ('NO_CASE','SEALED_SELECTION')),
  case_selection_revision bigint,
  candidate_version_event_id uuid,
  umbrella_version_event_id uuid,
  policy_version_event_id uuid,
  settings_witness_kind text not null check(settings_witness_kind in ('BASELINE_AT_ACCEPTANCE','ORDERED_VERSION')),
  evidence_sha256 bytea not null check(pg_catalog.octet_length(evidence_sha256)=32),
  captured_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  unique(prepare_command_id,candidate_id),
  unique(run_id,candidate_id),
  unique(id,run_id,candidate_id),
  unique(id,candidate_id),
  unique(id,run_id,candidate_id,case_selection_revision),
  unique(id,candidate_id,pay_week_start),
  foreign key(run_id,prepare_command_id)
    references private.bpay_next_run_command(run_id,command_id) on delete restrict,
  foreign key(prepare_command_id,accepted_sequence,module_epoch)
    references private.bpay_next_command(id,agency_sequence,module_epoch) on delete restrict,
  foreign key(prepare_command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  foreign key(run_id,candidate_id)
    references private.bpay_next_selection_candidate(run_id,candidate_id) on delete restrict,
  foreign key(run_id,candidate_id,case_selection_revision)
    references private.bpay_next_case_selection(run_id,candidate_id,selection_revision) on delete restrict,
  check(pay_date>=pay_week_start and pay_date<pay_week_start+7),
  check((floor_state='BOUND' and candidate_floor is not null)
    or (floor_state='UNBOUND' and candidate_floor is null)),
  check((case_scope='NO_CASE' and case_selection_revision is null)
    or (case_scope='SEALED_SELECTION' and case_selection_revision is not null and case_selection_revision>0)),
  check((target_pay_channel='PAYE' and target_umbrella_id is null
      and umbrella_vat_chargeable is null and umbrella_enabled is null and umbrella_version_event_id is null)
    or (target_pay_channel='UMBRELLA' and target_umbrella_id is not null
      and umbrella_vat_chargeable is not null and umbrella_enabled is true)),
  check(settings_witness_kind<>'ORDERED_VERSION' or candidate_version_event_id is not null)
);
create index bpay_next_accepted_input_policy_idx
  on private.bpay_next_accepted_input(valuation_policy_id,id);
create index bpay_next_accepted_input_umbrella_idx
  on private.bpay_next_accepted_input(target_umbrella_id,id) where target_umbrella_id is not null;

-- One bounded factual query/origin receipt per selected WORK. The canonical
-- Source reader owns PAY_QUERY_STATE_V3; this is retained evidence, not another
-- implementation of its CLEAR calculation. Ordinary WORK has no Source pin.
create table private.bpay_next_accepted_work_input (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  accepted_input_id uuid not null,
  run_id uuid not null,
  candidate_id uuid not null,
  work_id uuid not null,
  original_revision_id uuid not null,
  effective_revision_id uuid not null,
  publication_command_id uuid not null,
  predecessor_sequence bigint not null check(predecessor_sequence>0),
  selection_mode text not null check(selection_mode in ('ALL','SUBSET')),
  selected_line_count bigint not null check(selected_line_count>=0),
  physical_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  physical_timesheet_version integer not null check(physical_timesheet_version>0),
  raw_booking_id text not null check(pg_catalog.char_length(raw_booking_id) between 1 and 200),
  contract_id uuid not null references public.contracts(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  week_ending_date date not null check(pg_catalog.isfinite(week_ending_date)),
  source_kind text not null check(source_kind in ('ORDINARY','SOURCE','PROTECTED','EXPENSE','ADJUSTMENT')),
  source_event_id uuid,
  source_head_id uuid,
  financial_snapshot_id uuid references public.timesheets_financials(id) on delete restrict,
  valuation_policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
  query_disposition text check(query_disposition='CLEAR'),
  query_scope jsonb,
  query_state_sha256 bytea check(pg_catalog.octet_length(query_state_sha256)=32),
  query_facts jsonb,
  origin_proof jsonb not null check(pg_catalog.jsonb_typeof(origin_proof)='object'
    and pg_catalog.octet_length(origin_proof::text)<=65536),
  evidence_sha256 bytea not null check(pg_catalog.octet_length(evidence_sha256)=32),
  unique(accepted_input_id,work_id),
  unique(id,accepted_input_id,work_id,effective_revision_id),
  unique(id,run_id,work_id,original_revision_id,effective_revision_id),
  unique(id,accepted_input_id,work_id,effective_revision_id,publication_command_id,predecessor_sequence,selected_line_count),
  foreign key(accepted_input_id,run_id,candidate_id)
    references private.bpay_next_accepted_input(id,run_id,candidate_id) on delete restrict,
  foreign key(run_id,work_id,original_revision_id)
    references private.bpay_next_work_choice(run_id,work_id,expected_revision_id) on delete restrict,
  foreign key(work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key(work_id,effective_revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict,
  foreign key(publication_command_id,predecessor_sequence)
    references private.bpay_next_command(id,agency_sequence) on delete restrict,
  foreign key(publication_command_id,work_id,effective_revision_id,candidate_id)
    references private.bpay_next_publication(command_id,work_id,revision_id,candidate_id) on delete restrict,
  check((source_kind not in ('SOURCE','PROTECTED') and query_disposition is null
      and query_scope is null and query_state_sha256 is null and query_facts is null)
    or (source_kind in ('SOURCE','PROTECTED') and source_event_id is not null
      and query_disposition is not null and query_disposition='CLEAR' and query_state_sha256 is not null
      and query_scope is not null and pg_catalog.jsonb_typeof(query_scope)='object'
      and pg_catalog.octet_length(query_scope::text)<=8192
      and query_facts is not null and pg_catalog.jsonb_typeof(query_facts)='object'
      and pg_catalog.octet_length(query_facts::text)<=262144))
);
create index bpay_next_accepted_work_revision_idx
  on private.bpay_next_accepted_work_input(effective_revision_id,id);
create index bpay_next_accepted_work_publication_idx
  on private.bpay_next_accepted_work_input(publication_command_id,id);
create index bpay_next_accepted_work_policy_idx
  on private.bpay_next_accepted_work_input(valuation_policy_id,id);

-- Exact lineage mapping, not a date-based replacement of an obsolete UUID.
-- WB-36 can add its genuine segment/event identities once those owners exist.
-- ALL retains every certified effective line; SUBSET also retains its original
-- approved line and immutable selected-component FK.
create table private.bpay_next_accepted_work_member (
  accepted_work_input_id uuid not null references private.bpay_next_accepted_work_input(id) on delete restrict,
  run_id uuid not null,
  work_id uuid not null,
  original_revision_id uuid not null,
  original_approved_line_id uuid,
  effective_revision_id uuid not null,
  effective_approved_line_id uuid not null,
  component_key text not null check(pg_catalog.char_length(component_key) between 1 and 256),
  source_component_id uuid,
  component_kind text not null check(component_kind in ('WORK','PROTECTED_WORK','EXPENSE','MILEAGE','ADDITIONAL','ADJUSTMENT')),
  work_date date,
  tax_treatment text,
  source_binding_version_id uuid references private.bpay_next_source_component_binding_version(id) on delete restrict,
  source_origin_certificate jsonb check(source_origin_certificate is null or
    (pg_catalog.jsonb_typeof(source_origin_certificate)='object'
      and pg_catalog.octet_length(source_origin_certificate::text)<=262144)),
  source_binding_sha256 bytea check(pg_catalog.octet_length(source_binding_sha256)=32),
  member_no bigint not null check(member_no>0),
  primary key(accepted_work_input_id,effective_approved_line_id),
  unique(accepted_work_input_id,component_key),
  unique(accepted_work_input_id,member_no),
  foreign key(accepted_work_input_id,run_id,work_id,original_revision_id,effective_revision_id)
    references private.bpay_next_accepted_work_input(id,run_id,work_id,original_revision_id,effective_revision_id) on delete restrict,
  foreign key(run_id,work_id,original_revision_id)
    references private.bpay_next_work_choice(run_id,work_id,expected_revision_id) on delete restrict,
  foreign key(run_id,work_id,original_approved_line_id)
    references private.bpay_next_selected_component(run_id,work_id,approved_line_id) on delete restrict,
  foreign key(effective_revision_id,effective_approved_line_id,component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
  check((source_binding_version_id is null)=(source_binding_sha256 is null)),
  check(source_origin_certificate is null or source_binding_version_id is not null)
);
create index bpay_next_accepted_member_original_idx
  on private.bpay_next_accepted_work_member(original_approved_line_id,accepted_work_input_id)
  where original_approved_line_id is not null;
create index bpay_next_accepted_member_binding_idx
  on private.bpay_next_accepted_work_member(source_binding_version_id,accepted_work_input_id)
  where source_binding_version_id is not null;

create table private.bpay_next_effective_work_cut (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  accepted_input_id uuid not null,
  accepted_work_input_id uuid not null,
  candidate_id uuid not null,
  work_id uuid not null,
  effective_revision_id uuid not null,
  prepare_job_id uuid not null references private.bpay_next_job(id) on delete restrict,
  publication_command_id uuid not null references private.bpay_next_publication(command_id) on delete restrict,
  predecessor_sequence bigint not null check(predecessor_sequence>0),
  selected_line_count bigint not null check(selected_line_count>=0),
  evidence_sha256 bytea not null check(pg_catalog.octet_length(evidence_sha256)=32),
  sealed_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  unique(accepted_input_id,work_id),
  unique(id,work_id,effective_revision_id),
  foreign key(accepted_input_id,candidate_id)
    references private.bpay_next_accepted_input(id,candidate_id) on delete restrict,
  foreign key(accepted_work_input_id,accepted_input_id,work_id,effective_revision_id,publication_command_id,predecessor_sequence,selected_line_count)
    references private.bpay_next_accepted_work_input(id,accepted_input_id,work_id,effective_revision_id,publication_command_id,predecessor_sequence,selected_line_count) on delete restrict,
  foreign key(work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key(publication_command_id,predecessor_sequence)
    references private.bpay_next_command(id,agency_sequence) on delete restrict
);
create index bpay_next_effective_cut_job_idx
  on private.bpay_next_effective_work_cut(prepare_job_id,id);
-- The existing book guard validates this first-seed witness. It is metadata,
-- not a new book/counter; later READY writes must retain the exact first seed.
alter table private.bpay_next_position
  add column source_book_seed_effective_cut_id uuid
    references private.bpay_next_effective_work_cut(id) on delete restrict;
create index bpay_next_position_accepted_seed_idx
  on private.bpay_next_position(source_book_seed_effective_cut_id)
  where source_book_seed_effective_cut_id is not null;

create table private.bpay_next_accepted_case_input (
  accepted_input_id uuid not null,
  run_id uuid not null,
  candidate_id uuid not null,
  selection_revision bigint not null check(selection_revision>0),
  case_component_id uuid not null,
  case_id uuid not null,
  is_selected boolean not null,
  selection_no bigint not null check(selection_no>0),
  rule_id uuid not null,
  rule_revision bigint not null check(rule_revision>0),
  case_status text not null check(case_status in ('OPEN','PAUSED','CLOSED','WRITTEN_OFF')),
  case_revision bigint not null check(case_revision>=0),
  next_due_week_start date,
  component_revision bigint not null check(component_revision>0),
  funded_eligible boolean not null,
  original_funding_event_id uuid references private.bpay_next_case_event(id) on delete restrict,
  original_funded_at_utc timestamptz check(pg_catalog.isfinite(original_funded_at_utc)),
  economic_origin_kind text not null check(economic_origin_kind in ('CASE_CREATE','WORK_COLLECTION')),
  case_create_command_id uuid references private.bpay_next_case_create_request(command_id) on delete restrict,
  work_collection_id uuid references private.bpay_next_work_collection(id) on delete restrict,
  valuation_policy_id uuid references private.bpay_next_valuation_policy(id) on delete restrict,
  primary key(accepted_input_id,case_component_id),
  unique(accepted_input_id,selection_no),
  foreign key(accepted_input_id,run_id,candidate_id,selection_revision)
    references private.bpay_next_accepted_input(id,run_id,candidate_id,case_selection_revision) on delete restrict,
  foreign key(run_id,candidate_id,selection_revision,case_component_id,case_id,is_selected)
    references private.bpay_next_case_selection_item(run_id,candidate_id,selection_revision,case_component_id,case_id,is_selected) on delete restrict,
  foreign key(rule_id,case_id,candidate_id,rule_revision)
    references private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision) on delete restrict,
  foreign key(case_component_id,case_id,candidate_id)
    references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
  check((original_funding_event_id is null)=(original_funded_at_utc is null)),
  check(not funded_eligible or original_funding_event_id is not null),
  check((economic_origin_kind='CASE_CREATE' and case_create_command_id is not null and work_collection_id is null)
    or (economic_origin_kind='WORK_COLLECTION' and case_create_command_id is null and work_collection_id is not null))
);
create index bpay_next_accepted_case_rule_idx
  on private.bpay_next_accepted_case_input(rule_id,accepted_input_id);

-- Captured weekly eligibility is an input to the existing floor calculation.
-- It contains no new cash, paid, held, funded or recovered accounting counter.
alter table private.bpay_next_worker_week_contribution
  add constraint bpay_next_contribution_accepted_identity_key unique(id,candidate_id,pay_week_start,original_run_worker_id);
create table private.bpay_next_accepted_week_input (
  accepted_input_id uuid not null,
  candidate_id uuid not null,
  pay_week_start date not null check(pg_catalog.isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
  contribution_id uuid not null,
  contribution_revision bigint not null check(contribution_revision>0),
  original_run_worker_id uuid not null references private.bpay_next_run_worker(id) on delete restrict,
  original_pay_date date not null check(pg_catalog.isfinite(original_pay_date)),
  original_created_at_utc timestamptz not null check(pg_catalog.isfinite(original_created_at_utc)),
  economic_basis_command_id uuid not null,
  economic_basis_sequence bigint not null check(economic_basis_sequence>0),
  economic_basis_sha256 bytea not null check(pg_catalog.octet_length(economic_basis_sha256)=32),
  accepted_projection_id uuid,
  basis_kind text not null check(basis_kind in ('PAYROLL_NET','GROSS_FALLBACK')),
  payment_state text not null check(payment_state in ('ARRANGED','PAID','FAILED','CANCELLED','RETURNED_OWED','REISSUED_PAID')),
  eligibility_state text not null check(eligibility_state in ('ELIGIBLE','EXCLUDED')),
  eligible_arranged_amount private.bpay_next_penny_amount not null check(eligible_arranged_amount>=0),
  primary_binding_revision bigint not null check(primary_binding_revision>0),
  return_floor_binding_ref text check(pg_catalog.octet_length(return_floor_binding_ref) between 1 and 256),
  original_transfer_id uuid references private.bpay_next_transfer(id) on delete restrict,
  returned_cash_id uuid references private.bpay_next_return_cash(id) on delete restrict,
  reissue_transfer_id uuid references private.bpay_next_transfer(id) on delete restrict,
  primary key(accepted_input_id,contribution_id),
  unique(accepted_input_id,original_run_worker_id),
  foreign key(accepted_input_id,candidate_id,pay_week_start)
    references private.bpay_next_accepted_input(id,candidate_id,pay_week_start) on delete restrict,
  foreign key(contribution_id,candidate_id,pay_week_start,original_run_worker_id)
    references private.bpay_next_worker_week_contribution(id,candidate_id,pay_week_start,original_run_worker_id) on delete restrict,
  foreign key(economic_basis_command_id,economic_basis_sequence)
    references private.bpay_next_command(id,agency_sequence) on delete restrict,
  foreign key(accepted_projection_id,original_run_worker_id)
    references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  foreign key(original_transfer_id,original_run_worker_id)
    references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  foreign key(returned_cash_id,original_transfer_id,candidate_id)
    references private.bpay_next_return_cash(id,original_transfer_id,candidate_id) on delete restrict,
  foreign key(reissue_transfer_id,candidate_id)
    references private.bpay_next_transfer(id,candidate_id) on delete restrict,
  check(original_pay_date>=pay_week_start and original_pay_date<pay_week_start+7),
  check(basis_kind<>'PAYROLL_NET' or accepted_projection_id is not null),
  check(eligibility_state<>'EXCLUDED' or eligible_arranged_amount=0),
  check(payment_state not in ('FAILED','CANCELLED') or eligibility_state='EXCLUDED'),
  check(payment_state not in ('RETURNED_OWED','REISSUED_PAID') or
    (returned_cash_id is not null and original_transfer_id is not null
      and (return_floor_binding_ref is not null or (eligibility_state='EXCLUDED' and eligible_arranged_amount=0)))),
  check(payment_state<>'REISSUED_PAID' or reissue_transfer_id is not null)
);
create index bpay_next_accepted_week_keyset_idx on private.bpay_next_accepted_week_input
  (accepted_input_id,original_pay_date,original_created_at_utc,original_run_worker_id);

-- Version staging and final command receipts are distinct immutable records.
-- xid8 proves same-transaction ownership only; agency_sequence remains the
-- sole accepted order. These are typed financial-setting/query witnesses;
-- contacts, bank details, vectors and mutable finance counters are excluded.
create table private.bpay_next_input_version_event (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  preparing_transaction_id xid8 not null,
  module_epoch bigint not null check(module_epoch>0),
  input_kind text not null check(input_kind in ('CANDIDATE','UMBRELLA','POLICY','PAY_QUERY')),
  owner_relation text not null check(pg_catalog.char_length(owner_relation) between 1 and 128),
  owner_row_id uuid not null,
  original_command_id uuid references private.bpay_next_command(id) on delete restrict deferrable initially deferred,
  old_present boolean not null,
  new_present boolean not null,
  old_sha256 bytea not null check(pg_catalog.octet_length(old_sha256)=32),
  new_sha256 bytea not null check(pg_catalog.octet_length(new_sha256)=32),
  staged_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  unique(id,input_kind,owner_row_id),
  unique(id,owner_relation,owner_row_id),
  check(old_present or new_present)
);
create index bpay_next_version_event_transaction_idx
  on private.bpay_next_input_version_event(preparing_transaction_id,id);
create index bpay_next_version_event_owner_idx
  on private.bpay_next_input_version_event(input_kind,owner_row_id,id);
create table private.bpay_next_candidate_input_version (
  event_id uuid primary key references private.bpay_next_input_version_event(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  input_kind text generated always as ('CANDIDATE'::text) stored,
  old_pay_method text, new_pay_method text,
  old_umbrella_id uuid, new_umbrella_id uuid,
  old_floor numeric(12,2), new_floor numeric(12,2),
  unique(event_id,candidate_id),
  foreign key(event_id,input_kind,candidate_id)
    references private.bpay_next_input_version_event(id,input_kind,owner_row_id) on delete restrict
);
create index bpay_next_candidate_version_candidate_idx
  on private.bpay_next_candidate_input_version(candidate_id,event_id);
create table private.bpay_next_umbrella_input_version (
  event_id uuid primary key references private.bpay_next_input_version_event(id) on delete restrict,
  umbrella_id uuid not null references public.umbrellas(id) on delete restrict,
  input_kind text generated always as ('UMBRELLA'::text) stored,
  old_vat_chargeable boolean,new_vat_chargeable boolean,
  old_enabled boolean,new_enabled boolean,
  unique(event_id,umbrella_id),
  foreign key(event_id,input_kind,umbrella_id)
    references private.bpay_next_input_version_event(id,input_kind,owner_row_id) on delete restrict
);
create index bpay_next_umbrella_version_umbrella_idx
  on private.bpay_next_umbrella_input_version(umbrella_id,event_id);
create table private.bpay_next_policy_input_version (
  event_id uuid primary key references private.bpay_next_input_version_event(id) on delete restrict,
  old_policy_id uuid references private.bpay_next_valuation_policy(id) on delete restrict,
  new_policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
  input_kind text generated always as ('POLICY'::text) stored,
  old_policy_no bigint,new_policy_no bigint not null check(new_policy_no>0),
  unique(event_id,new_policy_id),
  foreign key(event_id,input_kind,new_policy_id)
    references private.bpay_next_input_version_event(id,input_kind,owner_row_id) on delete restrict
);
create index bpay_next_policy_version_policy_idx
  on private.bpay_next_policy_input_version(new_policy_id,event_id);
create table private.bpay_next_query_input_version (
  event_id uuid primary key references private.bpay_next_input_version_event(id) on delete restrict,
  owner_relation text not null,
  owner_row_id uuid not null,
  input_kind text generated always as ('PAY_QUERY'::text) stored,
  candidate_id uuid references public.candidates(id) on delete restrict,
  root_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  old_gate_tuple jsonb not null check(pg_catalog.jsonb_typeof(old_gate_tuple)='object'
    and pg_catalog.octet_length(old_gate_tuple::text)<=16384),
  new_gate_tuple jsonb not null check(pg_catalog.jsonb_typeof(new_gate_tuple)='object'
    and pg_catalog.octet_length(new_gate_tuple::text)<=16384),
  foreign key(event_id,owner_relation,owner_row_id)
    references private.bpay_next_input_version_event(id,owner_relation,owner_row_id) on delete restrict,
  foreign key(event_id,input_kind,owner_row_id)
    references private.bpay_next_input_version_event(id,input_kind,owner_row_id) on delete restrict
);
create index bpay_next_query_version_candidate_idx
  on private.bpay_next_query_input_version(candidate_id,event_id);
create index bpay_next_query_version_root_idx
  on private.bpay_next_query_input_version(root_timesheet_id,event_id);
-- Finite selected-row capture before the clock, consumed by the same atomic
-- Enqueue transaction. This avoids aggregating an entire selection/history
-- into one payload and never survives commit. It is operational staging;
-- retained accepted receipts above remain append-only.
create table private.bpay_next_acceptance_stage (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  preparing_transaction_id xid8 not null,
  run_id uuid not null references private.bpay_next_pay_run(id) on delete restrict,
  prepare_command_id uuid not null,
  record_kind text not null check(record_kind in ('CANDIDATE','WORK','MEMBER','CASE','WEEK')),
  entity_id uuid not null,
  accepted_input_id uuid not null,
  accepted_work_input_id uuid,
  record_json jsonb not null check(pg_catalog.jsonb_typeof(record_json)='object'
    and pg_catalog.octet_length(record_json::text)<=262144),
  unique(prepare_command_id,record_kind,entity_id),
  check((record_kind in ('WORK','MEMBER'))=(accepted_work_input_id is not null))
);
create index bpay_next_acceptance_stage_transaction_idx on private.bpay_next_acceptance_stage
  (preparing_transaction_id,prepare_command_id,record_kind,entity_id);
alter table private.bpay_next_acceptance_stage owner to postgres;
alter table private.bpay_next_acceptance_stage enable row level security;
revoke all on private.bpay_next_acceptance_stage from public,anon,authenticated,service_role;
create or replace function private.bpay_next_acceptance_stage_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
begin
  if tg_when='AFTER' then
    if exists(select 1 from private.bpay_next_acceptance_stage s
      where s.preparing_transaction_id=new.preparing_transaction_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_NOT_CONSUMED';
    end if;
    return null;
  end if;
  if tg_op='INSERT' then
    if new.preparing_transaction_id is distinct from pg_catalog.pg_current_xact_id()
      or exists(select 1 from private.bpay_next_run_command rc where rc.run_id=new.run_id)
      or not exists(select 1 from private.bpay_next_pay_run r
        where r.id=new.run_id and r.status='PREPARING' and r.selection_state='SEALED') then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_OWNER_MISMATCH';
    end if;
    return new;
  elsif tg_op='DELETE' then
    if old.preparing_transaction_id is distinct from pg_catalog.pg_current_xact_id()
      or not exists(select 1 from private.bpay_next_accepted_input i
        where i.id=old.accepted_input_id and i.run_id=old.run_id and i.prepare_command_id=old.prepare_command_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_OWNER_MISMATCH';
    end if;
    return old;
  end if;
  raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_IMMUTABLE';
end
$function$;
create trigger bpay_next_acceptance_stage_guard_v1 before insert or update or delete
  on private.bpay_next_acceptance_stage for each row execute function private.bpay_next_acceptance_stage_guard_v1();
create trigger bpay_next_acceptance_stage_no_truncate_v1 before truncate
  on private.bpay_next_acceptance_stage for each statement execute function private.bpay_next_acceptance_stage_guard_v1();
create constraint trigger bpay_next_acceptance_stage_consumed_v1 after insert
  on private.bpay_next_acceptance_stage deferrable initially deferred
  for each row execute function private.bpay_next_acceptance_stage_guard_v1();
alter function private.bpay_next_acceptance_stage_guard_v1() owner to postgres;
revoke all on function private.bpay_next_acceptance_stage_guard_v1() from public,anon,authenticated,service_role;
create table private.bpay_next_input_version_receipt (
  event_id uuid primary key references private.bpay_next_input_version_event(id) on delete restrict,
  command_id uuid not null,
  agency_sequence bigint not null check(agency_sequence>0),
  module_epoch bigint not null check(module_epoch>0),
  evidence_sha256 bytea not null check(pg_catalog.octet_length(evidence_sha256)=32),
  foreign key(command_id,agency_sequence,module_epoch)
    references private.bpay_next_command(id,agency_sequence,module_epoch) on delete restrict,
  -- The staging event must acquire its exact final receipt before committing.
  unique(event_id,module_epoch)
);
create index bpay_next_version_receipt_command_idx
  on private.bpay_next_input_version_receipt(command_id,event_id);
alter table private.bpay_next_input_version_event
  add constraint bpay_next_version_event_committed_receipt_fk foreign key(id,module_epoch)
    references private.bpay_next_input_version_receipt(event_id,module_epoch)
    on delete restrict deferrable initially deferred;
alter table private.bpay_next_accepted_input
  add constraint bpay_next_accepted_candidate_version_fk foreign key(candidate_version_event_id,candidate_id)
    references private.bpay_next_candidate_input_version(event_id,candidate_id) on delete restrict,
  add constraint bpay_next_accepted_umbrella_version_fk foreign key(umbrella_version_event_id,target_umbrella_id)
    references private.bpay_next_umbrella_input_version(event_id,umbrella_id) on delete restrict,
  add constraint bpay_next_accepted_policy_version_fk foreign key(policy_version_event_id,valuation_policy_id)
    references private.bpay_next_policy_input_version(event_id,new_policy_id) on delete restrict;

-- Install append-only protection in this migration: the between-migration/
-- repeatable interval must not expose writable retained evidence.
create or replace function private.bpay_next_accepted_evidence_immutable_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
begin
  raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_EVIDENCE_IMMUTABLE';
end
$function$;
alter function private.bpay_next_accepted_evidence_immutable_v1() owner to postgres;
revoke all on function private.bpay_next_accepted_evidence_immutable_v1()
  from public,anon,authenticated,service_role;
do $evidence_acl$
declare v_table text;
begin
  foreach v_table in array array['bpay_next_accepted_input','bpay_next_accepted_work_input',
    'bpay_next_accepted_work_member','bpay_next_effective_work_cut','bpay_next_accepted_case_input',
    'bpay_next_accepted_week_input','bpay_next_input_version_event','bpay_next_candidate_input_version',
    'bpay_next_umbrella_input_version','bpay_next_policy_input_version','bpay_next_query_input_version',
    'bpay_next_input_version_receipt'] loop
    execute pg_catalog.format('alter table private.%I owner to postgres',v_table);
    execute pg_catalog.format('alter table private.%I enable row level security',v_table);
    execute pg_catalog.format('revoke all on private.%I from public,anon,authenticated,service_role',v_table);
    execute pg_catalog.format('create trigger %I before update or delete on private.%I for each row execute function private.bpay_next_accepted_evidence_immutable_v1()',v_table||'_immutable',v_table);
    execute pg_catalog.format('create trigger %I before truncate on private.%I for each statement execute function private.bpay_next_accepted_evidence_immutable_v1()',v_table||'_no_truncate',v_table);
  end loop;
end
$evidence_acl$;

commit;
