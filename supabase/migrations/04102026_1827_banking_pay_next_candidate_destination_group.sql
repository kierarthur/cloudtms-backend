-- L22: typed genuine stored-credit origin and disjoint Candidate cash legs.
-- No legacy bootstrap, bank override, grant to browser roles, or financial reprice.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_stored_credit_origin (
  command_id uuid primary key references private.bpay_next_case_create_request(command_id) on delete restrict,
  legacy_case_id uuid not null unique references public.pay_advances(id) on delete restrict,
  legacy_component_id uuid not null unique references public.pay_finance_case_components(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  original_source_pay_channel text not null check (original_source_pay_channel='UMBRELLA'),
  original_tax_treatment text not null check (original_tax_treatment='NON_TAXABLE'),
  original_routing_kind text not null check (original_routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT'),
  principal_source_ex_vat private.bpay_next_penny_amount not null check (principal_source_ex_vat>0),
  original_created_at_utc timestamptz not null check (pg_catalog.isfinite(original_created_at_utc)),
  bank_version_at_utc timestamptz not null check (pg_catalog.isfinite(bank_version_at_utc)),
  bank_details_hash text not null check (pg_catalog.octet_length(bank_details_hash) between 1 and 256),
  beneficiary_name text not null check (pg_catalog.btrim(beneficiary_name)<>''),
  sort_code text not null check (sort_code ~ '^[0-9]{6}$'),
  account_number text not null check (account_number ~ '^[0-9]{8}$'),
  bound_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique(command_id,candidate_id),
  foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict
);
create index bpay_next_stored_credit_candidate_idx on private.bpay_next_stored_credit_origin(candidate_id,command_id);

-- The instruction's original identity and bank version are immutable after capture.
create table private.bpay_next_instruction_destination (
  instruction_id uuid primary key references private.bpay_next_run_case_instruction(id) on delete restrict,
  origin_command_id uuid not null references private.bpay_next_stored_credit_origin(command_id) on delete restrict,
  captured_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
create index bpay_next_instruction_destination_origin_idx on private.bpay_next_instruction_destination(origin_command_id,instruction_id);

-- Each NET pass accumulates only its <=100 current instruction page; completion
-- seals scalar conservation. Prior projection destinations are never rewritten.
create table private.bpay_next_net_destination_state (
  state_id uuid primary key references private.bpay_next_case_allocation_state(id) on delete restrict,
  run_worker_id uuid not null references private.bpay_next_run_worker(id) on delete restrict,
  projection_id uuid unique references private.bpay_next_net_projection(id) on delete restrict,
  external_credit_count bigint not null default 0 check (external_credit_count>=0),
  external_leg_count bigint not null default 0 check (external_leg_count>=0),
  external_amount private.bpay_next_penny_amount not null default 0 check (external_amount>=0),
  own_amount private.bpay_next_penny_amount check (own_amount>=0),
  cash_amount private.bpay_next_penny_amount check (cash_amount>=0),
  completed_at_utc timestamptz,
  check ((projection_id is null and own_amount is null and cash_amount is null and completed_at_utc is null)
    or (projection_id is not null and own_amount is not null and cash_amount is not null
      and completed_at_utc is not null and pg_catalog.isfinite(completed_at_utc) and own_amount+external_amount=cash_amount))
);
create index bpay_next_net_destination_worker_idx on private.bpay_next_net_destination_state(run_worker_id,state_id);
create table private.bpay_next_net_destination_amount (
  destination_id uuid primary key default pg_catalog.gen_random_uuid(),
  state_id uuid not null references private.bpay_next_net_destination_state(state_id) on delete restrict,
  bank_details_hash text not null,
  first_origin_command_id uuid not null references private.bpay_next_stored_credit_origin(command_id) on delete restrict,
  credit_count bigint not null check (credit_count>0),
  amount private.bpay_next_penny_amount not null check (amount>0),
  unique(state_id,bank_details_hash), unique(destination_id,state_id)
);
create index bpay_next_net_destination_page_idx on private.bpay_next_net_destination_amount(state_id,destination_id);
create index bpay_next_net_destination_origin_idx on private.bpay_next_net_destination_amount(first_origin_command_id,destination_id);
create table private.bpay_next_net_destination_credit (
  state_id uuid not null references private.bpay_next_net_destination_state(state_id) on delete restrict,
  instruction_id uuid not null references private.bpay_next_instruction_destination(instruction_id) on delete restrict,
  destination_id uuid not null,
  allocation_result_id uuid not null references private.bpay_next_case_allocation_result(id) on delete restrict,
  amount private.bpay_next_penny_amount not null check (amount>0),
  primary key(state_id,instruction_id),
  foreign key(destination_id,state_id) references private.bpay_next_net_destination_amount(destination_id,state_id) on delete restrict
);
create index bpay_next_net_destination_result_idx on private.bpay_next_net_destination_credit(allocation_result_id,state_id);
create index bpay_next_net_destination_credit_leg_idx on private.bpay_next_net_destination_credit(destination_id,instruction_id);

-- Own leg retains the existing unique build_command_id. Additional legs have
-- NULL build_command_id and exact typed membership, never independent jobs.
create table private.bpay_next_destination_group (
  anchor_transfer_id uuid primary key references private.bpay_next_transfer(id) on delete restrict,
  run_worker_id uuid not null unique references private.bpay_next_run_worker(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  projection_id uuid not null unique references private.bpay_next_net_projection(id) on delete restrict,
  draft_state_id uuid not null references private.bpay_next_case_allocation_state(id) on delete restrict,
  net_state_id uuid not null references private.bpay_next_net_destination_state(state_id) on delete restrict,
  preparation_revision bigint not null check (preparation_revision>0),
  selection_revision bigint not null check (selection_revision>0),
  stage text not null check (stage in ('WORK','CASE','ADJUST','SEAL','COMPLETE')),
  checkpoint bigint not null default 0 check (checkpoint>=0),
  expected_work_count bigint not null check (expected_work_count>=0),
  expected_case_count bigint not null check (expected_case_count>=0),
  expected_leg_count bigint not null check (expected_leg_count>1),
  processed_work_count bigint not null default 0 check (processed_work_count>=0),
  processed_case_count bigint not null default 0 check (processed_case_count>=0),
  created_leg_count bigint not null default 1 check (created_leg_count>0),
  sealed_leg_count bigint not null default 0 check (sealed_leg_count>=0),
  work_cursor bigint,
  cursor_age_key bigint, cursor_case_id uuid, cursor_component_ordinal bigint, cursor_case_component_id uuid,
  seal_cursor integer,
  work_cash_total private.bpay_next_penny_amount not null default 0,
  gross_additions_total private.bpay_next_penny_amount not null default 0,
  gross_deductions_total private.bpay_next_penny_amount not null default 0,
  net_additions_total private.bpay_next_penny_amount not null default 0,
  net_recoveries_total private.bpay_next_penny_amount not null default 0,
  member_cash_total private.bpay_next_penny_amount not null default 0,
  sealed_cash_total private.bpay_next_penny_amount not null default 0,
  completed_at_utc timestamptz,
  check (processed_work_count<=expected_work_count and processed_case_count<=expected_case_count
    and created_leg_count<=expected_leg_count and sealed_leg_count<=created_leg_count),
  check ((stage='COMPLETE')=(completed_at_utc is not null))
);
create index bpay_next_destination_group_candidate_idx on private.bpay_next_destination_group(candidate_id,anchor_transfer_id);
create table private.bpay_next_destination_group_leg (
  transfer_id uuid primary key references private.bpay_next_transfer(id) on delete restrict,
  anchor_transfer_id uuid not null references private.bpay_next_destination_group(anchor_transfer_id) on delete restrict,
  destination_id uuid unique references private.bpay_next_net_destination_amount(destination_id) on delete restrict,
  leg_kind text not null check (leg_kind in ('OWN','ONEOFF')),
  unique(anchor_transfer_id,transfer_id),
  check ((leg_kind='OWN')=(destination_id is null))
);
create unique index bpay_next_destination_group_own_idx on private.bpay_next_destination_group_leg(anchor_transfer_id) where leg_kind='OWN';
create index bpay_next_destination_group_leg_page_idx on private.bpay_next_destination_group_leg(anchor_transfer_id,transfer_id);
create table private.bpay_next_destination_group_member (
  anchor_transfer_id uuid not null references private.bpay_next_destination_group(anchor_transfer_id) on delete restrict,
  subject_kind text not null check (subject_kind in ('WORK','CASE','NET_ADJUSTMENT')),
  subject_id uuid not null,
  transfer_id uuid not null, member_no bigint not null,
  primary key(anchor_transfer_id,subject_kind,subject_id),
  unique(transfer_id,member_no),
  foreign key(anchor_transfer_id,transfer_id) references private.bpay_next_destination_group_leg(anchor_transfer_id,transfer_id) on delete restrict,
  foreign key(transfer_id,member_no) references private.bpay_next_transfer_member(transfer_id,member_no)
    on delete restrict deferrable initially deferred
);

do $acl$
declare n text;
begin
  foreach n in array array['bpay_next_stored_credit_origin','bpay_next_instruction_destination',
    'bpay_next_net_destination_state','bpay_next_net_destination_amount','bpay_next_net_destination_credit',
    'bpay_next_destination_group','bpay_next_destination_group_leg','bpay_next_destination_group_member'] loop
    execute pg_catalog.format('alter table private.%I owner to postgres',n);
    execute pg_catalog.format('revoke all on table private.%I from public,anon,authenticated,service_role',n);
  end loop;
end $acl$;

commit;
