-- Replacement Banking Pay: typed, disabled-first financial records.
-- This schema makes no existing payment route use it. A later reviewed authority
-- must prove the old writer fence before switching the single active owner.
-- Agency identity is the protected one-agency database identity; Source agency
-- identity is checked by its source owner before a work revision is published.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_module_control (
  id smallint primary key check (id = 1),
  active_owner text not null default 'LEGACY' check (active_owner in ('LEGACY','NEXT','DISABLED')),
  owner_epoch bigint not null default 1 check (owner_epoch > 0),
  changed_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
insert into private.bpay_next_module_control (id) values (1);

-- Effective-date conversion rules are copied ONCE when the finance setting
-- changes, never once per Candidate or during a Banking Pay modal read.
-- An approval pins the then-current immutable policy set. The target pay
-- channel/date selects one of its windows only when an offer is captured.
create table private.bpay_next_valuation_policy (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  policy_no bigint not null unique check (policy_no > 0),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
create table private.bpay_next_valuation_policy_window (
  policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
  source_window_id uuid not null,
  date_from date not null,
  date_to date,
  erni_pct numeric not null,
  vat_rate_pct numeric not null,
  primary key (policy_id,source_window_id),
  check (date_to is null or date_to >= date_from),
  check (erni_pct between 0 and 100),
  check (vat_rate_pct between 0 and 100)
);
create index bpay_next_valuation_policy_window_date_idx
  on private.bpay_next_valuation_policy_window(policy_id,date_from desc,source_window_id desc);
create table private.bpay_next_valuation_policy_control (
  id smallint primary key check (id=1),
  current_policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
  current_policy_no bigint not null check (current_policy_no > 0)
);

do $policy_bootstrap$
declare v_policy_id uuid;
begin
  insert into private.bpay_next_valuation_policy(policy_no)
    values (1) returning id into v_policy_id;
  insert into private.bpay_next_valuation_policy_window
    (policy_id,source_window_id,date_from,date_to,erni_pct,vat_rate_pct)
    select v_policy_id,w.id,w.date_from,w.date_to,w.erni_pct,w.vat_rate_pct
      from public.settings_finance_windows w;
  insert into private.bpay_next_valuation_policy_control
    (id,current_policy_id,current_policy_no) values (1,v_policy_id,1);
end
$policy_bootstrap$;

create or replace function private.bpay_next_current_valuation_policy_id_v1()
returns uuid language plpgsql volatile
set search_path = pg_catalog, private
as $function$
declare v_policy_id uuid;
begin
  -- This short row lock orders an approval against a concurrent settings edit.
  -- The edit takes FOR UPDATE on this same row before advancing the policy.
  select current_policy_id into strict v_policy_id
    from private.bpay_next_valuation_policy_control where id=1 for share;
  return v_policy_id;
end
$function$;

create or replace function private.bpay_next_finance_policy_change_v1()
returns trigger language plpgsql
security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_policy_id uuid;
  v_policy_no bigint;
begin
  select current_policy_no+1 into strict v_policy_no
    from private.bpay_next_valuation_policy_control where id=1 for update;
  insert into private.bpay_next_valuation_policy(policy_no)
    values (v_policy_no) returning id into v_policy_id;
  insert into private.bpay_next_valuation_policy_window
    (policy_id,source_window_id,date_from,date_to,erni_pct,vat_rate_pct)
    select v_policy_id,w.id,w.date_from,w.date_to,w.erni_pct,w.vat_rate_pct
      from public.settings_finance_windows w;
  update private.bpay_next_valuation_policy_control
    set current_policy_id=v_policy_id,current_policy_no=v_policy_no where id=1;
  return null;
end
$function$;
create trigger bpay_next_finance_policy_insert_v1
after insert on public.settings_finance_windows
for each statement execute function private.bpay_next_finance_policy_change_v1();
create trigger bpay_next_finance_policy_update_v1
after update on public.settings_finance_windows
for each statement execute function private.bpay_next_finance_policy_change_v1();
create trigger bpay_next_finance_policy_delete_v1
after delete on public.settings_finance_windows
for each statement execute function private.bpay_next_finance_policy_change_v1();
create trigger bpay_next_finance_policy_truncate_v1
after truncate on public.settings_finance_windows
for each statement execute function private.bpay_next_finance_policy_change_v1();

create table private.bpay_next_work (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  original_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  booking_id text not null check (pg_catalog.char_length(booking_id) between 1 and 200),
  work_kind text not null check (work_kind in ('ORDINARY','SOURCE','EXPENSE','ADJUSTMENT')),
  week_ending_date date not null,
  approval_state text not null default 'PENDING' check (approval_state in ('PENDING','APPROVED','WITHDRAWN')),
  current_revision_id uuid,
  applied_revision_id uuid,
  current_revision_no bigint not null default 0 check (current_revision_no >= 0),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (id,candidate_id),
  unique (id,original_timesheet_id),
  unique (original_timesheet_id),
  unique (booking_id),
  check ((approval_state='APPROVED') = (current_revision_id is not null))
);

create table private.bpay_next_work_revision (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  work_id uuid not null references private.bpay_next_work(id) on delete restrict,
  revision_no bigint not null check (revision_no > 0),
  source_kind text not null check (source_kind in ('ORDINARY','SOURCE','PROTECTED','EXPENSE','ADJUSTMENT')),
  source_head_id uuid,
  -- One immutable Source event (a first authorisation or a committed head)
  -- names one approval. It makes multi-root retry identity exact without
  -- scanning older Timesheet versions or inventing another history ledger.
  source_event_id uuid unique,
  physical_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  -- Ordinary approval pins the exact financial row used to create this revision.
  -- Source-owned revisions may instead pin their Source head above.
  financial_snapshot_id uuid references public.timesheets_financials(id) on delete restrict,
  physical_timesheet_version integer not null check (physical_timesheet_version > 0),
  source_pay_channel text not null check (source_pay_channel in ('PAYE','UMBRELLA')),
  valuation_policy_id uuid not null default private.bpay_next_current_valuation_policy_id_v1()
    references private.bpay_next_valuation_policy(id) on delete restrict,
  currency text not null default 'GBP' check (currency = 'GBP'),
  week_ending_date date not null,
  detail_kind text not null check (detail_kind in ('SHIFT','AGGREGATE','FIXED')),
  expected_line_count integer not null check (expected_line_count>=0),
  expected_rate_schedule_count integer not null default 0
    check (expected_rate_schedule_count>=0),
  certified_zero boolean not null default false,
  approved_source_ex_vat numeric(18,2) not null,
  candidate_display_name text not null,
  candidate_reference text,
  client_display_name text not null,
  job_title text,
  band_label text,
  timesheet_reference text,
  approved_at_utc timestamptz,
  sealed_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (work_id,revision_no),
  unique (work_id,id),
  check ((approved_at_utc is null) = (sealed_at_utc is null)),
  -- A genuinely zero-pay approval may still have worked shifts and rate
  -- evidence. Conversely, an empty line set can only certify zero.
  check (expected_line_count<>0 or certified_zero),
  check (not certified_zero or approved_source_ex_vat=0),
  check (pg_catalog.octet_length(candidate_display_name) <= 8192),
  check (pg_catalog.octet_length(client_display_name) <= 8192),
  check (job_title is null or pg_catalog.octet_length(job_title) <= 8192)
);

alter table private.bpay_next_work
  add constraint bpay_next_work_current_revision_fk
  foreign key (id,current_revision_id)
  references private.bpay_next_work_revision(work_id,id) on delete restrict deferrable;
alter table private.bpay_next_work
  add constraint bpay_next_work_applied_revision_fk
  foreign key (id,applied_revision_id)
  references private.bpay_next_work_revision(work_id,id) on delete restrict deferrable;

create table private.bpay_next_approved_line (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  revision_id uuid not null references private.bpay_next_work_revision(id) on delete restrict,
  line_no integer not null check (line_no > 0),
  component_key text not null check (pg_catalog.char_length(component_key) between 1 and 256),
  source_component_id uuid,
  component_kind text not null check (component_kind in ('WORK','PROTECTED_WORK','EXPENSE','MILEAGE','ADDITIONAL','ADJUSTMENT')),
  work_date date,
  unit_label text,
  approved_quantity numeric,
  approved_unit_rate numeric,
  expected_rate_detail_count smallint not null default 0
    check (expected_rate_detail_count between 0 and 5),
  source_pay_ex_vat numeric(18,2) not null,
  tax_treatment text,
  evidence_ref text,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (revision_id,line_no),
  unique (revision_id,component_key),
  unique (revision_id,id),
  unique (revision_id,id,component_key),
  -- A complete Source head may contain a signed correction component. The
  -- producer, not this shared storage row, decides which origins permit it.
  check (evidence_ref is null or pg_catalog.octet_length(evidence_ref) <= 8192)
);

create table private.bpay_next_shift_detail (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  approved_line_id uuid not null references private.bpay_next_approved_line(id) on delete restrict,
  detail_no integer not null check (detail_no > 0),
  work_date date not null,
  shift_start_at timestamptz,
  shift_end_at timestamptz,
  shift_start_local text,
  shift_end_local text,
  -- Local Source clocks do not imply a timezone. Retain the supplied
  -- overnight flag so a 21:00-07:30 shift is not read as ending earlier.
  shift_overnight boolean,
  submitted_minutes integer check (submitted_minutes >= 0),
  approved_minutes integer check (approved_minutes >= 0),
  approved_hours numeric(18,6),
  detail_label text,
  immutable_evidence_ref text,
  unique (approved_line_id,detail_no),
  check (approved_minutes is not null or approved_hours is not null
         or shift_start_at is not null or shift_start_local is not null),
  check ((shift_start_at is null) = (shift_end_at is null)),
  check (shift_start_at is null or shift_end_at > shift_start_at),
  check ((shift_start_local is null) = (shift_end_local is null)),
  check (shift_start_local is null or
         (shift_start_local ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' and
          shift_end_local ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')),
  check (detail_label is null or pg_catalog.octet_length(detail_label) <= 8192)
);

-- A segment has one exact money total above. Its rate buckets are frozen
-- separately, avoiding invented blended rates and duplicate money lines.
create table private.bpay_next_rate_detail (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  approved_line_id uuid not null references private.bpay_next_approved_line(id) on delete restrict,
  bucket text not null check (bucket in ('DAY','NIGHT','SAT','SUN','BH')),
  approved_hours numeric(18,6) not null,
  -- Source heads certify component money and bucket hours but do not carry an
  -- actual hourly rate. NULL is honest; it must not be replaced by a guessed
  -- contract or effective rate. First Source and ordinary approvals pin one.
  source_pay_rate numeric(18,6),
  unique (approved_line_id,bucket)
);

-- Freeze the available contract rates once per approved revision. Standard
-- hour buckets, configured additional-unit codes and mileage can all be
-- inspected when resolving a later PAYE/umbrella mismatch. Only the fields
-- applicable to that rate family are populated; no full contract JSON moves
-- into payment preparation or a Draft.
create table private.bpay_next_rate_schedule (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  revision_id uuid not null references private.bpay_next_work_revision(id) on delete restrict,
  rate_family text not null check (rate_family in ('STANDARD','ADDITIONAL','MILEAGE')),
  rate_code text not null check (pg_catalog.char_length(rate_code) between 1 and 128),
  unit_label text,
  paye_rate numeric(18,6),
  umbrella_rate numeric(18,6),
  charge_rate numeric(18,6),
  unique (revision_id,rate_family,rate_code),
  check (unit_label is null or pg_catalog.octet_length(unit_label)<=512),
  check (paye_rate is not null or umbrella_rate is not null or charge_rate is not null),
  check ((rate_family='STANDARD' and rate_code in ('DAY','NIGHT','SAT','SUN','BH'))
         or (rate_family='MILEAGE' and rate_code='MILE')
         or rate_family='ADDITIONAL')
);

-- A shift can contain multiple breaks. Each is a small immutable detail row;
-- a duration-only break must not acquire fabricated clock endpoints.
create table private.bpay_next_break_detail (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  shift_detail_id uuid not null references private.bpay_next_shift_detail(id) on delete restrict,
  break_no integer not null check (break_no > 0),
  break_start_at timestamptz,
  break_end_at timestamptz,
  -- Some approved schedules record a local HH:MM break without a timezone.
  -- Preserve those exact clock labels rather than inventing UTC instants.
  break_start_local text,
  break_end_local text,
  break_minutes integer not null check (break_minutes >= 0),
  unique (shift_detail_id,break_no),
  check ((break_start_at is null) = (break_end_at is null)),
  check ((break_start_local is null) = (break_end_local is null)),
  check (break_start_at is null or break_end_at > break_start_at),
  check (break_start_local is null or
         (break_start_local ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' and
          break_end_local ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'))
);

-- The accepted Source decision selects the exact presentation facts before
-- immediate or deferred publication. A head component by itself has money and
-- hours but not its immutable shift clocks or per-segment rates. Keep only
-- that small chosen detail, keyed by the proposed head and component; never
-- reconstruct it later from the latest upload or Timesheet.
create table private.bpay_next_source_chosen_detail (
  head_id uuid not null,
  component_id uuid not null,
  decision_bundle_id uuid not null,
  bundle_revision bigint not null,
  component_sha256 bytea not null
    check (pg_catalog.octet_length(component_sha256)=32),
  detail_json jsonb not null
    check (pg_catalog.jsonb_typeof(detail_json)='object'
      and pg_catalog.octet_length(detail_json::text)<=4096),
  detail_sha256 bytea not null
    check (pg_catalog.octet_length(detail_sha256)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  primary key (head_id,component_id),
  foreign key (decision_bundle_id,bundle_revision)
    references public.weekly_source_entitlement_decision_bundles
      (decision_bundle_id,bundle_revision) on delete restrict
);
create index bpay_next_source_chosen_detail_bundle_idx
  on private.bpay_next_source_chosen_detail
    (decision_bundle_id,bundle_revision,head_id,component_id);

create table private.bpay_next_position (
  work_id uuid not null references private.bpay_next_work(id) on delete restrict,
  component_key text not null check (pg_catalog.char_length(component_key) between 1 and 256),
  applied_revision_id uuid not null,
  -- The basis of any already realised/held SOURCE amount is retained when a
  -- later approval changes channel. Cross-basis subtraction is never payable.
  source_basis_channel text check (source_basis_channel in ('PAYE','UMBRELLA')),
  approved_source_ex_vat numeric(18,2) not null default 0,
  realised_source_ex_vat numeric(18,2) not null default 0,
  held_source_ex_vat numeric(18,2) not null default 0 check (held_source_ex_vat >= 0),
  realised_target_ex_vat numeric(18,2) not null default 0,
  realised_target_vat numeric(18,2) not null default 0,
  realised_target_inc_vat numeric(18,2) not null default 0,
  held_target_ex_vat numeric(18,2) not null default 0 check (held_target_ex_vat >= 0),
  held_target_vat numeric(18,2) not null default 0 check (held_target_vat >= 0),
  held_target_inc_vat numeric(18,2) not null default 0 check (held_target_inc_vat >= 0),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  primary key (work_id,component_key),
  foreign key (work_id,applied_revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict,
  check (realised_target_inc_vat = realised_target_ex_vat + realised_target_vat),
  check (held_target_inc_vat = held_target_ex_vat + held_target_vat)
);

-- Current case balances are maintained by exact, unique case events. A loan
-- payout is not an earnings line; its principal becomes effective once only
-- when the original cash is funded, not when returned cash is reissued.
create table private.bpay_next_finance_case (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  case_kind text not null check (case_kind in ('LOAN','ADVANCE','OVERPAYMENT','CREDIT','MANUAL_DEBT')),
  tax_treatment text not null check (tax_treatment in ('TAXABLE','NON_TAXABLE','NOT_APPLICABLE')),
  status text not null check (status in ('OPEN','PAUSED','CLOSED','WRITTEN_OFF')),
  principal_approved numeric(18,2) not null default 0 check (principal_approved >= 0),
  principal_funded numeric(18,2) not null default 0 check (principal_funded >= 0),
  principal_recovered numeric(18,2) not null default 0 check (principal_recovered >= 0),
  principal_written_off numeric(18,2) not null default 0 check (principal_written_off >= 0),
  active_hold_amount numeric(18,2) not null default 0 check (active_hold_amount >= 0),
  due_date date,
  order_key bigint not null default 0,
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (id,candidate_id),
  check (principal_recovered + principal_written_off <= principal_funded)
);

create table private.bpay_next_case_event (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  case_id uuid not null references private.bpay_next_finance_case(id) on delete restrict,
  operation_id uuid not null,
  operation_item_id uuid not null,
  event_kind text not null check (event_kind in ('APPROVED','FUNDED','RECOVERED','WRITTEN_OFF','CORRECTED')),
  approved_delta numeric(18,2) not null default 0,
  funded_delta numeric(18,2) not null default 0,
  recovered_delta numeric(18,2) not null default 0,
  written_off_delta numeric(18,2) not null default 0,
  original_transfer_id uuid,
  original_effect_id uuid,
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (operation_id,operation_item_id,event_kind)
);

create table private.bpay_next_financial_effect (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  operation_id uuid not null,
  operation_item_id uuid not null,
  effect_kind text not null check (pg_catalog.char_length(effect_kind) between 1 and 64),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  work_id uuid not null,
  component_key text not null,
  case_id uuid,
  original_timesheet_id uuid not null,
  original_transfer_id uuid,
  source_disposition_ex_vat numeric(18,2) not null default 0,
  target_amount_ex_vat numeric(18,2) not null default 0,
  target_amount_vat numeric(18,2) not null default 0,
  target_amount_inc_vat numeric(18,2) not null default 0,
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (operation_id,operation_item_id,effect_kind),
  foreign key (work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key (work_id,original_timesheet_id)
    references private.bpay_next_work(id,original_timesheet_id) on delete restrict,
  foreign key (work_id,component_key)
    references private.bpay_next_position(work_id,component_key) on delete restrict,
  foreign key (case_id,candidate_id)
    references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
  check (target_amount_inc_vat = target_amount_ex_vat + target_amount_vat),
  check (pg_catalog.char_length(component_key) between 1 and 256)
);

create table private.bpay_next_pay_run (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  pay_date date not null,
  status text not null check (status in ('PREPARING','REVIEW','DRAFT','CANCELLED','EXECUTING','COMPLETE')),
  selection_state text not null default 'OPEN'
    check (selection_state in ('OPEN','SEALED')),
  selection_page_count bigint not null default 0 check (selection_page_count>=0),
  selection_count bigint not null default 0 check (selection_count>=0),
  selected_candidate_count bigint not null default 0
    check (selected_candidate_count>=0),
  cancelled_candidate_count bigint not null default 0
    check (cancelled_candidate_count between 0 and selected_candidate_count),
  review_revision bigint not null default 0 check (review_revision >= 0),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  confirmed_at_utc timestamptz
);

-- A selection is recorded in small, retryable ID-only pages. The page JSON
-- contains at most 100 UUID strings, never Timesheet/history payloads. Its
-- exact bytes are retained only to distinguish a retry from changed input.
create table private.bpay_next_selection_page (
  run_id uuid not null references private.bpay_next_pay_run(id) on delete restrict,
  page_no bigint not null check (page_no>0),
  work_ids_json jsonb not null
    check (pg_catalog.jsonb_typeof(work_ids_json)='array'
      and pg_catalog.jsonb_array_length(work_ids_json) between 1 and 100
      and pg_catalog.octet_length(work_ids_json::text)<=262144),
  first_selection_no bigint not null check (first_selection_no>0),
  item_count integer not null check (item_count between 1 and 100),
  primary key (run_id,page_no)
);
create table private.bpay_next_run_selection (
  run_id uuid not null references private.bpay_next_pay_run(id) on delete restrict,
  selection_no bigint not null check (selection_no>0),
  work_id uuid not null,
  candidate_id uuid not null,
  primary key (run_id,work_id),
  unique (run_id,selection_no),
  foreign key (work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict
);
create index bpay_next_selection_candidate_page_idx
  on private.bpay_next_run_selection(run_id,candidate_id,selection_no);
create table private.bpay_next_selection_candidate (
  run_id uuid not null references private.bpay_next_pay_run(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  member_no bigint not null check (member_no>0),
  primary key (run_id,candidate_id),
  unique (run_id,member_no)
);

create table private.bpay_next_run_worker (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_id uuid not null references private.bpay_next_pay_run(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  target_pay_channel text not null check (target_pay_channel in ('PAYE','UMBRELLA')),
  target_umbrella_id uuid references public.umbrellas(id) on delete restrict,
  target_umbrella_vat_chargeable boolean,
  status text not null check (status in ('PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE')),
  review_issue_code text,
  review_issue_work_id uuid,
  gross_ex_vat numeric(18,2) not null default 0,
  gross_vat numeric(18,2) not null default 0,
  gross_inc_vat numeric(18,2) not null default 0,
  entered_paye_net numeric(18,2),
  net_request_revision bigint not null default 0 check (net_request_revision >= 0),
  net_projection_revision bigint not null default 0 check (net_projection_revision >= 0),
  captured_line_count bigint not null default 0 check (captured_line_count>=0),
  captured_position_count bigint not null default 0
    check (captured_position_count>=0),
  financial_resolution_count bigint not null default 0
    check (financial_resolution_count>=0),
  realised_effect_count bigint not null default 0 check (realised_effect_count>=0),
  unique (run_id,candidate_id),
  unique (id,candidate_id),
  foreign key (review_issue_work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict,
  check ((review_issue_code is null)=(review_issue_work_id is null)),
  check (review_issue_code is null or
    pg_catalog.octet_length(review_issue_code)<=256),
  check (review_issue_code is null or status='REVIEW'),
  check (gross_inc_vat = gross_ex_vat + gross_vat)
);

-- One captured revision per selected work, regardless of page size or the
-- number of component lines. Candidate identity is checked on both sides.
create table private.bpay_next_run_work (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null,
  candidate_id uuid not null,
  work_id uuid not null,
  captured_revision_id uuid not null,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (run_worker_id,work_id),
  unique (id,run_worker_id,work_id,captured_revision_id),
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key (work_id,captured_revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict
);

-- Small current-position evidence, not a copy of Timesheet or payment history.
-- A removed approved component can still have paid or held money and must not
-- vanish just because the new approval has no matching approved_line row.
create table private.bpay_next_run_position (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null,
  run_work_id uuid not null,
  work_id uuid not null,
  captured_revision_id uuid not null,
  component_key text not null,
  approved_line_id uuid,
  source_pay_channel text not null check (source_pay_channel in ('PAYE','UMBRELLA')),
  source_basis_channel text check (source_basis_channel in ('PAYE','UMBRELLA')),
  approved_source_ex_vat numeric(18,2) not null,
  realised_source_ex_vat numeric(18,2) not null,
  held_source_ex_vat numeric(18,2) not null check (held_source_ex_vat>=0),
  residual_source_ex_vat numeric(18,2) not null,
  realised_target_ex_vat numeric(18,2) not null,
  realised_target_vat numeric(18,2) not null,
  realised_target_inc_vat numeric(18,2) not null,
  held_target_ex_vat numeric(18,2) not null,
  held_target_vat numeric(18,2) not null,
  held_target_inc_vat numeric(18,2) not null,
  needs_financial_resolution boolean not null,
  captured_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (run_work_id,component_key),
  foreign key (run_work_id,run_worker_id,work_id,captured_revision_id)
    references private.bpay_next_run_work(id,run_worker_id,work_id,captured_revision_id)
    on delete restrict,
  foreign key (captured_revision_id,approved_line_id,component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key)
    on delete restrict,
  check (residual_source_ex_vat=
    approved_source_ex_vat-realised_source_ex_vat-held_source_ex_vat),
  check (realised_target_inc_vat=realised_target_ex_vat+realised_target_vat),
  check (held_target_inc_vat=held_target_ex_vat+held_target_vat),
  check (approved_line_id is not null or needs_financial_resolution)
);

create table private.bpay_next_run_line (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null,
  run_work_id uuid not null,
  line_no bigint not null check (line_no > 0),
  approved_line_id uuid not null references private.bpay_next_approved_line(id) on delete restrict,
  captured_revision_id uuid not null references private.bpay_next_work_revision(id) on delete restrict,
  work_id uuid not null references private.bpay_next_work(id) on delete restrict,
  component_key text not null,
  source_consumed_ex_vat numeric(18,2) not null,
  check (source_consumed_ex_vat>=0),
  source_pay_channel text not null check (source_pay_channel in ('PAYE','UMBRELLA')),
  target_pay_channel text not null check (target_pay_channel in ('PAYE','UMBRELLA')),
  valuation_policy_id uuid not null
    references private.bpay_next_valuation_policy(id) on delete restrict,
  policy_window_id uuid not null,
  -- These are valued for the selected pay date and target channel, not at approval.
  frozen_ex_vat numeric(18,2) not null,
  frozen_vat numeric(18,2) not null,
  frozen_inc_vat numeric(18,2) not null,
  unique (run_worker_id,line_no),
  unique (run_worker_id,approved_line_id),
  unique (run_work_id,component_key),
  unique (id,run_worker_id),
  unique (id,work_id,component_key),
  foreign key (run_work_id,run_worker_id,work_id,captured_revision_id)
    references private.bpay_next_run_work(id,run_worker_id,work_id,captured_revision_id) on delete restrict,
  foreign key (captured_revision_id,approved_line_id,component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
  check (frozen_inc_vat = frozen_ex_vat + frozen_vat)
);

create table private.bpay_next_hold (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_line_id uuid not null,
  work_id uuid not null references private.bpay_next_work(id) on delete restrict,
  component_key text not null,
  source_reserved_ex_vat numeric(18,2) not null check (source_reserved_ex_vat >= 0),
  target_amount_ex_vat numeric(18,2) not null check (target_amount_ex_vat >= 0),
  target_amount_vat numeric(18,2) not null check (target_amount_vat >= 0),
  target_amount_inc_vat numeric(18,2) not null check (target_amount_inc_vat >= 0),
  status text not null check (status in ('ACTIVE','REALISED','RELEASED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  finished_at_utc timestamptz,
  unique (run_line_id),
  foreign key (run_line_id,work_id,component_key)
    references private.bpay_next_run_line(id,work_id,component_key) on delete restrict,
  check ((status='ACTIVE') = (finished_at_utc is null)),
  check (target_amount_inc_vat=target_amount_ex_vat+target_amount_vat)
);

create table private.bpay_next_case_hold (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null,
  candidate_id uuid not null,
  case_id uuid not null,
  amount numeric(18,2) not null check (amount >= 0),
  status text not null check (status in ('ACTIVE','REALISED','RELEASED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  finished_at_utc timestamptz,
  unique (id,run_worker_id),
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (case_id,candidate_id)
    references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
  check ((status='ACTIVE') = (finished_at_utc is null))
);

-- A PAYE net entry replaces a projection row, never frozen approved work.
-- Original transfers retain the accepted projection they were made from.
create table private.bpay_next_net_projection (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null references private.bpay_next_run_worker(id) on delete restrict,
  request_command_id uuid unique,
  projection_no bigint not null check (projection_no > 0),
  input_kind text not null check (input_kind in ('PAYE_MANUAL','PAYE_IMPORT','UMBRELLA')),
  gross_ex_vat numeric(18,2) not null,
  gross_vat numeric(18,2) not null,
  gross_inc_vat numeric(18,2) not null,
  entered_paye_net numeric(18,2),
  accepted_recoveries numeric(18,2) not null default 0 check (accepted_recoveries >= 0),
  cash_amount numeric(18,2) not null check (cash_amount >= 0),
  accepted_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  retired_at_utc timestamptz,
  unique (run_worker_id,projection_no),
  unique (id,run_worker_id),
  check (gross_inc_vat=gross_ex_vat+gross_vat),
  check ((input_kind='UMBRELLA')=(entered_paye_net is null))
);

create table private.bpay_next_transfer (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null,
  candidate_id uuid not null,
  projection_id uuid,
  build_command_id uuid unique,
  transfer_no integer not null check (transfer_no > 0),
  beneficiary_kind text not null check (beneficiary_kind in ('CANDIDATE','UMBRELLA','OTHER_APPROVED')),
  beneficiary_id uuid not null,
  -- Bank approval is bound at execution, not at Draft or member assembly.
  account_approval_ref text,
  destination_rail text check (destination_rail in ('CSV','REVOLUT')),
  beneficiary_name_snapshot text,
  sort_code_snapshot text,
  account_number_snapshot text,
  bank_details_hash_snapshot text,
  currency text not null default 'GBP' check (currency = 'GBP'),
  cash_amount numeric(18,2) not null check (cash_amount >= 0),
  member_count bigint not null default 0 check (member_count >= 0),
  member_cash_sum numeric(18,2) not null default 0,
  status text not null check (status in ('BUILDING','MEMBERS_READY','DRAFT','SCHEDULED','ISSUED_CSV','SUBMITTED','UNKNOWN','SETTLED','RETURNED','REFUSED','CANCELLED')),
  original_transfer_id uuid,
  return_cash_id uuid,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (run_worker_id,transfer_no),
  unique (id,candidate_id),
  unique (id,run_worker_id),
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (projection_id,run_worker_id)
    references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  foreign key (original_transfer_id,candidate_id)
    references private.bpay_next_transfer(id,candidate_id) on delete restrict,
  check (original_transfer_id is distinct from id),
  check ((original_transfer_id is null)=(projection_id is not null)),
  check ((original_transfer_id is not null)=(return_cash_id is not null)),
  check (status in ('BUILDING','MEMBERS_READY','CANCELLED') or
    (account_approval_ref is not null and destination_rail is not null
     and nullif(pg_catalog.btrim(beneficiary_name_snapshot),'') is not null
     and sort_code_snapshot ~ '^[0-9]{6}$'
     and account_number_snapshot ~ '^[0-9]{8}$'
     and nullif(pg_catalog.btrim(bank_details_hash_snapshot),'') is not null)),
  check (status <> 'MEMBERS_READY' or
    (member_count > 0 and member_cash_sum=cash_amount))
);

-- The cash return has one owner. A reissue is cash, not a second payroll or
-- loan-principal event; it may be directed to a newly approved account for
-- the same candidate only. Short owner transactions maintain these balances.
create table private.bpay_next_return_cash (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  original_transfer_id uuid not null,
  candidate_id uuid not null,
  amount_owed numeric(18,2) not null check (amount_owed > 0),
  amount_held numeric(18,2) not null default 0 check (amount_held >= 0),
  amount_reissued_paid numeric(18,2) not null default 0 check (amount_reissued_paid >= 0),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (original_transfer_id),
  unique (id,candidate_id),
  unique (id,original_transfer_id,candidate_id),
  foreign key (original_transfer_id,candidate_id)
    references private.bpay_next_transfer(id,candidate_id) on delete restrict,
  check (amount_held+amount_reissued_paid<=amount_owed)
);
alter table private.bpay_next_transfer
  add constraint bpay_next_transfer_reissue_cash_fk
  foreign key (return_cash_id,original_transfer_id,candidate_id)
  references private.bpay_next_return_cash(id,original_transfer_id,candidate_id)
  on delete restrict deferrable;

-- An original transfer's constituents are row-based, not an all-run JSON.
-- Signed cash contributions may include a PAYE/tax or deduction adjustment.
create table private.bpay_next_transfer_member (
  transfer_id uuid not null,
  run_worker_id uuid not null,
  member_no bigint not null check (member_no > 0),
  subject_kind text not null check (subject_kind in ('WORK','CASE','NET_ADJUSTMENT','CASH_REISSUE')),
  run_line_id uuid,
  case_hold_id uuid,
  signed_cash_contribution numeric(18,2) not null,
  primary key (transfer_id,member_no),
  foreign key (transfer_id,run_worker_id)
    references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  foreign key (run_line_id,run_worker_id)
    references private.bpay_next_run_line(id,run_worker_id) on delete restrict,
  foreign key (case_hold_id,run_worker_id)
    references private.bpay_next_case_hold(id,run_worker_id) on delete restrict,
  check ((subject_kind='WORK' and run_line_id is not null and case_hold_id is null)
      or (subject_kind='CASE' and case_hold_id is not null and run_line_id is null)
      or (subject_kind in ('NET_ADJUSTMENT','CASH_REISSUE') and run_line_id is null and case_hold_id is null))
);
create unique index bpay_next_transfer_work_member_once_idx
  on private.bpay_next_transfer_member(transfer_id,run_line_id)
  where subject_kind='WORK';

create table private.bpay_next_transfer_outcome (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  transfer_id uuid not null references private.bpay_next_transfer(id) on delete restrict,
  receipt_id text not null check (pg_catalog.octet_length(receipt_id) between 1 and 256),
  outcome_kind text not null check (outcome_kind in ('SUBMITTED','UNKNOWN','SETTLED','RETURNED','REFUSED','CSV_ISSUED')),
  whole_transfer_amount numeric(18,2) not null check (whole_transfer_amount >= 0),
  occurred_at_utc timestamptz not null,
  received_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (receipt_id),
  unique (transfer_id,receipt_id)
);

-- The first CSV vertical slice identifies one externally executable
-- Candidate instruction. Exact bytes are retained for replay/download;
-- broader files will use row-backed membership rather than a growing JSON.
create table private.bpay_next_csv_instruction (
  id uuid primary key,
  transfer_id uuid not null unique,
  run_worker_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  payment_reference text not null unique
    check (pg_catalog.octet_length(payment_reference) between 1 and 64),
  bank_details_hash_snapshot text not null,
  cash_amount numeric(18,2) not null check (cash_amount > 0),
  file_name text not null unique
    check (pg_catalog.octet_length(file_name) between 1 and 128),
  csv_text text not null check (pg_catalog.octet_length(csv_text) between 1 and 4096),
  csv_sha256 text not null check (csv_sha256 ~ '^[0-9a-f]{64}$'),
  issued_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  foreign key (transfer_id,run_worker_id)
    references private.bpay_next_transfer(id,run_worker_id) on delete restrict
);

create table private.bpay_next_worker_control (
  candidate_id uuid primary key references public.candidates(id) on delete restrict,
  financial_view_revision bigint not null default 0 check (financial_view_revision >= 0),
  active_owner_epoch bigint not null default 0 check (active_owner_epoch >= 0),
  pending_outcome_count integer not null default 0 check (pending_outcome_count >= 0),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);

-- A command is received once and ordered once before its worker jobs exist.
-- Sealed membership is the boundary against a late outcome overtaking intake.
create table private.bpay_next_command_clock (
  id smallint primary key check (id=1),
  last_sequence bigint not null default 0 check (last_sequence>=0)
);
insert into private.bpay_next_command_clock(id) values (1);

create table private.bpay_next_command (
  id uuid primary key,
  agency_sequence bigint not null unique check (agency_sequence>0),
  module_epoch bigint not null check (module_epoch>0),
  command_kind text not null,
  status text not null check (status in ('RECEIVED','ENROLLING','SEALED','COMPLETE','FAILED')),
  expected_member_count bigint check (expected_member_count >= 0),
  enrolled_member_count bigint not null default 0 check (enrolled_member_count >= 0),
  enrollment_cursor text,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  sealed_at_utc timestamptz,
  unique (id,agency_sequence),
  check (expected_member_count is null or enrolled_member_count <= expected_member_count),
  check (status not in ('SEALED','COMPLETE') or sealed_at_utc is not null)
);

create table private.bpay_next_run_command (
  run_id uuid primary key references private.bpay_next_pay_run(id) on delete restrict,
  command_id uuid not null unique references private.bpay_next_command(id) on delete restrict
);

create table private.bpay_next_command_member (
  command_id uuid not null references private.bpay_next_command(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  member_no bigint not null check (member_no > 0),
  primary key (command_id,candidate_id),
  unique (command_id,member_no)
);

-- Accepted PAYE input is an ordered, immutable request. It is not a
-- projection, transfer, payroll posting or permission to release a hold.
-- Later replacements can have their own request numbers; the initial narrow
-- owner admits only the first request until replacement semantics are proved.
create table private.bpay_next_paye_net_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  request_no bigint not null check (request_no > 0),
  entered_paye_net numeric(18,2) not null check (entered_paye_net >= 0),
  frozen_gross_inc_vat numeric(18,2) not null check (frozen_gross_inc_vat >= 0),
  accepted_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (run_worker_id,request_no),
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict
);
alter table private.bpay_next_net_projection
  add constraint bpay_next_projection_request_fk
  foreign key (request_command_id)
  references private.bpay_next_paye_net_request(command_id) on delete restrict;
alter table private.bpay_next_transfer
  add constraint bpay_next_transfer_build_command_fk
  foreign key (build_command_id)
  references private.bpay_next_command(id) on delete restrict;

-- One exact external receipt enters financial command order before any
-- balance changes. Posting progress is separate from the bank outcome.
create table private.bpay_next_outcome_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  outcome_id uuid not null unique references private.bpay_next_transfer_outcome(id) on delete restrict,
  transfer_id uuid not null,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  posted_member_count bigint not null default 0 check (posted_member_count>=0),
  posting_complete boolean not null default false,
  foreign key (transfer_id,run_worker_id)
    references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  unique (transfer_id)
);

create table private.bpay_next_return_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  outcome_id uuid not null unique references private.bpay_next_transfer_outcome(id) on delete restrict,
  transfer_id uuid not null unique,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  posting_complete boolean not null default false,
  foreign key (transfer_id,run_worker_id)
    references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict
);

-- One exact returned-cash obligation is reserved by its ordered worker.
create table private.bpay_next_reissue_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  return_cash_id uuid not null,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  transfer_id uuid unique,
  foreign key (return_cash_id,candidate_id)
    references private.bpay_next_return_cash(id,candidate_id) on delete restrict,
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  foreign key (transfer_id,run_worker_id)
    references private.bpay_next_transfer(id,run_worker_id) on delete restrict
);
create unique index bpay_next_reissue_pending_cash_once_idx
  on private.bpay_next_reissue_request(return_cash_id) where transfer_id is null;

-- Whole-Candidate cancellation progresses over captured frozen rows. The
-- intent never disables an earlier accepted builder; the ordered owner fences
-- the group before releasing exact holds, then retains all audit artifacts.
create table private.bpay_next_cancel_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  expected_line_count bigint not null check (expected_line_count>=0),
  released_line_count bigint not null default 0
    check (released_line_count between 0 and expected_line_count),
  cursor_line_no bigint check (cursor_line_no>0),
  status text not null check (status in ('REQUESTED','CANCELLING','CANCELLED','BLOCKED')),
  blocked_code text check (octet_length(blocked_code)<=256),
  started_at_utc timestamptz,
  finished_at_utc timestamptz,
  foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict
);
create unique index bpay_next_cancel_pending_worker_idx
  on private.bpay_next_cancel_request(run_worker_id) where status in ('REQUESTED','CANCELLING');
create index bpay_next_case_hold_worker_idx on private.bpay_next_case_hold(run_worker_id,id);

-- A publication is one accepted approval, not an instruction to rediscover
-- historical Timesheets. Its predecessor is the previous accepted financial
-- revision even if another approval has arrived before a worker runs.
create table private.bpay_next_publication (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  work_id uuid not null,
  candidate_id uuid not null,
  revision_id uuid not null,
  predecessor_revision_id uuid,
  revision_no bigint not null check (revision_no>0),
  status text not null default 'QUEUED'
    check (status in ('QUEUED','APPLYING','APPLIED')),
  phase text not null default 'NEW' check (phase in ('NEW','REMOVED','DONE')),
  cursor_key text,
  applied_at_utc timestamptz,
  unique (work_id,revision_id),
  unique (work_id,revision_no),
  foreign key (work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key (work_id,revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict,
  foreign key (work_id,predecessor_revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict,
  foreign key (command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  check ((status='APPLIED') = (phase='DONE' and applied_at_utc is not null))
);

-- The enroller advances strictly through accepted agency sequence numbers.
-- It never holds a worker or Source/Timesheet lock.
create table private.bpay_next_enrollment_clock (
  id smallint primary key check (id=1),
  last_enrolled_sequence bigint not null default 0 check (last_enrolled_sequence>=0)
);
insert into private.bpay_next_enrollment_clock(id) values (1);

create table private.bpay_next_job (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  command_id uuid not null,
  command_sequence bigint not null check (command_sequence > 0),
  module_epoch bigint not null check (module_epoch>0),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  job_kind text not null,
  status text not null check (status in ('READY','LEASED','BLOCKED','DONE','FAILED')),
  phase text not null,
  cursor_key text,
  applied_line_count bigint not null default 0 check (applied_line_count>=0),
  position_work_cursor uuid,
  position_component_cursor text,
  owner_epoch bigint not null check (owner_epoch > 0),
  lease_nonce uuid,
  lease_until_utc timestamptz,
  available_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  attempt_count integer not null default 0 check (attempt_count >= 0),
  last_error_code text,
  unique (command_id,candidate_id,job_kind),
  foreign key (command_id,command_sequence)
    references private.bpay_next_command(id,agency_sequence) on delete restrict,
  foreign key (command_id,candidate_id)
    references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  check ((status='LEASED') = (lease_nonce is not null and lease_until_utc is not null))
);

create index bpay_next_work_candidate_week_idx
  on private.bpay_next_work(candidate_id,week_ending_date,id);
create index bpay_next_work_pending_idx
  on private.bpay_next_work(candidate_id,id) where approval_state='PENDING';
create index bpay_next_work_unapplied_idx
  on private.bpay_next_work(candidate_id,id)
  where current_revision_id is distinct from applied_revision_id;
create index bpay_next_revision_work_idx
  on private.bpay_next_work_revision(work_id,revision_no,id);
create index bpay_next_line_revision_idx
  on private.bpay_next_approved_line(revision_id,line_no,id);
create index bpay_next_detail_line_idx
  on private.bpay_next_shift_detail(approved_line_id,detail_no,id);
create index bpay_next_rate_detail_line_idx
  on private.bpay_next_rate_detail(approved_line_id,bucket);
create index bpay_next_rate_schedule_revision_idx
  on private.bpay_next_rate_schedule(revision_id,rate_family,rate_code);
create index bpay_next_effect_work_history_idx
  on private.bpay_next_financial_effect(work_id,component_key,occurred_at_utc,id);
create index bpay_next_hold_active_key_idx
  on private.bpay_next_hold(work_id,component_key,id) where status='ACTIVE';
create index bpay_next_hold_nonreleased_work_idx
  on private.bpay_next_hold(work_id,id) where status<>'RELEASED';
create index bpay_next_case_due_idx
  on private.bpay_next_finance_case(candidate_id,due_date,order_key,id) where status='OPEN';
create index bpay_next_case_candidate_exists_idx
  on private.bpay_next_finance_case(candidate_id,id);
create index bpay_next_case_hold_active_idx
  on private.bpay_next_case_hold(case_id,id) where status='ACTIVE';
create index bpay_next_run_worker_page_idx
  on private.bpay_next_run_worker(run_id,candidate_id,id);
-- Final review/confirmation asks whether any selected worker is not READY;
-- it never counts the entire run in one final transaction.
create index bpay_next_run_worker_unready_idx
  on private.bpay_next_run_worker(run_id,id) where status<>'READY';
create index bpay_next_run_line_page_idx
  on private.bpay_next_run_line(run_worker_id,line_no,id);
create index bpay_next_run_work_page_idx
  on private.bpay_next_run_work(run_worker_id,work_id,id);
create index bpay_next_run_work_work_idx
  on private.bpay_next_run_work(work_id,id);
create index bpay_next_run_position_worker_page_idx
  on private.bpay_next_run_position(run_worker_id,work_id,component_key);
create index bpay_next_run_position_resolution_idx
  on private.bpay_next_run_position(run_worker_id,work_id,component_key)
  where needs_financial_resolution;
create index bpay_next_position_relevant_idx
  on private.bpay_next_position(work_id,component_key)
  where approved_source_ex_vat<>0 or realised_source_ex_vat<>0
    or held_source_ex_vat<>0 or realised_target_ex_vat<>0
    or realised_target_vat<>0 or realised_target_inc_vat<>0
     or held_target_ex_vat<>0 or held_target_vat<>0
     or held_target_inc_vat<>0;
-- Revision-keyed capture includes certified-zero current components without
-- walking dormant all-zero keys left by earlier revisions. The NEW position
-- phase stamps every approved component, even one whose amount is zero.
create index bpay_next_position_current_revision_idx
  on private.bpay_next_position(work_id,applied_revision_id,component_key);
create index bpay_next_transfer_worker_idx
  on private.bpay_next_transfer(run_worker_id,transfer_no,id);
create index bpay_next_transfer_member_page_idx
  on private.bpay_next_transfer_member(transfer_id,member_no);
create index bpay_next_transfer_outcome_idx
  on private.bpay_next_transfer_outcome(transfer_id,occurred_at_utc,id);
create index bpay_next_job_ready_idx
  on private.bpay_next_job(status,available_at_utc,command_sequence,id)
  where status in ('READY','BLOCKED');
create index bpay_next_job_expired_lease_idx
  on private.bpay_next_job(lease_until_utc,id) where status='LEASED';
create index bpay_next_job_worker_order_idx
  on private.bpay_next_job(candidate_id,command_sequence,id)
  where status <> 'DONE';
create index bpay_next_prepare_job_open_command_idx
  on private.bpay_next_job(command_id,id)
  where job_kind='PREPARE' and status<>'DONE';
create index bpay_next_publication_work_order_idx
  on private.bpay_next_publication(work_id,revision_no);

-- These records are owner-only. Later service RPCs expose validated commands
-- and cursor pages; browser roles and service_role get no direct table writes.
alter table private.bpay_next_module_control owner to postgres;
alter table private.bpay_next_valuation_policy owner to postgres;
alter table private.bpay_next_valuation_policy_window owner to postgres;
alter table private.bpay_next_valuation_policy_control owner to postgres;
alter table private.bpay_next_work owner to postgres;
alter table private.bpay_next_work_revision owner to postgres;
alter table private.bpay_next_approved_line owner to postgres;
alter table private.bpay_next_shift_detail owner to postgres;
alter table private.bpay_next_rate_detail owner to postgres;
alter table private.bpay_next_rate_schedule owner to postgres;
alter table private.bpay_next_break_detail owner to postgres;
alter table private.bpay_next_source_chosen_detail owner to postgres;
alter table private.bpay_next_position owner to postgres;
alter table private.bpay_next_finance_case owner to postgres;
alter table private.bpay_next_case_event owner to postgres;
alter table private.bpay_next_financial_effect owner to postgres;
alter table private.bpay_next_pay_run owner to postgres;
alter table private.bpay_next_selection_page owner to postgres;
alter table private.bpay_next_run_selection owner to postgres;
alter table private.bpay_next_selection_candidate owner to postgres;
alter table private.bpay_next_run_worker owner to postgres;
alter table private.bpay_next_cancel_request owner to postgres;
alter table private.bpay_next_run_work owner to postgres;
alter table private.bpay_next_run_position owner to postgres;
alter table private.bpay_next_run_line owner to postgres;
alter table private.bpay_next_hold owner to postgres;
alter table private.bpay_next_case_hold owner to postgres;
alter table private.bpay_next_net_projection owner to postgres;
alter table private.bpay_next_transfer owner to postgres;
alter table private.bpay_next_return_cash owner to postgres;
alter table private.bpay_next_transfer_member owner to postgres;
alter table private.bpay_next_transfer_outcome owner to postgres;
alter table private.bpay_next_csv_instruction owner to postgres;
alter table private.bpay_next_worker_control owner to postgres;
alter table private.bpay_next_command_clock owner to postgres;
alter table private.bpay_next_command owner to postgres;
alter table private.bpay_next_run_command owner to postgres;
alter table private.bpay_next_command_member owner to postgres;
alter table private.bpay_next_paye_net_request owner to postgres;
alter table private.bpay_next_outcome_request owner to postgres;
alter table private.bpay_next_return_request owner to postgres;
alter table private.bpay_next_reissue_request owner to postgres;
alter table private.bpay_next_publication owner to postgres;
alter table private.bpay_next_enrollment_clock owner to postgres;
alter table private.bpay_next_job owner to postgres;

alter function private.bpay_next_current_valuation_policy_id_v1() owner to postgres;
alter function private.bpay_next_finance_policy_change_v1() owner to postgres;
revoke all on function private.bpay_next_current_valuation_policy_id_v1()
  from public, anon, authenticated, service_role;
revoke all on function private.bpay_next_finance_policy_change_v1()
  from public, anon, authenticated, service_role;

revoke all on private.bpay_next_module_control,
  private.bpay_next_valuation_policy, private.bpay_next_valuation_policy_window,
  private.bpay_next_valuation_policy_control,
  private.bpay_next_work, private.bpay_next_work_revision,
  private.bpay_next_approved_line, private.bpay_next_shift_detail,
  private.bpay_next_rate_detail,
  private.bpay_next_rate_schedule,
  private.bpay_next_break_detail,
  private.bpay_next_source_chosen_detail,
  private.bpay_next_position, private.bpay_next_finance_case,
  private.bpay_next_case_event, private.bpay_next_financial_effect,
  private.bpay_next_pay_run, private.bpay_next_selection_page,
  private.bpay_next_run_selection, private.bpay_next_run_worker,
  private.bpay_next_selection_candidate,
  private.bpay_next_run_work,
  private.bpay_next_run_position,
  private.bpay_next_run_line, private.bpay_next_hold,
  private.bpay_next_case_hold, private.bpay_next_net_projection,
  private.bpay_next_cancel_request,
  private.bpay_next_transfer, private.bpay_next_return_cash,
  private.bpay_next_transfer_member, private.bpay_next_transfer_outcome,
  private.bpay_next_csv_instruction,
  private.bpay_next_worker_control, private.bpay_next_command_clock,
  private.bpay_next_command, private.bpay_next_run_command,
  private.bpay_next_command_member, private.bpay_next_paye_net_request,
  private.bpay_next_outcome_request,
  private.bpay_next_return_request,
  private.bpay_next_reissue_request,
  private.bpay_next_publication,
  private.bpay_next_enrollment_clock, private.bpay_next_job
  from public, anon, authenticated, service_role;

commit;
