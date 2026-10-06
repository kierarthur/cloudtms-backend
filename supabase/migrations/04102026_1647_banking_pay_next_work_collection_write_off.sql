-- Typed, ordered forgiveness of ONE automatic ordinary WORK liability.
-- No history/backfill, cash, legacy delegate, schedule or protected-hold release.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_write_off_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  candidate_id uuid not null,
  case_id uuid not null,
  case_component_id uuid not null,
  work_collection_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  expected_component_revision bigint not null check (expected_component_revision>0),
  basis_case_revision bigint not null check (basis_case_revision>0),
  basis_principal private.bpay_next_penny_amount not null check (basis_principal>=0),
  basis_recovered private.bpay_next_penny_amount not null check (basis_recovered>=0),
  basis_written_off private.bpay_next_penny_amount not null check (basis_written_off>=0),
  basis_protected private.bpay_next_penny_amount not null check (basis_protected>=0),
  requested_scope text not null check (requested_scope in ('AMOUNT','ALL')),
  input_amount text,
  requested_amount private.bpay_next_penny_amount,
  reason text not null check (pg_catalog.octet_length(reason) between 1 and 1024 and pg_catalog.btrim(reason)<>''),
  accepted_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check (pg_catalog.isfinite(accepted_at_utc)),
  status text not null default 'REQUESTED' check (status in ('REQUESTED','WRITTEN_OFF','BLOCKED','REVIEW')),
  applied_amount private.bpay_next_penny_amount check (applied_amount>=0),
  remaining_amount private.bpay_next_penny_amount check (remaining_amount>=0),
  protected_amount private.bpay_next_penny_amount check (protected_amount>=0),
  event_id uuid unique,
  issue_code text check (issue_code in ('BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY','BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE',
    'BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED','BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND','BPAY_NEXT_WRITE_OFF_NO_BALANCE')),
  completed_at_utc timestamptz check (pg_catalog.isfinite(completed_at_utc)),
  unique(command_id,case_id,case_component_id,candidate_id),
  foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  foreign key(work_collection_id,candidate_id) references private.bpay_next_work_collection(id,candidate_id) on delete restrict,
  foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
  foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
  foreign key(event_id,case_id) references private.bpay_next_case_event(id,case_id) on delete restrict,
  check (work_collection_id=case_id and work_collection_id=case_component_id),
  check ((requested_scope='ALL' and input_amount is null and requested_amount is null)
    or (requested_scope='AMOUNT' and input_amount is not null and requested_amount is not null and requested_amount>0
      and pg_catalog.octet_length(input_amount)<=1024 and input_amount~'^(0|[1-9][0-9]*)([.][0-9]{1,2})?$')),
  check ((status='REQUESTED' and applied_amount is null and remaining_amount is null and protected_amount is null
      and event_id is null and issue_code is null and completed_at_utc is null)
    or (status='WRITTEN_OFF' and applied_amount is not null and applied_amount>0 and remaining_amount is not null and protected_amount is not null
      and event_id is not null and event_id=command_id and issue_code is null and completed_at_utc is not null
      and protected_amount<=remaining_amount)
    or (status in ('BLOCKED','REVIEW') and applied_amount is not null and applied_amount=0 and remaining_amount is not null and protected_amount is not null
      and protected_amount<=remaining_amount and event_id is null and issue_code is not null and completed_at_utc is not null))
);
create index bpay_next_write_off_request_candidate_idx on private.bpay_next_write_off_request(candidate_id,command_id);
-- Counter guards point at ONE actual leased W owner, not a case/request log.
create unique index bpay_next_write_off_active_candidate_once_idx on private.bpay_next_job(candidate_id)
  where job_kind='CASE_WRITE_OFF' and status='LEASED';
alter table private.bpay_next_write_off_request enable row level security;
alter table private.bpay_next_write_off_request owner to postgres;
revoke all on private.bpay_next_write_off_request from public,anon,authenticated,service_role;

commit;
