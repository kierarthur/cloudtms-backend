-- WB19 small control receipts and genuinely recorded causal failure metadata.
-- Existing jobs have no proved first-error clock: nullable, never backfilled.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_job
  add column first_recorded_error_code text check (pg_catalog.octet_length(first_recorded_error_code)<=256),
  add column first_recorded_error_at_utc timestamptz,
  add column last_error_at_utc timestamptz;
alter table private.bpay_next_job add column failed_lease_nonce uuid;

create table private.bpay_next_workbench_command_receipt (
  request_id uuid primary key,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  work_id uuid not null references private.bpay_next_work(id) on delete restrict,
  -- Native terminal-job cleanup may retire a job. Its immutable request/result
  -- retains the original ID without preventing PF04's temporary-row cleanup.
  job_id uuid not null,
  module_epoch bigint not null check(module_epoch>0),
  request_body jsonb not null check(jsonb_typeof(request_body)='object' and pg_catalog.octet_length(request_body::text)<=4096),
  result jsonb not null check(jsonb_typeof(result)='object' and pg_catalog.octet_length(result::text)<=8192),
  captured_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
revoke all on private.bpay_next_workbench_command_receipt from public,anon,authenticated,service_role;
alter table private.bpay_next_workbench_command_receipt enable row level security;
alter table private.bpay_next_workbench_command_receipt force row level security;
create policy service_owner on private.bpay_next_workbench_command_receipt
  to CURRENT_USER using(true) with check(true);
create function private.bpay_next_workbench_receipt_immutable_v1()
returns trigger language plpgsql set search_path=pg_catalog as $function$
begin
  raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_RECEIPT_IMMUTABLE';
end
$function$;
revoke all on function private.bpay_next_workbench_receipt_immutable_v1() from public,anon,authenticated,service_role;
create trigger bpay_next_workbench_receipt_immutable
before update or delete on private.bpay_next_workbench_command_receipt
for each row execute function private.bpay_next_workbench_receipt_immutable_v1();
create trigger bpay_next_workbench_receipt_no_truncate
before truncate on private.bpay_next_workbench_command_receipt
for each statement execute function private.bpay_next_workbench_receipt_immutable_v1();
create index bpay_next_position_job_pending_seek_idx
  on private.bpay_next_job(module_epoch,command_sequence,id)
  where job_kind='POSITION_APPLY' and status<>'DONE';
create index bpay_next_position_job_candidate_seek_idx
  on private.bpay_next_job(candidate_id,module_epoch,command_sequence,id)
  where job_kind='POSITION_APPLY' and status<>'DONE';
create index bpay_next_position_job_pending_age_idx
  on private.bpay_next_job(module_epoch,created_at_utc nulls first,id)
  where job_kind='POSITION_APPLY' and status<>'DONE';

commit;
