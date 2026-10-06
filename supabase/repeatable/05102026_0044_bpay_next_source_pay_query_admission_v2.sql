-- Query admission is operational transaction contention, never a money hold.
-- True outer fresh owners call this before their first business/module lock.
-- Immutable receipt replay does not acquire fresh admission. No Source reader
-- or writer is installed/activated by this definition.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_pay_query_admit_v2()
returns void language plpgsql volatile security definer
set search_path=pg_catalog
as $function$
begin
  if not pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
    'CLOUDTMS:BPAY_NEXT:SOURCE_PAY_QUERY_ADMISSION:V2',0)) then
    raise exception using errcode='55P03',
      message='WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY';
  end if;
  return;
end
$function$;

-- Only the fresh1600 INSERT is qualified against live preparation/currentness.
-- Consumers and exact receipt replay read the immutable row by run_work PK;
-- they never run this trigger or reinterpret a pin after later Source changes.
create or replace function private.bpay_next_run_work_query_pin_guard_v2()
returns trigger language plpgsql volatile security definer
set search_path=pg_catalog,private,public
as $function$
declare v_origin record;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='55000',message='BPAY_NEXT_QUERY_PIN_IMMUTABLE';
  end if;
  -- These are exact indexed identities. The pin owner already holds the
  -- established module/run/Candidate/job/WORK/revision locks. This trigger
  -- acquires no admission/business lock and calls no live Source reader.
  select r.physical_timesheet_id,r.physical_timesheet_version,
      r.week_ending_date,w.booking_id,w.contract_id,c.client_id
    into v_origin
    from private.bpay_next_run_work captured
    join private.bpay_next_run_worker worker
      on worker.id=captured.run_worker_id and worker.candidate_id=captured.candidate_id
    join private.bpay_next_pay_run run on run.id=worker.run_id
    join private.bpay_next_run_command rc on rc.run_id=run.id
    join private.bpay_next_job job on job.id=new.prepare_job_id and job.command_id=rc.command_id
      and job.candidate_id=worker.candidate_id
    join private.bpay_next_command command on command.id=job.command_id
      and command.agency_sequence=job.command_sequence
    join private.bpay_next_worker_control owner on owner.candidate_id=worker.candidate_id
    join private.bpay_next_module_control module on module.id=1
    join private.bpay_next_run_selection selected on selected.run_id=run.id
      and selected.work_id=captured.work_id and selected.candidate_id=worker.candidate_id
    join private.bpay_next_work_choice choice on choice.run_id=run.id and choice.work_id=captured.work_id
    join private.bpay_next_work w on w.id=captured.work_id and w.candidate_id=worker.candidate_id
    join private.bpay_next_work_revision r on r.work_id=w.id and r.id=captured.captured_revision_id
    join public.contracts c on c.id=w.contract_id and c.candidate_id=worker.candidate_id
    join public.timesheets physical_root on physical_root.timesheet_id=r.physical_timesheet_id
      and physical_root.contract_id=w.contract_id and physical_root.booking_id=w.booking_id
      and physical_root.version=r.physical_timesheet_version
      and physical_root.week_ending_date=r.week_ending_date
    where captured.id=new.run_work_id and captured.run_worker_id=new.run_worker_id
      and captured.candidate_id=new.candidate_id and captured.work_id=new.work_id
      and captured.captured_revision_id=new.captured_revision_id
      and module.active_owner='NEXT' and module.owner_epoch=new.module_epoch
      and job.module_epoch=new.module_epoch and command.module_epoch=new.module_epoch
      and command.command_kind='PREPARE' and command.status='SEALED'
      and job.job_kind='PREPARE' and job.status='LEASED' and job.phase in ('NEW','PINNING')
      and job.lease_nonce is not null and job.lease_until_utc>pg_catalog.clock_timestamp()
      and job.owner_epoch=owner.active_owner_epoch
      and run.status='PREPARING' and run.selection_state='SEALED' and worker.status='PREPARING'
      and choice.selection_state='SEALED' and choice.expected_revision_id=r.id
      and w.work_kind='SOURCE' and w.approval_state='APPROVED'
      and w.current_revision_id=r.id and w.applied_revision_id=r.id
      and r.source_kind in ('SOURCE','PROTECTED') and r.source_event_id is not null
      and r.approved_at_utc is not null and r.sealed_at_utc is not null
      and r.week_ending_date=w.week_ending_date and c.client_id is not null;
  if not found then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_PIN_OWNER_MISMATCH';
  end if;
  if new.scope is not null and (
      (new.scope->>'root_timesheet_id')::uuid is distinct from v_origin.physical_timesheet_id
      or (new.scope->>'root_version')::bigint is distinct from v_origin.physical_timesheet_version
      -- QueryV4 preserves the stored raw root/WORK booking in Scope. Trimmed
      -- lookup/collision checks belong to the Source owner, never this identity.
      or new.scope->>'family_booking_id' is distinct from v_origin.booking_id
      or (new.scope->>'candidate_id')::uuid is distinct from new.candidate_id
      or (new.scope->>'contract_id')::uuid is distinct from v_origin.contract_id
      or (new.scope->>'client_id')::uuid is distinct from v_origin.client_id
      or (new.scope->>'week_ending_date')::date is distinct from v_origin.week_ending_date) then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_PIN_SCOPE_MISMATCH';
  end if;
  -- Server time belongs to the actual successful INSERT, not caller evidence
  -- or an earlier transaction-start time before a downstream lock wait.
  new.captured_at_utc:=pg_catalog.clock_timestamp();
  return new;
end
$function$;

drop trigger if exists bpay_next_run_work_query_pin_guard_v2 on private.bpay_next_run_work_query_pin_v2;
create trigger bpay_next_run_work_query_pin_guard_v2 before insert or update or delete
  on private.bpay_next_run_work_query_pin_v2 for each row
  execute function private.bpay_next_run_work_query_pin_guard_v2();
drop trigger if exists bpay_next_run_work_query_pin_no_truncate_v2 on private.bpay_next_run_work_query_pin_v2;
create trigger bpay_next_run_work_query_pin_no_truncate_v2 before truncate
  on private.bpay_next_run_work_query_pin_v2 for each statement
  execute function private.bpay_next_run_work_query_pin_guard_v2();

alter function private.weekly_source_pay_query_admit_v2() owner to postgres;
alter function private.bpay_next_run_work_query_pin_guard_v2() owner to postgres;
revoke all on function private.weekly_source_pay_query_admit_v2()
  from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_run_work_query_pin_guard_v2()
  from public,anon,authenticated,service_role;

commit;
