\set ON_ERROR_STOP on

begin;

-- Record the accepted candidate-hours generation in the same transaction as
-- the workflow transition.  A retry of that transition cannot create a
-- second Office audit line for the same generation.
create or replace function private.candidate_hours_received_audit_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_timesheet_id uuid;
  v_correlation_id text;
begin
  if new.workflow_kind <> 'CONTRACT_HOURS'
    or new.state <> 'WORKER_SUBMITTED'
    or new.candidate_signed_at_utc is null
    or new.worker_submitted_at_utc is null
    or (old.generation = new.generation
      and old.state is not distinct from new.state
      and old.candidate_signed_at_utc is not distinct from new.candidate_signed_at_utc
      and old.worker_submitted_at_utc is not distinct from new.worker_submitted_at_utc)
  then
    return new;
  end if;

  v_timesheet_id:=coalesce(new.target_timesheet_id,new.anchor_timesheet_id);
  if v_timesheet_id is null then
    return new;
  end if;
  v_correlation_id:='candidate-hours-received:'||new.id::text||':'||new.generation::text;
  if not exists (
    select 1 from public.audit_events audit
    where audit.correlation_id=v_correlation_id
      and audit.object_type='timesheets'
      and audit.object_id_text=v_timesheet_id::text
      and audit.action='CANDIDATE_HOURS_RECEIVED'
  ) then
    insert into public.audit_events(
      ts_utc,actor_display,actor_role_at_time,object_type,object_id_text,
      action,after_json,reason,correlation_id
    ) values (
      new.worker_submitted_at_utc,'Candidate','candidate','timesheets',v_timesheet_id::text,
      'CANDIDATE_HOURS_RECEIVED',
      pg_catalog.jsonb_build_object('workflow_id',new.id,'generation',new.generation,
        'candidate_signed_at_utc',new.candidate_signed_at_utc,
        'workflow_kind',new.workflow_kind),
      'Candidate hours submitted',v_correlation_id
    );
  end if;
  return new;
end;
$function$;

alter function private.candidate_hours_received_audit_v1() owner to postgres;
revoke all on function private.candidate_hours_received_audit_v1()
  from public,anon,authenticated,service_role;

drop trigger if exists trg_candidate_hours_received_audit_v1
  on public.candidate_submission_workflows;
create trigger trg_candidate_hours_received_audit_v1
after update of generation,state,worker_submitted_at_utc,candidate_signed_at_utc
on public.candidate_submission_workflows
for each row execute function private.candidate_hours_received_audit_v1();

commit;
