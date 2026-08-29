-- Manager-refused Candidate claims use the same replacement-lineage guard as
-- server-rejected claims without replaying the older shared Candidate read
-- closure.

create or replace function private._candidate_rejection_replaced_v1(
  p_rejected_workflow_id uuid
)
returns boolean
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_rejected public.candidate_submission_workflows%rowtype;
begin
  if p_rejected_workflow_id is null then
    return false;
  end if;

  select workflow.* into v_rejected
  from public.candidate_submission_workflows workflow
  where workflow.id=p_rejected_workflow_id;

  if not found or v_rejected.state not in ('REJECTED','REFUSED') then
    return false;
  end if;

  -- Direct replacement lineage is durable historical truth.  A successor
  -- remains the replacement even if it is later cancelled, expires or is
  -- superseded; the original rejected or manager-refused workflow must never
  -- advertise a second impossible direct resubmission.
  if exists(
    select 1
    from public.candidate_submission_workflows direct_replacement
    where direct_replacement.replacement_of_workflow_id=v_rejected.id
  ) then
    return true;
  end if;

  return exists(
    select 1
    from public.candidate_submission_workflows later
    where later.candidate_id=v_rejected.candidate_id
      and later.contract_id is not distinct from v_rejected.contract_id
      and later.id<>v_rejected.id
      and later.state not in ('CANCELLED','EXPIRED','SUPERSEDED')
      and later.created_at_utc>=v_rejected.updated_at_utc
      and (
        (
          (
            v_rejected.workflow_kind='CONTRACT_EXPENSE'
            or v_rejected.rejection_scope='COMPLETE_EXPENSE_CLAIM'
          )
          and later.week_ending_date is not distinct from v_rejected.week_ending_date
          and later.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
        )
        or (
          v_rejected.workflow_kind='CONTRACT_COMBINED'
          and later.workflow_kind='CONTRACT_COMBINED'
          and later.contract_week_id is not distinct from v_rejected.contract_week_id
        )
        or (
          v_rejected.workflow_kind='CONTRACT_HOURS'
          and later.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
          and later.contract_week_id is not distinct from v_rejected.contract_week_id
        )
        or (
          v_rejected.workflow_kind='DAILY'
          and later.workflow_kind='DAILY'
          and later.work_date is not distinct from v_rejected.work_date
          and exists(
            select 1
            from public.timesheets rejected_timesheet
            join public.timesheets later_timesheet
              on nullif(btrim(coalesce(later_timesheet.booking_id,'')),'')
                =nullif(btrim(coalesce(rejected_timesheet.booking_id,'')),'')
            where rejected_timesheet.timesheet_id=coalesce(
                v_rejected.target_timesheet_id,v_rejected.anchor_timesheet_id
              )
              and later_timesheet.timesheet_id=coalesce(
                later.target_timesheet_id,later.anchor_timesheet_id
              )
              and nullif(btrim(coalesce(rejected_timesheet.booking_id,'')),'') is not null
          )
        )
      )
  );
end;
$function$;

revoke all on function private._candidate_rejection_replaced_v1(uuid)
  from public,anon,authenticated;
