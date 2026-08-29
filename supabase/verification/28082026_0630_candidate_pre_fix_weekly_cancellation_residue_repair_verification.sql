\set ON_ERROR_STOP on

do $verification$
declare
  v_residue_count integer:=0;
begin
  if to_regprocedure(
    'private._candidate_weekly_withdrawal_reset_v1(uuid,text,timestamptz)'
  ) is null then
    raise exception 'CANDIDATE_CANCELLATION_REPAIR_AUTHORITY_MISSING'
      using errcode='55000';
  end if;

  select count(*)::integer into v_residue_count
  from public.candidate_submission_workflows workflow_row
  join public.contract_weeks week_row
    on week_row.id=workflow_row.contract_week_id
   and week_row.timesheet_id=coalesce(
     workflow_row.target_timesheet_id,workflow_row.anchor_timesheet_id
   )
  join public.timesheets timesheet_row
    on timesheet_row.timesheet_id=week_row.timesheet_id
  where workflow_row.state='CANCELLED'
    and workflow_row.scope='WEEKLY'
    and workflow_row.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
    and timesheet_row.is_current=true
    and timesheet_row.archived_at_utc is null;

  if v_residue_count<>0 then
    raise exception 'CANDIDATE_CANCELLATION_RESIDUE_REMAINS'
      using errcode='55000',detail='count='||v_residue_count::text;
  end if;
end;
$verification$;
