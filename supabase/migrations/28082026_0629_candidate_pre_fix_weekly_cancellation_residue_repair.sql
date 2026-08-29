-- Before the atomic weekly-withdrawal reset was installed, a Candidate could
-- cancel a weekly workflow after its Timesheet had been created.  The workflow
-- became CANCELLED but the unprotected Timesheet and contract-week pointer
-- remained current, leaving the Candidate unable to correct and resubmit.
--
-- Repair only that exact historical residue through the current protected
-- reset helper.  No evidence is deleted: the old Timesheet and evidence become
-- revoked/superseded history and the contract week returns to OPEN.  Current
-- cancellations already invoke the same helper inside the atomic transition.

\set ON_ERROR_STOP on

begin;

do $migration$
declare
  v_residue_count integer:=0;
  v_repaired_count integer:=0;
  v_candidate record;
  v_route_authority jsonb;
  v_reset jsonb;
  v_reason text;
begin
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

  if v_residue_count>100 then
    raise exception 'CANDIDATE_CANCELLATION_RESIDUE_SCOPE_UNEXPECTED'
      using errcode='55000',detail='count='||v_residue_count::text;
  end if;
  if v_residue_count>0
     and (
       to_regprocedure('private._candidate_weekly_withdrawal_reset_v1(uuid,text,timestamptz)') is null
       or to_regprocedure('private._candidate_route_family_v1(uuid,uuid)') is null
     ) then
    raise exception 'CANDIDATE_CANCELLATION_REPAIR_AUTHORITY_MISSING'
      using errcode='55000';
  end if;

  for v_candidate in
    select workflow_row.id workflow_id,
      workflow_row.contract_week_id,
      week_row.timesheet_id,
      nullif(btrim(coalesce(
        workflow_row.last_mutation_response_json->>'cancellation_reason',''
      )), '') cancellation_reason
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
      and timesheet_row.archived_at_utc is null
    order by workflow_row.updated_at_utc,workflow_row.id
    for update of workflow_row,week_row,timesheet_row
  loop
    execute 'select private._candidate_route_family_v1($1,$2)'
      into v_route_authority
      using v_candidate.timesheet_id,v_candidate.contract_week_id;
    if coalesce(v_route_authority->>'route_family','')='IMPORT_AUTHORITATIVE' then
      raise exception 'CANDIDATE_RECORD_VIEW_ONLY' using errcode='55000';
    end if;

    v_reason:=coalesce(
      v_candidate.cancellation_reason,
      'Recovered a cancellation completed before the atomic withdrawal reset was installed.'
    );
    execute 'select private._candidate_weekly_withdrawal_reset_v1($1,$2,$3)'
      into v_reset
      using v_candidate.workflow_id,v_reason,clock_timestamp();
    if not coalesce((v_reset->>'reset')::boolean,false)
       or nullif(v_reset->>'current_timesheet_id','') is not null then
      raise exception 'CANDIDATE_CANCELLATION_RESIDUE_REPAIR_FAILED'
        using errcode='55000';
    end if;
    v_repaired_count:=v_repaired_count+1;
  end loop;

  if v_repaired_count<>v_residue_count then
    raise exception 'CANDIDATE_CANCELLATION_RESIDUE_REPAIR_INCOMPLETE'
      using errcode='55000',detail='expected='||v_residue_count::text
        ||'; repaired='||v_repaired_count::text;
  end if;
end;
$migration$;

commit;
