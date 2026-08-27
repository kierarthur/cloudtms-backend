do $verification$
declare
  v_definition text;
begin
  select pg_get_functiondef(
    'private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)'::regprocedure
  ) into v_definition;
  if position("'ELECTRONIC'" in v_definition)=0
     or position("'MANUAL'" in v_definition)>0 then
    raise exception 'Candidate electronic rejection rotation body is not current';
  end if;

  if has_function_privilege(
       'public','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     )
     or has_function_privilege(
       'anon','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     )
     or has_function_privilege(
       'authenticated','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     )
     or has_function_privilege(
       'service_role','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     ) then
    raise exception 'Candidate electronic rejection rotation helper is externally executable';
  end if;

  if exists (
    select 1
    from public.timesheets current_timesheet
    join public.contract_weeks week_row
      on week_row.timesheet_id=current_timesheet.timesheet_id
    join public.audit_events rotation_audit
      on rotation_audit.object_type='timesheet'
     and rotation_audit.object_id_text=current_timesheet.timesheet_id::text
     and rotation_audit.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'
    join public.timesheets_financials financials
      on financials.timesheet_id=current_timesheet.timesheet_id
     and financials.is_current=true
    join lateral (
      select workflow.id
      from public.candidate_submission_workflows workflow
      where workflow.state='REJECTED'
        and workflow.contract_week_id=week_row.id
        and not exists (
          select 1
          from public.candidate_submission_workflows replacement
          where replacement.replacement_of_workflow_id=workflow.id
        )
      order by workflow.updated_at_utc desc,workflow.id
      limit 1
    ) rejected_workflow on true
    where current_timesheet.is_current=true
      and current_timesheet.submission_mode='MANUAL'
      and current_timesheet.status='RECEIVED'
      and current_timesheet.archived_at_utc is null
      and current_timesheet.authorised_at_server is null
      and week_row.status='OPEN'
      and week_row.day_entries_json='[]'::jsonb
      and week_row.totals_json='{}'::jsonb
      and financials.processing_status in ('UNPROCESSED','PENDING_AUTH')
      and financials.authorised_at_utc is null
      and financials.paid_at_utc is null
      and financials.locked_by_invoice_id is null
      and coalesce(financials.total_hours,0)=0
      and coalesce(financials.total_pay_ex_vat,0)=0
      and coalesce(financials.total_charge_ex_vat,0)=0
      and coalesce(financials.expenses_pay_ex_vat,0)=0
      and coalesce(financials.expenses_charge_ex_vat,0)=0
      and coalesce(financials.mileage_units,0)=0
  ) then
    raise exception 'Unrepaired electronic rejection replacement remains installed';
  end if;

  if exists (
    select 1
    from public.timesheets current_timesheet
    join public.contract_weeks week_row
      on week_row.timesheet_id=current_timesheet.timesheet_id
    join public.timesheets_financials financials
      on financials.timesheet_id=current_timesheet.timesheet_id
     and financials.is_current=true
    join public.audit_events route_repair
      on route_repair.object_type='timesheet'
     and route_repair.object_id_text=current_timesheet.timesheet_id::text
     and route_repair.action='CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_REPAIRED'
    where current_timesheet.is_current=true
      and current_timesheet.status='RECEIVED'
      and current_timesheet.submission_mode='ELECTRONIC'
      and current_timesheet.archived_at_utc is null
      and current_timesheet.authorised_at_server is null
      and week_row.status='OPEN'
      and week_row.submission_mode_snapshot='ELECTRONIC'
      and week_row.day_entries_json='[]'::jsonb
      and week_row.totals_json='{}'::jsonb
      and financials.processing_status='PENDING_AUTH'
      and financials.authorised_at_utc is null
      and financials.paid_at_utc is null
      and financials.locked_by_invoice_id is null
      and coalesce(financials.total_hours,0)=0
      and coalesce(financials.total_pay_ex_vat,0)=0
      and coalesce(financials.total_charge_ex_vat,0)=0
      and coalesce(financials.expenses_pay_ex_vat,0)=0
      and coalesce(financials.expenses_charge_ex_vat,0)=0
      and coalesce(financials.mileage_units,0)=0
  ) then
    raise exception 'Repaired electronic rejection replacement remains lifecycle-locked';
  end if;
end;
$verification$;
