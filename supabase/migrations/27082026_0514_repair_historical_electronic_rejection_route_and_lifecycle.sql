-- Unlock the historical electronic rejection replacement whose electronic
-- route is already preserved by the contract-week snapshot.  The unsigned
-- current row intentionally remains MANUAL until finalisation writes both
-- signatures and changes it to ELECTRONIC atomically.  Eligibility is bounded
-- by the exact rotation audit, an unresolved rejected manager workflow, an
-- open empty week, and zero/protected-free financials.

do $migration$
declare
  v_row record;
begin
  for v_row in
    select
      current_timesheet.timesheet_id,
      financials.id as financials_id,
      financials.processing_status as prior_processing_status,
      week_row.id as contract_week_id,
      rejected_workflow.id as rejected_workflow_id
    from public.timesheets current_timesheet
    join public.contract_weeks week_row
      on week_row.timesheet_id=current_timesheet.timesheet_id
    join public.timesheets_financials financials
      on financials.timesheet_id=current_timesheet.timesheet_id
     and financials.is_current=true
    join public.audit_events rotation_audit
      on rotation_audit.object_type='timesheet'
     and rotation_audit.object_id_text=current_timesheet.timesheet_id::text
     and rotation_audit.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'
    join lateral (
      select workflow.id
      from public.candidate_submission_workflows workflow
      where workflow.state='REJECTED'
        and workflow.contract_week_id=week_row.id
        and workflow.route in ('PHONE','EMAIL')
        and not exists (
          select 1
          from public.candidate_submission_workflows replacement
          where replacement.replacement_of_workflow_id=workflow.id
        )
      order by workflow.updated_at_utc desc,workflow.id
      limit 1
    ) rejected_workflow on true
    where current_timesheet.is_current=true
      and current_timesheet.status='RECEIVED'
      and current_timesheet.submission_mode='MANUAL'
      and current_timesheet.archived_at_utc is null
      and current_timesheet.authorised_at_server is null
      and current_timesheet.qr_token is null
      and current_timesheet.qr_status is null
      and current_timesheet.qr_r2_key is null
      and current_timesheet.worked_start_iso is null
      and current_timesheet.worked_end_iso is null
      and coalesce(current_timesheet.actual_schedule_json,'[]'::jsonb) in ('[]'::jsonb,'{}'::jsonb)
      and week_row.status='OPEN'
      and week_row.submission_mode_snapshot='ELECTRONIC'
      and coalesce(week_row.day_entries_json,'[]'::jsonb)='[]'::jsonb
      and coalesce(week_row.totals_json,'{}'::jsonb)='{}'::jsonb
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
    order by current_timesheet.timesheet_id,rejected_workflow.id
    for update of current_timesheet,week_row,financials
  loop
    update public.timesheets_financials
    set processing_status='UNPROCESSED',updated_at=clock_timestamp()
    where id=v_row.financials_id
      and timesheet_id=v_row.timesheet_id
      and is_current=true
      and processing_status in ('UNPROCESSED','PENDING_AUTH')
      and authorised_at_utc is null
      and paid_at_utc is null
      and locked_by_invoice_id is null;
    if not found then
      raise exception 'CANDIDATE_ELECTRONIC_REJECTION_LIFECYCLE_REPAIR_CONFLICT'
        using errcode='40001';
    end if;

    insert into public.audit_events(
      actor_user_id,object_type,object_id_text,action,before_json,after_json,
      reason,correlation_id,ts_utc
    ) values (
      null,'timesheet',v_row.timesheet_id::text,
      'CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_REPAIRED',
      jsonb_build_object(
        'stored_submission_mode','MANUAL',
        'effective_submission_mode','ELECTRONIC',
        'processing_status',v_row.prior_processing_status,
        'contract_week_id',v_row.contract_week_id,
        'rejected_workflow_id',v_row.rejected_workflow_id
      ),
      jsonb_build_object(
        'stored_submission_mode','MANUAL',
        'effective_submission_mode','ELECTRONIC',
        'processing_status','UNPROCESSED',
        'contract_week_id',v_row.contract_week_id,
        'rejected_workflow_id',v_row.rejected_workflow_id
      ),
      'Unlock historical electronic rejection replacement while preserving the electronic route snapshot',
      'ELECTRONIC-REJECTION-ROUTE-LIFECYCLE-REPAIR:'||v_row.timesheet_id::text,
      clock_timestamp()
    );
  end loop;
end;
$migration$;
