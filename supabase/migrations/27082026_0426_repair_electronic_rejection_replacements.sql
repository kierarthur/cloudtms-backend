-- Repair only the empty MANUAL replacements created by the historical
-- electronic Office-rejection rotation defect.  Every candidate is proven by
-- its exact rejection audit lineage, immutable electronic predecessor, empty
-- current financial state, open contract week, and unresolved rejected
-- workflow.  No worked, authorised, paid, invoiced, QR, or already-replaced
-- record is eligible.

do $migration$
declare
  v_row record;
begin
  for v_row in
    select
      current_timesheet.timesheet_id,
      current_timesheet.version,
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
     and rotation_audit.after_json->>'new_timesheet_id'=current_timesheet.timesheet_id::text
    join public.timesheets previous_timesheet
      on previous_timesheet.timesheet_id=
        case
          when rotation_audit.before_json->>'old_timesheet_id'
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          then (rotation_audit.before_json->>'old_timesheet_id')::uuid
          else null
        end
     and previous_timesheet.booking_id=current_timesheet.booking_id
     and previous_timesheet.version=current_timesheet.version-1
     and previous_timesheet.is_current=false
     and previous_timesheet.status='REVOKED'
     and previous_timesheet.submission_mode='ELECTRONIC'
    join lateral (
      select workflow.id
      from public.candidate_submission_workflows workflow
      where workflow.state='REJECTED'
        and workflow.contract_week_id=week_row.id
        and workflow.target_timesheet_id=previous_timesheet.timesheet_id
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
      and current_timesheet.actual_schedule_json is null
      and week_row.status='OPEN'
      and week_row.submission_mode_snapshot='MANUAL'
      and week_row.day_entries_json='[]'::jsonb
      and week_row.totals_json='{}'::jsonb
      and financials.processing_status='UNPROCESSED'
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
    update public.timesheets
    set submission_mode='ELECTRONIC',updated_at=clock_timestamp()
    where timesheet_id=v_row.timesheet_id
      and is_current=true
      and submission_mode='MANUAL';
    if not found then
      raise exception 'CANDIDATE_ELECTRONIC_REJECTION_REPAIR_CONFLICT'
        using errcode='40001';
    end if;

    update public.contract_weeks
    set submission_mode_snapshot='ELECTRONIC',updated_at=clock_timestamp()
    where id=v_row.contract_week_id
      and timesheet_id=v_row.timesheet_id
      and status='OPEN'
      and submission_mode_snapshot='MANUAL';
    if not found then
      raise exception 'CANDIDATE_ELECTRONIC_REJECTION_REPAIR_CONFLICT'
        using errcode='40001';
    end if;

    insert into public.audit_events(
      actor_user_id,object_type,object_id_text,action,before_json,after_json,
      reason,correlation_id,ts_utc
    ) values (
      null,'timesheet',v_row.timesheet_id::text,
      'CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_REPAIRED',
      jsonb_build_object(
        'submission_mode','MANUAL',
        'contract_week_id',v_row.contract_week_id,
        'rejected_workflow_id',v_row.rejected_workflow_id
      ),
      jsonb_build_object(
        'submission_mode','ELECTRONIC',
        'contract_week_id',v_row.contract_week_id,
        'rejected_workflow_id',v_row.rejected_workflow_id
      ),
      'Repair empty electronic rejection replacement so Candidate resubmission remains available',
      'ELECTRONIC-REJECTION-REPAIR:'||v_row.timesheet_id::text,
      clock_timestamp()
    );
  end loop;
end;
$migration$;
