-- Preserve the electronic route when Office rejects a Candidate electronic
-- submission.  The rejected version remains immutable history; the fresh
-- current version is an empty ELECTRONIC record that the Candidate can edit
-- through RESUBMIT_REJECTED.  The preceding definition accidentally created
-- a MANUAL_NON_QR replacement, so the read contract offered resubmission while
-- the mutation authority correctly refused to edit that manual record.

create or replace function private._candidate_timesheet_reject_rotate_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_reason text,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
)
returns uuid
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_current public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_new_timesheet_id uuid;
  v_new_version integer;
begin
  select current_row.* into v_current
  from public.timesheets current_row
  join public.timesheets requested_row on requested_row.booking_id=current_row.booking_id
  where requested_row.timesheet_id=p_timesheet_id and current_row.is_current=true
  for update of current_row;
  if not found then
    raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002';
  end if;
  if v_current.timesheet_id is distinct from p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true for update;
  if v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_REJECT_PROTECTED_HISTORY' using errcode='55000';
  end if;
  v_new_version:=coalesce(v_current.version,1)+1;
  update public.timesheets set is_current=false,status='REVOKED',
    revoked_reason=btrim(p_reason),revoked_by=p_actor_user_id::text,updated_at=p_now_utc
  where timesheet_id=v_current.timesheet_id and is_current=true;
  insert into public.timesheets(
    booking_id,version,is_current,status,contract_id,submission_mode,line_type,sheet_scope,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,week_ending_date,
    worked_start_iso,worked_end_iso,break_start_iso,break_end_iso,break_minutes,actual_schedule_json,
    additional_units_week,additional_units_per_day,manual_pdf_r2_key,authorised_at_server,
    reference_number,day_references_json,qr_token,qr_status,qr_payload_json,qr_generated_at,
    qr_scanned_at,qr_scan_info_json,qr_r2_key,created_at,updated_at
  ) values (
    v_current.booking_id,v_new_version,true,'RECEIVED',v_current.contract_id,'ELECTRONIC',
    v_current.line_type,v_current.sheet_scope,v_current.occupant_key_norm,v_current.hospital_norm,
    v_current.ward_norm,v_current.job_title_norm,v_current.shift_label_norm,v_current.week_ending_date,
    null,null,null,null,null,null,'{}'::jsonb,'{}'::jsonb,null,null,null,null,
    null,null,'{}'::jsonb,null,null,null,null,p_now_utc,p_now_utc
  ) returning timesheet_id into v_new_timesheet_id;
  update public.contract_weeks set timesheet_id=v_new_timesheet_id,status='OPEN',
    submission_mode_snapshot='ELECTRONIC',day_entries_json='[]'::jsonb,
    totals_json='{}'::jsonb,updated_at=p_now_utc
  where timesheet_id=v_current.timesheet_id;
  if v_fin.id is not null then
    update public.timesheets_financials set
      timesheet_id=v_new_timesheet_id,timesheet_version=v_new_version,processing_status='UNPROCESSED',
      worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,break_minutes=null,
      actual_schedule_json=null,actual_minutes_by_day_json=null,
      hours_day=0,hours_night=0,hours_sat=0,hours_sun=0,hours_bh=0,total_hours=0,
      total_pay_ex_vat=0,total_charge_ex_vat=0,margin_ex_vat=0,
      additional_pay_ex_vat=0,additional_charge_ex_vat=0,additional_margin_ex_vat=0,
      additional_units_json='{}'::jsonb,expenses_pay_ex_vat=0,expenses_charge_ex_vat=0,
      expenses_description=null,expenses_evidence_r2_key=null,expenses_evidence_manifest=null,
      mileage_units=0,mileage_pay_ex_vat=0,mileage_charge_ex_vat=0,
      mileage_evidence_r2_key=null,mileage_evidence_manifest=null,
      travel_pay_ex_vat=0,travel_charge_ex_vat=0,accommodation_pay_ex_vat=0,
      accommodation_charge_ex_vat=0,other_pay_ex_vat=0,other_charge_ex_vat=0,
      authorised_at_utc=null,authorised_by_user_id=null,updated_at=p_now_utc
    where id=v_fin.id;
    insert into public.ts_financials_outbox(
      timesheet_id,reason,attempt_count,next_attempt_at,last_error,created_at
    ) values (v_new_timesheet_id,'REVOKED',0,p_now_utc,null,p_now_utc)
    on conflict on constraint uq_tsfin_outbox do nothing;
  end if;
  perform private._candidate_audit_v1(
    'timesheet',v_new_timesheet_id::text,'CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED',
    jsonb_build_object('old_timesheet_id',v_current.timesheet_id,'old_version',v_current.version),
    jsonb_build_object(
      'new_timesheet_id',v_new_timesheet_id,'new_version',v_new_version,
      'submission_mode','ELECTRONIC'
    ),btrim(p_reason),p_actor_user_id,null,p_now_utc
  );
  return v_new_timesheet_id;
end;
$function$;

revoke all on function private._candidate_timesheet_reject_rotate_v1(
  uuid,uuid,text,uuid,timestamptz
) from public,anon,authenticated,service_role;
