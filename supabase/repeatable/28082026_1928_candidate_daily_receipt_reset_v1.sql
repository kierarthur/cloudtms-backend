-- Candidate receipt adapter. Existing reset/financial owner is delegated unchanged
-- whenever a real financial row exists. This new branch rotates factual history only.
begin;

create or replace function private._candidate_daily_receipt_reset_v1(
  p_environment text,
  p_candidate_id uuid,
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_reason text,
  p_actor_user_id uuid,
  p_event text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_event text:=upper(btrim(coalesce(p_event,'')));
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_new_timesheet_id uuid;
  v_new_version integer;
  v_audit_actor_user_id uuid;
  v_receipt_context jsonb;
begin
  if p_timesheet_id is null
     or p_expected_timesheet_id is null
     or p_actor_user_id is null
     or nullif(btrim(coalesce(p_reason,'')),'') is null
     or v_event not in ('CANDIDATE_WITHDRAWN','OFFICE_REJECTED') then
    raise exception 'CANDIDATE_DAILY_RESET_INVALID' using errcode='22023';
  end if;

  select requested_row.* into v_requested
  from public.timesheets requested_row
  where requested_row.timesheet_id=p_timesheet_id;
  if not found
     or v_requested.sheet_scope<>'DAILY'::public.timesheet_scope_enum
     or nullif(btrim(coalesce(v_requested.booking_id,'')),'') is null then
    raise exception 'CANDIDATE_DAILY_RESET_TARGET_NOT_FOUND' using errcode='P0002';
  end if;

  select current_row.* into v_current
  from public.timesheets current_row
  where current_row.booking_id=v_requested.booking_id
    and current_row.is_current=true
    and current_row.archived_at_utc is null
    and current_row.sheet_scope='DAILY'::public.timesheet_scope_enum
  for update;
  if not found or v_current.timesheet_id is distinct from p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;

  -- Prove the same Candidate/environment even when the existing financial
  -- reset owner will perform the rotation. Do not delegate a foreign target.
  if v_event='OFFICE_REJECTED' and not private._candidate_office_service_context_valid_v1(
    p_environment,p_actor_user_id,'REJECT_CONFIRM') then
    raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_REQUIRED' using errcode='28000';
  end if;
  v_receipt_context:=private._candidate_daily_receipt_context_v1(
    p_environment,p_candidate_id,v_current.timesheet_id,
    v_event<>'OFFICE_REJECTED',p_now_utc);
  select financial_row.* into v_fin
  from public.timesheets_financials financial_row
  where financial_row.timesheet_id=v_current.timesheet_id
    and financial_row.is_current=true
  order by financial_row.computed_at_utc desc nulls last,
    financial_row.updated_at desc,financial_row.id desc
  limit 1
  for update;
  if v_current.authorised_at_server is not null
     or v_fin.authorised_at_utc is not null or v_fin.paid_at_utc is not null
     or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_DAILY_RESET_PROTECTED_HISTORY' using errcode='55000';
  end if;
  if coalesce((v_receipt_context->>'import_authoritative')::boolean,false)
     or v_current.qr_status is not null or v_current.qr_token is not null
     or v_current.qr_r2_key is not null
     or (v_current.submission_mode<>'ELECTRONIC'
       and v_current.candidate_submission_route_intent is distinct from 'ELECTRONIC') then
    raise exception 'CANDIDATE_RECORD_VIEW_ONLY' using errcode='55000';
  end if;
  if v_fin.id is not null then
    -- Keep the existing financial/history owner byte-for-byte unchanged.
    return private._candidate_daily_submission_reset_v1(p_timesheet_id,p_expected_timesheet_id,
      p_reason,p_actor_user_id,p_event,p_now_utc);
  end if;
  if not coalesce((v_receipt_context->>'candidate_first_receipt')::boolean,false) then
    raise exception 'CANDIDATE_DAILY_FINANCIALS_NOT_FOUND' using errcode='P0002';
  end if;
  if v_current.authorised_at_server is not null
     or v_fin.authorised_at_utc is not null
     or v_fin.paid_at_utc is not null
     or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_DAILY_RESET_PROTECTED_HISTORY' using errcode='55000';
  end if;

  v_new_version:=coalesce(v_current.version,1)+1;
  update public.timesheets set
    is_current=false,
    status='REVOKED',
    revoked_at=p_now_utc,
    revoked_reason=btrim(p_reason),
    revoked_by=p_actor_user_id::text,
    updated_at=p_now_utc
  where timesheet_id=v_current.timesheet_id and is_current=true;

  insert into public.timesheets(
    booking_id,version,is_current,status,contract_id,submission_mode,line_type,sheet_scope,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,band,
    scheduled_start_iso,scheduled_end_iso,week_ending_date,
    worked_start_iso,worked_end_iso,break_start_iso,break_end_iso,break_minutes,worked_minutes,
    actual_schedule_json,additional_units_week,additional_units_per_day,
    manual_pdf_r2_key,authorised_at_server,reference_number,day_references_json,
    qr_token,qr_status,qr_payload_json,qr_generated_at,qr_scanned_at,qr_scan_info_json,qr_r2_key,
    candidate_hint_text,candidate_submission_route_intent,created_at,updated_at
  ) values (
    v_current.booking_id,v_new_version,true,'RECEIVED',v_current.contract_id,'MANUAL',
    v_current.line_type,v_current.sheet_scope,v_current.occupant_key_norm,
    v_current.hospital_norm,v_current.ward_norm,v_current.job_title_norm,
    v_current.shift_label_norm,v_current.band,v_current.scheduled_start_iso,
    v_current.scheduled_end_iso,v_current.week_ending_date,
    null,null,null,null,null,null,null,'{}'::jsonb,'{}'::jsonb,
    null,null,null,null,null,null,'{}'::jsonb,null,null,null,null,
    v_current.candidate_hint_text,'ELECTRONIC',p_now_utc,p_now_utc
  ) returning timesheet_id into v_new_timesheet_id;

  -- No financial snapshot exists in this receipt-only branch. Do not
  -- fabricate one merely to rotate the same current booking family.

  update public.timesheet_evidence set processing_state='SUPERSEDED'
  where timesheet_id=v_current.timesheet_id and processing_state<>'SUPERSEDED';

  if v_event='OFFICE_REJECTED' then
    select actor_row.id into v_audit_actor_user_id
    from public.tms_users actor_row
    where actor_row.id=p_actor_user_id;
  else
    select settings_row.candidate_app_system_actor_user_id
    into v_audit_actor_user_id
    from public.settings_defaults settings_row
    where settings_row.id=1;
  end if;
  if v_audit_actor_user_id is null then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_REQUIRED' using errcode='55000';
  end if;

  perform private._candidate_audit_v1(
    'timesheet',v_new_timesheet_id::text,
    case when v_event='OFFICE_REJECTED'
      then 'CANDIDATE_DAILY_RECEIPT_REJECTED_VERSION_ROTATED'
      else 'CANDIDATE_DAILY_RECEIPT_WITHDRAWN_VERSION_ROTATED' end,
    jsonb_build_object(
      'old_timesheet_id',v_current.timesheet_id,
      'old_version',v_current.version,
      'previous_submission_mode',v_current.submission_mode
    ),
    jsonb_build_object(
      'new_timesheet_id',v_new_timesheet_id,
      'new_version',v_new_version,
      'candidate_submission_route_intent','ELECTRONIC'
    ),
    btrim(p_reason),v_audit_actor_user_id,null,p_now_utc
  );

  return jsonb_build_object(
    'reset',true,
    'scope','DAILY',
    'event',v_event,
    'old_timesheet_id',v_current.timesheet_id,
    'current_timesheet_id',v_new_timesheet_id,
    'timesheet_version',v_new_version,
    'draft_submission_mode','MANUAL',
    'effective_submission_mode','ELECTRONIC'
  );
end;
$function$;

alter function private._candidate_daily_receipt_reset_v1(text,uuid,uuid,uuid,text,uuid,text,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_receipt_reset_v1(text,uuid,uuid,uuid,text,uuid,text,timestamptz)
  from public,anon,authenticated,service_role;
commit;
