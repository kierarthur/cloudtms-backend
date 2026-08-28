-- Repeatable CloudTMS function/view authority: candidate_daily_read_context_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_daily_break_entry_v1(
  p_booking_id text,p_work_date date,p_client_id uuid default null,
  p_contract_id uuid default null,p_applicable boolean default true
) returns jsonb language plpgsql stable security definer set search_path=''
as $function$
declare
  v_resolution jsonb;
  v_applicable boolean:=coalesce(p_applicable,false);
  v_identity text;
  v_reason text;
begin
  if nullif(btrim(p_booking_id),'') is null or p_work_date is null then
    raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
  end if;
  if p_client_id is not null then
    v_resolution:=private._timesheet_break_entry_precedence_v1(
      p_client_id,p_contract_id,p_work_date);
  else
    v_resolution:=jsonb_build_object('mode','START_END_TIMES','source','DEFAULT',
      'settings_as_of_date',p_work_date);
  end if;
  v_applicable:=v_applicable and not coalesce((v_resolution->>'is_nhsp')::boolean,false)
    and not coalesce((v_resolution->>'no_timesheet_required')::boolean,false);
  v_reason:=case when v_applicable then 'CANDIDATE_EDITABLE_ELECTRONIC'
    else 'NOT_CANDIDATE_EDITABLE' end;
  -- Booking/date, never a fabricated Contract Week or version-specific TS ID.
  v_identity:=concat_ws('|','CANDIDATE_DAILY_BREAK_ENTRY_V1',p_booking_id,p_work_date,
    p_client_id,p_contract_id,v_resolution->>'client_settings_id',
    v_resolution->>'mode',v_resolution->>'source',v_applicable,v_reason);
  return jsonb_build_object('applicable',v_applicable,
    'mode',case when v_applicable then v_resolution->'mode' else 'null'::jsonb end,
    'source',case when v_applicable then v_resolution->'source'
      else to_jsonb('NOT_APPLICABLE'::text) end,'reason',v_reason,
    'context_version','CANDIDATE_BREAK_ENTRY_V1',
    'context_token',encode(extensions.digest(v_identity,'sha256'),'hex'));
end;
$function$;

create or replace function private._candidate_daily_read_projection_v1(
  p_environment text,p_candidate_id uuid,p_timesheet_id uuid,p_now_utc timestamptz default now()
) returns jsonb language plpgsql stable security definer set search_path=''
as $function$
declare
  v_context jsonb;
  v_ts public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_protected boolean;
  v_import boolean;
  v_entitled boolean;
  v_electronic boolean;
  v_editable boolean;
  v_has_evidence boolean;
  v_policy jsonb;
  v_caps jsonb;
  v_hours numeric;
begin
  -- False uses the non-locking, non-mutating exact ownership guard.
  -- A read never creates a Timesheet or financial row.
  v_context:=private._candidate_daily_receipt_context_v1(
    p_environment,p_candidate_id,p_timesheet_id,false,p_now_utc);
  select * into strict v_ts from public.timesheets where timesheet_id=p_timesheet_id;
  select * into v_fin from public.timesheets_financials
    where timesheet_id=p_timesheet_id and is_current
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  v_policy:=v_context->'policy';
  v_import:=coalesce((v_context->>'import_authoritative')::boolean,false);
  v_protected:=v_ts.authorised_at_server is not null or v_fin.authorised_at_utc is not null
    or v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null
    or upper(coalesce(v_ts.status::text,''))='INVOICED';
  v_entitled:=exists(select 1 from private.candidate_daily_entitlements e
    where e.environment=p_environment and e.candidate_id=p_candidate_id and e.enabled)
    and private._candidate_daily_entitled_v1(p_candidate_id);
  v_electronic:=(v_ts.submission_mode='ELECTRONIC'
      or v_ts.candidate_submission_route_intent='ELECTRONIC')
    and v_ts.qr_status is null and v_ts.qr_token is null and v_ts.qr_r2_key is null;
  v_editable:=v_entitled and coalesce(v_electronic,false) and not v_protected and not v_import;
  v_has_evidence:=exists(select 1 from public.timesheet_evidence e
    where e.timesheet_id=p_timesheet_id and upper(e.kind)='TIMESHEET'
      and e.processing_state<>'SUPERSEDED');
  -- Factual minutes are not pay/charge calculation.
  v_hours:=coalesce(v_fin.total_hours,v_ts.worked_minutes::numeric/60,0);
  v_caps:=jsonb_build_object('record_role',case when v_protected then 'PROTECTED'
      when v_import then 'IMPORT_HOURS' else 'HOURS_ONLY' end,
    'reason_codes','[]'::jsonb,'timesheet_id',p_timesheet_id,'contract_week_id',null,
    'contract_id',v_ts.contract_id,'candidate_id',p_candidate_id,
    'client_id',v_context->'client_id','week_ending_date',v_ts.week_ending_date,
    'additional_seq',0,'hours_value',v_hours,'additional_units_value',0,'expense_value',0,
    'effective_separation',false,'import_authoritative',v_import,
    'route_family',case when v_import then 'IMPORT' when v_electronic then 'ELECTRONIC' else 'MANUAL' end,
    'effective_submission_mode',case when v_electronic then 'ELECTRONIC' else v_ts.submission_mode::text end,
    'protected',v_protected,'candidate_mutation_locked',v_protected,
    'has_active_timesheet_evidence',v_has_evidence,
    'candidate_hours_submission_allowed',v_editable,'candidate_expenses_allowed',false,
    'candidate_paper_submission_allowed',false,'candidate_no_work_allowed',false,
    'can_edit_hours',v_editable,'can_edit_expenses',false,
    'can_attach_timesheet',v_editable and not v_has_evidence,
    'can_attach_expense_evidence',false,'can_attach_mileage_evidence',false,
    'can_attach_travel_evidence',false,'can_attach_accommodation_evidence',false,
    'can_attach_other_evidence',false,'can_process',false,
    -- Office rejection has its own authenticated preview/confirm authority.
    -- It must remain available after Rota access is removed.
    'can_reject_candidate_submission',coalesce(v_electronic,false) and not v_protected and not v_import,
    'reject_scope','COMPLETE_TIMESHEET_RECORD','requires_carrier',false,
    'expense_invoice_email_ready',false,'policy',v_policy);
  v_caps:=v_caps||jsonb_build_object('capability_hash',
    encode(extensions.digest(convert_to(v_caps::text,'UTF8'),'sha256'),'hex'));
  return jsonb_build_object('capabilities',v_caps,'hours',v_hours,
    'daily_shift',jsonb_build_object('booking_id',v_ts.booking_id,
      'work_date',v_context->'work_date','week_ending_date',v_ts.week_ending_date,
      'hospital',v_ts.hospital_norm,'ward',v_ts.ward_norm,'job_title',v_ts.job_title_norm,
      'shift_type',v_ts.shift_label_norm,'shift_starts_at',v_ts.scheduled_start_iso,
      'shift_ends_at',v_ts.scheduled_end_iso),
    'break_entry',private._candidate_daily_break_entry_v1(
      v_ts.booking_id,(v_context->>'work_date')::date,
      nullif(v_context->>'client_id','')::uuid,v_ts.contract_id,v_editable));
end;
$function$;

alter function private._candidate_daily_break_entry_v1(text,date,uuid,uuid,boolean) owner to postgres;
alter function private._candidate_daily_read_projection_v1(text,uuid,uuid,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_break_entry_v1(text,date,uuid,uuid,boolean)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_read_projection_v1(text,uuid,uuid,timestamptz)
  from public,anon,authenticated,service_role;

commit;
