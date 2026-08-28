-- Candidate factual Daily admission. Financial owners/calculations are unchanged.
begin;

create or replace function private._candidate_daily_receipt_context_v1(
  p_environment text,
  p_candidate_id uuid,
  p_timesheet_id uuid,
  p_require_mutable boolean default true,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_candidate public.candidates%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_contract public.contracts%rowtype;
  v_global public.settings_defaults%rowtype;
  v_client_id uuid;
  v_date date;
  v_owned_receipt boolean:=false;
  v_import jsonb;
  v_policy jsonb;
  v_fin_json jsonb;
begin
  if p_environment not in ('TEST','LIVE') or p_environment is null
     or p_candidate_id is null or p_timesheet_id is null
     or p_require_mutable is null then
    raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
  end if;
  select * into v_candidate from public.candidates
    where id=p_candidate_id and active=true;
  if not found then
    raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
  end if;
  if p_require_mutable then
    select * into v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current=true
      and archived_at_utc is null and sheet_scope='DAILY'
      and nullif(btrim(booking_id),'') is not null for update;
  else
    select * into v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current=true
      and archived_at_utc is null and sheet_scope='DAILY'
      and nullif(btrim(booking_id),'') is not null;
  end if;
  if not found then
    raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
  end if;
  if p_require_mutable then
    select * into v_fin from public.timesheets_financials
    where timesheet_id=p_timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1 for update;
  else
    select * into v_fin from public.timesheets_financials
    where timesheet_id=p_timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;
  if v_fin.id is not null and v_fin.candidate_id is not null
     and v_fin.candidate_id is distinct from p_candidate_id then
    raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
  end if;

  -- Before financial assignment exists, only a server-created Candidate receipt
  -- (or its exact booking-family replacement) proves ownership. A caller-supplied
  -- hint, matching name or a weekly Client/Contract is never enough.
  v_owned_receipt:=v_timesheet.candidate_hint_text->>'candidate_id'=p_candidate_id::text
    and ((nullif(btrim(v_candidate.key_norm),'') is not null
        and upper(btrim(v_timesheet.occupant_key_norm))=upper(btrim(v_candidate.key_norm))
        and v_timesheet.idempotency_key like 'candidate-daily-first:%')
      or exists(select 1 from public.candidate_submission_workflows w
        join public.timesheets origin on origin.timesheet_id=w.anchor_timesheet_id
        where w.environment=p_environment and w.candidate_id=p_candidate_id
          and w.workflow_kind='DAILY' and origin.booking_id=v_timesheet.booking_id
          and origin.idempotency_key like 'candidate-daily-first:%'
          and origin.candidate_hint_text->>'candidate_id'=p_candidate_id::text
          and upper(btrim(origin.occupant_key_norm))=upper(btrim(v_timesheet.occupant_key_norm))
          and w.creation_identity_json#>>'{request,daily_source,booking_id}'=v_timesheet.booking_id));
  if (v_fin.id is null or v_fin.candidate_id is null) and not coalesce(v_owned_receipt,false) then
    raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
  end if;
  if p_require_mutable then
    if not exists(select 1 from private.candidate_daily_entitlements e
      where e.environment=p_environment and e.candidate_id=p_candidate_id and e.enabled)
      or not private._candidate_daily_entitled_v1(p_candidate_id) then
      raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
    end if;
    if v_timesheet.authorised_at_server is not null
       or v_fin.authorised_at_utc is not null or v_fin.paid_at_utc is not null
       or v_fin.locked_by_invoice_id is not null then
      raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
    end if;
  end if;
  if v_timesheet.contract_id is not null then
    select * into v_contract from public.contracts
      where id=v_timesheet.contract_id and candidate_id=p_candidate_id;
    if not found then
      raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
    end if;
  end if;
  v_client_id:=coalesce(v_fin.client_id,v_contract.client_id);
  v_date:=private._candidate_daily_work_date_v1(
    coalesce(v_fin.worked_start_iso,v_timesheet.worked_start_iso),
    v_timesheet.scheduled_start_iso,v_timesheet.week_ending_date);
  v_fin_json:=coalesce(to_jsonb(v_fin),'{}'::jsonb);
  if v_client_id is not null then
    v_import:=private._candidate_import_authoritative_v1(
      v_client_id,v_contract.id,p_timesheet_id,v_fin_json,v_date);
    v_policy:=private._candidate_policy_resolve_v1(v_client_id,v_contract.id,v_date);
  else
    -- Preserve the current safe no-Client PHONE default without choosing a
    -- Client, role, rate or financial policy on Office's behalf.
    select * into v_global from public.settings_defaults where id=1;
    if not found then
      raise exception 'CANDIDATE_GLOBAL_SETTINGS_MISSING' using errcode='55000';
    end if;
    v_import:=jsonb_build_object('is_import_authoritative',
      nullif(v_fin_json->>'nhsp_import_id','') is not null
      or coalesce(v_fin_json->'external_source_rows_json','null'::jsonb)
        not in ('null'::jsonb,'[]'::jsonb,'{}'::jsonb)
      or upper(coalesce(v_fin_json->>'basis','')) in (
        'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_SELF_BILL_ADJUSTMENT',
        'HEALTHROSTER_ADJUSTMENT','HEALTHROSTER_WEEKLY','HEALTHROSTER_WEEKLY_ADJUSTMENT'));
    v_policy:=jsonb_build_object(
      'client_id',null,'contract_id',null,'evaluation_date',v_date,
      'candidate_electronic_auto_authorise',false,
      'candidate_electronic_auto_authorise_source','UNRESOLVED',
      'expenses_require_separate_timesheet',false,'paper_submission_enabled',false,
      'allow_daily_manager_authorise_on_phone',true,
      'allow_daily_manager_authorise_by_email',false,
      'manager_approval_policy',jsonb_build_object(
        'approved_emails','[]'::jsonb,'approved_domains','[]'::jsonb,
        'allow_free_business_email',false),
      'hours_deviation_pct',v_global.candidate_hours_deviation_pct,
      'client_setting_found',false,'contract_found',false,
      'global_settings_updated_at',v_global.updated_at);
  end if;
  if p_require_mutable and (coalesce((v_import->>'is_import_authoritative')::boolean,false)
      or v_timesheet.qr_status is not null or v_timesheet.qr_token is not null
      or v_timesheet.qr_r2_key is not null
      or (v_timesheet.submission_mode<>'ELECTRONIC'
        and v_timesheet.candidate_submission_route_intent is distinct from 'ELECTRONIC')) then
    raise exception 'CANDIDATE_RECORD_VIEW_ONLY' using errcode='55000';
  end if;
  -- Current Daily product policy is PHONE only. This projection never broadens
  -- a resolved Client's on-phone permission or its import authority.
  v_policy:=v_policy||jsonb_build_object('allow_daily_manager_authorise_by_email',false,
    'paper_submission_enabled',false,'expense_invoice_email_ready',false);
  v_policy:=(v_policy-'policy_fingerprint')||jsonb_build_object('policy_fingerprint',
    encode(extensions.digest(convert_to((v_policy-'policy_fingerprint')::text,'UTF8'),'sha256'),'hex'));
  return jsonb_build_object('timesheet_id',p_timesheet_id,'candidate_id',p_candidate_id,
    'client_id',v_client_id,'contract_id',v_contract.id,'work_date',v_date,
    'week_ending_date',v_timesheet.week_ending_date,'booking_id',v_timesheet.booking_id,
    'candidate_first_receipt',coalesce(v_owned_receipt,false),
    'financial_assignment_present',v_fin.id is not null,
    'import_authoritative',coalesce((v_import->>'is_import_authoritative')::boolean,false),
    'office_resolution_pending',v_fin.id is null or v_client_id is null
      or nullif(btrim(v_fin.role),'') is null
      or v_fin.processing_status::text in (
        'UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
      or coalesce(v_fin.has_rate_issue,false) or coalesce(v_fin.has_pay_channel_issue,false),
    'policy',v_policy);
end;
$function$;

alter function private._candidate_daily_receipt_context_v1(text,uuid,uuid,boolean,timestamptz) owner to postgres;
revoke all on function private._candidate_daily_receipt_context_v1(text,uuid,uuid,boolean,timestamptz)
  from public,anon,authenticated,service_role;
commit;
