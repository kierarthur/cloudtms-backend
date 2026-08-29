-- Candidate app finalisation authority.
--
-- This repeatable deliberately preserves the existing 63-operation HTTP
-- contract. It adds two service-only database seams used by existing
-- operations:
--   * an adaptive, versioned break-entry context; and
--   * a returned-paper page/QR proof bound to the current Candidate workflow.

\set ON_ERROR_STOP on

begin;

create or replace function private._timesheet_break_entry_precedence_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_as_of_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_as_of date:=coalesce(p_as_of_date,(statement_timestamp() at time zone 'Europe/London')::date);
  v_contract public.contracts%rowtype;
  v_settings public.client_settings%rowtype;
  v_mode public.timesheet_break_entry_mode_enum;
  v_source text;
  v_is_nhsp boolean;
  v_autoprocess_hr boolean;
  v_no_timesheet_required boolean;
begin
  if p_client_id is null then
    raise exception 'BREAK_ENTRY_CLIENT_REQUIRED' using errcode='22023';
  end if;
  if v_as_of<'2000-01-01'::date or v_as_of>'2200-12-31'::date then
    raise exception 'BREAK_ENTRY_AS_OF_DATE_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.clients client_row where client_row.id=p_client_id) then
    raise exception 'CLIENT_OR_SETTINGS_NOT_FOUND' using errcode='P0002';
  end if;

  if p_contract_id is not null then
    select * into v_contract from public.contracts where id=p_contract_id;
    if not found then raise exception 'CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
    if v_contract.client_id is distinct from p_client_id then
      raise exception 'CONTRACT_CLIENT_MISMATCH' using errcode='22023';
    end if;
  end if;

  select * into v_settings
  from public.client_settings settings
  where settings.client_id=p_client_id
    and (settings.effective_from is null or settings.effective_from<=v_as_of)
  order by settings.effective_from desc nulls last,
    settings.updated_at desc nulls last,settings.id desc
  limit 1;
  if not found then raise exception 'CLIENT_OR_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;

  if p_contract_id is not null
     and coalesce(v_contract.overrideclientsettings,false)
     and v_contract.timesheet_break_entry_mode is not null then
    v_mode:=v_contract.timesheet_break_entry_mode;
    v_source:='CONTRACT_OVERRIDE';
  elsif v_settings.timesheet_break_entry_mode is not null then
    v_mode:=v_settings.timesheet_break_entry_mode;
    v_source:='CLIENT_SETTINGS';
  else
    v_mode:='START_END_TIMES'::public.timesheet_break_entry_mode_enum;
    v_source:='DEFAULT';
  end if;

  v_is_nhsp:=case when p_contract_id is not null
      and coalesce(v_contract.overrideclientsettings,false)
      and v_contract.is_nhsp is not null
    then v_contract.is_nhsp else coalesce(v_settings.is_nhsp,false) end;
  v_autoprocess_hr:=case when p_contract_id is not null
      and coalesce(v_contract.overrideclientsettings,false)
      and v_contract.autoprocess_hr is not null
    then v_contract.autoprocess_hr else coalesce(v_settings.autoprocess_hr,false) end;
  v_no_timesheet_required:=case when p_contract_id is not null
      and coalesce(v_contract.overrideclientsettings,false)
      and v_contract.no_timesheet_required is not null
    then v_contract.no_timesheet_required else coalesce(v_settings.no_timesheet_required,false) end;

  return jsonb_build_object(
    'mode',v_mode,
    'source',v_source,
    'settings_as_of_date',v_as_of,
    'client_settings_id',v_settings.id,
    'contract_id',p_contract_id,
    'contract_override_active',coalesce(v_contract.overrideclientsettings,false),
    'is_nhsp',v_is_nhsp,
    'autoprocess_hr',v_autoprocess_hr,
    'no_timesheet_required',v_no_timesheet_required
  );
end
$function$;

create or replace function public.timesheet_break_entry_effective_get_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_as_of_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_resolution jsonb;
  v_applicable boolean;
  v_reason text;
begin
  v_resolution:=private._timesheet_break_entry_precedence_v1(
    p_client_id,p_contract_id,p_as_of_date
  );
  v_applicable:=coalesce((v_resolution->>'autoprocess_hr')::boolean,false)
    and not coalesce((v_resolution->>'no_timesheet_required')::boolean,false)
    and not coalesce((v_resolution->>'is_nhsp')::boolean,false);
  v_reason:=case
    when v_applicable then 'VALIDATION_TIMESHEETS'
    when coalesce((v_resolution->>'is_nhsp')::boolean,false) then 'DEDICATED_NHSP_WEEKLY'
    when coalesce((v_resolution->>'autoprocess_hr')::boolean,false)
      and coalesce((v_resolution->>'no_timesheet_required')::boolean,false)
      then 'IMPORT_AUTHORITATIVE_ROSTER'
    else 'MANUAL_TIMESHEETS'
  end;
  return (v_resolution-'is_nhsp'-'autoprocess_hr'-'no_timesheet_required')
    ||jsonb_build_object(
      'applicable',v_applicable,
      'mode',case when v_applicable then v_resolution->'mode' else 'null'::jsonb end,
      'source',case when v_applicable then v_resolution->'source'
        else to_jsonb('NOT_APPLICABLE'::text) end,
      'reason',v_reason
    );
end
$function$;

create or replace function private._candidate_break_entry_context_core_v1(
  p_timesheet_id uuid,
  p_contract_week_id uuid,
  p_as_of_date date,
  p_capabilities jsonb
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_resolution jsonb;
  v_applicable boolean;
  v_reason text;
  v_context_identity text;
begin
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current=true and archived_at_utc is null;
  end if;
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  elsif v_timesheet.timesheet_id is not null then
    select * into v_week from public.contract_weeks
    where timesheet_id=v_timesheet.timesheet_id
    order by updated_at desc,id desc limit 1;
  end if;
  if v_week.id is not null then
    select * into v_contract from public.contracts where id=v_week.contract_id;
  elsif v_timesheet.contract_id is not null then
    select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  end if;
  if not found or v_contract.id is null then
    raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002';
  end if;

  v_resolution:=private._timesheet_break_entry_precedence_v1(
    v_contract.client_id,v_contract.id,p_as_of_date
  );
  v_applicable:=coalesce((p_capabilities->>'can_edit_hours')::boolean,false)
    and not coalesce((p_capabilities->>'import_authoritative')::boolean,false)
    and coalesce(p_capabilities->>'route_family','')='ELECTRONIC'
    and not coalesce((v_resolution->>'is_nhsp')::boolean,false)
    and not coalesce((v_resolution->>'no_timesheet_required')::boolean,false);
  v_reason:=case
    when v_applicable then 'CANDIDATE_EDITABLE_ELECTRONIC'
    when coalesce((p_capabilities->>'import_authoritative')::boolean,false)
      then 'IMPORT_AUTHORITATIVE'
    when coalesce((v_resolution->>'is_nhsp')::boolean,false) then 'NHSP'
    when coalesce((v_resolution->>'no_timesheet_required')::boolean,false)
      then 'NO_TIMESHEET_REQUIRED'
    when coalesce(p_capabilities->>'route_family','')<>'ELECTRONIC' then 'NON_ELECTRONIC_ROUTE'
    else 'NOT_CANDIDATE_EDITABLE'
  end;
  v_context_identity:=concat_ws('|',
    'CANDIDATE_BREAK_ENTRY_V1',v_contract.client_id,v_contract.id,
    coalesce(v_timesheet.timesheet_id::text,''),coalesce(v_week.id::text,''),
    v_resolution->>'settings_as_of_date',v_resolution->>'client_settings_id',
    v_resolution->>'mode',v_resolution->>'source',v_applicable,v_reason
  );
  return jsonb_build_object(
    'applicable',v_applicable,
    'mode',case when v_applicable then v_resolution->'mode' else 'null'::jsonb end,
    'source',case when v_applicable then v_resolution->'source'
      else to_jsonb('NOT_APPLICABLE'::text) end,
    'reason',v_reason,
    'context_version','CANDIDATE_BREAK_ENTRY_V1',
    'context_token',encode(extensions.digest(v_context_identity,'sha256'),'hex')
  );
end
$function$;

create or replace function public.candidate_app_timesheet_detail_v2(
  p_session_id uuid,
  p_environment text,
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_workflow_id uuid default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_result jsonb;
  v_timesheet_id uuid;
  v_contract_week_id uuid;
  v_context jsonb;
begin
  v_result:=public.candidate_app_timesheet_detail_v1(
    p_session_id,p_environment,p_timesheet_id,p_contract_week_id,p_workflow_id,p_now_utc
  );
  v_timesheet_id:=nullif(v_result#>>'{timesheet,id}','')::uuid;
  v_contract_week_id:=nullif(v_result#>>'{contract_week,id}','')::uuid;
  v_context:=private._candidate_break_entry_context_core_v1(
    v_timesheet_id,v_contract_week_id,
    (p_now_utc at time zone 'Europe/London')::date,
    coalesce(v_result->'capabilities','{}'::jsonb)
  );
  return v_result||jsonb_build_object('break_entry',v_context);
end
$function$;

create or replace function public.candidate_break_entry_context_get_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_session jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_capabilities jsonb;
begin
  v_session:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,false
  );
  v_candidate_id:=nullif(v_session->>'selected_candidate_id','')::uuid;
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  v_capabilities:=private._candidate_record_capabilities_v1(
    v_workflow.target_timesheet_id,v_workflow.contract_week_id,'{}'::jsonb
  );
  return private._candidate_break_entry_context_core_v1(
    v_workflow.target_timesheet_id,v_workflow.contract_week_id,
    (p_now_utc at time zone 'Europe/London')::date,v_capabilities
  )||jsonb_build_object(
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation
  );
end
$function$;

create or replace function public.candidate_paper_return_proof_validate_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_manifest_sha256_hex text,
  p_page_key text,
  p_qr_token text default null,
  p_qr_token_sha256_hex text default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_session jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_page jsonb;
  v_manifest_hex text;
  v_qr_required boolean;
  v_qr_hash text;
  v_proof_identity text;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_writes');
  if p_expected_generation is null or p_expected_generation<1
     or lower(coalesce(p_manifest_sha256_hex,'')) !~ '^[0-9a-f]{64}$'
     or nullif(btrim(coalesce(p_page_key,'')),'') is null then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_INVALID' using errcode='22023';
  end if;
  v_session:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_session->>'selected_candidate_id','')::uuid;
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.generation<>p_expected_generation
     or v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or v_workflow.paper_return_manifest_sha256 is null then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_STALE' using errcode='40001';
  end if;
  v_manifest_hex:=encode(v_workflow.paper_return_manifest_sha256,'hex');
  if v_manifest_hex<>lower(p_manifest_sha256_hex)
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
       is distinct from v_workflow.paper_return_manifest_sha256 then
    raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='40001';
  end if;
  select page into v_page
  from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') page
  where page->>'page_key'=p_page_key;
  if v_page is null then
    raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='22023';
  end if;
  v_qr_required:=coalesce(v_page->>'component_kind','')='HOURS_TIMESHEET';
  select * into v_timesheet from public.timesheets
  where timesheet_id=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    and is_current=true and archived_at_utc is null;
  if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;

  v_qr_hash:=encode(extensions.digest(coalesce(v_timesheet.qr_token,''),'sha256'),'hex');
  if v_qr_required then
    if nullif(btrim(coalesce(v_timesheet.qr_token,'')),'') is null
       or upper(coalesce(v_timesheet.qr_status::text,''))<>'PENDING'
       or not (
         (p_qr_token is not null and p_qr_token=v_timesheet.qr_token)
         or (lower(coalesce(p_qr_token_sha256_hex,'')) ~ '^[0-9a-f]{64}$'
           and lower(p_qr_token_sha256_hex)=v_qr_hash)
       ) then
      raise exception 'CANDIDATE_PAPER_QR_PROOF_MISMATCH' using errcode='28000';
    end if;
  elsif p_qr_token is not null or p_qr_token_sha256_hex is not null then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_FORBIDDEN' using errcode='22023';
  end if;

  v_proof_identity:=concat_ws('|',
    'CANDIDATE_PAPER_RETURN_PROOF_V1',v_workflow.id,v_workflow.generation,
    v_manifest_hex,p_page_key,coalesce(v_page->>'component_kind',''),
    case when v_qr_required then v_qr_hash else '' end
  );
  return jsonb_build_object(
    'ok',true,
    'proof_contract_version','CANDIDATE_PAPER_RETURN_PROOF_V1',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id,
    'paper_return_manifest_sha256',v_manifest_hex,
    'paper_return_page_key',p_page_key,
    'page_component_kind',v_page->>'component_kind',
    'qr_required',v_qr_required,
    'qr_token_sha256',case when v_qr_required then v_qr_hash else null end,
    'proof_receipt_sha256',encode(extensions.digest(v_proof_identity,'sha256'),'hex')
  );
end
$function$;

create or replace function public.candidate_manager_email_settings_reset_v1(
  p_expected_version bigint,
  p_actor_identity_hmac_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_defaults public.candidate_manager_email_template_versions%rowtype;
  v_templates jsonb;
  v_result jsonb;
  v_body constant text:='The below candidate has submitted a timesheet or expenses for approval. You can approve or refuse the complete submission using the secure link below.';
  v_html constant text:='<p>The below candidate has submitted a timesheet or expenses for approval. You can approve or refuse the complete submission using the secure link below.</p>';
begin
  select * into strict v_defaults
  from public.candidate_manager_email_template_versions where version=1;
  v_templates:=pg_catalog.jsonb_set(
    pg_catalog.jsonb_set(v_defaults.templates_json,
      '{TIMESHEET,INITIAL,body_text}',pg_catalog.to_jsonb(v_body),false),
    '{TIMESHEET,INITIAL,body_html}',pg_catalog.to_jsonb(v_html),false
  );
  v_templates:=pg_catalog.jsonb_set(
    pg_catalog.jsonb_set(v_templates,
      '{EXPENSE_CLAIM,INITIAL,body_text}',pg_catalog.to_jsonb(v_body),false),
    '{EXPENSE_CLAIM,INITIAL,body_html}',pg_catalog.to_jsonb(v_html),false
  );
  v_result:=public.candidate_manager_email_settings_set_v1(
    p_expected_version,v_templates,v_defaults.sanitizer_policy_version,
    p_actor_identity_hmac_hex,p_idempotency_key,p_now_utc
  );
  update public.candidate_manager_email_template_versions set reason_code='OFFICE_RESET'
  where idempotency_key=p_idempotency_key and reason_code='OFFICE_SAVE';
  return v_result;
end
$function$;

alter function private._timesheet_break_entry_precedence_v1(uuid,uuid,date) owner to postgres;
alter function public.timesheet_break_entry_effective_get_v1(uuid,uuid,date) owner to postgres;
alter function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb) owner to postgres;
alter function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz) owner to postgres;
alter function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz) owner to postgres;
alter function public.candidate_paper_return_proof_validate_v1(uuid,text,uuid,integer,text,text,text,text,timestamptz) owner to postgres;
alter function public.candidate_manager_email_settings_reset_v1(bigint,text,text,timestamptz) owner to postgres;

revoke all on function private._timesheet_break_entry_precedence_v1(uuid,uuid,date)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.timesheet_break_entry_effective_get_v1(uuid,uuid,date)
  from public,anon,authenticated;
revoke all on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_paper_return_proof_validate_v1(uuid,text,uuid,integer,text,text,text,text,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_manager_email_settings_reset_v1(bigint,text,text,timestamptz)
  from public,anon,authenticated;

grant execute on function public.timesheet_break_entry_effective_get_v1(uuid,uuid,date)
  to service_role;
grant execute on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  to service_role;
grant execute on function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz)
  to service_role;
grant execute on function public.candidate_paper_return_proof_validate_v1(uuid,text,uuid,integer,text,text,text,text,timestamptz)
  to service_role;
grant execute on function public.candidate_manager_email_settings_reset_v1(bigint,text,text,timestamptz)
  to service_role;

comment on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz) is
  'Candidate detail v1 plus the server-resolved, versioned adaptive break-entry context.';
comment on function public.candidate_paper_return_proof_validate_v1(uuid,text,uuid,integer,text,text,text,text,timestamptz) is
  'Validates returned-paper manifest/page authority and the main-page TSQ1 token without retaining the raw token.';

commit;
