-- Daily Validation format compatibility, zero-shift declaration authority,
-- and the office-owned break-entry presentation resolver.

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
  v_as_of date:=coalesce(p_as_of_date,(statement_timestamp() at time zone 'Europe/London')::date);
  v_client public.clients%rowtype;
  v_contract public.contracts%rowtype;
  v_settings public.client_settings%rowtype;
  v_is_nhsp boolean;
  v_autoprocess_hr boolean;
  v_no_timesheet_required boolean;
  v_applicable boolean;
  v_mode public.timesheet_break_entry_mode_enum;
  v_source text;
  v_reason text;
begin
  if p_client_id is null then
    raise exception 'BREAK_ENTRY_CLIENT_REQUIRED' using errcode='22023';
  end if;
  if p_as_of_date is not null and (p_as_of_date<'2000-01-01'::date or p_as_of_date>'2200-12-31'::date) then
    raise exception 'BREAK_ENTRY_AS_OF_DATE_INVALID' using errcode='22023';
  end if;

  select * into v_client from public.clients where id=p_client_id;
  if not found then raise exception 'CLIENT_OR_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;

  if p_contract_id is not null then
    select * into v_contract from public.contracts where id=p_contract_id;
    if not found then raise exception 'CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
    if v_contract.client_id is distinct from p_client_id then
      raise exception 'CONTRACT_CLIENT_MISMATCH' using errcode='22023';
    end if;
  end if;

  select * into v_settings
  from public.client_settings cs
  where cs.client_id=p_client_id
    and (cs.effective_from is null or cs.effective_from<=v_as_of)
  order by cs.effective_from desc nulls last,cs.updated_at desc nulls last,cs.id desc
  limit 1;
  if not found then raise exception 'CLIENT_OR_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;

  v_is_nhsp:=case when p_contract_id is not null and coalesce(v_contract.overrideclientsettings,false)
      and v_contract.is_nhsp is not null then v_contract.is_nhsp else coalesce(v_settings.is_nhsp,false) end;
  v_autoprocess_hr:=case when p_contract_id is not null and coalesce(v_contract.overrideclientsettings,false)
      and v_contract.autoprocess_hr is not null then v_contract.autoprocess_hr else coalesce(v_settings.autoprocess_hr,false) end;
  v_no_timesheet_required:=case when p_contract_id is not null and coalesce(v_contract.overrideclientsettings,false)
      and v_contract.no_timesheet_required is not null then v_contract.no_timesheet_required else coalesce(v_settings.no_timesheet_required,false) end;

  v_applicable:=v_autoprocess_hr and not v_no_timesheet_required and not v_is_nhsp;
  if v_applicable then
    if p_contract_id is not null and coalesce(v_contract.overrideclientsettings,false)
       and v_contract.timesheet_break_entry_mode is not null then
      v_mode:=v_contract.timesheet_break_entry_mode;
      v_source:='CONTRACT_OVERRIDE';
    else
      v_mode:=coalesce(v_settings.timesheet_break_entry_mode,'START_END_TIMES'::public.timesheet_break_entry_mode_enum);
      v_source:='CLIENT_SETTINGS';
    end if;
    v_reason:='VALIDATION_TIMESHEETS';
  else
    v_mode:=null;
    v_source:='NOT_APPLICABLE';
    v_reason:=case when v_is_nhsp then 'DEDICATED_NHSP_WEEKLY'
      when v_autoprocess_hr and v_no_timesheet_required then 'IMPORT_AUTHORITATIVE_ROSTER'
      else 'MANUAL_TIMESHEETS' end;
  end if;

  return jsonb_build_object(
    'applicable',v_applicable,
    'mode',v_mode,
    'source',v_source,
    'reason',v_reason,
    'settings_as_of_date',v_as_of,
    'client_settings_id',v_settings.id,
    'contract_id',p_contract_id,
    'contract_override_active',coalesce(v_contract.overrideclientsettings,false)
  );
end
$function$;

alter function public.timesheet_break_entry_effective_get_v1(uuid,uuid,date) owner to postgres;
revoke all on function public.timesheet_break_entry_effective_get_v1(uuid,uuid,date) from public,anon,authenticated;
grant execute on function public.timesheet_break_entry_effective_get_v1(uuid,uuid,date) to service_role;

create or replace function public.daily_zero_shifts_review_create_v1(
  p_client_id uuid,
  p_coverage_start_date date,
  p_coverage_end_date date,
  p_actor_user_id uuid,
  p_tz_assumption text default 'Europe/London'
)
returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_today date:=(statement_timestamp() at time zone 'Europe/London')::date;
  v_client public.clients%rowtype;
  v_authority record;
  v_declaration jsonb;
  v_declaration_hash text;
  v_operation_key text;
  v_parser_version constant text:='IMPORT_REVIEW_DB_V1:HR_DAILY:ZERO_DECLARATION_V1';
  v_import_id uuid;
  v_existing public.hr_imports%rowtype;
  v_existing_state public.import_review_states%rowtype;
  v_target_count integer;
  v_result jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_client_id is null or p_coverage_start_date is null or p_coverage_end_date is null
     or p_coverage_start_date>p_coverage_end_date
     or p_coverage_end_date-p_coverage_start_date>365 then
    raise exception 'DAILY_ZERO_DECLARATION_DATE_RANGE_INVALID' using errcode='22023',
      detail=jsonb_build_object('maximum_inclusive_days',366)::text;
  end if;
  if btrim(coalesce(p_tz_assumption,''))<>'Europe/London' then
    raise exception 'DAILY_ZERO_DECLARATION_TIMEZONE_INVALID' using errcode='22023';
  end if;

  select * into v_client from public.clients where id=p_client_id;
  if not found then raise exception 'DAILY_ZERO_DECLARATION_CLIENT_NOT_FOUND' using errcode='P0002'; end if;

  select * into v_authority
  from public._import_review_effective_authority_core_v1('HR_DAILY',null,p_client_id,v_today);
  if not coalesce(v_authority.route_eligible,false)
     or not coalesce(v_authority.validation_eligible,false) then
    raise exception 'DAILY_ZERO_DECLARATION_CLIENT_INELIGIBLE' using errcode='22023';
  end if;

  v_declaration:=jsonb_build_object(
    'schema','DAILY_ZERO_DECLARATION_V1',
    'source_system','HEALTHROSTER_DAILY',
    'import_scope','HR_DAILY',
    'client_id',p_client_id,
    'coverage_start_date',p_coverage_start_date,
    'coverage_end_date',p_coverage_end_date,
    'coverage_mode','COMPLETE_ALL',
    'tz_assumption','Europe/London'
  );
  v_declaration_hash:=public._import_review_hash_v1(v_declaration::text);
  v_operation_key:='daily-zero:'||v_declaration_hash;

  perform pg_advisory_xact_lock(hashtextextended(v_operation_key,22082026));
  select * into v_existing from public.hr_imports
  where coverage_operation_key=v_operation_key for update;
  if found then
    select * into v_existing_state from public.import_review_states where import_id=v_existing.id;
    if not found then raise exception 'DAILY_ZERO_DECLARATION_REPLAY_INCOMPLETE' using errcode='55000'; end if;
    return jsonb_build_object(
      'ok',true,'replay',true,'import_id',v_existing.id,
      'status',v_existing_state.status,'state_version',v_existing_state.state_version,
      'coverage_fingerprint',v_existing.coverage_fingerprint,
      'input_format','ZERO_SHIFTS','declared_zero_shifts',true
    );
  end if;

  select count(distinct t.timesheet_id) into v_target_count
  from public.v_timesheets_daily_match t
  join public.timesheets ts on ts.timesheet_id=t.timesheet_id
    and ts.is_current and ts.revoked_at is null
  where t.client_id=p_client_id
    and t.sheet_scope::text='DAILY'
    and (t.worked_start_iso at time zone 'Europe/London')::date
      between p_coverage_start_date and p_coverage_end_date;
  if coalesce(v_target_count,0)>500 then
    raise exception 'DAILY_ZERO_DECLARATION_SCOPE_TOO_LARGE' using errcode='54000',
      detail=jsonb_build_object('supported_maximum',500,'target_count',v_target_count)::text;
  end if;

  v_import_id:=gen_random_uuid();
  insert into public.hr_imports(
    id,filename,uploaded_by,uploaded_at_utc,tz_assumption,parse_summary_json,
    source_system,file_r2_key,client_id,import_scope,source_file_sha256,parser_version
  ) values (
    v_import_id,
    'No shifts declared · '||p_coverage_start_date::text||' to '||p_coverage_end_date::text,
    p_actor_user_id,now(),'Europe/London',
    jsonb_build_object(
      'status','PARSED','rows_total',0,'rows_parsed',0,'rows_skipped',0,
      'notes','No shifts declared by the office user','header_rows','[]'::jsonb,
      'header_columns','[]'::jsonb,'input_format','ZERO_SHIFTS',
      'declared_zero_shifts',true,'declaration_schema','DAILY_ZERO_DECLARATION_V1',
      'declaration_hash',v_declaration_hash,'target_timesheet_count',coalesce(v_target_count,0)
    ),
    'HEALTHROSTER_DAILY'::public.hr_source_enum,null,p_client_id,'HR_DAILY',
    v_declaration_hash,v_parser_version
  );

  v_result:=public._import_review_create_core_v2(
    v_import_id,'COMPLETE_ALL',p_coverage_start_date,p_coverage_end_date,
    jsonb_build_array(jsonb_build_object(
      'source_client_key','client:'||p_client_id::text,
      'source_display_label',v_client.name,
      'client_id',p_client_id
    )),
    '[]'::jsonb,v_declaration_hash,v_parser_version,p_actor_user_id,v_operation_key,null,null
  );

  return v_result||jsonb_build_object(
    'ok',true,'import_id',v_import_id,'input_format','ZERO_SHIFTS',
    'declared_zero_shifts',true,'target_timesheet_count',coalesce(v_target_count,0),
    'settings_as_of_date',v_today
  );
end
$function$;

alter function public.daily_zero_shifts_review_create_v1(uuid,date,date,uuid,text) owner to postgres;
revoke all on function public.daily_zero_shifts_review_create_v1(uuid,date,date,uuid,text) from public,anon,authenticated;
grant execute on function public.daily_zero_shifts_review_create_v1(uuid,date,date,uuid,text) to service_role;
