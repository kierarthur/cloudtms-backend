-- Candidate Weekly printed-document claims use the same configured break-entry
-- authority as editable Electronic claims. Reassert the later Daily projection
-- first: changing the historical 23082026 owner must never restore its older
-- Contract-only detail reader over the no-contract Daily authority.

\set ON_ERROR_STOP on

\ir 30082026_1531_candidate_paper_return_proof_transaction_boundary_reassert_v1.sql

begin;

-- Reassert only the later Daily-aware public readers that depend on the core
-- helper below. Do not replay the whole 28082026 file: it also owns unrelated
-- private action authority that has newer successors.
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
  v_contract_id uuid;
  v_week_ending_date date;
  v_context jsonb;
  v_weekly_context jsonb;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_global public.settings_defaults%rowtype;
  v_additional_definitions jsonb:='[]'::jsonb;
  v_current_week_units jsonb:='[]'::jsonb;
  v_current_day_units jsonb:='[]'::jsonb;
  v_bh_dates jsonb:='[]'::jsonb;
  v_hours_entry_mode text;
begin
  v_result:=public.candidate_app_timesheet_detail_v1(
    p_session_id,p_environment,p_timesheet_id,p_contract_week_id,p_workflow_id,p_now_utc
  );
  v_timesheet_id:=nullif(v_result#>>'{timesheet,id}','')::uuid;
  v_contract_week_id:=nullif(v_result#>>'{contract_week,id}','')::uuid;
  v_contract_id:=nullif(v_result#>>'{capabilities,contract_id}','')::uuid;
  v_week_ending_date:=nullif(v_result#>>'{contract_week,week_ending_date}','')::date;

  if v_result#>>'{timesheet,sheet_scope}'='DAILY' then
    v_context:=private._candidate_daily_read_projection_v1(p_environment,
      (v_result#>>'{capabilities,candidate_id}')::uuid,v_timesheet_id,p_now_utc);
    return v_result||jsonb_build_object('break_entry',v_context->'break_entry',
      'weekly_entry',null);
  end if;
  v_context:=private._candidate_break_entry_context_core_v1(
    v_timesheet_id,v_contract_week_id,
    (p_now_utc at time zone 'Europe/London')::date,
    coalesce(v_result->'capabilities','{}'::jsonb)
  );

  if v_contract_id is null then
    raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002';
  end if;
  select * into v_contract from public.contracts where id=v_contract_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;

  if v_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_timesheet_id;
  end if;
  select * into v_global from public.settings_defaults where id=1;
  if not found then raise exception 'CANDIDATE_GLOBAL_SETTINGS_MISSING' using errcode='55000'; end if;

  if jsonb_typeof(v_contract.additional_rates_json)='array' then
    select coalesce(jsonb_agg(jsonb_build_object(
      'code',upper(btrim(rate->>'code')),
      'label',coalesce(nullif(btrim(rate->>'bucket_name'),''),upper(btrim(rate->>'code'))),
      'unit_name',coalesce(nullif(btrim(rate->>'unit_name'),''),'unit'),
      'frequency',case upper(coalesce(rate->>'frequency',''))
        when 'ONE_PER_DAY' then 'ONE_PER_DAY'
        when 'WEEKENDS_AND_BH_ONLY' then 'WEEKENDS_AND_BH_ONLY'
        when 'WEEKDAYS_EXCL_BH_ONLY' then 'WEEKDAYS_EXCL_BH_ONLY'
        else 'ONE_PER_WEEK'
      end
    ) order by ord),'[]'::jsonb)
    into v_additional_definitions
    from jsonb_array_elements(v_contract.additional_rates_json) with ordinality as configured(rate,ord)
    where jsonb_typeof(rate)='object'
      and upper(btrim(coalesce(rate->>'code',''))) ~ '^[A-Z0-9_]{1,40}$';
  end if;

  if jsonb_typeof(v_timesheet.additional_units_week)='object' then
    select coalesce(jsonb_agg(jsonb_build_object(
      'code',upper(btrim(code)),'value',quantity::numeric
    ) order by upper(btrim(code))),'[]'::jsonb)
    into v_current_week_units
    from jsonb_each(v_timesheet.additional_units_week) configured(code,raw_value)
    cross join lateral (select case
      when jsonb_typeof(raw_value)='number' then raw_value#>>'{}'
      when jsonb_typeof(raw_value)='object' then coalesce(
        raw_value->>'unit_count',raw_value->>'quantity',raw_value->>'units'
      )
      else null end as quantity) parsed
    where upper(btrim(code)) ~ '^[A-Z0-9_]{1,40}$'
      and quantity ~ '^\d+(\.\d+)?$'
      and quantity::numeric between 0 and 1000000;
  end if;

  if jsonb_typeof(v_timesheet.additional_units_per_day)='object' then
    select coalesce(jsonb_agg(jsonb_build_object(
      'date',work_date,'code',upper(btrim(code)),'value',quantity::numeric
    ) order by work_date,upper(btrim(code))),'[]'::jsonb)
    into v_current_day_units
    from jsonb_each(v_timesheet.additional_units_per_day) configured_days(work_date,day_values)
    cross join lateral jsonb_each(case when jsonb_typeof(day_values)='object' then day_values else '{}'::jsonb end)
      configured_units(code,raw_value)
    cross join lateral (select case
      when jsonb_typeof(raw_value)='number' then raw_value#>>'{}'
      when jsonb_typeof(raw_value)='object' then coalesce(
        raw_value->>'unit_count',raw_value->>'quantity',raw_value->>'units'
      )
      else null end as quantity) parsed
    where work_date ~ '^\d{4}-\d{2}-\d{2}$'
      and upper(btrim(code)) ~ '^[A-Z0-9_]{1,40}$'
      and quantity ~ '^\d+(\.\d+)?$'
      and quantity::numeric between 0 and 1000000;
  end if;

  if v_week_ending_date is not null and jsonb_typeof(v_global.bh_list)='array' then
    select coalesce(jsonb_agg(value order by value),'[]'::jsonb)
    into v_bh_dates
    from jsonb_array_elements_text(v_global.bh_list) as holiday(value)
    where value ~ '^\d{4}-\d{2}-\d{2}$'
      and value::date between v_week_ending_date-6 and v_week_ending_date;
  end if;

  v_hours_entry_mode:=case
    when coalesce((v_result#>>'{capabilities,import_authoritative}')::boolean,false)
      then 'VIEW_ONLY_IMPORT'
    when coalesce(v_contract.is_ad_hoc,false)
      then 'AD_HOC_BLANK'
    else 'PLANNED_PREFILL'
  end;

  v_weekly_context:=jsonb_build_object(
    'schema_version','CANDIDATE_WEEKLY_ENTRY_CONTEXT_V1',
    'hours_entry_mode',v_hours_entry_mode,
    'prepopulate_planned_schedule',v_hours_entry_mode='PLANNED_PREFILL',
    'additional_units_enabled',jsonb_array_length(v_additional_definitions)>0,
    'additional_unit_definitions',v_additional_definitions,
    'bank_holiday_dates',v_bh_dates,
    'current_additional_units_week',v_current_week_units,
    'current_additional_units_per_day',v_current_day_units
  );
  v_weekly_context:=v_weekly_context||jsonb_build_object(
    'context_sha256',encode(
      extensions.digest(convert_to(v_weekly_context::text,'UTF8'),'sha256'),'hex'
    )
  );

  return v_result||jsonb_build_object(
    'break_entry',v_context,
    'weekly_entry',v_weekly_context
  );
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
  if v_workflow.workflow_kind='DAILY' then
    v_capabilities:=private._candidate_daily_read_projection_v1(
      p_environment,v_candidate_id,v_workflow.target_timesheet_id,p_now_utc);
    return (v_capabilities->'break_entry')||jsonb_build_object(
      'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation);
  end if;
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
    and coalesce(p_capabilities->>'route_family','') in ('ELECTRONIC','PAPER')
    and not coalesce((v_resolution->>'is_nhsp')::boolean,false)
    and not coalesce((v_resolution->>'no_timesheet_required')::boolean,false);
  v_reason:=case
    when v_applicable then 'CANDIDATE_EDITABLE_ELECTRONIC'
    when coalesce((p_capabilities->>'import_authoritative')::boolean,false)
      then 'IMPORT_AUTHORITATIVE'
    when coalesce((v_resolution->>'is_nhsp')::boolean,false) then 'NHSP'
    when coalesce((v_resolution->>'no_timesheet_required')::boolean,false)
      then 'NO_TIMESHEET_REQUIRED'
    when coalesce(p_capabilities->>'route_family','') not in ('ELECTRONIC','PAPER')
      then 'NON_ELECTRONIC_ROUTE'
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

alter function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb)
  owner to postgres;
revoke all on function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb)
  from public,anon,authenticated,service_role;

alter function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  owner to postgres;
revoke all on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  from public,anon,authenticated;
grant execute on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  to service_role;

alter function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz)
  owner to postgres;
revoke all on function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz)
  from public,anon,authenticated;
grant execute on function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz)
  to service_role;

notify pgrst, 'reload schema';

commit;
