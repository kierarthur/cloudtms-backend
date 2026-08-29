-- Candidate weekly entry context authority.
--
-- This replacement preserves the existing HTTP operation and database RPC
-- signatures. It extends the Candidate detail response with only the
-- non-economic contract configuration the app requires to render weekly
-- entry without guessing.

\set ON_ERROR_STOP on

begin;

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

alter function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  owner to postgres;
revoke all on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  from public,anon,authenticated;
grant execute on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)
  to service_role;

comment on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz) is
  'Candidate detail v1 plus server-resolved adaptive break and closed weekly-entry contexts.';

commit;
