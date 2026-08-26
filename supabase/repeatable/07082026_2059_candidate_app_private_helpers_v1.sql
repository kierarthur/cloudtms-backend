-- Candidate App private helper authority.
-- Install after the two 07082026_2055 Candidate App migrations.

create or replace function private._candidate_normalize_email(p_email text)
returns text
language sql
immutable
parallel safe
set search_path = pg_catalog
as $function$
  select lower(btrim(coalesce(p_email,'')));
$function$;

create or replace function private._candidate_normalize_domain_v1(p_domain text)
returns text
language sql
immutable
parallel safe
set search_path = pg_catalog
as $function$
  select lower(regexp_replace(btrim(coalesce(p_domain,'')), '^@', ''));
$function$;

create or replace function private._candidate_normalize_domain_array_v1(p_domains jsonb)
returns jsonb
language sql
immutable
parallel safe
set search_path = pg_catalog, private
as $function$
  select coalesce(jsonb_agg(domain order by domain),'[]'::jsonb)
  from (
    select distinct private._candidate_normalize_domain_v1(value) as domain
    from jsonb_array_elements_text(case when jsonb_typeof(p_domains)='array' then p_domains else '[]'::jsonb end)
    where private._candidate_normalize_domain_v1(value)<>''
  ) normalized;
$function$;

create or replace function private._candidate_normalize_manager_policy_v1(p_policy jsonb)
returns jsonb
language sql
immutable
parallel safe
set search_path = pg_catalog, private
as $function$
  select coalesce(case when jsonb_typeof(p_policy)='object' then p_policy else '{}'::jsonb end,'{}'::jsonb)
    ||jsonb_build_object(
      'approved_emails',coalesce((
        select jsonb_agg(email order by email)
        from (
          select distinct private._candidate_normalize_email(value) email
          from jsonb_array_elements_text(case when jsonb_typeof(p_policy->'approved_emails')='array'
            then p_policy->'approved_emails' else '[]'::jsonb end)
          where private._candidate_normalize_email(value)<>''
        ) emails
      ),'[]'::jsonb),
      'approved_domains',private._candidate_normalize_domain_array_v1(p_policy->'approved_domains')
    );
$function$;

create or replace function private._candidate_daily_work_date_v1(
  p_worked_start_iso timestamptz,
  p_scheduled_start_iso timestamptz default null,
  p_fallback_date date default null
)
returns date
language sql
immutable
parallel safe
set search_path = pg_catalog
as $function$
  select coalesce(
    (p_worked_start_iso at time zone 'Europe/London')::date,
    (p_scheduled_start_iso at time zone 'Europe/London')::date,
    p_fallback_date
  );
$function$;

create or replace function private._candidate_daily_entitled_v1(p_candidate_id uuid)
returns boolean
language sql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select exists(
    select 1
    from public.candidates candidate
    where candidate.id=p_candidate_id
      and candidate.active=true
      and nullif(btrim(coalesce(candidate.key_norm,'')),'') is not null
  );
$function$;

create or replace function private._candidate_json_numeric_sum(p_value jsonb)
returns numeric
language sql
immutable
parallel safe
set search_path = pg_catalog
as $function$
  select coalesce(sum((value_item #>> '{}')::numeric),0::numeric)
  from jsonb_path_query(coalesce(p_value,'{}'::jsonb), '$.** ? (@.type() == "number")') value_item;
$function$;

create or replace function private._candidate_submission_mode_v1(
  p_client_id uuid,
  p_contract_id uuid,
  p_as_of_date date
)
returns public.submission_mode_enum
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_contract public.contracts%rowtype;
  v_client_mode public.submission_mode_enum;
begin
  select * into v_contract
  from public.contracts
  where id=p_contract_id and client_id=p_client_id;
  if not found then
    raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002';
  end if;

  select cs.default_submission_mode into v_client_mode
  from public.client_settings cs
  where cs.client_id=p_client_id
    and (cs.effective_from is null or cs.effective_from<=coalesce(p_as_of_date,current_date))
  order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc
  limit 1;

  return coalesce(
    case when coalesce(v_contract.overrideclientsettings,false)
      then v_contract.default_submission_mode end,
    v_client_mode,
    'ELECTRONIC'::public.submission_mode_enum
  );
end;
$function$;

create or replace function private._candidate_import_authoritative_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_timesheet_id uuid default null,
  p_snapshot_json jsonb default null,
  p_evaluation_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_evaluation_date date:=coalesce(p_evaluation_date,(current_timestamp at time zone 'Europe/London')::date);
  v_client public.client_settings%rowtype;
  v_contract public.contracts%rowtype;
  v_fin jsonb:=coalesce(p_snapshot_json,'{}'::jsonb);
  v_client_import boolean:=false;
  v_contract_import boolean:=false;
  v_snapshot_import boolean:=false;
  v_has_external_source_rows boolean:=false;
  v_route_import boolean:=false;
  v_route_type text:='';
  v_route_no_timesheet_required boolean:=false;
  v_source text:='NONE';
begin
  if p_client_id is null then
    raise exception 'CANDIDATE_IMPORT_CLIENT_REQUIRED' using errcode='22023';
  end if;
  select * into v_client from public.client_settings cs
  where cs.client_id=p_client_id
    and (cs.effective_from is null or cs.effective_from<=v_evaluation_date)
  order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc
  limit 1;
  v_client_import:=coalesce(v_client.is_nhsp,false)
    or (coalesce(v_client.requires_hr,false) and coalesce(v_client.no_timesheet_required,false));
  if p_contract_id is not null then
    select * into v_contract from public.contracts c
    where c.id=p_contract_id and c.client_id=p_client_id;
    if not found then raise exception 'CANDIDATE_IMPORT_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
    v_contract_import:=coalesce(v_contract.weekly_timesheet_source::text,'NONE')<>'NONE';
  end if;
  if p_snapshot_json is null and p_timesheet_id is not null then
    select to_jsonb(tf) into v_fin from public.timesheets_financials tf
    where tf.timesheet_id=p_timesheet_id and tf.is_current=true
    order by tf.computed_at_utc desc nulls last,tf.updated_at desc,tf.id desc limit 1;
    v_fin:=coalesce(v_fin,'{}'::jsonb);
  end if;
  if p_timesheet_id is not null then
    begin
      select upper(coalesce(summary.route_type,'')),
        coalesce(summary.client_no_timesheet_required,false)
      into v_route_type,v_route_no_timesheet_required
      from public.v_timesheets_summary summary
      where summary.timesheet_id=p_timesheet_id
      limit 1;
    exception when others then
      v_route_type:='';
      v_route_no_timesheet_required:=false;
    end;
  end if;
  v_route_import:=v_route_type in ('WEEKLY_NHSP','WEEKLY_NHSP_ADJUSTMENT')
    or (v_route_type='WEEKLY_HEALTHROSTER' and v_route_no_timesheet_required);
  v_has_external_source_rows:=case
    when jsonb_typeof(v_fin->'external_source_rows_json')='array'
      then jsonb_array_length(v_fin->'external_source_rows_json')>0
    when jsonb_typeof(v_fin->'external_source_rows_json')='object'
      then v_fin->'external_source_rows_json'<>'{}'::jsonb
    else false end;
  v_snapshot_import:=nullif(v_fin->>'nhsp_import_id','') is not null
    or v_has_external_source_rows
    or upper(coalesce(v_fin->>'basis','')) in (
      'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_SELF_BILL_ADJUSTMENT',
      'HEALTHROSTER_ADJUSTMENT','HEALTHROSTER_WEEKLY','HEALTHROSTER_WEEKLY_ADJUSTMENT'
    );
  v_source:=case
    when coalesce(v_client.is_nhsp,false) then 'NHSP_CLIENT'
    when coalesce(v_client.requires_hr,false) and coalesce(v_client.no_timesheet_required,false) then 'HEALTHROSTER_CREATE_CLIENT'
    when v_contract_import then 'CONTRACT_SOURCE_'||upper(v_contract.weekly_timesheet_source::text)
    when v_route_import then 'ROUTE_'||v_route_type
    when nullif(v_fin->>'nhsp_import_id','') is not null then 'NHSP_IMPORT_SNAPSHOT'
    when v_has_external_source_rows then 'EXTERNAL_SOURCE_SNAPSHOT'
    when v_snapshot_import then 'IMPORT_BASIS_SNAPSHOT'
    else 'NONE' end;
  return jsonb_build_object(
    'is_import_authoritative',v_client_import or v_contract_import or v_route_import or v_snapshot_import,
    'source_family',v_source,
    'candidate_hours_view_only',v_client_import or v_contract_import or v_route_import or v_snapshot_import,
    'mandatory_expense_separation',v_client_import or v_contract_import or v_route_import or v_snapshot_import
  );
end;
$function$;

create or replace function private._candidate_route_family_v1(
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_policy jsonb;
  v_effective_mode public.submission_mode_enum;
  v_import boolean:=false;
  v_qr_backed boolean:=false;
  v_family text;
  v_paper_fallback boolean:=false;
  v_is_daily boolean:=false;
  v_import_authority jsonb;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_RECORD_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
    if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  end if;
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select week_row.* into v_week
    from public.contract_weeks week_row
    where week_row.timesheet_id=p_timesheet_id
    order by week_row.updated_at desc,week_row.id desc
    limit 1;
  end if;
  if v_week.id is null then
    -- DAILY rows have no contract-week route snapshot.  Their contract and the
    -- constrained pending route-intent column are the complete authority.
    if v_timesheet.timesheet_id is null
       or v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum
       or v_timesheet.contract_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  else
    select * into v_contract from public.contracts where id=v_week.contract_id;
  end if;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
  if v_timesheet.timesheet_id is null and v_week.timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_week.timesheet_id;
  end if;
  if v_timesheet.timesheet_id is not null then
    select * into v_fin
    from public.timesheets_financials financial_row
    where financial_row.timesheet_id=v_timesheet.timesheet_id and financial_row.is_current=true
    order by financial_row.computed_at_utc desc nulls last,financial_row.updated_at desc,financial_row.id desc
    limit 1;
  end if;
  v_policy:=private._candidate_policy_resolve_v1(
    v_contract.client_id,v_contract.id,
    coalesce(v_week.week_ending_date,v_timesheet.week_ending_date,
      private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
      ))
  );
  -- A fresh electronic resubmission cannot be stored as an unsigned
  -- ELECTRONIC timesheet because the canonical two-signature constraint is
  -- intentionally unchanged.  Its authoritative route therefore comes from
  -- the ELECTRONIC contract-week snapshot until finalisation writes both
  -- signatures and switches the canonical row atomically.
  v_effective_mode:=case
    when v_timesheet.submission_mode='ELECTRONIC'::public.submission_mode_enum
      or v_timesheet.candidate_submission_route_intent='ELECTRONIC'
      or v_week.submission_mode_snapshot='ELECTRONIC'::public.submission_mode_enum
      then 'ELECTRONIC'::public.submission_mode_enum
    else coalesce(
      v_timesheet.submission_mode,
      v_week.submission_mode_snapshot,
      private._candidate_submission_mode_v1(
        v_contract.client_id,v_contract.id,
        coalesce(v_week.week_ending_date,v_timesheet.week_ending_date,
          private._candidate_daily_work_date_v1(
            v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
          ))
      )
    ) end;
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,v_timesheet.timesheet_id,to_jsonb(v_fin),
    coalesce(v_week.week_ending_date,v_timesheet.week_ending_date,
      private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
      ))
  );
  v_import:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);
  v_qr_backed:=v_timesheet.qr_status is not null
    or v_timesheet.qr_token is not null
    or v_timesheet.qr_r2_key is not null
    or exists(
      select 1
      from public.candidate_submission_workflows workflow
      where workflow.target_timesheet_id=v_timesheet.timesheet_id
        and workflow.route='PAPER'
        and workflow.state not in ('CANCELLED','REJECTED','REFUSED','EXPIRED','SUPERSEDED')
    );
  v_is_daily:=v_timesheet.timesheet_id is not null
    and v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum;
  -- Candidate PAPER/QR is a WEEKLY route only.  Existing office DAILY QR
  -- facts may still classify a legacy row as QR, but they never grant a
  -- Candidate paper mutation capability.
  v_paper_fallback:=not v_is_daily
    and coalesce((v_policy->>'paper_submission_enabled')::boolean,false);
  v_family:=case
    when v_import then 'IMPORT_AUTHORITATIVE'
    when v_qr_backed then 'QR'
    when v_effective_mode='ELECTRONIC' then 'ELECTRONIC'
    when v_timesheet.timesheet_id is null and v_paper_fallback then 'QR'
    else 'MANUAL_NON_QR'
  end;
  return jsonb_build_object(
    'route_family',v_family,
    'effective_submission_mode',v_effective_mode,
    'pending_route_intent',v_timesheet.candidate_submission_route_intent,
    'import_authoritative',v_import,
    'import_source_family',v_import_authority->>'source_family',
    'qr_backed',v_qr_backed,
    'electronic_paper_fallback_enabled',v_family='ELECTRONIC' and v_paper_fallback,
    'candidate_hours_submission_allowed',v_family='ELECTRONIC'
      or (v_family='QR' and not v_is_daily),
    'candidate_expenses_allowed',v_family in ('ELECTRONIC','QR','IMPORT_AUTHORITATIVE'),
    'candidate_paper_submission_allowed',not v_is_daily
      and (v_family='QR' or (v_family='ELECTRONIC' and v_paper_fallback)),
    'candidate_no_work_allowed',v_family='ELECTRONIC'
      or (v_family='QR' and not v_is_daily),
    'policy',v_policy
  );
end;
$function$;

create or replace function private._candidate_week_schedule_from_template_v1(
  p_template jsonb,
  p_week_ending_date date,
  p_contract_start date,
  p_contract_end date
)
returns jsonb
language plpgsql
immutable
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_date date;
  v_key text;
  v_day jsonb;
  v_start_text text;
  v_end_text text;
  v_start time;
  v_end time;
  v_break_minutes numeric;
  v_minutes integer;
  v_result jsonb:='[]'::jsonb;
begin
  if p_week_ending_date is null or p_contract_start is null or p_contract_end is null then
    raise exception 'CANDIDATE_WEEK_SCHEDULE_INPUT_INVALID' using errcode='22023';
  end if;
  if coalesce(jsonb_typeof(p_template),'null')<>'object' then
    return v_result;
  end if;

  for v_date in
    select g::date
    from generate_series(p_week_ending_date-6,p_week_ending_date,interval '1 day') g
    where g::date between p_contract_start and p_contract_end
    order by g
  loop
    v_key:=case extract(dow from v_date)::integer
      when 0 then 'sun' when 1 then 'mon' when 2 then 'tue' when 3 then 'wed'
      when 4 then 'thu' when 5 then 'fri' else 'sat' end;
    v_day:=p_template->v_key;
    if v_day is null or jsonb_typeof(v_day)<>'object' then continue; end if;
    v_start_text:=nullif(btrim(v_day->>'start'),'');
    v_end_text:=nullif(btrim(v_day->>'end'),'');
    if v_start_text is null or v_end_text is null
       or v_start_text!~'^(?:[01]?[0-9]|2[0-3]):[0-5][0-9]$'
       or v_end_text!~'^(?:[01]?[0-9]|2[0-3]):[0-5][0-9]$' then
      raise exception 'CANDIDATE_WEEK_SCHEDULE_TIME_INVALID'
        using errcode='22023',detail=jsonb_build_object('weekday',v_key)::text;
    end if;
    v_start:=v_start_text::time;
    v_end:=v_end_text::time;
    begin
      v_break_minutes:=greatest(coalesce(nullif(v_day->>'break_minutes','')::numeric,0),0);
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'CANDIDATE_WEEK_SCHEDULE_BREAK_INVALID'
        using errcode='22023',detail=jsonb_build_object('weekday',v_key)::text;
    end;
    v_minutes:=round(extract(epoch from (v_end-v_start))/60)::integer;
    if v_minutes<=0 then v_minutes:=v_minutes+1440; end if;
    v_minutes:=greatest(v_minutes-round(v_break_minutes)::integer,0);
    v_result:=v_result||jsonb_build_array(jsonb_build_object(
      'date',v_date,'start',v_start_text,'end',v_end_text,
      'breaks',case when jsonb_typeof(v_day->'breaks')='array' then v_day->'breaks' else '[]'::jsonb end,
      'break_minutes',v_break_minutes,'overnight',v_end<=v_start,'expected_minutes',v_minutes
    ));
  end loop;
  return v_result;
end;
$function$;

create or replace function private._candidate_assert_environment(p_environment text)
returns text
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_requested text := upper(btrim(coalesce(p_environment,'')));
  v_configured text;
begin
  if v_requested not in ('TEST','LIVE') then
    raise exception 'CANDIDATE_ENVIRONMENT_INVALID'
      using errcode='22023', detail=jsonb_build_object('code','CANDIDATE_ENVIRONMENT_INVALID')::text;
  end if;

  select upper(btrim(candidate_app_environment))
  into v_configured
  from public.settings_defaults
  where id=1;

  if v_configured is null then
    raise exception 'CANDIDATE_ENVIRONMENT_NOT_CONFIGURED'
      using errcode='55000', detail=jsonb_build_object('code','CANDIDATE_ENVIRONMENT_NOT_CONFIGURED')::text;
  end if;

  if v_requested is distinct from v_configured then
    raise exception 'CANDIDATE_ENVIRONMENT_MISMATCH'
      using errcode='28000', detail=jsonb_build_object(
        'code','CANDIDATE_ENVIRONMENT_MISMATCH',
        'requested_environment',v_requested,
        'configured_environment',v_configured
      )::text;
  end if;

  return v_requested;
end;
$function$;

create or replace function private._candidate_feature_enabled_v1(
  p_environment text,
  p_feature_key text
)
returns boolean
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_flags jsonb;
  v_key text := btrim(coalesce(p_feature_key,''));
begin
  v_environment := private._candidate_assert_environment(p_environment);
  if v_key='' then
    raise exception 'CANDIDATE_FEATURE_KEY_REQUIRED' using errcode='22023';
  end if;

  select candidate_app_feature_flags_json into v_flags
  from public.settings_defaults where id=1;

  return coalesce((v_flags->>v_key)::boolean,false);
exception
  when invalid_text_representation then
    raise exception 'CANDIDATE_FEATURE_FLAG_INVALID'
      using errcode='22023', detail=jsonb_build_object('code','CANDIDATE_FEATURE_FLAG_INVALID','feature',v_key)::text;
end;
$function$;

create or replace function private._candidate_feature_enabled_current_v1(
  p_feature_key text
)
returns boolean
language sql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select coalesce((sd.candidate_app_feature_flags_json->>btrim(coalesce(p_feature_key,'')))::boolean,false)
  from public.settings_defaults sd
  where sd.id=1;
$function$;

create or replace function private._candidate_require_feature_v1(
  p_environment text,
  p_feature_key text
)
returns void
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
begin
  if not private._candidate_feature_enabled_v1(p_environment,p_feature_key) then
    raise exception 'CANDIDATE_FEATURE_DISABLED'
      using errcode='42501', detail=jsonb_build_object(
        'code','CANDIDATE_FEATURE_DISABLED',
        'feature',btrim(coalesce(p_feature_key,''))
      )::text;
  end if;
end;
$function$;

create or replace function private._candidate_email_eligibility_v1(
  p_environment text,
  p_email text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_email text := private._candidate_normalize_email(p_email);
  v_candidate_ids jsonb := '[]'::jsonb;
  v_match_count integer := 0;
begin
  v_environment := private._candidate_assert_environment(p_environment);

  if v_email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
    return jsonb_build_object(
      'eligible',false,
      'match_count',0,
      'candidate_ids','[]'::jsonb,
      'reason_code','EMAIL_NOT_ELIGIBLE'
    );
  end if;

  select count(*)::integer,
         coalesce(jsonb_agg(candidate_row.id order by candidate_row.id),'[]'::jsonb)
  into v_match_count,v_candidate_ids
  from public.candidates candidate_row
  where candidate_row.active=true
    and lower(btrim(coalesce(candidate_row.email,'')))=v_email;

  if v_environment='LIVE' and v_match_count>1 then
    return jsonb_build_object(
      'eligible',false,
      'match_count',v_match_count,
      'candidate_ids','[]'::jsonb,
      'reason_code','LIVE_DUPLICATE_ACTIVE_EMAIL'
    );
  end if;

  return jsonb_build_object(
    'eligible',v_match_count>0,
    'match_count',v_match_count,
    'candidate_ids',case when v_match_count>0 then v_candidate_ids else '[]'::jsonb end,
    'selection_required',v_environment='TEST' and v_match_count>1,
    'reason_code',case when v_match_count>0 then 'ELIGIBLE' else 'EMAIL_NOT_ELIGIBLE' end
  );
end;
$function$;

create or replace function private._candidate_session_context_v1(
  p_session_id uuid,
  p_environment text,
  p_expected_rotation integer default null,
  p_now_utc timestamptz default now(),
  p_lock boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_session public.candidate_app_sessions%rowtype;
  v_account public.candidate_app_accounts%rowtype;
  v_eligibility jsonb;
  v_selected_owned boolean := false;
begin
  v_environment := private._candidate_assert_environment(p_environment);
  if p_session_id is null then
    raise exception 'CANDIDATE_SESSION_REQUIRED' using errcode='28000';
  end if;

  if p_lock then
    select * into v_session from public.candidate_app_sessions
    where id=p_session_id for update;
  else
    select * into v_session from public.candidate_app_sessions
    where id=p_session_id;
  end if;

  if not found or v_session.environment<>v_environment then
    raise exception 'CANDIDATE_SESSION_INVALID' using errcode='28000';
  end if;
  if v_session.status<>'ACTIVE'
     or v_session.expires_at_utc<=p_now_utc
     or v_session.absolute_expires_at_utc<=p_now_utc then
    raise exception 'CANDIDATE_SESSION_EXPIRED' using errcode='28000';
  end if;
  if p_expected_rotation is not null and v_session.rotation<>p_expected_rotation then
    raise exception 'CANDIDATE_SESSION_ROTATION_MISMATCH'
      using errcode='40001', detail=jsonb_build_object('code','CANDIDATE_SESSION_ROTATION_MISMATCH')::text;
  end if;

  select * into v_account from public.candidate_app_accounts
  where id=v_session.account_id;
  if not found or v_account.environment<>v_environment or v_account.status<>'ACTIVE' then
    raise exception 'CANDIDATE_ACCOUNT_INACTIVE' using errcode='28000';
  end if;

  v_eligibility := private._candidate_email_eligibility_v1(v_environment,v_account.email_normalized);
  if coalesce((v_eligibility->>'eligible')::boolean,false)=false then
    raise exception 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' using errcode='28000';
  end if;

  if v_session.selected_candidate_id is not null then
    select exists(
      select 1 from public.candidates c
      where c.id=v_session.selected_candidate_id
        and c.active=true
        and lower(btrim(coalesce(c.email,'')))=v_account.email_normalized
    ) into v_selected_owned;
    if not v_selected_owned then
      raise exception 'CANDIDATE_SELECTION_NOT_ALLOWED' using errcode='28000';
    end if;
  end if;

  return jsonb_build_object(
    'session_id',v_session.id,
    'account_id',v_account.id,
    'environment',v_environment,
    'email_normalized',v_account.email_normalized,
    'selected_candidate_id',v_session.selected_candidate_id,
    'rotation',v_session.rotation,
    'session_version',v_account.session_version,
    'eligibility',v_eligibility,
    'notification_preferences',v_account.notification_preferences_json,
    'expires_at_utc',v_session.expires_at_utc,
    'absolute_expires_at_utc',v_session.absolute_expires_at_utc
  );
end;
$function$;

create or replace function private._candidate_policy_resolve_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_evaluation_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_evaluation_date date := coalesce(p_evaluation_date,(current_timestamp at time zone 'Europe/London')::date);
  v_global public.settings_defaults%rowtype;
  v_client public.client_settings%rowtype;
  v_contract public.contracts%rowtype;
  v_client_found boolean := false;
  v_contract_found boolean := false;
  v_auto boolean;
  v_auto_source text;
  v_separate boolean;
  v_separate_source text;
  v_paper boolean;
  v_paper_source text;
  v_expense_email text;
  v_import_mandatory boolean := false;
  v_expense_email_ready boolean := false;
  v_manager_policy jsonb;
  v_result jsonb;
  v_import_authority jsonb;
begin
  if p_client_id is null then
    raise exception 'CANDIDATE_POLICY_CLIENT_REQUIRED' using errcode='22023';
  end if;

  select * into v_global from public.settings_defaults where id=1;
  if not found then raise exception 'CANDIDATE_GLOBAL_SETTINGS_MISSING' using errcode='55000'; end if;

  select * into v_client from public.client_settings cs
  where cs.client_id=p_client_id
    and (cs.effective_from is null or cs.effective_from<=v_evaluation_date)
  order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc
  limit 1;
  v_client_found:=found;

  if p_contract_id is not null then
    select * into v_contract from public.contracts c where c.id=p_contract_id;
    v_contract_found:=found;
    if not v_contract_found then raise exception 'CANDIDATE_POLICY_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
    if v_contract.client_id is distinct from p_client_id then
      raise exception 'CANDIDATE_POLICY_CONTRACT_CLIENT_MISMATCH' using errcode='22023';
    end if;
  end if;

  v_import_authority:=private._candidate_import_authoritative_v1(
    p_client_id,p_contract_id,null,null,v_evaluation_date
  );
  v_import_mandatory:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);

  if v_contract_found and v_contract.candidate_electronic_auto_authorise_override is not null then
    v_auto:=v_contract.candidate_electronic_auto_authorise_override; v_auto_source:='CONTRACT';
  elsif v_client_found and v_client.candidate_electronic_auto_authorise is not null then
    v_auto:=v_client.candidate_electronic_auto_authorise; v_auto_source:='CLIENT';
  else
    v_auto:=v_global.candidate_electronic_auto_authorise_default; v_auto_source:='GLOBAL';
  end if;

  if v_import_mandatory then
    v_separate:=true; v_separate_source:='IMPORT_MANDATORY';
  elsif v_contract_found and v_contract.candidate_expenses_require_separate_timesheet_override is not null then
    v_separate:=v_contract.candidate_expenses_require_separate_timesheet_override; v_separate_source:='CONTRACT';
  elsif v_client_found then
    v_separate:=v_client.candidate_expenses_require_separate_timesheet; v_separate_source:='CLIENT';
  else
    v_separate:=false; v_separate_source:='SAFE_DEFAULT';
  end if;

  if v_contract_found and v_contract.candidate_paper_submission_enabled_override is not null then
    v_paper:=v_contract.candidate_paper_submission_enabled_override; v_paper_source:='CONTRACT';
  elsif v_client_found then
    v_paper:=v_client.candidate_paper_submission_enabled; v_paper_source:='CLIENT';
  else
    v_paper:=false; v_paper_source:='SAFE_DEFAULT';
  end if;

  v_expense_email:=nullif(btrim(case
    when v_contract_found and nullif(btrim(v_contract.candidate_expense_invoice_email_override),'') is not null
      then v_contract.candidate_expense_invoice_email_override
    when v_client_found then v_client.candidate_expense_invoice_email
    else null end),'');
  v_expense_email_ready:=v_expense_email is not null
    and char_length(v_expense_email)<=320
    and v_expense_email ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';

  v_manager_policy:=private._candidate_normalize_manager_policy_v1(coalesce(case
    when v_contract_found and upper(coalesce(v_contract.candidate_manager_approval_policy_json->>'mode','INHERIT'))<>'INHERIT'
      then v_contract.candidate_manager_approval_policy_json
    when v_client_found then v_client.candidate_manager_approval_policy_json
    else '{}'::jsonb end,'{}'::jsonb));

  v_result:=jsonb_build_object(
    'client_id',p_client_id,
    'contract_id',p_contract_id,
    'evaluation_date',v_evaluation_date,
    'candidate_electronic_auto_authorise',v_auto,
    'candidate_electronic_auto_authorise_source',v_auto_source,
    'expenses_require_separate_timesheet',v_separate,
    'expenses_require_separate_timesheet_source',v_separate_source,
    'import_expense_separation_mandatory',v_import_mandatory,
    'import_source_family',v_import_authority->>'source_family',
    'paper_submission_enabled',v_paper,
    'paper_submission_enabled_source',v_paper_source,
    'expense_invoice_email',v_expense_email,
    'expense_invoice_email_ready',v_expense_email_ready,
    'manager_approval_policy',v_manager_policy,
    'allow_daily_manager_authorise_on_phone',coalesce(v_client.allow_daily_manager_authorise_on_phone,true),
    'allow_daily_manager_authorise_by_email',coalesce(v_client.allow_daily_manager_authorise_by_email,false),
    'hours_deviation_pct',v_global.candidate_hours_deviation_pct,
    'barred_manager_email_domains',private._candidate_normalize_domain_array_v1(v_global.candidate_barred_manager_email_domains),
    'client_setting_found',v_client_found,
    'client_settings_id',case when v_client_found then v_client.id else null end,
    'contract_found',v_contract_found,
    'global_settings_updated_at',v_global.updated_at
  );

  return v_result||jsonb_build_object(
    'policy_fingerprint',encode(extensions.digest(convert_to(v_result::text,'UTF8'),'sha256'),'hex')
  );
end;
$function$;

create or replace function private._candidate_record_capabilities_v1(
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_proposed_claim jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_policy jsonb;
  v_hours numeric:=0;
  v_additional numeric:=0;
  v_expenses numeric:=0;
  v_mileage numeric:=0;
  v_travel numeric:=0;
  v_accommodation numeric:=0;
  v_other numeric:=0;
  v_import boolean:=false;
  v_protected boolean:=false;
  v_candidate_mutation_locked boolean:=false;
  v_separate boolean:=false;
  v_has_timesheet boolean:=false;
  v_role text;
  v_route jsonb;
  v_route_family text;
  v_hours_route_allowed boolean:=false;
  v_expense_route_allowed boolean:=false;
  v_paper_route_allowed boolean:=false;
  v_no_work_route_allowed boolean:=false;
  v_reasons jsonb:='[]'::jsonb;
  v_result jsonb;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_RECORD_IDENTITY_REQUIRED' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
    if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  end if;

  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select cw.* into v_week from public.contract_weeks cw
    where cw.timesheet_id=p_timesheet_id order by cw.updated_at desc,cw.id desc limit 1;
  end if;
  if v_week.id is null then
    -- DAILY is timesheet-owned and intentionally has no contract_weeks row.
    if v_timesheet.timesheet_id is null
       or v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum
       or v_timesheet.contract_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  else
    select * into v_contract from public.contracts where id=v_week.contract_id;
  end if;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;

  if v_timesheet.timesheet_id is null and v_week.timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_week.timesheet_id;
  end if;

  if v_timesheet.timesheet_id is not null then
    select * into v_fin from public.timesheets_financials
    where timesheet_id=v_timesheet.timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;

  v_policy:=private._candidate_policy_resolve_v1(
    v_contract.client_id,v_contract.id,coalesce(v_week.week_ending_date,v_timesheet.week_ending_date)
  );
  v_route:=private._candidate_route_family_v1(v_timesheet.timesheet_id,v_week.id);
  v_route_family:=v_route->>'route_family';
  v_hours_route_allowed:=coalesce((v_route->>'candidate_hours_submission_allowed')::boolean,false);
  v_expense_route_allowed:=coalesce((v_route->>'candidate_expenses_allowed')::boolean,false);
  v_paper_route_allowed:=coalesce((v_route->>'candidate_paper_submission_allowed')::boolean,false);
  v_no_work_route_allowed:=coalesce((v_route->>'candidate_no_work_allowed')::boolean,false);
  v_separate:=coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false);
  v_hours:=coalesce(v_fin.total_hours,0);
  v_additional:=private._candidate_json_numeric_sum(coalesce(v_fin.additional_units_json,'{}'::jsonb));
  if v_additional=0 then
    v_additional:=private._candidate_json_numeric_sum(coalesce(v_timesheet.additional_units_week,'{}'::jsonb))
      +private._candidate_json_numeric_sum(coalesce(v_timesheet.additional_units_per_day,'{}'::jsonb));
  end if;
  v_mileage:=abs(coalesce(v_fin.mileage_units,0))+abs(coalesce(v_fin.mileage_pay_ex_vat,0))+abs(coalesce(v_fin.mileage_charge_ex_vat,0));
  v_travel:=abs(coalesce(v_fin.travel_pay_ex_vat,0))+abs(coalesce(v_fin.travel_charge_ex_vat,0));
  v_accommodation:=abs(coalesce(v_fin.accommodation_pay_ex_vat,0))+abs(coalesce(v_fin.accommodation_charge_ex_vat,0));
  v_other:=abs(coalesce(v_fin.expenses_pay_ex_vat,0))+abs(coalesce(v_fin.expenses_charge_ex_vat,0))
    +abs(coalesce(v_fin.other_pay_ex_vat,0))+abs(coalesce(v_fin.other_charge_ex_vat,0));
  v_expenses:=v_mileage+v_travel+v_accommodation+v_other;

  if jsonb_typeof(p_proposed_claim)='object' then
    v_expenses:=greatest(v_expenses,
      abs(coalesce(nullif(p_proposed_claim->>'expenses_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'expenses_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_charge_ex_vat','')::numeric,0))
    );
    v_mileage:=greatest(v_mileage,
      abs(coalesce(nullif(p_proposed_claim->>'mileage_units','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_charge_ex_vat','')::numeric,0)));
    v_travel:=greatest(v_travel,
      abs(coalesce(nullif(p_proposed_claim->>'travel_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'travel_charge_ex_vat','')::numeric,0)));
    v_accommodation:=greatest(v_accommodation,
      abs(coalesce(nullif(p_proposed_claim->>'accommodation_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'accommodation_charge_ex_vat','')::numeric,0)));
    v_other:=greatest(v_other,
      abs(coalesce(nullif(p_proposed_claim->>'expenses_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'expenses_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'other_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'other_charge_ex_vat','')::numeric,0)));
    v_expenses:=greatest(v_expenses,v_mileage+v_travel+v_accommodation+v_other);
  end if;

  v_import:=coalesce((v_route->>'import_authoritative')::boolean,false);
  v_protected:=v_timesheet.archived_at_utc is not null
    or (v_timesheet.timesheet_id is not null and (not v_timesheet.is_current))
    or v_fin.paid_at_utc is not null
    or v_fin.locked_by_invoice_id is not null
    or coalesce(v_week.status in (
      'INVOICED'::public.contract_week_status_enum,'CANCELLED'::public.contract_week_status_enum
    ),false);
  v_candidate_mutation_locked:=v_fin.authorised_at_utc is not null;
  if v_candidate_mutation_locked then
    v_reasons:=v_reasons||'"CANDIDATE_MUTATION_LOCKED_AUTHORISED"'::jsonb;
  end if;

  select exists(
    select 1 from public.timesheet_evidence e
    where e.timesheet_id=v_timesheet.timesheet_id
      and upper(btrim(e.kind))='TIMESHEET'
      and e.processing_state<>'SUPERSEDED'
  ) into v_has_timesheet;

  if v_protected then v_role:='PROTECTED'; v_reasons:=v_reasons||'"LIFECYCLE_PROTECTED"'::jsonb;
  elsif v_import and v_expenses<>0 then v_role:='CONFLICT'; v_reasons:=v_reasons||'"IMPORT_SOURCE_HAS_EXPENSES"'::jsonb;
  elsif v_import then v_role:='IMPORT_HOURS'; v_reasons:=v_reasons||'"IMPORT_AUTHORITATIVE_HOURS"'::jsonb;
  elsif v_separate and (v_hours<>0 or v_additional<>0) and v_expenses<>0 then v_role:='CONFLICT'; v_reasons:=v_reasons||'"SEPARATION_MIXED_ECONOMICS"'::jsonb;
  elsif v_expenses<>0 and v_hours=0 and v_additional=0 then v_role:='EXPENSE_ONLY';
  elsif (v_hours<>0 or v_additional<>0) and v_expenses=0 then v_role:='HOURS_ONLY';
  elsif not v_separate and v_timesheet.timesheet_id is not null then v_role:='COMBINED_ALLOWED';
  elsif v_week.additional_seq>0 and v_timesheet.timesheet_id is null then v_role:='FLEXIBLE';
  elsif v_week.additional_seq>0 and v_hours=0 and v_additional=0 and v_expenses=0 then v_role:='FLEXIBLE';
  else v_role:='HOURS_ONLY';
  end if;

  v_result:=jsonb_build_object(
    'record_role',v_role,
    'reason_codes',v_reasons,
    'timesheet_id',v_timesheet.timesheet_id,
    'contract_week_id',v_week.id,
    'contract_id',v_contract.id,
    'candidate_id',v_contract.candidate_id,
    'client_id',v_contract.client_id,
    'week_ending_date',v_week.week_ending_date,
    'additional_seq',v_week.additional_seq,
    'hours_value',v_hours,
    'additional_units_value',v_additional,
    'expense_value',v_expenses,
    'effective_separation',v_separate,
    'import_authoritative',v_import,
    'route_family',v_route_family,
    'effective_submission_mode',v_route->'effective_submission_mode',
    'protected',v_protected,
    'candidate_mutation_locked',v_candidate_mutation_locked,
    'has_active_timesheet_evidence',v_has_timesheet,
    'candidate_hours_submission_allowed',v_hours_route_allowed and not v_protected and not v_candidate_mutation_locked,
    'candidate_expenses_allowed',v_expense_route_allowed and not v_protected,
    'candidate_paper_submission_allowed',v_paper_route_allowed and not v_protected and not v_candidate_mutation_locked,
    'candidate_no_work_allowed',v_no_work_route_allowed and not v_protected and not v_candidate_mutation_locked
      and coalesce(v_week.additional_seq,0)=0 and not coalesce(v_week.is_adjustment,false),
    'can_edit_hours',v_hours_route_allowed and v_role in ('HOURS_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and not v_import,
    -- Imported hours remain immutable, but the Candidate may start the
    -- mandatory separate expense route against that worked-week anchor.
    'can_edit_expenses',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE','IMPORT_HOURS') and not v_protected and not v_candidate_mutation_locked,
    'can_attach_timesheet',v_hours_route_allowed and v_role in ('HOURS_ONLY','COMBINED_ALLOWED') and not v_protected and not v_candidate_mutation_locked and not v_has_timesheet,
    'can_attach_expense_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked,
    'can_attach_mileage_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_mileage<>0,
    'can_attach_travel_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_travel<>0,
    'can_attach_accommodation_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_accommodation<>0,
    'can_attach_other_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_other<>0,
    'can_process',v_role not in ('PROTECTED','CONFLICT') and not v_protected and not v_candidate_mutation_locked,
    'can_reject_candidate_submission',v_timesheet.timesheet_id is not null and not v_protected and v_fin.authorised_at_utc is null,
    'reject_scope',case when v_role='EXPENSE_ONLY' then 'COMPLETE_EXPENSE_CLAIM' else 'COMPLETE_TIMESHEET_RECORD' end,
    'requires_carrier',v_role='IMPORT_HOURS' or (v_separate and v_role='HOURS_ONLY'),
    'expense_invoice_email_ready',coalesce((v_policy->>'expense_invoice_email_ready')::boolean,false),
    'policy',v_policy
  );

  return v_result||jsonb_build_object(
    'capability_hash',encode(extensions.digest(convert_to(v_result::text,'UTF8'),'sha256'),'hex')
  );
exception
  when invalid_text_representation then
    raise exception 'CANDIDATE_PROPOSED_CLAIM_INVALID' using errcode='22023';
end;
$function$;

create or replace function private._candidate_submission_issue_codes_v1(
  p_workflow_id uuid,
  p_immutable_submission jsonb,
  p_policy_snapshot jsonb default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_week public.contract_weeks%rowtype;
  v_actual_schedule jsonb:='[]'::jsonb;
  v_planned_schedule jsonb:='[]'::jsonb;
  v_additional_week jsonb:='{}'::jsonb;
  v_additional_day jsonb:='{}'::jsonb;
  v_threshold numeric:=30;
  v_codes text[]:=array[]::text[];
  v_has_unexpected boolean:=false;
  v_has_nonstandard_daily_break boolean:=false;
  v_hr_validation_required boolean:=false;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_start timestamptz;
  v_daily_end timestamptz;
  v_daily_break_start timestamptz;
  v_daily_break_end timestamptz;
  v_daily_break_minutes integer;
  v_daily_work_date date;
  v_planned_unresolved boolean:=false;
begin
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id;
  if not found or jsonb_typeof(p_immutable_submission)<>'object' then
    raise exception 'CANDIDATE_ISSUE_CONTEXT_INVALID' using errcode='22023';
  end if;
  if v_workflow.contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=v_workflow.contract_week_id;
  end if;
  v_threshold:=coalesce(
    nullif(p_policy_snapshot->>'hours_deviation_pct','')::numeric,
    nullif(v_workflow.policy_snapshot_json->>'hours_deviation_pct','')::numeric,
    30
  );
  v_actual_schedule:=coalesce(
    p_immutable_submission#>'{hours_submission,timesheet_patch_json,actual_schedule_json}',
    p_immutable_submission#>'{timesheet_patch_json,actual_schedule_json}',
    '[]'::jsonb
  );
  v_planned_schedule:=coalesce(
    p_immutable_submission#>'{hours_submission,planned_schedule_json}',
    p_immutable_submission#>'{hours_submission,timesheet_patch_json,planned_schedule_json}',
    p_immutable_submission->'planned_schedule_json',
    p_immutable_submission#>'{timesheet_patch_json,planned_schedule_json}',
    v_week.planned_schedule_json,
    '[]'::jsonb
  );
  if v_workflow.workflow_kind='DAILY' then
    select * into v_daily_timesheet
    from public.timesheets
    where timesheet_id=v_workflow.target_timesheet_id and is_current=true;
    if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
    v_daily_start:=coalesce(
      nullif(p_immutable_submission#>>'{timesheet_patch_json,worked_start_iso}','')::timestamptz,
      nullif(p_immutable_submission->>'worked_start_iso','')::timestamptz
    );
    v_daily_end:=coalesce(
      nullif(p_immutable_submission#>>'{timesheet_patch_json,worked_end_iso}','')::timestamptz,
      nullif(p_immutable_submission->>'worked_end_iso','')::timestamptz
    );
    v_daily_break_start:=coalesce(
      nullif(p_immutable_submission#>>'{timesheet_patch_json,break_start_iso}','')::timestamptz,
      nullif(p_immutable_submission->>'break_start_iso','')::timestamptz
    );
    v_daily_break_end:=coalesce(
      nullif(p_immutable_submission#>>'{timesheet_patch_json,break_end_iso}','')::timestamptz,
      nullif(p_immutable_submission->>'break_end_iso','')::timestamptz
    );
    v_daily_break_minutes:=coalesce(
      nullif(p_immutable_submission#>>'{timesheet_patch_json,break_minutes}','')::integer,
      nullif(p_immutable_submission->>'break_minutes','')::integer,
      case when v_daily_break_start is not null and v_daily_break_end is not null
        then greatest(0,round(extract(epoch from (v_daily_break_end-v_daily_break_start))/60)::integer) end
    );
    v_daily_work_date:=private._candidate_daily_work_date_v1(
      v_daily_start,v_daily_timesheet.scheduled_start_iso,v_daily_timesheet.week_ending_date
    );
    if jsonb_typeof(v_actual_schedule)<>'array' or jsonb_array_length(v_actual_schedule)=0 then
      v_actual_schedule:=jsonb_build_array(jsonb_build_object(
        'date',v_daily_work_date,
        'start_iso',v_daily_start,
        'end_iso',v_daily_end,
        'break_start_iso',v_daily_break_start,
        'break_end_iso',v_daily_break_end,
        'break_minutes',v_daily_break_minutes
      ));
    end if;
    if jsonb_typeof(v_planned_schedule)<>'array' or jsonb_array_length(v_planned_schedule)=0 then
      v_planned_schedule:=jsonb_build_array(jsonb_build_object(
        'date',private._candidate_daily_work_date_v1(
          v_daily_timesheet.scheduled_start_iso,v_daily_timesheet.scheduled_start_iso,v_daily_timesheet.week_ending_date),
        'start_iso',v_daily_timesheet.scheduled_start_iso,
        'end_iso',v_daily_timesheet.scheduled_end_iso,
        'break_minutes',60
      ));
    end if;
    select exists(
      select 1
      from jsonb_array_elements(v_planned_schedule) planned_item
      where not (
        (nullif(coalesce(planned_item->>'start_iso',planned_item->>'scheduled_start_iso'),'') is not null
         and nullif(coalesce(planned_item->>'end_iso',planned_item->>'scheduled_end_iso'),'') is not null)
        or
        (coalesce(planned_item->>'start','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
         and coalesce(planned_item->>'end','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$')
      )
    ) into v_planned_unresolved;
    if v_planned_unresolved then
      v_codes:=array_append(v_codes,'PLANNED_HOURS_UNRESOLVED');
    end if;
    -- Reuse the same effective flags consumed by the installed TSFIN worker.
    -- This avoids the older summary-view approximation and ensures that the
    -- Candidate path cannot auto-authorise while invoice HR validation is due.
    select coalesce((context_row.out_effective_flags->>'hr_validation_required_for_invoice')::boolean,false)
        and upper(coalesce(context_row.out_effective_flags->>'validation_status','')) not in ('VALIDATION_OK','OVERRIDDEN')
    into v_hr_validation_required
    from public.tsfin_load_context_batch(array[v_workflow.target_timesheet_id]::uuid[]) context_row
    where context_row.effective_timesheet_id=v_workflow.target_timesheet_id;
  end if;
  v_additional_week:=coalesce(
    p_immutable_submission#>'{hours_submission,timesheet_patch_json,additional_units_week}',
    p_immutable_submission#>'{timesheet_patch_json,additional_units_week}',
    '{}'::jsonb
  );
  v_additional_day:=coalesce(
    p_immutable_submission#>'{hours_submission,timesheet_patch_json,additional_units_per_day}',
    p_immutable_submission#>'{timesheet_patch_json,additional_units_per_day}',
    '{}'::jsonb
  );
  if private._candidate_json_numeric_sum(v_additional_week)
       +private._candidate_json_numeric_sum(v_additional_day)>0 then
    v_codes:=array_append(v_codes,'ADDITIONAL_UNITS_NEEDS_CHECKING');
  end if;
  if jsonb_typeof(v_actual_schedule)='array' then
    with actual_rows as (
      select coalesce(
          nullif(item->>'date','')::date,
          (nullif(coalesce(item->>'start_iso',item->>'worked_start_iso'),'')::timestamptz at time zone 'Europe/London')::date
        ) as work_date,
        greatest(0,
          case
            when nullif(coalesce(item->>'start_iso',item->>'worked_start_iso'),'') is not null
             and nullif(coalesce(item->>'end_iso',item->>'worked_end_iso'),'') is not null
              then round(extract(epoch from (
                nullif(coalesce(item->>'end_iso',item->>'worked_end_iso'),'')::timestamptz
                -nullif(coalesce(item->>'start_iso',item->>'worked_start_iso'),'')::timestamptz
              ))/60)::integer
            when coalesce(item->>'start','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
             and coalesce(item->>'end','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
              then mod((extract(epoch from ((item->>'end')::time-(item->>'start')::time))/60)::integer+1440,1440)
            else 0
          end
          -case
            when coalesce(item->>'break_minutes',item->>'break_mins','')~'^\d+$'
              then coalesce(item->>'break_minutes',item->>'break_mins')::integer
            when nullif(item->>'break_start_iso','') is not null and nullif(item->>'break_end_iso','') is not null
              then greatest(0,round(extract(epoch from (
                (item->>'break_end_iso')::timestamptz-(item->>'break_start_iso')::timestamptz
              ))/60)::integer)
            when coalesce(item->>'break_start','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
             and coalesce(item->>'break_end','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
              then mod((extract(epoch from ((item->>'break_end')::time-(item->>'break_start')::time))/60)::integer+1440,1440)
            else 0 end
        ) as net_minutes,
        case
          when coalesce(item->>'break_minutes',item->>'break_mins','')~'^\d+$'
            then coalesce(item->>'break_minutes',item->>'break_mins')::integer
          when nullif(item->>'break_start_iso','') is not null and nullif(item->>'break_end_iso','') is not null
            then greatest(0,round(extract(epoch from (
              (item->>'break_end_iso')::timestamptz-(item->>'break_start_iso')::timestamptz
            ))/60)::integer)
          when coalesce(item->>'break_start','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
           and coalesce(item->>'break_end','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
            then mod((extract(epoch from ((item->>'break_end')::time-(item->>'break_start')::time))/60)::integer+1440,1440)
          else 0 end as break_minutes
      from jsonb_array_elements(v_actual_schedule) item
    ), actual_days as (
      select work_date,sum(net_minutes)::numeric as net_minutes,sum(break_minutes)::integer as break_minutes
      from actual_rows where work_date is not null group by work_date
    ), planned_rows as (
      select coalesce(
          nullif(item->>'date','')::date,
          (nullif(coalesce(item->>'start_iso',item->>'scheduled_start_iso'),'')::timestamptz at time zone 'Europe/London')::date
        ) as work_date,
        greatest(0,case
          when nullif(coalesce(item->>'start_iso',item->>'scheduled_start_iso'),'') is not null
           and nullif(coalesce(item->>'end_iso',item->>'scheduled_end_iso'),'') is not null
            then round(extract(epoch from (
              nullif(coalesce(item->>'end_iso',item->>'scheduled_end_iso'),'')::timestamptz
              -nullif(coalesce(item->>'start_iso',item->>'scheduled_start_iso'),'')::timestamptz
            ))/60)::integer
          when coalesce(item->>'start','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
           and coalesce(item->>'end','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
            then mod((extract(epoch from ((item->>'end')::time-(item->>'start')::time))/60)::integer+1440,1440)
          else 0 end) as gross_minutes,
        case
          when coalesce(item->>'break_minutes',item->>'break_mins','')~'^\d+$'
            then coalesce(item->>'break_minutes',item->>'break_mins')::integer
          when nullif(item->>'break_start_iso','') is not null and nullif(item->>'break_end_iso','') is not null
            then greatest(0,round(extract(epoch from (
              (item->>'break_end_iso')::timestamptz-(item->>'break_start_iso')::timestamptz
            ))/60)::integer)
          when coalesce(item->>'break_start','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
           and coalesce(item->>'break_end','')~'^([01][0-9]|2[0-3]):[0-5][0-9]$'
            then mod((extract(epoch from ((item->>'break_end')::time-(item->>'break_start')::time))/60)::integer+1440,1440)
          else null end as explicit_break_minutes
      from jsonb_array_elements(case when jsonb_typeof(v_planned_schedule)='array'
        then v_planned_schedule else '[]'::jsonb end) item
    ), planned_days as (
      select work_date,greatest(0,
        sum(gross_minutes)
        -case
          when count(explicit_break_minutes)>0 then sum(coalesce(explicit_break_minutes,0))
          when v_workflow.workflow_kind='DAILY' then 60
          else 0
        end
      )::numeric as net_minutes
      from planned_rows where work_date is not null group by work_date
    )
    select
      exists(
        select 1
        from actual_days actual
        full join planned_days planned using(work_date)
        where coalesce(actual.net_minutes,0)>0
          and (
            coalesce(planned.net_minutes,0)=0
            or coalesce(actual.net_minutes,0)>planned.net_minutes*(1+v_threshold/100.0)
          )
      ),
      exists(select 1 from actual_days where break_minutes<>60)
    into v_has_unexpected,v_has_nonstandard_daily_break;
  end if;
  if v_has_unexpected then v_codes:=array_append(v_codes,'UNEXPECTED_HOURS'); end if;
  if v_workflow.workflow_kind='DAILY' and v_has_nonstandard_daily_break then
    v_codes:=array_append(v_codes,'DAILY_BREAK_UNEXPECTED');
  end if;
  if v_workflow.workflow_kind='DAILY' and v_hr_validation_required then
    v_codes:=array_append(v_codes,'HEALTHROSTER_VALIDATION_REQUIRED');
  end if;
  if v_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     and exists(
       select 1
       from public.contract_weeks prior_week
       join public.timesheets prior_timesheet
         on prior_timesheet.timesheet_id=prior_week.timesheet_id
        and prior_timesheet.is_current=true
        and prior_timesheet.archived_at_utc is null
       join public.timesheets_financials prior_fin
         on prior_fin.timesheet_id=prior_timesheet.timesheet_id
        and prior_fin.is_current=true
       where prior_week.contract_id=v_workflow.contract_id
         and prior_week.week_ending_date=v_workflow.week_ending_date
         and prior_fin.authorised_at_utc is not null
         and prior_timesheet.timesheet_id is distinct from v_workflow.target_timesheet_id
         and (
           abs(coalesce(prior_fin.expenses_pay_ex_vat,0))+abs(coalesce(prior_fin.expenses_charge_ex_vat,0))
           +abs(coalesce(prior_fin.mileage_units,0))+abs(coalesce(prior_fin.mileage_pay_ex_vat,0))
           +abs(coalesce(prior_fin.mileage_charge_ex_vat,0))+abs(coalesce(prior_fin.travel_pay_ex_vat,0))
           +abs(coalesce(prior_fin.travel_charge_ex_vat,0))+abs(coalesce(prior_fin.accommodation_pay_ex_vat,0))
           +abs(coalesce(prior_fin.accommodation_charge_ex_vat,0))+abs(coalesce(prior_fin.other_pay_ex_vat,0))
           +abs(coalesce(prior_fin.other_charge_ex_vat,0))
         )>0
     ) then
    v_codes:=array_append(v_codes,'DUPLICATE_EXPENSE_REVIEW');
  end if;
  return coalesce((
    select jsonb_agg(code order by code)
    from (select distinct unnest(v_codes) as code) codes
  ),'[]'::jsonb);
exception
  when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'CANDIDATE_ISSUE_CONTEXT_INVALID' using errcode='22023';
end;
$function$;

create or replace function private._candidate_signature_component_v1(
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_capabilities jsonb:='{}'::jsonb;
  v_workflows jsonb:='[]'::jsonb;
  v_evidence jsonb:='[]'::jsonb;
  v_components jsonb:='[]'::jsonb;
begin
  if not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return null;
  end if;
  begin
    v_capabilities:=private._candidate_record_capabilities_v1(p_timesheet_id,p_contract_week_id,'{}'::jsonb);
  exception when others then
    v_capabilities:=jsonb_build_object('unavailable',true,'sqlstate',sqlstate);
  end;

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',w.id,'generation',w.generation,'state',w.state,'route',w.route,
    'issue_codes',w.issue_codes,'target_timesheet_id',w.target_timesheet_id,
    'contract_week_id',w.contract_week_id,
    'review_manifest_sha256',case when w.review_manifest_sha256 is null then null else encode(w.review_manifest_sha256,'hex') end,
    'candidate_signature_sha256',case when w.candidate_signature_sha256 is null then null else encode(w.candidate_signature_sha256,'hex') end,
    'manager_signature_sha256',case when w.manager_signature_sha256 is null then null else encode(w.manager_signature_sha256,'hex') end,
    'manager_approved_at_utc',w.manager_approved_at_utc,'updated_at_utc',w.updated_at_utc
  ) order by w.updated_at_utc,w.id),'[]'::jsonb)
  into v_workflows
  from public.candidate_submission_workflows w
  where (p_timesheet_id is not null and w.target_timesheet_id=p_timesheet_id)
     or (p_contract_week_id is not null and w.contract_week_id=p_contract_week_id);

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
    'processing_state',e.processing_state,'candidate_component_id',e.candidate_component_id,
    'updated_at',e.updated_at
  ) order by e.updated_at,e.id),'[]'::jsonb)
  into v_evidence
  from public.timesheet_evidence e
  where p_timesheet_id is not null and e.timesheet_id=p_timesheet_id;

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',c.id,'workflow_id',c.workflow_id,'workflow_generation',c.workflow_generation,
    'component_kind',c.component_kind,'required',c.required,'review_ordinal',c.review_ordinal,
    'state',c.state,'review_render_state',c.review_render_state,
    'review_content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
    'review_render_input_sha256',case when c.review_render_input_sha256 is null then null else encode(c.review_render_input_sha256,'hex') end,
    'final_signed_render_state',c.final_signed_render_state,
    'final_signed_content_sha256',case when c.final_signed_content_sha256 is null then null else encode(c.final_signed_content_sha256,'hex') end,
    'final_signed_render_input_sha256',case when c.final_signed_render_input_sha256 is null then null else encode(c.final_signed_render_input_sha256,'hex') end
  ) order by c.workflow_generation,c.review_ordinal,c.id),'[]'::jsonb)
  into v_components
  from public.candidate_submission_components c
  join public.candidate_submission_workflows w on w.id=c.workflow_id
  where ((p_timesheet_id is not null and w.target_timesheet_id=p_timesheet_id)
      or (p_contract_week_id is not null and w.contract_week_id=p_contract_week_id))
    and c.state<>'SUPERSEDED';

  return jsonb_build_object(
    'capabilities',v_capabilities,
    'workflows',v_workflows,
    'evidence',v_evidence,
    'components',v_components
  );
end;
$function$;

create or replace function private._candidate_office_context_overlay_v1(p_payload jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_timesheet_text text;
  v_week_text text;
  v_timesheet_id uuid;
  v_week_id uuid;
  v_capabilities jsonb;
  v_overlay jsonb;
  v_action_overlay jsonb;
  v_workflow_state text;
  v_workflow_generation integer;
  v_approval_state text;
  v_approval_method text;
  v_resend_count integer;
  v_last_sent_at timestamptz;
  v_manager_state text;
  v_can_remind boolean:=false;
  v_key text;
begin
  if jsonb_typeof(v_payload)<>'object'
     or not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return v_payload;
  end if;
  v_timesheet_text:=nullif(btrim(coalesce(
    v_payload->>'timesheet_id',v_payload->>'current_timesheet_id',
    v_payload#>>'{row,timesheet_id}',v_payload#>>'{row,current_timesheet_id}',
    v_payload#>>'{data_row,timesheet_id}',v_payload#>>'{row_patch,timesheet_id}','')),'');
  v_week_text:=nullif(btrim(coalesce(
    v_payload->>'contract_week_id',v_payload#>>'{row,contract_week_id}',
    v_payload#>>'{data_row,contract_week_id}',v_payload#>>'{row_patch,contract_week_id}','')),'');
  if v_timesheet_text~*'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    v_timesheet_id:=v_timesheet_text::uuid;
  end if;
  if v_week_text~*'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    v_week_id:=v_week_text::uuid;
  end if;
  if v_timesheet_id is null and v_week_id is null then return v_payload; end if;

  begin
    v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet_id,v_week_id,'{}'::jsonb);
  exception when others then
    return v_payload||jsonb_build_object(
      'candidate_capabilities_unavailable',true,
      'candidate_capabilities_error_code','CANDIDATE_CAPABILITIES_UNAVAILABLE'
    );
  end;

  select w.state,w.generation,a.state,a.method,a.resend_count,a.last_sent_at_utc
  into v_workflow_state,v_workflow_generation,v_approval_state,v_approval_method,v_resend_count,v_last_sent_at
  from public.candidate_submission_workflows w
  left join lateral (
    select ar.state,ar.method,ar.resend_count,ar.last_sent_at_utc
    from public.candidate_approval_requests ar
    where ar.workflow_id=w.id and ar.workflow_generation=w.generation
    order by ar.updated_at_utc desc,ar.id desc limit 1
  ) a on true
  where (v_timesheet_id is not null and w.target_timesheet_id=v_timesheet_id)
     or (v_week_id is not null and w.contract_week_id=v_week_id)
  order by w.updated_at_utc desc,w.id desc
  limit 1;

  v_manager_state:=case
    when v_approval_state='APPROVED' or v_workflow_state in (
      'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','FINALISED'
    ) then 'MANAGER_APPROVED'
    when v_approval_state='PENDING' or v_workflow_state='AWAITING_MANAGER_APPROVAL' then 'AWAITING_MANAGER_APPROVAL'
    else null end;
  v_can_remind:=v_manager_state='AWAITING_MANAGER_APPROVAL'
    and v_approval_method='EMAIL'
    and coalesce(v_resend_count,0)<5
    and (v_last_sent_at is null or v_last_sent_at<=now()-interval '24 hours');

  v_action_overlay:=jsonb_build_object(
    'can_send_manager_reminder',v_can_remind,
    'can_reject_candidate_submission',coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false),
    'candidate_reject_scope',v_capabilities->>'reject_scope',
    'can_attach_timesheet',coalesce((v_capabilities->>'can_attach_timesheet')::boolean,false),
    'can_edit_candidate_expenses',coalesce((v_capabilities->>'can_edit_expenses')::boolean,false)
  );
  v_overlay:=jsonb_strip_nulls(jsonb_build_object(
    'candidate_capabilities',v_capabilities,
    'candidate_record_role',v_capabilities->>'record_role',
    'candidate_capability_hash',v_capabilities->>'capability_hash',
    'candidate_manager_approval_state',v_manager_state,
    'candidate_workflow_state',v_workflow_state,
    'candidate_workflow_generation',v_workflow_generation,
    'candidate_can_send_manager_reminder',v_can_remind,
    'candidate_can_reject_submission',coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false),
    'candidate_reject_scope',v_capabilities->>'reject_scope',
    'candidate_attach_options',jsonb_build_object(
      'TIMESHEET',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_timesheet')::boolean,false)),
      'MILEAGE',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_mileage_evidence')::boolean,false)),
      'TRAVEL',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_travel_evidence')::boolean,false)),
      'ACCOMMODATION',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_accommodation_evidence')::boolean,false)),
      'OTHER',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_other_evidence')::boolean,false))
    ),
    'candidate_preview_border',jsonb_build_object(
      'width_px',4,
      'tone',case when coalesce((v_capabilities->>'can_attach_timesheet')::boolean,false) then 'success' else 'danger' end
    )
  ));
  v_payload:=v_payload||v_overlay||jsonb_build_object(
    'action_flags',coalesce(v_payload->'action_flags','{}'::jsonb)||v_action_overlay
  );
  foreach v_key in array array['row','data_row','row_patch','details','left_pane'] loop
    if jsonb_typeof(v_payload->v_key)='object' then
      v_payload:=jsonb_set(v_payload,array[v_key],(v_payload->v_key)||v_overlay||jsonb_build_object(
        'action_flags',coalesce(v_payload#>array[v_key,'action_flags'],'{}'::jsonb)||v_action_overlay
      ),true);
    end if;
  end loop;
  return v_payload;
end;
$function$;

create or replace function private._candidate_dataset_overlay_v1(p_payload jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_key text;
  v_rows jsonb;
begin
  if jsonb_typeof(v_payload)<>'object'
     or not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return v_payload;
  end if;
  foreach v_key in array array['unprocessed_rows','processed_rows','rows'] loop
    if jsonb_typeof(v_payload->v_key)='array' then
      select coalesce(jsonb_agg(private._candidate_office_context_overlay_v1(item.value) order by item.ordinality),'[]'::jsonb)
      into v_rows
      from jsonb_array_elements(v_payload->v_key) with ordinality item(value,ordinality);
      v_payload:=jsonb_set(v_payload,array[v_key],v_rows,true);
    end if;
  end loop;
  return v_payload;
end;
$function$;

create or replace function private._candidate_draft_totals_guard_v1(
  p_contract_week_id uuid,
  p_totals_json jsonb
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_policy jsonb;
  v_hours numeric;
  v_additional numeric;
  v_expenses numeric;
  v_import boolean;
  v_role text;
  v_import_authority jsonb;
begin
  if not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return '{}'::jsonb;
  end if;
  select * into v_week from public.contract_weeks where id=p_contract_week_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id;
  v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,v_week.week_ending_date);
  v_hours:=private._candidate_json_numeric_sum(coalesce(p_totals_json->'hours','{}'::jsonb));
  v_additional:=private._candidate_json_numeric_sum(coalesce(p_totals_json->'additional_units_week','{}'::jsonb))
    +private._candidate_json_numeric_sum(coalesce(p_totals_json->'additional_units_per_day','{}'::jsonb));
  v_expenses:=private._candidate_json_numeric_sum(coalesce(p_totals_json->'expenses_draft','{}'::jsonb));
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,v_week.timesheet_id,null,v_week.week_ending_date
  );
  v_import:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);
  if v_import and v_expenses<>0 then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  if coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false)
     and (v_hours<>0 or v_additional<>0) and v_expenses<>0 then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  v_role:=case
    when v_import then 'IMPORT_HOURS'
    when v_expenses<>0 and v_hours=0 and v_additional=0 then 'EXPENSE_ONLY'
    when (v_hours<>0 or v_additional<>0) and v_expenses=0 then 'HOURS_ONLY'
    when not coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false) and v_expenses<>0 then 'COMBINED_ALLOWED'
    else 'FLEXIBLE' end;
  return jsonb_build_object('record_role',v_role,'hours_value',v_hours,
    'additional_units_value',v_additional,'expense_value',v_expenses,'policy',v_policy);
end;
$function$;

create or replace function private._candidate_weekly_final_state_guard_v1(
  p_contract_week_id uuid,
  p_timesheet_id uuid default null,
  p_timesheet_create_json jsonb default null,
  p_timesheet_patch_json jsonb default '{}'::jsonb,
  p_tsfin_snapshot_json jsonb default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_policy jsonb;
  v_snapshot jsonb:=coalesce(p_tsfin_snapshot_json,'{}'::jsonb);
  v_hours numeric:=0;
  v_additional numeric:=0;
  v_mileage numeric:=0;
  v_non_mileage numeric:=0;
  v_expenses numeric:=0;
  v_import boolean;
  v_role text;
  v_expected_line_type text;
  v_requested_line_type text;
  v_import_authority jsonb;
begin
  if not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities')
     and coalesce(current_setting('cloudtms.candidate_electronic_finalise',true),'')='' then
    return '{}'::jsonb;
  end if;
  select * into v_week from public.contract_weeks where id=p_contract_week_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id;
  if p_tsfin_snapshot_json is null and p_timesheet_id is not null then
    select to_jsonb(tf) into v_snapshot
    from public.timesheets_financials tf
    where tf.timesheet_id=p_timesheet_id and tf.is_current=true
    order by tf.computed_at_utc desc nulls last,tf.updated_at desc,tf.id desc
    limit 1;
    v_snapshot:=coalesce(v_snapshot,'{}'::jsonb);
  end if;
  v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,v_week.week_ending_date);
  v_hours:=coalesce(nullif(v_snapshot->>'total_hours','')::numeric,
    coalesce(nullif(v_snapshot->>'hours_day','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_night','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_sat','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_sun','')::numeric,0)
    +coalesce(nullif(v_snapshot->>'hours_bh','')::numeric,0),0);
  v_additional:=greatest(
    private._candidate_json_numeric_sum(coalesce(v_snapshot->'additional_units_json','{}'::jsonb)),
    private._candidate_json_numeric_sum(coalesce(p_timesheet_create_json->'additional_units_week',p_timesheet_patch_json->'additional_units_week','{}'::jsonb))
      +private._candidate_json_numeric_sum(coalesce(p_timesheet_create_json->'additional_units_per_day',p_timesheet_patch_json->'additional_units_per_day','{}'::jsonb))
  );
  v_mileage:=abs(coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0));
  v_non_mileage:=abs(coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0))
    +abs(coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0));
  v_expenses:=v_mileage+v_non_mileage;
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,coalesce(p_timesheet_id,v_week.timesheet_id),v_snapshot,v_week.week_ending_date
  );
  v_import:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);
  if v_import and v_expenses<>0 then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  if coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false)
     and (v_hours<>0 or v_additional<>0) and v_expenses<>0 then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  v_role:=case
    when v_import then 'IMPORT_HOURS'
    when v_expenses<>0 and v_hours=0 and v_additional=0 then 'EXPENSE_ONLY'
    when (v_hours<>0 or v_additional<>0) and v_expenses=0 then 'HOURS_ONLY'
    when not coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false) then 'COMBINED_ALLOWED'
    else 'FLEXIBLE' end;
  v_expected_line_type:=case
    when v_role='EXPENSE_ONLY' and v_mileage<>0 and v_non_mileage=0 then 'MILEAGE'
    when v_role='EXPENSE_ONLY' then 'EXPENSES'
    else 'HOURS' end;
  v_requested_line_type:=upper(nullif(btrim(coalesce(
    p_timesheet_patch_json->>'line_type',p_timesheet_create_json->>'line_type','')),''));
  if v_requested_line_type is not null and v_requested_line_type<>v_expected_line_type then
    raise exception 'CANDIDATE_LINE_TYPE_FINAL_STATE_INVALID'
      using errcode='22023',detail=jsonb_build_object('record_role',v_role,
        'expected_line_type',v_expected_line_type,'requested_line_type',v_requested_line_type)::text;
  end if;
  return jsonb_build_object('record_role',v_role,'expected_line_type',v_expected_line_type,
    'hours_value',v_hours,'additional_units_value',v_additional,'expense_value',v_expenses,'policy',v_policy);
exception when invalid_text_representation then
  raise exception 'CANDIDATE_FINAL_STATE_INPUT_INVALID' using errcode='22023';
end;
$function$;

create or replace function private._candidate_manager_email_allowed_v1(
  p_policy jsonb,
  p_email text,
  p_barred_domains jsonb
)
returns jsonb
language plpgsql
immutable
set search_path = pg_catalog, private
as $function$
declare
  v_email text:=private._candidate_normalize_email(p_email);
  v_domain text;
  v_full boolean:=false;
  v_domain_allowed boolean:=false;
  v_barred boolean:=false;
  v_barred_policy_ready boolean:=jsonb_typeof(p_barred_domains)='array'
    and jsonb_array_length(p_barred_domains)>0;
  v_free boolean:=coalesce((p_policy->>'allow_free_business_email')::boolean,false);
begin
  if v_email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
    return jsonb_build_object('allowed',false,'reason_code','MANAGER_EMAIL_INVALID');
  end if;
  v_domain:=split_part(v_email,'@',2);
  select exists(select 1 from jsonb_array_elements_text(coalesce(p_policy->'approved_emails','[]'::jsonb)) x where lower(btrim(x))=v_email) into v_full;
  select exists(select 1 from jsonb_array_elements_text(coalesce(p_policy->'approved_domains','[]'::jsonb)) x
    where private._candidate_normalize_domain_v1(x)=v_domain) into v_domain_allowed;
  select exists(select 1 from jsonb_array_elements_text(coalesce(p_barred_domains,'[]'::jsonb)) x
    where private._candidate_normalize_domain_v1(x)=v_domain) into v_barred;
  return jsonb_build_object(
    'allowed',v_full or v_domain_allowed or (v_free and v_barred_policy_ready and not v_barred),
    'reason_code',case
      when v_full then 'APPROVED_EMAIL'
      when v_domain_allowed then 'APPROVED_DOMAIN'
      when v_free and not v_barred_policy_ready then 'BARRED_DOMAIN_POLICY_NOT_CONFIGURED'
      when v_free and not v_barred then 'FREE_BUSINESS_EMAIL'
      when v_barred then 'BARRED_DOMAIN'
      else 'MANAGER_EMAIL_NOT_APPROVED' end,
    'email_normalized',v_email,
    'domain',v_domain
  );
end;
$function$;

create or replace function private._candidate_daily_canonical_save_input_v1(
  p_workflow_id uuid,
  p_generation integer
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_hours jsonb;
  v_patch jsonb;
begin
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_generation;
  if not found or v_workflow.workflow_kind<>'DAILY' or v_workflow.scope<>'DAILY'
     or v_workflow.target_timesheet_id is null then
    raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
  end if;
  if v_workflow.immutable_submission_json is null
     or private._candidate_sha256_jsonb_v1(v_workflow.immutable_submission_json)
        is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  v_hours:=coalesce(v_workflow.immutable_submission_json->'hours_submission',v_workflow.immutable_submission_json);
  v_patch:=coalesce(v_hours->'timesheet_patch_json','{}'::jsonb);
  if jsonb_typeof(v_hours)<>'object' or jsonb_typeof(v_patch)<>'object' then
    raise exception 'CANDIDATE_DAILY_SAVE_PAYLOAD_INVALID' using errcode='22023';
  end if;
  return jsonb_build_object(
    'contract_version','CANDIDATE_DAILY_CANONICAL_SAVE_V1',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'timesheet_id',v_workflow.target_timesheet_id,
    'work_date',v_workflow.work_date,
    'timesheet_patch_json',jsonb_build_object(
      'worked_start_iso',coalesce(v_patch->'worked_start_iso',v_hours->'worked_start_iso','null'::jsonb),
      'worked_end_iso',coalesce(v_patch->'worked_end_iso',v_hours->'worked_end_iso','null'::jsonb),
      'break_start_iso',coalesce(v_patch->'break_start_iso',v_hours->'break_start_iso','null'::jsonb),
      'break_end_iso',coalesce(v_patch->'break_end_iso',v_hours->'break_end_iso','null'::jsonb),
      'break_minutes',coalesce(v_patch->'break_minutes',v_hours->'break_minutes','null'::jsonb),
      'actual_schedule_json',coalesce(v_patch->'actual_schedule_json',v_hours->'actual_schedule_json','null'::jsonb)
    )
  );
end;
$function$;
create or replace function private._candidate_audit_v1(
  p_object_type text,
  p_object_id text,
  p_action text,
  p_before jsonb default null,
  p_after jsonb default null,
  p_reason text default null,
  p_actor_user_id uuid default null,
  p_correlation_id text default null,
  p_now_utc timestamptz default now()
)
returns uuid
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare v_id uuid;
begin
  insert into public.audit_events(
    ts_utc,actor_user_id,object_type,object_id_text,action,before_json,after_json,reason,correlation_id
  ) values (
    p_now_utc,p_actor_user_id,btrim(p_object_type),btrim(p_object_id),btrim(p_action),p_before,p_after,p_reason,p_correlation_id
  ) returning id into v_id;
  return v_id;
end;
$function$;

create or replace function private._candidate_notification_insert_v1(
  p_account_id uuid,
  p_candidate_id uuid,
  p_workflow_id uuid,
  p_timesheet_id uuid,
  p_event_type text,
  p_preference_category text,
  p_template_key text,
  p_template_params jsonb,
  p_deep_link jsonb,
  p_dedupe_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare v_row public.candidate_notifications%rowtype;
begin
  insert into public.candidate_notifications(
    account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,dedupe_key,created_at_utc
  ) values (
    p_account_id,p_candidate_id,p_workflow_id,p_timesheet_id,btrim(p_event_type),btrim(p_preference_category),
    btrim(p_template_key),coalesce(p_template_params,'{}'::jsonb),coalesce(p_deep_link,'{}'::jsonb),btrim(p_dedupe_key),p_now_utc
  ) on conflict (dedupe_key) do update set dedupe_key=excluded.dedupe_key
  returning * into v_row;
  return jsonb_build_object('notification_id',v_row.id,'dedupe_key',v_row.dedupe_key,'state',v_row.state);
end;
$function$;

revoke all on function private._candidate_normalize_email(text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_normalize_domain_v1(text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_normalize_domain_array_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_normalize_manager_policy_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_work_date_v1(timestamptz,timestamptz,date) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_entitled_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_json_numeric_sum(jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_submission_mode_v1(uuid,uuid,date) from public,anon,authenticated,service_role;
revoke all on function private._candidate_import_authoritative_v1(uuid,uuid,uuid,jsonb,date) from public,anon,authenticated,service_role;
revoke all on function private._candidate_route_family_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_assert_environment(text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_feature_enabled_v1(text,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_feature_enabled_current_v1(text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_require_feature_v1(text,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_email_eligibility_v1(text,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_session_context_v1(uuid,text,integer,timestamptz,boolean) from public,anon,authenticated,service_role;
revoke all on function private._candidate_policy_resolve_v1(uuid,uuid,date) from public,anon,authenticated,service_role;
revoke all on function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_submission_issue_codes_v1(uuid,jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_manager_email_allowed_v1(jsonb,text,jsonb) from public,anon,authenticated,service_role;
revoke all on function private._candidate_daily_canonical_save_input_v1(uuid,integer) from public,anon,authenticated,service_role;
revoke all on function private._candidate_audit_v1(text,text,text,jsonb,jsonb,text,uuid,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_notification_insert_v1(uuid,uuid,uuid,uuid,text,text,text,jsonb,jsonb,text,timestamptz) from public,anon,authenticated,service_role;
comment on function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) is
  'Single server capability authority for role, editors, ATTACH choices, preview border, processing, rejection and expense placement.';
comment on function private._candidate_import_authoritative_v1(uuid,uuid,uuid,jsonb,date) is
  'Single Candidate App authority for NHSP/HealthRoster/import source truth, hours view-only behaviour and mandatory separated expenses.';
