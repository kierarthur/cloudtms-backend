-- Immutable CloudTMS TEST function snapshot, page 15.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- private._candidate_sha256_jsonb_v1(jsonb)
CREATE OR REPLACE FUNCTION private._candidate_sha256_jsonb_v1(p_value jsonb)
 RETURNS bytea
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'pg_catalog', 'extensions', 'pg_temp'
AS $function$
  select extensions.digest(convert_to(coalesce(p_value,'null'::jsonb)::text,'UTF8'),'sha256');
$function$;

-- private._candidate_signature_component_v1(uuid,uuid)
CREATE OR REPLACE FUNCTION private._candidate_signature_component_v1(p_timesheet_id uuid DEFAULT NULL::uuid, p_contract_week_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
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

-- private._candidate_status_code_v1(boolean,boolean,boolean,text,boolean,text,text)
CREATE OR REPLACE FUNCTION private._candidate_status_code_v1(p_paid boolean, p_authorised boolean, p_invoiced_not_paid boolean, p_active_workflow_state text, p_actionable_rejection boolean, p_processing_status text, p_contract_week_status text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
  select case
    when coalesce(p_paid,false) then 'PAID'
    when coalesce(p_authorised,false) then 'AUTHORISED'
    when coalesce(p_invoiced_not_paid,false) then 'INVOICED_NOT_PAID'
    when nullif(btrim(coalesce(p_active_workflow_state,'')),'') is not null
      then upper(btrim(p_active_workflow_state))
    when coalesce(p_actionable_rejection,false) then 'REJECTED'
    when nullif(btrim(coalesce(p_processing_status,'')),'') is not null
      then upper(btrim(p_processing_status))
    else upper(coalesce(nullif(btrim(p_contract_week_status),''),'AVAILABLE'))
  end;
$function$;

-- private._candidate_submission_issue_codes_v1(uuid,jsonb,jsonb)
CREATE OR REPLACE FUNCTION private._candidate_submission_issue_codes_v1(p_workflow_id uuid, p_immutable_submission jsonb, p_policy_snapshot jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
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

-- private._candidate_submission_mode_v1(uuid,uuid,date)
CREATE OR REPLACE FUNCTION private._candidate_submission_mode_v1(p_client_id uuid, p_contract_id uuid, p_as_of_date date)
 RETURNS submission_mode_enum
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
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

-- private._candidate_timesheet_action_contract_v1(text,jsonb,jsonb,uuid,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_timesheet_action_contract_v1(p_candidate_status_code text, p_workflows jsonb, p_capabilities jsonb, p_timesheet_id uuid, p_contract_week_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_primary jsonb;
  v_actions jsonb:='[]'::jsonb;
  v_workflow_json jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_manager_approval jsonb;
  v_provider_first_accepted_at timestamptz;
  v_provider_accepted_at timestamptz;
  v_latest_delivery_state text;
  v_latest_provider_status text;
  v_pending_delivery_count integer:=0;
  v_next_reminder_at timestamptz;
  v_reminder_eligible boolean:=false;
  v_renewal_eligible boolean:=false;
  v_cancel_eligible boolean:=false;
  v_effective_request_state text;
  v_disabled_reason text;
  v_action jsonb;
  v_cancel_code text;
  v_rejection jsonb;
  v_paper_pack jsonb;
begin
  v_primary:=private._candidate_timesheet_primary_action_v1(
    p_candidate_status_code,p_workflows,p_capabilities,p_timesheet_id,p_contract_week_id
  );
  if v_primary is not null then v_actions:=jsonb_build_array(v_primary); end if;

  for v_rejection in
    select item
    from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
    where item->>'state'='REJECTED'
      and coalesce((item->>'rejection_actionable')::boolean,false)
      and nullif(item->>'required_resubmission_action','') is not null
    order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
      item->>'updated_at_utc' desc,item->>'workflow_id'
  loop
    if not exists(
      select 1 from jsonb_array_elements(v_actions) existing
      where existing->>'workflow_id'=v_rejection->>'workflow_id'
        and existing->>'code'=v_rejection->>'required_resubmission_action'
    ) then
      v_action:=jsonb_build_object(
        'code',v_rejection->>'required_resubmission_action',
        'label',case v_rejection->>'required_resubmission_action'
          when 'RESUBMIT_EXPENSE_CLAIM' then 'Resubmit Expense Claim'
          when 'RESUBMIT_TIMESHEET_AND_EXPENSES' then 'Resubmit Timesheet and Expenses'
          else 'Resubmit Timesheet' end,
        'method','POST','path','/candidate-app/v1/workflows/'
          ||(v_rejection->>'workflow_id')||'/resubmit',
        'requires_confirmation',false,'requires_reason',false,'enabled',true,
        'disabled_reason',null,'workflow_id',v_rejection->>'workflow_id',
        'workflow_generation',nullif(v_rejection->>'generation','')::integer,
        'claim_family',v_rejection->>'claim_family'
      );
      v_actions:=v_actions||jsonb_build_array(v_action);
    end if;
  end loop;

  select item into v_workflow_json
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;

  if v_workflow_json is not null then
    select workflow_row.* into v_workflow
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=(v_workflow_json->>'workflow_id')::uuid;
  end if;

  if v_workflow.id is not null then
    v_cancel_eligible:=v_workflow.state not in (
      'FINALISED','CANCELLED','REJECTED','SUPERSEDED','EXPIRED'
    );
    v_cancel_code:=case when v_workflow.workflow_kind='CONTRACT_EXPENSE'
      then 'DISCARD_EXPENSE_CLAIM' else 'CANCEL_ENTIRE_CLAIM_AND_START_AGAIN' end;
    if v_cancel_eligible then
      v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
        'code',v_cancel_code,
        'label',case when v_cancel_code='DISCARD_EXPENSE_CLAIM'
          then 'Discard Expense Claim' else 'Cancel Entire Claim and Start Again' end,
        'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/cancel',
        'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
        'approval_request_id',null,'requires_confirmation',true,'requires_reason',true,
        'enabled',true,'disabled_reason',null
      ));
    end if;

    if v_workflow.route='EMAIL' and v_workflow.state='AWAITING_MANAGER_APPROVAL' then
      select request_row.* into v_approval
      from public.candidate_approval_requests request_row
      where request_row.workflow_id=v_workflow.id
        and request_row.workflow_generation=v_workflow.generation
        and request_row.method='EMAIL'
      order by request_row.request_generation desc,request_row.created_at_utc desc
      limit 1;
      if found then
        select
          min(delivery.sent_at) filter (
            where delivery.status='SENT' and delivery.sent_at is not null
              and upper(coalesce(delivery.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')
          ),
          max(delivery.sent_at) filter (
            where delivery.status='SENT' and delivery.sent_at is not null
              and upper(coalesce(delivery.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')
          ),
          count(*) filter (
            where delivery.status='QUEUED' and delivery.sent_at is null
              and lower(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_retired','false'))
                    in ('false','f','0','no')
          )::integer
        into v_provider_first_accepted_at,v_provider_accepted_at,v_pending_delivery_count
        from public.mail_outbox delivery
        where upper(coalesce(delivery.payment_scope_json->>'candidate_mail_authority',''))
                ='MANAGER_APPROVAL_V1'
          and delivery.payment_scope_json->>'candidate_manager_workflow_id'=v_workflow.id::text
          and delivery.payment_scope_json->>'candidate_manager_workflow_generation'=v_workflow.generation::text
          and delivery.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
          and delivery.payment_scope_json->>'candidate_approval_request_generation'=v_approval.request_generation::text
          and upper(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_kind',''))
                in ('INITIAL','REMINDER','RENEWAL');
        select delivery.status,delivery.provider_status
        into v_latest_delivery_state,v_latest_provider_status
        from public.mail_outbox delivery
        where upper(coalesce(delivery.payment_scope_json->>'candidate_mail_authority',''))
                ='MANAGER_APPROVAL_V1'
          and delivery.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
          and upper(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_kind',''))
                in ('INITIAL','REMINDER','RENEWAL')
        order by delivery.created_at_utc desc,delivery.id desc limit 1;

        v_effective_request_state:=case
          when v_approval.state='PENDING' and v_approval.expires_at_utc<=p_now_utc then 'EXPIRED'
          else v_approval.state end;
        v_next_reminder_at:=case when v_provider_accepted_at is null then null
          else v_provider_accepted_at+interval '24 hours' end;
        v_reminder_eligible:=v_effective_request_state='PENDING'
          and v_provider_accepted_at is not null
          and v_next_reminder_at<=p_now_utc
          and v_approval.resend_count<5
          and v_pending_delivery_count=0;
        v_renewal_eligible:=v_effective_request_state='EXPIRED'
          and v_approval.review_manifest_sha256 is not distinct from v_workflow.review_manifest_sha256;
        v_disabled_reason:=case
          when v_effective_request_state<>'PENDING' then 'MANAGER_APPROVAL_REQUEST_NOT_PENDING'
          when v_provider_accepted_at is null then 'MANAGER_EMAIL_NOT_PROVIDER_ACCEPTED'
          when v_pending_delivery_count>0 then 'MANAGER_EMAIL_DELIVERY_PENDING'
          when v_approval.resend_count>=5 then 'MANAGER_REMINDER_LIMIT_REACHED'
          when v_next_reminder_at>p_now_utc then 'MANAGER_REMINDER_WAIT_24_HOURS'
          else null end;
        v_manager_approval:=jsonb_build_object(
          'method','EMAIL','request_id',v_approval.id,
          'request_generation',v_approval.request_generation,
          'state',v_effective_request_state,
          'stored_state',v_approval.state,
          'provider_first_accepted_at_utc',v_provider_first_accepted_at,
          'provider_accepted_at_utc',v_provider_accepted_at,
          'delivery_state',v_latest_delivery_state,
          'provider_status',v_latest_provider_status,
          'delivery_pending',v_pending_delivery_count>0,
          'expires_at_utc',v_approval.expires_at_utc,
          'resend_count',v_approval.resend_count,
          'resends_remaining',greatest(5-v_approval.resend_count,0),
          'next_reminder_at_utc',v_next_reminder_at,
          'reminder_eligible',v_reminder_eligible,
          'renewal_eligible',v_renewal_eligible,
          'cancel_eligible',v_cancel_eligible
        );
        v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
          'code','SEND_MANAGER_REMINDER','label','Send Manager Reminder',
          'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/remind',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',v_approval.id,
          'approval_request_generation',v_approval.request_generation,
          'requires_confirmation',false,'requires_reason',false,
          'enabled',v_reminder_eligible,'disabled_reason',v_disabled_reason
        ));
        v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
          'code','REQUEST_APPROVAL_AGAIN','label','Request Approval Again',
          'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/renew',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',v_approval.id,
          'approval_request_generation',v_approval.request_generation,
          'requires_confirmation',true,'requires_reason',false,
          'enabled',v_renewal_eligible,
          'disabled_reason',case when v_renewal_eligible then null else 'MANAGER_APPROVAL_REQUEST_NOT_EXPIRED' end
        ));
      end if;
    end if;

    if v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN' then
      v_paper_pack:=private._candidate_paper_pack_readiness_v1(v_workflow.id,v_workflow.generation);
      v_actions:=v_actions||jsonb_build_array(
        jsonb_build_object(
          'code','DOWNLOAD_PAPER_DOCUMENTS','label','Download Documents','method','GET',
          'path','/candidate-app/v1/timesheets/'||coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)::text||'/paper-pack',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',null,'requires_confirmation',false,'requires_reason',false,
          'enabled',coalesce((v_paper_pack->>'download_available')::boolean,false),
          'disabled_reason',case when coalesce((v_paper_pack->>'download_available')::boolean,false)
            then null else v_paper_pack->>'reason_code' end
        ),
        jsonb_build_object(
          'code','UPLOAD_SIGNED_RETURN','label','Upload Signed Return','method','POST',
          'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/paper-return',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',null,'requires_confirmation',true,'requires_reason',false,
          'enabled',coalesce((v_paper_pack->>'upload_available')::boolean,false),
          'disabled_reason',case when coalesce((v_paper_pack->>'upload_available')::boolean,false)
            then null else v_paper_pack->>'reason_code' end
        )
      );
    elsif v_workflow.route='PAPER' and v_workflow.state='RECEIVED' then
      v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
        'code','RETRY_FINALISATION','label','Retry Finalisation','method','POST',
        'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/retry-finalisation',
        'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
        'approval_request_id',null,'requires_confirmation',false,'requires_reason',false,
        'enabled',true,'disabled_reason',null
      ));
    end if;
  end if;

  if coalesce((p_capabilities->>'candidate_no_work_allowed')::boolean,false) then
    v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
      'code','NO_WORK_THIS_WEEK','label','I Did Not Work This Week','method','POST',
      'path','/candidate-app/v1/contract-weeks/'||p_contract_week_id::text||'/no-work',
      'timesheet_id',p_timesheet_id,'contract_week_id',p_contract_week_id,
      'requires_confirmation',true,'requires_reason',false,'enabled',true,'disabled_reason',null
    ));
  end if;
  if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false)
     and coalesce(v_primary->>'code','')<>'ADD_EXPENSES' then
    v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
      'code','ADD_EXPENSES','label','Add Expenses','method','POST',
      'path','/candidate-app/v1/workflows','timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id,'requires_confirmation',false,
      'requires_reason',false,'enabled',true,'disabled_reason',null
    ));
  end if;
  select coalesce(jsonb_agg(private._candidate_action_invocation_v1(item)
    order by ordinal),'[]'::jsonb)
  into v_actions
  from jsonb_array_elements(v_actions) with ordinality action_item(item,ordinal);
  if v_primary is not null then
    v_primary:=private._candidate_action_invocation_v1(v_primary);
  end if;
  return jsonb_build_object(
    'primary_action',v_primary,
    'available_actions',v_actions,
    'manager_approval',v_manager_approval,
    'paper_pack',v_paper_pack
  );
end;
$function$;

-- private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
CREATE OR REPLACE FUNCTION private._candidate_timesheet_primary_action_v1(p_candidate_status_code text, p_workflows jsonb, p_capabilities jsonb, p_timesheet_id uuid, p_contract_week_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_workflow jsonb;
  v_action text;
begin
  if upper(coalesce(p_candidate_status_code,'')) in ('PAID','AUTHORISED','INVOICED_NOT_PAID') then
    return null;
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state'='REJECTED'
    and coalesce((item->>'rejection_actionable')::boolean,false)
    and nullif(item->>'required_resubmission_action','') is not null
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    v_action:=v_workflow->>'required_resubmission_action';
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'RESUBMIT_EXPENSE_CLAIM' then 'Resubmit Expense Claim'
        when 'RESUBMIT_TIMESHEET_AND_EXPENSES' then 'Resubmit Timesheet and Expenses'
        else 'Resubmit Timesheet' end,
      'method','POST',
      'path','/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/resubmit',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    v_action:=case
      when v_workflow->>'state'='REFUSED' then 'REVIEW_AND_RESUBMIT'
      when v_workflow->>'workflow_kind'='CONTRACT_EXPENSE' then 'CONTINUE_EXPENSE_CLAIM'
      else 'CONTINUE_TIMESHEET' end;
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'REVIEW_AND_RESUBMIT' then 'Review and Resubmit'
        when 'CONTINUE_EXPENSE_CLAIM' then 'Continue Expense Claim'
        else 'Continue Timesheet' end,
      'method',case when v_action='REVIEW_AND_RESUBMIT' then 'POST' else 'GET' end,
      'path',case when v_action='REVIEW_AND_RESUBMIT'
        then '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/actions/amend'
        else '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/timesheet-detail' end,
      'requires_confirmation',false,'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;
  if coalesce((p_capabilities->>'can_edit_hours')::boolean,false) then
    return jsonb_build_object(
      'code','ENTER_TIMESHEET','label','Enter Timesheet',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false) then
    return jsonb_build_object(
      'code','ADD_EXPENSES','label','Add Expenses',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  return null;
end;
$function$;

-- private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_timesheet_reject_rotate_v1(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_reason text, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
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
  if not found then raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002'; end if;
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
    v_current.booking_id,v_new_version,true,'RECEIVED',v_current.contract_id,'MANUAL',
    v_current.line_type,v_current.sheet_scope,v_current.occupant_key_norm,v_current.hospital_norm,
    v_current.ward_norm,v_current.job_title_norm,v_current.shift_label_norm,v_current.week_ending_date,
    null,null,null,null,null,null,'{}'::jsonb,'{}'::jsonb,null,null,null,null,
    null,null,'{}'::jsonb,null,null,null,null,p_now_utc,p_now_utc
  ) returning timesheet_id into v_new_timesheet_id;
  update public.contract_weeks set timesheet_id=v_new_timesheet_id,status='OPEN',
    day_entries_json='[]'::jsonb,totals_json='{}'::jsonb,updated_at=p_now_utc
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
    insert into public.ts_financials_outbox(timesheet_id,reason,attempt_count,next_attempt_at,last_error,created_at)
    values (v_new_timesheet_id,'REVOKED',0,p_now_utc,null,p_now_utc)
    on conflict on constraint uq_tsfin_outbox do nothing;
  end if;
  perform private._candidate_audit_v1('timesheet',v_new_timesheet_id::text,
    'CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED',
    jsonb_build_object('old_timesheet_id',v_current.timesheet_id,'old_version',v_current.version),
    jsonb_build_object('new_timesheet_id',v_new_timesheet_id,'new_version',v_new_version),
    btrim(p_reason),p_actor_user_id,null,p_now_utc);
  return v_new_timesheet_id;
end;
$function$;

-- private._candidate_week_ending_label_v1(date)
CREATE OR REPLACE FUNCTION private._candidate_week_ending_label_v1(p_date date)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
  select case when p_date is null then null else
    'Week Ending '||extract(day from p_date)::integer::text||' '
    ||(array['January','February','March','April','May','June','July','August',
              'September','October','November','December'])[extract(month from p_date)::integer]
    ||' '||extract(year from p_date)::integer::text end
$function$;

-- private._candidate_week_schedule_from_template_v1(jsonb,date,date,date)
CREATE OR REPLACE FUNCTION private._candidate_week_schedule_from_template_v1(p_template jsonb, p_week_ending_date date, p_contract_start date, p_contract_end date)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
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

-- private._candidate_weekly_final_state_guard_v1(uuid,uuid,jsonb,jsonb,jsonb)
CREATE OR REPLACE FUNCTION private._candidate_weekly_final_state_guard_v1(p_contract_week_id uuid, p_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
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

-- private._candidate_workflow_creation_identity_guard_v1()
CREATE OR REPLACE FUNCTION private._candidate_workflow_creation_identity_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
begin
  if old.creation_request_sha256 is not null
     and (
       new.creation_request_sha256 is distinct from old.creation_request_sha256
       or new.creation_identity_json is distinct from old.creation_identity_json
     ) then
    raise exception 'CANDIDATE_CREATION_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if (new.creation_request_sha256 is null)<>(new.creation_identity_json is null) then
    raise exception 'CANDIDATE_CREATION_IDENTITY_INVALID' using errcode='22023';
  end if;
  return new;
end;
$function$;

-- private._candidate_workflow_creation_request_sha256_v1(jsonb)
CREATE OR REPLACE FUNCTION private._candidate_workflow_creation_request_sha256_v1(p_request_identity jsonb)
 RETURNS bytea
 LANGUAGE sql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
  select extensions.digest(
    convert_to(coalesce(p_request_identity,'{}'::jsonb)::text,'UTF8'),
    'sha256'
  );
$function$;

-- private._candidate_workflow_maps_to_card_v1(uuid,uuid,uuid)
CREATE OR REPLACE FUNCTION private._candidate_workflow_maps_to_card_v1(p_workflow_id uuid, p_timesheet_id uuid, p_contract_week_id uuid)
 RETURNS boolean
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_card_timesheet public.timesheets%rowtype;
  v_anchor public.timesheets%rowtype;
  v_target public.timesheets%rowtype;
  v_parent public.timesheets%rowtype;
begin
  if p_workflow_id is null or p_contract_week_id is null then return false; end if;
  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id;
  if not found then return false; end if;
  if v_workflow.contract_week_id=p_contract_week_id then return true; end if;
  if p_timesheet_id is null
     or not (v_workflow.workflow_kind='CONTRACT_EXPENSE'
       or v_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM') then return false; end if;
  select * into v_card_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then return false; end if;
  if v_workflow.anchor_timesheet_id is not null then
    select * into v_anchor from public.timesheets where timesheet_id=v_workflow.anchor_timesheet_id;
    if found and (
      v_anchor.timesheet_id=v_card_timesheet.timesheet_id
      or (nullif(btrim(coalesce(v_anchor.booking_id,'')),'') is not null
        and v_anchor.booking_id=v_card_timesheet.booking_id
        and v_anchor.contract_id is not distinct from v_card_timesheet.contract_id
        and v_anchor.week_ending_date is not distinct from v_card_timesheet.week_ending_date)
    ) then return true; end if;
  end if;
  if v_workflow.target_timesheet_id is not null then
    select * into v_target from public.timesheets where timesheet_id=v_workflow.target_timesheet_id;
    if found and v_target.parent_timesheet_id is not null then
      select * into v_parent from public.timesheets where timesheet_id=v_target.parent_timesheet_id;
      if found and (
        v_parent.timesheet_id=v_card_timesheet.timesheet_id
        or (nullif(btrim(coalesce(v_parent.booking_id,'')),'') is not null
          and v_parent.booking_id=v_card_timesheet.booking_id
          and v_parent.contract_id is not distinct from v_card_timesheet.contract_id
          and v_parent.week_ending_date is not distinct from v_card_timesheet.week_ending_date)
      ) then return true; end if;
    end if;
  end if;
  return false;
end;
$function$;

-- private._candidate_workflow_mutation_receipt_v1(uuid,text,text,text,text,text,jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_workflow_mutation_receipt_v1(p_workflow_id uuid, p_idempotency_key text, p_request_sha256 text, p_action text, p_channel text, p_actor_identity text, p_response jsonb DEFAULT NULL::jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_before jsonb;
  v_after jsonb;
  v_receipt_workflow_id text;
  v_environment text;
  v_actor_user_id uuid;
begin
  if p_workflow_id is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null
     or coalesce(p_request_sha256,'') !~ '^[0-9a-f]{64}$'
     or nullif(btrim(coalesce(p_action,'')),'') is null
     or nullif(btrim(coalesce(p_channel,'')),'') is null then
    raise exception 'CANDIDATE_IDEMPOTENCY_RECEIPT_INVALID' using errcode='22023';
  end if;

  select w.environment into v_environment
  from public.candidate_submission_workflows w where w.id=p_workflow_id;
  if v_environment is null then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-workflow-mutation-key:'||v_environment||':'||btrim(p_idempotency_key),0
  ));

  select ae.object_id_text,ae.before_json,ae.after_json
  into v_receipt_workflow_id,v_before,v_after
  from public.audit_events ae
  join public.candidate_submission_workflows rw
    on rw.id::text=ae.object_id_text and rw.environment=v_environment
  where ae.object_type='candidate_workflow_mutation_receipt'
    and ae.correlation_id=btrim(p_idempotency_key)
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    if v_receipt_workflow_id is distinct from p_workflow_id::text
       or v_before->>'request_sha256' is distinct from p_request_sha256 then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_IDEMPOTENCY_CONFLICT',
          'workflow_id',p_workflow_id,
          'receipt_workflow_id',v_receipt_workflow_id,
          'idempotency_key',btrim(p_idempotency_key)
        )::text;
    end if;
    if upper(btrim(p_action))='PAPER_PACK_ATTEMPT_CLAIM' then
      v_after:=coalesce(v_after,'{}'::jsonb)||jsonb_build_object(
        'claim_acquired_new',false,
        'idempotent_replay',true
      );
    end if;
    return jsonb_build_object(
      'found',true,
      'response',coalesce(v_after,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true)
    );
  end if;
  if p_response is null then
    return jsonb_build_object('found',false);
  end if;

  if coalesce(p_actor_identity,'')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    v_actor_user_id:=p_actor_identity::uuid;
  end if;
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    correlation_id,ts_utc
  ) values (
    v_actor_user_id,'candidate_workflow_mutation_receipt',p_workflow_id::text,
    'CANDIDATE_WORKFLOW_MUTATION_RECEIPT',jsonb_build_object(
      'request_sha256',p_request_sha256,
      'workflow_action',upper(btrim(p_action)),
      'channel',upper(btrim(p_channel)),
      'actor_identity',nullif(btrim(coalesce(p_actor_identity,'')),'')
    ),p_response,btrim(p_idempotency_key),coalesce(p_now_utc,now())
  );
  return jsonb_build_object('found',false,'recorded',true);
end;
$function$;

-- private._invoice_batch_canonical_text_v2(jsonb)
CREATE OR REPLACE FUNCTION private._invoice_batch_canonical_text_v2(p_value jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
  select coalesce(p_value, 'null'::jsonb)::text;
$function$;

-- private._invoice_batch_generate_candidate_key_rows_v2(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_generate_candidate_key_rows_v2(p_query jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(selection_key text, scope_key text, row_kind text, invoice_id uuid, source_revision text, client_id uuid, client_name text, candidate_ids jsonb, candidate_display text, week_ending_date date, currency text, invoice_stream text, total_ex_vat numeric, total_inc_vat numeric, row_status_seed text, blocker_codes_seed jsonb, is_early boolean, sort_date_key date, sort_text_key text, sort_numeric_key numeric, page_ordinal bigint, full_scope_count bigint, candidate_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with
query_input as materialized (
  select
    coalesce(p_query,'{}'::jsonb) query_json
),
params as materialized (
  select
    upper(coalesce(query_json->>'mode','PAGE')) mode,
    greatest(1,case
      when coalesce(query_json->>'page_size','')~'^[1-9][0-9]*$'
        then (query_json->>'page_size')::integer
      else 100
    end) page_size,
    coalesce(
      (query_json#>>'{filters,allow_early}')::boolean,
      false
    ) allow_early,
    upper(coalesce(
      nullif(query_json#>>'{filters,display_mode}',''),
      'ALL'
    )) display_mode,
    lower(nullif(btrim(coalesce(
      query_json#>>'{filters,search}',
      ''
    )),'')) search_text,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{filters,week_ending_from}',''),
        'date'
      )
        then (query_json#>>'{filters,week_ending_from}')::date
    end week_ending_from,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{filters,week_ending_to}',''),
        'date'
      )
        then (query_json#>>'{filters,week_ending_to}')::date
    end week_ending_to,
    coalesce(query_json#>'{filters,client_ids}','[]'::jsonb)
      client_ids,
    coalesce(query_json#>'{filters,candidate_ids}','[]'::jsonb)
      candidate_ids,
    coalesce(query_json#>'{filters,week_endings}','[]'::jsonb)
      week_endings,
    coalesce(query_json#>'{filters,status_codes}','[]'::jsonb)
      status_codes,
    coalesce(query_json#>'{filters,blocker_codes}','[]'::jsonb)
      blocker_codes,
    coalesce(query_json#>'{filters,invoice_streams}','[]'::jsonb)
      invoice_streams,
    coalesce(query_json->'selection_keys','[]'::jsonb)
      selection_keys,
    upper(coalesce(
      nullif(query_json#>>'{sort,sort_key}',''),
      'WEEK_ENDING_DATE'
    )) sort_key,
    case
      when upper(coalesce(
        query_json#>>'{sort,sort_direction}',
        'ASC'
      ))='DESC'
        then 'DESC'
      else 'ASC'
    end sort_direction,
    nullif(query_json#>>'{cursor,after_selection_key}','')
      after_selection_key,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{cursor,after_sort_date}',''),
        'date'
      )
        then (query_json#>>'{cursor,after_sort_date}')::date
    end after_sort_date,
    nullif(query_json#>>'{cursor,after_sort_text}','')
      after_sort_text,
    case
      when coalesce(
        query_json#>>'{cursor,after_sort_numeric}',
        ''
      )~'^[+-]?[0-9]+([.][0-9]+)?$'
        then (
          query_json#>>'{cursor,after_sort_numeric}'
        )::numeric
    end after_sort_numeric
  from query_input
),
classified as materialized (
  select
    candidate.candidate_json,
    candidate.candidate_json->>'selection_key' selection_key,
    candidate.candidate_json->>'scope_key' scope_key,
    candidate.candidate_json->>'row_kind' row_kind,
    case
      when pg_input_is_valid(
        coalesce(candidate.candidate_json->>'invoice_id',''),
        'uuid'
      )
        then (candidate.candidate_json->>'invoice_id')::uuid
    end invoice_id,
    candidate.candidate_json->>'source_revision' source_revision,
    (candidate.candidate_json->>'client_id')::uuid client_id,
    candidate.candidate_json->>'client_name' client_name,
    coalesce(
      candidate.candidate_json->'candidate_ids',
      '[]'::jsonb
    ) candidate_ids,
    candidate.candidate_json->>'candidate_display' candidate_display,
    case
      when pg_input_is_valid(
        coalesce(candidate.candidate_json->>'week_ending_date',''),
        'date'
      )
        then (candidate.candidate_json->>'week_ending_date')::date
    end week_ending_date,
    coalesce(
      nullif(candidate.candidate_json->>'currency',''),
      'GBP'
    ) currency,
    upper(coalesce(
      nullif(candidate.candidate_json->>'invoice_stream',''),
      'NORMAL'
    )) invoice_stream,
    coalesce(
      (candidate.candidate_json->>'total_ex_vat')::numeric,
      0
    ) total_ex_vat,
    coalesce(
      (candidate.candidate_json->>'total_inc_vat')::numeric,
      0
    ) total_inc_vat,
    candidate.candidate_json->>'row_status' row_status,
    coalesce(
      candidate.candidate_json->'action_blocker_codes',
      '[]'::jsonb
    ) action_blocker_codes,
    coalesce(
      candidate.candidate_json->'informational_codes',
      '[]'::jsonb
    ) informational_codes,
    coalesce(
      (candidate.candidate_json->>'selectable')::boolean,
      false
    ) selectable,
    coalesce(
      (candidate.candidate_json->>'is_early')::boolean,
      false
    ) is_early
  from private._invoice_batch_generate_classification_v2(
    true,
    null,
    coalesce(p_now_utc,statement_timestamp())
  ) candidate
  where candidate.candidate_json->>'row_kind'='CREATE_INVOICE'
    and not (
      coalesce(candidate.candidate_json->'action_blocker_codes','[]'::jsonb)
      ?| array[
        'SEGMENT_ALREADY_LOCKED',
        'SOURCE_ALREADY_LOCKED',
        'SOURCE_ALREADY_INVOICED'
      ]
    )
),
selection_rules as materialized (
  select rule.*
  from query_input
  cross join lateral private._invoice_batch_selection_rules_v2(
    coalesce(
      query_input.query_json->'selection',
      jsonb_build_object(
        'contract_version','INVOICE_BATCH_SELECTION_V2',
        'mode','IMPLICIT_ALL',
        'default_selected',true,
        'rules','[]'::jsonb
      )
    )
  ) rule
),
classified_with_selection as materialized (
  select
    classified.*,
    coalesce((
      select rule.action
      from selection_rules rule
      where (rule.selector_type='ROW'
          and rule.selection_key=classified.selection_key)
         or (rule.selector_type='WEEK'
          and rule.week_ending_date=classified.week_ending_date)
         or (rule.selector_type='CLIENT'
          and rule.client_id=classified.client_id)
         or (rule.selector_type='CANDIDATE' and exists(
           select 1
           from jsonb_array_elements_text(
             classified.candidate_ids
           ) candidate(value)
           where pg_input_is_valid(candidate.value,'uuid')
             and candidate.value::uuid=rule.candidate_id
         ))
         or (rule.selector_type='STATUS'
          and rule.status_code=classified.row_status)
         or (rule.selector_type='WEEK_CLIENT'
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id)
         or (rule.selector_type='WEEK_CLIENT_CANDIDATE'
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id
          and exists(
            select 1
            from jsonb_array_elements_text(
              classified.candidate_ids
            ) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=rule.candidate_id
          ))
         or (rule.selector_type='STATUS_WEEK'
          and rule.status_code=classified.row_status
          and rule.week_ending_date=classified.week_ending_date)
         or (rule.selector_type='STATUS_WEEK_CLIENT'
          and rule.status_code=classified.row_status
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id)
         or (rule.selector_type='DIMENSION_GROUP'
          and (rule.week_ending_date is null or rule.week_ending_date=classified.week_ending_date)
          and (rule.client_id is null or rule.client_id=classified.client_id)
          and (rule.status_code is null or rule.status_code=classified.row_status)
          and (rule.candidate_id is null or exists (
            select 1 from jsonb_array_elements_text(classified.candidate_ids) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=rule.candidate_id
          )))
      order by rule.rule_sequence desc
      limit 1
    ),'INCLUDE') last_selection_action
  from classified
),
filtered as materialized (
  select classified.*
  from classified_with_selection classified
  cross join params
  where (params.allow_early or not classified.is_early)
    and (
      jsonb_array_length(params.client_ids)=0
      or classified.client_id::text in (
        select value
        from jsonb_array_elements_text(params.client_ids) value
      )
    )
    and (
      jsonb_array_length(params.candidate_ids)=0
      or exists(
        select 1
        from jsonb_array_elements_text(
          classified.candidate_ids
        ) candidate(value)
        where candidate.value in (
          select value
          from jsonb_array_elements_text(params.candidate_ids) value
        )
      )
    )
    and (
      jsonb_array_length(params.week_endings)=0
      or classified.week_ending_date::text in (
        select value
        from jsonb_array_elements_text(params.week_endings) value
      )
    )
    and (
      jsonb_array_length(params.status_codes)=0
      or classified.row_status in (
        select upper(value)
        from jsonb_array_elements_text(params.status_codes) value
      )
    )
    and (
      jsonb_array_length(params.blocker_codes)=0
      or exists(
        select 1
        from jsonb_array_elements_text(
          classified.action_blocker_codes
          ||classified.informational_codes
        ) code(value)
        where code.value in (
          select upper(value)
          from jsonb_array_elements_text(params.blocker_codes) value
        )
      )
    )
    and (
      jsonb_array_length(params.invoice_streams)=0
      or classified.invoice_stream in (
        select upper(value)
        from jsonb_array_elements_text(params.invoice_streams) value
      )
    )
    and (
      params.mode<>'EXPLICIT_KEYS'
      or classified.selection_key in (
        select value
        from jsonb_array_elements_text(params.selection_keys) value
      )
    )
    and (
      params.search_text is null
      or lower(
        coalesce(classified.client_name,'')||' '||
        coalesce(classified.candidate_display,'')||' '||
        coalesce(classified.scope_key,'')
      ) like '%'||params.search_text||'%'
    )
    and (
      params.week_ending_from is null
      or classified.week_ending_date>=params.week_ending_from
    )
    and (
      params.week_ending_to is null
      or classified.week_ending_date<=params.week_ending_to
    )
    and (
      params.mode='EXPAND_SELECTION'
      or params.display_mode='ALL'
      or (
        params.display_mode='READY'
        and classified.selectable
      )
      or (
        params.display_mode='BLOCKED'
        and classified.row_status='BLOCKED'
      )
    )
    and (
      params.mode<>'EXPAND_SELECTION'
      or (
        classified.selectable
        and classified.last_selection_action<>'EXCLUDE'
      )
    )
),
sortable as materialized (
  select
    filtered.*,
    case
      when params.sort_key='WEEK_ENDING_DATE'
        then coalesce(
          filtered.week_ending_date,
          case
            when params.sort_direction='DESC'
              then date '0001-01-01'
            else date '9999-12-31'
          end
        )
    end sort_date_key,
    case
      when params.sort_key='CLIENT_NAME'
        then lower(coalesce(filtered.client_name,''))
      when params.sort_key='CANDIDATE_NAME'
        then lower(coalesce(filtered.candidate_display,''))
      when params.sort_key='STATUS'
        then lpad((case filtered.row_status
          when 'READY' then 10
          when 'STALE' then 20
          when 'FAILED' then 30
          when 'IN_PROGRESS' then 40
          else 50
        end)::text,3,'0')||'|'||lower(filtered.row_status)
    end sort_text_key,
    case
      when params.sort_key='TOTAL_EX_VAT'
        then filtered.total_ex_vat
      when params.sort_key='TOTAL_INC_VAT'
        then filtered.total_inc_vat
    end sort_numeric_key
  from filtered
  cross join params
),
scope_count as materialized (
  select count(*)::bigint full_scope_count
  from sortable
),
cursor_filtered as materialized (
  select sortable.*
  from sortable
  cross join params
  where params.after_selection_key is null
     or (
       params.mode='EXPAND_SELECTION'
       and sortable.selection_key>params.after_selection_key
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key='WEEK_ENDING_DATE'
       and params.after_sort_date is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_date_key>params.after_sort_date
             or (
               sortable.sort_date_key=params.after_sort_date
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_date_key<params.after_sort_date
             or (
               sortable.sort_date_key=params.after_sort_date
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key in(
         'CLIENT_NAME','CANDIDATE_NAME','STATUS'
       )
       and params.after_sort_text is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_text_key>params.after_sort_text
             or (
               sortable.sort_text_key=params.after_sort_text
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_text_key<params.after_sort_text
             or (
               sortable.sort_text_key=params.after_sort_text
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key in('TOTAL_EX_VAT','TOTAL_INC_VAT')
       and params.after_sort_numeric is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_numeric_key>params.after_sort_numeric
             or (
               sortable.sort_numeric_key=params.after_sort_numeric
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_numeric_key<params.after_sort_numeric
             or (
               sortable.sort_numeric_key=params.after_sort_numeric
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
),
ordered as materialized (
  select
    cursor_filtered.*,
    row_number() over(order by
      case
        when params.mode='EXPAND_SELECTION'
          then cursor_filtered.selection_key
      end asc,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key='WEEK_ENDING_DATE'
         and params.sort_direction='ASC'
          then cursor_filtered.sort_date_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key='WEEK_ENDING_DATE'
         and params.sort_direction='DESC'
          then cursor_filtered.sort_date_key
      end desc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'CLIENT_NAME','CANDIDATE_NAME','STATUS'
         )
         and params.sort_direction='ASC'
          then cursor_filtered.sort_text_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'CLIENT_NAME','CANDIDATE_NAME','STATUS'
         )
         and params.sort_direction='DESC'
          then cursor_filtered.sort_text_key
      end desc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'TOTAL_EX_VAT','TOTAL_INC_VAT'
         )
         and params.sort_direction='ASC'
          then cursor_filtered.sort_numeric_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'TOTAL_EX_VAT','TOTAL_INC_VAT'
         )
         and params.sort_direction='DESC'
          then cursor_filtered.sort_numeric_key
      end desc nulls last,
      cursor_filtered.selection_key
    ) page_ordinal
  from cursor_filtered
  cross join params
)
select
  ordered.selection_key,
  ordered.scope_key,
  ordered.row_kind,
  ordered.invoice_id,
  ordered.source_revision,
  ordered.client_id,
  ordered.client_name,
  ordered.candidate_ids,
  ordered.candidate_display,
  ordered.week_ending_date,
  ordered.currency,
  ordered.invoice_stream,
  ordered.total_ex_vat,
  ordered.total_inc_vat,
  ordered.row_status row_status_seed,
  ordered.action_blocker_codes blocker_codes_seed,
  ordered.is_early,
  ordered.sort_date_key,
  ordered.sort_text_key,
  ordered.sort_numeric_key,
  ordered.page_ordinal,
  scope_count.full_scope_count,
  ordered.candidate_json
from ordered
cross join params
cross join scope_count
where ordered.page_ordinal<=case
  when params.mode='EXPLICIT_KEYS'
    then jsonb_array_length(params.selection_keys)
  else params.page_size+1
end
order by ordered.page_ordinal;
$function$;

-- private._invoice_batch_generate_candidate_keys_v2(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_generate_candidate_keys_v2(p_query jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(selection_key text, scope_key text, row_kind text, invoice_id uuid, source_revision text, client_id uuid, client_name text, candidate_ids jsonb, candidate_display text, week_ending_date date, currency text, invoice_stream text, total_ex_vat numeric, total_inc_vat numeric, row_status_seed text, blocker_codes_seed jsonb, is_early boolean, sort_date_key date, sort_text_key text, sort_numeric_key numeric, page_ordinal bigint, full_scope_count bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
select
  candidate.selection_key,
  candidate.scope_key,
  candidate.row_kind,
  candidate.invoice_id,
  candidate.source_revision,
  candidate.client_id,
  candidate.client_name,
  candidate.candidate_ids,
  candidate.candidate_display,
  candidate.week_ending_date,
  candidate.currency,
  candidate.invoice_stream,
  candidate.total_ex_vat,
  candidate.total_inc_vat,
  candidate.row_status_seed,
  candidate.blocker_codes_seed,
  candidate.is_early,
  candidate.sort_date_key,
  candidate.sort_text_key,
  candidate.sort_numeric_key,
  candidate.page_ordinal,
  candidate.full_scope_count
from private._invoice_batch_generate_candidate_key_rows_v2(
  p_query,
  p_now_utc
) candidate
order by candidate.page_ordinal
$function$;

-- private._invoice_batch_generate_candidate_rows_v1(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_generate_candidate_rows_v1(p_query jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_selection jsonb;
  v_page_size integer;
  v_after_key text;
  v_allow_early boolean;
  v_display_mode text;
  v_fetch_limit integer;
  v_legacy jsonb;
  v_result jsonb;
begin
  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if coalesce(v_query->>'contract_version','INVOICE_BATCH_QUERY_V1') <> 'INVOICE_BATCH_QUERY_V1' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  v_mode := upper(coalesce(nullif(v_query->>'mode',''),'PAGE'));
  if v_mode not in ('PAGE','FACETS','EXPAND_SELECTION','EXPLICIT_KEYS') then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_filters := case when jsonb_typeof(v_query->'filters') = 'object' then v_query->'filters' else '{}'::jsonb end;
  v_sort := case when jsonb_typeof(v_query->'sort') = 'object' then v_query->'sort' else '{}'::jsonb end;
  v_selection := case when jsonb_typeof(v_query->'selection') = 'object' then v_query->'selection' else jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V1',
    'mode','IMPLICIT_ALL',
    'default_selected',true,
    'rules','[]'::jsonb
  ) end;

  -- Validate the selection ledger up-front even when the current mode only pages rows.
  perform 1 from private._invoice_batch_selection_rules_v1(v_selection) limit 1;

  v_allow_early := lower(coalesce(v_query->>'allow_early',v_filters->>'allow_early','false')) in ('true','t','1','yes','on');
  v_display_mode := upper(coalesce(nullif(v_query->>'display_mode',''),nullif(v_filters->>'display_mode',''),'ALL'));
  if v_display_mode not in ('ALL','READY','BLOCKED') then
    raise exception using errcode='22023', message='INVOICE_BATCH_DISPLAY_MODE_INVALID';
  end if;

  if upper(coalesce(v_sort->>'group_preset','WEEK_CLIENT_CANDIDATE')) not in (
    'WEEK_CLIENT_CANDIDATE','CLIENT_WEEK_CANDIDATE','CANDIDATE_WEEK_CLIENT','STATUS_WEEK_CLIENT'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_GROUP_PRESET_INVALID';
  end if;

  if upper(coalesce(v_sort->>'sort_key','WEEK_ENDING_DATE')) not in (
    'WEEK_ENDING_DATE','CLIENT_NAME','CANDIDATE_NAME','TOTAL_EX_VAT','TOTAL_INC_VAT','STATUS'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SORT_KEY_INVALID';
  end if;

  v_page_size := case
    when coalesce(v_query->>'page_size','') ~ '^[1-9][0-9]{0,8}$'
      then greatest(1,least((v_query->>'page_size')::integer,100))
    else 100
  end;
  v_after_key := nullif(coalesce(v_query#>>'{cursor,after_selection_key}',v_query#>>'{cursor,last_selection_key}',v_query->>'after_selection_key'), '');
  v_fetch_limit := case when v_mode = 'PAGE' then 20000 else 20000 end;

  -- Reuse the existing canonical candidate authority for the underlying grouping/economics.
  -- The public candidate RPC must keep its legacy NULL-p_query branch when it is later extended
  -- to call this helper, otherwise this helper would recurse.
  -- Always ask the legacy authority for the wider set; this helper applies the
  -- locked Batch early visibility rule itself so early rows are invisible when
  -- allow_early=false instead of being shown as blocked.
  v_legacy := public.invoice_batch_generate_candidates(true, v_fetch_limit, null::text[]);

  with
  params as materialized (
    select
      v_mode mode,
      v_allow_early allow_early,
      v_display_mode display_mode,
      v_page_size page_size,
      v_after_key after_key,
      lower(nullif(btrim(coalesce(v_filters->>'search','')),'')) search_text,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_from',''),'date')
        then (v_filters->>'week_ending_from')::date end week_ending_from,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_to',''),'date')
        then (v_filters->>'week_ending_to')::date end week_ending_to,
      (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date today,
      case when jsonb_typeof(v_filters->'client_ids')='array' then v_filters->'client_ids' else '[]'::jsonb end client_ids,
      case when jsonb_typeof(v_filters->'candidate_ids')='array' then v_filters->'candidate_ids' else '[]'::jsonb end candidate_ids,
      case when jsonb_typeof(v_filters->'week_endings')='array' then v_filters->'week_endings' else '[]'::jsonb end week_endings,
      case when jsonb_typeof(v_filters->'status_codes')='array' then v_filters->'status_codes' else '[]'::jsonb end status_codes,
      case when jsonb_typeof(v_filters->'blocker_codes')='array' then v_filters->'blocker_codes' else '[]'::jsonb end blocker_codes,
      coalesce(nullif(upper(v_sort->>'group_preset'),''),'WEEK_CLIENT_CANDIDATE') group_preset,
      coalesce(nullif(upper(v_sort->>'sort_key'),''),'WEEK_ENDING_DATE') sort_key,
      case when upper(coalesce(v_sort->>'sort_direction','ASC'))='DESC' then 'DESC' else 'ASC' end sort_direction,
      case when pg_input_is_valid(
        nullif(coalesce(v_query#>>'{cursor,after_sort_date}',v_query#>>'{cursor,last_sort_date}',''),''),
        'date'
      )
        then coalesce(v_query#>>'{cursor,after_sort_date}',v_query#>>'{cursor,last_sort_date}')::date end after_sort_date,
      nullif(coalesce(v_query#>>'{cursor,after_sort_text}',v_query#>>'{cursor,last_sort_text}',''),'') after_sort_text,
      case when coalesce(v_query#>>'{cursor,after_sort_numeric}',v_query#>>'{cursor,last_sort_numeric}','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then coalesce(v_query#>>'{cursor,after_sort_numeric}',v_query#>>'{cursor,last_sort_numeric}')::numeric end after_sort_numeric
  ),
  selection_rules as materialized (
    select * from private._invoice_batch_selection_rules_v1(v_selection)
  ),
  legacy_clients as materialized (
    select client.value client_json
    from jsonb_array_elements(case when jsonb_typeof(v_legacy)='array' then v_legacy else '[]'::jsonb end) client(value)
  ),
  legacy_groups as materialized (
    select
      client_json,
      client_json->>'client_id' client_id_text,
      client_json->>'client_name' client_name,
      grp.value group_json
    from legacy_clients
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(client_json->'groups')='array' then client_json->'groups' else '[]'::jsonb end
    ) grp(value)
  ),
  generation_timesheets as materialized (
    select
      lg.group_json->>'group_key' group_key,
      t.value timesheet_json,
      case when pg_input_is_valid(t.value->>'timesheet_id','uuid') then (t.value->>'timesheet_id')::uuid end timesheet_id,
      t.value->>'candidate_name' candidate_name,
      case when pg_input_is_valid(t.value->>'week_ending_date','date') then (t.value->>'week_ending_date')::date end week_ending_date,
      case when coalesce(t.value->>'total_charge_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (t.value->>'total_charge_ex_vat')::numeric else 0 end total_ex_vat
    from legacy_groups lg
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(lg.group_json->'timesheets')='array' then lg.group_json->'timesheets' else '[]'::jsonb end
    ) t(value)
  ),
  generation_candidate_ids as materialized (
    select gt.group_key,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(gt.candidate_name)) filter(where nullif(gt.candidate_name,'') is not null),'[]'::jsonb) candidate_names
    from generation_timesheets gt
    left join public.v_timesheets_summary_base s on s.timesheet_id=gt.timesheet_id
    group by gt.group_key
  ),
  generation_vat_members as materialized (
    select distinct on (lg.group_json->>'group_key', gt.timesheet_id)
      lg.group_json->>'group_key' group_key,
      gt.timesheet_id,
      coalesce(gt.total_ex_vat,0) total_ex_vat,
      coalesce(vat.vat_rate,0) vat_rate
    from legacy_groups lg
    left join lateral jsonb_array_elements(
      case when jsonb_typeof(lg.group_json->'canonical_source_members')='array'
        then lg.group_json->'canonical_source_members' else '[]'::jsonb end
    ) member(value) on true
    left join generation_timesheets gt
      on gt.group_key=lg.group_json->>'group_key'
     and gt.timesheet_id=case when pg_input_is_valid(coalesce(member.value->>'related_timesheet_id',member.value->>'source_id',''),'uuid')
       then coalesce(member.value->>'related_timesheet_id',member.value->>'source_id')::uuid end
    left join lateral private._invoice_generation_vat_policy_batch(jsonb_build_array(jsonb_build_object(
      'source_member_key',coalesce(nullif(member.value->>'source_member_key',''), lg.group_json->>'group_key'||':'||coalesce(gt.timesheet_id::text,'')),
      'source_type',member.value->>'source_type',
      'source_id',member.value->>'source_id',
      'timesheet_id',coalesce(member.value->>'related_timesheet_id',member.value->>'source_id'),
      'segment_id',nullif(member.value->>'segment_id',''),
      'effective_date',member.value->>'effective_settings_date'
    ))) vat on true
    where gt.timesheet_id is not null
    order by lg.group_json->>'group_key', gt.timesheet_id, coalesce(vat.vat_rate,0) desc
  ),
  generation_vat as materialized (
    select group_key,
      round(coalesce(sum(total_ex_vat * vat_rate / 100.0),0),2) vat_amount
    from generation_vat_members
    group by group_key
  ),
  create_rows as materialized (
    select
      'generate:'||coalesce(lg.group_json->>'group_key','') selection_key,
      'CREATE_INVOICE' row_kind,
      lg.group_json->>'group_key' scope_key,
      null::uuid invoice_id,
      case when pg_input_is_valid(lg.client_id_text,'uuid') then lg.client_id_text::uuid end client_id,
      lg.client_name,
      coalesce(gci.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(gci.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(gci.candidate_names,'[]'::jsonb))=1
        then gci.candidate_names->>0
        when jsonb_array_length(coalesce(gci.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(gci.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      coalesce((
        select jsonb_agg(distinct to_jsonb(gt.week_ending_date) order by to_jsonb(gt.week_ending_date))
        from generation_timesheets gt where gt.group_key=lg.group_json->>'group_key' and gt.week_ending_date is not null
      ),'[]'::jsonb) week_ending_dates,
      case when pg_input_is_valid(lg.group_json->>'week_ending_date','date') then (lg.group_json->>'week_ending_date')::date end week_ending_date,
      coalesce(nullif(lg.group_json#>>'{command_payload,currency}',''),'GBP') currency,
      case when coalesce(lg.group_json->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then round((lg.group_json->>'subtotal_ex_vat')::numeric,2) else 0 end total_ex_vat,
      coalesce(gv.vat_amount,0) vat_amount,
      round((case when coalesce(lg.group_json->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (lg.group_json->>'subtotal_ex_vat')::numeric else 0 end) + coalesce(gv.vat_amount,0),2) total_inc_vat,
      'NOT_GENERATED' generation_state,
      coalesce(lg.group_json->>'blocker_code','') primary_blocker_code,
      coalesce((
        select jsonb_agg(to_jsonb(blocker_code) order by blocker_order, blocker_code)
        from (
          select blocker_code, min(blocker_order) blocker_order
          from (
            select 1 blocker_order, nullif(lg.group_json->>'blocker_code','') blocker_code
            union all
            select 2, nullif(lg.group_json#>>'{blocker_detail,code}','')
            union all
            select 10 + arr.ordinality::integer, nullif(arr.value,'')
            from jsonb_array_elements_text(
              case when jsonb_typeof(lg.group_json->'blocker_codes')='array'
                then lg.group_json->'blocker_codes' else '[]'::jsonb end
            ) with ordinality arr(value, ordinality)
            union all
            select 100 + src.ordinality::integer, nullif(src.value->>'code','')
            from jsonb_array_elements(
              case when jsonb_typeof(lg.group_json#>'{blocker_detail,sources}')='array'
                then lg.group_json#>'{blocker_detail,sources}' else '[]'::jsonb end
            ) with ordinality src(value, ordinality)
          ) raw_codes
          where blocker_code is not null
            and blocker_code not in ('EARLY_GENERATION_NOT_ALLOWED','SOURCE_ALREADY_INVOICED')
          group by blocker_code
        ) blocker_rows
      ),'[]'::jsonb) action_blocker_codes,
      (case when coalesce(lg.group_json->>'active_generation_status','') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        then jsonb_build_array('GENERATING') else '[]'::jsonb end)
      || (case when
          coalesce(lg.group_json->>'blocker_code','')='EXPENSE_INVOICE_EMAIL_REQUIRED'
          or coalesce(lg.group_json#>>'{blocker_detail,code}','')='EXPENSE_INVOICE_EMAIL_REQUIRED'
          or exists (
            select 1
            from jsonb_array_elements_text(
              case when jsonb_typeof(lg.group_json->'blocker_codes')='array'
                then lg.group_json->'blocker_codes' else '[]'::jsonb end
            ) code(value)
            where code.value='EXPENSE_INVOICE_EMAIL_REQUIRED'
          )
          or exists (
            select 1
            from jsonb_array_elements(
              case when jsonb_typeof(lg.group_json#>'{blocker_detail,sources}')='array'
                then lg.group_json#>'{blocker_detail,sources}' else '[]'::jsonb end
            ) source(value)
            where source.value->>'code'='EXPENSE_INVOICE_EMAIL_REQUIRED'
          )
        then jsonb_build_array('EXPENSE_EMAIL_MISSING') else '[]'::jsonb end)
        informational_codes,
      (coalesce(lg.group_json->>'active_generation_status','') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')) is_active,
      lg.group_json->>'active_generation_operation_id' active_operation_id_text,
      lg.group_json->>'active_generation_status' active_operation_status,
      lg.group_json->>'canonical_source_revision' source_revision,
      null::text document_revision,
      lg.group_json->'command_payload' command_payload,
      case when pg_input_is_valid(lg.group_json->>'week_ending_date','date')
        then (lg.group_json->>'week_ending_date')::date >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date
        else false end is_early
    from legacy_groups lg
    left join generation_candidate_ids gci on gci.group_key=lg.group_json->>'group_key'
    left join generation_vat gv on gv.group_key=lg.group_json->>'group_key'
    where coalesce(lg.group_json->>'group_key','')<>''
      and coalesce(lg.group_json->>'blocker_code','') not in ('SOURCE_ALREADY_INVOICED','EARLY_GENERATION_NOT_ALLOWED')
  ),
  stale_invoice_timesheets as materialized (
    select l.invoice_id,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_name)) filter(where nullif(s.candidate_name,'') is not null),'[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(s.week_ending_date)) filter(where s.week_ending_date is not null),'[]'::jsonb) week_ending_dates,
      min(s.week_ending_date) min_week_ending,
      max(s.week_ending_date) max_week_ending
    from public.invoice_lines l
    left join public.v_timesheets_summary_base s on s.timesheet_id=l.timesheet_id
    group by l.invoice_id
  ),
  stale_rows as materialized (
    select
      'invoice:'||i.id::text selection_key,
      case when coalesce(i.document_state,'')='FAILED' then 'RETRY_GENERATION' else 'REGENERATE_DRAFT' end row_kind,
      i.id::text scope_key,
      i.id invoice_id,
      i.client_id,
      c.name client_name,
      coalesce(sit.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(sit.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(sit.candidate_names,'[]'::jsonb))=1
        then sit.candidate_names->>0
        when jsonb_array_length(coalesce(sit.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(sit.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      coalesce(sit.week_ending_dates,'[]'::jsonb) week_ending_dates,
      coalesce(sit.min_week_ending, case when pg_input_is_valid(i.header_snapshot_json#>>'{meta,invoice_week_start}','date') then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date + 6 end) week_ending_date,
      coalesce(nullif(i.header_snapshot_json#>>'{meta,currency}',''), nullif(i.header_snapshot_json->>'currency',''), 'GBP') currency,
      round(coalesce(i.subtotal_ex_vat,0),2) total_ex_vat,
      round(coalesce(i.vat_amount,0),2) vat_amount,
      round(coalesce(i.total_inc_vat,coalesce(i.subtotal_ex_vat,0)+coalesce(i.vat_amount,0)),2) total_inc_vat,
      case
        when coalesce(i.document_state,'')='FAILED' then 'FAILED'
        when exists (
          select 1 from public.invoice_document_versions prior_state
          where prior_state.entity_type='INVOICE'
            and prior_state.entity_id=i.id
            and prior_state.purpose='DRAFT_PREVIEW'
        ) then 'STALE'
        else 'NOT_GENERATED'
      end generation_state,
      null::text primary_blocker_code,
      '[]'::jsonb action_blocker_codes,
      case when coalesce(active.status,'') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        then jsonb_build_array('GENERATING')
        when exists (
          select 1 from public.invoice_document_versions prior_state
          where prior_state.entity_type='INVOICE'
            and prior_state.entity_id=i.id
            and prior_state.purpose='DRAFT_PREVIEW'
        ) then jsonb_build_array('STALE')
        else jsonb_build_array('NOT_GENERATED') end informational_codes,
      (coalesce(active.status,'') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')) is_active,
      active.operation_id::text active_operation_id_text,
      active.status active_operation_status,
      i.document_revision::text source_revision,
      i.document_revision::text document_revision,
      jsonb_build_object(
        'command_type','VIEW_INVOICE_DOCUMENT',
        'invoice_id',i.id,
        'purpose','DRAFT_PREVIEW',
        'expected_revision',i.document_revision,
        'source_revision',i.document_revision::text
      ) command_payload,
      coalesce(
        sit.max_week_ending,
        case when pg_input_is_valid(i.header_snapshot_json#>>'{meta,invoice_week_start}','date')
          then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date + 6 end
      ) >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date is_early
    from public.invoices i
    join public.clients c on c.id=i.client_id
    left join stale_invoice_timesheets sit on sit.invoice_id=i.id
    left join lateral (
      select o.id operation_id,o.status
      from public.invoice_operations o
      where o.operation_type='BUILD_DOCUMENT'
        and o.entity_type='INVOICE'
        and o.entity_id=i.id
        and o.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      order by o.created_at_utc desc,o.id desc
      limit 1
    ) active on true
    where i.type::text='INVOICE'
      and i.status::text in ('DRAFT','ON_HOLD')
      and coalesce(i.document_revision,0)>0
      and not exists (
        select 1
        from public.invoice_document_versions v
        where v.entity_type='INVOICE'
          and v.entity_id=i.id
          and v.purpose='DRAFT_PREVIEW'
          and v.source_revision=i.document_revision::text
          and v.template_version='invoice-professional-v2'
          and v.status='READY'
          and v.r2_key is not null
          and v.sha256 ~ '^[0-9a-f]{64}$'
          and coalesce(v.size_bytes,0)>0
          and coalesce(v.page_count,0)>0
      )

  ),
  all_rows_raw as materialized (
    select * from create_rows
    union all
    select * from stale_rows
  ),
  all_rows as materialized (
    select r.*,
      (jsonb_array_length(coalesce(r.action_blocker_codes,'[]'::jsonb))=0 and not r.is_active) selectable,
      case when jsonb_array_length(coalesce(r.action_blocker_codes,'[]'::jsonb))>0 then 'BLOCKED'
           when r.is_active then 'IN_PROGRESS'
           when r.generation_state='STALE' then 'STALE'
           when r.generation_state='FAILED' then 'FAILED'
           else 'READY' end row_status
    from all_rows_raw r
  ),
  filtered_rows as materialized (
    select r.*
    from all_rows r
    cross join params p
    where (p.allow_early or not coalesce(r.is_early,false))
      and (p.display_mode='ALL'
        or (p.display_mode='READY' and r.selectable)
        or (p.display_mode='BLOCKED' and row_status='BLOCKED'))
      and (p.search_text is null
        or lower(coalesce(r.client_name,'')||' '||coalesce(r.candidate_display,'')||' '||coalesce(r.scope_key,'')) like '%'||p.search_text||'%')
      and (p.week_ending_from is null or r.week_ending_date >= p.week_ending_from)
      and (p.week_ending_to is null or r.week_ending_date <= p.week_ending_to)
      and (jsonb_array_length(p.client_ids)=0 or r.client_id::text in (select value from jsonb_array_elements_text(p.client_ids)))
      and (jsonb_array_length(p.candidate_ids)=0 or exists (
        select 1 from jsonb_array_elements_text(r.candidate_ids) row_candidate(id)
        where row_candidate.id in (select value from jsonb_array_elements_text(p.candidate_ids))
      ))
      and (jsonb_array_length(p.week_endings)=0 or r.week_ending_date::text in (select value from jsonb_array_elements_text(p.week_endings)))
      and (jsonb_array_length(p.status_codes)=0 or r.row_status in (select upper(value) from jsonb_array_elements_text(p.status_codes)))
      and (jsonb_array_length(p.blocker_codes)=0 or exists (
        select 1 from jsonb_array_elements_text(coalesce(r.action_blocker_codes,'[]'::jsonb) || coalesce(r.informational_codes,'[]'::jsonb)) badge(code)
        where badge.code in (select upper(value) from jsonb_array_elements_text(p.blocker_codes))
      ))
  ),
  sortable_rows as materialized (
    select fr.*,
      case when p.sort_key='WEEK_ENDING_DATE' then coalesce(fr.week_ending_date, case when p.sort_direction='DESC' then date '0001-01-01' else date '9999-12-31' end) end sort_date_key,
      case when p.sort_key='CLIENT_NAME' then coalesce(lower(fr.client_name), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='CANDIDATE_NAME' then coalesce(lower(fr.candidate_display), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='STATUS' then coalesce(lower(fr.row_status), case when p.sort_direction='DESC' then '' else repeat('~',100) end) end sort_text_key,
      case when p.sort_key='TOTAL_EX_VAT' then coalesce(fr.total_ex_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end)
           when p.sort_key='TOTAL_INC_VAT' then coalesce(fr.total_inc_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end) end sort_numeric_key
    from filtered_rows fr cross join params p
  ),
  cursor_filtered_rows as materialized (
    select sr.*
    from sortable_rows sr
    cross join params p
    where p.after_key is null
       or (
         p.sort_key='WEEK_ENDING_DATE'
         and p.after_sort_date is not null
         and ((p.sort_direction='ASC' and (sr.sort_date_key > p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_date_key < p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS')
         and p.after_sort_text is not null
         and ((p.sort_direction='ASC' and (sr.sort_text_key > p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_text_key < p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT')
         and p.after_sort_numeric is not null
         and ((p.sort_direction='ASC' and (sr.sort_numeric_key > p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_numeric_key < p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key))))
       )
       or (
         ((p.sort_key='WEEK_ENDING_DATE' and p.after_sort_date is null)
           or (p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.after_sort_text is null)
           or (p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.after_sort_numeric is null))
         and sr.selection_key > p.after_key
       )
  ),
  selected_rows as materialized (
    select fr.*,
      coalesce((
        select sr.action
        from selection_rules sr
        where (sr.selector_type='ROW' and sr.selection_key=fr.selection_key)
           or (sr.selector_type='WEEK' and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='CLIENT' and sr.client_id=fr.client_id)
           or (sr.selector_type='CANDIDATE' and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='WEEK_CLIENT' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='WEEK_CLIENT_CANDIDATE' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
        order by sr.rule_sequence desc
        limit 1
      ),'INCLUDE') last_selection_action
    from cursor_filtered_rows fr
  ),
  expanded_rows as materialized (
    select * from selected_rows
    where selectable and last_selection_action <> 'EXCLUDE'
  ),
  candidate_page_source as materialized (
    select * from expanded_rows where v_mode='EXPAND_SELECTION'
    union all
    select * from selected_rows where v_mode<>'EXPAND_SELECTION'
  ),
  ordered_page_rows as materialized (
    select src.*,
      row_number() over (order by
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='ASC' then src.sort_date_key end asc nulls last,
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='DESC' then src.sort_date_key end desc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.sort_direction='ASC' then src.sort_text_key end asc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.sort_direction='DESC' then src.sort_text_key end desc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='ASC' then src.sort_numeric_key end asc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='DESC' then src.sort_numeric_key end desc nulls last,
        src.selection_key asc
      ) page_ordinal
    from candidate_page_source src cross join params p
  ),
  page_rows as materialized (
    select * from ordered_page_rows
    where page_ordinal <= (select page_size + 1 from params)
  ),
  visible_rows as materialized (
    select * from page_rows
    where page_ordinal <= (select page_size from params)
  ),
  totals as materialized (
    select
      count(*)::integer all_count,
      count(*) filter(where selectable)::integer ready_count,
      count(*) filter(where not selectable)::integer blocked_count,
      count(*) filter(where row_status='IN_PROGRESS')::integer in_progress_count,
      count(*) filter(where generation_state='NOT_GENERATED')::integer not_generated_count,
      count(*) filter(where generation_state='STALE')::integer stale_count,
      count(*) filter(where generation_state='FAILED')::integer failed_retryable_count
    from filtered_rows
  ),
  row_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selection_key',selection_key,
      'row_kind',row_kind,
      'scope_key',scope_key,
      'invoice_id',invoice_id,
      'client_id',client_id,
      'client_name',client_name,
      'candidate_ids',candidate_ids,
      'candidate_names',candidate_names,
      'candidate_display',candidate_display,
      'week_ending_dates',week_ending_dates,
      'week_ending_date',week_ending_date,
      'week_ending_display',case when jsonb_array_length(week_ending_dates)>1 then 'Multiple weeks' else to_char(week_ending_date,'DD/MM/YYYY') end,
      'currency',currency,
      'total_ex_vat',total_ex_vat,
      'vat_amount',vat_amount,
      'total_inc_vat',total_inc_vat,
      'generation_state',generation_state,
      'row_status',row_status,
      'is_early',is_early,
      'selectable',selectable,
      'selected',selectable and last_selection_action <> 'EXCLUDE',
      'action_blocker_codes',coalesce(action_blocker_codes,'[]'::jsonb),
      'informational_codes',coalesce(informational_codes,'[]'::jsonb),
      'active_operation_id',active_operation_id_text,
      'active_operation_status',active_operation_status,
      'source_revision',source_revision,
      'document_revision',document_revision,
      'command_payload',command_payload,
      'sort_tuple',jsonb_build_object(
        'sort_date',case when sort_date_key is not null then sort_date_key::text end,
        'sort_text',sort_text_key,
        'sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end,
        'selection_key',selection_key
      )
    ) order by page_ordinal),'[]'::jsonb) rows
    from visible_rows
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_CANDIDATES_V1',
    'action','GENERATE',
    'mode',v_mode,
    'snapshot_at_utc',coalesce(v_query->>'snapshot_at_utc',p_now_utc::text),
    'normalised_filter',v_filters,
    'normalised_sort',v_sort,
    'filter_hash',encode(digest(coalesce(v_filters,'{}'::jsonb)::text || '|' || coalesce(v_sort,'{}'::jsonb)::text || '|GENERATE','sha256'),'hex'),
    'rows',(select rows from row_json),
    'page',jsonb_build_object(
      'page_size',v_page_size,
      'has_more',(select count(*) from page_rows)>v_page_size,
      'next_cursor_values',case when (select count(*) from page_rows)>v_page_size then (
        select jsonb_build_object(
          'after_selection_key',selection_key,
          'after_sort_date',case when sort_date_key is not null then sort_date_key::text end,
          'after_sort_text',sort_text_key,
          'after_sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end
        )
        from visible_rows
        order by page_ordinal desc
        limit 1
      ) else null end
    ),
    'totals',jsonb_build_object(
      'all',(select all_count from totals),
      'ready',(select ready_count from totals),
      'blocked',(select blocked_count from totals),
      'in_progress',(select in_progress_count from totals),
      'not_generated',(select not_generated_count from totals),
      'stale',(select stale_count from totals),
      'failed_retryable',(select failed_retryable_count from totals)
    ),
    'facets',jsonb_build_object(),
    'selection_seed',jsonb_build_object('mode','IMPLICIT_ALL','default_selected',true)
  ) into v_result;

  return v_result;
end;
$function$;

-- private._invoice_batch_generate_candidate_rows_v2(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_generate_candidate_rows_v2(p_query jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_selection jsonb;
  v_selection_keys jsonb;
  v_expected_source_revisions jsonb;
  v_page_size integer;
  v_after_key text;
  v_allow_early boolean;
  v_display_mode text;
  v_input_snapshot jsonb;
  v_snapshot jsonb;
  v_snapshot_after jsonb;
  v_filter_hash text;
  v_query_hash text;
  v_selection_hash text;
  v_result jsonb;
begin
  v_query := private._invoice_batch_query_validate_v2(v_query, 'GENERATE');

  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if coalesce(v_query->>'contract_version','INVOICE_BATCH_QUERY_V2') <> 'INVOICE_BATCH_QUERY_V2' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  v_mode := upper(coalesce(nullif(v_query->>'mode',''),'PAGE'));
  if v_mode not in ('PAGE','FACETS','SUMMARY','EXPAND_SELECTION','EXPLICIT_KEYS') then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_selection_keys := coalesce(v_query->'selection_keys','[]'::jsonb);
  v_expected_source_revisions := coalesce(
    v_query->'expected_source_revisions',
    '{}'::jsonb
  );
  if v_mode='EXPLICIT_KEYS' then
    if jsonb_typeof(v_selection_keys) is distinct from 'array'
       or jsonb_array_length(v_selection_keys) < 1
       or jsonb_array_length(v_selection_keys) > 100
       or jsonb_typeof(v_expected_source_revisions) is distinct from 'object'
       or exists (
         select 1
         from jsonb_array_elements(v_selection_keys) with ordinality key_item(value,ordinality)
         where jsonb_typeof(key_item.value) is distinct from 'string'
            or nullif(btrim(key_item.value #>> '{}'),'') is null
            or length(btrim(key_item.value #>> '{}')) > 512
       )
       or (
         select count(*) from jsonb_array_elements_text(v_selection_keys)
       ) <> (
         select count(distinct value)
         from jsonb_array_elements_text(v_selection_keys) explicit_key(value)
       )
       or exists (
         select 1
         from jsonb_array_elements_text(v_selection_keys) explicit_key(value)
         where nullif(v_expected_source_revisions->>explicit_key.value,'') is null
       ) then
      raise exception using
        errcode='22023',
        message='INVOICE_BATCH_EXPLICIT_KEYS_INVALID';
    end if;
  end if;

  v_filters := case when jsonb_typeof(v_query->'filters') = 'object' then v_query->'filters' else '{}'::jsonb end;
  v_sort := case when jsonb_typeof(v_query->'sort') = 'object' then v_query->'sort' else '{}'::jsonb end;
  v_selection := case when jsonb_typeof(v_query->'selection') = 'object' then v_query->'selection' else jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V2',
    'mode','IMPLICIT_ALL',
    'default_selected',true,
    'rules','[]'::jsonb
  ) end;

  -- Validate the selection ledger up-front even when the current mode only pages rows.
  perform 1 from private._invoice_batch_selection_rules_v2(v_selection) limit 1;

  v_allow_early := coalesce((v_filters->>'allow_early')::boolean,false);
  v_display_mode := upper(coalesce(
    nullif(v_filters->>'display_mode',''),
    'ALL'
  ));
  if v_display_mode not in ('ALL','READY','BLOCKED') then
    raise exception using errcode='22023', message='INVOICE_BATCH_DISPLAY_MODE_INVALID';
  end if;

  if upper(coalesce(v_sort->>'group_preset','WEEK_CLIENT_CANDIDATE')) not in (
    'WEEK_CLIENT_CANDIDATE','CLIENT_WEEK_CANDIDATE','CANDIDATE_WEEK_CLIENT','STATUS_WEEK_CLIENT'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_GROUP_PRESET_INVALID';
  end if;

  if upper(coalesce(v_sort->>'sort_key','WEEK_ENDING_DATE')) not in (
    'WEEK_ENDING_DATE','CLIENT_NAME','CANDIDATE_NAME','TOTAL_EX_VAT','TOTAL_INC_VAT','STATUS'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SORT_KEY_INVALID';
  end if;

  -- PAGE, EXPAND_SELECTION and EXPLICIT_KEYS always obtain their bounded
  -- keyset first. The complete classifier remains the scalar authority for
  -- exact filtering, totals and group state, but it must never cause the
  -- rich presentation hydrator to receive an unrestricted scope.

  v_page_size := case
    when v_mode = 'EXPAND_SELECTION'
      then (v_query->>'page_size')::integer
    when v_mode = 'PAGE'
      then (v_query->>'page_size')::integer
    else 100
  end;
  v_after_key := nullif(v_query#>>'{cursor,after_selection_key}', '');

  v_input_snapshot := v_query->'snapshot';
  if v_input_snapshot is null
     or jsonb_typeof(v_input_snapshot) = 'null' then
    if v_mode <> 'PAGE' or v_after_key is not null then
      raise exception using errcode='22023', message='BATCH_SNAPSHOT_REQUIRED';
    end if;
    v_snapshot := private._invoice_candidate_snapshot_get_v2(
      'GENERATE',
      coalesce(p_now_utc,now())
    );
  else
    v_snapshot := private._invoice_candidate_snapshot_verify_v2(
      'GENERATE',
      v_input_snapshot,
      coalesce(p_now_utc,now())
    );
  end if;

  v_filter_hash := private._invoice_batch_hash_v2(jsonb_build_object(
    'action','GENERATE',
    'filters',v_filters,
    'sort',v_sort
  ));
  v_query_hash := private._invoice_batch_hash_v2(jsonb_build_object(
    'contract_version','INVOICE_BATCH_QUERY_V2',
    'action','GENERATE',
    'filters',v_filters,
    'sort',v_sort,
    'snapshot',jsonb_build_object(
      'contract_version',v_snapshot->>'contract_version',
      'action',v_snapshot->>'action',
      'at_utc',v_snapshot->>'at_utc',
      'revision',v_snapshot->>'revision',
      'expires_at_utc',v_snapshot->>'expires_at_utc',
      'key_id',v_snapshot->>'key_id'
    )
  ));
  v_selection_hash := private._invoice_batch_hash_v2(v_selection);

  with
  params as materialized (
    select
      v_mode mode,
      v_allow_early allow_early,
      v_display_mode display_mode,
      v_page_size page_size,
      v_after_key after_key,
      lower(nullif(btrim(coalesce(v_filters->>'search','')),'')) search_text,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_from',''),'date')
        then (v_filters->>'week_ending_from')::date end week_ending_from,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_to',''),'date')
        then (v_filters->>'week_ending_to')::date end week_ending_to,
      (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date today,
      case when jsonb_typeof(v_filters->'client_ids')='array' then v_filters->'client_ids' else '[]'::jsonb end client_ids,
      case when jsonb_typeof(v_filters->'candidate_ids')='array' then v_filters->'candidate_ids' else '[]'::jsonb end candidate_ids,
      case when jsonb_typeof(v_filters->'week_endings')='array' then v_filters->'week_endings' else '[]'::jsonb end week_endings,
      case when jsonb_typeof(v_filters->'status_codes')='array' then v_filters->'status_codes' else '[]'::jsonb end status_codes,
      case when jsonb_typeof(v_filters->'blocker_codes')='array' then v_filters->'blocker_codes' else '[]'::jsonb end blocker_codes,
      case when jsonb_typeof(v_filters->'invoice_streams')='array' then v_filters->'invoice_streams' else '[]'::jsonb end invoice_streams,
      coalesce(nullif(upper(v_sort->>'group_preset'),''),'WEEK_CLIENT_CANDIDATE') group_preset,
      coalesce(nullif(upper(v_sort->>'sort_key'),''),'WEEK_ENDING_DATE') sort_key,
      case when upper(coalesce(v_sort->>'sort_direction','ASC'))='DESC' then 'DESC' else 'ASC' end sort_direction,
      case when pg_input_is_valid(
        nullif(coalesce(v_query#>>'{cursor,after_sort_date}',''),''),
        'date'
      )
        then (v_query#>>'{cursor,after_sort_date}')::date end after_sort_date,
      nullif(coalesce(v_query#>>'{cursor,after_sort_text}',''),'') after_sort_text,
      case when coalesce(v_query#>>'{cursor,after_sort_numeric}','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (v_query#>>'{cursor,after_sort_numeric}')::numeric end after_sort_numeric,
      lower(nullif(btrim(coalesce(v_query#>>'{facet_request,search}','')),'')) facet_search,
      case when coalesce(v_query#>>'{facet_request,limit_per_kind}','') ~ '^[1-9][0-9]{0,2}$'
        then least((v_query#>>'{facet_request,limit_per_kind}')::integer,100)
        else 100 end facet_limit,
      case when jsonb_typeof(v_query#>'{facet_request,kinds}')='array'
        then v_query#>'{facet_request,kinds}'
        else '["CLIENTS","CANDIDATES","WEEK_ENDINGS","STATUSES","BLOCKERS"]'::jsonb end facet_kinds,
      lower(nullif(v_query#>>'{facet_request,cursors,clients,after_label}','')) facet_client_after_label,
      nullif(v_query#>>'{facet_request,cursors,clients,after_id}','') facet_client_after_id,
      lower(nullif(v_query#>>'{facet_request,cursors,candidates,after_label}','')) facet_candidate_after_label,
      nullif(v_query#>>'{facet_request,cursors,candidates,after_id}','') facet_candidate_after_id,
      case when pg_input_is_valid(
        coalesce(v_query#>>'{facet_request,cursors,week_endings,after_value}',''),
        'date'
      ) then (v_query#>>'{facet_request,cursors,week_endings,after_value}')::date end facet_week_after_value,
      nullif(v_query#>>'{facet_request,cursors,statuses,after_code}','') facet_status_after_code,
      nullif(v_query#>>'{facet_request,cursors,blockers,after_code}','') facet_blocker_after_code
  ),
  selection_rules as materialized (
    select * from private._invoice_batch_selection_rules_v2(v_selection)
  ),
  candidate_keys as materialized (
    select key_row.*
    from (
      select 1
      where v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
    ) gate
    cross join lateral private._invoice_batch_generate_candidate_key_rows_v2(
      v_query,
      coalesce(p_now_utc,now())
    ) key_row
  ),
  create_scope_request as materialized (
    select
      case
        when v_mode in ('FACETS','SUMMARY') then '{}'::text[]
        else coalesce(array_agg(k.scope_key order by k.page_ordinal)
          filter (
            where k.row_kind='CREATE_INVOICE'
              and k.page_ordinal<=v_page_size
          ),'{}'::text[])
      end scope_keys
    from candidate_keys k
  ),
  source_groups as materialized (
    select
      '{}'::jsonb client_json,
      source.client_id::text client_id_text,
      source.client_name,
      source.group_json
    from create_scope_request request
    cross join lateral private._invoice_batch_generate_group_rows_v2(
      true,
      null,
      request.scope_keys,
      coalesce(p_now_utc,now())
    ) source
    where v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
      and cardinality(request.scope_keys)>0
  ),
  generation_timesheets as materialized (
    select
      lg.group_json->>'group_key' group_key,
      t.value timesheet_json,
      case when pg_input_is_valid(t.value->>'timesheet_id','uuid') then (t.value->>'timesheet_id')::uuid end timesheet_id,
      t.value->>'candidate_name' candidate_name,
      case when pg_input_is_valid(t.value->>'week_ending_date','date') then (t.value->>'week_ending_date')::date end week_ending_date,
      case when coalesce(t.value->>'total_charge_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (t.value->>'total_charge_ex_vat')::numeric else 0 end total_ex_vat
    from source_groups lg
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(lg.group_json->'timesheets')='array' then lg.group_json->'timesheets' else '[]'::jsonb end
    ) t(value)
  ),
  generation_candidate_ids as materialized (
    select gt.group_key,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(gt.candidate_name)) filter(where nullif(gt.candidate_name,'') is not null),'[]'::jsonb) candidate_names
    from generation_timesheets gt


    left join public.v_timesheets_summary_base s on s.timesheet_id=gt.timesheet_id
    group by gt.group_key
  ),
  generation_vat_members as materialized (
    select distinct on (lg.group_json->>'group_key', gt.timesheet_id)
      lg.group_json->>'group_key' group_key,
      gt.timesheet_id,
      coalesce(gt.total_ex_vat,0) total_ex_vat,
      coalesce(vat.vat_rate,0) vat_rate
    from source_groups lg
    left join lateral jsonb_array_elements(
      case when jsonb_typeof(lg.group_json->'canonical_source_members')='array'
        then lg.group_json->'canonical_source_members' else '[]'::jsonb end
    ) member(value) on true
    left join generation_timesheets gt
      on gt.group_key=lg.group_json->>'group_key'
     and gt.timesheet_id=case when pg_input_is_valid(coalesce(member.value->>'related_timesheet_id',member.value->>'source_id',''),'uuid')
       then coalesce(member.value->>'related_timesheet_id',member.value->>'source_id')::uuid end
    left join lateral private._invoice_generation_vat_policy_batch(jsonb_build_array(jsonb_build_object(
      'source_member_key',coalesce(nullif(member.value->>'source_member_key',''), lg.group_json->>'group_key'||':'||coalesce(gt.timesheet_id::text,'')),
      'source_type',member.value->>'source_type',
      'source_id',member.value->>'source_id',
      'timesheet_id',coalesce(member.value->>'related_timesheet_id',member.value->>'source_id'),
      'segment_id',nullif(member.value->>'segment_id',''),
      'effective_date',member.value->>'effective_settings_date'
    ))) vat on true
    where gt.timesheet_id is not null
    order by lg.group_json->>'group_key', gt.timesheet_id, coalesce(vat.vat_rate,0) desc
  ),
  generation_vat as materialized (
    select group_key,
      round(coalesce(sum(total_ex_vat * vat_rate / 100.0),0),2) vat_amount
    from generation_vat_members
    group by group_key
  ),
  create_rows as materialized (
    select
      'generate:'||coalesce(lg.group_json->>'group_key','') selection_key,
      'CREATE_INVOICE' row_kind,
      lg.group_json->>'group_key' scope_key,
      null::uuid invoice_id,
      case when pg_input_is_valid(lg.client_id_text,'uuid') then lg.client_id_text::uuid end client_id,
      lg.client_name,
      coalesce(gci.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(gci.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(gci.candidate_names,'[]'::jsonb))=1
        then gci.candidate_names->>0
        when jsonb_array_length(coalesce(gci.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(gci.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      coalesce((
        select jsonb_agg(distinct to_jsonb(gt.week_ending_date) order by to_jsonb(gt.week_ending_date))
        from generation_timesheets gt where gt.group_key=lg.group_json->>'group_key' and gt.week_ending_date is not null
      ),'[]'::jsonb) week_ending_dates,
      case when pg_input_is_valid(lg.group_json->>'week_ending_date','date') then (lg.group_json->>'week_ending_date')::date end week_ending_date,
      coalesce(nullif(lg.group_json#>>'{command_payload,currency}',''),'GBP') currency,
      upper(coalesce(
        nullif(lg.group_json->>'invoice_stream',''),
        nullif(lg.group_json->>'stream',''),
        'NORMAL'
      )) invoice_stream,
      case when coalesce(lg.group_json->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then round((lg.group_json->>'subtotal_ex_vat')::numeric,2) else 0 end total_ex_vat,
      coalesce(gv.vat_amount,0) vat_amount,
      round((case when coalesce(lg.group_json->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (lg.group_json->>'subtotal_ex_vat')::numeric else 0 end) + coalesce(gv.vat_amount,0),2) total_inc_vat,
      'NOT_GENERATED' generation_state,
      coalesce(lg.group_json->>'blocker_code','') primary_blocker_code,
      coalesce((
        select jsonb_agg(to_jsonb(blocker_code) order by blocker_order, blocker_code)
        from (
          select blocker_code, min(blocker_order) blocker_order
          from (
            select 1 blocker_order, nullif(lg.group_json->>'blocker_code','') blocker_code
            union all
            select 2, nullif(lg.group_json#>>'{blocker_detail,code}','')
            union all
            select 10 + arr.ordinality::integer, nullif(arr.value,'')
            from jsonb_array_elements_text(
              case when jsonb_typeof(lg.group_json->'blocker_codes')='array'
                then lg.group_json->'blocker_codes' else '[]'::jsonb end
            ) with ordinality arr(value, ordinality)
            union all
            select 100 + src.ordinality::integer, nullif(src.value->>'code','')
            from jsonb_array_elements(
              case when jsonb_typeof(lg.group_json#>'{blocker_detail,sources}')='array'
                then lg.group_json#>'{blocker_detail,sources}' else '[]'::jsonb end
            ) with ordinality src(value, ordinality)
          ) raw_codes
          where blocker_code is not null
            and blocker_code not in (
              'EARLY_GENERATION_NOT_ALLOWED',
              'SEGMENT_ALREADY_LOCKED',
              'SOURCE_ALREADY_LOCKED',
              'SOURCE_ALREADY_INVOICED'
            )
          group by blocker_code
        ) blocker_rows
      ),'[]'::jsonb) action_blocker_codes,
      case when coalesce(lg.group_json->>'active_generation_status','') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        then jsonb_build_array('GENERATING') else '[]'::jsonb end informational_codes,
      (coalesce(lg.group_json->>'active_generation_status','') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')) is_active,
      lg.group_json->>'active_generation_operation_id' active_operation_id_text,
      lg.group_json->>'active_generation_status' active_operation_status,
      lg.group_json->>'canonical_source_revision' source_revision,
      null::text document_revision,
      lg.group_json->'command_payload' command_payload,
      case when pg_input_is_valid(lg.group_json->>'week_ending_date','date')
        then (lg.group_json->>'week_ending_date')::date >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date
        else false end is_early
    from source_groups lg
    left join generation_candidate_ids gci on gci.group_key=lg.group_json->>'group_key'
    left join generation_vat gv on gv.group_key=lg.group_json->>'group_key'
    where coalesce(lg.group_json->>'group_key','')<>''
      and coalesce(lg.group_json->>'blocker_code','') not in (
        'SOURCE_ALREADY_INVOICED',
        'SEGMENT_ALREADY_LOCKED',
        'SOURCE_ALREADY_LOCKED',
        'EARLY_GENERATION_NOT_ALLOWED'
      )
      and coalesce(lg.group_json#>>'{blocker_detail,code}','') not in (
        'SOURCE_ALREADY_INVOICED',
        'SEGMENT_ALREADY_LOCKED',
        'SOURCE_ALREADY_LOCKED'
      )
      and not exists (
        select 1
        from jsonb_array_elements_text(
          case
            when jsonb_typeof(lg.group_json->'blocker_codes')='array'
              then lg.group_json->'blocker_codes'
            else '[]'::jsonb
          end
        ) blocked_code(value)
        where blocked_code.value in (
          'SOURCE_ALREADY_INVOICED',
          'SEGMENT_ALREADY_LOCKED',
          'SOURCE_ALREADY_LOCKED'
        )
      )
      and not exists (
        select 1
        from jsonb_array_elements(
          case
            when jsonb_typeof(lg.group_json#>'{blocker_detail,sources}')='array'
              then lg.group_json#>'{blocker_detail,sources}'
            else '[]'::jsonb
          end
        ) blocked_source(value)
        where blocked_source.value->>'code' in (
          'SOURCE_ALREADY_INVOICED',
          'SEGMENT_ALREADY_LOCKED',
          'SOURCE_ALREADY_LOCKED'
        )
      )
  ),
  stale_invoice_timesheets as materialized (
    select l.invoice_id,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_name)) filter(where nullif(s.candidate_name,'') is not null),'[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(s.week_ending_date)) filter(where s.week_ending_date is not null),'[]'::jsonb) week_ending_dates,
      min(s.week_ending_date) min_week_ending,
      max(s.week_ending_date) max_week_ending
    from public.invoice_lines l
    left join public.v_timesheets_summary_base s on s.timesheet_id=l.timesheet_id
    group by l.invoice_id
  ),
  stale_rows as materialized (
    select
      'invoice:'||i.id::text selection_key,
      case when coalesce(i.document_state,'')='FAILED' then 'RETRY_GENERATION' else 'REGENERATE_DRAFT' end row_kind,
      i.id::text scope_key,
      i.id invoice_id,
      i.client_id,
      c.name client_name,
      coalesce(sit.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(sit.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(sit.candidate_names,'[]'::jsonb))=1
        then sit.candidate_names->>0
        when jsonb_array_length(coalesce(sit.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(sit.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      coalesce(sit.week_ending_dates,'[]'::jsonb) week_ending_dates,
      coalesce(sit.min_week_ending, case when pg_input_is_valid(i.header_snapshot_json#>>'{meta,invoice_week_start}','date') then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date + 6 end) week_ending_date,
      coalesce(nullif(i.header_snapshot_json#>>'{meta,currency}',''), nullif(i.header_snapshot_json->>'currency',''), 'GBP') currency,
      upper(coalesce(
        nullif(i.header_snapshot_json#>>'{meta,invoice_stream}',''),
        nullif(i.header_snapshot_json->>'invoice_stream',''),
        case when lower(coalesce(
          i.header_snapshot_json#>>'{meta,self_bill}',
          i.header_snapshot_json->>'self_bill',
          'false'
        )) in ('true','t','1','yes') then 'SELF_BILL' end,
        'NORMAL'
      )) invoice_stream,
      round(coalesce(i.subtotal_ex_vat,0),2) total_ex_vat,
      round(coalesce(i.vat_amount,0),2) vat_amount,
      round(coalesce(i.total_inc_vat,coalesce(i.subtotal_ex_vat,0)+coalesce(i.vat_amount,0)),2) total_inc_vat,
      case
        when coalesce(i.document_state,'')='FAILED' then 'FAILED'
        when exists (
          select 1 from public.invoice_document_versions prior_state
          where prior_state.entity_type='INVOICE'
            and prior_state.entity_id=i.id
            and prior_state.purpose='DRAFT_PREVIEW'
        ) then 'STALE'
        else 'NOT_GENERATED'
      end generation_state,
      null::text primary_blocker_code,
      '[]'::jsonb action_blocker_codes,


      case when coalesce(active.status,'') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        then jsonb_build_array('GENERATING')
        when exists (
          select 1 from public.invoice_document_versions prior_state
          where prior_state.entity_type='INVOICE'
            and prior_state.entity_id=i.id
            and prior_state.purpose='DRAFT_PREVIEW'
        ) then jsonb_build_array('STALE')
        else jsonb_build_array('NOT_GENERATED') end informational_codes,
      (coalesce(active.status,'') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')) is_active,
      active.operation_id::text active_operation_id_text,
      active.status active_operation_status,
      i.document_revision::text source_revision,
      i.document_revision::text document_revision,
      jsonb_build_object(
        'command_type','VIEW_INVOICE_DOCUMENT',
        'invoice_id',i.id,
        'purpose','DRAFT_PREVIEW',
        'expected_revision',i.document_revision,
        'source_revision',i.document_revision::text
      ) command_payload,
      coalesce(
        sit.max_week_ending,
        case when pg_input_is_valid(i.header_snapshot_json#>>'{meta,invoice_week_start}','date')
          then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date + 6 end
      ) >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date is_early
    from public.invoices i
    join public.clients c on c.id=i.client_id
    left join stale_invoice_timesheets sit on sit.invoice_id=i.id
    left join lateral (
      select o.id operation_id,o.status
      from public.invoice_operations o
      where o.operation_type='BUILD_DOCUMENT'
        and o.entity_type='INVOICE'
        and o.entity_id=i.id
        and o.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      order by o.created_at_utc desc,o.id desc
      limit 1
    ) active on true
    where i.type::text='INVOICE'
      and i.status::text in ('DRAFT','ON_HOLD')
      and coalesce(i.document_revision,0)>0
      and (
        v_mode in ('FACETS','SUMMARY')
        or exists (
          select 1
          from candidate_keys selected_key
          where selected_key.row_kind<>'CREATE_INVOICE'
            and selected_key.invoice_id=i.id
            and selected_key.page_ordinal<=v_page_size
        )
      )
      and not exists (
        select 1
        from public.invoice_document_versions v
        where v.entity_type='INVOICE'
          and v.entity_id=i.id
          and v.purpose='DRAFT_PREVIEW'
          and v.source_revision=i.document_revision::text
          and v.template_version='invoice-professional-v2'
          and v.status='READY'
          and v.r2_key is not null
          and v.sha256 ~ '^[0-9a-f]{64}$'
          and coalesce(v.size_bytes,0)>0
          and coalesce(v.page_count,0)>0
      )

  ),
  all_rows_raw as materialized (
    select * from create_rows
  ),
  all_rows as materialized (
    select r.*,
      (jsonb_array_length(coalesce(r.action_blocker_codes,'[]'::jsonb))=0 and not r.is_active) selectable,
      case when jsonb_array_length(coalesce(r.action_blocker_codes,'[]'::jsonb))>0 then 'BLOCKED'
           when r.is_active then 'IN_PROGRESS'
           when r.generation_state='STALE' then 'STALE'
           when r.generation_state='FAILED' then 'FAILED'
           else 'READY' end row_status
    from all_rows_raw r
  ),
  classification_source as materialized (
    -- PAGE-family requests reuse the exact candidate payload that the keyset
    -- helper already classified. FACETS/SUMMARY do not invoke the keyset
    -- helper, so they retain one direct full-scope classification pass.
    select key_row.selection_key,key_row.candidate_json
    from candidate_keys key_row
    where key_row.page_ordinal<=v_page_size
    union all
    select candidate.selection_key,candidate.candidate_json
    from (
      select 1
      where v_mode in ('FACETS','SUMMARY')
    ) gate
    cross join lateral private._invoice_batch_generate_classification_v2(
      true,
      null,
      coalesce(p_now_utc,statement_timestamp())
    ) candidate
  ),
  authoritative_rows as materialized (
    select
      candidate.candidate_json->>'selection_key' selection_key,
      candidate.candidate_json->>'row_kind' row_kind,
      candidate.candidate_json->>'scope_key' scope_key,
      case
        when pg_input_is_valid(
          coalesce(candidate.candidate_json->>'invoice_id',''),
          'uuid'
        )
          then (candidate.candidate_json->>'invoice_id')::uuid
      end invoice_id,
      case
        when pg_input_is_valid(
          coalesce(candidate.candidate_json->>'client_id',''),
          'uuid'
        )
          then (candidate.candidate_json->>'client_id')::uuid
      end client_id,
      candidate.candidate_json->>'client_name' client_name,
      coalesce(
        candidate.candidate_json->'candidate_ids',
        '[]'::jsonb
      ) candidate_ids,
      coalesce(
        candidate.candidate_json->'candidate_names',
        '[]'::jsonb
      ) candidate_names,
      candidate.candidate_json->>'candidate_display' candidate_display,
      coalesce(
        candidate.candidate_json->'week_ending_dates',
        '[]'::jsonb
      ) week_ending_dates,
      case
        when pg_input_is_valid(
          coalesce(candidate.candidate_json->>'week_ending_date',''),
          'date'
        )
          then (candidate.candidate_json->>'week_ending_date')::date
      end week_ending_date,
      coalesce(
        nullif(candidate.candidate_json->>'currency',''),
        'GBP'
      ) currency,
      upper(coalesce(
        nullif(candidate.candidate_json->>'invoice_stream',''),
        'NORMAL'
      )) invoice_stream,
      coalesce(
        (candidate.candidate_json->>'total_ex_vat')::numeric,
        0
      ) total_ex_vat,
      coalesce(
        (candidate.candidate_json->>'vat_amount')::numeric,
        0
      ) vat_amount,
      coalesce(
        (candidate.candidate_json->>'total_inc_vat')::numeric,
        0
      ) total_inc_vat,
      candidate.candidate_json->>'generation_state' generation_state,
      nullif(
        candidate.candidate_json->>'primary_blocker_code',
        ''
      ) primary_blocker_code,
      coalesce(
        candidate.candidate_json->'action_blocker_codes',
        '[]'::jsonb
      ) action_blocker_codes,
      coalesce(
        candidate.candidate_json->'informational_codes',
        '[]'::jsonb
      ) informational_codes,
      coalesce(
        (candidate.candidate_json->>'is_active')::boolean,
        false
      ) is_active,
      candidate.candidate_json->>'active_operation_id'
        active_operation_id_text,
      candidate.candidate_json->>'active_operation_status'
        active_operation_status,
      candidate.candidate_json->>'source_revision' source_revision,
      candidate.candidate_json->>'document_revision' document_revision,
      candidate.candidate_json->'command_payload' command_payload,
      coalesce(
        (candidate.candidate_json->>'is_early')::boolean,
        false
      ) is_early,
      coalesce(
        (candidate.candidate_json->>'selectable')::boolean,
        false
      ) selectable,
      candidate.candidate_json->>'row_status' row_status
    from classification_source candidate
    where candidate.candidate_json->>'row_kind'='CREATE_INVOICE'
      and (
        v_mode in ('FACETS','SUMMARY')
        or candidate.candidate_json->>'selection_key' in (
          select selected_key.selection_key
          from candidate_keys selected_key
          where selected_key.page_ordinal<=v_page_size
        )
      )
      and coalesce(
        candidate.candidate_json->>'primary_blocker_code',
        ''
      ) not in (
      'SOURCE_ALREADY_INVOICED',
      'SEGMENT_ALREADY_LOCKED',
      'SOURCE_ALREADY_LOCKED'
    )
      and not exists (
        select 1
        from jsonb_array_elements_text(
          case
            when jsonb_typeof(
              candidate.candidate_json->'action_blocker_codes'
            )='array'
              then candidate.candidate_json->'action_blocker_codes'
            else '[]'::jsonb
          end
        ) blocked_code(value)
        where blocked_code.value in (
          'SOURCE_ALREADY_INVOICED',
          'SEGMENT_ALREADY_LOCKED',
          'SOURCE_ALREADY_LOCKED'
        )
      )
  ),
  filter_match_rows as materialized (
    select
      r.*,
      (
        jsonb_array_length(p.client_ids)=0
        or r.client_id::text in (
          select value from jsonb_array_elements_text(p.client_ids)
        )
      ) client_filter_match,
      (
        jsonb_array_length(p.candidate_ids)=0
        or exists (
          select 1
          from jsonb_array_elements_text(r.candidate_ids) row_candidate(id)
          where row_candidate.id in (
            select value from jsonb_array_elements_text(p.candidate_ids)
          )
        )
      ) candidate_filter_match,
      (
        jsonb_array_length(p.week_endings)=0
        or r.week_ending_date::text in (
          select value from jsonb_array_elements_text(p.week_endings)
        )
      ) week_filter_match,
      (
        jsonb_array_length(p.status_codes)=0
        or r.row_status in (
          select upper(value)
          from jsonb_array_elements_text(p.status_codes)
        )
      ) status_filter_match,
      (
        jsonb_array_length(p.blocker_codes)=0
        or exists (
          select 1
          from jsonb_array_elements_text(
            coalesce(r.action_blocker_codes,'[]'::jsonb)
            || coalesce(r.informational_codes,'[]'::jsonb)
          ) badge(code)
          where badge.code in (
            select upper(value)
            from jsonb_array_elements_text(p.blocker_codes)
          )
        )
      ) blocker_filter_match
    from authoritative_rows r
    cross join params p
    where (p.allow_early or not coalesce(r.is_early,false))
      and (
        jsonb_array_length(p.invoice_streams)=0
        or r.invoice_stream in (
          select upper(value)
          from jsonb_array_elements_text(p.invoice_streams)
        )
      )
      and (
        v_mode <> 'EXPLICIT_KEYS'
        or r.selection_key in (
          select value
          from jsonb_array_elements_text(v_selection_keys)
        )
      )
      and (p.search_text is null
        or lower(coalesce(r.client_name,'')||' '||coalesce(r.candidate_display,'')||' '||coalesce(r.scope_key,'')) like '%'||p.search_text||'%')
      and (p.week_ending_from is null or r.week_ending_date >= p.week_ending_from)
      and (p.week_ending_to is null or r.week_ending_date <= p.week_ending_to)
  ),
  scope_rows as materialized (
    select r.*
    from filter_match_rows r
    where r.client_filter_match
      and r.candidate_filter_match
      and r.week_filter_match
      and r.status_filter_match
      and r.blocker_filter_match
  ),
  facet_client_rows as materialized (
    select r.* from filter_match_rows r
    where r.candidate_filter_match and r.week_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_candidate_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.week_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_week_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_status_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.week_filter_match and r.blocker_filter_match
  ),
  facet_blocker_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.week_filter_match and r.status_filter_match
  ),
  facet_client_values_base as materialized (
    select
      client_id,
      min(coalesce(nullif(client_name,''),client_id::text)) label,
      count(*)::integer row_count
    from facet_client_rows
    where client_id is not null
    group by client_id
  ),
  facet_client_values as materialized (
    select b.*,
      row_number() over(order by lower(b.label),b.client_id) facet_ordinal,
      count(*) over() facet_total
    from facet_client_values_base b
    cross join params p
    where (p.facet_search is null
        or lower(b.label) like '%'||p.facet_search||'%'
        or b.client_id::text like p.facet_search||'%')
      and (p.facet_client_after_label is null
        or (lower(b.label),b.client_id::text)>
           (p.facet_client_after_label,coalesce(p.facet_client_after_id,'')))
  ),
  facet_candidate_values_base as materialized (
    select
      candidate.value candidate_id,
      min(coalesce(
        nullif(r.candidate_names->>(candidate.ordinality::integer-1),''),
        candidate.value
      )) label,
      count(distinct r.selection_key)::integer row_count
    from facet_candidate_rows r
    cross join lateral jsonb_array_elements_text(
      coalesce(r.candidate_ids,'[]'::jsonb)
    ) with ordinality candidate(value,ordinality)
    group by candidate.value
  ),
  facet_candidate_values as materialized (
    select b.*,
      row_number() over(order by lower(b.label),b.candidate_id) facet_ordinal,
      count(*) over() facet_total
    from facet_candidate_values_base b
    cross join params p
    where (p.facet_search is null
        or lower(b.label) like '%'||p.facet_search||'%'
        or b.candidate_id like p.facet_search||'%')
      and (p.facet_candidate_after_label is null
        or (lower(b.label),b.candidate_id)>
           (p.facet_candidate_after_label,coalesce(p.facet_candidate_after_id,'')))
  ),
  facet_week_values as materialized (
    select b.*,
      row_number() over(order by b.week_ending_date desc) facet_ordinal,
      count(*) over() facet_total
    from (
      select week_ending_date,count(*)::integer row_count
      from facet_week_rows
      where week_ending_date is not null
      group by week_ending_date
    ) b
    cross join params p
    where (p.facet_search is null
        or to_char(b.week_ending_date,'DD/MM/YYYY') like '%'||p.facet_search||'%'
        or b.week_ending_date::text like '%'||p.facet_search||'%')
      and (p.facet_week_after_value is null
        or b.week_ending_date<p.facet_week_after_value)
  ),
  facet_status_values as materialized (
    select b.*,
      row_number() over(order by b.row_status) facet_ordinal,
      count(*) over() facet_total
    from (
      select row_status,count(*)::integer row_count
      from facet_status_rows
      group by row_status
    ) b
    cross join params p
    where (p.facet_search is null
        or lower(b.row_status) like '%'||p.facet_search||'%'
        or lower(replace(b.row_status,'_',' ')) like '%'||p.facet_search||'%')
      and (p.facet_status_after_code is null
        or b.row_status>p.facet_status_after_code)
  ),
  facet_blocker_values as materialized (
    select b.*,
      row_number() over(order by b.code) facet_ordinal,
      count(*) over() facet_total
    from (
      select badge.code,count(distinct r.selection_key)::integer row_count
      from facet_blocker_rows r
      cross join lateral jsonb_array_elements_text(
        coalesce(r.action_blocker_codes,'[]'::jsonb)
        || coalesce(r.informational_codes,'[]'::jsonb)
      ) badge(code)
      group by badge.code
    ) b
    cross join params p
    where (p.facet_search is null
        or lower(b.code) like '%'||p.facet_search||'%'
        or lower(replace(b.code,'_',' ')) like '%'||p.facet_search||'%')
      and (p.facet_blocker_after_code is null
        or b.code>p.facet_blocker_after_code)
  ),
  grouped_scope_rows as materialized (
    select
      r.*,
      private._invoice_batch_hash_v2(jsonb_build_object(
        'action','GENERATE',
        'group_preset',p.group_preset,
        'status_code',case when p.group_preset='STATUS_WEEK_CLIENT' then r.row_status end,
        'week_ending_date',r.week_ending_date,
        'client_id',r.client_id,
        'candidate_ids',case
          when p.group_preset='STATUS_WEEK_CLIENT' then '[]'::jsonb
          else coalesce(r.candidate_ids,'[]'::jsonb)
        end
      )) group_key
    from scope_rows r
    cross join params p
  ),
  selection_scope_rows as materialized (
    select
      fr.*,
      coalesce((
        select sr.action
        from selection_rules sr
        where (sr.selector_type='ROW' and sr.selection_key=fr.selection_key)
           or (sr.selector_type='WEEK' and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='CLIENT' and sr.client_id=fr.client_id)
           or (sr.selector_type='CANDIDATE' and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='STATUS' and sr.status_code=fr.row_status)
           or (sr.selector_type='WEEK_CLIENT' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='WEEK_CLIENT_CANDIDATE' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='STATUS_WEEK' and sr.status_code=fr.row_status and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='STATUS_WEEK_CLIENT' and sr.status_code=fr.row_status and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='DIMENSION_GROUP'
             and (sr.week_ending_date is null or sr.week_ending_date=fr.week_ending_date)
             and (sr.client_id is null or sr.client_id=fr.client_id)
             and (sr.status_code is null or sr.status_code=fr.row_status)
             and (sr.candidate_id is null or exists (
               select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
               where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
             )))
        order by sr.rule_sequence desc
        limit 1
      ),'INCLUDE') last_selection_action
    from grouped_scope_rows fr
  ),
  filtered_rows as materialized (
    select r.*
    from selection_scope_rows r
    cross join params p
    where (
      v_mode='EXPAND_SELECTION'
      or p.display_mode='ALL'
      or (p.display_mode='READY' and r.selectable)
      or (p.display_mode='BLOCKED' and r.row_status='BLOCKED')
    )
      and (
        v_mode<>'EXPAND_SELECTION'
        or (
          r.selectable
          and r.last_selection_action<>'EXCLUDE'
        )
      )
  ),
  sortable_rows as materialized (
    select fr.*,
      case when p.sort_key='WEEK_ENDING_DATE' then coalesce(fr.week_ending_date, case when p.sort_direction='DESC' then date '0001-01-01' else date '9999-12-31' end) end sort_date_key,
      case when p.sort_key='CLIENT_NAME' then coalesce(lower(fr.client_name), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='CANDIDATE_NAME' then coalesce(lower(fr.candidate_display), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='STATUS' then
             lpad((case fr.row_status
               when 'READY' then 10
               when 'STALE' then 20
               when 'FAILED' then 30
               when 'IN_PROGRESS' then 40
               else 50
             end)::text,3,'0')||'|'||lower(coalesce(fr.row_status,'BLOCKED'))
           end sort_text_key,
      case when p.sort_key='TOTAL_EX_VAT' then coalesce(fr.total_ex_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end)
           when p.sort_key='TOTAL_INC_VAT' then coalesce(fr.total_inc_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end) end sort_numeric_key
    from filtered_rows fr cross join params p
  ),
  cursor_filtered_rows as materialized (
    select sr.*
    from sortable_rows sr
    cross join params p
    where p.after_key is null
       or (
         p.sort_key='WEEK_ENDING_DATE'
         and p.after_sort_date is not null
         and ((p.sort_direction='ASC' and (sr.sort_date_key > p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_date_key < p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS')
         and p.after_sort_text is not null
         and ((p.sort_direction='ASC' and (sr.sort_text_key > p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_text_key < p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT')
         and p.after_sort_numeric is not null
         and ((p.sort_direction='ASC' and (sr.sort_numeric_key > p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_numeric_key < p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key))))
       )
       or (
         ((p.sort_key='WEEK_ENDING_DATE' and p.after_sort_date is null)
           or (p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.after_sort_text is null)
           or (p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.after_sort_numeric is null))
         and sr.selection_key > p.after_key
       )
  ),
  selected_rows as materialized (
    select * from cursor_filtered_rows
  ),
  candidate_page_source as materialized (
    select * from selected_rows where v_mode='EXPAND_SELECTION'
    union all
    select * from selected_rows where v_mode in('PAGE','EXPLICIT_KEYS')
  ),
  ordered_page_rows as materialized (
    select src.*,
      row_number() over (order by
        case when v_mode='EXPAND_SELECTION' then src.selection_key end asc,
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='ASC' then src.sort_date_key end asc nulls last,
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='DESC' then src.sort_date_key end desc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.sort_direction='ASC' then src.sort_text_key end asc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.sort_direction='DESC' then src.sort_text_key end desc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='ASC' then src.sort_numeric_key end asc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='DESC' then src.sort_numeric_key end desc nulls last,
        src.selection_key asc
      ) page_ordinal
    from candidate_page_source src cross join params p
  ),
  page_rows as materialized (
    select * from ordered_page_rows
    where page_ordinal <= (select page_size + 1 from params)
  ),
  visible_rows as materialized (
    select * from page_rows
    where page_ordinal <= (select page_size from params)
  ),
  totals as materialized (
    select
      count(*)::integer all_count,
      count(*) filter(where selectable)::integer ready_count,
      count(*) filter(where selectable and last_selection_action <> 'EXCLUDE')::integer selected_count,
      count(*) filter(where not selectable)::integer blocked_count,
      count(*) filter(where row_status='IN_PROGRESS')::integer in_progress_count,
      count(*) filter(where generation_state='NOT_GENERATED')::integer not_generated_count,
      count(*) filter(where generation_state='STALE')::integer stale_count,
      count(*) filter(where generation_state='FAILED')::integer failed_retryable_count
    from selection_scope_rows
  ),
  display_totals as materialized (
    select count(*)::integer display_count
    from filtered_rows
  ),
  visible_group_keys as materialized (
    select distinct group_key
    from visible_rows
    where v_mode = 'PAGE'
  ),
  page_group_rollup as materialized (
    select
      r.group_key,
      min(r.week_ending_date) week_ending_date,
      (array_agg(r.client_id order by r.selection_key))[1] client_id,
      min(r.row_status) row_status,
      (array_agg(r.candidate_ids order by r.selection_key))[1] candidate_ids,
      count(*)::integer row_total,
      count(*) filter (where r.selectable)::integer eligible_total,
      count(*) filter (
        where r.selectable and r.last_selection_action <> 'EXCLUDE'
      )::integer selected_total,
      count(*) filter (where v.group_key is not null)::integer visible_total
    from selection_scope_rows r
    join visible_group_keys g on g.group_key = r.group_key
    left join visible_rows v
      on v.group_key = r.group_key
     and v.selection_key = r.selection_key
    group by r.group_key
  ),
  page_group_selection_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'group_key',g.group_key,
      'selector',case
        when p.group_preset='STATUS_WEEK_CLIENT' then jsonb_build_object(
          'type','STATUS_WEEK_CLIENT',
          'status_code',g.row_status,
          'week_ending_date',g.week_ending_date,
          'client_id',g.client_id
        )
        when jsonb_array_length(coalesce(g.candidate_ids,'[]'::jsonb))=1
          then jsonb_build_object(
            'type','WEEK_CLIENT_CANDIDATE',
            'week_ending_date',g.week_ending_date,
            'client_id',g.client_id,
            'candidate_id',g.candidate_ids->>0
          )
        else jsonb_build_object(
          'type','WEEK_CLIENT',
          'week_ending_date',g.week_ending_date,
          'client_id',g.client_id
        )
      end,
      'eligible_total',g.eligible_total,
      'selected_total',g.selected_total,
      'state',case
        when g.eligible_total = 0 then 'DISABLED'
        when g.selected_total = 0 then 'UNCHECKED'
        when g.selected_total = g.eligible_total then 'CHECKED'
        else 'INDETERMINATE'
      end,
      'has_hidden_override',
        g.visible_total < g.row_total
        and g.selected_total not in (0,g.eligible_total)
    ) order by g.group_key),'[]'::jsonb) groups
    from page_group_rollup g
    cross join params p
  ),
  requested_group_selectors as materialized (
    select
      selector.ordinality::integer request_ordinal,
      selector.value selector,
      upper(selector.value->>'type') selector_type,
      nullif(btrim(selector.value->>'selection_key'),'') selection_key,
      case when selector.value ? 'week_ending_date'
        then (selector.value->>'week_ending_date')::date end week_ending_date,
      case when selector.value ? 'client_id'
        then (selector.value->>'client_id')::uuid end client_id,
      case when selector.value ? 'candidate_id'
        then (selector.value->>'candidate_id')::uuid end candidate_id,
      nullif(upper(btrim(selector.value->>'status_code')),'') status_code
    from jsonb_array_elements(coalesce(v_query->'group_selectors','[]'::jsonb))
      with ordinality selector(value, ordinality)
    where v_mode = 'SUMMARY'
  ),
  requested_group_members as materialized (
    select
      requested.request_ordinal,
      requested.selector,
      row_scope.*
    from requested_group_selectors requested
    join selection_scope_rows row_scope on row_scope.selectable
      and (
        (requested.selector_type='ROW'
          and requested.selection_key=row_scope.selection_key)
        or (requested.selector_type='WEEK'
          and requested.week_ending_date=row_scope.week_ending_date)
        or (requested.selector_type='CLIENT'
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='CANDIDATE' and exists (
          select 1
          from jsonb_array_elements_text(
            coalesce(row_scope.candidate_ids,'[]'::jsonb)
          ) candidate(value)
          where pg_input_is_valid(candidate.value,'uuid')
            and candidate.value::uuid=requested.candidate_id
        ))
        or (requested.selector_type='STATUS'
          and requested.status_code=row_scope.row_status)
        or (requested.selector_type='WEEK_CLIENT'
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='WEEK_CLIENT_CANDIDATE'
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id
          and exists (
            select 1
            from jsonb_array_elements_text(
              coalesce(row_scope.candidate_ids,'[]'::jsonb)
            ) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=requested.candidate_id
          ))
        or (requested.selector_type='STATUS_WEEK'
          and requested.status_code=row_scope.row_status
          and requested.week_ending_date=row_scope.week_ending_date)
        or (requested.selector_type='STATUS_WEEK_CLIENT'
          and requested.status_code=row_scope.row_status
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='DIMENSION_GROUP'
          and (requested.week_ending_date is null or requested.week_ending_date=row_scope.week_ending_date)
          and (requested.client_id is null or requested.client_id=row_scope.client_id)
          and (requested.status_code is null or requested.status_code=row_scope.row_status)
          and (requested.candidate_id is null or exists (
            select 1 from jsonb_array_elements_text(row_scope.candidate_ids) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=requested.candidate_id
          )))
      )
  ),
  requested_group_base as materialized (
    select
      requested.*,
      coalesce((
        select rule.action
        from selection_rules rule
        where exists (
          select 1
          from requested_group_members member
          where member.request_ordinal=requested.request_ordinal
        )
          and not exists (
            select 1
            from requested_group_members member
            where member.request_ordinal=requested.request_ordinal
              and (
                (rule.selector_type='ROW'
                  and rule.selection_key=member.selection_key)
                or (rule.selector_type='WEEK'
                  and rule.week_ending_date=member.week_ending_date)
                or (rule.selector_type='CLIENT'
                  and rule.client_id=member.client_id)
                or (rule.selector_type='CANDIDATE' and exists (
                  select 1
                  from jsonb_array_elements_text(
                    coalesce(member.candidate_ids,'[]'::jsonb)
                  ) candidate(value)
                  where pg_input_is_valid(candidate.value,'uuid')
                    and candidate.value::uuid=rule.candidate_id
                ))
                or (rule.selector_type='STATUS'
                  and rule.status_code=member.row_status)
                or (rule.selector_type='WEEK_CLIENT'
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id)
                or (rule.selector_type='WEEK_CLIENT_CANDIDATE'
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id
                  and exists (
                    select 1
                    from jsonb_array_elements_text(
                      coalesce(member.candidate_ids,'[]'::jsonb)
                    ) candidate(value)
                    where pg_input_is_valid(candidate.value,'uuid')
                      and candidate.value::uuid=rule.candidate_id
                  ))
                or (rule.selector_type='STATUS_WEEK'
                  and rule.status_code=member.row_status
                  and rule.week_ending_date=member.week_ending_date)
                or (rule.selector_type='STATUS_WEEK_CLIENT'
                  and rule.status_code=member.row_status
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id)
                or (rule.selector_type='DIMENSION_GROUP'
                  and (rule.week_ending_date is null or rule.week_ending_date=member.week_ending_date)
                  and (rule.client_id is null or rule.client_id=member.client_id)
                  and (rule.status_code is null or rule.status_code=member.row_status)
                  and (rule.candidate_id is null or exists (
                    select 1 from jsonb_array_elements_text(member.candidate_ids) candidate(value)
                    where pg_input_is_valid(candidate.value,'uuid')
                      and candidate.value::uuid=rule.candidate_id
                  )))
              ) is not true
          )
        order by rule.rule_sequence desc
        limit 1
      ),'INCLUDE') base_action
    from requested_group_selectors requested
  ),
  requested_group_rollup as materialized (
    select
      requested.request_ordinal,
      requested.selector,
      case when count(distinct member.group_key)=1
        then min(member.group_key) end group_key,
      count(member.selection_key)::integer eligible_total,
      count(member.selection_key) filter (
        where member.last_selection_action <> 'EXCLUDE'
      )::integer selected_total,
      coalesce(bool_or(
        member.last_selection_action is distinct from requested.base_action
      ) filter (where member.selection_key is not null),false)
        has_hidden_override
    from requested_group_base requested
    left join requested_group_members member
      on member.request_ordinal=requested.request_ordinal
    group by
      requested.request_ordinal,
      requested.selector,
      requested.base_action
  ),
  summary_group_selection_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selector',rollup.selector,
      'group_key',rollup.group_key,
      'eligible_total',rollup.eligible_total,
      'selected_total',rollup.selected_total,
      'state',case
        when rollup.eligible_total=0 then 'DISABLED'
        when rollup.selected_total=0 then 'UNCHECKED'
        when rollup.selected_total=rollup.eligible_total
          and not rollup.has_hidden_override then 'CHECKED'
        else 'INDETERMINATE'
      end,
      'has_hidden_override',rollup.has_hidden_override
    ) order by rollup.request_ordinal),'[]'::jsonb) groups
    from requested_group_rollup rollup
  ),
  group_selection_json as materialized (
    select case
      when v_mode='SUMMARY' then summary_groups.groups
      else page_groups.groups
    end groups
    from page_group_selection_json page_groups
    cross join summary_group_selection_json summary_groups
  ),
  facet_json as materialized (
    select jsonb_strip_nulls(jsonb_build_object(
      'clients',case when p.facet_kinds ? 'CLIENTS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'id',f.client_id,'label',f.label,'count',f.row_count
        ) order by f.facet_ordinal) from facet_client_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_client_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_client_values f),false) then (
          select jsonb_build_object('after_label',lower(f.label),'after_id',f.client_id)
          from facet_client_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'candidates',case when p.facet_kinds ? 'CANDIDATES' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'id',f.candidate_id,'label',f.label,'count',f.row_count
        ) order by f.facet_ordinal) from facet_candidate_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_candidate_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_candidate_values f),false) then (
          select jsonb_build_object('after_label',lower(f.label),'after_id',f.candidate_id)
          from facet_candidate_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'week_endings',case when p.facet_kinds ? 'WEEK_ENDINGS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'value',f.week_ending_date,'label',to_char(f.week_ending_date,'DD/MM/YYYY'),
          'count',f.row_count
        ) order by f.facet_ordinal) from facet_week_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_week_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_week_values f),false) then (
          select jsonb_build_object('after_value',f.week_ending_date)
          from facet_week_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'statuses',case when p.facet_kinds ? 'STATUSES' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'code',f.row_status,'label',initcap(replace(lower(f.row_status),'_',' ')),
          'count',f.row_count
        ) order by f.facet_ordinal) from facet_status_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_status_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_status_values f),false) then (
          select jsonb_build_object('after_code',f.row_status)
          from facet_status_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'blockers',case when p.facet_kinds ? 'BLOCKERS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'code',f.code,'count',f.row_count
        ) order by f.facet_ordinal) from facet_blocker_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_blocker_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_blocker_values f),false) then (
          select jsonb_build_object('after_code',f.code)
          from facet_blocker_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end
    )) facets
    from params p
  ),
  row_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selection_key',selection_key,
      'group_key',group_key,
      'row_kind',row_kind,
      'scope_key',scope_key,
      'invoice_id',invoice_id,
      'client_id',client_id,
      'client_name',client_name,
      'candidate_ids',candidate_ids,
      'candidate_names',candidate_names,
      'candidate_display',candidate_display,
      'week_ending_dates',week_ending_dates,
      'week_ending_date',week_ending_date,
      'week_ending_display',case when jsonb_array_length(week_ending_dates)>1 then 'Multiple weeks' else to_char(week_ending_date,'DD/MM/YYYY') end,
      'currency',currency,
      'invoice_stream',invoice_stream,
      'total_ex_vat',total_ex_vat,
      'vat_amount',vat_amount,
      'total_inc_vat',total_inc_vat,
      'generation_state',generation_state,
      'row_status',row_status,
      'is_early',is_early,
      'selectable',selectable,
      'selected',selectable and last_selection_action <> 'EXCLUDE',
      'action_blocker_codes',coalesce(action_blocker_codes,'[]'::jsonb),
      'informational_codes',coalesce(informational_codes,'[]'::jsonb),
      'active_operation_id',active_operation_id_text,
      'active_operation_status',active_operation_status,
      'source_revision',source_revision,
      'document_revision',document_revision,
      'command_payload',command_payload,
      'sort_tuple',jsonb_build_object(
        'sort_date',case when sort_date_key is not null then sort_date_key::text end,
        'sort_text',sort_text_key,
        'sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end,
        'selection_key',selection_key
      )
    ) order by page_ordinal),'[]'::jsonb) rows
    from visible_rows
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_CANDIDATES_V2',
    'action','GENERATE',
    'mode',v_mode,
    'snapshot',v_snapshot,
    'normalised_filter',v_filters,
    'normalised_sort',v_sort,
    'filter_hash',v_filter_hash,
    'query_hash',v_query_hash,
    'selection_hash',v_selection_hash,
    'rows',(select rows from row_json),
    'page',jsonb_build_object(
      'page_size',v_page_size,
      'returned_count',(select count(*) from visible_rows),
      'total_count',case
        when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
          then coalesce((select max(k.full_scope_count) from candidate_keys k),0)
        else (select display_count from display_totals)
      end,
      'has_more',case
        when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
          then exists (
            select 1
            from candidate_keys k
            where k.page_ordinal>v_page_size
          )
        else (select count(*) from page_rows)>v_page_size
      end,
      'next_cursor_values',case when (
        case
          when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
            then exists (
              select 1
              from candidate_keys k
              where k.page_ordinal>v_page_size
            )
          else (select count(*) from page_rows)>v_page_size
        end
      ) then (
        select case when v_mode='EXPAND_SELECTION'
          then jsonb_build_object('after_selection_key',selection_key)
          else jsonb_build_object(
            'after_selection_key',selection_key,
            'after_sort_date',case when sort_date_key is not null then sort_date_key::text end,
            'after_sort_text',sort_text_key,
            'after_sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end
          )
        end
        from (
          select
            selection_key,
            sort_date_key,
            sort_text_key,
            sort_numeric_key,
            page_ordinal
          from candidate_keys
          where page_ordinal<=v_page_size
        ) cursor_source
        order by page_ordinal desc
        limit 1
      ) else null end
    ),
    'totals',jsonb_build_object(
      'all',(select all_count from totals),
      'filtered_total',(select all_count from totals),
      'display_total',(select display_count from display_totals),
      'eligible_total',(select ready_count from totals),
      'selected_total',(select selected_count from totals),
      'excluded_total',(select ready_count-selected_count from totals),
      'ready',(select ready_count from totals),
      'blocked',(select blocked_count from totals),
      'blocked_total',(select blocked_count from totals),
      'in_progress',(select in_progress_count from totals),
      'not_generated',(select not_generated_count from totals),
      'stale',(select stale_count from totals),
      'failed_retryable',(select failed_retryable_count from totals)
    ),
    'selection_summary',jsonb_build_object(
      'eligible_total',(select ready_count from totals),
      'selected_total',(select selected_count from totals),
      'excluded_total',(select ready_count-selected_count from totals),
      'blocked_total',(select blocked_count from totals),
      'exact',v_mode in ('FACETS','SUMMARY')
    ),
    'group_selection',(select groups from group_selection_json),
    'facets',case
      when v_mode='FACETS' then (select facets from facet_json)
      else jsonb_build_object()
    end,
    'selection_seed',jsonb_build_object('mode','IMPLICIT_ALL','default_selected',true)
  ) into v_result;

  if v_mode='SUMMARY'
     and coalesce((v_result#>>'{totals,filtered_total}')::integer,0)>25000 then
    raise exception using
      errcode='54000',
      message='BATCH_SUMMARY_SCOPE_TOO_LARGE';
  end if;

  if v_mode='EXPLICIT_KEYS' and (
    jsonb_array_length(coalesce(v_result->'rows','[]'::jsonb))
      <> jsonb_array_length(v_selection_keys)
    or exists (
      select 1
      from jsonb_array_elements(v_result->'rows') row_item(row_json)
      where coalesce(row_json->>'source_revision','')
        is distinct from coalesce(
          v_expected_source_revisions->>(row_json->>'selection_key'),
          ''
        )
    )
  ) then
    raise exception using
      errcode='40001',
      message='BATCH_SOURCE_CHANGED';
  end if;

  v_snapshot_after := private._invoice_candidate_snapshot_verify_v2(
    'GENERATE',
    v_snapshot,
    coalesce(p_now_utc,now())
  );
  if v_snapshot_after->>'revision' <> v_snapshot->>'revision' then
    raise exception using errcode='40001', message='BATCH_SNAPSHOT_CHANGED';
  end if;

  return v_result;
end;
$function$;

-- private._invoice_batch_generate_candidates_legacy_20260726(boolean,integer,text[])
CREATE OR REPLACE FUNCTION private._invoice_batch_generate_candidates_legacy_20260726(p_allow_early boolean DEFAULT false, p_limit integer DEFAULT 5000, p_scope_keys text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_scope_keys text[];
begin
  if p_scope_keys is not null then
    if cardinality(p_scope_keys)>500 then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_LIMIT_EXCEEDED';
    end if;
    if exists(
      select 1
      from unnest(p_scope_keys) value
      where value is null
        or btrim(value)=''
        or length(btrim(value))>512
    ) then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_INVALID';
    end if;
    select coalesce(array_agg(value order by value),'{}'::text[])
    into v_scope_keys
    from(
      select distinct btrim(raw_value) value
      from unnest(p_scope_keys) raw_value
    ) deduplicated;
  end if;

  return(
with anchor as materialized (
  select now() evaluation_utc,(now() at time zone 'Europe/London')::date today
),
source_candidates as materialized (
  select distinct tf.timesheet_id
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id=tf.timesheet_id and ts.is_current and ts.revoked_at is null
  cross join anchor a
  where tf.is_current and tf.client_id is not null
  order by tf.timesheet_id
),
command as materialized (
  select jsonb_build_array(jsonb_build_object(
    'command_type','GENERATE_SELECTED',
    'source_ids',coalesce(jsonb_agg(e.timesheet_id order by e.timesheet_id),'[]'::jsonb),
    'allow_early',coalesce(p_allow_early,false))) commands
  from source_candidates e
),
resolved_groups as materialized (
  select r.*
  from command c cross join anchor a
  cross join lateral private._invoice_generation_resolve_command_groups(
    c.commands,null,a.evaluation_utc) r
),
groups as materialized (
  select r.*
  from resolved_groups r
  where v_scope_keys is null
     or r.group_key=any(v_scope_keys)
  order by r.target_invoice_week nulls last,r.client_id,r.invoice_stream,r.group_key
  limit case
    when v_scope_keys is null
      then greatest(1,least(coalesce(p_limit,5000),20000))
    else 500
  end
),
group_sources as materialized (
  select g.*,m.value member,
    case when coalesce(m.value->>'source_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      then (m.value->>'source_id')::uuid end source_id,
    case when coalesce(m.value->>'related_timesheet_id',
        m.value->>'source_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      then coalesce(m.value->>'related_timesheet_id',
        m.value->>'source_id')::uuid end timesheet_id,
    nullif(m.value->>'segment_id','') segment_id
  from groups g
  cross join lateral jsonb_array_elements(g.canonical_source_members) m(value)
),
vat_policy as materialized (
  select v.*
  from private._invoice_generation_vat_policy_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',gs.member->>'source_member_key',
      'source_type',gs.member->>'source_type',
      'source_id',gs.source_id,
      'timesheet_id',gs.member->>'related_timesheet_id',
      'segment_id',gs.segment_id,
      'effective_date',gs.effective_settings_date)
      order by gs.group_key,gs.member->>'source_member_key')
    from group_sources gs
  ),'[]'::jsonb)) v
),
reference_policy as materialized (
  select r.*
  from private._invoice_source_reference_validate_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',gs.member->>'source_member_key',
      'source_type',gs.member->>'source_type',
      'source_id',gs.source_id,
      'related_timesheet_id',gs.timesheet_id,
      'segment_id',gs.segment_id,
      'target_invoice_week',gs.target_invoice_week,
      'invoice_stream',gs.invoice_stream,
      'consolidation_mode',gs.consolidation_mode)
      order by gs.group_key,gs.member->>'source_member_key')
    from group_sources gs
  ),'[]'::jsonb)) r
),
correction_scopes as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','generate-candidate:'||g.group_key,
    'scope_key',g.group_key,
    'validation_purpose','CANDIDATE_GENERATION',
    'expected_client_id',g.client_id,
    'expected_contract_id',case when cardinality(g.contract_ids)=1
      then g.contract_ids[1] end,
    'natural_source_week',case when cardinality(g.natural_source_weeks)=1
      then g.natural_source_weeks[1] end,
    'target_invoice_week',g.target_invoice_week,
    'expected_invoice_stream',g.invoice_stream,
    'planned_members',coalesce((select jsonb_agg(jsonb_build_object(
      'timesheet_id',gs.timesheet_id,
      'source_type',gs.member->>'source_type',
      'source_id',gs.source_id,
      'source_member_key',gs.member->>'source_member_key',
      'segment_id',gs.segment_id,
      'target_invoice_week',gs.target_invoice_week,
      'vat_rate_pct',vat.vat_rate)
      order by gs.member->>'source_member_key')
      from group_sources gs
      left join vat_policy vat
        on vat.source_member_key=gs.member->>'source_member_key'
      where gs.group_key=g.group_key),'[]'::jsonb))
    order by g.group_key),'[]'::jsonb) scopes
  from groups g
),
correction_validation as materialized (
  select c.*
  from correction_scopes s
  cross join lateral private._invoice_correction_validate_batch(
    s.scopes,(select today from anchor)) c
),
correction_group_results as materialized (
  select c.scope_key group_key,c.valid,c.blocker_code,c.blocker_codes,
    c.detail_json details
  from correction_validation c
),independent_member_blockers as materialized (
  select gs.group_key,gs.member->>'source_member_key' source_member_key,
    blocker.code blocker_code,blocker.ordinality blocker_order
  from group_sources gs
  left join public.timesheets ts
    on ts.timesheet_id=gs.timesheet_id and ts.is_current
  left join public.timesheets_financials tf
    on tf.timesheet_id=gs.timesheet_id and tf.is_current
  left join public.v_ts_invoice_precheck pc
    on pc.timesheet_id=gs.timesheet_id
  left join reference_policy ref
    on ref.source_member_key=gs.member->>'source_member_key'
  left join vat_policy vat
    on vat.source_member_key=gs.member->>'source_member_key'
  cross join lateral unnest(array_remove(array[
    case when ts.timesheet_id is null then 'TIMESHEET_NOT_CURRENT' end,
    case when tf.id is null then 'CURRENT_FINANCIALS_MISSING' end,
    case when coalesce(tf.is_stale,true) then 'FINANCIALS_STALE' end,
    case when coalesce(tf.processing_status::text,'')<>'READY_FOR_INVOICE'
      then 'NOT_READY_FOR_INVOICE' end,
    case when coalesce(tf.has_rate_issue,false) then 'RATE_MISSING' end,
    case when coalesce(tf.has_pay_channel_issue,false)
      then 'PAY_CHANNEL_MISSING' end,
    case when tf.locked_by_invoice_id is not null
      then 'SOURCE_ALREADY_LOCKED' end,
    case when upper(coalesce(ts.submission_mode::text,''))='QR'
      and(nullif(ts.qr_signed_hash,'') is null
        or ts.qr_signed_at_utc is null)
      then 'QR_TIMESHEET_UNSIGNED' end,
    case when coalesce(pc.require_reference_to_invoice,false)
        and coalesce(ref.reference_ready,false) is not true
      then coalesce(ref.blocker_code,'MISSING_REFERENCE') end,
    case when(coalesce(tf.mileage_pay_ex_vat,0)<>0
        or coalesce(tf.mileage_charge_ex_vat,0)<>0)
      and not exists(
        select 1 from public.timesheet_evidence e
        where e.timesheet_id=gs.timesheet_id
          and upper(coalesce(e.kind,''))='MILEAGE'
          and nullif(e.storage_key,'') is not null)
      then 'MISSING_MILEAGE_EVIDENCE' end,
    case when(coalesce(tf.expenses_pay_ex_vat,0)<>0
        or coalesce(tf.expenses_charge_ex_vat,0)<>0
        or coalesce(tf.travel_pay_ex_vat,0)<>0
        or coalesce(tf.travel_charge_ex_vat,0)<>0
        or coalesce(tf.accommodation_pay_ex_vat,0)<>0
        or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
      and not exists(
        select 1 from public.timesheet_evidence e
        where e.timesheet_id=gs.timesheet_id
          and upper(coalesce(e.kind,'')) in(
            'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
          and nullif(e.storage_key,'') is not null)
      then 'MISSING_EXPENSE_EVIDENCE' end,
    case when exists(
      select 1
      from public.timesheet_evidence e
      join public.invoice_document_assets a on a.id=e.document_asset_id
      where e.timesheet_id=gs.timesheet_id
        and a.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'))
      then 'REQUIRED_ASSET_PERMANENT_FAILURE' end,
    case when coalesce(vat.valid,false) is not true
      then coalesce(vat.blocker_code,'VAT_POLICY_UNRESOLVED') end,
    case when exists(
      select 1 from public.invoice_lines l
      join public.invoices i on i.id=l.invoice_id
      where l.timesheet_id=gs.timesheet_id
        and i.status in('DRAFT','ISSUED','ON_HOLD')
        and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS')
      then 'SOURCE_ALREADY_INVOICED' end
  ],null)) with ordinality blocker(code,ordinality)
),
independent_group_blockers as materialized (
  select b.group_key,
    (array_agg(b.blocker_code
      order by b.source_member_key,b.blocker_order))[1] blocker_code,
    jsonb_build_object(
      'code',(array_agg(b.blocker_code
        order by b.source_member_key,b.blocker_order))[1],
      'sources',jsonb_agg(jsonb_build_object(
        'source_member_key',b.source_member_key,
        'code',b.blocker_code)
        order by b.source_member_key,b.blocker_order)) blocker_detail
  from independent_member_blockers b
  group by b.group_key
),
selected_totals as materialized (
  select gs.group_key,gs.timesheet_id,
    case when bool_or(gs.segment_id is not null) then
      round(coalesce(sum(case when coalesce(seg.value->>'charge_amount','')~
        '^[+-]?[0-9]+([.][0-9]+)?$'
        then (seg.value->>'charge_amount')::numeric
        when coalesce(seg.value->>'charge_ex_vat','')~
        '^[+-]?[0-9]+([.][0-9]+)?$'
        then (seg.value->>'charge_ex_vat')::numeric else 0 end),0),2)
      else max(round(coalesce(tf.total_charge_ex_vat,0),2)) end total_charge_ex_vat,
    case when bool_or(gs.segment_id is not null) then
      round(coalesce(sum(
        (case when coalesce(seg.value->>'hours_day','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_day')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_night','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_night')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_sat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_sat')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_sun','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_sun')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_bh','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_bh')::numeric else 0 end)
      ),0),2)
      else max(round(coalesce(tf.total_hours,0),2)) end total_hours
  from group_sources gs
  join public.timesheets_financials tf
    on tf.timesheet_id=gs.timesheet_id and tf.is_current
  left join lateral jsonb_array_elements(
    case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
      then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg(value)
    on gs.segment_id is not null and seg.value->>'segment_id'=gs.segment_id
  group by gs.group_key,gs.timesheet_id
),
source_display as materialized (
  select distinct gs.group_key,tf.timesheet_id,tf.client_id,
    ts.week_ending_date,ts.submission_mode,tf.basis,
    totals.total_charge_ex_vat,totals.total_hours,
    s.client_name,s.candidate_name,s.validation_status,
    coalesce(s.hr_validation_required_for_invoice,false) hr_validation_required_for_invoice,
    coalesce(s.hr_validation_required_for_invoice,false)
      and(s.validation_status is null or s.validation_status<>all(array[
        'VALIDATION_OK'::public.validation_status_enum,
        'OVERRIDDEN'::public.validation_status_enum])) blocked_by_hr_validation,
    pc.precheck_status,
    exists(
      select 1 from public.timesheet_evidence e
      left join public.invoice_document_assets a on a.id=e.document_asset_id
      where e.timesheet_id=tf.timesheet_id
        and e.processing_state not in('SUPERSEDED')
        and(a.id is null or a.status<>'READY')) unready_evidence_asset,
    exists(
      select 1 from public.invoice_document_versions dv
      where dv.entity_type='TIMESHEET'
        and dv.entity_id=tf.timesheet_id
        and dv.purpose='TIMESHEET'
        and dv.source_revision=ts.document_revision::text
        and dv.status='READY'
        and dv.r2_key is not null and dv.sha256~'^[0-9a-f]{64}$'
        and coalesce(dv.size_bytes,0)>0
        and coalesce(dv.page_count,0)>0) timesheet_document_ready,
    ts.document_revision,ts.document_state,ts.current_document_version_id
  from group_sources gs
  join public.timesheets_financials tf
    on tf.timesheet_id=gs.timesheet_id and tf.is_current
  join public.timesheets ts on ts.timesheet_id=tf.timesheet_id and ts.is_current
  join selected_totals totals
    on totals.group_key=gs.group_key
   and totals.timesheet_id=gs.timesheet_id
  join public.v_ts_invoice_precheck pc on pc.timesheet_id=tf.timesheet_id
  left join public.v_timesheets_summary_base s on s.timesheet_id=tf.timesheet_id
),
active_exact as materialized (
  select distinct on(c.payload_json->>'group_key',c.payload_json->>'source_revision')
    c.payload_json->>'group_key' group_key,o.id operation_id,o.status,
    c.payload_json->>'source_revision' source_revision,
    o.progress_json,o.error_json,o.updated_at_utc
  from public.invoice_operation_chunks c
  join public.invoice_operations o on o.id=c.operation_id
  where c.chunk_type='GENERATION_GROUP'
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
  order by c.payload_json->>'group_key',c.payload_json->>'source_revision',
    o.priority desc,o.created_at_utc desc
),
group_json as materialized (
  select g.client_id,g.group_key,g.target_invoice_week,
    g.consolidation_mode,g.invoice_stream,g.source_revision_hash,
    g.canonical_source_ids,g.canonical_source_members,
    coalesce(sum(sd.total_charge_ex_vat),0)::numeric subtotal_ex_vat,
    coalesce(sum(sd.total_hours),0)::numeric total_hours,
    jsonb_agg(jsonb_build_object(
      'timesheet_id',sd.timesheet_id,
      'candidate_name',sd.candidate_name,
      'week_ending_date',sd.week_ending_date,
      'total_charge_ex_vat',sd.total_charge_ex_vat,
      'total_hours',sd.total_hours,
      'basis',sd.basis::text,
      'submission_mode',coalesce(sd.submission_mode::text,''),
      'validation_status',coalesce(sd.validation_status::text,''),
      'hr_validation_required_for_invoice',sd.hr_validation_required_for_invoice,
      'blocked_by_hr_validation',sd.blocked_by_hr_validation,
      'precheck_status',coalesce(sd.precheck_status::text,''),
      'document_revision',sd.document_revision,
      'document_state',sd.document_state,
      'current_document_version_id',sd.current_document_version_id,
      'timesheet_document_ready',sd.timesheet_document_ready,
      'unready_evidence_asset',sd.unready_evidence_asset)
      order by sd.candidate_name nulls last,sd.timesheet_id) timesheets,
    case
      when g.blocker_code is not null then g.blocker_code
      when independent.blocker_code is not null
        then independent.blocker_code
      when coalesce(correction.valid,true) is not true
        then coalesce(correction.blocker_code,
          'INVOICE_CORRECTION_UNIT_INVALID')
      when bool_or(sd.blocked_by_hr_validation) then 'HR_VALIDATION_BLOCKED'
      when not coalesce(p_allow_early,false)
        and exists(
          select 1 from jsonb_array_elements(g.canonical_source_members) early(value)
          where case when pg_input_is_valid(
              coalesce(early.value->>'target_invoice_week',''),'date')
            then(early.value->>'target_invoice_week')::date+6
              >=(select today from anchor)
            else false end)
      then 'EARLY_GENERATION_NOT_ALLOWED'
      end blocker_code,
    case
      when g.blocker_code is not null then g.blocker_detail
      when independent.blocker_code is not null
        then independent.blocker_detail
      when coalesce(correction.valid,true) is not true
        then jsonb_build_object(
          'code',coalesce(correction.blocker_code,
            'INVOICE_CORRECTION_UNIT_INVALID'),
          'correction_validation',correction.details)
      when bool_or(sd.blocked_by_hr_validation)
        then jsonb_build_object('code','HR_VALIDATION_BLOCKED',
          'sources',coalesce(jsonb_agg(sd.timesheet_id order by sd.timesheet_id)
            filter(where sd.blocked_by_hr_validation),'[]'::jsonb))
      when not coalesce(p_allow_early,false)
        and exists(
          select 1 from jsonb_array_elements(g.canonical_source_members) early(value)
          where case when pg_input_is_valid(
              coalesce(early.value->>'target_invoice_week',''),'date')
            then(early.value->>'target_invoice_week')::date+6
              >=(select today from anchor)
            else false end)
      then jsonb_build_object('code','EARLY_GENERATION_NOT_ALLOWED')
      end blocker_detail,
    a.operation_id active_operation_id,a.status active_status,
    a.progress_json active_progress,a.error_json active_error,
    coalesce(correction.details,'[]'::jsonb) correction_validation
  from groups g join source_display sd on sd.group_key=g.group_key
  left join independent_group_blockers independent
    on independent.group_key=g.group_key
  left join correction_group_results correction
    on correction.group_key=g.group_key
  left join active_exact a on a.group_key=g.group_key
    and a.source_revision=g.source_revision_hash
  group by g.client_id,g.group_key,g.target_invoice_week,g.consolidation_mode,
    g.invoice_stream,g.source_revision_hash,g.canonical_source_ids,
    g.canonical_source_members,g.blocker_code,g.blocker_detail,
    independent.blocker_code,independent.blocker_detail,
    correction.valid,correction.blocker_code,correction.details,
    a.operation_id,a.status,a.progress_json,a.error_json
),
clients as materialized (
  select g.client_id,max(c.name) client_name,
    jsonb_agg(jsonb_build_object(
      'group_key',g.group_key,
      'invoice_week_start',g.target_invoice_week,
      'week_ending_date',case when g.target_invoice_week is null
        then null else g.target_invoice_week+6 end,
      'subtotal_ex_vat',round(g.subtotal_ex_vat,2),
      'total_hours',round(g.total_hours,2),
      'stream',g.invoice_stream,
      'consolidation_mode',g.consolidation_mode,
      'canonical_source_ids',to_jsonb(g.canonical_source_ids),
      'canonical_source_members',g.canonical_source_members,
      'canonical_source_revision',g.source_revision_hash,
      'blocker_code',g.blocker_code,'blocker_detail',g.blocker_detail,
      'correction_validation',g.correction_validation,
      'document_dependencies',coalesce((
        select jsonb_agg(jsonb_build_object(
          'timesheet_id',d.value->>'timesheet_id',
          'code','TIMESHEET_DOCUMENT_NOT_READY')
          order by d.value->>'timesheet_id')
        from jsonb_array_elements(g.timesheets) d(value)
        where coalesce((d.value->>'timesheet_document_ready')::boolean,
          false) is false
      ),'[]'::jsonb),
      'command_payload',jsonb_build_object(
        'command_type','GENERATE_SELECTED',
        'canonical_source_ids',to_jsonb(g.canonical_source_ids),
        'canonical_source_members',g.canonical_source_members,
        'group_key',g.group_key,'source_revision',g.source_revision_hash,
        'target_invoice_week',g.target_invoice_week,
        'consolidation_mode',g.consolidation_mode,
        'invoice_stream',g.invoice_stream,
        'correction_validation',g.correction_validation,
        'allow_early',coalesce(p_allow_early,false)),
      'timesheets',g.timesheets,
      'active_generation_operation_id',g.active_operation_id,
      'active_generation_status',g.active_status,
      'active_generation_progress',g.active_progress,
      'last_generation_error',g.active_error,
      'retry_available',g.active_status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'))
      order by g.target_invoice_week nulls last,g.group_key) groups,
    count(*)::integer expected_invoice_count
  from group_json g join public.clients c on c.id=g.client_id
  group by g.client_id
)
select coalesce(jsonb_agg(jsonb_build_object(
  'client_id',c.client_id,'client_name',c.client_name,
  'invoice_consolidation_mode',
    coalesce(c.groups->0->>'consolidation_mode','NONE'),
  'consolidation_label',case coalesce(c.groups->0->>'consolidation_mode','NONE')
    when 'NONE' then 'One per timesheet'
    when 'BY_WEEK' then 'Consolidated by week'
    when 'ANY_WEEK' then 'Consolidated across weeks'
    else 'One per timesheet' end,
  'expected_invoice_count',c.expected_invoice_count,
  'groups',c.groups,
  'weeks',c.groups,
  'canonical_source_ids',coalesce((
    select jsonb_agg(distinct source_id order by source_id)
    from jsonb_array_elements(c.groups) g
    cross join lateral jsonb_array_elements_text(g->'canonical_source_ids') source_id
  ),'[]'::jsonb),
  'current_source_count',coalesce((
    select count(distinct source_id)
    from jsonb_array_elements(c.groups) g
    cross join lateral jsonb_array_elements_text(g->'canonical_source_ids') source_id
  ),0)
) order by c.client_name,c.client_id),'[]'::jsonb)
from clients c
  );
end;
$function$;

-- private._invoice_batch_generate_classification_v2(boolean,text[],timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_generate_classification_v2(p_allow_early boolean DEFAULT false, p_scope_keys text[] DEFAULT NULL::text[], p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(selection_key text, candidate_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_scope_keys text[];
begin
  if p_scope_keys is not null then
    if cardinality(p_scope_keys)>250 then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_LIMIT_EXCEEDED';
    end if;
    if exists(
      select 1
      from unnest(p_scope_keys) value
      where value is null
        or btrim(value)=''
        or length(btrim(value))>512
    ) then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_INVALID';
    end if;
    select coalesce(array_agg(value order by value),'{}'::text[])
    into v_scope_keys
    from(
      select distinct btrim(raw_value) value
      from unnest(p_scope_keys) raw_value
    ) deduplicated;
  end if;

  return query
  with
  anchor as materialized (
    select
      coalesce(p_now_utc,statement_timestamp()) evaluation_utc,
      (
        coalesce(p_now_utc,statement_timestamp())
        at time zone 'Europe/London'
      )::date today
  ),
  source_candidates as materialized (
    select distinct financial.timesheet_id
    from public.timesheets_financials financial
    join public.timesheets timesheet
      on timesheet.timesheet_id=financial.timesheet_id
     and timesheet.is_current
     and timesheet.revoked_at is null
    where financial.is_current
      and financial.client_id is not null
    order by financial.timesheet_id
  ),
  command as materialized (
    select jsonb_build_array(jsonb_build_object(
      'command_type','GENERATE_SELECTED',
      'source_ids',coalesce(
        jsonb_agg(source.timesheet_id order by source.timesheet_id),
        '[]'::jsonb
      ),
      'allow_early',coalesce(p_allow_early,false),
      'scope_keys',case
        when v_scope_keys is null then null
        else to_jsonb(v_scope_keys)
      end
    )) commands
    from source_candidates source
  ),
  groups as materialized (
    select resolved.*
    from command
    cross join anchor
    cross join lateral private._invoice_generation_resolve_command_groups(
      command.commands,
      null,
      anchor.evaluation_utc
    ) resolved
    where v_scope_keys is null
       or resolved.group_key=any(v_scope_keys)
  ),
  group_sources as materialized (
    select
      group_row.*,
      member.value member,
      case
        when coalesce(member.value->>'source_id','')~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
          then (member.value->>'source_id')::uuid
      end source_id,
      case
        when coalesce(
          member.value->>'related_timesheet_id',
          member.value->>'source_id',
          ''
        )~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
          then coalesce(
            member.value->>'related_timesheet_id',
            member.value->>'source_id'
          )::uuid
      end timesheet_id,
      nullif(member.value->>'segment_id','') segment_id
    from groups group_row
    cross join lateral jsonb_array_elements(
      group_row.canonical_source_members
    ) member(value)
  ),
  vat_policy as materialized (
    select policy.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',source.member->>'source_member_key',
        'source_type',source.member->>'source_type',
        'source_id',source.source_id,
        'timesheet_id',source.member->>'related_timesheet_id',
        'segment_id',source.segment_id,
        'effective_date',source.effective_settings_date
      ) order by
        source.group_key,
        source.member->>'source_member_key'
      )
      from group_sources source
    ),'[]'::jsonb)) policy
  ),
  reference_policy as materialized (
    select policy.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',source.member->>'source_member_key',
        'source_type',source.member->>'source_type',
        'source_id',source.source_id,
        'related_timesheet_id',source.timesheet_id,
        'segment_id',source.segment_id,
        'target_invoice_week',source.target_invoice_week,
        'invoice_stream',source.invoice_stream,
        'consolidation_mode',source.consolidation_mode
      ) order by
        source.group_key,
        source.member->>'source_member_key'
      )
      from group_sources source
    ),'[]'::jsonb)) policy
  ),
  correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generate-candidate:'||group_row.group_key,
      'scope_key',group_row.group_key,
      'validation_purpose','CANDIDATE_GENERATION',
      'expected_client_id',group_row.client_id,
      'expected_contract_id',case
        when cardinality(group_row.contract_ids)=1
          then group_row.contract_ids[1]
      end,
      'natural_source_week',case
        when cardinality(group_row.natural_source_weeks)=1
          then group_row.natural_source_weeks[1]
      end,
      'target_invoice_week',group_row.target_invoice_week,
      'expected_invoice_stream',group_row.invoice_stream,
      'planned_members',coalesce((
        select jsonb_agg(jsonb_build_object(
          'timesheet_id',source.timesheet_id,
          'source_type',source.member->>'source_type',
          'source_id',source.source_id,
          'source_member_key',source.member->>'source_member_key',
          'segment_id',source.segment_id,
          'target_invoice_week',source.target_invoice_week,
          'vat_rate_pct',vat.vat_rate
        ) order by source.member->>'source_member_key')
        from group_sources source
        left join vat_policy vat
          on vat.source_member_key=
            source.member->>'source_member_key'
        where source.group_key=group_row.group_key
      ),'[]'::jsonb)
    ) order by group_row.group_key),'[]'::jsonb) scopes
    from groups group_row
  ),
  correction_validation as materialized (
    select validation.*
    from correction_scopes scope
    cross join lateral private._invoice_correction_validate_batch(
      scope.scopes,
      (select today from anchor)
    ) validation
  ),
  correction_group_results as materialized (
    select
      validation.scope_key group_key,
      validation.valid,
      validation.blocker_code,
      validation.blocker_codes,
      validation.detail_json details
    from correction_validation validation
  ),
  independent_member_blockers as materialized (
    select
      source.group_key,
      source.member->>'source_member_key' source_member_key,
      blocker.code blocker_code,
      blocker.ordinality blocker_order
    from group_sources source
    left join public.timesheets timesheet
      on timesheet.timesheet_id=source.timesheet_id
     and timesheet.is_current
    left join public.timesheets_financials financial
      on financial.timesheet_id=source.timesheet_id
     and financial.is_current
    left join public.v_ts_invoice_precheck precheck
      on precheck.timesheet_id=source.timesheet_id
    left join reference_policy reference
      on reference.source_member_key=
        source.member->>'source_member_key'
    left join vat_policy vat
      on vat.source_member_key=source.member->>'source_member_key'
    cross join lateral unnest(array_remove(array[
      case
        when timesheet.timesheet_id is null
          then 'TIMESHEET_NOT_CURRENT'
      end,
      case
        when financial.timesheet_id is null
          then 'CURRENT_FINANCIALS_MISSING'
      end,
      case
        when coalesce(financial.is_stale,true)
          then 'FINANCIALS_STALE'
      end,
      case
        when coalesce(financial.processing_status::text,'')
          <>'READY_FOR_INVOICE'
          then 'NOT_READY_FOR_INVOICE'
      end,
      case
        when coalesce(financial.has_rate_issue,false)
          then 'RATE_MISSING'
      end,
      case
        when coalesce(financial.has_pay_channel_issue,false)
          then 'PAY_CHANNEL_MISSING'
      end,
      case
        when financial.locked_by_invoice_id is not null
          then 'SOURCE_ALREADY_LOCKED'
      end,
      case
        when upper(coalesce(timesheet.submission_mode::text,''))='QR'
         and (
           nullif(timesheet.qr_signed_hash,'') is null
           or timesheet.qr_signed_at_utc is null
         )
          then 'QR_TIMESHEET_UNSIGNED'
      end,
      case
        when coalesce(precheck.require_reference_to_invoice,false)
         and coalesce(financial.total_hours,0)>0
         and coalesce(reference.reference_ready,false) is not true
          then coalesce(reference.blocker_code,'MISSING_REFERENCE')
      end,
      case
        when (
          coalesce(financial.mileage_pay_ex_vat,0)<>0
          or coalesce(financial.mileage_charge_ex_vat,0)<>0
        )
        and not exists(
          select 1
          from public.timesheet_evidence evidence
          where evidence.timesheet_id=source.timesheet_id
            and upper(coalesce(evidence.kind,''))='MILEAGE'
            and nullif(evidence.storage_key,'') is not null
        )
          then 'MISSING_MILEAGE_EVIDENCE'
      end,
      case
        when (
          coalesce(financial.expenses_pay_ex_vat,0)<>0
          or coalesce(financial.expenses_charge_ex_vat,0)<>0
          or coalesce(financial.travel_pay_ex_vat,0)<>0
          or coalesce(financial.travel_charge_ex_vat,0)<>0
          or coalesce(financial.accommodation_pay_ex_vat,0)<>0
          or coalesce(financial.accommodation_charge_ex_vat,0)<>0
        )
        and not exists(
          select 1
          from public.timesheet_evidence evidence
          where evidence.timesheet_id=source.timesheet_id
            and upper(coalesce(evidence.kind,'')) in(
              'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES'
            )
            and nullif(evidence.storage_key,'') is not null
        )
          then 'MISSING_EXPENSE_EVIDENCE'
      end,
      case
        when exists(
          select 1
          from public.timesheet_evidence evidence
          join public.invoice_document_assets asset
            on asset.id=evidence.document_asset_id
          where evidence.timesheet_id=source.timesheet_id
            and asset.status in(
              'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'
            )
        )
          then 'REQUIRED_ASSET_PERMANENT_FAILURE'
      end,
      case
        when coalesce(vat.valid,false) is not true
          then coalesce(vat.blocker_code,'VAT_POLICY_UNRESOLVED')
      end,
      case
        when exists(
          select 1
          from public.invoice_lines line
          join public.invoices invoice on invoice.id=line.invoice_id
          where line.timesheet_id=source.timesheet_id
            and invoice.status in('DRAFT','ISSUED','ON_HOLD')
            and coalesce(
              financial.invoice_breakdown_json->>'mode',
              ''
            )<>'SEGMENTS'
        )
          then 'SOURCE_ALREADY_INVOICED'
      end
    ],null)) with ordinality blocker(code,ordinality)
  ),
  independent_group_blockers as materialized (
    select
      blocker.group_key,
      (
        array_agg(
          blocker.blocker_code
          order by blocker.source_member_key,blocker.blocker_order
        )
      )[1] blocker_code,
      jsonb_build_object(
        'code',(
          array_agg(
            blocker.blocker_code
            order by blocker.source_member_key,blocker.blocker_order
          )
        )[1],
        'sources',jsonb_agg(jsonb_build_object(
          'source_member_key',blocker.source_member_key,
          'code',blocker.blocker_code
        ) order by blocker.source_member_key,blocker.blocker_order)
      ) blocker_detail
    from independent_member_blockers blocker
    group by blocker.group_key
  ),
  selected_member_totals as materialized (
    select
      source.group_key,
      source.member->>'source_member_key' source_member_key,
      source.timesheet_id,
      source.segment_id,
      round(case
        when source.segment_id is not null then coalesce((
          select sum(case
            when coalesce(segment.value->>'charge_amount','')~
              '^[+-]?[0-9]+([.][0-9]+)?$'
              then (segment.value->>'charge_amount')::numeric
            when coalesce(segment.value->>'charge_ex_vat','')~
              '^[+-]?[0-9]+([.][0-9]+)?$'
              then (segment.value->>'charge_ex_vat')::numeric
            else 0
          end)
          from jsonb_array_elements(
            case
              when jsonb_typeof(
                financial.invoice_breakdown_json->'segments'
              )='array'
                then financial.invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) segment(value)
          where coalesce(
            nullif(segment.value->>'segment_id',''),
            left(
              encode(
                digest(segment.value::text,'sha256'),
                'hex'
              ),
              24
            )
          )=source.segment_id
        ),0)
        else coalesce(financial.total_charge_ex_vat,0)
      end,2) total_charge_ex_vat,
      round(case
        when source.segment_id is not null then coalesce((
          select sum(
            case
              when coalesce(segment.value->>'hours_day','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_day')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_night','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_night')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_sat','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_sat')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_sun','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_sun')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_bh','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_bh')::numeric
              else 0
            end
          )
          from jsonb_array_elements(
            case
              when jsonb_typeof(
                financial.invoice_breakdown_json->'segments'
              )='array'
                then financial.invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) segment(value)
          where coalesce(
            nullif(segment.value->>'segment_id',''),
            left(
              encode(
                digest(segment.value::text,'sha256'),
                'hex'
              ),
              24
            )
          )=source.segment_id
        ),0)
        else coalesce(financial.total_hours,0)
      end,2) total_hours
    from group_sources source
    join public.timesheets_financials financial
      on financial.timesheet_id=source.timesheet_id
     and financial.is_current
  ),
  generation_totals as materialized (
    select
      source.group_key,
      round(coalesce(sum(total.total_charge_ex_vat),0),2)
        total_ex_vat,
      round(coalesce(sum(round(
        total.total_charge_ex_vat*coalesce(vat.vat_rate,0)/100,
        2
      )),0),2) vat_amount,
      round(coalesce(sum(total.total_hours),0),2) total_hours
    from group_sources source
    join selected_member_totals total
      on total.group_key=source.group_key
     and total.source_member_key=
       source.member->>'source_member_key'
    left join vat_policy vat
      on vat.source_member_key=source.member->>'source_member_key'
    group by source.group_key
  ),
  timesheet_totals as materialized (
    select
      total.group_key,
      total.timesheet_id,
      round(sum(total.total_charge_ex_vat),2) total_charge_ex_vat,
      round(sum(total.total_hours),2) total_hours
    from selected_member_totals total
    group by total.group_key,total.timesheet_id
  ),
  source_display as materialized (
    select distinct
      source.group_key,
      financial.timesheet_id,
      financial.client_id,
      financial.candidate_id,
      timesheet.week_ending_date,
      timesheet.submission_mode,
      financial.basis,
      total.total_charge_ex_vat,
      total.total_hours,
      summary.client_name,
      summary.candidate_name,
      summary.validation_status,
      coalesce(
        summary.hr_validation_required_for_invoice,
        false
      ) hr_validation_required_for_invoice,
      coalesce(
        summary.hr_validation_required_for_invoice,
        false
      )
      and (
        summary.validation_status is null
        or summary.validation_status<>all(array[
          'VALIDATION_OK'::public.validation_status_enum,
          'OVERRIDDEN'::public.validation_status_enum
        ])
      ) blocked_by_hr_validation,
      precheck.precheck_status,
      exists(
        select 1
        from public.timesheet_evidence evidence
        left join public.invoice_document_assets asset
          on asset.id=evidence.document_asset_id
        where evidence.timesheet_id=financial.timesheet_id
          and evidence.processing_state not in('SUPERSEDED')
          and (asset.id is null or asset.status<>'READY')
      ) unready_evidence_asset,
      exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='TIMESHEET'
          and version.entity_id=financial.timesheet_id
          and version.purpose='TIMESHEET'
          and version.source_revision=timesheet.document_revision::text
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      ) timesheet_document_ready,
      timesheet.document_revision,
      timesheet.document_state,
      timesheet.current_document_version_id
    from group_sources source
    join public.timesheets_financials financial
      on financial.timesheet_id=source.timesheet_id
     and financial.is_current
    join public.timesheets timesheet
      on timesheet.timesheet_id=financial.timesheet_id
     and timesheet.is_current
    join timesheet_totals total
      on total.group_key=source.group_key
     and total.timesheet_id=source.timesheet_id
    join public.v_ts_invoice_precheck precheck
      on precheck.timesheet_id=financial.timesheet_id
    left join public.v_timesheets_summary_base summary
      on summary.timesheet_id=financial.timesheet_id
  ),
  active_exact as materialized (
    select distinct on(
      chunk.payload_json->>'group_key',
      chunk.payload_json->>'source_revision'
    )
      chunk.payload_json->>'group_key' group_key,
      operation.id operation_id,
      operation.status,
      chunk.payload_json->>'source_revision' source_revision,
      operation.progress_json,
      operation.error_json,
      operation.updated_at_utc
    from public.invoice_operation_chunks chunk
    join public.invoice_operations operation
      on operation.id=chunk.operation_id
    where chunk.chunk_type='GENERATION_GROUP'
      and chunk.status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
      )
      and operation.status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
      )
      and coalesce(
        chunk.payload_json->>'is_selection_expander',
        'false'
      )<>'true'
      and (not chunk.is_manifest_member or chunk.manifest_committed)
      and (
        not chunk.is_manifest_member
        or coalesce(chunk.entity_type,'')<>'OPERATION'
      )
    order by
      chunk.payload_json->>'group_key',
      chunk.payload_json->>'source_revision',
      operation.priority desc,
      operation.created_at_utc desc
  ),
  group_classification as materialized (
    select
      group_row.client_id,
      group_row.group_key,
      group_row.target_invoice_week,
      group_row.consolidation_mode,
      group_row.invoice_stream,
      group_row.source_revision_hash,
      group_row.canonical_source_ids,
      group_row.canonical_source_members,
      totals.total_ex_vat,
      totals.vat_amount,
      totals.total_hours,
      coalesce(jsonb_agg(
        distinct to_jsonb(display.candidate_id)
        order by to_jsonb(display.candidate_id)
      )
        filter(where display.candidate_id is not null),'[]'::jsonb)
        candidate_ids,
      coalesce(jsonb_agg(
        distinct to_jsonb(display.candidate_name)
        order by to_jsonb(display.candidate_name)
      )
        filter(where nullif(display.candidate_name,'') is not null),
        '[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(display.week_ending_date)
        order by to_jsonb(display.week_ending_date))
        filter(where display.week_ending_date is not null),
        '[]'::jsonb) week_ending_dates,
      max(display.week_ending_date) week_ending_date,
      case
        when group_row.blocker_code is not null
          then group_row.blocker_code
        when independent.blocker_code is not null
          then independent.blocker_code
        when coalesce(correction.valid,true) is not true
          then coalesce(
            correction.blocker_code,
            'INVOICE_CORRECTION_UNIT_INVALID'
          )
        when bool_or(display.blocked_by_hr_validation)
          then 'HR_VALIDATION_BLOCKED'
        when not coalesce(p_allow_early,false)
         and exists(
           select 1
           from jsonb_array_elements(
             group_row.canonical_source_members
           ) early(value)
           where case
             when pg_input_is_valid(
               coalesce(early.value->>'target_invoice_week',''),
               'date'
             )
               then (
                 early.value->>'target_invoice_week'
               )::date+6 >= (select today from anchor)
             else false
           end
         )
          then 'EARLY_GENERATION_NOT_ALLOWED'
      end blocker_code,
      case
        when group_row.blocker_code is not null
          then group_row.blocker_detail
        when independent.blocker_code is not null
          then independent.blocker_detail
        when coalesce(correction.valid,true) is not true
          then jsonb_build_object(
            'code',coalesce(
              correction.blocker_code,
              'INVOICE_CORRECTION_UNIT_INVALID'
            ),
            'correction_validation',correction.details
          )
        when bool_or(display.blocked_by_hr_validation)
          then jsonb_build_object(
            'code','HR_VALIDATION_BLOCKED',
            'sources',coalesce(
              jsonb_agg(display.timesheet_id order by display.timesheet_id)
                filter(where display.blocked_by_hr_validation),
              '[]'::jsonb
            )
          )
        when not coalesce(p_allow_early,false)
         and exists(
           select 1
           from jsonb_array_elements(
             group_row.canonical_source_members
           ) early(value)
           where case
             when pg_input_is_valid(
               coalesce(early.value->>'target_invoice_week',''),
               'date'
             )
               then (
                 early.value->>'target_invoice_week'
               )::date+6 >= (select today from anchor)
             else false
           end
         )
          then jsonb_build_object(
            'code','EARLY_GENERATION_NOT_ALLOWED'
          )
      end blocker_detail,
      active.operation_id active_operation_id,
      active.status active_status,
      active.progress_json active_progress,
      active.error_json active_error,
      coalesce(correction.details,'[]'::jsonb)
        correction_validation,
      jsonb_agg(jsonb_build_object(
        'timesheet_id',display.timesheet_id,
        'candidate_name',display.candidate_name,
        'week_ending_date',display.week_ending_date,
        'total_charge_ex_vat',display.total_charge_ex_vat,
        'total_hours',display.total_hours,
        'basis',display.basis::text,
        'submission_mode',coalesce(display.submission_mode::text,''),
        'validation_status',coalesce(display.validation_status::text,''),
        'hr_validation_required_for_invoice',
          display.hr_validation_required_for_invoice,
        'blocked_by_hr_validation',display.blocked_by_hr_validation,
        'precheck_status',coalesce(display.precheck_status::text,''),
        'document_revision',display.document_revision,
        'document_state',display.document_state,
        'current_document_version_id',
          display.current_document_version_id,
        'timesheet_document_ready',
          display.timesheet_document_ready,
        'unready_evidence_asset',display.unready_evidence_asset
      ) order by
        display.candidate_name nulls last,
        display.timesheet_id
      ) timesheet_projection
    from groups group_row
    join generation_totals totals
      on totals.group_key=group_row.group_key
    join source_display display
      on display.group_key=group_row.group_key
    left join independent_group_blockers independent
      on independent.group_key=group_row.group_key
    left join correction_group_results correction
      on correction.group_key=group_row.group_key
    left join active_exact active
      on active.group_key=group_row.group_key
     and active.source_revision=group_row.source_revision_hash
    group by
      group_row.client_id,
      group_row.group_key,
      group_row.target_invoice_week,
      group_row.consolidation_mode,
      group_row.invoice_stream,
      group_row.source_revision_hash,
      group_row.canonical_source_ids,
      group_row.canonical_source_members,
      group_row.blocker_code,
      group_row.blocker_detail,
      totals.total_ex_vat,
      totals.vat_amount,
      totals.total_hours,
      independent.blocker_code,
      independent.blocker_detail,
      correction.valid,
      correction.blocker_code,
      correction.details,
      active.operation_id,
      active.status,
      active.progress_json,
      active.error_json
  ),
  create_rows as materialized (
    select
      'generate:'||classification.group_key selection_key,
      jsonb_build_object(
        'selection_key','generate:'||classification.group_key,
        'row_kind','CREATE_INVOICE',
        'scope_key',classification.group_key,
        'invoice_id',null,
        'client_id',classification.client_id,
        'client_name',client.name,
        'candidate_ids',classification.candidate_ids,
        'candidate_names',classification.candidate_names,
        'candidate_display',case
          when jsonb_array_length(classification.candidate_names)=1
            then classification.candidate_names->>0
          when jsonb_array_length(classification.candidate_names)>1
            then 'Multiple candidates ('||
              jsonb_array_length(classification.candidate_names)::text||
              ')'
          else 'Unknown candidate'
        end,
        'week_ending_dates',classification.week_ending_dates,
        'week_ending_date',classification.week_ending_date,
        'currency','GBP',
        'invoice_stream',upper(coalesce(
          nullif(classification.invoice_stream,''),
          'NORMAL'
        )),
        'total_ex_vat',round(classification.total_ex_vat,2),
        'vat_amount',round(classification.vat_amount,2),
        'total_inc_vat',round(
          classification.total_ex_vat+classification.vat_amount,
          2
        ),
        'generation_state','NOT_GENERATED',
        'primary_blocker_code',classification.blocker_code,
        'action_blocker_codes',coalesce((
          select jsonb_agg(to_jsonb(code) order by first_order,code)
          from (
            select code,min(code_order) first_order
            from (
              select
                1 code_order,
                nullif(classification.blocker_code,'') code
              union all
              select
                2,
                nullif(classification.blocker_detail->>'code','')
              union all
              select
                100+source.ordinality::integer,
                nullif(source.value->>'code','')
              from jsonb_array_elements(
                case
                  when jsonb_typeof(
                    classification.blocker_detail->'sources'
                  )='array'
                    then classification.blocker_detail->'sources'
                  else '[]'::jsonb
                end
              ) with ordinality source(value,ordinality)
            ) raw_code
            where code is not null
              and code not in(
                'EARLY_GENERATION_NOT_ALLOWED',
                'SOURCE_ALREADY_INVOICED'
              )
            group by code
          ) code_row
        ),'[]'::jsonb),
        'informational_codes',case
          when classification.active_status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then jsonb_build_array('GENERATING')
          else '[]'::jsonb
        end,
        'is_active',classification.active_status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
        ),
        'active_operation_id',classification.active_operation_id,
        'active_operation_status',classification.active_status,
        'source_revision',classification.source_revision_hash,
        'document_revision',null,
        'command_payload',jsonb_build_object(
          'command_type','GENERATE_SELECTED',
          'canonical_source_ids',
            to_jsonb(classification.canonical_source_ids),
          'canonical_source_members',
            classification.canonical_source_members,
          'group_key',classification.group_key,
          'source_revision',classification.source_revision_hash,
          'target_invoice_week',classification.target_invoice_week,
          'consolidation_mode',classification.consolidation_mode,
          'invoice_stream',classification.invoice_stream,
          'correction_validation',
            classification.correction_validation,
          'allow_early',coalesce(p_allow_early,false)
        ),
        'is_early',coalesce(
          classification.week_ending_date
            >=(select today from anchor),
          false
        ),
        'selectable',
          jsonb_array_length(coalesce((
            select jsonb_agg(to_jsonb(code))
            from (
              select distinct code
              from (
                select nullif(classification.blocker_code,'') code
                union all
                select nullif(
                  classification.blocker_detail->>'code',
                  ''
                )
              ) code_source
              where code is not null
                and code not in(
                  'EARLY_GENERATION_NOT_ALLOWED',
                  'SOURCE_ALREADY_INVOICED'
                )
            ) blocking
          ),'[]'::jsonb))=0
          and classification.active_status is null,
        'row_status',case
          when classification.blocker_code is not null
           and classification.blocker_code not in(
             'EARLY_GENERATION_NOT_ALLOWED',
             'SOURCE_ALREADY_INVOICED'
           )
            then 'BLOCKED'
          when classification.active_status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then 'IN_PROGRESS'
          else 'READY'
        end,
        '_private',jsonb_build_object(
          'canonical_source_ids',
            to_jsonb(classification.canonical_source_ids),
          'canonical_source_members',
            classification.canonical_source_members,
          'target_invoice_week',classification.target_invoice_week,
          'consolidation_mode',classification.consolidation_mode,
          'correction_validation',
            classification.correction_validation,
          'blocker_detail',classification.blocker_detail,
          'total_hours',classification.total_hours,
          'timesheets',classification.timesheet_projection,
          'active_progress',classification.active_progress,
          'active_error',classification.active_error
        )
      ) candidate_json
    from group_classification classification
    join public.clients client on client.id=classification.client_id
    where coalesce(classification.blocker_code,'')
      not in('SOURCE_ALREADY_INVOICED','EARLY_GENERATION_NOT_ALLOWED')
  ),
  stale_invoice_timesheets as materialized (
    select
      line.invoice_id,
      coalesce(jsonb_agg(
        distinct to_jsonb(summary.candidate_id)
        order by to_jsonb(summary.candidate_id)
      )
        filter(where summary.candidate_id is not null),'[]'::jsonb)
        candidate_ids,
      coalesce(jsonb_agg(
        distinct to_jsonb(summary.candidate_name)
        order by to_jsonb(summary.candidate_name)
      )
        filter(where nullif(summary.candidate_name,'') is not null),
        '[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(
        distinct to_jsonb(summary.week_ending_date)
        order by to_jsonb(summary.week_ending_date)
      )
        filter(where summary.week_ending_date is not null),
        '[]'::jsonb) week_ending_dates,
      min(summary.week_ending_date) min_week_ending,
      max(summary.week_ending_date) max_week_ending
    from public.invoice_lines line
    left join public.v_timesheets_summary_base summary
      on summary.timesheet_id=line.timesheet_id
    group by line.invoice_id
  ),
  stale_rows as materialized (
    select
      'invoice:'||invoice.id::text selection_key,
      jsonb_build_object(
        'selection_key','invoice:'||invoice.id::text,
        'row_kind',case
          when coalesce(invoice.document_state,'')='FAILED'
            then 'RETRY_GENERATION'
          else 'REGENERATE_DRAFT'
        end,
        'scope_key',invoice.id::text,
        'invoice_id',invoice.id,
        'client_id',invoice.client_id,
        'client_name',client.name,
        'candidate_ids',coalesce(
          timesheet.candidate_ids,
          '[]'::jsonb
        ),
        'candidate_names',coalesce(
          timesheet.candidate_names,
          '[]'::jsonb
        ),
        'candidate_display',case
          when jsonb_array_length(coalesce(
            timesheet.candidate_names,
            '[]'::jsonb
          ))=1
            then timesheet.candidate_names->>0
          when jsonb_array_length(coalesce(
            timesheet.candidate_names,
            '[]'::jsonb
          ))>1
            then 'Multiple candidates ('||
              jsonb_array_length(timesheet.candidate_names)::text||
              ')'
          else 'Unknown candidate'
        end,
        'week_ending_dates',coalesce(
          timesheet.week_ending_dates,
          '[]'::jsonb
        ),
        'week_ending_date',coalesce(
          timesheet.min_week_ending,
          case
            when pg_input_is_valid(
              invoice.header_snapshot_json#>>'{meta,invoice_week_start}',
              'date'
            )
              then (
                invoice.header_snapshot_json#>>
                  '{meta,invoice_week_start}'
              )::date+6
          end
        ),
        'currency',coalesce(
          nullif(
            invoice.header_snapshot_json#>>'{meta,currency}',
            ''
          ),
          nullif(invoice.header_snapshot_json->>'currency',''),
          'GBP'
        ),
        'invoice_stream',upper(coalesce(
          nullif(
            invoice.header_snapshot_json#>>'{meta,invoice_stream}',
            ''
          ),
          nullif(invoice.header_snapshot_json->>'invoice_stream',''),
          case
            when lower(coalesce(
              invoice.header_snapshot_json#>>'{meta,self_bill}',
              invoice.header_snapshot_json->>'self_bill',
              'false'
            )) in('true','t','1','yes')
              then 'SELF_BILL'
          end,
          'NORMAL'
        )),
        'total_ex_vat',round(coalesce(invoice.subtotal_ex_vat,0),2),
        'vat_amount',round(coalesce(invoice.vat_amount,0),2),
        'total_inc_vat',round(coalesce(
          invoice.total_inc_vat,
          coalesce(invoice.subtotal_ex_vat,0)
            +coalesce(invoice.vat_amount,0)
        ),2),
        'generation_state',case
          when coalesce(invoice.document_state,'')='FAILED'
            then 'FAILED'
          when exists(
            select 1
            from public.invoice_document_versions version
            where version.entity_type='INVOICE'
              and version.entity_id=invoice.id
              and version.purpose='DRAFT_PREVIEW'
          )
            then 'STALE'
          else 'NOT_GENERATED'
        end,
        'primary_blocker_code',null,
        'action_blocker_codes','[]'::jsonb,
        'informational_codes',case
          when active.status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then jsonb_build_array('GENERATING')
          when exists(
            select 1
            from public.invoice_document_versions version
            where version.entity_type='INVOICE'
              and version.entity_id=invoice.id
              and version.purpose='DRAFT_PREVIEW'
          )
            then jsonb_build_array('STALE')
          else jsonb_build_array('NOT_GENERATED')
        end,
        'is_active',active.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
        ),
        'active_operation_id',active.operation_id,
        'active_operation_status',active.status,
        'source_revision',invoice.document_revision::text,
        'document_revision',invoice.document_revision::text,
        'command_payload',jsonb_build_object(
          'command_type','VIEW_INVOICE_DOCUMENT',
          'invoice_id',invoice.id,
          'purpose','DRAFT_PREVIEW',
          'expected_revision',invoice.document_revision,
          'source_revision',invoice.document_revision::text
        ),
        'is_early',coalesce(
          timesheet.max_week_ending,
          case
            when pg_input_is_valid(
              invoice.header_snapshot_json#>>
                '{meta,invoice_week_start}',
              'date'
            )
              then (
                invoice.header_snapshot_json#>>
                  '{meta,invoice_week_start}'
              )::date+6
          end
        ) >= (select today from anchor),
        'selectable',active.operation_id is null,
        'row_status',case
          when active.status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then 'IN_PROGRESS'
          when coalesce(invoice.document_state,'')='FAILED'
            then 'FAILED'
          when exists(
            select 1
            from public.invoice_document_versions version
            where version.entity_type='INVOICE'
              and version.entity_id=invoice.id
              and version.purpose='DRAFT_PREVIEW'
          )
            then 'STALE'
          else 'READY'
        end,
        '_private','{}'::jsonb
      ) candidate_json
    from public.invoices invoice
    join public.clients client on client.id=invoice.client_id
    left join stale_invoice_timesheets timesheet
      on timesheet.invoice_id=invoice.id
    left join lateral (
      select
        operation.id operation_id,
        operation.status
      from public.invoice_operations operation
      where operation.operation_type='BUILD_DOCUMENT'
        and operation.entity_type='INVOICE'
        and operation.entity_id=invoice.id
        and operation.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
        )
      order by
        operation.created_at_utc desc,
        operation.id desc
      limit 1
    ) active on true
    where invoice.type::text='INVOICE'
      and invoice.status::text in('DRAFT','ON_HOLD')
      and coalesce(invoice.document_revision,0)>0
      and (
        v_scope_keys is null
        or invoice.id::text=any(v_scope_keys)
      )
      and not exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='INVOICE'
          and version.entity_id=invoice.id
          and version.purpose='DRAFT_PREVIEW'
          and version.source_revision=invoice.document_revision::text
          and version.template_version='invoice-professional-v2'
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      )
  )
  select create_row.selection_key,create_row.candidate_json
  from create_rows create_row
  union all
  select stale_row.selection_key,stale_row.candidate_json
  from stale_rows stale_row
  order by selection_key;
end;
$function$;

-- private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_generate_group_rows_v2(p_allow_early boolean DEFAULT false, p_limit integer DEFAULT NULL::integer, p_scope_keys text[] DEFAULT NULL::text[], p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(client_id uuid, client_name text, group_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with classified as materialized (
  select
    candidate.selection_key,
    candidate.candidate_json
  from private._invoice_batch_generate_classification_v2(
    p_allow_early,
    p_scope_keys,
    p_now_utc
  ) candidate
  where candidate.candidate_json->>'row_kind'='CREATE_INVOICE'
  order by
    candidate.candidate_json->>'week_ending_date',
    candidate.candidate_json->>'client_id',
    candidate.candidate_json->>'scope_key'
  limit case
    when p_scope_keys is not null then 250
    when p_limit is null then null
    else greatest(1,p_limit)
  end
)
select
  (classified.candidate_json->>'client_id')::uuid client_id,
  classified.candidate_json->>'client_name' client_name,
  jsonb_build_object(
    'group_key',classified.candidate_json->>'scope_key',
    'invoice_week_start',
      classified.candidate_json#>>'{_private,target_invoice_week}',
    'week_ending_date',
      classified.candidate_json->>'week_ending_date',
    'subtotal_ex_vat',
      (classified.candidate_json->>'total_ex_vat')::numeric,
    'vat_amount',
      (classified.candidate_json->>'vat_amount')::numeric,
    'total_inc_vat',
      (classified.candidate_json->>'total_inc_vat')::numeric,
    'total_hours',coalesce(
      (classified.candidate_json#>>'{_private,total_hours}')::numeric,
      0
    ),
    'stream',classified.candidate_json->>'invoice_stream',
    'consolidation_mode',
      classified.candidate_json#>>'{_private,consolidation_mode}',
    'canonical_source_ids',
      classified.candidate_json#>'{_private,canonical_source_ids}',
    'canonical_source_members',
      classified.candidate_json#>'{_private,canonical_source_members}',
    'canonical_source_revision',
      classified.candidate_json->>'source_revision',
    'blocker_code',
      classified.candidate_json->>'primary_blocker_code',
    'blocker_codes',
      classified.candidate_json->'action_blocker_codes',
    'blocker_detail',
      classified.candidate_json#>'{_private,blocker_detail}',
    'correction_validation',
      classified.candidate_json#>'{_private,correction_validation}',
    'document_dependencies',coalesce((
      select jsonb_agg(jsonb_build_object(
        'timesheet_id',timesheet.value->>'timesheet_id',
        'code','TIMESHEET_DOCUMENT_NOT_READY'
      ) order by timesheet.value->>'timesheet_id')
      from jsonb_array_elements(
        coalesce(
          classified.candidate_json#>'{_private,timesheets}',
          '[]'::jsonb
        )
      ) timesheet(value)
      where coalesce(
        (timesheet.value->>'timesheet_document_ready')::boolean,
        false
      ) is false
    ),'[]'::jsonb),
    'command_payload',
      classified.candidate_json->'command_payload',
    'timesheets',coalesce(
      classified.candidate_json#>'{_private,timesheets}',
      '[]'::jsonb
    ),
    'active_generation_operation_id',
      classified.candidate_json->>'active_operation_id',
    'active_generation_status',
      classified.candidate_json->>'active_operation_status',
    'active_generation_progress',
      classified.candidate_json#>'{_private,active_progress}',
    'last_generation_error',
      classified.candidate_json#>'{_private,active_error}',
    'retry_available',
      classified.candidate_json->>'active_operation_status'
        in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
  ) group_json
from classified;
$function$;

-- private._invoice_batch_hash_v2(jsonb)
CREATE OR REPLACE FUNCTION private._invoice_batch_hash_v2(p_value jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
  select encode(
    extensions.digest(
      convert_to(private._invoice_batch_canonical_text_v2(p_value), 'UTF8'),
      'sha256'
    ),
    'hex'
  );
$function$;

-- private._invoice_batch_issue_candidate_key_rows_v2(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_candidate_key_rows_v2(p_query jsonb, p_now_utc timestamp with time zone)
 RETURNS TABLE(selection_key text, invoice_id uuid, invoice_number text, source_revision text, client_id uuid, client_name text, candidate_ids jsonb, candidate_display text, week_ending_date date, currency text, invoice_stream text, total_ex_vat numeric, vat_amount numeric, total_inc_vat numeric, row_status_seed text, blocker_codes_seed jsonb, is_early boolean, sort_date_key date, sort_text_key text, sort_numeric_key numeric, page_ordinal bigint, full_scope_count bigint, candidate_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with
query_contract as materialized (
  select private._invoice_batch_query_validate_v2(
    p_query,
    'ISSUE'
  ) query_json
),
params as materialized (
  select
    upper(coalesce(query_json->>'mode','PAGE')) mode,
    (query_json->>'page_size')::integer page_size,
    coalesce(
      (query_json#>>'{filters,allow_early}')::boolean,
      false
    ) allow_early,
    upper(coalesce(
      nullif(query_json#>>'{filters,display_mode}',''),
      'ALL'
    )) display_mode,
    lower(nullif(btrim(coalesce(
      query_json#>>'{filters,search}',
      ''
    )),'')) search_text,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{filters,week_ending_from}',''),
        'date'
      )
        then (query_json#>>'{filters,week_ending_from}')::date
    end week_ending_from,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{filters,week_ending_to}',''),
        'date'
      )
        then (query_json#>>'{filters,week_ending_to}')::date
    end week_ending_to,
    coalesce(query_json#>'{filters,client_ids}','[]'::jsonb)
      client_ids,
    coalesce(query_json#>'{filters,candidate_ids}','[]'::jsonb)
      candidate_ids,
    coalesce(query_json#>'{filters,week_endings}','[]'::jsonb)
      week_endings,
    coalesce(query_json#>'{filters,status_codes}','[]'::jsonb)
      status_codes,
    coalesce(query_json#>'{filters,blocker_codes}','[]'::jsonb)
      blocker_codes,
    coalesce(query_json#>'{filters,invoice_streams}','[]'::jsonb)
      invoice_streams,
    coalesce(query_json->'selection_keys','[]'::jsonb)
      selection_keys,
    upper(coalesce(
      nullif(query_json#>>'{sort,sort_key}',''),
      'WEEK_ENDING_DATE'
    )) sort_key,
    case
      when upper(coalesce(
        query_json#>>'{sort,sort_direction}',
        'ASC'
      ))='DESC'
        then 'DESC'
      else 'ASC'
    end sort_direction,
    nullif(query_json#>>'{cursor,after_selection_key}','')
      after_selection_key,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{cursor,after_sort_date}',''),
        'date'
      )
        then (query_json#>>'{cursor,after_sort_date}')::date
    end after_sort_date,
    nullif(query_json#>>'{cursor,after_sort_text}','')
      after_sort_text,
    case
      when coalesce(
        query_json#>>'{cursor,after_sort_numeric}',
        ''
      )~'^[+-]?[0-9]+([.][0-9]+)?$'
        then (
          query_json#>>'{cursor,after_sort_numeric}'
        )::numeric
    end after_sort_numeric
  from query_contract
),
classified as materialized (
  select
    candidate.candidate_json,
    candidate.candidate_json->>'selection_key' selection_key,
    (candidate.candidate_json->>'invoice_id')::uuid invoice_id,
    candidate.candidate_json->>'invoice_number' invoice_number,
    candidate.candidate_json->>'source_revision' source_revision,
    (candidate.candidate_json->>'client_id')::uuid client_id,
    candidate.candidate_json->>'client_name' client_name,
    coalesce(
      candidate.candidate_json->'candidate_ids',
      '[]'::jsonb
    ) candidate_ids,
    candidate.candidate_json->>'candidate_display' candidate_display,
    case
      when pg_input_is_valid(
        coalesce(candidate.candidate_json->>'week_ending_date',''),
        'date'
      )
        then (candidate.candidate_json->>'week_ending_date')::date
    end week_ending_date,
    coalesce(
      nullif(candidate.candidate_json->>'currency',''),
      'GBP'
    ) currency,
    upper(coalesce(
      nullif(candidate.candidate_json->>'invoice_stream',''),
      'NORMAL'
    )) invoice_stream,
    coalesce(
      (candidate.candidate_json->>'total_ex_vat')::numeric,
      0
    ) total_ex_vat,
    coalesce(
      (candidate.candidate_json->>'vat_amount')::numeric,
      0
    ) vat_amount,
    coalesce(
      (candidate.candidate_json->>'total_inc_vat')::numeric,
      0
    ) total_inc_vat,
    candidate.candidate_json->>'row_status' row_status,
    coalesce(
      candidate.candidate_json->'issue_blocker_codes',
      '[]'::jsonb
    ) issue_blocker_codes,
    coalesce(
      candidate.candidate_json->'delivery_blocker_codes',
      '[]'::jsonb
    ) delivery_blocker_codes,
    coalesce(
      candidate.candidate_json->'informational_codes',
      '[]'::jsonb
    ) informational_codes,
    coalesce(
      (candidate.candidate_json->>'blocked_for_sending')::boolean,
      false
    ) blocked_for_sending,
    coalesce(
      (candidate.candidate_json->>'selectable')::boolean,
      false
    ) selectable,
    coalesce(
      (candidate.candidate_json->>'is_early')::boolean,
      false
    ) is_early
  from private._invoice_batch_issue_classification_v2(
    true,
    null,
    coalesce(p_now_utc,statement_timestamp())
  ) candidate
),
selection_rules as materialized (
  select rule.*
  from query_contract
  cross join lateral private._invoice_batch_selection_rules_v2(
    coalesce(
      query_contract.query_json->'selection',
      jsonb_build_object(
        'contract_version','INVOICE_BATCH_SELECTION_V2',
        'mode','IMPLICIT_ALL',
        'default_selected',true,
        'rules','[]'::jsonb
      )
    )
  ) rule
),
classified_with_selection as materialized (
  select
    classified.*,
    coalesce((
      select rule.action
      from selection_rules rule
      where (rule.selector_type='ROW'
          and rule.selection_key=classified.selection_key)
         or (rule.selector_type='WEEK'
          and rule.week_ending_date=classified.week_ending_date)
         or (rule.selector_type='CLIENT'
          and rule.client_id=classified.client_id)
         or (rule.selector_type='CANDIDATE' and exists(
           select 1
           from jsonb_array_elements_text(
             classified.candidate_ids
           ) candidate(value)
           where pg_input_is_valid(candidate.value,'uuid')
             and candidate.value::uuid=rule.candidate_id
         ))
         or (rule.selector_type='STATUS'
          and rule.status_code=classified.row_status)
         or (rule.selector_type='WEEK_CLIENT'
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id)
         or (rule.selector_type='WEEK_CLIENT_CANDIDATE'
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id
          and exists(
            select 1
            from jsonb_array_elements_text(
              classified.candidate_ids
            ) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=rule.candidate_id
          ))
         or (rule.selector_type='STATUS_WEEK'
          and rule.status_code=classified.row_status
          and rule.week_ending_date=classified.week_ending_date)
         or (rule.selector_type='STATUS_WEEK_CLIENT'
          and rule.status_code=classified.row_status
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id)
         or (rule.selector_type='DIMENSION_GROUP'
          and (rule.week_ending_date is null or rule.week_ending_date=classified.week_ending_date)
          and (rule.client_id is null or rule.client_id=classified.client_id)
          and (rule.status_code is null or rule.status_code=classified.row_status)
          and (rule.candidate_id is null or exists (
            select 1 from jsonb_array_elements_text(classified.candidate_ids) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=rule.candidate_id
          )))
      order by rule.rule_sequence desc
      limit 1
    ),'INCLUDE') last_selection_action
  from classified
),
filtered as materialized (
  select classified.*
  from classified_with_selection classified
  cross join params
  where (params.allow_early or not classified.is_early)
    and (
      jsonb_array_length(params.client_ids)=0
      or classified.client_id::text in (
        select value
        from jsonb_array_elements_text(params.client_ids) value
      )
    )
    and (
      jsonb_array_length(params.candidate_ids)=0
      or exists(
        select 1
        from jsonb_array_elements_text(
          classified.candidate_ids
        ) candidate(value)
        where candidate.value in (
          select value
          from jsonb_array_elements_text(params.candidate_ids) value
        )
      )
    )
    and (
      jsonb_array_length(params.week_endings)=0
      or classified.week_ending_date::text in (
        select value
        from jsonb_array_elements_text(params.week_endings) value
      )
    )
    and (
      jsonb_array_length(params.status_codes)=0
      or classified.row_status in (
        select upper(value)
        from jsonb_array_elements_text(params.status_codes) value
      )
    )
    and (
      jsonb_array_length(params.blocker_codes)=0
      or exists(
        select 1
        from jsonb_array_elements_text(
          classified.issue_blocker_codes
          ||classified.delivery_blocker_codes
          ||classified.informational_codes
        ) code(value)
        where code.value in (
          select upper(value)
          from jsonb_array_elements_text(params.blocker_codes) value
        )
      )
    )
    and (
      jsonb_array_length(params.invoice_streams)=0
      or classified.invoice_stream in (
        select upper(value)
        from jsonb_array_elements_text(params.invoice_streams) value
      )
    )
    and (
      params.mode<>'EXPLICIT_KEYS'
      or classified.selection_key in (
        select value
        from jsonb_array_elements_text(params.selection_keys) value
      )
    )
    and (
      params.search_text is null
      or lower(
        coalesce(classified.client_name,'')||' '||
        coalesce(classified.candidate_display,'')||' '||
        coalesce(classified.invoice_number,'')
      ) like '%'||params.search_text||'%'
    )
    and (
      params.week_ending_from is null
      or classified.week_ending_date>=params.week_ending_from
    )
    and (
      params.week_ending_to is null
      or classified.week_ending_date<=params.week_ending_to
    )
    and (
      params.mode='EXPAND_SELECTION'
      or params.display_mode='ALL'
      or (
        params.display_mode='READY'
        and classified.selectable
      )
      or (
        params.display_mode='BLOCKED'
        and (
          classified.row_status in(
            'BLOCKED','STALE','FAILED'
          )
          or classified.blocked_for_sending
        )
      )
    )
    and (
      params.mode<>'EXPAND_SELECTION'
      or (
        classified.selectable
        and classified.last_selection_action<>'EXCLUDE'
      )
    )
),
sortable as materialized (
  select
    filtered.*,
    case
      when params.sort_key='WEEK_ENDING_DATE'
        then coalesce(
          filtered.week_ending_date,
          case
            when params.sort_direction='DESC'
              then date '0001-01-01'
            else date '9999-12-31'
          end
        )
    end sort_date_key,
    case
      when params.sort_key='CLIENT_NAME'
        then lower(coalesce(filtered.client_name,''))
      when params.sort_key='CANDIDATE_NAME'
        then lower(coalesce(filtered.candidate_display,''))
      when params.sort_key='INVOICE_NUMBER'
        then lower(coalesce(filtered.invoice_number,''))
      when params.sort_key='STATUS'
        then lpad((case
          when filtered.row_status='READY'
           and not filtered.blocked_for_sending then 10
          when filtered.row_status='READY'
           and filtered.blocked_for_sending then 20
          when filtered.row_status='STALE' then 30
          when filtered.row_status='IN_PROGRESS' then 40
          when filtered.row_status='FAILED' then 50
          else 60
        end)::text,3,'0')||'|'||lower(filtered.row_status)
    end sort_text_key,
    case
      when params.sort_key='TOTAL_EX_VAT'
        then filtered.total_ex_vat
      when params.sort_key='TOTAL_INC_VAT'
        then filtered.total_inc_vat
    end sort_numeric_key
  from filtered
  cross join params
),
scope_count as materialized (
  select count(*)::bigint full_scope_count
  from sortable
),
cursor_filtered as materialized (
  select sortable.*
  from sortable
  cross join params
  where params.after_selection_key is null
     or (
       params.mode='EXPAND_SELECTION'
       and sortable.selection_key>params.after_selection_key
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key='WEEK_ENDING_DATE'
       and params.after_sort_date is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_date_key>params.after_sort_date
             or (
               sortable.sort_date_key=params.after_sort_date
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_date_key<params.after_sort_date
             or (
               sortable.sort_date_key=params.after_sort_date
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key in(
         'CLIENT_NAME','CANDIDATE_NAME','STATUS',
         'INVOICE_NUMBER'
       )
       and params.after_sort_text is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_text_key>params.after_sort_text
             or (
               sortable.sort_text_key=params.after_sort_text
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_text_key<params.after_sort_text
             or (
               sortable.sort_text_key=params.after_sort_text
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key in('TOTAL_EX_VAT','TOTAL_INC_VAT')
       and params.after_sort_numeric is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_numeric_key>params.after_sort_numeric
             or (
               sortable.sort_numeric_key=params.after_sort_numeric
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_numeric_key<params.after_sort_numeric
             or (
               sortable.sort_numeric_key=params.after_sort_numeric
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
),
ordered as materialized (
  select
    cursor_filtered.*,
    row_number() over(order by
      case
        when params.mode='EXPAND_SELECTION'
          then cursor_filtered.selection_key
      end asc,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key='WEEK_ENDING_DATE'
         and params.sort_direction='ASC'
          then cursor_filtered.sort_date_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key='WEEK_ENDING_DATE'
         and params.sort_direction='DESC'
          then cursor_filtered.sort_date_key
      end desc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'CLIENT_NAME','CANDIDATE_NAME','STATUS',
           'INVOICE_NUMBER'
         )
         and params.sort_direction='ASC'
          then cursor_filtered.sort_text_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'CLIENT_NAME','CANDIDATE_NAME','STATUS',
           'INVOICE_NUMBER'
         )
         and params.sort_direction='DESC'
          then cursor_filtered.sort_text_key
      end desc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'TOTAL_EX_VAT','TOTAL_INC_VAT'
         )
         and params.sort_direction='ASC'
          then cursor_filtered.sort_numeric_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'TOTAL_EX_VAT','TOTAL_INC_VAT'
         )
         and params.sort_direction='DESC'
          then cursor_filtered.sort_numeric_key
      end desc nulls last,
      cursor_filtered.selection_key
    ) page_ordinal
  from cursor_filtered
  cross join params
)
select
  ordered.selection_key,
  ordered.invoice_id,
  ordered.invoice_number,
  ordered.source_revision,
  ordered.client_id,
  ordered.client_name,
  ordered.candidate_ids,
  ordered.candidate_display,
  ordered.week_ending_date,
  ordered.currency,
  ordered.invoice_stream,
  ordered.total_ex_vat,
  ordered.vat_amount,
  ordered.total_inc_vat,
  ordered.row_status row_status_seed,
  ordered.issue_blocker_codes blocker_codes_seed,
  ordered.is_early,
  ordered.sort_date_key,
  ordered.sort_text_key,
  ordered.sort_numeric_key,
  ordered.page_ordinal,
  scope_count.full_scope_count,
  ordered.candidate_json
from ordered
cross join params
cross join scope_count
where ordered.page_ordinal<=case
  when params.mode='EXPLICIT_KEYS'
    then jsonb_array_length(params.selection_keys)
  else params.page_size+1
end
order by ordered.page_ordinal;
$function$;

-- private._invoice_batch_issue_candidate_keys_v2(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_candidate_keys_v2(p_query jsonb, p_now_utc timestamp with time zone)
 RETURNS TABLE(selection_key text, invoice_id uuid, invoice_number text, source_revision text, client_id uuid, client_name text, candidate_ids jsonb, candidate_display text, week_ending_date date, currency text, invoice_stream text, total_ex_vat numeric, vat_amount numeric, total_inc_vat numeric, row_status_seed text, blocker_codes_seed jsonb, is_early boolean, sort_date_key date, sort_text_key text, sort_numeric_key numeric, page_ordinal bigint, full_scope_count bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
select
  candidate.selection_key,
  candidate.invoice_id,
  candidate.invoice_number,
  candidate.source_revision,
  candidate.client_id,
  candidate.client_name,
  candidate.candidate_ids,
  candidate.candidate_display,
  candidate.week_ending_date,
  candidate.currency,
  candidate.invoice_stream,
  candidate.total_ex_vat,
  candidate.vat_amount,
  candidate.total_inc_vat,
  candidate.row_status_seed,
  candidate.blocker_codes_seed,
  candidate.is_early,
  candidate.sort_date_key,
  candidate.sort_text_key,
  candidate.sort_numeric_key,
  candidate.page_ordinal,
  candidate.full_scope_count
from private._invoice_batch_issue_candidate_key_rows_v2(
  p_query,
  p_now_utc
) candidate
order by candidate.page_ordinal
$function$;

-- private._invoice_batch_issue_candidate_rows_v1(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_candidate_rows_v1(p_query jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_selection jsonb;
  v_page_size integer;
  v_after_key text;
  v_allow_early boolean;
  v_display_mode text;
  v_legacy jsonb;
  v_result jsonb;
begin
  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if coalesce(v_query->>'contract_version','INVOICE_BATCH_QUERY_V1') <> 'INVOICE_BATCH_QUERY_V1' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  v_mode := upper(coalesce(nullif(v_query->>'mode',''),'PAGE'));
  if v_mode not in ('PAGE','FACETS','EXPAND_SELECTION','EXPLICIT_KEYS') then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_filters := case when jsonb_typeof(v_query->'filters') = 'object' then v_query->'filters' else '{}'::jsonb end;
  v_sort := case when jsonb_typeof(v_query->'sort') = 'object' then v_query->'sort' else '{}'::jsonb end;
  v_selection := case when jsonb_typeof(v_query->'selection') = 'object' then v_query->'selection' else jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V1',
    'mode','IMPLICIT_ALL',
    'default_selected',true,
    'rules','[]'::jsonb
  ) end;

  perform 1 from private._invoice_batch_selection_rules_v1(v_selection) limit 1;

  v_allow_early := lower(coalesce(v_query->>'allow_early',v_filters->>'allow_early','false')) in ('true','t','1','yes','on');
  v_display_mode := upper(coalesce(nullif(v_query->>'display_mode',''),nullif(v_filters->>'display_mode',''),'ALL'));
  if v_display_mode not in ('ALL','READY','BLOCKED') then
    raise exception using errcode='22023', message='INVOICE_BATCH_DISPLAY_MODE_INVALID';
  end if;

  if upper(coalesce(v_sort->>'group_preset','WEEK_CLIENT_CANDIDATE')) not in (
    'WEEK_CLIENT_CANDIDATE','CLIENT_WEEK_CANDIDATE','CANDIDATE_WEEK_CLIENT','STATUS_WEEK_CLIENT'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_GROUP_PRESET_INVALID';
  end if;

  if upper(coalesce(v_sort->>'sort_key','WEEK_ENDING_DATE')) not in (
    'WEEK_ENDING_DATE','CLIENT_NAME','CANDIDATE_NAME','TOTAL_EX_VAT','TOTAL_INC_VAT','STATUS','INVOICE_NUMBER'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SORT_KEY_INVALID';
  end if;

  v_page_size := case
    when coalesce(v_query->>'page_size','') ~ '^[1-9][0-9]{0,8}$'
      then greatest(1,least((v_query->>'page_size')::integer,100))
    else 100
  end;
  v_after_key := nullif(coalesce(v_query#>>'{cursor,after_selection_key}',v_query#>>'{cursor,last_selection_key}',v_query->>'after_selection_key'), '');

  -- Always ask the legacy authority for the wider set; this helper applies the
  -- locked Batch early visibility rule itself so early rows are invisible when
  -- allow_early=false instead of being shown as blocked.
  v_legacy := public.invoice_batch_issue_candidates(true, 20000);

  with
  params as materialized (
    select
      v_mode mode,
      v_allow_early allow_early,
      v_display_mode display_mode,
      v_page_size page_size,
      v_after_key after_key,
      lower(nullif(btrim(coalesce(v_filters->>'search','')),'')) search_text,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_from',''),'date') then (v_filters->>'week_ending_from')::date end week_ending_from,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_to',''),'date') then (v_filters->>'week_ending_to')::date end week_ending_to,
      (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date today,
      case when jsonb_typeof(v_filters->'client_ids')='array' then v_filters->'client_ids' else '[]'::jsonb end client_ids,
      case when jsonb_typeof(v_filters->'candidate_ids')='array' then v_filters->'candidate_ids' else '[]'::jsonb end candidate_ids,
      case when jsonb_typeof(v_filters->'week_endings')='array' then v_filters->'week_endings' else '[]'::jsonb end week_endings,
      case when jsonb_typeof(v_filters->'status_codes')='array' then v_filters->'status_codes' else '[]'::jsonb end status_codes,
      case when jsonb_typeof(v_filters->'blocker_codes')='array' then v_filters->'blocker_codes' else '[]'::jsonb end blocker_codes,
      coalesce(nullif(upper(v_sort->>'group_preset'),''),'WEEK_CLIENT_CANDIDATE') group_preset,
      coalesce(nullif(upper(v_sort->>'sort_key'),''),'WEEK_ENDING_DATE') sort_key,
      case when upper(coalesce(v_sort->>'sort_direction','ASC'))='DESC' then 'DESC' else 'ASC' end sort_direction,
      case when pg_input_is_valid(
        nullif(coalesce(v_query#>>'{cursor,after_sort_date}',v_query#>>'{cursor,last_sort_date}',''),''),
        'date'
      )
        then coalesce(v_query#>>'{cursor,after_sort_date}',v_query#>>'{cursor,last_sort_date}')::date end after_sort_date,
      nullif(coalesce(v_query#>>'{cursor,after_sort_text}',v_query#>>'{cursor,last_sort_text}',''),'') after_sort_text,
      case when coalesce(v_query#>>'{cursor,after_sort_numeric}',v_query#>>'{cursor,last_sort_numeric}','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then coalesce(v_query#>>'{cursor,after_sort_numeric}',v_query#>>'{cursor,last_sort_numeric}')::numeric end after_sort_numeric
  ),
  selection_rules as materialized (
    select * from private._invoice_batch_selection_rules_v1(v_selection)
  ),
  legacy_clients as materialized (
    select client.value client_json
    from jsonb_array_elements(case when jsonb_typeof(v_legacy)='array' then v_legacy else '[]'::jsonb end) client(value)
  ),
  legacy_weeks as materialized (
    select client_json,
      client_json->>'client_id' client_id_text,
      client_json->>'client_name' client_name,
      week.value week_json
    from legacy_clients
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(client_json->'weeks')='array' then client_json->'weeks' else '[]'::jsonb end
    ) week(value)
  ),
  invoice_rows_raw as materialized (
    select
      'invoice:'||coalesce(inv.value->>'invoice_id','') selection_key,
      case when pg_input_is_valid(inv.value->>'invoice_id','uuid') then (inv.value->>'invoice_id')::uuid end invoice_id,
      inv.value->>'invoice_no' invoice_number,
      case when pg_input_is_valid(lw.client_id_text,'uuid') then lw.client_id_text::uuid end client_id,
      lw.client_name,
      case when pg_input_is_valid(inv.value->>'document_revision','int8') then (inv.value->>'document_revision')::bigint end document_revision,
      case when pg_input_is_valid(lw.week_json->>'week_ending_date','date') then (lw.week_json->>'week_ending_date')::date end week_ending_date,
      case when pg_input_is_valid(lw.week_json->>'week_ending_date','date')
        then (lw.week_json->>'week_ending_date')::date >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date
        else false end is_early,
      coalesce(nullif(inv.value->>'currency',''),'GBP') currency,
      case when coalesce(inv.value->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'subtotal_ex_vat')::numeric,2) else 0 end total_ex_vat,
      case when coalesce(inv.value->>'vat_amount','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'vat_amount')::numeric,2) else 0 end vat_amount,
      case when coalesce(inv.value->>'total_inc_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'total_inc_vat')::numeric,2) else 0 end total_inc_vat,
      upper(coalesce(inv.value->>'preview_document_state','')) preview_document_state,
      upper(coalesce(inv.value->>'status','')) invoice_status,
      coalesce(inv.value->'stable_blocker_codes','[]'::jsonb) hard_blocker_codes,
      coalesce(inv.value->'document_dependency_codes','[]'::jsonb) document_dependency_codes,
      coalesce(inv.value->'delivery_blocker_codes','[]'::jsonb) delivery_blocker_codes,
      coalesce(inv.value->'recipient_routing_warnings','[]'::jsonb) warning_codes,
      coalesce(inv.value->>'can_issue_only','false') in ('true','t','1','yes','on') can_issue_only,
      coalesce(inv.value->>'can_issue_and_deliver','false') in ('true','t','1','yes','on') can_issue_and_deliver,
      inv.value->'validation_detail' validation_detail,
      inv.value->'support_readiness' support_readiness,
      inv.value->>'active_issue_operation_id' active_issue_operation_id_text,
      inv.value->'active_issue_operation' active_issue_operation,
      inv.value->>'active_document_operation_id' active_document_operation_id_text,
      inv.value->'last_issue_error' last_issue_error,
      inv.value->'last_document_error' last_document_error
    from legacy_weeks lw
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(lw.week_json->'invoices')='array' then lw.week_json->'invoices' else '[]'::jsonb end
    ) inv(value)
  ),
  candidate_names as materialized (
    select r.invoice_id,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_name)) filter(where nullif(s.candidate_name,'') is not null),'[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(s.week_ending_date)) filter(where s.week_ending_date is not null),'[]'::jsonb) week_ending_dates
    from invoice_rows_raw r
    left join public.invoice_lines l on l.invoice_id=r.invoice_id
    left join public.v_timesheets_summary_base s on s.timesheet_id=l.timesheet_id
    group by r.invoice_id
  ),
  rows_with_state as materialized (
    select r.*,
      coalesce(cn.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(cn.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(cn.candidate_names,'[]'::jsonb))=1
        then cn.candidate_names->>0
        when jsonb_array_length(coalesce(cn.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(cn.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      case when jsonb_array_length(coalesce(cn.week_ending_dates,'[]'::jsonb))>0 then cn.week_ending_dates else jsonb_build_array(r.week_ending_date) end week_ending_dates,
      case
        when exists (
          select 1 from public.invoice_document_versions v
          where v.entity_type='INVOICE'
            and v.entity_id=r.invoice_id
            and v.purpose='DRAFT_PREVIEW'
            and v.source_revision=r.document_revision::text
            and v.template_version='invoice-professional-v2'
            and v.status='READY'
            and v.r2_key is not null
            and v.sha256 ~ '^[0-9a-f]{64}$'
            and coalesce(v.size_bytes,0)>0
            and coalesce(v.page_count,0)>0
        ) then 'FRESH'
        when exists (
          select 1 from public.invoice_document_versions prior_ready
          where prior_ready.entity_type='INVOICE'
            and prior_ready.entity_id=r.invoice_id
            and prior_ready.purpose='DRAFT_PREVIEW'
            and prior_ready.template_version='invoice-professional-v2'
            and prior_ready.status='READY'
            and prior_ready.r2_key is not null
            and prior_ready.sha256 ~ '^[0-9a-f]{64}$'
            and coalesce(prior_ready.size_bytes,0)>0
            and coalesce(prior_ready.page_count,0)>0
        ) then case
          when nullif(r.active_document_operation_id_text,'') is not null then 'ACTIVE'
          when r.preview_document_state in ('FAILED') or r.last_document_error is not null then 'FAILED'
          else 'STALE'
        end
        else 'NEVER_GENERATED'
      end generated_state,
      nullif(r.active_issue_operation_id_text,'') is not null is_active_issue,
      coalesce((jsonb_array_length(r.delivery_blocker_codes)>0 and r.can_issue_only and not r.can_issue_and_deliver),false) blocked_for_sending
    from invoice_rows_raw r
    left join candidate_names cn on cn.invoice_id=r.invoice_id
  ),
  candidate_rows as materialized (
    select r.*,
      (case when r.generated_state='STALE' then jsonb_build_array('STALE') else '[]'::jsonb end)
      || (case when r.generated_state='FAILED' then jsonb_build_array('FAILED_RENDER') else '[]'::jsonb end)
      || (case when r.generated_state='ACTIVE' then jsonb_build_array('GENERATING') else '[]'::jsonb end)
      || coalesce(r.hard_blocker_codes,'[]'::jsonb)
      || coalesce(r.document_dependency_codes,'[]'::jsonb) issue_blocker_codes,
      case when r.blocked_for_sending then jsonb_build_array('BLOCKED_FOR_SENDING') else '[]'::jsonb end
        || coalesce(r.warning_codes,'[]'::jsonb)
        || (case when exists (
          select 1
          from jsonb_array_elements_text(coalesce(r.delivery_blocker_codes,'[]'::jsonb)) code(value)
          where code.value='EXPENSE_INVOICE_EMAIL_REQUIRED'
        ) then jsonb_build_array('EXPENSE_EMAIL_MISSING') else '[]'::jsonb end)
        informational_codes,
      (r.generated_state='FRESH'
        and r.can_issue_only
        and jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))=0
        and jsonb_array_length(coalesce(r.document_dependency_codes,'[]'::jsonb))=0
        and not r.is_active_issue) selectable,
      case when r.is_active_issue then 'IN_PROGRESS'
           when r.generated_state='STALE' then 'STALE'
           when r.generated_state='FAILED' then 'FAILED'
           when r.generated_state='ACTIVE' then 'IN_PROGRESS'
           when not r.can_issue_only or jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))>0
             or jsonb_array_length(coalesce(r.document_dependency_codes,'[]'::jsonb))>0 then 'BLOCKED'
           else 'READY' end row_status
    from rows_with_state r
    where r.generated_state <> 'NEVER_GENERATED'
      and r.invoice_status in ('DRAFT','ON_HOLD')
      and nullif(r.invoice_number,'') is not null
  ),
  filtered_rows as materialized (
    select r.*
    from candidate_rows r
    cross join params p
    where (p.allow_early or not coalesce(r.is_early,false))
      and (p.display_mode='ALL'
        or (p.display_mode='READY' and r.selectable)
        or (p.display_mode='BLOCKED' and (r.row_status in ('BLOCKED','STALE','FAILED') or r.blocked_for_sending)))
      and (p.search_text is null
        or lower(coalesce(r.invoice_number,'')||' '||coalesce(r.client_name,'')||' '||coalesce(r.candidate_display,'')||' '||coalesce(r.invoice_id::text,'')) like '%'||p.search_text||'%')
      and (p.week_ending_from is null or r.week_ending_date >= p.week_ending_from)
      and (p.week_ending_to is null or r.week_ending_date <= p.week_ending_to)
      and (jsonb_array_length(p.client_ids)=0 or r.client_id::text in (select value from jsonb_array_elements_text(p.client_ids)))
      and (jsonb_array_length(p.candidate_ids)=0 or exists (
        select 1 from jsonb_array_elements_text(r.candidate_ids) row_candidate(id)
        where row_candidate.id in (select value from jsonb_array_elements_text(p.candidate_ids))
      ))
      and (jsonb_array_length(p.week_endings)=0 or r.week_ending_date::text in (select value from jsonb_array_elements_text(p.week_endings)))
      and (jsonb_array_length(p.status_codes)=0 or r.row_status in (select upper(value) from jsonb_array_elements_text(p.status_codes)))
      and (jsonb_array_length(p.blocker_codes)=0 or exists (
        select 1 from jsonb_array_elements_text(coalesce(r.issue_blocker_codes,'[]'::jsonb) || coalesce(r.delivery_blocker_codes,'[]'::jsonb) || coalesce(r.informational_codes,'[]'::jsonb)) badge(code)
        where badge.code in (select upper(value) from jsonb_array_elements_text(p.blocker_codes))
      ))
  ),
  sortable_rows as materialized (
    select fr.*,
      case when p.sort_key='WEEK_ENDING_DATE' then coalesce(fr.week_ending_date, case when p.sort_direction='DESC' then date '0001-01-01' else date '9999-12-31' end) end sort_date_key,
      case when p.sort_key='CLIENT_NAME' then coalesce(lower(fr.client_name), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='CANDIDATE_NAME' then coalesce(lower(fr.candidate_display), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='STATUS' then coalesce(lower(fr.row_status), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='INVOICE_NUMBER' then coalesce(lower(fr.invoice_number), case when p.sort_direction='DESC' then '' else repeat('~',100) end) end sort_text_key,
      case when p.sort_key='TOTAL_EX_VAT' then coalesce(fr.total_ex_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end)
           when p.sort_key='TOTAL_INC_VAT' then coalesce(fr.total_inc_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end) end sort_numeric_key
    from filtered_rows fr cross join params p
  ),
  cursor_filtered_rows as materialized (
    select sr.*
    from sortable_rows sr
    cross join params p
    where p.after_key is null
       or (
         p.sort_key='WEEK_ENDING_DATE'
         and p.after_sort_date is not null
         and ((p.sort_direction='ASC' and (sr.sort_date_key > p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_date_key < p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER')
         and p.after_sort_text is not null
         and ((p.sort_direction='ASC' and (sr.sort_text_key > p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_text_key < p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT')
         and p.after_sort_numeric is not null
         and ((p.sort_direction='ASC' and (sr.sort_numeric_key > p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_numeric_key < p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key))))
       )
       or (
         ((p.sort_key='WEEK_ENDING_DATE' and p.after_sort_date is null)
           or (p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.after_sort_text is null)
           or (p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.after_sort_numeric is null))
         and sr.selection_key > p.after_key
       )
  ),
  selected_rows as materialized (
    select fr.*,
      coalesce((
        select sr.action
        from selection_rules sr
        where (sr.selector_type='ROW' and sr.selection_key=fr.selection_key)
           or (sr.selector_type='WEEK' and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='CLIENT' and sr.client_id=fr.client_id)
           or (sr.selector_type='CANDIDATE' and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='WEEK_CLIENT' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='WEEK_CLIENT_CANDIDATE' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
        order by sr.rule_sequence desc
        limit 1
      ),'INCLUDE') last_selection_action
    from cursor_filtered_rows fr
  ),
  expanded_rows as materialized (
    select * from selected_rows
    where selectable and last_selection_action <> 'EXCLUDE'
  ),
  candidate_page_source as materialized (
    select * from expanded_rows where v_mode='EXPAND_SELECTION'
    union all
    select * from selected_rows where v_mode<>'EXPAND_SELECTION'
  ),
  ordered_page_rows as materialized (
    select src.*,
      row_number() over (order by
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='ASC' then src.sort_date_key end asc nulls last,
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='DESC' then src.sort_date_key end desc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.sort_direction='ASC' then src.sort_text_key end asc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.sort_direction='DESC' then src.sort_text_key end desc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='ASC' then src.sort_numeric_key end asc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='DESC' then src.sort_numeric_key end desc nulls last,
        src.selection_key asc
      ) page_ordinal
    from candidate_page_source src cross join params p
  ),
  page_rows as materialized (
    select * from ordered_page_rows
    where page_ordinal <= (select page_size + 1 from params)
  ),
  visible_rows as materialized (
    select * from page_rows
    where page_ordinal <= (select page_size from params)
  ),
  totals as materialized (
    select
      count(*)::integer all_count,
      count(*) filter(where selectable)::integer ready_count,
      count(*) filter(where not selectable)::integer blocked_count,
      count(*) filter(where blocked_for_sending)::integer blocked_for_sending_count,
      count(*) filter(where generated_state='STALE')::integer stale_count,
      count(*) filter(where row_status='IN_PROGRESS')::integer in_progress_count
    from filtered_rows
  ),
  row_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selection_key',selection_key,
      'invoice_id',invoice_id,
      'invoice_number',invoice_number,
      'document_revision',document_revision,
      'client_id',client_id,
      'client_name',client_name,
      'candidate_ids',candidate_ids,
      'candidate_names',candidate_names,
      'candidate_display',candidate_display,
      'week_ending_dates',week_ending_dates,
      'week_ending_date',week_ending_date,
      'week_ending_display',case when jsonb_array_length(week_ending_dates)>1 then 'Multiple weeks' else to_char(week_ending_date,'DD/MM/YYYY') end,
      'currency',currency,
      'total_ex_vat',total_ex_vat,
      'vat_amount',vat_amount,
      'total_inc_vat',total_inc_vat,
      'generated_state',generated_state,
      'row_status',row_status,
      'is_early',is_early,
      'selectable',selectable,
      'selected',selectable and last_selection_action <> 'EXCLUDE',
      'issue_blocker_codes',coalesce(issue_blocker_codes,'[]'::jsonb),
      'delivery_blocker_codes',coalesce(delivery_blocker_codes,'[]'::jsonb),
      'informational_codes',coalesce(informational_codes,'[]'::jsonb),
      'blocked_for_sending',blocked_for_sending,
      'can_issue_only',can_issue_only,
      'can_issue_and_deliver',can_issue_and_deliver,
      'active_issue_operation_id',active_issue_operation_id_text,
      'active_issue_status',active_issue_operation->>'status',
      'active_issue_operation',active_issue_operation,
      'active_document_operation_id',active_document_operation_id_text,
      'validation_detail',validation_detail,
      'support_readiness',support_readiness,
      'sort_tuple',jsonb_build_object(
        'sort_date',case when sort_date_key is not null then sort_date_key::text end,
        'sort_text',sort_text_key,
        'sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end,
        'selection_key',selection_key
      )
    ) order by page_ordinal),'[]'::jsonb) rows
    from visible_rows
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_CANDIDATES_V1',
    'action','ISSUE',
    'mode',v_mode,
    'snapshot_at_utc',coalesce(v_query->>'snapshot_at_utc',p_now_utc::text),
    'normalised_filter',v_filters,
    'normalised_sort',v_sort,
    'filter_hash',encode(digest(coalesce(v_filters,'{}'::jsonb)::text || '|' || coalesce(v_sort,'{}'::jsonb)::text || '|ISSUE','sha256'),'hex'),
    'rows',(select rows from row_json),
    'page',jsonb_build_object(
      'page_size',v_page_size,
      'has_more',(select count(*) from page_rows)>v_page_size,
      'next_cursor_values',case when (select count(*) from page_rows)>v_page_size then (
        select jsonb_build_object(
          'after_selection_key',selection_key,
          'after_sort_date',case when sort_date_key is not null then sort_date_key::text end,
          'after_sort_text',sort_text_key,
          'after_sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end
        )
        from visible_rows
        order by page_ordinal desc
        limit 1
      ) else null end
    ),
    'totals',jsonb_build_object(
      'all',(select all_count from totals),
      'ready',(select ready_count from totals),
      'blocked',(select blocked_count from totals),
      'blocked_for_sending',(select blocked_for_sending_count from totals),
      'stale',(select stale_count from totals),
      'in_progress',(select in_progress_count from totals)
    ),
    'facets',jsonb_build_object(),
    'selection_seed',jsonb_build_object('mode','IMPLICIT_ALL','default_selected',true)
  ) into v_result;

  return v_result;
end;
$function$;

-- private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_candidate_rows_v2(p_query jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_selection jsonb;
  v_selection_keys jsonb;
  v_expected_source_revisions jsonb;
  v_page_size integer;
  v_after_key text;
  v_allow_early boolean;
  v_display_mode text;
  v_input_snapshot jsonb;
  v_snapshot jsonb;
  v_snapshot_after jsonb;
  v_filter_hash text;
  v_query_hash text;
  v_selection_hash text;
  v_result jsonb;
begin
  v_query := private._invoice_batch_query_validate_v2(v_query, 'ISSUE');

  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if coalesce(v_query->>'contract_version','INVOICE_BATCH_QUERY_V2') <> 'INVOICE_BATCH_QUERY_V2' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  v_mode := upper(coalesce(nullif(v_query->>'mode',''),'PAGE'));
  if v_mode not in ('PAGE','FACETS','SUMMARY','EXPAND_SELECTION','EXPLICIT_KEYS') then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_selection_keys := coalesce(v_query->'selection_keys','[]'::jsonb);
  v_expected_source_revisions := coalesce(
    v_query->'expected_source_revisions',
    '{}'::jsonb
  );
  if v_mode='EXPLICIT_KEYS' then
    if jsonb_typeof(v_selection_keys) is distinct from 'array'
       or jsonb_array_length(v_selection_keys) < 1
       or jsonb_array_length(v_selection_keys) > 100
       or jsonb_typeof(v_expected_source_revisions) is distinct from 'object'
       or exists (
         select 1
         from jsonb_array_elements(v_selection_keys) with ordinality key_item(value,ordinality)
         where jsonb_typeof(key_item.value) is distinct from 'string'
            or nullif(btrim(key_item.value #>> '{}'),'') is null
            or length(btrim(key_item.value #>> '{}')) > 512
       )
       or (
         select count(*) from jsonb_array_elements_text(v_selection_keys)
       ) <> (
         select count(distinct value)
         from jsonb_array_elements_text(v_selection_keys) explicit_key(value)
       )
       or exists (
         select 1
         from jsonb_array_elements_text(v_selection_keys) explicit_key(value)
         where nullif(v_expected_source_revisions->>explicit_key.value,'') is null
       ) then
      raise exception using
        errcode='22023',
        message='INVOICE_BATCH_EXPLICIT_KEYS_INVALID';
    end if;
  end if;

  v_filters := case when jsonb_typeof(v_query->'filters') = 'object' then v_query->'filters' else '{}'::jsonb end;
  v_sort := case when jsonb_typeof(v_query->'sort') = 'object' then v_query->'sort' else '{}'::jsonb end;
  v_selection := case when jsonb_typeof(v_query->'selection') = 'object' then v_query->'selection' else jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V2',
    'mode','IMPLICIT_ALL',
    'default_selected',true,
    'rules','[]'::jsonb
  ) end;

  perform 1 from private._invoice_batch_selection_rules_v2(v_selection) limit 1;

  v_allow_early := coalesce((v_filters->>'allow_early')::boolean,false);
  v_display_mode := upper(coalesce(
    nullif(v_filters->>'display_mode',''),
    'ALL'
  ));
  if v_display_mode not in ('ALL','READY','BLOCKED') then
    raise exception using errcode='22023', message='INVOICE_BATCH_DISPLAY_MODE_INVALID';
  end if;

  if upper(coalesce(v_sort->>'group_preset','WEEK_CLIENT_CANDIDATE')) not in (
    'WEEK_CLIENT_CANDIDATE','CLIENT_WEEK_CANDIDATE','CANDIDATE_WEEK_CLIENT','STATUS_WEEK_CLIENT'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_GROUP_PRESET_INVALID';
  end if;

  if upper(coalesce(v_sort->>'sort_key','WEEK_ENDING_DATE')) not in (
    'WEEK_ENDING_DATE','CLIENT_NAME','CANDIDATE_NAME','TOTAL_EX_VAT','TOTAL_INC_VAT','STATUS','INVOICE_NUMBER'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SORT_KEY_INVALID';
  end if;

  -- V8 Issue totals, selection summaries, status ordering and cursors always
  -- use the shared authoritative scalar classifier.
  -- PAGE, EXPAND_SELECTION and EXPLICIT_KEYS always obtain their bounded
  -- keyset first. The complete classifier remains the scalar authority for
  -- exact filtering, totals and group state, but it must never cause the
  -- rich presentation hydrator to receive an unrestricted invoice set.

  v_page_size := case
    when v_mode = 'EXPAND_SELECTION'
      then (v_query->>'page_size')::integer
    when v_mode = 'PAGE'
      then (v_query->>'page_size')::integer
    else 100
  end;
  v_after_key := nullif(v_query#>>'{cursor,after_selection_key}', '');

  v_input_snapshot := v_query->'snapshot';
  if v_input_snapshot is null
     or jsonb_typeof(v_input_snapshot) = 'null' then
    if v_mode <> 'PAGE' or v_after_key is not null then
      raise exception using errcode='22023', message='BATCH_SNAPSHOT_REQUIRED';
    end if;
    v_snapshot := private._invoice_candidate_snapshot_get_v2(
      'ISSUE',
      coalesce(p_now_utc,now())
    );
  else
    v_snapshot := private._invoice_candidate_snapshot_verify_v2(
      'ISSUE',
      v_input_snapshot,
      coalesce(p_now_utc,now())
    );
  end if;

  v_filter_hash := private._invoice_batch_hash_v2(jsonb_build_object(
    'action','ISSUE',
    'filters',v_filters,
    'sort',v_sort
  ));
  v_query_hash := private._invoice_batch_hash_v2(jsonb_build_object(
    'contract_version','INVOICE_BATCH_QUERY_V2',
    'action','ISSUE',
    'filters',v_filters,
    'sort',v_sort,
    'snapshot',jsonb_build_object(
      'contract_version',v_snapshot->>'contract_version',
      'action',v_snapshot->>'action',
      'at_utc',v_snapshot->>'at_utc',
      'revision',v_snapshot->>'revision',
      'expires_at_utc',v_snapshot->>'expires_at_utc',
      'key_id',v_snapshot->>'key_id'
    )
  ));
  v_selection_hash := private._invoice_batch_hash_v2(v_selection);

  with
  params as materialized (
    select
      v_mode mode,
      v_allow_early allow_early,
      v_display_mode display_mode,
      v_page_size page_size,
      v_after_key after_key,
      lower(nullif(btrim(coalesce(v_filters->>'search','')),'')) search_text,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_from',''),'date') then (v_filters->>'week_ending_from')::date end week_ending_from,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_to',''),'date') then (v_filters->>'week_ending_to')::date end week_ending_to,
      (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date today,
      case when jsonb_typeof(v_filters->'client_ids')='array' then v_filters->'client_ids' else '[]'::jsonb end client_ids,
      case when jsonb_typeof(v_filters->'candidate_ids')='array' then v_filters->'candidate_ids' else '[]'::jsonb end candidate_ids,
      case when jsonb_typeof(v_filters->'week_endings')='array' then v_filters->'week_endings' else '[]'::jsonb end week_endings,
      case when jsonb_typeof(v_filters->'status_codes')='array' then v_filters->'status_codes' else '[]'::jsonb end status_codes,
      case when jsonb_typeof(v_filters->'blocker_codes')='array' then v_filters->'blocker_codes' else '[]'::jsonb end blocker_codes,
      case when jsonb_typeof(v_filters->'invoice_streams')='array' then v_filters->'invoice_streams' else '[]'::jsonb end invoice_streams,
      coalesce(nullif(upper(v_sort->>'group_preset'),''),'WEEK_CLIENT_CANDIDATE') group_preset,
      coalesce(nullif(upper(v_sort->>'sort_key'),''),'WEEK_ENDING_DATE') sort_key,
      case when upper(coalesce(v_sort->>'sort_direction','ASC'))='DESC' then 'DESC' else 'ASC' end sort_direction,
      case when pg_input_is_valid(
        nullif(coalesce(v_query#>>'{cursor,after_sort_date}',''),''),
        'date'
      )
        then (v_query#>>'{cursor,after_sort_date}')::date end after_sort_date,
      nullif(coalesce(v_query#>>'{cursor,after_sort_text}',''),'') after_sort_text,
      case when coalesce(v_query#>>'{cursor,after_sort_numeric}','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (v_query#>>'{cursor,after_sort_numeric}')::numeric end after_sort_numeric,
      lower(nullif(btrim(coalesce(v_query#>>'{facet_request,search}','')),'')) facet_search,
      case when coalesce(v_query#>>'{facet_request,limit_per_kind}','') ~ '^[1-9][0-9]{0,2}$'
        then least((v_query#>>'{facet_request,limit_per_kind}')::integer,100)
        else 100 end facet_limit,
      case when jsonb_typeof(v_query#>'{facet_request,kinds}')='array'
        then v_query#>'{facet_request,kinds}'
        else '["CLIENTS","CANDIDATES","WEEK_ENDINGS","STATUSES","BLOCKERS"]'::jsonb end facet_kinds,
      lower(nullif(v_query#>>'{facet_request,cursors,clients,after_label}','')) facet_client_after_label,
      nullif(v_query#>>'{facet_request,cursors,clients,after_id}','') facet_client_after_id,
      lower(nullif(v_query#>>'{facet_request,cursors,candidates,after_label}','')) facet_candidate_after_label,
      nullif(v_query#>>'{facet_request,cursors,candidates,after_id}','') facet_candidate_after_id,
      case when pg_input_is_valid(
        coalesce(v_query#>>'{facet_request,cursors,week_endings,after_value}',''),
        'date'
      ) then (v_query#>>'{facet_request,cursors,week_endings,after_value}')::date end facet_week_after_value,
      nullif(v_query#>>'{facet_request,cursors,statuses,after_code}','') facet_status_after_code,
      nullif(v_query#>>'{facet_request,cursors,blockers,after_code}','') facet_blocker_after_code
  ),
  selection_rules as materialized (
    select * from private._invoice_batch_selection_rules_v2(v_selection)
  ),
  candidate_keys as materialized (
    select key_row.*
    from (
      select 1
      where v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
    ) gate
    cross join lateral private._invoice_batch_issue_candidate_key_rows_v2(
      v_query,
      coalesce(p_now_utc,now())
    ) key_row
  ),
  issue_scope_request as materialized (
    select
      case
        when v_mode in ('FACETS','SUMMARY') then '{}'::uuid[]
        else coalesce(array_agg(k.invoice_id order by k.page_ordinal)
          filter (where k.page_ordinal<=v_page_size),'{}'::uuid[])
      end invoice_ids
    from candidate_keys k
  ),
  source_weeks as materialized (
    select
      '{}'::jsonb client_json,
      source.client_id::text client_id_text,
      source.client_name,
      source.invoice_week_start,
      source.week_ending_date,
      source.invoice_json
    from issue_scope_request request
    cross join lateral private._invoice_batch_issue_source_rows_for_ids_v2(
      request.invoice_ids,
      true,
      coalesce(p_now_utc,now())
    ) source
    where v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
      and cardinality(request.invoice_ids)>0
  ),
  invoice_rows_raw as materialized (
    select
      'invoice:'||coalesce(inv.value->>'invoice_id','') selection_key,


      case when pg_input_is_valid(inv.value->>'invoice_id','uuid') then (inv.value->>'invoice_id')::uuid end invoice_id,
      inv.value->>'invoice_no' invoice_number,
      case when pg_input_is_valid(lw.client_id_text,'uuid') then lw.client_id_text::uuid end client_id,
      lw.client_name,
      case when pg_input_is_valid(inv.value->>'document_revision','int8') then (inv.value->>'document_revision')::bigint end document_revision,
      lw.week_ending_date,
      case when lw.week_ending_date is not null
        then lw.week_ending_date >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date
        else false end is_early,
      coalesce(nullif(inv.value->>'currency',''),'GBP') currency,
      upper(coalesce(nullif(inv.value->>'invoice_stream',''),'NORMAL')) invoice_stream,
      case when coalesce(inv.value->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'subtotal_ex_vat')::numeric,2) else 0 end total_ex_vat,
      case when coalesce(inv.value->>'vat_amount','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'vat_amount')::numeric,2) else 0 end vat_amount,
      case when coalesce(inv.value->>'total_inc_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'total_inc_vat')::numeric,2) else 0 end total_inc_vat,
      upper(coalesce(inv.value->>'preview_document_state','')) preview_document_state,
      upper(coalesce(inv.value->>'status','')) invoice_status,
      coalesce(inv.value->'stable_blocker_codes','[]'::jsonb) hard_blocker_codes,
      coalesce(inv.value->'document_dependency_codes','[]'::jsonb) document_dependency_codes,
      coalesce(inv.value->'delivery_blocker_codes','[]'::jsonb) delivery_blocker_codes,
      coalesce(inv.value->'recipient_routing_warnings','[]'::jsonb) warning_codes,
      coalesce(inv.value->>'can_issue_only','false') in ('true','t','1','yes','on') can_issue_only,
      coalesce(inv.value->>'can_issue_and_deliver','false') in ('true','t','1','yes','on') can_issue_and_deliver,
      inv.value->'validation_detail' validation_detail,
      inv.value->'support_readiness' support_readiness,
      inv.value->>'active_issue_operation_id' active_issue_operation_id_text,
      inv.value->'active_issue_operation' active_issue_operation,
      inv.value->>'active_document_operation_id' active_document_operation_id_text,
      inv.value->'last_issue_error' last_issue_error,
      inv.value->'last_document_error' last_document_error
    from source_weeks lw
    cross join lateral (select lw.invoice_json value) inv
  ),
  candidate_names as materialized (
    select r.invoice_id,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_name)) filter(where nullif(s.candidate_name,'') is not null),'[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(s.week_ending_date)) filter(where s.week_ending_date is not null),'[]'::jsonb) week_ending_dates
    from invoice_rows_raw r
    left join public.invoice_lines l on l.invoice_id=r.invoice_id
    left join public.v_timesheets_summary_base s on s.timesheet_id=l.timesheet_id
    group by r.invoice_id
  ),
  rows_with_state as materialized (
    select r.*,
      coalesce(cn.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(cn.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(cn.candidate_names,'[]'::jsonb))=1
        then cn.candidate_names->>0
        when jsonb_array_length(coalesce(cn.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(cn.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      case when jsonb_array_length(coalesce(cn.week_ending_dates,'[]'::jsonb))>0 then cn.week_ending_dates else jsonb_build_array(r.week_ending_date) end week_ending_dates,
      case
        when exists (
          select 1 from public.invoice_document_versions v
          where v.entity_type='INVOICE'
            and v.entity_id=r.invoice_id
            and v.purpose='DRAFT_PREVIEW'
            and v.source_revision=r.document_revision::text
            and v.template_version='invoice-professional-v2'
            and v.status='READY'
            and v.r2_key is not null
            and v.sha256 ~ '^[0-9a-f]{64}$'
            and coalesce(v.size_bytes,0)>0
            and coalesce(v.page_count,0)>0
        ) then 'FRESH'
        when exists (
          select 1 from public.invoice_document_versions prior_ready
          where prior_ready.entity_type='INVOICE'
            and prior_ready.entity_id=r.invoice_id
            and prior_ready.purpose='DRAFT_PREVIEW'
            and prior_ready.template_version='invoice-professional-v2'
            and prior_ready.status='READY'
            and prior_ready.r2_key is not null
            and prior_ready.sha256 ~ '^[0-9a-f]{64}$'
            and coalesce(prior_ready.size_bytes,0)>0
            and coalesce(prior_ready.page_count,0)>0
        ) then case
          when nullif(r.active_document_operation_id_text,'') is not null then 'ACTIVE'
          when r.preview_document_state in ('FAILED') or r.last_document_error is not null then 'FAILED'
          else 'STALE'
        end
        else 'NEVER_GENERATED'
      end generated_state,
      nullif(r.active_issue_operation_id_text,'') is not null is_active_issue,
      coalesce((jsonb_array_length(r.delivery_blocker_codes)>0 and r.can_issue_only and not r.can_issue_and_deliver),false) blocked_for_sending
    from invoice_rows_raw r
    left join candidate_names cn on cn.invoice_id=r.invoice_id
  ),
  candidate_rows as materialized (
    select r.*,
      coalesce(r.hard_blocker_codes,'[]'::jsonb) issue_blocker_codes,
      case when r.blocked_for_sending then jsonb_build_array('BLOCKED_FOR_SENDING') else '[]'::jsonb end
        || coalesce(r.document_dependency_codes,'[]'::jsonb)
        || coalesce(r.warning_codes,'[]'::jsonb) informational_codes,
      (r.can_issue_only
        and jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))=0
        and not r.is_active_issue) selectable,
      case when r.is_active_issue then 'IN_PROGRESS'
           when not r.can_issue_only or jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))>0
             then 'BLOCKED'
           else 'READY' end row_status
    from rows_with_state r
    where r.invoice_status in ('DRAFT','ON_HOLD')
      and nullif(r.invoice_number,'') is not null
  ),
  classification_source as materialized (
    -- PAGE-family requests reuse the exact candidate payload that the keyset
    -- helper already classified. FACETS/SUMMARY do not invoke the keyset
    -- helper, so they retain one direct full-scope classification pass.
    select key_row.selection_key,key_row.candidate_json
    from candidate_keys key_row
    where key_row.page_ordinal<=v_page_size
    union all
    select candidate.selection_key,candidate.candidate_json
    from (
      select 1
      where v_mode in ('FACETS','SUMMARY')
    ) gate
    cross join lateral private._invoice_batch_issue_classification_v2(
      true,
      null,
      coalesce(p_now_utc,statement_timestamp())
    ) candidate
  ),
  authoritative_rows as materialized (
    select
      candidate.candidate_json->>'selection_key' selection_key,
      (candidate.candidate_json->>'invoice_id')::uuid invoice_id,
      candidate.candidate_json->>'invoice_number' invoice_number,
      (candidate.candidate_json->>'client_id')::uuid client_id,
      candidate.candidate_json->>'client_name' client_name,
      (candidate.candidate_json->>'document_revision')::bigint
        document_revision,
      case
        when pg_input_is_valid(
          coalesce(candidate.candidate_json->>'week_ending_date',''),
          'date'
        )
          then (candidate.candidate_json->>'week_ending_date')::date
      end week_ending_date,
      coalesce(
        (candidate.candidate_json->>'is_early')::boolean,
        false
      ) is_early,
      coalesce(
        nullif(candidate.candidate_json->>'currency',''),
        'GBP'
      ) currency,
      upper(coalesce(
        nullif(candidate.candidate_json->>'invoice_stream',''),
        'NORMAL'
      )) invoice_stream,
      coalesce(
        (candidate.candidate_json->>'total_ex_vat')::numeric,
        0
      ) total_ex_vat,
      coalesce(
        (candidate.candidate_json->>'vat_amount')::numeric,
        0
      ) vat_amount,
      coalesce(
        (candidate.candidate_json->>'total_inc_vat')::numeric,
        0
      ) total_inc_vat,
      candidate.candidate_json->>'preview_document_state'
        preview_document_state,
      candidate.candidate_json->>'invoice_status' invoice_status,
      coalesce(
        candidate.candidate_json->'hard_blocker_codes',
        '[]'::jsonb
      ) hard_blocker_codes,
      coalesce(
        candidate.candidate_json->'document_dependency_codes',
        '[]'::jsonb
      ) document_dependency_codes,
      coalesce(
        candidate.candidate_json->'delivery_blocker_codes',
        '[]'::jsonb
      ) delivery_blocker_codes,
      coalesce(
        candidate.candidate_json->'warning_codes',
        '[]'::jsonb
      ) warning_codes,
      coalesce(
        (candidate.candidate_json->>'can_issue_only')::boolean,
        false
      ) can_issue_only,
      coalesce(
        (candidate.candidate_json->>'can_issue_and_deliver')::boolean,
        false
      ) can_issue_and_deliver,
      candidate.candidate_json->'validation_detail'
        validation_detail,
      candidate.candidate_json->'support_readiness'
        support_readiness,
      candidate.candidate_json->>'active_issue_operation_id'
        active_issue_operation_id_text,
      candidate.candidate_json->'active_issue_operation'
        active_issue_operation,
      candidate.candidate_json->>'active_document_operation_id'
        active_document_operation_id_text,
      candidate.candidate_json->'last_issue_error'
        last_issue_error,
      candidate.candidate_json->'last_document_error'
        last_document_error,
      coalesce(
        candidate.candidate_json->'candidate_ids',
        '[]'::jsonb
      ) candidate_ids,
      coalesce(
        candidate.candidate_json->'candidate_names',
        '[]'::jsonb
      ) candidate_names,
      candidate.candidate_json->>'candidate_display'
        candidate_display,
      coalesce(
        candidate.candidate_json->'week_ending_dates',
        '[]'::jsonb
      ) week_ending_dates,
      candidate.candidate_json->>'generated_state'
        generated_state,
      coalesce(
        (candidate.candidate_json->>'blocked_for_sending')::boolean,
        false
      ) blocked_for_sending,
      coalesce(
        candidate.candidate_json->'issue_blocker_codes',
        '[]'::jsonb
      ) issue_blocker_codes,
      coalesce(
        candidate.candidate_json->'informational_codes',
        '[]'::jsonb
      ) informational_codes,
      coalesce(
        (candidate.candidate_json->>'selectable')::boolean,
        false
      ) selectable,
      candidate.candidate_json->>'row_status' row_status
    from classification_source candidate
  ),
  filter_match_rows as materialized (
    select
      r.*,
      (
        jsonb_array_length(p.client_ids)=0
        or r.client_id::text in (
          select value from jsonb_array_elements_text(p.client_ids)
        )
      ) client_filter_match,
      (
        jsonb_array_length(p.candidate_ids)=0
        or exists (
          select 1
          from jsonb_array_elements_text(r.candidate_ids) row_candidate(id)
          where row_candidate.id in (
            select value from jsonb_array_elements_text(p.candidate_ids)
          )
        )
      ) candidate_filter_match,
      (
        jsonb_array_length(p.week_endings)=0
        or r.week_ending_date::text in (
          select value from jsonb_array_elements_text(p.week_endings)
        )
      ) week_filter_match,
      (
        jsonb_array_length(p.status_codes)=0
        or r.row_status in (
          select upper(value)
          from jsonb_array_elements_text(p.status_codes)
        )
      ) status_filter_match,
      (
        jsonb_array_length(p.blocker_codes)=0
        or exists (
          select 1
          from jsonb_array_elements_text(
            coalesce(r.issue_blocker_codes,'[]'::jsonb)
            || coalesce(r.delivery_blocker_codes,'[]'::jsonb)
            || coalesce(r.informational_codes,'[]'::jsonb)
          ) badge(code)
          where badge.code in (
            select upper(value)
            from jsonb_array_elements_text(p.blocker_codes)
          )
        )
      ) blocker_filter_match
    from authoritative_rows r
    cross join params p
    where (p.allow_early or not coalesce(r.is_early,false))
      and (
        jsonb_array_length(p.invoice_streams)=0
        or r.invoice_stream in (
          select upper(value)
          from jsonb_array_elements_text(p.invoice_streams)
        )
      )
      and (
        v_mode <> 'EXPLICIT_KEYS'
        or r.selection_key in (
          select value
          from jsonb_array_elements_text(v_selection_keys)
        )
      )
      and (p.search_text is null


        or lower(coalesce(r.invoice_number,'')||' '||coalesce(r.client_name,'')||' '||coalesce(r.candidate_display,'')||' '||coalesce(r.invoice_id::text,'')) like '%'||p.search_text||'%')
      and (p.week_ending_from is null or r.week_ending_date >= p.week_ending_from)
      and (p.week_ending_to is null or r.week_ending_date <= p.week_ending_to)
  ),
  scope_rows as materialized (
    select r.*
    from filter_match_rows r
    where r.client_filter_match
      and r.candidate_filter_match
      and r.week_filter_match
      and r.status_filter_match
      and r.blocker_filter_match
  ),
  facet_client_rows as materialized (
    select r.* from filter_match_rows r
    where r.candidate_filter_match and r.week_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_candidate_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.week_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_week_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_status_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.week_filter_match and r.blocker_filter_match
  ),
  facet_blocker_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.week_filter_match and r.status_filter_match
  ),
  facet_client_values_base as materialized (
    select
      client_id,
      min(coalesce(nullif(client_name,''),client_id::text)) label,
      count(*)::integer row_count
    from facet_client_rows
    where client_id is not null
    group by client_id
  ),
  facet_client_values as materialized (
    select b.*,
      row_number() over(order by lower(b.label),b.client_id) facet_ordinal,
      count(*) over() facet_total
    from facet_client_values_base b
    cross join params p
    where (p.facet_search is null
        or lower(b.label) like '%'||p.facet_search||'%'
        or b.client_id::text like p.facet_search||'%')
      and (p.facet_client_after_label is null
        or (lower(b.label),b.client_id::text)>
           (p.facet_client_after_label,coalesce(p.facet_client_after_id,'')))
  ),
  facet_candidate_values_base as materialized (
    select
      candidate.value candidate_id,
      min(coalesce(
        nullif(r.candidate_names->>(candidate.ordinality::integer-1),''),
        candidate.value
      )) label,
      count(distinct r.selection_key)::integer row_count
    from facet_candidate_rows r
    cross join lateral jsonb_array_elements_text(
      coalesce(r.candidate_ids,'[]'::jsonb)
    ) with ordinality candidate(value,ordinality)
    group by candidate.value
  ),
  facet_candidate_values as materialized (
    select b.*,
      row_number() over(order by lower(b.label),b.candidate_id) facet_ordinal,
      count(*) over() facet_total
    from facet_candidate_values_base b
    cross join params p
    where (p.facet_search is null
        or lower(b.label) like '%'||p.facet_search||'%'
        or b.candidate_id like p.facet_search||'%')
      and (p.facet_candidate_after_label is null
        or (lower(b.label),b.candidate_id)>
           (p.facet_candidate_after_label,coalesce(p.facet_candidate_after_id,'')))
  ),
  facet_week_values as materialized (
    select b.*,
      row_number() over(order by b.week_ending_date desc) facet_ordinal,
      count(*) over() facet_total
    from (
      select week_ending_date,count(*)::integer row_count
      from facet_week_rows
      where week_ending_date is not null
      group by week_ending_date
    ) b
    cross join params p
    where (p.facet_search is null
        or to_char(b.week_ending_date,'DD/MM/YYYY') like '%'||p.facet_search||'%'
        or b.week_ending_date::text like '%'||p.facet_search||'%')
      and (p.facet_week_after_value is null
        or b.week_ending_date<p.facet_week_after_value)
  ),
  facet_status_values as materialized (
    select b.*,
      row_number() over(order by b.row_status) facet_ordinal,
      count(*) over() facet_total
    from (
      select row_status,count(*)::integer row_count
      from facet_status_rows
      group by row_status
    ) b
    cross join params p
    where (p.facet_search is null
        or lower(b.row_status) like '%'||p.facet_search||'%'
        or lower(replace(b.row_status,'_',' ')) like '%'||p.facet_search||'%')
      and (p.facet_status_after_code is null
        or b.row_status>p.facet_status_after_code)
  ),
  facet_blocker_values as materialized (
    select b.*,
      row_number() over(order by b.code) facet_ordinal,
      count(*) over() facet_total
    from (
      select badge.code,count(distinct r.selection_key)::integer row_count
      from facet_blocker_rows r
      cross join lateral jsonb_array_elements_text(
        coalesce(r.issue_blocker_codes,'[]'::jsonb)
        || coalesce(r.delivery_blocker_codes,'[]'::jsonb)
        || coalesce(r.informational_codes,'[]'::jsonb)
      ) badge(code)
      group by badge.code
    ) b
    cross join params p
    where (p.facet_search is null
        or lower(b.code) like '%'||p.facet_search||'%'
        or lower(replace(b.code,'_',' ')) like '%'||p.facet_search||'%')
      and (p.facet_blocker_after_code is null
        or b.code>p.facet_blocker_after_code)
  ),
  grouped_scope_rows as materialized (
    select
      r.*,
      private._invoice_batch_hash_v2(jsonb_build_object(
        'action','ISSUE',
        'group_preset',p.group_preset,
        'status_code',case when p.group_preset='STATUS_WEEK_CLIENT' then r.row_status end,
        'week_ending_date',r.week_ending_date,
        'client_id',r.client_id,
        'candidate_ids',case
          when p.group_preset='STATUS_WEEK_CLIENT' then '[]'::jsonb
          else coalesce(r.candidate_ids,'[]'::jsonb)
        end
      )) group_key
    from scope_rows r
    cross join params p
  ),
  selection_scope_rows as materialized (
    select
      fr.*,
      coalesce((
        select sr.action
        from selection_rules sr
        where (sr.selector_type='ROW' and sr.selection_key=fr.selection_key)
           or (sr.selector_type='WEEK' and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='CLIENT' and sr.client_id=fr.client_id)
           or (sr.selector_type='CANDIDATE' and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='STATUS' and sr.status_code=fr.row_status)
           or (sr.selector_type='WEEK_CLIENT' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='WEEK_CLIENT_CANDIDATE' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='STATUS_WEEK' and sr.status_code=fr.row_status and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='STATUS_WEEK_CLIENT' and sr.status_code=fr.row_status and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='DIMENSION_GROUP'
             and (sr.week_ending_date is null or sr.week_ending_date=fr.week_ending_date)
             and (sr.client_id is null or sr.client_id=fr.client_id)
             and (sr.status_code is null or sr.status_code=fr.row_status)
             and (sr.candidate_id is null or exists (
               select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
               where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
             )))
        order by sr.rule_sequence desc
        limit 1
      ),'INCLUDE') last_selection_action
    from grouped_scope_rows fr
  ),
  filtered_rows as materialized (
    select r.*
    from selection_scope_rows r
    cross join params p
    where (
      v_mode='EXPAND_SELECTION'
      or p.display_mode='ALL'
      or (p.display_mode='READY' and r.selectable)
      or (
        p.display_mode='BLOCKED'
        and (
          r.row_status in ('BLOCKED','STALE','FAILED')
          or r.blocked_for_sending
        )
      )
    )
      and (
        v_mode<>'EXPAND_SELECTION'
        or (
          r.selectable
          and r.last_selection_action<>'EXCLUDE'
        )
      )
  ),
  sortable_rows as materialized (
    select fr.*,
      case when p.sort_key='WEEK_ENDING_DATE' then coalesce(fr.week_ending_date, case when p.sort_direction='DESC' then date '0001-01-01' else date '9999-12-31' end) end sort_date_key,
      case when p.sort_key='CLIENT_NAME' then coalesce(lower(fr.client_name), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='CANDIDATE_NAME' then coalesce(lower(fr.candidate_display), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='STATUS' then
             lpad((case
               when fr.row_status='READY' and not fr.blocked_for_sending then 10
               when fr.row_status='READY' and fr.blocked_for_sending then 20
               when fr.row_status='STALE' then 30
               when fr.row_status='IN_PROGRESS' then 40
               when fr.row_status='FAILED' then 50
               else 60
             end)::text,3,'0')||'|'||lower(coalesce(fr.row_status,'BLOCKED'))
           when p.sort_key='INVOICE_NUMBER' then coalesce(lower(fr.invoice_number), case when p.sort_direction='DESC' then '' else repeat('~',100) end) end sort_text_key,
      case when p.sort_key='TOTAL_EX_VAT' then coalesce(fr.total_ex_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end)
           when p.sort_key='TOTAL_INC_VAT' then coalesce(fr.total_inc_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end) end sort_numeric_key
    from filtered_rows fr cross join params p
  ),
  cursor_filtered_rows as materialized (
    select sr.*
    from sortable_rows sr
    cross join params p
    where p.after_key is null
       or (
         p.sort_key='WEEK_ENDING_DATE'
         and p.after_sort_date is not null
         and ((p.sort_direction='ASC' and (sr.sort_date_key > p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_date_key < p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER')
         and p.after_sort_text is not null
         and ((p.sort_direction='ASC' and (sr.sort_text_key > p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_text_key < p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT')
         and p.after_sort_numeric is not null
         and ((p.sort_direction='ASC' and (sr.sort_numeric_key > p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_numeric_key < p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key))))
       )
       or (
         ((p.sort_key='WEEK_ENDING_DATE' and p.after_sort_date is null)
           or (p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.after_sort_text is null)
           or (p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.after_sort_numeric is null))
         and sr.selection_key > p.after_key
       )
  ),
  selected_rows as materialized (
    select * from cursor_filtered_rows
  ),
  candidate_page_source as materialized (
    select * from selected_rows where v_mode='EXPAND_SELECTION'
    union all
    select * from selected_rows where v_mode in('PAGE','EXPLICIT_KEYS')
  ),
  ordered_page_rows as materialized (
    select src.*,
      row_number() over (order by
        case when v_mode='EXPAND_SELECTION' then src.selection_key end asc,
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='ASC' then src.sort_date_key end asc nulls last,
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='DESC' then src.sort_date_key end desc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.sort_direction='ASC' then src.sort_text_key end asc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.sort_direction='DESC' then src.sort_text_key end desc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='ASC' then src.sort_numeric_key end asc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='DESC' then src.sort_numeric_key end desc nulls last,
        src.selection_key asc
      ) page_ordinal
    from candidate_page_source src cross join params p
  ),
  page_rows as materialized (
    select * from ordered_page_rows
    where page_ordinal <= (select page_size + 1 from params)
  ),
  visible_rows as materialized (
    select * from page_rows
    where page_ordinal <= (select page_size from params)
  ),
  visible_invoice_request as materialized (
    select array_agg(invoice_id order by invoice_id) invoice_ids
    from visible_rows
    where invoice_id is not null
  ),
  nonempty_visible_invoice_request as materialized (
    select invoice_ids
    from visible_invoice_request
    where cardinality(invoice_ids)>0
  ),
  visible_support as materialized (
    select
      (source.invoice_json->>'invoice_id')::uuid support_invoice_id,
      source.invoice_json
    from nonempty_visible_invoice_request request
    cross join lateral private._invoice_batch_issue_source_rows_for_ids_v2(
      request.invoice_ids,
      true,
      coalesce(p_now_utc,statement_timestamp())
    ) source
  ),
  totals as materialized (
    select
      count(*)::integer all_count,
      count(*) filter(where selectable)::integer ready_count,
      count(*) filter(where selectable and last_selection_action <> 'EXCLUDE')::integer selected_count,
      count(*) filter(where not selectable)::integer blocked_count,
      count(*) filter(where blocked_for_sending)::integer blocked_for_sending_count,
      count(*) filter(where generated_state='STALE')::integer stale_count,
      count(*) filter(where row_status='IN_PROGRESS')::integer in_progress_count
    from selection_scope_rows
  ),
  display_totals as materialized (
    select count(*)::integer display_count
    from filtered_rows
  ),
  visible_group_keys as materialized (
    select distinct group_key
    from visible_rows
    where v_mode = 'PAGE'
  ),
  page_group_rollup as materialized (
    select
      r.group_key,
      min(r.week_ending_date) week_ending_date,
      (array_agg(r.client_id order by r.selection_key))[1] client_id,
      min(r.row_status) row_status,
      (array_agg(r.candidate_ids order by r.selection_key))[1] candidate_ids,
      count(*)::integer row_total,
      count(*) filter (where r.selectable)::integer eligible_total,
      count(*) filter (
        where r.selectable and r.last_selection_action <> 'EXCLUDE'
      )::integer selected_total,
      count(*) filter (where v.group_key is not null)::integer visible_total
    from selection_scope_rows r
    join visible_group_keys g on g.group_key = r.group_key
    left join visible_rows v
      on v.group_key = r.group_key
     and v.selection_key = r.selection_key
    group by r.group_key
  ),
  page_group_selection_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'group_key',g.group_key,
      'selector',case
        when p.group_preset='STATUS_WEEK_CLIENT' then jsonb_build_object(
          'type','STATUS_WEEK_CLIENT',
          'status_code',g.row_status,
          'week_ending_date',g.week_ending_date,
          'client_id',g.client_id
        )
        when jsonb_array_length(coalesce(g.candidate_ids,'[]'::jsonb))=1
          then jsonb_build_object(
            'type','WEEK_CLIENT_CANDIDATE',
            'week_ending_date',g.week_ending_date,
            'client_id',g.client_id,
            'candidate_id',g.candidate_ids->>0
          )
        else jsonb_build_object(
          'type','WEEK_CLIENT',
          'week_ending_date',g.week_ending_date,
          'client_id',g.client_id
        )
      end,
      'eligible_total',g.eligible_total,
      'selected_total',g.selected_total,
      'state',case
        when g.eligible_total = 0 then 'DISABLED'
        when g.selected_total = 0 then 'UNCHECKED'
        when g.selected_total = g.eligible_total then 'CHECKED'
        else 'INDETERMINATE'
      end,
      'has_hidden_override',
        g.visible_total < g.row_total
        and g.selected_total not in (0,g.eligible_total)
    ) order by g.group_key),'[]'::jsonb) groups
    from page_group_rollup g
    cross join params p
  ),
  requested_group_selectors as materialized (
    select
      selector.ordinality::integer request_ordinal,
      selector.value selector,
      upper(selector.value->>'type') selector_type,
      nullif(btrim(selector.value->>'selection_key'),'') selection_key,
      case when selector.value ? 'week_ending_date'
        then (selector.value->>'week_ending_date')::date end week_ending_date,
      case when selector.value ? 'client_id'
        then (selector.value->>'client_id')::uuid end client_id,
      case when selector.value ? 'candidate_id'
        then (selector.value->>'candidate_id')::uuid end candidate_id,
      nullif(upper(btrim(selector.value->>'status_code')),'') status_code
    from jsonb_array_elements(coalesce(v_query->'group_selectors','[]'::jsonb))
      with ordinality selector(value, ordinality)
    where v_mode = 'SUMMARY'
  ),
  requested_group_members as materialized (
    select
      requested.request_ordinal,
      requested.selector,
      row_scope.*
    from requested_group_selectors requested
    join selection_scope_rows row_scope on row_scope.selectable
      and (
        (requested.selector_type='ROW'
          and requested.selection_key=row_scope.selection_key)
        or (requested.selector_type='WEEK'
          and requested.week_ending_date=row_scope.week_ending_date)
        or (requested.selector_type='CLIENT'
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='CANDIDATE' and exists (
          select 1
          from jsonb_array_elements_text(
            coalesce(row_scope.candidate_ids,'[]'::jsonb)
          ) candidate(value)
          where pg_input_is_valid(candidate.value,'uuid')
            and candidate.value::uuid=requested.candidate_id
        ))
        or (requested.selector_type='STATUS'
          and requested.status_code=row_scope.row_status)
        or (requested.selector_type='WEEK_CLIENT'
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='WEEK_CLIENT_CANDIDATE'
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id
          and exists (
            select 1
            from jsonb_array_elements_text(
              coalesce(row_scope.candidate_ids,'[]'::jsonb)
            ) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=requested.candidate_id
          ))
        or (requested.selector_type='STATUS_WEEK'
          and requested.status_code=row_scope.row_status
          and requested.week_ending_date=row_scope.week_ending_date)
        or (requested.selector_type='STATUS_WEEK_CLIENT'
          and requested.status_code=row_scope.row_status
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='DIMENSION_GROUP'
          and (requested.week_ending_date is null or requested.week_ending_date=row_scope.week_ending_date)
          and (requested.client_id is null or requested.client_id=row_scope.client_id)
          and (requested.status_code is null or requested.status_code=row_scope.row_status)
          and (requested.candidate_id is null or exists (
            select 1 from jsonb_array_elements_text(row_scope.candidate_ids) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=requested.candidate_id
          )))
      )
  ),
  requested_group_base as materialized (
    select
      requested.*,
      coalesce((
        select rule.action
        from selection_rules rule
        where exists (
          select 1
          from requested_group_members member
          where member.request_ordinal=requested.request_ordinal
        )
          and not exists (
            select 1
            from requested_group_members member
            where member.request_ordinal=requested.request_ordinal
              and (
                (rule.selector_type='ROW'
                  and rule.selection_key=member.selection_key)
                or (rule.selector_type='WEEK'
                  and rule.week_ending_date=member.week_ending_date)
                or (rule.selector_type='CLIENT'
                  and rule.client_id=member.client_id)
                or (rule.selector_type='CANDIDATE' and exists (
                  select 1
                  from jsonb_array_elements_text(
                    coalesce(member.candidate_ids,'[]'::jsonb)
                  ) candidate(value)
                  where pg_input_is_valid(candidate.value,'uuid')
                    and candidate.value::uuid=rule.candidate_id
                ))
                or (rule.selector_type='STATUS'
                  and rule.status_code=member.row_status)
                or (rule.selector_type='WEEK_CLIENT'
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id)
                or (rule.selector_type='WEEK_CLIENT_CANDIDATE'
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id
                  and exists (
                    select 1
                    from jsonb_array_elements_text(
                      coalesce(member.candidate_ids,'[]'::jsonb)
                    ) candidate(value)
                    where pg_input_is_valid(candidate.value,'uuid')
                      and candidate.value::uuid=rule.candidate_id
                  ))
                or (rule.selector_type='STATUS_WEEK'
                  and rule.status_code=member.row_status
                  and rule.week_ending_date=member.week_ending_date)
                or (rule.selector_type='STATUS_WEEK_CLIENT'
                  and rule.status_code=member.row_status
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id)
                or (rule.selector_type='DIMENSION_GROUP'
                  and (rule.week_ending_date is null or rule.week_ending_date=member.week_ending_date)
                  and (rule.client_id is null or rule.client_id=member.client_id)
                  and (rule.status_code is null or rule.status_code=member.row_status)
                  and (rule.candidate_id is null or exists (
                    select 1 from jsonb_array_elements_text(member.candidate_ids) candidate(value)
                    where pg_input_is_valid(candidate.value,'uuid')
                      and candidate.value::uuid=rule.candidate_id
                  )))
              ) is not true
          )
        order by rule.rule_sequence desc
        limit 1
      ),'INCLUDE') base_action
    from requested_group_selectors requested
  ),
  requested_group_rollup as materialized (
    select
      requested.request_ordinal,
      requested.selector,
      case when count(distinct member.group_key)=1
        then min(member.group_key) end group_key,
      count(member.selection_key)::integer eligible_total,
      count(member.selection_key) filter (
        where member.last_selection_action <> 'EXCLUDE'
      )::integer selected_total,
      coalesce(bool_or(
        member.last_selection_action is distinct from requested.base_action
      ) filter (where member.selection_key is not null),false)
        has_hidden_override
    from requested_group_base requested
    left join requested_group_members member
      on member.request_ordinal=requested.request_ordinal
    group by
      requested.request_ordinal,
      requested.selector,
      requested.base_action
  ),
  summary_group_selection_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selector',rollup.selector,
      'group_key',rollup.group_key,
      'eligible_total',rollup.eligible_total,
      'selected_total',rollup.selected_total,
      'state',case
        when rollup.eligible_total=0 then 'DISABLED'
        when rollup.selected_total=0 then 'UNCHECKED'
        when rollup.selected_total=rollup.eligible_total
          and not rollup.has_hidden_override then 'CHECKED'
        else 'INDETERMINATE'
      end,
      'has_hidden_override',rollup.has_hidden_override
    ) order by rollup.request_ordinal),'[]'::jsonb) groups
    from requested_group_rollup rollup
  ),
  group_selection_json as materialized (
    select case
      when v_mode='SUMMARY' then summary_groups.groups
      else page_groups.groups
    end groups
    from page_group_selection_json page_groups
    cross join summary_group_selection_json summary_groups
  ),
  facet_json as materialized (
    select jsonb_strip_nulls(jsonb_build_object(
      'clients',case when p.facet_kinds ? 'CLIENTS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'id',f.client_id,'label',f.label,'count',f.row_count
        ) order by f.facet_ordinal) from facet_client_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_client_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_client_values f),false) then (
          select jsonb_build_object('after_label',lower(f.label),'after_id',f.client_id)
          from facet_client_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'candidates',case when p.facet_kinds ? 'CANDIDATES' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'id',f.candidate_id,'label',f.label,'count',f.row_count
        ) order by f.facet_ordinal) from facet_candidate_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_candidate_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_candidate_values f),false) then (
          select jsonb_build_object('after_label',lower(f.label),'after_id',f.candidate_id)
          from facet_candidate_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'week_endings',case when p.facet_kinds ? 'WEEK_ENDINGS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'value',f.week_ending_date,'label',to_char(f.week_ending_date,'DD/MM/YYYY'),
          'count',f.row_count
        ) order by f.facet_ordinal) from facet_week_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_week_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_week_values f),false) then (
          select jsonb_build_object('after_value',f.week_ending_date)
          from facet_week_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'statuses',case when p.facet_kinds ? 'STATUSES' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'code',f.row_status,'label',initcap(replace(lower(f.row_status),'_',' ')),
          'count',f.row_count
        ) order by f.facet_ordinal) from facet_status_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_status_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_status_values f),false) then (
          select jsonb_build_object('after_code',f.row_status)
          from facet_status_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'blockers',case when p.facet_kinds ? 'BLOCKERS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'code',f.code,'count',f.row_count
        ) order by f.facet_ordinal) from facet_blocker_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_blocker_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_blocker_values f),false) then (
          select jsonb_build_object('after_code',f.code)
          from facet_blocker_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end
    )) facets
    from params p
  ),
  row_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selection_key',selection_key,
      'group_key',group_key,
      'invoice_id',invoice_id,
      'invoice_number',invoice_number,
      'source_revision',document_revision::text,
      'document_revision',document_revision,
      'client_id',client_id,


      'client_name',client_name,
      'candidate_ids',candidate_ids,
      'candidate_names',candidate_names,
      'candidate_display',candidate_display,
      'week_ending_dates',week_ending_dates,
      'week_ending_date',week_ending_date,
      'week_ending_display',case when jsonb_array_length(week_ending_dates)>1 then 'Multiple weeks' else to_char(week_ending_date,'DD/MM/YYYY') end,
      'currency',currency,
      'invoice_stream',invoice_stream,
      'total_ex_vat',total_ex_vat,
      'vat_amount',vat_amount,
      'total_inc_vat',total_inc_vat,
      'generated_state',generated_state,
      'row_status',row_status,
      'is_early',is_early,
      'selectable',selectable,
      'selected',selectable and last_selection_action <> 'EXCLUDE',
      'issue_blocker_codes',coalesce(issue_blocker_codes,'[]'::jsonb),
      'delivery_blocker_codes',coalesce(delivery_blocker_codes,'[]'::jsonb),
      'informational_codes',coalesce(informational_codes,'[]'::jsonb),
      'blocked_for_sending',blocked_for_sending,
      'can_issue_only',can_issue_only,
      'can_issue_and_deliver',can_issue_and_deliver,
      'active_issue_operation_id',active_issue_operation_id_text,
      'active_issue_status',active_issue_operation->>'status',
      'active_issue_operation',active_issue_operation,
      'active_document_operation_id',active_document_operation_id_text,
      'validation_detail',validation_detail,
      'support_readiness',coalesce(
        support.invoice_json->'support_readiness',
        visible_rows.support_readiness
      ),
      'sort_tuple',jsonb_build_object(
        'sort_date',case when sort_date_key is not null then sort_date_key::text end,
        'sort_text',sort_text_key,
        'sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end,
        'selection_key',selection_key
      )
    ) order by page_ordinal),'[]'::jsonb) rows
    from visible_rows
    left join visible_support support
      on support.support_invoice_id=visible_rows.invoice_id
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_CANDIDATES_V2',
    'action','ISSUE',
    'mode',v_mode,
    'snapshot',v_snapshot,
    'normalised_filter',v_filters,
    'normalised_sort',v_sort,
    'filter_hash',v_filter_hash,
    'query_hash',v_query_hash,
    'selection_hash',v_selection_hash,
    'rows',(select rows from row_json),
    'page',jsonb_build_object(
      'page_size',v_page_size,
      'returned_count',(select count(*) from visible_rows),
      'total_count',case
        when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
          then coalesce((select max(k.full_scope_count) from candidate_keys k),0)
        else (select display_count from display_totals)
      end,
      'has_more',case
        when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
          then exists (
            select 1
            from candidate_keys k
            where k.page_ordinal>v_page_size
          )
        else (select count(*) from page_rows)>v_page_size
      end,
      'next_cursor_values',case when (
        case
          when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
            then exists (
              select 1
              from candidate_keys k
              where k.page_ordinal>v_page_size
            )
          else (select count(*) from page_rows)>v_page_size
        end
      ) then (
        select case when v_mode='EXPAND_SELECTION'
          then jsonb_build_object('after_selection_key',selection_key)
          else jsonb_build_object(
            'after_selection_key',selection_key,
            'after_sort_date',case when sort_date_key is not null then sort_date_key::text end,
            'after_sort_text',sort_text_key,
            'after_sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end
          )
        end
        from (
          select
            selection_key,
            sort_date_key,
            sort_text_key,
            sort_numeric_key,
            page_ordinal
          from candidate_keys
          where page_ordinal<=v_page_size
        ) cursor_source
        order by page_ordinal desc
        limit 1
      ) else null end
    ),
    'totals',jsonb_build_object(
      'all',(select all_count from totals),
      'filtered_total',(select all_count from totals),
      'display_total',(select display_count from display_totals),
      'eligible_total',(select ready_count from totals),
      'selected_total',(select selected_count from totals),
      'excluded_total',(select ready_count-selected_count from totals),
      'ready',(select ready_count from totals),
      'blocked',(select blocked_count from totals),
      'blocked_total',(select blocked_count from totals),
      'blocked_for_sending',(select blocked_for_sending_count from totals),
      'stale',(select stale_count from totals),
      'in_progress',(select in_progress_count from totals)
    ),
    'selection_summary',jsonb_build_object(
      'eligible_total',(select ready_count from totals),
      'selected_total',(select selected_count from totals),
      'excluded_total',(select ready_count-selected_count from totals),
      'blocked_total',(select blocked_count from totals),
      'exact',v_mode in ('FACETS','SUMMARY')
    ),
    'group_selection',(select groups from group_selection_json),
    'facets',case
      when v_mode='FACETS' then (select facets from facet_json)
      else jsonb_build_object()
    end,
    'selection_seed',jsonb_build_object('mode','IMPLICIT_ALL','default_selected',true)
  ) into v_result;

  if v_mode='SUMMARY'
     and coalesce((v_result#>>'{totals,filtered_total}')::integer,0)>25000 then
    raise exception using
      errcode='54000',
      message='BATCH_SUMMARY_SCOPE_TOO_LARGE';
  end if;

  if v_mode='EXPLICIT_KEYS' and (
    jsonb_array_length(coalesce(v_result->'rows','[]'::jsonb))
      <> jsonb_array_length(v_selection_keys)
    or exists (
      select 1
      from jsonb_array_elements(v_result->'rows') row_item(row_json)
      where coalesce(row_json->>'source_revision','')
        is distinct from coalesce(
          v_expected_source_revisions->>(row_json->>'selection_key'),
          ''
        )
    )
  ) then
    raise exception using
      errcode='40001',
      message='BATCH_SOURCE_CHANGED';
  end if;

  v_snapshot_after := private._invoice_candidate_snapshot_verify_v2(
    'ISSUE',
    v_snapshot,
    coalesce(p_now_utc,now())
  );
  if v_snapshot_after->>'revision' <> v_snapshot->>'revision' then
    raise exception using errcode='40001', message='BATCH_SNAPSHOT_CHANGED';
  end if;

  return v_result;
end;
$function$;

-- private._invoice_batch_issue_candidates_legacy_20260726(boolean,integer)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_candidates_legacy_20260726(p_allow_early boolean DEFAULT false, p_limit integer DEFAULT 2000)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with
anchor as materialized (
  select (now() at time zone 'Europe/London')::date today,
    greatest(1,least(coalesce(p_limit,2000),20000)) row_limit
),
base as materialized (
  select i.*,c.name client_name,c.primary_invoice_email,
    case when coalesce(i.header_snapshot_json#>>'{meta,invoice_week_start}','')
      ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date end
      invoice_week_start,
    lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
      i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
      is_self_bill
  from public.invoices i
  join public.clients c on c.id=i.client_id
  where i.type::text='INVOICE' and i.status::text in('DRAFT','ON_HOLD')
  order by i.created_at desc nulls last,i.id
  limit (select row_limit from anchor)
),
validation_requests as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','candidate:'||b.id::text,
    'invoice_id',b.id,'expected_revision',b.document_revision,
    'allow_early',coalesce(p_allow_early,false),'deliver',true)
    order by b.id),'[]'::jsonb) commands
  from base b
),
validations as materialized (
  select v.*
  from validation_requests r
  cross join lateral private._invoice_issue_validate_batch(
    r.commands,(select today from anchor)) v
),
source_timesheets as materialized (
  select distinct l.invoice_id,l.timesheet_id
  from public.invoice_lines l
  join base b on b.id=l.invoice_id
  where l.timesheet_id is not null
),
timesheet_support as materialized (
  select s.invoice_id,s.timesheet_id,t.submission_mode,
    coalesce(pc.effective_ts_attach_to_invoice,true)
      and not coalesce(summary.client_no_timesheet_required,false)
      and not coalesce(summary.client_is_nhsp,false) required,
    ma.status manual_asset_state,
    coalesce(ma.normalised_page_count,0) manual_asset_pages,
    dv.status timesheet_document_state,
    coalesce(dv.page_count,0) timesheet_document_pages,
    case when dv.status<>'READY' then dv.operation_id end
      active_timesheet_document_operation_id,
    upper(coalesce(t.submission_mode::text,'')) in('MANUAL','QR') is_manual
  from source_timesheets s
  left join public.timesheets t
    on t.timesheet_id=s.timesheet_id and t.is_current
  left join public.v_ts_invoice_precheck pc on pc.timesheet_id=s.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join lateral (
    select ev.document_asset_id
    from public.timesheet_evidence ev
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=ev.document_asset_id
    where ev.timesheet_id=t.timesheet_id
      and upper(coalesce(ev.kind,''))='TIMESHEET'
      and coalesce(ev.processing_state,'')<>'SUPERSEDED'
    order by(ev.document_asset_id=t.manual_document_asset_id) desc,
      (candidate_asset.status='READY') desc,
      ev.created_at desc nulls last,ev.id desc
    limit 1
  ) manual_source on true
  left join public.invoice_document_assets ma
    on ma.id=coalesce(t.manual_document_asset_id,
      manual_source.document_asset_id)
  left join lateral (
    select v.*
    from public.invoice_document_versions v
    where v.entity_type='TIMESHEET' and v.entity_id=t.timesheet_id
      and v.purpose='TIMESHEET'
      and v.source_revision=t.document_revision::text
      and v.template_version='timesheet-professional-v1'
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
        'VERIFYING','READY','FAILED','SUPERSEDED','CANCELLED')
    order by
      (v.status='READY') desc,
      (v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING')) desc,
      v.created_at_utc desc,v.id desc
    limit 1
  ) dv on true
),
timesheet_support_agg as materialized (
  select b.id invoice_id,
    count(*) filter(where t.timesheet_id is not null and t.required
      and t.is_manual)::integer manual_count,
    count(*) filter(where t.timesheet_id is not null and t.required
      and not t.is_manual)::integer electronic_count,
    count(*) filter(where t.timesheet_id is not null and t.required
      and coalesce(t.timesheet_document_state,'NOT_READY')<>'READY')::integer
      timesheet_not_ready_count,
    coalesce(sum(t.timesheet_document_pages)
      filter(where t.required),0)::integer timesheet_pages,
    coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',t.timesheet_id,
      'required',t.required,
      'submission_mode',coalesce(t.submission_mode::text,''),
      'manual_asset_state',t.manual_asset_state,
      'manual_asset_pages',t.manual_asset_pages,
      'timesheet_document_state',t.timesheet_document_state,
      'timesheet_document_pages',t.timesheet_document_pages,
      'active_timesheet_document_operation_id',
        t.active_timesheet_document_operation_id)
      order by t.timesheet_id)
      filter(where t.timesheet_id is not null),'[]'::jsonb)
      timesheet_support_rows
  from base b
  left join timesheet_support t on t.invoice_id=b.id
  group by b.id
),
evidence_economics as materialized (
  select l.invoice_id,l.timesheet_id,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) in(
        'EXPENSE_MILEAGE','MILEAGE')
      or coalesce(l.source_key,'') like '%:MILEAGE') mileage_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%TRAVEL%') travel_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%ACCOMMODATION%') accommodation_required,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) like 'EXPENSE_%'
      and upper(coalesce(l.meta_json->>'line_type','')) not in(
        'EXPENSE_MILEAGE','EXPENSE_TRAVEL','EXPENSE_ACCOMMODATION'))
      general_expense_required
  from public.invoice_lines l
  join base b on b.id=l.invoice_id
  where l.timesheet_id is not null
  group by l.invoice_id,l.timesheet_id
),
evidence_rows as materialized (
  select distinct s.invoice_id,e.id evidence_id,e.timesheet_id,
    upper(coalesce(e.kind,'')) kind,e.document_asset_id,a.status,
    coalesce(a.normalised_page_count,0) pages,
    case
      when upper(coalesce(e.kind,''))='TIMESHEET'
        then coalesce(pc.effective_ts_attach_to_invoice,true)
          and not coalesce(summary.client_no_timesheet_required,false)
          and not coalesce(summary.client_is_nhsp,false)
      when upper(coalesce(e.kind,''))='MILEAGE'
        then coalesce(econ.mileage_required,false)
      when upper(coalesce(e.kind,''))='TRAVEL'
        then coalesce(econ.travel_required,false)
      when upper(coalesce(e.kind,''))='ACCOMMODATION'
        then coalesce(econ.accommodation_required,false)
      when upper(coalesce(e.kind,'')) in('OTHER','EXPENSE','EXPENSES')
        then coalesce(econ.general_expense_required,false)
      else false
    end required
  from source_timesheets s
  join public.timesheet_evidence e on e.timesheet_id=s.timesheet_id
  left join public.v_ts_invoice_precheck pc on pc.timesheet_id=s.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join evidence_economics econ
    on econ.invoice_id=s.invoice_id and econ.timesheet_id=s.timesheet_id
  left join public.invoice_document_assets a on a.id=e.document_asset_id
),
evidence_agg as materialized (
  select b.id invoice_id,
    count(e.evidence_id) filter(where e.required)::integer evidence_count,
    count(*) filter(where e.required and e.evidence_id is not null
      and e.document_asset_id is null)::integer unregistered_count,
    count(*) filter(where e.required and e.evidence_id is not null
      and e.document_asset_id is not null
      and coalesce(e.status,'DISCOVERED') not in(
        'READY','UNSUPPORTED','CORRUPT','MISSING','FAILED'))::integer not_ready_count,
    count(*) filter(where e.required
      and e.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED'))::integer failed_count,
    coalesce(sum(e.pages) filter(where e.required),0)::integer evidence_pages
  from base b left join evidence_rows e on e.invoice_id=b.id
  group by b.id
),
hr_support as materialized (
  select b.id invoice_id,
    count(h.source_system) filter(
      where upper(coalesce(h.source_system,''))='HEALTHROSTER')::integer
      healthroster_count,
    count(h.source_system) filter(
      where upper(coalesce(h.source_system,''))='NHSP')::integer nhsp_count
  from base b
  left join public.invoice_hr_source_rows h on h.invoice_id=b.id
  group by b.id
),
line_flags as materialized (
  select b.id invoice_id,count(l.id)::integer line_count,
    coalesce(bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%HIGHER_RATE%'),false) higher_rate_required
  from base b left join public.invoice_lines l on l.invoice_id=b.id
  group by b.id
),
active_issue as materialized (
  select distinct on(c.entity_id) c.entity_id invoice_id,c.operation_id,
    c.id chunk_id,c.status,c.phase,c.progress_json,c.error_json,o.change_seq
  from public.invoice_operation_chunks c
  join public.invoice_operations o on o.id=c.operation_id
  join base b on b.id=c.entity_id
  where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
  order by c.entity_id,c.updated_at_utc desc,c.id desc
),
evaluated as materialized (
  select b.*,b.invoice_week_start+6 week_ending_date,
    coalesce(v.hard_blocker_codes,'[]'::jsonb) blocker_codes,
    coalesce(v.warning_codes,'[]'::jsonb) routing_warnings,
    coalesce(v.document_dependency_codes,'[]'::jsonb)
      document_dependency_codes,
    coalesce(v.delivery_blocker_codes,'[]'::jsonb)
      delivery_blocker_codes,
    coalesce(v.can_issue_only,false) can_issue_only,
    coalesce(v.can_issue_and_deliver,false) can_issue_and_deliver,
    v.detail_json validation_detail,
    v.route_policy_result->'canonical_to' recipient,
    ts.*,ev.*,hr.*,lf.*,
    ai.operation_id active_issue_operation_id_resolved,
    ai.chunk_id active_issue_chunk_id,ai.status active_issue_status,
    ai.phase active_issue_phase,ai.progress_json active_issue_progress,
    ai.error_json active_issue_error,ai.change_seq active_issue_change_seq
  from base b
  left join validations v
    on v.request_key='candidate:'||b.id::text and v.invoice_id=b.id
  join timesheet_support_agg ts on ts.invoice_id=b.id
  join evidence_agg ev on ev.invoice_id=b.id
  join hr_support hr on hr.invoice_id=b.id
  join line_flags lf on lf.invoice_id=b.id
  left join active_issue ai on ai.invoice_id=b.id
),
weeks as materialized (
  select e.client_id,max(e.client_name) client_name,e.invoice_week_start,
    e.week_ending_date,round(sum(e.subtotal_ex_vat),2) subtotal_ex_vat_sum,
    round(sum(e.total_inc_vat),2) total_inc_vat_sum,
    jsonb_agg(jsonb_build_object(
      'invoice_id',e.id,'invoice_no',e.invoice_no,'status',e.status,
      'on_hold_reason',e.on_hold_reason,
      'subtotal_ex_vat',round(e.subtotal_ex_vat,2),
      'vat_amount',round(e.vat_amount,2),
      'total_inc_vat',round(e.total_inc_vat,2),
      'is_self_bill',e.is_self_bill,'do_not_send',e.do_not_send,
      'document_revision',e.document_revision,
      'preview_document_state',e.document_state,
      'stable_blocker_codes',e.blocker_codes,
      'document_dependency_codes',e.document_dependency_codes,
      'delivery_blocker_codes',e.delivery_blocker_codes,
      'can_issue_only',e.can_issue_only,
      'can_issue_and_deliver',e.can_issue_and_deliver,
      'validation_detail',e.validation_detail,
      'estimated_supporting_page_count',
        e.evidence_pages+e.timesheet_pages+e.healthroster_count+e.nhsp_count,
      'support_readiness',jsonb_build_object(
        'manual_timesheet_count',e.manual_count,
        'electronic_timesheet_count',e.electronic_count,
        'timesheet_not_ready_count',e.timesheet_not_ready_count,
        'timesheets',e.timesheet_support_rows,
        'evidence_count',e.evidence_count,
        'unregistered_asset_count',e.unregistered_count,
        'not_ready_asset_count',e.not_ready_count,
        'failed_asset_count',e.failed_count,
        'healthroster_count',e.healthroster_count,
        'nhsp_count',e.nhsp_count,
        'higher_rate_required',e.higher_rate_required),
      'recipient_ready',not exists(
        select 1 from jsonb_array_elements_text(
          e.delivery_blocker_codes) code(value)
          where code.value in('MISSING_RECIPIENT','CONTRACT_MANUAL_EMAIL_MISSING',
            'CLIENT_MANUAL_EMAIL_MISSING','CONTRACT_MANUAL_EMAIL_CONFLICT',
            'INVALID_TO_RECIPIENT','INVALID_CC_RECIPIENT',
            'INVALID_BCC_RECIPIENT')),
      'recipient',e.recipient,
      'recipient_routing_warnings',e.routing_warnings,
      'active_issue_operation_id',e.active_issue_operation_id_resolved,
      'active_issue_operation',case when e.active_issue_operation_id_resolved is not null
        then jsonb_build_object(
          'id',e.active_issue_operation_id_resolved,
          'chunk_id',e.active_issue_chunk_id,'status',e.active_issue_status,
          'phase',e.active_issue_phase,'progress',e.active_issue_progress,
          'error',e.active_issue_error,'change_seq',e.active_issue_change_seq) end,
      'active_document_operation_id',e.active_document_operation_id,
      'last_issue_error',e.active_issue_error,
      'last_document_error',e.last_document_error_json)
      order by e.status desc,e.invoice_no nulls last,e.id) invoices
  from evaluated e
  group by e.client_id,e.invoice_week_start,e.week_ending_date
),
clients as (
  select w.client_id,max(w.client_name) client_name,
    jsonb_agg(jsonb_build_object(
      'invoice_week_start',w.invoice_week_start,
      'week_ending_date',w.week_ending_date,
      'subtotal_ex_vat_sum',w.subtotal_ex_vat_sum,
      'total_inc_vat_sum',w.total_inc_vat_sum,
      'invoices',w.invoices)
      order by w.week_ending_date desc nulls last) weeks
  from weeks w group by w.client_id
)
select coalesce(jsonb_agg(jsonb_build_object(
  'client_id',c.client_id,'client_name',c.client_name,'weeks',c.weeks)
  order by c.client_name nulls last,c.client_id),'[]'::jsonb)
from clients c;
$function$;

-- private._invoice_batch_issue_classification_v2(boolean,uuid[],timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_classification_v2(p_allow_early boolean DEFAULT false, p_invoice_ids uuid[] DEFAULT NULL::uuid[], p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(selection_key text, candidate_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with
anchor as materialized (
  select (
    coalesce(p_now_utc,statement_timestamp())
    at time zone 'Europe/London'
  )::date today
),
base as materialized (
  select
    invoice.*,
    client.name client_name,
    case
      when pg_input_is_valid(
        coalesce(
          invoice.header_snapshot_json#>>
            '{meta,invoice_week_start}',
          ''
        ),
        'date'
      )
        then (
          invoice.header_snapshot_json#>>
            '{meta,invoice_week_start}'
        )::date
    end invoice_week_start,
    lower(coalesce(
      invoice.header_snapshot_json#>>'{meta,self_bill}',
      invoice.header_snapshot_json->>'self_bill',
      'false'
    )) in('true','t','1','yes') is_self_bill
  from public.invoices invoice
  join public.clients client on client.id=invoice.client_id
  where invoice.type::text='INVOICE'
    and invoice.status::text in('DRAFT','ON_HOLD')
    and (
      p_invoice_ids is null
      or invoice.id=any(p_invoice_ids)
    )
),
validation_requests as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','candidate:'||invoice.id::text,
    'invoice_id',invoice.id,
    'expected_revision',invoice.document_revision,
    'allow_early',coalesce(p_allow_early,false),
    'deliver',true
  ) order by invoice.id),'[]'::jsonb) commands
  from base invoice
),
validations as materialized (
  select validation.*
  from validation_requests request
  cross join lateral private._invoice_issue_validate_batch(
    request.commands,
    (select today from anchor)
  ) validation
),
candidate_projection as materialized (
  select
    invoice.id invoice_id,
    coalesce(jsonb_agg(
      distinct to_jsonb(summary.candidate_id)
      order by to_jsonb(summary.candidate_id)
    )
      filter(where summary.candidate_id is not null),'[]'::jsonb)
      candidate_ids,
    coalesce(jsonb_agg(
      distinct to_jsonb(summary.candidate_name)
      order by to_jsonb(summary.candidate_name)
    )
      filter(where nullif(summary.candidate_name,'') is not null),
      '[]'::jsonb) candidate_names,
    coalesce(jsonb_agg(distinct to_jsonb(summary.week_ending_date)
      order by to_jsonb(summary.week_ending_date))
      filter(where summary.week_ending_date is not null),
      '[]'::jsonb) week_ending_dates,
    min(summary.week_ending_date) min_week_ending,
    max(summary.week_ending_date) max_week_ending
  from base invoice
  left join public.invoice_lines line on line.invoice_id=invoice.id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=line.timesheet_id
  group by invoice.id
),
active_issue as materialized (
  select distinct on(chunk.entity_id)
    chunk.entity_id invoice_id,
    chunk.operation_id,
    chunk.id chunk_id,
    chunk.status,
    chunk.phase,
    chunk.progress_json,
    chunk.error_json,
    operation.change_seq
  from public.invoice_operation_chunks chunk
  join public.invoice_operations operation
    on operation.id=chunk.operation_id
  join base invoice on invoice.id=chunk.entity_id
  where chunk.chunk_type='ISSUE_INVOICE'
    and chunk.entity_type='INVOICE'
    and chunk.status in(
      'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
    )
    and operation.status in(
      'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
    )
    and coalesce(
      chunk.payload_json->>'is_selection_expander',
      'false'
    )<>'true'
    and (not chunk.is_manifest_member or chunk.manifest_committed)
    and (
      not chunk.is_manifest_member
      or coalesce(chunk.entity_type,'')<>'OPERATION'
    )
  order by
    chunk.entity_id,
    chunk.updated_at_utc desc,
    chunk.id desc
),
classified as materialized (
  select
    invoice.*,
    invoice.invoice_week_start+6 week_ending_date,
    coalesce(validation.hard_blocker_codes,'[]'::jsonb)
      hard_blocker_codes,
    coalesce(validation.warning_codes,'[]'::jsonb) warning_codes,
    coalesce(validation.document_dependency_codes,'[]'::jsonb)
      document_dependency_codes,
    coalesce(validation.delivery_blocker_codes,'[]'::jsonb)
      delivery_blocker_codes,
    coalesce(validation.can_issue_only,false) can_issue_only,
    coalesce(validation.can_issue_and_deliver,false)
      can_issue_and_deliver,
    validation.detail_json validation_detail,
    validation.route_policy_result->'canonical_to' recipient,
    projection.candidate_ids,
    projection.candidate_names,
    projection.week_ending_dates,
    projection.min_week_ending,
    projection.max_week_ending,
    active.operation_id current_issue_operation_id,
    active.chunk_id current_issue_chunk_id,
    active.status current_issue_status,
    active.phase current_issue_phase,
    active.progress_json current_issue_progress,
    active.error_json current_issue_error,
    active.change_seq current_issue_change_seq,
    case
      when exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='INVOICE'
          and version.entity_id=invoice.id
          and version.purpose='DRAFT_PREVIEW'
          and version.source_revision=
            invoice.document_revision::text
          and version.template_version='invoice-professional-v2'
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      )
        then 'FRESH'
      when exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='INVOICE'
          and version.entity_id=invoice.id
          and version.purpose='DRAFT_PREVIEW'
          and version.template_version='invoice-professional-v2'
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      )
        then case
          when invoice.active_document_operation_id is not null
            then 'ACTIVE'
          when upper(coalesce(invoice.document_state,''))='FAILED'
            or invoice.last_document_error_json is not null
            then 'FAILED'
          else 'STALE'
        end
      else 'NEVER_GENERATED'
    end generated_state
  from base invoice
  left join validations validation
    on validation.request_key='candidate:'||invoice.id::text
   and validation.invoice_id=invoice.id
  join candidate_projection projection
    on projection.invoice_id=invoice.id
  left join active_issue active on active.invoice_id=invoice.id
),
candidate_state as materialized (
  select
    classified.*,
    coalesce(
      jsonb_array_length(classified.delivery_blocker_codes)>0
      and classified.can_issue_only
      and not classified.can_issue_and_deliver,
      false
    ) blocked_for_sending,
    (
      classified.hard_blocker_codes
    ) issue_blocker_codes,
    (
      case
        when jsonb_array_length(
          classified.delivery_blocker_codes
        )>0
         and classified.can_issue_only
         and not classified.can_issue_and_deliver
          then jsonb_build_array('BLOCKED_FOR_SENDING')
        else '[]'::jsonb
      end
      ||classified.document_dependency_codes
      ||classified.warning_codes
    ) informational_codes
  from classified
),
final_rows as materialized (
  select
    state.*,
    (
      state.can_issue_only
      and jsonb_array_length(state.hard_blocker_codes)=0
      and state.current_issue_operation_id is null
    ) selectable,
    case
      when state.current_issue_operation_id is not null
        then 'IN_PROGRESS'
      when not state.can_issue_only
        or jsonb_array_length(state.hard_blocker_codes)>0
        then 'BLOCKED'
      else 'READY'
    end row_status
  from candidate_state state
  where state.status::text in('DRAFT','ON_HOLD')
    and nullif(state.invoice_no,'') is not null
)
select
  'invoice:'||invoice.id::text selection_key,
  jsonb_build_object(
    'selection_key','invoice:'||invoice.id::text,
    'invoice_id',invoice.id,
    'invoice_number',invoice.invoice_no,
    'source_revision',invoice.document_revision::text,
    'document_revision',invoice.document_revision,
    'client_id',invoice.client_id,
    'client_name',invoice.client_name,
    'candidate_ids',invoice.candidate_ids,
    'candidate_names',invoice.candidate_names,
    'candidate_display',case
      when jsonb_array_length(invoice.candidate_names)=1
        then invoice.candidate_names->>0
      when jsonb_array_length(invoice.candidate_names)>1
        then 'Multiple candidates ('||
          jsonb_array_length(invoice.candidate_names)::text||
          ')'
      else 'Unknown candidate'
    end,
    'week_ending_dates',case
      when jsonb_array_length(invoice.week_ending_dates)>0
        then invoice.week_ending_dates
      else jsonb_build_array(invoice.week_ending_date)
    end,
    'week_ending_date',coalesce(
      invoice.min_week_ending,
      invoice.week_ending_date
    ),
    'currency',coalesce(
      nullif(invoice.header_snapshot_json#>>'{meta,currency}',''),
      nullif(invoice.header_snapshot_json->>'currency',''),
      'GBP'
    ),
    'invoice_stream',upper(coalesce(
      nullif(
        invoice.header_snapshot_json#>>'{meta,invoice_stream}',
        ''
      ),
      nullif(invoice.header_snapshot_json->>'invoice_stream',''),
      case when invoice.is_self_bill then 'SELF_BILL' end,
      'NORMAL'
    )),
    'total_ex_vat',round(coalesce(invoice.subtotal_ex_vat,0),2),
    'vat_amount',round(coalesce(invoice.vat_amount,0),2),
    'total_inc_vat',round(coalesce(
      invoice.total_inc_vat,
      coalesce(invoice.subtotal_ex_vat,0)
        +coalesce(invoice.vat_amount,0)
    ),2),
    'preview_document_state',
      upper(coalesce(invoice.document_state,'')),
    'invoice_status',upper(coalesce(invoice.status::text,'')),
    'hard_blocker_codes',invoice.hard_blocker_codes,
    'document_dependency_codes',
      invoice.document_dependency_codes,
    'delivery_blocker_codes',invoice.delivery_blocker_codes,
    'warning_codes',invoice.warning_codes,
    'issue_blocker_codes',invoice.issue_blocker_codes,
    'informational_codes',invoice.informational_codes,
    'can_issue_only',invoice.can_issue_only,
    'can_issue_and_deliver',invoice.can_issue_and_deliver,
    'validation_detail',invoice.validation_detail,
    'support_readiness',null,
    'generated_state',invoice.generated_state,
    'blocked_for_sending',invoice.blocked_for_sending,
    'selectable',invoice.selectable,
    'row_status',invoice.row_status,
    'is_early',coalesce(
      invoice.max_week_ending,
      invoice.week_ending_date
    ) >= (select today from anchor),
    'active_issue_operation_id',invoice.current_issue_operation_id,
    'active_issue_operation',case
      when invoice.current_issue_operation_id is not null
        then jsonb_build_object(
          'id',invoice.current_issue_operation_id,
          'chunk_id',invoice.current_issue_chunk_id,
          'status',invoice.current_issue_status,
          'phase',invoice.current_issue_phase,
          'progress',invoice.current_issue_progress,
          'error',invoice.current_issue_error,
          'change_seq',invoice.current_issue_change_seq
        )
    end,
    'active_document_operation_id',
      invoice.active_document_operation_id,
    'last_issue_error',invoice.current_issue_error,
    'last_document_error',invoice.last_document_error_json,
    '_private',jsonb_build_object(
      'recipient',invoice.recipient,
      'routing_warnings',invoice.warning_codes
    )
  ) candidate_json
from final_rows invoice
order by invoice.client_name nulls last,
  invoice.week_ending_date desc nulls last,
  invoice.invoice_no nulls last,
  invoice.id;
$function$;

-- private._invoice_batch_issue_source_rows_core_v2(boolean,integer,timestamp with time zone,uuid[])
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_source_rows_core_v2(p_allow_early boolean DEFAULT false, p_limit integer DEFAULT NULL::integer, p_now_utc timestamp with time zone DEFAULT now(), p_invoice_ids uuid[] DEFAULT NULL::uuid[])
 RETURNS TABLE(client_id uuid, client_name text, invoice_week_start date, week_ending_date date, invoice_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with
classified as materialized (
  select
    candidate.candidate_json,
    (candidate.candidate_json->>'invoice_id')::uuid invoice_id
  from private._invoice_batch_issue_classification_v2(
    p_allow_early,
    p_invoice_ids,
    p_now_utc
  ) candidate
  order by
    candidate.candidate_json->>'client_name',
    candidate.candidate_json->>'week_ending_date' desc,
    candidate.candidate_json->>'invoice_number',
    candidate.candidate_json->>'invoice_id'
  limit case
    when p_invoice_ids is not null then 250
    when p_limit is null then null
    else greatest(1,p_limit)
  end
),
base as materialized (
  select
    classified.*,
    invoice.header_snapshot_json,
    invoice.do_not_send,
    invoice.on_hold_reason,
    invoice.active_document_operation_id,
    invoice.last_document_error_json,
    case
      when pg_input_is_valid(
        coalesce(
          invoice.header_snapshot_json#>>
            '{meta,invoice_week_start}',
          ''
        ),
        'date'
      )
        then (
          invoice.header_snapshot_json#>>
            '{meta,invoice_week_start}'
        )::date
    end invoice_week_start
  from classified
  join public.invoices invoice on invoice.id=classified.invoice_id
),
source_timesheets as materialized (
  select distinct line.invoice_id,line.timesheet_id
  from public.invoice_lines line
  join base invoice on invoice.invoice_id=line.invoice_id
  where line.timesheet_id is not null
),
timesheet_support as materialized (
  select
    source.invoice_id,
    source.timesheet_id,
    timesheet.submission_mode,
    coalesce(precheck.effective_ts_attach_to_invoice,true)
      and not coalesce(summary.client_no_timesheet_required,false)
      and not coalesce(summary.client_is_nhsp,false) required,
    manual_asset.status manual_asset_state,
    coalesce(manual_asset.normalised_page_count,0)
      manual_asset_pages,
    version.status timesheet_document_state,
    coalesce(version.page_count,0) timesheet_document_pages,
    case
      when version.status<>'READY' then version.operation_id
    end active_timesheet_document_operation_id,
    upper(coalesce(timesheet.submission_mode::text,''))
      in('MANUAL','QR') is_manual
  from source_timesheets source
  left join public.timesheets timesheet
    on timesheet.timesheet_id=source.timesheet_id
   and timesheet.is_current
  left join public.v_ts_invoice_precheck precheck
    on precheck.timesheet_id=source.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=source.timesheet_id
  left join lateral (
    select evidence.document_asset_id
    from public.timesheet_evidence evidence
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=evidence.document_asset_id
    where evidence.timesheet_id=timesheet.timesheet_id
      and upper(coalesce(evidence.kind,''))='TIMESHEET'
      and coalesce(evidence.processing_state,'')<>'SUPERSEDED'
    order by
      (
        evidence.document_asset_id=
          timesheet.manual_document_asset_id
      ) desc,
      (candidate_asset.status='READY') desc,
      evidence.created_at desc nulls last,
      evidence.id desc
    limit 1
  ) manual_source on true
  left join public.invoice_document_assets manual_asset
    on manual_asset.id=coalesce(
      timesheet.manual_document_asset_id,
      manual_source.document_asset_id
    )
  left join lateral (
    select candidate_version.*
    from public.invoice_document_versions candidate_version
    where candidate_version.entity_type='TIMESHEET'
      and candidate_version.entity_id=timesheet.timesheet_id
      and candidate_version.purpose='TIMESHEET'
      and candidate_version.source_revision=
        timesheet.document_revision::text
      and candidate_version.template_version=
        'timesheet-professional-v1'
      and candidate_version.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','READY','FAILED',
        'SUPERSEDED','CANCELLED'
      )
    order by
      (candidate_version.status='READY') desc,
      (
        candidate_version.status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING'
        )
      ) desc,
      candidate_version.created_at_utc desc,
      candidate_version.id desc
    limit 1
  ) version on true
),
timesheet_support_agg as materialized (
  select
    invoice.invoice_id,
    count(*) filter(
      where support.timesheet_id is not null
        and support.required
        and support.is_manual
    )::integer manual_count,
    count(*) filter(
      where support.timesheet_id is not null
        and support.required
        and not support.is_manual
    )::integer electronic_count,
    count(*) filter(
      where support.timesheet_id is not null
        and support.required
        and coalesce(
          support.timesheet_document_state,
          'NOT_READY'
        )<>'READY'
    )::integer timesheet_not_ready_count,
    coalesce(sum(support.timesheet_document_pages)
      filter(where support.required),0)::integer timesheet_pages,
    coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',support.timesheet_id,
      'required',support.required,
      'submission_mode',coalesce(
        support.submission_mode::text,
        ''
      ),
      'manual_asset_state',support.manual_asset_state,
      'manual_asset_pages',support.manual_asset_pages,
      'timesheet_document_state',
        support.timesheet_document_state,
      'timesheet_document_pages',
        support.timesheet_document_pages,
      'active_timesheet_document_operation_id',
        support.active_timesheet_document_operation_id
    ) order by support.timesheet_id)
      filter(where support.timesheet_id is not null),
      '[]'::jsonb) timesheet_support_rows
  from base invoice
  left join timesheet_support support
    on support.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
),
evidence_economics as materialized (
  select
    line.invoice_id,
    line.timesheet_id,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type','')) in(
        'EXPENSE_MILEAGE','MILEAGE'
      )
      or coalesce(line.source_key,'') like '%:MILEAGE'
    ) mileage_required,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like '%TRAVEL%'
    ) travel_required,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like '%ACCOMMODATION%'
    ) accommodation_required,
    bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like 'EXPENSE_%'
      and upper(coalesce(line.meta_json->>'line_type',''))
        not in(
          'EXPENSE_MILEAGE','EXPENSE_TRAVEL',
          'EXPENSE_ACCOMMODATION'
        )
    ) general_expense_required
  from public.invoice_lines line
  join base invoice on invoice.invoice_id=line.invoice_id
  where line.timesheet_id is not null
  group by line.invoice_id,line.timesheet_id
),
evidence_rows as materialized (
  select distinct
    source.invoice_id,
    evidence.id evidence_id,
    evidence.timesheet_id,
    upper(coalesce(evidence.kind,'')) kind,
    evidence.document_asset_id,
    asset.status,
    coalesce(asset.normalised_page_count,0) pages,
    case
      when upper(coalesce(evidence.kind,''))='TIMESHEET'
        then coalesce(precheck.effective_ts_attach_to_invoice,true)
          and not coalesce(
            summary.client_no_timesheet_required,
            false
          )
          and not coalesce(summary.client_is_nhsp,false)
      when upper(coalesce(evidence.kind,''))='MILEAGE'
        then coalesce(economics.mileage_required,false)
      when upper(coalesce(evidence.kind,''))='TRAVEL'
        then coalesce(economics.travel_required,false)
      when upper(coalesce(evidence.kind,''))='ACCOMMODATION'
        then coalesce(economics.accommodation_required,false)
      when upper(coalesce(evidence.kind,'')) in(
        'OTHER','EXPENSE','EXPENSES'
      )
        then coalesce(
          economics.general_expense_required,
          false
        )
      else false
    end required
  from source_timesheets source
  join public.timesheet_evidence evidence
    on evidence.timesheet_id=source.timesheet_id
  left join public.v_ts_invoice_precheck precheck
    on precheck.timesheet_id=source.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=source.timesheet_id
  left join evidence_economics economics
    on economics.invoice_id=source.invoice_id
   and economics.timesheet_id=source.timesheet_id
  left join public.invoice_document_assets asset
    on asset.id=evidence.document_asset_id
),
evidence_agg as materialized (
  select
    invoice.invoice_id,
    count(evidence.evidence_id)
      filter(where evidence.required)::integer evidence_count,
    count(*) filter(
      where evidence.required
        and evidence.evidence_id is not null
        and evidence.document_asset_id is null
    )::integer unregistered_count,
    count(*) filter(
      where evidence.required
        and evidence.evidence_id is not null
        and evidence.document_asset_id is not null
        and coalesce(evidence.status,'DISCOVERED')
          not in(
            'READY','UNSUPPORTED','CORRUPT','MISSING','FAILED'
          )
    )::integer not_ready_count,
    count(*) filter(
      where evidence.required
        and evidence.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED'
        )
    )::integer failed_count,
    coalesce(sum(evidence.pages)
      filter(where evidence.required),0)::integer evidence_pages
  from base invoice
  left join evidence_rows evidence
    on evidence.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
),
hr_support as materialized (
  select
    invoice.invoice_id,
    count(source.source_system) filter(
      where upper(coalesce(source.source_system,''))
        ='HEALTHROSTER'
    )::integer healthroster_count,
    count(source.source_system) filter(
      where upper(coalesce(source.source_system,''))='NHSP'
    )::integer nhsp_count
  from base invoice
  left join public.invoice_hr_source_rows source
    on source.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
),
line_flags as materialized (
  select
    invoice.invoice_id,
    count(line.id)::integer line_count,
    coalesce(bool_or(
      upper(coalesce(line.meta_json->>'line_type',''))
        like '%HIGHER_RATE%'
    ),false) higher_rate_required
  from base invoice
  left join public.invoice_lines line
    on line.invoice_id=invoice.invoice_id
  group by invoice.invoice_id
)
select
  (base.candidate_json->>'client_id')::uuid client_id,
  base.candidate_json->>'client_name' client_name,
  base.invoice_week_start,
  (base.candidate_json->>'week_ending_date')::date
    week_ending_date,
  jsonb_build_object(
    'invoice_id',base.invoice_id,
    'invoice_no',base.candidate_json->>'invoice_number',
    'status',base.candidate_json->>'invoice_status',
    'on_hold_reason',base.on_hold_reason,
    'subtotal_ex_vat',
      (base.candidate_json->>'total_ex_vat')::numeric,
    'vat_amount',
      (base.candidate_json->>'vat_amount')::numeric,
    'total_inc_vat',
      (base.candidate_json->>'total_inc_vat')::numeric,
    'currency',base.candidate_json->>'currency',
    'invoice_stream',base.candidate_json->>'invoice_stream',
    'is_self_bill',lower(coalesce(
      base.header_snapshot_json#>>'{meta,self_bill}',
      base.header_snapshot_json->>'self_bill',
      'false'
    )) in('true','t','1','yes'),
    'do_not_send',base.do_not_send,
    'document_revision',
      (base.candidate_json->>'document_revision')::bigint,
    'preview_document_state',
      base.candidate_json->>'preview_document_state',
    'stable_blocker_codes',
      base.candidate_json->'hard_blocker_codes',
    'document_dependency_codes',
      base.candidate_json->'document_dependency_codes',
    'delivery_blocker_codes',
      base.candidate_json->'delivery_blocker_codes',
    'can_issue_only',
      (base.candidate_json->>'can_issue_only')::boolean,
    'can_issue_and_deliver',
      (base.candidate_json->>'can_issue_and_deliver')::boolean,
    'validation_detail',
      base.candidate_json->'validation_detail',
    'estimated_supporting_page_count',
      evidence.evidence_pages
        +timesheet.timesheet_pages
        +hr.healthroster_count
        +hr.nhsp_count,
    'support_readiness',jsonb_build_object(
      'manual_timesheet_count',timesheet.manual_count,
      'electronic_timesheet_count',timesheet.electronic_count,
      'timesheet_not_ready_count',
        timesheet.timesheet_not_ready_count,
      'timesheets',timesheet.timesheet_support_rows,
      'evidence_count',evidence.evidence_count,
      'unregistered_asset_count',evidence.unregistered_count,
      'not_ready_asset_count',evidence.not_ready_count,
      'failed_asset_count',evidence.failed_count,
      'healthroster_count',hr.healthroster_count,
      'nhsp_count',hr.nhsp_count,
      'higher_rate_required',line.higher_rate_required
    ),
    'recipient_ready',not exists(
      select 1
      from jsonb_array_elements_text(
        coalesce(
          base.candidate_json->'delivery_blocker_codes',
          '[]'::jsonb
        )
      ) code(value)
      where code.value in(
        'MISSING_RECIPIENT','CONTRACT_MANUAL_EMAIL_MISSING',
        'CLIENT_MANUAL_EMAIL_MISSING',
        'CONTRACT_MANUAL_EMAIL_CONFLICT',
        'INVALID_TO_RECIPIENT','INVALID_CC_RECIPIENT',
        'INVALID_BCC_RECIPIENT'
      )
    ),
    'recipient',
      base.candidate_json#>'{_private,recipient}',
    'recipient_routing_warnings',
      base.candidate_json->'warning_codes',
    'active_issue_operation_id',
      base.candidate_json->>'active_issue_operation_id',
    'active_issue_operation',
      base.candidate_json->'active_issue_operation',
    'active_document_operation_id',
      base.active_document_operation_id,
    'last_issue_error',
      base.candidate_json->'last_issue_error',
    'last_document_error',
      base.last_document_error_json
  ) invoice_json
from base
join timesheet_support_agg timesheet
  on timesheet.invoice_id=base.invoice_id
join evidence_agg evidence on evidence.invoice_id=base.invoice_id
join hr_support hr on hr.invoice_id=base.invoice_id
join line_flags line on line.invoice_id=base.invoice_id
order by
  base.candidate_json->>'client_name' nulls last,
  base.candidate_json->>'week_ending_date' desc nulls last,
  base.candidate_json->>'invoice_number' nulls last,
  base.invoice_id;
$function$;

-- private._invoice_batch_issue_source_rows_for_ids_v2(uuid[],boolean,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_source_rows_for_ids_v2(p_invoice_ids uuid[], p_allow_early boolean DEFAULT false, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(client_id uuid, client_name text, invoice_week_start date, week_ending_date date, invoice_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_invoice_ids uuid[];
begin
  if p_invoice_ids is null
     or cardinality(p_invoice_ids)<1
     or cardinality(p_invoice_ids)>250 then
    raise exception using
      errcode='22023',
      message='ISSUE_CANDIDATE_ID_LIMIT_EXCEEDED';
  end if;

  if exists (
    select 1
    from unnest(p_invoice_ids) supplied(invoice_id)
    where supplied.invoice_id is null
  ) then
    raise exception using
      errcode='22023',
      message='ISSUE_CANDIDATE_ID_INVALID';
  end if;

  select array_agg(deduplicated.invoice_id order by deduplicated.invoice_id)
  into v_invoice_ids
  from (
    select distinct supplied.invoice_id
    from unnest(p_invoice_ids) supplied(invoice_id)
  ) deduplicated;

  return query
  select source.client_id,
    source.client_name,
    source.invoice_week_start,
    source.week_ending_date,
    source.invoice_json
  from private._invoice_batch_issue_source_rows_core_v2(
    coalesce(p_allow_early,false),
    cardinality(v_invoice_ids),
    coalesce(p_now_utc,now()),
    v_invoice_ids
  ) source;
end;
$function$;

-- private._invoice_batch_issue_source_rows_v2(boolean,integer,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_source_rows_v2(p_allow_early boolean DEFAULT false, p_limit integer DEFAULT NULL::integer, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(client_id uuid, client_name text, invoice_week_start date, week_ending_date date, invoice_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
select
  source.client_id,
  source.client_name,
  source.invoice_week_start,
  source.week_ending_date,
  source.invoice_json
from private._invoice_batch_issue_source_rows_core_v2(
  p_allow_early,
  p_limit,
  p_now_utc,
  null::uuid[]
) source;
$function$;

-- private._invoice_batch_manifest_advance_v2(jsonb,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_batch_manifest_advance_v2(p_claims jsonb, p_action text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_action text := upper(btrim(coalesce(p_action,'')));
  v_chunk_type text;
  v_result jsonb := '[]'::jsonb;
  v_expander record;
  v_page jsonb;
  v_page_rows jsonb;
  v_has_more boolean;
  v_next_cursor jsonb;
  v_row_count integer;
  v_manifest_generation integer;
  v_release_ids uuid[];
  v_remaining integer;
  v_requests jsonb;
  v_ensure jsonb;
begin
  if v_action not in ('GENERATE','ISSUE') then
    raise exception using
      errcode='22023',
      message='INVOICE_BATCH_MANIFEST_ACTION_INVALID';
  end if;

  if jsonb_typeof(coalesce(p_claims,'null'::jsonb)) is distinct from 'array'
     or jsonb_array_length(p_claims) < 1
     or jsonb_array_length(p_claims) > 100 then
    raise exception using
      errcode='22023',
      message='p_claims must contain 1..100 claims';
  end if;

  v_chunk_type := case
    when v_action='GENERATE' then 'GENERATION_GROUP'
    else 'ISSUE_INVOICE'
  end;

  for v_expander in
    select
      c.*,
      o.actor_user_id,
      o.control_version root_control_version,
      o.manifest_generation root_manifest_generation,
      o.input_json root_input_json
    from jsonb_array_elements(p_claims) claim
    join public.invoice_operation_chunks c
      on c.id = case
        when pg_input_is_valid(coalesce(claim->>'chunk_id',''),'uuid')
          then (claim->>'chunk_id')::uuid
      end
    join public.invoice_operations o on o.id=c.operation_id
    where c.chunk_type=v_chunk_type
      and c.phase in ('BUILD_MANIFEST','RELEASE_MANIFEST')
      and coalesce(c.payload_json->>'is_selection_expander','false')
        in ('true','t','1','yes','on')
    order by c.id
  loop
    v_manifest_generation := greatest(
      1,
      coalesce(v_expander.root_manifest_generation,0),
      case
        when coalesce(v_expander.payload_json->>'manifest_generation','')
          ~ '^[1-9][0-9]{0,8}$'
          then (v_expander.payload_json->>'manifest_generation')::integer
        else 1
      end
    );

    if v_expander.phase='BUILD_MANIFEST' then
      begin
        v_page := case
          when v_action='GENERATE' then
            private._invoice_batch_generate_candidate_rows_v2(
              (case
                when jsonb_typeof(v_expander.payload_json->'query')='object'
                  then v_expander.payload_json->'query'
                else '{}'::jsonb
              end)
              || jsonb_build_object(
                'contract_version','INVOICE_BATCH_QUERY_V2',
                'action',v_action,
                'mode','EXPAND_SELECTION',
                'page_size',250,
                'cursor',case
                  when jsonb_typeof(v_expander.payload_json->'cursor')='object'
                    then v_expander.payload_json->'cursor'
                  else '{}'::jsonb
                end,
                'selection',coalesce(
                  v_expander.payload_json#>'{selection_contract,selection}',
                  v_expander.payload_json#>'{query,selection}'
                )
              ),
              v_now
            )
          else
            private._invoice_batch_issue_candidate_rows_v2(
              (case
                when jsonb_typeof(v_expander.payload_json->'query')='object'
                  then v_expander.payload_json->'query'
                else '{}'::jsonb
              end)
              || jsonb_build_object(
                'contract_version','INVOICE_BATCH_QUERY_V2',
                'action',v_action,
                'mode','EXPAND_SELECTION',
                'page_size',250,
                'cursor',case
                  when jsonb_typeof(v_expander.payload_json->'cursor')='object'
                    then v_expander.payload_json->'cursor'
                  else '{}'::jsonb
                end,
                'selection',coalesce(
                  v_expander.payload_json#>'{selection_contract,selection}',
                  v_expander.payload_json#>'{query,selection}'
                )
              ),
              v_now
            )
        end;
      exception
        when serialization_failure then
          update public.invoice_operation_chunks c
          set
            status='SUPERSEDED',
            phase='SOURCE_CHANGED',
            manifest_committed=true,
            result_visible=(
              coalesce(c.payload_json->>'manifest_outcome','') <> 'EXCLUDED'
            ),
            result_json=coalesce(c.result_json,'{}'::jsonb)
              || jsonb_build_object(
                'result_category','CHANGED',
                'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
              ),
            completed_at_utc=v_now,
            updated_at_utc=v_now
          where c.operation_id=v_expander.operation_id
            and c.is_manifest_member
            and c.manifest_generation=v_manifest_generation
            and not c.manifest_committed;

          update public.invoice_operation_chunks c
          set
            status='BLOCKED',
            phase='SOURCE_CHANGED',
            error_json=jsonb_build_object(
              'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
            ),
            completed_at_utc=v_now,
            lease_owner=null,
            lease_token=null,
            lease_expires_at_utc=null,
            updated_at_utc=v_now
          where c.id=v_expander.id;

          update public.invoice_operations o
          set
            status='BLOCKED',
            phase='SOURCE_CHANGED',
            manifest_committed=false,
            release_complete=false,
            requires_user_action=true,
            error_json=jsonb_build_object(
              'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
            ),
            progress_json=coalesce(o.progress_json,'{}'::jsonb)
              || jsonb_build_object(
                'selection_expansion_pending',false,
                'manifest_committed',false,
                'manifest_status','SUPERSEDED',
                'release_pending_total',0,
                'release_complete',false,
                'superseded_manifest_generation',
                  v_manifest_generation,
                'status_message','Candidate data changed; reload the selection'
              ),
            updated_at_utc=v_now,
            change_seq=nextval('public.invoice_operation_change_seq')
          where o.id=v_expander.operation_id;

          v_result := v_result || jsonb_build_array(jsonb_build_object(
            'chunk_id',v_expander.id,
            'status','BLOCKED',
            'phase','SOURCE_CHANGED',
            'error',jsonb_build_object(
              'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
            )
          ));
          continue;
      end;

      v_page_rows := case
        when jsonb_typeof(v_page->'rows')='array' then v_page->'rows'
        else '[]'::jsonb
      end;
      v_row_count := jsonb_array_length(v_page_rows);
      v_has_more := coalesce(
        (v_page#>>'{page,has_more}')::boolean,
        false
      );
      v_next_cursor := case
        when jsonb_typeof(v_page#>'{page,next_cursor_values}')='object'
          then v_page#>'{page,next_cursor_values}'
        else '{}'::jsonb
      end;

      insert into public.invoice_operation_chunks(
        operation_id,
        chunk_type,
        phase,
        work_key,
        sequence_no,
        level_no,
        entity_type,
        entity_id,
        status,
        priority,
        run_after_utc,
        payload_json,
        progress_json,
        result_json,
        operation_control_version,
        manifest_generation,
        is_manifest_member,
        manifest_committed,
        result_visible,
        created_at_utc,
        updated_at_utc
      )
      select
        v_expander.operation_id,
        v_chunk_type,
        'AWAITING_MANIFEST_COMMIT',
        private._invoice_batch_hash_v2(jsonb_build_object(
          'root_operation_id',v_expander.operation_id,
          'manifest_generation',v_manifest_generation,
          'selection_key',row_json->>'selection_key',
          'source_revision',coalesce(
            row_json->>'source_revision',
            row_json->>'document_revision'
          ),
          'outcome',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'SELECTED'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end
        )),
        coalesce(
          case
            when coalesce(v_expander.payload_json->>'scanned','')
              ~ '^[0-9]{1,9}$'
              then (v_expander.payload_json->>'scanned')::integer
            else 0
          end,
          0
        ) + row_item.ordinality::integer,
        0,
        'OPERATION',
        v_expander.operation_id,
        'WAITING',
        greatest(
          coalesce(v_expander.priority,case when v_action='ISSUE' then 850 else 600 end),
          case when v_action='ISSUE' then 850 else 600 end
        ),
        v_now,
        (
          case
            when v_action='GENERATE'
             and jsonb_typeof(row_json->'command_payload')='object'
              then row_json->'command_payload'
            else '{}'::jsonb
          end
        )
        || jsonb_build_object(
          'contract_version','INVOICE_BATCH_MANIFEST_CARRIER_V2',
          'selection_key',row_json->>'selection_key',
          'group_key',row_json->>'group_key',
          'manifest_generation',v_manifest_generation,
          'is_manifest_member',true,
          'manifest_outcome',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'SELECTED'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end,
          'result_category',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'IN_PROGRESS'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end,
          'row_kind',row_json->>'row_kind',
          'source_revision',coalesce(
            row_json->>'source_revision',
            row_json->>'document_revision'
          ),
          'expected_revision',coalesce(
            row_json->>'document_revision',
            row_json->>'source_revision'
          ),
          'invoice_id',row_json->>'invoice_id',
          'invoice_number',row_json->>'invoice_number',
          'client_id',row_json->>'client_id',
          'client_name',row_json->>'client_name',
          'candidate_display',row_json->>'candidate_display',
          'week_ending_display',row_json->>'week_ending_display',
          'currency',coalesce(row_json->>'currency','GBP'),
          'total_ex_vat',row_json->'total_ex_vat',
          'total_inc_vat',row_json->'total_inc_vat',
          'action_blocker_codes',coalesce(
            row_json->'action_blocker_codes',
            row_json->'issue_blocker_codes',
            '[]'::jsonb
          ),
          'delivery_blocker_codes',coalesce(
            row_json->'delivery_blocker_codes',
            '[]'::jsonb
          ),
          'blocked_for_sending',coalesce(
            row_json->'blocked_for_sending',
            'false'::jsonb
          ),
          'allow_early',coalesce(
            v_expander.payload_json#>'{query,filters,allow_early}',
            'false'::jsonb
          ),
          'deliver',coalesce(
            v_expander.payload_json->'deliver',
            'false'::jsonb
          ),
          'command_token',v_expander.root_input_json->>'command_token',
          'delivery_request_token',
            v_expander.payload_json->>'delivery_request_token',
          'delivery_intent',coalesce(
            v_expander.payload_json->'delivery_intent',
            '{}'::jsonb
          ),
          'parent_expander_chunk_id',v_expander.id
        ),
        jsonb_build_object(
          'contract_version','INVOICE_BATCH_PROGRESS_V2',
          'status_message','Waiting for manifest commit'
        ),
        jsonb_build_object(
          'result_category',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'IN_PROGRESS'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end,
          'badge_codes',coalesce(
            row_json->'action_blocker_codes',
            row_json->'issue_blocker_codes',
            '[]'::jsonb
          )
        ),
        v_expander.root_control_version,
        v_manifest_generation,
        true,
        false,
        false,
        v_now,
        v_now
      from jsonb_array_elements(v_page_rows)
        with ordinality row_item(row_json,ordinality)
      where nullif(row_json->>'selection_key','') is not null
      on conflict do nothing;

      update public.invoice_operation_chunks c
      set
        status='QUEUED',
        phase=case
          when v_has_more then 'BUILD_MANIFEST'
          else 'RELEASE_MANIFEST'
        end,
        run_after_utc=v_now,
        payload_json=coalesce(c.payload_json,'{}'::jsonb)
          || jsonb_build_object(
            'cursor',case when v_has_more then v_next_cursor else '{}'::jsonb end,
            'scanned',coalesce(
              case
                when coalesce(c.payload_json->>'scanned','') ~ '^[0-9]{1,9}$'
                  then (c.payload_json->>'scanned')::integer
                else 0
              end,
              0
            ) + v_row_count,
            'last_candidate_page',v_page->'page',
            'completed',not v_has_more
          ),
        progress_json=jsonb_build_object(
          'contract_version','INVOICE_BATCH_PROGRESS_V2',
          'status_message',case
            when v_has_more then 'Building selection manifest'
            else 'Manifest committed; releasing work'
          end
        ),
        lease_owner=null,
        lease_token=null,
        lease_expires_at_utc=null,
        updated_at_utc=v_now
      where c.id=v_expander.id;

      if not v_has_more then
        update public.invoice_operation_chunks member
        set
          phase='AWAITING_RELEASE',
          updated_at_utc=v_now
        where member.operation_id=v_expander.operation_id
          and member.is_manifest_member
          and member.manifest_generation=v_manifest_generation
          and not member.manifest_committed
          and member.phase='AWAITING_MANIFEST_COMMIT';
      end if;

      update public.invoice_operations o
      set
        status='QUEUED',
        phase=case
          when v_has_more then 'BUILD_MANIFEST'
          else 'RELEASE_MANIFEST'
        end,
        manifest_committed=not v_has_more,
        release_complete=false,
        progress_json=coalesce(o.progress_json,'{}'::jsonb)
          || jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message',case
              when v_has_more then 'Building selection manifest'
              else 'Manifest committed; releasing work'
            end,
            'selection_expansion_pending',v_has_more,
            'manifest_committed',not v_has_more,
            'manifest_generation',v_manifest_generation,
            'manifest_status',case
              when v_has_more then 'BUILDING'
              else 'COMMITTED'
            end,
            'expected_scan_total',coalesce(
              case
                when coalesce(
                  o.progress_json->>'expected_scan_total',
                  ''
                ) ~ '^[0-9]+$'
                  then (
                    o.progress_json->>'expected_scan_total'
                  )::integer
              end,
              (
                select count(*)::integer
                from public.invoice_operation_chunks member
                where member.operation_id=o.id
                  and member.is_manifest_member
                  and member.manifest_generation=v_manifest_generation
              )
            ),
            'scanned_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
            ),
            'selected_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='SELECTED'
            ),
            'excluded_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='EXCLUDED'
            ),
            'blocked_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='BLOCKED'
            ),
            'already_active_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'
                  ='ALREADY_ACTIVE'
            ),
            'changed_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='CHANGED'
            ),
            'missing_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='MISSING'
            ),
            'release_pending_total',case
              when v_has_more then 0
              else (
                select count(*)::integer
                from public.invoice_operation_chunks member
                where member.operation_id=o.id
                  and member.is_manifest_member
                  and member.manifest_generation=v_manifest_generation
                  and member.payload_json->>'manifest_outcome'='SELECTED'
                  and not member.manifest_committed
              )
            end,
            'released_total',0,
            'release_conflict_total',0,
            'release_blocked_total',0,
            'release_complete',false,
            'committed_at_utc',case
              when v_has_more then o.progress_json->'committed_at_utc'
              else to_jsonb(v_now)
            end,
            'superseded_manifest_generation',
              o.progress_json->'superseded_manifest_generation',
            'expanded_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='SELECTED'
            )
          ),
        chunk_count=(
          select count(*)::integer
          from public.invoice_operation_chunks member
          where member.operation_id=o.id
        ),
        updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
      where o.id=v_expander.operation_id;

      v_result := v_result || jsonb_build_array(jsonb_build_object(
        'chunk_id',v_expander.id,
        'status','QUEUED',
        'phase',case
          when v_has_more then 'BUILD_MANIFEST'
          else 'RELEASE_MANIFEST'
        end,
        'result',jsonb_build_object(
          'manifest_committed',not v_has_more,
          'page',v_page->'page'
        )
      ));
      continue;
    end if;

    select coalesce(array_agg(release_member.id order by
      release_member.selection_key,
      release_member.id
    ),array[]::uuid[])
    into v_release_ids
    from (
      select c.id,c.selection_key
      from public.invoice_operation_chunks c
      where c.operation_id=v_expander.operation_id
        and c.is_manifest_member
        and c.manifest_generation=v_manifest_generation
        and not c.manifest_committed
      order by c.selection_key,c.id
      for update skip locked
      limit 250
    ) release_member;

    if cardinality(v_release_ids) > 0 then
      update public.invoice_operation_chunks c
      set
        manifest_committed=true,
        status=case c.payload_json->>'manifest_outcome'
          when 'EXCLUDED' then 'COMPLETE'
          when 'BLOCKED' then 'BLOCKED'
          when 'ALREADY_ACTIVE' then 'COMPLETE'
          else c.status
        end,
        phase=case c.payload_json->>'manifest_outcome'
          when 'EXCLUDED' then 'EXCLUDED'
          when 'BLOCKED' then 'BLOCKED'
          when 'ALREADY_ACTIVE' then 'ALREADY_ACTIVE'
          else c.phase
        end,
        result_visible=(
          c.payload_json->>'manifest_outcome' <> 'EXCLUDED'
        ),
        completed_at_utc=case
          when c.payload_json->>'manifest_outcome'
            in ('EXCLUDED','BLOCKED','ALREADY_ACTIVE')
            then v_now
          else c.completed_at_utc
        end,
        updated_at_utc=v_now
      where c.id=any(v_release_ids)
        and c.payload_json->>'manifest_outcome' <> 'SELECTED';

      if v_action='GENERATE' then
        update public.invoice_operation_chunks c
        set
          manifest_committed=true,
          entity_type='CLIENT',
          entity_id=case
            when pg_input_is_valid(coalesce(c.payload_json->>'client_id',''),'uuid')
              then (c.payload_json->>'client_id')::uuid
          end,
          status='QUEUED',
          phase='VALIDATE_SOURCES',
          result_visible=true,
          run_after_utc=v_now,
          progress_json=jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message','Queued for invoice generation'
          ),
          updated_at_utc=v_now
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and coalesce(c.payload_json->>'row_kind','CREATE_INVOICE')
            ='CREATE_INVOICE';

        select coalesce(jsonb_agg(jsonb_build_object(
          'request_key',c.selection_key,
          'invoice_id',c.payload_json->>'invoice_id',
          'document_revision',c.payload_json->>'expected_revision',
          'purpose','DRAFT_PREVIEW',
          'priority',greatest(c.priority,550),
          'parent_operation_id',c.operation_id,
          'actor_user_id',v_expander.actor_user_id,
          'template_version','invoice-professional-v2'
        ) order by c.selection_key),'[]'::jsonb)
        into v_requests
        from public.invoice_operation_chunks c
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and coalesce(c.payload_json->>'row_kind','CREATE_INVOICE')
            <> 'CREATE_INVOICE';

        if jsonb_array_length(v_requests) > 0 then
          v_ensure := private._invoice_document_operation_ensure_batch(
            v_requests,
            v_now
          );

          update public.invoice_operation_chunks c
          set
            manifest_committed=true,
            status=case
              when result_row->>'status'='READY' then 'COMPLETE'
              when coalesce((result_row->>'ok')::boolean,false) then 'WAITING'
              when result_row->>'code' in ('SOURCE_CHANGED','INVOICE_NOT_FOUND')
                then 'SUPERSEDED'
              else 'BLOCKED'
            end,
            phase=case
              when result_row->>'status'='READY' then 'COMPLETE'
              when coalesce((result_row->>'ok')::boolean,false)
                then 'WAITING_DOCUMENT'
              when result_row->>'code'='SOURCE_CHANGED'
                then 'SOURCE_CHANGED'
              when result_row->>'code'='INVOICE_NOT_FOUND'
                then 'SOURCE_MISSING'
              else 'BLOCKED'
            end,
            result_visible=true,
            document_version_id=case
              when pg_input_is_valid(
                coalesce(result_row->>'document_version_id',''),
                'uuid'
              ) then (result_row->>'document_version_id')::uuid
              else c.document_version_id
            end,
            result_json=coalesce(c.result_json,'{}'::jsonb)
              || jsonb_build_object(
                'result_category',case
                  when result_row->>'status'='READY' then 'REGENERATED'
                  when coalesce((result_row->>'ok')::boolean,false)
                    then 'IN_PROGRESS'
                  when result_row->>'code'='SOURCE_CHANGED'
                    then 'CHANGED'
                  when result_row->>'code'='INVOICE_NOT_FOUND'
                    then 'MISSING'
                  else 'BLOCKED'
                end,
                'document_operation_id',result_row->>'operation_id',
                'document_version_id',result_row->>'document_version_id',
                'code',result_row->>'code'
              ),
            completed_at_utc=case
              when result_row->>'status'='READY'
                or not coalesce((result_row->>'ok')::boolean,false)
                then v_now
              else null
            end,
            updated_at_utc=v_now
          from jsonb_array_elements(
            coalesce(v_ensure->'results','[]'::jsonb)
          ) result_item(result_row)
          where c.id=any(v_release_ids)
            and c.selection_key=result_row->>'request_key';
        end if;
      else
        perform i.id
        from public.invoices i
        where i.id in (
          select (c.payload_json->>'invoice_id')::uuid
          from public.invoice_operation_chunks c
          where c.id=any(v_release_ids)
            and c.payload_json->>'manifest_outcome'='SELECTED'
            and pg_input_is_valid(
              coalesce(c.payload_json->>'invoice_id',''),
              'uuid'
            )
        )
        order by i.id
        for update;

        update public.invoice_operation_chunks c
        set
          manifest_committed=true,
          status='COMPLETE',
          phase='ALREADY_ACTIVE',
          result_visible=true,
          result_json=coalesce(c.result_json,'{}'::jsonb)
            || jsonb_build_object(
              'result_category','ALREADY_ACTIVE',
              'code','ALREADY_ACTIVE'
            ),
          completed_at_utc=v_now,
          updated_at_utc=v_now
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and exists (
            select 1
            from public.invoice_operation_chunks active
            join public.invoice_operations active_operation
              on active_operation.id=active.operation_id
            where active.chunk_type='ISSUE_INVOICE'
              and active.entity_type='INVOICE'
              and active.entity_id=(c.payload_json->>'invoice_id')::uuid
              and active.status in (
                'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
              )
              and active.id<>c.id
              and active.replaced_by_chunk_id is null
              and active_operation.status
                in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
          );

        update public.invoice_operation_chunks c
        set
          manifest_committed=true,
          entity_type='INVOICE',
          entity_id=(c.payload_json->>'invoice_id')::uuid,
          status='QUEUED',
          phase='VALIDATE',
          result_visible=true,
          run_after_utc=v_now,
          payload_json=c.payload_json || jsonb_build_object(
            'request_key',c.selection_key,
            'evaluation_date',(v_now at time zone 'Europe/London')::date,
            'frozen_issue_at_utc',v_now
          ),
          progress_json=jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message','Queued for legal issue'
          ),
          updated_at_utc=v_now
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and c.status='WAITING';
      end if;
    end if;

    select count(*)::integer
    into v_remaining
    from public.invoice_operation_chunks c
    where c.operation_id=v_expander.operation_id
      and c.is_manifest_member
      and c.manifest_generation=v_manifest_generation
      and not c.manifest_committed;

    update public.invoice_operation_chunks c
    set
      status=case when v_remaining=0 then 'COMPLETE' else 'QUEUED' end,
      phase=case when v_remaining=0 then 'RELEASE_COMPLETE' else 'RELEASE_MANIFEST' end,
      run_after_utc=case when v_remaining=0 then c.run_after_utc else v_now end,
      completed_at_utc=case when v_remaining=0 then v_now else null end,
      payload_json=coalesce(c.payload_json,'{}'::jsonb)
        || jsonb_build_object(
          'release_remaining',v_remaining,
          'release_complete',v_remaining=0
        ),
      progress_json=jsonb_build_object(
        'contract_version','INVOICE_BATCH_PROGRESS_V2',
        'status_message',case
          when v_remaining=0 then 'Manifest release complete'
          else 'Releasing manifest work'
        end
      ),
      lease_owner=null,
      lease_token=null,
      lease_expires_at_utc=null,
      updated_at_utc=v_now
    where c.id=v_expander.id;

    update public.invoice_operations o
    set
      status='QUEUED',
      phase=case when v_remaining=0 then 'BUSINESS_WORK' else 'RELEASE_MANIFEST' end,
      manifest_committed=true,
      release_complete=v_remaining=0,
      progress_json=coalesce(o.progress_json,'{}'::jsonb)
        || jsonb_build_object(
          'contract_version','INVOICE_BATCH_PROGRESS_V2',
          'selection_expansion_pending',false,
          'manifest_committed',true,
          'manifest_generation',v_manifest_generation,
          'manifest_status',case
            when v_remaining=0 then 'RELEASE_COMPLETE'
            else 'RELEASE_MANIFEST'
          end,
          'release_pending_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and not member.manifest_committed
          ),
          'released_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and member.manifest_committed
              and coalesce(member.result_category,'') not in(
                'ALREADY_ACTIVE','BLOCKED','CHANGED','MISSING','FAILED'
              )
          ),
          'release_conflict_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and member.result_category='ALREADY_ACTIVE'
          ),
          'release_blocked_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and member.result_category in(
                'BLOCKED','CHANGED','MISSING','FAILED'
              )
          ),
          'release_complete',v_remaining=0,
          'status_message',case
            when v_remaining=0 then 'Processing selected work'
            else 'Releasing manifest work'
          end
        ),
      updated_at_utc=v_now,
      change_seq=nextval('public.invoice_operation_change_seq')
    where o.id=v_expander.operation_id;

    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'chunk_id',v_expander.id,
      'status',case when v_remaining=0 then 'COMPLETE' else 'QUEUED' end,
      'phase',case
        when v_remaining=0 then 'RELEASE_COMPLETE'
        else 'RELEASE_MANIFEST'
      end,
      'result',jsonb_build_object(
        'released_count',cardinality(v_release_ids),
        'remaining_count',v_remaining,
        'release_complete',v_remaining=0
      )
    ));
  end loop;

  return v_result;
end;
$function$;

-- private._invoice_batch_query_validate_v2(jsonb,text)
CREATE OR REPLACE FUNCTION private._invoice_batch_query_validate_v2(p_query jsonb, p_action text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_query jsonb := coalesce(p_query, '{}'::jsonb);
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_query_action text;
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_cursor jsonb;
  v_facet_request jsonb;
  v_selection jsonb;
  v_snapshot jsonb;
  v_page_size integer;
  v_normalized_filters jsonb;
  v_normalized_sort jsonb;
  v_group_selector_rules jsonb;
  v_normalized_group_selectors jsonb := '[]'::jsonb;
  v_normalized_query jsonb;
begin
  if v_action not in ('GENERATE', 'ISSUE')
     or jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_INVALID';
  end if;

  if octet_length(v_query::text) > 4194304 then
    raise exception using
      errcode = '22023',
      message = 'BATCH_REQUEST_TOO_LARGE';
  end if;

  if exists (
    select 1
    from jsonb_object_keys(v_query) key_name
    where key_name not in (
      'contract_version',
      'action',
      'mode',
      'snapshot',
      'page_size',
      'cursor',
      'filters',
      'sort',
      'selection',
      'selection_keys',
      'expected_source_revisions',
      'facet_request',
      'group_selectors'
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_UNKNOWN_FIELD';
  end if;

  if coalesce(v_query->>'contract_version', '') <>
      'INVOICE_BATCH_QUERY_V2' then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  if not (v_query ? 'action')
     or jsonb_typeof(v_query->'action') is distinct from 'string'
     or nullif(btrim(v_query->>'action'), '') is null then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_ACTION_REQUIRED';
  end if;

  v_query_action := upper(btrim(v_query->>'action'));
  if v_query_action not in ('GENERATE', 'ISSUE')
     or v_query_action <> v_action then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_ACTION_MISMATCH';
  end if;

  if not (v_query ? 'mode')
     or jsonb_typeof(v_query->'mode') is distinct from 'string' then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_mode := upper(btrim(v_query->>'mode'));
  if v_mode not in (
    'PAGE',
    'FACETS',
    'SUMMARY',
    'EXPAND_SELECTION',
    'EXPLICIT_KEYS'
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  if not (v_query ?& array[
    'contract_version',
    'action',
    'mode',
    'snapshot',
    'filters',
    'sort',
    'selection'
  ]) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_INVALID';
  end if;

  if exists (
    select 1
    from jsonb_object_keys(v_query) key_name
    where (
      v_mode = 'PAGE'
      and key_name not in (
        'contract_version','action','mode','snapshot','page_size','cursor',
        'filters','sort','selection'
      )
    ) or (
      v_mode = 'FACETS'
      and key_name not in (
        'contract_version','action','mode','snapshot','filters','sort',
        'selection','facet_request'
      )
    ) or (
      v_mode = 'SUMMARY'
      and key_name not in (
        'contract_version','action','mode','snapshot','filters','sort',
        'selection','group_selectors'
      )
    ) or (
      v_mode = 'EXPAND_SELECTION'
      and key_name not in (
        'contract_version','action','mode','snapshot','page_size','cursor',
        'filters','sort','selection'
      )
    ) or (
      v_mode = 'EXPLICIT_KEYS'
      and key_name not in (
        'contract_version','action','mode','snapshot','filters','sort',
        'selection','selection_keys','expected_source_revisions'
      )
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_MODE_FIELD_INVALID';
  end if;

  if v_mode = 'FACETS' and not (v_query ? 'facet_request')
     or v_mode = 'EXPLICIT_KEYS' and (
       not (v_query ? 'selection_keys')
       or not (v_query ? 'expected_source_revisions')
     ) then
    raise exception using
      errcode = '22023',
      message = case
        when v_mode = 'FACETS' then 'BATCH_FACET_REQUEST_INVALID'
        else 'BATCH_EXPLICIT_KEYS_INVALID'
      end;
  end if;

  if v_query ? 'page_size'
     and (
       jsonb_typeof(v_query->'page_size') <> 'number'
       or coalesce(v_query->>'page_size', '') !~ '^[1-9][0-9]{0,8}$'
       or (
         v_mode = 'PAGE'
         and (v_query->>'page_size')::integer > 100
       )
       or (
         v_mode = 'EXPAND_SELECTION'
         and (v_query->>'page_size')::integer > 250
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_PAGE_SIZE_INVALID';
  end if;

  v_page_size := case
    when v_mode = 'EXPAND_SELECTION'
      then coalesce((v_query->>'page_size')::integer, 250)
    when v_mode = 'PAGE'
      then coalesce((v_query->>'page_size')::integer, 100)
    else null
  end;

  v_filters := v_query->'filters';
  if jsonb_typeof(v_filters) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(v_filters) key_name
       where key_name not in (
         'client_ids',
         'candidate_ids',
         'week_endings',
         'week_ending_from',
         'week_ending_to',
         'status_codes',
         'blocker_codes',
         'search',
         'allow_early',
         'display_mode',
         'invoice_streams'
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_FILTER_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from (
      values
        ('client_ids', 'uuid', 500),
        ('candidate_ids', 'uuid', 500),
        ('week_endings', 'date', 500),
        ('status_codes', 'text', 100),
        ('blocker_codes', 'text', 250),
        ('invoice_streams', 'text', 20)
    ) definition(field_name, value_type, maximum_count)
    where v_filters ? definition.field_name
      and (
        jsonb_typeof(v_filters->definition.field_name) <> 'array'
        or jsonb_array_length(v_filters->definition.field_name) >
          definition.maximum_count
        or exists (
          select 1
          from jsonb_array_elements(
            case
              when jsonb_typeof(
                v_filters->definition.field_name
              ) = 'array'
                then v_filters->definition.field_name
              else '[]'::jsonb
            end
          ) item(value)
          where jsonb_typeof(item.value) <> 'string'
             or nullif(btrim(item.value #>> '{}'), '') is null
             or length(btrim(item.value #>> '{}')) > 120
             or (
               definition.value_type = 'uuid'
               and not pg_input_is_valid(item.value #>> '{}', 'uuid')
             )
             or (
               definition.value_type = 'date'
               and not pg_input_is_valid(item.value #>> '{}', 'date')
             )
        )
      )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_FILTER_INVALID';
  end if;

  if (v_filters ? 'week_ending_from'
      and jsonb_typeof(v_filters->'week_ending_from') <> 'null'
      and (
        jsonb_typeof(v_filters->'week_ending_from') <> 'string'
        or not pg_input_is_valid(v_filters->>'week_ending_from', 'date')
      ))
     or (v_filters ? 'week_ending_to'
      and jsonb_typeof(v_filters->'week_ending_to') <> 'null'
      and (
        jsonb_typeof(v_filters->'week_ending_to') <> 'string'
        or not pg_input_is_valid(v_filters->>'week_ending_to', 'date')
      ))
     or (
        v_filters ? 'week_ending_from'
        and v_filters ? 'week_ending_to'
        and jsonb_typeof(v_filters->'week_ending_from') <> 'null'
        and jsonb_typeof(v_filters->'week_ending_to') <> 'null'
        and (v_filters->>'week_ending_from')::date >
            (v_filters->>'week_ending_to')::date
      )
     or (v_filters ? 'search'
      and jsonb_typeof(v_filters->'search') <> 'null'
      and (
        jsonb_typeof(v_filters->'search') <> 'string'
        or length(btrim(v_filters->>'search')) > 200
      ))
     or (v_filters ? 'allow_early' and
       jsonb_typeof(v_filters->'allow_early') <> 'boolean')
     or (v_filters ? 'display_mode' and (
       jsonb_typeof(v_filters->'display_mode') <> 'string'
       or upper(v_filters->>'display_mode') not in ('ALL', 'READY', 'BLOCKED')
     )) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_FILTER_INVALID';
  end if;

  if exists (
    select 1
    from jsonb_array_elements_text(
      case
        when jsonb_typeof(v_filters->'status_codes') = 'array'
          then v_filters->'status_codes'
        else '[]'::jsonb
      end
    ) status_code(value)
    where (
      v_action = 'GENERATE'
      and upper(status_code.value) not in (
        'READY','STALE','FAILED','IN_PROGRESS','BLOCKED'
      )
    ) or (
      v_action = 'ISSUE'
      and upper(status_code.value) not in (
        'READY','READY_SEND_BLOCKED','STALE','FAILED',
        'IN_PROGRESS','BLOCKED'
      )
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_STATUS_INVALID';
  end if;

  v_sort := v_query->'sort';
  if jsonb_typeof(v_sort) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(v_sort) key_name
       where key_name not in ('group_preset', 'sort_key', 'sort_direction')
     )
     or (
       v_sort ? 'group_preset'
       and (
         jsonb_typeof(v_sort->'group_preset') <> 'string'
         or upper(v_sort->>'group_preset') not in (
           'WEEK_CLIENT_CANDIDATE',
           'CLIENT_WEEK_CANDIDATE',
           'CANDIDATE_WEEK_CLIENT',
           'STATUS_WEEK_CLIENT'
         )
       )
     )
     or (
       v_sort ? 'sort_key'
       and (
         jsonb_typeof(v_sort->'sort_key') <> 'string'
         or upper(v_sort->>'sort_key') not in (
           'WEEK_ENDING_DATE',
           'CLIENT_NAME',
           'CANDIDATE_NAME',
           'TOTAL_EX_VAT',
           'TOTAL_INC_VAT',
           'STATUS',
           'INVOICE_NUMBER'
         )
         or (
           upper(v_sort->>'sort_key') = 'INVOICE_NUMBER'
           and v_action <> 'ISSUE'
         )
       )
     )
     or (
       v_sort ? 'sort_direction'
       and (
         jsonb_typeof(v_sort->'sort_direction') <> 'string'
         or upper(v_sort->>'sort_direction') not in ('ASC', 'DESC')
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_SORT_INVALID';
  end if;

  v_cursor := case
    when jsonb_typeof(v_query->'cursor') = 'null' then '{}'::jsonb
    else coalesce(v_query->'cursor', '{}'::jsonb)
  end;
  if jsonb_typeof(v_cursor) is distinct from 'object'
     or exists (
       select 1
      from jsonb_object_keys(v_cursor) key_name
      where key_name not in (
         'after_selection_key',
         'after_sort_date',
         'after_sort_text',
         'after_sort_numeric'
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_CURSOR_INVALID';
  end if;

  if v_mode in ('PAGE', 'EXPAND_SELECTION')
     and jsonb_typeof(v_query->'cursor') not in ('object', 'null') then
    raise exception using
      errcode = '22023',
      message = 'BATCH_CURSOR_INVALID';
  end if;

  v_facet_request := coalesce(v_query->'facet_request', '{}'::jsonb);
  if jsonb_typeof(v_facet_request) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(v_facet_request) key_name
       where key_name not in ('kinds', 'search', 'limit_per_kind', 'cursors')
     )
     or (
       v_facet_request ? 'kinds'
       and (
         jsonb_typeof(v_facet_request->'kinds') <> 'array'
         or jsonb_array_length(v_facet_request->'kinds') < 1
         or jsonb_array_length(v_facet_request->'kinds') > 5
         or exists (
           select 1
           from jsonb_array_elements_text(
             case
               when jsonb_typeof(v_facet_request->'kinds') = 'array'
                 then v_facet_request->'kinds'
               else '[]'::jsonb
             end
           ) kind(value)
           where kind.value not in (
             'CLIENTS',
             'CANDIDATES',
             'WEEK_ENDINGS',
             'STATUSES',
             'BLOCKERS'
           )
         )
       )
     )
     or (
        v_facet_request ? 'search'
        and jsonb_typeof(v_facet_request->'search') <> 'null'
        and (
          jsonb_typeof(v_facet_request->'search') <> 'string'
          or length(btrim(v_facet_request->>'search')) > 200
       )
     )
     or (
       v_facet_request ? 'limit_per_kind'
       and (
         jsonb_typeof(v_facet_request->'limit_per_kind') <> 'number'
         or coalesce(v_facet_request->>'limit_per_kind', '') !~
            '^[1-9][0-9]{0,2}$'
         or (v_facet_request->>'limit_per_kind')::integer > 100
       )
     )
     or (
       v_facet_request ? 'cursors'
       and (
         jsonb_typeof(v_facet_request->'cursors') <> 'object'
         or exists (
           select 1
           from jsonb_object_keys(v_facet_request->'cursors') key_name
           where key_name not in (
             'clients',
             'candidates',
             'week_endings',
             'statuses',
             'blockers'
           )
         )
         or exists (
           select 1
           from jsonb_each(v_facet_request->'cursors') cursor_item(kind,value)
           where jsonb_typeof(cursor_item.value) <> 'object'
              or exists (
                select 1
                from jsonb_object_keys(cursor_item.value) cursor_key
                where (
                  cursor_item.kind in ('clients','candidates')
                  and cursor_key not in ('after_label','after_id')
                )
                   or (
                     cursor_item.kind='week_endings'
                     and cursor_key<>'after_value'
                   )
                   or (
                     cursor_item.kind in ('statuses','blockers')
                     and cursor_key<>'after_code'
                   )
              )
              or exists (
                select 1
                from jsonb_each(cursor_item.value) cursor_value(key,value)
                where jsonb_typeof(cursor_value.value)<>'string'
                   or nullif(btrim(cursor_value.value #>> '{}'),'') is null
                   or length(btrim(cursor_value.value #>> '{}'))>512
              )
         )
         or (
           v_facet_request#>>'{cursors,week_endings,after_value}' is not null
           and not pg_input_is_valid(
             v_facet_request#>>'{cursors,week_endings,after_value}',
             'date'
           )
         )
         or (
           v_facet_request#>>'{cursors,clients,after_id}' is not null
           and not pg_input_is_valid(
             v_facet_request#>>'{cursors,clients,after_id}',
             'uuid'
           )
         )
         or (
           v_facet_request#>>'{cursors,candidates,after_id}' is not null
           and not pg_input_is_valid(
             v_facet_request#>>'{cursors,candidates,after_id}',
             'uuid'
           )
         )
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_FACET_REQUEST_INVALID';
  end if;

  v_selection := coalesce(v_query->'selection', jsonb_build_object(
    'contract_version', 'INVOICE_BATCH_SELECTION_V2',
    'mode', 'IMPLICIT_ALL',
    'default_selected', true,
    'rules', '[]'::jsonb
  ));
  if octet_length(v_selection::text) > 3145728 then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SELECTION_PAYLOAD_TOO_LARGE';
  end if;
  perform 1
  from private._invoice_batch_selection_rules_v2(v_selection)
  limit 1;

  v_snapshot := v_query->'snapshot';
  if jsonb_typeof(v_snapshot) not in ('object', 'null') then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  if v_mode <> 'PAGE' and jsonb_typeof(v_snapshot) = 'null' then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_REQUIRED';
  end if;

  if jsonb_typeof(v_snapshot) = 'object'
     and (
       not (v_snapshot ?& array[
         'contract_version',
         'action',
         'at_utc',
         'revision',
         'expires_at_utc',
         'key_id',
         'token'
       ])
       or exists (
         select 1
         from jsonb_object_keys(v_snapshot) key_name
         where key_name not in (
           'contract_version',
           'action',
           'at_utc',
           'revision',
           'expires_at_utc',
           'key_id',
           'token'
         )
       )
       or coalesce(v_snapshot->>'contract_version', '') <>
          'INVOICE_BATCH_SNAPSHOT_V2'
       or upper(coalesce(v_snapshot->>'action', '')) <> v_action
       or coalesce(v_snapshot->>'revision', '') !~ '^[0-9]+$'
       or coalesce(v_snapshot->>'key_id', '') !~
          '^[a-z0-9][a-z0-9._-]{0,63}$'
       or nullif(v_snapshot->>'token', '') is null
       or not pg_input_is_valid(
         coalesce(v_snapshot->>'at_utc', ''),
         'timestamp with time zone'
       )
       or not pg_input_is_valid(
         coalesce(v_snapshot->>'expires_at_utc', ''),
         'timestamp with time zone'
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  if v_query ? 'group_selectors' then
    if jsonb_typeof(v_query->'group_selectors') is distinct from 'array'
       or jsonb_array_length(v_query->'group_selectors') > 400 then
      raise exception using
        errcode = '22023',
        message = 'INVOICE_BATCH_QUERY_MODE_FIELD_INVALID';
    end if;

    if exists (
      select 1
      from jsonb_array_elements(v_query->'group_selectors') selector(value)
      where jsonb_typeof(selector.value) is distinct from 'object'
    ) then
      raise exception using
        errcode = '22023',
        message = 'BATCH_SELECTION_SELECTOR_INVALID';
    end if;

    select coalesce(jsonb_agg(jsonb_build_object(
      'sequence', selector.ordinality,
      'action', 'INCLUDE',
      'selector', selector.value
    ) order by selector.ordinality), '[]'::jsonb)
    into v_group_selector_rules
    from jsonb_array_elements(v_query->'group_selectors')
      with ordinality selector(value, ordinality);

    select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
      'type', normalized.selector_type,
      'selection_key', normalized.selection_key,
      'week_ending_date', normalized.week_ending_date,
      'client_id', normalized.client_id,
      'candidate_id', normalized.candidate_id,
      'status_code', normalized.status_code
    )) order by normalized.rule_sequence), '[]'::jsonb)
    into v_normalized_group_selectors
    from private._invoice_batch_selection_rules_v2(jsonb_build_object(
      'contract_version', 'INVOICE_BATCH_SELECTION_V2',
      'mode', 'IMPLICIT_ALL',
      'default_selected', true,
      'rules', v_group_selector_rules
    )) normalized;

    if (
      select count(*)
      from jsonb_array_elements(v_normalized_group_selectors)
    ) <> (
      select count(distinct selector.value)
      from jsonb_array_elements(v_normalized_group_selectors) selector(value)
    ) then
      raise exception using
        errcode = '22023',
        message = 'BATCH_SELECTION_SELECTOR_INVALID';
    end if;
  end if;

  if v_mode = 'EXPLICIT_KEYS' and (
    jsonb_typeof(v_query->'selection_keys') is distinct from 'array'
    or jsonb_array_length(v_query->'selection_keys') < 1
    or jsonb_array_length(v_query->'selection_keys') > 100
    or jsonb_typeof(v_query->'expected_source_revisions')
       is distinct from 'object'
    or exists (
      select 1
      from jsonb_array_elements(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      ) key_item(value)
      where jsonb_typeof(key_item.value) is distinct from 'string'
         or nullif(btrim(key_item.value #>> '{}'), '') is null
         or length(btrim(key_item.value #>> '{}')) > 512
    )
    or (
      select count(*)
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      )
    ) <> (
      select count(distinct key_value)
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      )
        explicit_key(key_value)
    )
    or exists (
      select 1
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      )
        explicit_key(key_value)
      where nullif(
        v_query->'expected_source_revisions'->>explicit_key.key_value,
        ''
      ) is null
    )
    or exists (
      select 1
      from jsonb_each(
        case
          when jsonb_typeof(
            v_query->'expected_source_revisions'
          ) = 'object'
            then v_query->'expected_source_revisions'
          else '{}'::jsonb
        end
      ) expected_revision(selection_key,value)
      where not (
        v_query->'selection_keys' ? expected_revision.selection_key
      )
         or jsonb_typeof(expected_revision.value) <> 'string'
         or nullif(
              btrim(expected_revision.value #>> '{}'),
              ''
            ) is null
         or length(
              btrim(expected_revision.value #>> '{}')
            ) > 512
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_EXPLICIT_KEYS_INVALID';
  end if;

  select jsonb_build_object(
    'client_ids', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct lower(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'client_ids', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'candidate_ids', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct lower(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'candidate_ids', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'week_endings', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct (item.value::date)::text value
        from jsonb_array_elements_text(
          coalesce(v_filters->'week_endings', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'week_ending_from', case
      when nullif(v_filters->>'week_ending_from', '') is null then null
      else ((v_filters->>'week_ending_from')::date)::text
    end,
    'week_ending_to', case
      when nullif(v_filters->>'week_ending_to', '') is null then null
      else ((v_filters->>'week_ending_to')::date)::text
    end,
    'status_codes', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct upper(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'status_codes', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'blocker_codes', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct upper(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'blocker_codes', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'search', nullif(btrim(v_filters->>'search'), ''),
    'allow_early', coalesce((v_filters->>'allow_early')::boolean, false),
    'display_mode', upper(coalesce(
      nullif(v_filters->>'display_mode', ''),
      'ALL'
    )),
    'invoice_streams', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct upper(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'invoice_streams', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb)
  ) into v_normalized_filters;

  v_normalized_sort := jsonb_build_object(
    'group_preset', upper(coalesce(
      nullif(v_sort->>'group_preset', ''),
      'WEEK_CLIENT_CANDIDATE'
    )),
    'sort_key', upper(coalesce(
      nullif(v_sort->>'sort_key', ''),
      'WEEK_ENDING_DATE'
    )),
    'sort_direction', upper(coalesce(
      nullif(v_sort->>'sort_direction', ''),
      'ASC'
    ))
  );

  v_normalized_query := jsonb_build_object(
    'contract_version', 'INVOICE_BATCH_QUERY_V2',
    'action', v_action,
    'mode', v_mode,
    'snapshot', v_snapshot,
    'filters', v_normalized_filters,
    'sort', v_normalized_sort,
    'selection', v_selection
  );

  if v_mode in ('PAGE', 'EXPAND_SELECTION') then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'page_size', v_page_size,
      'cursor', case
        when jsonb_typeof(v_query->'cursor') = 'object' then v_cursor
        else null
      end
    );
  elsif v_mode = 'FACETS' then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'facet_request', v_facet_request
    );
  elsif v_mode = 'SUMMARY' then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'group_selectors', v_normalized_group_selectors
    );
  elsif v_mode = 'EXPLICIT_KEYS' then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'selection_keys', v_query->'selection_keys',
      'expected_source_revisions', v_query->'expected_source_revisions'
    );
  end if;

  return v_normalized_query;
end;
$function$;

-- private._invoice_batch_selection_rules_v1(jsonb)
CREATE OR REPLACE FUNCTION private._invoice_batch_selection_rules_v1(p_selection jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(rule_sequence integer, action text, selector_type text, selection_key text, week_ending_date date, client_id uuid, candidate_id uuid)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_selection jsonb := coalesce(p_selection,'{}'::jsonb);
  v_rules jsonb;
  v_rule_count integer;
begin
  if jsonb_typeof(v_selection) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_INVALID';
  end if;

  if coalesce(v_selection->>'contract_version','') <> 'INVOICE_BATCH_SELECTION_V1' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_CONTRACT_INVALID';
  end if;

  if upper(coalesce(v_selection->>'mode','')) <> 'IMPLICIT_ALL' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_MODE_UNSUPPORTED';
  end if;

  if coalesce(v_selection->>'default_selected','false') not in ('true','t','1','yes','on') then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_DEFAULT_INVALID';
  end if;

  v_rules := coalesce(v_selection->'rules','[]'::jsonb);
  if jsonb_typeof(v_rules) is distinct from 'array' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULES_INVALID';
  end if;

  v_rule_count := jsonb_array_length(v_rules);
  if v_rule_count > 10000 then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_LIMIT_EXCEEDED';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) with ordinality raw(rule, ordinal)
    where jsonb_typeof(raw.rule) is distinct from 'object'
       or jsonb_typeof(raw.rule->'selector') is distinct from 'object'
       or coalesce(raw.rule->>'sequence','') !~ '^[1-9][0-9]{0,8}$'
       or upper(coalesce(raw.rule->>'action','')) not in ('INCLUDE','EXCLUDE')
       or upper(coalesce(raw.rule#>>'{selector,type}','')) not in (
          'ROW','WEEK','CLIENT','CANDIDATE','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE'
       )
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_INVALID';
  end if;

  if exists (
    select 1
    from (
      select (rule->>'sequence')::integer seq, count(*) count_rows
      from jsonb_array_elements(v_rules) raw(rule)
      group by (rule->>'sequence')::integer
    ) duplicate_rules
    where duplicate_rules.count_rows > 1
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_SEQUENCE_DUPLICATE';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral jsonb_object_keys(raw.rule) key_name
    where key_name not in ('sequence','action','selector')
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral jsonb_object_keys(raw.rule->'selector') key_name
    where key_name not in ('type','selection_key','week_ending_date','client_id','candidate_id')
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_SELECTOR_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral (
      select upper(raw.rule#>>'{selector,type}') selector_type,
             btrim(coalesce(raw.rule#>>'{selector,selection_key}','')) selection_key,
             btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')) week_ending_text,
             btrim(coalesce(raw.rule#>>'{selector,client_id}','')) client_id_text,
             btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')) candidate_id_text
    ) selector_values
    where (selector_values.selector_type = 'ROW'
             and (selector_values.selection_key = '' or length(selector_values.selection_key) > 512))
       or (selector_values.selector_type in ('WEEK','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE')
             and not pg_input_is_valid(selector_values.week_ending_text,'date'))
       or (selector_values.selector_type in ('CLIENT','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE')
             and not pg_input_is_valid(selector_values.client_id_text,'uuid'))
       or (selector_values.selector_type in ('CANDIDATE','WEEK_CLIENT_CANDIDATE')
             and not pg_input_is_valid(selector_values.candidate_id_text,'uuid'))
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_SELECTOR_INVALID';
  end if;

  return query
  select
    (raw.rule->>'sequence')::integer as rule_sequence,
    upper(raw.rule->>'action') as action,
    upper(raw.rule#>>'{selector,type}') as selector_type,
    nullif(btrim(raw.rule#>>'{selector,selection_key}'),'') as selection_key,
    case when pg_input_is_valid(btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')),'date')
      then btrim(raw.rule#>>'{selector,week_ending_date}')::date end as week_ending_date,
    case when pg_input_is_valid(btrim(coalesce(raw.rule#>>'{selector,client_id}','')),'uuid')
      then btrim(raw.rule#>>'{selector,client_id}')::uuid end as client_id,
    case when pg_input_is_valid(btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')),'uuid')
      then btrim(raw.rule#>>'{selector,candidate_id}')::uuid end as candidate_id
  from jsonb_array_elements(v_rules) raw(rule)
  order by (raw.rule->>'sequence')::integer;
end;
$function$;

-- private._invoice_batch_selection_rules_v2(jsonb)
CREATE OR REPLACE FUNCTION private._invoice_batch_selection_rules_v2(p_selection jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(rule_sequence integer, action text, selector_type text, selection_key text, group_key text, week_ending_date date, client_id uuid, candidate_id uuid, status_code text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_selection jsonb := coalesce(p_selection, '{}'::jsonb);
  v_rules jsonb;
begin
  if jsonb_typeof(v_selection) is distinct from 'object' then
    raise exception using errcode='22023', message='BATCH_SELECTION_INVALID';
  end if;

  if exists (
    select 1 from jsonb_object_keys(v_selection) key_name
    where key_name not in ('contract_version','mode','default_selected','rules')
  ) then
    raise exception using errcode='22023', message='BATCH_SELECTION_UNKNOWN_FIELD';
  end if;

  if coalesce(v_selection->>'contract_version','') <>
      'INVOICE_BATCH_SELECTION_V2' then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_CONTRACT_INVALID';
  end if;

  if upper(coalesce(v_selection->>'mode','')) <> 'IMPLICIT_ALL' then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_MODE_UNSUPPORTED';
  end if;

  if jsonb_typeof(v_selection->'default_selected') is distinct from 'boolean'
     or coalesce((v_selection->>'default_selected')::boolean, false) is not true then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_DEFAULT_INVALID';
  end if;

  v_rules := coalesce(v_selection->'rules', '[]'::jsonb);
  if jsonb_typeof(v_rules) is distinct from 'array' then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULES_INVALID';
  end if;

  if jsonb_array_length(v_rules) > 10000 then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_LIMIT_EXCEEDED';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) with ordinality raw(rule, ordinal)
    where jsonb_typeof(raw.rule) is distinct from 'object'
       or jsonb_typeof(raw.rule->'selector') is distinct from 'object'
       or exists (
         select 1 from jsonb_object_keys(raw.rule) key_name
         where key_name not in ('sequence','action','selector')
       )
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    where coalesce(raw.rule->>'sequence','') !~ '^[1-9][0-9]{0,8}$'
       or upper(coalesce(raw.rule->>'action','')) not in ('INCLUDE','EXCLUDE')
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_SEQUENCE_INVALID';
  end if;

  if exists (
    select 1
    from (
      select
        (rule->>'sequence')::integer sequence_no,
        lag((rule->>'sequence')::integer) over (order by ordinal) previous_sequence
      from jsonb_array_elements(v_rules) with ordinality raw(rule, ordinal)
    ) ordered_rules
    where previous_sequence is not null
      and sequence_no <= previous_sequence
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_SEQUENCE_INVALID';
  end if;

  if exists (
    select 1
    from (
      select (rule->>'sequence')::integer sequence_no, count(*) row_count
      from jsonb_array_elements(v_rules) raw(rule)
      group by (rule->>'sequence')::integer
    ) duplicate_rules
    where row_count > 1
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_SEQUENCE_DUPLICATE';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral (
      select
        upper(coalesce(raw.rule#>>'{selector,type}','')) selector_type,
        array(
          select key_name
          from jsonb_object_keys(raw.rule->'selector') key_name
          where key_name <> 'type'
            and raw.rule->'selector'->key_name <> 'null'::jsonb
            and nullif(btrim(raw.rule->'selector'->>key_name),'') is not null
          order by key_name
        ) supplied_fields
    ) selector
    cross join lateral (
      select case selector.selector_type
        when 'ROW' then array['selection_key']::text[]
        when 'WEEK' then array['week_ending_date']::text[]
        when 'CLIENT' then array['client_id']::text[]
        when 'CANDIDATE' then array['candidate_id']::text[]
        when 'STATUS' then array['status_code']::text[]
        when 'WEEK_CLIENT' then array['client_id','week_ending_date']::text[]
        when 'WEEK_CLIENT_CANDIDATE'
          then array['candidate_id','client_id','week_ending_date']::text[]
        when 'STATUS_WEEK' then array['status_code','week_ending_date']::text[]
        when 'STATUS_WEEK_CLIENT'
          then array['client_id','status_code','week_ending_date']::text[]
        when 'DIMENSION_GROUP'
          then selector.supplied_fields
      end expected_fields
    ) expected
    where selector.selector_type not in (
      'ROW','WEEK','CLIENT','CANDIDATE','STATUS','WEEK_CLIENT',
      'WEEK_CLIENT_CANDIDATE','STATUS_WEEK','STATUS_WEEK_CLIENT',
      'DIMENSION_GROUP'
    )
       or expected.expected_fields is null
       or selector.supplied_fields is distinct from expected.expected_fields
       or (
         selector.selector_type = 'DIMENSION_GROUP'
         and (
           cardinality(selector.supplied_fields) = 0
           or exists (
             select 1
             from unnest(selector.supplied_fields) supplied_field
             where supplied_field not in (
               'week_ending_date','client_id','candidate_id','status_code'
             )
           )
         )
       )
       or exists (
         select 1
         from jsonb_object_keys(raw.rule->'selector') key_name
         where key_name not in (
           'type','selection_key','week_ending_date',
           'client_id','candidate_id','status_code'
         )
       )
       or (
         selector.selector_type = 'ROW'
         and (
           length(btrim(raw.rule#>>'{selector,selection_key}')) > 512
           or btrim(raw.rule#>>'{selector,selection_key}') = ''
         )
       )
       or (
         selector.selector_type in (
           'WEEK','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE',
           'STATUS_WEEK','STATUS_WEEK_CLIENT'
         )
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')),
           'date'
         )
       )
       or (
         selector.selector_type = 'DIMENSION_GROUP'
         and 'week_ending_date' = any(selector.supplied_fields)
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')),
           'date'
         )
       )
       or (
         selector.selector_type in (
           'CLIENT','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE',
           'STATUS_WEEK_CLIENT'
         )
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,client_id}','')),
           'uuid'
         )
       )
       or (
         selector.selector_type = 'DIMENSION_GROUP'
         and 'client_id' = any(selector.supplied_fields)
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,client_id}','')),
           'uuid'
         )
       )
       or (
         selector.selector_type in ('CANDIDATE','WEEK_CLIENT_CANDIDATE')
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')),
           'uuid'
         )
       )
       or (
         selector.selector_type = 'DIMENSION_GROUP'
         and 'candidate_id' = any(selector.supplied_fields)
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')),
           'uuid'
         )
       )
       or (
         selector.selector_type in ('STATUS','STATUS_WEEK','STATUS_WEEK_CLIENT')
         and (
           btrim(coalesce(raw.rule#>>'{selector,status_code}','')) = ''
           or length(btrim(raw.rule#>>'{selector,status_code}')) > 120
         )
       )
       or (
         selector.selector_type = 'DIMENSION_GROUP'
         and 'status_code' = any(selector.supplied_fields)
         and (
           btrim(coalesce(raw.rule#>>'{selector,status_code}','')) = ''
           or length(btrim(raw.rule#>>'{selector,status_code}')) > 120
         )
       )
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_SELECTOR_INVALID';
  end if;

  return query
  select
    (raw.rule->>'sequence')::integer,
    upper(raw.rule->>'action'),
    upper(raw.rule#>>'{selector,type}'),
    nullif(btrim(raw.rule#>>'{selector,selection_key}'),''),
    null::text,
    case when pg_input_is_valid(
      btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')),
      'date'
    ) then btrim(raw.rule#>>'{selector,week_ending_date}')::date end,
    case when pg_input_is_valid(
      btrim(coalesce(raw.rule#>>'{selector,client_id}','')),
      'uuid'
    ) then btrim(raw.rule#>>'{selector,client_id}')::uuid end,
    case when pg_input_is_valid(
      btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')),
      'uuid'
    ) then btrim(raw.rule#>>'{selector,candidate_id}')::uuid end,
    nullif(upper(btrim(raw.rule#>>'{selector,status_code}')),'')
  from jsonb_array_elements(v_rules) raw(rule)
  order by (raw.rule->>'sequence')::integer;
end;
$function$;

-- private._invoice_candidate_revision_trigger_v2()
CREATE OR REPLACE FUNCTION private._invoice_candidate_revision_trigger_v2()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_generate boolean := coalesce(tg_argv[0], 'false')::boolean;
  v_issue boolean := coalesce(tg_argv[1], 'false')::boolean;
  v_fields text[] := coalesce(tg_argv[2:array_length(tg_argv, 1)], array[]::text[]);
  v_identity_fields text[];
  v_compare_fields text[];
  v_changed boolean := false;
  v_qualifier text := 'true';
  v_sql text;
begin
  if tg_level <> 'STATEMENT' then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_CANDIDATE_REVISION_TRIGGER_MUST_BE_STATEMENT_LEVEL';
  end if;

  if tg_table_name = 'invoice_operations' then
    -- Selection roots, manifest bookkeeping, and durable operation-control
    -- receipts are not candidate work.
    v_qualifier :=
      $$coalesce(entity_type, '') <> 'INVOICE_BATCH'
        and coalesce(operation_type, '') <> 'OPERATION_CONTROL_REQUEST'$$;
  elsif tg_table_name = 'invoice_operation_chunks' then
    -- Never let an expander, an uncommitted carrier, or an outcome-only
    -- root-owned carrier invalidate the snapshot that is constructing it.
    v_qualifier :=
      $$coalesce(payload_json->>'is_selection_expander','false') <> 'true'
        and not (is_manifest_member and not manifest_committed)
        and not (is_manifest_member and coalesce(entity_type,'') = 'OPERATION')$$;
  end if;

  select coalesce(array_agg(a.attname order by key_column.ordinality), array[]::text[])
  into v_identity_fields
  from pg_index i
  cross join lateral unnest(i.indkey) with ordinality key_column(attnum, ordinality)
  join pg_attribute a
    on a.attrelid = i.indrelid
   and a.attnum = key_column.attnum
  where i.indrelid = tg_relid
    and i.indisprimary;

  if cardinality(v_identity_fields) = 0 then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_CANDIDATE_REVISION_PRIMARY_KEY_REQUIRED';
  end if;

  select array_agg(field_name order by field_name)
  into v_compare_fields
  from (
    select distinct field_name
    from unnest(v_identity_fields || v_fields) field_name
  ) compared;

  if tg_op = 'INSERT' then
    v_sql := format(
      'select exists(select 1 from new_rows where %s)',
      v_qualifier
    );
    execute v_sql into v_changed;
  elsif tg_op = 'DELETE' then
    v_sql := format(
      'select exists(select 1 from old_rows where %s)',
      v_qualifier
    );
    execute v_sql into v_changed;
  elsif tg_op = 'UPDATE' then
    if tg_table_name = 'invoice_operations' then
      select exists (
        select 1
        from old_rows o
        full join new_rows n on n.id = o.id
        where (
          (
            o.id is not null
            and coalesce(o.entity_type, '') <> 'INVOICE_BATCH'
            and coalesce(o.operation_type, '') <> 'OPERATION_CONTROL_REQUEST'
          ) <> (
            n.id is not null
            and coalesce(n.entity_type, '') <> 'INVOICE_BATCH'
            and coalesce(n.operation_type, '') <> 'OPERATION_CONTROL_REQUEST'
          )
        ) or (
          o.id is not null
          and n.id is not null
          and coalesce(o.entity_type, '') <> 'INVOICE_BATCH'
          and coalesce(n.entity_type, '') <> 'INVOICE_BATCH'
          and coalesce(o.operation_type, '') <> 'OPERATION_CONTROL_REQUEST'
          and coalesce(n.operation_type, '') <> 'OPERATION_CONTROL_REQUEST'
          and jsonb_build_object(
            'id', o.id,
            'parent_operation_id', o.parent_operation_id,
            'operation_type', o.operation_type,
            'entity_type', o.entity_type,
            'entity_id', o.entity_id,
            'status', o.status,
            'phase', o.phase,
            'source_revision', o.source_revision,
            'template_version', o.template_version,
            'control_version', o.control_version,
            'manifest_generation', o.manifest_generation,
            'manifest_committed', o.manifest_committed,
            'release_complete', o.release_complete,
            'legal_issue_state',
              o.result_json->'legal_issue_state',
            'delivery_state', o.result_json->'delivery_state',
            'document_version_id',
              o.result_json->'document_version_id',
            'issued_document_version_id',
              o.result_json->'issued_document_version_id',
            'error_code', o.error_json->'code'
          ) is distinct from jsonb_build_object(
            'id', n.id,
            'parent_operation_id', n.parent_operation_id,
            'operation_type', n.operation_type,
            'entity_type', n.entity_type,
            'entity_id', n.entity_id,
            'status', n.status,
            'phase', n.phase,
            'source_revision', n.source_revision,
            'template_version', n.template_version,
            'control_version', n.control_version,
            'manifest_generation', n.manifest_generation,
            'manifest_committed', n.manifest_committed,
            'release_complete', n.release_complete,
            'legal_issue_state',
              n.result_json->'legal_issue_state',
            'delivery_state', n.result_json->'delivery_state',
            'document_version_id',
              n.result_json->'document_version_id',
            'issued_document_version_id',
              n.result_json->'issued_document_version_id',
            'error_code', n.error_json->'code'
          )
        )
      ) into v_changed;
    elsif tg_table_name = 'invoice_operation_chunks' then
      select exists (
        select 1
        from old_rows o
        join new_rows n on n.id = o.id
        where coalesce(
                o.payload_json->>'is_selection_expander',
                'false'
              ) <> 'true'
          and coalesce(
                n.payload_json->>'is_selection_expander',
                'false'
              ) <> 'true'
          and not (o.is_manifest_member and not o.manifest_committed)
          and not (n.is_manifest_member and not n.manifest_committed)
          and not (
            o.is_manifest_member
            and coalesce(o.entity_type, '') = 'OPERATION'
          )
          and not (
            n.is_manifest_member
            and coalesce(n.entity_type, '') = 'OPERATION'
          )
          and jsonb_build_object(
            'id', o.id,
            'operation_id', o.operation_id,
            'chunk_type', o.chunk_type,
            'entity_type', o.entity_type,
            'entity_id', o.entity_id,
            'document_version_id', o.document_version_id,
            'document_asset_id', o.document_asset_id,
            'input_document_version_id', o.input_document_version_id,
            'status', o.status,
            'phase', o.phase,
            'replaced_by_chunk_id', o.replaced_by_chunk_id,
            'manifest_generation', o.manifest_generation,
            'is_manifest_member', o.is_manifest_member,
            'manifest_committed', o.manifest_committed,
            'result_visible', o.result_visible,
            'selection_key', o.selection_key,
            'result_category', o.result_category,
            'blocked_for_sending',
              o.payload_json->'blocked_for_sending',
            'row_kind', o.payload_json->'row_kind',
            'source_revision', o.payload_json->'source_revision',
            'document_result_version_id',
              o.result_json->'document_version_id',
            'issued_document_version_id',
              o.result_json->'issued_document_version_id',
            'legal_issue_state',
              o.result_json->'legal_issue_state',
            'delivery_state', o.result_json->'delivery_state',
            'error_code', o.error_json->'code'
          ) is distinct from jsonb_build_object(
            'id', n.id,
            'operation_id', n.operation_id,
            'chunk_type', n.chunk_type,
            'entity_type', n.entity_type,
            'entity_id', n.entity_id,
            'document_version_id', n.document_version_id,
            'document_asset_id', n.document_asset_id,
            'input_document_version_id', n.input_document_version_id,
            'status', n.status,
            'phase', n.phase,
            'replaced_by_chunk_id', n.replaced_by_chunk_id,
            'manifest_generation', n.manifest_generation,
            'is_manifest_member', n.is_manifest_member,
            'manifest_committed', n.manifest_committed,
            'result_visible', n.result_visible,
            'selection_key', n.selection_key,
            'result_category', n.result_category,
            'blocked_for_sending',
              n.payload_json->'blocked_for_sending',
            'row_kind', n.payload_json->'row_kind',
            'source_revision', n.payload_json->'source_revision',
            'document_result_version_id',
              n.result_json->'document_version_id',
            'issued_document_version_id',
              n.result_json->'issued_document_version_id',
            'legal_issue_state',
              n.result_json->'legal_issue_state',
            'delivery_state', n.result_json->'delivery_state',
            'error_code', n.error_json->'code'
          )
      ) into v_changed;
    else
      v_sql := format($sql$
        select exists (
          (
            select private._invoice_jsonb_pick_v2(to_jsonb(o), $1)
            from old_rows o
            where %1$s
            except all
            select private._invoice_jsonb_pick_v2(to_jsonb(n), $1)
            from new_rows n
            where %1$s
          )
          union all
          (
            select private._invoice_jsonb_pick_v2(to_jsonb(n), $1)
            from new_rows n
            where %1$s
            except all
            select private._invoice_jsonb_pick_v2(to_jsonb(o), $1)
            from old_rows o
            where %1$s
          )
        )
      $sql$, v_qualifier);
      execute v_sql into v_changed using v_compare_fields;
    end if;
  end if;

  if v_changed then
    perform private._invoice_candidate_snapshot_bump_v2(
      v_generate,
      v_issue,
      tg_op,
      tg_table_schema || '.' || tg_table_name,
      statement_timestamp()
    );
  end if;

  return null;
end;
$function$;

-- private._invoice_candidate_snapshot_bump_v2(boolean,boolean,text,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_candidate_snapshot_bump_v2(p_generate boolean, p_issue boolean, p_reason text, p_source text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_generate_revision bigint;
  v_issue_revision bigint;
begin
  if coalesce(p_generate, false) then
    perform pg_advisory_xact_lock(
      hashtextextended('invoice-candidate-snapshot:GENERATE', 0)
    );
    v_generate_revision :=
      nextval('private.invoice_generate_candidate_change_seq'::regclass);
  else
    v_generate_revision := coalesce(pg_sequence_last_value(
      'private.invoice_generate_candidate_change_seq'::regclass
    ), 0);
  end if;

  if coalesce(p_issue, false) then
    perform pg_advisory_xact_lock(
      hashtextextended('invoice-candidate-snapshot:ISSUE', 0)
    );
    v_issue_revision :=
      nextval('private.invoice_issue_candidate_change_seq'::regclass);
  else
    v_issue_revision := coalesce(pg_sequence_last_value(
      'private.invoice_issue_candidate_change_seq'::regclass
    ), 0);
  end if;

  return jsonb_build_object(
    'generate_revision', v_generate_revision::text,
    'issue_revision', v_issue_revision::text,
    'reason', left(coalesce(nullif(btrim(p_reason), ''), 'CANDIDATE_VISIBLE_CHANGE'), 120),
    'source', left(coalesce(nullif(btrim(p_source), ''), 'UNKNOWN'), 120),
    'at_utc', to_char(
      coalesce(p_now_utc, now()) at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    )
  );
end;
$function$;

-- private._invoice_candidate_snapshot_get_v2(text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_candidate_snapshot_get_v2(p_action text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'vault', 'pg_temp'
AS $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_now timestamptz := date_trunc(
    'milliseconds',
    coalesce(p_now_utc, statement_timestamp())
  );
  v_expires timestamptz;
  v_revision bigint;
  v_key_id text;
  v_secret text;
  v_payload jsonb;
  v_payload_b64 text;
  v_signing_input text;
  v_signature text;
begin
  if v_action not in ('GENERATE', 'ISSUE') then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_ACTION_INVALID';
  end if;

  -- Linearise snapshot issuance with candidate-visible revision changes.
  perform pg_advisory_xact_lock_shared(
    hashtextextended('invoice-candidate-snapshot:' || v_action, 0)
  );

  if v_action = 'GENERATE' then
    select coalesce(pg_sequence_last_value(
      'private.invoice_generate_candidate_change_seq'::regclass
    ), 0)
    into v_revision;
  else
    select coalesce(pg_sequence_last_value(
      'private.invoice_issue_candidate_change_seq'::regclass
    ), 0)
    into v_revision;
  end if;

  select
    k.key_id,
    s.decrypted_secret
  into
    v_key_id,
    v_secret
  from private.invoice_async_snapshot_hmac_keys k
  join vault.decrypted_secrets s on s.id = k.vault_secret_id
  where k.is_current
    and k.active_from_utc <= v_now
    and (k.active_to_utc is null or k.active_to_utc > v_now)
  order by k.active_from_utc desc, k.key_id
  limit 1;

  if v_key_id is null or v_secret is null then
    raise exception using
      errcode = '55000',
      message = 'BATCH_SNAPSHOT_SIGNING_KEY_UNAVAILABLE';
  end if;

  v_expires := v_now + interval '30 minutes';
  v_payload := jsonb_build_object(
    'action', v_action,
    'at_utc', to_char(
      v_now at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ),
    'contract_version', 'INVOICE_BATCH_SNAPSHOT_V2',
    'expires_at_utc', to_char(
      v_expires at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ),
    'revision', v_revision::text
  );
  v_payload_b64 := rtrim(
    replace(replace(replace(replace(
      encode(convert_to(v_payload::text, 'UTF8'), 'base64'),
      E'\n',
      ''
    ), E'\r', ''), '+', '-'), '/', '_'),
    '='
  );
  v_signing_input := 'v2.' || v_key_id || '.' || v_payload_b64;
  v_signature := encode(
    extensions.hmac(
      convert_to(v_signing_input, 'UTF8'),
      convert_to(v_secret, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  return v_payload || jsonb_build_object(
    'key_id', v_key_id,
    'token', v_signing_input || '.' || v_signature
  );
end;
$function$;

-- private._invoice_candidate_snapshot_verify_v2(text,jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_candidate_snapshot_verify_v2(p_action text, p_snapshot jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'vault', 'pg_temp'
AS $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_now timestamptz := date_trunc(
    'milliseconds',
    coalesce(p_now_utc, statement_timestamp())
  );
  v_at timestamptz;
  v_expires timestamptz;
  v_revision bigint;
  v_current_revision bigint;
  v_token text;
  v_parts text[];
  v_key_id text;
  v_secret text;
  v_payload jsonb;
  v_payload_b64 text;
  v_signing_input text;
  v_expected_signature text;
begin
  if v_action not in ('GENERATE', 'ISSUE') then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_ACTION_INVALID';
  end if;

  if jsonb_typeof(coalesce(p_snapshot, 'null'::jsonb)) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(p_snapshot) key_name
       where key_name not in (
         'contract_version',
         'action',
         'at_utc',
         'revision',
         'expires_at_utc',
         'key_id',
         'token'
       )
     )
     or coalesce(p_snapshot->>'contract_version', '') <>
        'INVOICE_BATCH_SNAPSHOT_V2'
     or upper(coalesce(p_snapshot->>'action', '')) <> v_action
     or coalesce(p_snapshot->>'revision', '') !~ '^[0-9]+$'
     or coalesce(p_snapshot->>'key_id', '') !~
        '^[a-z0-9][a-z0-9._-]{0,63}$'
     or not pg_input_is_valid(
       coalesce(p_snapshot->>'at_utc', ''),
       'timestamp with time zone'
     )
     or not pg_input_is_valid(
       coalesce(p_snapshot->>'expires_at_utc', ''),
       'timestamp with time zone'
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  v_at := date_trunc(
    'milliseconds',
    (p_snapshot->>'at_utc')::timestamptz
  );
  v_expires := date_trunc(
    'milliseconds',
    (p_snapshot->>'expires_at_utc')::timestamptz
  );
  v_revision := (p_snapshot->>'revision')::bigint;
  v_key_id := p_snapshot->>'key_id';
  v_token := coalesce(p_snapshot->>'token', '');
  v_parts := string_to_array(v_token, '.');

  if cardinality(v_parts) <> 4
     or v_parts[1] <> 'v2'
     or v_parts[2] !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
     or v_parts[2] is distinct from v_key_id
     or v_parts[3] !~ '^[A-Za-z0-9_-]+$'
     or v_parts[4] !~ '^[0-9a-f]{64}$'
     or v_expires is distinct from v_at + interval '30 minutes' then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  if v_expires <= v_now then
    raise exception using
      errcode = '40001',
      message = 'BATCH_SNAPSHOT_EXPIRED';
  end if;

  select s.decrypted_secret
  into v_secret
  from private.invoice_async_snapshot_hmac_keys k
  join vault.decrypted_secrets s on s.id = k.vault_secret_id
  where k.key_id = v_key_id
    and k.active_from_utc <= v_at
    and (k.active_to_utc is null or v_at < k.active_to_utc)
    and (
      k.is_current
      or (
        k.verify_until_utc is not null
        and v_now <= k.verify_until_utc
      )
    )
  limit 1;

  if v_secret is null then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_SIGNATURE_INVALID';
  end if;

  v_payload := jsonb_build_object(
    'action', v_action,
    'at_utc', to_char(
      v_at at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ),
    'contract_version', 'INVOICE_BATCH_SNAPSHOT_V2',
    'expires_at_utc', to_char(
      v_expires at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ),
    'revision', v_revision::text
  );
  v_payload_b64 := rtrim(
    replace(replace(replace(replace(
      encode(convert_to(v_payload::text, 'UTF8'), 'base64'),
      E'\n',
      ''
    ), E'\r', ''), '+', '-'), '/', '_'),
    '='
  );
  v_signing_input := 'v2.' || v_key_id || '.' || v_payload_b64;
  v_expected_signature := encode(
    extensions.hmac(
      convert_to(v_signing_input, 'UTF8'),
      convert_to(v_secret, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  if v_parts[3] is distinct from v_payload_b64
     or extensions.digest(convert_to(v_parts[4], 'UTF8'), 'sha256')
        is distinct from extensions.digest(
          convert_to(v_expected_signature, 'UTF8'),
          'sha256'
        ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_SIGNATURE_INVALID';
  end if;

  perform pg_advisory_xact_lock_shared(
    hashtextextended('invoice-candidate-snapshot:' || v_action, 0)
  );

  if v_action = 'GENERATE' then
    select coalesce(pg_sequence_last_value(
      'private.invoice_generate_candidate_change_seq'::regclass
    ), 0)
    into v_current_revision;
  else
    select coalesce(pg_sequence_last_value(
      'private.invoice_issue_candidate_change_seq'::regclass
    ), 0)
    into v_current_revision;
  end if;

  if v_current_revision is distinct from v_revision then
    raise exception using
      errcode = '40001',
      message = 'BATCH_SNAPSHOT_CHANGED';
  end if;

  return v_payload || jsonb_build_object(
    'key_id', v_key_id,
    'token', v_token
  );
end;
$function$;

-- private._invoice_candidate_triggers_install_v2()
CREATE OR REPLACE FUNCTION private._invoice_candidate_triggers_install_v2()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_item record;
  v_argument_list text;
  v_trigger_base text;
  v_installed integer := 0;
  v_manifest_rows jsonb := '[]'::jsonb;
begin
  if to_regprocedure('private._invoice_candidate_revision_trigger_v2()') is null
     or to_regprocedure('private._invoice_result_page_revision_trigger_v2()') is null then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_ASYNC_V8_TRIGGER_HELPER_NOT_INSTALLED';
  end if;

  for v_item in
    select manifest.table_name, manifest.bump_generate,
      manifest.bump_issue, manifest.fields,
      coalesce(manifest.json_paths,'[]'::jsonb) json_paths
    from jsonb_to_recordset($manifest$
    [
      {"table_name":"contracts","bump_generate":true,"bump_issue":true,"fields":["candidate_id","client_id","start_date","end_date","pay_method_snapshot","rates_json","std_hours_json","default_submission_mode","week_ending_weekday_snapshot","auto_invoice","require_reference_to_invoice","mileage_charge_rate","additional_rates_json","self_bill","weekly_timesheet_source","no_timesheet_required","daily_calc_of_invoices","group_nightsat_sunbh","is_nhsp","autoprocess_hr","requires_hr","hr_attach_to_invoice","ts_attach_to_invoice","overrideclientsettings","reference_number_required_to_issue_invoice","send_manual_invoices_to_different_email","manual_invoices_alt_email_address","is_ad_hoc","healthroster_import_auto_authorise_override","nhsp_import_auto_authorise_override"]},
      {"table_name":"contract_weeks","bump_generate":true,"bump_issue":true,"fields":["contract_id","week_ending_date","additional_seq","status","submission_mode_snapshot","timesheet_id","day_entries_json","totals_json","planned_schedule_json","is_adjustment","enforce_day_partition","allowed_days_mask","split_boundary_date","split_group_key"]},
      {"table_name":"client_settings","bump_generate":true,"bump_issue":true,"fields":["client_id","vat_rate_pct","effective_from","hr_validation_required","ts_reference_required","week_ending_weekday","autoprocess_hr","invoice_reference_required","default_submission_mode","is_nhsp","self_bill_no_invoices_sent","daily_calc_of_invoices","no_timesheet_required","group_nightsat_sunbh","requires_hr","hr_attach_to_invoice","ts_attach_to_invoice","auto_invoice_default","send_manual_invoices_to_different_email","manual_invoices_alt_email_address","invoice_consolidation_mode","reference_number_required_to_issue_invoice","reversal_complete_financials_date","reversal_replacement_financials_date"]},
      {"table_name":"clients","bump_generate":true,"bump_issue":true,"fields":["name","invoice_address","primary_invoice_email","vat_chargeable","payment_terms_days","ts_queries_email","client_address","contact_email"]},
      {"table_name":"settings_defaults","bump_generate":true,"bump_issue":true,"fields":["vat_registration_number","agency_name","agency_logo","registered_address","company_reg_number","bank_name","bank_sort_code","bank_account_number","finance_email","hr_attach_to_invoice","ts_attach_to_invoice","invoice_document_presentation_json","timesheet_header_json","timesheet_footer_json","temporary_worker_declaration_json","client_declaration_json"]},
      {"table_name":"settings_finance_windows","bump_generate":true,"bump_issue":false,"fields":["date_from","date_to","vat_rate_pct","mileage_charge_defaults"]},
      {"table_name":"candidates","bump_generate":true,"bump_issue":true,"fields":["first_name","last_name","display_name","active","key_norm"]},
      {"table_name":"nhsp_shifts","bump_generate":true,"bump_issue":false,"fields":["external_row_key","latest_import_id","candidate_id","client_id","contract_id","timesheet_id","work_date","ward","start_utc","end_utc","break_mins","pay_minutes","pay_amount_snapshot","charge_amount_snapshot","invoice_status","defer_until_run_after","invoice_id","source_system","hr_request_id","held_back_reason","assignment_code","ref_num","week_ending_date","cancelled_at_utc","cancelled_by_import_id","cancelled_reason"]},
      {"table_name":"invoices","bump_generate":true,"bump_issue":true,"fields":["client_id","invoice_no","type","original_invoice_id","subtotal_ex_vat","vat_amount","total_inc_vat","due_at_utc","notes","do_not_send","header_snapshot_json","status","issued_at_utc","paid_at_utc","on_hold_reason","document_revision","document_state","preview_document_version_id","issued_document_version_id","active_document_operation_id","issue_state","active_issue_operation_id","last_document_error_json"]},
      {"table_name":"invoice_lines","bump_generate":true,"bump_issue":true,"fields":["invoice_id","timesheet_id","booking_id","source_key","description","hours_day","hours_night","hours_sat","hours_sun","hours_bh","pay_day","pay_night","pay_sat","pay_sun","pay_bh","charge_day","charge_night","charge_sat","charge_sun","charge_bh","total_pay_ex_vat","total_charge_ex_vat","vat_rate_pct","vat_amount","total_inc_vat","margin_ex_vat","meta_json"]},
      {"table_name":"invoice_hr_source_rows","bump_generate":false,"bump_issue":true,"fields":["invoice_id","source_system","import_id","header_rows","header_columns","rows_json"]},
      {"table_name":"timesheets","bump_generate":true,"bump_issue":true,"fields":["booking_id","occupant_key_norm","hospital_norm","ward_norm","job_title_norm","shift_label_norm","scheduled_start_iso","scheduled_end_iso","worked_start_iso","worked_end_iso","break_start_iso","break_end_iso","break_minutes","worked_minutes","week_ending_date","auth_name","auth_job_title","authorised_at_server","r2_nurse_key","r2_auth_key","img_sha256_nurse","img_sha256_auth","reference_number","status","version","is_current","revoked_at","contract_id","submission_mode","line_type","sheet_scope","actual_schedule_json","additional_units_week","additional_units_per_day","day_references_json","qr_signed_hash","qr_signed_at_utc","qr_status","qr_r2_key","candidate_hint_text","band","is_adjustment","parent_timesheet_id","correction_id","correction_kind","adjustment_origin","archived_at_utc","archived_by_user_id","archived_reason_code","document_revision","document_state","current_document_version_id","active_document_operation_id","manual_document_asset_id","last_document_error_json"]},
      {"table_name":"timesheets_financials","bump_generate":true,"bump_issue":true,"fields":["timesheet_id","timesheet_version","basis","is_current","is_stale","worked_start_iso","worked_end_iso","break_start_iso","break_end_iso","break_minutes","candidate_id","client_id","role","band","policy_snapshot_json","rate_source_refs_json","hours_day","hours_night","hours_sat","hours_sun","hours_bh","pay_day","pay_night","pay_sat","pay_sun","pay_bh","charge_day","charge_night","charge_sat","charge_sun","charge_bh","total_hours","total_pay_ex_vat","total_charge_ex_vat","margin_ex_vat","processing_status","expenses_pay_ex_vat","expenses_charge_ex_vat","expenses_description","expenses_evidence_manifest","mileage_pay_ex_vat","mileage_charge_ex_vat","mileage_pay_rate","mileage_charge_rate","mileage_evidence_manifest","actual_schedule_json","actual_minutes_by_day_json","additional_units_json","additional_pay_ex_vat","additional_charge_ex_vat","additional_margin_ex_vat","invoice_breakdown_json","nhsp_import_id","has_rate_issue","hr_crosscheck_status","hr_crosscheck_issues","external_source_rows_json","mileage_units","travel_pay_ex_vat","travel_charge_ex_vat","accommodation_pay_ex_vat","accommodation_charge_ex_vat","other_pay_ex_vat","other_charge_ex_vat","stale_reason","pay_method","locked_by_invoice_id","unlocked_by_credit_note_id","po_number","pay_on_hold","pay_on_hold_reason","has_pay_channel_issue"]},
      {"table_name":"timesheet_evidence","bump_generate":true,"bump_issue":true,"fields":["timesheet_id","kind","storage_key","source_revision","display_name","document_asset_id","processing_state","processing_error_json"]},
      {"table_name":"invoice_document_versions","bump_generate":true,"bump_issue":true,"fields":["entity_type","entity_id","purpose","operation_id","source_revision","template_version","status","r2_key","sha256","size_bytes","page_count","ready_at_utc","verified_at_utc","superseded_at_utc","error_json"]},
      {"table_name":"invoice_document_assets","bump_generate":true,"bump_issue":true,"fields":["source_kind","source_id","source_revision","declared_media_type","detected_media_type","original_sha256","original_size_bytes","status","normalised_manifest_hash","normalised_r2_key","normalised_sha256","normalised_size_bytes","normalised_page_count","operation_id","error_json","ready_at_utc"]},
      {"table_name":"invoice_operations","bump_generate":true,"bump_issue":true,"fields":["parent_operation_id","operation_type","entity_type","entity_id","status","phase","source_revision","template_version","control_version","manifest_generation","manifest_committed","release_complete"],"json_paths":["result_json.legal_issue_state","result_json.delivery_state","result_json.document_version_id","result_json.issued_document_version_id","error_json.code"]},
      {"table_name":"invoice_operation_chunks","bump_generate":true,"bump_issue":true,"fields":["operation_id","chunk_type","entity_type","entity_id","document_version_id","document_asset_id","input_document_version_id","status","phase","replaced_by_chunk_id","manifest_generation","is_manifest_member","manifest_committed","result_visible","selection_key","result_category"],"json_paths":["payload_json.is_selection_expander","payload_json.blocked_for_sending","payload_json.row_kind","payload_json.source_revision","result_json.document_version_id","result_json.issued_document_version_id","result_json.legal_issue_state","result_json.delivery_state","error_json.code"]}
    ]
    $manifest$::jsonb) as manifest(
      table_name text,
      bump_generate boolean,
      bump_issue boolean,
      fields jsonb,
      json_paths jsonb
    )
  loop
    v_manifest_rows := v_manifest_rows || jsonb_build_array(
      jsonb_build_object(
        'table_name', v_item.table_name,
        'bump_generate', v_item.bump_generate,
        'bump_issue', v_item.bump_issue,
        'fields', v_item.fields,
        'json_paths', v_item.json_paths
      )
    );

    if to_regclass(format('public.%I', v_item.table_name)) is null then
      raise exception using
        errcode = '42P01',
        message = 'INVOICE_ASYNC_V8_TRIGGER_TABLE_MISSING:' ||
          v_item.table_name;
    end if;

    if exists (
      select 1
      from jsonb_array_elements_text(v_item.fields) field_name
      left join pg_attribute a
        on a.attrelid = format('public.%I', v_item.table_name)::regclass
       and a.attname = field_name.value
       and a.attnum > 0
       and not a.attisdropped
      where a.attname is null
    ) then
      raise exception using
        errcode = '42703',
        message = 'INVOICE_ASYNC_V8_TRIGGER_COLUMN_MISSING:' ||
          v_item.table_name;
    end if;

    select string_agg(quote_literal(field_name), ',' order by ordinal)
    into v_argument_list
    from jsonb_array_elements_text(v_item.fields)
      with ordinality field(field_name, ordinal);

    v_trigger_base := left(
      'trg_invoice_candidate_revision_v2_' || v_item.table_name,
      58
    );

    execute format(
      'drop trigger if exists %I on public.%I',
      v_trigger_base || '_i',
      v_item.table_name
    );
    execute format(
      'create trigger %I after insert on public.%I referencing new table as new_rows for each statement execute function private._invoice_candidate_revision_trigger_v2(%L,%L,%s)',
      v_trigger_base || '_i',
      v_item.table_name,
      v_item.bump_generate::text,
      v_item.bump_issue::text,
      v_argument_list
    );

    execute format(
      'drop trigger if exists %I on public.%I',
      v_trigger_base || '_u',
      v_item.table_name
    );
    execute format(
      'create trigger %I after update on public.%I referencing old table as old_rows new table as new_rows for each statement execute function private._invoice_candidate_revision_trigger_v2(%L,%L,%s)',
      v_trigger_base || '_u',
      v_item.table_name,
      v_item.bump_generate::text,
      v_item.bump_issue::text,
      v_argument_list
    );

    execute format(
      'drop trigger if exists %I on public.%I',
      v_trigger_base || '_d',
      v_item.table_name
    );
    execute format(
      'create trigger %I after delete on public.%I referencing old table as old_rows for each statement execute function private._invoice_candidate_revision_trigger_v2(%L,%L,%s)',
      v_trigger_base || '_d',
      v_item.table_name,
      v_item.bump_generate::text,
      v_item.bump_issue::text,
      v_argument_list
    );
    v_installed := v_installed + 3;
  end loop;

  drop trigger if exists trg_invoice_result_page_revision_v2_i
    on public.invoice_operation_chunks;
  create trigger trg_invoice_result_page_revision_v2_i
  after insert on public.invoice_operation_chunks
  referencing new table as new_rows
  for each statement
  execute function private._invoice_result_page_revision_trigger_v2();

  drop trigger if exists trg_invoice_result_page_revision_v2_u
    on public.invoice_operation_chunks;
  create trigger trg_invoice_result_page_revision_v2_u
  after update on public.invoice_operation_chunks
  referencing old table as old_rows new table as new_rows
  for each statement
  execute function private._invoice_result_page_revision_trigger_v2();

  drop trigger if exists trg_invoice_result_page_revision_v2_d
    on public.invoice_operation_chunks;
  create trigger trg_invoice_result_page_revision_v2_d
  after delete on public.invoice_operation_chunks
  referencing old table as old_rows
  for each statement
  execute function private._invoice_result_page_revision_trigger_v2();

  return jsonb_build_object(
    'contract_version', 'INVOICE_ASYNC_TRIGGER_MANIFEST_V2',
    'candidate_trigger_count', v_installed,
    'result_trigger_count', 3,
    'table_count', v_installed / 3,
    'manifest_digest', private._invoice_batch_hash_v2(v_manifest_rows)
  );
end;
$function$;

-- private._invoice_correction_validate_batch(jsonb,date)
CREATE OR REPLACE FUNCTION private._invoice_correction_validate_batch(p_scopes jsonb, p_evaluation_date date)
 RETURNS TABLE(request_key text, scope_key text, invoice_id uuid, timesheet_id uuid, correction_classification text, correction_id text, correction_ids text[], required_member_ids uuid[], missing_member_ids uuid[], conflicting_invoice_ids uuid[], balanced boolean, valid boolean, blocker_code text, blocker_codes text[], detail_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with recursive scope_input as materialized (
  select e.ordinality::integer input_ordinal,e.value scope_json
  from jsonb_array_elements(case when jsonb_typeof(p_scopes)='array'
    then p_scopes else '[]'::jsonb end) with ordinality e(value,ordinality)
),
scopes as materialized (
  select
    coalesce(nullif(btrim(scope_json->>'request_key'),''),
      'correction:'||input_ordinal::text) request_key,
    coalesce(nullif(btrim(scope_json->>'scope_key'),''),
      nullif(btrim(scope_json->>'request_key'),''),
      'scope:'||input_ordinal::text) external_scope_key,
    coalesce(nullif(btrim(scope_json->>'request_key'),''),
      'correction:'||input_ordinal::text)||E'\\x1f'||
    coalesce(nullif(btrim(scope_json->>'scope_key'),''),
      nullif(btrim(scope_json->>'request_key'),''),
      'scope:'||input_ordinal::text) scope_key,
    case when coalesce(scope_json->>'invoice_id',
      scope_json->>'target_invoice_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then coalesce(scope_json->>'invoice_id',
        scope_json->>'target_invoice_id')::uuid end invoice_id,
    upper(coalesce(nullif(btrim(scope_json->>'validation_purpose'),''),
      'VALIDATE')) validation_purpose,
    case when coalesce(scope_json->>'expected_client_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(scope_json->>'expected_client_id')::uuid end expected_client_id,
    case when coalesce(scope_json->>'expected_contract_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(scope_json->>'expected_contract_id')::uuid end expected_contract_id,
    case when coalesce(scope_json->>'natural_source_week','')
      ~'^\\d{4}-\\d{2}-\\d{2}$'
      then(scope_json->>'natural_source_week')::date end natural_source_week,
    case when coalesce(scope_json->>'target_invoice_week','')
      ~'^\\d{4}-\\d{2}-\\d{2}$'
      then(scope_json->>'target_invoice_week')::date end target_invoice_week,
    nullif(upper(btrim(scope_json->>'expected_invoice_stream')),'')
      expected_invoice_stream,
    case when coalesce(scope_json->>'expected_vat_rate_pct','')
      ~'^[+-]?[0-9]+([.][0-9]+)?$'
      then(scope_json->>'expected_vat_rate_pct')::numeric end
      expected_vat_rate_pct,
    case when jsonb_typeof(scope_json->'planned_members')='array'
      then scope_json->'planned_members' else '[]'::jsonb end planned_members,
    scope_json
  from scope_input
),
planned_lines as materialized (
  select distinct s.scope_key,s.invoice_id,
    case when coalesce(x.value->>'timesheet_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x.value->>'timesheet_id')::uuid end timesheet_id,
    case when coalesce(x.value->>'vat_rate_pct','')
      ~'^[+-]?[0-9]+([.][0-9]+)?$'
      then(x.value->>'vat_rate_pct')::numeric
      else s.expected_vat_rate_pct end vat_rate_pct,
    nullif(btrim(coalesce(x.value->>'segment_id',
      x.value->>'segment_key')),'') segment_id
  from scopes s
  cross join lateral jsonb_array_elements(s.planned_members) x(value)
  where coalesce(x.value->>'timesheet_id','')~*
    '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
),
targets as materialized (
  select distinct s.invoice_id,s.scope_key,l.timesheet_id
  from scopes s
  join public.invoice_lines l on l.invoice_id=s.invoice_id
  where s.invoice_id is not null and l.timesheet_id is not null
  union
  select p.invoice_id,p.scope_key,p.timesheet_id
  from planned_lines p
  where p.timesheet_id is not null
),
seed_units as materialized (
  select distinct t.correction_id,
    coalesce(t.candidate_hint_text->'correction_financials_policy_envelope',
      f.policy_snapshot_json->'correction_financials_policy_envelope',
      f.rate_source_refs_json->'correction_financials_policy_envelope')
      #>>'{operation,operation_id}' correction_operation_id,
    coalesce(t.candidate_hint_text->'correction_financials_policy_envelope',
      f.policy_snapshot_json->'correction_financials_policy_envelope',
      f.rate_source_refs_json->'correction_financials_policy_envelope')
      ->>'correction_chain_id' correction_chain_id
  from targets q
  join public.timesheets t on t.timesheet_id=q.timesheet_id
  left join public.timesheets_financials f
    on f.timesheet_id=t.timesheet_id and f.is_current
),all_current as materialized (
  select t.timesheet_id,t.correction_id,
    upper(btrim(coalesce(t.correction_kind,''))) correction_kind,
    upper(btrim(coalesce(t.adjustment_origin,''))) adjustment_origin,
    t.parent_timesheet_id,t.is_current,t.status::text timesheet_status,
    t.contract_id,t.week_ending_date,
    coalesce(f.client_id,c.client_id) client_id,
    f.id financial_id,f.processing_status::text processing_status,
    f.basis::text basis,f.total_charge_ex_vat,f.is_stale,f.stale_reason,
    f.locked_by_invoice_id,f.invoice_breakdown_json,
    f.policy_snapshot_json,f.rate_source_refs_json,
    coalesce(
      t.candidate_hint_text->'correction_financials_policy_envelope',
      f.policy_snapshot_json->'correction_financials_policy_envelope',
      f.rate_source_refs_json->'correction_financials_policy_envelope') envelope
  from public.timesheets t
  left join public.timesheets_financials f
    on f.timesheet_id=t.timesheet_id and f.is_current
  left join public.contracts c on c.id=t.contract_id
  where exists(select 1 from targets q where q.timesheet_id=t.timesheet_id)
     or(t.correction_id is not null and exists(
       select 1 from seed_units u where u.correction_id=t.correction_id))
     or exists(
       select 1 from seed_units u
       where u.correction_operation_id is not null
         and u.correction_chain_id is not null
         and u.correction_operation_id=coalesce(
           t.candidate_hint_text->'correction_financials_policy_envelope',
           f.policy_snapshot_json->'correction_financials_policy_envelope',
           f.rate_source_refs_json->'correction_financials_policy_envelope')
             #>>'{operation,operation_id}'
         and u.correction_chain_id=coalesce(
           t.candidate_hint_text->'correction_financials_policy_envelope',
           f.policy_snapshot_json->'correction_financials_policy_envelope',
           f.rate_source_refs_json->'correction_financials_policy_envelope')
             ->>'correction_chain_id')
),
classified as materialized (
  select a.*,
    case
      when jsonb_typeof(a.envelope)='object'
      then encode(digest(convert_to(
        (a.envelope-'envelope_fingerprint')::text,'UTF8'),
        'sha256'),'hex')
    end recomputed_fingerprint,
    cs.is_nhsp,cs.autoprocess_hr,cs.no_timesheet_required,
    (a.adjustment_origin in(
        'IMPORT_CORRECTION','IMPORT_CANCELLATION',
        'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
        'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION')
      or a.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT',
        'CANCELLATION_REVERSAL','CANCELLATION_REPLACEMENT'))
      import_declared,
    coalesce(
      a.adjustment_origin in(
        'IMPORT_CORRECTION','IMPORT_CANCELLATION',
        'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
        'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION')
      and a.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT',
        'CANCELLATION_REVERSAL','CANCELLATION_REPLACEMENT')
      and jsonb_typeof(a.envelope)='object'
      and a.envelope->>'policy_schema_version'=
        'IMPORT_CORRECTION_FINANCIALS_POLICY_V2'
      and a.envelope->>'route_family'='IMPORT_AUTHORITATIVE'
      and lower(coalesce(a.envelope#>>'{classification,canonical}','false'))
        in('true','t','1','yes')
      and nullif(a.envelope#>>'{operation,operation_id}','') is not null
      and nullif(a.envelope->>'correction_chain_id','') is not null
      and a.envelope->>'envelope_fingerprint'=
        case
          when jsonb_typeof(a.envelope)='object'
          then encode(digest(convert_to(
            (a.envelope-'envelope_fingerprint')::text,'UTF8'),
            'sha256'),'hex')
        end
      and(
        (a.correction_kind like 'CHANGED_HOURS_%'
          and a.envelope#>>'{operation,correction_action}'='CHANGED_HOURS')
        or
        (a.correction_kind like 'CANCELLATION_%'
          and a.envelope#>>'{operation,correction_action}'='CANCELLATION')),
      false) authoritative
  from all_current a
  left join lateral (
    select s.is_nhsp,s.autoprocess_hr,s.no_timesheet_required
    from public.client_settings s
    where s.client_id=a.client_id
      and(s.effective_from is null
        or s.effective_from<=p_evaluation_date)
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
),
policy_checked as materialized (
  select c.*,
    c.envelope#>>'{operation,operation_id}' correction_operation_id,
    c.envelope->>'correction_chain_id' correction_chain_id,
    c.envelope->>'root_timesheet_id' frozen_root_timesheet_id,
    upper(btrim(coalesce(c.envelope->>'correction_shape','')))
      correction_shape,
    case when coalesce(c.envelope->>'expected_member_count','')~'^[0-9]+$'
      then(c.envelope->>'expected_member_count')::integer end
      expected_member_count,
    c.envelope->'expected_member_roles' expected_member_roles,
    case
      when c.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL')
        then 'REVERSAL'
      when c.correction_kind in(
        'CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
        then 'REPLACEMENT'
      else 'INVALID'
    end correction_role,
    case
      when c.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL')
        then c.envelope->'reversal'
      when c.correction_kind in(
        'CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
        then c.envelope->'replacement'
    end correction_leg
  from classified c
),
policy_validated as materialized (
  select p.*,
    case when jsonb_typeof(p.correction_leg)='object'
      then encode(digest(convert_to(
        (p.correction_leg-'leg_fingerprint')::text,'UTF8'),'sha256'),'hex')
    end recomputed_leg_fingerprint,
    case when jsonb_typeof(p.correction_leg->'tsfin_policy')='object'
      then encode(digest(convert_to(
        ((p.correction_leg->'tsfin_policy')-'tsfin_policy_fingerprint')::text,
        'UTF8'),'sha256'),'hex')
    end recomputed_tsfin_policy_fingerprint,
    case when jsonb_typeof(p.correction_leg->'invoice_policy')='object'
      then encode(digest(convert_to(
        ((p.correction_leg->'invoice_policy')-'invoice_policy_fingerprint')::text,
        'UTF8'),'sha256'),'hex')
    end recomputed_invoice_policy_fingerprint,
    case when upper(coalesce(p.basis,'')) in(
      'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL',
      'HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end
      current_invoice_stream
  from policy_checked p
),
policy_final as materialized (
  select p.*,
    (
      not p.import_declared
      or(
        jsonb_typeof(p.envelope)='object'
        and nullif(p.envelope->>'envelope_fingerprint','') is not null
        and p.envelope->>'envelope_fingerprint'=p.recomputed_fingerprint
        and p.correction_shape in('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
        and p.expected_member_count between 1 and 100
        and jsonb_typeof(p.expected_member_roles)='array'
        and jsonb_array_length(p.expected_member_roles)=p.expected_member_count
        and nullif(p.correction_operation_id,'') is not null
        and nullif(p.correction_chain_id,'') is not null
        and jsonb_typeof(p.correction_leg)='object'
        and p.correction_leg->>'leg_fingerprint'
          =p.recomputed_leg_fingerprint
        and p.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
          =p.recomputed_tsfin_policy_fingerprint
        and p.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
          =p.recomputed_invoice_policy_fingerprint
        and p.correction_leg#>>'{invoice_policy,invoice_stream}'
          =p.envelope->>'invoice_stream'
        and p.current_invoice_stream=p.envelope->>'invoice_stream'
        and coalesce(
          p.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          p.policy_snapshot_json#>>
            '{correction_financials_policy_envelope,envelope_fingerprint}',
          p.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint')
          is not distinct from p.envelope->>'envelope_fingerprint'
        and coalesce(
          p.policy_snapshot_json->>'correction_leg_fingerprint',
          p.rate_source_refs_json->>'correction_leg_fingerprint')
          is not distinct from p.correction_leg->>'leg_fingerprint'
        and coalesce(
          p.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',
          p.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
          is not distinct from
            p.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
        and coalesce(
          p.policy_snapshot_json->>'correction_invoice_policy_fingerprint',
          p.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
          is not distinct from
            p.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
        and p.policy_snapshot_json->'correction_invoice_policy'
          is not distinct from p.correction_leg->'invoice_policy'
        and upper(btrim(coalesce(
          p.policy_snapshot_json->>'correction_invoice_stream','')))
          is not distinct from p.envelope->>'invoice_stream'
      )
    ) frozen_policy_valid
  from policy_validated p
),
target_classes as materialized (
  select t.invoice_id,t.scope_key,t.timesheet_id,c.correction_id,
    c.correction_kind,c.import_declared,c.authoritative,c.envelope,c.client_id,c.contract_id,
    c.week_ending_date,c.basis,c.correction_operation_id,
    c.correction_chain_id
  from targets t
  join policy_final c on c.timesheet_id=t.timesheet_id
),
members as materialized (
  select tc.invoice_id,tc.scope_key,tc.timesheet_id target_timesheet_id,
    tc.correction_id,tc.correction_kind target_kind,
    tc.envelope target_envelope,
    m.timesheet_id,m.correction_kind,m.client_id,m.contract_id,
    m.week_ending_date,m.basis,m.financial_id,m.processing_status,
    m.is_stale,m.stale_reason,m.locked_by_invoice_id,
    m.invoice_breakdown_json,
    m.envelope,m.recomputed_fingerprint,m.is_nhsp,m.autoprocess_hr,
    m.no_timesheet_required,m.import_declared,m.authoritative,
    m.frozen_policy_valid,m.adjustment_origin,m.timesheet_status,
    m.policy_snapshot_json,m.rate_source_refs_json,
    m.recomputed_leg_fingerprint,m.recomputed_tsfin_policy_fingerprint,
    m.recomputed_invoice_policy_fingerprint,m.correction_role,m.correction_shape,m.expected_member_count,
    m.expected_member_roles,m.correction_operation_id,
    m.correction_chain_id,m.frozen_root_timesheet_id,
    m.current_invoice_stream,m.correction_leg
  from target_classes tc
  join policy_final m
    on m.timesheet_id=tc.timesheet_id
      or(
        tc.authoritative
        and m.authoritative
        and m.correction_operation_id=tc.correction_operation_id
        and m.correction_chain_id=tc.correction_chain_id)
      or(
        not tc.authoritative
        and tc.correction_id is not null
        and m.correction_id=tc.correction_id)
),
member_ancestors as (
  select m.scope_key,m.target_timesheet_id,m.timesheet_id member_timesheet_id,
    t.timesheet_id ancestor_id,t.parent_timesheet_id,0 depth,
    array[t.timesheet_id]::uuid[] path,false cycle
  from members m
  join public.timesheets t on t.timesheet_id=m.timesheet_id
  union all
  select a.scope_key,a.target_timesheet_id,a.member_timesheet_id,
    p.timesheet_id,p.parent_timesheet_id,a.depth+1,
    a.path||p.timesheet_id,p.timesheet_id=any(a.path)
  from member_ancestors a
  join public.timesheets p on p.timesheet_id=a.parent_timesheet_id
  where a.parent_timesheet_id is not null
    and not a.cycle and a.depth<32
),
member_roots as materialized (
  select distinct on(
      a.scope_key,a.target_timesheet_id,a.member_timesheet_id)
    a.scope_key,a.target_timesheet_id,a.member_timesheet_id,
    a.ancestor_id root_timesheet_id,a.cycle,
    a.depth=32 and a.parent_timesheet_id is not null depth_exceeded
  from member_ancestors a
  order by a.scope_key,a.target_timesheet_id,a.member_timesheet_id,
    a.depth desc,a.ancestor_id
),
rollup as materialized (
  select m.invoice_id,m.scope_key,m.target_timesheet_id timesheet_id,
    m.correction_id,
    bool_or(m.import_declared) import_declared,
    bool_or(m.authoritative) authoritative,
    array_agg(distinct m.timesheet_id order by m.timesheet_id) required_ids,
    count(*) filter(where m.financial_id is not null
      and upper(coalesce(m.processing_status,''))='READY_FOR_INVOICE') ready_count,
    count(*) filter(where m.financial_id is null) missing_financial_count,
    count(*) filter(where coalesce(m.is_stale,false)) stale_financial_count,
    count(*) filter(where m.financial_id is not null
      and upper(coalesce(m.processing_status,''))<>'READY_FOR_INVOICE')
      not_ready_financial_count,
    count(*) filter(where m.locked_by_invoice_id is not null
      and(m.invoice_id is null
        or m.locked_by_invoice_id<>m.invoice_id)) whole_lock_conflict_count,
    count(*) filter(where exists(
      select 1
      from jsonb_array_elements(case
        when jsonb_typeof(m.invoice_breakdown_json->'segments')='array'
          then m.invoice_breakdown_json->'segments'
        else '[]'::jsonb end) seg(value)
      where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'') is not null
        and(m.invoice_id is null
          or seg.value->>'invoice_locked_invoice_id'<>m.invoice_id::text)
        and(
          not exists(
            select 1 from planned_lines selected
            where selected.scope_key=m.scope_key
              and selected.timesheet_id=m.timesheet_id
              and selected.segment_id is not null)
          or seg.value->>'segment_id' in(
            select selected.segment_id from planned_lines selected
            where selected.scope_key=m.scope_key
              and selected.timesheet_id=m.timesheet_id
              and selected.segment_id is not null)))
    )
      segment_lock_conflict_count,
    count(distinct m.timesheet_id) member_count,
    count(distinct m.client_id) client_count,
    (min(m.client_id::text))::uuid member_client_id,
    count(distinct m.contract_id) contract_count,
    (min(m.contract_id::text))::uuid member_contract_id,
    count(distinct m.week_ending_date) week_count,
    min(m.week_ending_date) member_week,
    count(distinct case when upper(coalesce(m.basis,'')) in(
      'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL',
      'HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)
      stream_count,
    min(case when m.import_declared then m.envelope->>'invoice_stream'
      else m.current_invoice_stream end) expected_invoice_stream,
    count(*) filter(where m.correction_kind like '%_REVERSAL') reversal_count,
    count(*) filter(where m.correction_kind like '%_REPLACEMENT')
      replacement_count,
    bool_or(m.correction_kind like 'CHANGED_HOURS_%') changed_hours,
    bool_or(m.correction_kind like 'CANCELLATION_%') cancellation,
    bool_and(m.frozen_policy_valid) envelope_valid,
    count(*) filter(where m.import_declared
      and jsonb_typeof(m.envelope) is distinct from 'object')
      missing_envelope_count,
    count(*) filter(where m.import_declared
      and coalesce(m.envelope->>'policy_schema_version','')<>
        'IMPORT_CORRECTION_FINANCIALS_POLICY_V2') invalid_schema_count,
    count(*) filter(where m.import_declared
      and coalesce(m.envelope->>'route_family','')<>'IMPORT_AUTHORITATIVE')
      invalid_route_count,
    count(*) filter(where m.import_declared and(
      lower(coalesce(m.envelope#>>'{classification,canonical}','false'))
        not in('true','t','1','yes')
      or lower(coalesce(
        m.envelope#>>'{classification,client_eligible_at_operation}',
        'false')) not in('true','t','1','yes')
      or nullif(btrim(m.envelope#>>'{classification,source_system}'),'')
        is null)) invalid_classification_count,
    count(*) filter(where m.import_declared
      and nullif(m.envelope#>>'{operation,operation_id}','') is null)
      missing_operation_count,
    count(*) filter(where m.import_declared
      and nullif(m.envelope->>'correction_chain_id','') is null)
      missing_chain_count,
    count(*) filter(where m.import_declared and(
      (m.correction_kind like 'CHANGED_HOURS_%'
        and coalesce(m.envelope#>>'{operation,correction_action}','')<>
          'CHANGED_HOURS')
      or(m.correction_kind like 'CANCELLATION_%'
        and coalesce(m.envelope#>>'{operation,correction_action}','')<>
          'CANCELLATION'))) invalid_action_count,
    count(*) filter(where m.import_declared and(
      nullif(m.envelope->>'envelope_fingerprint','') is null
      or m.envelope->>'envelope_fingerprint'
        is distinct from m.recomputed_fingerprint)) envelope_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared
      and jsonb_typeof(m.correction_leg) is distinct from 'object')
      missing_leg_count,
    count(*) filter(where m.import_declared and(
      nullif(m.correction_leg->>'leg_fingerprint','') is null
      or m.correction_leg->>'leg_fingerprint'
        is distinct from m.recomputed_leg_fingerprint))
      leg_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      jsonb_typeof(m.correction_leg->'tsfin_policy') is distinct from 'object'
      or m.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
        is distinct from m.recomputed_tsfin_policy_fingerprint))
      tsfin_policy_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      jsonb_typeof(m.correction_leg->'invoice_policy')
        is distinct from 'object'
      or m.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
        is distinct from m.recomputed_invoice_policy_fingerprint))
      invoice_policy_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      m.correction_leg#>>'{invoice_policy,invoice_stream}'
        is distinct from m.envelope->>'invoice_stream'
      or m.current_invoice_stream
        is distinct from m.envelope->>'invoice_stream'))
      policy_stream_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
      m.policy_snapshot_json#>>
        '{correction_financials_policy_envelope,envelope_fingerprint}',
      m.rate_source_refs_json->>
        'correction_financials_policy_envelope_fingerprint')
      is distinct from m.envelope->>'envelope_fingerprint')
      current_envelope_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_leg_fingerprint',
      m.rate_source_refs_json->>'correction_leg_fingerprint')
      is distinct from m.correction_leg->>'leg_fingerprint')
      current_leg_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',
      m.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
      is distinct from
        m.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}')
      current_tsfin_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_invoice_policy_fingerprint',
      m.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
      is distinct from
        m.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}')
      current_invoice_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      m.policy_snapshot_json->'correction_tsfin_policy'
        is distinct from m.correction_leg->'tsfin_policy'
      or m.policy_snapshot_json->'correction_invoice_policy'
        is distinct from m.correction_leg->'invoice_policy'))
      frozen_subpolicy_drift_count,
    count(distinct m.correction_operation_id)
      filter(where m.import_declared) correction_operation_count,
    count(distinct m.correction_chain_id)
      filter(where m.import_declared) correction_chain_count,
    count(distinct m.frozen_root_timesheet_id)
      filter(where m.import_declared) frozen_root_count,
    count(distinct mr.root_timesheet_id)
      filter(where m.import_declared) actual_root_count,
    bool_or(coalesce(mr.cycle,false)) chain_cycle,
    bool_or(coalesce(mr.depth_exceeded,false)) chain_depth_exceeded,
    bool_and(not m.import_declared
      or m.frozen_root_timesheet_id=mr.root_timesheet_id::text)
      frozen_root_matches,
    count(distinct m.envelope->>'envelope_fingerprint')
      filter(where m.import_declared) envelope_count,
    max(m.expected_member_count)
      filter(where m.import_declared) expected_member_count,
    count(distinct m.expected_member_count)
      filter(where m.import_declared) expected_count_variants,
    count(distinct m.correction_shape)
      filter(where m.import_declared) shape_variants,
    min(m.correction_shape)
      filter(where m.import_declared) correction_shape,
    coalesce(jsonb_agg(m.correction_role order by
      case m.correction_role when 'REVERSAL' then 1
        when 'REPLACEMENT' then 2 else 3 end,m.timesheet_id)
      filter(where m.import_declared),'[]'::jsonb) actual_member_roles,
    (min(m.expected_member_roles::text)
      filter(where m.import_declared))::jsonb expected_member_roles
  from members m
  left join member_roots mr on mr.scope_key=m.scope_key
    and mr.target_timesheet_id=m.target_timesheet_id
    and mr.member_timesheet_id=m.timesheet_id
  group by m.invoice_id,m.scope_key,m.target_timesheet_id,m.correction_id
),
candidate_lines as materialized (
  select distinct r.scope_key,r.invoice_id target_invoice_id,
    l.invoice_id line_invoice_id,l.timesheet_id,l.vat_rate_pct,
    false planned
  from rollup r
  join members m on m.scope_key=r.scope_key
    and m.target_timesheet_id=r.timesheet_id
  join public.invoice_lines l on l.timesheet_id=m.timesheet_id
  union all
  select p.scope_key,p.invoice_id,p.invoice_id,p.timesheet_id,p.vat_rate_pct,
    true
  from planned_lines p
  where not exists(
    select 1 from public.invoice_lines l
    where l.invoice_id=p.invoice_id and l.timesheet_id=p.timesheet_id)
),
line_scope as materialized (
  select r.invoice_id,r.scope_key,r.timesheet_id,
    array_agg(distinct l.timesheet_id order by l.timesheet_id)
      filter(where l.timesheet_id is not null and(
        l.planned or(
          r.invoice_id is not null and l.line_invoice_id=r.invoice_id)))
      present_ids,
    array_agg(distinct l.line_invoice_id order by l.line_invoice_id)
      filter(where not l.planned and(
        r.invoice_id is null or l.line_invoice_id<>r.invoice_id))
      conflicting_ids,
    count(distinct l.timesheet_id) filter(where l.timesheet_id is not null and(
      l.planned or(
        r.invoice_id is not null and l.line_invoice_id=r.invoice_id)))
      present_count,
    count(*) filter(where l.timesheet_id is not null and(
        l.planned or(
          r.invoice_id is not null and l.line_invoice_id=r.invoice_id))
      and coalesce(l.vat_rate_pct,-999999) is distinct from
        case when coalesce(
          m.correction_leg#>>'{invoice_policy,applied_vat_rate_pct}','')
          ~'^[+-]?[0-9]+([.][0-9]+)?$'
          then (m.correction_leg#>>
            '{invoice_policy,applied_vat_rate_pct}')::numeric
          else coalesce(l.vat_rate_pct,-999999) end) vat_mismatch_count
  from rollup r
  join members m on m.scope_key=r.scope_key
    and m.target_timesheet_id=r.timesheet_id
  left join candidate_lines l on l.scope_key=r.scope_key
    and l.timesheet_id=m.timesheet_id
  group by r.invoice_id,r.scope_key,r.timesheet_id
),
final as materialized (
  select r.*,coalesce(ls.present_ids,array[]::uuid[]) present_ids,
    coalesce(ls.conflicting_ids,array[]::uuid[]) conflicting_ids,
    coalesce(ls.present_count,0) present_count,
    coalesce(ls.vat_mismatch_count,0) vat_mismatch_count,
    array(select x from unnest(r.required_ids) x
      where not x=any(coalesce(ls.present_ids,array[]::uuid[]))) missing_ids,
    ti.id is not null target_exists,
    ti.status::text target_status,
    ti.issued_at_utc target_issued_at_utc,
    ti.client_id target_client_id,
    case when lower(coalesce(
      ti.header_snapshot_json#>>'{meta,self_bill}','false'))
      in('true','t','1','yes') then 'SELF_BILL' else 'NORMAL' end
      target_invoice_stream,
    coalesce(pair_scope.scope_json,'{}'::jsonb) pair_scope_json,
    (
      not r.import_declared
      or(
        r.correction_operation_count=1
        and r.correction_chain_count=1
        and r.frozen_root_count=1
        and r.actual_root_count=1
        and r.frozen_root_matches
        and not r.chain_cycle
        and not r.chain_depth_exceeded
        and r.envelope_count=1
        and r.expected_count_variants=1
        and r.shape_variants=1
        and r.expected_member_count=r.member_count
        and r.actual_member_roles=r.expected_member_roles
        and r.reversal_count=1
        and r.replacement_count=case
          when r.correction_shape='REVERSAL_ONLY' then 0 else 1 end
        and r.correction_shape in('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
      )
    ) pair_balanced
  from rollup r
  left join line_scope ls
    on ls.invoice_id is not distinct from r.invoice_id
   and ls.scope_key=r.scope_key
   and ls.timesheet_id=r.timesheet_id
  left join public.invoices ti on ti.id=r.invoice_id
  left join lateral (
    select public.invoice_correction_pair_scope_v1(
      r.timesheet_id,null::uuid,null::uuid,false,100) scope_json
    where r.import_declared
  ) pair_scope on true
),
member_results as materialized (
  select f.*,s.request_key,s.external_scope_key,s.validation_purpose,
    s.expected_client_id,s.expected_contract_id,s.natural_source_week,
    s.target_invoice_week,s.expected_invoice_stream scope_expected_stream,
    s.expected_vat_rate_pct,
    case when f.import_declared then 'IMPORT_AUTHORITATIVE'
      when f.correction_id is not null then 'NON_AUTHORITATIVE_CORRECTION'
      else 'ORDINARY' end correction_classification,
    b.blocker_codes,
    jsonb_build_object(
      'actual_member_count',f.member_count,
      'existing_line_member_count',f.present_count,
      'ready_count',f.ready_count,
      'missing_financial_count',f.missing_financial_count,
      'stale_financial_count',f.stale_financial_count,
      'not_ready_financial_count',f.not_ready_financial_count,
      'whole_lock_conflict_count',f.whole_lock_conflict_count,
      'segment_lock_conflict_count',f.segment_lock_conflict_count,
      'reversal_count',f.reversal_count,
      'replacement_count',f.replacement_count,
      'correction_shape',f.correction_shape,
      'expected_member_count',f.expected_member_count,
      'expected_member_roles',f.expected_member_roles,
      'actual_member_roles',f.actual_member_roles,
      'correction_operation_count',f.correction_operation_count,
      'correction_chain_count',f.correction_chain_count,
      'frozen_root_count',f.frozen_root_count,
      'actual_root_count',f.actual_root_count,
      'frozen_root_matches',f.frozen_root_matches,
      'chain_cycle',f.chain_cycle,
      'chain_depth_exceeded',f.chain_depth_exceeded,
      'expected_invoice_stream',f.expected_invoice_stream,
      'target_invoice_stream',case when f.invoice_id is null
        then null else f.target_invoice_stream end,
      'member_client_id',f.member_client_id,
      'member_contract_id',f.member_contract_id,
      'member_week',f.member_week,
      'target_client_id',f.target_client_id,
      'target_status',f.target_status,
      'vat_mismatch_count',f.vat_mismatch_count,
      'policy_failure_counts',jsonb_build_object(
        'missing_envelope',f.missing_envelope_count,
        'invalid_schema',f.invalid_schema_count,
        'invalid_route',f.invalid_route_count,
        'invalid_classification',f.invalid_classification_count,
        'missing_operation',f.missing_operation_count,
        'missing_chain',f.missing_chain_count,
        'invalid_action',f.invalid_action_count,
        'envelope_fingerprint',f.envelope_fingerprint_mismatch_count,
        'missing_leg',f.missing_leg_count,
        'leg_fingerprint',f.leg_fingerprint_mismatch_count,
        'tsfin_policy_fingerprint',f.tsfin_policy_fingerprint_mismatch_count,
        'invoice_policy_fingerprint',f.invoice_policy_fingerprint_mismatch_count,
        'policy_stream',f.policy_stream_mismatch_count,
        'current_envelope_fingerprint',
          f.current_envelope_fingerprint_mismatch_count,
        'current_leg_fingerprint',f.current_leg_fingerprint_mismatch_count,
        'current_tsfin_fingerprint',f.current_tsfin_fingerprint_mismatch_count,
        'current_invoice_fingerprint',
          f.current_invoice_fingerprint_mismatch_count,
        'frozen_subpolicy_drift',f.frozen_subpolicy_drift_count)) detail_json
  from final f
  join scopes s on s.scope_key=f.scope_key
  cross join lateral (
    select coalesce(array_agg(code order by ordinal)
      filter(where code is not null),array[]::text[]) blocker_codes
    from unnest(array[
      case when p_evaluation_date is null
        then 'EVALUATION_DATE_REQUIRED' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and not f.target_exists
        then 'INVOICE_CORRECTION_TARGET_NOT_FOUND' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and(upper(coalesce(f.target_status,''))<>'DRAFT'
          or f.target_issued_at_utc is not null)
        then 'INVOICE_CORRECTION_TARGET_NOT_APPENDABLE' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and f.target_client_id is distinct from f.member_client_id
        then 'INVOICE_CORRECTION_TARGET_CLIENT_MISMATCH' end,
      case when s.expected_client_id is not null
        and s.expected_client_id is distinct from f.member_client_id
        then 'INVOICE_CORRECTION_CLIENT_MISMATCH' end,
      case when s.expected_contract_id is not null
        and s.expected_contract_id is distinct from f.member_contract_id
        then 'INVOICE_CORRECTION_CONTRACT_MISMATCH' end,
      case when s.natural_source_week is not null
        and s.natural_source_week is distinct from f.member_week
        then 'INVOICE_CORRECTION_WEEK_MISMATCH' end,
      case when s.expected_invoice_stream is not null
        and s.expected_invoice_stream is distinct from f.expected_invoice_stream
        then 'INVOICE_CORRECTION_STREAM_MISMATCH' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and f.target_invoice_stream is distinct from f.expected_invoice_stream
        then 'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH' end,
      case when f.import_declared and f.missing_envelope_count>0
        then 'INVOICE_CORRECTION_ENVELOPE_MISSING' end,
      case when f.import_declared and f.invalid_schema_count>0
        then 'INVOICE_CORRECTION_ENVELOPE_SCHEMA_INVALID' end,
      case when f.import_declared and f.invalid_route_count>0
        then 'INVOICE_CORRECTION_ROUTE_FAMILY_INVALID' end,
      case when f.import_declared and f.invalid_classification_count>0
        then 'INVOICE_CORRECTION_CLASSIFICATION_INVALID' end,
      case when f.import_declared and f.missing_operation_count>0
        then 'INVOICE_CORRECTION_OPERATION_IDENTITY_INVALID' end,
      case when f.import_declared and f.missing_chain_count>0
        then 'INVOICE_CORRECTION_CHAIN_IDENTITY_INVALID' end,
      case when f.import_declared and f.invalid_action_count>0
        then 'INVOICE_CORRECTION_ACTION_INVALID' end,
      case when f.import_declared and f.envelope_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_ENVELOPE_FINGERPRINT_MISMATCH' end,
      case when f.import_declared and f.envelope_count<>1
        then 'INVOICE_CORRECTION_MEMBER_ENVELOPE_MISMATCH' end,
      case when f.import_declared and f.missing_leg_count>0
        then 'INVOICE_CORRECTION_LEG_MISSING' end,
      case when f.import_declared and f.leg_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_LEG_FINGERPRINT_MISMATCH' end,
      case when f.import_declared
        and f.tsfin_policy_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_TSFIN_POLICY_FINGERPRINT_MISMATCH' end,
      case when f.import_declared
        and f.invoice_policy_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_INVOICE_POLICY_FINGERPRINT_MISMATCH' end,
      case when f.import_declared and f.policy_stream_mismatch_count>0
        then 'INVOICE_CORRECTION_STREAM_MISMATCH' end,
      case when f.import_declared
        and(f.current_envelope_fingerprint_mismatch_count>0
          or f.current_leg_fingerprint_mismatch_count>0
          or f.current_tsfin_fingerprint_mismatch_count>0
          or f.current_invoice_fingerprint_mismatch_count>0)
        then 'INVOICE_CORRECTION_CURRENT_POLICY_FINGERPRINT_MISMATCH' end,
      case when f.import_declared and f.frozen_subpolicy_drift_count>0
        then 'INVOICE_CORRECTION_FROZEN_POLICY_DRIFT' end,
      case when f.chain_cycle then 'INVOICE_CORRECTION_CHAIN_CYCLE' end,
      case when f.chain_depth_exceeded
        then 'INVOICE_CORRECTION_CHAIN_DEPTH_EXCEEDED' end,
      case when f.member_count>100
        then 'INVOICE_CORRECTION_MEMBER_LIMIT_EXCEEDED' end,
      case when f.import_declared and not f.frozen_root_matches
        then 'INVOICE_CORRECTION_ENVELOPE_ROOT_MISMATCH' end,
      case when f.missing_financial_count>0
        then 'INVOICE_CORRECTION_TSFIN_MISSING' end,
      case when f.stale_financial_count>0
        then 'INVOICE_CORRECTION_TSFIN_STALE' end,
      case when f.not_ready_financial_count>0
        or f.ready_count<>f.member_count
        then 'INVOICE_CORRECTION_TSFIN_NOT_READY' end,
      case when f.client_count<>1
        then 'INVOICE_CORRECTION_CLIENT_MISMATCH' end,
      case when f.contract_count<>1
        then 'INVOICE_CORRECTION_CONTRACT_MISMATCH' end,
      case when f.week_count<>1
        then 'INVOICE_CORRECTION_WEEK_MISMATCH' end,
      case when f.stream_count<>1
        then 'INVOICE_CORRECTION_STREAM_MISMATCH' end,
      case when f.whole_lock_conflict_count>0
        then 'INVOICE_CORRECTION_SOURCE_LOCK_CONFLICT' end,
      case when f.segment_lock_conflict_count>0
        then 'INVOICE_CORRECTION_SEGMENT_LOCK_CONFLICT' end,
      case when f.pair_scope_json->>'placement_state'='INCOMPLETE_MOVE'
        then 'INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE' end,
      case when cardinality(f.conflicting_ids)>0
        and not (
          coalesce((f.pair_scope_json->>'valid')::boolean,false)
          and f.pair_scope_json->>'placement_state' in ('COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE')
        )
        then 'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES' end,
      case when (cardinality(f.missing_ids)>0
        and not (
          coalesce((f.pair_scope_json->>'valid')::boolean,false)
          and f.pair_scope_json->>'placement_state' in ('COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE')
        ))
        or(f.import_declared
          and coalesce(f.expected_member_count,0)>f.member_count
          and f.pair_scope_json->>'placement_state'<>'INCOMPLETE_MOVE')
        then 'INVOICE_CORRECTION_MEMBER_MISSING' end,
      case when f.vat_mismatch_count>0
        then 'INVOICE_CORRECTION_VAT_POLICY_MISMATCH' end,
      case when f.import_declared and not f.pair_balanced
        then 'INVOICE_CORRECTION_UNIT_INVALID' end
    ]::text[]) with ordinality u(code,ordinal)
  ) b
),
scope_blockers as materialized (
  select s.scope_key,coalesce(array_agg(x.code order by x.first_ordinal)
      filter(where x.code is not null),array[]::text[]) blocker_codes
  from scopes s
  left join lateral (
    select u.code,min(u.ordinal)::integer first_ordinal
    from member_results m
    cross join lateral unnest(m.blocker_codes) with ordinality u(code,ordinal)
    where m.scope_key=s.scope_key
    group by u.code
  ) x on true
  group by s.scope_key
),
scope_summary as materialized (
  select s.request_key,s.external_scope_key scope_key,s.invoice_id,
    case when count(distinct m.timesheet_id)=1 then (min(m.timesheet_id::text))::uuid end
      timesheet_id,
    case when bool_or(coalesce(m.import_declared,false))
      then 'IMPORT_AUTHORITATIVE'
      when bool_or(m.correction_id is not null)
      then 'NON_AUTHORITATIVE_CORRECTION'
      else 'ORDINARY' end correction_classification,
    case when count(distinct m.correction_id)=1 then min(m.correction_id) end
      correction_id,
    coalesce(array_agg(distinct m.correction_id order by m.correction_id)
      filter(where m.correction_id is not null),array[]::text[])
      correction_ids,
    coalesce(array(select distinct x from member_results mr
      cross join lateral unnest(mr.required_ids) q(x)
      where mr.scope_key=s.scope_key order by x),array[]::uuid[])
      required_member_ids,
    coalesce(array(select distinct x from member_results mr
      cross join lateral unnest(mr.missing_ids) q(x)
      where mr.scope_key=s.scope_key order by x),array[]::uuid[])
      missing_member_ids,
    coalesce(array(select distinct x from member_results mr
      cross join lateral unnest(mr.conflicting_ids) q(x)
      where mr.scope_key=s.scope_key order by x),array[]::uuid[])
      conflicting_invoice_ids,
    coalesce(bool_and(m.pair_balanced),true) balanced,
    sb.blocker_codes,
    jsonb_build_object(
      'validation_purpose',s.validation_purpose,
      'evaluation_date',p_evaluation_date,
      'natural_source_week',s.natural_source_week,
      'target_invoice_week',s.target_invoice_week,
      'unit_count',count(distinct(m.correction_id,m.timesheet_id))
        filter(where m.correction_id is not null),
      'units',coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',m.timesheet_id,
        'correction_id',m.correction_id,
        'classification',m.correction_classification,
        'required_member_ids',m.required_ids,
        'missing_member_ids',m.missing_ids,
        'conflicting_invoice_ids',m.conflicting_ids,
        'balanced',m.pair_balanced,
        'blocker_codes',m.blocker_codes,
        'detail',m.detail_json)
        order by m.correction_id nulls last,m.timesheet_id)
        filter(where m.timesheet_id is not null),'[]'::jsonb)) detail_json
  from scopes s
  left join member_results m on m.scope_key=s.scope_key
  join scope_blockers sb on sb.scope_key=s.scope_key
  group by s.request_key,s.external_scope_key,s.scope_key,s.invoice_id,
    s.validation_purpose,s.natural_source_week,s.target_invoice_week,
    sb.blocker_codes
)
select request_key,scope_key,invoice_id,timesheet_id,
  correction_classification,correction_id,correction_ids,
  required_member_ids,missing_member_ids,conflicting_invoice_ids,
  balanced,cardinality(blocker_codes)=0 valid,
  blocker_codes[1] blocker_code,blocker_codes,detail_json
from scope_summary
order by request_key,scope_key;
$function$;

-- private._invoice_current_chunk_ids_v2(uuid[],integer)
CREATE OR REPLACE FUNCTION private._invoice_current_chunk_ids_v2(p_chunk_ids uuid[], p_limit integer DEFAULT 500)
 RETURNS TABLE(requested_chunk_id uuid, current_chunk_id uuid, replacement_chain_status text, replacement_chain_error jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_limit integer := greatest(1, least(coalesce(p_limit, 500), 500));
begin
  if p_chunk_ids is null
     or cardinality(p_chunk_ids) = 0
     or exists (select 1 from unnest(p_chunk_ids) value where value is null) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_CURRENT_CHUNK_IDS_INVALID';
  end if;

  if cardinality(p_chunk_ids) > v_limit then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_CURRENT_CHUNK_ID_LIMIT_EXCEEDED';
  end if;

  return query
  with recursive
  requested as materialized (
    select distinct value requested_id
    from unnest(p_chunk_ids) value
  ),
  chain as (
    select
      r.requested_id,
      c.id chunk_id,
      c.replaced_by_chunk_id,
      array[c.id] path,
      1 depth,
      false cycle
    from requested r
    left join public.invoice_operation_chunks c on c.id = r.requested_id

    union all

    select
      ch.requested_id,
      next_chunk.id,
      next_chunk.replaced_by_chunk_id,
      ch.path || next_chunk.id,
      ch.depth + 1,
      next_chunk.id = any(ch.path)
    from chain ch
    join public.invoice_operation_chunks next_chunk
      on next_chunk.id = ch.replaced_by_chunk_id
    where ch.replaced_by_chunk_id is not null
      and not ch.cycle
      and ch.depth < 64
  ),
  terminal as materialized (
    select distinct on (ch.requested_id)
      ch.requested_id,
      ch.chunk_id,
      ch.replaced_by_chunk_id,
      ch.depth,
      ch.cycle,
      ch.path
    from chain ch
    order by ch.requested_id, ch.depth desc
  )
  select
    r.requested_id,
    case
      when t.chunk_id is not null
       and t.replaced_by_chunk_id is null
       and not t.cycle
       and t.depth < 64
      then t.chunk_id
    end,
    case
      when t.chunk_id is null then 'MISSING'
      when t.cycle then 'CYCLE'
      when t.depth >= 64 and t.replaced_by_chunk_id is not null then 'DEPTH_EXCEEDED'
      when t.replaced_by_chunk_id is not null then 'MISSING_LINK'
      else 'CURRENT'
    end,
    case
      when t.chunk_id is null then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_SOURCE_MISSING',
        'requested_chunk_id', r.requested_id
      )
      when t.cycle then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_CYCLE',
        'requested_chunk_id', r.requested_id,
        'depth', t.depth
      )
      when t.depth >= 64 and t.replaced_by_chunk_id is not null then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_DEPTH_EXCEEDED',
        'requested_chunk_id', r.requested_id,
        'maximum_depth', 64
      )
      when t.replaced_by_chunk_id is not null then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_LINK_MISSING',
        'requested_chunk_id', r.requested_id,
        'missing_chunk_id', t.replaced_by_chunk_id
      )
    end
  from requested r
  left join terminal t on t.requested_id = r.requested_id
  order by r.requested_id;
end;
$function$;

-- private._invoice_current_chunks_batch(uuid[],uuid[],uuid[],integer)
CREATE OR REPLACE FUNCTION private._invoice_current_chunks_batch(p_operation_ids uuid[] DEFAULT NULL::uuid[], p_document_version_ids uuid[] DEFAULT NULL::uuid[], p_asset_ids uuid[] DEFAULT NULL::uuid[], p_limit integer DEFAULT 10000)
 RETURNS TABLE(logical_slot_key text, current_chunk_id uuid, operation_id uuid, chunk_type text, level_no integer, sequence_no integer, work_key text, plan_generation integer, entity_type text, entity_id uuid, document_version_id uuid, document_asset_id uuid, input_document_version_id uuid, current_status text, current_phase text, replacement_chain_status text, replacement_chain_error jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_operation_count integer:=cardinality(coalesce(p_operation_ids,array[]::uuid[]));
  v_document_count integer:=cardinality(coalesce(p_document_version_ids,array[]::uuid[]));
  v_asset_count integer:=cardinality(coalesce(p_asset_ids,array[]::uuid[]));
  v_limit integer:=greatest(1,least(coalesce(p_limit,10000),10000));
  v_row_count integer;
begin
  if v_operation_count+v_document_count+v_asset_count<1 then
    raise exception using errcode='22023',
      message='At least one current-chunk scope is required';
  end if;
  if v_operation_count>500 or v_document_count>500 or v_asset_count>500
     or v_operation_count+v_document_count+v_asset_count>500 then
    raise exception using errcode='22023',
      message='Current-chunk scope is limited to 500 identifiers';
  end if;

  with requested_operations as materialized (
    select distinct x.id
    from unnest(coalesce(p_operation_ids,array[]::uuid[])) x(id)
    where x.id is not null
    union
    select distinct c.operation_id
    from public.invoice_operation_chunks c
    where c.document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.input_document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.document_asset_id=any(coalesce(p_asset_ids,array[]::uuid[]))
  )
  select count(*)::integer into v_row_count
  from (
    select 1
    from public.invoice_operation_chunks c
    join requested_operations r on r.id=c.operation_id
    limit v_limit+1
  ) bounded;

  if v_row_count>v_limit then
    raise exception using errcode='54000',
      message='CURRENT_CHUNK_SCOPE_TOO_LARGE';
  end if;

  return query
  with recursive requested_operations as materialized (
    select distinct x.id
    from unnest(coalesce(p_operation_ids,array[]::uuid[])) x(id)
    where x.id is not null
    union
    select distinct c.operation_id
    from public.invoice_operation_chunks c
    where c.document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.input_document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.document_asset_id=any(coalesce(p_asset_ids,array[]::uuid[]))
  ),
  raw as materialized (
    select c.*,
      encode(digest(concat_ws('|',
        c.operation_id::text,c.chunk_type,c.level_no::text,c.sequence_no::text,
        coalesce(c.entity_type,'~'),coalesce(c.entity_id::text,'~'),
        coalesce(c.document_version_id::text,'~'),
        coalesce(c.document_asset_id::text,'~'),
        coalesce(c.input_document_version_id::text,'~')),'sha256'),'hex')
        slot_key
    from public.invoice_operation_chunks c
    join requested_operations r on r.id=c.operation_id
  ),
  incoming as materialized (
    select r.replaced_by_chunk_id,count(*)::integer predecessor_count
    from raw r
    where r.replaced_by_chunk_id is not null
    group by r.replaced_by_chunk_id
  ),
  edge_validation as materialized (
    select old.id old_chunk_id,old.slot_key,
      case
        when old.replacement_required and old.replaced_by_chunk_id is null
          then 'REPLACEMENT_REQUIRED_MISSING'
        when old.replaced_by_chunk_id is null then null
        when old.status<>'SUPERSEDED' then 'REPLACEMENT_SOURCE_NOT_SUPERSEDED'
        when not old.replacement_required then 'REPLACEMENT_LINK_NOT_REQUIRED'
        when replacement.id is null then 'REPLACEMENT_MISSING'
        when replacement.operation_id<>old.operation_id
          then 'REPLACEMENT_CROSS_OPERATION'
        when replacement.slot_key<>old.slot_key then 'REPLACEMENT_CROSS_SLOT'
        when replacement.plan_generation<=old.plan_generation
          then 'REPLACEMENT_GENERATION_NOT_HIGHER'
        when coalesce(i.predecessor_count,0)>1
          then 'REPLACEMENT_MULTIPLE_PREDECESSORS'
        else null
      end error_code
    from raw old
    left join raw replacement on replacement.id=old.replaced_by_chunk_id
    left join incoming i on i.replaced_by_chunk_id=old.replaced_by_chunk_id
  ),
  walk(origin_chunk_id,current_chunk_id,slot_key,path,depth,cycle) as (
    select r.id,r.id,r.slot_key,array[r.id]::uuid[],0,false
    from raw r
    union all
    select w.origin_chunk_id,next_chunk.id,w.slot_key,
      w.path||next_chunk.id,w.depth+1,next_chunk.id=any(w.path)
    from walk w
    join raw current_chunk on current_chunk.id=w.current_chunk_id
    join raw next_chunk on next_chunk.id=current_chunk.replaced_by_chunk_id
    where not w.cycle and w.depth<64
  ),
  walk_validation as materialized (
    select w.slot_key,
      bool_or(w.cycle) has_cycle,
      bool_or(w.depth=64 and current_chunk.replaced_by_chunk_id is not null)
        too_deep
    from walk w
    join raw current_chunk on current_chunk.id=w.current_chunk_id
    group by w.slot_key
  ),
  leaves as materialized (
    select r.slot_key,count(*)::integer leaf_count,
      (array_agg(r.id order by r.id))[1] leaf_id
    from raw r
    where r.replaced_by_chunk_id is null
    group by r.slot_key
  ),
  slot_errors as materialized (
    select r.slot_key,
      coalesce(l.leaf_count,0) leaf_count,
      l.leaf_id,
      coalesce(w.has_cycle,false) has_cycle,
      coalesce(w.too_deep,false) too_deep,
      array_remove(array_agg(distinct e.error_code order by e.error_code),null)
        edge_errors
    from (select distinct slot_key from raw) r
    left join leaves l on l.slot_key=r.slot_key
    left join walk_validation w on w.slot_key=r.slot_key
    left join edge_validation e on e.slot_key=r.slot_key
    group by r.slot_key,l.leaf_count,l.leaf_id,w.has_cycle,w.too_deep
  ),
  classified as materialized (
    select s.*,
      case
        when s.has_cycle then 'REPLACEMENT_CYCLE'
        when s.too_deep then 'REPLACEMENT_CHAIN_TOO_DEEP'
        when cardinality(s.edge_errors)>0 then s.edge_errors[1]
        when s.leaf_count=0 then 'REPLACEMENT_CURRENT_LEAF_MISSING'
        when s.leaf_count>1 then 'REPLACEMENT_MULTIPLE_CURRENT_LEAVES'
        else null
      end primary_error
    from slot_errors s
  )
  select c.slot_key,
    case when c.leaf_count=1 then leaf.id end,
    coalesce(leaf.operation_id,slot.operation_id),
    coalesce(leaf.chunk_type,slot.chunk_type),
    coalesce(leaf.level_no,slot.level_no),
    coalesce(leaf.sequence_no,slot.sequence_no),
    leaf.work_key,leaf.plan_generation,
    coalesce(leaf.entity_type,slot.entity_type),
    coalesce(leaf.entity_id,slot.entity_id),
    coalesce(leaf.document_version_id,slot.document_version_id),
    coalesce(leaf.document_asset_id,slot.document_asset_id),
    coalesce(leaf.input_document_version_id,slot.input_document_version_id),
    leaf.status,leaf.phase,
    case when c.primary_error is null then 'VALID' else 'INVALID' end,
    case when c.primary_error is null then null
      else jsonb_build_object(
        'code',c.primary_error,
        'logical_slot_key',c.slot_key,
        'leaf_count',c.leaf_count,
        'edge_errors',to_jsonb(c.edge_errors))
    end
  from classified c
  join lateral (
    select r.* from raw r where r.slot_key=c.slot_key order by r.id limit 1
  ) slot on true
  left join raw leaf on leaf.id=c.leaf_id
  order by coalesce(leaf.operation_id,slot.operation_id),
    coalesce(leaf.chunk_type,slot.chunk_type),
    coalesce(leaf.level_no,slot.level_no),
    coalesce(leaf.sequence_no,slot.sequence_no),c.slot_key;
end;
$function$;

-- private._invoice_delivery_advance_batch(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_delivery_advance_batch(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb;
begin
  /*
   * The legal issue snapshot remains frozen. Delivery re-resolves the original
   * routing request once, set-wise, and sends only when the stable V4 policy
   * identity still matches the frozen issue-time route.
   */
  with recursive
  ids as materialized (
    select distinct (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(coalesce(p_claims,'[]'::jsonb)) x
    where x->>'phase' in('PREPARE','QUEUE_DELIVERY')
      and coalesce(x->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  ),
  requested as materialized (
    select c.id chunk_id,c.operation_id,c.entity_id invoice_id,
      c.payload_json,o.actor_user_id,o.config_json->'processor_policy'
        processor_policy,
      nullif(btrim(c.payload_json->>'request_key'),'') request_key,
      i.id found_invoice_id,i.client_id,i.invoice_no,i.status invoice_status,
      i.issued_document_version_id,
      case when coalesce(
          c.payload_json->>'issued_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then(c.payload_json->>'issued_document_version_id')::uuid
        else c.document_version_id end requested_document_version_id,
      case when jsonb_typeof(
          c.payload_json->'frozen_delivery_route')='object'
        then c.payload_json->'frozen_delivery_route'
        else '{}'::jsonb end frozen_route,
      case when jsonb_typeof(c.payload_json->'routing_request')='object'
        then c.payload_json->'routing_request'
        else '{}'::jsonb end routing_request,
      nullif(btrim(c.payload_json->>'delivery_request_token'),'')
        delivery_request_token,
      v.id document_version_id,v.entity_type document_entity_type,
      v.entity_id document_entity_id,v.purpose document_purpose,
      v.status document_status,v.r2_key,v.sha256,v.size_bytes,v.page_count
    from ids x
    join public.invoice_operation_chunks c on c.id=x.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    left join public.invoices i on i.id=c.entity_id
    left join public.invoice_document_versions v
      on v.id=case when coalesce(
          c.payload_json->>'issued_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then(c.payload_json->>'issued_document_version_id')::uuid
        else c.document_version_id end
  ),
  current_route_input as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key',r.chunk_id::text,
      'invoice_id',r.invoice_id,
      'recipient_set',case when jsonb_typeof(
          r.routing_request->'recipient_set')='array'
        then r.routing_request->'recipient_set' else '[]'::jsonb end,
      'cc',case when jsonb_typeof(r.routing_request->'cc')='array'
        then r.routing_request->'cc' else '[]'::jsonb end,
      'bcc',case when jsonb_typeof(r.routing_request->'bcc')='array'
        then r.routing_request->'bcc' else '[]'::jsonb end,
      'delivery_policy',coalesce(
        nullif(r.routing_request->>'delivery_policy',''),
        nullif(r.frozen_route->>'delivery_policy',''),'ATTACH'),
      'template_version',coalesce(
        nullif(r.routing_request->>'template_version',''),
        nullif(r.frozen_route->>'template_version',''),
        'invoice-delivery-v1'))
      order by r.chunk_id),'[]'::jsonb) requests
    from requested r
  ),
  current_routes as materialized (
    select route.*
    from current_route_input input
    cross join lateral private._invoice_delivery_routes_batch(
      input.requests,(v_now at time zone 'Europe/London')::date) route
  ),
  frozen as materialized (
    select r.*,
      route.canonical_to,route.canonical_cc,route.canonical_bcc,
      route.recipient_set_hash,route.route_policy_hash,
      route.route_source,
      coalesce(nullif(r.routing_request->>'template_version',''),
        nullif(r.frozen_route->>'template_version',''))
        template_version,
      upper(coalesce(nullif(r.routing_request->>'delivery_policy',''),
        r.frozen_route->>'delivery_policy',''))
        requested_delivery_policy,
      route.invoice_group_identity,
      route.suppression_reason,route.do_not_send,route.self_bill,
      route.delivery_suppressed,
      coalesce(route.warning_codes,'[]'::jsonb) warnings,
      coalesce(route.blocker_codes,'[]'::jsonb) route_blockers,
      nullif(r.frozen_route->>'route_policy_hash','')
        frozen_route_policy_hash,
      nullif(r.frozen_route->>'recipient_set_hash','')
        frozen_recipient_set_hash,
      route.route_policy_hash is distinct from
        nullif(r.frozen_route->>'route_policy_hash','') route_changed,
      case when coalesce(r.processor_policy#>>
          '{delivery,max_attachments_per_message}','')~'^[1-9][0-9]{0,3}$'
        then(r.processor_policy#>>
          '{delivery,max_attachments_per_message}')::integer end
        max_attachments_per_message,
      case when coalesce(r.processor_policy#>>
          '{delivery,max_cumulative_attachment_bytes}','')
          ~'^[1-9][0-9]{0,17}$'
        then(r.processor_policy#>>
          '{delivery,max_cumulative_attachment_bytes}')::bigint end
        max_cumulative_attachment_bytes,
      case when coalesce(r.processor_policy#>>
          '{delivery,max_individual_attachment_bytes}','')
          ~'^[1-9][0-9]{0,17}$'
        then(r.processor_policy#>>
          '{delivery,max_individual_attachment_bytes}')::bigint end
        max_individual_attachment_bytes,
      case when coalesce(r.processor_policy#>>
          '{delivery,secure_link_threshold_bytes}','')
          ~'^[1-9][0-9]{0,17}$'
        then(r.processor_policy#>>
          '{delivery,secure_link_threshold_bytes}')::bigint end
        secure_link_threshold_bytes
    from requested r
    left join current_routes route
      on route.request_key=r.chunk_id::text
     and route.invoice_id=r.invoice_id
  ),
  classified as materialized (
    select f.*,
      case
        when f.processor_policy->>'version'<>'INVOICE_PROCESSOR_LIMITS_V4'
          or f.max_attachments_per_message is null
          or f.max_cumulative_attachment_bytes is null
          or f.max_individual_attachment_bytes is null
          or f.secure_link_threshold_bytes is null
          or jsonb_typeof(f.processor_policy#>
            '{delivery,allowed_policies}')<>'array'
          then 'PROCESSOR_POLICY_INVALID'
        when f.request_key is distinct from f.chunk_id::text
          then 'REQUEST_CORRELATION_INVALID'
        when f.found_invoice_id is null then 'INVOICE_NOT_FOUND'
        when f.invoice_status not in('ISSUED','PAID')
          then 'INVOICE_NOT_ISSUED'
        when f.requested_document_version_id is null
          then 'ISSUED_DOCUMENT_MISSING'
        when f.issued_document_version_id is distinct from
          f.requested_document_version_id
          then 'ISSUED_DOCUMENT_POINTER_MISMATCH'
        when f.document_version_id is null then 'ISSUED_DOCUMENT_NOT_FOUND'
        when f.document_entity_type<>'INVOICE'
          or f.document_entity_id<>f.invoice_id
          then 'ISSUED_DOCUMENT_ENTITY_MISMATCH'
        when f.document_purpose<>'FINAL_ISSUE'
          then 'ISSUED_DOCUMENT_PURPOSE_MISMATCH'
        when f.document_status<>'READY' or f.r2_key is null
          or f.sha256!~'^[0-9a-f]{64}$'
          or coalesce(f.size_bytes,0)<=0 or coalesce(f.page_count,0)<=0
          then 'ISSUED_DOCUMENT_NOT_READY'
        when f.delivery_request_token is null
          then 'DELIVERY_REQUEST_TOKEN_MISSING'
        when nullif(f.frozen_route->>'route_policy_hash','')
            !~'^[0-9a-f]{64}$'
          or nullif(f.frozen_route->>'recipient_set_hash','')
            !~'^[0-9a-f]{64}$'
          or nullif(f.frozen_route->>'template_version','') is null
          then 'FROZEN_DELIVERY_ROUTE_MISSING'
        when f.route_policy_hash is null
          then 'CURRENT_DELIVERY_ROUTE_MISSING'
        when jsonb_typeof(f.canonical_to)<>'array'
          or jsonb_typeof(f.canonical_cc)<>'array'
          or jsonb_typeof(f.canonical_bcc)<>'array'
          or f.recipient_set_hash!~'^[0-9a-f]{64}$'
          or f.route_policy_hash!~'^[0-9a-f]{64}$'
          or f.template_version is null
          or f.invoice_group_identity is null
          then 'CURRENT_DELIVERY_ROUTE_INVALID'
        when f.requested_delivery_policy not in(
          'ATTACH','SPLIT','SECURE_LINK')
          or not(f.processor_policy#>'{delivery,allowed_policies}'
            @> jsonb_build_array(f.requested_delivery_policy))
          then 'PROCESSOR_POLICY_INVALID'
        when jsonb_typeof(f.route_blockers)<>'array'
          then 'FROZEN_DELIVERY_ROUTE_INVALID'
        when jsonb_array_length(f.route_blockers)>0
          then coalesce(f.route_blockers->>0,'DELIVERY_ROUTE_BLOCKED')
        when f.route_changed then 'DELIVERY_ROUTE_CHANGED'
        when not coalesce(f.delivery_suppressed,false)
          and jsonb_array_length(f.canonical_to)=0
          then 'RECIPIENT_MISSING'
      end blocker_code
    from frozen f
  ),
  deliverable as materialized (
    select c.*,
      case
        when c.requested_delivery_policy='SECURE_LINK'
          or c.size_bytes>c.max_individual_attachment_bytes
          or c.size_bytes>c.max_cumulative_attachment_bytes
          or c.size_bytes>=c.secure_link_threshold_bytes
          then 'SECURE_LINK' else 'ATTACHMENT'
      end descriptor_mode,
      (select string_agg(v.value,',' order by v.value)
        from jsonb_array_elements_text(c.canonical_to) v(value)) recipient_key,
      (select string_agg(v.value,',' order by v.value)
        from jsonb_array_elements_text(c.canonical_cc) v(value)) cc_key,
      (select string_agg(v.value,',' order by v.value)
        from jsonb_array_elements_text(c.canonical_bcc) v(value)) bcc_key
    from classified c
    where c.blocker_code is null and not c.delivery_suppressed
  ),
  compatibility as materialized (
    select d.*,
      encode(digest(jsonb_build_object(
        'delivery_root',d.operation_id,
        'client_id',d.client_id,
        'invoice_group_identity',d.invoice_group_identity,
        'to',d.canonical_to,'cc',d.canonical_cc,'bcc',d.canonical_bcc,
        'route_policy_hash',d.route_policy_hash,
        'template_version',d.template_version,
        'delivery_mode',d.descriptor_mode)::text,'sha256'),'hex')
        compatibility_key
    from deliverable d
  ),
  member_identity as materialized (
    select c.*,
      encode(digest(string_agg(concat_ws('|',
        c.invoice_id::text,c.document_version_id::text,
        c.delivery_request_token),'||')
        over(partition by c.compatibility_key
          order by c.invoice_no nulls last,c.invoice_id
          rows between unbounded preceding and unbounded following),
        'sha256'),'hex')
        ordered_member_token_hash,
      row_number() over(partition by c.compatibility_key
        order by c.invoice_no nulls last,c.invoice_id)::integer input_no
    from compatibility c
  ),
  delivery_pack(
    compatibility_key,input_no,part_no,part_count,direct_bytes
  ) as (
    select n.compatibility_key,n.input_no,1,1,
      case when n.descriptor_mode='ATTACHMENT'
        then n.size_bytes else 0 end
    from member_identity n where n.input_no=1
    union all
    select n.compatibility_key,n.input_no,
      p.part_no+case when split.start_new then 1 else 0 end,
      case when split.start_new then 1 else p.part_count+1 end,
      case when split.start_new
        then case when n.descriptor_mode='ATTACHMENT'
          then n.size_bytes else 0 end
        else p.direct_bytes+case when n.descriptor_mode='ATTACHMENT'
          then n.size_bytes else 0 end end
    from delivery_pack p
    join member_identity n
      on n.compatibility_key=p.compatibility_key
     and n.input_no=p.input_no+1
    cross join lateral(
      select p.part_count+1>n.max_attachments_per_message
        or(n.descriptor_mode='ATTACHMENT'
          and p.direct_bytes+n.size_bytes>
            n.max_cumulative_attachment_bytes) start_new
    ) split
  ),
  assigned as materialized (
    select n.*,p.part_no
    from member_identity n
    join delivery_pack p
      on p.compatibility_key=n.compatibility_key
     and p.input_no=n.input_no
  ),
  part_totals as materialized (
    select compatibility_key,max(part_no)::integer part_total
    from assigned group by compatibility_key
  ),
  message_groups as materialized (
    select a.operation_id,a.client_id,a.invoice_group_identity,
      a.recipient_key,a.cc_key,a.bcc_key,a.recipient_set_hash,
      a.route_policy_hash,a.template_version,a.descriptor_mode,
      a.ordered_member_token_hash,a.part_no,max(pt.part_total) part_total,
      min(a.actor_user_id::text)::uuid actor_user_id,
      min(a.invoice_id::text)::uuid first_invoice_id,
      array_agg(a.chunk_id order by a.input_no) chunk_ids,
      array_agg(a.invoice_id order by a.input_no) invoice_ids,
      array_agg(a.document_version_id order by a.input_no)
        document_version_ids,
      jsonb_agg(
        case when a.descriptor_mode='SECURE_LINK' then
          jsonb_build_object(
            'invoice_id',a.invoice_id,
            'document_version_id',a.document_version_id,
            'filename','Invoice_'||coalesce(nullif(regexp_replace(
              a.invoice_no,'[^A-Za-z0-9_-]+','','g'),''),
              a.invoice_id::text)||'.pdf',
            'sha256',a.sha256,'size_bytes',a.size_bytes,
            'page_count',a.page_count,'delivery_mode','SECURE_LINK',
            'secure_link_required',true,
            'recipient_set_hash',a.recipient_set_hash,
            'delivery_template',a.template_version,
            'delivery_policy',a.requested_delivery_policy,
            'route_policy_hash',a.route_policy_hash,
            'max_attachment_bytes',a.max_individual_attachment_bytes)
        else jsonb_build_object(
            'invoice_id',a.invoice_id,
            'document_version_id',a.document_version_id,
            'filename','Invoice_'||coalesce(nullif(regexp_replace(
              a.invoice_no,'[^A-Za-z0-9_-]+','','g'),''),
              a.invoice_id::text)||'.pdf',
            'r2_key',a.r2_key,'sha256',a.sha256,
            'mime_type','application/pdf','size_bytes',a.size_bytes,
            'page_count',a.page_count,'delivery_mode','ATTACHMENT',
            'recipient_set_hash',a.recipient_set_hash,
            'delivery_template',a.template_version,
            'delivery_policy',a.requested_delivery_policy,
            'route_policy_hash',a.route_policy_hash,
            'max_attachment_bytes',a.max_individual_attachment_bytes)
        end order by a.input_no) attachments,
      sum(case when a.descriptor_mode='ATTACHMENT'
        then a.size_bytes else 0 end)::bigint attachment_total_bytes
    from assigned a
    join part_totals pt using(compatibility_key)
    group by a.operation_id,a.client_id,a.invoice_group_identity,
      a.recipient_key,a.cc_key,a.bcc_key,a.recipient_set_hash,
      a.route_policy_hash,a.template_version,a.descriptor_mode,
      a.ordered_member_token_hash,a.part_no
  ),
  planned_mail as materialized (
    select g.*,
      'INVOICE_DELIVERY_V1:'||encode(digest(concat_ws('|',
        g.operation_id::text,g.ordered_member_token_hash,
        g.route_policy_hash,g.template_version,g.descriptor_mode,
        g.part_no::text),'sha256'),'hex') reference_key,
      encode(digest(concat_ws('|','INVOICE_DELIVERY',
        g.operation_id::text,g.ordered_member_token_hash,
        g.route_policy_hash,g.template_version,g.descriptor_mode,
        g.part_no::text),'sha256'),'hex') deterministic_key
    from message_groups g
  ),
  inserted_mail as (
    insert into public.mail_outbox(
      id,type,"to",cc,bcc,importance,email_type,subject,body_html,
      body_text,attachments,status,created_at_utc,created_by,reference,
      recipient_kind,recipient_id,context_kind,context_id,
      attachments_ready,waiting_invoice_operation_id,
      attachment_total_bytes,attachment_delivery_policy,
      deterministic_outbox_key
    )
    select gen_random_uuid(),'INVOICE',p.recipient_key,p.cc_key,p.bcc_key,
      'Normal','plain',
      'Invoices – Week ending '||
        case when p.invoice_group_identity='NO_WEEK'
          then '' else p.invoice_group_identity end,
      '<p>Please find the attached invoices.</p>',
      'Please find the attached invoices.',
      p.attachments,'QUEUED',v_now,p.actor_user_id,p.reference_key,
      'client',p.client_id,'invoices',
      case when cardinality(p.invoice_ids)=1
        then p.first_invoice_id end,
      true,null,p.attachment_total_bytes,
      case when p.descriptor_mode='SECURE_LINK' then 'SECURE_LINK'
        when p.part_total>1 then 'SPLIT' else 'ATTACH' end,
      p.deterministic_key
    from planned_mail p
    on conflict(reference)
      where reference like 'INVOICE_DELIVERY_V1:%' do nothing
    returning id,reference
  ),
  selected_mail as materialized (
    select m.id,m.reference,p.chunk_ids,p.invoice_ids,
      p.document_version_ids,p.ordered_member_token_hash
    from planned_mail p
    join inserted_mail m on m.reference=p.reference_key
    union all
    select m.id,m.reference,p.chunk_ids,p.invoice_ids,
      p.document_version_ids,p.ordered_member_token_hash
    from planned_mail p
    join public.mail_outbox m on m.reference=p.reference_key
    where not exists(select 1 from inserted_mail im
      where im.reference=p.reference_key)
  ),
  invalid_updates as (
    update public.invoice_operation_chunks c
    set status=case when k.blocker_code in(
          'INVOICE_NOT_FOUND','INVOICE_NOT_ISSUED',
          'ISSUED_DOCUMENT_MISSING','ISSUED_DOCUMENT_POINTER_MISMATCH',
          'ISSUED_DOCUMENT_NOT_FOUND','ISSUED_DOCUMENT_ENTITY_MISMATCH',
          'ISSUED_DOCUMENT_PURPOSE_MISMATCH','ISSUED_DOCUMENT_NOT_READY')
        then 'FAILED' else 'BLOCKED' end,
      phase=case when k.blocker_code in(
          'INVOICE_NOT_FOUND','INVOICE_NOT_ISSUED',
          'ISSUED_DOCUMENT_MISSING','ISSUED_DOCUMENT_POINTER_MISMATCH',
          'ISSUED_DOCUMENT_NOT_FOUND','ISSUED_DOCUMENT_ENTITY_MISMATCH',
          'ISSUED_DOCUMENT_PURPOSE_MISMATCH','ISSUED_DOCUMENT_NOT_READY')
        then 'FAILED' else 'BLOCKED' end,
      error_json=jsonb_build_object('code',k.blocker_code,
        'invoice_id',k.invoice_id,
        'document_version_id',k.requested_document_version_id,
        'frozen_route',jsonb_build_object(
          'route_policy_hash',k.frozen_route_policy_hash,
          'recipient_set_hash',k.frozen_recipient_set_hash,
          'to',k.frozen_route->'to','cc',k.frozen_route->'cc',
          'bcc',k.frozen_route->'bcc'),
        'current_route',jsonb_build_object(
          'route_policy_hash',k.route_policy_hash,
          'recipient_set_hash',k.recipient_set_hash,
          'to',k.canonical_to,'cc',k.canonical_cc,'bcc',k.canonical_bcc,
          'delivery_suppressed',k.delivery_suppressed,
          'suppression_reason',k.suppression_reason),
        'warnings',k.warnings),
      failed_at_utc=case when k.blocker_code in(
          'INVOICE_NOT_FOUND','INVOICE_NOT_ISSUED',
          'ISSUED_DOCUMENT_MISSING','ISSUED_DOCUMENT_POINTER_MISMATCH',
          'ISSUED_DOCUMENT_NOT_FOUND','ISSUED_DOCUMENT_ENTITY_MISMATCH',
          'ISSUED_DOCUMENT_PURPOSE_MISMATCH','ISSUED_DOCUMENT_NOT_READY')
        then v_now end,
      completed_at_utc=null,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from classified k
    where c.id=k.chunk_id and k.blocker_code is not null
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  completed_updates as (
    update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,
      failed_at_utc=null,error_json=null,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=jsonb_build_object(
        'invoice_id',k.invoice_id,
        'document_version_id',k.document_version_id,
        'delivery_request_token',k.delivery_request_token,
        'route_policy_hash',k.route_policy_hash,
        'recipient_set_hash',k.recipient_set_hash,
        'mail_outbox_ids',coalesce((
          select jsonb_agg(distinct sm.id order by sm.id)
          from selected_mail sm where k.chunk_id=any(sm.chunk_ids)
        ),'[]'::jsonb),
        'delivery_skipped',k.delivery_suppressed,
        'skip_reason',coalesce(k.suppression_reason,
          case when k.do_not_send then 'DO_NOT_SEND' end),
        'warnings',k.warnings)
    from classified k
    where c.id=k.chunk_id and k.blocker_code is null
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  results as (
    select * from invalid_updates
    union all
    select * from completed_updates
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'result',result_json,'error',error_json) order by id),'[]'::jsonb)
  into v_result from results;

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

-- private._invoice_delivery_routes_batch(jsonb,date)
CREATE OR REPLACE FUNCTION private._invoice_delivery_routes_batch(p_requests jsonb, p_evaluation_date date)
 RETURNS TABLE(request_key text, invoice_id uuid, canonical_to jsonb, canonical_cc jsonb, canonical_bcc jsonb, invalid_to_count integer, invalid_cc_count integer, invalid_bcc_count integer, recipient_set_hash text, route_policy_hash text, route_source text, client_settings_id uuid, contract_settings_ids jsonb, effective_date date, client_id uuid, invoice_group_identity text, self_bill boolean, do_not_send boolean, delivery_suppressed boolean, suppression_reason text, warning_codes jsonb, warning_details jsonb, blocker_codes jsonb, blocker_details jsonb, grouping_identity text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with raw as materialized (
  select x.ordinality::integer request_no,x.value request_json,
    nullif(btrim(coalesce(x.value->>'request_key','')),'') request_key,
    case when coalesce(x.value->>'invoice_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x.value->>'invoice_id')::uuid end invoice_id,
    case when jsonb_typeof(x.value->'recipient_set')='array'
      then x.value->'recipient_set' else '[]'::jsonb end requested_to,
    case when jsonb_typeof(x.value->'cc')='array'
      then x.value->'cc' else '[]'::jsonb end requested_cc,
    case when jsonb_typeof(x.value->'bcc')='array'
      then x.value->'bcc' else '[]'::jsonb end requested_bcc,
    upper(coalesce(nullif(btrim(x.value->>'delivery_policy'),''),
      'ATTACH')) delivery_policy,
    coalesce(nullif(btrim(x.value->>'template_version'),''),
      'invoice-delivery-v1') template_version
  from jsonb_array_elements(case when jsonb_typeof(p_requests)='array'
    then p_requests else '[]'::jsonb end)
    with ordinality x(value,ordinality)
  where jsonb_typeof(x.value)='object'
),
request_counts as materialized (
  select r.request_key,count(*)::integer request_key_count
  from raw r
  where r.request_key is not null
  group by r.request_key
),
facts as materialized (
  select r.*,coalesce(rc.request_key_count,0) request_key_count,
    i.client_id,
    coalesce(nullif(i.header_snapshot_json#>>'{meta,invoice_week_start}',''),
      nullif(i.header_snapshot_json->>'invoice_week_start',''),'NO_WEEK')
      invoice_week_identity,
    coalesce(i.do_not_send,false) invoice_do_not_send,
    lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
      i.header_snapshot_json->>'self_bill','false'))
      in('true','t','1','yes') invoice_self_bill,
    case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
      then upper(coalesce(nullif(btrim(i.header_snapshot_json->>'invoice_stream'),''),
        case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
          i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
          then 'SELF_BILL' else 'NORMAL' end))
      else case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
          i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
          then 'SELF_BILL' else 'NORMAL' end
    end invoice_stream,
    nullif(btrim(coalesce(i.header_snapshot_json->>
      'client_primary_invoice_email',cl.primary_invoice_email,'')),'')
      primary_email,
    cs.id client_settings_id,cs.effective_from client_settings_effective_from,
    cs.send_manual_invoices_to_different_email client_alt_enabled,
    nullif(btrim(cs.manual_invoices_alt_email_address),'') client_alt_email,
    lower(nullif(btrim(cs.candidate_expense_invoice_email),''))
      client_expense_invoice_email,
    cs.self_bill_no_invoices_sent
  from raw r
  left join request_counts rc on rc.request_key=r.request_key
  left join public.invoices i on i.id=r.invoice_id
  left join public.clients cl on cl.id=i.client_id
  left join lateral (
    select s.id,s.effective_from,s.send_manual_invoices_to_different_email,
      s.manual_invoices_alt_email_address,s.candidate_expense_invoice_email,
      s.self_bill_no_invoices_sent
    from public.client_settings s
    where p_evaluation_date is not null
      and s.client_id=i.client_id
      and(s.effective_from is null or s.effective_from<=p_evaluation_date)
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
),
line_routes as materialized (
  select f.request_no,f.request_key,f.invoice_id,l.timesheet_id,
    f.client_expense_invoice_email,
    l.timesheet_id is not null and ts.timesheet_id is null
      missing_current_timesheet,
    (
      (coalesce(ts.is_adjustment,false) or coalesce(cw.is_adjustment,false))
      and(
        upper(coalesce(ts.submission_mode::text,'')) in('MANUAL','QR')
        or nullif(btrim(coalesce(ts.qr_status::text,'')),'') is not null
        or nullif(btrim(coalesce(ts.qr_token::text,'')),'') is not null)
      and not(
        coalesce(ts.is_adjustment,false)
        and(
          left(upper(coalesce(ts.adjustment_origin::text,'')),7)='IMPORT_'
          or ts.correction_id is not null
          or nullif(btrim(coalesce(ts.correction_kind::text,'')),'') is not null))
    ) manual_adjustment,
    coalesce(ts.contract_id,cw.contract_id) contract_id
  from facts f
  join public.invoice_lines l on l.invoice_id=f.invoice_id
  left join public.timesheets ts
    on ts.timesheet_id=l.timesheet_id and ts.is_current
  left join lateral (
    select coalesce(bool_or(coalesce(w.is_adjustment,false)),false)
        is_adjustment,
      (array_agg(w.contract_id order by w.updated_at desc nulls last,
        w.created_at desc nulls last,w.id desc)
        filter(where w.contract_id is not null))[1] contract_id
    from public.contract_weeks w
    where w.timesheet_id=l.timesheet_id
  ) cw on true
  where l.timesheet_id is not null
),
contract_routes as materialized (
  select lr.*,ct.id is null contract_missing,
    coalesce(ct.overrideclientsettings,false)
      and coalesce(ct.send_manual_invoices_to_different_email,false)
      override_enabled,
    nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'')
      alt_email,
    lower(nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'') )
      contract_alt_policy_email,
    lower(coalesce(
      nullif(btrim(ct.candidate_expense_invoice_email_override),''),
      lr.client_expense_invoice_email)) effective_expense_invoice_email
  from line_routes lr
  left join public.contracts ct on ct.id=lr.contract_id
),
route_rollup as materialized (
  select f.request_no,f.request_key,f.invoice_id,
    coalesce(bool_or(cr.missing_current_timesheet),false)
      missing_current_timesheet,
    coalesce(bool_or(cr.manual_adjustment),false) has_manual_adjustment,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_id is null),false)
      missing_contract,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_missing),false)
      contract_data_missing,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled),false)
      has_contract_override,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled
      and cr.alt_email is null),false) contract_alt_missing,
    count(distinct lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_count,
    min(lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_email,
    count(distinct cr.effective_expense_invoice_email)
      filter(where cr.effective_expense_invoice_email is not null)
      expense_email_count,
    min(cr.effective_expense_invoice_email)
      filter(where cr.effective_expense_invoice_email is not null)
      expense_invoice_email,
    coalesce(jsonb_agg(distinct jsonb_build_object(
      'contract_id',cr.contract_id,
      'override_client_settings',cr.override_enabled,
      'manual_invoice_alternate_email',cr.contract_alt_policy_email,
      'contract_missing',cr.contract_missing)
      order by jsonb_build_object(
        'contract_id',cr.contract_id,
        'override_client_settings',cr.override_enabled,
        'manual_invoice_alternate_email',cr.contract_alt_policy_email,
        'contract_missing',cr.contract_missing))
      filter(where cr.manual_adjustment and cr.contract_id is not null),
      '[]'::jsonb) contract_setting_identities
  from facts f
  left join contract_routes cr
    on cr.request_no=f.request_no and cr.invoice_id=f.invoice_id
  group by f.request_no,f.request_key,f.invoice_id
),
chosen as materialized (
  select f.*,rr.missing_current_timesheet,rr.has_manual_adjustment,
    rr.missing_contract,rr.contract_data_missing,rr.has_contract_override,
    rr.contract_alt_missing,rr.contract_alt_count,rr.contract_alt_email,
    rr.expense_email_count,rr.expense_invoice_email,
    rr.contract_setting_identities,
    case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and f.invoice_stream='EXPENSE' then 'EXPENSE_INVOICE_EMAIL'
      when jsonb_array_length(f.requested_to)>0 then 'REQUESTED'
      when rr.contract_alt_count=1 then 'CONTRACT_MANUAL_ALTERNATE'
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then 'CLIENT_MANUAL_ALTERNATE'
      else 'CLIENT_PRIMARY'
    end route_source,
    case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and f.invoice_stream='EXPENSE'
        then jsonb_build_array(rr.expense_invoice_email)
      when jsonb_array_length(f.requested_to)>0 then f.requested_to
      when rr.contract_alt_count=1 then jsonb_build_array(rr.contract_alt_email)
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then jsonb_build_array(f.client_alt_email)
      else jsonb_build_array(f.primary_email)
    end selected_to,
    f.invoice_do_not_send
      or(f.invoice_stream<>'EXPENSE' and f.invoice_self_bill
        and coalesce(f.self_bill_no_invoices_sent,true))
      delivery_suppressed,
    case
      when f.invoice_do_not_send then 'DO_NOT_SEND'
      when f.invoice_stream<>'EXPENSE' and f.invoice_self_bill
        and coalesce(f.self_bill_no_invoices_sent,true)
        then 'SELF_BILL_SUPPRESSED'
    end suppression_reason
  from facts f
  left join route_rollup rr on rr.request_no=f.request_no
),
canonical as materialized (
  select c.*,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) to_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) cc_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) bcc_json,
    (select count(*)::integer
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_to,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_cc,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_bcc
  from chosen c
),
classified as materialized (
  select c.*,
    array_remove(array[
      case when c.missing_current_timesheet then
        'EMAIL_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.missing_contract then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_data_missing then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_alt_missing then
        'CONTRACT_MANUAL_EMAIL_MISSING' end,
      case when c.has_manual_adjustment and c.contract_alt_count>1 then
        'CONTRACT_MANUAL_EMAIL_CONFLICT' end,
      case when c.has_manual_adjustment and not c.has_contract_override
        and coalesce(c.client_alt_enabled,false) and c.client_alt_email is null
        then 'CLIENT_MANUAL_EMAIL_MISSING' end
    ],null)::text[] warnings,
    array_remove(array[
      case when c.request_key is null then 'REQUEST_KEY_REQUIRED' end,
      case when c.request_key_count>1 then 'REQUEST_KEY_DUPLICATE' end,
      case when c.invoice_id is null then 'INVOICE_ID_INVALID' end,
      case when c.client_id is null and c.invoice_id is not null
        then 'INVOICE_NOT_FOUND' end,
      case when p_evaluation_date is null then 'EVALUATION_DATE_REQUIRED' end,
      case when c.delivery_policy not in('ATTACH','SPLIT','SECURE_LINK')
        then 'DELIVERY_POLICY_INVALID' end,
      case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and c.invoice_stream='EXPENSE'
        and (coalesce(c.expense_email_count,0)<>1
          or jsonb_array_length(c.to_json)=0)
        then 'EXPENSE_INVOICE_EMAIL_REQUIRED' end,
      case when c.invoice_stream<>'EXPENSE'
        and not c.delivery_suppressed and jsonb_array_length(c.to_json)=0
        then 'MISSING_RECIPIENT' end,
      case when c.invalid_to>0 then 'INVALID_TO_RECIPIENT' end,
      case when c.invalid_cc>0 then 'INVALID_CC_RECIPIENT' end,
      case when c.invalid_bcc>0 then 'INVALID_BCC_RECIPIENT' end
    ],null)::text[] blockers
  from canonical c
),
hashed as materialized (
  select c.*,
    encode(digest(jsonb_build_object(
      'to',case when c.delivery_suppressed then '[]'::jsonb else c.to_json end,
      'cc',case when c.delivery_suppressed then '[]'::jsonb else c.cc_json end,
      'bcc',case when c.delivery_suppressed then '[]'::jsonb else c.bcc_json end
    )::text,'sha256'),'hex') calculated_recipient_set_hash
  from classified c
),
policy_hashed as materialized (
  select h.*,
    encode(digest((jsonb_build_object(
      'policy_version',case
        when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
          then 'INVOICE_DELIVERY_ROUTE_V6'
        else 'INVOICE_DELIVERY_ROUTE_V5'
      end,
      'client_id',h.client_id,
      'invoice_week_identity',h.invoice_week_identity,
      'recipient_set_hash',h.calculated_recipient_set_hash,
      'to',case when h.delivery_suppressed then '[]'::jsonb else h.to_json end,
      'cc',case when h.delivery_suppressed then '[]'::jsonb else h.cc_json end,
      'bcc',case when h.delivery_suppressed then '[]'::jsonb else h.bcc_json end,
      'route_source',h.route_source,
      'client_settings_id',h.client_settings_id,
      'client_settings_effective_from',h.client_settings_effective_from,
      'contract_settings',h.contract_setting_identities,
      'self_bill',h.invoice_self_bill,
      'do_not_send',h.invoice_do_not_send,
      'delivery_suppressed',h.delivery_suppressed,
      'suppression_reason',h.suppression_reason,
      'warnings',to_jsonb(h.warnings),'blockers',to_jsonb(h.blockers),
      'template_version',h.template_version,
      'delivery_policy',h.delivery_policy
    ) || case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then jsonb_build_object('invoice_stream',h.invoice_stream)
      else '{}'::jsonb
    end)::text,'sha256'),'hex') calculated_route_policy_hash
  from hashed h
)
select p.request_key,p.invoice_id,
  case when p.delivery_suppressed then '[]'::jsonb else p.to_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.cc_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.bcc_json end,
  p.invalid_to,p.invalid_cc,p.invalid_bcc,p.calculated_recipient_set_hash,
  p.calculated_route_policy_hash,p.route_source,p.client_settings_id,
  p.contract_setting_identities,p_evaluation_date,p.client_id,
  p.invoice_week_identity,p.invoice_self_bill,p.invoice_do_not_send,
  p.delivery_suppressed,p.suppression_reason,to_jsonb(p.warnings),
  jsonb_build_object(
    'missing_current_timesheet',p.missing_current_timesheet,
    'manual_adjustment',p.has_manual_adjustment,
    'contract_override_count',p.contract_alt_count)
    || case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then jsonb_build_object(
          'invoice_stream',p.invoice_stream,
          'expense_email_count',p.expense_email_count)
      else '{}'::jsonb
    end,
  to_jsonb(p.blockers),
  jsonb_build_object(
    'invalid_to_count',p.invalid_to,'invalid_cc_count',p.invalid_cc,
    'invalid_bcc_count',p.invalid_bcc),
  p.calculated_route_policy_hash
from policy_hashed p
order by p.request_key nulls first,p.invoice_id nulls first,p.request_no;
$function$;

-- private._invoice_dispatch_advance_batch(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_dispatch_advance_batch(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_chunk_type text;
  v_group jsonb;
  v_part jsonb;
  v_result jsonb := '[]'::jsonb;
  v_error_code text;
  v_error_message text;
  v_error_detail text;
  v_error_hint text;
begin
  if jsonb_typeof(p_claims)<>'array'
     or jsonb_array_length(p_claims)>100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing at most 100 claims';
  end if;

  for v_chunk_type in
    select distinct x.value->>'chunk_type'
    from jsonb_array_elements(p_claims) x(value)
    where x.value->>'chunk_type' in(
      'GENERATION_GROUP','DOCUMENT_PLAN','ISSUE_INVOICE',
      'DELIVERY_PREPARE','RECONCILE')
  loop
    select jsonb_agg(x.value order by x.ordinality)
    into v_group
    from jsonb_array_elements(p_claims) with ordinality x(value,ordinality)
    where x.value->>'chunk_type'=v_chunk_type
      and exists (
        select 1
        from public.invoice_operation_chunks carrier
        join public.invoice_operations root
          on root.id=carrier.operation_id
        where carrier.id=case
            when pg_input_is_valid(
              coalesce(x.value->>'chunk_id',''),
              'uuid'
            )
              then (x.value->>'chunk_id')::uuid
          end
          and (
            root.entity_type is distinct from 'INVOICE_BATCH'
            or (
              not carrier.is_manifest_member
              and coalesce(
                carrier.payload_json->>'is_selection_expander',
                'false'
              ) in ('true','t','1','yes','on')
              and carrier.chunk_type in (
                'GENERATION_GROUP',
                'ISSUE_INVOICE'
              )
              and carrier.phase in (
                'BUILD_MANIFEST',
                'RELEASE_MANIFEST'
              )
            )
            or (
              carrier.is_manifest_member
              and root.manifest_committed
              and carrier.manifest_committed
              and carrier.phase not in (
                'AWAITING_MANIFEST_COMMIT',
                'AWAITING_RELEASE'
              )
              and coalesce(
                carrier.payload_json->>'is_selection_expander',
                'false'
              ) not in ('true','t','1','yes','on')
            )
          )
      );

    /* One set-based call is made for each chunk type.  Expected business
       invalidity is returned by processors as typed rows. */
    if v_group is null then
      continue;
    end if;

    begin
      case v_chunk_type
        when 'GENERATION_GROUP' then
          v_part:=private._invoice_generation_advance_batch(v_group,v_now);
        when 'DOCUMENT_PLAN' then
          v_part:=private._invoice_document_advance_batch(v_group,v_now);
        when 'ISSUE_INVOICE' then
          v_part:=private._invoice_issue_advance_batch(v_group,v_now);
        when 'DELIVERY_PREPARE' then
          v_part:=private._invoice_delivery_advance_batch(v_group,v_now);
        when 'RECONCILE' then
          v_part:=private._invoice_reconcile_advance_batch(v_group,v_now);
      end case;
      v_result:=v_result||coalesce(v_part,'[]'::jsonb);
    exception when others then
      get stacked diagnostics
        v_error_code=returned_sqlstate,
        v_error_message=message_text,
        v_error_detail=pg_exception_detail,
        v_error_hint=pg_exception_hint;

      with supplied as materialized (
        select (x.value->>'chunk_id')::uuid chunk_id,
          (x.value->>'lease_token')::uuid lease_token,
          (x.value->>'fence_token')::bigint fence_token
        from jsonb_array_elements(v_group) x(value)
      ),
      changed as (
        update public.invoice_operation_chunks c
        set status=case when c.attempt_count>=c.max_attempts
              then 'DEAD_LETTER' else 'RETRY_WAIT' end,
            phase=case when c.attempt_count>=c.max_attempts
              then 'DEAD_LETTER' else c.phase end,
            run_after_utc=case when c.attempt_count>=c.max_attempts
              then c.run_after_utc
              else v_now+make_interval(secs=>
                least(900,15*(2^least(c.attempt_count,6)))::integer)
                +make_interval(secs=>(random()*10)::integer) end,
            error_json=jsonb_build_object(
              'code','PHASE_PROCESSOR_EXCEPTION',
              'chunk_type',v_chunk_type,'sqlstate',v_error_code,
              'message',left(coalesce(v_error_message,'Processor exception'),500),
              'detail',nullif(left(coalesce(v_error_detail,''),500),''),
              'hint',nullif(left(coalesce(v_error_hint,''),300),''),
              'retryable',c.attempt_count<c.max_attempts,'at_utc',v_now,
              'history',coalesce((
                select jsonb_agg(h.value order by h.ordinality)
                from jsonb_array_elements(
                  coalesce(c.error_json->'history','[]'::jsonb))
                  with ordinality h(value,ordinality)
                where h.ordinality>greatest(jsonb_array_length(
                  coalesce(c.error_json->'history','[]'::jsonb))-6,0)
              ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
                'code',coalesce(c.error_json->>'code','UNKNOWN'),
                'at_utc',v_now))),
            failed_at_utc=case when c.attempt_count>=c.max_attempts
              then v_now else null end,
            lease_owner=null,lease_token=null,lease_expires_at_utc=null,
            updated_at_utc=v_now
        from supplied s
        where c.id=s.chunk_id and c.status='RUNNING'
          and c.lease_token=s.lease_token and c.fence_token=s.fence_token
        returning c.id,c.operation_id,c.status,c.phase,c.error_json
      )
      select v_result||coalesce(jsonb_agg(jsonb_build_object(
        'chunk_id',id,'operation_id',operation_id,'status',status,
        'phase',phase,'error',error_json)),'[]'::jsonb)
      into v_result
      from changed;
    end;
  end loop;

  return v_result;
end;
$function$;

-- private._invoice_document_advance_batch_v6_downstream(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_document_advance_batch_v6_downstream(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb:='[]'::jsonb;
  v_part jsonb;
begin
  -- BUILD_MANIFEST creates metadata and bounded dependency work only.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='BUILD_MANIFEST'
  ),
  base as materialized (
    select c.*,o.source_revision,o.template_version,
      coalesce(c.payload_json->>'purpose',
        case when c.entity_type='TIMESHEET' then 'TIMESHEET' else 'DRAFT_PREVIEW' end) purpose
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
  ),
  reference_scope as materialized (
    select r.*
    from private._invoice_reference_rows_batch((
      select coalesce(array_agg(distinct b.entity_id),
        array[]::uuid[])
      from base b where b.entity_type='INVOICE'
    )) r
  ),
  legacy_version_seed as materialized (
    select b.*,v.id version_id,
      case when b.purpose='FINAL_ISSUE' then v.snapshot_json
        when b.entity_type='INVOICE' then
          jsonb_build_object(
            'snapshot_schema_version','INVOICE_PRESENTATION_SNAPSHOT_V4',
            'presentation_model',(select jsonb_build_object(
              'document_type',case when i.type='CREDIT_NOTE' then 'CREDIT_NOTE'
                when lower(coalesce(i.header_snapshot_json->>'self_bill','false'))='true' then 'SELF_BILL_INVOICE'
                else 'INVOICE' end,
              'invoice_number',i.invoice_no,
              'issue_date',i.issued_at_utc,
              'preview_date',case when i.status='DRAFT' then v_now else null end,
              'tax_point',i.header_snapshot_json->'tax_point_utc',
              'due_date',i.due_at_utc,
              'currency',coalesce(i.header_snapshot_json->'currency','"GBP"'::jsonb),
              'po_reference',i.header_snapshot_json->'po_reference',
              'payment_terms_days',i.header_snapshot_json->'payment_terms_days',
              'payment_terms_text',i.header_snapshot_json->'payment_terms_text',
              'supplier_legal_name',i.header_snapshot_json->'agency_name',
              'supplier_trading_name',i.header_snapshot_json->'agency_trading_name',
              'supplier_registered_address',i.header_snapshot_json->'registered_address',
              'company_registration_number',i.header_snapshot_json->'company_reg_number',
              'vat_registration_number',i.header_snapshot_json->'vat_registration_number',
              'supplier_contact',i.header_snapshot_json->'supplier_contact',
              'bank_name',i.header_snapshot_json->'bank_name',
              'bank_sort_code',i.header_snapshot_json->'bank_sort_code',
              'bank_account_number',i.header_snapshot_json->'bank_account_number',
              'client_legal_name',i.header_snapshot_json->'client_name',
              'client_billing_address',i.header_snapshot_json->'client_invoice_address',
              'client_reference',i.header_snapshot_json->'client_reference',
              'net_total',i.subtotal_ex_vat,'vat_total',i.vat_amount,'gross_total',i.total_inc_vat,
              'amount_paid',i.header_snapshot_json->'amount_paid',
              'amount_credited',i.header_snapshot_json->'amount_credited',
              'amount_outstanding',i.header_snapshot_json->'amount_outstanding',
              'vat_breakdown',i.header_snapshot_json->'vat_breakdown',
              'credit_note',jsonb_build_object('original_invoice_id',i.original_invoice_id,
                'original_invoice_number',i.header_snapshot_json->'original_invoice_number',
                'original_invoice_date',i.header_snapshot_json->'original_invoice_date',
                'reason',i.header_snapshot_json->'credit_reason'),
              'self_bill_wording',i.header_snapshot_json->'self_bill_wording',
              'legal_wording',i.header_snapshot_json->'legal_wording',
              'attachment_policy',i.header_snapshot_json->'attachment_policy',
              'branding_asset_identity',i.header_snapshot_json->'agency_logo',
              'template_version',b.template_version,'locale','en-GB',
              'page_geometry','A4_PORTRAIT_210X297MM')
              from public.invoices i where i.id=b.entity_id),
            'invoice',(select jsonb_build_object(
              'id',i.id,'type',i.type,'status',i.status,'invoice_no',i.invoice_no,
              'client_id',i.client_id,'status_date_utc',i.status_date_utc,
              'issued_at_utc',i.issued_at_utc,'due_at_utc',i.due_at_utc,
              'subtotal_ex_vat',i.subtotal_ex_vat,'vat_amount',i.vat_amount,
              'total_inc_vat',i.total_inc_vat,'original_invoice_id',i.original_invoice_id,
              'notes',i.notes,'header',i.header_snapshot_json,
              'document_revision',i.document_revision)
              from public.invoices i where i.id=b.entity_id),
            'lines',coalesce((select jsonb_agg(jsonb_build_object(
              'id',l.id,'timesheet_id',l.timesheet_id,'booking_id',l.booking_id,
              'description',l.description,'hours_day',l.hours_day,
              'hours_night',l.hours_night,'hours_sat',l.hours_sat,
              'hours_sun',l.hours_sun,'hours_bh',l.hours_bh,
              'pay_day',l.pay_day,'pay_night',l.pay_night,'pay_sat',l.pay_sat,
              'pay_sun',l.pay_sun,'pay_bh',l.pay_bh,
              'charge_day',l.charge_day,'charge_night',l.charge_night,
              'charge_sat',l.charge_sat,'charge_sun',l.charge_sun,
              'charge_bh',l.charge_bh,'total_pay_ex_vat',l.total_pay_ex_vat,
              'total_charge_ex_vat',l.total_charge_ex_vat,
              'margin_ex_vat',l.margin_ex_vat,'vat_rate_pct',l.vat_rate_pct,
              'vat_amount',l.vat_amount,'total_inc_vat',l.total_inc_vat,
              'source_key',l.source_key,'business_meta',l.meta_json)
              order by l.created_at,l.id)
              from public.invoice_lines l where l.invoice_id=b.entity_id),'[]'::jsonb),
            'timesheet_sources',coalesce((select jsonb_agg(jsonb_build_object(
              'timesheet_id',src.timesheet_id,
              'submission_mode',src.submission_mode,
              'document_revision',src.document_revision,
              'manual_document_asset_id',src.manual_document_asset_id,
              'client_is_nhsp',src.client_is_nhsp,
              'no_timesheet_required',src.no_timesheet_required,
              'attach_timesheet',src.attach_timesheet,
              'render_model',src.render_model) order by src.timesheet_id)
              from (
                select distinct t.timesheet_id,
                  t.submission_mode::text submission_mode,
                  t.document_revision,t.manual_document_asset_id,
                  coalesce(vs.client_is_nhsp,false) client_is_nhsp,
                  coalesce(vs.client_no_timesheet_required,false)
                    no_timesheet_required,
                  coalesce(pc.effective_ts_attach_to_invoice,true)
                    attach_timesheet,
                  jsonb_build_object(
                    'timesheet_id',t.timesheet_id,
                    'booking_id',t.booking_id,
                    'contract_id',t.contract_id,
                    'candidate_display',t.occupant_key_norm,
                    'hospital',t.hospital_norm,'ward',t.ward_norm,
                    'job_title',t.job_title_norm,'band',t.band,
                    'shift_label',t.shift_label_norm,
                    'week_ending_date',t.week_ending_date,
                    'reference_number',t.reference_number,
                    'sheet_scope',t.sheet_scope,
                    'submission_mode',t.submission_mode,
                    'scheduled_start_iso',t.scheduled_start_iso,
                    'scheduled_end_iso',t.scheduled_end_iso,
                    'worked_start_iso',t.worked_start_iso,
                    'worked_end_iso',t.worked_end_iso,
                    'break_start_iso',t.break_start_iso,
                    'break_end_iso',t.break_end_iso,
                    'break_minutes',t.break_minutes,
                    'worked_minutes',t.worked_minutes,
                    'day_references',t.day_references_json,
                    'actual_schedule',t.actual_schedule_json,
                    'additional_units_week',t.additional_units_week,
                    'additional_units_per_day',t.additional_units_per_day,
                    'authorisation',jsonb_build_object(
                      'name',t.auth_name,'job_title',t.auth_job_title,
                      'authorised_at_utc',t.authorised_at_server),
                    'signatures',jsonb_build_object(
                      'nurse_r2_key',t.r2_nurse_key,
                      'authorisation_r2_key',t.r2_auth_key,
                      'nurse_sha256',t.img_sha256_nurse,
                      'authorisation_sha256',t.img_sha256_auth),
                    'qr',jsonb_build_object(
                      'status',t.qr_status,'signed_hash',t.qr_signed_hash,
                      'signed_at_utc',t.qr_signed_at_utc,
                      'immutable_r2_key',t.qr_r2_key),
                    'template_version','timesheet-professional-v1',
                    'version',t.version,
                    'document_revision',t.document_revision,
                    'financials',jsonb_build_object(
                      'id',f.id,'timesheet_version',f.timesheet_version,
                      'basis',f.basis,'hours_day',f.hours_day,
                      'hours_night',f.hours_night,'hours_sat',f.hours_sat,
                      'hours_sun',f.hours_sun,'hours_bh',f.hours_bh,
                      'total_pay_ex_vat',f.total_pay_ex_vat,
                      'total_charge_ex_vat',f.total_charge_ex_vat,
                      'invoice_breakdown',f.invoice_breakdown_json))
                    render_model
                from public.invoice_lines il
                join public.timesheets t
                  on t.timesheet_id=il.timesheet_id and t.is_current
                left join public.timesheets_financials f
                  on f.timesheet_id=t.timesheet_id and f.is_current
                left join public.v_timesheets_summary_base vs
                  on vs.timesheet_id=t.timesheet_id
                left join public.v_ts_invoice_precheck pc
                  on pc.timesheet_id=t.timesheet_id
                where il.invoice_id=b.entity_id and il.timesheet_id is not null
              ) src),'[]'::jsonb),
            'references',coalesce((select jsonb_agg(jsonb_build_object(
              'timesheet_id',r.timesheet_id,'sheet_scope',r.sheet_scope,
              'submission_mode',r.submission_mode,'ref_target',r.ref_target,
              'segment_id',r.segment_id,'day_ymd',r.day_ymd,
              'current_reference',r.current_reference,'is_required',r.is_required,
              'row_key',r.row_key) order by r.row_key)
              from reference_scope r where r.invoice_id=b.entity_id),'[]'::jsonb),
            'supporting_sources',coalesce((select jsonb_agg(jsonb_build_object(
              'source_system',s.source_system,'import_id',s.import_id,
              'header_rows',s.header_rows,'header_columns',s.header_columns,
              'rows',s.rows_json) order by s.source_system,s.import_id)
              from public.invoice_hr_source_rows s
              where s.invoice_id=b.entity_id),'[]'::jsonb))
        else
          jsonb_build_object(
            'snapshot_schema_version','TIMESHEET_PRESENTATION_SNAPSHOT_V4',
            'timesheet',(select jsonb_build_object(
              'timesheet_id',t.timesheet_id,'booking_id',t.booking_id,
              'contract_id',t.contract_id,
              'candidate_display',t.occupant_key_norm,
              'hospital',t.hospital_norm,'ward',t.ward_norm,
              'job_title',t.job_title_norm,'band',t.band,
              'shift_label',t.shift_label_norm,
              'week_ending_date',t.week_ending_date,'reference_number',t.reference_number,
              'sheet_scope',t.sheet_scope,'submission_mode',t.submission_mode,
              'scheduled_start_iso',t.scheduled_start_iso,
              'scheduled_end_iso',t.scheduled_end_iso,
              'worked_start_iso',t.worked_start_iso,
              'worked_end_iso',t.worked_end_iso,
              'break_start_iso',t.break_start_iso,
              'break_end_iso',t.break_end_iso,
              'break_minutes',t.break_minutes,'worked_minutes',t.worked_minutes,
              'day_references',t.day_references_json,
              'actual_schedule',t.actual_schedule_json,
              'additional_units_week',t.additional_units_week,
              'additional_units_per_day',t.additional_units_per_day,
              'authorisation',jsonb_build_object(
                'name',t.auth_name,'job_title',t.auth_job_title,
                'authorised_at_utc',t.authorised_at_server),
              'signatures',jsonb_build_object(
                'nurse_r2_key',t.r2_nurse_key,
                'authorisation_r2_key',t.r2_auth_key,
                'nurse_sha256',t.img_sha256_nurse,
                'authorisation_sha256',t.img_sha256_auth),
              'qr',jsonb_build_object(
                'status',t.qr_status,'signed_hash',t.qr_signed_hash,
                'signed_at_utc',t.qr_signed_at_utc,
                'immutable_r2_key',t.qr_r2_key),
              'template_version','timesheet-professional-v1',
              'version',t.version,'document_revision',t.document_revision)
              from public.timesheets t
              where t.timesheet_id=b.entity_id and t.is_current),
            'financials',(select jsonb_build_object(
              'id',f.id,'timesheet_version',f.timesheet_version,'basis',f.basis,
              'hours_day',f.hours_day,'hours_night',f.hours_night,
              'hours_sat',f.hours_sat,'hours_sun',f.hours_sun,'hours_bh',f.hours_bh,
              'total_pay_ex_vat',f.total_pay_ex_vat,
              'total_charge_ex_vat',f.total_charge_ex_vat,
              'invoice_breakdown',f.invoice_breakdown_json)
              from public.timesheets_financials f
              where f.timesheet_id=b.entity_id and f.is_current))
        end
      snapshot_json
    from base b
    join public.invoice_document_versions v
      on v.id=b.document_version_id and v.operation_id=b.operation_id
      and v.entity_type=b.entity_type and v.entity_id=b.entity_id
      and v.purpose=b.purpose
  ),
  presentation_batch as materialized (
    select p.*
    from private._invoice_presentation_snapshot_batch(
      (select coalesce(jsonb_agg(jsonb_build_object(
        'request_key',s.id::text,
        'entity_type',s.entity_type,
        'entity_id',s.entity_id,
        'purpose',s.purpose,
        'template_version',s.template_version
      ) order by s.id),'[]'::jsonb)
      from legacy_version_seed s
      where s.purpose<>'FINAL_ISSUE'),
      v_now
    ) p
  ),
  version_seed as materialized (
    select s.*,
      case when s.purpose='FINAL_ISSUE' then s.snapshot_json
        else p.snapshot_json end snapshot_json_v5,
      p.snapshot_hash snapshot_hash_v5,
      p.valid presentation_valid,
      p.error_code presentation_error_code,
      p.error_detail presentation_error_detail
    from legacy_version_seed s
    left join presentation_batch p on p.request_key=s.id::text
  ),
  missing_versions as materialized (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code',coalesce(s.presentation_error_code,'DOCUMENT_VERSION_REQUIRED'),
          'document_version_id',c.document_version_id,
          'detail',coalesce(s.presentation_error_detail,'{}'::jsonb)),
        updated_at_utc=v_now
    from base b
    left join version_seed s on s.id=b.id
    where c.id=b.id and (s.id is null
      or (s.purpose<>'FINAL_ISSUE' and coalesce(s.presentation_valid,false)=false))
    returning c.id,c.status,c.phase,c.document_version_id,c.error_json
  ),
  updated_versions as (
    update public.invoice_document_versions v
    set snapshot_json=case when s.purpose='FINAL_ISSUE'
          then v.snapshot_json else s.snapshot_json_v5 end,
        snapshot_hash=case when s.purpose='FINAL_ISSUE'
          then v.snapshot_hash else s.snapshot_hash_v5 end,
        status=case when v.status='FAILED' then 'PLANNING' else v.status end
    from version_seed s where v.id=s.version_id
      and (s.purpose='FINAL_ISSUE' or s.presentation_valid)
    returning v.id
  ),
  linked as materialized (
    select c.*,s.purpose,s.version_id resolved_document_version_id
    from public.invoice_operation_chunks c join version_seed s on s.id=c.id
      and (s.purpose='FINAL_ISSUE' or s.presentation_valid)
  ),
  invoice_timesheets as materialized (
    select distinct l.id chunk_id,l.resolved_document_version_id document_version_id,
      l.operation_id,il.timesheet_id,t.submission_mode::text submission_mode,
      t.document_revision,t.manual_document_asset_id,
      coalesce(v.client_is_nhsp,false) client_is_nhsp,
      coalesce(v.client_no_timesheet_required,false) no_timesheet_required,
      coalesce(pc.effective_ts_attach_to_invoice,true) attach_timesheet,
      false frozen_source
    from linked l
    join public.invoice_lines il on l.entity_type='INVOICE' and il.invoice_id=l.entity_id
    join public.timesheets t on t.timesheet_id=il.timesheet_id and t.is_current
    left join public.v_timesheets_summary_base v on v.timesheet_id=t.timesheet_id
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=t.timesheet_id
    where l.purpose<>'FINAL_ISSUE'
    union
    select l.id,l.resolved_document_version_id,l.operation_id,t.timesheet_id,
      t.submission_mode::text,t.document_revision,t.manual_document_asset_id,
      coalesce(v.client_is_nhsp,false),coalesce(v.client_no_timesheet_required,false),
      true,false
    from linked l join public.timesheets t
      on l.entity_type='TIMESHEET' and t.timesheet_id=l.entity_id and t.is_current
    left join public.v_timesheets_summary_base v on v.timesheet_id=t.timesheet_id
    union
    select l.id,l.resolved_document_version_id,l.operation_id,
      case when coalesce(x.value->>'timesheet_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'timesheet_id')::uuid end,
      x.value->>'submission_mode',
      case when coalesce(x.value->>'document_revision','')~'^[0-9]+$'
        then (x.value->>'document_revision')::bigint end,
      case when coalesce(x.value->>'manual_document_asset_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(x.value->>'manual_document_asset_id')::uuid end,
      lower(coalesce(x.value->>'client_is_nhsp','false')) in('true','t','1','yes'),
      lower(coalesce(x.value->>'no_timesheet_required','false')) in('true','t','1','yes'),
      lower(coalesce(x.value->>'attach_timesheet','true')) in('true','t','1','yes'),true
    from linked l
    join public.invoice_document_versions dv
      on dv.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(dv.snapshot_json->'timesheet_sources')='array'
        then dv.snapshot_json->'timesheet_sources' else '[]'::jsonb end) x(value)
    where l.entity_type='INVOICE' and l.purpose='FINAL_ISSUE'
      and coalesce(x.value->>'timesheet_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and coalesce(x.value->>'document_revision','')~'^[0-9]+$'
  ),
  missing_manual_timesheet_source as materialized (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','MANUAL_TIMESHEET_ASSET_REQUIRED',
          'timesheet_id',it.timesheet_id,
          'submission_mode',it.submission_mode),
        updated_at_utc=v_now
    from linked l
    join invoice_timesheets it on it.chunk_id=l.id
    where c.id=l.id
      and l.entity_type='TIMESHEET'
      and upper(coalesce(it.submission_mode,'')) in('MANUAL','QR')
      and it.manual_document_asset_id is null
      and not exists(
        select 1
        from public.invoice_document_versions v
        where v.id=l.resolved_document_version_id
          and v.snapshot_json#>>'{presentation_model,schema_version}'=
            'TIMESHEET_RENDER_MODEL_V2'
          and v.snapshot_json#>>'{presentation_model,form_variant}'=
            'QR_UNSIGNED')
    returning c.id
  ),
  unsigned_electronic_timesheet_source as materialized (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','TIMESHEET_SIGNED_EVIDENCE_REQUIRED',
          'timesheet_id',src.timesheet_id),
        updated_at_utc=v_now
    from linked l
    join public.invoice_document_versions v
      on v.id=l.resolved_document_version_id
    cross join lateral (
      select (x.value->>'timesheet_id')::uuid timesheet_id
      from jsonb_array_elements(case
        when jsonb_typeof(v.snapshot_json->'timesheet_sources')='array'
          then v.snapshot_json->'timesheet_sources'
        else '[]'::jsonb end) x(value)
      where coalesce(x.value->>'timesheet_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        and x.value#>>'{render_model,schema_version}'=
          'TIMESHEET_RENDER_MODEL_V2'
        and x.value#>>'{render_model,form_variant}'=
          'ELECTRONIC_UNSIGNED'
        and lower(coalesce(x.value->>'attach_timesheet','true'))
          in('true','t','1','yes')
      limit 1
    ) src
    where c.id=l.id and l.entity_type='INVOICE'
    returning c.id
  ),
  evidence_candidates as materialized (
    select distinct it.chunk_id,it.document_version_id,it.operation_id,
      te.id source_id,'TIMESHEET_EVIDENCE'::text source_kind,te.storage_key original_r2_key,
      te.display_name,upper(coalesce(te.kind,'OTHER')) kind,
      coalesce(nullif(te.source_revision,''),encode(digest(concat_ws('|',
        te.id::text,te.storage_key,te.created_at::text),'sha256'),'hex'))
        source_revision,te.document_asset_id,te.created_at
    from invoice_timesheets it join public.timesheet_evidence te
      on te.timesheet_id=it.timesheet_id
    join linked root on root.id=it.chunk_id and root.entity_type='INVOICE'
    where not it.frozen_source
      and nullif(btrim(coalesce(te.storage_key,'')),'') is not null
      and upper(coalesce(te.kind,''))<>'TIMESHEET'
      and(
        upper(coalesce(te.kind,''))='MILEAGE' and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type',''))='MILEAGE')
        or upper(coalesce(te.kind,''))='TRAVEL' and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type','')) like '%TRAVEL%')
        or upper(coalesce(te.kind,''))='ACCOMMODATION' and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type','')) like '%ACCOMMODATION%')
        or upper(coalesce(te.kind,'')) not in(
          'MILEAGE','TRAVEL','ACCOMMODATION') and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type','')) like '%EXPENSE%'))
    union
    select distinct it.chunk_id,it.document_version_id,it.operation_id,
      it.timesheet_id,'MANUAL_TIMESHEET',a.original_r2_key,
      coalesce(a.original_filename,'Manual timesheet'),'TIMESHEET',
      a.source_revision,a.id,a.created_at_utc
    from invoice_timesheets it join public.invoice_document_assets a
      on a.id=it.manual_document_asset_id
    where upper(coalesce(it.submission_mode,'')) in('MANUAL','QR')
      and it.attach_timesheet and not it.no_timesheet_required
    union
    select distinct l.id,l.resolved_document_version_id,l.operation_id,
      (x.value->>'evidence_id')::uuid,'TIMESHEET_EVIDENCE',
      coalesce(x.value->>'original_r2_key',x.value->>'storage_key'),
      coalesce(x.value->>'display_name',x.value->>'kind','Evidence'),
      upper(coalesce(x.value->>'kind','OTHER')),
      x.value->>'source_revision',
      (x.value->>'asset_id')::uuid,v.created_at_utc
    from linked l
    join public.invoice_document_versions v
      on v.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(v.snapshot_json->'supporting_manifest')='array'
        then v.snapshot_json->'supporting_manifest' else '[]'::jsonb end) x(value)
    where l.purpose='FINAL_ISSUE'
      and upper(coalesce(x.value->>'kind',''))<>'TIMESHEET'
      and coalesce(x.value->>'asset_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and coalesce(x.value->>'evidence_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  evidence_items as materialized (
    select e.chunk_id,e.document_version_id,e.operation_id,
      2000+row_number() over(partition by e.chunk_id
        order by case e.kind when 'TIMESHEET' then 0 when 'MILEAGE' then 1
          when 'TRAVEL' then 2 when 'ACCOMMODATION' then 3 else 4 end,
          e.created_at,e.source_id)::integer ordinal,
      e.source_id,e.source_kind,e.original_r2_key,e.display_name,e.kind,
      e.source_revision,e.document_asset_id
    from evidence_candidates e
  ),
  assets as materialized (
    select e.*,a.id asset_id,a.status asset_status,a.normalised_page_count,
      a.normalised_size_bytes,a.normalised_r2_key,a.normalised_manifest_json,
      a.normalised_sha256,a.normalised_manifest_hash,
      a.operation_id asset_operation_id
      ,exists(
        select 1 from public.invoice_operations ao
        where ao.id=a.operation_id
          and ao.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT'))
        asset_operation_active
    from evidence_items e left join public.invoice_document_assets a
      on a.id=e.document_asset_id
      and a.source_kind=e.source_kind and a.source_id=e.source_id
      and a.source_revision=e.source_revision and a.original_r2_key=e.original_r2_key
  ),
  render_items as materialized (
    select it.chunk_id,it.document_version_id,
      1000+row_number() over(partition by it.chunk_id order by it.timesheet_id)::integer ordinal,
      'ELECTRONIC_TIMESHEET'::text input_type,'TIMESHEET'::text source_entity_type,
      it.timesheet_id source_entity_id,it.document_revision::text source_revision,
      'Electronic timesheet'::text display_label,null::integer expected_page_count,
      'TIMESHEET_POLICY'::text inclusion_reason
    from invoice_timesheets it
    join linked root on root.id=it.chunk_id and root.entity_type='INVOICE'
    where upper(coalesce(it.submission_mode,''))='ELECTRONIC'
      and it.attach_timesheet and not it.client_is_nhsp and not it.no_timesheet_required
    union all
    select l.id,l.resolved_document_version_id,
      3000+row_number() over(partition by l.id
        order by r.source_system,r.import_id)::integer,
      case when upper(r.source_system)='NHSP' then 'NHSP_SUPPORT'
        else 'HEALTHROSTER_SUPPORT' end,
      upper(r.source_system),r.import_id,
      encode(digest(concat_ws('|',r.import_id::text,r.header_rows::text,
        r.header_columns::text,r.rows_json::text),'sha256'),'hex'),
      initcap(lower(r.source_system))||' supporting report',null::integer,
      'FROZEN_SOURCE_SUPPORT'
    from linked l join public.invoice_hr_source_rows r
      on l.entity_type='INVOICE' and r.invoice_id=l.entity_id
    where l.purpose<>'FINAL_ISSUE'
    union all
    select l.id,l.resolved_document_version_id,
      3000+x.ordinality::integer,
      case when upper(x.value->>'source_system')='NHSP' then 'NHSP_SUPPORT'
        else 'HEALTHROSTER_SUPPORT' end,
      upper(x.value->>'source_system'),
      (x.value->>'import_id')::uuid,
      encode(digest(concat_ws('|',x.value->>'import_id',
        (x.value->'header_rows')::text,(x.value->'header_columns')::text,
        (x.value->'rows')::text),'sha256'),'hex'),
      initcap(lower(x.value->>'source_system'))||' supporting report',
      null::integer,'FROZEN_SOURCE_SUPPORT'
    from linked l
    join public.invoice_document_versions v
      on v.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(v.snapshot_json->'source_support')='array'
        then v.snapshot_json->'source_support' else '[]'::jsonb end)
      with ordinality x(value,ordinality)
    where l.purpose='FINAL_ISSUE'
      and coalesce(x.value->>'import_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    union all
    select l.id,l.resolved_document_version_id,4000,'HIGHER_RATE_SUPPORT',
      'INVOICE',l.entity_id,
      encode(digest(coalesce(jsonb_agg(jsonb_build_object(
        'line_id',il.id,'source_key',il.source_key,'meta',il.meta_json)
        order by il.id)::text,'[]'),'sha256'),'hex'),
      'Higher-rate supporting schedule',null::integer,'HIGHER_RATE_POLICY'
    from linked l join public.invoice_lines il
      on l.entity_type='INVOICE' and il.invoice_id=l.entity_id
    where l.purpose<>'FINAL_ISSUE'
      and (upper(coalesce(il.meta_json->>'line_type','')) like '%HIGHER%'
        or il.meta_json ? 'higher_rate')
    group by l.id,l.resolved_document_version_id,l.entity_id
    union all
    select l.id,l.resolved_document_version_id,4000,'HIGHER_RATE_SUPPORT',
      'INVOICE',l.entity_id,
      encode(digest(coalesce(jsonb_agg(x.value order by x.ordinality)::text,
        '[]'),'sha256'),'hex'),
      'Higher-rate supporting schedule',null::integer,'HIGHER_RATE_POLICY'
    from linked l
    join public.invoice_document_versions v
      on v.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(v.snapshot_json->'lines')='array'
        then v.snapshot_json->'lines' else '[]'::jsonb end)
      with ordinality x(value,ordinality)
    where l.purpose='FINAL_ISSUE'
      and (upper(coalesce(x.value#>>'{business_meta,line_type}','')) like '%HIGHER%'
        or coalesce(x.value->'business_meta','{}'::jsonb)?'higher_rate')
    group by l.id,l.resolved_document_version_id,l.entity_id
  ),
  section_pages as materialized (
    select l.id chunk_id,l.resolved_document_version_id document_version_id,
      1900 ordinal,'SECTION_SEPARATOR' input_type,
      'DOCUMENT' source_entity_type,
      l.resolved_document_version_id source_entity_id,
      encode(digest(l.resolved_document_version_id::text||
        '|EVIDENCE','sha256'),'hex') source_revision,
      null::uuid asset_id,null::uuid input_document_version_id,
      'Timesheet and evidence attachments' display_label,
      1 expected_page_count,'SECTION_SEPARATOR' inclusion_reason
    from linked l
    where exists(select 1 from assets a where a.chunk_id=l.id)
    union all
    select l.id,l.resolved_document_version_id,2900,'SECTION_SEPARATOR',
      'DOCUMENT',l.resolved_document_version_id,
      encode(digest(l.resolved_document_version_id::text||
        '|SOURCE_SUPPORT','sha256'),'hex'),
      null,null,'HealthRoster and NHSP support',1,'SECTION_SEPARATOR'
    from linked l
    where exists(select 1 from render_items r
      where r.chunk_id=l.id and r.input_type in(
        'HEALTHROSTER_SUPPORT','NHSP_SUPPORT'))
    union all
    select l.id,l.resolved_document_version_id,3900,'SECTION_SEPARATOR',
      'DOCUMENT',l.resolved_document_version_id,
      encode(digest(l.resolved_document_version_id::text||
        '|HIGHER_RATE_SUPPORT','sha256'),'hex'),
      null,null,'Higher-rate support',1,'SECTION_SEPARATOR'
    from linked l
    where exists(select 1 from render_items r
      where r.chunk_id=l.id and r.input_type='HIGHER_RATE_SUPPORT')
  ),
  manifest_items_raw as materialized (
    select l.id chunk_id,l.resolved_document_version_id document_version_id,0 ordinal,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE'
        else 'ELECTRONIC_TIMESHEET' end input_type,l.entity_type source_entity_type,
      l.entity_id source_entity_id,
      coalesce((select source_revision from public.invoice_document_versions v where v.id=l.resolved_document_version_id),'') source_revision,
      null::uuid asset_id,null::uuid input_document_version_id,'Invoice core' display_label,
      null::integer expected_page_count,'CORE' inclusion_reason
    from linked l
    where not exists(
        select 1 from missing_manual_timesheet_source missing
        where missing.id=l.id)
      and not exists(
        select 1 from unsigned_electronic_timesheet_source unsigned_source
        where unsigned_source.id=l.id)
      and (
        l.entity_type='INVOICE'
        or exists(
          select 1 from invoice_timesheets it
          where it.chunk_id=l.id
            and upper(coalesce(it.submission_mode,''))='ELECTRONIC')
        or (
          l.entity_type='TIMESHEET'
          and exists(
            select 1
            from public.invoice_document_versions v
            where v.id=l.resolved_document_version_id
              and v.snapshot_json#>>'{presentation_model,schema_version}'=
                'TIMESHEET_RENDER_MODEL_V2'
              and v.snapshot_json#>>'{presentation_model,form_variant}'=
                'QR_UNSIGNED'))
      )
    union all
    select s.chunk_id,s.document_version_id,s.ordinal,s.input_type,
      s.source_entity_type,s.source_entity_id,s.source_revision,s.asset_id,
      s.input_document_version_id,s.display_label,s.expected_page_count,s.inclusion_reason
    from section_pages s
    union all
    select r.chunk_id,r.document_version_id,r.ordinal,r.input_type,
      r.source_entity_type,r.source_entity_id,r.source_revision,
      null::uuid,null::uuid,r.display_label,r.expected_page_count,r.inclusion_reason
    from render_items r
    union all
    select a.chunk_id,a.document_version_id,a.ordinal,'ASSET',a.source_kind,a.source_id,a.source_revision,
      a.asset_id,null,a.display_name,a.normalised_page_count,'CONFIGURED_EVIDENCE'
    from assets a
  ),
  manifest_items as materialized (
    select r.chunk_id,r.document_version_id,
      row_number() over(partition by r.chunk_id
        order by r.ordinal,r.input_type,r.source_entity_type,
          r.source_entity_id,r.asset_id)::integer-1 ordinal,
      r.input_type,r.source_entity_type,r.source_entity_id,r.source_revision,
      r.asset_id,r.input_document_version_id,r.display_label,
      r.expected_page_count,r.inclusion_reason
    from manifest_items_raw r
  ),
  manifests as materialized (
    select m.chunk_id,m.document_version_id,
      jsonb_agg(jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,
        'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,
        'source_revision',m.source_revision,'document_asset_id',m.asset_id,
        'input_document_version_id',m.input_document_version_id,'display_label',m.display_label,
        'expected_page_count',m.expected_page_count,'inclusion_reason',m.inclusion_reason,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,
          m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,
          m.source_revision),'sha256'),'hex'))
        order by m.ordinal,m.source_entity_id) manifest_json
    from manifest_items m group by m.chunk_id,m.document_version_id
  ),
  version_updates as (
    update public.invoice_document_versions v
    set manifest_json=m.manifest_json,manifest_hash=encode(digest(m.manifest_json::text,'sha256'),'hex'),
      status='WAITING_FOR_INPUTS',
      expected_page_count=(select sum(coalesce(x.expected_page_count,0)) from manifest_items x where x.chunk_id=m.chunk_id)
    from manifests m where v.id=m.document_version_id returning v.id
  ),
  core_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER' else 'SOURCE_RENDER' end,
      'RENDER',
      encode(digest(concat_ws('|',
        case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER'
          else 'SOURCE_RENDER' end,l.resolved_document_version_id::text,
        '0',mi.input_type,mi.source_revision,
        case when l.entity_type='TIMESHEET' then 'timesheet-professional-v2'
          else (select template_version from public.invoice_document_versions v
            where v.id=l.resolved_document_version_id) end,'1'),'sha256'),'hex'),
      0,l.entity_type,l.entity_id,l.resolved_document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,
        'template_version',case when l.entity_type='TIMESHEET'
          then 'timesheet-professional-v2'
          else (select template_version from public.invoice_document_versions v
            where v.id=l.resolved_document_version_id) end,
        'source_chunk_key',encode(digest(concat_ws('|',l.resolved_document_version_id::text,
          '0',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,
          l.entity_type,l.entity_id::text,
          (select source_revision from public.invoice_document_versions v
            where v.id=l.resolved_document_version_id)),'sha256'),'hex')),
      o.control_version,v_now,v_now
    from linked l
    join manifest_items mi on mi.chunk_id=l.id and mi.ordinal=0
    join public.invoice_operations o on o.id=l.operation_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing returning id,operation_id
  ),
  source_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'SOURCE_RENDER','RENDER',
      encode(digest(concat_ws('|','SOURCE_RENDER',m.document_version_id::text,
        m.ordinal::text,m.input_type,m.source_revision,
        case when m.input_type='ELECTRONIC_TIMESHEET'
          then 'timesheet-professional-v2'
          else (select template_version from public.invoice_document_versions v
            where v.id=l.resolved_document_version_id) end,'1'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,
      m.source_entity_id,l.resolved_document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',m.input_type,'source_revision',m.source_revision,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,
          m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,
          m.source_revision),'sha256'),'hex'),
        'template_version',case when m.input_type='ELECTRONIC_TIMESHEET'
          then 'timesheet-professional-v2'
          else (select template_version from public.invoice_document_versions v
            where v.id=l.resolved_document_version_id) end),
      o.control_version,v_now,v_now
    from linked l join manifest_items m on m.chunk_id=l.id
    join public.invoice_operations o on o.id=l.operation_id
    where m.ordinal>0 and m.input_type not in('ASSET','ATTACHMENT_INDEX')
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id
  ),
  asset_chunks as (
    select null::uuid id,null::uuid operation_id,
      null::uuid document_asset_id
    where false
  ),
  asset_owner_update as (
    update public.invoice_document_assets a
    set operation_id=c.operation_id,status='INSPECTING',updated_at_utc=v_now
    from asset_chunks c
    where a.id=c.document_asset_id and a.status='DISCOVERED'
    returning a.id
  ),
  attachment_metadata as materialized (
    select m.chunk_id,m.document_version_id,m.ordinal,
      coalesce(
        (select ts.candidate_name
         from public.v_timesheets_summary_base ts
         where ts.timesheet_id=case
           when m.source_entity_type in('TIMESHEET','MANUAL_TIMESHEET')
             then m.source_entity_id
           when m.source_entity_type='TIMESHEET_EVIDENCE'
             then(select te.timesheet_id from public.timesheet_evidence te
                  where te.id=m.source_entity_id limit 1)
         end limit 1),
        (select nullif(r.value->>'worker','')
         from public.invoice_document_versions dv
         cross join lateral jsonb_array_elements(
           coalesce(dv.snapshot_json->'supporting_sources','[]'::jsonb)) s(value)
         cross join lateral jsonb_array_elements(
           coalesce(s.value#>'{render_model,rows}','[]'::jsonb)) r(value)
         where dv.id=m.document_version_id
           and s.value->>'import_id'=m.source_entity_id::text
           and nullif(r.value->>'worker','') is not null
         limit 1),
        (select coalesce(nullif(r.value->>'worker_name',''),
                         nullif(r.value->>'staff_name',''))
         from public.invoice_hr_source_rows hr
         cross join lateral jsonb_array_elements(
           coalesce(hr.rows_json,'[]'::jsonb)) r(value)
         where hr.import_id=m.source_entity_id
         limit 1)
      ) worker,
      coalesce(
        (select ts.week_ending_date::text
         from public.v_timesheets_summary_base ts
         where ts.timesheet_id=case
           when m.source_entity_type in('TIMESHEET','MANUAL_TIMESHEET')
             then m.source_entity_id
           when m.source_entity_type='TIMESHEET_EVIDENCE'
             then(select te.timesheet_id from public.timesheet_evidence te
                  where te.id=m.source_entity_id limit 1)
         end limit 1),
        (select nullif(r.value->>'shift_date','')
         from public.invoice_document_versions dv
         cross join lateral jsonb_array_elements(
           coalesce(dv.snapshot_json->'supporting_sources','[]'::jsonb)) s(value)
         cross join lateral jsonb_array_elements(
           coalesce(s.value#>'{render_model,rows}','[]'::jsonb)) r(value)
         where dv.id=m.document_version_id
           and s.value->>'import_id'=m.source_entity_id::text
           and nullif(r.value->>'shift_date','') is not null
         order by r.value->>'shift_date'
         limit 1),
        (select coalesce(nullif(r.value->>'work_date',''),
                         nullif(r.value->>'date_raw',''))
         from public.invoice_hr_source_rows hr
         cross join lateral jsonb_array_elements(
           coalesce(hr.rows_json,'[]'::jsonb)) r(value)
         where hr.import_id=m.source_entity_id
         order by coalesce(r.value->>'work_date',r.value->>'date_raw')
         limit 1)
      ) week_or_date,
      coalesce(
        (select nullif(r.value->>'booking_reference','')
         from public.invoice_document_versions dv
         cross join lateral jsonb_array_elements(
           coalesce(dv.snapshot_json->'supporting_sources','[]'::jsonb)) s(value)
         cross join lateral jsonb_array_elements(
           coalesce(s.value#>'{render_model,rows}','[]'::jsonb)) r(value)
         where dv.id=m.document_version_id
           and s.value->>'import_id'=m.source_entity_id::text
           and nullif(r.value->>'booking_reference','') is not null
         limit 1),
        (select coalesce(nullif(r.value->>'reference',''),
                         nullif(r.value->>'ref_num',''),
                         nullif(r.value->>'unique_id',''))
         from public.invoice_hr_source_rows hr
         cross join lateral jsonb_array_elements(
           coalesce(hr.rows_json,'[]'::jsonb)) r(value)
         where hr.import_id=m.source_entity_id
         limit 1),
        case when m.source_entity_type='TIMESHEET_EVIDENCE'
          then m.display_label end
      ) reference
    from manifest_items m
  ),
  input_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,document_asset_id,status,priority,run_after_utc,payload_json,
      result_json,error_json,expected_page_count,actual_page_count,expected_byte_count,actual_byte_count,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'DOCUMENT_INPUT','DEPENDENCY',
      encode(digest(concat_ws('|','DOCUMENT_INPUT',m.document_version_id::text,
        m.ordinal::text,m.input_type,m.source_revision,
        coalesce(m.asset_id::text,m.input_document_version_id::text,
          m.source_entity_id::text),'1'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,m.source_entity_id,
      l.resolved_document_version_id,m.asset_id,
      case when m.input_type='ASSET' and a.status='READY' then 'COMPLETE'
        when m.input_type='ASSET' and m.asset_id is null then 'BLOCKED'
        when m.input_type='ASSET' and a.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED') then 'BLOCKED'
        when m.input_type='ASSET' and not coalesce(ax.asset_operation_active,false)
          then 'BLOCKED'
        else 'WAITING' end,
      l.priority,v_now,
      jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,
        'display_label',m.display_label,
        'worker',am.worker,'week_or_date',am.week_or_date,
        'reference',am.reference,
        'source_revision',m.source_revision,'source_entity_type',m.source_entity_type,
        'source_entity_id',m.source_entity_id,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,
          m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,
          m.source_revision),'sha256'),'hex')),
      case when m.input_type='ASSET' and a.status='READY' then jsonb_build_object(
        'r2_key',a.normalised_r2_key,'parts',a.normalised_manifest_json,
        'sha256',a.normalised_sha256,
        'normalised_manifest_hash',a.normalised_manifest_hash,
        'size_bytes',a.normalised_size_bytes,
        'page_count',a.normalised_page_count,'source_revision',m.source_revision,
        'document_asset_id',a.id) end,
      case when m.input_type='ASSET' and a.status in(
        'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        then jsonb_build_object('code','ASSET_DEPENDENCY_PERMANENT_FAILURE',
          'asset_status',a.status,'document_asset_id',a.id,'source_entity_id',m.source_entity_id)
        when m.input_type='ASSET' and m.asset_id is null
        then jsonb_build_object('code','ASSET_NOT_REGISTERED',
          'source_entity_id',m.source_entity_id,
          'source_revision',m.source_revision)
        when m.input_type='ASSET' and not coalesce(ax.asset_operation_active,false)
        then jsonb_build_object('code','ASSET_PREPARATION_NOT_STARTED',
          'document_asset_id',m.asset_id,'source_entity_id',m.source_entity_id,
          'source_revision',m.source_revision)
      end,
      m.expected_page_count,
      case when m.input_type='ASSET' and a.status='READY'
        then a.normalised_page_count end,
      case when m.input_type='ASSET' and a.status='READY'
        then a.normalised_size_bytes end,
      case when m.input_type='ASSET' and a.status='READY'
        then a.normalised_size_bytes end,
      o.control_version,v_now,v_now
    from linked l join manifest_items m on m.chunk_id=l.id
    left join attachment_metadata am on am.chunk_id=m.chunk_id
      and am.document_version_id=m.document_version_id
      and am.ordinal=m.ordinal
    left join public.invoice_document_assets a on a.id=m.asset_id
    left join assets ax on ax.chunk_id=l.id and ax.asset_id=m.asset_id
    join public.invoice_operations o on o.id=l.operation_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set
      status=case when excluded.status in('COMPLETE','BLOCKED') then excluded.status
        else invoice_operation_chunks.status end,
      result_json=coalesce(excluded.result_json,invoice_operation_chunks.result_json),
      error_json=coalesce(excluded.error_json,invoice_operation_chunks.error_json),
      actual_page_count=coalesce(excluded.actual_page_count,
        invoice_operation_chunks.actual_page_count),
      actual_byte_count=coalesce(excluded.actual_byte_count,
        invoice_operation_chunks.actual_byte_count)
      where invoice_operation_chunks.status not in('SUPERSEDED','CANCELLED')
    returning id
  ),
  advanced as (
    update public.invoice_operation_chunks c
    set document_version_id=l.resolved_document_version_id,phase='WAIT_FOR_INPUTS',status='WAITING',
      progress_json=jsonb_build_object('status_message','Waiting for document inputs',
        'manifest_items',(select jsonb_array_length(v.manifest_json) from public.invoice_document_versions v where v.id=l.resolved_document_version_id)),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from linked l where c.id=l.id
    returning c.id,c.status,c.phase,c.document_version_id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'document_version_id',document_version_id,'error',error_json)),'[]'::jsonb)
  into v_part
  from(
    select id,status,phase,document_version_id,null::jsonb error_json from advanced
    union all
    select id,status,phase,document_version_id,error_json from missing_versions
  ) outcomes;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- WAIT_FOR_INPUTS: dependencies only; no R2 polling.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='WAIT_FOR_INPUTS'
  ),
  claim_scope as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,c.priority,
      o.control_version,v.template_version,
      o.config_json->'processor_policy' processor_policy
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    join public.invoice_document_versions v on v.id=c.document_version_id
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      (select array_agg(operation_id) from claim_scope),null,null,10000) g
    where g.replacement_chain_status='VALID'
  ),
  current_inputs as materialized (
    select d.*
    from current_graph g
    join public.invoice_operation_chunks d on d.id=g.current_chunk_id
    where d.chunk_type='DOCUMENT_INPUT'
  ),
  attachment_index_ready as materialized (
    select s.*,
      index_input.id index_input_chunk_id,index_input.sequence_no index_ordinal,
      index_input.payload_json->>'source_chunk_key' source_chunk_key,
      index_input.payload_json->>'display_label' index_display_label,
      index_input.payload_json->>'input_type' index_input_type,
      index_input.payload_json->>'source_revision' index_source_revision,
      index_input.payload_json->>'snapshot_hash' snapshot_hash,
      count(other.id)::integer supporting_input_count,
      coalesce(sum(other.actual_page_count),0)::integer supporting_pages,
      coalesce(sum(other.actual_byte_count),0)::bigint supporting_bytes,
      coalesce(max(other.actual_page_count) filter(
        where other.payload_json->>'input_type'='INVOICE_CORE'),0)::integer
        core_page_count,
      jsonb_agg(jsonb_build_object(
        'input_chunk_id',other.id,'ordinal',other.sequence_no,
        'logical_source_key',other.payload_json->>'source_chunk_key',
        'label',other.payload_json->>'display_label',
        'input_type',other.payload_json->>'input_type',
        'worker',other.payload_json->>'worker',
        'week_or_date',other.payload_json->>'week_or_date',
        'reference',other.payload_json->>'reference',
        'page_count',other.actual_page_count)
        order by other.sequence_no,other.id)
        filter(where other.id is not null
          and other.payload_json->>'input_type' not in(
            'INVOICE_CORE','ATTACHMENT_INDEX','SECTION_SEPARATOR')) attachments,
      coalesce((
        select jsonb_agg(section.row_json order by section.ordinal,
          section.physical_part_no,section.input_chunk_id)
        from(
          select stream_input.sequence_no ordinal,
            stream_input.id input_chunk_id,
            part.ordinality::integer physical_part_no,
            jsonb_build_object(
              'input_chunk_id',stream_input.id,
              'ordinal',stream_input.sequence_no,
              'physical_part_no',part.ordinality,
              'logical_source_key',
                stream_input.payload_json->>'source_chunk_key',
              'input_type',stream_input.payload_json->>'input_type',
              'label',stream_input.payload_json->>'display_label',
              'worker',stream_input.payload_json->>'worker',
              'week_or_date',stream_input.payload_json->>'week_or_date',
              'reference',stream_input.payload_json->>'reference',
              'page_count',case
                when stream_input.payload_json->>'input_type'=
                    'ATTACHMENT_INDEX'
                  then 0
                when coalesce(part.value->>'page_count','')~'^[1-9][0-9]*$'
                  then(part.value->>'page_count')::integer
                else stream_input.actual_page_count end,
              'is_displayed_attachment',
                stream_input.payload_json->>'input_type' not in(
                  'INVOICE_CORE','ATTACHMENT_INDEX','SECTION_SEPARATOR'),
              'is_separator',
                stream_input.payload_json->>'input_type'='SECTION_SEPARATOR')
              row_json
          from current_inputs stream_input
          cross join lateral jsonb_array_elements(
            case
              when stream_input.payload_json->>'input_type'='ATTACHMENT_INDEX'
                then jsonb_build_array(jsonb_build_object('page_count',0))
              when jsonb_typeof(stream_input.result_json->'parts')='array'
                  and jsonb_array_length(stream_input.result_json->'parts')>0
                then stream_input.result_json->'parts'
              else jsonb_build_array(jsonb_build_object(
                'page_count',stream_input.actual_page_count))
            end) with ordinality part(value,ordinality)
          where stream_input.operation_id=s.operation_id
            and stream_input.document_version_id=s.document_version_id
            and(
              stream_input.id=index_input.id
              or stream_input.status='COMPLETE')
        ) section
      ),'[]'::jsonb) pagination_stream
    from claim_scope s
    join current_inputs index_input
      on index_input.operation_id=s.operation_id
      and index_input.document_version_id=s.document_version_id
      and index_input.payload_json->>'input_type'='ATTACHMENT_INDEX'
      and index_input.status='WAITING'
    left join current_inputs other
      on other.operation_id=s.operation_id
      and other.document_version_id=s.document_version_id
      and other.id<>index_input.id
    group by s.chunk_id,s.operation_id,s.document_version_id,s.priority,
      s.control_version,s.template_version,s.processor_policy,
      index_input.id,index_input.sequence_no,index_input.payload_json
    having count(other.id)>0 and bool_and(other.status='COMPLETE'
      and coalesce(other.actual_page_count,0)>0
      and coalesce(other.actual_byte_count,0)>0)
  ),
  attachment_index_measure as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,
      payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select a.operation_id,'SOURCE_RENDER','ATTACHMENT_INDEX_MEASURE',
      encode(digest(concat_ws('|','ATTACHMENT_INDEX',a.document_version_id::text,
        a.index_ordinal::text,a.source_chunk_key,'MEASURE','1',
        a.template_version),'sha256'),'hex'),
      1,a.index_ordinal,'DOCUMENT',a.document_version_id,
      a.document_version_id,'QUEUED',a.priority,v_now,
      jsonb_build_object(
        'render_kind','ATTACHMENT_INDEX','layout_phase','MEASURE',
        'layout_pass',1,'max_layout_passes',
          (a.processor_policy#>>
            '{attachment_index,max_render_passes}')::integer,
        'template_version',a.template_version,
        'source_chunk_key',a.source_chunk_key,
        'source_revision',a.index_source_revision,
        'presentation_model_schema_version',
          'ATTACHMENT_INDEX_PRESENTATION_V1',
        'presentation_model_hash',encode(digest(jsonb_build_object(
          'display_label',a.index_display_label,
          'input_type',a.index_input_type,
          'manifest_ordinal',a.index_ordinal)::text,'sha256'),'hex'),
        'snapshot_hash',a.snapshot_hash,
        'core_page_count',a.core_page_count,
        'supporting_input_count',a.supporting_input_count,
        'supporting_pages',a.supporting_pages,
        'supporting_bytes',a.supporting_bytes,
        'display_rows',coalesce(a.attachments,'[]'::jsonb),
        'attachments',coalesce(a.attachments,'[]'::jsonb),
        'pagination_stream',coalesce(a.pagination_stream,'[]'::jsonb),
        'determinism',jsonb_build_object(
          'policy_version',a.processor_policy->>'version',
          'layout_version',a.processor_policy#>>
            '{attachment_index,layout_version}',
          'renderer_version',a.processor_policy#>>
            '{attachment_index,renderer_version}',
          'template_version',a.processor_policy#>>
            '{attachment_index,template_version}',
          'css_font_identity',a.processor_policy#>>
            '{attachment_index,css_font_identity}',
          'page_geometry',a.processor_policy#>>
            '{attachment_index,page_geometry}',
          'locale_identity',a.processor_policy#>>
            '{attachment_index,locale_identity}')),
      a.control_version,v_now,v_now
    from attachment_index_ready a
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
      do nothing
    returning id
  ),
  state as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,
      count(d.id) dependencies,count(d.id) filter(where d.status='COMPLETE') ready,
      count(d.id) filter(where d.status in ('FAILED','DEAD_LETTER','BLOCKED')
        or(d.status='COMPLETE'
          and nullif(d.result_json->>'r2_key','') is null
          and jsonb_array_length(case
            when jsonb_typeof(d.result_json->'parts')='array'
              then d.result_json->'parts' else '[]'::jsonb end)=0)) failed
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join current_inputs d
      on d.operation_id=c.operation_id
      and d.document_version_id=c.document_version_id
    group by c.id,c.operation_id,c.document_version_id
  ),
  blocked as (
    update public.invoice_operation_chunks c set status='BLOCKED',phase='BLOCKED',
      error_json=jsonb_build_object('code','DOCUMENT_INPUT_FAILED',
        'failed_inputs',(select jsonb_agg(jsonb_build_object('chunk_id',d.id,'entity_id',d.entity_id,'error',d.error_json))
                         from current_inputs d where d.operation_id=c.operation_id
                           and d.document_version_id=c.document_version_id
                           and d.chunk_type='DOCUMENT_INPUT' and d.status in ('FAILED','DEAD_LETTER','BLOCKED'))),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from state s where c.id=s.chunk_id and s.failed>0 returning c.id
  ),
  ready_plans as materialized (
    select s.* from state s where s.dependencies>0 and s.dependencies=s.ready and s.failed=0
  ),
  physical_inputs as materialized (
    select d.id,d.operation_id,d.document_version_id,d.sequence_no,
      d.payload_json||jsonb_build_object(
        'physical_part_no',p.ordinality,
        'logical_input_chunk_id',d.id) payload_json,
      coalesce(nullif(p.value->>'r2_key',''),nullif(p.value->>'key',''))
        physical_r2_key,
      coalesce(nullif(p.value->>'sha256',''),
        nullif(d.result_json->>'sha256','')) physical_sha256,
      case when coalesce(p.value->>'page_count','')~'^[1-9][0-9]*$'
        then(p.value->>'page_count')::integer
        when jsonb_array_length(case
          when jsonb_typeof(d.result_json->'parts')='array'
            then d.result_json->'parts' else '[]'::jsonb end)=0
          then d.actual_page_count end physical_page_count,
      case when coalesce(p.value->>'size_bytes','')~'^[1-9][0-9]*$'
        then(p.value->>'size_bytes')::bigint
        when jsonb_array_length(case
          when jsonb_typeof(d.result_json->'parts')='array'
            then d.result_json->'parts' else '[]'::jsonb end)=0
          then d.actual_byte_count end physical_byte_count,
      p.ordinality::integer physical_part_no,
      d.result_json||jsonb_build_object(
        'r2_key',coalesce(nullif(p.value->>'r2_key',''),
          nullif(p.value->>'key','')),
        'sha256',coalesce(nullif(p.value->>'sha256',''),
          nullif(d.result_json->>'sha256','')))
        result_json
    from ready_plans r
    join current_inputs d
      on d.operation_id=r.operation_id and d.chunk_type='DOCUMENT_INPUT'
      and d.document_version_id=r.document_version_id
      and d.status='COMPLETE'
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(d.result_json->'parts')='array'
          and jsonb_array_length(d.result_json->'parts')>0
        then d.result_json->'parts'
        else jsonb_build_array(jsonb_build_object(
          'r2_key',d.result_json->>'r2_key',
          'sha256',d.result_json->>'sha256',
          'page_count',d.actual_page_count,
          'size_bytes',d.actual_byte_count))
      end) with ordinality p(value,ordinality)
    where nullif(coalesce(p.value->>'r2_key',p.value->>'key'),'') is not null
      and coalesce(p.value->>'sha256',d.result_json->>'sha256','')<>''
  ),
  physical_receipt_inputs as materialized (
    select p.*,
      coalesce(nullif(p.payload_json->>'source_chunk_key',''),
        p.id::text) logical_source_key,
      p.sequence_no logical_manifest_ordinal,
      encode(digest(jsonb_build_object(
        'receipt_contract','ACTUAL_BYTES_OBJECT_RECEIPT_V3',
        'logical_source_key',coalesce(
          nullif(p.payload_json->>'source_chunk_key',''),p.id::text),
        'logical_manifest_ordinal',p.sequence_no,
        'physical_part_no',p.physical_part_no,
        'object_key',p.physical_r2_key,
        'stored_sha256',p.physical_sha256,
        'expected_page_count',p.physical_page_count,
        'expected_byte_count',p.physical_byte_count
      )::text,'sha256'),'hex') expected_physical_receipt
    from physical_inputs p
  ),
  logical_receipts as materialized (
    select p.operation_id,p.document_version_id,p.id logical_input_chunk_id,
      p.logical_source_key,p.logical_manifest_ordinal,
      count(*)::integer physical_part_count,
      encode(digest(jsonb_build_object(
        'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
        'logical_source_key',p.logical_source_key,
        'logical_manifest_ordinal',p.logical_manifest_ordinal,
        'ordered_physical_receipts',string_agg(
          p.expected_physical_receipt,'||'
          order by p.physical_part_no)
      )::text,'sha256'),'hex') expected_logical_receipt
    from physical_receipt_inputs p
    group by p.operation_id,p.document_version_id,p.id,
      p.logical_source_key,p.logical_manifest_ordinal
  ),
  receipt_inputs as materialized (
    select p.*,l.expected_logical_receipt,l.physical_part_count
    from physical_receipt_inputs p
    join logical_receipts l
      on l.operation_id=p.operation_id
     and l.document_version_id=p.document_version_id
     and l.logical_input_chunk_id=p.id
  ),
  ranked_inputs as materialized (
    select d.*,
      d.physical_page_count actual_page_count,
      d.physical_byte_count actual_byte_count,
      row_number() over(partition by d.operation_id,d.document_version_id
        order by d.sequence_no,d.physical_part_no,d.id)::integer input_no,
      case when coalesce(d.result_json->>'decoded_estimate_bytes','') ~ '^[0-9]{1,18}$'
        then(d.result_json->>'decoded_estimate_bytes')::bigint
        else coalesce(d.physical_byte_count,0)*4 end decoded_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_inputs}')::integer max_inputs,
      (o.config_json->'processor_policy'
        #>>'{merge,max_pages}')::integer max_pages,
      (o.config_json->'processor_policy'
        #>>'{merge,max_input_bytes}')::bigint max_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_estimated_decoded_bytes}')::bigint max_decoded_bytes
    from receipt_inputs d
    join public.invoice_operations o on o.id=d.operation_id
  ),
  oversized_inputs as materialized (
    select r.*
    from ranked_inputs r
    where coalesce(r.actual_page_count,0)<=0
       or coalesce(r.actual_byte_count,0)<=0
       or coalesce(r.decoded_bytes,0)<=0
       or r.actual_page_count>r.max_pages
       or r.actual_byte_count>r.max_bytes
       or r.decoded_bytes>r.max_decoded_bytes
  ),
  oversized_plans as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',
        error_json=jsonb_build_object(
          'code','MERGE_INPUT_EXCEEDS_POLICY',
          'document_version_id',bad.document_version_id,
          'input_chunk_id',bad.id,
          'page_count',bad.actual_page_count,
          'size_bytes',bad.actual_byte_count,
          'decoded_estimate_bytes',bad.decoded_bytes),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from (
      select distinct on(operation_id,document_version_id)
        operation_id,document_version_id,id,actual_page_count,
        actual_byte_count,decoded_bytes
      from oversized_inputs
      order by operation_id,document_version_id,input_no
    ) bad
    where c.operation_id=bad.operation_id
      and c.document_version_id=bad.document_version_id
      and c.chunk_type='DOCUMENT_PLAN'
      and c.status not in('COMPLETE','FAILED','DEAD_LETTER',
        'CANCELLED','SUPERSEDED')
    returning c.document_version_id
  ),
  oversized_versions as (
    update public.invoice_document_versions v
    set status='FAILED',
        error_json=jsonb_build_object('code','MERGE_INPUT_EXCEEDS_POLICY')
    where v.id in(select document_version_id from oversized_plans)
      and v.status<>'READY'
    returning v.id
  ),
  packable_inputs as materialized (
    select r.*
    from ranked_inputs r
    where not exists(
      select 1 from oversized_inputs bad
      where bad.operation_id=r.operation_id
        and bad.document_version_id=r.document_version_id)
  ),
  page_numbered_inputs as materialized (
    select r.operation_id,r.document_version_id,r.input_no,
      r.actual_page_count,r.payload_json->>'input_type' input_type,
      coalesce(sum(r.actual_page_count) over(
        partition by r.operation_id,r.document_version_id
        order by r.input_no
        rows between unbounded preceding and 1 preceding),0)::integer
        preceding_page_count
    from packable_inputs r
  ),
  page_numbering_policies as materialized (
    select numbered.operation_id,numbered.document_version_id,
      coalesce(jsonb_agg(
        numbered.preceding_page_count+page_offset
        order by numbered.preceding_page_count+page_offset)
        filter(where page_offset is not null),'[]'::jsonb)
        excluded_pages
    from page_numbered_inputs numbered
    left join lateral generate_series(
      1,case when numbered.input_type='ELECTRONIC_TIMESHEET'
        then numbered.actual_page_count else 0 end
    ) page_offset on true
    group by numbered.operation_id,numbered.document_version_id
  ),
  leaf_pack(
    operation_id,document_version_id,input_no,group_no,group_input_count,
    group_pages,group_bytes,group_decoded_bytes
  ) as (
    select d.operation_id,d.document_version_id,d.input_no,0,
      1,d.actual_page_count,d.actual_byte_count,d.decoded_bytes
    from packable_inputs d
    where d.input_no=1
    union all
    select n.operation_id,n.document_version_id,n.input_no,
      p.group_no+case when split.start_new then 1 else 0 end,
      case when split.start_new then 1 else p.group_input_count+1 end,
      case when split.start_new then n.actual_page_count
        else p.group_pages+n.actual_page_count end,
      case when split.start_new then n.actual_byte_count
        else p.group_bytes+n.actual_byte_count end,
      case when split.start_new then n.decoded_bytes
        else p.group_decoded_bytes+n.decoded_bytes end
    from leaf_pack p
    join packable_inputs n
      on n.operation_id=p.operation_id
      and n.document_version_id=p.document_version_id
      and n.input_no=p.input_no+1
    cross join lateral (
      select p.group_input_count+1>n.max_inputs
        or p.group_pages+n.actual_page_count>n.max_pages
        or p.group_bytes+n.actual_byte_count>n.max_bytes
        or p.group_decoded_bytes+n.decoded_bytes>n.max_decoded_bytes
        start_new
    ) split
  ),
  leaf_group_logical as materialized (
    select d.operation_id,d.document_version_id,p.group_no,
      d.logical_source_key,min(d.logical_manifest_ordinal) logical_ordinal,
      encode(digest(jsonb_build_object(
        'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
        'logical_source_key',d.logical_source_key,
        'logical_manifest_ordinal',min(d.logical_manifest_ordinal),
        'ordered_physical_receipts',string_agg(
          d.expected_physical_receipt,'||'
          order by d.logical_manifest_ordinal,d.physical_part_no,d.id)
      )::text,'sha256'),'hex') expected_logical_receipt
    from packable_inputs d
    join leaf_pack p
      on p.operation_id=d.operation_id
     and p.document_version_id=d.document_version_id
     and p.input_no=d.input_no
    group by d.operation_id,d.document_version_id,p.group_no,
      d.logical_source_key
  ),
  leaf_group_roots as materialized (
    select groups.operation_id,groups.document_version_id,groups.group_no,
      jsonb_agg(jsonb_build_object(
        'logical_source_key',groups.logical_source_key,
        'logical_manifest_ordinal',groups.logical_manifest_ordinal,
        'physical_part_no',groups.physical_part_no,
        'physical_receipt',groups.expected_physical_receipt)
        order by groups.input_no) expected_physical_receipts,
      encode(digest(string_agg(groups.expected_physical_receipt,'||'
        order by groups.input_no),'sha256'),'hex')
        expected_physical_receipt_root,
      encode(digest(string_agg(groups.expected_physical_receipt,'||'
        order by groups.input_no),'sha256'),'hex')
        expected_child_receipt_hash,
      (
        select encode(digest(string_agg(
          logical.expected_logical_receipt,'||'
          order by logical.logical_ordinal,logical.logical_source_key),
          'sha256'),'hex')
        from leaf_group_logical logical
        where logical.operation_id=groups.operation_id
          and logical.document_version_id=groups.document_version_id
          and logical.group_no=groups.group_no
      ) expected_logical_receipt_root
    from (
      select d.operation_id,d.document_version_id,p.group_no,d.input_no,
        d.logical_source_key,d.logical_manifest_ordinal,d.physical_part_no,
        d.expected_physical_receipt
      from packable_inputs d
      join leaf_pack p
        on p.operation_id=d.operation_id
       and p.document_version_id=d.document_version_id
       and p.input_no=d.input_no
    ) groups
    group by groups.operation_id,groups.document_version_id,groups.group_no
  ),
  leaf_groups as materialized (
    select d.operation_id,d.document_version_id,p.group_no,
      jsonb_agg(jsonb_build_object('input_chunk_id',d.id,
        'input_order',d.input_no,'ordinal',d.sequence_no,
        'physical_part_no',d.physical_part_no,
        'r2_key',d.physical_r2_key,'sha256',d.physical_sha256,
        'page_count',d.actual_page_count,'size_bytes',d.actual_byte_count,
        'logical_source_key',d.logical_source_key,
        'logical_manifest_ordinal',d.logical_manifest_ordinal,
        'expected_logical_receipt',d.expected_logical_receipt,
        'expected_physical_receipt',d.expected_physical_receipt,
        'decoded_estimate_bytes',d.result_json->>'decoded_estimate_bytes')
        order by d.sequence_no,d.physical_part_no,d.id) inputs,
      encode(digest(string_agg(concat_ws('|',d.input_no::text,d.id::text,
        d.physical_r2_key,d.physical_sha256,d.actual_page_count::text,
        d.actual_byte_count::text,''),'||'
        order by d.sequence_no,d.physical_part_no,d.id),
        'sha256'),'hex') ordered_input_hash,
      roots.expected_child_receipt_hash,
      roots.expected_logical_receipt_root,
      roots.expected_physical_receipt_root,
      roots.expected_physical_receipts,
      sum(coalesce(d.actual_page_count,0))::integer expected_pages,
      sum(coalesce(d.actual_byte_count,0))::bigint expected_bytes
    from packable_inputs d
    join leaf_pack p
      on p.operation_id=d.operation_id
      and p.document_version_id=d.document_version_id
      and p.input_no=d.input_no
    join leaf_group_roots roots
      on roots.operation_id=d.operation_id
     and roots.document_version_id=d.document_version_id
     and roots.group_no=p.group_no
    group by d.operation_id,d.document_version_id,p.group_no,
      roots.expected_child_receipt_hash,
      roots.expected_logical_receipt_root,
      roots.expected_physical_receipt_root,
      roots.expected_physical_receipts
    having count(*)<=max(d.max_inputs)
      and sum(d.actual_page_count)<=max(d.max_pages)
      and sum(d.actual_byte_count)<=max(d.max_bytes)
      and sum(d.decoded_bytes)<=max(d.max_decoded_bytes)
  ),
  independent_expectations as (
    update public.invoice_document_versions v
    set expected_page_count=e.expected_pages,
      status='ASSEMBLING',
      error_json=null
    from (
      select r.document_version_id,sum(r.actual_page_count)::integer expected_pages,
        encode(digest(string_agg(concat_ws('|',r.sequence_no::text,
          r.payload_json->>'source_chunk_key',r.physical_part_no::text,
          r.physical_sha256,r.actual_page_count::text),'||'
          order by r.sequence_no,r.physical_part_no),'sha256'),'hex')
          coverage_hash
      from packable_inputs r group by r.document_version_id
    ) e
    where v.id=e.document_version_id
    returning v.id,v.expected_page_count
  ),
  merge_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,level_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      expected_page_count,expected_byte_count,operation_control_version,created_at_utc,updated_at_utc)
    select g.operation_id,'PDF_MERGE','MERGE',
      encode(digest(concat_ws('|','PDF_MERGE',g.document_version_id::text,
        '0',g.group_no::text,g.ordered_input_hash,pg.plan_generation::text,
        case when (select count(*) from leaf_groups all_groups
          where all_groups.operation_id=g.operation_id
            and all_groups.document_version_id=g.document_version_id)=1
          and (select entity_type from public.invoice_document_versions
            where id=g.document_version_id)='INVOICE'
          then 'FINAL_MERGE_SELECTIVE_V2' else 'NONE' end,
        coalesce(numbering.excluded_pages,'[]'::jsonb)::text),
        'sha256'),'hex'),
      pg.plan_generation,g.group_no,0,'DOCUMENT',
      g.document_version_id,g.document_version_id,'QUEUED',550,v_now,
      jsonb_build_object('level',0,'inputs',g.inputs,
        'fan_in',jsonb_array_length(g.inputs),
        'ordered_input_hash',g.ordered_input_hash,
        'expected_child_receipt_hash',g.expected_child_receipt_hash,
        'expected_logical_receipt_root',g.expected_logical_receipt_root,
        'expected_physical_receipt_root',g.expected_physical_receipt_root,
        'expected_physical_receipts',g.expected_physical_receipts,
        'receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
        'plan_generation',pg.plan_generation,
        'apply_final_page_numbers',
          (select count(*) from leaf_groups all_groups
             where all_groups.operation_id=g.operation_id
               and all_groups.document_version_id=g.document_version_id)=1
          and (select entity_type from public.invoice_document_versions
            where id=g.document_version_id)='INVOICE',
        'page_numbering_contract',case
          when (select count(*) from leaf_groups all_groups
             where all_groups.operation_id=g.operation_id
               and all_groups.document_version_id=g.document_version_id)=1
            and (select entity_type from public.invoice_document_versions
              where id=g.document_version_id)='INVOICE'
          then 'FINAL_MERGE_SELECTIVE_V2' else null end,
        'page_numbering_excluded_pages',case
          when (select count(*) from leaf_groups all_groups
             where all_groups.operation_id=g.operation_id
               and all_groups.document_version_id=g.document_version_id)=1
            and (select entity_type from public.invoice_document_versions
              where id=g.document_version_id)='INVOICE'
          then coalesce(numbering.excluded_pages,'[]'::jsonb)
          else '[]'::jsonb end,
        'document_entity_type',(select entity_type
          from public.invoice_document_versions where id=g.document_version_id),
        'document_version_id',g.document_version_id),
      g.expected_pages,g.expected_bytes,o.control_version,v_now,v_now
    from leaf_groups g
    join public.invoice_operations o on o.id=g.operation_id
    left join page_numbering_policies numbering
      on numbering.operation_id=g.operation_id
     and numbering.document_version_id=g.document_version_id
    cross join lateral (
      select coalesce((
        select prior.plan_generation
        from public.invoice_operation_chunks prior
        where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
          and prior.document_version_id=g.document_version_id
          and prior.level_no=0 and prior.sequence_no=g.group_no
          and prior.payload_json->>'ordered_input_hash'=g.ordered_input_hash
        order by prior.created_at_utc desc,prior.id desc limit 1),
        1+coalesce((select max(prior.plan_generation)
          from public.invoice_operation_chunks prior
          where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
            and prior.document_version_id=g.document_version_id
            and prior.level_no=0 and prior.sequence_no=g.group_no),0))
        plan_generation
    ) pg
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,level_no,sequence_no,plan_generation
  ),
  replaced_leaf_merges as (
    update public.invoice_operation_chunks stale
    set status='SUPERSEDED',phase='SUPERSEDED',completed_at_utc=v_now,
        failed_at_utc=null,
        replaced_by_chunk_id=fresh.id,replacement_required=true,
        result_json=coalesce(stale.result_json,'{}'::jsonb)
          ||jsonb_build_object('replacement_chunk_id',fresh.id),
        error_json=jsonb_build_object(
          'code','REPLACED_AFTER_UPSTREAM_RETRY',
          'replacement_chunk_id',fresh.id),
        updated_at_utc=v_now
    from merge_chunks fresh
    where stale.operation_id=fresh.operation_id
      and stale.chunk_type='PDF_MERGE'
      and stale.document_version_id=fresh.document_version_id
      and stale.level_no=fresh.level_no
      and stale.sequence_no=fresh.sequence_no
      and stale.id<>fresh.id and stale.status='BLOCKED'
      and stale.error_json->>'code'='UPSTREAM_RETRY_INVALIDATED'
    returning stale.id
  ),
  advanced as (
    update public.invoice_operation_chunks c
    set phase=case when s.failed>0 then 'BLOCKED'
                   when s.dependencies=s.ready and s.dependencies>0 then 'WAIT_FOR_MERGE'
                   else 'WAIT_FOR_INPUTS' end,
        status=case when s.failed>0 then 'BLOCKED' else 'WAITING' end,
        progress_json=jsonb_build_object('status_message',
          case when s.dependencies=s.ready and s.dependencies>0 then 'Inputs ready; merge queued' else 'Waiting for inputs' end,
          'inputs_ready',s.ready,'inputs_total',s.dependencies),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from state s where c.id=s.chunk_id
    returning c.id,c.status,c.phase,c.progress_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,
    'progress',progress_json)),'[]'::jsonb) into v_part from advanced;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- WAIT_FOR_MERGE seeds one bounded level at a time.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='WAIT_FOR_MERGE'
  ),
  claim_operations as materialized (
    select distinct c.operation_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      (select array_agg(operation_id) from claim_operations),
      null,null,10000) g
    where g.replacement_chain_status='VALID'
  ),
  current_merges as materialized (
    select m.*
    from current_graph g
    join public.invoice_operation_chunks m on m.id=g.current_chunk_id
    where m.chunk_type='PDF_MERGE'
  ),
  current_inputs as materialized (
    select d.*
    from current_graph g
    join public.invoice_operation_chunks d on d.id=g.current_chunk_id
    where d.chunk_type='DOCUMENT_INPUT'
  ),
  page_numbered_inputs as materialized (
    select d.operation_id,d.document_version_id,d.sequence_no,
      d.actual_page_count,d.payload_json->>'input_type' input_type,
      coalesce(sum(d.actual_page_count) over(
        partition by d.operation_id,d.document_version_id
        order by d.sequence_no,d.id
        rows between unbounded preceding and 1 preceding),0)::integer
        preceding_page_count
    from current_inputs d
    where d.status='COMPLETE'
  ),
  page_numbering_policies as materialized (
    select numbered.operation_id,numbered.document_version_id,
      coalesce(jsonb_agg(
        numbered.preceding_page_count+page_offset
        order by numbered.preceding_page_count+page_offset)
        filter(where page_offset is not null),'[]'::jsonb)
        excluded_pages
    from page_numbered_inputs numbered
    left join lateral generate_series(
      1,case when numbered.input_type='ELECTRONIC_TIMESHEET'
        then numbered.actual_page_count else 0 end
    ) page_offset on true
    group by numbered.operation_id,numbered.document_version_id
  ),
  plan_state as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,
      coalesce(max(m.level_no),0) current_level,
      count(m.id) filter(where m.level_no=(select max(m2.level_no)
        from current_merges m2 where m2.operation_id=c.operation_id
          and m2.document_version_id=c.document_version_id)) level_count,
      count(m.id) filter(where m.level_no=(select max(m2.level_no)
        from current_merges m2 where m2.operation_id=c.operation_id
          and m2.document_version_id=c.document_version_id)
        and m.status='COMPLETE') complete_count,
      count(m.id) filter(where m.status in ('FAILED','DEAD_LETTER','BLOCKED')) failed_count
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join current_merges m on m.operation_id=c.operation_id
      and m.document_version_id=c.document_version_id
      and m.chunk_type='PDF_MERGE'
    group by c.id,c.operation_id,c.document_version_id
  ),
  ranked_merges as materialized (
    select m.*,ps.current_level,
      row_number() over(partition by m.operation_id,m.document_version_id
        order by m.sequence_no,m.id)::integer input_no,
      coalesce(m.actual_byte_count,0)*4 decoded_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_inputs}')::integer max_inputs,
      (o.config_json->'processor_policy'
        #>>'{merge,max_pages}')::integer max_pages,
      (o.config_json->'processor_policy'
        #>>'{merge,max_input_bytes}')::bigint max_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_estimated_decoded_bytes}')::bigint max_decoded_bytes
    from plan_state ps join current_merges m on m.operation_id=ps.operation_id
      and m.document_version_id=ps.document_version_id
      and m.chunk_type='PDF_MERGE' and m.level_no=ps.current_level and m.status='COMPLETE'
    join public.invoice_operations o on o.id=m.operation_id
    where ps.failed_count=0 and ps.level_count=ps.complete_count and ps.level_count>1
  ),
  merge_pack(
    operation_id,document_version_id,input_no,group_no,group_input_count,
    group_pages,group_bytes,group_decoded_bytes
  ) as (
    select m.operation_id,m.document_version_id,m.input_no,0,
      1,m.actual_page_count,m.actual_byte_count,m.decoded_bytes
    from ranked_merges m
    where m.input_no=1
    union all
    select n.operation_id,n.document_version_id,n.input_no,
      p.group_no+case when split.start_new then 1 else 0 end,
      case when split.start_new then 1 else p.group_input_count+1 end,
      case when split.start_new then n.actual_page_count
        else p.group_pages+n.actual_page_count end,
      case when split.start_new then n.actual_byte_count
        else p.group_bytes+n.actual_byte_count end,
      case when split.start_new then n.decoded_bytes
        else p.group_decoded_bytes+n.decoded_bytes end
    from merge_pack p
    join ranked_merges n
      on n.operation_id=p.operation_id
      and n.document_version_id=p.document_version_id
      and n.input_no=p.input_no+1
    cross join lateral (
      select p.group_input_count+1>n.max_inputs
        or p.group_pages+n.actual_page_count>n.max_pages
        or p.group_bytes+n.actual_byte_count>n.max_bytes
        or p.group_decoded_bytes+n.decoded_bytes>n.max_decoded_bytes
        start_new
    ) split
  ),
  numbered_merges as materialized (
    select m.*,p.group_no,p.group_input_count input_order
    from ranked_merges m
    join merge_pack p
      on p.operation_id=m.operation_id
      and p.document_version_id=m.document_version_id
      and p.input_no=m.input_no
  ),
  next_group_physical as materialized (
    select m.operation_id,m.document_version_id,m.current_level,m.group_no,
      m.sequence_no child_sequence_no,m.id child_chunk_id,
      part.ordinality::integer child_physical_no,
      part.value physical_receipt
    from numbered_merges m
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(
          m.result_json#>'{merge_receipt,physical_receipts}')='array'
        then m.result_json#>'{merge_receipt,physical_receipts}'
        else '[]'::jsonb end) with ordinality part(value,ordinality)
  ),
  next_group_logical as materialized (
    select p.operation_id,p.document_version_id,p.current_level,p.group_no,
      p.physical_receipt->>'logical_source_key' logical_source_key,
      min((p.physical_receipt->>'logical_manifest_ordinal')::integer)
        logical_manifest_ordinal,
      encode(digest(jsonb_build_object(
        'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
        'logical_source_key',
          p.physical_receipt->>'logical_source_key',
        'logical_manifest_ordinal',
          min((p.physical_receipt->>
            'logical_manifest_ordinal')::integer),
        'ordered_physical_receipts',string_agg(
          p.physical_receipt->>'physical_receipt','||'
          order by(p.physical_receipt->>
            'logical_manifest_ordinal')::integer,
            (p.physical_receipt->>'physical_part_no')::integer,
            p.child_sequence_no,p.child_physical_no)
      )::text,'sha256'),'hex') logical_receipt
    from next_group_physical p
    where coalesce(p.physical_receipt->>'logical_manifest_ordinal','')
        ~'^[0-9]{1,9}$'
      and coalesce(p.physical_receipt->>'physical_part_no','')
        ~'^[1-9][0-9]{0,8}$'
      and coalesce(p.physical_receipt->>'physical_receipt','')
        ~'^[0-9a-f]{64}$'
      and nullif(p.physical_receipt->>'logical_source_key','') is not null
    group by p.operation_id,p.document_version_id,p.current_level,p.group_no,
      p.physical_receipt->>'logical_source_key'
  ),
  next_group_roots as materialized (
    select p.operation_id,p.document_version_id,p.current_level,p.group_no,
      count(*)::integer physical_receipt_count,
      jsonb_agg(p.physical_receipt order by
        (p.physical_receipt->>'logical_manifest_ordinal')::integer,
        (p.physical_receipt->>'physical_part_no')::integer,
        p.child_sequence_no,p.child_physical_no) physical_receipts,
      encode(digest(string_agg(
        p.physical_receipt->>'physical_receipt','||'
        order by(p.physical_receipt->>'logical_manifest_ordinal')::integer,
          (p.physical_receipt->>'physical_part_no')::integer,
          p.child_sequence_no,p.child_physical_no),
        'sha256'),'hex') expected_physical_receipt_root,
      (
        select encode(digest(string_agg(
          l.logical_receipt,'||'
          order by l.logical_manifest_ordinal,l.logical_source_key),
          'sha256'),'hex')
        from next_group_logical l
        where l.operation_id=p.operation_id
          and l.document_version_id=p.document_version_id
          and l.current_level=p.current_level
          and l.group_no=p.group_no
      ) expected_logical_receipt_root
    from next_group_physical p
    where coalesce(p.physical_receipt->>'logical_manifest_ordinal','')
        ~'^[0-9]{1,9}$'
      and coalesce(p.physical_receipt->>'physical_part_no','')
        ~'^[1-9][0-9]{0,8}$'
      and coalesce(p.physical_receipt->>'physical_receipt','')
        ~'^[0-9a-f]{64}$'
    group by p.operation_id,p.document_version_id,p.current_level,p.group_no
  ),
  next_groups as materialized (
    select m.operation_id,m.document_version_id,(m.current_level+1) next_level,m.group_no,
      jsonb_agg(jsonb_build_object('input_chunk_id',m.id,
        'input_order',m.input_order,'r2_key',m.result_json->>'r2_key',
        'sha256',m.result_json->>'sha256','page_count',m.actual_page_count,
        'size_bytes',m.actual_byte_count,
        'child_merge_receipt',m.result_json->'merge_receipt',
        'child_merge_receipt_hash',encode(digest(
          coalesce(m.result_json->'merge_receipt','{}'::jsonb)::text,
          'sha256'),'hex'),
        'logical_receipt_root',m.result_json#>>
          '{merge_receipt,combined_logical_receipt_root}',
        'physical_receipt_root',m.result_json#>>
          '{merge_receipt,combined_physical_receipt_root}')
        order by m.sequence_no,m.id) inputs,
      encode(digest(string_agg(concat_ws('|',m.input_no::text,m.id::text,
        m.result_json->>'r2_key',m.result_json->>'sha256',m.actual_page_count::text,
        m.actual_byte_count::text,encode(digest(
          coalesce(m.result_json->'merge_receipt','{}'::jsonb)::text,
          'sha256'),'hex')),'||' order by m.sequence_no,m.id),
        'sha256'),'hex') ordered_input_hash,
      encode(digest(string_agg(encode(digest(
        coalesce(m.result_json->'merge_receipt','{}'::jsonb)::text,
        'sha256'),'hex'),'||' order by m.sequence_no,m.id),
        'sha256'),'hex') expected_child_receipt_hash,
      roots.expected_logical_receipt_root,
      roots.expected_physical_receipt_root,
      roots.physical_receipts,
      sum(m.actual_page_count)::integer expected_pages,sum(m.actual_byte_count)::bigint expected_bytes
    from numbered_merges m
    join next_group_roots roots
      on roots.operation_id=m.operation_id
     and roots.document_version_id=m.document_version_id
     and roots.current_level=m.current_level
     and roots.group_no=m.group_no
    group by m.operation_id,m.document_version_id,m.current_level,m.group_no
      ,roots.expected_logical_receipt_root
      ,roots.expected_physical_receipt_root,roots.physical_receipts
    having count(*)<=max(m.max_inputs)
      and sum(m.actual_page_count)<=max(m.max_pages)
      and sum(m.actual_byte_count)<=max(m.max_bytes)
      and sum(m.decoded_bytes)<=max(m.max_decoded_bytes)
  ),
  next_merges as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,level_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      expected_page_count,expected_byte_count,operation_control_version,created_at_utc,updated_at_utc)
    select g.operation_id,'PDF_MERGE','MERGE',
      encode(digest(concat_ws('|','PDF_MERGE',g.document_version_id::text,
        g.next_level::text,g.group_no::text,g.ordered_input_hash,
        pg.plan_generation::text,
        case when (select count(*) from next_groups all_groups
          where all_groups.operation_id=g.operation_id
            and all_groups.document_version_id=g.document_version_id)=1
          and (select entity_type from public.invoice_document_versions
            where id=g.document_version_id)='INVOICE'
          then 'FINAL_MERGE_SELECTIVE_V2' else 'NONE' end,
        coalesce(numbering.excluded_pages,'[]'::jsonb)::text),
        'sha256'),'hex'),
      pg.plan_generation,g.group_no,g.next_level,'DOCUMENT',g.document_version_id,
      g.document_version_id,'QUEUED',550,v_now,
      jsonb_build_object('level',g.next_level,'inputs',g.inputs,
        'fan_in',jsonb_array_length(g.inputs),
        'ordered_input_hash',g.ordered_input_hash,
        'expected_child_receipt_hash',g.expected_child_receipt_hash,
        'expected_logical_receipt_root',g.expected_logical_receipt_root,
        'expected_physical_receipt_root',g.expected_physical_receipt_root,
        'expected_physical_receipts',g.physical_receipts,
        'receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
        'plan_generation',pg.plan_generation,
        'apply_final_page_numbers',
          (select count(*) from next_groups all_groups
             where all_groups.operation_id=g.operation_id
               and all_groups.document_version_id=g.document_version_id)=1
          and (select entity_type from public.invoice_document_versions
            where id=g.document_version_id)='INVOICE',
        'page_numbering_contract',case
          when (select count(*) from next_groups all_groups
             where all_groups.operation_id=g.operation_id
               and all_groups.document_version_id=g.document_version_id)=1
            and (select entity_type from public.invoice_document_versions
              where id=g.document_version_id)='INVOICE'
          then 'FINAL_MERGE_SELECTIVE_V2' else null end,
        'page_numbering_excluded_pages',case
          when (select count(*) from next_groups all_groups
             where all_groups.operation_id=g.operation_id
               and all_groups.document_version_id=g.document_version_id)=1
            and (select entity_type from public.invoice_document_versions
              where id=g.document_version_id)='INVOICE'
          then coalesce(numbering.excluded_pages,'[]'::jsonb)
          else '[]'::jsonb end,
        'document_entity_type',(select entity_type
          from public.invoice_document_versions where id=g.document_version_id),
        'document_version_id',g.document_version_id),
      g.expected_pages,g.expected_bytes,o.control_version,v_now,v_now
    from next_groups g join public.invoice_operations o on o.id=g.operation_id
    left join page_numbering_policies numbering
      on numbering.operation_id=g.operation_id
     and numbering.document_version_id=g.document_version_id
    cross join lateral (
      select coalesce((
        select prior.plan_generation
        from public.invoice_operation_chunks prior
        where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
          and prior.document_version_id=g.document_version_id
          and prior.level_no=g.next_level and prior.sequence_no=g.group_no
          and prior.payload_json->>'ordered_input_hash'=g.ordered_input_hash
        order by prior.created_at_utc desc,prior.id desc limit 1),
        1+coalesce((select max(prior.plan_generation)
          from public.invoice_operation_chunks prior
          where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
            and prior.document_version_id=g.document_version_id
            and prior.level_no=g.next_level and prior.sequence_no=g.group_no),0))
        plan_generation
    ) pg
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,level_no,sequence_no,plan_generation
  ),
  replaced_next_merges as (
    update public.invoice_operation_chunks stale
    set status='SUPERSEDED',phase='SUPERSEDED',completed_at_utc=v_now,
        failed_at_utc=null,
        replaced_by_chunk_id=fresh.id,replacement_required=true,
        result_json=coalesce(stale.result_json,'{}'::jsonb)
          ||jsonb_build_object('replacement_chunk_id',fresh.id),
        error_json=jsonb_build_object(
          'code','REPLACED_AFTER_UPSTREAM_RETRY',
          'replacement_chunk_id',fresh.id),
        updated_at_utc=v_now
    from next_merges fresh
    where stale.operation_id=fresh.operation_id
      and stale.chunk_type='PDF_MERGE'
      and stale.document_version_id=fresh.document_version_id
      and stale.level_no=fresh.level_no
      and stale.sequence_no=fresh.sequence_no
      and stale.id<>fresh.id and stale.status='BLOCKED'
      and stale.error_json->>'code'='UPSTREAM_RETRY_INVALIDATED'
    returning stale.id
  ),
  final_merge as materialized (
    select ps.*,m.id merge_chunk_id,m.payload_json,m.result_json,
      m.actual_page_count,m.actual_byte_count
    from plan_state ps join current_merges m on m.operation_id=ps.operation_id
      and m.document_version_id=ps.document_version_id
      and m.chunk_type='PDF_MERGE' and m.level_no=ps.current_level and m.status='COMPLETE'
    where ps.level_count=1 and ps.complete_count=1 and ps.failed_count=0
  ),
  verify_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,expected_page_count,expected_byte_count,
      operation_control_version,created_at_utc,updated_at_utc)
    select f.operation_id,'DOCUMENT_VERIFY','VERIFY',
      encode(digest(concat_ws('|','DOCUMENT_VERIFY',
        f.document_version_id::text,f.result_json->>'sha256',
        v.manifest_hash,coverage.manifest_coverage_hash,
        f.payload_json->>'expected_logical_receipt_root',
        f.payload_json->>'expected_physical_receipt_root',
        pg.plan_generation::text),'sha256'),'hex'),
      pg.plan_generation,0,'DOCUMENT',f.document_version_id,f.document_version_id,
      'QUEUED',550,v_now,jsonb_build_object('candidate_chunk_id',f.merge_chunk_id,
        'candidate_r2_key',f.result_json->>'r2_key','candidate_sha256',f.result_json->>'sha256',
        'candidate_size_bytes',f.actual_byte_count,'candidate_page_count',f.actual_page_count,
        'manifest_hash',v.manifest_hash,
        'expected_coverage_hash',coverage.manifest_coverage_hash,
        'resolved_input_coverage_hash',coverage.input_coverage_hash,
        'expected_input_count',coverage.manifest_input_count,
        'resolved_input_count',coverage.resolved_input_count,
        'expected_physical_input_count',coverage.physical_input_count,
        'expected_physical_input_hash',coverage.physical_input_hash,
        'expected_logical_source_count',receipts.logical_source_count,
        'expected_logical_root_receipt',
          f.payload_json->>'expected_logical_receipt_root',
        'expected_physical_root_receipt',
          f.payload_json->>'expected_physical_receipt_root',
        'expected_ordered_input_root',
          f.payload_json->>'expected_child_receipt_hash',
        'root_merge_receipt_identity',encode(digest(
          jsonb_build_object(
            'receipt_contract','DOCUMENT_ROOT_RECEIPT_V3',
            'logical_root',
              f.payload_json->>'expected_logical_receipt_root',
            'physical_root',
              f.payload_json->>'expected_physical_receipt_root',
            'ordered_input_root',
              f.payload_json->>'expected_child_receipt_hash',
            'page_count',v.expected_page_count,
            'output_sha256',f.result_json->>'sha256')::text,
          'sha256'),'hex'),
        'receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
        'plan_generation',pg.plan_generation,
        'final_merge_receipt',f.result_json->'merge_receipt',
        'final_merge_receipt_hash',encode(digest(
          coalesce(f.result_json->'merge_receipt','{}'::jsonb)::text,
          'sha256'),'hex'),
        'independent_expected_page_count',v.expected_page_count),
      v.expected_page_count,f.actual_byte_count,
      o.control_version,v_now,v_now
    from final_merge f join public.invoice_document_versions v on v.id=f.document_version_id
    join public.invoice_operations o on o.id=f.operation_id
    join lateral (
      select
        (select encode(digest(string_agg(concat_ws('|',
            x.value->>'ordinal',x.value->>'source_chunk_key'),
            '||' order by (x.value->>'ordinal')::integer),
          'sha256'),'hex')
         from jsonb_array_elements(v.manifest_json) x(value)
         where coalesce(x.value->>'ordinal','')~'^[0-9]+$')
          manifest_coverage_hash,
        (select encode(digest(string_agg(concat_ws('|',r.sequence_no::text,
            r.payload_json->>'source_chunk_key'),
            '||' order by r.sequence_no,r.id),'sha256'),'hex')
         from current_inputs r
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE')
          input_coverage_hash,
        jsonb_array_length(v.manifest_json)::integer manifest_input_count,
        (select count(*)::integer
         from current_inputs r
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE')
          resolved_input_count,
        (select count(*)::integer
         from current_inputs r
         cross join lateral jsonb_array_elements(
           case when jsonb_typeof(r.result_json->'normalised_manifest')='array'
                  and jsonb_array_length(r.result_json->'normalised_manifest')>0
             then r.result_json->'normalised_manifest'
             else jsonb_build_array(jsonb_build_object(
               'r2_key',coalesce(r.result_json->>'r2_key',
                 r.result_json->>'normalised_r2_key'),
               'sha256',r.result_json->>'sha256')) end)
           with ordinality part(value,ordinality)
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE'
           and coalesce(part.value->>'r2_key','')<>'')
          physical_input_count,
        (select encode(digest(string_agg(concat_ws('|',
            r.sequence_no::text,part.ordinality::text,
            part.value->>'r2_key',
            coalesce(part.value->>'sha256',r.result_json->>'sha256','')),
            '||' order by r.sequence_no,part.ordinality),'sha256'),'hex')
         from current_inputs r
         cross join lateral jsonb_array_elements(
           case when jsonb_typeof(r.result_json->'normalised_manifest')='array'
                  and jsonb_array_length(r.result_json->'normalised_manifest')>0
             then r.result_json->'normalised_manifest'
             else jsonb_build_array(jsonb_build_object(
               'r2_key',coalesce(r.result_json->>'r2_key',
                 r.result_json->>'normalised_r2_key'),
               'sha256',r.result_json->>'sha256')) end)
           with ordinality part(value,ordinality)
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE'
           and coalesce(part.value->>'r2_key','')<>'')
          physical_input_hash
    ) coverage on coverage.manifest_coverage_hash=coverage.input_coverage_hash
      and coverage.manifest_input_count=coverage.resolved_input_count
    join lateral (
      with physical as materialized (
        select r.id input_chunk_id,r.sequence_no,
          coalesce(nullif(r.payload_json->>'source_chunk_key',''),
            r.id::text) logical_source_key,
          part.ordinality::integer physical_part_no,
          coalesce(nullif(part.value->>'r2_key',''),
            nullif(part.value->>'key',''),
            nullif(r.result_json->>'r2_key',''),
            nullif(r.result_json->>'normalised_r2_key','')) object_key,
          coalesce(nullif(part.value->>'sha256',''),
            nullif(r.result_json->>'sha256','')) stored_sha256,
          case when coalesce(part.value->>'page_count','')
              ~'^[1-9][0-9]*$'
            then(part.value->>'page_count')::integer
            else r.actual_page_count end page_count,
          case when coalesce(part.value->>'size_bytes','')
              ~'^[1-9][0-9]*$'
            then(part.value->>'size_bytes')::bigint
            else r.actual_byte_count end byte_count
        from current_inputs r
        cross join lateral jsonb_array_elements(
          case
            when jsonb_typeof(r.result_json->'parts')='array'
                and jsonb_array_length(r.result_json->'parts')>0
              then r.result_json->'parts'
            when jsonb_typeof(r.result_json->'normalised_manifest')='array'
                and jsonb_array_length(r.result_json->'normalised_manifest')>0
              then r.result_json->'normalised_manifest'
            else jsonb_build_array(jsonb_build_object(
              'r2_key',coalesce(r.result_json->>'r2_key',
                r.result_json->>'normalised_r2_key'),
              'sha256',r.result_json->>'sha256',
              'page_count',r.actual_page_count,
              'size_bytes',r.actual_byte_count))
          end) with ordinality part(value,ordinality)
        where r.operation_id=f.operation_id
          and r.document_version_id=f.document_version_id
          and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE'
      ),
      physical_receipts as materialized (
        select p.*,encode(digest(jsonb_build_object(
          'receipt_contract','ACTUAL_BYTES_OBJECT_RECEIPT_V3',
          'logical_source_key',p.logical_source_key,
          'logical_manifest_ordinal',p.sequence_no,
          'physical_part_no',p.physical_part_no,
          'object_key',p.object_key,
          'stored_sha256',p.stored_sha256,
          'expected_page_count',p.page_count,
          'expected_byte_count',p.byte_count
        )::text,'sha256'),'hex') physical_receipt
        from physical p
      ),
      logical_receipts as materialized (
        select p.input_chunk_id,p.sequence_no,p.logical_source_key,
          encode(digest(jsonb_build_object(
            'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
            'logical_source_key',p.logical_source_key,
            'logical_manifest_ordinal',p.sequence_no,
            'ordered_physical_receipts',string_agg(
              p.physical_receipt,'||' order by p.physical_part_no)
          )::text,'sha256'),'hex') logical_receipt
        from physical_receipts p
        group by p.input_chunk_id,p.sequence_no,p.logical_source_key
      )
      select
        (select count(*)::integer from logical_receipts)
          logical_source_count,
        (select encode(digest(string_agg(l.logical_receipt,'||'
          order by l.sequence_no,l.logical_source_key),
          'sha256'),'hex') from logical_receipts l)
          expected_logical_root_receipt,
        (select encode(digest(string_agg(p.physical_receipt,'||'
          order by p.sequence_no,p.physical_part_no,p.input_chunk_id),
          'sha256'),'hex') from physical_receipts p)
          expected_physical_root_receipt,
        (select encode(digest(string_agg(concat_ws('|',
          p.sequence_no::text,p.logical_source_key,
          p.physical_part_no::text,p.physical_receipt),'||'
          order by p.sequence_no,p.physical_part_no,p.input_chunk_id),
          'sha256'),'hex') from physical_receipts p)
          expected_ordered_input_root
    ) receipts on receipts.logical_source_count=coverage.manifest_input_count
      and receipts.expected_logical_root_receipt=
        f.payload_json->>'expected_logical_receipt_root'
      and receipts.expected_physical_root_receipt=
        f.payload_json->>'expected_physical_receipt_root'
    cross join lateral (
      select coalesce((
        select prior.plan_generation
        from public.invoice_operation_chunks prior
        where prior.operation_id=f.operation_id
          and prior.chunk_type='DOCUMENT_VERIFY'
          and prior.document_version_id=f.document_version_id
          and prior.level_no=0 and prior.sequence_no=0
          and prior.payload_json->>'candidate_sha256'=f.result_json->>'sha256'
          and prior.payload_json->>'expected_physical_input_hash'=
            coverage.physical_input_hash
        order by prior.created_at_utc desc,prior.id desc limit 1),
        1+coalesce((select max(prior.plan_generation)
          from public.invoice_operation_chunks prior
          where prior.operation_id=f.operation_id
            and prior.chunk_type='DOCUMENT_VERIFY'
            and prior.document_version_id=f.document_version_id
            and prior.level_no=0 and prior.sequence_no=0),0))
        plan_generation
    ) pg
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,level_no,sequence_no,plan_generation
  ),
  replaced_verifications as (
    update public.invoice_operation_chunks stale
    set status='SUPERSEDED',phase='SUPERSEDED',completed_at_utc=v_now,
        failed_at_utc=null,
        replaced_by_chunk_id=fresh.id,replacement_required=true,
        result_json=coalesce(stale.result_json,'{}'::jsonb)
          ||jsonb_build_object('replacement_chunk_id',fresh.id),
        error_json=jsonb_build_object(
          'code','REPLACED_AFTER_UPSTREAM_RETRY',
          'replacement_chunk_id',fresh.id),
        updated_at_utc=v_now
    from verify_chunks fresh
    where stale.operation_id=fresh.operation_id
      and stale.chunk_type='DOCUMENT_VERIFY'
      and stale.document_version_id=fresh.document_version_id
      and stale.level_no=fresh.level_no
      and stale.sequence_no=fresh.sequence_no
      and stale.id<>fresh.id and stale.status='BLOCKED'
      and stale.error_json->>'code'='UPSTREAM_RETRY_INVALIDATED'
    returning stale.id
  ),
  advanced as (
    update public.invoice_operation_chunks c
    set phase=case when ps.failed_count>0 then 'BLOCKED'
                   when ps.level_count=1 and ps.complete_count=1 then 'COMPLETE'
                   else 'WAIT_FOR_MERGE' end,
        status=case when ps.failed_count>0 then 'BLOCKED'
                    when ps.level_count=1 and ps.complete_count=1 then 'WAITING'
                    else 'WAITING' end,
        error_json=case when ps.failed_count>0 then jsonb_build_object('code','MERGE_FAILED') else null end,
        progress_json=jsonb_build_object('status_message',
          case when ps.level_count=1 and ps.complete_count=1 then 'Final verification queued'
               else 'Waiting for merge level' end,'merge_level',ps.current_level,
          'parts_complete',ps.complete_count,'parts_total',ps.level_count),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from plan_state ps where c.id=ps.chunk_id returning c.id,c.status,c.phase,c.progress_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,
    'progress',progress_json)),'[]'::jsonb) into v_part from advanced;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- COMPLETE only observes a verified READY version.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='COMPLETE'
  ),
  ready as materialized (
    select c.*,v.r2_key,v.sha256,v.size_bytes,v.page_count
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_document_versions v on v.id=c.document_version_id and v.status='READY'
  ),
  invoice_ptr as (
    update public.invoices i set
      preview_document_version_id=case when v.purpose='DRAFT_PREVIEW' then v.id else i.preview_document_version_id end,
      invoice_pdf_r2_key=case when v.purpose='DRAFT_PREVIEW' then v.r2_key else i.invoice_pdf_r2_key end,
      invoice_pdf_generated_at_utc=case when v.purpose='DRAFT_PREVIEW' then v.verified_at_utc else i.invoice_pdf_generated_at_utc end,
      document_state=case when v.purpose='DRAFT_PREVIEW' then 'READY' else i.document_state end,
      active_document_operation_id=case when i.active_document_operation_id=v.operation_id then null else i.active_document_operation_id end,
      last_document_error_json=null,updated_at=v_now
    from ready r join public.invoice_document_versions v on v.id=r.document_version_id
    where r.entity_type='INVOICE' and i.id=r.entity_id returning i.id
  ),
  timesheet_ptr as (
    update public.timesheets t set current_document_version_id=v.id,
      manual_pdf_r2_key=case when t.manual_document_asset_id is not null
        then v.r2_key else t.manual_pdf_r2_key end,
      generated_pdf_at_utc=v.verified_at_utc,document_state='READY',
      active_document_operation_id=null,last_document_error_json=null,updated_at=v_now
    from ready r join public.invoice_document_versions v on v.id=r.document_version_id
    where r.entity_type='TIMESHEET' and t.timesheet_id=r.entity_id and t.is_current returning t.timesheet_id
  ),
  issue_requeue as (
    update public.invoice_operation_chunks issue
    set status='QUEUED',phase='FINALISE',run_after_utc=v_now,updated_at_utc=v_now
    from ready r
    where issue.chunk_type='ISSUE_INVOICE' and issue.phase='WAIT_DOCUMENT'
      and issue.document_version_id=r.document_version_id and issue.status='WAITING'
    returning issue.id
  ),
  completed as (
    update public.invoice_operation_chunks c set status='COMPLETE',
      completed_at_utc=v_now,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=jsonb_build_object('document_version_id',r.document_version_id,'r2_key',r.r2_key,
        'sha256',r.sha256,'size_bytes',r.size_bytes,'page_count',r.page_count)
    from ready r where c.id=r.id returning c.id,c.status,c.phase,c.result_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'result',result_json)),'[]'::jsonb)
    into v_part from completed;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

-- private._invoice_document_advance_batch(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_document_advance_batch(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_manifest_results jsonb := '[]'::jsonb;
  v_passthrough_results jsonb := '[]'::jsonb;
begin
  if p_claims is null or jsonb_typeof(p_claims) is distinct from 'array' then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  if jsonb_array_length(p_claims) < 1
     or jsonb_array_length(p_claims) > 100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  -- Non-BUILD_MANIFEST phases are unchanged downstream graph machinery.  The
  -- migration preserves the current implementation as
  -- private._invoice_document_advance_batch_v6_downstream.  BUILD_MANIFEST is
  -- implemented here without the obsolete V4 presentation snapshot materialisation.
  if exists (
    select 1 from jsonb_array_elements(p_claims) c
    where coalesce(c->>'phase','') <> 'BUILD_MANIFEST'
  ) then
    select private._invoice_document_advance_batch_v6_downstream(
      (select coalesce(jsonb_agg(c),'[]'::jsonb)
       from jsonb_array_elements(p_claims) c
       where coalesce(c->>'phase','') <> 'BUILD_MANIFEST'),
      v_now)
    into v_passthrough_results;
  end if;

  if not exists (
    select 1 from jsonb_array_elements(p_claims) c
    where coalesce(c->>'phase','') = 'BUILD_MANIFEST'
  ) then
    return coalesce(v_passthrough_results,'[]'::jsonb);
  end if;

  with claim_ids as materialized (
    select (c->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) c
    where coalesce(c->>'phase','')='BUILD_MANIFEST'
      and coalesce(c->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  base as materialized (
    select c.id,c.operation_id,c.entity_type,c.entity_id,c.document_version_id,c.priority,
           c.operation_control_version,c.payload_json,c.status,c.phase,c.plan_generation,
           o.source_revision,o.template_version,o.control_version,
           coalesce(c.payload_json->>'purpose',case when c.entity_type='TIMESHEET' then 'TIMESHEET' else 'DRAFT_PREVIEW' end) purpose
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    where c.phase='BUILD_MANIFEST' and c.chunk_type='DOCUMENT_PLAN'
  ),
  version_base as materialized (
    select b.*,v.id version_id,v.snapshot_json existing_snapshot_json,v.snapshot_hash existing_snapshot_hash,
           v.template_version version_template_version
    from base b
    join public.invoice_document_versions v
      on v.id=b.document_version_id and v.operation_id=b.operation_id
     and v.entity_type=b.entity_type and v.entity_id=b.entity_id and v.purpose=b.purpose
  ),
  presentation_requests as materialized (
    select jsonb_agg(jsonb_build_object(
        'request_key',b.id::text,
        'entity_type',b.entity_type,
        'entity_id',b.entity_id,
        'purpose',b.purpose,
        'template_version',coalesce(b.version_template_version,b.template_version),
        'issue_at_utc',b.payload_json->>'frozen_issue_at_utc',
        'tax_point_utc',coalesce(b.payload_json->>'frozen_tax_point_utc',b.payload_json->>'frozen_issue_at_utc'),
        'due_at_utc',b.payload_json->>'frozen_due_at_utc') order by b.id) request_json
    from version_base b where b.purpose<>'FINAL_ISSUE'
    having count(*) > 0
  ),
  presentation_batch as materialized (
    select p.*
    from presentation_requests pr
    cross join lateral private._invoice_presentation_snapshot_batch(pr.request_json, v_now) p
  ),
  version_seed as materialized (
    select b.*,
      case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_json else p.snapshot_json end snapshot_json_v5,
      case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_hash else p.snapshot_hash end snapshot_hash_v5,
      case when b.purpose='FINAL_ISSUE' then true else coalesce(p.valid,false) end presentation_valid,
      p.error_code presentation_error_code,p.error_detail presentation_error_detail,
      coalesce(case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_json#>>'{presentation_model,schema_version}' else p.presentation_model->>'schema_version' end,'') presentation_schema,
      coalesce(case when b.purpose='FINAL_ISSUE' then b.existing_snapshot_json->>'presentation_model_hash' else p.snapshot_json->>'presentation_model_hash' end,'') presentation_hash
    from version_base b
    left join presentation_batch p on p.request_key=b.id::text
  ),
  blocked as materialized (
    update public.invoice_operation_chunks c
       set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
           lease_owner=null,lease_token=null,lease_expires_at_utc=null,
           error_json=jsonb_build_object('code',coalesce(s.presentation_error_code,'DOCUMENT_PRESENTATION_INVALID'),
             'detail',coalesce(s.presentation_error_detail,'{}'::jsonb),
             'document_version_id',s.document_version_id),
           updated_at_utc=v_now
    from version_seed s
    where c.id=s.id and not s.presentation_valid
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  ),
  linked as materialized (
    select s.*
    from version_seed s
    where s.presentation_valid
  ),
  direct_timesheet as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,
      upper(coalesce(t.submission_mode::text,'')) submission_mode,
      t.manual_document_asset_id,
      a.id asset_id,a.source_kind,a.source_id,a.source_revision,
      a.original_filename,a.normalised_page_count,a.status asset_status
    from linked l
    join public.timesheets t
      on l.entity_type='TIMESHEET'
      and t.timesheet_id=l.entity_id
      and t.is_current
    left join public.invoice_document_assets a
      on a.id=t.manual_document_asset_id
  ),
  blocked_direct_timesheet_source as materialized (
    update public.invoice_operation_chunks c
       set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
           lease_owner=null,lease_token=null,lease_expires_at_utc=null,
           error_json=jsonb_build_object(
             'code','MANUAL_TIMESHEET_ASSET_REQUIRED',
             'timesheet_id',c.entity_id,
             'submission_mode',dt.submission_mode),
           updated_at_utc=v_now
    from direct_timesheet dt
    where c.id=dt.chunk_id
      and dt.submission_mode in('MANUAL','QR')
      and dt.asset_id is null
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  ),
  invoice_ts as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,
      x.value timesheet_source,
      case when coalesce(x.value->>'timesheet_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'timesheet_id')::uuid end timesheet_id,
      upper(coalesce(x.value->>'submission_mode','')) submission_mode,
      case when lower(coalesce(x.value->>'attach_timesheet','')) in('true','t','1','yes') then true when lower(coalesce(x.value->>'attach_timesheet','')) in('false','f','0','no') then false else true end attach_timesheet,
      case when lower(coalesce(x.value->>'client_is_nhsp','')) in('true','t','1','yes') then true else false end client_is_nhsp,
      case when lower(coalesce(x.value->>'no_timesheet_required','')) in('true','t','1','yes') then true else false end no_timesheet_required,
      x.value#>>'{render_model,document_revision}' document_revision,
      case when coalesce(x.value->>'manual_document_asset_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x.value->>'manual_document_asset_id')::uuid end manual_document_asset_id
    from linked l
    cross join lateral jsonb_array_elements(case when jsonb_typeof(l.snapshot_json_v5->'timesheet_sources')='array' then l.snapshot_json_v5->'timesheet_sources' else '[]'::jsonb end) x(value)
    where l.entity_type='INVOICE'
  ),
  support_sources as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,x.ordinality::integer source_no,x.value support_source,
      upper(coalesce(x.value->>'source_system',x.value#>>'{render_model,source_identity,source_system}','HEALTHROSTER')) source_system,
      case when coalesce(x.value->>'import_id',x.value#>>'{render_model,source_identity,import_id}','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (coalesce(x.value->>'import_id',x.value#>>'{render_model,source_identity,import_id}'))::uuid end import_id
    from linked l
    cross join lateral jsonb_array_elements(case when jsonb_typeof(l.snapshot_json_v5->'supporting_sources')='array' then l.snapshot_json_v5->'supporting_sources' else '[]'::jsonb end) with ordinality x(value,ordinality)
    where l.entity_type='INVOICE'
  ),
  evidence_assets as materialized (
    select l.id chunk_id,l.operation_id,l.document_version_id,x.ordinality::integer evidence_no,x.value evidence,
      case when coalesce(x.value->>'asset_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x.value->>'asset_id')::uuid end asset_id,
      coalesce(x.value->>'source_revision',encode(digest(x.value::text,'sha256'),'hex')) source_revision,
      upper(coalesce(x.value->>'kind','EVIDENCE')) kind,
      coalesce(x.value->>'display_name',x.value->>'kind','Evidence') display_label,
      case when coalesce(x.value->>'evidence_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x.value->>'evidence_id')::uuid end evidence_id
    from linked l
    cross join lateral jsonb_array_elements(case when jsonb_typeof(l.snapshot_json_v5->'supporting_manifest')='array' then l.snapshot_json_v5->'supporting_manifest' else '[]'::jsonb end) with ordinality x(value,ordinality)
    where l.entity_type='INVOICE'
  ),
  manifest_items as materialized (
    select l.id chunk_id,l.document_version_id,0::integer ordinal,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end input_type,
      l.entity_type source_entity_type,l.entity_id source_entity_id,
      l.source_revision source_revision,
      null::uuid document_asset_id,null::uuid input_document_version_id,
      case when l.entity_type='INVOICE' then 'Invoice core document' else 'Electronic timesheet' end display_label,
      0::integer expected_page_count,'CORE_RENDER'::text inclusion_reason,
      l.presentation_schema,l.presentation_hash
    from linked l
    left join direct_timesheet dt on dt.chunk_id=l.id
    where l.entity_type='INVOICE' or dt.submission_mode='ELECTRONIC'
    union all
    select dt.chunk_id,dt.document_version_id,0::integer,
      'ASSET',dt.source_kind,dt.source_id,dt.source_revision,
      dt.asset_id,null::uuid,
      coalesce(nullif(dt.original_filename,''),'Manual/evidence timesheet asset'),
      dt.normalised_page_count,'TIMESHEET_ASSET_POLICY',
      'ASSET_SOURCE_V1',
      encode(digest(concat_ws('|',dt.asset_id::text,dt.source_revision,
        dt.source_kind,dt.source_id::text),'sha256'),'hex')
    from direct_timesheet dt
    where dt.submission_mode in('MANUAL','QR') and dt.asset_id is not null
    union all
    select it.chunk_id,it.document_version_id,
      1000+row_number() over(partition by it.chunk_id order by it.timesheet_id)::integer,
      'ELECTRONIC_TIMESHEET','TIMESHEET',it.timesheet_id,
      coalesce(it.document_revision,encode(digest(it.timesheet_source::text,'sha256'),'hex')),
      null::uuid,null::uuid,'Electronic timesheet',null::integer,'TIMESHEET_POLICY',
      'TIMESHEET_RENDER_MODEL_V2',
      encode(digest((it.timesheet_source->'render_model')::text,'sha256'),'hex')
    from invoice_ts it
    where it.submission_mode='ELECTRONIC' and it.attach_timesheet and not it.client_is_nhsp and not it.no_timesheet_required and it.timesheet_id is not null
    union all
    select it.chunk_id,it.document_version_id,
      2000+row_number() over(partition by it.chunk_id order by it.timesheet_id)::integer,
      'ASSET','TIMESHEET',it.timesheet_id,
      coalesce(it.document_revision,encode(digest(it.timesheet_source::text,'sha256'),'hex')),
      it.manual_document_asset_id,null::uuid,'Manual/evidence timesheet asset',null::integer,'TIMESHEET_ASSET_POLICY',
      'ASSET_SOURCE_V1',encode(digest(coalesce(it.timesheet_source,'{}'::jsonb)::text,'sha256'),'hex')
    from invoice_ts it
    where it.attach_timesheet and it.manual_document_asset_id is not null
      and (it.submission_mode <> 'ELECTRONIC' or it.client_is_nhsp or it.no_timesheet_required)
    union all
    select ea.chunk_id,ea.document_version_id,2500+ea.evidence_no,
      'ASSET','TIMESHEET_EVIDENCE',coalesce(ea.evidence_id,ea.asset_id),
      ea.source_revision,ea.asset_id,null::uuid,ea.display_label,null::integer,'EVIDENCE_ASSET_POLICY',
      'ASSET_SOURCE_V1',encode(digest(coalesce(ea.evidence,'{}'::jsonb)::text,'sha256'),'hex')
    from evidence_assets ea where ea.asset_id is not null
    union all
    select ss.chunk_id,ss.document_version_id,3000+ss.source_no,
      case when ss.source_system='NHSP' then 'NHSP_SUPPORT' else 'HEALTHROSTER_SUPPORT' end,
      ss.source_system,ss.import_id,
      encode(digest(ss.support_source::text,'sha256'),'hex'),
      null::uuid,null::uuid,initcap(lower(ss.source_system))||' supporting report',null::integer,'FROZEN_SOURCE_SUPPORT',
      case when ss.source_system='NHSP' then 'NHSP_PRESENTATION_V1'
        else 'HEALTHROSTER_PRESENTATION_V2' end,
      encode(digest((ss.support_source->'render_model')::text,'sha256'),'hex')
    from support_sources ss where ss.import_id is not null
    union all
    select l.id,l.document_version_id,4000,
      'HIGHER_RATE_SUPPORT','INVOICE',l.entity_id,
      encode(digest(coalesce(l.snapshot_json_v5->'higher_rate_support','{}'::jsonb)::text,'sha256'),'hex'),
      null::uuid,null::uuid,'Higher-rate support',null::integer,'HIGHER_RATE_SUPPORT',
      'HIGHER_RATE_PRESENTATION_V1',encode(digest(coalesce(l.snapshot_json_v5->'higher_rate_support','{}'::jsonb)::text,'sha256'),'hex')
    from linked l
    where jsonb_array_length(case when jsonb_typeof(l.snapshot_json_v5#>'{higher_rate_support,rows}')='array' then l.snapshot_json_v5#>'{higher_rate_support,rows}' else '[]'::jsonb end)>0

  ),
  manifest_build as materialized (
    select m.chunk_id,m.document_version_id,
      jsonb_agg(jsonb_build_object(
        'ordinal',m.ordinal,'input_type',m.input_type,'render_kind',m.input_type,
        'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,
        'source_revision',m.source_revision,'document_asset_id',m.document_asset_id,
        'input_document_version_id',m.input_document_version_id,'display_label',m.display_label,
        'expected_page_count',m.expected_page_count,'inclusion_reason',m.inclusion_reason,
        'presentation_model_schema_version',m.presentation_schema,
        'presentation_model_hash',m.presentation_hash,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,m.source_revision,m.presentation_hash),'sha256'),'hex'))
        order by m.ordinal,m.source_entity_id) manifest_json,
      encode(digest(jsonb_agg(jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,'source_revision',m.source_revision,'presentation_model_hash',m.presentation_hash) order by m.ordinal,m.source_entity_id)::text,'sha256'),'hex') manifest_hash,
      sum(coalesce(m.expected_page_count,0))::integer expected_page_count
    from manifest_items m group by m.chunk_id,m.document_version_id
  ),
  update_manifests as materialized (
    update public.invoice_document_versions v
       set snapshot_json=l.snapshot_json_v5,
           snapshot_hash=l.snapshot_hash_v5,
           manifest_json=mb.manifest_json,
           manifest_hash=mb.manifest_hash,
           expected_page_count=mb.expected_page_count,
           status='WAITING_FOR_INPUTS',
           error_json=null
    from manifest_build mb
    join linked l
      on l.id=mb.chunk_id
     and l.version_id=mb.document_version_id
    where v.id=mb.document_version_id
    returning v.id
  ),
  core_chunks as materialized (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER' else 'SOURCE_RENDER' end,
      'RENDER',
      encode(digest(concat_ws('|',case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER' else 'SOURCE_RENDER' end,l.document_version_id::text,'0',l.snapshot_hash_v5,l.presentation_hash,coalesce(l.template_version,''),'2'),'sha256'),'hex'),
      0,l.entity_type,l.entity_id,l.document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,
        'template_version',coalesce(l.version_template_version,l.template_version),
        'source_revision',l.source_revision,
        'source_chunk_key',encode(digest(concat_ws('|',l.document_version_id::text,'0',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,l.entity_type,l.entity_id::text,l.source_revision,l.presentation_hash),'sha256'),'hex'),
        'presentation_model_schema_version',l.presentation_schema,
        'presentation_model_hash',l.presentation_hash,
        'snapshot_hash',l.snapshot_hash_v5),
      l.control_version,v_now,v_now
    from linked l
    left join direct_timesheet dt on dt.chunk_id=l.id
    where l.entity_type='INVOICE' or dt.submission_mode='ELECTRONIC'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do update set
      priority=greatest(public.invoice_operation_chunks.priority,excluded.priority),
      payload_json=excluded.payload_json,
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id
  ),
  source_chunks as materialized (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'SOURCE_RENDER','RENDER',
      encode(digest(concat_ws('|','SOURCE_RENDER',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_revision,m.presentation_hash,
        case when m.input_type='ELECTRONIC_TIMESHEET'
          then 'timesheet-professional-v2'
          else coalesce(l.version_template_version,l.template_version,'')
        end,'2'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,m.source_entity_id,l.document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',m.input_type,'source_revision',m.source_revision,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,m.source_revision,m.presentation_hash),'sha256'),'hex'),
        'template_version',case when m.input_type='ELECTRONIC_TIMESHEET'
          then 'timesheet-professional-v2'
          else coalesce(l.version_template_version,l.template_version)
        end,
        'presentation_model_schema_version',m.presentation_schema,
        'presentation_model_hash',m.presentation_hash,
        'snapshot_hash',l.snapshot_hash_v5),
      l.control_version,v_now,v_now
    from linked l join manifest_items m on m.chunk_id=l.id
    where m.ordinal>0 and m.input_type not in('ASSET','ATTACHMENT_INDEX')
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do update set
      priority=greatest(public.invoice_operation_chunks.priority,excluded.priority),
      payload_json=excluded.payload_json,
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id
  ),
  input_chunks as materialized (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,document_asset_id,status,priority,run_after_utc,payload_json,
      result_json,error_json,expected_page_count,actual_page_count,expected_byte_count,actual_byte_count,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'DOCUMENT_INPUT','DEPENDENCY',
      encode(digest(concat_ws('|','DOCUMENT_INPUT',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_revision,coalesce(m.document_asset_id::text,m.input_document_version_id::text,m.source_entity_id::text),m.presentation_hash,'2'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,m.source_entity_id,l.document_version_id,m.document_asset_id,
      case
        when m.input_type='ASSET' and a.status='READY' then 'COMPLETE'
        when m.input_type='ASSET' and m.document_asset_id is null then 'BLOCKED'
        when m.input_type='ASSET' and a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED') then 'BLOCKED'
        when m.input_type='ASSET' then 'WAITING'
        else 'WAITING' end,
      l.priority,v_now,
      jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,'display_label',m.display_label,
        'source_revision',m.source_revision,'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,
        'presentation_model_schema_version',m.presentation_schema,'presentation_model_hash',m.presentation_hash,'snapshot_hash',l.snapshot_hash_v5,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,m.source_revision,m.presentation_hash),'sha256'),'hex')),
      case when m.input_type='ASSET' and a.status='READY' then jsonb_build_object(
        'r2_key',a.normalised_r2_key,'parts',a.normalised_manifest_json,
        'sha256',a.normalised_sha256,'normalised_manifest_hash',a.normalised_manifest_hash,
        'size_bytes',a.normalised_size_bytes,'page_count',a.normalised_page_count,
        'source_revision',m.source_revision,'document_asset_id',a.id) end,
      case
        when m.input_type='ASSET' and m.document_asset_id is null then jsonb_build_object('code','ASSET_NOT_REGISTERED','source_entity_id',m.source_entity_id,'source_revision',m.source_revision)
        when m.input_type='ASSET' and a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED') then jsonb_build_object('code','ASSET_DEPENDENCY_PERMANENT_FAILURE','asset_status',a.status,'document_asset_id',a.id,'source_entity_id',m.source_entity_id)
      end,
      m.expected_page_count,
      case when m.input_type='ASSET' and a.status='READY' then a.normalised_page_count end,
      case when m.input_type='ASSET' and a.status='READY' then a.normalised_size_bytes end,
      case when m.input_type='ASSET' and a.status='READY' then a.normalised_size_bytes end,
      l.control_version,v_now,v_now
    from linked l
    join manifest_items m on m.chunk_id=l.id
    left join public.invoice_document_assets a on a.id=m.document_asset_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do update set
      status=case when excluded.status in('COMPLETE','BLOCKED') then excluded.status else public.invoice_operation_chunks.status end,
      payload_json=excluded.payload_json,
      result_json=coalesce(excluded.result_json,public.invoice_operation_chunks.result_json),
      error_json=coalesce(excluded.error_json,public.invoice_operation_chunks.error_json),
      actual_page_count=coalesce(excluded.actual_page_count,public.invoice_operation_chunks.actual_page_count),
      actual_byte_count=coalesce(excluded.actual_byte_count,public.invoice_operation_chunks.actual_byte_count),
      updated_at_utc=excluded.updated_at_utc
      where public.invoice_operation_chunks.status not in('SUPERSEDED','CANCELLED')
    returning id,operation_id,status
  ),
  advanced as materialized (
    update public.invoice_operation_chunks c
       set document_version_id=l.document_version_id,
           phase='WAIT_FOR_INPUTS',
           status=case when exists(
             select 1 from direct_timesheet dt
             where dt.chunk_id=l.id
               and dt.submission_mode in('MANUAL','QR')
               and dt.asset_status='READY')
             then 'QUEUED' else 'WAITING' end,
           progress_json=jsonb_build_object(
             'status_message',case when exists(
               select 1 from direct_timesheet dt
               where dt.chunk_id=l.id
                 and dt.submission_mode in('MANUAL','QR')
                 and dt.asset_status='READY')
               then 'Document inputs ready'
               else 'Waiting for document inputs' end,
             'manifest_items',(select jsonb_array_length(v.manifest_json)
               from public.invoice_document_versions v
               where v.id=l.document_version_id)),
           lease_owner=null,lease_token=null,lease_expires_at_utc=null,
           updated_at_utc=v_now
    from linked l
    where c.id=l.id
      and not exists(
        select 1 from blocked_direct_timesheet_source blocked_source
        where blocked_source.id=l.id)
    returning c.id,c.operation_id,c.document_version_id,c.status,c.phase
  ),
  rollup_ops as materialized (
    select private._invoice_operation_rollup_batch(array_agg(distinct operation_id),v_now) ignored
    from (
      select operation_id from advanced
      union select operation_id from blocked
      union select operation_id from blocked_direct_timesheet_source
      union select operation_id from core_chunks
      union select operation_id from source_chunks
      union select operation_id from input_chunks
    ) u
  ),
  all_results as materialized (
    select jsonb_build_object('chunk_id',b.id,'operation_id',b.operation_id,'status',b.status,'phase',b.phase,'error',b.error_json) result from blocked b
    union all
    select jsonb_build_object('chunk_id',b.id,'operation_id',b.operation_id,'status',b.status,'phase',b.phase,'error',b.error_json) result from blocked_direct_timesheet_source b
    union all
    select jsonb_build_object('chunk_id',p.id,'operation_id',p.operation_id,'status',p.status,'phase',p.phase,'document_version_id',p.document_version_id) result from advanced p
  )
  select coalesce(jsonb_agg(result),'[]'::jsonb) into v_manifest_results from all_results;

  return coalesce(v_manifest_results,'[]'::jsonb) || coalesce(v_passthrough_results,'[]'::jsonb);
end;
$function$;

