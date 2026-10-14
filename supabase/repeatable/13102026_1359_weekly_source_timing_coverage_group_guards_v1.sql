-- WP-F8: immutable input timing/coverage gate plus purpose-scoped saved facts.
-- Checking uploads remain admissible any day. These predicates do not publish
-- source, alter clocks, create an issue, or change an economic fingerprint.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_rule_coverage_floor_v1(p_client_id uuid,p_cycle_id uuid)
returns date language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_cycle public.weekly_source_cycles%rowtype; v_floor date;
begin
  select * into strict v_cycle from public.weekly_source_cycles where id=p_cycle_id;
  if private.weekly_source_effective_rule_set_v1(p_client_id,p_cycle_id) is null then return null; end if;
  -- F1's only feature writer refuses false->false. A RULE_SET purpose and
  -- complete receipt remain after disable; a never-enabled draft has neither.
  -- Refuse malformed activated history rather than advancing a client's floor.
  if exists(select 1 from public.weekly_source_group_rule_assignments a
    join public.weekly_source_group_clients gc on gc.source_group_id=a.source_group_id and gc.client_id=p_client_id
      and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]')
        && daterange(a.effective_from_week_ending,coalesce(a.effective_to_week_ending,'infinity'::date),'[]')
    where a.source_group_id=v_cycle.source_group_id and a.feature_purpose='RULE_SET'
      and a.effective_from_week_ending<=v_cycle.finalisation_week_ending and (
        a.rule_set_version_id is null or a.feature_release_id is null or a.feature_changed_at_utc is null
        or a.feature_changed_by_user_id is null or a.feature_request_id is null or a.feature_request_sha256 is null
        or a.feature_receipt_json->>'contract' is distinct from 'WEEKLY_SOURCE_RULE_FEATURE_RECEIPT_V1'
        or a.feature_receipt_json->>'assignment_id' is distinct from a.id::text
        or a.feature_receipt_json->>'source_group_id' is distinct from a.source_group_id::text
        or a.feature_receipt_json->>'rule_set_version_id' is distinct from a.rule_set_version_id::text
        or a.feature_receipt_json->>'effective_from_week_ending' is distinct from a.effective_from_week_ending::text
        or a.feature_receipt_json->>'release_id' is distinct from a.feature_release_id
        or a.feature_receipt_json->>'actor_user_id' is distinct from a.feature_changed_by_user_id::text
        or a.feature_receipt_json->>'request_id' is distinct from a.feature_request_id::text
        or (a.feature_receipt_json->>'changed_at_utc')::timestamptz is distinct from a.feature_changed_at_utc
        or a.feature_receipt_json->>'feature_purpose' is distinct from 'RULE_SET'
        or a.feature_receipt_json->'feature_enabled' is distinct from to_jsonb(a.feature_enabled)
      )) then
    raise exception 'WEEKLY_SOURCE_RULE_COVERAGE_HISTORY_UNPROVABLE' using errcode='55000';
  end if;
  with applicable_assignments as (
    select greatest(a.effective_from_week_ending+((7-extract(dow from a.effective_from_week_ending)::integer)%7),
      gc.valid_from+((7-extract(dow from gc.valid_from)::integer)%7)) first_week,
      least(coalesce(a.effective_to_week_ending,'infinity'::date),coalesce(gc.valid_to,'infinity'::date),
        v_cycle.finalisation_week_ending) last_week
    from public.weekly_source_group_rule_assignments a
    join public.weekly_source_group_clients gc on gc.source_group_id=a.source_group_id and gc.client_id=p_client_id
    join public.weekly_source_rule_set_versions version on version.id=a.rule_set_version_id
    join public.weekly_source_rule_sets rules on rules.id=version.rule_set_id
    join public.weekly_source_groups source_group on source_group.id=a.source_group_id
      and source_group.agency_id=rules.agency_id and source_group.environment=rules.environment
    where a.source_group_id=v_cycle.source_group_id and a.feature_purpose='RULE_SET'
      and a.feature_release_id is not null and a.feature_changed_at_utc is not null
  ), frozen_use as (
    select source_cycle.finalisation_week_ending first_week
    from public.weekly_source_final_revisions final
    join public.weekly_source_cycles source_cycle on source_cycle.id=final.source_cycle_id
      and source_cycle.source_group_id=v_cycle.source_group_id
    join public.weekly_source_group_clients gc on gc.source_group_id=source_cycle.source_group_id
      and gc.client_id=p_client_id and source_cycle.finalisation_week_ending
        between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    join public.weekly_source_rule_set_versions version on version.id=final.rule_set_version_id
    join public.weekly_source_rule_sets rules on rules.id=version.rule_set_id
    join public.weekly_source_groups source_group on source_group.id=source_cycle.source_group_id
      and source_group.agency_id=rules.agency_id and source_group.environment=rules.environment
    where source_cycle.finalisation_week_ending<=v_cycle.finalisation_week_ending
      and (source_cycle.scope_client_id=p_client_id or exists(
        select 1 from public.weekly_source_client_manifests manifest
        where manifest.final_revision_id=final.id and manifest.client_id=p_client_id)
        or exists(select 1 from public.weekly_source_report_scopes report
          where report.id=final.report_scope_id and report.source_cycle_id=source_cycle.id and report.client_id=p_client_id))
  ) select min(first_week)-6 into v_floor from (
    select first_week from applicable_assignments where first_week<=last_week
    union all select first_week from frozen_use
  ) history;
  if v_floor is null then raise exception 'WEEKLY_SOURCE_RULE_COVERAGE_HISTORY_UNPROVABLE' using errcode='55000'; end if;
  return v_floor;
exception when invalid_text_representation or invalid_datetime_format or datetime_field_overflow then
  raise exception 'WEEKLY_SOURCE_RULE_COVERAGE_HISTORY_UNPROVABLE' using errcode='55000';
end;
$function$;

create or replace function private.weekly_source_upload_coverage_context_v1(p_client_id uuid,p_cycle_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_cycle public.weekly_source_cycles%rowtype; v_rules jsonb; v_floor date;
  v_oldest date; v_completed date; v_has_final boolean;
begin
  select * into strict v_cycle from public.weekly_source_cycles where id=p_cycle_id;
  v_rules:=private.weekly_source_effective_rule_set_v1(p_client_id,p_cycle_id);
  if v_rules is null then return null; end if;
  v_floor:=private.weekly_source_rule_coverage_floor_v1(p_client_id,p_cycle_id);
  select min(event.work_date) into v_oldest
  from public.weekly_exceptional_pay_family_events event
  join public.weekly_exceptional_payment_approvals approval on approval.id=event.evidence_approval_id
    and approval.client_id=p_client_id and approval.withdrawn_at_utc is null
  join public.weekly_source_cycles origin on origin.id=approval.source_cycle_id
    and origin.source_group_id=v_cycle.source_group_id
  where event.state='WAIT' and event.work_date>=v_floor
    and event.event_sequence=(select max(latest.event_sequence)
      from public.weekly_exceptional_pay_family_events latest
      where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id);
  -- A completed Contract week has ended in UK local time; planned future weeks
  -- and cancelled weeks are not evidence of a completed period.
  select max(week.week_ending_date) into v_completed
  from public.contract_weeks week join public.contracts contract on contract.id=week.contract_id
  where contract.client_id=p_client_id and week.status::text<>'CANCELLED'
    and week.week_ending_date<(statement_timestamp() at time zone 'Europe/London')::date;
  select exists(select 1 from public.weekly_source_final_revisions final
    join public.weekly_source_cycles cycle on cycle.id=final.source_cycle_id
      and cycle.source_group_id=v_cycle.source_group_id
    join public.weekly_source_client_manifests manifest on manifest.final_revision_id=final.id
      and manifest.client_id=p_client_id
    where final.state='CURRENT' and final.authority_scope_kind='CYCLE') into v_has_final;
  return jsonb_build_object('contract','WEEKLY_SOURCE_UPLOAD_COVERAGE_CONTEXT_V1',
    'coverage_floor_local_date',v_floor,'oldest_protected_work_date',v_oldest,
    'last_completed_contract_week',v_completed,'has_current_final_source',v_has_final,
    'lookback_warning_enabled',v_rules#>>'{values,lookback_warning}'='WARN_IF_AFTER_OLDEST_PROTECTED');
end;
$function$;

create or replace function private.weekly_source_upload_coverage_floor_assert_v1(
  p_client_id uuid,p_cycle_id uuid,p_profile text,p_start date
) returns void language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_floor date;
begin
  -- Report files retain their own report scope. They never claim complete
  -- weekly export coverage. Only the three existing export profiles use this.
  if p_profile not in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
    'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1') then return; end if;
  v_floor:=private.weekly_source_rule_coverage_floor_v1(p_client_id,p_cycle_id);
  if v_floor is not null and (p_start is null or p_start<v_floor) then
    raise exception 'WEEKLY_SOURCE_COVERAGE_BEFORE_RULE_SET' using errcode='55000',
      detail='Import coverage starts from '||to_char(v_floor,'DD/MM/YYYY')||', the first week using these settings.';
  end if;
end;
$function$;

create or replace function private.weekly_source_final_upload_facts_v1(
  p_profile text,p_week_ending date,p_cutoff_at_utc timestamptz,p_uploaded_at_utc timestamptz,
  p_coverage_state text,p_coverage_start date,p_coverage_end date,p_rules jsonb,p_coverage_floor date
) returns jsonb language plpgsql immutable security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_code text; v_message text; v_earliest timestamptz;
  v_weekday integer; v_clock time; v_local timestamp; v_required_start date;
  v_days constant text[]:=array['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
begin
  -- Legacy/off/unassigned, NHSP and Mode A retain their existing owners.
  if p_rules is null or p_profile in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') then
    return jsonb_build_object('contract','WEEKLY_SOURCE_FINAL_UPLOAD_GATE_V1','allowed',true,
      'code',null,'message',null,'earliest_upload_at_utc',null,'coverage_floor_local_date',null);
  end if;
  if p_profile not in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
      'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1') or jsonb_typeof(p_rules) is distinct from 'object'
      or p_week_ending is null or p_cutoff_at_utc is null or p_uploaded_at_utc is null
      or p_coverage_floor is null or p_rules->>'checking_profile_kind' is distinct from p_profile then
    v_code:='WEEKLY_SOURCE_FINAL_UPLOAD_CONTEXT_INVALID';
    v_message:='The final file could not be checked. Refresh the import before trying again.';
  elsif p_rules->'earliest_final_weekday'='null'::jsonb
      and p_rules->'earliest_final_local_time'='null'::jsonb then
    v_earliest:=p_cutoff_at_utc;
  elsif jsonb_typeof(p_rules->'earliest_final_weekday')='number'
      and coalesce(p_rules->>'earliest_final_weekday','') ~ '^[0-6]$'
      and coalesce(p_rules->>'earliest_final_local_time','') ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
    v_weekday:=(p_rules->>'earliest_final_weekday')::integer;
    v_clock:=(p_rules->>'earliest_final_local_time')::time;
    -- A source cycle's Sunday is the completed work week. Its cutoff/S12
    -- occurrence is the following configured weekday, as the cycle owner does.
    v_earliest:=((p_week_ending+v_weekday)+v_clock) at time zone 'Europe/London';
    if v_earliest<p_cutoff_at_utc then
      v_code:='WEEKLY_SOURCE_FINAL_UPLOAD_CONTEXT_INVALID';
      v_message:='The final file could not be checked. Refresh the import before trying again.';
    end if;
  else
    v_code:='WEEKLY_SOURCE_FINAL_UPLOAD_CONTEXT_INVALID';
    v_message:='The final file could not be checked. Refresh the import before trying again.';
  end if;
  if v_code is null and p_uploaded_at_utc<v_earliest then
    v_local:=v_earliest at time zone 'Europe/London';
    v_code:='WEEKLY_SOURCE_FINAL_FILE_UPLOADED_TOO_EARLY';
    v_message:='The final file can be uploaded from '
      ||v_days[extract(dow from v_local)::integer+1]||' '||to_char(v_local,'HH24:MI')
      ||' ('||to_char(v_local,'DD/MM/YYYY')||').';
  end if;
  v_required_start:=greatest(p_week_ending-6,p_coverage_floor);
  if v_code is null and (p_coverage_state is distinct from 'COMPLETE' or p_coverage_start is null
      or p_coverage_end is null or p_coverage_start>p_coverage_end) then
    v_code:='WEEKLY_SOURCE_FINAL_FILE_COVERAGE_INCOMPLETE';
    v_message:='Upload an export covering the whole week.';
  elsif v_code is null and p_coverage_start<p_coverage_floor then
    v_code:='WEEKLY_SOURCE_COVERAGE_BEFORE_RULE_SET';
    v_message:='Import coverage starts from '||to_char(p_coverage_floor,'DD/MM/YYYY')
      ||', the first week using these settings.';
  elsif v_code is null and (p_coverage_start>v_required_start or p_coverage_end<p_week_ending) then
    v_code:='WEEKLY_SOURCE_FINAL_FILE_COVERAGE_INCOMPLETE';
    v_message:='The current file covers '||to_char(p_coverage_start,'DD/MM/YYYY')||'–'
      ||to_char(p_coverage_end,'DD/MM/YYYY')||' only. Upload an export covering the whole week.';
  end if;
  return jsonb_build_object('contract','WEEKLY_SOURCE_FINAL_UPLOAD_GATE_V1','allowed',v_code is null,
    'code',v_code,'message',v_message,'earliest_upload_at_utc',v_earliest,
    'coverage_floor_local_date',p_coverage_floor);
end;
$function$;

create or replace function private.weekly_source_final_upload_gate_v1(p_upload_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_upload public.weekly_source_uploads%rowtype; v_cycle public.weekly_source_cycles%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype; v_rules jsonb; v_floor date; v_client uuid;
begin
  select * into strict v_upload from public.weekly_source_uploads where id=p_upload_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_upload.source_cycle_id;
  select * into strict v_profile from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  v_client:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
  v_rules:=private.weekly_source_effective_rule_set_v1(v_client,v_cycle.id);
  if v_rules is not null then
    -- Retained activation receipts and frozen revisions prove this client's
    -- first applicable rule week, including historical disabled assignments.
    v_floor:=private.weekly_source_rule_coverage_floor_v1(v_client,v_cycle.id);
  end if;
  return private.weekly_source_final_upload_facts_v1(v_profile.profile_code,v_cycle.finalisation_week_ending,
    v_cycle.cutoff_at_utc,v_upload.uploaded_at_utc,v_upload.coverage_state,
    v_upload.confirmed_coverage_start_local_date,v_upload.confirmed_coverage_end_local_date,v_rules->'values',v_floor);
end;
$function$;

create or replace function private.weekly_source_final_upload_assert_v1(p_upload_id uuid)
returns void language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_gate jsonb;
begin
  v_gate:=private.weekly_source_final_upload_gate_v1(p_upload_id);
  if v_gate->'allowed' is distinct from 'true'::jsonb then
    raise exception '%',v_gate->>'code' using errcode='55000',detail=v_gate->>'message';
  end if;
end;
$function$;

alter function private.weekly_source_final_upload_facts_v1(text,date,timestamptz,timestamptz,text,date,date,jsonb,date) owner to postgres;
alter function private.weekly_source_final_upload_gate_v1(uuid) owner to postgres;
alter function private.weekly_source_final_upload_assert_v1(uuid) owner to postgres;
alter function private.weekly_source_rule_coverage_floor_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_upload_coverage_context_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_upload_coverage_floor_assert_v1(uuid,uuid,text,date) owner to postgres;
revoke all on function private.weekly_source_final_upload_facts_v1(text,date,timestamptz,timestamptz,text,date,date,jsonb,date),
  private.weekly_source_final_upload_gate_v1(uuid),private.weekly_source_final_upload_assert_v1(uuid),
  private.weekly_source_rule_coverage_floor_v1(uuid,uuid),private.weekly_source_upload_coverage_context_v1(uuid,uuid),
  private.weekly_source_upload_coverage_floor_assert_v1(uuid,uuid,text,date)
  from public,anon,authenticated,service_role;

commit;
