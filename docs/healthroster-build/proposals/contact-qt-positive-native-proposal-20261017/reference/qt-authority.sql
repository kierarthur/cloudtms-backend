-- Complete V5 QT administration and immutable agency authority. Dispatch/START
-- consumers are separately required before activation; no lazy bootstrap here.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_contact_assert_closed_v1(
  p_value jsonb,p_allowed text[],p_required text[],p_error text
) returns void
language plpgsql immutable security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_value is null or pg_catalog.jsonb_typeof(p_value)<>'object'
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_value) k where not(k=any(p_allowed)))
    or exists(select 1 from pg_catalog.unnest(p_required) k where not(p_value ? k)) then
    raise exception '%',p_error using errcode='22023';
  end if;
end;
$function$;

create or replace function private.weekly_contact_revision_v1(p_value jsonb,p_positive boolean)
returns bigint
language plpgsql immutable security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_text text:=p_value#>>'{}';
begin
  if p_value is null or pg_catalog.jsonb_typeof(p_value)<>'string'
    or v_text !~ '^(0|[1-9][0-9]*)$' or pg_catalog.length(v_text)>19 then
    raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
  end if;
  if v_text::numeric>9223372036854775807 or (p_positive and v_text='0') then
    raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
  end if;
  return v_text::bigint;
end;
$function$;

create or replace function private.candidate_push_timing_policy_validate_v1(p_policy jsonb)
returns void
language plpgsql immutable security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_days text[]:=array['monday','tuesday','wednesday','thursday','friday','saturday','sunday'];
  v_key text; v_rule jsonb; v_mode text; v_viable boolean:=false;
begin
  perform private.weekly_contact_assert_closed_v1(p_policy,
    array['version','enabled','timezone','normal_quiet_start','normal_quiet_end','day_rules','bank_holiday_rule'],
    array['version','enabled','timezone','normal_quiet_start','normal_quiet_end','day_rules','bank_holiday_rule'],
    'PUSH_TIMING_REQUEST_INVALID');
  if p_policy->>'version' is distinct from 'PUSH_TIMING_POLICY_V1'
    or pg_catalog.jsonb_typeof(p_policy->'enabled') is distinct from 'boolean'
    or p_policy->>'timezone' is distinct from 'Europe/London'
    or pg_catalog.jsonb_typeof(p_policy->'normal_quiet_start') is distinct from 'string'
    or pg_catalog.jsonb_typeof(p_policy->'normal_quiet_end') is distinct from 'string'
    or (p_policy->>'normal_quiet_start') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
    or (p_policy->>'normal_quiet_end') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
    or ((p_policy->>'enabled')::boolean and p_policy->>'normal_quiet_start'=p_policy->>'normal_quiet_end') then
    raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_contact_assert_closed_v1(p_policy->'day_rules',v_days,v_days,'PUSH_TIMING_REQUEST_INVALID');
  foreach v_key in array v_days||array['bank_holiday_rule'] loop
    v_rule:=case when v_key='bank_holiday_rule' then p_policy->v_key else p_policy->'day_rules'->v_key end;
    v_mode:=v_rule->>'mode';
    if v_mode in ('INHERIT','NO_PUSH_DAY') then
      perform private.weekly_contact_assert_closed_v1(v_rule,array['mode'],array['mode'],'PUSH_TIMING_REQUEST_INVALID');
    elsif v_mode='ALLOW_WINDOW' then
      perform private.weekly_contact_assert_closed_v1(v_rule,array['mode','start','end'],array['mode','start','end'],'PUSH_TIMING_REQUEST_INVALID');
      if pg_catalog.jsonb_typeof(v_rule->'start') is distinct from 'string'
        or pg_catalog.jsonb_typeof(v_rule->'end') is distinct from 'string'
        or (v_rule->>'start') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
        or (v_rule->>'end') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
        or v_rule->>'start'>=v_rule->>'end' then
        raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
      end if;
    else
      raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
    end if;
    if v_key<>'bank_holiday_rule' and v_mode<>'NO_PUSH_DAY' then v_viable:=true; end if;
  end loop;
  if not v_viable then raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023'; end if;
end;
$function$;

create or replace function private.weekly_contact_deployment_lock_v1(p_agency_id uuid,p_environment text)
returns void
language plpgsql volatile security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_agency_id is null or p_environment not in ('TEST','LIVE') or p_environment is null then
    raise exception 'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' using errcode='P0001';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_CONTACT_DEPLOYMENT_V1|'||p_environment||'|'||p_agency_id::text,0));
end;
$function$;

create or replace function private.candidate_push_timing_immutable_guard_v1()
returns trigger
language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if TG_TABLE_NAME='candidate_push_timing_commands' or TG_OP='DELETE' then
    raise exception 'PUSH_TIMING_IMMUTABLE_AUTHORITY' using errcode='23514';
  end if;
  if TG_OP='UPDATE' and (
    new.environment is distinct from old.environment or new.agency_id is distinct from old.agency_id
    or new.deployment_slot is distinct from old.deployment_slot
    or new.revision is distinct from old.revision+case when new.policy_json is distinct from old.policy_json then 1 else 0 end
  ) then raise exception 'PUSH_TIMING_IMMUTABLE_AUTHORITY' using errcode='23514'; end if;
  if TG_OP='INSERT' and new.revision<>1 then
    raise exception 'PUSH_TIMING_IMMUTABLE_AUTHORITY' using errcode='23514';
  end if;
  perform private.candidate_push_timing_policy_validate_v1(new.policy_json);
  if new.policy_sha256 is distinct from private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',new.policy_json) then
    raise exception 'PUSH_TIMING_IMMUTABLE_AUTHORITY' using errcode='23514';
  end if;
  return new;
end;
$function$;

drop trigger if exists candidate_push_timing_immutable_guard on public.candidate_push_timing_policy;
create trigger candidate_push_timing_immutable_guard before insert or update or delete
  on public.candidate_push_timing_policy for each row execute function private.candidate_push_timing_immutable_guard_v1();
drop trigger if exists candidate_push_timing_commands_immutable_guard on public.candidate_push_timing_commands;
create trigger candidate_push_timing_commands_immutable_guard before update or delete
  on public.candidate_push_timing_commands for each row execute function private.candidate_push_timing_immutable_guard_v1();

create or replace function public.candidate_push_timing_get_v1(p_request jsonb)
returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_actor uuid; v_agency uuid; v_policy public.candidate_push_timing_policy%rowtype; v_key text;
begin
  perform private.weekly_contact_assert_closed_v1(p_request,
    array['version','actor_user_id','agency_id','environment'],
    array['version','actor_user_id','agency_id','environment'],'PUSH_TIMING_REQUEST_INVALID');
  if p_request->>'version' is distinct from 'PUSH_TIMING_GET_V1' then
    raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023'; end if;
  foreach v_key in array array['actor_user_id','agency_id'] loop
    if pg_catalog.jsonb_typeof(p_request->v_key) is distinct from 'string'
      or (p_request->>v_key) !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023'; end if;
  end loop;
  v_actor:=(p_request->>'actor_user_id')::uuid; v_agency:=(p_request->>'agency_id')::uuid;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  select * into v_policy from public.candidate_push_timing_policy
    where environment=p_request->>'environment' and agency_id=v_agency;
  if not found then raise exception 'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' using errcode='P0001'; end if;
  perform private.candidate_push_timing_policy_validate_v1(v_policy.policy_json);
  return pg_catalog.jsonb_build_object('ok',true,'version','PUSH_TIMING_STATE_V1',
    'revision',v_policy.revision::text,'policy',v_policy.policy_json,
    'policy_sha256',pg_catalog.encode(v_policy.policy_sha256,'hex'),
    'updated_at_utc',pg_catalog.to_char(v_policy.updated_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'can_edit',true);
end;
$function$;

create or replace function public.candidate_push_timing_save_v1(p_request jsonb)
returns jsonb
language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_agency uuid; v_request uuid; v_expected bigint; v_key text;
  v_policy public.candidate_push_timing_policy%rowtype;
  v_saved public.candidate_push_timing_commands%rowtype;
  v_digest bytea; v_reply jsonb; v_now timestamptz;
begin
  perform private.weekly_contact_assert_closed_v1(p_request,
    array['version','request_id','expected_revision','policy','actor_user_id','agency_id','environment'],
    array['version','request_id','expected_revision','policy','actor_user_id','agency_id','environment'],
    'PUSH_TIMING_REQUEST_INVALID');
  if p_request->>'version' is distinct from 'PUSH_TIMING_SAVE_V1' then
    raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023'; end if;
  foreach v_key in array array['actor_user_id','agency_id','request_id'] loop
    if pg_catalog.jsonb_typeof(p_request->v_key) is distinct from 'string'
      or (p_request->>v_key) !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023'; end if;
  end loop;
  v_actor:=(p_request->>'actor_user_id')::uuid; v_agency:=(p_request->>'agency_id')::uuid;
  v_request:=(p_request->>'request_id')::uuid;
  v_expected:=private.weekly_contact_revision_v1(p_request->'expected_revision',true);
  perform private.candidate_push_timing_policy_validate_v1(p_request->'policy');
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  perform private.weekly_contact_deployment_lock_v1(v_agency,p_request->>'environment');
  -- Fresh authority after any wait, before replay or policy disclosure.
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  select * into v_policy from public.candidate_push_timing_policy
    where environment=p_request->>'environment' and agency_id=v_agency for update;
  if not found then raise exception 'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' using errcode='P0001'; end if;
  v_digest:=private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_SAVE_REQUEST_V1',p_request);
  select * into v_saved from public.candidate_push_timing_commands
    where environment=v_policy.environment and agency_id=v_agency and request_id=v_request for update;
  if found then
    if v_saved.actor_user_id is distinct from v_actor or v_saved.request_digest is distinct from v_digest then
      raise exception 'PUSH_TIMING_IDEMPOTENCY_CONFLICT' using errcode='P0001'; end if;
    return v_saved.response_json;
  end if;
  if v_policy.revision<>v_expected then
    raise exception 'PUSH_TIMING_REVISION_CONFLICT' using errcode='P0001'; end if;
  v_now:=pg_catalog.clock_timestamp();
  if v_policy.policy_json is distinct from p_request->'policy' then
    if v_policy.revision=9223372036854775807 then
      raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023'; end if;
    update public.candidate_push_timing_policy
      set policy_json=p_request->'policy',
          policy_sha256=private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',p_request->'policy'),
          revision=revision+1,updated_by_user_id=v_actor,updated_at_utc=v_now
      where environment=v_policy.environment and agency_id=v_agency returning * into v_policy;
  end if;
  v_reply:=pg_catalog.jsonb_build_object('ok',true,'version','PUSH_TIMING_SAVED_V1','request_id',v_request,
    'revision',v_policy.revision::text,'policy',v_policy.policy_json,
    'policy_sha256',pg_catalog.encode(v_policy.policy_sha256,'hex'),
    'updated_at_utc',pg_catalog.to_char(v_policy.updated_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
  insert into public.candidate_push_timing_commands(environment,agency_id,request_id,actor_user_id,
    request_digest,expected_revision,accepted_revision,response_json,created_at_utc,completed_at_utc)
    values(v_policy.environment,v_agency,v_request,v_actor,v_digest,v_expected,v_policy.revision,v_reply,v_now,v_now);
  return v_reply;
end;
$function$;

alter function private.weekly_contact_assert_closed_v1(jsonb,text[],text[],text) owner to postgres;
alter function private.weekly_contact_revision_v1(jsonb,boolean) owner to postgres;
alter function private.candidate_push_timing_policy_validate_v1(jsonb) owner to postgres;
alter function private.weekly_contact_deployment_lock_v1(uuid,text) owner to postgres;
alter function private.candidate_push_timing_immutable_guard_v1() owner to postgres;
alter function public.candidate_push_timing_get_v1(jsonb) owner to postgres;
alter function public.candidate_push_timing_save_v1(jsonb) owner to postgres;
revoke all on function private.weekly_contact_assert_closed_v1(jsonb,text[],text[],text),
  private.weekly_contact_revision_v1(jsonb,boolean),private.candidate_push_timing_policy_validate_v1(jsonb),
  private.weekly_contact_deployment_lock_v1(uuid,text),private.candidate_push_timing_immutable_guard_v1()
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_push_timing_get_v1(jsonb),public.candidate_push_timing_save_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.candidate_push_timing_get_v1(jsonb),public.candidate_push_timing_save_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
