-- Existing agency holiday authority and bounded London timing only.
-- These readers grant no provider permission and never create a policy row.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_contact_london_boundary_v1(
 p_date date,p_time time without time zone,p_later boolean
) returns timestamptz
language plpgsql stable security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_local timestamp;v_guess timestamptz;v_result timestamptz;
begin
 if p_date is null or p_time is null or p_later is null
  or extract(second from p_time)<>0 or extract(hour from p_time)>23 then
  raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
 end if;
 v_local:=p_date+p_time;
 v_guess:=v_local at time zone 'Europe/London';
 -- PG's default repeated-time choice is the later occurrence. Explicitly
 -- inspect the adjacent real instants so start/end use the required side.
 select case when p_later then max(x.instant) else min(x.instant) end
 into v_result
 from (values(v_guess-interval '2 hours'),(v_guess-interval '1 hour'),
   (v_guess),(v_guess+interval '1 hour'),(v_guess+interval '2 hours')) x(instant)
 where x.instant at time zone 'Europe/London'=v_local;
 if v_result is not null then return v_result;end if;
 -- A spring-forward missing minute resolves to the first real instant
 -- after the gap, rather than shifting the missing minute by an hour.
 select min(x.instant) into v_result
 from pg_catalog.generate_series(v_guess-interval '3 hours',v_guess+interval '3 hours',interval '1 minute') x(instant)
 where x.instant at time zone 'Europe/London'>=v_local
  and (x.instant at time zone 'Europe/London')::date=p_date;
 if v_result is null then
  raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';
 end if;
 return v_result;
end;
$function$;

create or replace function private.weekly_contact_push_allowed_windows_v1(
 p_policy jsonb,p_date date,p_is_holiday boolean
) returns table(window_start timestamptz,window_end timestamptz)
language plpgsql stable security invoker
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_rule jsonb;v_day text;v_start time;v_end time;v_midnight timestamptz;v_tomorrow timestamptz;
begin
 perform private.candidate_push_timing_policy_validate_v1(p_policy);
 if p_date is null then raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end if;
 v_midnight:=private.weekly_contact_london_boundary_v1(p_date,'00:00'::time,false);
 v_tomorrow:=private.weekly_contact_london_boundary_v1(p_date+1,'00:00'::time,false);
 if not(p_policy->>'enabled')::boolean then
  return query select v_midnight,v_tomorrow;return;
 end if;
 v_day:=(array['monday','tuesday','wednesday','thursday','friday','saturday','sunday'])[extract(isodow from p_date)::integer];
 if p_policy->'bank_holiday_rule'->>'mode'<>'INHERIT' then
  if p_is_holiday is null then raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
  if p_is_holiday then v_rule:=p_policy->'bank_holiday_rule';end if;
 end if;
 v_rule:=coalesce(v_rule,p_policy->'day_rules'->v_day);
 if v_rule->>'mode'='NO_PUSH_DAY' then return;end if;
 if v_rule->>'mode'='ALLOW_WINDOW' then
  window_start:=private.weekly_contact_london_boundary_v1(p_date,(v_rule->>'start')::time,false);
  window_end:=private.weekly_contact_london_boundary_v1(p_date,(v_rule->>'end')::time,true);
  if window_start<window_end then return next;end if;return;
 end if;
 v_start:=(p_policy->>'normal_quiet_start')::time;
 v_end:=(p_policy->>'normal_quiet_end')::time;
 if v_start<v_end then
  window_start:=v_midnight;
  window_end:=private.weekly_contact_london_boundary_v1(p_date,v_start,false);
  if window_start<window_end then return next;end if;
  window_start:=private.weekly_contact_london_boundary_v1(p_date,v_end,true);
  window_end:=v_tomorrow;
  if window_start<window_end then return next;end if;
 else
  window_start:=private.weekly_contact_london_boundary_v1(p_date,v_end,true);
  window_end:=private.weekly_contact_london_boundary_v1(p_date,v_start,false);
  if window_start<window_end then return next;end if;
 end if;
end;
$function$;

create or replace function private.weekly_contact_push_timing_decision_v1(
 p_policy jsonb,p_at_utc timestamptz,p_expires_at_utc timestamptz,p_calendar jsonb
) returns jsonb
language plpgsql stable security invoker
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_date date;v_offset integer;v_holiday boolean;v_next timestamptz;v_window record;v_candidate timestamptz;
begin
 perform private.candidate_push_timing_policy_validate_v1(p_policy);
 if p_at_utc is null or p_expires_at_utc is null then
  raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
 end if;
 if p_at_utc>=p_expires_at_utc then
  return jsonb_build_object('allowed_now',false,'reason','EXPIRED_TIMING','next_allowed_utc',null,
   'expires_at_utc',to_char(p_expires_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
 end if;
 for v_offset in 0..27 loop
  v_date:=(p_at_utc at time zone 'Europe/London')::date+v_offset;
  exit when private.weekly_contact_london_boundary_v1(v_date,'00:00'::time,false)>=p_expires_at_utc;
  v_holiday:=null;
  -- Disabled QT and INHERIT holiday policy do not depend on calendar I/O.
  if (p_policy->>'enabled')::boolean and p_policy->'bank_holiday_rule'->>'mode'<>'INHERIT' then
   if p_calendar is null or jsonb_typeof(p_calendar)<>'object'
    or not(p_calendar ? v_date::text) or jsonb_typeof(p_calendar->v_date::text)<>'boolean' then
    raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';
   end if;
   v_holiday:=(p_calendar->>v_date::text)::boolean;
  end if;
  for v_window in select * from private.weekly_contact_push_allowed_windows_v1(p_policy,v_date,v_holiday) loop
   v_candidate:=greatest(p_at_utc,v_window.window_start);
   if v_candidate<least(v_window.window_end,p_expires_at_utc)
    and (v_next is null or v_candidate<v_next) then v_next:=v_candidate;end if;
  end loop;
  exit when v_next is not null;
 end loop;
 return jsonb_build_object('allowed_now',v_next=p_at_utc and v_next is not null,
  'reason',case when v_next is null then 'EXPIRED_TIMING' when v_next=p_at_utc then 'ALLOWED' else 'QUIET' end,
  'next_allowed_utc',case when v_next is null then null else to_char(v_next at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') end,
  'expires_at_utc',to_char(p_expires_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
end;
$function$;

create or replace function public.weekly_source_push_calendar_decision_v1(p_request jsonb)
returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare v_keys text[]:=array['version','candidate_id','client_id','local_dates','agency_id','environment'];
 v_candidate uuid;v_client uuid;v_agency uuid;v_environment text;v_dates date[];v_value jsonb;v_date date;
 v_defaults public.settings_defaults%rowtype;v_holidays jsonb;v_effective jsonb;v_rows jsonb;v_hash text;
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_assert_closed_v1(p_request,v_keys,v_keys,'PUSH_TIMING_REQUEST_INVALID');
 if p_request->>'version' is distinct from 'PUSH_CALENDAR_DECISION_V1'
  or jsonb_typeof(p_request->'candidate_id') is distinct from 'string'
  or p_request->>'candidate_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  or jsonb_typeof(p_request->'agency_id') is distinct from 'string'
  or p_request->>'agency_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  or (p_request->'client_id'<>'null'::jsonb and (jsonb_typeof(p_request->'client_id') is distinct from 'string'
   or p_request->>'client_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'))
  or p_request->>'environment' is null or p_request->>'environment' not in ('TEST','LIVE')
  or jsonb_typeof(p_request->'local_dates') is distinct from 'array' then
  raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
 end if;
 if jsonb_array_length(p_request->'local_dates') not between 1 and 28 then
  raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
 end if;
 v_candidate:=(p_request->>'candidate_id')::uuid;v_client:=(p_request->>'client_id')::uuid;
 v_agency:=(p_request->>'agency_id')::uuid;v_environment:=p_request->>'environment';
 if not exists(select 1 from public.candidate_push_timing_policy p where p.environment=v_environment and p.agency_id=v_agency)
 then raise exception 'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' using errcode='P0001';end if;
 if not exists(select 1 from public.candidates c where c.id=v_candidate)
  or (v_client is not null and not exists(select 1 from public.clients c where c.id=v_client)) then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';
 end if;
 for v_value in select value from jsonb_array_elements(p_request->'local_dates') loop
  if jsonb_typeof(v_value)<>'string' or v_value#>>'{}' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then
   raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';
  end if;
  begin v_date:=(v_value#>>'{}')::date;
  exception when datetime_field_overflow or invalid_datetime_format then
   raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end;
  if v_date=any(coalesce(v_dates,array[]::date[])) then
   raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end if;
  v_dates:=array_append(v_dates,v_date);
 end loop;
 select * into v_defaults from public.settings_defaults where id=1;
 if not found or jsonb_typeof(v_defaults.bh_list) is distinct from 'array'
  or jsonb_typeof(v_defaults.bh_feed_list) is distinct from 'array' then
  raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
 -- This is the same normalized distinct union as the effective owner. The
 -- null-client path reads the actual agency defaults without a fake Contract.
 select coalesce(jsonb_agg(to_jsonb(h.date_value) order by h.date_value),'[]'::jsonb) into v_holidays
 from(select distinct btrim(x.date_value) date_value from(
  select jsonb_array_elements_text(v_defaults.bh_list) date_value
  union all select jsonb_array_elements_text(v_defaults.bh_feed_list) date_value
 )x where btrim(x.date_value)~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')h;
 if v_client is not null then
  foreach v_date in array v_dates loop
   begin v_effective:=private._contract_settings_effective_core_v1(v_client,null,v_date,'OFFICE',null);
   exception when others then raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end;
   if v_effective->'values'->'bh_list' is distinct from v_holidays then
    raise exception 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' using errcode='P0001';end if;
  end loop;
 end if;
 v_hash:=encode(private.weekly_source_sha256_jsonb_v1('PUSH_CONFIGURED_CALENDAR_V1',v_holidays),'hex');
 select jsonb_agg(jsonb_build_object('local_date',d::text,'is_holiday',v_holidays ? d::text) order by d)
 into v_rows from unnest(v_dates)d;
 return jsonb_build_object('timezone','Europe/London','dates',v_rows,'calendar_digest',v_hash);
end;
$function$;

alter function private.weekly_contact_london_boundary_v1(date,time without time zone,boolean) owner to postgres;
alter function private.weekly_contact_push_allowed_windows_v1(jsonb,date,boolean) owner to postgres;
alter function private.weekly_contact_push_timing_decision_v1(jsonb,timestamptz,timestamptz,jsonb) owner to postgres;
alter function public.weekly_source_push_calendar_decision_v1(jsonb) owner to postgres;
revoke all on function private.weekly_contact_london_boundary_v1(date,time without time zone,boolean),
 private.weekly_contact_push_allowed_windows_v1(jsonb,date,boolean),
 private.weekly_contact_push_timing_decision_v1(jsonb,timestamptz,timestamptz,jsonb),
 public.weekly_source_push_calendar_decision_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_push_calendar_decision_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
