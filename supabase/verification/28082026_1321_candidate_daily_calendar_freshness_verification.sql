-- Real first-use proof, synthetic rows only; every change rolls back.
\set ON_ERROR_STOP on
begin;
do $verification$
declare
  v_candidate uuid:='00000000-0000-4000-8000-00000000e960';
  v_source text:=repeat('7',64);
  v_today date:=(clock_timestamp() at time zone 'Europe/London')::date;
  v_date date; v_midnight timestamptz; v_next timestamptz;
  v_days jsonb; v_result jsonb; v_generation uuid; v_original_date date;
  v_candidate_context jsonb; v_before bigint;
  v_context jsonb:=jsonb_build_object('policy','SIGNED_SYSTEM_SYNC','environment','TEST',
    'system_auth_verified',true,'nonce_consumed',true,'environment_trusted',true,
    'stable_operation_identity',true,'approved_source_mapping',true,'source_scope_ready',true,
    'authority_mode_compatible',true,'transition_ready',true);
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
    values(v_candidate,'rota-calendar-proof@example.invalid','Rota Calendar Proof','Rota','Calendar Proof',true,
      'cid1-960abcdefghjkmnpqrs');
  select jsonb_agg(jsonb_build_object('date',(v_today+n)::text,'booked',false,
    'system_blocked',false,'source_row_hash',lpad(to_hex(n+1),64,'0')) order by n)
    into v_days from generate_series(0,13)n;
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_context,
    '00000000-0000-4000-8000-00000000e961','rota-calendar-proof-publish-0001',jsonb_build_array(
      jsonb_build_object('candidate_global_key','CID1-960ABCDEFGHJKMNPQRS','candidate_source_hmac',v_source,
        'source_hmac_key_version',1,'source_event_id','rota-calendar-proof-first-0001',
        'source_revision','rota-calendar-proof-1','source_hash',repeat('c',64),
        'window_start',v_today::text,'days',v_days,'source_event_time',clock_timestamp()::text,
        'item_key','rota-calendar-item-first-0001')),'01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result#>>'{outcomes,0,status}' is distinct from 'COMMITTED' then
    raise exception 'ROTA_CALENDAR_PROOF: publication fixture failed';
  end if;
  v_generation:=(v_result#>>'{outcomes,0,generation_id}')::uuid;
  insert into private.candidate_daily_sync_state(environment,candidate_id,target,state)
    values('TEST',v_candidate,'MASTER_AVAILABILITY_SHEET','READY')
    on conflict(environment,candidate_id,target) do nothing;
  update public.settings_defaults set candidate_app_feature_flags_json=
    candidate_app_feature_flags_json||'{"candidate_daily_enabled":true}'::jsonb where id=1;
  insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
    values('TEST',v_candidate,true,'Synthetic calendar proof',repeat('d',64))
    on conflict(environment,candidate_id) do update set enabled=true;
  update private.candidate_daily_authority_scopes set authority_mode='SUPABASE_PRIMARY'
    where environment='TEST' and candidate_id=v_candidate;

  -- Today, the 23-hour spring day and the 25-hour autumn day all use UK midnight.
  v_original_date:=v_today;
  foreach v_date in array array[v_today,date '2027-03-28',date '2027-10-31'] loop
    v_midnight:=v_date::timestamp at time zone 'Europe/London';
    v_next:=(v_date+1)::timestamp at time zone 'Europe/London';
    update public.candidate_daily_rota_days set rota_date=rota_date+(v_date-v_original_date)
      where generation_id=v_generation;
    update public.candidate_daily_rota_generations set window_start=v_date,window_end=v_date+13,
      published_at_utc=v_midnight where generation_id=v_generation;
    v_original_date:=v_date;
    v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,v_midnight+interval '5 hours');
    if v_result->>'ready' is distinct from 'true' or (v_result->>'generation_age_seconds')::integer<>18000
       or (v_result->>'generation_max_age_seconds')::integer<>extract(epoch from v_next-v_midnight)::integer
       or v_result->>'projection_warning_seconds'<>'120' then
      raise exception 'ROTA_CALENDAR_PROOF: unchanged current window incorrectly expired or age was falsified';
    end if;
    v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,v_next-interval '1 second');
    if v_result->>'ready' is distinct from 'true' then
      raise exception 'ROTA_CALENDAR_PROOF: current window expired before London midnight';
    end if;
    v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,v_next);
    if v_result->>'ready' is distinct from 'true'
       or (v_result->>'generation_age_seconds')::integer<>extract(epoch from v_next-v_midnight)::integer then
      raise exception 'ROTA_CALENDAR_PROOF: last complete window disappeared at midnight';
    end if;
    v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,v_next+interval '20 days');
    if v_result->>'ready' is distinct from 'true' then
      raise exception 'ROTA_CALENDAR_PROOF: elapsed time hid the last complete window';
    end if;
    v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,v_midnight-interval '1 second');
    if v_result->>'ready' is distinct from 'false' then
      raise exception 'ROTA_CALENDAR_PROOF: future window was accepted';
    end if;
  end loop;
  update public.candidate_daily_rota_days set rota_date=rota_date+(v_today-v_original_date)
    where generation_id=v_generation;
  update public.candidate_daily_rota_generations set window_start=v_today,window_end=v_today+13,
    published_at_utc=(v_today::timestamp at time zone 'Europe/London') where generation_id=v_generation;
  v_result:=private._candidate_daily_capability_v1('TEST',v_candidate,clock_timestamp());
  if v_result->>'enabled' is distinct from 'true' then
    raise exception 'ROTA_CALENDAR_PROOF: entitled current Rota was hidden';
  end if;
  -- Real Candidate read/write first use after midnight with the old complete window.
  for v_date in select rota_date from public.candidate_daily_rota_days
    where generation_id=v_generation order by rota_date loop
    update public.candidate_daily_rota_days set rota_date=v_date-1
      where generation_id=v_generation and rota_date=v_date;
  end loop;
  update public.candidate_daily_rota_generations set window_start=v_today-1,window_end=v_today+12,
    published_at_utc=((v_today-1)::timestamp at time zone 'Europe/London') where generation_id=v_generation;
  v_candidate_context:=jsonb_build_object('policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate);
  v_result:=public.candidate_daily_tiles_get_v1(v_candidate_context,v_today,14);
  if v_result->>'window_start' is distinct from (v_today-1)::text
     or v_result->>'window_end' is distinct from (v_today+12)::text
     or jsonb_array_length(v_result->'tiles')<>14
     or v_result#>>'{tiles,0,editable}' is distinct from 'false'
     or v_result#>>'{tiles,1,editable}' is distinct from 'true'
     or exists(select 1 from jsonb_array_elements(v_result->'tiles')t where (t->>'date')::date=v_today+13) then
    raise exception 'ROTA_CALENDAR_PROOF: retained dates relabelled, past editable or unpublished day invented';
  end if;
  v_before:=(v_result->>'availability_version')::bigint;
  v_result:=public.candidate_daily_availability_apply_atomic_v1(v_candidate_context,
    'rota-calendar-proof-past-0001',v_before,
    jsonb_build_array(jsonb_build_object('date',v_today-1,'availability','LONG_DAY')),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result->>'error_code' is distinct from 'AVAILABILITY_DATE_NOT_EDITABLE' then
    raise exception 'ROTA_CALENDAR_PROOF: past availability write accepted';
  end if;
  v_result:=public.candidate_daily_availability_apply_atomic_v1(v_candidate_context,
    'rota-calendar-proof-future-0001',v_before,
    jsonb_build_array(jsonb_build_object('date',v_today+13,'availability','LONG_DAY')),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result->>'error_code' is distinct from 'AVAILABILITY_DATE_NOT_EDITABLE' then
    raise exception 'ROTA_CALENDAR_PROOF: unpublished date write accepted';
  end if;
  if exists(select 1 from public.candidate_daily_availability_days where candidate_id=v_candidate)
     or exists(select 1 from public.candidate_daily_sheet_projection_outbox where candidate_id=v_candidate) then
    raise exception 'ROTA_CALENDAR_PROOF: rejected writes left availability or projection effects';
  end if;
  -- Restore the fixture's actual current dates for the independent integrity checks.
  for v_date in select rota_date from public.candidate_daily_rota_days
    where generation_id=v_generation order by rota_date desc loop
    update public.candidate_daily_rota_days set rota_date=v_date+1
      where generation_id=v_generation and rota_date=v_date;
  end loop;
  update public.candidate_daily_rota_generations set window_start=v_today,window_end=v_today+13,
    published_at_utc=(v_today::timestamp at time zone 'Europe/London') where generation_id=v_generation;
  update private.candidate_daily_sync_state set required_visible_cursor=1,effective_visible_cursor=0
    where environment='TEST' and candidate_id=v_candidate and target='MASTER_AVAILABILITY_SHEET';
  v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,clock_timestamp());
  if v_result->>'ready' is distinct from 'false' or not(v_result->'reasons' ? 'PROJECTION_LAG') then
    raise exception 'ROTA_CALENDAR_PROOF: projection lag bypassed';
  end if;
  update private.candidate_daily_sync_state set required_visible_cursor=0,terminal_count=1
    where environment='TEST' and candidate_id=v_candidate and target='MASTER_AVAILABILITY_SHEET';
  v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,clock_timestamp());
  if v_result->>'ready' is distinct from 'false' or not(v_result->'reasons' ? 'TERMINAL_OUTBOX') then
    raise exception 'ROTA_CALENDAR_PROOF: failed projection bypassed';
  end if;
  update private.candidate_daily_sync_state set terminal_count=0
    where environment='TEST' and candidate_id=v_candidate and target='MASTER_AVAILABILITY_SHEET';
  v_result:=public.candidate_daily_availability_apply_atomic_v1(v_candidate_context,
    'rota-calendar-proof-current-0001',v_before,
    jsonb_build_array(jsonb_build_object('date',v_today,'availability','NIGHT')),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result->>'error_code' is not null
     or v_result->>'availability_version' is distinct from (v_before+1)::text
     or not exists(select 1 from public.candidate_daily_availability_days
       where candidate_id=v_candidate and availability_date=v_today and preference='NIGHT')
     or (select count(*) from public.candidate_daily_sheet_projection_outbox
       where candidate_id=v_candidate and availability_date=v_today)<>1 then
    raise exception 'ROTA_CALENDAR_PROOF: current date edit did not commit its exact projection';
  end if;
  -- A falsely complete header cannot hide a missing day; no real row is touched.
  delete from public.candidate_daily_rota_days where generation_id=v_generation and rota_date=v_today+13;
  v_result:=private._candidate_daily_freshness_v1('TEST',v_candidate,clock_timestamp());
  if v_result->>'ready' is distinct from 'false' or not(v_result->'reasons' ? 'GENERATION_INCOMPLETE') then
    raise exception 'ROTA_CALENDAR_PROOF: partial window accepted';
  end if;
  if has_function_privilege('anon','private._candidate_daily_freshness_v1(text,uuid,timestamptz)','EXECUTE')
     or has_function_privilege('authenticated','private._candidate_daily_freshness_v1(text,uuid,timestamptz)','EXECUTE')
     or has_function_privilege('service_role','private._candidate_daily_freshness_v1(text,uuid,timestamptz)','EXECUTE') then
    raise exception 'ROTA_CALENDAR_PROOF: internal helper exposed';
  end if;
  if not has_function_privilege('service_role','public.candidate_daily_tiles_get_v1(jsonb,date,integer)','EXECUTE')
     or not has_function_privilege('service_role','public.candidate_daily_availability_apply_atomic_v1(jsonb,text,bigint,jsonb,text)','EXECUTE')
     or has_function_privilege('anon','public.candidate_daily_tiles_get_v1(jsonb,date,integer)','EXECUTE')
     or has_function_privilege('authenticated','public.candidate_daily_availability_apply_atomic_v1(jsonb,text,bigint,jsonb,text)','EXECUTE') then
    raise exception 'ROTA_CALENDAR_PROOF: Candidate service boundary changed';
  end if;
  raise notice 'ROTA_CALENDAR_PROOF: PASS true age, 23/25-hour DST, continuous rollover reads, past/unpublished writes denied, capability, projection lag, terminal error, actual 14 days, internal ACL';
end;
$verification$;
rollback;
