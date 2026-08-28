-- First-use proof of booked/blocked availability clearing and exact replay.
-- All synthetic Candidate, generation, availability and outbox rows roll back.
\set ON_ERROR_STOP on
begin;
do $verification$
declare
  v_candidate uuid := '00000000-0000-4000-8000-00000000e956';
  v_source text := repeat('9',64);
  v_key text := 'CID1-956ABCDEFGHJKMNPQRS';
  v_today date := (clock_timestamp() at time zone 'Europe/London')::date;
  v_context jsonb := jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true);
  v_days jsonb; v_item jsonb; v_missing jsonb; v_result jsonb; v_replay jsonb;
  v_generation uuid; v_batch uuid := '00000000-0000-4000-8000-00000000e957';
  v_command uuid; v_version bigint; v_count integer;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values(v_candidate,'rota-clear-proof@example.invalid','Rota Clear Proof','Rota','Clear Proof',true,lower(v_key));
  select jsonb_agg(jsonb_build_object('date',(v_today+n)::text,'booked',false,
    'system_blocked',false,'source_row_hash',lpad(to_hex(n+1),64,'0')) order by n)
  into v_days from generate_series(0,13)n;
  v_item := jsonb_build_object('candidate_global_key',v_key,'candidate_source_hmac',v_source,
    'source_hmac_key_version',1,'source_event_id','rota-clear-proof-first-0001',
    'source_revision','rota-clear-proof-1','source_hash',repeat('a',64),
    'window_start',v_today::text,'days',v_days,'source_event_time',clock_timestamp()::text,
    'item_key','rota-clear-item-first-0001');
  v_result := public.candidate_daily_rota_generation_publish_atomic_v1(v_context,
    '00000000-0000-4000-8000-00000000e958','rota-clear-proof-initial-0001',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result#>>'{outcomes,0,status}' is distinct from 'COMMITTED' then
    raise exception 'ROTA_CLEAR_PROOF: initial blank generation failed';
  end if;

  -- The old app's accepted preference is cleared only for booked/blocked dates.
  v_result := public.candidate_daily_legacy_availability_apply_atomic_v1(
    v_context||jsonb_build_object('policy','LEGACY_COMPAT','candidate_source_hmac',v_source),
    v_source,'00000000-0000-4000-8000-00000000e959','rota-clear-proof-availability-0001',
    jsonb_build_array(jsonb_build_object('date',v_today::text,'availability','LD'),
      jsonb_build_object('date',(v_today+1)::text,'availability','N'),
      jsonb_build_object('date',(v_today+2)::text,'availability','LD')),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ');
  select canonical_version into v_version from private.candidate_daily_authority_scopes
    where environment='TEST' and candidate_id=v_candidate;
  if (select count(*) from public.candidate_daily_availability_days where environment='TEST'
      and candidate_id=v_candidate and preference<>'PENDING')<>3 then
    raise exception 'ROTA_CLEAR_PROOF: availability fixture did not seed three preferences';
  end if;
  v_days := jsonb_set(v_days,'{0}',(v_days->0)||jsonb_build_object('booked',true,
    'booking_id','ROTA-CLEAR-PROOF-BOOKING','shift_starts_at',(v_today::timestamp+interval '9 hours')::text||'Z',
    'shift_ends_at',(v_today::timestamp+interval '17 hours')::text||'Z'));
  v_days := jsonb_set(v_days,'{1}',(v_days->1)||jsonb_build_object('system_blocked',true));
  v_item := v_item||jsonb_build_object('source_event_id','rota-clear-proof-next-0001',
    'source_revision','rota-clear-proof-2','source_hash',repeat('b',64),'days',v_days,
    'item_key','rota-clear-item-next-0001');
  v_missing := v_item||jsonb_build_object('candidate_global_key','CID1-956MNPQRSTVWXYZ2345',
    'candidate_source_hmac',repeat('8',64),'item_key','rota-clear-unlinked-0001');
  v_result := public.candidate_daily_rota_generation_publish_atomic_v1(v_context,v_batch,
    'rota-clear-proof-next-batch-0001',jsonb_build_array(v_missing,v_item),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result#>>'{outcomes,0,error_code}' is distinct from 'IDENTITY_LINK_MISSING'
    or v_result#>>'{outcomes,1,status}' is distinct from 'COMMITTED' then
    raise exception 'ROTA_CLEAR_PROOF: mixed batch failed to progress past the unlinked Candidate';
  end if;
  v_generation := (v_result#>>'{outcomes,1,generation_id}')::uuid;
  select command_id into strict v_command from public.candidate_daily_command_receipts
    where environment='TEST' and candidate_id=v_candidate and command_class='ROTA_GENERATION_CLEAR'
      and idempotency_key='rota-clear:'||v_batch::text||':rota-clear-item-next-0001'
      and item_key='clear:rota-clear-item-next-0001' and state='COMPLETED';
  if (select count(*) from public.candidate_daily_availability_days where environment='TEST'
      and candidate_id=v_candidate and availability_date in(v_today,v_today+1)
      and preference='PENDING' and source_command_id=v_command and availability_version=v_version+1)<>2 then
    raise exception 'ROTA_CLEAR_PROOF: booked and blocked dates were not cleared exactly';
  end if;
  if not exists(select 1 from public.candidate_daily_availability_days where environment='TEST'
      and candidate_id=v_candidate and availability_date=v_today+2 and preference='LONG_DAY'
      and availability_version=v_version) then
    raise exception 'ROTA_CLEAR_PROOF: unrelated availability was changed';
  end if;
  if (select count(*) from public.candidate_daily_sheet_projection_outbox
      where environment='TEST' and candidate_id=v_candidate and command_id=v_command
        and preference='PENDING')<>2 then
    raise exception 'ROTA_CLEAR_PROOF: exact changed-date projection was not created';
  end if;
  if (select count(*) from public.candidate_daily_rota_days where generation_id=v_generation)<>14 then
    raise exception 'ROTA_CLEAR_PROOF: complete fourteen-day window missing';
  end if;
  v_replay := public.candidate_daily_rota_generation_publish_atomic_v1(v_context,v_batch,
    'rota-clear-proof-next-batch-0001',jsonb_build_array(v_missing,v_item),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_replay->>'_idempotent_replay' is distinct from 'true'
    or v_replay#>>'{outcomes,1,generation_id}' is distinct from v_generation::text
    or (select canonical_version from private.candidate_daily_authority_scopes
      where environment='TEST' and candidate_id=v_candidate)<>v_version+1
    or (select count(*) from public.candidate_daily_command_receipts where environment='TEST'
      and candidate_id=v_candidate and command_class='ROTA_GENERATION_CLEAR')<>1 then
    raise exception 'ROTA_CLEAR_PROOF: replay duplicated the clear operation';
  end if;
  if has_function_privilege('anon','public.candidate_daily_rota_generation_publish_atomic_v1(jsonb,uuid,text,jsonb,text)','EXECUTE')
    or has_function_privilege('authenticated','public.candidate_daily_rota_generation_publish_atomic_v1(jsonb,uuid,text,jsonb,text)','EXECUTE')
    or not has_function_privilege('service_role','public.candidate_daily_rota_generation_publish_atomic_v1(jsonb,uuid,text,jsonb,text)','EXECUTE') then
    raise exception 'ROTA_CLEAR_PROOF: service-only privilege boundary changed';
  end if;
  raise notice 'ROTA_CLEAR_PROOF: PASS blank window, mixed batch, booked/blocked clear, retained date, exact keys, 14 days, idempotent replay, service-only ACL';
end;
$verification$;
rollback;
