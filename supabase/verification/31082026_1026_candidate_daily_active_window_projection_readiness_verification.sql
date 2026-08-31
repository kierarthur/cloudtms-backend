\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_candidate uuid:='00000000-0000-4000-8000-00000000fa01';
  v_command uuid:='00000000-0000-4000-8000-00000000fa02';
  v_past_outbox uuid:='00000000-0000-4000-8000-00000000fa03';
  v_current_outbox uuid:='00000000-0000-4000-8000-00000000fa04';
  v_current_terminal uuid:='00000000-0000-4000-8000-00000000fa05';
  v_today date:=(clock_timestamp() at time zone 'Europe/London')::date;
  v_source text:=repeat('a',64);
  v_days jsonb;
  v_result jsonb;
  v_context jsonb:=jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,
    'authority_mode_compatible',true,'transition_ready',true
  );
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values(v_candidate,'active-window-projection@example.invalid','Active Window Projection',
    'Active','Window Projection',true,'cid1-fa01abcdefghjkmnpqrs');
  select jsonb_agg(jsonb_build_object('date',(v_today+n)::text,'booked',false,
    'system_blocked',false,'source_row_hash',lpad(to_hex(n+1),64,'0')) order by n)
  into v_days from generate_series(0,13)n;
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_context,'00000000-0000-4000-8000-00000000fa06',
    'active-window-projection-publish-0001',jsonb_build_array(jsonb_build_object(
      'candidate_global_key','CID1-FA01ABCDEFGHJKMNPQRS','candidate_source_hmac',v_source,
      'source_hmac_key_version',1,'source_event_id','active-window-projection-event-0001',
      'source_revision','active-window-projection-1','source_hash',repeat('b',64),
      'window_start',v_today::text,'days',v_days,'source_event_time',clock_timestamp()::text,
      'item_key','active-window-projection-item-0001')),
    '01K2ABCDEFGHJKMNPQRSTVWXYZ'
  );
  if v_result#>>'{outcomes,0,status}' is distinct from 'COMMITTED' then
    raise exception 'ACTIVE_WINDOW_PROJECTION: publication fixture failed: %',v_result;
  end if;

  update private.candidate_daily_authority_scopes
  set canonical_version=2
  where environment='TEST' and candidate_id=v_candidate;
  insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,
    actor_class,command_class,idempotency_key,request_sha256,canonical_version_before,
    canonical_version_after,state,terminal_http_status,terminal_body_json,
    terminal_body_sha256,correlation_id,completed_at_utc)
  values(v_command,'TEST',v_candidate,'CANDIDATE','AVAILABILITY_APPLY',
    'active-window-projection-command-0001',repeat('c',64),0,2,'COMPLETED',200,
    '{}'::jsonb,repeat('d',64),'01K2ABCDEFGHJKMNPQRSTVWXY1',clock_timestamp());
  insert into public.candidate_daily_sheet_projection_outbox(outbox_id,environment,
    candidate_id,availability_date,availability_version,preference,command_id,state,
    delivery_attempt_count,safe_error_code,correlation_id,completed_at_utc)
  values
    (v_past_outbox,'TEST',v_candidate,v_today-1,1,'LONG_DAY',v_command,'TERMINAL',12,
      'LEGACY_TARGET_UNAVAILABLE','01K2ABCDEFGHJKMNPQRSTVWXY1',clock_timestamp()),
    (v_current_outbox,'TEST',v_candidate,v_today,2,'NIGHT',v_command,'DELIVERED',0,
      null,'01K2ABCDEFGHJKMNPQRSTVWXY1',clock_timestamp());

  v_result:=private._candidate_daily_refresh_sync_state_v1(
    'TEST',v_candidate,'MASTER_AVAILABILITY_SHEET',clock_timestamp());
  if v_result->>'state' is distinct from 'READY'
     or v_result->>'effective_visible_cursor' is distinct from '2'
     or v_result->>'delivered_visible_cursor' is distinct from '2'
     or v_result->>'terminal_count' is distinct from '0'
     or not exists(select 1 from public.candidate_daily_sheet_projection_outbox
       where outbox_id=v_past_outbox and state='TERMINAL'
         and safe_error_code='LEGACY_TARGET_UNAVAILABLE') then
    raise exception 'ACTIVE_WINDOW_PROJECTION: historical failure blocked or was erased';
  end if;

  insert into public.candidate_daily_sheet_projection_outbox(outbox_id,environment,
    candidate_id,availability_date,availability_version,preference,command_id,state,
    delivery_attempt_count,safe_error_code,correlation_id,completed_at_utc)
  values(v_current_terminal,'TEST',v_candidate,v_today+1,2,'LONG_DAY',v_command,
    'TERMINAL',12,'LEGACY_TARGET_UNAVAILABLE','01K2ABCDEFGHJKMNPQRSTVWXY1',clock_timestamp());
  v_result:=private._candidate_daily_refresh_sync_state_v1(
    'TEST',v_candidate,'MASTER_AVAILABILITY_SHEET',clock_timestamp());
  if v_result->>'state' is distinct from 'ERROR'
     or v_result->>'effective_visible_cursor' is distinct from '1'
     or v_result->>'terminal_count' is distinct from '1' then
    raise exception 'ACTIVE_WINDOW_PROJECTION: current-window failure was bypassed';
  end if;
  if has_function_privilege('anon',
       'private._candidate_daily_refresh_sync_state_v1(text,uuid,text,timestamptz)','EXECUTE')
     or has_function_privilege('authenticated',
       'private._candidate_daily_refresh_sync_state_v1(text,uuid,text,timestamptz)','EXECUTE')
     or has_function_privilege('service_role',
       'private._candidate_daily_refresh_sync_state_v1(text,uuid,text,timestamptz)','EXECUTE') then
    raise exception 'ACTIVE_WINDOW_PROJECTION: internal helper exposed';
  end if;
  raise notice 'ACTIVE_WINDOW_PROJECTION: PASS historical failure retained, current window ready, current failure blocks, ACL';
end;
$verification$;

rollback;
