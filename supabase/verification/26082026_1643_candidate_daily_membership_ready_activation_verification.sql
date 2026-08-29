-- Runtime proof for immediate Rota entitlement after active MyTMS membership.
-- Synthetic rows are rolled back. No customer, Candidate or provider state survives.
\set ON_ERROR_STOP on

begin;

do $verification$
declare
  v_candidate_id uuid:='00000000-0000-4000-8000-00000000e601';
  v_account_id uuid:='00000000-0000-4000-8000-00000000e602';
  v_membership_id uuid:='00000000-0000-4000-8000-00000000e603';
  v_generation_id uuid:='00000000-0000-4000-8000-00000000e604';
  v_generation_batch_id uuid:='00000000-0000-4000-8000-00000000e605';
  v_link_group_id uuid:='00000000-0000-4000-8000-00000000e606';
  v_today date:=(pg_catalog.clock_timestamp() at time zone 'Europe/London')::date;
  v_day integer;
  v_result jsonb;
begin
  update public.settings_defaults
  set candidate_app_feature_flags_json=
    candidate_app_feature_flags_json||'{"candidate_daily_enabled":true}'::jsonb
  where id=1;

  insert into public.candidates(
    id,email,display_name,first_name,last_name,active,key_norm
  ) values (
    v_candidate_id,'membership-ready@example.invalid','Membership Ready',
    'Membership','Ready',true,'cid1-abcde234'
  );

  insert into public.candidate_app_accounts(
    id,environment,email_normalized,status
  ) values (
    v_account_id,'TEST','membership-ready@example.invalid','ACTIVE'
  );

  insert into public.candidate_app_global_membership_links(
    membership_id,global_account_identity_hmac,account_id,candidate_id,
    candidate_code,membership_generation,state
  ) values (
    v_membership_id,decode(repeat('a',64),'hex'),v_account_id,v_candidate_id,
    'CID1-ABCDE234',3,'ACTIVE'
  );

  insert into private.candidate_daily_authority_scopes(
    environment,candidate_id,authority_mode,canonical_version
  ) values ('TEST',v_candidate_id,'GOOGLE_PRIMARY',0);

  insert into private.candidate_daily_entitlements(
    environment,candidate_id,enabled,reason,evidence_sha256
  ) values (
    'TEST',v_candidate_id,false,'Membership-ready verification',repeat('1',64)
  );

  insert into private.candidate_daily_source_links(
    environment,candidate_id,source_system,canonicalization_version,
    link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256
  ) values (
    'TEST',v_candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
    v_link_group_id,repeat('2',64),1,'PRIMARY',repeat('3',64)
  );

  insert into private.candidate_daily_batch_receipts(
    batch_receipt_id,environment,actor_class,operation_class,idempotency_key,
    request_hash,item_keys_json,item_count,state,terminal_http_status,
    terminal_response_body,terminal_response_sha256,correlation_id,completed_at_utc
  ) values (
    v_generation_batch_id,'TEST','SIGNED_SYSTEM','ROTA_GENERATION_PUBLISH',
    'membership-ready-generation-e605',repeat('4',64),
    jsonb_build_array(v_candidate_id::text),1,'COMPLETED',200,'{}'::jsonb,
    repeat('5',64),'01K2ABCDEF0123456789ABCDE6',clock_timestamp()
  );

  insert into public.candidate_daily_rota_generations(
    generation_id,environment,candidate_id,generation_version,window_start,
    window_end,state,expected_day_count,actual_day_count,source_system,
    source_event_id,source_revision,source_event_time,item_key,source_hash,
    generation_row_hash,batch_receipt_id,correlation_id,activated_at_utc,
    published_at_utc
  ) values (
    v_generation_id,'TEST',v_candidate_id,1,v_today,v_today+13,'ACTIVE',14,14,
    'MASTER_ROTA','membership-ready-source-e604','membership-ready-source-v1',
    clock_timestamp()-interval '1 day','membership-ready-item-e604',
    repeat('6',64),repeat('7',64),v_generation_batch_id,
    '01K2ABCDEF0123456789ABCDE6',clock_timestamp()-interval '1 day',
    clock_timestamp()-interval '1 day'
  );

  for v_day in 0..13 loop
    insert into public.candidate_daily_rota_days(
      generation_id,environment,candidate_id,rota_date,booked,system_blocked,
      source_row_hash
    ) values (
      v_generation_id,'TEST',v_candidate_id,v_today+v_day,false,false,
      repeat(substr(md5(v_candidate_id::text||v_day::text),1,1),64)
    );
  end loop;

  update private.candidate_daily_authority_scopes
  set active_generation_id=v_generation_id
  where environment='TEST' and candidate_id=v_candidate_id;

  insert into private.candidate_daily_sync_state(
    environment,candidate_id,target,accepted_canonical_cursor,
    required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,
    effective_visible_cursor,state
  ) values (
    'TEST',v_candidate_id,'MASTER_AVAILABILITY_SHEET',0,0,0,0,0,'READY'
  );

  v_result:=public.candidate_daily_system_policy_activate_ready_v1(
    jsonb_build_object(
      'policy','SIGNED_SYSTEM_SYNC','environment','TEST',
      'system_auth_verified',true,'nonce_consumed',true,
      'environment_trusted',true,'stable_operation_identity',true,
      'approved_source_mapping',true,'source_scope_ready',true,
      'authority_mode_compatible',true,'transition_ready',true,
      'activation_reason','FEDERATED_MEMBERSHIP_ACTIVE',
      'membership_id',v_membership_id,'membership_generation',3,
      'candidate_id',v_candidate_id
    ),'[]'::jsonb,'[]'::jsonb,'01K2ABCDEF0123456789ABCDE6'
  );

  if v_result#>>'{outcomes,0,status}'<>'ACTIVATED' then
    raise exception 'Membership-ready activation did not commit: %',v_result;
  end if;
  if not exists(
    select 1 from private.candidate_daily_entitlements e
    where e.environment='TEST' and e.candidate_id=v_candidate_id and e.enabled
  ) then
    raise exception 'Membership-ready activation did not enable the Rota entitlement';
  end if;
  if (select authority_mode from private.candidate_daily_authority_scopes
      where environment='TEST' and candidate_id=v_candidate_id)<>'SUPABASE_PRIMARY' then
    raise exception 'Membership-ready activation did not move authority atomically';
  end if;
  if (select key_norm from public.candidates where id=v_candidate_id)<>'cid1-abcde234' then
    raise exception 'Membership-ready activation rewrote Candidate identity';
  end if;
  if exists(
    select 1 from public.candidate_daily_availability_days a
    where a.environment='TEST' and a.candidate_id=v_candidate_id
  ) then
    raise exception 'Blank availability was not preserved as an unprovided window';
  end if;

  v_result:=public.candidate_daily_system_policy_activate_ready_v1(
    jsonb_build_object(
      'policy','SIGNED_SYSTEM_SYNC','environment','TEST',
      'system_auth_verified',true,'nonce_consumed',true,
      'environment_trusted',true,'stable_operation_identity',true,
      'approved_source_mapping',true,'source_scope_ready',true,
      'authority_mode_compatible',true,'transition_ready',true,
      'activation_reason','FEDERATED_MEMBERSHIP_ACTIVE',
      'membership_id',v_membership_id,'membership_generation',3,
      'candidate_id',v_candidate_id
    ),'[]'::jsonb,'[]'::jsonb,'01K2ABCDEF0123456789ABCDE6'
  );
  if v_result#>>'{outcomes,0,status}'<>'ALREADY_ENABLED' then
    raise exception 'Membership-ready replay was not safely idempotent: %',v_result;
  end if;
end;
$verification$;

rollback;
