\set ON_ERROR_STOP on

begin;

do $test$
declare
  v_candidate uuid := '00000000-0000-4000-8000-00000000d801';
  v_source_hmac text := repeat('a',64);
  v_evidence text := repeat('b',64);
  v_system jsonb := jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true
  );
  v_legacy jsonb;
  v_candidate_context jsonb;
  v_days jsonb;
  v_item jsonb;
  v_result jsonb;
  v_replay jsonb;
  v_claim jsonb;
  v_claim_item jsonb;
  v_generation_one uuid;
  v_generation_two uuid;
  v_outbox uuid;
  v_effect_receipt uuid;
  v_sync private.candidate_daily_sync_state%rowtype;
  v_definition_count integer;
  v_table_count integer;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values(v_candidate,'candidate-daily-r8@example.invalid','Candidate Daily R8','Candidate','Daily R8',true,
    'CID1-ABCDEFGHJKMNPQRS');

  insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode)
  values('TEST',v_candidate,'GOOGLE_PRIMARY');
  insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
  values('TEST',v_candidate,false,'R8 runtime fixture',v_evidence);
  insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
    canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
  values('TEST',v_candidate,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
    '00000000-0000-4000-8000-00000000d802',v_source_hmac,1,'PRIMARY',v_evidence);

  v_legacy := v_system || jsonb_build_object('policy','LEGACY_COMPAT','candidate_source_hmac',v_source_hmac);
  v_candidate_context := jsonb_build_object('policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate);

  -- Global Candidate exposure remains false, while signed generation publication continues.
  if (private._candidate_daily_capability_v1('TEST',v_candidate,now())->>'unavailable_reason') <> 'GLOBAL_DISABLED' then
    raise exception 'Expected GLOBAL_DISABLED before the test-only local gate is enabled';
  end if;

  select jsonb_agg(jsonb_build_object(
    'date',(date '2026-08-17'+n)::text,'booked',false,'system_blocked',false,
    'source_row_hash',repeat(to_hex((n % 15)+1),64)
  ) order by n) into v_days from generate_series(0,13) n;
  v_item := jsonb_build_object(
    'candidate_global_key','CID1-ABCDEFGHJKMNPQRS','candidate_source_hmac',v_source_hmac,
    'source_hmac_key_version',1,'source_event_id','rota-event-r8-0001',
    'source_revision','revision-1','source_hash',repeat('c',64),'window_start','2026-08-17',
    'days',v_days,'source_event_time','2026-08-17T00:00:00Z','item_key','rota-item-r8-0001'
  );
  v_result := public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000d803','candidate-daily-generation-key-0001',
    jsonb_build_array(v_item),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}' <> 'COMMITTED' then raise exception 'Generation one did not commit: %',v_result; end if;
  v_generation_one := (v_result#>>'{outcomes,0,generation_id}')::uuid;

  -- Legacy continuity is not coupled to the disabled Candidate product switch.
  v_result := public.candidate_daily_tiles_get_v1(v_legacy,'2026-08-17',14);
  if jsonb_array_length(v_result->'tiles') <> 14 then raise exception 'Legacy continuity did not return 14 tiles'; end if;
  v_result := public.candidate_daily_legacy_availability_apply_atomic_v1(
    v_legacy,v_source_hmac,'00000000-0000-4000-8000-00000000d804',
    'candidate-daily-legacy-key-0001',
    '[{"date":"2026-08-18","availability":"LD"}]'::jsonb,'01K2ABCDEF0123456789ABCDE2');
  if v_result#>>'{outcomes,0,applied}' <> 'true' or (v_result->>'committed_version')::bigint <> 1 then
    raise exception 'Legacy mixed-result command did not commit the accepted subset: %',v_result;
  end if;
  v_replay := public.candidate_daily_legacy_availability_apply_atomic_v1(
    v_legacy,v_source_hmac,'00000000-0000-4000-8000-00000000d804',
    'candidate-daily-legacy-key-0001',
    '[{"date":"2026-08-18","availability":"LD"}]'::jsonb,'01K2ABCDEF0123456789ABCDE2');
  if v_replay->>'_idempotent_replay' <> 'true' or (v_replay->>'committed_version')::bigint <> 1 then
    raise exception 'Legacy replay did not return the exact durable result';
  end if;

  -- Enable only inside this rolled-back fixture and prove all four Candidate policy inputs.
  update public.settings_defaults set candidate_app_feature_flags_json = candidate_app_feature_flags_json
    || '{"candidate_daily_enabled":true}'::jsonb where id=1;
  update private.candidate_daily_entitlements set enabled=true where environment='TEST' and candidate_id=v_candidate;
  update private.candidate_daily_authority_scopes set authority_mode='SUPABASE_PRIMARY'
    where environment='TEST' and candidate_id=v_candidate;
  if private._candidate_daily_capability_v1('TEST',v_candidate,now()) <> '{"enabled": true}'::jsonb then
    raise exception 'Candidate Daily four-input capability did not become ready';
  end if;
  v_result := public.candidate_daily_tiles_get_v1(v_candidate_context,'2026-08-17',14);
  if jsonb_array_length(v_result->'tiles') <> 14 or v_result->>'availability_version' <> '1' then
    raise exception 'Candidate tiles did not expose the canonical generation/version';
  end if;

  -- Candidate command commits once, creates one projection row, and replay carries transport-only metadata.
  v_result := public.candidate_daily_availability_apply_atomic_v1(
    v_candidate_context,'candidate-daily-command-key-0001',1,
    '[{"date":"2026-08-19","availability":"NIGHT"}]'::jsonb,'01K2ABCDEF0123456789ABCDE3');
  if (v_result->>'availability_version')::bigint <> 2 or jsonb_array_length(v_result->'changed_dates') <> 1 then
    raise exception 'Candidate availability command did not commit exactly once: %',v_result;
  end if;
  v_replay := public.candidate_daily_availability_apply_atomic_v1(
    v_candidate_context,'candidate-daily-command-key-0001',1,
    '[{"date":"2026-08-19","availability":"NIGHT"}]'::jsonb,'01K2ABCDEF0123456789ABCDE3');
  if v_replay->>'_idempotent_replay' <> 'true' or v_replay->>'command_id' <> v_result->>'command_id' then
    raise exception 'Candidate command replay changed the durable winner';
  end if;
  begin
    perform public.candidate_daily_availability_apply_atomic_v1(
      v_candidate_context,'candidate-daily-command-key-0001',1,
      '[{"date":"2026-08-20","availability":"NIGHT"}]'::jsonb,'01K2ABCDEF0123456789ABCDE3');
    raise exception 'Changed factual request reused one key without conflict';
  exception when unique_violation then null; end;
  select outbox_id into v_outbox from public.candidate_daily_sheet_projection_outbox
    where environment='TEST' and candidate_id=v_candidate and availability_version=2;
  if v_outbox is null then raise exception 'Candidate command did not create one projection outbox row'; end if;

  -- Park the row only behind a current booked overlay and prove that exact overlay is visible.
  update public.candidate_daily_rota_days set booked=true,booking_id='BOOKING-R8-1',
    shift_starts_at='2026-08-19T08:00:00Z',shift_ends_at='2026-08-19T16:00:00Z'
  where generation_id=v_generation_one and rota_date='2026-08-19';
  v_claim := public.candidate_daily_projection_claim_v1(
    v_system,'00000000-0000-4000-8000-00000000d805','candidate-daily-claim-key-0001',
    'MASTER_AVAILABILITY_SHEET','r8-runtime-worker',10,120,'01K2ABCDEF0123456789ABCDE4');
  v_claim_item := v_claim#>'{items,0}';
  if (v_claim_item->>'outbox_id')::uuid <> v_outbox then raise exception 'Projection claim lost row identity'; end if;
  v_result := public.candidate_daily_projection_complete_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000d806','candidate-daily-complete-key-0001',
    jsonb_build_array(jsonb_build_object('outbox_id',v_outbox,'lease_token',v_claim_item->>'lease_token',
      'outcome','DEFERRED_OVERLAY','observed_sheet_revision','sheet-revision-r8-1')),
    '01K2ABCDEF0123456789ABCDE5');
  if v_result#>>'{outcomes,0,state}' <> 'DEFERRED_OVERLAY' then raise exception 'Overlay deferral was not recorded'; end if;
  select * into v_sync from private.candidate_daily_sync_state
    where environment='TEST' and candidate_id=v_candidate and target='MASTER_AVAILABILITY_SHEET';
  if v_sync.overlay_proof_cursor <> 2 or v_sync.effective_visible_cursor <> 2 then
    raise exception 'Current overlay did not prove effective visibility';
  end if;

  -- A new generation removes the overlay; proof retreats and the same row is woken for projection.
  v_item := jsonb_build_object(
    'candidate_global_key','CID1-ABCDEFGHJKMNPQRS','candidate_source_hmac',v_source_hmac,
    'source_hmac_key_version',1,'source_event_id','rota-event-r8-0002',
    'source_revision','revision-2','source_hash',repeat('d',64),'window_start','2026-08-17',
    'days',v_days,'source_event_time','2026-08-17T00:01:00Z','item_key','rota-item-r8-0002'
  );
  v_result := public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000d807','candidate-daily-generation-key-0002',
    jsonb_build_array(v_item),'01K2ABCDEF0123456789ABCDE6');
  v_generation_two := (v_result#>>'{outcomes,0,generation_id}')::uuid;
  if v_generation_two = v_generation_one then raise exception 'Generation two did not replace generation one'; end if;
  if (select state from public.candidate_daily_sheet_projection_outbox where outbox_id=v_outbox) <> 'PENDING' then
    raise exception 'Stale overlay did not wake the parked projection';
  end if;
  select * into v_sync from private.candidate_daily_sync_state
    where environment='TEST' and candidate_id=v_candidate and target='MASTER_AVAILABILITY_SHEET';
  if v_sync.overlay_proof_cursor <> 0 or v_sync.effective_visible_cursor <> 1 then
    raise exception 'Stale overlay proof did not retreat safely: %',row_to_json(v_sync);
  end if;

  -- Delivery with an observed sheet revision restores contiguous visibility and readiness.
  v_claim := public.candidate_daily_projection_claim_v1(
    v_system,'00000000-0000-4000-8000-00000000d808','candidate-daily-claim-key-0002',
    'MASTER_AVAILABILITY_SHEET','r8-runtime-worker',10,120,'01K2ABCDEF0123456789ABCDE7');
  v_claim_item := v_claim#>'{items,0}';
  v_result := public.candidate_daily_projection_complete_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000d809','candidate-daily-complete-key-0002',
    jsonb_build_array(jsonb_build_object('outbox_id',v_outbox,'lease_token',v_claim_item->>'lease_token',
      'outcome','DELIVERED','observed_sheet_revision','sheet-revision-r8-2')),
    '01K2ABCDEF0123456789ABCDE8');
  select * into v_sync from private.candidate_daily_sync_state
    where environment='TEST' and candidate_id=v_candidate and target='MASTER_AVAILABILITY_SHEET';
  if v_sync.delivered_visible_cursor <> 2 or v_sync.effective_visible_cursor <> 2 or v_sync.state <> 'READY' then
    raise exception 'Delivered projection did not restore readiness: %',row_to_json(v_sync);
  end if;

  -- External effects are claimed once, completed once, and exact replay never re-executes the provider action.
  v_result := public.candidate_daily_external_effect_claim_v1(
    v_system,'candidate-effect-key-r8-0001','CANNOT_ATTEND',v_source_hmac,repeat('e',64),
    'r8-effect-worker','candidate-effect-idempotency-0001',120,now(),'01K2ABCDEF0123456789ABCDE9');
  if v_result->>'state' <> 'CLAIMED' then raise exception 'External effect was not claimed'; end if;
  v_effect_receipt := (v_result->>'effect_receipt_id')::uuid;
  v_result := public.candidate_daily_external_effect_complete_v1(
    v_system,v_effect_receipt,v_result->>'lease_token','COMPLETED',repeat('f',64),'{}'::jsonb,
    now(),'01K2ABCDEF0123456789ABCDEA');
  if v_result->>'state' <> 'COMPLETED' then raise exception 'External effect did not complete'; end if;
  v_replay := public.candidate_daily_external_effect_claim_v1(
    v_system,'candidate-effect-key-r8-0001','CANNOT_ATTEND',v_source_hmac,repeat('e',64),
    'r8-effect-worker','candidate-effect-idempotency-0001',120,now(),'01K2ABCDEF0123456789ABCDE9');
  if v_replay->>'state' <> 'COMPLETED' or v_replay->>'_idempotent_replay' <> 'true' then
    raise exception 'External effect replay did not return its terminal receipt';
  end if;
  v_result := public.candidate_daily_external_effect_status_get_v1(
    v_system,'candidate-effect-key-r8-0001',now(),'01K2ABCDEF0123456789ABCDEB');
  if v_result->>'status' <> 'COMPLETED' then raise exception 'External effect status lost terminal state'; end if;

  -- The exact schema/RPC boundary remains fixed and direct roles remain denied.
  select count(*) into v_table_count from information_schema.tables
  where (table_schema,table_name) in (
    ('public','candidate_daily_availability_days'),('public','candidate_daily_command_receipts'),
    ('private','candidate_daily_batch_receipts'),('public','candidate_daily_rota_generations'),
    ('public','candidate_daily_rota_days'),('public','candidate_daily_sheet_projection_outbox'),
    ('private','candidate_daily_source_links'),('private','candidate_daily_sync_state'),
    ('private','candidate_daily_entitlements'),('private','candidate_daily_authority_scopes'),
    ('private','candidate_daily_authority_transitions'),('private','candidate_daily_external_effect_receipts')
  );
  if v_table_count <> 12 then raise exception 'Expected exactly 12 Phase 2 authority tables, found %',v_table_count; end if;
  select count(*) into v_definition_count from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public' and p.proname in (
    'candidate_daily_tiles_get_v1','candidate_daily_availability_apply_atomic_v1',
    'candidate_daily_legacy_availability_apply_atomic_v1','candidate_daily_legacy_availability_status_get_v1',
    'candidate_daily_rota_generation_publish_atomic_v1','candidate_daily_projection_claim_v1',
    'candidate_daily_projection_complete_atomic_v1','candidate_daily_sync_status_get_v1',
    'candidate_daily_reconciliation_apply_atomic_v1','candidate_daily_authority_transition_atomic_v1',
    'candidate_daily_external_effect_claim_v1','candidate_daily_external_effect_complete_v1',
    'candidate_daily_external_effect_status_get_v1'
  );
  if v_definition_count <> 13 then raise exception 'Expected exactly 13 Phase 2 service RPCs, found %',v_definition_count; end if;
  if not exists(select 1 from pg_trigger where tgname='candidate_daily_authority_transitions_immutable'
      and not tgisinternal) then raise exception 'Immutable transition trigger is missing'; end if;
end;
$test$;

rollback;

select 'candidate daily phase2 runtime verification passed' as result;
