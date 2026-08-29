\set ON_ERROR_STOP on

begin;

do $test$
declare
  v_candidate uuid:='00000000-0000-4000-8000-00000000e101';
  v_other_candidate uuid:='00000000-0000-4000-8000-00000000e102';
  v_conflict_candidate uuid:='00000000-0000-4000-8000-00000000e103';
  v_existing_primary_candidate uuid:='00000000-0000-4000-8000-00000000e104';
  v_atomic_candidate uuid:='00000000-0000-4000-8000-00000000e105';
  v_duplicate_one uuid:='00000000-0000-4000-8000-00000000e106';
  v_duplicate_two uuid:='00000000-0000-4000-8000-00000000e107';
  v_inactive_candidate uuid:='00000000-0000-4000-8000-00000000e108';
  v_global_key text:='CID1-ABCDEFGHJKMNPQRS';
  v_source_hmac text:=repeat('1',64);
  v_other_hmac text:=repeat('2',64);
  v_existing_primary_hmac text:=repeat('3',64);
  v_replacement_hmac text:=repeat('4',64);
  v_atomic_hmac text:=repeat('5',64);
  v_evidence text:=repeat('e',64);
  v_system jsonb:=jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true
  );
  v_legacy jsonb;
  v_candidate_context jsonb;
  v_days jsonb;
  v_invalid_days jsonb;
  v_item jsonb;
  v_result jsonb;
  v_replay jsonb;
  v_reconciliation jsonb;
  v_transition jsonb;
  v_generation_id uuid;
  v_candidate_count_before integer;
  v_candidate_read_blocked boolean:=false;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm) values
    (v_candidate,'r15-first@example.invalid','R15 First Candidate','R15','First',true,'  cid1-abcdefghjkmnpqrs  '),
    (v_other_candidate,'r15-other@example.invalid','R15 Other Candidate','R15','Other',true,'CID1-TVWXYZ0123456789'),
    (v_conflict_candidate,'r15-conflict@example.invalid','R15 Conflict Candidate','R15','Conflict',true,'CID1-56789ABCDEFGHJKM'),
    (v_existing_primary_candidate,'r15-primary@example.invalid','R15 Primary Candidate','R15','Primary',true,'CID1-NPQRSTVWXYZ01234'),
    (v_atomic_candidate,'r15-atomic@example.invalid','R15 Atomic Candidate','R15','Atomic',true,'CID1-89ABCDEFGHJKMNPQ'),
    (v_duplicate_one,'r15-duplicate-one@example.invalid','R15 Duplicate One','R15','Duplicate One',true,'CID1-RSTVWXYZ01234567'),
    (v_duplicate_two,'r15-duplicate-two@example.invalid','R15 Duplicate Two','R15','Duplicate Two',false,'  cid1-rstvwxyz01234567  '),
    (v_inactive_candidate,'r15-inactive@example.invalid','R15 Inactive Candidate','R15','Inactive',false,'CID1-BCDEFGHJKMNPQRST');

  insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode)
  values('TEST',v_other_candidate,'GOOGLE_PRIMARY'),
    ('TEST',v_existing_primary_candidate,'GOOGLE_PRIMARY');
  insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
    canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
  values
    ('TEST',v_other_candidate,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      '00000000-0000-4000-8000-00000000e201',v_other_hmac,1,'PRIMARY',v_evidence),
    ('TEST',v_existing_primary_candidate,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
      '00000000-0000-4000-8000-00000000e202',v_existing_primary_hmac,1,'PRIMARY',v_evidence);

  select count(*) into v_candidate_count_before from public.candidates;
  select jsonb_agg(jsonb_build_object(
    'date',(date '2026-08-18'+n)::text,'booked',false,'system_blocked',false,
    'source_row_hash',lpad(to_hex(n+1),64,'0')
  ) order by n) into v_days from generate_series(0,13)n;
  v_invalid_days:=v_days-13;

  v_item:=jsonb_build_object(
    'candidate_global_key',v_global_key,'candidate_source_hmac',v_source_hmac,
    'source_hmac_key_version',1,'source_event_id','rota-event-r15-first-0001',
    'source_revision','r15-first-revision-1','source_hash',repeat('a',64),
    'window_start','2026-08-18','days',v_days,'source_event_time','2026-08-18T00:00:00Z',
    'item_key','rota-item-r15-first-0001'
  );
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e301','candidate-r15-first-generation-key-0001',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED' then
    raise exception 'First generation did not commit: %',v_result;
  end if;
  v_generation_id:=(v_result#>>'{outcomes,0,generation_id}')::uuid;

  if (select count(*) from private.candidate_daily_source_links l
      where l.environment='TEST' and l.candidate_id=v_candidate
        and l.identifier_hmac=v_source_hmac and l.hmac_key_version=1 and l.state='PRIMARY')<>1 then
    raise exception 'First generation did not create exactly one source link';
  end if;
  if not exists(select 1 from private.candidate_daily_authority_scopes s
      where s.environment='TEST' and s.candidate_id=v_candidate
        and s.authority_mode='GOOGLE_PRIMARY' and s.active_generation_id=v_generation_id) then
    raise exception 'First generation did not create the Google scope and active generation';
  end if;
  if exists(select 1 from private.candidate_daily_entitlements e
      where e.environment='TEST' and e.candidate_id=v_candidate) then
    raise exception 'First generation created an entitlement';
  end if;
  if (select count(*) from public.candidates)<>v_candidate_count_before then
    raise exception 'First generation created a Candidate row';
  end if;

  v_replay:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e301','candidate-r15-first-generation-key-0001',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_replay->>'_idempotent_replay'<>'true'
     or v_replay#>>'{outcomes,0,generation_id}'<>v_generation_id::text
     or (select count(*) from private.candidate_daily_source_links l
          where l.environment='TEST' and l.candidate_id=v_candidate and l.state='PRIMARY')<>1 then
    raise exception 'Exact replay changed the durable first-generation winner: %',v_replay;
  end if;

  v_legacy:=v_system||jsonb_build_object('policy','LEGACY_COMPAT','candidate_source_hmac',v_source_hmac);
  v_result:=public.candidate_daily_tiles_get_v1(v_legacy,'2026-08-18',14);
  if jsonb_array_length(v_result->'tiles')<>14
     or (v_result->>'generation_version')::bigint<>1 then
    raise exception 'Legacy enabled app did not read the canonical generation: %',v_result;
  end if;

  v_candidate_context:=jsonb_build_object('policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate);
  update public.settings_defaults
    set candidate_app_feature_flags_json=candidate_app_feature_flags_json||'{"candidate_daily_enabled":true}'::jsonb
  where id=1;
  begin
    perform public.candidate_daily_tiles_get_v1(v_candidate_context,'2026-08-18',14);
  exception when others then
    if sqlerrm='CANDIDATE_DAILY_DISABLED' then
      v_candidate_read_blocked:=true;
    else
      raise;
    end if;
  end;
  if not v_candidate_read_blocked then
    raise exception 'Candidate read succeeded before the controlled authority transition';
  end if;

  v_reconciliation:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system||jsonb_build_object('route_operation','RECONCILIATION'),
    '00000000-0000-4000-8000-00000000e309',
    'candidate-r16-reconciliation-key-0001',
    jsonb_build_array(jsonb_build_object(
      'candidate_source_hmac',v_source_hmac,
      'date','2026-08-18',
      'observed_value','',
      'observed_sheet_revision','r16-sheet-reconciled-1',
      'source_event_id','rota-event-r16-reconciliation-0001',
      'source_revision','r16-reconciliation-revision-1',
      'source_event_time','2026-08-18T00:00:30Z',
      'source_hash',repeat('d',64),
      'item_key','rota-item-r16-reconciliation-0001'
    )),
    '01K2ABCDEFGHJKMNPQRSTVWXY8'
  );
  if v_reconciliation#>>'{outcomes,0,classification}'<>'CANONICAL_COMMAND_REQUIRED' then
    raise exception 'Real reconciliation path did not establish the expected readiness fact: %',v_reconciliation;
  end if;

  v_transition:=public.candidate_daily_authority_transition_atomic_v1(
    v_system||jsonb_build_object('actor_user_id','00000000-0000-4000-8000-00000000e401'),
    '00000000-0000-4000-8000-00000000e310',
    'candidate-r16-authority-transition-key-0001',
    jsonb_build_array(jsonb_build_object(
      'candidate_id',v_candidate,
      'expected_authority_mode','GOOGLE_PRIMARY',
      'expected_canonical_version',0,
      'expected_entitlement_enabled',false,
      'new_authority_mode','SUPABASE_PRIMARY',
      'entitlement_enabled',true,
      'in_flight_disposition','DRAINED',
      'expected_generation_id',v_generation_id,
      'expected_generation_version',1,
      'expected_accepted_canonical_cursor',0,
      'expected_required_visible_cursor',0,
      'expected_effective_visible_cursor',0
    )),
    '00000000-0000-4000-8000-00000000e402',
    'R16 rolled-back controlled dual-consumer transition proof',
    v_evidence,
    '01K2ABCDEFGHJKMNPQRSTVWXY9'
  );
  if v_transition#>>'{outcomes,0,status}'<>'COMMITTED'
     or v_transition#>>'{outcomes,0,authority_mode}'<>'SUPABASE_PRIMARY'
     or v_transition#>>'{outcomes,0,entitlement_enabled}'<>'true'
     or not exists(select 1 from private.candidate_daily_authority_transitions t
       where t.environment='TEST' and t.candidate_id=v_candidate
         and t.actor_user_id='00000000-0000-4000-8000-00000000e401'
         and t.independent_approver_user_id='00000000-0000-4000-8000-00000000e402'
         and t.outcome='COMMITTED')
     or not exists(select 1 from private.candidate_daily_entitlements e
       where e.environment='TEST' and e.candidate_id=v_candidate and e.enabled) then
    raise exception 'Controlled authority transition did not create its durable authority and entitlement receipts: %',v_transition;
  end if;

  v_result:=public.candidate_daily_tiles_get_v1(v_candidate_context,'2026-08-18',14);
  if jsonb_array_length(v_result->'tiles')<>14
     or (v_result->>'generation_version')::bigint<>1 then
    raise exception 'New Candidate app did not read the same canonical generation: %',v_result;
  end if;
  update public.settings_defaults
    set candidate_app_feature_flags_json=candidate_app_feature_flags_json||'{"candidate_daily_enabled":false}'::jsonb
  where id=1;

  v_item:=v_item||jsonb_build_object(
    'source_event_id','rota-event-r15-first-0002','source_revision','r15-first-revision-2',
    'source_hash',repeat('b',64),'source_event_time','2026-08-18T00:01:00Z',
    'item_key','rota-item-r15-first-0002'
  );
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e302','candidate-r15-first-generation-key-0002',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXY1');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED'
     or (select count(*) from private.candidate_daily_source_links l
          where l.environment='TEST' and l.candidate_id=v_candidate and l.state='PRIMARY')<>1 then
    raise exception 'Later factual generation did not reuse the one source link: %',v_result;
  end if;

  v_item:=v_item||jsonb_build_object(
    'candidate_global_key','CID1-WXYZ0123456789AB','candidate_source_hmac',repeat('6',64),
    'source_event_id','rota-event-r15-missing-0001','source_hash',repeat('6',64),
    'item_key','rota-item-r15-missing-0001'
  );
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e303','candidate-r15-missing-global-key-0001',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXY2');
  if v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_MISSING'
     or exists(select 1 from private.candidate_daily_source_links where identifier_hmac=repeat('6',64)) then
    raise exception 'Missing global key did not reject without a link: %',v_result;
  end if;

  begin
    update public.candidates set active=true where id=v_duplicate_two;
    raise exception 'Normalized active CID1 duplicate unexpectedly became active';
  exception when unique_violation then
    null;
  end;
  if not exists(select 1 from public.candidates where id=v_duplicate_two and active=false) then
    raise exception 'Failed normalized activation did not preserve the inactive Candidate';
  end if;

  v_item:=v_item||jsonb_build_object(
    'candidate_global_key','CID1-BCDEFGHJKMNPQRST','candidate_source_hmac',repeat('8',64),
    'source_event_id','rota-event-r15-inactive-0001','source_hash',repeat('8',64),
    'item_key','rota-item-r15-inactive-0001'
  );
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e305','candidate-r15-inactive-key-0001',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXY4');
  if v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_MISSING' then
    raise exception 'Inactive Candidate key did not reject: %',v_result;
  end if;

  v_item:=v_item||jsonb_build_object(
    'candidate_global_key','CID1-56789ABCDEFGHJKM','candidate_source_hmac',v_other_hmac,
    'source_event_id','rota-event-r15-owner-conflict','source_hash',repeat('9',64),
    'item_key','rota-item-r15-owner-conflict'
  );
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e306','candidate-r15-owner-conflict-key',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXY5');
  if v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_CONFLICT'
     or exists(select 1 from public.candidate_daily_rota_generations
          where environment='TEST' and candidate_id=v_conflict_candidate) then
    raise exception 'Source owner conflict did not reject atomically: %',v_result;
  end if;

  v_item:=v_item||jsonb_build_object(
    'candidate_global_key','CID1-NPQRSTVWXYZ01234','candidate_source_hmac',v_replacement_hmac,
    'source_event_id','rota-event-r15-primary-conflict','source_hash',repeat('a',64),
    'item_key','rota-item-r15-primary-conflict'
  );
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e307','candidate-r15-primary-conflict-key',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXY6');
  if v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_CONFLICT'
     or exists(select 1 from private.candidate_daily_source_links where identifier_hmac=v_replacement_hmac) then
    raise exception 'Different primary source did not reject atomically: %',v_result;
  end if;

  v_item:=v_item||jsonb_build_object(
    'candidate_global_key','CID1-89ABCDEFGHJKMNPQ','candidate_source_hmac',v_atomic_hmac,
    'source_event_id','rota-event-r15-atomic-0001','source_hash',repeat('b',64),
    'days',v_invalid_days,'item_key','rota-item-r15-atomic-0001'
  );
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000e308','candidate-r15-atomic-rollback-key',
    jsonb_build_array(v_item),'01K2ABCDEFGHJKMNPQRSTVWXY7');
  if v_result#>>'{outcomes,0,error_code}'<>'GENERATION_INCOMPLETE'
     or exists(select 1 from private.candidate_daily_source_links where identifier_hmac=v_atomic_hmac)
     or exists(select 1 from private.candidate_daily_authority_scopes
          where environment='TEST' and candidate_id=v_atomic_candidate) then
    raise exception 'Generation rejection retained a first-use link or scope: %',v_result;
  end if;

  if (select count(*) from public.candidates)<>v_candidate_count_before then
    raise exception 'A negative journey created or deleted a Candidate row';
  end if;
end;
$test$;

rollback;

select 'candidate daily first-generation source link runtime verification passed' as result;
