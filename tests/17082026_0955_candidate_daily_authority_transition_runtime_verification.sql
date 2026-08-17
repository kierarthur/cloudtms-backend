\set ON_ERROR_STOP on

begin;

create function pg_temp.candidate_daily_transition_context()
returns jsonb language sql stable as $function$
  select jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true,'actor_user_id','00000000-0000-4000-8000-00000000e101'
  );
$function$;

create function pg_temp.candidate_daily_transition_item(
  p_candidate_id uuid,
  p_expected_mode text,
  p_expected_version bigint,
  p_expected_entitlement boolean,
  p_new_mode text,
  p_entitlement boolean,
  p_disposition text,
  p_generation_id uuid default null,
  p_generation_version bigint default null,
  p_accepted_cursor bigint default null,
  p_required_cursor bigint default null,
  p_effective_cursor bigint default null,
  p_source_link jsonb default null
)
returns jsonb language sql immutable as $function$
  select jsonb_strip_nulls(jsonb_build_object(
    'candidate_id',p_candidate_id,
    'expected_authority_mode',p_expected_mode,
    'expected_canonical_version',p_expected_version,
    'expected_entitlement_enabled',p_expected_entitlement,
    'new_authority_mode',p_new_mode,
    'entitlement_enabled',p_entitlement,
    'in_flight_disposition',p_disposition,
    'expected_generation_id',p_generation_id,
    'expected_generation_version',p_generation_version,
    'expected_accepted_canonical_cursor',p_accepted_cursor,
    'expected_required_visible_cursor',p_required_cursor,
    'expected_effective_visible_cursor',p_effective_cursor,
    'source_link',p_source_link
  ));
$function$;

create function pg_temp.candidate_daily_transition_fixture(
  p_candidate_id uuid,
  p_generation_id uuid,
  p_source_hmac text,
  p_canonical_version bigint default 0,
  p_generation_state text default 'ACTIVE',
  p_generation_day_count integer default 14,
  p_generation_age interval default interval '0 seconds',
  p_source_mode text default 'PRIMARY'
)
returns void language plpgsql as $function$
declare
  v_generation_batch uuid:=gen_random_uuid();
  v_link_group uuid:=gen_random_uuid();
  v_day integer;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active)
  values(p_candidate_id,p_candidate_id::text||'@candidate-daily-r9.invalid',
    'Candidate Daily R9','Candidate','Daily R9',true);
  insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode,canonical_version)
  values('TEST',p_candidate_id,'GOOGLE_PRIMARY',p_canonical_version);
  insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
  values('TEST',p_candidate_id,false,'R9 authority transition fixture',repeat('9',64));
  if p_source_mode<>'NONE' then
    insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
      canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,
      valid_from_utc,valid_to_utc,evidence_sha256)
    values('TEST',p_candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',v_link_group,
      replace(p_candidate_id::text,'-','')||left(p_source_hmac,32),1,'PRIMARY',now()-interval '1 day',
      case when p_source_mode='DISABLED' then now()-interval '1 minute' else null end,repeat('8',64));
    if p_source_mode='AMBIGUOUS' then
      insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
        canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
      values('TEST',p_candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',gen_random_uuid(),
        replace(p_candidate_id::text,'-','')||left(p_source_hmac,31)||
          case right(p_source_hmac,1) when 'f' then 'e' else 'f' end,
        2,'OVERLAP',repeat('8',64));
    end if;
  end if;
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,
    operation_class,idempotency_key,request_hash,item_keys_json,item_count,state,terminal_http_status,
    terminal_response_body,terminal_response_sha256,correlation_id,completed_at_utc)
  values(v_generation_batch,'TEST','SIGNED_SYSTEM','ROTA_GENERATION_PUBLISH',
    'r9-generation-'||replace(p_candidate_id::text,'-',''),repeat('7',64),
    jsonb_build_array(p_candidate_id::text),1,'COMPLETED',200,'{}'::jsonb,repeat('6',64),
    '01K2ABCDEF0123456789ABCDE1',now());
  insert into public.candidate_daily_rota_generations(generation_id,environment,candidate_id,
    generation_version,window_start,window_end,state,expected_day_count,actual_day_count,source_system,
    source_event_id,source_revision,source_event_time,item_key,source_hash,generation_row_hash,
    batch_receipt_id,correlation_id,activated_at_utc,published_at_utc)
  values(p_generation_id,'TEST',p_candidate_id,1,date '2026-08-17',date '2026-08-30',
    p_generation_state,14,p_generation_day_count,'MASTER_ROTA','r9-source-'||p_candidate_id::text,
    'r9-source-revision',now()-p_generation_age,'r9-item-'||p_candidate_id::text,
    repeat('5',64),repeat('4',64),v_generation_batch,'01K2ABCDEF0123456789ABCDE1',
    case when p_generation_state='ACTIVE' then now()-p_generation_age else null end,
    case when p_generation_state='ACTIVE' then now()-p_generation_age else null end);
  for v_day in 0..p_generation_day_count-1 loop
    insert into public.candidate_daily_rota_days(generation_id,environment,candidate_id,rota_date,
      booked,system_blocked,source_row_hash)
    values(p_generation_id,'TEST',p_candidate_id,date '2026-08-17'+v_day,false,false,
      repeat(substr(md5(p_candidate_id::text||v_day::text),1,1),64));
  end loop;
  update private.candidate_daily_authority_scopes set active_generation_id=p_generation_id
  where environment='TEST' and candidate_id=p_candidate_id;
  insert into private.candidate_daily_sync_state(environment,candidate_id,target,
    accepted_canonical_cursor,required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,
    effective_visible_cursor,observed_source_revision,state,last_acknowledged_at_utc,last_pulled_at_utc,
    last_reconciled_at_utc)
  values('TEST',p_candidate_id,'MASTER_AVAILABILITY_SHEET',p_canonical_version,p_canonical_version,
    p_canonical_version,p_canonical_version,p_canonical_version,'sheet-revision-ready','READY',
    now(),now(),now());
end;
$function$;

create function pg_temp.candidate_daily_outbox_fixture(
  p_candidate_id uuid,
  p_generation_id uuid,
  p_state text,
  p_valid_overlay boolean default false
)
returns uuid language plpgsql as $function$
declare
  v_command_id uuid:=gen_random_uuid();
  v_outbox_id uuid:=gen_random_uuid();
  v_source_hash text;
begin
  select source_row_hash into v_source_hash from public.candidate_daily_rota_days
  where generation_id=p_generation_id and rota_date=date '2026-08-17';
  insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
    command_class,idempotency_key,request_sha256,canonical_version_before,canonical_version_after,state,
    terminal_http_status,terminal_body_json,terminal_body_sha256,correlation_id,completed_at_utc)
  values(v_command_id,'TEST',p_candidate_id,'CANDIDATE','AVAILABILITY_APPLY',
    'r9-outbox-'||replace(v_command_id::text,'-',''),repeat('3',64),0,1,'COMPLETED',200,
    '{}'::jsonb,repeat('2',64),'01K2ABCDEF0123456789ABCDE1',now());
  insert into public.candidate_daily_sheet_projection_outbox(outbox_id,environment,candidate_id,
    availability_date,availability_version,preference,command_id,state,lease_owner,lease_token,
    lease_expires_at_utc,overlay_generation_id,overlay_generation_version,overlay_source_row_hash,
    correlation_id,completed_at_utc)
  values(v_outbox_id,'TEST',p_candidate_id,date '2026-08-17',1,'LONG_DAY',v_command_id,p_state,
    case when p_state='CLAIMED' then 'r9-worker' else null end,
    case when p_state='CLAIMED' then repeat('a',32) else null end,
    case when p_state='CLAIMED' then now()+interval '1 minute' else null end,
    case when p_state='DEFERRED_OVERLAY' then p_generation_id else null end,
    case when p_state='DEFERRED_OVERLAY' then 1 else null end,
    case when p_state='DEFERRED_OVERLAY' then
      case when p_valid_overlay then v_source_hash else repeat('0',64) end else null end,
    '01K2ABCDEF0123456789ABCDE1',case when p_state in ('DEFERRED_OVERLAY','DELIVERED','TERMINAL') then now() else null end);
  update private.candidate_daily_sync_state set last_reconciled_at_utc=now(),updated_at_utc=now()
  where environment='TEST' and candidate_id=p_candidate_id and target='MASTER_AVAILABILITY_SHEET';
  return v_outbox_id;
end;
$function$;

do $test$
declare
  v_actor uuid:='00000000-0000-4000-8000-00000000e101';
  v_approver uuid:='00000000-0000-4000-8000-00000000e102';
  v_candidate uuid:='00000000-0000-4000-8000-00000000e201';
  v_generation uuid:='00000000-0000-4000-8000-00000000e211';
  v_missing uuid:='00000000-0000-4000-8000-00000000e299';
  v_result jsonb; v_replay jsonb; v_item jsonb; v_before integer; v_after integer;
begin
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('1',64));

  -- A cutover may be prepared dark, but it still requires the complete database-owned proof.
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e221',
    'r9-forward-transition-0001',jsonb_build_array(v_item),v_approver,
    'R9 proved forward transition',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED' then raise exception 'Valid dark forward cutover failed: %',v_result; end if;
  if (select authority_mode from private.candidate_daily_authority_scopes
      where environment='TEST' and candidate_id=v_candidate)<>'SUPABASE_PRIMARY' then
    raise exception 'Forward transition did not update authority';
  end if;
  if exists(select 1 from private.candidate_daily_authority_transitions
      where candidate_id=v_candidate and (generation_id_snapshot is null or generation_version_snapshot<>1
        or sync_snapshot_json='{}'::jsonb or in_flight_disposition<>'DRAINED')) then
    raise exception 'Forward transition did not freeze the proved winner facts';
  end if;

  -- Exact replay returns the durable result; changed use of the same key conflicts.
  v_replay:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e221',
    'r9-forward-transition-0001',jsonb_build_array(v_item),v_approver,
    'R9 proved forward transition',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  if v_replay->>'_idempotent_replay'<>'true' or v_replay#>>'{outcomes,0,transition_id}'<>v_result#>>'{outcomes,0,transition_id}' then
    raise exception 'Exact transition replay changed the winner';
  end if;
  begin
    perform public.candidate_daily_authority_transition_atomic_v1(
      pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e221',
      'r9-forward-transition-0001',jsonb_build_array(v_item||jsonb_build_object('entitlement_enabled',true)),
      v_approver,'R9 proved forward transition',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
    raise exception 'Changed transition reused one key without conflict';
  exception when unique_violation then null; end;

  -- A disabled global switch rejects entitlement activation; exact true permits it after all barriers pass.
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'SUPABASE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',true,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e222',
    'r9-global-off-transition-0001',jsonb_build_array(v_item),v_approver,
    'R9 global off rejection',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'REJECTED' or v_result#>>'{outcomes,0,error_code}'<>'SEMANTIC_REJECTION' then
    raise exception 'Global-off entitlement activation was not rejected: %',v_result;
  end if;
  update public.settings_defaults set candidate_app_feature_flags_json=
    candidate_app_feature_flags_json||'{"candidate_daily_enabled":true}'::jsonb where id=1;
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e223',
    'r9-global-on-transition-0001',jsonb_build_array(v_item),v_approver,
    'R9 cohort entitlement activation',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED' or v_result#>>'{outcomes,0,entitlement_enabled}'<>'true' then
    raise exception 'Global-on entitlement activation did not commit: %',v_result;
  end if;

  -- Rollback is explicit: disable first, enter ROLLBACK_PENDING, prove parity, then return to Google.
  update public.settings_defaults set candidate_app_feature_flags_json=
    candidate_app_feature_flags_json||'{"candidate_daily_enabled":false}'::jsonb where id=1;
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'SUPABASE_PRIMARY',0,true,
    'ROLLBACK_PENDING',false,'DRAINED');
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e224',
    'r9-rollback-pending-0001',jsonb_build_array(v_item),v_approver,
    'R9 rollback fence',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED' then raise exception 'ROLLBACK_PENDING failed: %',v_result; end if;
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'ROLLBACK_PENDING',0,false,
    'GOOGLE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e225',
    'r9-rollback-complete-0001',jsonb_build_array(v_item),v_approver,
    'R9 rollback parity proved',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED'
     or v_result#>>'{outcomes,0,authority_mode}'<>'GOOGLE_PRIMARY' then
    raise exception 'Proved rollback did not complete: %',v_result;
  end if;

  -- An exact no-op changes neither the transition ledger nor the current authority.
  select count(*) into v_before from private.candidate_daily_authority_transitions where candidate_id=v_candidate;
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'GOOGLE_PRIMARY',false,'NONE');
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e226',
    'r9-authority-no-change-0001',jsonb_build_array(v_item),v_approver,
    'R9 exact no change',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  select count(*) into v_after from private.candidate_daily_authority_transitions where candidate_id=v_candidate;
  if v_result#>>'{outcomes,0,status}'<>'NO_CHANGE' or v_after<>v_before then
    raise exception 'No-op created authority drift: %',v_result;
  end if;

  -- A transition never creates a missing scope or a partial authority.
  insert into public.candidates(id,email,display_name,first_name,last_name,active)
  values(v_missing,'missing-scope-r9@example.invalid','Missing Scope R9','Missing','Scope R9',true);
  v_item:=pg_temp.candidate_daily_transition_item(v_missing,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED','00000000-0000-4000-8000-00000000e291',1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(
    pg_temp.candidate_daily_transition_context(),'00000000-0000-4000-8000-00000000e227',
    'r9-missing-scope-transition',jsonb_build_array(v_item),v_approver,
    'R9 missing scope rejection',repeat('a',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'REJECTED'
     or exists(select 1 from private.candidate_daily_authority_scopes where candidate_id=v_missing) then
    raise exception 'Missing scope was created or accepted: %',v_result;
  end if;
end;
$test$;

do $negative$
declare
  v_approver uuid:='00000000-0000-4000-8000-00000000e102';
  v_candidate uuid; v_generation uuid; v_item jsonb; v_result jsonb; v_outbox uuid;
  v_case integer:=0;
begin
  -- Missing, incomplete, stale and expected-generation mismatch all fail closed.
  v_candidate:='00000000-0000-4000-8000-00000000e301';
  v_generation:='00000000-0000-4000-8000-00000000e311';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('2',64),0,'BUILDING',13);
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e321','r9-incomplete-generation',jsonb_build_array(v_item),
    v_approver,'R9 incomplete generation',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,error_code}'<>'GENERATION_INCOMPLETE' then raise exception 'Incomplete generation accepted: %',v_result; end if;

  v_candidate:='00000000-0000-4000-8000-00000000e302';
  v_generation:='00000000-0000-4000-8000-00000000e312';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('3',64),0,'ACTIVE',14,interval '10 minutes');
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e322','r9-stale-generation-001',jsonb_build_array(v_item),
    v_approver,'R9 stale generation',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,error_code}'<>'CANDIDATE_DAILY_NOT_READY' then raise exception 'Stale generation accepted: %',v_result; end if;

  v_candidate:='00000000-0000-4000-8000-00000000e303';
  v_generation:='00000000-0000-4000-8000-00000000e313';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('4',64));
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED','00000000-0000-4000-8000-00000000e399',1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e323','r9-generation-mismatch',jsonb_build_array(v_item),
    v_approver,'R9 generation mismatch',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,error_code}'<>'SEMANTIC_REJECTION' then raise exception 'Generation mismatch accepted: %',v_result; end if;

  v_candidate:='00000000-0000-4000-8000-00000000e304';
  v_generation:='00000000-0000-4000-8000-00000000e314';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('d',64));
  update private.candidate_daily_authority_scopes set active_generation_id=null where candidate_id=v_candidate;
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e324','r9-missing-generation-001',jsonb_build_array(v_item),
    v_approver,'R9 missing generation',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'REJECTED' then raise exception 'Missing generation accepted: %',v_result; end if;

  v_candidate:='00000000-0000-4000-8000-00000000e305';
  v_generation:='00000000-0000-4000-8000-00000000e315';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('e',64),2);
  update private.candidate_daily_sync_state set effective_visible_cursor=1 where candidate_id=v_candidate;
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',2,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,2,2,2);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e325','r9-lagging-cursor-00001',jsonb_build_array(v_item),
    v_approver,'R9 lagging cursor',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,error_code}'<>'CANDIDATE_DAILY_NOT_READY' then
    raise exception 'Lagging effective cursor accepted: %',v_result;
  end if;

  v_candidate:='00000000-0000-4000-8000-00000000e306';
  v_generation:='00000000-0000-4000-8000-00000000e316';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('9',64));
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,2,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e326','r9-generation-version-mismatch',jsonb_build_array(v_item),
    v_approver,'R9 generation version mismatch',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,error_code}'<>'GENERATION_INCOMPLETE' then
    raise exception 'Generation version mismatch accepted: %',v_result;
  end if;

  v_candidate:='00000000-0000-4000-8000-00000000e307';
  v_generation:='00000000-0000-4000-8000-00000000e317';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('a',64));
  delete from private.candidate_daily_sync_state where candidate_id=v_candidate;
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e327','r9-missing-sync-state-0001',jsonb_build_array(v_item),
    v_approver,'R9 missing sync state',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'REJECTED'
     or exists(select 1 from private.candidate_daily_sync_state where candidate_id=v_candidate) then
    raise exception 'Missing sync state was accepted or repaired by transition: %',v_result;
  end if;

  v_candidate:='00000000-0000-4000-8000-00000000e308';
  v_generation:='00000000-0000-4000-8000-00000000e318';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('b',64));
  update private.candidate_daily_sync_state
    set last_reconciled_at_utc=now()-interval '10 minutes'
    where candidate_id=v_candidate;
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
    'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e328','r9-stale-reconciliation-001',jsonb_build_array(v_item),
    v_approver,'R9 stale reconciliation watermark',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,error_code}'<>'CANDIDATE_DAILY_NOT_READY' then
    raise exception 'Stale reconciliation watermark accepted: %',v_result;
  end if;

  -- Missing, ambiguous and disabled source identity are distinct fail-closed results.
  for v_case in 0..2 loop
    v_candidate:=('00000000-0000-4000-8000-'||lpad((340+v_case)::text,12,'0'))::uuid;
    v_generation:=('00000000-0000-4000-8000-'||lpad((350+v_case)::text,12,'0'))::uuid;
    perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,
      repeat(substr('567',v_case+1,1),64),0,'ACTIVE',14,interval '0 seconds',
      case v_case when 0 then 'NONE' when 1 then 'AMBIGUOUS' else 'DISABLED' end);
    v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
      'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0);
    v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
      gen_random_uuid(),'r9-source-state-'||v_case::text||'-0001',jsonb_build_array(v_item),v_approver,
      'R9 source state rejection',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
    if v_result#>>'{outcomes,0,status}'<>'REJECTED' then raise exception 'Invalid source state accepted: %',v_result; end if;
    if v_case=1 and v_result#>>'{outcomes,0,error_code}'<>'IDENTITY_LINK_AMBIGUOUS' then
      raise exception 'Ambiguous source did not report its stable rejection: %',v_result;
    end if;
  end loop;

  -- Invalid overlay proof is not accepted as visibility; an exact current overlay is accepted as RECONCILED.
  v_candidate:='00000000-0000-4000-8000-00000000e361';
  v_generation:='00000000-0000-4000-8000-00000000e371';
  perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat('8',64),1);
  update public.candidate_daily_rota_days set booked=true,booking_id='R9-BOOKING-1',
    shift_starts_at=now(),shift_ends_at=now()+interval '8 hours'
  where generation_id=v_generation and rota_date=date '2026-08-17';
  v_outbox:=pg_temp.candidate_daily_outbox_fixture(v_candidate,v_generation,'DEFERRED_OVERLAY',false);
  v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',1,false,
    'SUPABASE_PRIMARY',false,'RECONCILED',v_generation,1,1,1,1);
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e381','r9-invalid-overlay-0001',jsonb_build_array(v_item),
    v_approver,'R9 invalid overlay rejection',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,error_code}'<>'PROJECTION_STALE_COMPLETION' then raise exception 'Invalid overlay accepted: %',v_result; end if;
  update public.candidate_daily_sheet_projection_outbox set overlay_source_row_hash=(
    select source_row_hash from public.candidate_daily_rota_days where generation_id=v_generation and rota_date=date '2026-08-17'),
    updated_at_utc=now() where outbox_id=v_outbox;
  update private.candidate_daily_sync_state set last_reconciled_at_utc=now() where candidate_id=v_candidate;
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e382','r9-valid-overlay-000001',jsonb_build_array(v_item),
    v_approver,'R9 exact overlay proof',repeat('b',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED' then raise exception 'Valid overlay did not commit: %',v_result; end if;
  if (select in_flight_disposition from private.candidate_daily_authority_transitions
      where transition_id=(v_result#>>'{outcomes,0,transition_id}')::uuid)<>'RECONCILED' then
    raise exception 'Valid overlay did not freeze RECONCILED disposition';
  end if;
end;
$negative$;

do $inflight$
declare
  v_approver uuid:='00000000-0000-4000-8000-00000000e102';
  v_candidate uuid; v_generation uuid; v_item jsonb; v_result jsonb; v_state text; v_command uuid;
  v_effect uuid; v_kind integer;
begin
  -- Every unresolved projection state blocks strict cutover; a false DRAINED claim is rejected independently.
  foreach v_state in array array['PENDING','CLAIMED','RETRY','TERMINAL'] loop
    v_kind:=case v_state when 'PENDING' then 1 when 'CLAIMED' then 2 when 'RETRY' then 3 else 4 end;
    v_candidate:=('00000000-0000-4000-8000-'||lpad((400+v_kind)::text,12,'0'))::uuid;
    v_generation:=('00000000-0000-4000-8000-'||lpad((410+v_kind)::text,12,'0'))::uuid;
    perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat(v_kind::text,64),1);
    perform pg_temp.candidate_daily_outbox_fixture(v_candidate,v_generation,v_state,false);
    v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',1,false,
      'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,1,1,1);
    v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
      gen_random_uuid(),'r9-outbox-false-drained-'||v_kind::text,jsonb_build_array(v_item),v_approver,
      'R9 caller disposition rejection',repeat('c',64),'01K2ABCDEF0123456789ABCDE1');
    if v_result#>>'{outcomes,0,error_code}'<>'SEMANTIC_REJECTION' then
      raise exception 'Caller-controlled DRAINED survived %: %',v_state,v_result;
    end if;
    v_item:=v_item||jsonb_build_object('in_flight_disposition','NONE');
    v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
      gen_random_uuid(),'r9-outbox-derived-none-'||v_kind::text,jsonb_build_array(v_item),v_approver,
      'R9 unresolved projection rejection',repeat('c',64),'01K2ABCDEF0123456789ABCDE1');
    if v_result#>>'{outcomes,0,status}'<>'REJECTED' then raise exception 'Unresolved projection accepted: %, %',v_state,v_result; end if;
  end loop;

  -- IN_PROGRESS command and batch receipts, and IN_PROGRESS/UNKNOWN effects, block strict transition.
  for v_kind in 1..4 loop
    v_candidate:=('00000000-0000-4000-8000-'||lpad((430+v_kind)::text,12,'0'))::uuid;
    v_generation:=('00000000-0000-4000-8000-'||lpad((440+v_kind)::text,12,'0'))::uuid;
    perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat((v_kind+4)::text,64));
    if v_kind=1 then
      v_command:=gen_random_uuid();
      insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
        command_class,idempotency_key,request_sha256,state,correlation_id)
      values(v_command,'TEST',v_candidate,'CANDIDATE','AVAILABILITY_APPLY',
        'r9-in-progress-command-0001',repeat('d',64),'IN_PROGRESS','01K2ABCDEF0123456789ABCDE1');
    elsif v_kind=2 then
      insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
        idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
      values(gen_random_uuid(),'TEST','SIGNED_SYSTEM','RECONCILIATION','r9-in-progress-batch-0001',
        repeat('d',64),jsonb_build_array(v_candidate::text),1,'IN_PROGRESS','01K2ABCDEF0123456789ABCDE1');
    else
      v_effect:=gen_random_uuid();
      insert into private.candidate_daily_external_effect_receipts(effect_receipt_id,environment,candidate_id,
        effect_key,operation,request_hash,idempotency_key,state,first_claimed_at_utc,lease_owner,lease_token,
        lease_expires_at_utc,stable_provider_request_id,safe_evidence_json,terminal_result_json,
        terminal_body_sha256,correlation_id,retain_until_utc,completed_at_utc)
      values(v_effect,'TEST',v_candidate,'r9-effect-key-'||v_kind::text||'-0001','CANNOT_ATTEND',repeat('d',64),
        'r9-effect-idempotency-'||v_kind::text,
        case when v_kind=3 then 'IN_PROGRESS' else 'UNKNOWN' end,now(),
        case when v_kind=3 then 'r9-effect-worker' else null end,
        case when v_kind=3 then repeat('e',32) else null end,
        case when v_kind=3 then now()+interval '1 minute' else null end,
        'r9-effect-provider-'||v_kind::text,'{}'::jsonb,
        case when v_kind=4 then '{}'::jsonb else null end,
        case when v_kind=4 then repeat('e',64) else null end,
        '01K2ABCDEF0123456789ABCDE1',now()+interval '7 years',
        case when v_kind=4 then now() else null end);
    end if;
    v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'GOOGLE_PRIMARY',0,false,
      'SUPABASE_PRIMARY',false,'NONE',v_generation,1,0,0,0);
    v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
      gen_random_uuid(),'r9-inflight-owner-'||v_kind::text||'-0001',jsonb_build_array(v_item),v_approver,
      'R9 in-flight owner rejection',repeat('c',64),'01K2ABCDEF0123456789ABCDE1');
    if v_result#>>'{outcomes,0,status}'<>'REJECTED' then raise exception 'In-flight owner % was accepted: %',v_kind,v_result; end if;
  end loop;
end;
$inflight$;

do $rollback_unresolved$
declare
  v_approver uuid:='00000000-0000-4000-8000-00000000e102';
  v_candidate uuid; v_generation uuid; v_item jsonb; v_result jsonb; v_state text; v_command uuid;
  v_effect uuid; v_kind integer; v_transition_count bigint;
begin
  -- The first rollback edge is itself an authority switch. Every unresolved projection state blocks it,
  -- including when the caller truthfully reports the database-derived NONE disposition.
  foreach v_state in array array['PENDING','CLAIMED','RETRY','TERMINAL'] loop
    v_kind:=case v_state when 'PENDING' then 1 when 'CLAIMED' then 2 when 'RETRY' then 3 else 4 end;
    v_candidate:=('00000000-0000-4000-8000-'||lpad((600+v_kind)::text,12,'0'))::uuid;
    v_generation:=('00000000-0000-4000-8000-'||lpad((610+v_kind)::text,12,'0'))::uuid;
    perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat(v_kind::text,64),1);
    update private.candidate_daily_authority_scopes set authority_mode='SUPABASE_PRIMARY'
    where environment='TEST' and candidate_id=v_candidate;
    perform pg_temp.candidate_daily_outbox_fixture(v_candidate,v_generation,v_state,false);
    v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'SUPABASE_PRIMARY',1,false,
      'ROLLBACK_PENDING',false,'DRAINED');
    v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
      gen_random_uuid(),'r10-rollback-false-drained-'||v_kind::text,jsonb_build_array(v_item),v_approver,
      'R10 caller disposition rejection',repeat('1',64),'01K2ABCDEF0123456789ABCDE1');
    if v_result#>>'{outcomes,0,error_code}'<>'SEMANTIC_REJECTION' then
      raise exception 'Rollback accepted false DRAINED for %: %',v_state,v_result;
    end if;
    v_item:=v_item||jsonb_build_object('in_flight_disposition','NONE');
    v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
      gen_random_uuid(),'r10-rollback-derived-none-'||v_kind::text,jsonb_build_array(v_item),v_approver,
      'R10 unresolved rollback rejection',repeat('1',64),'01K2ABCDEF0123456789ABCDE1');
    if v_result#>>'{outcomes,0,error_code}'<>'CANDIDATE_DAILY_NOT_READY' then
      raise exception 'Rollback accepted derived NONE for %: %',v_state,v_result;
    end if;
    if (select authority_mode from private.candidate_daily_authority_scopes
        where environment='TEST' and candidate_id=v_candidate)<>'SUPABASE_PRIMARY'
       or (select transition_in_progress from private.candidate_daily_authority_scopes
        where environment='TEST' and candidate_id=v_candidate)
       or (select enabled from private.candidate_daily_entitlements
        where environment='TEST' and candidate_id=v_candidate)
       or exists(select 1 from private.candidate_daily_authority_transitions
        where environment='TEST' and candidate_id=v_candidate) then
      raise exception 'Rejected rollback changed authority, entitlement, fence or ledger for %',v_state;
    end if;
  end loop;

  -- Active commands, other active batches, and in-progress/unknown external effects block the same edge.
  for v_kind in 1..4 loop
    v_candidate:=('00000000-0000-4000-8000-'||lpad((630+v_kind)::text,12,'0'))::uuid;
    v_generation:=('00000000-0000-4000-8000-'||lpad((640+v_kind)::text,12,'0'))::uuid;
    perform pg_temp.candidate_daily_transition_fixture(v_candidate,v_generation,repeat((v_kind+4)::text,64));
    update private.candidate_daily_authority_scopes set authority_mode='SUPABASE_PRIMARY'
    where environment='TEST' and candidate_id=v_candidate;
    if v_kind=1 then
      v_command:=gen_random_uuid();
      insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
        command_class,idempotency_key,request_sha256,state,correlation_id)
      values(v_command,'TEST',v_candidate,'CANDIDATE','AVAILABILITY_APPLY',
        'r10-rollback-in-progress-command-0001',repeat('2',64),'IN_PROGRESS','01K2ABCDEF0123456789ABCDE1');
    elsif v_kind=2 then
      insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
        idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
      values(gen_random_uuid(),'TEST','SIGNED_SYSTEM','RECONCILIATION','r10-rollback-in-progress-batch-0001',
        repeat('2',64),jsonb_build_array(v_candidate::text),1,'IN_PROGRESS','01K2ABCDEF0123456789ABCDE1');
    else
      v_effect:=gen_random_uuid();
      insert into private.candidate_daily_external_effect_receipts(effect_receipt_id,environment,candidate_id,
        effect_key,operation,request_hash,idempotency_key,state,first_claimed_at_utc,lease_owner,lease_token,
        lease_expires_at_utc,stable_provider_request_id,safe_evidence_json,terminal_result_json,
        terminal_body_sha256,correlation_id,retain_until_utc,completed_at_utc)
      values(v_effect,'TEST',v_candidate,'r10-rollback-effect-key-'||v_kind::text||'-0001','CANNOT_ATTEND',
        repeat('2',64),'r10-rollback-effect-idempotency-'||v_kind::text,
        case when v_kind=3 then 'IN_PROGRESS' else 'UNKNOWN' end,now(),
        case when v_kind=3 then 'r10-effect-worker' else null end,
        case when v_kind=3 then repeat('3',32) else null end,
        case when v_kind=3 then now()+interval '1 minute' else null end,
        'r10-effect-provider-'||v_kind::text,'{}'::jsonb,
        case when v_kind=4 then '{}'::jsonb else null end,
        case when v_kind=4 then repeat('3',64) else null end,
        '01K2ABCDEF0123456789ABCDE1',now()+interval '7 years',
        case when v_kind=4 then now() else null end);
    end if;
    select count(*) into v_transition_count from private.candidate_daily_authority_transitions
    where environment='TEST' and candidate_id=v_candidate;
    v_item:=pg_temp.candidate_daily_transition_item(v_candidate,'SUPABASE_PRIMARY',0,false,
      'ROLLBACK_PENDING',false,'NONE');
    v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
      gen_random_uuid(),'r10-rollback-inflight-owner-'||v_kind::text||'-0001',jsonb_build_array(v_item),v_approver,
      'R10 in-flight rollback rejection',repeat('2',64),'01K2ABCDEF0123456789ABCDE1');
    if v_result#>>'{outcomes,0,error_code}'<>'CANDIDATE_DAILY_NOT_READY' then
      raise exception 'Rollback accepted in-flight owner %: %',v_kind,v_result;
    end if;
    if (select authority_mode from private.candidate_daily_authority_scopes
        where environment='TEST' and candidate_id=v_candidate)<>'SUPABASE_PRIMARY'
       or (select transition_in_progress from private.candidate_daily_authority_scopes
        where environment='TEST' and candidate_id=v_candidate)
       or (select enabled from private.candidate_daily_entitlements
        where environment='TEST' and candidate_id=v_candidate)
       or (select count(*) from private.candidate_daily_authority_transitions
        where environment='TEST' and candidate_id=v_candidate)<>v_transition_count then
      raise exception 'Rejected rollback changed authority, entitlement, fence or ledger for owner %',v_kind;
    end if;
  end loop;
end;
$rollback_unresolved$;

do $cohort$
declare
  v_approver uuid:='00000000-0000-4000-8000-00000000e102';
  v_good uuid:='00000000-0000-4000-8000-00000000e501';
  v_bad uuid:='00000000-0000-4000-8000-00000000e502';
  v_generation uuid:='00000000-0000-4000-8000-00000000e511';
  v_result jsonb; v_items jsonb;
begin
  perform pg_temp.candidate_daily_transition_fixture(v_good,v_generation,repeat('a',64));
  insert into public.candidates(id,email,display_name,first_name,last_name,active)
  values(v_bad,'partial-bad-r9@example.invalid','Partial Bad R9','Partial','Bad R9',true);
  v_items:=jsonb_build_array(
    pg_temp.candidate_daily_transition_item(v_good,'GOOGLE_PRIMARY',0,false,
      'SUPABASE_PRIMARY',false,'DRAINED',v_generation,1,0,0,0),
    pg_temp.candidate_daily_transition_item(v_bad,'GOOGLE_PRIMARY',0,false,
      'SUPABASE_PRIMARY',false,'DRAINED','00000000-0000-4000-8000-00000000e599',1,0,0,0));
  v_result:=public.candidate_daily_authority_transition_atomic_v1(pg_temp.candidate_daily_transition_context(),
    '00000000-0000-4000-8000-00000000e521','r9-partial-cohort-000001',v_items,v_approver,
    'R9 explicit partial cohort',repeat('f',64),'01K2ABCDEF0123456789ABCDE1');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED' or v_result#>>'{outcomes,1,status}'<>'REJECTED'
     or (select authority_mode from private.candidate_daily_authority_scopes where candidate_id=v_good)<>'SUPABASE_PRIMARY'
     or exists(select 1 from private.candidate_daily_authority_scopes where candidate_id=v_bad) then
    raise exception 'Partial cohort result was not explicit and isolated: %',v_result;
  end if;
  if exists(select 1 from private.candidate_daily_authority_scopes where transition_in_progress) then
    raise exception 'A transition fence remained set after the cohort operation';
  end if;
end;
$cohort$;

rollback;

select 'candidate daily authority transition runtime verification passed' as result;
