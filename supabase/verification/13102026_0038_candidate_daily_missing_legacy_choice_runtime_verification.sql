-- Missing legacy choices may be seeded only under stable DB authority.
-- All fixture changes are rolled back, including command/projection receipts.
\set ON_ERROR_STOP on
begin;
do $test$
declare
  v_candidate uuid:=gen_random_uuid();
  v_source text:=encode(extensions.digest(gen_random_uuid()::text,'sha256'),'hex');
  v_key text:='CID1-'||upper(replace(v_candidate::text,'-',''));
  v_batch uuid:=gen_random_uuid();
  v_generation uuid;
  v_system jsonb:=jsonb_build_object('policy','SIGNED_SYSTEM_SYNC','environment','TEST',
    'system_auth_verified',true,'nonce_consumed',true,'environment_trusted',true,
    'stable_operation_identity',true,'approved_source_mapping',true,'source_scope_ready',true,
    'authority_mode_compatible',true,'transition_ready',true,'route_operation','RECONCILIATION');
  v_days jsonb;
  v_items jsonb;
  v_result jsonb;
  v_replay jsonb;
  v_before jsonb;
  v_receipt uuid:=gen_random_uuid();
  v_version bigint;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values(v_candidate,'missing-choice-'||v_candidate||'@example.invalid',
    'Missing Choice Fixture','Missing','Choice',true,v_key);
  select jsonb_agg(jsonb_build_object('date',(date '2026-10-12'+n)::text,
    'booked',false,'system_blocked',false,'source_row_hash',lpad(to_hex(n+1),64,'0')) order by n)
    into v_days from generate_series(0,13)n;
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_system,gen_random_uuid(),
    'missing-choice-generation-'||v_candidate,jsonb_build_array(jsonb_build_object(
      'candidate_global_key',v_key,'candidate_source_hmac',v_source,'source_hmac_key_version',1,
      'source_event_id','missing-choice-generation-'||v_candidate,'source_revision','missing-choice-v1',
      'source_hash',repeat('a',64),'window_start','2026-10-12','days',v_days,
      'source_event_time','2026-10-13T00:00:00Z','item_key','missing-choice-generation-'||v_candidate)),
    '01M00000000000000000000008');
  if v_result#>>'{outcomes,0,status}'<>'COMMITTED' then raise exception 'Fixture generation failed: %',v_result; end if;
  v_generation:=(v_result#>>'{outcomes,0,generation_id}')::uuid;
  update private.candidate_daily_authority_scopes set authority_mode='SUPABASE_PRIMARY'
    where environment='TEST' and candidate_id=v_candidate;
  update public.candidate_daily_rota_days set booked=true,booking_id='missing-choice-fixture',
    shift_starts_at='2026-10-18T07:30:00Z',shift_ends_at='2026-10-18T19:30:00Z'
    where generation_id=v_generation and rota_date='2026-10-18';
  update public.candidate_daily_rota_days set system_blocked=true
    where generation_id=v_generation and rota_date='2026-10-19';
  insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,
    actor_class,command_class,idempotency_key,request_sha256,canonical_version_before,
    canonical_version_after,state,correlation_id,terminal_http_status,terminal_body_json,
    terminal_body_sha256,completed_at_utc)
  values(v_receipt,'TEST',v_candidate,'CANDIDATE','AVAILABILITY_APPLY',
    'missing-choice-original-'||v_candidate,repeat('b',64),0,1,'COMPLETED',
    '01M00000000000000000000008',200,'{}',repeat('b',64),now());
  update private.candidate_daily_authority_scopes set canonical_version=1
    where environment='TEST' and candidate_id=v_candidate;
  insert into public.candidate_daily_availability_days(environment,candidate_id,availability_date,
    preference,availability_version,source_class,source_command_id,changed_by_class,row_hash)
  values('TEST',v_candidate,'2026-10-16','PENDING',1,'CANDIDATE',v_receipt,'CANDIDATE',repeat('b',64)),
    ('TEST',v_candidate,'2026-10-17','NIGHT',1,'CANDIDATE',v_receipt,'CANDIDATE',repeat('b',64));
  select jsonb_agg(to_jsonb(a) order by availability_date) into v_before
    from public.candidate_daily_availability_days a where candidate_id=v_candidate;
  select jsonb_agg(jsonb_build_object('candidate_source_hmac',v_source,
    'date',(date '2026-10-12'+n)::text,
    'observed_value',case when n=8 then '' else 'LD' end,
    'observed_sheet_revision','missing-choice-window-v1',
    'source_event_id','missing-choice-observation-'||v_candidate||'-'||n,
    'source_revision','missing-choice-window-v1','source_event_time','2026-10-13T00:01:00Z',
    'source_hash',lpad(to_hex(n+100),64,'0'),
    'item_key','missing-choice-observation-'||v_candidate||'-'||n) order by n) into v_items
    from generate_series(0,8)n;
  -- n=9 is outside the active window.
  v_items:=v_items||jsonb_build_array(jsonb_set(v_items->0,'{date}','"2026-10-26"')||
    jsonb_build_object('item_key','missing-choice-outside-'||v_candidate,
      'source_event_id','missing-choice-outside-'||v_candidate));
  v_result:=public.candidate_daily_reconciliation_apply_atomic_v1(v_system,v_batch,
    'missing-choice-reconciliation-'||v_batch,v_items,'01M00000000000000000000008');
  if (select count(*) from jsonb_array_elements(v_result->'outcomes')o where o->>'classification'='MATCH')<>5
     or (select count(*) from jsonb_array_elements(v_result->'outcomes')o where o->>'classification'='REPAIR_PROJECTION')<>2
     or (select count(*) from jsonb_array_elements(v_result->'outcomes')o where o->>'classification'='CANONICAL_COMMAND_REQUIRED')<>3 then
    raise exception 'Unexpected missing-choice results: %',v_result;
  end if;
  if (select jsonb_agg(to_jsonb(a) order by availability_date) from public.candidate_daily_availability_days a
      where candidate_id=v_candidate and availability_date in ('2026-10-16','2026-10-17')) is distinct from v_before then
    raise exception 'Existing canonical choice (including explicit PENDING) was overwritten';
  end if;
  if (select count(*) from public.candidate_daily_availability_days where candidate_id=v_candidate)<>6
     or exists(select 1 from public.candidate_daily_availability_days where candidate_id=v_candidate
       and availability_date in ('2026-10-18','2026-10-19','2026-10-20','2026-10-26')) then
    raise exception 'Booked/blocked/blank/outside dates were backfilled';
  end if;
  if (select count(*) from public.candidate_daily_command_receipts where candidate_id=v_candidate
    and command_class='RECONCILIATION' and state='COMPLETED'
    and terminal_body_json->>'reason'='MISSING_AVAILABILITY_BACKFILLED')<>4 then
    raise exception 'Backfill did not produce exact durable audit receipts';
  end if;
  select canonical_version into v_version from private.candidate_daily_authority_scopes
    where environment='TEST' and candidate_id=v_candidate;
  if v_version<>5 then raise exception 'Backfill version count invalid: %',v_version; end if;
  v_replay:=public.candidate_daily_reconciliation_apply_atomic_v1(v_system,v_batch,
    'missing-choice-reconciliation-'||v_batch,v_items,'01M00000000000000000000008');
  if v_replay->>'_idempotent_replay'<>'true' or v_replay->'outcomes'<>v_result->'outcomes'
    or (select canonical_version from private.candidate_daily_authority_scopes
      where environment='TEST' and candidate_id=v_candidate)<>v_version then
    raise exception 'Replay changed availability or its audit version';
  end if;
  -- A new batch seeing the same now-existing facts must not create another command.
  perform public.candidate_daily_reconciliation_apply_atomic_v1(v_system,gen_random_uuid(),
    'missing-choice-new-batch-'||v_candidate,v_items,'01M00000000000000000000008');
  if (select count(*) from public.candidate_daily_command_receipts where candidate_id=v_candidate
    and command_class='RECONCILIATION')<>4 then raise exception 'Fresh reconciliation duplicated backfill'; end if;
  update private.candidate_daily_authority_scopes set transition_in_progress=true
    where environment='TEST' and candidate_id=v_candidate;
  v_items:=jsonb_build_array(jsonb_set(v_items->0,'{date}','"2026-10-21"')||
    jsonb_build_object('item_key','missing-choice-transition-'||v_candidate,
      'source_event_id','missing-choice-transition-'||v_candidate));
  perform public.candidate_daily_reconciliation_apply_atomic_v1(v_system,gen_random_uuid(),
    'missing-choice-transition-'||v_candidate,v_items,'01M00000000000000000000008');
  if exists(select 1 from public.candidate_daily_availability_days where candidate_id=v_candidate
    and availability_date='2026-10-21') then raise exception 'Transition backfill was allowed'; end if;
  update private.candidate_daily_authority_scopes set transition_in_progress=false,authority_mode='GOOGLE_PRIMARY'
    where environment='TEST' and candidate_id=v_candidate;
  perform public.candidate_daily_reconciliation_apply_atomic_v1(v_system,gen_random_uuid(),
    'missing-choice-google-primary-'||v_candidate,v_items,'01M00000000000000000000008');
  if exists(select 1 from public.candidate_daily_availability_days where candidate_id=v_candidate
    and availability_date='2026-10-21') then raise exception 'Google-primary transition semantics changed'; end if;
end;
$test$;
rollback;
