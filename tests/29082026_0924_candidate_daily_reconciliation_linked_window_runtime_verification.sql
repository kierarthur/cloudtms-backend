\set ON_ERROR_STOP on

begin;

do $test$
declare
  v_candidate uuid:='00000000-0000-4000-8000-00000000f201';
  v_source_hmac text:=repeat('a',64);
  v_unlinked_hmac text:=repeat('b',64);
  v_evidence text:=repeat('e',64);
  v_system jsonb:=jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true,'route_operation','RECONCILIATION'
  );
  v_observations jsonb;
  v_result jsonb;
  v_replay jsonb;
  v_bulk jsonb;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values(v_candidate,'linked-window@example.invalid','Linked Window Candidate','Linked','Window',true,
    'CID1-LINKEDWINDOW0001');

  insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode)
  values('TEST',v_candidate,'GOOGLE_PRIMARY');
  insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
    canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
  values('TEST',v_candidate,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
    '00000000-0000-4000-8000-00000000f202',v_source_hmac,1,'PRIMARY',v_evidence);

  v_observations:=jsonb_build_array(
    jsonb_build_object(
      'candidate_source_hmac',v_source_hmac,'date','2026-08-29','observed_value','',
      'observed_sheet_revision',repeat('1',64),'source_event_id','linked-window-event-0001',
      'source_revision','linked-window-revision-0001','source_event_time','2026-08-29T00:00:00Z',
      'source_hash',repeat('2',64),'item_key','linked-window-item-0001'
    ),
    jsonb_build_object(
      'candidate_source_hmac',v_source_hmac,'date','2026-08-30','observed_value','',
      'observed_sheet_revision',repeat('3',64),'source_event_id','linked-window-event-0002',
      'source_revision','linked-window-revision-0002','source_event_time','2026-08-29T00:00:01Z',
      'source_hash',repeat('4',64),'item_key','linked-window-item-0002'
    ),
    jsonb_build_object(
      'candidate_source_hmac',v_unlinked_hmac,'date','2026-08-29','observed_value','',
      'observed_sheet_revision',repeat('5',64),'source_event_id','linked-window-event-0003',
      'source_revision','linked-window-revision-0003','source_event_time','2026-08-29T00:00:02Z',
      'source_hash',repeat('6',64),'item_key','linked-window-item-0003'
    )
  );

  v_result:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000f203','linked-window-batch-0001',
    v_observations,'01M00000000000000000000001'
  );
  if v_result#>>'{outcomes,0,classification}'<>'CANONICAL_COMMAND_REQUIRED'
     or v_result#>>'{outcomes,1,classification}'<>'CANONICAL_COMMAND_REQUIRED'
     or v_result#>>'{outcomes,2,classification}'<>'NOT_ENROLLED'
     or jsonb_array_length(v_result->'outcomes')<>3 then
    raise exception 'Linked/unlinked reconciliation classifications are incorrect: %',v_result;
  end if;
  if not exists(
    select 1 from private.candidate_daily_sync_state s
    where s.environment='TEST' and s.candidate_id=v_candidate
      and s.target='MASTER_AVAILABILITY_SHEET'
      and s.observed_source_revision=repeat('3',64)
  ) then
    raise exception 'Linked Candidate sync state was not refreshed once with the final observed revision';
  end if;
  if exists(
    select 1 from public.candidate_daily_sheet_projection_outbox o
    where o.environment='TEST' and o.candidate_id=v_candidate
  ) then
    raise exception 'Canonical-command-required observations unexpectedly created projection work';
  end if;

  v_replay:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000f203','linked-window-batch-0001',
    v_observations,'01M00000000000000000000001'
  );
  if v_replay->>'_idempotent_replay'<>'true'
     or v_replay->'outcomes'<>v_result->'outcomes' then
    raise exception 'Exact linked-window replay changed its terminal receipt: %',v_replay;
  end if;

  select jsonb_agg(jsonb_build_object(
    'candidate_source_hmac',case when n<=2 then v_source_hmac else lpad(to_hex(1000+n),64,'0') end,
    'date',(date '2026-08-29'+((n-1)%14))::text,
    'observed_value','',
    'observed_sheet_revision',lpad(to_hex(2000+n),64,'0'),
    'source_event_id','linked-window-bulk-event-'||lpad(n::text,4,'0'),
    'source_revision','linked-window-bulk-revision-'||lpad(n::text,4,'0'),
    'source_event_time','2026-08-29T00:01:00Z',
    'source_hash',lpad(to_hex(3000+n),64,'0'),
    'item_key','linked-window-bulk-item-'||lpad(n::text,4,'0')
  ) order by n) into v_bulk
  from generate_series(1,100)n;

  v_result:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000f204','linked-window-batch-0002',
    v_bulk,'01M00000000000000000000002'
  );
  if jsonb_array_length(v_result->'outcomes')<>100
     or (select count(*) from jsonb_array_elements(v_result->'outcomes')o
          where o->>'classification'='NOT_ENROLLED')<>98
     or (select count(*) from jsonb_array_elements(v_result->'outcomes')o
          where o->>'classification'='CANONICAL_COMMAND_REQUIRED')<>2 then
    raise exception 'Bounded 100-item linked-window probe did not classify every source safely: %',v_result;
  end if;
end;
$test$;

rollback;
