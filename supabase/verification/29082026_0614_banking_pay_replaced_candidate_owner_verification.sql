\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for replacement-session candidate work.
-- Candidate work must never be copied with an old session build identity.  A
-- candidate already present in target scope receives one canonical current
-- owner; a candidate not yet seeded is deferred to the target root scope job;
-- and an already-persisted raw replay owner is repaired without touching Draft,
-- provider, payment or settlement state.
BEGIN;
SET LOCAL statement_timeout='30s';

DO $verification$
DECLARE
  v_snapshot_id uuid:=gen_random_uuid();
  v_actor_id uuid:=gen_random_uuid();
  v_source_session_id uuid:=gen_random_uuid();
  v_target_session_id uuid:=gen_random_uuid();
  v_candidate_current uuid:=gen_random_uuid();
  v_candidate_repair uuid:=gen_random_uuid();
  v_candidate_deferred uuid:=gen_random_uuid();
  v_source_candidate_job uuid:=gen_random_uuid();
  v_source_root_job uuid:=gen_random_uuid();
  v_raw_target_job uuid:=gen_random_uuid();
  v_invalid_target_owner uuid:=gen_random_uuid();
  v_deferred_source_job uuid:=gen_random_uuid();
  v_result jsonb;
  v_repair_result jsonb;
  v_owner record;
  v_prefix text:='BANKING_PAY_REPLACED_CANDIDATE_VERIFY:' || gen_random_uuid()::text;
BEGIN
  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-01-09',DATE '2099-01-04',DATE '2098-12-29',
    DATE '2098-12-01',DATE '2099-01-04','OPEN',false
  );

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (
    v_actor_id,
    'bpay-replay-' || replace(v_actor_id::text,'-','') || '@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true
  );

  INSERT INTO public.candidates(id,display_name,tms_ref) VALUES
    (v_candidate_current,v_prefix || ':CURRENT',v_prefix || ':CURRENT'),
    (v_candidate_repair,v_prefix || ':REPAIR',v_prefix || ':REPAIR'),
    (v_candidate_deferred,v_prefix || ':DEFERRED',v_prefix || ':DEFERRED');

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version,discarded_at_utc
  ) VALUES
    (v_source_session_id,v_actor_id,DATE '2099-01-09',DATE '2099-01-04',
      v_prefix || ':SOURCE',v_snapshot_id,'DISCARDED',1,clock_timestamp()),
    (v_target_session_id,v_actor_id,DATE '2099-01-09',DATE '2099-01-04',
      v_prefix || ':TARGET',v_snapshot_id,'OPEN',2,NULL);

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty
  ) VALUES
    (v_target_session_id,v_candidate_current,1,'READY',true,false),
    (v_target_session_id,v_candidate_repair,2,'READY',true,false);

  INSERT INTO public.app_change_counters(entity_key,seq) VALUES
    ('pay_candidate:' || v_candidate_current::text,9),
    ('pay_candidate:' || v_candidate_repair::text,11),
    ('pay_candidate:' || v_candidate_deferred::text,13)
  ON CONFLICT(entity_key) DO UPDATE SET seq=EXCLUDED.seq;

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version
  ) VALUES
    (v_source_candidate_job,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
      clock_timestamp(),0,8,v_prefix || ':SOURCE_CANDIDATE',v_snapshot_id,
      v_source_session_id,v_candidate_current,jsonb_build_object(
        'session_id',v_source_session_id::text,'session_version',1,
        'source_change_seq',7,
        'source_build_run_id','10000000-0000-4000-8000-000000009901',
        'refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL'
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1),
    (v_source_root_job,'WORKBENCH_SESSION_SCOPE_SEED','QUEUED',40,
      clock_timestamp(),0,8,v_prefix || ':SOURCE_ROOT',v_snapshot_id,
      v_source_session_id,NULL,jsonb_build_object(
        'session_id',v_source_session_id::text,'session_version',1,'root_job',true
      ),NULL,NULL,NULL,'{}'::jsonb,NULL);

  v_result:=public.pay_workbench_session_replay_replaced_queue_v1(
    v_source_session_id,v_target_session_id,'VERIFICATION_CANONICAL_REPLAY','{}'::jsonb
  );

  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_result->>'candidate_refresh_enqueued_count')::integer,-1)<>1
     OR COALESCE((v_result->>'session_job_replayed_count')::integer,-1)<>1
     OR COALESCE((v_result->>'source_queued_terminalised_count')::integer,-1)<>2 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLACED_CANDIDATE_REPLAY_RESULT_INVALID',DETAIL=v_result::text;
  END IF;

  IF EXISTS(
    SELECT 1 FROM public.banking_pay_workbench_jobs AS copied_candidate
    WHERE copied_candidate.session_id=v_target_session_id
      AND copied_candidate.candidate_id=v_candidate_current
      AND copied_candidate.dedupe_key=(
        'REPLAY_REPLACED_SESSION:' || v_target_session_id::text ||
        ':source_job:' || v_source_candidate_job::text
      )
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_REPLACED_CANDIDATE_RAW_JOB_COPIED';
  END IF;

  SELECT scope_row.status,scope_row.pending_job_id,owner_job.status AS job_status,
    owner_job.dedupe_key,owner_job.payload_json->>'session_version' AS session_version,
    owner_job.payload_json->>'source_change_seq' AS source_change_seq,
    owner_job.payload_json->>'source_build_run_id' AS source_build_run_id,
    owner_job.payload_json->>'canonical_refresh_from_replaced_session' AS canonical_replay
  INTO STRICT v_owner
  FROM public.banking_pay_workbench_session_scope AS scope_row
  JOIN public.banking_pay_workbench_jobs AS owner_job ON owner_job.id=scope_row.pending_job_id
  WHERE scope_row.session_id=v_target_session_id
    AND scope_row.candidate_id=v_candidate_current;

  IF v_owner.status IS DISTINCT FROM 'SOURCE_BUILD_PENDING'
     OR v_owner.job_status NOT IN ('QUEUED','RUNNING')
     OR v_owner.session_version IS DISTINCT FROM '2'
     OR v_owner.source_change_seq::bigint<9
     OR v_owner.source_build_run_id !~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR v_owner.source_build_run_id='10000000-0000-4000-8000-000000009901'
     OR LOWER(COALESCE(v_owner.canonical_replay,'false'))<>'true'
     OR v_owner.dedupe_key LIKE 'REPLAY_REPLACED_SESSION:%' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLACED_CANDIDATE_CANONICAL_OWNER_INVALID',DETAIL=to_jsonb(v_owner)::text;
  END IF;

  SELECT status,economic_build_id,private_stage,private_cursor_kind,
    private_cursor_json,private_stage_version,last_error_json->>'code' AS error_code
  INTO STRICT v_owner
  FROM public.banking_pay_workbench_jobs WHERE id=v_source_candidate_job;
  IF v_owner.status IS DISTINCT FROM 'DEAD'
     OR v_owner.economic_build_id IS NOT NULL
     OR v_owner.private_stage IS NOT NULL
     OR v_owner.private_cursor_kind IS NOT NULL
     OR v_owner.private_cursor_json IS DISTINCT FROM '{}'::jsonb
     OR v_owner.private_stage_version IS NOT NULL
     OR v_owner.error_code IS DISTINCT FROM 'REPLACED_SESSION_QUEUE_REPLAYED' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLACED_CANDIDATE_SOURCE_TERMINAL_INVALID',DETAIL=to_jsonb(v_owner)::text;
  END IF;

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version
  ) VALUES (
    v_raw_target_job,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
    clock_timestamp(),0,8,
    'REPLAY_REPLACED_SESSION:' || v_target_session_id::text || ':source_job:' || gen_random_uuid()::text,
    v_snapshot_id,v_target_session_id,v_candidate_repair,jsonb_build_object(
      'session_id',v_target_session_id::text,'session_version',2,
      'source_change_seq',7,
      'source_build_run_id','10000000-0000-4000-8000-000000009902',
      'replayed_from_replaced_session',true,
      'replayed_from_session_id',v_source_session_id::text,
      'replayed_from_job_id',gen_random_uuid()::text
    ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1
  );

  -- Reproduce the real persisted recovery shape: the raw replay row still
  -- exists, but a later automatic-recovery pass has already pointed the scope
  -- at a different queued owner whose session context is stale.
  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version
  ) VALUES (
    v_invalid_target_owner,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
    clock_timestamp(),0,8,v_prefix || ':INVALID_RECOVERY_SUCCESSOR',v_snapshot_id,
    v_target_session_id,v_candidate_repair,jsonb_build_object(
      'session_id',v_target_session_id::text,'session_version',1,
      'source_change_seq',7,
      'source_build_run_id',gen_random_uuid()::text,
      'owner_repair',true,'owner_repair_reason','HISTORICAL_REPLAY_RECOVERY'
    ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1
  );
  UPDATE public.banking_pay_workbench_session_scope
  SET status='SOURCE_BUILD_PENDING',dirty=true,pending_job_id=v_invalid_target_owner,error_json=NULL
  WHERE session_id=v_target_session_id AND candidate_id=v_candidate_repair;

  v_repair_result:=public.pay_workbench_repair_replayed_candidate_jobs_v1(
    v_target_session_id,v_candidate_repair,1,'VERIFICATION_RAW_REPLAY_REPAIR'
  );
  IF COALESCE((v_repair_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_repair_result->>'repaired_candidate_count')::integer,-1)<>1
     OR COALESCE((v_repair_result->>'terminalised_replay_job_count')::integer,-1)<>1 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLAYED_CANDIDATE_REPAIR_RESULT_INVALID',DETAIL=v_repair_result::text;
  END IF;

  SELECT status,economic_build_id,private_stage,private_cursor_kind,
    private_cursor_json,private_stage_version,last_error_json->>'code' AS error_code
  INTO STRICT v_owner
  FROM public.banking_pay_workbench_jobs WHERE id=v_raw_target_job;
  IF v_owner.status IS DISTINCT FROM 'DEAD'
     OR v_owner.economic_build_id IS NOT NULL
     OR v_owner.private_stage IS NOT NULL
     OR v_owner.private_cursor_kind IS NOT NULL
     OR v_owner.private_cursor_json IS DISTINCT FROM '{}'::jsonb
     OR v_owner.private_stage_version IS NOT NULL
     OR v_owner.error_code IS DISTINCT FROM 'REPLACED_SESSION_CANDIDATE_JOB_REQUIRES_CANONICAL_OWNER' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLAYED_CANDIDATE_REPAIR_TERMINAL_INVALID',DETAIL=to_jsonb(v_owner)::text;
  END IF;

  SELECT scope_row.status,scope_row.pending_job_id,owner_job.status AS job_status,
    owner_job.dedupe_key,owner_job.payload_json->>'session_version' AS session_version,
    owner_job.payload_json->>'source_change_seq' AS source_change_seq,
    owner_job.payload_json->>'source_build_run_id' AS source_build_run_id,
    owner_job.payload_json->>'replayed_from_replaced_session' AS raw_replay
  INTO STRICT v_owner
  FROM public.banking_pay_workbench_session_scope AS scope_row
  JOIN public.banking_pay_workbench_jobs AS owner_job ON owner_job.id=scope_row.pending_job_id
  WHERE scope_row.session_id=v_target_session_id
    AND scope_row.candidate_id=v_candidate_repair;
  IF v_owner.status IS DISTINCT FROM 'SOURCE_BUILD_PENDING'
     OR v_owner.job_status NOT IN ('QUEUED','RUNNING')
     OR v_owner.session_version IS DISTINCT FROM '2'
     OR v_owner.source_change_seq::bigint<11
     OR v_owner.source_build_run_id !~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR v_owner.source_build_run_id='10000000-0000-4000-8000-000000009902'
     OR LOWER(COALESCE(v_owner.raw_replay,'false'))='true'
     OR v_owner.pending_job_id IN (v_raw_target_job,v_invalid_target_owner)
     OR v_owner.dedupe_key LIKE 'REPLAY_REPLACED_SESSION:%' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLAYED_CANDIDATE_REPAIR_OWNER_INVALID',DETAIL=to_jsonb(v_owner)::text;
  END IF;

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version
  ) VALUES (
    v_deferred_source_job,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
    clock_timestamp(),0,8,v_prefix || ':DEFERRED_SOURCE',v_snapshot_id,
    v_source_session_id,v_candidate_deferred,jsonb_build_object(
      'session_id',v_source_session_id::text,'session_version',1,
      'source_change_seq',12,
      'source_build_run_id','10000000-0000-4000-8000-000000009903'
    ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1
  );

  v_result:=public.pay_workbench_session_replay_replaced_queue_v1(
    v_source_session_id,v_target_session_id,'VERIFICATION_DEFERRED_REPLAY','{}'::jsonb
  );
  IF COALESCE((v_result->>'candidate_refresh_deferred_count')::integer,-1)<>1
     OR COALESCE((v_result->>'candidate_scope_missing_count')::integer,-1)<>1
     OR EXISTS(
       SELECT 1 FROM public.banking_pay_workbench_jobs AS copied_deferred
       WHERE copied_deferred.session_id=v_target_session_id
         AND copied_deferred.candidate_id=v_candidate_deferred
     ) THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLACED_CANDIDATE_DEFER_CONTRACT_INVALID',DETAIL=v_result::text;
  END IF;

  v_result:=public.pay_workbench_repair_replayed_candidate_jobs_v1(
    v_target_session_id,NULL,10,'VERIFICATION_IDEMPOTENT_REPAIR'
  );
  IF COALESCE((v_result->>'repaired_candidate_count')::integer,-1)<>0
     OR COALESCE((v_result->>'terminalised_replay_job_count')::integer,-1)<>0 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REPLAYED_CANDIDATE_REPAIR_NOT_IDEMPOTENT',DETAIL=v_result::text;
  END IF;

  RAISE NOTICE 'PASS: replacement candidate replay uses canonical current owners, repairs persisted raw replay jobs and defers unseeded scope to the active target root.';
END;
$verification$;

ROLLBACK;
