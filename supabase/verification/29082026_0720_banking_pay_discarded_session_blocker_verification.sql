\set ON_ERROR_STOP on

-- Rollback-contained first-use proof. A source-build job from a discarded
-- Workbench session may no longer block the current session's canonical owner.
-- Open-session work and all financial/Draft authorities remain untouched.
BEGIN;
SET LOCAL statement_timeout='30s';

DO $verification$
DECLARE
  v_snapshot_id uuid:=gen_random_uuid();
  v_actor_id uuid:=gen_random_uuid();
  v_old_session_id uuid:=gen_random_uuid();
  v_current_session_id uuid:=gen_random_uuid();
  v_other_open_session_id uuid:=gen_random_uuid();
  v_candidate_id uuid:=gen_random_uuid();
  v_open_candidate_id uuid:=gen_random_uuid();
  v_old_job_id uuid:=gen_random_uuid();
  v_current_job_id uuid:=gen_random_uuid();
  v_open_job_id uuid:=gen_random_uuid();
  v_build_id uuid:=gen_random_uuid();
  v_attempt_id uuid:=gen_random_uuid();
  v_source_run_id uuid:=gen_random_uuid();
  v_started_at_utc timestamptz:=clock_timestamp()-interval '2 days';
  v_lease_expires_at_utc timestamptz:=v_started_at_utc+interval '1 day';
  v_result jsonb;
  v_state jsonb;
  v_row record;
  v_definition text;
  v_prefix text:='BANKING_PAY_DISCARDED_BLOCKER_VERIFY:' || gen_random_uuid()::text;
BEGIN
  SELECT pg_get_functiondef(
    'public.pay_workbench_repair_discarded_session_blockers_v1(uuid,uuid,integer,text)'::regprocedure
  ) INTO STRICT v_definition;
  IF v_definition ~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' THEN
    RAISE EXCEPTION 'BANKING_PAY_DISCARDED_BLOCKER_ILLEGAL_CONDITIONAL_PREFIX';
  END IF;

  IF has_function_privilege('anon',
       'public.pay_workbench_repair_discarded_session_blockers_v1(uuid,uuid,integer,text)',
       'EXECUTE')
     OR has_function_privilege('authenticated',
       'public.pay_workbench_repair_discarded_session_blockers_v1(uuid,uuid,integer,text)',
       'EXECUTE')
     OR NOT has_function_privilege('service_role',
       'public.pay_workbench_repair_discarded_session_blockers_v1(uuid,uuid,integer,text)',
       'EXECUTE') THEN
    RAISE EXCEPTION 'BANKING_PAY_DISCARDED_BLOCKER_ACL_INVALID';
  END IF;

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-02-13',DATE '2099-02-08',DATE '2099-02-02',
    DATE '2099-01-01',DATE '2099-02-08','OPEN',false
  );

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (v_actor_id,
    'bpay-discarded-' || replace(v_actor_id::text,'-','') || '@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true);

  INSERT INTO public.candidates(id,display_name,tms_ref) VALUES
    (v_candidate_id,v_prefix || ':CURRENT',v_prefix || ':CURRENT'),
    (v_open_candidate_id,v_prefix || ':OPEN',v_prefix || ':OPEN');

  INSERT INTO public.app_change_counters(entity_key,seq) VALUES
    ('pay_candidate:' || v_candidate_id::text,7),
    ('pay_candidate:' || v_open_candidate_id::text,5)
  ON CONFLICT(entity_key) DO UPDATE SET seq=EXCLUDED.seq;

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version,discarded_at_utc
  ) VALUES
    (v_old_session_id,v_actor_id,DATE '2099-02-13',DATE '2099-02-08',
      v_prefix || ':DISCARDED',v_snapshot_id,'DISCARDED',1,clock_timestamp()),
    (v_current_session_id,v_actor_id,DATE '2099-02-13',DATE '2099-02-08',
      v_prefix || ':CURRENT',v_snapshot_id,'OPEN',2,NULL),
    (v_other_open_session_id,v_actor_id,DATE '2099-02-13',DATE '2099-02-08',
      v_prefix || ':OTHER_OPEN',v_snapshot_id,'OPEN',3,NULL);

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty
  ) VALUES
    (v_current_session_id,v_candidate_id,1,'SOURCE_BUILD_PENDING',true,true),
    (v_current_session_id,v_open_candidate_id,2,'SOURCE_BUILD_PENDING',true,true);

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version,
    created_at_utc,updated_at_utc
  ) VALUES
    (v_old_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
      v_started_at_utc,0,8,v_prefix || ':OLD_JOB',v_snapshot_id,
      v_old_session_id,v_candidate_id,jsonb_build_object(
        'session_id',v_old_session_id::text,'session_version',1,
        'source_change_seq',6,'source_build_run_id',v_source_run_id::text,
        'run_mode','BOUNDED_CONTINUATION','continuation',true
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,
      v_started_at_utc,v_started_at_utc),
    (v_current_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
      clock_timestamp(),0,8,v_prefix || ':CURRENT_JOB',v_snapshot_id,
      v_current_session_id,v_candidate_id,jsonb_build_object(
        'session_id',v_current_session_id::text,'session_version',2,
        'source_change_seq',7,'source_build_run_id',gen_random_uuid()::text
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,
      clock_timestamp(),clock_timestamp()),
    (v_open_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
      clock_timestamp(),0,8,v_prefix || ':OPEN_JOB',v_snapshot_id,
      v_other_open_session_id,v_open_candidate_id,jsonb_build_object(
        'session_id',v_other_open_session_id::text,'session_version',3,
        'source_change_seq',5,'source_build_run_id',gen_random_uuid()::text
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,
      clock_timestamp(),clock_timestamp());

  UPDATE public.banking_pay_workbench_session_scope
  SET pending_job_id=CASE candidate_id
      WHEN v_candidate_id THEN v_current_job_id ELSE v_open_job_id END
  WHERE session_id=v_current_session_id
    AND candidate_id IN (v_candidate_id,v_open_candidate_id);

  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,stage_version,created_at_utc,updated_at_utc
  ) VALUES (
    v_build_id,v_candidate_id,v_old_session_id,1,v_snapshot_id,v_source_run_id,
    v_old_job_id,0,6,'COLLECTING','DEPENDENCY_CLOSURE',1,
    v_started_at_utc,v_started_at_utc
  );

  UPDATE public.banking_pay_workbench_jobs
  SET status='RUNNING',attempt_count=1,economic_build_id=v_build_id,
    private_stage='DEPENDENCY_CLOSURE',private_cursor_kind='DEPENDENCY_CLOSURE',
    private_cursor_json=jsonb_build_object(
      'cursor_kind','DEPENDENCY_CLOSURE','cursor_version',1,
      'build_id',v_build_id::text,'candidate_id',v_candidate_id::text),
    private_stage_version=1,started_at_utc=v_started_at_utc
  WHERE id=v_old_job_id;

  INSERT INTO private.banking_pay_workbench_stage_attempts(
    id,job_id,build_id,candidate_id,private_stage,attempt_number,worker_id,
    lane_identity,captured_candidate_generation,captured_source_change_seq,
    execution_profile_version,attempt_status,started_at_utc,lease_expires_at_utc,
    created_at_utc,updated_at_utc
  ) VALUES (
    v_attempt_id,v_old_job_id,v_build_id,v_candidate_id,'DEPENDENCY_CLOSURE',1,
    'ROLLBACK_VERIFIER','ROLLBACK_VERIFIER_LANE',0,6,1,'STARTED',
    v_started_at_utc,v_lease_expires_at_utc,v_started_at_utc,v_started_at_utc
  );

  v_state:=public._pay_workbench_candidate_serial_active_state(
    v_current_job_id,v_candidate_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    (SELECT payload_json FROM public.banking_pay_workbench_jobs WHERE id=v_current_job_id),
    clock_timestamp()
  );
  IF COALESCE((v_state->>'blocked')::boolean,false) IS NOT TRUE
     OR (v_state->>'blocked_job_id')::uuid IS DISTINCT FROM v_old_job_id THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DISCARDED_BLOCKER_FIXTURE_NOT_REPRODUCED',
      DETAIL=v_state::text;
  END IF;

  v_result:=public.pay_workbench_repair_discarded_session_blockers_v1(
    v_current_session_id,v_candidate_id,1,'ROLLBACK_FIRST_USE_VERIFICATION'
  );
  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_result->>'repaired_candidate_count')::integer,-1)<>1
     OR COALESCE((v_result->>'terminalised_job_count')::integer,-1)<>1
     OR COALESCE((v_result->>'terminalised_attempt_count')::integer,-1)<>1
     OR COALESCE((v_result->>'obsoleted_build_count')::integer,-1)<>1 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DISCARDED_BLOCKER_RESULT_INVALID',
      DETAIL=v_result::text;
  END IF;

  SELECT job.status AS job_status,job.last_error_json->>'code' AS error_code,
    attempt.attempt_status,attempt.result_code,build.status AS build_status
  INTO STRICT v_row
  FROM public.banking_pay_workbench_jobs AS job
  JOIN private.banking_pay_workbench_stage_attempts AS attempt
    ON attempt.job_id=job.id
  JOIN private.banking_pay_workbench_economic_builds AS build
    ON build.id=job.economic_build_id
  WHERE job.id=v_old_job_id;
  IF v_row.job_status IS DISTINCT FROM 'DEAD'
     OR v_row.error_code IS DISTINCT FROM 'DISCARDED_SESSION_SOURCE_BUILD_TERMINALISED'
     OR v_row.attempt_status IS DISTINCT FROM 'OBSOLETE'
     OR v_row.result_code IS DISTINCT FROM 'DISCARDED_SESSION_SOURCE_BUILD_TERMINALISED'
     OR v_row.build_status IS DISTINCT FROM 'OBSOLETE' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DISCARDED_BLOCKER_TERMINAL_SHAPE_INVALID',
      DETAIL=to_jsonb(v_row)::text;
  END IF;

  SELECT scope_row.status,scope_row.pending_job_id,owner_job.status AS owner_status
  INTO STRICT v_row
  FROM public.banking_pay_workbench_session_scope AS scope_row
  JOIN public.banking_pay_workbench_jobs AS owner_job
    ON owner_job.id=scope_row.pending_job_id
  WHERE scope_row.session_id=v_current_session_id
    AND scope_row.candidate_id=v_candidate_id;
  IF v_row.status IS DISTINCT FROM 'SOURCE_BUILD_PENDING'
     OR v_row.pending_job_id IS DISTINCT FROM v_current_job_id
     OR v_row.owner_status IS DISTINCT FROM 'QUEUED' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_CURRENT_OWNER_CHANGED',DETAIL=to_jsonb(v_row)::text;
  END IF;

  v_state:=public._pay_workbench_candidate_serial_active_state(
    v_current_job_id,v_candidate_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    (SELECT payload_json FROM public.banking_pay_workbench_jobs WHERE id=v_current_job_id),
    clock_timestamp()
  );
  IF COALESCE((v_state->>'blocked')::boolean,true) IS TRUE THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_CURRENT_OWNER_STILL_BLOCKED',DETAIL=v_state::text;
  END IF;

  v_result:=public.pay_workbench_repair_discarded_session_blockers_v1(
    v_current_session_id,v_candidate_id,1,'ROLLBACK_IDEMPOTENCY_VERIFICATION'
  );
  IF COALESCE((v_result->>'repaired_candidate_count')::integer,-1)<>0
     OR COALESCE((v_result->>'terminalised_job_count')::integer,-1)<>0 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DISCARDED_BLOCKER_REPAIR_NOT_IDEMPOTENT',
      DETAIL=v_result::text;
  END IF;

  v_result:=public.pay_workbench_repair_discarded_session_blockers_v1(
    v_current_session_id,v_open_candidate_id,1,'ROLLBACK_OPEN_SESSION_NEGATIVE'
  );
  IF COALESCE((v_result->>'repaired_candidate_count')::integer,-1)<>0
     OR (SELECT status FROM public.banking_pay_workbench_jobs WHERE id=v_open_job_id)
        IS DISTINCT FROM 'QUEUED' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_OPEN_SESSION_WORK_WAS_TOUCHED',DETAIL=v_result::text;
  END IF;

  RAISE NOTICE 'PASS: discarded-session source-build blocker is terminalised; current and open-session owners remain intact; first use and idempotency pass.';
END;
$verification$;

ROLLBACK;
