-- Banking Pay bounded-scope V1.2.4: exact-nonce transaction-two execute RPC.

CREATE OR REPLACE FUNCTION public.pay_workbench_source_build_attempt_execute_v1(
  p_job_id uuid,
  p_build_id uuid,
  p_private_stage text,
  p_attempt_id uuid,
  p_attempt_nonce uuid,
  p_worker_id text,
  p_lane_identity text
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_stage text := upper(NULLIF(btrim(COALESCE(p_private_stage,'')),''));
  v_worker_id text := NULLIF(btrim(COALESCE(p_worker_id,'')),'');
  v_lane_identity text := NULLIF(btrim(COALESCE(p_lane_identity,'')),'');
  v_candidate_id uuid;
  v_lock_key bigint;
  v_candidate_lock_acquired boolean:=false;
  v_candidate_lock_wait_started_at timestamptz;
  v_candidate_lock_wait_limit interval:=interval '750 milliseconds';
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_attempt private.banking_pay_workbench_stage_attempts%ROWTYPE;
  v_stage_result jsonb := '{}'::jsonb;
  v_completion_result jsonb := '{}'::jsonb;
  v_failure_result jsonb := '{}'::jsonb;
  v_error_json jsonb := '{}'::jsonb;
  v_elapsed_ms integer;
  v_result_code text;
  v_execution_profile_version integer:=1;
  v_call_cursor jsonb:='{}'::jsonb;
  v_call_started_at timestamptz;
  v_call_elapsed_ms integer:=0;
  v_microsteps integer:=0;
  v_fact_page_records integer:=0;
  v_zero_fact_page_records integer:=0;
  v_physical_fact_rows integer:=0;
  v_before_token text;
  v_after_token text;
  v_previous_stage text;
BEGIN
  IF p_job_id IS NULL OR p_build_id IS NULL OR p_attempt_id IS NULL
     OR p_attempt_nonce IS NULL OR v_stage IS NULL
     OR v_worker_id IS NULL OR v_lane_identity IS NULL
     OR char_length(v_worker_id)>200 OR char_length(v_lane_identity)>200 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_EXECUTE_INPUT_INVALID'
      USING ERRCODE='22023',DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_ATTEMPT_EXECUTE_INPUT_INVALID'
      )::text;
  END IF;

  SELECT attempt_row.candidate_id INTO v_candidate_id
  FROM private.banking_pay_workbench_stage_attempts AS attempt_row
  WHERE attempt_row.id=p_attempt_id
    AND attempt_row.job_id=p_job_id
    AND attempt_row.build_id=p_build_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok',true,'processed',false,'job_id',p_job_id,'build_id',p_build_id,
      'private_stage',v_stage,'result_code','ATTEMPT_NOT_FOUND','elapsed_ms',0
    );
  END IF;

  v_lock_key := hashtextextended(
    public._pay_workbench_candidate_serial_key(v_candidate_id),24062027
  );
  v_candidate_lock_wait_started_at:=clock_timestamp();
  LOOP
    v_candidate_lock_acquired:=pg_catalog.pg_try_advisory_xact_lock(v_lock_key);
    EXIT WHEN v_candidate_lock_acquired;
    EXIT WHEN clock_timestamp()>=v_candidate_lock_wait_started_at+v_candidate_lock_wait_limit;
    PERFORM pg_catalog.pg_sleep(0.01);
  END LOOP;
  IF NOT v_candidate_lock_acquired THEN
    RETURN jsonb_build_object(
      'ok',true,'processed',false,'job_id',p_job_id,'build_id',p_build_id,
      'private_stage',v_stage,'result_code','CANDIDATE_LOCK_BUSY',
      'candidate_lock_wait_ms',GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_candidate_lock_wait_started_at)*1000)::integer),
      'elapsed_ms',GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_started_at)*1000)::integer)
    );
  END IF;

  -- Global order: candidate, registry, build, job, attempt.
  PERFORM 1 FROM public.candidates AS candidate_row
  WHERE candidate_row.id=v_candidate_id FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok',true,'processed',false,'job_id',p_job_id,'build_id',p_build_id,
      'private_stage',v_stage,'result_code','CANDIDATE_DELETED','elapsed_ms',0
    );
  END IF;

  SELECT * INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  WHERE registry.candidate_id=v_candidate_id FOR UPDATE;
  SELECT * INTO v_build
  FROM private.banking_pay_workbench_economic_builds AS build_row
  WHERE build_row.id=p_build_id FOR UPDATE;
  SELECT * INTO v_job
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.id=p_job_id FOR UPDATE;
  SELECT * INTO v_attempt
  FROM private.banking_pay_workbench_stage_attempts AS attempt_row
  WHERE attempt_row.id=p_attempt_id FOR UPDATE;

  IF v_registry.candidate_id IS NULL OR v_build.id IS NULL
     OR v_job.id IS NULL OR v_attempt.id IS NULL
     OR v_attempt.attempt_nonce IS DISTINCT FROM p_attempt_nonce
     OR v_attempt.attempt_status <> 'STARTED'
     OR v_attempt.job_id <> p_job_id OR v_attempt.build_id <> p_build_id
     OR v_attempt.candidate_id <> v_candidate_id
     OR v_attempt.private_stage <> v_stage
     OR v_attempt.worker_id <> v_worker_id
     OR v_attempt.lane_identity <> v_lane_identity
     OR v_job.status <> 'RUNNING'
     OR v_job.job_type <> 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
     OR v_job.economic_build_id IS DISTINCT FROM p_build_id
     OR v_job.private_stage IS DISTINCT FROM v_stage
     OR v_build.candidate_id <> v_candidate_id
     OR v_build.source_job_id IS DISTINCT FROM p_job_id THEN
    RETURN jsonb_build_object(
      'ok',true,'processed',false,'job_id',p_job_id,'build_id',p_build_id,
      'private_stage',v_stage,'result_code','ATTEMPT_STALE_OR_SUPERSEDED',
      'elapsed_ms',GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_started_at)*1000)::integer)
    );
  END IF;

  IF clock_timestamp() >= v_attempt.lease_expires_at_utc
     OR v_registry.dirty_generation <> v_attempt.captured_candidate_generation
     OR v_registry.current_source_change_seq <> v_attempt.captured_source_change_seq
     OR v_registry.current_build_id IS DISTINCT FROM p_build_id
     OR v_build.captured_candidate_generation <> v_attempt.captured_candidate_generation
     OR v_build.source_change_seq <> v_attempt.captured_source_change_seq THEN
    RETURN jsonb_build_object(
      'ok',true,'processed',false,'job_id',p_job_id,'build_id',p_build_id,
      'private_stage',v_stage,
      'result_code',CASE WHEN clock_timestamp() >= v_attempt.lease_expires_at_utc
        THEN 'ATTEMPT_EXPIRED' ELSE 'BUILD_GENERATION_STALE' END,
      'elapsed_ms',GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_started_at)*1000)::integer)
    );
  END IF;

  BEGIN
    v_execution_profile_version:=COALESCE(
      NULLIF(v_build.attestation_json->>'execution_profile_version','')::integer,
      1
    );
  EXCEPTION WHEN OTHERS THEN
    v_execution_profile_version:=0;
  END;
  IF v_execution_profile_version NOT IN (1,2)
     OR v_attempt.execution_profile_version IS DISTINCT FROM v_execution_profile_version THEN
    RETURN jsonb_build_object(
      'ok',true,'processed',false,'job_id',p_job_id,'build_id',p_build_id,
      'private_stage',v_stage,'result_code','EXECUTION_PROFILE_CONFLICT',
      'elapsed_ms',GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_started_at)*1000)::integer)
    );
  END IF;
  PERFORM pg_catalog.set_config(
    'cloudtms.pay_workbench_execution_profile_version',
    v_execution_profile_version::text,
    true
  );

  IF v_stage='RECONCILE_EXECUTE' THEN
    IF to_regclass('pg_temp._bpay_wb_sync_context_v1') IS NOT NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_CONTEXT_PREEXISTS'
        USING ERRCODE='42501';
    END IF;
    CREATE TEMP TABLE pg_temp._bpay_wb_sync_context_v1(
      job_id uuid NOT NULL,build_id uuid NOT NULL,attempt_id uuid NOT NULL,
      attempt_nonce uuid NOT NULL,build_token uuid NOT NULL,candidate_id uuid NOT NULL,
      private_stage text NOT NULL,captured_generation bigint NOT NULL,
      source_change_seq bigint NOT NULL,created_at_utc timestamptz NOT NULL
    ) ON COMMIT DROP;
    INSERT INTO pg_temp._bpay_wb_sync_context_v1 VALUES(
      p_job_id,p_build_id,p_attempt_id,p_attempt_nonce,v_build.build_token,v_candidate_id,
      v_stage,v_attempt.captured_candidate_generation,v_attempt.captured_source_change_seq,
      clock_timestamp()
    );
  END IF;

  v_call_cursor:=COALESCE(v_job.private_cursor_json,'{}'::jsonb);
  BEGIN
    LOOP
      v_previous_stage:=COALESCE(v_stage_result->>'private_stage',v_stage);
      v_before_token:=pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
        pg_catalog.concat_ws('|',v_previous_stage,v_call_cursor::text,
          v_fact_page_records::text,v_physical_fact_rows::text),'UTF8'
      ),'sha256'),'hex');
      v_call_started_at:=clock_timestamp();
      v_stage_result := public.pay_workbench_candidate_source_build_chunk(
        p_session_id=>v_job.session_id,
        p_candidate_id=>v_candidate_id,
        p_cursor_json=>v_call_cursor,
        p_payload_json=>COALESCE(v_job.payload_json,'{}'::jsonb) || jsonb_build_object(
          'economic_build_id',p_build_id,'attempt_id',p_attempt_id,
          'attempt_nonce',p_attempt_nonce,'private_stage',v_stage,
          'bounded_scope_runtime_version',1,
          'execution_profile_version',v_execution_profile_version
        ),
        p_limit=>COALESCE((SELECT settings_row.banking_pay_workbench_stage_work_units_per_job
          FROM public.settings_defaults AS settings_row WHERE settings_row.id=1),25)
      );
      v_microsteps:=v_microsteps+1;
      v_call_elapsed_ms:=GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_call_started_at)*1000)::integer);

      SELECT count(*)::integer,
             count(*) FILTER(WHERE fact_page.actual_fact_count=0)::integer,
             COALESCE(sum(fact_page.actual_fact_count),0)::integer
      INTO v_fact_page_records,v_zero_fact_page_records,v_physical_fact_rows
      FROM private.banking_pay_workbench_economic_build_fact_pages AS fact_page
      WHERE fact_page.attempt_id=p_attempt_id;

      v_after_token:=pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
        pg_catalog.concat_ws('|',COALESCE(v_stage_result->>'private_stage',''),
          COALESCE(v_stage_result->'next_cursor_json','{}'::jsonb)::text,
          v_fact_page_records::text,v_physical_fact_rows::text),'UTF8'
      ),'sha256'),'hex');
      IF v_before_token=v_after_token
         AND COALESCE((v_stage_result->>'has_more')::boolean,false) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_MATERIAL_ATTEMPT_NO_PROGRESS'
          USING ERRCODE='P0001',DETAIL=jsonb_build_object(
            'code','PAY_WORKBENCH_MATERIAL_ATTEMPT_NO_PROGRESS','attempt_id',p_attempt_id
          )::text;
      END IF;

      EXIT WHEN v_execution_profile_version=1;
      EXIT WHEN v_stage NOT IN ('PREPARE_SCOPE','DEPENDENCY_CLOSURE','WORKSPACE_FACT');
      EXIT WHEN COALESCE((v_stage_result->>'has_more')::boolean,false) IS NOT TRUE;
      EXIT WHEN COALESCE(v_stage_result->>'private_stage','') IS DISTINCT FROM v_stage;
      EXIT WHEN v_microsteps>=16 OR v_fact_page_records>=64
        OR v_zero_fact_page_records>=64 OR v_physical_fact_rows>=400;
      EXIT WHEN clock_timestamp()>=v_started_at+interval '3.5 seconds';
      EXIT WHEN clock_timestamp()>=v_attempt.lease_expires_at_utc-interval '10 seconds';
      EXIT WHEN v_call_elapsed_ms>750;
      v_call_cursor:=COALESCE(v_stage_result->'next_cursor_json','{}'::jsonb);
    END LOOP;

    v_stage_result:=v_stage_result||jsonb_build_object(
      'execution_profile_version',v_execution_profile_version,
      'material_microsteps',v_microsteps,
      'fact_page_records',v_fact_page_records,
      'zero_fact_page_records',v_zero_fact_page_records,
      'physical_fact_rows',v_physical_fact_rows,
      'material_budget_stop_reason',CASE
        WHEN v_execution_profile_version=1 THEN 'PROFILE_1_SINGLE_STEP'
        WHEN COALESCE((v_stage_result->>'has_more')::boolean,false) IS NOT TRUE THEN 'STAGE_COMPLETE'
        WHEN COALESCE(v_stage_result->>'private_stage','') IS DISTINCT FROM v_stage THEN 'STAGE_CHANGED'
        WHEN v_microsteps>=16 THEN 'MICROSTEP_CAP'
        WHEN v_fact_page_records>=64 THEN 'PAGE_LEDGER_CAP'
        WHEN v_zero_fact_page_records>=64 THEN 'EMPTY_LEDGER_CAP'
        WHEN v_physical_fact_rows>=400 THEN 'PHYSICAL_ROW_CAP'
        WHEN clock_timestamp()>=v_attempt.lease_expires_at_utc-interval '10 seconds' THEN 'LEASE_RESERVE'
        WHEN v_call_elapsed_ms>750 THEN 'SLOW_STEP_GUARD'
        ELSE 'SOFT_TIME_BUDGET'
      END
    );
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_result_code=RETURNED_SQLSTATE;
    v_error_json := jsonb_build_object(
      'code',v_result_code,'message',SQLERRM,'private_stage',v_stage,
      'build_id',p_build_id,'attempt_id',p_attempt_id
    );
    -- The inner stage subtransaction has rolled back.  fail_job owns the one
    -- durable STARTED -> FAILED/retry transition in this outer transaction.
    v_failure_result := public.pay_workbench_fail_job(
      p_job_id=>p_job_id,p_error_json=>v_error_json,p_retry_after_seconds=>NULL
    );
    v_elapsed_ms := GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_started_at)*1000)::integer);
    RETURN jsonb_build_object(
      'ok',false,'processed',true,'job_id',p_job_id,'build_id',p_build_id,
      'private_stage',v_stage,'attempt_number',v_attempt.attempt_number,
      'stage_status','FAILED','continuation_enqueued',false,'has_more',false,
      'next_cursor_json','{}'::jsonb,'result_code','STAGE_ERROR',
      'elapsed_ms',v_elapsed_ms,'failure_result',v_failure_result
    );
  END;

  -- Final lease/generation/nonce fence. Failure raises so all stage writes roll
  -- back; RPC 1's committed attempt remains recoverable.
  IF clock_timestamp() >= v_attempt.lease_expires_at_utc-interval '500 milliseconds'
     OR NOT EXISTS (
       SELECT 1
       FROM private.banking_pay_workbench_stage_attempts AS final_attempt
       JOIN private.banking_pay_workbench_candidate_scope_registry AS final_registry
         ON final_registry.candidate_id=final_attempt.candidate_id
       JOIN private.banking_pay_workbench_economic_builds AS final_build
         ON final_build.id=final_attempt.build_id
       WHERE final_attempt.id=p_attempt_id
         AND final_attempt.attempt_nonce=p_attempt_nonce
         AND final_attempt.attempt_status='STARTED'
         AND final_registry.dirty_generation=final_attempt.captured_candidate_generation
         AND final_registry.current_source_change_seq=final_attempt.captured_source_change_seq
         AND final_build.source_job_id=p_job_id
         AND (
           -- Most material stages keep the build current until complete_job
           -- advances the durable continuation.  A small reconciliation can
           -- also publish atomically in this same RPC 2 transaction.  In that
           -- terminal path SOURCE_PUBLISH deliberately clears current_build_id
           -- only after the registry and build have both reached their complete
           -- generation-fenced states.  Accept that exact terminal authority;
           -- do not mistake successful publication for a stale build.
           final_registry.current_build_id=p_build_id
           OR (
             COALESCE(v_stage_result->>'private_stage','')='COMPLETE'
             AND COALESCE(v_stage_result->>'stage_status','')='COMPLETE'
             AND COALESCE((v_stage_result->>'has_more')::boolean,false)=false
             AND final_registry.current_build_id IS NULL
             AND final_registry.initialisation_status='READY'
             AND final_registry.evaluated_generation=final_attempt.captured_candidate_generation
             AND final_build.status='COMPLETE'
             AND final_build.private_stage='COMPLETE'
             AND final_build.completed_at_utc IS NOT NULL
           )
         )
     ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_FINAL_FENCE_FAILED'
      USING ERRCODE='40001',DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_ATTEMPT_FINAL_FENCE_FAILED','attempt_id',p_attempt_id
      )::text;
  END IF;

  UPDATE private.banking_pay_workbench_stage_attempts
  SET attempt_status='COMPLETED',completed_at_utc=clock_timestamp(),
      result_code=COALESCE(v_stage_result->>'result_code','STAGE_COMPLETED'),
      result_digest=md5(v_stage_result::text),updated_at_utc=clock_timestamp()
  WHERE id=p_attempt_id AND attempt_nonce=p_attempt_nonce AND attempt_status='STARTED';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_FINAL_FENCE_FAILED' USING ERRCODE='40001';
  END IF;

  v_completion_result := public.pay_workbench_complete_job(p_job_id,v_stage_result);
  v_elapsed_ms := GREATEST(0,round(extract(epoch FROM clock_timestamp()-v_started_at)*1000)::integer);
  RETURN jsonb_build_object(
    'ok',true,'processed',true,'job_id',p_job_id,'build_id',p_build_id,
    'private_stage',v_stage,'attempt_number',v_attempt.attempt_number,
    'stage_status',COALESCE(v_stage_result->>'stage_status','COMPLETED'),
    'continuation_enqueued',COALESCE((v_completion_result->>'continuation_enqueued')::boolean,false),
    'has_more',COALESCE((v_stage_result->>'has_more')::boolean,false),
    'next_cursor_json',COALESCE(v_stage_result->'next_cursor_json','{}'::jsonb),
    'result_code',COALESCE(v_stage_result->>'result_code','STAGE_COMPLETED'),
    'execution_profile_version',v_execution_profile_version,
    'material_microsteps',v_microsteps,
    'elapsed_ms',v_elapsed_ms
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_source_build_attempt_execute_v1(uuid,uuid,text,uuid,uuid,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_source_build_attempt_execute_v1(uuid,uuid,text,uuid,uuid,text,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_source_build_attempt_execute_v1(uuid,uuid,text,uuid,uuid,text,text) TO service_role;
