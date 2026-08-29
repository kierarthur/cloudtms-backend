-- Rollback-contained first-use proof for Banking Pay version-one refresh.
-- No Draft, provider, payment, settlement, remittance, cancellation or
-- communications operation is created or executed.

\set ON_ERROR_STOP on

begin;

DO $verify$
DECLARE
  v_actor_id constant uuid := '12000000-0000-4000-8000-000000000001';
  v_candidate_id constant uuid := '12000000-0000-4000-8000-000000000002';
  v_snapshot_id constant uuid := '12000000-0000-4000-8000-000000000003';
  v_session_id constant uuid := '12000000-0000-4000-8000-000000000004';
  v_result jsonb := '{}'::jsonb;
  v_replay jsonb := '{}'::jsonb;
  v_route jsonb := '{}'::jsonb;
  v_definition text := '';
  v_job_count integer := 0;
BEGIN
  IF EXISTS (SELECT 1 FROM public.tms_users WHERE id = v_actor_id)
     OR EXISTS (SELECT 1 FROM public.candidates WHERE id = v_candidate_id)
     OR EXISTS (SELECT 1 FROM public.banking_pay_snapshot_runs WHERE id = v_snapshot_id)
     OR EXISTS (SELECT 1 FROM public.banking_pay_workbench_sessions WHERE id = v_session_id) THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_FIXTURE_COLLISION';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer)'::regprocedure
  ) INTO v_definition;

  IF pg_catalog.strpos(v_definition,'IF v_session.version = 1 THEN') = 0
     OR pg_catalog.strpos(v_definition,'SESSION_VERSION_ONE_HAS_NO_PREVIOUS_VERSION') = 0
     OR pg_catalog.strpos(v_definition,'private.pay_workbench_candidate_session_version_rebase_v1') = 0
     OR pg_catalog.strpos(v_definition,'v_session.version - 1') = 0
     OR pg_catalog.strpos(v_definition,'pay_workbench_enqueue_session_candidate_refresh') = 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_SOURCE_CONTRACT_MISSING';
  END IF;

  IF v_definition ~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_ILLEGAL_CONDITIONAL_PREFIX';
  END IF;

  IF pg_catalog.has_function_privilege(
       'anon',
       'public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer)',
       'EXECUTE'
     )
     OR pg_catalog.has_function_privilege(
       'authenticated',
       'public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer)',
       'EXECUTE'
     )
     OR NOT pg_catalog.has_function_privilege(
       'service_role',
       'public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer)',
       'EXECUTE'
     ) THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_ACL_INVALID';
  END IF;

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (v_actor_id,'version-one-refresh@example.invalid','UNUSABLE_ROLLBACK_FIXTURE','admin',true);

  INSERT INTO public.candidates(id,display_name,tms_ref)
  VALUES (v_candidate_id,'Version one refresh fixture','VERSION-ONE');

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,
    eligibility_from_date,eligibility_to_date
  ) VALUES (
    v_snapshot_id,'2026-08-29','2026-08-30','2026-08-24',
    '2026-08-01','2026-08-31'
  );

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,version,progress_counter_version,progress_json
  ) VALUES (
    v_session_id,v_actor_id,'2026-08-29','2026-08-30',
    'version-one-refresh-rollback-fixture',v_snapshot_id,1,1,'{}'::jsonb
  );

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,
    certified_preview_publication_attestation_json
  ) VALUES (
    v_session_id,v_candidate_id,1,'SOURCE_BUILD_PENDING',true,true,'{}'::jsonb
  );

  v_result := public.pay_workbench_session_refresh_current_authority_v1(
    v_session_id,v_actor_id,'{}'::jsonb,100
  );
  v_route := COALESCE(v_result->'route_results'->0,'{}'::jsonb);

  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR (v_result->>'candidate_count')::integer <> 1
     OR (v_result->>'enqueued_candidate_count')::integer <> 1
     OR (v_result->>'work_candidate_count')::integer <> 1
     OR (v_result->>'version_rebased_count')::integer <> 0
     OR COALESCE(v_route->>'route','') <> 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
     OR COALESCE(v_route->'version_rebase_result'->>'reason','')
          <> 'SESSION_VERSION_ONE_HAS_NO_PREVIOUS_VERSION'
     OR COALESCE(v_result->>'policy_x_scope','') <> 'PRE_DRAFT_LIVE_TRUTH' THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_FIRST_USE_FAILED:%',v_result;
  END IF;

  SELECT pg_catalog.count(*)::integer INTO v_job_count
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.session_id = v_session_id
    AND job_row.candidate_id = v_candidate_id
    AND job_row.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    AND job_row.status = 'QUEUED';
  IF v_job_count <> 1 THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_JOB_NOT_CANONICAL:%',v_job_count;
  END IF;

  v_replay := public.pay_workbench_session_refresh_current_authority_v1(
    v_session_id,v_actor_id,'{}'::jsonb,100
  );
  v_route := COALESCE(v_replay->'route_results'->0,'{}'::jsonb);
  SELECT pg_catalog.count(*)::integer INTO v_job_count
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.session_id = v_session_id
    AND job_row.candidate_id = v_candidate_id
    AND job_row.status IN ('QUEUED','RUNNING');

  IF COALESCE((v_replay->>'ok')::boolean,false) IS NOT TRUE
     OR (v_replay->>'candidate_count')::integer <> 1
     OR (v_replay->>'enqueued_candidate_count')::integer <> 1
     OR COALESCE((v_route->'enqueue_result'->'enqueue_result'->>'queued_count')::integer,-1) <> 0
     OR COALESCE((v_route->'enqueue_result'->'enqueue_result'->>'reused_count')::integer,-1) <> 1
     OR v_job_count <> 1 THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_REPLAY_NOT_IDEMPOTENT:%',v_replay;
  END IF;

  IF EXISTS (
    SELECT 1 FROM public.pay_batches AS batch_row
    WHERE batch_row.id IN (v_session_id,v_snapshot_id,v_candidate_id,v_actor_id)
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_VERSION_ONE_REFRESH_CREATED_DRAFT';
  END IF;
END;
$verify$;

rollback;
