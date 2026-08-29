\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the two candidate-source terminal
-- failures observed in the real TEST Banking Pay refresh. Withdrawn history
-- is not a current booking-family member, while every non-withdrawn identity
-- still needs the existing financial owner. A genuinely empty, fully sealed
-- candidate build can advance to the existing zero-line publication stage
-- without any Draft, provider or payment mutation.
BEGIN;
SET LOCAL statement_timeout='30s';

DO $verification$
DECLARE
  v_snapshot_id uuid:=gen_random_uuid();
  v_actor_id uuid:=gen_random_uuid();
  v_session_id uuid:=gen_random_uuid();
  v_dependency_candidate_id uuid:=gen_random_uuid();
  v_empty_candidate_id uuid:=gen_random_uuid();
  v_dependency_job_id uuid:=gen_random_uuid();
  v_empty_job_id uuid:=gen_random_uuid();
  v_dependency_build_id uuid:=gen_random_uuid();
  v_empty_build_id uuid:=gen_random_uuid();
  v_empty_attempt_id uuid:=gen_random_uuid();
  v_empty_attempt_nonce uuid:=gen_random_uuid();
  v_dependency_run_id uuid:=gen_random_uuid();
  v_empty_run_id uuid:=gen_random_uuid();
  v_root_timesheet_id uuid:=gen_random_uuid();
  v_revoked_timesheet_id uuid:=gen_random_uuid();
  v_active_unowned_timesheet_id uuid:=gen_random_uuid();
  v_booking_id text:='BPAY-REFRESH-VERIFY-'||gen_random_uuid()::text;
  v_prefix text:='BANKING_PAY_SOURCE_REFRESH_VERIFY:'||gen_random_uuid()::text;
  v_result jsonb;
  v_capture_result jsonb;
  v_definition text;
  v_row record;
  v_case_count_before bigint;
  v_now timestamptz:=clock_timestamp();
BEGIN
  SELECT pg_get_functiondef(
    'private.pay_workbench_timesheet_dependency_closure_v2(uuid,jsonb,integer)'::regprocedure
  )||E'\n'||pg_get_functiondef(
    'private.pay_workbench_reconcile_empty_scope_v1(uuid,uuid,uuid,uuid)'::regprocedure
  )||E'\n'||pg_get_functiondef(
    'public.pay_sync_overpayments_from_preview(date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[])'::regprocedure
  ) INTO STRICT v_definition;
  IF v_definition ~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' THEN
    RAISE EXCEPTION 'BANKING_PAY_SOURCE_REFRESH_ILLEGAL_CONDITIONAL_PREFIX';
  END IF;

  IF has_function_privilege('anon',
       'private.pay_workbench_reconcile_empty_scope_v1(uuid,uuid,uuid,uuid)','EXECUTE')
     OR has_function_privilege('authenticated',
       'private.pay_workbench_reconcile_empty_scope_v1(uuid,uuid,uuid,uuid)','EXECUTE')
     OR has_function_privilege('service_role',
       'private.pay_workbench_reconcile_empty_scope_v1(uuid,uuid,uuid,uuid)','EXECUTE')
     OR NOT has_function_privilege('service_role',
       'public.pay_sync_overpayments_from_preview(date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[])',
       'EXECUTE') THEN
    RAISE EXCEPTION 'BANKING_PAY_SOURCE_REFRESH_ACL_INVALID';
  END IF;

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-03-06',DATE '2099-03-01',DATE '2099-02-23',
    DATE '2099-01-01',DATE '2099-03-01','OPEN',false
  );

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (v_actor_id,
    'bpay-source-refresh-'||replace(v_actor_id::text,'-','')||'@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true);

  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method) VALUES
    (v_dependency_candidate_id,v_prefix||':DEPENDENCY',v_prefix||':DEPENDENCY','PAYE'),
    (v_empty_candidate_id,v_prefix||':EMPTY',v_prefix||':EMPTY','PAYE');

  INSERT INTO public.app_change_counters(entity_key,seq) VALUES
    ('pay_candidate:'||v_dependency_candidate_id::text,1),
    ('pay_candidate:'||v_empty_candidate_id::text,1)
  ON CONFLICT(entity_key) DO UPDATE SET seq=EXCLUDED.seq;

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version
  ) VALUES (
    v_session_id,v_actor_id,DATE '2099-03-06',DATE '2099-03-01',
    v_prefix||':SESSION',v_snapshot_id,'OPEN',1
  );

  INSERT INTO private.banking_pay_workbench_candidate_scope_registry AS registry(
    candidate_id,initialisation_status,dirty_generation,evaluated_generation,
    current_source_change_seq,last_dirty_reason,last_evaluated_at_utc,
    initialised_at_utc,last_dirtied_at_utc,created_at_utc,updated_at_utc
  ) VALUES
    (v_dependency_candidate_id,'READY',1,1,1,'ROLLBACK_VERIFICATION',
      v_now,v_now,v_now,v_now,v_now),
    (v_empty_candidate_id,'READY',1,1,1,'ROLLBACK_VERIFICATION',
      v_now,v_now,v_now,v_now,v_now)
  ON CONFLICT(candidate_id) DO UPDATE SET
    initialisation_status='READY',dirty_generation=1,evaluated_generation=1,
    current_source_change_seq=1,current_build_id=NULL,
    last_dirty_reason='ROLLBACK_VERIFICATION',last_evaluated_at_utc=clock_timestamp(),
    initialised_at_utc=COALESCE(
      registry.initialised_at_utc,
      clock_timestamp()),failure_json='{}'::jsonb,updated_at_utc=clock_timestamp();

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version,
    created_at_utc,updated_at_utc
  ) VALUES
    (v_dependency_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
      clock_timestamp(),0,8,v_prefix||':DEPENDENCY_JOB',v_snapshot_id,
      v_session_id,v_dependency_candidate_id,jsonb_build_object(
        'session_id',v_session_id,'session_version',1,'source_change_seq',1,
        'source_build_run_id',v_dependency_run_id
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,
      clock_timestamp(),clock_timestamp()),
    (v_empty_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,
      clock_timestamp(),0,8,v_prefix||':EMPTY_JOB',v_snapshot_id,
      v_session_id,v_empty_candidate_id,jsonb_build_object(
        'session_id',v_session_id,'session_version',1,'source_change_seq',1,
        'source_build_run_id',v_empty_run_id
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,
      clock_timestamp(),clock_timestamp());

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,pending_job_id
  ) VALUES (
    v_session_id,v_empty_candidate_id,1,'SOURCE_BUILD_PENDING',true,true,v_empty_job_id
  );

  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,stage_version,
    seed_scope_count,seed_scope_digest,seed_scope_sealed_at_utc,
    scope_count,scope_cursor_json,created_at_utc,updated_at_utc
  ) VALUES (
    v_dependency_build_id,v_dependency_candidate_id,v_session_id,1,v_snapshot_id,
    v_dependency_run_id,v_dependency_job_id,1,1,'COLLECTING','DEPENDENCY_CLOSURE',1,
    1,md5(v_root_timesheet_id::text),v_now,1,
    '{"terminal":true}'::jsonb,v_now,v_now
  );

  UPDATE private.banking_pay_workbench_candidate_scope_registry
  SET current_build_id=v_dependency_build_id,updated_at_utc=clock_timestamp()
  WHERE candidate_id=v_dependency_candidate_id;

  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,week_ending_date,status,is_current,version
  ) VALUES
    (v_root_timesheet_id,v_booking_id,v_prefix,'VERIFY','VERIFY','VERIFY',
      DATE '2099-03-01','RECEIVED',true,3),
    (v_revoked_timesheet_id,v_booking_id,v_prefix,'VERIFY','VERIFY','VERIFY',
      DATE '2099-03-01','REVOKED',false,1);

  INSERT INTO private.banking_pay_workbench_economic_build_scope(
    build_id,timesheet_id,candidate_id,root_timesheet_id,seed_reasons,
    dependency_reasons,captured_dirty_generation,closure_status,
    required_fact_families,completed_fact_families
  ) VALUES (
    v_dependency_build_id,v_root_timesheet_id,v_dependency_candidate_id,
    v_root_timesheet_id,ARRAY['ROLLBACK_VERIFICATION'],ARRAY[]::text[],1,
    'PENDING',ARRAY[]::text[],ARRAY[]::text[]
  );

  v_result:=private.pay_workbench_timesheet_dependency_closure_v2(
    v_dependency_build_id,
    jsonb_build_object(
      'cursor_kind','DEPENDENCY_CLOSURE','cursor_version',1,
      'build_id',v_dependency_build_id,'candidate_id',v_dependency_candidate_id,
      'frontier_timesheet_id',v_root_timesheet_id,'dependency_family_ordinal',5,
      'processed_edge_count',0,'processed_emission_count',0,'page_number',1
    ),25
  );
  IF EXISTS(
       SELECT 1 FROM private.banking_pay_workbench_economic_build_scope
       WHERE build_id=v_dependency_build_id AND timesheet_id=v_revoked_timesheet_id
     ) OR COALESCE((v_result->>'inserted_scope_count')::integer,0)<>0 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_REVOKED_HISTORY_ENTERED_CURRENT_SCOPE',
      DETAIL=v_result::text;
  END IF;

  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,week_ending_date,status,is_current,version
  ) VALUES (
    v_active_unowned_timesheet_id,v_booking_id,v_prefix,'VERIFY','VERIFY','VERIFY',
    DATE '2099-03-01','RECEIVED',false,2
  );
  UPDATE private.banking_pay_workbench_economic_build_scope
  SET closure_status='PENDING',closure_family_ordinal=1,closure_last_edge_key=NULL,
      updated_at_utc=clock_timestamp()
  WHERE build_id=v_dependency_build_id AND timesheet_id=v_root_timesheet_id;

  BEGIN
    PERFORM private.pay_workbench_timesheet_dependency_closure_v2(
      v_dependency_build_id,
      jsonb_build_object(
        'cursor_kind','DEPENDENCY_CLOSURE','cursor_version',1,
        'build_id',v_dependency_build_id,'candidate_id',v_dependency_candidate_id,
        'frontier_timesheet_id',v_root_timesheet_id,'dependency_family_ordinal',5,
        'processed_edge_count',0,'processed_emission_count',0,'page_number',1
      ),25
    );
    RAISE EXCEPTION 'BANKING_PAY_ACTIVE_UNOWNED_DEPENDENCY_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE '23514' THEN
    IF SQLERRM IS DISTINCT FROM 'PAY_WORKBENCH_DEPENDENCY_IDENTITY_CONFLICT' THEN
      RAISE;
    END IF;
  END;

  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,stage_version,
    seed_scope_count,seed_scope_digest,seed_scope_sealed_at_utc,
    scope_count,dependency_node_count,dependency_edge_count,tagged_edge_count,
    row_seal_count,last_stable_ordinal,scope_cursor_json,closure_cursor_json,
    dependency_edge_stream_complete,dependency_edge_stream_digest,
    edge_tag_stream_complete,edge_tag_digest,unit_digest,scope_digest,
    dependency_digest,sealed_fingerprint_digest,dependency_closure_sealed_at_utc,
    attestation_json,ready_at_utc,created_at_utc,updated_at_utc
  ) VALUES (
    v_empty_build_id,v_empty_candidate_id,v_session_id,1,v_snapshot_id,
    v_empty_run_id,v_empty_job_id,1,1,'RECONCILING','RECONCILE_EXECUTE',1,
    0,md5(''),v_now,0,0,0,0,0,0,'{"terminal":true}'::jsonb,
    '{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,true,md5(''),
    true,md5(''),md5(''),md5(''),md5(''),md5(''),v_now,
    jsonb_build_object(
      'execution_profile_version',1,'reconciliation_optimization_version',0,
      'effect_plan_sealed',true,'effect_plan_count',0,
      'effect_plan_digest',md5('[]'),'effect_plan_created_at_utc',clock_timestamp()
    ),v_now,v_now,v_now
  );

  UPDATE private.banking_pay_workbench_candidate_scope_registry
  SET current_build_id=v_empty_build_id,updated_at_utc=clock_timestamp()
  WHERE candidate_id=v_empty_candidate_id;

  UPDATE public.banking_pay_workbench_jobs
  SET status='RUNNING',attempt_count=1,economic_build_id=v_empty_build_id,
    private_stage='RECONCILE_EXECUTE',private_cursor_kind='RECONCILE_EXECUTE',
    private_cursor_json=jsonb_build_object(
      'cursor_kind','RECONCILE_EXECUTE','cursor_version',1,
      'build_id',v_empty_build_id,'candidate_id',v_empty_candidate_id,
      'effect_plan_digest',md5('[]')
    ),private_stage_version=1,started_at_utc=clock_timestamp(),
    updated_at_utc=clock_timestamp()
  WHERE id=v_empty_job_id;

  INSERT INTO private.banking_pay_workbench_stage_attempts(
    id,job_id,build_id,candidate_id,private_stage,attempt_number,attempt_nonce,
    worker_id,lane_identity,captured_candidate_generation,
    captured_source_change_seq,execution_profile_version,attempt_status,
    started_at_utc,lease_expires_at_utc,created_at_utc,updated_at_utc
  ) VALUES (
    v_empty_attempt_id,v_empty_job_id,v_empty_build_id,v_empty_candidate_id,
    'RECONCILE_EXECUTE',1,v_empty_attempt_nonce,'ROLLBACK_VERIFIER',
    'ROLLBACK_VERIFIER_LANE',1,1,1,'STARTED',v_now,
    v_now+interval '10 minutes',v_now,v_now
  );

  CREATE TEMP TABLE pg_temp._bpay_wb_sync_context_v1(
    job_id uuid NOT NULL,build_id uuid NOT NULL,attempt_id uuid NOT NULL,
    attempt_nonce uuid NOT NULL,build_token uuid NOT NULL,candidate_id uuid NOT NULL,
    private_stage text NOT NULL,captured_generation bigint NOT NULL,
    source_change_seq bigint NOT NULL,created_at_utc timestamptz NOT NULL
  ) ON COMMIT DROP;
  INSERT INTO pg_temp._bpay_wb_sync_context_v1
  SELECT v_empty_job_id,v_empty_build_id,v_empty_attempt_id,v_empty_attempt_nonce,
    build_row.build_token,v_empty_candidate_id,'RECONCILE_EXECUTE',1,1,v_now
  FROM private.banking_pay_workbench_economic_builds AS build_row
  WHERE build_row.id=v_empty_build_id;
  PERFORM pg_catalog.set_config(
    'cloudtms.pay_workbench_execution_profile_version','1',true
  );

  -- Match the controller's rollback-contained capture protocol exactly. The
  -- public wrapper must expose an empty effect array instead of rejecting the
  -- legacy authoritative-empty result shape.
  BEGIN
    PERFORM pg_catalog.set_config(
      'cloudtms.pay_workbench_effect_capture_mode','capture',true
    );
    v_capture_result:=public.pay_sync_overpayments_from_preview(
      DATE '2099-03-06',DATE '2099-03-01',v_actor_id,'PAYE',
      ARRAY[v_empty_candidate_id],'{}'::jsonb,NULL,NULL,NULL
    );
    IF COALESCE((v_capture_result->>'effect_plan_capture')::boolean,false) IS NOT TRUE
       OR v_capture_result->'captured_effects' IS DISTINCT FROM '[]'::jsonb
       OR COALESCE((v_capture_result->>'explicit_empty_timesheet_scope')::boolean,false)
          IS NOT TRUE THEN
      RAISE EXCEPTION USING MESSAGE='BANKING_PAY_EMPTY_SCOPE_CAPTURE_INVALID',
        DETAIL=v_capture_result::text;
    END IF;
    RAISE EXCEPTION 'BANKING_PAY_EMPTY_SCOPE_CAPTURE_ROLLBACK' USING ERRCODE='PZ001';
  EXCEPTION WHEN SQLSTATE 'PZ001' THEN
    NULL;
  END;

  SELECT count(*) INTO v_case_count_before
  FROM public.pay_advances WHERE candidate_id=v_empty_candidate_id;

  v_result:=public.pay_sync_overpayments_from_preview(
    DATE '2099-03-06',DATE '2099-03-01',v_actor_id,'PAYE',
    ARRAY[v_empty_candidate_id],'{}'::jsonb,NULL,NULL,NULL
  );
  SELECT status,private_stage,scope_count,canonical_count,canonical_digest,
    attestation_json->>'policy_x_authority_scope' AS policy_scope,
    attestation_json->>'empty_scope_reconciliation_version' AS empty_version
  INTO STRICT v_row
  FROM private.banking_pay_workbench_economic_builds
  WHERE id=v_empty_build_id;
  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_result->>'explicit_empty_timesheet_scope')::boolean,false)
        IS NOT TRUE
     OR v_row.status IS DISTINCT FROM 'RECONCILED'
     OR v_row.private_stage IS DISTINCT FROM 'SOURCE_PUBLISH'
     OR v_row.scope_count<>0 OR v_row.canonical_count<>0
     OR v_row.canonical_digest IS DISTINCT FROM md5('')
     OR v_row.policy_scope IS DISTINCT FROM 'PRE_DRAFT_LIVE_TRUTH'
     OR v_row.empty_version IS DISTINCT FROM '1'
     OR EXISTS(
       SELECT 1 FROM private.banking_pay_workbench_canonical_stage_lines
       WHERE build_id=v_empty_build_id
     )
     OR (SELECT count(*) FROM public.pay_advances
         WHERE candidate_id=v_empty_candidate_id) IS DISTINCT FROM v_case_count_before THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_EMPTY_SCOPE_RECONCILIATION_INVALID',
      DETAIL=jsonb_build_object('result',v_result,'build',to_jsonb(v_row))::text;
  END IF;

  RAISE NOTICE 'PASS: revoked booking history is excluded; active unowned identities fail closed; zero-scope reconciliation reaches SOURCE_PUBLISH without finance mutation.';
END;
$verification$;

ROLLBACK;
