\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for Banking Pay DIRTY_APPLY family
-- authority. A trigger may initially invalidate one Timesheet, while the
-- bounded dependency/rotation closure later proves a wider version family.
-- The processor must establish one current authority for the final family
-- before scanning sessions. Historical terminal jobs for the two proven
-- authority errors remain immutable audit evidence and receive one canonical,
-- idempotent full-Candidate successor while an open session is still stuck.
BEGIN;
SET LOCAL statement_timeout='30s';

DO $verification$
DECLARE
  v_snapshot_id uuid:=gen_random_uuid();
  v_actor_id uuid:=gen_random_uuid();
  v_family_candidate_id uuid:=gen_random_uuid();
  v_repair_candidate_id uuid:=gen_random_uuid();
  v_current_timesheet_id uuid:=gen_random_uuid();
  v_history_timesheet_id uuid:=gen_random_uuid();
  v_family_job_id uuid;
  v_dead_job_id uuid:=gen_random_uuid();
  v_repair_session_id uuid:=gen_random_uuid();
  v_booking_id text:='BPAY-FAMILY-VERIFY-'||gen_random_uuid()::text;
  v_prefix text:='BANKING_PAY_DIRTY_FAMILY_VERIFY:'||gen_random_uuid()::text;
  v_seed_result jsonb:='{}'::jsonb;
  v_stage_result jsonb:='{}'::jsonb;
  v_process_result jsonb:='{}'::jsonb;
  v_repair_result jsonb:='{}'::jsonb;
  v_replay_result jsonb:='{}'::jsonb;
  v_pre_repair jsonb:='{}'::jsonb;
  v_old_generation bigint:=0;
  v_new_generation bigint:=0;
  v_dead_before jsonb:='{}'::jsonb;
  v_dead_after jsonb:='{}'::jsonb;
  v_successor_id uuid;
  v_definition text;
  v_authority_contract_mismatch jsonb:='[]'::jsonb;
  v_pre record;
  v_operations_before bigint;
  v_batches_before bigint;
  v_provider_before bigint;
  v_settlement_before bigint;
  v_remittance_before bigint;
BEGIN
  WITH expected(identity,definition_sha256) AS (
    VALUES
      ('public._pay_active_settled_components(uuid[])'::regprocedure,NULL::text),
      ('public.bulk_authorise_dataset_v1(jsonb)'::regprocedure,NULL::text),
      ('public.bulk_authorise_row_context_v1(jsonb)'::regprocedure,NULL::text),
      ('public.bulk_process_dataset_v1(jsonb)'::regprocedure,NULL::text),
      ('public.bulk_process_row_context_v1(jsonb)'::regprocedure,NULL::text),
      ('public.bulk_timesheet_row_patch_v1(jsonb)'::regprocedure,NULL::text),
      ('public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb)'::regprocedure,
       '89543b82378468b1ae43534f5a4b1a200ffc60ffbef76196398b7f7d6521792f'::text),
      ('public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)'::regprocedure,NULL::text),
      ('public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid)'::regprocedure,NULL::text),
      ('public.pay_timesheet_summary_pay_state_refresh_trigger()'::regprocedure,NULL::text),
      ('public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer)'::regprocedure,NULL::text),
      ('public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer)'::regprocedure,NULL::text),
      ('public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb)'::regprocedure,NULL::text),
      ('public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer)'::regprocedure,NULL::text),
      ('public.pay_workbench_repair_invalid_source_build_poison(uuid,uuid,integer,timestamptz,text)'::regprocedure,NULL::text),
      ('public.pay_workbench_session_clear_case_resolution(uuid,uuid,jsonb)'::regprocedure,NULL::text),
      ('public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb)'::regprocedure,NULL::text),
      ('public.pay_workbench_session_replay_replaced_queue_v1(uuid,uuid,text,jsonb)'::regprocedure,
       '363aeab20aed70b8396793808f9a2263766e984d66914317bdf0a767e6e0f360'::text),
      ('public.pay_workbench_session_set_selected_rows(uuid,jsonb,uuid)'::regprocedure,
       '7d622194f7bca877bf8420cb6f10f9ad46a69bad118c5f8fb9ed16810492d98c'::text),
      ('public.pay_workbench_worker_drain_chunk(integer,timestamptz,uuid,uuid,text[],text,integer)'::regprocedure,NULL::text),
      ('public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz)'::regprocedure,NULL::text),
      ('public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text)'::regprocedure,NULL::text),
      ('public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text)'::regprocedure,NULL::text),
      ('public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)'::regprocedure,NULL::text),
      ('public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz)'::regprocedure,
       '090fcbd7a66ade81f107635c360a038a514a5c26358c0b4aa716bdea91245347'::text)
  ), actual AS (
    SELECT
      expected.identity,
      expected.definition_sha256,
      pg_catalog.encode(extensions.digest(
        pg_catalog.convert_to(pg_catalog.pg_get_functiondef(expected.identity::oid),'UTF8'),
        'sha256'
      ),'hex') AS actual_definition_sha256,
      pg_catalog.pg_get_userbyid(proc.proowner) AS owner_name,
      pg_catalog.has_function_privilege('service_role',expected.identity::oid,'EXECUTE') AS service_execute,
      pg_catalog.has_function_privilege('anon',expected.identity::oid,'EXECUTE') AS anon_execute,
      pg_catalog.has_function_privilege('authenticated',expected.identity::oid,'EXECUTE') AS authenticated_execute
    FROM expected
    JOIN pg_catalog.pg_proc AS proc ON proc.oid=expected.identity::oid
  )
  SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'identity',identity::text,
    'expected_definition_sha256',definition_sha256,
    'actual_definition_sha256',actual_definition_sha256,
    'owner_name',owner_name,
    'service_execute',service_execute,
    'anon_execute',anon_execute,
    'authenticated_execute',authenticated_execute
  ) ORDER BY identity::text),'[]'::jsonb)
  INTO v_authority_contract_mismatch
  FROM actual
  WHERE (definition_sha256 IS NOT NULL AND actual_definition_sha256 IS DISTINCT FROM definition_sha256)
     OR owner_name IS DISTINCT FROM current_user
     OR service_execute IS NOT TRUE
     OR anon_execute IS TRUE
     OR authenticated_execute IS TRUE;

  IF pg_catalog.jsonb_array_length(v_authority_contract_mismatch)<>0 THEN
    RAISE EXCEPTION USING
      MESSAGE='BANKING_PAY_FINAL_AUTHORITY_CLOSURE_MISMATCH',
      DETAIL=v_authority_contract_mismatch::text;
  END IF;

  SELECT pg_get_functiondef(
    'public.pay_workbench_candidate_dirty_apply_job_process(uuid,integer)'::regprocedure
  )||E'\n'||pg_get_functiondef(
    'public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(uuid,uuid,integer,text)'::regprocedure
  ) INTO STRICT v_definition;
  IF v_definition ~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' THEN
    RAISE EXCEPTION 'BANKING_PAY_DIRTY_FAMILY_ILLEGAL_CONDITIONAL_PREFIX';
  END IF;

  IF has_function_privilege(
       'anon','public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(uuid,uuid,integer,text)','EXECUTE'
     )
     OR has_function_privilege(
       'authenticated','public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(uuid,uuid,integer,text)','EXECUTE'
     )
     OR NOT has_function_privilege(
       'service_role','public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(uuid,uuid,integer,text)','EXECUTE'
     )
     OR has_function_privilege(
       'anon','public.pay_workbench_candidate_dirty_apply_job_process(uuid,integer)','EXECUTE'
     )
     OR has_function_privilege(
       'authenticated','public.pay_workbench_candidate_dirty_apply_job_process(uuid,integer)','EXECUTE'
     )
     OR NOT has_function_privilege(
       'service_role','public.pay_workbench_candidate_dirty_apply_job_process(uuid,integer)','EXECUTE'
     ) THEN
    RAISE EXCEPTION 'BANKING_PAY_DIRTY_FAMILY_ACL_INVALID';
  END IF;

  SELECT count(*) INTO v_operations_before FROM public.banking_pay_operations;
  SELECT count(*) INTO v_batches_before FROM public.pay_batches;
  SELECT count(*) INTO v_provider_before
  FROM public.banking_pay_operation_provider_attempts;
  SELECT count(*) INTO v_settlement_before
  FROM public.banking_pay_operation_settlement_scope;
  SELECT count(*) INTO v_remittance_before
  FROM public.banking_pay_operation_remittance_scope;

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-04-10',DATE '2099-04-05',DATE '2099-03-30',
    DATE '2099-03-01',DATE '2099-04-05','OPEN',false
  );

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (
    v_actor_id,
    'bpay-dirty-family-'||replace(v_actor_id::text,'-','')||'@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true
  );

  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method) VALUES
    (v_family_candidate_id,v_prefix||':FAMILY',v_prefix||':FAMILY','PAYE'),
    (v_repair_candidate_id,v_prefix||':REPAIR',v_prefix||':REPAIR','PAYE');

  INSERT INTO public.app_change_counters(entity_key,seq,scope_change_generation)
  VALUES
    ('pay_candidate:'||v_family_candidate_id::text,7,0),
    ('pay_candidate:'||v_repair_candidate_id::text,11,0)
  ON CONFLICT(entity_key) DO UPDATE SET
    seq=EXCLUDED.seq,scope_change_generation=EXCLUDED.scope_change_generation;

  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,week_ending_date,status,is_current,version,revoked_at,
    revoked_reason
  ) VALUES
    (v_history_timesheet_id,v_booking_id,v_prefix,'VERIFY','VERIFY','VERIFY',
      DATE '2099-04-05','REVOKED',false,1,clock_timestamp(),'VERSION_REPLACED'),
    (v_current_timesheet_id,v_booking_id,v_prefix,'VERIFY','VERIFY','VERIFY',
      DATE '2099-04-05','RECEIVED',true,2,NULL,NULL);

  INSERT INTO public.timesheets_financials(
    timesheet_id,timesheet_version,is_current,candidate_id,
    candidate_assignment,processing_status
  ) VALUES (
    v_current_timesheet_id,2,true,v_family_candidate_id,'ASSIGNED','READY_FOR_HR'
  );

  -- Reproduce the original one-row trigger knowledge. The invalidator creates
  -- the ordinary canonical DIRTY_APPLY owner and finalises its authority.
  v_seed_result:=private.pay_workbench_scope_invalidate_v1(
    ARRAY[v_family_candidate_id],ARRAY[v_current_timesheet_id],
    'VERIFICATION_ONE_ROW_TRIGGER',NULL::uuid,
    jsonb_build_object(
      'source_change_seq',7,
      'latest_source_change_seq',7,
      'trigger_source','ROLLBACK_VERIFICATION',
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
      'economic_truth_mutation_allowed',false
    )
  );

  -- The real trigger request commits before the worker claims its job. Force
  -- only this rollback verifier's deferred constraint at the same boundary.
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM set_config('cloudtms.scope_generation_finalising','false',true);
  PERFORM set_config('cloudtms.banking_pay_scope_tx_token','',true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  SELECT job.id,job.scope_change_generation
  INTO STRICT v_family_job_id,v_old_generation
  FROM public.banking_pay_workbench_jobs AS job
  WHERE job.candidate_id=v_family_candidate_id
    AND job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND job.status='QUEUED'
  ORDER BY job.created_at_utc DESC,job.id DESC
  LIMIT 1;

  IF COALESCE((v_seed_result->>'ok')::boolean,false) IS NOT TRUE
     OR v_old_generation<1
     OR jsonb_array_length(
       (SELECT payload_json->'targeted_timesheet_ids'
        FROM public.banking_pay_workbench_jobs WHERE id=v_family_job_id)
     )<>1 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DIRTY_FAMILY_SEED_INVALID',
      DETAIL=v_seed_result::text;
  END IF;

  UPDATE public.banking_pay_workbench_jobs
  SET status='RUNNING',attempt_count=1,started_at_utc=clock_timestamp(),
      updated_at_utc=clock_timestamp()
  WHERE id=v_family_job_id;

  v_stage_result:=public.pay_workbench_candidate_dirty_apply_job_process(
    v_family_job_id,100
  );

  IF COALESCE((v_stage_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_stage_result->>'preinvalidated_scope_reissued')::boolean,false)
          IS NOT TRUE
     OR COALESCE((v_stage_result->>'preinvalidated_scope_reissue_pending_finalization')::boolean,false)
          IS NOT TRUE
     OR COALESCE((v_stage_result->>'family_timesheet_count')::integer,-1)<>2
     OR (SELECT status FROM public.banking_pay_workbench_jobs
         WHERE id=v_family_job_id) IS DISTINCT FROM 'QUEUED' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DIRTY_FAMILY_REISSUE_NOT_STAGED',
      DETAIL=v_stage_result::text;
  END IF;

  -- The real RPC now commits, the canonical deferred finaliser assigns one
  -- generation, and the worker later claims the same durable job again.
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM set_config('cloudtms.scope_generation_finalising','false',true);
  PERFORM set_config('cloudtms.banking_pay_scope_tx_token','',true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  SELECT job.payload_json,job.scope_change_generation,
    tx.state AS tx_state,tx.allocated_generation AS tx_generation,
    registry.dirty_generation AS registry_generation,
    counter.scope_change_generation AS live_generation,
    (SELECT count(*) FROM private.banking_pay_workbench_timesheet_scope_state state
      WHERE state.candidate_id=v_family_candidate_id
        AND state.timesheet_id IN (v_history_timesheet_id,v_current_timesheet_id)
        AND state.dirty_generation=job.scope_change_generation) AS matched_count
  INTO STRICT v_pre
  FROM public.banking_pay_workbench_jobs job
  LEFT JOIN public.banking_pay_scope_change_transactions tx
    ON tx.tx_token=(job.payload_json->>'scope_change_tx_token')::uuid
  JOIN private.banking_pay_workbench_candidate_scope_registry registry
    ON registry.candidate_id=job.candidate_id
  LEFT JOIN public.app_change_counters counter
    ON counter.entity_key='pay_candidate:'||job.candidate_id::text
  WHERE job.id=v_family_job_id;
  IF v_pre.tx_state IS DISTINCT FROM 'FINALIZED'
     OR v_pre.tx_generation IS DISTINCT FROM v_pre.scope_change_generation
     OR v_pre.registry_generation IS DISTINCT FROM v_pre.scope_change_generation
     OR v_pre.live_generation IS DISTINCT FROM v_pre.scope_change_generation
     OR v_pre.matched_count<>2
     OR lower(COALESCE(v_pre.payload_json->>'bounded_scope_state_precedes_job','false'))<>'true' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DIRTY_FAMILY_STAGED_AUTHORITY_NOT_FINAL',
      DETAIL=to_jsonb(v_pre)::text;
  END IF;
  UPDATE public.banking_pay_workbench_jobs
  SET status='RUNNING',attempt_count=attempt_count+1,
      started_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp()
  WHERE id=v_family_job_id;

  v_process_result:=public.pay_workbench_candidate_dirty_apply_job_process(
    v_family_job_id,100
  );
  v_new_generation:=(v_process_result->>'effective_scope_change_generation')::bigint;

  IF COALESCE((v_process_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_process_result->>'preinvalidated_scope_reissued')::boolean,false)
          IS NOT TRUE
     OR COALESCE((v_process_result->>'family_timesheet_count')::integer,-1)<>2
     OR v_new_generation<=v_old_generation
     OR (SELECT scope_change_generation
         FROM public.banking_pay_workbench_jobs WHERE id=v_family_job_id)
          IS DISTINCT FROM v_new_generation
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
         WHERE scope_state.candidate_id=v_family_candidate_id
           AND scope_state.timesheet_id IN (
             v_history_timesheet_id,v_current_timesheet_id
           )
           AND scope_state.dirty_generation=v_new_generation)<>2
     OR (SELECT status FROM public.timesheets
         WHERE timesheet_id=v_history_timesheet_id) IS DISTINCT FROM 'REVOKED'
     OR (SELECT is_current FROM public.timesheets
         WHERE timesheet_id=v_history_timesheet_id) IS NOT FALSE THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DIRTY_FAMILY_FINAL_AUTHORITY_INVALID',
      DETAIL=jsonb_build_object(
        'seed',v_seed_result,'stage',v_stage_result,'process',v_process_result,
        'old_generation',v_old_generation,'new_generation',v_new_generation
      )::text;
  END IF;

  -- Build the historical failure shape independently. The repair needs an
  -- open still-dirty session, but it must never alter the DEAD source row.
  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version,scope_change_generation_target,
    scope_change_generation_applied
  ) VALUES (
    v_repair_session_id,v_actor_id,DATE '2099-04-10',DATE '2099-04-05',
    v_prefix||':REPAIR_SESSION',v_snapshot_id,'OPEN',4,2,1
  );

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,error_json
  ) VALUES (
    v_repair_session_id,v_repair_candidate_id,1,'FAILED',true,true,
    jsonb_build_object('code','VERIFICATION_STUCK_SCOPE')
  );

  -- Candidate creation itself legitimately emits an ordinary dirty event.
  -- Close only that rollback-fixture owner before constructing the later DEAD
  -- authority failure, so the repair is tested against its real eligibility
  -- rule: no currently active DIRTY_APPLY already owns the Candidate.
  UPDATE public.banking_pay_workbench_jobs
  SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),
      updated_at_utc=clock_timestamp()
  WHERE candidate_id=v_repair_candidate_id
    AND job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND status IN ('QUEUED','RUNNING');

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,failed_at_utc,
    last_error_json,created_at_utc,updated_at_utc
  ) VALUES (
    v_dead_job_id,'WORKBENCH_CANDIDATE_DIRTY_APPLY','DEAD',-1000,
    clock_timestamp(),8,8,v_prefix||':DEAD',v_snapshot_id,
    v_repair_session_id,v_repair_candidate_id,
    jsonb_build_object('source_change_seq',11,'targeted_timesheet_ids','[]'::jsonb),
    clock_timestamp(),
    jsonb_build_object(
      'message','PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED',
      'code','PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED'
    ),clock_timestamp(),clock_timestamp()
  );

  -- A directly inserted fixture job passes through the normal job-stage
  -- trigger. Finish that historical row's own transaction before capturing
  -- its immutable terminal shape, as would already be true in production.
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM set_config('cloudtms.scope_generation_finalising','false',true);
  PERFORM set_config('cloudtms.banking_pay_scope_tx_token','',true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  SELECT to_jsonb(job) INTO STRICT v_dead_before
  FROM public.banking_pay_workbench_jobs AS job WHERE job.id=v_dead_job_id;

  SELECT jsonb_build_object(
    'dead_matches',(SELECT count(*) FROM public.banking_pay_workbench_jobs job
      WHERE job.id=v_dead_job_id AND job.status='DEAD'
        AND job.last_error_json->>'message' IN (
          'PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED',
          'PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT')),
    'open_stuck_scope',(SELECT count(*)
      FROM public.banking_pay_workbench_sessions session_row
      JOIN public.banking_pay_workbench_session_scope scope_row
        ON scope_row.session_id=session_row.id
       AND scope_row.candidate_id=v_repair_candidate_id
      WHERE session_row.id=v_repair_session_id
        AND session_row.status='OPEN' AND session_row.discarded_at_utc IS NULL
        AND (scope_row.dirty OR scope_row.error_json IS NOT NULL
          OR session_row.scope_change_generation_applied
             <session_row.scope_change_generation_target)),
    'active_dirty',(SELECT count(*) FROM public.banking_pay_workbench_jobs job
      WHERE job.candidate_id=v_repair_candidate_id
        AND job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND job.status IN ('QUEUED','RUNNING')),
    'later_success',(SELECT count(*) FROM public.banking_pay_workbench_jobs job
      WHERE job.candidate_id=v_repair_candidate_id
        AND job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND job.status='SUCCEEDED'
        AND (job.updated_at_utc,job.id)>
            ((v_dead_before->>'updated_at_utc')::timestamptz,v_dead_job_id))
  ) INTO v_pre_repair;
  IF (v_pre_repair->>'dead_matches')::integer<>1
     OR (v_pre_repair->>'open_stuck_scope')::integer<>1
     OR (v_pre_repair->>'active_dirty')::integer<>0
     OR (v_pre_repair->>'later_success')::integer<>0 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DIRTY_FAMILY_REPAIR_FIXTURE_INVALID',
      DETAIL=v_pre_repair::text;
  END IF;

  v_repair_result:=public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(
    v_repair_session_id,v_repair_candidate_id,1,'ROLLBACK_VERIFICATION'
  );
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM set_config('cloudtms.scope_generation_finalising','false',true);
  PERFORM set_config('cloudtms.banking_pay_scope_tx_token','',true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  SELECT to_jsonb(job) INTO STRICT v_dead_after
  FROM public.banking_pay_workbench_jobs AS job WHERE job.id=v_dead_job_id;
  SELECT job.id INTO v_successor_id
  FROM public.banking_pay_workbench_jobs AS job
  WHERE job.candidate_id=v_repair_candidate_id
    AND job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND job.status IN ('QUEUED','RUNNING')
    AND job.payload_json->>'invalid_dirty_apply_repair_source_job_id'
        =v_dead_job_id::text
  ORDER BY job.created_at_utc DESC,job.id DESC LIMIT 1;

  IF COALESCE((v_repair_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_repair_result->>'repaired_candidate_count')::integer,-1)<>1
     OR COALESCE((v_repair_result->>'successor_job_count')::integer,-1)<>1
     OR COALESCE((v_repair_result->>'failed_count')::integer,-1)<>0
     OR v_dead_after IS DISTINCT FROM v_dead_before
     OR v_successor_id IS NULL
     OR COALESCE((SELECT scope_change_generation
         FROM public.banking_pay_workbench_jobs WHERE id=v_successor_id),0)<1
     OR (SELECT dirty_generation
         FROM private.banking_pay_workbench_candidate_scope_registry
         WHERE candidate_id=v_repair_candidate_id)
          IS DISTINCT FROM (SELECT scope_change_generation
            FROM public.banking_pay_workbench_jobs WHERE id=v_successor_id)
     OR (SELECT scope_change_generation FROM public.app_change_counters
         WHERE entity_key='pay_candidate:'||v_repair_candidate_id::text)
          IS DISTINCT FROM (SELECT scope_change_generation
            FROM public.banking_pay_workbench_jobs WHERE id=v_successor_id)
     OR jsonb_array_length(
       (SELECT payload_json->'targeted_timesheet_ids'
        FROM public.banking_pay_workbench_jobs WHERE id=v_successor_id)
     )<>0
     OR (SELECT status FROM public.banking_pay_workbench_jobs
         WHERE id=v_dead_job_id) IS DISTINCT FROM 'DEAD' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DIRTY_FAMILY_REPAIR_INVALID',
      DETAIL=jsonb_build_object(
        'result',v_repair_result,'dead_unchanged',v_dead_after=v_dead_before,
        'successor_id',v_successor_id,'dead_before',v_dead_before,
        'dead_after',v_dead_after
      )::text;
  END IF;

  v_replay_result:=public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(
    v_repair_session_id,v_repair_candidate_id,1,'ROLLBACK_VERIFICATION_REPLAY'
  );
  IF COALESCE((v_replay_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_replay_result->>'examined_candidate_count')::integer,-1)<>0
     OR COALESCE((v_replay_result->>'repaired_candidate_count')::integer,-1)<>0
     OR (SELECT count(*) FROM public.banking_pay_workbench_jobs
         WHERE candidate_id=v_repair_candidate_id
           AND job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
           AND payload_json->>'invalid_dirty_apply_repair_source_job_id'
               =v_dead_job_id::text)<>1 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DIRTY_FAMILY_REPAIR_NOT_IDEMPOTENT',
      DETAIL=v_replay_result::text;
  END IF;

  IF (SELECT count(*) FROM public.banking_pay_operations)<>v_operations_before
     OR (SELECT count(*) FROM public.pay_batches)<>v_batches_before
     OR (SELECT count(*) FROM public.banking_pay_operation_provider_attempts)
          <>v_provider_before
     OR (SELECT count(*) FROM public.banking_pay_operation_settlement_scope)
          <>v_settlement_before
     OR (SELECT count(*) FROM public.banking_pay_operation_remittance_scope)
          <>v_remittance_before THEN
    RAISE EXCEPTION 'BANKING_PAY_DIRTY_FAMILY_FINANCIAL_LIFECYCLE_CHANGED';
  END IF;

  RAISE NOTICE 'PASS: final Timesheet-family authority is reissued once; DEAD audit history is immutable; canonical repair is idempotent and does not touch financial lifecycle authority.';
END;
$verification$;

ROLLBACK;
