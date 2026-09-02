\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for Workbench source-build causal
-- recovery.  This verifier exercises the real full schema and existing
-- enqueue/progress owners.  It does not create a Draft or cross a provider,
-- payment, settlement or remittance boundary.
BEGIN;
SET LOCAL statement_timeout='60s';
SET LOCAL jit=off;

DO $verification$
DECLARE
  v_snapshot_id uuid:=gen_random_uuid();
  v_actor_id uuid:=gen_random_uuid();
  v_session_id uuid:=gen_random_uuid();
  v_deterministic_candidate_id uuid:=gen_random_uuid();
  v_changed_candidate_id uuid:=gen_random_uuid();
  v_lease_candidate_id uuid:=gen_random_uuid();
  v_deterministic_job_id uuid:=gen_random_uuid();
  v_changed_job_id uuid:=gen_random_uuid();
  v_lease_job_id uuid:=gen_random_uuid();
  v_deterministic_build_id uuid:=gen_random_uuid();
  v_changed_build_id uuid:=gen_random_uuid();
  v_lease_build_id uuid:=gen_random_uuid();
  v_deterministic_run_id uuid:=gen_random_uuid();
  v_changed_run_id uuid:=gen_random_uuid();
  v_lease_run_id uuid:=gen_random_uuid();
  v_first_cause_code text:='PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID';
  v_prefix text:='BANKING_PAY_CAUSAL_RECOVERY_VERIFY:'||gen_random_uuid()::text;
  v_now timestamptz:=clock_timestamp();
  v_result jsonb:='{}'::jsonb;
  v_replay_result jsonb:='{}'::jsonb;
  v_enqueue_result jsonb:='{}'::jsonb;
  v_successor_id uuid:=NULL::uuid;
  v_jobs_before_replay bigint:=0;
  v_preview_before bigint:=0;
  v_operations_before bigint:=0;
  v_batches_before bigint:=0;
  v_provider_before bigint:=0;
  v_settlement_before bigint:=0;
  v_remittance_before bigint:=0;
  v_policy_projection_before jsonb:='{}'::jsonb;
  v_policy_projection_after jsonb:='{}'::jsonb;
  v_definition text;
  v_row record;
BEGIN
  SELECT pg_get_functiondef(
      'public.pay_workbench_fail_job(uuid,jsonb,integer)'::regprocedure
    )||E'\n'||pg_get_functiondef(
      'public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamp with time zone,text)'::regprocedure
    )||E'\n'||pg_get_functiondef(
      'public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamp with time zone,uuid,uuid)'::regprocedure
    )
  INTO STRICT v_definition;
  IF v_definition ~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' THEN
    RAISE EXCEPTION 'BANKING_PAY_CAUSAL_RECOVERY_ILLEGAL_CONDITIONAL_PREFIX';
  END IF;

  IF has_function_privilege(
       'anon',
       'public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamp with time zone,text)',
       'EXECUTE'
     )
     OR has_function_privilege(
       'authenticated',
       'public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamp with time zone,text)',
       'EXECUTE'
     )
     OR NOT has_function_privilege(
       'service_role',
       'public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamp with time zone,text)',
       'EXECUTE'
     ) THEN
    RAISE EXCEPTION 'BANKING_PAY_CAUSAL_RECOVERY_ACL_INVALID';
  END IF;

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-04-03',DATE '2099-03-29',DATE '2099-03-23',
    DATE '2099-03-01',DATE '2099-03-29','OPEN',false
  );

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (
    v_actor_id,
    'bpay-causal-recovery-'||replace(v_actor_id::text,'-','')||'@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true
  );

  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method) VALUES
    (v_deterministic_candidate_id,v_prefix||':DETERMINISTIC',v_prefix||':DETERMINISTIC','PAYE'),
    (v_changed_candidate_id,v_prefix||':CHANGED',v_prefix||':CHANGED','UMBRELLA'),
    (v_lease_candidate_id,v_prefix||':LEASE',v_prefix||':LEASE','PAYE');

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version,discarded_at_utc,
    scope_seed_complete,scope_total_count,scope_seeded_count
  ) VALUES (
    v_session_id,v_actor_id,DATE '2099-04-03',DATE '2099-03-29',
    v_prefix||':SESSION',v_snapshot_id,'OPEN',3,NULL,true,3,3
  );

  INSERT INTO public.app_change_counters(entity_key,seq) VALUES
    ('pay_candidate:'||v_deterministic_candidate_id::text,11),
    ('pay_candidate:'||v_changed_candidate_id::text,21),
    ('pay_candidate:'||v_lease_candidate_id::text,31)
  ON CONFLICT(entity_key) DO UPDATE SET seq=EXCLUDED.seq,updated_at=clock_timestamp();

  UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry_row
  SET initialisation_status='READY',
      dirty_generation=CASE
        WHEN registry_row.candidate_id=v_deterministic_candidate_id THEN 7
        WHEN registry_row.candidate_id=v_changed_candidate_id THEN 8
        ELSE 9
      END,
      evaluated_generation=CASE
        WHEN registry_row.candidate_id=v_deterministic_candidate_id THEN 6
        WHEN registry_row.candidate_id=v_changed_candidate_id THEN 7
        ELSE 8
      END,
      current_source_change_seq=CASE
        WHEN registry_row.candidate_id=v_deterministic_candidate_id THEN 11
        WHEN registry_row.candidate_id=v_changed_candidate_id THEN 21
        ELSE 31
      END,
      last_dirty_reason='ROLLBACK_VERIFICATION',
      last_evaluated_at_utc=clock_timestamp(),
      initialised_at_utc=clock_timestamp(),
      last_dirtied_at_utc=clock_timestamp(),
      failure_json='{}'::jsonb,
      updated_at_utc=clock_timestamp()
  WHERE registry_row.candidate_id IN (
    v_deterministic_candidate_id,v_changed_candidate_id,v_lease_candidate_id
  );

  -- Build-initialise is the only valid no-build job shape.  The build rows are
  -- inserted next, then the jobs transition to their real failed/running
  -- stage identities under the installed trigger constraints.
  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version,
    created_at_utc,updated_at_utc
  ) VALUES
    (v_deterministic_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,v_now,
      0,8,v_prefix||':DETERMINISTIC_JOB',v_snapshot_id,v_session_id,
      v_deterministic_candidate_id,jsonb_build_object(
        'session_id',v_session_id::text,'source_session_id',v_session_id::text,
        'candidate_id',v_deterministic_candidate_id::text,'session_version',3,
        'source_change_seq',11,'scope_change_generation',7,
        'source_build_run_id',v_deterministic_run_id::text,
        'refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL'
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,v_now,v_now),
    (v_changed_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,v_now,
      0,8,v_prefix||':CHANGED_JOB',v_snapshot_id,v_session_id,
      v_changed_candidate_id,jsonb_build_object(
        'session_id',v_session_id::text,'source_session_id',v_session_id::text,
        'candidate_id',v_changed_candidate_id::text,'session_version',3,
        'source_change_seq',21,'scope_change_generation',8,
        'source_build_run_id',v_changed_run_id::text,
        'refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL'
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,v_now,v_now),
    (v_lease_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',40,v_now,
      0,2,v_prefix||':LEASE_JOB',v_snapshot_id,v_session_id,
      v_lease_candidate_id,jsonb_build_object(
        'session_id',v_session_id::text,'source_session_id',v_session_id::text,
        'candidate_id',v_lease_candidate_id::text,'session_version',3,
        'source_change_seq',31,'scope_change_generation',9,
        'source_build_run_id',v_lease_run_id::text,
        'refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL'
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1,v_now,v_now);

  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,stage_version,failure_json,
    failed_at_utc,created_at_utc,updated_at_utc
  ) VALUES
    (v_deterministic_build_id,v_deterministic_candidate_id,v_session_id,3,
      v_snapshot_id,v_deterministic_run_id,v_deterministic_job_id,7,11,
      'FAILED','RECONCILE_EXECUTE',1,jsonb_build_object(
        'code',v_first_cause_code,
        'causal_contract_version','WORKBENCH_FIRST_DIVERGENT_CAUSE_V1',
        'first_divergent_cause',jsonb_build_object(
          'code',v_first_cause_code,'message','Synthetic deterministic evidence failure'
        ),'first_divergent_attempt_number',1
      ),v_now,v_now,v_now),
    (v_changed_build_id,v_changed_candidate_id,v_session_id,3,
      v_snapshot_id,v_changed_run_id,v_changed_job_id,8,21,
      'FAILED','RECONCILE_EXECUTE',1,jsonb_build_object(
        'code',v_first_cause_code,
        'causal_contract_version','WORKBENCH_FIRST_DIVERGENT_CAUSE_V1',
        'first_divergent_cause',jsonb_build_object(
          'code',v_first_cause_code,'message','Synthetic superseded deterministic evidence failure'
        ),'first_divergent_attempt_number',1
      ),v_now,v_now,v_now),
    (v_lease_build_id,v_lease_candidate_id,v_session_id,3,
      v_snapshot_id,v_lease_run_id,v_lease_job_id,9,31,
      'RECONCILING','RECONCILE_EXECUTE',1,'{}'::jsonb,NULL,v_now,v_now);

  UPDATE public.banking_pay_workbench_jobs AS job_row
  SET status=CASE WHEN job_row.id=v_lease_job_id THEN 'RUNNING' ELSE 'FAILED' END,
      attempt_count=CASE WHEN job_row.id=v_lease_job_id THEN 2 ELSE 1 END,
      economic_build_id=CASE
        WHEN job_row.id=v_deterministic_job_id THEN v_deterministic_build_id
        WHEN job_row.id=v_changed_job_id THEN v_changed_build_id
        ELSE v_lease_build_id
      END,
      private_stage='RECONCILE_EXECUTE',
      private_cursor_kind='RECONCILE_EXECUTE',
      private_cursor_json=jsonb_build_object('cursor_kind','RECONCILE_EXECUTE','cursor_version',1),
      private_stage_version=1,
      started_at_utc=CASE WHEN job_row.id=v_lease_job_id THEN v_now-interval '5 minutes' ELSE NULL END,
      failed_at_utc=CASE WHEN job_row.id=v_lease_job_id THEN NULL ELSE v_now END,
      last_error_json=CASE
        WHEN job_row.id=v_lease_job_id THEN jsonb_build_object(
          'code',v_first_cause_code,
          'causal_contract_version','WORKBENCH_FIRST_DIVERGENT_CAUSE_V1',
          'first_divergent_cause',jsonb_build_object(
            'code',v_first_cause_code,'message','Synthetic earliest deterministic cause'
          ),'first_divergent_attempt_number',1,
          'latest_observed_failure',jsonb_build_object('code',v_first_cause_code),
          'latest_attempt_number',1
        )
        ELSE jsonb_build_object(
          'code',v_first_cause_code,
          'causal_contract_version','WORKBENCH_FIRST_DIVERGENT_CAUSE_V1',
          'first_divergent_cause',jsonb_build_object(
            'code',v_first_cause_code,'message',CASE
              WHEN job_row.id=v_changed_job_id
                THEN 'Synthetic superseded deterministic evidence failure'
              ELSE 'Synthetic deterministic evidence failure'
            END
          ),'first_divergent_attempt_number',1,
          'latest_observed_failure',jsonb_build_object('code',v_first_cause_code),
          'latest_attempt_number',1
        )
      END,
      updated_at_utc=clock_timestamp()
  WHERE job_row.id IN (v_deterministic_job_id,v_changed_job_id,v_lease_job_id);

  UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry_row
  SET current_build_id=CASE
        WHEN registry_row.candidate_id=v_deterministic_candidate_id THEN v_deterministic_build_id
        WHEN registry_row.candidate_id=v_changed_candidate_id THEN v_changed_build_id
        ELSE v_lease_build_id
      END,
      updated_at_utc=clock_timestamp()
  WHERE registry_row.candidate_id IN (
    v_deterministic_candidate_id,v_changed_candidate_id,v_lease_candidate_id
  );

  INSERT INTO private.banking_pay_workbench_stage_attempts(
    job_id,build_id,candidate_id,private_stage,attempt_number,worker_id,
    lane_identity,captured_candidate_generation,captured_source_change_seq,
    execution_profile_version,attempt_status,started_at_utc,
    lease_expires_at_utc,failed_at_utc,error_class,error_json,
    created_at_utc,updated_at_utc
  ) VALUES
    (v_deterministic_job_id,v_deterministic_build_id,v_deterministic_candidate_id,
      'RECONCILE_EXECUTE',1,'ROLLBACK_VERIFIER','H1:DETERMINISTIC',7,11,1,
      'FAILED',v_now-interval '4 minutes',v_now+interval '6 minutes',
      v_now-interval '3 minutes','DETERMINISTIC_STAGE_ERROR',jsonb_build_object(
        'code',v_first_cause_code,'message','Synthetic deterministic evidence failure'
      ),v_now-interval '4 minutes',v_now-interval '3 minutes'),
    (v_changed_job_id,v_changed_build_id,v_changed_candidate_id,
      'RECONCILE_EXECUTE',1,'ROLLBACK_VERIFIER','H1:CHANGED',8,21,1,
      'FAILED',v_now-interval '4 minutes',v_now+interval '6 minutes',
      v_now-interval '3 minutes','DETERMINISTIC_STAGE_ERROR',jsonb_build_object(
        'code',v_first_cause_code,'message','Synthetic superseded deterministic evidence failure'
      ),v_now-interval '4 minutes',v_now-interval '3 minutes'),
    (v_lease_job_id,v_lease_build_id,v_lease_candidate_id,
      'RECONCILE_EXECUTE',1,'ROLLBACK_VERIFIER','H1:LEASE:1',9,31,1,
      'FAILED',v_now-interval '8 minutes',v_now-interval '7 minutes',
      v_now-interval '7 minutes','DETERMINISTIC_STAGE_ERROR',jsonb_build_object(
        'code',v_first_cause_code,'message','Synthetic earliest deterministic cause'
      ),v_now-interval '8 minutes',v_now-interval '7 minutes'),
    (v_lease_job_id,v_lease_build_id,v_lease_candidate_id,
      'RECONCILE_EXECUTE',2,'ROLLBACK_VERIFIER','H1:LEASE:2',9,31,1,
      'STARTED',v_now-interval '5 minutes',v_now-interval '1 minute',NULL,NULL,
      '{}'::jsonb,v_now-interval '5 minutes',v_now-interval '5 minutes');

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,pending_job_id,error_json
  ) VALUES
    (v_session_id,v_deterministic_candidate_id,1,'SOURCE_BUILD_PENDING',true,true,
      v_deterministic_job_id,NULL),
    (v_session_id,v_changed_candidate_id,2,'SOURCE_BUILD_PENDING',true,true,
      v_changed_job_id,NULL),
    (v_session_id,v_lease_candidate_id,3,'SOURCE_BUILD_PENDING',true,true,
      v_lease_job_id,NULL);

  INSERT INTO public.banking_pay_workbench_session_candidate_state(
    session_id,candidate_id,status,source_change_seq,session_version,
    pending_job_id,last_error_json
  ) VALUES
    (v_session_id,v_deterministic_candidate_id,'PENDING',11,3,
      v_deterministic_job_id,NULL),
    (v_session_id,v_changed_candidate_id,'PENDING',21,3,
      v_changed_job_id,NULL),
    (v_session_id,v_lease_candidate_id,'PENDING',31,3,
      v_lease_job_id,NULL);

  -- Immutable synthetic policy sentinels across PAYE, Umbrella and recovery.
  -- Recovery may change only job/scope execution state; any mutation of these
  -- pre-Draft selection/economic facts is a policy change and fails release.
  INSERT INTO public.banking_pay_workbench_preview_rows(
    session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,
    key_value,selected,selection_state,status,session_version
  ) VALUES
    (
      v_session_id,v_deterministic_candidate_id,'canonical_preview_lines',
      v_prefix||':POLICY:PAYE',1,
      jsonb_build_object(
        'pay_channel','PAYE','payment_method','PAYE','sign','POSITIVE',
        'semantic_kind','WORKED_TIME_AMOUNT','economic_key','POLICY:PAYE:WORKED',
        'amount_ex_vat',125.50::numeric,'vat_amount',0::numeric,
        'amount_inc_vat',125.50::numeric,'source_reservation_amount',0::numeric,
        'prior_paid_amount_ex_vat',0::numeric,'supersession_treatment','NONE',
        'recovery_headroom_allocation',0::numeric,'selection_allowed',true,
        'draftable',true,'is_ready_for_draft',true,'approval_state','NOT_REQUIRED',
        'hold_state','NONE','resolution_state','NOT_REQUIRED',
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
        'provider_action','NONE','settlement_action','NONE'
      ),
      'POLICY_SENTINEL','PAYE:WORKED',true,'SELECTED','READY',3
    ),
    (
      v_session_id,v_changed_candidate_id,'canonical_preview_lines',
      v_prefix||':POLICY:UMBRELLA',2,
      jsonb_build_object(
        'pay_channel','UMBRELLA','payment_method','UMBRELLA','sign','POSITIVE',
        'semantic_kind','EXPENSE_DELTA','economic_key','POLICY:UMBRELLA:EXPENSE',
        'amount_ex_vat',60::numeric,'vat_amount',12::numeric,
        'amount_inc_vat',72::numeric,'source_reservation_amount',0::numeric,
        'prior_paid_amount_ex_vat',0::numeric,'supersession_treatment','NONE',
        'recovery_headroom_allocation',0::numeric,'selection_allowed',true,
        'draftable',true,'is_ready_for_draft',true,'approval_state','NOT_REQUIRED',
        'hold_state','NONE','resolution_state','NOT_REQUIRED',
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
        'provider_action','NONE','settlement_action','NONE'
      ),
      'POLICY_SENTINEL','UMBRELLA:EXPENSE',true,'SELECTED','READY',3
    ),
    (
      v_session_id,v_lease_candidate_id,'canonical_preview_lines',
      v_prefix||':POLICY:RECOVERY',3,
      jsonb_build_object(
        'pay_channel','PAYE','payment_method','PAYE','sign','NEGATIVE',
        'semantic_kind','OVERPAYMENT_RECOVERY','economic_key','POLICY:PAYE:RECOVERY',
        'amount_ex_vat',-25::numeric,'vat_amount',0::numeric,
        'amount_inc_vat',-25::numeric,'source_reservation_amount',25::numeric,
        'prior_paid_amount_ex_vat',25::numeric,'supersession_treatment','UNCHANGED',
        'recovery_headroom_allocation',25::numeric,'selection_allowed',true,
        'draftable',true,'is_ready_for_draft',true,'approval_state','NOT_REQUIRED',
        'hold_state','NONE','resolution_state','RESOLVED',
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
        'provider_action','NONE','settlement_action','NONE'
      ),
      'POLICY_SENTINEL','PAYE:RECOVERY',true,'SELECTED','READY',3
    );

  SELECT jsonb_build_object(
    'candidate_pay_methods',COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'candidate_id',candidate_row.id,'pay_method',candidate_row.pay_method
      ) ORDER BY candidate_row.id::text)
      FROM public.candidates AS candidate_row
      WHERE candidate_row.id IN (
        v_deterministic_candidate_id,v_changed_candidate_id,v_lease_candidate_id
      )
    ),'[]'::jsonb),
    'selected_preview_policy_facts',COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'id',preview_row.id,'candidate_id',preview_row.candidate_id,
        'section',preview_row.section,'row_key',preview_row.row_key,
        'row_ordinal',preview_row.row_ordinal,'row_json',preview_row.row_json,
        'key_type',preview_row.key_type,'key_value',preview_row.key_value,
        'selected',preview_row.selected,'selection_state',preview_row.selection_state,
        'status',preview_row.status,'session_version',preview_row.session_version
      ) ORDER BY preview_row.row_ordinal,preview_row.id::text)
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id=v_session_id
    ),'[]'::jsonb),
    'published_candidate_policy_fragments',COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'candidate_id',state_row.candidate_id,
        'session_version',state_row.session_version,
        'candidate',state_row.effective_candidate_fragment_json,
        'summary',state_row.effective_summary_fragment_json,
        'paye',state_row.effective_paye_candidate_json,
        'non_paye',state_row.effective_non_paye_payee_json,
        'payees',state_row.effective_payees_json,
        'case_resolutions',state_row.effective_case_resolution_states_json,
        'canonical_preview',state_row.effective_canonical_preview_lines_json
      ) ORDER BY state_row.candidate_id::text)
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      WHERE state_row.session_id=v_session_id
    ),'[]'::jsonb),
    'financial_boundary_counts',jsonb_build_object(
      'operations',(SELECT COUNT(*) FROM public.banking_pay_operations),
      'batches',(SELECT COUNT(*) FROM public.pay_batches),
      'provider_attempts',(SELECT COUNT(*) FROM public.banking_pay_operation_provider_attempts),
      'settlement_scope',(SELECT COUNT(*) FROM public.banking_pay_operation_settlement_scope),
      'remittance_scope',(SELECT COUNT(*) FROM public.banking_pay_operation_remittance_scope)
    )
  ) INTO STRICT v_policy_projection_before;

  SELECT COUNT(*) INTO v_preview_before
  FROM public.banking_pay_workbench_preview_rows
  WHERE session_id=v_session_id;
  SELECT COUNT(*) INTO v_operations_before FROM public.banking_pay_operations;
  SELECT COUNT(*) INTO v_batches_before FROM public.pay_batches;
  SELECT COUNT(*) INTO v_provider_before
  FROM public.banking_pay_operation_provider_attempts;
  SELECT COUNT(*) INTO v_settlement_before
  FROM public.banking_pay_operation_settlement_scope;
  SELECT COUNT(*) INTO v_remittance_before
  FROM public.banking_pay_operation_remittance_scope;

  -- Unchanged current deterministic authority must fail closed without a
  -- successor and must retain the terminal job/attempt evidence.
  v_result:=public.pay_workbench_repair_orphaned_pending_source_build(
    v_session_id,v_deterministic_candidate_id,10,clock_timestamp(),
    'H1_DETERMINISTIC_FIRST_USE'
  );
  IF COALESCE((v_result->>'failed_closed_count')::integer,-1)<>1
     OR COALESCE((v_result->>'enqueued_count')::integer,-1)<>0
     OR NOT EXISTS(
       SELECT 1 FROM jsonb_array_elements(COALESCE(v_result->'results','[]'::jsonb)) item
       WHERE item->>'candidate_id'=v_deterministic_candidate_id::text
         AND item->>'action'='FAILED_CLOSED_DETERMINISTIC_SOURCE'
         AND COALESCE((item->>'state_transition_proven')::boolean,false) IS TRUE
     ) THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DETERMINISTIC_FAIL_CLOSE_INVALID',
      DETAIL=v_result::text;
  END IF;

  SELECT scope_row.status AS scope_status,scope_row.pending_job_id,scope_row.dirty,
    scope_row.error_json->>'code' AS scope_code,
    scope_row.error_json#>>'{first_divergent_cause,code}' AS scope_first_code,
    scope_row.error_json->>'automatic_recovery_scheduled' AS auto_recovery,
    scope_row.error_json->>'deterministic_successor_suppressed' AS suppressed,
    state_row.status AS state_status,state_row.pending_job_id AS state_pending_job_id,
    state_row.last_error_json#>>'{first_divergent_cause,code}' AS state_first_code
  INTO STRICT v_row
  FROM public.banking_pay_workbench_session_scope AS scope_row
  JOIN public.banking_pay_workbench_session_candidate_state AS state_row
    ON state_row.session_id=scope_row.session_id
   AND state_row.candidate_id=scope_row.candidate_id
  WHERE scope_row.session_id=v_session_id
    AND scope_row.candidate_id=v_deterministic_candidate_id;
  IF v_row.scope_status IS DISTINCT FROM 'SOURCE_BUILD_ERROR'
     OR v_row.pending_job_id IS NOT NULL
     OR v_row.dirty IS NOT TRUE
     OR v_row.scope_code IS DISTINCT FROM v_first_cause_code
     OR v_row.scope_first_code IS DISTINCT FROM v_first_cause_code
     OR lower(COALESCE(v_row.auto_recovery,'false'))<>'false'
     OR lower(COALESCE(v_row.suppressed,'false'))<>'true'
     OR v_row.state_status IS DISTINCT FROM 'FAILED'
     OR v_row.state_pending_job_id IS NOT NULL
     OR v_row.state_first_code IS DISTINCT FROM v_first_cause_code THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DETERMINISTIC_TERMINAL_STATE_INVALID',
      DETAIL=to_jsonb(v_row)::text;
  END IF;

  IF EXISTS(
    SELECT 1 FROM public.banking_pay_workbench_jobs AS successor
    WHERE successor.session_id=v_session_id
      AND successor.candidate_id=v_deterministic_candidate_id
      AND successor.id<>v_deterministic_job_id
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_DETERMINISTIC_SUCCESSOR_CREATED';
  END IF;

  SELECT COUNT(*) INTO v_jobs_before_replay
  FROM public.banking_pay_workbench_jobs
  WHERE session_id=v_session_id
    AND candidate_id=v_deterministic_candidate_id;
  FOR replay_index IN 1..3 LOOP
    v_replay_result:=public.pay_workbench_repair_orphaned_pending_source_build(
      v_session_id,v_deterministic_candidate_id,10,clock_timestamp(),
      'H1_DETERMINISTIC_REPLAY'
    );
    IF COALESCE((v_replay_result->>'examined_count')::integer,-1)<>0
       OR COALESCE((v_replay_result->>'enqueued_count')::integer,-1)<>0 THEN
      RAISE EXCEPTION USING MESSAGE='BANKING_PAY_DETERMINISTIC_REPLAY_NOT_NOOP',
        DETAIL=v_replay_result::text;
    END IF;
  END LOOP;
  IF (SELECT COUNT(*) FROM public.banking_pay_workbench_jobs
      WHERE session_id=v_session_id
        AND candidate_id=v_deterministic_candidate_id)<>v_jobs_before_replay THEN
    RAISE EXCEPTION 'BANKING_PAY_DETERMINISTIC_REPLAY_CREATED_JOB';
  END IF;

  -- A real source sequence change after fail-close remains recoverable only
  -- through the existing canonical enqueue owner.
  UPDATE public.app_change_counters
  SET seq=12,updated_at=clock_timestamp()
  WHERE entity_key='pay_candidate:'||v_deterministic_candidate_id::text;
  v_enqueue_result:=public.pay_workbench_enqueue_candidate_refresh(
    p_snapshot_run_id=>v_snapshot_id,
    p_candidate_id=>v_deterministic_candidate_id,
    p_reason=>'H1_GENUINE_SOURCE_CHANGE_AFTER_FAIL_CLOSE',
    p_actor_user_id=>v_actor_id,
    p_payload_json=>jsonb_build_object(
      'session_id',v_session_id::text,'source_session_id',v_session_id::text,
      'candidate_id',v_deterministic_candidate_id::text,'session_version',3,
      'source_change_seq',12,'refresh_scope_kind','CANDIDATE_FULL_LIVE',
      'pay_channel_scope','ALL','force_legacy',true,'force_broad_legacy',true,
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
    )
  );
  IF COALESCE(v_enqueue_result->>'job_id','') !~*
       '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_GENUINE_CHANGE_NOT_ENQUEUED',
      DETAIL=v_enqueue_result::text;
  END IF;
  v_successor_id:=(v_enqueue_result->>'job_id')::uuid;
  IF (SELECT COUNT(*) FROM public.banking_pay_workbench_jobs
      WHERE session_id=v_session_id
        AND candidate_id=v_deterministic_candidate_id
        AND status IN ('QUEUED','RUNNING'))<>1
     OR NOT EXISTS(
       SELECT 1 FROM public.banking_pay_workbench_jobs AS successor
       JOIN public.banking_pay_workbench_session_scope AS scope_row
         ON scope_row.pending_job_id=successor.id
       WHERE successor.id=v_successor_id
         AND successor.status='QUEUED'
         AND successor.attempt_count=0
         AND successor.max_attempts=8
         AND successor.payload_json->>'source_change_seq'='12'
         AND scope_row.session_id=v_session_id
         AND scope_row.candidate_id=v_deterministic_candidate_id
         AND scope_row.status='SOURCE_BUILD_PENDING'
         AND scope_row.error_json IS NULL
     ) THEN
    RAISE EXCEPTION 'BANKING_PAY_GENUINE_CHANGE_SUCCESSOR_INVALID';
  END IF;

  -- If source authority advances before repair, the old deterministic
  -- generation must not poison the new generation; normal successor recovery
  -- remains in force.
  UPDATE public.app_change_counters
  SET seq=22,updated_at=clock_timestamp()
  WHERE entity_key='pay_candidate:'||v_changed_candidate_id::text;
  v_result:=public.pay_workbench_repair_orphaned_pending_source_build(
    v_session_id,v_changed_candidate_id,10,clock_timestamp(),
    'H1_CHANGED_SOURCE_FIRST_USE'
  );
  IF COALESCE((v_result->>'enqueued_count')::integer,-1)<>1
     OR COALESCE((v_result->>'failed_closed_count')::integer,-1)<>0
     OR NOT EXISTS(
       SELECT 1 FROM jsonb_array_elements(COALESCE(v_result->'results','[]'::jsonb)) item
       WHERE item->>'candidate_id'=v_changed_candidate_id::text
         AND item->>'action'='ENQUEUED_CANONICAL_SUCCESSOR'
         AND COALESCE((item->>'state_transition_proven')::boolean,false) IS TRUE
     )
     OR (SELECT COUNT(*) FROM public.banking_pay_workbench_jobs
         WHERE session_id=v_session_id AND candidate_id=v_changed_candidate_id
           AND status IN ('QUEUED','RUNNING'))<>1 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_CHANGED_SOURCE_RECOVERY_INVALID',
      DETAIL=v_result::text;
  END IF;

  -- Lease expiry may be the latest terminal label, but the earliest
  -- deterministic cause remains the causal authority and suppresses a fresh
  -- unchanged successor.
  v_result:=public.pay_workbench_repair_orphaned_pending_source_build(
    v_session_id,v_lease_candidate_id,10,clock_timestamp(),
    'H1_LEASE_EXHAUSTION_FIRST_CAUSE'
  );
  SELECT job_row.status,job_row.last_error_json->>'code' AS latest_code,
    job_row.last_error_json#>>'{first_divergent_cause,code}' AS first_code,
    job_row.last_error_json->>'latest_attempt_number' AS latest_attempt_number,
    build_row.status AS build_status,
    build_row.failure_json#>>'{first_divergent_cause,code}' AS build_first_code,
    attempt_row.attempt_status,attempt_row.result_code
  INTO STRICT v_row
  FROM public.banking_pay_workbench_jobs AS job_row
  JOIN private.banking_pay_workbench_economic_builds AS build_row
    ON build_row.id=job_row.economic_build_id
  JOIN private.banking_pay_workbench_stage_attempts AS attempt_row
    ON attempt_row.job_id=job_row.id AND attempt_row.attempt_number=2
  WHERE job_row.id=v_lease_job_id;
  IF COALESCE((v_result->>'expired_attempt_count')::integer,-1)<>1
     OR COALESCE((v_result->>'expired_attempt_exhausted_count')::integer,-1)<>1
     OR COALESCE((v_result->>'failed_closed_count')::integer,-1)<>1
     OR COALESCE((v_result->>'enqueued_count')::integer,-1)<>0
     OR v_row.status IS DISTINCT FROM 'FAILED'
     OR v_row.latest_code IS DISTINCT FROM 'DELIVERED_ATTEMPT_EXHAUSTED'
     OR v_row.first_code IS DISTINCT FROM v_first_cause_code
     OR v_row.latest_attempt_number IS DISTINCT FROM '2'
     OR v_row.build_status IS DISTINCT FROM 'FAILED'
     OR v_row.build_first_code IS DISTINCT FROM v_first_cause_code
     OR v_row.attempt_status IS DISTINCT FROM 'EXPIRED'
     OR v_row.result_code IS DISTINCT FROM 'LEASE_EXPIRED_AFTER_CANCELLATION_GRACE'
     OR EXISTS(
       SELECT 1 FROM public.banking_pay_workbench_jobs AS successor
       WHERE successor.session_id=v_session_id
         AND successor.candidate_id=v_lease_candidate_id
         AND successor.id<>v_lease_job_id
     ) THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_LEASE_FIRST_CAUSE_INVALID',
      DETAIL=jsonb_build_object('result',v_result,'row',to_jsonb(v_row))::text;
  END IF;

  IF (SELECT COUNT(*) FROM public.banking_pay_workbench_preview_rows
      WHERE session_id=v_session_id)<>v_preview_before
     OR (SELECT COUNT(*) FROM public.banking_pay_operations)<>v_operations_before
     OR (SELECT COUNT(*) FROM public.pay_batches)<>v_batches_before
     OR (SELECT COUNT(*) FROM public.banking_pay_operation_provider_attempts)<>v_provider_before
     OR (SELECT COUNT(*) FROM public.banking_pay_operation_settlement_scope)<>v_settlement_before
     OR (SELECT COUNT(*) FROM public.banking_pay_operation_remittance_scope)<>v_remittance_before THEN
    RAISE EXCEPTION 'BANKING_PAY_CAUSAL_RECOVERY_CROSSED_FINANCIAL_BOUNDARY';
  END IF;

  SELECT jsonb_build_object(
    'candidate_pay_methods',COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'candidate_id',candidate_row.id,'pay_method',candidate_row.pay_method
      ) ORDER BY candidate_row.id::text)
      FROM public.candidates AS candidate_row
      WHERE candidate_row.id IN (
        v_deterministic_candidate_id,v_changed_candidate_id,v_lease_candidate_id
      )
    ),'[]'::jsonb),
    'selected_preview_policy_facts',COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'id',preview_row.id,'candidate_id',preview_row.candidate_id,
        'section',preview_row.section,'row_key',preview_row.row_key,
        'row_ordinal',preview_row.row_ordinal,'row_json',preview_row.row_json,
        'key_type',preview_row.key_type,'key_value',preview_row.key_value,
        'selected',preview_row.selected,'selection_state',preview_row.selection_state,
        'status',preview_row.status,'session_version',preview_row.session_version
      ) ORDER BY preview_row.row_ordinal,preview_row.id::text)
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id=v_session_id
    ),'[]'::jsonb),
    'published_candidate_policy_fragments',COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'candidate_id',state_row.candidate_id,
        'session_version',state_row.session_version,
        'candidate',state_row.effective_candidate_fragment_json,
        'summary',state_row.effective_summary_fragment_json,
        'paye',state_row.effective_paye_candidate_json,
        'non_paye',state_row.effective_non_paye_payee_json,
        'payees',state_row.effective_payees_json,
        'case_resolutions',state_row.effective_case_resolution_states_json,
        'canonical_preview',state_row.effective_canonical_preview_lines_json
      ) ORDER BY state_row.candidate_id::text)
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      WHERE state_row.session_id=v_session_id
    ),'[]'::jsonb),
    'financial_boundary_counts',jsonb_build_object(
      'operations',(SELECT COUNT(*) FROM public.banking_pay_operations),
      'batches',(SELECT COUNT(*) FROM public.pay_batches),
      'provider_attempts',(SELECT COUNT(*) FROM public.banking_pay_operation_provider_attempts),
      'settlement_scope',(SELECT COUNT(*) FROM public.banking_pay_operation_settlement_scope),
      'remittance_scope',(SELECT COUNT(*) FROM public.banking_pay_operation_remittance_scope)
    )
  ) INTO STRICT v_policy_projection_after;

  IF v_policy_projection_after IS DISTINCT FROM v_policy_projection_before THEN
    RAISE EXCEPTION USING
      MESSAGE='BANKING_PAY_CAUSAL_RECOVERY_PAYMENT_POLICY_PARITY_CHANGED',
      DETAIL=jsonb_build_object(
        'before',v_policy_projection_before,
        'after',v_policy_projection_after
      )::text;
  END IF;

  RAISE NOTICE 'PASS: deterministic current authority fails closed without successors; replay is a no-op; genuine changes recover canonically; lease exhaustion preserves the first cause; exact before/after payment-policy projection is unchanged.';
END;
$verification$;

ROLLBACK;
