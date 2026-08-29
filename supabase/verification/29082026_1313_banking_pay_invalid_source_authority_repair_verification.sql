\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for historical queued source-build jobs
-- which predate the current authority-fingerprint contract.  These rows are
-- refresh instructions only.  The verifier proves that they fail the current
-- claim, are terminalised under Candidate serial authority, are replaced only
-- through the canonical enqueue owner, cannot remain current, and do not touch
-- Draft, batch, provider, payment, settlement or remittance authority.
BEGIN;
SET LOCAL statement_timeout='30s';

DO $verification$
DECLARE
  v_snapshot_id uuid:=gen_random_uuid();
  v_actor_id uuid:=gen_random_uuid();
  v_session_id uuid:=gen_random_uuid();
  v_candidate_repair uuid:=gen_random_uuid();
  v_candidate_preserve uuid:=gen_random_uuid();
  v_invalid_job_one uuid:=gen_random_uuid();
  v_invalid_job_two uuid:=gen_random_uuid();
  v_invalid_stray_job uuid:=gen_random_uuid();
  v_preserved_owner_id uuid:=NULL::uuid;
  v_repaired_owner_id uuid:=NULL::uuid;
  v_prefix text:='BANKING_PAY_INVALID_SOURCE_AUTHORITY_VERIFY:' || gen_random_uuid()::text;
  v_result jsonb:='{}'::jsonb;
  v_replay_result jsonb:='{}'::jsonb;
  v_claim_error text:=NULL::text;
  v_owner record;
  v_terminal record;
  v_operations_before bigint:=0;
  v_batches_before bigint:=0;
  v_provider_before bigint:=0;
  v_settlement_before bigint:=0;
  v_remittance_before bigint:=0;
BEGIN
  UPDATE public.settings_defaults
  SET banking_pay_same_authority_build_election_v1_enabled=true
  WHERE id=1;

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
    'bpay-invalid-authority-' || replace(v_actor_id::text,'-','') || '@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true
  );

  INSERT INTO public.candidates(id,display_name,tms_ref) VALUES
    (v_candidate_repair,v_prefix || ':REPAIR',v_prefix || ':REPAIR'),
    (v_candidate_preserve,v_prefix || ':PRESERVE',v_prefix || ':PRESERVE');

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version,discarded_at_utc
  ) VALUES (
    v_session_id,v_actor_id,DATE '2099-01-09',DATE '2099-01-04',
    v_prefix || ':SESSION',v_snapshot_id,'OPEN',7,NULL
  );

  INSERT INTO public.app_change_counters(entity_key,seq) VALUES
    ('pay_candidate:' || v_candidate_repair::text,31),
    ('pay_candidate:' || v_candidate_preserve::text,41)
  ON CONFLICT(entity_key) DO UPDATE SET seq=EXCLUDED.seq;

  UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry_row
  SET initialisation_status='READY',
      dirty_generation=CASE
        WHEN registry_row.candidate_id=v_candidate_repair THEN 3 ELSE 4 END,
      evaluated_generation=CASE
        WHEN registry_row.candidate_id=v_candidate_repair THEN 2 ELSE 3 END,
      current_source_change_seq=CASE
        WHEN registry_row.candidate_id=v_candidate_repair THEN 31 ELSE 41 END,
      last_dirty_reason='VERIFICATION_DIRTY',
      last_dirtied_at_utc=clock_timestamp(),
      last_evaluated_at_utc=clock_timestamp(),
      initialised_at_utc=clock_timestamp(),
      failure_json='{}'::jsonb,
      updated_at_utc=clock_timestamp()
  WHERE registry_row.candidate_id IN (v_candidate_repair,v_candidate_preserve);

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version
  ) VALUES
    (v_invalid_job_one,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',10,
      clock_timestamp(),0,8,v_prefix || ':INVALID_ONE',v_snapshot_id,
      v_session_id,v_candidate_repair,pg_catalog.jsonb_build_object(
        'session_id',v_session_id::text,'session_version',7,
        'source_change_seq',31,'scope_change_generation',3,
        'source_build_run_id','10000000-0000-4000-8000-000000001301',
        'refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL',
        'reason','DIRTY_TRIGGER:CONTRACTS:UPDATE'
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1),
    (v_invalid_job_two,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',20,
      clock_timestamp(),0,8,v_prefix || ':INVALID_TWO',v_snapshot_id,
      v_session_id,v_candidate_repair,pg_catalog.jsonb_build_object(
        'session_id',v_session_id::text,'session_version',7,
        'source_change_seq',31,'scope_change_generation',3,
        'source_build_run_id','10000000-0000-4000-8000-000000001302',
        'authority_fingerprint_version',3,
        'authority_fingerprint',repeat('a',64),
        'required_physical_publication_contract_version','not-a-version',
        'refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL',
        'reason','DIRTY_TRIGGER:CLIENT_SETTINGS:UPDATE'
      ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1);

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,pending_job_id,error_json
  ) VALUES
    (v_session_id,v_candidate_repair,1,'SOURCE_BUILD_PENDING',true,true,v_invalid_job_one,NULL),
    (v_session_id,v_candidate_preserve,2,'READY',true,false,NULL,NULL);

  INSERT INTO public.banking_pay_workbench_session_candidate_state(
    session_id,candidate_id,status,source_change_seq,session_version,pending_job_id,last_error_json
  ) VALUES
    (v_session_id,v_candidate_repair,'PENDING',31,7,v_invalid_job_one,NULL),
    (v_session_id,v_candidate_preserve,'READY',41,7,NULL,NULL);

  v_result:=public.pay_workbench_enqueue_candidate_refresh(
    p_snapshot_run_id=>v_snapshot_id,
    p_candidate_id=>v_candidate_preserve,
    p_reason=>'VERIFICATION_CURRENT_CANONICAL_OWNER',
    p_actor_user_id=>v_actor_id,
    p_payload_json=>pg_catalog.jsonb_build_object(
      'session_id',v_session_id::text,
      'source_session_id',v_session_id::text,
      'candidate_id',v_candidate_preserve::text,
      'session_version',7,
      'source_change_seq',41,
      'refresh_scope_kind','CANDIDATE_FULL_LIVE',
      'pay_channel_scope','ALL',
      'force_legacy',true,
      'force_broad_legacy',true
    )
  );
  IF COALESCE(v_result->>'job_id','') !~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_INVALID_AUTHORITY_CANONICAL_FIXTURE_NOT_CREATED',
      DETAIL=v_result::text;
  END IF;
  v_preserved_owner_id:=(v_result->>'job_id')::uuid;

  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
    snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
    private_stage,private_cursor_kind,private_cursor_json,private_stage_version
  ) VALUES (
    v_invalid_stray_job,'WORKBENCH_CANDIDATE_SOURCE_BUILD','QUEUED',30,
    clock_timestamp(),0,8,v_prefix || ':INVALID_STRAY',v_snapshot_id,
    v_session_id,v_candidate_preserve,pg_catalog.jsonb_build_object(
      'session_id',v_session_id::text,'session_version',7,
      'source_change_seq',41,'scope_change_generation',4,
      'source_build_run_id','10000000-0000-4000-8000-000000001303',
      'refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL',
      'reason','DIRTY_TRIGGER:CONTRACTS:UPDATE'
    ),NULL,'BUILD_INITIALISE','BUILD_INITIALISE','{}'::jsonb,1
  );

  SELECT COUNT(*) INTO v_operations_before FROM public.banking_pay_operations;
  SELECT COUNT(*) INTO v_batches_before FROM public.pay_batches;
  SELECT COUNT(*) INTO v_provider_before FROM public.banking_pay_operation_provider_attempts;
  SELECT COUNT(*) INTO v_settlement_before FROM public.banking_pay_operation_settlement_scope;
  SELECT COUNT(*) INTO v_remittance_before FROM public.banking_pay_operation_remittance_scope;

  BEGIN
    PERFORM public.pay_workbench_source_build_attempt_claim_start_v1(
      'invalid-authority-verifier','invalid-authority-verifier:lane:0',25,
      clock_timestamp(),v_session_id,v_candidate_repair
    );
    RAISE EXCEPTION 'BANKING_PAY_INVALID_AUTHORITY_OLD_FAILURE_NOT_REPRODUCED';
  EXCEPTION WHEN SQLSTATE '22023' THEN
    GET STACKED DIAGNOSTICS v_claim_error=MESSAGE_TEXT;
    IF v_claim_error IS DISTINCT FROM 'PAY_WORKBENCH_SOURCE_BUILD_AUTHORITY_FINGERPRINT_REQUIRED' THEN
      RAISE EXCEPTION USING MESSAGE='BANKING_PAY_INVALID_AUTHORITY_WRONG_OLD_FAILURE',
        DETAIL=COALESCE(v_claim_error,'NULL');
    END IF;
  END;

  v_result:=public.pay_workbench_repair_invalid_source_authority_jobs_v1(
    v_session_id,NULL::uuid,10,'VERIFICATION_INVALID_SOURCE_AUTHORITY_REPAIR'
  );
  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_result->>'examined_candidate_count')::integer,-1)<>2
     OR COALESCE((v_result->>'repaired_candidate_count')::integer,-1)<>2
     OR COALESCE((v_result->>'terminalised_job_count')::integer,-1)<>3
     OR COALESCE((v_result->>'failed_count')::integer,-1)<>0
     OR COALESCE((v_result->>'remaining_invalid_active_count')::integer,-1)<>0
     OR COALESCE((v_result->>'all_state_transitions_proven')::boolean,false) IS NOT TRUE THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_INVALID_AUTHORITY_REPAIR_RESULT_INVALID',
      DETAIL=v_result::text;
  END IF;

  FOR v_terminal IN
    SELECT status,completed_at_utc,failed_at_utc,economic_build_id,private_stage,
      private_cursor_kind,private_cursor_json,private_stage_version,
      last_error_json->>'code' AS error_code,
      payload_json->>'invalid_source_authority_terminalised' AS terminalised
    FROM public.banking_pay_workbench_jobs
    WHERE id IN (v_invalid_job_one,v_invalid_job_two,v_invalid_stray_job)
  LOOP
    IF v_terminal.status IS DISTINCT FROM 'DEAD'
       OR v_terminal.completed_at_utc IS NOT NULL
       OR v_terminal.failed_at_utc IS NULL
       OR v_terminal.economic_build_id IS NOT NULL
       OR v_terminal.private_stage IS NOT NULL
       OR v_terminal.private_cursor_kind IS NOT NULL
       OR v_terminal.private_cursor_json IS DISTINCT FROM '{}'::jsonb
       OR v_terminal.private_stage_version IS NOT NULL
       OR v_terminal.error_code IS DISTINCT FROM
          'SOURCE_BUILD_AUTHORITY_PAYLOAD_INVALID_TERMINALISED'
       OR LOWER(COALESCE(v_terminal.terminalised,'false'))<>'true' THEN
      RAISE EXCEPTION USING MESSAGE='BANKING_PAY_INVALID_AUTHORITY_TERMINAL_SHAPE_INVALID',
        DETAIL=pg_catalog.to_jsonb(v_terminal)::text;
    END IF;
  END LOOP;

  SELECT scope_row.pending_job_id,scope_row.status AS scope_status,scope_row.dirty,
    scope_row.error_json,owner_job.status AS owner_status,
    owner_job.payload_json->>'created_by_helper' AS created_by_helper,
    owner_job.payload_json->>'session_version' AS session_version,
    owner_job.payload_json->>'source_change_seq' AS source_change_seq,
    owner_job.payload_json->>'source_build_run_id' AS source_build_run_id,
    owner_job.payload_json->>'authority_fingerprint_version' AS fingerprint_version,
    owner_job.payload_json->>'authority_fingerprint' AS fingerprint,
    state_row.status AS state_status,state_row.pending_job_id AS state_pending_job_id,
    state_row.session_version AS state_session_version,
    state_row.source_change_seq AS state_source_change_seq
  INTO STRICT v_owner
  FROM public.banking_pay_workbench_session_scope AS scope_row
  JOIN public.banking_pay_workbench_jobs AS owner_job ON owner_job.id=scope_row.pending_job_id
  JOIN public.banking_pay_workbench_session_candidate_state AS state_row
    ON state_row.session_id=scope_row.session_id
   AND state_row.candidate_id=scope_row.candidate_id
  WHERE scope_row.session_id=v_session_id
    AND scope_row.candidate_id=v_candidate_repair;
  v_repaired_owner_id:=v_owner.pending_job_id;

  IF v_repaired_owner_id IN (v_invalid_job_one,v_invalid_job_two)
     OR v_owner.scope_status IS DISTINCT FROM 'SOURCE_BUILD_PENDING'
     OR v_owner.dirty IS NOT TRUE
     OR v_owner.error_json IS NOT NULL
     OR v_owner.owner_status NOT IN ('QUEUED','RUNNING')
     OR v_owner.created_by_helper IS DISTINCT FROM 'pay_workbench_enqueue_candidate_refresh'
     OR v_owner.session_version IS DISTINCT FROM '7'
     OR v_owner.source_change_seq::bigint<31
     OR v_owner.source_build_run_id !~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR v_owner.fingerprint_version NOT IN ('2','3')
     OR v_owner.fingerprint !~ '^[0-9a-f]{64}$'
     OR v_owner.state_status IS DISTINCT FROM 'PENDING'
     OR v_owner.state_pending_job_id IS DISTINCT FROM v_repaired_owner_id
     OR v_owner.state_session_version IS DISTINCT FROM 7
     OR v_owner.state_source_change_seq<31 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_INVALID_AUTHORITY_REPLACEMENT_OWNER_INVALID',
      DETAIL=pg_catalog.to_jsonb(v_owner)::text;
  END IF;

  SELECT scope_row.pending_job_id,owner_job.payload_json->>'created_by_helper' AS created_by_helper,
    owner_job.payload_json->>'authority_fingerprint' AS fingerprint
  INTO STRICT v_owner
  FROM public.banking_pay_workbench_session_scope AS scope_row
  JOIN public.banking_pay_workbench_jobs AS owner_job ON owner_job.id=scope_row.pending_job_id
  WHERE scope_row.session_id=v_session_id
    AND scope_row.candidate_id=v_candidate_preserve;
  IF v_owner.pending_job_id IS DISTINCT FROM v_preserved_owner_id
     OR v_owner.created_by_helper IS DISTINCT FROM 'pay_workbench_enqueue_candidate_refresh'
     OR v_owner.fingerprint !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_INVALID_AUTHORITY_CURRENT_OWNER_NOT_PRESERVED',
      DETAIL=pg_catalog.to_jsonb(v_owner)::text;
  END IF;

  IF EXISTS(
    SELECT 1 FROM public.banking_pay_workbench_session_scope AS current_scope
    WHERE current_scope.session_id=v_session_id
      AND current_scope.pending_job_id IN (
        v_invalid_job_one,v_invalid_job_two,v_invalid_stray_job
      )
  ) OR EXISTS(
    SELECT 1 FROM public.banking_pay_workbench_session_candidate_state AS current_state
    WHERE current_state.session_id=v_session_id
      AND current_state.pending_job_id IN (
        v_invalid_job_one,v_invalid_job_two,v_invalid_stray_job
      )
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_INVALID_AUTHORITY_DEAD_JOB_REMAINS_CURRENT';
  END IF;

  v_replay_result:=public.pay_workbench_repair_invalid_source_authority_jobs_v1(
    v_session_id,NULL::uuid,10,'VERIFICATION_INVALID_SOURCE_AUTHORITY_REPAIR_REPLAY'
  );
  IF COALESCE((v_replay_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_replay_result->>'examined_candidate_count')::integer,-1)<>0
     OR COALESCE((v_replay_result->>'repaired_candidate_count')::integer,-1)<>0
     OR COALESCE((v_replay_result->>'terminalised_job_count')::integer,-1)<>0
     OR COALESCE((v_replay_result->>'remaining_invalid_active_count')::integer,-1)<>0 THEN
    RAISE EXCEPTION USING MESSAGE='BANKING_PAY_INVALID_AUTHORITY_REPAIR_NOT_IDEMPOTENT',
      DETAIL=v_replay_result::text;
  END IF;

  IF (SELECT pending_job_id FROM public.banking_pay_workbench_session_scope
      WHERE session_id=v_session_id AND candidate_id=v_candidate_repair)
        IS DISTINCT FROM v_repaired_owner_id
     OR (SELECT pending_job_id FROM public.banking_pay_workbench_session_scope
      WHERE session_id=v_session_id AND candidate_id=v_candidate_preserve)
        IS DISTINCT FROM v_preserved_owner_id THEN
    RAISE EXCEPTION 'BANKING_PAY_INVALID_AUTHORITY_REPLAY_CHANGED_CURRENT_OWNER';
  END IF;

  IF (SELECT COUNT(*) FROM public.banking_pay_operations)<>v_operations_before
     OR (SELECT COUNT(*) FROM public.pay_batches)<>v_batches_before
     OR (SELECT COUNT(*) FROM public.banking_pay_operation_provider_attempts)<>v_provider_before
     OR (SELECT COUNT(*) FROM public.banking_pay_operation_settlement_scope)<>v_settlement_before
     OR (SELECT COUNT(*) FROM public.banking_pay_operation_remittance_scope)<>v_remittance_before THEN
    RAISE EXCEPTION 'BANKING_PAY_INVALID_AUTHORITY_FINANCIAL_LIFECYCLE_CHANGED';
  END IF;

  IF pg_catalog.has_function_privilege(
       'anon','public.pay_workbench_repair_invalid_source_authority_jobs_v1(uuid,uuid,integer,text)','EXECUTE'
     )
     OR pg_catalog.has_function_privilege(
       'authenticated','public.pay_workbench_repair_invalid_source_authority_jobs_v1(uuid,uuid,integer,text)','EXECUTE'
     )
     OR NOT pg_catalog.has_function_privilege(
       'service_role','public.pay_workbench_repair_invalid_source_authority_jobs_v1(uuid,uuid,integer,text)','EXECUTE'
     ) THEN
    RAISE EXCEPTION 'BANKING_PAY_INVALID_AUTHORITY_REPAIR_ACL_INVALID';
  END IF;
END;
$verification$;

ROLLBACK;
