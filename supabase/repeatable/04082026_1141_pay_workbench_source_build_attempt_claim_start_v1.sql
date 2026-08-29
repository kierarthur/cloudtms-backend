-- Banking Pay bounded-scope V1.2.8: durable transaction-one claim/start RPC.
-- Each lane owns durable CLAIM and RECOVERY keyset cursors.  The cursor is
-- advanced before advisory/exact-row locking, so an indefinitely locked prefix
-- cannot make a later eligible job invisible across calls.

CREATE OR REPLACE FUNCTION public.pay_workbench_source_build_attempt_claim_start_v1(
  p_worker_id text,
  p_lane_identity text,
  p_lease_seconds integer DEFAULT NULL::integer,
  p_now_utc timestamptz DEFAULT NULL::timestamptz,
  p_session_id uuid DEFAULT NULL::uuid,
  p_candidate_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_database_now timestamptz := clock_timestamp();
  v_now timestamptz := LEAST(COALESCE(p_now_utc,v_database_now),v_database_now);
  v_worker_id text := NULLIF(btrim(COALESCE(p_worker_id,'')),'');
  v_lane_identity text := NULLIF(btrim(COALESCE(p_lane_identity,'')),'');
  v_configured_lease integer := 25;
  v_effective_lease integer;
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_build_id uuid;
  v_claimed_job_id uuid;
  v_attempt_id uuid;
  v_attempt_nonce uuid;
  v_attempt_number integer;
  v_stage text;
  v_cursor_kind text;
  v_cursor_json jsonb;
  v_source_build_run_id uuid;
  v_source_change_seq bigint;
  v_captured_generation bigint;
  v_attempt_started_at timestamptz;
  v_lease_expires timestamptz;
  v_bootstrap_id uuid;
  v_is_bootstrap boolean:=false;
  v_recovery record;
  v_claim record;
  v_recovery_cursor_generation bigint;
  v_recovery_cursor_due_at timestamptz;
  v_recovery_cursor_object_id uuid;
  v_claim_cursor_generation bigint;
  v_claim_cursor_chain_rank smallint;
  v_claim_cursor_priority integer;
  v_claim_cursor_due_at timestamptz;
  v_claim_cursor_created_at timestamptz;
  v_claim_cursor_object_id uuid;
  v_scan_scope_key text;
  v_recovery_examined integer:=0;
  v_claim_examined integer:=0;
  v_recovered_count integer:=0;
  v_scan_limit integer:=50;
  -- Four source-build lanes may collect and publish concurrently, but the
  -- reconciliation stage performs the expensive finance/effect comparison.
  -- Admit at most two such attempts at once so four Worker lanes cannot turn
  -- a healthy ~6 second attempt into a 25 second lease-expiry stampede.
  v_reconcile_attempt_limit integer:=2;
BEGIN
  IF v_worker_id IS NULL OR v_lane_identity IS NULL
     OR char_length(v_worker_id)>200 OR char_length(v_lane_identity)>200 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_CALLER_INVALID'
      USING ERRCODE='22023', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_ATTEMPT_CALLER_INVALID'
      )::text;
  END IF;
  IF p_now_utc IS NOT NULL AND abs(extract(epoch FROM p_now_utc-v_database_now))>300 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_CUTOFF_INVALID' USING ERRCODE='22023';
  END IF;

  SELECT COALESCE(settings_row.banking_pay_workbench_db_worker_lease_seconds,25)
  INTO v_configured_lease
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;
  v_configured_lease := LEAST(GREATEST(COALESCE(v_configured_lease,25),5),120);
  v_effective_lease := LEAST(
    v_configured_lease,
    LEAST(GREATEST(COALESCE(p_lease_seconds,v_configured_lease),5),120)
  );

  v_scan_scope_key:=COALESCE(p_session_id::text,'*')||':'||
    COALESCE(p_candidate_id::text,'*');

  INSERT INTO private.banking_pay_workbench_queue_scan_state(
    lane_identity,scan_kind,scan_scope_key)
  VALUES(v_lane_identity,'RECOVERY',v_scan_scope_key)
  ON CONFLICT(lane_identity,scan_kind,scan_scope_key) DO NOTHING;
  SELECT cursor_generation,cursor_due_at,cursor_object_id
  INTO v_recovery_cursor_generation,v_recovery_cursor_due_at,v_recovery_cursor_object_id
  FROM private.banking_pay_workbench_queue_scan_state
  WHERE lane_identity=v_lane_identity AND scan_kind='RECOVERY'
    AND scan_scope_key=v_scan_scope_key
  FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok',true,'claimed',false,'result_code','LANE_SCAN_BUSY');
  END IF;

  -- Small indexed lease-recovery page.  No financial source relation is read.
  FOR v_recovery IN
    SELECT attempt.id attempt_id,attempt.job_id,attempt.build_id,attempt.candidate_id,
           attempt.lease_expires_at_utc,
           job.attempt_count,job.max_attempts,
           CASE WHEN COALESCE(job.payload_json->>'recovery_scan_generation','') ~ '^\d+$'
             THEN LEAST((job.payload_json->>'recovery_scan_generation')::bigint,2147483647)
             ELSE 0 END AS scan_generation
    FROM private.banking_pay_workbench_stage_attempts attempt
    JOIN public.banking_pay_workbench_jobs job ON job.id=attempt.job_id
    WHERE attempt.attempt_status='STARTED'
      AND clock_timestamp()>=attempt.lease_expires_at_utc+interval '15 seconds'
      AND job.status='RUNNING' AND job.economic_build_id=attempt.build_id
      AND (p_session_id IS NULL OR job.session_id=p_session_id)
      AND (p_candidate_id IS NULL OR job.candidate_id=p_candidate_id)
      AND (COALESCE(job.payload_json->>'recovery_scan_deferred_epoch','') !~ '^\d+(\.\d+)?$'
        OR (job.payload_json->>'recovery_scan_deferred_epoch')::numeric
          <=extract(epoch FROM clock_timestamp()))
      AND (v_recovery_cursor_object_id IS NULL OR
        ROW(
          CASE WHEN COALESCE(job.payload_json->>'recovery_scan_generation','') ~ '^\d+$'
            THEN LEAST((job.payload_json->>'recovery_scan_generation')::bigint,2147483647)
            ELSE 0 END,
          attempt.lease_expires_at_utc,attempt.id
        ) > ROW(v_recovery_cursor_generation,
          v_recovery_cursor_due_at,v_recovery_cursor_object_id))
    ORDER BY scan_generation,attempt.lease_expires_at_utc,attempt.id LIMIT v_scan_limit
  LOOP
    v_recovery_examined:=v_recovery_examined+1;
    UPDATE private.banking_pay_workbench_queue_scan_state
    SET cursor_generation=v_recovery.scan_generation,cursor_chain_rank=NULL,
      cursor_priority=NULL,cursor_due_at=v_recovery.lease_expires_at_utc,
      cursor_created_at=NULL,cursor_object_id=v_recovery.attempt_id,
      updated_at_utc=clock_timestamp()
    WHERE lane_identity=v_lane_identity AND scan_kind='RECOVERY'
      AND scan_scope_key=v_scan_scope_key;
    IF pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(v_recovery.candidate_id),24062027)) THEN
      PERFORM 1 FROM private.banking_pay_workbench_candidate_scope_registry
      WHERE candidate_id=v_recovery.candidate_id FOR UPDATE SKIP LOCKED;
      IF NOT FOUND THEN CONTINUE; END IF;
      PERFORM 1 FROM private.banking_pay_workbench_economic_builds
      WHERE id=v_recovery.build_id FOR UPDATE SKIP LOCKED;
      IF NOT FOUND THEN CONTINUE; END IF;
      PERFORM 1 FROM public.banking_pay_workbench_jobs
      WHERE id=v_recovery.job_id FOR UPDATE SKIP LOCKED;
      IF NOT FOUND THEN CONTINUE; END IF;
      PERFORM 1 FROM private.banking_pay_workbench_stage_attempts
      WHERE id=v_recovery.attempt_id FOR UPDATE SKIP LOCKED;
      IF NOT FOUND THEN CONTINUE; END IF;
      UPDATE private.banking_pay_workbench_stage_attempts SET attempt_status='EXPIRED',
        expired_at_utc=clock_timestamp(),result_code='LEASE_EXPIRED_AFTER_CANCELLATION_GRACE',
        error_class='DELIVERED_ATTEMPT_EXPIRED',updated_at_utc=clock_timestamp()
      WHERE id=v_recovery.attempt_id AND attempt_status='STARTED'
        AND clock_timestamp()>=lease_expires_at_utc+interval '15 seconds';
      IF FOUND THEN
        v_recovered_count:=v_recovered_count+1;
        IF v_recovery.attempt_count<v_recovery.max_attempts THEN
          UPDATE public.banking_pay_workbench_jobs SET status='QUEUED',started_at_utc=NULL,
            run_at_utc=clock_timestamp()+make_interval(secs=>LEAST(300,
              GREATEST(1,power(2,LEAST(v_recovery.attempt_count,8))::integer))),
            payload_json=COALESCE(payload_json,'{}'::jsonb)
              -'recovery_scan_deferred_epoch'-'recovery_scan_deferral_count'-'recovery_scan_generation',
            last_error_json=jsonb_build_object('code','DELIVERED_ATTEMPT_EXPIRED'),
            updated_at_utc=clock_timestamp() WHERE id=v_recovery.job_id AND status='RUNNING';
        ELSE
          UPDATE public.banking_pay_workbench_jobs SET status='FAILED',failed_at_utc=clock_timestamp(),
            payload_json=COALESCE(payload_json,'{}'::jsonb)
              -'recovery_scan_deferred_epoch'-'recovery_scan_deferral_count'-'recovery_scan_generation',
            last_error_json=jsonb_build_object('code','DELIVERED_ATTEMPT_EXHAUSTED'),
            updated_at_utc=clock_timestamp() WHERE id=v_recovery.job_id AND status='RUNNING';
          UPDATE private.banking_pay_workbench_economic_builds SET status='FAILED',
            failed_at_utc=clock_timestamp(),failure_json=jsonb_build_object(
              'code','DELIVERED_ATTEMPT_EXHAUSTED'),updated_at_utc=clock_timestamp()
          WHERE id=v_recovery.build_id AND status NOT IN ('COMPLETE','OBSOLETE');
        END IF;
        IF v_recovered_count>=5 THEN EXIT; END IF;
      END IF;
    ELSE
      -- The durable lane cursor already advanced past this attempt. Never wait
      -- behind the transaction that owns the candidate merely to annotate its
      -- public job; another bounded scan can revisit it after cursor wrap.
      CONTINUE;
    END IF;
  END LOOP;

  IF v_recovery_examined=0 AND v_recovery_cursor_object_id IS NOT NULL THEN
    UPDATE private.banking_pay_workbench_queue_scan_state
    SET cursor_generation=NULL,cursor_chain_rank=NULL,cursor_priority=NULL,
      cursor_due_at=NULL,cursor_created_at=NULL,cursor_object_id=NULL,
      sweep_generation=sweep_generation+1,updated_at_utc=clock_timestamp()
    WHERE lane_identity=v_lane_identity AND scan_kind='RECOVERY'
      AND scan_scope_key=v_scan_scope_key;
  END IF;

  INSERT INTO private.banking_pay_workbench_queue_scan_state(
    lane_identity,scan_kind,scan_scope_key)
  VALUES(v_lane_identity,'CLAIM',v_scan_scope_key)
  ON CONFLICT(lane_identity,scan_kind,scan_scope_key) DO NOTHING;
  SELECT cursor_generation,cursor_chain_rank,cursor_priority,cursor_due_at,
    cursor_created_at,cursor_object_id
  INTO v_claim_cursor_generation,v_claim_cursor_chain_rank,v_claim_cursor_priority,
    v_claim_cursor_due_at,v_claim_cursor_created_at,v_claim_cursor_object_id
  FROM private.banking_pay_workbench_queue_scan_state
  WHERE lane_identity=v_lane_identity AND scan_kind='CLAIM'
    AND scan_scope_key=v_scan_scope_key
  FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok',true,'claimed',false,'result_code','LANE_SCAN_BUSY');
  END IF;

  -- Source-only form of the installed claim ordering and candidate-serial
  -- predicates.  The advisory lock is attempted only after the bounded final
  -- eligibility set, then the exact queue row is locked and rechecked.  RPC 1
  -- does not transition the job to RUNNING until its concrete build exists.
  FOR v_claim IN
    WITH claim_source AS MATERIALIZED (
      SELECT job.id,job.priority,job.run_at_utc,job.created_at_utc,job.payload_json,
        job.private_stage,
        CASE WHEN COALESCE(job.payload_json->>'claim_scan_generation','') ~ '^\d+$'
          THEN LEAST((job.payload_json->>'claim_scan_generation')::bigint,2147483647)
          ELSE 0 END AS scan_generation,
        public._pay_workbench_candidate_serial_candidate_id(job.candidate_id,job.payload_json) AS serial_candidate_id,
        public._pay_workbench_candidate_serial_key(
          public._pay_workbench_candidate_serial_candidate_id(job.candidate_id,job.payload_json)) AS serial_key,
        CASE WHEN (lower(btrim(COALESCE(job.payload_json->>'continuation','false'))) IN ('true','t','1','yes','y','on')
          OR upper(btrim(COALESCE(job.payload_json->>'run_mode',''))) IN
            ('BOUNDED_CONTINUATION','CONTINUATION','STAGE_CONTINUATION')
          OR NULLIF(btrim(COALESCE(job.payload_json->>'source_job_id',
            job.payload_json->>'continuation_source_job_id',
            job.payload_json->>'bounded_continuation_source_job_id','')),'') IS NOT NULL)
          THEN 0 ELSE 1 END AS chain_rank
      FROM public.banking_pay_workbench_jobs job
      WHERE job.status='QUEUED'
        AND job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND job.run_at_utc<=v_now
        AND COALESCE(job.attempt_count,0)<COALESCE(job.max_attempts,8)
        AND job.candidate_id IS NOT NULL AND job.session_id IS NOT NULL
        AND (p_session_id IS NULL OR job.session_id=p_session_id)
        AND (p_candidate_id IS NULL OR job.candidate_id=p_candidate_id)
        AND NOT EXISTS(SELECT 1 FROM private.banking_pay_workbench_stage_attempts active_attempt
          WHERE active_attempt.job_id=job.id AND active_attempt.attempt_status='STARTED')
        AND ((job.economic_build_id IS NULL AND ((job.private_stage='BUILD_INITIALISE'
            AND job.private_cursor_kind='BUILD_INITIALISE') OR (job.private_stage IS NULL
            AND job.private_cursor_kind IS NULL AND job.private_stage_version IS NULL)))
          OR (job.economic_build_id IS NOT NULL AND job.private_stage IS NOT NULL
            AND job.private_stage<>'BUILD_INITIALISE' AND job.private_cursor_kind IS NOT NULL
            AND job.private_stage_version IS NOT NULL))
        AND (v_claim_cursor_object_id IS NULL OR ROW(
          CASE WHEN COALESCE(job.payload_json->>'claim_scan_generation','') ~ '^\d+$'
            THEN LEAST((job.payload_json->>'claim_scan_generation')::bigint,2147483647)
            ELSE 0 END,
          CASE WHEN (lower(btrim(COALESCE(job.payload_json->>'continuation','false'))) IN ('true','t','1','yes','y','on')
            OR upper(btrim(COALESCE(job.payload_json->>'run_mode',''))) IN
              ('BOUNDED_CONTINUATION','CONTINUATION','STAGE_CONTINUATION')
            OR NULLIF(btrim(COALESCE(job.payload_json->>'source_job_id',
              job.payload_json->>'continuation_source_job_id',
              job.payload_json->>'bounded_continuation_source_job_id','')),'') IS NOT NULL)
            THEN 0 ELSE 1 END,
          job.priority,job.run_at_utc,job.created_at_utc,job.id
        ) > ROW(v_claim_cursor_generation,v_claim_cursor_chain_rank,
          v_claim_cursor_priority,v_claim_cursor_due_at,
          v_claim_cursor_created_at,v_claim_cursor_object_id))
      ORDER BY scan_generation,chain_rank,job.priority,job.run_at_utc,job.created_at_utc,job.id
      LIMIT v_scan_limit
    ), ranked AS MATERIALIZED (
      SELECT claim_source.*,row_number() OVER (
        PARTITION BY COALESCE(claim_source.serial_key,claim_source.id::text)
        ORDER BY claim_source.scan_generation,
          claim_source.chain_rank,
          claim_source.priority,claim_source.run_at_utc,claim_source.created_at_utc,claim_source.id) AS serial_rank
      FROM claim_source
    )
    SELECT ranked.*,
      lower(btrim(COALESCE(serial_state.state_json->>'blocked','false')))
        IN ('true','t','1','yes','y','on') AS serial_blocked
    FROM ranked
    CROSS JOIN LATERAL (SELECT public._pay_workbench_candidate_serial_active_state(
      ranked.id,ranked.serial_candidate_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      ranked.payload_json,v_now) AS state_json) serial_state
    WHERE ranked.serial_rank=1
    ORDER BY ranked.scan_generation,ranked.chain_rank,
      ranked.priority,ranked.run_at_utc,ranked.created_at_utc,ranked.id
  LOOP
    v_claim_examined:=v_claim_examined+1;
    UPDATE private.banking_pay_workbench_queue_scan_state
    SET cursor_generation=v_claim.scan_generation,cursor_chain_rank=v_claim.chain_rank,
      cursor_priority=v_claim.priority,cursor_due_at=v_claim.run_at_utc,
      cursor_created_at=v_claim.created_at_utc,cursor_object_id=v_claim.id,
      updated_at_utc=clock_timestamp()
    WHERE lane_identity=v_lane_identity AND scan_kind='CLAIM'
      AND scan_scope_key=v_scan_scope_key;
    IF v_claim.serial_blocked THEN
      -- Cursor progress supplies fairness. Mutating a serial-blocked queue row
      -- here can wait behind its owner and turn a safe no-claim into an
      -- uncertain HTTP result.
      CONTINUE;
    END IF;
    IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
      COALESCE(v_claim.serial_key,v_claim.id::text),24062027)) THEN
      -- Losing lanes skip immediately. They must not update the row protected
      -- by the winning lane, because that update introduces an avoidable wait
      -- into the one-second claim transaction.
      CONTINUE;
    END IF;
    IF v_claim.private_stage='RECONCILE_EXECUTE' THEN
      -- Serialise the admission check itself.  The lock is transaction-scoped:
      -- once the first claim commits, the next lane observes its durable
      -- STARTED attempt before deciding whether another reconciliation fits.
      IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
        'BANKING_PAY_WORKBENCH:RECONCILE_EXECUTE:ADMISSION',24062027)) THEN
        CONTINUE;
      END IF;
      IF (
        SELECT count(*)
        FROM private.banking_pay_workbench_stage_attempts AS active_reconcile
        JOIN public.banking_pay_workbench_jobs AS active_reconcile_job
          ON active_reconcile_job.id=active_reconcile.job_id
        WHERE active_reconcile.attempt_status='STARTED'
          AND active_reconcile.private_stage='RECONCILE_EXECUTE'
          AND active_reconcile_job.status='RUNNING'
          AND clock_timestamp()
              < active_reconcile.lease_expires_at_utc+interval '15 seconds'
      )>=v_reconcile_attempt_limit THEN
        CONTINUE;
      END IF;
    END IF;
    v_job:=NULL;
    SELECT claimed_job.* INTO v_job
    FROM public.banking_pay_workbench_jobs claimed_job
    WHERE claimed_job.id=v_claim.id AND claimed_job.status='QUEUED'
      AND claimed_job.run_at_utc<=v_now
      AND COALESCE(claimed_job.attempt_count,0)<COALESCE(claimed_job.max_attempts,8)
      AND NOT EXISTS(SELECT 1 FROM private.banking_pay_workbench_stage_attempts active_attempt
        WHERE active_attempt.job_id=claimed_job.id AND active_attempt.attempt_status='STARTED')
    FOR UPDATE OF claimed_job SKIP LOCKED;
    EXIT WHEN v_job.id IS NOT NULL;
  END LOOP;

  IF v_claim_examined=0 AND v_claim_cursor_object_id IS NOT NULL THEN
    UPDATE private.banking_pay_workbench_queue_scan_state
    SET cursor_generation=NULL,cursor_chain_rank=NULL,cursor_priority=NULL,
      cursor_due_at=NULL,cursor_created_at=NULL,cursor_object_id=NULL,
      sweep_generation=sweep_generation+1,updated_at_utc=clock_timestamp()
    WHERE lane_identity=v_lane_identity AND scan_kind='CLAIM'
      AND scan_scope_key=v_scan_scope_key;
  END IF;

  IF v_job.id IS NULL THEN
    RETURN jsonb_build_object('ok',true,'claimed',false);
  END IF;

  PERFORM 1 FROM public.candidates AS candidate_row
  WHERE candidate_row.id=v_job.candidate_id FOR UPDATE;
  IF NOT FOUND THEN
    UPDATE public.banking_pay_workbench_jobs
    SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp(),
        economic_build_id=NULL,private_stage=NULL,private_cursor_kind=NULL,
        private_cursor_json='{}'::jsonb,private_stage_version=NULL,
        last_error_json=jsonb_build_object('code','CANDIDATE_DELETED_BEFORE_ATTEMPT')
    WHERE id=v_job.id AND status='QUEUED';
    RETURN jsonb_build_object('ok',true,'claimed',false,'result_code','CANDIDATE_DELETED');
  END IF;

  INSERT INTO private.banking_pay_workbench_candidate_scope_registry(
    candidate_id,last_dirtied_at_utc,created_at_utc,updated_at_utc
  )
  VALUES(v_job.candidate_id,v_database_now,v_database_now,v_database_now)
  ON CONFLICT(candidate_id) DO NOTHING;

  SELECT * INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  WHERE registry.candidate_id=v_job.candidate_id
  FOR UPDATE;

  SELECT * INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id=v_job.session_id
  FOR SHARE;
  IF NOT FOUND OR v_session.status NOT IN ('OPEN','READY','REFRESHING') THEN
    IF v_job.economic_build_id IS NOT NULL THEN
      UPDATE private.banking_pay_workbench_economic_builds
      SET status='OBSOLETE',obsolete_at_utc=clock_timestamp(),
          failure_json=jsonb_build_object('code','SESSION_NO_LONGER_CURRENT'),
          updated_at_utc=clock_timestamp()
      WHERE id=v_job.economic_build_id AND status NOT IN ('COMPLETE','FAILED','OBSOLETE');
      UPDATE private.banking_pay_workbench_candidate_scope_registry
      SET current_build_id=NULL,updated_at_utc=clock_timestamp()
      WHERE candidate_id=v_job.candidate_id AND current_build_id=v_job.economic_build_id;
    END IF;
    UPDATE public.banking_pay_workbench_jobs
    SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp(),
        economic_build_id=NULL,private_stage=NULL,private_cursor_kind=NULL,
        private_cursor_json='{}'::jsonb,private_stage_version=NULL,
        last_error_json=jsonb_build_object('code','SESSION_NO_LONGER_CURRENT')
    WHERE id=v_job.id AND status='QUEUED';
    RETURN jsonb_build_object('ok',true,'claimed',false,'result_code','SESSION_OBSOLETE');
  END IF;

  v_source_change_seq := GREATEST(
    v_registry.current_source_change_seq,
    COALESCE(CASE WHEN COALESCE(v_job.payload_json->>'source_change_seq','') ~ '^\d+$'
      THEN (v_job.payload_json->>'source_change_seq')::bigint END,0)
  );
  v_captured_generation := COALESCE(v_job.scope_change_generation,v_registry.dirty_generation,0);

  IF v_job.economic_build_id IS NULL THEN
    IF COALESCE(v_job.payload_json->>'source_build_run_id','')
       !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED'
        USING ERRCODE='22023', DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED','job_id',v_job.id
        )::text;
    END IF;
    v_source_build_run_id := (v_job.payload_json->>'source_build_run_id')::uuid;
    v_is_bootstrap := COALESCE(v_job.payload_json->>'bootstrap_id','')
      ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND v_registry.initialisation_status<>'READY';
    IF v_is_bootstrap THEN
      v_bootstrap_id := (v_job.payload_json->>'bootstrap_id')::uuid;
    END IF;
    v_build_id := gen_random_uuid();
    INSERT INTO private.banking_pay_workbench_economic_builds(
      id,candidate_id,session_id,session_version,source_snapshot_run_id,
      source_build_run_id,source_job_id,captured_candidate_generation,
      source_change_seq,status,private_stage,scope_cursor_json
    ) VALUES (
      v_build_id,v_job.candidate_id,v_job.session_id,v_session.version,v_session.source_snapshot_run_id,
      v_source_build_run_id,v_job.id,v_captured_generation,v_source_change_seq,
      'COLLECTING',CASE WHEN v_is_bootstrap THEN 'BOOTSTRAP_DISCOVERY' ELSE 'PREPARE_SCOPE' END,
      CASE WHEN v_is_bootstrap THEN jsonb_build_object(
        'cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
        'bootstrap_id',v_bootstrap_id,'bootstrap_stream','TIMESHEETS_TSFIN',
        'last_source_key',NULL,'build_id',v_build_id,
        'candidate_id',v_job.candidate_id,'captured_candidate_generation',v_captured_generation,
        'captured_source_change_seq',v_source_change_seq,'processed_source_rows',0
      ) ELSE jsonb_build_object(
        'cursor_kind','SCOPE_SELECT','cursor_version',1,
        'seed_family','ACTIVE_STATE','last_source_key',NULL,'build_id',v_build_id,
        'candidate_id',v_job.candidate_id,'captured_candidate_generation',v_captured_generation,
        'captured_source_change_seq',v_source_change_seq,'processed_source_rows',0
      ) END
    ) RETURNING id,scope_cursor_json INTO v_build_id,v_cursor_json;
    UPDATE private.banking_pay_workbench_candidate_scope_registry
    SET current_build_id=v_build_id,
        initialisation_status=CASE WHEN v_is_bootstrap THEN 'DISCOVERING' ELSE initialisation_status END,
        bootstrap_id=CASE WHEN v_is_bootstrap THEN v_bootstrap_id ELSE bootstrap_id END,
        bootstrap_stream=CASE WHEN v_is_bootstrap THEN 'TIMESHEETS_TSFIN' ELSE bootstrap_stream END,
        bootstrap_cursor_json=CASE WHEN v_is_bootstrap THEN v_cursor_json ELSE bootstrap_cursor_json END,
        bootstrap_rows_seen=CASE WHEN v_is_bootstrap THEN 0 ELSE bootstrap_rows_seen END,
        bootstrap_timesheets_registered=CASE WHEN v_is_bootstrap THEN 0 ELSE bootstrap_timesheets_registered END,
        bootstrap_captured_generation=CASE WHEN v_is_bootstrap THEN v_captured_generation ELSE bootstrap_captured_generation END,
        bootstrap_captured_source_change_seq=CASE WHEN v_is_bootstrap THEN v_source_change_seq ELSE bootstrap_captured_source_change_seq END,
        updated_at_utc=clock_timestamp()
    WHERE candidate_id=v_job.candidate_id;
    v_stage := CASE WHEN v_is_bootstrap THEN 'BOOTSTRAP_DISCOVERY' ELSE 'PREPARE_SCOPE' END;
    v_cursor_kind := CASE WHEN v_is_bootstrap THEN 'BOOTSTRAP_DISCOVERY' ELSE 'SCOPE_SELECT' END;
  ELSE
    v_build_id := v_job.economic_build_id;
    v_stage := v_job.private_stage;
    v_cursor_kind := v_job.private_cursor_kind;
    v_cursor_json := v_job.private_cursor_json;
    SELECT build_row.* INTO v_build
    FROM private.banking_pay_workbench_economic_builds AS build_row
    WHERE build_row.id=v_build_id AND build_row.candidate_id=v_job.candidate_id
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_BUILD_MISMATCH'
        USING ERRCODE='40001',DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_ATTEMPT_BUILD_MISMATCH','job_id',v_job.id,'build_id',v_build_id
        )::text;
    END IF;
    IF v_registry.current_build_id IS DISTINCT FROM v_build_id
       OR v_build.captured_candidate_generation IS DISTINCT FROM v_registry.dirty_generation
       OR v_build.source_change_seq IS DISTINCT FROM v_registry.current_source_change_seq THEN
      UPDATE private.banking_pay_workbench_economic_builds
      SET status='OBSOLETE',obsolete_at_utc=clock_timestamp(),
          failure_json=jsonb_build_object('code','ATTEMPT_GENERATION_OBSOLETE'),
          updated_at_utc=clock_timestamp()
      WHERE id=v_build_id AND status NOT IN ('COMPLETE','FAILED','OBSOLETE');
      UPDATE private.banking_pay_workbench_candidate_scope_registry
      SET current_build_id=NULL,updated_at_utc=clock_timestamp()
      WHERE candidate_id=v_job.candidate_id AND current_build_id=v_build_id;
      UPDATE public.banking_pay_workbench_jobs
      SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),
          economic_build_id=NULL,private_stage=NULL,private_cursor_kind=NULL,
          private_cursor_json='{}'::jsonb,private_stage_version=NULL,
          last_error_json=jsonb_build_object('code','ATTEMPT_GENERATION_OBSOLETE'),
          updated_at_utc=clock_timestamp()
      WHERE id=v_job.id AND status='QUEUED';
      RETURN jsonb_build_object('ok',true,'claimed',false,
        'result_code','ATTEMPT_GENERATION_OBSOLETE');
    END IF;
    v_captured_generation:=v_build.captured_candidate_generation;
    v_source_change_seq:=v_build.source_change_seq;
    UPDATE private.banking_pay_workbench_economic_builds
    SET source_job_id=v_job.id,updated_at_utc=clock_timestamp()
    WHERE id=v_build_id;
  END IF;

  v_attempt_number := COALESCE(v_job.attempt_count,0)+1;
  v_claimed_job_id:=v_job.id;
  UPDATE public.banking_pay_workbench_jobs claimed_job
  SET status='RUNNING',attempt_count=v_attempt_number,
      started_at_utc=COALESCE(claimed_job.started_at_utc,clock_timestamp()),
      updated_at_utc=clock_timestamp(),completed_at_utc=NULL,failed_at_utc=NULL,
      last_error_json=NULL,economic_build_id=v_build_id,private_stage=v_stage,
      private_cursor_kind=v_cursor_kind,private_cursor_json=v_cursor_json,
      private_stage_version=1,
      payload_json=jsonb_strip_nulls((COALESCE(claimed_job.payload_json,'{}'::jsonb)
          -'claim_scan_deferred_reason'-'claim_scan_deferral_count'-'claim_scan_generation'
          -'recovery_scan_deferred_epoch'-'recovery_scan_deferral_count'-'recovery_scan_generation')
        ||jsonb_build_object(
          'claimed_at_utc',clock_timestamp()::text,
          'candidate_serial_key',public._pay_workbench_candidate_serial_key(v_job.candidate_id),
          'candidate_serial_candidate_id',v_job.candidate_id::text,
          'candidate_serial_active_chain_id',COALESCE(
            NULLIF(btrim(COALESCE(claimed_job.payload_json->>'candidate_serial_active_chain_id','')),''),
            NULLIF(btrim(COALESCE(claimed_job.payload_json->>'source_job_id','')),''),
            claimed_job.id::text),
          'candidate_serial_source_job_id',NULLIF(btrim(COALESCE(
            claimed_job.payload_json->>'source_job_id','')),''),
          'candidate_serial_started_at_utc',clock_timestamp()::text,
          'candidate_serial_reason','CANDIDATE_SERIAL_CLAIM_GRANTED'))
  WHERE claimed_job.id=v_claimed_job_id AND claimed_job.status='QUEUED'
    AND claimed_job.run_at_utc<=v_now
    AND COALESCE(claimed_job.attempt_count,0)=v_attempt_number-1
    AND COALESCE(claimed_job.attempt_count,0)<COALESCE(claimed_job.max_attempts,8)
    AND NOT EXISTS (
      SELECT 1 FROM private.banking_pay_workbench_stage_attempts active_attempt
      WHERE active_attempt.job_id=claimed_job.id AND active_attempt.attempt_status='STARTED'
    )
  RETURNING claimed_job.* INTO v_job;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_CLAIM_ADOPTION_FAILED'
      USING ERRCODE='40001',DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_ATTEMPT_CLAIM_ADOPTION_FAILED','job_id',v_claimed_job_id
      )::text;
  END IF;

  v_attempt_started_at := clock_timestamp();
  v_lease_expires := v_attempt_started_at+make_interval(secs=>v_effective_lease);
  INSERT INTO private.banking_pay_workbench_stage_attempts(
    job_id,build_id,candidate_id,private_stage,attempt_number,worker_id,lane_identity,
    captured_candidate_generation,captured_source_change_seq,execution_profile_version,
    started_at_utc,lease_expires_at_utc,created_at_utc,updated_at_utc
  ) VALUES (
    v_job.id,v_build_id,v_job.candidate_id,v_stage,v_attempt_number,v_worker_id,v_lane_identity,
    v_captured_generation,v_source_change_seq,1,v_attempt_started_at,v_lease_expires,
    v_attempt_started_at,v_attempt_started_at
  ) RETURNING id,attempt_nonce INTO v_attempt_id,v_attempt_nonce;

  RETURN jsonb_build_object(
    'ok',true,'claimed',true,'job_id',v_job.id,'build_id',v_build_id,
    'candidate_id',v_job.candidate_id,'private_stage',v_stage,
    'attempt_id',v_attempt_id,'attempt_number',v_attempt_number,
    'attempt_nonce',v_attempt_nonce,'attempt_started_at_utc',v_attempt_started_at,
    'lease_expires_at_utc',v_lease_expires,'execution_profile_version',1
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid) TO service_role;
