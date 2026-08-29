-- Banking Pay bounded-scope V1.2.10: durable transaction-one claim/start RPC.
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
  v_execution_profile_version integer:=1;
  v_reconciliation_optimization_version integer:=0;
  v_settings_json jsonb:='{}'::jsonb;
  v_obsolete_repair_result jsonb:='{}'::jsonb;
  v_obsolete_successor_job_id uuid:=NULL::uuid;
  v_obsolete_active_successor_proven boolean:=false;
  v_obsolete_terminal_current_proven boolean:=false;
  v_obsolete_successor_wait_count integer:=0;
  v_obsolete_successor_wait_seconds integer:=2;
  v_obsolete_successor_retry_at_utc timestamptz:=NULL::timestamptz;
  v_draft_deferral_enabled boolean:=false;
  v_draft_operation_id uuid:=NULL::uuid;
  v_draft_operation public.banking_pay_operations%ROWTYPE;
  v_draft_deferral_count integer:=0;
  v_draft_deferral_seconds integer:=2;
  v_same_authority_election_enabled boolean:=false;
  v_authority_fingerprint_version smallint:=NULL::smallint;
  v_authority_fingerprint text:=NULL::text;
  v_required_physical_publication_contract_version smallint:=0;
  v_unique_constraint_name text:=NULL::text;
  v_winner_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_winner_active_job_id uuid:=NULL::uuid;
  v_winner_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_winner_complete_current_proven boolean:=false;
  v_terminal_repair_result jsonb:='{}'::jsonb;
  v_terminal_repair_row jsonb:='{}'::jsonb;
  v_terminal_action text:=NULL::text;
  v_terminal_successor_job_id uuid:=NULL::uuid;
  v_terminal_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_terminal_candidate_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_terminal_candidate_state_present boolean:=false;
  v_terminal_owner_valid boolean:=false;
  v_terminal_progress_json jsonb:='{}'::jsonb;
  v_terminal_progress_recompute_result jsonb:='{}'::jsonb;
  v_terminal_existing_successor_proven boolean:=false;
  v_terminal_live_change_seq bigint:=0;
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

  SELECT COALESCE(settings_row.banking_pay_workbench_db_worker_lease_seconds,25),
         to_jsonb(settings_row)
  INTO v_configured_lease,v_settings_json
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  v_same_authority_election_enabled:=COALESCE(
    (v_settings_json->>'banking_pay_same_authority_build_election_v1_enabled')::boolean,
    false
  );
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
           job.session_id,
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

          /* A prior bounded repair may already have rebound the scope to a
             valid successor while the exhausted delivered attempt remains
             STARTED.  In that state the orphan repair correctly has no row to
             change.  Requiring repaired_count=1 makes this recovery transaction
             roll back forever before the already-owned successor can be
             claimed.  Prove that exact already-rebound state directly; only
             call the repair owner when no such successor exists. */
          v_terminal_existing_successor_proven:=false;
          v_terminal_successor_job_id:=NULL::uuid;
          SELECT COALESCE(change_counter.seq,0)
          INTO v_terminal_live_change_seq
          FROM public.app_change_counters AS change_counter
          WHERE change_counter.entity_key='pay_candidate:'||v_recovery.candidate_id::text;
          v_terminal_live_change_seq:=COALESCE(v_terminal_live_change_seq,0);

          SELECT scope_row.* INTO v_terminal_scope
          FROM public.banking_pay_workbench_session_scope AS scope_row
          WHERE scope_row.session_id=v_recovery.session_id
            AND scope_row.candidate_id=v_recovery.candidate_id
            AND scope_row.pending_job_id IS NOT NULL
            AND scope_row.pending_job_id IS DISTINCT FROM v_recovery.job_id
            AND UPPER(BTRIM(COALESCE(scope_row.status,'')))='SOURCE_BUILD_PENDING'
            AND COALESCE(scope_row.dirty,false)=true
            AND scope_row.error_json IS NULL
          FOR UPDATE;

          IF FOUND THEN
            SELECT successor.id INTO v_terminal_successor_job_id
            FROM public.banking_pay_workbench_jobs AS successor
            JOIN public.banking_pay_workbench_sessions AS successor_session
              ON successor_session.id=successor.session_id
            WHERE successor.id=v_terminal_scope.pending_job_id
              AND successor.session_id=v_terminal_scope.session_id
              AND successor.candidate_id=v_terminal_scope.candidate_id
              AND UPPER(BTRIM(COALESCE(successor.status,''))) IN ('QUEUED','RUNNING')
              AND UPPER(BTRIM(COALESCE(successor.job_type,''))) IN (
                'WORKBENCH_CANDIDATE_SOURCE_BUILD',
                'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
                'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
                'CANDIDATE_SOURCE_BUILD',
                'CANDIDATE_SOURCE_BUILD_CHUNK',
                'SOURCE_BUILD',
                'SOURCE_BUILD_PAGE')
              AND CASE
                WHEN COALESCE(successor.payload_json->>'session_version','') ~ '^[0-9]{1,18}$'
                  THEN (successor.payload_json->>'session_version')::bigint
                ELSE NULL::bigint END=successor_session.version
              AND CASE
                WHEN COALESCE(successor.payload_json->>'source_change_seq','') ~ '^[0-9]{1,18}$'
                  THEN (successor.payload_json->>'source_change_seq')::bigint
                ELSE NULL::bigint END>=v_terminal_live_change_seq
              AND COALESCE(successor.payload_json->>'source_build_run_id','')
                ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            FOR UPDATE OF successor,successor_session;
            v_terminal_existing_successor_proven:=FOUND;
          END IF;

          IF v_terminal_existing_successor_proven THEN
            v_terminal_action:='REBOUND_ACTIVE_SUCCESSOR';
            v_terminal_progress_recompute_result:=
              public.pay_workbench_session_recompute_progress_counters(
                p_session_id=>v_recovery.session_id,
                p_apply=>true,
                p_reason=>'EXHAUSTED_ATTEMPT_EXISTING_SUCCESSOR_PROVEN',
                p_write_progress_json=>true);
            IF jsonb_typeof(v_terminal_progress_recompute_result)<>'object'
               OR COALESCE((v_terminal_progress_recompute_result->>'ok')::boolean,false)
                  IS NOT TRUE THEN
              RAISE EXCEPTION 'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN'
                USING ERRCODE='40001',DETAIL='EXISTING_SUCCESSOR_PROGRESS_RECOMPUTE';
            END IF;
            v_terminal_repair_row:=jsonb_build_object(
              'session_id',v_recovery.session_id::text,
              'candidate_id',v_recovery.candidate_id::text,
              'old_pending_job_id',v_recovery.job_id::text,
              'action',v_terminal_action,
              'successor_job_id',v_terminal_successor_job_id::text,
              'state_transition_proven',true,
              'progress_recomputed',true,
              'already_rebound_successor_proven',true);
            v_terminal_repair_result:=jsonb_build_object(
              'ok',true,
              'already_rebound_successor_proven',true,
              'results',jsonb_build_array(v_terminal_repair_row));
          ELSE
            v_terminal_repair_result:=public.pay_workbench_repair_orphaned_pending_source_build(
              p_session_id=>v_recovery.session_id,
              p_candidate_id=>v_recovery.candidate_id,
              p_limit=>1,
              p_now_utc=>v_database_now,
              p_reason=>'EXHAUSTED_DELIVERED_ATTEMPT_ATOMIC_CONVERGENCE');
            v_terminal_repair_row:=COALESCE(v_terminal_repair_result->'results'->0,'{}'::jsonb);
            v_terminal_action:=NULLIF(BTRIM(v_terminal_repair_row->>'action'),'');
            v_terminal_successor_job_id:=CASE
              WHEN COALESCE(v_terminal_repair_row->>'successor_job_id','')
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                THEN (v_terminal_repair_row->>'successor_job_id')::uuid END;

            IF COALESCE((v_terminal_repair_result->>'ok')::boolean,false) IS NOT TRUE
               OR COALESCE((v_terminal_repair_result->>'examined_count')::integer,-1)<>1
               OR COALESCE((v_terminal_repair_result->>'repaired_count')::integer,-1)<>1
               OR COALESCE((v_terminal_repair_result->>'skipped_count')::integer,-1)<>0
               OR COALESCE((v_terminal_repair_result->>'unresolved_count')::integer,-1)<>0
               OR COALESCE((v_terminal_repair_result->>'progress_recompute_failed_count')::integer,-1)<>0
               OR COALESCE((v_terminal_repair_result->>'all_state_transitions_proven')::boolean,false)
                  IS NOT TRUE
               OR COALESCE((v_terminal_repair_result->>'all_progress_recomputed')::boolean,false)
                  IS NOT TRUE
               OR jsonb_array_length(COALESCE(v_terminal_repair_result->'results','[]'::jsonb))<>1
               OR COALESCE(v_terminal_repair_row->>'candidate_id','')<>v_recovery.candidate_id::text
               OR COALESCE((v_terminal_repair_row->>'state_transition_proven')::boolean,false)
                  IS NOT TRUE
               OR COALESCE((v_terminal_repair_row->>'progress_recomputed')::boolean,false)
                  IS NOT TRUE
               OR v_terminal_action NOT IN ('FAILED_CLOSED_MAX_ATTEMPTS',
                 'REBOUND_ACTIVE_SUCCESSOR','RECONCILED_SUCCESSFUL_BUILD') THEN
              RAISE EXCEPTION 'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN'
                USING ERRCODE='40001',DETAIL=jsonb_build_object(
                  'candidate_id',v_recovery.candidate_id,
                  'job_id',v_recovery.job_id,
                  'repair_result',v_terminal_repair_result)::text;
            END IF;
          END IF;

          SELECT scope_row.* INTO v_terminal_scope
          FROM public.banking_pay_workbench_session_scope scope_row
          WHERE scope_row.session_id=(v_terminal_repair_row->>'session_id')::uuid
            AND scope_row.candidate_id=v_recovery.candidate_id
          FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN'
              USING ERRCODE='40001',DETAIL='TERMINAL_SCOPE_MISSING';
          END IF;

          SELECT state_row.* INTO v_terminal_candidate_state
          FROM public.banking_pay_workbench_session_candidate_state state_row
          WHERE state_row.session_id=v_terminal_scope.session_id
            AND state_row.candidate_id=v_terminal_scope.candidate_id
          FOR UPDATE;
          v_terminal_candidate_state_present:=FOUND;

          SELECT COALESCE(session_row.progress_json,'{}'::jsonb)
          INTO v_terminal_progress_json
          FROM public.banking_pay_workbench_sessions session_row
          WHERE session_row.id=v_terminal_scope.session_id
          FOR UPDATE;

          IF v_terminal_action='FAILED_CLOSED_MAX_ATTEMPTS' THEN
            IF UPPER(BTRIM(COALESCE(v_terminal_scope.status,'')))<>'SOURCE_BUILD_ERROR'
               OR v_terminal_scope.pending_job_id IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(v_terminal_scope.error_json->>'code','')),'') IS NULL
               OR (v_terminal_candidate_state_present AND (
                 UPPER(BTRIM(COALESCE(v_terminal_candidate_state.status,'')))<>'FAILED'
                 OR v_terminal_candidate_state.pending_job_id IS NOT NULL
                 OR v_terminal_scope.error_json->>'code' IS DISTINCT FROM
                      v_terminal_candidate_state.last_error_json->>'code')) THEN
              RAISE EXCEPTION 'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN'
                USING ERRCODE='40001',DETAIL='FAILED_CLOSED_POSTCONDITION';
            END IF;
          ELSIF v_terminal_action='REBOUND_ACTIVE_SUCCESSOR' THEN
            SELECT EXISTS(SELECT 1
              FROM public.banking_pay_workbench_jobs successor
              WHERE successor.id=v_terminal_successor_job_id
                AND successor.session_id=v_terminal_scope.session_id
                AND successor.candidate_id=v_terminal_scope.candidate_id
                AND UPPER(BTRIM(COALESCE(successor.status,''))) IN ('QUEUED','RUNNING'))
            INTO v_terminal_owner_valid;
            IF UPPER(BTRIM(COALESCE(v_terminal_scope.status,'')))<>'SOURCE_BUILD_PENDING'
               OR v_terminal_successor_job_id IS NULL
               OR v_terminal_scope.pending_job_id IS DISTINCT FROM v_terminal_successor_job_id
               OR v_terminal_owner_valid IS NOT TRUE
               OR (v_terminal_candidate_state_present AND (
                 UPPER(BTRIM(COALESCE(v_terminal_candidate_state.status,'')))<>'PENDING'
                 OR v_terminal_candidate_state.pending_job_id IS DISTINCT FROM
                      v_terminal_successor_job_id
                 OR COALESCE(v_terminal_candidate_state.last_error_json,'{}'::jsonb)<>'{}'::jsonb)) THEN
              RAISE EXCEPTION 'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN'
                USING ERRCODE='40001',DETAIL='REBOUND_POSTCONDITION';
            END IF;
          ELSE
            IF UPPER(BTRIM(COALESCE(v_terminal_scope.status,''))) NOT IN (
                 'SOURCE_READY','LINE_WORK_PENDING','MATERIALISED','MATERIALIZED','READY','SOURCE_EMPTY')
               OR COALESCE(v_terminal_scope.dirty,true)
               OR v_terminal_scope.pending_job_id IS NOT DISTINCT FROM v_recovery.job_id
               OR (v_terminal_candidate_state_present AND (
                 UPPER(BTRIM(COALESCE(v_terminal_candidate_state.status,'')))='FAILED'
                 OR v_terminal_candidate_state.pending_job_id IS NOT DISTINCT FROM v_recovery.job_id
                 OR v_terminal_candidate_state.session_version IS DISTINCT FROM
                      v_terminal_scope.session_version
                 OR v_terminal_candidate_state.source_change_seq IS DISTINCT FROM
                      v_terminal_scope.source_change_seq)) THEN
              RAISE EXCEPTION 'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN'
                USING ERRCODE='40001',DETAIL='RECONCILED_POSTCONDITION';
            END IF;
          END IF;

          IF COALESCE((v_terminal_progress_json->>'recovery_required_count')::integer,0)<>0
             OR COALESCE((v_terminal_progress_json#>>'{blocker_counts,pending_scope_without_active_job}')::integer,0)<>0 THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN'
              USING ERRCODE='40001',DETAIL='SESSION_PROGRESS_POSTCONDITION';
          END IF;
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
    -- Reaching the end of a non-empty durable claim sweep is progress, not an
    -- empty queue.  The next bounded RPC 1 call must start at the beginning of
    -- the new sweep.  Return an explicit result so the Worker can continue
    -- within its existing burst/runtime limits instead of waiting for cron or
    -- unrelated UI traffic.
    RETURN jsonb_build_object(
      'ok',true,
      'claimed',false,
      'result_code','CLAIM_SCAN_CURSOR_WRAPPED',
      'scan_progress',true,
      'claim_scan_wrapped',true,
      'claim_examined',0
    );
  END IF;

  IF v_job.id IS NULL THEN
    IF v_claim_examined>0 THEN
      -- The lane advanced past a candidate whose serial/advisory/job-row
      -- authority was temporarily unavailable.  Preserve that fairness
      -- progress and request another bounded pass; do not present it as an
      -- authoritative no-work result.
      RETURN jsonb_build_object(
        'ok',true,
        'claimed',false,
        'result_code','CLAIM_SCAN_PROGRESS',
        'scan_progress',true,
        'claim_scan_wrapped',false,
        'claim_examined',v_claim_examined
      );
    END IF;
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
  WHERE session_row.id=v_job.session_id;
  -- The session is validation input, not RPC-1 mutation authority. Taking a
  -- row lock here serialised otherwise independent candidate lanes behind a
  -- long-running RPC-2 attempt on the same session. The claimed build freezes
  -- the session version below and RPC 2/final publication revalidate it before
  -- any result can become authoritative.
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

  v_draft_deferral_enabled:=COALESCE(
    (v_settings_json->>'banking_pay_draft_self_invalidation_claim_deferral_v1_enabled')::boolean,
    false
  );
  IF v_draft_deferral_enabled
     AND v_job.economic_build_id IS NULL
     AND COALESCE(v_job.payload_json->>'draft_operation_id','')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     AND NULLIF(pg_catalog.btrim(COALESCE(v_job.payload_json->>'draft_context_token','')),'') IS NOT NULL
     AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_job.payload_json->>'draft_phase',''))) IN (
       'INSERT_ITEMS','APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS',
       'CREATE_TIMESHEET_SNAPSHOTS','BUILD_ITEM_BREAKDOWNS'
     ) THEN
    v_draft_operation_id:=(v_job.payload_json->>'draft_operation_id')::uuid;
    SELECT operation_row.* INTO v_draft_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id=v_draft_operation_id;

    IF FOUND
       AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_draft_operation.operation_type,'')))='DRAFT_CREATE'
       AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_draft_operation.status,''))) IN
         ('QUEUED','RUNNING','PROCESSING','CLAIMED','IN_PROGRESS')
       AND v_draft_operation.workbench_session_id=v_job.session_id
       AND pg_catalog.upper(pg_catalog.btrim(COALESCE(v_draft_operation.phase,''))) IN (
         'INSERT_ITEMS','APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS',
         'POPULATE_CANDIDATE_SUMMARIES','CREATE_TIMESHEET_SNAPSHOTS','BUILD_ITEM_BREAKDOWNS',
         'ASSERT_INTEGRITY'
       )
       AND EXISTS(
         SELECT 1
         FROM public.banking_pay_operation_candidate_scope AS operation_scope
         WHERE operation_scope.operation_id=v_draft_operation_id
           AND operation_scope.workbench_session_id=v_job.session_id
           AND operation_scope.candidate_id=v_job.candidate_id
           AND operation_scope.pay_batch_id=v_draft_operation.pay_batch_id
           AND operation_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
       )
       AND v_source_change_seq<=v_registry.current_source_change_seq
       AND v_captured_generation<=v_registry.dirty_generation THEN
      v_draft_deferral_count:=CASE
        WHEN COALESCE(v_job.payload_json->>'draft_self_invalidation_deferral_count','') ~ '^\d+$'
          THEN LEAST((v_job.payload_json->>'draft_self_invalidation_deferral_count')::integer,12)
        ELSE 0
      END;
      IF v_draft_deferral_count<12 THEN
        v_draft_deferral_seconds:=CASE
          WHEN v_draft_deferral_count=0 THEN 2
          WHEN v_draft_deferral_count=1 THEN 3
          ELSE 5
        END;
        UPDATE public.banking_pay_workbench_jobs AS deferred_job
        SET run_at_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>v_draft_deferral_seconds),
            payload_json=COALESCE(deferred_job.payload_json,'{}'::jsonb)
              || pg_catalog.jsonb_build_object(
                'draft_self_invalidation_deferral_count',v_draft_deferral_count+1,
                'draft_self_invalidation_deferred_at_utc',pg_catalog.clock_timestamp(),
                'draft_self_invalidation_result_code','DRAFT_CREATE_SELF_INVALIDATION_DEFERRED'
              ),
            updated_at_utc=pg_catalog.clock_timestamp()
        WHERE deferred_job.id=v_job.id AND deferred_job.status='QUEUED';
        RETURN pg_catalog.jsonb_build_object(
          'ok',true,'claimed',false,
          'result_code','DRAFT_CREATE_SELF_INVALIDATION_DEFERRED',
          'job_id',v_job.id,'candidate_id',v_job.candidate_id,
          'operation_id',v_draft_operation_id,
          'deferral_count',v_draft_deferral_count+1,
          'defer_seconds',v_draft_deferral_seconds
        );
      END IF;
    END IF;
  END IF;

  IF v_job.economic_build_id IS NULL THEN
    BEGIN
      v_execution_profile_version := COALESCE(
        NULLIF(v_settings_json->>'banking_pay_workbench_source_build_execution_profile_version','')::integer,
        1
      );
    EXCEPTION WHEN OTHERS THEN
      v_execution_profile_version := 1;
    END;
    IF v_execution_profile_version NOT IN (1,2) THEN
      v_execution_profile_version := 1;
    END IF;
    IF v_execution_profile_version=2 THEN
      BEGIN
        v_reconciliation_optimization_version:=COALESCE(
          NULLIF(v_settings_json->>'banking_pay_workbench_reconciliation_optimization_version','')::integer,0
        );
      EXCEPTION WHEN OTHERS THEN
        v_reconciliation_optimization_version:=0;
      END;
    END IF;
    v_reconciliation_optimization_version:=CASE WHEN v_reconciliation_optimization_version=1 THEN 1 ELSE 0 END;
    IF COALESCE(v_job.payload_json->>'source_build_run_id','')
       !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED'
        USING ERRCODE='22023', DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED','job_id',v_job.id
        )::text;
    END IF;
    v_source_build_run_id := (v_job.payload_json->>'source_build_run_id')::uuid;
    IF v_same_authority_election_enabled THEN
      IF COALESCE(v_job.payload_json->>'authority_fingerprint_version','') NOT IN ('2','3')
         OR COALESCE(v_job.payload_json->>'authority_fingerprint','') !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_SOURCE_BUILD_AUTHORITY_FINGERPRINT_REQUIRED'
          USING ERRCODE='22023',DETAIL=pg_catalog.jsonb_build_object(
            'code','PAY_WORKBENCH_SOURCE_BUILD_AUTHORITY_FINGERPRINT_REQUIRED',
            'job_id',v_job.id,
            'candidate_id',v_job.candidate_id
          )::text;
      END IF;
      v_authority_fingerprint_version:=(v_job.payload_json->>'authority_fingerprint_version')::smallint;
      v_authority_fingerprint:=v_job.payload_json->>'authority_fingerprint';
      v_required_physical_publication_contract_version:=CASE
        WHEN v_authority_fingerprint_version=3 THEN COALESCE(
          NULLIF(v_job.payload_json->>'required_physical_publication_contract_version','')::smallint,
          0
        ) ELSE 0 END;
      IF v_authority_fingerprint_version=3
         AND v_required_physical_publication_contract_version NOT IN (0,1) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_SOURCE_BUILD_PHYSICAL_PUBLICATION_CONTRACT_REQUIRED'
          USING ERRCODE='22023',DETAIL=pg_catalog.jsonb_build_object(
            'code','PAY_WORKBENCH_SOURCE_BUILD_PHYSICAL_PUBLICATION_CONTRACT_REQUIRED',
            'job_id',v_job.id,'candidate_id',v_job.candidate_id
          )::text;
      END IF;
    END IF;
    v_is_bootstrap := COALESCE(v_job.payload_json->>'bootstrap_id','')
      ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND v_registry.initialisation_status<>'READY';
    IF v_is_bootstrap THEN
      v_bootstrap_id := (v_job.payload_json->>'bootstrap_id')::uuid;
    END IF;
    v_build_id := gen_random_uuid();
    BEGIN
    INSERT INTO private.banking_pay_workbench_economic_builds(
      id,candidate_id,session_id,session_version,source_snapshot_run_id,
      source_build_run_id,source_job_id,captured_candidate_generation,
      source_change_seq,status,private_stage,scope_cursor_json,attestation_json,
      authority_fingerprint_version,authority_fingerprint
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
      ) END,
      jsonb_build_object(
        'execution_profile_version',v_execution_profile_version,
        'execution_profile_frozen_at_utc',clock_timestamp(),
        'execution_profile_authority','BUILD_CREATION_SETTINGS',
        'reconciliation_optimization_version',v_reconciliation_optimization_version,
        'reconciliation_optimization_frozen_at_utc',clock_timestamp(),
        'reconciliation_optimization_authority','BUILD_CREATION_SETTINGS',
        'authority_fingerprint_version',v_authority_fingerprint_version,
        'authority_fingerprint',v_authority_fingerprint,
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'source_publication_baseline_required',v_required_physical_publication_contract_version>=1
      ),
      v_authority_fingerprint_version,v_authority_fingerprint
    ) RETURNING id,scope_cursor_json INTO v_build_id,v_cursor_json;
    EXCEPTION WHEN unique_violation THEN
      GET STACKED DIAGNOSTICS v_unique_constraint_name=CONSTRAINT_NAME;
      IF v_same_authority_election_enabled IS NOT TRUE
         OR v_unique_constraint_name IS DISTINCT FROM 'uq_bpay_wb_economic_build_authority_v1'
         OR v_authority_fingerprint_version IS NULL
         OR v_authority_fingerprint IS NULL THEN
        RAISE;
      END IF;

      SELECT winner.* INTO v_winner_build
      FROM private.banking_pay_workbench_economic_builds AS winner
      WHERE winner.candidate_id=v_job.candidate_id
        AND winner.authority_fingerprint_version=v_authority_fingerprint_version
        AND winner.authority_fingerprint=v_authority_fingerprint
        AND winner.status NOT IN ('FAILED','OBSOLETE','CLEANING')
      ORDER BY winner.created_at_utc,winner.id
      LIMIT 1
      FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_SAME_AUTHORITY_WINNER_NOT_PROVEN'
          USING ERRCODE='40001',DETAIL=pg_catalog.jsonb_build_object(
            'code','PAY_WORKBENCH_SAME_AUTHORITY_WINNER_NOT_PROVEN',
            'job_id',v_job.id,'candidate_id',v_job.candidate_id,
            'authority_fingerprint_version',v_authority_fingerprint_version,
            'authority_fingerprint',v_authority_fingerprint
          )::text;
      END IF;

      IF v_winner_build.status='COMPLETE' THEN
        v_winner_scope:=NULL;
        SELECT current_scope.*
        INTO v_winner_scope
        FROM public.banking_pay_workbench_session_scope AS current_scope
        JOIN public.banking_pay_workbench_session_candidate_state AS current_state
          ON current_state.session_id=current_scope.session_id
         AND current_state.candidate_id=current_scope.candidate_id
        WHERE current_scope.session_id=v_job.session_id
          AND current_scope.candidate_id=v_job.candidate_id
          AND current_scope.pending_job_id=v_job.id
          AND current_scope.certified_preview_publication_required IS TRUE
          AND current_scope.certified_preview_publication_parity_ok IS TRUE
          AND current_scope.certified_preview_publication_session_version=v_winner_build.session_version
          AND current_scope.certified_preview_publication_source_change_seq=v_winner_build.source_change_seq
          AND current_scope.certified_preview_publication_source_build_run_id=v_winner_build.source_build_run_id
          AND (
            COALESCE(
              (v_settings_json->>'banking_pay_workbench_semantic_ready_publication_v3_enabled')::boolean,
              false
            ) IS NOT TRUE
            OR (
              current_scope.certified_preview_publication_attestation_json->>'attestation_version'
                    ='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
              AND current_scope.certified_preview_publication_attestation_json->>'contract_version'='3'
              AND current_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
                    ='READY_TO_PAY_SEMANTIC_V2'
              AND COALESCE(
                (current_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,
                false
              )
              AND COALESCE(
                (current_scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean,
                false
              )
            )
          )
          AND (
            v_required_physical_publication_contract_version=0
            OR (
              current_scope.certified_preview_publication_source_publication_id IS NOT NULL
              AND current_scope.certified_preview_publication_attestation_json->>'source_publication_id'
                    =current_scope.certified_preview_publication_source_publication_id::text
            )
          )
          AND current_state.pending_job_id=v_job.id
          AND current_state.session_version=v_winner_build.session_version
          AND current_state.source_change_seq=v_winner_build.source_change_seq
        FOR UPDATE OF current_scope,current_state;
        v_winner_complete_current_proven:=FOUND;

        IF v_winner_complete_current_proven THEN
          UPDATE public.banking_pay_workbench_jobs AS loser_job
          SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp(),
              economic_build_id=NULL,private_stage=NULL,private_cursor_kind=NULL,
              private_cursor_json='{}'::jsonb,private_stage_version=NULL,
              last_error_json=pg_catalog.jsonb_build_object(
                'code','SAME_AUTHORITY_COMPLETE_WINNER_COALESCED',
                'winner_build_id',v_winner_build.id,
                'authority_fingerprint_version',v_authority_fingerprint_version,
                'authority_fingerprint',v_authority_fingerprint
              )
          WHERE loser_job.id=v_job.id AND loser_job.status='QUEUED';

          UPDATE public.banking_pay_workbench_session_scope AS scope_row
          SET status='MATERIALISED',dirty=false,pending_job_id=NULL,error_json=NULL,
              updated_at_utc=clock_timestamp()
          WHERE scope_row.session_id=v_job.session_id
            AND scope_row.candidate_id=v_job.candidate_id
            AND scope_row.pending_job_id=v_job.id;
          UPDATE public.banking_pay_workbench_session_candidate_state AS state_row
          SET status='READY',pending_job_id=NULL,updated_at_utc=clock_timestamp()
          WHERE state_row.session_id=v_job.session_id
            AND state_row.candidate_id=v_job.candidate_id
            AND state_row.pending_job_id=v_job.id;
          UPDATE private.banking_pay_workbench_candidate_scope_registry
          SET current_build_id=v_winner_build.id,updated_at_utc=clock_timestamp()
          WHERE candidate_id=v_job.candidate_id
            AND current_source_change_seq=v_winner_build.source_change_seq
            AND dirty_generation=v_winner_build.captured_candidate_generation;

          RETURN pg_catalog.jsonb_build_object(
            'ok',true,'claimed',false,
            'result_code','SAME_AUTHORITY_COMPLETE_WINNER_COALESCED',
            'job_id',v_job.id,'candidate_id',v_job.candidate_id,
            'winner_build_id',v_winner_build.id,
            'authority_fingerprint_version',v_authority_fingerprint_version,
            'authority_fingerprint',v_authority_fingerprint,
            'current_terminal_authority',true
          );
        END IF;

        -- A legacy V2 owner can be economically complete while lacking the
        -- newly required V3 semantic publication.  The losing V2 job must not
        -- spin forever on the uniqueness fence: terminate it, then let the
        -- canonical orphan-repair/enqueue owner elect the distinct V3
        -- successor in this same transaction.
        IF COALESCE(
             (v_settings_json->>'banking_pay_workbench_semantic_ready_publication_v3_enabled')::boolean,
             false
           )
           AND COALESCE(v_authority_fingerprint_version,0)<3 THEN
          UPDATE public.banking_pay_workbench_jobs AS loser_job
          SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp(),
              economic_build_id=NULL,private_stage=NULL,private_cursor_kind=NULL,
              private_cursor_json='{}'::jsonb,private_stage_version=NULL,
              last_error_json=pg_catalog.jsonb_build_object(
                'code','SAME_AUTHORITY_LEGACY_WINNER_V3_REELECTION_REQUIRED',
                'winner_build_id',v_winner_build.id,
                'authority_fingerprint_version',v_authority_fingerprint_version,
                'authority_fingerprint',v_authority_fingerprint
              )
          WHERE loser_job.id=v_job.id AND loser_job.status='QUEUED';

          v_obsolete_repair_result:=public.pay_workbench_repair_orphaned_pending_source_build(
            p_session_id=>v_job.session_id,
            p_candidate_id=>v_job.candidate_id,
            p_limit=>1,
            p_now_utc=>v_database_now,
            p_reason=>'SAME_AUTHORITY_LEGACY_WINNER_V3_REELECTION'
          );

          IF jsonb_typeof(v_obsolete_repair_result) IS DISTINCT FROM 'object'
             OR lower(BTRIM(COALESCE(v_obsolete_repair_result->>'ok','false'))) <> 'true'
             OR COALESCE((v_obsolete_repair_result->>'unresolved_count')::integer,0) <> 0
             OR lower(BTRIM(COALESCE(
                  v_obsolete_repair_result->>'all_state_transitions_proven','false'
                ))) <> 'true' THEN
            RAISE EXCEPTION 'SOURCE_BUILD_SAME_AUTHORITY_V3_SUCCESSOR_NOT_PROVEN'
              USING ERRCODE='40001',DETAIL=pg_catalog.jsonb_build_object(
                'code','SOURCE_BUILD_SAME_AUTHORITY_V3_SUCCESSOR_NOT_PROVEN',
                'job_id',v_job.id,
                'candidate_id',v_job.candidate_id,
                'winner_build_id',v_winner_build.id
              )::text;
          END IF;

          SELECT pending_scope.pending_job_id,
                 EXISTS (
                   SELECT 1
                   FROM public.banking_pay_workbench_jobs AS successor_job
                   JOIN public.banking_pay_workbench_session_candidate_state AS successor_state
                     ON successor_state.session_id=successor_job.session_id
                    AND successor_state.candidate_id=successor_job.candidate_id
                   WHERE successor_job.id=pending_scope.pending_job_id
                     AND successor_job.session_id=v_job.session_id
                     AND successor_job.candidate_id=v_job.candidate_id
                     AND successor_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
                     AND successor_job.status IN ('QUEUED','RUNNING')
                     AND COALESCE(
                       NULLIF(successor_job.payload_json->>'authority_fingerprint_version','')::integer,
                       0
                     )>=3
                     AND successor_state.status='PENDING'
                     AND successor_state.pending_job_id=successor_job.id
                 )
          INTO v_obsolete_successor_job_id,
               v_obsolete_active_successor_proven
          FROM public.banking_pay_workbench_session_scope AS pending_scope
          WHERE pending_scope.session_id=v_job.session_id
            AND pending_scope.candidate_id=v_job.candidate_id;

          IF COALESCE(v_obsolete_active_successor_proven,false) IS NOT TRUE THEN
            RAISE EXCEPTION 'SOURCE_BUILD_SAME_AUTHORITY_V3_SUCCESSOR_NOT_PROVEN'
              USING ERRCODE='40001',DETAIL=pg_catalog.jsonb_build_object(
                'code','SOURCE_BUILD_SAME_AUTHORITY_V3_SUCCESSOR_NOT_PROVEN',
                'job_id',v_job.id,
                'candidate_id',v_job.candidate_id,
                'successor_job_id',v_obsolete_successor_job_id
              )::text;
          END IF;

          RETURN pg_catalog.jsonb_build_object(
            'ok',true,'claimed',false,
            'result_code','SAME_AUTHORITY_LEGACY_WINNER_V3_REELECTED',
            'job_id',v_job.id,'candidate_id',v_job.candidate_id,
            'winner_build_id',v_winner_build.id,
            'successor_resolution',pg_catalog.jsonb_build_object(
              'required',true,'proven',true,
              'successor_created_or_reused',true,
              'successor_job_id',v_obsolete_successor_job_id
            )
          );
        END IF;

        RAISE EXCEPTION 'PAY_WORKBENCH_SAME_AUTHORITY_COMPLETE_WINNER_NOT_CURRENT'
          USING ERRCODE='40001',DETAIL=pg_catalog.jsonb_build_object(
            'code','PAY_WORKBENCH_SAME_AUTHORITY_COMPLETE_WINNER_NOT_CURRENT',
            'job_id',v_job.id,'winner_build_id',v_winner_build.id
          )::text;
      END IF;

      SELECT winner_job.id INTO v_winner_active_job_id
      FROM public.banking_pay_workbench_jobs AS winner_job
      WHERE winner_job.economic_build_id=v_winner_build.id
        AND winner_job.status IN ('QUEUED','RUNNING')
      ORDER BY CASE WHEN winner_job.status='RUNNING' THEN 0 ELSE 1 END,
               winner_job.run_at_utc,winner_job.created_at_utc,winner_job.id
      LIMIT 1
      FOR UPDATE;
      IF v_winner_active_job_id IS NULL THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_SAME_AUTHORITY_ACTIVE_WINNER_NOT_PROVEN'
          USING ERRCODE='40001',DETAIL=pg_catalog.jsonb_build_object(
            'code','PAY_WORKBENCH_SAME_AUTHORITY_ACTIVE_WINNER_NOT_PROVEN',
            'job_id',v_job.id,'winner_build_id',v_winner_build.id
          )::text;
      END IF;

      UPDATE public.banking_pay_workbench_jobs AS loser_job
      SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp(),
          economic_build_id=NULL,private_stage=NULL,private_cursor_kind=NULL,
          private_cursor_json='{}'::jsonb,private_stage_version=NULL,
          last_error_json=pg_catalog.jsonb_build_object(
            'code','SAME_AUTHORITY_OWNER_COALESCED',
            'winner_build_id',v_winner_build.id,
            'winner_job_id',v_winner_active_job_id,
            'authority_fingerprint_version',v_authority_fingerprint_version,
            'authority_fingerprint',v_authority_fingerprint
          )
      WHERE loser_job.id=v_job.id AND loser_job.status='QUEUED';

      UPDATE public.banking_pay_workbench_session_scope AS scope_row
      SET pending_job_id=v_winner_active_job_id,updated_at_utc=clock_timestamp()
      WHERE scope_row.session_id=v_job.session_id
        AND scope_row.candidate_id=v_job.candidate_id
        AND scope_row.pending_job_id=v_job.id;
      UPDATE public.banking_pay_workbench_session_candidate_state AS state_row
      SET pending_job_id=v_winner_active_job_id,updated_at_utc=clock_timestamp()
      WHERE state_row.session_id=v_job.session_id
        AND state_row.candidate_id=v_job.candidate_id
        AND state_row.pending_job_id=v_job.id;
      UPDATE private.banking_pay_workbench_candidate_scope_registry
      SET current_build_id=v_winner_build.id,updated_at_utc=clock_timestamp()
      WHERE candidate_id=v_job.candidate_id;

      RETURN pg_catalog.jsonb_build_object(
        'ok',true,'claimed',false,'result_code','SAME_AUTHORITY_OWNER_COALESCED',
        'job_id',v_job.id,'candidate_id',v_job.candidate_id,
        'winner_build_id',v_winner_build.id,'winner_job_id',v_winner_active_job_id,
        'authority_fingerprint_version',v_authority_fingerprint_version,
        'authority_fingerprint',v_authority_fingerprint
      );
    END;
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
      v_obsolete_repair_result:=public.pay_workbench_repair_orphaned_pending_source_build(
        p_session_id=>v_job.session_id,
        p_candidate_id=>v_job.candidate_id,
        p_limit=>1,
        p_now_utc=>v_database_now,
        p_reason=>'ATTEMPT_GENERATION_OBSOLETE_SUCCESSOR'
      );

      IF jsonb_typeof(v_obsolete_repair_result) IS DISTINCT FROM 'object'
         OR lower(BTRIM(COALESCE(v_obsolete_repair_result->>'ok','false'))) <> 'true' THEN
        RAISE EXCEPTION 'SOURCE_BUILD_OBSOLETE_SUCCESSOR_NOT_PROVEN'
          USING ERRCODE='40001',DETAIL=jsonb_build_object(
            'code','SOURCE_BUILD_OBSOLETE_SUCCESSOR_NOT_PROVEN',
            'job_id',v_job.id,
            'candidate_id',v_job.candidate_id,
            'repair_result_code',v_obsolete_repair_result->>'repair_code'
          )::text;
      END IF;

      SELECT pending_scope.pending_job_id,
             EXISTS (
               SELECT 1
               FROM public.banking_pay_workbench_jobs AS successor_job
               JOIN public.banking_pay_workbench_session_candidate_state AS successor_state
                 ON successor_state.session_id=successor_job.session_id
                AND successor_state.candidate_id=successor_job.candidate_id
               WHERE successor_job.id=pending_scope.pending_job_id
                 AND successor_job.session_id=v_job.session_id
                 AND successor_job.candidate_id=v_job.candidate_id
                 AND successor_job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
                 AND successor_job.status IN ('QUEUED','RUNNING')
                 AND CASE
                       WHEN COALESCE(successor_job.payload_json->>'source_change_seq','') ~ '^\d+$'
                         THEN (successor_job.payload_json->>'source_change_seq')::bigint
                       ELSE NULL::bigint
                     END=successor_registry.current_source_change_seq
                 AND COALESCE(
                       successor_job.scope_change_generation,
                       CASE
                         WHEN COALESCE(successor_job.payload_json->>'scope_change_generation','') ~ '^\d+$'
                           THEN (successor_job.payload_json->>'scope_change_generation')::bigint
                         ELSE NULL::bigint
                       END,
                       successor_registry.dirty_generation
                     )=successor_registry.dirty_generation
                 AND successor_state.status='PENDING'
                 AND successor_state.pending_job_id=successor_job.id
                 AND successor_state.source_change_seq=successor_registry.current_source_change_seq
                 AND successor_state.session_version=v_session.version
             ),
             (
               pending_scope.status IN ('READY','MATERIALISED','MATERIALIZED','SOURCE_EMPTY')
               AND COALESCE(pending_scope.dirty,false) IS NOT TRUE
               AND pending_scope.pending_job_id IS NULL
               AND COALESCE(pending_scope.certified_preview_publication_required,false) IS TRUE
               AND COALESCE(pending_scope.certified_preview_publication_parity_ok,false) IS TRUE
               AND pending_scope.certified_preview_publication_session_version=v_session.version
               AND pending_scope.certified_preview_publication_source_change_seq=
                   successor_registry.current_source_change_seq
             )
      INTO v_obsolete_successor_job_id,
           v_obsolete_active_successor_proven,
           v_obsolete_terminal_current_proven
      FROM public.banking_pay_workbench_session_scope AS pending_scope
      JOIN private.banking_pay_workbench_candidate_scope_registry AS successor_registry
        ON successor_registry.candidate_id=pending_scope.candidate_id
      WHERE pending_scope.session_id=v_job.session_id
        AND pending_scope.candidate_id=v_job.candidate_id;

      IF COALESCE(v_obsolete_active_successor_proven,false) IS NOT TRUE
         AND COALESCE(v_obsolete_terminal_current_proven,false) IS NOT TRUE THEN
        v_obsolete_successor_wait_count:=CASE
          WHEN COALESCE(v_job.payload_json->>'obsolete_successor_wait_count','')~'^[0-9]{1,9}$'
            THEN LEAST((v_job.payload_json->>'obsolete_successor_wait_count')::integer,1000000)
          ELSE 0
        END;
        v_obsolete_successor_wait_seconds:=LEAST(30,GREATEST(2,
          power(2,LEAST(v_obsolete_successor_wait_count+1,5))::integer
        ));
        v_obsolete_successor_retry_at_utc:=pg_catalog.clock_timestamp()
          +pg_catalog.make_interval(secs=>v_obsolete_successor_wait_seconds);

        UPDATE public.banking_pay_workbench_jobs AS waiting_job
        SET run_at_utc=v_obsolete_successor_retry_at_utc,
            payload_json=(COALESCE(waiting_job.payload_json,'{}'::jsonb)
              -'obsolete_successor_retry_at_utc')||pg_catalog.jsonb_build_object(
                'obsolete_successor_wait_contract_version',
                  'SOURCE_BUILD_OBSOLETE_SUCCESSOR_WAIT_V1',
                'obsolete_successor_wait_count',v_obsolete_successor_wait_count+1,
                'obsolete_successor_wait_started_at_utc',COALESCE(
                  waiting_job.payload_json->>'obsolete_successor_wait_started_at_utc',
                  pg_catalog.clock_timestamp()::text
                ),
                'obsolete_successor_retry_at_utc',v_obsolete_successor_retry_at_utc,
                'obsolete_successor_expected_source_change_seq',
                  v_registry.current_source_change_seq,
                'obsolete_successor_expected_dirty_generation',v_registry.dirty_generation,
                'obsolete_successor_last_repair_code',
                  v_obsolete_repair_result->>'repair_code',
                'obsolete_successor_repair_unresolved_count',COALESCE(
                  CASE WHEN COALESCE(v_obsolete_repair_result->>'unresolved_count','')
                    ~'^[0-9]{1,9}$'
                    THEN (v_obsolete_repair_result->>'unresolved_count')::integer END,0
                ),
                'obsolete_successor_repair_transitions_proven',
                  pg_catalog.lower(pg_catalog.btrim(COALESCE(
                    v_obsolete_repair_result->>'all_state_transitions_proven','false'
                  ))) IN ('true','t','1','yes','y','on')
              ),
            updated_at_utc=pg_catalog.clock_timestamp()
        WHERE waiting_job.id=v_job.id AND waiting_job.status='QUEUED';

        RETURN pg_catalog.jsonb_build_object(
          'ok',true,'claimed',false,
          'result_code','SOURCE_BUILD_OBSOLETE_SUCCESSOR_PENDING',
          'job_id',v_job.id,'candidate_id',v_job.candidate_id,
          'retry_at_utc',v_obsolete_successor_retry_at_utc,
          'repair_result_code',v_obsolete_repair_result->>'repair_code',
          'expected_source_change_seq',v_registry.current_source_change_seq,
          'expected_generation',v_registry.dirty_generation
        );
      END IF;

      UPDATE public.banking_pay_workbench_jobs AS obsolete_job
      SET status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp(),
          economic_build_id=NULL,private_stage=NULL,private_cursor_kind=NULL,
          private_cursor_json='{}'::jsonb,private_stage_version=NULL,
          payload_json=COALESCE(obsolete_job.payload_json,'{}'::jsonb)
            -'obsolete_successor_wait_contract_version'
            -'obsolete_successor_wait_count'
            -'obsolete_successor_wait_started_at_utc'
            -'obsolete_successor_retry_at_utc'
            -'obsolete_successor_expected_source_change_seq'
            -'obsolete_successor_expected_dirty_generation'
            -'obsolete_successor_last_repair_code'
            -'obsolete_successor_repair_unresolved_count'
            -'obsolete_successor_repair_transitions_proven',
          last_error_json=pg_catalog.jsonb_build_object('code','ATTEMPT_GENERATION_OBSOLETE'),
          updated_at_utc=pg_catalog.clock_timestamp()
      WHERE obsolete_job.id=v_job.id AND obsolete_job.status='QUEUED';

      RETURN jsonb_build_object('ok',true,'claimed',false,
        'result_code','ATTEMPT_GENERATION_OBSOLETE',
        'successor_resolution',jsonb_build_object(
          'required',true,
          'proven',true,
          'successor_created_or_reused',v_obsolete_active_successor_proven,
          'successor_job_id',v_obsolete_successor_job_id,
          'current_terminal_authority',v_obsolete_terminal_current_proven,
          'source_change_seq',v_registry.current_source_change_seq,
          'generation',v_registry.dirty_generation
        ));
    END IF;
    v_captured_generation:=v_build.captured_candidate_generation;
    v_source_change_seq:=v_build.source_change_seq;
    BEGIN
      v_execution_profile_version := COALESCE(
        NULLIF(v_build.attestation_json->>'execution_profile_version','')::integer,
        1
      );
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_EXECUTION_PROFILE_INVALID'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_EXECUTION_PROFILE_INVALID','build_id',v_build_id
        )::text;
    END;
    IF v_execution_profile_version NOT IN (1,2)
       OR EXISTS (
         SELECT 1
         FROM private.banking_pay_workbench_stage_attempts AS prior_attempt
         WHERE prior_attempt.build_id=v_build_id
           AND prior_attempt.execution_profile_version IS DISTINCT FROM v_execution_profile_version
       ) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_EXECUTION_PROFILE_CONFLICT'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_EXECUTION_PROFILE_CONFLICT',
          'build_id',v_build_id,
          'execution_profile_version',v_execution_profile_version
        )::text;
    END IF;
    BEGIN
      v_reconciliation_optimization_version:=COALESCE(
        NULLIF(v_build.attestation_json->>'reconciliation_optimization_version','')::integer,0
      );
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_OPTIMIZATION_INVALID'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_RECONCILIATION_OPTIMIZATION_INVALID','build_id',v_build_id
        )::text;
    END;
    IF v_reconciliation_optimization_version NOT IN (0,1)
       OR (v_execution_profile_version=1 AND v_reconciliation_optimization_version<>0) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_OPTIMIZATION_CONFLICT'
        USING ERRCODE='P0001';
    END IF;
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
    v_captured_generation,v_source_change_seq,v_execution_profile_version,v_attempt_started_at,v_lease_expires,
    v_attempt_started_at,v_attempt_started_at
  ) RETURNING id,attempt_nonce INTO v_attempt_id,v_attempt_nonce;

  RETURN jsonb_build_object(
    'ok',true,'claimed',true,'job_id',v_job.id,'build_id',v_build_id,
    'candidate_id',v_job.candidate_id,'private_stage',v_stage,
    'attempt_id',v_attempt_id,'attempt_number',v_attempt_number,
    'attempt_nonce',v_attempt_nonce,'attempt_started_at_utc',v_attempt_started_at,
    'lease_expires_at_utc',v_lease_expires,'execution_profile_version',v_execution_profile_version,
    'reconciliation_optimization_version',v_reconciliation_optimization_version
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid) TO service_role;
