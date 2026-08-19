-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_fail_job(p_job_id uuid, p_error_json jsonb, p_retry_after_seconds integer)
-- Installed pg_get_functiondef MD5: ecd46bc980e3f07e3be2d4905025d1fa
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_fail_job(p_job_id uuid, p_error_json jsonb, p_retry_after_seconds integer DEFAULT NULL::integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz := now();
  v_job_type text;
  v_canonical_job_type text := NULL::text;
  v_attempt_count integer;
  v_max_attempts integer;
  v_retry_after_seconds integer;
  v_next_run_at_utc timestamptz;
  v_new_status text;
  v_failed_at_utc timestamptz;
  v_job_snapshot_run_id uuid := NULL::uuid;
  v_job_session_id uuid := NULL::uuid;
  v_job_candidate_id uuid := NULL::uuid;
  v_job_dedupe_key text := NULL;
  v_job_created_at_utc timestamptz := NULL::timestamptz;
  v_job_payload_json jsonb := '{}'::jsonb;
  v_job_source_change_seq bigint := 0;
  v_job_session_version bigint := 0;
  v_live_candidate_change_seq bigint := 0;
  v_snapshot_state_status text := NULL;
  v_snapshot_state_source_change_seq bigint := 0;
  v_current_session_status text := NULL;
  v_current_session_version bigint := 0;
  v_current_session_snapshot_run_id uuid := NULL::uuid;
  v_session_candidate_status text := NULL;
  v_session_candidate_source_change_seq bigint := 0;
  v_session_candidate_session_version bigint := 0;
  v_other_active_job_id uuid := NULL::uuid;
  v_completed_equivalent_job_id uuid := NULL::uuid;
  v_is_obsolete boolean := false;
  v_obsolete_reason text := NULL;
  v_error_code text := UPPER(COALESCE(p_error_json->>'code', ''));
  v_error_message text := UPPER(COALESCE(p_error_json->>'message', ''));
  v_is_statement_timeout boolean := false;
  v_is_no_change_loop boolean := false;
  v_failed_line_work_count integer := 0;
  v_failed_source_row_count integer := 0;
  v_terminal_scope_updated_count integer := 0;
  v_source_build_run_id_text text := NULL::text;
  v_invalid_source_build_without_run_id boolean := false;
  v_economic_build_id uuid := NULL::uuid;
  v_private_stage text := NULL::text;
  v_attempt_id uuid := NULL::uuid;
  v_scale_block boolean := false;
  v_is_deterministic_stage_error boolean := false;
  v_terminal_repair_result jsonb := '{}'::jsonb;
  v_terminal_scope_status text := NULL::text;
  v_terminal_scope_pending_job_id uuid := NULL::uuid;
  v_terminal_scope_error_json jsonb := NULL::jsonb;
  v_terminal_owner_valid boolean := false;
BEGIN
  IF p_job_id IS NULL THEN
    RAISE EXCEPTION 'job_id is required';
  END IF;

  IF p_error_json IS NULL THEN
    RAISE EXCEPTION 'error_json is required';
  END IF;

  SELECT j.job_type,
         j.attempt_count,
         j.max_attempts,
         j.snapshot_run_id,
         j.session_id,
         j.candidate_id,
         j.dedupe_key,
         j.created_at_utc,
         COALESCE(j.payload_json, '{}'::jsonb),
         j.economic_build_id,
         j.private_stage
  INTO v_job_type,
       v_attempt_count,
       v_max_attempts,
       v_job_snapshot_run_id,
       v_job_session_id,
       v_job_candidate_id,
       v_job_dedupe_key,
       v_job_created_at_utc,
       v_job_payload_json,
       v_economic_build_id,
       v_private_stage
  FROM public.banking_pay_workbench_jobs AS j
  WHERE j.id = p_job_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_jobs row % not found', p_job_id;
  END IF;

  v_job_source_change_seq := COALESCE(
    CASE
      WHEN COALESCE(v_job_payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
        THEN (v_job_payload_json->>'source_change_seq')::bigint
      ELSE 0::bigint
    END,
    0::bigint
  );
  v_job_session_version := COALESCE(
    CASE
      WHEN COALESCE(v_job_payload_json->>'session_version', '') ~ '^[0-9]+$'
        THEN (v_job_payload_json->>'session_version')::bigint
      ELSE 0::bigint
    END,
    0::bigint
  );

  v_canonical_job_type := CASE
    WHEN UPPER(BTRIM(COALESCE(v_job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
      THEN 'WORKBENCH_SESSION_SCOPE_SEED'
    WHEN UPPER(BTRIM(COALESCE(v_job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
      THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    WHEN UPPER(BTRIM(COALESCE(v_job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE')
      THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
    WHEN UPPER(BTRIM(COALESCE(v_job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
      THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
    WHEN UPPER(BTRIM(COALESCE(v_job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
      THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
    ELSE UPPER(BTRIM(COALESCE(v_job_type, '')))
  END;

  -- Source-stage terminalisation and orphan repair must share the same
  -- candidate-serial authority.  The initial job read above is deliberately
  -- unlocked; candidate ownership is established before the job/attempt rows.
  IF v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
     AND v_job_candidate_id IS NOT NULL THEN
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(v_job_candidate_id),
      24062027
    ));
  END IF;

  IF v_canonical_job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
     AND v_economic_build_id IS NOT NULL THEN
    PERFORM 1 FROM public.banking_pay_workbench_jobs WHERE id=p_job_id FOR UPDATE;
    SELECT attempt.id INTO v_attempt_id
    FROM private.banking_pay_workbench_stage_attempts attempt
    WHERE attempt.job_id=p_job_id AND attempt.build_id=v_economic_build_id
      AND attempt.private_stage=v_private_stage AND attempt.attempt_status='STARTED'
    ORDER BY attempt.attempt_number DESC,attempt.id DESC LIMIT 1 FOR UPDATE;
    IF v_attempt_id IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ATTEMPT_FAILURE_CONTEXT_REQUIRED' USING ERRCODE='40001';
    END IF;
    v_scale_block:=v_error_code IN ('BLOCKED_UNVALIDATED_RECONCILIATION_SCALE',
      'PAY_WORKBENCH_UNVALIDATED_RECONCILIATION_SCALE');
    v_is_deterministic_stage_error := v_error_code IN (
      'CERTIFIED_SOURCE_PREVIEW_SEMANTIC_PARITY_FAILED',
      'PAYMENT_CORRECTION_SCOPE_TYPE_REQUIRED',
      'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISSING',
      'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH',
      'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED'
    );
    v_is_obsolete:=v_error_code IN ('PAY_WORKBENCH_BUILD_NOT_CURRENT','PAY_WORKBENCH_RECONCILIATION_ATTEMPT_STALE',
      'PAY_WORKBENCH_GENERATION_CHANGED','PAY_WORKBENCH_SOURCE_SEQUENCE_CHANGED');
    v_obsolete_reason:=CASE WHEN v_is_obsolete THEN COALESCE(NULLIF(v_error_code,''),'BUILD_OBSOLETE') END;
    UPDATE private.banking_pay_workbench_stage_attempts SET
      attempt_status=CASE WHEN v_is_obsolete THEN 'OBSOLETE' ELSE 'FAILED' END,
      failed_at_utc=CASE WHEN v_is_obsolete THEN NULL ELSE clock_timestamp() END,
      obsolete_at_utc=CASE WHEN v_is_obsolete THEN clock_timestamp() ELSE NULL END,
      result_code=COALESCE(NULLIF(v_error_code,''),'STAGE_ERROR'),
      error_class=CASE
        WHEN v_scale_block THEN 'SCALE_BLOCK'
        WHEN v_is_obsolete THEN 'OBSOLETE'
        WHEN v_is_deterministic_stage_error THEN 'DETERMINISTIC_STAGE_ERROR'
        ELSE 'RETRYABLE_STAGE_ERROR'
      END,
      error_json=jsonb_strip_nulls(jsonb_build_object('code',p_error_json->>'code',
        'message',p_error_json->>'message','sqlstate',p_error_json->>'sqlstate')),
      updated_at_utc=clock_timestamp()
    WHERE id=v_attempt_id AND attempt_status='STARTED';
    IF v_scale_block THEN
      UPDATE private.banking_pay_workbench_economic_builds SET
        status='BLOCKED_UNVALIDATED_RECONCILIATION_SCALE',failure_json=p_error_json,
        updated_at_utc=clock_timestamp() WHERE id=v_economic_build_id;
      UPDATE public.banking_pay_workbench_jobs SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),
        failed_at_utc=NULL,last_error_json=p_error_json,updated_at_utc=clock_timestamp()
      WHERE id=p_job_id AND status='RUNNING';
      RETURN jsonb_build_object('ok',true,'job_id',p_job_id,'status','SUCCEEDED',
        'result_code','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE','retry_scheduled',false);
    ELSIF v_is_obsolete THEN
      UPDATE private.banking_pay_workbench_economic_builds SET status='OBSOLETE',
        obsolete_at_utc=clock_timestamp(),failure_json=p_error_json,updated_at_utc=clock_timestamp()
      WHERE id=v_economic_build_id AND status NOT IN ('COMPLETE','FAILED');
      UPDATE public.banking_pay_workbench_jobs SET status='SUCCEEDED',completed_at_utc=clock_timestamp(),
        failed_at_utc=NULL,last_error_json=p_error_json,updated_at_utc=clock_timestamp()
      WHERE id=p_job_id AND status='RUNNING';

      v_terminal_repair_result := public.pay_workbench_repair_orphaned_pending_source_build(
        p_session_id => v_job_session_id,
        p_candidate_id => v_job_candidate_id,
        p_limit => 1,
        p_now_utc => clock_timestamp(),
        p_reason => 'OBSOLETE_SOURCE_STAGE_ATOMIC_REPAIR'
      );

      SELECT terminal_scope.status,
             terminal_scope.pending_job_id,
             terminal_scope.error_json
      INTO v_terminal_scope_status,
           v_terminal_scope_pending_job_id,
           v_terminal_scope_error_json
      FROM public.banking_pay_workbench_session_scope AS terminal_scope
      WHERE terminal_scope.session_id = v_job_session_id
        AND terminal_scope.candidate_id = v_job_candidate_id
      FOR UPDATE;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_OBSOLETE_SCOPE_MISSING'
          USING ERRCODE = 'P0001';
      END IF;

      IF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_PENDING'
         AND v_terminal_scope_pending_job_id IS NOT NULL THEN
        SELECT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_jobs AS terminal_owner
          WHERE terminal_owner.id = v_terminal_scope_pending_job_id
            AND terminal_owner.session_id = v_job_session_id
            AND terminal_owner.candidate_id = v_job_candidate_id
            AND UPPER(BTRIM(COALESCE(terminal_owner.status, ''))) IN ('QUEUED', 'RUNNING')
        )
        INTO v_terminal_owner_valid;
      ELSE
        v_terminal_owner_valid := false;
      END IF;

      IF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_PENDING'
         AND v_terminal_owner_valid IS NOT TRUE THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_OBSOLETE_SUCCESSOR_NOT_PROVEN'
          USING ERRCODE = 'P0001';
      END IF;

      IF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_ERROR'
         AND v_terminal_scope_pending_job_id IS NULL THEN
        UPDATE public.banking_pay_workbench_session_candidate_state AS terminal_candidate_state
        SET status = 'ERROR',
            pending_job_id = NULL::uuid,
            last_error_json = COALESCE(v_terminal_scope_error_json, p_error_json),
            updated_at_utc = clock_timestamp()
        WHERE terminal_candidate_state.session_id = v_job_session_id
          AND terminal_candidate_state.candidate_id = v_job_candidate_id;
      ELSIF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_PENDING'
            AND v_terminal_owner_valid THEN
        UPDATE public.banking_pay_workbench_session_candidate_state AS terminal_candidate_state
        SET status = 'PENDING',
            pending_job_id = v_terminal_scope_pending_job_id,
            last_error_json = '{}'::jsonb,
            updated_at_utc = clock_timestamp()
        WHERE terminal_candidate_state.session_id = v_job_session_id
          AND terminal_candidate_state.candidate_id = v_job_candidate_id;
      END IF;

      RETURN jsonb_build_object('ok',true,'job_id',p_job_id,'status','SUCCEEDED',
        'result_code','OBSOLETE','retry_scheduled',false,
        'terminal_repair_result',v_terminal_repair_result,
        'terminal_scope_status',v_terminal_scope_status,
        'terminal_successor_job_id',v_terminal_scope_pending_job_id,
        'terminal_successor_proven',(
          UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) <> 'SOURCE_BUILD_PENDING'
          OR v_terminal_owner_valid
        ));
    END IF;
    v_retry_after_seconds:=COALESCE(p_retry_after_seconds,
      LEAST(300,GREATEST(1,power(2,LEAST(COALESCE(v_attempt_count,1),8))::integer)));
    IF v_is_deterministic_stage_error IS NOT TRUE
       AND COALESCE(v_attempt_count,0)<COALESCE(v_max_attempts,8) THEN
      v_next_run_at_utc:=clock_timestamp()+make_interval(secs=>v_retry_after_seconds);
      UPDATE public.banking_pay_workbench_jobs SET status='QUEUED',run_at_utc=v_next_run_at_utc,
        started_at_utc=NULL,failed_at_utc=NULL,last_error_json=p_error_json,
        updated_at_utc=clock_timestamp() WHERE id=p_job_id AND status='RUNNING';
      RETURN jsonb_build_object('ok',true,'job_id',p_job_id,'status','QUEUED',
        'attempt_count',v_attempt_count,'max_attempts',v_max_attempts,
        'retry_scheduled',true,'next_run_at_utc',v_next_run_at_utc);
    END IF;
    UPDATE private.banking_pay_workbench_economic_builds SET status='FAILED',
      failed_at_utc=clock_timestamp(),failure_json=p_error_json,updated_at_utc=clock_timestamp()
    WHERE id=v_economic_build_id;
    UPDATE public.banking_pay_workbench_jobs SET status='FAILED',failed_at_utc=clock_timestamp(),
      completed_at_utc=NULL,last_error_json=p_error_json,updated_at_utc=clock_timestamp()
    WHERE id=p_job_id AND status='RUNNING';

    v_terminal_repair_result := public.pay_workbench_repair_orphaned_pending_source_build(
      p_session_id => v_job_session_id,
      p_candidate_id => v_job_candidate_id,
      p_limit => 1,
      p_now_utc => clock_timestamp(),
      p_reason => 'TERMINAL_SOURCE_STAGE_FAILURE_ATOMIC_REPAIR'
    );

    SELECT terminal_scope.status,
           terminal_scope.pending_job_id,
           terminal_scope.error_json
    INTO v_terminal_scope_status,
         v_terminal_scope_pending_job_id,
         v_terminal_scope_error_json
    FROM public.banking_pay_workbench_session_scope AS terminal_scope
    WHERE terminal_scope.session_id = v_job_session_id
      AND terminal_scope.candidate_id = v_job_candidate_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_TERMINAL_FAILURE_SCOPE_MISSING'
        USING ERRCODE = 'P0001';
    END IF;

    IF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_PENDING'
       AND v_terminal_scope_pending_job_id IS NOT NULL THEN
      SELECT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS terminal_owner
        WHERE terminal_owner.id = v_terminal_scope_pending_job_id
          AND terminal_owner.session_id = v_job_session_id
          AND terminal_owner.candidate_id = v_job_candidate_id
          AND UPPER(BTRIM(COALESCE(terminal_owner.status, ''))) IN ('QUEUED', 'RUNNING')
      )
      INTO v_terminal_owner_valid;
    ELSE
      v_terminal_owner_valid := false;
    END IF;

    IF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_PENDING'
       AND v_terminal_owner_valid IS NOT TRUE THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_TERMINAL_FAILURE_SUCCESSOR_NOT_PROVEN'
        USING ERRCODE = 'P0001';
    END IF;

    IF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_ERROR'
       AND v_terminal_scope_pending_job_id IS NULL THEN
      UPDATE public.banking_pay_workbench_session_candidate_state AS terminal_candidate_state
      SET status = 'ERROR',
          pending_job_id = NULL::uuid,
          last_error_json = COALESCE(v_terminal_scope_error_json, p_error_json),
          updated_at_utc = clock_timestamp()
      WHERE terminal_candidate_state.session_id = v_job_session_id
        AND terminal_candidate_state.candidate_id = v_job_candidate_id;
    ELSIF UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) = 'SOURCE_BUILD_PENDING'
          AND v_terminal_owner_valid THEN
      UPDATE public.banking_pay_workbench_session_candidate_state AS terminal_candidate_state
      SET status = 'PENDING',
          pending_job_id = v_terminal_scope_pending_job_id,
          last_error_json = '{}'::jsonb,
          updated_at_utc = clock_timestamp()
      WHERE terminal_candidate_state.session_id = v_job_session_id
        AND terminal_candidate_state.candidate_id = v_job_candidate_id;
    END IF;

    RETURN jsonb_build_object('ok',true,'job_id',p_job_id,'status','FAILED',
      'attempt_count',v_attempt_count,'max_attempts',v_max_attempts,
      'retry_scheduled',false,
      'result_code',CASE
        WHEN v_is_deterministic_stage_error THEN 'DETERMINISTIC_STAGE_ERROR'
        ELSE 'ATTEMPT_EXHAUSTED'
      END,
      'terminal_repair_result',v_terminal_repair_result,
      'terminal_scope_status',v_terminal_scope_status,
      'terminal_successor_job_id',v_terminal_scope_pending_job_id,
      'terminal_successor_proven',(
        UPPER(BTRIM(COALESCE(v_terminal_scope_status, ''))) <> 'SOURCE_BUILD_PENDING'
        OR v_terminal_owner_valid
      ));
  END IF;

  v_is_statement_timeout := (
    v_error_code = '57014'
    OR v_error_message LIKE '%STATEMENT TIMEOUT%'
    OR v_error_message LIKE '%CANCELING STATEMENT DUE TO STATEMENT TIMEOUT%'
  );

  v_is_no_change_loop := (
    v_error_code IN ('SNAPSHOT_ALREADY_CURRENT', 'ALREADY_CURRENT', 'NO_EFFECTIVE_CHANGE', 'PAYEE_READINESS_NO_EFFECTIVE_CHANGE', 'STALE_SESSION_VERSION', 'SESSION_DISCARDED')
    OR v_error_message LIKE '%ALREADY CURRENT%'
    OR v_error_message LIKE '%NO EFFECTIVE CHANGE%'
    OR v_error_message LIKE '%STALE SESSION VERSION%'
    OR v_error_message LIKE '%SESSION DISCARDED%'
  );

  IF v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    v_source_build_run_id_text := NULLIF(BTRIM(COALESCE(
      v_job_payload_json->>'source_build_run_id',
      v_job_payload_json#>>'{source_build,source_build_run_id}',
      v_job_payload_json#>>'{source_build,run_id}',
      v_job_payload_json#>>'{cursor,source_build_run_id}',
      v_job_payload_json#>>'{cursor_json,source_build_run_id}',
      v_job_payload_json#>>'{result_json,source_build_run_id}',
      ''
    )), '');

    IF v_source_build_run_id_text IS NULL
       OR v_source_build_run_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_source_build_run_id_text := NULL::text;
      v_invalid_source_build_without_run_id := true;
    END IF;
  END IF;

  IF v_job_type = 'SNAPSHOT_CANDIDATE_REFRESH' THEN
    IF v_job_snapshot_run_id IS NULL OR v_job_candidate_id IS NULL THEN
      v_is_obsolete := true;
      v_obsolete_reason := 'INVALID_SNAPSHOT_CONTEXT';
    ELSE
      SELECT COALESCE(acc.seq, 0)
      INTO v_live_candidate_change_seq
      FROM public.app_change_counters AS acc
      WHERE acc.entity_key = 'pay_candidate:' || v_job_candidate_id::text;

      SELECT COALESCE(snap.status, NULL), COALESCE(snap.source_change_seq, 0)
      INTO v_snapshot_state_status, v_snapshot_state_source_change_seq
      FROM public.banking_pay_snapshot_candidate_state AS snap
      WHERE snap.snapshot_run_id = v_job_snapshot_run_id
        AND snap.candidate_id = v_job_candidate_id
      LIMIT 1;

      SELECT j.id
      INTO v_other_active_job_id
      FROM public.banking_pay_workbench_jobs AS j
      WHERE j.id <> p_job_id
        AND j.job_type = 'SNAPSHOT_CANDIDATE_REFRESH'
        AND j.snapshot_run_id = v_job_snapshot_run_id
        AND j.candidate_id = v_job_candidate_id
        AND j.status IN ('QUEUED', 'RUNNING')
        AND COALESCE(
              CASE
                WHEN COALESCE(j.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
                  THEN (j.payload_json->>'source_change_seq')::bigint
                ELSE 0::bigint
              END,
              0::bigint
            ) >= GREATEST(v_job_source_change_seq, v_live_candidate_change_seq)
      ORDER BY COALESCE(
                 CASE
                   WHEN COALESCE(j.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
                     THEN (j.payload_json->>'source_change_seq')::bigint
                   ELSE 0::bigint
                 END,
                 0::bigint
               ) DESC,
               j.updated_at_utc DESC NULLS LAST,
               j.created_at_utc DESC NULLS LAST,
               j.id DESC
      LIMIT 1;

      IF v_live_candidate_change_seq > v_job_source_change_seq THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SUPERSEDED_BY_LIVE_CHANGE_SEQ';
      ELSIF v_other_active_job_id IS NOT NULL THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'MATCHING_ACTIVE_SNAPSHOT_JOB_EXISTS';
      ELSIF UPPER(COALESCE(v_snapshot_state_status, '')) = 'READY'
            AND v_snapshot_state_source_change_seq >= GREATEST(v_job_source_change_seq, v_live_candidate_change_seq) THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SNAPSHOT_ALREADY_CURRENT';
      END IF;
    END IF;
  ELSIF v_job_type = 'SESSION_CANDIDATE_RECOMPUTE' THEN
    IF v_job_session_id IS NULL OR v_job_candidate_id IS NULL THEN
      v_is_obsolete := true;
      v_obsolete_reason := 'INVALID_SESSION_CONTEXT';
    ELSE
      SELECT COALESCE(acc.seq, 0)
      INTO v_live_candidate_change_seq
      FROM public.app_change_counters AS acc
      WHERE acc.entity_key = 'pay_candidate:' || v_job_candidate_id::text;

      SELECT COALESCE(ws.status, NULL), COALESCE(ws.version, 0)
      INTO v_current_session_status, v_current_session_version
      FROM public.banking_pay_workbench_sessions AS ws
      WHERE ws.id = v_job_session_id
      LIMIT 1;

      IF v_current_session_status IS NULL OR UPPER(COALESCE(v_current_session_status, '')) <> 'OPEN' THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SESSION_NOT_OPEN';
      ELSE
        SELECT COALESCE(scs.status, NULL),
               COALESCE(scs.source_change_seq, 0),
               COALESCE(scs.session_version, 0)
        INTO v_session_candidate_status,
             v_session_candidate_source_change_seq,
             v_session_candidate_session_version
        FROM public.banking_pay_workbench_session_candidate_state AS scs
        WHERE scs.session_id = v_job_session_id
          AND scs.candidate_id = v_job_candidate_id
        LIMIT 1;

        SELECT j.id
        INTO v_other_active_job_id
        FROM public.banking_pay_workbench_jobs AS j
        WHERE j.id <> p_job_id
          AND j.job_type = 'SESSION_CANDIDATE_RECOMPUTE'
          AND j.session_id = v_job_session_id
          AND j.candidate_id = v_job_candidate_id
          AND j.status IN ('QUEUED', 'RUNNING')
          AND COALESCE(
                CASE
                  WHEN COALESCE(j.payload_json->>'session_version', '') ~ '^[0-9]+$'
                    THEN (j.payload_json->>'session_version')::bigint
                  ELSE 0::bigint
                END,
                0::bigint
              ) >= v_current_session_version
          AND COALESCE(
                CASE
                  WHEN COALESCE(j.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
                    THEN (j.payload_json->>'source_change_seq')::bigint
                  ELSE 0::bigint
                END,
                0::bigint
              ) >= GREATEST(v_job_source_change_seq, v_live_candidate_change_seq)
        ORDER BY COALESCE(
                   CASE
                     WHEN COALESCE(j.payload_json->>'session_version', '') ~ '^[0-9]+$'
                       THEN (j.payload_json->>'session_version')::bigint
                     ELSE 0::bigint
                   END,
                   0::bigint
                 ) DESC,
                 COALESCE(
                   CASE
                     WHEN COALESCE(j.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
                       THEN (j.payload_json->>'source_change_seq')::bigint
                     ELSE 0::bigint
                   END,
                   0::bigint
                 ) DESC,
                 j.updated_at_utc DESC NULLS LAST,
                 j.created_at_utc DESC NULLS LAST,
                 j.id DESC
        LIMIT 1;

        IF v_current_session_version > v_job_session_version THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'SUPERSEDED_BY_NEWER_SESSION_VERSION';
        ELSIF v_other_active_job_id IS NOT NULL THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'MATCHING_ACTIVE_SESSION_JOB_EXISTS';
        ELSIF UPPER(COALESCE(v_session_candidate_status, '')) = 'READY'
              AND v_session_candidate_session_version >= v_current_session_version
              AND v_session_candidate_source_change_seq >= GREATEST(v_job_source_change_seq, v_live_candidate_change_seq) THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'SESSION_ALREADY_CURRENT';
        END IF;
      END IF;
    END IF;
  ELSIF v_canonical_job_type IN (
    'WORKBENCH_SESSION_SCOPE_SEED',
    'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
    'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
    'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
  ) THEN
    IF v_job_session_id IS NULL THEN
      v_is_obsolete := true;
      v_obsolete_reason := 'INVALID_WORKBENCH_SESSION_CONTEXT';
    ELSE
      SELECT COALESCE(workbench_session.status, NULL),
             COALESCE(workbench_session.version, 0),
             workbench_session.source_snapshot_run_id
      INTO v_current_session_status,
           v_current_session_version,
           v_current_session_snapshot_run_id
      FROM public.banking_pay_workbench_sessions AS workbench_session
      WHERE workbench_session.id = v_job_session_id
      LIMIT 1;

      IF v_current_session_status IS NULL OR UPPER(COALESCE(v_current_session_status, '')) <> 'OPEN' THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SESSION_NOT_OPEN';
      ELSIF v_job_snapshot_run_id IS NOT NULL
            AND v_current_session_snapshot_run_id IS DISTINCT FROM v_job_snapshot_run_id THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SESSION_SNAPSHOT_MISMATCH';
      ELSIF v_job_session_version > 0
            AND v_current_session_version > v_job_session_version THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SUPERSEDED_BY_NEWER_SESSION_VERSION';
      END IF;
    END IF;

    IF v_is_obsolete IS NOT TRUE
       AND v_canonical_job_type IN (
         'WORKBENCH_CANDIDATE_SOURCE_BUILD',
         'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
         'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
         'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
       ) THEN
      IF v_job_candidate_id IS NULL THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'INVALID_WORKBENCH_CANDIDATE_CONTEXT';
      ELSE
        SELECT COALESCE(scope_row.status, NULL)
        INTO v_session_candidate_status
        FROM public.banking_pay_workbench_session_scope AS scope_row
        WHERE scope_row.session_id = v_job_session_id
          AND scope_row.candidate_id = v_job_candidate_id
        LIMIT 1;

        IF v_session_candidate_status IS NULL THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'CANDIDATE_NOT_IN_SESSION_SCOPE';
        END IF;
      END IF;
    END IF;

    IF v_is_obsolete IS NOT TRUE
       AND v_job_candidate_id IS NOT NULL
       AND v_job_source_change_seq > 0 THEN
      SELECT COALESCE(acc.seq, 0)
      INTO v_live_candidate_change_seq
      FROM public.app_change_counters AS acc
      WHERE acc.entity_key = 'pay_candidate:' || v_job_candidate_id::text;

      IF COALESCE(v_live_candidate_change_seq, 0) > COALESCE(v_job_source_change_seq, 0) THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SUPERSEDED_BY_LIVE_CHANGE_SEQ';
      END IF;
    END IF;

    IF v_is_obsolete IS NOT TRUE
       AND v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
       AND v_source_build_run_id_text IS NOT NULL
       AND v_job_session_id IS NOT NULL
       AND v_job_candidate_id IS NOT NULL
       AND v_job_source_change_seq > 0 THEN
      SELECT completed_source_build.id
      INTO v_completed_equivalent_job_id
      FROM public.banking_pay_workbench_jobs AS completed_source_build
      CROSS JOIN LATERAL (
        SELECT NULLIF(BTRIM(COALESCE(
          completed_source_build.payload_json->>'source_build_run_id',
          completed_source_build.payload_json#>>'{source_build,source_build_run_id}',
          completed_source_build.payload_json#>>'{source_build,run_id}',
          completed_source_build.payload_json#>>'{cursor,source_build_run_id}',
          completed_source_build.payload_json#>>'{cursor_json,source_build_run_id}',
          completed_source_build.payload_json#>>'{result_json,source_build_run_id}',
          ''
        )), '') AS source_build_run_id_text
      ) AS completed_run
      CROSS JOIN LATERAL (
        SELECT CASE
          WHEN COALESCE(completed_source_build.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
            THEN (completed_source_build.payload_json->>'source_change_seq')::bigint
          WHEN COALESCE(completed_source_build.payload_json#>>'{source_build,source_change_seq}', '') ~ '^[0-9]+$'
            THEN (completed_source_build.payload_json#>>'{source_build,source_change_seq}')::bigint
          ELSE 0::bigint
        END AS source_change_seq
      ) AS completed_seq
      CROSS JOIN LATERAL (
        SELECT CASE
          WHEN COALESCE(completed_source_build.payload_json->>'session_version', '') ~ '^[0-9]+$'
            THEN (completed_source_build.payload_json->>'session_version')::bigint
          WHEN COALESCE(completed_source_build.payload_json#>>'{source_build,session_version}', '') ~ '^[0-9]+$'
            THEN (completed_source_build.payload_json#>>'{source_build,session_version}')::bigint
          ELSE 0::bigint
        END AS session_version
      ) AS completed_version
      WHERE completed_source_build.id <> p_job_id
        AND completed_source_build.session_id = v_job_session_id
        AND completed_source_build.candidate_id = v_job_candidate_id
        AND completed_source_build.snapshot_run_id IS NOT DISTINCT FROM v_job_snapshot_run_id
        AND UPPER(BTRIM(COALESCE(completed_source_build.status, ''))) = 'SUCCEEDED'
        AND completed_source_build.completed_at_utc IS NOT NULL
        AND completed_source_build.failed_at_utc IS NULL
        AND completed_source_build.created_at_utc > v_job_created_at_utc
        AND CASE
              WHEN UPPER(BTRIM(COALESCE(completed_source_build.job_type, ''))) IN (
                'WORKBENCH_CANDIDATE_SOURCE_BUILD',
                'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
                'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
                'CANDIDATE_SOURCE_BUILD',
                'CANDIDATE_SOURCE_BUILD_CHUNK',
                'SOURCE_BUILD',
                'SOURCE_BUILD_PAGE'
              ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
              ELSE UPPER(BTRIM(COALESCE(completed_source_build.job_type, '')))
            END = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND completed_run.source_build_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND completed_run.source_build_run_id_text IS DISTINCT FROM v_source_build_run_id_text
        AND completed_seq.source_change_seq = v_job_source_change_seq
        AND (
          v_job_session_version <= 0
          OR completed_version.session_version = v_job_session_version
        )
        AND EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_candidate_source_lines AS completed_source_row
          WHERE completed_source_row.session_id = v_job_session_id
            AND completed_source_row.candidate_id = v_job_candidate_id
            AND completed_source_row.source_build_run_id = completed_run.source_build_run_id_text::uuid
            AND completed_source_row.source_change_seq = v_job_source_change_seq
            AND (
              v_job_session_version <= 0
              OR completed_source_row.session_version = v_job_session_version
            )
            AND UPPER(BTRIM(COALESCE(completed_source_row.status, ''))) = 'CURRENT'
        )
      ORDER BY completed_source_build.completed_at_utc DESC,
               completed_source_build.created_at_utc DESC,
               completed_source_build.id DESC
      LIMIT 1;

      IF v_completed_equivalent_job_id IS NOT NULL THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SUPERSEDED_BY_NEWER_SUCCESSFUL_SOURCE_BUILD';
      END IF;
    END IF;
  ELSIF v_job_type = 'PAYEE_READINESS_ENSURE' THEN
    IF v_job_candidate_id IS NULL THEN
      v_is_obsolete := true;
      v_obsolete_reason := 'INVALID_READINESS_CONTEXT';
    ELSE
      SELECT COALESCE(acc.seq, 0)
      INTO v_live_candidate_change_seq
      FROM public.app_change_counters AS acc
      WHERE acc.entity_key = 'pay_candidate:' || v_job_candidate_id::text;

      IF v_job_session_id IS NOT NULL THEN
        SELECT COALESCE(ws.status, NULL), COALESCE(ws.version, 0)
        INTO v_current_session_status, v_current_session_version
        FROM public.banking_pay_workbench_sessions AS ws
        WHERE ws.id = v_job_session_id
        LIMIT 1;

        IF v_current_session_status IS NULL OR UPPER(COALESCE(v_current_session_status, '')) <> 'OPEN' THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'SESSION_NOT_OPEN';
        ELSIF v_current_session_version > v_job_session_version THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'SUPERSEDED_BY_NEWER_SESSION_VERSION';
        END IF;
      END IF;

      IF NOT v_is_obsolete AND v_live_candidate_change_seq > v_job_source_change_seq THEN
        v_is_obsolete := true;
        v_obsolete_reason := 'SUPERSEDED_BY_LIVE_CHANGE_SEQ';
      END IF;

      IF NOT v_is_obsolete THEN
        SELECT j.id
        INTO v_completed_equivalent_job_id
        FROM public.banking_pay_workbench_jobs AS j
        WHERE j.id <> p_job_id
          AND j.job_type = 'PAYEE_READINESS_ENSURE'
          AND j.dedupe_key = v_job_dedupe_key
          AND j.completed_at_utc IS NOT NULL
          AND j.failed_at_utc IS NULL
        ORDER BY j.completed_at_utc DESC, j.id DESC
        LIMIT 1;

        IF v_completed_equivalent_job_id IS NOT NULL THEN
          v_is_obsolete := true;
          v_obsolete_reason := 'COMPLETED_EQUIVALENT_EXISTS';
        ELSE
          SELECT j.id
          INTO v_other_active_job_id
          FROM public.banking_pay_workbench_jobs AS j
          WHERE j.id <> p_job_id
            AND j.job_type = 'PAYEE_READINESS_ENSURE'
            AND j.dedupe_key = v_job_dedupe_key
            AND j.status IN ('QUEUED', 'RUNNING')
          ORDER BY j.updated_at_utc DESC NULLS LAST, j.created_at_utc DESC NULLS LAST, j.id DESC
          LIMIT 1;

          IF v_other_active_job_id IS NOT NULL THEN
            v_is_obsolete := true;
            v_obsolete_reason := 'MATCHING_ACTIVE_READINESS_JOB_EXISTS';
          END IF;
        END IF;
      END IF;
    END IF;
  END IF;

  IF v_is_statement_timeout THEN
    v_retry_after_seconds := GREATEST(
      300,
      LEAST(
        COALESCE(p_retry_after_seconds, GREATEST(COALESCE(v_attempt_count, 1), 1) * 300),
        14400
      )
    );
  ELSE
    v_retry_after_seconds := GREATEST(
      5,
      LEAST(
        COALESCE(p_retry_after_seconds, GREATEST(30, LEAST(COALESCE(v_attempt_count, 1) * 60, 3600))),
        86400
      )
    );
  END IF;

  IF v_is_obsolete
     OR v_is_no_change_loop
     OR (
       v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
       AND v_invalid_source_build_without_run_id IS TRUE
     )
     OR COALESCE(v_attempt_count, 0) >= COALESCE(v_max_attempts, 8) THEN
    v_new_status := CASE
      WHEN v_canonical_job_type IN (
        'WORKBENCH_SESSION_SCOPE_SEED',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
        'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
        'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
      ) THEN 'FAILED'
      ELSE 'DEAD'
    END;
    v_next_run_at_utc := NULL;
  ELSE
    v_new_status := 'QUEUED';
    v_next_run_at_utc := v_now + make_interval(secs => v_retry_after_seconds);
  END IF;

  UPDATE public.banking_pay_workbench_jobs AS j
  SET status = v_new_status,
      run_at_utc = COALESCE(v_next_run_at_utc, j.run_at_utc),
      updated_at_utc = v_now,
      started_at_utc = CASE WHEN v_new_status = 'QUEUED' THEN NULL ELSE j.started_at_utc END,
      completed_at_utc = NULL,
      failed_at_utc = CASE WHEN v_new_status = 'QUEUED' THEN NULL ELSE v_now END,
      last_error_json = p_error_json,
      payload_json = jsonb_strip_nulls(
        COALESCE(j.payload_json, '{}'::jsonb)
        || jsonb_build_object('last_failure_json', p_error_json)
        || CASE
          WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
               AND v_invalid_source_build_without_run_id IS TRUE THEN
            jsonb_build_object(
              'invalid_source_build_without_run_id_failed_closed', true,
              'invalid_source_build_without_run_id_non_blocking', true,
              'non_blocking_terminal_failure', true,
              'non_blocking_terminal_failure_reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
              'non_blocking_terminal_failure_at_utc', v_now::text,
              'source_build_run_id_required', true,
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            )
          WHEN v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
               AND v_obsolete_reason = 'SUPERSEDED_BY_NEWER_SUCCESSFUL_SOURCE_BUILD' THEN
            jsonb_build_object(
              'superseded_at_utc', v_now::text,
              'superseded_reason', v_obsolete_reason,
              'superseded_by_job_id', v_completed_equivalent_job_id::text,
              'non_blocking_terminal_failure', true,
              'non_blocking_terminal_failure_reason', v_obsolete_reason,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
          ELSE '{}'::jsonb
        END
      )
  WHERE j.id = p_job_id
  RETURNING j.failed_at_utc
  INTO v_failed_at_utc;

  IF v_new_status = 'FAILED'
     AND v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
     AND v_is_obsolete IS NOT TRUE
     AND v_job_session_id IS NOT NULL
     AND v_job_candidate_id IS NOT NULL THEN
    IF v_source_build_run_id_text IS NULL THEN
      v_failed_source_row_count := 0;

      PERFORM public._audit_insert(
        'banking_pay_workbench_candidate_source_lines',
        v_job_candidate_id::text,
        'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_DID_NOT_MARK_ROWS_ERROR',
        jsonb_build_object(
          'session_id', v_job_session_id::text,
          'candidate_id', v_job_candidate_id::text,
          'bad_job_id', p_job_id::text,
          'source_build_run_id', NULL::text,
          'source_change_seq', CASE WHEN v_job_source_change_seq = 0 THEN NULL ELSE v_job_source_change_seq END,
          'old_status', 'CURRENT_OR_DIRTY',
          'reason', 'SOURCE_BUILD_JOB_MISSING_SOURCE_BUILD_RUN_ID',
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        ),
        jsonb_build_object(
          'session_id', v_job_session_id::text,
          'candidate_id', v_job_candidate_id::text,
          'bad_job_id', p_job_id::text,
          'source_build_run_id', NULL::text,
          'source_change_seq', CASE WHEN v_job_source_change_seq = 0 THEN NULL ELSE v_job_source_change_seq END,
          'affected_row_count', 0,
          'source_rows_marked_error_count', 0,
          'new_status', 'UNCHANGED',
          'reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        ),
        'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_DID_NOT_MARK_ROWS_ERROR',
        NULL
      );

      PERFORM public._audit_insert(
        'banking_pay_workbench_session_scope',
        v_job_candidate_id::text,
        'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_IGNORED',
        jsonb_build_object(
          'session_id', v_job_session_id::text,
          'candidate_id', v_job_candidate_id::text,
          'bad_job_id', p_job_id::text,
          'source_build_run_id', NULL::text,
          'source_change_seq', CASE WHEN v_job_source_change_seq = 0 THEN NULL ELSE v_job_source_change_seq END,
          'reason', 'SOURCE_BUILD_JOB_MISSING_SOURCE_BUILD_RUN_ID',
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        ),
        jsonb_build_object(
          'session_id', v_job_session_id::text,
          'candidate_id', v_job_candidate_id::text,
          'bad_job_id', p_job_id::text,
          'source_build_run_id', NULL::text,
          'source_change_seq', CASE WHEN v_job_source_change_seq = 0 THEN NULL ELSE v_job_source_change_seq END,
          'scope_status_changed', false,
          'session_progress_changed', false,
          'reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        ),
        'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_IGNORED',
        NULL
      );
    ELSE
      IF to_regclass('public.banking_pay_workbench_candidate_source_lines') IS NOT NULL THEN
        WITH failed_source_rows AS (
          UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line
          SET status = 'ERROR',
              source_row_json = COALESCE(source_line.source_row_json, '{}'::jsonb)
                || jsonb_build_object(
                  'source_build_error', jsonb_build_object(
                    'code', 'WORKBENCH_SOURCE_BUILD_JOB_FAILED',
                    'job_id', p_job_id::text,
                    'canonical_job_type', v_canonical_job_type,
                    'source_build_run_id', v_source_build_run_id_text,
                    'failed_at_utc', v_now::text,
                    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                  )
                ),
              updated_at_utc = v_now
          WHERE source_line.session_id = v_job_session_id
            AND source_line.candidate_id = v_job_candidate_id
            AND source_line.status IN ('CURRENT', 'DIRTY')
            AND source_line.source_build_run_id::text = v_source_build_run_id_text
          RETURNING source_line.id
        )
        SELECT COUNT(*)::integer
        INTO v_failed_source_row_count
        FROM failed_source_rows;
      END IF;

      UPDATE public.banking_pay_workbench_session_scope AS source_scope_row
      SET status = 'SOURCE_BUILD_ERROR',
          pending_job_id = NULL::uuid,
          dirty = true,
          error_json = jsonb_build_object(
            'code', 'WORKBENCH_SOURCE_BUILD_JOB_FAILED',
            'message', 'Candidate source build could not be completed after all retry attempts.',
            'job_id', p_job_id::text,
            'canonical_job_type', v_canonical_job_type,
            'source_build_run_id', v_source_build_run_id_text,
            'source_rows_marked_error_count', COALESCE(v_failed_source_row_count, 0),
            'attempt_count', COALESCE(v_attempt_count, 0),
            'max_attempts', COALESCE(v_max_attempts, 8),
            'automatic_recovery_scheduled', false,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          ),
          updated_at_utc = v_now
      WHERE source_scope_row.session_id = v_job_session_id
        AND source_scope_row.candidate_id = v_job_candidate_id
        AND (
          source_scope_row.pending_job_id = p_job_id
          OR source_scope_row.pending_job_id IS NULL
        );

      GET DIAGNOSTICS v_terminal_scope_updated_count = ROW_COUNT;

      UPDATE public.banking_pay_workbench_sessions AS source_session_row
      SET progress_state = 'ERROR',
          progress_json = COALESCE(source_session_row.progress_json, '{}'::jsonb) || jsonb_build_object(
            'last_source_build_failure_at_utc', v_now::text,
            'last_source_build_failure_job_id', p_job_id::text,
            'last_source_build_failure_code', COALESCE(NULLIF(BTRIM(p_error_json->>'code'), ''), 'WORKBENCH_SOURCE_BUILD_JOB_FAILED'),
            'last_source_build_failure_source_build_run_id', v_source_build_run_id_text,
            'last_source_build_source_rows_marked_error_count', COALESCE(v_failed_source_row_count, 0)
          ),
          progress_counter_version = COALESCE(source_session_row.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE source_session_row.id = v_job_session_id
        AND v_terminal_scope_updated_count > 0;
    END IF;
  END IF;

  IF v_new_status = 'FAILED'
     AND v_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
     AND v_is_obsolete IS NOT TRUE
     AND v_job_session_id IS NOT NULL
     AND v_job_candidate_id IS NOT NULL THEN
    WITH failed_lines AS (
      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
      SET status = 'ERROR',
          error_json = jsonb_build_object(
            'code', 'WORKBENCH_LINE_WORK_PROCESS_JOB_FAILED',
            'message', 'Candidate line work was marked failed because its process job failed terminally.',
            'job_id', p_job_id::text,
            'session_id', v_job_session_id::text,
            'candidate_id', v_job_candidate_id::text,
            'attempt_count', COALESCE(v_attempt_count, 0),
            'max_attempts', COALESCE(v_max_attempts, 8),
            'automatic_recovery_scheduled', false,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          ),
          updated_at_utc = v_now
      WHERE line_work.session_id = v_job_session_id
        AND line_work.candidate_id = v_job_candidate_id
        AND UPPER(BTRIM(COALESCE(line_work.status, ''))) = 'PENDING'
        AND EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_session_scope AS owned_scope
          WHERE owned_scope.session_id = v_job_session_id
            AND owned_scope.candidate_id = v_job_candidate_id
            AND (
              owned_scope.pending_job_id = p_job_id
              OR owned_scope.pending_job_id IS NULL
            )
        )
      RETURNING line_work.id
    )
    SELECT COUNT(*)::integer
    INTO v_failed_line_work_count
    FROM failed_lines;

    IF COALESCE(v_failed_line_work_count, 0) > 0 THEN
      UPDATE public.banking_pay_workbench_session_scope AS scope_row
      SET status = 'ERROR',
          pending_job_id = NULL::uuid,
          dirty = true,
          error_json = jsonb_build_object(
            'code', 'WORKBENCH_LINE_WORK_PROCESS_JOB_FAILED',
            'message', 'Candidate line work process job failed terminally.',
            'job_id', p_job_id::text,
            'line_work_failed_count', v_failed_line_work_count,
            'attempt_count', COALESCE(v_attempt_count, 0),
            'max_attempts', COALESCE(v_max_attempts, 8),
            'automatic_recovery_scheduled', false,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          ),
          updated_at_utc = v_now
      WHERE scope_row.session_id = v_job_session_id
        AND scope_row.candidate_id = v_job_candidate_id
        AND (
          scope_row.pending_job_id = p_job_id
          OR scope_row.pending_job_id IS NULL
        );

      GET DIAGNOSTICS v_terminal_scope_updated_count = ROW_COUNT;

      UPDATE public.banking_pay_workbench_sessions AS session_row
      SET line_units_pending = GREATEST(COALESCE(session_row.line_units_pending, 0) - COALESCE(v_failed_line_work_count, 0), 0),
          line_units_failed = GREATEST(COALESCE(session_row.line_units_failed, 0) + COALESCE(v_failed_line_work_count, 0), 0),
          progress_state = 'ERROR',
          progress_json = COALESCE(session_row.progress_json, '{}'::jsonb) || jsonb_build_object(
            'last_line_process_failure_at_utc', v_now::text,
            'last_line_process_failure_job_id', p_job_id::text,
            'last_line_process_failure_code', COALESCE(NULLIF(BTRIM(p_error_json->>'code'), ''), 'WORKBENCH_LINE_WORK_PROCESS_JOB_FAILED'),
            'last_line_process_failure_count', v_failed_line_work_count
          ),
          progress_counter_version = COALESCE(session_row.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE session_row.id = v_job_session_id
        AND v_terminal_scope_updated_count > 0;
    END IF;
  END IF;

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    p_job_id::text,
    CASE WHEN v_new_status = 'QUEUED' THEN 'REQUEUED' WHEN v_new_status = 'FAILED' THEN 'FAILED' ELSE 'DEAD' END,
    NULL,
    jsonb_build_object(
      'id', p_job_id::text,
      'job_type', v_job_type,
      'canonical_job_type', v_canonical_job_type,
      'status', v_new_status,
      'attempt_count', v_attempt_count,
      'max_attempts', v_max_attempts,
      'run_at_utc', v_next_run_at_utc,
      'failed_at_utc', v_failed_at_utc,
      'last_error_json', p_error_json,
      'retry_after_seconds', CASE WHEN v_new_status = 'QUEUED' THEN v_retry_after_seconds ELSE NULL END,
      'obsolete', v_is_obsolete,
      'obsolete_reason', v_obsolete_reason,
      'statement_timeout', v_is_statement_timeout,
      'deterministic_no_change_loop', v_is_no_change_loop,
      'source_build_run_id', v_source_build_run_id_text,
      'invalid_source_build_without_run_id_failed_closed', COALESCE(v_invalid_source_build_without_run_id, false),
      'invalid_source_build_without_run_id_non_blocking', COALESCE(v_invalid_source_build_without_run_id, false) AND v_new_status <> 'QUEUED',
      'superseded_by_job_id', CASE
        WHEN v_obsolete_reason = 'SUPERSEDED_BY_NEWER_SUCCESSFUL_SOURCE_BUILD'
          THEN v_completed_equivalent_job_id::text
        ELSE NULL::text
      END,
      'non_blocking_terminal_failure', (
        COALESCE(v_invalid_source_build_without_run_id, false)
        OR v_obsolete_reason = 'SUPERSEDED_BY_NEWER_SUCCESSFUL_SOURCE_BUILD'
      ) AND v_new_status <> 'QUEUED',
      'line_work_failed_count', COALESCE(v_failed_line_work_count, 0),
      'source_rows_marked_error_count', COALESCE(v_failed_source_row_count, 0)
    ),
    CASE
      WHEN v_new_status = 'QUEUED' THEN 'WORKBENCH_JOB_REQUEUED'
      WHEN v_new_status = 'FAILED'
           AND v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
           AND v_invalid_source_build_without_run_id IS TRUE THEN 'WORKBENCH_INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_FAILED_CLOSED'
      WHEN v_new_status = 'FAILED' THEN 'WORKBENCH_JOB_FAILED'
      ELSE 'WORKBENCH_JOB_DEAD'
    END,
    NULL
  );

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', p_job_id::text,
    'status', v_new_status,
    'canonical_job_type', v_canonical_job_type,
    'attempt_count', v_attempt_count,
    'max_attempts', v_max_attempts,
    'retry_after_seconds', CASE WHEN v_new_status = 'QUEUED' THEN v_retry_after_seconds ELSE NULL END,
    'next_run_at_utc', v_next_run_at_utc,
    'failed_at_utc', v_failed_at_utc,
    'error_json', p_error_json,
    'obsolete', v_is_obsolete,
    'obsolete_reason', v_obsolete_reason,
    'statement_timeout', v_is_statement_timeout,
    'deterministic_no_change_loop', v_is_no_change_loop,
    'source_build_run_id', v_source_build_run_id_text,
    'invalid_source_build_without_run_id_failed_closed', COALESCE(v_invalid_source_build_without_run_id, false),
    'invalid_source_build_without_run_id_non_blocking', COALESCE(v_invalid_source_build_without_run_id, false) AND v_new_status <> 'QUEUED',
    'superseded_by_job_id', CASE
      WHEN v_obsolete_reason = 'SUPERSEDED_BY_NEWER_SUCCESSFUL_SOURCE_BUILD'
        THEN v_completed_equivalent_job_id::text
      ELSE NULL::text
    END,
    'non_blocking_terminal_failure', (
      COALESCE(v_invalid_source_build_without_run_id, false)
      OR v_obsolete_reason = 'SUPERSEDED_BY_NEWER_SUCCESSFUL_SOURCE_BUILD'
    ) AND v_new_status <> 'QUEUED',
    'line_work_failed_count', COALESCE(v_failed_line_work_count, 0),
    'source_rows_marked_error_count', COALESCE(v_failed_source_row_count, 0)
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_fail_job(p_job_id uuid, p_error_json jsonb, p_retry_after_seconds integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_fail_job(p_job_id uuid, p_error_json jsonb, p_retry_after_seconds integer) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_fail_job(p_job_id uuid, p_error_json jsonb, p_retry_after_seconds integer) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_fail_job(p_job_id uuid, p_error_json jsonb, p_retry_after_seconds integer) TO service_role;
