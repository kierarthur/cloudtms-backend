-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline, extended only to initialise typed source-build jobs.

CREATE OR REPLACE FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(
  p_source_session_id uuid,
  p_target_session_id uuid,
  p_reason text DEFAULT 'REPLACED_SESSION_QUEUE_REPLAY'::text,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz := now();
  v_reason text := COALESCE(NULLIF(BTRIM(p_reason), ''), 'REPLACED_SESSION_QUEUE_REPLAY');
  v_options_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_options_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_source_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_target_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_candidate_scope_missing_count integer := 0;
  v_source_active_job_count integer := 0;
  v_source_queued_job_count integer := 0;
  v_source_running_job_count integer := 0;
  v_replayed_job_count integer := 0;
  v_source_queued_terminalised_count integer := 0;
  v_replay_job_ids jsonb := '[]'::jsonb;
  v_source_job_ids jsonb := '[]'::jsonb;
  v_terminalise_source_queued boolean := true;
BEGIN
  IF p_source_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code','PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_REQUIRED')::text;
  END IF;

  IF p_target_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code','PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_REQUIRED')::text;
  END IF;

  SELECT source_session.*
  INTO v_source_session
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id = p_source_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code','PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_NOT_FOUND',
              'source_session_id', p_source_session_id::text
            )::text;
  END IF;

  SELECT target_session.*
  INTO v_target_session
  FROM public.banking_pay_workbench_sessions AS target_session
  WHERE target_session.id = p_target_session_id
    AND UPPER(BTRIM(COALESCE(target_session.status, ''))) = 'OPEN'
    AND target_session.discarded_at_utc IS NULL;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code','PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_NOT_OPEN',
              'target_session_id', p_target_session_id::text
            )::text;
  END IF;

  IF p_source_session_id = p_target_session_id THEN
    RETURN jsonb_build_object(
      'ok', true,
      'source_session_id', p_source_session_id::text,
      'target_session_id', p_target_session_id::text,
      'skipped', true,
      'skip_reason', 'SOURCE_AND_TARGET_IDENTICAL',
      'replayed_job_count', 0,
      'source_queued_terminalised_count', 0,
      'source_running_job_count', 0
    );
  END IF;

  v_terminalise_source_queued := LOWER(BTRIM(COALESCE(v_options_json->>'terminalise_source_queued', 'true'))) NOT IN ('false','f','0','no','n','off');

  DROP TABLE IF EXISTS pg_temp._bpay_replaced_session_queue_replay_jobs;
  CREATE TEMP TABLE _bpay_replaced_session_queue_replay_jobs ON COMMIT DROP AS
  SELECT
    source_job.id AS source_job_id,
    source_job.job_type,
    UPPER(BTRIM(COALESCE(source_job.status, ''))) AS source_status,
    source_job.priority,
    source_job.run_at_utc,
    source_job.attempt_count,
    source_job.max_attempts,
    source_job.snapshot_run_id,
    source_job.session_id,
    source_job.candidate_id,
    COALESCE(source_job.payload_json, '{}'::jsonb) AS payload_json,
    (
      'REPLAY_REPLACED_SESSION:'
      || p_target_session_id::text
      || ':source_job:'
      || source_job.id::text
    ) AS replay_dedupe_key
  FROM public.banking_pay_workbench_jobs AS source_job
  WHERE source_job.session_id = p_source_session_id
    AND UPPER(BTRIM(COALESCE(source_job.status, ''))) IN ('QUEUED','RUNNING')
    AND UPPER(BTRIM(COALESCE(source_job.job_type, ''))) IN (
      'WORKBENCH_SESSION_SCOPE_SEED',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
      'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
      'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
      'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
      'CANDIDATE_SOURCE_BUILD',
      'CANDIDATE_SOURCE_BUILD_CHUNK',
      'SOURCE_BUILD',
      'SOURCE_BUILD_PAGE'
    );

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE source_status = 'QUEUED')::integer,
         COUNT(*) FILTER (WHERE source_status = 'RUNNING')::integer,
         COALESCE(jsonb_agg(source_job_id::text ORDER BY source_job_id::text), '[]'::jsonb)
  INTO v_source_active_job_count,
       v_source_queued_job_count,
       v_source_running_job_count,
       v_source_job_ids
  FROM pg_temp._bpay_replaced_session_queue_replay_jobs;

  IF COALESCE(v_source_active_job_count, 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'source_session_id', p_source_session_id::text,
      'target_session_id', p_target_session_id::text,
      'reason', v_reason,
      'source_active_job_count', 0,
      'source_queued_job_count', 0,
      'source_running_job_count', 0,
      'replayed_job_count', 0,
      'source_queued_terminalised_count', 0,
      'replay_job_ids', '[]'::jsonb,
      'source_job_ids', '[]'::jsonb
    );
  END IF;

  SELECT COUNT(*)::integer
  INTO v_candidate_scope_missing_count
  FROM pg_temp._bpay_replaced_session_queue_replay_jobs AS replay_job
  WHERE replay_job.candidate_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_session_scope AS target_scope
      WHERE target_scope.session_id = p_target_session_id
        AND target_scope.candidate_id = replay_job.candidate_id
    );

  WITH replayed_jobs AS (
    INSERT INTO public.banking_pay_workbench_jobs (
      job_type,
      status,
      priority,
      run_at_utc,
      attempt_count,
      max_attempts,
      dedupe_key,
      snapshot_run_id,
      session_id,
      candidate_id,
      payload_json,
      economic_build_id,
      private_stage,
      private_cursor_kind,
      private_cursor_json,
      private_stage_version,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      CASE
        WHEN UPPER(BTRIM(replay_job.job_type)) IN ('CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE')
          THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        ELSE replay_job.job_type
      END,
      'QUEUED',
      replay_job.priority,
      LEAST(COALESCE(replay_job.run_at_utc, v_now), v_now),
      0,
      GREATEST(COALESCE(replay_job.max_attempts, 8), 1),
      replay_job.replay_dedupe_key,
      v_target_session.source_snapshot_run_id,
      p_target_session_id,
      replay_job.candidate_id,
      jsonb_strip_nulls(
        (replay_job.payload_json - ARRAY[
          'worker_id',
          'worker_claimed_at_utc',
          'worker_lease_seconds',
          'worker_lease_expires_at_utc',
          'worker_function',
          'result_json',
          'completion_json',
          'session_id',
          'sessionId',
          'source_session_id',
          'sourceSessionId',
          'target_session_id',
          'targetSessionId',
          'snapshot_run_id',
          'source_snapshot_run_id',
          'session_version',
          'sessionVersion'
        ]::text[])
        || jsonb_build_object(
          'session_id', p_target_session_id::text,
          'target_session_id', p_target_session_id::text,
          'snapshot_run_id', CASE WHEN v_target_session.source_snapshot_run_id IS NULL THEN NULL ELSE v_target_session.source_snapshot_run_id::text END,
          'source_snapshot_run_id', CASE WHEN v_target_session.source_snapshot_run_id IS NULL THEN NULL ELSE v_target_session.source_snapshot_run_id::text END,
          'session_version', v_target_session.version,
          'replayed_from_replaced_session', true,
          'replayed_from_session_id', p_source_session_id::text,
          'replayed_from_job_id', replay_job.source_job_id::text,
          'replayed_from_job_status', replay_job.source_status,
          'replay_reason', v_reason,
          'replayed_at_utc', v_now::text
        )
      ),
      NULL::uuid,
      CASE
        WHEN UPPER(BTRIM(replay_job.job_type)) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE'
        ) THEN 'BUILD_INITIALISE'
        ELSE NULL::text
      END,
      CASE
        WHEN UPPER(BTRIM(replay_job.job_type)) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE'
        ) THEN 'BUILD_INITIALISE'
        ELSE NULL::text
      END,
      '{}'::jsonb,
      CASE
        WHEN UPPER(BTRIM(replay_job.job_type)) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE'
        ) THEN 1
        ELSE NULL::integer
      END,
      v_now,
      v_now
    FROM pg_temp._bpay_replaced_session_queue_replay_jobs AS replay_job
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED','RUNNING')
    DO UPDATE SET
      priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
      run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
      payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb)
        || EXCLUDED.payload_json
        || jsonb_build_object(
          'queue_replay_reused', true,
          'queue_replay_reused_at_utc', v_now::text
        ),
      updated_at_utc = v_now
    RETURNING id
  )
  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(replayed_jobs.id::text ORDER BY replayed_jobs.id::text), '[]'::jsonb)
  INTO v_replayed_job_count,
       v_replay_job_ids
  FROM replayed_jobs;

  IF COALESCE(v_terminalise_source_queued, true) THEN
    UPDATE public.banking_pay_workbench_jobs AS old_job
    SET status = 'DEAD',
        failed_at_utc = COALESCE(old_job.failed_at_utc, v_now),
        updated_at_utc = v_now,
        last_error_json = jsonb_strip_nulls(
          jsonb_build_object(
            'code', 'REPLACED_SESSION_QUEUE_REPLAYED',
            'message', 'Queued work was replayed against the replacement Banking Pay workbench session.',
            'source_session_id', p_source_session_id::text,
            'replacement_session_id', p_target_session_id::text,
            'replay_reason', v_reason,
            'replayed_at_utc', v_now::text
          )
        ),
        payload_json = COALESCE(old_job.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'terminalised_as_replaced_session_queue_replayed', true,
            'replacement_session_id', p_target_session_id::text,
            'terminalised_at_utc', v_now::text
          )
    FROM pg_temp._bpay_replaced_session_queue_replay_jobs AS replay_job
    WHERE old_job.id = replay_job.source_job_id
      AND old_job.status = 'QUEUED';

    GET DIAGNOSTICS v_source_queued_terminalised_count = ROW_COUNT;
  END IF;

  PERFORM public._audit_insert(
    'banking_pay_workbench_session',
    p_source_session_id::text,
    'WORKBENCH_REPLACED_SESSION_QUEUE_REPLAYED',
    jsonb_build_object(
      'source_session_id', p_source_session_id::text,
      'target_session_id', p_target_session_id::text,
      'source_active_job_count', COALESCE(v_source_active_job_count, 0),
      'source_queued_job_count', COALESCE(v_source_queued_job_count, 0),
      'source_running_job_count', COALESCE(v_source_running_job_count, 0),
      'source_job_ids', COALESCE(v_source_job_ids, '[]'::jsonb)
    ),
    jsonb_build_object(
      'replayed_job_count', COALESCE(v_replayed_job_count, 0),
      'source_queued_terminalised_count', COALESCE(v_source_queued_terminalised_count, 0),
      'candidate_scope_missing_count', COALESCE(v_candidate_scope_missing_count, 0),
      'replay_job_ids', COALESCE(v_replay_job_ids, '[]'::jsonb)
    ),
    v_reason,
    NULL::uuid
  );

  RETURN jsonb_build_object(
    'ok', true,
    'source_session_id', p_source_session_id::text,
    'target_session_id', p_target_session_id::text,
    'reason', v_reason,
    'source_active_job_count', COALESCE(v_source_active_job_count, 0),
    'source_queued_job_count', COALESCE(v_source_queued_job_count, 0),
    'source_running_job_count', COALESCE(v_source_running_job_count, 0),
    'replayed_job_count', COALESCE(v_replayed_job_count, 0),
    'source_queued_terminalised_count', COALESCE(v_source_queued_terminalised_count, 0),
    'candidate_scope_missing_count', COALESCE(v_candidate_scope_missing_count, 0),
    'source_job_ids', COALESCE(v_source_job_ids, '[]'::jsonb),
    'replay_job_ids', COALESCE(v_replay_job_ids, '[]'::jsonb),
    'running_source_jobs_left_to_complete_stale', COALESCE(v_source_running_job_count, 0)
  );
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid, uuid, text, jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid, uuid, text, jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid, uuid, text, jsonb) TO postgres, service_role;
