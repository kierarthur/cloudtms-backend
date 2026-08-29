-- SUPERSEDED — INCORRECT POST-DELTA HASH MANIFEST. DO NOT USE.
-- Retained as historical evidence. Use the dated replacement rollback in this folder.
-- TEST-only rollback for the Banking Pay Source-Build Owner Recovery Completion Plan.
-- Restores only the three pre-delta function definitions that were installed
-- immediately before this completion delta. It does not remove the helper and
-- does not restore the original vulnerable pre-recovery implementation.

BEGIN;

SELECT pg_advisory_xact_lock(
  hashtextextended('cloudtms:banking-pay-owner-recovery-completion', 0)
);

DO $guard$
DECLARE
  v_actual text;
BEGIN
  SELECT md5(pg_get_functiondef(
    'public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamp with time zone,text)'::regprocedure
  )) INTO v_actual;
  IF v_actual IS DISTINCT FROM '78d2a4ac9dd7b8309ed5c77112d981f0' THEN
    RAISE EXCEPTION 'POST_DELTA_HELPER_HASH_MISMATCH' USING ERRCODE = 'P0001';
  END IF;

  SELECT md5(pg_get_functiondef(
    'public.pay_workbench_session_get_progress_light(uuid)'::regprocedure
  )) INTO v_actual;
  IF v_actual IS DISTINCT FROM '497a7be67673cae16b2d95e47290fd3c' THEN
    RAISE EXCEPTION 'POST_DELTA_PROGRESS_LIGHT_HASH_MISMATCH' USING ERRCODE = 'P0001';
  END IF;

  SELECT md5(pg_get_functiondef(
    'public.pay_workbench_session_recompute_progress_counters(uuid,boolean,text,boolean)'::regprocedure
  )) INTO v_actual;
  IF v_actual IS DISTINCT FROM 'acc358aa65a14b4466cc47919d7132e5' THEN
    RAISE EXCEPTION 'POST_DELTA_RECOMPUTE_HASH_MISMATCH' USING ERRCODE = 'P0001';
  END IF;
END
$guard$;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_progress_light(p_session_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_replacement_session_id_text text := NULL::text;
  v_session_obsolete boolean := false;
  v_replacement_required boolean := false;
  v_scope_cursor_remaining boolean := false;
  v_active_jobs_json jsonb := '[]'::jsonb;
  v_active_job_count integer := 0;
  v_active_queued_count integer := 0;
  v_active_running_count integer := 0;
  v_unresolved_failed_count integer := 0;
  v_unresolved_dead_count integer := 0;
  v_terminal_failure boolean := false;
  v_recovery_required_count integer := 0;
  v_recovery_scheduled_count integer := 0;
  v_pending_owner_failures_json jsonb := '[]'::jsonb;
  v_delta_refresh_pending_count integer := 0;
  v_fallback_legacy_pending_count integer := 0;
  v_patching_preview_rows boolean := false;
  v_clone_rebase_pending boolean := false;
  v_work_queued boolean := false;
  v_still_running boolean := false;
  v_progress_state text := 'REFRESHING_CANDIDATES';
  v_phase text := 'REFRESHING_CANDIDATES';
  v_status_text text := 'Preparing payment preview.';
  v_next_recommended_action text := 'WAIT_FOR_WORKER';
  v_ready boolean := false;
  v_ready_empty boolean := false;
  v_ready_for_draft boolean := false;
  v_rows_available boolean := false;
  v_selected_rows_available boolean := false;
  v_scope_total_count integer := 0;
  v_scope_seeded_count integer := 0;
  v_scope_ready_count integer := 0;
  v_scope_pending_count integer := 0;
  v_scope_failed_count integer := 0;
  v_line_units_total integer := 0;
  v_line_units_total_display integer := 0;
  v_line_units_complete integer := 0;
  v_line_units_ready integer := 0;
  v_line_units_ready_not_materialised integer := 0;
  v_line_units_pending integer := 0;
  v_line_units_failed integer := 0;
  v_preview_row_count integer := 0;
  v_selected_row_count integer := 0;
  v_section_counts_json jsonb := '{}'::jsonb;
  v_candidate_sample_rows_json jsonb := '[]'::jsonb;
  v_session_blocker_codes jsonb := '[]'::jsonb;
  v_draft_blocker_codes jsonb := '[]'::jsonb;
  v_response_progress_json jsonb := '{}'::jsonb;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'WORKBENCH_SESSION_NOT_FOUND',
      'code', 'WORKBENCH_SESSION_NOT_FOUND',
      'session_id', p_session_id::text,
      'session_obsolete', false,
      'obsolete', false,
      'replacement_required', true,
      'rebase_required', true,
      'requires_new_session', true,
      'replacement_session_id', NULL::text,
      'replacement_available', false,
      'ready', false,
      'ready_flag', false,
      'session_ready', false,
      'ready_for_draft', false,
      'can_create_draft', false,
      'ready_empty', false,
      'rows_available', false,
      'selected_rows_available', false,
      'has_materialised_preview_rows', false,
      'still_running', false,
      'work_queued', false,
      'scope_seed_complete', false,
      'scope_cursor_remaining', false,
      'candidate_counts', jsonb_build_object('pending', 0, 'processing', 0, 'materialisation_pending', 0, 'dirty', 0, 'failed', 0, 'unknown', 0, 'unseeded', 0),
      'line_counts', jsonb_build_object('pending', 0, 'ready_not_materialised', 0, 'failed', 0, 'unknown', 0),
      'job_counts', jsonb_build_object('queued', 0, 'running', 0, 'unresolved_failed', 0, 'unresolved_dead', 0),
      'session_blocker_codes', jsonb_build_array('WORKBENCH_SESSION_NOT_FOUND'),
      'draft_blocker_codes', jsonb_build_array('WORKBENCH_SESSION_NOT_FOUND'),
      'blocker_codes', jsonb_build_array('WORKBENCH_SESSION_NOT_FOUND'),
      'blocker_counts', '{}'::jsonb,
      'selected_eligible_ready_row_count', 0,
      'status', NULL::text,
      'session_status', NULL::text,
      'phase', 'REBASE_REQUIRED',
      'progress_state', 'REBASE_REQUIRED',
      'status_text', 'Payment preview needs refreshing.',
      'next_recommended_action', 'OPEN_NEW_SESSION',
      'stored_ready_mismatch', false,
      'read_only', true
    );
  END IF;

  v_scope_total_count := GREATEST(COALESCE(v_session_row.scope_total_count, 0), 0);
  v_scope_seeded_count := GREATEST(COALESCE(v_session_row.scope_seeded_count, 0), 0);
  v_scope_ready_count := GREATEST(COALESCE(v_session_row.scope_ready_count, 0), 0);
  v_scope_pending_count := GREATEST(COALESCE(v_session_row.scope_pending_count, 0), 0);
  v_scope_failed_count := GREATEST(COALESCE(v_session_row.scope_failed_count, 0), 0);
  v_line_units_total := GREATEST(COALESCE(v_session_row.line_units_total, 0), 0);
  v_line_units_ready := GREATEST(COALESCE(v_session_row.line_units_ready, 0), 0);
  v_line_units_pending := GREATEST(COALESCE(v_session_row.line_units_pending, 0), 0);
  v_line_units_failed := GREATEST(COALESCE(v_session_row.line_units_failed, 0), 0);
  v_preview_row_count := GREATEST(COALESCE(v_session_row.preview_row_count, 0), 0);
  v_selected_row_count := GREATEST(COALESCE(v_session_row.selected_row_count, 0), 0);
  v_section_counts_json := CASE WHEN jsonb_typeof(COALESCE(v_session_row.section_counts_json, '{}'::jsonb)) = 'object' THEN COALESCE(v_session_row.section_counts_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_candidate_sample_rows_json := CASE WHEN jsonb_typeof(COALESCE(v_session_row.candidate_sample_rows_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_session_row.candidate_sample_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END;
  v_scope_cursor_remaining := jsonb_typeof(COALESCE(v_session_row.scope_next_cursor_json, '{}'::jsonb)) = 'object'
    AND COALESCE(v_session_row.scope_next_cursor_json, '{}'::jsonb) <> '{}'::jsonb;
  v_session_obsolete := UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
    OR v_session_row.discarded_at_utc IS NOT NULL
    OR v_session_row.replacement_session_id IS NOT NULL;
  v_replacement_required := v_session_obsolete
    OR v_session_row.replacement_idempotency_key IS NOT NULL
    OR LOWER(BTRIM(COALESCE(v_session_row.progress_json->>'replacement_required', v_session_row.progress_json->>'requires_new_session', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_replacement_session_id_text := CASE WHEN v_session_row.replacement_session_id IS NULL THEN NULL ELSE v_session_row.replacement_session_id::text END;

  SELECT
    COALESCE(jsonb_agg(jsonb_build_object(
      'job_type', active_job.job_type,
      'canonical_job_type', active_job.canonical_job_type,
      'status', active_job.status,
      'candidate_id', CASE WHEN active_job.candidate_id IS NULL THEN NULL ELSE active_job.candidate_id::text END,
      'run_at_utc', active_job.run_at_utc,
      'started_at_utc', active_job.started_at_utc
    ) ORDER BY active_job.run_at_utc, active_job.priority, active_job.id), '[]'::jsonb),
    COUNT(*)::integer,
    COUNT(*) FILTER (WHERE active_job.status = 'QUEUED')::integer,
    COUNT(*) FILTER (WHERE active_job.status = 'RUNNING')::integer,
    COUNT(*) FILTER (WHERE active_job.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH')::integer,
    COUNT(*) FILTER (WHERE active_job.canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' AND LOWER(BTRIM(COALESCE(active_job.payload_json->>'fallback_from_delta', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'))::integer,
    (COUNT(*) FILTER (WHERE active_job.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' AND UPPER(BTRIM(COALESCE(active_job.payload_json->>'projection_mode', ''))) IN ('READINESS_PATCH', 'RESERVATION_PATCH', 'POST_DRAFT_OVERLAY')) > 0),
    (COUNT(*) FILTER (WHERE active_job.canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE') > 0)
  INTO v_active_jobs_json,
       v_active_job_count,
       v_active_queued_count,
       v_active_running_count,
       v_delta_refresh_pending_count,
       v_fallback_legacy_pending_count,
       v_patching_preview_rows,
       v_clone_rebase_pending
  FROM (
    SELECT queued_job.id,
           queued_job.job_type,
           CASE
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH') THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE') THEN 'WORKBENCH_SESSION_CLONE_REBASE'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE') THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE') THEN 'WORKBENCH_SESSION_SCOPE_SEED'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK') THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
             ELSE UPPER(BTRIM(COALESCE(queued_job.job_type, '')))
           END AS canonical_job_type,
           queued_job.status,
           queued_job.candidate_id,
           queued_job.run_at_utc,
           queued_job.started_at_utc,
           queued_job.priority,
           queued_job.payload_json
    FROM public.banking_pay_workbench_jobs AS queued_job
    WHERE queued_job.session_id = p_session_id
      AND queued_job.status IN ('QUEUED', 'RUNNING')
    ORDER BY queued_job.run_at_utc, queued_job.priority, queued_job.id
    LIMIT 10
  ) AS active_job;

  SELECT
    COUNT(*)::integer,
    COUNT(*) FILTER (
      WHERE COALESCE(scope_row.seeded, false)
         OR UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN (
           'READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY'
         )
    )::integer,
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY')
    )::integer,
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) NOT IN (
        'READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY',
        'ERROR', 'FAILED', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR'
      )
    )::integer,
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN (
        'ERROR', 'FAILED', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR'
      )
    )::integer
  INTO
    v_scope_total_count,
    v_scope_seeded_count,
    v_scope_ready_count,
    v_scope_pending_count,
    v_scope_failed_count
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id;

  WITH source_pending AS (
    SELECT
      scope_row.candidate_id,
      scope_row.scope_ordinal,
      scope_row.pending_job_id,
      owner_job.status AS owner_status,
      owner_job.attempt_count,
      owner_job.max_attempts,
      COALESCE(change_counter.seq, 0) AS live_change_seq,
      (
        owner_job.id IS NOT NULL
        AND owner_job.session_id = scope_row.session_id
        AND owner_job.candidate_id = scope_row.candidate_id
        AND UPPER(BTRIM(COALESCE(owner_job.status, ''))) IN ('QUEUED', 'RUNNING')
        AND UPPER(BTRIM(COALESCE(owner_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        )
        AND CASE
              WHEN COALESCE(owner_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                THEN (owner_job.payload_json->>'session_version')::bigint
              ELSE NULL::bigint
            END = v_session_row.version
        AND CASE
              WHEN COALESCE(owner_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                THEN (owner_job.payload_json->>'source_change_seq')::bigint
              ELSE NULL::bigint
            END >= COALESCE(change_counter.seq, 0)
        AND COALESCE(owner_job.payload_json->>'source_build_run_id', '') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS owner_valid,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS successor_job
        WHERE successor_job.id IS DISTINCT FROM scope_row.pending_job_id
          AND successor_job.session_id = scope_row.session_id
          AND successor_job.candidate_id = scope_row.candidate_id
          AND UPPER(BTRIM(COALESCE(successor_job.status, ''))) IN ('QUEUED', 'RUNNING')
          AND UPPER(BTRIM(COALESCE(successor_job.job_type, ''))) IN (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
            'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
            'CANDIDATE_SOURCE_BUILD',
            'CANDIDATE_SOURCE_BUILD_CHUNK',
            'SOURCE_BUILD',
            'SOURCE_BUILD_PAGE'
          )
          AND CASE
                WHEN COALESCE(successor_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                  THEN (successor_job.payload_json->>'session_version')::bigint
                ELSE NULL::bigint
              END = v_session_row.version
          AND CASE
                WHEN COALESCE(successor_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                  THEN (successor_job.payload_json->>'source_change_seq')::bigint
                ELSE NULL::bigint
              END >= COALESCE(change_counter.seq, 0)
          AND COALESCE(successor_job.payload_json->>'source_build_run_id', '') ~*
              '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS successor_valid
    FROM public.banking_pay_workbench_session_scope AS scope_row
    LEFT JOIN public.banking_pay_workbench_jobs AS owner_job
      ON owner_job.id = scope_row.pending_job_id
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key = 'pay_candidate:' || scope_row.candidate_id::text
    WHERE scope_row.session_id = p_session_id
      AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SOURCE_BUILD_PENDING'
  ), ownership_failure AS (
    SELECT
      source_pending.*,
      CASE
        WHEN source_pending.pending_job_id IS NULL THEN 'PENDING_JOB_ID_MISSING'
        WHEN source_pending.owner_status IS NULL THEN 'PENDING_JOB_ROW_MISSING'
        WHEN UPPER(BTRIM(COALESCE(source_pending.owner_status, ''))) NOT IN ('QUEUED', 'RUNNING') THEN 'PENDING_JOB_TERMINAL'
        ELSE 'PENDING_JOB_CONTEXT_INVALID'
      END AS owner_failure_reason
    FROM source_pending
    WHERE source_pending.owner_valid IS NOT TRUE
  ), ownership_summary AS (
    SELECT
      COUNT(*) FILTER (WHERE ownership_failure.successor_valid IS NOT TRUE)::integer AS recovery_required_count,
      COUNT(*) FILTER (WHERE ownership_failure.successor_valid IS TRUE)::integer AS recovery_scheduled_count
    FROM ownership_failure
  ), ownership_samples AS (
    SELECT COALESCE(
      jsonb_agg(
        jsonb_strip_nulls(jsonb_build_object(
          'candidate_id', ownership_sample.candidate_id::text,
          'scope_status', 'SOURCE_BUILD_PENDING',
          'pending_job_id', CASE WHEN ownership_sample.pending_job_id IS NULL THEN NULL ELSE ownership_sample.pending_job_id::text END,
          'pending_job_status', ownership_sample.owner_status,
          'attempt_count', COALESCE(ownership_sample.attempt_count, 0),
          'max_attempts', COALESCE(ownership_sample.max_attempts, 8),
          'failure_code', 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB',
          'owner_failure_reason', ownership_sample.owner_failure_reason,
          'automatic_recovery_scheduled', ownership_sample.successor_valid,
          'message', CASE
            WHEN ownership_sample.successor_valid
              THEN 'CloudTMS found a valid successor job and will rebind this candidate automatically.'
            ELSE 'This candidate is pending without a valid active refresh job.'
          END
        ))
        ORDER BY ownership_sample.scope_ordinal, ownership_sample.candidate_id
      ),
      '[]'::jsonb
    ) AS sample_json
    FROM (
      SELECT *
      FROM ownership_failure
      ORDER BY scope_ordinal, candidate_id
      LIMIT 10
    ) AS ownership_sample
  )
  SELECT
    COALESCE(ownership_summary.recovery_required_count, 0),
    COALESCE(ownership_summary.recovery_scheduled_count, 0),
    COALESCE(ownership_samples.sample_json, '[]'::jsonb)
  INTO
    v_recovery_required_count,
    v_recovery_scheduled_count,
    v_pending_owner_failures_json
  FROM ownership_summary
  CROSS JOIN ownership_samples;

  v_scope_pending_count := GREATEST(v_scope_pending_count - v_recovery_required_count, 0);
  v_scope_failed_count := v_scope_failed_count + v_recovery_required_count;

  SELECT
    COUNT(*)::integer,
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SKIPPED')
    )::integer,
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) NOT IN (
        'READY', 'MATERIALISED', 'MATERIALIZED', 'SKIPPED', 'ERROR', 'FAILED'
      )
    )::integer,
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(line_work.status, ''))) IN ('ERROR', 'FAILED')
    )::integer
  INTO
    v_line_units_total,
    v_line_units_ready,
    v_line_units_pending,
    v_line_units_failed
  FROM public.banking_pay_workbench_candidate_line_work AS line_work
  WHERE line_work.session_id = p_session_id;

  SELECT COALESCE(
    jsonb_agg(
      jsonb_strip_nulls(jsonb_build_object(
        'candidate_id', candidate_sample.candidate_id::text,
        'status', candidate_sample.status,
        'seeded', candidate_sample.seeded,
        'dirty', candidate_sample.dirty,
        'error_code', candidate_sample.error_code
      ))
      ORDER BY candidate_sample.scope_ordinal, candidate_sample.candidate_id
    ),
    '[]'::jsonb
  )
  INTO v_candidate_sample_rows_json
  FROM (
    SELECT
      scope_row.candidate_id,
      scope_row.scope_ordinal,
      UPPER(BTRIM(COALESCE(scope_row.status, 'UNKNOWN'))) AS status,
      (
        COALESCE(scope_row.seeded, false)
        OR UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN (
          'READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY'
        )
      ) AS seeded,
      COALESCE(scope_row.dirty, false) AS dirty,
      NULLIF(BTRIM(COALESCE(
        scope_row.error_json->>'code',
        scope_row.error_json#>>'{job_error_json,code}',
        scope_row.error_json#>>'{last_error_json,code}',
        ''
      )), '') AS error_code
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
    ORDER BY scope_row.scope_ordinal, scope_row.candidate_id
    LIMIT 25
  ) AS candidate_sample;

  WITH failed_scope AS (
    SELECT scope_row.candidate_id
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
      AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN (
        'ERROR', 'FAILED', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR'
      )
  ), latest_terminal_job AS (
    SELECT failed_scope.candidate_id, terminal_job.status
    FROM failed_scope
    LEFT JOIN LATERAL (
      SELECT UPPER(BTRIM(COALESCE(workbench_job.status, ''))) AS status
      FROM public.banking_pay_workbench_jobs AS workbench_job
      WHERE workbench_job.session_id = p_session_id
        AND workbench_job.candidate_id = failed_scope.candidate_id
        AND UPPER(BTRIM(COALESCE(workbench_job.status, ''))) IN ('FAILED', 'DEAD')
      ORDER BY
        workbench_job.failed_at_utc DESC NULLS LAST,
        workbench_job.updated_at_utc DESC NULLS LAST,
        workbench_job.created_at_utc DESC NULLS LAST,
        workbench_job.id DESC
      LIMIT 1
    ) AS terminal_job ON true
  )
  SELECT
    COUNT(*) FILTER (WHERE latest_terminal_job.status = 'FAILED')::integer,
    COUNT(*) FILTER (WHERE latest_terminal_job.status = 'DEAD')::integer
  INTO v_unresolved_failed_count, v_unresolved_dead_count
  FROM latest_terminal_job;

  v_terminal_failure := COALESCE(v_scope_failed_count, 0) > 0
    OR COALESCE(v_line_units_failed, 0) > 0
    OR COALESCE(v_unresolved_failed_count, 0) > 0
    OR COALESCE(v_unresolved_dead_count, 0) > 0;

  v_patching_preview_rows := COALESCE(v_patching_preview_rows, false) OR UPPER(BTRIM(COALESCE(v_session_row.progress_state, ''))) = 'PATCHING_PREVIEW_ROWS';
  v_clone_rebase_pending := COALESCE(v_clone_rebase_pending, false) OR UPPER(BTRIM(COALESCE(v_session_row.progress_state, ''))) = 'CLONE_REBASING';
  v_still_running := COALESCE(v_active_running_count, 0) > 0;
  v_work_queued := COALESCE(v_active_job_count, 0) > 0
    OR COALESCE(v_scope_pending_count, 0) > 0
    OR COALESCE(v_line_units_pending, 0) > 0
    OR COALESCE(v_scope_cursor_remaining, false)
    OR COALESCE(v_session_row.scope_seed_complete, false) IS NOT TRUE;

  IF COALESCE(v_terminal_failure, false)
     AND COALESCE(v_active_job_count, 0) = 0 THEN
    v_work_queued := false;
    v_still_running := false;
  END IF;

  -- Use active READY preview rows as the draft/readiness source of truth. The
  -- session counter columns are reconciled asynchronously and can otherwise keep
  -- a just-superseded unauthorised row draftable for one polling cycle.
  WITH active_ready_preview_rows AS (
    SELECT preview_row.section,
           preview_row.selected,
           preview_row.selection_state
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = v_session_row.version
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
  ), active_ready_counts AS (
    SELECT COUNT(*)::integer AS preview_count,
           COUNT(*) FILTER (
             WHERE active_ready_preview_rows.selected IS TRUE
               AND active_ready_preview_rows.selection_state = 'SELECTED'
           )::integer AS selected_count
    FROM active_ready_preview_rows
  ), active_section_counts AS (
    SELECT active_ready_preview_rows.section,
           COUNT(*)::integer AS row_count
    FROM active_ready_preview_rows
    GROUP BY active_ready_preview_rows.section
  ), active_section_json AS (
    SELECT COALESCE(jsonb_object_agg(active_section_counts.section, active_section_counts.row_count ORDER BY active_section_counts.section), '{}'::jsonb) AS section_counts_json
    FROM active_section_counts
  )
  SELECT COALESCE(active_ready_counts.preview_count, 0),
         COALESCE(active_ready_counts.selected_count, 0),
         COALESCE(active_section_json.section_counts_json, '{}'::jsonb)
  INTO v_preview_row_count,
       v_selected_row_count,
       v_section_counts_json
  FROM active_ready_counts
  CROSS JOIN active_section_json;

  v_line_units_total_display := GREATEST(COALESCE(v_line_units_total, 0), COALESCE(v_preview_row_count, 0));
  v_line_units_complete := LEAST(COALESCE(v_preview_row_count, 0), COALESCE(v_line_units_total_display, 0));
  v_line_units_ready_not_materialised := GREATEST(COALESCE(v_line_units_ready, 0) - COALESCE(v_line_units_complete, 0), 0);

  IF COALESCE(v_work_queued, false) IS NOT TRUE THEN
    v_line_units_ready_not_materialised := 0;
  END IF;

  v_rows_available := COALESCE(v_preview_row_count, 0) > 0;
  v_selected_rows_available := COALESCE(v_selected_row_count, 0) > 0;

  IF v_replacement_required THEN
    v_progress_state := 'REBASE_REQUIRED';
    v_phase := 'REBASE_REQUIRED';
    v_status_text := 'Payment preview needs refreshing.';
    v_next_recommended_action := 'OPEN_NEW_SESSION';
  ELSIF COALESCE(v_recovery_required_count, 0) > 0 THEN
    v_progress_state := 'RECOVERY_REQUIRED';
    v_phase := 'RECOVERY_REQUIRED';
    v_status_text := 'Payment preview stopped because a candidate refresh no longer has an active job.';
    v_next_recommended_action := 'REFRESH_OR_RETRY';
  ELSIF COALESCE(v_recovery_scheduled_count, 0) > 0 THEN
    v_progress_state := 'AUTOMATIC_RECOVERY';
    v_phase := 'AUTOMATIC_RECOVERY';
    v_status_text := 'CloudTMS is reconnecting a candidate to its active refresh job.';
    v_next_recommended_action := 'WAIT_FOR_WORKER';
  ELSIF COALESCE(v_terminal_failure, false) THEN
    v_progress_state := 'ERROR';
    v_phase := 'ERROR';
    v_status_text := 'Payment preview could not be refreshed. Review the failed candidate and retry.';
    v_next_recommended_action := 'REVIEW_WORKBENCH_ERROR';
  ELSIF v_clone_rebase_pending THEN
    v_progress_state := 'CLONE_REBASING';
    v_phase := 'CLONE_REBASING';
    v_status_text := 'Reusing certified payment preview rows.';
    v_next_recommended_action := 'WAIT_FOR_WORKER';
  ELSIF COALESCE(v_fallback_legacy_pending_count, 0) > 0 THEN
    v_progress_state := 'FALLING_BACK_TO_LEGACY_REFRESH';
    v_phase := 'FALLING_BACK_TO_LEGACY_REFRESH';
    v_status_text := 'Refreshing candidate through the legacy source-build path.';
    v_next_recommended_action := 'BUILD_SOURCE_CHUNK';
  ELSIF COALESCE(v_delta_refresh_pending_count, 0) > 0
     OR (UPPER(BTRIM(COALESCE(v_session_row.progress_state, ''))) = 'DELTA_REFRESHING' AND COALESCE(v_work_queued, false) IS TRUE) THEN
    v_progress_state := 'DELTA_REFRESHING';
    v_phase := 'DELTA_REFRESHING';
    v_status_text := 'Refreshing changed candidate rows.';
    v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
  ELSIF COALESCE(v_session_row.scope_seed_complete, false) IS NOT TRUE OR COALESCE(v_scope_cursor_remaining, false) THEN
    v_progress_state := 'SEEDING_SCOPE';
    v_phase := 'SEEDING_SCOPE';
    v_status_text := 'Finding candidates for the payment preview.';
    v_next_recommended_action := 'SEED_SCOPE_CHUNK';
  ELSIF v_patching_preview_rows THEN
    v_progress_state := 'PATCHING_PREVIEW_ROWS';
    v_phase := 'PATCHING_PREVIEW_ROWS';
    v_status_text := 'Patching payment preview rows.';
    v_next_recommended_action := 'WAIT_FOR_WORKER';
  ELSE
    v_progress_state := CASE
      WHEN COALESCE(v_work_queued, false) IS TRUE THEN COALESCE(NULLIF(BTRIM(v_session_row.progress_state), ''), 'REFRESHING_CANDIDATES')
      WHEN COALESCE(v_preview_row_count, 0) = 0 THEN 'READY_EMPTY'
      ELSE 'READY'
    END;
    v_phase := CASE
      WHEN COALESCE(v_work_queued, false) IS TRUE THEN COALESCE(NULLIF(BTRIM(v_session_row.progress_json->>'phase'), ''), v_progress_state)
      ELSE 'READY'
    END;
    v_status_text := CASE
      WHEN COALESCE(v_work_queued, false) IS TRUE THEN COALESCE(NULLIF(BTRIM(v_session_row.progress_json->>'status_text'), ''), 'Preparing payment preview.')
      ELSE 'Payment preview is ready.'
    END;
    v_next_recommended_action := CASE
      WHEN COALESCE(v_work_queued, false) IS TRUE THEN COALESCE(NULLIF(BTRIM(v_session_row.progress_json->>'next_recommended_action'), ''), 'WAIT_FOR_WORKER')
      ELSE 'READ_PREVIEW_PAGE'
    END;
  END IF;

  v_ready := v_replacement_required IS NOT TRUE
    AND COALESCE(v_session_row.scope_seed_complete, false) IS TRUE
    AND COALESCE(v_scope_cursor_remaining, false) IS NOT TRUE
    AND v_work_queued IS NOT TRUE
    AND COALESCE(v_scope_failed_count, 0) = 0
    AND COALESCE(v_line_units_failed, 0) = 0
    AND COALESCE(v_scope_pending_count, 0) = 0
    AND COALESCE(v_line_units_pending, 0) = 0;
  v_ready_empty := v_ready AND COALESCE(v_preview_row_count, 0) = 0;
  v_ready_for_draft := v_ready AND COALESCE(v_selected_row_count, 0) > 0;

  IF v_ready THEN
    v_line_units_ready_not_materialised := 0;
    v_line_units_complete := COALESCE(v_line_units_total_display, 0);
    v_progress_state := CASE WHEN v_ready_empty THEN 'READY_EMPTY' ELSE 'READY' END;
    v_phase := v_progress_state;
    v_status_text := CASE WHEN v_ready_empty THEN 'No payable rows found.' ELSE 'Payment preview is ready.' END;
    v_next_recommended_action := 'READ_PREVIEW_PAGE';
    v_patching_preview_rows := false;
    v_clone_rebase_pending := false;
  END IF;

  IF v_replacement_required THEN
    v_session_blocker_codes := v_session_blocker_codes || jsonb_build_array('REPLACEMENT_REQUIRED');
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('REPLACEMENT_REQUIRED');
  END IF;
  IF COALESCE(v_scope_failed_count, 0) > 0 OR COALESCE(v_line_units_failed, 0) > 0 THEN
    v_session_blocker_codes := v_session_blocker_codes || jsonb_build_array('WORKBENCH_ERRORS_PRESENT');
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_ERRORS_PRESENT');
  END IF;
  IF COALESCE(v_recovery_required_count, 0) > 0 THEN
    v_session_blocker_codes := v_session_blocker_codes || jsonb_build_array('WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB');
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB');
  END IF;
  IF COALESCE(v_recovery_scheduled_count, 0) > 0 THEN
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_AUTOMATIC_RECOVERY_PENDING');
  END IF;
  IF COALESCE(v_work_queued, false) THEN
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_REFRESH_PENDING');
  END IF;
  IF v_ready AND COALESCE(v_selected_row_count, 0) = 0 THEN
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('NO_SELECTED_ROWS');
  END IF;


  v_response_progress_json := public.pay_workbench_session_compact_progress_json(
    jsonb_strip_nulls(
      jsonb_build_object(
        'ok', true,
        'session_id', p_session_id::text,
        'server_utc', v_now,
        'updated_at_utc', v_now,
        'status', v_session_row.status,
        'session_status', v_session_row.status,
        'session_version', v_session_row.version,
        'progress_state', v_progress_state,
        'phase', v_phase,
        'status_text', v_status_text,
        'next_recommended_action', v_next_recommended_action,
        'ready', v_ready,
        'ready_flag', v_ready,
        'session_ready', v_ready,
        'ready_for_draft', v_ready_for_draft,
        'can_create_draft', v_ready_for_draft,
        'ready_empty', v_ready_empty,
        'rows_available', v_rows_available,
        'selected_rows_available', v_selected_rows_available,
        'has_materialised_preview_rows', v_rows_available,
        'still_running', v_still_running,
        'work_queued', v_work_queued
      )
      || jsonb_build_object(
        'terminal_failure', COALESCE(v_terminal_failure, false),
        'recovery_required', COALESCE(v_recovery_required_count, 0) > 0,
        'recovery_required_count', COALESCE(v_recovery_required_count, 0),
        'recovery_scheduled', COALESCE(v_recovery_scheduled_count, 0) > 0,
        'recovery_scheduled_count', COALESCE(v_recovery_scheduled_count, 0),
        'pending_owner_failures', COALESCE(v_pending_owner_failures_json, '[]'::jsonb),
        'pending_refresh', v_work_queued,
        'refresh_pending', v_work_queued,
        'preview_refresh_pending', v_work_queued,
        'preview_deferred', v_work_queued,
        'scope_seed_complete', COALESCE(v_session_row.scope_seed_complete, false),
        'scope_cursor_remaining', v_scope_cursor_remaining,
        'selected_eligible_ready_row_count', v_selected_row_count,
        'active_jobs', COALESCE(v_active_jobs_json, '[]'::jsonb),
        'candidate_counts', jsonb_build_object(
          'total', v_scope_total_count,
          'ready', LEAST(v_scope_ready_count, v_scope_total_count),
          'terminal_materialised', LEAST(v_scope_ready_count, v_scope_total_count),
          'pending', v_scope_pending_count,
          'processing', v_scope_pending_count,
          'materialisation_pending', 0,
          'dirty', v_scope_pending_count,
          'failed', v_scope_failed_count,
          'unknown', 0,
          'unseeded', GREATEST(v_scope_total_count - v_scope_seeded_count, 0)
        ),
        'line_counts', jsonb_build_object(
          'total', v_line_units_total_display,
          'complete', v_line_units_complete,
          'materialised_or_skipped', v_line_units_complete,
          'pending', v_line_units_pending,
          'ready_not_materialised', v_line_units_ready_not_materialised,
          'failed', v_line_units_failed,
          'unknown', 0
        ),
        'job_counts', jsonb_build_object(
          'queued', COALESCE(v_active_queued_count, 0),
          'running', COALESCE(v_active_running_count, 0),
          'unresolved_failed', COALESCE(v_unresolved_failed_count, 0),
          'unresolved_dead', COALESCE(v_unresolved_dead_count, 0)
        ),
        'session_blocker_codes', COALESCE(v_session_blocker_codes, '[]'::jsonb),
        'draft_blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
        'blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
        'blocker_counts', jsonb_build_object(
          'pending_scope_without_active_job', COALESCE(v_recovery_required_count, 0),
          'automatic_recovery_pending', COALESCE(v_recovery_scheduled_count, 0)
        )
      )
    ),
    true
  );

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'status', v_session_row.status,
    'session_status', v_session_row.status,
    'session_version', v_session_row.version,
    'progress_state', v_progress_state,
    'phase', v_phase,
    'status_text', v_status_text,
    'next_recommended_action', v_next_recommended_action,
    'progress_json', v_response_progress_json,
    'progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
    'progress_updated_at_utc', v_session_row.progress_updated_at_utc,
    'ready', v_ready,
    'ready_flag', v_ready,
    'session_ready', v_ready,
    'ready_for_draft', v_ready_for_draft,
    'can_create_draft', v_ready_for_draft,
    'ready_empty', v_ready_empty,
    'rows_available', v_rows_available,
    'selected_rows_available', v_selected_rows_available,
    'has_materialised_preview_rows', v_rows_available,
    'still_running', v_still_running,
    'work_queued', v_work_queued,
    'pending_refresh', v_work_queued,
    'refresh_pending', v_work_queued,
    'preview_refresh_pending', v_work_queued,
    'preview_deferred', v_work_queued,
    'session_obsolete', v_session_obsolete,
    'obsolete', v_session_obsolete,
    'replacement_required', v_replacement_required,
    'rebase_required', v_replacement_required,
    'requires_new_session', v_replacement_required,
    'replacement_session_id', v_replacement_session_id_text,
    'replacement_available', v_replacement_session_id_text IS NOT NULL,
    'scope_seed_complete', COALESCE(v_session_row.scope_seed_complete, false),
    'scope_cursor_remaining', v_scope_cursor_remaining,
    'scope_total_count', v_scope_total_count,
    'scope_seeded_count', v_scope_seeded_count,
    'scope_ready_count', v_scope_ready_count,
    'scope_pending_count', v_scope_pending_count,
    'scope_failed_count', v_scope_failed_count,
    'total_candidates', v_scope_total_count,
    'total_count', v_scope_total_count,
    'completed_candidates', LEAST(v_scope_ready_count, v_scope_total_count),
    'completed_count', LEAST(v_scope_ready_count, v_scope_total_count),
    'ready_candidates', LEAST(v_scope_ready_count, v_scope_total_count),
    'pending_candidates', v_scope_pending_count,
    'pending_count', v_scope_pending_count
  )
  || jsonb_build_object(
    'failed_candidates', v_scope_failed_count,
    'failed_count', v_scope_failed_count,
    'terminal_failure', COALESCE(v_terminal_failure, false),
    'recovery_required', COALESCE(v_recovery_required_count, 0) > 0,
    'recovery_required_count', COALESCE(v_recovery_required_count, 0),
    'recovery_scheduled', COALESCE(v_recovery_scheduled_count, 0) > 0,
    'recovery_scheduled_count', COALESCE(v_recovery_scheduled_count, 0),
    'line_units_total', v_line_units_total_display,
    'line_units_complete', v_line_units_complete,
    'line_units_ready', v_line_units_ready,
    'line_units_ready_not_materialised', v_line_units_ready_not_materialised,
    'line_units_pending', v_line_units_pending,
    'line_units_failed', v_line_units_failed,
    'preview_row_count', v_preview_row_count,
    'selected_row_count', v_selected_row_count
  )
  || jsonb_build_object(
    'selected_eligible_ready_row_count', v_selected_row_count,
    'section_counts_json', v_section_counts_json,
    'section_counts', v_section_counts_json,
    'candidate_sample_rows_json', v_candidate_sample_rows_json,
    'pending_owner_failures', COALESCE(v_pending_owner_failures_json, '[]'::jsonb),
    'active_jobs', COALESCE(v_active_jobs_json, '[]'::jsonb),
    'unresolved_failed_jobs', COALESCE(v_unresolved_failed_count, 0),
    'unresolved_dead_jobs', COALESCE(v_unresolved_dead_count, 0),
    'pending_job_ids_json', '[]'::jsonb,
    'delta_refresh_pending_count', COALESCE(v_delta_refresh_pending_count, 0),
    'fallback_legacy_pending_count', COALESCE(v_fallback_legacy_pending_count, 0),
    'patching_preview_rows', COALESCE(v_patching_preview_rows, false),
    'clone_rebase_pending', COALESCE(v_clone_rebase_pending, false),
    'candidate_counts', jsonb_build_object(
      'total', v_scope_total_count,
      'ready', LEAST(v_scope_ready_count, v_scope_total_count),
      'terminal_materialised', LEAST(v_scope_ready_count, v_scope_total_count),
      'pending', v_scope_pending_count,
      'processing', v_scope_pending_count,
      'materialisation_pending', 0,
      'dirty', v_scope_pending_count,
      'failed', v_scope_failed_count,
      'unknown', 0,
      'unseeded', GREATEST(v_scope_total_count - v_scope_seeded_count, 0)
    ),
    'line_counts', jsonb_build_object(
      'total', v_line_units_total_display,
      'complete', v_line_units_complete,
      'materialised_or_skipped', v_line_units_complete,
      'pending', v_line_units_pending,
      'ready_not_materialised', v_line_units_ready_not_materialised,
      'failed', v_line_units_failed,
      'unknown', 0
    ),
    'job_counts', jsonb_build_object(
      'queued', COALESCE(v_active_queued_count, 0),
      'running', COALESCE(v_active_running_count, 0),
      'unresolved_failed', COALESCE(v_unresolved_failed_count, 0),
      'unresolved_dead', COALESCE(v_unresolved_dead_count, 0)
    ),
    'source_build_counts', jsonb_build_object(
      'fallback_legacy_pending', COALESCE(v_fallback_legacy_pending_count, 0),
      'delta_refresh_pending', COALESCE(v_delta_refresh_pending_count, 0)
    ),
    'session_blocker_codes', COALESCE(v_session_blocker_codes, '[]'::jsonb),
    'draft_blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
    'blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
    'blocker_counts', jsonb_build_object(
      'pending_scope_without_active_job', COALESCE(v_recovery_required_count, 0),
      'automatic_recovery_pending', COALESCE(v_recovery_scheduled_count, 0)
    ),
    'stored_ready_mismatch', false,
    'read_only', v_replacement_required OR COALESCE(v_terminal_failure, false)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_recompute_progress_counters(p_session_id uuid, p_apply boolean DEFAULT true, p_reason text DEFAULT 'AUTHORITATIVE_COUNTER_RECOMPUTE'::text, p_write_progress_json boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_cursor_remaining boolean := false;
  v_session_obsolete boolean := false;
  v_replacement_required boolean := false;
  v_replacement_session_id_text text := NULL::text;

  v_scope_total_count integer := 0;
  v_scope_seeded_count integer := 0;
  v_scope_ready_count integer := 0;
  v_scope_pending_count integer := 0;
  v_scope_failed_count integer := 0;
  v_scope_dirty_count integer := 0;
  v_recovery_required_count integer := 0;
  v_recovery_scheduled_count integer := 0;
  v_pending_owner_failures_json jsonb := '[]'::jsonb;

  v_line_units_total integer := 0;
  v_line_units_ready integer := 0;
  v_line_units_pending integer := 0;
  v_line_units_failed integer := 0;

  v_preview_row_count integer := 0;
  v_selected_row_count integer := 0;
  v_section_counts_json jsonb := '{}'::jsonb;
  v_candidate_sample_rows_json jsonb := '[]'::jsonb;

  v_active_jobs_json jsonb := '[]'::jsonb;
  v_active_job_count integer := 0;
  v_active_queued_count integer := 0;
  v_active_running_count integer := 0;
  v_delta_refresh_pending_count integer := 0;
  v_fallback_legacy_pending_count integer := 0;
  v_patching_preview_rows boolean := false;
  v_clone_rebase_pending boolean := false;

  v_work_queued boolean := false;
  v_still_running boolean := false;
  v_ready boolean := false;
  v_ready_empty boolean := false;
  v_ready_for_draft boolean := false;
  v_rows_available boolean := false;
  v_selected_rows_available boolean := false;

  v_progress_state text := 'REFRESHING_CANDIDATES';
  v_phase text := 'REFRESHING_CANDIDATES';
  v_status_text text := 'Preparing payment preview.';
  v_next_recommended_action text := 'WAIT_FOR_WORKER';

  v_session_blocker_codes jsonb := '[]'::jsonb;
  v_draft_blocker_codes jsonb := '[]'::jsonb;
  v_line_units_total_display integer := 0;
  v_line_units_complete integer := 0;
  v_line_units_ready_not_materialised integer := 0;
  v_progress_json jsonb := '{}'::jsonb;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'WORKBENCH_SESSION_NOT_FOUND',
      'code', 'WORKBENCH_SESSION_NOT_FOUND',
      'session_id', p_session_id::text,
      'session_ready', false,
      'ready', false,
      'ready_flag', false,
      'ready_for_draft', false,
      'ready_empty', false,
      'phase', 'REBASE_REQUIRED',
      'progress_state', 'REBASE_REQUIRED',
      'status_text', 'Payment preview needs refreshing.'
    );
  END IF;

  v_scope_cursor_remaining := jsonb_typeof(COALESCE(v_session_row.scope_next_cursor_json, '{}'::jsonb)) = 'object'
    AND COALESCE(v_session_row.scope_next_cursor_json, '{}'::jsonb) <> '{}'::jsonb;

  v_session_obsolete := UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
    OR v_session_row.discarded_at_utc IS NOT NULL
    OR v_session_row.replacement_session_id IS NOT NULL;

  v_replacement_required := v_session_obsolete
    OR v_session_row.replacement_idempotency_key IS NOT NULL
    OR LOWER(BTRIM(COALESCE(v_session_row.progress_json->>'replacement_required', v_session_row.progress_json->>'requires_new_session', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_replacement_session_id_text := CASE WHEN v_session_row.replacement_session_id IS NULL THEN NULL ELSE v_session_row.replacement_session_id::text END;

  WITH classified_scope AS (
    SELECT
      scope_row.id,
      UPPER(BTRIM(COALESCE(scope_row.status, ''))) AS status_key,
      COALESCE(scope_row.seeded, false) AS seeded,
      COALESCE(scope_row.dirty, false) AS dirty,
      NOT (
        scope_row.error_json IS NULL
        OR scope_row.error_json = '{}'::jsonb
        OR scope_row.error_json = 'null'::jsonb
      ) AS has_error
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
  ), counted_scope AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (WHERE seeded IS TRUE)::integer AS seeded_count,
      COUNT(*) FILTER (
        WHERE dirty IS NOT TRUE
          AND has_error IS NOT TRUE
          AND status_key IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY')
      )::integer AS ready_count,
      COUNT(*) FILTER (
        WHERE has_error IS TRUE
          OR status_key IN ('FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR')
      )::integer AS failed_count,
      COUNT(*) FILTER (WHERE dirty IS TRUE)::integer AS dirty_count
    FROM classified_scope
  )
  SELECT
    GREATEST(COALESCE(total_count, 0), 0),
    LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(seeded_count, 0), 0)),
    LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(ready_count, 0), 0)),
    GREATEST(
      GREATEST(COALESCE(total_count, 0), 0)
      - LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(ready_count, 0), 0))
      - LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(failed_count, 0), 0)),
      0
    ),
    LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(failed_count, 0), 0)),
    GREATEST(COALESCE(dirty_count, 0), 0)
  INTO v_scope_total_count,
       v_scope_seeded_count,
       v_scope_ready_count,
       v_scope_pending_count,
       v_scope_failed_count,
       v_scope_dirty_count
  FROM counted_scope;

  WITH source_pending AS (
    SELECT
      scope_row.candidate_id,
      scope_row.scope_ordinal,
      scope_row.pending_job_id,
      owner_job.status AS owner_status,
      owner_job.attempt_count,
      owner_job.max_attempts,
      (
        owner_job.id IS NOT NULL
        AND owner_job.session_id = scope_row.session_id
        AND owner_job.candidate_id = scope_row.candidate_id
        AND UPPER(BTRIM(COALESCE(owner_job.status, ''))) IN ('QUEUED', 'RUNNING')
        AND UPPER(BTRIM(COALESCE(owner_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        )
        AND CASE
              WHEN COALESCE(owner_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                THEN (owner_job.payload_json->>'session_version')::bigint
              ELSE NULL::bigint
            END = v_session_row.version
        AND CASE
              WHEN COALESCE(owner_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                THEN (owner_job.payload_json->>'source_change_seq')::bigint
              ELSE NULL::bigint
            END >= COALESCE(change_counter.seq, 0)
        AND COALESCE(owner_job.payload_json->>'source_build_run_id', '') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS owner_valid,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS successor_job
        WHERE successor_job.id IS DISTINCT FROM scope_row.pending_job_id
          AND successor_job.session_id = scope_row.session_id
          AND successor_job.candidate_id = scope_row.candidate_id
          AND UPPER(BTRIM(COALESCE(successor_job.status, ''))) IN ('QUEUED', 'RUNNING')
          AND UPPER(BTRIM(COALESCE(successor_job.job_type, ''))) IN (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
            'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
            'CANDIDATE_SOURCE_BUILD',
            'CANDIDATE_SOURCE_BUILD_CHUNK',
            'SOURCE_BUILD',
            'SOURCE_BUILD_PAGE'
          )
          AND CASE
                WHEN COALESCE(successor_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                  THEN (successor_job.payload_json->>'session_version')::bigint
                ELSE NULL::bigint
              END = v_session_row.version
          AND CASE
                WHEN COALESCE(successor_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                  THEN (successor_job.payload_json->>'source_change_seq')::bigint
                ELSE NULL::bigint
              END >= COALESCE(change_counter.seq, 0)
          AND COALESCE(successor_job.payload_json->>'source_build_run_id', '') ~*
              '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS successor_valid
    FROM public.banking_pay_workbench_session_scope AS scope_row
    LEFT JOIN public.banking_pay_workbench_jobs AS owner_job
      ON owner_job.id = scope_row.pending_job_id
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key = 'pay_candidate:' || scope_row.candidate_id::text
    WHERE scope_row.session_id = p_session_id
      AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SOURCE_BUILD_PENDING'
  ), ownership_failure AS (
    SELECT
      source_pending.*,
      CASE
        WHEN source_pending.pending_job_id IS NULL THEN 'PENDING_JOB_ID_MISSING'
        WHEN source_pending.owner_status IS NULL THEN 'PENDING_JOB_ROW_MISSING'
        WHEN UPPER(BTRIM(COALESCE(source_pending.owner_status, ''))) NOT IN ('QUEUED', 'RUNNING') THEN 'PENDING_JOB_TERMINAL'
        ELSE 'PENDING_JOB_CONTEXT_INVALID'
      END AS owner_failure_reason
    FROM source_pending
    WHERE source_pending.owner_valid IS NOT TRUE
  ), ownership_summary AS (
    SELECT
      COUNT(*) FILTER (WHERE ownership_failure.successor_valid IS NOT TRUE)::integer AS recovery_required_count,
      COUNT(*) FILTER (WHERE ownership_failure.successor_valid IS TRUE)::integer AS recovery_scheduled_count
    FROM ownership_failure
  ), ownership_samples AS (
    SELECT COALESCE(
      jsonb_agg(
        jsonb_strip_nulls(jsonb_build_object(
          'candidate_id', ownership_sample.candidate_id::text,
          'scope_status', 'SOURCE_BUILD_PENDING',
          'pending_job_id', CASE WHEN ownership_sample.pending_job_id IS NULL THEN NULL ELSE ownership_sample.pending_job_id::text END,
          'pending_job_status', ownership_sample.owner_status,
          'attempt_count', COALESCE(ownership_sample.attempt_count, 0),
          'max_attempts', COALESCE(ownership_sample.max_attempts, 8),
          'failure_code', 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB',
          'owner_failure_reason', ownership_sample.owner_failure_reason,
          'automatic_recovery_scheduled', ownership_sample.successor_valid,
          'message', CASE
            WHEN ownership_sample.successor_valid
              THEN 'CloudTMS found a valid successor job and will rebind this candidate automatically.'
            ELSE 'This candidate is pending without a valid active refresh job.'
          END
        ))
        ORDER BY ownership_sample.scope_ordinal, ownership_sample.candidate_id
      ),
      '[]'::jsonb
    ) AS sample_json
    FROM (
      SELECT *
      FROM ownership_failure
      ORDER BY scope_ordinal, candidate_id
      LIMIT 10
    ) AS ownership_sample
  )
  SELECT
    COALESCE(ownership_summary.recovery_required_count, 0),
    COALESCE(ownership_summary.recovery_scheduled_count, 0),
    COALESCE(ownership_samples.sample_json, '[]'::jsonb)
  INTO
    v_recovery_required_count,
    v_recovery_scheduled_count,
    v_pending_owner_failures_json
  FROM ownership_summary
  CROSS JOIN ownership_samples;

  v_scope_pending_count := GREATEST(v_scope_pending_count - v_recovery_required_count, 0);
  v_scope_failed_count := LEAST(v_scope_total_count, v_scope_failed_count + v_recovery_required_count);

  WITH classified_line_work AS (
    SELECT
      line_work.id,
      UPPER(BTRIM(COALESCE(line_work.status, ''))) AS status_key,
      NOT (
        line_work.error_json IS NULL
        OR line_work.error_json = '{}'::jsonb
        OR line_work.error_json = 'null'::jsonb
      ) AS has_error
    FROM public.banking_pay_workbench_candidate_line_work AS line_work
    WHERE line_work.session_id = p_session_id
  ), counted_line_work AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (
        WHERE has_error IS NOT TRUE
          AND status_key IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SKIPPED', 'SUPERSEDED', 'SOURCE_EMPTY', 'NOT_APPLICABLE', 'OBSOLETE')
      )::integer AS ready_count,
      COUNT(*) FILTER (
        WHERE has_error IS TRUE
          OR status_key IN ('ERROR', 'FAILED')
      )::integer AS failed_count
    FROM classified_line_work
  )
  SELECT
    GREATEST(COALESCE(total_count, 0), 0),
    LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(ready_count, 0), 0)),
    GREATEST(
      GREATEST(COALESCE(total_count, 0), 0)
      - LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(ready_count, 0), 0))
      - LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(failed_count, 0), 0)),
      0
    ),
    LEAST(GREATEST(COALESCE(total_count, 0), 0), GREATEST(COALESCE(failed_count, 0), 0))
  INTO v_line_units_total,
       v_line_units_ready,
       v_line_units_pending,
       v_line_units_failed
  FROM counted_line_work;

  WITH active_ready_preview_rows AS (
    SELECT preview_row.id,
           preview_row.candidate_id,
           preview_row.section,
           preview_row.row_key,
           preview_row.row_ordinal,
           preview_row.selected,
           preview_row.selection_state
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = v_session_row.version
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
  ), active_ready_counts AS (
    SELECT COUNT(*)::integer AS preview_count,
           COUNT(*) FILTER (
             WHERE active_ready_preview_rows.selected IS TRUE
               AND active_ready_preview_rows.selection_state = 'SELECTED'
           )::integer AS selected_count
    FROM active_ready_preview_rows
  ), active_section_json AS (
    SELECT COALESCE(jsonb_object_agg(section_counts.section, section_counts.row_count ORDER BY section_counts.section), '{}'::jsonb) AS section_counts_json
    FROM (
      SELECT active_ready_preview_rows.section,
             COUNT(*)::integer AS row_count
      FROM active_ready_preview_rows
      GROUP BY active_ready_preview_rows.section
    ) AS section_counts
  ), active_sample_json AS (
    SELECT COALESCE(jsonb_agg(sample_rows.sample_json ORDER BY sample_rows.row_ordinal, sample_rows.id), '[]'::jsonb) AS candidate_sample_rows_json
    FROM (
      SELECT active_ready_preview_rows.id,
             active_ready_preview_rows.row_ordinal,
             jsonb_build_object(
               'id', active_ready_preview_rows.id::text,
               'candidate_id', active_ready_preview_rows.candidate_id::text,
               'section', active_ready_preview_rows.section,
               'row_key', active_ready_preview_rows.row_key,
               'selected', active_ready_preview_rows.selected,
               'selection_state', active_ready_preview_rows.selection_state
             ) AS sample_json
      FROM active_ready_preview_rows
      ORDER BY active_ready_preview_rows.row_ordinal, active_ready_preview_rows.id
      LIMIT 50
    ) AS sample_rows
  )
  SELECT COALESCE(active_ready_counts.preview_count, 0),
         COALESCE(active_ready_counts.selected_count, 0),
         COALESCE(active_section_json.section_counts_json, '{}'::jsonb),
         COALESCE(active_sample_json.candidate_sample_rows_json, '[]'::jsonb)
  INTO v_preview_row_count,
       v_selected_row_count,
       v_section_counts_json,
       v_candidate_sample_rows_json
  FROM active_ready_counts
  CROSS JOIN active_section_json
  CROSS JOIN active_sample_json;

  WITH normalised_active_jobs AS (
    SELECT queued_job.id,
           queued_job.job_type,
           CASE
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH') THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE') THEN 'WORKBENCH_SESSION_CLONE_REBASE'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE') THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
             WHEN UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK') THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
             ELSE UPPER(BTRIM(COALESCE(queued_job.job_type, '')))
           END AS canonical_job_type,
           queued_job.status,
           queued_job.priority,
           queued_job.candidate_id,
           queued_job.run_at_utc,
           queued_job.started_at_utc,
           queued_job.created_at_utc,
           queued_job.payload_json
    FROM public.banking_pay_workbench_jobs AS queued_job
    WHERE queued_job.session_id = p_session_id
      AND UPPER(BTRIM(COALESCE(queued_job.status, ''))) IN ('QUEUED', 'RUNNING')
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'job_type', normalised_active_jobs.job_type,
           'canonical_job_type', normalised_active_jobs.canonical_job_type,
           'status', normalised_active_jobs.status,
           'candidate_id', CASE WHEN normalised_active_jobs.candidate_id IS NULL THEN NULL ELSE normalised_active_jobs.candidate_id::text END,
           'run_at_utc', normalised_active_jobs.run_at_utc,
           'started_at_utc', normalised_active_jobs.started_at_utc
         ) ORDER BY normalised_active_jobs.run_at_utc, normalised_active_jobs.priority, normalised_active_jobs.id), '[]'::jsonb),
         COUNT(*)::integer,
         COUNT(*) FILTER (WHERE normalised_active_jobs.status = 'QUEUED')::integer,
         COUNT(*) FILTER (WHERE normalised_active_jobs.status = 'RUNNING')::integer,
         COUNT(*) FILTER (WHERE normalised_active_jobs.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH')::integer,
         COUNT(*) FILTER (WHERE normalised_active_jobs.canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' AND LOWER(BTRIM(COALESCE(normalised_active_jobs.payload_json->>'fallback_from_delta', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'))::integer,
         COUNT(*) FILTER (WHERE normalised_active_jobs.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' AND UPPER(BTRIM(COALESCE(normalised_active_jobs.payload_json->>'projection_mode', ''))) IN ('READINESS_PATCH', 'RESERVATION_PATCH', 'POST_DRAFT_OVERLAY')) > 0,
         COUNT(*) FILTER (WHERE normalised_active_jobs.canonical_job_type = 'WORKBENCH_SESSION_CLONE_REBASE') > 0
  INTO v_active_jobs_json,
       v_active_job_count,
       v_active_queued_count,
       v_active_running_count,
       v_delta_refresh_pending_count,
       v_fallback_legacy_pending_count,
       v_patching_preview_rows,
       v_clone_rebase_pending
  FROM normalised_active_jobs;

  v_still_running := COALESCE(v_active_running_count, 0) > 0;
  v_work_queued := COALESCE(v_active_job_count, 0) > 0
    OR COALESCE(v_scope_pending_count, 0) > 0
    OR COALESCE(v_line_units_pending, 0) > 0
    OR COALESCE(v_scope_cursor_remaining, false)
    OR COALESCE(v_session_row.scope_seed_complete, false) IS NOT TRUE;

  v_line_units_total_display := GREATEST(COALESCE(v_line_units_total, 0), COALESCE(v_preview_row_count, 0));
  v_line_units_complete := GREATEST(v_line_units_total_display - COALESCE(v_line_units_pending, 0) - COALESCE(v_line_units_failed, 0), 0);
  v_line_units_complete := LEAST(v_line_units_complete, v_line_units_total_display);
  v_line_units_ready_not_materialised := GREATEST(COALESCE(v_line_units_ready, 0) - COALESCE(v_preview_row_count, 0), 0);

  IF COALESCE(v_work_queued, false) IS NOT TRUE THEN
    v_line_units_ready_not_materialised := 0;
    v_line_units_complete := COALESCE(v_line_units_total_display, 0);
  END IF;

  v_rows_available := COALESCE(v_preview_row_count, 0) > 0;
  v_selected_rows_available := COALESCE(v_selected_row_count, 0) > 0;

  IF v_replacement_required THEN
    v_progress_state := 'REBASE_REQUIRED';
    v_phase := 'REBASE_REQUIRED';
    v_status_text := 'Payment preview needs refreshing.';
    v_next_recommended_action := 'OPEN_NEW_SESSION';
  ELSIF COALESCE(v_recovery_required_count, 0) > 0 THEN
    v_progress_state := 'RECOVERY_REQUIRED';
    v_phase := 'RECOVERY_REQUIRED';
    v_status_text := 'Payment preview stopped because a candidate refresh no longer has an active job.';
    v_next_recommended_action := 'REFRESH_OR_RETRY';
  ELSIF COALESCE(v_recovery_scheduled_count, 0) > 0 THEN
    v_progress_state := 'AUTOMATIC_RECOVERY';
    v_phase := 'AUTOMATIC_RECOVERY';
    v_status_text := 'CloudTMS is reconnecting a candidate to its active refresh job.';
    v_next_recommended_action := 'WAIT_FOR_WORKER';
  ELSIF COALESCE(v_clone_rebase_pending, false) THEN
    v_progress_state := 'CLONE_REBASING';
    v_phase := 'CLONE_REBASING';
    v_status_text := 'Reusing certified payment preview rows.';
    v_next_recommended_action := 'WAIT_FOR_WORKER';
  ELSIF COALESCE(v_fallback_legacy_pending_count, 0) > 0 THEN
    v_progress_state := 'FALLING_BACK_TO_LEGACY_REFRESH';
    v_phase := 'FALLING_BACK_TO_LEGACY_REFRESH';
    v_status_text := 'Refreshing candidate through the legacy source-build path.';
    v_next_recommended_action := 'BUILD_SOURCE_CHUNK';
  ELSIF COALESCE(v_delta_refresh_pending_count, 0) > 0 THEN
    v_progress_state := 'DELTA_REFRESHING';
    v_phase := 'DELTA_REFRESHING';
    v_status_text := 'Refreshing changed candidate rows.';
    v_next_recommended_action := 'DELTA_REFRESH_CHUNK';
  ELSIF COALESCE(v_session_row.scope_seed_complete, false) IS NOT TRUE OR COALESCE(v_scope_cursor_remaining, false) THEN
    v_progress_state := 'SEEDING_SCOPE';
    v_phase := 'SEEDING_SCOPE';
    v_status_text := 'Finding candidates for the payment preview.';
    v_next_recommended_action := 'SEED_SCOPE_CHUNK';
  ELSIF COALESCE(v_patching_preview_rows, false) THEN
    v_progress_state := 'PATCHING_PREVIEW_ROWS';
    v_phase := 'PATCHING_PREVIEW_ROWS';
    v_status_text := 'Patching payment preview rows.';
    v_next_recommended_action := 'WAIT_FOR_WORKER';
  ELSIF COALESCE(v_scope_failed_count, 0) > 0 OR COALESCE(v_line_units_failed, 0) > 0 THEN
    v_progress_state := 'ERROR';
    v_phase := 'ERROR';
    v_status_text := 'Payment preview has unresolved processing errors.';
    v_next_recommended_action := 'RETRY_OR_REFRESH';
  ELSE
    v_progress_state := CASE
      WHEN COALESCE(v_work_queued, false) IS TRUE THEN COALESCE(NULLIF(BTRIM(v_session_row.progress_state), ''), 'REFRESHING_CANDIDATES')
      WHEN COALESCE(v_preview_row_count, 0) = 0 THEN 'READY_EMPTY'
      ELSE 'READY'
    END;
    v_phase := CASE WHEN COALESCE(v_work_queued, false) IS TRUE THEN v_progress_state ELSE 'READY' END;
    v_status_text := CASE WHEN COALESCE(v_work_queued, false) IS TRUE THEN 'Preparing payment preview.' ELSE 'Payment preview is ready.' END;
    v_next_recommended_action := CASE WHEN COALESCE(v_work_queued, false) IS TRUE THEN 'WAIT_FOR_WORKER' ELSE 'READ_PREVIEW_PAGE' END;
  END IF;

  v_ready := v_replacement_required IS NOT TRUE
    AND COALESCE(v_session_row.scope_seed_complete, false) IS TRUE
    AND COALESCE(v_scope_cursor_remaining, false) IS NOT TRUE
    AND COALESCE(v_active_job_count, 0) = 0
    AND COALESCE(v_scope_failed_count, 0) = 0
    AND COALESCE(v_line_units_failed, 0) = 0
    AND COALESCE(v_scope_pending_count, 0) = 0
    AND COALESCE(v_line_units_pending, 0) = 0;

  v_ready_empty := v_ready AND COALESCE(v_preview_row_count, 0) = 0;
  v_ready_for_draft := v_ready AND COALESCE(v_selected_row_count, 0) > 0;

  IF v_ready THEN
    v_line_units_ready_not_materialised := 0;
    v_line_units_complete := COALESCE(v_line_units_total_display, 0);
    v_progress_state := CASE WHEN v_ready_empty THEN 'READY_EMPTY' ELSE 'READY' END;
    v_phase := v_progress_state;
    v_status_text := CASE WHEN v_ready_empty THEN 'No payable rows found.' ELSE 'Payment preview is ready.' END;
    v_next_recommended_action := 'READ_PREVIEW_PAGE';
    v_patching_preview_rows := false;
    v_clone_rebase_pending := false;
  END IF;

  IF v_replacement_required THEN
    v_session_blocker_codes := v_session_blocker_codes || jsonb_build_array('REPLACEMENT_REQUIRED');
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('REPLACEMENT_REQUIRED');
  END IF;

  IF COALESCE(v_scope_failed_count, 0) > 0 OR COALESCE(v_line_units_failed, 0) > 0 THEN
    v_session_blocker_codes := v_session_blocker_codes || jsonb_build_array('WORKBENCH_ERRORS_PRESENT');
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_ERRORS_PRESENT');
  END IF;

  IF COALESCE(v_recovery_required_count, 0) > 0 THEN
    v_session_blocker_codes := v_session_blocker_codes || jsonb_build_array('WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB');
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB');
  END IF;

  IF COALESCE(v_recovery_scheduled_count, 0) > 0 THEN
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_AUTOMATIC_RECOVERY_PENDING');
  END IF;

  IF COALESCE(v_work_queued, false) THEN
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('WORKBENCH_REFRESH_PENDING');
  END IF;

  IF v_ready AND COALESCE(v_selected_row_count, 0) = 0 THEN
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('NO_SELECTED_ROWS');
  END IF;

  v_progress_json := public.pay_workbench_session_compact_progress_json(
    jsonb_strip_nulls(
      jsonb_build_object(
        'ok', true,
        'session_id', p_session_id::text,
        'server_utc', v_now,
        'updated_at_utc', v_now,
        'status', v_session_row.status,
        'session_status', v_session_row.status,
        'session_version', v_session_row.version,
        'progress_state', v_progress_state,
        'phase', v_phase,
        'status_text', v_status_text,
        'next_recommended_action', v_next_recommended_action,
        'ready', v_ready,
        'ready_flag', v_ready,
        'session_ready', v_ready,
        'ready_for_draft', v_ready_for_draft,
        'can_create_draft', v_ready_for_draft,
        'ready_empty', v_ready_empty,
        'rows_available', v_rows_available,
        'selected_rows_available', v_selected_rows_available,
        'has_materialised_preview_rows', v_rows_available,
        'still_running', v_still_running,
        'work_queued', v_work_queued,
        'terminal_failure', COALESCE(v_recovery_required_count, 0) > 0
          OR COALESCE(v_scope_failed_count, 0) > 0
          OR COALESCE(v_line_units_failed, 0) > 0,
        'recovery_required', COALESCE(v_recovery_required_count, 0) > 0,
        'recovery_required_count', COALESCE(v_recovery_required_count, 0),
        'recovery_scheduled', COALESCE(v_recovery_scheduled_count, 0) > 0,
        'recovery_scheduled_count', COALESCE(v_recovery_scheduled_count, 0),
        'pending_refresh', v_work_queued,
        'refresh_pending', v_work_queued,
        'preview_refresh_pending', v_work_queued,
        'preview_deferred', v_work_queued,
        'session_obsolete', v_session_obsolete,
        'obsolete', v_session_obsolete,
        'replacement_required', v_replacement_required,
        'rebase_required', v_replacement_required,
        'requires_new_session', v_replacement_required,
        'replacement_session_id', v_replacement_session_id_text,
        'replacement_available', v_replacement_session_id_text IS NOT NULL,
        'scope_seed_complete', COALESCE(v_session_row.scope_seed_complete, false),
        'scope_cursor_remaining', v_scope_cursor_remaining
      )
      || jsonb_build_object(
        'scope_total_count', v_scope_total_count,
        'scope_seeded_count', v_scope_seeded_count,
        'scope_ready_count', v_scope_ready_count,
        'scope_pending_count', v_scope_pending_count,
        'scope_failed_count', v_scope_failed_count,
        'total_candidates', v_scope_total_count,
        'total_count', v_scope_total_count,
        'completed_candidates', LEAST(v_scope_ready_count, v_scope_total_count),
        'completed_count', LEAST(v_scope_ready_count, v_scope_total_count),
        'ready_candidates', LEAST(v_scope_ready_count, v_scope_total_count),
        'pending_candidates', v_scope_pending_count,
        'pending_count', v_scope_pending_count,
        'failed_candidates', v_scope_failed_count,
        'failed_count', v_scope_failed_count,
        'line_units_total', v_line_units_total_display,
        'line_units_complete', v_line_units_complete,
        'line_units_ready', v_line_units_ready,
        'line_units_ready_not_materialised', v_line_units_ready_not_materialised,
        'line_units_pending', v_line_units_pending,
        'line_units_failed', v_line_units_failed,
        'preview_row_count', v_preview_row_count,
        'selected_row_count', v_selected_row_count,
        'selected_eligible_ready_row_count', v_selected_row_count,
        'section_counts_json', COALESCE(v_section_counts_json, '{}'::jsonb),
        'section_counts', COALESCE(v_section_counts_json, '{}'::jsonb),
        'candidate_sample_rows_json', COALESCE(v_candidate_sample_rows_json, '[]'::jsonb),
        'pending_owner_failures', COALESCE(v_pending_owner_failures_json, '[]'::jsonb),
        'active_jobs', COALESCE(v_active_jobs_json, '[]'::jsonb),
        'pending_job_ids_json', CASE WHEN COALESCE(v_active_job_count, 0) = 0 THEN '[]'::jsonb ELSE '[]'::jsonb END,
        'delta_refresh_pending_count', COALESCE(v_delta_refresh_pending_count, 0),
        'fallback_legacy_pending_count', COALESCE(v_fallback_legacy_pending_count, 0),
        'patching_preview_rows', COALESCE(v_patching_preview_rows, false),
        'clone_rebase_pending', COALESCE(v_clone_rebase_pending, false)
      )
      || jsonb_build_object(
        'candidate_counts', jsonb_build_object(
          'total', v_scope_total_count,
          'ready', LEAST(v_scope_ready_count, v_scope_total_count),
          'terminal_materialised', LEAST(v_scope_ready_count, v_scope_total_count),
          'pending', v_scope_pending_count,
          'processing', v_scope_pending_count,
          'materialisation_pending', 0,
          'dirty', v_scope_dirty_count,
          'failed', v_scope_failed_count,
          'unknown', 0,
          'unseeded', GREATEST(v_scope_total_count - v_scope_seeded_count, 0)
        ),
        'line_counts', jsonb_build_object(
          'total', v_line_units_total_display,
          'complete', v_line_units_complete,
          'materialised_or_skipped', v_line_units_complete,
          'pending', v_line_units_pending,
          'ready_not_materialised', v_line_units_ready_not_materialised,
          'failed', v_line_units_failed,
          'unknown', 0
        ),
        'job_counts', jsonb_build_object(
          'queued', COALESCE(v_active_queued_count, 0),
          'running', COALESCE(v_active_running_count, 0),
          'unresolved_failed', 0,
          'unresolved_dead', 0
        ),
        'source_build_counts', jsonb_build_object(
          'fallback_legacy_pending', COALESCE(v_fallback_legacy_pending_count, 0),
          'delta_refresh_pending', COALESCE(v_delta_refresh_pending_count, 0)
        ),
        'session_blocker_codes', COALESCE(v_session_blocker_codes, '[]'::jsonb),
        'draft_blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
        'blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
        'blocker_counts', jsonb_build_object(
          'pending_scope_without_active_job', COALESCE(v_recovery_required_count, 0),
          'automatic_recovery_pending', COALESCE(v_recovery_scheduled_count, 0)
        ),
        'stored_ready_mismatch', false,
        'read_only', v_replacement_required
          OR COALESCE(v_recovery_required_count, 0) > 0
          OR COALESCE(v_scope_failed_count, 0) > 0
          OR COALESCE(v_line_units_failed, 0) > 0,
        'counter_recompute_reason', COALESCE(NULLIF(BTRIM(p_reason), ''), 'AUTHORITATIVE_COUNTER_RECOMPUTE')
      )
    ),
    true
  );

  IF COALESCE(p_apply, true) IS TRUE THEN
    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET scope_total_count = v_scope_total_count,
        scope_seeded_count = v_scope_seeded_count,
        scope_ready_count = v_scope_ready_count,
        scope_pending_count = v_scope_pending_count,
        scope_failed_count = v_scope_failed_count,
        line_units_total = v_line_units_total,
        line_units_ready = v_line_units_ready,
        line_units_pending = v_line_units_pending,
        line_units_failed = v_line_units_failed,
        preview_row_count = v_preview_row_count,
        selected_row_count = v_selected_row_count,
        section_counts_json = COALESCE(v_section_counts_json, '{}'::jsonb),
        candidate_sample_rows_json = COALESCE(v_candidate_sample_rows_json, '[]'::jsonb),
        progress_state = v_progress_state,
        progress_json = CASE WHEN COALESCE(p_write_progress_json, true) IS TRUE THEN v_progress_json ELSE session_update.progress_json END,
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = p_session_id
    RETURNING session_update.*
    INTO v_session_row;
  END IF;

  RETURN v_progress_json || jsonb_build_object(
    'applied', COALESCE(p_apply, true),
    'progress_json_written', COALESCE(p_apply, true) IS TRUE AND COALESCE(p_write_progress_json, true) IS TRUE
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(
  p_session_id uuid DEFAULT NULL::uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 10,
  p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone,
  p_reason text DEFAULT 'PENDING_SCOPE_OWNER_REPAIR'::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 10), 1), 25);
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'PENDING_SCOPE_OWNER_REPAIR');
  v_candidate record;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_owner public.banking_pay_workbench_jobs%ROWTYPE;
  v_success public.banking_pay_workbench_jobs%ROWTYPE;
  v_success_run_id uuid := NULL::uuid;
  v_success_source_change_seq bigint := 0;
  v_active public.banking_pay_workbench_jobs%ROWTYPE;
  v_live_change_seq bigint := 0;
  v_owner_canonical_type text := NULL::text;
  v_owner_valid boolean := false;
  v_owner_reason text := NULL::text;
  v_enqueue_payload jsonb := '{}'::jsonb;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_success_result jsonb := '{}'::jsonb;
  v_successor_job_id uuid := NULL::uuid;
  v_successor_run_id_text text := NULL::text;
  v_successor_valid boolean := false;
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;
  v_linked_timesheet_ids jsonb := '[]'::jsonb;
  v_pay_channel_scope text := 'ALL';
  v_repaired_count integer := 0;
  v_reconciled_count integer := 0;
  v_rebound_count integer := 0;
  v_enqueued_count integer := 0;
  v_failed_closed_count integer := 0;
  v_skipped_count integer := 0;
  v_result_rows jsonb := '[]'::jsonb;
  v_action text := NULL::text;
  v_safe_error_code text := NULL::text;
  v_safe_error_message text := NULL::text;
  v_audit_failed boolean := false;
BEGIN
  FOR v_candidate IN
    SELECT scope_row.session_id, scope_row.candidate_id
    FROM public.banking_pay_workbench_session_scope AS scope_row
    JOIN public.banking_pay_workbench_sessions AS session_row
      ON session_row.id = scope_row.session_id
    LEFT JOIN public.banking_pay_workbench_jobs AS owner_job
      ON owner_job.id = scope_row.pending_job_id
    WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
      AND session_row.discarded_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SOURCE_BUILD_PENDING'
      AND (p_session_id IS NULL OR scope_row.session_id = p_session_id)
      AND (p_candidate_id IS NULL OR scope_row.candidate_id = p_candidate_id)
      AND (
        scope_row.pending_job_id IS NULL
        OR owner_job.id IS NULL
        OR UPPER(BTRIM(COALESCE(owner_job.status, ''))) NOT IN ('QUEUED', 'RUNNING')
        OR owner_job.session_id IS DISTINCT FROM scope_row.session_id
        OR owner_job.candidate_id IS DISTINCT FROM scope_row.candidate_id
        OR (
          CASE
            WHEN UPPER(BTRIM(COALESCE(owner_job.job_type, ''))) IN (
              'WORKBENCH_CANDIDATE_SOURCE_BUILD',
              'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
              'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
              'CANDIDATE_SOURCE_BUILD',
              'CANDIDATE_SOURCE_BUILD_CHUNK',
              'SOURCE_BUILD',
              'SOURCE_BUILD_PAGE'
            ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            ELSE UPPER(BTRIM(COALESCE(owner_job.job_type, '')))
          END
        ) <> 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        OR CASE
             WHEN COALESCE(owner_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
               THEN (owner_job.payload_json->>'session_version')::bigint
             ELSE NULL::bigint
           END IS DISTINCT FROM session_row.version
        OR CASE
             WHEN COALESCE(owner_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
               THEN (owner_job.payload_json->>'source_change_seq')::bigint
             ELSE NULL::bigint
           END IS NULL
        OR COALESCE(owner_job.payload_json->>'source_build_run_id', '') !~*
           '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
    ORDER BY scope_row.updated_at_utc ASC NULLS FIRST, scope_row.session_id, scope_row.candidate_id
    LIMIT v_limit
  LOOP
    v_action := NULL::text;
    v_safe_error_code := NULL::text;
    v_safe_error_message := NULL::text;
    v_successor_job_id := NULL::uuid;
    v_successor_run_id_text := NULL::text;
    v_successor_valid := false;
    v_audit_failed := false;

    SELECT session_row.*
    INTO v_session
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_candidate.session_id
    FOR UPDATE;

    IF NOT FOUND
       OR UPPER(BTRIM(COALESCE(v_session.status, ''))) <> 'OPEN'
       OR v_session.discarded_at_utc IS NOT NULL THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    SELECT scope_row.*
    INTO v_scope
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = v_candidate.session_id
      AND scope_row.candidate_id = v_candidate.candidate_id
    FOR UPDATE;

    IF NOT FOUND OR UPPER(BTRIM(COALESCE(v_scope.status, ''))) <> 'SOURCE_BUILD_PENDING' THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    v_live_change_seq := 0;
    SELECT COALESCE(change_counter.seq, 0)
    INTO v_live_change_seq
    FROM public.app_change_counters AS change_counter
    WHERE change_counter.entity_key = 'pay_candidate:' || v_scope.candidate_id::text;
    v_live_change_seq := COALESCE(v_live_change_seq, 0);

    v_owner := NULL::public.banking_pay_workbench_jobs;
    IF v_scope.pending_job_id IS NOT NULL THEN
      SELECT owner_job.*
      INTO v_owner
      FROM public.banking_pay_workbench_jobs AS owner_job
      WHERE owner_job.id = v_scope.pending_job_id
      FOR UPDATE;
    END IF;

    v_owner_canonical_type := CASE
      WHEN UPPER(BTRIM(COALESCE(v_owner.job_type, ''))) IN (
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
        'CANDIDATE_SOURCE_BUILD',
        'CANDIDATE_SOURCE_BUILD_CHUNK',
        'SOURCE_BUILD',
        'SOURCE_BUILD_PAGE'
      ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      ELSE UPPER(BTRIM(COALESCE(v_owner.job_type, '')))
    END;

    v_owner_valid := v_owner.id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(v_owner.status, ''))) IN ('QUEUED', 'RUNNING')
      AND v_owner.session_id = v_scope.session_id
      AND v_owner.candidate_id = v_scope.candidate_id
      AND v_owner_canonical_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND CASE
            WHEN COALESCE(v_owner.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
              THEN (v_owner.payload_json->>'session_version')::bigint
            ELSE NULL::bigint
          END = v_session.version
      AND CASE
            WHEN COALESCE(v_owner.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_owner.payload_json->>'source_change_seq')::bigint
            ELSE NULL::bigint
          END >= v_live_change_seq
      AND COALESCE(v_owner.payload_json->>'source_build_run_id', '') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    IF v_owner_valid THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    v_owner_reason := CASE
      WHEN v_scope.pending_job_id IS NULL THEN 'PENDING_JOB_ID_MISSING'
      WHEN v_owner.id IS NULL THEN 'PENDING_JOB_ROW_MISSING'
      WHEN UPPER(BTRIM(COALESCE(v_owner.status, ''))) NOT IN ('QUEUED', 'RUNNING') THEN 'PENDING_JOB_TERMINAL'
      WHEN v_owner.session_id IS DISTINCT FROM v_scope.session_id
        OR v_owner.candidate_id IS DISTINCT FROM v_scope.candidate_id THEN 'PENDING_JOB_SCOPE_MISMATCH'
      WHEN v_owner_canonical_type <> 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'PENDING_JOB_TYPE_MISMATCH'
      WHEN COALESCE(v_owner.payload_json->>'source_build_run_id', '') !~*
           '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN 'PENDING_JOB_RUN_ID_INVALID'
      WHEN COALESCE(v_owner.payload_json->>'session_version', '') !~ '^[0-9]{1,18}$'
        OR (v_owner.payload_json->>'session_version')::bigint IS DISTINCT FROM v_session.version
        THEN 'PENDING_JOB_SESSION_VERSION_STALE'
      ELSE 'PENDING_JOB_SOURCE_CHANGE_SEQ_STALE'
    END;

    v_success := NULL::public.banking_pay_workbench_jobs;
    SELECT successful_job.*
    INTO v_success
    FROM public.banking_pay_workbench_jobs AS successful_job
    WHERE successful_job.session_id = v_scope.session_id
      AND successful_job.candidate_id = v_scope.candidate_id
      AND UPPER(BTRIM(COALESCE(successful_job.status, ''))) = 'SUCCEEDED'
      AND UPPER(BTRIM(COALESCE(successful_job.job_type, ''))) IN (
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
        'CANDIDATE_SOURCE_BUILD',
        'CANDIDATE_SOURCE_BUILD_CHUNK',
        'SOURCE_BUILD',
        'SOURCE_BUILD_PAGE'
      )
      AND CASE
            WHEN COALESCE(successful_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
              THEN (successful_job.payload_json->>'session_version')::bigint
            ELSE NULL::bigint
          END = v_session.version
      AND CASE
            WHEN COALESCE(successful_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (successful_job.payload_json->>'source_change_seq')::bigint
            ELSE NULL::bigint
          END >= v_live_change_seq
      AND COALESCE(successful_job.payload_json->>'source_build_run_id', '') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_candidate_source_lines AS successful_source
        WHERE successful_source.session_id = v_scope.session_id
          AND successful_source.candidate_id = v_scope.candidate_id
          AND successful_source.source_build_run_id::text = successful_job.payload_json->>'source_build_run_id'
          AND successful_source.session_version = v_session.version
          AND successful_source.source_change_seq >= v_live_change_seq
          AND UPPER(BTRIM(COALESCE(successful_source.status, ''))) IN ('CURRENT', 'DIRTY')
      )
    ORDER BY (successful_job.payload_json->>'source_change_seq')::bigint DESC,
             successful_job.completed_at_utc DESC NULLS LAST,
             successful_job.id DESC
    LIMIT 1;

    IF FOUND THEN
      v_success_run_id := (v_success.payload_json->>'source_build_run_id')::uuid;
      v_success_source_change_seq := (v_success.payload_json->>'source_change_seq')::bigint;
      v_success_result := public.pay_workbench_reconcile_successful_source_build(
        p_session_id => v_scope.session_id,
        p_candidate_id => v_scope.candidate_id,
        p_source_build_run_id => v_success_run_id,
        p_source_change_seq => v_success_source_change_seq,
        p_session_version => v_session.version,
        p_success_job_id => v_success.id,
        p_refresh_scope_kind => COALESCE(NULLIF(BTRIM(v_success.payload_json->>'refresh_scope_kind'), ''), 'CANDIDATE_FULL_LIVE'),
        p_targeted_timesheet_ids => CASE WHEN jsonb_typeof(v_success.payload_json->'targeted_timesheet_ids') = 'array' THEN v_success.payload_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END,
        p_linked_timesheet_ids => CASE WHEN jsonb_typeof(v_success.payload_json->'linked_timesheet_ids') = 'array' THEN v_success.payload_json->'linked_timesheet_ids' ELSE '[]'::jsonb END,
        p_recompute_session_progress => true
      );
      v_action := 'RECONCILED_SUCCESSFUL_BUILD';
      v_reconciled_count := v_reconciled_count + 1;
      v_repaired_count := v_repaired_count + 1;
      v_successor_job_id := v_success.id;
    ELSE
      v_active := NULL::public.banking_pay_workbench_jobs;
      SELECT active_job.*
      INTO v_active
      FROM public.banking_pay_workbench_jobs AS active_job
      WHERE active_job.id IS DISTINCT FROM v_scope.pending_job_id
        AND active_job.session_id = v_scope.session_id
        AND active_job.candidate_id = v_scope.candidate_id
        AND UPPER(BTRIM(COALESCE(active_job.status, ''))) IN ('QUEUED', 'RUNNING')
        AND UPPER(BTRIM(COALESCE(active_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        )
        AND CASE
              WHEN COALESCE(active_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                THEN (active_job.payload_json->>'session_version')::bigint
              ELSE NULL::bigint
            END = v_session.version
        AND CASE
              WHEN COALESCE(active_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                THEN (active_job.payload_json->>'source_change_seq')::bigint
              ELSE NULL::bigint
            END >= v_live_change_seq
        AND COALESCE(active_job.payload_json->>'source_build_run_id', '') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ORDER BY CASE WHEN UPPER(BTRIM(COALESCE(active_job.status, ''))) = 'RUNNING' THEN 0 ELSE 1 END,
               (active_job.payload_json->>'source_change_seq')::bigint DESC,
               active_job.created_at_utc ASC,
               active_job.id ASC
      LIMIT 1
      FOR UPDATE;

      IF FOUND THEN
        UPDATE public.banking_pay_workbench_session_scope AS rebound_scope
        SET pending_job_id = v_active.id,
            status = 'SOURCE_BUILD_PENDING',
            dirty = true,
            error_json = NULL::jsonb,
            updated_at_utc = v_now
        WHERE rebound_scope.id = v_scope.id
          AND (
            rebound_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id
            OR rebound_scope.pending_job_id IS NULL
          );
        v_action := 'REBOUND_ACTIVE_SUCCESSOR';
        v_rebound_count := v_rebound_count + 1;
        v_repaired_count := v_repaired_count + 1;
        v_successor_job_id := v_active.id;
      ELSIF v_owner.id IS NOT NULL
            AND COALESCE(v_owner.attempt_count, 0) >= COALESCE(v_owner.max_attempts, 8) THEN
        v_action := 'FAILED_CLOSED_MAX_ATTEMPTS';
        v_safe_error_code := 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB';
        v_safe_error_message := 'Candidate refresh could not be recovered because all job attempts were used.';
        UPDATE public.banking_pay_workbench_session_scope AS failed_scope
        SET status = 'SOURCE_BUILD_ERROR',
            pending_job_id = NULL::uuid,
            dirty = true,
            error_json = jsonb_build_object(
              'code', v_safe_error_code,
              'message', v_safe_error_message,
              'job_id', v_owner.id::text,
              'attempt_count', COALESCE(v_owner.attempt_count, 0),
              'max_attempts', COALESCE(v_owner.max_attempts, 8),
              'automatic_recovery_scheduled', false,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            ),
            updated_at_utc = v_now
        WHERE failed_scope.id = v_scope.id
          AND failed_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id;
        v_failed_closed_count := v_failed_closed_count + 1;
        v_repaired_count := v_repaired_count + 1;
      ELSE
        v_refresh_scope_kind := COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_owner.payload_json->>'refresh_scope_kind', ''))), ''), 'CANDIDATE_FULL_LIVE');
        IF v_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
          v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
        END IF;
        v_targeted_timesheet_ids := CASE
          WHEN jsonb_typeof(v_owner.payload_json->'targeted_timesheet_ids') = 'array'
            THEN v_owner.payload_json->'targeted_timesheet_ids'
          ELSE '[]'::jsonb
        END;
        v_linked_timesheet_ids := CASE
          WHEN jsonb_typeof(v_owner.payload_json->'linked_timesheet_ids') = 'array'
            THEN v_owner.payload_json->'linked_timesheet_ids'
          ELSE '[]'::jsonb
        END;
        v_pay_channel_scope := COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_owner.payload_json->>'pay_channel_scope', ''))), ''), 'ALL');
        IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
          v_pay_channel_scope := 'ALL';
        END IF;

        BEGIN
          v_enqueue_payload := jsonb_build_object(
            'session_id', v_session.id::text,
            'source_session_id', v_session.id::text,
            'candidate_id', v_scope.candidate_id::text,
            'session_version', v_session.version,
            'source_change_seq', v_live_change_seq,
            'refresh_scope_kind', v_refresh_scope_kind,
            'targeted_timesheet_ids', v_targeted_timesheet_ids,
            'linked_timesheet_ids', v_linked_timesheet_ids,
            'pay_channel_scope', v_pay_channel_scope,
            'force_legacy', true,
            'force_broad_legacy', v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE',
            'owner_repair', true,
            'owner_repair_reason', v_owner_reason,
            'replaces_job_id', CASE WHEN v_owner.id IS NULL THEN NULL ELSE v_owner.id::text END,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          );
          v_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
            p_snapshot_run_id => v_session.source_snapshot_run_id,
            p_candidate_id => v_scope.candidate_id,
            p_reason => v_reason,
            p_actor_user_id => v_session.actor_user_id,
            p_payload_json => v_enqueue_payload
          );

          IF COALESCE(v_enqueue_result->>'job_id', '') ~*
             '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            v_successor_job_id := (v_enqueue_result->>'job_id')::uuid;
          END IF;

          SELECT successor_job.payload_json->>'source_build_run_id'
          INTO v_successor_run_id_text
          FROM public.banking_pay_workbench_jobs AS successor_job
          JOIN public.banking_pay_workbench_session_scope AS successor_scope
            ON successor_scope.session_id = successor_job.session_id
           AND successor_scope.candidate_id = successor_job.candidate_id
          WHERE successor_job.id = v_successor_job_id
            AND successor_job.session_id = v_session.id
            AND successor_job.candidate_id = v_scope.candidate_id
            AND UPPER(BTRIM(COALESCE(successor_job.status, ''))) IN ('QUEUED', 'RUNNING')
            AND UPPER(BTRIM(COALESCE(successor_job.job_type, ''))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            AND successor_scope.pending_job_id = successor_job.id
            AND UPPER(BTRIM(COALESCE(successor_scope.status, ''))) = 'SOURCE_BUILD_PENDING'
            AND CASE
                  WHEN COALESCE(successor_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                    THEN (successor_job.payload_json->>'session_version')::bigint
                  ELSE NULL::bigint
                END = v_session.version
            AND CASE
                  WHEN COALESCE(successor_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                    THEN (successor_job.payload_json->>'source_change_seq')::bigint
                  ELSE NULL::bigint
                END >= v_live_change_seq
            AND COALESCE(successor_job.payload_json->>'source_build_run_id', '') ~*
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
          v_successor_valid := FOUND;

          IF v_successor_valid IS NOT TRUE THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_OWNER_REPAIR_SUCCESSOR_INVALID'
              USING ERRCODE = 'P0001';
          END IF;

          IF v_owner.id IS NOT NULL THEN
            UPDATE public.banking_pay_workbench_jobs AS repaired_old_job
            SET payload_json = COALESCE(repaired_old_job.payload_json, '{}'::jsonb)
              || jsonb_build_object(
                'owner_repair_applied', true,
                'owner_repair_reason', v_owner_reason,
                'owner_repair_successor_job_id', v_successor_job_id::text,
                'owner_repair_successor_source_build_run_id', v_successor_run_id_text,
                'owner_repair_at_utc', v_now::text
              ),
                updated_at_utc = v_now
            WHERE repaired_old_job.id = v_owner.id;
          END IF;

          v_action := 'ENQUEUED_CANONICAL_SUCCESSOR';
          v_enqueued_count := v_enqueued_count + 1;
          v_repaired_count := v_repaired_count + 1;
        EXCEPTION WHEN OTHERS THEN
          v_action := 'FAILED_CLOSED_REPAIR_ERROR';
          v_safe_error_code := 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB';
          v_safe_error_message := 'Candidate refresh could not be scheduled automatically. Refresh Banking Pay or retry the candidate.';
          UPDATE public.banking_pay_workbench_session_scope AS failed_repair_scope
          SET status = 'SOURCE_BUILD_ERROR',
              pending_job_id = NULL::uuid,
              dirty = true,
              error_json = jsonb_build_object(
                'code', v_safe_error_code,
                'message', v_safe_error_message,
                'job_id', CASE WHEN v_owner.id IS NULL THEN NULL ELSE v_owner.id::text END,
                'automatic_recovery_scheduled', false,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              ),
              updated_at_utc = v_now
          WHERE failed_repair_scope.id = v_scope.id
            AND failed_repair_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id;
          v_failed_closed_count := v_failed_closed_count + 1;
          v_repaired_count := v_repaired_count + 1;
        END;
      END IF;
    END IF;

    BEGIN
      PERFORM public._audit_insert(
        'banking_pay_workbench_session_scope',
        v_scope.candidate_id::text,
        'PENDING_SOURCE_BUILD_OWNER_REPAIRED',
        jsonb_build_object(
          'session_id', v_scope.session_id::text,
          'candidate_id', v_scope.candidate_id::text,
          'old_pending_job_id', CASE WHEN v_scope.pending_job_id IS NULL THEN NULL ELSE v_scope.pending_job_id::text END,
          'owner_failure_reason', v_owner_reason
        ),
        jsonb_build_object(
          'session_id', v_scope.session_id::text,
          'candidate_id', v_scope.candidate_id::text,
          'action', v_action,
          'successor_job_id', CASE WHEN v_successor_job_id IS NULL THEN NULL ELSE v_successor_job_id::text END,
          'automatic_recovery_scheduled', v_action IN ('REBOUND_ACTIVE_SUCCESSOR', 'ENQUEUED_CANONICAL_SUCCESSOR'),
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        ),
        'PAY_WORKBENCH_PENDING_SOURCE_BUILD_OWNER_REPAIR',
        v_session.actor_user_id
      );
    EXCEPTION WHEN OTHERS THEN
      v_audit_failed := true;
    END;

    v_result_rows := v_result_rows || jsonb_build_array(
      jsonb_strip_nulls(jsonb_build_object(
        'session_id', v_scope.session_id::text,
        'candidate_id', v_scope.candidate_id::text,
        'old_pending_job_id', CASE WHEN v_scope.pending_job_id IS NULL THEN NULL ELSE v_scope.pending_job_id::text END,
        'owner_failure_reason', v_owner_reason,
        'action', v_action,
        'successor_job_id', CASE WHEN v_successor_job_id IS NULL THEN NULL ELSE v_successor_job_id::text END,
        'automatic_recovery_scheduled', v_action IN ('REBOUND_ACTIVE_SUCCESSOR', 'ENQUEUED_CANONICAL_SUCCESSOR'),
        'failure_code', v_safe_error_code,
        'message', v_safe_error_message,
        'audit_failed', v_audit_failed
      ))
    );
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'repair_code', 'PAY_WORKBENCH_PENDING_SOURCE_BUILD_OWNER_REPAIR',
    'examined_count', jsonb_array_length(v_result_rows) + v_skipped_count,
    'repaired_count', v_repaired_count,
    'reconciled_count', v_reconciled_count,
    'rebound_count', v_rebound_count,
    'enqueued_count', v_enqueued_count,
    'failed_closed_count', v_failed_closed_count,
    'skipped_count', v_skipped_count,
    'automatic_recovery_scheduled', (v_rebound_count + v_enqueued_count) > 0,
    'results', v_result_rows,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_get_progress_light(uuid)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_progress_light(uuid)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_progress_light(uuid)
  TO postgres, authenticated, service_role;
COMMENT ON FUNCTION public.pay_workbench_session_get_progress_light(uuid)
  IS NULL;

ALTER FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
  TO postgres, authenticated, service_role;
COMMENT ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
  IS NULL;

ALTER FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
  TO postgres, service_role;
COMMENT ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
  IS NULL;

DO $guard$
DECLARE
  v_actual text;
BEGIN
  SELECT md5(pg_get_functiondef(
    'public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamp with time zone,text)'::regprocedure
  )) INTO v_actual;
  IF v_actual IS DISTINCT FROM '977f2aa68b33a10649c69e308cf86e16' THEN
    RAISE EXCEPTION 'PRE_DELTA_RESTORE_HELPER_HASH_MISMATCH' USING ERRCODE = 'P0001';
  END IF;

  SELECT md5(pg_get_functiondef(
    'public.pay_workbench_session_get_progress_light(uuid)'::regprocedure
  )) INTO v_actual;
  IF v_actual IS DISTINCT FROM '64a227e561acf1be8bf434b13dd253c7' THEN
    RAISE EXCEPTION 'PRE_DELTA_RESTORE_PROGRESS_LIGHT_HASH_MISMATCH' USING ERRCODE = 'P0001';
  END IF;

  SELECT md5(pg_get_functiondef(
    'public.pay_workbench_session_recompute_progress_counters(uuid,boolean,text,boolean)'::regprocedure
  )) INTO v_actual;
  IF v_actual IS DISTINCT FROM '0830bcf4a7895de0cfee6960120580df' THEN
    RAISE EXCEPTION 'PRE_DELTA_RESTORE_RECOMPUTE_HASH_MISMATCH' USING ERRCODE = 'P0001';
  END IF;
END
$guard$;

COMMIT;
