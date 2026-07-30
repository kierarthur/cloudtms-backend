CREATE OR REPLACE FUNCTION public.pay_workbench_claim_due_jobs(p_limit integer DEFAULT 25, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_session_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_allowed_job_types text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_limit integer := GREATEST(1, LEAST(COALESCE(p_limit, 25), 500));
  v_now timestamptz := now();
  v_cutoff timestamptz := COALESCE(p_now_utc, v_now);
  v_stale_running_seconds integer := 180;
  v_configured_lease_seconds integer := NULL::integer;
  v_stale_cutoff timestamptz := NULL::timestamptz;
  v_recovered_stale jsonb := '[]'::jsonb;
  v_dead_stale jsonb := '[]'::jsonb;
  v_claimed jsonb := '[]'::jsonb;
  v_recovered_stale_count integer := 0;
  v_dead_stale_count integer := 0;
  v_claimed_count integer := 0;
  v_claimed_delta_refresh_count integer := 0;
  v_claimed_clone_rebase_count integer := 0;
  v_stale_row record;
  v_stale_before_json jsonb := NULL;
  v_stale_after_json jsonb := '{}'::jsonb;
  v_stale_error_json jsonb := NULL;
  v_claimed_row record;
  v_stale_job_source_change_seq bigint := 0;
  v_stale_job_session_version bigint := 0;
  v_stale_live_candidate_change_seq bigint := 0;
  v_stale_snapshot_state_status text := NULL;
  v_stale_snapshot_state_source_change_seq bigint := 0;
  v_stale_session_status text := NULL;
  v_stale_session_version bigint := 0;
  v_stale_session_candidate_status text := NULL;
  v_stale_session_candidate_source_change_seq bigint := 0;
  v_stale_session_candidate_session_version bigint := 0;
  v_stale_other_active_job_id uuid := NULL::uuid;
  v_stale_completed_equivalent_id uuid := NULL::uuid;
  v_stale_obsolete boolean := false;
  v_stale_obsolete_reason text := NULL;
  v_stale_canonical_job_type text := NULL::text;
  v_stale_failed_line_work_count integer := 0;
  v_stale_failed_source_row_count integer := 0;
  v_allowed_job_types text[] := NULL::text[];
  v_stale_recovery_limit integer := 0;
  v_preclaim_due_queued_count integer := 0;
  v_preclaim_due_queued_sample jsonb := '[]'::jsonb;
  v_claim_lock_contention_detected boolean := false;
  v_claim_lock_contention_count integer := 0;
  v_delta_queued_coalesced_count integer := 0;
  v_delta_queued_coalesced_hot_key_count integer := 0;
  v_projection_lifecycle_repair_json jsonb := '{}'::jsonb;
  v_invalid_source_build_poison_repair_json jsonb := '{}'::jsonb;
  v_orphaned_pending_source_build_repair_json jsonb := '{}'::jsonb;
  v_stale_source_build_run_id_text text := NULL::text;
  v_stale_invalid_source_build_without_run_id boolean := false;
  v_delta_stale_continuation_superseded_count integer := 0;
  v_delta_stale_continuation_sample jsonb := '[]'::jsonb;
  v_delta_stale_projection_terminalised_count integer := 0;
  v_delta_stale_projection_terminalisation_sample jsonb := '[]'::jsonb;
  v_claim_mismatch_detected boolean := false;
  v_claim_mismatch_reason text := NULL::text;
  v_claim_mismatch_json jsonb := '{}'::jsonb;
  v_claimable_sample jsonb := '[]'::jsonb;
  v_claim_reject_summary jsonb := '{}'::jsonb;
  v_advisory_lock_reject_count integer := 0;
  v_candidate_serial_reject_count integer := 0;
  v_skip_locked_or_concurrent_lock_count integer := 0;
  v_allowed_job_types_reject_count integer := 0;
  v_status_changed_before_claim_count integer := 0;
  v_obsolete_or_superseded_reject_count integer := 0;
  v_active_projection_reject_count integer := 0;
  v_unknown_claim_query_mismatch_count integer := 0;
BEGIN
  IF p_allowed_job_types IS NOT NULL THEN
    SELECT ARRAY(
      SELECT DISTINCT normalised_allowed.job_type
      FROM (
        SELECT
          CASE
            WHEN normalised_raw.raw_value_text IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
              THEN 'WORKBENCH_SESSION_SCOPE_SEED'
            WHEN normalised_raw.raw_value_text IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
              THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            WHEN normalised_raw.raw_value_text IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
              THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
            WHEN normalised_raw.raw_value_text IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
              THEN 'WORKBENCH_SESSION_CLONE_REBASE'
            WHEN normalised_raw.raw_value_text IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
              THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
            WHEN normalised_raw.raw_value_text IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
              THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
            WHEN normalised_raw.raw_value_text IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
              THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
            ELSE normalised_raw.raw_value_text
          END AS job_type
        FROM unnest(p_allowed_job_types) AS allowed_job_type(raw_value)
        CROSS JOIN LATERAL (
          SELECT UPPER(BTRIM(COALESCE(allowed_job_type.raw_value, ''))) AS raw_value_text
        ) AS normalised_raw
        WHERE normalised_raw.raw_value_text <> ''
      ) AS normalised_allowed
      WHERE normalised_allowed.job_type <> ''
      ORDER BY normalised_allowed.job_type
    )
    INTO v_allowed_job_types;

    IF COALESCE(array_length(v_allowed_job_types, 1), 0) = 0 THEN
      v_allowed_job_types := ARRAY[]::text[];
    END IF;
  ELSE
    v_allowed_job_types := NULL::text[];
  END IF;

  IF p_session_id IS NOT NULL
     AND (
       v_allowed_job_types IS NULL
       OR 'WORKBENCH_SESSION_SCOPE_SEED' = ANY(v_allowed_job_types)
       OR 'WORKBENCH_CANDIDATE_SOURCE_BUILD' = ANY(v_allowed_job_types)
       OR 'WORKBENCH_CANDIDATE_DELTA_REFRESH' = ANY(v_allowed_job_types)
       OR 'WORKBENCH_SESSION_CLONE_REBASE' = ANY(v_allowed_job_types)
       OR 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' = ANY(v_allowed_job_types)
       OR 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS' = ANY(v_allowed_job_types)
       OR 'WORKBENCH_PREVIEW_ROWS_MATERIALISE' = ANY(v_allowed_job_types)
     ) THEN
    v_stale_running_seconds := 25;
  ELSE
    v_stale_running_seconds := 180;
  END IF;

  SELECT sd.banking_pay_workbench_db_worker_lease_seconds
  INTO v_configured_lease_seconds
  FROM public.settings_defaults AS sd
  WHERE sd.id = 1
  LIMIT 1;

  IF v_configured_lease_seconds IS NOT NULL THEN
    v_stale_running_seconds := LEAST(GREATEST(v_configured_lease_seconds, 25), 3600);
  END IF;

  v_stale_cutoff := v_cutoff - make_interval(secs => v_stale_running_seconds);
  v_stale_recovery_limit := LEAST(v_limit, 3);

  IF v_allowed_job_types IS NULL
     OR 'WORKBENCH_CANDIDATE_DELTA_REFRESH' = ANY(v_allowed_job_types) THEN
    v_projection_lifecycle_repair_json := public.pay_workbench_projection_lifecycle_repair(
      p_session_id => p_session_id,
      p_candidate_id => p_candidate_id,
      p_safe_age_seconds => GREATEST(v_stale_running_seconds, 300),
      p_limit => LEAST(GREATEST(v_limit, 1), 100),
      p_reason => 'CLAIM_DUE_JOBS_PROJECTION_LIFECYCLE_REPAIR'
    );
  END IF;

  IF v_allowed_job_types IS NULL
     OR 'WORKBENCH_CANDIDATE_SOURCE_BUILD' = ANY(v_allowed_job_types)
     OR 'WORKBENCH_CANDIDATE_LINE_WORK_SEED' = ANY(v_allowed_job_types) THEN
    IF to_regprocedure('public.pay_workbench_repair_invalid_source_build_poison(uuid,uuid,integer,timestamp with time zone,text)') IS NOT NULL THEN
      BEGIN
        EXECUTE 'SELECT public.pay_workbench_repair_invalid_source_build_poison($1, $2, $3, $4, $5)'
        INTO v_invalid_source_build_poison_repair_json
        USING
          p_session_id,
          p_candidate_id,
          LEAST(GREATEST(v_limit, 1), 10),
          v_now,
          'CLAIM_DUE_JOBS_INVALID_SOURCE_BUILD_POISON_REPAIR';
      EXCEPTION WHEN OTHERS THEN
        v_invalid_source_build_poison_repair_json := jsonb_build_object(
          'ok', false,
          'deferred', true,
          'reason', 'CLAIM_DUE_JOBS_INVALID_SOURCE_BUILD_POISON_REPAIR_FAILED',
          'code', SQLSTATE,
          'message', SQLERRM,
          'session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
          'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        );
      END;
    END IF;
  END IF;

  IF v_allowed_job_types IS NULL
     OR 'WORKBENCH_CANDIDATE_SOURCE_BUILD' = ANY(v_allowed_job_types) THEN
    IF to_regprocedure('public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamp with time zone,text)') IS NOT NULL THEN
      BEGIN
        EXECUTE 'SELECT public.pay_workbench_repair_orphaned_pending_source_build($1, $2, $3, $4, $5)'
        INTO v_orphaned_pending_source_build_repair_json
        USING
          p_session_id,
          p_candidate_id,
          LEAST(GREATEST(v_limit, 1), 10),
          v_now,
          'CLAIM_DUE_JOBS_PENDING_SOURCE_BUILD_OWNER_REPAIR';
      EXCEPTION WHEN OTHERS THEN
        v_orphaned_pending_source_build_repair_json := jsonb_build_object(
          'ok', false,
          'deferred', true,
          'reason', 'CLAIM_DUE_JOBS_PENDING_SOURCE_BUILD_OWNER_REPAIR_FAILED',
          'code', SQLSTATE,
          'message', SQLERRM,
          'session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
          'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        );
      END;
    END IF;
  END IF;

  FOR v_stale_row IN
    WITH stale_candidates AS (
      SELECT
        stale_job.id,
        stale_job.job_type,
        stale_job.status,
        stale_job.priority,
        stale_job.run_at_utc,
        stale_job.attempt_count,
        stale_job.max_attempts,
        stale_job.dedupe_key,
        stale_job.snapshot_run_id,
        stale_job.session_id,
        stale_job.candidate_id,
        stale_job.payload_json,
        stale_job.created_at_utc,
        stale_job.updated_at_utc,
        stale_job.started_at_utc,
        stale_job.completed_at_utc,
        stale_job.failed_at_utc,
        stale_job.last_error_json,
        COALESCE(
          stale_job.updated_at_utc,
          stale_job.started_at_utc,
          stale_job.run_at_utc,
          stale_job.created_at_utc
        ) AS last_activity_utc
      FROM public.banking_pay_workbench_jobs AS stale_job
      WHERE stale_job.status = 'RUNNING'
        AND (
          CASE
            WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
              THEN 'WORKBENCH_SESSION_SCOPE_SEED'
            WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
              THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
              THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
              WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
                THEN 'WORKBENCH_SESSION_CLONE_REBASE'
            WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
              THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
            WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
              THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
            WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
              THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
            ELSE UPPER(BTRIM(COALESCE(stale_job.job_type, '')))
          END
        ) IN (
          'WORKBENCH_SESSION_SCOPE_SEED',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_DELTA_REFRESH',
          'WORKBENCH_SESSION_CLONE_REBASE',
          'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
          'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
          'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
        )
        AND (p_session_id IS NULL OR stale_job.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR stale_job.candidate_id = p_candidate_id)
        AND (
          v_allowed_job_types IS NULL
          OR UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) = ANY(v_allowed_job_types)
          OR (
            CASE
              WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
                THEN 'WORKBENCH_SESSION_SCOPE_SEED'
              WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
                THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
              WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
                THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
                WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
                  THEN 'WORKBENCH_SESSION_CLONE_REBASE'
              WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
                THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
              WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
                THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
              WHEN UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
                THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
              ELSE UPPER(BTRIM(COALESCE(stale_job.job_type, '')))
            END
          ) = ANY(v_allowed_job_types)
        )
        AND stale_job.completed_at_utc IS NULL
        AND stale_job.failed_at_utc IS NULL
        AND COALESCE(
              stale_job.updated_at_utc,
              stale_job.started_at_utc,
              stale_job.run_at_utc,
              stale_job.created_at_utc
            ) <= v_stale_cutoff
      ORDER BY
        COALESCE(
          stale_job.updated_at_utc,
          stale_job.started_at_utc,
          stale_job.run_at_utc,
          stale_job.created_at_utc
        ) ASC,
        stale_job.priority ASC,
        stale_job.run_at_utc ASC,
        stale_job.created_at_utc ASC,
        stale_job.id ASC
      LIMIT v_stale_recovery_limit
      FOR UPDATE SKIP LOCKED
    )
    SELECT
      stale_candidates.id,
      stale_candidates.job_type,
      stale_candidates.status,
      stale_candidates.priority,
      stale_candidates.run_at_utc,
      stale_candidates.attempt_count,
      stale_candidates.max_attempts,
      stale_candidates.dedupe_key,
      stale_candidates.snapshot_run_id,
      stale_candidates.session_id,
      stale_candidates.candidate_id,
      stale_candidates.payload_json,
      stale_candidates.created_at_utc,
      stale_candidates.updated_at_utc,
      stale_candidates.started_at_utc,
      stale_candidates.completed_at_utc,
      stale_candidates.failed_at_utc,
      stale_candidates.last_error_json,
      stale_candidates.last_activity_utc
    FROM stale_candidates
  LOOP
    v_stale_job_source_change_seq := COALESCE(
      CASE
        WHEN COALESCE(v_stale_row.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
          THEN (v_stale_row.payload_json->>'source_change_seq')::bigint
        ELSE 0::bigint
      END,
      0::bigint
    );
    v_stale_job_session_version := COALESCE(
      CASE
        WHEN COALESCE(v_stale_row.payload_json->>'session_version', '') ~ '^[0-9]+$'
          THEN (v_stale_row.payload_json->>'session_version')::bigint
        ELSE 0::bigint
      END,
      0::bigint
    );
    v_stale_live_candidate_change_seq := 0;
    v_stale_snapshot_state_status := NULL;
    v_stale_snapshot_state_source_change_seq := 0;
    v_stale_session_status := NULL;
    v_stale_session_version := 0;
    v_stale_session_candidate_status := NULL;
    v_stale_session_candidate_source_change_seq := 0;
    v_stale_session_candidate_session_version := 0;
    v_stale_other_active_job_id := NULL::uuid;
    v_stale_completed_equivalent_id := NULL::uuid;
    v_stale_obsolete := false;
    v_stale_obsolete_reason := NULL;
    v_stale_failed_line_work_count := 0;
    v_stale_failed_source_row_count := 0;
    v_stale_source_build_run_id_text := NULL::text;
    v_stale_invalid_source_build_without_run_id := false;
    v_stale_canonical_job_type := CASE
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
        THEN 'WORKBENCH_SESSION_SCOPE_SEED'
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
        THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
          THEN 'WORKBENCH_SESSION_CLONE_REBASE'
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
        THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
        THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
        THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
      ELSE UPPER(BTRIM(COALESCE(v_stale_row.job_type, '')))
    END;

    IF v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
      v_stale_source_build_run_id_text := NULLIF(BTRIM(COALESCE(
        v_stale_row.payload_json->>'source_build_run_id',
        v_stale_row.payload_json#>>'{source_build,source_build_run_id}',
        v_stale_row.payload_json#>>'{source_build,run_id}',
        v_stale_row.payload_json#>>'{cursor,source_build_run_id}',
        v_stale_row.payload_json#>>'{cursor_json,source_build_run_id}',
        v_stale_row.payload_json#>>'{result_json,source_build_run_id}',
        ''
      )), '');

      IF v_stale_source_build_run_id_text IS NULL
         OR v_stale_source_build_run_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        v_stale_source_build_run_id_text := NULL::text;
        v_stale_invalid_source_build_without_run_id := true;
      END IF;
    END IF;

    IF UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) = 'SNAPSHOT_CANDIDATE_REFRESH' THEN
      IF v_stale_row.snapshot_run_id IS NULL OR v_stale_row.candidate_id IS NULL THEN
        v_stale_obsolete := true;
        v_stale_obsolete_reason := 'INVALID_SNAPSHOT_CONTEXT';
      ELSE
        SELECT COALESCE(acc.seq, 0)
        INTO v_stale_live_candidate_change_seq
        FROM public.app_change_counters AS acc
        WHERE acc.entity_key = 'pay_candidate:' || v_stale_row.candidate_id::text;

        SELECT COALESCE(snap.status, NULL), COALESCE(snap.source_change_seq, 0)
        INTO v_stale_snapshot_state_status, v_stale_snapshot_state_source_change_seq
        FROM public.banking_pay_snapshot_candidate_state AS snap
        WHERE snap.snapshot_run_id = v_stale_row.snapshot_run_id
          AND snap.candidate_id = v_stale_row.candidate_id
        LIMIT 1;

        SELECT j.id
        INTO v_stale_other_active_job_id
        FROM public.banking_pay_workbench_jobs AS j
        WHERE j.id <> v_stale_row.id
          AND j.job_type = 'SNAPSHOT_CANDIDATE_REFRESH'
          AND j.snapshot_run_id = v_stale_row.snapshot_run_id
          AND j.candidate_id = v_stale_row.candidate_id
          AND j.status IN ('QUEUED', 'RUNNING')
          AND COALESCE(
                CASE
                  WHEN COALESCE(j.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
                    THEN (j.payload_json->>'source_change_seq')::bigint
                  ELSE 0::bigint
                END,
                0::bigint
              ) >= GREATEST(v_stale_job_source_change_seq, v_stale_live_candidate_change_seq)
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

        IF v_stale_live_candidate_change_seq > v_stale_job_source_change_seq THEN
          v_stale_obsolete := true;
          v_stale_obsolete_reason := 'SUPERSEDED_BY_LIVE_CHANGE_SEQ';
        ELSIF v_stale_other_active_job_id IS NOT NULL THEN
          v_stale_obsolete := true;
          v_stale_obsolete_reason := 'MATCHING_ACTIVE_SNAPSHOT_JOB_EXISTS';
        ELSIF UPPER(COALESCE(v_stale_snapshot_state_status, '')) = 'READY'
              AND v_stale_snapshot_state_source_change_seq >= GREATEST(v_stale_job_source_change_seq, v_stale_live_candidate_change_seq) THEN
          v_stale_obsolete := true;
          v_stale_obsolete_reason := 'SNAPSHOT_ALREADY_CURRENT';
        END IF;
      END IF;
    ELSIF UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) = 'SESSION_CANDIDATE_RECOMPUTE' THEN
      IF v_stale_row.session_id IS NULL OR v_stale_row.candidate_id IS NULL THEN
        v_stale_obsolete := true;
        v_stale_obsolete_reason := 'INVALID_SESSION_CONTEXT';
      ELSE
        SELECT COALESCE(acc.seq, 0)
        INTO v_stale_live_candidate_change_seq
        FROM public.app_change_counters AS acc
        WHERE acc.entity_key = 'pay_candidate:' || v_stale_row.candidate_id::text;

        SELECT COALESCE(ws.status, NULL), COALESCE(ws.version, 0)
        INTO v_stale_session_status, v_stale_session_version
        FROM public.banking_pay_workbench_sessions AS ws
        WHERE ws.id = v_stale_row.session_id
        LIMIT 1;

        IF v_stale_session_status IS NULL OR UPPER(COALESCE(v_stale_session_status, '')) <> 'OPEN' THEN
          v_stale_obsolete := true;
          v_stale_obsolete_reason := 'SESSION_NOT_OPEN';
        ELSE
          SELECT COALESCE(scs.status, NULL),
                 COALESCE(scs.source_change_seq, 0),
                 COALESCE(scs.session_version, 0)
          INTO v_stale_session_candidate_status,
               v_stale_session_candidate_source_change_seq,
               v_stale_session_candidate_session_version
          FROM public.banking_pay_workbench_session_candidate_state AS scs
          WHERE scs.session_id = v_stale_row.session_id
            AND scs.candidate_id = v_stale_row.candidate_id
          LIMIT 1;

          SELECT j.id
          INTO v_stale_other_active_job_id
          FROM public.banking_pay_workbench_jobs AS j
          WHERE j.id <> v_stale_row.id
            AND j.job_type = 'SESSION_CANDIDATE_RECOMPUTE'
            AND j.session_id = v_stale_row.session_id
            AND j.candidate_id = v_stale_row.candidate_id
            AND j.status IN ('QUEUED', 'RUNNING')
            AND COALESCE(
                  CASE
                    WHEN COALESCE(j.payload_json->>'session_version', '') ~ '^[0-9]+$'
                      THEN (j.payload_json->>'session_version')::bigint
                    ELSE 0::bigint
                  END,
                  0::bigint
                ) >= v_stale_session_version
            AND COALESCE(
                  CASE
                    WHEN COALESCE(j.payload_json->>'source_change_seq', '') ~ '^[0-9]+$'
                      THEN (j.payload_json->>'source_change_seq')::bigint
                    ELSE 0::bigint
                  END,
                  0::bigint
                ) >= GREATEST(v_stale_job_source_change_seq, v_stale_live_candidate_change_seq)
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

          IF v_stale_session_version > v_stale_job_session_version THEN
            v_stale_obsolete := true;
            v_stale_obsolete_reason := 'SUPERSEDED_BY_NEWER_SESSION_VERSION';
          ELSIF v_stale_other_active_job_id IS NOT NULL THEN
            v_stale_obsolete := true;
            v_stale_obsolete_reason := 'MATCHING_ACTIVE_SESSION_JOB_EXISTS';
          ELSIF UPPER(COALESCE(v_stale_session_candidate_status, '')) = 'READY'
                AND v_stale_session_candidate_session_version >= v_stale_session_version
                AND v_stale_session_candidate_source_change_seq >= GREATEST(v_stale_job_source_change_seq, v_stale_live_candidate_change_seq) THEN
            v_stale_obsolete := true;
            v_stale_obsolete_reason := 'SESSION_ALREADY_CURRENT';
          END IF;
        END IF;
      END IF;
    ELSIF UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) = 'PAYEE_READINESS_ENSURE' THEN
      IF v_stale_row.candidate_id IS NULL THEN
        v_stale_obsolete := true;
        v_stale_obsolete_reason := 'INVALID_READINESS_CONTEXT';
      ELSE
        SELECT COALESCE(acc.seq, 0)
        INTO v_stale_live_candidate_change_seq
        FROM public.app_change_counters AS acc
        WHERE acc.entity_key = 'pay_candidate:' || v_stale_row.candidate_id::text;

        IF v_stale_row.session_id IS NOT NULL THEN
          SELECT COALESCE(ws.status, NULL), COALESCE(ws.version, 0)
          INTO v_stale_session_status, v_stale_session_version
          FROM public.banking_pay_workbench_sessions AS ws
          WHERE ws.id = v_stale_row.session_id
          LIMIT 1;

          IF v_stale_session_status IS NULL OR UPPER(COALESCE(v_stale_session_status, '')) <> 'OPEN' THEN
            v_stale_obsolete := true;
            v_stale_obsolete_reason := 'SESSION_NOT_OPEN';
          ELSIF v_stale_session_version > v_stale_job_session_version THEN
            v_stale_obsolete := true;
            v_stale_obsolete_reason := 'SUPERSEDED_BY_NEWER_SESSION_VERSION';
          END IF;
        END IF;

        IF NOT v_stale_obsolete AND v_stale_live_candidate_change_seq > v_stale_job_source_change_seq THEN
          v_stale_obsolete := true;
          v_stale_obsolete_reason := 'SUPERSEDED_BY_LIVE_CHANGE_SEQ';
        END IF;

        IF NOT v_stale_obsolete THEN
          SELECT j.id
          INTO v_stale_completed_equivalent_id
          FROM public.banking_pay_workbench_jobs AS j
          WHERE j.id <> v_stale_row.id
            AND j.job_type = 'PAYEE_READINESS_ENSURE'
            AND j.dedupe_key = v_stale_row.dedupe_key
            AND j.completed_at_utc IS NOT NULL
            AND j.failed_at_utc IS NULL
          ORDER BY j.completed_at_utc DESC, j.id DESC
          LIMIT 1;

          IF v_stale_completed_equivalent_id IS NOT NULL THEN
            v_stale_obsolete := true;
            v_stale_obsolete_reason := 'COMPLETED_EQUIVALENT_EXISTS';
          ELSE
            SELECT j.id
            INTO v_stale_other_active_job_id
            FROM public.banking_pay_workbench_jobs AS j
            WHERE j.id <> v_stale_row.id
              AND j.job_type = 'PAYEE_READINESS_ENSURE'
              AND j.dedupe_key = v_stale_row.dedupe_key
              AND j.status IN ('QUEUED', 'RUNNING')
            ORDER BY j.updated_at_utc DESC NULLS LAST, j.created_at_utc DESC NULLS LAST, j.id DESC
            LIMIT 1;

            IF v_stale_other_active_job_id IS NOT NULL THEN
              v_stale_obsolete := true;
              v_stale_obsolete_reason := 'MATCHING_ACTIVE_READINESS_JOB_EXISTS';
            END IF;
          END IF;
        END IF;
      END IF;
    ELSIF (
      CASE
        WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
          THEN 'WORKBENCH_SESSION_SCOPE_SEED'
        WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
        THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
          THEN 'WORKBENCH_SESSION_CLONE_REBASE'
      WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
          THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
        WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
          THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
        WHEN UPPER(BTRIM(COALESCE(v_stale_row.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
          THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
        ELSE UPPER(BTRIM(COALESCE(v_stale_row.job_type, '')))
      END
    ) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_DELTA_REFRESH', 'WORKBENCH_SESSION_CLONE_REBASE', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE') THEN
      IF v_stale_row.session_id IS NULL THEN
        v_stale_obsolete := true;
        v_stale_obsolete_reason := 'INVALID_WORKBENCH_SESSION_CONTEXT';
      ELSE
        SELECT COALESCE(workbench_session.status, NULL), COALESCE(workbench_session.version, 0)
        INTO v_stale_session_status, v_stale_session_version
        FROM public.banking_pay_workbench_sessions AS workbench_session
        WHERE workbench_session.id = v_stale_row.session_id
        LIMIT 1;

        IF v_stale_session_status IS NULL OR UPPER(COALESCE(v_stale_session_status, '')) <> 'OPEN' THEN
          v_stale_obsolete := true;
          v_stale_obsolete_reason := 'SESSION_NOT_OPEN';
        ELSIF v_stale_job_session_version > 0 AND v_stale_session_version > v_stale_job_session_version THEN
          v_stale_obsolete := true;
          v_stale_obsolete_reason := 'SUPERSEDED_BY_NEWER_SESSION_VERSION';
        END IF;
      END IF;

    END IF;

    v_stale_before_json := jsonb_build_object(
      'id', v_stale_row.id::text,
      'job_type', v_stale_row.job_type,
      'status', v_stale_row.status,
      'priority', v_stale_row.priority,
      'run_at_utc', v_stale_row.run_at_utc,
      'attempt_count', v_stale_row.attempt_count,
      'max_attempts', v_stale_row.max_attempts,
      'dedupe_key', v_stale_row.dedupe_key,
      'snapshot_run_id', CASE WHEN v_stale_row.snapshot_run_id IS NULL THEN NULL ELSE v_stale_row.snapshot_run_id::text END,
      'session_id', CASE WHEN v_stale_row.session_id IS NULL THEN NULL ELSE v_stale_row.session_id::text END,
      'candidate_id', CASE WHEN v_stale_row.candidate_id IS NULL THEN NULL ELSE v_stale_row.candidate_id::text END,
      'payload_json', v_stale_row.payload_json,
      'created_at_utc', v_stale_row.created_at_utc,
      'updated_at_utc', v_stale_row.updated_at_utc,
      'started_at_utc', v_stale_row.started_at_utc,
      'completed_at_utc', v_stale_row.completed_at_utc,
      'failed_at_utc', v_stale_row.failed_at_utc,
      'last_error_json', v_stale_row.last_error_json,
      'last_activity_utc', v_stale_row.last_activity_utc
    );

    v_stale_error_json := jsonb_build_object(
      'code', CASE
        WHEN v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
             AND v_stale_invalid_source_build_without_run_id IS TRUE THEN 'STALE_RUNNING_WORKBENCH_SOURCE_BUILD_RUN_ID_MISSING_FAILED_CLOSED'
        WHEN v_stale_obsolete THEN 'STALE_RUNNING_WORKBENCH_JOB_MAX_ATTEMPTS'
        WHEN COALESCE(v_stale_row.attempt_count, 0) >= COALESCE(v_stale_row.max_attempts, 8) THEN 'STALE_RUNNING_WORKBENCH_JOB_MAX_ATTEMPTS'
        ELSE 'STALE_RUNNING_WORKBENCH_JOB_RECOVERED'
      END,
      'message', CASE
        WHEN v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
             AND v_stale_invalid_source_build_without_run_id IS TRUE THEN 'Failed stale running-like Banking Pay source-build job because its payload is missing source_build_run_id and cannot safely authorise source-row mutation.'
        WHEN v_stale_obsolete THEN 'Failed stale running-like Banking Pay workbench preview job because its context is obsolete or superseded.'
        WHEN COALESCE(v_stale_row.attempt_count, 0) >= COALESCE(v_stale_row.max_attempts, 8) THEN 'Failed stale running-like Banking Pay workbench preview job because max attempts are exhausted.'
        ELSE 'Recovered stale running-like Banking Pay workbench preview job during due-job claim cycle.'
      END,
      'job_id', v_stale_row.id::text,
      'job_type', v_stale_row.job_type,
      'previous_status', v_stale_row.status,
      'attempt_count', COALESCE(v_stale_row.attempt_count, 0),
      'max_attempts', COALESCE(v_stale_row.max_attempts, 8),
      'last_activity_utc', v_stale_row.last_activity_utc,
      'stale_cutoff_utc', v_stale_cutoff,
      'stale_running_seconds', v_stale_running_seconds,
      'recovered_at_utc', v_now,
      'obsolete', v_stale_obsolete,
      'obsolete_reason', v_stale_obsolete_reason,
      'source_change_seq', v_stale_job_source_change_seq,
      'session_version', v_stale_job_session_version,
      'live_candidate_change_seq', CASE WHEN v_stale_live_candidate_change_seq = 0 THEN NULL ELSE v_stale_live_candidate_change_seq END,
      'snapshot_state_status', v_stale_snapshot_state_status,
      'snapshot_state_source_change_seq', CASE WHEN v_stale_snapshot_state_source_change_seq = 0 THEN NULL ELSE v_stale_snapshot_state_source_change_seq END,
      'session_status', v_stale_session_status,
      'current_session_version', CASE WHEN v_stale_session_version = 0 THEN NULL ELSE v_stale_session_version END,
      'session_candidate_status', v_stale_session_candidate_status,
      'session_candidate_source_change_seq', CASE WHEN v_stale_session_candidate_source_change_seq = 0 THEN NULL ELSE v_stale_session_candidate_source_change_seq END,
      'session_candidate_session_version', CASE WHEN v_stale_session_candidate_session_version = 0 THEN NULL ELSE v_stale_session_candidate_session_version END,
      'other_active_job_id', CASE WHEN v_stale_other_active_job_id IS NULL THEN NULL ELSE v_stale_other_active_job_id::text END,
      'completed_equivalent_job_id', CASE WHEN v_stale_completed_equivalent_id IS NULL THEN NULL ELSE v_stale_completed_equivalent_id::text END
    );

    IF v_stale_obsolete
       OR (
         v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
         AND v_stale_invalid_source_build_without_run_id IS TRUE
       )
       OR COALESCE(v_stale_row.attempt_count, 0) >= COALESCE(v_stale_row.max_attempts, 8) THEN
      UPDATE public.banking_pay_workbench_jobs AS dead_job
      SET status = 'FAILED',
          updated_at_utc = v_now,
          completed_at_utc = NULL,
          failed_at_utc = v_now,
          last_error_json = v_stale_error_json,
          payload_json = jsonb_strip_nulls(
            COALESCE(dead_job.payload_json, '{}'::jsonb)
            || jsonb_build_object('last_failure_json', v_stale_error_json)
            || CASE
              WHEN v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
                   AND v_stale_invalid_source_build_without_run_id IS TRUE THEN
                jsonb_build_object(
                  'invalid_source_build_without_run_id_failed_closed', true,
                  'invalid_source_build_without_run_id_non_blocking', true,
                  'non_blocking_terminal_failure', true,
                  'non_blocking_terminal_failure_reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
                  'non_blocking_terminal_failure_at_utc', v_now::text,
                  'source_build_run_id_required', true,
                  'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
                )
              ELSE '{}'::jsonb
            END
          )
      WHERE dead_job.id = v_stale_row.id
      RETURNING
        dead_job.id,
        dead_job.job_type,
        dead_job.status,
        dead_job.priority,
        dead_job.run_at_utc,
        dead_job.attempt_count,
        dead_job.max_attempts,
        dead_job.dedupe_key,
        dead_job.snapshot_run_id,
        dead_job.session_id,
        dead_job.candidate_id,
        dead_job.payload_json,
        dead_job.created_at_utc,
        dead_job.updated_at_utc,
        dead_job.started_at_utc,
        dead_job.completed_at_utc,
        dead_job.failed_at_utc,
        dead_job.last_error_json
      INTO
        v_stale_row.id,
        v_stale_row.job_type,
        v_stale_row.status,
        v_stale_row.priority,
        v_stale_row.run_at_utc,
        v_stale_row.attempt_count,
        v_stale_row.max_attempts,
        v_stale_row.dedupe_key,
        v_stale_row.snapshot_run_id,
        v_stale_row.session_id,
        v_stale_row.candidate_id,
        v_stale_row.payload_json,
        v_stale_row.created_at_utc,
        v_stale_row.updated_at_utc,
        v_stale_row.started_at_utc,
        v_stale_row.completed_at_utc,
        v_stale_row.failed_at_utc,
        v_stale_row.last_error_json;

      IF v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
         AND v_stale_obsolete IS NOT TRUE
         AND (
           v_stale_invalid_source_build_without_run_id IS TRUE
           OR COALESCE(v_stale_row.attempt_count, 0) >= GREATEST(COALESCE(v_stale_row.max_attempts, 8), 1)
         )
         AND v_stale_row.session_id IS NOT NULL
         AND v_stale_row.candidate_id IS NOT NULL THEN
        IF v_stale_source_build_run_id_text IS NULL THEN
          v_stale_failed_source_row_count := 0;

          PERFORM public._audit_insert(
            'banking_pay_workbench_candidate_source_lines',
            v_stale_row.candidate_id::text,
            'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_DID_NOT_MARK_ROWS_ERROR',
            jsonb_build_object(
              'session_id', v_stale_row.session_id::text,
              'candidate_id', v_stale_row.candidate_id::text,
              'bad_job_id', v_stale_row.id::text,
              'source_build_run_id', NULL::text,
              'source_change_seq', CASE WHEN v_stale_job_source_change_seq = 0 THEN NULL ELSE v_stale_job_source_change_seq END,
              'old_status', 'CURRENT_OR_DIRTY',
              'reason', 'STALE_SOURCE_BUILD_JOB_MISSING_SOURCE_BUILD_RUN_ID',
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            ),
            jsonb_build_object(
              'session_id', v_stale_row.session_id::text,
              'candidate_id', v_stale_row.candidate_id::text,
              'bad_job_id', v_stale_row.id::text,
              'source_build_run_id', NULL::text,
              'source_change_seq', CASE WHEN v_stale_job_source_change_seq = 0 THEN NULL ELSE v_stale_job_source_change_seq END,
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
            v_stale_row.candidate_id::text,
            'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_IGNORED',
            jsonb_build_object(
              'session_id', v_stale_row.session_id::text,
              'candidate_id', v_stale_row.candidate_id::text,
              'bad_job_id', v_stale_row.id::text,
              'source_build_run_id', NULL::text,
              'source_change_seq', CASE WHEN v_stale_job_source_change_seq = 0 THEN NULL ELSE v_stale_job_source_change_seq END,
              'reason', 'STALE_SOURCE_BUILD_JOB_MISSING_SOURCE_BUILD_RUN_ID',
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            ),
            jsonb_build_object(
              'session_id', v_stale_row.session_id::text,
              'candidate_id', v_stale_row.candidate_id::text,
              'bad_job_id', v_stale_row.id::text,
              'source_build_run_id', NULL::text,
              'source_change_seq', CASE WHEN v_stale_job_source_change_seq = 0 THEN NULL ELSE v_stale_job_source_change_seq END,
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
                      'stale_source_build_error', jsonb_build_object(
                        'code', 'STALE_RUNNING_WORKBENCH_SOURCE_BUILD_FAILED',
                        'job_id', v_stale_row.id::text,
                        'job_type', v_stale_row.job_type,
                        'source_build_run_id', v_stale_source_build_run_id_text,
                        'failed_at_utc', v_now::text,
                        'job_error_json', COALESCE(v_stale_error_json, '{}'::jsonb),
                        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                      )
                    ),
                  updated_at_utc = v_now
              WHERE source_line.session_id = v_stale_row.session_id
                AND source_line.candidate_id = v_stale_row.candidate_id
                AND source_line.status IN ('CURRENT', 'DIRTY')
                AND source_line.source_build_run_id::text = v_stale_source_build_run_id_text
              RETURNING source_line.id
            )
            SELECT COUNT(*)::integer
            INTO v_stale_failed_source_row_count
            FROM failed_source_rows;
          END IF;

          UPDATE public.banking_pay_workbench_session_scope AS source_scope_row
          SET status = 'SOURCE_BUILD_ERROR',
              dirty = true,
              error_json = jsonb_build_object(
                'code', 'STALE_RUNNING_WORKBENCH_SOURCE_BUILD_FAILED',
                'message', 'Candidate source build job exhausted attempts.',
                'job_id', v_stale_row.id::text,
                'job_type', v_stale_row.job_type,
                'source_build_run_id', v_stale_source_build_run_id_text,
                'source_rows_marked_error_count', COALESCE(v_stale_failed_source_row_count, 0),
                'job_error_json', COALESCE(v_stale_error_json, '{}'::jsonb)
              ),
              updated_at_utc = v_now
          WHERE source_scope_row.session_id = v_stale_row.session_id
            AND source_scope_row.candidate_id = v_stale_row.candidate_id;

          UPDATE public.banking_pay_workbench_sessions AS source_session_row
          SET progress_state = 'ERROR',
              progress_json = public.pay_workbench_session_compact_progress_json(COALESCE(source_session_row.progress_json, '{}'::jsonb), true) || jsonb_build_object(
                'last_source_build_failure_at_utc', v_now::text,
                'last_source_build_failure_job_id', v_stale_row.id::text,
                'last_source_build_failure_code', 'STALE_RUNNING_WORKBENCH_SOURCE_BUILD_FAILED',
                'last_source_build_failure_source_build_run_id', v_stale_source_build_run_id_text,
                'last_source_build_source_rows_marked_error_count', COALESCE(v_stale_failed_source_row_count, 0)
              ),
              progress_counter_version = COALESCE(source_session_row.progress_counter_version, 0) + 1,
              progress_updated_at_utc = v_now,
              updated_at_utc = v_now
          WHERE source_session_row.id = v_stale_row.session_id;
        END IF;
      END IF;

      IF v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
         AND v_stale_obsolete IS NOT TRUE
         AND COALESCE(v_stale_row.attempt_count, 0) >= GREATEST(COALESCE(v_stale_row.max_attempts, 8), 1)
         AND v_stale_row.session_id IS NOT NULL
         AND v_stale_row.candidate_id IS NOT NULL THEN
        WITH failed_lines AS (
          UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
          SET status = 'ERROR',
              error_json = jsonb_build_object(
                'code', 'STALE_RUNNING_WORKBENCH_LINE_WORK_PROCESS_FAILED',
                'message', 'Candidate line work was marked failed because its stale process job exhausted attempts.',
                'job_id', v_stale_row.id::text,
                'job_type', v_stale_row.job_type,
                'session_id', v_stale_row.session_id::text,
                'candidate_id', v_stale_row.candidate_id::text,
                'job_error_json', COALESCE(v_stale_error_json, '{}'::jsonb)
              ),
              updated_at_utc = v_now
          WHERE line_work.session_id = v_stale_row.session_id
            AND line_work.candidate_id = v_stale_row.candidate_id
            AND UPPER(BTRIM(COALESCE(line_work.status, ''))) = 'PENDING'
          RETURNING line_work.id
        )
        SELECT COUNT(*)::integer
        INTO v_stale_failed_line_work_count
        FROM failed_lines;

        IF COALESCE(v_stale_failed_line_work_count, 0) > 0 THEN
          UPDATE public.banking_pay_workbench_session_scope AS scope_row
          SET status = 'ERROR',
              dirty = true,
              error_json = jsonb_build_object(
                'code', 'STALE_RUNNING_WORKBENCH_LINE_WORK_PROCESS_FAILED',
                'message', 'Candidate line work process job exhausted attempts.',
                'job_id', v_stale_row.id::text,
                'line_work_failed_count', v_stale_failed_line_work_count,
                'job_error_json', COALESCE(v_stale_error_json, '{}'::jsonb)
              ),
              updated_at_utc = v_now
          WHERE scope_row.session_id = v_stale_row.session_id
            AND scope_row.candidate_id = v_stale_row.candidate_id;

          UPDATE public.banking_pay_workbench_sessions AS session_row
          SET line_units_pending = GREATEST(COALESCE(session_row.line_units_pending, 0) - COALESCE(v_stale_failed_line_work_count, 0), 0),
              line_units_failed = GREATEST(COALESCE(session_row.line_units_failed, 0) + COALESCE(v_stale_failed_line_work_count, 0), 0),
              progress_state = 'ERROR',
              progress_json = public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true) || jsonb_build_object(
                'last_line_process_failure_at_utc', v_now::text,
                'last_line_process_failure_job_id', v_stale_row.id::text,
                'last_line_process_failure_code', 'STALE_RUNNING_WORKBENCH_LINE_WORK_PROCESS_FAILED',
                'last_line_process_failure_count', v_stale_failed_line_work_count
              ),
              progress_counter_version = COALESCE(session_row.progress_counter_version, 0) + 1,
              progress_updated_at_utc = v_now,
              updated_at_utc = v_now
          WHERE session_row.id = v_stale_row.session_id;
        END IF;
      END IF;

      v_stale_after_json := jsonb_build_object(
        'id', v_stale_row.id::text,
        'job_type', v_stale_row.job_type,
        'status', v_stale_row.status,
        'priority', v_stale_row.priority,
        'run_at_utc', v_stale_row.run_at_utc,
        'attempt_count', v_stale_row.attempt_count,
        'max_attempts', v_stale_row.max_attempts,
        'dedupe_key', v_stale_row.dedupe_key,
        'snapshot_run_id', CASE WHEN v_stale_row.snapshot_run_id IS NULL THEN NULL ELSE v_stale_row.snapshot_run_id::text END,
        'session_id', CASE WHEN v_stale_row.session_id IS NULL THEN NULL ELSE v_stale_row.session_id::text END,
        'candidate_id', CASE WHEN v_stale_row.candidate_id IS NULL THEN NULL ELSE v_stale_row.candidate_id::text END,
        'payload_json', v_stale_row.payload_json,
        'created_at_utc', v_stale_row.created_at_utc,
        'updated_at_utc', v_stale_row.updated_at_utc,
        'started_at_utc', v_stale_row.started_at_utc,
        'completed_at_utc', v_stale_row.completed_at_utc,
        'failed_at_utc', v_stale_row.failed_at_utc,
        'last_error_json', v_stale_row.last_error_json,
        'last_activity_utc', COALESCE(
          v_stale_row.updated_at_utc,
          v_stale_row.started_at_utc,
          v_stale_row.run_at_utc,
          v_stale_row.created_at_utc
        ),
        'stale_recovery', true,
        'failed_terminal', true,
        'stale_cutoff_utc', v_stale_cutoff,
        'stale_running_seconds', v_stale_running_seconds,
        'obsolete', v_stale_obsolete,
        'obsolete_reason', v_stale_obsolete_reason,
        'line_work_failed_count', COALESCE(v_stale_failed_line_work_count, 0),
        'source_rows_marked_error_count', COALESCE(v_stale_failed_source_row_count, 0),
        'invalid_source_build_without_run_id_non_blocking', COALESCE(v_stale_invalid_source_build_without_run_id, false),
        'non_blocking_terminal_failure', COALESCE(v_stale_invalid_source_build_without_run_id, false)
      );

      v_dead_stale := v_dead_stale || jsonb_build_array(
        jsonb_build_object(
          'job_id', v_stale_row.id::text,
          'job_type', v_stale_row.job_type,
          'priority', v_stale_row.priority,
          'run_at_utc', v_stale_row.run_at_utc,
          'attempt_count', v_stale_row.attempt_count,
          'max_attempts', v_stale_row.max_attempts,
          'snapshot_run_id', CASE WHEN v_stale_row.snapshot_run_id IS NULL THEN NULL ELSE v_stale_row.snapshot_run_id::text END,
          'session_id', CASE WHEN v_stale_row.session_id IS NULL THEN NULL ELSE v_stale_row.session_id::text END,
          'candidate_id', CASE WHEN v_stale_row.candidate_id IS NULL THEN NULL ELSE v_stale_row.candidate_id::text END,
          'payload_json', v_stale_row.payload_json,
          'failed_at_utc', v_stale_row.failed_at_utc,
          'last_error_json', v_stale_row.last_error_json,
          'obsolete', v_stale_obsolete,
          'obsolete_reason', v_stale_obsolete_reason,
          'source_build_run_id', v_stale_source_build_run_id_text,
          'invalid_source_build_without_run_id_failed_closed', COALESCE(v_stale_invalid_source_build_without_run_id, false),
          'invalid_source_build_without_run_id_non_blocking', COALESCE(v_stale_invalid_source_build_without_run_id, false),
          'non_blocking_terminal_failure', COALESCE(v_stale_invalid_source_build_without_run_id, false),
          'source_rows_marked_error_count', COALESCE(v_stale_failed_source_row_count, 0)
        )
      );

      v_dead_stale_count := v_dead_stale_count + 1;

      PERFORM public._audit_insert(
        'banking_pay_workbench_job',
        v_stale_row.id::text,
        'FAILED',
        v_stale_before_json,
        v_stale_after_json,
        CASE
          WHEN v_stale_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
               AND v_stale_invalid_source_build_without_run_id IS TRUE
            THEN 'WORKBENCH_INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_FAILED_CLOSED'
          ELSE 'WORKBENCH_JOB_STALE_RUNNER_FAILED'
        END,
        NULL
      );
    ELSE
      UPDATE public.banking_pay_workbench_jobs AS requeue_job
      SET status = 'QUEUED',
          run_at_utc = v_cutoff,
          updated_at_utc = v_now,
          started_at_utc = NULL,
          completed_at_utc = NULL,
          failed_at_utc = NULL,
          last_error_json = v_stale_error_json
      WHERE requeue_job.id = v_stale_row.id
      RETURNING
        requeue_job.id,
        requeue_job.job_type,
        requeue_job.status,
        requeue_job.priority,
        requeue_job.run_at_utc,
        requeue_job.attempt_count,
        requeue_job.max_attempts,
        requeue_job.dedupe_key,
        requeue_job.snapshot_run_id,
        requeue_job.session_id,
        requeue_job.candidate_id,
        requeue_job.payload_json,
        requeue_job.created_at_utc,
        requeue_job.updated_at_utc,
        requeue_job.started_at_utc,
        requeue_job.completed_at_utc,
        requeue_job.failed_at_utc,
        requeue_job.last_error_json
      INTO
        v_stale_row.id,
        v_stale_row.job_type,
        v_stale_row.status,
        v_stale_row.priority,
        v_stale_row.run_at_utc,
        v_stale_row.attempt_count,
        v_stale_row.max_attempts,
        v_stale_row.dedupe_key,
        v_stale_row.snapshot_run_id,
        v_stale_row.session_id,
        v_stale_row.candidate_id,
        v_stale_row.payload_json,
        v_stale_row.created_at_utc,
        v_stale_row.updated_at_utc,
        v_stale_row.started_at_utc,
        v_stale_row.completed_at_utc,
        v_stale_row.failed_at_utc,
        v_stale_row.last_error_json;

      v_stale_after_json := jsonb_build_object(
        'id', v_stale_row.id::text,
        'job_type', v_stale_row.job_type,
        'status', v_stale_row.status,
        'priority', v_stale_row.priority,
        'run_at_utc', v_stale_row.run_at_utc,
        'attempt_count', v_stale_row.attempt_count,
        'max_attempts', v_stale_row.max_attempts,
        'dedupe_key', v_stale_row.dedupe_key,
        'snapshot_run_id', CASE WHEN v_stale_row.snapshot_run_id IS NULL THEN NULL ELSE v_stale_row.snapshot_run_id::text END,
        'session_id', CASE WHEN v_stale_row.session_id IS NULL THEN NULL ELSE v_stale_row.session_id::text END,
        'candidate_id', CASE WHEN v_stale_row.candidate_id IS NULL THEN NULL ELSE v_stale_row.candidate_id::text END,
        'payload_json', v_stale_row.payload_json,
        'created_at_utc', v_stale_row.created_at_utc,
        'updated_at_utc', v_stale_row.updated_at_utc,
        'started_at_utc', v_stale_row.started_at_utc,
        'completed_at_utc', v_stale_row.completed_at_utc,
        'failed_at_utc', v_stale_row.failed_at_utc,
        'last_error_json', v_stale_row.last_error_json,
        'last_activity_utc', COALESCE(
          v_stale_row.updated_at_utc,
          v_stale_row.started_at_utc,
          v_stale_row.run_at_utc,
          v_stale_row.created_at_utc
        ),
        'stale_recovery', true,
        'requeued', true,
        'stale_cutoff_utc', v_stale_cutoff,
        'stale_running_seconds', v_stale_running_seconds,
        'obsolete', false,
        'obsolete_reason', NULL
      );

      v_recovered_stale := v_recovered_stale || jsonb_build_array(
        jsonb_build_object(
          'job_id', v_stale_row.id::text,
          'job_type', v_stale_row.job_type,
          'priority', v_stale_row.priority,
          'run_at_utc', v_stale_row.run_at_utc,
          'attempt_count', v_stale_row.attempt_count,
          'max_attempts', v_stale_row.max_attempts,
          'snapshot_run_id', CASE WHEN v_stale_row.snapshot_run_id IS NULL THEN NULL ELSE v_stale_row.snapshot_run_id::text END,
          'session_id', CASE WHEN v_stale_row.session_id IS NULL THEN NULL ELSE v_stale_row.session_id::text END,
          'candidate_id', CASE WHEN v_stale_row.candidate_id IS NULL THEN NULL ELSE v_stale_row.candidate_id::text END,
          'payload_json', v_stale_row.payload_json,
          'updated_at_utc', v_stale_row.updated_at_utc,
          'last_error_json', v_stale_row.last_error_json
        )
      );

      v_recovered_stale_count := v_recovered_stale_count + 1;

      PERFORM public._audit_insert(
        'banking_pay_workbench_job',
        v_stale_row.id::text,
        'REQUEUED',
        v_stale_before_json,
        v_stale_after_json,
        'WORKBENCH_JOB_STALE_RUNNER_REQUEUED',
        NULL
      );
    END IF;
  END LOOP;

  BEGIN
    WITH visible_due AS (
      SELECT
        claim_job.id,
        claim_job.job_type,
        claim_job.priority,
        claim_job.run_at_utc,
        claim_job.session_id,
        claim_job.candidate_id,
        claim_job.created_at_utc
      FROM public.banking_pay_workbench_jobs AS claim_job
      WHERE claim_job.status = 'QUEUED'
        AND claim_job.run_at_utc <= v_cutoff
        AND (p_session_id IS NULL OR claim_job.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR claim_job.candidate_id = p_candidate_id)
        AND (
          v_allowed_job_types IS NULL
          OR claim_job.job_type = ANY(v_allowed_job_types)
          OR UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) = ANY(v_allowed_job_types)
          OR (
            CASE
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
                THEN 'WORKBENCH_SESSION_SCOPE_SEED'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
                THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
                THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
                WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
                  THEN 'WORKBENCH_SESSION_CLONE_REBASE'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
                THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
                THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
                THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
              ELSE UPPER(BTRIM(COALESCE(claim_job.job_type, '')))
            END
          ) = ANY(v_allowed_job_types)
        )
      ORDER BY claim_job.priority ASC, claim_job.run_at_utc ASC, claim_job.created_at_utc ASC, claim_job.id ASC
      LIMIT v_limit
    )
    SELECT
      COALESCE(COUNT(*), 0)::integer,
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'job_id', visible_due.id::text,
            'job_type', visible_due.job_type,
            'priority', visible_due.priority,
            'run_at_utc', visible_due.run_at_utc,
            'session_id', CASE WHEN visible_due.session_id IS NULL THEN NULL ELSE visible_due.session_id::text END,
            'candidate_id', CASE WHEN visible_due.candidate_id IS NULL THEN NULL ELSE visible_due.candidate_id::text END,
            'created_at_utc', visible_due.created_at_utc
          )
          ORDER BY visible_due.priority ASC, visible_due.run_at_utc ASC, visible_due.created_at_utc ASC, visible_due.id ASC
        ),
        '[]'::jsonb
      )
    INTO v_preclaim_due_queued_count, v_preclaim_due_queued_sample
    FROM visible_due;
  EXCEPTION WHEN OTHERS THEN
    v_preclaim_due_queued_count := 0;
    v_preclaim_due_queued_sample := '[]'::jsonb;
  END;

  -- Latest-wins queue compaction for queued DELTA jobs.
  -- This collapses repeated lifecycle flips for the same session/candidate/timesheet hot key
  -- before expensive projection work is claimed.  It does not touch payment economics.
  BEGIN
    WITH queued_delta AS (
      SELECT
        delta_job.id,
        delta_job.priority,
        delta_job.run_at_utc,
        delta_job.created_at_utc,
        delta_job.updated_at_utc,
        delta_job.payload_json,
        delta_job.dedupe_key,
        COALESCE(
          NULLIF(BTRIM(COALESCE(
            delta_job.payload_json->>'normalised_delta_family_key',
            delta_job.payload_json->>'delta_family_key',
            delta_job.payload_json->>'delta_coalescing_key',
            ''
          )), ''),
          (
          SELECT
            'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
            || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
              delta_job.session_id::text,
              (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'session_id',
              (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
              (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
              (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
              ''
            )), ''), 'none')
            || ':version:' || COALESCE(
              CASE
                WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                  THEN delta_family_values.session_version_text::bigint
                ELSE COALESCE(NULL::bigint, 0)::bigint
              END,
              0
            )::text
            || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
              NULLIF(delta_family_values.projection_mode_text, ''),
              NULLIF(NULL::text, ''),
              'DELTA'
            ))), ''), 'DELTA')
            || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
              NULLIF(delta_family_values.projection_class_text, ''),
              NULLIF(NULL::text, ''),
              'UNKNOWN'
            ))), ''), 'UNKNOWN')
            || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
              NULLIF(delta_family_values.refresh_scope_kind_text, ''),
              NULLIF(NULL::text, ''),
              CASE
                WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                  OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                  THEN 'TARGETED_TIMESHEETS'
                ELSE 'CANDIDATE_FULL_LIVE'
              END
            ))), ''), 'CANDIDATE_FULL_LIVE')
            || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
              delta_job.candidate_id::text,
              (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
              ((COALESCE(delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
              ''
            )), ''), 'none')
            || ':timesheets:' || md5(
              COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
              || ':'
              || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
            )
          FROM (
            SELECT
              NULLIF(BTRIM(COALESCE(
                (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'session_version',
                (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'version',
                CASE WHEN NULL::bigint IS NULL THEN NULL ELSE (NULL::bigint)::text END,
                '0'
              )), '') AS session_version_text,
              UPPER(BTRIM(COALESCE(
                NULLIF((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                NULLIF((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                NULLIF((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                NULLIF(NULL::text, ''),
                'DELTA'
              ))) AS projection_mode_text,
              UPPER(BTRIM(COALESCE(
                NULLIF((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                NULLIF((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                NULLIF(((COALESCE(delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                NULLIF(NULL::text, ''),
                'UNKNOWN'
              ))) AS projection_class_text,
              UPPER(BTRIM(COALESCE(
                NULLIF((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                NULLIF((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                NULLIF(((COALESCE(delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                NULLIF(((COALESCE(delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                NULLIF(NULL::text, '')
              ))) AS refresh_scope_kind_text
          ) AS delta_family_values
          CROSS JOIN (
            SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
            FROM (
              SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
              FROM jsonb_array_elements_text(
                CASE
                  WHEN jsonb_typeof((COALESCE(delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                    THEN (COALESCE(delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                  WHEN jsonb_typeof((COALESCE(delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                    THEN jsonb_build_array((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                  WHEN jsonb_typeof(((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                    THEN ((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                  WHEN jsonb_typeof(((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                    THEN ((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                  WHEN NULLIF(BTRIM(COALESCE(
                         (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                         (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                         ''
                       )), '') IS NOT NULL
                    THEN jsonb_build_array(COALESCE(
                      (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                      (COALESCE(delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                    ))
                  ELSE '[]'::jsonb
                END
              ) AS delta_family_targeted_raw(value)
              WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ) AS delta_family_targeted_sorted
          ) AS delta_family_targeted
          CROSS JOIN (
            SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
            FROM (
              SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
              FROM jsonb_array_elements_text(
                CASE
                  WHEN jsonb_typeof((COALESCE(delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                    THEN (COALESCE(delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                  WHEN jsonb_typeof((COALESCE(delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                    THEN jsonb_build_array((COALESCE(delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                  WHEN jsonb_typeof(((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                    THEN ((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                  WHEN jsonb_typeof(((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                    THEN ((COALESCE(delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                  ELSE '[]'::jsonb
                END
              ) AS delta_family_linked_raw(value)
              WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ) AS delta_family_linked_sorted
          ) AS delta_family_linked
          )
        ) AS hot_key,
        GREATEST(
          CASE WHEN COALESCE(delta_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
            THEN (delta_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
          CASE WHEN COALESCE(delta_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
            THEN (delta_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
          CASE WHEN COALESCE(delta_job.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
            THEN (delta_job.payload_json->>'source_change_sequence')::bigint ELSE 0::bigint END
        ) AS source_change_seq
      FROM public.banking_pay_workbench_jobs AS delta_job
      WHERE delta_job.status = 'QUEUED'
        AND delta_job.run_at_utc <= v_cutoff
        AND (p_session_id IS NULL OR delta_job.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR delta_job.candidate_id = p_candidate_id)
        AND UPPER(BTRIM(COALESCE(delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        AND (
          v_allowed_job_types IS NULL
          OR delta_job.job_type = ANY(v_allowed_job_types)
          OR UPPER(BTRIM(COALESCE(delta_job.job_type, ''))) = ANY(v_allowed_job_types)
          OR 'WORKBENCH_CANDIDATE_DELTA_REFRESH' = ANY(v_allowed_job_types)
        )
      ORDER BY delta_job.priority ASC, delta_job.run_at_utc ASC, delta_job.created_at_utc ASC, delta_job.id ASC
      LIMIT GREATEST(v_limit * 10, 50)
      FOR UPDATE SKIP LOCKED
    ),
    ranked_delta AS (
      SELECT
        queued_delta.*,
        ROW_NUMBER() OVER (
          PARTITION BY queued_delta.hot_key
          ORDER BY queued_delta.source_change_seq DESC, queued_delta.priority ASC, queued_delta.run_at_utc ASC, queued_delta.updated_at_utc DESC NULLS LAST, queued_delta.created_at_utc DESC, queued_delta.id DESC
        ) AS hot_key_rank,
        COUNT(*) OVER (PARTITION BY queued_delta.hot_key) AS hot_key_count,
        MAX(queued_delta.source_change_seq) OVER (PARTITION BY queued_delta.hot_key) AS latest_hot_key_seq
      FROM queued_delta
      WHERE NULLIF(BTRIM(COALESCE(queued_delta.hot_key, '')), '') IS NOT NULL
    ),
    kept_heads AS (
      UPDATE public.banking_pay_workbench_jobs AS head_job
      SET payload_json = jsonb_strip_nulls(
            COALESCE(head_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'delta_coalesced_queue_head', true,
              'delta_coalesced_queue_duplicate_count', GREATEST(ranked_delta.hot_key_count - 1, 0),
              'latest_source_change_seq', ranked_delta.latest_hot_key_seq,
              'source_change_seq', ranked_delta.latest_hot_key_seq,
              'source_change_sequence', ranked_delta.latest_hot_key_seq,
              'claim_coalesced_at_utc', v_now::text,
              'delta_family_key', ranked_delta.hot_key,
              'normalised_delta_family_key', ranked_delta.hot_key,
              'delta_coalescing_key', ranked_delta.hot_key
            )
          ),
          updated_at_utc = v_now
      FROM ranked_delta
      WHERE head_job.id = ranked_delta.id
        AND ranked_delta.hot_key_rank = 1
        AND ranked_delta.hot_key_count > 1
      RETURNING head_job.id, ranked_delta.hot_key
    ),
    superseded_delta AS (
      UPDATE public.banking_pay_workbench_jobs AS obsolete_job
      SET status = 'SUCCEEDED',
          completed_at_utc = v_now,
          updated_at_utc = v_now,
          last_error_json = NULL::jsonb,
          payload_json = jsonb_strip_nulls(
            COALESCE(obsolete_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_by_delta_queue_coalescing', true,
              'superseded_by_delta_queue_coalescing_at_utc', v_now::text,
              'superseded_reason', 'SUPERSEDED_BY_NEWER_SOURCE_CHANGE_SEQ_BEFORE_CLAIM',
              'latest_source_change_seq', ranked_delta.latest_hot_key_seq,
              'coalesced_hot_key', ranked_delta.hot_key,
              'delta_family_key', ranked_delta.hot_key,
              'normalised_delta_family_key', ranked_delta.hot_key
            )
          )
      FROM ranked_delta
      WHERE obsolete_job.id = ranked_delta.id
        AND ranked_delta.hot_key_rank > 1
      RETURNING obsolete_job.id, ranked_delta.hot_key
    )
    SELECT
      COALESCE((SELECT COUNT(*) FROM superseded_delta), 0)::integer,
      GREATEST(
        COALESCE((SELECT COUNT(DISTINCT superseded_delta.hot_key) FROM superseded_delta), 0),
        COALESCE((SELECT COUNT(DISTINCT kept_heads.hot_key) FROM kept_heads), 0)
      )::integer
    INTO v_delta_queued_coalesced_count, v_delta_queued_coalesced_hot_key_count;
  EXCEPTION WHEN OTHERS THEN
    -- Queue compaction is an optimisation.  Claiming must continue if diagnostic payloads are malformed.
    v_delta_queued_coalesced_count := 0;
    v_delta_queued_coalesced_hot_key_count := 0;
  END;

  IF v_allowed_job_types IS NULL
     OR 'WORKBENCH_CANDIDATE_DELTA_REFRESH' = ANY(v_allowed_job_types) THEN
    WITH queued_delta_continuation AS (
      SELECT
        stale_job.id,
        stale_job.session_id,
        stale_job.candidate_id,
        stale_job.payload_json,
        stale_job.created_at_utc,
        COALESCE(live_change.seq, 0) AS live_source_change_seq,
        GREATEST(
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json->>'latest_source_change_seq')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json->>'source_change_seq')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json->>'source_change_sequence')::bigint END, 0)
        ) AS payload_source_change_seq,
        GREATEST(
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{cursor,source_change_seq}')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{cursor,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{cursor,cursor,source_change_seq}')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{cursor_json,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{cursor_json,source_change_seq}')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{cursor_json,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{cursor_json,cursor,source_change_seq}')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{result_json,next_cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{result_json,next_cursor,source_change_seq}')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{result_json,next_cursor,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{result_json,next_cursor,cursor,source_change_seq}')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{result_json,next_cursor_json,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{result_json,next_cursor_json,source_change_seq}')::bigint END, 0),
          COALESCE(CASE WHEN COALESCE(stale_job.payload_json#>>'{result_json,next_cursor_json,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (stale_job.payload_json#>>'{result_json,next_cursor_json,cursor,source_change_seq}')::bigint END, 0)
        ) AS cursor_source_change_seq,
        COALESCE(projection_run.source_change_seq, 0) AS projection_run_source_change_seq,
        COALESCE(stale_job.payload_json->>'normalised_delta_family_key', stale_job.payload_json->>'delta_family_key', stale_job.payload_json->>'delta_coalescing_key', '') AS normalised_delta_family_key,
        projection_id.projection_run_id_text,
        projection_id.projection_run_id
      FROM public.banking_pay_workbench_jobs AS stale_job
      CROSS JOIN LATERAL (
        SELECT
          COALESCE(
            stale_job.payload_json->>'projection_run_id',
            stale_job.payload_json#>>'{cursor,projection_run_id}',
            stale_job.payload_json#>>'{cursor,cursor,projection_run_id}',
            stale_job.payload_json#>>'{cursor_json,projection_run_id}',
            stale_job.payload_json#>>'{cursor_json,cursor,projection_run_id}',
            stale_job.payload_json#>>'{result_json,next_cursor,projection_run_id}',
            stale_job.payload_json#>>'{result_json,next_cursor,cursor,projection_run_id}',
            stale_job.payload_json#>>'{result_json,next_cursor_json,projection_run_id}',
            stale_job.payload_json#>>'{result_json,next_cursor_json,cursor,projection_run_id}',
            ''
          ) AS projection_run_id_text
      ) AS projection_id_text
      CROSS JOIN LATERAL (
        SELECT
          projection_id_text.projection_run_id_text,
          CASE
            WHEN projection_id_text.projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN projection_id_text.projection_run_id_text::uuid
            ELSE NULL::uuid
          END AS projection_run_id
      ) AS projection_id
      LEFT JOIN public.app_change_counters AS live_change
        ON live_change.entity_key = 'pay_candidate:' || stale_job.candidate_id::text
      LEFT JOIN public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
        ON projection_run.id = projection_id.projection_run_id
      WHERE stale_job.status = 'QUEUED'
        AND stale_job.run_at_utc <= v_cutoff
        AND (p_session_id IS NULL OR stale_job.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR stale_job.candidate_id = p_candidate_id)
        AND UPPER(BTRIM(COALESCE(stale_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        AND (
          lower(BTRIM(COALESCE(stale_job.payload_json->>'continuation', 'false'))) IN ('true','t','1','yes','y','on')
          OR UPPER(BTRIM(COALESCE(stale_job.payload_json->>'run_mode', ''))) = 'BOUNDED_CONTINUATION'
          OR jsonb_typeof(stale_job.payload_json->'cursor') = 'object'
          OR jsonb_typeof(stale_job.payload_json->'cursor_json') = 'object'
          OR jsonb_typeof(stale_job.payload_json#>'{result_json,next_cursor}') = 'object'
          OR jsonb_typeof(stale_job.payload_json#>'{result_json,next_cursor_json}') = 'object'
        )
      ORDER BY stale_job.priority ASC, stale_job.run_at_utc ASC, stale_job.created_at_utc ASC, stale_job.id ASC
      LIMIT GREATEST(v_limit * 10, 50)
      FOR UPDATE OF stale_job SKIP LOCKED
    ), stale_delta_continuation AS (
      SELECT qdc.*
      FROM queued_delta_continuation AS qdc
      WHERE (
        (COALESCE(qdc.cursor_source_change_seq, 0) > 0 AND GREATEST(COALESCE(qdc.live_source_change_seq, 0), COALESCE(qdc.payload_source_change_seq, 0)) > COALESCE(qdc.cursor_source_change_seq, 0))
        OR (COALESCE(qdc.projection_run_source_change_seq, 0) > 0 AND GREATEST(COALESCE(qdc.live_source_change_seq, 0), COALESCE(qdc.payload_source_change_seq, 0)) > COALESCE(qdc.projection_run_source_change_seq, 0))
      )
    ), terminalised_delta_projection AS MATERIALIZED (
      SELECT
        stale_delta_continuation.*,
        public._pay_workbench_delta_projection_terminalise_if_orphaned(
          p_projection_run_id => stale_delta_continuation.projection_run_id,
          p_session_id => stale_delta_continuation.session_id,
          p_candidate_id => stale_delta_continuation.candidate_id,
          p_superseded_job_id => stale_delta_continuation.id,
          p_payload_source_change_seq => stale_delta_continuation.payload_source_change_seq,
          p_cursor_source_change_seq => stale_delta_continuation.cursor_source_change_seq,
          p_projection_run_source_change_seq => stale_delta_continuation.projection_run_source_change_seq,
          p_live_candidate_source_change_seq => stale_delta_continuation.live_source_change_seq,
          p_reason => 'STALE_CONTINUATION_SUPERSEDED_BEFORE_CLAIM',
          p_now_utc => v_now,
          p_actor_user_id => NULL::uuid
        ) AS terminalise_json
      FROM stale_delta_continuation
    ), superseded_delta_continuation AS (
      UPDATE public.banking_pay_workbench_jobs AS upd_stale
      SET status = 'SUCCEEDED',
          completed_at_utc = v_now,
          updated_at_utc = v_now,
          last_error_json = NULL::jsonb,
          payload_json = jsonb_strip_nulls(
            COALESCE(upd_stale.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'superseded_stale_continuation_before_claim', true,
              'superseded_reason', 'STALE_CONTINUATION_SUPERSEDED_BEFORE_CLAIM',
              'superseded_at_utc', v_now::text,
              'payload_source_change_seq', terminalised_delta_projection.payload_source_change_seq,
              'cursor_source_change_seq', terminalised_delta_projection.cursor_source_change_seq,
              'projection_run_source_change_seq', terminalised_delta_projection.projection_run_source_change_seq,
              'live_candidate_source_change_seq', terminalised_delta_projection.live_source_change_seq,
              'normalised_delta_family_key', terminalised_delta_projection.normalised_delta_family_key,
              'projection_run_id', NULLIF(terminalised_delta_projection.projection_run_id_text, ''),
              'projection_run_terminalised', lower(BTRIM(COALESCE(terminalised_delta_projection.terminalise_json->>'projection_run_terminalised', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
              'projection_run_status_after', terminalised_delta_projection.terminalise_json->>'projection_run_status_after',
              'projection_run_terminalised_reason', terminalised_delta_projection.terminalise_json->>'projection_run_terminalised_reason',
              'projection_run_terminalisation_json', terminalised_delta_projection.terminalise_json,
              'no_active_continuation_job', lower(BTRIM(COALESCE(terminalised_delta_projection.terminalise_json->>'no_active_continuation_job', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
              'candidate_serial_unblocked', lower(BTRIM(COALESCE(terminalised_delta_projection.terminalise_json->>'candidate_serial_unblocked', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
          )
      FROM terminalised_delta_projection
      WHERE upd_stale.id = terminalised_delta_projection.id
      RETURNING upd_stale.id,
                terminalised_delta_projection.session_id,
                terminalised_delta_projection.candidate_id,
                terminalised_delta_projection.normalised_delta_family_key,
                terminalised_delta_projection.payload_source_change_seq,
                terminalised_delta_projection.cursor_source_change_seq,
                terminalised_delta_projection.projection_run_source_change_seq,
                terminalised_delta_projection.live_source_change_seq,
                terminalised_delta_projection.projection_run_id_text,
                terminalised_delta_projection.projection_run_id,
                terminalised_delta_projection.terminalise_json
    )
    SELECT COALESCE(COUNT(*), 0)::integer,
           COALESCE(jsonb_agg(jsonb_build_object(
             'job_id', superseded_delta_continuation.id::text,
             'session_id', superseded_delta_continuation.session_id::text,
             'candidate_id', superseded_delta_continuation.candidate_id::text,
             'normalised_delta_family_key', superseded_delta_continuation.normalised_delta_family_key,
             'payload_source_change_seq', superseded_delta_continuation.payload_source_change_seq,
             'cursor_source_change_seq', superseded_delta_continuation.cursor_source_change_seq,
             'projection_run_source_change_seq', superseded_delta_continuation.projection_run_source_change_seq,
             'live_candidate_source_change_seq', superseded_delta_continuation.live_source_change_seq,
             'projection_run_id', NULLIF(superseded_delta_continuation.projection_run_id_text, ''),
             'projection_run_terminalised', lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'projection_run_terminalised', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
             'projection_run_status_after', superseded_delta_continuation.terminalise_json->>'projection_run_status_after',
             'projection_run_terminalised_reason', superseded_delta_continuation.terminalise_json->>'projection_run_terminalised_reason',
             'no_active_continuation_job', lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'no_active_continuation_job', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
             'candidate_serial_unblocked', lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'candidate_serial_unblocked', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
             'projection_terminalisation_json', superseded_delta_continuation.terminalise_json
           ) ORDER BY superseded_delta_continuation.id), '[]'::jsonb),
           COALESCE(COUNT(*) FILTER (WHERE lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'projection_run_terminalised', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')), 0)::integer,
           COALESCE(jsonb_agg(jsonb_build_object(
             'job_id', superseded_delta_continuation.id::text,
             'projection_run_id', NULLIF(superseded_delta_continuation.projection_run_id_text, ''),
             'projection_run_terminalised', lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'projection_run_terminalised', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
             'projection_run_status_after', superseded_delta_continuation.terminalise_json->>'projection_run_status_after',
             'projection_run_terminalised_reason', superseded_delta_continuation.terminalise_json->>'projection_run_terminalised_reason',
             'no_active_continuation_job', lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'no_active_continuation_job', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
             'candidate_serial_unblocked', lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'candidate_serial_unblocked', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           ) ORDER BY superseded_delta_continuation.id) FILTER (WHERE lower(BTRIM(COALESCE(superseded_delta_continuation.terminalise_json->>'projection_run_terminalised', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')), '[]'::jsonb)
    INTO v_delta_stale_continuation_superseded_count,
         v_delta_stale_continuation_sample,
         v_delta_stale_projection_terminalised_count,
         v_delta_stale_projection_terminalisation_sample
    FROM superseded_delta_continuation;

    IF COALESCE(v_delta_stale_continuation_superseded_count, 0) > 0 THEN
      PERFORM public._audit_insert(
        'banking_pay_workbench_job',
        'delta_stale_continuation_preclaim',
        'STALE_CONTINUATION_SUPERSEDED_BEFORE_CLAIM',
        NULL::jsonb,
        jsonb_build_object(
          'superseded_stale_continuation_count', v_delta_stale_continuation_superseded_count,
          'projection_run_terminalised_count', COALESCE(v_delta_stale_projection_terminalised_count, 0),
          'projection_run_terminalised', COALESCE(v_delta_stale_projection_terminalised_count, 0) > 0,
          'projection_terminalisation_sample', COALESCE(v_delta_stale_projection_terminalisation_sample, '[]'::jsonb),
          'sample', v_delta_stale_continuation_sample,
          'claim_session_filter', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
          'claim_candidate_filter', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
          'session_progress_lock_skipped', true,
          'claimed_running_skipped', true,
          'candidate_serial_unblocked', COALESCE(v_delta_stale_projection_terminalised_count, 0) > 0,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        ),
        'STALE_CONTINUATION_SUPERSEDED_BEFORE_CLAIM',
        NULL::uuid
      );
    END IF;
  END IF;

  FOR v_claimed_row IN
    WITH claim_source AS MATERIALIZED (
      SELECT
        claim_job.id,
        claim_job.job_type,
        claim_job.priority,
        claim_job.run_at_utc,
        claim_job.attempt_count,
        claim_job.max_attempts,
        claim_job.snapshot_run_id,
        claim_job.session_id,
        claim_job.candidate_id,
        claim_job.payload_json,
        claim_job.created_at_utc,
        public._pay_workbench_candidate_serial_candidate_id(claim_job.candidate_id, claim_job.payload_json) AS candidate_serial_candidate_id,
        public._pay_workbench_candidate_serial_key(public._pay_workbench_candidate_serial_candidate_id(claim_job.candidate_id, claim_job.payload_json)) AS candidate_serial_key,
        public._pay_workbench_candidate_serial_is_candidate_job(claim_job.job_type, claim_job.candidate_id, claim_job.payload_json) AS candidate_serial_required,
        (
          lower(BTRIM(COALESCE(claim_job.payload_json->>'continuation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR UPPER(BTRIM(COALESCE(claim_job.payload_json->>'run_mode', ''))) IN ('BOUNDED_CONTINUATION', 'CONTINUATION', 'STAGE_CONTINUATION')
          OR lower(BTRIM(COALESCE(claim_job.payload_json->>'fallback_from_delta', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR (
            lower(BTRIM(COALESCE(claim_job.payload_json->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            AND NULLIF(BTRIM(COALESCE(claim_job.payload_json->>'fallback_reason', '')), '') IS NOT NULL
          )
          OR (
            NULLIF(BTRIM(COALESCE(claim_job.payload_json->>'source_job_id', claim_job.payload_json->>'continuation_source_job_id', claim_job.payload_json->>'bounded_continuation_source_job_id', '')), '') IS NOT NULL
            AND UPPER(BTRIM(COALESCE(claim_job.payload_json->>'run_mode', ''))) NOT IN ('LATEST_STATE_HEAD', 'LATEST_RERUN_AFTER_RUNNING')
          )
        ) AS candidate_serial_is_chain_continuation,
        CASE
          WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
            THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
          ELSE UPPER(BTRIM(COALESCE(claim_job.job_type, '')))
        END AS canonical_job_type,
        CASE
          WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH') THEN
            COALESCE(
              NULLIF(BTRIM(COALESCE(
                claim_job.payload_json->>'normalised_delta_family_key',
                claim_job.payload_json->>'delta_family_key',
                claim_job.payload_json->>'delta_coalescing_key',
                ''
              )), ''),
              (
              SELECT
                'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
                || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                  claim_job.session_id::text,
                  (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'session_id',
                  (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                  (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'source_session_id',
                  (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'target_session_id',
                  ''
                )), ''), 'none')
                || ':version:' || COALESCE(
                  CASE
                    WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                      THEN delta_family_values.session_version_text::bigint
                    ELSE COALESCE(NULL::bigint, 0)::bigint
                  END,
                  0
                )::text
                || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.projection_mode_text, ''),
                  NULLIF(NULL::text, ''),
                  'DELTA'
                ))), ''), 'DELTA')
                || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.projection_class_text, ''),
                  NULLIF(NULL::text, ''),
                  'UNKNOWN'
                ))), ''), 'UNKNOWN')
                || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                  NULLIF(NULL::text, ''),
                  CASE
                    WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                      OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                      THEN 'TARGETED_TIMESHEETS'
                    ELSE 'CANDIDATE_FULL_LIVE'
                  END
                ))), ''), 'CANDIDATE_FULL_LIVE')
                || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                  claim_job.candidate_id::text,
                  (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'candidate_id',
                  ((COALESCE(claim_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                  ''
                )), ''), 'none')
                || ':timesheets:' || md5(
                  COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                  || ':'
                  || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
                )
              FROM (
                SELECT
                  NULLIF(BTRIM(COALESCE(
                    (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'session_version',
                    (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                    (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'version',
                    CASE WHEN NULL::bigint IS NULL THEN NULL ELSE (NULL::bigint)::text END,
                    '0'
                  )), '') AS session_version_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                    NULLIF((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                    NULLIF((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'mode', ''),
                    NULLIF(NULL::text, ''),
                    'DELTA'
                  ))) AS projection_mode_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                    NULLIF((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                    NULLIF(((COALESCE(claim_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                    NULLIF(NULL::text, ''),
                    'UNKNOWN'
                  ))) AS projection_class_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                    NULLIF((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                    NULLIF(((COALESCE(claim_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                    NULLIF(((COALESCE(claim_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                    NULLIF(NULL::text, '')
                  ))) AS refresh_scope_kind_text
              ) AS delta_family_values
              CROSS JOIN (
                SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
                FROM (
                  SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                  FROM jsonb_array_elements_text(
                    CASE
                      WHEN jsonb_typeof((COALESCE(claim_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                        THEN (COALESCE(claim_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                      WHEN jsonb_typeof((COALESCE(claim_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                        THEN jsonb_build_array((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                      WHEN jsonb_typeof(((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                      WHEN jsonb_typeof(((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                      WHEN NULLIF(BTRIM(COALESCE(
                             (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                             (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                             ''
                           )), '') IS NOT NULL
                        THEN jsonb_build_array(COALESCE(
                          (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                          (COALESCE(claim_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                        ))
                      ELSE '[]'::jsonb
                    END
                  ) AS delta_family_targeted_raw(value)
                  WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                ) AS delta_family_targeted_sorted
              ) AS delta_family_targeted
              CROSS JOIN (
                SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
                FROM (
                  SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                  FROM jsonb_array_elements_text(
                    CASE
                      WHEN jsonb_typeof((COALESCE(claim_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                        THEN (COALESCE(claim_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                      WHEN jsonb_typeof((COALESCE(claim_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                        THEN jsonb_build_array((COALESCE(claim_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                      WHEN jsonb_typeof(((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                      WHEN jsonb_typeof(((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(claim_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                      ELSE '[]'::jsonb
                    END
                  ) AS delta_family_linked_raw(value)
                  WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                ) AS delta_family_linked_sorted
              ) AS delta_family_linked
              )
            )
          ELSE COALESCE(
            NULLIF(BTRIM(COALESCE(claim_job.payload_json->>'coalescing_key', '')), ''),
            claim_job.dedupe_key,
            claim_job.id::text
          )
        END AS hot_key
      FROM public.banking_pay_workbench_jobs AS claim_job
      WHERE claim_job.status = 'QUEUED'
        AND claim_job.run_at_utc <= v_cutoff
        AND (p_session_id IS NULL OR claim_job.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR claim_job.candidate_id = p_candidate_id)
        AND (
          v_allowed_job_types IS NULL
          OR claim_job.job_type = ANY(v_allowed_job_types)
          OR UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) = ANY(v_allowed_job_types)
          OR (
            CASE
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
                THEN 'WORKBENCH_SESSION_SCOPE_SEED'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
                THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
                THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
                THEN 'WORKBENCH_SESSION_CLONE_REBASE'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
                THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
                THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
              WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
                THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
              ELSE UPPER(BTRIM(COALESCE(claim_job.job_type, '')))
            END
          ) = ANY(v_allowed_job_types)
        )
      ORDER BY
        candidate_serial_is_chain_continuation DESC,
        claim_job.priority ASC,
        claim_job.run_at_utc ASC,
        claim_job.created_at_utc ASC,
        claim_job.id ASC
      LIMIT GREATEST(v_limit * 5, v_limit)
    ),
    claim_pool AS MATERIALIZED (
      SELECT
        claim_source.*,
        ROW_NUMBER() OVER (
          PARTITION BY CASE
            WHEN claim_source.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN claim_source.hot_key
            ELSE claim_source.id::text
          END
          ORDER BY claim_source.priority ASC, claim_source.run_at_utc ASC, claim_source.created_at_utc ASC, claim_source.id ASC
        ) AS hot_key_rank,
        ROW_NUMBER() OVER (
          PARTITION BY COALESCE(claim_source.candidate_serial_key, claim_source.id::text)
          ORDER BY
            CASE WHEN claim_source.candidate_serial_required IS TRUE AND claim_source.candidate_serial_is_chain_continuation IS TRUE THEN 0 ELSE 1 END ASC,
            claim_source.priority ASC,
            claim_source.run_at_utc ASC,
            claim_source.created_at_utc ASC,
            claim_source.id ASC
        ) AS candidate_serial_rank
      FROM claim_source
    ),
    eligible_claim AS MATERIALIZED (
      SELECT claim_pool.*
      FROM claim_pool
      CROSS JOIN LATERAL (
        SELECT public._pay_workbench_candidate_serial_active_state(
          claim_pool.id,
          claim_pool.candidate_serial_candidate_id,
          claim_pool.job_type,
          claim_pool.payload_json,
          v_now
        ) AS state_json
      ) AS candidate_serial_state
      WHERE claim_pool.hot_key_rank = 1
        AND (
          claim_pool.candidate_serial_required IS NOT TRUE
          OR claim_pool.candidate_serial_rank = 1
        )
        AND (
          claim_pool.candidate_serial_required IS NOT TRUE
          OR lower(BTRIM(COALESCE(candidate_serial_state.state_json->>'blocked', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        AND NOT (
          claim_pool.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
          AND (
            lower(BTRIM(COALESCE(claim_pool.payload_json->>'continuation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            OR UPPER(BTRIM(COALESCE(claim_pool.payload_json->>'run_mode', ''))) = 'BOUNDED_CONTINUATION'
            OR jsonb_typeof(claim_pool.payload_json->'cursor') = 'object'
            OR jsonb_typeof(claim_pool.payload_json->'cursor_json') = 'object'
          )
          AND COALESCE((
            SELECT
              (
                COALESCE(delta_claim_staleness.cursor_source_change_seq, 0) > 0
                AND GREATEST(
                  COALESCE(delta_claim_live_change.seq, 0),
                  COALESCE(delta_claim_staleness.payload_source_change_seq, 0)
                ) > COALESCE(delta_claim_staleness.cursor_source_change_seq, 0)
              )
              OR (
                COALESCE(delta_claim_projection_run.source_change_seq, 0) > 0
                AND GREATEST(
                  COALESCE(delta_claim_live_change.seq, 0),
                  COALESCE(delta_claim_staleness.payload_source_change_seq, 0)
                ) > COALESCE(delta_claim_projection_run.source_change_seq, 0)
              )
            FROM (
              SELECT
                GREATEST(
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json->>'latest_source_change_seq')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json->>'source_change_seq')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json->>'source_change_sequence')::bigint END, 0)
                ) AS payload_source_change_seq,
                GREATEST(
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{cursor,source_change_seq}')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{cursor,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{cursor,cursor,source_change_seq}')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{cursor_json,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{cursor_json,source_change_seq}')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{cursor_json,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{cursor_json,cursor,source_change_seq}')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{result_json,next_cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{result_json,next_cursor,source_change_seq}')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{result_json,next_cursor,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{result_json,next_cursor,cursor,source_change_seq}')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{result_json,next_cursor_json,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{result_json,next_cursor_json,source_change_seq}')::bigint END, 0),
                  COALESCE(CASE WHEN COALESCE(claim_pool.payload_json#>>'{result_json,next_cursor_json,cursor,source_change_seq}', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json#>>'{result_json,next_cursor_json,cursor,source_change_seq}')::bigint END, 0)
                ) AS cursor_source_change_seq,
                COALESCE(
                  claim_pool.payload_json->>'projection_run_id',
                  claim_pool.payload_json#>>'{cursor,projection_run_id}',
                  claim_pool.payload_json#>>'{cursor,cursor,projection_run_id}',
                  claim_pool.payload_json#>>'{cursor_json,projection_run_id}',
                  claim_pool.payload_json#>>'{cursor_json,cursor,projection_run_id}',
                  claim_pool.payload_json#>>'{result_json,next_cursor,projection_run_id}',
                  claim_pool.payload_json#>>'{result_json,next_cursor,cursor,projection_run_id}',
                  claim_pool.payload_json#>>'{result_json,next_cursor_json,projection_run_id}',
                  claim_pool.payload_json#>>'{result_json,next_cursor_json,cursor,projection_run_id}',
                  ''
                ) AS projection_run_id_text
            ) AS delta_claim_staleness
            LEFT JOIN public.app_change_counters AS delta_claim_live_change
              ON delta_claim_live_change.entity_key = 'pay_candidate:' || claim_pool.candidate_id::text
            LEFT JOIN public.banking_pay_workbench_candidate_delta_projection_runs AS delta_claim_projection_run
              ON delta_claim_projection_run.id = CASE
                   WHEN delta_claim_staleness.projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                     THEN delta_claim_staleness.projection_run_id_text::uuid
                   ELSE NULL::uuid
                 END
          ), false)
        )
        AND NOT (
          claim_pool.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
          AND EXISTS (
            SELECT 1
            FROM public.banking_pay_workbench_jobs AS running_delta_job
            WHERE running_delta_job.status = 'RUNNING'
              AND running_delta_job.id IS DISTINCT FROM claim_pool.id
              AND running_delta_job.session_id IS NOT DISTINCT FROM claim_pool.session_id
              AND running_delta_job.candidate_id IS NOT DISTINCT FROM claim_pool.candidate_id
              AND UPPER(BTRIM(COALESCE(running_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
              AND COALESCE(
                    NULLIF(BTRIM(COALESCE(
                      running_delta_job.payload_json->>'normalised_delta_family_key',
                      running_delta_job.payload_json->>'delta_family_key',
                      running_delta_job.payload_json->>'delta_coalescing_key',
                      ''
                    )), ''),
                    (
                    SELECT
                      'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
                      || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                        running_delta_job.session_id::text,
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                        ''
                      )), ''), 'none')
                      || ':version:' || COALESCE(
                        CASE
                          WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                            THEN delta_family_values.session_version_text::bigint
                          ELSE COALESCE(CASE WHEN COALESCE(claim_pool.payload_json->>'session_version', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json->>'session_version')::bigint ELSE NULL::bigint END, 0)::bigint
                        END,
                        0
                      )::text
                      || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                        NULLIF(delta_family_values.projection_mode_text, ''),
                        NULLIF(NULLIF(UPPER(BTRIM(COALESCE(claim_pool.payload_json->>'projection_mode', claim_pool.payload_json->>'resolved_mode', ''))), ''), ''),
                        'DELTA'
                      ))), ''), 'DELTA')
                      || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                        NULLIF(delta_family_values.projection_class_text, ''),
                        NULLIF(NULLIF(UPPER(BTRIM(COALESCE(claim_pool.payload_json->>'projection_class', ''))), ''), ''),
                        'UNKNOWN'
                      ))), ''), 'UNKNOWN')
                      || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                        NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                        NULLIF(NULLIF(UPPER(BTRIM(COALESCE(claim_pool.payload_json->>'refresh_scope_kind', ''))), ''), ''),
                        CASE
                          WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                            OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                            THEN 'TARGETED_TIMESHEETS'
                          ELSE 'CANDIDATE_FULL_LIVE'
                        END
                      ))), ''), 'CANDIDATE_FULL_LIVE')
                      || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                        running_delta_job.candidate_id::text,
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                        ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                        ''
                      )), ''), 'none')
                      || ':timesheets:' || md5(
                        COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                        || ':'
                        || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
                      )
                    FROM (
                      SELECT
                        NULLIF(BTRIM(COALESCE(
                          (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                          (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                          (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'version',
                          CASE WHEN CASE WHEN COALESCE(claim_pool.payload_json->>'session_version', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json->>'session_version')::bigint ELSE NULL::bigint END IS NULL THEN NULL ELSE (CASE WHEN COALESCE(claim_pool.payload_json->>'session_version', '') ~ '^-?[0-9]{1,18}$' THEN (claim_pool.payload_json->>'session_version')::bigint ELSE NULL::bigint END)::text END,
                          '0'
                        )), '') AS session_version_text,
                        UPPER(BTRIM(COALESCE(
                          NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                          NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                          NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                          NULLIF(NULLIF(UPPER(BTRIM(COALESCE(claim_pool.payload_json->>'projection_mode', claim_pool.payload_json->>'resolved_mode', ''))), ''), ''),
                          'DELTA'
                        ))) AS projection_mode_text,
                        UPPER(BTRIM(COALESCE(
                          NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                          NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                          NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                          NULLIF(NULLIF(UPPER(BTRIM(COALESCE(claim_pool.payload_json->>'projection_class', ''))), ''), ''),
                          'UNKNOWN'
                        ))) AS projection_class_text,
                        UPPER(BTRIM(COALESCE(
                          NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                          NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                          NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                          NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                          NULLIF(NULLIF(UPPER(BTRIM(COALESCE(claim_pool.payload_json->>'refresh_scope_kind', ''))), ''), '')
                        ))) AS refresh_scope_kind_text
                    ) AS delta_family_values
                    CROSS JOIN (
                      SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
                      FROM (
                        SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                        FROM jsonb_array_elements_text(
                          CASE
                            WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                              THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                            WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                              THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                            WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                              THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                            WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                              THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                            WHEN NULLIF(BTRIM(COALESCE(
                                   (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                                   (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                                   ''
                                 )), '') IS NOT NULL
                              THEN jsonb_build_array(COALESCE(
                                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                              ))
                            ELSE '[]'::jsonb
                          END
                        ) AS delta_family_targeted_raw(value)
                        WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                      ) AS delta_family_targeted_sorted
                    ) AS delta_family_targeted
                    CROSS JOIN (
                      SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
                      FROM (
                        SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                        FROM jsonb_array_elements_text(
                          CASE
                            WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                              THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                            WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                              THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                            WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                              THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                            WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                              THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                            ELSE '[]'::jsonb
                          END
                        ) AS delta_family_linked_raw(value)
                        WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                      ) AS delta_family_linked_sorted
                    ) AS delta_family_linked
                    )
                  ) = claim_pool.hot_key
          )
        )
      ORDER BY claim_pool.priority ASC, claim_pool.run_at_utc ASC, claim_pool.created_at_utc ASC, claim_pool.id ASC
      LIMIT v_limit
    ),
    candidate_locked_claim AS MATERIALIZED (
      SELECT
        eligible_claim.*,
        CASE
          WHEN eligible_claim.candidate_serial_required IS NOT TRUE THEN true
          ELSE pg_try_advisory_xact_lock(hashtextextended(eligible_claim.candidate_serial_key, 24062027))
        END AS candidate_advisory_lock_granted
      FROM eligible_claim
    ),
    hot_key_locked_claim AS MATERIALIZED (
      SELECT
        candidate_locked_claim.*,
        CASE
          WHEN candidate_locked_claim.canonical_job_type <> 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN true
          ELSE pg_try_advisory_xact_lock(
            hashtextextended(
              COALESCE(NULLIF(BTRIM(candidate_locked_claim.hot_key), ''), candidate_locked_claim.id::text),
              24062026
            )
          )
        END AS hot_key_advisory_lock_granted
      FROM candidate_locked_claim
      WHERE candidate_locked_claim.candidate_advisory_lock_granted IS TRUE
    ),
    claim AS MATERIALIZED (
      SELECT hot_key_locked_claim.*
      FROM hot_key_locked_claim
      WHERE hot_key_locked_claim.hot_key_advisory_lock_granted IS TRUE
    ),
    upd AS (
      UPDATE public.banking_pay_workbench_jobs AS upd_job
      SET status = 'RUNNING',
          attempt_count = COALESCE(upd_job.attempt_count, 0) + 1,
          started_at_utc = COALESCE(upd_job.started_at_utc, v_now),
          updated_at_utc = v_now,
          completed_at_utc = NULL,
          failed_at_utc = NULL,
          last_error_json = NULL,
          payload_json = CASE
            WHEN claim_row.canonical_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
              jsonb_strip_nulls(
                COALESCE(upd_job.payload_json, '{}'::jsonb)
                || jsonb_build_object(
                  'latest_source_change_seq', GREATEST(
                    COALESCE(live_change.seq, 0),
                    CASE WHEN COALESCE(upd_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                    CASE WHEN COALESCE(upd_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
                    CASE WHEN COALESCE(upd_job.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'source_change_sequence')::bigint ELSE 0::bigint END
                  ),
                  'source_change_seq', GREATEST(
                    COALESCE(live_change.seq, 0),
                    CASE WHEN COALESCE(upd_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                    CASE WHEN COALESCE(upd_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
                    CASE WHEN COALESCE(upd_job.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'source_change_sequence')::bigint ELSE 0::bigint END
                  ),
                  'source_change_sequence', GREATEST(
                    COALESCE(live_change.seq, 0),
                    CASE WHEN COALESCE(upd_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                    CASE WHEN COALESCE(upd_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
                    CASE WHEN COALESCE(upd_job.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
                      THEN (upd_job.payload_json->>'source_change_sequence')::bigint ELSE 0::bigint END
                  ),
                  'claim_live_source_guard_applied', true,
                  'claim_live_source_seq', COALESCE(live_change.seq, 0),
                  'claimed_delta_hot_key', claim_row.hot_key,
                  'delta_family_key', claim_row.hot_key,
                  'normalised_delta_family_key', claim_row.hot_key,
                  'delta_coalescing_key', claim_row.hot_key,
                  'claimed_at_utc', v_now::text,
                  'candidate_serial_key', CASE WHEN claim_row.candidate_serial_required IS TRUE THEN claim_row.candidate_serial_key ELSE NULL END,
                  'candidate_serial_candidate_id', CASE WHEN claim_row.candidate_serial_candidate_id IS NULL THEN NULL ELSE claim_row.candidate_serial_candidate_id::text END,
                  'candidate_serial_active_chain_id', CASE WHEN claim_row.candidate_serial_required IS TRUE THEN COALESCE(NULLIF(BTRIM(COALESCE(upd_job.payload_json->>'candidate_serial_active_chain_id', '')), ''), NULLIF(BTRIM(COALESCE(upd_job.payload_json->>'source_job_id', '')), ''), upd_job.id::text) ELSE NULL END,
                  'candidate_serial_source_job_id', NULLIF(BTRIM(COALESCE(upd_job.payload_json->>'source_job_id', '')), ''),
                  'candidate_serial_started_at_utc', CASE WHEN claim_row.candidate_serial_required IS TRUE THEN v_now::text ELSE NULL END,
                  'candidate_serial_reason', CASE WHEN claim_row.candidate_serial_required IS TRUE THEN 'CANDIDATE_SERIAL_CLAIM_GRANTED' ELSE NULL END
                )
              )
            ELSE
              CASE WHEN claim_row.candidate_serial_required IS TRUE THEN
                jsonb_strip_nulls(
                  COALESCE(upd_job.payload_json, '{}'::jsonb)
                  || jsonb_build_object(
                    'claimed_at_utc', v_now::text,
                    'candidate_serial_key', claim_row.candidate_serial_key,
                    'candidate_serial_candidate_id', CASE WHEN claim_row.candidate_serial_candidate_id IS NULL THEN NULL ELSE claim_row.candidate_serial_candidate_id::text END,
                    'candidate_serial_active_chain_id', COALESCE(NULLIF(BTRIM(COALESCE(upd_job.payload_json->>'candidate_serial_active_chain_id', '')), ''), NULLIF(BTRIM(COALESCE(upd_job.payload_json->>'source_job_id', '')), ''), upd_job.id::text),
                    'candidate_serial_source_job_id', NULLIF(BTRIM(COALESCE(upd_job.payload_json->>'source_job_id', '')), ''),
                    'candidate_serial_started_at_utc', v_now::text,
                    'candidate_serial_reason', 'CANDIDATE_SERIAL_CLAIM_GRANTED'
                  )
                )
              ELSE upd_job.payload_json END
          END
      FROM claim AS claim_row
      LEFT JOIN public.app_change_counters AS live_change
        ON live_change.entity_key = 'pay_candidate:' || COALESCE(claim_row.candidate_serial_candidate_id, claim_row.candidate_id)::text
      WHERE upd_job.id = claim_row.id
        AND upd_job.status = 'QUEUED'
        AND upd_job.run_at_utc <= v_cutoff
      RETURNING
        upd_job.id,
        upd_job.job_type,
        upd_job.priority,
        upd_job.run_at_utc,
        upd_job.attempt_count,
        upd_job.max_attempts,
        upd_job.snapshot_run_id,
        upd_job.session_id,
        upd_job.candidate_id,
        upd_job.payload_json,
        upd_job.started_at_utc,
        upd_job.created_at_utc
    )
    SELECT
      upd.id,
      upd.job_type,
      upd.priority,
      upd.run_at_utc,
      upd.attempt_count,
      upd.max_attempts,
      upd.snapshot_run_id,
      upd.session_id,
      upd.candidate_id,
      upd.payload_json,
      upd.started_at_utc,
      upd.created_at_utc
    FROM upd
    ORDER BY upd.priority ASC, upd.run_at_utc ASC, upd.created_at_utc ASC, upd.id ASC
  LOOP
    v_claimed := v_claimed || jsonb_build_array(
      jsonb_build_object(
        'job_id', v_claimed_row.id::text,
        'job_type', v_claimed_row.job_type,
        'priority', v_claimed_row.priority,
        'run_at_utc', v_claimed_row.run_at_utc,
        'attempt_count', v_claimed_row.attempt_count,
        'max_attempts', v_claimed_row.max_attempts,
        'snapshot_run_id', CASE WHEN v_claimed_row.snapshot_run_id IS NULL THEN NULL ELSE v_claimed_row.snapshot_run_id::text END,
        'session_id', CASE WHEN v_claimed_row.session_id IS NULL THEN NULL ELSE v_claimed_row.session_id::text END,
        'candidate_id', CASE WHEN v_claimed_row.candidate_id IS NULL THEN NULL ELSE v_claimed_row.candidate_id::text END,
        'payload_json', v_claimed_row.payload_json,
        'started_at_utc', v_claimed_row.started_at_utc,
        'created_at_utc', v_claimed_row.created_at_utc
      )
    );

    v_claimed_count := v_claimed_count + 1;
    IF UPPER(BTRIM(COALESCE(v_claimed_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH') THEN
      v_claimed_delta_refresh_count := v_claimed_delta_refresh_count + 1;
    ELSIF UPPER(BTRIM(COALESCE(v_claimed_row.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE') THEN
      v_claimed_clone_rebase_count := v_claimed_clone_rebase_count + 1;
    END IF;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_claimed_row.id::text,
      'RUNNING',
      NULL,
      jsonb_build_object(
        'id', v_claimed_row.id::text,
        'job_type', v_claimed_row.job_type,
        'status', 'RUNNING',
        'attempt_count', v_claimed_row.attempt_count,
        'max_attempts', v_claimed_row.max_attempts,
        'run_at_utc', v_claimed_row.run_at_utc,
        'started_at_utc', v_claimed_row.started_at_utc,
        'snapshot_run_id', CASE WHEN v_claimed_row.snapshot_run_id IS NULL THEN NULL ELSE v_claimed_row.snapshot_run_id::text END,
        'session_id', CASE WHEN v_claimed_row.session_id IS NULL THEN NULL ELSE v_claimed_row.session_id::text END,
        'candidate_id', CASE WHEN v_claimed_row.candidate_id IS NULL THEN NULL ELSE v_claimed_row.candidate_id::text END,
        'candidate_serial_key', v_claimed_row.payload_json->>'candidate_serial_key',
        'candidate_serial_active_chain_id', v_claimed_row.payload_json->>'candidate_serial_active_chain_id'
      ),
      'WORKBENCH_JOB_CLAIMED',
      NULL
    );

    IF NULLIF(BTRIM(COALESCE(v_claimed_row.payload_json->>'candidate_serial_key', '')), '') IS NOT NULL THEN
      PERFORM public._pay_workbench_candidate_serial_audit(
        'CANDIDATE_SERIAL_CLAIM_GRANTED',
        v_claimed_row.id,
        public._pay_workbench_candidate_serial_candidate_id(v_claimed_row.candidate_id, v_claimed_row.payload_json),
        jsonb_build_object(
          'job_type', v_claimed_row.job_type,
          'candidate_serial_key', v_claimed_row.payload_json->>'candidate_serial_key',
          'candidate_serial_active_chain_id', v_claimed_row.payload_json->>'candidate_serial_active_chain_id',
          'claimed_at_utc', v_now::text
        ),
        'CANDIDATE_SERIAL_CLAIM_GRANTED',
        NULL::uuid
      );
    END IF;
  END LOOP;

  v_claim_mismatch_detected := (
    v_claimed_count = 0
    AND v_preclaim_due_queued_count > 0
    AND v_recovered_stale_count = 0
    AND v_dead_stale_count = 0
  );
  v_claim_lock_contention_detected := false;
  v_claim_lock_contention_count := 0;
  IF v_claim_mismatch_detected IS TRUE THEN
    BEGIN
      WITH visible_due AS (
        SELECT
          claim_job.id,
          claim_job.job_type,
          claim_job.status,
          claim_job.priority,
          claim_job.run_at_utc,
          claim_job.created_at_utc,
          claim_job.started_at_utc,
          claim_job.attempt_count,
          claim_job.max_attempts,
          claim_job.session_id,
          claim_job.candidate_id,
          claim_job.dedupe_key,
          COALESCE(claim_job.payload_json, '{}'::jsonb) AS payload_json,
          CASE
            WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE')
              THEN 'WORKBENCH_SESSION_SCOPE_SEED'
            WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE')
              THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
              THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
            WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE')
              THEN 'WORKBENCH_SESSION_CLONE_REBASE'
            WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH')
              THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
            WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK')
              THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
            WHEN UPPER(BTRIM(COALESCE(claim_job.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK')
              THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
            ELSE UPPER(BTRIM(COALESCE(claim_job.job_type, '')))
          END AS canonical_job_type
        FROM public.banking_pay_workbench_jobs AS claim_job
        WHERE UPPER(BTRIM(COALESCE(claim_job.status, ''))) = 'QUEUED'
          AND claim_job.run_at_utc <= v_cutoff
          AND (p_session_id IS NULL OR claim_job.session_id = p_session_id)
          AND (p_candidate_id IS NULL OR claim_job.candidate_id = p_candidate_id)
        ORDER BY claim_job.priority ASC, claim_job.run_at_utc ASC, claim_job.created_at_utc ASC, claim_job.id ASC
        LIMIT 5
      ),
      enriched AS (
        SELECT
          vd.*,
          public._pay_workbench_candidate_serial_candidate_id(vd.candidate_id, vd.payload_json) AS derived_candidate_id,
          public._pay_workbench_candidate_serial_is_candidate_job(vd.job_type, vd.candidate_id, vd.payload_json) AS is_candidate_job,
          public._pay_workbench_candidate_serial_key(public._pay_workbench_candidate_serial_candidate_id(vd.candidate_id, vd.payload_json)) AS derived_candidate_serial_key,
          public._pay_workbench_candidate_serial_active_state(
            vd.id,
            public._pay_workbench_candidate_serial_candidate_id(vd.candidate_id, vd.payload_json),
            vd.job_type,
            vd.payload_json,
            v_now
          ) AS serial_state_json
        FROM visible_due AS vd
      ),
      rejected AS (
        SELECT
          e.*,
          (
            SELECT same_job.id
            FROM public.banking_pay_workbench_jobs AS same_job
            WHERE same_job.id IS DISTINCT FROM e.id
              AND UPPER(BTRIM(COALESCE(same_job.status, ''))) = 'RUNNING'
              AND public._pay_workbench_candidate_serial_candidate_id(same_job.candidate_id, same_job.payload_json) = e.derived_candidate_id
              AND public._pay_workbench_candidate_serial_is_candidate_job(same_job.job_type, same_job.candidate_id, same_job.payload_json)
            ORDER BY same_job.started_at_utc ASC NULLS FIRST, same_job.created_at_utc ASC, same_job.id ASC
            LIMIT 1
          ) AS same_candidate_active_job_id,
          (
            SELECT same_projection.id
            FROM public.banking_pay_workbench_candidate_delta_projection_runs AS same_projection
            WHERE same_projection.candidate_id = e.derived_candidate_id
              AND UPPER(BTRIM(COALESCE(same_projection.status, ''))) IN ('RUNNING', 'PROCESSING', 'IN_PROGRESS')
            ORDER BY same_projection.started_at_utc ASC NULLS FIRST, same_projection.updated_at_utc ASC NULLS FIRST, same_projection.id ASC
            LIMIT 1
          ) AS same_candidate_active_projection_run_id,
          CASE
            WHEN UPPER(BTRIM(COALESCE(e.status, ''))) <> 'QUEUED' THEN 'JOB_STATUS_CHANGED_BEFORE_UPDATE'
            WHEN e.run_at_utc > v_cutoff THEN 'RUN_AT_CHANGED_OR_NOT_DUE'
            WHEN v_allowed_job_types IS NOT NULL AND NOT (e.canonical_job_type = ANY(v_allowed_job_types) OR e.job_type = ANY(v_allowed_job_types) OR UPPER(BTRIM(COALESCE(e.job_type, ''))) = ANY(v_allowed_job_types)) THEN 'ALLOWED_JOB_TYPES_FILTER_MISMATCH'
            WHEN lower(BTRIM(COALESCE(e.serial_state_json->>'candidate_serial_blocked', e.serial_state_json->>'blocked', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'CANDIDATE_SERIAL_BLOCKED'
            WHEN e.serial_state_json->>'blocking_projection_run_id' IS NOT NULL OR e.serial_state_json->>'projection_run_id' IS NOT NULL THEN 'ACTIVE_PROJECTION_RUN'
            WHEN lower(BTRIM(COALESCE(e.payload_json->>'continuation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AND e.serial_state_json->>'blocked_chain_job_id' IS NOT NULL THEN 'ACTIVE_CONTINUATION'
            ELSE 'UNKNOWN_CLAIM_QUERY_MISMATCH'
          END AS claim_reject_reason
        FROM enriched AS e
      )
      SELECT
        COALESCE(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
          'job_id', r.id::text,
          'job_type', r.job_type,
          'status', r.status,
          'candidate_id', CASE WHEN r.candidate_id IS NULL THEN NULL ELSE r.candidate_id::text END,
          'session_id', CASE WHEN r.session_id IS NULL THEN NULL ELSE r.session_id::text END,
          'dedupe_key', r.dedupe_key,
          'priority', r.priority,
          'run_at_utc', r.run_at_utc,
          'created_at_utc', r.created_at_utc,
          'started_at_utc', r.started_at_utc,
          'attempt_count', r.attempt_count,
          'max_attempts', r.max_attempts,
          'payload_candidate_serial_key', r.payload_json->>'candidate_serial_key',
          'payload_candidate_serial_wait_reason', r.payload_json->>'candidate_serial_wait_reason',
          'payload_projection_run_id', COALESCE(r.payload_json->>'projection_run_id', r.payload_json#>>'{cursor,projection_run_id}', r.payload_json#>>'{cursor_json,projection_run_id}'),
          'payload_source_change_seq', r.payload_json->>'source_change_seq',
          'payload_latest_source_change_seq', r.payload_json->>'latest_source_change_seq',
          'payload_continuation', r.payload_json->>'continuation',
          'payload_source_job_id', r.payload_json->>'source_job_id',
          'payload_run_mode', r.payload_json->>'run_mode',
          'payload_targeted_timesheet_ids', r.payload_json->'targeted_timesheet_ids',
          'is_candidate_job', r.is_candidate_job,
          'derived_candidate_id', CASE WHEN r.derived_candidate_id IS NULL THEN NULL ELSE r.derived_candidate_id::text END,
          'derived_candidate_serial_key', r.derived_candidate_serial_key,
          'candidate_serial_active_state', r.serial_state_json,
          'same_candidate_active_job_id', CASE WHEN r.same_candidate_active_job_id IS NULL THEN NULL ELSE r.same_candidate_active_job_id::text END,
          'same_candidate_active_projection_run_id', CASE WHEN r.same_candidate_active_projection_run_id IS NULL THEN NULL ELSE r.same_candidate_active_projection_run_id::text END,
          'advisory_lock_attempted', false,
          'advisory_lock_granted', NULL,
          'claim_filter_passed', false,
          'allowed_job_types_filter_passed', NOT (v_allowed_job_types IS NOT NULL AND NOT (r.canonical_job_type = ANY(v_allowed_job_types) OR r.job_type = ANY(v_allowed_job_types) OR UPPER(BTRIM(COALESCE(r.job_type, ''))) = ANY(v_allowed_job_types))),
          'claim_reject_reason', r.claim_reject_reason,
          'worker_id', NULL,
          'origin', 'pay_workbench_claim_due_jobs'
        )) ORDER BY r.priority ASC, r.run_at_utc ASC, r.created_at_utc ASC, r.id ASC), '[]'::jsonb),
        COALESCE(COUNT(*) FILTER (WHERE r.claim_reject_reason = 'CANDIDATE_SERIAL_BLOCKED'), 0)::integer,
        COALESCE(COUNT(*) FILTER (WHERE r.claim_reject_reason = 'ACTIVE_PROJECTION_RUN'), 0)::integer,
        COALESCE(COUNT(*) FILTER (WHERE r.claim_reject_reason = 'ALLOWED_JOB_TYPES_FILTER_MISMATCH'), 0)::integer,
        COALESCE(COUNT(*) FILTER (WHERE r.claim_reject_reason = 'JOB_STATUS_CHANGED_BEFORE_UPDATE'), 0)::integer,
        COALESCE(COUNT(*) FILTER (WHERE r.claim_reject_reason = 'OBSOLETE_OR_SUPERSEDED'), 0)::integer,
        COALESCE(COUNT(*) FILTER (WHERE r.claim_reject_reason = 'UNKNOWN_CLAIM_QUERY_MISMATCH'), 0)::integer,
        COALESCE((array_agg(r.claim_reject_reason ORDER BY r.priority ASC, r.run_at_utc ASC, r.created_at_utc ASC, r.id ASC))[1], 'UNKNOWN_CLAIM_QUERY_MISMATCH')
      INTO
        v_claimable_sample,
        v_candidate_serial_reject_count,
        v_active_projection_reject_count,
        v_allowed_job_types_reject_count,
        v_status_changed_before_claim_count,
        v_obsolete_or_superseded_reject_count,
        v_unknown_claim_query_mismatch_count,
        v_claim_mismatch_reason
      FROM rejected AS r;
    EXCEPTION WHEN OTHERS THEN
      v_claimable_sample := COALESCE(v_preclaim_due_queued_sample, '[]'::jsonb);
      v_claim_mismatch_reason := 'UNKNOWN_CLAIM_QUERY_MISMATCH';
      v_unknown_claim_query_mismatch_count := GREATEST(COALESCE(v_preclaim_due_queued_count, 0), 1);
    END;

    v_claim_reject_summary := jsonb_strip_nulls(jsonb_build_object(
      'candidate_serial_reject_count', COALESCE(v_candidate_serial_reject_count, 0),
      'advisory_lock_reject_count', COALESCE(v_advisory_lock_reject_count, 0),
      'skip_locked_or_concurrent_lock_count', COALESCE(v_skip_locked_or_concurrent_lock_count, 0),
      'allowed_job_types_reject_count', COALESCE(v_allowed_job_types_reject_count, 0),
      'status_changed_before_claim_count', COALESCE(v_status_changed_before_claim_count, 0),
      'obsolete_or_superseded_reject_count', COALESCE(v_obsolete_or_superseded_reject_count, 0),
      'active_projection_reject_count', COALESCE(v_active_projection_reject_count, 0),
      'unknown_claim_query_mismatch_count', COALESCE(v_unknown_claim_query_mismatch_count, 0)
    ));
    v_claim_mismatch_json := jsonb_strip_nulls(jsonb_build_object(
      'claim_mismatch_detected', true,
      'claim_mismatch_reason', COALESCE(v_claim_mismatch_reason, 'UNKNOWN_CLAIM_QUERY_MISMATCH'),
      'claimable_count', COALESCE(v_preclaim_due_queued_count, 0),
      'claimed_count', COALESCE(v_claimed_count, 0),
      'claimable_sample', COALESCE(v_claimable_sample, '[]'::jsonb),
      'claim_reject_summary', COALESCE(v_claim_reject_summary, '{}'::jsonb),
      'advisory_lock_reject_count', COALESCE(v_advisory_lock_reject_count, 0),
      'candidate_serial_reject_count', COALESCE(v_candidate_serial_reject_count, 0),
      'skip_locked_or_concurrent_lock_count', COALESCE(v_skip_locked_or_concurrent_lock_count, 0),
      'allowed_job_types_reject_count', COALESCE(v_allowed_job_types_reject_count, 0),
      'status_changed_before_claim_count', COALESCE(v_status_changed_before_claim_count, 0),
      'obsolete_or_superseded_reject_count', COALESCE(v_obsolete_or_superseded_reject_count, 0),
      'active_projection_reject_count', COALESCE(v_active_projection_reject_count, 0),
      'unknown_claim_query_mismatch_count', COALESCE(v_unknown_claim_query_mismatch_count, 0),
      'function_name', 'pay_workbench_claim_due_jobs',
      'stage', 'CLAIMABLE_BUT_UNCLAIMED_EXPLAIN',
      'filtered_session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
      'filtered_candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
      'allowed_job_types', CASE WHEN v_allowed_job_types IS NULL THEN NULL ELSE to_jsonb(v_allowed_job_types) END
    ));
    BEGIN
      PERFORM public._temp_diag_log(
        'TEMP_WORKBENCH_CLAIM_DIAG',
        'CLAIMABLE_BUT_UNCLAIMED_EXPLAIN',
        COALESCE(p_session_id::text, p_candidate_id::text, 'pay_workbench_claim_due_jobs'),
        v_claim_mismatch_json
      );
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'server_utc', v_now,
    'cutoff_utc', v_cutoff,
    'stale_cutoff_utc', v_stale_cutoff,
    'stale_running_seconds', v_stale_running_seconds,
    'limit', v_limit,
    'stale_recovery_limit', v_stale_recovery_limit,
    'due_queued_count', v_preclaim_due_queued_count,
    'visible_due_queued_count', v_preclaim_due_queued_count,
    'claimable_count', v_preclaim_due_queued_count,
    'claimable_due_count', v_preclaim_due_queued_count,
    'claim_mismatch_detected', COALESCE(v_claim_mismatch_detected, false),
    'claim_mismatch_reason', CASE WHEN v_claim_mismatch_detected THEN COALESCE(v_claim_mismatch_reason, 'UNKNOWN_CLAIM_QUERY_MISMATCH') ELSE NULL END,
    'claim_mismatch_json', COALESCE(v_claim_mismatch_json, '{}'::jsonb),
    'claimable_sample', CASE WHEN v_claim_mismatch_detected THEN COALESCE(v_claimable_sample, '[]'::jsonb) ELSE COALESCE(v_preclaim_due_queued_sample, '[]'::jsonb) END,
    'claim_reject_summary', COALESCE(v_claim_reject_summary, '{}'::jsonb),
    'advisory_lock_reject_count', COALESCE(v_advisory_lock_reject_count, 0),
    'candidate_serial_reject_count', COALESCE(v_candidate_serial_reject_count, 0),
    'skip_locked_or_concurrent_lock_count', COALESCE(v_skip_locked_or_concurrent_lock_count, 0),
    'allowed_job_types_reject_count', COALESCE(v_allowed_job_types_reject_count, 0),
    'status_changed_before_claim_count', COALESCE(v_status_changed_before_claim_count, 0),
    'obsolete_or_superseded_reject_count', COALESCE(v_obsolete_or_superseded_reject_count, 0),
    'active_projection_reject_count', COALESCE(v_active_projection_reject_count, 0),
    'unknown_claim_query_mismatch_count', COALESCE(v_unknown_claim_query_mismatch_count, 0),
    'claim_lock_contention_detected', v_claim_lock_contention_detected,
    'lock_contention_detected', v_claim_lock_contention_detected,
    'claim_lock_contention_count', v_claim_lock_contention_count,
    'lock_contention_count', v_claim_lock_contention_count,
    'delta_queued_coalesced_count', COALESCE(v_delta_queued_coalesced_count, 0),
    'delta_queued_coalesced_hot_key_count', COALESCE(v_delta_queued_coalesced_hot_key_count, 0),
    'delta_stale_continuation_superseded_before_claim_count', COALESCE(v_delta_stale_continuation_superseded_count, 0),
    'delta_stale_continuation_superseded_before_claim_sample', COALESCE(v_delta_stale_continuation_sample, '[]'::jsonb),
    'delta_stale_projection_terminalised_before_claim_count', COALESCE(v_delta_stale_projection_terminalised_count, 0),
    'delta_stale_projection_terminalised_before_claim_sample', COALESCE(v_delta_stale_projection_terminalisation_sample, '[]'::jsonb),
    'claim_lock_contention_sample', CASE
      WHEN v_claim_lock_contention_detected THEN COALESCE(v_claimable_sample, '[]'::jsonb)
      ELSE '[]'::jsonb
    END
  )
  || jsonb_build_object(
    'filtered_session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
    'filtered_candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
    'allowed_job_types', CASE WHEN v_allowed_job_types IS NULL THEN NULL ELSE to_jsonb(v_allowed_job_types) END,
    'recovered_stale_count', v_recovered_stale_count,
    'dead_stale_count', v_dead_stale_count,
    'failed_stale_count', v_dead_stale_count,
    'projection_lifecycle_repair', COALESCE(v_projection_lifecycle_repair_json, '{}'::jsonb),
    'projection_lifecycle_repaired_count', CASE WHEN COALESCE(v_projection_lifecycle_repair_json->>'repaired_count', '') ~ '^[0-9]+$' THEN (v_projection_lifecycle_repair_json->>'repaired_count')::integer ELSE 0 END,
    'invalid_source_build_poison_repair', COALESCE(v_invalid_source_build_poison_repair_json, '{}'::jsonb),
    'invalid_source_build_poison_repaired_count', CASE WHEN COALESCE(v_invalid_source_build_poison_repair_json->>'repaired_count', '') ~ '^[0-9]+$' THEN (v_invalid_source_build_poison_repair_json->>'repaired_count')::integer ELSE 0 END,
    'orphaned_pending_source_build_repair', COALESCE(v_orphaned_pending_source_build_repair_json, '{}'::jsonb),
    'orphaned_pending_source_build_repaired_count', CASE WHEN COALESCE(v_orphaned_pending_source_build_repair_json->>'repaired_count', '') ~ '^[0-9]+$' THEN (v_orphaned_pending_source_build_repair_json->>'repaired_count')::integer ELSE 0 END,
    'orphaned_pending_source_build_recovery_scheduled', LOWER(BTRIM(COALESCE(v_orphaned_pending_source_build_repair_json->>'automatic_recovery_scheduled', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'claimed_count', v_claimed_count,
    'claimed_delta_refresh_count', COALESCE(v_claimed_delta_refresh_count, 0),
    'claimed_clone_rebase_count', COALESCE(v_claimed_clone_rebase_count, 0),
    'recovered_stale', v_recovered_stale,
    'dead_stale', v_dead_stale,
    'failed_stale', v_dead_stale,
    'claimed', v_claimed
  );
END;
$function$
