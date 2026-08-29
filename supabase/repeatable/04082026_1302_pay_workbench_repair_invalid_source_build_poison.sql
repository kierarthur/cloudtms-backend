-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline, extended only to initialise typed source-build jobs.

CREATE OR REPLACE FUNCTION public.pay_workbench_repair_invalid_source_build_poison(p_session_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_limit integer DEFAULT 10, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_reason text DEFAULT 'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_POISON_REPAIR'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_limit integer := GREATEST(1, LEAST(COALESCE(p_limit, 10), 100));
  v_reason text := COALESCE(NULLIF(BTRIM(p_reason), ''), 'INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_POISON_REPAIR');
  v_repair_row record;
  v_processed_count integer := 0;
  v_repaired_count integer := 0;
  v_skipped_count integer := 0;
  v_restored_source_row_count integer := 0;
  v_total_restored_source_row_count integer := 0;
  v_requeued_line_seed_count integer := 0;
  v_repaired_scope_count integer := 0;
  v_repaired_session_count integer := 0;
  v_repairs_json jsonb := '[]'::jsonb;
  v_skips_json jsonb := '[]'::jsonb;
  v_source_rows_before_json jsonb := '{}'::jsonb;
  v_source_rows_after_json jsonb := '{}'::jsonb;
  v_bad_job_audit_before_json jsonb := '{}'::jsonb;
  v_bad_job_audit_after_json jsonb := '{}'::jsonb;
  v_line_seed_before_json jsonb := NULL::jsonb;
  v_line_seed_after_json jsonb := NULL::jsonb;
  v_scope_before_json jsonb := NULL::jsonb;
  v_scope_after_json jsonb := NULL::jsonb;
  v_session_before_json jsonb := NULL::jsonb;
  v_session_after_json jsonb := NULL::jsonb;
  v_progress_recompute_json jsonb := '{}'::jsonb;
  v_progress_recompute_failed boolean := false;
  v_progress_recompute_error_json jsonb := NULL::jsonb;
  v_source_row_object_id text := NULL::text;
  v_zero_repair_row record;
  v_zero_source_row_repaired_count integer := 0;
  v_corrected_source_build_job_queued_count integer := 0;
  v_corrected_source_build_job_reused_count integer := 0;
  v_corrected_source_build_job_succeeded_reused_count integer := 0;
  v_line_seed_attempt_count_reset_count integer := 0;
  v_zero_source_row_repairs_json jsonb := '[]'::jsonb;
  v_zero_source_row_skips_json jsonb := '[]'::jsonb;
  v_zero_corrected_job_id uuid := NULL::uuid;
  v_zero_corrected_job_status text := NULL::text;
  v_zero_corrected_job_payload_json jsonb := '{}'::jsonb;
  v_zero_existing_corrected_failed_job_id uuid := NULL::uuid;
  v_zero_source_change_seq bigint := 0;
  v_zero_session_version bigint := 0;
  v_zero_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_zero_pay_channel_scope text := 'ALL';
  v_zero_reason text := NULL::text;
  v_zero_source_session_id uuid := NULL::uuid;
  v_zero_source_session_id_text text := NULL::text;
  v_zero_source_session_version bigint := NULL::bigint;
  v_zero_source_session_signature text := NULL::text;
  v_zero_source_snapshot_run_id uuid := NULL::uuid;
  v_zero_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_zero_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_zero_source_build_seed_text text := NULL::text;
  v_zero_source_build_hash text := NULL::text;
  v_zero_source_build_run_id uuid := NULL::uuid;
  v_zero_corrected_dedupe_key text := NULL::text;
  v_zero_corrected_payload_json jsonb := '{}'::jsonb;
  v_zero_corrected_job_action text := NULL::text;
  v_zero_scope_before_json jsonb := NULL::jsonb;
  v_zero_scope_after_json jsonb := NULL::jsonb;
  v_zero_bad_job_before_json jsonb := NULL::jsonb;
  v_zero_bad_job_after_json jsonb := NULL::jsonb;
  v_zero_corrected_job_before_json jsonb := NULL::jsonb;
  v_zero_corrected_job_after_json jsonb := NULL::jsonb;
  v_zero_session_before_json jsonb := NULL::jsonb;
  v_zero_session_after_json jsonb := NULL::jsonb;
  v_post_recompute_progress_state text := NULL::text;
  v_post_recompute_scope_failed_count integer := NULL::integer;
  v_post_recompute_scope_pending_count integer := NULL::integer;
  v_orphan_repair_row record;
  v_orphan_enqueue_result jsonb := '{}'::jsonb;
  v_orphan_enqueue_error_json jsonb := NULL::jsonb;
  v_orphan_enqueue_job_id_text text := NULL::text;
  v_orphan_enqueue_job_id uuid := NULL::uuid;
  v_orphan_enqueue_job_type text := NULL::text;
  v_orphan_enqueue_job_status text := NULL::text;
  v_orphan_enqueue_source_build_run_id_text text := NULL::text;
  v_orphan_enqueue_valid boolean := false;
  v_orphan_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_orphan_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_orphan_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_orphan_pay_channel_scope text := 'ALL';
  v_orphan_source_change_seq bigint := 0;
  v_orphan_scope_before_json jsonb := NULL::jsonb;
  v_orphan_scope_after_json jsonb := NULL::jsonb;
  v_orphan_bad_job_before_json jsonb := NULL::jsonb;
  v_orphan_bad_job_after_json jsonb := NULL::jsonb;
  v_orphan_session_before_json jsonb := NULL::jsonb;
  v_orphan_session_after_json jsonb := NULL::jsonb;
  v_orphan_pending_repaired_count integer := 0;
  v_orphan_pending_failed_closed_count integer := 0;
  v_orphan_pending_repairs_json jsonb := '[]'::jsonb;
  v_orphan_pending_skips_json jsonb := '[]'::jsonb;
BEGIN
  FOR v_repair_row IN
    WITH poisoned_scope AS (
      SELECT
        scope_row.id AS scope_id,
        scope_row.session_id,
        scope_row.candidate_id,
        scope_row.status AS scope_status,
        scope_row.pending_job_id AS scope_pending_job_id,
        scope_row.dirty AS scope_dirty,
        scope_row.error_json AS scope_error_json,
        scope_row.updated_at_utc AS scope_updated_at_utc,
        NULLIF(BTRIM(COALESCE(
          scope_row.error_json->>'job_id',
          scope_row.error_json#>>'{job,job_id}',
          scope_row.error_json#>>'{source_build,job_id}',
          scope_row.error_json#>>'{last_error_json,job_id}',
          scope_row.error_json#>>'{job_error_json,job_id}',
          ''
        )), '') AS bad_job_id_text
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SOURCE_BUILD_ERROR'
        AND (p_session_id IS NULL OR scope_row.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR scope_row.candidate_id = p_candidate_id)
      ORDER BY scope_row.updated_at_utc ASC, scope_row.id ASC
      LIMIT v_limit
      FOR UPDATE OF scope_row SKIP LOCKED
    ), bad_scope AS (
      SELECT
        poisoned_scope.*,
        bad_job_id.bad_job_id,
        bad_job.job_type AS bad_job_type,
        bad_job.status AS bad_job_status,
        bad_job.attempt_count AS bad_job_attempt_count,
        bad_job.max_attempts AS bad_job_max_attempts,
        bad_job.dedupe_key AS bad_job_dedupe_key,
        bad_job.payload_json AS bad_job_payload_json,
        bad_job.last_error_json AS bad_job_last_error_json,
        bad_job.created_at_utc AS bad_job_created_at_utc,
        bad_job.updated_at_utc AS bad_job_updated_at_utc,
        bad_job.failed_at_utc AS bad_job_failed_at_utc,
        NULLIF(BTRIM(COALESCE(
          bad_job.payload_json->>'source_build_run_id',
          bad_job.payload_json#>>'{source_build,source_build_run_id}',
          bad_job.payload_json#>>'{source_build,run_id}',
          bad_job.payload_json#>>'{cursor,source_build_run_id}',
          bad_job.payload_json#>>'{cursor_json,source_build_run_id}',
          bad_job.payload_json#>>'{result_json,source_build_run_id}',
          ''
        )), '') AS bad_source_build_run_id_text,
        COALESCE(app_counter.seq, 0)::bigint AS live_candidate_change_seq
      FROM poisoned_scope
      JOIN LATERAL (
        SELECT poisoned_scope.bad_job_id_text::uuid AS bad_job_id
        WHERE poisoned_scope.bad_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS bad_job_id ON true
      JOIN public.banking_pay_workbench_jobs AS bad_job
        ON bad_job.id = bad_job_id.bad_job_id
      LEFT JOIN public.app_change_counters AS app_counter
        ON app_counter.entity_key = 'pay_candidate:' || poisoned_scope.candidate_id::text
      WHERE bad_job.session_id = poisoned_scope.session_id
        AND bad_job.candidate_id = poisoned_scope.candidate_id
        AND UPPER(BTRIM(COALESCE(bad_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        )
        AND UPPER(BTRIM(COALESCE(bad_job.status, ''))) IN ('FAILED', 'DEAD')
        AND NULLIF(BTRIM(COALESCE(
          bad_job.payload_json->>'source_build_run_id',
          bad_job.payload_json#>>'{source_build,source_build_run_id}',
          bad_job.payload_json#>>'{source_build,run_id}',
          bad_job.payload_json#>>'{cursor,source_build_run_id}',
          bad_job.payload_json#>>'{cursor_json,source_build_run_id}',
          bad_job.payload_json#>>'{result_json,source_build_run_id}',
          ''
        )), '') IS NULL
        AND (
          COALESCE(bad_job.last_error_json, '{}'::jsonb)::text ILIKE '%PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED%'
          OR COALESCE(poisoned_scope.scope_error_json, '{}'::jsonb)::text ILIKE '%STALE_RUNNING_WORKBENCH_SOURCE_BUILD_FAILED%'
          OR COALESCE(poisoned_scope.scope_error_json, '{}'::jsonb)::text ILIKE '%WORKBENCH_SOURCE_BUILD_JOB_FAILED%'
          OR COALESCE(bad_job.payload_json, '{}'::jsonb)::text ILIKE '%CLONE_REBASE_INELIGIBLE_SOURCE_BUILD%'
        )
    ), candidate_runs AS (
      SELECT
        bad_scope.*,
        source_line.source_build_run_id,
        source_line.source_change_seq,
        COUNT(*)::integer AS source_rows_error_count,
        MIN(source_line.created_at_utc) AS source_rows_first_created_at_utc,
        MAX(source_line.updated_at_utc) AS source_rows_last_updated_at_utc
      FROM bad_scope
      JOIN public.banking_pay_workbench_candidate_source_lines AS source_line
        ON source_line.session_id = bad_scope.session_id
       AND source_line.candidate_id = bad_scope.candidate_id
       AND UPPER(BTRIM(COALESCE(source_line.status, ''))) = 'ERROR'
      GROUP BY
        bad_scope.scope_id,
        bad_scope.session_id,
        bad_scope.candidate_id,
        bad_scope.scope_status,
        bad_scope.scope_pending_job_id,
        bad_scope.scope_dirty,
        bad_scope.scope_error_json,
        bad_scope.scope_updated_at_utc,
        bad_scope.bad_job_id_text,
        bad_scope.bad_job_id,
        bad_scope.bad_job_type,
        bad_scope.bad_job_status,
        bad_scope.bad_job_attempt_count,
        bad_scope.bad_job_max_attempts,
        bad_scope.bad_job_dedupe_key,
        bad_scope.bad_job_payload_json,
        bad_scope.bad_job_last_error_json,
        bad_scope.bad_job_created_at_utc,
        bad_scope.bad_job_updated_at_utc,
        bad_scope.bad_job_failed_at_utc,
        bad_scope.bad_source_build_run_id_text,
        bad_scope.live_candidate_change_seq,
        source_line.source_build_run_id,
        source_line.source_change_seq
    ), repair_choices AS (
      SELECT
        candidate_runs.*,
        good_source_job.id AS good_source_build_job_id,
        good_source_job.status AS good_source_build_job_status,
        good_source_job.updated_at_utc AS good_source_build_job_updated_at_utc,
        line_seed_job.id AS line_seed_job_id,
        line_seed_job.status AS line_seed_job_status,
        line_seed_job.attempt_count AS line_seed_attempt_count,
        line_seed_job.max_attempts AS line_seed_max_attempts,
        line_seed_job.updated_at_utc AS line_seed_updated_at_utc,
        line_seed_job.last_error_json AS line_seed_last_error_json
      FROM candidate_runs
      JOIN LATERAL (
        SELECT source_job.*
        FROM public.banking_pay_workbench_jobs AS source_job
        CROSS JOIN LATERAL (
          SELECT
            NULLIF(BTRIM(COALESCE(
              source_job.payload_json->>'source_build_run_id',
              source_job.payload_json#>>'{source_build,source_build_run_id}',
              source_job.payload_json#>>'{source_build,run_id}',
              source_job.payload_json#>>'{cursor,source_build_run_id}',
              source_job.payload_json#>>'{cursor_json,source_build_run_id}',
              source_job.payload_json#>>'{result_json,source_build_run_id}',
              ''
            )), '') AS source_build_run_id_text,
            COALESCE(
              CASE WHEN COALESCE(source_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (source_job.payload_json->>'source_change_seq')::bigint ELSE NULL::bigint END,
              CASE WHEN COALESCE(source_job.payload_json->>'source_change_sequence', '') ~ '^[0-9]{1,18}$' THEN (source_job.payload_json->>'source_change_sequence')::bigint ELSE NULL::bigint END,
              CASE WHEN COALESCE(source_job.payload_json#>>'{source_build,source_change_seq}', '') ~ '^[0-9]{1,18}$' THEN (source_job.payload_json#>>'{source_build,source_change_seq}')::bigint ELSE NULL::bigint END,
              CASE WHEN COALESCE(source_job.payload_json#>>'{result_json,source_change_seq}', '') ~ '^[0-9]{1,18}$' THEN (source_job.payload_json#>>'{result_json,source_change_seq}')::bigint ELSE NULL::bigint END
            ) AS source_change_seq
        ) AS source_job_extract
        WHERE source_job.session_id = candidate_runs.session_id
          AND source_job.candidate_id = candidate_runs.candidate_id
          AND source_job.id <> candidate_runs.bad_job_id
          AND UPPER(BTRIM(COALESCE(source_job.job_type, ''))) IN (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
            'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
            'CANDIDATE_SOURCE_BUILD',
            'CANDIDATE_SOURCE_BUILD_CHUNK',
            'SOURCE_BUILD',
            'SOURCE_BUILD_PAGE'
          )
          AND UPPER(BTRIM(COALESCE(source_job.status, ''))) = 'SUCCEEDED'
          AND source_job_extract.source_build_run_id_text = candidate_runs.source_build_run_id::text
          AND (
            source_job_extract.source_change_seq IS NULL
            OR source_job_extract.source_change_seq = candidate_runs.source_change_seq
          )
        ORDER BY source_job.updated_at_utc DESC NULLS LAST, source_job.created_at_utc DESC NULLS LAST, source_job.id DESC
        LIMIT 1
      ) AS good_source_job ON true
      JOIN LATERAL (
        SELECT line_job.*
        FROM public.banking_pay_workbench_jobs AS line_job
        CROSS JOIN LATERAL (
          SELECT NULLIF(BTRIM(COALESCE(
            line_job.payload_json->>'source_build_run_id',
            line_job.payload_json#>>'{source_build,source_build_run_id}',
            line_job.payload_json#>>'{source_build,run_id}',
            line_job.payload_json#>>'{cursor,source_build_run_id}',
            line_job.payload_json#>>'{cursor_json,source_build_run_id}',
            line_job.payload_json#>>'{source_build_cursor,source_build_run_id}',
            line_job.payload_json#>>'{result_json,source_build_run_id}',
            ''
          )), '') AS source_build_run_id_text
        ) AS line_job_extract
        WHERE line_job.session_id = candidate_runs.session_id
          AND line_job.candidate_id = candidate_runs.candidate_id
          AND UPPER(BTRIM(COALESCE(line_job.job_type, ''))) IN (
            'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
            'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE',
            'CANDIDATE_LINE_WORK_SEED',
            'CANDIDATE_LINE_WORK_SEED_PAGE',
            'LINE_WORK_SEED_PAGE',
            'SNAPSHOT_CANDIDATE_REFRESH',
            'CANDIDATE_REFRESH'
          )
          AND UPPER(BTRIM(COALESCE(line_job.status, ''))) IN ('FAILED', 'DEAD')
          AND line_job_extract.source_build_run_id_text = candidate_runs.source_build_run_id::text
          AND COALESCE(line_job.last_error_json, '{}'::jsonb)::text ILIKE '%SOURCE_ROWS_MISSING%'
        ORDER BY line_job.updated_at_utc DESC NULLS LAST, line_job.created_at_utc DESC NULLS LAST, line_job.id DESC
        LIMIT 1
      ) AS line_seed_job ON true
      ORDER BY
        CASE WHEN candidate_runs.source_change_seq = candidate_runs.live_candidate_change_seq THEN 0 ELSE 1 END,
        candidate_runs.source_change_seq DESC,
        good_source_job.updated_at_utc DESC NULLS LAST,
        line_seed_job.updated_at_utc DESC NULLS LAST
      LIMIT v_limit
    )
    SELECT *
    FROM repair_choices
  LOOP
    v_processed_count := v_processed_count + 1;
    v_restored_source_row_count := 0;
    v_progress_recompute_json := '{}'::jsonb;
    v_progress_recompute_failed := false;
    v_progress_recompute_error_json := NULL::jsonb;
    v_line_seed_before_json := NULL::jsonb;
    v_line_seed_after_json := NULL::jsonb;
    v_scope_before_json := NULL::jsonb;
    v_scope_after_json := NULL::jsonb;
    v_session_before_json := NULL::jsonb;
    v_session_after_json := NULL::jsonb;
    v_source_row_object_id := v_repair_row.source_build_run_id::text;

    v_bad_job_audit_before_json := jsonb_build_object(
      'session_id', v_repair_row.session_id::text,
      'candidate_id', v_repair_row.candidate_id::text,
      'bad_job_id', v_repair_row.bad_job_id::text,
      'job_type', v_repair_row.bad_job_type,
      'status', v_repair_row.bad_job_status,
      'attempt_count', v_repair_row.bad_job_attempt_count,
      'max_attempts', v_repair_row.bad_job_max_attempts,
      'source_build_run_id', NULL::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'last_error_json', v_repair_row.bad_job_last_error_json,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    );
    SELECT COALESCE(jsonb_agg(status_counts.status_json ORDER BY status_counts.status), '[]'::jsonb)
    INTO v_source_rows_before_json
    FROM (
      SELECT
        source_line.status,
        jsonb_build_object(
          'status', source_line.status,
          'count', COUNT(*)::integer
        ) AS status_json
      FROM public.banking_pay_workbench_candidate_source_lines AS source_line
      WHERE source_line.session_id = v_repair_row.session_id
        AND source_line.candidate_id = v_repair_row.candidate_id
        AND source_line.source_build_run_id = v_repair_row.source_build_run_id
        AND source_line.source_change_seq = v_repair_row.source_change_seq
      GROUP BY source_line.status
    ) AS status_counts;

    WITH restored_source_rows AS (
      UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line
      SET status = 'CURRENT',
          source_row_json = jsonb_strip_nulls(
            (COALESCE(source_line.source_row_json, '{}'::jsonb) - 'stale_source_build_error' - 'source_build_error')
            || jsonb_build_object(
              'source_build_poison_repair', jsonb_build_object(
                'code', 'SOURCE_BUILD_ROWS_RESTORED',
                'repaired_at_utc', v_now::text,
                'session_id', v_repair_row.session_id::text,
                'candidate_id', v_repair_row.candidate_id::text,
                'bad_job_id', v_repair_row.bad_job_id::text,
                'source_build_run_id', v_repair_row.source_build_run_id::text,
                'source_change_seq', v_repair_row.source_change_seq,
                'line_seed_job_id', v_repair_row.line_seed_job_id::text,
                'reason', v_reason,
                'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
              )
            )
          ),
          updated_at_utc = v_now
      WHERE source_line.session_id = v_repair_row.session_id
        AND source_line.candidate_id = v_repair_row.candidate_id
        AND source_line.source_build_run_id = v_repair_row.source_build_run_id
        AND source_line.source_change_seq = v_repair_row.source_change_seq
        AND UPPER(BTRIM(COALESCE(source_line.status, ''))) = 'ERROR'
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_candidate_source_lines AS existing_current_source_line
          WHERE existing_current_source_line.id <> source_line.id
            AND existing_current_source_line.session_id = source_line.session_id
            AND existing_current_source_line.candidate_id = source_line.candidate_id
            AND existing_current_source_line.session_version = source_line.session_version
            AND existing_current_source_line.source_change_seq = source_line.source_change_seq
            AND existing_current_source_line.source_build_run_id = source_line.source_build_run_id
            AND COALESCE(existing_current_source_line.timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid) = COALESCE(source_line.timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid)
            AND existing_current_source_line.line_key = source_line.line_key
            AND UPPER(BTRIM(COALESCE(existing_current_source_line.status, ''))) = 'CURRENT'
        )
      RETURNING source_line.id
    )
    SELECT COUNT(*)::integer
    INTO v_restored_source_row_count
    FROM restored_source_rows;

    SELECT COALESCE(jsonb_agg(status_counts.status_json ORDER BY status_counts.status), '[]'::jsonb)
    INTO v_source_rows_after_json
    FROM (
      SELECT
        source_line.status,
        jsonb_build_object(
          'status', source_line.status,
          'count', COUNT(*)::integer
        ) AS status_json
      FROM public.banking_pay_workbench_candidate_source_lines AS source_line
      WHERE source_line.session_id = v_repair_row.session_id
        AND source_line.candidate_id = v_repair_row.candidate_id
        AND source_line.source_build_run_id = v_repair_row.source_build_run_id
        AND source_line.source_change_seq = v_repair_row.source_change_seq
      GROUP BY source_line.status
    ) AS status_counts;

    PERFORM public._audit_insert(
      'banking_pay_workbench_candidate_source_lines',
      v_source_row_object_id,
      'SOURCE_BUILD_ROWS_RESTORED',
      jsonb_build_object(
        'session_id', v_repair_row.session_id::text,
        'candidate_id', v_repair_row.candidate_id::text,
        'bad_job_id', v_repair_row.bad_job_id::text,
        'good_source_build_job_id', v_repair_row.good_source_build_job_id::text,
        'source_build_run_id', v_repair_row.source_build_run_id::text,
        'source_change_seq', v_repair_row.source_change_seq,
        'status_counts', COALESCE(v_source_rows_before_json, '[]'::jsonb),
        'old_status', 'ERROR',
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ),
      jsonb_build_object(
        'session_id', v_repair_row.session_id::text,
        'candidate_id', v_repair_row.candidate_id::text,
        'bad_job_id', v_repair_row.bad_job_id::text,
        'good_source_build_job_id', v_repair_row.good_source_build_job_id::text,
        'source_build_run_id', v_repair_row.source_build_run_id::text,
        'source_change_seq', v_repair_row.source_change_seq,
        'source_rows_restored_count', COALESCE(v_restored_source_row_count, 0),
        'affected_row_count', COALESCE(v_restored_source_row_count, 0),
        'status_counts', COALESCE(v_source_rows_after_json, '[]'::jsonb),
        'new_status', 'CURRENT',
        'reason', v_reason,
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ),
      'SOURCE_BUILD_ROWS_RESTORED',
      NULL
    );

    IF COALESCE(v_restored_source_row_count, 0) <= 0 THEN
      v_skipped_count := v_skipped_count + 1;
      v_skips_json := v_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_repair_row.session_id::text,
        'candidate_id', v_repair_row.candidate_id::text,
        'bad_job_id', v_repair_row.bad_job_id::text,
        'source_build_run_id', v_repair_row.source_build_run_id::text,
        'source_change_seq', v_repair_row.source_change_seq,
        'reason', 'NO_ERROR_SOURCE_ROWS_RESTORED',
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      CONTINUE;
    END IF;

    UPDATE public.banking_pay_workbench_jobs AS bad_job_update
    SET payload_json = jsonb_strip_nulls(
          COALESCE(bad_job_update.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'invalid_source_build_without_run_id_failed_closed', true,
            'invalid_source_build_without_run_id_non_blocking', true,
            'non_blocking_terminal_failure', true,
            'non_blocking_terminal_failure_reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
            'non_blocking_terminal_failure_at_utc', v_now::text,
            'source_build_poison_repair_resolved', true,
            'source_build_poison_repair_resolved_at_utc', v_now::text,
            'source_build_poison_repair_reason', v_reason,
            'source_build_poison_repair_good_source_build_job_id', v_repair_row.good_source_build_job_id::text,
            'source_build_poison_repair_source_build_run_id', v_repair_row.source_build_run_id::text,
            'source_build_poison_repair_source_change_seq', v_repair_row.source_change_seq,
            'source_build_poison_repair_line_seed_job_id', v_repair_row.line_seed_job_id::text,
            'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
          )
        ),
        updated_at_utc = v_now
    WHERE bad_job_update.id = v_repair_row.bad_job_id
    RETURNING jsonb_build_object(
      'session_id', v_repair_row.session_id::text,
      'candidate_id', v_repair_row.candidate_id::text,
      'bad_job_id', bad_job_update.id::text,
      'job_type', bad_job_update.job_type,
      'status', bad_job_update.status,
      'retained_terminal', true,
      'hard_deleted', false,
      'bad_job_mutated', true,
      'source_build_run_id', NULL::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'non_blocking_terminal_failure', true,
      'invalid_source_build_without_run_id_non_blocking', true,
      'good_source_build_job_id', v_repair_row.good_source_build_job_id::text,
      'repaired_source_build_run_id', v_repair_row.source_build_run_id::text,
      'line_seed_job_id', v_repair_row.line_seed_job_id::text,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_bad_job_audit_after_json;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_repair_row.bad_job_id::text,
      'FAILED',
      v_bad_job_audit_before_json,
      v_bad_job_audit_after_json,
      'WORKBENCH_INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_FAILED_CLOSED',
      NULL
    );

    SELECT jsonb_build_object(
      'id', line_seed_job.id::text,
      'job_type', line_seed_job.job_type,
      'status', line_seed_job.status,
      'run_at_utc', line_seed_job.run_at_utc,
      'attempt_count', line_seed_job.attempt_count,
      'max_attempts', line_seed_job.max_attempts,
      'session_id', CASE WHEN line_seed_job.session_id IS NULL THEN NULL ELSE line_seed_job.session_id::text END,
      'candidate_id', CASE WHEN line_seed_job.candidate_id IS NULL THEN NULL ELSE line_seed_job.candidate_id::text END,
      'source_build_run_id', v_repair_row.source_build_run_id::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'last_error_json', line_seed_job.last_error_json,
      'failed_at_utc', line_seed_job.failed_at_utc,
      'updated_at_utc', line_seed_job.updated_at_utc,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_line_seed_before_json
    FROM public.banking_pay_workbench_jobs AS line_seed_job
    WHERE line_seed_job.id = v_repair_row.line_seed_job_id
    FOR UPDATE;

    UPDATE public.banking_pay_workbench_jobs AS line_seed_job
    SET status = 'QUEUED',
        attempt_count = 0,
        run_at_utc = v_now,
        started_at_utc = NULL::timestamptz,
        completed_at_utc = NULL::timestamptz,
        failed_at_utc = NULL::timestamptz,
        last_error_json = NULL::jsonb,
        payload_json = jsonb_strip_nulls(
          (COALESCE(line_seed_job.payload_json, '{}'::jsonb) - 'last_failure_json')
          || jsonb_build_object(
            'source_build_poison_repair', jsonb_build_object(
              'code', 'WORKBENCH_LINE_SEED_REQUEUED_AFTER_SOURCE_BUILD_POISON_REPAIR',
              'requeued_at_utc', v_now::text,
              'session_id', v_repair_row.session_id::text,
              'candidate_id', v_repair_row.candidate_id::text,
              'bad_job_id', v_repair_row.bad_job_id::text,
              'source_build_run_id', v_repair_row.source_build_run_id::text,
              'source_change_seq', v_repair_row.source_change_seq,
              'restored_source_row_count', COALESCE(v_restored_source_row_count, 0),
              'attempt_count_reset_to', 0,
              'reason', v_reason,
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            )
          )
        ),
        updated_at_utc = v_now
    WHERE line_seed_job.id = v_repair_row.line_seed_job_id
      AND UPPER(BTRIM(COALESCE(line_seed_job.status, ''))) IN ('FAILED', 'DEAD')
    RETURNING jsonb_build_object(
      'id', line_seed_job.id::text,
      'job_type', line_seed_job.job_type,
      'status', line_seed_job.status,
      'run_at_utc', line_seed_job.run_at_utc,
      'attempt_count', line_seed_job.attempt_count,
      'max_attempts', line_seed_job.max_attempts,
      'session_id', CASE WHEN line_seed_job.session_id IS NULL THEN NULL ELSE line_seed_job.session_id::text END,
      'candidate_id', CASE WHEN line_seed_job.candidate_id IS NULL THEN NULL ELSE line_seed_job.candidate_id::text END,
      'source_build_run_id', v_repair_row.source_build_run_id::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'last_error_json', line_seed_job.last_error_json,
      'failed_at_utc', line_seed_job.failed_at_utc,
      'updated_at_utc', line_seed_job.updated_at_utc,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_line_seed_after_json;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_repair_row.line_seed_job_id::text,
      'REQUEUED',
      v_line_seed_before_json,
      v_line_seed_after_json,
      'WORKBENCH_LINE_SEED_REQUEUED_AFTER_SOURCE_BUILD_POISON_REPAIR',
      NULL
    );

    IF v_line_seed_after_json IS NOT NULL THEN
      v_line_seed_attempt_count_reset_count := v_line_seed_attempt_count_reset_count + 1;
    END IF;

    SELECT jsonb_build_object(
      'id', scope_row.id::text,
      'session_id', scope_row.session_id::text,
      'candidate_id', scope_row.candidate_id::text,
      'status', scope_row.status,
      'dirty', scope_row.dirty,
      'pending_job_id', CASE WHEN scope_row.pending_job_id IS NULL THEN NULL ELSE scope_row.pending_job_id::text END,
      'error_json', scope_row.error_json,
      'source_build_run_id', v_repair_row.source_build_run_id::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'bad_job_id', v_repair_row.bad_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_scope_before_json
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.id = v_repair_row.scope_id
    FOR UPDATE;

    UPDATE public.banking_pay_workbench_session_scope AS scope_row
    SET status = 'LINE_WORK_PENDING',
        dirty = true,
        pending_job_id = v_repair_row.line_seed_job_id,
        error_json = NULL::jsonb,
        updated_at_utc = v_now
    WHERE scope_row.id = v_repair_row.scope_id
      AND scope_row.session_id = v_repair_row.session_id
      AND scope_row.candidate_id = v_repair_row.candidate_id
    RETURNING jsonb_build_object(
      'id', scope_row.id::text,
      'session_id', scope_row.session_id::text,
      'candidate_id', scope_row.candidate_id::text,
      'status', scope_row.status,
      'dirty', scope_row.dirty,
      'pending_job_id', CASE WHEN scope_row.pending_job_id IS NULL THEN NULL ELSE scope_row.pending_job_id::text END,
      'error_json', scope_row.error_json,
      'source_build_run_id', v_repair_row.source_build_run_id::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'bad_job_id', v_repair_row.bad_job_id::text,
      'restored_source_row_count', COALESCE(v_restored_source_row_count, 0),
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_scope_after_json;

    PERFORM public._audit_insert(
      'banking_pay_workbench_session_scope',
      v_repair_row.candidate_id::text,
      'SOURCE_BUILD_POISON_REPAIRED',
      v_scope_before_json,
      v_scope_after_json,
      'SOURCE_BUILD_POISON_REPAIRED',
      NULL
    );

    SELECT jsonb_build_object(
      'id', session_row.id::text,
      'status', session_row.status,
      'progress_state', session_row.progress_state,
      'progress_counter_version', session_row.progress_counter_version,
      'progress_updated_at_utc', session_row.progress_updated_at_utc,
      'progress_json', public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true),
      'session_id', session_row.id::text,
      'candidate_id', v_repair_row.candidate_id::text,
      'bad_job_id', v_repair_row.bad_job_id::text,
      'source_build_run_id', v_repair_row.source_build_run_id::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_session_before_json
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_repair_row.session_id
    FOR UPDATE;

    UPDATE public.banking_pay_workbench_sessions AS session_row
    SET progress_state = 'REFRESHING_CANDIDATES',
        progress_json = jsonb_strip_nulls(
          (
            public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true)
            - 'last_source_build_failure_at_utc'
            - 'last_source_build_failure_job_id'
            - 'last_source_build_failure_code'
            - 'last_source_build_failure_source_build_run_id'
            - 'last_source_build_source_rows_marked_error_count'
            - 'blocker_codes'
            - 'draft_blocker_codes'
            - 'session_blocker_codes'
          )
          || jsonb_build_object(
            'source_build_poison_repair', jsonb_build_object(
              'code', 'PROGRESS_REPAIRED_AFTER_SOURCE_BUILD_POISON',
              'repaired_at_utc', v_now::text,
              'session_id', v_repair_row.session_id::text,
              'candidate_id', v_repair_row.candidate_id::text,
              'bad_job_id', v_repair_row.bad_job_id::text,
              'source_build_run_id', v_repair_row.source_build_run_id::text,
              'source_change_seq', v_repair_row.source_change_seq,
              'line_seed_job_id', v_repair_row.line_seed_job_id::text,
              'restored_source_row_count', COALESCE(v_restored_source_row_count, 0),
              'reason', v_reason,
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            )
          )
        ),
        progress_counter_version = COALESCE(session_row.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_row.id = v_repair_row.session_id;

    BEGIN
      v_progress_recompute_json := public.pay_workbench_session_recompute_progress_counters(
        p_session_id => v_repair_row.session_id,
        p_apply => true,
        p_reason => 'SOURCE_BUILD_POISON_REPAIRED',
        p_write_progress_json => true
      );
    EXCEPTION WHEN OTHERS THEN
      v_progress_recompute_failed := true;
      v_progress_recompute_error_json := jsonb_build_object(
        'code', SQLSTATE,
        'message', SQLERRM,
        'session_id', v_repair_row.session_id::text,
        'candidate_id', v_repair_row.candidate_id::text,
        'bad_job_id', v_repair_row.bad_job_id::text,
        'source_build_run_id', v_repair_row.source_build_run_id::text,
        'source_change_seq', v_repair_row.source_change_seq,
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      );
      v_progress_recompute_json := COALESCE(v_progress_recompute_error_json, '{}'::jsonb);
    END;

    SELECT jsonb_build_object(
      'id', session_row.id::text,
      'status', session_row.status,
      'progress_state', session_row.progress_state,
      'progress_counter_version', session_row.progress_counter_version,
      'progress_updated_at_utc', session_row.progress_updated_at_utc,
      'progress_json', public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true),
      'session_id', session_row.id::text,
      'candidate_id', v_repair_row.candidate_id::text,
      'bad_job_id', v_repair_row.bad_job_id::text,
      'source_build_run_id', v_repair_row.source_build_run_id::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'line_seed_job_id', v_repair_row.line_seed_job_id::text,
      'restored_source_row_count', COALESCE(v_restored_source_row_count, 0),
      'recompute_failed', v_progress_recompute_failed,
      'recompute_json', COALESCE(v_progress_recompute_json, '{}'::jsonb),
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_session_after_json
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_repair_row.session_id;

    PERFORM public._audit_insert(
      'banking_pay_workbench_session',
      v_repair_row.session_id::text,
      'PROGRESS_REPAIRED_AFTER_SOURCE_BUILD_POISON',
      v_session_before_json,
      v_session_after_json,
      'PROGRESS_REPAIRED_AFTER_SOURCE_BUILD_POISON',
      NULL
    );

    v_repaired_count := v_repaired_count + 1;
    v_total_restored_source_row_count := v_total_restored_source_row_count + COALESCE(v_restored_source_row_count, 0);
    v_requeued_line_seed_count := v_requeued_line_seed_count + 1;
    v_repaired_scope_count := v_repaired_scope_count + 1;
    v_repaired_session_count := v_repaired_session_count + 1;

    v_repairs_json := v_repairs_json || jsonb_build_array(jsonb_build_object(
      'session_id', v_repair_row.session_id::text,
      'candidate_id', v_repair_row.candidate_id::text,
      'bad_job_id', v_repair_row.bad_job_id::text,
      'good_source_build_job_id', v_repair_row.good_source_build_job_id::text,
      'source_build_run_id', v_repair_row.source_build_run_id::text,
      'source_change_seq', v_repair_row.source_change_seq,
      'line_seed_job_id', v_repair_row.line_seed_job_id::text,
      'source_rows_restored_count', COALESCE(v_restored_source_row_count, 0),
      'line_seed_requeued', true,
      'line_seed_attempt_count_reset', v_line_seed_after_json IS NOT NULL,
      'scope_repaired', true,
      'session_progress_repaired', true,
      'progress_recompute_failed', v_progress_recompute_failed,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    ));
  END LOOP;

  FOR v_zero_repair_row IN
    WITH zero_poisoned_scope AS (
      SELECT
        scope_row.id AS scope_id,
        scope_row.session_id,
        scope_row.candidate_id,
        scope_row.status AS scope_status,
        scope_row.pending_job_id AS scope_pending_job_id,
        scope_row.dirty AS scope_dirty,
        scope_row.error_json AS scope_error_json,
        scope_row.updated_at_utc AS scope_updated_at_utc,
        NULLIF(BTRIM(COALESCE(
          scope_row.error_json->>'job_id',
          scope_row.error_json#>>'{job,job_id}',
          scope_row.error_json#>>'{source_build,job_id}',
          scope_row.error_json#>>'{last_error_json,job_id}',
          scope_row.error_json#>>'{job_error_json,job_id}',
          ''
        )), '') AS bad_job_id_text
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SOURCE_BUILD_ERROR'
        AND (p_session_id IS NULL OR scope_row.session_id = p_session_id)
        AND (p_candidate_id IS NULL OR scope_row.candidate_id = p_candidate_id)
      ORDER BY scope_row.updated_at_utc ASC, scope_row.id ASC
      LIMIT GREATEST(v_limit - v_processed_count, 0)
      FOR UPDATE OF scope_row SKIP LOCKED
    ), zero_bad_scope AS (
      SELECT
        zero_poisoned_scope.*,
        bad_job.id AS bad_job_id,
        bad_job.job_type AS bad_job_type,
        bad_job.status AS bad_job_status,
        bad_job.attempt_count AS bad_job_attempt_count,
        bad_job.max_attempts AS bad_job_max_attempts,
        bad_job.dedupe_key AS bad_job_dedupe_key,
        COALESCE(bad_job.payload_json, '{}'::jsonb) AS bad_job_payload_json,
        bad_job.last_error_json AS bad_job_last_error_json,
        bad_job.created_at_utc AS bad_job_created_at_utc,
        bad_job.updated_at_utc AS bad_job_updated_at_utc,
        bad_job.failed_at_utc AS bad_job_failed_at_utc,
        target_session.version AS target_session_version,
        target_session.session_signature AS target_session_signature,
        target_session.source_snapshot_run_id AS target_source_snapshot_run_id,
        COALESCE(app_counter.seq, 0)::bigint AS live_candidate_change_seq,
        NULLIF(BTRIM(COALESCE(
          bad_job.payload_json->>'source_build_run_id',
          bad_job.payload_json#>>'{source_build,source_build_run_id}',
          bad_job.payload_json#>>'{source_build,run_id}',
          bad_job.payload_json#>>'{cursor,source_build_run_id}',
          bad_job.payload_json#>>'{cursor_json,source_build_run_id}',
          bad_job.payload_json#>>'{result_json,source_build_run_id}',
          ''
        )), '') AS bad_source_build_run_id_text,
        COALESCE(
          CASE WHEN COALESCE(zero_poisoned_scope.scope_error_json->>'source_rows_marked_error_count', '') ~ '^[0-9]+$' THEN (zero_poisoned_scope.scope_error_json->>'source_rows_marked_error_count')::integer ELSE NULL::integer END,
          CASE WHEN COALESCE(zero_poisoned_scope.scope_error_json#>>'{job_error_json,source_rows_marked_error_count}', '') ~ '^[0-9]+$' THEN (zero_poisoned_scope.scope_error_json#>>'{job_error_json,source_rows_marked_error_count}')::integer ELSE NULL::integer END,
          CASE WHEN COALESCE(bad_job.last_error_json->>'source_rows_marked_error_count', '') ~ '^[0-9]+$' THEN (bad_job.last_error_json->>'source_rows_marked_error_count')::integer ELSE NULL::integer END,
          CASE WHEN COALESCE(bad_job.payload_json#>>'{last_failure_json,source_rows_marked_error_count}', '') ~ '^[0-9]+$' THEN (bad_job.payload_json#>>'{last_failure_json,source_rows_marked_error_count}')::integer ELSE NULL::integer END,
          0
        ) AS source_rows_marked_error_count,
        NULLIF(BTRIM(COALESCE(
          bad_job.payload_json->>'source_session_id',
          bad_job.payload_json->>'clone_from_session_id',
          bad_job.payload_json#>>'{source_build,source_session_id}',
          bad_job.payload_json#>>'{source_build,clone_from_session_id}',
          zero_poisoned_scope.scope_error_json->>'source_session_id',
          zero_poisoned_scope.scope_error_json->>'clone_from_session_id',
          ''
        )), '') AS source_session_id_text,
        COALESCE(
          CASE WHEN COALESCE(bad_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (bad_job.payload_json->>'source_change_seq')::bigint ELSE NULL::bigint END,
          CASE WHEN COALESCE(bad_job.payload_json->>'source_change_sequence', '') ~ '^[0-9]{1,18}$' THEN (bad_job.payload_json->>'source_change_sequence')::bigint ELSE NULL::bigint END,
          CASE WHEN COALESCE(bad_job.payload_json#>>'{source_build,source_change_seq}', '') ~ '^[0-9]{1,18}$' THEN (bad_job.payload_json#>>'{source_build,source_change_seq}')::bigint ELSE NULL::bigint END,
          CASE WHEN COALESCE(zero_poisoned_scope.scope_error_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (zero_poisoned_scope.scope_error_json->>'source_change_seq')::bigint ELSE NULL::bigint END,
          COALESCE(app_counter.seq, 0)::bigint,
          0::bigint
        ) AS repaired_source_change_seq,
        COALESCE(
          CASE WHEN COALESCE(bad_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$' THEN (bad_job.payload_json->>'session_version')::bigint ELSE NULL::bigint END,
          CASE WHEN COALESCE(bad_job.payload_json#>>'{source_build,session_version}', '') ~ '^[0-9]{1,18}$' THEN (bad_job.payload_json#>>'{source_build,session_version}')::bigint ELSE NULL::bigint END,
          target_session.version,
          1::bigint
        ) AS repaired_session_version,
        COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
          bad_job.payload_json->>'refresh_scope_kind',
          bad_job.payload_json#>>'{source_build,refresh_scope_kind}',
          zero_poisoned_scope.scope_error_json->>'refresh_scope_kind',
          ''
        ))), ''), 'CANDIDATE_FULL_LIVE') AS repaired_refresh_scope_kind,
        COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
          bad_job.payload_json->>'pay_channel_scope',
          bad_job.payload_json#>>'{source_build,pay_channel_scope}',
          zero_poisoned_scope.scope_error_json->>'pay_channel_scope',
          ''
        ))), ''), 'ALL') AS repaired_pay_channel_scope,
        COALESCE(NULLIF(BTRIM(COALESCE(
          bad_job.payload_json->>'fallback_reason',
          bad_job.payload_json->>'refresh_reason',
          bad_job.payload_json->>'reason',
          bad_job.payload_json#>>'{source_build,reason}',
          zero_poisoned_scope.scope_error_json->>'reason',
          ''
        )), ''), 'CLONE_REBASE_INELIGIBLE') AS repaired_reason
      FROM zero_poisoned_scope
      JOIN LATERAL (
        SELECT zero_poisoned_scope.bad_job_id_text::uuid AS bad_job_id
        WHERE zero_poisoned_scope.bad_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS bad_job_id ON true
      JOIN public.banking_pay_workbench_jobs AS bad_job
        ON bad_job.id = bad_job_id.bad_job_id
      JOIN public.banking_pay_workbench_sessions AS target_session
        ON target_session.id = zero_poisoned_scope.session_id
      LEFT JOIN public.app_change_counters AS app_counter
        ON app_counter.entity_key = 'pay_candidate:' || zero_poisoned_scope.candidate_id::text
      WHERE bad_job.session_id = zero_poisoned_scope.session_id
        AND bad_job.candidate_id = zero_poisoned_scope.candidate_id
        AND UPPER(BTRIM(COALESCE(bad_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        )
        AND UPPER(BTRIM(COALESCE(bad_job.status, ''))) IN ('FAILED', 'DEAD')
        AND NULLIF(BTRIM(COALESCE(
          bad_job.payload_json->>'source_build_run_id',
          bad_job.payload_json#>>'{source_build,source_build_run_id}',
          bad_job.payload_json#>>'{source_build,run_id}',
          bad_job.payload_json#>>'{cursor,source_build_run_id}',
          bad_job.payload_json#>>'{cursor_json,source_build_run_id}',
          bad_job.payload_json#>>'{result_json,source_build_run_id}',
          ''
        )), '') IS NULL
        AND (
          COALESCE(bad_job.last_error_json, '{}'::jsonb)::text ILIKE '%PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED%'
          OR COALESCE(zero_poisoned_scope.scope_error_json, '{}'::jsonb)::text ILIKE '%PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED%'
          OR COALESCE(bad_job.payload_json, '{}'::jsonb)::text ILIKE '%CLONE_REBASE_INELIGIBLE_SOURCE_BUILD%'
          OR COALESCE(zero_poisoned_scope.scope_error_json, '{}'::jsonb)::text ILIKE '%CLONE_REBASE_INELIGIBLE_SOURCE_BUILD%'
        )
        AND COALESCE(
          CASE WHEN COALESCE(zero_poisoned_scope.scope_error_json->>'source_rows_marked_error_count', '') ~ '^[0-9]+$' THEN (zero_poisoned_scope.scope_error_json->>'source_rows_marked_error_count')::integer ELSE NULL::integer END,
          CASE WHEN COALESCE(zero_poisoned_scope.scope_error_json#>>'{job_error_json,source_rows_marked_error_count}', '') ~ '^[0-9]+$' THEN (zero_poisoned_scope.scope_error_json#>>'{job_error_json,source_rows_marked_error_count}')::integer ELSE NULL::integer END,
          CASE WHEN COALESCE(bad_job.last_error_json->>'source_rows_marked_error_count', '') ~ '^[0-9]+$' THEN (bad_job.last_error_json->>'source_rows_marked_error_count')::integer ELSE NULL::integer END,
          CASE WHEN COALESCE(bad_job.payload_json#>>'{last_failure_json,source_rows_marked_error_count}', '') ~ '^[0-9]+$' THEN (bad_job.payload_json#>>'{last_failure_json,source_rows_marked_error_count}')::integer ELSE NULL::integer END,
          0
        ) = 0
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_candidate_source_lines AS error_source_line
          WHERE error_source_line.session_id = zero_poisoned_scope.session_id
            AND error_source_line.candidate_id = zero_poisoned_scope.candidate_id
            AND UPPER(BTRIM(COALESCE(error_source_line.status, ''))) = 'ERROR'
        )
    ), zero_authority AS (
      SELECT
        zero_bad_scope.*,
        source_session.id AS source_session_id,
        source_session.version AS source_session_version,
        source_session.session_signature AS source_session_signature,
        source_session.source_snapshot_run_id AS source_snapshot_run_id,
        COALESCE(targeted_ids.targeted_timesheet_ids_json, '[]'::jsonb) AS targeted_timesheet_ids_json,
        COALESCE(linked_ids.linked_timesheet_ids_json, '[]'::jsonb) AS linked_timesheet_ids_json,
        CASE
          WHEN zero_bad_scope.source_session_id_text IS NULL THEN true
          WHEN zero_bad_scope.source_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               AND source_session.id IS NOT NULL THEN true
          ELSE false
        END AS authority_ok,
        CASE
          WHEN zero_bad_scope.source_session_id_text IS NOT NULL
               AND zero_bad_scope.source_session_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN 'ZERO_SOURCE_ROW_REPAIR_INVALID_SOURCE_SESSION_ID'
          WHEN zero_bad_scope.source_session_id_text IS NOT NULL
               AND source_session.id IS NULL
            THEN 'ZERO_SOURCE_ROW_REPAIR_SOURCE_SESSION_NOT_FOUND'
          WHEN zero_bad_scope.target_session_signature IS NULL
               OR zero_bad_scope.target_source_snapshot_run_id IS NULL
            THEN 'ZERO_SOURCE_ROW_REPAIR_INSUFFICIENT_SOURCE_BUILD_AUTHORITY'
          ELSE NULL::text
        END AS authority_skip_reason
      FROM zero_bad_scope
      LEFT JOIN public.banking_pay_workbench_sessions AS source_session
        ON source_session.id = CASE
          WHEN zero_bad_scope.source_session_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN zero_bad_scope.source_session_id_text::uuid
          ELSE NULL::uuid
        END
      CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(targeted_sorted.timesheet_id_text ORDER BY targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
        FROM (
          SELECT DISTINCT targeted_raw.timesheet_id_text
          FROM (
            SELECT NULLIF(BTRIM(targeted_value.value), '') AS timesheet_id_text
            FROM jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->'targeted_timesheet_ids') = 'array'
                  THEN COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->'targeted_timesheet_ids'
                WHEN jsonb_typeof(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->'targeted_timesheet_ids') = 'string'
                  THEN jsonb_build_array(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->>'targeted_timesheet_ids')
                ELSE '[]'::jsonb
              END
            ) AS targeted_value(value)
            UNION ALL
            SELECT NULLIF(BTRIM(targeted_nested_value.value), '') AS timesheet_id_text
            FROM jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof((COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>'{source_build,targeted_timesheet_ids}')) = 'array'
                  THEN COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>'{source_build,targeted_timesheet_ids}'
                WHEN jsonb_typeof((COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>'{source_build,targeted_timesheet_ids}')) = 'string'
                  THEN jsonb_build_array(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>>'{source_build,targeted_timesheet_ids}')
                ELSE '[]'::jsonb
              END
            ) AS targeted_nested_value(value)
          ) AS targeted_raw
          WHERE targeted_raw.timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ) AS targeted_sorted
      ) AS targeted_ids
      CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(linked_sorted.timesheet_id_text ORDER BY linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
        FROM (
          SELECT DISTINCT linked_raw.timesheet_id_text
          FROM (
            SELECT NULLIF(BTRIM(linked_value.value), '') AS timesheet_id_text
            FROM jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->'linked_timesheet_ids') = 'array'
                  THEN COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->'linked_timesheet_ids'
                WHEN jsonb_typeof(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->'linked_timesheet_ids') = 'string'
                  THEN jsonb_build_array(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)->>'linked_timesheet_ids')
                ELSE '[]'::jsonb
              END
            ) AS linked_value(value)
            UNION ALL
            SELECT NULLIF(BTRIM(linked_nested_value.value), '') AS timesheet_id_text
            FROM jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof((COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>'{source_build,linked_timesheet_ids}')) = 'array'
                  THEN COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>'{source_build,linked_timesheet_ids}'
                WHEN jsonb_typeof((COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>'{source_build,linked_timesheet_ids}')) = 'string'
                  THEN jsonb_build_array(COALESCE(zero_bad_scope.bad_job_payload_json, '{}'::jsonb)#>>'{source_build,linked_timesheet_ids}')
                ELSE '[]'::jsonb
              END
            ) AS linked_nested_value(value)
          ) AS linked_raw
          WHERE linked_raw.timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ) AS linked_sorted
      ) AS linked_ids
    )
    SELECT *
    FROM zero_authority
  LOOP
    v_processed_count := v_processed_count + 1;
    v_progress_recompute_json := '{}'::jsonb;
    v_progress_recompute_failed := false;
    v_progress_recompute_error_json := NULL::jsonb;
    v_zero_corrected_job_id := NULL::uuid;
    v_zero_corrected_job_status := NULL::text;
    v_zero_corrected_job_payload_json := '{}'::jsonb;
    v_zero_existing_corrected_failed_job_id := NULL::uuid;
    v_zero_corrected_job_action := NULL::text;
    v_zero_source_change_seq := COALESCE(v_zero_repair_row.repaired_source_change_seq, 0);
    v_zero_session_version := COALESCE(v_zero_repair_row.repaired_session_version, v_zero_repair_row.target_session_version, 1);
    v_zero_refresh_scope_kind := COALESCE(NULLIF(UPPER(BTRIM(v_zero_repair_row.repaired_refresh_scope_kind)), ''), 'CANDIDATE_FULL_LIVE');
    v_zero_pay_channel_scope := COALESCE(NULLIF(UPPER(BTRIM(v_zero_repair_row.repaired_pay_channel_scope)), ''), 'ALL');
    v_zero_reason := COALESCE(NULLIF(BTRIM(v_zero_repair_row.repaired_reason), ''), 'CLONE_REBASE_INELIGIBLE');
    v_zero_source_session_id := v_zero_repair_row.source_session_id;
    v_zero_source_session_id_text := v_zero_repair_row.source_session_id_text;
    v_zero_source_session_version := v_zero_repair_row.source_session_version;
    v_zero_source_session_signature := v_zero_repair_row.source_session_signature;
    v_zero_source_snapshot_run_id := v_zero_repair_row.source_snapshot_run_id;
    v_zero_targeted_timesheet_ids_json := COALESCE(v_zero_repair_row.targeted_timesheet_ids_json, '[]'::jsonb);
    v_zero_linked_timesheet_ids_json := COALESCE(v_zero_repair_row.linked_timesheet_ids_json, '[]'::jsonb);

    IF COALESCE(v_zero_repair_row.authority_ok, false) IS NOT TRUE THEN
      v_skipped_count := v_skipped_count + 1;
      v_zero_source_row_skips_json := v_zero_source_row_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_zero_repair_row.session_id::text,
        'candidate_id', v_zero_repair_row.candidate_id::text,
        'bad_job_id', v_zero_repair_row.bad_job_id::text,
        'reason', COALESCE(v_zero_repair_row.authority_skip_reason, 'ZERO_SOURCE_ROW_REPAIR_INSUFFICIENT_SOURCE_BUILD_AUTHORITY'),
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      v_skips_json := v_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_zero_repair_row.session_id::text,
        'candidate_id', v_zero_repair_row.candidate_id::text,
        'bad_job_id', v_zero_repair_row.bad_job_id::text,
        'reason', COALESCE(v_zero_repair_row.authority_skip_reason, 'ZERO_SOURCE_ROW_REPAIR_INSUFFICIENT_SOURCE_BUILD_AUTHORITY'),
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      CONTINUE;
    END IF;

    v_zero_source_build_seed_text := concat_ws(
      ':',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'ZERO_SOURCE_ROW_POISON_REPAIR',
      'target_session_id', v_zero_repair_row.session_id::text,
      'target_session_version', COALESCE(v_zero_session_version, 1)::text,
      'target_session_signature', COALESCE(v_zero_repair_row.target_session_signature, ''),
      'target_snapshot_run_id', COALESCE(v_zero_repair_row.target_source_snapshot_run_id::text, ''),
      'source_session_id', COALESCE(v_zero_source_session_id::text, COALESCE(v_zero_source_session_id_text, '')),
      'source_session_version', COALESCE(v_zero_source_session_version::text, ''),
      'source_session_signature', COALESCE(v_zero_source_session_signature, ''),
      'source_snapshot_run_id', COALESCE(v_zero_source_snapshot_run_id::text, ''),
      'candidate_id', v_zero_repair_row.candidate_id::text,
      'source_change_seq', COALESCE(v_zero_source_change_seq, 0)::text,
      'refresh_scope_kind', v_zero_refresh_scope_kind,
      'targeted_timesheet_ids', COALESCE(v_zero_targeted_timesheet_ids_json, '[]'::jsonb)::text,
      'linked_timesheet_ids', COALESCE(v_zero_linked_timesheet_ids_json, '[]'::jsonb)::text,
      'pay_channel_scope', v_zero_pay_channel_scope,
      'clone_from_session_id', COALESCE(v_zero_source_session_id::text, COALESCE(v_zero_source_session_id_text, '')),
      'clone_to_session_id', v_zero_repair_row.session_id::text,
      'reason', v_zero_reason
    );
    v_zero_source_build_hash := md5(v_zero_source_build_seed_text);
    v_zero_source_build_run_id := (
      substr(v_zero_source_build_hash, 1, 8) || '-' ||
      substr(v_zero_source_build_hash, 9, 4) || '-' ||
      substr(v_zero_source_build_hash, 13, 4) || '-' ||
      substr(v_zero_source_build_hash, 17, 4) || '-' ||
      substr(v_zero_source_build_hash, 21, 12)
    )::uuid;
    v_zero_corrected_dedupe_key := 'WORKBENCH_CANDIDATE_SOURCE_BUILD:session:' || v_zero_repair_row.session_id::text || ':candidate:' || v_zero_repair_row.candidate_id::text || ':clone_ineligible:corrected:' || v_zero_source_build_run_id::text;

    v_zero_corrected_payload_json := jsonb_strip_nulls(
      jsonb_build_object(
        'session_id', v_zero_repair_row.session_id::text,
        'candidate_id', v_zero_repair_row.candidate_id::text,
        'session_version', v_zero_session_version,
        'source_change_seq', COALESCE(v_zero_source_change_seq, 0),
        'source_change_sequence', COALESCE(v_zero_source_change_seq, 0),
        'source_build_run_id', v_zero_source_build_run_id::text,
        'operation_type', 'CLONE_REBASE_INELIGIBLE_SOURCE_BUILD',
        'fallback_reason', v_zero_reason,
        'refresh_reason', v_zero_reason,
        'reason', v_zero_reason,
        'refresh_scope_kind', v_zero_refresh_scope_kind,
        'pay_channel_scope', v_zero_pay_channel_scope,
        'source_snapshot_run_id', v_zero_repair_row.target_source_snapshot_run_id::text,
        'snapshot_run_id', v_zero_repair_row.target_source_snapshot_run_id::text,
        'source_session_id', CASE WHEN v_zero_source_session_id IS NULL THEN NULL ELSE v_zero_source_session_id::text END,
        'clone_from_session_id', COALESCE(v_zero_source_session_id::text, v_zero_source_session_id_text),
        'clone_to_session_id', v_zero_repair_row.session_id::text,
        'target_session_signature', v_zero_repair_row.target_session_signature,
        'source_session_signature', v_zero_source_session_signature,
        'force_legacy', true,
        'source_build_required', true,
        'line_work_required', true,
        'delta_refresh_required', false
      )
      || jsonb_build_object(
        'targeted_timesheet_ids', COALESCE(v_zero_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_zero_linked_timesheet_ids_json, '[]'::jsonb),
        'zero_source_row_poison_repair', true,
        'zero_source_row_bad_job_id', v_zero_repair_row.bad_job_id::text,
        'zero_source_row_original_dedupe_key', v_zero_repair_row.bad_job_dedupe_key,
        'source_build_run_id_source', jsonb_build_object(
          'method', 'DETERMINISTIC_MD5_UUID_FROM_AUTHORITY_INPUTS',
          'seed_text', v_zero_source_build_seed_text,
          'target_session_id', v_zero_repair_row.session_id::text,
          'target_session_version', v_zero_session_version,
          'target_session_signature', v_zero_repair_row.target_session_signature,
          'target_snapshot_run_id', v_zero_repair_row.target_source_snapshot_run_id::text,
          'source_session_id', CASE WHEN v_zero_source_session_id IS NULL THEN NULL ELSE v_zero_source_session_id::text END,
          'source_session_version', v_zero_source_session_version,
          'source_session_signature', v_zero_source_session_signature,
          'source_snapshot_run_id', CASE WHEN v_zero_source_snapshot_run_id IS NULL THEN NULL ELSE v_zero_source_snapshot_run_id::text END,
          'candidate_id', v_zero_repair_row.candidate_id::text,
          'source_change_seq', COALESCE(v_zero_source_change_seq, 0),
          'refresh_scope_kind', v_zero_refresh_scope_kind,
          'targeted_timesheet_ids', COALESCE(v_zero_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_zero_linked_timesheet_ids_json, '[]'::jsonb),
          'pay_channel_scope', v_zero_pay_channel_scope,
          'clone_from_session_id', COALESCE(v_zero_source_session_id::text, v_zero_source_session_id_text),
          'clone_to_session_id', v_zero_repair_row.session_id::text
        )
      )
      || jsonb_build_object(
        'source_build', jsonb_build_object(
          'required', true,
          'run_id', v_zero_source_build_run_id::text,
          'source_build_run_id', v_zero_source_build_run_id::text,
          'source_change_seq', COALESCE(v_zero_source_change_seq, 0),
          'source_change_sequence', COALESCE(v_zero_source_change_seq, 0),
          'session_version', v_zero_session_version,
          'refresh_scope_kind', v_zero_refresh_scope_kind,
          'pay_channel_scope', v_zero_pay_channel_scope,
          'targeted_timesheet_ids', COALESCE(v_zero_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_zero_linked_timesheet_ids_json, '[]'::jsonb),
          'reason', v_zero_reason
        ),
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      )
    );

    SELECT corrected_job.id,
           corrected_job.status,
           COALESCE(corrected_job.payload_json, '{}'::jsonb)
    INTO v_zero_corrected_job_id,
         v_zero_corrected_job_status,
         v_zero_corrected_job_payload_json
    FROM public.banking_pay_workbench_jobs AS corrected_job
    WHERE corrected_job.session_id = v_zero_repair_row.session_id
      AND corrected_job.candidate_id = v_zero_repair_row.candidate_id
      AND corrected_job.dedupe_key = v_zero_corrected_dedupe_key
      AND corrected_job.status IN ('QUEUED', 'RUNNING')
    ORDER BY CASE WHEN corrected_job.status = 'QUEUED' THEN 0 ELSE 1 END,
             corrected_job.updated_at_utc DESC NULLS LAST,
             corrected_job.created_at_utc DESC NULLS LAST,
             corrected_job.id DESC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF v_zero_corrected_job_id IS NOT NULL THEN
      v_zero_corrected_job_action := 'REUSED_ACTIVE_CORRECTED_SOURCE_BUILD_JOB';
      v_corrected_source_build_job_reused_count := v_corrected_source_build_job_reused_count + 1;
    ELSE
      SELECT corrected_job.id,
             corrected_job.status,
             COALESCE(corrected_job.payload_json, '{}'::jsonb)
      INTO v_zero_corrected_job_id,
           v_zero_corrected_job_status,
           v_zero_corrected_job_payload_json
      FROM public.banking_pay_workbench_jobs AS corrected_job
      WHERE corrected_job.session_id = v_zero_repair_row.session_id
        AND corrected_job.candidate_id = v_zero_repair_row.candidate_id
        AND corrected_job.dedupe_key = v_zero_corrected_dedupe_key
        AND corrected_job.status IN ('QUEUED', 'RUNNING')
      ORDER BY CASE WHEN corrected_job.status = 'QUEUED' THEN 0 ELSE 1 END,
               corrected_job.updated_at_utc DESC NULLS LAST,
               corrected_job.created_at_utc DESC NULLS LAST,
               corrected_job.id DESC
      LIMIT 1;

      IF v_zero_corrected_job_id IS NOT NULL THEN
        v_zero_corrected_job_action := 'REUSED_ACTIVE_CORRECTED_SOURCE_BUILD_JOB_NOT_LOCKED';
        v_corrected_source_build_job_reused_count := v_corrected_source_build_job_reused_count + 1;
      ELSE
        SELECT corrected_job.id,
               corrected_job.status,
               COALESCE(corrected_job.payload_json, '{}'::jsonb)
        INTO v_zero_corrected_job_id,
             v_zero_corrected_job_status,
             v_zero_corrected_job_payload_json
        FROM public.banking_pay_workbench_jobs AS corrected_job
        WHERE corrected_job.session_id = v_zero_repair_row.session_id
          AND corrected_job.candidate_id = v_zero_repair_row.candidate_id
          AND corrected_job.dedupe_key = v_zero_corrected_dedupe_key
          AND corrected_job.status = 'SUCCEEDED'
        ORDER BY corrected_job.completed_at_utc DESC NULLS LAST,
                 corrected_job.updated_at_utc DESC NULLS LAST,
                 corrected_job.created_at_utc DESC NULLS LAST,
                 corrected_job.id DESC
        LIMIT 1;

        IF v_zero_corrected_job_id IS NOT NULL THEN
          v_zero_corrected_job_action := 'REUSED_SUCCEEDED_CORRECTED_SOURCE_BUILD_JOB';
          v_corrected_source_build_job_succeeded_reused_count := v_corrected_source_build_job_succeeded_reused_count + 1;
        ELSE
          SELECT corrected_job.id
          INTO v_zero_existing_corrected_failed_job_id
          FROM public.banking_pay_workbench_jobs AS corrected_job
          CROSS JOIN LATERAL (
            SELECT NULLIF(BTRIM(COALESCE(
              corrected_job.payload_json->>'source_build_run_id',
              corrected_job.payload_json#>>'{source_build,source_build_run_id}',
              corrected_job.payload_json#>>'{source_build,run_id}',
              corrected_job.payload_json#>>'{cursor,source_build_run_id}',
              corrected_job.payload_json#>>'{cursor_json,source_build_run_id}',
              corrected_job.payload_json#>>'{result_json,source_build_run_id}',
              ''
            )), '') AS source_build_run_id_text
          ) AS corrected_extract
          WHERE corrected_job.session_id = v_zero_repair_row.session_id
            AND corrected_job.candidate_id = v_zero_repair_row.candidate_id
            AND corrected_job.dedupe_key = v_zero_corrected_dedupe_key
            AND corrected_job.status IN ('FAILED', 'DEAD')
            AND corrected_extract.source_build_run_id_text = v_zero_source_build_run_id::text
          ORDER BY corrected_job.updated_at_utc DESC NULLS LAST,
                   corrected_job.created_at_utc DESC NULLS LAST,
                   corrected_job.id DESC
          LIMIT 1;

          IF v_zero_existing_corrected_failed_job_id IS NOT NULL THEN
            v_skipped_count := v_skipped_count + 1;
            v_zero_source_row_skips_json := v_zero_source_row_skips_json || jsonb_build_array(jsonb_build_object(
              'session_id', v_zero_repair_row.session_id::text,
              'candidate_id', v_zero_repair_row.candidate_id::text,
              'bad_job_id', v_zero_repair_row.bad_job_id::text,
              'corrected_failed_job_id', v_zero_existing_corrected_failed_job_id::text,
              'corrected_source_build_run_id', v_zero_source_build_run_id::text,
              'reason', 'ZERO_SOURCE_ROW_CORRECTED_JOB_ALREADY_FAILED_OR_DEAD',
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            ));
            v_skips_json := v_skips_json || jsonb_build_array(jsonb_build_object(
              'session_id', v_zero_repair_row.session_id::text,
              'candidate_id', v_zero_repair_row.candidate_id::text,
              'bad_job_id', v_zero_repair_row.bad_job_id::text,
              'corrected_failed_job_id', v_zero_existing_corrected_failed_job_id::text,
              'reason', 'ZERO_SOURCE_ROW_CORRECTED_JOB_ALREADY_FAILED_OR_DEAD',
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            ));
            CONTINUE;
          END IF;

          INSERT INTO public.banking_pay_workbench_jobs (
            job_type,
            status,
            priority,
            run_at_utc,
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
          VALUES (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'QUEUED',
            60,
            v_now,
            v_zero_corrected_dedupe_key,
            v_zero_repair_row.target_source_snapshot_run_id,
            v_zero_repair_row.session_id,
            v_zero_repair_row.candidate_id,
            v_zero_corrected_payload_json,
            NULL::uuid,
            'BUILD_INITIALISE',
            'BUILD_INITIALISE',
            '{}'::jsonb,
            1,
            v_now,
            v_now
          )
          ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
          DO UPDATE
          SET run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
              priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
              payload_json = jsonb_strip_nulls(COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || EXCLUDED.payload_json),
              updated_at_utc = v_now
          WHERE public.banking_pay_workbench_jobs.status = 'QUEUED'
          RETURNING id, status, payload_json
          INTO v_zero_corrected_job_id, v_zero_corrected_job_status, v_zero_corrected_job_payload_json;

          IF v_zero_corrected_job_id IS NOT NULL THEN
            v_zero_corrected_job_action := 'QUEUED_CORRECTED_SOURCE_BUILD_JOB';
            v_corrected_source_build_job_queued_count := v_corrected_source_build_job_queued_count + 1;
          ELSE
            SELECT corrected_job.id,
                   corrected_job.status,
                   COALESCE(corrected_job.payload_json, '{}'::jsonb)
            INTO v_zero_corrected_job_id,
                 v_zero_corrected_job_status,
                 v_zero_corrected_job_payload_json
            FROM public.banking_pay_workbench_jobs AS corrected_job
            WHERE corrected_job.session_id = v_zero_repair_row.session_id
              AND corrected_job.candidate_id = v_zero_repair_row.candidate_id
              AND corrected_job.dedupe_key = v_zero_corrected_dedupe_key
              AND corrected_job.status IN ('QUEUED', 'RUNNING', 'SUCCEEDED')
            ORDER BY CASE WHEN corrected_job.status = 'QUEUED' THEN 0 WHEN corrected_job.status = 'RUNNING' THEN 1 ELSE 2 END,
                     corrected_job.updated_at_utc DESC NULLS LAST,
                     corrected_job.created_at_utc DESC NULLS LAST,
                     corrected_job.id DESC
            LIMIT 1;

            IF v_zero_corrected_job_id IS NOT NULL THEN
              v_zero_corrected_job_action := 'REUSED_CORRECTED_SOURCE_BUILD_JOB_AFTER_CONFLICT';
              IF v_zero_corrected_job_status = 'SUCCEEDED' THEN
                v_corrected_source_build_job_succeeded_reused_count := v_corrected_source_build_job_succeeded_reused_count + 1;
              ELSE
                v_corrected_source_build_job_reused_count := v_corrected_source_build_job_reused_count + 1;
              END IF;
            END IF;
          END IF;
        END IF;
      END IF;
    END IF;

    IF v_zero_corrected_job_id IS NULL THEN
      v_skipped_count := v_skipped_count + 1;
      v_zero_source_row_skips_json := v_zero_source_row_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_zero_repair_row.session_id::text,
        'candidate_id', v_zero_repair_row.candidate_id::text,
        'bad_job_id', v_zero_repair_row.bad_job_id::text,
        'corrected_source_build_run_id', v_zero_source_build_run_id::text,
        'reason', 'ZERO_SOURCE_ROW_CORRECTED_JOB_ENQUEUE_FAILED',
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      v_skips_json := v_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_zero_repair_row.session_id::text,
        'candidate_id', v_zero_repair_row.candidate_id::text,
        'bad_job_id', v_zero_repair_row.bad_job_id::text,
        'reason', 'ZERO_SOURCE_ROW_CORRECTED_JOB_ENQUEUE_FAILED',
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      CONTINUE;
    END IF;

    SELECT jsonb_build_object(
      'session_id', v_zero_repair_row.session_id::text,
      'candidate_id', v_zero_repair_row.candidate_id::text,
      'bad_job_id', bad_job.id::text,
      'job_type', bad_job.job_type,
      'status', bad_job.status,
      'attempt_count', bad_job.attempt_count,
      'max_attempts', bad_job.max_attempts,
      'dedupe_key', bad_job.dedupe_key,
      'payload_json', bad_job.payload_json,
      'last_error_json', bad_job.last_error_json,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_zero_bad_job_before_json
    FROM public.banking_pay_workbench_jobs AS bad_job
    WHERE bad_job.id = v_zero_repair_row.bad_job_id
    FOR UPDATE;

    UPDATE public.banking_pay_workbench_jobs AS bad_job_update
    SET payload_json = jsonb_strip_nulls(
          COALESCE(bad_job_update.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'invalid_source_build_without_run_id_failed_closed', true,
            'invalid_source_build_without_run_id_non_blocking', true,
            'non_blocking_terminal_failure', true,
            'non_blocking_terminal_failure_reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
            'non_blocking_terminal_failure_at_utc', v_now::text,
            'zero_source_row_repair_resolved', true,
            'zero_source_row_repair_resolved_at_utc', v_now::text,
            'zero_source_row_corrected_job_id', v_zero_corrected_job_id::text,
            'zero_source_row_corrected_source_build_run_id', v_zero_source_build_run_id::text,
            'zero_source_row_corrected_dedupe_key', v_zero_corrected_dedupe_key,
            'source_rows_marked_error_count', COALESCE(v_zero_repair_row.source_rows_marked_error_count, 0),
            'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
          )
        ),
        updated_at_utc = v_now
    WHERE bad_job_update.id = v_zero_repair_row.bad_job_id
    RETURNING jsonb_build_object(
      'session_id', v_zero_repair_row.session_id::text,
      'candidate_id', v_zero_repair_row.candidate_id::text,
      'bad_job_id', bad_job_update.id::text,
      'job_type', bad_job_update.job_type,
      'status', bad_job_update.status,
      'retained_terminal', true,
      'hard_deleted', false,
      'bad_job_mutated', true,
      'zero_source_row_repair_resolved', true,
      'non_blocking_terminal_failure', true,
      'invalid_source_build_without_run_id_non_blocking', true,
      'corrected_source_build_job_id', v_zero_corrected_job_id::text,
      'corrected_source_build_run_id', v_zero_source_build_run_id::text,
      'corrected_dedupe_key', v_zero_corrected_dedupe_key,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_zero_bad_job_after_json;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_zero_repair_row.bad_job_id::text,
      'ZERO_SOURCE_ROW_BAD_JOB_MARKED_NON_BLOCKING',
      v_zero_bad_job_before_json,
      v_zero_bad_job_after_json,
      'ZERO_SOURCE_ROW_INVALID_SOURCE_BUILD_WITHOUT_RUN_ID_FAILED_CLOSED',
      NULL
    );

    SELECT jsonb_build_object(
      'id', corrected_job.id::text,
      'job_type', corrected_job.job_type,
      'status', corrected_job.status,
      'dedupe_key', corrected_job.dedupe_key,
      'session_id', CASE WHEN corrected_job.session_id IS NULL THEN NULL ELSE corrected_job.session_id::text END,
      'candidate_id', CASE WHEN corrected_job.candidate_id IS NULL THEN NULL ELSE corrected_job.candidate_id::text END,
      'source_build_run_id', v_zero_source_build_run_id::text,
      'payload_json', corrected_job.payload_json,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_zero_corrected_job_after_json
    FROM public.banking_pay_workbench_jobs AS corrected_job
    WHERE corrected_job.id = v_zero_corrected_job_id;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_zero_corrected_job_id::text,
      COALESCE(v_zero_corrected_job_action, 'CORRECTED_SOURCE_BUILD_JOB_REUSED'),
      v_zero_corrected_job_before_json,
      v_zero_corrected_job_after_json,
      'ZERO_SOURCE_ROW_CORRECTED_SOURCE_BUILD_JOB_READY',
      NULL
    );

    SELECT jsonb_build_object(
      'id', scope_row.id::text,
      'session_id', scope_row.session_id::text,
      'candidate_id', scope_row.candidate_id::text,
      'status', scope_row.status,
      'dirty', scope_row.dirty,
      'seeded', scope_row.seeded,
      'pending_job_id', CASE WHEN scope_row.pending_job_id IS NULL THEN NULL ELSE scope_row.pending_job_id::text END,
      'error_json', scope_row.error_json,
      'bad_job_id', v_zero_repair_row.bad_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_zero_scope_before_json
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.id = v_zero_repair_row.scope_id
    FOR UPDATE;

    UPDATE public.banking_pay_workbench_session_scope AS scope_row
    SET status = 'SOURCE_BUILD_PENDING',
        pending_job_id = v_zero_corrected_job_id,
        seeded = false,
        dirty = true,
        error_json = NULL::jsonb,
        updated_at_utc = v_now
    WHERE scope_row.id = v_zero_repair_row.scope_id
      AND scope_row.session_id = v_zero_repair_row.session_id
      AND scope_row.candidate_id = v_zero_repair_row.candidate_id
    RETURNING jsonb_build_object(
      'id', scope_row.id::text,
      'session_id', scope_row.session_id::text,
      'candidate_id', scope_row.candidate_id::text,
      'status', scope_row.status,
      'dirty', scope_row.dirty,
      'seeded', scope_row.seeded,
      'pending_job_id', CASE WHEN scope_row.pending_job_id IS NULL THEN NULL ELSE scope_row.pending_job_id::text END,
      'error_json', scope_row.error_json,
      'bad_job_id', v_zero_repair_row.bad_job_id::text,
      'corrected_source_build_job_id', v_zero_corrected_job_id::text,
      'corrected_source_build_run_id', v_zero_source_build_run_id::text,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_zero_scope_after_json;

    PERFORM public._audit_insert(
      'banking_pay_workbench_session_scope',
      v_zero_repair_row.candidate_id::text,
      'ZERO_SOURCE_ROW_SOURCE_BUILD_POISON_REPAIRED',
      v_zero_scope_before_json,
      v_zero_scope_after_json,
      'ZERO_SOURCE_ROW_SOURCE_BUILD_POISON_REPAIRED',
      NULL
    );

    SELECT jsonb_build_object(
      'id', session_row.id::text,
      'status', session_row.status,
      'progress_state', session_row.progress_state,
      'scope_failed_count', session_row.scope_failed_count,
      'scope_pending_count', session_row.scope_pending_count,
      'scope_ready_count', session_row.scope_ready_count,
      'blocker_codes', COALESCE(session_row.progress_json->'blocker_codes', '[]'::jsonb),
      'draft_blocker_codes', COALESCE(session_row.progress_json->'draft_blocker_codes', '[]'::jsonb),
      'session_blocker_codes', COALESCE(session_row.progress_json->'session_blocker_codes', '[]'::jsonb),
      'progress_json', public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true),
      'bad_job_id', v_zero_repair_row.bad_job_id::text,
      'corrected_source_build_job_id', v_zero_corrected_job_id::text,
      'corrected_source_build_run_id', v_zero_source_build_run_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_zero_session_before_json
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_zero_repair_row.session_id
    FOR UPDATE;

    UPDATE public.banking_pay_workbench_sessions AS session_row
    SET progress_state = 'REFRESHING_CANDIDATES',
        progress_json = jsonb_strip_nulls(
          public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true)
          - 'last_source_build_failure_at_utc'
          - 'last_source_build_failure_job_id'
          - 'last_source_build_failure_code'
          - 'last_source_build_failure_source_build_run_id'
          - 'last_source_build_source_rows_marked_error_count'
          - 'blocker_codes'
          - 'draft_blocker_codes'
          - 'session_blocker_codes'
        ),
        progress_counter_version = COALESCE(session_row.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_row.id = v_zero_repair_row.session_id;

    BEGIN
      v_progress_recompute_json := public.pay_workbench_session_recompute_progress_counters(
        p_session_id => v_zero_repair_row.session_id,
        p_apply => true,
        p_reason => 'ZERO_SOURCE_ROW_SOURCE_BUILD_POISON_REPAIRED',
        p_write_progress_json => true
      );
    EXCEPTION WHEN OTHERS THEN
      v_progress_recompute_failed := true;
      v_progress_recompute_error_json := jsonb_build_object(
        'code', SQLSTATE,
        'message', SQLERRM,
        'session_id', v_zero_repair_row.session_id::text,
        'candidate_id', v_zero_repair_row.candidate_id::text,
        'bad_job_id', v_zero_repair_row.bad_job_id::text,
        'corrected_source_build_job_id', v_zero_corrected_job_id::text,
        'corrected_source_build_run_id', v_zero_source_build_run_id::text,
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      );
      v_progress_recompute_json := COALESCE(v_progress_recompute_error_json, '{}'::jsonb);
    END;

    SELECT session_row.progress_state,
           session_row.scope_failed_count,
           session_row.scope_pending_count,
           jsonb_build_object(
             'id', session_row.id::text,
             'status', session_row.status,
             'progress_state', session_row.progress_state,
             'scope_failed_count', session_row.scope_failed_count,
             'scope_pending_count', session_row.scope_pending_count,
             'scope_ready_count', session_row.scope_ready_count,
             'blocker_codes', COALESCE(session_row.progress_json->'blocker_codes', '[]'::jsonb),
             'draft_blocker_codes', COALESCE(session_row.progress_json->'draft_blocker_codes', '[]'::jsonb),
             'session_blocker_codes', COALESCE(session_row.progress_json->'session_blocker_codes', '[]'::jsonb),
             'progress_json', public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true),
             'bad_job_id', v_zero_repair_row.bad_job_id::text,
             'corrected_source_build_job_id', v_zero_corrected_job_id::text,
             'corrected_source_build_run_id', v_zero_source_build_run_id::text,
             'recompute_failed', v_progress_recompute_failed,
             'recompute_json', COALESCE(v_progress_recompute_json, '{}'::jsonb),
             'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
           )
    INTO v_post_recompute_progress_state,
         v_post_recompute_scope_failed_count,
         v_post_recompute_scope_pending_count,
         v_zero_session_after_json
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_zero_repair_row.session_id;

    PERFORM public._audit_insert(
      'banking_pay_workbench_session',
      v_zero_repair_row.session_id::text,
      'ZERO_SOURCE_ROW_PROGRESS_REPAIRED_AFTER_SOURCE_BUILD_POISON',
      v_zero_session_before_json,
      v_zero_session_after_json,
      'ZERO_SOURCE_ROW_PROGRESS_REPAIRED_AFTER_SOURCE_BUILD_POISON',
      NULL
    );

    v_zero_source_row_repaired_count := v_zero_source_row_repaired_count + 1;
    v_repaired_count := v_repaired_count + 1;
    v_repaired_scope_count := v_repaired_scope_count + 1;
    v_repaired_session_count := v_repaired_session_count + 1;

    v_zero_source_row_repairs_json := v_zero_source_row_repairs_json || jsonb_build_array(jsonb_build_object(
      'session_id', v_zero_repair_row.session_id::text,
      'candidate_id', v_zero_repair_row.candidate_id::text,
      'bad_job_id', v_zero_repair_row.bad_job_id::text,
      'corrected_source_build_job_id', v_zero_corrected_job_id::text,
      'corrected_source_build_job_status', v_zero_corrected_job_status,
      'corrected_source_build_run_id', v_zero_source_build_run_id::text,
      'corrected_dedupe_key', v_zero_corrected_dedupe_key,
      'source_change_seq', COALESCE(v_zero_source_change_seq, 0),
      'scope_repaired', true,
      'session_progress_repaired', true,
      'progress_recompute_failed', v_progress_recompute_failed,
      'post_recompute_progress_state', v_post_recompute_progress_state,
      'post_recompute_scope_failed_count', v_post_recompute_scope_failed_count,
      'post_recompute_scope_pending_count', v_post_recompute_scope_pending_count,
      'corrected_job_action', v_zero_corrected_job_action,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    ));

    v_repairs_json := v_repairs_json || jsonb_build_array(jsonb_build_object(
      'session_id', v_zero_repair_row.session_id::text,
      'candidate_id', v_zero_repair_row.candidate_id::text,
      'bad_job_id', v_zero_repair_row.bad_job_id::text,
      'corrected_source_build_job_id', v_zero_corrected_job_id::text,
      'corrected_source_build_run_id', v_zero_source_build_run_id::text,
      'zero_source_row_repair', true,
      'scope_repaired', true,
      'session_progress_repaired', true,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    ));
  END LOOP;

  FOR v_orphan_repair_row IN
    SELECT
      scope_row.id AS scope_id,
      scope_row.session_id,
      scope_row.candidate_id,
      scope_row.status AS scope_status,
      scope_row.pending_job_id AS bad_job_id,
      scope_row.dirty AS scope_dirty,
      scope_row.seeded AS scope_seeded,
      scope_row.error_json AS scope_error_json,
      scope_row.updated_at_utc AS scope_updated_at_utc,
      bad_job.job_type AS bad_job_type,
      bad_job.status AS bad_job_status,
      bad_job.dedupe_key AS bad_job_dedupe_key,
      COALESCE(bad_job.payload_json, '{}'::jsonb) AS bad_job_payload_json,
      bad_job.last_error_json AS bad_job_last_error_json,
      bad_job.failed_at_utc AS bad_job_failed_at_utc,
      target_session.actor_user_id AS session_actor_user_id,
      target_session.version AS session_version,
      target_session.session_signature,
      target_session.source_snapshot_run_id,
      COALESCE(app_counter.seq, 0)::bigint AS live_candidate_change_seq
    FROM public.banking_pay_workbench_session_scope AS scope_row
    JOIN public.banking_pay_workbench_jobs AS bad_job
      ON bad_job.id = scope_row.pending_job_id
    JOIN public.banking_pay_workbench_sessions AS target_session
      ON target_session.id = scope_row.session_id
    LEFT JOIN public.app_change_counters AS app_counter
      ON app_counter.entity_key = 'pay_candidate:' || scope_row.candidate_id::text
    WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SOURCE_BUILD_PENDING'
      AND COALESCE(scope_row.dirty, false) IS TRUE
      AND scope_row.pending_job_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(bad_job.job_type, ''))) IN (
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
        'CANDIDATE_SOURCE_BUILD',
        'CANDIDATE_SOURCE_BUILD_CHUNK',
        'SOURCE_BUILD',
        'SOURCE_BUILD_PAGE'
      )
      AND UPPER(BTRIM(COALESCE(bad_job.status, ''))) IN ('FAILED', 'DEAD')
      AND bad_job.session_id = scope_row.session_id
      AND bad_job.candidate_id = scope_row.candidate_id
      AND NULLIF(BTRIM(COALESCE(
        bad_job.payload_json->>'source_build_run_id',
        bad_job.payload_json#>>'{source_build,source_build_run_id}',
        bad_job.payload_json#>>'{source_build,run_id}',
        bad_job.payload_json#>>'{cursor,source_build_run_id}',
        bad_job.payload_json#>>'{cursor_json,source_build_run_id}',
        bad_job.payload_json#>>'{result_json,source_build_run_id}',
        ''
      )), '') IS NULL
      AND (
        COALESCE(bad_job.last_error_json, '{}'::jsonb)::text ILIKE '%PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED%'
        OR COALESCE(bad_job.payload_json#>'{last_failure_json}', '{}'::jsonb)::text ILIKE '%PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_RUN_ID_REQUIRED%'
        OR (
          lower(BTRIM(COALESCE(bad_job.payload_json->>'non_blocking_terminal_failure', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND UPPER(BTRIM(COALESCE(bad_job.payload_json->>'non_blocking_terminal_failure_reason', ''))) = 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE'
        )
        OR lower(BTRIM(COALESCE(bad_job.payload_json->>'invalid_source_build_without_run_id_failed_closed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      )
      AND UPPER(BTRIM(COALESCE(target_session.status, ''))) = 'OPEN'
      AND target_session.discarded_at_utc IS NULL
      AND target_session.source_snapshot_run_id IS NOT NULL
      AND (p_session_id IS NULL OR scope_row.session_id = p_session_id)
      AND (p_candidate_id IS NULL OR scope_row.candidate_id = p_candidate_id)
    ORDER BY scope_row.updated_at_utc ASC, scope_row.id ASC
    LIMIT GREATEST(v_limit - v_processed_count, 0)
  LOOP
    PERFORM 1
    FROM public.banking_pay_workbench_sessions AS locked_session
    WHERE locked_session.id = v_orphan_repair_row.session_id
      AND UPPER(BTRIM(COALESCE(locked_session.status, ''))) = 'OPEN'
      AND locked_session.discarded_at_utc IS NULL
      AND locked_session.source_snapshot_run_id IS NOT NULL
    FOR UPDATE;

    IF NOT FOUND THEN
      v_skipped_count := v_skipped_count + 1;
      v_orphan_pending_skips_json := v_orphan_pending_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_orphan_repair_row.session_id::text,
        'candidate_id', v_orphan_repair_row.candidate_id::text,
        'bad_job_id', v_orphan_repair_row.bad_job_id::text,
        'reason', 'ORPHANED_PENDING_REPAIR_SESSION_NO_LONGER_OPEN',
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      CONTINUE;
    END IF;

    PERFORM 1
    FROM public.banking_pay_workbench_session_scope AS locked_scope
    JOIN public.banking_pay_workbench_jobs AS locked_bad_job
      ON locked_bad_job.id = locked_scope.pending_job_id
    WHERE locked_scope.id = v_orphan_repair_row.scope_id
      AND locked_scope.session_id = v_orphan_repair_row.session_id
      AND locked_scope.candidate_id = v_orphan_repair_row.candidate_id
      AND UPPER(BTRIM(COALESCE(locked_scope.status, ''))) = 'SOURCE_BUILD_PENDING'
      AND COALESCE(locked_scope.dirty, false) IS TRUE
      AND locked_scope.pending_job_id = v_orphan_repair_row.bad_job_id
      AND UPPER(BTRIM(COALESCE(locked_bad_job.status, ''))) IN ('FAILED', 'DEAD')
      AND locked_bad_job.session_id = locked_scope.session_id
      AND locked_bad_job.candidate_id = locked_scope.candidate_id
    FOR UPDATE OF locked_scope, locked_bad_job;

    IF NOT FOUND THEN
      v_skipped_count := v_skipped_count + 1;
      v_orphan_pending_skips_json := v_orphan_pending_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_orphan_repair_row.session_id::text,
        'candidate_id', v_orphan_repair_row.candidate_id::text,
        'bad_job_id', v_orphan_repair_row.bad_job_id::text,
        'reason', 'ORPHANED_PENDING_REPAIR_STATE_ALREADY_CHANGED',
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      CONTINUE;
    END IF;

    v_processed_count := v_processed_count + 1;
    v_orphan_enqueue_result := '{}'::jsonb;
    v_orphan_enqueue_error_json := NULL::jsonb;
    v_orphan_enqueue_job_id_text := NULL::text;
    v_orphan_enqueue_job_id := NULL::uuid;
    v_orphan_enqueue_job_type := NULL::text;
    v_orphan_enqueue_job_status := NULL::text;
    v_orphan_enqueue_source_build_run_id_text := NULL::text;
    v_orphan_enqueue_valid := false;
    v_progress_recompute_json := '{}'::jsonb;
    v_progress_recompute_failed := false;
    v_progress_recompute_error_json := NULL::jsonb;

    WITH targeted_arrays(value_json) AS (
      VALUES
        (v_orphan_repair_row.bad_job_payload_json->'targeted_timesheet_ids'),
        (v_orphan_repair_row.bad_job_payload_json->'targeted_timesheet_ids_requested'),
        (v_orphan_repair_row.bad_job_payload_json#>'{source_build,targeted_timesheet_ids}'),
        (v_orphan_repair_row.bad_job_payload_json#>'{source_build,targeted_timesheet_ids_requested}')
    ), targeted_values(timesheet_id_text) AS (
      SELECT NULLIF(BTRIM(targeted_value.value), '')
      FROM targeted_arrays AS targeted_array
      CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(targeted_array.value_json) = 'array' THEN targeted_array.value_json ELSE '[]'::jsonb END
      ) AS targeted_value(value)
    )
    SELECT COALESCE(jsonb_agg(targeted_ids.timesheet_id_text ORDER BY targeted_ids.timesheet_id_text), '[]'::jsonb)
    INTO v_orphan_targeted_timesheet_ids_json
    FROM (
      SELECT DISTINCT targeted_value.timesheet_id_text
      FROM targeted_values AS targeted_value
      WHERE targeted_value.timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) AS targeted_ids;

    WITH linked_arrays(value_json) AS (
      VALUES
        (v_orphan_repair_row.bad_job_payload_json->'linked_timesheet_ids'),
        (v_orphan_repair_row.bad_job_payload_json->'linked_timesheet_ids_requested'),
        (v_orphan_repair_row.bad_job_payload_json#>'{source_build,linked_timesheet_ids}'),
        (v_orphan_repair_row.bad_job_payload_json#>'{source_build,linked_timesheet_ids_requested}')
    ), linked_values(timesheet_id_text) AS (
      SELECT NULLIF(BTRIM(linked_value.value), '')
      FROM linked_arrays AS linked_array
      CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(linked_array.value_json) = 'array' THEN linked_array.value_json ELSE '[]'::jsonb END
      ) AS linked_value(value)
    )
    SELECT COALESCE(jsonb_agg(linked_ids.timesheet_id_text ORDER BY linked_ids.timesheet_id_text), '[]'::jsonb)
    INTO v_orphan_linked_timesheet_ids_json
    FROM (
      SELECT DISTINCT linked_value.timesheet_id_text
      FROM linked_values AS linked_value
      WHERE linked_value.timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) AS linked_ids;

    v_orphan_refresh_scope_kind := UPPER(BTRIM(COALESCE(
      NULLIF(v_orphan_repair_row.bad_job_payload_json->>'refresh_scope_kind', ''),
      NULLIF(v_orphan_repair_row.bad_job_payload_json#>>'{source_build,refresh_scope_kind}', ''),
      CASE
        WHEN jsonb_array_length(COALESCE(v_orphan_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
          OR jsonb_array_length(COALESCE(v_orphan_linked_timesheet_ids_json, '[]'::jsonb)) > 0
        THEN 'TARGETED_TIMESHEETS'
        ELSE 'CANDIDATE_FULL_LIVE'
      END
    )));

    IF v_orphan_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
      v_orphan_refresh_scope_kind := CASE
        WHEN jsonb_array_length(COALESCE(v_orphan_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
          OR jsonb_array_length(COALESCE(v_orphan_linked_timesheet_ids_json, '[]'::jsonb)) > 0
        THEN 'TARGETED_TIMESHEETS'
        ELSE 'CANDIDATE_FULL_LIVE'
      END;
    END IF;

    v_orphan_pay_channel_scope := UPPER(BTRIM(COALESCE(
      NULLIF(v_orphan_repair_row.bad_job_payload_json->>'pay_channel_scope', ''),
      NULLIF(v_orphan_repair_row.bad_job_payload_json#>>'{source_build,pay_channel_scope}', ''),
      'ALL'
    )));

    IF v_orphan_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
      v_orphan_pay_channel_scope := 'ALL';
    END IF;

    v_orphan_source_change_seq := COALESCE(
      CASE WHEN COALESCE(v_orphan_repair_row.bad_job_payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (v_orphan_repair_row.bad_job_payload_json->>'source_change_seq')::bigint ELSE NULL::bigint END,
      CASE WHEN COALESCE(v_orphan_repair_row.bad_job_payload_json->>'source_change_sequence', '') ~ '^[0-9]{1,18}$' THEN (v_orphan_repair_row.bad_job_payload_json->>'source_change_sequence')::bigint ELSE NULL::bigint END,
      v_orphan_repair_row.live_candidate_change_seq,
      0::bigint
    );

    v_orphan_scope_before_json := jsonb_build_object(
      'id', v_orphan_repair_row.scope_id::text,
      'session_id', v_orphan_repair_row.session_id::text,
      'candidate_id', v_orphan_repair_row.candidate_id::text,
      'status', v_orphan_repair_row.scope_status,
      'dirty', v_orphan_repair_row.scope_dirty,
      'seeded', v_orphan_repair_row.scope_seeded,
      'pending_job_id', v_orphan_repair_row.bad_job_id::text,
      'error_json', v_orphan_repair_row.scope_error_json,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    );

    v_orphan_bad_job_before_json := jsonb_build_object(
      'id', v_orphan_repair_row.bad_job_id::text,
      'session_id', v_orphan_repair_row.session_id::text,
      'candidate_id', v_orphan_repair_row.candidate_id::text,
      'job_type', v_orphan_repair_row.bad_job_type,
      'status', v_orphan_repair_row.bad_job_status,
      'dedupe_key', v_orphan_repair_row.bad_job_dedupe_key,
      'source_build_run_id', NULL::text,
      'last_error_json', v_orphan_repair_row.bad_job_last_error_json,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    );

    BEGIN
      v_orphan_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
        p_snapshot_run_id => v_orphan_repair_row.source_snapshot_run_id,
        p_candidate_id => v_orphan_repair_row.candidate_id,
        p_reason => 'ORPHANED_PENDING_SOURCE_BUILD_POISON_REPAIR',
        p_actor_user_id => v_orphan_repair_row.session_actor_user_id,
        p_payload_json => jsonb_strip_nulls(
          jsonb_build_object(
            'session_id', v_orphan_repair_row.session_id::text,
            'source_session_id', v_orphan_repair_row.session_id::text,
            'source_snapshot_run_id', v_orphan_repair_row.source_snapshot_run_id::text,
            'snapshot_run_id', v_orphan_repair_row.source_snapshot_run_id::text,
            'session_version', v_orphan_repair_row.session_version,
            'session_signature', v_orphan_repair_row.session_signature,
            'force_legacy', true,
            'fallback_reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
            'projection_class', COALESCE(NULLIF(BTRIM(v_orphan_repair_row.bad_job_payload_json->>'projection_class'), ''), 'POST_DRAFT_PATCH_COMPLEX')
          )
          || jsonb_build_object(
            'source_change_seq', COALESCE(v_orphan_source_change_seq, 0),
            'refresh_scope_kind', v_orphan_refresh_scope_kind,
            'pay_channel_scope', v_orphan_pay_channel_scope,
            'targeted_timesheet_ids', COALESCE(v_orphan_targeted_timesheet_ids_json, '[]'::jsonb),
            'linked_timesheet_ids', COALESCE(v_orphan_linked_timesheet_ids_json, '[]'::jsonb),
            'source_build_required', true,
            'line_work_required', true,
            'delta_refresh_required', false,
            'enqueue_origin', 'PAY_WORKBENCH_REPAIR_INVALID_SOURCE_BUILD_POISON',
            'source_build_poison_repair', true,
            'invalid_source_build_bad_job_id', v_orphan_repair_row.bad_job_id::text,
            'repair_request_reason', v_reason,
            'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
          )
        )
      );
    EXCEPTION WHEN OTHERS THEN
      v_orphan_enqueue_error_json := jsonb_build_object(
        'code', SQLSTATE,
        'message', SQLERRM,
        'session_id', v_orphan_repair_row.session_id::text,
        'candidate_id', v_orphan_repair_row.candidate_id::text,
        'bad_job_id', v_orphan_repair_row.bad_job_id::text,
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      );
    END;

    v_orphan_enqueue_job_id_text := NULLIF(BTRIM(COALESCE(v_orphan_enqueue_result->>'job_id', '')), '');

    IF v_orphan_enqueue_error_json IS NULL
       AND v_orphan_enqueue_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_orphan_enqueue_job_id := v_orphan_enqueue_job_id_text::uuid;

      SELECT
        successor_job.job_type,
        successor_job.status,
        NULLIF(BTRIM(COALESCE(
          successor_job.payload_json->>'source_build_run_id',
          successor_job.payload_json#>>'{source_build,source_build_run_id}',
          successor_job.payload_json#>>'{source_build,run_id}',
          ''
        )), '')
      INTO
        v_orphan_enqueue_job_type,
        v_orphan_enqueue_job_status,
        v_orphan_enqueue_source_build_run_id_text
      FROM public.banking_pay_workbench_jobs AS successor_job
      WHERE successor_job.id = v_orphan_enqueue_job_id
        AND successor_job.session_id = v_orphan_repair_row.session_id
        AND successor_job.candidate_id = v_orphan_repair_row.candidate_id
      FOR UPDATE;

      v_orphan_enqueue_valid := FOUND
        AND UPPER(BTRIM(COALESCE(v_orphan_enqueue_job_type, ''))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND UPPER(BTRIM(COALESCE(v_orphan_enqueue_job_status, ''))) IN ('QUEUED', 'RUNNING')
        AND v_orphan_enqueue_source_build_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
    END IF;

    IF v_orphan_enqueue_valid IS NOT TRUE THEN
      v_orphan_enqueue_error_json := COALESCE(
        v_orphan_enqueue_error_json,
        jsonb_build_object(
          'code', 'SOURCE_BUILD_POISON_REPAIR_CANONICAL_SUCCESSOR_REQUIRED',
          'session_id', v_orphan_repair_row.session_id::text,
          'candidate_id', v_orphan_repair_row.candidate_id::text,
          'bad_job_id', v_orphan_repair_row.bad_job_id::text,
          'enqueue_result', COALESCE(v_orphan_enqueue_result, '{}'::jsonb),
          'successor_job_id', v_orphan_enqueue_job_id_text,
          'successor_job_type', v_orphan_enqueue_job_type,
          'successor_job_status', v_orphan_enqueue_job_status,
          'successor_source_build_run_id', v_orphan_enqueue_source_build_run_id_text,
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        )
      );

      UPDATE public.banking_pay_workbench_session_scope AS failed_scope
      SET status = 'SOURCE_BUILD_ERROR',
          dirty = true,
          pending_job_id = NULL::uuid,
          error_json = jsonb_build_object(
            'code', 'SOURCE_BUILD_POISON_REPAIR_CANONICAL_SUCCESSOR_REQUIRED',
            'job_id', v_orphan_repair_row.bad_job_id::text,
            'repair_error_json', v_orphan_enqueue_error_json,
            'reason', v_reason,
            'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
          ),
          updated_at_utc = v_now
      WHERE failed_scope.id = v_orphan_repair_row.scope_id
        AND failed_scope.session_id = v_orphan_repair_row.session_id
        AND failed_scope.candidate_id = v_orphan_repair_row.candidate_id
        AND (
          failed_scope.pending_job_id = v_orphan_repair_row.bad_job_id
          OR (v_orphan_enqueue_job_id IS NOT NULL AND failed_scope.pending_job_id = v_orphan_enqueue_job_id)
        );

      UPDATE public.banking_pay_workbench_jobs AS bad_job_update
      SET payload_json = jsonb_strip_nulls(
            COALESCE(bad_job_update.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'source_build_poison_repair_attempted', true,
              'source_build_poison_repair_attempted_at_utc', v_now::text,
              'source_build_poison_repair_successor_created', false,
              'source_build_poison_repair_error_json', v_orphan_enqueue_error_json,
              'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
            )
          ),
          updated_at_utc = v_now
      WHERE bad_job_update.id = v_orphan_repair_row.bad_job_id;

      BEGIN
        v_progress_recompute_json := public.pay_workbench_session_recompute_progress_counters(
          p_session_id => v_orphan_repair_row.session_id,
          p_apply => true,
          p_reason => 'SOURCE_BUILD_POISON_REPAIR_SUCCESSOR_FAILED_CLOSED',
          p_write_progress_json => true
        );
      EXCEPTION WHEN OTHERS THEN
        v_progress_recompute_failed := true;
        v_progress_recompute_error_json := jsonb_build_object(
          'code', SQLSTATE,
          'message', SQLERRM,
          'session_id', v_orphan_repair_row.session_id::text,
          'candidate_id', v_orphan_repair_row.candidate_id::text,
          'bad_job_id', v_orphan_repair_row.bad_job_id::text,
          'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
        );
      END;

      SELECT jsonb_build_object(
        'id', failed_scope.id::text,
        'session_id', failed_scope.session_id::text,
        'candidate_id', failed_scope.candidate_id::text,
        'status', failed_scope.status,
        'dirty', failed_scope.dirty,
        'seeded', failed_scope.seeded,
        'pending_job_id', CASE WHEN failed_scope.pending_job_id IS NULL THEN NULL ELSE failed_scope.pending_job_id::text END,
        'error_json', failed_scope.error_json,
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      )
      INTO v_orphan_scope_after_json
      FROM public.banking_pay_workbench_session_scope AS failed_scope
      WHERE failed_scope.id = v_orphan_repair_row.scope_id;

      PERFORM public._audit_insert(
        'banking_pay_workbench_session_scope',
        v_orphan_repair_row.candidate_id::text,
        'SOURCE_BUILD_POISON_REPAIR_SUCCESSOR_FAILED_CLOSED',
        v_orphan_scope_before_json,
        v_orphan_scope_after_json,
        'SOURCE_BUILD_POISON_REPAIR_SUCCESSOR_FAILED_CLOSED',
        NULL
      );

      v_skipped_count := v_skipped_count + 1;
      v_orphan_pending_failed_closed_count := v_orphan_pending_failed_closed_count + 1;
      v_orphan_pending_skips_json := v_orphan_pending_skips_json || jsonb_build_array(jsonb_build_object(
        'session_id', v_orphan_repair_row.session_id::text,
        'candidate_id', v_orphan_repair_row.candidate_id::text,
        'bad_job_id', v_orphan_repair_row.bad_job_id::text,
        'scope_failed_closed', true,
        'failed_job_reference_cleared', true,
        'repair_error_json', v_orphan_enqueue_error_json,
        'progress_recompute_failed', v_progress_recompute_failed,
        'progress_recompute_error_json', v_progress_recompute_error_json,
        'reason', v_reason,
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      ));
      CONTINUE;
    END IF;

    PERFORM 1
    FROM public.banking_pay_workbench_session_scope AS repaired_scope
    WHERE repaired_scope.id = v_orphan_repair_row.scope_id
      AND repaired_scope.session_id = v_orphan_repair_row.session_id
      AND repaired_scope.candidate_id = v_orphan_repair_row.candidate_id
      AND UPPER(BTRIM(COALESCE(repaired_scope.status, ''))) = 'SOURCE_BUILD_PENDING'
      AND COALESCE(repaired_scope.dirty, false) IS TRUE
      AND repaired_scope.pending_job_id = v_orphan_enqueue_job_id
      AND repaired_scope.error_json IS NULL;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'SOURCE_BUILD_POISON_REPAIR_SCOPE_REBIND_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SOURCE_BUILD_POISON_REPAIR_SCOPE_REBIND_REQUIRED',
                'session_id', v_orphan_repair_row.session_id::text,
                'candidate_id', v_orphan_repair_row.candidate_id::text,
                'bad_job_id', v_orphan_repair_row.bad_job_id::text,
                'successor_job_id', v_orphan_enqueue_job_id::text,
                'successor_source_build_run_id', v_orphan_enqueue_source_build_run_id_text,
                'message', 'The canonical candidate refresh helper did not replace the failed pending job reference with the validated successor.'
              )::text;
    END IF;

    UPDATE public.banking_pay_workbench_jobs AS bad_job_update
    SET payload_json = jsonb_strip_nulls(
          COALESCE(bad_job_update.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'source_build_poison_repair_resolved', true,
            'source_build_poison_repair_resolved_at_utc', v_now::text,
            'source_build_poison_repair_reason', v_reason,
            'source_build_poison_repair_successor_job_id', v_orphan_enqueue_job_id::text,
            'source_build_poison_repair_successor_source_build_run_id', v_orphan_enqueue_source_build_run_id_text,
            'source_build_poison_repair_successor_status', v_orphan_enqueue_job_status,
            'non_blocking_terminal_failure', true,
            'non_blocking_terminal_failure_reason', 'MISSING_SOURCE_BUILD_RUN_ID_IS_NOT_AUTHORITATIVE',
            'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
          )
        ),
        updated_at_utc = v_now
    WHERE bad_job_update.id = v_orphan_repair_row.bad_job_id
    RETURNING jsonb_build_object(
      'id', bad_job_update.id::text,
      'session_id', v_orphan_repair_row.session_id::text,
      'candidate_id', v_orphan_repair_row.candidate_id::text,
      'job_type', bad_job_update.job_type,
      'status', bad_job_update.status,
      'retained_terminal', true,
      'successor_job_id', v_orphan_enqueue_job_id::text,
      'successor_source_build_run_id', v_orphan_enqueue_source_build_run_id_text,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_orphan_bad_job_after_json;

    SELECT jsonb_build_object(
      'id', repaired_scope.id::text,
      'session_id', repaired_scope.session_id::text,
      'candidate_id', repaired_scope.candidate_id::text,
      'status', repaired_scope.status,
      'dirty', repaired_scope.dirty,
      'seeded', repaired_scope.seeded,
      'pending_job_id', CASE WHEN repaired_scope.pending_job_id IS NULL THEN NULL ELSE repaired_scope.pending_job_id::text END,
      'error_json', repaired_scope.error_json,
      'successor_source_build_run_id', v_orphan_enqueue_source_build_run_id_text,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_orphan_scope_after_json
    FROM public.banking_pay_workbench_session_scope AS repaired_scope
    WHERE repaired_scope.id = v_orphan_repair_row.scope_id;

    SELECT jsonb_build_object(
      'id', session_row.id::text,
      'status', session_row.status,
      'progress_state', session_row.progress_state,
      'scope_pending_count', session_row.scope_pending_count,
      'scope_failed_count', session_row.scope_failed_count,
      'progress_counter_version', session_row.progress_counter_version,
      'progress_json', public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true),
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_orphan_session_before_json
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_orphan_repair_row.session_id
    FOR UPDATE;

    BEGIN
      v_progress_recompute_json := public.pay_workbench_session_recompute_progress_counters(
        p_session_id => v_orphan_repair_row.session_id,
        p_apply => true,
        p_reason => 'ORPHANED_PENDING_SOURCE_BUILD_POISON_REPAIRED',
        p_write_progress_json => true
      );
    EXCEPTION WHEN OTHERS THEN
      v_progress_recompute_failed := true;
      v_progress_recompute_error_json := jsonb_build_object(
        'code', SQLSTATE,
        'message', SQLERRM,
        'session_id', v_orphan_repair_row.session_id::text,
        'candidate_id', v_orphan_repair_row.candidate_id::text,
        'bad_job_id', v_orphan_repair_row.bad_job_id::text,
        'successor_job_id', v_orphan_enqueue_job_id::text,
        'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
      );
      v_progress_recompute_json := COALESCE(v_progress_recompute_error_json, '{}'::jsonb);
    END;

    SELECT jsonb_build_object(
      'id', session_row.id::text,
      'status', session_row.status,
      'progress_state', session_row.progress_state,
      'scope_pending_count', session_row.scope_pending_count,
      'scope_failed_count', session_row.scope_failed_count,
      'progress_counter_version', session_row.progress_counter_version,
      'progress_json', public.pay_workbench_session_compact_progress_json(COALESCE(session_row.progress_json, '{}'::jsonb), true),
      'recompute_failed', v_progress_recompute_failed,
      'recompute_json', COALESCE(v_progress_recompute_json, '{}'::jsonb),
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    )
    INTO v_orphan_session_after_json
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_orphan_repair_row.session_id;

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_orphan_repair_row.bad_job_id::text,
      'ORPHANED_PENDING_SOURCE_BUILD_POISON_REPAIRED',
      v_orphan_bad_job_before_json,
      v_orphan_bad_job_after_json,
      'ORPHANED_PENDING_SOURCE_BUILD_POISON_REPAIRED',
      NULL
    );

    PERFORM public._audit_insert(
      'banking_pay_workbench_session_scope',
      v_orphan_repair_row.candidate_id::text,
      'ORPHANED_PENDING_SOURCE_BUILD_POISON_REPAIRED',
      v_orphan_scope_before_json,
      v_orphan_scope_after_json,
      'ORPHANED_PENDING_SOURCE_BUILD_POISON_REPAIRED',
      NULL
    );

    PERFORM public._audit_insert(
      'banking_pay_workbench_session',
      v_orphan_repair_row.session_id::text,
      'PROGRESS_RECOMPUTED_AFTER_ORPHANED_SOURCE_BUILD_POISON',
      v_orphan_session_before_json,
      v_orphan_session_after_json,
      'PROGRESS_RECOMPUTED_AFTER_ORPHANED_SOURCE_BUILD_POISON',
      NULL
    );

    v_repaired_count := v_repaired_count + 1;
    v_repaired_scope_count := v_repaired_scope_count + 1;
    v_repaired_session_count := v_repaired_session_count + 1;
    v_orphan_pending_repaired_count := v_orphan_pending_repaired_count + 1;

    IF lower(BTRIM(COALESCE(v_orphan_enqueue_result->>'reused', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      v_corrected_source_build_job_reused_count := v_corrected_source_build_job_reused_count + 1;
    ELSE
      v_corrected_source_build_job_queued_count := v_corrected_source_build_job_queued_count + 1;
    END IF;

    v_orphan_pending_repairs_json := v_orphan_pending_repairs_json || jsonb_build_array(jsonb_build_object(
      'session_id', v_orphan_repair_row.session_id::text,
      'candidate_id', v_orphan_repair_row.candidate_id::text,
      'bad_job_id', v_orphan_repair_row.bad_job_id::text,
      'successor_job_id', v_orphan_enqueue_job_id::text,
      'successor_job_status', v_orphan_enqueue_job_status,
      'successor_source_build_run_id', v_orphan_enqueue_source_build_run_id_text,
      'refresh_scope_kind', v_orphan_refresh_scope_kind,
      'targeted_timesheet_ids', COALESCE(v_orphan_targeted_timesheet_ids_json, '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(v_orphan_linked_timesheet_ids_json, '[]'::jsonb),
      'failed_job_reference_replaced', true,
      'scope_dirty_preserved', true,
      'session_progress_recomputed', true,
      'progress_recompute_failed', v_progress_recompute_failed,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    ));

    v_repairs_json := v_repairs_json || jsonb_build_array(jsonb_build_object(
      'session_id', v_orphan_repair_row.session_id::text,
      'candidate_id', v_orphan_repair_row.candidate_id::text,
      'bad_job_id', v_orphan_repair_row.bad_job_id::text,
      'corrected_source_build_job_id', v_orphan_enqueue_job_id::text,
      'corrected_source_build_run_id', v_orphan_enqueue_source_build_run_id_text,
      'orphaned_pending_repair', true,
      'scope_repaired', true,
      'session_progress_repaired', true,
      'reason', v_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
    ));
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'server_utc', v_now,
    'reason', v_reason,
    'filtered_session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
    'filtered_candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
    'limit', v_limit,
    'processed_count', v_processed_count,
    'repaired_count', v_repaired_count,
    'skipped_count', v_skipped_count,
    'source_rows_restored_count', v_total_restored_source_row_count,
    'line_seed_requeued_count', v_requeued_line_seed_count,
    'scope_repaired_count', v_repaired_scope_count,
    'session_progress_repaired_count', v_repaired_session_count,
    'zero_source_row_repaired_count', v_zero_source_row_repaired_count,
    'corrected_source_build_job_queued_count', v_corrected_source_build_job_queued_count,
    'corrected_source_build_job_reused_count', v_corrected_source_build_job_reused_count,
    'corrected_source_build_job_succeeded_reused_count', v_corrected_source_build_job_succeeded_reused_count,
    'line_seed_attempt_count_reset_count', v_line_seed_attempt_count_reset_count,
    'post_recompute_progress_state', v_post_recompute_progress_state,
    'post_recompute_scope_failed_count', v_post_recompute_scope_failed_count,
    'post_recompute_scope_pending_count', v_post_recompute_scope_pending_count,
    'zero_source_row_repairs', COALESCE(v_zero_source_row_repairs_json, '[]'::jsonb),
    'zero_source_row_skips', COALESCE(v_zero_source_row_skips_json, '[]'::jsonb),
    'orphaned_pending_repaired_count', v_orphan_pending_repaired_count,
    'orphaned_pending_failed_closed_count', v_orphan_pending_failed_closed_count,
    'orphaned_pending_repairs', COALESCE(v_orphan_pending_repairs_json, '[]'::jsonb),
    'orphaned_pending_skips', COALESCE(v_orphan_pending_skips_json, '[]'::jsonb),
    'repairs', COALESCE(v_repairs_json, '[]'::jsonb),
    'skips', COALESCE(v_skips_json, '[]'::jsonb),
    'policy_x_authority_scope', 'PRE_DRAFT_WORKBENCH_REPAIR_ONLY'
  );
END;
$function$;
ALTER FUNCTION public.pay_workbench_repair_invalid_source_build_poison(uuid, uuid, integer, timestamptz, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_invalid_source_build_poison(uuid, uuid, integer, timestamptz, text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_invalid_source_build_poison(uuid, uuid, integer, timestamptz, text) TO authenticated, service_role;
