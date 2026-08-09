-- Banking Pay Workbench certified CURRENT-source publication readiness guard.
-- Exact existing progress contract retained; one compact attestation invariant added.
-- Policy X remains pre-draft live truth / post-draft frozen artifacts.

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
  v_certified_publication_incomplete_count integer := 0;
  v_certified_publication_incomplete_candidates_json jsonb := '[]'::jsonb;

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
      ) AS has_error,
      scope_row.certified_preview_publication_required IS TRUE AS publication_required,
      (
        scope_row.certified_preview_publication_required IS NOT TRUE
        OR (
          scope_row.certified_preview_publication_parity_ok IS TRUE
          AND scope_row.certified_preview_publication_session_version = v_session_row.version
          AND scope_row.certified_preview_publication_source_change_seq = candidate_state.source_change_seq
          AND scope_row.certified_preview_publication_source_build_run_id IS NOT NULL
          AND scope_row.certified_preview_publication_attested_at_utc IS NOT NULL
          AND scope_row.certified_preview_publication_attestation_json->>'parity_complete'='true'
          AND (
            (
              scope_row.certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1'
              AND scope_row.certified_preview_publication_attestation_json->>'authority_kind'='BOUNDED_FULL_SOURCE_BUILD'
            )
            OR (
              scope_row.certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'
              AND scope_row.certified_preview_publication_attestation_json->>'contract_version'='2'
              AND scope_row.certified_preview_publication_attestation_json->>'authority_kind' IN ('CERTIFIED_CLONE','TARGETED_DELTA')
              AND scope_row.certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
            )
            OR (
              scope_row.certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
              AND scope_row.certified_preview_publication_attestation_json->>'contract_version'='3'
              AND scope_row.certified_preview_publication_attestation_json->>'semantic_contract_version'='READY_TO_PAY_SEMANTIC_V2'
              AND scope_row.certified_preview_publication_attestation_json->>'authority_kind' IN (
                'BOUNDED_FULL_SOURCE_BUILD','CERTIFIED_CLONE','TARGETED_DELTA','CERTIFIED_CANCELLATION_REVERSION'
              )
              AND scope_row.certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
              AND scope_row.certified_preview_publication_attestation_json->>'semantic_ready'='true'
              AND COALESCE((scope_row.certified_preview_publication_attestation_json->>'invalid_selectable_row_count')::integer,-1)=0
              AND (scope_row.certified_preview_publication_attestation_json->>'candidate_ready_amount')::numeric>=0
              AND NULLIF(BTRIM(COALESCE(scope_row.certified_preview_publication_attestation_json->>'semantic_proof_digest','')),'') IS NOT NULL
            )
          )
        )
      ) AS publication_current
    FROM public.banking_pay_workbench_session_scope AS scope_row
    LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
      ON candidate_state.session_id = scope_row.session_id
     AND candidate_state.candidate_id = scope_row.candidate_id
    WHERE scope_row.session_id = p_session_id
  ), counted_scope AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (WHERE seeded IS TRUE)::integer AS seeded_count,
      COUNT(*) FILTER (
        WHERE dirty IS NOT TRUE
          AND has_error IS NOT TRUE
          AND publication_current IS TRUE
          AND status_key IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY')
      )::integer AS ready_count,
      COUNT(*) FILTER (
        WHERE has_error IS TRUE
          OR status_key IN ('FAILED', 'ERROR', 'LINE_WORK_ERROR', 'LINE_WORK_PROCESS_ERROR', 'SOURCE_BUILD_ERROR')
      )::integer AS failed_count,
      COUNT(*) FILTER (WHERE dirty IS TRUE)::integer AS dirty_count,
      COUNT(*) FILTER (WHERE publication_required IS TRUE AND publication_current IS NOT TRUE)::integer AS publication_incomplete_count
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
    GREATEST(COALESCE(dirty_count, 0), 0),
    GREATEST(COALESCE(publication_incomplete_count, 0), 0)
  INTO v_scope_total_count,
       v_scope_seeded_count,
       v_scope_ready_count,
       v_scope_pending_count,
       v_scope_failed_count,
       v_scope_dirty_count,
       v_certified_publication_incomplete_count
  FROM counted_scope;

  SELECT COALESCE(jsonb_agg(sample_row.sample_json ORDER BY sample_row.scope_ordinal, sample_row.candidate_id), '[]'::jsonb)
  INTO v_certified_publication_incomplete_candidates_json
  FROM (
    SELECT scope_row.scope_ordinal,
           scope_row.candidate_id,
           jsonb_build_object(
             'candidate_id', scope_row.candidate_id::text,
             'scope_status', scope_row.status,
             'failure_code', 'CURRENT_SOURCE_PREVIEW_PUBLICATION_INCOMPLETE',
             'message', 'Certified payment preview publication is incomplete.'
           ) AS sample_json
    FROM public.banking_pay_workbench_session_scope AS scope_row
    LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
      ON candidate_state.session_id = scope_row.session_id
     AND candidate_state.candidate_id = scope_row.candidate_id
    WHERE scope_row.session_id = p_session_id
      AND scope_row.certified_preview_publication_required IS TRUE
      AND NOT (
        scope_row.certified_preview_publication_parity_ok IS TRUE
        AND scope_row.certified_preview_publication_session_version = v_session_row.version
        AND scope_row.certified_preview_publication_source_change_seq = candidate_state.source_change_seq
        AND scope_row.certified_preview_publication_source_build_run_id IS NOT NULL
        AND scope_row.certified_preview_publication_attested_at_utc IS NOT NULL
        AND scope_row.certified_preview_publication_attestation_json->>'parity_complete'='true'
        AND (
          (
            scope_row.certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1'
            AND scope_row.certified_preview_publication_attestation_json->>'authority_kind'='BOUNDED_FULL_SOURCE_BUILD'
          )
          OR (
            scope_row.certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'
            AND scope_row.certified_preview_publication_attestation_json->>'contract_version'='2'
            AND scope_row.certified_preview_publication_attestation_json->>'authority_kind' IN ('CERTIFIED_CLONE','TARGETED_DELTA')
            AND scope_row.certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
          )
          OR (
            scope_row.certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
            AND scope_row.certified_preview_publication_attestation_json->>'contract_version'='3'
            AND scope_row.certified_preview_publication_attestation_json->>'semantic_contract_version'='READY_TO_PAY_SEMANTIC_V2'
            AND scope_row.certified_preview_publication_attestation_json->>'authority_kind' IN (
              'BOUNDED_FULL_SOURCE_BUILD','CERTIFIED_CLONE','TARGETED_DELTA','CERTIFIED_CANCELLATION_REVERSION'
            )
            AND scope_row.certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
            AND scope_row.certified_preview_publication_attestation_json->>'semantic_ready'='true'
            AND COALESCE((scope_row.certified_preview_publication_attestation_json->>'invalid_selectable_row_count')::integer,-1)=0
            AND (scope_row.certified_preview_publication_attestation_json->>'candidate_ready_amount')::numeric>=0
            AND NULLIF(BTRIM(COALESCE(scope_row.certified_preview_publication_attestation_json->>'semantic_proof_digest','')),'') IS NOT NULL
          )
        )
      )
    ORDER BY scope_row.scope_ordinal, scope_row.candidate_id
    LIMIT 10
  ) AS sample_row;

  WITH source_pending AS (
    SELECT
      scope_row.candidate_id,
      scope_row.scope_ordinal,
      scope_row.pending_job_id,
      owner_job.status AS owner_status,
      owner_job.attempt_count,
      owner_job.max_attempts,
      successor_lookup.successor_job_id,
      successor_lookup.successor_job_status,
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
      successor_lookup.successor_job_id IS NOT NULL AS successor_valid
    FROM public.banking_pay_workbench_session_scope AS scope_row
    LEFT JOIN public.banking_pay_workbench_jobs AS owner_job
      ON owner_job.id = scope_row.pending_job_id
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key = 'pay_candidate:' || scope_row.candidate_id::text
    LEFT JOIN LATERAL (
      SELECT
        successor_job.id AS successor_job_id,
        UPPER(BTRIM(COALESCE(successor_job.status, ''))) AS successor_job_status
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
      ORDER BY
        CASE WHEN UPPER(BTRIM(COALESCE(successor_job.status, ''))) = 'RUNNING' THEN 0 ELSE 1 END,
        CASE
          WHEN COALESCE(successor_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
            THEN (successor_job.payload_json->>'source_change_seq')::bigint
          ELSE NULL::bigint
        END DESC,
        successor_job.created_at_utc ASC,
        successor_job.id ASC
      LIMIT 1
    ) AS successor_lookup ON TRUE
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
          'successor_job_id', CASE WHEN ownership_sample.successor_job_id IS NULL THEN NULL ELSE ownership_sample.successor_job_id::text END,
          'successor_job_status', ownership_sample.successor_job_status,
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
    OR COALESCE(v_certified_publication_incomplete_count, 0) > 0
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
  ELSIF COALESCE(v_certified_publication_incomplete_count, 0) > 0 THEN
    v_progress_state := 'REFRESHING_CANDIDATES';
    v_phase := 'REFRESHING_CANDIDATES';
    v_status_text := 'Publishing certified payment preview.';
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
    AND COALESCE(v_line_units_pending, 0) = 0
    AND COALESCE(v_certified_publication_incomplete_count, 0) = 0;

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

  IF COALESCE(v_certified_publication_incomplete_count, 0) > 0 THEN
    v_session_blocker_codes := v_session_blocker_codes || jsonb_build_array('CURRENT_SOURCE_PREVIEW_PUBLICATION_INCOMPLETE');
    v_draft_blocker_codes := v_draft_blocker_codes || jsonb_build_array('CURRENT_SOURCE_PREVIEW_PUBLICATION_INCOMPLETE');
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
        'certified_preview_publication_incomplete_count', COALESCE(v_certified_publication_incomplete_count, 0),
        'certified_preview_publication_incomplete_candidates', COALESCE(v_certified_publication_incomplete_candidates_json, '[]'::jsonb),
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
          'automatic_recovery_pending', COALESCE(v_recovery_scheduled_count, 0),
          'current_source_preview_publication_incomplete', COALESCE(v_certified_publication_incomplete_count, 0)
        ),
        'stored_ready_mismatch', false,
        'read_only', v_replacement_required
          OR COALESCE(v_recovery_required_count, 0) > 0
          OR COALESCE(v_certified_publication_incomplete_count, 0) > 0
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

ALTER FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean) TO postgres, authenticated, service_role;

