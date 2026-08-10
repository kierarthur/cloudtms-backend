-- Banking Pay shared-session and exact selection-review guard.
-- Policy X: this validates live workbench truth only before draft freeze; all
-- post-draft behaviour continues to use frozen batch artifacts.

CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft(p_session_id uuid, p_actor_user_id uuid, p_selected_preview_row_ids jsonb DEFAULT NULL::jsonb, p_pay_channel_scope text DEFAULT NULL::text, p_override_reason text DEFAULT NULL::text, p_override_continue boolean DEFAULT false, p_override_verified boolean DEFAULT false, p_override_verified_by_user_id uuid DEFAULT NULL::uuid, p_override_verified_at_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_operation_id uuid DEFAULT NULL::uuid, p_operation_mode boolean DEFAULT false, p_allow_legacy_unchunked boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_scope_filter text := UPPER(BTRIM(COALESCE(NULLIF(p_pay_channel_scope, ''), 'ALL')));
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_operation_mode boolean := (COALESCE(p_operation_mode, false) = true OR p_operation_id IS NOT NULL);
  v_input_count integer := 0;
  v_sample_row_count integer := 0;
  v_sample_candidate_count integer := 0;
  v_sample_paye_count integer := 0;
  v_sample_umbrella_count integer := 0;
  v_selected_ready_row_count integer := 0;
  v_selected_ready_paye_row_count integer := 0;
  v_selected_ready_umbrella_row_count integer := 0;
  v_scope_selected_ready_row_count integer := 0;
  v_no_rows_block_reason_code text := NULL::text;
  v_no_rows_block_message text := NULL::text;
  v_ready_selected_exists boolean := false;
  v_unready_selected_exists boolean := false;
  v_paye_guardrails jsonb := '{}'::jsonb;
  v_paye_create_blocked boolean := false;
  v_paye_override_required boolean := false;
  v_paye_scope_blocked boolean := false;
  v_paye_block_reason_code text := NULL::text;
  v_paye_block_message text := NULL::text;
  v_selected_sample jsonb := '[]'::jsonb;
  v_stale_selection_ids jsonb := '[]'::jsonb;
  v_missing_selection_ids jsonb := '[]'::jsonb;
  v_current_unready_selected_exists boolean := false;
  v_malformed_selected_exists boolean := false;
  v_malformed_selected_sample jsonb := '[]'::jsonb;
  v_synthetic_total_selected_exists boolean := false;
  v_synthetic_total_selected_sample jsonb := '[]'::jsonb;
  v_direct_current_selection_match_count integer := 0;
  v_current_ready_selected_row_count integer := 0;
  v_selection_resolved_to_current boolean := false;
  v_resolved_current_selection_ids jsonb := '[]'::jsonb;
  v_resolved_selection_match_count integer := 0;
  v_resolved_selection_distinct_count integer := 0;
  v_active_workbench_job_count integer := 0;
  v_active_workbench_job_sample jsonb := '[]'::jsonb;
  v_expected_progress_counter_version bigint := NULL::bigint;
  v_expected_progress_counter_version_text text := NULL::text;
  v_expected_selected_preview_row_ids jsonb := '[]'::jsonb;
  v_expected_selected_preview_row_count integer := 0;
  v_expected_selected_preview_row_distinct_count integer := 0;
  v_current_selected_preview_row_ids jsonb := '[]'::jsonb;
  v_current_selected_preview_row_count integer := 0;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_PROGRESS');

  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  IF v_scope_filter NOT IN ('ALL', 'PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'pay_channel_scope must be ALL, PAYE, or UMBRELLA';
  END IF;

  IF COALESCE(p_allow_legacy_unchunked, false) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_PREPARE_DRAFT_LEGACY_UNCHUNKED_DISABLED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_PREPARE_DRAFT_LEGACY_UNCHUNKED_DISABLED',
              'message', 'Legacy all-at-once draft creation is permanently disabled. Use the operation-scoped DRAFT_CREATE runner.',
              'session_id', CASE WHEN p_session_id IS NULL THEN NULL ELSE p_session_id::text END,
              'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END
            )::text;
  END IF;

  IF p_selected_preview_row_ids IS NOT NULL THEN
    IF jsonb_typeof(p_selected_preview_row_ids) <> 'array' THEN
      RAISE EXCEPTION 'selected_preview_row_ids must be a JSON array when supplied';
    END IF;

    v_input_count := jsonb_array_length(p_selected_preview_row_ids);

    IF v_input_count > 100 THEN
      RAISE EXCEPTION 'selected_preview_row_ids exceeds the 100 row operation-validation cap: %', v_input_count;
    END IF;
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'tms_users row % not found', p_actor_user_id;
  END IF;

  IF v_operation_mode IS NOT TRUE THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_REQUIRED',
      'message', 'Normal draft creation must use the scalable DRAFT_CREATE operation flow. Legacy all-at-once draft creation is disabled for the full row-backed cutover.',
      'session_id', p_session_id::text,
      'allow_legacy_unchunked', COALESCE(p_allow_legacy_unchunked, false)
    )::text;
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_ID_REQUIRED',
      'message', 'pay_workbench_prepare_draft operation mode requires p_operation_id.',
      'session_id', p_session_id::text
    )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_NOT_FOUND',
      'message', 'pay_workbench_prepare_draft operation mode could not find the supplied operation.',
      'session_id', p_session_id::text,
      'operation_id', p_operation_id::text
    )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation_row.operation_type, ''))) <> 'DRAFT_CREATE' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_TYPE_INVALID',
      'message', 'pay_workbench_prepare_draft operation mode requires a DRAFT_CREATE operation.',
      'session_id', p_session_id::text,
      'operation_id', p_operation_id::text,
      'operation_type', v_operation_row.operation_type
    )::text;
  END IF;

  IF v_operation_row.workbench_session_id IS DISTINCT FROM p_session_id THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_SESSION_MISMATCH',
      'message', 'pay_workbench_prepare_draft operation must already be bound to this exact workbench session.',
      'session_id', p_session_id::text,
      'operation_id', p_operation_id::text,
      'operation_workbench_session_id', CASE WHEN v_operation_row.workbench_session_id IS NULL THEN NULL ELSE v_operation_row.workbench_session_id::text END
    )::text;
  END IF;

  IF v_operation_row.actor_user_id IS DISTINCT FROM p_actor_user_id THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_ACTOR_MISMATCH',
      'message', 'pay_workbench_prepare_draft operation must already be bound to this exact actor.',
      'session_id', p_session_id::text,
      'operation_id', p_operation_id::text
    )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation_row.status, ''))) <> 'RUNNING' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_STATUS_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_STATUS_INVALID',
              'operation_id', p_operation_id::text,
              'status', v_operation_row.status,
              'required_status', 'RUNNING'
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation_row.phase, ''))) NOT IN ('INITIALISE', 'VALIDATE_SESSION') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_PHASE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_PHASE_INVALID',
              'operation_id', p_operation_id::text,
              'phase', v_operation_row.phase,
              'allowed_phases', jsonb_build_array('INITIALISE', 'VALIDATE_SESSION')
            )::text;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
  END IF;

  -- Workbench sessions are intentionally shared across authenticated Banking Pay
  -- users. session.actor_user_id records who created the session; operation.actor_user_id
  -- above remains the authoritative audit actor for this draft operation.

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN' OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not OPEN', p_session_id;
  END IF;

  IF v_session_row.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'WORKBENCH_SESSION_REPLACED',
      'message', 'This payment workbench has been replaced. Refresh the Banking Pay workbench and create the draft from the current session.',
      'session_id', p_session_id::text,
      'replacement_session_id', v_session_row.replacement_session_id::text
    )::text;
  END IF;

  -- Atomic final selection-review guard. The Worker records the exact global
  -- selected set and progress revision reviewed by the user before starting the
  -- operation. Holding the session row lock prevents a concurrent selection
  -- mutation from passing between this comparison and draft preparation.
  v_expected_progress_counter_version_text := NULLIF(
    BTRIM(COALESCE(v_operation_row.input_json->>'expected_workbench_progress_counter_version', '')),
    ''
  );

  IF v_expected_progress_counter_version_text IS NULL
     OR LENGTH(v_expected_progress_counter_version_text) > 18
     OR v_expected_progress_counter_version_text !~ '^[0-9]+$' THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED',
              'message', 'Banking Pay changed before the draft could start. Review the latest selection, then click Create Draft again.',
              'session_id', p_session_id::text,
              'operation_id', p_operation_id::text,
              'reason', 'EXPECTED_PROGRESS_REVISION_MISSING_OR_INVALID'
            )::text;
  END IF;

  v_expected_progress_counter_version := v_expected_progress_counter_version_text::bigint;

  IF COALESCE(v_session_row.progress_counter_version, 0)::bigint IS DISTINCT FROM v_expected_progress_counter_version THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED',
              'message', 'Banking Pay was changed by another user or window. Review the latest selection, then click Create Draft again.',
              'session_id', p_session_id::text,
              'operation_id', p_operation_id::text,
              'expected_progress_counter_version', v_expected_progress_counter_version,
              'current_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0)
            )::text;
  END IF;

  v_expected_selected_preview_row_ids := COALESCE(
    v_operation_row.input_json->'expected_workbench_selected_preview_row_ids',
    'null'::jsonb
  );

  IF jsonb_typeof(v_expected_selected_preview_row_ids) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED',
              'message', 'Banking Pay changed before the draft could start. Review the latest selection, then click Create Draft again.',
              'session_id', p_session_id::text,
              'operation_id', p_operation_id::text,
              'reason', 'EXPECTED_SELECTION_MISSING_OR_INVALID'
            )::text;
  END IF;

  v_expected_selected_preview_row_count := jsonb_array_length(v_expected_selected_preview_row_ids);

  IF EXISTS (
    SELECT 1
    FROM jsonb_array_elements_text(v_expected_selected_preview_row_ids) AS expected_item(value)
    WHERE BTRIM(expected_item.value) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED',
              'message', 'Banking Pay changed before the draft could start. Review the latest selection, then click Create Draft again.',
              'session_id', p_session_id::text,
              'operation_id', p_operation_id::text,
              'reason', 'EXPECTED_SELECTION_ROW_ID_INVALID'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_expected_selected_preview_row_distinct_count
  FROM (
    SELECT DISTINCT LOWER(BTRIM(expected_item.value)) AS preview_row_id
    FROM jsonb_array_elements_text(v_expected_selected_preview_row_ids) AS expected_item(value)
  ) AS distinct_expected_rows;

  IF v_expected_selected_preview_row_count IS DISTINCT FROM v_expected_selected_preview_row_distinct_count THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED',
              'message', 'Banking Pay changed before the draft could start. Review the latest selection, then click Create Draft again.',
              'session_id', p_session_id::text,
              'operation_id', p_operation_id::text,
              'reason', 'EXPECTED_SELECTION_CONTAINS_DUPLICATES'
            )::text;
  END IF;

  SELECT COALESCE(jsonb_agg(to_jsonb(expected_rows.preview_row_id) ORDER BY expected_rows.preview_row_id), '[]'::jsonb)
  INTO v_expected_selected_preview_row_ids
  FROM (
    SELECT DISTINCT LOWER(BTRIM(expected_item.value)) AS preview_row_id
    FROM jsonb_array_elements_text(v_expected_selected_preview_row_ids) AS expected_item(value)
  ) AS expected_rows;

  SELECT COALESCE(jsonb_agg(to_jsonb(current_rows.preview_row_id) ORDER BY current_rows.preview_row_id), '[]'::jsonb),
         COUNT(*)::integer
  INTO v_current_selected_preview_row_ids,
       v_current_selected_preview_row_count
  FROM (
    SELECT preview_row.id::text AS preview_row_id
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = v_session_row.version
      AND LOWER(private.pay_workbench_preview_effective_section_v1(
            preview_row.section,
            preview_row.row_json
          )) = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
      AND COALESCE(preview_row.selected, false) = true
      AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
  ) AS current_rows;

  IF v_current_selected_preview_row_count IS DISTINCT FROM v_expected_selected_preview_row_count
     OR v_current_selected_preview_row_ids IS DISTINCT FROM v_expected_selected_preview_row_ids THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED',
              'message', 'Banking Pay was changed by another user or window. Review the latest selection, then click Create Draft again.',
              'session_id', p_session_id::text,
              'operation_id', p_operation_id::text,
              'expected_progress_counter_version', v_expected_progress_counter_version,
              'current_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
              'expected_selected_preview_row_count', v_expected_selected_preview_row_count,
              'current_selected_preview_row_count', v_current_selected_preview_row_count
            )::text;
  END IF;

  WITH active_workbench_jobs AS (
    SELECT job_row.id,
           job_row.job_type,
           job_row.status,
           job_row.candidate_id,
           CASE
             WHEN UPPER(BTRIM(COALESCE(job_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH') THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
             WHEN UPPER(BTRIM(COALESCE(job_row.job_type, ''))) IN ('WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE') THEN 'WORKBENCH_SESSION_CLONE_REBASE'
             WHEN UPPER(BTRIM(COALESCE(job_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE') THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
             WHEN UPPER(BTRIM(COALESCE(job_row.job_type, ''))) IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED', 'WORKBENCH_SCOPE_SEED_PAGE', 'SCOPE_SEED_PAGE') THEN 'WORKBENCH_SESSION_SCOPE_SEED'
             WHEN UPPER(BTRIM(COALESCE(job_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
             WHEN UPPER(BTRIM(COALESCE(job_row.job_type, ''))) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
             WHEN UPPER(BTRIM(COALESCE(job_row.job_type, ''))) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK') THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
             ELSE UPPER(BTRIM(COALESCE(job_row.job_type, '')))
           END AS canonical_job_type
    FROM public.banking_pay_workbench_jobs AS job_row
    WHERE job_row.session_id = p_session_id
      AND UPPER(BTRIM(COALESCE(job_row.status, ''))) IN ('QUEUED', 'RUNNING')
  ), blocking_jobs AS (
    SELECT active_workbench_jobs.*
    FROM active_workbench_jobs
    WHERE active_workbench_jobs.canonical_job_type IN (
      'WORKBENCH_SESSION_SCOPE_SEED',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'WORKBENCH_SESSION_CLONE_REBASE',
      'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
      'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
      'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
    )
  )
  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(jsonb_build_object(
           'job_id', blocking_jobs.id::text,
           'job_type', blocking_jobs.job_type,
           'canonical_job_type', blocking_jobs.canonical_job_type,
           'status', blocking_jobs.status,
           'candidate_id', CASE WHEN blocking_jobs.candidate_id IS NULL THEN NULL ELSE blocking_jobs.candidate_id::text END
         ) ORDER BY blocking_jobs.canonical_job_type, blocking_jobs.id) FILTER (WHERE blocking_jobs.id IS NOT NULL), '[]'::jsonb)
  INTO v_active_workbench_job_count,
       v_active_workbench_job_sample
  FROM blocking_jobs;

  IF COALESCE(v_active_workbench_job_count, 0) > 0 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'WORKBENCH_REFRESH_IN_PROGRESS',
      'message', 'Banking Pay is refreshing this workbench. Wait for the refresh to finish, review the selection, and then create the draft.',
      'session_id', p_session_id::text,
      'active_workbench_job_count', COALESCE(v_active_workbench_job_count, 0),
      'active_workbench_jobs', COALESCE(v_active_workbench_job_sample, '[]'::jsonb)
    )::text;
  END IF;

  UPDATE public.banking_pay_operations AS operation_update
  SET updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids ON COMMIT DROP AS
  SELECT DISTINCT BTRIM(supplied_id.value) AS supplied_id
  FROM jsonb_array_elements_text(COALESCE(p_selected_preview_row_ids, '[]'::jsonb)) AS supplied_id(value)
  WHERE p_selected_preview_row_ids IS NOT NULL
    AND BTRIM(supplied_id.value) <> '';

  IF p_selected_preview_row_ids IS NOT NULL AND COALESCE(v_input_count, 0) > 0 THEN
    DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_prepare_draft_resolved_ids;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_prepare_draft_resolved_ids ON COMMIT DROP AS
    WITH supplied_ids AS (
      SELECT supplied_id.supplied_id
      FROM pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids AS supplied_id
    ),
    direct_current_matches AS (
      SELECT DISTINCT
        supplied_ids.supplied_id,
        current_preview_row.id::text AS resolved_id
      FROM supplied_ids
      JOIN public.banking_pay_workbench_preview_rows AS current_preview_row
        ON current_preview_row.session_id = p_session_id
       AND current_preview_row.session_version = v_session_row.version
       AND UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
       AND COALESCE(current_preview_row.selected, false) = true
       AND UPPER(BTRIM(COALESCE(current_preview_row.selection_state, ''))) = 'SELECTED'
       AND UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND (
         current_preview_row.id::text = supplied_ids.supplied_id
         OR current_preview_row.row_key = supplied_ids.supplied_id
         OR current_preview_row.row_json->>'preview_row_id' = supplied_ids.supplied_id
         OR current_preview_row.row_json->>'row_id' = supplied_ids.supplied_id
         OR current_preview_row.row_json->>'line_id' = supplied_ids.supplied_id
       )
    ),
    historical_candidates AS (
      SELECT
        supplied_ids.supplied_id,
        historical_preview_row.id AS historical_preview_id,
        historical_preview_row.session_version,
        historical_preview_row.row_ordinal,
        historical_preview_row.candidate_id,
        historical_preview_row.timesheet_id,
        historical_preview_row.row_key,
        private.pay_workbench_preview_effective_section_v1(
          historical_preview_row.section,
          historical_preview_row.row_json
        ) AS section,
        NULLIF(BTRIM(COALESCE(historical_preview_row.key_type, historical_preview_row.row_json#>>'{economic_key,key_type}', historical_preview_row.row_json->>'component_key_type', '')), '') AS key_type,
        NULLIF(BTRIM(COALESCE(historical_preview_row.key_value, historical_preview_row.row_json#>>'{economic_key,key_value}', historical_preview_row.row_json->>'component_key_value', '')), '') AS key_value,
        UPPER(BTRIM(COALESCE(historical_preview_row.row_json->>'pay_channel', historical_preview_row.row_json->>'current_pay_method', historical_preview_row.row_json->>'candidate_pay_method', ''))) AS pay_channel,
        ROW_NUMBER() OVER (
          PARTITION BY supplied_ids.supplied_id
          ORDER BY historical_preview_row.session_version DESC, historical_preview_row.updated_at_utc DESC NULLS LAST, historical_preview_row.created_at_utc DESC NULLS LAST, historical_preview_row.id DESC
        ) AS match_rank
      FROM supplied_ids
      JOIN public.banking_pay_workbench_preview_rows AS historical_preview_row
        ON historical_preview_row.session_id = p_session_id
       AND (
         historical_preview_row.id::text = supplied_ids.supplied_id
         OR historical_preview_row.row_key = supplied_ids.supplied_id
         OR historical_preview_row.row_json->>'preview_row_id' = supplied_ids.supplied_id
         OR historical_preview_row.row_json->>'row_id' = supplied_ids.supplied_id
         OR historical_preview_row.row_json->>'line_id' = supplied_ids.supplied_id
       )
    ),
    historical_best AS (
      SELECT
        historical_candidates.supplied_id,
        historical_candidates.candidate_id,
        historical_candidates.timesheet_id,
        historical_candidates.row_key,
        historical_candidates.section,
        historical_candidates.key_type,
        historical_candidates.key_value,
        historical_candidates.pay_channel
      FROM historical_candidates
      WHERE historical_candidates.match_rank = 1
    ),
    economic_key_current_matches AS (
      SELECT DISTINCT
        historical_best.supplied_id,
        current_preview_row.id::text AS resolved_id
      FROM historical_best
      JOIN public.banking_pay_workbench_preview_rows AS current_preview_row
        ON current_preview_row.session_id = p_session_id
       AND current_preview_row.session_version = v_session_row.version
       AND UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
       AND COALESCE(current_preview_row.selected, false) = true
       AND UPPER(BTRIM(COALESCE(current_preview_row.selection_state, ''))) = 'SELECTED'
       AND UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND current_preview_row.candidate_id IS NOT DISTINCT FROM historical_best.candidate_id
       AND current_preview_row.timesheet_id IS NOT DISTINCT FROM historical_best.timesheet_id
       AND private.pay_workbench_preview_effective_section_v1(
             current_preview_row.section,
             current_preview_row.row_json
           ) = COALESCE(NULLIF(BTRIM(historical_best.section), ''), 'canonical_preview_lines')
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_type, current_preview_row.row_json#>>'{economic_key,key_type}', current_preview_row.row_json->>'component_key_type', '')), '') IS NOT DISTINCT FROM historical_best.key_type
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_value, current_preview_row.row_json#>>'{economic_key,key_value}', current_preview_row.row_json->>'component_key_value', '')), '') IS NOT DISTINCT FROM historical_best.key_value
       AND NULLIF(BTRIM(current_preview_row.row_key), '') IS NOT DISTINCT FROM NULLIF(BTRIM(historical_best.row_key), '')
       AND (
         historical_best.pay_channel IS NULL
         OR historical_best.pay_channel = ''
         OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = historical_best.pay_channel
       )
    ),
    operation_contracts AS (
      SELECT
        contract_entry.ordinality::integer AS contract_index,
        NULLIF(BTRIM(COALESCE(
          contract_entry.value->>'preview_row_id',
          contract_entry.value->>'materialised_preview_row_id',
          contract_entry.value->>'row_id',
          contract_entry.value->>'id',
          contract_entry.value->>'selected_preview_row_id'
        )), '') AS supplied_id,
        NULLIF(BTRIM(COALESCE(contract_entry.value->>'presentation_preview_row_id', contract_entry.value->>'line_id', '')), '') AS presentation_preview_row_id,
        NULLIF(BTRIM(COALESCE(contract_entry.value->>'row_key', '')), '') AS row_key,
        COALESCE(NULLIF(BTRIM(contract_entry.value->>'section'), ''), 'canonical_preview_lines') AS section,
        CASE
          WHEN NULLIF(BTRIM(COALESCE(contract_entry.value->>'candidate_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN NULLIF(BTRIM(COALESCE(contract_entry.value->>'candidate_id', '')), '')::uuid
          ELSE NULL::uuid
        END AS candidate_id,
        CASE
          WHEN NULLIF(BTRIM(COALESCE(contract_entry.value->>'timesheet_id', contract_entry.value#>>'{economic_key,timesheet_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN NULLIF(BTRIM(COALESCE(contract_entry.value->>'timesheet_id', contract_entry.value#>>'{economic_key,timesheet_id}', '')), '')::uuid
          ELSE NULL::uuid
        END AS timesheet_id,
        NULLIF(BTRIM(COALESCE(contract_entry.value->>'key_type', contract_entry.value#>>'{economic_key,key_type}', '')), '') AS key_type,
        NULLIF(BTRIM(COALESCE(contract_entry.value->>'key_value', contract_entry.value#>>'{economic_key,key_value}', '')), '') AS key_value,
        UPPER(NULLIF(BTRIM(COALESCE(contract_entry.value->>'pay_channel', '')), '')) AS pay_channel
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(v_operation_row.input_json->'draft_selected_preview_row_contracts', '[]'::jsonb)) = 'array'
            THEN COALESCE(v_operation_row.input_json->'draft_selected_preview_row_contracts', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS contract_entry(value, ordinality)
      WHERE jsonb_typeof(contract_entry.value) = 'object'
    ),
    operation_contract_current_matches AS (
      SELECT DISTINCT
        supplied_ids.supplied_id,
        current_preview_row.id::text AS resolved_id
      FROM supplied_ids
      JOIN operation_contracts AS operation_contract
        ON (
          operation_contract.supplied_id = supplied_ids.supplied_id
          OR operation_contract.presentation_preview_row_id = supplied_ids.supplied_id
          OR operation_contract.row_key = supplied_ids.supplied_id
        )
      JOIN public.banking_pay_workbench_preview_rows AS current_preview_row
        ON current_preview_row.session_id = p_session_id
       AND current_preview_row.session_version = v_session_row.version
       AND UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
       AND COALESCE(current_preview_row.selected, false) = true
       AND UPPER(BTRIM(COALESCE(current_preview_row.selection_state, ''))) = 'SELECTED'
       AND UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND operation_contract.candidate_id IS NOT NULL
       AND operation_contract.timesheet_id IS NOT NULL
       AND operation_contract.row_key IS NOT NULL
       AND operation_contract.key_type IS NOT NULL
       AND operation_contract.key_value IS NOT NULL
       AND current_preview_row.candidate_id IS NOT DISTINCT FROM operation_contract.candidate_id
       AND current_preview_row.timesheet_id IS NOT DISTINCT FROM operation_contract.timesheet_id
       AND private.pay_workbench_preview_effective_section_v1(
             current_preview_row.section,
             current_preview_row.row_json
           ) = COALESCE(NULLIF(BTRIM(operation_contract.section), ''), 'canonical_preview_lines')
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_type, current_preview_row.row_json#>>'{economic_key,key_type}', current_preview_row.row_json->>'component_key_type', '')), '') IS NOT DISTINCT FROM operation_contract.key_type
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_value, current_preview_row.row_json#>>'{economic_key,key_value}', current_preview_row.row_json->>'component_key_value', '')), '') IS NOT DISTINCT FROM operation_contract.key_value
       AND NULLIF(BTRIM(current_preview_row.row_key), '') IS NOT DISTINCT FROM NULLIF(BTRIM(operation_contract.row_key), '')
       AND (
         operation_contract.pay_channel IS NULL
         OR operation_contract.pay_channel = ''
         OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = operation_contract.pay_channel
       )
    )
    SELECT
      supplied_ids.supplied_id,
      COALESCE(direct_current_matches.resolved_id, economic_key_current_matches.resolved_id, operation_contract_current_matches.resolved_id) AS resolved_id
    FROM supplied_ids
    LEFT JOIN direct_current_matches
      ON direct_current_matches.supplied_id = supplied_ids.supplied_id
    LEFT JOIN economic_key_current_matches
      ON economic_key_current_matches.supplied_id = supplied_ids.supplied_id
    LEFT JOIN operation_contract_current_matches
      ON operation_contract_current_matches.supplied_id = supplied_ids.supplied_id;

    SELECT COUNT(*)::integer,
           COUNT(DISTINCT resolved_selection.resolved_id)::integer,
           COALESCE(jsonb_agg(to_jsonb(resolved_selection.resolved_id) ORDER BY resolved_selection.resolved_id), '[]'::jsonb)
    INTO v_resolved_selection_match_count, v_resolved_selection_distinct_count, v_resolved_current_selection_ids
    FROM pg_temp.tmp_pay_workbench_prepare_draft_resolved_ids AS resolved_selection
    WHERE NULLIF(BTRIM(COALESCE(resolved_selection.resolved_id, '')), '') IS NOT NULL;

    IF COALESCE(v_resolved_selection_match_count, 0) = COALESCE(v_input_count, 0)
       AND COALESCE(v_resolved_selection_distinct_count, 0) = COALESCE(v_input_count, 0) THEN
      SELECT EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_workbench_prepare_draft_resolved_ids AS resolved_selection
        WHERE resolved_selection.resolved_id IS DISTINCT FROM resolved_selection.supplied_id
      )
      INTO v_selection_resolved_to_current;

      TRUNCATE TABLE pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids;

      INSERT INTO pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids (supplied_id)
      SELECT resolved_selection.resolved_id
      FROM pg_temp.tmp_pay_workbench_prepare_draft_resolved_ids AS resolved_selection
      WHERE NULLIF(BTRIM(COALESCE(resolved_selection.resolved_id, '')), '') IS NOT NULL
      ORDER BY resolved_selection.resolved_id;
    ELSE
      SELECT COUNT(*)::integer
      INTO v_current_ready_selected_row_count
      FROM public.banking_pay_workbench_preview_rows AS current_preview_row
      WHERE current_preview_row.session_id = p_session_id
        AND current_preview_row.session_version = v_session_row.version
        AND UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
        AND COALESCE(current_preview_row.selected, false) = true
        AND UPPER(BTRIM(COALESCE(current_preview_row.selection_state, ''))) = 'SELECTED'
        AND UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
        AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (v_scope_filter = 'ALL' OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = v_scope_filter);
    END IF;
  END IF;

  IF p_selected_preview_row_ids IS NOT NULL THEN
    SELECT COALESCE(jsonb_agg(to_jsonb(supplied_ids.supplied_id) ORDER BY supplied_ids.supplied_id), '[]'::jsonb)
    INTO v_stale_selection_ids
    FROM pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids AS supplied_ids
    WHERE EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS any_preview_row
      WHERE any_preview_row.session_id = p_session_id
        AND (
          any_preview_row.id::text = supplied_ids.supplied_id
          OR any_preview_row.row_key = supplied_ids.supplied_id
          OR any_preview_row.row_json->>'preview_row_id' = supplied_ids.supplied_id
          OR any_preview_row.row_json->>'row_id' = supplied_ids.supplied_id
          OR any_preview_row.row_json->>'line_id' = supplied_ids.supplied_id
        )
    )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS current_preview_row
        WHERE current_preview_row.session_id = p_session_id
          AND current_preview_row.session_version = v_session_row.version
          AND UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
          AND (
            current_preview_row.id::text = supplied_ids.supplied_id
            OR current_preview_row.row_key = supplied_ids.supplied_id
            OR current_preview_row.row_json->>'preview_row_id' = supplied_ids.supplied_id
            OR current_preview_row.row_json->>'row_id' = supplied_ids.supplied_id
            OR current_preview_row.row_json->>'line_id' = supplied_ids.supplied_id
          )
      );

    SELECT COALESCE(jsonb_agg(to_jsonb(supplied_ids.supplied_id) ORDER BY supplied_ids.supplied_id), '[]'::jsonb)
    INTO v_missing_selection_ids
    FROM pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids AS supplied_ids
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS any_preview_row
      WHERE any_preview_row.session_id = p_session_id
        AND (
          any_preview_row.id::text = supplied_ids.supplied_id
          OR any_preview_row.row_key = supplied_ids.supplied_id
          OR any_preview_row.row_json->>'preview_row_id' = supplied_ids.supplied_id
          OR any_preview_row.row_json->>'row_id' = supplied_ids.supplied_id
          OR any_preview_row.row_json->>'line_id' = supplied_ids.supplied_id
        )
    );

    IF jsonb_array_length(COALESCE(v_stale_selection_ids, '[]'::jsonb)) > 0
       OR jsonb_array_length(COALESCE(v_missing_selection_ids, '[]'::jsonb)) > 0 THEN
      RAISE EXCEPTION 'WORKBENCH_STALE_SELECTION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORKBENCH_STALE_SELECTION',
                'session_id', p_session_id::text,
                'session_version', v_session_row.version,
                'rejected_preview_row_ids', COALESCE(v_stale_selection_ids, '[]'::jsonb) || COALESCE(v_missing_selection_ids, '[]'::jsonb),
                'stale_preview_row_ids', COALESCE(v_stale_selection_ids, '[]'::jsonb),
                'missing_preview_row_ids', COALESCE(v_missing_selection_ids, '[]'::jsonb),
                'message', 'Selected preview rows are not part of the current ready workbench session version. Refresh the preview page and try again.'
              )::text;
    END IF;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_preview_rows AS blocked_selected_row
    WHERE blocked_selected_row.session_id = p_session_id
      AND blocked_selected_row.session_version = v_session_row.version
      AND COALESCE(blocked_selected_row.selected, false) = true
      AND (v_scope_filter = 'ALL' OR UPPER(BTRIM(COALESCE(blocked_selected_row.row_json->>'pay_channel', blocked_selected_row.row_json->>'current_pay_method', blocked_selected_row.row_json->>'candidate_pay_method', ''))) = v_scope_filter)
      AND (
        p_selected_preview_row_ids IS NULL
        OR EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids AS supplied_id
          WHERE supplied_id.supplied_id IN (blocked_selected_row.id::text, blocked_selected_row.row_key, blocked_selected_row.row_json->>'preview_row_id', blocked_selected_row.row_json->>'row_id', blocked_selected_row.row_json->>'line_id')
        )
      )
      AND (
        UPPER(BTRIM(COALESCE(blocked_selected_row.status, ''))) <> 'READY'
        OR UPPER(BTRIM(COALESCE(blocked_selected_row.selection_state, ''))) <> 'SELECTED'
      )
  )
  INTO v_current_unready_selected_exists;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE full_ready_selected.pay_channel = 'PAYE')::integer,
         COUNT(*) FILTER (WHERE full_ready_selected.pay_channel = 'UMBRELLA')::integer,
         COUNT(*) FILTER (WHERE v_scope_filter = 'ALL' OR full_ready_selected.pay_channel = v_scope_filter)::integer
  INTO v_selected_ready_row_count,
       v_selected_ready_paye_row_count,
       v_selected_ready_umbrella_row_count,
       v_scope_selected_ready_row_count
  FROM (
    SELECT
      UPPER(BTRIM(COALESCE(full_preview_row.row_json->>'pay_channel', full_preview_row.row_json->>'current_pay_method', full_preview_row.row_json->>'candidate_pay_method', ''))) AS pay_channel
    FROM public.banking_pay_workbench_preview_rows AS full_preview_row
    WHERE full_preview_row.session_id = p_session_id
      AND full_preview_row.session_version = v_session_row.version
      AND UPPER(BTRIM(COALESCE(full_preview_row.status, ''))) = 'READY'
      AND COALESCE(full_preview_row.selected, false) = true
      AND UPPER(BTRIM(COALESCE(full_preview_row.selection_state, ''))) = 'SELECTED'
      AND UPPER(BTRIM(COALESCE(full_preview_row.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
      AND LOWER(BTRIM(COALESCE(full_preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(full_preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND UPPER(BTRIM(COALESCE(full_preview_row.row_json->>'pay_channel', full_preview_row.row_json->>'current_pay_method', full_preview_row.row_json->>'candidate_pay_method', ''))) IN ('PAYE', 'UMBRELLA')
      AND (
        p_selected_preview_row_ids IS NULL
        OR EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids AS supplied_id
          WHERE supplied_id.supplied_id IN (full_preview_row.id::text, full_preview_row.row_key, full_preview_row.row_json->>'preview_row_id', full_preview_row.row_json->>'row_id', full_preview_row.row_json->>'line_id')
        )
      )
  ) AS full_ready_selected;

  IF COALESCE(v_scope_selected_ready_row_count, 0) = 0 THEN
    v_no_rows_block_reason_code := CASE
      WHEN v_scope_filter = 'PAYE' THEN 'NO_PAYE_ROWS_FOR_DRAFT_SCOPE'
      WHEN v_scope_filter = 'UMBRELLA' THEN 'NO_UMBRELLA_ROWS_FOR_DRAFT_SCOPE'
      ELSE 'NO_SELECTED_READY_ROWS'
    END;

    v_no_rows_block_message := CASE
      WHEN v_scope_filter = 'PAYE' THEN 'No PAYE rows are available for Create Draft. Choose Umbrella only or select PAYE-ready rows.'
      WHEN v_scope_filter = 'UMBRELLA' THEN 'No Umbrella rows are available for Create Draft. Choose PAYE only or select Umbrella-ready rows.'
      ELSE 'No selected Ready to Pay rows are available for Create Draft.'
    END;

    UPDATE public.banking_pay_operations AS operation_update
    SET progress_json = jsonb_strip_nulls(
          COALESCE(operation_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'draft_prepare_validated_at_utc', v_now::text,
            'draft_prepare_scope', v_scope_filter,
            'draft_prepare_block_reason_code', v_no_rows_block_reason_code,
            'draft_prepare_selected_ready_row_count', COALESCE(v_selected_ready_row_count, 0),
            'draft_prepare_selected_ready_paye_row_count', COALESCE(v_selected_ready_paye_row_count, 0),
            'draft_prepare_selected_ready_umbrella_row_count', COALESCE(v_selected_ready_umbrella_row_count, 0),
            'draft_prepare_scope_selected_ready_row_count', COALESCE(v_scope_selected_ready_row_count, 0),
            'draft_prepare_input_selection_count', COALESCE(v_input_count, 0),
            'draft_prepare_selection_resolved_to_current', COALESCE(v_selection_resolved_to_current, false)
          )
        ),
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    RETURN jsonb_build_object(
      'ok', true,
      'operation_mode', true,
      'operation_id', p_operation_id::text,
      'session_id', p_session_id::text,
      'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
      'session_version', v_session_row.version,
      'pay_date', CASE WHEN v_session_row.pay_date IS NULL THEN NULL ELSE v_session_row.pay_date::text END,
      'week_ending_cutoff', CASE WHEN v_session_row.week_ending_cutoff IS NULL THEN NULL ELSE v_session_row.week_ending_cutoff::text END,
      'pay_channel_scope', v_scope_filter,
      'selected_preview_row_count_known', true,
      'selected_preview_row_sample_count', 0,
      'selected_candidate_sample_count', 0,
      'paye_selected_row_sample_count', 0,
      'umbrella_selected_row_sample_count', 0,
      'selected_ready_row_count', COALESCE(v_selected_ready_row_count, 0),
      'selected_ready_paye_row_count', COALESCE(v_selected_ready_paye_row_count, 0),
      'selected_ready_umbrella_row_count', COALESCE(v_selected_ready_umbrella_row_count, 0),
      'scope_selected_ready_row_count', COALESCE(v_scope_selected_ready_row_count, 0),
      'scope_counts', jsonb_build_object(
        'selected_ready_total', COALESCE(v_selected_ready_row_count, 0),
        'selected_ready_paye', COALESCE(v_selected_ready_paye_row_count, 0),
        'selected_ready_umbrella', COALESCE(v_selected_ready_umbrella_row_count, 0),
        'selected_ready_for_scope', COALESCE(v_scope_selected_ready_row_count, 0)
      ),
      'selected_row_sample', '[]'::jsonb,
      'input_selected_preview_row_count', COALESCE(v_input_count, 0),
      'selection_resolved_to_current', COALESCE(v_selection_resolved_to_current, false),
      'resolved_current_selected_preview_row_ids', COALESCE(v_resolved_current_selection_ids, '[]'::jsonb),
      'same_week_paye_override', jsonb_build_object(
        'guardrails', '{}'::jsonb,
        'create_blocked', false,
        'override_required', false,
        'scope_blocked', false,
        'block_reason_code', NULL,
        'block_message', NULL,
        'override_reason_present', (NULLIF(BTRIM(COALESCE(p_override_reason, '')), '') IS NOT NULL),
        'override_continue', COALESCE(p_override_continue, false),
        'override_verified', COALESCE(p_override_verified, false),
        'override_verified_by_user_id', CASE WHEN p_override_verified_by_user_id IS NULL THEN NULL ELSE p_override_verified_by_user_id::text END,
        'override_verified_at_utc', CASE WHEN p_override_verified_at_utc IS NULL THEN NULL ELSE p_override_verified_at_utc::text END
      ),
      'next_phase', 'BLOCKED',
      'block_reason_code', v_no_rows_block_reason_code,
      'message', v_no_rows_block_message,
      'requires_row_backed_scope_seed', false
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_prepare_draft_selected_sample;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_prepare_draft_selected_sample ON COMMIT DROP AS
  WITH selected_rows AS (
    SELECT
      preview_row.id,
      preview_row.row_key,
      preview_row.candidate_id,
      preview_row.timesheet_id,
      preview_row.row_ordinal,
      UPPER(BTRIM(COALESCE(preview_row.row_json->>'pay_channel', preview_row.row_json->>'current_pay_method', preview_row.row_json->>'candidate_pay_method', ''))) AS pay_channel,
      preview_row.status,
      preview_row.selection_state,
      preview_row.selected,
      preview_row.key_type,
      preview_row.key_value,
      private.pay_workbench_preview_effective_section_v1(
        preview_row.section,
        preview_row.row_json
      ) AS section,
      preview_row.row_json,
      jsonb_strip_nulls(jsonb_build_object(
        'timesheet_id', CASE WHEN preview_row.timesheet_id IS NULL THEN NULL ELSE preview_row.timesheet_id::text END,
        'key_type', NULLIF(BTRIM(COALESCE(preview_row.key_type, preview_row.row_json#>>'{economic_key,key_type}', preview_row.row_json->>'component_key_type', '')), ''),
        'key_value', NULLIF(BTRIM(COALESCE(preview_row.key_value, preview_row.row_json#>>'{economic_key,key_value}', preview_row.row_json->>'component_key_value', '')), '')
      )) AS economic_key_json
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = v_session_row.version
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
      AND COALESCE(preview_row.selected, false) = true
      AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
      AND (v_scope_filter = 'ALL' OR UPPER(BTRIM(COALESCE(preview_row.row_json->>'pay_channel', preview_row.row_json->>'current_pay_method', preview_row.row_json->>'candidate_pay_method', ''))) = v_scope_filter)
      AND (
        p_selected_preview_row_ids IS NULL
        OR EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_workbench_prepare_draft_supplied_ids AS supplied_id
          WHERE supplied_id.supplied_id IN (preview_row.id::text, preview_row.row_key, preview_row.row_json->>'preview_row_id', preview_row.row_json->>'row_id', preview_row.row_json->>'line_id')
        )
      )
    ORDER BY preview_row.row_ordinal, preview_row.id
    LIMIT 101
  )
  SELECT
    selected_rows.*,
    public.pay_workbench_preview_line_contract_ok(
      p_line_json => jsonb_strip_nulls(
        selected_rows.row_json
        || jsonb_build_object(
          'row_key', selected_rows.row_key,
          'preview_row_pk', selected_rows.id::text,
          'selection_state', selected_rows.selection_state
        )
      ),
      p_economic_key_json => selected_rows.economic_key_json,
      p_target_section => COALESCE(NULLIF(BTRIM(selected_rows.section), ''), 'canonical_preview_lines')
    ) AS preview_contract_json
  FROM selected_rows;


  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', synthetic_total_rows.id::text,
           'row_key', synthetic_total_rows.row_key,
           'candidate_id', synthetic_total_rows.candidate_id::text,
           'timesheet_id', CASE WHEN synthetic_total_rows.timesheet_id IS NULL THEN NULL ELSE synthetic_total_rows.timesheet_id::text END,
           'key_type', synthetic_total_rows.key_type,
           'key_value', synthetic_total_rows.key_value,
           'amount_ex_vat', synthetic_total_rows.row_json->>'amount_ex_vat',
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_SELECTED'
         ) ORDER BY synthetic_total_rows.row_ordinal, synthetic_total_rows.id), '[]'::jsonb)
  INTO v_synthetic_total_selected_sample
  FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS synthetic_total_rows
  WHERE UPPER(BTRIM(COALESCE(synthetic_total_rows.key_type, synthetic_total_rows.row_json#>>'{economic_key,key_type}', ''))) = 'TS_TOTAL'
    AND UPPER(BTRIM(COALESCE(synthetic_total_rows.key_value, synthetic_total_rows.row_json#>>'{economic_key,key_value}', ''))) = 'TOTAL'
    AND LOWER(BTRIM(COALESCE(synthetic_total_rows.row_key, synthetic_total_rows.row_json->>'row_key', synthetic_total_rows.row_json->>'line_key', synthetic_total_rows.row_json->>'source_ref', ''))) LIKE '%:non_segment:total%'
    AND (
      lower(btrim(coalesce(synthetic_total_rows.row_json->>'resolved_segment_rows_replace_source_total', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json#>>'{case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR (
        COALESCE(synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
        AND (synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_component_count}')::integer > 0
      )
      OR EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS sibling_segment
        WHERE sibling_segment.candidate_id = synthetic_total_rows.candidate_id
          AND sibling_segment.timesheet_id IS NOT DISTINCT FROM synthetic_total_rows.timesheet_id
          AND UPPER(BTRIM(COALESCE(sibling_segment.key_type, sibling_segment.row_json#>>'{economic_key,key_type}', ''))) = 'TS_DAY'
          AND LOWER(BTRIM(COALESCE(sibling_segment.row_key, sibling_segment.row_json->>'row_key', sibling_segment.row_json->>'line_key', sibling_segment.row_json->>'source_ref', ''))) LIKE '%:segment:%'
      )
    );

  v_synthetic_total_selected_exists := jsonb_array_length(COALESCE(v_synthetic_total_selected_sample, '[]'::jsonb)) > 0;

  IF COALESCE(v_synthetic_total_selected_exists, false) THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_NOT_DRAFTABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_NOT_DRAFTABLE',
              'session_id', p_session_id::text,
              'session_version', v_session_row.version,
              'operation_id', p_operation_id::text,
              'synthetic_total_selected_preview_rows', COALESCE(v_synthetic_total_selected_sample, '[]'::jsonb),
              'message', 'A selected resolved timesheet row is stale and cannot be drafted. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  SELECT EXISTS (
           SELECT 1
           FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS malformed_sample
           WHERE LOWER(BTRIM(COALESCE(malformed_sample.preview_contract_json->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
              OR LOWER(BTRIM(COALESCE(malformed_sample.preview_contract_json->>'selection_allowed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
              OR UPPER(BTRIM(COALESCE(malformed_sample.selection_state, ''))) <> 'SELECTED'
              OR UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'presentation_section', ''))) <> 'READY_TO_PAY'
              OR (
                LOWER(BTRIM(COALESCE(malformed_sample.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                AND (
                  UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'selection_state', malformed_sample.selection_state, ''))) <> 'SELECTED'
                  OR UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
                )
              )
              OR (
                UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'projection_path', ''))) = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
                AND (
                  LOWER(BTRIM(COALESCE(malformed_sample.row_json->>'projection_certified', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
                  OR UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'policy_x_authority_scope', ''))) <> 'PRE_DRAFT_LIVE_TRUTH'
                  OR malformed_sample.economic_key_json IS NULL
                  OR malformed_sample.economic_key_json = '{}'::jsonb
                  OR NULLIF(BTRIM(COALESCE(malformed_sample.economic_key_json->>'key_type', '')), '') IS NULL
                  OR NULLIF(BTRIM(COALESCE(malformed_sample.economic_key_json->>'key_value', '')), '') IS NULL
                  OR NULLIF(BTRIM(COALESCE(malformed_sample.economic_key_json->>'timesheet_id', '')), '') IS NULL
                )
              )
         )
  INTO v_malformed_selected_exists;

  IF COALESCE(v_malformed_selected_exists, false) THEN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'preview_row_id', malformed_rows.id::text,
             'row_key', malformed_rows.row_key,
             'candidate_id', malformed_rows.candidate_id::text,
             'reasons', COALESCE(malformed_rows.preview_contract_json->'reasons', '[]'::jsonb)
           ) ORDER BY malformed_rows.row_ordinal, malformed_rows.id), '[]'::jsonb)
    INTO v_malformed_selected_sample
    FROM (
      SELECT malformed_sample.*
      FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS malformed_sample
      WHERE LOWER(BTRIM(COALESCE(malformed_sample.preview_contract_json->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
         OR LOWER(BTRIM(COALESCE(malformed_sample.preview_contract_json->>'selection_allowed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
         OR UPPER(BTRIM(COALESCE(malformed_sample.selection_state, ''))) <> 'SELECTED'
         OR UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'presentation_section', ''))) <> 'READY_TO_PAY'
         OR (
           LOWER(BTRIM(COALESCE(malformed_sample.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND (
             UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'selection_state', malformed_sample.selection_state, ''))) <> 'SELECTED'
             OR UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
           )
         )
         OR (
           UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'projection_path', ''))) = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
           AND (
             LOWER(BTRIM(COALESCE(malformed_sample.row_json->>'projection_certified', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
             OR UPPER(BTRIM(COALESCE(malformed_sample.row_json->>'policy_x_authority_scope', ''))) <> 'PRE_DRAFT_LIVE_TRUTH'
             OR malformed_sample.economic_key_json IS NULL
             OR malformed_sample.economic_key_json = '{}'::jsonb
             OR NULLIF(BTRIM(COALESCE(malformed_sample.economic_key_json->>'key_type', '')), '') IS NULL
             OR NULLIF(BTRIM(COALESCE(malformed_sample.economic_key_json->>'key_value', '')), '') IS NULL
             OR NULLIF(BTRIM(COALESCE(malformed_sample.economic_key_json->>'timesheet_id', '')), '') IS NULL
           )
         )
      ORDER BY malformed_sample.row_ordinal, malformed_sample.id
      LIMIT 25
    ) AS malformed_rows;

    RAISE EXCEPTION 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE',
              'session_id', p_session_id::text,
              'session_version', v_session_row.version,
              'operation_id', p_operation_id::text,
              'malformed_selected_preview_rows', COALESCE(v_malformed_selected_sample, '[]'::jsonb),
              'message', 'Selected preview rows are not valid draftable Ready to Pay rows. Refresh the preview and try again.'
            )::text;
  END IF;

  SELECT COUNT(*)::integer,
         COUNT(DISTINCT selected_sample.candidate_id)::integer,
         COUNT(*) FILTER (WHERE selected_sample.pay_channel = 'PAYE')::integer,
         COUNT(*) FILTER (WHERE selected_sample.pay_channel = 'UMBRELLA')::integer,
         EXISTS (
           SELECT 1
           FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS ready_sample
           WHERE ready_sample.pay_channel IN ('PAYE', 'UMBRELLA')
             AND COALESCE(ready_sample.selected, false) = true
             AND UPPER(BTRIM(COALESCE(ready_sample.selection_state, ''))) = 'SELECTED'
             AND UPPER(BTRIM(COALESCE(ready_sample.status, ''))) = 'READY'
             AND LOWER(BTRIM(COALESCE(ready_sample.preview_contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
             AND LOWER(BTRIM(COALESCE(ready_sample.preview_contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
             AND UPPER(BTRIM(COALESCE(ready_sample.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
         ),
         EXISTS (
           SELECT 1
           FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS blocked_sample
           WHERE UPPER(BTRIM(COALESCE(blocked_sample.status, ''))) IN ('DIRTY', 'ERROR', 'FAILED')
              OR UPPER(BTRIM(COALESCE(blocked_sample.selection_state, ''))) <> 'SELECTED'
              OR (
                LOWER(BTRIM(COALESCE(blocked_sample.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                AND (
                  UPPER(BTRIM(COALESCE(blocked_sample.row_json->>'selection_state', blocked_sample.selection_state, ''))) <> 'SELECTED'
                  OR UPPER(BTRIM(COALESCE(blocked_sample.row_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
                )
              )
              OR LOWER(BTRIM(COALESCE(blocked_sample.preview_contract_json->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
              OR LOWER(BTRIM(COALESCE(blocked_sample.preview_contract_json->>'selection_allowed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
              OR UPPER(BTRIM(COALESCE(blocked_sample.row_json->>'presentation_section', ''))) <> 'READY_TO_PAY'
         )
  INTO v_sample_row_count, v_sample_candidate_count, v_sample_paye_count, v_sample_umbrella_count, v_ready_selected_exists, v_unready_selected_exists
  FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS selected_sample;

  v_unready_selected_exists := COALESCE(v_unready_selected_exists, false) OR COALESCE(v_current_unready_selected_exists, false);

  IF COALESCE(v_sample_row_count, 0) = 0 THEN
    RAISE EXCEPTION 'No selected preview rows are available for session % and scope %', p_session_id, v_scope_filter;
  END IF;

  IF v_ready_selected_exists IS NOT TRUE THEN
    RAISE EXCEPTION 'No selected preview rows are READY and draftable for session % and scope %', p_session_id, v_scope_filter;
  END IF;

  IF v_unready_selected_exists THEN
    RAISE EXCEPTION 'Selected preview rows include dirty, failed, excluded, or non-selectable rows for session %', p_session_id;
  END IF;

  IF v_scope_filter IN ('ALL', 'PAYE') AND COALESCE(v_selected_ready_paye_row_count, 0) > 0 THEN
    PERFORM pg_advisory_xact_lock(94201, 1);

    v_paye_guardrails := public.pay_paye_guardrails(
      p_pay_date => v_session_row.pay_date,
      p_ignore_pay_batch_id => NULL::uuid,
      p_actor_user_id => p_actor_user_id
    );

    v_paye_create_blocked := COALESCE((v_paye_guardrails->>'create_paye_blocked')::boolean, false);
    v_paye_override_required := COALESCE((v_paye_guardrails->>'override_required')::boolean, false);

    IF v_paye_create_blocked THEN
      v_paye_scope_blocked := true;
      v_paye_block_reason_code := 'PAYE_DRAFT_ALREADY_EXISTS';
      v_paye_block_message := COALESCE(v_paye_guardrails #>> '{ui,existing_draft_block,message}', 'A PAYE draft batch already exists. Cancel or delete the existing PAYE draft before creating another PAYE draft.');
    ELSIF v_paye_override_required THEN
      IF NULLIF(BTRIM(COALESCE(p_override_reason, '')), '') IS NULL THEN
        v_paye_scope_blocked := true;
        v_paye_block_reason_code := 'SAME_WEEK_OVERRIDE_REASON_REQUIRED';
        v_paye_block_message := 'A same-week PAYE override reason is required before creating another PAYE batch in the same Monday-based payroll week.';
      ELSIF COALESCE(p_override_continue, false) = false THEN
        v_paye_scope_blocked := true;
        v_paye_block_reason_code := 'SAME_WEEK_OVERRIDE_CONTINUE_REQUIRED';
        v_paye_block_message := 'Explicit continuation is required before creating another PAYE batch in the same Monday-based payroll week.';
      ELSIF COALESCE(p_override_verified, false) = false OR p_override_verified_by_user_id IS NULL OR p_override_verified_by_user_id <> p_actor_user_id OR p_override_verified_at_utc IS NULL THEN
        v_paye_scope_blocked := true;
        v_paye_block_reason_code := 'SAME_WEEK_OVERRIDE_VERIFICATION_REQUIRED';
        v_paye_block_message := 'Valid password reauthentication and 2FA verification are required before creating another PAYE batch in the same Monday-based payroll week.';
      END IF;
    END IF;
  END IF;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', sample_rows.id::text,
           'row_key', sample_rows.row_key,
           'candidate_id', sample_rows.candidate_id::text,
           'pay_channel', sample_rows.pay_channel,
           'row_ordinal', sample_rows.row_ordinal
         ) ORDER BY sample_rows.row_ordinal, sample_rows.id), '[]'::jsonb)
  INTO v_selected_sample
  FROM (
    SELECT selected_sample.*
    FROM pg_temp.tmp_pay_workbench_prepare_draft_selected_sample AS selected_sample
    ORDER BY selected_sample.row_ordinal, selected_sample.id
    LIMIT 25
  ) AS sample_rows;

  UPDATE public.banking_pay_operations AS operation_update
  SET progress_json = jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'draft_prepare_validated_at_utc', v_now::text,
          'draft_prepare_scope', v_scope_filter,
          'draft_prepare_sample_row_count', COALESCE(v_sample_row_count, 0),
          'draft_prepare_sample_candidate_count', COALESCE(v_sample_candidate_count, 0),
          'draft_prepare_selected_ready_row_count', COALESCE(v_selected_ready_row_count, 0),
          'draft_prepare_selected_ready_paye_row_count', COALESCE(v_selected_ready_paye_row_count, 0),
          'draft_prepare_selected_ready_umbrella_row_count', COALESCE(v_selected_ready_umbrella_row_count, 0),
          'draft_prepare_scope_selected_ready_row_count', COALESCE(v_scope_selected_ready_row_count, 0),
          'draft_prepare_input_selection_count', COALESCE(v_input_count, 0),
          'draft_prepare_selection_resolved_to_current', COALESCE(v_selection_resolved_to_current, false)
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_mode', true,
    'operation_id', p_operation_id::text,
    'session_id', p_session_id::text,
    'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
    'session_version', v_session_row.version,
    'pay_date', CASE WHEN v_session_row.pay_date IS NULL THEN NULL ELSE v_session_row.pay_date::text END,
    'week_ending_cutoff', CASE WHEN v_session_row.week_ending_cutoff IS NULL THEN NULL ELSE v_session_row.week_ending_cutoff::text END,
    'pay_channel_scope', v_scope_filter,
    'selected_preview_row_count_known', true,
    'selected_preview_row_sample_count', COALESCE(v_sample_row_count, 0),
    'selected_candidate_sample_count', COALESCE(v_sample_candidate_count, 0),
    'paye_selected_row_sample_count', COALESCE(v_sample_paye_count, 0),
    'umbrella_selected_row_sample_count', COALESCE(v_sample_umbrella_count, 0),
    'selected_ready_row_count', COALESCE(v_selected_ready_row_count, 0),
    'selected_ready_paye_row_count', COALESCE(v_selected_ready_paye_row_count, 0),
    'selected_ready_umbrella_row_count', COALESCE(v_selected_ready_umbrella_row_count, 0),
    'scope_selected_ready_row_count', COALESCE(v_scope_selected_ready_row_count, 0),
    'scope_counts', jsonb_build_object(
      'selected_ready_total', COALESCE(v_selected_ready_row_count, 0),
      'selected_ready_paye', COALESCE(v_selected_ready_paye_row_count, 0),
      'selected_ready_umbrella', COALESCE(v_selected_ready_umbrella_row_count, 0),
      'selected_ready_for_scope', COALESCE(v_scope_selected_ready_row_count, 0)
    ),
    'selected_row_sample', COALESCE(v_selected_sample, '[]'::jsonb),
    'input_selected_preview_row_count', COALESCE(v_input_count, 0),
    'selection_resolved_to_current', COALESCE(v_selection_resolved_to_current, false),
    'resolved_current_selected_preview_row_ids', COALESCE(v_resolved_current_selection_ids, '[]'::jsonb),
    'same_week_paye_override', jsonb_build_object(
      'guardrails', COALESCE(v_paye_guardrails, '{}'::jsonb),
      'create_blocked', v_paye_create_blocked,
      'override_required', v_paye_override_required,
      'scope_blocked', v_paye_scope_blocked,
      'block_reason_code', v_paye_block_reason_code,
      'block_message', v_paye_block_message,
      'override_reason_present', (NULLIF(BTRIM(COALESCE(p_override_reason, '')), '') IS NOT NULL),
      'override_continue', COALESCE(p_override_continue, false),
      'override_verified', COALESCE(p_override_verified, false),
      'override_verified_by_user_id', CASE WHEN p_override_verified_by_user_id IS NULL THEN NULL ELSE p_override_verified_by_user_id::text END,
      'override_verified_at_utc', CASE WHEN p_override_verified_at_utc IS NULL THEN NULL ELSE p_override_verified_at_utc::text END
    ),
    'next_phase', CASE WHEN v_paye_scope_blocked THEN 'AWAIT_SAME_WEEK_PAYE_OVERRIDE' ELSE 'SEED_CANDIDATE_SCOPE' END,
    'requires_row_backed_scope_seed', (v_paye_scope_blocked IS NOT TRUE),
    'message', CASE WHEN v_paye_scope_blocked THEN v_paye_block_message ELSE 'Workbench draft validation passed. Continue with operation-scoped draft creation.' END
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_prepare_draft(uuid, uuid, jsonb, text, text, boolean, boolean, uuid, timestamp with time zone, uuid, boolean, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_workbench_prepare_draft(uuid, uuid, jsonb, text, text, boolean, boolean, uuid, timestamp with time zone, uuid, boolean, boolean) FROM anon;
REVOKE ALL ON FUNCTION public.pay_workbench_prepare_draft(uuid, uuid, jsonb, text, text, boolean, boolean, uuid, timestamp with time zone, uuid, boolean, boolean) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_prepare_draft(uuid, uuid, jsonb, text, text, boolean, boolean, uuid, timestamp with time zone, uuid, boolean, boolean) TO service_role;
