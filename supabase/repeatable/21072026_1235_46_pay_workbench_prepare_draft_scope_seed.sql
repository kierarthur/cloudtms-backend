-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 97708b68e486.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_scope_seed(p_operation_id uuid, p_workbench_session_id uuid, p_actor_user_id uuid, p_selected_preview_row_ids jsonb DEFAULT NULL::jsonb, p_pay_channel_scope text DEFAULT NULL::text, p_same_week_paye_override_json jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(candidate_scope_count integer, selected_row_count integer, timesheet_count integer, finance_case_count integer, pay_channel_count integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope_filter text := UPPER(COALESCE(NULLIF(BTRIM(p_pay_channel_scope), ''), 'ALL'));
  v_selected_input_count integer := 0;
  v_candidate_scope_count integer := 0;
  v_selected_count integer := 0;
  v_timesheet_count integer := 0;
  v_finance_case_count integer := 0;
  v_pay_channel_count integer := 0;
  v_stale_selection_ids jsonb := '[]'::jsonb;
  v_missing_selection_ids jsonb := '[]'::jsonb;
  v_malformed_selected_preview_rows jsonb := '[]'::jsonb;
  v_synthetic_total_selected_preview_rows jsonb := '[]'::jsonb;
  v_selection_resolved_to_current boolean := false;
  v_resolved_current_selection_ids jsonb := '[]'::jsonb;
  v_resolved_selection_match_count integer := 0;
  v_resolved_selection_distinct_count integer := 0;
  v_active_workbench_job_count integer := 0;
  v_active_workbench_job_sample jsonb := '[]'::jsonb;
  v_stale_candidate_authority_count integer := 0;
  v_stale_candidate_authority_sample jsonb := '[]'::jsonb;
BEGIN
  perform public._ctms_assert_session_correction_residuals_draftable_v1(p_workbench_session_id,p_selected_preview_row_ids,'PAY_WORKBENCH_PREPARE_DRAFT');
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed: p_operation_id is required';
  END IF;

  IF p_workbench_session_id IS NULL THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed: p_workbench_session_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed: p_actor_user_id is required';
  END IF;

  IF v_scope_filter NOT IN ('ALL', 'PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed unsupported pay channel scope: %', v_scope_filter;
  END IF;

  IF jsonb_typeof(COALESCE(p_same_week_paye_override_json, '{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed requires p_same_week_paye_override_json to be a JSON object';
  END IF;

  IF p_selected_preview_row_ids IS NOT NULL THEN
    IF jsonb_typeof(p_selected_preview_row_ids) <> 'array' THEN
      RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed requires p_selected_preview_row_ids to be null or a JSON array';
    END IF;

    v_selected_input_count := jsonb_array_length(p_selected_preview_row_ids);

    IF v_selected_input_count > 100 THEN
      RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed selected preview row input exceeds the 100 row cap: %', v_selected_input_count;
    END IF;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed operation not found: %', p_operation_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE' THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed expected DRAFT_CREATE operation %, got %', p_operation_id, v_operation.operation_type;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed cannot seed terminal operation % with status %', p_operation_id, v_operation.status;
  END IF;

  IF v_operation.workbench_session_id IS NOT NULL AND v_operation.workbench_session_id <> p_workbench_session_id THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed operation % belongs to a different workbench session', p_operation_id;
  END IF;

  IF v_operation.actor_user_id IS NOT NULL AND v_operation.actor_user_id <> p_actor_user_id THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed operation % belongs to a different actor', p_operation_id;
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_workbench_session_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed workbench session not found: %', p_workbench_session_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session.status, ''))) <> 'OPEN' OR v_session.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_scope_seed workbench session % is not OPEN', p_workbench_session_id;
  END IF;

  IF v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'WORKBENCH_SESSION_REPLACED',
      'message', 'This payment workbench has been replaced. Refresh the Banking Pay workbench and create the draft from the current session.',
      'session_id', p_workbench_session_id::text,
      'replacement_session_id', v_session.replacement_session_id::text
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
    WHERE job_row.session_id = p_workbench_session_id
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
      'session_id', p_workbench_session_id::text,
      'active_workbench_job_count', COALESCE(v_active_workbench_job_count, 0),
      'active_workbench_jobs', COALESCE(v_active_workbench_job_sample, '[]'::jsonb)
    )::text;
  END IF;

  UPDATE public.banking_pay_operations AS operation_update
  SET workbench_session_id = COALESCE(operation_update.workbench_session_id, p_workbench_session_id),
      actor_user_id = COALESCE(operation_update.actor_user_id, p_actor_user_id),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_draft_scope_supplied_ids;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_draft_scope_supplied_ids ON COMMIT DROP AS
  SELECT DISTINCT BTRIM(supplied_id.value) AS supplied_id
  FROM jsonb_array_elements_text(COALESCE(p_selected_preview_row_ids, '[]'::jsonb)) AS supplied_id(value)
  WHERE p_selected_preview_row_ids IS NOT NULL
    AND BTRIM(supplied_id.value) <> '';

  IF p_selected_preview_row_ids IS NOT NULL AND COALESCE(v_selected_input_count, 0) > 0 THEN
    DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_draft_scope_resolved_ids;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_draft_scope_resolved_ids ON COMMIT DROP AS
    WITH supplied_ids AS (
      SELECT supplied_id.supplied_id
      FROM pg_temp.tmp_pay_workbench_draft_scope_supplied_ids AS supplied_id
    ),
    direct_current_matches AS (
      SELECT DISTINCT
        supplied_ids.supplied_id,
        current_preview_row.id::text AS resolved_id
      FROM supplied_ids
      JOIN public.banking_pay_workbench_preview_rows AS current_preview_row
        ON current_preview_row.session_id = p_workbench_session_id
       AND current_preview_row.session_version = v_session.version
       AND UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
       AND COALESCE(current_preview_row.selected, false) = true
       AND UPPER(BTRIM(COALESCE(current_preview_row.selection_state, ''))) = 'SELECTED'
       AND UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND (v_scope_filter = 'ALL' OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = v_scope_filter)
       AND (
         current_preview_row.id::text = supplied_ids.supplied_id
         OR current_preview_row.row_key = supplied_ids.supplied_id
         OR current_preview_row.row_json->>'preview_row_id' = supplied_ids.supplied_id
         OR current_preview_row.row_json->>'row_id' = supplied_ids.supplied_id
         OR current_preview_row.row_json->>'line_id' = supplied_ids.supplied_id
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
          WHEN NULLIF(BTRIM(COALESCE(contract_entry.value#>>'{economic_key,timesheet_id}', contract_entry.value->>'timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN NULLIF(BTRIM(COALESCE(contract_entry.value#>>'{economic_key,timesheet_id}', contract_entry.value->>'timesheet_id', '')), '')::uuid
          ELSE NULL::uuid
        END AS timesheet_id,
        NULLIF(BTRIM(COALESCE(contract_entry.value#>>'{economic_key,key_type}', contract_entry.value->>'key_type', '')), '') AS key_type,
        NULLIF(BTRIM(COALESCE(contract_entry.value#>>'{economic_key,key_value}', contract_entry.value->>'key_value', '')), '') AS key_value,
        UPPER(NULLIF(BTRIM(COALESCE(contract_entry.value->>'pay_channel', '')), '')) AS pay_channel
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(v_operation.input_json->'draft_selected_preview_row_contracts', '[]'::jsonb)) = 'array'
            THEN COALESCE(v_operation.input_json->'draft_selected_preview_row_contracts', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS contract_entry(value, ordinality)
      WHERE jsonb_typeof(contract_entry.value) = 'object'
    ),
    operation_contract_timesheet_ids AS (
      SELECT COALESCE(
        array_agg(DISTINCT operation_contracts.timesheet_id ORDER BY operation_contracts.timesheet_id),
        array[]::uuid[]
      ) AS timesheet_ids
      FROM operation_contracts
      WHERE operation_contracts.timesheet_id IS NOT NULL
    ),
    operation_contract_rotation AS (
      SELECT DISTINCT ON (rotation_scope.requested_timesheet_id)
        rotation_scope.requested_timesheet_id,
        COALESCE(rotation_scope.canonical_timesheet_id, rotation_scope.requested_timesheet_id) AS canonical_timesheet_id
      FROM operation_contract_timesheet_ids
      JOIN public._pay_timesheet_rotation_scope(operation_contract_timesheet_ids.timesheet_ids) AS rotation_scope
        ON true
      ORDER BY
        rotation_scope.requested_timesheet_id,
        rotation_scope.family_is_current DESC NULLS LAST,
        rotation_scope.family_version DESC NULLS LAST,
        rotation_scope.family_timesheet_id
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
      LEFT JOIN operation_contract_rotation
        ON operation_contract_rotation.requested_timesheet_id = operation_contract.timesheet_id
      JOIN public.banking_pay_workbench_preview_rows AS current_preview_row
        ON current_preview_row.session_id = p_workbench_session_id
       AND current_preview_row.session_version = v_session.version
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
       AND current_preview_row.timesheet_id IS NOT DISTINCT FROM COALESCE(operation_contract_rotation.canonical_timesheet_id, operation_contract.timesheet_id)
       AND COALESCE(NULLIF(BTRIM(current_preview_row.section), ''), 'canonical_preview_lines') = COALESCE(NULLIF(BTRIM(operation_contract.section), ''), 'canonical_preview_lines')
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_type, current_preview_row.row_json#>>'{economic_key,key_type}', current_preview_row.row_json->>'component_key_type', '')), '') IS NOT DISTINCT FROM operation_contract.key_type
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_value, current_preview_row.row_json#>>'{economic_key,key_value}', current_preview_row.row_json->>'component_key_value', '')), '') IS NOT DISTINCT FROM operation_contract.key_value
       AND NULLIF(BTRIM(current_preview_row.row_key), '') IS NOT DISTINCT FROM NULLIF(BTRIM(operation_contract.row_key), '')
       AND (v_scope_filter = 'ALL' OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = v_scope_filter)
       AND (
         operation_contract.pay_channel IS NULL
         OR operation_contract.pay_channel = ''
         OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = operation_contract.pay_channel
       )
    ),
    historical_preview_inputs AS (
      SELECT DISTINCT
        supplied_ids.supplied_id,
        any_preview_row.candidate_id,
        COALESCE(NULLIF(BTRIM(any_preview_row.section), ''), 'canonical_preview_lines') AS section,
        any_preview_row.row_key,
        any_preview_row.timesheet_id,
        NULLIF(BTRIM(COALESCE(any_preview_row.key_type, any_preview_row.row_json#>>'{economic_key,key_type}', any_preview_row.row_json->>'component_key_type', '')), '') AS key_type,
        NULLIF(BTRIM(COALESCE(any_preview_row.key_value, any_preview_row.row_json#>>'{economic_key,key_value}', any_preview_row.row_json->>'component_key_value', '')), '') AS key_value,
        UPPER(NULLIF(BTRIM(COALESCE(any_preview_row.row_json->>'pay_channel', any_preview_row.row_json->>'current_pay_method', any_preview_row.row_json->>'candidate_pay_method', '')), '')) AS pay_channel
      FROM supplied_ids
      JOIN public.banking_pay_workbench_preview_rows AS any_preview_row
        ON any_preview_row.session_id = p_workbench_session_id
       AND (
         any_preview_row.id::text = supplied_ids.supplied_id
         OR any_preview_row.row_key = supplied_ids.supplied_id
         OR any_preview_row.row_json->>'preview_row_id' = supplied_ids.supplied_id
         OR any_preview_row.row_json->>'row_id' = supplied_ids.supplied_id
         OR any_preview_row.row_json->>'line_id' = supplied_ids.supplied_id
       )
      WHERE any_preview_row.timesheet_id IS NOT NULL
        AND any_preview_row.row_key IS NOT NULL
    ),
    historical_timesheet_ids AS (
      SELECT COALESCE(
        array_agg(DISTINCT historical_preview_inputs.timesheet_id ORDER BY historical_preview_inputs.timesheet_id),
        array[]::uuid[]
      ) AS timesheet_ids
      FROM historical_preview_inputs
    ),
    historical_rotation AS (
      SELECT DISTINCT ON (rotation_scope.requested_timesheet_id)
        rotation_scope.requested_timesheet_id,
        COALESCE(rotation_scope.canonical_timesheet_id, rotation_scope.requested_timesheet_id) AS canonical_timesheet_id
      FROM historical_timesheet_ids
      JOIN public._pay_timesheet_rotation_scope(historical_timesheet_ids.timesheet_ids) AS rotation_scope
        ON true
      ORDER BY
        rotation_scope.requested_timesheet_id,
        rotation_scope.family_is_current DESC NULLS LAST,
        rotation_scope.family_version DESC NULLS LAST,
        rotation_scope.family_timesheet_id
    ),
    historical_preview_current_matches AS (
      SELECT DISTINCT
        historical_preview_inputs.supplied_id,
        current_preview_row.id::text AS resolved_id
      FROM historical_preview_inputs
      JOIN historical_rotation
        ON historical_rotation.requested_timesheet_id = historical_preview_inputs.timesheet_id
      JOIN public.banking_pay_workbench_preview_rows AS current_preview_row
        ON current_preview_row.session_id = p_workbench_session_id
       AND current_preview_row.session_version = v_session.version
       AND UPPER(BTRIM(COALESCE(current_preview_row.status, ''))) = 'READY'
       AND COALESCE(current_preview_row.selected, false) = true
       AND UPPER(BTRIM(COALESCE(current_preview_row.selection_state, ''))) = 'SELECTED'
       AND UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'presentation_section', ''))) = 'READY_TO_PAY'
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND LOWER(BTRIM(COALESCE(current_preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       AND current_preview_row.candidate_id IS NOT DISTINCT FROM historical_preview_inputs.candidate_id
       AND current_preview_row.timesheet_id IS NOT DISTINCT FROM historical_rotation.canonical_timesheet_id
       AND COALESCE(NULLIF(BTRIM(current_preview_row.section), ''), 'canonical_preview_lines') = historical_preview_inputs.section
       AND NULLIF(BTRIM(current_preview_row.row_key), '') IS NOT DISTINCT FROM NULLIF(BTRIM(historical_preview_inputs.row_key), '')
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_type, current_preview_row.row_json#>>'{economic_key,key_type}', current_preview_row.row_json->>'component_key_type', '')), '') IS NOT DISTINCT FROM historical_preview_inputs.key_type
       AND NULLIF(BTRIM(COALESCE(current_preview_row.key_value, current_preview_row.row_json#>>'{economic_key,key_value}', current_preview_row.row_json->>'component_key_value', '')), '') IS NOT DISTINCT FROM historical_preview_inputs.key_value
       AND (v_scope_filter = 'ALL' OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = v_scope_filter)
       AND (
         historical_preview_inputs.pay_channel IS NULL
         OR historical_preview_inputs.pay_channel = ''
         OR UPPER(BTRIM(COALESCE(current_preview_row.row_json->>'pay_channel', current_preview_row.row_json->>'current_pay_method', current_preview_row.row_json->>'candidate_pay_method', ''))) = historical_preview_inputs.pay_channel
       )
    )
    SELECT
      supplied_ids.supplied_id,
      COALESCE(direct_current_matches.resolved_id, operation_contract_current_matches.resolved_id, historical_preview_current_matches.resolved_id) AS resolved_id
    FROM supplied_ids
    LEFT JOIN direct_current_matches
      ON direct_current_matches.supplied_id = supplied_ids.supplied_id
    LEFT JOIN operation_contract_current_matches
      ON operation_contract_current_matches.supplied_id = supplied_ids.supplied_id
    LEFT JOIN historical_preview_current_matches
      ON historical_preview_current_matches.supplied_id = supplied_ids.supplied_id;

    SELECT COUNT(*)::integer,
           COUNT(DISTINCT resolved_selection.resolved_id)::integer,
           COALESCE(jsonb_agg(to_jsonb(resolved_selection.resolved_id) ORDER BY resolved_selection.resolved_id), '[]'::jsonb)
    INTO v_resolved_selection_match_count, v_resolved_selection_distinct_count, v_resolved_current_selection_ids
    FROM pg_temp.tmp_pay_workbench_draft_scope_resolved_ids AS resolved_selection
    WHERE NULLIF(BTRIM(COALESCE(resolved_selection.resolved_id, '')), '') IS NOT NULL;

    IF COALESCE(v_resolved_selection_match_count, 0) = COALESCE(v_selected_input_count, 0)
       AND COALESCE(v_resolved_selection_distinct_count, 0) = COALESCE(v_selected_input_count, 0) THEN
      SELECT EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_workbench_draft_scope_resolved_ids AS resolved_selection
        WHERE resolved_selection.resolved_id IS DISTINCT FROM resolved_selection.supplied_id
      )
      INTO v_selection_resolved_to_current;

      TRUNCATE TABLE pg_temp.tmp_pay_workbench_draft_scope_supplied_ids;

      INSERT INTO pg_temp.tmp_pay_workbench_draft_scope_supplied_ids (supplied_id)
      SELECT resolved_selection.resolved_id
      FROM pg_temp.tmp_pay_workbench_draft_scope_resolved_ids AS resolved_selection
      WHERE NULLIF(BTRIM(COALESCE(resolved_selection.resolved_id, '')), '') IS NOT NULL
      ORDER BY resolved_selection.resolved_id;
    END IF;
  END IF;

  IF p_selected_preview_row_ids IS NOT NULL THEN
    SELECT COALESCE(jsonb_agg(to_jsonb(supplied_ids.supplied_id) ORDER BY supplied_ids.supplied_id), '[]'::jsonb)
    INTO v_stale_selection_ids
    FROM pg_temp.tmp_pay_workbench_draft_scope_supplied_ids AS supplied_ids
    WHERE EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS any_preview_row
      WHERE any_preview_row.session_id = p_workbench_session_id
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
        WHERE current_preview_row.session_id = p_workbench_session_id
          AND current_preview_row.session_version = v_session.version
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
    FROM pg_temp.tmp_pay_workbench_draft_scope_supplied_ids AS supplied_ids
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS any_preview_row
      WHERE any_preview_row.session_id = p_workbench_session_id
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
                'session_id', p_workbench_session_id::text,
                'session_version', v_session.version,
                'operation_id', p_operation_id::text,
                'rejected_preview_row_ids', COALESCE(v_stale_selection_ids, '[]'::jsonb) || COALESCE(v_missing_selection_ids, '[]'::jsonb),
                'stale_preview_row_ids', COALESCE(v_stale_selection_ids, '[]'::jsonb),
                'missing_preview_row_ids', COALESCE(v_missing_selection_ids, '[]'::jsonb),
                'message', 'Selected preview rows are not part of the current ready workbench session version. Refresh the preview page and try again.'
              )::text;
    END IF;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_draft_scope_selected_candidates;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_draft_scope_selected_candidates ON COMMIT DROP AS
  WITH selected_base_raw AS (
    SELECT
      preview_row.id AS preview_row_id,
      preview_row.row_key,
      preview_row.session_id,
      preview_row.candidate_id,
      COALESCE(NULLIF(BTRIM(preview_row.section), ''), 'canonical_preview_lines') AS section,
      preview_row.selection_state,
      preview_row.status AS table_status,
      COALESCE(preview_row.selected, false) AS table_selected,
      preview_row.row_ordinal,
      preview_row.row_json,
      preview_row.timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(preview_row.key_type, preview_row.row_json#>>'{economic_key,key_type}', preview_row.row_json->>'component_key_type', '')), '')) AS key_type,
      NULLIF(BTRIM(COALESCE(preview_row.key_value, preview_row.row_json#>>'{economic_key,key_value}', preview_row.row_json->>'component_key_value', '')), '') AS key_value,
      UPPER(NULLIF(BTRIM(COALESCE(preview_row.row_json->>'pay_channel', preview_row.row_json->>'current_pay_method', preview_row.row_json->>'candidate_pay_method', '')), '')) AS pay_channel,
      UPPER(NULLIF(BTRIM(COALESCE(preview_row.row_json->>'line_type', preview_row.row_json->>'item_type', preview_row.row_json->>'case_type', '')), '')) AS line_type,
      UPPER(NULLIF(BTRIM(COALESCE(preview_row.row_json->>'case_type', '')), '')) AS case_type,
      UPPER(NULLIF(BTRIM(COALESCE(preview_row.row_json->>'item_direction', preview_row.row_json->>'direction', '')), '')) AS item_direction,
      CASE WHEN NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_case_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_case_id', '')), '')::uuid ELSE NULL::uuid END AS finance_case_id,
      CASE WHEN NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_component_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NULLIF(BTRIM(COALESCE(preview_row.row_json->>'finance_component_id', '')), '')::uuid ELSE NULL::uuid END AS finance_component_id,
      CASE WHEN COALESCE(preview_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ROUND((preview_row.row_json->>'amount_ex_vat')::numeric, 2) ELSE NULL::numeric END AS amount_ex_vat,
      (
        COALESCE(preview_row.row_json->>'projection_path', '') = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        OR preview_row.row_json ? 'projection_run_id'
      ) AS is_delta_projection,
      LOWER(BTRIM(COALESCE(preview_row.row_json->>'projection_certified', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS projection_certified,
      COALESCE(
        NULLIF(BTRIM(preview_row.row_json->>'policy_x_authority_scope'), ''),
        NULLIF(BTRIM(preview_row.row_json#>>'{contract_json,policy_x_authority_scope}'), ''),
        NULLIF(BTRIM(preview_row.row_json#>>'{contract,policy_x_authority_scope}'), ''),
        ''
      ) AS policy_x_authority_scope,
      (
        LOWER(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          UPPER(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          OR LOWER(BTRIM(COALESCE(preview_row.row_json->>'selected', 'true'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
          OR UPPER(BTRIM(COALESCE(preview_row.row_json->>'selection_state', preview_row.selection_state, ''))) <> 'SELECTED'
          OR UPPER(BTRIM(COALESCE(preview_row.row_json->>'status', preview_row.status, ''))) <> 'READY'
        )
      ) AS post_draft_overlay_unavailable,
      (
        (preview_row.row_json ? 'selected' AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'selected', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') IS DISTINCT FROM COALESCE(preview_row.selected, false))
        OR (preview_row.row_json ? 'selection_state' AND UPPER(BTRIM(COALESCE(preview_row.row_json->>'selection_state', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))))
        OR (preview_row.row_json ? 'status' AND UPPER(BTRIM(COALESCE(preview_row.row_json->>'status', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(preview_row.status, ''))))
      ) AS table_state_conflicts_row_json
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_workbench_session_id
      AND preview_row.session_version = v_session.version
      AND COALESCE(preview_row.selected, false) = true
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
      AND (v_scope_filter = 'ALL' OR UPPER(BTRIM(COALESCE(preview_row.row_json->>'pay_channel', preview_row.row_json->>'current_pay_method', preview_row.row_json->>'candidate_pay_method', ''))) = v_scope_filter)
      AND (
        p_selected_preview_row_ids IS NULL
        OR EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_workbench_draft_scope_supplied_ids AS supplied_id
          WHERE supplied_id.supplied_id IN (preview_row.id::text, preview_row.row_key, preview_row.row_json->>'preview_row_id', preview_row.row_json->>'row_id', preview_row.row_json->>'line_id')
        )
      )
      AND (
        p_selected_preview_row_ids IS NOT NULL
        OR NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_candidate_scope AS existing_scope
          WHERE existing_scope.operation_id = p_operation_id
            AND existing_scope.workbench_session_id = p_workbench_session_id
            AND existing_scope.candidate_id = preview_row.candidate_id
            AND existing_scope.pay_channel = UPPER(BTRIM(COALESCE(preview_row.row_json->>'pay_channel', preview_row.row_json->>'current_pay_method', preview_row.row_json->>'candidate_pay_method', '')))
        )
      )
    ORDER BY preview_row.row_ordinal, preview_row.id
    LIMIT 100
  ), selected_timesheet_ids AS (
    SELECT COALESCE(
      array_agg(DISTINCT selected_base_raw.timesheet_id ORDER BY selected_base_raw.timesheet_id),
      array[]::uuid[]
    ) AS timesheet_ids
    FROM selected_base_raw
    WHERE selected_base_raw.timesheet_id IS NOT NULL
  ), selected_rotation_scope AS (
    SELECT DISTINCT ON (rotation_scope.requested_timesheet_id)
      rotation_scope.requested_timesheet_id,
      COALESCE(rotation_scope.canonical_timesheet_id, rotation_scope.requested_timesheet_id) AS canonical_timesheet_id,
      (rotation_scope.family_timesheet_id IS NOT NULL AND rotation_scope.family_is_current IS NOT NULL) AS rotation_scope_resolved,
      COALESCE(rotation_scope.requested_is_canonical, false) AS requested_is_canonical
    FROM selected_timesheet_ids
    JOIN public._pay_timesheet_rotation_scope(selected_timesheet_ids.timesheet_ids) AS rotation_scope
      ON true
    ORDER BY
      rotation_scope.requested_timesheet_id,
      rotation_scope.family_is_current DESC NULLS LAST,
      rotation_scope.family_version DESC NULLS LAST,
      rotation_scope.family_timesheet_id
  ), selected_base AS (
    SELECT
      selected_base_raw.preview_row_id,
      selected_base_raw.row_key,
      selected_base_raw.session_id,
      selected_base_raw.candidate_id,
      selected_base_raw.section,
      selected_base_raw.selection_state,
      selected_base_raw.table_status,
      selected_base_raw.table_selected,
      selected_base_raw.is_delta_projection,
      selected_base_raw.projection_certified,
      selected_base_raw.policy_x_authority_scope,
      selected_base_raw.post_draft_overlay_unavailable,
      selected_base_raw.table_state_conflicts_row_json,
      selected_base_raw.row_ordinal,
      jsonb_strip_nulls(
        CASE
          WHEN selected_base_raw.timesheet_id IS NULL OR selected_rotation_scope.canonical_timesheet_id IS NULL THEN selected_base_raw.row_json
          ELSE selected_base_raw.row_json
            || jsonb_build_object(
              'timesheet_id', selected_rotation_scope.canonical_timesheet_id::text,
              'real_business_timesheet_id', selected_rotation_scope.canonical_timesheet_id::text,
              'economic_key', COALESCE(selected_base_raw.row_json->'economic_key', '{}'::jsonb)
                || jsonb_build_object('timesheet_id', selected_rotation_scope.canonical_timesheet_id::text)
            )
            || CASE
              WHEN selected_base_raw.timesheet_id IS DISTINCT FROM selected_rotation_scope.canonical_timesheet_id THEN jsonb_build_object(
                'rotation_requested_timesheet_id', selected_base_raw.timesheet_id::text
              )
              ELSE '{}'::jsonb
            END
        END
      ) AS row_json,
      COALESCE(selected_rotation_scope.canonical_timesheet_id, selected_base_raw.timesheet_id) AS timesheet_id,
      selected_base_raw.key_type,
      selected_base_raw.key_value,
      selected_base_raw.pay_channel,
      selected_base_raw.line_type,
      selected_base_raw.case_type,
      selected_base_raw.item_direction,
      selected_base_raw.finance_case_id,
      selected_base_raw.finance_component_id,
      selected_base_raw.amount_ex_vat,
      CASE
        WHEN selected_base_raw.timesheet_id IS NULL THEN false
        WHEN COALESCE(selected_rotation_scope.rotation_scope_resolved, false) = false THEN true
        WHEN selected_rotation_scope.canonical_timesheet_id IS NULL THEN true
        WHEN COALESCE(selected_rotation_scope.requested_is_canonical, false) = false THEN true
        ELSE false
      END AS rotation_validation_failed,
      CASE
        WHEN selected_base_raw.timesheet_id IS NULL THEN NULL::text
        WHEN COALESCE(selected_rotation_scope.rotation_scope_resolved, false) = false THEN 'SELECTED_PREVIEW_TIMESHEET_ROTATION_SCOPE_UNRESOLVED'
        WHEN selected_rotation_scope.canonical_timesheet_id IS NULL THEN 'SELECTED_PREVIEW_CANONICAL_TIMESHEET_NOT_RESOLVED'
        WHEN COALESCE(selected_rotation_scope.requested_is_canonical, false) = false THEN 'SELECTED_PREVIEW_TIMESHEET_NOT_CANONICAL_REFRESH_REQUIRED'
        ELSE NULL::text
      END AS rotation_failure_reason
    FROM selected_base_raw
    LEFT JOIN selected_rotation_scope
      ON selected_rotation_scope.requested_timesheet_id = selected_base_raw.timesheet_id
  )
  SELECT
    selected_base.*,
    jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', CASE WHEN selected_base.timesheet_id IS NULL THEN NULL ELSE selected_base.timesheet_id::text END,
      'key_type', selected_base.key_type,
      'key_value', selected_base.key_value
    )) AS economic_key_json,
    public.pay_workbench_preview_line_contract_ok(
      p_line_json => jsonb_strip_nulls(
        selected_base.row_json
        || jsonb_build_object(
          'row_key', selected_base.row_key,
          'preview_row_pk', selected_base.preview_row_id::text,
          'selection_state', selected_base.selection_state
        )
      ),
      p_economic_key_json => jsonb_strip_nulls(jsonb_build_object(
        'timesheet_id', CASE WHEN selected_base.timesheet_id IS NULL THEN NULL ELSE selected_base.timesheet_id::text END,
        'key_type', selected_base.key_type,
        'key_value', selected_base.key_value
      )),
      p_target_section => selected_base.section
    ) AS preview_contract_json
  FROM selected_base;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', invalid_rows.preview_row_id::text,
           'row_key', invalid_rows.row_key,
           'candidate_id', invalid_rows.candidate_id::text,
           'reasons', COALESCE(invalid_rows.preview_contract_json->'reasons', '[]'::jsonb)
             || CASE WHEN invalid_rows.rotation_validation_failed IS TRUE THEN jsonb_build_array(invalid_rows.rotation_failure_reason) ELSE '[]'::jsonb END
             || CASE
               WHEN ROUND(COALESCE(invalid_rows.amount_ex_vat, 0), 2) < 0
                AND NOT (
                  invalid_rows.finance_case_id IS NOT NULL
                  AND (invalid_rows.item_direction IS NULL OR invalid_rows.item_direction = 'DEDUCTION')
                  AND invalid_rows.line_type IN ('OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','LOAN_REPAYMENT')
                ) THEN jsonb_build_array('NEGATIVE_ENTITLEMENT_MUST_ROUTE_TO_FINANCE_CASE')
               ELSE '[]'::jsonb
             END
         ) ORDER BY invalid_rows.row_ordinal, invalid_rows.preview_row_id), '[]'::jsonb)
  INTO v_malformed_selected_preview_rows
  FROM (
    SELECT selected_candidate.*
    FROM pg_temp.tmp_pay_workbench_draft_scope_selected_candidates AS selected_candidate
    WHERE selected_candidate.rotation_validation_failed IS TRUE
       OR LOWER(BTRIM(COALESCE(selected_candidate.preview_contract_json->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR LOWER(BTRIM(COALESCE(selected_candidate.preview_contract_json->>'selection_allowed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR UPPER(BTRIM(COALESCE(selected_candidate.section, ''))) <> 'CANONICAL_PREVIEW_LINES'
       OR UPPER(BTRIM(COALESCE(selected_candidate.selection_state, ''))) <> 'SELECTED'
       OR UPPER(BTRIM(COALESCE(selected_candidate.table_status, ''))) <> 'READY'
       OR COALESCE(selected_candidate.table_selected, false) IS NOT TRUE
       OR (COALESCE(selected_candidate.is_delta_projection, false) IS TRUE AND COALESCE(selected_candidate.projection_certified, false) IS NOT TRUE)
       OR COALESCE(selected_candidate.policy_x_authority_scope, '') <> 'PRE_DRAFT_LIVE_TRUTH'
       OR selected_candidate.timesheet_id IS NULL
       OR COALESCE(selected_candidate.post_draft_overlay_unavailable, false) IS TRUE
       OR COALESCE(selected_candidate.table_state_conflicts_row_json, false) IS TRUE
       OR UPPER(BTRIM(COALESCE(selected_candidate.key_type, ''))) NOT IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
       OR selected_candidate.key_value IS NULL
       OR (selected_candidate.key_type = 'TS_DAY' AND selected_candidate.key_value !~ '^\d{4}-\d{2}-\d{2}$')
       OR selected_candidate.amount_ex_vat IS NULL
       OR ROUND(COALESCE(selected_candidate.amount_ex_vat, 0), 2) = 0
       OR (
         ROUND(COALESCE(selected_candidate.amount_ex_vat, 0), 2) < 0
         AND NOT (
           selected_candidate.finance_case_id IS NOT NULL
           AND (selected_candidate.item_direction IS NULL OR selected_candidate.item_direction = 'DEDUCTION')
           AND selected_candidate.line_type IN ('OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','LOAN_REPAYMENT')
         )
       )
    ORDER BY selected_candidate.row_ordinal, selected_candidate.preview_row_id
    LIMIT 25
  ) AS invalid_rows;

  IF jsonb_array_length(COALESCE(v_malformed_selected_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE',
              'operation_id', p_operation_id::text,
              'workbench_session_id', p_workbench_session_id::text,
              'malformed_selected_preview_rows', COALESCE(v_malformed_selected_preview_rows, '[]'::jsonb),
              'message', 'Selected preview rows are not valid draftable Ready to Pay rows. Refresh the preview and try again.'
            )::text;
  END IF;

  /*
   * Final pre-draft defence: selected rows must come from the current adopted
   * candidate authority. This performs only bounded validation and never
   * rebuilds or recalculates economics inside draft creation.
   */
  WITH selected_candidates AS (
    SELECT DISTINCT selected_candidate.candidate_id
    FROM pg_temp.tmp_pay_workbench_draft_scope_selected_candidates
      AS selected_candidate
  ), selected_candidate_authority AS (
    SELECT
      selected_candidates.candidate_id,
      COALESCE(candidate_state.status, 'MISSING') AS candidate_state_status,
      COALESCE(candidate_state.source_change_seq, -1) AS candidate_state_source_change_seq,
      COALESCE(change_counter.seq, 0) AS live_source_change_seq,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_candidate_source_lines AS stale_source
        WHERE stale_source.session_id = p_workbench_session_id
          AND stale_source.candidate_id = selected_candidates.candidate_id
          AND stale_source.session_version = v_session.version
          AND UPPER(BTRIM(COALESCE(stale_source.status, ''))) IN (
            'DIRTY', 'PENDING', 'PROCESSING', 'RUNNING', 'QUEUED'
          )
      ) AS has_incomplete_source,
      EXISTS (
        SELECT 1
        FROM (
          SELECT current_source.line_key, current_source.timesheet_id
          FROM public.banking_pay_workbench_candidate_source_lines AS current_source
          WHERE current_source.session_id = p_workbench_session_id
            AND current_source.candidate_id = selected_candidates.candidate_id
            AND current_source.session_version = v_session.version
            AND UPPER(BTRIM(COALESCE(current_source.status, ''))) = 'CURRENT'
          GROUP BY current_source.line_key, current_source.timesheet_id
          HAVING COUNT(*) > 1
        ) AS duplicate_source
      ) AS has_duplicate_current_source,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_candidate_line_work AS incomplete_line
        WHERE incomplete_line.session_id = p_workbench_session_id
          AND incomplete_line.candidate_id = selected_candidates.candidate_id
          AND UPPER(BTRIM(COALESCE(incomplete_line.status, ''))) IN (
            'DIRTY', 'PENDING', 'PROCESSING', 'RUNNING', 'QUEUED'
          )
      ) AS has_incomplete_line_work,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS dirty_preview
        WHERE dirty_preview.session_id = p_workbench_session_id
          AND dirty_preview.candidate_id = selected_candidates.candidate_id
          AND dirty_preview.session_version = v_session.version
          AND UPPER(BTRIM(COALESCE(dirty_preview.status, ''))) IN (
            'DIRTY', 'PENDING', 'PROCESSING', 'RUNNING', 'QUEUED'
          )
      ) AS has_dirty_preview
    FROM selected_candidates
    LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
      ON candidate_state.session_id = p_workbench_session_id
     AND candidate_state.candidate_id = selected_candidates.candidate_id
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key =
           'pay_candidate:' || selected_candidates.candidate_id::text
  ), invalid_candidates AS (
    SELECT
      selected_candidate_authority.*,
      CASE
        WHEN UPPER(BTRIM(COALESCE(selected_candidate_authority.candidate_state_status, ''))) <> 'READY'
          THEN 'CANDIDATE_STATE_NOT_READY'
        WHEN selected_candidate_authority.candidate_state_source_change_seq
               IS DISTINCT FROM selected_candidate_authority.live_source_change_seq
          THEN 'CANDIDATE_STATE_SEQUENCE_STALE'
        WHEN selected_candidate_authority.has_incomplete_source
          THEN 'SOURCE_FAMILY_INCOMPLETE'
        WHEN selected_candidate_authority.has_duplicate_current_source
          THEN 'SOURCE_FAMILY_DUPLICATED'
        WHEN selected_candidate_authority.has_incomplete_line_work
          THEN 'LINE_WORK_NOT_TERMINAL'
        WHEN selected_candidate_authority.has_dirty_preview
          THEN 'PREVIEW_ROWS_NOT_CURRENT'
        ELSE NULL::text
      END AS stale_reason
    FROM selected_candidate_authority
  )
  SELECT
    COUNT(*)::integer,
    COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'candidate_id', invalid_candidates.candidate_id::text,
          'reason', invalid_candidates.stale_reason,
          'candidate_state_status', invalid_candidates.candidate_state_status,
          'candidate_state_source_change_seq',
            invalid_candidates.candidate_state_source_change_seq,
          'live_source_change_seq',
            invalid_candidates.live_source_change_seq
        )
        ORDER BY invalid_candidates.candidate_id
      ) FILTER (WHERE invalid_candidates.stale_reason IS NOT NULL),
      '[]'::jsonb
    )
  INTO
    v_stale_candidate_authority_count,
    v_stale_candidate_authority_sample
  FROM invalid_candidates
  WHERE invalid_candidates.stale_reason IS NOT NULL;

  IF COALESCE(v_stale_candidate_authority_count, 0) > 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_STALE_CANDIDATE_AUTHORITY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_DRAFT_STALE_CANDIDATE_AUTHORITY',
              'operation_id', p_operation_id::text,
              'workbench_session_id', p_workbench_session_id::text,
              'affected_candidates',
                COALESCE(v_stale_candidate_authority_sample, '[]'::jsonb),
              'message',
                'Banking Pay changed and is refreshing. No draft was created. Review the current rows and try again.'
            )::text;
  END IF;


  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', synthetic_total_rows.preview_row_id::text,
           'row_key', synthetic_total_rows.row_key,
           'candidate_id', synthetic_total_rows.candidate_id::text,
           'timesheet_id', CASE WHEN synthetic_total_rows.timesheet_id IS NULL THEN NULL ELSE synthetic_total_rows.timesheet_id::text END,
           'key_type', synthetic_total_rows.key_type,
           'key_value', synthetic_total_rows.key_value,
           'amount_ex_vat', synthetic_total_rows.amount_ex_vat,
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_SELECTED'
         ) ORDER BY synthetic_total_rows.row_ordinal, synthetic_total_rows.preview_row_id), '[]'::jsonb)
  INTO v_synthetic_total_selected_preview_rows
  FROM pg_temp.tmp_pay_workbench_draft_scope_selected_candidates AS synthetic_total_rows
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
        FROM pg_temp.tmp_pay_workbench_draft_scope_selected_candidates AS sibling_segment
        WHERE sibling_segment.candidate_id = synthetic_total_rows.candidate_id
          AND sibling_segment.timesheet_id IS NOT DISTINCT FROM synthetic_total_rows.timesheet_id
          AND UPPER(BTRIM(COALESCE(sibling_segment.key_type, sibling_segment.row_json#>>'{economic_key,key_type}', ''))) = 'TS_DAY'
          AND LOWER(BTRIM(COALESCE(sibling_segment.row_key, sibling_segment.row_json->>'row_key', sibling_segment.row_json->>'line_key', sibling_segment.row_json->>'source_ref', ''))) LIKE '%:segment:%'
      )
    );

  IF jsonb_array_length(COALESCE(v_synthetic_total_selected_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_NOT_DRAFTABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_NOT_DRAFTABLE',
              'operation_id', p_operation_id::text,
              'workbench_session_id', p_workbench_session_id::text,
              'synthetic_total_selected_preview_rows', COALESCE(v_synthetic_total_selected_preview_rows, '[]'::jsonb),
              'message', 'A selected resolved timesheet row is stale and cannot be drafted. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_draft_scope_selected_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_draft_scope_selected_page ON COMMIT DROP AS
  SELECT selected_candidate.*
  FROM pg_temp.tmp_pay_workbench_draft_scope_selected_candidates AS selected_candidate
  WHERE selected_candidate.rotation_validation_failed IS NOT TRUE
    AND LOWER(BTRIM(COALESCE(selected_candidate.preview_contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND LOWER(BTRIM(COALESCE(selected_candidate.preview_contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(COALESCE(selected_candidate.section, '')) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(selected_candidate.selection_state, ''))) = 'SELECTED'
    AND UPPER(BTRIM(COALESCE(selected_candidate.table_status, ''))) = 'READY'
    AND COALESCE(selected_candidate.table_selected, false) IS TRUE
    AND (COALESCE(selected_candidate.is_delta_projection, false) IS NOT TRUE OR COALESCE(selected_candidate.projection_certified, false) IS TRUE)
    AND COALESCE(selected_candidate.policy_x_authority_scope, '') = 'PRE_DRAFT_LIVE_TRUTH'
    AND selected_candidate.timesheet_id IS NOT NULL
    AND COALESCE(selected_candidate.post_draft_overlay_unavailable, false) IS NOT TRUE
    AND COALESCE(selected_candidate.table_state_conflicts_row_json, false) IS NOT TRUE
    AND selected_candidate.pay_channel IN ('PAYE', 'UMBRELLA')
    AND selected_candidate.key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
    AND selected_candidate.key_value IS NOT NULL
    AND NOT (selected_candidate.key_type = 'TS_DAY' AND selected_candidate.key_value !~ '^\d{4}-\d{2}-\d{2}$')
    AND selected_candidate.amount_ex_vat IS NOT NULL
    AND ROUND(COALESCE(selected_candidate.amount_ex_vat, 0), 2) <> 0
    AND (
      ROUND(COALESCE(selected_candidate.amount_ex_vat, 0), 2) > 0
      OR (
        selected_candidate.finance_case_id IS NOT NULL
        AND (selected_candidate.item_direction IS NULL OR selected_candidate.item_direction = 'DEDUCTION')
        AND selected_candidate.line_type IN ('OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','LOAN_REPAYMENT')
      )
    )
  ORDER BY selected_candidate.row_ordinal, selected_candidate.preview_row_id
  LIMIT 100;

  IF NOT EXISTS (SELECT 1 FROM pg_temp.tmp_pay_workbench_draft_scope_selected_page) THEN
    IF COALESCE(v_selected_input_count, 0) > 0 THEN
      RAISE EXCEPTION 'DRAFT_CANDIDATE_SCOPE_EMPTY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_CANDIDATE_SCOPE_EMPTY',
                'operation_id', p_operation_id::text,
                'workbench_session_id', p_workbench_session_id::text,
                'selected_input_count', v_selected_input_count,
                'message', 'Selected preview rows did not produce any valid row-backed candidate scope rows.'
              )::text;
    END IF;
    RETURN QUERY SELECT 0::integer, 0::integer, 0::integer, 0::integer, 0::integer;
    RETURN;
  END IF;

  INSERT INTO public.banking_pay_operation_candidate_scope (
    operation_id,
    workbench_session_id,
    source_snapshot_run_id,
    source_session_version,
    candidate_id,
    pay_channel,
    pay_batch_id,
    selected_preview_row_ids_json,
    selected_timesheet_ids_json,
    selected_finance_case_ids_json,
    effective_payees_json,
    effective_case_resolution_states_json,
    effective_canonical_preview_lines_json,
    selected_canonical_preview_lines_json,
    baseline_component_rows_json,
    candidate_totals_json,
    allocation_basis_json,
    scope_hash,
    chunk_sequence,
    status,
    created_at_utc,
    updated_at_utc
  )
  SELECT
    p_operation_id,
    p_workbench_session_id,
    v_session.source_snapshot_run_id,
    v_session.version,
    selected_page.candidate_id,
    selected_page.pay_channel,
    v_operation.pay_batch_id,
    COALESCE(jsonb_agg(to_jsonb(selected_page.preview_row_id::text) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id), '[]'::jsonb),
    COALESCE(jsonb_agg(to_jsonb(selected_page.timesheet_id::text) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id) FILTER (WHERE selected_page.timesheet_id IS NOT NULL), '[]'::jsonb),
    COALESCE(jsonb_agg(to_jsonb(selected_page.finance_case_id::text) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id) FILTER (WHERE selected_page.finance_case_id IS NOT NULL), '[]'::jsonb),
    COALESCE(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
      'source', 'banking_pay_workbench_preview_rows',
      'preview_row_id', selected_page.preview_row_id::text,
      'row_key', selected_page.row_key,
      'pay_channel', selected_page.pay_channel,
      'payee', COALESCE(selected_page.row_json->'effective_payee', selected_page.row_json->'payee', selected_page.row_json->'payee_map')
    )) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id) FILTER (WHERE selected_page.row_json ? 'effective_payee' OR selected_page.row_json ? 'payee' OR selected_page.row_json ? 'payee_map'), '[]'::jsonb),
    jsonb_strip_nulls(jsonb_build_object(
      'source', 'banking_pay_workbench_preview_rows',
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'same_week_paye_override', COALESCE(p_same_week_paye_override_json, '{}'::jsonb),
      'rows', COALESCE(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'preview_row_id', selected_page.preview_row_id::text,
        'row_key', selected_page.row_key,
        'finance_case_id', CASE WHEN selected_page.finance_case_id IS NULL THEN NULL ELSE selected_page.finance_case_id::text END,
        'case_resolution_summary', selected_page.row_json->'case_resolution_summary'
      )) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id) FILTER (WHERE selected_page.row_json ? 'case_resolution_summary' OR selected_page.finance_case_id IS NOT NULL), '[]'::jsonb)
    )),
    COALESCE(jsonb_agg(jsonb_strip_nulls(
      selected_page.row_json
      || jsonb_build_object(
        'source', 'banking_pay_workbench_preview_rows',
        'preview_row_id', selected_page.preview_row_id::text,
        'preview_row_pk', selected_page.preview_row_id::text,
        'row_key', selected_page.row_key,
        'row_ordinal', selected_page.row_ordinal,
        'candidate_id', selected_page.candidate_id::text,
        'timesheet_id', CASE WHEN selected_page.timesheet_id IS NULL THEN NULL ELSE selected_page.timesheet_id::text END,
        'finance_case_id', CASE WHEN selected_page.finance_case_id IS NULL THEN NULL ELSE selected_page.finance_case_id::text END,
        'finance_component_id', CASE WHEN selected_page.finance_component_id IS NULL THEN NULL ELSE selected_page.finance_component_id::text END,
        'line_type', selected_page.line_type,
        'case_type', selected_page.case_type,
        'item_direction', selected_page.item_direction,
        'pay_channel', selected_page.pay_channel,
        'section', selected_page.section,
        'selection_state', 'SELECTED',
        'status', 'READY',
        'amount_ex_vat', selected_page.amount_ex_vat,
        'key_type', selected_page.key_type,
        'key_value', selected_page.key_value,
        'economic_key', selected_page.economic_key_json,
        'preview_contract', selected_page.preview_contract_json,
        'draftable', true,
        'row_backed_draft_scope', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
        'effective', true
      )
    ) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id), '[]'::jsonb),
    COALESCE(jsonb_agg(jsonb_strip_nulls(
      selected_page.row_json
      || jsonb_build_object(
        'source', 'banking_pay_workbench_preview_rows',
        'preview_row_id', selected_page.preview_row_id::text,
        'preview_row_pk', selected_page.preview_row_id::text,
        'row_key', selected_page.row_key,
        'row_ordinal', selected_page.row_ordinal,
        'candidate_id', selected_page.candidate_id::text,
        'timesheet_id', CASE WHEN selected_page.timesheet_id IS NULL THEN NULL ELSE selected_page.timesheet_id::text END,
        'finance_case_id', CASE WHEN selected_page.finance_case_id IS NULL THEN NULL ELSE selected_page.finance_case_id::text END,
        'finance_component_id', CASE WHEN selected_page.finance_component_id IS NULL THEN NULL ELSE selected_page.finance_component_id::text END,
        'line_type', selected_page.line_type,
        'case_type', selected_page.case_type,
        'item_direction', selected_page.item_direction,
        'pay_channel', selected_page.pay_channel,
        'section', selected_page.section,
        'selection_state', 'SELECTED',
        'status', 'READY',
        'amount_ex_vat', selected_page.amount_ex_vat,
        'key_type', selected_page.key_type,
        'key_value', selected_page.key_value,
        'economic_key', selected_page.economic_key_json,
        'preview_contract', selected_page.preview_contract_json,
        'draftable', true,
        'row_backed_draft_scope', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )
    ) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id), '[]'::jsonb),
    COALESCE(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
      'source', 'banking_pay_workbench_preview_rows',
      'preview_row_id', selected_page.preview_row_id::text,
      'row_key', selected_page.row_key,
      'row_ordinal', selected_page.row_ordinal,
      'timesheet_id', CASE WHEN selected_page.timesheet_id IS NULL THEN NULL ELSE selected_page.timesheet_id::text END,
      'candidate_id', selected_page.candidate_id::text,
      'pay_channel', selected_page.pay_channel,
      'economic_key', selected_page.economic_key_json,
      'amount_ex_vat', selected_page.amount_ex_vat,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    )) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id), '[]'::jsonb),
    jsonb_build_object(
      'selected_row_count_seeded_in_page', COUNT(*)::integer,
      'selected_amount_ex_vat_seeded_in_page', ROUND(COALESCE(SUM(selected_page.amount_ex_vat), 0), 2),
      'selected_preview_row_ids', COALESCE(jsonb_agg(to_jsonb(selected_page.preview_row_id::text) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id), '[]'::jsonb),
      'row_backed_draft_scope', true,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ),
    jsonb_build_object(
      'source', 'banking_pay_workbench_preview_rows',
      'operation_id', p_operation_id::text,
      'workbench_session_id', p_workbench_session_id::text,
      'pay_channel_scope', v_scope_filter,
      'same_week_paye_override', COALESCE(p_same_week_paye_override_json, '{}'::jsonb),
      'selected_rows_are_row_backed', true,
      'selected_preview_row_ids', COALESCE(jsonb_agg(to_jsonb(selected_page.preview_row_id::text) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id), '[]'::jsonb),
      'economic_keyspace', 'timesheet_id,key_type,key_value',
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ),
    md5(jsonb_build_object(
      'operation_id', p_operation_id::text,
      'workbench_session_id', p_workbench_session_id::text,
      'candidate_id', selected_page.candidate_id::text,
      'pay_channel', selected_page.pay_channel,
      'selected_preview_row_ids', COALESCE(jsonb_agg(to_jsonb(selected_page.preview_row_id::text) ORDER BY selected_page.row_ordinal, selected_page.preview_row_id), '[]'::jsonb),
      'source_session_version', v_session.version
    )::text),
    ROW_NUMBER() OVER (ORDER BY selected_page.pay_channel, selected_page.candidate_id)::integer,
    'SCOPED',
    v_now,
    v_now
  FROM pg_temp.tmp_pay_workbench_draft_scope_selected_page AS selected_page
  WHERE selected_page.pay_channel IN ('PAYE', 'UMBRELLA')
  GROUP BY selected_page.candidate_id, selected_page.pay_channel
  ON CONFLICT (operation_id, candidate_id, pay_channel)
  DO UPDATE
  SET workbench_session_id = EXCLUDED.workbench_session_id,
      source_snapshot_run_id = EXCLUDED.source_snapshot_run_id,
      source_session_version = EXCLUDED.source_session_version,
      pay_batch_id = COALESCE(public.banking_pay_operation_candidate_scope.pay_batch_id, EXCLUDED.pay_batch_id),
      selected_preview_row_ids_json = EXCLUDED.selected_preview_row_ids_json,
      selected_timesheet_ids_json = EXCLUDED.selected_timesheet_ids_json,
      selected_finance_case_ids_json = EXCLUDED.selected_finance_case_ids_json,
      effective_payees_json = EXCLUDED.effective_payees_json,
      effective_case_resolution_states_json = EXCLUDED.effective_case_resolution_states_json,
      effective_canonical_preview_lines_json = EXCLUDED.effective_canonical_preview_lines_json,
      selected_canonical_preview_lines_json = EXCLUDED.selected_canonical_preview_lines_json,
      baseline_component_rows_json = EXCLUDED.baseline_component_rows_json,
      candidate_totals_json = EXCLUDED.candidate_totals_json,
      allocation_basis_json = COALESCE(public.banking_pay_operation_candidate_scope.allocation_basis_json, '{}'::jsonb) || EXCLUDED.allocation_basis_json,
      scope_hash = EXCLUDED.scope_hash,
      chunk_sequence = EXCLUDED.chunk_sequence,
      status = CASE WHEN public.banking_pay_operation_candidate_scope.status IN ('PENDING', 'SCOPED') THEN 'SCOPED' ELSE public.banking_pay_operation_candidate_scope.status END,
      updated_at_utc = v_now;

  SELECT COUNT(DISTINCT selected_page.candidate_id::text || ':' || selected_page.pay_channel)::integer,
         COUNT(*)::integer,
         COUNT(DISTINCT selected_page.timesheet_id) FILTER (WHERE selected_page.timesheet_id IS NOT NULL)::integer,
         COUNT(DISTINCT selected_page.finance_case_id) FILTER (WHERE selected_page.finance_case_id IS NOT NULL)::integer,
         COUNT(DISTINCT selected_page.pay_channel)::integer
  INTO v_candidate_scope_count, v_selected_count, v_timesheet_count, v_finance_case_count, v_pay_channel_count
  FROM pg_temp.tmp_pay_workbench_draft_scope_selected_page AS selected_page;

  IF COALESCE(v_selected_count, 0) > 0 AND COALESCE(v_candidate_scope_count, 0) <= 0 THEN
    RAISE EXCEPTION 'DRAFT_CANDIDATE_SCOPE_EMPTY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_CANDIDATE_SCOPE_EMPTY',
              'operation_id', p_operation_id::text,
              'workbench_session_id', p_workbench_session_id::text,
              'selected_row_count', COALESCE(v_selected_count, 0),
              'message', 'Selected preview rows did not persist into candidate scope.'
            )::text;
  END IF;

  UPDATE public.banking_pay_operations AS operation_update
  SET progress_json = jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'draft_scope_last_seeded_at_utc', v_now::text,
          'draft_scope_last_selected_row_count', COALESCE(v_selected_count, 0),
          'draft_scope_last_candidate_scope_count', COALESCE(v_candidate_scope_count, 0),
          'draft_scope_selection_resolved_to_current', COALESCE(v_selection_resolved_to_current, false),
          'draft_scope_resolved_current_selected_preview_row_ids', COALESCE(v_resolved_current_selection_ids, '[]'::jsonb)
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN QUERY
  SELECT
    COALESCE(v_candidate_scope_count, 0),
    COALESCE(v_selected_count, 0),
    COALESCE(v_timesheet_count, 0),
    COALESCE(v_finance_case_count, 0),
    COALESCE(v_pay_channel_count, 0);
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_prepare_draft_scope_seed(
  uuid, uuid, uuid, jsonb, text, jsonb
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.pay_workbench_prepare_draft_scope_seed(
  uuid, uuid, uuid, jsonb, text, jsonb
) TO service_role;
