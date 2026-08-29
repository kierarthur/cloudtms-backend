CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(p_operation_id uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
 RETURNS TABLE(candidate_scopes_processed integer, allocation_rows_inserted integer, allocation_rows_reused integer, failures integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_scope_ids jsonb := COALESCE(p_candidate_scope_ids, '[]'::jsonb);
  v_scope_id_count integer := 0;
  v_candidate_scopes_processed integer := 0;
  v_expected_count integer := 0;
  v_inserted integer := 0;
  v_reused integer := 0;
  v_malformed_allocation_preview_rows jsonb := '[]'::jsonb;
  v_synthetic_total_allocation_preview_rows jsonb := '[]'::jsonb;
  v_semantic_draft_guard_enabled boolean := false;
  v_source_publication_identity_enforce_enabled boolean := false;
  v_semantic_draft_failures jsonb := '[]'::jsonb;
  v_semantic_source_failure_count integer := 0;
  v_plan_scope_ids jsonb := '[]'::jsonb;
  v_plan_drift_details jsonb := '[]'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed: p_operation_id is required';
  END IF;

  IF p_candidate_scope_ids IS NOT NULL AND jsonb_typeof(p_candidate_scope_ids) <> 'array' THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed requires p_candidate_scope_ids to be null or a JSON array';
  END IF;

  IF p_candidate_scope_ids IS NOT NULL THEN
    v_scope_id_count := jsonb_array_length(v_scope_ids);
    IF v_scope_id_count = 0 THEN
      RAISE EXCEPTION 'DRAFT_ALLOCATION_SCOPE_EMPTY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'DRAFT_ALLOCATION_SCOPE_EMPTY', 'operation_id', p_operation_id::text, 'message', 'DRAFT_CREATE allocation seeding requires at least one candidate scope id.')::text;
    END IF;
    IF v_scope_id_count > 100 THEN
      RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed candidate scope id array exceeds the 100 row cap: %', v_scope_id_count;
    END IF;
    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
      WHERE NOT ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    ) THEN
      RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed requires candidate scope ids to be UUID strings';
    END IF;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed operation not found: %', p_operation_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE' THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed expected DRAFT_CREATE operation %, got %', p_operation_id, v_operation.operation_type;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed cannot seed allocation rows for terminal operation % with status %', p_operation_id, v_operation.status;
  END IF;

  SELECT COALESCE(settings_row.banking_pay_workbench_semantic_ready_draft_guard_v2_enabled, false),
         COALESCE(settings_row.banking_pay_source_publication_identity_enforce_v1_enabled, false)
  INTO v_semantic_draft_guard_enabled,v_source_publication_identity_enforce_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_allocation_scope_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_allocation_scope_rows ON COMMIT DROP AS
  SELECT scope_row.*
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND (
      (
        p_candidate_scope_ids IS NULL
        AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN ('PENDING', 'SCOPED')
      )
      OR (
        p_candidate_scope_ids IS NOT NULL
        AND scope_row.id IN (
          SELECT (supplied_scope.scope_value #>> '{}')::uuid
          FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
        )
      )
    )
  ORDER BY scope_row.chunk_sequence NULLS LAST, scope_row.pay_channel, scope_row.candidate_id, scope_row.id
  LIMIT 100;

  SELECT COUNT(*)::integer
  INTO v_candidate_scopes_processed
  FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope;

  IF p_candidate_scope_ids IS NOT NULL AND COALESCE(v_candidate_scopes_processed, 0) <> COALESCE(v_scope_id_count, 0) THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed one or more candidate scope ids do not belong to operation %', p_operation_id;
  END IF;

  IF COALESCE(v_candidate_scopes_processed, 0) = 0 THEN
    RETURN QUERY SELECT 0::integer, 0::integer, 0::integer, 0::integer;
    RETURN;
  END IF;

  SELECT COALESCE(jsonb_agg(selected_scope.id::text ORDER BY selected_scope.id::text), '[]'::jsonb)
  INTO v_plan_scope_ids
  FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_allocation_preview_candidates;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_allocation_preview_candidates ON COMMIT DROP AS
  WITH scope_selected_lines AS (
    SELECT
      selected_scope.operation_id,
      selected_scope.id AS candidate_scope_id,
      selected_scope.pay_batch_id,
      selected_scope.workbench_session_id,
      selected_scope.source_session_version,
      selected_scope.candidate_id,
      selected_scope.pay_channel,
      selected_scope.candidate_totals_json,
      selected_scope.allocation_basis_json AS scope_allocation_basis_json,
      line_values.value AS line_json,
      line_values.ordinality::bigint AS line_ordinal
    FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(selected_scope.selected_canonical_preview_lines_json) = 'array'
          AND jsonb_array_length(selected_scope.selected_canonical_preview_lines_json) > 0
          THEN selected_scope.selected_canonical_preview_lines_json
        WHEN jsonb_typeof(selected_scope.effective_canonical_preview_lines_json) = 'array'
          AND jsonb_array_length(selected_scope.effective_canonical_preview_lines_json) > 0
          THEN selected_scope.effective_canonical_preview_lines_json
        ELSE '[]'::jsonb
      END
    ) WITH ORDINALITY AS line_values(value, ordinality)
  ), normalised_scope_lines_raw AS (
    SELECT
      scope_selected_lines.operation_id,
      scope_selected_lines.candidate_scope_id,
      scope_selected_lines.pay_batch_id,
      scope_selected_lines.workbench_session_id,
      scope_selected_lines.source_session_version,
      scope_selected_lines.candidate_id,
      scope_selected_lines.pay_channel,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'preview_row_id', scope_selected_lines.line_json->>'preview_row_pk', scope_selected_lines.line_json->>'row_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'preview_row_id', scope_selected_lines.line_json->>'preview_row_pk', scope_selected_lines.line_json->>'row_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS preview_row_id,
      NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'row_key', scope_selected_lines.line_json->>'source_ref', '')), '') AS row_key,
      CASE WHEN COALESCE(scope_selected_lines.line_json->>'row_ordinal', '') ~ '^[0-9]+$' THEN (scope_selected_lines.line_json->>'row_ordinal')::bigint ELSE scope_selected_lines.line_ordinal END AS row_ordinal,
      scope_selected_lines.line_json AS row_json,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,timesheet_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,timesheet_id}', '')), '')::uuid
        ELSE NULL::uuid
      END AS economic_key_timesheet_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'timesheet_id', scope_selected_lines.line_json->>'real_business_timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'timesheet_id', scope_selected_lines.line_json->>'real_business_timesheet_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS top_level_timesheet_id,
      COALESCE(NULLIF(BTRIM(scope_selected_lines.line_json->>'section'), ''), 'canonical_preview_lines') AS section,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,key_type}', scope_selected_lines.line_json->>'key_type', scope_selected_lines.line_json->>'component_key_type', '')), '')) AS key_type,
      NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,key_value}', scope_selected_lines.line_json->>'key_value', scope_selected_lines.line_json->>'component_key_value', '')), '') AS key_value,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'selection_state', 'SELECTED')), '')) AS selection_state,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'status', 'READY')), '')) AS line_status,
      LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'selected', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS line_selected,
      (
        COALESCE(scope_selected_lines.line_json->>'projection_path', '') = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        OR scope_selected_lines.line_json ? 'projection_run_id'
      ) AS is_delta_projection,
      LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'projection_certified', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS projection_certified,
      COALESCE(
        NULLIF(BTRIM(scope_selected_lines.line_json->>'policy_x_authority_scope'), ''),
        NULLIF(BTRIM(scope_selected_lines.line_json#>>'{contract_json,policy_x_authority_scope}'), ''),
        NULLIF(BTRIM(scope_selected_lines.line_json#>>'{contract,policy_x_authority_scope}'), ''),
        ''
      ) AS policy_x_authority_scope,
      (
        LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          UPPER(BTRIM(COALESCE(scope_selected_lines.line_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          OR LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'selected', 'true'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
          OR UPPER(BTRIM(COALESCE(scope_selected_lines.line_json->>'selection_state', 'SELECTED'))) <> 'SELECTED'
          OR UPPER(BTRIM(COALESCE(scope_selected_lines.line_json->>'status', 'READY'))) <> 'READY'
        )
      ) AS post_draft_overlay_unavailable,
      CASE WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_case_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_case_id', '')), '')::uuid ELSE NULL::uuid END AS finance_case_id,
      CASE WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_component_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_component_id', '')), '')::uuid ELSE NULL::uuid END AS finance_component_id,
      UPPER(COALESCE(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'line_type', '')), ''), NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'case_type', '')), ''), 'PREVIEW_ROW')) AS allocation_type,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'item_direction', scope_selected_lines.line_json->>'direction', '')), '')) AS item_direction,
      component_probe.single_fixed_reimbursement_key_type,
      component_probe.single_fixed_reimbursement_key_value,
      NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'source_ref', scope_selected_lines.line_json->>'row_key', '')), '') AS source_ref,
      CASE
        WHEN COALESCE(scope_selected_lines.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND((scope_selected_lines.line_json->>'amount_ex_vat')::numeric, 2)
        WHEN COALESCE(scope_selected_lines.line_json->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND((scope_selected_lines.line_json->>'preview_amount_ex_vat')::numeric, 2)
        WHEN COALESCE(scope_selected_lines.line_json->>'allocated_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND((scope_selected_lines.line_json->>'allocated_amount')::numeric, 2)
        ELSE NULL::numeric
      END AS allocated_amount
    FROM scope_selected_lines
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN component_counts.object_component_count = 1
           AND component_counts.fixed_reimbursement_component_count = 1 THEN component_counts.fixed_reimbursement_key_type
          ELSE NULL::text
        END AS single_fixed_reimbursement_key_type,
        CASE
          WHEN component_counts.object_component_count = 1
           AND component_counts.fixed_reimbursement_component_count = 1 THEN component_counts.fixed_reimbursement_key_value
          ELSE NULL::text
        END AS single_fixed_reimbursement_key_value
      FROM (
        SELECT
          (COUNT(*) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
          ))::integer AS object_component_count,
          (COUNT(*) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
              AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
          ))::integer AS fixed_reimbursement_component_count,
          MAX(UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), ''))) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
              AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
          ) AS fixed_reimbursement_key_type,
          MAX(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '')) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
              AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
          ) AS fixed_reimbursement_key_value
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(scope_selected_lines.line_json->'case_components') = 'array' THEN scope_selected_lines.line_json->'case_components'
            ELSE '[]'::jsonb
          END
        ) AS component_element(value)
      ) AS component_counts
    ) AS component_probe
  ), allocation_timesheet_refs AS (
    SELECT normalised_scope_lines_raw.economic_key_timesheet_id AS timesheet_id
    FROM normalised_scope_lines_raw
    WHERE normalised_scope_lines_raw.economic_key_timesheet_id IS NOT NULL
    UNION
    SELECT normalised_scope_lines_raw.top_level_timesheet_id AS timesheet_id
    FROM normalised_scope_lines_raw
    WHERE normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL
  ), allocation_timesheet_id_array AS (
    SELECT COALESCE(
      array_agg(DISTINCT allocation_timesheet_refs.timesheet_id ORDER BY allocation_timesheet_refs.timesheet_id),
      array[]::uuid[]
    ) AS timesheet_ids
    FROM allocation_timesheet_refs
  ), allocation_rotation_scope AS (
    SELECT DISTINCT ON (rotation_scope.requested_timesheet_id)
      rotation_scope.requested_timesheet_id,
      COALESCE(rotation_scope.canonical_timesheet_id, rotation_scope.requested_timesheet_id) AS canonical_timesheet_id,
      (rotation_scope.family_timesheet_id IS NOT NULL AND rotation_scope.family_is_current IS NOT NULL) AS rotation_scope_resolved
    FROM allocation_timesheet_id_array
    JOIN public._pay_timesheet_rotation_scope(allocation_timesheet_id_array.timesheet_ids) AS rotation_scope
      ON true
    ORDER BY
      rotation_scope.requested_timesheet_id,
      rotation_scope.family_is_current DESC NULLS LAST,
      rotation_scope.family_version DESC NULLS LAST,
      rotation_scope.family_timesheet_id
  ), normalised_scope_lines AS (
    SELECT
      normalised_scope_lines_raw.operation_id,
      normalised_scope_lines_raw.candidate_scope_id,
      normalised_scope_lines_raw.pay_batch_id,
      normalised_scope_lines_raw.workbench_session_id,
      normalised_scope_lines_raw.source_session_version,
      normalised_scope_lines_raw.candidate_id,
      normalised_scope_lines_raw.pay_channel,
      normalised_scope_lines_raw.preview_row_id,
      normalised_scope_lines_raw.row_key,
      normalised_scope_lines_raw.row_ordinal,
      jsonb_strip_nulls(
        normalised_scope_lines_raw.row_json
        || CASE
          WHEN COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id) IS NULL THEN '{}'::jsonb
          ELSE jsonb_build_object(
            'timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)::text,
            'real_business_timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)::text,
            'economic_key', COALESCE(normalised_scope_lines_raw.row_json->'economic_key', '{}'::jsonb)
              || jsonb_build_object('timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)::text)
          )
        END
        || CASE
          WHEN normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL
           AND normalised_scope_lines_raw.top_level_timesheet_id IS DISTINCT FROM COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)
          THEN jsonb_build_object('rotation_requested_timesheet_id', normalised_scope_lines_raw.top_level_timesheet_id::text)
          ELSE '{}'::jsonb
        END
      ) AS row_json,
      COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id, normalised_scope_lines_raw.economic_key_timesheet_id, normalised_scope_lines_raw.top_level_timesheet_id) AS timesheet_id,
      normalised_scope_lines_raw.section,
      normalised_scope_lines_raw.key_type,
      normalised_scope_lines_raw.key_value,
      normalised_scope_lines_raw.selection_state,
      normalised_scope_lines_raw.line_status,
      normalised_scope_lines_raw.line_selected,
      normalised_scope_lines_raw.is_delta_projection,
      normalised_scope_lines_raw.projection_certified,
      normalised_scope_lines_raw.policy_x_authority_scope,
      normalised_scope_lines_raw.post_draft_overlay_unavailable,
      (
        normalised_scope_lines_raw.preview_row_id IS NULL
        OR backing_preview_row.id IS NULL
        OR UPPER(BTRIM(COALESCE(backing_preview_row.status, ''))) <> 'READY'
        OR COALESCE(backing_preview_row.selected, false) IS NOT TRUE
        OR UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, ''))) <> 'SELECTED'
        OR backing_preview_row.candidate_id IS DISTINCT FROM normalised_scope_lines_raw.candidate_id
        OR private.pay_workbench_preview_effective_section_v1(
             backing_preview_row.section,
             backing_preview_row.row_json
           ) IS DISTINCT FROM COALESCE(NULLIF(BTRIM(normalised_scope_lines_raw.section), ''), 'canonical_preview_lines')
        OR NULLIF(BTRIM(backing_preview_row.row_key), '') IS DISTINCT FROM NULLIF(BTRIM(normalised_scope_lines_raw.row_key), '')
        OR (
          normalised_scope_lines_raw.finance_case_id IS NULL
          AND backing_preview_row.timesheet_id IS NULL
        )
        OR (
          normalised_scope_lines_raw.finance_case_id IS NULL
          AND backing_preview_row.timesheet_id IS DISTINCT FROM COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id, normalised_scope_lines_raw.economic_key_timesheet_id, normalised_scope_lines_raw.top_level_timesheet_id)
        )
        OR UPPER(NULLIF(BTRIM(COALESCE(backing_preview_row.key_type, backing_preview_row.row_json#>>'{economic_key,key_type}', backing_preview_row.row_json->>'component_key_type', '')), '')) IS DISTINCT FROM normalised_scope_lines_raw.key_type
        OR NULLIF(BTRIM(COALESCE(backing_preview_row.key_value, backing_preview_row.row_json#>>'{economic_key,key_value}', backing_preview_row.row_json->>'component_key_value', '')), '') IS DISTINCT FROM normalised_scope_lines_raw.key_value
        OR COALESCE(
          NULLIF(BTRIM(backing_preview_row.row_json->>'policy_x_authority_scope'), ''),
          NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract_json,policy_x_authority_scope}'), ''),
          NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract,policy_x_authority_scope}'), ''),
          ''
        ) <> 'PRE_DRAFT_LIVE_TRUTH'
        OR (
          (
            COALESCE(backing_preview_row.row_json->>'projection_path', '') = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
            OR backing_preview_row.row_json ? 'projection_run_id'
          )
          AND LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'projection_certified', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        OR (
          backing_preview_row.row_json ? 'selected'
          AND (LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'selected', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) IS DISTINCT FROM COALESCE(backing_preview_row.selected, false)
        )
        OR (
          backing_preview_row.row_json ? 'selection_state'
          AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'selection_state', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, '')))
        )
        OR (
          backing_preview_row.row_json ? 'status'
          AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'status', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.status, '')))
        )
        OR (
          LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND (
            UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
            OR LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'selected', 'true'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
            OR UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'selection_state', backing_preview_row.selection_state, ''))) <> 'SELECTED'
            OR UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'status', backing_preview_row.status, ''))) <> 'READY'
          )
        )
      ) AS backing_table_state_invalid,
      CASE
        WHEN normalised_scope_lines_raw.preview_row_id IS NULL THEN 'ALLOCATION_BACKING_PREVIEW_ROW_ID_MISSING'
        WHEN backing_preview_row.id IS NULL THEN 'ALLOCATION_BACKING_PREVIEW_ROW_NOT_CURRENT'
        WHEN UPPER(BTRIM(COALESCE(backing_preview_row.status, ''))) <> 'READY' THEN 'ALLOCATION_BACKING_PREVIEW_ROW_NOT_READY'
        WHEN COALESCE(backing_preview_row.selected, false) IS NOT TRUE THEN 'ALLOCATION_BACKING_PREVIEW_ROW_NOT_SELECTED'
        WHEN UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, ''))) <> 'SELECTED' THEN 'ALLOCATION_BACKING_PREVIEW_ROW_SELECTION_STATE_INVALID'
        WHEN backing_preview_row.candidate_id IS DISTINCT FROM normalised_scope_lines_raw.candidate_id THEN 'ALLOCATION_BACKING_PREVIEW_ROW_CANDIDATE_MISMATCH'
        WHEN private.pay_workbench_preview_effective_section_v1(
               backing_preview_row.section,
               backing_preview_row.row_json
             ) IS DISTINCT FROM COALESCE(NULLIF(BTRIM(normalised_scope_lines_raw.section), ''), 'canonical_preview_lines') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_SECTION_MISMATCH'
        WHEN NULLIF(BTRIM(backing_preview_row.row_key), '') IS DISTINCT FROM NULLIF(BTRIM(normalised_scope_lines_raw.row_key), '') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_KEY_MISMATCH'
        WHEN normalised_scope_lines_raw.finance_case_id IS NULL AND backing_preview_row.timesheet_id IS NULL THEN 'ALLOCATION_BACKING_PREVIEW_ROW_ECONOMIC_KEY_MISSING'
        WHEN normalised_scope_lines_raw.finance_case_id IS NULL AND backing_preview_row.timesheet_id IS DISTINCT FROM COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id, normalised_scope_lines_raw.economic_key_timesheet_id, normalised_scope_lines_raw.top_level_timesheet_id) THEN 'ALLOCATION_BACKING_PREVIEW_ROW_TIMESHEET_MISMATCH'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(backing_preview_row.key_type, backing_preview_row.row_json#>>'{economic_key,key_type}', backing_preview_row.row_json->>'component_key_type', '')), '')) IS DISTINCT FROM normalised_scope_lines_raw.key_type THEN 'ALLOCATION_BACKING_PREVIEW_ROW_KEY_TYPE_MISMATCH'
        WHEN NULLIF(BTRIM(COALESCE(backing_preview_row.key_value, backing_preview_row.row_json#>>'{economic_key,key_value}', backing_preview_row.row_json->>'component_key_value', '')), '') IS DISTINCT FROM normalised_scope_lines_raw.key_value THEN 'ALLOCATION_BACKING_PREVIEW_ROW_KEY_VALUE_MISMATCH'
        WHEN COALESCE(NULLIF(BTRIM(backing_preview_row.row_json->>'policy_x_authority_scope'), ''), NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract_json,policy_x_authority_scope}'), ''), NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract,policy_x_authority_scope}'), ''), '') <> 'PRE_DRAFT_LIVE_TRUTH' THEN 'ALLOCATION_BACKING_PREVIEW_ROW_POLICY_X_AUTHORITY_INVALID'
        WHEN (COALESCE(backing_preview_row.row_json->>'projection_path', '') = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' OR backing_preview_row.row_json ? 'projection_run_id') AND LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'projection_certified', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_DELTA_NOT_CERTIFIED'
        WHEN LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_POST_DRAFT_OVERLAY_UNAVAILABLE'
        WHEN (
          (backing_preview_row.row_json ? 'selected' AND (LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'selected', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) IS DISTINCT FROM COALESCE(backing_preview_row.selected, false))
          OR (backing_preview_row.row_json ? 'selection_state' AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'selection_state', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, ''))))
          OR (backing_preview_row.row_json ? 'status' AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'status', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.status, ''))))
        ) THEN 'ALLOCATION_BACKING_PREVIEW_ROW_TABLE_JSON_CONFLICT'
        ELSE NULL::text
      END AS backing_table_state_failure_reason,
      normalised_scope_lines_raw.finance_case_id,
      normalised_scope_lines_raw.finance_component_id,
      normalised_scope_lines_raw.allocation_type,
      normalised_scope_lines_raw.item_direction,
      normalised_scope_lines_raw.single_fixed_reimbursement_key_type,
      normalised_scope_lines_raw.single_fixed_reimbursement_key_value,
      normalised_scope_lines_raw.source_ref,
      normalised_scope_lines_raw.allocated_amount,
      CASE
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NULL AND normalised_scope_lines_raw.top_level_timesheet_id IS NULL THEN false
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NOT NULL AND COALESCE(economic_key_rotation.rotation_scope_resolved, false) = false THEN true
        WHEN normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL AND COALESCE(top_level_rotation.rotation_scope_resolved, false) = false THEN true
        WHEN economic_key_rotation.canonical_timesheet_id IS NOT NULL
         AND top_level_rotation.canonical_timesheet_id IS NOT NULL
         AND economic_key_rotation.canonical_timesheet_id IS DISTINCT FROM top_level_rotation.canonical_timesheet_id THEN true
        ELSE false
      END AS rotation_validation_failed,
      CASE
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NULL AND normalised_scope_lines_raw.top_level_timesheet_id IS NULL THEN NULL::text
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NOT NULL AND COALESCE(economic_key_rotation.rotation_scope_resolved, false) = false THEN 'ALLOCATION_ECONOMIC_KEY_TIMESHEET_ROTATION_SCOPE_UNRESOLVED'
        WHEN normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL AND COALESCE(top_level_rotation.rotation_scope_resolved, false) = false THEN 'ALLOCATION_LINE_TIMESHEET_ROTATION_SCOPE_UNRESOLVED'
        WHEN economic_key_rotation.canonical_timesheet_id IS NOT NULL
         AND top_level_rotation.canonical_timesheet_id IS NOT NULL
         AND economic_key_rotation.canonical_timesheet_id IS DISTINCT FROM top_level_rotation.canonical_timesheet_id THEN 'ALLOCATION_LINE_AND_ECONOMIC_KEY_CANONICAL_TIMESHEET_MISMATCH'
        ELSE NULL::text
      END AS rotation_failure_reason
    FROM normalised_scope_lines_raw
    LEFT JOIN allocation_rotation_scope AS economic_key_rotation
      ON economic_key_rotation.requested_timesheet_id = normalised_scope_lines_raw.economic_key_timesheet_id
    LEFT JOIN allocation_rotation_scope AS top_level_rotation
      ON top_level_rotation.requested_timesheet_id = normalised_scope_lines_raw.top_level_timesheet_id
    LEFT JOIN public.banking_pay_workbench_preview_rows AS backing_preview_row
      ON backing_preview_row.id = normalised_scope_lines_raw.preview_row_id
     AND (
       normalised_scope_lines_raw.workbench_session_id IS NULL
       OR backing_preview_row.session_id = normalised_scope_lines_raw.workbench_session_id
     )
     AND (
       normalised_scope_lines_raw.source_session_version IS NULL
       OR backing_preview_row.session_version = normalised_scope_lines_raw.source_session_version
     )
  )
  SELECT
    normalised_scope_lines.*,
    jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', CASE WHEN normalised_scope_lines.timesheet_id IS NULL THEN NULL ELSE normalised_scope_lines.timesheet_id::text END,
      'key_type', normalised_scope_lines.key_type,
      'key_value', normalised_scope_lines.key_value
    )) AS economic_key_json,
    public.pay_workbench_preview_line_contract_ok(
      p_line_json => jsonb_strip_nulls(
        normalised_scope_lines.row_json
        || jsonb_build_object(
          'row_key', normalised_scope_lines.row_key,
          'preview_row_pk', CASE WHEN normalised_scope_lines.preview_row_id IS NULL THEN NULL ELSE normalised_scope_lines.preview_row_id::text END,
          'selection_state', normalised_scope_lines.selection_state
        )
      ),
      p_economic_key_json => jsonb_strip_nulls(jsonb_build_object(
        'timesheet_id', CASE WHEN normalised_scope_lines.timesheet_id IS NULL THEN NULL ELSE normalised_scope_lines.timesheet_id::text END,
        'key_type', normalised_scope_lines.key_type,
        'key_value', normalised_scope_lines.key_value
      )),
      p_target_section => normalised_scope_lines.section
    ) AS preview_contract_json
  FROM normalised_scope_lines
  ORDER BY normalised_scope_lines.pay_channel, normalised_scope_lines.candidate_id, normalised_scope_lines.row_ordinal, normalised_scope_lines.preview_row_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', invalid_rows.preview_row_id::text,
           'row_key', invalid_rows.row_key,
           'candidate_id', invalid_rows.candidate_id::text,
           'candidate_scope_id', invalid_rows.candidate_scope_id::text,
           'reasons', COALESCE(invalid_rows.preview_contract_json->'reasons', '[]'::jsonb)
             || CASE WHEN invalid_rows.rotation_validation_failed IS TRUE THEN jsonb_build_array(invalid_rows.rotation_failure_reason) ELSE '[]'::jsonb END
             || CASE WHEN invalid_rows.backing_table_state_invalid IS TRUE THEN jsonb_build_array(COALESCE(invalid_rows.backing_table_state_failure_reason, 'ALLOCATION_BACKING_PREVIEW_ROW_STATE_INVALID')) ELSE '[]'::jsonb END
             || CASE
               WHEN invalid_rows.single_fixed_reimbursement_key_type IS NOT NULL
                AND (
                  invalid_rows.key_type IS DISTINCT FROM invalid_rows.single_fixed_reimbursement_key_type
                  OR invalid_rows.key_value IS DISTINCT FROM invalid_rows.single_fixed_reimbursement_key_value
                ) THEN jsonb_build_array('POLICY_X_REIMBURSEMENT_KEY_MISMATCH')
               ELSE '[]'::jsonb
             END
             || CASE
               WHEN ROUND(COALESCE(invalid_rows.allocated_amount, 0), 2) < 0
                AND NOT (
                  invalid_rows.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
                  AND invalid_rows.finance_case_id IS NOT NULL
                  AND (invalid_rows.item_direction IS NULL OR invalid_rows.item_direction = 'DEDUCTION')
                ) THEN jsonb_build_array('NEGATIVE_ENTITLEMENT_MUST_ROUTE_TO_FINANCE_CASE')
               ELSE '[]'::jsonb
             END
         ) ORDER BY invalid_rows.row_ordinal, invalid_rows.preview_row_id), '[]'::jsonb)
  INTO v_malformed_allocation_preview_rows
  FROM (
    SELECT candidate_row.*
    FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS candidate_row
    WHERE candidate_row.rotation_validation_failed IS TRUE
       OR LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'selection_allowed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR candidate_row.preview_row_id IS NULL
       OR COALESCE(candidate_row.backing_table_state_invalid, false) IS TRUE
       OR lower(COALESCE(candidate_row.section, '')) <> 'canonical_preview_lines'
       OR UPPER(BTRIM(COALESCE(candidate_row.selection_state, ''))) <> 'SELECTED'
       OR UPPER(BTRIM(COALESCE(candidate_row.line_status, ''))) <> 'READY'
       OR COALESCE(candidate_row.line_selected, false) IS NOT TRUE
       OR (COALESCE(candidate_row.is_delta_projection, false) IS TRUE AND COALESCE(candidate_row.projection_certified, false) IS NOT TRUE)
       OR COALESCE(candidate_row.policy_x_authority_scope, '') <> 'PRE_DRAFT_LIVE_TRUTH'
       OR (candidate_row.timesheet_id IS NULL AND candidate_row.finance_case_id IS NULL)
       OR COALESCE(candidate_row.post_draft_overlay_unavailable, false) IS TRUE
       OR candidate_row.pay_channel NOT IN ('PAYE', 'UMBRELLA')
       OR (
         candidate_row.key_type NOT IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
         AND NOT (candidate_row.finance_case_id IS NOT NULL AND candidate_row.key_type IN ('CASE_TOTAL','FINANCE_COMPONENT'))
       )
       OR candidate_row.key_value IS NULL
       OR (
         candidate_row.single_fixed_reimbursement_key_type IS NOT NULL
         AND (
           candidate_row.key_type IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_type
           OR candidate_row.key_value IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_value
         )
       )
       OR (candidate_row.key_type = 'TS_DAY' AND candidate_row.key_value !~ '^\d{4}-\d{2}-\d{2}$')
       OR candidate_row.allocated_amount IS NULL
       OR ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) = 0
       OR (
         ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) < 0
         AND NOT (
           candidate_row.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
           AND candidate_row.finance_case_id IS NOT NULL
           AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
         )
       )
    ORDER BY candidate_row.row_ordinal, candidate_row.preview_row_id
    LIMIT 25
  ) AS invalid_rows;

  IF jsonb_array_length(COALESCE(v_malformed_allocation_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE',
              'operation_id', p_operation_id::text,
              'malformed_selected_preview_rows', COALESCE(v_malformed_allocation_preview_rows, '[]'::jsonb),
              'message', 'Selected preview rows cannot be converted into allocation rows because they are not valid draftable Ready to Pay rows.'
            )::text;
  END IF;

  -- A set of individually valid deduction rows is not by itself a valid Draft
  -- candidate.  Headroom is candidate-local and is calculated only from the
  -- positive ordinary rows actually selected into this Draft operation.
  IF COALESCE(v_semantic_draft_guard_enabled, false) THEN
    SELECT COUNT(*)::integer
    INTO v_semantic_source_failure_count
    FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
    WHERE selected_scope.allocation_basis_json#>>'{source_publication_attestation,attestation_version}'
            IS DISTINCT FROM 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
       OR selected_scope.allocation_basis_json#>>'{source_publication_attestation,semantic_contract_version}'
            IS DISTINCT FROM 'READY_TO_PAY_SEMANTIC_V2'
       OR COALESCE((selected_scope.allocation_basis_json#>>'{source_publication_attestation,semantic_ready}')::boolean,false)
            IS NOT TRUE
       OR NULLIF(selected_scope.allocation_basis_json->>'semantic_proof_digest','') IS NULL
       OR NULLIF(selected_scope.allocation_basis_json->>'source_build_run_id','') IS NULL
       OR (
         v_source_publication_identity_enforce_enabled
         AND COALESCE(selected_scope.allocation_basis_json->>'source_publication_id','')
           !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       )
       OR (
         COALESCE(selected_scope.allocation_basis_json->>'source_publication_id','')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         AND NOT EXISTS (
           SELECT 1
           FROM public.banking_pay_workbench_session_scope AS source_scope
           WHERE source_scope.session_id=selected_scope.workbench_session_id
             AND source_scope.candidate_id=selected_scope.candidate_id
             AND source_scope.certified_preview_publication_parity_ok IS TRUE
             AND source_scope.certified_preview_publication_session_version=selected_scope.source_session_version
             AND source_scope.certified_preview_publication_source_publication_id=
                   (selected_scope.allocation_basis_json->>'source_publication_id')::uuid
             AND source_scope.certified_preview_publication_attestation_json->>'source_publication_id'=
                   selected_scope.allocation_basis_json->>'source_publication_id'
         )
       );

    IF v_semantic_source_failure_count > 0 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_SOURCE_SEMANTIC_CERTIFICATION_REQUIRED'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_DRAFT_SOURCE_SEMANTIC_CERTIFICATION_REQUIRED',
          'candidate_scope_count',v_semantic_source_failure_count,
          'message','Allocation seeding requires the V3 semantic source frozen by Draft scope creation.'
        )::text;
    END IF;

    WITH candidate_semantics AS (
      SELECT
        candidate_row.candidate_id,
        candidate_row.candidate_scope_id,
        ROUND(COALESCE(SUM(candidate_row.allocated_amount) FILTER (
          WHERE candidate_row.allocated_amount > 0
            AND candidate_row.allocation_type = 'TIMESHEET_PAYMENT'
            AND COALESCE((candidate_row.preview_contract_json->>'ok')::boolean, false)
            AND COALESCE((candidate_row.preview_contract_json->>'selection_allowed')::boolean, false)
        ), 0), 2) AS selected_positive_ordinary_amount,
        ROUND(COALESCE(SUM(candidate_row.allocated_amount) FILTER (
          WHERE candidate_row.allocated_amount < 0
            AND candidate_row.allocation_type IN (
              'OVERPAYMENT_RECOVERY',
              'MANUAL_DEBT_RECOVERY',
              'PAYMENT_ADVANCE_REPAYMENT',
              'LOAN_REPAYMENT'
            )
            AND candidate_row.finance_case_id IS NOT NULL
            AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
            AND COALESCE((candidate_row.preview_contract_json->>'ok')::boolean, false)
            AND COALESCE((candidate_row.preview_contract_json->>'selection_allowed')::boolean, false)
        ), 0), 2) AS selected_deduction_amount
      FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS candidate_row
      WHERE candidate_row.line_selected IS TRUE
        AND UPPER(COALESCE(candidate_row.selection_state, '')) = 'SELECTED'
        AND UPPER(COALESCE(candidate_row.line_status, '')) = 'READY'
      GROUP BY candidate_row.candidate_id, candidate_row.candidate_scope_id
    ), failed AS (
      SELECT
        candidate_semantics.*,
        ROUND(candidate_semantics.selected_positive_ordinary_amount
          + candidate_semantics.selected_deduction_amount, 2) AS selected_candidate_result,
        CASE
          WHEN candidate_semantics.selected_deduction_amount < 0
           AND candidate_semantics.selected_positive_ordinary_amount <= 0
            THEN 'PAY_WORKBENCH_DRAFT_RECOVERY_WITHOUT_POSITIVE_HEADROOM'
          WHEN -candidate_semantics.selected_deduction_amount
             > candidate_semantics.selected_positive_ordinary_amount
            THEN 'PAY_WORKBENCH_DRAFT_DEDUCTION_EXCEEDS_SELECTED_HEADROOM'
          WHEN candidate_semantics.selected_positive_ordinary_amount
             + candidate_semantics.selected_deduction_amount < 0
            THEN 'PAY_WORKBENCH_DRAFT_CANDIDATE_RESULT_NEGATIVE'
          ELSE NULL
        END AS failure_code
      FROM candidate_semantics
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'candidate_id', failed.candidate_id::text,
      'candidate_scope_id', failed.candidate_scope_id::text,
      'selected_positive_ordinary_amount', failed.selected_positive_ordinary_amount,
      'selected_deduction_amount', failed.selected_deduction_amount,
      'selected_candidate_result', failed.selected_candidate_result,
      'code', failed.failure_code
    ) ORDER BY failed.candidate_id), '[]'::jsonb)
    INTO v_semantic_draft_failures
    FROM failed
    WHERE failed.failure_code IS NOT NULL;

    IF jsonb_array_length(COALESCE(v_semantic_draft_failures, '[]'::jsonb)) > 0 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_SELECTED_SEMANTIC_PROOF_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_DRAFT_SELECTED_SEMANTIC_PROOF_FAILED',
                'operation_id', p_operation_id::text,
                'candidate_failures', v_semantic_draft_failures,
                'message', 'Draft selection requires positive ordinary entitlement for the same candidate, deductions within selected headroom, and a non-negative candidate result.'
              )::text;
    END IF;
  END IF;


  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', CASE WHEN synthetic_total_rows.preview_row_id IS NULL THEN NULL ELSE synthetic_total_rows.preview_row_id::text END,
           'row_key', synthetic_total_rows.row_key,
           'candidate_id', synthetic_total_rows.candidate_id::text,
           'candidate_scope_id', synthetic_total_rows.candidate_scope_id::text,
           'timesheet_id', CASE WHEN synthetic_total_rows.timesheet_id IS NULL THEN NULL ELSE synthetic_total_rows.timesheet_id::text END,
           'key_type', synthetic_total_rows.key_type,
           'key_value', synthetic_total_rows.key_value,
           'allocated_amount', synthetic_total_rows.allocated_amount,
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
         ) ORDER BY synthetic_total_rows.row_ordinal, synthetic_total_rows.preview_row_id), '[]'::jsonb)
  INTO v_synthetic_total_allocation_preview_rows
  FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS synthetic_total_rows
  WHERE UPPER(BTRIM(COALESCE(synthetic_total_rows.key_type, synthetic_total_rows.row_json#>>'{economic_key,key_type}', ''))) = 'TS_TOTAL'
    AND UPPER(BTRIM(COALESCE(synthetic_total_rows.key_value, synthetic_total_rows.row_json#>>'{economic_key,key_value}', ''))) = 'TOTAL'
    AND LOWER(BTRIM(COALESCE(synthetic_total_rows.source_ref, synthetic_total_rows.row_key, synthetic_total_rows.row_json->>'row_key', synthetic_total_rows.row_json->>'line_key', synthetic_total_rows.row_json->>'source_ref', ''))) LIKE '%:non_segment:total%'
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
        FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS sibling_segment
        WHERE sibling_segment.candidate_id = synthetic_total_rows.candidate_id
          AND sibling_segment.candidate_scope_id = synthetic_total_rows.candidate_scope_id
          AND sibling_segment.timesheet_id IS NOT DISTINCT FROM synthetic_total_rows.timesheet_id
          AND UPPER(BTRIM(COALESCE(sibling_segment.key_type, sibling_segment.row_json#>>'{economic_key,key_type}', ''))) = 'TS_DAY'
          AND LOWER(BTRIM(COALESCE(sibling_segment.source_ref, sibling_segment.row_key, sibling_segment.row_json->>'row_key', sibling_segment.row_json->>'line_key', sibling_segment.row_json->>'source_ref', ''))) LIKE '%:segment:%'
      )
    );

  IF jsonb_array_length(COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED',
              'operation_id', p_operation_id::text,
              'synthetic_total_allocation_preview_rows', COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb),
              'message', 'A stale resolved-timesheet synthetic total row reached allocation scope. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_allocation_expected_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_allocation_expected_rows ON COMMIT DROP AS
  WITH selected_preview_rows AS (
    SELECT candidate_row.*
    FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS candidate_row
    WHERE candidate_row.rotation_validation_failed IS NOT TRUE
      AND LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND candidate_row.preview_row_id IS NOT NULL
      AND COALESCE(candidate_row.backing_table_state_invalid, false) IS NOT TRUE
      AND lower(COALESCE(candidate_row.section, '')) = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(candidate_row.selection_state, ''))) = 'SELECTED'
      AND UPPER(BTRIM(COALESCE(candidate_row.line_status, ''))) = 'READY'
      AND COALESCE(candidate_row.line_selected, false) IS TRUE
      AND (COALESCE(candidate_row.is_delta_projection, false) IS NOT TRUE OR COALESCE(candidate_row.projection_certified, false) IS TRUE)
      AND COALESCE(candidate_row.policy_x_authority_scope, '') = 'PRE_DRAFT_LIVE_TRUTH'
      AND (candidate_row.timesheet_id IS NOT NULL OR candidate_row.finance_case_id IS NOT NULL)
      AND COALESCE(candidate_row.post_draft_overlay_unavailable, false) IS NOT TRUE
      AND candidate_row.pay_channel IN ('PAYE', 'UMBRELLA')
      AND (
        candidate_row.key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
        OR (candidate_row.finance_case_id IS NOT NULL AND candidate_row.key_type IN ('CASE_TOTAL','FINANCE_COMPONENT'))
      )
      AND candidate_row.key_value IS NOT NULL
      AND NOT (
        candidate_row.single_fixed_reimbursement_key_type IS NOT NULL
        AND (
          candidate_row.key_type IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_type
          OR candidate_row.key_value IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_value
        )
      )
      AND NOT (candidate_row.key_type = 'TS_DAY' AND candidate_row.key_value !~ '^\d{4}-\d{2}-\d{2}$')
      AND candidate_row.allocated_amount IS NOT NULL
      AND ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) <> 0
      AND (
        ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) > 0
        OR (
          candidate_row.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
          AND candidate_row.finance_case_id IS NOT NULL
          AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
        )
      )
  )
  , finance_component_source_rows AS (
    SELECT
      selected_preview_rows.*,
      component_element.ordinality::integer AS finance_component_ordinality,
      component_element.value AS finance_component_json,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(component_element.value->>'finance_component_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(component_element.value->>'finance_component_id', '')), '')::uuid
        ELSE selected_preview_rows.finance_component_id
      END AS effective_finance_component_id,
      UPPER(COALESCE(
        NULLIF(BTRIM(component_element.value->>'component_key_type'), ''),
        selected_preview_rows.key_type,
        'CASE_TOTAL'
      )) AS effective_key_type,
      COALESCE(
        NULLIF(BTRIM(component_element.value->>'component_key_value'), ''),
        selected_preview_rows.key_value,
        'TOTAL'
      ) AS effective_key_value,
      ROUND(COALESCE(
        CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'allocated_source_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'target_pay_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'ready_preview_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        ABS(COALESCE(selected_preview_rows.allocated_amount, 0))
      ), 2) AS raw_component_abs_amount,
      ROW_NUMBER() OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
        ORDER BY component_element.ordinality
      ) AS component_rn,
      COUNT(*) OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
      ) AS component_count,
      ROUND(SUM(ROUND(COALESCE(
        CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'allocated_source_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'target_pay_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'ready_preview_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        ABS(COALESCE(selected_preview_rows.allocated_amount, 0))
      ), 2)) OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
      ), 2) AS component_abs_amount_sum,
      ROUND(COALESCE(SUM(ROUND(COALESCE(
        CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'allocated_source_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'target_pay_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'ready_preview_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        ABS(COALESCE(selected_preview_rows.allocated_amount, 0))
      ), 2)) OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
        ORDER BY component_element.ordinality
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0), 2) AS preceding_component_abs_amount
    FROM selected_preview_rows
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN selected_preview_rows.finance_case_id IS NOT NULL
         AND jsonb_typeof(selected_preview_rows.row_json->'case_components') = 'array'
         AND jsonb_array_length(selected_preview_rows.row_json->'case_components') > 0
          THEN selected_preview_rows.row_json->'case_components'
        ELSE '[]'::jsonb
      END
    ) WITH ORDINALITY AS component_element(value, ordinality)
    WHERE selected_preview_rows.finance_case_id IS NOT NULL
      AND jsonb_typeof(component_element.value) = 'object'
  ), allocation_expanded_rows AS (
    SELECT
      selected_preview_rows.operation_id,
      selected_preview_rows.candidate_scope_id,
      selected_preview_rows.pay_batch_id,
      selected_preview_rows.candidate_id,
      selected_preview_rows.preview_row_id,
      selected_preview_rows.row_key,
      selected_preview_rows.timesheet_id,
      selected_preview_rows.key_type,
      selected_preview_rows.key_value,
      selected_preview_rows.pay_channel,
      selected_preview_rows.finance_case_id,
      selected_preview_rows.finance_component_id,
      selected_preview_rows.allocation_type,
      selected_preview_rows.source_ref,
      selected_preview_rows.operation_id::text || ':allocation:' || selected_preview_rows.candidate_scope_id::text || ':' || selected_preview_rows.preview_row_id::text AS operation_source_key,
      selected_preview_rows.allocated_amount,
      selected_preview_rows.preview_contract_json,
      selected_preview_rows.item_direction,
      selected_preview_rows.row_json,
      selected_preview_rows.row_ordinal,
      NULL::jsonb AS finance_component_json,
      NULL::integer AS finance_component_ordinality
    FROM selected_preview_rows
    WHERE selected_preview_rows.finance_case_id IS NULL
       OR NOT EXISTS (
         SELECT 1
         FROM finance_component_source_rows AS component_probe
         WHERE component_probe.operation_id = selected_preview_rows.operation_id
           AND component_probe.candidate_scope_id = selected_preview_rows.candidate_scope_id
           AND component_probe.preview_row_id = selected_preview_rows.preview_row_id
       )

    UNION ALL

    SELECT
      finance_component_source_rows.operation_id,
      finance_component_source_rows.candidate_scope_id,
      finance_component_source_rows.pay_batch_id,
      finance_component_source_rows.candidate_id,
      finance_component_source_rows.preview_row_id,
      finance_component_source_rows.row_key,
      finance_component_source_rows.timesheet_id,
      finance_component_source_rows.effective_key_type AS key_type,
      finance_component_source_rows.effective_key_value AS key_value,
      finance_component_source_rows.pay_channel,
      finance_component_source_rows.finance_case_id,
      finance_component_source_rows.effective_finance_component_id AS finance_component_id,
      finance_component_source_rows.allocation_type,
      finance_component_source_rows.source_ref,
      finance_component_source_rows.operation_id::text || ':allocation:' || finance_component_source_rows.candidate_scope_id::text || ':' || finance_component_source_rows.preview_row_id::text || ':component:' || COALESCE(finance_component_source_rows.effective_finance_component_id::text, md5(finance_component_source_rows.finance_component_json::text)) AS operation_source_key,
      CASE
        WHEN ROUND(COALESCE(finance_component_source_rows.allocated_amount, 0), 2) < 0 THEN -1
        ELSE 1
      END * ROUND(
        LEAST(
          COALESCE(finance_component_source_rows.raw_component_abs_amount, 0),
          GREATEST(
            ABS(COALESCE(finance_component_source_rows.allocated_amount, 0))
              - COALESCE(finance_component_source_rows.preceding_component_abs_amount, 0),
            0
          )
        ),
        2
      ) AS allocated_amount,
      finance_component_source_rows.preview_contract_json,
      finance_component_source_rows.item_direction,
      jsonb_strip_nulls(
        finance_component_source_rows.row_json
        || jsonb_build_object(
          'finance_component_id', CASE WHEN finance_component_source_rows.effective_finance_component_id IS NULL THEN NULL ELSE finance_component_source_rows.effective_finance_component_id::text END,
          'component_key_type', finance_component_source_rows.effective_key_type,
          'component_key_value', finance_component_source_rows.effective_key_value,
          'key_type', finance_component_source_rows.effective_key_type,
          'key_value', finance_component_source_rows.effective_key_value,
          'component', finance_component_source_rows.finance_component_json
        )
      ) AS row_json,
      (finance_component_source_rows.row_ordinal * 1000 + finance_component_source_rows.finance_component_ordinality)::bigint AS row_ordinal,
      finance_component_source_rows.finance_component_json,
      finance_component_source_rows.finance_component_ordinality
    FROM finance_component_source_rows
    WHERE ROUND(COALESCE(finance_component_source_rows.raw_component_abs_amount, 0), 2) > 0
  )
  SELECT
    allocation_expanded_rows.operation_id,
    allocation_expanded_rows.candidate_scope_id,
    allocation_expanded_rows.pay_batch_id,
    allocation_expanded_rows.candidate_id,
    allocation_expanded_rows.preview_row_id,
    allocation_expanded_rows.row_key,
    allocation_expanded_rows.timesheet_id,
    allocation_expanded_rows.key_type,
    allocation_expanded_rows.key_value,
    allocation_expanded_rows.pay_channel,
    allocation_expanded_rows.finance_case_id,
    allocation_expanded_rows.finance_component_id,
    allocation_expanded_rows.allocation_type,
    allocation_expanded_rows.source_ref,
    allocation_expanded_rows.operation_source_key,
    allocation_expanded_rows.allocated_amount,
    jsonb_build_object(
      'source', 'banking_pay_workbench_preview_rows',
      'preview_row_id', allocation_expanded_rows.preview_row_id::text,
      'row_key', allocation_expanded_rows.row_key,
      'row_ordinal', allocation_expanded_rows.row_ordinal,
      'timesheet_id', CASE WHEN allocation_expanded_rows.timesheet_id IS NULL THEN NULL ELSE allocation_expanded_rows.timesheet_id::text END,
      'key_type', allocation_expanded_rows.key_type,
      'key_value', allocation_expanded_rows.key_value,
      'allocation_type', allocation_expanded_rows.allocation_type,
      'item_direction', allocation_expanded_rows.item_direction,
      'finance_case_id', CASE WHEN allocation_expanded_rows.finance_case_id IS NULL THEN NULL ELSE allocation_expanded_rows.finance_case_id::text END,
      'finance_component_id', CASE WHEN allocation_expanded_rows.finance_component_id IS NULL THEN NULL ELSE allocation_expanded_rows.finance_component_id::text END,
      'finance_component', allocation_expanded_rows.finance_component_json,
      'economic_key', jsonb_strip_nulls(jsonb_build_object(
        'timesheet_id', CASE WHEN allocation_expanded_rows.timesheet_id IS NULL THEN NULL ELSE allocation_expanded_rows.timesheet_id::text END,
        'key_type', allocation_expanded_rows.key_type,
        'key_value', allocation_expanded_rows.key_value
      )),
      'preview_contract', allocation_expanded_rows.preview_contract_json,
      'line', jsonb_strip_nulls(
        allocation_expanded_rows.row_json
        || jsonb_build_object(
          'row_key', allocation_expanded_rows.row_key,
          'preview_row_pk', allocation_expanded_rows.preview_row_id::text,
          'selection_state', 'SELECTED'
        )
      )
    ) AS allocation_basis_json,
    row_number() OVER (
      PARTITION BY
        allocation_expanded_rows.operation_id,
        allocation_expanded_rows.candidate_scope_id
      ORDER BY
        allocation_expanded_rows.row_ordinal,
        allocation_expanded_rows.preview_row_id,
        allocation_expanded_rows.operation_source_key
    )::integer AS sort_order
  FROM allocation_expanded_rows
  WHERE ROUND(COALESCE(allocation_expanded_rows.allocated_amount, 0), 2) <> 0;


  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', synthetic_total_rows.preview_row_id::text,
           'row_key', synthetic_total_rows.row_key,
           'candidate_id', synthetic_total_rows.candidate_id::text,
           'candidate_scope_id', synthetic_total_rows.candidate_scope_id::text,
           'timesheet_id', CASE WHEN synthetic_total_rows.timesheet_id IS NULL THEN NULL ELSE synthetic_total_rows.timesheet_id::text END,
           'key_type', synthetic_total_rows.key_type,
           'key_value', synthetic_total_rows.key_value,
           'allocated_amount', synthetic_total_rows.allocated_amount,
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
         ) ORDER BY synthetic_total_rows.sort_order, synthetic_total_rows.preview_row_id), '[]'::jsonb)
  INTO v_synthetic_total_allocation_preview_rows
  FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS synthetic_total_rows
  WHERE UPPER(BTRIM(COALESCE(synthetic_total_rows.key_type, synthetic_total_rows.allocation_basis_json#>>'{economic_key,key_type}', ''))) = 'TS_TOTAL'
    AND UPPER(BTRIM(COALESCE(synthetic_total_rows.key_value, synthetic_total_rows.allocation_basis_json#>>'{economic_key,key_value}', ''))) = 'TOTAL'
    AND LOWER(BTRIM(COALESCE(synthetic_total_rows.source_ref, synthetic_total_rows.row_key, synthetic_total_rows.allocation_basis_json#>>'{line,row_key}', synthetic_total_rows.allocation_basis_json#>>'{line,line_key}', synthetic_total_rows.allocation_basis_json#>>'{line,source_ref}', ''))) LIKE '%:non_segment:total%'
    AND (
      lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,resolved_segment_rows_replace_source_total}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR (
        COALESCE(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
        AND (synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}')::integer > 0
      )
      OR EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS sibling_segment
        WHERE sibling_segment.candidate_id = synthetic_total_rows.candidate_id
          AND sibling_segment.candidate_scope_id = synthetic_total_rows.candidate_scope_id
          AND sibling_segment.timesheet_id IS NOT DISTINCT FROM synthetic_total_rows.timesheet_id
          AND UPPER(BTRIM(COALESCE(sibling_segment.key_type, sibling_segment.allocation_basis_json#>>'{economic_key,key_type}', ''))) = 'TS_DAY'
          AND LOWER(BTRIM(COALESCE(sibling_segment.source_ref, sibling_segment.row_key, sibling_segment.allocation_basis_json#>>'{line,row_key}', sibling_segment.allocation_basis_json#>>'{line,line_key}', sibling_segment.allocation_basis_json#>>'{line,source_ref}', ''))) LIKE '%:segment:%'
      )
    );

  IF jsonb_array_length(COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED',
              'operation_id', p_operation_id::text,
              'synthetic_total_allocation_preview_rows', COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb),
              'message', 'A stale resolved-timesheet synthetic total row reached allocation. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_expected_count
  FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS expected_row_count;

  IF COALESCE(v_expected_count, 0) = 0 AND EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
    WHERE COALESCE(selected_scope.candidate_totals_json->>'selected_row_count_seeded_in_page', '0') ~ '^[0-9]+$'
      AND (selected_scope.candidate_totals_json->>'selected_row_count_seeded_in_page')::integer > 0
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_candidate_allocation_rows AS existing_row
        WHERE existing_row.operation_id = p_operation_id
          AND existing_row.candidate_scope_id = selected_scope.id
      )
  ) THEN
    RAISE EXCEPTION 'DRAFT_ALLOCATION_ROWS_EMPTY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ALLOCATION_ROWS_EMPTY',
              'operation_id', p_operation_id::text,
              'candidate_scopes_processed', COALESCE(v_candidate_scopes_processed, 0),
              'message', 'Row-backed candidate scope did not contain draftable selected preview lines for allocation.'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_reused
  FROM public.banking_pay_operation_candidate_allocation_rows AS existing_row
  WHERE existing_row.operation_id = p_operation_id
    AND existing_row.candidate_scope_id IN (SELECT selected_scope.id FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope);

  IF COALESCE(v_expected_count, 0) > 0 THEN
    WITH inserted_rows AS (
      INSERT INTO public.banking_pay_operation_candidate_allocation_rows (
        operation_id,
        candidate_scope_id,
        pay_batch_id,
        candidate_id,
        pay_channel,
        finance_case_id,
        finance_component_id,
        allocation_type,
        source_ref,
        operation_source_key,
        allocated_amount,
        allocation_basis_json,
        sort_order,
        status,
        created_at_utc,
        updated_at_utc
      )
      SELECT
        expected_row.operation_id,
        expected_row.candidate_scope_id,
        expected_row.pay_batch_id,
        expected_row.candidate_id,
        expected_row.pay_channel,
        expected_row.finance_case_id,
        expected_row.finance_component_id,
        expected_row.allocation_type,
        expected_row.source_ref,
        expected_row.operation_source_key,
        expected_row.allocated_amount,
        expected_row.allocation_basis_json,
        expected_row.sort_order,
        'PENDING',
        v_now,
        v_now
      FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS expected_row
      ON CONFLICT (operation_id, operation_source_key) DO NOTHING
      RETURNING public.banking_pay_operation_candidate_allocation_rows.id
    )
    SELECT COUNT(*)::integer
    INTO v_inserted
    FROM inserted_rows;
  ELSE
    v_inserted := 0;
  END IF;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'allocation_source_key', plan_row.allocation_source_key,
           'existing_plan_digest', existing_row.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}',
           'current_plan_digest', plan_row.plan_digest
         ) ORDER BY plan_row.allocation_source_key), '[]'::jsonb)
  INTO v_plan_drift_details
  FROM private.pay_workbench_draft_finance_item_plan_v1(p_operation_id, v_plan_scope_ids) AS plan_row
  JOIN public.banking_pay_operation_candidate_allocation_rows AS existing_row
    ON existing_row.operation_id = plan_row.operation_id
   AND existing_row.operation_source_key = plan_row.allocation_source_key
  WHERE existing_row.allocation_basis_json ? 'draft_finance_item_plan'
    AND NULLIF(existing_row.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}', '')
        IS DISTINCT FROM plan_row.plan_digest;

  IF jsonb_array_length(COALESCE(v_plan_drift_details, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'DRAFT_FINANCE_ITEM_PLAN_DRIFT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_FINANCE_ITEM_PLAN_DRIFT',
              'operation_id', p_operation_id::text,
              'mismatches', v_plan_drift_details,
              'message', 'Frozen Draft finance allocation evidence no longer matches its canonical item plan.'
            )::text;
  END IF;

  WITH current_plan AS (
    SELECT *
    FROM private.pay_workbench_draft_finance_item_plan_v1(p_operation_id, v_plan_scope_ids)
  )
  UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_update
  SET allocation_basis_json = COALESCE(allocation_update.allocation_basis_json, '{}'::jsonb)
        || jsonb_build_object(
             'draft_finance_item_plan',
             jsonb_build_object(
               'contract_version', 1,
               'planned_item_key', current_plan.planned_item_key,
               'planned_item_type', current_plan.planned_item_type,
               'contribution_amount', current_plan.contribution_amount,
               'planned_item_amount', current_plan.planned_item_amount,
               'contribution_count', current_plan.contribution_count,
               'plan_digest', current_plan.plan_digest
             )
           ),
      updated_at_utc = v_now
  FROM current_plan
  WHERE allocation_update.operation_id = current_plan.operation_id
    AND allocation_update.operation_source_key = current_plan.allocation_source_key
    AND allocation_update.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}' IS DISTINCT FROM current_plan.plan_digest;

  UPDATE public.banking_pay_operation_candidate_scope AS scope_update
  SET status = CASE
        WHEN EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_candidate_allocation_rows AS existing_row
          WHERE existing_row.operation_id = scope_update.operation_id
            AND existing_row.candidate_scope_id = scope_update.id
        ) THEN 'ALLOCATED'
        ELSE scope_update.status
      END,
      updated_at_utc = v_now
  WHERE scope_update.operation_id = p_operation_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
      WHERE selected_scope.id = scope_update.id
    );

  UPDATE public.banking_pay_operations AS operation_update
  SET updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN QUERY
  SELECT
    COALESCE(v_candidate_scopes_processed, 0),
    COALESCE(v_inserted, 0),
    COALESCE(v_reused, 0),
    0::integer;
END;
$function$;
