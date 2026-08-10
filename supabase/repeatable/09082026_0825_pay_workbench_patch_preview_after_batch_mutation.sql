-- Focused authority for semantic/cancellation-safe batch mutation patching.

CREATE OR REPLACE FUNCTION public.pay_workbench_patch_preview_after_batch_mutation(p_session_id uuid, p_pay_batch_id uuid, p_operation_type text, p_actor_user_id uuid DEFAULT NULL::uuid, p_options_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_options_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_options_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_operation_type text := UPPER(BTRIM(COALESCE(p_operation_type, '')));
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_patch_enabled boolean := true;
  v_affected_candidate_count integer := 0;
  v_affected_row_count integer := 0;
  v_patched_row_count integer := 0;
  v_targeted_refresh_enqueued_count integer := 0;
  v_complex_candidate_count integer := 0;
  v_missing_key_candidate_count integer := 0;
  v_complex_refresh_candidate_ids jsonb := '[]'::jsonb;
  v_missing_key_candidate_ids jsonb := '[]'::jsonb;
  v_refresh_candidate_row record;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_enqueue_job_id_text text := NULL::text;
  v_enqueue_job_id uuid := NULL::uuid;
  v_enqueue_job_type text := NULL::text;
  v_enqueue_job_status text := NULL::text;
  v_enqueue_source_build_run_id_text text := NULL::text;
  v_session_preview_row_count integer := 0;
  v_session_selected_row_count integer := 0;
  v_session_section_counts_json jsonb := '{}'::jsonb;
  v_session_candidate_samples_json jsonb := '[]'::jsonb;
  v_affected_candidate_ids_json jsonb := '[]'::jsonb;
  v_patched_row_ids_json jsonb := '[]'::jsonb;
  v_affected_timesheet_ids_json jsonb := '[]'::jsonb;
  v_affected_economic_keys_json jsonb := '[]'::jsonb;
  v_targeted_refresh_candidate_ids_json jsonb := '[]'::jsonb;
  v_candidate_state_results_json jsonb := '[]'::jsonb;
  v_candidate_state_result jsonb := '{}'::jsonb;
  v_candidate_state_candidate_id uuid := NULL::uuid;
  v_patch_projection_run_id uuid := gen_random_uuid();
  v_selected_before_count integer := 0;
  v_selected_after_count integer := 0;
  v_ready_before_count integer := 0;
  v_ready_after_count integer := 0;
  v_selected_count_delta integer := 0;
  v_ready_count_delta integer := 0;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_is_cancel_delete boolean := false;
  v_supplied_item_scope_count integer := 0;
  v_exact_item_scope_count integer := 0;
  v_invalid_item_scope_count integer := 0;
  v_out_of_batch_item_scope_count integer := 0;
  v_has_specific_scope_selector boolean := false;
  v_whole_batch_scope_proven boolean := false;
  v_cancel_delete_total_item_count integer := 0;
  v_cancel_delete_voided_item_count integer := 0;
  v_scope_resolution_mode text := NULL::text;
  v_scope_diagnostic_json jsonb := '{}'::jsonb;
  v_defer_complex_enqueue boolean := lower(btrim(COALESCE(v_options_json->>'defer_complex_enqueue','false')))
    IN ('true','t','1','yes','y','on');
  v_draft_adoption_result jsonb := '{}'::jsonb;
  v_draft_operation_id uuid := NULL::uuid;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_session_id IS NULL OR p_pay_batch_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'SESSION_ID_AND_PAY_BATCH_ID_REQUIRED',
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb
    );
  END IF;

  IF v_operation_type NOT IN ('DRAFT_CREATE', 'DRAFT_DELETE', 'DRAFT_CANCEL', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE') THEN
    RETURN jsonb_build_object(
      'ok', false,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'UNSUPPORTED_BATCH_MUTATION_OPERATION',
      'operation_type', v_operation_type,
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb
    );
  END IF;

  v_is_cancel_delete := v_operation_type IN ('DRAFT_DELETE', 'DRAFT_CANCEL');

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
    AND UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
    AND session_row.discarded_at_utc IS NULL
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'WORKBENCH_SESSION_NOT_OPEN',
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb
    );
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'PAY_BATCH_NOT_FOUND',
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb
    );
  END IF;

  IF v_batch_row.source_workbench_session_id IS NOT NULL
     AND v_batch_row.source_workbench_session_id IS DISTINCT FROM p_session_id
     AND lower(BTRIM(COALESCE(v_options_json->>'allow_cross_session_patch', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    RETURN jsonb_build_object(
      'ok', false,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'PAY_BATCH_SESSION_MISMATCH',
      'source_workbench_session_id', v_batch_row.source_workbench_session_id::text,
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb
    );
  END IF;

  SELECT lower(BTRIM(COALESCE(to_jsonb(settings_row)->>'banking_pay_workbench_patch_after_batch_mutation_enabled', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on')
  INTO v_patch_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  LIMIT 1;

  v_patch_enabled := COALESCE(v_patch_enabled, true);

  IF v_patch_enabled IS NOT TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'PATCH_AFTER_BATCH_MUTATION_DISABLED',
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_supplied_item_ids;
  CREATE TEMP TABLE _bpay_batch_mutation_supplied_item_ids ON COMMIT DROP AS
  WITH option_item_arrays(path_name, value_json) AS (
    VALUES
      ('changed_pay_batch_item_ids', v_options_json->'changed_pay_batch_item_ids'),
      ('cancelled_pay_batch_item_ids', v_options_json->'cancelled_pay_batch_item_ids'),
      ('voided_pay_batch_item_ids', v_options_json->'voided_pay_batch_item_ids'),
      ('pay_batch_item_ids', v_options_json->'pay_batch_item_ids'),
      ('selected_pay_batch_item_ids', v_options_json->'selected_pay_batch_item_ids'),
      ('expected_pay_batch_item_ids', v_options_json->'expected_pay_batch_item_ids'),
      ('cancellation_result.changed_pay_batch_item_ids', v_options_json#>'{cancellation_result,changed_pay_batch_item_ids}'),
      ('cancellation_result.cancelled_pay_batch_item_ids', v_options_json#>'{cancellation_result,cancelled_pay_batch_item_ids}'),
      ('cancellation_result.voided_pay_batch_item_ids', v_options_json#>'{cancellation_result,voided_pay_batch_item_ids}'),
      ('cancellation_result.process_result.changed_pay_batch_item_ids', v_options_json#>'{cancellation_result,process_result,changed_pay_batch_item_ids}'),
      ('cancellation_result.process_result.changed_scope_json.changed_pay_batch_item_ids', v_options_json#>'{cancellation_result,process_result,changed_scope_json,changed_pay_batch_item_ids}'),
      ('cancellation_result.changed_scope_json.changed_pay_batch_item_ids', v_options_json#>'{cancellation_result,changed_scope_json,changed_pay_batch_item_ids}'),
      ('process_result.changed_pay_batch_item_ids', v_options_json#>'{process_result,changed_pay_batch_item_ids}'),
      ('process_result.changed_scope_json.changed_pay_batch_item_ids', v_options_json#>'{process_result,changed_scope_json,changed_pay_batch_item_ids}'),
      ('result.changed_pay_batch_item_ids', v_options_json#>'{result,changed_pay_batch_item_ids}'),
      ('result.changed_scope_json.changed_pay_batch_item_ids', v_options_json#>'{result,changed_scope_json,changed_pay_batch_item_ids}'),
      ('result.process_result.changed_pay_batch_item_ids', v_options_json#>'{result,process_result,changed_pay_batch_item_ids}'),
      ('result.process_result.changed_scope_json.changed_pay_batch_item_ids', v_options_json#>'{result,process_result,changed_scope_json,changed_pay_batch_item_ids}'),
      ('selection_json.pay_batch_item_ids', v_options_json#>'{selection_json,pay_batch_item_ids}'),
      ('selection_json.selected_pay_batch_item_ids', v_options_json#>'{selection_json,selected_pay_batch_item_ids}'),
      ('selection_json.expected_pay_batch_item_ids', v_options_json#>'{selection_json,expected_pay_batch_item_ids}'),
      ('confirmation_json.pay_batch_item_ids', v_options_json#>'{confirmation_json,pay_batch_item_ids}'),
      ('confirmation_json.selected_pay_batch_item_ids', v_options_json#>'{confirmation_json,selected_pay_batch_item_ids}'),
      ('confirmation_json.expected_pay_batch_item_ids', v_options_json#>'{confirmation_json,expected_pay_batch_item_ids}')
  ), raw_item_values AS (
    SELECT option_item_arrays.path_name,
           item_array_values.raw_value
    FROM option_item_arrays
    CROSS JOIN LATERAL jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(option_item_arrays.value_json) = 'array' THEN option_item_arrays.value_json
        ELSE '[]'::jsonb
      END
    ) AS item_array_values(raw_value)
    UNION ALL
    SELECT option_item_arrays.path_name,
           TRIM(BOTH '"' FROM option_item_arrays.value_json::text) AS raw_value
    FROM option_item_arrays
    WHERE jsonb_typeof(option_item_arrays.value_json) = 'string'
  ), cleaned_item_values AS (
    SELECT raw_item_values.path_name,
           NULLIF(BTRIM(COALESCE(raw_item_values.raw_value, '')), '') AS clean_value
    FROM raw_item_values
  )
  SELECT DISTINCT
    cleaned_item_values.path_name,
    cleaned_item_values.clean_value AS raw_pay_batch_item_id,
    CASE
      WHEN cleaned_item_values.clean_value ~* v_uuid_regex THEN cleaned_item_values.clean_value::uuid
      ELSE NULL::uuid
    END AS pay_batch_item_id
  FROM cleaned_item_values
  WHERE cleaned_item_values.clean_value IS NOT NULL;

  SELECT COUNT(DISTINCT supplied_item.pay_batch_item_id)::integer
  INTO v_supplied_item_scope_count
  FROM pg_temp._bpay_batch_mutation_supplied_item_ids AS supplied_item
  WHERE supplied_item.pay_batch_item_id IS NOT NULL;

  SELECT COUNT(DISTINCT supplied_item.raw_pay_batch_item_id)::integer
  INTO v_invalid_item_scope_count
  FROM pg_temp._bpay_batch_mutation_supplied_item_ids AS supplied_item
  WHERE supplied_item.pay_batch_item_id IS NULL;

  DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_exact_item_ids;
  CREATE TEMP TABLE _bpay_batch_mutation_exact_item_ids ON COMMIT DROP AS
  SELECT DISTINCT supplied_item.pay_batch_item_id
  FROM pg_temp._bpay_batch_mutation_supplied_item_ids AS supplied_item
  JOIN public.pay_batch_items AS exact_batch_item
    ON exact_batch_item.id = supplied_item.pay_batch_item_id
  JOIN public.pay_batch_candidates AS exact_batch_candidate
    ON exact_batch_candidate.id = exact_batch_item.pay_batch_candidate_id
  WHERE exact_batch_candidate.pay_batch_id = p_pay_batch_id
    AND supplied_item.pay_batch_item_id IS NOT NULL;

  SELECT COUNT(*)::integer
  INTO v_exact_item_scope_count
  FROM pg_temp._bpay_batch_mutation_exact_item_ids AS exact_item;

  v_out_of_batch_item_scope_count := GREATEST(COALESCE(v_supplied_item_scope_count, 0) - COALESCE(v_exact_item_scope_count, 0), 0);

  SELECT EXISTS (
    WITH option_scope_objects(path_name, object_json) AS (
      VALUES
        ('root', v_options_json),
        ('selection_json', v_options_json->'selection_json'),
        ('confirmation_json', v_options_json->'confirmation_json'),
        ('cancellation_result', v_options_json->'cancellation_result'),
        ('process_result', v_options_json->'process_result'),
        ('result', v_options_json->'result')
    )
    SELECT 1
    FROM option_scope_objects
    CROSS JOIN LATERAL jsonb_each(
      CASE
        WHEN jsonb_typeof(option_scope_objects.object_json) = 'object' THEN option_scope_objects.object_json
        ELSE '{}'::jsonb
      END
    ) AS scope_entry(selector_key, selector_value)
    WHERE scope_entry.selector_key = ANY(ARRAY[
      'pay_batch_candidate_id',
      'pay_batch_candidate_ids',
      'payBatchCandidateId',
      'payBatchCandidateIds',
      'pay_bank_transfer_id',
      'pay_bank_transfer_ids',
      'payBankTransferId',
      'payBankTransferIds',
      'pay_batch_item_id',
      'pay_batch_item_ids',
      'payBatchItemId',
      'payBatchItemIds',
      'selected_pay_batch_item_ids',
      'expected_pay_batch_item_ids',
      'candidate_id',
      'candidate_ids',
      'candidateId',
      'candidateIds',
      'umbrella_id',
      'umbrella_ids',
      'umbrellaId',
      'umbrellaIds',
      'transfer_group_key',
      'transfer_group_keys',
      'transferGroupKey',
      'transferGroupKeys',
      'finance_case_id',
      'finance_case_ids',
      'financeCaseId',
      'financeCaseIds',
      'finance_component_id',
      'finance_component_ids',
      'financeComponentId',
      'financeComponentIds',
      'reservation_id',
      'reservation_ids',
      'reservationId',
      'reservationIds',
      'item_type',
      'item_types',
      'selected_scope_keys',
      'selectedScopeKeys',
      'selected_row_keys',
      'selectedRowKeys',
      'current_page_row_keys',
      'currentPageRowKeys'
    ]::text[])
      AND (
        (jsonb_typeof(scope_entry.selector_value) = 'array' AND jsonb_array_length(scope_entry.selector_value) > 0)
        OR (jsonb_typeof(scope_entry.selector_value) = 'string' AND NULLIF(TRIM(BOTH '"' FROM scope_entry.selector_value::text), '') IS NOT NULL)
        OR jsonb_typeof(scope_entry.selector_value) IN ('number', 'boolean', 'object')
      )
  )
  INTO v_has_specific_scope_selector;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE COALESCE(batch_item.is_voided, false) IS TRUE)::integer
  INTO v_cancel_delete_total_item_count,
       v_cancel_delete_voided_item_count
  FROM public.pay_batch_candidates AS batch_candidate
  JOIN public.pay_batch_items AS batch_item
    ON batch_item.pay_batch_candidate_id = batch_candidate.id
  WHERE batch_candidate.pay_batch_id = p_pay_batch_id;

  WITH scope_values(raw_value) AS (
    VALUES
      (v_options_json->>'scope_type'),
      (v_options_json->>'scopeType'),
      (v_options_json->>'scope'),
      (v_options_json->>'cancel_scope'),
      (v_options_json->>'cancelScope'),
      (v_options_json->>'operation_scope'),
      (v_options_json->>'operationScope'),
      (v_options_json#>>'{selection_json,scope_type}'),
      (v_options_json#>>'{selection_json,scopeType}'),
      (v_options_json#>>'{selection_json,scope}'),
      (v_options_json#>>'{selection_json,cancel_scope}'),
      (v_options_json#>>'{selection_json,cancelScope}'),
      (v_options_json#>>'{selection_json,operation_scope}'),
      (v_options_json#>>'{selection_json,operationScope}'),
      (v_options_json#>>'{confirmation_json,scope_type}'),
      (v_options_json#>>'{confirmation_json,scopeType}'),
      (v_options_json#>>'{confirmation_json,scope}'),
      (v_options_json#>>'{confirmation_json,cancel_scope}'),
      (v_options_json#>>'{confirmation_json,cancelScope}'),
      (v_options_json#>>'{confirmation_json,operation_scope}'),
      (v_options_json#>>'{confirmation_json,operationScope}'),
      (v_options_json#>>'{cancellation_result,scope_type}'),
      (v_options_json#>>'{cancellation_result,scopeType}'),
      (v_options_json#>>'{cancellation_result,scope}'),
      (v_options_json#>>'{cancellation_result,cancel_scope}'),
      (v_options_json#>>'{cancellation_result,cancelScope}'),
      (v_options_json#>>'{cancellation_result,operation_scope}'),
      (v_options_json#>>'{cancellation_result,operationScope}')
  ), whole_batch_flags(raw_value) AS (
    VALUES
      (v_options_json->>'whole_batch'),
      (v_options_json->>'wholeBatch'),
      (v_options_json->>'all_items'),
      (v_options_json->>'allItems'),
      (v_options_json->>'delete_entire_batch'),
      (v_options_json->>'deleteEntireBatch'),
      (v_options_json->>'whole_batch_overview_action'),
      (v_options_json->>'wholeBatchOverviewAction'),
      (v_options_json#>>'{selection_json,whole_batch}'),
      (v_options_json#>>'{selection_json,wholeBatch}'),
      (v_options_json#>>'{selection_json,all_items}'),
      (v_options_json#>>'{selection_json,allItems}'),
      (v_options_json#>>'{selection_json,delete_entire_batch}'),
      (v_options_json#>>'{selection_json,deleteEntireBatch}'),
      (v_options_json#>>'{selection_json,whole_batch_overview_action}'),
      (v_options_json#>>'{selection_json,wholeBatchOverviewAction}'),
      (v_options_json#>>'{confirmation_json,whole_batch}'),
      (v_options_json#>>'{confirmation_json,wholeBatch}'),
      (v_options_json#>>'{confirmation_json,all_items}'),
      (v_options_json#>>'{confirmation_json,allItems}'),
      (v_options_json#>>'{confirmation_json,delete_entire_batch}'),
      (v_options_json#>>'{confirmation_json,deleteEntireBatch}'),
      (v_options_json#>>'{confirmation_json,whole_batch_overview_action}'),
      (v_options_json#>>'{confirmation_json,wholeBatchOverviewAction}'),
      (v_options_json#>>'{cancellation_result,whole_batch}'),
      (v_options_json#>>'{cancellation_result,wholeBatch}'),
      (v_options_json#>>'{cancellation_result,all_items}'),
      (v_options_json#>>'{cancellation_result,allItems}'),
      (v_options_json#>>'{cancellation_result,delete_entire_batch}'),
      (v_options_json#>>'{cancellation_result,deleteEntireBatch}'),
      (v_options_json#>>'{cancellation_result,whole_batch_overview_action}'),
      (v_options_json#>>'{cancellation_result,wholeBatchOverviewAction}')
  )
  SELECT (
    v_is_cancel_delete IS TRUE
    AND COALESCE(v_exact_item_scope_count, 0) = 0
    AND COALESCE(v_has_specific_scope_selector, false) IS NOT TRUE
    AND (
      EXISTS (
        SELECT 1
        FROM scope_values AS scope_value
        WHERE UPPER(BTRIM(COALESCE(scope_value.raw_value, ''))) IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH')
      )
      OR EXISTS (
        SELECT 1
        FROM whole_batch_flags AS whole_batch_flag
        WHERE lower(BTRIM(COALESCE(whole_batch_flag.raw_value, ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
      )
    )
  )
  INTO v_whole_batch_scope_proven;

  v_scope_resolution_mode := CASE
    WHEN v_is_cancel_delete IS NOT TRUE THEN 'ACTIVE_NON_VOIDED_ITEMS'
    WHEN COALESCE(v_exact_item_scope_count, 0) > 0 THEN 'EXACT_ITEM_IDS'
    WHEN COALESCE(v_whole_batch_scope_proven, false) IS TRUE THEN 'WHOLE_BATCH_VOIDED_ITEMS'
    ELSE 'UNPROVEN_CANCEL_DELETE_SCOPE'
  END;

  v_scope_diagnostic_json := jsonb_build_object(
    'scope_resolution_mode', v_scope_resolution_mode,
    'operation_type', v_operation_type,
    'supplied_item_scope_count', COALESCE(v_supplied_item_scope_count, 0),
    'exact_item_scope_count', COALESCE(v_exact_item_scope_count, 0),
    'invalid_item_scope_count', COALESCE(v_invalid_item_scope_count, 0),
    'out_of_batch_item_scope_count', COALESCE(v_out_of_batch_item_scope_count, 0),
    'has_specific_scope_selector', COALESCE(v_has_specific_scope_selector, false),
    'whole_batch_scope_proven', COALESCE(v_whole_batch_scope_proven, false),
    'batch_item_count', COALESCE(v_cancel_delete_total_item_count, 0),
    'voided_batch_item_count', COALESCE(v_cancel_delete_voided_item_count, 0)
  );

  IF v_is_cancel_delete IS TRUE
     AND COALESCE(v_supplied_item_scope_count, 0) > 0
     AND (COALESCE(v_invalid_item_scope_count, 0) > 0 OR COALESCE(v_out_of_batch_item_scope_count, 0) > 0) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'DRAFT_CANCEL_DELETE_INVALID_OR_OUT_OF_BATCH_ITEM_SCOPE',
      'operation_type', v_operation_type,
      'pay_batch_id', p_pay_batch_id::text,
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb,
      'scope_resolution_mode', v_scope_resolution_mode,
      'scope_diagnostic', v_scope_diagnostic_json
    );
  END IF;

  IF v_is_cancel_delete IS TRUE
     AND COALESCE(v_exact_item_scope_count, 0) = 0
     AND COALESCE(v_whole_batch_scope_proven, false) IS NOT TRUE THEN
    RETURN jsonb_build_object(
      'ok', false,
      'patch_applied', false,
      'replacement_session_required', false,
      'fallback_required', true,
      'fallback_reason', 'DRAFT_CANCEL_DELETE_ITEM_SCOPE_NOT_PROVEN',
      'operation_type', v_operation_type,
      'pay_batch_id', p_pay_batch_id::text,
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb,
      'scope_resolution_mode', v_scope_resolution_mode,
      'scope_diagnostic', v_scope_diagnostic_json
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_keys;
  CREATE TEMP TABLE _bpay_batch_mutation_keys ON COMMIT DROP AS
  SELECT DISTINCT
    batch_candidate.candidate_id,
    COALESCE(
      batch_item.timesheet_id,
      CASE
        WHEN UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
         AND NULLIF(BTRIM(COALESCE(batch_item.frozen_source_basis_json->>'timesheet_id', '')), '')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (BTRIM(batch_item.frozen_source_basis_json->>'timesheet_id'))::uuid
        ELSE NULL::uuid
      END
    ) AS timesheet_id,
    NULLIF(BTRIM(COALESCE(
      batch_item.frozen_component_key_type,
      batch_item.frozen_source_basis_json->>'key_type',
      batch_item.frozen_source_basis_json->>'component_key_type',
      batch_item.frozen_component_snapshot_json->>'key_type',
      batch_item.frozen_component_snapshot_json->>'component_key_type',
      CASE
        WHEN batch_item.item_type IN ('SEGMENT_DELTA', 'TIMESHEET_DELTA') THEN 'TS_TOTAL'
        WHEN batch_item.item_type IN ('MILEAGE_DELTA') THEN 'EXPENSE_CODE'
        WHEN batch_item.item_type IN ('EXPENSE_DELTA') THEN 'EXPENSE_CODE'
        ELSE NULL::text
      END
    )), '') AS key_type,
    NULLIF(BTRIM(COALESCE(
      batch_item.frozen_component_key_value,
      batch_item.frozen_source_basis_json->>'key_value',
      batch_item.frozen_source_basis_json->>'component_key_value',
      batch_item.frozen_component_snapshot_json->>'key_value',
      batch_item.frozen_component_snapshot_json->>'component_key_value',
      batch_item.segment_key,
      batch_item.source_ref,
      CASE WHEN batch_item.item_type = 'MILEAGE_DELTA' THEN 'MILEAGE' ELSE NULL::text END
    )), '') AS key_value,
    p_pay_batch_id AS pay_batch_id,
    batch_item.id AS pay_batch_item_id,
    COALESCE(batch_item.is_voided, false) AS batch_item_is_voided,
    batch_item.finance_case_id,
    batch_item.item_type,
    batch_item.pay_channel
  FROM public.pay_batch_candidates AS batch_candidate
  JOIN public.pay_batch_items AS batch_item
    ON batch_item.pay_batch_candidate_id = batch_candidate.id
  WHERE batch_candidate.pay_batch_id = p_pay_batch_id
    AND (
      (
        v_is_cancel_delete IS NOT TRUE
        AND COALESCE(batch_item.is_voided, false) IS NOT TRUE
      )
      OR (
        v_is_cancel_delete IS TRUE
        AND COALESCE(v_exact_item_scope_count, 0) > 0
        AND EXISTS (
          SELECT 1
          FROM pg_temp._bpay_batch_mutation_exact_item_ids AS exact_item
          WHERE exact_item.pay_batch_item_id = batch_item.id
        )
      )
      OR (
        v_is_cancel_delete IS TRUE
        AND COALESCE(v_exact_item_scope_count, 0) = 0
        AND COALESCE(v_whole_batch_scope_proven, false) IS TRUE
        AND COALESCE(batch_item.is_voided, false) IS TRUE
      )
    );

  SELECT COUNT(DISTINCT affected_key.candidate_id)::integer
  INTO v_affected_candidate_count
  FROM pg_temp._bpay_batch_mutation_keys AS affected_key;

  SELECT COALESCE(jsonb_agg(DISTINCT affected_key.candidate_id::text ORDER BY affected_key.candidate_id::text), '[]'::jsonb)
  INTO v_affected_candidate_ids_json
  FROM pg_temp._bpay_batch_mutation_keys AS affected_key
  WHERE affected_key.candidate_id IS NOT NULL;

  SELECT COALESCE(jsonb_agg(DISTINCT affected_key.timesheet_id::text ORDER BY affected_key.timesheet_id::text), '[]'::jsonb)
  INTO v_affected_timesheet_ids_json
  FROM pg_temp._bpay_batch_mutation_keys AS affected_key
  WHERE affected_key.timesheet_id IS NOT NULL;

  SELECT COALESCE(jsonb_agg(economic_key_rows.economic_key_json ORDER BY economic_key_rows.economic_key_json::text), '[]'::jsonb)
  INTO v_affected_economic_keys_json
  FROM (
    SELECT DISTINCT jsonb_build_object(
      'candidate_id', affected_key.candidate_id::text,
      'timesheet_id', CASE WHEN affected_key.timesheet_id IS NULL THEN NULL ELSE affected_key.timesheet_id::text END,
      'key_type', affected_key.key_type,
      'key_value', affected_key.key_value
    ) AS economic_key_json
    FROM pg_temp._bpay_batch_mutation_keys AS affected_key
    WHERE affected_key.key_type IS NOT NULL
      AND affected_key.key_value IS NOT NULL
  ) AS economic_key_rows;

  IF COALESCE(v_affected_candidate_count, 0) = 0 THEN
    IF v_is_cancel_delete IS TRUE
       AND COALESCE(v_cancel_delete_voided_item_count, 0) > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'patch_applied', false,
        'replacement_session_required', false,
        'fallback_required', true,
        'fallback_reason', 'DRAFT_CANCEL_DELETE_PATCH_SCOPE_EMPTY_WITH_VOIDED_ITEMS',
        'operation_type', v_operation_type,
        'pay_batch_id', p_pay_batch_id::text,
        'affected_candidate_count', 0,
        'affected_row_count', 0,
        'patched_row_count', 0,
        'targeted_refresh_enqueued_count', 0,
        'complex_refresh_candidate_ids', '[]'::jsonb,
        'scope_resolution_mode', v_scope_resolution_mode,
        'scope_diagnostic', v_scope_diagnostic_json
      );
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'patch_applied', true,
      'replacement_session_required', false,
      'affected_candidate_count', 0,
      'affected_row_count', 0,
      'patched_row_count', 0,
      'targeted_refresh_enqueued_count', 0,
      'complex_refresh_candidate_ids', '[]'::jsonb,
      'scope_resolution_mode', v_scope_resolution_mode,
      'scope_diagnostic', v_scope_diagnostic_json
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_candidates;
  CREATE TEMP TABLE _bpay_batch_mutation_candidates ON COMMIT DROP AS
  SELECT
    candidate_keys.candidate_id,
    COALESCE(array_agg(DISTINCT candidate_keys.timesheet_id ORDER BY candidate_keys.timesheet_id) FILTER (WHERE candidate_keys.timesheet_id IS NOT NULL), ARRAY[]::uuid[]) AS timesheet_ids,
    bool_or(candidate_keys.key_type IS NULL OR candidate_keys.key_value IS NULL OR candidate_keys.timesheet_id IS NULL) AS has_missing_economic_key,
    bool_or(candidate_keys.finance_case_id IS NOT NULL) AS has_batch_finance_case
  FROM pg_temp._bpay_batch_mutation_keys AS candidate_keys
  GROUP BY candidate_keys.candidate_id;

  ALTER TABLE pg_temp._bpay_batch_mutation_candidates
    ADD COLUMN has_complexity boolean NOT NULL DEFAULT false,
    ADD COLUMN targeted_refresh_job_id uuid NULL;

  UPDATE pg_temp._bpay_batch_mutation_candidates AS candidate_update
  SET has_complexity = COALESCE(candidate_update.has_batch_finance_case, false)
    OR EXISTS (
      SELECT 1
      FROM public.pay_finance_case_components AS component_row
      WHERE component_row.candidate_id = candidate_update.candidate_id
        AND component_row.closed_at_utc IS NULL
        AND (
          COALESCE(array_length(candidate_update.timesheet_ids, 1), 0) = 0
          OR component_row.linked_timesheet_id IS NULL
          OR component_row.linked_timesheet_id = ANY(candidate_update.timesheet_ids)
        )
    )
    OR EXISTS (
      SELECT 1
      FROM public.pay_advances AS advance_row
      WHERE advance_row.candidate_id = candidate_update.candidate_id
        AND UPPER(COALESCE(advance_row.status::text, '')) IN ('ACTIVE', 'PAUSED')
        AND advance_row.cleared_at_utc IS NULL
        AND advance_row.written_off_at_utc IS NULL
        AND (
          COALESCE(array_length(candidate_update.timesheet_ids, 1), 0) = 0
          OR advance_row.linked_timesheet_id IS NULL
          OR advance_row.linked_timesheet_id = ANY(candidate_update.timesheet_ids)
        )
    )
    OR EXISTS (
      SELECT 1
      FROM public.timesheet_payment_overrides AS override_row
      WHERE override_row.candidate_id = candidate_update.candidate_id
        AND override_row.override_type = 'ADVANCE_THIS_PAYMENT'
        AND override_row.consumed_at_utc IS NULL
        AND override_row.cleared_at_utc IS NULL
        AND (
          COALESCE(array_length(candidate_update.timesheet_ids, 1), 0) = 0
          OR override_row.timesheet_id = ANY(candidate_update.timesheet_ids)
        )
    )
    OR EXISTS (
      SELECT 1
      FROM public.ts_pay_adjustments AS adjustment_row
      WHERE adjustment_row.candidate_id = candidate_update.candidate_id
        AND adjustment_row.paid_at_utc IS NULL
        AND (
          COALESCE(array_length(candidate_update.timesheet_ids, 1), 0) = 0
          OR adjustment_row.timesheet_id = ANY(candidate_update.timesheet_ids)
        )
    )
    OR EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND resolution_row.candidate_id = candidate_update.candidate_id
        AND (
          COALESCE(array_length(candidate_update.timesheet_ids, 1), 0) = 0
          OR resolution_row.timesheet_id IS NULL
          OR resolution_row.timesheet_id = ANY(candidate_update.timesheet_ids)
        )
    )
  WHERE EXISTS (
    SELECT 1
    FROM pg_temp._bpay_batch_mutation_keys AS affected_key
    WHERE affected_key.candidate_id = candidate_update.candidate_id
  );

  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(candidate_row.candidate_id::text ORDER BY candidate_row.candidate_id), '[]'::jsonb)
  INTO v_complex_candidate_count,
       v_complex_refresh_candidate_ids
  FROM pg_temp._bpay_batch_mutation_candidates AS candidate_row
  WHERE candidate_row.has_complexity IS TRUE;

  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(candidate_row.candidate_id::text ORDER BY candidate_row.candidate_id), '[]'::jsonb)
  INTO v_missing_key_candidate_count,
       v_missing_key_candidate_ids
  FROM pg_temp._bpay_batch_mutation_candidates AS candidate_row
  WHERE COALESCE(candidate_row.has_missing_economic_key, false) IS TRUE;


  DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_existing_preview_rows;
  CREATE TEMP TABLE _bpay_batch_mutation_existing_preview_rows ON COMMIT DROP AS
  SELECT
    preview_row.id,
    preview_row.candidate_id,
    preview_row.timesheet_id,
    preview_row.key_type,
    preview_row.key_value,
    preview_row.section,
    preview_row.selected,
    preview_row.selection_state,
    preview_row.status
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  JOIN pg_temp._bpay_batch_mutation_keys AS affected_key
    ON affected_key.candidate_id = preview_row.candidate_id
   AND preview_row.timesheet_id IS NOT DISTINCT FROM affected_key.timesheet_id
   AND preview_row.key_type IS NOT DISTINCT FROM affected_key.key_type
   AND preview_row.key_value IS NOT DISTINCT FROM affected_key.key_value
  WHERE preview_row.session_id = p_session_id
    AND preview_row.session_version = v_session_row.version
    AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) IN ('READY', 'DIRTY');

  IF v_operation_type IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE') THEN
    UPDATE public.banking_pay_workbench_preview_rows AS preview_update
    SET selected = false,
        selection_state = 'NOT_SELECTABLE',
        status = 'READY',
        row_json = jsonb_strip_nulls(
          COALESCE(preview_update.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'post_draft_overlay_applied', true,
            'post_draft_overlay_active', true,
            'post_draft_unavailable', true,
            'post_draft_unavailable_reason', 'ACTIVE_BATCH_RESERVATION',
            'post_draft_overlay_operation_type', v_operation_type,
            'post_draft_overlay_pay_batch_id', p_pay_batch_id::text,
            'post_draft_overlay_at_utc', v_now::text,
            'post_draft_effective_draftable', false,
            'post_draft_effective_is_ready_for_draft', false,
            'post_draft_effective_selection_allowed', false,
            'post_draft_effective_readiness_state', 'NOT_SELECTABLE',
            'post_draft_effective_presentation_section', 'DRAFTED',
            'selected', false,
            'selection_state', 'NOT_SELECTABLE'
          )
        ),
        updated_at_utc = v_now
    FROM pg_temp._bpay_batch_mutation_keys AS affected_key
    WHERE preview_update.session_id = p_session_id
      AND preview_update.candidate_id = affected_key.candidate_id
      AND UPPER(BTRIM(COALESCE(preview_update.status, ''))) = 'READY'
      AND preview_update.timesheet_id IS NOT DISTINCT FROM affected_key.timesheet_id
      AND preview_update.key_type IS NOT DISTINCT FROM affected_key.key_type
      AND preview_update.key_value IS NOT DISTINCT FROM affected_key.key_value;

    GET DIAGNOSTICS v_patched_row_count = ROW_COUNT;
    v_affected_row_count := v_patched_row_count;
  ELSE
    DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_restore_rows;
    CREATE TEMP TABLE _bpay_batch_mutation_restore_rows ON COMMIT DROP AS
    SELECT
      preview_row.id AS preview_row_id,
      preview_row.candidate_id,
      preview_row.timesheet_id,
      preview_row.key_type,
      preview_row.key_value,
      preview_row.row_json,
      candidate_row.has_complexity,
      candidate_row.has_missing_economic_key,
      outstanding_row.outstanding_ex_vat,
      outstanding_row.reservation_overrun_detected,
      public.pay_workbench_preview_line_contract_ok(
        preview_row.row_json,
        jsonb_build_object(
          'timesheet_id', preview_row.timesheet_id::text,
          'key_type', preview_row.key_type,
          'key_value', preview_row.key_value
        ),
        preview_row.section
      ) AS contract_result_json
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    JOIN pg_temp._bpay_batch_mutation_keys AS affected_key
      ON affected_key.candidate_id = preview_row.candidate_id
     AND preview_row.timesheet_id IS NOT DISTINCT FROM affected_key.timesheet_id
     AND preview_row.key_type IS NOT DISTINCT FROM affected_key.key_type
     AND preview_row.key_value IS NOT DISTINCT FROM affected_key.key_value
    JOIN pg_temp._bpay_batch_mutation_candidates AS candidate_row
      ON candidate_row.candidate_id = preview_row.candidate_id
    LEFT JOIN public._pay_outstanding_components(
      (SELECT COALESCE(array_agg(DISTINCT key_row.timesheet_id ORDER BY key_row.timesheet_id), ARRAY[]::uuid[]) FROM pg_temp._bpay_batch_mutation_keys AS key_row WHERE key_row.timesheet_id IS NOT NULL),
      p_pay_batch_id
    ) AS outstanding_row
      ON outstanding_row.timesheet_id = preview_row.timesheet_id
     AND outstanding_row.key_type = preview_row.key_type
     AND outstanding_row.key_value = preview_row.key_value
    WHERE preview_row.session_id = p_session_id
      AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) IN ('READY', 'DIRTY');

    UPDATE public.banking_pay_workbench_preview_rows AS preview_update
    SET selected = CASE
          WHEN restore_row.has_complexity IS NOT TRUE
           AND restore_row.has_missing_economic_key IS NOT TRUE
           AND COALESCE(restore_row.reservation_overrun_detected, false) IS NOT TRUE
           AND COALESCE(restore_row.outstanding_ex_vat, 0) > 0
           AND COALESCE((restore_row.contract_result_json->>'ok')::boolean, false) IS TRUE
           AND COALESCE((restore_row.contract_result_json->>'materialisable')::boolean, false) IS TRUE
           AND lower(BTRIM(COALESCE(restore_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND lower(BTRIM(COALESCE(restore_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND lower(BTRIM(COALESCE(restore_row.row_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          THEN true
          ELSE false
        END,
        selection_state = CASE
          WHEN restore_row.has_complexity IS NOT TRUE
           AND restore_row.has_missing_economic_key IS NOT TRUE
           AND COALESCE(restore_row.reservation_overrun_detected, false) IS NOT TRUE
           AND COALESCE(restore_row.outstanding_ex_vat, 0) > 0
           AND COALESCE((restore_row.contract_result_json->>'ok')::boolean, false) IS TRUE
           AND COALESCE((restore_row.contract_result_json->>'materialisable')::boolean, false) IS TRUE
           AND lower(BTRIM(COALESCE(restore_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND lower(BTRIM(COALESCE(restore_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
           AND lower(BTRIM(COALESCE(restore_row.row_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          THEN 'SELECTED'
          ELSE 'NOT_SELECTABLE'
        END,
        status = CASE
          WHEN restore_row.has_complexity IS TRUE OR restore_row.has_missing_economic_key IS TRUE THEN 'DIRTY'
          ELSE 'READY'
        END,
        row_json = jsonb_strip_nulls(
          COALESCE(preview_update.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'post_draft_overlay_applied', true,
            'post_draft_overlay_active', false,
            'post_draft_unavailable', false,
            'post_draft_unavailable_reason', NULL::text,
            'post_draft_overlay_operation_type', v_operation_type,
            'post_draft_overlay_pay_batch_id', p_pay_batch_id::text,
            'post_draft_overlay_at_utc', v_now::text,
            'post_draft_effective_draftable', NULL::boolean,
            'post_draft_effective_is_ready_for_draft', NULL::boolean,
            'post_draft_effective_selection_allowed', NULL::boolean,
            'post_draft_effective_readiness_state', NULL::text,
            'post_draft_effective_presentation_section', NULL::text,
            'restore_checked', true,
            'selected', CASE
              WHEN restore_row.has_complexity IS NOT TRUE
               AND restore_row.has_missing_economic_key IS NOT TRUE
               AND COALESCE(restore_row.reservation_overrun_detected, false) IS NOT TRUE
               AND COALESCE(restore_row.outstanding_ex_vat, 0) > 0
               AND COALESCE((restore_row.contract_result_json->>'ok')::boolean, false) IS TRUE
               AND COALESCE((restore_row.contract_result_json->>'materialisable')::boolean, false) IS TRUE
               AND lower(BTRIM(COALESCE(restore_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND lower(BTRIM(COALESCE(restore_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND lower(BTRIM(COALESCE(restore_row.row_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              THEN true
              ELSE false
            END,
            'selection_state', CASE
              WHEN restore_row.has_complexity IS NOT TRUE
               AND restore_row.has_missing_economic_key IS NOT TRUE
               AND COALESCE(restore_row.reservation_overrun_detected, false) IS NOT TRUE
               AND COALESCE(restore_row.outstanding_ex_vat, 0) > 0
               AND COALESCE((restore_row.contract_result_json->>'ok')::boolean, false) IS TRUE
               AND COALESCE((restore_row.contract_result_json->>'materialisable')::boolean, false) IS TRUE
               AND lower(BTRIM(COALESCE(restore_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND lower(BTRIM(COALESCE(restore_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
               AND lower(BTRIM(COALESCE(restore_row.row_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              THEN 'SELECTED'
              ELSE 'NOT_SELECTABLE'
            END
          )
        ),
        updated_at_utc = v_now
    FROM pg_temp._bpay_batch_mutation_restore_rows AS restore_row
    WHERE preview_update.id = restore_row.preview_row_id;

    GET DIAGNOSTICS v_patched_row_count = ROW_COUNT;
    v_affected_row_count := v_patched_row_count;
  END IF;

  IF NOT v_defer_complex_enqueue THEN
  FOR v_refresh_candidate_row IN
    SELECT
      candidate_row.candidate_id,
      candidate_row.timesheet_ids,
      candidate_row.has_missing_economic_key,
      candidate_row.has_complexity
    FROM pg_temp._bpay_batch_mutation_candidates AS candidate_row
    WHERE v_operation_type IN ('DRAFT_DELETE', 'DRAFT_CANCEL')
      AND (
        candidate_row.has_missing_economic_key IS TRUE
        OR candidate_row.has_complexity IS TRUE
      )
    ORDER BY candidate_row.candidate_id
  LOOP
    v_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
      p_snapshot_run_id => v_session_row.source_snapshot_run_id,
      p_candidate_id => v_refresh_candidate_row.candidate_id,
      p_reason => 'BATCH_MUTATION_COMPLEX_OR_UNCERTAIN',
      p_actor_user_id => COALESCE(p_actor_user_id, v_session_row.actor_user_id),
      p_payload_json => jsonb_strip_nulls(
        jsonb_build_object(
          'session_id', p_session_id::text,
          'source_session_id', p_session_id::text,
          'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
          'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
          'session_version', v_session_row.version,
          'session_signature', v_session_row.session_signature,
          'operation_type', v_operation_type,
          'pay_batch_id', p_pay_batch_id::text,
          'enqueue_origin', 'PAY_WORKBENCH_PATCH_PREVIEW_AFTER_BATCH_MUTATION',
          'force_legacy', true
        )
        || jsonb_build_object(
          'targeted_timesheet_ids', COALESCE(to_jsonb(v_refresh_candidate_row.timesheet_ids), '[]'::jsonb),
          'linked_timesheet_ids', '[]'::jsonb,
          'refresh_scope_kind', CASE WHEN COALESCE(array_length(v_refresh_candidate_row.timesheet_ids, 1), 0) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
          'pay_channel_scope', 'ALL',
          'projection_mode', 'LEGACY',
          'projection_class', CASE WHEN v_refresh_candidate_row.has_missing_economic_key IS TRUE THEN 'TARGET_SCOPE_MISSING' ELSE 'POST_DRAFT_PATCH_COMPLEX' END,
          'fallback_reason', CASE WHEN v_refresh_candidate_row.has_missing_economic_key IS TRUE THEN 'POST_DRAFT_PATCH_MISSING_ECONOMIC_KEY' ELSE 'POST_DRAFT_CANCEL_COMPLEX_REFRESH_REQUIRED' END,
          'source_build_required', true,
          'line_work_required', true,
          'delta_refresh_required', false,
          'complex_refresh_required', true,
          'scope_resolution_mode', v_scope_resolution_mode,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      )
    );

    v_enqueue_job_id_text := NULLIF(BTRIM(COALESCE(v_enqueue_result->>'job_id', '')), '');

    IF v_enqueue_job_id_text IS NULL OR v_enqueue_job_id_text !~* v_uuid_regex THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BATCH_MUTATION_CANONICAL_REFRESH_JOB_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_BATCH_MUTATION_CANONICAL_REFRESH_JOB_REQUIRED',
                'session_id', p_session_id::text,
                'candidate_id', v_refresh_candidate_row.candidate_id::text,
                'pay_batch_id', p_pay_batch_id::text,
                'operation_type', v_operation_type,
                'enqueue_result', COALESCE(v_enqueue_result, '{}'::jsonb)
              )::text;
    END IF;

    v_enqueue_job_id := v_enqueue_job_id_text::uuid;
    v_enqueue_job_type := NULL::text;
    v_enqueue_job_status := NULL::text;
    v_enqueue_source_build_run_id_text := NULL::text;

    SELECT
      active_job.job_type,
      active_job.status,
      NULLIF(BTRIM(COALESCE(
        active_job.payload_json->>'source_build_run_id',
        active_job.payload_json#>>'{source_build,source_build_run_id}',
        active_job.payload_json#>>'{source_build,run_id}',
        ''
      )), '')
    INTO
      v_enqueue_job_type,
      v_enqueue_job_status,
      v_enqueue_source_build_run_id_text
    FROM public.banking_pay_workbench_jobs AS active_job
    WHERE active_job.id = v_enqueue_job_id
      AND active_job.session_id = p_session_id
      AND active_job.candidate_id = v_refresh_candidate_row.candidate_id
    FOR UPDATE;

    IF NOT FOUND
       OR UPPER(BTRIM(COALESCE(v_enqueue_job_type, ''))) <> 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
       OR UPPER(BTRIM(COALESCE(v_enqueue_job_status, ''))) NOT IN ('QUEUED', 'RUNNING')
       OR v_enqueue_source_build_run_id_text IS NULL
       OR v_enqueue_source_build_run_id_text !~* v_uuid_regex THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BATCH_MUTATION_CANONICAL_REFRESH_JOB_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_BATCH_MUTATION_CANONICAL_REFRESH_JOB_INVALID',
                'session_id', p_session_id::text,
                'candidate_id', v_refresh_candidate_row.candidate_id::text,
                'pay_batch_id', p_pay_batch_id::text,
                'operation_type', v_operation_type,
                'job_id', v_enqueue_job_id_text,
                'job_type', v_enqueue_job_type,
                'job_status', v_enqueue_job_status,
                'source_build_run_id', v_enqueue_source_build_run_id_text
              )::text;
    END IF;

    UPDATE pg_temp._bpay_batch_mutation_candidates AS candidate_update
    SET targeted_refresh_job_id = v_enqueue_job_id
    WHERE candidate_update.candidate_id = v_refresh_candidate_row.candidate_id;

    PERFORM 1
    FROM public.banking_pay_workbench_session_scope AS scope_check
    WHERE scope_check.session_id = p_session_id
      AND scope_check.candidate_id = v_refresh_candidate_row.candidate_id
      AND UPPER(BTRIM(COALESCE(scope_check.status, ''))) = 'SOURCE_BUILD_PENDING'
      AND COALESCE(scope_check.dirty, false) IS TRUE
      AND scope_check.pending_job_id = v_enqueue_job_id
      AND scope_check.error_json IS NULL;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BATCH_MUTATION_SCOPE_BIND_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_BATCH_MUTATION_SCOPE_BIND_REQUIRED',
                'session_id', p_session_id::text,
                'candidate_id', v_refresh_candidate_row.candidate_id::text,
                'job_id', v_enqueue_job_id::text,
                'source_build_run_id', v_enqueue_source_build_run_id_text,
                'message', 'The canonical candidate refresh helper did not bind the workbench scope to the validated successor job.'
              )::text;
    END IF;

    v_targeted_refresh_enqueued_count := v_targeted_refresh_enqueued_count + 1;
  END LOOP;
  END IF;

  SELECT COALESCE(jsonb_agg(DISTINCT candidate_row.candidate_id::text ORDER BY candidate_row.candidate_id::text), '[]'::jsonb)
  INTO v_targeted_refresh_candidate_ids_json
  FROM pg_temp._bpay_batch_mutation_candidates AS candidate_row
  WHERE v_operation_type IN ('DRAFT_DELETE', 'DRAFT_CANCEL')
    AND (
      candidate_row.has_missing_economic_key IS TRUE
      OR candidate_row.has_complexity IS TRUE
    );

  DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_current_preview_rows;
  CREATE TEMP TABLE _bpay_batch_mutation_current_preview_rows ON COMMIT DROP AS
  SELECT preview_row.id,
         preview_row.candidate_id,
         preview_row.selected,
         preview_row.selection_state,
         preview_row.status
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  JOIN pg_temp._bpay_batch_mutation_existing_preview_rows AS existing_row
    ON existing_row.id = preview_row.id
  WHERE preview_row.updated_at_utc = v_now;

  SELECT COALESCE(jsonb_agg(current_row.id::text ORDER BY current_row.id::text), '[]'::jsonb)
  INTO v_patched_row_ids_json
  FROM pg_temp._bpay_batch_mutation_current_preview_rows AS current_row;

  SELECT COUNT(*) FILTER (WHERE existing_row.selected IS TRUE AND existing_row.selection_state = 'SELECTED' AND UPPER(BTRIM(COALESCE(existing_row.status, ''))) = 'READY')::integer,
         COUNT(*) FILTER (WHERE current_row.selected IS TRUE AND current_row.selection_state = 'SELECTED' AND UPPER(BTRIM(COALESCE(current_row.status, ''))) = 'READY')::integer,
         COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(existing_row.status, ''))) = 'READY')::integer,
         COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(current_row.status, ''))) = 'READY')::integer
  INTO v_selected_before_count,
       v_selected_after_count,
       v_ready_before_count,
       v_ready_after_count
  FROM pg_temp._bpay_batch_mutation_existing_preview_rows AS existing_row
  JOIN pg_temp._bpay_batch_mutation_current_preview_rows AS current_row
    ON current_row.id = existing_row.id;

  v_selected_count_delta := COALESCE(v_selected_after_count, 0) - COALESCE(v_selected_before_count, 0);
  v_ready_count_delta := COALESCE(v_ready_after_count, 0) - COALESCE(v_ready_before_count, 0);

  DROP TABLE IF EXISTS pg_temp._bpay_batch_mutation_effective_preview_rows;
  CREATE TEMP TABLE _bpay_batch_mutation_effective_preview_rows ON COMMIT DROP AS
  SELECT
    preview_row.id,
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
    AND NOT (
      COALESCE(
        lower(BTRIM(COALESCE(preview_row.row_json->>'post_draft_unavailable', '')))
          IN ('true', 't', '1', 'yes', 'y', 'on'),
        false
      )
      OR (
        COALESCE(
          lower(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_applied', '')))
            IN ('true', 't', '1', 'yes', 'y', 'on'),
          false
        )
        AND UPPER(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_operation_type', '')))
          IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
        AND lower(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_active', 'true')))
          NOT IN ('false', 'f', '0', 'no', 'n', 'off')
      )
    );

  SELECT
    COUNT(*)::integer,
    COUNT(*) FILTER (
      WHERE effective_row.selected IS TRUE
        AND UPPER(BTRIM(COALESCE(effective_row.selection_state, ''))) = 'SELECTED'
    )::integer
  INTO v_session_preview_row_count,
       v_session_selected_row_count
  FROM pg_temp._bpay_batch_mutation_effective_preview_rows AS effective_row;

  SELECT COALESCE(
           jsonb_object_agg(section_count.section, section_count.row_count ORDER BY section_count.section),
           '{}'::jsonb
         )
  INTO v_session_section_counts_json
  FROM (
    SELECT effective_row.section,
           COUNT(*)::integer AS row_count
    FROM pg_temp._bpay_batch_mutation_effective_preview_rows AS effective_row
    GROUP BY effective_row.section
  ) AS section_count;

  SELECT COALESCE(
           jsonb_agg(
             jsonb_build_object(
               'id', sample_row.id::text,
               'row_key', sample_row.row_key,
               'section', sample_row.section,
               'selected', COALESCE(sample_row.selected, false),
               'candidate_id', sample_row.candidate_id::text,
               'selection_state', sample_row.selection_state
             )
             ORDER BY sample_row.section, sample_row.row_ordinal, sample_row.id
           ),
           '[]'::jsonb
         )
  INTO v_session_candidate_samples_json
  FROM (
    SELECT effective_row.*
    FROM pg_temp._bpay_batch_mutation_effective_preview_rows AS effective_row
    ORDER BY effective_row.section, effective_row.row_ordinal, effective_row.id
    LIMIT 25
  ) AS sample_row;

  IF to_regprocedure('public.pay_workbench_delta_update_candidate_state_v1(uuid,uuid,uuid,jsonb)') IS NOT NULL THEN
    FOR v_candidate_state_candidate_id IN
      SELECT candidate_row.candidate_id
      FROM pg_temp._bpay_batch_mutation_candidates AS candidate_row
      ORDER BY candidate_row.candidate_id
    LOOP
      v_candidate_state_result := public.pay_workbench_delta_update_candidate_state_v1(
        p_session_id,
        v_candidate_state_candidate_id,
        v_patch_projection_run_id,
        jsonb_build_object(
          'context', 'POST_DRAFT_PATCH',
          'pay_batch_id', p_pay_batch_id::text,
          'operation_type', v_operation_type,
          'affected_economic_keys', COALESCE(v_affected_economic_keys_json, '[]'::jsonb),
          'patched_row_ids', COALESCE(v_patched_row_ids_json, '[]'::jsonb)
        )
      );
      v_candidate_state_results_json := v_candidate_state_results_json || jsonb_build_array(v_candidate_state_result);
    END LOOP;
  ELSE
    v_candidate_state_results_json := jsonb_build_array(jsonb_build_object(
      'ok', false,
      'fallback_required', true,
      'fallback_reason', 'PAY_WORKBENCH_DELTA_UPDATE_CANDIDATE_STATE_V1_MISSING'
    ));
  END IF;

  UPDATE public.banking_pay_workbench_sessions AS session_update
  SET progress_state = CASE
        WHEN COALESCE(v_targeted_refresh_enqueued_count, 0) > 0 THEN 'REFRESHING_CANDIDATES'
        WHEN UPPER(BTRIM(COALESCE(session_update.progress_state, ''))) IN ('READY', 'READY_EMPTY')
          THEN CASE WHEN COALESCE(v_session_preview_row_count, 0) = 0 THEN 'READY_EMPTY' ELSE 'READY' END
        ELSE session_update.progress_state
      END,
      preview_row_count = GREATEST(COALESCE(v_session_preview_row_count, 0), 0),
      selected_row_count = GREATEST(COALESCE(v_session_selected_row_count, 0), 0),
      section_counts_json = COALESCE(v_session_section_counts_json, '{}'::jsonb),
      candidate_sample_rows_json = COALESCE(v_session_candidate_samples_json, '[]'::jsonb),
      server_selected_preview_row_ids = CASE
        WHEN v_operation_type IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE') THEN (
          SELECT COALESCE(jsonb_agg(selected_id.value ORDER BY selected_id.ordinality), '[]'::jsonb)
          FROM jsonb_array_elements_text(
            CASE
              WHEN jsonb_typeof(COALESCE(session_update.server_selected_preview_row_ids, '[]'::jsonb)) = 'array'
                THEN COALESCE(session_update.server_selected_preview_row_ids, '[]'::jsonb)
              ELSE '[]'::jsonb
            END
          ) WITH ORDINALITY AS selected_id(value, ordinality)
          WHERE NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(COALESCE(v_patched_row_ids_json, '[]'::jsonb)) AS patched_id(value)
            WHERE patched_id.value = selected_id.value
          )
        )
        ELSE session_update.server_selected_preview_row_ids
      END,
      progress_json = jsonb_strip_nulls(
        COALESCE(session_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_batch_mutation_patch_at_utc', v_now::text,
          'last_batch_mutation_patch_pay_batch_id', p_pay_batch_id::text,
          'last_batch_mutation_patch_operation_type', v_operation_type,
          'last_batch_mutation_patch_rows', COALESCE(v_patched_row_count, 0),
          'last_batch_mutation_refresh_enqueued', COALESCE(v_targeted_refresh_enqueued_count, 0),
          'last_batch_mutation_patched_row_ids', COALESCE(v_patched_row_ids_json, '[]'::jsonb),
          'last_batch_mutation_affected_candidate_ids', COALESCE(v_affected_candidate_ids_json, '[]'::jsonb),
          'last_batch_mutation_affected_timesheet_ids', COALESCE(v_affected_timesheet_ids_json, '[]'::jsonb),
          'last_batch_mutation_scope_resolution_mode', v_scope_resolution_mode,
          'last_batch_mutation_scope_diagnostic', COALESCE(v_scope_diagnostic_json, '{}'::jsonb),
          'preview_row_count', GREATEST(COALESCE(v_session_preview_row_count, 0), 0),
          'selected_row_count', GREATEST(COALESCE(v_session_selected_row_count, 0), 0),
          'selected_eligible_ready_row_count', GREATEST(COALESCE(v_session_selected_row_count, 0), 0),
          'selected_rows_available', GREATEST(COALESCE(v_session_selected_row_count, 0), 0) > 0,
          'ready_for_draft', GREATEST(COALESCE(v_session_selected_row_count, 0), 0) > 0,
          'can_create_draft', GREATEST(COALESCE(v_session_selected_row_count, 0), 0) > 0,
          'section_counts_json', COALESCE(v_session_section_counts_json, '{}'::jsonb),
          'section_counts', COALESCE(v_session_section_counts_json, '{}'::jsonb),
          'candidate_sample_rows_json', COALESCE(v_session_candidate_samples_json, '[]'::jsonb),
          'progress_state', CASE
            WHEN COALESCE(v_targeted_refresh_enqueued_count, 0) > 0 THEN 'REFRESHING_CANDIDATES'
            WHEN COALESCE(v_session_preview_row_count, 0) = 0 THEN 'READY_EMPTY'
            ELSE 'READY'
          END,
          'phase', CASE
            WHEN COALESCE(v_targeted_refresh_enqueued_count, 0) > 0 THEN 'REFRESHING_CANDIDATES'
            WHEN COALESCE(v_session_preview_row_count, 0) = 0 THEN 'READY_EMPTY'
            ELSE 'READY'
          END,
          'ready_empty', COALESCE(v_session_preview_row_count, 0) = 0,
          'rows_available', COALESCE(v_session_preview_row_count, 0) > 0,
          'has_materialised_preview_rows', COALESCE(v_session_preview_row_count, 0) > 0,
          'status_text', CASE
            WHEN COALESCE(v_targeted_refresh_enqueued_count, 0) > 0 THEN 'Preparing payment preview.'
            WHEN COALESCE(v_session_preview_row_count, 0) = 0 THEN 'No payable rows found.'
            ELSE 'Payment preview is ready.'
          END,
          'next_recommended_action', CASE
            WHEN COALESCE(v_targeted_refresh_enqueued_count, 0) > 0 THEN 'WAIT_FOR_WORKER'
            ELSE 'READ_PREVIEW_PAGE'
          END
        )
      ),
      progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
      progress_updated_at_utc = v_now,
      updated_at_utc = v_now
  WHERE session_update.id = p_session_id;

  IF v_operation_type='DRAFT_CREATE'
     AND COALESCE(v_options_json->>'operation_id','') ~* v_uuid_regex THEN
    v_draft_operation_id:=(v_options_json->>'operation_id')::uuid;
    v_draft_adoption_result:=private.pay_workbench_draft_create_adoption_finalize_v1(
      v_draft_operation_id,p_pay_batch_id,p_session_id,
      ARRAY(
        SELECT candidate_row.candidate_id
        FROM pg_temp._bpay_batch_mutation_candidates AS candidate_row
        ORDER BY candidate_row.candidate_id
      ),
      jsonb_build_object('operation_type',v_operation_type)
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'patch_applied', true,
    'replacement_session_required', false,
    'affected_candidate_count', COALESCE(v_affected_candidate_count, 0),
    'affected_row_count', COALESCE(v_affected_row_count, 0),
    'patched_row_count', COALESCE(v_patched_row_count, 0),
    'targeted_refresh_enqueued_count', COALESCE(v_targeted_refresh_enqueued_count, 0),
    'targeted_refresh_enqueued', COALESCE(v_targeted_refresh_enqueued_count, 0) > 0,
    'complex_enqueue_deferred',v_defer_complex_enqueue,
    'replacement_session_required', false,
    'replacement_session_created', false,
    'affected_candidate_ids', COALESCE(v_affected_candidate_ids_json, '[]'::jsonb),
    'patched_row_ids', COALESCE(v_patched_row_ids_json, '[]'::jsonb),
    'affected_timesheet_ids', COALESCE(v_affected_timesheet_ids_json, '[]'::jsonb),
    'affected_economic_keys', COALESCE(v_affected_economic_keys_json, '[]'::jsonb),
    'targeted_refresh_candidate_ids', COALESCE(v_targeted_refresh_candidate_ids_json, '[]'::jsonb),
    'candidate_state_results', COALESCE(v_candidate_state_results_json, '[]'::jsonb),
    'shadow_compare_failed', false,
    'complex_refresh_candidate_ids', COALESCE(v_complex_refresh_candidate_ids, '[]'::jsonb),
    'missing_key_candidate_ids', COALESCE(v_missing_key_candidate_ids, '[]'::jsonb),
    'operation_type', v_operation_type,
    'pay_batch_id', p_pay_batch_id::text,
    'scope_resolution_mode', v_scope_resolution_mode,
    'scope_diagnostic', COALESCE(v_scope_diagnostic_json, '{}'::jsonb),
    'draft_create_adoption',COALESCE(v_draft_adoption_result,'{}'::jsonb),
    'post_action_refresh', jsonb_build_object(
      'mode', 'PATCH_EXISTING_SESSION',
      'patch_applied', true,
      'patched_session_id', p_session_id::text,
      'replacement_session_created', false,
      'affected_candidate_ids', COALESCE(v_affected_candidate_ids_json, '[]'::jsonb),
      'patched_row_ids', COALESCE(v_patched_row_ids_json, '[]'::jsonb),
      'affected_timesheet_ids', COALESCE(v_affected_timesheet_ids_json, '[]'::jsonb),
      'affected_economic_keys', COALESCE(v_affected_economic_keys_json, '[]'::jsonb),
      'targeted_refresh_candidate_ids', COALESCE(v_targeted_refresh_candidate_ids_json, '[]'::jsonb),
      'targeted_refresh_enqueued', COALESCE(v_targeted_refresh_enqueued_count, 0) > 0,
      'scope_resolution_mode', v_scope_resolution_mode,
      'scope_diagnostic', COALESCE(v_scope_diagnostic_json, '{}'::jsonb),
      'shadow_compare_failed', false
    )
  );
END;
$function$;
