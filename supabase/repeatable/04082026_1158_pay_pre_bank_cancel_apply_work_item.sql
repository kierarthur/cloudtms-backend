-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Preserves the installed function identity; no overload is added.

CREATE OR REPLACE FUNCTION public.pay_pre_bank_cancel_apply_work_item(
  p_work_item_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '6000ms'
SET lock_timeout TO '1000ms'
AS $function$
DECLARE
  v_work_item public.pay_payment_correction_work_items%rowtype;
  v_request public.pay_payment_correction_requests%rowtype;
  v_batch public.pay_batches%rowtype;
  v_operation public.banking_pay_operations%rowtype;
  v_membership public.pay_payment_correction_request_candidates%rowtype;
  v_mutation_guard jsonb := '{}'::jsonb;
  v_now timestamptz := now();
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := NULL;
  v_blocker jsonb := NULL::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_reservation_count integer := 0;
  v_selected_finance_component_count integer := 0;
  v_voided_item_count integer := 0;
  v_inserted_correction_item_count integer := 0;
  v_released_reservation_count integer := 0;
  v_restored_component_count integer := 0;
  v_reset_payout_count integer := 0;
  v_cancelled_mail_count integer := 0;
  v_recalculated_transfer_count integer := 0;
  v_dirty_candidate_count integer := 0;
  v_has_settlement_evidence boolean := false;
  v_has_bank_submission_evidence boolean := false;
  v_has_authorised_partial_transfer_change boolean := false;
  v_batch_execution_commit_state text := 'NOT_SUBMITTED';
  v_is_authorised_or_scheduled_batch boolean := false;
  v_candidate_id uuid;
  v_refresh_result jsonb;
  v_scope_type text := NULL::text;
  v_work_unit text := NULL::text;
  v_is_whole_batch_work_item boolean := false;
  v_total_active_batch_item_count integer := 0;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_id_count integer := 0;
  v_expected_item_mismatch_count integer := 0;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_batch_cancel_result jsonb := NULL::jsonb;
  v_mail_selected_scope_json jsonb := '{}'::jsonb;
  v_communications_review_required_count integer := 0;
  v_mail_scope_matching jsonb := '{}'::jsonb;
  v_summary_refresh_candidate_count integer := 0;
  v_active_batch_item_count_after integer := 0;
  v_active_batch_amount_inc_vat_after numeric := 0;
  v_batch_empty_after boolean := false;
  v_resolved_scope_json jsonb := '{}'::jsonb;
  v_manual_adjustment_result jsonb := '{}'::jsonb;
  v_carry_forward_create_result jsonb := '{}'::jsonb;
  v_carry_forward_release_result jsonb := '{}'::jsonb;
  v_carry_forward_created_count integer := 0;
  v_carry_forward_existing_count integer := 0;
  v_carry_forward_released_count integer := 0;
  v_changed_scope_json jsonb := '{}'::jsonb;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_same_request_partial_continuation boolean := false;
  v_original_expected_json_count integer := 0;
  v_original_expected_valid_count integer := 0;
  v_original_expected_belongs_count integer := 0;
  v_original_expected_active_count integer := 0;
  v_original_expected_same_request_voided_count integer := 0;
  v_original_expected_disallowed_state_count integer := 0;
  v_active_outside_original_count integer := 0;
  v_current_expected_outside_original_count integer := 0;
  v_applied_sibling_count integer := 0;
  v_membership_item_mismatch_count integer := 0;
  v_current_active_item_count integer := 0;
  v_current_source_row_count integer := 0;
  v_capacity_selected_scope_json jsonb := '{}'::jsonb;
  v_current_candidate_scope_hash text;
  v_candidate_scope_contract_version integer := 1;
  v_candidate_scope_contract_raw text;
  v_candidate_scope_hash_version integer := 1;
  v_source_row_count_semantics text := 'FINANCIAL_AND_QUEUED_COMMUNICATIONS';
  v_matching_queued_count integer := 0;
  v_unsafe_queued_count integer := 0;
  v_already_sent_untouched_count integer := 0;
  v_other_terminal_untouched_count integer := 0;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WORK_START',
    jsonb_build_object(
      'work_item_id', p_work_item_id,
      'actor_user_id', p_actor_user_id
    ),
    'pay_payment_correction',
    COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_work_item_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_work_items.*
  INTO v_work_item
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.id = p_work_item_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND',
              'work_item_id', p_work_item_id
            )::text;
  END IF;

  v_mutation_guard := private.pay_payment_mutation_guard_v1(
    v_work_item.pay_batch_id,
    v_work_item.correction_request_id,
    'CORRECTION_APPLY'
  );

  IF COALESCE((v_mutation_guard->>'ok')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_mutation_guard->>'code', 'PAYMENT_CORRECTION_GATE_OWNER_MISMATCH')
      USING ERRCODE = 'P0001', DETAIL = v_mutation_guard::text;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = v_work_item.correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'correction_request_id', v_work_item.correction_request_id
            )::text;
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_request.requested_by_user_id);

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_work_item.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND_FOR_WORK_ITEM',
              'work_item_id', p_work_item_id,
              'pay_batch_id', v_work_item.pay_batch_id
            )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND operation_row.input_json->>'correction_request_id' = v_request.id::text
  ORDER BY operation_row.created_at_utc
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND OR v_operation.pay_batch_id IS DISTINCT FROM v_batch.id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_NOT_FOUND_FOR_WORK_ITEM'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'OPERATION_MISMATCH', 'work_item_id', p_work_item_id
      )::text;
  END IF;

  SELECT public.pay_payment_correction_work_items.*
  INTO v_work_item
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.id = p_work_item_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND', 'work_item_id', p_work_item_id
      )::text;
  END IF;

  IF v_work_item.correction_request_id IS DISTINCT FROM v_request.id
     OR v_work_item.pay_batch_id IS DISTINCT FROM v_batch.id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_SCOPE_CHANGED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'SELECTION_STALE')::text;
  END IF;

  IF v_work_item.work_kind <> 'PRE_BANK_CANCEL' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_ITEM_KIND_NOT_PRE_BANK_CANCEL',
      'message', 'This work item is not a pre-bank cancellation work item.',
      'work_kind', v_work_item.work_kind
    );

    UPDATE public.pay_payment_correction_work_items AS blocked_work_kind
    SET status = 'BLOCKED', locked_at_utc = NULL, locked_by = NULL,
        processed_at_utc = v_now, last_error = v_blocker->>'message',
        result_json = COALESCE(blocked_work_kind.result_json, '{}'::jsonb)
          || jsonb_build_object('ok', false, 'status', 'BLOCKED',
                                'blocker', v_blocker, 'processed_at_utc', v_now)
    WHERE blocked_work_kind.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_candidate_scope_contract_raw := NULLIF(BTRIM(COALESCE(
    v_work_item.selection_json->>'candidate_scope_contract_version',
    v_request.plan_json->>'candidate_scope_contract_version',
    ''
  )), '');
  IF v_candidate_scope_contract_raw IS NULL OR v_candidate_scope_contract_raw = '1' THEN
    v_candidate_scope_contract_version := 1;
    v_candidate_scope_hash_version := 1;
    v_source_row_count_semantics := 'FINANCIAL_AND_QUEUED_COMMUNICATIONS';
  ELSIF v_candidate_scope_contract_raw = '2' THEN
    v_candidate_scope_contract_version := 2;
    v_candidate_scope_hash_version := 2;
    v_source_row_count_semantics := 'FINANCIAL_ONLY';
    IF v_request.plan_json->>'candidate_scope_contract_version' IS DISTINCT FROM '2'
       OR v_work_item.selection_json->>'candidate_scope_contract_version' IS DISTINCT FROM '2'
       OR v_request.plan_json->>'candidate_scope_hash_version' IS DISTINCT FROM '2'
       OR v_work_item.selection_json->>'candidate_scope_hash_version' IS DISTINCT FROM '2'
       OR v_request.plan_json->>'source_row_count_semantics' IS DISTINCT FROM 'FINANCIAL_ONLY'
       OR v_work_item.selection_json->>'source_row_count_semantics' IS DISTINCT FROM 'FINANCIAL_ONLY'
       OR v_request.plan_json->>'communication_cleanup_contract_version' IS DISTINCT FROM '1'
       OR v_work_item.selection_json->>'communication_cleanup_contract_version' IS DISTINCT FROM '1' THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH',
          'work_item_id', p_work_item_id,
          'correction_request_id', v_request.id
        )::text;
    END IF;
  ELSE
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED',
        'work_item_id', p_work_item_id,
        'candidate_scope_contract_version', v_candidate_scope_contract_raw
      )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_selected;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_selected ON COMMIT DROP AS
  SELECT
    selected_rows.pay_batch_id,
    selected_rows.pay_batch_candidate_id,
    selected_rows.candidate_id,
    selected_rows.pay_batch_item_id,
    selected_rows.item_type,
    selected_rows.timesheet_id,
    selected_rows.pay_bank_transfer_id,
    selected_rows.transfer_group_key,
    selected_rows.umbrella_id,
    selected_rows.finance_case_id,
    selected_rows.finance_component_id,
    selected_rows.reservation_id,
    selected_pay_advances.payout_transfer_id,
    selected_rows.economic_key_type,
    selected_rows.economic_key_value,
    selected_rows.source_amount_ex_vat,
    selected_rows.amount_ex_vat,
    selected_rows.amount_vat,
    selected_rows.amount_inc_vat,
    selected_rows.is_voided,
    selected_rows.already_corrected,
    selected_rows.key_resolution_failure_reason
  FROM public._pay_payment_correction_selected_items(
    v_work_item.pay_batch_id,
    v_work_item.selection_json,
    false
  ) AS selected_rows
  LEFT JOIN public.pay_advances AS selected_pay_advances
    ON selected_pay_advances.id = selected_rows.finance_case_id;

  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (umbrella_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (transfer_group_key);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (reservation_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (payout_transfer_id);
  CREATE INDEX ON pg_temp._tmp_pre_bank_cancel_selected (finance_component_id);

  SELECT
    count(*)::integer,
    count(DISTINCT pre_bank_selected.candidate_id) FILTER (WHERE pre_bank_selected.candidate_id IS NOT NULL)::integer,
    count(DISTINCT pre_bank_selected.pay_bank_transfer_id) FILTER (WHERE pre_bank_selected.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT pre_bank_selected.reservation_id) FILTER (WHERE pre_bank_selected.reservation_id IS NOT NULL)::integer,
    count(DISTINCT pre_bank_selected.finance_component_id) FILTER (WHERE pre_bank_selected.finance_component_id IS NOT NULL)::integer
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_reservation_count,
    v_selected_finance_component_count
  FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected;

  SELECT immutable_membership.*
  INTO v_membership
  FROM public.pay_payment_correction_request_candidates AS immutable_membership
  WHERE immutable_membership.correction_request_id = v_work_item.correction_request_id
    AND immutable_membership.pay_batch_candidate_id = v_work_item.pay_batch_candidate_id;

  IF NOT FOUND THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_MEMBERSHIP_MISMATCH',
      'message', 'The correction work item does not have immutable request membership.'
    );
  ELSIF v_membership.active_item_count NOT BETWEEN 1 AND 128
     OR v_membership.source_row_count NOT BETWEEN 1 AND 512
     OR v_membership.active_item_count <> v_selected_item_count
     OR v_work_item.selection_hash IS DISTINCT FROM v_membership.candidate_scope_hash THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_MEMBERSHIP_MISMATCH',
      'message', 'The correction work item no longer matches its immutable request membership.',
      'expected_item_count', v_membership.active_item_count,
      'resolved_item_count', v_selected_item_count
    );
  ELSE
    SELECT count(*)::integer
    INTO v_membership_item_mismatch_count
    FROM (
      (
        SELECT unnest(v_membership.pay_batch_item_ids) AS pay_batch_item_id
        EXCEPT
        SELECT selected_membership_items.pay_batch_item_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_membership_items
      )
      UNION ALL
      (
        SELECT selected_membership_items.pay_batch_item_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_membership_items
        EXCEPT
        SELECT unnest(v_membership.pay_batch_item_ids) AS pay_batch_item_id
      )
    ) AS membership_difference;

    IF v_membership_item_mismatch_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'WORK_MEMBERSHIP_MISMATCH',
        'message', 'The frozen payment-item membership differs from the reviewed selection.',
        'mismatch_count', v_membership_item_mismatch_count
      );
    END IF;
  END IF;

  IF v_blocker IS NOT NULL THEN
    UPDATE public.pay_payment_correction_work_items AS membership_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(membership_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE membership_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  IF v_selected_item_count = 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_SELECTED_PAYMENT_ITEMS_FOR_PRE_BANK_CANCEL',
      'message', 'No selectable pay_batch_items were resolved for the pre-bank cancellation work item.'
    );

    UPDATE public.pay_payment_correction_work_items AS no_items_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_items_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_items_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
v_scope_type := upper(btrim(COALESCE(v_work_item.selection_json->>'scope_type', '')));
v_work_unit := upper(btrim(COALESCE(v_work_item.selection_json->>'work_unit', '')));

SELECT count(*)::integer
INTO v_total_active_batch_item_count
FROM public.pay_batch_items AS total_batch_items
JOIN public.pay_batch_candidates AS total_batch_candidates
  ON total_batch_candidates.id = total_batch_items.pay_batch_candidate_id
WHERE total_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
  AND COALESCE(total_batch_items.is_voided, false) = false;

v_is_whole_batch_work_item := (
  v_scope_type = 'BATCH'
  AND COALESCE(NULLIF(v_work_unit, ''), 'BATCH') = 'BATCH'
  AND v_selected_item_count = COALESCE(v_total_active_batch_item_count, 0)
);

IF v_work_item.selection_json ? 'expected_item_count' THEN
  IF COALESCE(v_work_item.selection_json->>'expected_item_count', '') !~ '^[0-9]+$' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item expected_item_count is not a valid non-negative integer.',
      'expected_item_count_raw', v_work_item.selection_json->>'expected_item_count'
    );

    UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_invalid_expected_count_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(pre_bank_cancel_invalid_expected_count_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE pre_bank_cancel_invalid_expected_count_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_expected_item_count := (v_work_item.selection_json->>'expected_item_count')::integer;

  IF v_expected_item_count <> v_selected_item_count THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_item count.',
      'expected_item_count', v_expected_item_count,
      'resolved_item_count', v_selected_item_count
    );

    UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_expected_count_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(pre_bank_cancel_expected_count_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE pre_bank_cancel_expected_count_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;

IF v_work_item.selection_json ? 'expected_pay_batch_item_ids'
   AND COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids must be a JSON array.'
  );

  UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_expected_ids_type_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(pre_bank_cancel_expected_ids_type_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE pre_bank_cancel_expected_ids_type_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_expected_items;
CREATE TEMP TABLE _tmp_pre_bank_cancel_expected_items ON COMMIT DROP AS
WITH raw_expected_item_ids AS (
  SELECT jsonb_array_elements_text(
    CASE
      WHEN COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
        THEN v_work_item.selection_json->'expected_pay_batch_item_ids'
      ELSE '[]'::jsonb
    END
  ) AS raw_pay_batch_item_id
)
SELECT DISTINCT
  raw_expected_item_ids.raw_pay_batch_item_id,
  CASE
    WHEN raw_expected_item_ids.raw_pay_batch_item_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN raw_expected_item_ids.raw_pay_batch_item_id::uuid
    ELSE NULL::uuid
  END AS pay_batch_item_id
FROM raw_expected_item_ids;

SELECT count(*)::integer
INTO v_expected_item_mismatch_count
FROM pg_temp._tmp_pre_bank_cancel_expected_items AS invalid_expected_items
WHERE invalid_expected_items.pay_batch_item_id IS NULL;

IF v_expected_item_mismatch_count > 0 THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids contains invalid UUID values.',
    'invalid_expected_item_count', v_expected_item_mismatch_count
  );

  UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_invalid_expected_ids_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(pre_bank_cancel_invalid_expected_ids_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE pre_bank_cancel_invalid_expected_ids_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

SELECT count(*)::integer
INTO v_expected_item_id_count
FROM pg_temp._tmp_pre_bank_cancel_expected_items AS expected_item_count
WHERE expected_item_count.pay_batch_item_id IS NOT NULL;

IF v_expected_item_id_count > 0 THEN
  SELECT count(*)::integer
  INTO v_expected_item_mismatch_count
  FROM (
    (
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
      EXCEPT
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_items
    )
    UNION ALL
    (
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_items
      EXCEPT
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_pre_bank_cancel_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
    )
  ) AS expected_item_drift;

  IF v_expected_item_mismatch_count > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_SELECTION_DRIFT',
      'message', 'Correction work item selection no longer resolves to the expected selected pay_batch_items.',
      'expected_item_count', v_expected_item_id_count,
      'resolved_item_count', v_selected_item_count,
      'mismatch_count', v_expected_item_mismatch_count
    );

    UPDATE public.pay_payment_correction_work_items AS pre_bank_cancel_expected_ids_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(pre_bank_cancel_expected_ids_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE pre_bank_cancel_expected_ids_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;


  PERFORM 1
  FROM public.pay_batch_candidates AS locked_batch_candidates
  WHERE locked_batch_candidates.id IN (
    SELECT DISTINCT lock_selected_candidates.pay_batch_candidate_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_candidates
    WHERE lock_selected_candidates.pay_batch_candidate_id IS NOT NULL
  )
  FOR UPDATE OF locked_batch_candidates;

  PERFORM 1
  FROM public.pay_batch_items AS locked_batch_items
  JOIN pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_items
    ON lock_selected_items.pay_batch_item_id = locked_batch_items.id
  ORDER BY locked_batch_items.id
  FOR UPDATE OF locked_batch_items;

  PERFORM 1
  FROM public.pay_bank_transfers AS locked_bank_transfers
  WHERE locked_bank_transfers.id IN (
    SELECT DISTINCT lock_selected_transfers.pay_bank_transfer_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_transfers
    WHERE lock_selected_transfers.pay_bank_transfer_id IS NOT NULL
  )
  FOR UPDATE OF locked_bank_transfers;

  PERFORM 1
  FROM public.pay_advance_reservations AS locked_advance_reservations
  WHERE locked_advance_reservations.id IN (
    SELECT DISTINCT lock_selected_reservations.reservation_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_reservations
    WHERE lock_selected_reservations.reservation_id IS NOT NULL
  )
  FOR UPDATE OF locked_advance_reservations;

  PERFORM 1
  FROM public.pay_finance_case_components AS locked_finance_case_components
  WHERE locked_finance_case_components.id IN (
    SELECT DISTINCT lock_selected_components.finance_component_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_components
    WHERE lock_selected_components.finance_component_id IS NOT NULL
  )
  FOR UPDATE OF locked_finance_case_components;

  PERFORM 1
  FROM public.pay_advances AS locked_pay_advances
  WHERE locked_pay_advances.id IN (
    SELECT DISTINCT lock_selected_cases.finance_case_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS lock_selected_cases
    WHERE lock_selected_cases.finance_case_id IS NOT NULL
  )
  FOR UPDATE OF locked_pay_advances;

  -- Rebuild and lock the complete candidate-owned financial/source scope after
  -- acquiring the mutation guard and before the first financial write. The
  -- planning count is an immutable promise, not execution-time authority.
  v_capacity_selected_scope_json := jsonb_build_object(
    'scope_type', 'CANDIDATES',
    'work_unit', 'CANDIDATE',
    'pay_batch_ids', jsonb_build_array(v_work_item.pay_batch_id),
    'pay_batch_candidate_ids', jsonb_build_array(v_work_item.pay_batch_candidate_id),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (SELECT DISTINCT selected_scope.candidate_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.candidate_id IS NOT NULL) AS scope_value
    ), '[]'::jsonb),
    'pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (SELECT DISTINCT selected_scope.pay_batch_item_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope) AS scope_value
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (SELECT DISTINCT selected_scope.umbrella_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.umbrella_id IS NOT NULL) AS scope_value
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (SELECT DISTINCT selected_scope.finance_case_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.finance_case_id IS NOT NULL) AS scope_value
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (SELECT DISTINCT selected_scope.finance_component_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.finance_component_id IS NOT NULL) AS scope_value
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (SELECT DISTINCT selected_scope.reservation_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.reservation_id IS NOT NULL) AS scope_value
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
        UNION SELECT DISTINCT selected_scope.payout_transfer_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.payout_transfer_id IS NOT NULL
      ) AS scope_value
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (
        SELECT DISTINCT selected_scope.payout_transfer_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.payout_transfer_id IS NOT NULL
        UNION SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS scope_value
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (SELECT DISTINCT selected_scope.transfer_group_key AS value_text FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL) AS scope_value
    ), '[]'::jsonb)
  );

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_capacity_mail_scope;
  CREATE TEMP TABLE _tmp_pre_bank_capacity_mail_scope ON COMMIT DROP AS
  SELECT
    mail_row.id AS mail_outbox_id,
    mail_match.match_result,
    COALESCE(NULLIF(mail_match.match_result->>'safe_to_cancel', '')::boolean, false) AS safe_to_cancel,
    COALESCE(NULLIF(mail_match.match_result->>'requires_review', '')::boolean, false) AS requires_review,
    COALESCE(mail_match.match_result->>'match_kind', 'NONE') AS match_kind,
    COALESCE(mail_match.match_result->>'match_confidence', 'NONE') AS match_confidence
  FROM public.mail_outbox AS mail_row
  CROSS JOIN LATERAL (
    SELECT public._pay_payment_correction_mail_scope_match(
      mail_row.id, v_work_item.pay_batch_id, v_work_item.selection_json,
      v_capacity_selected_scope_json, false
    ) AS match_result
  ) AS mail_match
  WHERE upper(btrim(COALESCE(mail_row.status::text, ''))) = 'QUEUED'
    AND lower(concat_ws('|', mail_row.type, mail_row.email_type, mail_row.context_kind, mail_row.reference, COALESCE(mail_row.payment_scope_json::text, '{}'))) LIKE ANY (ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%'])
    AND COALESCE(NULLIF(mail_match.match_result->>'matched', '')::boolean, false);

  PERFORM 1
  FROM public.mail_outbox AS locked_mail
  JOIN pg_temp._tmp_pre_bank_capacity_mail_scope AS capacity_mail
    ON capacity_mail.mail_outbox_id = locked_mail.id
  FOR UPDATE OF locked_mail;

  -- Recheck status after any row-lock wait. Sender-wins rows are now SENT and
  -- are deliberately outside the V2 financial hash; cancellation-wins rows
  -- remain locked QUEUED until the atomic communications phase below.
  SELECT
    pg_catalog.count(*) FILTER (
      WHERE upper(btrim(COALESCE(locked_mail.status::text, ''))) = 'QUEUED'
    )::integer,
    pg_catalog.count(*) FILTER (
      WHERE upper(btrim(COALESCE(locked_mail.status::text, ''))) = 'QUEUED'
        AND NOT (
          COALESCE(capacity_mail.safe_to_cancel, false)
          AND (
            capacity_mail.match_confidence = 'EXACT'
            OR (v_is_whole_batch_work_item AND capacity_mail.match_kind = 'WHOLE_BATCH')
          )
        )
    )::integer
  INTO v_matching_queued_count, v_unsafe_queued_count
  FROM pg_temp._tmp_pre_bank_capacity_mail_scope AS capacity_mail
  JOIN public.mail_outbox AS locked_mail
    ON locked_mail.id = capacity_mail.mail_outbox_id;

  IF v_candidate_scope_contract_version = 2 THEN
    SELECT
      pg_catalog.count(*) FILTER (
        WHERE upper(btrim(COALESCE(mail_row.status::text, ''))) = 'SENT'
      )::integer,
      pg_catalog.count(*) FILTER (
        WHERE upper(btrim(COALESCE(mail_row.status::text, ''))) NOT IN ('QUEUED', 'SENT')
      )::integer
    INTO v_already_sent_untouched_count, v_other_terminal_untouched_count
    FROM public.mail_outbox AS mail_row
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_correction_mail_scope_match(
        mail_row.id, v_work_item.pay_batch_id, v_work_item.selection_json,
        v_capacity_selected_scope_json, false
      ) AS match_result
    ) AS mail_match
    WHERE lower(concat_ws('|', mail_row.type, mail_row.email_type, mail_row.context_kind, mail_row.reference, COALESCE(mail_row.payment_scope_json::text, '{}'))) LIKE ANY (ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%'])
      AND COALESCE(NULLIF(mail_match.match_result->>'matched', '')::boolean, false);

    IF v_unsafe_queued_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'COMMUNICATION_CLEANUP_UNSAFE',
        'message', 'Queued payment communications could not be bound safely to the reviewed cancellation scope.',
        'unsafe_queued_count', v_unsafe_queued_count
      );
      UPDATE public.pay_payment_correction_work_items AS communication_blocked_work
      SET status = 'BLOCKED', locked_at_utc = NULL, locked_by = NULL,
          processed_at_utc = v_now, last_error = v_blocker->>'message',
          result_json = COALESCE(communication_blocked_work.result_json, '{}'::jsonb)
            || jsonb_build_object(
              'ok', false,
              'status', 'BLOCKED',
              'blocker', v_blocker,
              'candidate_scope_contract_version', 2,
              'communication_cleanup_contract_version', 1,
              'processed_at_utc', v_now
            )
      WHERE communication_blocked_work.id = p_work_item_id;
      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;
  END IF;

  PERFORM 1
  FROM public.pay_batch_items AS locked_instruction_items
  LEFT JOIN public.pay_bank_transfers AS instruction_transfer
    ON instruction_transfer.id = locked_instruction_items.pay_bank_transfer_id
  WHERE locked_instruction_items.id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope)
     OR locked_instruction_items.pay_bank_transfer_id IN (
          SELECT selected_scope.pay_bank_transfer_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
          UNION SELECT selected_scope.payout_transfer_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.payout_transfer_id IS NOT NULL
     )
     OR instruction_transfer.transfer_group_key IN (
          SELECT selected_scope.transfer_group_key FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL
     )
  FOR SHARE OF locked_instruction_items;

  PERFORM 1 FROM public.pay_batch_item_breakdowns AS locked_source WHERE locked_source.pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.pay_batch_timesheet_snapshots AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND locked_source.candidate_id = v_work_item.candidate_id FOR SHARE OF locked_source;
  PERFORM 1 FROM public.timesheet_pay_state_history AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND locked_source.timesheet_id IN (SELECT selected_scope.timesheet_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.timesheet_id IS NOT NULL) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.pay_manual_adjustment_carry_forwards AS locked_source WHERE locked_source.source_pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope) OR locked_source.target_pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope) FOR UPDATE OF locked_source;
  PERFORM 1 FROM public.pay_bank_transfer_events AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND (locked_source.pay_bank_transfer_id IN (SELECT selected_scope.pay_bank_transfer_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL UNION SELECT selected_scope.payout_transfer_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.payout_transfer_id IS NOT NULL) OR locked_source.candidate_id = v_work_item.candidate_id) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.banking_pay_operation_transfer_scope AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND (locked_source.pay_bank_transfer_id IN (SELECT selected_scope.pay_bank_transfer_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL UNION SELECT selected_scope.payout_transfer_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE selected_scope.payout_transfer_id IS NOT NULL) OR locked_source.transfer_group_key IN (SELECT selected_scope.transfer_group_key FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL)) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.banking_pay_operation_transfer_scope_items AS locked_source WHERE locked_source.pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.pay_batch_paye_net_inputs AS locked_source WHERE locked_source.pay_batch_candidate_id = v_work_item.pay_batch_candidate_id FOR SHARE OF locked_source;

  -- Obtain the two independent authorities without multiplying source rows by
  -- item rows.
  SELECT count(*)::integer INTO v_current_active_item_count
  FROM public.pay_batch_items AS current_item
  WHERE current_item.id = ANY(v_membership.pay_batch_item_ids)
    AND COALESCE(current_item.is_voided, false) IS NOT TRUE;
  WITH selected_items AS (SELECT selected_scope.* FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope),
  transfer_ids AS (SELECT selected_item.pay_bank_transfer_id AS id FROM selected_items AS selected_item WHERE selected_item.pay_bank_transfer_id IS NOT NULL UNION SELECT selected_item.payout_transfer_id FROM selected_items AS selected_item WHERE selected_item.payout_transfer_id IS NOT NULL),
  transfer_groups AS (SELECT DISTINCT selected_item.transfer_group_key FROM selected_items AS selected_item WHERE NULLIF(btrim(COALESCE(selected_item.transfer_group_key, '')), '') IS NOT NULL),
  financial_scope_items AS (SELECT DISTINCT item_scope.id FROM public.pay_batch_items AS item_scope LEFT JOIN public.pay_bank_transfers AS item_transfer ON item_transfer.id = item_scope.pay_bank_transfer_id WHERE item_scope.id IN (SELECT selected_item.pay_batch_item_id FROM selected_items AS selected_item) OR item_scope.pay_bank_transfer_id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id) OR item_transfer.transfer_group_key IN (SELECT transfer_group.transfer_group_key FROM transfer_groups AS transfer_group)),
  source_counts AS (
    SELECT 1::bigint AS row_count UNION ALL SELECT count(*) FROM financial_scope_items
    UNION ALL SELECT count(*) FROM public.pay_batch_item_breakdowns AS source_row WHERE source_row.pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
    UNION ALL SELECT count(*) FROM public.pay_batch_timesheet_snapshots AS source_row WHERE source_row.pay_batch_id = v_work_item.pay_batch_id AND source_row.candidate_id = v_work_item.candidate_id
    UNION ALL SELECT count(*) FROM public.timesheet_pay_state_history AS source_row WHERE source_row.pay_batch_id = v_work_item.pay_batch_id AND source_row.timesheet_id IN (SELECT selected_item.timesheet_id FROM selected_items AS selected_item WHERE selected_item.timesheet_id IS NOT NULL)
    UNION ALL SELECT count(*) FROM public.pay_advance_reservations AS source_row WHERE source_row.pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
    UNION ALL SELECT count(*) FROM public.pay_finance_case_components AS source_row WHERE source_row.id IN (SELECT selected_item.finance_component_id FROM selected_items AS selected_item WHERE selected_item.finance_component_id IS NOT NULL)
    UNION ALL SELECT count(*) FROM public.pay_manual_adjustment_carry_forwards AS source_row WHERE source_row.source_pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item) OR source_row.target_pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
    UNION ALL SELECT count(*) FROM public.pay_advances AS source_row WHERE source_row.id IN (SELECT selected_item.finance_case_id FROM selected_items AS selected_item WHERE selected_item.finance_case_id IS NOT NULL)
    UNION ALL SELECT count(*) FROM public.pay_bank_transfers AS source_row WHERE source_row.id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id) OR source_row.transfer_group_key IN (SELECT transfer_group.transfer_group_key FROM transfer_groups AS transfer_group)
    UNION ALL SELECT count(*) FROM public.pay_bank_transfer_events AS source_row WHERE source_row.pay_batch_id = v_work_item.pay_batch_id AND (source_row.pay_bank_transfer_id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id) OR source_row.candidate_id = v_work_item.candidate_id)
    UNION ALL SELECT count(*) FROM public.banking_pay_operation_transfer_scope AS source_row WHERE source_row.pay_batch_id = v_work_item.pay_batch_id AND (source_row.pay_bank_transfer_id IN (SELECT transfer_id.id FROM transfer_ids AS transfer_id) OR source_row.transfer_group_key IN (SELECT transfer_group.transfer_group_key FROM transfer_groups AS transfer_group))
    UNION ALL SELECT count(*) FROM public.banking_pay_operation_transfer_scope_items AS source_row WHERE source_row.pay_batch_item_id IN (SELECT scope_item.id FROM financial_scope_items AS scope_item)
    UNION ALL SELECT count(*) FROM public.pay_batch_paye_net_inputs AS source_row WHERE source_row.pay_batch_candidate_id = v_work_item.pay_batch_candidate_id
  ) SELECT COALESCE(sum(source_count.row_count), 0)::integer INTO v_current_source_row_count FROM source_counts AS source_count;

  IF v_candidate_scope_contract_version = 1 THEN
    v_current_source_row_count := v_current_source_row_count + v_matching_queued_count;
  END IF;

  SELECT private.pay_payment_correction_sha256_v1(
    jsonb_build_object(
      'version', v_candidate_scope_hash_version,
      'pay_batch_candidate_id', v_work_item.pay_batch_candidate_id,
      'candidate_id', v_work_item.candidate_id,
      'requested_action', COALESCE(v_request.plan_json->>'requested_action', v_request.selection_json->>'requested_action'),
      'pay_batch_item_ids', to_jsonb(v_membership.pay_batch_item_ids),
      'item_count', v_current_active_item_count,
      'source_row_count', v_current_source_row_count,
      'active_amount_pence', round(COALESCE(candidate_scope.net_bank_amount, 0) * 100)::bigint,
      'item_contract', (
        SELECT jsonb_agg(jsonb_build_object(
          'id', item_scope.id, 'item_type', item_scope.item_type,
          'amount_ex_vat_pence', round(COALESCE(item_scope.amount_ex_vat, 0) * 100)::bigint,
          'amount_vat_pence', round(COALESCE(item_scope.amount_vat, 0) * 100)::bigint,
          'amount_inc_vat_pence', round(COALESCE(item_scope.amount_inc_vat, 0) * 100)::bigint,
          'reservation_id', item_scope.reservation_id,
          'finance_component_id', item_scope.finance_component_id,
          'pay_bank_transfer_id', item_scope.pay_bank_transfer_id,
          'operation_source_key', item_scope.operation_source_key,
          'frozen_component_snapshot_json', item_scope.frozen_component_snapshot_json,
          'frozen_source_basis_json', item_scope.frozen_source_basis_json
        ) ORDER BY item_scope.id)
        FROM public.pay_batch_items AS item_scope
        WHERE item_scope.id = ANY(v_membership.pay_batch_item_ids)
          AND COALESCE(item_scope.is_voided, false) IS NOT TRUE
      ),
      'shared_instruction_scope_hash', v_membership.shared_instruction_scope_hash,
      'eligibility_code', v_membership.eligibility_code_at_plan,
      'cancellation_reversion_pre_request_authority_digest',
        COALESCE(
          v_work_item.selection_json->>'candidate_scope_hash_pre_request_authority_digest',
          v_work_item.selection_json->'cancellation_reversion_pre_request_authority'->>'authority_digest'
        )
    ) || CASE WHEN v_candidate_scope_contract_version = 2 THEN
      jsonb_build_object(
        'contract', 'PAYMENT_CORRECTION_CANDIDATE_SCOPE_V2',
        'source_row_count_semantics', 'FINANCIAL_ONLY'
      )
    ELSE '{}'::jsonb END
  ) INTO v_current_candidate_scope_hash
  FROM public.pay_batch_candidates AS candidate_scope
  WHERE candidate_scope.id = v_work_item.pay_batch_candidate_id;

  IF v_current_active_item_count <> v_membership.active_item_count
     OR v_current_active_item_count > 128
     OR v_current_source_row_count <> v_membership.source_row_count
     OR v_current_source_row_count > 512
     OR v_current_candidate_scope_hash IS DISTINCT FROM v_membership.candidate_scope_hash THEN
    v_blocker := jsonb_build_object(
      'code', CASE WHEN v_current_active_item_count > 128 OR v_current_source_row_count > 512 THEN 'CANDIDATE_SCOPE_TOO_LARGE' ELSE 'SOURCE_SCOPE_CHANGED' END,
      'message', 'The payment source scope changed after review. Refresh and start again.',
      'expected_item_count', v_membership.active_item_count,
      'actual_item_count', v_current_active_item_count,
      'expected_source_row_count', v_membership.source_row_count,
      'actual_source_row_count', v_current_source_row_count,
      'candidate_scope_hash_matches', v_current_candidate_scope_hash = v_membership.candidate_scope_hash
    );
    UPDATE public.pay_payment_correction_work_items AS capacity_blocked_work
    SET status = 'BLOCKED', locked_at_utc = NULL, locked_by = NULL,
        processed_at_utc = v_now, last_error = v_blocker->>'message',
        result_json = COALESCE(capacity_blocked_work.result_json, '{}'::jsonb)
          || jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'processed_at_utc', v_now)
    WHERE capacity_blocked_work.id = p_work_item_id;
    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_resolved_scope_json := public._pay_resolve_payment_scope_for_cancel_rewind(
    v_work_item.pay_batch_id,
    COALESCE(v_work_item.selection_json, '{}'::jsonb),
    true,
    v_effective_actor_user_id
  );

  IF COALESCE((v_resolved_scope_json->>'is_full_scope')::boolean, false) IS NOT TRUE
     OR jsonb_array_length(COALESCE(v_resolved_scope_json->'partial_scope_blockers', '[]'::jsonb)) > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'SELECT_FULL_UNPAID_PAYMENT_SCOPE_REQUIRED',
      'message', 'Pre-bank cancellation requires the full frozen payment scope so linked finance, reservations, transfers, and manual adjustment lines are not left behind.',
      'resolved_scope', v_resolved_scope_json,
      'partial_scope_blockers', COALESCE(v_resolved_scope_json->'partial_scope_blockers', '[]'::jsonb)
    );

    UPDATE public.pay_payment_correction_work_items AS partial_scope_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(partial_scope_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'resolved_scope', v_resolved_scope_json,
        'processed_at_utc', v_now
      )
    WHERE partial_scope_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'resolved_scope', v_resolved_scope_json);
  END IF;
  v_classification_result := public._pay_payment_movement_classify(
    v_work_item.pay_batch_id,
    COALESCE(v_work_item.selection_json, '{}'::jsonb) || jsonb_build_object(
      'requested_action', 'CANCEL_PAYMENT_ATTEMPT',
      'source_context', 'WORK_ITEM_APPLY'
    )
  );

  v_classification := COALESCE(v_classification_result->>'classification', COALESCE(v_classification_result->>'payment_lifecycle_state', 'AMBIGUOUS_REVIEW_REQUIRED'));

  IF v_classification IN ('PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION', 'CANCELLED_BEFORE_BANK_SUBMISSION') THEN
    WITH original_expected_raw AS (
      SELECT original_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(v_request.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
            THEN v_request.selection_json->'expected_pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS original_values(raw_value)
    ),
    original_expected AS (
      SELECT DISTINCT
        original_expected_raw.raw_value,
        CASE
          WHEN original_expected_raw.raw_value ~* v_uuid_regex
            THEN original_expected_raw.raw_value::uuid
          ELSE NULL::uuid
        END AS pay_batch_item_id
      FROM original_expected_raw
    ),
    original_expected_valid AS (
      SELECT original_expected.pay_batch_item_id
      FROM original_expected
      WHERE original_expected.pay_batch_item_id IS NOT NULL
    ),
    original_item_state AS (
      SELECT
        original_expected_valid.pay_batch_item_id,
        batch_items.id IS NOT NULL
          AND batch_candidates.pay_batch_id = v_work_item.pay_batch_id AS belongs_to_batch,
        COALESCE(batch_items.is_voided, false) AS is_voided,
        EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_items AS same_request_correction_items
          WHERE same_request_correction_items.correction_request_id = v_work_item.correction_request_id
            AND same_request_correction_items.pay_batch_item_id = original_expected_valid.pay_batch_item_id
            AND same_request_correction_items.correction_item_kind = 'PRE_BANK_CANCEL'
            AND same_request_correction_items.status = 'APPLIED'
        ) AS voided_by_same_request,
        EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_items AS any_applied_correction_items
          WHERE any_applied_correction_items.pay_batch_item_id = original_expected_valid.pay_batch_item_id
            AND any_applied_correction_items.status = 'APPLIED'
        ) AS has_any_applied_correction
      FROM original_expected_valid
      LEFT JOIN public.pay_batch_items AS batch_items
        ON batch_items.id = original_expected_valid.pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS batch_candidates
        ON batch_candidates.id = batch_items.pay_batch_candidate_id
    )
    SELECT
      jsonb_array_length(
        CASE
          WHEN COALESCE(jsonb_typeof(v_request.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
            THEN v_request.selection_json->'expected_pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      )::integer,
      (SELECT count(*)::integer FROM original_expected_valid),
      (SELECT count(*)::integer FROM original_item_state WHERE original_item_state.belongs_to_batch),
      (SELECT count(*)::integer FROM original_item_state WHERE original_item_state.belongs_to_batch AND NOT original_item_state.is_voided),
      (SELECT count(*)::integer FROM original_item_state WHERE original_item_state.belongs_to_batch AND original_item_state.is_voided AND original_item_state.voided_by_same_request),
      (
        SELECT count(*)::integer
        FROM original_item_state
        WHERE original_item_state.belongs_to_batch
          AND NOT (
            (
              NOT original_item_state.is_voided
              AND NOT original_item_state.has_any_applied_correction
            )
            OR (
              original_item_state.is_voided
              AND original_item_state.voided_by_same_request
            )
          )
      ),
      (
        SELECT count(*)::integer
        FROM public.pay_batch_items AS active_outside_items
        JOIN public.pay_batch_candidates AS active_outside_candidates
          ON active_outside_candidates.id = active_outside_items.pay_batch_candidate_id
        WHERE active_outside_candidates.pay_batch_id = v_work_item.pay_batch_id
          AND COALESCE(active_outside_items.is_voided, false) = false
          AND NOT EXISTS (
            SELECT 1
            FROM original_expected_valid
            WHERE original_expected_valid.pay_batch_item_id = active_outside_items.id
          )
      ),
      (
        SELECT count(*)::integer
        FROM pg_temp._tmp_pre_bank_cancel_expected_items AS current_expected_items
        WHERE current_expected_items.pay_batch_item_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM original_expected_valid
            WHERE original_expected_valid.pay_batch_item_id = current_expected_items.pay_batch_item_id
          )
      ),
      (
        SELECT count(*)::integer
        FROM public.pay_payment_correction_work_items AS applied_sibling_items
        WHERE applied_sibling_items.correction_request_id = v_work_item.correction_request_id
          AND applied_sibling_items.id <> p_work_item_id
          AND applied_sibling_items.work_kind = 'PRE_BANK_CANCEL'
          AND applied_sibling_items.status = 'APPLIED'
      )
    INTO
      v_original_expected_json_count,
      v_original_expected_valid_count,
      v_original_expected_belongs_count,
      v_original_expected_active_count,
      v_original_expected_same_request_voided_count,
      v_original_expected_disallowed_state_count,
      v_active_outside_original_count,
      v_current_expected_outside_original_count,
      v_applied_sibling_count;

    v_same_request_partial_continuation := (
      v_request.correction_kind = 'PRE_BANK_CANCEL'
      AND UPPER(BTRIM(COALESCE(v_request.selection_json->>'scope_type', ''))) IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH')
      AND NULLIF(BTRIM(COALESCE(v_work_item.selection_json->>'source_correction_request_id', '')), '') = v_work_item.correction_request_id::text
      AND v_original_expected_json_count > 0
      AND v_original_expected_valid_count = v_original_expected_json_count
      AND v_original_expected_belongs_count = v_original_expected_json_count
      AND v_original_expected_active_count > 0
      AND v_original_expected_same_request_voided_count > 0
      AND v_original_expected_active_count + v_original_expected_same_request_voided_count = v_original_expected_json_count
      AND v_original_expected_disallowed_state_count = 0
      AND v_active_outside_original_count = 0
      AND v_current_expected_outside_original_count = 0
      AND v_applied_sibling_count > 0
    );
  END IF;

  IF NOT (
    v_classification IN ('LOCAL_PREPARED_NOT_SENT', 'SCHEDULED_LOCAL_NOT_SENT')
    OR COALESCE((v_classification_result->>'can_pre_provider_cancel')::boolean, false) = true
    OR COALESCE(v_classification_result->>'recommended_action', '') = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
    OR (
      v_classification IN ('PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION', 'CANCELLED_BEFORE_BANK_SUBMISSION')
      AND v_same_request_partial_continuation
    )
  ) THEN
    v_blocker := jsonb_build_object(
      'code', 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED',
      'message', 'Selected scope is no longer classified as local/pre-provider cancellation.',
      'classification', v_classification,
      'classification_result', v_classification_result,
      'same_request_partial_continuation', v_same_request_partial_continuation,
      'same_request_continuation_proof', jsonb_build_object(
        'original_expected_json_count', v_original_expected_json_count,
        'original_expected_valid_count', v_original_expected_valid_count,
        'original_expected_belongs_count', v_original_expected_belongs_count,
        'original_expected_active_count', v_original_expected_active_count,
        'original_expected_same_request_voided_count', v_original_expected_same_request_voided_count,
        'original_expected_disallowed_state_count', v_original_expected_disallowed_state_count,
        'active_outside_original_count', v_active_outside_original_count,
        'current_expected_outside_original_count', v_current_expected_outside_original_count,
        'applied_sibling_count', v_applied_sibling_count
      )
    );

    UPDATE public.pay_payment_correction_work_items AS classification_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(classification_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE classification_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_batch_candidates
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_selected
      ON settled_selected.pay_batch_candidate_id = public.pay_batch_candidates.id
    WHERE upper(btrim(COALESCE(public.pay_batch_candidates.settlement_status, ''))) = 'SETTLED'
       OR public.pay_batch_candidates.settled_at_utc IS NOT NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.pay_bank_transfers
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_transfer_selected
      ON settled_transfer_selected.pay_bank_transfer_id = public.pay_bank_transfers.id
    WHERE upper(btrim(COALESCE(public.pay_bank_transfers.status, ''))) = 'COMPLETED'
       OR public.pay_bank_transfers.completed_at_utc IS NOT NULL
  ) OR EXISTS (
    SELECT 1
    FROM public.timesheet_pay_state_history
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_history_selected
      ON settled_history_selected.timesheet_id = public.timesheet_pay_state_history.timesheet_id
    WHERE public.timesheet_pay_state_history.pay_batch_id = v_work_item.pay_batch_id
  ) OR EXISTS (
    SELECT 1
    FROM public.pay_advance_reservations
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS settled_reservation_selected
      ON settled_reservation_selected.reservation_id = public.pay_advance_reservations.id
    WHERE upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) = 'SETTLED'
       OR public.pay_advance_reservations.settled_at_utc IS NOT NULL
  )
  INTO v_has_settlement_evidence;

  IF v_has_settlement_evidence THEN
    v_blocker := jsonb_build_object(
      'code', 'SELECTED_SCOPE_HAS_SETTLEMENT_EVIDENCE',
      'message', 'Pre-bank cancellation cannot apply because selected scope has settlement evidence.'
    );

    UPDATE public.pay_payment_correction_work_items AS settled_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(settled_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE settled_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_batch_execution_commit_state := upper(btrim(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED')));

  SELECT EXISTS (
    SELECT 1
    FROM public._pay_bank_transfer_provider_evidence_classify(
      v_work_item.pay_batch_id,
      NULL::uuid,
      v_resolved_scope_json,
      NULL::uuid
    ) AS provider_evidence
    WHERE COALESCE(provider_evidence.provider_submitted, false) = true
       OR COALESCE(provider_evidence.provider_request_sent, false) = true
       OR COALESCE(provider_evidence.provider_response_present, false) = true
       OR COALESCE(provider_evidence.provider_event_present, false) = true
       OR COALESCE(provider_evidence.provider_external_id_present, false) = true
       OR provider_evidence.evidence_class IN (
         'PROVIDER_REQUEST_SENT',
         'PROVIDER_RESPONSE_PRESENT',
         'PROVIDER_EVENT_PRESENT',
         'PROVIDER_EXTERNAL_ID_PRESENT',
         'PROVIDER_SUBMITTED',
         'PROVIDER_OUTCOME_UNKNOWN'
       )
  ) OR v_batch_execution_commit_state <> 'NOT_SUBMITTED'
    OR NULLIF(btrim(COALESCE(v_batch.execution_commit_ref, '')), '') IS NOT NULL
    OR v_batch.execution_committed_at_utc IS NOT NULL
  INTO v_has_bank_submission_evidence;

  IF v_has_bank_submission_evidence THEN
    v_blocker := jsonb_build_object(
      'code', 'BANK_SUBMISSION_EVIDENCE_PRESENT',
      'message', 'Pre-bank cancellation cannot apply because bank/provider submission evidence exists.'
    );

    UPDATE public.pay_payment_correction_work_items AS submission_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(submission_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE submission_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  -- A scheduled no-bank/local-manual execution remains RUNNING only so its
  -- durable runner can wake at the scheduled time. Once this work item has
  -- re-proved that no provider or execution-commit evidence exists, retire
  -- that exact wait before changing any frozen payment scope. Provider-bound
  -- phases and every ambiguous execution state remain untouched.
  UPDATE public.banking_pay_operations AS scheduled_local_operation
  SET status = 'CANCELLED',
      phase = 'COMPLETE',
      runner_state = 'CANCELLED',
      requires_user_action = false,
      result_json = COALESCE(scheduled_local_operation.result_json, '{}'::jsonb)
        || jsonb_build_object(
          'code', 'SCHEDULED_LOCAL_EXECUTION_CANCELLED_BEFORE_APPLY',
          'correction_request_id', v_work_item.correction_request_id,
          'work_item_id', p_work_item_id,
          'provider_submission_attempted', false,
          'submitted_to_bank', false
        ),
      completed_at_utc = COALESCE(scheduled_local_operation.completed_at_utc, v_now),
      lease_owner = NULL,
      lease_expires_at_utc = NULL,
      locked_by = NULL,
      lock_expires_at_utc = NULL,
      run_after_utc = NULL,
      updated_at_utc = v_now,
      resume_reason = 'SCHEDULED_LOCAL_EXECUTION_CANCELLED_BEFORE_APPLY'
  WHERE scheduled_local_operation.pay_batch_id = v_work_item.pay_batch_id
    AND scheduled_local_operation.operation_type = 'PAYMENT_EXECUTE'
    AND scheduled_local_operation.status IN ('QUEUED', 'RUNNING', 'PROCESSING', 'CLAIMED', 'IN_PROGRESS')
    AND scheduled_local_operation.phase = 'SCHEDULE_PAYMENT'
    AND scheduled_local_operation.resume_reason IN (
      'WAIT_FOR_SCHEDULED_NO_BANK_PAYMENT',
      'WAIT_FOR_SCHEDULED_LOCAL_MANUAL_SETTLEMENT'
    );

  v_is_authorised_or_scheduled_batch := upper(btrim(COALESCE(v_batch.status, ''))) IN (
    'AWAITING_AUTHORISATION',
    'AUTHORISED_FOR_PAYMENT',
    'SCHEDULED',
    'EXECUTING',
    'WAITING_BANK_CONFIRM'
  );

  SELECT EXISTS (
    WITH affected_transfers AS (
      SELECT DISTINCT partial_selected.pay_bank_transfer_id
      FROM pg_temp._tmp_pre_bank_cancel_selected AS partial_selected
      WHERE partial_selected.pay_bank_transfer_id IS NOT NULL
    ),
    selected_transfer_items AS (
      SELECT
        affected_transfers.pay_bank_transfer_id,
        count(public.pay_batch_items.id)::integer AS selected_count
      FROM affected_transfers
      JOIN public.pay_batch_items
        ON public.pay_batch_items.pay_bank_transfer_id = affected_transfers.pay_bank_transfer_id
      JOIN pg_temp._tmp_pre_bank_cancel_selected AS partial_selected_items
        ON partial_selected_items.pay_batch_item_id = public.pay_batch_items.id
      GROUP BY affected_transfers.pay_bank_transfer_id
    ),
    all_transfer_items AS (
      SELECT
        affected_transfers.pay_bank_transfer_id,
        count(public.pay_batch_items.id)::integer AS total_count
      FROM affected_transfers
      JOIN public.pay_batch_items
        ON public.pay_batch_items.pay_bank_transfer_id = affected_transfers.pay_bank_transfer_id
      JOIN public.pay_batch_candidates
        ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
      WHERE public.pay_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
        AND COALESCE(public.pay_batch_items.is_voided, false) = false
      GROUP BY affected_transfers.pay_bank_transfer_id
    )
    SELECT 1
    FROM selected_transfer_items
    JOIN all_transfer_items
      ON all_transfer_items.pay_bank_transfer_id = selected_transfer_items.pay_bank_transfer_id
    WHERE selected_transfer_items.selected_count < all_transfer_items.total_count
  )
  INTO v_has_authorised_partial_transfer_change;

  IF v_is_authorised_or_scheduled_batch AND v_has_authorised_partial_transfer_change THEN
    v_blocker := jsonb_build_object(
      'code', 'AUTHORISED_TRANSFER_PARTIAL_CANCEL_BLOCKED',
      'message', 'Selected items are part of an authorised/scheduled transfer that still has remaining items. Cancel the whole transfer/whole batch or recreate the batch.'
    );

    UPDATE public.pay_payment_correction_work_items AS partial_transfer_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(partial_transfer_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE partial_transfer_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;


IF v_is_whole_batch_work_item THEN
  PERFORM public._imp_debug_audit(
    v_effective_actor_user_id,
    'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WHOLE_BATCH_SCOPE_APPLY_IN_PLACE',
    jsonb_build_object(
      'work_item_id', p_work_item_id,
      'correction_request_id', v_work_item.correction_request_id,
      'pay_batch_id', v_work_item.pay_batch_id,
      'selected_item_count', v_selected_item_count,
      'message', 'Whole-batch cancellation work item is applied in-place against its resolved payment scope; it must not delegate back to pay_batch_cancel.'
    ),
    'pay_payment_correction',
    p_work_item_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );
END IF;

  v_manual_adjustment_result := public._pay_detect_manual_adjustments_for_carry_forward(
    v_work_item.pay_batch_id,
    v_resolved_scope_json,
    v_effective_actor_user_id
  );

  IF jsonb_array_length(COALESCE(v_manual_adjustment_result->'carry_forward_blockers', '[]'::jsonb)) > 0
     OR COALESCE((v_manual_adjustment_result->>'can_carry_forward_automatically')::boolean, true) IS NOT TRUE THEN
    v_blocker := jsonb_build_object(
      'code', 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS',
      'message', 'Pre-bank cancellation is blocked because one or more source-less manual adjustments cannot be safely carried forward.',
      'manual_adjustment_support_details_json', v_manual_adjustment_result
    );

    UPDATE public.pay_payment_correction_work_items AS manual_adjustment_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(manual_adjustment_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'manual_adjustment_support_details_json', v_manual_adjustment_result,
        'processed_at_utc', v_now
      )
    WHERE manual_adjustment_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'manual_adjustment_support_details_json', v_manual_adjustment_result);
  END IF;

  v_carry_forward_create_result := public._pay_manual_adjustment_carry_forward_create(
    NULL::uuid[],
    v_resolved_scope_json,
    v_work_item.correction_request_id,
    p_work_item_id,
    v_effective_actor_user_id
  );

  v_carry_forward_release_result := public._pay_manual_adjustment_carry_forward_release_for_scope(
    v_work_item.pay_batch_id,
    v_resolved_scope_json,
    NULL::uuid[],
    v_effective_actor_user_id,
    'PRE_BANK_CANCEL'
  );

  v_carry_forward_created_count := COALESCE(NULLIF(v_carry_forward_create_result->>'created_count', '')::integer, 0);
  v_carry_forward_existing_count := COALESCE(NULLIF(v_carry_forward_create_result->>'existing_count', '')::integer, 0);
  v_carry_forward_released_count := COALESCE(NULLIF(v_carry_forward_release_result->>'released_count', '')::integer, 0);

  INSERT INTO public.pay_payment_correction_items(
    correction_request_id,
    pay_batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    pay_bank_transfer_id,
    timesheet_id,
    finance_case_id,
    finance_component_id,
    reservation_id,
    item_type,
    correction_item_kind,
    source_amount,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    economic_key_type,
    economic_key_value,
    before_snapshot_json,
    after_snapshot_json,
    status,
    created_at_utc,
    applied_at_utc
  )
  SELECT
    v_work_item.correction_request_id,
    selected_for_ledger.pay_batch_id,
    selected_for_ledger.pay_batch_candidate_id,
    selected_for_ledger.candidate_id,
    selected_for_ledger.pay_batch_item_id,
    selected_for_ledger.pay_bank_transfer_id,
    selected_for_ledger.timesheet_id,
    selected_for_ledger.finance_case_id,
    selected_for_ledger.finance_component_id,
    selected_for_ledger.reservation_id,
    selected_for_ledger.item_type,
    'PRE_BANK_CANCEL',
    selected_for_ledger.source_amount_ex_vat,
    selected_for_ledger.amount_ex_vat,
    selected_for_ledger.amount_vat,
    selected_for_ledger.amount_inc_vat,
    selected_for_ledger.economic_key_type,
    selected_for_ledger.economic_key_value,
    to_jsonb(ledger_items),
    to_jsonb(ledger_items) || jsonb_build_object('is_voided', true, 'updated_at', v_now),
    'APPLIED',
    v_now,
    v_now
  FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_for_ledger
  JOIN public.pay_batch_items AS ledger_items
    ON ledger_items.id = selected_for_ledger.pay_batch_item_id
  ON CONFLICT (pay_batch_item_id, correction_item_kind) WHERE status = 'APPLIED' AND pay_batch_item_id IS NOT NULL DO NOTHING;

  GET DIAGNOSTICS v_inserted_correction_item_count = ROW_COUNT;

  UPDATE public.pay_batch_items AS items_to_void
  SET
    is_voided = true,
    updated_at = v_now
  WHERE items_to_void.id IN (
    SELECT selected_to_void.pay_batch_item_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_to_void
  )
    AND COALESCE(items_to_void.is_voided, false) = false;

  GET DIAGNOSTICS v_voided_item_count = ROW_COUNT;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_released_reservations;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_released_reservations ON COMMIT DROP AS
  WITH release_candidates AS (
    SELECT DISTINCT
      public.pay_advance_reservations.id,
      public.pay_advance_reservations.finance_case_id,
      public.pay_advance_reservations.finance_component_id,
      public.pay_advance_reservations.pay_batch_item_id,
      public.pay_advance_reservations.reserved_amount,
      public.pay_advance_reservations.reserved_source_amount
    FROM public.pay_advance_reservations
    JOIN pg_temp._tmp_pre_bank_cancel_selected AS selected_reservation_items
      ON selected_reservation_items.reservation_id = public.pay_advance_reservations.id
      OR selected_reservation_items.pay_batch_item_id = public.pay_advance_reservations.pay_batch_item_id
    WHERE public.pay_advance_reservations.pay_batch_id = v_work_item.pay_batch_id
      AND (
        upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) = 'RESERVED'
        OR (
          upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) = 'COMMITTED'
          AND public.pay_advance_reservations.settled_at_utc IS NULL
        )
      )
  ),
  released_rows AS (
    UPDATE public.pay_advance_reservations AS reservations_to_release
    SET
      status = 'RELEASED',
      released_at_utc = COALESCE(reservations_to_release.released_at_utc, v_now),
      released_reason = 'PRE_BANK_CANCEL',
      updated_by_user_id = p_actor_user_id
    FROM release_candidates
    WHERE reservations_to_release.id = release_candidates.id
    RETURNING
      reservations_to_release.id,
      reservations_to_release.finance_case_id,
      reservations_to_release.finance_component_id,
      reservations_to_release.pay_batch_item_id,
      reservations_to_release.reserved_amount,
      reservations_to_release.reserved_source_amount
  )
  SELECT
    released_rows.id AS reservation_id,
    released_rows.finance_case_id,
    released_rows.finance_component_id,
    released_rows.pay_batch_item_id,
    released_rows.reserved_amount,
    released_rows.reserved_source_amount
  FROM released_rows;

  SELECT count(*)::integer
  INTO v_released_reservation_count
  FROM pg_temp._tmp_pre_bank_cancel_released_reservations AS released_reservation_count;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_component_restore;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_component_restore ON COMMIT DROP AS
  SELECT
    restore_source.finance_component_id,
    restore_source.finance_case_id,
    round(sum(restore_source.restore_source_amount), 2)::numeric AS restore_source_amount
  FROM (
    SELECT
      COALESCE(released_reservations.finance_component_id, public.pay_batch_items.finance_component_id) AS finance_component_id,
      COALESCE(released_reservations.finance_case_id, public.pay_batch_items.finance_case_id) AS finance_case_id,
      round(abs(COALESCE(
        released_reservations.reserved_source_amount,
        public._pay_batch_item_source_reservation_amount_ex_vat(public.pay_batch_items.id),
        public.pay_batch_items.frozen_source_amount,
        released_reservations.reserved_amount,
        public.pay_batch_items.amount_ex_vat,
        public.pay_batch_items.amount_inc_vat,
        0
      )), 2)::numeric AS restore_source_amount
    FROM pg_temp._tmp_pre_bank_cancel_released_reservations AS released_reservations
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.id = released_reservations.pay_batch_item_id
  ) AS restore_source
  WHERE restore_source.finance_component_id IS NOT NULL
    AND restore_source.restore_source_amount > 0
  GROUP BY restore_source.finance_component_id, restore_source.finance_case_id;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_component_restore_apply;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_component_restore_apply ON COMMIT DROP AS
  SELECT
    public.pay_finance_case_components.id AS finance_component_id,
    public.pay_finance_case_components.finance_case_id,
    public.pay_finance_case_components.remaining_source_amount AS remaining_before,
    /* Draft reservations are tracked separately and do not decrement the
       component's live outstanding balance.  Releasing a pre-bank reservation
       must therefore leave remaining_source_amount unchanged; adding the
       reservation here would resurrect already-settled recovery value. */
    COALESCE(public.pay_finance_case_components.remaining_source_amount, 0) AS remaining_after,
    component_restore.restore_source_amount
  FROM pg_temp._tmp_pre_bank_cancel_component_restore AS component_restore
  JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = component_restore.finance_component_id;

  UPDATE public.pay_finance_case_components AS components_to_restore
  SET
    remaining_source_amount = component_restore_apply.remaining_after,
    resolved_at_utc = CASE WHEN component_restore_apply.remaining_after > 0 THEN NULL ELSE components_to_restore.resolved_at_utc END,
    closed_at_utc = NULL,
    updated_at_utc = v_now
  FROM pg_temp._tmp_pre_bank_cancel_component_restore_apply AS component_restore_apply
  WHERE components_to_restore.id = component_restore_apply.finance_component_id;

  GET DIAGNOSTICS v_restored_component_count = ROW_COUNT;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    component_restore_apply.finance_case_id,
    component_restore_apply.finance_component_id,
    'COMPONENT_RESTORED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    NULL::uuid,
    jsonb_build_object('remaining_source_amount', component_restore_apply.remaining_before),
    jsonb_build_object(
      'remaining_source_amount', component_restore_apply.remaining_after,
      'restored_source_amount', component_restore_apply.restore_source_amount,
      'correction_kind', 'PRE_BANK_CANCEL',
      'work_item_id', p_work_item_id
    ),
    'PRE_BANK_CANCEL',
    'Payment correction pre-bank cancellation released the reservation without changing the live component outstanding balance.'
  FROM pg_temp._tmp_pre_bank_cancel_component_restore_apply AS component_restore_apply;

  INSERT INTO public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  SELECT
    released_reservations.finance_case_id,
    released_reservations.finance_component_id,
    'RESERVATION_RELEASED',
    v_now,
    p_actor_user_id,
    v_work_item.pay_batch_id,
    released_reservations.reservation_id,
    jsonb_build_object('reservation_status', 'RESERVED_OR_COMMITTED'),
    jsonb_build_object(
      'reservation_status', 'RELEASED',
      'released_reason', 'PRE_BANK_CANCEL',
      'work_item_id', p_work_item_id
    ),
    'PRE_BANK_CANCEL',
    'Payment correction pre-bank cancellation released reservation.'
  FROM pg_temp._tmp_pre_bank_cancel_released_reservations AS released_reservations
  WHERE released_reservations.finance_case_id IS NOT NULL;

  UPDATE public.pay_advances AS payout_cases_to_reset
  SET
    payout_status = 'PENDING',
    payout_pay_batch_id = NULL,
    payout_transfer_id = NULL,
    updated_at = v_now
  WHERE payout_cases_to_reset.id IN (
    SELECT DISTINCT selected_payout_cases.finance_case_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_payout_cases
    WHERE selected_payout_cases.finance_case_id IS NOT NULL
  )
    AND COALESCE(payout_cases_to_reset.payout_status::text, '') <> 'PAID'
    AND (
      payout_cases_to_reset.payout_pay_batch_id = v_work_item.pay_batch_id
      OR payout_cases_to_reset.payout_transfer_id IN (
        SELECT DISTINCT selected_payout_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_payout_transfers
        WHERE selected_payout_transfers.pay_bank_transfer_id IS NOT NULL
      )
    );

  GET DIAGNOSTICS v_reset_payout_count = ROW_COUNT;

  SELECT jsonb_build_object(
    'scope_type', v_scope_type,
    'work_unit', COALESCE(NULLIF(v_work_unit, ''), v_scope_type, 'UNKNOWN'),
    'pay_batch_id', v_work_item.pay_batch_id::text,
    'pay_batch_ids', jsonb_build_array(v_work_item.pay_batch_id::text),
    'is_whole_batch', v_is_whole_batch_work_item,
    'selected_candidate_scope_complete', (
      v_scope_type = 'CANDIDATES'
      AND NOT (COALESCE(v_work_item.selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.candidate_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.umbrella_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.umbrella_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.finance_case_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.finance_component_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.reservation_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.payout_transfer_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.payout_transfer_id IS NOT NULL
        UNION
        SELECT DISTINCT pre_bank_selected.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE pre_bank_selected.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT pre_bank_selected.transfer_group_key AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS pre_bank_selected
        WHERE NULLIF(btrim(COALESCE(pre_bank_selected.transfer_group_key, '')), '') IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb)
  )
  INTO v_mail_selected_scope_json;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_mail_scope_matches;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_mail_scope_matches ON COMMIT DROP AS
  WITH candidate_mail AS (
    SELECT
      public.mail_outbox.id,
      public.mail_outbox.status::text AS status,
      public.mail_outbox.type,
      public.mail_outbox.email_type,
      public.mail_outbox.context_kind,
      public.mail_outbox.context_id,
      public.mail_outbox.recipient_kind,
      public.mail_outbox.recipient_id,
      public.mail_outbox.reference,
      COALESCE(public.mail_outbox.payment_scope_json, '{}'::jsonb) AS payment_scope_json
    FROM public.mail_outbox
    WHERE upper(btrim(COALESCE(public.mail_outbox.status::text, ''))) = 'QUEUED'
      AND lower(concat_ws('|', public.mail_outbox.type, public.mail_outbox.email_type, public.mail_outbox.context_kind, public.mail_outbox.reference, COALESCE(public.mail_outbox.payment_scope_json::text, '{}'))) LIKE ANY (
        ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%']
      )
  ), matched_mail AS (
    SELECT
      candidate_mail.id AS mail_outbox_id,
      candidate_mail.status,
      candidate_mail.type,
      candidate_mail.email_type,
      candidate_mail.context_kind,
      candidate_mail.context_id,
      candidate_mail.recipient_kind,
      candidate_mail.recipient_id,
      candidate_mail.reference,
      candidate_mail.payment_scope_json,
      mail_match.match_result
    FROM candidate_mail
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_correction_mail_scope_match(
        candidate_mail.id,
        v_work_item.pay_batch_id,
        v_work_item.selection_json,
        v_mail_selected_scope_json,
        false
      ) AS match_result
    ) AS mail_match
  )
  SELECT
    matched_mail.mail_outbox_id,
    matched_mail.status,
    matched_mail.type,
    matched_mail.email_type,
    matched_mail.context_kind,
    matched_mail.context_id,
    matched_mail.recipient_kind,
    matched_mail.recipient_id,
    matched_mail.reference,
    matched_mail.payment_scope_json,
    COALESCE(matched_mail.match_result->>'match_kind', 'NONE') AS match_kind,
    COALESCE(matched_mail.match_result->>'match_confidence', 'NONE') AS match_confidence,
    COALESCE(NULLIF(matched_mail.match_result->>'safe_to_cancel', '')::boolean, false) AS safe_to_cancel,
    COALESCE(NULLIF(matched_mail.match_result->>'requires_review', '')::boolean, false) AS requires_review,
    COALESCE(matched_mail.match_result->>'reason', 'NO_SCOPE_MATCH') AS match_reason,
    matched_mail.match_result
  FROM matched_mail
  WHERE COALESCE(NULLIF(matched_mail.match_result->>'matched', '')::boolean, false);

  UPDATE public.mail_outbox AS queued_mail_to_cancel
  SET
    status = 'FAILED',
    failed_at = COALESCE(queued_mail_to_cancel.failed_at, v_now),
    last_error = 'CANCELLED_INTERNAL_PAYMENT_CORRECTION'
  FROM pg_temp._tmp_pre_bank_cancel_mail_scope_matches AS mail_scope_match
  WHERE queued_mail_to_cancel.id = mail_scope_match.mail_outbox_id
    AND upper(btrim(COALESCE(queued_mail_to_cancel.status::text, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.safe_to_cancel, false)
    AND (
      mail_scope_match.match_confidence = 'EXACT'
      OR (
        v_is_whole_batch_work_item
        AND mail_scope_match.match_kind = 'WHOLE_BATCH'
      )
    );

  GET DIAGNOSTICS v_cancelled_mail_count = ROW_COUNT;

  SELECT count(*)::integer
  INTO v_communications_review_required_count
  FROM pg_temp._tmp_pre_bank_cancel_mail_scope_matches AS mail_scope_match
  WHERE upper(btrim(COALESCE(mail_scope_match.status, ''))) = 'QUEUED'
    AND COALESCE(mail_scope_match.requires_review, false);

  v_mail_scope_matching := jsonb_build_object(
    'exact_cancelled', v_cancelled_mail_count,
    'legacy_review', v_communications_review_required_count,
    'selected_scope_json', v_mail_selected_scope_json,
    'matches', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', mail_scope_match.mail_outbox_id,
        'match_kind', mail_scope_match.match_kind,
        'match_confidence', mail_scope_match.match_confidence,
        'safe_to_cancel', mail_scope_match.safe_to_cancel,
        'requires_review', mail_scope_match.requires_review,
        'reason', mail_scope_match.match_reason,
        'status', mail_scope_match.status,
        'type', mail_scope_match.type,
        'email_type', mail_scope_match.email_type,
        'context_kind', mail_scope_match.context_kind,
        'context_id', mail_scope_match.context_id,
        'recipient_kind', mail_scope_match.recipient_kind,
        'recipient_id', mail_scope_match.recipient_id,
        'reference', mail_scope_match.reference,
        'payment_scope_json', mail_scope_match.payment_scope_json
      ) ORDER BY mail_scope_match.mail_outbox_id)
      FROM pg_temp._tmp_pre_bank_cancel_mail_scope_matches AS mail_scope_match
    ), '[]'::jsonb)
  );

  WITH affected_transfers AS (
    SELECT DISTINCT selected_transfer_amounts.pay_bank_transfer_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_transfer_amounts
    WHERE selected_transfer_amounts.pay_bank_transfer_id IS NOT NULL
  ),
  recalculated_transfers AS (
    SELECT
      affected_transfers.pay_bank_transfer_id,
      round(COALESCE(sum(COALESCE(public.pay_batch_items.amount_inc_vat, 0)), 0), 2)::numeric AS remaining_amount
    FROM affected_transfers
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_bank_transfer_id = affected_transfers.pay_bank_transfer_id
     AND COALESCE(public.pay_batch_items.is_voided, false) = false
    GROUP BY affected_transfers.pay_bank_transfer_id
  )
  UPDATE public.pay_bank_transfers AS transfer_to_recalculate
  SET
    amount = recalculated_transfers.remaining_amount,
    status = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN 'VOIDED'
      ELSE transfer_to_recalculate.status
    END,
    failed_reason = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN COALESCE(transfer_to_recalculate.failed_reason, 'PRE_BANK_CANCEL_VOIDED')
      ELSE transfer_to_recalculate.failed_reason
    END,
    rail_meta_json = CASE
      WHEN recalculated_transfers.remaining_amount = 0
           AND upper(btrim(COALESCE(transfer_to_recalculate.status, ''))) NOT IN ('COMPLETED')
        THEN COALESCE(transfer_to_recalculate.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'pre_bank_cancel_applied', true,
          'pre_bank_cancel_work_item_id', p_work_item_id::text,
          'pre_bank_cancel_at_utc', v_now,
          'pre_bank_cancel_status_note', 'Transfer amount became zero after selected pre-bank cancellation; status set to VOIDED.'
        )
      ELSE transfer_to_recalculate.rail_meta_json
    END
  FROM recalculated_transfers
  WHERE transfer_to_recalculate.id = recalculated_transfers.pay_bank_transfer_id;

  GET DIAGNOSTICS v_recalculated_transfer_count = ROW_COUNT;

  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_affected_candidates;
  CREATE TEMP TABLE _tmp_pre_bank_cancel_affected_candidates ON COMMIT DROP AS
  SELECT DISTINCT
    selected_affected_candidates.pay_batch_candidate_id,
    selected_affected_candidates.candidate_id
  FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_affected_candidates
  WHERE selected_affected_candidates.pay_batch_candidate_id IS NOT NULL;

  WITH affected_candidate_sums AS (
    SELECT
      public.pay_batch_candidates.id AS pay_batch_candidate_id,
      count(public.pay_batch_items.id) FILTER (
        WHERE COALESCE(public.pay_batch_items.is_voided, false) = false
      )::integer AS active_item_count,
      bool_or(
        COALESCE(public.pay_batch_items.is_voided, false) = false
        AND upper(btrim(COALESCE(public.pay_batch_items.pay_channel, ''))) = 'PAYE'
      ) AS has_active_paye_item,
      bool_or(
        COALESCE(public.pay_batch_items.is_voided, false) = false
        AND upper(btrim(COALESCE(public.pay_batch_items.pay_channel, ''))) = 'UMBRELLA'
      ) AS has_active_umbrella_item,
      round(COALESCE(sum(
        CASE
          WHEN COALESCE(public.pay_batch_items.is_voided, false) = false
           AND public.pay_batch_items.item_type NOT IN ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT', 'LOAN_REPAYMENT', 'MANUAL_DEBT_RECOVERY', 'MANUAL_CREDIT_PAYOUT', 'LOAN_PAYOUT', 'DEBT_CREATED')
          THEN COALESCE(public.pay_batch_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS earnings_ex,
      round(COALESCE(sum(
        CASE
          WHEN COALESCE(public.pay_batch_items.is_voided, false) = false
           AND public.pay_batch_items.item_type <> 'DEBT_CREATED'
          THEN COALESCE(public.pay_batch_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS earnings_inc,
      round(COALESCE(sum(
        CASE
          WHEN COALESCE(public.pay_batch_items.is_voided, false) = false
           AND COALESCE(public.pay_batch_items.paye_treatment, 'NONE') IN ('GROSS_ADD', 'GROSS_DEDUCT')
          THEN COALESCE(public.pay_batch_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS gross_adjustments_ex_sum,
      round(COALESCE(sum(
        CASE
          WHEN COALESCE(public.pay_batch_items.is_voided, false) = false
           AND COALESCE(public.pay_batch_items.paye_treatment, 'NONE') IN ('NET_ADD', 'NET_DEDUCT')
          THEN COALESCE(public.pay_batch_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS net_adjustments_ex_sum,
      round(COALESCE(sum(
        CASE
          WHEN COALESCE(public.pay_batch_items.is_voided, false) = false
           AND public.pay_batch_items.item_type = 'OVERPAYMENT_RECOVERY'
          THEN -COALESCE(public.pay_batch_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS overpayment_recovery_taken_ex,
      round(COALESCE(sum(
        CASE
          WHEN COALESCE(public.pay_batch_items.is_voided, false) = false
           AND public.pay_batch_items.item_type = 'LOAN_REPAYMENT'
          THEN -COALESCE(public.pay_batch_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS loan_repayment_taken_ex,
      round(COALESCE(sum(
        CASE
          WHEN COALESCE(public.pay_batch_items.is_voided, false) = false
           AND public.pay_batch_items.item_type = 'DEBT_CREATED'
          THEN COALESCE(public.pay_batch_items.amount_inc_vat, public.pay_batch_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS debt_created_sum
    FROM pg_temp._tmp_pre_bank_cancel_affected_candidates AS affected_candidates
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = affected_candidates.pay_batch_candidate_id
     AND public.pay_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_batch_candidate_id = public.pay_batch_candidates.id
    GROUP BY public.pay_batch_candidates.id
  ),
  paye_net AS (
    SELECT
      public.pay_batch_paye_net_inputs.pay_batch_candidate_id,
      public.pay_batch_paye_net_inputs.net_amount::numeric(12,2) AS net_amount
    FROM public.pay_batch_paye_net_inputs
    WHERE public.pay_batch_paye_net_inputs.pay_batch_candidate_id IN (
      SELECT affected_candidates.pay_batch_candidate_id
      FROM pg_temp._tmp_pre_bank_cancel_affected_candidates AS affected_candidates
    )
  ),
  candidate_summary_update AS (
    UPDATE public.pay_batch_candidates AS candidates_to_refresh
    SET
      awaiting_net_amount = CASE
        WHEN COALESCE(affected_candidate_sums.active_item_count, 0) = 0 THEN false
        WHEN COALESCE(affected_candidate_sums.has_active_paye_item, false) = true
          THEN NOT EXISTS (
            SELECT 1
            FROM public.pay_batch_paye_net_inputs AS paye_net_input_check
            WHERE paye_net_input_check.pay_batch_candidate_id = candidates_to_refresh.id
          )
        ELSE false
      END,
      gross_preview = CASE
        WHEN COALESCE(affected_candidate_sums.active_item_count, 0) = 0 THEN 0::numeric(12,2)
        WHEN COALESCE(affected_candidate_sums.has_active_paye_item, false) = true
          THEN round(COALESCE(affected_candidate_sums.earnings_ex, 0) + COALESCE(affected_candidate_sums.gross_adjustments_ex_sum, 0), 2)::numeric(12,2)
        ELSE greatest(COALESCE(affected_candidate_sums.earnings_inc, 0), 0)::numeric(12,2)
      END,
      net_bank_amount = CASE
        WHEN COALESCE(affected_candidate_sums.active_item_count, 0) = 0 THEN 0::numeric(12,2)
        WHEN COALESCE(affected_candidate_sums.has_active_paye_item, false) = true THEN
          CASE
            WHEN NOT EXISTS (
              SELECT 1
              FROM public.pay_batch_paye_net_inputs AS paye_net_input_check
              WHERE paye_net_input_check.pay_batch_candidate_id = candidates_to_refresh.id
            ) THEN NULL::numeric
            ELSE greatest(round(COALESCE(paye_net.net_amount, 0) + COALESCE(affected_candidate_sums.net_adjustments_ex_sum, 0), 2), 0)::numeric(12,2)
          END
        ELSE greatest(COALESCE(affected_candidate_sums.earnings_inc, 0), 0)::numeric(12,2)
      END,
      debt_created = COALESCE(affected_candidate_sums.debt_created_sum, 0)::numeric(12,2),
      overpayment_recovery_taken = COALESCE(affected_candidate_sums.overpayment_recovery_taken_ex, 0)::numeric(12,2),
      loan_repayment_taken = COALESCE(affected_candidate_sums.loan_repayment_taken_ex, 0)::numeric(12,2),
      mismatch_settlement_choice = NULL,
      updated_at = v_now
    FROM affected_candidate_sums
    LEFT JOIN paye_net
      ON paye_net.pay_batch_candidate_id = affected_candidate_sums.pay_batch_candidate_id
    WHERE candidates_to_refresh.id = affected_candidate_sums.pay_batch_candidate_id
      AND candidates_to_refresh.pay_batch_id = v_work_item.pay_batch_id
    RETURNING candidates_to_refresh.id
  )
  SELECT count(*)::integer
  INTO v_summary_refresh_candidate_count
  FROM candidate_summary_update;

  SELECT
    count(public.pay_batch_items.id)::integer,
    round(COALESCE(sum(COALESCE(public.pay_batch_items.amount_inc_vat, 0)), 0), 2)::numeric
  INTO
    v_active_batch_item_count_after,
    v_active_batch_amount_inc_vat_after
  FROM public.pay_batch_items
  JOIN public.pay_batch_candidates
    ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
  WHERE public.pay_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
    AND COALESCE(public.pay_batch_items.is_voided, false) = false;

  v_batch_empty_after := COALESCE(v_active_batch_item_count_after, 0) = 0;

  UPDATE public.pay_batches AS batch_to_refresh
  SET
    total_bank_out = COALESCE((
      SELECT round(COALESCE(sum(COALESCE(public.pay_batch_candidates.net_bank_amount, 0)), 0), 2)::numeric
      FROM public.pay_batch_candidates
      WHERE public.pay_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
    ), 0)::numeric,
    total_debt_created = COALESCE((
      SELECT round(COALESCE(sum(COALESCE(public.pay_batch_candidates.debt_created, 0)), 0), 2)::numeric
      FROM public.pay_batch_candidates
      WHERE public.pay_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
    ), 0)::numeric
  WHERE batch_to_refresh.id = v_work_item.pay_batch_id;

  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)
  SELECT
    'pay_candidate:' || dirty_candidates.candidate_id::text,
    1,
    v_now
  FROM (
    SELECT DISTINCT selected_dirty_candidates.candidate_id
    FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_dirty_candidates
    WHERE selected_dirty_candidates.candidate_id IS NOT NULL
  ) AS dirty_candidates
  ON CONFLICT (entity_key)
  DO UPDATE
  SET
    seq = public.app_change_counters.seq + 1,
    updated_at = v_now;

  GET DIAGNOSTICS v_dirty_candidate_count = ROW_COUNT;
  -- Workbench refresh is staged once by PROCESS_CHUNK/REFRESH_WORKBENCH.
  v_refresh_result := jsonb_build_object(
    'status', 'PENDING_OPERATION_REFRESH',
    'owner', 'pay_payment_correction_process_chunk'
  );

  v_changed_scope_json := jsonb_build_object(
    'pay_batch_id', v_work_item.pay_batch_id::text,
    'correction_request_id', v_work_item.correction_request_id::text,
    'work_item_id', p_work_item_id::text,
    'change_kind', 'PRE_BANK_CANCEL',
    'changed_pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope
        WHERE selected_scope.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'changed_pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope
        WHERE selected_scope.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'changed_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.candidate_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope
        WHERE selected_scope.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'changed_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope
        WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'changed_finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_case_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope
        WHERE selected_scope.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'changed_finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_component_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope
        WHERE selected_scope.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'changed_reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.reservation_id::text AS value_text
        FROM pg_temp._tmp_pre_bank_cancel_selected AS selected_scope
        WHERE selected_scope.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'voided_item_count', COALESCE(v_voided_item_count, 0),
    'released_reservation_count', COALESCE(v_released_reservation_count, 0),
    'restored_component_count', COALESCE(v_restored_component_count, 0),
    'carry_forward_created_count', COALESCE(v_carry_forward_created_count, 0),
    'carry_forward_existing_count', COALESCE(v_carry_forward_existing_count, 0),
    'carry_forward_released_count', COALESCE(v_carry_forward_released_count, 0)
  );


  v_result := jsonb_build_object(
    'ok', true,
    'status', 'APPLIED',
    'result_code', 'APPLIED',
    'work_item_id', p_work_item_id,
    'candidate_id', v_work_item.candidate_id,
    'expected_item_count', v_selected_item_count,
    'applied_item_count', v_inserted_correction_item_count,
    'released_reservations', v_released_reservation_count,
    'restored_finance_components', v_restored_component_count,
    'carry_forward_created', COALESCE(v_carry_forward_created_count, 0),
    'carry_forward_existing', COALESCE(v_carry_forward_existing_count, 0),
    'carry_forward_released', COALESCE(v_carry_forward_released_count, 0),
    'blocker', NULL,
    'correction_request_id', v_work_item.correction_request_id,
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_item_kind', 'PRE_BANK_CANCEL',
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'voided_item_count', v_voided_item_count,
    'inserted_correction_item_count', v_inserted_correction_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'candidate_scope_contract_version', v_candidate_scope_contract_version,
    'candidate_scope_hash_version', v_candidate_scope_hash_version,
    'source_row_count_semantics', v_source_row_count_semantics,
    'communication_cleanup_contract_version', CASE
      WHEN v_candidate_scope_contract_version = 2 THEN 1 ELSE NULL::integer END,
    'matching_queued_count', v_matching_queued_count,
    'cancelled_queued_count', v_cancelled_mail_count,
    'already_sent_untouched_count', v_already_sent_untouched_count,
    'other_terminal_untouched_count', v_other_terminal_untouched_count,
    'unsafe_queued_count', v_unsafe_queued_count,
    'communication_review_required', v_unsafe_queued_count > 0,
    'mail_scope_matching', v_mail_scope_matching,
    'recalculated_transfer_count', v_recalculated_transfer_count,
    'summary_refresh_candidate_count', v_summary_refresh_candidate_count,
    'active_batch_item_count_after', COALESCE(v_active_batch_item_count_after, 0),
    'active_batch_amount_inc_vat_after', COALESCE(v_active_batch_amount_inc_vat_after, 0),
    'batch_empty_after', COALESCE(v_batch_empty_after, false),
    'dirty_candidate_count', v_dirty_candidate_count,
    'manual_adjustment_support_details_json', COALESCE(v_manual_adjustment_result, '{}'::jsonb),
    'carry_forward_created_count', COALESCE(v_carry_forward_created_count, 0),
    'carry_forward_existing_count', COALESCE(v_carry_forward_existing_count, 0),
    'carry_forward_released_count', COALESCE(v_carry_forward_released_count, 0),
    'carry_forward_create_result', COALESCE(v_carry_forward_create_result, '{}'::jsonb),
    'carry_forward_release_result', COALESCE(v_carry_forward_release_result, '{}'::jsonb),
    'changed_scope_json', COALESCE(v_changed_scope_json, '{}'::jsonb),
    'resolved_scope', COALESCE(v_resolved_scope_json, '{}'::jsonb),
    'classification_result', v_classification_result,
    'same_request_partial_continuation', v_same_request_partial_continuation,
    'same_request_continuation_proof', jsonb_build_object(
      'original_expected_json_count', v_original_expected_json_count,
      'original_expected_valid_count', v_original_expected_valid_count,
      'original_expected_belongs_count', v_original_expected_belongs_count,
      'original_expected_active_count', v_original_expected_active_count,
      'original_expected_same_request_voided_count', v_original_expected_same_request_voided_count,
      'original_expected_disallowed_state_count', v_original_expected_disallowed_state_count,
      'active_outside_original_count', v_active_outside_original_count,
      'current_expected_outside_original_count', v_current_expected_outside_original_count,
      'applied_sibling_count', v_applied_sibling_count
    ),
    'applied_at_utc', v_now
  );

  UPDATE public.pay_payment_correction_work_items AS applied_work_item
  SET
    status = 'APPLIED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = COALESCE(applied_work_item.processed_at_utc, v_now),
    last_error = NULL,
    result_json = COALESCE(applied_work_item.result_json, '{}'::jsonb) || v_result
  WHERE applied_work_item.id = p_work_item_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WORK_RESULT',
    v_result,
    'pay_payment_correction',
    p_work_item_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PRE_BANK_CANCEL_WORK_ERROR',
      jsonb_build_object(
        'work_item_id', p_work_item_id,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;
ALTER FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) TO service_role;
