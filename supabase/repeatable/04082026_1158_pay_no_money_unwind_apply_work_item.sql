-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Preserves the installed function identity; no overload is added.

CREATE OR REPLACE FUNCTION public.pay_no_money_unwind_apply_work_item(p_work_item_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
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
  v_updated_transfer_count integer := 0;
  v_updated_candidate_count integer := 0;
  v_dirty_candidate_count integer := 0;
  v_notice_group_id uuid := NULL::uuid;
  v_quiet_minutes integer := 10;
  v_max_wait_minutes integer := 60;
  v_has_settlement_evidence boolean := false;
  v_has_aggregate_subset_blocker boolean := false;
  v_provider_submission_in_progress boolean := false;
  v_provider_evidence_result jsonb := '{}'::jsonb;
  v_provider_evidence_class text := NULL::text;
  v_provider_evidence_cash_state text := NULL::text;
  v_provider_evidence_blocker_code text := NULL::text;
  v_provider_evidence_submitted boolean := false;
  v_provider_evidence_pending boolean := false;
  v_provider_evidence_unknown boolean := false;
  v_rail_state_summary_json jsonb := '{}'::jsonb;
  v_rail_final_paid_count integer := 0;
  v_rail_terminal_no_money_count integer := 0;
  v_rail_pending_non_final_count integer := 0;
  v_rail_unknown_count integer := 0;
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
  v_finance_resolution_result jsonb := NULL::jsonb;
  v_notice_queue_result jsonb := NULL::jsonb;
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
  v_provider_failure_reason_group text := NULL::text;
  v_provider_failure_reason_groups_json jsonb := '[]'::jsonb;
  v_changed_scope_json jsonb := '{}'::jsonb;
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_workbench_refresh_results jsonb := '[]'::jsonb;
  v_workbench_refresh_job_ids jsonb := '[]'::jsonb;
  v_workbench_refresh_queued_count integer := 0;
  v_workbench_refresh_deferred_count integer := 0;
  v_workbench_refresh_failed_count integer := 0;
  v_workbench_refresh_status text := 'NOT_REQUIRED';
  v_workbench_requires_session boolean := false;
  v_membership_item_mismatch_count integer := 0;
  v_current_active_item_count integer := 0;
  v_current_source_row_count integer := 0;
  v_capacity_selected_scope_json jsonb := '{}'::jsonb;
  v_current_candidate_scope_hash text;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_START',
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

  IF v_work_item.work_kind <> 'NO_MONEY_UNWIND' THEN
    v_blocker := jsonb_build_object(
      'code', 'WORK_ITEM_KIND_NOT_NO_MONEY_UNWIND',
      'message', 'This work item is not a no-money unwind work item.',
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

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_selected;
  CREATE TEMP TABLE _tmp_no_money_unwind_selected ON COMMIT DROP AS
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
  ) AS selected_rows;

  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (umbrella_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (transfer_group_key);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (reservation_id);
  CREATE INDEX ON pg_temp._tmp_no_money_unwind_selected (finance_component_id);

  SELECT
    count(*)::integer,
    count(DISTINCT no_money_selected.candidate_id) FILTER (WHERE no_money_selected.candidate_id IS NOT NULL)::integer,
    count(DISTINCT no_money_selected.pay_bank_transfer_id) FILTER (WHERE no_money_selected.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT no_money_selected.reservation_id) FILTER (WHERE no_money_selected.reservation_id IS NOT NULL)::integer,
    count(DISTINCT no_money_selected.finance_component_id) FILTER (WHERE no_money_selected.finance_component_id IS NOT NULL)::integer
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_reservation_count,
    v_selected_finance_component_count
  FROM pg_temp._tmp_no_money_unwind_selected AS no_money_selected;

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
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_membership_items
      )
      UNION ALL
      (
        SELECT selected_membership_items.pay_batch_item_id
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_membership_items
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
      'code', 'NO_SELECTED_PAYMENT_ITEMS_FOR_NO_MONEY_UNWIND',
      'message', 'No selectable pay_batch_items were resolved for the no-money unwind work item.'
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

    UPDATE public.pay_payment_correction_work_items AS no_money_invalid_expected_count_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_money_invalid_expected_count_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_money_invalid_expected_count_work.id = p_work_item_id;

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

    UPDATE public.pay_payment_correction_work_items AS no_money_expected_count_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_money_expected_count_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_money_expected_count_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;

IF v_work_item.selection_json ? 'expected_pay_batch_item_ids'
   AND COALESCE(jsonb_typeof(v_work_item.selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids must be a JSON array.'
  );

  UPDATE public.pay_payment_correction_work_items AS no_money_expected_ids_type_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(no_money_expected_ids_type_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE no_money_expected_ids_type_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_expected_items;
CREATE TEMP TABLE _tmp_no_money_unwind_expected_items ON COMMIT DROP AS
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
FROM pg_temp._tmp_no_money_unwind_expected_items AS invalid_expected_items
WHERE invalid_expected_items.pay_batch_item_id IS NULL;

IF v_expected_item_mismatch_count > 0 THEN
  v_blocker := jsonb_build_object(
    'code', 'WORK_SELECTION_DRIFT',
    'message', 'Correction work item expected_pay_batch_item_ids contains invalid UUID values.',
    'invalid_expected_item_count', v_expected_item_mismatch_count
  );

  UPDATE public.pay_payment_correction_work_items AS no_money_invalid_expected_ids_work
  SET
    status = 'BLOCKED',
    locked_at_utc = NULL,
    locked_by = NULL,
    processed_at_utc = v_now,
    last_error = v_blocker->>'message',
    result_json = COALESCE(no_money_invalid_expected_ids_work.result_json, '{}'::jsonb) || jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'processed_at_utc', v_now
    )
  WHERE no_money_invalid_expected_ids_work.id = p_work_item_id;

  RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
END IF;

SELECT count(*)::integer
INTO v_expected_item_id_count
FROM pg_temp._tmp_no_money_unwind_expected_items AS expected_item_count
WHERE expected_item_count.pay_batch_item_id IS NOT NULL;

IF v_expected_item_id_count > 0 THEN
  SELECT count(*)::integer
  INTO v_expected_item_mismatch_count
  FROM (
    (
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_expected_items AS expected_items
      WHERE expected_items.pay_batch_item_id IS NOT NULL
      EXCEPT
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_items
    )
    UNION ALL
    (
      SELECT selected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_items
      EXCEPT
      SELECT expected_items.pay_batch_item_id
      FROM pg_temp._tmp_no_money_unwind_expected_items AS expected_items
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

    UPDATE public.pay_payment_correction_work_items AS no_money_expected_ids_drift_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(no_money_expected_ids_drift_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'processed_at_utc', v_now
      )
    WHERE no_money_expected_ids_drift_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;
END IF;


  PERFORM 1
  FROM public.pay_batch_candidates AS locked_batch_candidates
  WHERE locked_batch_candidates.id IN (
    SELECT DISTINCT lock_selected_candidates.pay_batch_candidate_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_candidates
    WHERE lock_selected_candidates.pay_batch_candidate_id IS NOT NULL
  )
  FOR UPDATE OF locked_batch_candidates;

  PERFORM 1
  FROM public.pay_batch_items AS locked_batch_items
  JOIN pg_temp._tmp_no_money_unwind_selected AS lock_selected_items
    ON lock_selected_items.pay_batch_item_id = locked_batch_items.id
  ORDER BY locked_batch_items.id
  FOR UPDATE OF locked_batch_items;

  PERFORM 1
  FROM public.pay_bank_transfers AS locked_bank_transfers
  WHERE locked_bank_transfers.id IN (
    SELECT DISTINCT lock_selected_transfers.pay_bank_transfer_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_transfers
    WHERE lock_selected_transfers.pay_bank_transfer_id IS NOT NULL
  )
  FOR UPDATE OF locked_bank_transfers;

  PERFORM 1
  FROM public.pay_advance_reservations AS locked_advance_reservations
  WHERE locked_advance_reservations.id IN (
    SELECT DISTINCT lock_selected_reservations.reservation_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_reservations
    WHERE lock_selected_reservations.reservation_id IS NOT NULL
  )
  FOR UPDATE OF locked_advance_reservations;

  PERFORM 1
  FROM public.pay_finance_case_components AS locked_finance_case_components
  WHERE locked_finance_case_components.id IN (
    SELECT DISTINCT lock_selected_components.finance_component_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_components
    WHERE lock_selected_components.finance_component_id IS NOT NULL
  )
  FOR UPDATE OF locked_finance_case_components;

  PERFORM 1
  FROM public.pay_advances AS locked_pay_advances
  WHERE locked_pay_advances.id IN (
    SELECT DISTINCT lock_selected_cases.finance_case_id
    FROM pg_temp._tmp_no_money_unwind_selected AS lock_selected_cases
    WHERE lock_selected_cases.finance_case_id IS NOT NULL
  )
  FOR UPDATE OF locked_pay_advances;

  -- Recalculate the exact candidate-owned scope under the correction guard and
  -- row locks before the first release/financial write.
  v_capacity_selected_scope_json := jsonb_build_object(
    'scope_type', 'CANDIDATES',
    'work_unit', 'CANDIDATE',
    'pay_batch_ids', jsonb_build_array(v_work_item.pay_batch_id),
    'pay_batch_candidate_ids', jsonb_build_array(v_work_item.pay_batch_candidate_id),
    'candidate_ids', COALESCE((SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text) FROM (SELECT DISTINCT selected_scope.candidate_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.candidate_id IS NOT NULL) AS scope_value), '[]'::jsonb),
    'pay_batch_item_ids', COALESCE((SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text) FROM (SELECT DISTINCT selected_scope.pay_batch_item_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope) AS scope_value), '[]'::jsonb),
    'umbrella_ids', COALESCE((SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text) FROM (SELECT DISTINCT selected_scope.umbrella_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.umbrella_id IS NOT NULL) AS scope_value), '[]'::jsonb),
    'finance_case_ids', COALESCE((SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text) FROM (SELECT DISTINCT selected_scope.finance_case_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.finance_case_id IS NOT NULL) AS scope_value), '[]'::jsonb),
    'finance_component_ids', COALESCE((SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text) FROM (SELECT DISTINCT selected_scope.finance_component_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.finance_component_id IS NOT NULL) AS scope_value), '[]'::jsonb),
    'reservation_ids', COALESCE((SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text) FROM (SELECT DISTINCT selected_scope.reservation_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.reservation_id IS NOT NULL) AS scope_value), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
        UNION SELECT DISTINCT advance_scope.payout_transfer_id::text AS value_text FROM public.pay_advances AS advance_scope JOIN pg_temp._tmp_no_money_unwind_selected AS selected_scope ON selected_scope.finance_case_id = advance_scope.id WHERE advance_scope.payout_transfer_id IS NOT NULL
      ) AS scope_value
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text)
      FROM (
        SELECT DISTINCT advance_scope.payout_transfer_id::text AS value_text FROM public.pay_advances AS advance_scope JOIN pg_temp._tmp_no_money_unwind_selected AS selected_scope ON selected_scope.finance_case_id = advance_scope.id WHERE advance_scope.payout_transfer_id IS NOT NULL
        UNION SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS scope_value
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((SELECT jsonb_agg(scope_value.value_text ORDER BY scope_value.value_text) FROM (SELECT DISTINCT selected_scope.transfer_group_key AS value_text FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL) AS scope_value), '[]'::jsonb)
  );

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_capacity_mail_scope;
  CREATE TEMP TABLE _tmp_no_money_capacity_mail_scope ON COMMIT DROP AS
  SELECT mail_row.id AS mail_outbox_id
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

  PERFORM 1 FROM public.mail_outbox AS locked_mail JOIN pg_temp._tmp_no_money_capacity_mail_scope AS capacity_mail ON capacity_mail.mail_outbox_id = locked_mail.id FOR UPDATE OF locked_mail;
  PERFORM 1
  FROM public.pay_batch_items AS locked_instruction_items
  LEFT JOIN public.pay_bank_transfers AS instruction_transfer ON instruction_transfer.id = locked_instruction_items.pay_bank_transfer_id
  WHERE locked_instruction_items.id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope)
     OR locked_instruction_items.pay_bank_transfer_id IN (
          SELECT selected_scope.pay_bank_transfer_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
          UNION SELECT advance_scope.payout_transfer_id FROM public.pay_advances AS advance_scope JOIN pg_temp._tmp_no_money_unwind_selected AS selected_scope ON selected_scope.finance_case_id = advance_scope.id WHERE advance_scope.payout_transfer_id IS NOT NULL
     )
     OR instruction_transfer.transfer_group_key IN (SELECT selected_scope.transfer_group_key FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL)
  FOR SHARE OF locked_instruction_items;

  PERFORM 1 FROM public.pay_batch_item_breakdowns AS locked_source WHERE locked_source.pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.pay_batch_timesheet_snapshots AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND locked_source.candidate_id = v_work_item.candidate_id FOR SHARE OF locked_source;
  PERFORM 1 FROM public.timesheet_pay_state_history AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND locked_source.timesheet_id IN (SELECT selected_scope.timesheet_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.timesheet_id IS NOT NULL) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.pay_manual_adjustment_carry_forwards AS locked_source WHERE locked_source.source_pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope) OR locked_source.target_pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope) FOR UPDATE OF locked_source;
  PERFORM 1 FROM public.pay_bank_transfer_events AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND (locked_source.pay_bank_transfer_id IN (SELECT selected_scope.pay_bank_transfer_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL UNION SELECT advance_scope.payout_transfer_id FROM public.pay_advances AS advance_scope JOIN pg_temp._tmp_no_money_unwind_selected AS selected_scope ON selected_scope.finance_case_id = advance_scope.id WHERE advance_scope.payout_transfer_id IS NOT NULL) OR locked_source.candidate_id = v_work_item.candidate_id) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.banking_pay_operation_transfer_scope AS locked_source WHERE locked_source.pay_batch_id = v_work_item.pay_batch_id AND (locked_source.pay_bank_transfer_id IN (SELECT selected_scope.pay_bank_transfer_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE selected_scope.pay_bank_transfer_id IS NOT NULL UNION SELECT advance_scope.payout_transfer_id FROM public.pay_advances AS advance_scope JOIN pg_temp._tmp_no_money_unwind_selected AS selected_scope ON selected_scope.finance_case_id = advance_scope.id WHERE advance_scope.payout_transfer_id IS NOT NULL) OR locked_source.transfer_group_key IN (SELECT selected_scope.transfer_group_key FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL)) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.banking_pay_operation_transfer_scope_items AS locked_source WHERE locked_source.pay_batch_item_id IN (SELECT selected_scope.pay_batch_item_id FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope) FOR SHARE OF locked_source;
  PERFORM 1 FROM public.pay_batch_paye_net_inputs AS locked_source WHERE locked_source.pay_batch_candidate_id = v_work_item.pay_batch_candidate_id FOR SHARE OF locked_source;

  SELECT count(*)::integer INTO v_current_active_item_count
  FROM public.pay_batch_items AS current_item
  WHERE current_item.id = ANY(v_membership.pay_batch_item_ids)
    AND COALESCE(current_item.is_voided, false) IS NOT TRUE;

  WITH selected_items AS (SELECT selected_scope.* FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope),
  transfer_ids AS (
    SELECT selected_item.pay_bank_transfer_id AS id FROM selected_items AS selected_item WHERE selected_item.pay_bank_transfer_id IS NOT NULL
    UNION SELECT advance_scope.payout_transfer_id FROM public.pay_advances AS advance_scope JOIN selected_items AS selected_item ON selected_item.finance_case_id = advance_scope.id WHERE advance_scope.payout_transfer_id IS NOT NULL
  ),
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
    UNION ALL SELECT count(*) FROM pg_temp._tmp_no_money_capacity_mail_scope
  ) SELECT COALESCE(sum(source_count.row_count), 0)::integer INTO v_current_source_row_count FROM source_counts AS source_count;

  SELECT private.pay_payment_correction_sha256_v1(
    jsonb_build_object(
      'version', 1,
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
        v_work_item.selection_json->'cancellation_reversion_pre_request_authority'->>'authority_digest'
    )
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
      'message', 'No-money unwind requires the full frozen unpaid payment scope so linked finance, reservations, transfers, and manual adjustment lines are not left behind.',
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

  SELECT
    jsonb_build_object(
      'evidence_class', provider_evidence.evidence_class,
      'provider_submitted', provider_evidence.provider_submitted,
      'provider_request_sent', provider_evidence.provider_request_sent,
      'provider_response_present', provider_evidence.provider_response_present,
      'provider_event_present', provider_evidence.provider_event_present,
      'provider_external_id_present', provider_evidence.provider_external_id_present,
      'local_prepared_only', provider_evidence.local_prepared_only,
      'cash_state', provider_evidence.cash_state,
      'blocker_code', provider_evidence.blocker_code,
      'reason', provider_evidence.reason,
      'support_details_json', provider_evidence.support_details_json
    ),
    provider_evidence.evidence_class,
    provider_evidence.cash_state,
    provider_evidence.blocker_code,
    COALESCE(provider_evidence.provider_submitted, false),
    COALESCE(provider_evidence.cash_state, '') = 'PENDING_NON_FINAL',
    (
      COALESCE(provider_evidence.blocker_code, '') = 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER'
      OR provider_evidence.evidence_class = 'PROVIDER_OUTCOME_UNKNOWN'
      OR (
        COALESCE(provider_evidence.cash_state, 'UNKNOWN') = 'UNKNOWN'
        AND provider_evidence.evidence_class IN (
          'PROVIDER_REQUEST_SENT',
          'PROVIDER_RESPONSE_PRESENT',
          'PROVIDER_EVENT_PRESENT',
          'PROVIDER_EXTERNAL_ID_PRESENT',
          'PROVIDER_SUBMITTED'
        )
      )
    )
  INTO
    v_provider_evidence_result,
    v_provider_evidence_class,
    v_provider_evidence_cash_state,
    v_provider_evidence_blocker_code,
    v_provider_evidence_submitted,
    v_provider_evidence_pending,
    v_provider_evidence_unknown
  FROM public._pay_bank_transfer_provider_evidence_classify(
    v_work_item.pay_batch_id,
    NULL::uuid,
    v_resolved_scope_json,
    v_effective_actor_user_id
  ) AS provider_evidence
  LIMIT 1;

  v_provider_evidence_result := COALESCE(v_provider_evidence_result, '{}'::jsonb);

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_transfer_money_state;
  CREATE TEMP TABLE _tmp_no_money_unwind_transfer_money_state ON COMMIT DROP AS
  SELECT
    transfer_rows.id AS pay_bank_transfer_id,
    transfer_rows.status AS transfer_status,
    transfer_rows.rail_state AS transfer_rail_state,
    money_state.cash_state,
    money_state.normalised_transfer_status,
    money_state.is_final_money_moved,
    money_state.is_terminal_no_money,
    money_state.is_pending_non_final,
    money_state.completed_at_allowed,
    money_state.reason,
    money_state.support_details_json
  FROM public.pay_bank_transfers AS transfer_rows
  JOIN (
    SELECT DISTINCT selected_transfers.pay_bank_transfer_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_transfers
    WHERE selected_transfers.pay_bank_transfer_id IS NOT NULL
  ) AS selected_transfer_ids
    ON selected_transfer_ids.pay_bank_transfer_id = transfer_rows.id
  CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
    transfer_rows.status,
    transfer_rows.rail_state,
    COALESCE(transfer_rows.rail_meta_json, '{}'::jsonb),
    COALESCE(transfer_rows.rail_meta_json, '{}'::jsonb)
  ) AS money_state;

  SELECT
    jsonb_build_object(
      'selected_transfer_count', count(*)::integer,
      'final_paid_count', count(*) FILTER (WHERE COALESCE(transfer_money_state.is_final_money_moved, false))::integer,
      'terminal_no_money_count', count(*) FILTER (WHERE COALESCE(transfer_money_state.is_terminal_no_money, false))::integer,
      'pending_non_final_count', count(*) FILTER (WHERE COALESCE(transfer_money_state.is_pending_non_final, false))::integer,
      'unknown_count', count(*) FILTER (
        WHERE COALESCE(transfer_money_state.cash_state, 'UNKNOWN') = 'UNKNOWN'
          AND COALESCE(transfer_money_state.is_final_money_moved, false) IS NOT TRUE
          AND COALESCE(transfer_money_state.is_terminal_no_money, false) IS NOT TRUE
          AND COALESCE(transfer_money_state.is_pending_non_final, false) IS NOT TRUE
      )::integer,
      'states', COALESCE(jsonb_agg(
        jsonb_build_object(
          'pay_bank_transfer_id', transfer_money_state.pay_bank_transfer_id::text,
          'transfer_status', transfer_money_state.transfer_status,
          'transfer_rail_state', transfer_money_state.transfer_rail_state,
          'cash_state', transfer_money_state.cash_state,
          'normalised_transfer_status', transfer_money_state.normalised_transfer_status,
          'is_final_money_moved', transfer_money_state.is_final_money_moved,
          'is_terminal_no_money', transfer_money_state.is_terminal_no_money,
          'is_pending_non_final', transfer_money_state.is_pending_non_final,
          'completed_at_allowed', transfer_money_state.completed_at_allowed,
          'reason', transfer_money_state.reason,
          'support_details_json', transfer_money_state.support_details_json
        )
        ORDER BY transfer_money_state.pay_bank_transfer_id::text
      ), '[]'::jsonb)
    ),
    count(*) FILTER (WHERE COALESCE(transfer_money_state.is_final_money_moved, false))::integer,
    count(*) FILTER (WHERE COALESCE(transfer_money_state.is_terminal_no_money, false))::integer,
    count(*) FILTER (WHERE COALESCE(transfer_money_state.is_pending_non_final, false))::integer,
    count(*) FILTER (
      WHERE COALESCE(transfer_money_state.cash_state, 'UNKNOWN') = 'UNKNOWN'
        AND COALESCE(transfer_money_state.is_final_money_moved, false) IS NOT TRUE
        AND COALESCE(transfer_money_state.is_terminal_no_money, false) IS NOT TRUE
        AND COALESCE(transfer_money_state.is_pending_non_final, false) IS NOT TRUE
    )::integer
  INTO
    v_rail_state_summary_json,
    v_rail_final_paid_count,
    v_rail_terminal_no_money_count,
    v_rail_pending_non_final_count,
    v_rail_unknown_count
  FROM pg_temp._tmp_no_money_unwind_transfer_money_state AS transfer_money_state;

  v_rail_state_summary_json := COALESCE(v_rail_state_summary_json, jsonb_build_object(
    'selected_transfer_count', 0,
    'final_paid_count', 0,
    'terminal_no_money_count', 0,
    'pending_non_final_count', 0,
    'unknown_count', 0,
    'states', '[]'::jsonb
  ));

  WITH selected_transfer_failure_reasons AS (
    SELECT DISTINCT
      COALESCE(
        NULLIF(BTRIM(COALESCE(bank_event.provider_failure_reason_group, '')), ''),
        NULLIF(BTRIM(COALESCE(bank_transfer.rail_meta_json #>> '{provider_failure_reason_group}', '')), ''),
        NULLIF(BTRIM(COALESCE(bank_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_failure_reason_group}', '')), ''),
        public._banking_provider_failure_reason_normalise(
          COALESCE(bank_event.provider_key, bank_transfer.rail_provider, v_batch.rail_provider_snapshot),
          COALESCE(bank_event.provider_state, bank_event.normalised_state, bank_transfer.rail_state, bank_transfer.status),
          COALESCE(bank_event.provider_failure_reason_code, bank_event.raw_payload #>> '{provider_error_code}', bank_transfer.rail_meta_json #>> '{provider_error_code}', bank_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_error_code}'),
          COALESCE(bank_event.raw_payload #>> '{provider_error_message_redacted}', bank_event.raw_payload #>> '{failed_reason}', bank_transfer.failed_reason, bank_transfer.rail_meta_json #>> '{provider_error_message_redacted}', bank_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_error_message_redacted}'),
          COALESCE(bank_event.raw_payload, bank_transfer.rail_meta_json, '{}'::jsonb)
        )->>'failure_reason_group'
      ) AS failure_reason_group
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
    JOIN public.pay_bank_transfers AS bank_transfer
      ON bank_transfer.id = selected_scope.pay_bank_transfer_id
     AND bank_transfer.pay_batch_id = v_work_item.pay_batch_id
    LEFT JOIN LATERAL (
      SELECT event_rows.*
      FROM public.pay_bank_transfer_events AS event_rows
      WHERE event_rows.pay_bank_transfer_id = bank_transfer.id
        AND event_rows.pay_batch_id = v_work_item.pay_batch_id
      ORDER BY event_rows.received_at_utc DESC NULLS LAST,
               event_rows.event_time_utc DESC NULLS LAST,
               event_rows.id DESC
      LIMIT 1
    ) AS bank_event ON TRUE
    WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
  )
  SELECT
    (
      SELECT selected_reasons.failure_reason_group
      FROM selected_transfer_failure_reasons AS selected_reasons
      WHERE NULLIF(BTRIM(COALESCE(selected_reasons.failure_reason_group, '')), '') IS NOT NULL
      ORDER BY selected_reasons.failure_reason_group
      LIMIT 1
    ),
    COALESCE((
      SELECT jsonb_agg(to_jsonb(selected_reasons.failure_reason_group) ORDER BY selected_reasons.failure_reason_group)
      FROM selected_transfer_failure_reasons AS selected_reasons
      WHERE NULLIF(BTRIM(COALESCE(selected_reasons.failure_reason_group, '')), '') IS NOT NULL
    ), '[]'::jsonb)
  INTO
    v_provider_failure_reason_group,
    v_provider_failure_reason_groups_json;

  IF COALESCE(v_provider_evidence_cash_state, '') = 'FINAL_PAID'
     OR COALESCE(v_rail_final_paid_count, 0) > 0 THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_HAS_SETTLEMENT_EVIDENCE',
      'message', 'No-money unwind cannot apply because selected scope has final paid/settled evidence.',
      'provider_evidence_result', v_provider_evidence_result,
      'rail_state_summary', v_rail_state_summary_json
    );

    UPDATE public.pay_payment_correction_work_items AS final_paid_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(final_paid_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'provider_evidence_result', v_provider_evidence_result,
        'rail_state_summary', v_rail_state_summary_json,
        'processed_at_utc', v_now
      )
    WHERE final_paid_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'provider_evidence_result', v_provider_evidence_result,
      'rail_state_summary', v_rail_state_summary_json
    );
  END IF;

  IF COALESCE(v_provider_evidence_pending, false)
     OR (
       COALESCE(v_rail_pending_non_final_count, 0) > 0
       AND COALESCE(v_provider_evidence_cash_state, '') <> 'TERMINAL_NO_MONEY'
     ) THEN
    v_blocker := jsonb_build_object(
      'code', 'PROVIDER_SUBMITTED_PENDING',
      'message', 'No-money unwind cannot apply because selected scope still has pending non-final provider/rail evidence and no stronger terminal no-money provider evidence for this scope.',
      'provider_evidence_result', v_provider_evidence_result,
      'rail_state_summary', v_rail_state_summary_json
    );

    UPDATE public.pay_payment_correction_work_items AS pending_provider_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(pending_provider_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'provider_evidence_result', v_provider_evidence_result,
        'rail_state_summary', v_rail_state_summary_json,
        'processed_at_utc', v_now
      )
    WHERE pending_provider_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'provider_evidence_result', v_provider_evidence_result,
      'rail_state_summary', v_rail_state_summary_json
    );
  END IF;

  IF COALESCE(v_provider_evidence_unknown, false)
     OR (
       COALESCE(v_rail_unknown_count, 0) > 0
       AND COALESCE(v_provider_evidence_cash_state, '') <> 'TERMINAL_NO_MONEY'
     ) THEN
    v_blocker := jsonb_build_object(
      'code', 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER',
      'message', 'No-money unwind cannot apply because selected scope has unknown provider/rail outcome evidence and no stronger terminal no-money provider evidence for this scope.',
      'provider_evidence_result', v_provider_evidence_result,
      'rail_state_summary', v_rail_state_summary_json
    );

    UPDATE public.pay_payment_correction_work_items AS unknown_provider_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(unknown_provider_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'provider_evidence_result', v_provider_evidence_result,
        'rail_state_summary', v_rail_state_summary_json,
        'processed_at_utc', v_now
      )
    WHERE unknown_provider_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'blocker', v_blocker,
      'provider_evidence_result', v_provider_evidence_result,
      'rail_state_summary', v_rail_state_summary_json
    );
  END IF;

  v_classification_result := public._pay_payment_movement_classify(
    v_work_item.pay_batch_id,
    COALESCE(v_work_item.selection_json, '{}'::jsonb) || jsonb_build_object(
      'requested_action', 'UNWIND_FAILED_PAYMENT',
      'source_context', 'WORK_ITEM_APPLY'
    )
  );

  v_classification := COALESCE(v_classification_result->>'classification', COALESCE(v_classification_result->>'payment_lifecycle_state', 'AMBIGUOUS_REVIEW_REQUIRED'));

  IF v_classification NOT IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY') THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_CLASSIFICATION_REQUIRED',
      'message', 'Selected scope is no longer classified as terminal no-money provider failure/cancellation.',
      'classification', v_classification,
      'classification_result', v_classification_result
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

  IF COALESCE((v_classification_result#>>'{evidence,settlement,has_settlement_evidence}')::boolean, false) THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_HAS_SETTLEMENT_EVIDENCE',
      'message', 'No-money unwind cannot apply because selected scope now has settlement evidence.'
    );

    UPDATE public.pay_payment_correction_work_items AS settlement_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(settlement_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE settlement_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(COALESCE(v_classification_result->'blockers', '[]'::jsonb)) AS blocker_elements(blocker_value)
    WHERE COALESCE(blocker_elements.blocker_value->>'code', '') = 'AGGREGATE_UMBRELLA_TRANSFER_SUBSET_SELECTED'
  )
  INTO v_has_aggregate_subset_blocker;

  IF COALESCE(v_has_aggregate_subset_blocker, false) THEN
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_AGGREGATE_TRANSFER_SUBSET_BLOCKED',
      'message', 'Provider failure maps to an aggregate transfer, but the selected scope is only a subset. Apply to the whole transfer or use manual evidence review.',
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS aggregate_subset_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(aggregate_subset_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE aggregate_subset_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_operations AS provider_submit_operation
    WHERE provider_submit_operation.pay_batch_id = v_work_item.pay_batch_id
      AND provider_submit_operation.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
      AND provider_submit_operation.phase IN ('SUBMIT_PROVIDER_TRANSFERS', 'PROVIDER_SUBMIT', 'CLAIM_PROVIDER_SUBMIT', 'EXECUTE_PROVIDER')
      AND provider_submit_operation.status IN ('QUEUED', 'RUNNING', 'PROCESSING', 'CLAIMED')
    LIMIT 1
  )
  INTO v_provider_submission_in_progress;

  IF COALESCE(v_provider_submission_in_progress, false) THEN
    v_blocker := jsonb_build_object(
      'code', 'PROVIDER_SUBMISSION_IN_PROGRESS',
      'message', 'No-money unwind cannot apply while provider submission is claimed or in progress for this batch.',
      'classification_result', v_classification_result
    );

    UPDATE public.pay_payment_correction_work_items AS provider_submit_in_progress_blocked_work
    SET
      status = 'BLOCKED',
      locked_at_utc = NULL,
      locked_by = NULL,
      processed_at_utc = v_now,
      last_error = v_blocker->>'message',
      result_json = COALESCE(provider_submit_in_progress_blocked_work.result_json, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', v_blocker,
        'classification_result', v_classification_result,
        'processed_at_utc', v_now
      )
    WHERE provider_submit_in_progress_blocked_work.id = p_work_item_id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker, 'classification_result', v_classification_result);
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
      'message', 'No-money unwind is blocked because one or more source-less manual adjustments cannot be safely carried forward.',
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
    'NO_MONEY_UNWIND'
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
    'NO_MONEY_UNWIND',
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
  FROM pg_temp._tmp_no_money_unwind_selected AS selected_for_ledger
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
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_to_void
  )
    AND COALESCE(items_to_void.is_voided, false) = false;

  GET DIAGNOSTICS v_voided_item_count = ROW_COUNT;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_released_reservations;
  CREATE TEMP TABLE _tmp_no_money_unwind_released_reservations ON COMMIT DROP AS
  WITH release_candidates AS (
    SELECT DISTINCT
      public.pay_advance_reservations.id,
      public.pay_advance_reservations.finance_case_id,
      public.pay_advance_reservations.finance_component_id,
      public.pay_advance_reservations.pay_batch_item_id,
      public.pay_advance_reservations.reserved_amount,
      public.pay_advance_reservations.reserved_source_amount
    FROM public.pay_advance_reservations
    JOIN pg_temp._tmp_no_money_unwind_selected AS selected_reservation_items
      ON selected_reservation_items.reservation_id = public.pay_advance_reservations.id
      OR selected_reservation_items.pay_batch_item_id = public.pay_advance_reservations.pay_batch_item_id
    WHERE public.pay_advance_reservations.pay_batch_id = v_work_item.pay_batch_id
      AND public.pay_advance_reservations.settled_at_utc IS NULL
      AND upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) <> 'SETTLED'
      AND upper(btrim(COALESCE(public.pay_advance_reservations.status, ''))) <> 'RELEASED'
  ),
  released_rows AS (
    UPDATE public.pay_advance_reservations AS reservations_to_release
    SET
      status = 'RELEASED',
      released_at_utc = COALESCE(reservations_to_release.released_at_utc, v_now),
      released_reason = 'NO_MONEY_UNWIND',
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
  FROM pg_temp._tmp_no_money_unwind_released_reservations AS released_reservation_count;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_component_restore;
  CREATE TEMP TABLE _tmp_no_money_unwind_component_restore ON COMMIT DROP AS
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
    FROM pg_temp._tmp_no_money_unwind_released_reservations AS released_reservations
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.id = released_reservations.pay_batch_item_id
  ) AS restore_source
  WHERE restore_source.finance_component_id IS NOT NULL
    AND restore_source.restore_source_amount > 0
  GROUP BY restore_source.finance_component_id, restore_source.finance_case_id;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_component_restore_apply;
  CREATE TEMP TABLE _tmp_no_money_unwind_component_restore_apply ON COMMIT DROP AS
  SELECT
    public.pay_finance_case_components.id AS finance_component_id,
    public.pay_finance_case_components.finance_case_id,
    public.pay_finance_case_components.remaining_source_amount AS remaining_before,
    /* A no-money unwind releases a separate reservation record.  Reservation
       creation does not decrement remaining_source_amount, so the unwind must
       not add that reservation to the component and resurrect settled value. */
    COALESCE(public.pay_finance_case_components.remaining_source_amount, 0) AS remaining_after,
    component_restore.restore_source_amount
  FROM pg_temp._tmp_no_money_unwind_component_restore AS component_restore
  JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = component_restore.finance_component_id;

  UPDATE public.pay_finance_case_components AS components_to_restore
  SET
    remaining_source_amount = component_restore_apply.remaining_after,
    resolved_at_utc = CASE WHEN component_restore_apply.remaining_after > 0 THEN NULL ELSE components_to_restore.resolved_at_utc END,
    closed_at_utc = NULL,
    updated_at_utc = v_now
  FROM pg_temp._tmp_no_money_unwind_component_restore_apply AS component_restore_apply
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
      'correction_kind', 'NO_MONEY_UNWIND',
      'work_item_id', p_work_item_id
    ),
    'NO_MONEY_UNWIND',
    'Payment correction no-money unwind restored reserved component amount.'
  FROM pg_temp._tmp_no_money_unwind_component_restore_apply AS component_restore_apply;

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
      'released_reason', 'NO_MONEY_UNWIND',
      'work_item_id', p_work_item_id
    ),
    'NO_MONEY_UNWIND',
    'Payment correction no-money unwind released reservation.'
  FROM pg_temp._tmp_no_money_unwind_released_reservations AS released_reservations
  WHERE released_reservations.finance_case_id IS NOT NULL;

  UPDATE public.pay_advances AS payout_cases_to_reset
  SET
    payout_status = 'PENDING',
    payout_pay_batch_id = NULL,
    payout_transfer_id = NULL,
    updated_at = v_now
  WHERE payout_cases_to_reset.id IN (
    SELECT DISTINCT selected_payout_cases.finance_case_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_payout_cases
    WHERE selected_payout_cases.finance_case_id IS NOT NULL
  )
    AND COALESCE(payout_cases_to_reset.payout_status::text, '') <> 'PAID'
    AND (
      payout_cases_to_reset.payout_pay_batch_id = v_work_item.pay_batch_id
      OR payout_cases_to_reset.payout_transfer_id IN (
        SELECT DISTINCT selected_payout_transfers.pay_bank_transfer_id
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_payout_transfers
        WHERE selected_payout_transfers.pay_bank_transfer_id IS NOT NULL
      )
    );

  GET DIAGNOSTICS v_reset_payout_count = ROW_COUNT;

  UPDATE public.pay_bank_transfers AS transfer_to_mark_failed
  SET
    status = CASE
      WHEN upper(btrim(COALESCE(transfer_to_mark_failed.status, ''))) = 'COMPLETED' THEN transfer_to_mark_failed.status
      ELSE 'FAILED'
    END,
    failed_reason = COALESCE(NULLIF(btrim(COALESCE(transfer_to_mark_failed.failed_reason, '')), ''), 'NO_MONEY_UNWIND'),
    rail_meta_json = COALESCE(transfer_to_mark_failed.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
      'no_money_unwind_applied', true,
      'no_money_unwind_work_item_id', p_work_item_id::text,
      'no_money_unwind_at_utc', v_now
    )
  WHERE transfer_to_mark_failed.id IN (
    SELECT DISTINCT selected_transfer_to_fail.pay_bank_transfer_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_transfer_to_fail
    WHERE selected_transfer_to_fail.pay_bank_transfer_id IS NOT NULL
  )
    AND upper(btrim(COALESCE(transfer_to_mark_failed.status, ''))) <> 'COMPLETED';

  GET DIAGNOSTICS v_updated_transfer_count = ROW_COUNT;

  UPDATE public.pay_batch_candidates AS candidates_to_clear
  SET
    settlement_status = CASE
      WHEN upper(btrim(COALESCE(candidates_to_clear.settlement_status, ''))) = 'SETTLED' THEN NULL
      ELSE candidates_to_clear.settlement_status
    END,
    settled_at_utc = NULL,
    settled_via = NULL,
    settled_note = COALESCE(NULLIF(btrim(COALESCE(candidates_to_clear.settled_note, '')), ''), 'NO_MONEY_UNWIND'),
    updated_at = v_now
  WHERE candidates_to_clear.id IN (
    SELECT DISTINCT selected_candidate_to_clear.pay_batch_candidate_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_candidate_to_clear
    WHERE selected_candidate_to_clear.pay_batch_candidate_id IS NOT NULL
  )
    AND NOT EXISTS (
      SELECT 1
      FROM public.timesheet_pay_state_history
      JOIN pg_temp._tmp_no_money_unwind_selected AS selected_history_check
        ON selected_history_check.timesheet_id = public.timesheet_pay_state_history.timesheet_id
      WHERE public.timesheet_pay_state_history.pay_batch_id = v_work_item.pay_batch_id
        AND selected_history_check.pay_batch_candidate_id = candidates_to_clear.id
    );

  GET DIAGNOSTICS v_updated_candidate_count = ROW_COUNT;


  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_affected_candidates;
  CREATE TEMP TABLE _tmp_no_money_unwind_affected_candidates ON COMMIT DROP AS
  SELECT DISTINCT
    selected_affected_candidates.pay_batch_candidate_id,
    selected_affected_candidates.candidate_id
  FROM pg_temp._tmp_no_money_unwind_selected AS selected_affected_candidates
  WHERE selected_affected_candidates.pay_batch_candidate_id IS NOT NULL;

  WITH active_candidate_items AS (
    SELECT
      public.pay_batch_candidates.id AS pay_batch_candidate_id,
      public.pay_batch_items.id AS pay_batch_item_id,
      public.pay_batch_items.item_type,
      public.pay_batch_items.amount_ex_vat,
      public.pay_batch_items.amount_inc_vat,
      public.pay_batch_items.pay_channel,
      public.pay_batch_items.paye_treatment
    FROM pg_temp._tmp_no_money_unwind_affected_candidates AS affected_candidates
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = affected_candidates.pay_batch_candidate_id
     AND public.pay_batch_candidates.pay_batch_id = v_work_item.pay_batch_id
    LEFT JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_batch_candidate_id = public.pay_batch_candidates.id
     AND COALESCE(public.pay_batch_items.is_voided, false) = false
     AND NOT EXISTS (
       SELECT 1
       FROM public.pay_payment_correction_items AS applied_candidate_item_corrections
       WHERE applied_candidate_item_corrections.pay_batch_item_id = public.pay_batch_items.id
         AND applied_candidate_item_corrections.status = 'APPLIED'
         AND applied_candidate_item_corrections.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
     )
  ),
  affected_candidate_sums AS (
    SELECT
      active_candidate_items.pay_batch_candidate_id,
      count(active_candidate_items.pay_batch_item_id)::integer AS active_item_count,
      COALESCE(bool_or(upper(btrim(COALESCE(active_candidate_items.pay_channel, ''))) = 'PAYE') FILTER (WHERE active_candidate_items.pay_batch_item_id IS NOT NULL), false) AS has_active_paye_item,
      COALESCE(bool_or(upper(btrim(COALESCE(active_candidate_items.pay_channel, ''))) = 'UMBRELLA') FILTER (WHERE active_candidate_items.pay_batch_item_id IS NOT NULL), false) AS has_active_umbrella_item,
      round(COALESCE(sum(
        CASE
          WHEN active_candidate_items.pay_batch_item_id IS NOT NULL
           AND active_candidate_items.item_type NOT IN ('OVERPAYMENT_RECOVERY', 'LOAN_REPAYMENT', 'MANUAL_DEBT_RECOVERY', 'MANUAL_CREDIT_PAYOUT', 'LOAN_PAYOUT', 'UNDERPAYMENT_PAYMENT', 'DEBT_CREATED')
          THEN COALESCE(active_candidate_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS earnings_ex,
      round(COALESCE(sum(
        CASE
          WHEN active_candidate_items.pay_batch_item_id IS NOT NULL
           AND active_candidate_items.item_type <> 'DEBT_CREATED'
          THEN COALESCE(active_candidate_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS earnings_inc,
      round(COALESCE(sum(
        CASE
          WHEN active_candidate_items.pay_batch_item_id IS NOT NULL
           AND COALESCE(active_candidate_items.paye_treatment, 'NONE') IN ('GROSS_ADD', 'GROSS_DEDUCT')
          THEN COALESCE(active_candidate_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS gross_adjustments_ex_sum,
      round(COALESCE(sum(
        CASE
          WHEN active_candidate_items.pay_batch_item_id IS NOT NULL
           AND COALESCE(active_candidate_items.paye_treatment, 'NONE') IN ('NET_ADD', 'NET_DEDUCT')
          THEN COALESCE(active_candidate_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS net_adjustments_ex_sum,
      round(COALESCE(sum(
        CASE
          WHEN active_candidate_items.pay_batch_item_id IS NOT NULL
           AND active_candidate_items.item_type = 'OVERPAYMENT_RECOVERY'
          THEN -COALESCE(active_candidate_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS overpayment_recovery_taken_ex,
      round(COALESCE(sum(
        CASE
          WHEN active_candidate_items.pay_batch_item_id IS NOT NULL
           AND active_candidate_items.item_type = 'LOAN_REPAYMENT'
          THEN -COALESCE(active_candidate_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS loan_repayment_taken_ex,
      round(COALESCE(sum(
        CASE
          WHEN active_candidate_items.pay_batch_item_id IS NOT NULL
           AND active_candidate_items.item_type = 'DEBT_CREATED'
          THEN COALESCE(active_candidate_items.amount_inc_vat, active_candidate_items.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(12,2) AS debt_created_sum
    FROM active_candidate_items
    GROUP BY active_candidate_items.pay_batch_candidate_id
  ),
  paye_net AS (
    SELECT
      public.pay_batch_paye_net_inputs.pay_batch_candidate_id,
      public.pay_batch_paye_net_inputs.net_amount::numeric(12,2) AS net_amount
    FROM public.pay_batch_paye_net_inputs
    WHERE public.pay_batch_paye_net_inputs.pay_batch_candidate_id IN (
      SELECT affected_candidates.pay_batch_candidate_id
      FROM pg_temp._tmp_no_money_unwind_affected_candidates AS affected_candidates
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
    AND COALESCE(public.pay_batch_items.is_voided, false) = false
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_batch_item_corrections
      WHERE applied_batch_item_corrections.pay_batch_item_id = public.pay_batch_items.id
        AND applied_batch_item_corrections.status = 'APPLIED'
        AND applied_batch_item_corrections.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
    );

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
        SELECT DISTINCT selected_scope.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.candidate_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.umbrella_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.umbrella_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_case_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.finance_component_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.reservation_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT selected_scope.transfer_group_key AS value_text
        FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
        WHERE NULLIF(btrim(COALESCE(selected_scope.transfer_group_key, '')), '') IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb)
  )
  INTO v_mail_selected_scope_json;

  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_mail_scope_matches;
  CREATE TEMP TABLE _tmp_no_money_unwind_mail_scope_matches ON COMMIT DROP AS
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
  FROM pg_temp._tmp_no_money_unwind_mail_scope_matches AS mail_scope_match
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
  FROM pg_temp._tmp_no_money_unwind_mail_scope_matches AS mail_scope_match
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
      FROM pg_temp._tmp_no_money_unwind_mail_scope_matches AS mail_scope_match
    ), '[]'::jsonb)
  );


  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)
  SELECT
    'pay_candidate:' || dirty_candidates.candidate_id::text,
    1,
    v_now
  FROM (
    SELECT DISTINCT selected_dirty_candidates.candidate_id
    FROM pg_temp._tmp_no_money_unwind_selected AS selected_dirty_candidates
    WHERE selected_dirty_candidates.candidate_id IS NOT NULL
  ) AS dirty_candidates
  ON CONFLICT (entity_key)
  DO UPDATE
  SET
    seq = public.app_change_counters.seq + 1,
    updated_at = v_now;

  GET DIAGNOSTICS v_dirty_candidate_count = ROW_COUNT;




v_finance_resolution_result := jsonb_build_object(
  'ok', true,
  'skipped', true,
  'reason', 'NO_MONEY_UNWIND_DOES_NOT_APPLY_CASE_RESOLUTION',
  'message', 'No-money unwind mechanically releases failed frozen authority and does not require or apply Case Resolution.'
);

-- Workbench refresh is staged once by PROCESS_CHUNK/REFRESH_WORKBENCH.
v_workbench_refresh_status := 'PENDING_OPERATION_REFRESH';
v_workbench_refresh_results := jsonb_build_array(jsonb_build_object(
  'status', 'PENDING_OPERATION_REFRESH',
  'owner', 'pay_payment_correction_process_chunk'
));
v_workbench_refresh_queued_count := 0;
v_workbench_refresh_deferred_count := 0;
v_workbench_refresh_failed_count := 0;

v_changed_scope_json := jsonb_build_object(
  'pay_batch_id', v_work_item.pay_batch_id::text,
  'correction_request_id', v_work_item.correction_request_id::text,
  'work_item_id', p_work_item_id::text,
  'change_kind', 'NO_MONEY_UNWIND',
  'provider_failure_reason_group', v_provider_failure_reason_group,
  'provider_failure_reason_groups_json', COALESCE(v_provider_failure_reason_groups_json, '[]'::jsonb),
  'changed_pay_batch_item_ids', COALESCE((
    SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
    FROM (
      SELECT DISTINCT selected_scope.pay_batch_item_id::text AS value_text
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
      WHERE selected_scope.pay_batch_item_id IS NOT NULL
    ) AS selected_values
  ), '[]'::jsonb),
  'changed_pay_batch_candidate_ids', COALESCE((
    SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
    FROM (
      SELECT DISTINCT selected_scope.pay_batch_candidate_id::text AS value_text
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
      WHERE selected_scope.pay_batch_candidate_id IS NOT NULL
    ) AS selected_values
  ), '[]'::jsonb),
  'changed_candidate_ids', COALESCE((
    SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
    FROM (
      SELECT DISTINCT selected_scope.candidate_id::text AS value_text
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
      WHERE selected_scope.candidate_id IS NOT NULL
    ) AS selected_values
  ), '[]'::jsonb),
  'changed_transfer_ids', COALESCE((
    SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
    FROM (
      SELECT DISTINCT selected_scope.pay_bank_transfer_id::text AS value_text
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
      WHERE selected_scope.pay_bank_transfer_id IS NOT NULL
    ) AS selected_values
  ), '[]'::jsonb),
  'changed_finance_case_ids', COALESCE((
    SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
    FROM (
      SELECT DISTINCT selected_scope.finance_case_id::text AS value_text
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
      WHERE selected_scope.finance_case_id IS NOT NULL
    ) AS selected_values
  ), '[]'::jsonb),
  'changed_finance_component_ids', COALESCE((
    SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
    FROM (
      SELECT DISTINCT selected_scope.finance_component_id::text AS value_text
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
      WHERE selected_scope.finance_component_id IS NOT NULL
    ) AS selected_values
  ), '[]'::jsonb),
  'changed_reservation_ids', COALESCE((
    SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
    FROM (
      SELECT DISTINCT selected_scope.reservation_id::text AS value_text
      FROM pg_temp._tmp_no_money_unwind_selected AS selected_scope
      WHERE selected_scope.reservation_id IS NOT NULL
    ) AS selected_values
  ), '[]'::jsonb),
  'voided_item_count', COALESCE(v_voided_item_count, 0),
  'released_reservation_count', COALESCE(v_released_reservation_count, 0),
  'restored_component_count', COALESCE(v_restored_component_count, 0),
  'updated_transfer_count', COALESCE(v_updated_transfer_count, 0),
  'updated_candidate_count', COALESCE(v_updated_candidate_count, 0),
  'carry_forward_created_count', COALESCE(v_carry_forward_created_count, 0),
  'carry_forward_existing_count', COALESCE(v_carry_forward_existing_count, 0),
  'carry_forward_released_count', COALESCE(v_carry_forward_released_count, 0),
  'workbench_refresh_status', COALESCE(v_workbench_refresh_status, 'NOT_REQUIRED'),
  'workbench_refresh_queued_count', COALESCE(v_workbench_refresh_queued_count, 0),
  'workbench_refresh_deferred_count', COALESCE(v_workbench_refresh_deferred_count, 0),
  'workbench_refresh_failed_count', COALESCE(v_workbench_refresh_failed_count, 0),
  'workbench_refresh_job_ids', COALESCE(v_workbench_refresh_job_ids, '[]'::jsonb),
  'workbench_refresh_results', COALESCE(v_workbench_refresh_results, '[]'::jsonb),
  'requires_workbench_session', COALESCE(v_workbench_requires_session, false)
);

v_notice_queue_result := public.pay_payment_return_admin_notice_queue(
  p_notice_kind => 'NO_MONEY_UNWIND_APPLIED',
  p_pay_batch_id => v_work_item.pay_batch_id,
  p_provider_key => COALESCE(v_batch.rail_provider_snapshot, 'UNKNOWN'),
  p_execution_commit_ref => v_batch.execution_commit_ref,
  p_summary_json => jsonb_build_object(
    'pay_batch_id', v_work_item.pay_batch_id,
    'correction_request_id', v_work_item.correction_request_id,
    'work_item_id', p_work_item_id,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'voided_item_count', v_voided_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'summary_refresh_candidate_count', v_summary_refresh_candidate_count,
    'active_batch_item_count_after', COALESCE(v_active_batch_item_count_after, 0),
    'active_batch_amount_inc_vat_after', COALESCE(v_active_batch_amount_inc_vat_after, 0),
    'batch_empty_after', COALESCE(v_batch_empty_after, false),
    'accepted_finance_resolution', v_finance_resolution_result,
    'provider_failure_reason_group', v_provider_failure_reason_group,
    'provider_failure_reason_groups_json', COALESCE(v_provider_failure_reason_groups_json, '[]'::jsonb),
    'changed_scope_json', COALESCE(v_changed_scope_json, '{}'::jsonb),
    'workbench_refresh_status', COALESCE(v_workbench_refresh_status, 'NOT_REQUIRED'),
    'workbench_refresh_queued_count', COALESCE(v_workbench_refresh_queued_count, 0),
    'workbench_refresh_deferred_count', COALESCE(v_workbench_refresh_deferred_count, 0),
    'workbench_refresh_failed_count', COALESCE(v_workbench_refresh_failed_count, 0),
    'workbench_refresh_job_ids', COALESCE(v_workbench_refresh_job_ids, '[]'::jsonb),
    'requires_workbench_session', COALESCE(v_workbench_requires_session, false),
    'applied_at_utc', v_now
  )
);

v_notice_group_id := NULLIF(v_notice_queue_result->>'notice_group_id', '')::uuid;


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
    'correction_item_kind', 'NO_MONEY_UNWIND',
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'voided_item_count', v_voided_item_count,
    'failed_transfer_count', v_updated_transfer_count,
    'voided_or_failed_transfer_count', v_updated_transfer_count,
    'inserted_correction_item_count', v_inserted_correction_item_count,
    'released_reservation_count', v_released_reservation_count,
    'restored_component_count', v_restored_component_count,
    'reset_payout_count', v_reset_payout_count,
    'updated_transfer_count', v_updated_transfer_count,
    'updated_candidate_count', v_updated_candidate_count,
    'cancelled_mail_count', v_cancelled_mail_count,
    'communications_cancelled', v_cancelled_mail_count,
    'communications_review_required', v_communications_review_required_count,
    'mail_scope_matching', v_mail_scope_matching,
    'dirty_candidate_count', v_dirty_candidate_count,
    'freshness_dirtied', COALESCE(v_dirty_candidate_count, 0) > 0,
    'freshness_dirty_candidate_count', COALESCE(v_dirty_candidate_count, 0),
    'blockers', '[]'::jsonb,
    'manual_adjustment_support_details_json', COALESCE(v_manual_adjustment_result, '{}'::jsonb),
    'carry_forward_created_count', COALESCE(v_carry_forward_created_count, 0),
    'carry_forward_existing_count', COALESCE(v_carry_forward_existing_count, 0),
    'carry_forward_released_count', COALESCE(v_carry_forward_released_count, 0),
    'carry_forward_create_result', COALESCE(v_carry_forward_create_result, '{}'::jsonb),
    'carry_forward_release_result', COALESCE(v_carry_forward_release_result, '{}'::jsonb),
    'provider_failure_reason_group', v_provider_failure_reason_group,
    'provider_failure_reason_groups_json', COALESCE(v_provider_failure_reason_groups_json, '[]'::jsonb),
    'changed_scope_json', COALESCE(v_changed_scope_json, '{}'::jsonb),
    'resolved_scope', COALESCE(v_resolved_scope_json, '{}'::jsonb),
    'summary_refresh_candidate_count', v_summary_refresh_candidate_count,
    'active_batch_item_count_after', COALESCE(v_active_batch_item_count_after, 0),
    'active_batch_amount_inc_vat_after', COALESCE(v_active_batch_amount_inc_vat_after, 0),
    'batch_empty_after', COALESCE(v_batch_empty_after, false),
    'notice_group_id', v_notice_group_id,
    'notice_queue_result', v_notice_queue_result,
    'accepted_finance_resolution', v_finance_resolution_result,
    'classification_result', v_classification_result,
    'provider_evidence_result', COALESCE(v_provider_evidence_result, '{}'::jsonb),
    'rail_state_summary', COALESCE(v_rail_state_summary_json, '{}'::jsonb)
  ) || jsonb_build_object(
    'workbench_refresh_status', COALESCE(v_workbench_refresh_status, 'NOT_REQUIRED'),
    'workbench_refresh_queued_count', COALESCE(v_workbench_refresh_queued_count, 0),
    'workbench_refresh_deferred_count', COALESCE(v_workbench_refresh_deferred_count, 0),
    'workbench_refresh_failed_count', COALESCE(v_workbench_refresh_failed_count, 0),
    'workbench_refresh_job_ids', COALESCE(v_workbench_refresh_job_ids, '[]'::jsonb),
    'requires_workbench_session', COALESCE(v_workbench_requires_session, false),
    'workbench_refresh', jsonb_build_object(
      'status', COALESCE(v_workbench_refresh_status, 'NOT_REQUIRED'),
      'queued_count', COALESCE(v_workbench_refresh_queued_count, 0),
      'deferred_count', COALESCE(v_workbench_refresh_deferred_count, 0),
      'failed_count', COALESCE(v_workbench_refresh_failed_count, 0),
      'job_ids', COALESCE(v_workbench_refresh_job_ids, '[]'::jsonb),
      'results', COALESCE(v_workbench_refresh_results, '[]'::jsonb),
      'requires_workbench_session', COALESCE(v_workbench_requires_session, false),
      'session_id', CASE WHEN v_batch.source_workbench_session_id IS NULL THEN NULL ELSE v_batch.source_workbench_session_id::text END,
      'source_snapshot_run_id', CASE WHEN v_batch.source_snapshot_run_id IS NULL THEN NULL ELSE v_batch.source_snapshot_run_id::text END
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
    'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT',
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
    v_blocker := jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_WORK_ITEM_BLOCKED_BY_EXCEPTION',
      'message', 'No-money unwind work item was blocked after an exception; no partial correction was committed.',
      'work_item_id', p_work_item_id,
      'correction_request_id', CASE WHEN v_work_item.correction_request_id IS NULL THEN NULL ELSE v_work_item.correction_request_id END,
      'pay_batch_id', CASE WHEN v_work_item.pay_batch_id IS NULL THEN NULL ELSE v_work_item.pay_batch_id END,
      'sqlstate', SQLSTATE,
      'error_message', SQLERRM,
      'blocked_at_utc', v_now
    );

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_ERROR',
      v_blocker,
      'pay_payment_correction',
      COALESCE(p_work_item_id::text, 'NO_WORK_ITEM_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    IF p_work_item_id IS NOT NULL THEN
      UPDATE public.pay_payment_correction_work_items AS exception_blocked_work_item
      SET
        status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = COALESCE(exception_blocked_work_item.processed_at_utc, v_now),
        last_error = SQLERRM,
        result_json = COALESCE(exception_blocked_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'processed_at_utc', v_now
        )
      WHERE exception_blocked_work_item.id = p_work_item_id;
    END IF;

    RETURN jsonb_build_object(
      'ok', false,
      'status', 'BLOCKED',
      'result_code', 'NO_MONEY_UNWIND_WORK_ITEM_BLOCKED_BY_EXCEPTION',
      'work_item_id', p_work_item_id,
      'candidate_id', v_work_item.candidate_id,
      'expected_item_count', COALESCE(v_selected_item_count, 0),
      'applied_item_count', 0,
      'released_reservations', 0,
      'restored_finance_components', 0,
      'carry_forward_created', 0,
      'carry_forward_existing', 0,
      'carry_forward_released', 0,
      'changed_scope_json', '{}'::jsonb,
      'blocker', v_blocker
    );
END;
$function$;
ALTER FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) TO service_role;
