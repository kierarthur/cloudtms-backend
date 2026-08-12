-- Retire the obsolete, wholly local transfer overlay when a completed
-- pre-bank correction returns an intact remainder to Draft for reauthorisation.
--
-- Policy X: this function never recalculates payment economics. It only
-- detaches active frozen batch items from an obsolete local transfer and
-- voids that transfer after proving that no provider, rail, settlement or
-- transfer-event boundary has been crossed.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_reauthorisation_overlay_reset_v1(
  p_correction_request_id uuid,
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_dry_run boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
SET statement_timeout TO '5000ms'
SET lock_timeout TO '1000ms'
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_request public.pay_payment_correction_requests%ROWTYPE;
  v_operation public.banking_pay_operations%ROWTYPE;
  v_batch public.pay_batches%ROWTYPE;
  v_requested_action text := NULL::text;
  v_active_item_count integer := 0;
  v_linked_active_item_count integer := 0;
  v_transfer_count integer := 0;
  v_unsafe_transfer_count integer := 0;
  v_item_links_cleared integer := 0;
  v_bank_references_cleared integer := 0;
  v_transfers_voided integer := 0;
  v_transfer_ids jsonb := '[]'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_correction_request_id IS NULL OR p_operation_id IS NULL OR p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_OVERLAY_IDENTIFIERS_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_OVERLAY_IDENTIFIERS_REQUIRED'
            )::text;
  END IF;

  SELECT request_row.*
  INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND OR v_request.pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_REQUEST_BATCH_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_REQUEST_BATCH_MISMATCH',
              'correction_request_id', p_correction_request_id,
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type, ''))) <> 'PAYMENT_CORRECTION'
     OR v_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id
     OR COALESCE(v_operation.input_json->>'correction_request_id', '') <> p_correction_request_id::text THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_OPERATION_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_OPERATION_MISMATCH',
              'correction_request_id', p_correction_request_id,
              'operation_id', p_operation_id,
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_requested_action := pg_catalog.upper(pg_catalog.btrim(COALESCE(
    v_request.plan_json->>'requested_action',
    v_request.selection_json->>'requested_action',
    ''
  )));

  IF v_requested_action NOT IN ('PRE_BANK_CANCEL', 'CANCEL_PAYMENT') THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'required', false,
      'code', 'PAYMENT_CORRECTION_REAUTHORISATION_OVERLAY_NOT_REQUIRED_FOR_ACTION',
      'requested_action', v_requested_action,
      'correction_request_id', p_correction_request_id,
      'operation_id', p_operation_id,
      'pay_batch_id', p_pay_batch_id
    );
  END IF;

  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_batch.status, ''))) <> 'DRAFT'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
     OR NULLIF(pg_catalog.btrim(COALESCE(v_batch.execution_commit_ref, '')), '') IS NOT NULL
     OR v_batch.execution_committed_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_BATCH_STATE_UNSAFE'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_BATCH_STATE_UNSAFE',
              'pay_batch_id', p_pay_batch_id,
              'batch_status', v_batch.status,
              'execution_commit_state', v_batch.execution_commit_state,
              'execution_commit_ref_present', NULLIF(pg_catalog.btrim(COALESCE(v_batch.execution_commit_ref, '')), '') IS NOT NULL,
              'execution_committed_at_present', v_batch.execution_committed_at_utc IS NOT NULL
            )::text;
  END IF;

  -- Serialize the exact active frozen remainder before resolving linked local
  -- transfers. This never reads live finance/timesheet truth.
  PERFORM 1
  FROM public.pay_batch_items AS item_lock
  JOIN public.pay_batch_candidates AS candidate_lock
    ON candidate_lock.id = item_lock.pay_batch_candidate_id
  WHERE candidate_lock.pay_batch_id = p_pay_batch_id
    AND COALESCE(item_lock.is_voided, false) = false
  ORDER BY item_lock.id
  FOR UPDATE OF item_lock;

  SELECT pg_catalog.count(*)::integer
  INTO v_active_item_count
  FROM public.pay_batch_items AS active_item
  JOIN public.pay_batch_candidates AS active_candidate
    ON active_candidate.id = active_item.pay_batch_candidate_id
  WHERE active_candidate.pay_batch_id = p_pay_batch_id
    AND COALESCE(active_item.is_voided, false) = false;

  IF COALESCE(v_active_item_count, 0) = 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'required', false,
      'code', 'PAYMENT_CORRECTION_REAUTHORISATION_NO_ACTIVE_REMAINDER',
      'correction_request_id', p_correction_request_id,
      'operation_id', p_operation_id,
      'pay_batch_id', p_pay_batch_id,
      'active_item_count', 0
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_correction_reauthorisation_transfers;
  CREATE TEMPORARY TABLE pg_temp._tmp_correction_reauthorisation_transfers ON COMMIT DROP AS
  SELECT DISTINCT
    transfer_row.id AS pay_bank_transfer_id,
    transfer_row.payment_reference
  FROM public.pay_batch_items AS active_item
  JOIN public.pay_batch_candidates AS active_candidate
    ON active_candidate.id = active_item.pay_batch_candidate_id
  JOIN public.pay_bank_transfers AS transfer_row
    ON transfer_row.id = active_item.pay_bank_transfer_id
  WHERE active_candidate.pay_batch_id = p_pay_batch_id
    AND COALESCE(active_item.is_voided, false) = false
    AND active_item.pay_bank_transfer_id IS NOT NULL;

  SELECT pg_catalog.count(*)::integer,
         COALESCE(pg_catalog.jsonb_agg(
           transfer_scope.pay_bank_transfer_id::text
           ORDER BY transfer_scope.pay_bank_transfer_id
         ), '[]'::jsonb)
  INTO v_transfer_count, v_transfer_ids
  FROM pg_temp._tmp_correction_reauthorisation_transfers AS transfer_scope;

  SELECT pg_catalog.count(*)::integer
  INTO v_linked_active_item_count
  FROM public.pay_batch_items AS linked_item
  JOIN public.pay_batch_candidates AS linked_candidate
    ON linked_candidate.id = linked_item.pay_batch_candidate_id
  JOIN pg_temp._tmp_correction_reauthorisation_transfers AS target_transfer
    ON target_transfer.pay_bank_transfer_id = linked_item.pay_bank_transfer_id
  WHERE linked_candidate.pay_batch_id = p_pay_batch_id
    AND COALESCE(linked_item.is_voided, false) = false;

  IF COALESCE(v_transfer_count, 0) = 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'required', false,
      'code', 'PAYMENT_CORRECTION_REAUTHORISATION_NO_TRANSFER_OVERLAY',
      'correction_request_id', p_correction_request_id,
      'operation_id', p_operation_id,
      'pay_batch_id', p_pay_batch_id,
      'active_item_count', v_active_item_count
    );
  END IF;

  IF v_transfer_count > 2000 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_TRANSFER_SCOPE_TOO_LARGE'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_TRANSFER_SCOPE_TOO_LARGE',
              'transfer_count', v_transfer_count,
              'maximum_transfer_count', 2000
            )::text;
  END IF;

  PERFORM 1
  FROM public.pay_bank_transfers AS transfer_lock
  JOIN pg_temp._tmp_correction_reauthorisation_transfers AS target_transfer
    ON target_transfer.pay_bank_transfer_id = transfer_lock.id
  ORDER BY transfer_lock.id
  FOR UPDATE OF transfer_lock;

  SELECT pg_catalog.count(*)::integer
  INTO v_unsafe_transfer_count
  FROM public.pay_bank_transfers AS transfer_row
  JOIN pg_temp._tmp_correction_reauthorisation_transfers AS target_transfer
    ON target_transfer.pay_bank_transfer_id = transfer_row.id
  WHERE transfer_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(transfer_row.status, ''))) <> 'PENDING'
     OR NULLIF(pg_catalog.btrim(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
     OR transfer_row.completed_at_utc IS NOT NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(transfer_row.failed_reason, '')), '') IS NOT NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', '')), '') IS NOT NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', '')), '') IS NOT NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') IS NOT NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(transfer_row.rail_meta_json #>> '{provider_submission_id}', '')), '') IS NOT NULL
     OR EXISTS (
       SELECT 1
       FROM public.pay_bank_transfer_events AS transfer_event
       WHERE transfer_event.pay_bank_transfer_id = transfer_row.id
     )
     OR EXISTS (
       SELECT 1
       FROM public.banking_pay_operation_transfer_scope AS execution_scope
       WHERE execution_scope.pay_bank_transfer_id = transfer_row.id
         AND (
           COALESCE(execution_scope.provider_submit_attempt_count, 0) > 0
           OR NULLIF(pg_catalog.btrim(COALESCE(execution_scope.provider_transaction_id, '')), '') IS NOT NULL
           OR execution_scope.provider_request_sending_at_utc IS NOT NULL
           OR execution_scope.provider_request_sent_at_utc IS NOT NULL
           OR execution_scope.provider_response_at_utc IS NOT NULL
         )
     )
     OR EXISTS (
       SELECT 1
       FROM public.banking_pay_operation_provider_attempts AS provider_attempt
       JOIN public.banking_pay_operation_transfer_scope AS attempted_scope
         ON attempted_scope.id = provider_attempt.transfer_scope_id
       WHERE attempted_scope.pay_bank_transfer_id = transfer_row.id
     );

  IF COALESCE(v_unsafe_transfer_count, 0) > 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_TRANSFER_EVIDENCE_UNSAFE'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_TRANSFER_EVIDENCE_UNSAFE',
              'unsafe_transfer_count', v_unsafe_transfer_count,
              'transfer_count', v_transfer_count,
              'correction_request_id', p_correction_request_id,
              'operation_id', p_operation_id,
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF COALESCE(p_dry_run, false) THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'required', true,
      'dry_run', true,
      'code', 'PAYMENT_CORRECTION_REAUTHORISATION_OVERLAY_SAFE_TO_RESET',
      'correction_request_id', p_correction_request_id,
      'operation_id', p_operation_id,
      'pay_batch_id', p_pay_batch_id,
      'active_item_count', v_active_item_count,
      'linked_active_item_count', v_linked_active_item_count,
      'transfer_count', v_transfer_count,
      'transfer_ids', v_transfer_ids
    );
  END IF;

  WITH item_rows_to_clear AS (
    SELECT active_item.id AS pay_batch_item_id,
           active_item.bank_reference AS previous_bank_reference,
           target_transfer.payment_reference AS transfer_payment_reference
    FROM public.pay_batch_items AS active_item
    JOIN public.pay_batch_candidates AS active_candidate
      ON active_candidate.id = active_item.pay_batch_candidate_id
    JOIN pg_temp._tmp_correction_reauthorisation_transfers AS target_transfer
      ON target_transfer.pay_bank_transfer_id = active_item.pay_bank_transfer_id
    WHERE active_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(active_item.is_voided, false) = false
  ), cleared_item_links AS (
    UPDATE public.pay_batch_items AS item_update
    SET pay_bank_transfer_id = NULL::uuid,
        bank_reference = CASE
          WHEN item_update.bank_reference = item_rows_to_clear.transfer_payment_reference THEN NULL::text
          ELSE item_update.bank_reference
        END,
        updated_at = v_now
    FROM item_rows_to_clear
    WHERE item_update.id = item_rows_to_clear.pay_batch_item_id
    RETURNING item_update.id,
              item_rows_to_clear.previous_bank_reference,
              item_rows_to_clear.transfer_payment_reference
  )
  SELECT pg_catalog.count(*)::integer,
         (pg_catalog.count(*) FILTER (
           WHERE cleared_item_links.previous_bank_reference = cleared_item_links.transfer_payment_reference
             AND NULLIF(pg_catalog.btrim(COALESCE(cleared_item_links.previous_bank_reference, '')), '') IS NOT NULL
         ))::integer
  INTO v_item_links_cleared, v_bank_references_cleared
  FROM cleared_item_links;

  WITH voided_transfers AS (
    UPDATE public.pay_bank_transfers AS transfer_update
    SET amount = 0,
        status = 'VOIDED',
        failed_reason = COALESCE(
          NULLIF(pg_catalog.btrim(COALESCE(transfer_update.failed_reason, '')), ''),
          'CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED'
        ),
        rail_meta_json = COALESCE(transfer_update.rail_meta_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'cancellation_reauthorisation_overlay_voided', true,
            'correction_request_id', p_correction_request_id,
            'correction_operation_id', p_operation_id,
            'voided_at_utc', v_now,
            'voided_by_user_id', p_actor_user_id
          )
    FROM pg_temp._tmp_correction_reauthorisation_transfers AS target_transfer
    WHERE transfer_update.id = target_transfer.pay_bank_transfer_id
      AND transfer_update.pay_batch_id = p_pay_batch_id
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(transfer_update.status, ''))) = 'PENDING'
      AND NULLIF(pg_catalog.btrim(COALESCE(transfer_update.rail_tx_id, '')), '') IS NULL
      AND transfer_update.completed_at_utc IS NULL
      AND NULLIF(pg_catalog.btrim(COALESCE(transfer_update.failed_reason, '')), '') IS NULL
    RETURNING transfer_update.id
  )
  SELECT pg_catalog.count(*)::integer
  INTO v_transfers_voided
  FROM voided_transfers;

  IF v_transfers_voided IS DISTINCT FROM v_transfer_count
     OR v_item_links_cleared IS DISTINCT FROM v_linked_active_item_count THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REAUTHORISATION_OVERLAY_EFFECT_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REAUTHORISATION_OVERLAY_EFFECT_MISMATCH',
              'expected_transfer_count', v_transfer_count,
              'transfers_voided', v_transfers_voided,
              'expected_linked_active_item_count', v_linked_active_item_count,
              'item_links_cleared', v_item_links_cleared
            )::text;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'required', true,
    'dry_run', false,
    'code', 'PAYMENT_CORRECTION_REAUTHORISATION_OVERLAY_RESET',
    'correction_request_id', p_correction_request_id,
    'operation_id', p_operation_id,
    'pay_batch_id', p_pay_batch_id,
    'active_item_count', v_active_item_count,
    'linked_active_item_count', v_linked_active_item_count,
    'transfer_count', v_transfer_count,
    'item_links_cleared', v_item_links_cleared,
    'bank_references_cleared', v_bank_references_cleared,
    'transfers_voided', v_transfers_voided,
    'transfer_ids', v_transfer_ids,
    'policy_x_economics_changed', false
  );
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_reauthorisation_overlay_reset_v1(uuid, uuid, uuid, uuid, boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauthorisation_overlay_reset_v1(uuid, uuid, uuid, uuid, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauthorisation_overlay_reset_v1(uuid, uuid, uuid, uuid, boolean) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauthorisation_overlay_reset_v1(uuid, uuid, uuid, uuid, boolean) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_reauthorisation_overlay_reset_v1(uuid, uuid, uuid, uuid, boolean) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_reauthorisation_overlay_reset_v1(uuid, uuid, uuid, uuid, boolean) TO service_role;
