-- Permit a subsequent execution after cancellation has safely voided the exact prior local transfer overlay.
-- VOIDED alone is not provider evidence; every provider/rail/sent/response fence below remains fail-closed.
-- Policy X: this does not recalculate or change any frozen payment economics.

CREATE OR REPLACE FUNCTION public.pay_execute_bank_transfer_chunk_prepare(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_transfer_scope_ids jsonb,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_chunk_row public.banking_pay_operation_chunks%ROWTYPE;
  v_chunk_id uuid := NULL::uuid;
  v_limit integer := 25;
  v_requested_count integer := 0;
  v_prepared_count integer := 0;
  v_reused_count integer := 0;
  v_failed_count integer := 0;
  v_remaining_exists boolean := false;
  v_authorisation_ready_count integer := 0;
  v_item_transfer_linked_count integer := 0;
  v_item_transfer_reused_count integer := 0;
  v_item_transfer_conflict_count integer := 0;
  v_execution_mode_raw text := NULL::text;
  v_execution_mode text := NULL::text;
  v_is_local_manual_mode boolean := false;
  v_pay_channel_scope text := 'ALL';
  v_manual_confirmation_mode text := NULL::text;
  v_csv_uploaded_confirmed boolean := false;
  v_csv_bank_confirm_ref text := NULL::text;
  v_bank_csv_generated boolean := false;
  v_bank_csv_current boolean := false;
  v_bank_csv_scope text := NULL::text;
  v_bank_csv_paye_net_state_hash text := NULL::text;
  v_bank_csv_bank_payment_projection_hash text := NULL::text;
  v_bank_csv_row_count integer := NULL::integer;
  v_bank_csv_total_amount numeric(14,2) := NULL::numeric;
  v_external_settlement_comment text := NULL::text;
  v_payment_date text := NULL::text;
  v_mode_evidence_ready boolean := true;
  v_mode_evidence_code text := NULL::text;
  v_mode_evidence_message text := NULL::text;
  v_provider_scope_evidence_count integer := 0;
  v_provider_attempt_row_count integer := 0;
  v_provider_transfer_event_count integer := 0;
  v_provider_transfer_evidence_count integer := 0;
  v_provider_submit_chunk_count integer := 0;
  v_provider_boundary_evidence_count integer := 0;
  v_next_required_phase text := NULL::text;
BEGIN
  PERFORM set_config('lock_timeout', '3s', true);
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_PAY_BATCH_ID_REQUIRED', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_ACTOR_REQUIRED', 'operation_id', p_operation_id::text)::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF upper(btrim(coalesce(v_operation_row.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_TYPE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_TYPE_INVALID', 'operation_id', p_operation_id::text, 'operation_type', v_operation_row.operation_type)::text;
  END IF;

  IF v_operation_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_BATCH_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_OPERATION_BATCH_MISMATCH', 'operation_id', p_operation_id::text, 'operation_pay_batch_id', CASE WHEN v_operation_row.pay_batch_id IS NULL THEN NULL::text ELSE v_operation_row.pay_batch_id::text END, 'pay_batch_id', p_pay_batch_id::text)::text;
  END IF;

  v_execution_mode_raw := upper(btrim(coalesce(v_operation_row.input_json->>'execution_mode', '')));
  v_execution_mode := CASE
    WHEN v_execution_mode_raw IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
    WHEN v_execution_mode_raw IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
    WHEN v_execution_mode_raw IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
    ELSE NULL::text
  END;

  IF v_execution_mode IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_EXECUTION_MODE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_EXECUTION_MODE_INVALID',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'execution_mode_raw', NULLIF(v_execution_mode_raw, ''),
        'message', 'The durable payment execution mode is missing or unsupported.'
      )::text;
  END IF;

  v_is_local_manual_mode := v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT');
  v_pay_channel_scope := upper(btrim(coalesce(NULLIF(v_operation_row.input_json->>'pay_channel_scope', ''), 'ALL')));
  IF v_pay_channel_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA', 'LOANS') THEN
    v_pay_channel_scope := 'ALL';
  END IF;
  v_manual_confirmation_mode := upper(NULLIF(btrim(coalesce(v_operation_row.input_json->>'manual_confirmation_mode', '')), ''));
  v_csv_uploaded_confirmed := lower(btrim(coalesce(v_operation_row.input_json->>'csv_uploaded_confirmed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_csv_bank_confirm_ref := NULLIF(btrim(coalesce(v_operation_row.input_json->>'csv_bank_confirm_ref', '')), '');
  v_bank_csv_generated := lower(btrim(coalesce(v_operation_row.input_json->>'bank_csv_generated', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_bank_csv_current := lower(btrim(coalesce(v_operation_row.input_json->>'bank_csv_current', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_bank_csv_scope := upper(NULLIF(btrim(coalesce(v_operation_row.input_json->>'bank_csv_scope', '')), ''));
  v_bank_csv_paye_net_state_hash := NULLIF(btrim(coalesce(v_operation_row.input_json->>'bank_csv_paye_net_state_hash', '')), '');
  v_bank_csv_bank_payment_projection_hash := NULLIF(btrim(coalesce(v_operation_row.input_json->>'bank_csv_bank_payment_projection_hash', '')), '');
  IF coalesce(v_operation_row.input_json->>'bank_csv_row_count', '') ~ '^[0-9]+$' THEN
    v_bank_csv_row_count := (v_operation_row.input_json->>'bank_csv_row_count')::integer;
  END IF;
  IF coalesce(v_operation_row.input_json->>'bank_csv_total_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
    v_bank_csv_total_amount := round((v_operation_row.input_json->>'bank_csv_total_amount')::numeric, 2)::numeric(14,2);
  END IF;
  v_external_settlement_comment := NULLIF(btrim(coalesce(v_operation_row.input_json->>'external_settlement_comment', '')), '');
  v_payment_date := NULLIF(btrim(coalesce(v_operation_row.input_json->>'payment_date', '')), '');

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id::text)::text;
  END IF;

  IF v_is_local_manual_mode THEN
    SELECT count(*)::integer
    INTO v_provider_scope_evidence_count
    FROM public.banking_pay_operation_transfer_scope AS provider_scope
    WHERE provider_scope.operation_id = p_operation_id
      AND provider_scope.pay_batch_id = p_pay_batch_id
      AND (
        coalesce(provider_scope.provider_submit_ready, false) = true
        OR upper(btrim(coalesce(provider_scope.provider_submit_state, 'NOT_READY'))) NOT IN ('', 'NOT_READY')
        OR provider_scope.provider_submit_chunk_id IS NOT NULL
        OR provider_scope.provider_submit_claimed_at_utc IS NOT NULL
        OR coalesce(provider_scope.provider_submit_attempt_count, 0) > 0
        OR NULLIF(btrim(coalesce(provider_scope.provider_idempotency_key, '')), '') IS NOT NULL
        OR NULLIF(btrim(coalesce(provider_scope.provider_request_id, '')), '') IS NOT NULL
        OR NULLIF(btrim(coalesce(provider_scope.provider_transaction_id, '')), '') IS NOT NULL
        OR provider_scope.provider_request_prepared_at_utc IS NOT NULL
        OR provider_scope.provider_request_sending_at_utc IS NOT NULL
        OR provider_scope.provider_request_sent_at_utc IS NOT NULL
        OR provider_scope.provider_response_at_utc IS NOT NULL
        OR NULLIF(btrim(coalesce(provider_scope.provider_submission_status, '')), '') IS NOT NULL
      );

    SELECT count(*)::integer
    INTO v_provider_attempt_row_count
    FROM public.banking_pay_operation_provider_attempts AS provider_attempt
    WHERE provider_attempt.operation_id = p_operation_id
       OR provider_attempt.pay_batch_id = p_pay_batch_id;

    SELECT count(*)::integer
    INTO v_provider_transfer_event_count
    FROM public.pay_bank_transfer_events AS transfer_event
    WHERE transfer_event.pay_batch_id = p_pay_batch_id
      AND (
        upper(btrim(coalesce(transfer_event.event_source, ''))) IN ('PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'PROVIDER_RESPONSE')
        OR NULLIF(btrim(coalesce(transfer_event.provider_request_id, '')), '') IS NOT NULL
        OR NULLIF(btrim(coalesce(transfer_event.provider_transaction_id, '')), '') IS NOT NULL
        OR NULLIF(btrim(coalesce(transfer_event.provider_event_id, '')), '') IS NOT NULL
        OR transfer_event.provider_webhook_receipt_id IS NOT NULL
      );

    SELECT count(*)::integer
    INTO v_provider_transfer_evidence_count
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
      AND (
        upper(btrim(coalesce(transfer_row.status, ''))) NOT IN ('PENDING', 'BLOCKED', 'FAILED')
        OR NULLIF(btrim(coalesce(transfer_row.request_id, '')), '') IS NOT NULL
        OR NULLIF(btrim(coalesce(transfer_row.rail_tx_id, '')), '') IS NOT NULL
        OR transfer_row.completed_at_utc IS NOT NULL
        OR upper(btrim(coalesce(transfer_row.rail_state, ''))) NOT IN ('', 'LOCAL', 'PENDING')
        OR upper(btrim(coalesce(transfer_row.failed_reason, ''))) IN ('PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'PROVIDER_OUTCOME_UNKNOWN', 'REQUEST_SENT_LOCAL')
        OR upper(btrim(coalesce(transfer_row.rail_meta_json->>'provider_stage', ''))) IN ('REQUEST_PREPARING', 'REQUEST_SENDING', 'REQUEST_SENT_LOCAL', 'PROVIDER_ACCEPTED', 'PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'REVIEW_REQUIRED', 'CHUNK_FINALISED')
        OR NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), '') IS NOT NULL
        OR lower(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_request_sent}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR lower(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_called}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR lower(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR lower(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_acceptance_evidence_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      );

    SELECT count(*)::integer
    INTO v_provider_submit_chunk_count
    FROM public.banking_pay_operation_chunks AS provider_chunk
    WHERE provider_chunk.operation_id = p_operation_id
      AND (
        upper(btrim(coalesce(provider_chunk.chunk_type, ''))) = 'TRANSFER_SUBMIT'
        OR upper(btrim(coalesce(provider_chunk.phase, ''))) IN ('SUBMIT_PROVIDER_TRANSFERS', 'SEND_PROVIDER_CHUNK', 'REQUEST_PROVIDER_SEND', 'FINALISE_PROVIDER_CHUNK', 'APPLY_RAIL_UPDATES')
      );

    v_provider_boundary_evidence_count := coalesce(v_provider_scope_evidence_count, 0)
      + coalesce(v_provider_attempt_row_count, 0)
      + coalesce(v_provider_transfer_event_count, 0)
      + coalesce(v_provider_transfer_evidence_count, 0)
      + coalesce(v_provider_submit_chunk_count, 0);

    IF v_provider_boundary_evidence_count > 0 THEN
      UPDATE public.banking_pay_operations AS operation_review
      SET status = 'REVIEW_REQUIRED',
          phase = 'REVIEW_REQUIRED',
          runner_state = 'WAITING_USER_REVIEW',
          requires_user_action = true,
          resume_reason = 'LOCAL_MANUAL_SETTLEMENT_PROVIDER_BOUNDARY_EVIDENCE',
          progress_json = jsonb_strip_nulls(coalesce(operation_review.progress_json, '{}'::jsonb) || jsonb_build_object(
            'execution_mode', v_execution_mode,
            'provider_submission_required', false,
            'provider_submission_attempted', false,
            'submitted_to_bank', false,
            'review_required', true,
            'review_reason_code', 'LOCAL_MANUAL_SETTLEMENT_PROVIDER_BOUNDARY_EVIDENCE',
            'provider_scope_evidence_count', coalesce(v_provider_scope_evidence_count, 0),
            'provider_attempt_row_count', coalesce(v_provider_attempt_row_count, 0),
            'provider_transfer_event_count', coalesce(v_provider_transfer_event_count, 0),
            'provider_transfer_evidence_count', coalesce(v_provider_transfer_evidence_count, 0),
            'provider_submit_chunk_count', coalesce(v_provider_submit_chunk_count, 0),
            'provider_boundary_evidence_count', v_provider_boundary_evidence_count,
            'next_required_phase', 'REVIEW_REQUIRED',
            'review_required_at_utc', v_now::text
          )),
          error_json = jsonb_strip_nulls(coalesce(operation_review.error_json, '{}'::jsonb) || jsonb_build_object(
            'code', 'LOCAL_MANUAL_SETTLEMENT_PROVIDER_BOUNDARY_EVIDENCE',
            'message', 'Local/manual settlement cannot continue because provider or bank-dispatch evidence already exists.',
            'operation_id', p_operation_id::text,
            'pay_batch_id', p_pay_batch_id::text,
            'execution_mode', v_execution_mode,
            'provider_scope_evidence_count', coalesce(v_provider_scope_evidence_count, 0),
            'provider_attempt_row_count', coalesce(v_provider_attempt_row_count, 0),
            'provider_transfer_event_count', coalesce(v_provider_transfer_event_count, 0),
            'provider_transfer_evidence_count', coalesce(v_provider_transfer_evidence_count, 0),
            'provider_submit_chunk_count', coalesce(v_provider_submit_chunk_count, 0),
            'detected_at_utc', v_now::text
          )),
          updated_at_utc = v_now
      WHERE operation_review.id = p_operation_id;

      RETURN jsonb_build_object(
        'ok', false,
        'hard_blocker', true,
        'code', 'LOCAL_MANUAL_SETTLEMENT_PROVIDER_BOUNDARY_EVIDENCE',
        'message', 'Local/manual settlement cannot continue because provider or bank-dispatch evidence already exists.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'execution_mode', v_execution_mode,
        'provider_submission_required', false,
        'provider_submission_attempted', false,
        'submitted_to_bank', false,
        'provider_scope_evidence_count', coalesce(v_provider_scope_evidence_count, 0),
        'provider_attempt_row_count', coalesce(v_provider_attempt_row_count, 0),
        'provider_transfer_event_count', coalesce(v_provider_transfer_event_count, 0),
        'provider_transfer_evidence_count', coalesce(v_provider_transfer_evidence_count, 0),
        'provider_submit_chunk_count', coalesce(v_provider_submit_chunk_count, 0),
        'provider_boundary_evidence_count', v_provider_boundary_evidence_count,
        'next_required_phase', 'REVIEW_REQUIRED',
        'server_utc', v_now::text
      );
    END IF;

    IF v_execution_mode = 'CSV_SETTLEMENT' THEN
      v_mode_evidence_ready := v_bank_csv_generated
        AND v_bank_csv_current
        AND v_csv_uploaded_confirmed
        AND v_csv_bank_confirm_ref IS NOT NULL
        AND v_manual_confirmation_mode = 'BANK_UPLOAD_CONFIRMED'
        AND v_bank_csv_scope IS NOT DISTINCT FROM v_pay_channel_scope
        AND v_bank_csv_paye_net_state_hash IS NOT NULL
        AND v_bank_csv_bank_payment_projection_hash IS NOT NULL
        AND coalesce(v_bank_csv_row_count, 0) > 0
        AND coalesce(v_bank_csv_total_amount, 0) > 0
        AND v_payment_date IS NOT NULL;
      v_mode_evidence_code := 'CSV_SETTLEMENT_PROOF_REQUIRED';
      v_mode_evidence_message := 'CSV settlement requires the current CloudTMS CSV proof, upload confirmation, bank reference, and payment date before transfer evidence can be prepared.';
    ELSIF v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN
      v_mode_evidence_ready := v_external_settlement_comment IS NOT NULL
        AND v_manual_confirmation_mode = 'EXTERNAL_SETTLEMENT_CONFIRMED'
        AND v_payment_date IS NOT NULL;
      v_mode_evidence_code := 'EXTERNAL_SETTLEMENT_CONFIRMATION_REQUIRED';
      v_mode_evidence_message := 'External settlement requires the frozen external/manual confirmation comment, confirmation mode, and payment date before transfer evidence can be prepared.';
    END IF;

  END IF;

  IF p_transfer_scope_ids IS NOT NULL AND jsonb_typeof(p_transfer_scope_ids) <> 'object' THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_SCOPE_PAYLOAD_DISABLED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_SCOPE_PAYLOAD_DISABLED',
        'operation_id', p_operation_id::text,
        'message', 'Transfer chunk prepare is row-backed and chunk-owned. Pass a JSON object with chunk_id, or omit p_transfer_scope_ids so the database claims the next bounded scope page. Array/scalar transfer-scope payloads are disabled.'
      )::text;
  END IF;

  IF p_transfer_scope_ids IS NOT NULL AND jsonb_typeof(p_transfer_scope_ids) = 'object' THEN
    IF COALESCE(nullif(btrim(coalesce(p_transfer_scope_ids->>'chunk_id', p_transfer_scope_ids->>'chunkId', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_CHUNK_ID_REQUIRED'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_CHUNK_ID_REQUIRED',
          'operation_id', p_operation_id::text,
          'message', 'When p_transfer_scope_ids is supplied, it must be a JSON object containing a valid chunk_id. Omit p_transfer_scope_ids to let the database claim the next bounded scope page.'
        )::text;
    END IF;

    v_chunk_id := coalesce(p_transfer_scope_ids->>'chunk_id', p_transfer_scope_ids->>'chunkId')::uuid;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_prepare_scope_request;
  CREATE TEMPORARY TABLE pg_temp.tmp_prepare_scope_request (
    transfer_scope_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  IF v_chunk_id IS NOT NULL THEN
    SELECT chunk_row.*
    INTO v_chunk_row
    FROM public.banking_pay_operation_chunks AS chunk_row
    WHERE chunk_row.id = v_chunk_id
      AND chunk_row.operation_id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_CHUNK_NOT_FOUND'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_CHUNK_NOT_FOUND', 'operation_id', p_operation_id::text, 'chunk_id', v_chunk_id::text)::text;
    END IF;

    INSERT INTO pg_temp.tmp_prepare_scope_request (transfer_scope_id)
    SELECT DISTINCT scope_text.transfer_scope_id_text::uuid
    FROM (
      SELECT chunk_scope.value #>> '{}' AS transfer_scope_id_text
      FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_chunk_row.payload_json->'transfer_scope_ids') = 'array' THEN v_chunk_row.payload_json->'transfer_scope_ids' ELSE '[]'::jsonb END) AS chunk_scope(value)
      UNION ALL
      SELECT coalesce(unit_value.value->>'transfer_scope_id', unit_value.value #>> '{unit,transfer_scope_id}') AS transfer_scope_id_text
      FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_chunk_row.payload_json->'units') = 'array' THEN v_chunk_row.payload_json->'units' ELSE '[]'::jsonb END) AS unit_value(value)
      UNION ALL
      SELECT v_chunk_row.payload_json->>'transfer_scope_id' AS transfer_scope_id_text
    ) AS scope_text
    WHERE nullif(btrim(coalesce(scope_text.transfer_scope_id_text, '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    LIMIT v_limit;
  ELSE
    INSERT INTO pg_temp.tmp_prepare_scope_request (transfer_scope_id)
    SELECT scope_row.id
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(scope_row.status, ''))) IN ('ROLLED_UP', 'PENDING')
      AND coalesce(scope_row.provider_review_required, false) = false
      AND coalesce(scope_row.prepared_item_count, 0) > 0
      AND nullif(btrim(coalesce(scope_row.prepared_result_hash, '')), '') IS NOT NULL
      AND (
        (
          v_execution_mode = 'STANDARD_BANK'
          AND upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) IN ('', 'NOT_READY')
        )
        OR (
          v_is_local_manual_mode
          AND coalesce(scope_row.provider_submit_ready, false) = false
          AND upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) IN ('', 'NOT_READY')
        )
      )
    ORDER BY scope_row.updated_at_utc NULLS FIRST, scope_row.id
    LIMIT v_limit
    FOR UPDATE SKIP LOCKED;
  END IF;

  SELECT count(*)::integer
  INTO v_requested_count
  FROM pg_temp.tmp_prepare_scope_request AS requested_scope;

  IF coalesce(v_requested_count, 0) <= 0 THEN
    v_next_required_phase := CASE
      WHEN v_execution_mode = 'STANDARD_BANK' THEN 'PROVIDER_SUBMIT_CLAIM'
      ELSE 'PREPARE_BATCH_PROOF'
    END;

    RETURN jsonb_build_object(
      'ok', true,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'chunk_id', CASE WHEN v_chunk_id IS NULL THEN NULL ELSE v_chunk_id::text END,
      'requested_scope_count', 0,
      'prepared_count', 0,
      'reused_count', 0,
      'failed_count', 0,
      'item_transfer_linked_count', 0,
      'item_transfer_reused_count', 0,
      'item_transfer_conflict_count', 0,
      'remaining_count', 0,
      'has_more', false,
      'execution_mode', v_execution_mode,
      'provider_submission_required', v_execution_mode = 'STANDARD_BANK',
      'provider_submission_attempted', false,
      'submitted_to_bank', false,
      'local_settlement_evidence_only', v_is_local_manual_mode,
      'next_required_phase', v_next_required_phase
    ) || jsonb_strip_nulls(jsonb_build_object(
      'manual_confirmation_mode', CASE WHEN v_is_local_manual_mode THEN v_manual_confirmation_mode ELSE NULL::text END,
      'bank_csv_generated', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_generated ELSE NULL::boolean END,
      'bank_csv_current', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_current ELSE NULL::boolean END,
      'bank_csv_scope', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_scope ELSE NULL::text END,
      'bank_csv_paye_net_state_hash', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_paye_net_state_hash ELSE NULL::text END,
      'bank_csv_bank_payment_projection_hash', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_bank_payment_projection_hash ELSE NULL::text END,
      'bank_csv_row_count', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_row_count ELSE NULL::integer END,
      'bank_csv_total_amount', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_total_amount ELSE NULL::numeric END,
      'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
      'csv_bank_confirm_ref', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref ELSE NULL::text END,
      'external_settlement_comment', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment ELSE NULL::text END
    ));
  END IF;


  IF v_is_local_manual_mode AND v_mode_evidence_ready IS NOT TRUE THEN
    UPDATE public.banking_pay_operations AS operation_review
    SET status = 'REVIEW_REQUIRED',
        phase = 'REVIEW_REQUIRED',
        runner_state = 'WAITING_USER_REVIEW',
        requires_user_action = true,
        resume_reason = v_mode_evidence_code,
        progress_json = jsonb_strip_nulls(coalesce(operation_review.progress_json, '{}'::jsonb) || jsonb_build_object(
          'execution_mode', v_execution_mode,
          'provider_submission_required', false,
          'provider_submission_attempted', false,
          'submitted_to_bank', false,
          'review_required', true,
          'review_reason_code', v_mode_evidence_code,
          'manual_confirmation_mode', v_manual_confirmation_mode,
          'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
          'csv_bank_confirm_ref', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref ELSE NULL::text END,
          'external_settlement_comment', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment ELSE NULL::text END,
          'next_required_phase', 'REVIEW_REQUIRED',
          'review_required_at_utc', v_now::text
        )),
        error_json = jsonb_strip_nulls(coalesce(operation_review.error_json, '{}'::jsonb) || jsonb_build_object(
          'code', v_mode_evidence_code,
          'message', v_mode_evidence_message,
          'operation_id', p_operation_id::text,
          'pay_batch_id', p_pay_batch_id::text,
          'execution_mode', v_execution_mode,
          'manual_confirmation_mode', v_manual_confirmation_mode,
          'bank_csv_generated', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_generated ELSE NULL::boolean END,
          'bank_csv_current', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_current ELSE NULL::boolean END,
          'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
          'csv_bank_confirm_ref_present', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref IS NOT NULL ELSE NULL::boolean END,
          'external_settlement_comment_present', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment IS NOT NULL ELSE NULL::boolean END,
          'payment_date_present', v_payment_date IS NOT NULL,
          'detected_at_utc', v_now::text
        )),
        updated_at_utc = v_now
    WHERE operation_review.id = p_operation_id;

    RETURN jsonb_build_object(
      'ok', false,
      'hard_blocker', true,
      'code', v_mode_evidence_code,
      'message', v_mode_evidence_message,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'execution_mode', v_execution_mode,
      'provider_submission_required', false,
      'provider_submission_attempted', false,
      'submitted_to_bank', false,
      'manual_confirmation_mode', v_manual_confirmation_mode,
      'bank_csv_generated', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_generated ELSE NULL::boolean END,
      'bank_csv_current', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_current ELSE NULL::boolean END,
      'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
      'csv_bank_confirm_ref_present', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref IS NOT NULL ELSE NULL::boolean END,
      'external_settlement_comment_present', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment IS NOT NULL ELSE NULL::boolean END,
      'payment_date_present', v_payment_date IS NOT NULL,
      'next_required_phase', 'REVIEW_REQUIRED',
      'server_utc', v_now::text
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_prepare_scope_eval;
  CREATE TEMPORARY TABLE pg_temp.tmp_prepare_scope_eval ON COMMIT DROP AS
  SELECT scope_row.id AS transfer_scope_id,
         scope_row.pay_batch_id,
         scope_row.pay_channel,
         scope_row.transfer_group_key,
         scope_row.candidate_id,
         scope_row.umbrella_id,
         scope_row.amount,
         coalesce(nullif(btrim(scope_row.currency), ''), 'GBP') AS currency,
         scope_row.payment_reference,
         scope_row.payee_name,
         scope_row.sort_code,
         scope_row.account_number,
         scope_row.account_type,
         scope_row.bank_details_hash_snapshot,
         scope_row.payee_entity_kind,
         scope_row.payee_entity_id,
         scope_row.grouping_mode_used,
         scope_row.week_ending_bucket,
         scope_row.request_id,
         scope_row.pay_bank_transfer_id,
         scope_row.prepared_item_count,
         scope_row.prepared_amount_total,
         scope_row.prepared_scope_hash,
         scope_row.prepared_result_hash,
         scope_row.provider_review_required,
         scope_row.provider_unsafe_reason,
         upper(btrim(coalesce(scope_row.status, ''))) AS current_scope_status,
         upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) AS current_provider_submit_state,
         CASE
           WHEN v_is_local_manual_mode AND (
             coalesce(scope_row.provider_submit_ready, false) = true
             OR upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) NOT IN ('', 'NOT_READY')
             OR scope_row.provider_submit_chunk_id IS NOT NULL
             OR scope_row.provider_submit_claimed_at_utc IS NOT NULL
             OR coalesce(scope_row.provider_submit_attempt_count, 0) > 0
             OR NULLIF(btrim(coalesce(scope_row.provider_idempotency_key, '')), '') IS NOT NULL
             OR NULLIF(btrim(coalesce(scope_row.provider_request_id, '')), '') IS NOT NULL
             OR NULLIF(btrim(coalesce(scope_row.provider_transaction_id, '')), '') IS NOT NULL
             OR scope_row.provider_request_prepared_at_utc IS NOT NULL
             OR scope_row.provider_request_sending_at_utc IS NOT NULL
             OR scope_row.provider_request_sent_at_utc IS NOT NULL
             OR scope_row.provider_response_at_utc IS NOT NULL
             OR NULLIF(btrim(coalesce(scope_row.provider_submission_status, '')), '') IS NOT NULL
           ) THEN 'LOCAL_MANUAL_SETTLEMENT_PROVIDER_BOUNDARY_EVIDENCE'
           WHEN upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) IN ('CLAIMED', 'REQUEST_PREPARING', 'REQUEST_SENDING', 'REQUEST_SENT_LOCAL', 'PROVIDER_ACCEPTED', 'PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'REVIEW_REQUIRED', 'CHUNK_FINALISED') THEN 'TRANSFER_SCOPE_PROVIDER_SUBMISSION_ALREADY_STARTED'
           WHEN upper(btrim(coalesce(scope_row.status, ''))) IN ('SUBMITTED', 'COMPLETED', 'COMPLETE') THEN 'TRANSFER_SCOPE_ALREADY_SUBMITTED'
           WHEN existing_transfer.id IS NOT NULL
            AND (
              upper(btrim(coalesce(existing_transfer.status, ''))) NOT IN ('PENDING', 'BLOCKED', 'FAILED', 'VOIDED')
              OR (v_is_local_manual_mode AND nullif(btrim(coalesce(existing_transfer.request_id, '')), '') IS NOT NULL)
              OR nullif(btrim(coalesce(existing_transfer.rail_tx_id, '')), '') IS NOT NULL
              OR existing_transfer.completed_at_utc IS NOT NULL
              OR (
                CASE
                  WHEN v_is_local_manual_mode THEN upper(btrim(coalesce(existing_transfer.rail_state, ''))) NOT IN ('', 'LOCAL', 'PENDING')
                  ELSE upper(btrim(coalesce(existing_transfer.rail_state, ''))) NOT IN ('', 'PENDING')
                END
              )
              OR upper(btrim(coalesce(existing_transfer.failed_reason, ''))) IN ('PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'PROVIDER_OUTCOME_UNKNOWN', 'REQUEST_SENT_LOCAL')
              OR upper(btrim(coalesce(existing_transfer.rail_meta_json->>'provider_stage', ''))) IN ('REQUEST_SENDING', 'REQUEST_SENT_LOCAL', 'PROVIDER_ACCEPTED', 'PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'REVIEW_REQUIRED', 'CHUNK_FINALISED')
              OR lower(btrim(coalesce(existing_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_request_sent}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR lower(btrim(coalesce(existing_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_called}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR lower(btrim(coalesce(existing_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR lower(btrim(coalesce(existing_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_acceptance_evidence_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            ) THEN 'EXISTING_TRANSFER_PROVIDER_EVIDENCE_PRESENT'
           WHEN coalesce(scope_row.provider_review_required, false) THEN 'PROVIDER_REVIEW_REQUIRED'
           WHEN coalesce(scope_row.prepared_item_count, 0) <= 0 THEN 'TRANSFER_SCOPE_NO_PREPARED_ITEMS'
           WHEN v_is_local_manual_mode AND nullif(btrim(coalesce(scope_row.prepared_scope_hash, '')), '') IS NULL THEN 'TRANSFER_SCOPE_PREPARED_SCOPE_PROOF_MISSING'
           WHEN nullif(btrim(coalesce(scope_row.prepared_result_hash, '')), '') IS NULL THEN 'TRANSFER_SCOPE_PREPARED_PROOF_MISSING'
           WHEN round(coalesce(scope_row.prepared_amount_total, 0), 2) = 0 THEN 'TRANSFER_SCOPE_ZERO_AMOUNT'
           ELSE NULL::text
         END AS blocker_code
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  JOIN pg_temp.tmp_prepare_scope_request AS requested_scope
    ON requested_scope.transfer_scope_id = scope_row.id
  LEFT JOIN public.pay_bank_transfers AS existing_transfer
    ON existing_transfer.pay_batch_id = scope_row.pay_batch_id
   AND existing_transfer.pay_channel = scope_row.pay_channel
   AND existing_transfer.transfer_group_key = scope_row.transfer_group_key
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = p_pay_batch_id;

  IF (SELECT count(*) FROM pg_temp.tmp_prepare_scope_eval AS eval_row) <> v_requested_count THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_SCOPE_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_SCOPE_MISMATCH', 'operation_id', p_operation_id::text, 'pay_batch_id', p_pay_batch_id::text)::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.tmp_prepare_scope_eval AS eval_row
    WHERE eval_row.blocker_code IN ('TRANSFER_SCOPE_PROVIDER_SUBMISSION_ALREADY_STARTED', 'TRANSFER_SCOPE_ALREADY_SUBMITTED')
  ) THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_PROVIDER_STATE_ALREADY_STARTED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'PAY_EXECUTE_TRANSFER_CHUNK_PREPARE_PROVIDER_STATE_ALREADY_STARTED',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'chunk_id', CASE WHEN v_chunk_id IS NULL THEN NULL ELSE v_chunk_id::text END,
        'message', 'Transfer chunk prepare cannot run against a scope after provider submission has started or completed. Reconcile provider state instead of preparing/resending.'
      )::text;
  END IF;

  UPDATE public.banking_pay_operation_transfer_scope AS scope_update
  SET status = 'FAILED',
      provider_submit_ready = false,
      provider_submit_state = 'REVIEW_REQUIRED',
      provider_review_required = true,
      provider_unsafe_reason = coalesce(eval_row.blocker_code, 'TRANSFER_SCOPE_NOT_PREPARABLE'),
      updated_at_utc = v_now
  FROM pg_temp.tmp_prepare_scope_eval AS eval_row
  WHERE scope_update.id = eval_row.transfer_scope_id
    AND eval_row.blocker_code IS NOT NULL;

  GET DIAGNOSTICS v_failed_count = ROW_COUNT;

  WITH preparable_scope AS (
    SELECT eval_row.*
    FROM pg_temp.tmp_prepare_scope_eval AS eval_row
    WHERE eval_row.blocker_code IS NULL
  ), upserted_transfers AS (
    INSERT INTO public.pay_bank_transfers (
      pay_batch_id,
      candidate_id,
      umbrella_id,
      pay_channel,
      amount,
      currency,
      status,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      created_at_utc,
      completed_at_utc,
      failed_reason,
      rail_provider,
      rail_env,
      request_id,
      rail_tx_id,
      rail_state,
      rail_meta_json,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used,
      week_ending_bucket
    )
    SELECT preparable_scope.pay_batch_id,
           preparable_scope.candidate_id,
           preparable_scope.umbrella_id,
           preparable_scope.pay_channel,
           round(coalesce(preparable_scope.prepared_amount_total, preparable_scope.amount, 0), 2),
           preparable_scope.currency,
           'PENDING',
           preparable_scope.payment_reference,
           preparable_scope.payee_name,
           CASE
             WHEN regexp_replace(coalesce(preparable_scope.sort_code, ''), '[^0-9]', '', 'g') ~ '^[0-9]{6}$' THEN regexp_replace(
               regexp_replace(coalesce(preparable_scope.sort_code, ''), '[^0-9]', '', 'g'),
               '^([0-9]{2})([0-9]{2})([0-9]{2})$',
               '\1-\2-\3'
             )
             ELSE NULL::text
           END,
           regexp_replace(coalesce(preparable_scope.account_number, ''), '[^0-9]', '', 'g'),
           preparable_scope.account_type,
           v_now,
           NULL::timestamptz,
           NULL::text,
           v_batch_row.rail_provider_snapshot,
           v_batch_row.rail_env_snapshot,
           CASE
             WHEN v_execution_mode = 'STANDARD_BANK' THEN coalesce(nullif(btrim(preparable_scope.request_id), ''), 'op:' || p_operation_id::text || ':scope:' || preparable_scope.transfer_scope_id::text)
             ELSE NULL::text
           END,
           NULL::text,
           NULL::text,
           jsonb_build_object(
             'operation_id', p_operation_id::text,
             'transfer_scope_id', preparable_scope.transfer_scope_id::text,
             'prepared_at_utc', v_now::text,
             'prepared_result_hash', preparable_scope.prepared_result_hash
           ) || CASE
             WHEN v_is_local_manual_mode THEN jsonb_build_object(
               'execution_mode', v_execution_mode,
               'local_settlement_evidence_only', true,
               'provider_submission_required', false,
               'provider_submission_attempted', false,
               'submitted_to_bank', false
             )
             ELSE '{}'::jsonb
           END,
           preparable_scope.bank_details_hash_snapshot,
           preparable_scope.payee_entity_kind,
           preparable_scope.payee_entity_id,
           preparable_scope.transfer_group_key,
           preparable_scope.grouping_mode_used,
           preparable_scope.week_ending_bucket
    FROM preparable_scope
    ON CONFLICT (pay_batch_id, pay_channel, transfer_group_key)
    DO UPDATE
    SET candidate_id = EXCLUDED.candidate_id,
        umbrella_id = EXCLUDED.umbrella_id,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        status = CASE WHEN public.pay_bank_transfers.status IN ('PENDING', 'BLOCKED', 'FAILED') THEN 'PENDING' ELSE public.pay_bank_transfers.status END,
        payment_reference = EXCLUDED.payment_reference,
        payee_name = EXCLUDED.payee_name,
        sort_code = EXCLUDED.sort_code,
        account_number = EXCLUDED.account_number,
        account_type = EXCLUDED.account_type,
        rail_provider = EXCLUDED.rail_provider,
        rail_env = EXCLUDED.rail_env,
        request_id = CASE
          WHEN v_execution_mode = 'STANDARD_BANK' THEN coalesce(nullif(public.pay_bank_transfers.request_id, ''), EXCLUDED.request_id)
          ELSE public.pay_bank_transfers.request_id
        END,
        rail_meta_json = jsonb_strip_nulls(coalesce(public.pay_bank_transfers.rail_meta_json, '{}'::jsonb) || coalesce(EXCLUDED.rail_meta_json, '{}'::jsonb)),
        bank_details_hash_snapshot = EXCLUDED.bank_details_hash_snapshot,
        payee_entity_kind = EXCLUDED.payee_entity_kind,
        payee_entity_id = EXCLUDED.payee_entity_id,
        grouping_mode_used = EXCLUDED.grouping_mode_used,
        week_ending_bucket = EXCLUDED.week_ending_bucket
    WHERE public.pay_bank_transfers.status IN ('PENDING', 'BLOCKED', 'FAILED')
    RETURNING public.pay_bank_transfers.id,
              public.pay_bank_transfers.pay_batch_id,
              public.pay_bank_transfers.pay_channel,
              public.pay_bank_transfers.transfer_group_key,
              (xmax = 0) AS was_inserted
  ), linked_scope AS (
    UPDATE public.banking_pay_operation_transfer_scope AS scope_update
    SET status = 'PREPARED',
        pay_bank_transfer_id = upserted_transfers.id,
        amount = round(coalesce(eval_row.prepared_amount_total, scope_update.amount, 0), 2),
        provider_submit_ready = CASE WHEN v_execution_mode = 'STANDARD_BANK' THEN true ELSE false END,
        provider_submit_state = CASE WHEN v_execution_mode = 'STANDARD_BANK' THEN 'READY' ELSE 'NOT_READY' END,
        provider_review_required = false,
        provider_unsafe_reason = NULL::text,
        provider_idempotency_key = CASE
          WHEN v_execution_mode = 'STANDARD_BANK' THEN coalesce(nullif(btrim(scope_update.provider_idempotency_key), ''), coalesce(nullif(btrim(scope_update.request_id), ''), 'scope:' || scope_update.id::text))
          ELSE NULL::text
        END,
        provider_request_id = CASE
          WHEN v_execution_mode = 'STANDARD_BANK' THEN coalesce(nullif(btrim(scope_update.provider_request_id), ''), upserted_transfers.id::text)
          ELSE NULL::text
        END,
        updated_at_utc = v_now
    FROM upserted_transfers
    JOIN pg_temp.tmp_prepare_scope_eval AS eval_row
      ON eval_row.pay_batch_id = upserted_transfers.pay_batch_id
     AND eval_row.pay_channel = upserted_transfers.pay_channel
     AND eval_row.transfer_group_key = upserted_transfers.transfer_group_key
    WHERE scope_update.id = eval_row.transfer_scope_id
      AND eval_row.blocker_code IS NULL
    RETURNING scope_update.id,
              upserted_transfers.was_inserted
  )
  SELECT count(*) FILTER (WHERE linked_scope.was_inserted)::integer,
         count(*) FILTER (WHERE linked_scope.was_inserted IS NOT TRUE)::integer
  INTO v_prepared_count,
       v_reused_count
  FROM linked_scope;

  DROP TABLE IF EXISTS pg_temp.tmp_prepare_item_transfer_link;
  CREATE TEMPORARY TABLE pg_temp.tmp_prepare_item_transfer_link ON COMMIT DROP AS
  SELECT DISTINCT
         scope_row.id AS transfer_scope_id,
         scope_row.pay_bank_transfer_id AS pay_bank_transfer_id,
         scope_item_row.pay_batch_item_id AS pay_batch_item_id,
         batch_item.pay_bank_transfer_id AS existing_pay_bank_transfer_id
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  JOIN pg_temp.tmp_prepare_scope_eval AS eval_row
    ON eval_row.transfer_scope_id = scope_row.id
   AND eval_row.blocker_code IS NULL
  JOIN public.banking_pay_operation_transfer_scope_items AS scope_item_row
    ON scope_item_row.operation_id = p_operation_id
   AND scope_item_row.pay_batch_id = p_pay_batch_id
   AND scope_item_row.transfer_scope_id = scope_row.id
  JOIN public.pay_batch_items AS batch_item
    ON batch_item.id = scope_item_row.pay_batch_item_id
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id = batch_item.pay_batch_candidate_id
   AND batch_candidate.pay_batch_id = p_pay_batch_id
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = p_pay_batch_id
    AND scope_row.pay_bank_transfer_id IS NOT NULL
    AND COALESCE(batch_item.is_voided, false) = false;

  SELECT COUNT(*) FILTER (
           WHERE link_row.existing_pay_bank_transfer_id IS NOT NULL
             AND link_row.existing_pay_bank_transfer_id <> link_row.pay_bank_transfer_id
         )::integer,
         COUNT(*) FILTER (
           WHERE link_row.existing_pay_bank_transfer_id = link_row.pay_bank_transfer_id
         )::integer
  INTO v_item_transfer_conflict_count,
       v_item_transfer_reused_count
  FROM pg_temp.tmp_prepare_item_transfer_link AS link_row;

  -- Preserve the exact cause of the following pay_batch_items statement.  The
  -- normal financial-scope trigger must still dirty the candidate; this
  -- transaction-local envelope lets that trigger prove that the only change
  -- was the execution owner's provider-unsubmitted transfer link.
  DROP TABLE IF EXISTS pg_temp._bpay_wb_unsent_execution_overlay_context_v1;
  CREATE TEMPORARY TABLE pg_temp._bpay_wb_unsent_execution_overlay_context_v1 (
    contract_version text NOT NULL,
    execution_operation_id uuid NOT NULL,
    pay_batch_id uuid NOT NULL,
    pay_batch_candidate_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    timesheet_id uuid,
    pay_batch_item_id uuid NOT NULL,
    pay_bank_transfer_id uuid NOT NULL,
    transfer_scope_id uuid NOT NULL,
    source_workbench_session_id uuid,
    source_snapshot_run_id uuid,
    source_session_version bigint,
    row_context_digest text NOT NULL,
    created_at_utc timestamptz NOT NULL
  ) ON COMMIT DROP;

  INSERT INTO pg_temp._bpay_wb_unsent_execution_overlay_context_v1 (
    contract_version,execution_operation_id,pay_batch_id,pay_batch_candidate_id,
    candidate_id,timesheet_id,pay_batch_item_id,pay_bank_transfer_id,
    transfer_scope_id,source_workbench_session_id,source_snapshot_run_id,
    source_session_version,row_context_digest,created_at_utc
  )
  SELECT
    'EXECUTION_UNSENT_OVERLAY_CONTEXT_V1',p_operation_id,p_pay_batch_id,
    batch_item.pay_batch_candidate_id,batch_candidate.candidate_id,
    batch_item.timesheet_id,link_row.pay_batch_item_id,link_row.pay_bank_transfer_id,
    link_row.transfer_scope_id,v_batch_row.source_workbench_session_id,
    v_batch_row.source_snapshot_run_id,v_batch_row.source_session_version,
    md5(
      p_operation_id::text||'|'||p_pay_batch_id::text||'|'||
      batch_candidate.candidate_id::text||'|'||batch_item.pay_batch_candidate_id::text||'|'||
      link_row.pay_batch_item_id::text||'|'||COALESCE(batch_item.timesheet_id::text,'')||'|'||
      link_row.transfer_scope_id::text||'|'||link_row.pay_bank_transfer_id::text||'|'||
      COALESCE(v_batch_row.source_workbench_session_id::text,'')||'|'||
      COALESCE(v_batch_row.source_snapshot_run_id::text,'')||'|'||
      COALESCE(v_batch_row.source_session_version::text,'')||
      '|EXECUTION_UNSENT_OVERLAY_CONTEXT_V1'
    ),v_now
  FROM pg_temp.tmp_prepare_item_transfer_link AS link_row
  JOIN public.pay_batch_items AS batch_item ON batch_item.id=link_row.pay_batch_item_id
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id=batch_item.pay_batch_candidate_id
   AND batch_candidate.pay_batch_id=p_pay_batch_id
  WHERE batch_item.pay_bank_transfer_id IS NULL
    AND link_row.pay_bank_transfer_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM pg_temp.tmp_prepare_item_transfer_link AS conflict_row
      WHERE conflict_row.pay_batch_item_id=link_row.pay_batch_item_id
        AND conflict_row.existing_pay_bank_transfer_id IS NOT NULL
        AND conflict_row.existing_pay_bank_transfer_id<>conflict_row.pay_bank_transfer_id
    );

  WITH linked_batch_items AS (
    UPDATE public.pay_batch_items AS batch_item_update
    SET pay_bank_transfer_id = link_row.pay_bank_transfer_id,
        updated_at = v_now
    FROM pg_temp.tmp_prepare_item_transfer_link AS link_row
    WHERE batch_item_update.id = link_row.pay_batch_item_id
      AND batch_item_update.pay_bank_transfer_id IS NULL
      AND link_row.pay_bank_transfer_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM pg_temp.tmp_prepare_item_transfer_link AS conflict_row
        WHERE conflict_row.pay_batch_item_id = link_row.pay_batch_item_id
          AND conflict_row.existing_pay_bank_transfer_id IS NOT NULL
          AND conflict_row.existing_pay_bank_transfer_id <> conflict_row.pay_bank_transfer_id
      )
    RETURNING batch_item_update.id
  )
  SELECT COUNT(*)::integer
  INTO v_item_transfer_linked_count
  FROM linked_batch_items;

  IF COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN
    UPDATE public.banking_pay_operation_transfer_scope AS scope_conflict_update
    SET status = 'FAILED',
        provider_submit_ready = false,
        provider_submit_state = 'REVIEW_REQUIRED',
        provider_review_required = true,
        provider_unsafe_reason = 'PAY_BATCH_ITEM_TRANSFER_LINK_CONFLICT',
        updated_at_utc = v_now
    FROM (
      SELECT DISTINCT conflict_link.transfer_scope_id
      FROM pg_temp.tmp_prepare_item_transfer_link AS conflict_link
      WHERE conflict_link.existing_pay_bank_transfer_id IS NOT NULL
        AND conflict_link.existing_pay_bank_transfer_id <> conflict_link.pay_bank_transfer_id
    ) AS conflict_scope
    WHERE scope_conflict_update.id = conflict_scope.transfer_scope_id
      AND scope_conflict_update.operation_id = p_operation_id
      AND scope_conflict_update.pay_batch_id = p_pay_batch_id;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(scope_row.status, ''))) IN ('ROLLED_UP', 'PENDING')
      AND coalesce(scope_row.provider_review_required, false) = false
      AND coalesce(scope_row.prepared_item_count, 0) > 0
      AND nullif(btrim(coalesce(scope_row.prepared_result_hash, '')), '') IS NOT NULL
      AND round(coalesce(scope_row.prepared_amount_total, 0), 2) <> 0
      AND (
        (
          v_execution_mode = 'STANDARD_BANK'
          AND upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) IN ('', 'NOT_READY')
        )
        OR (
          v_is_local_manual_mode
          AND coalesce(scope_row.provider_submit_ready, false) = false
          AND upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) IN ('', 'NOT_READY')
        )
      )
    LIMIT 1
  )
  INTO v_remaining_exists;

  SELECT count(*)::integer
  INTO v_authorisation_ready_count
  FROM pg_temp.tmp_prepare_scope_eval AS eval_row
  JOIN public.banking_pay_operation_transfer_scope AS scope_row
    ON scope_row.id = eval_row.transfer_scope_id
  JOIN public.pay_bank_transfers AS transfer_row
    ON transfer_row.id = scope_row.pay_bank_transfer_id
   AND transfer_row.pay_batch_id = scope_row.pay_batch_id
  WHERE upper(btrim(coalesce(scope_row.status, ''))) = 'PREPARED'
    AND scope_row.pay_bank_transfer_id IS NOT NULL
    AND (
      (
        v_execution_mode = 'STANDARD_BANK'
        AND coalesce(scope_row.provider_submit_ready, false) = true
        AND upper(btrim(coalesce(scope_row.provider_submit_state, ''))) = 'READY'
      )
      OR (
        v_is_local_manual_mode
        AND v_mode_evidence_ready
        AND coalesce(scope_row.provider_submit_ready, false) = false
        AND upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) IN ('', 'NOT_READY')
        AND coalesce(scope_row.provider_review_required, false) = false
        AND NULLIF(btrim(coalesce(scope_row.provider_unsafe_reason, '')), '') IS NULL
        AND coalesce(scope_row.prepared_item_count, 0) > 0
        AND NULLIF(btrim(coalesce(scope_row.prepared_scope_hash, '')), '') IS NOT NULL
        AND NULLIF(btrim(coalesce(scope_row.prepared_result_hash, '')), '') IS NOT NULL
        AND round(coalesce(scope_row.prepared_amount_total, 0), 2) = round(coalesce(scope_row.amount, 0), 2)
        AND round(coalesce(scope_row.prepared_amount_total, 0), 2) > 0
        AND upper(btrim(coalesce(transfer_row.status, ''))) = 'PENDING'
        AND transfer_row.pay_channel = scope_row.pay_channel
        AND transfer_row.transfer_group_key = scope_row.transfer_group_key
        AND round(coalesce(transfer_row.amount, 0), 2) = round(coalesce(scope_row.amount, 0), 2)
        AND upper(btrim(coalesce(transfer_row.currency, 'GBP'))) = upper(btrim(coalesce(scope_row.currency, 'GBP')))
        AND NULLIF(btrim(coalesce(transfer_row.request_id, '')), '') IS NULL
        AND NULLIF(btrim(coalesce(transfer_row.rail_tx_id, '')), '') IS NULL
        AND transfer_row.completed_at_utc IS NULL
        AND NULLIF(btrim(coalesce(transfer_row.failed_reason, '')), '') IS NULL
        AND upper(btrim(coalesce(transfer_row.rail_state, ''))) IN ('', 'LOCAL', 'PENDING')
        AND (
          SELECT count(*)
          FROM public.banking_pay_operation_transfer_scope_items AS scope_item_count
          WHERE scope_item_count.operation_id = p_operation_id
            AND scope_item_count.pay_batch_id = p_pay_batch_id
            AND scope_item_count.transfer_scope_id = scope_row.id
        ) = coalesce(scope_row.prepared_item_count, 0)
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_transfer_scope_items AS scope_item_check
          JOIN public.pay_batch_items AS batch_item_check
            ON batch_item_check.id = scope_item_check.pay_batch_item_id
          WHERE scope_item_check.operation_id = p_operation_id
            AND scope_item_check.pay_batch_id = p_pay_batch_id
            AND scope_item_check.transfer_scope_id = scope_row.id
            AND batch_item_check.pay_bank_transfer_id IS DISTINCT FROM scope_row.pay_bank_transfer_id
        )
      )
    );

  v_next_required_phase := CASE
    WHEN coalesce(v_item_transfer_conflict_count, 0) > 0 OR coalesce(v_failed_count, 0) > 0 THEN 'REVIEW_REQUIRED'
    WHEN v_remaining_exists THEN 'TRANSFER_CHUNK_PREPARE_PAGE'
    WHEN v_execution_mode = 'STANDARD_BANK' THEN 'PROVIDER_SUBMIT_CLAIM'
    ELSE 'PREPARE_BATCH_PROOF'
  END;

  UPDATE public.banking_pay_operations AS operation_update
  SET status = CASE WHEN COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN 'REVIEW_REQUIRED' ELSE operation_update.status END,
      phase = CASE
        WHEN COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN 'REVIEW_REQUIRED'
        WHEN v_remaining_exists THEN 'PREPARE_TRANSFER_CHUNKS'
        WHEN v_execution_mode = 'STANDARD_BANK' THEN 'CLAIM_PROVIDER_SUBMIT'
        ELSE 'PREPARE_BATCH_PROOF'
      END,
      runner_state = CASE WHEN COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN 'WAITING_USER_REVIEW' ELSE operation_update.runner_state END,
      requires_user_action = CASE WHEN COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN true ELSE operation_update.requires_user_action END,
      progress_json = jsonb_strip_nulls(coalesce(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
        'last_transfer_chunk_prepare_at_utc', v_now::text,
        'last_transfer_chunk_prepare_scope_count', coalesce(v_requested_count, 0),
        'last_transfer_chunk_prepare_ready_count', coalesce(v_authorisation_ready_count, 0),
        'item_transfer_linked_count', COALESCE(v_item_transfer_linked_count, 0),
        'item_transfer_reused_count', COALESCE(v_item_transfer_reused_count, 0),
        'item_transfer_conflict_count', COALESCE(v_item_transfer_conflict_count, 0),
        'execution_mode', v_execution_mode,
        'provider_submission_required', CASE WHEN v_is_local_manual_mode THEN false ELSE NULL::boolean END,
        'provider_submission_attempted', CASE WHEN v_is_local_manual_mode THEN false ELSE NULL::boolean END,
        'submitted_to_bank', CASE WHEN v_is_local_manual_mode THEN false ELSE NULL::boolean END,
        'local_settlement_evidence_only', CASE WHEN v_is_local_manual_mode THEN true ELSE NULL::boolean END,
        'manual_confirmation_mode', CASE WHEN v_is_local_manual_mode THEN v_manual_confirmation_mode ELSE NULL::text END,
        'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
        'csv_bank_confirm_ref', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref ELSE NULL::text END,
        'external_settlement_comment', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment ELSE NULL::text END,
        'review_required', CASE WHEN COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN true ELSE NULL::boolean END,
        'review_reason_code', CASE WHEN COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN 'PAY_BATCH_ITEM_TRANSFER_LINK_CONFLICT' ELSE NULL::text END,
        'next_required_phase', v_next_required_phase
      )),
      error_json = CASE
        WHEN COALESCE(v_item_transfer_conflict_count, 0) > 0 THEN jsonb_strip_nulls(coalesce(operation_update.error_json, '{}'::jsonb) || jsonb_build_object(
          'code', 'PAY_BATCH_ITEM_TRANSFER_LINK_CONFLICT',
          'message', CASE
            WHEN v_is_local_manual_mode THEN 'One or more frozen pay batch items are already linked to a different bank transfer. Local/manual settlement evidence preparation has been blocked for the affected transfer scope.'
            ELSE 'One or more frozen pay batch items are already linked to a different bank transfer. Provider submission has been blocked for the affected transfer scope.'
          END,
          'operation_id', p_operation_id::text,
          'pay_batch_id', p_pay_batch_id::text,
          'conflict_count', COALESCE(v_item_transfer_conflict_count, 0),
          'detected_at_utc', v_now::text
        ))
        ELSE operation_update.error_json
      END,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', coalesce(v_failed_count, 0) = 0 AND COALESCE(v_item_transfer_conflict_count, 0) = 0,
    'hard_blocker', coalesce(v_failed_count, 0) > 0 OR COALESCE(v_item_transfer_conflict_count, 0) > 0,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'chunk_id', CASE WHEN v_chunk_id IS NULL THEN NULL ELSE v_chunk_id::text END,
    'requested_scope_count', coalesce(v_requested_count, 0),
    'prepared_count', coalesce(v_prepared_count, 0),
    'reused_count', coalesce(v_reused_count, 0),
    'authorisation_ready_count', coalesce(v_authorisation_ready_count, 0),
    'failed_count', coalesce(v_failed_count, 0),
    'item_transfer_linked_count', COALESCE(v_item_transfer_linked_count, 0),
    'item_transfer_reused_count', COALESCE(v_item_transfer_reused_count, 0),
    'item_transfer_conflict_count', COALESCE(v_item_transfer_conflict_count, 0),
    'remaining_count', CASE WHEN v_remaining_exists THEN 1 ELSE 0 END,
    'has_more', v_remaining_exists,
    'execution_mode', v_execution_mode,
    'provider_submission_required', v_execution_mode = 'STANDARD_BANK',
    'provider_submission_attempted', false,
    'submitted_to_bank', false,
    'provider_submit_ready', CASE WHEN v_execution_mode = 'STANDARD_BANK' THEN coalesce(v_authorisation_ready_count, 0) > 0 ELSE false END,
    'local_settlement_evidence_only', v_is_local_manual_mode,
    'next_required_phase', v_next_required_phase,
    'server_utc', v_now::text
  ) || jsonb_strip_nulls(jsonb_build_object(
    'manual_confirmation_mode', CASE WHEN v_is_local_manual_mode THEN v_manual_confirmation_mode ELSE NULL::text END,
    'bank_csv_generated', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_generated ELSE NULL::boolean END,
    'bank_csv_current', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_current ELSE NULL::boolean END,
    'bank_csv_scope', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_scope ELSE NULL::text END,
    'bank_csv_paye_net_state_hash', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_paye_net_state_hash ELSE NULL::text END,
    'bank_csv_bank_payment_projection_hash', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_bank_payment_projection_hash ELSE NULL::text END,
    'bank_csv_row_count', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_row_count ELSE NULL::integer END,
    'bank_csv_total_amount', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_bank_csv_total_amount ELSE NULL::numeric END,
    'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
    'csv_bank_confirm_ref', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref ELSE NULL::text END,
    'external_settlement_comment', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment ELSE NULL::text END
  ));
END;
$function$;
