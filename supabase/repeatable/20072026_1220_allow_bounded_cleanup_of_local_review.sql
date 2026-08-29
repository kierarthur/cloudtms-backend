-- Canonical bounded cleanup base.
-- A REVIEW_REQUIRED scope is safe to clean only when its reason is an
-- allow-listed pre-provider validation result and every provider/bank evidence
-- field remains empty. Any provider or ambiguous evidence continues to block
-- cleanup and requires reconciliation.

CREATE OR REPLACE FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(
  p_operation_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_failure_phase text DEFAULT NULL::text,
  p_failure_error_json jsonb DEFAULT '{}'::jsonb,
  p_dry_run boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_operation_type_upper text := NULL::text;
  v_failure_phase text := NULL::text;
  v_failure_error_json jsonb := '{}'::jsonb;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_batch_execution_boundary_crossed boolean := false;
  v_chunk_id uuid := NULL::uuid;
  v_provider_state_risk_count integer := 0;
  v_provider_chunk_risk_count integer := 0;
  v_transfer_event_risk_count integer := 0;
  v_scope_rows_considered integer := 0;
  v_transfer_rows_considered integer := 0;
  v_scope_rows_deleted integer := 0;
  v_transfer_rows_deleted integer := 0;
  v_item_links_cleared integer := 0;
  v_bank_references_cleared integer := 0;
  v_chunks_marked_failed integer := 0;
  v_chunks_marked_skipped integer := 0;
  v_locks_released integer := 0;
  v_review_required boolean := false;
  v_safe_to_retry boolean := false;
  v_retry_blocked boolean := true;
  v_retry_blocked_reason text := NULL::text;
  v_cleanup_mode text := 'REVIEW_REQUIRED';
  v_deleted_scope_ids jsonb := '[]'::jsonb;
  v_deleted_transfer_ids jsonb := '[]'::jsonb;
  v_safe_scope_ids jsonb := '[]'::jsonb;
  v_safe_transfer_ids jsonb := '[]'::jsonb;
  v_unsafe_reasons jsonb := '[]'::jsonb;
  v_active_auth_request_count integer := 0;
  v_operation_active_auth_request_count integer := 0;
  v_auth_requests_cancelled integer := 0;
  v_auth_tokens_voided integer := 0;
  v_batch_execution_intent_cleared integer := 0;
  v_result jsonb := '{}'::jsonb;
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_ID_REQUIRED',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: operation_id is required'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_failure_error_json IS NOT NULL AND jsonb_typeof(p_failure_error_json) = 'object' THEN
    v_failure_error_json := p_failure_error_json;
  ELSE
    v_failure_error_json := '{}'::jsonb;
  END IF;

  v_failure_phase := NULLIF(BTRIM(COALESCE(p_failure_phase, v_failure_error_json->>'phase', v_failure_error_json->>'failure_phase', '')), '');

  IF COALESCE(v_failure_error_json->>'chunk_id', v_failure_error_json->>'provider_chunk_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_chunk_id := COALESCE(v_failure_error_json->>'chunk_id', v_failure_error_json->>'provider_chunk_id')::uuid;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF v_operation_row.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_NOT_FOUND',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: operation not found',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_operation_row.actor_user_id);
  IF v_failure_phase IS NULL THEN
    v_failure_phase := NULLIF(BTRIM(COALESCE(v_operation_row.phase, '')), '');
  END IF;

  v_operation_type_upper := upper(BTRIM(COALESCE(v_operation_row.operation_type, '')));
  IF v_operation_type_upper NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_TYPE_NOT_SUPPORTED',
      'message', 'Only PAYMENT_EXECUTE and PAYMENT_RETRY_BLOCKED_FUNDS operations are supported.',
      'operation_id', p_operation_id::text,
      'operation_type', v_operation_row.operation_type
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF upper(BTRIM(COALESCE(v_operation_row.status, ''))) = 'COMPLETE' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'COMPLETE_OPERATION_CANNOT_BE_CLEANED',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_row.pay_batch_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_operation_row.pay_batch_id
  FOR UPDATE;

  IF v_batch_row.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'PAY_BATCH_NOT_FOUND',
      'operation_id', p_operation_id::text,
      'pay_batch_id', v_operation_row.pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_batch_execution_boundary_crossed := (
    upper(BTRIM(COALESCE(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
    OR NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
    OR v_batch_row.execution_committed_at_utc IS NOT NULL
  );

  SELECT COUNT(*)::integer
  INTO v_scope_rows_considered
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id);

  SELECT COUNT(DISTINCT scope_row.pay_bank_transfer_id)::integer
  INTO v_transfer_rows_considered
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND scope_row.pay_bank_transfer_id IS NOT NULL
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id);

  SELECT COUNT(*)::integer
  INTO v_provider_state_risk_count
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id)
    AND (
      upper(BTRIM(COALESCE(scope_row.provider_submit_state, ''))) IN (
        'REQUEST_SENDING',
        'REQUEST_SENT_LOCAL',
        'PROVIDER_ACCEPTED',
        'PROVIDER_REJECTED',
        'PROVIDER_UNKNOWN',
        'CHUNK_FINALISED'
      )
      OR (
        upper(BTRIM(COALESCE(scope_row.provider_submit_state, ''))) = 'REVIEW_REQUIRED'
        AND NOT (
          upper(BTRIM(COALESCE(scope_row.provider_unsafe_reason, ''))) IN (
            'TRANSFER_GROUP_PAYOUT_INSTRUCTION_INVALID',
            'PAYE_NET_REQUIRED_FOR_EXECUTION',
            'TRANSFER_SCOPE_NON_POSITIVE_AMOUNT',
            'TRANSFER_SCOPE_ITEM_SOURCE_EMPTY',
            'TRANSFER_SCOPE_ITEM_SEED_ERROR',
            'TRANSFER_SCOPE_ITEM_SEED_INCOMPLETE',
            'TRANSFER_SCOPE_ITEM_ROLLUP_PENDING'
          )
          AND scope_row.pay_bank_transfer_id IS NULL
          AND scope_row.provider_submit_chunk_id IS NULL
          AND COALESCE(scope_row.provider_submit_attempt_count, 0) = 0
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_idempotency_key, '')), '') IS NULL
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_request_id, '')), '') IS NULL
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_transaction_id, '')), '') IS NULL
          AND scope_row.provider_request_prepared_at_utc IS NULL
          AND scope_row.provider_request_sending_at_utc IS NULL
          AND scope_row.provider_request_sent_at_utc IS NULL
          AND scope_row.provider_response_at_utc IS NULL
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_submission_status, '')), '') IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM public.banking_pay_operation_provider_attempts AS provider_attempt
            WHERE provider_attempt.operation_id = scope_row.operation_id
              AND provider_attempt.transfer_scope_id = scope_row.id
          )
        )
      )
    );

  SELECT COUNT(*)::integer
  INTO v_provider_chunk_risk_count
  FROM public.banking_pay_operation_chunks AS chunk_row
  WHERE chunk_row.operation_id = p_operation_id
    AND (v_chunk_id IS NULL OR chunk_row.id = v_chunk_id)
    AND (chunk_row.phase = 'SUBMIT_PROVIDER_TRANSFERS' OR chunk_row.chunk_type = 'TRANSFER_SUBMIT')
    AND (
      upper(BTRIM(COALESCE(chunk_row.status, ''))) IN ('RUNNING', 'COMPLETE', 'FAILED')
      OR chunk_row.started_at_utc IS NOT NULL
      OR COALESCE(chunk_row.result_json, '{}'::jsonb) <> '{}'::jsonb
      OR COALESCE(chunk_row.error_json, '{}'::jsonb) <> '{}'::jsonb
    );

  SELECT COUNT(*)::integer
  INTO v_transfer_event_risk_count
  FROM public.pay_bank_transfer_events AS event_row
  WHERE event_row.pay_batch_id = v_operation_row.pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_transfer_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
        AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id)
        AND scope_row.pay_bank_transfer_id = event_row.pay_bank_transfer_id
    );

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE auth_request.execution_intent_json->>'operation_id' = p_operation_id::text)::integer
  INTO v_active_auth_request_count,
       v_operation_active_auth_request_count
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
    AND auth_request.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

  v_review_required := v_batch_execution_boundary_crossed
    OR COALESCE(v_provider_state_risk_count, 0) > 0
    OR COALESCE(v_provider_chunk_risk_count, 0) > 0
    OR COALESCE(v_transfer_event_risk_count, 0) > 0
    OR COALESCE(v_active_auth_request_count, 0) > COALESCE(v_operation_active_auth_request_count, 0);

  IF v_review_required THEN
    v_retry_blocked := true;
    v_safe_to_retry := false;
    v_retry_blocked_reason := CASE
      WHEN v_batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED'
      WHEN COALESCE(v_provider_state_risk_count, 0) > 0 THEN 'PROVIDER_STATE_REQUIRES_RECONCILIATION'
      WHEN COALESCE(v_provider_chunk_risk_count, 0) > 0 THEN 'PROVIDER_SUBMIT_CHUNK_REQUIRES_RECONCILIATION'
      WHEN COALESCE(v_transfer_event_risk_count, 0) > 0 THEN 'TRANSFER_EVENT_PRESENT'
      ELSE 'ACTIVE_AUTH_REQUEST_PRESENT'
    END;
    v_cleanup_mode := 'REVIEW_REQUIRED_' || v_retry_blocked_reason;
    v_unsafe_reasons := jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
      'reason', v_retry_blocked_reason,
      'provider_state_risk_count', COALESCE(v_provider_state_risk_count, 0),
      'provider_chunk_risk_count', COALESCE(v_provider_chunk_risk_count, 0),
      'transfer_event_risk_count', COALESCE(v_transfer_event_risk_count, 0),
      'active_auth_request_count', COALESCE(v_active_auth_request_count, 0)
    )));
  ELSE
    v_retry_blocked := false;
    v_safe_to_retry := true;
    v_cleanup_mode := CASE WHEN COALESCE(p_dry_run, false) THEN 'DRY_RUN_BOUNDED_LOCAL_CLEANUP' ELSE 'CLEANED_BOUNDED_LOCAL_ARTIFACTS' END;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_cleanup_scope_ids;
  CREATE TEMPORARY TABLE pg_temp.tmp_cleanup_scope_ids AS
  SELECT scope_row.id AS scope_id,
         scope_row.pay_bank_transfer_id
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id)
    AND COALESCE(v_review_required, false) IS FALSE
  ORDER BY scope_row.id
  LIMIT 100;

  SELECT COALESCE(jsonb_agg(to_jsonb(cleanup_scope.scope_id::text) ORDER BY cleanup_scope.scope_id), '[]'::jsonb)
  INTO v_safe_scope_ids
  FROM pg_temp.tmp_cleanup_scope_ids AS cleanup_scope;

  SELECT COALESCE(jsonb_agg(DISTINCT to_jsonb(cleanup_scope.pay_bank_transfer_id::text)), '[]'::jsonb)
  INTO v_safe_transfer_ids
  FROM pg_temp.tmp_cleanup_scope_ids AS cleanup_scope
  WHERE cleanup_scope.pay_bank_transfer_id IS NOT NULL;

  IF COALESCE(p_dry_run, false) IS FALSE AND COALESCE(v_review_required, false) IS FALSE THEN
    WITH same_operation_auth_requests AS (
      SELECT auth_request.id
      FROM public.pay_batch_auth_requests AS auth_request
      WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
        AND auth_request.state IN ('AWAITING', 'AUTHORISED')
        AND auth_request.execution_intent_json->>'operation_id' = p_operation_id::text
    ), cancelled_auth_requests AS (
      UPDATE public.pay_batch_auth_requests AS auth_request_update
      SET state = 'CANCELLED',
          finalised_at_utc = COALESCE(auth_request_update.finalised_at_utc, v_now),
          finalised_by_user_id = COALESCE(auth_request_update.finalised_by_user_id, v_effective_actor_user_id),
          execution_intent_json = jsonb_strip_nulls(COALESCE(auth_request_update.execution_intent_json, '{}'::jsonb) || jsonb_build_object(
            'cancelled_by_bounded_execution_cleanup', true,
            'cleanup_operation_id', p_operation_id::text,
            'cleanup_at_utc', v_now::text,
            'failure_phase', v_failure_phase
          ))
      FROM same_operation_auth_requests
      WHERE auth_request_update.id = same_operation_auth_requests.id
      RETURNING auth_request_update.id
    )
    SELECT COUNT(*)::integer
    INTO v_auth_requests_cancelled
    FROM cancelled_auth_requests;

    WITH voided_tokens AS (
      UPDATE public.pay_batch_auth_tokens AS auth_token_update
      SET used_at_utc = COALESCE(auth_token_update.used_at_utc, v_now),
          expires_at_utc = CASE WHEN auth_token_update.expires_at_utc > v_now THEN v_now ELSE auth_token_update.expires_at_utc END
      WHERE auth_token_update.auth_request_id IN (
        SELECT auth_request.id
        FROM public.pay_batch_auth_requests AS auth_request
        WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
          AND auth_request.execution_intent_json->>'operation_id' = p_operation_id::text
          AND auth_request.state = 'CANCELLED'
      )
        AND (auth_token_update.used_at_utc IS NULL OR auth_token_update.expires_at_utc > v_now)
      RETURNING auth_token_update.token
    )
    SELECT COUNT(*)::integer
    INTO v_auth_tokens_voided
    FROM voided_tokens;

    WITH item_rows_to_clear AS (
      SELECT item_row.id AS pay_batch_item_id,
             item_row.bank_reference AS previous_bank_reference,
             transfer_row.payment_reference AS transfer_payment_reference
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = item_row.pay_batch_candidate_id
       AND batch_candidate.pay_batch_id = v_operation_row.pay_batch_id
      JOIN pg_temp.tmp_cleanup_scope_ids AS cleanup_scope
        ON cleanup_scope.pay_bank_transfer_id = item_row.pay_bank_transfer_id
      JOIN public.pay_bank_transfers AS transfer_row
        ON transfer_row.id = cleanup_scope.pay_bank_transfer_id
    ), cleared_item_links AS (
      UPDATE public.pay_batch_items AS item_update
      SET pay_bank_transfer_id = NULL,
          bank_reference = CASE WHEN item_update.bank_reference = item_rows_to_clear.transfer_payment_reference THEN NULL ELSE item_update.bank_reference END,
          updated_at = v_now
      FROM item_rows_to_clear
      WHERE item_update.id = item_rows_to_clear.pay_batch_item_id
      RETURNING item_update.id,
                item_rows_to_clear.previous_bank_reference,
                item_rows_to_clear.transfer_payment_reference
    )
    SELECT COUNT(*)::integer,
           COALESCE((COUNT(*) FILTER (
             WHERE cleared_item_links.previous_bank_reference = cleared_item_links.transfer_payment_reference
               AND NULLIF(BTRIM(COALESCE(cleared_item_links.previous_bank_reference, '')), '') IS NOT NULL
           )), 0)::integer
    INTO v_item_links_cleared,
         v_bank_references_cleared
    FROM cleared_item_links;

    WITH deleted_scope_rows AS (
      DELETE FROM public.banking_pay_operation_transfer_scope AS scope_delete
      USING pg_temp.tmp_cleanup_scope_ids AS cleanup_scope
      WHERE scope_delete.id = cleanup_scope.scope_id
        AND scope_delete.operation_id = p_operation_id
        AND scope_delete.pay_batch_id = v_operation_row.pay_batch_id
      RETURNING scope_delete.id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(to_jsonb(deleted_scope_rows.id::text) ORDER BY deleted_scope_rows.id), '[]'::jsonb)
    INTO v_scope_rows_deleted,
         v_deleted_scope_ids
    FROM deleted_scope_rows;

    WITH deleted_transfer_rows AS (
      DELETE FROM public.pay_bank_transfers AS transfer_delete
      WHERE transfer_delete.pay_batch_id = v_operation_row.pay_batch_id
        AND transfer_delete.id IN (SELECT cleanup_scope.pay_bank_transfer_id FROM pg_temp.tmp_cleanup_scope_ids AS cleanup_scope WHERE cleanup_scope.pay_bank_transfer_id IS NOT NULL)
        AND transfer_delete.status = 'PENDING'
        AND transfer_delete.rail_tx_id IS NULL
        AND transfer_delete.completed_at_utc IS NULL
        AND transfer_delete.failed_reason IS NULL
        AND NOT EXISTS (SELECT 1 FROM public.banking_pay_operation_transfer_scope AS remaining_scope WHERE remaining_scope.pay_bank_transfer_id = transfer_delete.id)
        AND NOT EXISTS (SELECT 1 FROM public.pay_batch_items AS remaining_item WHERE remaining_item.pay_bank_transfer_id = transfer_delete.id)
        AND NOT EXISTS (SELECT 1 FROM public.pay_bank_transfer_events AS transfer_event WHERE transfer_event.pay_bank_transfer_id = transfer_delete.id)
      RETURNING transfer_delete.id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(to_jsonb(deleted_transfer_rows.id::text) ORDER BY deleted_transfer_rows.id), '[]'::jsonb)
    INTO v_transfer_rows_deleted,
         v_deleted_transfer_ids
    FROM deleted_transfer_rows;

    WITH chunks_to_mutate AS (
      SELECT operation_chunk.id AS operation_chunk_id,
             operation_chunk.status AS previous_status,
             (operation_chunk.locked_by IS NOT NULL OR operation_chunk.lock_expires_at_utc IS NOT NULL) AS had_lock
      FROM public.banking_pay_operation_chunks AS operation_chunk
      WHERE operation_chunk.operation_id = p_operation_id
        AND operation_chunk.status IN ('PENDING', 'RUNNING')
        AND NOT (operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS' OR operation_chunk.chunk_type = 'TRANSFER_SUBMIT')
      ORDER BY operation_chunk.sequence_no, operation_chunk.id
      LIMIT 100
    ), mutated_chunks AS (
      UPDATE public.banking_pay_operation_chunks AS operation_chunk_update
      SET status = CASE WHEN chunks_to_mutate.previous_status = 'RUNNING' THEN 'FAILED' ELSE 'SKIPPED' END,
          error_json = CASE WHEN chunks_to_mutate.previous_status = 'RUNNING' THEN jsonb_build_object('code', 'PAYMENT_EXECUTE_BOUNDED_LOCAL_CLEANUP', 'operation_id', p_operation_id::text, 'failure_phase', v_failure_phase, 'cleanup_at_utc', v_now::text) ELSE operation_chunk_update.error_json END,
          locked_by = NULL::text,
          lock_expires_at_utc = NULL::timestamptz,
          completed_at_utc = COALESCE(operation_chunk_update.completed_at_utc, v_now),
          updated_at_utc = v_now
      FROM chunks_to_mutate
      WHERE operation_chunk_update.id = chunks_to_mutate.operation_chunk_id
      RETURNING chunks_to_mutate.previous_status,
                chunks_to_mutate.had_lock
    )
    SELECT COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.previous_status = 'RUNNING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.previous_status = 'PENDING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.had_lock)), 0)::integer
    INTO v_chunks_marked_failed,
         v_chunks_marked_skipped,
         v_locks_released
    FROM mutated_chunks;

    UPDATE public.pay_batches AS batch_update
    SET execution_intent_json = CASE
          WHEN NULLIF(BTRIM(COALESCE(batch_update.execution_intent_json->>'operation_id', '')), '') = p_operation_id::text THEN NULL::jsonb
          ELSE batch_update.execution_intent_json
        END
    WHERE batch_update.id = v_operation_row.pay_batch_id
      AND v_batch_execution_boundary_crossed IS FALSE;
    GET DIAGNOSTICS v_batch_execution_intent_cleared = ROW_COUNT;
  END IF;

  IF COALESCE(p_dry_run, false) IS FALSE THEN
    UPDATE public.banking_pay_operations AS operation_update
    SET status = CASE WHEN v_review_required THEN 'REVIEW_REQUIRED' ELSE 'FAILED' END,
        runner_state = CASE WHEN v_review_required THEN 'WAITING_USER_REVIEW' ELSE 'FAILED' END,
        run_after_utc = NULL::timestamptz,
        requires_user_action = v_review_required,
        resume_reason = CASE WHEN v_review_required THEN COALESCE(v_retry_blocked_reason, 'PAYMENT_EXECUTION_CLEANUP_REVIEW_REQUIRED') ELSE 'PAYMENT_EXECUTION_LOCAL_ARTIFACTS_CLEANED' END,
        error_json = jsonb_strip_nulls(COALESCE(operation_update.error_json, '{}'::jsonb) || jsonb_build_object(
          'cleanup_at_utc', v_now::text,
          'cleanup_mode', v_cleanup_mode,
          'retry_blocked_reason', v_retry_blocked_reason,
          'failure_phase', v_failure_phase
        )),
        locked_by = NULL::text,
        lock_expires_at_utc = NULL::timestamptz,
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    BEGIN
      PERFORM public.pay_batch_display_summary_touch(v_operation_row.pay_batch_id);
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;

    BEGIN
      PERFORM public.banking_pay_batch_signal_touch(
        v_operation_row.pay_batch_id,
        CASE WHEN v_review_required THEN 'PAYMENT_EXECUTION_CLEANUP_REVIEW_REQUIRED' ELSE 'PAYMENT_EXECUTION_LOCAL_ARTIFACTS_CLEANED' END,
        'pay_execute_operation_cleanup_failed_local_artifacts',
        jsonb_strip_nulls(jsonb_build_object(
          'operation_id', p_operation_id::text,
          'cleanup_mode', v_cleanup_mode,
          'retry_blocked_reason', v_retry_blocked_reason
        )),
        true,
        false,
        v_review_required,
        true
      );
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END IF;

  v_result := jsonb_strip_nulls(jsonb_build_object(
    'ok', true,
    'dry_run', COALESCE(p_dry_run, false),
    'operation_id', p_operation_id::text,
    'pay_batch_id', v_operation_row.pay_batch_id::text,
    'chunk_id', CASE WHEN v_chunk_id IS NULL THEN NULL ELSE v_chunk_id::text END,
    'failure_phase', v_failure_phase,
    'cleanup_mode', v_cleanup_mode,
    'retry_blocked_reason', v_retry_blocked_reason,
    'safe_to_retry', v_safe_to_retry,
    'retry_blocked', v_retry_blocked,
    'review_required', v_review_required,
    'scope_rows_considered', COALESCE(v_scope_rows_considered, 0),
    'transfer_rows_considered', COALESCE(v_transfer_rows_considered, 0),
    'provider_state_risk_count', COALESCE(v_provider_state_risk_count, 0),
    'provider_chunk_risk_count', COALESCE(v_provider_chunk_risk_count, 0),
    'transfer_event_risk_count', COALESCE(v_transfer_event_risk_count, 0),
    'scope_rows_deleted', COALESCE(v_scope_rows_deleted, 0),
    'transfer_rows_deleted', COALESCE(v_transfer_rows_deleted, 0),
    'item_links_cleared', COALESCE(v_item_links_cleared, 0),
    'bank_references_cleared', COALESCE(v_bank_references_cleared, 0),
    'chunks_marked_failed', COALESCE(v_chunks_marked_failed, 0),
    'chunks_marked_skipped', COALESCE(v_chunks_marked_skipped, 0),
    'locks_released', COALESCE(v_locks_released, 0),
    'safe_scope_ids', CASE WHEN COALESCE(p_dry_run, false) THEN COALESCE(v_safe_scope_ids, '[]'::jsonb) ELSE COALESCE(v_deleted_scope_ids, '[]'::jsonb) END,
    'safe_transfer_ids', CASE WHEN COALESCE(p_dry_run, false) THEN COALESCE(v_safe_transfer_ids, '[]'::jsonb) ELSE COALESCE(v_deleted_transfer_ids, '[]'::jsonb) END,
    'unsafe_reasons', COALESCE(v_unsafe_reasons, '[]'::jsonb),
    'active_auth_request_count', COALESCE(v_active_auth_request_count, 0),
    'operation_active_auth_request_count', COALESCE(v_operation_active_auth_request_count, 0),
    'auth_requests_cancelled', COALESCE(v_auth_requests_cancelled, 0),
    'auth_tokens_voided', COALESCE(v_auth_tokens_voided, 0),
    'batch_execution_intent_cleared', COALESCE(v_batch_execution_intent_cleared, 0),
    'batch_execution_boundary_crossed', v_batch_execution_boundary_crossed,
    'execution_commit_state', upper(BTRIM(COALESCE(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))),
    'execution_commit_ref_present', NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL,
    'execution_committed_at_utc_present', v_batch_row.execution_committed_at_utc IS NOT NULL
  ));

  RETURN v_result;
END;
$function$;

REVOKE ALL ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) FROM anon;
REVOKE ALL ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) FROM authenticated;
GRANT EXECUTE ON FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(uuid, uuid, text, jsonb, boolean) TO service_role;

