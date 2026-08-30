-- Cancellation completion and workbench repayment restoration.
--
-- Policy X boundary:
--   * the cancellation itself continues to operate only on frozen batch artifacts;
--   * only after the draft has been cancelled do affected candidates return to
--     PRE_DRAFT_LIVE_TRUTH and receive a full-candidate live rebuild;
--   * no frozen economic key is replaced with a live fallback.

CREATE OR REPLACE FUNCTION public.pay_payment_cancel_finalise_metadata_v1(
  p_pay_batch_id uuid,
  p_correction_request_id uuid,
  p_actor_user_id uuid,
  p_reason text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_batch public.pay_batches%ROWTYPE;
  v_request public.pay_payment_correction_requests%ROWTYPE;
  v_effective_actor_user_id uuid;
  v_actor_display text;
  v_actor_role text;
  v_effective_reason text;
  v_non_voided_item_count integer := 0;
  v_non_terminal_work_count integer := 0;
  v_audit_inserted_count integer := 0;
  v_before_json jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL OR p_correction_request_id IS NULL OR p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_REQUIRED_IDENTIFIERS_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_REQUIRED_IDENTIFIERS_MISSING',
              'pay_batch_id_present', p_pay_batch_id IS NOT NULL,
              'correction_request_id_present', p_correction_request_id IS NOT NULL,
              'actor_user_id_present', p_actor_user_id IS NOT NULL
            )::text;
  END IF;

  SELECT request_row.*
  INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_request.pay_batch_id IS DISTINCT FROM p_pay_batch_id
     OR UPPER(BTRIM(COALESCE(v_request.correction_kind, ''))) <> 'PRE_BANK_CANCEL' THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_CORRECTION_REQUEST_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_CORRECTION_REQUEST_INVALID',
              'pay_batch_id', p_pay_batch_id::text,
              'correction_request_id', p_correction_request_id::text
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_request.status, ''))) <> 'APPLIED' THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_CORRECTION_NOT_APPLIED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_CORRECTION_NOT_APPLIED',
              'pay_batch_id', p_pay_batch_id::text,
              'correction_request_id', p_correction_request_id::text,
              'correction_status', v_request.status
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_non_terminal_work_count
  FROM public.pay_payment_correction_work_items AS work_item
  WHERE work_item.correction_request_id = p_correction_request_id
    AND UPPER(BTRIM(COALESCE(work_item.status, ''))) NOT IN ('APPLIED', 'SKIPPED', 'CANCELLED');

  IF COALESCE(v_non_terminal_work_count, 0) <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_WORK_NOT_TERMINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_WORK_NOT_TERMINAL',
              'pay_batch_id', p_pay_batch_id::text,
              'correction_request_id', p_correction_request_id::text,
              'non_terminal_work_count', v_non_terminal_work_count
            )::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_batch.status, ''))) <> 'CANCELLED'
     OR UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
     OR NULLIF(BTRIM(COALESCE(v_batch.execution_commit_ref, '')), '') IS NOT NULL
     OR v_batch.execution_committed_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_BATCH_NOT_SAFE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_BATCH_NOT_SAFE',
              'pay_batch_id', p_pay_batch_id::text,
              'status', v_batch.status,
              'execution_commit_state', v_batch.execution_commit_state,
              'execution_commit_ref_present', NULLIF(BTRIM(COALESCE(v_batch.execution_commit_ref, '')), '') IS NOT NULL,
              'execution_committed_at_present', v_batch.execution_committed_at_utc IS NOT NULL
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_non_voided_item_count
  FROM public.pay_batch_items AS batch_item
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id = batch_item.pay_batch_candidate_id
  WHERE batch_candidate.pay_batch_id = p_pay_batch_id
    AND COALESCE(batch_item.is_voided, false) IS NOT TRUE
    AND UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) <> 'DEBT_CREATED';

  IF COALESCE(v_non_voided_item_count, 0) <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_ACTIVE_ITEMS_REMAIN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_ACTIVE_ITEMS_REMAIN',
              'pay_batch_id', p_pay_batch_id::text,
              'active_item_count', v_non_voided_item_count
            )::text;
  END IF;

  v_effective_actor_user_id := COALESCE(
    v_batch.cancelled_by_user_id,
    v_request.requested_by_user_id,
    p_actor_user_id
  );
  v_effective_reason := COALESCE(
    NULLIF(BTRIM(COALESCE(v_batch.cancel_reason, '')), ''),
    NULLIF(BTRIM(COALESCE(p_reason, '')), ''),
    NULLIF(BTRIM(COALESCE(v_request.reason, '')), ''),
    'Draft batch cancelled before bank submission'
  );

  SELECT
    COALESCE(NULLIF(BTRIM(COALESCE(user_row.display_name, '')), ''), NULLIF(BTRIM(COALESCE(user_row.email, '')), ''), 'CloudTMS user'),
    COALESCE(NULLIF(BTRIM(COALESCE(user_row.role, '')), ''), 'system')
  INTO v_actor_display, v_actor_role
  FROM public.tms_users AS user_row
  WHERE user_row.id = v_effective_actor_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FINALISE_ACTOR_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_FINALISE_ACTOR_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text,
              'correction_request_id', p_correction_request_id::text
            )::text;
  END IF;

  v_before_json := jsonb_build_object(
    'status', v_batch.status,
    'cancelled_at_utc', v_batch.cancelled_at_utc,
    'cancelled_by_user_id', v_batch.cancelled_by_user_id,
    'cancel_reason', v_batch.cancel_reason,
    'schedule_kind', v_batch.schedule_kind,
    'scheduled_at_utc', v_batch.scheduled_at_utc,
    'scheduled_by_user_id', v_batch.scheduled_by_user_id,
    'funding_account_ref_present', NULLIF(BTRIM(COALESCE(v_batch.funding_account_ref, '')), '') IS NOT NULL
  );

  UPDATE public.pay_batches AS batch_to_finalise
  SET cancelled_at_utc = COALESCE(batch_to_finalise.cancelled_at_utc, v_request.applied_at_utc, v_now),
      cancelled_by_user_id = COALESCE(batch_to_finalise.cancelled_by_user_id, v_effective_actor_user_id),
      cancel_reason = COALESCE(NULLIF(BTRIM(COALESCE(batch_to_finalise.cancel_reason, '')), ''), v_effective_reason),
      schedule_kind = NULL,
      scheduled_at_utc = NULL,
      scheduled_by_user_id = NULL,
      funding_account_ref = NULL,
      funds_warning_hours_json = NULL,
      execution_commit_state = COALESCE(NULLIF(BTRIM(COALESCE(batch_to_finalise.execution_commit_state, '')), ''), 'NOT_SUBMITTED')
  WHERE batch_to_finalise.id = p_pay_batch_id;

  INSERT INTO public.audit_events (
    actor_user_id,
    actor_display,
    actor_role_at_time,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    correlation_id
  )
  SELECT
    v_effective_actor_user_id,
    v_actor_display,
    v_actor_role,
    'pay_batch',
    p_pay_batch_id::text,
    'PAY_BATCH_CANCELLED',
    v_before_json,
    jsonb_build_object(
      'status', 'CANCELLED',
      'cancelled_at_utc', final_batch.cancelled_at_utc,
      'cancelled_by_user_id', final_batch.cancelled_by_user_id,
      'cancelled_by_display', v_actor_display,
      'cancel_reason', final_batch.cancel_reason,
      'correction_request_id', p_correction_request_id::text,
      'source_workbench_session_id', final_batch.source_workbench_session_id,
      'source_session_preserved', true,
      'execution_commit_state', final_batch.execution_commit_state
    ),
    v_effective_reason,
    p_correction_request_id::text
  FROM public.pay_batches AS final_batch
  WHERE final_batch.id = p_pay_batch_id
    AND NOT EXISTS (
      SELECT 1
      FROM public.audit_events AS existing_audit
      WHERE existing_audit.object_type = 'pay_batch'
        AND existing_audit.object_id_text = p_pay_batch_id::text
        AND existing_audit.action = 'PAY_BATCH_CANCELLED'
        AND existing_audit.correlation_id = p_correction_request_id::text
    );

  GET DIAGNOSTICS v_audit_inserted_count = ROW_COUNT;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'correction_request_id', p_correction_request_id::text,
    'batch_status', 'CANCELLED',
    'cancelled_by_user_id', v_effective_actor_user_id::text,
    'cancelled_by_display', v_actor_display,
    'audit_action', 'PAY_BATCH_CANCELLED',
    'audit_inserted', COALESCE(v_audit_inserted_count, 0) > 0,
    'source_session_preserved', true,
    'policy_x_checked', true
  );
END;
$function$;

ALTER FUNCTION public.pay_payment_cancel_finalise_metadata_v1(uuid, uuid, uuid, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_finalise_metadata_v1(uuid, uuid, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_finalise_metadata_v1(uuid, uuid, uuid, text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_finalise_metadata_v1(uuid, uuid, uuid, text) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.pay_payment_cancel_finalise_metadata_v1(uuid, uuid, uuid, text) TO service_role;


CREATE OR REPLACE FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(
  p_pay_batch_id uuid,
  p_selection_json jsonb DEFAULT '{}'::jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_reason text DEFAULT NULL::text,
  p_idempotency_key text DEFAULT NULL::text,
  p_confirmation_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_result jsonb := '{}'::jsonb;
  v_chunk_result jsonb := '{}'::jsonb;
  v_finalise_result jsonb := '{}'::jsonb;
  v_changed_scope_json jsonb := '{}'::jsonb;
  v_correction_request_id uuid;
  v_complete boolean := false;
  v_iteration integer := 0;
  v_released_reservations integer := 0;
  v_restored_finance_components integer := 0;
  v_carry_forward_created integer := 0;
  v_carry_forward_existing integer := 0;
  v_carry_forward_released integer := 0;
BEGIN
  v_result := public.pay_payment_cancel_not_sent_and_recalculate(
    p_pay_batch_id => p_pay_batch_id,
    p_selection_json => p_selection_json,
    p_actor_user_id => p_actor_user_id,
    p_reason => p_reason,
    p_idempotency_key => p_idempotency_key,
    p_confirmation_json => p_confirmation_json
  );

  IF COALESCE((v_result->>'ok')::boolean, true) IS NOT TRUE THEN
    RETURN v_result;
  END IF;

  IF COALESCE(v_result->>'correction_request_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_COMPLETE_CORRECTION_REQUEST_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_COMPLETE_CORRECTION_REQUEST_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  v_correction_request_id := (v_result->>'correction_request_id')::uuid;
  v_complete := COALESCE(
    NULLIF(v_result->>'is_complete', '')::boolean,
    NULLIF(v_result#>>'{process_result,complete}', '')::boolean,
    false
  );
  v_released_reservations := COALESCE((v_result->>'released_reservation_count')::integer, 0);
  v_restored_finance_components := COALESCE((v_result->>'restored_finance_component_count')::integer, 0);
  v_carry_forward_created := COALESCE((v_result->>'carry_forward_created_count')::integer, 0);
  v_carry_forward_existing := COALESCE((v_result->>'carry_forward_existing_count')::integer, 0);
  v_carry_forward_released := COALESCE((v_result->>'carry_forward_released_count')::integer, 0);

  WHILE v_complete IS NOT TRUE AND v_iteration < 100 LOOP
    v_iteration := v_iteration + 1;
    v_chunk_result := public.pay_payment_correction_process_chunk(
      p_correction_request_id => v_correction_request_id,
      p_limit => 100,
      p_worker_id => 'cancel-not-sent-complete-v1',
      p_actor_user_id => p_actor_user_id
    );

    v_released_reservations := v_released_reservations + COALESCE((v_chunk_result->>'released_reservations')::integer, 0);
    v_restored_finance_components := v_restored_finance_components + COALESCE((v_chunk_result->>'restored_finance_components')::integer, 0);
    v_carry_forward_created := v_carry_forward_created + COALESCE((v_chunk_result->>'carry_forward_created')::integer, 0);
    v_carry_forward_existing := v_carry_forward_existing + COALESCE((v_chunk_result->>'carry_forward_existing')::integer, 0);
    v_carry_forward_released := v_carry_forward_released + COALESCE((v_chunk_result->>'carry_forward_released')::integer, 0);
    v_complete := COALESCE((v_chunk_result->>'complete')::boolean, false);

    IF COALESCE((v_chunk_result->>'requires_user_action')::boolean, false)
       OR COALESCE((v_chunk_result#>>'{totals,blocked}')::integer, 0) > 0
       OR COALESCE((v_chunk_result#>>'{totals,failed_retryable}')::integer, 0) > 0
       OR COALESCE((v_chunk_result#>>'{totals,failed_final}')::integer, 0) > 0 THEN
      RETURN v_result || jsonb_build_object(
        'ok', false,
        'is_complete', v_complete,
        'error_code', 'PAYMENT_CANCEL_NOT_SENT_PROCESS_INCOMPLETE',
        'code', 'PAYMENT_CANCEL_NOT_SENT_PROCESS_INCOMPLETE',
        'message', 'The draft was not cancelled because one or more frozen payment items could not be safely released. Refresh Banking Pay and review the specific blocked item.',
        'user_message', 'The draft was not cancelled because one or more frozen payment items could not be safely released. Refresh Banking Pay and review the specific blocked item.',
        'process_result', v_chunk_result
      );
    END IF;
  END LOOP;

  IF v_complete IS NOT TRUE THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_COMPLETE_LIMIT_EXCEEDED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CANCEL_COMPLETE_LIMIT_EXCEEDED',
              'message', 'Cancellation could not finish within the safe processing limit, so no partial cancellation was committed.',
              'pay_batch_id', p_pay_batch_id::text,
              'correction_request_id', v_correction_request_id::text,
              'iterations', v_iteration
            )::text;
  END IF;

  SELECT jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'correction_request_id', v_correction_request_id::text,
    'change_kind', 'PRE_BANK_CANCEL',
    'changed_pay_batch_item_ids', COALESCE(jsonb_agg(DISTINCT item_row.pay_batch_item_id::text ORDER BY item_row.pay_batch_item_id::text) FILTER (WHERE item_row.pay_batch_item_id IS NOT NULL), '[]'::jsonb),
    'changed_pay_batch_candidate_ids', COALESCE(jsonb_agg(DISTINCT item_row.pay_batch_candidate_id::text ORDER BY item_row.pay_batch_candidate_id::text) FILTER (WHERE item_row.pay_batch_candidate_id IS NOT NULL), '[]'::jsonb),
    'changed_candidate_ids', COALESCE(jsonb_agg(DISTINCT item_row.candidate_id::text ORDER BY item_row.candidate_id::text) FILTER (WHERE item_row.candidate_id IS NOT NULL), '[]'::jsonb),
    'changed_transfer_ids', COALESCE(jsonb_agg(DISTINCT item_row.pay_bank_transfer_id::text ORDER BY item_row.pay_bank_transfer_id::text) FILTER (WHERE item_row.pay_bank_transfer_id IS NOT NULL), '[]'::jsonb),
    'changed_finance_case_ids', COALESCE(jsonb_agg(DISTINCT item_row.finance_case_id::text ORDER BY item_row.finance_case_id::text) FILTER (WHERE item_row.finance_case_id IS NOT NULL), '[]'::jsonb),
    'changed_finance_component_ids', COALESCE(jsonb_agg(DISTINCT item_row.finance_component_id::text ORDER BY item_row.finance_component_id::text) FILTER (WHERE item_row.finance_component_id IS NOT NULL), '[]'::jsonb),
    'changed_reservation_ids', COALESCE(jsonb_agg(DISTINCT item_row.reservation_id::text ORDER BY item_row.reservation_id::text) FILTER (WHERE item_row.reservation_id IS NOT NULL), '[]'::jsonb),
    'applied_correction_item_count', COUNT(*)::integer
  )
  INTO v_changed_scope_json
  FROM public.pay_payment_correction_items AS item_row
  WHERE item_row.correction_request_id = v_correction_request_id
    AND item_row.correction_item_kind = 'PRE_BANK_CANCEL'
    AND item_row.status = 'APPLIED';

  v_finalise_result := public.pay_payment_cancel_finalise_metadata_v1(
    p_pay_batch_id,
    v_correction_request_id,
    p_actor_user_id,
    p_reason
  );

  RETURN v_result || jsonb_build_object(
    'ok', true,
    'is_complete', true,
    'complete', true,
    'process_result', CASE WHEN v_iteration > 0 THEN v_chunk_result ELSE COALESCE(v_result->'process_result', '{}'::jsonb) END,
    'changed_scope_json', COALESCE(v_changed_scope_json, '{}'::jsonb),
    'released_reservation_count', v_released_reservations,
    'restored_finance_component_count', v_restored_finance_components,
    'carry_forward_created_count', v_carry_forward_created,
    'carry_forward_existing_count', v_carry_forward_existing,
    'carry_forward_released_count', v_carry_forward_released,
    'completion_chunk_count', v_iteration,
    'cancellation_finalisation', v_finalise_result,
    'source_session_preserved', true,
    'policy_x_checked', true
  );
END;
$function$;

ALTER FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid, jsonb, uuid, text, text, jsonb) OWNER TO postgres;
ALTER FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid, jsonb, uuid, text, text, jsonb) SET plpgsql_check.mode TO 'disabled';
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid, jsonb, uuid, text, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid, jsonb, uuid, text, text, jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid, jsonb, uuid, text, text, jsonb) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid, jsonb, uuid, text, text, jsonb) TO service_role;
