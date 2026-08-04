-- Exact installed TEST rollback definition and ACL captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: 1b25a2ef5e8a21997d3e707b5e205849

CREATE OR REPLACE FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(p_pay_batch_id uuid, p_selection_json jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_reason text DEFAULT NULL::text, p_idempotency_key text DEFAULT NULL::text, p_confirmation_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
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

ALTER FUNCTION pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) TO service_role;
