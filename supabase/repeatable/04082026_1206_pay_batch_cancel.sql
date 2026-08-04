-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Exact installed identity retained; the legacy synchronous completion path is retired.

CREATE OR REPLACE FUNCTION public.pay_batch_cancel(
  p_pay_batch_id uuid,
  p_actor_user_id uuid,
  p_reason text,
  p_correction_request_id uuid DEFAULT NULL::uuid,
  p_work_item_id uuid DEFAULT NULL::uuid
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
  v_batch public.pay_batches%rowtype;
  v_result jsonb := '{}'::jsonb;
  v_active_candidate_count integer := 0;
  v_active_item_count integer := 0;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_CANCEL_PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANCEL_PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_CANCEL_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANCEL_ACTOR_REQUIRED')::text;
  END IF;

  IF p_work_item_id IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_CANCEL_LEGACY_WORK_ITEM_CALL_RETIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANCEL_LEGACY_WORK_ITEM_CALL_RETIRED')::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_CANCEL_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANCEL_BATCH_NOT_FOUND')::text;
  END IF;

  IF upper(btrim(COALESCE(v_batch.status, ''))) <> 'DRAFT' THEN
    RAISE EXCEPTION 'PAY_BATCH_CANCEL_BATCH_NOT_DRAFT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANCEL_BATCH_NOT_DRAFT')::text;
  END IF;

  SELECT
    count(DISTINCT candidates.id)::integer,
    count(items.id)::integer
  INTO v_active_candidate_count, v_active_item_count
  FROM public.pay_batch_candidates AS candidates
  JOIN public.pay_batch_items AS items
    ON items.pay_batch_candidate_id = candidates.id
   AND COALESCE(items.is_voided, false) = false
  WHERE candidates.pay_batch_id = p_pay_batch_id;

  IF v_active_candidate_count = 0 OR v_active_item_count = 0 THEN
    RAISE EXCEPTION 'PAY_BATCH_CANCEL_NO_ACTIVE_PAYMENTS'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANCEL_NO_ACTIVE_PAYMENTS')::text;
  END IF;

  v_result := public.pay_payment_correction_request_start(
    p_pay_batch_id => p_pay_batch_id,
    p_selection_json => jsonb_build_object(
      'command', 'PREPARE',
      'contract_version', 1,
      'mode', 'ALL_MATCHING',
      'requested_action', 'DRAFT_CANCEL',
      'filter', '{}'::jsonb,
      'exclusions', '[]'::jsonb,
      'correction_request_id', p_correction_request_id,
      'source_context', 'pay_batch_cancel'
    ),
    p_reason => 'DRAFT_PAYMENT_CANCELLED_BY_USER',
    p_actor_user_id => p_actor_user_id,
    p_source_bank_event_id => NULL::uuid,
    p_auto_requested => false,
    p_accepted_resolution_json => NULL::jsonb
  );

  RETURN COALESCE(v_result, '{}'::jsonb) || jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id,
    'correction_request_id', NULLIF(v_result->>'correction_request_id', '')::uuid,
    'operation_id', NULLIF(v_result->>'operation_id', '')::uuid,
    'request_status', COALESCE(v_result->>'request_status', 'PLANNING'),
    'operation_status', COALESCE(v_result->>'operation_status', 'RUNNING'),
    'phase', COALESCE(v_result->>'phase', 'PREPARE_SELECTION'),
    'status_url', '/api/banking/pay/correction/' || COALESCE(v_result->>'correction_request_id', '') || '/status',
    'display_message', 'Draft cancellation is being prepared.'
  );
END;
$function$;

ALTER FUNCTION public.pay_batch_cancel(uuid,uuid,text,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_cancel(uuid,uuid,text,uuid,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batch_cancel(uuid,uuid,text,uuid,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batch_cancel(uuid,uuid,text,uuid,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batch_cancel(uuid,uuid,text,uuid,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_cancel(uuid,uuid,text,uuid,uuid) TO service_role;
