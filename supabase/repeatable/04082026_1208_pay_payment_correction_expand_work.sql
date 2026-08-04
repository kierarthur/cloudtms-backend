-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Exact installed identity retained. Expansion reads immutable child membership only.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_expand_work(
  p_correction_request_id uuid,
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
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_request public.pay_payment_correction_requests%rowtype;
  v_operation public.banking_pay_operations%rowtype;
  v_last_ordinal bigint := 0;
  v_next_ordinal bigint := 0;
  v_page_count integer := 0;
  v_child_count integer := 0;
  v_work_count integer := 0;
  v_mismatch_count integer := 0;
  v_complete boolean := false;
  v_work_kind text;
  v_requested_action text;
BEGIN
  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND operation_row.input_json->>'correction_request_id' = p_correction_request_id::text
  ORDER BY operation_row.created_at_utc
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND OR v_operation.phase IS DISTINCT FROM 'EXPAND_WORK' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_EXPAND_OPERATION_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'OPERATION_MISMATCH')::text;
  END IF;

  SELECT request_row.*
  INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND OR v_request.status NOT IN ('AUTHORISED', 'EXPANDED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_STATE_INVALID')::text;
  END IF;

  IF p_actor_user_id IS NOT NULL
     AND p_actor_user_id IS DISTINCT FROM v_request.requested_by_user_id
     AND coalesce(v_request.auto_requested, false) IS NOT TRUE THEN
    IF NOT EXISTS (
      SELECT 1 FROM public.tms_users AS actor_row
      WHERE actor_row.id = p_actor_user_id AND coalesce(actor_row.is_active, false)
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_EXPAND_ACTOR_NOT_ALLOWED'
        USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object('code', 'PERMISSION_DENIED')::text;
    END IF;
  END IF;

  v_last_ordinal := coalesce(
    NULLIF(v_operation.progress_json->>'last_expanded_selection_ordinal', '')::bigint,
    0
  );
  v_work_kind := CASE WHEN v_request.correction_kind = 'NO_MONEY_UNWIND'
                      THEN 'NO_MONEY_UNWIND' ELSE 'PRE_BANK_CANCEL' END;
  v_requested_action := coalesce(
    v_request.plan_json->>'requested_action', v_request.selection_json->>'requested_action'
  );

  WITH page AS (
    SELECT member_row.*
    FROM public.pay_payment_correction_request_candidates AS member_row
    WHERE member_row.correction_request_id = p_correction_request_id
      AND member_row.selection_ordinal > v_last_ordinal
    ORDER BY member_row.selection_ordinal
    LIMIT 100
  ), inserted AS (
    INSERT INTO public.pay_payment_correction_work_items (
      correction_request_id, pay_batch_id, pay_batch_candidate_id,
      pay_bank_transfer_id, candidate_id, umbrella_id, work_kind,
      selection_json, selection_hash, status, attempt_count, last_error,
      locked_at_utc, locked_by, created_at_utc, processed_at_utc, result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      page.pay_batch_candidate_id,
      (
        SELECT item_row.pay_bank_transfer_id
        FROM public.pay_batch_items AS item_row
        WHERE item_row.id = ANY(page.pay_batch_item_ids)
          AND item_row.pay_bank_transfer_id IS NOT NULL
        ORDER BY item_row.pay_bank_transfer_id
        LIMIT 1
      ),
      candidate_row.candidate_id,
      (
        SELECT item_row.umbrella_id
        FROM public.pay_batch_items AS item_row
        WHERE item_row.id = ANY(page.pay_batch_item_ids)
          AND item_row.umbrella_id IS NOT NULL
        ORDER BY item_row.umbrella_id
        LIMIT 1
      ),
      v_work_kind,
      pg_catalog.jsonb_build_object(
        'contract_version', 1,
        'scope_type', 'CANDIDATES',
        'work_unit', 'CANDIDATE',
        'requested_action', v_requested_action,
        'selection_ordinal', page.selection_ordinal,
        'pay_batch_candidate_ids', pg_catalog.jsonb_build_array(page.pay_batch_candidate_id),
        'pay_batch_item_ids', pg_catalog.to_jsonb(page.pay_batch_item_ids),
        'expected_pay_batch_item_ids', pg_catalog.to_jsonb(page.pay_batch_item_ids),
        'expected_item_count', page.active_item_count,
        'source_row_count', page.source_row_count,
        'amount_inc_vat', page.active_amount,
        'shared_instruction_scope_hash', page.shared_instruction_scope_hash,
        'eligibility_code_at_plan', page.eligibility_code_at_plan,
        'source_correction_request_id', p_correction_request_id
      ),
      page.candidate_scope_hash,
      'PENDING', 0, NULL, NULL, NULL, v_now, NULL,
      pg_catalog.jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'selection_ordinal', page.selection_ordinal,
        'candidate_scope_hash', page.candidate_scope_hash
      )
    FROM page
    JOIN public.pay_batch_candidates AS candidate_row
      ON candidate_row.id = page.pay_batch_candidate_id
     AND candidate_row.pay_batch_id = v_request.pay_batch_id
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING
    RETURNING id
  )
  SELECT pg_catalog.count(*)::integer INTO v_page_count FROM inserted;

  SELECT coalesce(pg_catalog.max(member_row.selection_ordinal), v_last_ordinal)
  INTO v_next_ordinal
  FROM public.pay_payment_correction_request_candidates AS member_row
  WHERE member_row.correction_request_id = p_correction_request_id
    AND member_row.selection_ordinal > v_last_ordinal
    AND member_row.selection_ordinal <= v_last_ordinal + 100;

  v_next_ordinal := greatest(v_next_ordinal, v_last_ordinal);
  v_complete := NOT EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_request_candidates AS remaining_member
    WHERE remaining_member.correction_request_id = p_correction_request_id
      AND remaining_member.selection_ordinal > v_next_ordinal
  );

  UPDATE public.banking_pay_operations AS progress_operation
  SET progress_json = coalesce(progress_operation.progress_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'last_expanded_selection_ordinal', v_next_ordinal,
          'expanded_page_count', v_page_count
        ),
      updated_at_utc = v_now
  WHERE progress_operation.id = v_operation.id;

  IF v_complete THEN
    SELECT pg_catalog.count(*)::integer INTO v_child_count
    FROM public.pay_payment_correction_request_candidates AS member_row
    WHERE member_row.correction_request_id = p_correction_request_id;

    SELECT pg_catalog.count(*)::integer INTO v_work_count
    FROM public.pay_payment_correction_work_items AS work_row
    WHERE work_row.correction_request_id = p_correction_request_id;

    SELECT pg_catalog.count(*)::integer INTO v_mismatch_count
    FROM (
      (
        SELECT member_row.pay_batch_candidate_id, member_row.candidate_scope_hash
        FROM public.pay_payment_correction_request_candidates AS member_row
        WHERE member_row.correction_request_id = p_correction_request_id
        EXCEPT
        SELECT work_row.pay_batch_candidate_id, work_row.selection_hash
        FROM public.pay_payment_correction_work_items AS work_row
        WHERE work_row.correction_request_id = p_correction_request_id
      )
      UNION ALL
      (
        SELECT work_row.pay_batch_candidate_id, work_row.selection_hash
        FROM public.pay_payment_correction_work_items AS work_row
        WHERE work_row.correction_request_id = p_correction_request_id
        EXCEPT
        SELECT member_row.pay_batch_candidate_id, member_row.candidate_scope_hash
        FROM public.pay_payment_correction_request_candidates AS member_row
        WHERE member_row.correction_request_id = p_correction_request_id
      )
    ) AS membership_difference;

    IF v_child_count < 1 OR v_child_count <> v_work_count OR v_mismatch_count <> 0 THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_MEMBERSHIP_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'WORK_MEMBERSHIP_MISMATCH',
          'child_count', v_child_count, 'work_count', v_work_count,
          'mismatch_count', v_mismatch_count
        )::text;
    END IF;

    UPDATE public.pay_payment_correction_requests AS expanded_request
    SET status = 'EXPANDED', updated_at_utc = v_now
    WHERE expanded_request.id = p_correction_request_id;

    UPDATE public.banking_pay_operations AS expanded_operation
    SET phase = 'PROCESS_CHUNKS', status = 'RUNNING', runner_state = 'RUNNABLE',
        run_after_utc = v_now, requires_user_action = false,
        progress_json = coalesce(expanded_operation.progress_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object('materialised_work_count', v_work_count),
        updated_at_utc = v_now
    WHERE expanded_operation.id = v_operation.id;

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id, pay_batch_id, actor_kind, actor_user_id, action,
      action_at_utc, note, before_json, after_json, metadata_json
    ) VALUES (
      p_correction_request_id, v_request.pay_batch_id,
      CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
      p_actor_user_id, 'EXPAND_WORK', v_now, NULL, NULL,
      pg_catalog.jsonb_build_object('status', 'EXPANDED'),
      pg_catalog.jsonb_build_object('work_count', v_work_count, 'membership_reconciled', true)
    );
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true, 'correction_request_id', p_correction_request_id,
    'operation_id', v_operation.id, 'page_work_count', v_page_count,
    'inserted_count', v_page_count,
    'existing_count', greatest(
      (v_next_ordinal - v_last_ordinal)::integer - v_page_count,
      0
    ),
    'blocked_count', 0,
    'next_selection_ordinal', v_next_ordinal,
    'has_more', NOT v_complete,
    'deprecation_code', NULL,
    'last_selection_ordinal', v_next_ordinal, 'complete', v_complete,
    'phase', CASE WHEN v_complete THEN 'PROCESS_CHUNKS' ELSE 'EXPAND_WORK' END,
    'code', CASE WHEN v_complete THEN 'PAYMENT_CORRECTION_WORK_EXPANDED' ELSE 'PAYMENT_CORRECTION_WORK_PAGE_EXPANDED' END
  );
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) TO service_role;
