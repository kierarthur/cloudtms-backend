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
  v_batch public.pay_batches%rowtype;
  v_operation public.banking_pay_operations%rowtype;
  v_batch_id uuid;
  v_guard jsonb;
  v_last_ordinal bigint := 0;
  v_next_ordinal bigint := 0;
  v_page_count integer := 0;
  v_child_count integer := 0;
  v_work_count integer := 0;
  v_mismatch_count integer := 0;
  v_complete boolean := false;
  v_work_kind text;
  v_requested_action text;
  v_fast_draft_enabled boolean := false;
  v_fast_draft_result jsonb := '{}'::jsonb;
  v_fast_draft_after_candidate_id uuid := NULL::uuid;
BEGIN
  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;

  -- Resolve identifiers without taking row locks, then use the canonical
  -- mutation order: guard -> request -> batch -> operation.
  SELECT request_row.pay_batch_id
  INTO v_batch_id
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;

  v_guard := private.pay_payment_mutation_guard_v1(
    v_batch_id,
    p_correction_request_id,
    'CORRECTION_APPLY'
  );

  SELECT request_row.*
  INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND OR v_request.status NOT IN ('AUTHORISED', 'EXPANDED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_STATE_INVALID')::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_request.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_EXPAND_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND')::text;
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

  IF v_operation.pay_batch_id IS DISTINCT FROM v_request.pay_batch_id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_EXPAND_OPERATION_BATCH_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'OPERATION_MISMATCH')::text;
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

  SELECT COALESCE(settings_row.banking_pay_draft_overlay_fast_cancel_v1_enabled,false)
  INTO v_fast_draft_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id=1;

  IF COALESCE(v_fast_draft_enabled,false)
     AND UPPER(BTRIM(COALESCE(v_batch.status,''))) IN ('DRAFT','DRAFT_CREATED','CANCELLED')
     AND v_batch.source_workbench_session_id IS NOT NULL THEN
    v_fast_draft_after_candidate_id := CASE
      WHEN COALESCE(v_operation.progress_json->>'last_fast_draft_candidate_id','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (v_operation.progress_json->>'last_fast_draft_candidate_id')::uuid
      ELSE NULL::uuid END;

    v_fast_draft_result := private.pay_workbench_draft_overlay_remove_page_v1(
      p_correction_request_id,v_request.pay_batch_id,v_batch.source_workbench_session_id,
      v_fast_draft_after_candidate_id,100,500,p_actor_user_id,
      jsonb_build_object('requested_action',v_requested_action)
    );

    IF COALESCE((v_fast_draft_result->>'fast_route_eligible')::boolean,true) IS NOT TRUE THEN
      -- Admission occurs before any Draft mutation.  A rejected candidate page
      -- therefore falls through to the existing frozen financial correction
      -- route without partial voiding, reservation release or publication.
      UPDATE public.banking_pay_operations AS fallback_operation
      SET progress_json=COALESCE(fallback_operation.progress_json,'{}'::jsonb)
            || jsonb_build_object(
              'draft_overlay_fast_rejected',true,
              'draft_overlay_fast_rejection',v_fast_draft_result
            ),
          updated_at_utc=v_now
      WHERE fallback_operation.id=v_operation.id;
    ELSE
    UPDATE public.banking_pay_operations AS fast_operation
    SET progress_json=COALESCE(fast_operation.progress_json,'{}'::jsonb)
          || jsonb_build_object(
            'draft_overlay_fast',true,
            'last_fast_draft_candidate_id',v_fast_draft_result->>'last_candidate_id',
            'last_fast_draft_page',v_fast_draft_result
          ),
        phase=CASE WHEN COALESCE((v_fast_draft_result->>'has_more')::boolean,false)
          THEN 'EXPAND_WORK' ELSE 'COMPLETE' END,
        status=CASE WHEN COALESCE((v_fast_draft_result->>'has_more')::boolean,false)
          THEN 'RUNNING' ELSE 'COMPLETE' END,
        runner_state=CASE WHEN COALESCE((v_fast_draft_result->>'has_more')::boolean,false)
          THEN 'RUNNABLE' ELSE 'COMPLETE' END,
        run_after_utc=CASE WHEN COALESCE((v_fast_draft_result->>'has_more')::boolean,false)
          THEN v_now ELSE NULL END,
        completed_at_utc=CASE WHEN COALESCE((v_fast_draft_result->>'has_more')::boolean,false)
          THEN fast_operation.completed_at_utc ELSE v_now END,
        updated_at_utc=v_now
    WHERE fast_operation.id=v_operation.id;

    IF COALESCE((v_fast_draft_result->>'has_more')::boolean,false) IS NOT TRUE THEN
      UPDATE public.pay_payment_correction_requests AS fast_request
      SET status='APPLIED',applied_at_utc=COALESCE(fast_request.applied_at_utc,v_now),updated_at_utc=v_now
      WHERE fast_request.id=p_correction_request_id;

      INSERT INTO public.pay_payment_correction_actions(
        correction_request_id,pay_batch_id,actor_kind,actor_user_id,action,
        action_at_utc,note,before_json,after_json,metadata_json
      ) VALUES (
        p_correction_request_id,v_request.pay_batch_id,
        CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
        p_actor_user_id,'APPLY',v_now,'DRAFT_OVERLAY_FAST_COMPLETE',NULL,
        jsonb_build_object('status','APPLIED'),v_fast_draft_result
      );

      -- The fast Draft route intentionally creates no financial work items, so
      -- it never enters process_chunk's ordinary terminal alert branch.  Emit
      -- the same request-scoped success event here from frozen correction
      -- membership.  The unique request event key makes lost-response replay
      -- idempotent and prevents per-candidate alert fanout.
      INSERT INTO public.banking_alert_success_events (
        pay_batch_id,
        alert_kind,
        event_key,
        payload_json,
        occurred_at_utc,
        expires_at_utc,
        created_at_utc,
        updated_at_utc
      )
      SELECT
        v_request.pay_batch_id,
        'BATCH_CANCELLATION_SUCCESS',
        'CANCELLATION:' || p_correction_request_id::text,
        pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
          'contract_version', 'BANKING_ALERT_CANCELLATION_SUCCESS_V1',
          'correction_request_id', p_correction_request_id::text,
          'operation_id', v_operation.id::text,
          'pay_batch_id', v_request.pay_batch_id::text,
          'cancelled_payment_count', frozen_scope.payment_count,
          'cancelled_amount_pence', frozen_scope.amount_pence,
          'user_label', frozen_scope.payment_count::text || ' payment'
            || CASE WHEN frozen_scope.payment_count = 1 THEN '' ELSE 's' END
            || ' cancelled',
          'user_description', 'Cancellation completed for '
            || frozen_scope.payment_count::text || ' payment'
            || CASE WHEN frozen_scope.payment_count = 1 THEN '' ELSE 's' END
            || '. Banking Pay has been updated.',
          'required_user_action', 'Review or clear this Banking alert.',
          'stable_issue_key', v_request.pay_batch_id::text
            || ':BATCH_CANCELLATION_SUCCESS:' || p_correction_request_id::text,
          'dedupe_key', v_request.pay_batch_id::text
            || ':BATCH_CANCELLATION_SUCCESS:' || p_correction_request_id::text,
          'policy_x_source', 'FROZEN_CORRECTION_REQUEST_MEMBERSHIP'
        )),
        v_now,
        v_now + interval '365 days',
        v_now,
        v_now
      FROM (
        SELECT
          pg_catalog.count(*)::integer AS payment_count,
          pg_catalog.round(COALESCE(pg_catalog.sum(request_candidate.active_amount),0) * 100)::bigint AS amount_pence
        FROM public.pay_payment_correction_request_candidates AS request_candidate
        WHERE request_candidate.correction_request_id = p_correction_request_id
      ) AS frozen_scope
      WHERE frozen_scope.payment_count > 0
      ON CONFLICT (pay_batch_id, alert_kind, event_key) DO NOTHING;
    END IF;

    RETURN v_fast_draft_result || jsonb_build_object(
      'correction_request_id',p_correction_request_id,
      'operation_id',v_operation.id,
      'phase',CASE WHEN COALESCE((v_fast_draft_result->>'has_more')::boolean,false)
        THEN 'EXPAND_WORK' ELSE 'COMPLETE' END,
      'complete',COALESCE((v_fast_draft_result->>'has_more')::boolean,false) IS NOT TRUE,
      'processing_continues',COALESCE((v_fast_draft_result->>'has_more')::boolean,false),
      'code',CASE WHEN COALESCE((v_fast_draft_result->>'has_more')::boolean,false)
        THEN 'DRAFT_OVERLAY_FAST_PAGE' ELSE 'DRAFT_OVERLAY_FAST_COMPLETE' END
    );
    END IF;
  END IF;

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
