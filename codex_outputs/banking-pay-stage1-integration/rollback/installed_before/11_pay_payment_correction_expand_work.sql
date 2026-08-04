-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: 62366ecb6bdc1fd233662a76dccc0c0e

CREATE OR REPLACE FUNCTION public.pay_payment_correction_expand_work(p_correction_request_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_request public.pay_payment_correction_requests%rowtype;
  v_now timestamptz := now();
  v_work_kind text;
  v_plan_work_unit text;
  v_effective_work_unit text;
  v_existing_work_item_count integer := 0;
  v_inserted_work_item_count integer := 0;
  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_finance_case_count integer := 0;
  v_progress jsonb := '{}'::jsonb;
  v_before_json jsonb := '{}'::jsonb;
  v_after_json jsonb := '{}'::jsonb;
  v_selected_pay_batch_item_ids jsonb := '[]'::jsonb;
  v_selected_pay_bank_transfer_ids jsonb := '[]'::jsonb;
  v_selected_finance_case_ids jsonb := '[]'::jsonb;
  v_selected_finance_component_ids jsonb := '[]'::jsonb;
  v_selected_reservation_ids jsonb := '[]'::jsonb;
  v_plan_provider_failure_reason_group text := NULL::text;
  v_plan_provider_failure_reason_code text := NULL::text;
  v_plan_provider_failure_reason_label text := NULL::text;
  v_plan_source_bank_event_links jsonb := '{}'::jsonb;
  v_plan_alert_candidate_kind text := NULL::text;
  v_plan_alert_candidate_severity text := NULL::text;
  v_plan_live_status_signature text := NULL::text;
  v_request_scope_type text := NULL::text;
  v_total_active_batch_item_count integer := 0;
  v_preserve_whole_batch_work_item boolean := false;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_EXPAND_WORK_START',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'actor_user_id', p_actor_user_id
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'NO_CORRECTION_REQUEST_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  v_before_json := jsonb_build_object(
    'status', v_request.status,
    'correction_kind', v_request.correction_kind,
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'plan_hash', v_request.plan_hash
  );

  IF v_request.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED', 'REJECTED', 'CANCELLED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_TERMINAL_CANNOT_EXPAND_WORK'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_TERMINAL_CANNOT_EXPAND_WORK',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  IF v_request.status NOT IN ('AUTHORISED', 'EXPANDED', 'PROCESSING') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISED_FOR_EXPANSION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISED_FOR_EXPANSION',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  SELECT count(*)::integer
  INTO v_existing_work_item_count
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

  IF v_existing_work_item_count > 0 THEN
    IF v_request.status = 'AUTHORISED' THEN
      UPDATE public.pay_payment_correction_requests AS existing_work_request
      SET
        status = 'EXPANDED',
        updated_at_utc = v_now
      WHERE existing_work_request.id = p_correction_request_id
      RETURNING existing_work_request.*
      INTO v_request;
    END IF;

    SELECT jsonb_build_object(
      'total', count(*)::integer,
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
    )
    INTO v_progress
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_EXPAND_WORK_EXISTING',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'pay_batch_id', v_request.pay_batch_id,
        'status', v_request.status,
        'existing_work_item_count', v_existing_work_item_count,
        'progress', v_progress
      ),
      'pay_payment_correction',
      p_correction_request_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'already_expanded', true,
      'correction_request_id', p_correction_request_id,
      'pay_batch_id', v_request.pay_batch_id,
      'status', v_request.status,
      'work_kind', CASE v_request.correction_kind
        WHEN 'PRE_BANK_CANCEL' THEN 'PRE_BANK_CANCEL'
        WHEN 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND'
        WHEN 'MANUAL_EVIDENCE_NO_MONEY' THEN 'NO_MONEY_UNWIND'
        ELSE NULL
      END,
      'inserted_work_item_count', 0,
      'existing_work_item_count', v_existing_work_item_count,
      'progress', v_progress
    );
  END IF;

  v_work_kind := CASE v_request.correction_kind
    WHEN 'PRE_BANK_CANCEL' THEN 'PRE_BANK_CANCEL'
    WHEN 'NO_MONEY_UNWIND' THEN 'NO_MONEY_UNWIND'
    WHEN 'MANUAL_EVIDENCE_NO_MONEY' THEN 'NO_MONEY_UNWIND'
    ELSE NULL
  END;

  v_plan_provider_failure_reason_group := NULLIF(btrim(COALESCE(v_request.plan_json->>'provider_failure_reason_group', '')), '');
  v_plan_provider_failure_reason_code := NULLIF(btrim(COALESCE(v_request.plan_json->>'provider_failure_reason_code', '')), '');
  v_plan_provider_failure_reason_label := NULLIF(btrim(COALESCE(v_request.plan_json->>'provider_failure_reason_label', '')), '');
  v_plan_source_bank_event_links := COALESCE(v_request.plan_json->'source_bank_event_links', '{}'::jsonb);
  v_plan_alert_candidate_kind := NULLIF(btrim(COALESCE(v_request.plan_json->>'alert_candidate_kind', '')), '');
  v_plan_alert_candidate_severity := NULLIF(btrim(COALESCE(v_request.plan_json->>'alert_candidate_severity', '')), '');
  v_plan_live_status_signature := COALESCE(NULLIF(btrim(v_request.plan_json->>'live_status_signature'), ''), NULLIF(btrim(v_request.plan_json->>'status_update_signature'), ''));

  IF v_work_kind IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_KIND_NOT_RESOLVED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_KIND_NOT_RESOLVED',
              'correction_request_id', p_correction_request_id,
              'correction_kind', v_request.correction_kind
            )::text;
  END IF;

  v_plan_work_unit := upper(nullif(btrim(COALESCE(v_request.plan_json->'work_expansion_plan'->>'work_unit', '')), ''));
  v_request_scope_type := upper(nullif(btrim(COALESCE(v_request.selection_json->>'scope_type', '')), ''));

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_expand_selected;
  CREATE TEMP TABLE _tmp_payment_correction_expand_selected ON COMMIT DROP AS
  SELECT
    expand_selected.pay_batch_id,
    expand_selected.pay_batch_candidate_id,
    expand_selected.candidate_id,
    expand_selected.candidate_display_name,
    expand_selected.pay_batch_item_id,
    expand_selected.item_type,
    expand_selected.timesheet_id,
    expand_selected.pay_bank_transfer_id,
    expand_selected.transfer_group_key,
    expand_selected.pay_channel,
    expand_selected.payee_entity_kind,
    expand_selected.payee_entity_id,
    expand_selected.umbrella_id,
    expand_selected.finance_case_id,
    expand_selected.finance_component_id,
    expand_selected.reservation_id,
    expand_selected.amount_inc_vat
  FROM public._pay_payment_correction_selected_items(
    v_request.pay_batch_id,
    v_request.selection_json,
    false
  ) AS expand_selected;

  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_expand_selected (finance_case_id);

  SELECT
    count(*)::integer,
    count(DISTINCT expand_selected.pay_batch_candidate_id) FILTER (WHERE expand_selected.pay_batch_candidate_id IS NOT NULL)::integer,
    count(DISTINCT expand_selected.pay_bank_transfer_id) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT expand_selected.finance_case_id) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)::integer
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_finance_case_count
  FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected;

  SELECT
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_bank_transfer_id::text ORDER BY expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb),
    COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb)
  INTO
    v_selected_pay_batch_item_ids,
    v_selected_pay_bank_transfer_ids,
    v_selected_finance_case_ids,
    v_selected_finance_component_ids,
    v_selected_reservation_ids
  FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected;

  SELECT count(*)::integer
  INTO v_total_active_batch_item_count
  FROM public.pay_batch_items AS active_batch_items
  JOIN public.pay_batch_candidates AS active_batch_candidates
    ON active_batch_candidates.id = active_batch_items.pay_batch_candidate_id
  WHERE active_batch_candidates.pay_batch_id = v_request.pay_batch_id
    AND COALESCE(active_batch_items.is_voided, false) = false;

  v_preserve_whole_batch_work_item := (
    v_work_kind = 'PRE_BANK_CANCEL'
    AND v_plan_work_unit = 'BATCH'
    AND v_request_scope_type IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH')
    AND COALESCE(v_selected_item_count, 0) > 0
    AND v_selected_item_count = COALESCE(v_total_active_batch_item_count, 0)
  );

  IF v_selected_item_count <= 0 THEN
    RAISE EXCEPTION 'NO_SELECTED_PAYMENT_ITEMS_FOR_WORK_EXPANSION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'NO_SELECTED_PAYMENT_ITEMS_FOR_WORK_EXPANSION',
              'correction_request_id', p_correction_request_id,
              'pay_batch_id', v_request.pay_batch_id
            )::text;
  END IF;

  v_effective_work_unit := CASE
    WHEN v_plan_work_unit IN ('TRANSFER', 'CANDIDATE', 'CANDIDATE_TRANSFER', 'FINANCE_CASE', 'BATCH') THEN v_plan_work_unit
    WHEN v_work_kind = 'NO_MONEY_UNWIND' AND v_selected_transfer_count > 0 THEN 'TRANSFER'
    WHEN v_selected_finance_case_count > 0 THEN 'FINANCE_CASE'
    WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE'
    ELSE 'BATCH'
  END;

  IF v_effective_work_unit = 'TRANSFER' AND v_selected_transfer_count = 0 THEN
    v_effective_work_unit := CASE WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE' ELSE 'BATCH' END;
  END IF;

  IF v_effective_work_unit = 'FINANCE_CASE' AND v_selected_finance_case_count = 0 THEN
    v_effective_work_unit := CASE WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE' ELSE 'BATCH' END;
  END IF;

  IF v_effective_work_unit = 'CANDIDATE' AND v_selected_candidate_count = 0 THEN
    v_effective_work_unit := 'BATCH';
  END IF;

  IF v_effective_work_unit = 'CANDIDATE_TRANSFER'
     AND (v_selected_candidate_count = 0 OR v_selected_transfer_count = 0) THEN
    v_effective_work_unit := CASE
      WHEN v_selected_transfer_count > 0 THEN 'TRANSFER'
      WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE'
      ELSE 'BATCH'
    END;
  END IF;

  IF v_effective_work_unit = 'BATCH' THEN
    v_effective_work_unit := CASE
      WHEN v_selected_transfer_count > 0 THEN 'TRANSFER'
      WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE'
      ELSE 'CANDIDATE'
    END;
  END IF;

  IF v_preserve_whole_batch_work_item THEN
    v_effective_work_unit := 'BATCH';
  ELSIF v_work_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND') THEN
    v_effective_work_unit := CASE
      WHEN v_selected_transfer_count > 0 THEN 'TRANSFER'
      ELSE 'CANDIDATE_PAYEE'
    END;
  END IF;

  IF v_effective_work_unit = 'BATCH' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    VALUES (
      p_correction_request_id,
      v_request.pay_batch_id,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      v_work_kind,
      v_request.selection_json || jsonb_build_object(
        'work_unit', 'BATCH',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'pay_batch_item_ids', v_selected_pay_batch_item_ids,
        'expected_pay_batch_item_ids', v_selected_pay_batch_item_ids,
        'expected_item_count', v_selected_item_count,
        'pay_bank_transfer_ids', v_selected_pay_bank_transfer_ids,
        'finance_case_ids', v_selected_finance_case_ids,
        'finance_component_ids', v_selected_finance_component_ids,
        'reservation_ids', v_selected_reservation_ids,
        'provider_failure_reason_group', v_plan_provider_failure_reason_group,
        'provider_failure_reason_code', v_plan_provider_failure_reason_code,
        'provider_failure_reason_label', v_plan_provider_failure_reason_label,
        'provider_evidence_snapshot', COALESCE(v_request.plan_json->'provider_evidence', '{}'::jsonb),
        'source_bank_event_links', v_plan_source_bank_event_links,
        'source_bank_event_id', CASE WHEN v_request.source_bank_event_id IS NULL THEN NULL ELSE v_request.source_bank_event_id::text END,
        'provider_webhook_receipt_id', v_plan_source_bank_event_links->>'provider_webhook_receipt_id',
        'provider_event_key', v_plan_source_bank_event_links->>'provider_event_key',
        'provider_event_type', v_plan_source_bank_event_links->>'provider_event_type',
        'provider_transaction_id', v_plan_source_bank_event_links->>'provider_transaction_id',
        'provider_request_id', v_plan_source_bank_event_links->>'provider_request_id',
        'alert_candidate_kind', v_plan_alert_candidate_kind,
        'alert_candidate_severity', v_plan_alert_candidate_severity,
        'live_status_signature', v_plan_live_status_signature,
        'expected_signed_amount_summary', jsonb_build_object('amount_inc_vat', COALESCE(NULLIF(btrim(v_request.plan_json#>>'{amounts,amount_inc_vat}'), '')::numeric, 0)),
        'carry_forward_source_item_ids', COALESCE(v_request.plan_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
        'existing_carry_forward_target_ids', COALESCE(v_request.plan_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb)
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'work_unit', 'BATCH',
        'expected_pay_batch_item_ids', v_selected_pay_batch_item_ids,
        'pay_bank_transfer_ids', v_selected_pay_bank_transfer_ids,
        'finance_case_ids', v_selected_finance_case_ids,
        'finance_component_ids', v_selected_finance_component_ids,
        'reservation_ids', v_selected_reservation_ids
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'BATCH',
        'selected_item_count', v_selected_item_count,
        'expected_pay_batch_item_ids', v_selected_pay_batch_item_ids
      )
    )
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSIF v_effective_work_unit = 'TRANSFER' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      NULL::uuid,
      transfer_work.pay_bank_transfer_id,
      NULL::uuid,
      transfer_work.umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'TRANSFER',
        'pay_bank_transfer_ids', jsonb_build_array(transfer_work.pay_bank_transfer_id::text),
        'pay_batch_item_ids', transfer_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', transfer_work.pay_batch_item_ids,
        'expected_item_count', transfer_work.item_count,
        'work_unit', 'TRANSFER',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'finance_case_ids', transfer_work.finance_case_ids,
        'finance_component_ids', transfer_work.finance_component_ids,
        'reservation_ids', transfer_work.reservation_ids,
        'item_count', transfer_work.item_count,
        'candidate_count', transfer_work.candidate_count,
        'amount_inc_vat', transfer_work.amount_inc_vat,
        'expected_signed_amount_summary', jsonb_build_object('amount_inc_vat', transfer_work.amount_inc_vat),
        'lifecycle_state_at_expansion', COALESCE(v_request.plan_json->>'payment_lifecycle_state', v_request.plan_json->>'classification'),
        'provider_evidence_snapshot', COALESCE(v_request.plan_json->'provider_evidence', '{}'::jsonb),
        'provider_failure_reason_group', v_plan_provider_failure_reason_group,
        'provider_failure_reason_code', v_plan_provider_failure_reason_code,
        'provider_failure_reason_label', v_plan_provider_failure_reason_label,
        'source_bank_event_links', v_plan_source_bank_event_links,
        'source_bank_event_id', CASE WHEN v_request.source_bank_event_id IS NULL THEN NULL ELSE v_request.source_bank_event_id::text END,
        'provider_webhook_receipt_id', v_plan_source_bank_event_links->>'provider_webhook_receipt_id',
        'provider_event_key', v_plan_source_bank_event_links->>'provider_event_key',
        'provider_event_type', v_plan_source_bank_event_links->>'provider_event_type',
        'provider_transaction_id', v_plan_source_bank_event_links->>'provider_transaction_id',
        'provider_request_id', v_plan_source_bank_event_links->>'provider_request_id',
        'alert_candidate_kind', v_plan_alert_candidate_kind,
        'alert_candidate_severity', v_plan_alert_candidate_severity,
        'live_status_signature', v_plan_live_status_signature,
        'carry_forward_source_item_ids', COALESCE(v_request.plan_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
        'existing_carry_forward_target_ids', COALESCE(v_request.plan_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb)
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'TRANSFER',
        'pay_bank_transfer_ids', jsonb_build_array(transfer_work.pay_bank_transfer_id::text),
        'expected_pay_batch_item_ids', transfer_work.pay_batch_item_ids,
        'finance_case_ids', transfer_work.finance_case_ids,
        'finance_component_ids', transfer_work.finance_component_ids,
        'reservation_ids', transfer_work.reservation_ids,
        'work_unit', 'TRANSFER'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'TRANSFER',
        'item_count', transfer_work.item_count,
        'candidate_count', transfer_work.candidate_count,
        'amount_inc_vat', transfer_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.pay_bank_transfer_id,
        (array_agg(expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb) AS finance_case_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        count(DISTINCT expand_selected.candidate_id)::integer AS candidate_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.pay_bank_transfer_id IS NOT NULL
      GROUP BY expand_selected.pay_bank_transfer_id
    ) AS transfer_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;

    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      untransferred_work.pay_batch_candidate_id,
      NULL::uuid,
      untransferred_work.candidate_id,
      untransferred_work.umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(untransferred_work.pay_batch_candidate_id::text),
        'pay_channel', untransferred_work.pay_channel,
        'payee_entity_kind', untransferred_work.payee_entity_kind,
        'payee_entity_id', CASE WHEN untransferred_work.payee_entity_id IS NULL THEN NULL ELSE untransferred_work.payee_entity_id::text END,
        'umbrella_ids', untransferred_work.umbrella_ids,
        'pay_bank_transfer_ids', '[]'::jsonb,
        'pay_batch_item_ids', untransferred_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', untransferred_work.pay_batch_item_ids,
        'expected_item_count', untransferred_work.item_count,
        'work_unit', 'CANDIDATE_PAYEE_UNTRANSFERRED',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'finance_case_ids', untransferred_work.finance_case_ids,
        'finance_component_ids', untransferred_work.finance_component_ids,
        'reservation_ids', untransferred_work.reservation_ids,
        'item_count', untransferred_work.item_count,
        'candidate_count', 1,
        'amount_inc_vat', untransferred_work.amount_inc_vat,
        'expected_signed_amount_summary', jsonb_build_object('amount_inc_vat', untransferred_work.amount_inc_vat),
        'lifecycle_state_at_expansion', COALESCE(v_request.plan_json->>'payment_lifecycle_state', v_request.plan_json->>'classification'),
        'provider_evidence_snapshot', COALESCE(v_request.plan_json->'provider_evidence', '{}'::jsonb),
        'provider_failure_reason_group', v_plan_provider_failure_reason_group,
        'provider_failure_reason_code', v_plan_provider_failure_reason_code,
        'provider_failure_reason_label', v_plan_provider_failure_reason_label,
        'source_bank_event_links', v_plan_source_bank_event_links,
        'source_bank_event_id', CASE WHEN v_request.source_bank_event_id IS NULL THEN NULL ELSE v_request.source_bank_event_id::text END,
        'provider_webhook_receipt_id', v_plan_source_bank_event_links->>'provider_webhook_receipt_id',
        'provider_event_key', v_plan_source_bank_event_links->>'provider_event_key',
        'provider_event_type', v_plan_source_bank_event_links->>'provider_event_type',
        'provider_transaction_id', v_plan_source_bank_event_links->>'provider_transaction_id',
        'provider_request_id', v_plan_source_bank_event_links->>'provider_request_id',
        'alert_candidate_kind', v_plan_alert_candidate_kind,
        'alert_candidate_severity', v_plan_alert_candidate_severity,
        'live_status_signature', v_plan_live_status_signature,
        'carry_forward_source_item_ids', COALESCE(v_request.plan_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
        'existing_carry_forward_target_ids', COALESCE(v_request.plan_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb)
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_id', untransferred_work.pay_batch_candidate_id::text,
        'pay_channel', untransferred_work.pay_channel,
        'payee_entity_kind', untransferred_work.payee_entity_kind,
        'payee_entity_id', CASE WHEN untransferred_work.payee_entity_id IS NULL THEN NULL ELSE untransferred_work.payee_entity_id::text END,
        'umbrella_ids', untransferred_work.umbrella_ids,
        'expected_pay_batch_item_ids', untransferred_work.pay_batch_item_ids,
        'finance_case_ids', untransferred_work.finance_case_ids,
        'finance_component_ids', untransferred_work.finance_component_ids,
        'reservation_ids', untransferred_work.reservation_ids,
        'work_unit', 'CANDIDATE_PAYEE_UNTRANSFERRED'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'CANDIDATE_PAYEE_UNTRANSFERRED',
        'item_count', untransferred_work.item_count,
        'candidate_count', 1,
        'amount_inc_vat', untransferred_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.pay_batch_candidate_id,
        (array_agg(DISTINCT expand_selected.candidate_id ORDER BY expand_selected.candidate_id))[1] AS candidate_id,
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.pay_channel, '')), ''), 'UNKNOWN') AS pay_channel,
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.payee_entity_kind, '')), ''), 'UNKNOWN') AS payee_entity_kind,
        expand_selected.payee_entity_id,
        (array_agg(DISTINCT expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.umbrella_id::text ORDER BY expand_selected.umbrella_id::text) FILTER (WHERE expand_selected.umbrella_id IS NOT NULL)), '[]'::jsonb) AS umbrella_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb) AS finance_case_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.pay_bank_transfer_id IS NULL
        AND expand_selected.pay_batch_candidate_id IS NOT NULL
      GROUP BY
        expand_selected.pay_batch_candidate_id,
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.pay_channel, '')), ''), 'UNKNOWN'),
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.payee_entity_kind, '')), ''), 'UNKNOWN'),
        expand_selected.payee_entity_id
    ) AS untransferred_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSIF v_effective_work_unit = 'FINANCE_CASE' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      finance_work.primary_pay_batch_candidate_id,
      finance_work.primary_pay_bank_transfer_id,
      finance_work.primary_candidate_id,
      finance_work.primary_umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', finance_work.pay_batch_candidate_ids,
        'finance_case_ids', jsonb_build_array(finance_work.finance_case_id::text),
        'finance_component_ids', finance_work.finance_component_ids,
        'reservation_ids', finance_work.reservation_ids,
        'pay_bank_transfer_ids', finance_work.pay_bank_transfer_ids,
        'pay_batch_item_ids', finance_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', finance_work.pay_batch_item_ids,
        'expected_item_count', finance_work.item_count,
        'work_unit', 'FINANCE_CASE',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'item_count', finance_work.item_count,
        'candidate_count', finance_work.candidate_count,
        'amount_inc_vat', finance_work.amount_inc_vat,
        'expected_signed_amount_summary', jsonb_build_object('amount_inc_vat', finance_work.amount_inc_vat),
        'lifecycle_state_at_expansion', COALESCE(v_request.plan_json->>'payment_lifecycle_state', v_request.plan_json->>'classification'),
        'provider_evidence_snapshot', COALESCE(v_request.plan_json->'provider_evidence', '{}'::jsonb),
        'provider_failure_reason_group', v_plan_provider_failure_reason_group,
        'provider_failure_reason_code', v_plan_provider_failure_reason_code,
        'provider_failure_reason_label', v_plan_provider_failure_reason_label,
        'source_bank_event_links', v_plan_source_bank_event_links,
        'source_bank_event_id', CASE WHEN v_request.source_bank_event_id IS NULL THEN NULL ELSE v_request.source_bank_event_id::text END,
        'provider_webhook_receipt_id', v_plan_source_bank_event_links->>'provider_webhook_receipt_id',
        'provider_event_key', v_plan_source_bank_event_links->>'provider_event_key',
        'provider_event_type', v_plan_source_bank_event_links->>'provider_event_type',
        'provider_transaction_id', v_plan_source_bank_event_links->>'provider_transaction_id',
        'provider_request_id', v_plan_source_bank_event_links->>'provider_request_id',
        'alert_candidate_kind', v_plan_alert_candidate_kind,
        'alert_candidate_severity', v_plan_alert_candidate_severity,
        'live_status_signature', v_plan_live_status_signature,
        'carry_forward_source_item_ids', COALESCE(v_request.plan_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
        'existing_carry_forward_target_ids', COALESCE(v_request.plan_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb)
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'FINANCE_CASE',
        'finance_case_id', finance_work.finance_case_id::text,
        'expected_pay_batch_item_ids', finance_work.pay_batch_item_ids,
        'finance_component_ids', finance_work.finance_component_ids,
        'reservation_ids', finance_work.reservation_ids,
        'pay_bank_transfer_ids', finance_work.pay_bank_transfer_ids,
        'work_unit', 'FINANCE_CASE'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'FINANCE_CASE',
        'finance_case_id', finance_work.finance_case_id,
        'item_count', finance_work.item_count,
        'candidate_count', finance_work.candidate_count,
        'amount_inc_vat', finance_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.finance_case_id,
        (array_agg(DISTINCT expand_selected.pay_batch_candidate_id ORDER BY expand_selected.pay_batch_candidate_id))[1] AS primary_pay_batch_candidate_id,
        (array_agg(DISTINCT expand_selected.pay_bank_transfer_id ORDER BY expand_selected.pay_bank_transfer_id NULLS LAST))[1] AS primary_pay_bank_transfer_id,
        (array_agg(DISTINCT expand_selected.candidate_id ORDER BY expand_selected.candidate_id))[1] AS primary_candidate_id,
        (array_agg(DISTINCT expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS primary_umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_candidate_id::text ORDER BY expand_selected.pay_batch_candidate_id::text) FILTER (WHERE expand_selected.pay_batch_candidate_id IS NOT NULL)), '[]'::jsonb) AS pay_batch_candidate_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_bank_transfer_id::text ORDER BY expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)), '[]'::jsonb) AS pay_bank_transfer_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        count(DISTINCT expand_selected.candidate_id)::integer AS candidate_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.finance_case_id IS NOT NULL
      GROUP BY expand_selected.finance_case_id
    ) AS finance_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSIF v_effective_work_unit = 'CANDIDATE_TRANSFER' THEN
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      candidate_transfer_work.pay_batch_candidate_id,
      candidate_transfer_work.pay_bank_transfer_id,
      candidate_transfer_work.candidate_id,
      candidate_transfer_work.umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(candidate_transfer_work.pay_batch_candidate_id::text),
        'pay_bank_transfer_ids', jsonb_build_array(candidate_transfer_work.pay_bank_transfer_id::text),
        'pay_batch_item_ids', candidate_transfer_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', candidate_transfer_work.pay_batch_item_ids,
        'expected_item_count', candidate_transfer_work.item_count,
        'finance_case_ids', candidate_transfer_work.finance_case_ids,
        'finance_component_ids', candidate_transfer_work.finance_component_ids,
        'reservation_ids', candidate_transfer_work.reservation_ids,
        'work_unit', 'CANDIDATE_TRANSFER',
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'item_count', candidate_transfer_work.item_count,
        'amount_inc_vat', candidate_transfer_work.amount_inc_vat,
        'expected_signed_amount_summary', jsonb_build_object('amount_inc_vat', candidate_transfer_work.amount_inc_vat),
        'lifecycle_state_at_expansion', COALESCE(v_request.plan_json->>'payment_lifecycle_state', v_request.plan_json->>'classification'),
        'provider_evidence_snapshot', COALESCE(v_request.plan_json->'provider_evidence', '{}'::jsonb),
        'provider_failure_reason_group', v_plan_provider_failure_reason_group,
        'provider_failure_reason_code', v_plan_provider_failure_reason_code,
        'provider_failure_reason_label', v_plan_provider_failure_reason_label,
        'source_bank_event_links', v_plan_source_bank_event_links,
        'source_bank_event_id', CASE WHEN v_request.source_bank_event_id IS NULL THEN NULL ELSE v_request.source_bank_event_id::text END,
        'provider_webhook_receipt_id', v_plan_source_bank_event_links->>'provider_webhook_receipt_id',
        'provider_event_key', v_plan_source_bank_event_links->>'provider_event_key',
        'provider_event_type', v_plan_source_bank_event_links->>'provider_event_type',
        'provider_transaction_id', v_plan_source_bank_event_links->>'provider_transaction_id',
        'provider_request_id', v_plan_source_bank_event_links->>'provider_request_id',
        'alert_candidate_kind', v_plan_alert_candidate_kind,
        'alert_candidate_severity', v_plan_alert_candidate_severity,
        'live_status_signature', v_plan_live_status_signature,
        'carry_forward_source_item_ids', COALESCE(v_request.plan_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
        'existing_carry_forward_target_ids', COALESCE(v_request.plan_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb)
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'CANDIDATE_TRANSFER',
        'pay_batch_candidate_id', candidate_transfer_work.pay_batch_candidate_id::text,
        'pay_bank_transfer_id', candidate_transfer_work.pay_bank_transfer_id::text,
        'expected_pay_batch_item_ids', candidate_transfer_work.pay_batch_item_ids,
        'finance_case_ids', candidate_transfer_work.finance_case_ids,
        'finance_component_ids', candidate_transfer_work.finance_component_ids,
        'reservation_ids', candidate_transfer_work.reservation_ids,
        'work_unit', 'CANDIDATE_TRANSFER'
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', 'CANDIDATE_TRANSFER',
        'item_count', candidate_transfer_work.item_count,
        'amount_inc_vat', candidate_transfer_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.pay_batch_candidate_id,
        expand_selected.pay_bank_transfer_id,
        (array_agg(DISTINCT expand_selected.candidate_id ORDER BY expand_selected.candidate_id))[1] AS candidate_id,
        (array_agg(DISTINCT expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS umbrella_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb) AS finance_case_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.pay_batch_candidate_id IS NOT NULL
        AND expand_selected.pay_bank_transfer_id IS NOT NULL
      GROUP BY expand_selected.pay_batch_candidate_id, expand_selected.pay_bank_transfer_id
    ) AS candidate_transfer_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  ELSE
    INSERT INTO public.pay_payment_correction_work_items(
      correction_request_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      work_kind,
      selection_json,
      selection_hash,
      status,
      attempt_count,
      last_error,
      locked_at_utc,
      locked_by,
      created_at_utc,
      processed_at_utc,
      result_json
    )
    SELECT
      p_correction_request_id,
      v_request.pay_batch_id,
      candidate_work.pay_batch_candidate_id,
      candidate_work.primary_pay_bank_transfer_id,
      candidate_work.candidate_id,
      candidate_work.umbrella_id,
      v_work_kind,
      jsonb_build_object(
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(candidate_work.pay_batch_candidate_id::text),
        'pay_bank_transfer_ids', candidate_work.pay_bank_transfer_ids,
        'pay_channel', candidate_work.pay_channel,
        'payee_entity_kind', candidate_work.payee_entity_kind,
        'payee_entity_id', CASE WHEN candidate_work.payee_entity_id IS NULL THEN NULL ELSE candidate_work.payee_entity_id::text END,
        'umbrella_ids', candidate_work.umbrella_ids,
        'pay_batch_item_ids', candidate_work.pay_batch_item_ids,
        'expected_pay_batch_item_ids', candidate_work.pay_batch_item_ids,
        'expected_item_count', candidate_work.item_count,
        'finance_case_ids', candidate_work.finance_case_ids,
        'finance_component_ids', candidate_work.finance_component_ids,
        'reservation_ids', candidate_work.reservation_ids,
        'work_unit', CASE WHEN v_effective_work_unit = 'CANDIDATE_PAYEE' THEN 'CANDIDATE_PAYEE' ELSE 'CANDIDATE' END,
        'source_correction_request_id', p_correction_request_id::text,
        'expanded_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'item_count', candidate_work.item_count,
        'amount_inc_vat', candidate_work.amount_inc_vat,
        'expected_signed_amount_summary', jsonb_build_object('amount_inc_vat', candidate_work.amount_inc_vat),
        'lifecycle_state_at_expansion', COALESCE(v_request.plan_json->>'payment_lifecycle_state', v_request.plan_json->>'classification'),
        'provider_evidence_snapshot', COALESCE(v_request.plan_json->'provider_evidence', '{}'::jsonb),
        'provider_failure_reason_group', v_plan_provider_failure_reason_group,
        'provider_failure_reason_code', v_plan_provider_failure_reason_code,
        'provider_failure_reason_label', v_plan_provider_failure_reason_label,
        'source_bank_event_links', v_plan_source_bank_event_links,
        'source_bank_event_id', CASE WHEN v_request.source_bank_event_id IS NULL THEN NULL ELSE v_request.source_bank_event_id::text END,
        'provider_webhook_receipt_id', v_plan_source_bank_event_links->>'provider_webhook_receipt_id',
        'provider_event_key', v_plan_source_bank_event_links->>'provider_event_key',
        'provider_event_type', v_plan_source_bank_event_links->>'provider_event_type',
        'provider_transaction_id', v_plan_source_bank_event_links->>'provider_transaction_id',
        'provider_request_id', v_plan_source_bank_event_links->>'provider_request_id',
        'alert_candidate_kind', v_plan_alert_candidate_kind,
        'alert_candidate_severity', v_plan_alert_candidate_severity,
        'live_status_signature', v_plan_live_status_signature,
        'carry_forward_source_item_ids', COALESCE(v_request.plan_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
        'existing_carry_forward_target_ids', COALESCE(v_request.plan_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb)
      ),
      md5(jsonb_build_object(
        'correction_request_id', p_correction_request_id::text,
        'work_kind', v_work_kind,
        'scope_type', 'CANDIDATES',
        'pay_batch_candidate_ids', jsonb_build_array(candidate_work.pay_batch_candidate_id::text),
        'pay_bank_transfer_ids', candidate_work.pay_bank_transfer_ids,
        'pay_channel', candidate_work.pay_channel,
        'payee_entity_kind', candidate_work.payee_entity_kind,
        'payee_entity_id', CASE WHEN candidate_work.payee_entity_id IS NULL THEN NULL ELSE candidate_work.payee_entity_id::text END,
        'umbrella_ids', candidate_work.umbrella_ids,
        'expected_pay_batch_item_ids', candidate_work.pay_batch_item_ids,
        'finance_case_ids', candidate_work.finance_case_ids,
        'finance_component_ids', candidate_work.finance_component_ids,
        'reservation_ids', candidate_work.reservation_ids,
        'work_unit', CASE WHEN v_effective_work_unit = 'CANDIDATE_PAYEE' THEN 'CANDIDATE_PAYEE' ELSE 'CANDIDATE' END
      )::text),
      'PENDING',
      0,
      NULL::text,
      NULL::timestamptz,
      NULL::text,
      v_now,
      NULL::timestamptz,
      jsonb_build_object(
        'created_by', 'pay_payment_correction_expand_work',
        'work_unit', CASE WHEN v_effective_work_unit = 'CANDIDATE_PAYEE' THEN 'CANDIDATE_PAYEE' ELSE 'CANDIDATE' END,
        'item_count', candidate_work.item_count,
        'amount_inc_vat', candidate_work.amount_inc_vat
      )
    FROM (
      SELECT
        expand_selected.pay_batch_candidate_id,
        (array_agg(DISTINCT expand_selected.candidate_id ORDER BY expand_selected.candidate_id))[1] AS candidate_id,
        (array_agg(DISTINCT expand_selected.pay_bank_transfer_id ORDER BY expand_selected.pay_bank_transfer_id NULLS LAST))[1] AS primary_pay_bank_transfer_id,
        (array_agg(DISTINCT expand_selected.umbrella_id ORDER BY expand_selected.umbrella_id NULLS LAST))[1] AS umbrella_id,
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.pay_channel, '')), ''), 'UNKNOWN') AS pay_channel,
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.payee_entity_kind, '')), ''), 'UNKNOWN') AS payee_entity_kind,
        expand_selected.payee_entity_id,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.umbrella_id::text ORDER BY expand_selected.umbrella_id::text) FILTER (WHERE expand_selected.umbrella_id IS NOT NULL)), '[]'::jsonb) AS umbrella_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_bank_transfer_id::text ORDER BY expand_selected.pay_bank_transfer_id::text) FILTER (WHERE expand_selected.pay_bank_transfer_id IS NOT NULL)), '[]'::jsonb) AS pay_bank_transfer_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.pay_batch_item_id::text ORDER BY expand_selected.pay_batch_item_id::text)), '[]'::jsonb) AS pay_batch_item_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_case_id::text ORDER BY expand_selected.finance_case_id::text) FILTER (WHERE expand_selected.finance_case_id IS NOT NULL)), '[]'::jsonb) AS finance_case_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.finance_component_id::text ORDER BY expand_selected.finance_component_id::text) FILTER (WHERE expand_selected.finance_component_id IS NOT NULL)), '[]'::jsonb) AS finance_component_ids,
        COALESCE(to_jsonb(array_agg(DISTINCT expand_selected.reservation_id::text ORDER BY expand_selected.reservation_id::text) FILTER (WHERE expand_selected.reservation_id IS NOT NULL)), '[]'::jsonb) AS reservation_ids,
        count(*)::integer AS item_count,
        round(COALESCE(sum(COALESCE(expand_selected.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
      FROM pg_temp._tmp_payment_correction_expand_selected AS expand_selected
      WHERE expand_selected.pay_batch_candidate_id IS NOT NULL
      GROUP BY
        expand_selected.pay_batch_candidate_id,
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.pay_channel, '')), ''), 'UNKNOWN'),
        COALESCE(NULLIF(btrim(COALESCE(expand_selected.payee_entity_kind, '')), ''), 'UNKNOWN'),
        expand_selected.payee_entity_id
    ) AS candidate_work
    ON CONFLICT (correction_request_id, work_kind, selection_hash) DO NOTHING;
  END IF;

  SELECT count(*)::integer
  INTO v_inserted_work_item_count
  FROM public.pay_payment_correction_work_items AS inserted_work_items_count
  WHERE inserted_work_items_count.correction_request_id = p_correction_request_id;

  UPDATE public.pay_payment_correction_requests AS expanded_request
  SET
    status = 'EXPANDED',
    updated_at_utc = v_now
  WHERE expanded_request.id = p_correction_request_id
  RETURNING expanded_request.*
  INTO v_request;

  v_after_json := jsonb_build_object(
    'status', v_request.status,
    'correction_kind', v_request.correction_kind,
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'work_kind', v_work_kind,
    'work_unit', v_effective_work_unit,
    'inserted_work_item_count', v_inserted_work_item_count
  );

  INSERT INTO public.pay_payment_correction_actions(
    correction_request_id,
    pay_batch_id,
    actor_kind,
    actor_user_id,
    action,
    action_at_utc,
    note,
    before_json,
    after_json,
    metadata_json
  )
  VALUES (
    p_correction_request_id,
    v_request.pay_batch_id,
    CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
    p_actor_user_id,
    'EXPAND_WORK',
    v_now,
    NULL::text,
    v_before_json,
    v_after_json,
    jsonb_build_object(
      'work_kind', v_work_kind,
      'plan_work_unit', v_plan_work_unit,
      'effective_work_unit', v_effective_work_unit,
      'selected_item_count', v_selected_item_count,
      'selected_candidate_count', v_selected_candidate_count,
      'selected_transfer_count', v_selected_transfer_count,
      'selected_finance_case_count', v_selected_finance_case_count,
      'total_active_batch_item_count', v_total_active_batch_item_count,
      'preserve_whole_batch_work_item', v_preserve_whole_batch_work_item,
      'inserted_work_item_count', v_inserted_work_item_count
    )
  );

  SELECT jsonb_build_object(
    'total', count(*)::integer,
    'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
    'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
    'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
    'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
    'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
    'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
    'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
    'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
  )
  INTO v_progress
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_EXPAND_WORK_RESULT',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'pay_batch_id', v_request.pay_batch_id,
      'status', v_request.status,
      'work_kind', v_work_kind,
      'plan_work_unit', v_plan_work_unit,
      'effective_work_unit', v_effective_work_unit,
      'inserted_work_item_count', v_inserted_work_item_count,
      'progress', v_progress
    ),
    'pay_payment_correction',
    p_correction_request_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'already_expanded', false,
    'correction_request_id', p_correction_request_id,
    'pay_batch_id', v_request.pay_batch_id,
    'status', v_request.status,
    'work_kind', v_work_kind,
    'plan_work_unit', v_plan_work_unit,
    'effective_work_unit', v_effective_work_unit,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'selected_finance_case_count', v_selected_finance_case_count,
    'inserted_work_item_count', v_inserted_work_item_count,
    'progress', v_progress,
    'provider_failure_reason_group', v_plan_provider_failure_reason_group,
    'source_bank_event_links', v_plan_source_bank_event_links,
    'live_status_signature', v_plan_live_status_signature
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_EXPAND_WORK_ERROR',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_correction_request_id::text, 'NO_CORRECTION_REQUEST_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;

ALTER FUNCTION pay_payment_correction_expand_work(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_correction_expand_work(uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_correction_expand_work(uuid,uuid) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_correction_expand_work(uuid,uuid) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_payment_correction_expand_work(uuid,uuid) TO service_role;
