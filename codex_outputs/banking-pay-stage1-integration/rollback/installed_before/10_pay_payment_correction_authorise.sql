-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: 40592a3654f77b521611ae711a960c67

CREATE OR REPLACE FUNCTION public.pay_payment_correction_authorise(p_correction_request_id uuid, p_actor_user_id uuid, p_action text, p_note text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_request public.pay_payment_correction_requests%rowtype;
  v_before_request jsonb;
  v_after_request jsonb;
  v_action text;
  v_actor_exists boolean := false;
  v_actor_active boolean := false;
  v_actor_payment_authoriser boolean := false;
  v_actor_payment_golden_key boolean := false;
  v_duplicate_authorisation boolean := false;
  v_new_approved_count integer := 0;
  v_fresh_plan jsonb := '{}'::jsonb;
  v_fresh_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_fresh_plan_can_apply boolean := false;
  v_fresh_hard_blockers jsonb := '[]'::jsonb;
  v_effective_hard_blockers jsonb := '[]'::jsonb;
  v_suggested_resolution_required boolean := false;
  v_accepted_resolution_supplied boolean := false;
  v_accepted_resolution_is_stale boolean := false;
  v_fresh_plan_material_json jsonb;
  v_fresh_plan_hash text;
  v_block_reason text;
  v_expand_result jsonb := NULL::jsonb;
  v_work_item_counts jsonb := '{}'::jsonb;
  v_plan_changed_fields jsonb := '[]'::jsonb;
  v_plan_stale_detail jsonb := '{}'::jsonb;
  v_stored_effective_blocker_codes text := '';
  v_fresh_effective_blocker_codes text := '';
  v_finance_resolution_validation jsonb := NULL::jsonb;
  v_finance_resolution_blocker jsonb := NULL::jsonb;
  v_immediate_process_result jsonb := NULL::jsonb;
  v_total_work_item_count integer := 0;
  v_pending_work_item_count integer := 0;
BEGIN
  v_action := upper(nullif(btrim(COALESCE(p_action, '')), ''));

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_AUTHORISE_BEGIN',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'actor_user_id', p_actor_user_id,
      'action', v_action
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'NO_CORRECTION_REQUEST_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'CORRECTION_REQUEST_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'ACTOR_USER_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_USER_ID_REQUIRED',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  IF v_action NOT IN ('AUTHORISE', 'USE_GOLDEN_KEY', 'REJECT', 'CANCEL') THEN
    RAISE EXCEPTION 'UNSUPPORTED_PAYMENT_CORRECTION_AUTHORISE_ACTION'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_PAYMENT_CORRECTION_AUTHORISE_ACTION',
              'correction_request_id', p_correction_request_id,
              'action', p_action,
              'supported_actions', jsonb_build_array('AUTHORISE', 'USE_GOLDEN_KEY', 'REJECT', 'CANCEL')
            )::text;
  END IF;

  SELECT
    true,
    COALESCE(public.tms_users.is_active, false),
    COALESCE(public.tms_users.payment_authoriser, false),
    COALESCE(public.tms_users.payment_golden_key, false)
  INTO
    v_actor_exists,
    v_actor_active,
    v_actor_payment_authoriser,
    v_actor_payment_golden_key
  FROM public.tms_users
  WHERE public.tms_users.id = p_actor_user_id;

  IF NOT COALESCE(v_actor_exists, false) THEN
    RAISE EXCEPTION 'ACTOR_USER_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_USER_NOT_FOUND',
              'actor_user_id', p_actor_user_id
            )::text;
  END IF;

  IF NOT COALESCE(v_actor_active, false) THEN
    RAISE EXCEPTION 'ACTOR_USER_INACTIVE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_USER_INACTIVE',
              'actor_user_id', p_actor_user_id
            )::text;
  END IF;

  IF v_action = 'AUTHORISE' AND NOT COALESCE(v_actor_payment_authoriser, false) THEN
    RAISE EXCEPTION 'ACTOR_IS_NOT_PAYMENT_AUTHORISER'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_IS_NOT_PAYMENT_AUTHORISER',
              'actor_user_id', p_actor_user_id,
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  IF v_action = 'USE_GOLDEN_KEY' AND NOT COALESCE(v_actor_payment_golden_key, false) THEN
    RAISE EXCEPTION 'ACTOR_DOES_NOT_HAVE_PAYMENT_GOLDEN_KEY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACTOR_DOES_NOT_HAVE_PAYMENT_GOLDEN_KEY',
              'actor_user_id', p_actor_user_id,
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id
  FOR UPDATE;

  IF v_request.id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  IF v_request.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED', 'REJECTED', 'CANCELLED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ALREADY_TERMINAL'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_ALREADY_TERMINAL',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  v_before_request := to_jsonb(v_request);

  IF v_action = 'REJECT' THEN
    UPDATE public.pay_payment_correction_requests
    SET
      status = 'REJECTED',
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
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
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'REJECT',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object('previous_status', v_before_request->>'status', 'new_status', v_request.status)
    );

    RETURN jsonb_build_object(
      'ok', true,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'status', v_request.status,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expanded', false,
      'progress', '{}'::jsonb
    );
  END IF;

  IF v_action = 'CANCEL' THEN
    UPDATE public.pay_payment_correction_work_items
    SET
      status = 'CANCELLED',
      locked_at_utc = NULL,
      locked_by = NULL,
      result_json = COALESCE(public.pay_payment_correction_work_items.result_json, '{}'::jsonb) || jsonb_build_object(
        'cancelled_by_user_id', p_actor_user_id,
        'cancelled_at_utc', now(),
        'cancel_note', NULLIF(btrim(COALESCE(p_note, '')), '')
      )
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id
      AND public.pay_payment_correction_work_items.status = 'PENDING';

    UPDATE public.pay_payment_correction_requests
    SET
      status = 'CANCELLED',
      cancelled_at_utc = COALESCE(public.pay_payment_correction_requests.cancelled_at_utc, now()),
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
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
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'CANCEL',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object('previous_status', v_before_request->>'status', 'new_status', v_request.status)
    );

    RETURN jsonb_build_object(
      'ok', true,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'status', v_request.status,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expanded', false,
      'progress', '{}'::jsonb
    );
  END IF;

  IF v_request.status NOT IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'BLOCKED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISABLE_IN_CURRENT_STATUS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_AUTHORISABLE_IN_CURRENT_STATUS',
              'correction_request_id', p_correction_request_id,
              'status', v_request.status
            )::text;
  END IF;

  IF v_action = 'AUTHORISE' THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_actions AS existing_action
      WHERE existing_action.correction_request_id = p_correction_request_id
        AND existing_action.actor_user_id = p_actor_user_id
        AND existing_action.action = 'AUTHORISE'
    )
    INTO v_duplicate_authorisation;

    IF COALESCE(v_duplicate_authorisation, false) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_DUPLICATE_AUTHORISATION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_DUPLICATE_AUTHORISATION',
                'correction_request_id', p_correction_request_id,
                'actor_user_id', p_actor_user_id
              )::text;
    END IF;

    v_new_approved_count := LEAST(COALESCE(v_request.approved_count, 0) + 1, GREATEST(COALESCE(v_request.required_quantity, 1), 1));

    UPDATE public.pay_payment_correction_requests
    SET
      approved_count = v_new_approved_count,
      status = CASE
        WHEN v_new_approved_count >= GREATEST(COALESCE(public.pay_payment_correction_requests.required_quantity, 1), 1) THEN 'AUTHORISED'
        ELSE 'AWAITING_AUTHORISATION'
      END,
      authorised_at_utc = CASE
        WHEN v_new_approved_count >= GREATEST(COALESCE(public.pay_payment_correction_requests.required_quantity, 1), 1)
          THEN COALESCE(public.pay_payment_correction_requests.authorised_at_utc, now())
        ELSE public.pay_payment_correction_requests.authorised_at_utc
      END,
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
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
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'AUTHORISE',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object(
        'approved_count_before', COALESCE((v_before_request->>'approved_count')::integer, 0),
        'approved_count_after', v_request.approved_count,
        'required_quantity', v_request.required_quantity
      )
    );

    IF v_request.approved_count < GREATEST(COALESCE(v_request.required_quantity, 1), 1) THEN
      RETURN jsonb_build_object(
        'ok', true,
        'correction_request_id', v_request.id,
        'pay_batch_id', v_request.pay_batch_id,
        'action', v_action,
        'status', v_request.status,
        'approved_count', v_request.approved_count,
        'required_quantity', v_request.required_quantity,
        'authorised', false,
        'expanded', false,
        'progress', '{}'::jsonb
      );
    END IF;
  END IF;

  IF v_action = 'USE_GOLDEN_KEY' THEN
    UPDATE public.pay_payment_correction_requests
    SET
      golden_key_used = true,
      golden_key_user_id = p_actor_user_id,
      approved_count = GREATEST(COALESCE(public.pay_payment_correction_requests.required_quantity, 1), 1),
      status = 'AUTHORISED',
      authorised_at_utc = COALESCE(public.pay_payment_correction_requests.authorised_at_utc, now()),
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
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
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'USE_GOLDEN_KEY',
      now(),
      NULLIF(btrim(COALESCE(p_note, '')), ''),
      v_before_request,
      v_after_request,
      jsonb_build_object(
        'required_quantity', v_request.required_quantity,
        'golden_key_user_id', p_actor_user_id
      )
    );
  END IF;

  v_fresh_plan := public.pay_payment_correction_plan(
    v_request.pay_batch_id,
    v_request.selection_json,
    p_actor_user_id
  );

  v_fresh_classification := COALESCE(v_fresh_plan->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');
  v_fresh_plan_can_apply := COALESCE((v_fresh_plan->>'can_apply')::boolean, false);
  v_fresh_hard_blockers := COALESCE(v_fresh_plan->'hard_blockers', '[]'::jsonb);
  v_suggested_resolution_required := COALESCE((v_fresh_plan->>'suggested_resolution_required')::boolean, false);
  v_accepted_resolution_supplied := v_request.accepted_resolution_json IS NOT NULL;

  SELECT COALESCE(jsonb_agg(blocker_elements.blocker_value ORDER BY blocker_elements.blocker_ordinal), '[]'::jsonb)
  INTO v_effective_hard_blockers
  FROM jsonb_array_elements(v_fresh_hard_blockers) WITH ORDINALITY AS blocker_elements(blocker_value, blocker_ordinal)
  WHERE NOT (
    v_suggested_resolution_required
    AND v_accepted_resolution_supplied
    AND COALESCE(blocker_elements.blocker_value->>'code', '') = 'SUGGESTED_RESOLUTION_REQUIRED'
  );

  v_accepted_resolution_is_stale := v_request.accepted_resolution_json IS NOT NULL
    AND (
      lower(COALESCE(v_request.accepted_resolution_json->>'is_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json->>'stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json->>'resolution_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json#>>'{validation,is_stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(v_request.accepted_resolution_json#>>'{validation,stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
    );

  IF v_accepted_resolution_is_stale THEN
    v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_STALE',
      'message', 'The accepted suggested finance resolution is stale and must be regenerated before correction apply.'
    ));
  END IF;

  IF v_suggested_resolution_required AND v_accepted_resolution_supplied THEN
    v_finance_resolution_validation := public._pay_payment_correction_validate_accepted_finance_resolution(
      p_pay_batch_id => v_request.pay_batch_id,
      p_selection_json => v_request.selection_json,
      p_plan_json => v_fresh_plan,
      p_accepted_resolution_json => v_request.accepted_resolution_json,
      p_actor_user_id => p_actor_user_id
    );

    IF NOT COALESCE((v_finance_resolution_validation->>'ok')::boolean, false)
       OR NOT COALESCE((v_finance_resolution_validation->>'validated')::boolean, false) THEN
      v_finance_resolution_blocker := COALESCE(
        v_finance_resolution_validation->'blocker',
        jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_VALIDATION_FAILED',
          'message', 'Accepted finance resolution validation failed before correction authorisation.'
        )
      );

      v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(v_finance_resolution_blocker);
    END IF;
  END IF;

  SELECT COALESCE(string_agg(stored_blocker_codes.blocker_code, '|' ORDER BY stored_blocker_codes.blocker_code), '')
  INTO v_stored_effective_blocker_codes
  FROM (
    SELECT DISTINCT COALESCE(NULLIF(btrim(stored_blocker.value->>'code'), ''), stored_blocker.value::text) AS blocker_code
    FROM jsonb_array_elements(COALESCE(v_request.plan_json->'hard_blockers', '[]'::jsonb)) AS stored_blocker(value)
    WHERE NOT (
      v_suggested_resolution_required
      AND v_accepted_resolution_supplied
      AND COALESCE(stored_blocker.value->>'code', '') = 'SUGGESTED_RESOLUTION_REQUIRED'
    )
  ) AS stored_blocker_codes;

  SELECT COALESCE(string_agg(fresh_blocker_codes.blocker_code, '|' ORDER BY fresh_blocker_codes.blocker_code), '')
  INTO v_fresh_effective_blocker_codes
  FROM (
    SELECT DISTINCT COALESCE(NULLIF(btrim(fresh_blocker.value->>'code'), ''), fresh_blocker.value::text) AS blocker_code
    FROM jsonb_array_elements(COALESCE(v_effective_hard_blockers, '[]'::jsonb)) AS fresh_blocker(value)
  ) AS fresh_blocker_codes;

  v_fresh_plan_material_json := jsonb_build_object(
    'classification', v_fresh_classification,
    'recommended_action', v_fresh_plan->>'recommended_action',
    'selection', COALESCE(v_fresh_plan->'selection', '{}'::jsonb),
    'amounts', COALESCE(v_fresh_plan->'amounts', '{}'::jsonb),
    'hard_blockers', v_fresh_hard_blockers,
    'effective_hard_blockers', v_effective_hard_blockers,
    'suggested_resolution_required', v_suggested_resolution_required,
    'work_expansion_plan', COALESCE(v_fresh_plan->'work_expansion_plan', '{}'::jsonb),
    'movement_classification_detail', jsonb_build_object(
      'classification', v_fresh_plan#>>'{movement_classification_detail,classification}',
      'counts', COALESCE(v_fresh_plan#>'{movement_classification_detail,counts}', '{}'::jsonb),
      'blockers', COALESCE(v_fresh_plan#>'{movement_classification_detail,blockers}', '[]'::jsonb),
      'safe_to_auto_apply', COALESCE(v_fresh_plan#>'{movement_classification_detail,safe_to_auto_apply}', 'false'::jsonb)
    )
  );

  v_fresh_plan_hash := md5(v_fresh_plan_material_json::text);

  SELECT COALESCE(jsonb_agg(plan_change.field_name ORDER BY plan_change.field_name), '[]'::jsonb)
  INTO v_plan_changed_fields
  FROM (
    VALUES
      ('amounts', COALESCE((v_request.plan_json->'amounts')::text, ''), COALESCE((v_fresh_plan->'amounts')::text, '')),
      ('classification', COALESCE(v_request.plan_json->>'classification', ''), COALESCE(v_fresh_classification, '')),
      ('effective_hard_blocker_codes', COALESCE(v_stored_effective_blocker_codes, ''), COALESCE(v_fresh_effective_blocker_codes, '')),
      ('recommended_action', COALESCE(v_request.plan_json->>'recommended_action', ''), COALESCE(v_fresh_plan->>'recommended_action', '')),
      ('selected_item_count', COALESCE(v_request.plan_json#>>'{selection,selected_item_count}', ''), COALESCE(v_fresh_plan#>>'{selection,selected_item_count}', '')),
      ('selected_selection_hash', COALESCE(v_request.plan_json#>>'{selection,selected_selection_hash}', ''), COALESCE(v_fresh_plan#>>'{selection,selected_selection_hash}', '')),
      ('suggested_resolution_required', COALESCE(v_request.plan_json->>'suggested_resolution_required', ''), COALESCE(v_suggested_resolution_required::text, '')),
      ('work_unit', COALESCE(v_request.plan_json#>>'{work_expansion_plan,work_unit}', ''), COALESCE(v_fresh_plan#>>'{work_expansion_plan,work_unit}', ''))
  ) AS plan_change(field_name, stored_value, fresh_value)
  WHERE plan_change.stored_value IS DISTINCT FROM plan_change.fresh_value;

  v_plan_stale_detail := jsonb_build_object(
    'stored_plan_hash', v_request.plan_hash,
    'fresh_plan_hash', v_fresh_plan_hash,
    'changed_fields', v_plan_changed_fields,
    'stored_summary', jsonb_build_object(
      'classification', v_request.plan_json->>'classification',
      'recommended_action', v_request.plan_json->>'recommended_action',
      'selected_item_count', v_request.plan_json#>>'{selection,selected_item_count}',
      'selected_selection_hash', v_request.plan_json#>>'{selection,selected_selection_hash}',
      'suggested_resolution_required', v_request.plan_json->>'suggested_resolution_required',
      'work_unit', v_request.plan_json#>>'{work_expansion_plan,work_unit}',
      'amounts', COALESCE(v_request.plan_json->'amounts', '{}'::jsonb),
      'hard_blockers', COALESCE(v_request.plan_json->'hard_blockers', '[]'::jsonb)
    ),
    'fresh_summary', jsonb_build_object(
      'classification', v_fresh_classification,
      'recommended_action', v_fresh_plan->>'recommended_action',
      'selected_item_count', v_fresh_plan#>>'{selection,selected_item_count}',
      'selected_selection_hash', v_fresh_plan#>>'{selection,selected_selection_hash}',
      'suggested_resolution_required', v_suggested_resolution_required,
      'work_unit', v_fresh_plan#>>'{work_expansion_plan,work_unit}',
      'amounts', COALESCE(v_fresh_plan->'amounts', '{}'::jsonb),
      'hard_blockers', v_fresh_hard_blockers,
      'effective_hard_blockers', v_effective_hard_blockers
    )
  );

  IF v_accepted_resolution_is_stale THEN
    v_block_reason := 'ACCEPTED_RESOLUTION_STALE';
  ELSIF v_finance_resolution_blocker IS NOT NULL THEN
    v_block_reason := COALESCE(NULLIF(btrim(v_finance_resolution_blocker->>'code'), ''), 'ACCEPTED_RESOLUTION_VALIDATION_FAILED');
  ELSIF jsonb_array_length(v_plan_changed_fields) > 0 THEN
    v_block_reason := 'PLAN_STALE';
  ELSIF v_fresh_classification = 'AMBIGUOUS_REVIEW_REQUIRED' THEN
    v_block_reason := 'CLASSIFICATION_AMBIGUOUS_REVIEW_REQUIRED';
  ELSIF jsonb_array_length(v_effective_hard_blockers) > 0 THEN
    v_block_reason := 'PLAN_HAS_BLOCKERS';
  ELSIF NOT v_fresh_plan_can_apply
        AND NOT (v_suggested_resolution_required AND v_accepted_resolution_supplied AND jsonb_array_length(v_effective_hard_blockers) = 0) THEN
    v_block_reason := 'PLAN_CANNOT_APPLY';
  ELSE
    v_block_reason := NULL;
  END IF;

  IF v_block_reason IS NOT NULL THEN
    v_before_request := to_jsonb(v_request);

    UPDATE public.pay_payment_correction_requests
    SET
      status = 'BLOCKED',
      updated_at_utc = now()
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id
    RETURNING public.pay_payment_correction_requests.* INTO v_request;

    v_after_request := to_jsonb(v_request);

    INSERT INTO public.pay_payment_correction_actions (
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
      v_request.id,
      v_request.pay_batch_id,
      'USER',
      p_actor_user_id,
      'BLOCK',
      now(),
      v_block_reason,
      v_before_request,
      v_after_request,
      jsonb_build_object(
        'block_reason', v_block_reason,
        'stored_plan_hash', v_request.plan_hash,
        'fresh_plan_hash', v_fresh_plan_hash,
        'effective_hard_blockers', v_effective_hard_blockers,
        'fresh_classification', v_fresh_classification,
        'fresh_can_apply', v_fresh_plan_can_apply,
        'finance_resolution_validation', v_finance_resolution_validation,
        'plan_stale_detail', v_plan_stale_detail
      )
    );

    RETURN jsonb_build_object(
      'ok', false,
      'correction_request_id', v_request.id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'status', v_request.status,
      'block_reason', v_block_reason,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expanded', false,
      'fresh_plan', v_fresh_plan,
      'effective_hard_blockers', v_effective_hard_blockers,
      'stored_plan_hash', v_request.plan_hash,
      'fresh_plan_hash', v_fresh_plan_hash,
      'plan_changed_fields', v_plan_changed_fields,
      'plan_stale_detail', v_plan_stale_detail,
      'progress', '{}'::jsonb
    );
  END IF;

  v_expand_result := public.pay_payment_correction_expand_work(
    p_correction_request_id,
    p_actor_user_id
  );

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_AUTHORISE_RESULT',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'pay_batch_id', v_request.pay_batch_id,
      'action', v_action,
      'approved_count', v_request.approved_count,
      'required_quantity', v_request.required_quantity,
      'expand_result', v_expand_result,
      'immediate_process_result', v_immediate_process_result
    ),
    'pay_payment_correction',
    p_correction_request_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  SELECT jsonb_build_object(
    'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING'),
    'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING'),
    'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED'),
    'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED'),
    'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED'),
    'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE'),
    'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL'),
    'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED'),
    'total', count(*)
  )
  INTO v_work_item_counts
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

  v_total_work_item_count := COALESCE((v_work_item_counts->>'total')::integer, 0);
  v_pending_work_item_count := COALESCE((v_work_item_counts->>'pending')::integer, 0);

  IF v_total_work_item_count > 0
     AND v_total_work_item_count <= 50
     AND v_pending_work_item_count > 0 THEN
    v_immediate_process_result := public.pay_payment_correction_process_chunk(
      p_correction_request_id => p_correction_request_id,
      p_limit => 50,
      p_worker_id => 'correction-authorise',
      p_actor_user_id => p_actor_user_id
    );

    SELECT jsonb_build_object(
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING'),
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING'),
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED'),
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED'),
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED'),
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE'),
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL'),
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED'),
      'total', count(*)
    )
    INTO v_work_item_counts
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id;

  RETURN jsonb_build_object(
    'ok', true,
    'correction_request_id', v_request.id,
    'pay_batch_id', v_request.pay_batch_id,
    'action', v_action,
    'status', v_request.status,
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'authorised', true,
    'expanded', true,
    'expand_result', v_expand_result,
    'immediate_process_result', v_immediate_process_result,
    'progress', v_work_item_counts
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_AUTHORISE_ERROR',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'actor_user_id', p_actor_user_id,
        'action', p_action,
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

ALTER FUNCTION pay_payment_correction_authorise(uuid,uuid,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_correction_authorise(uuid,uuid,text,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_correction_authorise(uuid,uuid,text,text) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_correction_authorise(uuid,uuid,text,text) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_payment_correction_authorise(uuid,uuid,text,text) TO service_role;
