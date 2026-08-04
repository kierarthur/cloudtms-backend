-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: de062f00facd426a52b00511f5d70c44

CREATE OR REPLACE FUNCTION public.pay_payment_correction_request_start(p_pay_batch_id uuid, p_selection_json jsonb, p_reason text, p_actor_user_id uuid, p_source_bank_event_id uuid DEFAULT NULL::uuid, p_auto_requested boolean DEFAULT false, p_accepted_resolution_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_batch_exists boolean := false;
  v_actor_exists boolean := false;
  v_actor_active boolean := false;
  v_source_event_exists boolean := false;
  v_source_event_source text;
  v_source_event_mapping_status text;
  v_source_event_mapping_method text;
  v_source_event_transport text;
  v_source_event_signature_valid boolean := NULL::boolean;
  v_source_event_webhook_receipt_id uuid := NULL::uuid;
  v_source_event_provider_event_type text := NULL::text;
  v_source_event_provider_event_id text := NULL::text;
  v_source_event_provider_transaction_id text := NULL::text;
  v_source_event_provider_request_id text := NULL::text;
  v_source_event_provider_event_key text := NULL::text;
  v_source_event_failure_reason_group text := NULL::text;
  v_source_event_failure_reason_code text := NULL::text;
  v_plan jsonb := '{}'::jsonb;
  v_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_plan_can_apply boolean := false;
  v_hard_blockers jsonb := '[]'::jsonb;
  v_effective_hard_blockers jsonb := '[]'::jsonb;
  v_suggested_resolution_required boolean := false;
  v_accepted_resolution_supplied boolean := false;
  v_accepted_resolution_is_stale boolean := false;
  v_safe_to_auto_apply boolean := false;
  v_ambiguous_mapping_count integer := 0;
  v_amount_mismatch_count integer := 0;
  v_aggregate_subset_issue_count integer := 0;
  v_correction_kind text;
  v_status text;
  v_required_quantity integer := 1;
  v_approved_count integer := 0;
  v_selection_hash text;
  v_plan_material_json jsonb;
  v_plan_hash text;
  v_accepted_resolution_hash text;
  v_finance_resolution_validation jsonb := NULL::jsonb;
  v_finance_resolution_blocker jsonb := NULL::jsonb;
  v_selected_scope_json jsonb := '{}'::jsonb;
  v_request public.pay_payment_correction_requests%rowtype;
  v_existing_request public.pay_payment_correction_requests%rowtype;
  v_draft_removal_requested boolean := false;
  v_draft_removal_auto_authorised boolean := false;
  v_selected_item_count integer := 0;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_mismatch_count integer := 0;
  v_expected_item_id_count integer := 0;
  v_batch_pre_bank_safe boolean := false;
  v_bank_submission_evidence_count integer := 0;
  v_recommended_action text := NULL::text;
  v_requested_action_upper text := NULL::text;
  v_enriched_accepted_resolution_json jsonb := NULL::jsonb;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_REQUEST_START_BEGIN',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', CASE WHEN p_selection_json IS NULL THEN NULL ELSE p_selection_json->>'scope_type' END,
      'actor_user_id', p_actor_user_id,
      'source_bank_event_id', p_source_bank_event_id,
      'auto_requested', COALESCE(p_auto_requested, false),
      'accepted_resolution_supplied', p_accepted_resolution_json IS NOT NULL
    ),
    'pay_payment_correction',
    COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR COALESCE(jsonb_typeof(p_selection_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_batches AS batch_check
    WHERE batch_check.id = p_pay_batch_id
  )
  INTO v_batch_exists;

  IF NOT COALESCE(v_batch_exists, false) THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF COALESCE(p_auto_requested, false) = false THEN
    IF p_actor_user_id IS NULL THEN
      RAISE EXCEPTION 'ACTOR_USER_ID_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'ACTOR_USER_ID_REQUIRED',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;

    SELECT
      true,
      COALESCE(public.tms_users.is_active, false)
    INTO
      v_actor_exists,
      v_actor_active
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

    IF NULLIF(btrim(COALESCE(p_reason, '')), '') IS NULL THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REASON_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_REASON_REQUIRED',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;
  END IF;

  IF COALESCE(p_auto_requested, false) = true THEN
    IF p_source_bank_event_id IS NULL THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;
  END IF;

  IF p_source_bank_event_id IS NOT NULL THEN
    SELECT
      true,
      public.pay_bank_transfer_events.event_source,
      public.pay_bank_transfer_events.mapping_status,
      public.pay_bank_transfer_events.mapping_method,
      public.pay_bank_transfer_events.provider_event_transport,
      public.pay_bank_transfer_events.provider_signature_valid,
      public.pay_bank_transfer_events.provider_webhook_receipt_id,
      public.pay_bank_transfer_events.provider_event_type,
      public.pay_bank_transfer_events.provider_event_id,
      public.pay_bank_transfer_events.provider_transaction_id,
      public.pay_bank_transfer_events.provider_request_id,
      public.pay_bank_transfer_events.provider_event_key,
      public.pay_bank_transfer_events.provider_failure_reason_group,
      public.pay_bank_transfer_events.provider_failure_reason_code
    INTO
      v_source_event_exists,
      v_source_event_source,
      v_source_event_mapping_status,
      v_source_event_mapping_method,
      v_source_event_transport,
      v_source_event_signature_valid,
      v_source_event_webhook_receipt_id,
      v_source_event_provider_event_type,
      v_source_event_provider_event_id,
      v_source_event_provider_transaction_id,
      v_source_event_provider_request_id,
      v_source_event_provider_event_key,
      v_source_event_failure_reason_group,
      v_source_event_failure_reason_code
    FROM public.pay_bank_transfer_events
    WHERE public.pay_bank_transfer_events.id = p_source_bank_event_id
      AND public.pay_bank_transfer_events.pay_batch_id = p_pay_batch_id;

    IF NOT COALESCE(v_source_event_exists, false) THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SOURCE_BANK_EVENT_NOT_FOUND_FOR_BATCH',
                'pay_batch_id', p_pay_batch_id,
                'source_bank_event_id', p_source_bank_event_id
              )::text;
    END IF;
  END IF;

  IF p_accepted_resolution_json IS NOT NULL
     AND COALESCE(jsonb_typeof(p_accepted_resolution_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'ACCEPTED_RESOLUTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'ACCEPTED_RESOLUTION_JSON_MUST_BE_OBJECT',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_requested_action_upper := upper(btrim(COALESCE(
    p_selection_json->>'requested_action',
    p_selection_json->>'correction_kind',
    p_selection_json->>'correction_action',
    p_accepted_resolution_json->>'requested_action',
    p_accepted_resolution_json->>'correction_kind',
    ''
  )));

  IF v_requested_action_upper IN ('TRUE_SETTLED_REVERSAL_REQUIRED', 'SETTLED_REVERSAL', 'MANUAL_EVIDENCE_SETTLED_RETURN') THEN
    RAISE EXCEPTION 'PAID_SETTLED_RECOVERY_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAID_SETTLED_RECOVERY_REQUIRED',
              'message', 'Paid/settled payment states must be handled through amendment and recovery, not settled reversal correction requests.',
              'pay_batch_id', p_pay_batch_id,
              'requested_action', v_requested_action_upper
            )::text;
  END IF;

  v_plan := public.pay_payment_correction_plan(
    p_pay_batch_id,
    p_selection_json,
    p_actor_user_id
  );

  v_classification := COALESCE(
    NULLIF(btrim(v_plan->>'payment_lifecycle_state'), ''),
    NULLIF(btrim(v_plan->>'classification'), ''),
    'AMBIGUOUS_REVIEW_REQUIRED'
  );
  v_recommended_action := COALESCE(NULLIF(btrim(v_plan->>'recommended_action'), ''), NULLIF(btrim(v_plan->>'user_facing_recommended_action'), ''));
  v_plan_can_apply := COALESCE((v_plan->>'can_apply')::boolean, false);
  v_hard_blockers := COALESCE(v_plan->'hard_blockers', '[]'::jsonb);
  IF jsonb_array_length(COALESCE(v_plan->'carry_forward_blockers', '[]'::jsonb)) > 0 THEN
    v_hard_blockers := v_hard_blockers || COALESCE(v_plan->'carry_forward_blockers', '[]'::jsonb);
  END IF;
  v_suggested_resolution_required := COALESCE((v_plan->>'suggested_resolution_required')::boolean, false);
  v_accepted_resolution_supplied := p_accepted_resolution_json IS NOT NULL;
  v_safe_to_auto_apply := COALESCE((v_plan#>>'{movement_classification_detail,safe_to_auto_apply}')::boolean, false);
  v_ambiguous_mapping_count := COALESCE((v_plan#>>'{movement_classification_detail,counts,bank_event_ambiguous_mapping_count}')::integer, 0);
  v_amount_mismatch_count := COALESCE((v_plan#>>'{movement_classification_detail,counts,bank_event_amount_mismatch_count}')::integer, 0);
  v_aggregate_subset_issue_count := COALESCE((v_plan#>>'{movement_classification_detail,counts,aggregate_subset_issue_count}')::integer, 0);

  v_accepted_resolution_is_stale := p_accepted_resolution_json IS NOT NULL
    AND (
      lower(COALESCE(p_accepted_resolution_json->>'is_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json->>'stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json->>'resolution_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json#>>'{validation,is_stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
      OR lower(COALESCE(p_accepted_resolution_json#>>'{validation,stale}', 'false')) IN ('true', 't', 'yes', 'y', '1')
    );

  v_draft_removal_requested := COALESCE(NULLIF(btrim(p_selection_json->>'source_context'), ''), '') = 'DRAFT_REMOVE_FROM_BATCH'
    AND COALESCE(NULLIF(btrim(p_selection_json->>'requested_action'), ''), '') = 'CANCEL_PAYMENT_ATTEMPT';

  SELECT COALESCE(jsonb_agg(blocker_elements.blocker_value ORDER BY blocker_elements.blocker_ordinal), '[]'::jsonb)
  INTO v_effective_hard_blockers
  FROM jsonb_array_elements(v_hard_blockers) WITH ORDINALITY AS blocker_elements(blocker_value, blocker_ordinal)
  WHERE NOT (
    v_suggested_resolution_required
    AND (
      v_accepted_resolution_supplied
      OR (v_draft_removal_requested AND v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE')
    )
    AND COALESCE(blocker_elements.blocker_value->>'code', '') = 'SUGGESTED_RESOLUTION_REQUIRED'
  );

  IF v_accepted_resolution_is_stale THEN
    v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_STALE',
      'message', 'The accepted suggested finance resolution is stale and must be regenerated before correction request start.'
    ));
  END IF;

  IF v_draft_removal_requested
     AND v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
     AND v_suggested_resolution_required THEN
    v_suggested_resolution_required := false;
    v_hard_blockers := v_effective_hard_blockers;
    v_plan := v_plan || jsonb_build_object(
      'suggested_resolution_required', false,
      'suggested_resolution', NULL::jsonb,
      'hard_blockers', v_effective_hard_blockers
    );

    IF NOT COALESCE(v_plan_can_apply, false)
       AND jsonb_array_length(v_effective_hard_blockers) = 0 THEN
      v_plan_can_apply := true;
      v_plan := v_plan || jsonb_build_object('can_apply', true);
    END IF;
  END IF;

  IF v_suggested_resolution_required AND v_accepted_resolution_supplied THEN
    v_finance_resolution_validation := public._pay_payment_correction_validate_accepted_finance_resolution(
      p_pay_batch_id => p_pay_batch_id,
      p_selection_json => p_selection_json,
      p_plan_json => v_plan,
      p_accepted_resolution_json => p_accepted_resolution_json,
      p_actor_user_id => p_actor_user_id
    );

    IF NOT COALESCE((v_finance_resolution_validation->>'ok')::boolean, false)
       OR NOT COALESCE((v_finance_resolution_validation->>'validated')::boolean, false) THEN
      v_finance_resolution_blocker := COALESCE(
        v_finance_resolution_validation->'blocker',
        jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_VALIDATION_FAILED',
          'message', 'Accepted finance resolution validation failed before correction request start.'
        )
      );

      v_effective_hard_blockers := v_effective_hard_blockers || jsonb_build_array(v_finance_resolution_blocker);
    END IF;
  END IF;

  IF v_classification = 'AMBIGUOUS_REVIEW_REQUIRED' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_CLASSIFICATION_AMBIGUOUS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_CLASSIFICATION_AMBIGUOUS',
              'pay_batch_id', p_pay_batch_id,
              'classification', v_classification,
              'recommended_action', v_recommended_action,
              'blockers', v_effective_hard_blockers
            )::text;
  END IF;

  IF v_suggested_resolution_required AND NOT v_accepted_resolution_supplied THEN
    RAISE EXCEPTION 'SUGGESTED_RESOLUTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SUGGESTED_RESOLUTION_REQUIRED',
              'pay_batch_id', p_pay_batch_id,
              'suggested_resolution', v_plan->'suggested_resolution'
            )::text;
  END IF;

  IF jsonb_array_length(v_effective_hard_blockers) > 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PLAN_HAS_BLOCKERS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_PLAN_HAS_BLOCKERS',
              'pay_batch_id', p_pay_batch_id,
              'blockers', v_effective_hard_blockers
            )::text;
  END IF;

  IF COALESCE(v_recommended_action, '') IN ('CHECK_PROVIDER_STATUS', 'RETRY_PROVIDER_LATER', 'AMEND_AND_RECOVER_OVERPAYMENT')
     OR v_classification IN ('PROVIDER_SUBMITTED_PENDING', 'PROVIDER_OUTCOME_UNKNOWN', 'PROVIDER_OUTAGE_RETRY_LATER', 'PAID_OR_SETTLED') THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ACTION_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', CASE
                WHEN COALESCE(v_recommended_action, '') = 'AMEND_AND_RECOVER_OVERPAYMENT' OR v_classification = 'PAID_OR_SETTLED' THEN 'PAID_SETTLED_RECOVERY_REQUIRED'
                WHEN COALESCE(v_recommended_action, '') = 'CHECK_PROVIDER_STATUS' OR v_classification IN ('PROVIDER_SUBMITTED_PENDING', 'PROVIDER_OUTCOME_UNKNOWN') THEN 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER'
                WHEN COALESCE(v_recommended_action, '') = 'RETRY_PROVIDER_LATER' OR v_classification = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'RETRY_PROVIDER_LATER'
                ELSE 'PAYMENT_CORRECTION_REQUEST_ACTION_NOT_ALLOWED'
              END,
              'pay_batch_id', p_pay_batch_id,
              'payment_lifecycle_state', v_classification,
              'recommended_action', v_recommended_action,
              'blockers', v_effective_hard_blockers,
              'plan', v_plan
            )::text;
  END IF;

  IF NOT v_plan_can_apply
     AND NOT (v_suggested_resolution_required AND v_accepted_resolution_supplied AND jsonb_array_length(v_effective_hard_blockers) = 0) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PLAN_CANNOT_APPLY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_PLAN_CANNOT_APPLY',
              'pay_batch_id', p_pay_batch_id,
              'classification', v_classification,
              'can_apply', v_plan_can_apply,
              'blockers', v_effective_hard_blockers
            )::text;
  END IF;

  IF COALESCE(p_auto_requested, false) THEN
    IF NOT v_safe_to_auto_apply THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_NOT_SAFE_TO_APPLY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_NOT_SAFE_TO_APPLY',
                'pay_batch_id', p_pay_batch_id,
                'classification', v_classification,
                'safe_to_auto_apply', v_safe_to_auto_apply,
                'blockers', v_effective_hard_blockers
              )::text;
    END IF;

    IF v_suggested_resolution_required THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_BLOCKED_BY_SUGGESTED_RESOLUTION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_BLOCKED_BY_SUGGESTED_RESOLUTION',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;

    IF COALESCE(upper(v_source_event_mapping_status), '') <> 'MATCHED'
       OR COALESCE(upper(v_source_event_mapping_method), '') NOT IN (
         'TRANSFER_ID',
         'PROVIDER_EVENT_ID',
         'PROVIDER_REFERENCE',
         'PROVIDER_TRANSACTION_ID',
         'RAIL_TX_ID'
       )
       OR COALESCE(upper(COALESCE(v_source_event_transport, v_source_event_source, '')), '') NOT IN (
         'PROVIDER_RESPONSE',
         'PROVIDER_POLL',
         'PROVIDER_WEBHOOK',
         'FAILED_WEBHOOK_REPLAY'
       )
       OR (
         COALESCE(upper(COALESCE(v_source_event_transport, v_source_event_source, '')), '') IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY')
         AND (
           COALESCE(v_source_event_signature_valid, false) IS NOT TRUE
           OR v_source_event_webhook_receipt_id IS NULL
         )
       ) THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_REQUIRES_STRONG_BANK_EVENT_MAPPING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_REQUIRES_STRONG_BANK_EVENT_MAPPING',
                'pay_batch_id', p_pay_batch_id,
                'source_bank_event_id', p_source_bank_event_id,
                'mapping_status', v_source_event_mapping_status,
                'mapping_method', v_source_event_mapping_method,
                'provider_event_transport', v_source_event_transport,
                'event_source', v_source_event_source,
                'provider_signature_valid', v_source_event_signature_valid,
                'provider_webhook_receipt_id', CASE WHEN v_source_event_webhook_receipt_id IS NULL THEN NULL ELSE v_source_event_webhook_receipt_id::text END,
                'message', 'Automatic no-money correction requires verified provider evidence. Bare request IDs, payment references, and local/manual transfer selections are not sufficient.'
              )::text;
    END IF;

    IF v_ambiguous_mapping_count > 0 OR v_amount_mismatch_count > 0 OR v_aggregate_subset_issue_count > 0 THEN
      RAISE EXCEPTION 'AUTO_PAYMENT_CORRECTION_HAS_AMBIGUITY_OR_AMOUNT_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'AUTO_PAYMENT_CORRECTION_HAS_AMBIGUITY_OR_AMOUNT_MISMATCH',
                'pay_batch_id', p_pay_batch_id,
                'ambiguous_mapping_count', v_ambiguous_mapping_count,
                'amount_mismatch_count', v_amount_mismatch_count,
                'aggregate_subset_issue_count', v_aggregate_subset_issue_count
              )::text;
    END IF;
  END IF;

  v_correction_kind := CASE
    WHEN v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE' THEN 'PRE_BANK_CANCEL'
    WHEN v_recommended_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'
         AND COALESCE(upper(v_source_event_source), '') = 'MANUAL_EVIDENCE' THEN 'MANUAL_EVIDENCE_NO_MONEY'
    WHEN v_recommended_action = 'NO_MONEY_UNWIND_AND_RECALCULATE' THEN 'NO_MONEY_UNWIND'
    ELSE NULL
  END;

  IF v_correction_kind IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_KIND_COULD_NOT_BE_DETERMINED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_KIND_COULD_NOT_BE_DETERMINED',
              'pay_batch_id', p_pay_batch_id,
              'classification', v_classification,
              'recommended_action', v_recommended_action
            )::text;
  END IF;

  SELECT COALESCE(public.settings_defaults.payment_authoriser_quantity, 1)
  INTO v_required_quantity
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_required_quantity := GREATEST(COALESCE(v_required_quantity, 1), 1);

  IF COALESCE(p_auto_requested, false) THEN
    v_status := 'AUTHORISED';
    v_approved_count := v_required_quantity;
  ELSE
    v_status := 'AWAITING_AUTHORISATION';
    v_approved_count := 0;
  END IF;

  WITH selected_scope_rows AS (
    SELECT selected_items.*
    FROM public._pay_payment_correction_selected_items(
      p_pay_batch_id,
      p_selection_json,
      true
    ) AS selected_items
  )
  SELECT jsonb_build_object(
    'pay_batch_id', p_pay_batch_id,
    'requested_action', COALESCE(NULLIF(btrim(p_selection_json->>'requested_action'), ''), ''),
    'correction_kind', v_correction_kind,
    'scope_type', COALESCE(NULLIF(btrim(p_selection_json->>'scope_type'), ''), ''),
    'transfer_group_key', COALESCE(NULLIF(btrim(p_selection_json->>'transfer_group_key'), ''), ''),
    'pay_batch_item_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.pay_batch_item_id ORDER BY selected_scope_rows.pay_batch_item_id) FILTER (WHERE selected_scope_rows.pay_batch_item_id IS NOT NULL), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.pay_bank_transfer_id ORDER BY selected_scope_rows.pay_bank_transfer_id) FILTER (WHERE selected_scope_rows.pay_bank_transfer_id IS NOT NULL), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.pay_batch_candidate_id ORDER BY selected_scope_rows.pay_batch_candidate_id) FILTER (WHERE selected_scope_rows.pay_batch_candidate_id IS NOT NULL), '[]'::jsonb),
    'candidate_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.candidate_id ORDER BY selected_scope_rows.candidate_id) FILTER (WHERE selected_scope_rows.candidate_id IS NOT NULL), '[]'::jsonb),
    'finance_case_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.finance_case_id ORDER BY selected_scope_rows.finance_case_id) FILTER (WHERE selected_scope_rows.finance_case_id IS NOT NULL), '[]'::jsonb),
    'finance_component_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.finance_component_id ORDER BY selected_scope_rows.finance_component_id) FILTER (WHERE selected_scope_rows.finance_component_id IS NOT NULL), '[]'::jsonb),
    'reservation_ids', COALESCE(jsonb_agg(DISTINCT selected_scope_rows.reservation_id ORDER BY selected_scope_rows.reservation_id) FILTER (WHERE selected_scope_rows.reservation_id IS NOT NULL), '[]'::jsonb)
  )
  INTO v_selected_scope_json
  FROM selected_scope_rows;

  v_selected_scope_json := COALESCE(v_selected_scope_json, jsonb_build_object(
    'pay_batch_id', p_pay_batch_id,
    'requested_action', COALESCE(NULLIF(btrim(p_selection_json->>'requested_action'), ''), ''),
    'correction_kind', v_correction_kind,
    'scope_type', COALESCE(NULLIF(btrim(p_selection_json->>'scope_type'), ''), ''),
    'transfer_group_key', COALESCE(NULLIF(btrim(p_selection_json->>'transfer_group_key'), ''), ''),
    'pay_batch_item_ids', '[]'::jsonb,
    'pay_bank_transfer_ids', '[]'::jsonb,
    'pay_batch_candidate_ids', '[]'::jsonb,
    'candidate_ids', '[]'::jsonb,
    'finance_case_ids', '[]'::jsonb,
    'finance_component_ids', '[]'::jsonb,
    'reservation_ids', '[]'::jsonb
  ));

  v_selection_hash := md5((v_selected_scope_json - 'requested_action' - 'correction_kind')::text);

  v_draft_removal_requested := COALESCE(NULLIF(btrim(p_selection_json->>'source_context'), ''), '') = 'DRAFT_REMOVE_FROM_BATCH'
    AND COALESCE(NULLIF(btrim(p_selection_json->>'requested_action'), ''), '') = 'CANCEL_PAYMENT_ATTEMPT';

  IF v_draft_removal_requested THEN
    v_selected_item_count := COALESCE(jsonb_array_length(COALESCE(v_selected_scope_json->'pay_batch_item_ids', '[]'::jsonb)), 0);

    IF v_selected_item_count <= 0 THEN
      RAISE EXCEPTION 'DRAFT_REMOVE_SELECTED_ITEM_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_REMOVE_SELECTED_ITEM_REQUIRED',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;

    IF NULLIF(btrim(COALESCE(p_selection_json->>'expected_item_count', '')), '') IS NOT NULL THEN
      IF (p_selection_json->>'expected_item_count') !~ '^\d+$' THEN
        RAISE EXCEPTION 'DRAFT_REMOVE_EXPECTED_ITEM_COUNT_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'DRAFT_REMOVE_EXPECTED_ITEM_COUNT_INVALID',
                  'pay_batch_id', p_pay_batch_id,
                  'expected_item_count', p_selection_json->>'expected_item_count'
                )::text;
      END IF;
      v_expected_item_count := (p_selection_json->>'expected_item_count')::integer;
      IF v_expected_item_count IS DISTINCT FROM v_selected_item_count THEN
        RAISE EXCEPTION 'DRAFT_REMOVE_SELECTION_DRIFT'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'DRAFT_REMOVE_SELECTION_DRIFT',
                  'pay_batch_id', p_pay_batch_id,
                  'expected_item_count', v_expected_item_count,
                  'selected_item_count', v_selected_item_count
                )::text;
      END IF;
    END IF;

    IF COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') = 'array' THEN
      SELECT
        count(*)::integer,
        COALESCE(sum(CASE WHEN NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(COALESCE(v_selected_scope_json->'pay_batch_item_ids', '[]'::jsonb)) AS selected_item_ids(value)
          WHERE selected_item_ids.value = expected_item_ids.value
        ) THEN 1 ELSE 0 END), 0)::integer
      INTO v_expected_item_id_count, v_expected_item_mismatch_count
      FROM jsonb_array_elements_text(p_selection_json->'expected_pay_batch_item_ids') AS expected_item_ids(value);

      IF v_expected_item_id_count IS DISTINCT FROM v_selected_item_count
         OR COALESCE(v_expected_item_mismatch_count, 0) > 0 THEN
        RAISE EXCEPTION 'DRAFT_REMOVE_SELECTION_DRIFT'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'DRAFT_REMOVE_SELECTION_DRIFT',
                  'pay_batch_id', p_pay_batch_id,
                  'expected_item_id_count', v_expected_item_id_count,
                  'selected_item_count', v_selected_item_count,
                  'mismatch_count', v_expected_item_mismatch_count
                )::text;
      END IF;
    END IF;

    WITH batch_state AS (
      SELECT
        public.pay_batches.status,
        public.pay_batches.execution_commit_state,
        public.pay_batches.executing_started_at_utc,
        public.pay_batches.cancelled_at_utc
      FROM public.pay_batches
      WHERE public.pay_batches.id = p_pay_batch_id
    ),
    submission_evidence AS (
      SELECT public.pay_batch_submission_evidence(p_pay_batch_id, true) AS evidence_json
    )
    SELECT
      (
        UPPER(BTRIM(COALESCE(batch_state.status, ''))) IN ('DRAFT', 'DRAFT_CREATED')
        AND UPPER(BTRIM(COALESCE(batch_state.execution_commit_state, ''))) NOT IN ('SUBMITTED_NOT_COMMITTED', 'COMMITTED', 'SETTLED', 'CANCELLED')
        AND batch_state.executing_started_at_utc IS NULL
        AND batch_state.cancelled_at_utc IS NULL
      ),
      CASE
        WHEN COALESCE((submission_evidence.evidence_json->>'has_external_provider_submission')::boolean, false)
          OR COALESCE((submission_evidence.evidence_json->>'has_pending_provider_outcome')::boolean, false)
          OR COALESCE((submission_evidence.evidence_json->>'has_unknown_provider_outcome')::boolean, false)
          OR COALESCE((submission_evidence.evidence_json->>'has_terminal_no_money')::boolean, false)
          OR COALESCE((submission_evidence.evidence_json->>'has_final_paid_or_settled')::boolean, false)
        THEN 1
        ELSE 0
      END
    INTO v_batch_pre_bank_safe, v_bank_submission_evidence_count
    FROM batch_state
    CROSS JOIN submission_evidence;

    IF NOT COALESCE(v_batch_pre_bank_safe, false) OR COALESCE(v_bank_submission_evidence_count, 0) > 0 THEN
      RAISE EXCEPTION 'DRAFT_REMOVE_NO_LONGER_PRE_BANK'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_REMOVE_NO_LONGER_PRE_BANK',
                'pay_batch_id', p_pay_batch_id,
                'bank_submission_evidence_count', v_bank_submission_evidence_count
              )::text;
    END IF;

    IF v_recommended_action <> 'PRE_PROVIDER_CANCEL_AND_RECALCULATE' OR v_correction_kind IS DISTINCT FROM 'PRE_BANK_CANCEL' THEN
      RAISE EXCEPTION 'DRAFT_REMOVE_CLASSIFICATION_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_REMOVE_CLASSIFICATION_MISMATCH',
                'pay_batch_id', p_pay_batch_id,
                'classification', v_classification,
                'correction_kind', v_correction_kind
              )::text;
    END IF;

    IF v_suggested_resolution_required THEN
      RAISE EXCEPTION 'DRAFT_REMOVE_BLOCKED_BY_SUGGESTED_RESOLUTION'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_REMOVE_BLOCKED_BY_SUGGESTED_RESOLUTION',
                'pay_batch_id', p_pay_batch_id
              )::text;
    END IF;

    v_draft_removal_auto_authorised := true;
    v_status := 'AUTHORISED';
    v_approved_count := v_required_quantity;
  END IF;

  v_plan := COALESCE(v_plan, '{}'::jsonb) || jsonb_build_object(
    'payment_lifecycle_state', v_classification,
    'recommended_action', v_recommended_action,
    'provider_failure_reason_code', COALESCE(NULLIF(btrim(v_plan->>'provider_failure_reason_code'), ''), v_source_event_failure_reason_code),
    'provider_failure_reason_group', COALESCE(NULLIF(btrim(v_plan->>'provider_failure_reason_group'), ''), v_source_event_failure_reason_group),
    'provider_failure_reason_label', NULLIF(btrim(COALESCE(v_plan->>'provider_failure_reason_label', '')), ''),
    'alert_candidate_kind', NULLIF(btrim(COALESCE(v_plan->>'alert_candidate_kind', '')), ''),
    'alert_candidate_severity', NULLIF(btrim(COALESCE(v_plan->>'alert_candidate_severity', '')), ''),
    'live_status_signature', COALESCE(NULLIF(btrim(v_plan->>'live_status_signature'), ''), NULLIF(btrim(v_plan->>'status_update_signature'), '')),
    'status_update_signature', COALESCE(NULLIF(btrim(v_plan->>'status_update_signature'), ''), NULLIF(btrim(v_plan->>'live_status_signature'), '')),
    'source_bank_event_links', jsonb_strip_nulls(jsonb_build_object(
      'source_bank_event_id', CASE WHEN p_source_bank_event_id IS NULL THEN NULL ELSE p_source_bank_event_id::text END,
      'event_source', v_source_event_source,
      'provider_event_transport', v_source_event_transport,
      'provider_webhook_receipt_id', CASE WHEN v_source_event_webhook_receipt_id IS NULL THEN NULL ELSE v_source_event_webhook_receipt_id::text END,
      'provider_event_type', v_source_event_provider_event_type,
      'provider_event_id', v_source_event_provider_event_id,
      'provider_transaction_id', v_source_event_provider_transaction_id,
      'provider_request_id', v_source_event_provider_request_id,
      'provider_event_key', v_source_event_provider_event_key,
      'provider_signature_valid', v_source_event_signature_valid,
      'mapping_status', v_source_event_mapping_status,
      'mapping_method', v_source_event_mapping_method
    )),
    'full_payment_scope', COALESCE(v_plan->'full_payment_scope', v_plan->'resolved_full_payment_scope_json', '{}'::jsonb),
    'finance_scope', COALESCE(v_plan->'finance_scope', v_plan->'finance_scope_json', '{}'::jsonb),
    'provider_evidence', COALESCE(v_plan->'provider_evidence', '{}'::jsonb),
    'paid_blockers', COALESCE(v_plan->'paid_evidence', v_plan->'blocking_paid_evidence_json', '{}'::jsonb),
    'carry_forward_detection', jsonb_build_object(
      'manual_adjustment_carry_forward_required', COALESCE(NULLIF(v_plan->>'manual_adjustment_carry_forward_required', '')::boolean, false),
      'manual_adjustments_to_carry_forward', COALESCE(v_plan->'manual_adjustments_to_carry_forward', '[]'::jsonb),
      'safe_carry_forward_items', COALESCE(v_plan->'manual_adjustments_to_carry_forward', '[]'::jsonb),
      'signed_carry_forward_amounts', COALESCE(v_plan->'manual_adjustments_to_carry_forward', '[]'::jsonb),
      'manual_adjustments_carried_forward_existing', COALESCE(v_plan->'manual_adjustments_carried_forward_existing', '[]'::jsonb),
      'carry_forward_blockers', COALESCE(v_plan->'carry_forward_blockers', '[]'::jsonb),
      'manual_adjustment_support_details_json', COALESCE(v_plan->'manual_adjustment_support_details_json', '{}'::jsonb)
    ),
    'confirmation_payload', COALESCE(p_accepted_resolution_json, '{}'::jsonb),
    'idempotency_material', jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'source_bank_event_id', CASE WHEN p_source_bank_event_id IS NULL THEN NULL ELSE p_source_bank_event_id::text END,
      'auto_requested', COALESCE(p_auto_requested, false)
    )
  );

  v_plan_material_json := jsonb_build_object(
    'classification', v_classification,
    'recommended_action', v_plan->>'recommended_action',
    'selection', COALESCE(v_plan->'selection', '{}'::jsonb),
    'amounts', COALESCE(v_plan->'amounts', '{}'::jsonb),
    'hard_blockers', v_hard_blockers,
    'effective_hard_blockers', v_effective_hard_blockers,
    'suggested_resolution_required', v_suggested_resolution_required,
    'work_expansion_plan', COALESCE(v_plan->'work_expansion_plan', '{}'::jsonb),
    'movement_classification_detail', jsonb_build_object(
      'classification', v_plan#>>'{movement_classification_detail,classification}',
      'counts', COALESCE(v_plan#>'{movement_classification_detail,counts}', '{}'::jsonb),
      'blockers', COALESCE(v_plan#>'{movement_classification_detail,blockers}', '[]'::jsonb),
      'safe_to_auto_apply', COALESCE(v_plan#>'{movement_classification_detail,safe_to_auto_apply}', 'false'::jsonb)
    )
  );

  v_plan_hash := md5(v_plan_material_json::text);
  v_enriched_accepted_resolution_json := CASE
    WHEN p_accepted_resolution_json IS NULL THEN NULL::jsonb
    ELSE jsonb_strip_nulls(COALESCE(p_accepted_resolution_json, '{}'::jsonb) || jsonb_build_object(
      'confirmation_payload', COALESCE(p_accepted_resolution_json, '{}'::jsonb),
      'full_payment_scope', COALESCE(v_plan->'full_payment_scope', v_plan->'resolved_full_payment_scope_json', '{}'::jsonb),
      'selected_scope', v_selected_scope_json,
      'finance_scope', COALESCE(v_plan->'finance_scope', v_plan->'finance_scope_json', '{}'::jsonb),
      'payment_lifecycle_state', v_classification,
      'recommended_action', v_recommended_action,
      'provider_evidence', COALESCE(v_plan->'provider_evidence', '{}'::jsonb),
      'paid_blockers', COALESCE(v_plan->'paid_blockers', v_plan->'paid_evidence', v_plan->'blocking_paid_evidence_json', '{}'::jsonb),
      'carry_forward_detection', COALESCE(v_plan->'carry_forward_detection', '{}'::jsonb),
      'idempotency_material', jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'selection_hash', v_selection_hash,
        'correction_kind', v_correction_kind,
        'source_bank_event_id', CASE WHEN p_source_bank_event_id IS NULL THEN NULL ELSE p_source_bank_event_id::text END,
        'provider_event_key', v_source_event_provider_event_key,
        'auto_requested', COALESCE(p_auto_requested, false)
      ),
      'selection_scope_hash', v_selection_hash,
      'changed_scope_context', v_selected_scope_json,
      'signed_carry_forward_amounts', COALESCE(v_plan->'manual_adjustments_to_carry_forward', '[]'::jsonb)
    ))
  END;

  v_accepted_resolution_hash := CASE
    WHEN v_enriched_accepted_resolution_json IS NULL THEN NULL
    ELSE md5(v_enriched_accepted_resolution_json::text)
  END;

  SELECT public.pay_payment_correction_requests.*
  INTO v_existing_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.pay_batch_id = p_pay_batch_id
    AND public.pay_payment_correction_requests.selection_hash = v_selection_hash
    AND public.pay_payment_correction_requests.correction_kind = v_correction_kind
    AND public.pay_payment_correction_requests.status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING')
  ORDER BY public.pay_payment_correction_requests.created_at_utc
  LIMIT 1;

  IF v_existing_request.id IS NOT NULL THEN
    IF COALESCE(v_draft_removal_auto_authorised, false)
       AND v_existing_request.status <> 'AUTHORISED' THEN
      UPDATE public.pay_payment_correction_requests AS existing_draft_remove_request
      SET status = 'AUTHORISED',
          approved_count = GREATEST(COALESCE(existing_draft_remove_request.required_quantity, v_required_quantity, 1), 1),
          authorised_at_utc = COALESCE(existing_draft_remove_request.authorised_at_utc, now()),
          updated_at_utc = now()
      WHERE existing_draft_remove_request.id = v_existing_request.id
      RETURNING existing_draft_remove_request.* INTO v_existing_request;

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
        v_existing_request.id,
        p_pay_batch_id,
        'USER',
        p_actor_user_id,
        'AUTHORISE',
        now(),
        'Auto-authorised draft removal after strict PRE_BANK_CANCEL validation.',
        NULL::jsonb,
        to_jsonb(v_existing_request),
        jsonb_build_object(
          'classification', v_classification,
          'correction_kind', v_correction_kind,
          'draft_removal_auto_authorised', true,
          'selection_hash', v_selection_hash,
          'selected_scope', v_selected_scope_json
        )
      );
    END IF;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_REQUEST_START_EXISTING_OPEN_REQUEST',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'existing_correction_request_id', v_existing_request.id,
        'correction_kind', v_existing_request.correction_kind,
        'status', v_existing_request.status
      ),
      'pay_payment_correction',
      p_pay_batch_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'existing_request', true,
      'correction_request_id', v_existing_request.id,
      'pay_batch_id', v_existing_request.pay_batch_id,
      'correction_kind', v_existing_request.correction_kind,
      'status', v_existing_request.status,
      'authorisation_required', v_existing_request.status <> 'AUTHORISED',
      'approved_count', v_existing_request.approved_count,
      'required_quantity', v_existing_request.required_quantity,
      'plan', v_existing_request.plan_json
    );
  END IF;

  INSERT INTO public.pay_payment_correction_requests (
    pay_batch_id,
    correction_kind,
    status,
    requested_by_user_id,
    requested_at_utc,
    required_quantity,
    approved_count,
    golden_key_used,
    golden_key_user_id,
    reason,
    selection_json,
    selection_hash,
    plan_json,
    plan_hash,
    accepted_resolution_json,
    accepted_resolution_hash,
    source_bank_event_id,
    auto_requested,
    created_at_utc,
    authorised_at_utc,
    applied_at_utc,
    cancelled_at_utc,
    updated_at_utc
  )
  VALUES (
    p_pay_batch_id,
    v_correction_kind,
    v_status,
    CASE WHEN COALESCE(p_auto_requested, false) THEN NULL::uuid ELSE p_actor_user_id END,
    now(),
    v_required_quantity,
    v_approved_count,
    false,
    NULL::uuid,
    NULLIF(btrim(COALESCE(p_reason, '')), ''),
    p_selection_json,
    v_selection_hash,
    v_plan,
    v_plan_hash,
    v_enriched_accepted_resolution_json,
    v_accepted_resolution_hash,
    p_source_bank_event_id,
    COALESCE(p_auto_requested, false),
    now(),
    CASE WHEN COALESCE(p_auto_requested, false) OR COALESCE(v_draft_removal_auto_authorised, false) THEN now() ELSE NULL::timestamptz END,
    NULL::timestamptz,
    NULL::timestamptz,
    now()
  )
  RETURNING public.pay_payment_correction_requests.* INTO v_request;

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
    p_pay_batch_id,
    CASE WHEN COALESCE(p_auto_requested, false) THEN 'SYSTEM' ELSE 'USER' END,
    CASE WHEN COALESCE(p_auto_requested, false) THEN NULL::uuid ELSE p_actor_user_id END,
    'REQUEST',
    now(),
    NULLIF(btrim(COALESCE(p_reason, '')), ''),
    NULL::jsonb,
    to_jsonb(v_request),
    jsonb_build_object(
      'classification', v_classification,
      'correction_kind', v_correction_kind,
      'auto_requested', COALESCE(p_auto_requested, false),
      'source_bank_event_id', p_source_bank_event_id,
      'source_bank_event_links', COALESCE(v_plan->'source_bank_event_links', '{}'::jsonb),
      'provider_failure_reason_group', COALESCE(NULLIF(btrim(v_plan->>'provider_failure_reason_group'), ''), v_source_event_failure_reason_group),
      'plan_hash', v_plan_hash,
      'selection_hash', v_selection_hash,
      'accepted_resolution_hash', v_accepted_resolution_hash,
      'selected_scope', v_selected_scope_json,
      'finance_resolution_validation', v_finance_resolution_validation,
      'draft_removal_auto_authorised', v_draft_removal_auto_authorised
    )
  );

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_REQUEST_START_CREATED',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'correction_request_id', v_request.id,
      'correction_kind', v_correction_kind,
      'status', v_status,
      'classification', v_classification,
      'auto_requested', COALESCE(p_auto_requested, false),
      'required_quantity', v_required_quantity,
      'approved_count', v_approved_count,
      'suggested_resolution_required', v_suggested_resolution_required,
      'accepted_resolution_supplied', v_accepted_resolution_supplied,
      'draft_removal_auto_authorised', v_draft_removal_auto_authorised
    ),
    'pay_payment_correction',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'existing_request', false,
    'correction_request_id', v_request.id,
    'pay_batch_id', v_request.pay_batch_id,
    'correction_kind', v_request.correction_kind,
    'status', v_request.status,
    'authorisation_required', NOT (COALESCE(v_request.auto_requested, false) OR COALESCE(v_draft_removal_auto_authorised, false)),
    'approved_count', v_request.approved_count,
    'required_quantity', v_request.required_quantity,
    'plan', v_plan,
    'selection_hash', v_selection_hash,
    'selection_scope_hash', v_selection_hash,
    'plan_hash', v_plan_hash,
    'accepted_resolution_hash', v_accepted_resolution_hash,
    'selected_scope', v_selected_scope_json,
    'changed_scope_context', v_selected_scope_json,
    'source_bank_event_links', COALESCE(v_plan->'source_bank_event_links', '{}'::jsonb),
    'provider_failure_reason_group', COALESCE(NULLIF(btrim(v_plan->>'provider_failure_reason_group'), ''), v_source_event_failure_reason_group),
    'finance_resolution_validation', v_finance_resolution_validation,
    'draft_removal_auto_authorised', v_draft_removal_auto_authorised
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_REQUEST_START_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'scope_type', CASE WHEN p_selection_json IS NULL THEN NULL ELSE p_selection_json->>'scope_type' END,
        'source_bank_event_id', p_source_bank_event_id,
        'auto_requested', COALESCE(p_auto_requested, false),
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;

ALTER FUNCTION pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb) TO service_role;
