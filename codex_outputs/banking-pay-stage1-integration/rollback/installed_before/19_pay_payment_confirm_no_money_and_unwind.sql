-- Exact installed TEST rollback definition and ACL captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: d358cf01f4d0063cd823c784d1c4b993

CREATE OR REPLACE FUNCTION public.pay_payment_confirm_no_money_and_unwind(p_pay_batch_id uuid, p_selection_json jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_confirmation_json jsonb DEFAULT '{}'::jsonb, p_reason text DEFAULT NULL::text, p_idempotency_key text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_selection_json jsonb := '{}'::jsonb;
  v_confirmation_json jsonb := '{}'::jsonb;
  v_diagnostic_json jsonb := '{}'::jsonb;
  v_resolved_scope_json jsonb := '{}'::jsonb;
  v_lifecycle_state text := NULL::text;
  v_recommended_action text := NULL::text;
  v_blockers jsonb := '[]'::jsonb;
  v_race_blockers jsonb := '[]'::jsonb;
  v_carry_forward_blockers jsonb := '[]'::jsonb;
  v_effective_selection_json jsonb := '{}'::jsonb;
  v_selection_hash text := NULL::text;
  v_plan_json jsonb := '{}'::jsonb;
  v_plan_hash text := NULL::text;
  v_accepted_resolution_json jsonb := '{}'::jsonb;
  v_accepted_resolution_hash text := NULL::text;
  v_required_quantity integer := 1;
  v_reason text := NULL::text;
  v_existing_request public.pay_payment_correction_requests%ROWTYPE;
  v_request public.pay_payment_correction_requests%ROWTYPE;
  v_expand_result jsonb := '{}'::jsonb;
  v_process_result jsonb := '{}'::jsonb;
  v_signal_result jsonb := '{}'::jsonb;
  v_idempotency_key text := NULL::text;
  v_scope_type text := NULL::text;
  v_actor_valid boolean := true;
  v_source_text text := NULL::text;
  v_source_bank_event_id uuid := NULL::uuid;
  v_source_bank_event_text text := NULL::text;
  v_source_bank_event_pay_batch_id uuid := NULL::uuid;
  v_source_bank_event_provider_key text := NULL::text;
  v_source_bank_event_transport text := NULL::text;
  v_source_bank_event_key text := NULL::text;
  v_source_bank_event_provider_event_id text := NULL::text;
  v_source_bank_event_provider_transaction_id text := NULL::text;
  v_source_bank_event_provider_request_id text := NULL::text;
  v_source_bank_event_mapping_status text := NULL::text;
  v_source_bank_event_mapping_method text := NULL::text;
  v_source_bank_event_webhook_receipt_id uuid := NULL::uuid;
  v_source_bank_event_signature_valid boolean := NULL::boolean;
  v_source_bank_event_terminal_no_money boolean := false;
  v_source_bank_event_final_paid boolean := false;
  v_source_bank_event_pending boolean := false;
  v_provider_key_text text := NULL::text;
  v_provider_event_key_text text := NULL::text;
  v_provider_event_id_text text := NULL::text;
  v_provider_transaction_id_text text := NULL::text;
  v_provider_request_id_text text := NULL::text;
  v_provider_webhook_receipt_id_text text := NULL::text;
  v_provider_webhook_receipt_id uuid := NULL::uuid;
  v_provider_signature_valid_confirmed boolean := false;
  v_source_is_provider boolean := false;
  v_webhook_receipt_status text := NULL::text;
  v_webhook_receipt_provider_key text := NULL::text;
  v_webhook_receipt_rail_env text := NULL::text;
  v_webhook_receipt_event_key text := NULL::text;
  v_webhook_receipt_signature_valid boolean := NULL::boolean;
  v_webhook_receipt_ok boolean := false;
  v_failed_webhook_replay_receipt_ok boolean := false;
  v_diagnostic_terminal_no_money_count integer := 0;
  v_scope_is_full boolean := true;
  v_strong_mapping_confirmed boolean := false;
  v_strong_provider_evidence boolean := false;
  v_manual_confirmation_ok boolean := false;
  v_manual_confirmation_reference_text text := NULL::text;
  v_is_system_request boolean := false;
  v_auto_unwind_setting boolean := false;
  v_correction_kind text := 'NO_MONEY_UNWIND';
  v_is_complete boolean := false;
  v_grouped_alert_updates jsonb := '[]'::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_BATCH_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_BATCH_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NOT NULL AND COALESCE(jsonb_typeof(p_selection_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_SELECTION_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_SELECTION_MUST_BE_OBJECT')::text;
  END IF;

  IF p_confirmation_json IS NOT NULL AND COALESCE(jsonb_typeof(p_confirmation_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_CONFIRMATION_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_CONFIRMATION_MUST_BE_OBJECT')::text;
  END IF;

  IF p_actor_user_id IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.tms_users AS actor_user
      WHERE actor_user.id = p_actor_user_id
        AND COALESCE(actor_user.is_active, false) = true
    )
    INTO v_actor_valid;

    IF COALESCE(v_actor_valid, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_ACTOR_NOT_ALLOWED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_ACTOR_NOT_ALLOWED', 'actor_user_id', p_actor_user_id::text)::text;
    END IF;
  END IF;

  v_selection_json := COALESCE(p_selection_json, '{}'::jsonb);
  v_confirmation_json := COALESCE(p_confirmation_json, '{}'::jsonb);
  v_reason := NULLIF(BTRIM(COALESCE(p_reason, '')), '');
  v_source_text := UPPER(NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'source', v_confirmation_json ->> 'evidence_source', '')), ''));
  v_provider_key_text := UPPER(NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'provider_key', v_confirmation_json ->> 'provider', v_confirmation_json ->> 'rail_provider', '')), ''));
  v_provider_event_key_text := NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'provider_event_key', v_confirmation_json ->> 'event_key', '')), '');
  v_provider_event_id_text := NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'provider_event_id', v_confirmation_json ->> 'event_id', '')), '');
  v_provider_transaction_id_text := NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'provider_transaction_id', v_confirmation_json ->> 'transaction_id', v_confirmation_json ->> 'provider_payment_id', '')), '');
  v_provider_request_id_text := NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'provider_request_id', v_confirmation_json ->> 'request_id', '')), '');
  v_provider_webhook_receipt_id_text := NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'provider_webhook_receipt_id', v_confirmation_json ->> 'webhook_receipt_id', '')), '');
  v_provider_signature_valid_confirmed := lower(BTRIM(COALESCE(v_confirmation_json ->> 'provider_signature_valid', v_confirmation_json ->> 'signature_valid', 'false'))) IN ('true','t','yes','y','1','on');
  v_source_is_provider := v_source_text IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY', 'PROVIDER_POLL', 'PROVIDER_RESPONSE');

  v_source_bank_event_text := COALESCE(
    NULLIF(BTRIM(v_confirmation_json ->> 'source_bank_event_id'), ''),
    NULLIF(BTRIM(v_confirmation_json ->> 'bank_event_id'), '')
  );

  IF v_source_bank_event_text IS NOT NULL THEN
    IF v_source_bank_event_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_INVALID_SOURCE_BANK_EVENT_ID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_INVALID_SOURCE_BANK_EVENT_ID',
                'source_bank_event_id', v_source_bank_event_text
              )::text;
    END IF;
    v_source_bank_event_id := v_source_bank_event_text::uuid;
  END IF;

  IF v_provider_webhook_receipt_id_text IS NOT NULL THEN
    IF v_provider_webhook_receipt_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_INVALID_WEBHOOK_RECEIPT_ID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_INVALID_WEBHOOK_RECEIPT_ID',
                'provider_webhook_receipt_id', v_provider_webhook_receipt_id_text
              )::text;
    END IF;
    v_provider_webhook_receipt_id := v_provider_webhook_receipt_id_text::uuid;
  END IF;

  v_manual_confirmation_reference_text := NULLIF(BTRIM(COALESCE(
    v_confirmation_json ->> 'provider_reference',
    v_confirmation_json ->> 'provider_ref',
    v_confirmation_json ->> 'bank_reference',
    v_confirmation_json ->> 'bank_ref',
    v_confirmation_json ->> 'rail_tx_id',
    v_confirmation_json ->> 'provider_transaction_id',
    v_confirmation_json ->> 'audit_note',
    v_confirmation_json ->> 'manual_note',
    v_confirmation_json ->> 'note',
    v_reason,
    ''
  )), '');
  v_is_system_request := p_actor_user_id IS NULL;

  SELECT COALESCE(settings_row.banking_pay_auto_unwind_terminal_no_money, false)
  INTO v_auto_unwind_setting
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  IF v_is_system_request AND COALESCE(v_auto_unwind_setting, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_AUTO_UNWIND_DISABLED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_AUTO_UNWIND_DISABLED')::text;
  END IF;

  v_diagnostic_json := public.pay_payment_cancelability_diagnostic(p_pay_batch_id, v_selection_json, p_actor_user_id);
  v_lifecycle_state := v_diagnostic_json ->> 'payment_lifecycle_state';
  v_recommended_action := v_diagnostic_json ->> 'recommended_action';
  v_blockers := COALESCE(v_diagnostic_json -> 'blockers', '[]'::jsonb);
  v_race_blockers := COALESCE(v_diagnostic_json -> 'race_or_submission_blockers', '[]'::jsonb);
  v_carry_forward_blockers := COALESCE(v_diagnostic_json -> 'carry_forward_blockers', '[]'::jsonb);
  v_resolved_scope_json := COALESCE(v_diagnostic_json -> 'resolved_full_payment_scope_json', '{}'::jsonb);
  v_scope_type := COALESCE(NULLIF(BTRIM(v_resolved_scope_json ->> 'scope_type'), ''), 'BATCH');
  v_scope_is_full := CASE
    WHEN LOWER(BTRIM(COALESCE(v_resolved_scope_json ->> 'is_full_scope', 'true'))) IN ('true','t','yes','y','1','on') THEN true
    WHEN LOWER(BTRIM(COALESCE(v_resolved_scope_json ->> 'is_full_scope', 'true'))) IN ('false','f','no','n','0','off') THEN false
    ELSE true
  END;

  IF v_source_bank_event_id IS NOT NULL THEN
    SELECT source_event_row.pay_batch_id,
           source_event_row.provider_key,
           source_event_row.provider_event_transport,
           source_event_row.provider_event_key,
           source_event_row.provider_event_id,
           source_event_row.provider_transaction_id,
           source_event_row.provider_request_id,
           source_event_row.mapping_status,
           source_event_row.mapping_method,
           source_event_row.provider_webhook_receipt_id,
           source_event_row.provider_signature_valid,
           COALESCE(source_event_classification.is_terminal_no_money, false),
           COALESCE(source_event_classification.is_final_money_moved, false),
           COALESCE(source_event_classification.is_pending_non_final, false)
    INTO v_source_bank_event_pay_batch_id,
         v_source_bank_event_provider_key,
         v_source_bank_event_transport,
         v_source_bank_event_key,
         v_source_bank_event_provider_event_id,
         v_source_bank_event_provider_transaction_id,
         v_source_bank_event_provider_request_id,
         v_source_bank_event_mapping_status,
         v_source_bank_event_mapping_method,
         v_source_bank_event_webhook_receipt_id,
         v_source_bank_event_signature_valid,
         v_source_bank_event_terminal_no_money,
         v_source_bank_event_final_paid,
         v_source_bank_event_pending
    FROM public.pay_bank_transfer_events AS source_event_row
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      source_event_row.normalised_state,
      source_event_row.provider_state,
      COALESCE(source_event_row.raw_payload, '{}'::jsonb),
      jsonb_build_object(
        'provider_key', source_event_row.provider_key,
        'provider_event_transport', source_event_row.provider_event_transport,
        'provider_event_key', source_event_row.provider_event_key,
        'provider_transaction_id', source_event_row.provider_transaction_id,
        'provider_request_id', source_event_row.provider_request_id,
        'rail_env', source_event_row.rail_env
      )
    ) AS source_event_classification
    WHERE source_event_row.id = v_source_bank_event_id;

    IF v_source_bank_event_pay_batch_id IS NULL THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_SOURCE_BANK_EVENT_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_SOURCE_BANK_EVENT_NOT_FOUND',
                'source_bank_event_id', v_source_bank_event_id::text
              )::text;
    END IF;

    IF v_source_bank_event_pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_SOURCE_BANK_EVENT_BATCH_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_SOURCE_BANK_EVENT_BATCH_MISMATCH',
                'pay_batch_id', p_pay_batch_id::text,
                'source_bank_event_pay_batch_id', v_source_bank_event_pay_batch_id::text,
                'source_bank_event_id', v_source_bank_event_id::text
              )::text;
    END IF;

    v_provider_key_text := COALESCE(v_provider_key_text, UPPER(NULLIF(BTRIM(v_source_bank_event_provider_key), '')));
    v_source_text := COALESCE(v_source_text, UPPER(NULLIF(BTRIM(v_source_bank_event_transport), '')));
    v_source_is_provider := v_source_text IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY', 'PROVIDER_POLL', 'PROVIDER_RESPONSE');
    v_provider_event_key_text := COALESCE(v_provider_event_key_text, NULLIF(BTRIM(v_source_bank_event_key), ''));
    v_provider_event_id_text := COALESCE(v_provider_event_id_text, NULLIF(BTRIM(v_source_bank_event_provider_event_id), ''));
    v_provider_transaction_id_text := COALESCE(v_provider_transaction_id_text, NULLIF(BTRIM(v_source_bank_event_provider_transaction_id), ''));
    v_provider_request_id_text := COALESCE(v_provider_request_id_text, NULLIF(BTRIM(v_source_bank_event_provider_request_id), ''));
    v_provider_webhook_receipt_id := COALESCE(v_provider_webhook_receipt_id, v_source_bank_event_webhook_receipt_id);
    v_provider_signature_valid_confirmed := COALESCE(v_provider_signature_valid_confirmed, false) OR COALESCE(v_source_bank_event_signature_valid, false);
  END IF;

  IF v_lifecycle_state NOT IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY')
     OR v_recommended_action IS DISTINCT FROM 'NO_MONEY_UNWIND_AND_RECALCULATE' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_NOT_AVAILABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_NOT_AVAILABLE',
              'pay_batch_id', p_pay_batch_id::text,
              'payment_lifecycle_state', v_lifecycle_state,
              'recommended_action', v_recommended_action,
              'diagnostic_json', v_diagnostic_json
            )::text;
  END IF;

  IF COALESCE(v_scope_is_full, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_FULL_SCOPE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_FULL_SCOPE_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text,
              'scope_type', v_scope_type,
              'resolved_full_payment_scope_json', v_resolved_scope_json
            )::text;
  END IF;

  IF COALESCE(jsonb_array_length(v_blockers), 0) > 0
     OR COALESCE(jsonb_array_length(v_race_blockers), 0) > 0
     OR COALESCE(jsonb_array_length(v_carry_forward_blockers), 0) > 0 THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_BLOCKED',
              'pay_batch_id', p_pay_batch_id::text,
              'blockers', v_blockers,
              'race_or_submission_blockers', v_race_blockers,
              'carry_forward_blockers', v_carry_forward_blockers
            )::text;
  END IF;

  v_diagnostic_terminal_no_money_count := COALESCE(NULLIF(BTRIM(COALESCE(v_diagnostic_json #>> '{terminal_no_money_evidence_json,terminal_no_money_count}', '')), '')::integer, 0);

  IF v_provider_webhook_receipt_id IS NOT NULL THEN
    SELECT receipt_row.status,
           receipt_row.provider_key,
           receipt_row.rail_env,
           receipt_row.provider_event_key,
           receipt_row.signature_valid
    INTO v_webhook_receipt_status,
         v_webhook_receipt_provider_key,
         v_webhook_receipt_rail_env,
         v_webhook_receipt_event_key,
         v_webhook_receipt_signature_valid
    FROM public.bank_provider_webhook_receipts AS receipt_row
    WHERE receipt_row.id = v_provider_webhook_receipt_id;

    v_provider_event_key_text := COALESCE(v_provider_event_key_text, v_webhook_receipt_event_key);
  END IF;

  v_webhook_receipt_ok := (
    v_provider_webhook_receipt_id IS NOT NULL
    AND v_webhook_receipt_status IS NOT NULL
    AND UPPER(COALESCE(v_webhook_receipt_status, '')) NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL', 'FAILED_RETRYABLE')
    AND v_webhook_receipt_signature_valid IS TRUE
    AND (v_provider_key_text IS NULL OR v_webhook_receipt_provider_key IS NOT DISTINCT FROM v_provider_key_text)
    AND NULLIF(BTRIM(COALESCE(v_provider_event_key_text, '')), '') IS NOT NULL
    AND v_webhook_receipt_event_key IS NOT NULL
    AND v_webhook_receipt_event_key = v_provider_event_key_text
  );

  v_failed_webhook_replay_receipt_ok := (
    v_provider_webhook_receipt_id IS NOT NULL
    AND v_webhook_receipt_status IS NOT NULL
    AND UPPER(COALESCE(v_webhook_receipt_status, '')) NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL')
    AND (v_provider_key_text IS NULL OR v_webhook_receipt_provider_key IS NOT DISTINCT FROM v_provider_key_text)
    AND NULLIF(BTRIM(COALESCE(v_provider_event_key_text, '')), '') IS NOT NULL
    AND v_webhook_receipt_event_key IS NOT NULL
    AND v_webhook_receipt_event_key = v_provider_event_key_text
  );

  v_strong_mapping_confirmed := (
    lower(BTRIM(COALESCE(v_confirmation_json ->> 'strong_mapping_method', v_confirmation_json ->> 'strong_transfer_mapping', 'false'))) IN ('true','t','yes','y','1','on')
    OR (
      UPPER(BTRIM(COALESCE(v_source_bank_event_mapping_status, ''))) = 'MATCHED'
      AND UPPER(BTRIM(COALESCE(v_source_bank_event_mapping_method, ''))) IN (
        'TRANSFER_ID',
        'PROVIDER_EVENT_ID',
        'PROVIDER_TRANSACTION_ID',
        'PROVIDER_REQUEST_ID',
        'PROVIDER_REFERENCE',
        'RAIL_TX_ID',
        'MANUAL_TRANSFER_SELECTION'
      )
    )
    OR (
      v_source_bank_event_id IS NULL
      AND jsonb_array_length(COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb)) > 0
      AND v_source_text IN ('PROVIDER_POLL', 'PROVIDER_RESPONSE')
    )
  );

  v_strong_provider_evidence := (
    v_source_is_provider
    AND v_provider_key_text IS NOT NULL
    AND (v_provider_event_key_text IS NOT NULL OR v_provider_event_id_text IS NOT NULL OR v_source_bank_event_id IS NOT NULL)
    AND lower(BTRIM(COALESCE(v_confirmation_json ->> 'terminal_no_money_evidence', 'false'))) IN ('true','t','yes','y','1','on')
    AND COALESCE(v_diagnostic_terminal_no_money_count, 0) > 0
    AND COALESCE(v_strong_mapping_confirmed, false) IS TRUE
    AND (v_source_bank_event_id IS NULL OR v_source_bank_event_terminal_no_money IS TRUE)
    AND v_source_bank_event_final_paid IS NOT TRUE
    AND v_source_bank_event_pending IS NOT TRUE
    AND (
      v_source_text <> 'PROVIDER_WEBHOOK'
      OR (
        v_provider_signature_valid_confirmed IS TRUE
        AND v_webhook_receipt_ok IS TRUE
      )
    )
    AND (
      v_source_text <> 'FAILED_WEBHOOK_REPLAY'
      OR v_failed_webhook_replay_receipt_ok IS TRUE
    )
  );

  v_manual_confirmation_ok := lower(BTRIM(COALESCE(v_confirmation_json ->> 'provider_checked', 'false'))) IN ('true','t','yes','y','1','on')
                              AND lower(BTRIM(COALESCE(v_confirmation_json ->> 'no_payment_made', 'false'))) IN ('true','t','yes','y','1','on')
                              AND lower(BTRIM(COALESCE(v_confirmation_json ->> 'terminal_no_money_confirmed', 'false'))) IN ('true','t','yes','y','1','on')
                              AND lower(BTRIM(COALESCE(v_confirmation_json ->> 'will_not_later_be_paid', 'false'))) IN ('true','t','yes','y','1','on')
                              AND v_manual_confirmation_reference_text IS NOT NULL;

  IF v_source_is_provider AND v_strong_provider_evidence IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_PROVIDER_SOURCE_REQUIRES_STRONG_PROVIDER_EVIDENCE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_PROVIDER_SOURCE_REQUIRES_STRONG_PROVIDER_EVIDENCE',
              'confirmation_json', v_confirmation_json,
              'source', v_source_text,
              'provider_key_present', v_provider_key_text IS NOT NULL,
              'provider_event_identity_present', (v_provider_event_key_text IS NOT NULL OR v_provider_event_id_text IS NOT NULL OR v_source_bank_event_id IS NOT NULL),
              'diagnostic_terminal_no_money_count', v_diagnostic_terminal_no_money_count,
              'provider_signature_valid', v_provider_signature_valid_confirmed,
              'webhook_receipt_ok', v_webhook_receipt_ok,
              'failed_webhook_replay_receipt_ok', v_failed_webhook_replay_receipt_ok,
              'scope_is_full', v_scope_is_full,
              'strong_mapping_confirmed', v_strong_mapping_confirmed,
              'source_bank_event_id', CASE WHEN v_source_bank_event_id IS NULL THEN NULL ELSE v_source_bank_event_id::text END,
              'source_bank_event_terminal_no_money', v_source_bank_event_terminal_no_money,
              'source_bank_event_final_paid', v_source_bank_event_final_paid,
              'source_bank_event_pending', v_source_bank_event_pending
            )::text;
  END IF;

  IF v_is_system_request AND v_strong_provider_evidence IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_SYSTEM_REQUIRES_STRONG_PROVIDER_EVIDENCE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_SYSTEM_REQUIRES_STRONG_PROVIDER_EVIDENCE',
              'confirmation_json', v_confirmation_json,
              'source', v_source_text,
              'provider_key_present', v_provider_key_text IS NOT NULL,
              'provider_event_identity_present', (v_provider_event_key_text IS NOT NULL OR v_provider_event_id_text IS NOT NULL OR v_source_bank_event_id IS NOT NULL),
              'diagnostic_terminal_no_money_count', v_diagnostic_terminal_no_money_count,
              'provider_signature_valid', v_provider_signature_valid_confirmed,
              'webhook_receipt_ok', v_webhook_receipt_ok,
              'failed_webhook_replay_receipt_ok', v_failed_webhook_replay_receipt_ok,
              'scope_is_full', v_scope_is_full,
              'strong_mapping_confirmed', v_strong_mapping_confirmed,
              'source_bank_event_id', CASE WHEN v_source_bank_event_id IS NULL THEN NULL ELSE v_source_bank_event_id::text END,
              'source_bank_event_terminal_no_money', v_source_bank_event_terminal_no_money,
              'source_bank_event_final_paid', v_source_bank_event_final_paid,
              'source_bank_event_pending', v_source_bank_event_pending
            )::text;
  END IF;

  IF v_is_system_request IS NOT TRUE AND v_source_is_provider IS NOT TRUE AND v_strong_provider_evidence IS NOT TRUE AND v_manual_confirmation_ok IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CONFIRM_NO_MONEY_MANUAL_CONFIRMATION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CONFIRM_NO_MONEY_MANUAL_CONFIRMATION_REQUIRED',
              'required_fields', jsonb_build_array('provider_checked', 'no_payment_made', 'terminal_no_money_confirmed', 'will_not_later_be_paid', 'provider_reference_or_audit_note')
            )::text;
  END IF;

  v_correction_kind := CASE
    WHEN v_strong_provider_evidence THEN 'NO_MONEY_UNWIND'
    ELSE 'MANUAL_EVIDENCE_NO_MONEY'
  END;

  v_effective_selection_json := v_selection_json
    || jsonb_build_object(
      'requested_action', 'NO_MONEY_UNWIND_AND_RECALCULATE',
      'correction_kind', v_correction_kind,
      'scope_type', v_scope_type,
      'pay_batch_id', p_pay_batch_id::text,
      'pay_batch_item_ids', COALESCE(v_resolved_scope_json -> 'pay_batch_item_ids', '[]'::jsonb),
      'expected_pay_batch_item_ids', COALESCE(v_resolved_scope_json -> 'pay_batch_item_ids', '[]'::jsonb),
      'pay_bank_transfer_ids', COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb),
      'pay_batch_candidate_ids', COALESCE(v_resolved_scope_json -> 'pay_batch_candidate_ids', '[]'::jsonb)
    )
    || jsonb_build_object(
      'candidate_ids', COALESCE(v_resolved_scope_json -> 'candidate_ids', '[]'::jsonb),
      'finance_case_ids', COALESCE(v_resolved_scope_json -> 'finance_case_ids', '[]'::jsonb),
      'finance_component_ids', COALESCE(v_resolved_scope_json -> 'finance_component_ids', '[]'::jsonb),
      'reservation_ids', COALESCE(v_resolved_scope_json -> 'reservation_ids', '[]'::jsonb),
      'manual_adjustments_to_carry_forward', COALESCE(v_diagnostic_json -> 'manual_adjustments_to_carry_forward', '[]'::jsonb),
      'manual_adjustments_carried_forward_existing', COALESCE(v_diagnostic_json -> 'manual_adjustments_carried_forward_existing', '[]'::jsonb)
    );

  v_selection_hash := md5(v_effective_selection_json::text);
  v_idempotency_key := COALESCE(NULLIF(BTRIM(COALESCE(p_idempotency_key, '')), ''), md5(jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'selection_hash', v_selection_hash,
    'action', 'NO_MONEY_UNWIND_AND_RECALCULATE',
    'source_bank_event_id', CASE WHEN v_source_bank_event_id IS NULL THEN NULL ELSE v_source_bank_event_id::text END,
    'provider_event_key', v_provider_event_key_text,
    'provider_event_id', v_provider_event_id_text,
    'provider_webhook_receipt_id', CASE WHEN v_provider_webhook_receipt_id IS NULL THEN NULL ELSE v_provider_webhook_receipt_id::text END,
    'provider_transaction_id', v_provider_transaction_id_text,
    'provider_request_id', v_provider_request_id_text,
    'pay_bank_transfer_ids', COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb),
    'source', v_source_text
  )::text));

  SELECT GREATEST(COALESCE(settings_row.payment_authoriser_quantity, 1), 1)
  INTO v_required_quantity
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  v_required_quantity := GREATEST(COALESCE(v_required_quantity, 1), 1);

  v_plan_json := jsonb_build_object(
    'classification', v_lifecycle_state,
    'payment_lifecycle_state', v_lifecycle_state,
    'recommended_action', v_recommended_action,
    'resolved_full_payment_scope_json', v_resolved_scope_json,
    'finance_scope_json', COALESCE(v_diagnostic_json -> 'finance_scope_json', '{}'::jsonb),
    'provider_evidence', COALESCE(v_diagnostic_json -> 'provider_evidence_summary_json', '{}'::jsonb),
    'terminal_no_money_evidence_json', COALESCE(v_diagnostic_json -> 'terminal_no_money_evidence_json', '{}'::jsonb),
    'carry_forward_blockers', v_carry_forward_blockers,
    'manual_adjustments_to_carry_forward', COALESCE(v_diagnostic_json -> 'manual_adjustments_to_carry_forward', '[]'::jsonb),
    'work_expansion_plan', jsonb_build_object('work_unit', CASE WHEN jsonb_array_length(COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb)) > 0 THEN 'TRANSFER' ELSE 'CANDIDATE_PAYEE' END, 'chunk_size', 50),
    'idempotency_key', v_idempotency_key,
    'provider_event_key', v_provider_event_key_text,
    'provider_event_id', v_provider_event_id_text,
    'provider_webhook_receipt_id', CASE WHEN v_provider_webhook_receipt_id IS NULL THEN NULL ELSE v_provider_webhook_receipt_id::text END,
    'provider_transaction_id', v_provider_transaction_id_text,
    'provider_request_id', v_provider_request_id_text,
    'strong_provider_evidence', v_strong_provider_evidence,
    'strong_mapping_confirmed', v_strong_mapping_confirmed,
    'webhook_receipt_ok', v_webhook_receipt_ok,
    'failed_webhook_replay_receipt_ok', v_failed_webhook_replay_receipt_ok,
    'scope_is_full', v_scope_is_full,
    'source_bank_event_id', CASE WHEN v_source_bank_event_id IS NULL THEN NULL ELSE v_source_bank_event_id::text END,
    'source_bank_event_terminal_no_money', v_source_bank_event_terminal_no_money,
    'source_bank_event_final_paid', v_source_bank_event_final_paid,
    'source_bank_event_pending', v_source_bank_event_pending,
    'manual_confirmation_ok', v_manual_confirmation_ok
  );

  v_plan_hash := md5(v_plan_json::text);

  v_accepted_resolution_json := jsonb_build_object(
    'accepted_action', 'NO_MONEY_UNWIND_AND_RECALCULATE',
    'actor_kind', CASE WHEN v_is_system_request THEN 'SYSTEM' ELSE 'USER' END,
    'accepted_at_utc', now(),
    'accepted_by_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
    'reason', v_reason,
    'manual_confirmation_reference_text', v_manual_confirmation_reference_text,
    'confirmation_json', v_confirmation_json,
    'diagnostic_json', v_diagnostic_json,
    'carry_forward_blockers', v_carry_forward_blockers,
    'idempotency_key', v_idempotency_key
  );

  v_accepted_resolution_hash := md5(v_accepted_resolution_json::text);

  SELECT request_rows.*
  INTO v_existing_request
  FROM public.pay_payment_correction_requests AS request_rows
  WHERE request_rows.pay_batch_id = p_pay_batch_id
    AND request_rows.selection_hash = v_selection_hash
    AND request_rows.correction_kind = v_correction_kind
    AND request_rows.status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING')
  ORDER BY request_rows.created_at_utc
  LIMIT 1
  FOR UPDATE;

  IF FOUND THEN
    UPDATE public.pay_payment_correction_requests AS request_to_authorise
    SET status = CASE WHEN request_to_authorise.status IN ('REQUESTED', 'AWAITING_AUTHORISATION') THEN 'AUTHORISED' ELSE request_to_authorise.status END,
        approved_count = GREATEST(COALESCE(request_to_authorise.approved_count, 0), COALESCE(request_to_authorise.required_quantity, v_required_quantity, 1)),
        authorised_at_utc = COALESCE(request_to_authorise.authorised_at_utc, now()),
        accepted_resolution_json = COALESCE(request_to_authorise.accepted_resolution_json, v_accepted_resolution_json),
        accepted_resolution_hash = COALESCE(request_to_authorise.accepted_resolution_hash, v_accepted_resolution_hash),
        updated_at_utc = now()
    WHERE request_to_authorise.id = v_existing_request.id
    RETURNING request_to_authorise.*
    INTO v_request;
  ELSE
    BEGIN
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
        'AUTHORISED',
        CASE WHEN v_is_system_request THEN NULL::uuid ELSE p_actor_user_id END,
        now(),
        v_required_quantity,
        v_required_quantity,
        false,
        NULL::uuid,
        v_reason,
        v_effective_selection_json,
        v_selection_hash,
        v_plan_json,
        v_plan_hash,
        v_accepted_resolution_json,
        v_accepted_resolution_hash,
        v_source_bank_event_id,
        v_is_system_request,
        now(),
        now(),
        NULL::timestamptz,
        NULL::timestamptz,
        now()
      )
      RETURNING public.pay_payment_correction_requests.*
      INTO v_request;

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
        CASE WHEN v_is_system_request THEN 'SYSTEM' ELSE 'USER' END,
        CASE WHEN v_is_system_request THEN NULL::uuid ELSE p_actor_user_id END,
        'REQUEST',
        now(),
        v_reason,
        NULL::jsonb,
        to_jsonb(v_request),
        jsonb_build_object(
          'requested_action', 'NO_MONEY_UNWIND_AND_RECALCULATE',
          'correction_kind', v_correction_kind,
          'selection_hash', v_selection_hash,
          'idempotency_key', v_idempotency_key,
          'source_bank_event_id', CASE WHEN v_source_bank_event_id IS NULL THEN NULL ELSE v_source_bank_event_id::text END,
          'source', v_source_text
        )
      );
    EXCEPTION WHEN unique_violation THEN
      SELECT request_rows.*
      INTO v_request
      FROM public.pay_payment_correction_requests AS request_rows
      WHERE request_rows.pay_batch_id = p_pay_batch_id
        AND request_rows.selection_hash = v_selection_hash
        AND request_rows.correction_kind = v_correction_kind
        AND request_rows.status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING')
      ORDER BY request_rows.created_at_utc
      LIMIT 1
      FOR UPDATE;
    END;
  END IF;

  v_expand_result := public.pay_payment_correction_expand_work(v_request.id, p_actor_user_id);
  v_process_result := public.pay_payment_correction_process_chunk(v_request.id, 50, 'confirm-no-money-unwind', p_actor_user_id);
  v_grouped_alert_updates := COALESCE(v_process_result -> 'grouped_alert_updates', '[]'::jsonb);
  v_is_complete := COALESCE((v_process_result ->> 'complete')::boolean, false);

  v_signal_result := public.banking_pay_batch_signal_touch(
    p_pay_batch_id,
    'PAYMENT_CONFIRM_NO_MONEY_AND_UNWIND',
    'pay_payment_confirm_no_money_and_unwind',
    jsonb_build_object(
      'correction_request_id', v_request.id::text,
      'selection_hash', v_selection_hash,
      'payment_lifecycle_state', v_lifecycle_state,
      'provider_failure_reason_group', v_diagnostic_json ->> 'provider_failure_reason_group',
      'progress', v_process_result
    ),
    true,
    true,
    COALESCE(jsonb_array_length(v_grouped_alert_updates), 0) > 0 OR v_is_complete IS NOT TRUE,
    true
  );

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'correction_request_id', v_request.id::text,
    'payment_lifecycle_state', v_lifecycle_state,
    'recommended_action', v_recommended_action,
    'progress_completed', COALESCE((v_process_result -> 'totals' ->> 'applied')::integer, COALESCE((v_process_result ->> 'applied')::integer, 0)),
    'progress_total', COALESCE((v_process_result -> 'totals' ->> 'total')::integer, 0),
    'is_complete', v_is_complete,
    'released_reservation_count', COALESCE((v_process_result ->> 'released_reservations')::integer, 0),
    'restored_finance_component_count', COALESCE((v_process_result ->> 'restored_finance_components')::integer, 0),
    'voided_transfer_count', COALESCE((v_process_result ->> 'updated_transfer_count')::integer, 0),
    'voided_item_count', COALESCE((v_process_result ->> 'applied')::integer, 0),
    'carry_forward_created_count', COALESCE((v_process_result ->> 'carry_forward_created')::integer, 0),
    'carry_forward_existing_count', COALESCE((v_process_result ->> 'carry_forward_existing')::integer, 0),
    'carry_forward_released_count', COALESCE((v_process_result ->> 'carry_forward_released')::integer, 0),
    'freshness_dirty_result', COALESCE(v_process_result -> 'freshness_dirty_result', '{}'::jsonb),
    'blocked_work_items', COALESCE(v_process_result -> 'blocked_work_items', '[]'::jsonb),
    'diagnostic_json', v_diagnostic_json,
    'expand_result', v_expand_result,
    'process_result', v_process_result,
    'workbench_refresh_status', COALESCE(NULLIF(v_process_result->>'workbench_refresh_status', ''), 'NOT_REQUIRED'),
    'workbench_refresh_queued_count', COALESCE(NULLIF(v_process_result->>'workbench_refresh_queued_count', '')::integer, 0),
    'workbench_refresh_deferred_count', COALESCE(NULLIF(v_process_result->>'workbench_refresh_deferred_count', '')::integer, 0),
    'workbench_refresh_failed_count', COALESCE(NULLIF(v_process_result->>'workbench_refresh_failed_count', '')::integer, 0),
    'workbench_refresh_job_ids', COALESCE(v_process_result->'workbench_refresh_job_ids', '[]'::jsonb),
    'requires_workbench_session', lower(BTRIM(COALESCE(v_process_result->>'requires_workbench_session', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'workbench_refresh', COALESCE(v_process_result->'workbench_refresh', jsonb_build_object(
      'status', COALESCE(NULLIF(v_process_result->>'workbench_refresh_status', ''), 'NOT_REQUIRED'),
      'queued_count', COALESCE(NULLIF(v_process_result->>'workbench_refresh_queued_count', '')::integer, 0),
      'deferred_count', COALESCE(NULLIF(v_process_result->>'workbench_refresh_deferred_count', '')::integer, 0),
      'failed_count', COALESCE(NULLIF(v_process_result->>'workbench_refresh_failed_count', '')::integer, 0),
      'job_ids', COALESCE(v_process_result->'workbench_refresh_job_ids', '[]'::jsonb),
      'requires_workbench_session', lower(BTRIM(COALESCE(v_process_result->>'requires_workbench_session', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    )),
    'live_signal', v_signal_result,
    'grouped_alert_summary_impact', v_grouped_alert_updates
  );
END;
$function$;

ALTER FUNCTION pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) TO service_role;
