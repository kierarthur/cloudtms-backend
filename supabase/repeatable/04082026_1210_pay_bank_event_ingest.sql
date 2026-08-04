-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Preserves the installed function identity; no overload is added.


CREATE OR REPLACE FUNCTION public.pay_bank_event_ingest(
  p_event_json jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_ingest_options_json jsonb DEFAULT '{}'::jsonb
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
  v_event_json jsonb := COALESCE(p_event_json, '{}'::jsonb);
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_now timestamptz := now();
  v_ingest_options_json jsonb := COALESCE(p_ingest_options_json, '{}'::jsonb);
  v_touch_signal boolean := true;
  v_signal_mode text := 'IMMEDIATE';
  v_should_touch_signal boolean := true;
  v_source_delivery_id text := NULL::text;
  v_source_batch_event_group_key text := NULL::text;
  v_signal_recommendation_json jsonb := '{}'::jsonb;

  v_pay_batch_id uuid := NULL::uuid;
  v_supplied_pay_batch_id uuid := NULL::uuid;
  v_pay_batch_id_text text;
  v_pay_bank_transfer_id uuid := NULL::uuid;
  v_pay_bank_transfer_id_text text;
  v_candidate_id uuid := NULL::uuid;
  v_candidate_id_text text;
  v_umbrella_id uuid := NULL::uuid;
  v_umbrella_id_text text;

  v_provider_key text;
  v_provider_event_id text;
  v_provider_reference text;
  v_provider_state text;
  v_normalised_state text;
  v_event_source text;
  v_event_time_utc timestamptz := NULL::timestamptz;
  v_event_time_text text;
  v_amount numeric := NULL::numeric;
  v_amount_text text;
  v_currency text := 'GBP';
  v_raw_payload jsonb := '{}'::jsonb;
  v_idempotency_key text;
  v_provider_webhook_receipt_id uuid := NULL::uuid;
  v_provider_webhook_receipt_id_text text := NULL::text;
  v_provider_webhook_id text := NULL::text;
  v_provider_event_type text := NULL::text;
  v_provider_transaction_id text := NULL::text;
  v_provider_request_id text := NULL::text;
  v_provider_event_key text := NULL::text;
  v_provider_signature_valid boolean := NULL::boolean;
  v_provider_event_transport text := NULL::text;
  v_adapter_key text := NULL::text;
  v_adapter_version text := NULL::text;
  v_rail_env text := NULL::text;
  v_provider_failure_reason_code text := NULL::text;
  v_provider_failure_reason_group text := NULL::text;
  v_provider_failure_reason_label text := NULL::text;
  v_receipt_status text := NULL::text;
  v_receipt_provider_key text := NULL::text;
  v_receipt_rail_env text := NULL::text;
  v_receipt_provider_event_key text := NULL::text;
  v_receipt_signature_valid boolean := NULL::boolean;
  v_receipt_normalised_events_json jsonb := '[]'::jsonb;
  v_receipt_ingest_results_json jsonb := '[]'::jsonb;
  v_provider_event_key_in_receipt boolean := false;
  v_webhook_gate_ok boolean := false;
  v_replay_gate_ok boolean := false;
  v_mapping_hints_json jsonb := '{}'::jsonb;
  v_failure_reason_result jsonb := '{}'::jsonb;
  v_live_signal_result jsonb := '{}'::jsonb;

  v_mapping_status text := 'UNMATCHED';
  v_mapping_method text := 'UNMATCHED';
  v_mapping_candidate_count integer := 0;
  v_has_strong_transfer_mapping boolean := false;
  v_existing_transfer_is_final_paid boolean := false;
  v_pre_insert_pay_batch_id uuid := NULL::uuid;
  v_pre_insert_pay_bank_transfer_id uuid := NULL::uuid;
  v_pre_insert_candidate_id uuid := NULL::uuid;
  v_pre_insert_umbrella_id uuid := NULL::uuid;
  v_pre_insert_mapping_status text := NULL::text;
  v_pre_insert_mapping_method text := NULL::text;
  v_duplicate_event_should_continue boolean := false;

  v_transfer public.pay_bank_transfers%rowtype;
  v_batch public.pay_batches%rowtype;
  v_event_id uuid := NULL::uuid;
  v_inserted_event boolean := false;

  v_selection_json jsonb := '{}'::jsonb;
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_safe_to_auto_apply boolean := false;
  v_auto_setting boolean := false;
  v_correction_disposition text := 'AMBIGUOUS';

  v_correction_request_id uuid := NULL::uuid;
  v_request_start_result jsonb := NULL::jsonb;
  v_expand_result jsonb := NULL::jsonb;
  v_process_result jsonb := NULL::jsonb;

  v_admin_notice_group_id uuid := NULL::uuid;
  v_admin_notice_result jsonb := NULL::jsonb;
  v_notice_kind text := 'BANK_FAILURE_DETECTED';
  v_exact_mapping_required_blocker jsonb := NULL::jsonb;

  v_provider_state_upper text := NULL::text;
  v_money_movement_classification record;
  v_event_cash_state text := 'UNKNOWN';
  v_event_is_final_paid boolean := false;
  v_event_is_terminal_no_money boolean := false;
  v_event_is_pending_non_final boolean := false;
  v_event_required_action text := NULL::text;
  v_auto_unwind_blocker jsonb := NULL::jsonb;
  v_auto_unwind_result jsonb := NULL::jsonb;
  v_final_work_item_totals jsonb := jsonb_build_object(
    'total', 0,
    'applied', 0,
    'skipped', 0,
    'blocked', 0,
    'failed_retryable', 0,
    'failed_final', 0,
    'pending', 0,
    'processing', 0
  );
  v_carry_forward_mark_result jsonb := '{}'::jsonb;
  v_consumed_carry_forward_count integer := 0;
  v_work_total_count integer := 0;
  v_work_applied_count integer := 0;
  v_work_skipped_count integer := 0;
  v_work_blocked_count integer := 0;
  v_work_failed_retryable_count integer := 0;
  v_work_failed_final_count integer := 0;
  v_work_pending_count integer := 0;
  v_work_processing_count integer := 0;

  v_terminal_bank_event boolean := false;
  v_settlement_required boolean := false;
  v_settlement_trigger text := 'BANK_EVENT_NON_TERMINAL';
  v_all_batch_transfers_terminal boolean := false;
  v_terminal_success_transfer_count integer := 0;
  v_terminal_failed_transfer_count integer := 0;
  v_pending_transfer_count integer := 0;
  v_unknown_transfer_count integer := 0;
  v_pending_or_unknown_transfer_count integer := 0;
  v_settlement_intent_json jsonb := '{}'::jsonb;
  v_settlement_actor_user_id uuid := NULL::uuid;
  v_settlement_apply_result jsonb := '{}'::jsonb;
  v_mutation_guard jsonb := '{}'::jsonb;
  v_suppress_auto_unwind boolean := false;
  v_manual_confirmed_not_paid boolean := false;
  v_paid_after_release boolean := false;
  v_review_acknowledgement boolean := false;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_BANK_EVENT_INGEST_START',
    jsonb_build_object(
      'actor_user_id', p_actor_user_id,
      'ingest_options', COALESCE(p_ingest_options_json, '{}'::jsonb),
      'event_keys', CASE
        WHEN p_event_json IS NULL OR jsonb_typeof(p_event_json) <> 'object' THEN '[]'::jsonb
        ELSE COALESCE((
          SELECT jsonb_agg(event_keys.key_name ORDER BY event_keys.key_name)
          FROM jsonb_object_keys(p_event_json) AS event_keys(key_name)
        ), '[]'::jsonb)
      END
    ),
    'pay_payment_correction',
    'BANK_EVENT_INGEST',
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_event_json IS NULL OR COALESCE(jsonb_typeof(p_event_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANK_EVENT_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANK_EVENT_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF p_ingest_options_json IS NULL THEN

    v_ingest_options_json := '{}'::jsonb;
  ELSIF COALESCE(jsonb_typeof(p_ingest_options_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANK_EVENT_INGEST_OPTIONS_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANK_EVENT_INGEST_OPTIONS_MUST_BE_OBJECT')::text;
  ELSE
    v_ingest_options_json := p_ingest_options_json;
  END IF;

  v_suppress_auto_unwind := pg_catalog.lower(pg_catalog.btrim(COALESCE(
    v_event_json->>'suppress_auto_unwind',
    v_ingest_options_json->>'suppress_auto_unwind',
    'false'
  ))) IN ('true','t','1','yes','y','on');
  v_manual_confirmed_not_paid := pg_catalog.upper(COALESCE(
    v_event_json->>'resolution', v_event_json->>'normalised_state', ''
  )) = 'CONFIRMED_NOT_PAID'
    AND pg_catalog.upper(COALESCE(v_event_json->>'event_source', '')) = 'MANUAL_EVIDENCE';
  v_review_acknowledgement := pg_catalog.upper(COALESCE(
    v_event_json->>'review_status', v_ingest_options_json->>'review_status', ''
  )) = 'ACKNOWLEDGED';

  IF v_ingest_options_json ? 'touch_signal' THEN
    v_touch_signal := lower(btrim(COALESCE(v_ingest_options_json->>'touch_signal', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  ELSE
    v_touch_signal := true;
  END IF;

  v_signal_mode := upper(nullif(btrim(COALESCE(v_ingest_options_json->>'signal_mode', 'IMMEDIATE')), ''));
  IF v_signal_mode IS NULL OR v_signal_mode NOT IN ('IMMEDIATE', 'DEFERRED') THEN
    v_signal_mode := 'IMMEDIATE';
  END IF;
  v_should_touch_signal := COALESCE(v_touch_signal, true) AND v_signal_mode <> 'DEFERRED';
  v_source_delivery_id := nullif(btrim(COALESCE(v_ingest_options_json->>'source_delivery_id', '')), '');
  v_source_batch_event_group_key := nullif(btrim(COALESCE(v_ingest_options_json->>'source_batch_event_group_key', '')), '');

  v_pay_batch_id_text := nullif(btrim(COALESCE(
    v_event_json->>'pay_batch_id',
    v_event_json->>'batch_id',
    ''
  )), '');

  v_pay_bank_transfer_id_text := nullif(btrim(COALESCE(
    v_event_json->>'pay_bank_transfer_id',
    v_event_json->>'transfer_id',
    v_event_json->>'bank_transfer_id',
    ''
  )), '');

  v_candidate_id_text := nullif(btrim(COALESCE(v_event_json->>'candidate_id', '')), '');
  v_umbrella_id_text := nullif(btrim(COALESCE(v_event_json->>'umbrella_id', '')), '');

  IF v_pay_batch_id_text IS NOT NULL THEN
    IF v_pay_batch_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_PAY_BATCH_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_PAY_BATCH_ID_IN_BANK_EVENT', 'pay_batch_id', v_pay_batch_id_text)::text;
    END IF;
    v_pay_batch_id := v_pay_batch_id_text::uuid;
    v_supplied_pay_batch_id := v_pay_batch_id;
  END IF;

  IF v_pay_bank_transfer_id_text IS NOT NULL THEN
    IF v_pay_bank_transfer_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_PAY_BANK_TRANSFER_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_PAY_BANK_TRANSFER_ID_IN_BANK_EVENT', 'pay_bank_transfer_id', v_pay_bank_transfer_id_text)::text;
    END IF;
    v_pay_bank_transfer_id := v_pay_bank_transfer_id_text::uuid;
  END IF;

  IF v_candidate_id_text IS NOT NULL THEN
    IF v_candidate_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_CANDIDATE_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_CANDIDATE_ID_IN_BANK_EVENT', 'candidate_id', v_candidate_id_text)::text;
    END IF;
    v_candidate_id := v_candidate_id_text::uuid;
  END IF;

  IF v_umbrella_id_text IS NOT NULL THEN
    IF v_umbrella_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_UMBRELLA_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_UMBRELLA_ID_IN_BANK_EVENT', 'umbrella_id', v_umbrella_id_text)::text;
    END IF;
    v_umbrella_id := v_umbrella_id_text::uuid;
  END IF;

  v_provider_key := upper(nullif(btrim(COALESCE(v_event_json->>'provider_key', v_event_json->>'provider', v_event_json->>'rail_provider', '')), ''));
  v_provider_event_id := nullif(btrim(COALESCE(v_event_json->>'provider_event_id', v_event_json->>'event_id', v_event_json->>'id', '')), '');
  v_provider_webhook_receipt_id_text := nullif(btrim(COALESCE(v_event_json->>'provider_webhook_receipt_id', v_event_json->>'webhook_receipt_id', '')), '');
  IF v_provider_webhook_receipt_id_text IS NOT NULL THEN
    IF v_provider_webhook_receipt_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_PROVIDER_WEBHOOK_RECEIPT_ID_IN_BANK_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_PROVIDER_WEBHOOK_RECEIPT_ID_IN_BANK_EVENT', 'provider_webhook_receipt_id', v_provider_webhook_receipt_id_text)::text;
    END IF;
    v_provider_webhook_receipt_id := v_provider_webhook_receipt_id_text::uuid;
  END IF;
  v_provider_webhook_id := nullif(btrim(COALESCE(v_event_json->>'provider_webhook_id', v_event_json->>'webhook_id', '')), '');
  v_provider_event_type := nullif(btrim(COALESCE(v_event_json->>'provider_event_type', v_event_json->>'event_type', v_event_json->>'event', '')), '');
  v_provider_transaction_id := nullif(btrim(COALESCE(v_event_json->>'provider_transaction_id', v_event_json->>'transaction_id', v_event_json->>'provider_payment_id', v_event_json->>'payment_id', v_event_json #>> '{data,id}', '')), '');
  v_provider_request_id := nullif(btrim(COALESCE(v_event_json->>'provider_request_id', v_event_json->>'request_id', v_event_json #>> '{data,request_id}', '')), '');
  v_provider_event_key := nullif(btrim(COALESCE(v_event_json->>'provider_event_key', v_event_json->>'event_key', '')), '');
  v_provider_signature_valid := CASE
    WHEN v_event_json ? 'provider_signature_valid' OR v_event_json ? 'signature_valid' THEN lower(btrim(COALESCE(v_event_json->>'provider_signature_valid', v_event_json->>'signature_valid', ''))) IN ('true','t','1','yes','y','on')
    ELSE NULL::boolean
  END;
  v_provider_event_transport := upper(nullif(btrim(COALESCE(v_event_json->>'provider_event_transport', v_event_json->>'event_source', v_event_json->>'source', 'PROVIDER_POLL')), ''));
  v_adapter_key := nullif(btrim(COALESCE(v_event_json->>'adapter_key', '')), '');
  v_adapter_version := nullif(btrim(COALESCE(v_event_json->>'adapter_version', '')), '');
  v_rail_env := upper(nullif(btrim(COALESCE(v_event_json->>'rail_env', v_event_json->>'rail_environment', 'PROD')), ''));
  v_provider_failure_reason_code := nullif(btrim(COALESCE(v_event_json->>'provider_failure_reason_code', v_event_json->>'failure_reason_code', v_event_json->>'reason_code', v_event_json->>'error_code', v_event_json #>> '{data,reason_code}', v_event_json #>> '{data,error_code}', '')), '');
  v_provider_failure_reason_group := upper(nullif(btrim(COALESCE(v_event_json->>'provider_failure_reason_group', v_event_json->>'failure_reason_group', '')), ''));
  IF v_event_json ? 'mapping_hints_json' AND jsonb_typeof(v_event_json->'mapping_hints_json') = 'object' THEN
    v_mapping_hints_json := v_event_json->'mapping_hints_json';
  ELSIF v_event_json ? 'mapping_hints' AND jsonb_typeof(v_event_json->'mapping_hints') = 'object' THEN
    v_mapping_hints_json := v_event_json->'mapping_hints';
  ELSE
    v_mapping_hints_json := '{}'::jsonb;
  END IF;
  v_provider_reference := nullif(btrim(COALESCE(
    v_event_json->>'provider_reference',
    v_event_json->>'provider_ref',
    v_event_json->>'rail_tx_id',
    v_event_json->>'payment_reference',
    v_provider_transaction_id,
    ''
  )), '');
  v_provider_state := nullif(btrim(COALESCE(v_event_json->>'provider_state', v_event_json->>'state', v_event_json->>'status', v_event_json #>> '{data,new_state}', v_event_json #>> '{data,state}', '')), '');
  v_normalised_state := upper(nullif(btrim(COALESCE(v_event_json->>'normalised_state', v_event_json->>'normalized_state', v_provider_state, 'UNKNOWN')), ''));
  v_event_source := v_provider_event_transport;
  v_event_time_text := nullif(btrim(COALESCE(v_event_json->>'event_time_utc', v_event_json->>'event_time', v_event_json->>'created_at', v_event_json->>'timestamp', '')), '');
  v_amount_text := nullif(btrim(COALESCE(v_event_json->>'amount', v_event_json->>'amount_inc_vat', '')), '');
  v_currency := upper(nullif(btrim(COALESCE(v_event_json->>'currency', 'GBP')), ''));
  v_raw_payload := COALESCE(v_event_json->'raw_payload', v_event_json);

  IF v_event_time_text IS NOT NULL THEN
    BEGIN
      v_event_time_utc := v_event_time_text::timestamptz;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION 'INVALID_BANK_EVENT_TIME'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'INVALID_BANK_EVENT_TIME', 'event_time_utc', v_event_time_text)::text;
    END;
  END IF;

  IF v_amount_text IS NOT NULL THEN
    BEGIN
      v_amount := v_amount_text::numeric;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION 'INVALID_BANK_EVENT_AMOUNT'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'INVALID_BANK_EVENT_AMOUNT', 'amount', v_amount_text)::text;
    END;
  END IF;

  SELECT classification_rows.*
  INTO v_money_movement_classification
  FROM public._pay_rail_state_money_movement_classify(
    v_normalised_state,
    v_provider_state,
    COALESCE(v_event_json, '{}'::jsonb),
    COALESCE(v_raw_payload, '{}'::jsonb)
  ) AS classification_rows
  LIMIT 1;

  v_event_cash_state := COALESCE(v_money_movement_classification.cash_state, 'UNKNOWN');
  v_event_is_final_paid := COALESCE(v_money_movement_classification.is_final_money_moved, false);
  v_event_is_terminal_no_money := COALESCE(v_money_movement_classification.is_terminal_no_money, false);
  v_event_is_pending_non_final := COALESCE(v_money_movement_classification.is_pending_non_final, false);

  v_normalised_state := CASE
    WHEN v_event_is_final_paid THEN 'COMPLETED'
    WHEN v_event_is_terminal_no_money AND upper(btrim(coalesce(v_provider_state, v_normalised_state, ''))) IN ('CANCELED', 'CANCELLED', 'CANCELLED_BEFORE_RELEASE', 'CANCELED_BEFORE_RELEASE') THEN 'CANCELLED'
    WHEN v_event_is_terminal_no_money THEN 'FAILED'
    WHEN v_event_is_pending_non_final AND upper(btrim(coalesce(v_provider_state, v_normalised_state, ''))) IN ('SUBMITTED', 'SENT') THEN 'SUBMITTED'
    WHEN v_event_is_pending_non_final AND upper(btrim(coalesce(v_provider_state, v_normalised_state, ''))) = 'PROCESSING' THEN 'PROCESSING'
    WHEN v_event_is_pending_non_final THEN 'PENDING'
    ELSE 'UNKNOWN'
  END;

  v_failure_reason_result := public._banking_provider_failure_reason_normalise(
    v_provider_key,
    COALESCE(v_provider_state, v_normalised_state),
    v_provider_failure_reason_code,
    COALESCE(v_event_json->>'provider_failure_reason_text', v_event_json->>'failure_reason', v_event_json->>'reason', v_event_json->>'message', v_event_json->>'error_message'),
    COALESCE(v_raw_payload, '{}'::jsonb)
  );
  v_provider_failure_reason_code := COALESCE(v_provider_failure_reason_code, NULLIF(BTRIM(v_failure_reason_result->>'failure_reason_code'), ''));

  v_provider_failure_reason_group := COALESCE(v_provider_failure_reason_group, NULLIF(BTRIM(v_failure_reason_result->>'failure_reason_group'), ''));
  v_provider_failure_reason_label := NULLIF(BTRIM(v_failure_reason_result->>'failure_reason_label'), '');

  v_provider_state_upper := upper(btrim(coalesce(v_provider_state, '')));

  IF v_event_source NOT IN ('PROVIDER_RESPONSE', 'PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'FAILED_WEBHOOK_REPLAY', 'MANUAL_CONFIRM', 'MANUAL_EVIDENCE', 'SYSTEM') THEN
    v_event_source := CASE
      WHEN v_event_source LIKE '%FAILED%WEBHOOK%REPLAY%' THEN 'FAILED_WEBHOOK_REPLAY'
      WHEN v_event_source LIKE '%WEBHOOK%' THEN 'PROVIDER_WEBHOOK'
      WHEN v_event_source LIKE '%RESPONSE%' THEN 'PROVIDER_RESPONSE'
      WHEN v_event_source LIKE '%MANUAL%EVIDENCE%' THEN 'MANUAL_EVIDENCE'
      WHEN v_event_source LIKE '%MANUAL%' THEN 'MANUAL_CONFIRM'
      WHEN v_event_source LIKE '%SYSTEM%' THEN 'SYSTEM'
      ELSE 'PROVIDER_POLL'
    END;
  END IF;

  v_provider_event_transport := CASE
    WHEN v_event_source IN ('PROVIDER_RESPONSE', 'PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'FAILED_WEBHOOK_REPLAY', 'MANUAL_CONFIRM') THEN v_event_source
    WHEN v_event_source = 'MANUAL_EVIDENCE' THEN 'MANUAL_CONFIRM'
    WHEN v_event_source = 'SYSTEM' THEN 'LOCAL_STATE'
    ELSE 'LOCAL_STATE'
  END;

  v_event_source := CASE
    WHEN v_provider_event_transport = 'PROVIDER_WEBHOOK' THEN 'PROVIDER_WEBHOOK'
    WHEN v_provider_event_transport = 'FAILED_WEBHOOK_REPLAY' THEN 'PROVIDER_WEBHOOK'
    WHEN v_provider_event_transport = 'PROVIDER_RESPONSE' THEN 'PROVIDER_POLL'
    WHEN v_provider_event_transport = 'PROVIDER_POLL' THEN 'PROVIDER_POLL'
    WHEN COALESCE(v_event_source, '') = 'MANUAL_EVIDENCE' THEN 'MANUAL_EVIDENCE'
    WHEN v_provider_event_transport = 'MANUAL_CONFIRM' THEN 'MANUAL_CONFIRM'
    ELSE 'SYSTEM'
  END;

  IF v_event_source IN ('MANUAL_CONFIRM', 'MANUAL_EVIDENCE')
     AND (
       p_actor_user_id IS NULL
       OR NOT EXISTS (
         SELECT 1
         FROM public.tms_users AS manual_actor
         WHERE manual_actor.id = p_actor_user_id
           AND COALESCE(manual_actor.is_active, false) IS TRUE
       )
     ) THEN
    RAISE EXCEPTION 'BANK_EVENT_MANUAL_ACTOR_REQUIRED'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PERMISSION_DENIED',
        'message', 'An active user is required to record manual payment evidence.'
      )::text;
  END IF;

  IF v_provider_webhook_receipt_id IS NOT NULL THEN
    SELECT receipt_row.status,
           receipt_row.provider_key,
           receipt_row.rail_env,
           receipt_row.provider_event_key,
           receipt_row.signature_valid,
           COALESCE(receipt_row.normalised_events_json, '[]'::jsonb),
           COALESCE(receipt_row.ingest_results_json, '[]'::jsonb)
    INTO v_receipt_status,
         v_receipt_provider_key,
         v_receipt_rail_env,
         v_receipt_provider_event_key,
         v_receipt_signature_valid,
         v_receipt_normalised_events_json,
         v_receipt_ingest_results_json
    FROM public.bank_provider_webhook_receipts AS receipt_row
    WHERE receipt_row.id = v_provider_webhook_receipt_id;
  END IF;

  IF v_provider_event_transport IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY')
     AND v_provider_signature_valid IS NULL
     AND v_receipt_signature_valid IS TRUE THEN
    v_provider_signature_valid := true;
  END IF;

  IF NULLIF(btrim(COALESCE(v_provider_event_key, '')), '') IS NOT NULL THEN
    SELECT EXISTS (
      WITH RECURSIVE receipt_json_walk(json_value) AS (
        SELECT jsonb_build_array(
          to_jsonb(v_receipt_provider_event_key),
          COALESCE(v_receipt_normalised_events_json, '[]'::jsonb),
          COALESCE(v_receipt_ingest_results_json, '[]'::jsonb)
        )
        UNION ALL
        SELECT receipt_json_child.child_value
        FROM receipt_json_walk AS receipt_json_parent
        CROSS JOIN LATERAL (
          SELECT array_child.value AS child_value
          FROM jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(receipt_json_parent.json_value) = 'array' THEN receipt_json_parent.json_value
              ELSE '[]'::jsonb
            END
          ) AS array_child(value)
          UNION ALL
          SELECT object_child.value AS child_value
          FROM jsonb_each(
            CASE
              WHEN jsonb_typeof(receipt_json_parent.json_value) = 'object' THEN receipt_json_parent.json_value
              ELSE '{}'::jsonb
            END
          ) AS object_child(key_name, value)
          UNION ALL
          SELECT to_jsonb(object_child_key.key_name) AS child_value
          FROM jsonb_each(
            CASE
              WHEN jsonb_typeof(receipt_json_parent.json_value) = 'object' THEN receipt_json_parent.json_value
              ELSE '{}'::jsonb
            END
          ) AS object_child_key(key_name, value)
        ) AS receipt_json_child
      )
      SELECT 1
      FROM receipt_json_walk AS receipt_json_match
      WHERE (
        jsonb_typeof(receipt_json_match.json_value) = 'string'
        AND trim(both '"' from receipt_json_match.json_value::text) = v_provider_event_key
      )
    )
    INTO v_provider_event_key_in_receipt;
  ELSE
    v_provider_event_key_in_receipt := false;
  END IF;

  v_webhook_gate_ok := (
    v_provider_event_transport = 'PROVIDER_WEBHOOK'
    AND v_provider_signature_valid IS DISTINCT FROM false
    AND v_provider_webhook_receipt_id IS NOT NULL
    AND v_receipt_status IS NOT NULL
    AND upper(COALESCE(v_receipt_status, '')) IN ('VERIFIED', 'NORMALISED', 'NORMALIZED', 'INGESTED', 'FAILED_RETRYABLE', 'UNMATCHED_REVIEW_REQUIRED')
    AND v_receipt_signature_valid IS TRUE
    AND v_receipt_provider_key IS NOT DISTINCT FROM v_provider_key
    AND v_receipt_rail_env IS NOT DISTINCT FROM v_rail_env
    AND NULLIF(btrim(COALESCE(v_provider_event_key, '')), '') IS NOT NULL
    AND COALESCE(v_provider_event_key_in_receipt, false) IS TRUE
  );

  IF v_provider_event_transport = 'PROVIDER_WEBHOOK' AND COALESCE(v_webhook_gate_ok, false) IS NOT TRUE THEN
    IF v_provider_webhook_receipt_id IS NOT NULL
       AND (v_provider_signature_valid IS FALSE OR COALESCE(v_receipt_signature_valid, false) IS NOT TRUE) THEN
      UPDATE public.bank_provider_webhook_receipts AS receipt_to_update
      SET status = CASE WHEN upper(COALESCE(receipt_to_update.status, '')) = 'SIGNATURE_INVALID' THEN receipt_to_update.status ELSE 'SIGNATURE_INVALID' END,
          error_code = COALESCE(receipt_to_update.error_code, 'WEBHOOK_SIGNATURE_OR_RECEIPT_INVALID'),
          error_message = COALESCE(receipt_to_update.error_message, 'Webhook event was rejected because the webhook signature or receipt signature was invalid.'),
          updated_at_utc = v_now
      WHERE receipt_to_update.id = v_provider_webhook_receipt_id;
    END IF;

    RETURN jsonb_build_object(
      'ok', false,
      'event_id', NULL::uuid,
      'idempotent', false,
      'mapped', false,
      'normalised_state', v_normalised_state,
      'manual_resolution_recorded', false,
      'release_eligible', false,
      'auto_release_request_prepared', false,
      'existing_release_request_id', NULL::uuid,
      'existing_release_operation_woken', false,
      'paid_after_release', false,
      'requires_user_action', true,
      'display_status', 'Provider evidence rejected',
      'display_message', CASE
        WHEN v_provider_signature_valid IS FALSE OR COALESCE(v_receipt_signature_valid, false) IS NOT TRUE THEN 'Webhook event was not ingested because its signature or receipt could not be validated.'
        ELSE 'Webhook event was not ingested because it was not present in the signed receipt.'
      END,
      'ignored', true,
      'code', CASE
        WHEN v_provider_signature_valid IS FALSE OR COALESCE(v_receipt_signature_valid, false) IS NOT TRUE THEN 'WEBHOOK_SIGNATURE_OR_RECEIPT_INVALID'
        ELSE 'WEBHOOK_RECEIPT_EVENT_MEMBERSHIP_INVALID'
      END,
      'provider_event_transport', v_provider_event_transport,
      'provider_webhook_receipt_id', CASE WHEN v_provider_webhook_receipt_id IS NULL THEN NULL ELSE v_provider_webhook_receipt_id::text END,
      'provider_event_key', v_provider_event_key,
      'provider_signature_valid', COALESCE(v_provider_signature_valid, v_receipt_signature_valid, false),
      'receipt_status', v_receipt_status,
      'receipt_signature_valid', COALESCE(v_receipt_signature_valid, false),
      'message', CASE
        WHEN v_provider_signature_valid IS FALSE OR COALESCE(v_receipt_signature_valid, false) IS NOT TRUE THEN 'Webhook event was not ingested as provider evidence because signature/receipt validation failed.'
        ELSE 'Webhook event was not ingested as provider evidence because the event key was not present in the signed webhook receipt.'
      END
    );
  END IF;

  v_replay_gate_ok := (
    v_provider_event_transport = 'FAILED_WEBHOOK_REPLAY'
    AND v_provider_webhook_receipt_id IS NOT NULL
    AND v_receipt_status IS NOT NULL
    AND upper(COALESCE(v_receipt_status, '')) IN ('VERIFIED', 'NORMALISED', 'NORMALIZED', 'INGESTED', 'FAILED_RETRYABLE', 'UNMATCHED_REVIEW_REQUIRED')
    AND v_receipt_signature_valid IS TRUE
    AND v_receipt_provider_key IS NOT DISTINCT FROM v_provider_key
    AND v_receipt_rail_env IS NOT DISTINCT FROM v_rail_env
    AND NULLIF(btrim(COALESCE(v_provider_event_key, '')), '') IS NOT NULL
    AND COALESCE(v_provider_event_key_in_receipt, false) IS TRUE
  );

  IF v_provider_event_transport = 'FAILED_WEBHOOK_REPLAY' AND COALESCE(v_replay_gate_ok, false) IS NOT TRUE THEN
    RETURN jsonb_build_object(
      'ok', false,
      'event_id', NULL::uuid,
      'idempotent', false,
      'mapped', false,
      'normalised_state', v_normalised_state,
      'manual_resolution_recorded', false,
      'release_eligible', false,
      'auto_release_request_prepared', false,
      'existing_release_request_id', NULL::uuid,
      'existing_release_operation_woken', false,
      'paid_after_release', false,
      'requires_user_action', true,
      'display_status', 'Provider evidence rejected',
      'display_message', 'Failed-webhook replay evidence was not ingested because its signed receipt provenance was invalid.',
      'ignored', true,
      'code', 'FAILED_WEBHOOK_REPLAY_RECEIPT_INVALID',
      'provider_event_transport', v_provider_event_transport,
      'provider_webhook_receipt_id', CASE WHEN v_provider_webhook_receipt_id IS NULL THEN NULL ELSE v_provider_webhook_receipt_id::text END,
      'receipt_status', v_receipt_status,
      'message', 'Failed-webhook replay event was not ingested as provider evidence because replay provenance was not valid.'
    );
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    SELECT public.pay_bank_transfers.*
    INTO v_transfer
    FROM public.pay_bank_transfers

    WHERE public.pay_bank_transfers.id = v_pay_bank_transfer_id;

    IF v_transfer.id IS NULL THEN
      RAISE EXCEPTION 'BANK_TRANSFER_NOT_FOUND_FOR_EVENT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANK_TRANSFER_NOT_FOUND_FOR_EVENT', 'pay_bank_transfer_id', v_pay_bank_transfer_id)::text;
    END IF;

    IF v_supplied_pay_batch_id IS NOT NULL AND v_supplied_pay_batch_id <> v_transfer.pay_batch_id THEN
      RAISE EXCEPTION 'BANK_EVENT_TRANSFER_BATCH_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANK_EVENT_TRANSFER_BATCH_MISMATCH',
                'supplied_pay_batch_id', v_supplied_pay_batch_id,
                'transfer_pay_batch_id', v_transfer.pay_batch_id,
                'pay_bank_transfer_id', v_transfer.id
              )::text;
    END IF;

    v_pay_batch_id := v_transfer.pay_batch_id;
    v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
    v_umbrella_id := COALESCE(
      v_umbrella_id,
      v_transfer.umbrella_id,
      CASE
        WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
        ELSE NULL::uuid
      END
    );
    v_mapping_method := CASE
      WHEN v_event_source = 'MANUAL_EVIDENCE' THEN 'MANUAL_TRANSFER_SELECTION'
      ELSE 'TRANSFER_ID'
    END;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_provider_transaction_id IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers AS transfer_match
    WHERE (
      transfer_match.rail_tx_id = v_provider_transaction_id
      OR transfer_match.rail_meta_json #>> '{provider_transaction_id}' = v_provider_transaction_id
      OR transfer_match.rail_meta_json #>> '{transaction_id}' = v_provider_transaction_id
      OR transfer_match.rail_meta_json #>> '{provider_payment_id}' = v_provider_transaction_id
      OR transfer_match.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}' = v_provider_transaction_id
      OR transfer_match.rail_meta_json #>> '{id}' = v_provider_transaction_id
    )
      AND (v_pay_batch_id IS NULL OR transfer_match.pay_batch_id = v_pay_batch_id);

    IF v_mapping_candidate_count = 1 THEN
      SELECT transfer_match.*
      INTO v_transfer
      FROM public.pay_bank_transfers AS transfer_match
      WHERE (
        transfer_match.rail_tx_id = v_provider_transaction_id
        OR transfer_match.rail_meta_json #>> '{provider_transaction_id}' = v_provider_transaction_id
        OR transfer_match.rail_meta_json #>> '{transaction_id}' = v_provider_transaction_id
        OR transfer_match.rail_meta_json #>> '{provider_payment_id}' = v_provider_transaction_id
        OR transfer_match.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}' = v_provider_transaction_id
        OR transfer_match.rail_meta_json #>> '{id}' = v_provider_transaction_id
      )
        AND (v_pay_batch_id IS NULL OR transfer_match.pay_batch_id = v_pay_batch_id)
      ORDER BY transfer_match.created_at_utc DESC, transfer_match.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;
      v_pay_batch_id := COALESCE(v_pay_batch_id, v_transfer.pay_batch_id);
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(v_umbrella_id, v_transfer.umbrella_id, CASE WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id ELSE NULL::uuid END);
      v_mapping_method := 'PROVIDER_TRANSACTION_ID';
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_provider_request_id IS NOT NULL AND v_provider_event_transport IN ('PROVIDER_RESPONSE','PROVIDER_POLL','PROVIDER_WEBHOOK','FAILED_WEBHOOK_REPLAY') THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers AS transfer_match
    WHERE (
        transfer_match.request_id = v_provider_request_id
        OR transfer_match.rail_meta_json #>> '{provider_submit_diagnostic,request_id}' = v_provider_request_id
        OR transfer_match.rail_meta_json #>> '{provider_submit_diagnostic,local_provider_request_id}' = v_provider_request_id
      )
      AND (v_pay_batch_id IS NULL OR transfer_match.pay_batch_id = v_pay_batch_id);

    IF v_mapping_candidate_count = 1 THEN
      SELECT transfer_match.*
      INTO v_transfer
      FROM public.pay_bank_transfers AS transfer_match
      WHERE (
          transfer_match.request_id = v_provider_request_id
          OR transfer_match.rail_meta_json #>> '{provider_submit_diagnostic,request_id}' = v_provider_request_id
          OR transfer_match.rail_meta_json #>> '{provider_submit_diagnostic,local_provider_request_id}' = v_provider_request_id
        )
        AND (v_pay_batch_id IS NULL OR transfer_match.pay_batch_id = v_pay_batch_id)
      ORDER BY transfer_match.created_at_utc DESC, transfer_match.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;
      v_pay_batch_id := COALESCE(v_pay_batch_id, v_transfer.pay_batch_id);
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(v_umbrella_id, v_transfer.umbrella_id, CASE WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id ELSE NULL::uuid END);
      v_mapping_method := 'REQUEST_ID';
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_provider_reference IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers
    WHERE (
      public.pay_bank_transfers.request_id = v_provider_reference
      OR public.pay_bank_transfers.rail_tx_id = v_provider_reference
      OR public.pay_bank_transfers.payment_reference = v_provider_reference
    )
      AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id);

    IF v_mapping_candidate_count = 1 THEN
      SELECT public.pay_bank_transfers.*
      INTO v_transfer
      FROM public.pay_bank_transfers
      WHERE (
        public.pay_bank_transfers.request_id = v_provider_reference
        OR public.pay_bank_transfers.rail_tx_id = v_provider_reference
        OR public.pay_bank_transfers.payment_reference = v_provider_reference
      )
        AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id)
      ORDER BY public.pay_bank_transfers.created_at_utc DESC, public.pay_bank_transfers.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;
      v_pay_batch_id := COALESCE(v_pay_batch_id, v_transfer.pay_batch_id);
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(
        v_umbrella_id,
        v_transfer.umbrella_id,
        CASE
          WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
          ELSE NULL::uuid
        END
      );
      v_mapping_method := CASE
        WHEN v_transfer.request_id = v_provider_reference THEN 'REQUEST_ID'
        WHEN v_transfer.rail_tx_id = v_provider_reference THEN 'RAIL_TX_ID'
        WHEN v_transfer.payment_reference = v_provider_reference THEN 'PAYMENT_REFERENCE'
        ELSE 'PROVIDER_REFERENCE'
      END;
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_provider_event_id IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers
    WHERE (
      public.pay_bank_transfers.request_id = v_provider_event_id
      OR public.pay_bank_transfers.rail_tx_id = v_provider_event_id
      OR public.pay_bank_transfers.payment_reference = v_provider_event_id
    )
      AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id);

    IF v_mapping_candidate_count = 1 THEN
      SELECT public.pay_bank_transfers.*
      INTO v_transfer
      FROM public.pay_bank_transfers
      WHERE (
        public.pay_bank_transfers.request_id = v_provider_event_id
        OR public.pay_bank_transfers.rail_tx_id = v_provider_event_id
        OR public.pay_bank_transfers.payment_reference = v_provider_event_id
      )
        AND (v_pay_batch_id IS NULL OR public.pay_bank_transfers.pay_batch_id = v_pay_batch_id)
      ORDER BY public.pay_bank_transfers.created_at_utc DESC, public.pay_bank_transfers.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;

      v_pay_batch_id := COALESCE(v_pay_batch_id, v_transfer.pay_batch_id);
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(
        v_umbrella_id,
        v_transfer.umbrella_id,
        CASE
          WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
          ELSE NULL::uuid
        END
      );
      v_mapping_method := 'PROVIDER_EVENT_ID';
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NULL
     AND (
       v_provider_transaction_id IS NOT NULL
       OR v_provider_request_id IS NOT NULL
       OR v_provider_reference IS NOT NULL
     ) THEN
    SELECT count(DISTINCT event_match.pay_bank_transfer_id)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfer_events AS event_match
    JOIN public.pay_bank_transfers AS transfer_match
      ON transfer_match.id = event_match.pay_bank_transfer_id
    WHERE event_match.pay_bank_transfer_id IS NOT NULL
      AND upper(COALESCE(event_match.mapping_status, '')) = 'MATCHED'
      AND upper(COALESCE(event_match.mapping_method, '')) IN (
        'TRANSFER_ID',
        'PROVIDER_EVENT_ID',
        'PROVIDER_TRANSACTION_ID',
        'REQUEST_ID',
        'PROVIDER_REFERENCE',
        'RAIL_TX_ID',
        'MATCHED_PROVIDER_EVENT',
        'MANUAL_TRANSFER_SELECTION'
      )
      AND (
        event_match.provider_transaction_id = v_provider_transaction_id
        OR event_match.provider_request_id = v_provider_request_id
        OR event_match.provider_reference = v_provider_transaction_id
        OR event_match.provider_reference = v_provider_request_id
        OR event_match.provider_transaction_id = v_provider_reference
        OR event_match.provider_request_id = v_provider_reference
      )
      AND (
        v_pay_batch_id IS NULL
        OR event_match.pay_batch_id = v_pay_batch_id
        OR transfer_match.pay_batch_id = v_pay_batch_id
      );

    IF v_mapping_candidate_count = 1 THEN
      SELECT transfer_match.*
      INTO v_transfer
      FROM public.pay_bank_transfer_events AS event_match
      JOIN public.pay_bank_transfers AS transfer_match
        ON transfer_match.id = event_match.pay_bank_transfer_id
      WHERE event_match.pay_bank_transfer_id IS NOT NULL
        AND upper(COALESCE(event_match.mapping_status, '')) = 'MATCHED'
        AND upper(COALESCE(event_match.mapping_method, '')) IN (
          'TRANSFER_ID',
          'PROVIDER_EVENT_ID',
          'PROVIDER_TRANSACTION_ID',
          'REQUEST_ID',
          'PROVIDER_REFERENCE',
          'RAIL_TX_ID',
          'MATCHED_PROVIDER_EVENT',
          'MANUAL_TRANSFER_SELECTION'
        )
        AND (
          event_match.provider_transaction_id = v_provider_transaction_id
          OR event_match.provider_request_id = v_provider_request_id
          OR event_match.provider_reference = v_provider_transaction_id
          OR event_match.provider_reference = v_provider_request_id
          OR event_match.provider_transaction_id = v_provider_reference
          OR event_match.provider_request_id = v_provider_reference
        )
        AND (
          v_pay_batch_id IS NULL
          OR event_match.pay_batch_id = v_pay_batch_id
          OR transfer_match.pay_batch_id = v_pay_batch_id
        )
      ORDER BY event_match.received_at_utc DESC NULLS LAST,
               event_match.event_time_utc DESC NULLS LAST,
               event_match.created_at_utc DESC NULLS LAST,
               event_match.id DESC
      LIMIT 1;

      IF v_transfer.id IS NOT NULL THEN
        v_pay_bank_transfer_id := v_transfer.id;
        v_pay_batch_id := COALESCE(v_pay_batch_id, v_transfer.pay_batch_id);
        v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
        v_umbrella_id := COALESCE(
          v_umbrella_id,
          v_transfer.umbrella_id,
          CASE
            WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
            ELSE NULL::uuid
          END
        );
        v_mapping_method := 'MATCHED_PROVIDER_EVENT';
      ELSE
        v_mapping_candidate_count := 0;
        v_mapping_method := 'UNMATCHED';
      END IF;
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NULL AND v_pay_batch_id IS NOT NULL AND v_amount IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_mapping_candidate_count
    FROM public.pay_bank_transfers
    WHERE public.pay_bank_transfers.pay_batch_id = v_pay_batch_id
      AND abs(COALESCE(public.pay_bank_transfers.amount, 0) - COALESCE(v_amount, 0)) <= 0.01;

    IF v_mapping_candidate_count = 1 THEN
      SELECT public.pay_bank_transfers.*
      INTO v_transfer
      FROM public.pay_bank_transfers
      WHERE public.pay_bank_transfers.pay_batch_id = v_pay_batch_id
        AND abs(COALESCE(public.pay_bank_transfers.amount, 0) - COALESCE(v_amount, 0)) <= 0.01
      ORDER BY public.pay_bank_transfers.created_at_utc DESC, public.pay_bank_transfers.id DESC
      LIMIT 1;

      v_pay_bank_transfer_id := v_transfer.id;
      v_candidate_id := COALESCE(v_candidate_id, v_transfer.candidate_id);
      v_umbrella_id := COALESCE(
        v_umbrella_id,
        v_transfer.umbrella_id,
        CASE
          WHEN upper(COALESCE(v_transfer.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN v_transfer.payee_entity_id
          ELSE NULL::uuid
        END
      );
      v_mapping_method := 'AMOUNT_ONLY_UNIQUE';
    ELSIF v_mapping_candidate_count > 1 THEN
      v_mapping_method := 'AMBIGUOUS';
    END IF;
  END IF;

  IF v_pay_batch_id IS NULL
     AND v_provider_event_transport IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY')
     AND v_provider_webhook_receipt_id IS NOT NULL THEN
    UPDATE public.bank_provider_webhook_receipts AS receipt_to_update
    SET
      status = 'UNMATCHED_REVIEW_REQUIRED',
      error_code = COALESCE(receipt_to_update.error_code, 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED'),
      error_message = COALESCE(receipt_to_update.error_message, 'Provider webhook/replay could not be matched to a CloudTMS pay batch or transfer; retained for manual review.'),
      updated_at_utc = v_now
    WHERE receipt_to_update.id = v_provider_webhook_receipt_id;

    RETURN jsonb_build_object(
      'ok', true,
      'event_id', NULL::uuid,
      'idempotent', false,
      'mapped', false,
      'normalised_state', v_normalised_state,
      'manual_resolution_recorded', v_manual_confirmed_not_paid,
      'release_eligible', false,
      'auto_release_request_prepared', false,
      'existing_release_request_id', NULL::uuid,
      'existing_release_operation_woken', false,
      'paid_after_release', false,
      'requires_user_action', true,
      'display_status', 'Provider evidence needs review',
      'display_message', 'The provider evidence was retained but could not be matched to a payment.',
      'ignored', true,
      'mapping_status', 'UNMATCHED',
      'mapping_method', 'UNMATCHED',
      'correction_disposition', 'UNMATCHED_REVIEW_REQUIRED',
      'code', 'BANK_EVENT_PAY_BATCH_ID_COULD_NOT_BE_RESOLVED_RETAINED_FOR_REVIEW',
      'provider_event_transport', v_provider_event_transport,
      'provider_webhook_receipt_id', v_provider_webhook_receipt_id::text,
      'provider_event_key', v_provider_event_key,
      'provider_transaction_id', v_provider_transaction_id,
      'provider_request_id', v_provider_request_id,
      'message', 'Provider webhook/replay was retained for review because no CloudTMS pay batch or transfer could be resolved.'
    );
  END IF;

  IF v_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'BANK_EVENT_PAY_BATCH_ID_COULD_NOT_BE_RESOLVED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANK_EVENT_PAY_BATCH_ID_COULD_NOT_BE_RESOLVED',
              'pay_bank_transfer_id', v_pay_bank_transfer_id,
              'provider_reference', v_provider_reference
            )::text;
  END IF;


  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANK_EVENT_PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANK_EVENT_PAY_BATCH_NOT_FOUND', 'pay_batch_id', v_pay_batch_id)::text;
  END IF;

  v_mutation_guard := private.pay_payment_mutation_guard_v1(
    v_pay_batch_id,
    NULL::uuid,
    'AUTHORITATIVE_EVENT'
  );

  IF COALESCE((v_mutation_guard->>'ok')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_mutation_guard->>'code', 'PAYMENT_MUTATION_LOCK_TIMEOUT')
      USING ERRCODE = 'P0001', DETAIL = v_mutation_guard::text;
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    v_mapping_status := 'MATCHED';
    IF v_mapping_method IS NULL OR v_mapping_method = 'UNMATCHED' THEN
      v_mapping_method := 'TRANSFER_ID';
    END IF;
  ELSIF v_mapping_candidate_count > 1 THEN
    v_mapping_status := 'AMBIGUOUS';
    v_mapping_method := 'AMBIGUOUS';
  ELSE
    v_mapping_status := 'UNMATCHED';
    v_mapping_method := 'UNMATCHED';
  END IF;

  IF v_mapping_status = 'UNMATCHED'
     AND (
       COALESCE(v_event_json->>'legacy_no_artifact', 'false') IN ('true', 't', 'yes', 'y', '1')
       OR NOT EXISTS (
         SELECT 1
         FROM public.pay_bank_transfers AS legacy_artifact_check
         WHERE legacy_artifact_check.pay_batch_id = v_pay_batch_id
       )
     ) THEN
    v_mapping_status := 'LEGACY_NO_ARTIFACT';
    v_mapping_method := 'LEGACY_NO_ARTIFACT';
  END IF;

  IF v_provider_event_transport IN ('PROVIDER_WEBHOOK', 'FAILED_WEBHOOK_REPLAY')
     AND v_provider_webhook_receipt_id IS NOT NULL
     AND v_mapping_status IN ('UNMATCHED', 'AMBIGUOUS', 'LEGACY_NO_ARTIFACT') THEN
    UPDATE public.bank_provider_webhook_receipts AS receipt_to_update
    SET
      status = 'UNMATCHED_REVIEW_REQUIRED',
      error_code = COALESCE(receipt_to_update.error_code, 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED'),
      error_message = COALESCE(receipt_to_update.error_message, 'Provider webhook/replay was verified but could not be strongly matched to a CloudTMS transfer; manual review is required.'),
      updated_at_utc = v_now
    WHERE receipt_to_update.id = v_provider_webhook_receipt_id;
  END IF;

  v_has_strong_transfer_mapping := (
    v_mapping_status = 'MATCHED'
    AND v_mapping_method IN (
      'TRANSFER_ID',
      'PROVIDER_EVENT_ID',
      'PROVIDER_TRANSACTION_ID',
      'REQUEST_ID',
      'PROVIDER_REFERENCE',
      'RAIL_TX_ID',
      'MATCHED_PROVIDER_EVENT',
      'MANUAL_TRANSFER_SELECTION'
    )
  );

  v_pre_insert_pay_batch_id := v_pay_batch_id;
  v_pre_insert_pay_bank_transfer_id := v_pay_bank_transfer_id;
  v_pre_insert_candidate_id := v_candidate_id;
  v_pre_insert_umbrella_id := v_umbrella_id;
  v_pre_insert_mapping_status := v_mapping_status;
  v_pre_insert_mapping_method := v_mapping_method;

  v_idempotency_key := nullif(btrim(COALESCE(v_event_json->>'idempotency_key', v_provider_event_key, '')), '');

  IF v_idempotency_key IS NULL THEN
    v_idempotency_key := CASE
      WHEN v_provider_key IS NOT NULL AND v_provider_event_id IS NOT NULL
        THEN v_provider_key || '|' || v_provider_event_id
      WHEN v_provider_key IS NOT NULL AND v_provider_reference IS NOT NULL
        THEN v_provider_key || '|' || v_provider_reference || '|' || v_normalised_state || '|' || COALESCE(v_amount::text, '') || '|' || COALESCE(v_event_time_utc::text, '')
      WHEN v_event_source IN ('MANUAL_CONFIRM', 'MANUAL_EVIDENCE')
        THEN 'MANUAL|' || v_pay_batch_id::text || '|' || COALESCE(v_pay_bank_transfer_id::text, 'NO_TRANSFER') || '|' || v_normalised_state || '|' || COALESCE(p_actor_user_id::text, 'SYSTEM') || '|' || COALESCE(v_event_time_utc::text, v_now::text)
      ELSE 'SYSTEM|' || v_pay_batch_id::text || '|' || COALESCE(v_pay_bank_transfer_id::text, 'NO_TRANSFER') || '|' || v_normalised_state || '|' || md5(v_event_json::text)
    END;
  END IF;

  INSERT INTO public.pay_bank_transfer_events(
    pay_batch_id,
    pay_bank_transfer_id,
    candidate_id,
    umbrella_id,
    provider_key,
    provider_event_id,
    provider_reference,
    provider_state,
    normalised_state,
    event_source,
    event_time_utc,
    received_at_utc,
    amount,
    currency,
    mapping_status,
    mapping_method,
    movement_classification,
    correction_disposition,
    raw_payload,
    idempotency_key,
    provider_webhook_receipt_id,
    provider_event_type,
    provider_transaction_id,
    provider_request_id,
    provider_event_key,
    provider_signature_valid,
    provider_event_transport,
    adapter_key,
    adapter_version,
    rail_env,
    provider_failure_reason_code,
    provider_failure_reason_group,
    mapping_hints_json,
    created_at_utc
  )
  VALUES (
    v_pay_batch_id,
    v_pay_bank_transfer_id,
    v_candidate_id,
    v_umbrella_id,
    v_provider_key,
    v_provider_event_id,
    v_provider_reference,
    v_provider_state,
    v_normalised_state,
    v_event_source,
    v_event_time_utc,
    v_now,
    v_amount,
    COALESCE(v_currency, 'GBP'),
    v_mapping_status,
    v_mapping_method,
    NULL::text,
    NULL::text,
    COALESCE(v_raw_payload, '{}'::jsonb),
    v_idempotency_key,
    v_provider_webhook_receipt_id,
    v_provider_event_type,
    v_provider_transaction_id,
    v_provider_request_id,
    v_provider_event_key,
    v_provider_signature_valid,
    v_provider_event_transport,
    v_adapter_key,
    v_adapter_version,
    v_rail_env,
    v_provider_failure_reason_code,
    v_provider_failure_reason_group,
    v_mapping_hints_json,
    v_now
  )
  ON CONFLICT DO NOTHING
  RETURNING public.pay_bank_transfer_events.id
  INTO v_event_id;

  v_inserted_event := v_event_id IS NOT NULL;

  IF v_event_id IS NULL THEN
    SELECT public.pay_bank_transfer_events.id,
           public.pay_bank_transfer_events.mapping_status,
           public.pay_bank_transfer_events.mapping_method,
           public.pay_bank_transfer_events.movement_classification,
           public.pay_bank_transfer_events.correction_disposition,
           public.pay_bank_transfer_events.pay_bank_transfer_id,
           public.pay_bank_transfer_events.pay_batch_id,
           public.pay_bank_transfer_events.candidate_id,
           public.pay_bank_transfer_events.umbrella_id
    INTO v_event_id,
         v_mapping_status,
         v_mapping_method,
         v_classification,
         v_correction_disposition,
         v_pay_bank_transfer_id,
         v_pay_batch_id,

         v_candidate_id,
         v_umbrella_id
    FROM public.pay_bank_transfer_events
    WHERE public.pay_bank_transfer_events.idempotency_key = v_idempotency_key
       OR (
         v_provider_event_key IS NOT NULL
         AND public.pay_bank_transfer_events.provider_key IS NOT DISTINCT FROM v_provider_key
         AND public.pay_bank_transfer_events.rail_env IS NOT DISTINCT FROM v_rail_env
         AND public.pay_bank_transfer_events.provider_event_transport IS NOT DISTINCT FROM v_provider_event_transport
         AND public.pay_bank_transfer_events.provider_event_key = v_provider_event_key
       )
    ORDER BY CASE WHEN public.pay_bank_transfer_events.idempotency_key = v_idempotency_key THEN 0 ELSE 1 END,
             public.pay_bank_transfer_events.received_at_utc DESC NULLS LAST,
             public.pay_bank_transfer_events.id DESC
    LIMIT 1;

    v_duplicate_event_should_continue := false;

    IF v_event_id IS NOT NULL
       AND v_pre_insert_mapping_status = 'MATCHED'
       AND v_pre_insert_pay_batch_id IS NOT NULL
       AND v_pre_insert_pay_bank_transfer_id IS NOT NULL
       AND v_pre_insert_mapping_method IN (
         'TRANSFER_ID',
         'PROVIDER_EVENT_ID',
         'PROVIDER_TRANSACTION_ID',
         'REQUEST_ID',
         'PROVIDER_REFERENCE',
         'RAIL_TX_ID',
         'MATCHED_PROVIDER_EVENT',
         'MANUAL_TRANSFER_SELECTION'
       )
       AND (
         v_mapping_status IS DISTINCT FROM 'MATCHED'
         OR v_pay_bank_transfer_id IS NULL
         OR v_mapping_method NOT IN (
           'TRANSFER_ID',
           'PROVIDER_EVENT_ID',
           'PROVIDER_TRANSACTION_ID',
           'REQUEST_ID',
           'PROVIDER_REFERENCE',
           'RAIL_TX_ID',
           'MATCHED_PROVIDER_EVENT',
           'MANUAL_TRANSFER_SELECTION'
         )
       ) THEN
      UPDATE public.pay_bank_transfer_events AS existing_event_to_upgrade
      SET
        pay_batch_id = v_pre_insert_pay_batch_id,
        pay_bank_transfer_id = v_pre_insert_pay_bank_transfer_id,
        candidate_id = COALESCE(existing_event_to_upgrade.candidate_id, v_pre_insert_candidate_id),
        umbrella_id = COALESCE(existing_event_to_upgrade.umbrella_id, v_pre_insert_umbrella_id),
        mapping_status = 'MATCHED',
        mapping_method = v_pre_insert_mapping_method,
        correction_disposition = NULL::text
      WHERE existing_event_to_upgrade.id = v_event_id
      RETURNING
        existing_event_to_upgrade.mapping_status,
        existing_event_to_upgrade.mapping_method,
        existing_event_to_upgrade.movement_classification,
        existing_event_to_upgrade.correction_disposition,
        existing_event_to_upgrade.pay_bank_transfer_id,
        existing_event_to_upgrade.pay_batch_id,
        existing_event_to_upgrade.candidate_id,
        existing_event_to_upgrade.umbrella_id
      INTO
        v_mapping_status,
        v_mapping_method,
        v_classification,
        v_correction_disposition,
        v_pay_bank_transfer_id,
        v_pay_batch_id,
        v_candidate_id,
        v_umbrella_id;

      v_duplicate_event_should_continue := true;
      v_inserted_event := false;
    END IF;

    v_has_strong_transfer_mapping := (
      v_mapping_status = 'MATCHED'
      AND v_pay_bank_transfer_id IS NOT NULL
      AND v_mapping_method IN (
        'TRANSFER_ID',
        'PROVIDER_EVENT_ID',
        'PROVIDER_TRANSACTION_ID',
        'REQUEST_ID',
        'PROVIDER_REFERENCE',
        'RAIL_TX_ID',
        'MATCHED_PROVIDER_EVENT',
        'MANUAL_TRANSFER_SELECTION'
      )
    );

    SELECT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS released_item
      WHERE released_item.status = 'APPLIED'
        AND released_item.correction_item_kind IN (
          'PRE_BANK_CANCEL', 'NO_MONEY_UNWIND'
        )
        AND (
          (
            v_pay_bank_transfer_id IS NOT NULL
            AND released_item.pay_bank_transfer_id = v_pay_bank_transfer_id
          )
          OR (
            v_pay_bank_transfer_id IS NULL
            AND v_candidate_id IS NOT NULL
            AND released_item.candidate_id = v_candidate_id
          )
        )
    ) AND COALESCE(v_event_is_final_paid, false)
    INTO v_paid_after_release;

    IF COALESCE(v_duplicate_event_should_continue, false) IS NOT TRUE
       AND COALESCE(v_review_acknowledgement, false) IS NOT TRUE
       AND COALESCE(v_paid_after_release, false) IS NOT TRUE THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_BANK_EVENT_INGEST_IDEMPOTENT_EXISTING',
      jsonb_build_object(
        'event_id', v_event_id,
        'idempotency_key', v_idempotency_key,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'classification', v_classification,
        'correction_disposition', v_correction_disposition
      ),
      'pay_payment_correction',
      COALESCE(v_event_id::text, v_pay_batch_id::text),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );


    v_terminal_bank_event := COALESCE(v_has_strong_transfer_mapping, false)
      AND (
        COALESCE(v_event_is_final_paid, false)
        OR COALESCE(v_event_is_terminal_no_money, false)
        OR v_normalised_state IN ('COMPLETED', 'FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED', 'REVERSED')
      );

    v_settlement_required := false;
    v_settlement_trigger := CASE
      WHEN COALESCE(v_has_strong_transfer_mapping, false) IS NOT TRUE THEN 'BANK_EVENT_UNMATCHED_OR_WEAK'
      WHEN COALESCE(v_terminal_bank_event, false) THEN 'BANK_EVENT_TERMINAL_TRANSFER_ONLY'
      ELSE 'BANK_EVENT_NON_TERMINAL'
    END;
    v_all_batch_transfers_terminal := false;
    v_terminal_success_transfer_count := 0;
    v_terminal_failed_transfer_count := 0;
    v_pending_transfer_count := 0;
    v_unknown_transfer_count := 0;
    v_pending_or_unknown_transfer_count := 0;

    IF v_pay_batch_id IS NOT NULL AND COALESCE(v_terminal_bank_event, false) THEN
      SELECT
        COALESCE(COUNT(*) FILTER (WHERE terminality_rows.is_success_terminal), 0)::integer,
        COALESCE(COUNT(*) FILTER (WHERE terminality_rows.is_failed_terminal), 0)::integer,
        COALESCE(COUNT(*) FILTER (WHERE terminality_rows.is_pending_non_terminal), 0)::integer,
        COALESCE(COUNT(*) FILTER (
          WHERE terminality_rows.is_success_terminal IS NOT TRUE
            AND terminality_rows.is_failed_terminal IS NOT TRUE
            AND terminality_rows.is_pending_non_terminal IS NOT TRUE
        ), 0)::integer
      INTO
        v_terminal_success_transfer_count,
        v_terminal_failed_transfer_count,
        v_pending_transfer_count,
        v_unknown_transfer_count
      FROM (
        SELECT
          transfer_row.id AS pay_bank_transfer_id,
          (
            COALESCE(transfer_class.is_final_money_moved, false)
            OR upper(btrim(COALESCE(transfer_row.status, ''))) IN ('COMPLETED', 'COMPLETE', 'PAID', 'SETTLED', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
          ) AS is_success_terminal,
          (
            COALESCE(transfer_class.is_terminal_no_money, false)
            OR upper(btrim(COALESCE(transfer_row.status, ''))) IN ('FAILED', 'FAILURE', 'REJECTED', 'DECLINED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED', 'REVERSED')
          ) AS is_failed_terminal,
          (
            COALESCE(transfer_class.is_pending_non_final, false)
            OR upper(btrim(COALESCE(transfer_row.status, ''))) IN ('PENDING', 'SUBMITTED', 'QUEUED', 'ACCEPTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'WAITING', 'WAITING_BANK_CONFIRM')
          ) AS is_pending_non_terminal
        FROM public.pay_bank_transfers AS transfer_row
        LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
          transfer_row.status,
          transfer_row.rail_state,
          COALESCE(transfer_row.rail_meta_json, '{}'::jsonb),
          COALESCE(transfer_row.rail_meta_json, '{}'::jsonb)
        ) AS transfer_class ON true
        WHERE transfer_row.pay_batch_id = v_pay_batch_id
      ) AS terminality_rows;

      v_pending_or_unknown_transfer_count := COALESCE(v_pending_transfer_count, 0) + COALESCE(v_unknown_transfer_count, 0);
      v_all_batch_transfers_terminal := (
        COALESCE(v_terminal_success_transfer_count, 0) + COALESCE(v_terminal_failed_transfer_count, 0) > 0
        AND COALESCE(v_pending_or_unknown_transfer_count, 0) = 0
      );

      v_settlement_required := COALESCE(v_all_batch_transfers_terminal, false);
      v_settlement_trigger := CASE
        WHEN COALESCE(v_settlement_required, false) THEN 'BANK_EVENT_TERMINAL_BATCH_READY'
        ELSE 'BANK_EVENT_TERMINAL_TRANSFER_ONLY'
      END;
    END IF;

    v_settlement_intent_json := jsonb_strip_nulls(jsonb_build_object(
      'settlement_required', COALESCE(v_settlement_required, false),
      'settlement_trigger', v_settlement_trigger,
      'pay_batch_id', CASE WHEN v_pay_batch_id IS NULL THEN NULL ELSE v_pay_batch_id::text END,
      'pay_bank_transfer_id', CASE WHEN v_pay_bank_transfer_id IS NULL THEN NULL ELSE v_pay_bank_transfer_id::text END,
      'normalised_state', v_normalised_state,
      'cash_state', v_event_cash_state,
      'all_batch_transfers_terminal', COALESCE(v_all_batch_transfers_terminal, false),
      'terminal_success_transfer_count', COALESCE(v_terminal_success_transfer_count, 0),
      'terminal_failed_transfer_count', COALESCE(v_terminal_failed_transfer_count, 0),
      'pending_or_unknown_transfer_count', COALESCE(v_pending_or_unknown_transfer_count, 0),
      'unknown_transfer_count', COALESCE(v_unknown_transfer_count, 0)
    ));

    v_signal_recommendation_json := jsonb_build_object(
      'pay_batch_id', CASE WHEN v_pay_batch_id IS NULL THEN NULL ELSE v_pay_batch_id::text END,
      'touch_payment_status', false,
      'touch_correction_progress', false,
      'touch_alerts', false,
      'touch_overview', false,
      'change_reason', 'BANK_EVENT_DUPLICATE_NO_SIGNAL',
      'change_source', 'pay_bank_event_ingest',
      'changed_transfer_ids', '[]'::jsonb,
      'changed_candidate_ids', '[]'::jsonb,
      'changed_pay_batch_item_ids', '[]'::jsonb,
      'alert_candidate_kind', NULL::text,
      'alert_candidate_is_success_only', false,
      'provider_failure_reason_code', v_provider_failure_reason_code,
      'provider_failure_reason_group', v_provider_failure_reason_group,
      'source_delivery_id', v_source_delivery_id,
      'source_batch_event_group_key', v_source_batch_event_group_key,
      'change_scope_json', v_settlement_intent_json
    ) || v_settlement_intent_json;

    v_live_signal_result := jsonb_build_object(
      'ok', true,
      'changed', false,
      'duplicate_event', true,
      'reason', 'Bank event already recorded; live signal was not touched.',
      'signal_recommendation_json', v_signal_recommendation_json
    );

    RETURN jsonb_build_object(
      'ok', true,
      'event_id', v_event_id,
      'idempotent', true,
      'mapped', v_mapping_status = 'MATCHED',
      'normalised_state', v_normalised_state,
      'manual_resolution_recorded', v_manual_confirmed_not_paid,
      'release_eligible', v_manual_confirmed_not_paid,
      'auto_release_request_prepared', false,
      'existing_release_request_id', NULL::uuid,
      'existing_release_operation_woken', false,
      'paid_after_release', v_paid_after_release,
      'requires_user_action', v_paid_after_release OR v_mapping_status <> 'MATCHED',
      'display_status', CASE WHEN v_paid_after_release THEN 'Paid — evidence received after release' ELSE 'Payment status already recorded' END,
      'display_message', CASE
        WHEN v_paid_after_release THEN 'The bank confirmed this payment after CloudTMS released its payment reservation. CloudTMS has blocked any further payment action and retained both histories for Finance review.'
        ELSE 'This payment status was already recorded.'
      END,
      'inserted', false,
      'mapping_status', v_mapping_status,
      'mapping_method', v_mapping_method,
      'classification', COALESCE(v_classification, 'UNKNOWN'),
      'correction_disposition', COALESCE(v_correction_disposition, 'ALREADY_RECORDED'),
      'correction_request_id', NULL::uuid,
      'admin_notice_group_id', NULL::uuid,
      'live_signal', v_live_signal_result,
      'signal_recommendation_json', v_signal_recommendation_json
    ) || v_settlement_intent_json;
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    SELECT public.pay_bank_transfers.*
    INTO v_transfer
    FROM public.pay_bank_transfers
    WHERE public.pay_bank_transfers.id = v_pay_bank_transfer_id
    FOR UPDATE;

    IF v_transfer.id IS NOT NULL THEN
      SELECT COALESCE(existing_transfer_classification.is_final_money_moved, false)
      INTO v_existing_transfer_is_final_paid
      FROM public._pay_rail_state_money_movement_classify(
        v_transfer.status,
        v_transfer.rail_state,
        COALESCE(v_transfer.rail_meta_json, '{}'::jsonb),
        COALESCE(v_transfer.rail_meta_json, '{}'::jsonb)
      ) AS existing_transfer_classification
      LIMIT 1;

      v_existing_transfer_is_final_paid := COALESCE(v_existing_transfer_is_final_paid, false);
    ELSE
      v_existing_transfer_is_final_paid := false;
    END IF;
  ELSE
    v_existing_transfer_is_final_paid := false;
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL AND COALESCE(v_has_strong_transfer_mapping, false) THEN
    IF v_normalised_state = 'COMPLETED' THEN
      UPDATE public.pay_bank_transfers AS transfer_to_update
      SET
        status = 'COMPLETED',
        completed_at_utc = COALESCE(transfer_to_update.completed_at_utc, v_event_time_utc, v_now),
        rail_state = COALESCE(v_provider_state, transfer_to_update.rail_state),
        rail_tx_id = COALESCE(NULLIF(v_provider_transaction_id, ''), NULLIF(v_provider_reference, ''), transfer_to_update.rail_tx_id),
        rail_meta_json = COALESCE(transfer_to_update.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'latest_bank_event_id', v_event_id,
          'latest_bank_event_state', v_normalised_state,
          'latest_bank_event_at_utc', v_now
        )
      WHERE transfer_to_update.id = v_pay_bank_transfer_id;
    ELSIF COALESCE(v_existing_transfer_is_final_paid, false) THEN
      UPDATE public.pay_bank_transfers AS transfer_to_update
      SET
        rail_meta_json = COALESCE(transfer_to_update.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'latest_bank_event_id', v_event_id,
          'latest_bank_event_state', v_normalised_state,
          'latest_bank_event_at_utc', v_now,
          'ignored_after_final_paid', true,
          'ignored_after_final_paid_reason', 'Existing transfer already has final-paid evidence; non-final or no-money event must not downgrade settlement evidence.'
        )
      WHERE transfer_to_update.id = v_pay_bank_transfer_id;
    ELSIF v_event_is_terminal_no_money AND v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED') THEN
      UPDATE public.pay_bank_transfers AS transfer_to_update
      SET
        status = v_normalised_state,
        failed_reason = CASE
          WHEN v_normalised_state IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED') THEN COALESCE(NULLIF(v_provider_state, ''), transfer_to_update.failed_reason)
          ELSE transfer_to_update.failed_reason
        END,
        rail_state = COALESCE(v_provider_state, transfer_to_update.rail_state),
        rail_meta_json = COALESCE(transfer_to_update.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'latest_bank_event_id', v_event_id,
          'latest_bank_event_state', v_normalised_state,
          'latest_bank_event_at_utc', v_now
        )
      WHERE transfer_to_update.id = v_pay_bank_transfer_id;
    ELSIF v_normalised_state IN ('SUBMITTED', 'PENDING', 'PROCESSING', 'UNKNOWN') THEN
      UPDATE public.pay_bank_transfers AS transfer_to_update
      SET
        status = CASE
          WHEN v_normalised_state = 'SUBMITTED' THEN 'PENDING'
          WHEN v_normalised_state IN ('PENDING', 'PROCESSING', 'UNKNOWN') THEN v_normalised_state
          ELSE transfer_to_update.status
        END,
        rail_state = COALESCE(v_provider_state, transfer_to_update.rail_state),
        rail_tx_id = COALESCE(NULLIF(v_provider_transaction_id, ''), NULLIF(v_provider_reference, ''), transfer_to_update.rail_tx_id),
        rail_meta_json = COALESCE(transfer_to_update.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'latest_bank_event_id', v_event_id,
          'latest_bank_event_state', v_normalised_state,
          'latest_bank_event_at_utc', v_now
        )
      WHERE transfer_to_update.id = v_pay_bank_transfer_id;
    END IF;
  END IF;


  v_terminal_bank_event := COALESCE(v_has_strong_transfer_mapping, false)
    AND (
    COALESCE(v_event_is_final_paid, false)
    OR COALESCE(v_event_is_terminal_no_money, false)
    OR v_normalised_state IN ('COMPLETED', 'FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED', 'REVERSED')
    );

  v_settlement_required := false;
  v_settlement_trigger := CASE
    WHEN COALESCE(v_has_strong_transfer_mapping, false) IS NOT TRUE THEN 'BANK_EVENT_UNMATCHED_OR_WEAK'
    WHEN COALESCE(v_terminal_bank_event, false) THEN 'BANK_EVENT_TERMINAL_TRANSFER_ONLY'
    ELSE 'BANK_EVENT_NON_TERMINAL'
  END;
  v_all_batch_transfers_terminal := false;
  v_terminal_success_transfer_count := 0;
  v_terminal_failed_transfer_count := 0;
  v_pending_transfer_count := 0;
  v_unknown_transfer_count := 0;
  v_pending_or_unknown_transfer_count := 0;

  IF v_pay_batch_id IS NOT NULL AND COALESCE(v_terminal_bank_event, false) THEN
    SELECT
    COALESCE(COUNT(*) FILTER (WHERE terminality_rows.is_success_terminal), 0)::integer,
    COALESCE(COUNT(*) FILTER (WHERE terminality_rows.is_failed_terminal), 0)::integer,
    COALESCE(COUNT(*) FILTER (WHERE terminality_rows.is_pending_non_terminal), 0)::integer,
    COALESCE(COUNT(*) FILTER (
      WHERE terminality_rows.is_success_terminal IS NOT TRUE
        AND terminality_rows.is_failed_terminal IS NOT TRUE
        AND terminality_rows.is_pending_non_terminal IS NOT TRUE

    ), 0)::integer
    INTO
    v_terminal_success_transfer_count,
    v_terminal_failed_transfer_count,
    v_pending_transfer_count,
    v_unknown_transfer_count
    FROM (
    SELECT
      transfer_row.id AS pay_bank_transfer_id,
      (
        COALESCE(transfer_class.is_final_money_moved, false)
        OR upper(btrim(COALESCE(transfer_row.status, ''))) IN ('COMPLETED', 'COMPLETE', 'PAID', 'SETTLED', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
      ) AS is_success_terminal,
      (
        COALESCE(transfer_class.is_terminal_no_money, false)
        OR upper(btrim(COALESCE(transfer_row.status, ''))) IN ('FAILED', 'FAILURE', 'REJECTED', 'DECLINED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED', 'REVERSED')
      ) AS is_failed_terminal,
      (
        COALESCE(transfer_class.is_pending_non_final, false)
        OR upper(btrim(COALESCE(transfer_row.status, ''))) IN ('PENDING', 'SUBMITTED', 'QUEUED', 'ACCEPTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'WAITING', 'WAITING_BANK_CONFIRM')
      ) AS is_pending_non_terminal
    FROM public.pay_bank_transfers AS transfer_row
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      transfer_row.status,
      transfer_row.rail_state,
      COALESCE(transfer_row.rail_meta_json, '{}'::jsonb),
      COALESCE(transfer_row.rail_meta_json, '{}'::jsonb)
    ) AS transfer_class ON true
    WHERE transfer_row.pay_batch_id = v_pay_batch_id
    ) AS terminality_rows;

  v_pending_or_unknown_transfer_count := COALESCE(v_pending_transfer_count, 0) + COALESCE(v_unknown_transfer_count, 0);
  v_all_batch_transfers_terminal := (
    COALESCE(v_terminal_success_transfer_count, 0) + COALESCE(v_terminal_failed_transfer_count, 0) > 0
    AND COALESCE(v_pending_or_unknown_transfer_count, 0) = 0
    );
  v_settlement_required := COALESCE(v_all_batch_transfers_terminal, false);
  v_settlement_trigger := CASE
    WHEN COALESCE(v_settlement_required, false) THEN 'BANK_EVENT_TERMINAL_BATCH_READY'
    ELSE 'BANK_EVENT_TERMINAL_TRANSFER_ONLY'
    END;
  END IF;

  v_settlement_intent_json := jsonb_strip_nulls(jsonb_build_object(
    'settlement_required', COALESCE(v_settlement_required, false),
    'settlement_trigger', v_settlement_trigger,
    'pay_batch_id', CASE WHEN v_pay_batch_id IS NULL THEN NULL ELSE v_pay_batch_id::text END,
    'pay_bank_transfer_id', CASE WHEN v_pay_bank_transfer_id IS NULL THEN NULL ELSE v_pay_bank_transfer_id::text END,
    'normalised_state', v_normalised_state,
    'cash_state', v_event_cash_state,
    'all_batch_transfers_terminal', COALESCE(v_all_batch_transfers_terminal, false),
    'terminal_success_transfer_count', COALESCE(v_terminal_success_transfer_count, 0),
    'terminal_failed_transfer_count', COALESCE(v_terminal_failed_transfer_count, 0),
    'pending_or_unknown_transfer_count', COALESCE(v_pending_or_unknown_transfer_count, 0),
    'unknown_transfer_count', COALESCE(v_unknown_transfer_count, 0)
  ));

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS released_item
    WHERE released_item.status = 'APPLIED'
      AND released_item.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
      AND (
        (v_pay_bank_transfer_id IS NOT NULL AND released_item.pay_bank_transfer_id = v_pay_bank_transfer_id)
        OR (v_pay_bank_transfer_id IS NULL AND v_candidate_id IS NOT NULL AND released_item.candidate_id = v_candidate_id)
      )
  ) AND COALESCE(v_event_is_final_paid, false)
  INTO v_paid_after_release;

  IF v_normalised_state = 'COMPLETED' AND COALESCE(v_has_strong_transfer_mapping, false) THEN
    SELECT COALESCE(
             p_actor_user_id,
             (
               SELECT operation_row.actor_user_id
               FROM public.banking_pay_operations AS operation_row
               WHERE operation_row.pay_batch_id = v_pay_batch_id
                 AND operation_row.actor_user_id IS NOT NULL
               ORDER BY operation_row.created_at_utc DESC, operation_row.id DESC
               LIMIT 1
             )
           )
    INTO v_settlement_actor_user_id;

    IF v_settlement_actor_user_id IS NULL THEN
      RAISE EXCEPTION 'PAY_BANK_EVENT_SETTLEMENT_ACTOR_REQUIRED'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'EVENT_AUTHORITY_REQUIRED',
          'bank_event_id', v_event_id,
          'pay_batch_id', v_pay_batch_id,
          'pay_bank_transfer_id', v_pay_bank_transfer_id,
          'message', 'The paid event was retained, but the payment execution actor could not be resolved for settlement application.'
        )::text;
    END IF;

    v_settlement_apply_result := public.pay_settle_rail(
      p_pay_batch_id => v_pay_batch_id,
      p_settlement_json => jsonb_build_array(jsonb_build_object(
        'bank_event_id', v_event_id,
        'transfer_id', v_pay_bank_transfer_id,
        'status', 'COMPLETED',
        'normalised_state', 'COMPLETED',
        'rail_tx_id', COALESCE(v_provider_transaction_id, v_provider_reference, v_transfer.rail_tx_id),
        'rail_state', COALESCE(v_provider_state, v_transfer.rail_state, 'COMPLETED'),
        'provider_event_id', v_provider_event_id,
        'provider_transaction_id', v_provider_transaction_id,
        'provider_request_id', v_provider_request_id,
        'provider_reference', v_provider_reference,
        'event_source', v_event_source,
        'event_time_utc', COALESCE(v_event_time_utc, v_now),
        'rail_meta_json', jsonb_build_object(
          'bank_event_id', v_event_id,
          'provider_event_id', v_provider_event_id,
          'provider_transaction_id', v_provider_transaction_id,
          'provider_request_id', v_provider_request_id,
          'provider_reference', v_provider_reference,
          'event_source', v_event_source,
          'settlement_authority', 'EXISTING_MAPPED_TERMINAL_EVENT'
        )
      )),
      p_actor_user_id => v_settlement_actor_user_id,
      p_operation_id => NULL::uuid,
      p_settlement_scope_ids => NULL::jsonb
    );
  END IF;

  IF v_normalised_state = 'COMPLETED'
     AND COALESCE(v_has_strong_transfer_mapping, false)
     AND COALESCE(v_paid_after_release, false) IS NOT TRUE THEN
    IF v_pay_bank_transfer_id IS NOT NULL THEN
      SELECT public._pay_manual_adjustment_carry_forward_mark_consumed(
        v_pay_batch_id,
        NULL::uuid[],
        jsonb_build_object(
          'pay_batch_id', v_pay_batch_id::text,
          'pay_bank_transfer_ids', jsonb_build_array(v_pay_bank_transfer_id::text),
          'pay_batch_item_ids', COALESCE((
            SELECT jsonb_agg(event_item_rows.id::text ORDER BY event_item_rows.id::text)
            FROM public.pay_batch_items AS event_item_rows
            JOIN public.pay_batch_candidates AS event_candidate_rows
              ON event_candidate_rows.id = event_item_rows.pay_batch_candidate_id
            WHERE event_candidate_rows.pay_batch_id = v_pay_batch_id
              AND event_item_rows.pay_bank_transfer_id = v_pay_bank_transfer_id
          ), '[]'::jsonb)
        ),
        jsonb_build_object(
          'final_paid', true,
          'cash_state', 'FINAL_PAID',
          'source', 'pay_bank_event_ingest',
          'bank_event_id', v_event_id::text
        ),
        v_settlement_actor_user_id
      )
      INTO v_carry_forward_mark_result;

      v_consumed_carry_forward_count := COALESCE(NULLIF(v_carry_forward_mark_result->>'consumed_count', '')::integer, 0);
    END IF;

    v_classification := NULL::text;
    v_correction_disposition := 'NO_CORRECTION_REQUIRED';
    v_classification_result := jsonb_build_object(
      'classification', NULL::text,
      'reasons', jsonb_build_array('COMPLETED_EVENT_NO_PAYMENT_CORRECTION_REQUIRED'),
      'evidence', jsonb_build_object(
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'normalised_state', v_normalised_state
      ),
      'counts', '{}'::jsonb,
      'blockers', '[]'::jsonb,
      'selected_amounts', '{}'::jsonb,
      'safe_to_auto_apply', false
    );

    UPDATE public.pay_bank_transfer_events AS bank_event_to_update
    SET
      movement_classification = NULL::text,
      correction_disposition = v_correction_disposition
    WHERE bank_event_to_update.id = v_event_id;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_BANK_EVENT_INGEST_COMPLETED_NO_CORRECTION',
      jsonb_build_object(
        'event_id', v_event_id,
        'pay_batch_id', v_pay_batch_id,
        'pay_bank_transfer_id', v_pay_bank_transfer_id,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'correction_disposition', v_correction_disposition
      ),
      'pay_payment_correction',
      v_event_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    v_signal_recommendation_json := jsonb_build_object(
      'pay_batch_id', v_pay_batch_id::text,
      'touch_payment_status', true,
      'touch_correction_progress', false,
      'touch_alerts', false,
      'touch_overview', true,
      'change_reason', 'BANK_EVENT_COMPLETED',
      'change_source', 'pay_bank_event_ingest',
      'changed_transfer_ids', CASE WHEN v_pay_bank_transfer_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_pay_bank_transfer_id::text) END,
      'changed_candidate_ids', CASE WHEN v_candidate_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_candidate_id::text) END,
      'changed_pay_batch_item_ids', '[]'::jsonb,
      'alert_candidate_kind', NULL::text,
      'alert_candidate_is_success_only', true,
      'provider_failure_reason_code', v_provider_failure_reason_code,
      'provider_failure_reason_group', v_provider_failure_reason_group,
      'source_delivery_id', v_source_delivery_id,
      'source_batch_event_group_key', v_source_batch_event_group_key,
      'change_scope_json', jsonb_build_object(
        'bank_event_id', v_event_id::text,
        'pay_bank_transfer_id', CASE WHEN v_pay_bank_transfer_id IS NULL THEN NULL ELSE v_pay_bank_transfer_id::text END,
        'normalised_state', v_normalised_state,
        'provider_event_transport', v_provider_event_transport,
        'provider_transaction_id', v_provider_transaction_id,
        'provider_request_id', v_provider_request_id
      ) || v_settlement_intent_json
    ) || v_settlement_intent_json;

    IF v_should_touch_signal THEN
      v_live_signal_result := public.banking_pay_batch_signal_touch(
        v_pay_batch_id,
        'BANK_EVENT_COMPLETED',
        'pay_bank_event_ingest',
        v_signal_recommendation_json->'change_scope_json',
        true,
        false,
        false,
        true
      );
    ELSE
      v_live_signal_result := jsonb_build_object(
        'ok', true,
        'changed', false,
        'deferred', true,
        'signal_recommendation_json', v_signal_recommendation_json
      );
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'event_id', v_event_id,
      'idempotent', NOT v_inserted_event,
      'mapped', true,
      'normalised_state', v_normalised_state,
      'manual_resolution_recorded', false,
      'release_eligible', false,
      'auto_release_request_prepared', false,
      'existing_release_request_id', NULL::uuid,
      'existing_release_operation_woken', false,
      'paid_after_release', v_paid_after_release,
      'requires_user_action', v_paid_after_release,
      'display_status', CASE WHEN v_paid_after_release THEN 'Paid — evidence received after release' ELSE 'Paid' END,
      'display_message', CASE
        WHEN v_paid_after_release THEN 'The bank confirmed this payment after CloudTMS released its payment reservation. CloudTMS has blocked any further payment action and retained both histories for Finance review.'
        ELSE 'The bank confirmed this payment.'
      END,
      'inserted', v_inserted_event,
      'mapping_status', v_mapping_status,

      'mapping_method', v_mapping_method,
      'classification', NULL::text,
      'correction_disposition', v_correction_disposition,
      'correction_request_id', NULL::uuid,
      'admin_notice_group_id', NULL::uuid,
      'live_signal', v_live_signal_result,
      'signal_recommendation_json', v_signal_recommendation_json,
      'selection_json', NULL::jsonb,
      'classification_result', v_classification_result,
      'auto_apply', jsonb_build_object(
        'auto_setting', false,
        'safe_to_auto_apply', false,
        'request_start_result', NULL::jsonb,
        'expand_result', NULL::jsonb,
        'process_result', NULL::jsonb,
        'blocker', NULL::jsonb
      ),
      'consumed_carry_forward_count', COALESCE(v_consumed_carry_forward_count, 0),
      'carry_forward_mark_consumed_result', COALESCE(v_carry_forward_mark_result, '{}'::jsonb),
      'settlement_result', COALESCE(v_settlement_apply_result, '{}'::jsonb)
    ) || v_settlement_intent_json;
  END IF;

  IF v_normalised_state IN ('SUBMITTED', 'PENDING', 'PROCESSING') THEN
    IF v_mapping_status = 'MATCHED'
       AND v_mapping_method IN (
         'TRANSFER_ID',
         'PROVIDER_EVENT_ID',
         'PROVIDER_TRANSACTION_ID',
         'REQUEST_ID',
         'PROVIDER_REFERENCE',
         'RAIL_TX_ID',
         'MATCHED_PROVIDER_EVENT',
         'MANUAL_TRANSFER_SELECTION'
       )
       AND v_provider_state_upper NOT IN ('TIMEOUT', 'TIMED_OUT', 'TIMEDOUT', 'API_TIMEOUT') THEN
      v_classification := NULL::text;
      v_correction_disposition := 'NO_CORRECTION_REQUIRED';
      v_classification_result := jsonb_build_object(
        'classification', NULL::text,
        'reasons', jsonb_build_array('NON_TERMINAL_PROVIDER_STATE_NO_PAYMENT_CORRECTION_REQUIRED'),
        'evidence', jsonb_build_object(
          'mapping_status', v_mapping_status,
          'mapping_method', v_mapping_method,
          'normalised_state', v_normalised_state,
          'provider_state', v_provider_state
        ),
        'counts', '{}'::jsonb,
        'blockers', '[]'::jsonb,
        'selected_amounts', '{}'::jsonb,
        'safe_to_auto_apply', false
      );

      UPDATE public.pay_bank_transfer_events AS bank_event_to_update
      SET
        movement_classification = NULL::text,
        correction_disposition = v_correction_disposition,
        mapping_method = v_mapping_method
      WHERE bank_event_to_update.id = v_event_id;

      PERFORM public._imp_debug_audit(
        p_actor_user_id,
        'PAYMENT_BANK_EVENT_INGEST_NON_TERMINAL_NO_CORRECTION',
        jsonb_build_object(
          'event_id', v_event_id,
          'pay_batch_id', v_pay_batch_id,
          'pay_bank_transfer_id', v_pay_bank_transfer_id,
          'normalised_state', v_normalised_state,
          'mapping_status', v_mapping_status,
          'mapping_method', v_mapping_method,
          'correction_disposition', v_correction_disposition
        ),
        'pay_payment_correction',
        v_event_id::text,
        NULL::jsonb,
        NULL::text,
        NULL::text,
        NULL::text
      );

      v_signal_recommendation_json := jsonb_build_object(
        'pay_batch_id', v_pay_batch_id::text,
        'touch_payment_status', true,
        'touch_correction_progress', false,
        'touch_alerts', false,
        'touch_overview', true,
        'change_reason', 'BANK_EVENT_PENDING_OR_NON_TERMINAL',
        'change_source', 'pay_bank_event_ingest',
        'changed_transfer_ids', CASE WHEN v_pay_bank_transfer_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_pay_bank_transfer_id::text) END,
        'changed_candidate_ids', CASE WHEN v_candidate_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_candidate_id::text) END,
        'changed_pay_batch_item_ids', '[]'::jsonb,
        'alert_candidate_kind', NULL::text,
        'alert_candidate_is_success_only', false,
        'provider_failure_reason_code', v_provider_failure_reason_code,
        'provider_failure_reason_group', v_provider_failure_reason_group,
        'source_delivery_id', v_source_delivery_id,
        'source_batch_event_group_key', v_source_batch_event_group_key,
        'change_scope_json', jsonb_build_object(
          'bank_event_id', v_event_id::text,
          'pay_bank_transfer_id', CASE WHEN v_pay_bank_transfer_id IS NULL THEN NULL ELSE v_pay_bank_transfer_id::text END,
          'normalised_state', v_normalised_state,
          'provider_event_transport', v_provider_event_transport
        ) || v_settlement_intent_json
      ) || v_settlement_intent_json;

      IF v_should_touch_signal THEN
        v_live_signal_result := public.banking_pay_batch_signal_touch(
          v_pay_batch_id,
          'BANK_EVENT_PENDING_OR_NON_TERMINAL',
          'pay_bank_event_ingest',
          v_signal_recommendation_json->'change_scope_json',
          true,
          false,
          false,
          true
        );
      ELSE
        v_live_signal_result := jsonb_build_object(
          'ok', true,
          'changed', false,
          'deferred', true,
          'signal_recommendation_json', v_signal_recommendation_json
        );
      END IF;

      RETURN jsonb_build_object(
        'ok', true,
        'event_id', v_event_id,
        'idempotent', NOT v_inserted_event,
        'mapped', true,
        'normalised_state', v_normalised_state,
        'manual_resolution_recorded', false,
        'release_eligible', false,
        'auto_release_request_prepared', false,
        'existing_release_request_id', NULL::uuid,
        'existing_release_operation_woken', false,
        'paid_after_release', false,
        'requires_user_action', false,
        'display_status', 'Payment status updated',
        'display_message', 'The latest provider payment status was recorded.',
        'inserted', v_inserted_event,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'classification', NULL::text,
        'correction_disposition', v_correction_disposition,
        'correction_request_id', NULL::uuid,
        'admin_notice_group_id', NULL::uuid,
        'live_signal', v_live_signal_result,
        'signal_recommendation_json', v_signal_recommendation_json,
        'message', 'Provider state recorded; no correction action required yet.',
        'selection_json', NULL::jsonb,
        'classification_result', v_classification_result,
        'auto_apply', jsonb_build_object(
          'auto_setting', false,
          'safe_to_auto_apply', false,
          'request_start_result', NULL::jsonb,
          'expand_result', NULL::jsonb,
          'process_result', NULL::jsonb,
          'final_work_item_totals', v_final_work_item_totals,
          'blocker', NULL::jsonb
        )
      ) || v_settlement_intent_json;
    END IF;
  END IF;

  IF v_pay_bank_transfer_id IS NOT NULL THEN
    v_selection_json := jsonb_build_object(
      'scope_type', 'TRANSFER',
      'pay_bank_transfer_ids', jsonb_build_array(v_pay_bank_transfer_id::text),
      'source_context', 'BANK_EVENT_INGEST',
      'requested_action', CASE
        WHEN v_event_is_terminal_no_money THEN 'NO_MONEY_UNWIND_AND_RECALCULATE'
        WHEN v_event_is_final_paid THEN 'AMEND_AND_RECOVER_OVERPAYMENT'
        ELSE 'CHECK_PROVIDER_STATUS'
      END
    );
  ELSIF v_candidate_id IS NOT NULL THEN
    v_selection_json := jsonb_build_object(
      'scope_type', 'CANDIDATES',
      'pay_batch_candidate_ids', COALESCE((
        SELECT jsonb_agg(candidate_scope_rows.id::text ORDER BY candidate_scope_rows.id::text)
        FROM public.pay_batch_candidates AS candidate_scope_rows
        WHERE candidate_scope_rows.pay_batch_id = v_pay_batch_id
          AND candidate_scope_rows.candidate_id = v_candidate_id
      ), '[]'::jsonb),
      'source_context', 'BANK_EVENT_INGEST',
      'requested_action', CASE
        WHEN v_event_is_terminal_no_money THEN 'NO_MONEY_UNWIND_AND_RECALCULATE'
        WHEN v_event_is_final_paid THEN 'AMEND_AND_RECOVER_OVERPAYMENT'
        ELSE 'CHECK_PROVIDER_STATUS'
      END
    );
  ELSE

    v_selection_json := jsonb_build_object(
      'scope_type', 'BATCH',
      'source_context', 'BANK_EVENT_INGEST',
      'requested_action', CASE
        WHEN v_event_is_terminal_no_money THEN 'NO_MONEY_UNWIND_AND_RECALCULATE'
        WHEN v_event_is_final_paid THEN 'AMEND_AND_RECOVER_OVERPAYMENT'
        ELSE 'CHECK_PROVIDER_STATUS'
      END
    );
  END IF;

  IF v_mapping_status = 'MATCHED'
     AND v_mapping_method IN (
       'TRANSFER_ID',
       'PROVIDER_EVENT_ID',
       'PROVIDER_TRANSACTION_ID',
       'REQUEST_ID',
       'PROVIDER_REFERENCE',
       'RAIL_TX_ID',
       'MATCHED_PROVIDER_EVENT',
       'MANUAL_TRANSFER_SELECTION'
     ) THEN
    v_classification_result := public._pay_payment_movement_classify(
      v_pay_batch_id,
      v_selection_json
    );
    v_classification := COALESCE(v_classification_result->>'classification', 'AMBIGUOUS_REVIEW_REQUIRED');
    v_safe_to_auto_apply := COALESCE((v_classification_result->>'safe_to_auto_apply')::boolean, false);
  ELSE
    v_classification := 'AMBIGUOUS_REVIEW_REQUIRED';
    v_classification_result := jsonb_build_object(
      'classification', v_classification,
      'reasons', jsonb_build_array('BANK_EVENT_MAPPING_NOT_STRONG_MATCH'),
      'evidence', jsonb_build_object(
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method
      ),
      'counts', '{}'::jsonb,
      'blockers', jsonb_build_array(jsonb_build_object(
        'code', 'BANK_EVENT_MAPPING_NOT_STRONG_MATCH',
        'message', 'Bank event could not be mapped to a transfer using a strong exact mapping method. Amount-only, unmatched, ambiguous, and legacy no-artifact mappings require manual review.'
      )),
      'selected_amounts', '{}'::jsonb,
      'safe_to_auto_apply', false
    );
    v_safe_to_auto_apply := false;
  END IF;

  SELECT COALESCE(public.settings_defaults.banking_pay_auto_unwind_terminal_no_money, false)
  INTO v_auto_setting
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_event_required_action := NULLIF(btrim(COALESCE(v_classification_result->>'recommended_action', '')), '');

  v_notice_kind := CASE
    WHEN COALESCE(v_existing_transfer_is_final_paid, false)
      OR v_event_is_final_paid
      OR v_classification = 'PAID_OR_SETTLED'
      OR v_event_required_action = 'AMEND_AND_RECOVER_OVERPAYMENT'
      THEN 'PAID_SETTLED_RECOVERY_REQUIRED'
    WHEN v_classification IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY')
      OR v_event_required_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'
      THEN 'NO_MONEY_UNWIND_REQUIRED'
    WHEN v_classification = 'PROVIDER_OUTCOME_UNKNOWN' OR v_event_required_action = 'CHECK_PROVIDER_STATUS' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    ELSE 'BANK_FAILURE_DETECTED'
  END;

  IF COALESCE(v_existing_transfer_is_final_paid, false) AND v_event_is_terminal_no_money THEN
    v_correction_disposition := 'ACTION_REQUIRED';
  ELSIF v_event_is_final_paid AND COALESCE(v_has_strong_transfer_mapping, false) THEN
    v_correction_disposition := 'NO_CORRECTION_REQUIRED';
  ELSIF v_event_is_final_paid THEN
    v_correction_disposition := 'ACTION_REQUIRED';
  ELSIF v_classification IS NULL THEN
    v_correction_disposition := 'NO_CORRECTION_REQUIRED';
  ELSIF v_classification = 'AMBIGUOUS_REVIEW_REQUIRED' THEN
    v_correction_disposition := 'AMBIGUOUS';
  ELSIF v_event_is_pending_non_final OR v_event_cash_state = 'UNKNOWN' OR v_event_required_action = 'CHECK_PROVIDER_STATUS' THEN
    v_correction_disposition := 'ACTION_REQUIRED';
  ELSIF NOT COALESCE(v_auto_setting, false) THEN
    v_correction_disposition := 'ACTION_REQUIRED';
  ELSIF NOT COALESCE(v_safe_to_auto_apply, false) THEN
    v_correction_disposition := 'BLOCKED';
  ELSE
    v_correction_disposition := 'ACTION_REQUIRED';
  END IF;
  IF COALESCE(v_auto_setting, false)
     AND COALESCE(v_safe_to_auto_apply, false)
     AND COALESCE(v_suppress_auto_unwind, false) IS NOT TRUE
     AND COALESCE(v_manual_confirmed_not_paid, false) IS NOT TRUE
     AND v_event_source NOT IN ('MANUAL_EVIDENCE', 'MANUAL_CONFIRM')
     AND v_mapping_status = 'MATCHED'
     AND NOT COALESCE(v_existing_transfer_is_final_paid, false)
     AND v_mapping_method IN (
       'TRANSFER_ID',
       'PROVIDER_EVENT_ID',
       'PROVIDER_TRANSACTION_ID',
       'REQUEST_ID',
       'PROVIDER_REFERENCE',
       'RAIL_TX_ID',
       'MATCHED_PROVIDER_EVENT',
       'MANUAL_TRANSFER_SELECTION'
     )
     AND v_event_is_terminal_no_money
     AND (
       v_classification IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY')
       OR v_event_required_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'
     ) THEN
    BEGIN
      v_selection_json := v_selection_json || jsonb_build_object(
        'command', 'PREPARE',
        'contract_version', 1,
        'mode', 'EXPLICIT',
        'requested_action', 'NO_MONEY_UNWIND',
        'explicit_candidate_tokens', COALESCE((
          SELECT jsonb_agg(DISTINCT candidate_scope.id::text ORDER BY candidate_scope.id::text)
          FROM public.pay_batch_candidates AS candidate_scope
          JOIN public.pay_batch_items AS candidate_item
            ON candidate_item.pay_batch_candidate_id = candidate_scope.id
          WHERE candidate_scope.pay_batch_id = v_pay_batch_id
            AND (
              (v_pay_bank_transfer_id IS NOT NULL AND candidate_item.pay_bank_transfer_id = v_pay_bank_transfer_id)
              OR (v_pay_bank_transfer_id IS NULL AND v_candidate_id IS NOT NULL AND candidate_scope.candidate_id = v_candidate_id)
            )
        ), '[]'::jsonb),
        'filter_json', '{}'::jsonb,
        'sort_key', 'STATUS',
        'sort_direction', 'ASC',
        'idempotency_key', 'bank-event-correction:' || v_event_id::text
      );

      -- Make the provider boundary used by PREPARE deterministic. This update
      -- is inside the exception subtransaction: if request creation fails it
      -- rolls back and the outer recovery records BLOCKED instead.
      UPDATE public.pay_bank_transfer_events AS auto_event
      SET correction_disposition = 'AUTO_PROCESSING'
      WHERE auto_event.id = v_event_id;
      v_correction_disposition := 'AUTO_PROCESSING';

      v_request_start_result := public.pay_payment_correction_request_start(
        p_pay_batch_id => v_pay_batch_id,
        p_selection_json => v_selection_json,
        p_reason => 'Automatic provider-confirmed no-money release.',
        p_actor_user_id => p_actor_user_id,
        p_source_bank_event_id => v_event_id,
        p_auto_requested => true,
        p_accepted_resolution_json => jsonb_build_object(
          'source', v_provider_event_transport,
          'provider_key', v_provider_key,
          'provider_event_id', v_provider_event_id,
          'provider_webhook_receipt_id', v_provider_webhook_receipt_id,
          'provider_transaction_id', v_provider_transaction_id,
          'provider_request_id', v_provider_request_id,
          'terminal_no_money_evidence', true,
          'signature_valid', COALESCE(v_provider_signature_valid, v_receipt_signature_valid, false),
          'source_bank_event_id', v_event_id
        )
      );

      v_correction_request_id := NULLIF(v_request_start_result->>'correction_request_id', '')::uuid;
      v_correction_disposition := CASE
        WHEN v_correction_request_id IS NULL THEN 'BLOCKED'
        ELSE 'AUTO_PROCESSING'
      END;
      v_exact_mapping_required_blocker := CASE
        WHEN v_correction_request_id IS NULL THEN jsonb_build_object(
          'code', 'AUTO_CORRECTION_REQUEST_NOT_CREATED',
          'message', 'Automatic no-money evidence was retained but the asynchronous correction request was not created.'
        )
        ELSE NULL::jsonb
      END;
      v_final_work_item_totals := jsonb_build_object(
        'total', 0, 'applied', 0, 'skipped', 0, 'blocked', 0,
        'failed_retryable', 0, 'failed_final', 0, 'pending', 0, 'processing', 0,
        'asynchronous', true
      );
    EXCEPTION WHEN OTHERS THEN
      v_correction_disposition := 'BLOCKED';
      v_exact_mapping_required_blocker := jsonb_build_object(
        'code', 'AUTO_CORRECTION_REQUEST_FAILED',
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      );
    END;
  ELSIF v_manual_confirmed_not_paid AND v_suppress_auto_unwind THEN
    v_correction_disposition := 'ACTION_REQUIRED';
    v_event_required_action := 'RELEASE_FAILED_PAYMENT';
    UPDATE public.pay_bank_transfer_events AS manual_no_money_event
    SET mapping_hints_json = COALESCE(manual_no_money_event.mapping_hints_json, '{}'::jsonb)
          || jsonb_build_object(
            'business_code', 'CONFIRMED_NOT_PAID',
            'release_eligible', true,
            'suppress_auto_unwind', true,
            'display_status', 'Not paid — ready to release'
          )
    WHERE manual_no_money_event.id = v_event_id;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS released_item
    WHERE released_item.status = 'APPLIED'
      AND released_item.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
      AND (
        (v_pay_bank_transfer_id IS NOT NULL AND released_item.pay_bank_transfer_id = v_pay_bank_transfer_id)
        OR (v_pay_bank_transfer_id IS NULL AND v_candidate_id IS NOT NULL AND released_item.candidate_id = v_candidate_id)
      )
  ) AND COALESCE(v_event_is_final_paid, false)
  INTO v_paid_after_release;

  IF v_paid_after_release THEN
    v_classification := 'AMBIGUOUS_REVIEW_REQUIRED';
    v_correction_disposition := 'BLOCKED';
    v_event_required_action := 'FINANCE_REVIEW_ONLY';
    v_exact_mapping_required_blocker := jsonb_build_object(
      'code', 'PAID_EVIDENCE_AFTER_RELEASE',
      'message', 'Paid evidence arrived after CloudTMS released the payment reservation. Further payment action is blocked for Finance review.'
    );

    UPDATE public.pay_bank_transfer_events AS late_paid_event
    SET movement_classification = 'AMBIGUOUS_REVIEW_REQUIRED',
        correction_disposition = 'BLOCKED',
        mapping_status = 'MATCHED',
        mapping_hints_json = COALESCE(late_paid_event.mapping_hints_json, '{}'::jsonb)
          || jsonb_build_object(
            'business_code', 'PAID_EVIDENCE_AFTER_RELEASE',
            'review_status', CASE WHEN v_review_acknowledgement THEN 'ACKNOWLEDGED' ELSE 'REQUIRED' END,
            'blocks_new_payment_actions', true,
            'finance_review_only', true
          )
    WHERE late_paid_event.id = v_event_id;

    UPDATE public.pay_bank_transfers AS late_paid_transfer
    SET status = 'COMPLETED', rail_state = 'COMPLETED',
        rail_meta_json = COALESCE(late_paid_transfer.rail_meta_json, '{}'::jsonb)
          || jsonb_build_object(
            'business_code', 'PAID_EVIDENCE_AFTER_RELEASE',
            'bank_event_id', v_event_id,
            'finance_review_only', true
          )
    WHERE late_paid_transfer.id = v_pay_bank_transfer_id;

    UPDATE public.pay_batch_candidates AS late_paid_candidate
    SET settlement_status = 'SETTLED',
        settled_at_utc = COALESCE(late_paid_candidate.settled_at_utc, v_event_time_utc, v_now),
        settled_via = COALESCE(late_paid_candidate.settled_via, 'BANK_EVENT')
    WHERE late_paid_candidate.pay_batch_id = v_pay_batch_id
      AND (
        (v_candidate_id IS NOT NULL AND late_paid_candidate.candidate_id = v_candidate_id)
        OR late_paid_candidate.id IN (
          SELECT item_row.pay_batch_candidate_id
          FROM public.pay_batch_items AS item_row
          WHERE item_row.pay_bank_transfer_id = v_pay_bank_transfer_id
        )
      );

    UPDATE public.pay_payment_correction_work_items AS late_paid_work
    SET status = CASE WHEN late_paid_work.status = 'APPLIED' THEN 'APPLIED' ELSE 'BLOCKED' END,
        last_error = CASE WHEN late_paid_work.status = 'APPLIED' THEN late_paid_work.last_error ELSE 'PAID_EVIDENCE_AFTER_RELEASE' END,
        processed_at_utc = COALESCE(late_paid_work.processed_at_utc, v_now),
        result_json = COALESCE(late_paid_work.result_json, '{}'::jsonb)
          || jsonb_build_object('code', 'PAID_EVIDENCE_AFTER_RELEASE', 'bank_event_id', v_event_id)
    WHERE late_paid_work.pay_batch_id = v_pay_batch_id
      AND (
        late_paid_work.pay_bank_transfer_id = v_pay_bank_transfer_id
        OR (v_candidate_id IS NOT NULL AND late_paid_work.candidate_id = v_candidate_id)
      );

    WITH affected_replacement_batches AS (
      SELECT DISTINCT replacement_candidate.pay_batch_id
      FROM public.pay_batch_candidates AS replacement_candidate
      JOIN public.pay_batch_items AS replacement_item
        ON replacement_item.pay_batch_candidate_id = replacement_candidate.id
       AND COALESCE(replacement_item.is_voided, false) IS NOT TRUE
      WHERE replacement_candidate.pay_batch_id <> v_pay_batch_id
        AND replacement_candidate.candidate_id IN (
          SELECT original_candidate.candidate_id
          FROM public.pay_batch_candidates AS original_candidate
          WHERE original_candidate.pay_batch_id = v_pay_batch_id
            AND (
              (v_candidate_id IS NOT NULL AND original_candidate.candidate_id = v_candidate_id)
              OR original_candidate.id IN (
                SELECT original_item.pay_batch_candidate_id
                FROM public.pay_batch_items AS original_item
                WHERE original_item.pay_bank_transfer_id = v_pay_bank_transfer_id
              )
            )
        )
    )
    UPDATE public.pay_batches AS replacement_batch
    SET status = CASE
          WHEN replacement_batch.status IN ('AUTHORISED_FOR_PAYMENT','SCHEDULED','EXECUTING') THEN 'AWAITING_AUTHORISATION'
          ELSE replacement_batch.status
        END,
        schedule_kind = NULL, scheduled_at_utc = NULL, scheduled_by_user_id = NULL,
        funding_account_ref = NULL, funds_warning_hours_json = NULL,
        freshness_validation_status = 'STALE_POST_SCOPE_FREEZE',
        source_scope_change_generation = COALESCE(replacement_batch.source_scope_change_generation, 0) + 1
    FROM affected_replacement_batches
    WHERE replacement_batch.id = affected_replacement_batches.pay_batch_id;

    UPDATE public.pay_batch_auth_requests AS replacement_auth
    SET state = 'CANCELLED',
        finalised_at_utc = COALESCE(replacement_auth.finalised_at_utc, v_now),
        finalised_by_user_id = COALESCE(replacement_auth.finalised_by_user_id, p_actor_user_id)
    WHERE replacement_auth.state IN ('AWAITING','AUTHORISED')
      AND EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS replacement_candidate
        JOIN public.pay_batch_items AS replacement_item
          ON replacement_item.pay_batch_candidate_id = replacement_candidate.id
         AND COALESCE(replacement_item.is_voided, false) IS NOT TRUE
        WHERE replacement_candidate.pay_batch_id = replacement_auth.pay_batch_id
          AND replacement_candidate.candidate_id IN (
            SELECT original_candidate.candidate_id
            FROM public.pay_batch_candidates AS original_candidate
            WHERE original_candidate.pay_batch_id = v_pay_batch_id
              AND (v_candidate_id IS NULL OR original_candidate.candidate_id = v_candidate_id)
          )
      );

    UPDATE public.banking_pay_operation_transfer_scope AS unsafe_scope
    SET provider_submit_ready = false,
        provider_review_required = true,
        provider_unsafe_reason = 'PAID_EVIDENCE_AFTER_RELEASE'
    WHERE EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_transfer_scope_items AS unsafe_item
      JOIN public.pay_batch_candidates AS replacement_candidate
        ON replacement_candidate.id = unsafe_item.pay_batch_candidate_id
      WHERE unsafe_item.transfer_scope_id = unsafe_scope.id
        AND replacement_candidate.candidate_id IN (
          SELECT original_candidate.candidate_id
          FROM public.pay_batch_candidates AS original_candidate
          WHERE original_candidate.pay_batch_id = v_pay_batch_id
            AND (v_candidate_id IS NULL OR original_candidate.candidate_id = v_candidate_id)
        )
    );

    UPDATE public.banking_pay_operations AS unsafe_operation
    SET status = 'REVIEW_REQUIRED', requires_user_action = true,
        result_json = COALESCE(unsafe_operation.result_json, '{}'::jsonb)
          || jsonb_build_object('code', 'PAID_EVIDENCE_AFTER_RELEASE', 'bank_event_id', v_event_id),
        run_after_utc = v_now
    WHERE unsafe_operation.status NOT IN ('COMPLETE', 'COMPLETED', 'FAILED', 'CANCELLED')
      AND (
       unsafe_operation.pay_batch_id = v_pay_batch_id
       OR EXISTS (
         SELECT 1
         FROM public.pay_batch_candidates AS replacement_candidate
         JOIN public.pay_batch_items AS replacement_item
           ON replacement_item.pay_batch_candidate_id = replacement_candidate.id
          AND COALESCE(replacement_item.is_voided, false) IS NOT TRUE
         WHERE replacement_candidate.pay_batch_id = unsafe_operation.pay_batch_id
           AND replacement_candidate.candidate_id IN (
             SELECT original_candidate.candidate_id
             FROM public.pay_batch_candidates AS original_candidate
             WHERE original_candidate.pay_batch_id = v_pay_batch_id
               AND (v_candidate_id IS NULL OR original_candidate.candidate_id = v_candidate_id)
           )
       )
      );

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id, pay_batch_id, actor_kind, actor_user_id, action,
      action_at_utc, note, before_json, after_json, metadata_json
    )
    SELECT DISTINCT correction_request.id, correction_request.pay_batch_id,
      CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
      p_actor_user_id, 'APPLY', v_now,
      'Paid evidence received after reservation release.', NULL, NULL,
      jsonb_build_object(
        'business_code', 'PAID_EVIDENCE_AFTER_RELEASE',
        'review_status', CASE WHEN v_review_acknowledgement THEN 'ACKNOWLEDGED' ELSE 'REQUIRED' END,
        'bank_event_id', v_event_id,
        'pay_bank_transfer_id', v_pay_bank_transfer_id
      )
    FROM public.pay_payment_correction_requests AS correction_request
    JOIN public.pay_payment_correction_items AS correction_item
      ON correction_item.correction_request_id = correction_request.id
     AND correction_item.status = 'APPLIED'
    WHERE (
        correction_item.pay_bank_transfer_id = v_pay_bank_transfer_id
        OR (v_candidate_id IS NOT NULL AND correction_item.candidate_id = v_candidate_id)
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_actions AS existing_review_action
        WHERE existing_review_action.correction_request_id = correction_request.id
          AND existing_review_action.metadata_json ->> 'business_code' = 'PAID_EVIDENCE_AFTER_RELEASE'
          AND existing_review_action.metadata_json ->> 'review_status'
              = CASE WHEN v_review_acknowledgement THEN 'ACKNOWLEDGED' ELSE 'REQUIRED' END
          AND existing_review_action.metadata_json ->> 'bank_event_id' = v_event_id::text
      );
  END IF;

  IF v_correction_disposition NOT IN ('NO_CORRECTION_REQUIRED', 'AUTO_PROCESSING') THEN
    v_admin_notice_result := public.pay_payment_return_admin_notice_queue(
      p_notice_kind => CASE
        WHEN v_correction_disposition = 'AMBIGUOUS' THEN 'AUTO_CORRECTION_BLOCKED'
        WHEN v_correction_disposition = 'BLOCKED' THEN 'AUTO_CORRECTION_BLOCKED'
        WHEN v_correction_disposition = 'FAILED' THEN 'AUTO_CORRECTION_BLOCKED'
        WHEN v_correction_disposition = 'AUTO_APPLIED' AND (v_classification IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY') OR v_event_required_action = 'NO_MONEY_UNWIND_AND_RECALCULATE') THEN 'NO_MONEY_UNWIND_APPLIED'
        ELSE v_notice_kind
      END,
      p_pay_batch_id => v_pay_batch_id,
      p_provider_key => COALESCE(v_provider_key, v_batch.rail_provider_snapshot, 'UNKNOWN'),
      p_execution_commit_ref => v_batch.execution_commit_ref,
      p_summary_json => jsonb_build_object(
        'pay_batch_id', v_pay_batch_id,
        'pay_bank_transfer_id', v_pay_bank_transfer_id,
        'bank_event_id', v_event_id,
        'mapping_status', v_mapping_status,
        'mapping_method', v_mapping_method,
        'classification', v_classification,
        'correction_disposition', v_correction_disposition,
        'provider_key', v_provider_key,
        'provider_state', v_provider_state,
        'normalised_state', v_normalised_state,
        'amount', v_amount,
        'currency', v_currency,
        'correction_request_id', CASE WHEN v_correction_request_id IS NULL THEN NULL ELSE v_correction_request_id::text END,
        'request_start_result', COALESCE(v_request_start_result, '{}'::jsonb),
        'expand_result', COALESCE(v_expand_result, '{}'::jsonb),
        'process_result', COALESCE(v_process_result, '{}'::jsonb),
        'final_work_item_totals', COALESCE(v_final_work_item_totals, '{}'::jsonb),
        'blocker', v_exact_mapping_required_blocker
      )
    );

    v_admin_notice_group_id := NULLIF(v_admin_notice_result->>'notice_group_id', '')::uuid;
  END IF;

  UPDATE public.pay_bank_transfer_events AS bank_event_to_update
  SET
    movement_classification = v_classification,
    correction_disposition = v_correction_disposition,
    mapping_method = v_mapping_method
  WHERE bank_event_to_update.id = v_event_id;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_BANK_EVENT_INGEST_RESULT',
    jsonb_build_object(
      'event_id', v_event_id,
      'inserted', v_inserted_event,
      'pay_batch_id', v_pay_batch_id,
      'pay_bank_transfer_id', v_pay_bank_transfer_id,
      'mapping_status', v_mapping_status,
      'mapping_method', v_mapping_method,
      'classification', v_classification,
      'safe_to_auto_apply', v_safe_to_auto_apply,
      'auto_setting', v_auto_setting,
      'correction_disposition', v_correction_disposition,
      'correction_request_id', v_correction_request_id,
      'admin_notice_group_id', v_admin_notice_group_id
    ),
    'pay_payment_correction',
    v_event_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  v_signal_recommendation_json := jsonb_build_object(
    'pay_batch_id', v_pay_batch_id::text,
    'touch_payment_status', true,
    'touch_correction_progress', v_correction_disposition IN ('AUTO_PROCESSING', 'AUTO_APPLIED'),
    'touch_alerts', (
      v_event_is_final_paid IS NOT TRUE
      AND (
        v_correction_disposition IN ('ACTION_REQUIRED', 'AMBIGUOUS', 'BLOCKED', 'FAILED', 'AUTO_PROCESSING')
        OR v_event_required_action IN ('CHECK_PROVIDER_STATUS', 'NO_MONEY_UNWIND_AND_RECALCULATE')
      )
    ),
    'touch_overview', true,
    'change_reason', 'BANK_EVENT_INGEST_RESULT',
    'change_source', 'pay_bank_event_ingest',
    'changed_transfer_ids', CASE WHEN v_pay_bank_transfer_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_pay_bank_transfer_id::text) END,
    'changed_candidate_ids', CASE WHEN v_candidate_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_candidate_id::text) END,
    'changed_pay_batch_item_ids', '[]'::jsonb,
    'alert_candidate_kind', CASE
      WHEN COALESCE(v_existing_transfer_is_final_paid, false) AND v_event_is_terminal_no_money THEN 'PAID_SETTLED_RECOVERY_REQUIRED'
      WHEN v_event_is_final_paid THEN NULL::text
      WHEN v_event_required_action = 'AMEND_AND_RECOVER_OVERPAYMENT' THEN 'PAID_SETTLED_RECOVERY_REQUIRED'
      WHEN v_event_required_action = 'CHECK_PROVIDER_STATUS' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
      WHEN v_event_required_action = 'NO_MONEY_UNWIND_AND_RECALCULATE' THEN CASE WHEN COALESCE(v_auto_setting, false) THEN 'AUTO_UNWIND_PROGRESS' ELSE 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' END
      ELSE NULL::text
    END,
    'alert_candidate_is_success_only', COALESCE(v_event_is_final_paid, false) AND v_event_required_action IS DISTINCT FROM 'AMEND_AND_RECOVER_OVERPAYMENT',
    'provider_failure_reason_code', v_provider_failure_reason_code,
    'provider_failure_reason_group', v_provider_failure_reason_group,
    'source_delivery_id', v_source_delivery_id,
    'source_batch_event_group_key', v_source_batch_event_group_key,
    'change_scope_json', jsonb_build_object(
      'bank_event_id', v_event_id::text,
      'pay_bank_transfer_id', CASE WHEN v_pay_bank_transfer_id IS NULL THEN NULL ELSE v_pay_bank_transfer_id::text END,
      'normalised_state', v_normalised_state,
      'correction_disposition', v_correction_disposition,
      'provider_event_transport', v_provider_event_transport,
      'provider_failure_reason_group', v_provider_failure_reason_group
    ) || v_settlement_intent_json
  ) || v_settlement_intent_json;

  IF v_should_touch_signal THEN
    v_live_signal_result := public.banking_pay_batch_signal_touch(
      v_pay_batch_id,
      'BANK_EVENT_INGEST_RESULT',
      'pay_bank_event_ingest',
      v_signal_recommendation_json->'change_scope_json',
      true,
      v_correction_disposition IN ('AUTO_PROCESSING', 'AUTO_APPLIED'),
      (
        v_event_is_final_paid IS NOT TRUE
        AND (
          v_correction_disposition IN ('ACTION_REQUIRED', 'AMBIGUOUS', 'BLOCKED', 'FAILED', 'AUTO_PROCESSING')

          OR v_event_required_action IN ('CHECK_PROVIDER_STATUS', 'NO_MONEY_UNWIND_AND_RECALCULATE')
        )
      ),
      true
    );
  ELSE
    v_live_signal_result := jsonb_build_object(
      'ok', true,
      'changed', false,
      'deferred', true,
      'signal_recommendation_json', v_signal_recommendation_json
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'event_id', v_event_id,
    'idempotent', NOT v_inserted_event,
    'mapped', v_mapping_status = 'MATCHED',
    'normalised_state', v_normalised_state,
    'manual_resolution_recorded', v_manual_confirmed_not_paid,
    'release_eligible', v_manual_confirmed_not_paid OR (
      v_event_is_terminal_no_money AND v_correction_request_id IS NULL
    ),
    'auto_release_request_prepared', v_auto_setting AND v_safe_to_auto_apply AND v_correction_request_id IS NOT NULL,
    'existing_release_request_id', v_correction_request_id,
    'existing_release_operation_woken', COALESCE((v_request_start_result->>'existing_request')::boolean, false),
    'paid_after_release', v_paid_after_release,
    'requires_user_action', v_paid_after_release OR v_correction_disposition IN ('ACTION_REQUIRED', 'AMBIGUOUS', 'BLOCKED', 'FAILED'),
    'display_status', CASE
      WHEN v_paid_after_release THEN 'Paid — evidence received after release'
      WHEN v_manual_confirmed_not_paid THEN 'Not paid — ready to release'
      WHEN v_correction_disposition = 'AUTO_PROCESSING' THEN 'Failed payment release is being prepared'
      WHEN v_mapping_status <> 'MATCHED' THEN 'Payment status needs review'
      ELSE 'Payment status updated'
    END,
    'display_message', CASE
      WHEN v_paid_after_release THEN 'The bank confirmed this payment after CloudTMS released its payment reservation. CloudTMS has blocked any further payment action and retained both histories for Finance review.'
      WHEN v_manual_confirmed_not_paid THEN 'The payment is confirmed not paid and can now be released as a separate authorised action.'
      WHEN v_correction_disposition = 'AUTO_PROCESSING' THEN 'CloudTMS is preparing the provider-confirmed failed-payment release asynchronously.'
      WHEN v_mapping_status <> 'MATCHED' THEN 'The provider evidence was retained and needs review before a financial action can continue.'
      ELSE 'The provider payment status was recorded.'
    END,
    'inserted', v_inserted_event,
    'mapping_status', v_mapping_status,
    'mapping_method', v_mapping_method,
    'classification', v_classification,
    'correction_disposition', v_correction_disposition,
    'correction_request_id', v_correction_request_id,
    'admin_notice_group_id', v_admin_notice_group_id,
    'provider_failure_reason_code', v_provider_failure_reason_code,
    'provider_failure_reason_group', v_provider_failure_reason_group,
    'provider_failure_reason_label', v_provider_failure_reason_label,
    'live_signal', v_live_signal_result,
    'signal_recommendation_json', v_signal_recommendation_json,
    'selection_json', v_selection_json,
    'classification_result', v_classification_result,
    'auto_apply', jsonb_build_object(
      'auto_setting', v_auto_setting,
      'safe_to_auto_apply', v_safe_to_auto_apply,
      'request_start_result', v_request_start_result,
      'expand_result', v_expand_result,
      'process_result', v_process_result,
      'final_work_item_totals', v_final_work_item_totals,
      'status', v_correction_disposition,
      'requires_user_action', v_correction_disposition IN ('ACTION_REQUIRED', 'AMBIGUOUS', 'BLOCKED', 'FAILED'),
      'blocker', v_exact_mapping_required_blocker
    ),
    'consumed_carry_forward_count', COALESCE(v_consumed_carry_forward_count, 0),
    'carry_forward_mark_consumed_result', COALESCE(v_carry_forward_mark_result, '{}'::jsonb),
    'money_movement_classification', jsonb_build_object(
      'cash_state', v_event_cash_state,
      'is_final_money_moved', v_event_is_final_paid,
      'is_terminal_no_money', v_event_is_terminal_no_money,
      'is_pending_non_final', v_event_is_pending_non_final,
      'normalised_state', v_normalised_state,
      'reason', COALESCE(v_money_movement_classification.reason, NULL::text)
    ),
    'grouped_alert_summary_impact', jsonb_build_object(
      'alert_kind', CASE
        WHEN COALESCE(v_existing_transfer_is_final_paid, false) AND v_event_is_terminal_no_money THEN 'PAID_SETTLED_RECOVERY_REQUIRED'
        WHEN v_event_is_final_paid THEN NULL::text
        WHEN v_event_required_action = 'AMEND_AND_RECOVER_OVERPAYMENT' THEN 'PAID_SETTLED_RECOVERY_REQUIRED'
        WHEN v_event_required_action = 'CHECK_PROVIDER_STATUS' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN v_event_required_action = 'NO_MONEY_UNWIND_AND_RECALCULATE' THEN CASE WHEN COALESCE(v_auto_setting, false) THEN 'AUTO_UNWIND_PROGRESS' ELSE 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' END
        ELSE NULL::text
      END,
      'pay_batch_id', CASE WHEN v_pay_batch_id IS NULL THEN NULL ELSE v_pay_batch_id::text END,
      'pay_bank_transfer_id', CASE WHEN v_pay_bank_transfer_id IS NULL THEN NULL ELSE v_pay_bank_transfer_id::text END,
      'link_target', 'CURRENT_PAYMENT_STATUS'
    ),
    'policy_x_checked', true
  ) || v_settlement_intent_json;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_BANK_EVENT_INGEST_ERROR',
      jsonb_build_object(
        'event_keys', COALESCE(
          (
            SELECT jsonb_agg(event_key ORDER BY event_key)
            FROM jsonb_object_keys(COALESCE(p_event_json, '{}'::jsonb)) AS event_key
          ),
          '[]'::jsonb
        ),
        'event_hash', CASE
          WHEN p_event_json IS NULL THEN NULL::text
          ELSE encode(extensions.digest(convert_to(p_event_json::text, 'UTF8'), 'sha256'), 'hex')
        END,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      'BANK_EVENT_INGEST',
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;

ALTER FUNCTION public.pay_bank_event_ingest(jsonb,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_bank_event_ingest(jsonb,uuid,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_bank_event_ingest(jsonb,uuid,jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_bank_event_ingest(jsonb,uuid,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_bank_event_ingest(jsonb,uuid,jsonb) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_bank_event_ingest(jsonb,uuid,jsonb) TO service_role;
