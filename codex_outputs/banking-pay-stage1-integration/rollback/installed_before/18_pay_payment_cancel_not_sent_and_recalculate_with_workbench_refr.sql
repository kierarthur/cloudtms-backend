-- Exact installed TEST rollback definition and ACL captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: ef8ae04dc5d89e718f36b98ae174d054

CREATE OR REPLACE FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(p_pay_batch_id uuid, p_selection_json jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_reason text DEFAULT NULL::text, p_idempotency_key text DEFAULT NULL::text, p_confirmation_json jsonb DEFAULT '{}'::jsonb, p_source_workbench_session_id uuid DEFAULT NULL::uuid, p_expected_source_session_version bigint DEFAULT NULL::bigint, p_replacement_idempotency_key text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_actor_is_active boolean := false;
  v_source_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_cancellation_result jsonb := '{}'::jsonb;
  v_process_result jsonb := '{}'::jsonb;
  v_replacement_result jsonb := NULL::jsonb;
  v_final_result jsonb := NULL::jsonb;
  v_replay_metadata_json jsonb := NULL::jsonb;
  v_stored_result_json jsonb := NULL::jsonb;
  v_stored_input_fingerprint text := NULL::text;
  v_input_fingerprint text := NULL::text;
  v_replacement_idempotency_key text := NULLIF(
    BTRIM(COALESCE(p_replacement_idempotency_key, '')),
    ''
  );
  v_cancellation_idempotency_key text := NULLIF(
    BTRIM(COALESCE(p_idempotency_key, '')),
    ''
  );
  v_cancellation_ok boolean := false;
  v_payable_state_changed boolean := false;
  v_process_applied_count integer := 0;
  v_voided_item_count integer := 0;
  v_voided_transfer_count integer := 0;
  v_released_reservation_count integer := 0;
  v_restored_finance_component_count integer := 0;
  v_carry_forward_created_count integer := 0;
  v_carry_forward_released_count integer := 0;
  v_replacement_session_id_text text := NULL::text;
  v_replacement_session_id uuid := NULL::uuid;
  v_replacement_session_version bigint := NULL::bigint;
  v_replacement_created boolean := false;
  v_replacement_reused boolean := false;
  v_work_queued boolean := false;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_BATCH_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_BATCH_REQUIRED'
            )::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_REQUIRED'
            )::text;
  END IF;

  IF p_selection_json IS NOT NULL
     AND COALESCE(jsonb_typeof(p_selection_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_SELECTION_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_SELECTION_MUST_BE_OBJECT'
            )::text;
  END IF;

  IF p_confirmation_json IS NOT NULL
     AND COALESCE(jsonb_typeof(p_confirmation_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_CONFIRMATION_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_CONFIRMATION_MUST_BE_OBJECT'
            )::text;
  END IF;

  IF p_source_workbench_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_SESSION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_SESSION_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  IF p_expected_source_session_version IS NULL
     OR p_expected_source_session_version < 1 THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_EXPECTED_VERSION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_EXPECTED_VERSION_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text
            )::text;
  END IF;

  IF v_replacement_idempotency_key IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_IDEMPOTENCY_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_IDEMPOTENCY_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text
            )::text;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor_user
    WHERE actor_user.id = p_actor_user_id
      AND COALESCE(actor_user.is_active, false) = true
  )
  INTO v_actor_is_active;

  IF COALESCE(v_actor_is_active, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_NOT_ALLOWED',
              'actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  v_input_fingerprint := md5(
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'selection_hash', md5(COALESCE(p_selection_json, '{}'::jsonb)::text),
      'actor_user_id', p_actor_user_id::text,
      'reason', NULLIF(BTRIM(COALESCE(p_reason, '')), ''),
      'cancellation_idempotency_key', v_cancellation_idempotency_key,
      'ui_mode', UPPER(REPLACE(NULLIF(BTRIM(COALESCE(
        p_confirmation_json->>'ui_mode',
        p_selection_json->>'ui_mode',
        ''
      )), ''), '-', '_')),
      'source_workbench_session_id', p_source_workbench_session_id::text,
      'expected_source_session_version', p_expected_source_session_version,
      'replacement_idempotency_key', v_replacement_idempotency_key
    )::text
  );

  PERFORM pg_advisory_xact_lock(
    pg_catalog.hashtext('public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refresh'),
    pg_catalog.hashtext(v_replacement_idempotency_key)
  );

  SELECT source_session.*
  INTO v_source_session_row
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id = p_source_workbench_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_SESSION_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text
            )::text;
  END IF;

  IF v_source_session_row.replacement_session_id IS NOT NULL
     AND v_source_session_row.replacement_idempotency_key IS DISTINCT FROM v_replacement_idempotency_key THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_ALREADY_REPLACED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_ALREADY_REPLACED',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text,
              'replacement_session_id', v_source_session_row.replacement_session_id::text,
              'existing_replacement_idempotency_key', v_source_session_row.replacement_idempotency_key,
              'requested_replacement_idempotency_key', v_replacement_idempotency_key
            )::text;
  END IF;

  IF v_source_session_row.replacement_session_id IS NOT NULL
     AND v_source_session_row.replacement_idempotency_key = v_replacement_idempotency_key THEN
    SELECT replay_audit.after_json
    INTO v_replay_metadata_json
    FROM public.audit_events AS replay_audit
    WHERE replay_audit.object_type = 'banking_pay_workbench_session'
      AND replay_audit.object_id_text = p_source_workbench_session_id::text
      AND replay_audit.action = 'PAYMENT_CANCEL_WORKBENCH_SESSION_REPLACED'
      AND replay_audit.after_json->>'replacement_idempotency_key' = v_replacement_idempotency_key
    ORDER BY replay_audit.ts_utc DESC,
             replay_audit.id DESC
    LIMIT 1;

    v_stored_input_fingerprint := NULLIF(
      BTRIM(COALESCE(v_replay_metadata_json->>'input_fingerprint', '')),
      ''
    );
    v_stored_result_json := CASE
      WHEN jsonb_typeof(v_replay_metadata_json->'result') = 'object'
        THEN v_replay_metadata_json->'result'
      ELSE NULL::jsonb
    END;

    IF v_stored_input_fingerprint IS NULL OR v_stored_result_json IS NULL THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_IDEMPOTENCY_RESULT_INCOMPLETE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_IDEMPOTENCY_RESULT_INCOMPLETE',
                'pay_batch_id', p_pay_batch_id::text,
                'source_workbench_session_id', p_source_workbench_session_id::text,
                'replacement_session_id', v_source_session_row.replacement_session_id::text,
                'replacement_idempotency_key', v_replacement_idempotency_key
              )::text;
    END IF;

    IF v_stored_input_fingerprint IS DISTINCT FROM v_input_fingerprint THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_IDEMPOTENCY_CONFLICT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_IDEMPOTENCY_CONFLICT',
                'pay_batch_id', p_pay_batch_id::text,
                'source_workbench_session_id', p_source_workbench_session_id::text,
                'replacement_idempotency_key', v_replacement_idempotency_key
              )::text;
    END IF;

    v_replacement_result := public.pay_workbench_session_replace_after_mutation(
      p_actor_user_id => p_actor_user_id,
      p_source_session_id => p_source_workbench_session_id,
      p_replacement_idempotency_key => v_replacement_idempotency_key,
      p_expected_source_version => p_expected_source_session_version,
      p_reason => 'PAYMENT_CANCEL_NOT_SENT_AND_RECALCULATE'
    );

    IF jsonb_typeof(COALESCE(v_replacement_result, '{}'::jsonb)) <> 'object'
       OR LOWER(BTRIM(COALESCE(v_replacement_result->>'ok', 'false')))
          NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLAY_REPLACEMENT_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLAY_REPLACEMENT_INVALID',
                'pay_batch_id', p_pay_batch_id::text,
                'source_workbench_session_id', p_source_workbench_session_id::text,
                'replacement_idempotency_key', v_replacement_idempotency_key,
                'replacement_result', COALESCE(v_replacement_result, '{}'::jsonb)
              )::text;
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_stored_result_json->>'replacement_session_id', '')), '')
       IS DISTINCT FROM v_source_session_row.replacement_session_id::text THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_IDEMPOTENCY_REPLACEMENT_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_IDEMPOTENCY_REPLACEMENT_MISMATCH',
                'pay_batch_id', p_pay_batch_id::text,
                'source_workbench_session_id', p_source_workbench_session_id::text,
                'stored_replacement_session_id', v_stored_result_json->>'replacement_session_id',
                'actual_replacement_session_id', v_source_session_row.replacement_session_id::text
              )::text;
    END IF;

    RETURN v_stored_result_json;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_source_session_row.status, ''))) <> 'OPEN'
     OR v_source_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_SOURCE_SESSION_NOT_OPEN',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text,
              'source_session_status', v_source_session_row.status,
              'source_discarded_at_utc', v_source_session_row.discarded_at_utc
            )::text;
  END IF;

  IF v_source_session_row.version IS DISTINCT FROM p_expected_source_session_version THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_EXPECTED_VERSION_CONFLICT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_EXPECTED_VERSION_CONFLICT',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text,
              'expected_source_session_version', p_expected_source_session_version,
              'actual_source_session_version', v_source_session_row.version
            )::text;
  END IF;

  v_cancellation_result := public.pay_payment_cancel_not_sent_and_recalculate(
    p_pay_batch_id => p_pay_batch_id,
    p_selection_json => p_selection_json,
    p_actor_user_id => p_actor_user_id,
    p_reason => p_reason,
    p_idempotency_key => p_idempotency_key,
    p_confirmation_json => p_confirmation_json
  );

  IF v_cancellation_result IS NULL
     OR jsonb_typeof(v_cancellation_result) <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_CANCELLATION_RESULT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_CANCELLATION_RESULT_INVALID',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  v_cancellation_ok := LOWER(BTRIM(COALESCE(v_cancellation_result->>'ok', 'false')))
    IN ('true', 't', '1', 'yes', 'y', 'on');

  v_process_result := CASE
    WHEN jsonb_typeof(v_cancellation_result->'process_result') = 'object'
      THEN COALESCE(v_cancellation_result->'process_result', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_process_applied_count := CASE
    WHEN COALESCE(v_process_result #>> '{totals,applied}', '') ~ '^[0-9]{1,9}$'
      THEN (v_process_result #>> '{totals,applied}')::integer
    WHEN COALESCE(v_process_result->>'applied', '') ~ '^[0-9]{1,9}$'
      THEN (v_process_result->>'applied')::integer
    WHEN COALESCE(v_cancellation_result->>'progress_completed', '') ~ '^[0-9]{1,9}$'
         AND COALESCE(v_cancellation_result->>'voided_item_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_cancellation_result->>'voided_item_count')::integer
    ELSE 0
  END;

  v_voided_item_count := CASE
    WHEN COALESCE(v_cancellation_result->>'voided_item_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_cancellation_result->>'voided_item_count')::integer
    ELSE 0
  END;

  v_voided_transfer_count := CASE
    WHEN COALESCE(v_cancellation_result->>'voided_transfer_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_cancellation_result->>'voided_transfer_count')::integer
    ELSE 0
  END;

  v_released_reservation_count := CASE
    WHEN COALESCE(v_cancellation_result->>'released_reservation_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_cancellation_result->>'released_reservation_count')::integer
    ELSE 0
  END;

  v_restored_finance_component_count := CASE
    WHEN COALESCE(v_cancellation_result->>'restored_finance_component_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_cancellation_result->>'restored_finance_component_count')::integer
    ELSE 0
  END;

  v_carry_forward_created_count := CASE
    WHEN COALESCE(v_cancellation_result->>'carry_forward_created_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_cancellation_result->>'carry_forward_created_count')::integer
    ELSE 0
  END;

  v_carry_forward_released_count := CASE
    WHEN COALESCE(v_cancellation_result->>'carry_forward_released_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_cancellation_result->>'carry_forward_released_count')::integer
    ELSE 0
  END;

  v_payable_state_changed := v_cancellation_ok
    AND (
      v_process_applied_count > 0
      OR v_voided_item_count > 0
      OR v_voided_transfer_count > 0
      OR v_released_reservation_count > 0
      OR v_restored_finance_component_count > 0
      OR v_carry_forward_created_count > 0
      OR v_carry_forward_released_count > 0
    );

  IF v_payable_state_changed IS NOT TRUE THEN
    RETURN v_cancellation_result
      || jsonb_build_object(
        'payable_state_changed', false,
        'workbench_refresh_required', false,
        'workbench_replacement_attempted', false,
        'workbench_session_replaced', false,
        'source_workbench_session_id', p_source_workbench_session_id::text,
        'expected_source_session_version', p_expected_source_session_version,
        'replacement_idempotency_key', v_replacement_idempotency_key,
        'replacement_session_id', NULL::text,
        'replacement_session_version', NULL::bigint,
        'replacement_created', false,
        'replacement_reused', false,
        'work_queued', false,
        'workbench_refresh', NULL::jsonb
      );
  END IF;

  v_replacement_result := public.pay_workbench_session_replace_after_mutation(
    p_actor_user_id => p_actor_user_id,
    p_source_session_id => p_source_workbench_session_id,
    p_replacement_idempotency_key => v_replacement_idempotency_key,
    p_expected_source_version => p_expected_source_session_version,
    p_reason => 'PAYMENT_CANCEL_NOT_SENT_AND_RECALCULATE'
  );

  IF jsonb_typeof(COALESCE(v_replacement_result, '{}'::jsonb)) <> 'object'
     OR LOWER(BTRIM(COALESCE(v_replacement_result->>'ok', 'false')))
        NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_FAILED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_FAILED',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text,
              'replacement_idempotency_key', v_replacement_idempotency_key,
              'replacement_result', COALESCE(v_replacement_result, '{}'::jsonb)
            )::text;
  END IF;

  v_replacement_session_id_text := NULLIF(
    BTRIM(COALESCE(v_replacement_result->>'replacement_session_id', '')),
    ''
  );

  IF v_replacement_session_id_text IS NULL THEN
    v_replacement_session_id_text := NULLIF(
      BTRIM(COALESCE(v_replacement_result->>'session_id', '')),
      ''
    );
  END IF;

  IF COALESCE(v_replacement_session_id_text, '')
     !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_SESSION_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_SESSION_INVALID',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text,
              'replacement_result', v_replacement_result
            )::text;
  END IF;

  v_replacement_session_id := v_replacement_session_id_text::uuid;

  SELECT source_session.*
  INTO v_source_session_row
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id = p_source_workbench_session_id;

  IF NOT FOUND
     OR v_source_session_row.replacement_session_id IS DISTINCT FROM v_replacement_session_id
     OR v_source_session_row.replacement_idempotency_key IS DISTINCT FROM v_replacement_idempotency_key THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_LINK_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_WORKBENCH_REPLACEMENT_LINK_MISMATCH',
              'pay_batch_id', p_pay_batch_id::text,
              'source_workbench_session_id', p_source_workbench_session_id::text,
              'expected_replacement_session_id', v_replacement_session_id_text,
              'actual_replacement_session_id', CASE
                WHEN v_source_session_row.replacement_session_id IS NULL THEN NULL::text
                ELSE v_source_session_row.replacement_session_id::text
              END,
              'expected_replacement_idempotency_key', v_replacement_idempotency_key,
              'actual_replacement_idempotency_key', v_source_session_row.replacement_idempotency_key
            )::text;
  END IF;

  v_replacement_session_version := CASE
    WHEN COALESCE(v_replacement_result->>'replacement_session_version', '') ~ '^[0-9]{1,18}$'
      THEN (v_replacement_result->>'replacement_session_version')::bigint
    WHEN COALESCE(v_replacement_result->>'session_version', '') ~ '^[0-9]{1,18}$'
      THEN (v_replacement_result->>'session_version')::bigint
    ELSE NULL::bigint
  END;

  v_replacement_created := LOWER(BTRIM(COALESCE(
    v_replacement_result->>'replacement_created',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_replacement_reused := LOWER(BTRIM(COALESCE(
    v_replacement_result->>'replacement_reused',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_work_queued := LOWER(BTRIM(COALESCE(
    v_replacement_result->>'work_queued',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_final_result := v_cancellation_result
    || jsonb_build_object(
      'payable_state_changed', true,
      'workbench_refresh_required', true,
      'workbench_replacement_attempted', true,
      'workbench_session_replaced', true,
      'source_workbench_session_id', p_source_workbench_session_id::text,
      'expected_source_session_version', p_expected_source_session_version,
      'replacement_idempotency_key', v_replacement_idempotency_key,
      'replacement_session_id', v_replacement_session_id_text,
      'replacement_session_version', v_replacement_session_version,
      'replacement_created', v_replacement_created,
      'replacement_reused', v_replacement_reused,
      'work_queued', v_work_queued
    )
    || jsonb_build_object(
      'source_session_obsolete', LOWER(BTRIM(COALESCE(
        v_replacement_result->>'source_session_obsolete',
        'true'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'replacement_session_status', v_replacement_result->>'replacement_session_status',
      'replacement_progress_state', v_replacement_result->>'replacement_progress_state',
      'replacement_lifecycle_action', v_replacement_result->>'replacement_lifecycle_action',
      'old_rows_retained', LOWER(BTRIM(COALESCE(
        v_replacement_result->>'old_rows_retained',
        'true'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'atomic_replacement', LOWER(BTRIM(COALESCE(
        v_replacement_result->>'atomic_replacement',
        'true'
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'workbench_refresh', v_replacement_result
    );

  PERFORM public._audit_insert(
    'banking_pay_workbench_session',
    p_source_workbench_session_id::text,
    'PAYMENT_CANCEL_WORKBENCH_SESSION_REPLACED',
    NULL::jsonb,
    jsonb_build_object(
      'input_fingerprint', v_input_fingerprint,
      'pay_batch_id', p_pay_batch_id::text,
      'cancellation_idempotency_key', v_cancellation_idempotency_key,
      'replacement_idempotency_key', v_replacement_idempotency_key,
      'replacement_session_id', v_replacement_session_id_text,
      'completed_at_utc', v_now::text,
      'result', v_final_result
    ),
    'PAYMENT_CANCEL_NOT_SENT_AND_RECALCULATE',
    p_actor_user_id
  );

  RETURN v_final_result;
END;
$function$;

ALTER FUNCTION pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) TO service_role;
