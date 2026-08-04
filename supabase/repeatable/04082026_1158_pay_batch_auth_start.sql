-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Preserves the installed function identity; no overload is added.

CREATE OR REPLACE FUNCTION public.pay_batch_auth_start(p_pay_batch_id uuid, p_schedule_kind text, p_scheduled_at_utc timestamp with time zone, p_funding_account_ref text, p_warning_hours_json jsonb, p_actor_user_id uuid, p_actor_intent text DEFAULT NULL::text, p_execution_mode text DEFAULT 'STANDARD_BANK'::text, p_payment_date date DEFAULT NULL::date, p_pay_channel_scope text DEFAULT 'ALL'::text, p_suppress_remittances boolean DEFAULT false, p_suppress_remittances_confirmed boolean DEFAULT false, p_csv_uploaded_confirmed boolean DEFAULT false, p_csv_bank_confirm_ref text DEFAULT NULL::text, p_external_settlement_comment text DEFAULT NULL::text, p_operation_id uuid DEFAULT NULL::uuid, p_idempotency_key text DEFAULT NULL::text, p_freshness_result_hash text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO pg_catalog, private, extensions, pg_temp
 SET statement_timeout TO '6000ms'
 SET lock_timeout TO '1000ms'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_kind text := upper(btrim(coalesce(p_schedule_kind, '')));
  v_intent text := upper(btrim(coalesce(p_actor_intent, '')));
  v_execution_mode text := upper(btrim(coalesce(p_execution_mode, 'STANDARD_BANK')));
  v_pay_channel_scope text := upper(btrim(coalesce(p_pay_channel_scope, 'ALL')));
  v_actor_row record;
  v_batch_row public.pay_batches%ROWTYPE;
  v_cfg_row public.settings_defaults%ROWTYPE;
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_prepare_json jsonb := '{}'::jsonb;
  v_summary_json jsonb := '{}'::jsonb;
  v_existing_auth_id uuid := NULL::uuid;
  v_existing_auth_state text := NULL::text;
  v_existing_auth_intent_json jsonb := '{}'::jsonb;
  v_existing_auth_operation_id text := NULL::text;
  v_existing_auth_idempotency_key text := NULL::text;
  v_conflicting_auth_id uuid := NULL::uuid;
  v_conflicting_auth_state text := NULL::text;
  v_conflicting_auth_operation_id text := NULL::text;
  v_conflicting_auth_idempotency_key text := NULL::text;
  v_requested_idempotency_key text := NULL::text;
  v_effective_freshness_result_hash text := NULL::text;
  v_effective_freshness_scope_hash text := NULL::text;
  v_auth_id uuid := NULL::uuid;
  v_auth_state text := 'AWAITING';
  v_required_quantity integer := 1;
  v_use_golden_key boolean := false;
  v_payment_date date := p_payment_date;
  v_scheduled_at_utc timestamptz := NULL::timestamptz;
  v_funding_account_ref text := NULL::text;
  v_warning_hours_json jsonb := '[]'::jsonb;
  v_execution_intent_json jsonb := '{}'::jsonb;
  v_next_required_phase text := 'WAIT_FOR_AUTHORISATION';
  v_ready boolean := false;
  v_blocker_count integer := 0;
  v_pending_transfer_count integer := 0;
  v_authorisation_ready_transfer_count integer := 0;
  v_local_only_transfer_count integer := 0;
  v_provider_attempt_or_evidence_transfer_count integer := 0;
  v_provider_or_ambiguous_evidence_transfer_count integer := 0;
  v_unsafe_transfer_count integer := 0;
  v_blocked_transfer_count integer := 0;
  v_canonical_pending_status_transfer_count integer := 0;
  v_scoped_operation_scope_count integer := 0;
  v_scoped_scope_prepared_count integer := 0;
  v_scoped_scope_failed_count integer := 0;
  v_scoped_scope_skipped_count integer := 0;
  v_scoped_scope_without_transfer_count integer := 0;
  v_non_cancellable_auth_request_count integer := 0;
  v_auth_request_retry_blocker_count integer := 0;
  v_execution_commit_state text := 'NOT_SUBMITTED';
  v_auth_start_path text := 'FULL_PREPARE_PATH';
  v_used_operation_scope_proof boolean := false;
  v_provider_submit_chunk_risk_count integer := 0;
  v_operation_provider_submit_marker_count integer := 0;
  v_transfer_amount_mismatch_count integer := 0;
  v_transfer_currency_mismatch_count integer := 0;
  v_transfer_status_not_pending_count integer := 0;
  v_transfer_external_state_count integer := 0;
  v_transfer_identity_mismatch_count integer := 0;
  v_scope_provider_not_ready_count integer := 0;
  v_scope_prepared_hash_missing_count integer := 0;
  v_scope_prepared_amount_mismatch_count integer := 0;
  v_scope_item_rollup_pending_count integer := 0;
  v_payout_instruction_missing_count integer := 0;
  v_scope_amount_total numeric(14,2) := 0;
  v_transfer_amount_total numeric(14,2) := 0;
  v_scope_item_amount_total numeric(14,2) := 0;
  v_batch_item_amount_total numeric(14,2) := 0;
  v_auth_freshness_json jsonb := '{}'::jsonb;
  v_auth_carry_forward_freshness_json jsonb := '{}'::jsonb;
  v_auth_carry_forward_blocker_count integer := 0;
  v_auth_freshness_blocker_reasons jsonb := '[]'::jsonb;
  v_global_paye_missing_count integer := 0;
  v_global_paye_zero_count integer := 0;
  v_global_paye_positive_count integer := 0;
  v_global_positive_bank_payment_count integer := 0;
  v_global_positive_bank_payment_total numeric(14,2) := 0;
  v_global_invalid_payment_row_count integer := 0;
  v_current_paye_net_state_hash text := NULL::text;
  v_all_bank_payment_projection_hash text := NULL::text;
  v_scoped_paye_missing_count integer := 0;
  v_scoped_explicit_zero_count integer := 0;
  v_scoped_paye_positive_count integer := 0;
  v_scoped_positive_bank_payment_count integer := 0;
  v_scoped_positive_bank_payment_total numeric(14,2) := 0;
  v_scoped_invalid_payment_row_count integer := 0;
  v_scoped_paye_net_state_hash text := NULL::text;
  v_current_bank_payment_projection_hash text := NULL::text;
  v_operation_no_bank_payment_marker boolean := false;
  v_operation_allow_explicit_zero_marker boolean := false;
  v_operation_scoped_no_transfer_marker boolean := false;
  v_operation_projection_scope text := NULL::text;
  v_operation_no_bank_proof_json jsonb := '{}'::jsonb;
  v_operation_proof_source text := NULL::text;
  v_operation_proof_operation_id uuid := NULL::uuid;
  v_operation_proof_pay_batch_id uuid := NULL::uuid;
  v_operation_expected_global_paye_net_state_hash text := NULL::text;
  v_operation_expected_global_bank_payment_projection_hash text := NULL::text;
  v_operation_expected_scoped_paye_net_state_hash text := NULL::text;
  v_operation_expected_scoped_bank_payment_projection_hash text := NULL::text;
  v_operation_expected_global_missing_count integer := NULL::integer;
  v_operation_expected_global_zero_count integer := NULL::integer;
  v_operation_expected_global_positive_count integer := NULL::integer;
  v_operation_expected_global_invalid_count integer := NULL::integer;
  v_operation_expected_scoped_missing_count integer := NULL::integer;
  v_operation_expected_scoped_zero_count integer := NULL::integer;
  v_operation_expected_scoped_positive_count integer := NULL::integer;
  v_operation_expected_scoped_invalid_count integer := NULL::integer;
  v_server_explicit_zero_scope_eligible boolean := false;
  v_server_scoped_no_transfer_eligible boolean := false;
  v_server_no_bank_payment_eligible boolean := false;
  v_allow_explicit_zero_no_bank_scopes_validated boolean := false;
  v_scoped_no_transfer_execution_validated boolean := false;
  v_no_bank_payment_validated boolean := false;
  v_seed_proof_validated boolean := false;
  v_server_projection_proof_json jsonb := '{}'::jsonb;
  v_no_bank_scope_artifact_count integer := 0;
  v_no_bank_batch_transfer_count integer := 0;
  v_no_bank_transfer_event_count integer := 0;
  v_no_bank_provider_attempt_count integer := 0;
  v_bank_csv_export_json jsonb := '{}'::jsonb;
  v_stored_csv_scope text := NULL::text;
  v_stored_csv_paye_net_state_hash text := NULL::text;
  v_stored_csv_bank_projection_hash text := NULL::text;
  v_stored_csv_row_count integer := NULL::integer;
  v_stored_csv_total numeric(14,2) := NULL::numeric;
  v_stored_csv_generated_at_utc text := NULL::text;
  v_stored_csv_generated_by_user_id text := NULL::text;
  v_stored_csv_filename text := NULL::text;
  v_stored_csv_zero_row boolean := false;
  v_csv_evidence_validated boolean := false;
  v_csv_evidence_json jsonb := NULL::jsonb;
  v_effective_csv_uploaded_confirmed boolean := false;
  v_effective_csv_bank_confirm_ref text := NULL::text;
  v_effective_external_settlement_comment text := NULL::text;
  v_prepare_blockers jsonb := '[]'::jsonb;
  v_prepare_unwaived_blockers jsonb := '[]'::jsonb;
  v_prepare_waived_blockers jsonb := '[]'::jsonb;
  v_supplied_execution_mode_raw text := upper(btrim(coalesce(p_execution_mode, 'STANDARD_BANK')));
  v_durable_execution_mode_raw text := NULL::text;
  v_durable_execution_mode text := NULL::text;
  v_durable_operation_idempotency_key text := NULL::text;
  v_manual_confirmation_mode text := NULL::text;
  v_prepare_execution_mode text := NULL::text;
  v_prepare_next_required_phase text := NULL::text;
  v_existing_auth_execution_mode text := NULL::text;
  v_existing_auth_compatible boolean := false;
  v_existing_auth_cross_operation boolean := false;
  v_existing_auth_cross_operation_authorised boolean := false;
  v_existing_auth_compatibility_json jsonb := '{}'::jsonb;
  v_auth_lookup_pass integer := 0;
  v_old_operation_row public.banking_pay_operations%ROWTYPE;
  v_old_operation_id uuid := NULL::uuid;
  v_old_operation_execution_mode_raw text := NULL::text;
  v_old_operation_execution_mode text := NULL::text;
  v_old_auth_execution_mode text := NULL::text;
  v_old_auth_scope text := NULL::text;
  v_old_operation_reset_terminal boolean := false;
  v_old_operation_manual_reset_marker boolean := false;
  v_stale_auth_request_cancelled boolean := false;
  v_stale_auth_cancelled_request_id uuid := NULL::uuid;
  v_stale_auth_cancelled_operation_id uuid := NULL::uuid;
  v_stale_auth_tokens_voided integer := 0;
  v_stale_auth_safety_json jsonb := '{}'::jsonb;
  v_stale_auth_failed_checks jsonb := '[]'::jsonb;
  v_stale_provider_attempt_count integer := 0;
  v_stale_transfer_event_count integer := 0;
  v_stale_scope_provider_evidence_count integer := 0;
  v_stale_transfer_provider_evidence_count integer := 0;
  v_stale_provider_chunk_evidence_count integer := 0;
  v_stale_settlement_scope_count integer := 0;
  v_stale_remittance_scope_count integer := 0;
  v_stale_correction_request_count integer := 0;
  v_stale_correction_item_count integer := 0;
  v_stale_carry_forward_count integer := 0;
  v_stale_advance_patch_count integer := 0;
  v_stale_advance_reservation_count integer := 0;
  v_stale_advance_payout_count integer := 0;
  v_stale_other_active_operation_count integer := 0;
  v_stale_remaining_active_auth_count integer := 0;
  v_expected_manual_confirmation_mode text := NULL::text;
  v_existing_auth_schedule_kind text := NULL::text;
  v_existing_auth_scheduled_at_utc timestamptz := NULL::timestamptz;
  v_existing_auth_funding_account_ref text := NULL::text;
  v_existing_auth_warning_hours_json jsonb := NULL::jsonb;
  v_conflicting_auth_intent_json jsonb := '{}'::jsonb;
  v_existing_auth_identity_matches boolean := false;
  v_existing_auth_mode_specific_proof_matches boolean := false;
  v_old_operation_found boolean := false;
  v_old_operation_scope_compatible boolean := false;
  v_mutation_guard jsonb := '{}'::jsonb;
  v_current_active_scope_hash text := NULL::text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('OPERATION_ADVANCE');
  PERFORM set_config('lock_timeout', '1s', true);

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_auth_start: pay_batch_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_auth_start: actor_user_id is required';
  END IF;

  v_mutation_guard := private.pay_payment_mutation_guard_v1(
    p_pay_batch_id,
    NULL::uuid,
    'NEW_PAYMENT_ACTION'
  );

  IF COALESCE((v_mutation_guard->>'blocked')::boolean, true) THEN
    RAISE EXCEPTION 'PAYMENT_CHANGE_IN_PROGRESS'
      USING ERRCODE = 'P0001', DETAIL = JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', COALESCE(
          v_mutation_guard->>'code',
          'PAYMENT_CHANGE_IN_PROGRESS'
        ),
        'message', COALESCE(
          v_mutation_guard->>'message',
          'A payment change is in progress.'
        ),
        'pay_batch_id', p_pay_batch_id
      )::text;
  END IF;

  IF v_kind NOT IN ('IMMEDIATE', 'SCHEDULED') THEN
    RAISE EXCEPTION 'pay_batch_auth_start: invalid schedule_kind (IMMEDIATE|SCHEDULED)';
  END IF;

  IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
    RAISE EXCEPTION 'pay_batch_auth_start: invalid pay_channel_scope';
  END IF;

  IF v_execution_mode NOT IN ('STANDARD_BANK', 'BANK', 'CSV', 'CSV_SETTLEMENT', 'EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN
    RAISE EXCEPTION 'pay_batch_auth_start: invalid execution_mode';
  END IF;

  v_execution_mode := CASE
    WHEN v_execution_mode IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
    WHEN v_execution_mode IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
    WHEN v_execution_mode IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
    ELSE v_execution_mode
  END;

  v_requested_idempotency_key := nullif(btrim(coalesce(p_idempotency_key, '')), '');

  SELECT actor_user.id,
         actor_user.is_active,
         actor_user.payment_authoriser,
         actor_user.payment_golden_key
  INTO v_actor_row
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id;

  IF v_actor_row.id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_auth_start: actor_user not found';
  END IF;

  IF coalesce(v_actor_row.is_active, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'pay_batch_auth_start: actor_user is not active';
  END IF;

  IF coalesce(v_actor_row.payment_authoriser, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'pay_batch_auth_start: actor_user must be a payment authoriser';
  END IF;

  v_use_golden_key := v_intent = 'USE_GOLDEN_KEY';
  IF v_use_golden_key AND coalesce(v_actor_row.payment_golden_key, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'pay_batch_auth_start: actor_user does not have payment golden key';
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batch_auth_start: pay_batch not found';
  END IF;

  IF p_operation_id IS NOT NULL THEN
    SELECT operation_row.*
    INTO v_operation_row
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_NOT_FOUND',
        'message', 'The payment execution operation was not found.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF UPPER(BTRIM(COALESCE(v_operation_row.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_TYPE_NOT_SUPPORTED_FOR_AUTH_START',
        'message', 'Payment authorisation must be bound to a payment execution operation.',
        'operation_id', p_operation_id::text,
        'operation_type', v_operation_row.operation_type,
        'pay_batch_id', p_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_operation_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_BATCH_MISMATCH',
        'message', 'The payment execution operation is not bound to the requested pay batch.',
        'operation_id', p_operation_id::text,
        'operation_pay_batch_id', CASE WHEN v_operation_row.pay_batch_id IS NULL THEN NULL::text ELSE v_operation_row.pay_batch_id::text END,
        'pay_batch_id', p_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_operation_row.actor_user_id IS NOT NULL AND v_operation_row.actor_user_id <> p_actor_user_id THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_ACTOR_MISMATCH',
        'message', 'The payment execution operation belongs to another actor.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_durable_execution_mode_raw := UPPER(BTRIM(COALESCE(v_operation_row.input_json->>'execution_mode', '')));
    v_durable_execution_mode := CASE
      WHEN v_durable_execution_mode_raw IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
      WHEN v_durable_execution_mode_raw IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
      WHEN v_durable_execution_mode_raw IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
      ELSE NULL::text
    END;

    IF v_durable_execution_mode IS NULL THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_EXECUTION_MODE_INVALID',
        'message', 'The durable payment execution mode is missing or unsupported.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'durable_execution_mode_raw', NULLIF(v_durable_execution_mode_raw, ''),
        'supported_execution_modes', JSONB_BUILD_ARRAY('STANDARD_BANK', 'CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_execution_mode IS DISTINCT FROM v_durable_execution_mode THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_EXECUTION_MODE_MISMATCH',
        'message', 'The requested authorisation mode does not match the durable payment execution mode.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'requested_execution_mode_raw', NULLIF(v_supplied_execution_mode_raw, ''),
        'requested_execution_mode', v_execution_mode,
        'durable_execution_mode_raw', NULLIF(v_durable_execution_mode_raw, ''),
        'durable_execution_mode', v_durable_execution_mode
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_execution_mode := v_durable_execution_mode;
    v_durable_operation_idempotency_key := NULLIF(BTRIM(COALESCE(v_operation_row.idempotency_key, '')), '');
    IF v_requested_idempotency_key IS NOT NULL
       AND v_durable_operation_idempotency_key IS NOT NULL
       AND v_requested_idempotency_key IS DISTINCT FROM v_durable_operation_idempotency_key THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_IDEMPOTENCY_KEY_MISMATCH',
        'message', 'The supplied authorisation idempotency key does not match the durable payment execution operation.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'supplied_idempotency_key_present', true,
        'durable_idempotency_key_present', true
      )::text USING ERRCODE = 'P0001';
    END IF;
    v_requested_idempotency_key := COALESCE(v_requested_idempotency_key, v_durable_operation_idempotency_key);
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_auth_projection_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_auth_projection_rows
  ON COMMIT DROP
  AS
  SELECT projection_row.*
  FROM public._pay_batch_bank_payment_projection_rows(
    p_pay_batch_id,
    'ALL'
  ) AS projection_row;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_auth_scoped_projection_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_auth_scoped_projection_rows
  ON COMMIT DROP
  AS
  SELECT projection_row.*
  FROM public._pay_batch_bank_payment_projection_rows(
    p_pay_batch_id,
    v_pay_channel_scope
  ) AS projection_row;

  SELECT
    COUNT(*) FILTER (
      WHERE projection_row.is_paye_net_state_row
        AND projection_row.paye_net_classification = 'MISSING'
    )::integer,
    COUNT(*) FILTER (
      WHERE projection_row.is_paye_net_state_row
        AND projection_row.paye_net_classification = 'ZERO'
    )::integer,
    COUNT(*) FILTER (
      WHERE projection_row.is_paye_net_state_row
        AND projection_row.paye_net_classification = 'POSITIVE'
    )::integer,
    COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer,
    ROUND(
      COALESCE(
        SUM(projection_row.amount) FILTER (WHERE projection_row.is_positive_bank_payment),
        0
      ),
      2
    )::numeric(14,2),
    COUNT(*) FILTER (
      WHERE (
        projection_row.paye_net_required IS TRUE
        AND COALESCE(projection_row.paye_net_classification, '') NOT IN ('MISSING', 'ZERO', 'POSITIVE')
      )
      OR (
        COALESCE(projection_row.paye_net_required, false) IS FALSE
        AND (
          projection_row.final_frozen_bank_amount IS NULL
          OR ROUND(projection_row.final_frozen_bank_amount, 2) <= 0
        )
      )
    )::integer,
    MAX(projection_row.paye_net_state_hash),
    MAX(projection_row.bank_payment_projection_hash)
  INTO
    v_global_paye_missing_count,
    v_global_paye_zero_count,
    v_global_paye_positive_count,
    v_global_positive_bank_payment_count,
    v_global_positive_bank_payment_total,
    v_global_invalid_payment_row_count,
    v_current_paye_net_state_hash,
    v_all_bank_payment_projection_hash
  FROM pg_temp.tmp_pay_batch_auth_projection_rows AS projection_row;

  SELECT
    COUNT(*) FILTER (
      WHERE projection_row.is_paye_net_state_row
        AND projection_row.paye_net_classification = 'MISSING'
    )::integer,
    COUNT(*) FILTER (
      WHERE projection_row.is_paye_net_state_row
        AND projection_row.paye_net_classification = 'ZERO'
    )::integer,
    COUNT(*) FILTER (
      WHERE projection_row.is_paye_net_state_row
        AND projection_row.paye_net_classification = 'POSITIVE'
    )::integer,
    COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer,
    ROUND(
      COALESCE(
        SUM(projection_row.amount) FILTER (WHERE projection_row.is_positive_bank_payment),
        0
      ),
      2
    )::numeric(14,2),
    COUNT(*) FILTER (
      WHERE (
        projection_row.paye_net_required IS TRUE
        AND COALESCE(projection_row.paye_net_classification, '') NOT IN ('MISSING', 'ZERO', 'POSITIVE')
      )
      OR (
        COALESCE(projection_row.paye_net_required, false) IS FALSE
        AND (
          projection_row.final_frozen_bank_amount IS NULL
          OR ROUND(projection_row.final_frozen_bank_amount, 2) <= 0
        )
      )
    )::integer,
    MAX(projection_row.paye_net_state_hash),
    MAX(projection_row.bank_payment_projection_hash)
  INTO
    v_scoped_paye_missing_count,
    v_scoped_explicit_zero_count,
    v_scoped_paye_positive_count,
    v_scoped_positive_bank_payment_count,
    v_scoped_positive_bank_payment_total,
    v_scoped_invalid_payment_row_count,
    v_scoped_paye_net_state_hash,
    v_current_bank_payment_projection_hash
  FROM pg_temp.tmp_pay_batch_auth_scoped_projection_rows AS projection_row;

  v_global_paye_missing_count := COALESCE(v_global_paye_missing_count, 0);
  v_global_paye_zero_count := COALESCE(v_global_paye_zero_count, 0);
  v_global_paye_positive_count := COALESCE(v_global_paye_positive_count, 0);
  v_global_positive_bank_payment_count := COALESCE(v_global_positive_bank_payment_count, 0);
  v_global_positive_bank_payment_total := ROUND(COALESCE(v_global_positive_bank_payment_total, 0), 2);
  v_global_invalid_payment_row_count := COALESCE(v_global_invalid_payment_row_count, 0);
  v_current_paye_net_state_hash := COALESCE(
    v_current_paye_net_state_hash,
    MD5(JSONB_BUILD_OBJECT(
      'pay_batch_id', p_pay_batch_id::text,
      'scope', 'ALL',
      'rows', '[]'::jsonb
    )::text)
  );
  v_all_bank_payment_projection_hash := COALESCE(
    v_all_bank_payment_projection_hash,
    MD5(JSONB_BUILD_OBJECT(
      'pay_batch_id', p_pay_batch_id::text,
      'scope', 'ALL',
      'rows', '[]'::jsonb
    )::text)
  );

  v_scoped_paye_missing_count := COALESCE(v_scoped_paye_missing_count, 0);
  v_scoped_explicit_zero_count := COALESCE(v_scoped_explicit_zero_count, 0);
  v_scoped_paye_positive_count := COALESCE(v_scoped_paye_positive_count, 0);
  v_scoped_positive_bank_payment_count := COALESCE(v_scoped_positive_bank_payment_count, 0);
  v_scoped_positive_bank_payment_total := ROUND(COALESCE(v_scoped_positive_bank_payment_total, 0), 2);
  v_scoped_invalid_payment_row_count := COALESCE(v_scoped_invalid_payment_row_count, 0);
  v_scoped_paye_net_state_hash := COALESCE(
    v_scoped_paye_net_state_hash,
    MD5(JSONB_BUILD_OBJECT(
      'pay_batch_id', p_pay_batch_id::text,
      'scope', v_pay_channel_scope,
      'rows', '[]'::jsonb
    )::text)
  );
  v_current_bank_payment_projection_hash := COALESCE(
    v_current_bank_payment_projection_hash,
    MD5(JSONB_BUILD_OBJECT(
      'pay_batch_id', p_pay_batch_id::text,
      'scope', v_pay_channel_scope,
      'rows', '[]'::jsonb
    )::text)
  );

  IF v_global_paye_missing_count > 0 THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_BATCH_AUTH_START',
      'code', 'PAYE_NET_REQUIRED_FOR_EXECUTION',
      'message', 'Every required PAYE net amount must be explicitly entered or imported before payment authorisation can start.',
      'pay_batch_id', p_pay_batch_id::text,
      'pay_channel_scope', v_pay_channel_scope,
      'missing_explicit_paye_input_count', v_global_paye_missing_count
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_server_explicit_zero_scope_eligible := (
    v_global_paye_missing_count = 0
    AND v_scoped_explicit_zero_count > 0
    AND v_scoped_invalid_payment_row_count = 0
  );
  v_server_scoped_no_transfer_eligible := (
    v_server_explicit_zero_scope_eligible
    AND v_scoped_positive_bank_payment_count = 0
  );
  v_server_no_bank_payment_eligible := (
    v_server_scoped_no_transfer_eligible
    AND v_global_positive_bank_payment_count = 0
    AND v_global_invalid_payment_row_count = 0
  );

  IF p_operation_id IS NOT NULL THEN
    v_operation_no_bank_proof_json := CASE
      WHEN JSONB_TYPEOF(v_operation_row.progress_json->'no_bank_payment_proof') = 'object'
        THEN COALESCE(v_operation_row.progress_json->'no_bank_payment_proof', '{}'::jsonb)
      ELSE '{}'::jsonb
    END;

    v_operation_proof_source := UPPER(NULLIF(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'proof_source',
      v_operation_row.progress_json->>'proof_source',
      ''
    )), ''));

    IF NULLIF(BTRIM(COALESCE(v_operation_no_bank_proof_json->>'operation_id', '')), '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_operation_proof_operation_id := (v_operation_no_bank_proof_json->>'operation_id')::uuid;
    END IF;
    IF NULLIF(BTRIM(COALESCE(v_operation_no_bank_proof_json->>'pay_batch_id', '')), '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_operation_proof_pay_batch_id := (v_operation_no_bank_proof_json->>'pay_batch_id')::uuid;
    END IF;

    v_operation_no_bank_payment_marker := LOWER(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'no_bank_payment_execution',
      v_operation_row.progress_json->>'no_bank_payment_execution',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_operation_allow_explicit_zero_marker := LOWER(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'allow_explicit_zero_no_bank_scopes',
      v_operation_row.progress_json->>'allow_explicit_zero_no_bank_scopes',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_operation_scoped_no_transfer_marker := LOWER(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'scoped_no_transfer_execution',
      v_operation_row.progress_json->>'scoped_no_transfer_execution',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');

    v_operation_projection_scope := UPPER(NULLIF(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'pay_channel_scope',
      v_operation_row.progress_json->>'pay_channel_scope',
      v_operation_row.progress_json->>'scope',
      ''
    )), ''));
    v_operation_expected_global_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'global_paye_net_state_hash',
      v_operation_no_bank_proof_json->>'paye_net_state_hash',
      ''
    )), '');
    v_operation_expected_global_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'global_bank_payment_projection_hash',
      ''
    )), '');
    v_operation_expected_scoped_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'scoped_paye_net_state_hash',
      ''
    )), '');
    v_operation_expected_scoped_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_operation_no_bank_proof_json->>'scoped_bank_payment_projection_hash',
      v_operation_no_bank_proof_json->>'bank_payment_projection_hash',
      ''
    )), '');

    IF COALESCE(v_operation_no_bank_proof_json->>'global_missing_explicit_paye_input_count', v_operation_no_bank_proof_json->>'missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_global_missing_count := COALESCE(v_operation_no_bank_proof_json->>'global_missing_explicit_paye_input_count', v_operation_no_bank_proof_json->>'missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_operation_no_bank_proof_json->>'global_explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_global_zero_count := (v_operation_no_bank_proof_json->>'global_explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_operation_no_bank_proof_json->>'global_positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_global_positive_count := (v_operation_no_bank_proof_json->>'global_positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_operation_no_bank_proof_json->>'global_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_global_invalid_count := (v_operation_no_bank_proof_json->>'global_invalid_payment_row_count')::integer;
    END IF;
    IF COALESCE(v_operation_no_bank_proof_json->>'scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_scoped_missing_count := (v_operation_no_bank_proof_json->>'scoped_missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_operation_no_bank_proof_json->>'scoped_explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_scoped_zero_count := (v_operation_no_bank_proof_json->>'scoped_explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_operation_no_bank_proof_json->>'scoped_positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_scoped_positive_count := (v_operation_no_bank_proof_json->>'scoped_positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_operation_no_bank_proof_json->>'scoped_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_operation_expected_scoped_invalid_count := (v_operation_no_bank_proof_json->>'scoped_invalid_payment_row_count')::integer;
    END IF;
  END IF;

  IF v_scoped_explicit_zero_count > 0 THEN
    v_seed_proof_validated := (
      p_operation_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(v_operation_row.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
      AND v_operation_proof_source = 'PAY_EXECUTE_BANK_TRANSFER_SCOPE_SEED'
      AND v_operation_proof_operation_id = p_operation_id
      AND v_operation_proof_pay_batch_id = p_pay_batch_id
      AND v_operation_projection_scope IS NOT DISTINCT FROM v_pay_channel_scope
      AND v_operation_allow_explicit_zero_marker IS TRUE
      AND v_operation_scoped_no_transfer_marker IS NOT DISTINCT FROM v_server_scoped_no_transfer_eligible
      AND v_operation_no_bank_payment_marker IS NOT DISTINCT FROM v_server_no_bank_payment_eligible
      AND v_operation_expected_global_paye_net_state_hash IS NOT DISTINCT FROM v_current_paye_net_state_hash
      AND v_operation_expected_global_bank_payment_projection_hash IS NOT DISTINCT FROM v_all_bank_payment_projection_hash
      AND v_operation_expected_scoped_paye_net_state_hash IS NOT DISTINCT FROM v_scoped_paye_net_state_hash
      AND v_operation_expected_scoped_bank_payment_projection_hash IS NOT DISTINCT FROM v_current_bank_payment_projection_hash
      AND v_operation_expected_global_missing_count IS NOT DISTINCT FROM v_global_paye_missing_count
      AND v_operation_expected_global_zero_count IS NOT DISTINCT FROM v_global_paye_zero_count
      AND v_operation_expected_global_positive_count IS NOT DISTINCT FROM v_global_positive_bank_payment_count
      AND v_operation_expected_global_invalid_count IS NOT DISTINCT FROM v_global_invalid_payment_row_count
      AND v_operation_expected_scoped_missing_count IS NOT DISTINCT FROM v_scoped_paye_missing_count
      AND v_operation_expected_scoped_zero_count IS NOT DISTINCT FROM v_scoped_explicit_zero_count
      AND v_operation_expected_scoped_positive_count IS NOT DISTINCT FROM v_scoped_positive_bank_payment_count
      AND v_operation_expected_scoped_invalid_count IS NOT DISTINCT FROM v_scoped_invalid_payment_row_count
    );

    IF v_seed_proof_validated IS NOT TRUE THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'EXPLICIT_ZERO_SEED_PROOF_REQUIRED',
        'message', 'Explicit-zero PAYE authorisation requires a matching server-owned transfer-scope seed proof.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
        'pay_channel_scope', v_pay_channel_scope,
        'operation_projection_scope', v_operation_projection_scope,
        'seed_proof_source', v_operation_proof_source,
        'scoped_no_transfer_execution', v_server_scoped_no_transfer_eligible,
        'no_bank_payment_execution', v_server_no_bank_payment_eligible,
        'global_invalid_payment_row_count', v_global_invalid_payment_row_count,
        'scoped_invalid_payment_row_count', v_scoped_invalid_payment_row_count
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_allow_explicit_zero_no_bank_scopes_validated := true;
    v_scoped_no_transfer_execution_validated := v_server_scoped_no_transfer_eligible;
    v_no_bank_payment_validated := v_server_no_bank_payment_eligible;
  ELSE
    IF v_operation_allow_explicit_zero_marker
       OR v_operation_scoped_no_transfer_marker
       OR v_operation_no_bank_payment_marker THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'ZERO_SCOPE_SEED_PROOF_INVALID',
        'message', 'The stored explicit-zero/no-bank seed markers do not match the current frozen payment projection.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
        'pay_channel_scope', v_pay_channel_scope
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF v_scoped_no_transfer_execution_validated THEN
    SELECT COUNT(*)::integer
    INTO v_no_bank_scope_artifact_count
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope_row
    WHERE transfer_scope_row.operation_id = p_operation_id
      AND transfer_scope_row.pay_batch_id = p_pay_batch_id
      AND (
        v_pay_channel_scope = 'ALL'
        OR UPPER(BTRIM(COALESCE(transfer_scope_row.pay_channel, ''))) = v_pay_channel_scope
        OR (
          v_pay_channel_scope = 'LOANS'
          AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) = 'LOANS'
          AND UPPER(BTRIM(COALESCE(transfer_scope_row.pay_channel, ''))) = 'PAYE'
        )
      );

    SELECT COUNT(*)::integer
    INTO v_no_bank_batch_transfer_count
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
      AND (
        v_pay_channel_scope = 'ALL'
        OR UPPER(BTRIM(COALESCE(transfer_row.pay_channel, ''))) = v_pay_channel_scope
        OR (
          v_pay_channel_scope = 'LOANS'
          AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) = 'LOANS'
          AND UPPER(BTRIM(COALESCE(transfer_row.pay_channel, ''))) = 'PAYE'
        )
      );

    SELECT COUNT(*)::integer
    INTO v_no_bank_transfer_event_count
    FROM public.pay_bank_transfer_events AS transfer_event_row
    LEFT JOIN public.pay_bank_transfers AS event_transfer_row
      ON event_transfer_row.id = transfer_event_row.pay_bank_transfer_id
    WHERE transfer_event_row.pay_batch_id = p_pay_batch_id
      AND (
        v_pay_channel_scope = 'ALL'
        OR UPPER(BTRIM(COALESCE(event_transfer_row.pay_channel, ''))) = v_pay_channel_scope
        OR (
          v_pay_channel_scope = 'LOANS'
          AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) = 'LOANS'
          AND UPPER(BTRIM(COALESCE(event_transfer_row.pay_channel, ''))) = 'PAYE'
        )
        OR transfer_event_row.pay_bank_transfer_id IS NULL
      );

    SELECT COUNT(*)::integer
    INTO v_no_bank_provider_attempt_count
    FROM public.banking_pay_operation_provider_attempts AS provider_attempt_row
    WHERE provider_attempt_row.operation_id = p_operation_id;

    IF COALESCE(v_no_bank_scope_artifact_count, 0) <> 0
       OR COALESCE(v_no_bank_batch_transfer_count, 0) <> 0
       OR COALESCE(v_no_bank_transfer_event_count, 0) <> 0
       OR COALESCE(v_no_bank_provider_attempt_count, 0) <> 0 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'NO_TRANSFER_EXECUTION_EVIDENCE_CONFLICT',
        'message', 'No-transfer authorisation cannot continue because transfer or provider-submission evidence already exists in the requested execution scope.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'pay_channel_scope', v_pay_channel_scope,
        'transfer_scope_count', COALESCE(v_no_bank_scope_artifact_count, 0),
        'bank_transfer_count', COALESCE(v_no_bank_batch_transfer_count, 0),
        'bank_transfer_event_count', COALESCE(v_no_bank_transfer_event_count, 0),
        'provider_attempt_count', COALESCE(v_no_bank_provider_attempt_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  v_bank_csv_export_json := CASE
    WHEN JSONB_TYPEOF(v_batch_row.bank_csv_export_json) = 'object'
      THEN COALESCE(v_batch_row.bank_csv_export_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_stored_csv_scope := UPPER(NULLIF(BTRIM(COALESCE(
    v_bank_csv_export_json->>'scope',
    v_bank_csv_export_json->>'pay_channel_scope',
    v_bank_csv_export_json->>'payChannelScope',
    ''
  )), ''));
  v_stored_csv_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
    v_bank_csv_export_json->>'paye_net_state_hash',
    v_bank_csv_export_json->>'current_paye_net_state_hash',
    v_bank_csv_export_json->>'payeNetStateHash',
    ''
  )), '');
  v_stored_csv_bank_projection_hash := NULLIF(BTRIM(COALESCE(
    v_bank_csv_export_json->>'bank_payment_projection_hash',
    v_bank_csv_export_json->>'current_bank_payment_projection_hash',
    v_bank_csv_export_json->>'bankPaymentProjectionHash',
    ''
  )), '');
  IF COALESCE(v_bank_csv_export_json->>'row_count', v_bank_csv_export_json->>'rowCount', '') ~ '^[0-9]+$' THEN
    v_stored_csv_row_count := COALESCE(v_bank_csv_export_json->>'row_count', v_bank_csv_export_json->>'rowCount')::integer;
  END IF;
  IF COALESCE(v_bank_csv_export_json->>'total_amount', v_bank_csv_export_json->>'totalAmount', v_bank_csv_export_json->>'total', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
    v_stored_csv_total := ROUND(COALESCE(v_bank_csv_export_json->>'total_amount', v_bank_csv_export_json->>'totalAmount', v_bank_csv_export_json->>'total')::numeric, 2)::numeric(14,2);
  END IF;
  v_stored_csv_generated_at_utc := NULLIF(BTRIM(COALESCE(v_bank_csv_export_json->>'generated_at_utc', v_bank_csv_export_json->>'generatedAtUtc', '')), '');
  v_stored_csv_generated_by_user_id := NULLIF(BTRIM(COALESCE(v_bank_csv_export_json->>'generated_by_user_id', v_bank_csv_export_json->>'generatedByUserId', '')), '');
  v_stored_csv_filename := NULLIF(BTRIM(COALESCE(v_bank_csv_export_json->>'filename', '')), '');
  v_stored_csv_zero_row := COALESCE(v_stored_csv_row_count, -1) = 0;

  v_server_projection_proof_json := JSONB_BUILD_OBJECT(
    'server_owned_payment_projection_proof', true,
    'payment_projection_proof_source', 'PAY_BATCH_AUTH_START',
    'payment_projection_proof_generated_at_utc', v_now::text,
    'pay_batch_id', p_pay_batch_id::text,
    'pay_channel_scope', v_pay_channel_scope,
    'scoped_no_transfer_execution', v_scoped_no_transfer_execution_validated,
    'no_bank_payment_execution', v_no_bank_payment_validated,
    'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_validated,
    'paye_net_state_hash', v_current_paye_net_state_hash,
    'global_paye_net_state_hash', v_current_paye_net_state_hash,
    'global_bank_payment_projection_hash', v_all_bank_payment_projection_hash,
    'scoped_paye_net_state_hash', v_scoped_paye_net_state_hash,
    'bank_payment_projection_hash', v_current_bank_payment_projection_hash,
    'scoped_bank_payment_projection_hash', v_current_bank_payment_projection_hash,
    'missing_explicit_paye_input_count', v_global_paye_missing_count,
    'global_missing_explicit_paye_input_count', v_global_paye_missing_count,
    'explicit_zero_count', v_global_paye_zero_count,
    'global_explicit_zero_count', v_global_paye_zero_count,
    'global_paye_positive_count', v_global_paye_positive_count,
    'global_positive_bank_payment_count', v_global_positive_bank_payment_count,
    'global_positive_bank_payment_total', v_global_positive_bank_payment_total,
    'global_invalid_payment_row_count', v_global_invalid_payment_row_count,
    'current_global_paye_net_state_hash', v_current_paye_net_state_hash,
    'current_global_bank_payment_projection_hash', v_all_bank_payment_projection_hash,
    'scoped_missing_explicit_paye_input_count', v_scoped_paye_missing_count,
    'scoped_explicit_zero_count', v_scoped_explicit_zero_count,
    'scoped_paye_positive_count', v_scoped_paye_positive_count,
    'positive_bank_payment_count', v_scoped_positive_bank_payment_count,
    'scoped_positive_bank_payment_count', v_scoped_positive_bank_payment_count,
    'positive_bank_payment_total', v_scoped_positive_bank_payment_total,
    'scoped_positive_bank_payment_total', v_scoped_positive_bank_payment_total,
    'scoped_invalid_payment_row_count', v_scoped_invalid_payment_row_count,
    'current_scoped_paye_net_state_hash', v_scoped_paye_net_state_hash,
    'current_scoped_bank_payment_projection_hash', v_current_bank_payment_projection_hash
  );

  v_execution_commit_state := upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED')));
  IF v_execution_commit_state NOT IN ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED') THEN
    v_execution_commit_state := 'NOT_SUBMITTED';
  END IF;

  IF v_execution_commit_state <> 'NOT_SUBMITTED' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_AUTH_START',
      'code', 'EXECUTION_STATE_CONFLICT',
      'message', 'This payment batch has already crossed the execution submission boundary and cannot be authorised again.',
      'pay_batch_id', p_pay_batch_id::text,
      'execution_commit_state', v_execution_commit_state
    )::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
     OR v_batch_row.execution_committed_at_utc IS NOT NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_AUTH_START',
      'code', 'EXECUTION_STATE_CONFLICT',
      'message', 'This payment batch has provider submission or execution commit evidence and cannot be authorised again.',
      'pay_batch_id', p_pay_batch_id::text,
      'execution_commit_state', v_execution_commit_state,
      'execution_commit_ref_present', NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL,
      'execution_committed_at_utc_present', v_batch_row.execution_committed_at_utc IS NOT NULL
    )::text USING ERRCODE = 'P0001';
  END IF;

  SELECT settings_default.*
  INTO v_cfg_row
  FROM public.settings_defaults AS settings_default
  WHERE settings_default.id = 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batch_auth_start: settings_defaults missing (id=1)';
  END IF;

  v_required_quantity := GREATEST(1, coalesce(v_cfg_row.payment_authoriser_quantity, 1));
  v_warning_hours_json := CASE WHEN p_warning_hours_json IS NOT NULL THEN p_warning_hours_json ELSE coalesce(v_cfg_row.funds_warning_hours_json, '[]'::jsonb) END;

  IF v_warning_hours_json IS NOT NULL AND jsonb_typeof(v_warning_hours_json) <> 'array' THEN
    RAISE EXCEPTION 'pay_batch_auth_start: warning_hours_json must be a JSON array';
  END IF;

  v_payment_date := coalesce(v_payment_date, v_batch_row.authoritative_payment_date, v_batch_row.pay_date);
  IF v_payment_date IS NULL THEN
    RAISE EXCEPTION 'pay_batch_auth_start: payment_date is required';
  END IF;

  IF v_kind = 'SCHEDULED' THEN
    IF p_scheduled_at_utc IS NULL THEN
      RAISE EXCEPTION 'pay_batch_auth_start: scheduled_at_utc is required when schedule_kind=SCHEDULED';
    END IF;
    v_scheduled_at_utc := p_scheduled_at_utc;
  ELSE
    v_scheduled_at_utc := v_now;
  END IF;

  v_funding_account_ref := nullif(btrim(coalesce(p_funding_account_ref, '')), '');
  v_funding_account_ref := coalesce(v_funding_account_ref, nullif(btrim(coalesce(v_batch_row.funding_account_ref, '')), ''), nullif(btrim(coalesce(v_cfg_row.rail_default_funding_account_ref, '')), ''));

  IF v_funding_account_ref IS NULL THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_BATCH_AUTH_START',
      'code', 'FUNDING_ACCOUNT_REF_REQUIRED',
      'message', 'A funding account reference is required by the installed authorisation request contract.',
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
      'execution_mode', v_execution_mode
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF coalesce(p_suppress_remittances, false) AND coalesce(p_suppress_remittances_confirmed, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'pay_batch_auth_start: suppress_remittances requires explicit user confirmation';
  END IF;

  v_effective_csv_uploaded_confirmed := COALESCE(p_csv_uploaded_confirmed, false);
  v_effective_csv_bank_confirm_ref := NULLIF(BTRIM(COALESCE(p_csv_bank_confirm_ref, '')), '');
  v_effective_external_settlement_comment := NULLIF(BTRIM(COALESCE(p_external_settlement_comment, '')), '');
  v_expected_manual_confirmation_mode := CASE
    WHEN v_execution_mode = 'CSV_SETTLEMENT' AND v_scoped_positive_bank_payment_count > 0 THEN 'BANK_UPLOAD_CONFIRMED'
    WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN 'ZERO_ROW_REVIEW_NO_BANK_PAYMENT'
    WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' AND v_scoped_positive_bank_payment_count > 0 THEN 'EXTERNAL_SETTLEMENT_CONFIRMED'
    WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN 'NO_BANK_PAYMENT_CONFIRMED'
    ELSE NULL::text
  END;
  v_manual_confirmation_mode := CASE
    WHEN p_operation_id IS NOT NULL THEN
      UPPER(NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'manual_confirmation_mode', '')), ''))
    ELSE
      UPPER(NULLIF(BTRIM(COALESCE(
        v_batch_row.execution_intent_json->>'manual_confirmation_mode',
        v_expected_manual_confirmation_mode,
        ''
      )), ''))
  END;

  IF v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
     AND v_manual_confirmation_mode IS DISTINCT FROM v_expected_manual_confirmation_mode THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_BATCH_AUTH_START',
      'code', 'MANUAL_CONFIRMATION_MODE_MISMATCH',
      'message', 'The local/manual settlement confirmation mode does not match the durable execution route and frozen projection.',
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
      'execution_mode', v_execution_mode,
      'expected_manual_confirmation_mode', v_expected_manual_confirmation_mode,
      'actual_manual_confirmation_mode', v_manual_confirmation_mode,
      'scoped_positive_bank_payment_count', v_scoped_positive_bank_payment_count,
      'scoped_explicit_zero_count', v_scoped_explicit_zero_count
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_operation_id IS NOT NULL THEN
    IF UPPER(BTRIM(COALESCE(
         NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'pay_channel_scope', '')), ''),
         NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'payChannelScope', '')), ''),
         'ALL'
       ))) IS DISTINCT FROM v_pay_channel_scope THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_SCOPE_MISMATCH',
        'message', 'The requested authorisation scope does not match the durable payment execution scope.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'requested_scope', v_pay_channel_scope,
        'durable_scope', UPPER(BTRIM(COALESCE(v_operation_row.input_json->>'pay_channel_scope', v_operation_row.input_json->>'payChannelScope', 'ALL')))
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'payment_date', '')), '') IS NOT NULL
       AND NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'payment_date', '')), '') IS DISTINCT FROM v_payment_date::text THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_PAYMENT_DATE_MISMATCH',
        'message', 'The requested accounting payment date does not match the durable payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'requested_payment_date', v_payment_date::text,
        'durable_payment_date', v_operation_row.input_json->>'payment_date'
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'schedule_kind', '')), '') IS NOT NULL
       AND UPPER(BTRIM(COALESCE(v_operation_row.input_json->>'schedule_kind', ''))) IS DISTINCT FROM v_kind THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_SCHEDULE_KIND_MISMATCH',
        'message', 'The requested schedule kind does not match the durable payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'requested_schedule_kind', v_kind,
        'durable_schedule_kind', v_operation_row.input_json->>'schedule_kind'
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'funding_account_ref', '')), '') IS NOT NULL
       AND NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'funding_account_ref', '')), '') IS DISTINCT FROM v_funding_account_ref THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_FUNDING_ACCOUNT_MISMATCH',
        'message', 'The requested funding account does not match the durable payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'actor_intent', '')), '') IS NOT NULL
       AND UPPER(BTRIM(COALESCE(v_operation_row.input_json->>'actor_intent', ''))) IS DISTINCT FROM NULLIF(v_intent, '') THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_ACTOR_INTENT_MISMATCH',
        'message', 'The requested authorisation intent does not match the durable payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_execution_mode = 'CSV_SETTLEMENT'
       AND (
         LOWER(BTRIM(COALESCE(v_operation_row.input_json->>'csv_uploaded_confirmed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
         OR v_effective_csv_uploaded_confirmed IS NOT TRUE
         OR NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'csv_bank_confirm_ref', '')), '') IS DISTINCT FROM v_effective_csv_bank_confirm_ref
       ) THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_CSV_CONFIRMATION_MISMATCH',
        'message', 'The supplied CSV confirmation evidence does not match the durable payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'durable_csv_uploaded_confirmed', v_operation_row.input_json->'csv_uploaded_confirmed',
        'supplied_csv_uploaded_confirmed', v_effective_csv_uploaded_confirmed,
        'durable_csv_bank_confirm_ref', v_operation_row.input_json->>'csv_bank_confirm_ref',
        'supplied_csv_bank_confirm_ref', v_effective_csv_bank_confirm_ref
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_execution_mode = 'EXTERNAL_SETTLEMENT'
       AND NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'external_settlement_comment', '')), '') IS DISTINCT FROM v_effective_external_settlement_comment THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_EXTERNAL_CONFIRMATION_MISMATCH',
        'message', 'The supplied external settlement evidence does not match the durable payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'durable_external_comment_present', NULLIF(BTRIM(COALESCE(v_operation_row.input_json->>'external_settlement_comment', '')), '') IS NOT NULL,
        'supplied_external_comment_present', v_effective_external_settlement_comment IS NOT NULL
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF v_execution_mode = 'CSV_SETTLEMENT' THEN
    IF v_stored_csv_scope IS DISTINCT FROM v_pay_channel_scope
       OR v_stored_csv_paye_net_state_hash IS DISTINCT FROM v_scoped_paye_net_state_hash
       OR v_stored_csv_bank_projection_hash IS DISTINCT FROM v_current_bank_payment_projection_hash
       OR v_stored_csv_row_count IS NULL
       OR v_stored_csv_row_count <> v_scoped_positive_bank_payment_count
       OR v_stored_csv_total IS NULL
       OR ROUND(v_stored_csv_total, 2) <> ROUND(v_scoped_positive_bank_payment_total, 2)
       OR v_scoped_invalid_payment_row_count <> 0 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'CSV_REGENERATION_REQUIRED',
        'message', 'The generated Bank CSV is missing, stale, or does not match the exact authorised payment scope.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
        'requested_scope', v_pay_channel_scope,
        'stored_scope', v_stored_csv_scope,
        'stored_paye_net_state_hash', v_stored_csv_paye_net_state_hash,
        'current_paye_net_state_hash', v_scoped_paye_net_state_hash,
        'stored_bank_payment_projection_hash', v_stored_csv_bank_projection_hash,
        'current_bank_payment_projection_hash', v_current_bank_payment_projection_hash,
        'stored_row_count', v_stored_csv_row_count,
        'current_row_count', v_scoped_positive_bank_payment_count,
        'stored_total_amount', v_stored_csv_total,
        'current_total_amount', v_scoped_positive_bank_payment_total,
        'invalid_payment_row_count', v_scoped_invalid_payment_row_count
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_effective_csv_uploaded_confirmed IS NOT TRUE THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'CSV_UPLOAD_CONFIRMATION_REQUIRED',
        'message', CASE
          WHEN v_scoped_no_transfer_execution_validated
            THEN 'Confirm that the current zero-row Bank CSV was reviewed and that no bank upload or payment is required.'
          ELSE 'Confirm that the current CloudTMS Bank CSV was uploaded or processed with the bank.'
        END,
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
        'pay_channel_scope', v_pay_channel_scope
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_scoped_positive_bank_payment_count > 0
       AND v_effective_csv_bank_confirm_ref IS NULL THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'CSV_BANK_CONFIRM_REF_REQUIRED',
        'message', 'Positive CSV settlement requires a bank confirmation reference before authorisation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
        'pay_channel_scope', v_pay_channel_scope
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_scoped_no_transfer_execution_validated THEN
      v_effective_csv_bank_confirm_ref := NULL::text;
      v_payment_date := COALESCE(v_batch_row.authoritative_payment_date, v_batch_row.pay_date);
      IF v_payment_date IS NULL THEN
        RAISE EXCEPTION 'pay_batch_auth_start: batch payment date is required for zero-row CSV settlement';
      END IF;
    END IF;

    v_stored_csv_zero_row := v_stored_csv_row_count = 0;
    v_csv_evidence_validated := true;
    v_csv_evidence_json := JSONB_STRIP_NULLS(
      JSONB_BUILD_OBJECT(
        'validated', true,
        'validated_at_utc', v_now::text,
        'scope', v_stored_csv_scope,
        'paye_net_state_hash', v_stored_csv_paye_net_state_hash,
        'bank_payment_projection_hash', v_stored_csv_bank_projection_hash,
        'row_count', v_stored_csv_row_count,
        'total_amount', v_stored_csv_total,
        'zero_row', v_stored_csv_zero_row,
        'no_bank_payment_required_for_scope', v_scoped_no_transfer_execution_validated,
        'confirmation_kind', CASE
          WHEN v_scoped_no_transfer_execution_validated THEN 'ZERO_ROW_REVIEW'
          ELSE 'BANK_UPLOAD_CONFIRMATION'
        END,
        'generated_at_utc', v_stored_csv_generated_at_utc,
        'generated_by_user_id', v_stored_csv_generated_by_user_id,
        'filename', v_stored_csv_filename
      )
      || JSONB_BUILD_OBJECT(
        'csv_uploaded_confirmed', v_effective_csv_uploaded_confirmed,
        'csv_bank_confirm_ref', v_effective_csv_bank_confirm_ref,
        'authoritative_accounting_payment_date', v_payment_date::text,
        'source', 'PAY_BATCH_AUTH_START'
      )
    );
  ELSIF v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN
    IF v_scoped_positive_bank_payment_count > 0
       AND v_effective_external_settlement_comment IS NULL THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'EXTERNAL_SETTLEMENT_COMMENT_REQUIRED',
        'message', 'Positive external settlement requires a mandatory settlement comment before authorisation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
        'pay_channel_scope', v_pay_channel_scope
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF p_operation_id IS NOT NULL THEN
    v_effective_freshness_result_hash := nullif(btrim(coalesce(p_freshness_result_hash, v_batch_row.freshness_result_hash, v_operation_row.progress_json->>'freshness_result_hash', '')), '');
    v_effective_freshness_scope_hash := nullif(btrim(coalesce(v_batch_row.freshness_scope_hash, v_operation_row.progress_json->>'freshness_scope_hash', '')), '');

    IF nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_NOT_READY',
        'message', 'Payment batch freshness result hash is required before operation-scoped bank authorisation can start.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'batch_freshness_result_hash_present', false
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF upper(btrim(coalesce(v_operation_row.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_TYPE_NOT_SUPPORTED_FOR_AUTH_START_FAST_PATH',
        'message', 'Operation-scoped payment authorisation can only use a payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'operation_type', v_operation_row.operation_type
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF upper(btrim(coalesce(v_operation_row.status, ''))) NOT IN ('QUEUED', 'RUNNING', 'WAITING_AUTHORISATION') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'OPERATION_NOT_ACTIVE_FOR_AUTH_START',
        'message', 'Operation-scoped payment authorisation requires an active payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'operation_status', v_operation_row.status
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF upper(btrim(coalesce(v_batch_row.status, ''))) <> 'DRAFT' THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_NOT_READY',
        'message', 'Only a draft payment batch can be authorised for execution.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'batch_status', v_batch_row.status
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF nullif(btrim(coalesce(p_freshness_result_hash, '')), '') IS NOT NULL
       AND nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') IS NOT NULL
       AND nullif(btrim(coalesce(p_freshness_result_hash, '')), '') <> nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'FRESHNESS_RESULT_HASH_MISMATCH',
        'message', 'Payment freshness proof does not match the batch freshness result.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'provided_freshness_result_hash', nullif(btrim(coalesce(p_freshness_result_hash, '')), ''),
        'batch_freshness_result_hash', nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '')
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF nullif(btrim(coalesce(v_operation_row.progress_json->>'freshness_result_hash', '')), '') IS NOT NULL
       AND nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') IS NOT NULL
       AND nullif(btrim(coalesce(v_operation_row.progress_json->>'freshness_result_hash', '')), '') <> nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'FRESHNESS_RESULT_HASH_MISMATCH',
        'message', 'Operation freshness proof does not match the batch freshness result.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'operation_freshness_result_hash', nullif(btrim(coalesce(v_operation_row.progress_json->>'freshness_result_hash', '')), ''),
        'batch_freshness_result_hash', nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '')
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF nullif(btrim(coalesce(v_effective_freshness_result_hash, '')), '') IS NULL
       OR upper(btrim(coalesce(v_batch_row.freshness_validation_status, ''))) <> 'PASSED'
       OR coalesce((v_batch_row.freshness_result_json->>'is_stale')::boolean, false) IS TRUE THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_NOT_READY',
        'message', 'Payment batch freshness must be passed and non-stale before authorisation can start.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'freshness_validation_status', v_batch_row.freshness_validation_status,
        'freshness_result_hash_present', nullif(btrim(coalesce(v_effective_freshness_result_hash, '')), '') IS NOT NULL,
        'freshness_is_stale', coalesce((v_batch_row.freshness_result_json->>'is_stale')::boolean, false)
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  ---------------------------------------------------------------------------
  -- Final pre-authorisation freshness gate.
  -- Operation-proof path must not run the legacy full-batch freshness scan.
  -- It trusts the already persisted central freshness proof on pay_batches,
  -- then performs only the explicit carry-forward freshness check, which is
  -- bounded to this batch/scope and does not remap batch economic keys.
  ---------------------------------------------------------------------------
  IF p_operation_id IS NOT NULL THEN
    v_auth_freshness_json := jsonb_build_object(
      'is_stale', false,
      'stale_reasons', '[]'::jsonb,
      'diff', '[]'::jsonb,
      'proof_mode', true,
      'freshness_result_hash', v_effective_freshness_result_hash,
      'freshness_scope_hash', v_effective_freshness_scope_hash
    );

    v_auth_carry_forward_freshness_json := public._pay_manual_adjustment_carry_forward_freshness_check(
      p_pay_batch_id,
      NULL::uuid[],
      NULL::uuid[],
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'surface', 'pay_batch_auth_start',
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
        'idempotency_key_present', v_requested_idempotency_key IS NOT NULL
      ),
      p_actor_user_id
    );

    v_auth_carry_forward_blocker_count := jsonb_array_length(
      CASE
        WHEN jsonb_typeof(v_auth_carry_forward_freshness_json->'blockers') = 'array'
          THEN COALESCE(v_auth_carry_forward_freshness_json->'blockers', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    );

    SELECT COALESCE(jsonb_agg(DISTINCT reason_rows.reason_text ORDER BY reason_rows.reason_text), '[]'::jsonb)
    INTO v_auth_freshness_blocker_reasons
    FROM (
      SELECT CASE
        WHEN COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
          THEN 'Manual adjustment carry-forward was consumed elsewhere'
        WHEN COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
          THEN 'Source payment scope changed'
        ELSE 'Manual adjustment carry-forward changed'
      END AS reason_text
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(v_auth_carry_forward_freshness_json->'blockers') = 'array'
            THEN COALESCE(v_auth_carry_forward_freshness_json->'blockers', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS carry_forward_blockers(blocker_value)
    ) AS reason_rows
    WHERE NULLIF(BTRIM(COALESCE(reason_rows.reason_text, '')), '') IS NOT NULL;

    IF COALESCE(v_auth_carry_forward_blocker_count, 0) > 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_STALE',
        'message', 'Payment batch freshness is stale and must be refreshed before authorisation can start.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
        'stale_reasons', COALESCE(v_auth_freshness_blocker_reasons, '[]'::jsonb),
        'freshness_result', COALESCE(v_auth_freshness_json, '{}'::jsonb),
        'carry_forward_freshness_result', COALESCE(v_auth_carry_forward_freshness_json, '{}'::jsonb)
      )::text USING ERRCODE = 'P0001';
    END IF;
  ELSE
    v_auth_freshness_json := public.pay_batch_validate_freshness(
      p_pay_batch_id,
      p_actor_user_id,
      false
    );

    v_auth_carry_forward_freshness_json := public._pay_manual_adjustment_carry_forward_freshness_check(
      p_pay_batch_id,
      NULL::uuid[],
      NULL::uuid[],
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'surface', 'pay_batch_auth_start',
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
        'idempotency_key_present', v_requested_idempotency_key IS NOT NULL
      ),
      p_actor_user_id
    );

    v_auth_carry_forward_blocker_count := jsonb_array_length(
      CASE
        WHEN jsonb_typeof(v_auth_carry_forward_freshness_json->'blockers') = 'array'
          THEN COALESCE(v_auth_carry_forward_freshness_json->'blockers', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    );

    SELECT COALESCE(jsonb_agg(DISTINCT reason_rows.reason_text ORDER BY reason_rows.reason_text), '[]'::jsonb)
    INTO v_auth_freshness_blocker_reasons
    FROM (
      SELECT stale_reason_values.reason_text
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_auth_freshness_json->'stale_reasons') = 'array'
            THEN COALESCE(v_auth_freshness_json->'stale_reasons', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS stale_reason_values(reason_text)

      UNION ALL

      SELECT CASE
        WHEN COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
          THEN 'Manual adjustment carry-forward was consumed elsewhere'
        WHEN COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
          THEN 'Source payment scope changed'
        ELSE 'Manual adjustment carry-forward changed'
      END AS reason_text
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(v_auth_carry_forward_freshness_json->'blockers') = 'array'
            THEN COALESCE(v_auth_carry_forward_freshness_json->'blockers', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS carry_forward_blockers(blocker_value)
    ) AS reason_rows
    WHERE NULLIF(BTRIM(COALESCE(reason_rows.reason_text, '')), '') IS NOT NULL;

    IF LOWER(COALESCE(v_auth_freshness_json->>'is_stale', 'false')) IN ('true', 't', 'yes', 'y', '1')
       OR COALESCE(v_auth_carry_forward_blocker_count, 0) > 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_STALE',
        'message', 'Payment batch freshness is stale and must be refreshed before authorisation can start.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
        'stale_reasons', COALESCE(v_auth_freshness_blocker_reasons, '[]'::jsonb),
        'freshness_result', COALESCE(v_auth_freshness_json, '{}'::jsonb),
        'carry_forward_freshness_result', COALESCE(v_auth_carry_forward_freshness_json, '{}'::jsonb)
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;


  PERFORM 1
  FROM public.pay_batch_auth_requests AS auth_request_lock
  WHERE auth_request_lock.pay_batch_id = p_pay_batch_id
    AND auth_request_lock.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
  FOR UPDATE OF auth_request_lock;

  IF p_operation_id IS NOT NULL THEN
    v_auth_start_path := CASE
      WHEN v_scoped_no_transfer_execution_validated THEN 'OPERATION_SCOPED_NO_TRANSFER_PATH'
      WHEN v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT') THEN 'OPERATION_SCOPED_LOCAL_MANUAL_PROOF_PATH'
      ELSE 'OPERATION_SCOPED_PROOF_PATH'
    END;
    v_used_operation_scope_proof := v_scoped_no_transfer_execution_validated IS NOT TRUE;

    v_prepare_json := public.pay_batch_prepare(
      p_pay_batch_id => p_pay_batch_id,
      p_actor_user_id => p_actor_user_id,
      p_operation_id => p_operation_id,
      p_freshness_result_hash => p_freshness_result_hash
    );

    v_prepare_execution_mode := CASE
      WHEN UPPER(BTRIM(COALESCE(v_prepare_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
      WHEN UPPER(BTRIM(COALESCE(v_prepare_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
      WHEN UPPER(BTRIM(COALESCE(v_prepare_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
      ELSE NULL::text
    END;
    v_prepare_next_required_phase := UPPER(BTRIM(COALESCE(v_prepare_json->>'next_required_phase', '')));

    IF v_prepare_execution_mode IS DISTINCT FROM v_execution_mode THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_PREPARE_EXECUTION_MODE_MISMATCH',
        'message', 'Payment batch prepare returned an execution mode that does not match the durable operation mode.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'durable_execution_mode', v_execution_mode,
        'prepare_execution_mode', v_prepare_execution_mode,
        'prepare_next_required_phase', NULLIF(v_prepare_next_required_phase, '')
      )::text USING ERRCODE = 'P0001';
    END IF;
    IF v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
       AND (
         v_prepare_next_required_phase IN (
           'PROVIDER_SUBMIT_CLAIM', 'CLAIM_PROVIDER_SUBMIT', 'SUBMIT_PROVIDER_TRANSFERS',
           'SEND_PROVIDER_CHUNK', 'REQUEST_PROVIDER_SEND', 'FINALISE_PROVIDER_CHUNK', 'APPLY_RAIL_UPDATES'
         )
         OR LOWER(BTRIM(COALESCE(v_prepare_json->>'provider_submission_required', ''))) <> 'false'
         OR LOWER(BTRIM(COALESCE(v_prepare_json->>'provider_submission_attempted', ''))) <> 'false'
         OR LOWER(BTRIM(COALESCE(v_prepare_json->>'submitted_to_bank', ''))) <> 'false'
       ) THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'LOCAL_MANUAL_PREPARE_PROVIDER_ROUTE_REJECTED',
        'message', 'Local/manual payment preparation returned provider-submission routing or evidence.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_mode', v_execution_mode,
        'prepare_next_required_phase', NULLIF(v_prepare_next_required_phase, ''),
        'provider_submission_required', v_prepare_json->'provider_submission_required',
        'provider_submission_attempted', v_prepare_json->'provider_submission_attempted',
        'submitted_to_bank', v_prepare_json->'submitted_to_bank'
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_ready := COALESCE((v_prepare_json->>'ready')::boolean, false);
    v_blocker_count := COALESCE(NULLIF(v_prepare_json->>'blocker_count', '')::integer, 0);
    v_prepare_blockers := CASE
      WHEN JSONB_TYPEOF(v_prepare_json->'blockers') = 'array'
        THEN COALESCE(v_prepare_json->'blockers', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;

    IF JSONB_ARRAY_LENGTH(v_prepare_blockers) = 0
       AND COALESCE(v_prepare_json->>'code', '') IN (
         'OPERATION_TRANSFER_SCOPE_PROOF_MISSING',
         'OPERATION_TRANSFER_SCOPE_NOT_PREPARED',
         'OPERATION_TRANSFER_SCOPE_NOT_FULLY_PREPARED',
         'BANK_TRANSFERS_NOT_PREPARED',
         'NO_AUTHORISATION_READY_TRANSFERS'
       ) THEN
      v_prepare_blockers := JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', v_prepare_json->>'code',
        'message', v_prepare_json->>'message'
      ));
    END IF;

    IF v_ready IS NOT TRUE AND v_scoped_no_transfer_execution_validated THEN
      SELECT
        COALESCE(JSONB_AGG(blocker_element.value ORDER BY blocker_element.ordinality) FILTER (
          WHERE COALESCE(blocker_element.value->>'code', '') NOT IN (
            'OPERATION_TRANSFER_SCOPE_PROOF_MISSING',
            'OPERATION_TRANSFER_SCOPE_NOT_PREPARED',
            'OPERATION_TRANSFER_SCOPE_NOT_FULLY_PREPARED',
            'BANK_TRANSFERS_NOT_PREPARED',
            'NO_AUTHORISATION_READY_TRANSFERS'
          )
        ), '[]'::jsonb),
        COALESCE(JSONB_AGG(blocker_element.value ORDER BY blocker_element.ordinality) FILTER (
          WHERE COALESCE(blocker_element.value->>'code', '') IN (
            'OPERATION_TRANSFER_SCOPE_PROOF_MISSING',
            'OPERATION_TRANSFER_SCOPE_NOT_PREPARED',
            'OPERATION_TRANSFER_SCOPE_NOT_FULLY_PREPARED',
            'BANK_TRANSFERS_NOT_PREPARED',
            'NO_AUTHORISATION_READY_TRANSFERS'
          )
        ), '[]'::jsonb)
      INTO
        v_prepare_unwaived_blockers,
        v_prepare_waived_blockers
      FROM JSONB_ARRAY_ELEMENTS(v_prepare_blockers) WITH ORDINALITY AS blocker_element(value, ordinality);

      IF JSONB_ARRAY_LENGTH(v_prepare_blockers) > 0
         AND JSONB_ARRAY_LENGTH(v_prepare_unwaived_blockers) = 0 THEN
        v_ready := true;
        v_blocker_count := 0;
        v_auth_start_path := 'OPERATION_SCOPED_NO_TRANSFER_PATH';
        v_used_operation_scope_proof := false;
        v_prepare_json := v_prepare_json
          || JSONB_BUILD_OBJECT(
            'ready', true,
            'blocker_count', 0,
            'blockers', '[]'::jsonb,
            'no_bank_payment_execution', true,
            'waived_no_transfer_blockers', v_prepare_waived_blockers
          );
      END IF;
    END IF;

    IF v_ready IS NOT TRUE THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_NOT_READY',
        'message', 'Payment batch is not ready for operation-scoped authorisation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'auth_start_path', v_auth_start_path,
        'blocker_count', v_blocker_count,
        'blockers', COALESCE(v_prepare_json->'blockers', '[]'::jsonb)
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
       AND v_prepare_next_required_phase NOT IN ('START_AUTHORISATION', 'START_AUTHORISATION_PROOF') THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'LOCAL_MANUAL_PREPARE_AUTHORISATION_ROUTE_REQUIRED',
        'message', 'A ready local/manual payment proof must route to authorisation, never provider submission or direct settlement.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_mode', v_execution_mode,
        'prepare_next_required_phase', NULLIF(v_prepare_next_required_phase, '')
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_summary_json := COALESCE(v_prepare_json->'execution_summary', '{}'::jsonb);
    v_effective_freshness_result_hash := NULLIF(BTRIM(COALESCE(p_freshness_result_hash, v_prepare_json #>> '{freshness,freshness_result_hash}', v_batch_row.freshness_result_hash, '')), '');
    v_effective_freshness_scope_hash := NULLIF(BTRIM(COALESCE(v_prepare_json #>> '{freshness,freshness_scope_hash}', v_batch_row.freshness_scope_hash, '')), '');
    v_authorisation_ready_transfer_count := COALESCE(NULLIF(v_prepare_json->>'authorisation_ready_transfer_count', '')::integer, NULLIF(v_summary_json->>'authorisation_ready_transfer_count', '')::integer, 0);
    v_scoped_operation_scope_count := COALESCE(NULLIF(v_prepare_json->>'scoped_operation_scope_count', '')::integer, NULLIF(v_summary_json->>'transfer_count', '')::integer, 0);
    v_scoped_scope_prepared_count := COALESCE(NULLIF(v_prepare_json->>'scoped_scope_prepared_count', '')::integer, v_scoped_operation_scope_count, 0);
    v_pending_transfer_count := COALESCE(v_authorisation_ready_transfer_count, 0);
    v_provider_attempt_or_evidence_transfer_count := COALESCE(NULLIF(v_prepare_json->>'provider_attempt_or_evidence_transfer_count', '')::integer, 0);
    v_provider_or_ambiguous_evidence_transfer_count := COALESCE(NULLIF(v_prepare_json->>'provider_or_ambiguous_evidence_transfer_count', '')::integer, 0);
    v_canonical_pending_status_transfer_count := COALESCE(NULLIF(v_prepare_json->>'canonical_pending_status_transfer_count', '')::integer, 0);
    v_unsafe_transfer_count := COALESCE(NULLIF(v_prepare_json->>'unsafe_transfer_count', '')::integer, 0);
    v_scope_amount_total := COALESCE(NULLIF(v_summary_json->>'scope_amount_total', '')::numeric, 0);
    v_transfer_amount_total := COALESCE(NULLIF(v_summary_json->>'transfer_amount_total', '')::numeric, v_scope_amount_total);
    v_scope_item_amount_total := COALESCE(NULLIF(v_summary_json->>'scope_item_amount_total', '')::numeric, v_scope_amount_total);
    v_batch_item_amount_total := COALESCE(NULLIF(v_summary_json->>'batch_item_amount_total', '')::numeric, v_scope_amount_total);

  ELSE
    v_prepare_json := public.pay_batch_prepare(
      p_pay_batch_id => p_pay_batch_id,
      p_actor_user_id => p_actor_user_id,
      p_operation_id => p_operation_id,
      p_freshness_result_hash => p_freshness_result_hash
    );
    v_ready := coalesce((v_prepare_json->>'ready')::boolean, false);
    v_blocker_count := coalesce(NULLIF(v_prepare_json->>'blocker_count', '')::integer, 0);
    v_effective_freshness_result_hash := nullif(btrim(coalesce(p_freshness_result_hash, v_prepare_json #>> '{freshness,freshness_result_hash}', '')), '');
    v_effective_freshness_scope_hash := nullif(btrim(coalesce(v_prepare_json #>> '{freshness,freshness_scope_hash}', '')), '');

    IF v_ready IS NOT TRUE THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'PAY_BATCH_NOT_READY',
        'message', 'Payment batch is not ready for authorisation.',
        'pay_batch_id', p_pay_batch_id::text,
        'blocker_count', v_blocker_count,
        'blockers', coalesce(v_prepare_json->'blockers', '[]'::jsonb)
      )::text;
    END IF;

    v_summary_json := coalesce(v_prepare_json->'execution_summary', public.pay_batch_execution_summary_get(p_pay_batch_id, p_actor_user_id));
    v_pending_transfer_count := coalesce(NULLIF(v_summary_json->>'pending_transfer_count', '')::integer, 0);

    IF v_execution_mode IN ('STANDARD_BANK', 'BANK') THEN
      SELECT
        count(*) FILTER (WHERE classified_transfer.is_authorisation_ready)::integer,
        count(*) FILTER (WHERE classified_transfer.evidence_classification = 'local_only_evidence')::integer,
        count(*) FILTER (WHERE classified_transfer.has_provider_submission_evidence OR classified_transfer.has_provider_event_evidence OR classified_transfer.has_provider_attempt_without_external_id OR classified_transfer.has_operation_submit_attempt)::integer,
        count(*) FILTER (WHERE classified_transfer.has_provider_submission_evidence OR classified_transfer.has_provider_event_evidence OR classified_transfer.has_provider_attempt_without_external_id OR classified_transfer.has_operation_submit_attempt OR classified_transfer.has_ambiguous_external_evidence)::integer,
        count(*) FILTER (WHERE classified_transfer.is_failed_or_blocked)::integer,
        count(*) FILTER (WHERE classified_transfer.status_upper = 'PENDING')::integer,
        count(DISTINCT classified_transfer.auth_request_id) FILTER (
          WHERE classified_transfer.auth_request_id IS NOT NULL
            AND classified_transfer.has_non_cancellable_auth_request IS TRUE
        )::integer,
        count(DISTINCT classified_transfer.auth_request_id) FILTER (
          WHERE classified_transfer.auth_request_id IS NOT NULL
            AND (
              classified_transfer.has_non_cancellable_auth_request IS TRUE
              OR classified_transfer.has_auth_request_provider_risk IS TRUE
              OR classified_transfer.has_other_operation_active_auth_request IS TRUE
            )
        )::integer
      INTO v_authorisation_ready_transfer_count,
           v_local_only_transfer_count,
           v_provider_attempt_or_evidence_transfer_count,
           v_provider_or_ambiguous_evidence_transfer_count,
           v_blocked_transfer_count,
           v_canonical_pending_status_transfer_count,
           v_non_cancellable_auth_request_count,
           v_auth_request_retry_blocker_count
      FROM public.pay_bank_transfer_execution_classify(
        p_pay_batch_id => p_pay_batch_id,
        p_pay_channel_scope => v_pay_channel_scope,
        p_operation_id => p_operation_id,
        p_include_unscoped_transfers => CASE WHEN p_operation_id IS NULL THEN true ELSE false END,
        p_action_context => 'AUTHORISATION_ACTION'
      ) AS classified_transfer;

      IF p_operation_id IS NOT NULL THEN
        SELECT count(*)::integer,
               count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'PREPARED')::integer,
               count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'FAILED')::integer,
               count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'SKIPPED')::integer,
               count(*) FILTER (WHERE operation_scope.pay_bank_transfer_id IS NULL)::integer
        INTO v_scoped_operation_scope_count,
             v_scoped_scope_prepared_count,
             v_scoped_scope_failed_count,
             v_scoped_scope_skipped_count,
             v_scoped_scope_without_transfer_count
        FROM public.banking_pay_operation_transfer_scope AS operation_scope
        WHERE operation_scope.pay_batch_id = p_pay_batch_id
          AND operation_scope.operation_id = p_operation_id
          AND (
            v_pay_channel_scope IN ('ALL', 'ANY', '*')
            OR upper(btrim(coalesce(operation_scope.pay_channel, ''))) = v_pay_channel_scope
          );
      END IF;

      IF COALESCE(v_non_cancellable_auth_request_count, 0) > 0 THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_BATCH_AUTH_START',
          'code', 'AUTH_REQUEST_HELD_BY_PREVIOUS_OPERATION',
          'message', 'An active authorisation request is not safe to reuse or cancel automatically for this payment execution attempt.',
          'pay_batch_id', p_pay_batch_id::text,
          'pay_channel_scope', v_pay_channel_scope,
          'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
          'non_cancellable_auth_request_count', COALESCE(v_non_cancellable_auth_request_count, 0),
          'auth_request_retry_blocker_count', COALESCE(v_auth_request_retry_blocker_count, 0)
        )::text USING ERRCODE = 'P0001';
      END IF;

      IF p_operation_id IS NOT NULL
         AND (
           COALESCE(v_scoped_operation_scope_count, 0) <= 0
           OR COALESCE(v_scoped_scope_prepared_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
           OR COALESCE(v_scoped_scope_failed_count, 0) <> 0
           OR COALESCE(v_scoped_scope_skipped_count, 0) <> 0
           OR COALESCE(v_scoped_scope_without_transfer_count, 0) <> 0
           OR COALESCE(v_authorisation_ready_transfer_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
         ) THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_BATCH_AUTH_START',
          'code', CASE WHEN COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0) > 0 THEN 'BANK_TRANSFER_PROVIDER_REVIEW_REQUIRED' ELSE 'NO_AUTHORISATION_READY_TRANSFERS' END,
          'message', CASE WHEN COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0) > 0 THEN 'One or more scoped transfers have provider submission evidence or ambiguous provider state and must be reviewed before authorisation can continue.' ELSE 'All scoped transfer groups must be prepared and authorisation-ready before authorisation can start.' END,
          'pay_batch_id', p_pay_batch_id::text,
          'pay_channel_scope', v_pay_channel_scope,
          'operation_id', p_operation_id::text,
          'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
          'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
          'scoped_scope_failed_count', COALESCE(v_scoped_scope_failed_count, 0),
          'scoped_scope_skipped_count', COALESCE(v_scoped_scope_skipped_count, 0),
          'scoped_scope_without_transfer_count', COALESCE(v_scoped_scope_without_transfer_count, 0),
          'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
          'canonical_pending_status_transfer_count', COALESCE(v_canonical_pending_status_transfer_count, 0),
          'local_only_transfer_count', COALESCE(v_local_only_transfer_count, 0),
          'provider_attempt_or_evidence_transfer_count', COALESCE(v_provider_attempt_or_evidence_transfer_count, 0),
          'provider_or_ambiguous_evidence_transfer_count', COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0),
          'blocked_transfer_count', COALESCE(v_blocked_transfer_count, 0)
        )::text USING ERRCODE = 'P0001';
      END IF;

      IF p_operation_id IS NULL AND COALESCE(v_authorisation_ready_transfer_count, 0) <= 0 THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_BATCH_AUTH_START',
          'code', CASE WHEN COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0) > 0 THEN 'BANK_TRANSFER_PROVIDER_REVIEW_REQUIRED' ELSE 'NO_AUTHORISATION_READY_TRANSFERS' END,
          'message', CASE WHEN COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0) > 0 THEN 'One or more transfers have provider submission evidence or ambiguous provider state and must be reviewed before authorisation can continue.' ELSE 'No safe locally prepared transfers are available for authorisation.' END,
          'pay_batch_id', p_pay_batch_id::text,
          'pay_channel_scope', v_pay_channel_scope,
          'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
          'canonical_pending_status_transfer_count', COALESCE(v_canonical_pending_status_transfer_count, 0),
          'local_only_transfer_count', COALESCE(v_local_only_transfer_count, 0),
          'provider_attempt_or_evidence_transfer_count', COALESCE(v_provider_attempt_or_evidence_transfer_count, 0),
          'provider_or_ambiguous_evidence_transfer_count', COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0),
          'blocked_transfer_count', COALESCE(v_blocked_transfer_count, 0)
        )::text USING ERRCODE = 'P0001';
      END IF;
    END IF;
  END IF;

  IF v_effective_freshness_result_hash IS NULL THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_BATCH_AUTH_START',
      'code', 'FRESHNESS_RESULT_HASH_REQUIRED',
      'message', 'A server-owned freshness result hash is required before an authorisation request can be created or reused.',
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
      'execution_mode', v_execution_mode
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_current_active_scope_hash := private.pay_payment_correction_sha256_v1(
    JSONB_BUILD_OBJECT(
      'version', 1,
      'pay_batch_id', p_pay_batch_id,
      'freshness_result_hash', v_effective_freshness_result_hash,
      'freshness_scope_hash', v_effective_freshness_scope_hash,
      'active_items', (
        SELECT COALESCE(
          JSONB_AGG(
            JSONB_BUILD_ARRAY(
              candidate_row.id,
              item_row.id,
              ROUND(COALESCE(item_row.amount_inc_vat, 0) * 100)::bigint,
              item_row.item_type,
              item_row.pay_channel,
              item_row.reservation_id,
              item_row.finance_component_id,
              item_row.pay_bank_transfer_id
            )
            ORDER BY candidate_row.id, item_row.id
          ),
          '[]'::jsonb
        )
        FROM public.pay_batch_candidates AS candidate_row
        JOIN public.pay_batch_items AS item_row
          ON item_row.pay_batch_candidate_id = candidate_row.id
        WHERE candidate_row.pay_batch_id = p_pay_batch_id
          AND COALESCE(item_row.is_voided, false) IS NOT TRUE
      )
    )
  );

  v_execution_intent_json := JSONB_STRIP_NULLS(
    JSONB_BUILD_OBJECT(
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
      'idempotency_key', v_requested_idempotency_key,
      'execution_mode', v_execution_mode,
      'pay_channel_scope', v_pay_channel_scope,
      'payment_date', v_payment_date::text,
      'freshness_result_hash', v_effective_freshness_result_hash,
      'freshness_scope_hash', v_effective_freshness_scope_hash,
      'active_scope_hash', v_current_active_scope_hash,
      'schedule_kind', v_kind,
      'scheduled_at_utc', CASE WHEN v_scheduled_at_utc IS NULL THEN NULL::text ELSE v_scheduled_at_utc::text END,
      'funding_account_ref', v_funding_account_ref,
      'funds_warning_hours_json', v_warning_hours_json,
      'actor_intent', NULLIF(v_intent, ''),
      'actor_user_id', p_actor_user_id::text
    )
    || JSONB_BUILD_OBJECT(
      'suppress_remittances', COALESCE(p_suppress_remittances, false),
      'suppress_remittances_confirmed', COALESCE(p_suppress_remittances_confirmed, false),
      'auth_start_path', v_auth_start_path,
      'pay_batch_prepare_skipped', v_used_operation_scope_proof,
      'used_operation_scope_proof', v_used_operation_scope_proof,
      'waived_no_transfer_blockers', CASE WHEN v_scoped_no_transfer_execution_validated THEN v_prepare_waived_blockers ELSE NULL::jsonb END,
      'provider_submission_required', v_execution_mode = 'STANDARD_BANK',
      'provider_submission_attempted', false,
      'submitted_to_bank', false
    )
    || v_server_projection_proof_json
    || CASE
         WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN
           JSONB_BUILD_OBJECT(
             'csv_uploaded_confirmed', v_effective_csv_uploaded_confirmed,
             'csv_bank_confirm_ref', v_effective_csv_bank_confirm_ref,
             'manual_confirmation_mode', v_manual_confirmation_mode,
             'bank_csv_evidence', v_csv_evidence_json,
             'csv_evidence_validated', v_csv_evidence_validated,
             'csv_zero_row', v_stored_csv_zero_row,
             'bank_csv_generated', true,
             'bank_csv_current', true,
             'bank_csv_scope', v_stored_csv_scope,
             'bank_csv_paye_net_state_hash', v_stored_csv_paye_net_state_hash,
             'bank_csv_bank_payment_projection_hash', v_stored_csv_bank_projection_hash,
             'bank_csv_row_count', v_stored_csv_row_count,
             'bank_csv_total_amount', v_stored_csv_total
           )
           || JSONB_BUILD_OBJECT(
             'csv_currentness_proof', JSONB_BUILD_OBJECT(
               'validated', true,
               'current', true,
               'validated_at_utc', v_now::text,
               'scope', v_stored_csv_scope,
               'paye_net_state_hash', v_stored_csv_paye_net_state_hash,
               'bank_payment_projection_hash', v_stored_csv_bank_projection_hash,
               'row_count', v_stored_csv_row_count,
               'total_amount', v_stored_csv_total,
               'source', 'PAY_BATCH_AUTH_START'
             )
           )
         WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN
           JSONB_BUILD_OBJECT(
             'external_settlement_comment', v_effective_external_settlement_comment,
             'manual_confirmation_mode', v_manual_confirmation_mode,
             'external_settlement_proof_source', 'EXTERNAL_MANUAL_CONFIRMATION',
             'external_settlement_confirmed', true
           )
         ELSE '{}'::jsonb
       END
  );


  <<auth_lookup_loop>>
  LOOP
    v_auth_lookup_pass := v_auth_lookup_pass + 1;
    IF v_auth_lookup_pass > 2 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'AUTH_REQUEST_RECONCILIATION_LOOP_GUARD',
        'message', 'Authorisation request reconciliation did not reach a stable state.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_existing_auth_id := NULL::uuid;
    v_existing_auth_state := NULL::text;
    v_existing_auth_intent_json := '{}'::jsonb;
    v_existing_auth_operation_id := NULL::text;
    v_existing_auth_idempotency_key := NULL::text;
    v_existing_auth_schedule_kind := NULL::text;
    v_existing_auth_scheduled_at_utc := NULL::timestamptz;
    v_existing_auth_funding_account_ref := NULL::text;
    v_existing_auth_warning_hours_json := NULL::jsonb;
    v_existing_auth_execution_mode := NULL::text;
    v_existing_auth_compatible := false;
    v_existing_auth_cross_operation := false;
    v_existing_auth_cross_operation_authorised := false;
    v_existing_auth_identity_matches := false;
    v_existing_auth_mode_specific_proof_matches := false;
    v_existing_auth_compatibility_json := '{}'::jsonb;
    v_conflicting_auth_id := NULL::uuid;
    v_conflicting_auth_state := NULL::text;
    v_conflicting_auth_operation_id := NULL::text;
    v_conflicting_auth_idempotency_key := NULL::text;
    v_conflicting_auth_intent_json := '{}'::jsonb;

    SELECT auth_request.id,
           auth_request.state,
           COALESCE(auth_request.execution_intent_json, '{}'::jsonb),
           NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'operation_id', '')), ''),
           NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'idempotency_key', '')), ''),
           UPPER(BTRIM(COALESCE(auth_request.schedule_kind, ''))),
           auth_request.scheduled_at_utc,
           NULLIF(BTRIM(COALESCE(auth_request.funding_account_ref, '')), ''),
           COALESCE(auth_request.funds_warning_hours_json, '[]'::jsonb)
    INTO v_existing_auth_id,
         v_existing_auth_state,
         v_existing_auth_intent_json,
         v_existing_auth_operation_id,
         v_existing_auth_idempotency_key,
         v_existing_auth_schedule_kind,
         v_existing_auth_scheduled_at_utc,
         v_existing_auth_funding_account_ref,
         v_existing_auth_warning_hours_json
    FROM public.pay_batch_auth_requests AS auth_request
    WHERE auth_request.pay_batch_id = p_pay_batch_id
      AND auth_request.state IN ('AWAITING', 'AUTHORISED')
      AND (
        (
          p_operation_id IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'operation_id', '')), '') = p_operation_id::text
        )
        OR (
          v_requested_idempotency_key IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'idempotency_key', '')), '') = v_requested_idempotency_key
        )
      )
    ORDER BY
      CASE
        WHEN p_operation_id IS NOT NULL
         AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'operation_id', '')), '') = p_operation_id::text THEN 0
        WHEN v_requested_idempotency_key IS NOT NULL
         AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'idempotency_key', '')), '') = v_requested_idempotency_key THEN 1
        ELSE 2
      END,
      auth_request.created_at_utc DESC NULLS LAST,
      auth_request.id DESC
    LIMIT 1;

    IF v_existing_auth_id IS NOT NULL THEN
      v_existing_auth_execution_mode := CASE
        WHEN UPPER(BTRIM(COALESCE(v_existing_auth_intent_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
        WHEN UPPER(BTRIM(COALESCE(v_existing_auth_intent_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
        WHEN UPPER(BTRIM(COALESCE(v_existing_auth_intent_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
        ELSE NULL::text
      END;
      v_existing_auth_identity_matches := (
        (p_operation_id IS NOT NULL AND v_existing_auth_operation_id = p_operation_id::text)
        OR (v_requested_idempotency_key IS NOT NULL AND v_existing_auth_idempotency_key = v_requested_idempotency_key)
      );
      v_existing_auth_cross_operation := p_operation_id IS NOT NULL
        AND v_existing_auth_operation_id IS NOT NULL
        AND v_existing_auth_operation_id <> p_operation_id::text;
      v_existing_auth_cross_operation_authorised := v_existing_auth_cross_operation
        AND UPPER(BTRIM(COALESCE(v_existing_auth_state, ''))) = 'AUTHORISED';

      v_existing_auth_mode_specific_proof_matches := CASE
        WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN
          LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'csv_uploaded_confirmed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'csv_bank_confirm_ref', '')), '') IS NOT DISTINCT FROM v_effective_csv_bank_confirm_ref
          AND UPPER(NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'manual_confirmation_mode', '')), '')) IS NOT DISTINCT FROM v_manual_confirmation_mode
          AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'csv_evidence_validated', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'bank_csv_generated', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'bank_csv_current', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND UPPER(NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'bank_csv_scope', '')), '')) IS NOT DISTINCT FROM v_stored_csv_scope
          AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'bank_csv_paye_net_state_hash', '')), '') IS NOT DISTINCT FROM v_stored_csv_paye_net_state_hash
          AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'bank_csv_bank_payment_projection_hash', '')), '') IS NOT DISTINCT FROM v_stored_csv_bank_projection_hash
          AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'bank_csv_row_count', '')), '') IS NOT DISTINCT FROM v_stored_csv_row_count::text
          AND CASE
                WHEN COALESCE(v_existing_auth_intent_json->>'bank_csv_total_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                  THEN ROUND((v_existing_auth_intent_json->>'bank_csv_total_amount')::numeric, 2) IS NOT DISTINCT FROM ROUND(v_stored_csv_total, 2)
                ELSE false
              END
          AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json #>> '{csv_currentness_proof,validated}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json #>> '{csv_currentness_proof,current}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN
          UPPER(NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'manual_confirmation_mode', '')), '')) IS NOT DISTINCT FROM v_manual_confirmation_mode
          AND UPPER(NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'external_settlement_proof_source', '')), '')) = 'EXTERNAL_MANUAL_CONFIRMATION'
          AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'external_settlement_confirmed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND (
            v_scoped_positive_bank_payment_count = 0
            OR NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'external_settlement_comment', '')), '') IS NOT DISTINCT FROM v_effective_external_settlement_comment
          )
        ELSE true
      END;

      v_existing_auth_compatible := (
        v_existing_auth_identity_matches
        AND v_existing_auth_cross_operation_authorised IS NOT TRUE
        AND (p_operation_id IS NULL OR v_existing_auth_operation_id = p_operation_id::text)
        AND UPPER(BTRIM(COALESCE(v_existing_auth_state, ''))) IN ('AWAITING', 'AUTHORISED')
        AND v_existing_auth_execution_mode IS NOT DISTINCT FROM v_execution_mode
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'pay_batch_id', '')), '') = p_pay_batch_id::text
        AND UPPER(NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'pay_channel_scope', '')), '')) IS NOT DISTINCT FROM v_pay_channel_scope
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'payment_date', '')), '') IS NOT DISTINCT FROM v_payment_date::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'freshness_result_hash', '')), '') IS NOT DISTINCT FROM v_effective_freshness_result_hash
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'freshness_scope_hash', '')), '') IS NOT DISTINCT FROM v_effective_freshness_scope_hash
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_paye_net_state_hash', '')), '') IS NOT DISTINCT FROM v_current_paye_net_state_hash
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_bank_payment_projection_hash', '')), '') IS NOT DISTINCT FROM v_all_bank_payment_projection_hash
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_paye_net_state_hash', '')), '') IS NOT DISTINCT FROM v_scoped_paye_net_state_hash
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_bank_payment_projection_hash', '')), '') IS NOT DISTINCT FROM v_current_bank_payment_projection_hash
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_missing_explicit_paye_input_count', '')), '') IS NOT DISTINCT FROM v_global_paye_missing_count::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_explicit_zero_count', '')), '') IS NOT DISTINCT FROM v_global_paye_zero_count::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_positive_bank_payment_count', '')), '') IS NOT DISTINCT FROM v_global_positive_bank_payment_count::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_invalid_payment_row_count', '')), '') IS NOT DISTINCT FROM v_global_invalid_payment_row_count::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_missing_explicit_paye_input_count', '')), '') IS NOT DISTINCT FROM v_scoped_paye_missing_count::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_explicit_zero_count', '')), '') IS NOT DISTINCT FROM v_scoped_explicit_zero_count::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_positive_bank_payment_count', '')), '') IS NOT DISTINCT FROM v_scoped_positive_bank_payment_count::text
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_invalid_payment_row_count', '')), '') IS NOT DISTINCT FROM v_scoped_invalid_payment_row_count::text
        AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND UPPER(BTRIM(COALESCE(v_existing_auth_schedule_kind, ''))) IS NOT DISTINCT FROM v_kind
        AND (v_kind <> 'SCHEDULED' OR v_existing_auth_scheduled_at_utc IS NOT DISTINCT FROM v_scheduled_at_utc)
        AND v_existing_auth_funding_account_ref IS NOT DISTINCT FROM v_funding_account_ref
        AND COALESCE(v_existing_auth_warning_hours_json, '[]'::jsonb) IS NOT DISTINCT FROM COALESCE(v_warning_hours_json, '[]'::jsonb)
        AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'suppress_remittances', 'false'))) = CASE WHEN COALESCE(p_suppress_remittances, false) THEN 'true' ELSE 'false' END
        AND LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'suppress_remittances_confirmed', 'false'))) = CASE WHEN COALESCE(p_suppress_remittances_confirmed, false) THEN 'true' ELSE 'false' END
        AND UPPER(NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'actor_intent', '')), '')) IS NOT DISTINCT FROM NULLIF(v_intent, '')
        AND v_existing_auth_mode_specific_proof_matches
        AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'auth_request_id', '')), '') = v_existing_auth_id::text
      );

      v_existing_auth_compatibility_json := JSONB_BUILD_OBJECT(
        'identity_matches', v_existing_auth_identity_matches,
        'operation_binding_matches', p_operation_id IS NULL OR v_existing_auth_operation_id = p_operation_id::text,
        'cross_operation_authorised', v_existing_auth_cross_operation_authorised,
        'state_reusable', UPPER(BTRIM(COALESCE(v_existing_auth_state, ''))) IN ('AWAITING', 'AUTHORISED'),
        'execution_mode_matches', v_existing_auth_execution_mode IS NOT DISTINCT FROM v_execution_mode,
        'pay_batch_matches', NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'pay_batch_id', '')), '') = p_pay_batch_id::text,
        'scope_matches', UPPER(NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'pay_channel_scope', '')), '')) IS NOT DISTINCT FROM v_pay_channel_scope,
        'payment_date_matches', NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'payment_date', '')), '') IS NOT DISTINCT FROM v_payment_date::text,
        'freshness_result_hash_matches', NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'freshness_result_hash', '')), '') IS NOT DISTINCT FROM v_effective_freshness_result_hash,
        'freshness_scope_hash_matches', NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'freshness_scope_hash', '')), '') IS NOT DISTINCT FROM v_effective_freshness_scope_hash,
        'projection_hashes_match',
          NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_paye_net_state_hash', '')), '') IS NOT DISTINCT FROM v_current_paye_net_state_hash
          AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'global_bank_payment_projection_hash', '')), '') IS NOT DISTINCT FROM v_all_bank_payment_projection_hash
          AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_paye_net_state_hash', '')), '') IS NOT DISTINCT FROM v_scoped_paye_net_state_hash
          AND NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'scoped_bank_payment_projection_hash', '')), '') IS NOT DISTINCT FROM v_current_bank_payment_projection_hash,
        'mode_specific_proof_matches', v_existing_auth_mode_specific_proof_matches,
        'compatible', v_existing_auth_compatible
      ) || JSONB_BUILD_OBJECT(
        'schedule_matches', UPPER(BTRIM(COALESCE(v_existing_auth_schedule_kind, ''))) IS NOT DISTINCT FROM v_kind
          AND (v_kind <> 'SCHEDULED' OR v_existing_auth_scheduled_at_utc IS NOT DISTINCT FROM v_scheduled_at_utc),
        'funding_matches', v_existing_auth_funding_account_ref IS NOT DISTINCT FROM v_funding_account_ref,
        'warning_hours_match', COALESCE(v_existing_auth_warning_hours_json, '[]'::jsonb) IS NOT DISTINCT FROM COALESCE(v_warning_hours_json, '[]'::jsonb),
        'server_owned_projection_proof_present', LOWER(BTRIM(COALESCE(v_existing_auth_intent_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'auth_request_binding_matches', NULLIF(BTRIM(COALESCE(v_existing_auth_intent_json->>'auth_request_id', '')), '') = v_existing_auth_id::text
      );

      IF v_existing_auth_compatible THEN
        EXIT auth_lookup_loop;
      END IF;

      IF v_existing_auth_cross_operation IS NOT TRUE THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_BATCH_AUTH_START',
          'code', 'AUTH_REQUEST_REUSE_PROOF_MISMATCH',
          'message', 'The same-operation or same-idempotency authorisation request is not compatible with the current frozen payment proof.',
          'pay_batch_id', p_pay_batch_id::text,
          'auth_request_id', v_existing_auth_id::text,
          'auth_request_state', v_existing_auth_state,
          'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
          'compatibility', v_existing_auth_compatibility_json
        )::text USING ERRCODE = 'P0001';
      END IF;

      v_conflicting_auth_id := v_existing_auth_id;
      v_conflicting_auth_state := v_existing_auth_state;
      v_conflicting_auth_operation_id := v_existing_auth_operation_id;
      v_conflicting_auth_idempotency_key := v_existing_auth_idempotency_key;
      v_conflicting_auth_intent_json := v_existing_auth_intent_json;
    END IF;

    IF v_conflicting_auth_id IS NULL THEN
      SELECT auth_request.id,
             auth_request.state,
             NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'operation_id', '')), ''),
             NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'idempotency_key', '')), ''),
             COALESCE(auth_request.execution_intent_json, '{}'::jsonb)
      INTO v_conflicting_auth_id,
           v_conflicting_auth_state,
           v_conflicting_auth_operation_id,
           v_conflicting_auth_idempotency_key,
           v_conflicting_auth_intent_json
      FROM public.pay_batch_auth_requests AS auth_request
      WHERE auth_request.pay_batch_id = p_pay_batch_id
        AND auth_request.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
      ORDER BY auth_request.created_at_utc DESC NULLS LAST,
               auth_request.id DESC
      LIMIT 1;
    END IF;

    IF v_conflicting_auth_id IS NULL THEN
      EXIT auth_lookup_loop;
    END IF;

    IF p_operation_id IS NULL
       OR v_execution_mode NOT IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT') THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'AUTH_REQUEST_HELD_BY_PREVIOUS_OPERATION',
        'message', 'An active authorisation request belongs to another execution attempt or is not safely reusable.',
        'pay_batch_id', p_pay_batch_id::text,
        'auth_request_id', v_conflicting_auth_id::text,
        'auth_request_state', v_conflicting_auth_state,
        'auth_request_operation_id', v_conflicting_auth_operation_id,
        'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL::text ELSE p_operation_id::text END,
        'execution_mode', v_execution_mode,
        'compatibility', CASE WHEN v_existing_auth_id = v_conflicting_auth_id THEN v_existing_auth_compatibility_json ELSE NULL::jsonb END
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_stale_auth_failed_checks := '[]'::jsonb;
    v_stale_auth_safety_json := '{}'::jsonb;
    v_old_operation_id := NULL::uuid;
    v_old_operation_found := false;
    v_old_operation_execution_mode_raw := NULL::text;
    v_old_operation_execution_mode := NULL::text;
    v_old_auth_execution_mode := CASE
      WHEN UPPER(BTRIM(COALESCE(v_conflicting_auth_intent_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
      WHEN UPPER(BTRIM(COALESCE(v_conflicting_auth_intent_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
      WHEN UPPER(BTRIM(COALESCE(v_conflicting_auth_intent_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
      ELSE NULL::text
    END;
    v_old_auth_scope := UPPER(NULLIF(BTRIM(COALESCE(v_conflicting_auth_intent_json->>'pay_channel_scope', v_conflicting_auth_intent_json->>'scope', '')), ''));
    v_old_operation_reset_terminal := false;
    v_old_operation_manual_reset_marker := false;
    v_old_operation_scope_compatible := false;

    IF COALESCE(v_conflicting_auth_operation_id, '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_old_operation_id := v_conflicting_auth_operation_id::uuid;
      SELECT old_operation.*
      INTO v_old_operation_row
      FROM public.banking_pay_operations AS old_operation
      WHERE old_operation.id = v_old_operation_id
      FOR UPDATE;
      v_old_operation_found := FOUND;
    END IF;

    IF v_old_operation_found THEN
      v_old_operation_execution_mode_raw := UPPER(BTRIM(COALESCE(v_old_operation_row.input_json->>'execution_mode', '')));
      v_old_operation_execution_mode := CASE
        WHEN v_old_operation_execution_mode_raw IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
        WHEN v_old_operation_execution_mode_raw IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
        WHEN v_old_operation_execution_mode_raw IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
        ELSE NULL::text
      END;
      v_old_operation_manual_reset_marker := (
        LOWER(BTRIM(COALESCE(
          v_old_operation_row.progress_json->>'manual_reset_completed',
          v_old_operation_row.progress_json->>'safe_manual_reset_completed',
          v_old_operation_row.result_json->>'manual_reset_completed',
          v_old_operation_row.error_json->>'manual_reset_completed',
          'false'
        ))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND NULLIF(BTRIM(COALESCE(
          v_old_operation_row.progress_json->>'manual_reset_at_utc',
          v_old_operation_row.progress_json->>'safe_manual_reset_at_utc',
          v_old_operation_row.result_json->>'manual_reset_at_utc',
          v_old_operation_row.error_json->>'manual_reset_at_utc',
          ''
        )), '') IS NOT NULL
      );
      v_old_operation_reset_terminal := (
        UPPER(BTRIM(COALESCE(v_old_operation_row.status, ''))) = 'CANCELLED'
        OR (
          UPPER(BTRIM(COALESCE(v_old_operation_row.status, ''))) = 'FAILED'
          AND UPPER(BTRIM(COALESCE(v_old_operation_row.resume_reason, ''))) = 'PAYMENT_EXECUTION_LOCAL_ARTIFACTS_CLEANED'
          AND UPPER(BTRIM(COALESCE(
            v_old_operation_row.error_json->>'cleanup_mode',
            v_old_operation_row.progress_json->>'cleanup_mode',
            v_old_operation_row.result_json->>'cleanup_mode',
            ''
          ))) = 'CLEANED_BOUNDED_LOCAL_ARTIFACTS'
        )
        OR (
          UPPER(BTRIM(COALESCE(v_old_operation_row.status, ''))) IN ('FAILED', 'CANCELLED')
          AND v_old_operation_manual_reset_marker
        )
      );
      v_old_operation_scope_compatible := v_old_operation_row.pay_batch_id = p_pay_batch_id
        AND UPPER(BTRIM(COALESCE(v_old_operation_row.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
        AND v_old_operation_execution_mode IS NOT DISTINCT FROM v_execution_mode
        AND v_old_auth_execution_mode IS NOT DISTINCT FROM v_execution_mode
        AND v_old_auth_scope IS NOT DISTINCT FROM v_pay_channel_scope;
    END IF;

    SELECT COUNT(*)::integer
    INTO v_stale_provider_attempt_count
    FROM public.banking_pay_operation_provider_attempts AS provider_attempt
    WHERE provider_attempt.pay_batch_id = p_pay_batch_id
       OR provider_attempt.operation_id = v_old_operation_id;

    SELECT COUNT(*)::integer
    INTO v_stale_transfer_event_count
    FROM public.pay_bank_transfer_events AS transfer_event
    WHERE transfer_event.pay_batch_id = p_pay_batch_id;

    SELECT COUNT(*)::integer
    INTO v_stale_scope_provider_evidence_count
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope
    WHERE transfer_scope.pay_batch_id = p_pay_batch_id
      AND (
        COALESCE(transfer_scope.provider_submit_ready, false)
        OR COALESCE(transfer_scope.provider_submit_attempt_count, 0) > 0
        OR transfer_scope.provider_submit_chunk_id IS NOT NULL
        OR transfer_scope.provider_submit_claimed_at_utc IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_scope.provider_idempotency_key, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_scope.provider_request_id, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_scope.provider_transaction_id, '')), '') IS NOT NULL
        OR transfer_scope.provider_request_prepared_at_utc IS NOT NULL
        OR transfer_scope.provider_request_sending_at_utc IS NOT NULL
        OR transfer_scope.provider_request_sent_at_utc IS NOT NULL
        OR transfer_scope.provider_response_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(transfer_scope.provider_submit_state, 'NOT_READY'))) NOT IN ('', 'NOT_READY')
        OR UPPER(BTRIM(COALESCE(transfer_scope.status, ''))) = 'SUBMITTED'
      );

    SELECT COUNT(*)::integer
    INTO v_stale_transfer_provider_evidence_count
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
      AND (
        NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
        OR transfer_row.completed_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(transfer_row.status, ''))) NOT IN ('PENDING', 'BLOCKED', 'FAILED')
        OR UPPER(BTRIM(COALESCE(transfer_row.rail_state, ''))) NOT IN ('', 'LOCAL', 'PENDING')
        OR UPPER(BTRIM(COALESCE(transfer_row.failed_reason, ''))) IN ('PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'PROVIDER_OUTCOME_UNKNOWN', 'REQUEST_SENT_LOCAL')
        OR UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json->>'provider_stage', ''))) IN (
          'REQUEST_PREPARING', 'REQUEST_SENDING', 'REQUEST_SENT_LOCAL', 'PROVIDER_ACCEPTED',
          'PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'REVIEW_REQUIRED', 'CHUNK_FINALISED'
        )
        OR LOWER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_called}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR LOWER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_request_sent}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR LOWER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      );

    SELECT COUNT(*)::integer
    INTO v_stale_provider_chunk_evidence_count
    FROM public.banking_pay_operation_chunks AS provider_chunk
    WHERE provider_chunk.operation_id = v_old_operation_id
      AND (
        UPPER(BTRIM(COALESCE(provider_chunk.chunk_type, ''))) = 'TRANSFER_SUBMIT'
        OR UPPER(BTRIM(COALESCE(provider_chunk.phase, ''))) IN (
          'SUBMIT_PROVIDER_TRANSFERS', 'SEND_PROVIDER_CHUNK', 'REQUEST_PROVIDER_SEND',
          'FINALISE_PROVIDER_CHUNK', 'APPLY_RAIL_UPDATES'
        )
      )
      AND UPPER(BTRIM(COALESCE(provider_chunk.status, ''))) <> 'SKIPPED';

    SELECT COUNT(*)::integer
    INTO v_stale_settlement_scope_count
    FROM public.banking_pay_operation_settlement_scope AS settlement_scope
    WHERE settlement_scope.pay_batch_id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(settlement_scope.status, ''))) <> 'SKIPPED'
      AND settlement_scope.operation_id IN (
        SELECT related_operation.id
        FROM public.banking_pay_operations AS related_operation
        WHERE related_operation.id = v_old_operation_id
           OR related_operation.root_operation_id = v_old_operation_id
      );

    SELECT COUNT(*)::integer
    INTO v_stale_remittance_scope_count
    FROM public.banking_pay_operation_remittance_scope AS remittance_scope
    WHERE remittance_scope.pay_batch_id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(remittance_scope.status, ''))) <> 'SKIPPED'
      AND remittance_scope.operation_id IN (
        SELECT related_operation.id
        FROM public.banking_pay_operations AS related_operation
        WHERE related_operation.id = v_old_operation_id
           OR related_operation.root_operation_id = v_old_operation_id
      );

    SELECT COUNT(*)::integer
    INTO v_stale_correction_request_count
    FROM public.pay_payment_correction_requests AS correction_request
    WHERE correction_request.pay_batch_id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(correction_request.status, ''))) NOT IN ('CANCELLED', 'REJECTED');

    SELECT COUNT(*)::integer
    INTO v_stale_correction_item_count
    FROM public.pay_payment_correction_items AS correction_item
    WHERE correction_item.pay_batch_id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(correction_item.status, ''))) IN ('APPLIED', 'BLOCKED', 'FAILED');

    SELECT COUNT(*)::integer
    INTO v_stale_carry_forward_count
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward
    WHERE (carry_forward.source_pay_batch_id = p_pay_batch_id OR carry_forward.target_pay_batch_id = p_pay_batch_id)
      AND UPPER(BTRIM(COALESCE(carry_forward.status, ''))) NOT IN ('CANCELLED', 'SUPERSEDED');

    SELECT COUNT(*)::integer
    INTO v_stale_advance_patch_count
    FROM public.pay_advance_patches AS advance_patch
    WHERE advance_patch.pay_batch_id = p_pay_batch_id;

    SELECT COUNT(*)::integer
    INTO v_stale_advance_reservation_count
    FROM public.pay_advance_reservations AS advance_reservation
    WHERE advance_reservation.pay_batch_id = p_pay_batch_id
      AND (
        UPPER(BTRIM(COALESCE(advance_reservation.status, ''))) IN ('COMMITTED', 'SETTLED')
        OR advance_reservation.committed_at_utc IS NOT NULL
        OR advance_reservation.settled_at_utc IS NOT NULL
      );

    SELECT COUNT(*)::integer
    INTO v_stale_advance_payout_count
    FROM public.pay_advances AS advance_row
    WHERE advance_row.payout_pay_batch_id = p_pay_batch_id
      AND (
        UPPER(BTRIM(COALESCE(advance_row.payout_status::text, ''))) = 'PAID'
        OR advance_row.payout_transfer_id IS NOT NULL
      );

    SELECT COUNT(*)::integer
    INTO v_stale_other_active_operation_count
    FROM public.banking_pay_operations AS active_operation
    WHERE active_operation.pay_batch_id = p_pay_batch_id
      AND active_operation.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
      AND active_operation.id <> p_operation_id
      AND active_operation.id IS DISTINCT FROM v_old_operation_id
      AND UPPER(BTRIM(COALESCE(active_operation.status, ''))) NOT IN ('COMPLETE', 'FAILED', 'CANCELLED');

    SELECT COUNT(*)::integer
    INTO v_stale_remaining_active_auth_count
    FROM public.pay_batch_auth_requests AS other_auth_request
    WHERE other_auth_request.pay_batch_id = p_pay_batch_id
      AND other_auth_request.id <> v_conflicting_auth_id
      AND other_auth_request.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

    IF v_old_operation_id IS NULL THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'OLD_OPERATION_ID_VALID', 'passed', false));
    END IF;
    IF v_old_operation_found IS NOT TRUE THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'OLD_OPERATION_EXISTS', 'passed', false));
    END IF;
    IF v_old_operation_reset_terminal IS NOT TRUE THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'OLD_OPERATION_EXPLICITLY_RESET_TERMINAL', 'passed', false, 'old_operation_status', CASE WHEN v_old_operation_found THEN v_old_operation_row.status ELSE NULL::text END, 'old_operation_resume_reason', CASE WHEN v_old_operation_found THEN v_old_operation_row.resume_reason ELSE NULL::text END));
    END IF;
    IF v_old_operation_scope_compatible IS NOT TRUE THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'BATCH_SCOPE_MODE_COMPATIBLE', 'passed', false, 'old_operation_type', CASE WHEN v_old_operation_found THEN v_old_operation_row.operation_type ELSE NULL::text END, 'old_operation_mode', v_old_operation_execution_mode, 'old_auth_mode', v_old_auth_execution_mode, 'current_mode', v_execution_mode, 'old_auth_scope', v_old_auth_scope, 'current_scope', v_pay_channel_scope));
    END IF;
    IF UPPER(BTRIM(COALESCE(v_batch_row.status, ''))) <> 'DRAFT' THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'BATCH_STATUS_DRAFT', 'passed', false, 'batch_status', v_batch_row.status));
    END IF;
    IF UPPER(BTRIM(COALESCE(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
       OR NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
       OR v_batch_row.execution_committed_at_utc IS NOT NULL THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'EXECUTION_BOUNDARY_NOT_CROSSED', 'passed', false, 'execution_commit_state', v_batch_row.execution_commit_state, 'execution_commit_ref_present', NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL, 'execution_committed_at_utc_present', v_batch_row.execution_committed_at_utc IS NOT NULL));
    END IF;
    IF v_batch_row.settlement_confirmation_json IS NOT NULL
       AND v_batch_row.settlement_confirmation_json <> '{}'::jsonb THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_SETTLEMENT_CONFIRMATION', 'passed', false));
    END IF;
    IF COALESCE(v_stale_provider_attempt_count, 0) > 0 THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_PROVIDER_ATTEMPTS', 'passed', false, 'count', v_stale_provider_attempt_count));
    END IF;
    IF COALESCE(v_stale_transfer_event_count, 0) > 0 THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_TRANSFER_EVENTS', 'passed', false, 'count', v_stale_transfer_event_count));
    END IF;
    IF COALESCE(v_stale_scope_provider_evidence_count, 0) > 0
       OR COALESCE(v_stale_transfer_provider_evidence_count, 0) > 0
       OR COALESCE(v_stale_provider_chunk_evidence_count, 0) > 0 THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_PROVIDER_REQUEST_TRANSACTION_OR_SUBMISSION_EVIDENCE', 'passed', false, 'scope_evidence_count', v_stale_scope_provider_evidence_count, 'transfer_evidence_count', v_stale_transfer_provider_evidence_count, 'provider_chunk_count', v_stale_provider_chunk_evidence_count));
    END IF;
    IF COALESCE(v_stale_settlement_scope_count, 0) > 0 OR COALESCE(v_stale_remittance_scope_count, 0) > 0 THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_SETTLEMENT_OR_REMITTANCE_EXECUTION_SCOPE', 'passed', false, 'settlement_scope_count', v_stale_settlement_scope_count, 'remittance_scope_count', v_stale_remittance_scope_count));
    END IF;
    IF COALESCE(v_stale_correction_request_count, 0) > 0
       OR COALESCE(v_stale_correction_item_count, 0) > 0
       OR COALESCE(v_stale_carry_forward_count, 0) > 0
       OR COALESCE(v_stale_advance_patch_count, 0) > 0
       OR COALESCE(v_stale_advance_reservation_count, 0) > 0
       OR COALESCE(v_stale_advance_payout_count, 0) > 0 THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_ECONOMIC_CORRECTION_CARRY_FORWARD_OR_ADVANCE_CONSEQUENCE', 'passed', false, 'correction_request_count', v_stale_correction_request_count, 'correction_item_count', v_stale_correction_item_count, 'carry_forward_count', v_stale_carry_forward_count, 'advance_patch_count', v_stale_advance_patch_count, 'advance_reservation_count', v_stale_advance_reservation_count, 'advance_payout_count', v_stale_advance_payout_count));
    END IF;
    IF COALESCE(v_stale_other_active_operation_count, 0) > 0 THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_OTHER_ACTIVE_PAYMENT_EXECUTE_OPERATION', 'passed', false, 'count', v_stale_other_active_operation_count));
    END IF;
    IF COALESCE(v_stale_remaining_active_auth_count, 0) > 0 THEN
      v_stale_auth_failed_checks := v_stale_auth_failed_checks || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('check', 'NO_OTHER_ACTIVE_AUTH_REQUEST', 'passed', false, 'count', v_stale_remaining_active_auth_count));
    END IF;

    v_stale_auth_safety_json := JSONB_BUILD_OBJECT(
      'safe_to_cancel', JSONB_ARRAY_LENGTH(v_stale_auth_failed_checks) = 0,
      'failed_checks', v_stale_auth_failed_checks,
      'old_auth_request_id', v_conflicting_auth_id::text,
      'old_operation_id', CASE WHEN v_old_operation_id IS NULL THEN NULL::text ELSE v_old_operation_id::text END,
      'superseding_operation_id', p_operation_id::text,
      'execution_mode', v_execution_mode,
      'pay_channel_scope', v_pay_channel_scope,
      'provider_attempt_count', COALESCE(v_stale_provider_attempt_count, 0),
      'transfer_event_count', COALESCE(v_stale_transfer_event_count, 0),
      'scope_provider_evidence_count', COALESCE(v_stale_scope_provider_evidence_count, 0),
      'transfer_provider_evidence_count', COALESCE(v_stale_transfer_provider_evidence_count, 0),
      'provider_chunk_evidence_count', COALESCE(v_stale_provider_chunk_evidence_count, 0),
      'settlement_scope_count', COALESCE(v_stale_settlement_scope_count, 0),
      'remittance_scope_count', COALESCE(v_stale_remittance_scope_count, 0)
    ) || JSONB_BUILD_OBJECT(
      'correction_request_count', COALESCE(v_stale_correction_request_count, 0),
      'correction_item_count', COALESCE(v_stale_correction_item_count, 0),
      'carry_forward_count', COALESCE(v_stale_carry_forward_count, 0),
      'advance_patch_count', COALESCE(v_stale_advance_patch_count, 0),
      'advance_reservation_count', COALESCE(v_stale_advance_reservation_count, 0),
      'advance_payout_count', COALESCE(v_stale_advance_payout_count, 0),
      'other_active_operation_count', COALESCE(v_stale_other_active_operation_count, 0),
      'other_active_auth_request_count', COALESCE(v_stale_remaining_active_auth_count, 0)
    );

    IF JSONB_ARRAY_LENGTH(v_stale_auth_failed_checks) > 0 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'AUTH_REQUEST_HELD_BY_PREVIOUS_OPERATION',
        'message', 'The previous operation authorisation request could not be cancelled because one or more execution-boundary safety checks failed.',
        'pay_batch_id', p_pay_batch_id::text,
        'auth_request_id', v_conflicting_auth_id::text,
        'auth_request_state', v_conflicting_auth_state,
        'auth_request_operation_id', v_conflicting_auth_operation_id,
        'operation_id', p_operation_id::text,
        'safety_reconciliation', v_stale_auth_safety_json
      )::text USING ERRCODE = 'P0001';
    END IF;

    UPDATE public.pay_batch_auth_requests AS stale_auth_update
    SET state = 'CANCELLED',
        finalised_at_utc = COALESCE(stale_auth_update.finalised_at_utc, v_now),
        finalised_by_user_id = COALESCE(stale_auth_update.finalised_by_user_id, p_actor_user_id),
        execution_intent_json = JSONB_STRIP_NULLS(
          COALESCE(stale_auth_update.execution_intent_json, '{}'::jsonb)
          || JSONB_BUILD_OBJECT(
            'stale_auth_safe_cancelled', true,
            'stale_auth_safe_cancel_reason', 'SUPERSEDED_AFTER_EXPLICIT_OPERATION_RESET_WITH_NO_EXECUTION_BOUNDARY',
            'stale_auth_safe_cancelled_at_utc', v_now::text,
            'stale_auth_safe_cancelled_by_user_id', p_actor_user_id::text,
            'old_operation_id', CASE WHEN v_old_operation_id IS NULL THEN NULL::text ELSE v_old_operation_id::text END,
            'superseding_operation_id', p_operation_id::text,
            'superseding_idempotency_key', v_requested_idempotency_key,
            'stale_auth_safety_reconciliation', v_stale_auth_safety_json
          )
        )
    WHERE stale_auth_update.id = v_conflicting_auth_id
      AND stale_auth_update.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

    IF NOT FOUND THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_BATCH_AUTH_START',
        'code', 'STALE_AUTH_REQUEST_CANCELLATION_RACE',
        'message', 'The stale authorisation request changed state before safe cancellation could be recorded.',
        'pay_batch_id', p_pay_batch_id::text,
        'auth_request_id', v_conflicting_auth_id::text,
        'operation_id', p_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    UPDATE public.pay_batch_auth_tokens AS stale_token_update
    SET used_at_utc = COALESCE(stale_token_update.used_at_utc, v_now),
        expires_at_utc = LEAST(stale_token_update.expires_at_utc, v_now)
    WHERE stale_token_update.auth_request_id = v_conflicting_auth_id
      AND stale_token_update.used_at_utc IS NULL;
    GET DIAGNOSTICS v_stale_auth_tokens_voided = ROW_COUNT;

    IF v_old_operation_found THEN
      UPDATE public.banking_pay_operations AS old_operation_audit
      SET progress_json = JSONB_STRIP_NULLS(
            COALESCE(old_operation_audit.progress_json, '{}'::jsonb)
            || JSONB_BUILD_OBJECT(
              'stale_auth_request_cancelled', true,
              'stale_auth_request_id', v_conflicting_auth_id::text,
              'stale_auth_request_cancelled_at_utc', v_now::text,
              'superseding_operation_id', p_operation_id::text,
              'stale_auth_safety_reconciliation', v_stale_auth_safety_json
            )
          ),
          updated_at_utc = v_now
      WHERE old_operation_audit.id = v_old_operation_id;
    END IF;

    UPDATE public.banking_pay_operations AS current_operation_audit
    SET progress_json = JSONB_STRIP_NULLS(
          COALESCE(current_operation_audit.progress_json, '{}'::jsonb)
          || JSONB_BUILD_OBJECT(
            'stale_auth_request_cancelled', true,
            'stale_auth_request_id', v_conflicting_auth_id::text,
            'stale_auth_request_operation_id', CASE WHEN v_old_operation_id IS NULL THEN NULL::text ELSE v_old_operation_id::text END,
            'stale_auth_request_cancelled_at_utc', v_now::text,
            'stale_auth_tokens_voided', COALESCE(v_stale_auth_tokens_voided, 0),
            'stale_auth_safety_reconciliation', v_stale_auth_safety_json
          )
        ),
        updated_at_utc = v_now
    WHERE current_operation_audit.id = p_operation_id;

    v_stale_auth_request_cancelled := true;
    v_stale_auth_cancelled_request_id := v_conflicting_auth_id;
    v_stale_auth_cancelled_operation_id := v_old_operation_id;
    CONTINUE auth_lookup_loop;
  END LOOP auth_lookup_loop;

  IF v_existing_auth_id IS NOT NULL AND v_existing_auth_compatible THEN
    v_auth_state := v_existing_auth_state;
    v_execution_intent_json := v_existing_auth_intent_json;
    v_next_required_phase := CASE
      WHEN v_auth_state = 'AUTHORISED' AND v_scoped_no_transfer_execution_validated AND v_kind = 'SCHEDULED' THEN 'WAIT_FOR_SCHEDULE'
      WHEN v_auth_state = 'AUTHORISED' AND v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT') THEN 'SETTLEMENT'
      WHEN v_auth_state = 'AUTHORISED' AND v_kind = 'SCHEDULED' THEN 'WAIT_FOR_SCHEDULE'
      WHEN v_auth_state = 'AUTHORISED' THEN 'SUBMIT_PROVIDER_TRANSFERS'
      ELSE 'WAIT_FOR_AUTHORISATION'
    END;

    UPDATE public.pay_batches AS existing_auth_batch_update
    SET execution_intent_json = v_execution_intent_json
    WHERE existing_auth_batch_update.id = p_pay_batch_id;

    IF p_operation_id IS NOT NULL
       AND v_auth_state = 'AWAITING' THEN
      UPDATE public.banking_pay_operations AS operation_wait
      SET status = 'WAITING_AUTHORISATION',
          requires_user_action = true,
          runner_state = 'WAITING_USER',
          resume_reason = 'AWAITING_PAYMENT_AUTHORISATION',
          progress_json = JSONB_STRIP_NULLS(
            COALESCE(operation_wait.progress_json, '{}'::jsonb)
            || JSONB_BUILD_OBJECT(
              'waiting_authorisation_at_utc', v_now::text,
              'auth_request_id', v_existing_auth_id::text,
              'auth_state', v_auth_state,
              'next_required_phase', 'WAIT_FOR_AUTHORISATION'
            )
          ),
          updated_at_utc = v_now
      WHERE operation_wait.id = p_operation_id;
    END IF;

    RETURN JSONB_BUILD_OBJECT(
      'ok', true,
      'idempotent_reuse', true,
      'pay_batch_id', p_pay_batch_id::text,
      'auth_request_id', v_existing_auth_id::text,
      'auth_state', v_auth_state,
      'state', v_auth_state,
      'active_candidate_count', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_batch_candidates AS active_candidate
        WHERE active_candidate.pay_batch_id = p_pay_batch_id
          AND EXISTS (
            SELECT 1
            FROM public.pay_batch_items AS active_item
            WHERE active_item.pay_batch_candidate_id = active_candidate.id
              AND coalesce(active_item.is_voided, false) IS NOT TRUE
          )
      ),
      'active_amount', (
        SELECT coalesce(
          pg_catalog.sum(active_candidate.net_bank_amount),
          0::numeric
        )::numeric(14,2)
        FROM public.pay_batch_candidates AS active_candidate
        WHERE active_candidate.pay_batch_id = p_pay_batch_id
          AND EXISTS (
            SELECT 1
            FROM public.pay_batch_items AS active_item
            WHERE active_item.pay_batch_candidate_id = active_candidate.id
              AND coalesce(active_item.is_voided, false) IS NOT TRUE
          )
      ),
      'active_scope_hash', v_current_active_scope_hash,
      'reauthorisation', EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_requests AS completed_correction
        WHERE completed_correction.pay_batch_id = p_pay_batch_id
          AND completed_correction.status IN (
            'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED'
          )
      ),
      'display_message', 'Existing payment authorisation request reopened for the current active scope.',
      'operation_id', v_execution_intent_json->>'operation_id',
      'idempotency_key', v_execution_intent_json->>'idempotency_key',
      'freshness_result_hash', v_execution_intent_json->>'freshness_result_hash',
      'freshness_scope_hash', v_execution_intent_json->>'freshness_scope_hash',
      'next_required_phase', v_next_required_phase,
      'execution_mode', v_execution_mode,
      'schedule_kind', v_kind,
      'auth_request_reuse_reason', CASE
        WHEN p_operation_id IS NOT NULL AND v_existing_auth_operation_id = p_operation_id::text THEN 'SAME_OPERATION'
        ELSE 'SAME_IDEMPOTENCY_KEY'
      END,
      'auth_request_compatibility', v_existing_auth_compatibility_json,
      'stale_auth_request_cancelled', v_stale_auth_request_cancelled,
      'stale_auth_cancelled_request_id', CASE WHEN v_stale_auth_cancelled_request_id IS NULL THEN NULL::text ELSE v_stale_auth_cancelled_request_id::text END,
      'stale_auth_cancelled_operation_id', CASE WHEN v_stale_auth_cancelled_operation_id IS NULL THEN NULL::text ELSE v_stale_auth_cancelled_operation_id::text END,
      'stale_auth_tokens_voided', COALESCE(v_stale_auth_tokens_voided, 0),
      'execution_intent_json', v_execution_intent_json
    );
  END IF;

  UPDATE public.pay_batches AS batch_update
  SET schedule_kind = v_kind,
      scheduled_at_utc = CASE WHEN v_kind = 'SCHEDULED' THEN v_scheduled_at_utc ELSE NULL END,
      scheduled_by_user_id = CASE WHEN v_kind = 'SCHEDULED' THEN p_actor_user_id ELSE NULL END,
      funding_account_ref = v_funding_account_ref,
      funds_warning_hours_json = v_warning_hours_json,
      authoritative_payment_date = v_payment_date,
      authoritative_payment_date_source = 'PAY_BATCH_AUTH_START',
      execution_intent_json = v_execution_intent_json
  WHERE batch_update.id = p_pay_batch_id;

  INSERT INTO public.pay_batch_auth_requests (
    pay_batch_id,
    requested_by_user_id,
    required_quantity,
    schedule_kind,
    scheduled_at_utc,
    funding_account_ref,
    funds_warning_hours_json,
    execution_intent_json,
    state,
    golden_key_used,
    golden_key_user_id,
    created_at_utc
  )
  VALUES (
    p_pay_batch_id,
    p_actor_user_id,
    v_required_quantity,
    v_kind,
    v_scheduled_at_utc,
    v_funding_account_ref,
    v_warning_hours_json,
    v_execution_intent_json,
    'AWAITING',
    false,
    NULL::uuid,
    v_now
  )
  RETURNING id INTO v_auth_id;

  v_execution_intent_json := jsonb_set(v_execution_intent_json, '{auth_request_id}', to_jsonb(v_auth_id::text), true);

  UPDATE public.pay_batch_auth_requests AS auth_request_update
  SET execution_intent_json = v_execution_intent_json
  WHERE auth_request_update.id = v_auth_id;

  UPDATE public.pay_batches AS batch_intent_update
  SET execution_intent_json = v_execution_intent_json
  WHERE batch_intent_update.id = p_pay_batch_id;


  INSERT INTO public.pay_batch_auth_actions (
    auth_request_id,
    pay_batch_id,
    actor_user_id,
    action,
    action_at_utc,
    note
  )
  VALUES (
    v_auth_id,
    p_pay_batch_id,
    p_actor_user_id,
    CASE WHEN v_use_golden_key THEN 'USE_GOLDEN_KEY' ELSE 'AUTHORISE' END,
    v_now,
    NULL::text
  )
  ON CONFLICT (auth_request_id, actor_user_id) DO NOTHING;

  IF v_use_golden_key OR v_required_quantity <= 1 THEN
    UPDATE public.pay_batch_auth_requests AS auth_request_finalise
    SET state = 'AUTHORISED',
        golden_key_used = v_use_golden_key,
        golden_key_user_id = CASE WHEN v_use_golden_key THEN p_actor_user_id ELSE NULL::uuid END,
        finalised_at_utc = v_now,
        finalised_by_user_id = p_actor_user_id,
        execution_intent_json = v_execution_intent_json
    WHERE auth_request_finalise.id = v_auth_id;

    v_auth_state := 'AUTHORISED';
  ELSE
    v_auth_state := 'AWAITING';
  END IF;

  v_next_required_phase := CASE
    WHEN v_auth_state = 'AUTHORISED' AND v_scoped_no_transfer_execution_validated AND v_kind = 'SCHEDULED' THEN 'WAIT_FOR_SCHEDULE'
    WHEN v_auth_state = 'AUTHORISED' AND (v_scoped_no_transfer_execution_validated OR v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')) THEN 'SETTLEMENT'
    WHEN v_auth_state = 'AUTHORISED' AND v_kind = 'SCHEDULED' THEN 'WAIT_FOR_SCHEDULE'
    WHEN v_auth_state = 'AUTHORISED' THEN 'SUBMIT_PROVIDER_TRANSFERS'
    ELSE 'WAIT_FOR_AUTHORISATION'
  END;

  IF p_operation_id IS NOT NULL
     AND v_auth_state IN ('AWAITING', 'PENDING_AUTHORISATION') THEN
    UPDATE public.banking_pay_operations AS operation_wait
    SET status = 'WAITING_AUTHORISATION',
        requires_user_action = true,
        runner_state = 'WAITING_USER',
        resume_reason = 'AWAITING_PAYMENT_AUTHORISATION',
        progress_json = jsonb_strip_nulls(
          COALESCE(operation_wait.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'waiting_authorisation_at_utc', v_now::text,
            'auth_request_id', CASE WHEN v_auth_id IS NULL THEN NULL ELSE v_auth_id::text END,
            'auth_state', v_auth_state,
            'next_required_phase', 'WAIT_FOR_AUTHORISATION'
          )
        ),
        updated_at_utc = v_now
    WHERE operation_wait.id = p_operation_id;
  END IF;



  RETURN jsonb_build_object(
    'ok', true,
    'idempotent_reuse', false,
    'pay_batch_id', p_pay_batch_id::text,
    'auth_request_id', v_auth_id::text,
    'auth_state', v_auth_state,
    'state', v_auth_state,
    'active_candidate_count', (
      SELECT pg_catalog.count(*)::integer
      FROM public.pay_batch_candidates AS active_candidate
      WHERE active_candidate.pay_batch_id = p_pay_batch_id
        AND EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS active_item
          WHERE active_item.pay_batch_candidate_id = active_candidate.id
            AND coalesce(active_item.is_voided, false) IS NOT TRUE
        )
    ),
    'active_amount', (
      SELECT coalesce(
        pg_catalog.sum(active_candidate.net_bank_amount),
        0::numeric
      )::numeric(14,2)
      FROM public.pay_batch_candidates AS active_candidate
      WHERE active_candidate.pay_batch_id = p_pay_batch_id
        AND EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS active_item
          WHERE active_item.pay_batch_candidate_id = active_candidate.id
            AND coalesce(active_item.is_voided, false) IS NOT TRUE
        )
    ),
    'active_scope_hash', v_current_active_scope_hash,
    'reauthorisation', EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_requests AS completed_correction
      WHERE completed_correction.pay_batch_id = p_pay_batch_id
        AND completed_correction.status IN (
          'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED'
        )
    ),
    'display_message', 'Payment authorisation requested for the current active scope.',
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'idempotency_key', v_requested_idempotency_key,
    'freshness_result_hash', v_execution_intent_json->>'freshness_result_hash',
    'freshness_scope_hash', v_execution_intent_json->>'freshness_scope_hash',
    'next_required_phase', v_next_required_phase,
    'execution_mode', v_execution_mode,
    'schedule_kind', v_kind,
    'scheduled_at_utc', CASE WHEN v_scheduled_at_utc IS NULL THEN NULL ELSE v_scheduled_at_utc::text END,
    'funding_account_ref', v_funding_account_ref,
    'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
    'provider_attempt_or_evidence_transfer_count', COALESCE(v_provider_attempt_or_evidence_transfer_count, 0),
    'canonical_pending_status_transfer_count', COALESCE(v_canonical_pending_status_transfer_count, 0),
    'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
    'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
    'scoped_scope_failed_count', COALESCE(v_scoped_scope_failed_count, 0),
    'scoped_scope_skipped_count', COALESCE(v_scoped_scope_skipped_count, 0),
    'scoped_scope_without_transfer_count', COALESCE(v_scoped_scope_without_transfer_count, 0),
    'non_cancellable_auth_request_count', COALESCE(v_non_cancellable_auth_request_count, 0),
    'auth_request_retry_blocker_count', COALESCE(v_auth_request_retry_blocker_count, 0),
    'payment_date', v_payment_date::text,
    'required_quantity', v_required_quantity,
    'auth_start_path', v_auth_start_path,
    'pay_batch_prepare_skipped', v_used_operation_scope_proof,
    'used_operation_scope_proof', v_used_operation_scope_proof
  ) || jsonb_build_object(
    'provider_submit_chunk_risk_count', COALESCE(v_provider_submit_chunk_risk_count, 0),
    'operation_provider_submit_marker_count', COALESCE(v_operation_provider_submit_marker_count, 0),
    'transfer_identity_mismatch_count', COALESCE(v_transfer_identity_mismatch_count, 0),
    'transfer_amount_mismatch_count', COALESCE(v_transfer_amount_mismatch_count, 0),
    'transfer_currency_mismatch_count', COALESCE(v_transfer_currency_mismatch_count, 0),
    'transfer_external_state_count', COALESCE(v_transfer_external_state_count, 0),
    'scope_provider_not_ready_count', COALESCE(v_scope_provider_not_ready_count, 0),
    'scope_prepared_hash_missing_count', COALESCE(v_scope_prepared_hash_missing_count, 0),
    'scope_prepared_amount_mismatch_count', COALESCE(v_scope_prepared_amount_mismatch_count, 0),
    'scope_item_rollup_pending_count', COALESCE(v_scope_item_rollup_pending_count, 0),
    'payout_instruction_missing_count', COALESCE(v_payout_instruction_missing_count, 0),
    'scope_amount_total', ROUND(COALESCE(v_scope_amount_total, 0), 2),
    'transfer_amount_total', ROUND(COALESCE(v_transfer_amount_total, 0), 2),
    'scope_item_amount_total', ROUND(COALESCE(v_scope_item_amount_total, 0), 2),
    'batch_item_amount_total', ROUND(COALESCE(v_batch_item_amount_total, 0), 2),
    'scoped_no_transfer_execution', v_scoped_no_transfer_execution_validated,
    'no_bank_payment_execution', v_no_bank_payment_validated,
    'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_validated,
    'server_owned_payment_projection_proof', true,
    'payment_projection_proof_source', 'PAY_BATCH_AUTH_START',
    'paye_net_state_hash', v_current_paye_net_state_hash,
    'global_bank_payment_projection_hash', v_all_bank_payment_projection_hash,
    'scoped_paye_net_state_hash', v_scoped_paye_net_state_hash,
    'bank_payment_projection_hash', v_current_bank_payment_projection_hash,
    'missing_explicit_paye_input_count', v_global_paye_missing_count,
    'explicit_zero_count', v_global_paye_zero_count,
    'global_positive_bank_payment_count', v_global_positive_bank_payment_count,
    'global_invalid_payment_row_count', v_global_invalid_payment_row_count,
    'scoped_missing_explicit_paye_input_count', v_scoped_paye_missing_count,
    'scoped_explicit_zero_count', v_scoped_explicit_zero_count,
    'positive_bank_payment_count', v_scoped_positive_bank_payment_count,
    'positive_bank_payment_total', v_scoped_positive_bank_payment_total,
    'scoped_invalid_payment_row_count', v_scoped_invalid_payment_row_count,
    'bank_csv_evidence', v_csv_evidence_json,
    'waived_no_transfer_blockers', v_prepare_waived_blockers,
    'stale_auth_request_cancelled', v_stale_auth_request_cancelled,
    'stale_auth_cancelled_request_id', CASE WHEN v_stale_auth_cancelled_request_id IS NULL THEN NULL::text ELSE v_stale_auth_cancelled_request_id::text END,
    'stale_auth_cancelled_operation_id', CASE WHEN v_stale_auth_cancelled_operation_id IS NULL THEN NULL::text ELSE v_stale_auth_cancelled_operation_id::text END,
    'stale_auth_tokens_voided', COALESCE(v_stale_auth_tokens_voided, 0),
    'execution_intent_json', v_execution_intent_json
  );
END;
$function$;
ALTER FUNCTION public.pay_batch_auth_start(uuid,text,timestamptz,text,jsonb,uuid,text,text,date,text,boolean,boolean,boolean,text,text,uuid,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_auth_start(uuid,text,timestamptz,text,jsonb,uuid,text,text,date,text,boolean,boolean,boolean,text,text,uuid,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batch_auth_start(uuid,text,timestamptz,text,jsonb,uuid,text,text,date,text,boolean,boolean,boolean,text,text,uuid,text,text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batch_auth_start(uuid,text,timestamptz,text,jsonb,uuid,text,text,date,text,boolean,boolean,boolean,text,text,uuid,text,text) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batch_auth_start(uuid,text,timestamptz,text,jsonb,uuid,text,text,date,text,boolean,boolean,boolean,text,text,uuid,text,text) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_auth_start(uuid,text,timestamptz,text,jsonb,uuid,text,text,date,text,boolean,boolean,boolean,text,text,uuid,text,text) TO service_role;
