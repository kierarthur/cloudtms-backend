-- Canonical payment-batch prepare function.
-- Ordinary PAYE execution proves one frozen candidate destination at the
-- prepared transfer-scope level. Gross-side deductions remain fully linked
-- accounting members and do not require their own payout snapshot. Umbrella
-- and Loans execution retain row-level payout-instruction validation.
-- Policy X remains frozen-artifact only.

CREATE OR REPLACE FUNCTION public.pay_batch_prepare(p_pay_batch_id uuid, p_actor_user_id uuid, p_operation_id uuid DEFAULT NULL::uuid, p_freshness_result_hash text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_batch_row public.pay_batches%ROWTYPE;
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_summary_json jsonb := '{}'::jsonb;
  v_fresh_json jsonb := '{}'::jsonb;
  v_blockers_json jsonb := '[]'::jsonb;
  v_warnings_json jsonb := '[]'::jsonb;
  v_next_required_phase text := 'START_AUTHORISATION';
  v_ready boolean := false;
  v_is_stale boolean := false;
  v_batch_status text := NULL::text;
  v_execution_commit_state text := 'NOT_SUBMITTED';
  v_execution_mode text := 'STANDARD_BANK';
  v_execution_mode_raw text := NULL::text;
  v_is_local_manual_mode boolean := false;
  v_operation_input_json jsonb := '{}'::jsonb;
  v_manual_confirmation_mode text := NULL::text;
  v_csv_uploaded_confirmed boolean := false;
  v_csv_bank_confirm_ref text := NULL::text;
  v_external_settlement_comment text := NULL::text;
  v_payment_date_text text := NULL::text;
  v_operation_projection_scope text := NULL::text;
  v_operation_projection_proof_source text := NULL::text;
  v_operation_server_owned_projection_proof boolean := false;
  v_operation_global_paye_net_state_hash text := NULL::text;
  v_operation_global_bank_payment_projection_hash text := NULL::text;
  v_operation_scoped_paye_net_state_hash text := NULL::text;
  v_operation_scoped_bank_payment_projection_hash text := NULL::text;
  v_operation_global_missing_count integer := NULL::integer;
  v_operation_global_zero_count integer := NULL::integer;
  v_operation_global_positive_count integer := NULL::integer;
  v_operation_global_invalid_count integer := NULL::integer;
  v_operation_scoped_missing_count integer := NULL::integer;
  v_operation_scoped_zero_count integer := NULL::integer;
  v_operation_scoped_positive_count integer := NULL::integer;
  v_operation_scoped_positive_total numeric(14,2) := NULL::numeric;
  v_operation_scoped_invalid_count integer := NULL::integer;
  v_operation_bank_csv_generated boolean := false;
  v_operation_bank_csv_current boolean := false;
  v_operation_bank_csv_scope text := NULL::text;
  v_operation_bank_csv_paye_net_state_hash text := NULL::text;
  v_operation_bank_csv_bank_payment_projection_hash text := NULL::text;
  v_operation_bank_csv_row_count integer := NULL::integer;
  v_operation_bank_csv_total_amount numeric(14,2) := NULL::numeric;
  v_bank_csv_export_json jsonb := '{}'::jsonb;
  v_stored_csv_scope text := NULL::text;
  v_stored_csv_paye_net_state_hash text := NULL::text;
  v_stored_csv_bank_payment_projection_hash text := NULL::text;
  v_stored_csv_row_count integer := NULL::integer;
  v_stored_csv_total_amount numeric(14,2) := NULL::numeric;
  v_projection_proof_valid boolean := false;
  v_csv_proof_valid boolean := false;
  v_external_proof_valid boolean := false;
  v_mode_specific_proof_valid boolean := true;
  v_provider_scope_evidence_count integer := 0;
  v_provider_attempt_row_count integer := 0;
  v_provider_event_count integer := 0;
  v_provider_transfer_evidence_count integer := 0;
  v_provider_submit_chunk_count integer := 0;
  v_provider_boundary_evidence_count integer := 0;
  v_scope_item_transfer_link_mismatch_count integer := 0;
  v_scope_item_count_mismatch_count integer := 0;
  v_scope_item_amount_mismatch_count integer := 0;
  v_candidate_count integer := 0;
  v_item_count integer := 0;
  v_transfer_count integer := 0;
  v_pending_transfer_count integer := 0;
  v_authorisation_ready_transfer_count integer := 0;
  v_unattempted_submit_eligible_transfer_count integer := 0;
  v_remaining_unattempted_submit_required integer := 0;
  v_provider_attempt_or_evidence_transfer_count integer := 0;
  v_provider_or_ambiguous_evidence_transfer_count integer := 0;
  v_local_only_transfer_count integer := 0;
  v_canonical_pending_status_transfer_count integer := 0;
  v_safe_local_cleanup_transfer_count integer := 0;
  v_unsafe_transfer_count integer := 0;
  v_scoped_failed_or_blocked_transfer_count integer := 0;
  v_scoped_blocked_transfer_count integer := 0;
  v_scoped_operation_scope_count integer := 0;
  v_scoped_scope_prepared_count integer := 0;
  v_scoped_scope_pending_count integer := 0;
  v_scoped_scope_failed_count integer := 0;
  v_scoped_scope_skipped_count integer := 0;
  v_scoped_scope_without_transfer_count integer := 0;
  v_non_cancellable_auth_request_count integer := 0;
  v_auth_request_retry_blocker_count integer := 0;
  v_all_scoped_operation_scopes_authorisation_ready boolean := false;
  v_pay_channel_scope text := 'ALL';
  v_failed_transfer_count integer := 0;
  v_blocked_transfer_count integer := 0;
  v_submitted_transfer_count integer := 0;
  v_awaiting_net_count integer := 0;
  v_blocker_count integer := 0;
  v_warning_count integer := 0;
  v_diff_sample jsonb := '[]'::jsonb;
  v_operation_mode boolean := false;
  v_expected_freshness_hash text := null;
  v_expected_freshness_scope_hash text := null;
  v_batch_freshness_hash text := null;
  v_batch_freshness_scope_hash text := null;
  v_batch_freshness_status text := null;
  v_batch_freshness_json jsonb := '{}'::jsonb;
  v_large_batch boolean := false;
  v_scope_amount_total numeric(14,2) := 0;
  v_transfer_amount_total numeric(14,2) := 0;
  v_scope_item_amount_total numeric(14,2) := 0;
  v_batch_item_amount_total numeric(14,2) := 0;
  v_scope_prepared_amount_mismatch_count integer := 0;
  v_scope_prepared_hash_missing_count integer := 0;
  v_scope_provider_not_ready_count integer := 0;
  v_scope_item_rollup_pending_count integer := 0;
  v_payout_instruction_missing_count integer := 0;
  v_transfer_identity_mismatch_count integer := 0;
  v_transfer_amount_mismatch_count integer := 0;
  v_transfer_currency_mismatch_count integer := 0;
  v_transfer_status_not_pending_count integer := 0;
  v_transfer_external_state_count integer := 0;
  v_transfer_proof_hash text := NULL::text;
  v_transfer_scope_hash text := NULL::text;
  v_transfer_proof_hash_mismatch_count integer := 0;
  v_local_manual_zero_only_scope boolean := false;
  v_display_summary_row public.pay_batch_display_summary%ROWTYPE;
  v_operation_progress_json jsonb := '{}'::jsonb;
  v_operation_freshness_summary_json jsonb := '{}'::jsonb;
  v_operation_transfer_proof_json jsonb := '{}'::jsonb;
  v_operation_prepare_proof_json jsonb := '{}'::jsonb;
  v_operation_proof_status text := NULL::text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('OPERATION_ADVANCE');

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_batch_prepare: pay_batch_id is required'
    )::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_batch_prepare: actor_user_id is required',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id
    AND coalesce(actor_user.is_active, false) = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'ACTOR_USER_NOT_FOUND',
      'message', 'pay_batch_prepare: actor_user was not found or is inactive',
      'actor_user_id', p_actor_user_id::text,
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_PREPARE',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'pay_batch_prepare: pay_batch not found',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  END IF;

  v_batch_status := upper(btrim(coalesce(v_batch_row.status, '')));
  v_execution_commit_state := upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED')));
  IF v_execution_commit_state NOT IN ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED') THEN
    v_execution_commit_state := 'NOT_SUBMITTED';
  END IF;

  v_operation_mode := p_operation_id IS NOT NULL OR nullif(btrim(coalesce(p_freshness_result_hash, '')), '') IS NOT NULL;

  IF p_operation_id IS NOT NULL THEN
    SELECT operation_row.*
    INTO v_operation_row
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_PREPARE',
        'code', 'OPERATION_NOT_FOUND',
        'message', 'pay_batch_prepare: operation was not found',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_operation_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_PREPARE',
        'code', 'OPERATION_BATCH_MISMATCH',
        'message', 'pay_batch_prepare: operation belongs to another batch or is not bound to this batch',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'operation_pay_batch_id', CASE WHEN v_operation_row.pay_batch_id IS NULL THEN NULL::text ELSE v_operation_row.pay_batch_id::text END
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_operation_input_json := COALESCE(v_operation_row.input_json, '{}'::jsonb);
    v_execution_mode_raw := UPPER(BTRIM(COALESCE(v_operation_input_json->>'execution_mode', '')));
    v_execution_mode := CASE
      WHEN v_execution_mode_raw IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
      WHEN v_execution_mode_raw IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
      WHEN v_execution_mode_raw IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
      ELSE NULL::text
    END;

    IF v_execution_mode IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_PREPARE',
        'code', 'OPERATION_EXECUTION_MODE_INVALID',
        'message', 'The durable payment execution mode is missing or unsupported.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_mode_raw', NULLIF(v_execution_mode_raw, ''),
        'supported_execution_modes', jsonb_build_array('STANDARD_BANK', 'CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_is_local_manual_mode := v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT');

    IF v_operation_row.actor_user_id IS NOT NULL AND v_operation_row.actor_user_id <> p_actor_user_id THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_PREPARE',
        'code', 'OPERATION_ACTOR_MISMATCH',
        'message', 'pay_batch_prepare: operation belongs to another actor',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_expected_freshness_hash := COALESCE(
      NULLIF(BTRIM(COALESCE(p_freshness_result_hash, '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_row.progress_json->>'freshness_result_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_input_json->>'freshness_result_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_row.result_json #>> '{freshness,freshness_result_hash}', '')), '')
    );
    v_expected_freshness_scope_hash := COALESCE(
      NULLIF(BTRIM(COALESCE(v_operation_row.progress_json->>'freshness_scope_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_input_json->>'freshness_scope_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_row.result_json #>> '{freshness,freshness_scope_hash}', '')), '')
    );
    v_pay_channel_scope := UPPER(BTRIM(COALESCE(
      NULLIF(BTRIM(COALESCE(v_operation_input_json->>'pay_channel_scope', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_input_json->>'payChannelScope', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_row.config_json->>'pay_channel_scope', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_row.config_json->>'payChannelScope', '')), ''),
      'ALL'
    )));
    IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'LOANS', 'ALL') THEN
      v_pay_channel_scope := 'ALL';
    END IF;

    v_manual_confirmation_mode := UPPER(NULLIF(BTRIM(COALESCE(v_operation_input_json->>'manual_confirmation_mode', '')), ''));
    v_csv_uploaded_confirmed := LOWER(BTRIM(COALESCE(v_operation_input_json->>'csv_uploaded_confirmed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_csv_bank_confirm_ref := NULLIF(BTRIM(COALESCE(v_operation_input_json->>'csv_bank_confirm_ref', '')), '');
    v_external_settlement_comment := NULLIF(BTRIM(COALESCE(v_operation_input_json->>'external_settlement_comment', '')), '');
    v_payment_date_text := NULLIF(BTRIM(COALESCE(v_operation_input_json->>'payment_date', '')), '');
    v_operation_projection_scope := UPPER(NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'pay_channel_scope',
      v_operation_input_json->>'projection_scope',
      v_operation_input_json->>'scope',
      ''
    )), ''));
    v_operation_projection_proof_source := UPPER(NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'payment_projection_proof_source',
      v_operation_input_json->>'proof_source',
      ''
    )), ''));
    v_operation_server_owned_projection_proof := LOWER(BTRIM(COALESCE(v_operation_input_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_operation_global_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'global_paye_net_state_hash',
      v_operation_input_json->>'current_global_paye_net_state_hash',
      ''
    )), '');
    v_operation_global_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'global_bank_payment_projection_hash',
      v_operation_input_json->>'current_global_bank_payment_projection_hash',
      ''
    )), '');
    v_operation_scoped_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'scoped_paye_net_state_hash',
      v_operation_input_json->>'current_scoped_paye_net_state_hash',
      v_operation_input_json->>'paye_net_state_hash',
      v_operation_input_json->>'current_paye_net_state_hash',
      ''
    )), '');
    v_operation_scoped_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'scoped_bank_payment_projection_hash',
      v_operation_input_json->>'current_scoped_bank_payment_projection_hash',
      v_operation_input_json->>'bank_payment_projection_hash',
      v_operation_input_json->>'current_bank_payment_projection_hash',
      ''
    )), '');

    IF COALESCE(v_operation_input_json->>'global_missing_explicit_paye_input_count', v_operation_input_json->>'missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_operation_global_missing_count := COALESCE(v_operation_input_json->>'global_missing_explicit_paye_input_count', v_operation_input_json->>'missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'global_explicit_zero_count', v_operation_input_json->>'explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_operation_global_zero_count := COALESCE(v_operation_input_json->>'global_explicit_zero_count', v_operation_input_json->>'explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'global_positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_operation_global_positive_count := (v_operation_input_json->>'global_positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'global_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_operation_global_invalid_count := (v_operation_input_json->>'global_invalid_payment_row_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_operation_scoped_missing_count := (v_operation_input_json->>'scoped_missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'scoped_explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_operation_scoped_zero_count := (v_operation_input_json->>'scoped_explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'scoped_positive_bank_payment_count', v_operation_input_json->>'positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_operation_scoped_positive_count := COALESCE(v_operation_input_json->>'scoped_positive_bank_payment_count', v_operation_input_json->>'positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'scoped_positive_bank_payment_total', v_operation_input_json->>'positive_bank_payment_total', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
      v_operation_scoped_positive_total := ROUND(COALESCE(v_operation_input_json->>'scoped_positive_bank_payment_total', v_operation_input_json->>'positive_bank_payment_total')::numeric, 2)::numeric(14,2);
    END IF;
    IF COALESCE(v_operation_input_json->>'scoped_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_operation_scoped_invalid_count := (v_operation_input_json->>'scoped_invalid_payment_row_count')::integer;
    END IF;

    v_operation_bank_csv_generated := LOWER(BTRIM(COALESCE(v_operation_input_json->>'bank_csv_generated', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_operation_bank_csv_current := LOWER(BTRIM(COALESCE(v_operation_input_json->>'bank_csv_current', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_operation_bank_csv_scope := UPPER(NULLIF(BTRIM(COALESCE(v_operation_input_json->>'bank_csv_scope', '')), ''));
    v_operation_bank_csv_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'bank_csv_scoped_paye_net_state_hash',
      v_operation_input_json->>'bank_csv_paye_net_state_hash',
      ''
    )), '');
    v_operation_bank_csv_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_operation_input_json->>'bank_csv_scoped_bank_payment_projection_hash',
      v_operation_input_json->>'bank_csv_bank_payment_projection_hash',
      ''
    )), '');
    IF COALESCE(v_operation_input_json->>'bank_csv_row_count', '') ~ '^[0-9]+$' THEN
      v_operation_bank_csv_row_count := (v_operation_input_json->>'bank_csv_row_count')::integer;
    END IF;
    IF COALESCE(v_operation_input_json->>'bank_csv_total_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
      v_operation_bank_csv_total_amount := ROUND((v_operation_input_json->>'bank_csv_total_amount')::numeric, 2)::numeric(14,2);
    END IF;

    v_bank_csv_export_json := CASE
      WHEN JSONB_TYPEOF(v_batch_row.bank_csv_export_json) = 'object' THEN COALESCE(v_batch_row.bank_csv_export_json, '{}'::jsonb)
      ELSE '{}'::jsonb
    END;
    v_stored_csv_scope := UPPER(NULLIF(BTRIM(COALESCE(
      v_bank_csv_export_json->>'scope',
      v_bank_csv_export_json->>'pay_channel_scope',
      v_bank_csv_export_json->>'payChannelScope',
      ''
    )), ''));
    v_stored_csv_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_bank_csv_export_json->>'scoped_paye_net_state_hash',
      v_bank_csv_export_json->>'current_scoped_paye_net_state_hash',
      v_bank_csv_export_json->>'paye_net_state_hash',
      v_bank_csv_export_json->>'current_paye_net_state_hash',
      v_bank_csv_export_json->>'payeNetStateHash',
      v_bank_csv_export_json->>'scopedPayeNetStateHash',
      ''
    )), '');
    v_stored_csv_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_bank_csv_export_json->>'scoped_bank_payment_projection_hash',
      v_bank_csv_export_json->>'current_scoped_bank_payment_projection_hash',
      v_bank_csv_export_json->>'bank_payment_projection_hash',
      v_bank_csv_export_json->>'current_bank_payment_projection_hash',
      v_bank_csv_export_json->>'bankPaymentProjectionHash',
      v_bank_csv_export_json->>'scopedBankPaymentProjectionHash',
      ''
    )), '');
    IF COALESCE(v_bank_csv_export_json->>'row_count', v_bank_csv_export_json->>'rowCount', '') ~ '^[0-9]+$' THEN
      v_stored_csv_row_count := COALESCE(v_bank_csv_export_json->>'row_count', v_bank_csv_export_json->>'rowCount')::integer;
    END IF;
    IF COALESCE(v_bank_csv_export_json->>'total_amount', v_bank_csv_export_json->>'totalAmount', v_bank_csv_export_json->>'total', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
      v_stored_csv_total_amount := ROUND(COALESCE(v_bank_csv_export_json->>'total_amount', v_bank_csv_export_json->>'totalAmount', v_bank_csv_export_json->>'total')::numeric, 2)::numeric(14,2);
    END IF;

    v_projection_proof_valid := (
      v_operation_server_owned_projection_proof
      AND v_operation_projection_proof_source IS NOT NULL
      AND v_operation_projection_scope IS NOT DISTINCT FROM v_pay_channel_scope
      AND v_operation_global_paye_net_state_hash IS NOT NULL
      AND v_operation_global_bank_payment_projection_hash IS NOT NULL
      AND v_operation_scoped_paye_net_state_hash IS NOT NULL
      AND v_operation_scoped_bank_payment_projection_hash IS NOT NULL
      AND v_operation_global_missing_count IS NOT NULL
      AND v_operation_global_zero_count IS NOT NULL
      AND v_operation_global_positive_count IS NOT NULL
      AND v_operation_global_invalid_count IS NOT NULL
      AND v_operation_scoped_missing_count IS NOT NULL
      AND v_operation_scoped_zero_count IS NOT NULL
      AND v_operation_scoped_positive_count IS NOT NULL
      AND v_operation_scoped_positive_total IS NOT NULL
      AND v_operation_scoped_invalid_count IS NOT NULL
      AND v_operation_global_missing_count = 0
      AND v_operation_scoped_missing_count = 0
      AND v_operation_global_invalid_count = 0
      AND v_operation_scoped_invalid_count = 0
      AND (v_operation_scoped_positive_count > 0 OR v_operation_scoped_zero_count > 0)
    );

    v_csv_proof_valid := (
      v_execution_mode = 'CSV_SETTLEMENT'
      AND v_projection_proof_valid
      AND v_operation_bank_csv_generated
      AND v_operation_bank_csv_current
      AND v_bank_csv_export_json <> '{}'::jsonb
      AND v_operation_bank_csv_scope IS NOT DISTINCT FROM v_pay_channel_scope
      AND v_stored_csv_scope IS NOT DISTINCT FROM v_pay_channel_scope
      AND v_operation_bank_csv_paye_net_state_hash IS NOT DISTINCT FROM v_operation_scoped_paye_net_state_hash
      AND v_operation_bank_csv_bank_payment_projection_hash IS NOT DISTINCT FROM v_operation_scoped_bank_payment_projection_hash
      AND v_stored_csv_paye_net_state_hash IS NOT DISTINCT FROM v_operation_scoped_paye_net_state_hash
      AND v_stored_csv_bank_payment_projection_hash IS NOT DISTINCT FROM v_operation_scoped_bank_payment_projection_hash
      AND v_operation_bank_csv_row_count IS NOT DISTINCT FROM v_operation_scoped_positive_count
      AND v_stored_csv_row_count IS NOT DISTINCT FROM v_operation_scoped_positive_count
      AND ROUND(COALESCE(v_operation_bank_csv_total_amount, 0), 2) IS NOT DISTINCT FROM ROUND(COALESCE(v_operation_scoped_positive_total, 0), 2)
      AND ROUND(COALESCE(v_stored_csv_total_amount, 0), 2) IS NOT DISTINCT FROM ROUND(COALESCE(v_operation_scoped_positive_total, 0), 2)
      AND v_csv_uploaded_confirmed
      AND v_payment_date_text IS NOT NULL
      AND (
        (
          v_operation_scoped_positive_count > 0
          AND v_csv_bank_confirm_ref IS NOT NULL
          AND v_manual_confirmation_mode = 'BANK_UPLOAD_CONFIRMED'
        )
        OR (
          v_operation_scoped_positive_count = 0
          AND v_operation_scoped_zero_count > 0
          AND v_manual_confirmation_mode = 'ZERO_ROW_REVIEW_NO_BANK_PAYMENT'
        )
      )
    );

    v_external_proof_valid := (
      v_execution_mode = 'EXTERNAL_SETTLEMENT'
      AND v_projection_proof_valid
      AND v_payment_date_text IS NOT NULL
      AND (
        (
          v_operation_scoped_positive_count > 0
          AND v_external_settlement_comment IS NOT NULL
          AND v_manual_confirmation_mode = 'EXTERNAL_SETTLEMENT_CONFIRMED'
        )
        OR (
          v_operation_scoped_positive_count = 0
          AND v_operation_scoped_zero_count > 0
          AND v_manual_confirmation_mode = 'NO_BANK_PAYMENT_CONFIRMED'
        )
      )
    );

    v_mode_specific_proof_valid := CASE
      WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_proof_valid
      WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_proof_valid
      ELSE true
    END;

    v_local_manual_zero_only_scope := (
      v_is_local_manual_mode
      AND COALESCE(v_operation_scoped_positive_count, 0) = 0
      AND COALESCE(v_operation_scoped_zero_count, 0) > 0
      AND v_mode_specific_proof_valid IS TRUE
    );
  ELSE
    v_expected_freshness_hash := NULLIF(BTRIM(COALESCE(p_freshness_result_hash, '')), '');
  END IF;


  v_batch_freshness_status := upper(btrim(coalesce(v_batch_row.freshness_validation_status, '')));
  v_batch_freshness_hash := nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '');
  v_batch_freshness_scope_hash := nullif(btrim(coalesce(v_batch_row.freshness_scope_hash, '')), '');
  v_batch_freshness_json := coalesce(v_batch_row.freshness_result_json, '{}'::jsonb);

  IF p_operation_id IS NOT NULL THEN
    v_operation_progress_json := COALESCE(v_operation_row.progress_json, '{}'::jsonb);
    v_operation_freshness_summary_json := CASE
      WHEN JSONB_TYPEOF(v_operation_progress_json->'freshness_summary') = 'object' THEN COALESCE(v_operation_progress_json->'freshness_summary', '{}'::jsonb)
      ELSE '{}'::jsonb
    END;
    v_operation_transfer_proof_json := COALESCE(
      CASE WHEN JSONB_TYPEOF(v_operation_progress_json->'prepared_transfer_proof') = 'object' THEN v_operation_progress_json->'prepared_transfer_proof' ELSE NULL::jsonb END,
      CASE WHEN JSONB_TYPEOF(v_operation_progress_json->'transfer_prepare_proof') = 'object' THEN v_operation_progress_json->'transfer_prepare_proof' ELSE NULL::jsonb END,
      CASE WHEN JSONB_TYPEOF(v_operation_progress_json->'transfer_scope_proof') = 'object' THEN v_operation_progress_json->'transfer_scope_proof' ELSE NULL::jsonb END,
      CASE WHEN JSONB_TYPEOF(v_operation_progress_json#>'{pay_batch_prepare_proof,summary}') = 'object' THEN v_operation_progress_json#>'{pay_batch_prepare_proof,summary}' ELSE NULL::jsonb END,
      '{}'::jsonb
    );
    v_transfer_proof_hash := COALESCE(
      NULLIF(BTRIM(COALESCE(v_operation_transfer_proof_json->>'prepared_transfer_proof_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_transfer_proof_json->>'prepared_transfer_scope_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_transfer_proof_json->>'transfer_prepare_proof_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_transfer_proof_json->>'transfer_scope_hash', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation_transfer_proof_json->>'prepared_result_hash', '')), '')
    );

    SELECT COUNT(*)::integer
    INTO v_candidate_count
    FROM public.pay_batch_candidates AS proof_candidate
    WHERE proof_candidate.pay_batch_id = p_pay_batch_id;

    SELECT
      COUNT(*) FILTER (
        WHERE COALESCE(proof_item.is_voided, false) = false
          AND COALESCE(proof_item.item_type, '') <> 'DEBT_CREATED'
      )::integer,
      ROUND(COALESCE(SUM(
        CASE
          WHEN COALESCE(proof_item.is_voided, false) = false
           AND COALESCE(proof_item.item_type, '') <> 'DEBT_CREATED'
            THEN COALESCE(proof_item.amount_inc_vat, proof_item.amount_ex_vat, 0)
          ELSE 0
        END
      ), 0), 2)::numeric(14,2),
      COUNT(*) FILTER (
        WHERE COALESCE(proof_item.is_voided, false) = false
          AND COALESCE(proof_item.amount_inc_vat, proof_item.amount_ex_vat, 0) <> 0
          AND COALESCE(proof_item.item_type, '') <> 'DEBT_CREATED'
          AND NOT (
            UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = 'PAYE'
            AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) <> 'LOANS'
          )
          AND proof_item.payout_instruction_snapshot_json IS NULL
      )::integer
    INTO
      v_item_count,
      v_batch_item_amount_total,
      v_payout_instruction_missing_count
    FROM public.pay_batch_items AS proof_item
    JOIN public.pay_batch_candidates AS proof_candidate
      ON proof_candidate.id = proof_item.pay_batch_candidate_id
     AND proof_candidate.pay_batch_id = p_pay_batch_id
    WHERE (
      v_pay_channel_scope = 'ALL'
      OR UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = v_pay_channel_scope
      OR (
        v_pay_channel_scope = 'LOANS'
        AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) = 'LOANS'
        AND UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = 'PAYE'
      )
    );

    SELECT COUNT(*)::integer
    INTO v_awaiting_net_count
    FROM public.pay_batch_candidates AS proof_candidate
    WHERE proof_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(proof_candidate.awaiting_net_amount, false) = true
      AND (
        v_pay_channel_scope = 'ALL'
        OR EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS proof_item
          WHERE proof_item.pay_batch_candidate_id = proof_candidate.id
            AND COALESCE(proof_item.is_voided, false) = false
            AND (
              UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = v_pay_channel_scope
              OR (
                v_pay_channel_scope = 'LOANS'
                AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) = 'LOANS'
                AND UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = 'PAYE'
              )
            )
        )
      );

    WITH scoped_scope AS MATERIALIZED (
      SELECT
        operation_scope.id AS transfer_scope_id,
        operation_scope.operation_id,
        operation_scope.pay_batch_id,
        operation_scope.pay_channel,
        operation_scope.transfer_group_key,
        operation_scope.currency,
        operation_scope.amount,
        operation_scope.status,
        operation_scope.pay_bank_transfer_id,
        operation_scope.provider_submit_ready,
        operation_scope.provider_submit_state,
        operation_scope.provider_submit_chunk_id,
        operation_scope.provider_submit_claimed_at_utc,
        operation_scope.provider_submit_attempt_count,
        operation_scope.provider_idempotency_key,
        operation_scope.provider_request_id,
        operation_scope.provider_transaction_id,
        operation_scope.provider_request_prepared_at_utc,
        operation_scope.provider_request_sending_at_utc,
        operation_scope.provider_request_sent_at_utc,
        operation_scope.provider_response_at_utc,
        operation_scope.provider_submission_status,
        operation_scope.provider_review_required,
        operation_scope.provider_unsafe_reason,
        operation_scope.prepared_item_count,
        operation_scope.prepared_amount_total,
        operation_scope.prepared_scope_hash,
        operation_scope.prepared_result_hash,
        linked_transfer.id AS linked_transfer_id,
        linked_transfer.pay_batch_id AS linked_transfer_pay_batch_id,
        linked_transfer.pay_channel AS linked_transfer_pay_channel,
        linked_transfer.transfer_group_key AS linked_transfer_group_key,
        linked_transfer.amount AS linked_transfer_amount,
        linked_transfer.currency AS linked_transfer_currency,
        linked_transfer.status AS linked_transfer_status,
        linked_transfer.request_id AS linked_transfer_request_id,
        linked_transfer.rail_tx_id AS linked_transfer_rail_tx_id,
        linked_transfer.rail_state AS linked_transfer_rail_state,
        linked_transfer.completed_at_utc AS linked_transfer_completed_at_utc,
        linked_transfer.failed_reason AS linked_transfer_failed_reason,
        linked_transfer.rail_meta_json AS linked_transfer_rail_meta_json,
        COALESCE(scope_item_proof.item_count, 0) AS scope_item_count,
        COALESCE(scope_item_proof.item_amount_total, 0)::numeric(14,2) AS scope_item_amount_total,
        COALESCE(scope_item_proof.rollup_pending_count, 0) AS scope_item_rollup_pending_count,
        COALESCE(scope_item_proof.transfer_link_mismatch_count, 0) AS scope_item_transfer_link_mismatch_count
      FROM public.banking_pay_operation_transfer_scope AS operation_scope
      LEFT JOIN public.pay_bank_transfers AS linked_transfer
        ON linked_transfer.id = operation_scope.pay_bank_transfer_id
       AND linked_transfer.pay_batch_id = operation_scope.pay_batch_id
      LEFT JOIN LATERAL (
        SELECT
          COUNT(scope_item.id)::integer AS item_count,
          ROUND(COALESCE(SUM(scope_item.item_amount), 0), 2)::numeric(14,2) AS item_amount_total,
          COUNT(scope_item.id) FILTER (
            WHERE UPPER(BTRIM(COALESCE(scope_item.rollup_status, ''))) NOT IN ('ROLLED_UP', 'ROLLEDUP', 'READY', 'PREPARED', 'COMPLETE', 'COMPLETED')
          )::integer AS rollup_pending_count,
          COUNT(scope_item.id) FILTER (
            WHERE batch_item.id IS NULL
               OR batch_item.pay_bank_transfer_id IS DISTINCT FROM operation_scope.pay_bank_transfer_id
          )::integer AS transfer_link_mismatch_count
        FROM public.banking_pay_operation_transfer_scope_items AS scope_item
        LEFT JOIN public.pay_batch_items AS batch_item
          ON batch_item.id = scope_item.pay_batch_item_id
        WHERE scope_item.operation_id = operation_scope.operation_id
          AND scope_item.pay_batch_id = operation_scope.pay_batch_id
          AND scope_item.transfer_scope_id = operation_scope.id
      ) AS scope_item_proof ON true
      WHERE operation_scope.operation_id = p_operation_id
        AND operation_scope.pay_batch_id = p_pay_batch_id
        AND (
          v_pay_channel_scope = 'ALL'
          OR UPPER(BTRIM(COALESCE(operation_scope.pay_channel, ''))) = v_pay_channel_scope
          OR (
            v_pay_channel_scope = 'LOANS'
            AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) = 'LOANS'
            AND UPPER(BTRIM(COALESCE(operation_scope.pay_channel, ''))) = 'PAYE'
          )
        )
    ), scope_evidence AS MATERIALIZED (
      SELECT
        scoped_scope.*,
        (
          scoped_scope.linked_transfer_id IS NOT NULL
          AND scoped_scope.linked_transfer_pay_batch_id IS NOT DISTINCT FROM scoped_scope.pay_batch_id
          AND scoped_scope.linked_transfer_pay_channel IS NOT DISTINCT FROM scoped_scope.pay_channel
          AND scoped_scope.linked_transfer_group_key IS NOT DISTINCT FROM scoped_scope.transfer_group_key
          AND ROUND(COALESCE(scoped_scope.linked_transfer_amount, 0), 2) IS NOT DISTINCT FROM ROUND(COALESCE(scoped_scope.amount, 0), 2)
          AND UPPER(BTRIM(COALESCE(scoped_scope.linked_transfer_currency, 'GBP'))) IS NOT DISTINCT FROM UPPER(BTRIM(COALESCE(scoped_scope.currency, 'GBP')))
        ) AS transfer_identity_clean,
        (
          scoped_scope.linked_transfer_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(scoped_scope.linked_transfer_status, ''))) = 'PENDING'
          AND NULLIF(BTRIM(COALESCE(scoped_scope.linked_transfer_rail_tx_id, '')), '') IS NULL
          AND scoped_scope.linked_transfer_completed_at_utc IS NULL
          AND NULLIF(BTRIM(COALESCE(scoped_scope.linked_transfer_failed_reason, '')), '') IS NULL
          AND UPPER(BTRIM(COALESCE(scoped_scope.linked_transfer_rail_state, ''))) IN ('', 'LOCAL', 'PENDING')
        ) AS transfer_local_state_clean,
        EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS transfer_event
          WHERE transfer_event.pay_batch_id = p_pay_batch_id
            AND (
              transfer_event.pay_bank_transfer_id = scoped_scope.linked_transfer_id
              OR (
                transfer_event.pay_bank_transfer_id IS NULL
                AND (
                  NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = scoped_scope.linked_transfer_id::text
                  OR NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = NULLIF(BTRIM(COALESCE(scoped_scope.linked_transfer_request_id, '')), '')
                  OR NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), '') = NULLIF(BTRIM(COALESCE(scoped_scope.linked_transfer_request_id, '')), '')
                  OR NULLIF(BTRIM(COALESCE(transfer_event.provider_request_id, '')), '') = NULLIF(BTRIM(COALESCE(scoped_scope.linked_transfer_request_id, '')), '')
                  OR NULLIF(BTRIM(COALESCE(transfer_event.provider_transaction_id, '')), '') = NULLIF(BTRIM(COALESCE(scoped_scope.linked_transfer_rail_tx_id, '')), '')
                )
              )
            )
            AND (
              UPPER(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'PROVIDER_RESPONSE')
              OR NULLIF(BTRIM(COALESCE(transfer_event.provider_request_id, '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(transfer_event.provider_transaction_id, '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(transfer_event.provider_event_id, '')), '') IS NOT NULL
              OR transfer_event.provider_webhook_receipt_id IS NOT NULL
            )
        ) AS provider_event_present,
        EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_provider_attempts AS provider_attempt
          WHERE provider_attempt.operation_id = p_operation_id
            AND (
              provider_attempt.transfer_scope_id = scoped_scope.transfer_scope_id
              OR provider_attempt.transfer_scope_id IS NULL
            )
        ) AS provider_attempt_present,
        (
          COALESCE(scoped_scope.provider_submit_attempt_count, 0) > 0
          OR scoped_scope.provider_submit_chunk_id IS NOT NULL
          OR scoped_scope.provider_submit_claimed_at_utc IS NOT NULL
          OR scoped_scope.provider_request_prepared_at_utc IS NOT NULL
          OR scoped_scope.provider_request_sending_at_utc IS NOT NULL
          OR scoped_scope.provider_request_sent_at_utc IS NOT NULL
          OR scoped_scope.provider_response_at_utc IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(scoped_scope.provider_transaction_id, '')), '') IS NOT NULL
          OR UPPER(BTRIM(COALESCE(scoped_scope.provider_submit_state, 'NOT_READY'))) IN (
            'CLAIMED', 'REQUEST_PREPARING', 'REQUEST_SENDING', 'REQUEST_SENT_LOCAL',
            'PROVIDER_ACCEPTED', 'PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'CHUNK_FINALISED'
          )
        ) AS provider_dispatch_started,
        (
          UPPER(BTRIM(COALESCE(scoped_scope.status, ''))) = 'PREPARED'
          AND COALESCE(scoped_scope.prepared_item_count, 0) > 0
          AND NULLIF(BTRIM(COALESCE(scoped_scope.prepared_scope_hash, '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(scoped_scope.prepared_result_hash, '')), '') IS NOT NULL
          AND ROUND(COALESCE(scoped_scope.prepared_amount_total, 0), 2) IS NOT DISTINCT FROM ROUND(COALESCE(scoped_scope.amount, 0), 2)
          AND ROUND(COALESCE(scoped_scope.prepared_amount_total, 0), 2) > 0
          AND scoped_scope.scope_item_count = COALESCE(scoped_scope.prepared_item_count, 0)
          AND scoped_scope.scope_item_rollup_pending_count = 0
          AND scoped_scope.scope_item_transfer_link_mismatch_count = 0
          AND ROUND(COALESCE(scoped_scope.scope_item_amount_total, 0), 2) IS NOT DISTINCT FROM ROUND(COALESCE(scoped_scope.amount, 0), 2)
        ) AS frozen_scope_proof_clean
      FROM scoped_scope
    ), scope_readiness AS MATERIALIZED (
      SELECT
        scope_evidence.*,
        (
          scope_evidence.transfer_identity_clean
          AND scope_evidence.transfer_local_state_clean
          AND scope_evidence.provider_event_present IS NOT TRUE
          AND scope_evidence.provider_attempt_present IS NOT TRUE
          AND scope_evidence.provider_dispatch_started IS NOT TRUE
        ) AS transfer_evidence_clean,
        CASE
          WHEN v_execution_mode = 'STANDARD_BANK' THEN
            COALESCE(scope_evidence.provider_submit_ready, false) = true
            AND UPPER(BTRIM(COALESCE(scope_evidence.provider_submit_state, ''))) = 'READY'
            AND COALESCE(scope_evidence.provider_review_required, false) = false
            AND NULLIF(BTRIM(COALESCE(scope_evidence.provider_unsafe_reason, '')), '') IS NULL
          ELSE
            v_mode_specific_proof_valid
            AND COALESCE(scope_evidence.provider_submit_ready, false) = false
            AND UPPER(BTRIM(COALESCE(scope_evidence.provider_submit_state, 'NOT_READY'))) IN ('', 'NOT_READY')
            AND scope_evidence.provider_submit_chunk_id IS NULL
            AND scope_evidence.provider_submit_claimed_at_utc IS NULL
            AND COALESCE(scope_evidence.provider_submit_attempt_count, 0) = 0
            AND NULLIF(BTRIM(COALESCE(scope_evidence.provider_idempotency_key, '')), '') IS NULL
            AND NULLIF(BTRIM(COALESCE(scope_evidence.provider_request_id, '')), '') IS NULL
            AND NULLIF(BTRIM(COALESCE(scope_evidence.provider_transaction_id, '')), '') IS NULL
            AND scope_evidence.provider_request_prepared_at_utc IS NULL
            AND scope_evidence.provider_request_sending_at_utc IS NULL
            AND scope_evidence.provider_request_sent_at_utc IS NULL
            AND scope_evidence.provider_response_at_utc IS NULL
            AND NULLIF(BTRIM(COALESCE(scope_evidence.provider_submission_status, '')), '') IS NULL
            AND COALESCE(scope_evidence.provider_review_required, false) = false
            AND NULLIF(BTRIM(COALESCE(scope_evidence.provider_unsafe_reason, '')), '') IS NULL
        END AS mode_route_ready
      FROM scope_evidence
    )
    SELECT
      COUNT(*)::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_readiness.status, ''))) = 'PREPARED')::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_readiness.status, ''))) IN ('PENDING', 'ROLLED_UP'))::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_readiness.status, ''))) = 'FAILED')::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_readiness.status, ''))) = 'SKIPPED')::integer,
      COUNT(*) FILTER (WHERE scope_readiness.pay_bank_transfer_id IS NULL OR scope_readiness.linked_transfer_id IS NULL)::integer,
      COUNT(*) FILTER (
        WHERE scope_readiness.frozen_scope_proof_clean
          AND scope_readiness.transfer_evidence_clean
          AND scope_readiness.mode_route_ready
      )::integer,
      COUNT(*) FILTER (
        WHERE scope_readiness.provider_event_present
           OR scope_readiness.provider_attempt_present
           OR scope_readiness.provider_dispatch_started
           OR scope_readiness.transfer_local_state_clean IS NOT TRUE
           OR (
             v_is_local_manual_mode
             AND (
               COALESCE(scope_readiness.provider_submit_ready, false) = true
               OR UPPER(BTRIM(COALESCE(scope_readiness.provider_submit_state, 'NOT_READY'))) NOT IN ('', 'NOT_READY')
               OR NULLIF(BTRIM(COALESCE(scope_readiness.provider_idempotency_key, '')), '') IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(scope_readiness.provider_request_id, '')), '') IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(scope_readiness.provider_submission_status, '')), '') IS NOT NULL
             )
           )
      )::integer,
      COUNT(*) FILTER (WHERE scope_readiness.transfer_identity_clean IS NOT TRUE)::integer,
      COUNT(*) FILTER (
        WHERE scope_readiness.linked_transfer_id IS NOT NULL
          AND ROUND(COALESCE(scope_readiness.linked_transfer_amount, 0), 2) IS DISTINCT FROM ROUND(COALESCE(scope_readiness.amount, 0), 2)
      )::integer,
      COUNT(*) FILTER (
        WHERE scope_readiness.linked_transfer_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(scope_readiness.linked_transfer_currency, 'GBP'))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(scope_readiness.currency, 'GBP')))
      )::integer,
      COUNT(*) FILTER (
        WHERE scope_readiness.linked_transfer_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(scope_readiness.linked_transfer_status, ''))) <> 'PENDING'
      )::integer,
      COUNT(*) FILTER (
        WHERE scope_readiness.linked_transfer_id IS NOT NULL
          AND scope_readiness.transfer_local_state_clean IS NOT TRUE
      )::integer,
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_readiness.status, ''))) = 'PREPARED'
          AND scope_readiness.mode_route_ready IS NOT TRUE
      )::integer,
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_readiness.status, ''))) = 'PREPARED'
          AND (
            NULLIF(BTRIM(COALESCE(scope_readiness.prepared_scope_hash, '')), '') IS NULL
            OR NULLIF(BTRIM(COALESCE(scope_readiness.prepared_result_hash, '')), '') IS NULL
          )
      )::integer,
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_readiness.status, ''))) = 'PREPARED'
          AND ROUND(COALESCE(scope_readiness.prepared_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(scope_readiness.amount, 0), 2)
      )::integer,
      COALESCE(SUM(scope_readiness.scope_item_rollup_pending_count), 0)::integer,
      COALESCE(SUM(scope_readiness.scope_item_transfer_link_mismatch_count), 0)::integer,
      COUNT(*) FILTER (
        WHERE scope_readiness.scope_item_count IS DISTINCT FROM COALESCE(scope_readiness.prepared_item_count, 0)
      )::integer,
      COUNT(*) FILTER (
        WHERE ROUND(COALESCE(scope_readiness.scope_item_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(scope_readiness.amount, 0), 2)
      )::integer,
      ROUND(COALESCE(SUM(scope_readiness.amount), 0), 2)::numeric(14,2),
      ROUND(COALESCE(SUM(scope_readiness.linked_transfer_amount), 0), 2)::numeric(14,2),
      ROUND(COALESCE(SUM(scope_readiness.scope_item_amount_total), 0), 2)::numeric(14,2),
      MD5(COALESCE(STRING_AGG(
        scope_readiness.transfer_scope_id::text || ':' ||
        COALESCE(scope_readiness.pay_bank_transfer_id::text, '') || ':' ||
        COALESCE(scope_readiness.status, '') || ':' ||
        ROUND(COALESCE(scope_readiness.amount, 0), 2)::text || ':' ||
        COALESCE(scope_readiness.prepared_scope_hash, '') || ':' ||
        COALESCE(scope_readiness.prepared_result_hash, ''),
        '|' ORDER BY scope_readiness.transfer_scope_id
      ), 'NO_SCOPE'))
    INTO
      v_scoped_operation_scope_count,
      v_scoped_scope_prepared_count,
      v_scoped_scope_pending_count,
      v_scoped_scope_failed_count,
      v_scoped_scope_skipped_count,
      v_scoped_scope_without_transfer_count,
      v_authorisation_ready_transfer_count,
      v_unsafe_transfer_count,
      v_transfer_identity_mismatch_count,
      v_transfer_amount_mismatch_count,
      v_transfer_currency_mismatch_count,
      v_transfer_status_not_pending_count,
      v_transfer_external_state_count,
      v_scope_provider_not_ready_count,
      v_scope_prepared_hash_missing_count,
      v_scope_prepared_amount_mismatch_count,
      v_scope_item_rollup_pending_count,
      v_scope_item_transfer_link_mismatch_count,
      v_scope_item_count_mismatch_count,
      v_scope_item_amount_mismatch_count,
      v_scope_amount_total,
      v_transfer_amount_total,
      v_scope_item_amount_total,
      v_transfer_scope_hash
    FROM scope_readiness;

    v_transfer_proof_hash_mismatch_count := CASE
      WHEN v_transfer_proof_hash IS NOT NULL
       AND v_transfer_scope_hash IS NOT NULL
       AND v_transfer_proof_hash IS DISTINCT FROM v_transfer_scope_hash THEN 1
      ELSE 0
    END;

    IF v_is_local_manual_mode THEN
      SELECT COUNT(*)::integer
      INTO v_provider_scope_evidence_count
      FROM public.banking_pay_operation_transfer_scope AS provider_scope
      WHERE provider_scope.operation_id = p_operation_id
        AND provider_scope.pay_batch_id = p_pay_batch_id
        AND (
          COALESCE(provider_scope.provider_submit_ready, false) = true
          OR UPPER(BTRIM(COALESCE(provider_scope.provider_submit_state, 'NOT_READY'))) NOT IN ('', 'NOT_READY')
          OR provider_scope.provider_submit_chunk_id IS NOT NULL
          OR provider_scope.provider_submit_claimed_at_utc IS NOT NULL
          OR COALESCE(provider_scope.provider_submit_attempt_count, 0) > 0
          OR NULLIF(BTRIM(COALESCE(provider_scope.provider_idempotency_key, '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(provider_scope.provider_request_id, '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(provider_scope.provider_transaction_id, '')), '') IS NOT NULL
          OR provider_scope.provider_request_prepared_at_utc IS NOT NULL
          OR provider_scope.provider_request_sending_at_utc IS NOT NULL
          OR provider_scope.provider_request_sent_at_utc IS NOT NULL
          OR provider_scope.provider_response_at_utc IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(provider_scope.provider_submission_status, '')), '') IS NOT NULL
        );

      SELECT COUNT(*)::integer
      INTO v_provider_attempt_row_count
      FROM public.banking_pay_operation_provider_attempts AS provider_attempt
      WHERE provider_attempt.operation_id = p_operation_id
         OR provider_attempt.pay_batch_id = p_pay_batch_id;

      SELECT COUNT(*)::integer
      INTO v_provider_event_count
      FROM public.pay_bank_transfer_events AS transfer_event
      WHERE transfer_event.pay_batch_id = p_pay_batch_id
        AND (
          UPPER(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_WEBHOOK', 'PROVIDER_POLL', 'PROVIDER_RESPONSE')
          OR NULLIF(BTRIM(COALESCE(transfer_event.provider_request_id, '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_event.provider_transaction_id, '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_event.provider_event_id, '')), '') IS NOT NULL
          OR transfer_event.provider_webhook_receipt_id IS NOT NULL
        );

      SELECT COUNT(*)::integer
      INTO v_provider_transfer_evidence_count
      FROM public.pay_bank_transfers AS transfer_row
      WHERE transfer_row.pay_batch_id = p_pay_batch_id
        AND (
          UPPER(BTRIM(COALESCE(transfer_row.status, ''))) NOT IN ('PENDING', 'BLOCKED', 'FAILED')
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
          OR transfer_row.completed_at_utc IS NOT NULL
          OR UPPER(BTRIM(COALESCE(transfer_row.rail_state, ''))) NOT IN ('', 'LOCAL', 'PENDING')
          OR UPPER(BTRIM(COALESCE(transfer_row.failed_reason, ''))) IN ('PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'PROVIDER_OUTCOME_UNKNOWN', 'REQUEST_SENT_LOCAL')
          OR UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json->>'provider_stage', ''))) IN ('REQUEST_PREPARING', 'REQUEST_SENDING', 'REQUEST_SENT_LOCAL', 'PROVIDER_ACCEPTED', 'PROVIDER_REJECTED', 'PROVIDER_UNKNOWN', 'REVIEW_REQUIRED', 'CHUNK_FINALISED')
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), '') IS NOT NULL
          OR LOWER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_request_sent}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR LOWER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_called}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR LOWER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR LOWER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_acceptance_evidence_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        );

      SELECT COUNT(*)::integer
      INTO v_provider_submit_chunk_count
      FROM public.banking_pay_operation_chunks AS provider_chunk
      WHERE provider_chunk.operation_id = p_operation_id
        AND (
          UPPER(BTRIM(COALESCE(provider_chunk.chunk_type, ''))) = 'TRANSFER_SUBMIT'
          OR UPPER(BTRIM(COALESCE(provider_chunk.phase, ''))) IN ('SUBMIT_PROVIDER_TRANSFERS', 'SEND_PROVIDER_CHUNK', 'REQUEST_PROVIDER_SEND', 'FINALISE_PROVIDER_CHUNK', 'APPLY_RAIL_UPDATES')
        );

      v_provider_boundary_evidence_count := COALESCE(v_provider_scope_evidence_count, 0)
        + COALESCE(v_provider_attempt_row_count, 0)
        + COALESCE(v_provider_event_count, 0)
        + COALESCE(v_provider_transfer_evidence_count, 0)
        + COALESCE(v_provider_submit_chunk_count, 0);
    END IF;

    IF v_execution_commit_state <> 'NOT_SUBMITTED'
       OR NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
       OR v_batch_row.execution_committed_at_utc IS NOT NULL THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'EXECUTION_STATE_CONFLICT',
        'message', 'This payment batch has already crossed the execution boundary and cannot be prepared again.',
        'execution_commit_state', v_execution_commit_state
      ));
    END IF;

    IF v_batch_status IN ('CANCELLED', 'CANCELED') THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'BATCH_CANCELLED',
        'message', 'The payment batch has been cancelled.'
      ));
    ELSIF v_batch_status <> 'DRAFT' THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'BATCH_STATUS_NOT_DRAFT',
        'message', 'Only a DRAFT batch can be prepared for payment execution.',
        'batch_status', v_batch_row.status
      ));
    END IF;

    IF v_candidate_count <= 0 THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'NO_CANDIDATES',
        'message', 'The payment batch has no candidates.'
      ));
    END IF;

    IF v_item_count <= 0 THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'NO_PAYMENT_ITEMS',
        'message', 'The payment batch has no payment items in the requested frozen scope.'
      ));
    END IF;

    IF v_awaiting_net_count > 0
       OR COALESCE(v_operation_global_missing_count, 0) > 0
       OR COALESCE(v_operation_scoped_missing_count, 0) > 0 THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'PAYE_NET_REQUIRED',
        'message', 'PAYE net amounts are required before this batch can proceed.',
        'candidate_count', v_awaiting_net_count,
        'global_missing_explicit_paye_input_count', v_operation_global_missing_count,
        'scoped_missing_explicit_paye_input_count', v_operation_scoped_missing_count
      ));
    END IF;

    IF v_batch_freshness_status <> 'PASSED'
       OR v_batch_freshness_hash IS NULL
       OR COALESCE((v_batch_freshness_json->>'is_stale')::boolean, false) IS TRUE THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', CASE WHEN v_batch_freshness_status = 'STALE' THEN 'BATCH_STALE' ELSE 'FRESHNESS_REQUIRED' END,
        'message', 'Chunked freshness must pass before this payment batch can be prepared.',
        'freshness_validation_status', NULLIF(v_batch_freshness_status, ''),
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash,
        'stale_reasons', COALESCE(v_batch_freshness_json->'stale_reasons', '[]'::jsonb)
      ));
    ELSIF v_expected_freshness_hash IS NULL THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'OPERATION_FRESHNESS_RESULT_HASH_MISSING',
        'message', 'The payment operation does not carry a completed freshness result hash.',
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash
      ));
    ELSIF v_batch_freshness_hash IS DISTINCT FROM v_expected_freshness_hash THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'FRESHNESS_RESULT_HASH_MISMATCH',
        'message', 'The stored freshness result does not match the operation freshness result.',
        'expected_freshness_result_hash', v_expected_freshness_hash,
        'actual_freshness_result_hash', v_batch_freshness_hash
      ));
    ELSIF v_expected_freshness_scope_hash IS NOT NULL
       AND v_batch_freshness_scope_hash IS DISTINCT FROM v_expected_freshness_scope_hash THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'FRESHNESS_SCOPE_HASH_MISMATCH',
        'message', 'The stored freshness scope does not match the operation freshness scope.',
        'expected_freshness_scope_hash', v_expected_freshness_scope_hash,
        'actual_freshness_scope_hash', v_batch_freshness_scope_hash
      ));
    END IF;

    IF v_is_local_manual_mode AND v_projection_proof_valid IS NOT TRUE THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'LOCAL_MANUAL_SETTLEMENT_PROJECTION_PROOF_INVALID',
        'message', 'Local/manual settlement requires the complete server-owned frozen payment projection proof.',
        'execution_mode', v_execution_mode,
        'pay_channel_scope', v_pay_channel_scope,
        'projection_proof_source', v_operation_projection_proof_source,
        'server_owned_payment_projection_proof', v_operation_server_owned_projection_proof,
        'global_missing_count', v_operation_global_missing_count,
        'scoped_missing_count', v_operation_scoped_missing_count,
        'global_invalid_count', v_operation_global_invalid_count,
        'scoped_invalid_count', v_operation_scoped_invalid_count
      ));
    END IF;

    IF v_execution_mode = 'CSV_SETTLEMENT' AND v_csv_proof_valid IS NOT TRUE THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'CSV_SETTLEMENT_PROOF_INVALID',
        'message', 'The current CloudTMS Bank CSV proof, upload confirmation, or positive-route bank confirmation does not match the frozen authorised scope.',
        'requested_scope', v_pay_channel_scope,
        'stored_csv_scope', v_stored_csv_scope,
        'operation_csv_scope', v_operation_bank_csv_scope,
        'stored_csv_row_count', v_stored_csv_row_count,
        'operation_csv_row_count', v_operation_bank_csv_row_count,
        'expected_positive_row_count', v_operation_scoped_positive_count,
        'stored_csv_total_amount', v_stored_csv_total_amount,
        'operation_csv_total_amount', v_operation_bank_csv_total_amount,
        'expected_positive_total_amount', v_operation_scoped_positive_total,
        'csv_uploaded_confirmed', v_csv_uploaded_confirmed,
        'csv_bank_confirm_ref_present', v_csv_bank_confirm_ref IS NOT NULL,
        'manual_confirmation_mode', v_manual_confirmation_mode
      ));
    ELSIF v_execution_mode = 'EXTERNAL_SETTLEMENT' AND v_external_proof_valid IS NOT TRUE THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'EXTERNAL_SETTLEMENT_PROOF_INVALID',
        'message', 'External settlement requires the complete frozen projection and external/manual confirmation evidence.',
        'pay_channel_scope', v_pay_channel_scope,
        'payment_date_present', v_payment_date_text IS NOT NULL,
        'external_settlement_comment_present', v_external_settlement_comment IS NOT NULL,
        'manual_confirmation_mode', v_manual_confirmation_mode,
        'scoped_positive_bank_payment_count', v_operation_scoped_positive_count,
        'scoped_explicit_zero_count', v_operation_scoped_zero_count
      ));
    END IF;

    IF v_is_local_manual_mode AND v_provider_boundary_evidence_count > 0 THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'LOCAL_MANUAL_SETTLEMENT_PROVIDER_BOUNDARY_EVIDENCE',
        'message', 'Local/manual settlement cannot continue because provider or bank-dispatch evidence exists.',
        'execution_mode', v_execution_mode,
        'provider_scope_evidence_count', COALESCE(v_provider_scope_evidence_count, 0),
        'provider_attempt_row_count', COALESCE(v_provider_attempt_row_count, 0),
        'provider_event_count', COALESCE(v_provider_event_count, 0),
        'provider_transfer_evidence_count', COALESCE(v_provider_transfer_evidence_count, 0),
        'provider_submit_chunk_count', COALESCE(v_provider_submit_chunk_count, 0),
        'provider_boundary_evidence_count', COALESCE(v_provider_boundary_evidence_count, 0)
      ));
    END IF;

    IF v_local_manual_zero_only_scope IS NOT TRUE
       AND (
         COALESCE(v_scoped_operation_scope_count, 0) <= 0
         OR v_transfer_proof_hash IS NULL
         OR v_transfer_scope_hash IS NULL
       ) THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', 'OPERATION_TRANSFER_SCOPE_PROOF_MISSING',
        'message', 'A persisted prepared transfer proof and frozen operation transfer scope are required before prepare can complete.',
        'operation_id', p_operation_id::text,
        'execution_mode', v_execution_mode,
        'persisted_transfer_proof_hash', v_transfer_proof_hash,
        'current_transfer_scope_hash', v_transfer_scope_hash
      ));
    ELSIF v_local_manual_zero_only_scope IS NOT TRUE
       AND (
         COALESCE(v_scoped_scope_prepared_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
         OR COALESCE(v_scoped_scope_pending_count, 0) > 0
       OR COALESCE(v_scoped_scope_failed_count, 0) > 0
       OR COALESCE(v_scoped_scope_skipped_count, 0) > 0
       OR COALESCE(v_scoped_scope_without_transfer_count, 0) > 0
       OR COALESCE(v_authorisation_ready_transfer_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
       OR COALESCE(v_unsafe_transfer_count, 0) > 0
       OR (v_execution_mode = 'STANDARD_BANK' AND COALESCE(v_scope_provider_not_ready_count, 0) > 0)
       OR COALESCE(v_scope_prepared_hash_missing_count, 0) > 0
       OR COALESCE(v_scope_prepared_amount_mismatch_count, 0) > 0
       OR COALESCE(v_scope_item_rollup_pending_count, 0) > 0
       OR COALESCE(v_scope_item_transfer_link_mismatch_count, 0) > 0
       OR COALESCE(v_transfer_proof_hash_mismatch_count, 0) > 0
       OR COALESCE(v_scope_item_count_mismatch_count, 0) > 0
       OR COALESCE(v_scope_item_amount_mismatch_count, 0) > 0
       OR COALESCE(v_payout_instruction_missing_count, 0) > 0
       OR ROUND(COALESCE(v_scope_item_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(v_scope_amount_total, 0), 2)
       OR ROUND(COALESCE(v_transfer_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(v_scope_amount_total, 0), 2)
       ) THEN
      v_blockers_json := v_blockers_json || JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'code', CASE
          WHEN COALESCE(v_unsafe_transfer_count, 0) > 0 OR COALESCE(v_provider_boundary_evidence_count, 0) > 0
            THEN 'BANK_TRANSFER_PROVIDER_REVIEW_REQUIRED'
          ELSE 'OPERATION_TRANSFER_SCOPE_NOT_FULLY_PREPARED'
        END,
        'message', CASE
          WHEN v_is_local_manual_mode
            THEN 'Every local/manual settlement transfer scope must be prepared, proofed, item-linked, amount-matched, and free of provider evidence before authorisation can start.'
          ELSE 'Every scoped transfer group must be prepared, proofed, provider-ready, and linked to a safe bank transfer before authorisation can start.'
        END,
        'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
        'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
        'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
        'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
        'scope_provider_not_ready_count', COALESCE(v_scope_provider_not_ready_count, 0),
        'scope_prepared_hash_missing_count', COALESCE(v_scope_prepared_hash_missing_count, 0),
        'scope_prepared_amount_mismatch_count', COALESCE(v_scope_prepared_amount_mismatch_count, 0),
        'scope_item_rollup_pending_count', COALESCE(v_scope_item_rollup_pending_count, 0),
        'scope_item_transfer_link_mismatch_count', COALESCE(v_scope_item_transfer_link_mismatch_count, 0),
        'transfer_proof_hash_mismatch_count', COALESCE(v_transfer_proof_hash_mismatch_count, 0),
        'persisted_transfer_proof_hash', v_transfer_proof_hash,
        'current_transfer_scope_hash', v_transfer_scope_hash,
        'scope_item_count_mismatch_count', COALESCE(v_scope_item_count_mismatch_count, 0),
        'scope_item_amount_mismatch_count', COALESCE(v_scope_item_amount_mismatch_count, 0),
        'payout_instruction_missing_count', COALESCE(v_payout_instruction_missing_count, 0)
      ));
    END IF;

    v_blocker_count := JSONB_ARRAY_LENGTH(v_blockers_json);
    v_warning_count := JSONB_ARRAY_LENGTH(v_warnings_json);
    v_ready := v_blocker_count = 0;
    v_all_scoped_operation_scopes_authorisation_ready := v_ready;
    v_next_required_phase := CASE
      WHEN v_execution_commit_state <> 'NOT_SUBMITTED' THEN 'STOP_EXECUTION_BOUNDARY_CROSSED'
      WHEN EXISTS (
        SELECT 1
        FROM JSONB_ARRAY_ELEMENTS(v_blockers_json) AS blocker(elem)
        WHERE blocker.elem->>'code' IN (
          'BATCH_STALE', 'FRESHNESS_REQUIRED', 'FRESHNESS_REQUIRES_CHUNKED_VALIDATION',
          'FRESHNESS_RESULT_HASH_MISMATCH', 'OPERATION_FRESHNESS_RESULT_HASH_MISSING'
        )
      ) THEN 'VALIDATE_FRESHNESS'
      WHEN v_batch_status IN ('CANCELLED', 'CANCELED') THEN 'STOP_CANCELLED'
      WHEN v_blocker_count > 0 THEN 'RESOLVE_BLOCKERS'
      ELSE 'START_AUTHORISATION'
    END;

    v_summary_json := JSONB_BUILD_OBJECT(
      'mode', 'OPERATION_PROOF',
      'execution_mode', v_execution_mode,
      'candidate_count', COALESCE(v_candidate_count, 0),
      'item_count', COALESCE(v_item_count, 0),
      'transfer_count', COALESCE(v_scoped_operation_scope_count, 0),
      'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
      'provider_submission_required', v_execution_mode = 'STANDARD_BANK',
      'provider_submission_attempted', false,
      'submitted_to_bank', false,
      'local_settlement_evidence_only', v_is_local_manual_mode,
      'local_manual_zero_only_scope', v_local_manual_zero_only_scope,
      'mode_specific_proof_valid', v_mode_specific_proof_valid,
      'persisted_transfer_proof_hash', v_transfer_proof_hash,
      'prepared_transfer_proof_hash', v_transfer_scope_hash,
      'transfer_proof_hash_mismatch_count', COALESCE(v_transfer_proof_hash_mismatch_count, 0),
      'scope_amount_total', ROUND(COALESCE(v_scope_amount_total, 0), 2),
      'transfer_amount_total', ROUND(COALESCE(v_transfer_amount_total, 0), 2),
      'scope_item_amount_total', ROUND(COALESCE(v_scope_item_amount_total, 0), 2),
      'batch_item_amount_total', ROUND(COALESCE(v_batch_item_amount_total, 0), 2)
    );

    v_operation_prepare_proof_json := JSONB_BUILD_OBJECT(
      'checked_at_utc', v_now::text,
      'ready', v_ready,
      'blocker_count', v_blocker_count,
      'execution_mode', v_execution_mode,
      'provider_submission_required', v_execution_mode = 'STANDARD_BANK',
      'provider_submission_attempted', false,
      'submitted_to_bank', false,
      'local_settlement_evidence_only', v_is_local_manual_mode,
      'local_manual_zero_only_scope', v_local_manual_zero_only_scope,
      'mode_specific_proof_valid', v_mode_specific_proof_valid,
      'projection_proof_valid', v_projection_proof_valid,
      'csv_proof_valid', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_proof_valid ELSE NULL::boolean END,
      'external_proof_valid', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_proof_valid ELSE NULL::boolean END,
      'provider_boundary_evidence_count', CASE WHEN v_is_local_manual_mode THEN COALESCE(v_provider_boundary_evidence_count, 0) ELSE NULL::integer END,
      'persisted_transfer_proof_hash', v_transfer_proof_hash,
      'prepared_transfer_proof_hash', v_transfer_scope_hash,
      'transfer_proof_hash_mismatch_count', COALESCE(v_transfer_proof_hash_mismatch_count, 0),
      'freshness_result_hash', v_batch_freshness_hash,
      'freshness_scope_hash', v_batch_freshness_scope_hash,
      'pay_channel_scope', v_pay_channel_scope,
      'summary', v_summary_json
    );

    UPDATE public.banking_pay_operations AS operation_update
    SET progress_json = JSONB_STRIP_NULLS(
          COALESCE(operation_update.progress_json, '{}'::jsonb)
          || JSONB_BUILD_OBJECT(
            'pay_batch_prepare_proof', v_operation_prepare_proof_json,
            'execution_mode', v_execution_mode,
            'provider_submission_required', CASE WHEN v_is_local_manual_mode THEN false ELSE NULL::boolean END,
            'provider_submission_attempted', CASE WHEN v_is_local_manual_mode THEN false ELSE NULL::boolean END,
            'submitted_to_bank', CASE WHEN v_is_local_manual_mode THEN false ELSE NULL::boolean END,
            'manual_confirmation_mode', CASE WHEN v_is_local_manual_mode THEN v_manual_confirmation_mode ELSE NULL::text END,
            'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
            'csv_bank_confirm_ref', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref ELSE NULL::text END,
            'external_settlement_comment', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment ELSE NULL::text END
          )
        ),
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    RETURN JSONB_BUILD_OBJECT(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'ready', v_ready,
      'ready_flag', v_ready,
      'proof_mode', true,
      'used_operation_scope_proof', true,
      'pay_batch_prepare_skipped_full_classification', true,
      'blocker_count', v_blocker_count,
      'warning_count', v_warning_count,
      'blockers', v_blockers_json,
      'warnings', v_warnings_json,
      'next_required_phase', v_next_required_phase,
      'execution_summary', v_summary_json,
      'batch_status', v_batch_row.status,
      'execution_commit_state', v_execution_commit_state,
      'execution_mode', v_execution_mode,
      'pay_channel_scope', v_pay_channel_scope,
      'provider_submission_required', v_execution_mode = 'STANDARD_BANK',
      'provider_submission_attempted', false,
      'submitted_to_bank', false,
      'local_settlement_evidence_only', v_is_local_manual_mode,
      'mode_specific_proof_valid', v_mode_specific_proof_valid,
      'projection_proof_valid', v_projection_proof_valid,
      'csv_proof_valid', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_proof_valid ELSE NULL::boolean END,
      'external_proof_valid', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_proof_valid ELSE NULL::boolean END,
      'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
      'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
      'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
      'all_scoped_operation_scopes_authorisation_ready', COALESCE(v_all_scoped_operation_scopes_authorisation_ready, false),
      'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
      'provider_boundary_evidence_count', CASE WHEN v_is_local_manual_mode THEN COALESCE(v_provider_boundary_evidence_count, 0) ELSE 0 END,
      'transfer_proof_hash_mismatch_count', COALESCE(v_transfer_proof_hash_mismatch_count, 0),
      'freshness', JSONB_BUILD_OBJECT(
        'freshness_validation_status', v_batch_freshness_status,
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash,
        'expected_freshness_result_hash', v_expected_freshness_hash
      ),
      'prepared_transfer_proof', v_operation_prepare_proof_json,
      'server_utc', v_now
    ) || JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
      'manual_confirmation_mode', CASE WHEN v_is_local_manual_mode THEN v_manual_confirmation_mode ELSE NULL::text END,
      'csv_uploaded_confirmed', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_uploaded_confirmed ELSE NULL::boolean END,
      'csv_bank_confirm_ref', CASE WHEN v_execution_mode = 'CSV_SETTLEMENT' THEN v_csv_bank_confirm_ref ELSE NULL::text END,
      'external_settlement_comment', CASE WHEN v_execution_mode = 'EXTERNAL_SETTLEMENT' THEN v_external_settlement_comment ELSE NULL::text END,
      'payment_date', CASE WHEN v_is_local_manual_mode THEN v_payment_date_text ELSE NULL::text END
    ));
  END IF;

  IF p_operation_id IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_candidate_count
    FROM public.pay_batch_candidates AS proof_candidate
    WHERE proof_candidate.pay_batch_id = p_pay_batch_id;

    SELECT
      count(*)::integer,
      round(coalesce(sum(CASE WHEN COALESCE(proof_item.is_voided, false) = false AND proof_item.item_type <> 'DEBT_CREATED' THEN COALESCE(proof_item.amount_inc_vat, proof_item.amount_ex_vat, 0) ELSE 0 END), 0), 2)::numeric(14,2),
      count(*) FILTER (
        WHERE COALESCE(proof_item.is_voided, false) = false
          AND COALESCE(proof_item.amount_inc_vat, proof_item.amount_ex_vat, 0) <> 0
          AND proof_item.item_type <> 'DEBT_CREATED'
          AND NOT (
            UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = 'PAYE'
            AND UPPER(BTRIM(COALESCE(v_batch_row.batch_kind_fixed, ''))) <> 'LOANS'
          )
          AND proof_item.payout_instruction_snapshot_json IS NULL
      )::integer
    INTO v_item_count, v_batch_item_amount_total, v_payout_instruction_missing_count
    FROM public.pay_batch_items AS proof_item
    JOIN public.pay_batch_candidates AS proof_candidate
      ON proof_candidate.id = proof_item.pay_batch_candidate_id
    WHERE proof_candidate.pay_batch_id = p_pay_batch_id
      AND (
        v_pay_channel_scope IN ('ALL', 'ANY', '*')
        OR UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = v_pay_channel_scope
      );

    SELECT count(*)::integer
    INTO v_awaiting_net_count
    FROM public.pay_batch_candidates AS proof_candidate
    WHERE proof_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(proof_candidate.awaiting_net_amount, false) = true
      AND (
        v_pay_channel_scope IN ('ALL', 'ANY', '*')
        OR EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS proof_item
          WHERE proof_item.pay_batch_candidate_id = proof_candidate.id
            AND UPPER(BTRIM(COALESCE(proof_item.pay_channel, ''))) = v_pay_channel_scope
            AND COALESCE(proof_item.is_voided, false) = false
        )
      );

    SELECT
      count(*)::integer,
      count(*) FILTER (WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) = 'PREPARED')::integer,
      count(*) FILTER (WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) IN ('PENDING', 'QUEUED', 'RUNNING'))::integer,
      count(*) FILTER (WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) = 'FAILED')::integer,
      count(*) FILTER (WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) = 'SKIPPED')::integer,
      count(*) FILTER (WHERE operation_scope.pay_bank_transfer_id IS NULL OR linked_transfer.id IS NULL)::integer,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) = 'PREPARED'
          AND linked_transfer.id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(linked_transfer.status, ''))) = 'PENDING'
          AND linked_transfer.pay_batch_id = operation_scope.pay_batch_id
          AND linked_transfer.pay_channel = operation_scope.pay_channel
          AND linked_transfer.transfer_group_key = operation_scope.transfer_group_key
          AND ROUND(COALESCE(linked_transfer.amount, 0), 2) = ROUND(COALESCE(operation_scope.amount, 0), 2)
          AND UPPER(BTRIM(COALESCE(linked_transfer.currency, 'GBP'))) = UPPER(BTRIM(COALESCE(operation_scope.currency, 'GBP')))
          AND NULLIF(BTRIM(COALESCE(linked_transfer.rail_tx_id, '')), '') IS NULL
          AND linked_transfer.completed_at_utc IS NULL
          AND NULLIF(BTRIM(COALESCE(linked_transfer.failed_reason, '')), '') IS NULL
          AND UPPER(BTRIM(COALESCE(linked_transfer.rail_state, ''))) IN ('', 'LOCAL', 'PENDING')
          AND COALESCE(operation_scope.provider_submit_ready, false) = true
          AND UPPER(BTRIM(COALESCE(operation_scope.provider_submit_state, ''))) = 'READY'
          AND COALESCE(operation_scope.provider_review_required, false) = false
          AND NULLIF(BTRIM(COALESCE(operation_scope.provider_unsafe_reason, '')), '') IS NULL
          AND NULLIF(BTRIM(COALESCE(operation_scope.prepared_scope_hash, '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(operation_scope.prepared_result_hash, '')), '') IS NOT NULL
          AND ROUND(COALESCE(operation_scope.prepared_amount_total, 0), 2) = ROUND(COALESCE(operation_scope.amount, 0), 2)
          AND NOT EXISTS (
            SELECT 1
            FROM public.pay_bank_transfer_events AS transfer_event
            WHERE transfer_event.pay_batch_id = p_pay_batch_id
              AND (
                transfer_event.pay_bank_transfer_id = linked_transfer.id
                OR (
                  transfer_event.pay_bank_transfer_id IS NULL
                  AND (
                    NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = linked_transfer.id::text
                    OR NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = NULLIF(BTRIM(COALESCE(linked_transfer.rail_tx_id, '')), '')
                    OR NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = NULLIF(BTRIM(COALESCE(linked_transfer.request_id, '')), '')
                    OR NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = NULLIF(BTRIM(COALESCE(linked_transfer.payment_reference, '')), '')
                    OR NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), '') = NULLIF(BTRIM(COALESCE(linked_transfer.request_id, '')), '')
                    OR NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), '') = NULLIF(BTRIM(COALESCE(linked_transfer.rail_meta_json #>> '{idempotency_key}', '')), '')
                  )
                )
              )
          )
      )::integer,
      count(*) FILTER (
        WHERE linked_transfer.id IS NOT NULL
          AND (
            UPPER(BTRIM(COALESCE(linked_transfer.status, ''))) <> 'PENDING'
            OR linked_transfer.pay_batch_id IS DISTINCT FROM operation_scope.pay_batch_id
            OR linked_transfer.pay_channel IS DISTINCT FROM operation_scope.pay_channel
            OR linked_transfer.transfer_group_key IS DISTINCT FROM operation_scope.transfer_group_key
            OR ROUND(COALESCE(linked_transfer.amount, 0), 2) IS DISTINCT FROM ROUND(COALESCE(operation_scope.amount, 0), 2)
            OR UPPER(BTRIM(COALESCE(linked_transfer.currency, 'GBP'))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(operation_scope.currency, 'GBP')))
            OR NULLIF(BTRIM(COALESCE(linked_transfer.rail_tx_id, '')), '') IS NOT NULL
            OR linked_transfer.completed_at_utc IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(linked_transfer.failed_reason, '')), '') IS NOT NULL
            OR UPPER(BTRIM(COALESCE(linked_transfer.rail_state, ''))) NOT IN ('', 'LOCAL', 'PENDING')
            OR EXISTS (
              SELECT 1
              FROM public.pay_bank_transfer_events AS transfer_event
              WHERE transfer_event.pay_batch_id = p_pay_batch_id
                AND transfer_event.pay_bank_transfer_id = linked_transfer.id
            )
          )
      )::integer,
      count(*) FILTER (WHERE linked_transfer.id IS NOT NULL AND linked_transfer.pay_batch_id IS DISTINCT FROM operation_scope.pay_batch_id)::integer,
      count(*) FILTER (WHERE linked_transfer.id IS NOT NULL AND ROUND(COALESCE(linked_transfer.amount, 0), 2) IS DISTINCT FROM ROUND(COALESCE(operation_scope.amount, 0), 2))::integer,
      count(*) FILTER (WHERE linked_transfer.id IS NOT NULL AND UPPER(BTRIM(COALESCE(linked_transfer.currency, 'GBP'))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(operation_scope.currency, 'GBP'))))::integer,
      count(*) FILTER (WHERE linked_transfer.id IS NOT NULL AND UPPER(BTRIM(COALESCE(linked_transfer.status, ''))) <> 'PENDING')::integer,
      count(*) FILTER (
        WHERE linked_transfer.id IS NOT NULL
          AND (
            NULLIF(BTRIM(COALESCE(linked_transfer.rail_tx_id, '')), '') IS NOT NULL
            OR linked_transfer.completed_at_utc IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(linked_transfer.failed_reason, '')), '') IS NOT NULL
            OR UPPER(BTRIM(COALESCE(linked_transfer.rail_state, ''))) NOT IN ('', 'LOCAL', 'PENDING')
          )
      )::integer,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) = 'PREPARED'
          AND (
            COALESCE(operation_scope.provider_submit_ready, false) IS NOT TRUE
            OR UPPER(BTRIM(COALESCE(operation_scope.provider_submit_state, ''))) <> 'READY'
            OR COALESCE(operation_scope.provider_review_required, false) = true
            OR NULLIF(BTRIM(COALESCE(operation_scope.provider_unsafe_reason, '')), '') IS NOT NULL
          )
      )::integer,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) = 'PREPARED'
          AND (
            NULLIF(BTRIM(COALESCE(operation_scope.prepared_scope_hash, '')), '') IS NULL
            OR NULLIF(BTRIM(COALESCE(operation_scope.prepared_result_hash, '')), '') IS NULL
          )
      )::integer,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(operation_scope.status, ''))) = 'PREPARED'
          AND ROUND(COALESCE(operation_scope.prepared_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(operation_scope.amount, 0), 2)
      )::integer,
      round(COALESCE(sum(operation_scope.amount), 0), 2)::numeric(14,2),
      round(COALESCE(sum(linked_transfer.amount), 0), 2)::numeric(14,2),
      md5(COALESCE(string_agg(
        operation_scope.id::text || ':' ||
        COALESCE(operation_scope.pay_bank_transfer_id::text, '') || ':' ||
        COALESCE(operation_scope.status, '') || ':' ||
        ROUND(COALESCE(operation_scope.amount, 0), 2)::text || ':' ||
        COALESCE(operation_scope.prepared_scope_hash, '') || ':' ||
        COALESCE(operation_scope.prepared_result_hash, ''),
        '|' ORDER BY operation_scope.id
      ), 'NO_SCOPE')) AS transfer_scope_hash
    INTO
      v_scoped_operation_scope_count,
      v_scoped_scope_prepared_count,
      v_scoped_scope_pending_count,
      v_scoped_scope_failed_count,
      v_scoped_scope_skipped_count,
      v_scoped_scope_without_transfer_count,
      v_authorisation_ready_transfer_count,
      v_unsafe_transfer_count,
      v_transfer_identity_mismatch_count,
      v_transfer_amount_mismatch_count,
      v_transfer_currency_mismatch_count,
      v_transfer_status_not_pending_count,
      v_transfer_external_state_count,
      v_scope_provider_not_ready_count,
      v_scope_prepared_hash_missing_count,
      v_scope_prepared_amount_mismatch_count,
      v_scope_amount_total,
      v_transfer_amount_total,
      v_transfer_scope_hash
    FROM public.banking_pay_operation_transfer_scope AS operation_scope
    LEFT JOIN public.pay_bank_transfers AS linked_transfer
      ON linked_transfer.id = operation_scope.pay_bank_transfer_id
     AND linked_transfer.pay_batch_id = operation_scope.pay_batch_id
    WHERE operation_scope.pay_batch_id = p_pay_batch_id
      AND operation_scope.operation_id = p_operation_id
      AND (
        v_pay_channel_scope IN ('ALL', 'ANY', '*')
        OR UPPER(BTRIM(COALESCE(operation_scope.pay_channel, ''))) = v_pay_channel_scope
      );

    SELECT
      count(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_item.rollup_status, ''))) NOT IN ('ROLLED_UP', 'ROLLEDUP', 'READY', 'PREPARED', 'COMPLETE', 'COMPLETED'))::integer,
      round(COALESCE(sum(scope_item.item_amount), 0), 2)::numeric(14,2)
    INTO v_scope_item_rollup_pending_count, v_scope_item_amount_total
    FROM public.banking_pay_operation_transfer_scope_items AS scope_item
    WHERE scope_item.pay_batch_id = p_pay_batch_id
      AND scope_item.operation_id = p_operation_id
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_transfer_scope AS operation_scope
        WHERE operation_scope.id = scope_item.transfer_scope_id
          AND operation_scope.pay_batch_id = scope_item.pay_batch_id
          AND operation_scope.operation_id = scope_item.operation_id
          AND (
            v_pay_channel_scope IN ('ALL', 'ANY', '*')
            OR UPPER(BTRIM(COALESCE(operation_scope.pay_channel, ''))) = v_pay_channel_scope
          )
      );

    IF v_execution_commit_state <> 'NOT_SUBMITTED'
       OR NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
       OR v_batch_row.execution_committed_at_utc IS NOT NULL THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'EXECUTION_STATE_CONFLICT',
        'message', 'This payment batch has already crossed the execution boundary and cannot be prepared again.',
        'execution_commit_state', v_execution_commit_state,
        'execution_commit_ref', v_batch_row.execution_commit_ref,
        'execution_committed_at_utc', CASE WHEN v_batch_row.execution_committed_at_utc IS NULL THEN NULL ELSE v_batch_row.execution_committed_at_utc::text END
      ));
    END IF;

    IF v_batch_status IN ('CANCELLED', 'CANCELED') THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'BATCH_CANCELLED',
        'message', 'The payment batch has been cancelled.'
      ));
    ELSIF v_batch_status <> 'DRAFT' THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'BATCH_STATUS_NOT_DRAFT',
        'message', 'Only a DRAFT batch can be prepared for payment execution.',
        'batch_status', v_batch_row.status
      ));
    END IF;

    IF v_candidate_count <= 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'NO_CANDIDATES',
        'message', 'The payment batch has no candidates.'
      ));
    END IF;

    IF v_item_count <= 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'NO_PAYMENT_ITEMS',
        'message', 'The payment batch has no payment items.'
      ));
    END IF;

    IF v_awaiting_net_count > 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'PAYE_NET_REQUIRED',
        'message', 'PAYE net amounts are required before this batch can proceed.',
        'candidate_count', v_awaiting_net_count
      ));
    END IF;

    IF v_batch_freshness_status <> 'PASSED'
       OR v_batch_freshness_hash IS NULL
       OR COALESCE((v_batch_freshness_json->>'is_stale')::boolean, false) IS TRUE THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', CASE WHEN v_batch_freshness_status = 'STALE' THEN 'BATCH_STALE' ELSE 'FRESHNESS_REQUIRED' END,
        'message', 'Chunked freshness must pass before this payment batch can be prepared.',
        'freshness_validation_status', NULLIF(v_batch_freshness_status, ''),
        'freshness_operation_id', CASE WHEN v_batch_row.freshness_operation_id IS NULL THEN NULL ELSE v_batch_row.freshness_operation_id::text END,
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash,
        'stale_reasons', COALESCE(v_batch_freshness_json->'stale_reasons', '[]'::jsonb),
        'diff_sample', COALESCE(v_batch_freshness_json->'diff_sample', COALESCE(v_batch_freshness_json->'diff', '[]'::jsonb))
      ));
    ELSIF v_expected_freshness_hash IS NULL THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'OPERATION_FRESHNESS_RESULT_HASH_MISSING',
        'message', 'The payment operation does not carry a completed freshness result hash.',
        'freshness_validation_status', NULLIF(v_batch_freshness_status, ''),
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash
      ));
    ELSIF v_batch_freshness_hash IS DISTINCT FROM v_expected_freshness_hash THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'FRESHNESS_RESULT_HASH_MISMATCH',
        'message', 'The stored freshness result does not match the operation freshness result.',
        'expected_freshness_result_hash', v_expected_freshness_hash,
        'actual_freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash
      ));
    ELSIF v_expected_freshness_scope_hash IS NOT NULL AND v_batch_freshness_scope_hash IS DISTINCT FROM v_expected_freshness_scope_hash THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'FRESHNESS_SCOPE_HASH_MISMATCH',
        'message', 'The stored freshness scope does not match the operation freshness scope.',
        'expected_freshness_scope_hash', v_expected_freshness_scope_hash,
        'actual_freshness_scope_hash', v_batch_freshness_scope_hash,
        'freshness_result_hash', v_batch_freshness_hash
      ));
    END IF;

    IF COALESCE(v_scoped_operation_scope_count, 0) <= 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'OPERATION_TRANSFER_SCOPE_NOT_PREPARED',
        'message', 'No operation-scoped bank transfer groups are prepared for this payment execution attempt.',
        'operation_id', p_operation_id::text,
        'pay_channel_scope', v_pay_channel_scope
      ));
    ELSIF COALESCE(v_scoped_scope_prepared_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
       OR COALESCE(v_scoped_scope_pending_count, 0) > 0
       OR COALESCE(v_scoped_scope_failed_count, 0) > 0
       OR COALESCE(v_scoped_scope_skipped_count, 0) > 0
       OR COALESCE(v_scoped_scope_without_transfer_count, 0) > 0
       OR COALESCE(v_authorisation_ready_transfer_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
       OR COALESCE(v_unsafe_transfer_count, 0) > 0
       OR COALESCE(v_scope_provider_not_ready_count, 0) > 0
       OR COALESCE(v_scope_prepared_hash_missing_count, 0) > 0
       OR COALESCE(v_scope_prepared_amount_mismatch_count, 0) > 0
       OR COALESCE(v_scope_item_rollup_pending_count, 0) > 0
       OR ROUND(COALESCE(v_scope_item_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(v_scope_amount_total, 0), 2)
       OR ROUND(COALESCE(v_scope_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(v_batch_item_amount_total, 0), 2)
       OR ROUND(COALESCE(v_scope_item_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(v_batch_item_amount_total, 0), 2)
       OR ROUND(COALESCE(v_transfer_amount_total, 0), 2) IS DISTINCT FROM ROUND(COALESCE(v_scope_amount_total, 0), 2) THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', CASE WHEN COALESCE(v_unsafe_transfer_count, 0) > 0 THEN 'BANK_TRANSFER_PROVIDER_REVIEW_REQUIRED' ELSE 'OPERATION_TRANSFER_SCOPE_NOT_FULLY_PREPARED' END,
        'message', 'Every scoped transfer group must be prepared, proofed, local-only, and linked to a safe bank transfer before authorisation can start.',
        'operation_id', p_operation_id::text,
        'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
        'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
        'scoped_scope_pending_count', COALESCE(v_scoped_scope_pending_count, 0),
        'scoped_scope_failed_count', COALESCE(v_scoped_scope_failed_count, 0),
        'scoped_scope_skipped_count', COALESCE(v_scoped_scope_skipped_count, 0),
        'scoped_scope_without_transfer_count', COALESCE(v_scoped_scope_without_transfer_count, 0),
        'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
        'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
        'provider_not_ready_count', COALESCE(v_scope_provider_not_ready_count, 0),
        'prepared_hash_missing_count', COALESCE(v_scope_prepared_hash_missing_count, 0),
        'prepared_amount_mismatch_count', COALESCE(v_scope_prepared_amount_mismatch_count, 0),
        'scope_item_rollup_pending_count', COALESCE(v_scope_item_rollup_pending_count, 0),
        'scope_amount_total', ROUND(COALESCE(v_scope_amount_total, 0), 2),
        'transfer_amount_total', ROUND(COALESCE(v_transfer_amount_total, 0), 2),
        'scope_item_amount_total', ROUND(COALESCE(v_scope_item_amount_total, 0), 2),
        'batch_item_amount_total', ROUND(COALESCE(v_batch_item_amount_total, 0), 2),
        'pay_channel_scope', v_pay_channel_scope
      ));
    END IF;

    IF COALESCE(v_payout_instruction_missing_count, 0) > 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'FROZEN_PAYOUT_INSTRUCTION_MISSING',
        'message', 'Every payable frozen batch item must have a payout instruction snapshot before authorisation can start.',
        'missing_count', COALESCE(v_payout_instruction_missing_count, 0)
      ));
    END IF;

    v_blocker_count := jsonb_array_length(v_blockers_json);
    v_warning_count := jsonb_array_length(v_warnings_json);
    v_ready := v_blocker_count = 0;
    v_all_scoped_operation_scopes_authorisation_ready := (
      COALESCE(v_scoped_operation_scope_count, 0) > 0
      AND COALESCE(v_scoped_scope_prepared_count, 0) = COALESCE(v_scoped_operation_scope_count, 0)
      AND COALESCE(v_authorisation_ready_transfer_count, 0) = COALESCE(v_scoped_operation_scope_count, 0)
      AND COALESCE(v_unsafe_transfer_count, 0) = 0
    );

    v_next_required_phase := CASE
      WHEN v_execution_commit_state <> 'NOT_SUBMITTED' THEN 'STOP_EXECUTION_BOUNDARY_CROSSED'
      WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(v_blockers_json) AS blocker(elem) WHERE blocker.elem->>'code' IN ('BATCH_STALE','FRESHNESS_REQUIRED','FRESHNESS_REQUIRES_CHUNKED_VALIDATION','FRESHNESS_RESULT_HASH_MISMATCH','OPERATION_FRESHNESS_RESULT_HASH_MISSING')) THEN 'VALIDATE_FRESHNESS'
      WHEN v_batch_status IN ('CANCELLED', 'CANCELED') THEN 'STOP_CANCELLED'
      WHEN v_blocker_count > 0 THEN 'RESOLVE_BLOCKERS'
      ELSE 'START_AUTHORISATION'
    END;

    v_summary_json := jsonb_build_object(
      'mode', 'OPERATION_PROOF',
      'candidate_count', COALESCE(v_candidate_count, 0),
      'item_count', COALESCE(v_item_count, 0),
      'transfer_count', COALESCE(v_scoped_operation_scope_count, 0),
      'pending_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
      'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
      'provider_attempt_or_evidence_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
      'provider_or_ambiguous_evidence_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
      'blocked_transfer_count', COALESCE(v_scoped_failed_or_blocked_transfer_count, 0),
      'prepared_transfer_proof_hash', v_transfer_scope_hash,
      'scope_amount_total', ROUND(COALESCE(v_scope_amount_total, 0), 2),
      'transfer_amount_total', ROUND(COALESCE(v_transfer_amount_total, 0), 2),
      'scope_item_amount_total', ROUND(COALESCE(v_scope_item_amount_total, 0), 2),
      'batch_item_amount_total', ROUND(COALESCE(v_batch_item_amount_total, 0), 2)
    );

    UPDATE public.banking_pay_operations AS operation_update
    SET progress_json = jsonb_strip_nulls(
          COALESCE(operation_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'pay_batch_prepare_proof', jsonb_build_object(
              'checked_at_utc', v_now::text,
              'ready', v_ready,
              'blocker_count', v_blocker_count,
              'prepared_transfer_proof_hash', v_transfer_scope_hash,
              'scope_amount_total', ROUND(COALESCE(v_scope_amount_total, 0), 2),
              'transfer_amount_total', ROUND(COALESCE(v_transfer_amount_total, 0), 2),
              'scope_item_amount_total', ROUND(COALESCE(v_scope_item_amount_total, 0), 2),
              'batch_item_amount_total', ROUND(COALESCE(v_batch_item_amount_total, 0), 2),
              'freshness_result_hash', v_batch_freshness_hash,
              'freshness_scope_hash', v_batch_freshness_scope_hash,
              'pay_channel_scope', v_pay_channel_scope
            )
          )
        ),
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    RETURN jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'ready', v_ready,
      'ready_flag', v_ready,
      'proof_mode', true,
      'used_operation_scope_proof', true,
      'pay_batch_prepare_skipped_full_classification', true,
      'blocker_count', v_blocker_count,
      'warning_count', v_warning_count,
      'blockers', v_blockers_json,
      'warnings', v_warnings_json,
      'next_required_phase', v_next_required_phase,
      'execution_summary', v_summary_json,
      'batch_status', v_batch_row.status,
      'execution_commit_state', v_execution_commit_state,
      'execution_mode', v_execution_mode,
      'pay_channel_scope', v_pay_channel_scope,
      'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
      'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
      'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
      'scoped_scope_pending_count', COALESCE(v_scoped_scope_pending_count, 0),
      'scoped_scope_failed_count', COALESCE(v_scoped_scope_failed_count, 0),
      'scoped_scope_skipped_count', COALESCE(v_scoped_scope_skipped_count, 0),
      'scoped_scope_without_transfer_count', COALESCE(v_scoped_scope_without_transfer_count, 0),
      'all_scoped_operation_scopes_authorisation_ready', COALESCE(v_all_scoped_operation_scopes_authorisation_ready, false),
      'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
      'freshness', jsonb_build_object(
        'freshness_validation_status', v_batch_freshness_status,
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash,
        'freshness_operation_id', CASE WHEN v_batch_row.freshness_operation_id IS NULL THEN NULL ELSE v_batch_row.freshness_operation_id::text END,
        'expected_freshness_result_hash', v_expected_freshness_hash,
        'freshness_result_json', v_batch_freshness_json
      ),
      'prepared_transfer_proof', jsonb_build_object(
        'prepared_transfer_proof_hash', v_transfer_scope_hash,
        'scope_amount_total', ROUND(COALESCE(v_scope_amount_total, 0), 2),
        'transfer_amount_total', ROUND(COALESCE(v_transfer_amount_total, 0), 2),
        'scope_item_amount_total', ROUND(COALESCE(v_scope_item_amount_total, 0), 2),
        'batch_item_amount_total', ROUND(COALESCE(v_batch_item_amount_total, 0), 2),
        'payout_instruction_missing_count', COALESCE(v_payout_instruction_missing_count, 0)
      ),
      'server_utc', v_now
    );
  END IF;

  v_summary_json := public.pay_batch_execution_summary_get(
    p_pay_batch_id => p_pay_batch_id,
    p_actor_user_id => p_actor_user_id
  );

  v_execution_mode_raw := UPPER(BTRIM(COALESCE(
    v_summary_json->>'execution_mode',
    v_batch_row.execution_intent_json->>'execution_mode',
    v_batch_row.execution_intent_json->>'mode',
    'STANDARD_BANK'
  )));
  v_execution_mode := CASE
    WHEN v_execution_mode_raw IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
    WHEN v_execution_mode_raw IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
    WHEN v_execution_mode_raw IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
    ELSE 'STANDARD_BANK'
  END;
  v_is_local_manual_mode := v_execution_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT');

  v_candidate_count := coalesce(NULLIF(v_summary_json->>'candidate_count', '')::integer, 0);
  v_item_count := coalesce(NULLIF(v_summary_json->>'item_count', '')::integer, 0);
  v_transfer_count := coalesce(NULLIF(v_summary_json->>'transfer_count', '')::integer, 0);
  v_pending_transfer_count := coalesce(NULLIF(v_summary_json->>'pending_transfer_count', '')::integer, 0);
  v_authorisation_ready_transfer_count := coalesce(NULLIF(v_summary_json->>'authorisation_ready_transfer_count', '')::integer, 0);
  v_unattempted_submit_eligible_transfer_count := coalesce(NULLIF(v_summary_json->>'unattempted_submit_eligible_transfer_count', '')::integer, 0);
  v_remaining_unattempted_submit_required := coalesce(NULLIF(v_summary_json->>'remaining_unattempted_submit_required', '')::integer, v_unattempted_submit_eligible_transfer_count);
  v_provider_attempt_or_evidence_transfer_count := coalesce(NULLIF(v_summary_json->>'provider_attempt_or_evidence_transfer_count', '')::integer, 0);
  v_provider_or_ambiguous_evidence_transfer_count := coalesce(NULLIF(v_summary_json->>'provider_or_ambiguous_evidence_transfer_count', '')::integer, 0);
  v_local_only_transfer_count := coalesce(NULLIF(v_summary_json->>'local_only_transfer_count', '')::integer, 0);
  v_canonical_pending_status_transfer_count := coalesce(NULLIF(v_summary_json->>'canonical_pending_status_transfer_count', '')::integer, 0);
  v_safe_local_cleanup_transfer_count := coalesce(NULLIF(v_summary_json->>'safe_local_cleanup_transfer_count', '')::integer, 0);
  SELECT
    count(*) FILTER (WHERE classified_transfer.is_authorisation_ready)::integer,
    count(*) FILTER (WHERE classified_transfer.is_unattempted_submit_eligible)::integer,
    count(*) FILTER (WHERE classified_transfer.has_provider_submission_evidence OR classified_transfer.has_provider_event_evidence OR classified_transfer.has_provider_attempt_without_external_id OR classified_transfer.has_operation_submit_attempt)::integer,
    count(*) FILTER (WHERE classified_transfer.has_provider_submission_evidence OR classified_transfer.has_provider_event_evidence OR classified_transfer.has_provider_attempt_without_external_id OR classified_transfer.has_operation_submit_attempt OR classified_transfer.has_ambiguous_external_evidence)::integer,
    count(*) FILTER (WHERE classified_transfer.status_upper = 'PENDING')::integer,
    count(*) FILTER (
      WHERE classified_transfer.pay_bank_transfer_id IS NOT NULL
        AND classified_transfer.is_authorisation_ready IS NOT TRUE
        AND (
          classified_transfer.has_provider_submission_evidence
          OR classified_transfer.has_provider_event_evidence
          OR classified_transfer.has_provider_attempt_without_external_id
          OR classified_transfer.has_operation_submit_attempt
          OR classified_transfer.has_ambiguous_external_evidence
          OR classified_transfer.is_failed_or_blocked
          OR classified_transfer.is_terminal_or_completed
          OR classified_transfer.has_different_operation_scope
          OR classified_transfer.has_stale_auth_request_evidence
          OR classified_transfer.has_non_cancellable_auth_request
          OR NULLIF(BTRIM(COALESCE(classified_transfer.unsafe_reason, '')), '') IS NOT NULL
        )
    )::integer,
    count(*) FILTER (WHERE classified_transfer.is_failed_or_blocked IS TRUE)::integer,
    count(*) FILTER (
      WHERE classified_transfer.status_upper = 'BLOCKED'
         OR classified_transfer.rail_state_upper = 'BLOCKED'
    )::integer,
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
       v_unattempted_submit_eligible_transfer_count,
       v_provider_attempt_or_evidence_transfer_count,
       v_provider_or_ambiguous_evidence_transfer_count,
       v_canonical_pending_status_transfer_count,
       v_unsafe_transfer_count,
       v_scoped_failed_or_blocked_transfer_count,
       v_scoped_blocked_transfer_count,
       v_non_cancellable_auth_request_count,
       v_auth_request_retry_blocker_count
  FROM public.pay_bank_transfer_execution_classify(
    p_pay_batch_id => p_pay_batch_id,
    p_pay_channel_scope => v_pay_channel_scope,
    p_operation_id => p_operation_id,
    p_include_unscoped_transfers => CASE WHEN p_operation_id IS NULL THEN true ELSE false END,
    p_action_context => 'PREPARE_ACTION'
  ) AS classified_transfer
  WHERE (
    p_operation_id IS NULL
    OR EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_transfer_scope AS scoped_operation_filter
      WHERE scoped_operation_filter.pay_batch_id = p_pay_batch_id
        AND scoped_operation_filter.operation_id = p_operation_id
        AND (
          v_pay_channel_scope IN ('ALL', 'ANY', '*')
          OR upper(btrim(coalesce(scoped_operation_filter.pay_channel, ''))) = v_pay_channel_scope
        )
        AND (
          scoped_operation_filter.id = classified_transfer.scope_id
          OR (
            classified_transfer.pay_bank_transfer_id IS NOT NULL
            AND scoped_operation_filter.pay_bank_transfer_id = classified_transfer.pay_bank_transfer_id
          )
        )
    )
  );

  IF p_operation_id IS NOT NULL THEN
    SELECT count(*)::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'PREPARED')::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'PENDING')::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'FAILED')::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'SKIPPED')::integer,
           count(*) FILTER (WHERE operation_scope.pay_bank_transfer_id IS NULL)::integer
    INTO v_scoped_operation_scope_count,
         v_scoped_scope_prepared_count,
         v_scoped_scope_pending_count,
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

    v_all_scoped_operation_scopes_authorisation_ready := (
      COALESCE(v_scoped_operation_scope_count, 0) > 0
      AND COALESCE(v_scoped_scope_prepared_count, 0) = COALESCE(v_scoped_operation_scope_count, 0)
      AND COALESCE(v_scoped_scope_pending_count, 0) = 0
      AND COALESCE(v_scoped_scope_failed_count, 0) = 0
      AND COALESCE(v_scoped_scope_skipped_count, 0) = 0
      AND COALESCE(v_scoped_scope_without_transfer_count, 0) = 0
      AND COALESCE(v_authorisation_ready_transfer_count, 0) = COALESCE(v_scoped_operation_scope_count, 0)
      AND COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0) = 0
      AND COALESCE(v_non_cancellable_auth_request_count, 0) = 0
    );
  END IF;
  v_remaining_unattempted_submit_required := COALESCE(v_unattempted_submit_eligible_transfer_count, 0);
  v_failed_transfer_count := coalesce(NULLIF(v_summary_json->>'failed_transfer_count', '')::integer, 0);
  v_blocked_transfer_count := coalesce(NULLIF(v_summary_json->>'blocked_transfer_count', '')::integer, 0);
  v_submitted_transfer_count := coalesce(NULLIF(v_summary_json->>'provider_submitted_transfer_count', '')::integer, coalesce(NULLIF(v_summary_json->>'submitted_transfer_count', '')::integer, 0));

  v_batch_freshness_status := upper(btrim(coalesce(v_batch_row.freshness_validation_status, '')));
  v_batch_freshness_hash := nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '');
  v_batch_freshness_scope_hash := nullif(btrim(coalesce(v_batch_row.freshness_scope_hash, '')), '');
  v_batch_freshness_json := coalesce(v_batch_row.freshness_result_json, '{}'::jsonb);

  IF v_execution_commit_state <> 'NOT_SUBMITTED'
     OR nullif(btrim(coalesce(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
     OR v_batch_row.execution_committed_at_utc IS NOT NULL THEN
    v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
      'code', 'EXECUTION_STATE_CONFLICT',
      'message', 'This payment batch has already crossed the execution boundary and cannot be prepared again.',
      'execution_commit_state', v_execution_commit_state,
      'execution_commit_ref', v_batch_row.execution_commit_ref,
      'execution_committed_at_utc', CASE WHEN v_batch_row.execution_committed_at_utc IS NULL THEN NULL ELSE v_batch_row.execution_committed_at_utc::text END
    ));
  ELSIF v_operation_mode THEN
    IF v_batch_freshness_status <> 'PASSED' THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', CASE WHEN v_batch_freshness_status = 'STALE' THEN 'BATCH_STALE' ELSE 'FRESHNESS_REQUIRED' END,
        'message', 'Chunked freshness must pass before this payment batch can be prepared.',
        'freshness_validation_status', nullif(v_batch_freshness_status, ''),
        'freshness_operation_id', CASE WHEN v_batch_row.freshness_operation_id IS NULL THEN NULL ELSE v_batch_row.freshness_operation_id::text END,
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash,
        'stale_reasons', coalesce(v_batch_freshness_json->'stale_reasons', '[]'::jsonb),
        'diff_sample', coalesce(v_batch_freshness_json->'diff_sample', '[]'::jsonb)
      ));
    ELSIF p_operation_id IS NOT NULL AND v_expected_freshness_hash IS NULL THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'OPERATION_FRESHNESS_RESULT_HASH_MISSING',
        'message', 'The payment operation does not carry a completed freshness result hash.',
        'freshness_validation_status', nullif(v_batch_freshness_status, ''),
        'freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash
      ));
    ELSIF v_expected_freshness_hash IS NOT NULL AND v_batch_freshness_hash IS DISTINCT FROM v_expected_freshness_hash THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'FRESHNESS_RESULT_HASH_MISMATCH',
        'message', 'The stored freshness result does not match the operation freshness result.',
        'expected_freshness_result_hash', v_expected_freshness_hash,
        'actual_freshness_result_hash', v_batch_freshness_hash,
        'freshness_scope_hash', v_batch_freshness_scope_hash
      ));
    ELSIF v_expected_freshness_scope_hash IS NOT NULL AND v_batch_freshness_scope_hash IS DISTINCT FROM v_expected_freshness_scope_hash THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'FRESHNESS_SCOPE_HASH_MISMATCH',
        'message', 'The stored freshness scope does not match the operation freshness scope.',
        'expected_freshness_scope_hash', v_expected_freshness_scope_hash,
        'actual_freshness_scope_hash', v_batch_freshness_scope_hash,
        'freshness_result_hash', v_batch_freshness_hash
      ));
    END IF;
  ELSE
    v_large_batch := coalesce(v_candidate_count, 0) > 100 OR coalesce(v_item_count, 0) > 250 OR coalesce(v_transfer_count, 0) > 100;

    IF v_large_batch THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'FRESHNESS_REQUIRES_CHUNKED_VALIDATION',
        'message', 'This batch must use chunked freshness validation before payment can continue.',
        'candidate_count', v_candidate_count,
        'item_count', v_item_count,
        'transfer_count', v_transfer_count
      ));
    ELSE
      v_fresh_json := public.pay_batch_validate_freshness(
        p_pay_batch_id => p_pay_batch_id,
        p_actor_user_id => p_actor_user_id,
        p_allow_large_full_scan => false
      );
      v_is_stale := coalesce((v_fresh_json->>'is_stale')::boolean, false);

      IF v_is_stale THEN
        SELECT coalesce(jsonb_agg(diff_row.elem), '[]'::jsonb)
        INTO v_diff_sample
        FROM (
          SELECT diff_elem.elem
          FROM jsonb_array_elements(coalesce(v_fresh_json->'diff', '[]'::jsonb)) AS diff_elem(elem)
          LIMIT 20
        ) AS diff_row;

        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', CASE WHEN coalesce((v_fresh_json->>'requires_chunked_freshness')::boolean, false) THEN 'FRESHNESS_REQUIRES_CHUNKED_VALIDATION' ELSE 'BATCH_STALE' END,
          'message', CASE WHEN coalesce((v_fresh_json->>'requires_chunked_freshness')::boolean, false) THEN 'This batch must use chunked freshness validation before payment can continue.' ELSE 'The payment batch is no longer up to date and must be regenerated before payment can continue.' END,
          'stale_reasons', coalesce(v_fresh_json->'stale_reasons', '[]'::jsonb),
          'diff_sample', v_diff_sample
        ));
      END IF;
    END IF;
  END IF;

  IF v_batch_status IN ('CANCELLED', 'CANCELED') THEN
    v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
      'code', 'BATCH_CANCELLED',
      'message', 'The payment batch has been cancelled.'
    ));
  END IF;

  IF v_candidate_count <= 0 THEN
    v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
      'code', 'NO_CANDIDATES',
      'message', 'The payment batch has no candidates.'
    ));
  END IF;

  IF v_item_count <= 0 THEN
    v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
      'code', 'NO_PAYMENT_ITEMS',
      'message', 'The payment batch has no payment items.'
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_awaiting_net_count
  FROM public.pay_batch_candidates AS batch_candidate
  WHERE batch_candidate.pay_batch_id = p_pay_batch_id
    AND coalesce(batch_candidate.awaiting_net_amount, false) = true;

  IF v_awaiting_net_count > 0 THEN
    v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
      'code', 'PAYE_NET_REQUIRED',
      'message', 'PAYE net amounts are required before this batch can proceed.',
      'candidate_count', v_awaiting_net_count
    ));
  END IF;

  IF v_execution_mode = 'STANDARD_BANK' THEN
    IF v_transfer_count <= 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'BANK_TRANSFERS_NOT_PREPARED',
        'message', 'Bank transfer groups must be prepared before payment authorisation can start.'
      ));
    END IF;

    IF p_operation_id IS NULL THEN
      IF v_blocked_transfer_count > 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'BLOCKED_TRANSFERS',
          'message', 'One or more bank transfers are blocked and must be corrected before submission.',
          'blocked_transfer_count', v_blocked_transfer_count
        ));
      END IF;

      IF v_failed_transfer_count > 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'FAILED_TRANSFERS',
          'message', 'One or more bank transfers are failed and must be corrected before submission.',
          'failed_transfer_count', v_failed_transfer_count
        ));
      END IF;
    ELSE
      IF COALESCE(v_scoped_blocked_transfer_count, 0) > 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'BLOCKED_TRANSFERS',
          'message', 'One or more scoped bank transfers are blocked and must be corrected before submission.',
          'operation_id', p_operation_id::text,
          'blocked_transfer_count', COALESCE(v_scoped_blocked_transfer_count, 0),
          'pay_channel_scope', v_pay_channel_scope
        ));
      END IF;

      IF COALESCE(v_scoped_failed_or_blocked_transfer_count, 0) > 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'FAILED_TRANSFERS',
          'message', 'One or more scoped bank transfers are failed or blocked and must be corrected before submission.',
          'operation_id', p_operation_id::text,
          'failed_transfer_count', COALESCE(v_scoped_failed_or_blocked_transfer_count, 0),
          'pay_channel_scope', v_pay_channel_scope
        ));
      END IF;
    END IF;

    IF p_operation_id IS NOT NULL THEN
      IF COALESCE(v_scoped_operation_scope_count, 0) <= 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'OPERATION_TRANSFER_SCOPE_NOT_PREPARED',
          'message', 'No operation-scoped bank transfer groups are prepared for this payment execution attempt.',
          'operation_id', p_operation_id::text,
          'pay_channel_scope', v_pay_channel_scope
        ));
      END IF;

      IF COALESCE(v_scoped_scope_prepared_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
         OR COALESCE(v_scoped_scope_pending_count, 0) > 0
         OR COALESCE(v_scoped_scope_failed_count, 0) > 0
         OR COALESCE(v_scoped_scope_skipped_count, 0) > 0
         OR COALESCE(v_scoped_scope_without_transfer_count, 0) > 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'OPERATION_TRANSFER_SCOPE_NOT_FULLY_PREPARED',
          'message', 'Every scoped transfer group must be prepared and linked to a safe local bank transfer before authorisation can start.',
          'operation_id', p_operation_id::text,
          'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
          'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
          'scoped_scope_pending_count', COALESCE(v_scoped_scope_pending_count, 0),
          'scoped_scope_failed_count', COALESCE(v_scoped_scope_failed_count, 0),
          'scoped_scope_skipped_count', COALESCE(v_scoped_scope_skipped_count, 0),
          'scoped_scope_without_transfer_count', COALESCE(v_scoped_scope_without_transfer_count, 0),
          'pay_channel_scope', v_pay_channel_scope
        ));
      END IF;

      IF COALESCE(v_non_cancellable_auth_request_count, 0) > 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'AUTH_REQUEST_HELD_BY_PREVIOUS_OPERATION',
          'message', 'An active authorisation request is not safe to reuse or cancel automatically for this payment execution attempt.',
          'operation_id', p_operation_id::text,
          'non_cancellable_auth_request_count', COALESCE(v_non_cancellable_auth_request_count, 0),
          'auth_request_retry_blocker_count', COALESCE(v_auth_request_retry_blocker_count, 0),
          'pay_channel_scope', v_pay_channel_scope
        ));
      END IF;

      IF COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0) > 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'BANK_TRANSFER_PROVIDER_REVIEW_REQUIRED',
          'message', 'One or more scoped bank transfers have provider submission evidence or ambiguous provider state and must be reviewed before this batch can be authorised again.',
          'operation_id', p_operation_id::text,
          'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
          'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
          'provider_attempt_or_evidence_transfer_count', COALESCE(v_provider_attempt_or_evidence_transfer_count, 0),
          'provider_or_ambiguous_evidence_transfer_count', COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0),
          'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
          'batch_submitted_transfer_count', COALESCE(v_submitted_transfer_count, 0),
          'pay_channel_scope', v_pay_channel_scope
        ));
      END IF;

      IF COALESCE(v_scoped_operation_scope_count, 0) > 0
         AND COALESCE(v_authorisation_ready_transfer_count, 0) <> COALESCE(v_scoped_operation_scope_count, 0)
         AND COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0) = 0 THEN
        v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
          'code', 'NO_AUTHORISATION_READY_TRANSFERS',
          'message', 'All scoped bank transfer groups must be authorisation-ready before authorisation can start.',
          'operation_id', p_operation_id::text,
          'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
          'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
          'canonical_pending_status_transfer_count', COALESCE(v_canonical_pending_status_transfer_count, 0),
          'local_only_transfer_count', COALESCE(v_local_only_transfer_count, 0),
          'safe_local_cleanup_transfer_count', COALESCE(v_safe_local_cleanup_transfer_count, 0),
          'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
          'pay_channel_scope', v_pay_channel_scope
        ));
      END IF;
    END IF;

    IF p_operation_id IS NULL AND COALESCE(v_non_cancellable_auth_request_count, 0) > 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'AUTH_REQUEST_HELD_BY_PREVIOUS_OPERATION',
        'message', 'An active authorisation request is not safe to reuse or cancel automatically for this payment batch.',
        'non_cancellable_auth_request_count', COALESCE(v_non_cancellable_auth_request_count, 0),
        'auth_request_retry_blocker_count', COALESCE(v_auth_request_retry_blocker_count, 0),
        'pay_channel_scope', v_pay_channel_scope
      ));
    END IF;

    IF p_operation_id IS NULL AND v_provider_or_ambiguous_evidence_transfer_count > 0 AND v_transfer_count > 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'BANK_TRANSFER_PROVIDER_REVIEW_REQUIRED',
        'message', 'One or more bank transfers have provider submission evidence or ambiguous provider state and must be reviewed before this batch can be authorised again.',
        'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
        'provider_attempt_or_evidence_transfer_count', COALESCE(v_provider_attempt_or_evidence_transfer_count, 0),
        'provider_or_ambiguous_evidence_transfer_count', COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0),
        'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
        'submitted_transfer_count', COALESCE(v_submitted_transfer_count, 0),
        'pay_channel_scope', v_pay_channel_scope
      ));
    ELSIF p_operation_id IS NULL AND v_authorisation_ready_transfer_count <= 0 AND v_transfer_count > 0 THEN
      v_blockers_json := v_blockers_json || jsonb_build_array(jsonb_build_object(
        'code', 'NO_AUTHORISATION_READY_TRANSFERS',
        'message', 'No safe locally prepared bank transfers are available for authorisation.',
        'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
        'canonical_pending_status_transfer_count', COALESCE(v_canonical_pending_status_transfer_count, 0),
        'local_only_transfer_count', COALESCE(v_local_only_transfer_count, 0),
        'safe_local_cleanup_transfer_count', COALESCE(v_safe_local_cleanup_transfer_count, 0),
        'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
        'pay_channel_scope', v_pay_channel_scope
      ));
    ELSIF v_pending_transfer_count <= 0 AND v_authorisation_ready_transfer_count > 0 THEN
      v_warnings_json := v_warnings_json || jsonb_build_array(jsonb_build_object(
        'code', 'EVIDENCE_PENDING_COUNT_ZERO_AUTHORISATION_READY_PRESENT',
        'message', 'Evidence pending count is zero, but safe local transfers are ready for authorisation.',
        'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
        'evidence_pending_transfer_count', COALESCE(v_pending_transfer_count, 0),
        'pay_channel_scope', v_pay_channel_scope
      ));
    END IF;
  END IF;

  v_blocker_count := jsonb_array_length(v_blockers_json);
  v_warning_count := jsonb_array_length(v_warnings_json);
  v_ready := v_blocker_count = 0;

  v_next_required_phase := CASE
    WHEN v_execution_commit_state <> 'NOT_SUBMITTED' THEN 'STOP_EXECUTION_BOUNDARY_CROSSED'
    WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(v_blockers_json) AS blocker(elem) WHERE blocker.elem->>'code' IN ('BATCH_STALE','FRESHNESS_REQUIRED','FRESHNESS_REQUIRES_CHUNKED_VALIDATION','FRESHNESS_RESULT_HASH_MISMATCH')) THEN 'VALIDATE_FRESHNESS'
    WHEN v_batch_status IN ('CANCELLED', 'CANCELED') THEN 'STOP_CANCELLED'
    WHEN v_is_local_manual_mode AND v_ready THEN 'START_AUTHORISATION'
    WHEN v_transfer_count <= 0 THEN 'PREPARE_TRANSFER_SCOPE'
    WHEN v_blocker_count > 0 THEN 'RESOLVE_BLOCKERS'
    WHEN v_execution_mode IN ('STANDARD_BANK', 'BANK') AND p_operation_id IS NOT NULL AND COALESCE(v_all_scoped_operation_scopes_authorisation_ready, false) IS NOT TRUE THEN 'RESOLVE_BLOCKERS'
    WHEN v_execution_mode IN ('STANDARD_BANK', 'BANK') AND v_authorisation_ready_transfer_count <= 0 THEN 'RESOLVE_BLOCKERS'
    ELSE 'START_AUTHORISATION'
  END;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'ready', v_ready,
    'ready_flag', v_ready,
    'blocker_count', v_blocker_count,
    'warning_count', v_warning_count,
    'blockers', v_blockers_json,
    'warnings', v_warnings_json,
    'next_required_phase', v_next_required_phase,
    'execution_summary', v_summary_json,
    'batch_status', v_batch_row.status,
    'execution_commit_state', v_execution_commit_state,
    'execution_mode', v_execution_mode,
    'pay_channel_scope', v_pay_channel_scope,
    'provider_submission_required', v_execution_mode = 'STANDARD_BANK',
    'provider_submission_attempted', false,
    'submitted_to_bank', false,
    'local_settlement_evidence_only', v_is_local_manual_mode,
    'authorisation_ready_transfer_count', COALESCE(v_authorisation_ready_transfer_count, 0),
    'scoped_operation_scope_count', COALESCE(v_scoped_operation_scope_count, 0),
    'scoped_scope_prepared_count', COALESCE(v_scoped_scope_prepared_count, 0),
    'scoped_scope_pending_count', COALESCE(v_scoped_scope_pending_count, 0),
    'scoped_scope_failed_count', COALESCE(v_scoped_scope_failed_count, 0),
    'scoped_scope_skipped_count', COALESCE(v_scoped_scope_skipped_count, 0),
    'scoped_scope_without_transfer_count', COALESCE(v_scoped_scope_without_transfer_count, 0),
    'all_scoped_operation_scopes_authorisation_ready', COALESCE(v_all_scoped_operation_scopes_authorisation_ready, false),
    'non_cancellable_auth_request_count', COALESCE(v_non_cancellable_auth_request_count, 0),
    'auth_request_retry_blocker_count', COALESCE(v_auth_request_retry_blocker_count, 0),
    'unattempted_submit_eligible_transfer_count', COALESCE(v_unattempted_submit_eligible_transfer_count, 0),
    'remaining_unattempted_submit_required', COALESCE(v_remaining_unattempted_submit_required, 0),
    'provider_attempt_or_evidence_transfer_count', COALESCE(v_provider_attempt_or_evidence_transfer_count, 0),
    'provider_or_ambiguous_evidence_transfer_count', COALESCE(v_provider_or_ambiguous_evidence_transfer_count, 0),
    'canonical_pending_status_transfer_count', COALESCE(v_canonical_pending_status_transfer_count, 0),
    'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
    'scoped_failed_or_blocked_transfer_count', COALESCE(v_scoped_failed_or_blocked_transfer_count, 0),
    'scoped_blocked_transfer_count', COALESCE(v_scoped_blocked_transfer_count, 0),
    'evidence_pending_transfer_count', COALESCE(v_pending_transfer_count, 0),
    'freshness', jsonb_build_object(
      'freshness_validation_status', v_batch_freshness_status,
      'freshness_result_hash', v_batch_freshness_hash,
      'freshness_scope_hash', v_batch_freshness_scope_hash,
      'freshness_operation_id', CASE WHEN v_batch_row.freshness_operation_id IS NULL THEN NULL ELSE v_batch_row.freshness_operation_id::text END,
      'expected_freshness_result_hash', v_expected_freshness_hash,
      'freshness_result_json', v_batch_freshness_json
    ),
    'schedule_state', coalesce(v_summary_json->'schedule_state', '{}'::jsonb),
    'authorisation_state', coalesce(v_summary_json->'authorisation_state', '{}'::jsonb),
    'blocked_funds_state', coalesce(v_summary_json->'blocked_funds_state', '{}'::jsonb),
    'settlement_state', coalesce(v_summary_json->'settlement_state', '{}'::jsonb),
    'remittance_state', coalesce(v_summary_json->'remittance_state', '{}'::jsonb),
    'server_utc', v_now
  );
END;
$function$;

ALTER FUNCTION public.pay_batch_prepare(uuid, uuid, uuid, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_prepare(uuid, uuid, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batch_prepare(uuid, uuid, uuid, text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batch_prepare(uuid, uuid, uuid, text) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batch_prepare(uuid, uuid, uuid, text) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_prepare(uuid, uuid, uuid, text) TO service_role;
