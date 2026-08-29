-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: 31db645571274568dbe5846bfbae7a90

CREATE OR REPLACE FUNCTION public.pay_settle_rail(p_pay_batch_id uuid, p_settlement_json jsonb, p_actor_user_id uuid, p_operation_id uuid DEFAULT NULL::uuid, p_settlement_scope_ids jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_batch record;

  v_now timestamptz := now();

  v_newly_settled_candidates jsonb := '[]'::jsonb;

  v_pending_transfers jsonb := '[]'::jsonb;
  v_failed_transfers  jsonb := '[]'::jsonb;
  v_returned_transfers jsonb := '[]'::jsonb;
  v_blocked_transfers jsonb := '[]'::jsonb;

  v_batch_status text;

  v_missing_timesheets jsonb := '[]'::jsonb;
  v_ambig_timesheets jsonb := '[]'::jsonb;

  v_adv_id uuid;
  v_old_sched jsonb;
  v_new_sched jsonb;
  v_old_out numeric;
  v_new_out numeric;
  v_old_next date;
  v_new_next date;
  v_total_taken numeric;
  v_case_type public.pay_finance_case_type_enum;
  v_min_earnings_threshold numeric;
  v_take_home_floor_override numeric;
  v_snooze_active boolean;
  v_active_snooze_report_count integer := 0;
  v_active_snooze_report_sample jsonb := '[]'::jsonb;
  v_date_context jsonb := '{}'::jsonb;
  v_today_uk date := NULL::date;
  v_was_cleared boolean;

  v_linked_transfer_ct int := 0;
  v_has_pos_net boolean := false;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;
  v_stored_freshness_status text := null;
  v_stored_freshness_result_hash text := null;
  v_stored_freshness_scope_hash text := null;
  v_stored_freshness_result_json jsonb := '{}'::jsonb;
  v_stored_freshness_operation_id uuid := null;

  v_overpay_patched_ct int := 0;
  v_payment_advance_recovery_patched_ct int := 0;
  v_manual_debt_patched_ct int := 0;
  v_payout_cases_marked_paid_ct int := 0;
  v_component_settled_count int := 0;
  v_component_settled_amount numeric := 0;
  v_component_unresolved_count int := 0;
  v_component_unresolved_amount numeric := 0;
  v_component_unresolved_json jsonb := '[]'::jsonb;
  v_component_reconciliation_bad jsonb := '[]'::jsonb;
  v_component_reconciliation_bad_ct int := 0;
  v_component_reconciliation_checked_ct int := 0;

  v_comm_result jsonb := '{}'::jsonb;
  v_comm_trigger_status text := null;
  v_comm_error text := null;
  v_worker_communications jsonb := '{}'::jsonb;
  v_catchup_needed boolean := false;
  v_changed_channel_audit record;
  v_changed_channel_audit_after_json jsonb := null;
  v_changed_channel_settled_ct int := 0;
  v_execution_commit_state text := 'NOT_SUBMITTED';
  v_execution_commit_ref text := null;
  v_execution_committed_at_utc timestamptz := null;
  v_completed_transfer_count int := 0;
  v_detected_execution_commit_ref text := null;
  v_detected_execution_committed_at_utc timestamptz := null;
  v_execution_intent_json jsonb := '{}'::jsonb;
  v_settlement_confirmation_json jsonb := '{}'::jsonb;
  v_durably_finalised_candidate_ids jsonb := '[]'::jsonb;
  v_suppress_remittances boolean := false;
  v_settlement_mode text := 'STANDARD_BANK';
  v_effective_payment_date date := null;
  v_suppression_audit_json jsonb := '{}'::jsonb;
  v_bank_event_ingest_results jsonb := '[]'::jsonb;
  v_bank_event_ingest_count int := 0;

  v_operation_mode boolean := false;
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_requested_scope_count integer := 0;
  v_matched_scope_count integer := 0;
  v_settled_this_chunk integer := 0;
  v_reused_this_chunk integer := 0;
  v_failed_this_chunk integer := 0;
  v_pending_this_chunk integer := 0;
  v_remaining_scope_count integer := 0;
  v_existing_failed_scope_count integer := 0;
  v_total_failed_scope_count integer := 0;
  v_duplicate_transfer_ids jsonb := '[]'::jsonb;
  v_out_of_scope_transfer_ids jsonb := '[]'::jsonb;
  v_has_more boolean := false;
  v_carry_forward_mark_result jsonb := '{}'::jsonb;
  v_consumed_carry_forward_count integer := 0;
  v_live_signal_result jsonb := '{}'::jsonb;
  v_scope_settlement_complete boolean := false;
  v_operation_completed_transfer_count integer := 0;
  v_successful_scope_count integer := 0;
  v_terminal_failed_scope_count integer := 0;
  v_pending_scope_count integer := 0;
  v_unknown_scope_count integer := 0;
  v_processor_failed_scope_count integer := 0;
  v_all_scopes_terminal boolean := false;
  v_all_scopes_successful boolean := false;
  v_completed_with_failed_payments boolean := false;
  v_batch_status_label text := null;
  v_requires_full_batch_finalisation boolean := false;
  v_full_batch_finalisation_safe boolean := false;
  v_finalisation_idempotency_key text := null;
  v_finalisation_payload_hash text := null;

  v_execution_operation_row public.banking_pay_operations%ROWTYPE;
  v_root_operation_input_json jsonb := '{}'::jsonb;
  v_operation_auth_intent_json jsonb := '{}'::jsonb;
  v_batch_intent_json jsonb := '{}'::jsonb;
  v_auth_locator_json jsonb := '{}'::jsonb;
  v_batch_intent_operation_id uuid := NULL::uuid;
  v_auth_intent_auth_request_id uuid := NULL::uuid;
  v_batch_intent_auth_request_id uuid := NULL::uuid;
  v_authorised_execution_mode text := NULL::text;
  v_batch_execution_mode text := NULL::text;
  v_batch_projection_scope text := NULL::text;
  v_server_owned_projection_proof boolean := false;
  v_batch_server_owned_projection_proof boolean := false;
  v_batch_no_bank_payment_marker boolean := false;
  v_batch_allow_explicit_zero_no_bank_scopes_marker boolean := false;
  v_batch_suppress_remittances boolean := false;
  v_expected_global_zero_count integer := NULL::integer;
  v_batch_expected_global_zero_count integer := NULL::integer;
  v_expected_global_positive_count integer := NULL::integer;
  v_batch_expected_global_positive_count integer := NULL::integer;
  v_expected_global_invalid_count integer := NULL::integer;
  v_batch_expected_global_invalid_count integer := NULL::integer;
  v_expected_scoped_missing_count integer := NULL::integer;
  v_batch_expected_scoped_missing_count integer := NULL::integer;
  v_expected_scoped_invalid_count integer := NULL::integer;
  v_batch_expected_scoped_invalid_count integer := NULL::integer;
  v_batch_expected_paye_net_state_hash text := NULL::text;
  v_expected_global_bank_payment_projection_hash text := NULL::text;
  v_batch_expected_global_bank_payment_projection_hash text := NULL::text;
  v_expected_scoped_paye_net_state_hash text := NULL::text;
  v_batch_expected_scoped_paye_net_state_hash text := NULL::text;
  v_batch_expected_bank_payment_projection_hash text := NULL::text;
  v_batch_expected_missing_count integer := NULL::integer;
  v_batch_expected_zero_count integer := NULL::integer;
  v_batch_expected_positive_count integer := NULL::integer;
  v_scoped_no_transfer_marker boolean := false;
  v_batch_scoped_no_transfer_marker boolean := false;
  v_authorised_payment_date date := NULL::date;
  v_batch_authorised_payment_date date := NULL::date;
  v_authorised_payment_date_raw text := NULL::text;
  v_batch_authorised_payment_date_raw text := NULL::text;
  v_total_scope_count integer := 0;
  v_no_bank_eligible_scope_count integer := 0;
  v_selected_no_bank_scope_count integer := 0;
  v_selected_eligible_no_bank_scope_count integer := 0;
  v_operation_scope_state_json jsonb := '[]'::jsonb;
  v_proof_validation_outcome text := 'NOT_VALIDATED';
  v_auth_request_candidate_count integer := 0;
  v_no_bank_settlement_operation_count integer := 0;
  v_explicit_settlement_operation_id uuid := NULL::uuid;
  v_no_bank_distinct_candidate_count integer := 0;
  v_positive_scope_count integer := 0;
  v_positive_nonterminal_scope_count integer := 0;
  v_positive_failed_scope_count integer := 0;
  v_operation_auth_request_id uuid := NULL::uuid;
  v_operation_auth_state text := NULL::text;
  v_execution_operation_id uuid := NULL::uuid;
  v_execution_operation_resolved_from_batch_intent boolean := false;
  v_auth_intent_operation_id uuid := NULL::uuid;
  v_no_bank_payment_marker boolean := false;
  v_allow_explicit_zero_no_bank_scopes_marker boolean := false;
  v_no_bank_scope_authorised boolean := false;
  v_no_bank_payment_execution_validated boolean := false;
  v_operation_projection_scope text := NULL::text;
  v_expected_paye_net_state_hash text := NULL::text;
  v_expected_bank_payment_projection_hash text := NULL::text;
  v_expected_missing_count integer := NULL::integer;
  v_expected_zero_count integer := NULL::integer;
  v_expected_positive_count integer := NULL::integer;
  v_current_paye_net_state_hash text := NULL::text;
  v_all_bank_payment_projection_hash text := NULL::text;
  v_current_scoped_paye_net_state_hash text := NULL::text;
  v_current_bank_payment_projection_hash text := NULL::text;
  v_current_missing_count integer := 0;
  v_global_zero_count integer := 0;
  v_global_positive_count integer := 0;
  v_global_invalid_count integer := 0;
  v_current_scoped_missing_count integer := 0;
  v_current_zero_count integer := 0;
  v_current_positive_count integer := 0;
  v_current_positive_total numeric(14,2) := 0;
  v_current_scoped_invalid_count integer := 0;
  v_current_projection_changed boolean := false;
  v_projection_diagnostic_json jsonb := '{}'::jsonb;
  v_operation_effective_payment_date date := NULL::date;
  v_no_bank_settled_this_chunk integer := 0;
  v_no_bank_total_scope_count integer := 0;
  v_local_no_bank_commit_ref text := NULL::text;
  v_no_bank_scope_artifact_count integer := 0;
  v_no_bank_batch_transfer_count integer := 0;
  v_no_bank_transfer_event_count integer := 0;
  v_no_bank_provider_attempt_count integer := 0;
  v_no_bank_settlement_operation_id uuid := NULL::uuid;
  v_settlement_operation_id uuid := NULL::uuid;
  v_settlement_operation_count integer := 0;
  v_related_settlement_operation_count integer := 0;
  v_bound_settlement_scope_count integer := 0;
  v_completed_transfer_total numeric(14,2) := 0;
  v_completed_transfer_ids jsonb := '[]'::jsonb;
  v_settlement_scope_ids jsonb := '[]'::jsonb;
  v_local_positive_commit_ref text := NULL::text;
  v_bank_confirm_ref text := NULL::text;
  v_submitted_to_bank boolean := false;
  v_provider_submission_required boolean := false;
  v_provider_submission_attempted boolean := false;
  v_local_settlement_evidence_only boolean := false;
  v_legacy_direct_settlement boolean := false;
  v_settlement_positive_scope_count integer := 0;
  v_settlement_positive_transfer_count integer := 0;
  v_settlement_missing_event_count integer := 0;
  v_settlement_nonterminal_scope_count integer := 0;
  v_settlement_failed_scope_count integer := 0;
  v_settlement_local_proof_invalid_count integer := 0;
  v_settlement_unmatched_transfer_count integer := 0;
  v_settlement_unmatched_scope_transfer_count integer := 0;
  v_no_bank_nonterminal_scope_count integer := 0;
  v_no_bank_failed_scope_count integer := 0;
  v_no_bank_invalid_scope_count integer := 0;
  v_no_bank_missing_scope_count integer := 0;
  v_unproved_missing_paye_candidate_count integer := 0;
  v_unproved_zero_paye_candidate_count integer := 0;
begin
  BEGIN
    v_date_context := public.pay_banking_official_date_context_v1(NULL::timestamptz);
    v_today_uk := CASE
      WHEN COALESCE(v_date_context->>'london_current_date', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN (v_date_context->>'london_current_date')::date
      ELSE NULL::date
    END;
    IF v_today_uk IS NULL THEN
      v_date_context := COALESCE(v_date_context, '{}'::jsonb) || jsonb_build_object(
        'ok', false,
        'code', 'PAY_SETTLE_RAIL_LONDON_DATE_UNAVAILABLE',
        'message', 'The current Europe/London business date could not be resolved for settlement diagnostics.'
      );
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_today_uk := NULL::date;
    v_date_context := jsonb_build_object(
      'ok', false,
      'code', 'PAY_SETTLE_RAIL_LONDON_DATE_UNAVAILABLE',
      'message', 'The current Europe/London business date could not be resolved for settlement diagnostics.',
      'technical_error', SQLERRM
    );
  END;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAY_SETTLE_RAIL_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'actor_user_id', p_actor_user_id,
      'settlement_row_count', CASE WHEN p_settlement_json IS NULL OR jsonb_typeof(p_settlement_json) <> 'array' THEN NULL ELSE jsonb_array_length(p_settlement_json) END
    ),
    'pay_batches',
    COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  if p_pay_batch_id is null then
    raise exception 'pay_settle_rail: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_settle_rail: actor_user_id is required';
  end if;

  if p_settlement_json is null or jsonb_typeof(p_settlement_json) <> 'array' then
    raise exception 'pay_settle_rail: settlement_json must be a JSON array';
  end if;

  v_operation_mode := (p_operation_id IS NOT NULL OR p_settlement_scope_ids IS NOT NULL);

  IF v_operation_mode THEN
    IF p_operation_id IS NULL THEN
      RAISE EXCEPTION 'pay_settle_rail operation mode requires p_operation_id';
    END IF;
    IF p_settlement_scope_ids IS NULL OR jsonb_typeof(p_settlement_scope_ids) <> 'array' OR jsonb_array_length(p_settlement_scope_ids) = 0 THEN
      RAISE EXCEPTION 'pay_settle_rail operation mode requires non-empty p_settlement_scope_ids';
    END IF;

    -- Resolve the execution binding without holding a row lock, then acquire
    -- canonical locks in root-execution -> requested-operation -> batch order.
    -- The requested operation and batch binding are re-read and revalidated after
    -- the canonical locks are held so an initial read can never become authority.
    SELECT operation_row.*
    INTO v_operation_row
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'banking_pay_operations row % not found', p_operation_id;
    END IF;

    IF v_operation_row.operation_type NOT IN ('PAYMENT_SETTLEMENT', 'PAYMENT_EXECUTE') THEN
      RAISE EXCEPTION 'operation % is not a settlement-capable operation', p_operation_id;
    END IF;

    IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
      RAISE EXCEPTION 'operation % is for pay batch %, not %', p_operation_id, v_operation_row.pay_batch_id, p_pay_batch_id;
    END IF;

    IF v_operation_row.actor_user_id IS NOT NULL AND v_operation_row.actor_user_id <> p_actor_user_id THEN
      RAISE EXCEPTION 'operation % belongs to a different actor', p_operation_id;
    END IF;

    IF UPPER(BTRIM(COALESCE(v_operation_row.operation_type, ''))) = 'PAYMENT_EXECUTE' THEN
      v_execution_operation_id := v_operation_row.id;
    ELSIF v_operation_row.root_operation_id IS NOT NULL THEN
      v_execution_operation_id := v_operation_row.root_operation_id;
    ELSE
      SELECT CASE
        WHEN JSONB_TYPEOF(pb.execution_intent_json) = 'object'
          THEN COALESCE(pb.execution_intent_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END
      INTO v_batch_intent_json
      FROM public.pay_batches AS pb
      WHERE pb.id = p_pay_batch_id;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'pay_settle_rail: pay_batch not found';
      END IF;

      IF NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'operation_id', '')), '')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        v_execution_operation_id := (v_batch_intent_json->>'operation_id')::uuid;
        v_execution_operation_resolved_from_batch_intent := true;
      END IF;
    END IF;

    IF v_execution_operation_id IS NULL THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'EXECUTION_OPERATION_REQUIRED',
        'message', 'The settlement operation is not bound to a payment execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    SELECT execution_operation.*
    INTO v_execution_operation_row
    FROM public.banking_pay_operations AS execution_operation
    WHERE execution_operation.id = v_execution_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_execution_operation_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id
       OR v_execution_operation_row.operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'EXECUTION_OPERATION_INVALID',
        'message', 'The resolved payment execution operation is missing or incompatible with this settlement operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_operation_id', v_execution_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF p_operation_id = v_execution_operation_id THEN
      v_operation_row := v_execution_operation_row;
    ELSE
      SELECT operation_row.*
      INTO v_operation_row
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.id = p_operation_id
      FOR UPDATE;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'banking_pay_operations row % not found', p_operation_id;
      END IF;
    END IF;

    IF v_operation_row.operation_type NOT IN ('PAYMENT_SETTLEMENT', 'PAYMENT_EXECUTE') THEN
      RAISE EXCEPTION 'operation % is not a settlement-capable operation', p_operation_id;
    END IF;

    IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
      RAISE EXCEPTION 'operation % is for pay batch %, not %', p_operation_id, v_operation_row.pay_batch_id, p_pay_batch_id;
    END IF;

    IF v_operation_row.actor_user_id IS NOT NULL AND v_operation_row.actor_user_id <> p_actor_user_id THEN
      RAISE EXCEPTION 'operation % belongs to a different actor', p_operation_id;
    END IF;

    IF UPPER(BTRIM(COALESCE(v_operation_row.operation_type, ''))) = 'PAYMENT_EXECUTE' THEN
      IF v_operation_row.id IS DISTINCT FROM v_execution_operation_id THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'EXECUTION_OPERATION_BINDING_CHANGED',
          'message', 'The requested operation changed its execution-operation binding while rail settlement was acquiring its canonical locks.',
          'pay_batch_id', p_pay_batch_id::text,
          'operation_id', p_operation_id::text,
          'locked_execution_operation_id', v_execution_operation_id::text,
          'current_operation_type', v_operation_row.operation_type,
          'current_root_operation_id', CASE WHEN v_operation_row.root_operation_id IS NULL THEN NULL::text ELSE v_operation_row.root_operation_id::text END
        )::text USING ERRCODE = 'P0001';
      END IF;
    ELSIF v_operation_row.root_operation_id IS NOT NULL THEN
      IF v_operation_row.root_operation_id IS DISTINCT FROM v_execution_operation_id THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'EXECUTION_OPERATION_BINDING_CHANGED',
          'message', 'The settlement operation changed its execution-operation binding while rail settlement was acquiring its canonical locks.',
          'pay_batch_id', p_pay_batch_id::text,
          'operation_id', p_operation_id::text,
          'locked_execution_operation_id', v_execution_operation_id::text,
          'current_root_operation_id', v_operation_row.root_operation_id::text
        )::text USING ERRCODE = 'P0001';
      END IF;
    ELSE
      v_execution_operation_resolved_from_batch_intent := true;
    END IF;

    SELECT pb.*
    INTO v_batch
    FROM public.pay_batches AS pb
    WHERE pb.id = p_pay_batch_id
    FOR UPDATE;

    IF v_batch.id IS NULL THEN
      RAISE EXCEPTION 'pay_settle_rail: pay_batch not found';
    END IF;

    v_stored_freshness_status := upper(btrim(coalesce(v_batch.freshness_validation_status, '')));
    v_stored_freshness_result_hash := nullif(btrim(coalesce(v_batch.freshness_result_hash, '')), '');
    v_stored_freshness_scope_hash := nullif(btrim(coalesce(v_batch.freshness_scope_hash, '')), '');
    v_stored_freshness_result_json := coalesce(v_batch.freshness_result_json, '{}'::jsonb);
    v_stored_freshness_operation_id := v_batch.freshness_operation_id;
    v_execution_commit_state := UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED')));
    IF v_execution_commit_state NOT IN ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED') THEN
      v_execution_commit_state := 'NOT_SUBMITTED';
    END IF;
    v_execution_commit_ref := v_batch.execution_commit_ref;
    v_execution_committed_at_utc := v_batch.execution_committed_at_utc;

    v_batch_intent_json := CASE
      WHEN JSONB_TYPEOF(v_batch.execution_intent_json) = 'object'
        THEN COALESCE(v_batch.execution_intent_json, '{}'::jsonb)
      ELSE '{}'::jsonb
    END;

    IF v_execution_operation_resolved_from_batch_intent
       AND (
         CASE
           WHEN NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'operation_id', '')), '')
                ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             THEN (v_batch_intent_json->>'operation_id')::uuid
           ELSE NULL::uuid
         END
       ) IS DISTINCT FROM v_execution_operation_id THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'EXECUTION_OPERATION_BINDING_CHANGED',
        'message', 'The batch execution-operation binding changed while rail settlement was acquiring its canonical locks.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'locked_execution_operation_id', v_execution_operation_id::text,
        'current_batch_execution_operation_id', NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'operation_id', '')), '')
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_root_operation_input_json := COALESCE(v_execution_operation_row.input_json, '{}'::jsonb);
    v_auth_locator_json := COALESCE(v_operation_row.input_json, '{}'::jsonb)
      || CASE
           WHEN JSONB_TYPEOF(v_operation_row.input_json->'execution_intent_json') = 'object'
             THEN v_operation_row.input_json->'execution_intent_json'
           ELSE '{}'::jsonb
         END
      || v_root_operation_input_json
      || CASE
           WHEN JSONB_TYPEOF(v_root_operation_input_json->'execution_intent_json') = 'object'
             THEN v_root_operation_input_json->'execution_intent_json'
           ELSE '{}'::jsonb
         END
      || v_batch_intent_json;

    IF NULLIF(BTRIM(COALESCE(
      v_auth_locator_json->>'auth_request_id',
      v_auth_locator_json->>'authRequestId',
      ''
    )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_operation_auth_request_id := COALESCE(
        v_auth_locator_json->>'auth_request_id',
        v_auth_locator_json->>'authRequestId'
      )::uuid;
    END IF;

    IF v_operation_auth_request_id IS NULL THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'AUTHORISED_EXECUTION_INTENT_REQUIRED',
        'message', 'An authorised server-frozen execution intent is required before operation-mode rail settlement can continue.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_operation_id', v_execution_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    SELECT
      auth_request.state,
      CASE
        WHEN JSONB_TYPEOF(auth_request.execution_intent_json) = 'object'
          THEN COALESCE(auth_request.execution_intent_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END
    INTO
      v_operation_auth_state,
      v_operation_auth_intent_json
    FROM public.pay_batch_auth_requests AS auth_request
    WHERE auth_request.id = v_operation_auth_request_id
      AND auth_request.pay_batch_id = p_pay_batch_id
    FOR UPDATE;

    IF NOT FOUND
       OR UPPER(BTRIM(COALESCE(v_operation_auth_state, ''))) <> 'AUTHORISED'
       OR JSONB_TYPEOF(v_operation_auth_intent_json) <> 'object'
       OR v_operation_auth_intent_json = '{}'::jsonb THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'AUTHORISED_EXECUTION_INTENT_REQUIRED',
        'message', 'The payment auth request is missing, belongs to another batch, is not authorised, or has no execution intent.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text,
        'auth_state', v_operation_auth_state
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_batch_intent_json = '{}'::jsonb THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'BATCH_EXECUTION_INTENT_REQUIRED',
        'message', 'The batch does not contain the server-frozen execution intent required for settlement.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'operation_id', '')), '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_auth_intent_operation_id := (v_operation_auth_intent_json->>'operation_id')::uuid;
    END IF;
    IF NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'operation_id', '')), '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_batch_intent_operation_id := (v_batch_intent_json->>'operation_id')::uuid;
    END IF;
    IF NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'auth_request_id', '')), '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_auth_intent_auth_request_id := (v_operation_auth_intent_json->>'auth_request_id')::uuid;
    END IF;
    IF NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'auth_request_id', '')), '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_batch_intent_auth_request_id := (v_batch_intent_json->>'auth_request_id')::uuid;
    END IF;

    v_authorised_execution_mode := CASE
      WHEN UPPER(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
      WHEN UPPER(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
      WHEN UPPER(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
      ELSE UPPER(NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', '')), ''))
    END;
    v_batch_execution_mode := CASE
      WHEN UPPER(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
      WHEN UPPER(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
      WHEN UPPER(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
      ELSE UPPER(NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', '')), ''))
    END;
    v_operation_projection_scope := UPPER(NULLIF(BTRIM(COALESCE(
      v_operation_auth_intent_json->>'pay_channel_scope',
      v_operation_auth_intent_json->>'scope',
      ''
    )), ''));
    v_batch_projection_scope := UPPER(NULLIF(BTRIM(COALESCE(
      v_batch_intent_json->>'pay_channel_scope',
      v_batch_intent_json->>'scope',
      ''
    )), ''));

    v_expected_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_operation_auth_intent_json->>'global_paye_net_state_hash',
      v_operation_auth_intent_json->>'paye_net_state_hash',
      ''
    )), '');
    v_batch_expected_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_batch_intent_json->>'global_paye_net_state_hash',
      v_batch_intent_json->>'paye_net_state_hash',
      ''
    )), '');
    v_expected_global_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_operation_auth_intent_json->>'global_bank_payment_projection_hash',
      ''
    )), '');
    v_batch_expected_global_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_batch_intent_json->>'global_bank_payment_projection_hash',
      ''
    )), '');
    v_expected_scoped_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_operation_auth_intent_json->>'scoped_paye_net_state_hash',
      ''
    )), '');
    v_batch_expected_scoped_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
      v_batch_intent_json->>'scoped_paye_net_state_hash',
      ''
    )), '');
    v_expected_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_operation_auth_intent_json->>'scoped_bank_payment_projection_hash',
      v_operation_auth_intent_json->>'bank_payment_projection_hash',
      ''
    )), '');
    v_batch_expected_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
      v_batch_intent_json->>'scoped_bank_payment_projection_hash',
      v_batch_intent_json->>'bank_payment_projection_hash',
      ''
    )), '');

    IF COALESCE(v_operation_auth_intent_json->>'global_missing_explicit_paye_input_count', v_operation_auth_intent_json->>'missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_expected_missing_count := COALESCE(v_operation_auth_intent_json->>'global_missing_explicit_paye_input_count', v_operation_auth_intent_json->>'missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'global_missing_explicit_paye_input_count', v_batch_intent_json->>'missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_missing_count := COALESCE(v_batch_intent_json->>'global_missing_explicit_paye_input_count', v_batch_intent_json->>'missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_operation_auth_intent_json->>'global_explicit_zero_count', v_operation_auth_intent_json->>'explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_expected_global_zero_count := COALESCE(v_operation_auth_intent_json->>'global_explicit_zero_count', v_operation_auth_intent_json->>'explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'global_explicit_zero_count', v_batch_intent_json->>'explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_global_zero_count := COALESCE(v_batch_intent_json->>'global_explicit_zero_count', v_batch_intent_json->>'explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_operation_auth_intent_json->>'global_positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_expected_global_positive_count := (v_operation_auth_intent_json->>'global_positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'global_positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_global_positive_count := (v_batch_intent_json->>'global_positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_operation_auth_intent_json->>'global_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_expected_global_invalid_count := (v_operation_auth_intent_json->>'global_invalid_payment_row_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'global_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_global_invalid_count := (v_batch_intent_json->>'global_invalid_payment_row_count')::integer;
    END IF;
    IF COALESCE(v_operation_auth_intent_json->>'scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_expected_scoped_missing_count := (v_operation_auth_intent_json->>'scoped_missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_scoped_missing_count := (v_batch_intent_json->>'scoped_missing_explicit_paye_input_count')::integer;
    END IF;
    IF COALESCE(v_operation_auth_intent_json->>'scoped_explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_expected_zero_count := (v_operation_auth_intent_json->>'scoped_explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'scoped_explicit_zero_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_zero_count := (v_batch_intent_json->>'scoped_explicit_zero_count')::integer;
    END IF;
    IF COALESCE(v_operation_auth_intent_json->>'scoped_positive_bank_payment_count', v_operation_auth_intent_json->>'positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_expected_positive_count := COALESCE(v_operation_auth_intent_json->>'scoped_positive_bank_payment_count', v_operation_auth_intent_json->>'positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'scoped_positive_bank_payment_count', v_batch_intent_json->>'positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_positive_count := COALESCE(v_batch_intent_json->>'scoped_positive_bank_payment_count', v_batch_intent_json->>'positive_bank_payment_count')::integer;
    END IF;
    IF COALESCE(v_operation_auth_intent_json->>'scoped_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_expected_scoped_invalid_count := (v_operation_auth_intent_json->>'scoped_invalid_payment_row_count')::integer;
    END IF;
    IF COALESCE(v_batch_intent_json->>'scoped_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
      v_batch_expected_scoped_invalid_count := (v_batch_intent_json->>'scoped_invalid_payment_row_count')::integer;
    END IF;

    v_server_owned_projection_proof := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_batch_server_owned_projection_proof := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_scoped_no_transfer_marker := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'scoped_no_transfer_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_batch_scoped_no_transfer_marker := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'scoped_no_transfer_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_no_bank_payment_marker := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_batch_no_bank_payment_marker := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_allow_explicit_zero_no_bank_scopes_marker := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_batch_allow_explicit_zero_no_bank_scopes_marker := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_suppress_remittances := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_batch_suppress_remittances := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

    v_authorised_payment_date_raw := NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'payment_date', '')), '');
    v_batch_authorised_payment_date_raw := NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'payment_date', '')), '');
    IF v_authorised_payment_date_raw IS NOT NULL THEN
      BEGIN
        v_authorised_payment_date := v_authorised_payment_date_raw::date;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'PAY_SETTLE_RAIL_AUTH_INTENT_PAYMENT_DATE_INVALID' USING ERRCODE = 'P0001';
      END;
    END IF;
    IF v_batch_authorised_payment_date_raw IS NOT NULL THEN
      BEGIN
        v_batch_authorised_payment_date := v_batch_authorised_payment_date_raw::date;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'PAY_SETTLE_RAIL_BATCH_INTENT_PAYMENT_DATE_INVALID' USING ERRCODE = 'P0001';
      END;
    END IF;

    IF v_auth_intent_operation_id IS NULL
       OR v_batch_intent_operation_id IS NULL
       OR v_auth_intent_operation_id <> v_execution_operation_id
       OR v_batch_intent_operation_id <> v_execution_operation_id
       OR v_auth_intent_auth_request_id IS DISTINCT FROM v_operation_auth_request_id
       OR v_batch_intent_auth_request_id IS DISTINCT FROM v_operation_auth_request_id
       OR v_authorised_execution_mode NOT IN ('STANDARD_BANK', 'CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
       OR v_authorised_execution_mode IS DISTINCT FROM v_batch_execution_mode
       OR v_operation_projection_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA', 'LOANS')
       OR v_operation_projection_scope IS DISTINCT FROM v_batch_projection_scope
       OR v_expected_paye_net_state_hash IS NULL
       OR v_expected_paye_net_state_hash IS DISTINCT FROM v_batch_expected_paye_net_state_hash
       OR v_expected_global_bank_payment_projection_hash IS NULL
       OR v_expected_global_bank_payment_projection_hash IS DISTINCT FROM v_batch_expected_global_bank_payment_projection_hash
       OR v_expected_scoped_paye_net_state_hash IS NULL
       OR v_expected_scoped_paye_net_state_hash IS DISTINCT FROM v_batch_expected_scoped_paye_net_state_hash
       OR v_expected_bank_payment_projection_hash IS NULL
       OR v_expected_bank_payment_projection_hash IS DISTINCT FROM v_batch_expected_bank_payment_projection_hash
       OR v_expected_missing_count IS NULL
       OR v_expected_missing_count IS DISTINCT FROM v_batch_expected_missing_count
       OR v_expected_global_zero_count IS NULL
       OR v_expected_global_zero_count IS DISTINCT FROM v_batch_expected_global_zero_count
       OR v_expected_global_positive_count IS NULL
       OR v_expected_global_positive_count IS DISTINCT FROM v_batch_expected_global_positive_count
       OR v_expected_global_invalid_count IS NULL
       OR v_expected_global_invalid_count IS DISTINCT FROM v_batch_expected_global_invalid_count
       OR v_expected_scoped_missing_count IS NULL
       OR v_expected_scoped_missing_count IS DISTINCT FROM v_batch_expected_scoped_missing_count
       OR v_expected_zero_count IS NULL
       OR v_expected_zero_count IS DISTINCT FROM v_batch_expected_zero_count
       OR v_expected_positive_count IS NULL
       OR v_expected_positive_count IS DISTINCT FROM v_batch_expected_positive_count
       OR v_expected_scoped_invalid_count IS NULL
       OR v_expected_scoped_invalid_count IS DISTINCT FROM v_batch_expected_scoped_invalid_count
       OR v_server_owned_projection_proof IS NOT TRUE
       OR v_batch_server_owned_projection_proof IS NOT TRUE
       OR v_scoped_no_transfer_marker IS DISTINCT FROM v_batch_scoped_no_transfer_marker
       OR v_no_bank_payment_marker IS DISTINCT FROM v_batch_no_bank_payment_marker
       OR v_allow_explicit_zero_no_bank_scopes_marker IS DISTINCT FROM v_batch_allow_explicit_zero_no_bank_scopes_marker
       OR v_suppress_remittances IS DISTINCT FROM v_batch_suppress_remittances
       OR v_authorised_payment_date IS DISTINCT FROM v_batch_authorised_payment_date THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'AUTH_BATCH_EXECUTION_INTENT_MISMATCH',
        'message', 'The authorised execution intent and batch execution intent are missing required server proof or are materially inconsistent.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text,
        'authorised_execution_mode', v_authorised_execution_mode,
        'authorised_scope', v_operation_projection_scope
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_settlement_mode := v_authorised_execution_mode;
    v_operation_effective_payment_date := COALESCE(
      v_authorised_payment_date,
      v_batch.authoritative_payment_date,
      v_batch.pay_date
    );

    DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_projection_rows;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_projection_rows
    ON COMMIT DROP
    AS
    SELECT projection_row.*
    FROM public._pay_batch_bank_payment_projection_rows(p_pay_batch_id, 'ALL') AS projection_row;

    DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_scoped_projection_rows;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_scoped_projection_rows
    ON COMMIT DROP
    AS
    SELECT projection_row.*
    FROM public._pay_batch_bank_payment_projection_rows(p_pay_batch_id, v_operation_projection_scope) AS projection_row;

    SELECT
      COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'MISSING')::integer,
      COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'ZERO')::integer,
      COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer,
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
      v_current_missing_count,
      v_global_zero_count,
      v_global_positive_count,
      v_global_invalid_count,
      v_current_paye_net_state_hash,
      v_all_bank_payment_projection_hash
    FROM pg_temp.tmp_pay_settle_rail_projection_rows AS projection_row;

    SELECT
      COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'MISSING')::integer,
      COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'ZERO')::integer,
      COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer,
      ROUND(COALESCE(SUM(projection_row.amount) FILTER (WHERE projection_row.is_positive_bank_payment), 0), 2)::numeric(14,2),
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
      v_current_scoped_missing_count,
      v_current_zero_count,
      v_current_positive_count,
      v_current_positive_total,
      v_current_scoped_invalid_count,
      v_current_scoped_paye_net_state_hash,
      v_current_bank_payment_projection_hash
    FROM pg_temp.tmp_pay_settle_rail_scoped_projection_rows AS projection_row;

    v_current_missing_count := COALESCE(v_current_missing_count, 0);
    v_global_zero_count := COALESCE(v_global_zero_count, 0);
    v_global_positive_count := COALESCE(v_global_positive_count, 0);
    v_global_invalid_count := COALESCE(v_global_invalid_count, 0);
    v_current_scoped_missing_count := COALESCE(v_current_scoped_missing_count, 0);
    v_current_zero_count := COALESCE(v_current_zero_count, 0);
    v_current_positive_count := COALESCE(v_current_positive_count, 0);
    v_current_positive_total := ROUND(COALESCE(v_current_positive_total, 0), 2);
    v_current_scoped_invalid_count := COALESCE(v_current_scoped_invalid_count, 0);
    v_current_paye_net_state_hash := COALESCE(v_current_paye_net_state_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', 'ALL', 'rows', '[]'::jsonb)::text));
    v_all_bank_payment_projection_hash := COALESCE(v_all_bank_payment_projection_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', 'ALL', 'rows', '[]'::jsonb)::text));
    v_current_scoped_paye_net_state_hash := COALESCE(v_current_scoped_paye_net_state_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', v_operation_projection_scope, 'rows', '[]'::jsonb)::text));
    v_current_bank_payment_projection_hash := COALESCE(v_current_bank_payment_projection_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', v_operation_projection_scope, 'rows', '[]'::jsonb)::text));

    v_current_projection_changed := (
      v_expected_paye_net_state_hash IS DISTINCT FROM v_current_paye_net_state_hash
      OR v_expected_global_bank_payment_projection_hash IS DISTINCT FROM v_all_bank_payment_projection_hash
      OR v_expected_scoped_paye_net_state_hash IS DISTINCT FROM v_current_scoped_paye_net_state_hash
      OR v_expected_bank_payment_projection_hash IS DISTINCT FROM v_current_bank_payment_projection_hash
      OR v_expected_missing_count IS DISTINCT FROM v_current_missing_count
      OR v_expected_global_zero_count IS DISTINCT FROM v_global_zero_count
      OR v_expected_global_positive_count IS DISTINCT FROM v_global_positive_count
      OR v_expected_global_invalid_count IS DISTINCT FROM v_global_invalid_count
      OR v_expected_scoped_missing_count IS DISTINCT FROM v_current_scoped_missing_count
      OR v_expected_zero_count IS DISTINCT FROM v_current_zero_count
      OR v_expected_positive_count IS DISTINCT FROM v_current_positive_count
      OR v_expected_scoped_invalid_count IS DISTINCT FROM v_current_scoped_invalid_count
    );

    v_projection_diagnostic_json := JSONB_BUILD_OBJECT(
      'projection_changed_after_authorisation', v_current_projection_changed,
      'settlement_blocked_by_projection_change', false,
      'authorised_global_paye_net_state_hash', v_expected_paye_net_state_hash,
      'current_global_paye_net_state_hash', v_current_paye_net_state_hash,
      'authorised_global_bank_payment_projection_hash', v_expected_global_bank_payment_projection_hash,
      'current_global_bank_payment_projection_hash', v_all_bank_payment_projection_hash,
      'authorised_scoped_paye_net_state_hash', v_expected_scoped_paye_net_state_hash,
      'current_scoped_paye_net_state_hash', v_current_scoped_paye_net_state_hash,
      'authorised_scoped_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
      'current_scoped_bank_payment_projection_hash', v_current_bank_payment_projection_hash,
      'authorised_scope', v_operation_projection_scope,
      'diagnostic_generated_at_utc', v_now::text
    );

    v_no_bank_scope_authorised := v_allow_explicit_zero_no_bank_scopes_marker
      AND COALESCE(v_expected_missing_count, -1) = 0
      AND COALESCE(v_expected_scoped_missing_count, -1) = 0
      AND COALESCE(v_expected_zero_count, 0) > 0
      AND COALESCE(v_expected_scoped_invalid_count, -1) = 0;
    v_no_bank_payment_execution_validated := v_no_bank_payment_marker
      AND v_scoped_no_transfer_marker
      AND v_no_bank_scope_authorised
      AND COALESCE(v_expected_positive_count, -1) = 0
      AND COALESCE(v_expected_global_positive_count, -1) = 0
      AND COALESCE(v_expected_global_invalid_count, -1) = 0;
    v_local_no_bank_commit_ref := 'NO_BANK_PAYMENT:' || v_operation_auth_request_id::text;
    v_proof_validation_outcome := CASE
      WHEN v_no_bank_payment_execution_validated THEN 'VALIDATED_NO_BANK_PAYMENT_FROM_AUTHORISED_INTENT'
      WHEN v_no_bank_scope_authorised AND COALESCE(v_expected_positive_count, 0) > 0 THEN 'VALIDATED_MIXED_ZERO_AND_TRANSFER_FROM_AUTHORISED_INTENT'
      WHEN v_no_bank_scope_authorised THEN 'VALIDATED_SCOPED_NO_TRANSFER_FROM_AUTHORISED_INTENT'
      ELSE 'VALIDATED_TRANSFER_BACKED_FROM_AUTHORISED_INTENT'
    END;

    IF v_no_bank_scope_authorised
       AND v_execution_operation_row.operation_type <> 'PAYMENT_EXECUTE' THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'NO_BANK_PAYMENT_EXECUTION_OPERATION_REQUIRED',
        'message', 'Explicit-zero no-bank settlement must remain bound to the original PAYMENT_EXECUTE operation and cannot be introduced by a blocked-funds retry.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'execution_operation_type', v_execution_operation_row.operation_type
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF v_no_bank_payment_execution_validated THEN
      SELECT COUNT(*)::integer
      INTO v_no_bank_scope_artifact_count
      FROM public.banking_pay_operation_transfer_scope AS transfer_scope_row
      WHERE transfer_scope_row.operation_id = v_execution_operation_id
        AND transfer_scope_row.pay_batch_id = p_pay_batch_id;

      SELECT COUNT(*)::integer
      INTO v_no_bank_batch_transfer_count
      FROM public.pay_bank_transfers AS transfer_row
      WHERE transfer_row.pay_batch_id = p_pay_batch_id;

      SELECT COUNT(*)::integer
      INTO v_no_bank_transfer_event_count
      FROM public.pay_bank_transfer_events AS transfer_event_row
      WHERE transfer_event_row.pay_batch_id = p_pay_batch_id;

      SELECT COUNT(*)::integer
      INTO v_no_bank_provider_attempt_count
      FROM public.banking_pay_operation_provider_attempts AS provider_attempt_row
      WHERE provider_attempt_row.pay_batch_id = p_pay_batch_id
         OR provider_attempt_row.operation_id = v_execution_operation_id;

      IF COALESCE(v_no_bank_scope_artifact_count, 0) <> 0
         OR COALESCE(v_no_bank_batch_transfer_count, 0) <> 0
         OR COALESCE(v_no_bank_transfer_event_count, 0) <> 0
         OR COALESCE(v_no_bank_provider_attempt_count, 0) <> 0 THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'NO_BANK_PAYMENT_EXECUTION_EVIDENCE_CONFLICT',
          'message', 'No-bank payment settlement cannot continue because transfer or provider-submission evidence already exists.',
          'pay_batch_id', p_pay_batch_id::text,
          'operation_id', p_operation_id::text,
          'execution_operation_id', v_execution_operation_id::text,
          'transfer_scope_count', COALESCE(v_no_bank_scope_artifact_count, 0),
          'bank_transfer_count', COALESCE(v_no_bank_batch_transfer_count, 0),
          'bank_transfer_event_count', COALESCE(v_no_bank_transfer_event_count, 0),
          'provider_attempt_count', COALESCE(v_no_bank_provider_attempt_count, 0)
        )::text USING ERRCODE = 'P0001';
      END IF;

      IF UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED'))) NOT IN ('NOT_SUBMITTED', 'COMMITTED')
         OR (
           NULLIF(BTRIM(COALESCE(v_batch.execution_commit_ref, '')), '') IS NOT NULL
           AND NULLIF(BTRIM(COALESCE(v_batch.execution_commit_ref, '')), '') <> v_local_no_bank_commit_ref
         )
         OR (
           UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED'))) = 'COMMITTED'
           AND (
             NULLIF(BTRIM(COALESCE(v_batch.execution_commit_ref, '')), '') IS DISTINCT FROM v_local_no_bank_commit_ref
             OR v_batch.execution_committed_at_utc IS NULL
           )
         )
         OR (
           UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
           AND v_batch.execution_committed_at_utc IS NOT NULL
         ) THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'NO_BANK_PAYMENT_EXECUTION_COMMIT_CONFLICT',
          'message', 'The batch contains execution commit evidence that is incompatible with the authorised no-bank-payment execution.',
          'pay_batch_id', p_pay_batch_id::text,
          'operation_id', p_operation_id::text,
          'execution_commit_state', v_batch.execution_commit_state,
          'execution_commit_ref', v_batch.execution_commit_ref,
          'expected_local_commit_reference', v_local_no_bank_commit_ref
        )::text USING ERRCODE = 'P0001';
      END IF;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_requested_scope_ids;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_requested_scope_ids (
      settlement_scope_id uuid PRIMARY KEY
    ) ON COMMIT DROP;

    INSERT INTO pg_temp.tmp_pay_settle_rail_requested_scope_ids (settlement_scope_id)
    SELECT DISTINCT (scope_element.value #>> '{}')::uuid
    FROM JSONB_ARRAY_ELEMENTS(p_settlement_scope_ids) AS scope_element(value)
    WHERE (scope_element.value #>> '{}')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COUNT(*)::integer
    INTO v_requested_scope_count
    FROM pg_temp.tmp_pay_settle_rail_requested_scope_ids AS requested_scope_id;

    IF v_requested_scope_count <> JSONB_ARRAY_LENGTH(p_settlement_scope_ids) THEN
      RAISE EXCEPTION 'p_settlement_scope_ids contains invalid or duplicate uuid values';
    END IF;

    PERFORM 1
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    JOIN pg_temp.tmp_pay_settle_rail_requested_scope_ids AS requested_scope_id
      ON requested_scope_id.settlement_scope_id = scope_row.id
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
    FOR UPDATE OF scope_row;

    SELECT
      COUNT(*)::integer,
      COUNT(*) FILTER (WHERE scope_row.status = 'SETTLED')::integer,
      COUNT(*) FILTER (WHERE scope_row.status = 'FAILED')::integer
    INTO
      v_matched_scope_count,
      v_reused_this_chunk,
      v_existing_failed_scope_count
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    JOIN pg_temp.tmp_pay_settle_rail_requested_scope_ids AS requested_scope_id
      ON requested_scope_id.settlement_scope_id = scope_row.id
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id;

    v_failed_this_chunk := COALESCE(v_existing_failed_scope_count, 0);

    IF v_matched_scope_count <> v_requested_scope_count THEN
      RAISE EXCEPTION 'one or more settlement scope ids do not belong to operation % and batch %', p_operation_id, p_pay_batch_id;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_eligible_no_bank_scope;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_eligible_no_bank_scope
    ON COMMIT DROP
    AS
    WITH frozen_scope AS (
      SELECT
        scope_row.id AS settlement_scope_id,
        scope_row.status,
        scope_row.pay_batch_candidate_id,
        scope_row.candidate_id,
        scope_row.payload_json
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND scope_row.status IN ('PENDING', 'SETTLED')
        AND scope_row.settlement_event_id IS NULL
        AND v_no_bank_scope_authorised
        AND UPPER(BTRIM(COALESCE(scope_row.pay_channel, ''))) = 'PAYE'
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_batch_id', '')), '') = p_pay_batch_id::text
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_batch_candidate_id', '')), '') = scope_row.pay_batch_candidate_id::text
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'candidate_id', '')), '') = scope_row.candidate_id::text
        AND UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_channel', '')), '')) = 'PAYE'
        AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
        AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
        AND LOWER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_scope', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(scope_row.payload_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_bank_transfer_id', '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'execution_operation_id', '')), '') = v_execution_operation_id::text
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'settlement_operation_id', '')), '') = p_operation_id::text
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'auth_request_id', '')), '') = v_operation_auth_request_id::text
        AND UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_execution_mode', '')), '')) IS NOT DISTINCT FROM v_authorised_execution_mode
        AND UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_scope', scope_row.payload_json->>'projection_scope', '')), '')) IS NOT DISTINCT FROM v_operation_projection_scope
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_paye_net_state_hash', scope_row.payload_json->>'paye_net_state_hash', '')), '') = v_expected_paye_net_state_hash
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'global_bank_payment_projection_hash', '')), '') = v_expected_global_bank_payment_projection_hash
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'scoped_paye_net_state_hash', '')), '') = v_expected_scoped_paye_net_state_hash
        AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_bank_payment_projection_hash', scope_row.payload_json->>'bank_payment_projection_hash', '')), '') = v_expected_bank_payment_projection_hash
        AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'scoped_no_transfer_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_scoped_no_transfer_marker
        AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_no_bank_payment_marker
        AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_allow_explicit_zero_no_bank_scopes_marker
        AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_suppress_remittances
        AND COALESCE(scope_row.payload_json->>'authorised_missing_explicit_paye_input_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_missing_explicit_paye_input_count')::integer = v_expected_missing_count
        AND COALESCE(scope_row.payload_json->>'authorised_explicit_zero_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_explicit_zero_count')::integer = v_expected_global_zero_count
        AND COALESCE(scope_row.payload_json->>'authorised_global_positive_bank_payment_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_global_positive_bank_payment_count')::integer = v_expected_global_positive_count
        AND COALESCE(scope_row.payload_json->>'authorised_global_invalid_payment_row_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_global_invalid_payment_row_count')::integer = v_expected_global_invalid_count
        AND COALESCE(scope_row.payload_json->>'authorised_scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_scoped_missing_explicit_paye_input_count')::integer = v_expected_scoped_missing_count
        AND COALESCE(scope_row.payload_json->>'authorised_scoped_explicit_zero_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_scoped_explicit_zero_count')::integer = v_expected_zero_count
        AND COALESCE(scope_row.payload_json->>'authorised_scoped_positive_bank_payment_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_scoped_positive_bank_payment_count')::integer = v_expected_positive_count
        AND COALESCE(scope_row.payload_json->>'authorised_scoped_invalid_payment_row_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'authorised_scoped_invalid_payment_row_count')::integer = v_expected_scoped_invalid_count
        AND JSONB_TYPEOF(scope_row.payload_json->'pay_batch_item_ids') = 'array'
        AND JSONB_ARRAY_LENGTH(scope_row.payload_json->'pay_batch_item_ids') > 0
        AND COALESCE(scope_row.payload_json->>'item_count', '') ~ '^[0-9]+$'
        AND (scope_row.payload_json->>'item_count')::integer = JSONB_ARRAY_LENGTH(scope_row.payload_json->'pay_batch_item_ids')
        AND NOT EXISTS (
          SELECT 1
          FROM JSONB_ARRAY_ELEMENTS_TEXT(scope_row.payload_json->'pay_batch_item_ids') AS item_element(item_id_text)
          WHERE item_element.item_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             OR NOT EXISTS (
               SELECT 1
               FROM public.pay_batch_items AS payload_item
               WHERE payload_item.id = CASE
                       WHEN item_element.item_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                         THEN item_element.item_id_text::uuid
                       ELSE NULL::uuid
                     END
                 AND payload_item.pay_batch_candidate_id = scope_row.pay_batch_candidate_id
                 AND UPPER(BTRIM(COALESCE(payload_item.pay_channel, ''))) = 'PAYE'
                 AND COALESCE(payload_item.is_voided, false) = false
                 AND COALESCE(payload_item.item_type, '') <> 'DEBT_CREATED'
                 AND payload_item.pay_bank_transfer_id IS NULL
             )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS candidate_scope_item
          WHERE candidate_scope_item.pay_batch_candidate_id = scope_row.pay_batch_candidate_id
            AND UPPER(BTRIM(COALESCE(candidate_scope_item.pay_channel, ''))) = 'PAYE'
            AND COALESCE(candidate_scope_item.is_voided, false) = false
            AND COALESCE(candidate_scope_item.item_type, '') <> 'DEBT_CREATED'
            AND NOT (scope_row.payload_json->'pay_batch_item_ids' @> JSONB_BUILD_ARRAY(TO_JSONB(candidate_scope_item.id::text)))
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS linked_item
          WHERE linked_item.pay_batch_candidate_id = scope_row.pay_batch_candidate_id
            AND UPPER(BTRIM(COALESCE(linked_item.pay_channel, ''))) = 'PAYE'
            AND COALESCE(linked_item.is_voided, false) = false
            AND linked_item.pay_bank_transfer_id IS NOT NULL
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_transfer_scope AS transfer_scope_evidence
          WHERE transfer_scope_evidence.pay_batch_id = p_pay_batch_id
            AND (
              transfer_scope_evidence.candidate_id = scope_row.candidate_id
              OR transfer_scope_evidence.candidate_id IS NULL
            )
            AND UPPER(BTRIM(COALESCE(transfer_scope_evidence.pay_channel, ''))) = 'PAYE'
            AND (
              transfer_scope_evidence.pay_bank_transfer_id IS NOT NULL
              OR transfer_scope_evidence.provider_submit_attempt_count > 0
              OR transfer_scope_evidence.provider_request_sent_at_utc IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(transfer_scope_evidence.provider_request_id, '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(transfer_scope_evidence.provider_transaction_id, '')), '') IS NOT NULL
            )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_bank_transfers AS transfer_evidence
          WHERE transfer_evidence.pay_batch_id = p_pay_batch_id
            AND (
              transfer_evidence.candidate_id = scope_row.candidate_id
              OR transfer_evidence.candidate_id IS NULL
            )
            AND UPPER(BTRIM(COALESCE(transfer_evidence.pay_channel, ''))) = 'PAYE'
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS transfer_event_evidence
          LEFT JOIN public.pay_bank_transfers AS event_transfer_evidence
            ON event_transfer_evidence.id = transfer_event_evidence.pay_bank_transfer_id
          WHERE transfer_event_evidence.pay_batch_id = p_pay_batch_id
            AND (
              COALESCE(transfer_event_evidence.candidate_id, event_transfer_evidence.candidate_id) = scope_row.candidate_id
              OR (
                transfer_event_evidence.candidate_id IS NULL
                AND transfer_event_evidence.pay_bank_transfer_id IS NULL
              )
            )
            AND (
              transfer_event_evidence.pay_bank_transfer_id IS NULL
              OR UPPER(BTRIM(COALESCE(event_transfer_evidence.pay_channel, ''))) = 'PAYE'
            )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_provider_attempts AS provider_attempt_evidence
          LEFT JOIN public.banking_pay_operation_transfer_scope AS provider_attempt_scope
            ON provider_attempt_scope.id = provider_attempt_evidence.transfer_scope_id
          WHERE (
              provider_attempt_evidence.pay_batch_id = p_pay_batch_id
              OR provider_attempt_scope.pay_batch_id = p_pay_batch_id
            )
            AND (
              (
                provider_attempt_scope.candidate_id = scope_row.candidate_id
                AND UPPER(BTRIM(COALESCE(provider_attempt_scope.pay_channel, ''))) = 'PAYE'
              )
              OR (
                provider_attempt_evidence.transfer_scope_id IS NULL
                AND (
                  provider_attempt_evidence.operation_id = v_execution_operation_id
                  OR provider_attempt_evidence.pay_batch_id = p_pay_batch_id
                )
              )
            )
        )
    ), current_zero AS (
      SELECT DISTINCT
        projection_row.pay_batch_candidate_id,
        projection_row.candidate_id,
        projection_row.effective_paye_net_input_id
      FROM pg_temp.tmp_pay_settle_rail_scoped_projection_rows AS projection_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = projection_row.pay_batch_candidate_id
       AND batch_candidate.pay_batch_id = p_pay_batch_id
      WHERE projection_row.is_paye_net_state_row
        AND projection_row.pay_channel = 'PAYE'
        AND projection_row.has_effective_paye_input IS TRUE
        AND projection_row.effective_paye_net_input_id IS NOT NULL
        AND projection_row.paye_net_classification = 'ZERO'
        AND projection_row.final_frozen_bank_amount IS NOT NULL
        AND ROUND(projection_row.final_frozen_bank_amount, 2) = 0
        AND batch_candidate.net_bank_amount IS NOT NULL
        AND ROUND(batch_candidate.net_bank_amount, 2) = 0
    )
    SELECT
      frozen_scope.settlement_scope_id,
      frozen_scope.pay_batch_candidate_id,
      frozen_scope.candidate_id
    FROM frozen_scope
    LEFT JOIN current_zero
      ON current_zero.pay_batch_candidate_id = frozen_scope.pay_batch_candidate_id
     AND current_zero.candidate_id = frozen_scope.candidate_id
    WHERE frozen_scope.status = 'SETTLED'
       OR (
         frozen_scope.status = 'PENDING'
         AND current_zero.pay_batch_candidate_id IS NOT NULL
         AND (
           v_no_bank_payment_marker IS NOT TRUE
           OR v_current_projection_changed IS NOT TRUE
         )
         AND NULLIF(BTRIM(COALESCE(frozen_scope.payload_json->>'effective_paye_net_input_id', '')), '') = current_zero.effective_paye_net_input_id::text
         AND COALESCE(frozen_scope.payload_json->>'net_bank_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
         AND ROUND((frozen_scope.payload_json->>'net_bank_amount')::numeric, 2) = 0
       );


    SELECT
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
      )::integer,
      COUNT(eligible_scope.settlement_scope_id)::integer
    INTO
      v_selected_no_bank_scope_count,
      v_selected_eligible_no_bank_scope_count
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    JOIN pg_temp.tmp_pay_settle_rail_requested_scope_ids AS requested_scope_id
      ON requested_scope_id.settlement_scope_id = scope_row.id
    LEFT JOIN pg_temp.tmp_pay_settle_rail_eligible_no_bank_scope AS eligible_scope
      ON eligible_scope.settlement_scope_id = scope_row.id
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id;

    IF COALESCE(v_selected_no_bank_scope_count, 0) <> COALESCE(v_selected_eligible_no_bank_scope_count, 0) THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'NO_BANK_SETTLEMENT_SCOPE_PROOF_INVALID',
        'message', 'One or more requested no-bank settlement scopes are not bound to the authorised execution proof or contain transfer/provider evidence.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text,
        'requested_no_bank_scope_count', COALESCE(v_selected_no_bank_scope_count, 0),
        'eligible_no_bank_scope_count', COALESCE(v_selected_eligible_no_bank_scope_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF EXISTS (
      SELECT 1
      FROM JSONB_ARRAY_ELEMENTS(p_settlement_json) AS settlement_element(value)
      WHERE JSONB_TYPEOF(settlement_element.value) <> 'object'
    ) THEN
      RAISE EXCEPTION 'pay_settle_rail: settlement_json must contain only JSON objects';
    END IF;

    IF JSONB_ARRAY_LENGTH(p_settlement_json) = 0
       AND EXISTS (
         SELECT 1
         FROM public.banking_pay_operation_settlement_scope AS pending_requested_scope
         JOIN pg_temp.tmp_pay_settle_rail_requested_scope_ids AS pending_requested_scope_id
           ON pending_requested_scope_id.settlement_scope_id = pending_requested_scope.id
         WHERE pending_requested_scope.operation_id = p_operation_id
           AND pending_requested_scope.pay_batch_id = p_pay_batch_id
           AND pending_requested_scope.status = 'PENDING'
           AND NOT EXISTS (
             SELECT 1
             FROM pg_temp.tmp_pay_settle_rail_eligible_no_bank_scope AS pending_eligible_no_bank_scope
             WHERE pending_eligible_no_bank_scope.settlement_scope_id = pending_requested_scope.id
           )
       ) THEN
      RAISE EXCEPTION 'PAY_SETTLE_RAIL_TRANSFER_PAYLOAD_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = JSONB_BUILD_OBJECT(
                'code', 'PAY_SETTLE_RAIL_TRANSFER_PAYLOAD_REQUIRED',
                'pay_batch_id', p_pay_batch_id::text,
                'operation_id', p_operation_id::text,
                'requested_scope_count', COALESCE(v_requested_scope_count, 0),
                'requested_no_bank_scope_count', COALESCE(v_selected_no_bank_scope_count, 0),
                'eligible_no_bank_scope_count', COALESCE(v_selected_eligible_no_bank_scope_count, 0),
                'message', 'An empty rail settlement payload is valid only when every requested scope that is still pending is an authorised explicit-zero no-bank-payment scope; already-terminal scopes remain idempotently reusable.'
              )::text;
    END IF;

    CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_operation_rail_settlement_updates (
      transfer_id uuid NULL,
      status text NOT NULL,
      rail_tx_id text NULL,
      rail_state text NULL,
      rail_meta_json jsonb NULL,
      provider_reference text NULL,
      provider_state text NULL,
      event_time_utc timestamptz NULL,
      input_json jsonb NOT NULL
    ) ON COMMIT DROP;
    TRUNCATE TABLE pg_temp.tmp_operation_rail_settlement_updates;

    INSERT INTO pg_temp.tmp_operation_rail_settlement_updates (
      transfer_id,
      status,
      rail_tx_id,
      rail_state,
      rail_meta_json,
      provider_reference,
      provider_state,
      event_time_utc,
      input_json
    )
    SELECT
      CASE
        WHEN NULLIF(BTRIM(COALESCE(update_element.value->>'transfer_id', update_element.value->>'pay_bank_transfer_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(update_element.value->>'transfer_id', update_element.value->>'pay_bank_transfer_id', '')), '')::uuid
        ELSE NULL::uuid
      END,
      CASE
        WHEN upper(BTRIM(COALESCE(update_element.value->>'status', update_element.value->>'normalised_state', update_element.value->>'normalized_state', 'UNKNOWN'))) IN ('SUCCESS', 'SUCCEEDED', 'SETTLED', 'PAID') THEN 'COMPLETED'
        WHEN upper(BTRIM(COALESCE(update_element.value->>'status', update_element.value->>'normalised_state', update_element.value->>'normalized_state', 'UNKNOWN'))) IN ('CANCELED', 'CANCELLED') THEN 'CANCELLED'
        WHEN upper(BTRIM(COALESCE(update_element.value->>'status', update_element.value->>'normalised_state', update_element.value->>'normalized_state', 'UNKNOWN'))) IN ('REVERSED', 'REVERTED') THEN 'RETURNED'
        WHEN upper(BTRIM(COALESCE(update_element.value->>'status', update_element.value->>'normalised_state', update_element.value->>'normalized_state', 'UNKNOWN'))) IN ('SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT', 'DECLINED', 'REJECTED') THEN 'FAILED'
        ELSE upper(BTRIM(COALESCE(update_element.value->>'status', update_element.value->>'normalised_state', update_element.value->>'normalized_state', 'UNKNOWN')))
      END,
      NULLIF(BTRIM(COALESCE(update_element.value->>'rail_tx_id', '')), ''),
      NULLIF(BTRIM(COALESCE(update_element.value->>'rail_state', update_element.value->>'provider_state', '')), ''),
      CASE WHEN update_element.value ? 'rail_meta_json' THEN update_element.value->'rail_meta_json' ELSE '{}'::jsonb END,
      NULLIF(BTRIM(COALESCE(
        update_element.value->>'provider_reference',
        update_element.value->>'provider_event_id',
        update_element.value->>'provider_submission_id',
        update_element.value->>'submission_id',
        update_element.value->>'rail_submission_id',
        update_element.value->>'provider_transfer_id',
        update_element.value->>'provider_payment_id',
        update_element.value->>'payment_id',
        update_element.value->>'external_payment_id',
        update_element.value->>'revolut_payment_id',
        update_element.value->>'external_transfer_id',
        update_element.value->>'provider_transaction_id',
        update_element.value->>'transaction_id',
        update_element.value #>> '{rail_meta_json,provider_reference}',
        update_element.value #>> '{rail_meta_json,provider_event_id}',
        update_element.value #>> '{rail_meta_json,provider_submission_id}',
        update_element.value #>> '{rail_meta_json,submission_id}',
        update_element.value #>> '{rail_meta_json,rail_submission_id}',
        update_element.value #>> '{rail_meta_json,provider_transfer_id}',
        update_element.value #>> '{rail_meta_json,provider_payment_id}',
        update_element.value #>> '{rail_meta_json,payment_id}',
        update_element.value #>> '{rail_meta_json,external_payment_id}',
        update_element.value #>> '{rail_meta_json,revolut_payment_id}',
        update_element.value #>> '{rail_meta_json,external_transfer_id}',
        update_element.value #>> '{rail_meta_json,provider_transaction_id}',
        update_element.value #>> '{rail_meta_json,transaction_id}',
        update_element.value->>'rail_tx_id',
        ''
      )), ''),
      NULLIF(BTRIM(COALESCE(update_element.value->>'provider_state', update_element.value->>'rail_state', update_element.value->>'status', '')), ''),
      CASE WHEN NULLIF(BTRIM(COALESCE(update_element.value->>'event_time_utc', '')), '') IS NOT NULL THEN NULLIF(BTRIM(COALESCE(update_element.value->>'event_time_utc', '')), '')::timestamptz ELSE v_now END,
      update_element.value
    FROM jsonb_array_elements(p_settlement_json) AS update_element(value)
    WHERE jsonb_typeof(update_element.value) = 'object';

    IF EXISTS (SELECT 1 FROM pg_temp.tmp_operation_rail_settlement_updates AS invalid_update WHERE invalid_update.transfer_id IS NULL LIMIT 1) THEN
      RAISE EXCEPTION 'pay_settle_rail: settlement_json contains an invalid or missing transfer_id';
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_operation_rail_settlement_classification;
    CREATE TEMP TABLE tmp_operation_rail_settlement_classification ON COMMIT DROP AS
    SELECT
      update_rows.transfer_id,
      classification_rows.cash_state,
      classification_rows.normalised_transfer_status,
      classification_rows.is_final_money_moved,
      classification_rows.is_terminal_no_money,
      classification_rows.is_pending_non_final,
      classification_rows.completed_at_allowed,
      classification_rows.reason,
      classification_rows.support_details_json
    FROM pg_temp.tmp_operation_rail_settlement_updates AS update_rows
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      update_rows.status,
      COALESCE(update_rows.rail_state, update_rows.provider_state),
      COALESCE(update_rows.rail_meta_json, '{}'::jsonb) || COALESCE(update_rows.input_json, '{}'::jsonb),
      COALESCE(update_rows.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
        'provider_state', update_rows.provider_state,
        'provider_reference', update_rows.provider_reference,
        'settlement_operation_id', p_operation_id::text
      )
    ) AS classification_rows;

    UPDATE pg_temp.tmp_operation_rail_settlement_updates AS update_rows
    SET
      status = CASE
        WHEN classification_rows.is_final_money_moved THEN 'COMPLETED'
        WHEN classification_rows.is_terminal_no_money AND upper(COALESCE(update_rows.status, '')) IN ('CANCELLED','CANCELED') THEN 'CANCELLED'
        WHEN classification_rows.is_terminal_no_money THEN 'FAILED'
        WHEN classification_rows.is_pending_non_final THEN CASE WHEN upper(COALESCE(update_rows.status, '')) IN ('PROCESSING','SUBMITTED','SENT','ACCEPTED') THEN 'PROCESSING' ELSE 'PENDING' END
        ELSE 'UNKNOWN'
      END,
      rail_meta_json = COALESCE(update_rows.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
        'money_movement_classification', jsonb_build_object(
          'cash_state', classification_rows.cash_state,
          'normalised_transfer_status', classification_rows.normalised_transfer_status,
          'is_final_money_moved', classification_rows.is_final_money_moved,
          'is_terminal_no_money', classification_rows.is_terminal_no_money,
          'is_pending_non_final', classification_rows.is_pending_non_final,
          'completed_at_allowed', classification_rows.completed_at_allowed,
          'reason', classification_rows.reason
        ),
        'settlement_final_paid_required', true
      )
    FROM pg_temp.tmp_operation_rail_settlement_classification AS classification_rows
    WHERE classification_rows.transfer_id = update_rows.transfer_id;

    SELECT COALESCE(jsonb_agg(to_jsonb(duplicate_update.transfer_id::text) ORDER BY duplicate_update.transfer_id::text), '[]'::jsonb)
    INTO v_duplicate_transfer_ids
    FROM (
      SELECT duplicate_source.transfer_id
      FROM pg_temp.tmp_operation_rail_settlement_updates AS duplicate_source
      GROUP BY duplicate_source.transfer_id
      HAVING COUNT(*) > 1
    ) AS duplicate_update;

    IF jsonb_array_length(v_duplicate_transfer_ids) > 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_SETTLE_RAIL_DUPLICATE_TRANSFER_UPDATE',
        'message', 'A rail settlement update chunk cannot contain duplicate transfer IDs.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'duplicate_transfer_ids', v_duplicate_transfer_ids
      )::text;
    END IF;

    WITH requested_scope AS (
      SELECT DISTINCT (scope_element.value #>> '{}')::uuid AS settlement_scope_id
      FROM jsonb_array_elements(p_settlement_scope_ids) AS scope_element(value)
      WHERE (scope_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ), selected_scope AS (
      SELECT scope_row.*
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      JOIN requested_scope
        ON requested_scope.settlement_scope_id = scope_row.id
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
    ), payload_transfer AS (
      SELECT selected_scope.id AS settlement_scope_id,
             CASE
               WHEN COALESCE(selected_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                 THEN (selected_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}')::uuid
               ELSE NULL::uuid
             END AS pay_bank_transfer_id
      FROM selected_scope
    ), payload_item_ids AS (
      SELECT selected_scope.id AS settlement_scope_id,
             (item_element.value #>> '{}')::uuid AS pay_batch_item_id
      FROM selected_scope
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(selected_scope.payload_json->'pay_batch_item_ids', '[]'::jsonb)) AS item_element(value)
      WHERE (item_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ), scope_transfers AS (
      SELECT DISTINCT selected_scope.id AS settlement_scope_id,
             COALESCE(payload_transfer.pay_bank_transfer_id, batch_item.pay_bank_transfer_id) AS pay_bank_transfer_id
      FROM selected_scope
      LEFT JOIN payload_transfer
        ON payload_transfer.settlement_scope_id = selected_scope.id
      LEFT JOIN payload_item_ids
        ON payload_item_ids.settlement_scope_id = selected_scope.id
      LEFT JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = payload_item_ids.pay_batch_item_id
      WHERE COALESCE(payload_transfer.pay_bank_transfer_id, batch_item.pay_bank_transfer_id) IS NOT NULL
    ), out_of_scope_updates AS (
      SELECT DISTINCT update_row.transfer_id
      FROM pg_temp.tmp_operation_rail_settlement_updates AS update_row
      WHERE NOT EXISTS (
        SELECT 1
        FROM scope_transfers
        WHERE scope_transfers.pay_bank_transfer_id = update_row.transfer_id
      )
    )
    SELECT COALESCE(jsonb_agg(to_jsonb(out_of_scope_updates.transfer_id::text) ORDER BY out_of_scope_updates.transfer_id::text), '[]'::jsonb)
    INTO v_out_of_scope_transfer_ids
    FROM out_of_scope_updates;

    IF jsonb_array_length(v_out_of_scope_transfer_ids) > 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_SETTLE_RAIL_OUT_OF_SCOPE_TRANSFER_UPDATE',
        'message', 'A rail settlement update chunk contained transfer IDs that are not part of the supplied settlement scope rows.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'out_of_scope_transfer_ids', v_out_of_scope_transfer_ids
      )::text;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM pg_temp.tmp_operation_rail_settlement_updates AS update_row
      JOIN public.pay_bank_transfers AS bank_transfer
        ON bank_transfer.id = update_row.transfer_id
       AND bank_transfer.pay_batch_id = p_pay_batch_id
      WHERE update_row.status = 'COMPLETED'
        AND NOT EXISTS (
          SELECT 1
          FROM (VALUES
            (update_row.provider_reference),
            (update_row.rail_tx_id),
            (update_row.rail_meta_json #>> '{provider_reference}'),
            (update_row.rail_meta_json #>> '{provider_event_id}'),
            (update_row.rail_meta_json #>> '{provider_submission_id}'),
            (update_row.rail_meta_json #>> '{submission_id}'),
            (update_row.rail_meta_json #>> '{rail_submission_id}'),
            (update_row.rail_meta_json #>> '{provider_payment_id}'),
            (update_row.rail_meta_json #>> '{payment_id}'),
            (update_row.rail_meta_json #>> '{external_payment_id}'),
            (update_row.rail_meta_json #>> '{revolut_payment_id}'),
            (update_row.rail_meta_json #>> '{provider_transfer_id}'),
            (update_row.rail_meta_json #>> '{transfer_id}'),
            (update_row.rail_meta_json #>> '{external_transfer_id}'),
            (update_row.rail_meta_json #>> '{provider_transaction_id}'),
            (update_row.rail_meta_json #>> '{transaction_id}')
          ) AS provider_identifier(identifier_value)
          WHERE NULLIF(BTRIM(COALESCE(provider_identifier.identifier_value, '')), '') IS NOT NULL
            AND NOT (
              NULLIF(BTRIM(COALESCE(provider_identifier.identifier_value, '')), '') = ANY(
                ARRAY_REMOVE(ARRAY[
                  bank_transfer.id::text,
                  NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''),
                  NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''),
                  NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''),
                  NULLIF(BTRIM(COALESCE(bank_transfer.rail_meta_json #>> '{request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(bank_transfer.rail_meta_json #>> '{idempotency_key}', '')), ''),
                  NULLIF(BTRIM(COALESCE(bank_transfer.rail_meta_json #>> '{payment_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(bank_transfer.rail_meta_json #>> '{bulk_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.input_json #>> '{request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.input_json #>> '{idempotency_key}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.input_json #>> '{payment_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.input_json #>> '{bulk_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.rail_meta_json #>> '{request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.rail_meta_json #>> '{idempotency_key}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.rail_meta_json #>> '{payment_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(update_row.rail_meta_json #>> '{bulk_reference}', '')), '')
                ]::text[], NULL::text)
              )
            )
        )
      LIMIT 1
    ) THEN
      RAISE EXCEPTION 'PAY_SETTLE_RAIL_PROVIDER_EVIDENCE_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_SETTLE_RAIL_PROVIDER_EVIDENCE_REQUIRED',
                'pay_batch_id', p_pay_batch_id::text,
                'operation_id', p_operation_id::text,
                'message', 'STANDARD_BANK rail settlement marked as completed requires genuine provider or bank evidence, not local request/payment references.'
              )::text;
    END IF;

    WITH settled_scope_for_event_repair AS (
      SELECT
        scope_row.id AS settlement_scope_id,
        scope_row.pay_batch_candidate_id,
        scope_row.payload_json
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      JOIN pg_temp.tmp_pay_settle_rail_requested_scope_ids AS requested_scope_id
        ON requested_scope_id.settlement_scope_id = scope_row.id
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
        AND scope_row.settlement_event_id IS NULL
        AND NOT (
          UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
        )
      FOR UPDATE OF scope_row
    ), direct_scope_transfer AS (
      SELECT
        settled_scope.settlement_scope_id,
        CASE
          WHEN COALESCE(
            NULLIF(BTRIM(COALESCE(settled_scope.payload_json->>'pay_bank_transfer_id', '')), ''),
            NULLIF(BTRIM(COALESCE(settled_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '')
          ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN COALESCE(
              NULLIF(BTRIM(COALESCE(settled_scope.payload_json->>'pay_bank_transfer_id', '')), ''),
              NULLIF(BTRIM(COALESCE(settled_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '')
            )::uuid
          ELSE NULL::uuid
        END AS pay_bank_transfer_id
      FROM settled_scope_for_event_repair AS settled_scope
    ), item_scope_transfer AS (
      SELECT DISTINCT
        settled_scope.settlement_scope_id,
        batch_item.pay_bank_transfer_id
      FROM settled_scope_for_event_repair AS settled_scope
      CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
        CASE
          WHEN JSONB_TYPEOF(settled_scope.payload_json->'pay_batch_item_ids') = 'array'
            THEN settled_scope.payload_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_element(value)
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = CASE
             WHEN (item_element.value #>> '{}')
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN (item_element.value #>> '{}')::uuid
             ELSE NULL::uuid
           END
       AND batch_item.pay_batch_candidate_id = settled_scope.pay_batch_candidate_id
       AND COALESCE(batch_item.is_voided, false) = false
      WHERE batch_item.pay_bank_transfer_id IS NOT NULL
    ), scope_transfer_candidate AS (
      SELECT
        direct_scope_transfer.settlement_scope_id,
        direct_scope_transfer.pay_bank_transfer_id
      FROM direct_scope_transfer
      WHERE direct_scope_transfer.pay_bank_transfer_id IS NOT NULL

      UNION

      SELECT
        item_scope_transfer.settlement_scope_id,
        item_scope_transfer.pay_bank_transfer_id
      FROM item_scope_transfer
      WHERE item_scope_transfer.pay_bank_transfer_id IS NOT NULL
    ), resolved_scope_transfer AS (
      SELECT
        scope_transfer_candidate.settlement_scope_id,
        (ARRAY_AGG(scope_transfer_candidate.pay_bank_transfer_id ORDER BY scope_transfer_candidate.pay_bank_transfer_id))[1] AS pay_bank_transfer_id
      FROM scope_transfer_candidate
      GROUP BY scope_transfer_candidate.settlement_scope_id
      HAVING COUNT(*) = 1
    ), ranked_matching_event AS (
      SELECT
        resolved_scope_transfer.settlement_scope_id,
        matching_event.id AS event_id,
        ROW_NUMBER() OVER (
          PARTITION BY resolved_scope_transfer.settlement_scope_id
          ORDER BY
            matching_event.event_time_utc DESC NULLS LAST,
            matching_event.received_at_utc DESC NULLS LAST,
            matching_event.created_at_utc DESC NULLS LAST,
            matching_event.id DESC
        ) AS event_rank
      FROM resolved_scope_transfer
      JOIN public.pay_bank_transfer_events AS matching_event
        ON matching_event.pay_batch_id = p_pay_batch_id
       AND matching_event.pay_bank_transfer_id = resolved_scope_transfer.pay_bank_transfer_id
      WHERE (
          UPPER(BTRIM(COALESCE(matching_event.normalised_state, ''))) IN ('COMPLETED', 'SETTLED', 'PAID', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
          OR UPPER(BTRIM(COALESCE(matching_event.provider_state, ''))) IN ('COMPLETED', 'SETTLED', 'PAID', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
        )
        AND (
          matching_event.idempotency_key LIKE
            'rail-settle:' || p_operation_id::text || ':' || resolved_scope_transfer.settlement_scope_id::text || ':' || resolved_scope_transfer.pay_bank_transfer_id::text || ':%'
          OR (
            NULLIF(BTRIM(COALESCE(matching_event.raw_payload->>'source_rpc', '')), '') = 'pay_settle_rail'
            AND NULLIF(BTRIM(COALESCE(matching_event.raw_payload->>'operation_id', '')), '') = p_operation_id::text
            AND NULLIF(BTRIM(COALESCE(matching_event.raw_payload->>'settlement_scope_id', '')), '') = resolved_scope_transfer.settlement_scope_id::text
          )
        )
    )
    UPDATE public.banking_pay_operation_settlement_scope AS scope_repair
    SET settlement_event_id = ranked_matching_event.event_id,
        updated_at_utc = v_now
    FROM ranked_matching_event
    WHERE scope_repair.id = ranked_matching_event.settlement_scope_id
      AND ranked_matching_event.event_rank = 1
      AND scope_repair.operation_id = p_operation_id
      AND scope_repair.pay_batch_id = p_pay_batch_id
      AND scope_repair.status = 'SETTLED'
      AND scope_repair.settlement_event_id IS NULL;

    WITH requested_scope AS (
      SELECT DISTINCT (scope_element.value #>> '{}')::uuid AS settlement_scope_id
      FROM jsonb_array_elements(p_settlement_scope_ids) AS scope_element(value)
      WHERE (scope_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ), pending_scope AS (
      SELECT scope_row.*
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      JOIN requested_scope
        ON requested_scope.settlement_scope_id = scope_row.id
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND scope_row.status = 'PENDING'
      FOR UPDATE OF scope_row
    ), payload_transfer AS (
      SELECT pending_scope.id AS settlement_scope_id,
             CASE
               WHEN COALESCE(pending_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                 THEN (pending_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}')::uuid
               ELSE NULL::uuid
             END AS pay_bank_transfer_id
      FROM pending_scope
    ), payload_item_ids AS (
      SELECT pending_scope.id AS settlement_scope_id,
             (item_element.value #>> '{}')::uuid AS pay_batch_item_id
      FROM pending_scope
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pending_scope.payload_json->'pay_batch_item_ids', '[]'::jsonb)) AS item_element(value)
      WHERE (item_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ), scope_transfers AS (
      SELECT DISTINCT pending_scope.id AS settlement_scope_id,
             COALESCE(payload_transfer.pay_bank_transfer_id, batch_item.pay_bank_transfer_id) AS pay_bank_transfer_id
      FROM pending_scope
      LEFT JOIN payload_transfer
        ON payload_transfer.settlement_scope_id = pending_scope.id
      LEFT JOIN payload_item_ids
        ON payload_item_ids.settlement_scope_id = pending_scope.id
      LEFT JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = payload_item_ids.pay_batch_item_id
      WHERE COALESCE(payload_transfer.pay_bank_transfer_id, batch_item.pay_bank_transfer_id) IS NOT NULL
    ), eligible_no_bank_scope AS (
      SELECT
        eligible_scope.settlement_scope_id,
        eligible_scope.pay_batch_candidate_id,
        eligible_scope.candidate_id
      FROM pg_temp.tmp_pay_settle_rail_eligible_no_bank_scope AS eligible_scope
      JOIN pending_scope
        ON pending_scope.id = eligible_scope.settlement_scope_id
    ), failed_scope_no_transfer AS (
      UPDATE public.banking_pay_operation_settlement_scope AS scope_update
      SET status = 'FAILED',
          payload_json = jsonb_strip_nulls(COALESCE(scope_update.payload_json, '{}'::jsonb) || jsonb_build_object(
            'settlement_failure_code', 'NO_PAY_BANK_TRANSFER_FOR_SCOPE',
            'settlement_failure_message', 'Rail/provider settlement scope could not be processed because it does not resolve to a frozen pay_bank_transfer row.',
            'failed_at_utc', v_now::text
          )),
          updated_at_utc = v_now
      FROM pending_scope
      WHERE scope_update.id = pending_scope.id
        AND NOT EXISTS (
          SELECT 1
          FROM scope_transfers
          WHERE scope_transfers.settlement_scope_id = pending_scope.id
        )
        AND NOT EXISTS (
          SELECT 1
          FROM eligible_no_bank_scope
          WHERE eligible_no_bank_scope.settlement_scope_id = pending_scope.id
        )
      RETURNING scope_update.id,
                scope_update.pay_batch_candidate_id
    ), matched_updates AS (
      SELECT scope_transfers.settlement_scope_id,
             scope_transfers.pay_bank_transfer_id,
             update_row.status,
             update_row.rail_tx_id,
             update_row.rail_state,
             update_row.rail_meta_json,
             update_row.provider_reference,
             update_row.provider_state,
             update_row.event_time_utc,
             update_row.input_json
      FROM scope_transfers
      JOIN pg_temp.tmp_operation_rail_settlement_updates AS update_row
        ON update_row.transfer_id = scope_transfers.pay_bank_transfer_id
    ), inserted_events AS (
      INSERT INTO public.pay_bank_transfer_events (
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
        movement_classification,
        correction_disposition,
        raw_payload,
        idempotency_key,
        mapping_method
      )
      SELECT
        p_pay_batch_id,
        matched_updates.pay_bank_transfer_id,
        bank_transfer.candidate_id,
        bank_transfer.umbrella_id,
        COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.rail_provider, '')), ''), v_batch.rail_provider_snapshot),
        CASE
          WHEN NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') IS NOT NULL
           AND NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') NOT IN (
             COALESCE(bank_transfer.id::text, '__no_transfer_id__'),
             COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
             COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
             COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
           )
            THEN NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '')
          WHEN NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') IS NOT NULL
           AND NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') NOT IN (
             COALESCE(bank_transfer.id::text, '__no_transfer_id__'),
             COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
             COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
             COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
           )
            THEN NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '')
          ELSE NULL::text
        END,
        COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') NOT IN (
               COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
               COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
               COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
             )
              THEN NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '')
            ELSE NULL::text
          END,
          CASE
            WHEN NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') NOT IN (
               COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
               COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
               COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
             )
              THEN NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '')
            ELSE NULL::text
          END,
          CASE
            WHEN NULLIF(BTRIM(COALESCE(bank_transfer.rail_tx_id, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(bank_transfer.rail_tx_id, '')), '') NOT IN (
               COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
               COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
               COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
             )
              THEN NULLIF(BTRIM(COALESCE(bank_transfer.rail_tx_id, '')), '')
            ELSE NULL::text
          END
        ),
        CASE
          WHEN matched_updates.status = 'COMPLETED' AND ((
              NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') NOT IN (
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR (
              NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') NOT IN (
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR lower(btrim(coalesce(matched_updates.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) THEN COALESCE(matched_updates.provider_state, matched_updates.rail_state, matched_updates.status)
          WHEN matched_updates.status = 'COMPLETED' THEN 'UNKNOWN'
          ELSE COALESCE(matched_updates.provider_state, matched_updates.rail_state, matched_updates.status)
        END,
        CASE
          WHEN matched_updates.status = 'COMPLETED' AND ((
              NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') NOT IN (
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR (
              NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') NOT IN (
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR lower(btrim(coalesce(matched_updates.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) THEN 'COMPLETED'
          WHEN matched_updates.status = 'COMPLETED' THEN 'UNKNOWN'
          WHEN matched_updates.status IN ('FAILED', 'RETURNED', 'CANCELLED', 'PENDING', 'PROCESSING', 'UNKNOWN') THEN matched_updates.status
          ELSE 'UNKNOWN'
        END,
        'PROVIDER_POLL',
        COALESCE(matched_updates.event_time_utc, v_now),
        v_now,
        bank_transfer.amount,
        COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer.currency, '')), ''), 'GBP'),
        'MATCHED',
        NULL::text,
        NULL::text,
        jsonb_strip_nulls(COALESCE(matched_updates.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'source_rpc', 'pay_settle_rail',
          'settlement_event_source', 'PROVIDER_SETTLEMENT',
          'operation_id', p_operation_id::text,
          'settlement_scope_id', matched_updates.settlement_scope_id::text,
          'input_json', matched_updates.input_json
        )),
        'rail-settle:' || p_operation_id::text || ':' || matched_updates.settlement_scope_id::text || ':' || matched_updates.pay_bank_transfer_id::text || ':' || md5(jsonb_strip_nulls(jsonb_build_object(
          'status', matched_updates.status,
          'rail_tx_id', matched_updates.rail_tx_id,
          'rail_state', matched_updates.rail_state,
          'provider_reference', matched_updates.provider_reference,
          'provider_state', matched_updates.provider_state,
          'rail_meta_json', COALESCE(matched_updates.rail_meta_json, '{}'::jsonb)
        ))::text),
        'TRANSFER_ID'
      FROM matched_updates
      JOIN public.pay_bank_transfers AS bank_transfer
        ON bank_transfer.id = matched_updates.pay_bank_transfer_id
       AND bank_transfer.pay_batch_id = p_pay_batch_id
      WHERE NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS existing_event
        WHERE existing_event.pay_batch_id = p_pay_batch_id
          AND existing_event.idempotency_key = 'rail-settle:' || p_operation_id::text || ':' || matched_updates.settlement_scope_id::text || ':' || matched_updates.pay_bank_transfer_id::text || ':' || md5(jsonb_strip_nulls(jsonb_build_object(
            'status', matched_updates.status,
            'rail_tx_id', matched_updates.rail_tx_id,
            'rail_state', matched_updates.rail_state,
            'provider_reference', matched_updates.provider_reference,
            'provider_state', matched_updates.provider_state,
            'rail_meta_json', COALESCE(matched_updates.rail_meta_json, '{}'::jsonb)
          ))::text)
      )
      RETURNING public.pay_bank_transfer_events.id,
                public.pay_bank_transfer_events.pay_bank_transfer_id,
                public.pay_bank_transfer_events.idempotency_key
    ), transfer_updates AS (
      UPDATE public.pay_bank_transfers AS transfer_update
      SET status = CASE
            WHEN matched_updates.status = 'COMPLETED' AND ((
              NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') NOT IN (
                COALESCE(transfer_update.id::text, '__no_transfer_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR (
              NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') NOT IN (
                COALESCE(transfer_update.id::text, '__no_transfer_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR lower(btrim(coalesce(matched_updates.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) THEN 'COMPLETED'
            WHEN matched_updates.status = 'COMPLETED' THEN 'UNKNOWN'
            WHEN matched_updates.status IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED') THEN matched_updates.status
            WHEN matched_updates.status = 'PROCESSING' THEN 'PROCESSING'
            WHEN matched_updates.status = 'UNKNOWN' THEN 'UNKNOWN'
            ELSE 'PENDING'
          END,
          rail_tx_id = CASE
            WHEN matched_updates.status = 'COMPLETED' AND ((
              NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') NOT IN (
                COALESCE(transfer_update.id::text, '__no_transfer_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR (
              NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') NOT IN (
                COALESCE(transfer_update.id::text, '__no_transfer_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR lower(btrim(coalesce(matched_updates.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) THEN COALESCE(matched_updates.rail_tx_id, transfer_update.rail_tx_id)
            WHEN matched_updates.status <> 'COMPLETED' THEN COALESCE(matched_updates.rail_tx_id, transfer_update.rail_tx_id)
            ELSE transfer_update.rail_tx_id
          END,
          rail_state = COALESCE(matched_updates.rail_state, matched_updates.provider_state, transfer_update.rail_state),
          rail_meta_json = jsonb_strip_nulls(COALESCE(transfer_update.rail_meta_json, '{}'::jsonb) || COALESCE(matched_updates.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
            'provider_reference', matched_updates.provider_reference,
            'provider_state', matched_updates.provider_state,
            'settlement_operation_id', p_operation_id::text
          )),
          completed_at_utc = CASE WHEN matched_updates.status = 'COMPLETED' AND ((
              NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.provider_reference, '')), '') NOT IN (
                COALESCE(transfer_update.id::text, '__no_transfer_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR (
              NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(matched_updates.rail_tx_id, '')), '') NOT IN (
                COALESCE(transfer_update.id::text, '__no_transfer_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_update.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
              )
            )
            OR lower(btrim(coalesce(matched_updates.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) THEN COALESCE(transfer_update.completed_at_utc, matched_updates.event_time_utc, v_now) ELSE transfer_update.completed_at_utc END,
          failed_reason = CASE WHEN matched_updates.status IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'RETURNED', 'REVERTED') THEN COALESCE(transfer_update.failed_reason, matched_updates.provider_state, matched_updates.rail_state, matched_updates.status) ELSE transfer_update.failed_reason END
      FROM matched_updates
      WHERE transfer_update.id = matched_updates.pay_bank_transfer_id
        AND transfer_update.pay_batch_id = p_pay_batch_id
      RETURNING transfer_update.id,
                matched_updates.settlement_scope_id,
                transfer_update.status
    ), scope_current_transfer_status AS (
      SELECT
        pending_scope.id AS settlement_scope_id,
        scope_transfers.pay_bank_transfer_id,
        COALESCE(transfer_updates.status, bank_transfer.status) AS effective_transfer_status,
        COALESCE(bank_transfer.rail_state, transfer_updates.status) AS effective_rail_state,
        COALESCE(bank_transfer.completed_at_utc, NULL::timestamptz) AS completed_at_utc,
        COALESCE(transfer_classifier.is_final_money_moved, false) AS is_final_money_moved,
        COALESCE(transfer_classifier.is_terminal_no_money, false) AS is_terminal_no_money,
        COALESCE(transfer_classifier.is_pending_non_final, false) AS is_pending_non_final
      FROM pending_scope
      JOIN scope_transfers
        ON scope_transfers.settlement_scope_id = pending_scope.id
      LEFT JOIN transfer_updates
        ON transfer_updates.settlement_scope_id = pending_scope.id
       AND transfer_updates.id = scope_transfers.pay_bank_transfer_id
      LEFT JOIN public.pay_bank_transfers AS bank_transfer
        ON bank_transfer.id = scope_transfers.pay_bank_transfer_id
       AND bank_transfer.pay_batch_id = p_pay_batch_id
      LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
        COALESCE(transfer_updates.status, bank_transfer.status),
        COALESCE(bank_transfer.rail_state, transfer_updates.status),
        COALESCE(bank_transfer.rail_meta_json, '{}'::jsonb),
        COALESCE(bank_transfer.rail_meta_json, '{}'::jsonb)
      ) AS transfer_classifier ON bank_transfer.id IS NOT NULL
    ), scope_status AS (
      SELECT
        scope_current_transfer_status.settlement_scope_id,
        CASE
          WHEN BOOL_OR(
            scope_current_transfer_status.is_terminal_no_money
            OR upper(btrim(COALESCE(scope_current_transfer_status.effective_transfer_status, ''))) IN ('FAILED', 'FAILURE', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED', 'REVERSED')
            OR upper(btrim(COALESCE(scope_current_transfer_status.effective_rail_state, ''))) IN ('FAILED', 'FAILURE', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED', 'REVERSED')
          ) THEN 'FAILED'
          WHEN COUNT(scope_current_transfer_status.pay_bank_transfer_id) > 0
           AND BOOL_AND(
             scope_current_transfer_status.is_final_money_moved
             OR upper(btrim(COALESCE(scope_current_transfer_status.effective_transfer_status, ''))) IN ('COMPLETED', 'COMPLETE', 'PAID', 'SETTLED', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
             OR upper(btrim(COALESCE(scope_current_transfer_status.effective_rail_state, ''))) IN ('COMPLETED', 'COMPLETE', 'PAID', 'SETTLED', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
             OR scope_current_transfer_status.completed_at_utc IS NOT NULL
           ) THEN 'SETTLED'
          ELSE 'PENDING'
        END AS new_scope_status
      FROM scope_current_transfer_status
      GROUP BY scope_current_transfer_status.settlement_scope_id
    ), event_candidate AS (
      SELECT
        matched_updates.settlement_scope_id,
        inserted_event.id AS event_id,
        0::integer AS source_priority
      FROM matched_updates
      JOIN inserted_events AS inserted_event
        ON inserted_event.pay_bank_transfer_id = matched_updates.pay_bank_transfer_id
       AND inserted_event.idempotency_key = 'rail-settle:' || p_operation_id::text || ':' || matched_updates.settlement_scope_id::text || ':' || matched_updates.pay_bank_transfer_id::text || ':' || md5(jsonb_strip_nulls(jsonb_build_object(
          'status', matched_updates.status,
          'rail_tx_id', matched_updates.rail_tx_id,
          'rail_state', matched_updates.rail_state,
          'provider_reference', matched_updates.provider_reference,
          'provider_state', matched_updates.provider_state,
          'rail_meta_json', COALESCE(matched_updates.rail_meta_json, '{}'::jsonb)
        ))::text)

      UNION ALL

      SELECT
        matched_updates.settlement_scope_id,
        existing_event.id AS event_id,
        1::integer AS source_priority
      FROM matched_updates
      JOIN public.pay_bank_transfer_events AS existing_event
        ON existing_event.pay_batch_id = p_pay_batch_id
       AND existing_event.idempotency_key = 'rail-settle:' || p_operation_id::text || ':' || matched_updates.settlement_scope_id::text || ':' || matched_updates.pay_bank_transfer_id::text || ':' || md5(jsonb_strip_nulls(jsonb_build_object(
          'status', matched_updates.status,
          'rail_tx_id', matched_updates.rail_tx_id,
          'rail_state', matched_updates.rail_state,
          'provider_reference', matched_updates.provider_reference,
          'provider_state', matched_updates.provider_state,
          'rail_meta_json', COALESCE(matched_updates.rail_meta_json, '{}'::jsonb)
        ))::text)
    ), event_by_scope AS (
      SELECT
        event_candidate.settlement_scope_id,
        (ARRAY_AGG(event_candidate.event_id ORDER BY event_candidate.source_priority, event_candidate.event_id))[1] AS event_id
      FROM event_candidate
      GROUP BY event_candidate.settlement_scope_id
    ), scope_updates AS (
      UPDATE public.banking_pay_operation_settlement_scope AS scope_update
      SET status = scope_status.new_scope_status,
          settlement_event_id = COALESCE(event_by_scope.event_id, scope_update.settlement_event_id),
          updated_at_utc = v_now
      FROM scope_status
      LEFT JOIN event_by_scope
        ON event_by_scope.settlement_scope_id = scope_status.settlement_scope_id
      WHERE scope_update.id = scope_status.settlement_scope_id
      RETURNING scope_update.id,
                scope_update.pay_batch_candidate_id,
                scope_update.status
    ), settled_no_bank_scope AS (
      UPDATE public.banking_pay_operation_settlement_scope AS scope_update
      SET status = 'SETTLED',
          settlement_event_id = NULL::uuid,
          payload_json = JSONB_STRIP_NULLS(
            COALESCE(scope_update.payload_json, '{}'::jsonb)
            || JSONB_BUILD_OBJECT(
              'scope_kind', 'NO_BANK_PAYMENT',
              'no_bank_payment_reason', 'EXPLICIT_ZERO_PAYE',
              'no_bank_payment_scope', true,
              'server_owned_payment_projection_proof', true,
              'no_bank_payment_execution', v_no_bank_payment_execution_validated,
              'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_marker,
              'settlement_mode', v_settlement_mode,
              'confirmation_mode', CASE
                WHEN v_no_bank_payment_execution_validated AND v_settlement_mode = 'CSV_SETTLEMENT' THEN 'NO_BANK_PAYMENT_REVIEW'
                WHEN v_no_bank_payment_execution_validated AND v_settlement_mode = 'EXTERNAL_SETTLEMENT' THEN 'NO_BANK_PAYMENT_EXTERNAL_CONFIRMATION'
                WHEN v_no_bank_payment_execution_validated THEN 'NO_BANK_PAYMENT_EXECUTION'
                ELSE 'EXPLICIT_ZERO_NO_BANK_SCOPE'
              END,
              'local_commit_reference', v_local_no_bank_commit_ref,
              'paye_net_state_hash', v_expected_paye_net_state_hash,
              'authorised_paye_net_state_hash', v_expected_paye_net_state_hash,
              'global_bank_payment_projection_hash', v_expected_global_bank_payment_projection_hash,
              'scoped_paye_net_state_hash', v_expected_scoped_paye_net_state_hash,
              'bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
              'authorised_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
              'projection_scope', v_operation_projection_scope,
              'authorised_scope', v_operation_projection_scope,
              'scoped_no_transfer_execution', v_scoped_no_transfer_marker,
              'accounting_payment_date', CASE WHEN v_operation_effective_payment_date IS NULL THEN NULL ELSE v_operation_effective_payment_date::text END
            )
            || JSONB_BUILD_OBJECT(
              'execution_operation_id', v_execution_operation_id::text,
              'settlement_operation_id', p_operation_id::text,
              'auth_request_id', v_operation_auth_request_id::text,
              'authorised_execution_mode', v_authorised_execution_mode,
              'authorised_missing_explicit_paye_input_count', v_expected_missing_count,
              'authorised_explicit_zero_count', v_expected_global_zero_count,
              'authorised_global_positive_bank_payment_count', v_expected_global_positive_count,
              'authorised_global_invalid_payment_row_count', v_expected_global_invalid_count,
              'authorised_scoped_missing_explicit_paye_input_count', v_expected_scoped_missing_count,
              'authorised_scoped_explicit_zero_count', v_expected_zero_count,
              'authorised_scoped_positive_bank_payment_count', v_expected_positive_count,
              'authorised_scoped_invalid_payment_row_count', v_expected_scoped_invalid_count,
              'current_missing_explicit_paye_input_count', v_current_missing_count,
              'current_explicit_zero_count', v_global_zero_count,
              'current_global_positive_bank_payment_count', v_global_positive_count,
              'current_global_invalid_payment_row_count', v_global_invalid_count,
              'current_scoped_missing_explicit_paye_input_count', v_current_scoped_missing_count,
              'current_scoped_explicit_zero_count', v_current_zero_count,
              'current_scoped_positive_bank_payment_count', v_current_positive_count,
              'current_scoped_invalid_payment_row_count', v_current_scoped_invalid_count,
              'projection_diagnostic', v_projection_diagnostic_json,
              'suppress_remittances', v_suppress_remittances,
              'settled_at_utc', v_now::text,
              'settled_by_user_id', p_actor_user_id::text,
              'no_bank_payment_note', CASE
                WHEN v_no_bank_payment_execution_validated AND v_settlement_mode = 'CSV_SETTLEMENT'
                  THEN 'The current zero-row CloudTMS Bank CSV was reviewed; no bank upload or payment occurred or was required.'
                WHEN v_no_bank_payment_execution_validated
                  THEN 'No bank transfer or provider submission occurred or was required for this explicit-zero PAYE execution.'
                ELSE 'This explicit-zero PAYE scope required no bank transfer; positive scopes, if any, remain transfer-backed.'
              END
            )
          ),
          updated_at_utc = v_now
      FROM eligible_no_bank_scope
      WHERE scope_update.id = eligible_no_bank_scope.settlement_scope_id
      RETURNING scope_update.id,
                scope_update.pay_batch_candidate_id,
                scope_update.status
    ), processed_scope_updates AS (
      SELECT
        scope_updates.id,
        scope_updates.pay_batch_candidate_id,
        scope_updates.status,
        false AS no_bank_payment_scope
      FROM scope_updates
      UNION ALL
      SELECT
        settled_no_bank_scope.id,
        settled_no_bank_scope.pay_batch_candidate_id,
        settled_no_bank_scope.status,
        true AS no_bank_payment_scope
      FROM settled_no_bank_scope
    ), candidate_complete AS (
      SELECT DISTINCT processed_scope_updates.pay_batch_candidate_id
      FROM processed_scope_updates
      WHERE processed_scope_updates.status = 'SETTLED'
        AND processed_scope_updates.pay_batch_candidate_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_settlement_scope AS remaining_for_candidate
          WHERE remaining_for_candidate.operation_id = p_operation_id
            AND remaining_for_candidate.pay_batch_id = p_pay_batch_id
            AND remaining_for_candidate.pay_batch_candidate_id = processed_scope_updates.pay_batch_candidate_id
            AND remaining_for_candidate.status NOT IN ('SETTLED', 'SKIPPED')
            AND NOT EXISTS (
              SELECT 1
              FROM processed_scope_updates AS current_scope_result
              WHERE current_scope_result.id = remaining_for_candidate.id
                AND current_scope_result.status IN ('SETTLED', 'SKIPPED')
            )
        )
    ), no_bank_candidate_complete AS (
      SELECT DISTINCT settled_no_bank_scope.pay_batch_candidate_id
      FROM settled_no_bank_scope
      WHERE settled_no_bank_scope.pay_batch_candidate_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_settlement_scope AS remaining_for_no_bank_candidate
          WHERE remaining_for_no_bank_candidate.operation_id = p_operation_id
            AND remaining_for_no_bank_candidate.pay_batch_id = p_pay_batch_id
            AND remaining_for_no_bank_candidate.pay_batch_candidate_id = settled_no_bank_scope.pay_batch_candidate_id
            AND remaining_for_no_bank_candidate.status NOT IN ('SETTLED', 'SKIPPED')
            AND NOT EXISTS (
              SELECT 1
              FROM processed_scope_updates AS current_scope_result
              WHERE current_scope_result.id = remaining_for_no_bank_candidate.id
                AND current_scope_result.status IN ('SETTLED', 'SKIPPED')
            )
        )
    ), transfer_backed_candidate_complete AS (
      SELECT candidate_complete.pay_batch_candidate_id
      FROM candidate_complete
      WHERE EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS candidate_item
        WHERE candidate_item.pay_batch_candidate_id = candidate_complete.pay_batch_candidate_id
          AND COALESCE(candidate_item.is_voided, false) = false
          AND candidate_item.pay_bank_transfer_id IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_settlement_scope AS candidate_scope
        WHERE candidate_scope.operation_id = p_operation_id
          AND candidate_scope.pay_batch_id = p_pay_batch_id
          AND candidate_scope.pay_batch_candidate_id = candidate_complete.pay_batch_candidate_id
          AND candidate_scope.settlement_event_id IS NOT NULL
      )
    ), candidate_updates AS (
      UPDATE public.pay_batch_candidates AS candidate_update
      SET settlement_status = 'SETTLED',
          settled_at_utc = COALESCE(candidate_update.settled_at_utc, v_now),
          settled_via = COALESCE(NULLIF(BTRIM(COALESCE(v_batch.rail_provider_snapshot, '')), ''), 'RAIL'),
          settled_note = COALESCE(candidate_update.settled_note, 'Rail/provider settlement confirmed')
      FROM transfer_backed_candidate_complete
      WHERE candidate_update.id = transfer_backed_candidate_complete.pay_batch_candidate_id
      RETURNING candidate_update.id
    ), no_bank_candidate_updates AS (
      UPDATE public.pay_batch_candidates AS candidate_update
      SET settlement_status = 'SETTLED',
          settled_at_utc = COALESCE(candidate_update.settled_at_utc, v_now),
          settled_via = 'NO_BANK_PAYMENT',
          settled_note = COALESCE(candidate_update.settled_note, v_local_no_bank_commit_ref)
      FROM no_bank_candidate_complete
      WHERE candidate_update.id = no_bank_candidate_complete.pay_batch_candidate_id
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS transfer_backed_candidate_item
          WHERE transfer_backed_candidate_item.pay_batch_candidate_id = no_bank_candidate_complete.pay_batch_candidate_id
            AND COALESCE(transfer_backed_candidate_item.is_voided, false) = false
            AND transfer_backed_candidate_item.pay_bank_transfer_id IS NOT NULL
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_settlement_scope AS transfer_backed_candidate_scope
          WHERE transfer_backed_candidate_scope.operation_id = p_operation_id
            AND transfer_backed_candidate_scope.pay_batch_id = p_pay_batch_id
            AND transfer_backed_candidate_scope.pay_batch_candidate_id = no_bank_candidate_complete.pay_batch_candidate_id
            AND transfer_backed_candidate_scope.settlement_event_id IS NOT NULL
        )
      RETURNING candidate_update.id
    )
    SELECT count(*) FILTER (WHERE processed_scope_updates.status = 'SETTLED')::integer,
           COALESCE(v_failed_this_chunk, 0)
             + COALESCE((SELECT COUNT(*)::integer FROM failed_scope_no_transfer), 0)
             + count(*) FILTER (WHERE processed_scope_updates.status = 'FAILED')::integer,
           count(*) FILTER (WHERE processed_scope_updates.status = 'PENDING')::integer,
           count(*) FILTER (WHERE processed_scope_updates.no_bank_payment_scope)::integer
    INTO v_settled_this_chunk,
         v_failed_this_chunk,
         v_pending_this_chunk,
         v_no_bank_settled_this_chunk
    FROM processed_scope_updates;

    SELECT
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
          AND scope_row.status <> 'SKIPPED'
      )::integer,
      COUNT(eligible_scope.settlement_scope_id)::integer
    INTO
      v_no_bank_total_scope_count,
      v_no_bank_eligible_scope_count
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    LEFT JOIN pg_temp.tmp_pay_settle_rail_eligible_no_bank_scope AS eligible_scope
      ON eligible_scope.settlement_scope_id = scope_row.id
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id;

    v_no_bank_invalid_scope_count := GREATEST(
      COALESCE(v_no_bank_total_scope_count, 0) - COALESCE(v_no_bank_eligible_scope_count, 0),
      0
    );

    IF COALESCE(v_no_bank_total_scope_count, 0) > 0
       AND (
         v_no_bank_scope_authorised IS NOT TRUE
         OR COALESCE(v_no_bank_invalid_scope_count, 0) <> 0
       ) THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'NO_BANK_PAYMENT_PROOF_REQUIRED',
        'message', 'A no-bank-payment settlement scope exists without a currently valid authorised proof or contains invalid scope evidence.',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'no_bank_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
        'eligible_no_bank_scope_count', COALESCE(v_no_bank_eligible_scope_count, 0),
        'invalid_no_bank_scope_count', COALESCE(v_no_bank_invalid_scope_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;

    WITH scope_summary AS (
      SELECT
        COUNT(*)::integer AS total_scope_count,
        COUNT(*) FILTER (WHERE scope_row.status IN ('SETTLED', 'SKIPPED'))::integer AS successful_scope_count,
        COUNT(*) FILTER (WHERE scope_row.status = 'FAILED')::integer AS terminal_failed_scope_count,
        COUNT(*) FILTER (WHERE scope_row.status = 'PENDING')::integer AS pending_scope_count,
        COUNT(*) FILTER (WHERE scope_row.status NOT IN ('SETTLED', 'SKIPPED', 'FAILED', 'PENDING'))::integer AS unknown_scope_count,
        COALESCE(
          JSONB_AGG(
            JSONB_BUILD_OBJECT(
              'scope_id', scope_row.id::text,
              'status', scope_row.status,
              'settlement_event_id', CASE WHEN scope_row.settlement_event_id IS NULL THEN NULL::text ELSE scope_row.settlement_event_id::text END,
              'scope_kind', scope_row.payload_json->>'scope_kind'
            )
            ORDER BY scope_row.id::text
          ),
          '[]'::jsonb
        ) AS scope_state_json
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
    ), completed_scope_transfer_ids AS (
      SELECT DISTINCT
        CASE
          WHEN COALESCE(scope_row.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')
               ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN (scope_row.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}')::uuid
          ELSE NULL::uuid
        END AS pay_bank_transfer_id
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND scope_row.status = 'SETTLED'

      UNION

      SELECT DISTINCT batch_item.pay_bank_transfer_id
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
        CASE
          WHEN JSONB_TYPEOF(scope_row.payload_json->'pay_batch_item_ids') = 'array'
            THEN scope_row.payload_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_element(value)
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = CASE
             WHEN (item_element.value #>> '{}')
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN (item_element.value #>> '{}')::uuid
             ELSE NULL::uuid
           END
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND scope_row.status = 'SETTLED'
        AND batch_item.pay_bank_transfer_id IS NOT NULL
    ), completed_transfer_summary AS (
      SELECT COUNT(DISTINCT transfer_row.id)::integer AS completed_transfer_count
      FROM completed_scope_transfer_ids AS completed_scope_transfer_id
      JOIN public.pay_bank_transfers AS transfer_row
        ON transfer_row.id = completed_scope_transfer_id.pay_bank_transfer_id
       AND transfer_row.pay_batch_id = p_pay_batch_id
      LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
        transfer_row.status,
        transfer_row.rail_state,
        COALESCE(transfer_row.rail_meta_json, '{}'::jsonb),
        COALESCE(transfer_row.rail_meta_json, '{}'::jsonb)
      ) AS transfer_classifier ON true
      WHERE COALESCE(transfer_classifier.is_final_money_moved, false)
         OR transfer_row.completed_at_utc IS NOT NULL
         OR UPPER(BTRIM(COALESCE(transfer_row.status, ''))) IN ('COMPLETED', 'COMPLETE', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
         OR UPPER(BTRIM(COALESCE(transfer_row.rail_state, ''))) IN ('COMPLETED', 'COMPLETE', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
    )
    SELECT
      COALESCE(scope_summary.total_scope_count, 0),
      COALESCE(scope_summary.successful_scope_count, 0),
      COALESCE(scope_summary.terminal_failed_scope_count, 0),
      COALESCE(scope_summary.pending_scope_count, 0),
      COALESCE(scope_summary.unknown_scope_count, 0),
      COALESCE(completed_transfer_summary.completed_transfer_count, 0),
      scope_summary.scope_state_json
    INTO
      v_total_scope_count,
      v_successful_scope_count,
      v_terminal_failed_scope_count,
      v_pending_scope_count,
      v_unknown_scope_count,
      v_operation_completed_transfer_count,
      v_operation_scope_state_json
    FROM scope_summary
    CROSS JOIN completed_transfer_summary;

    v_processor_failed_scope_count := 0;
    v_total_failed_scope_count := COALESCE(v_terminal_failed_scope_count, 0);
    v_remaining_scope_count := COALESCE(v_pending_scope_count, 0);
    v_has_more := COALESCE(v_pending_scope_count, 0) > 0 OR COALESCE(v_unknown_scope_count, 0) > 0;
    v_all_scopes_terminal := COALESCE(v_total_scope_count, 0) > 0
      AND COALESCE(v_pending_scope_count, 0) = 0
      AND COALESCE(v_unknown_scope_count, 0) = 0;
    v_all_scopes_successful := v_all_scopes_terminal
      AND COALESCE(v_terminal_failed_scope_count, 0) = 0
      AND COALESCE(v_successful_scope_count, 0) = COALESCE(v_total_scope_count, 0);
    v_completed_with_failed_payments := v_all_scopes_terminal
      AND COALESCE(v_terminal_failed_scope_count, 0) > 0;
    v_scope_settlement_complete := v_all_scopes_successful;
    v_requires_full_batch_finalisation := v_all_scopes_terminal;
    v_full_batch_finalisation_safe := v_all_scopes_successful
      AND COALESCE(v_no_bank_invalid_scope_count, 0) = 0
      AND COALESCE(v_no_bank_total_scope_count, 0) = COALESCE(v_expected_zero_count, 0)
      AND COALESCE(v_operation_completed_transfer_count, 0) = COALESCE(v_expected_positive_count, 0)
      AND (
        COALESCE(v_expected_positive_count, 0) > 0
        OR (
          COALESCE(v_expected_positive_count, 0) = 0
          AND COALESCE(v_expected_zero_count, 0) > 0
          AND v_no_bank_payment_execution_validated
        )
      );

    v_batch_status_label := CASE
      WHEN v_completed_with_failed_payments THEN 'Completed with failed payments'
      WHEN v_all_scopes_successful THEN 'Completed'
      WHEN COALESCE(v_pending_scope_count, 0) + COALESCE(v_unknown_scope_count, 0) > 0 THEN 'Waiting for bank confirmation'
      ELSE NULL::text
    END;

    v_finalisation_payload_hash := CASE
      WHEN v_requires_full_batch_finalisation THEN MD5(
        JSONB_STRIP_NULLS(
          JSONB_BUILD_OBJECT(
            'pay_batch_id', p_pay_batch_id::text,
            'operation_id', p_operation_id::text,
            'execution_operation_id', v_execution_operation_id::text,
            'auth_request_id', v_operation_auth_request_id::text,
            'settlement_mode', v_settlement_mode,
            'projection_scope', v_operation_projection_scope,
            'scope_state', v_operation_scope_state_json,
            'successful_scope_count', COALESCE(v_successful_scope_count, 0),
            'terminal_failed_scope_count', COALESCE(v_terminal_failed_scope_count, 0),
            'pending_scope_count', COALESCE(v_pending_scope_count, 0),
            'unknown_scope_count', COALESCE(v_unknown_scope_count, 0),
            'completed_transfer_count', COALESCE(v_operation_completed_transfer_count, 0),
            'no_bank_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
            'no_bank_payment_execution', v_no_bank_payment_execution_validated,
            'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_marker,
            'global_paye_net_state_hash', v_expected_paye_net_state_hash,
            'global_bank_payment_projection_hash', v_expected_global_bank_payment_projection_hash,
            'scoped_paye_net_state_hash', v_expected_scoped_paye_net_state_hash,
            'scoped_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
            'projection_changed_after_authorisation', v_current_projection_changed,
            'local_commit_reference', CASE WHEN v_no_bank_scope_authorised THEN v_local_no_bank_commit_ref ELSE NULL::text END
          )
        )::text
      )
      ELSE NULL::text
    END;
    v_finalisation_idempotency_key := CASE
      WHEN v_requires_full_batch_finalisation
        THEN 'rail-finalise:batch:' || p_pay_batch_id::text || ':operation:' || p_operation_id::text || ':hash:' || v_finalisation_payload_hash
      ELSE NULL::text
    END;

    UPDATE public.banking_pay_operations AS operation_update
    SET pay_batch_id = COALESCE(operation_update.pay_batch_id, p_pay_batch_id),
        progress_json = JSONB_STRIP_NULLS(
          COALESCE(operation_update.progress_json, '{}'::jsonb)
          || JSONB_BUILD_OBJECT(
            'scope_settlement_complete', v_scope_settlement_complete,
            'all_scopes_terminal', v_all_scopes_terminal,
            'all_scopes_successful', v_all_scopes_successful,
            'completed_with_failed_payments', v_completed_with_failed_payments,
            'total_scope_count', COALESCE(v_total_scope_count, 0),
            'successful_scope_count', COALESCE(v_successful_scope_count, 0),
            'terminal_failed_scope_count', COALESCE(v_terminal_failed_scope_count, 0),
            'pending_scope_count', COALESCE(v_pending_scope_count, 0),
            'unknown_scope_count', COALESCE(v_unknown_scope_count, 0),
            'completed_transfer_count', COALESCE(v_operation_completed_transfer_count, 0),
            'no_bank_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
            'requires_full_batch_finalisation', v_requires_full_batch_finalisation,
            'full_batch_finalisation_safe', v_full_batch_finalisation_safe,
            'finalisation_idempotency_key', v_finalisation_idempotency_key,
            'finalisation_payload_hash', v_finalisation_payload_hash,
            'projection_changed_after_authorisation', v_current_projection_changed,
            'projection_diagnostic', v_projection_diagnostic_json,
            'scope_settlement_updated_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    v_live_signal_result := public.banking_pay_batch_signal_touch(
      p_pay_batch_id := p_pay_batch_id,
      p_change_reason := 'PAY_SETTLE_RAIL_OPERATION_CHUNK',
      p_change_source := 'pay_settle_rail',
      p_change_scope_json := jsonb_strip_nulls(jsonb_build_object(
        'operation_id', p_operation_id::text,
        'settlement_scope_ids', COALESCE(p_settlement_scope_ids, '[]'::jsonb),
        'settled_this_chunk', COALESCE(v_settled_this_chunk, 0),
        'reused_this_chunk', COALESCE(v_reused_this_chunk, 0),
        'failed_this_chunk', COALESCE(v_failed_this_chunk, 0),
        'pending_this_chunk', COALESCE(v_pending_this_chunk, 0),
        'remaining', COALESCE(v_remaining_scope_count, 0),
        'scope_settlement_complete', v_scope_settlement_complete,
        'successful_scope_count', COALESCE(v_successful_scope_count, 0),
        'terminal_failed_scope_count', COALESCE(v_terminal_failed_scope_count, 0),
        'pending_scope_count', COALESCE(v_pending_scope_count, 0),
        'unknown_scope_count', COALESCE(v_unknown_scope_count, 0),
        'processor_failed_scope_count', COALESCE(v_processor_failed_scope_count, 0),
        'all_scopes_terminal', v_all_scopes_terminal,
        'all_scopes_successful', v_all_scopes_successful,
        'completed_with_failed_payments', v_completed_with_failed_payments,
        'batch_status_label', v_batch_status_label,
        'completed_transfer_count', COALESCE(v_operation_completed_transfer_count, 0),
        'no_bank_scopes_settled_this_chunk', COALESCE(v_no_bank_settled_this_chunk, 0),
        'no_bank_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
        'no_bank_payment_execution', v_no_bank_payment_execution_validated,
        'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_marker,
        'proposed_local_commit_reference', CASE WHEN v_no_bank_scope_authorised THEN v_local_no_bank_commit_ref ELSE NULL::text END,
        'requires_full_batch_finalisation', v_requires_full_batch_finalisation,
        'full_batch_finalisation_safe', v_full_batch_finalisation_safe,
        'finalisation_idempotency_key', v_finalisation_idempotency_key,
        'finalisation_payload_hash', v_finalisation_payload_hash,
        'consumed_carry_forward_count', COALESCE(v_consumed_carry_forward_count, 0)
      )),
      p_touch_payment_status := true,
      p_touch_correction_progress := true,
      p_touch_alerts := false,
      p_touch_overview := true
    );

    RETURN JSONB_BUILD_OBJECT(
      'ok', true,
      'operation_mode', true,
      'operation_id', p_operation_id::text,
      'settlement_operation_id', p_operation_id::text,
      'execution_operation_id', v_execution_operation_id::text,
      'auth_request_id', v_operation_auth_request_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'settlement_mode', v_settlement_mode,
      'projection_scope', v_operation_projection_scope,
      'settled_this_chunk', COALESCE(v_settled_this_chunk, 0),
      'reused_this_chunk', COALESCE(v_reused_this_chunk, 0),
      'failed_this_chunk', COALESCE(v_failed_this_chunk, 0),
      'pending_this_chunk', COALESCE(v_pending_this_chunk, 0),
      'no_bank_scopes_settled_this_chunk', COALESCE(v_no_bank_settled_this_chunk, 0),
      'remaining', COALESCE(v_remaining_scope_count, 0),
      'has_more', v_has_more,
      'scope_settlement_complete', v_scope_settlement_complete,
      'all_scopes_terminal', v_all_scopes_terminal,
      'all_scopes_successful', v_all_scopes_successful,
      'completed_with_failed_payments', v_completed_with_failed_payments,
      'batch_status_label', v_batch_status_label,
      'total_scope_count', COALESCE(v_total_scope_count, 0),
      'successful_scope_count', COALESCE(v_successful_scope_count, 0),
      'terminal_failed_scope_count', COALESCE(v_terminal_failed_scope_count, 0),
      'pending_scope_count', COALESCE(v_pending_scope_count, 0),
      'unknown_scope_count', COALESCE(v_unknown_scope_count, 0),
      'processor_failed_scope_count', COALESCE(v_processor_failed_scope_count, 0),
      'failed_scope_count', COALESCE(v_total_failed_scope_count, 0),
      'completed_transfer_count', COALESCE(v_operation_completed_transfer_count, 0),
      'no_bank_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
      'eligible_no_bank_scope_count', COALESCE(v_no_bank_eligible_scope_count, 0),
      'invalid_no_bank_scope_count', COALESCE(v_no_bank_invalid_scope_count, 0)
    )
    || JSONB_BUILD_OBJECT(
      'server_owned_payment_projection_proof', true,
      'proof_validation_outcome', v_proof_validation_outcome,
      'no_bank_payment_execution', v_no_bank_payment_execution_validated,
      'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_marker,
      'authorised_paye_net_state_hash', v_expected_paye_net_state_hash,
      'current_paye_net_state_hash', v_current_paye_net_state_hash,
      'authorised_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
      'current_bank_payment_projection_hash', v_current_bank_payment_projection_hash,
      'authorised_missing_explicit_paye_input_count', v_expected_missing_count,
      'current_missing_explicit_paye_input_count', v_current_missing_count,
      'authorised_explicit_zero_count', v_expected_global_zero_count,
      'current_explicit_zero_count', v_global_zero_count,
      'authorised_scoped_explicit_zero_count', v_expected_zero_count,
      'current_scoped_explicit_zero_count', v_current_zero_count,
      'authorised_scoped_positive_bank_payment_count', v_expected_positive_count,
      'current_scoped_positive_bank_payment_count', v_current_positive_count,
      'proposed_local_commit_reference', CASE WHEN v_no_bank_scope_authorised THEN v_local_no_bank_commit_ref ELSE NULL::text END,
      'projection_changed_after_authorisation', v_current_projection_changed,
      'projection_diagnostic', v_projection_diagnostic_json,
      'execution_commit_state', v_execution_commit_state,
      'execution_commit_ref', v_execution_commit_ref,
      'execution_committed_at_utc', CASE WHEN v_execution_committed_at_utc IS NULL THEN NULL::text ELSE v_execution_committed_at_utc::text END,
      'remittance_ready', v_full_batch_finalisation_safe,
      'requires_full_batch_finalisation', v_requires_full_batch_finalisation,
      'full_batch_finalisation_safe', v_full_batch_finalisation_safe,
      'finalisation_idempotency_key', v_finalisation_idempotency_key,
      'finalisation_payload_hash', v_finalisation_payload_hash,
      'remittance_queued', false,
      'consumed_carry_forward_count', COALESCE(v_consumed_carry_forward_count, 0),
      'carry_forward_mark_consumed_result', COALESCE(v_carry_forward_mark_result, '{}'::jsonb),
      'live_signal', COALESCE(v_live_signal_result, '{}'::jsonb),
      'freshness', JSONB_BUILD_OBJECT(
        'freshness_validation_status', NULLIF(v_stored_freshness_status, ''),
        'freshness_result_hash', v_stored_freshness_result_hash,
        'freshness_scope_hash', v_stored_freshness_scope_hash,
        'freshness_operation_id', CASE WHEN v_stored_freshness_operation_id IS NULL THEN NULL::text ELSE v_stored_freshness_operation_id::text END,
        'source', 'stored_freshness_metadata_non_blocking'
      ),
      'message', 'Rail settlement scope chunk processed; durable batch finalisation remains the responsibility of full-batch pay_settle_rail.'
    );
  END IF;

  SELECT CASE
    WHEN JSONB_TYPEOF(pb.execution_intent_json) = 'object'
      THEN COALESCE(pb.execution_intent_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END
  INTO v_batch_intent_json
  FROM public.pay_batches AS pb
  WHERE pb.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_settle_rail: pay_batch not found';
  END IF;

  IF v_batch_intent_json = '{}'::jsonb THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_SETTLE_RAIL',
      'code', 'BATCH_EXECUTION_INTENT_REQUIRED',
      'message', 'Full-batch settlement requires the server-frozen batch execution intent.',
      'pay_batch_id', p_pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'operation_id', '')), '')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_execution_operation_id := (v_batch_intent_json->>'operation_id')::uuid;
    v_batch_intent_operation_id := v_execution_operation_id;
    v_execution_operation_resolved_from_batch_intent := true;
  END IF;

  IF v_execution_operation_id IS NULL THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_SETTLE_RAIL',
      'code', 'EXECUTION_OPERATION_REQUIRED',
      'message', 'Full-batch settlement cannot determine the authorised payment execution operation.',
      'pay_batch_id', p_pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  SELECT execution_operation.*
  INTO v_execution_operation_row
  FROM public.banking_pay_operations AS execution_operation
  WHERE execution_operation.id = v_execution_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_execution_operation_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id
     OR v_execution_operation_row.operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_SETTLE_RAIL',
      'code', 'EXECUTION_OPERATION_INVALID',
      'message', 'The batch execution intent points to a missing or incompatible payment execution operation.',
      'pay_batch_id', p_pay_batch_id::text,
      'execution_operation_id', v_execution_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.authoritative_payment_date,
    pb.authoritative_payment_date_source,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot,
    pb.bulk_reference,
    pb.batch_kind_fixed,
    pb.execution_intent_json,
    pb.settlement_confirmation_json,
    pb.execution_commit_state,
    pb.execution_commit_ref,
    pb.execution_committed_at_utc,
    pb.freshness_validation_status,
    pb.freshness_result_hash,
    pb.freshness_scope_hash,
    pb.freshness_result_json,
    pb.freshness_operation_id
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_settle_rail: pay_batch not found';
  end if;

  v_stored_freshness_status := upper(btrim(coalesce(v_batch.freshness_validation_status, '')));
  v_stored_freshness_result_hash := nullif(btrim(coalesce(v_batch.freshness_result_hash, '')), '');
  v_stored_freshness_scope_hash := nullif(btrim(coalesce(v_batch.freshness_scope_hash, '')), '');
  v_stored_freshness_result_json := coalesce(v_batch.freshness_result_json, '{}'::jsonb);
  v_stored_freshness_operation_id := v_batch.freshness_operation_id;

  v_batch_intent_json := CASE
    WHEN JSONB_TYPEOF(v_batch.execution_intent_json) = 'object'
      THEN COALESCE(v_batch.execution_intent_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_execution_intent_json := v_batch_intent_json;

  IF v_execution_operation_resolved_from_batch_intent
     AND (
       CASE
         WHEN NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'operation_id', '')), '')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           THEN (v_batch_intent_json->>'operation_id')::uuid
         ELSE NULL::uuid
       END
     ) IS DISTINCT FROM v_execution_operation_id THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_SETTLE_RAIL',
      'code', 'EXECUTION_OPERATION_BINDING_CHANGED',
      'message', 'The batch execution-operation binding changed while full-batch settlement was acquiring its canonical locks.',
      'pay_batch_id', p_pay_batch_id::text,
      'locked_execution_operation_id', v_execution_operation_id::text,
      'current_batch_execution_operation_id', NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'operation_id', '')), '')
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_root_operation_input_json := COALESCE(v_execution_operation_row.input_json, '{}'::jsonb);

  IF NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'auth_request_id', '')), '')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_operation_auth_request_id := (v_batch_intent_json->>'auth_request_id')::uuid;
  END IF;

  IF v_operation_auth_request_id IS NULL THEN
    SELECT COUNT(*)::integer
    INTO v_auth_request_candidate_count
    FROM public.pay_batch_auth_requests AS auth_request_candidate
    WHERE auth_request_candidate.pay_batch_id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(auth_request_candidate.state, ''))) = 'AUTHORISED'
      AND JSONB_TYPEOF(auth_request_candidate.execution_intent_json) = 'object'
      AND NULLIF(BTRIM(COALESCE(auth_request_candidate.execution_intent_json->>'operation_id', '')), '') = v_execution_operation_id::text;

    IF COALESCE(v_auth_request_candidate_count, 0) = 1 THEN
      SELECT auth_request_candidate.id
      INTO v_operation_auth_request_id
      FROM public.pay_batch_auth_requests AS auth_request_candidate
      WHERE auth_request_candidate.pay_batch_id = p_pay_batch_id
        AND UPPER(BTRIM(COALESCE(auth_request_candidate.state, ''))) = 'AUTHORISED'
        AND JSONB_TYPEOF(auth_request_candidate.execution_intent_json) = 'object'
        AND NULLIF(BTRIM(COALESCE(auth_request_candidate.execution_intent_json->>'operation_id', '')), '') = v_execution_operation_id::text;
    ELSE
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'AUTHORISED_EXECUTION_INTENT_REQUIRED',
        'message', 'Full-batch settlement requires exactly one authorised auth request bound to the execution operation.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'authorised_auth_request_count', COALESCE(v_auth_request_candidate_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  SELECT
    auth_request.state,
    CASE
      WHEN JSONB_TYPEOF(auth_request.execution_intent_json) = 'object'
        THEN COALESCE(auth_request.execution_intent_json, '{}'::jsonb)
      ELSE '{}'::jsonb
    END
  INTO
    v_operation_auth_state,
    v_operation_auth_intent_json
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.id = v_operation_auth_request_id
    AND auth_request.pay_batch_id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND
     OR UPPER(BTRIM(COALESCE(v_operation_auth_state, ''))) <> 'AUTHORISED'
     OR JSONB_TYPEOF(v_operation_auth_intent_json) <> 'object'
     OR v_operation_auth_intent_json = '{}'::jsonb THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_SETTLE_RAIL',
      'code', 'AUTHORISED_EXECUTION_INTENT_REQUIRED',
      'message', 'The payment auth request is missing, belongs to another batch, is not authorised, or has no execution intent.',
      'pay_batch_id', p_pay_batch_id::text,
      'execution_operation_id', v_execution_operation_id::text,
      'auth_request_id', CASE WHEN v_operation_auth_request_id IS NULL THEN NULL::text ELSE v_operation_auth_request_id::text END,
      'auth_state', v_operation_auth_state
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'operation_id', '')), '')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_auth_intent_operation_id := (v_operation_auth_intent_json->>'operation_id')::uuid;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'auth_request_id', '')), '')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_auth_intent_auth_request_id := (v_operation_auth_intent_json->>'auth_request_id')::uuid;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'auth_request_id', '')), '')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_batch_intent_auth_request_id := (v_batch_intent_json->>'auth_request_id')::uuid;
  END IF;

  v_authorised_execution_mode := CASE
    WHEN UPPER(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
    WHEN UPPER(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
    WHEN UPPER(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
    ELSE UPPER(NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'execution_mode', '')), ''))
  END;
  v_batch_execution_mode := CASE
    WHEN UPPER(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
    WHEN UPPER(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
    WHEN UPPER(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
    ELSE UPPER(NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'execution_mode', '')), ''))
  END;
  v_operation_projection_scope := UPPER(NULLIF(BTRIM(COALESCE(
    v_operation_auth_intent_json->>'pay_channel_scope',
    v_operation_auth_intent_json->>'scope',
    ''
  )), ''));
  v_batch_projection_scope := UPPER(NULLIF(BTRIM(COALESCE(
    v_batch_intent_json->>'pay_channel_scope',
    v_batch_intent_json->>'scope',
    ''
  )), ''));

  v_expected_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
    v_operation_auth_intent_json->>'global_paye_net_state_hash',
    v_operation_auth_intent_json->>'paye_net_state_hash',
    ''
  )), '');
  v_batch_expected_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
    v_batch_intent_json->>'global_paye_net_state_hash',
    v_batch_intent_json->>'paye_net_state_hash',
    ''
  )), '');
  v_expected_global_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
    v_operation_auth_intent_json->>'global_bank_payment_projection_hash',
    ''
  )), '');
  v_batch_expected_global_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
    v_batch_intent_json->>'global_bank_payment_projection_hash',
    ''
  )), '');
  v_expected_scoped_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
    v_operation_auth_intent_json->>'scoped_paye_net_state_hash',
    ''
  )), '');
  v_batch_expected_scoped_paye_net_state_hash := NULLIF(BTRIM(COALESCE(
    v_batch_intent_json->>'scoped_paye_net_state_hash',
    ''
  )), '');
  v_expected_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
    v_operation_auth_intent_json->>'scoped_bank_payment_projection_hash',
    v_operation_auth_intent_json->>'bank_payment_projection_hash',
    ''
  )), '');
  v_batch_expected_bank_payment_projection_hash := NULLIF(BTRIM(COALESCE(
    v_batch_intent_json->>'scoped_bank_payment_projection_hash',
    v_batch_intent_json->>'bank_payment_projection_hash',
    ''
  )), '');

  IF COALESCE(v_operation_auth_intent_json->>'global_missing_explicit_paye_input_count', v_operation_auth_intent_json->>'missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
    v_expected_missing_count := COALESCE(v_operation_auth_intent_json->>'global_missing_explicit_paye_input_count', v_operation_auth_intent_json->>'missing_explicit_paye_input_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'global_missing_explicit_paye_input_count', v_batch_intent_json->>'missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_missing_count := COALESCE(v_batch_intent_json->>'global_missing_explicit_paye_input_count', v_batch_intent_json->>'missing_explicit_paye_input_count')::integer;
  END IF;
  IF COALESCE(v_operation_auth_intent_json->>'global_explicit_zero_count', v_operation_auth_intent_json->>'explicit_zero_count', '') ~ '^[0-9]+$' THEN
    v_expected_global_zero_count := COALESCE(v_operation_auth_intent_json->>'global_explicit_zero_count', v_operation_auth_intent_json->>'explicit_zero_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'global_explicit_zero_count', v_batch_intent_json->>'explicit_zero_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_global_zero_count := COALESCE(v_batch_intent_json->>'global_explicit_zero_count', v_batch_intent_json->>'explicit_zero_count')::integer;
  END IF;
  IF COALESCE(v_operation_auth_intent_json->>'global_positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
    v_expected_global_positive_count := (v_operation_auth_intent_json->>'global_positive_bank_payment_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'global_positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_global_positive_count := (v_batch_intent_json->>'global_positive_bank_payment_count')::integer;
  END IF;
  IF COALESCE(v_operation_auth_intent_json->>'global_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
    v_expected_global_invalid_count := (v_operation_auth_intent_json->>'global_invalid_payment_row_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'global_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_global_invalid_count := (v_batch_intent_json->>'global_invalid_payment_row_count')::integer;
  END IF;
  IF COALESCE(v_operation_auth_intent_json->>'scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
    v_expected_scoped_missing_count := (v_operation_auth_intent_json->>'scoped_missing_explicit_paye_input_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_scoped_missing_count := (v_batch_intent_json->>'scoped_missing_explicit_paye_input_count')::integer;
  END IF;
  IF COALESCE(v_operation_auth_intent_json->>'scoped_explicit_zero_count', '') ~ '^[0-9]+$' THEN
    v_expected_zero_count := (v_operation_auth_intent_json->>'scoped_explicit_zero_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'scoped_explicit_zero_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_zero_count := (v_batch_intent_json->>'scoped_explicit_zero_count')::integer;
  END IF;
  IF COALESCE(v_operation_auth_intent_json->>'scoped_positive_bank_payment_count', v_operation_auth_intent_json->>'positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
    v_expected_positive_count := COALESCE(v_operation_auth_intent_json->>'scoped_positive_bank_payment_count', v_operation_auth_intent_json->>'positive_bank_payment_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'scoped_positive_bank_payment_count', v_batch_intent_json->>'positive_bank_payment_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_positive_count := COALESCE(v_batch_intent_json->>'scoped_positive_bank_payment_count', v_batch_intent_json->>'positive_bank_payment_count')::integer;
  END IF;
  IF COALESCE(v_operation_auth_intent_json->>'scoped_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
    v_expected_scoped_invalid_count := (v_operation_auth_intent_json->>'scoped_invalid_payment_row_count')::integer;
  END IF;
  IF COALESCE(v_batch_intent_json->>'scoped_invalid_payment_row_count', '') ~ '^[0-9]+$' THEN
    v_batch_expected_scoped_invalid_count := (v_batch_intent_json->>'scoped_invalid_payment_row_count')::integer;
  END IF;

  v_server_owned_projection_proof := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_batch_server_owned_projection_proof := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_scoped_no_transfer_marker := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'scoped_no_transfer_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_batch_scoped_no_transfer_marker := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'scoped_no_transfer_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_no_bank_payment_marker := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_batch_no_bank_payment_marker := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_allow_explicit_zero_no_bank_scopes_marker := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_batch_allow_explicit_zero_no_bank_scopes_marker := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_suppress_remittances := LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_batch_suppress_remittances := LOWER(BTRIM(COALESCE(v_batch_intent_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_authorised_payment_date_raw := NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'payment_date', '')), '');
  v_batch_authorised_payment_date_raw := NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'payment_date', '')), '');
  IF v_authorised_payment_date_raw IS NOT NULL THEN
    BEGIN
      v_authorised_payment_date := v_authorised_payment_date_raw::date;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'PAY_SETTLE_RAIL_AUTH_INTENT_PAYMENT_DATE_INVALID' USING ERRCODE = 'P0001';
    END;
  END IF;
  IF v_batch_authorised_payment_date_raw IS NOT NULL THEN
    BEGIN
      v_batch_authorised_payment_date := v_batch_authorised_payment_date_raw::date;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'PAY_SETTLE_RAIL_BATCH_INTENT_PAYMENT_DATE_INVALID' USING ERRCODE = 'P0001';
    END;
  END IF;

  IF v_auth_intent_operation_id IS NULL
     OR v_batch_intent_operation_id IS NULL
     OR v_auth_intent_operation_id <> v_execution_operation_id
     OR v_batch_intent_operation_id <> v_execution_operation_id
     OR v_auth_intent_auth_request_id IS DISTINCT FROM v_operation_auth_request_id
     OR v_batch_intent_auth_request_id IS DISTINCT FROM v_operation_auth_request_id
     OR v_authorised_execution_mode NOT IN ('STANDARD_BANK', 'CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
     OR v_authorised_execution_mode IS DISTINCT FROM v_batch_execution_mode
     OR v_operation_projection_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA', 'LOANS')
     OR v_operation_projection_scope IS DISTINCT FROM v_batch_projection_scope
     OR v_expected_paye_net_state_hash IS NULL
     OR v_expected_paye_net_state_hash IS DISTINCT FROM v_batch_expected_paye_net_state_hash
     OR v_expected_global_bank_payment_projection_hash IS NULL
     OR v_expected_global_bank_payment_projection_hash IS DISTINCT FROM v_batch_expected_global_bank_payment_projection_hash
     OR v_expected_scoped_paye_net_state_hash IS NULL
     OR v_expected_scoped_paye_net_state_hash IS DISTINCT FROM v_batch_expected_scoped_paye_net_state_hash
     OR v_expected_bank_payment_projection_hash IS NULL
     OR v_expected_bank_payment_projection_hash IS DISTINCT FROM v_batch_expected_bank_payment_projection_hash
     OR v_expected_missing_count IS NULL
     OR v_expected_missing_count IS DISTINCT FROM v_batch_expected_missing_count
     OR v_expected_global_zero_count IS NULL
     OR v_expected_global_zero_count IS DISTINCT FROM v_batch_expected_global_zero_count
     OR v_expected_global_positive_count IS NULL
     OR v_expected_global_positive_count IS DISTINCT FROM v_batch_expected_global_positive_count
     OR v_expected_global_invalid_count IS NULL
     OR v_expected_global_invalid_count IS DISTINCT FROM v_batch_expected_global_invalid_count
     OR v_expected_scoped_missing_count IS NULL
     OR v_expected_scoped_missing_count IS DISTINCT FROM v_batch_expected_scoped_missing_count
     OR v_expected_zero_count IS NULL
     OR v_expected_zero_count IS DISTINCT FROM v_batch_expected_zero_count
     OR v_expected_positive_count IS NULL
     OR v_expected_positive_count IS DISTINCT FROM v_batch_expected_positive_count
     OR v_expected_scoped_invalid_count IS NULL
     OR v_expected_scoped_invalid_count IS DISTINCT FROM v_batch_expected_scoped_invalid_count
     OR v_server_owned_projection_proof IS NOT TRUE
     OR v_batch_server_owned_projection_proof IS NOT TRUE
     OR v_scoped_no_transfer_marker IS DISTINCT FROM v_batch_scoped_no_transfer_marker
     OR v_no_bank_payment_marker IS DISTINCT FROM v_batch_no_bank_payment_marker
     OR v_allow_explicit_zero_no_bank_scopes_marker IS DISTINCT FROM v_batch_allow_explicit_zero_no_bank_scopes_marker
     OR v_suppress_remittances IS DISTINCT FROM v_batch_suppress_remittances
     OR v_authorised_payment_date IS DISTINCT FROM v_batch_authorised_payment_date THEN
    RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
      'error', 'PAY_SETTLE_RAIL',
      'code', 'AUTH_BATCH_EXECUTION_INTENT_MISMATCH',
      'message', 'The authorised execution intent and batch execution intent are missing required server proof or are materially inconsistent.',
      'pay_batch_id', p_pay_batch_id::text,
      'execution_operation_id', v_execution_operation_id::text,
      'auth_request_id', v_operation_auth_request_id::text,
      'authorised_execution_mode', v_authorised_execution_mode,
      'authorised_scope', v_operation_projection_scope
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_settlement_mode := v_authorised_execution_mode;
  v_effective_payment_date := COALESCE(
    v_authorised_payment_date,
    v_batch.authoritative_payment_date,
    v_batch.pay_date
  );
  v_execution_commit_state := UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED')));
  IF v_execution_commit_state NOT IN ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED') THEN
    v_execution_commit_state := 'NOT_SUBMITTED';
  END IF;
  v_execution_commit_ref := v_batch.execution_commit_ref;
  v_execution_committed_at_utc := v_batch.execution_committed_at_utc;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_projection_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_projection_rows
  ON COMMIT DROP
  AS
  SELECT projection_row.*
  FROM public._pay_batch_bank_payment_projection_rows(p_pay_batch_id, 'ALL') AS projection_row;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_scoped_projection_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_scoped_projection_rows
  ON COMMIT DROP
  AS
  SELECT projection_row.*
  FROM public._pay_batch_bank_payment_projection_rows(p_pay_batch_id, v_operation_projection_scope) AS projection_row;

  SELECT
    COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'MISSING')::integer,
    COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'ZERO')::integer,
    COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer,
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
    v_current_missing_count,
    v_global_zero_count,
    v_global_positive_count,
    v_global_invalid_count,
    v_current_paye_net_state_hash,
    v_all_bank_payment_projection_hash
  FROM pg_temp.tmp_pay_settle_rail_projection_rows AS projection_row;

  SELECT
    COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'MISSING')::integer,
    COUNT(*) FILTER (WHERE projection_row.is_paye_net_state_row AND projection_row.paye_net_classification = 'ZERO')::integer,
    COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer,
    ROUND(COALESCE(SUM(projection_row.amount) FILTER (WHERE projection_row.is_positive_bank_payment), 0), 2)::numeric(14,2),
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
    v_current_scoped_missing_count,
    v_current_zero_count,
    v_current_positive_count,
    v_current_positive_total,
    v_current_scoped_invalid_count,
    v_current_scoped_paye_net_state_hash,
    v_current_bank_payment_projection_hash
  FROM pg_temp.tmp_pay_settle_rail_scoped_projection_rows AS projection_row;

  v_current_missing_count := COALESCE(v_current_missing_count, 0);
  v_global_zero_count := COALESCE(v_global_zero_count, 0);
  v_global_positive_count := COALESCE(v_global_positive_count, 0);
  v_global_invalid_count := COALESCE(v_global_invalid_count, 0);
  v_current_scoped_missing_count := COALESCE(v_current_scoped_missing_count, 0);
  v_current_zero_count := COALESCE(v_current_zero_count, 0);
  v_current_positive_count := COALESCE(v_current_positive_count, 0);
  v_current_positive_total := ROUND(COALESCE(v_current_positive_total, 0), 2);
  v_current_scoped_invalid_count := COALESCE(v_current_scoped_invalid_count, 0);
  v_current_paye_net_state_hash := COALESCE(v_current_paye_net_state_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', 'ALL', 'rows', '[]'::jsonb)::text));
  v_all_bank_payment_projection_hash := COALESCE(v_all_bank_payment_projection_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', 'ALL', 'rows', '[]'::jsonb)::text));
  v_current_scoped_paye_net_state_hash := COALESCE(v_current_scoped_paye_net_state_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', v_operation_projection_scope, 'rows', '[]'::jsonb)::text));
  v_current_bank_payment_projection_hash := COALESCE(v_current_bank_payment_projection_hash, MD5(JSONB_BUILD_OBJECT('pay_batch_id', p_pay_batch_id::text, 'scope', v_operation_projection_scope, 'rows', '[]'::jsonb)::text));

  v_current_projection_changed := (
    v_expected_paye_net_state_hash IS DISTINCT FROM v_current_paye_net_state_hash
    OR v_expected_global_bank_payment_projection_hash IS DISTINCT FROM v_all_bank_payment_projection_hash
    OR v_expected_scoped_paye_net_state_hash IS DISTINCT FROM v_current_scoped_paye_net_state_hash
    OR v_expected_bank_payment_projection_hash IS DISTINCT FROM v_current_bank_payment_projection_hash
    OR v_expected_missing_count IS DISTINCT FROM v_current_missing_count
    OR v_expected_global_zero_count IS DISTINCT FROM v_global_zero_count
    OR v_expected_global_positive_count IS DISTINCT FROM v_global_positive_count
    OR v_expected_global_invalid_count IS DISTINCT FROM v_global_invalid_count
    OR v_expected_scoped_missing_count IS DISTINCT FROM v_current_scoped_missing_count
    OR v_expected_zero_count IS DISTINCT FROM v_current_zero_count
    OR v_expected_positive_count IS DISTINCT FROM v_current_positive_count
    OR v_expected_scoped_invalid_count IS DISTINCT FROM v_current_scoped_invalid_count
  );

  v_projection_diagnostic_json := JSONB_BUILD_OBJECT(
    'projection_changed_after_authorisation', v_current_projection_changed,
    'settlement_blocked_by_projection_change', false,
    'authorised_global_paye_net_state_hash', v_expected_paye_net_state_hash,
    'current_global_paye_net_state_hash', v_current_paye_net_state_hash,
    'authorised_global_bank_payment_projection_hash', v_expected_global_bank_payment_projection_hash,
    'current_global_bank_payment_projection_hash', v_all_bank_payment_projection_hash,
    'authorised_scoped_paye_net_state_hash', v_expected_scoped_paye_net_state_hash,
    'current_scoped_paye_net_state_hash', v_current_scoped_paye_net_state_hash,
    'authorised_scoped_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
    'current_scoped_bank_payment_projection_hash', v_current_bank_payment_projection_hash,
    'authorised_scope', v_operation_projection_scope,
    'diagnostic_generated_at_utc', v_now::text
  );

  v_no_bank_scope_authorised := v_allow_explicit_zero_no_bank_scopes_marker
    AND COALESCE(v_expected_missing_count, -1) = 0
    AND COALESCE(v_expected_scoped_missing_count, -1) = 0
    AND COALESCE(v_expected_zero_count, 0) > 0
    AND COALESCE(v_expected_scoped_invalid_count, -1) = 0;
  v_no_bank_payment_execution_validated := v_no_bank_payment_marker
    AND v_scoped_no_transfer_marker
    AND v_no_bank_scope_authorised
    AND COALESCE(v_expected_positive_count, -1) = 0
    AND COALESCE(v_expected_global_positive_count, -1) = 0
    AND COALESCE(v_expected_global_invalid_count, -1) = 0;
  v_local_no_bank_commit_ref := 'NO_BANK_PAYMENT:' || v_operation_auth_request_id::text;
  v_proof_validation_outcome := CASE
    WHEN v_no_bank_payment_execution_validated THEN 'VALIDATED_NO_BANK_PAYMENT_FROM_AUTHORISED_INTENT'
    WHEN v_no_bank_scope_authorised AND COALESCE(v_expected_positive_count, 0) > 0 THEN 'VALIDATED_MIXED_ZERO_AND_TRANSFER_FROM_AUTHORISED_INTENT'
    WHEN v_no_bank_scope_authorised THEN 'VALIDATED_SCOPED_NO_TRANSFER_FROM_AUTHORISED_INTENT'
    ELSE 'VALIDATED_TRANSFER_BACKED_FROM_AUTHORISED_INTENT'
  END;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope (
    settlement_scope_id uuid PRIMARY KEY,
    pay_batch_candidate_id uuid NOT NULL,
    candidate_id uuid NOT NULL
  ) ON COMMIT DROP;

  IF v_no_bank_scope_authorised THEN
    IF v_execution_operation_row.operation_type <> 'PAYMENT_EXECUTE' THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'NO_BANK_PAYMENT_EXECUTION_OPERATION_REQUIRED',
        'message', 'Explicit-zero no-bank finalisation must remain bound to the original PAYMENT_EXECUTE operation and cannot be introduced by a blocked-funds retry.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'execution_operation_type', v_execution_operation_row.operation_type
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_explicit_settlement_operation_id := NULL::uuid;
    IF NULLIF(BTRIM(COALESCE(
      v_batch_intent_json->>'settlement_operation_id',
      v_operation_auth_intent_json->>'settlement_operation_id',
      v_batch.settlement_confirmation_json->>'settlement_operation_id',
      v_batch.settlement_confirmation_json->>'operation_id',
      ''
    )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_explicit_settlement_operation_id := COALESCE(
        v_batch_intent_json->>'settlement_operation_id',
        v_operation_auth_intent_json->>'settlement_operation_id',
        v_batch.settlement_confirmation_json->>'settlement_operation_id',
        v_batch.settlement_confirmation_json->>'operation_id'
      )::uuid;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_no_bank_operation_ids;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_no_bank_operation_ids (
      operation_id uuid PRIMARY KEY
    ) ON COMMIT DROP;

    IF v_explicit_settlement_operation_id IS NOT NULL THEN
      INSERT INTO pg_temp.tmp_pay_settle_rail_no_bank_operation_ids (operation_id)
      SELECT settlement_operation.id
      FROM public.banking_pay_operations AS settlement_operation
      WHERE settlement_operation.id = v_explicit_settlement_operation_id
        AND settlement_operation.pay_batch_id = p_pay_batch_id
        AND (
          (
            settlement_operation.operation_type = 'PAYMENT_EXECUTE'
            AND settlement_operation.id = v_execution_operation_id
          )
          OR (
            settlement_operation.operation_type = 'PAYMENT_SETTLEMENT'
            AND (
              settlement_operation.root_operation_id = v_execution_operation_id
              OR (
                settlement_operation.root_operation_id IS NULL
                AND EXISTS (
                  SELECT 1
                  FROM public.banking_pay_operation_settlement_scope AS explicit_scope_binding
                  WHERE explicit_scope_binding.operation_id = settlement_operation.id
                    AND explicit_scope_binding.pay_batch_id = p_pay_batch_id
                    AND NULLIF(BTRIM(COALESCE(explicit_scope_binding.payload_json->>'execution_operation_id', '')), '') = v_execution_operation_id::text
                    AND NULLIF(BTRIM(COALESCE(explicit_scope_binding.payload_json->>'auth_request_id', '')), '') = v_operation_auth_request_id::text
                )
              )
            )
          )
        )
      ON CONFLICT (operation_id) DO NOTHING;

      IF NOT EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_settle_rail_no_bank_operation_ids AS explicit_operation
        WHERE explicit_operation.operation_id = v_explicit_settlement_operation_id
      ) THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'NO_BANK_PAYMENT_SETTLEMENT_OPERATION_MISMATCH',
          'message', 'The stored settlement-operation locator is not compatible with the authorised execution intent.',
          'pay_batch_id', p_pay_batch_id::text,
          'execution_operation_id', v_execution_operation_id::text,
          'settlement_operation_id', v_explicit_settlement_operation_id::text
        )::text USING ERRCODE = 'P0001';
      END IF;
    END IF;

    INSERT INTO pg_temp.tmp_pay_settle_rail_no_bank_operation_ids (operation_id)
    SELECT DISTINCT no_bank_operation.id
    FROM public.banking_pay_operation_settlement_scope AS no_bank_scope
    JOIN public.banking_pay_operations AS no_bank_operation
      ON no_bank_operation.id = no_bank_scope.operation_id
     AND no_bank_operation.pay_batch_id = p_pay_batch_id
    WHERE no_bank_scope.pay_batch_id = p_pay_batch_id
      AND no_bank_scope.status = 'SETTLED'
      AND UPPER(BTRIM(COALESCE(no_bank_scope.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
      AND UPPER(BTRIM(COALESCE(no_bank_scope.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
      AND NULLIF(BTRIM(COALESCE(no_bank_scope.payload_json->>'execution_operation_id', '')), '') = v_execution_operation_id::text
      AND NULLIF(BTRIM(COALESCE(no_bank_scope.payload_json->>'auth_request_id', '')), '') = v_operation_auth_request_id::text
      AND (
        (
          no_bank_operation.operation_type = 'PAYMENT_EXECUTE'
          AND no_bank_operation.id = v_execution_operation_id
        )
        OR (
          no_bank_operation.operation_type = 'PAYMENT_SETTLEMENT'
          AND (
            no_bank_operation.root_operation_id = v_execution_operation_id
            OR no_bank_operation.root_operation_id IS NULL
          )
        )
      )
    ON CONFLICT (operation_id) DO NOTHING;

    SELECT COUNT(*)::integer
    INTO v_no_bank_settlement_operation_count
    FROM pg_temp.tmp_pay_settle_rail_no_bank_operation_ids AS candidate_operation;

    IF COALESCE(v_no_bank_settlement_operation_count, 0) <> 1 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'NO_BANK_PAYMENT_SETTLEMENT_OPERATION_REQUIRED',
        'message', 'Full no-bank finalisation requires exactly one settlement operation bound to the authorised execution proof.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text,
        'compatible_settlement_operation_count', COALESCE(v_no_bank_settlement_operation_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;

    SELECT candidate_operation.operation_id
    INTO v_no_bank_settlement_operation_id
    FROM pg_temp.tmp_pay_settle_rail_no_bank_operation_ids AS candidate_operation;

    DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope
    ON COMMIT DROP
    AS
    SELECT DISTINCT
      scope_row.id AS settlement_scope_id,
      scope_row.pay_batch_candidate_id,
      scope_row.candidate_id
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = scope_row.pay_batch_candidate_id
     AND batch_candidate.pay_batch_id = p_pay_batch_id
    WHERE scope_row.operation_id = v_no_bank_settlement_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND scope_row.status = 'SETTLED'
      AND scope_row.settlement_event_id IS NULL
      AND UPPER(BTRIM(COALESCE(scope_row.pay_channel, ''))) = 'PAYE'
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_batch_id', '')), '') = p_pay_batch_id::text
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_batch_candidate_id', '')), '') = scope_row.pay_batch_candidate_id::text
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'candidate_id', '')), '') = scope_row.candidate_id::text
      AND UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_channel', '')), '')) = 'PAYE'
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_bank_transfer_id', '')), '') IS NULL
      AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
      AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
      AND LOWER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_scope', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(scope_row.payload_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND UPPER(BTRIM(COALESCE(scope_row.payload_json #>> '{payment_scope_json,scope_kind}', ''))) = 'NO_BANK_PAYMENT'
      AND UPPER(BTRIM(COALESCE(scope_row.payload_json #>> '{payment_scope_json,no_bank_payment_reason}', ''))) = 'EXPLICIT_ZERO_PAYE'
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '') IS NULL
      AND LOWER(BTRIM(COALESCE(scope_row.payload_json->>'has_effective_paye_input', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND COALESCE(scope_row.payload_json->>'net_bank_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND ROUND((scope_row.payload_json->>'net_bank_amount')::numeric, 2) = 0
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'effective_paye_net_input_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_paye_net_state_hash', scope_row.payload_json->>'paye_net_state_hash', '')), '') = v_expected_paye_net_state_hash
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'global_bank_payment_projection_hash', '')), '') = v_expected_global_bank_payment_projection_hash
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'scoped_paye_net_state_hash', '')), '') = v_expected_scoped_paye_net_state_hash
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_bank_payment_projection_hash', scope_row.payload_json->>'bank_payment_projection_hash', '')), '') = v_expected_bank_payment_projection_hash
      AND UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'projection_scope', '')), '')) IS NOT DISTINCT FROM v_operation_projection_scope
      AND UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_scope', scope_row.payload_json->>'projection_scope', '')), '')) IS NOT DISTINCT FROM v_operation_projection_scope
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'execution_operation_id', '')), '') = v_execution_operation_id::text
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'settlement_operation_id', '')), '') = v_no_bank_settlement_operation_id::text
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'auth_request_id', '')), '') = v_operation_auth_request_id::text
      AND UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'authorised_execution_mode', '')), '')) IS NOT DISTINCT FROM v_authorised_execution_mode
      AND (
        NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'settlement_mode', '')), '') IS NULL
        OR UPPER(NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'settlement_mode', '')), '')) = v_settlement_mode
      )
      AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'scoped_no_transfer_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_scoped_no_transfer_marker
      AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_no_bank_payment_marker
      AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_allow_explicit_zero_no_bank_scopes_marker
      AND (LOWER(BTRIM(COALESCE(scope_row.payload_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) = v_suppress_remittances
      AND COALESCE(scope_row.payload_json->>'authorised_missing_explicit_paye_input_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_missing_explicit_paye_input_count')::integer = v_expected_missing_count
      AND COALESCE(scope_row.payload_json->>'authorised_explicit_zero_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_explicit_zero_count')::integer = v_expected_global_zero_count
      AND COALESCE(scope_row.payload_json->>'authorised_global_positive_bank_payment_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_global_positive_bank_payment_count')::integer = v_expected_global_positive_count
      AND COALESCE(scope_row.payload_json->>'authorised_global_invalid_payment_row_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_global_invalid_payment_row_count')::integer = v_expected_global_invalid_count
      AND COALESCE(scope_row.payload_json->>'authorised_scoped_missing_explicit_paye_input_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_scoped_missing_explicit_paye_input_count')::integer = v_expected_scoped_missing_count
      AND COALESCE(scope_row.payload_json->>'authorised_scoped_explicit_zero_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_scoped_explicit_zero_count')::integer = v_expected_zero_count
      AND COALESCE(scope_row.payload_json->>'authorised_scoped_positive_bank_payment_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_scoped_positive_bank_payment_count')::integer = v_expected_positive_count
      AND COALESCE(scope_row.payload_json->>'authorised_scoped_invalid_payment_row_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'authorised_scoped_invalid_payment_row_count')::integer = v_expected_scoped_invalid_count
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'local_commit_reference', '')), '') = v_local_no_bank_commit_ref
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'settled_at_utc', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'settled_by_user_id', '')), '') IS NOT NULL
      AND (
        (
          v_no_bank_payment_execution_validated
          AND v_settlement_mode = 'CSV_SETTLEMENT'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'confirmation_mode', ''))) = 'NO_BANK_PAYMENT_REVIEW'
        )
        OR (
          v_no_bank_payment_execution_validated
          AND v_settlement_mode = 'EXTERNAL_SETTLEMENT'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'confirmation_mode', ''))) IN ('NO_BANK_PAYMENT_EXTERNAL_CONFIRMATION', 'NO_BANK_PAYMENT_EXECUTION')
        )
        OR (
          v_no_bank_payment_execution_validated
          AND v_settlement_mode = 'STANDARD_BANK'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'confirmation_mode', ''))) = 'NO_BANK_PAYMENT_EXECUTION'
        )
        OR (
          v_no_bank_payment_execution_validated IS NOT TRUE
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'confirmation_mode', ''))) = 'EXPLICIT_ZERO_NO_BANK_SCOPE'
        )
      )
      AND JSONB_TYPEOF(scope_row.payload_json->'pay_batch_item_ids') = 'array'
      AND JSONB_ARRAY_LENGTH(scope_row.payload_json->'pay_batch_item_ids') > 0
      AND COALESCE(scope_row.payload_json->>'item_count', '') ~ '^[0-9]+$'
      AND (scope_row.payload_json->>'item_count')::integer = JSONB_ARRAY_LENGTH(scope_row.payload_json->'pay_batch_item_ids')
      AND NOT EXISTS (
        SELECT 1
        FROM JSONB_ARRAY_ELEMENTS_TEXT(scope_row.payload_json->'pay_batch_item_ids') AS item_element(item_id_text)
        WHERE item_element.item_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR NOT EXISTS (
             SELECT 1
             FROM public.pay_batch_items AS payload_item
             WHERE payload_item.id = CASE
                     WHEN item_element.item_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                       THEN item_element.item_id_text::uuid
                     ELSE NULL::uuid
                   END
               AND payload_item.pay_batch_candidate_id = scope_row.pay_batch_candidate_id
               AND UPPER(BTRIM(COALESCE(payload_item.pay_channel, ''))) = 'PAYE'
               AND COALESCE(payload_item.is_voided, false) = false
               AND COALESCE(payload_item.item_type, '') <> 'DEBT_CREATED'
               AND payload_item.pay_bank_transfer_id IS NULL
           )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS candidate_scope_item
        WHERE candidate_scope_item.pay_batch_candidate_id = scope_row.pay_batch_candidate_id
          AND UPPER(BTRIM(COALESCE(candidate_scope_item.pay_channel, ''))) = 'PAYE'
          AND COALESCE(candidate_scope_item.is_voided, false) = false
          AND COALESCE(candidate_scope_item.item_type, '') <> 'DEBT_CREATED'
          AND NOT (scope_row.payload_json->'pay_batch_item_ids' @> JSONB_BUILD_ARRAY(TO_JSONB(candidate_scope_item.id::text)))
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS linked_item
        WHERE linked_item.pay_batch_candidate_id = scope_row.pay_batch_candidate_id
          AND UPPER(BTRIM(COALESCE(linked_item.pay_channel, ''))) = 'PAYE'
          AND COALESCE(linked_item.is_voided, false) = false
          AND linked_item.pay_bank_transfer_id IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_transfer_scope AS transfer_scope_evidence
        WHERE transfer_scope_evidence.pay_batch_id = p_pay_batch_id
          AND (
            transfer_scope_evidence.candidate_id = scope_row.candidate_id
            OR transfer_scope_evidence.candidate_id IS NULL
          )
          AND UPPER(BTRIM(COALESCE(transfer_scope_evidence.pay_channel, ''))) = 'PAYE'
          AND (
            transfer_scope_evidence.pay_bank_transfer_id IS NOT NULL
            OR transfer_scope_evidence.provider_submit_attempt_count > 0
            OR transfer_scope_evidence.provider_request_sent_at_utc IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_scope_evidence.provider_request_id, '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_scope_evidence.provider_transaction_id, '')), '') IS NOT NULL
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfers AS transfer_evidence
        WHERE transfer_evidence.pay_batch_id = p_pay_batch_id
          AND (
            transfer_evidence.candidate_id = scope_row.candidate_id
            OR transfer_evidence.candidate_id IS NULL
          )
          AND UPPER(BTRIM(COALESCE(transfer_evidence.pay_channel, ''))) = 'PAYE'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS transfer_event_evidence
        LEFT JOIN public.pay_bank_transfers AS event_transfer_evidence
          ON event_transfer_evidence.id = transfer_event_evidence.pay_bank_transfer_id
        WHERE transfer_event_evidence.pay_batch_id = p_pay_batch_id
          AND (
            COALESCE(transfer_event_evidence.candidate_id, event_transfer_evidence.candidate_id) = scope_row.candidate_id
            OR (
              transfer_event_evidence.candidate_id IS NULL
              AND transfer_event_evidence.pay_bank_transfer_id IS NULL
            )
          )
          AND (
            transfer_event_evidence.pay_bank_transfer_id IS NULL
            OR UPPER(BTRIM(COALESCE(event_transfer_evidence.pay_channel, ''))) = 'PAYE'
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_provider_attempts AS provider_attempt_evidence
        LEFT JOIN public.banking_pay_operation_transfer_scope AS provider_attempt_scope
          ON provider_attempt_scope.id = provider_attempt_evidence.transfer_scope_id
        WHERE (
            provider_attempt_evidence.pay_batch_id = p_pay_batch_id
            OR provider_attempt_scope.pay_batch_id = p_pay_batch_id
          )
          AND (
            (
              provider_attempt_scope.candidate_id = scope_row.candidate_id
              AND UPPER(BTRIM(COALESCE(provider_attempt_scope.pay_channel, ''))) = 'PAYE'
            )
            OR (
              provider_attempt_evidence.transfer_scope_id IS NULL
              AND (
                provider_attempt_evidence.operation_id = v_execution_operation_id
                OR provider_attempt_evidence.pay_batch_id = p_pay_batch_id
              )
            )
          )
      );


    SELECT
      COUNT(*)::integer,
      COUNT(DISTINCT scope_row.pay_batch_candidate_id)::integer,
      COUNT(*) FILTER (WHERE scope_row.status NOT IN ('SETTLED', 'SKIPPED'))::integer,
      COUNT(*) FILTER (WHERE scope_row.status = 'FAILED')::integer
    INTO
      v_no_bank_total_scope_count,
      v_no_bank_distinct_candidate_count,
      v_no_bank_nonterminal_scope_count,
      v_no_bank_failed_scope_count
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    WHERE scope_row.operation_id = v_no_bank_settlement_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND scope_row.status <> 'SKIPPED'
      AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
      AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE';

    SELECT COUNT(*)::integer
    INTO v_no_bank_eligible_scope_count
    FROM pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope AS eligible_scope;

    v_no_bank_invalid_scope_count := GREATEST(
      COALESCE(v_no_bank_total_scope_count, 0) - COALESCE(v_no_bank_eligible_scope_count, 0),
      0
    );

    v_no_bank_missing_scope_count := GREATEST(
      COALESCE(v_expected_zero_count, 0) - COALESCE(v_no_bank_distinct_candidate_count, 0),
      0
    );

    IF COALESCE(v_no_bank_invalid_scope_count, 0) <> 0
       OR COALESCE(v_no_bank_missing_scope_count, 0) <> 0
       OR COALESCE(v_no_bank_nonterminal_scope_count, 0) <> 0
       OR COALESCE(v_no_bank_failed_scope_count, 0) <> 0
       OR COALESCE(v_no_bank_total_scope_count, 0) <> COALESCE(v_expected_zero_count, 0)
       OR COALESCE(v_no_bank_distinct_candidate_count, 0) <> COALESCE(v_expected_zero_count, 0) THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'NO_BANK_PAYMENT_SCOPE_EVIDENCE_INVALID',
        'message', 'The locally settled explicit-zero scopes are invalid, incomplete, duplicated, non-terminal, or no longer match the frozen zero-payment proof.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'settlement_operation_id', v_no_bank_settlement_operation_id::text,
        'expected_zero_candidate_count', COALESCE(v_expected_zero_count, 0),
        'settled_no_bank_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
        'eligible_no_bank_scope_count', COALESCE(v_no_bank_eligible_scope_count, 0),
        'distinct_no_bank_candidate_count', COALESCE(v_no_bank_distinct_candidate_count, 0),
        'missing_no_bank_scope_count', COALESCE(v_no_bank_missing_scope_count, 0),
        'nonterminal_no_bank_scope_count', COALESCE(v_no_bank_nonterminal_scope_count, 0),
        'failed_no_bank_scope_count', COALESCE(v_no_bank_failed_scope_count, 0),
        'invalid_no_bank_scope_count', COALESCE(v_no_bank_invalid_scope_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;

    IF COALESCE(v_expected_positive_count, 0) > 0 THEN
      SELECT
        COUNT(*)::integer,
        COUNT(*) FILTER (WHERE scope_row.status NOT IN ('SETTLED', 'SKIPPED'))::integer,
        COUNT(*) FILTER (WHERE scope_row.status = 'FAILED')::integer
      INTO
        v_positive_scope_count,
        v_positive_nonterminal_scope_count,
        v_positive_failed_scope_count
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.operation_id = v_no_bank_settlement_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND NOT (
          UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
        );

      IF COALESCE(v_positive_scope_count, 0) = 0
         OR COALESCE(v_positive_nonterminal_scope_count, 0) <> 0
         OR COALESCE(v_positive_failed_scope_count, 0) <> 0 THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'MIXED_SETTLEMENT_SCOPE_FINALISATION_INCOMPLETE',
          'message', 'Mixed positive/zero finalisation cannot continue until every transfer-backed scope is terminal and successful.',
          'pay_batch_id', p_pay_batch_id::text,
          'settlement_operation_id', v_no_bank_settlement_operation_id::text,
          'positive_scope_count', COALESCE(v_positive_scope_count, 0),
          'positive_nonterminal_scope_count', COALESCE(v_positive_nonterminal_scope_count, 0),
          'positive_failed_scope_count', COALESCE(v_positive_failed_scope_count, 0)
        )::text USING ERRCODE = 'P0001';
      END IF;
    END IF;

    IF v_no_bank_payment_execution_validated THEN
      IF JSONB_ARRAY_LENGTH(p_settlement_json) <> 0 THEN
        RAISE EXCEPTION 'PAY_SETTLE_RAIL_NO_BANK_PAYMENT_PAYLOAD_FORBIDDEN'
          USING ERRCODE = 'P0001',
                DETAIL = JSONB_BUILD_OBJECT(
                  'code', 'PAY_SETTLE_RAIL_NO_BANK_PAYMENT_PAYLOAD_FORBIDDEN',
                  'pay_batch_id', p_pay_batch_id::text,
                  'execution_operation_id', v_execution_operation_id::text,
                  'auth_request_id', v_operation_auth_request_id::text,
                  'message', 'Pure no-bank-payment full finalisation requires an empty settlement payload and cannot ingest transfer or provider evidence.'
                )::text;
      END IF;

      SELECT COUNT(*)::integer
      INTO v_no_bank_scope_artifact_count
      FROM public.banking_pay_operation_transfer_scope AS transfer_scope_row
      WHERE transfer_scope_row.operation_id = v_execution_operation_id
        AND transfer_scope_row.pay_batch_id = p_pay_batch_id;

      SELECT COUNT(*)::integer
      INTO v_no_bank_batch_transfer_count
      FROM public.pay_bank_transfers AS transfer_row
      WHERE transfer_row.pay_batch_id = p_pay_batch_id;

      SELECT COUNT(*)::integer
      INTO v_no_bank_transfer_event_count
      FROM public.pay_bank_transfer_events AS transfer_event_row
      WHERE transfer_event_row.pay_batch_id = p_pay_batch_id;

      SELECT COUNT(*)::integer
      INTO v_no_bank_provider_attempt_count
      FROM public.banking_pay_operation_provider_attempts AS provider_attempt_row
      WHERE provider_attempt_row.pay_batch_id = p_pay_batch_id
         OR provider_attempt_row.operation_id = v_execution_operation_id;

      IF COALESCE(v_no_bank_scope_artifact_count, 0) <> 0
         OR COALESCE(v_no_bank_batch_transfer_count, 0) <> 0
         OR COALESCE(v_no_bank_transfer_event_count, 0) <> 0
         OR COALESCE(v_no_bank_provider_attempt_count, 0) <> 0 THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'NO_BANK_PAYMENT_FINALISATION_EVIDENCE_CONFLICT',
          'message', 'No-bank-payment full finalisation cannot continue because transfer or provider-submission evidence exists.',
          'pay_batch_id', p_pay_batch_id::text,
          'execution_operation_id', v_execution_operation_id::text,
          'transfer_scope_count', COALESCE(v_no_bank_scope_artifact_count, 0),
          'bank_transfer_count', COALESCE(v_no_bank_batch_transfer_count, 0),
          'bank_transfer_event_count', COALESCE(v_no_bank_transfer_event_count, 0),
          'provider_attempt_count', COALESCE(v_no_bank_provider_attempt_count, 0)
        )::text USING ERRCODE = 'P0001';
      END IF;

      IF v_execution_commit_state NOT IN ('NOT_SUBMITTED', 'COMMITTED')
         OR (
           NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), '') IS NOT NULL
           AND NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), '') <> v_local_no_bank_commit_ref
         )
         OR (
           v_execution_commit_state = 'COMMITTED'
           AND (
             NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), '') IS DISTINCT FROM v_local_no_bank_commit_ref
             OR v_execution_committed_at_utc IS NULL
           )
         )
         OR (
           v_execution_commit_state = 'NOT_SUBMITTED'
           AND v_execution_committed_at_utc IS NOT NULL
         ) THEN
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'NO_BANK_PAYMENT_FINALISATION_COMMIT_CONFLICT',
          'message', 'The batch contains execution commit evidence incompatible with no-bank-payment finalisation.',
          'pay_batch_id', p_pay_batch_id::text,
          'execution_commit_state', v_execution_commit_state,
          'execution_commit_ref', v_execution_commit_ref,
          'expected_local_commit_reference', v_local_no_bank_commit_ref,
          'execution_committed_at_utc', CASE WHEN v_execution_committed_at_utc IS NULL THEN NULL::text ELSE v_execution_committed_at_utc::text END
        )::text USING ERRCODE = 'P0001';
      END IF;
    END IF;
  END IF;

  -- Settlement follows bank/provider truth after the execution boundary.
  -- Do not run current/live freshness as a blocking settlement check here.
  -- Stored freshness-at-execution metadata is carried as informational context only.
  v_fresh := jsonb_build_object(
    'is_stale', coalesce(v_stored_freshness_status in ('STALE', 'FAILED'), false),
    'stale_reasons', coalesce(v_stored_freshness_result_json->'stale_reasons', '[]'::jsonb),
    'diff', coalesce(v_stored_freshness_result_json->'diff_sample', '[]'::jsonb),
    'freshness_validation_status', nullif(v_stored_freshness_status, ''),
    'freshness_result_hash', v_stored_freshness_result_hash,
    'freshness_scope_hash', v_stored_freshness_scope_hash,
    'freshness_operation_id', case when v_stored_freshness_operation_id is null then null else v_stored_freshness_operation_id::text end,
    'source', 'stored_freshness_metadata_non_blocking'
  );
  v_is_stale := coalesce(v_stored_freshness_status in ('STALE', 'FAILED'), false);
  v_stale_reasons := coalesce(v_stored_freshness_result_json->'stale_reasons', '[]'::jsonb);
  v_diff_sample := coalesce(v_stored_freshness_result_json->'diff_sample', '[]'::jsonb);

  if v_is_stale = true then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_SETTLE_RAIL:STORED_STALE_METADATA_PROCEEDING',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'freshness_validation_status', v_stored_freshness_status,
          'freshness_result_hash', v_stored_freshness_result_hash,
          'freshness_scope_hash', v_stored_freshness_scope_hash,
          'stale_reasons', v_stale_reasons,
          'diff_sample', v_diff_sample
        ),
        'pay_batches',
        p_pay_batch_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;
  end if;

  create temp table if not exists _tmp_settle_in (
    transfer_id uuid null,
    status text not null,
    rail_tx_id text null,
    rail_state text null,
    rail_meta_json jsonb null
  ) on commit drop;

  truncate table _tmp_settle_in;

  insert into _tmp_settle_in(transfer_id, status, rail_tx_id, rail_state, rail_meta_json)
  select
    case
      when nullif(btrim(coalesce(e->>'transfer_id','')),'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then nullif(btrim(coalesce(e->>'transfer_id','')),'')::uuid
      else null::uuid
    end as transfer_id,
    upper(btrim(coalesce(e->>'status',''))) as status,
    nullif(btrim(coalesce(e->>'rail_tx_id','')),'') as rail_tx_id,
    nullif(btrim(coalesce(e->>'rail_state','')),'') as rail_state,
    case
      when (e ? 'rail_meta_json') and jsonb_typeof(e->'rail_meta_json') in ('object','array','string','number','boolean','null')
        then e->'rail_meta_json'
      else null
    end as rail_meta_json
  from jsonb_array_elements(p_settlement_json) e
  where e is not null and jsonb_typeof(e) = 'object';

  update _tmp_settle_in AS settle_status_normalise
  set status = case
    when settle_status_normalise.status = 'CANCELED' then 'CANCELLED'
    when settle_status_normalise.status in ('SUCCESS','SUCCEEDED','SETTLED','PAID') then 'COMPLETED'
    when settle_status_normalise.status in ('EXECUTED','COMMITTED') then 'UNKNOWN'
    when settle_status_normalise.status = 'REVERSED' then 'REVERTED'
    when settle_status_normalise.status in ('SUBMISSION_FAILED','FAILED_BEFORE_COMMIT') then 'FAILED'
    else settle_status_normalise.status
  end
  where settle_status_normalise.status in (
    'CANCELED',
    'SUCCESS',
    'SUCCEEDED',
    'SETTLED',
    'PAID',
    'EXECUTED',
    'COMMITTED',
    'REVERSED',
    'SUBMISSION_FAILED',
    'FAILED_BEFORE_COMMIT'
  );

  DROP TABLE IF EXISTS pg_temp._tmp_settle_in_classification;
  CREATE TEMP TABLE _tmp_settle_in_classification ON COMMIT DROP AS
  SELECT
    settle_input_rows.transfer_id,
    classification_rows.cash_state,
    classification_rows.normalised_transfer_status,
    classification_rows.is_final_money_moved,
    classification_rows.is_terminal_no_money,
    classification_rows.is_pending_non_final,
    classification_rows.completed_at_allowed,
    classification_rows.reason,
    classification_rows.support_details_json
  FROM _tmp_settle_in AS settle_input_rows
  CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
    settle_input_rows.status,
    settle_input_rows.rail_state,
    COALESCE(settle_input_rows.rail_meta_json, '{}'::jsonb),
    COALESCE(settle_input_rows.rail_meta_json, '{}'::jsonb)
  ) AS classification_rows;

  UPDATE _tmp_settle_in AS settle_input_rows
  SET
    status = CASE
      WHEN classification_rows.is_final_money_moved THEN 'COMPLETED'
      WHEN classification_rows.is_terminal_no_money AND upper(COALESCE(settle_input_rows.status, '')) IN ('CANCELLED','CANCELED') THEN 'CANCELLED'
      WHEN classification_rows.is_terminal_no_money THEN 'FAILED'
      WHEN classification_rows.is_pending_non_final THEN CASE WHEN upper(COALESCE(settle_input_rows.status, '')) IN ('PROCESSING','SUBMITTED','SENT','ACCEPTED') THEN 'PROCESSING' ELSE 'PENDING' END
      ELSE 'UNKNOWN'
    END,
    rail_meta_json = COALESCE(settle_input_rows.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
      'money_movement_classification', jsonb_build_object(
        'cash_state', classification_rows.cash_state,
        'normalised_transfer_status', classification_rows.normalised_transfer_status,
        'is_final_money_moved', classification_rows.is_final_money_moved,
        'is_terminal_no_money', classification_rows.is_terminal_no_money,
        'is_pending_non_final', classification_rows.is_pending_non_final,
        'completed_at_allowed', classification_rows.completed_at_allowed,
        'reason', classification_rows.reason
      ),
      'settlement_final_paid_required', true
    )
  FROM pg_temp._tmp_settle_in_classification AS classification_rows
  WHERE classification_rows.transfer_id = settle_input_rows.transfer_id;

  if exists (select 1 from _tmp_settle_in t where t.transfer_id is null limit 1) then
    raise exception 'pay_settle_rail: settlement_json contains an invalid or missing transfer_id';
  end if;

  if exists (
    select 1
    from _tmp_settle_in t
    where t.status not in ('PENDING','PROCESSING','UNKNOWN','COMPLETED','FAILED','DECLINED','REJECTED','CANCELLED','RETURNED','REVERTED')
    limit 1
  ) then
    raise exception 'pay_settle_rail: invalid status in settlement_json (allowed: PENDING|PROCESSING|UNKNOWN|COMPLETED|FAILED|DECLINED|REJECTED|CANCELLED|RETURNED|REVERTED)';
  end if;

  if exists (
    select 1
    from _tmp_settle_in t
    left join public.pay_bank_transfers pbt
      on pbt.id = t.transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    where pbt.id is null
    limit 1
  ) then
    raise exception 'pay_settle_rail: one or more transfer_id values do not belong to the specified pay batch';
  end if;

  if v_settlement_mode = 'STANDARD_BANK' and exists (
    select 1
    from _tmp_settle_in as t
    join public.pay_bank_transfers as pbt
      on pbt.id = t.transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    where t.status = 'COMPLETED'
      and not exists (
        select 1
        from (values
          (t.rail_tx_id),
          (t.rail_meta_json #>> '{provider_reference}'),
          (t.rail_meta_json #>> '{provider_event_id}'),
          (t.rail_meta_json #>> '{provider_submission_id}'),
          (t.rail_meta_json #>> '{submission_id}'),
          (t.rail_meta_json #>> '{rail_submission_id}'),
          (t.rail_meta_json #>> '{provider_payment_id}'),
          (t.rail_meta_json #>> '{payment_id}'),
          (t.rail_meta_json #>> '{external_payment_id}'),
          (t.rail_meta_json #>> '{revolut_payment_id}'),
          (t.rail_meta_json #>> '{provider_transfer_id}'),
          (t.rail_meta_json #>> '{transfer_id}'),
          (t.rail_meta_json #>> '{external_transfer_id}'),
          (t.rail_meta_json #>> '{provider_transaction_id}'),
          (t.rail_meta_json #>> '{transaction_id}')
        ) as provider_identifier(identifier_value)
        where nullif(btrim(coalesce(provider_identifier.identifier_value, '')), '') is not null
          and not (
            nullif(btrim(coalesce(provider_identifier.identifier_value, '')), '') = any(
              array_remove(array[
                pbt.id::text,
                nullif(btrim(coalesce(pbt.request_id, '')), ''),
                nullif(btrim(coalesce(pbt.payment_reference, '')), ''),
                nullif(btrim(coalesce(v_batch.bulk_reference, '')), ''),
                nullif(btrim(coalesce(pbt.rail_meta_json #>> '{request_id}', '')), ''),
                nullif(btrim(coalesce(pbt.rail_meta_json #>> '{idempotency_key}', '')), ''),
                nullif(btrim(coalesce(pbt.rail_meta_json #>> '{payment_reference}', '')), ''),
                nullif(btrim(coalesce(pbt.rail_meta_json #>> '{bulk_reference}', '')), ''),
                nullif(btrim(coalesce(t.rail_meta_json #>> '{request_id}', '')), ''),
                nullif(btrim(coalesce(t.rail_meta_json #>> '{idempotency_key}', '')), ''),
                nullif(btrim(coalesce(t.rail_meta_json #>> '{payment_reference}', '')), ''),
                nullif(btrim(coalesce(t.rail_meta_json #>> '{bulk_reference}', '')), '')
              ]::text[], null::text)
            )
          )
      )
    limit 1
  ) then
    raise exception 'PAY_SETTLE_RAIL_PROVIDER_EVIDENCE_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_SETTLE_RAIL_PROVIDER_EVIDENCE_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'STANDARD_BANK settlement marked as completed requires genuine provider or bank evidence, not local request/payment references.'
            )::text;
  end if;


  update public.pay_bank_transfers pbt
  set
    status = CASE
      WHEN t.status = 'COMPLETED' THEN 'COMPLETED'
      WHEN t.status IN ('FAILED','DECLINED','REJECTED','CANCELLED','RETURNED','REVERTED') THEN t.status
      WHEN t.status = 'BLOCKED' THEN 'BLOCKED'
      WHEN t.status = 'PROCESSING' THEN 'PROCESSING'
      WHEN t.status = 'UNKNOWN' THEN 'UNKNOWN'
      WHEN t.status = 'PENDING' THEN 'PENDING'
      ELSE 'PENDING'
    END,
    rail_tx_id = coalesce(t.rail_tx_id, pbt.rail_tx_id),
    rail_state = coalesce(t.rail_state, pbt.rail_state),
    rail_meta_json = case
      when t.rail_meta_json is null then pbt.rail_meta_json
      when pbt.rail_meta_json is null then t.rail_meta_json
      else (pbt.rail_meta_json || t.rail_meta_json)
    end,
    completed_at_utc = case
      when t.status = 'COMPLETED' then coalesce(pbt.completed_at_utc, v_now)
      else pbt.completed_at_utc
    end,
    failed_reason = case
      when t.status IN ('FAILED','DECLINED','REJECTED','CANCELLED') then coalesce(pbt.failed_reason, nullif(btrim(coalesce(t.rail_state,'')),''), t.status)
      else pbt.failed_reason
    end
  from _tmp_settle_in t
  where pbt.id = t.transfer_id
    and pbt.pay_batch_id = p_pay_batch_id;

  DROP TABLE IF EXISTS pg_temp._tmp_settle_bank_event_ingest_results;
  CREATE TEMP TABLE _tmp_settle_bank_event_ingest_results ON COMMIT DROP AS
  SELECT
    settle_event_rows.transfer_id,
    settle_event_rows.status,
    public.pay_bank_event_ingest(
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'pay_bank_transfer_id', settle_event_rows.transfer_id::text,
        'provider_key', COALESCE(v_batch.rail_provider_snapshot, 'UNKNOWN'),
        'provider_reference', CASE
          WHEN v_settlement_mode IN ('CSV_SETTLEMENT','EXTERNAL_SETTLEMENT') THEN COALESCE(settle_event_rows.rail_tx_id, bank_transfer_for_event.rail_tx_id, bank_transfer_for_event.request_id, bank_transfer_for_event.payment_reference)
          ELSE COALESCE(
            CASE
              WHEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_tx_id, '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(settle_event_rows.rail_tx_id, '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_tx_id, '')), '')
              ELSE NULL::text
            END,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_reference}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_reference}', '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_reference}', '')), '')
              ELSE NULL::text
            END,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_event_id}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_event_id}', '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_event_id}', '')), '')
              ELSE NULL::text
            END,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_submission_id}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_submission_id}', '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_submission_id}', '')), '')
              ELSE NULL::text
            END,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_payment_id}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_payment_id}', '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_payment_id}', '')), '')
              ELSE NULL::text
            END,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_transfer_id}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_transfer_id}', '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_transfer_id}', '')), '')
              ELSE NULL::text
            END,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_transaction_id}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_transaction_id}', '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(settle_event_rows.rail_meta_json #>> '{provider_transaction_id}', '')), '')
              ELSE NULL::text
            END,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(bank_transfer_for_event.rail_tx_id, '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(bank_transfer_for_event.rail_tx_id, '')), '') NOT IN (
                 COALESCE(bank_transfer_for_event.id::text, '__no_transfer_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.request_id, '')), ''), '__no_request_id__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(bank_transfer_for_event.payment_reference, '')), ''), '__no_payment_reference__'),
                 COALESCE(NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''), '__no_bulk_reference__')
               )
                THEN NULLIF(BTRIM(COALESCE(bank_transfer_for_event.rail_tx_id, '')), '')
              ELSE NULL::text
            END
          )
        END,
        'provider_state', COALESCE(settle_event_rows.rail_state, settle_event_rows.status),
        'normalised_state', settle_event_rows.status,
        'event_source', CASE WHEN v_settlement_mode IN ('CSV_SETTLEMENT','EXTERNAL_SETTLEMENT') THEN 'MANUAL_CONFIRM' ELSE 'PROVIDER_POLL' END,
        'event_time_utc', v_now::text,
        'amount', bank_transfer_for_event.amount,
        'currency', 'GBP',
        'raw_payload', COALESCE(settle_event_rows.rail_meta_json, '{}'::jsonb) || jsonb_build_object(
          'settlement_mode', v_settlement_mode,
          'source_rpc', 'pay_settle_rail'
        )
      ),
      p_actor_user_id
    ) AS ingest_result
  FROM _tmp_settle_in AS settle_event_rows
  JOIN public.pay_bank_transfers AS bank_transfer_for_event
    ON bank_transfer_for_event.id = settle_event_rows.transfer_id;

  SELECT
    COALESCE(jsonb_agg(ingest_rows.ingest_result ORDER BY ingest_rows.transfer_id), '[]'::jsonb),
    count(*)::int
  INTO v_bank_event_ingest_results, v_bank_event_ingest_count
  FROM pg_temp._tmp_settle_bank_event_ingest_results AS ingest_rows;

  DROP TABLE IF EXISTS pg_temp._tmp_settle_external_completed_transfers;
  CREATE TEMP TABLE _tmp_settle_external_completed_transfers ON COMMIT DROP AS
  WITH completed_transfer_base AS (
    SELECT
      completed_transfer.id AS transfer_id,
      completed_transfer.rail_tx_id,
      completed_transfer.completed_at_utc,
      completed_transfer.status,
      completed_transfer.rail_state,
      COALESCE(completed_transfer.rail_meta_json, '{}'::jsonb) AS rail_meta_json,
      lower(btrim(coalesce(completed_transfer.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS last_update_was_provider_evidence,
      ARRAY_REMOVE(ARRAY[
        completed_transfer.id::text,
        NULLIF(BTRIM(COALESCE(completed_transfer.request_id, '')), ''),
        NULLIF(BTRIM(COALESCE(completed_transfer.payment_reference, '')), ''),
        NULLIF(BTRIM(COALESCE(v_batch.bulk_reference, '')), ''),
        NULLIF(BTRIM(COALESCE(completed_transfer.rail_meta_json #>> '{request_id}', '')), ''),
        NULLIF(BTRIM(COALESCE(completed_transfer.rail_meta_json #>> '{idempotency_key}', '')), ''),
        NULLIF(BTRIM(COALESCE(completed_transfer.rail_meta_json #>> '{payment_reference}', '')), ''),
        NULLIF(BTRIM(COALESCE(completed_transfer.rail_meta_json #>> '{bulk_reference}', '')), '')
      ]::text[], NULL::text) AS local_identity_values
    FROM public.pay_bank_transfers AS completed_transfer
    WHERE completed_transfer.pay_batch_id = p_pay_batch_id
      AND EXISTS (
        SELECT 1
        FROM public._pay_rail_state_money_movement_classify(
          completed_transfer.status,
          completed_transfer.rail_state,
          COALESCE(completed_transfer.rail_meta_json, '{}'::jsonb),
          COALESCE(completed_transfer.rail_meta_json, '{}'::jsonb)
        ) AS completed_transfer_classifier
        WHERE completed_transfer_classifier.is_final_money_moved = true
      )
  ), externally_supported_completed AS (
    SELECT
      completed_transfer_base.transfer_id,
      completed_transfer_base.rail_tx_id,
      completed_transfer_base.completed_at_utc,
      (
        v_settlement_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
        OR EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS completed_event
          WHERE completed_event.pay_bank_transfer_id = completed_transfer_base.transfer_id
            AND upper(btrim(coalesce(completed_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER', 'PROVIDER_SETTLEMENT')
            AND (
              upper(btrim(coalesce(completed_event.normalised_state, ''))) IN ('COMPLETED', 'SETTLED', 'PAID', 'SUCCESS', 'SUCCEEDED')
              OR upper(btrim(coalesce(completed_event.provider_state, ''))) IN ('COMPLETED', 'SETTLED', 'PAID', 'SUCCESS', 'SUCCEEDED')
            )
            AND EXISTS (
              SELECT 1
              FROM (VALUES
                (completed_event.provider_event_id),
                (completed_event.provider_reference),
                (completed_event.raw_payload #>> '{provider_event_id}'),
                (completed_event.raw_payload #>> '{provider_reference}'),
                (completed_event.raw_payload #>> '{provider_submission_id}'),
                (completed_event.raw_payload #>> '{submission_id}'),
                (completed_event.raw_payload #>> '{rail_submission_id}'),
                (completed_event.raw_payload #>> '{provider_payment_id}'),
                (completed_event.raw_payload #>> '{payment_id}'),
                (completed_event.raw_payload #>> '{external_payment_id}'),
                (completed_event.raw_payload #>> '{revolut_payment_id}'),
                (completed_event.raw_payload #>> '{provider_transfer_id}'),
                (completed_event.raw_payload #>> '{transfer_id}'),
                (completed_event.raw_payload #>> '{external_transfer_id}'),
                (completed_event.raw_payload #>> '{provider_transaction_id}'),
                (completed_event.raw_payload #>> '{transaction_id}')
              ) AS completed_event_identifier(identifier_value)
              WHERE NULLIF(BTRIM(COALESCE(completed_event_identifier.identifier_value, '')), '') IS NOT NULL
                AND NOT (NULLIF(BTRIM(COALESCE(completed_event_identifier.identifier_value, '')), '') = ANY(completed_transfer_base.local_identity_values))
            )
        )
        OR (
          completed_transfer_base.last_update_was_provider_evidence IS TRUE
          AND EXISTS (
            SELECT 1
            FROM (VALUES
              (completed_transfer_base.rail_tx_id),
              (completed_transfer_base.rail_meta_json #>> '{provider_event_id}'),
              (completed_transfer_base.rail_meta_json #>> '{provider_reference}'),
              (completed_transfer_base.rail_meta_json #>> '{provider_submission_id}'),
              (completed_transfer_base.rail_meta_json #>> '{submission_id}'),
              (completed_transfer_base.rail_meta_json #>> '{rail_submission_id}'),
              (completed_transfer_base.rail_meta_json #>> '{provider_payment_id}'),
              (completed_transfer_base.rail_meta_json #>> '{payment_id}'),
              (completed_transfer_base.rail_meta_json #>> '{external_payment_id}'),
              (completed_transfer_base.rail_meta_json #>> '{revolut_payment_id}'),
              (completed_transfer_base.rail_meta_json #>> '{provider_transfer_id}'),
              (completed_transfer_base.rail_meta_json #>> '{transfer_id}'),
              (completed_transfer_base.rail_meta_json #>> '{external_transfer_id}'),
              (completed_transfer_base.rail_meta_json #>> '{provider_transaction_id}'),
              (completed_transfer_base.rail_meta_json #>> '{transaction_id}')
            ) AS completed_transfer_identifier(identifier_value)
            WHERE NULLIF(BTRIM(COALESCE(completed_transfer_identifier.identifier_value, '')), '') IS NOT NULL
              AND NOT (NULLIF(BTRIM(COALESCE(completed_transfer_identifier.identifier_value, '')), '') = ANY(completed_transfer_base.local_identity_values))
          )
        )
      ) AS has_external_completion_evidence
    FROM completed_transfer_base
  )
  SELECT
    externally_supported_completed.transfer_id,
    externally_supported_completed.rail_tx_id,
    externally_supported_completed.completed_at_utc
  FROM externally_supported_completed
  WHERE externally_supported_completed.has_external_completion_evidence IS TRUE;

  SELECT
    COUNT(*)::int,
    ROUND(COALESCE(SUM(bank_transfer_total.amount), 0), 2)::numeric(14,2),
    COALESCE(
      JSONB_AGG(completed_transfer_count_row.transfer_id::text ORDER BY completed_transfer_count_row.transfer_id),
      '[]'::jsonb
    ),
    (
      SELECT NULLIF(BTRIM(COALESCE(completed_transfer_ref.rail_tx_id, bank_transfer_ref.rail_tx_id, '')), '')
      FROM pg_temp._tmp_settle_external_completed_transfers AS completed_transfer_ref
      JOIN public.pay_bank_transfers AS bank_transfer_ref
        ON bank_transfer_ref.id = completed_transfer_ref.transfer_id
       AND bank_transfer_ref.pay_batch_id = p_pay_batch_id
      WHERE NULLIF(BTRIM(COALESCE(completed_transfer_ref.rail_tx_id, bank_transfer_ref.rail_tx_id, '')), '') IS NOT NULL
      ORDER BY COALESCE(completed_transfer_ref.completed_at_utc, bank_transfer_ref.completed_at_utc) DESC NULLS LAST, bank_transfer_ref.id DESC
      LIMIT 1
    ),
    (
      SELECT COALESCE(completed_transfer_ts.completed_at_utc, bank_transfer_ts.completed_at_utc)
      FROM pg_temp._tmp_settle_external_completed_transfers AS completed_transfer_ts
      JOIN public.pay_bank_transfers AS bank_transfer_ts
        ON bank_transfer_ts.id = completed_transfer_ts.transfer_id
       AND bank_transfer_ts.pay_batch_id = p_pay_batch_id
      ORDER BY COALESCE(completed_transfer_ts.completed_at_utc, bank_transfer_ts.completed_at_utc) DESC NULLS LAST, bank_transfer_ts.id DESC
      LIMIT 1
    )
  INTO
    v_completed_transfer_count,
    v_completed_transfer_total,
    v_completed_transfer_ids,
    v_detected_execution_commit_ref,
    v_detected_execution_committed_at_utc
  FROM pg_temp._tmp_settle_external_completed_transfers AS completed_transfer_count_row
  JOIN public.pay_bank_transfers AS bank_transfer_total
    ON bank_transfer_total.id = completed_transfer_count_row.transfer_id
   AND bank_transfer_total.pay_batch_id = p_pay_batch_id;

  IF COALESCE(v_completed_transfer_count, 0) > 0 THEN
    v_execution_commit_state := 'COMMITTED';
    v_execution_commit_ref := COALESCE(v_execution_commit_ref, v_detected_execution_commit_ref);
    v_execution_committed_at_utc := COALESCE(v_execution_committed_at_utc, v_detected_execution_committed_at_utc, v_now);
  END IF;

  IF v_no_bank_settlement_operation_id IS NOT NULL THEN
    v_settlement_operation_id := v_no_bank_settlement_operation_id;
  ELSIF COALESCE(v_completed_transfer_count, 0) > 0 THEN
    DROP TABLE IF EXISTS pg_temp.tmp_pay_settle_rail_compatible_settlement_operations;
    CREATE TEMPORARY TABLE pg_temp.tmp_pay_settle_rail_compatible_settlement_operations
    ON COMMIT DROP
    AS
    SELECT settlement_operation.id AS operation_id
    FROM public.banking_pay_operations AS settlement_operation
    WHERE settlement_operation.pay_batch_id = p_pay_batch_id
      AND settlement_operation.operation_type = 'PAYMENT_SETTLEMENT'
      AND settlement_operation.root_operation_id = v_execution_operation_id
      AND JSONB_TYPEOF(settlement_operation.input_json) = 'object'
      AND NULLIF(BTRIM(COALESCE(
        settlement_operation.input_json->>'execution_operation_id',
        settlement_operation.input_json->>'root_operation_id',
        ''
      )), '') = v_execution_operation_id::text
      AND NULLIF(BTRIM(COALESCE(settlement_operation.input_json->>'auth_request_id', '')), '') = v_operation_auth_request_id::text
      AND CASE
        WHEN UPPER(BTRIM(COALESCE(
          settlement_operation.input_json->>'settlement_mode',
          settlement_operation.input_json->>'execution_mode',
          ''
        ))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
        WHEN UPPER(BTRIM(COALESCE(
          settlement_operation.input_json->>'settlement_mode',
          settlement_operation.input_json->>'execution_mode',
          ''
        ))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
        WHEN UPPER(BTRIM(COALESCE(
          settlement_operation.input_json->>'settlement_mode',
          settlement_operation.input_json->>'execution_mode',
          ''
        ))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
        ELSE UPPER(BTRIM(COALESCE(
          settlement_operation.input_json->>'settlement_mode',
          settlement_operation.input_json->>'execution_mode',
          ''
        )))
      END = v_settlement_mode
      AND UPPER(BTRIM(COALESCE(
        settlement_operation.input_json->>'pay_channel_scope',
        settlement_operation.input_json->>'scope',
        ''
      ))) = v_operation_projection_scope
      AND LOWER(BTRIM(COALESCE(settlement_operation.input_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND NULLIF(BTRIM(COALESCE(settlement_operation.input_json->>'paye_net_state_hash', '')), '') = v_expected_paye_net_state_hash
      AND NULLIF(BTRIM(COALESCE(settlement_operation.input_json->>'bank_payment_projection_hash', '')), '') = v_expected_bank_payment_projection_hash
      AND CASE
        WHEN COALESCE(settlement_operation.input_json->>'missing_explicit_paye_input_count', '') ~ '^[0-9]+$'
          THEN (settlement_operation.input_json->>'missing_explicit_paye_input_count')::integer
        ELSE -1
      END = COALESCE(v_expected_missing_count, -1)
      AND CASE
        WHEN COALESCE(settlement_operation.input_json->>'scoped_explicit_zero_count', '') ~ '^[0-9]+$'
          THEN (settlement_operation.input_json->>'scoped_explicit_zero_count')::integer
        ELSE -1
      END = COALESCE(v_expected_zero_count, -1)
      AND CASE
        WHEN COALESCE(settlement_operation.input_json->>'scoped_positive_bank_payment_count', '') ~ '^[0-9]+$'
          THEN (settlement_operation.input_json->>'scoped_positive_bank_payment_count')::integer
        ELSE -1
      END = COALESCE(v_expected_positive_count, -1)
      AND LOWER(BTRIM(COALESCE(settlement_operation.input_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') = v_no_bank_payment_marker
      AND LOWER(BTRIM(COALESCE(settlement_operation.input_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') = v_allow_explicit_zero_no_bank_scopes_marker
      AND LOWER(BTRIM(COALESCE(settlement_operation.input_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') = v_suppress_remittances
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_settlement_scope AS settlement_scope
        WHERE settlement_scope.operation_id = settlement_operation.id
          AND settlement_scope.pay_batch_id = p_pay_batch_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_settlement_scope AS settlement_scope
        WHERE settlement_scope.operation_id = settlement_operation.id
          AND settlement_scope.pay_batch_id = p_pay_batch_id
          AND UPPER(BTRIM(COALESCE(settlement_scope.status, ''))) NOT IN ('SETTLED', 'SKIPPED')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_settlement_scope AS settlement_scope
        WHERE settlement_scope.operation_id = settlement_operation.id
          AND settlement_scope.pay_batch_id = p_pay_batch_id
          AND UPPER(BTRIM(COALESCE(settlement_scope.status, ''))) = 'FAILED'
      );

    SELECT
      COUNT(*)::integer,
      (ARRAY_AGG(compatible_operation.operation_id ORDER BY compatible_operation.operation_id))[1]
    INTO
      v_settlement_operation_count,
      v_settlement_operation_id
    FROM pg_temp.tmp_pay_settle_rail_compatible_settlement_operations AS compatible_operation;

    SELECT COUNT(*)::integer
    INTO v_related_settlement_operation_count
    FROM public.banking_pay_operations AS related_operation
    WHERE related_operation.pay_batch_id = p_pay_batch_id
      AND related_operation.operation_type = 'PAYMENT_SETTLEMENT'
      AND (
        related_operation.root_operation_id = v_execution_operation_id
        OR NULLIF(BTRIM(COALESCE(related_operation.input_json->>'execution_operation_id', '')), '') = v_execution_operation_id::text
      );

    SELECT COUNT(*)::integer
    INTO v_bound_settlement_scope_count
    FROM public.banking_pay_operation_settlement_scope AS bound_scope
    JOIN public.banking_pay_operations AS bound_operation
      ON bound_operation.id = bound_scope.operation_id
     AND bound_operation.pay_batch_id = p_pay_batch_id
     AND bound_operation.operation_type = 'PAYMENT_SETTLEMENT'
    WHERE bound_scope.pay_batch_id = p_pay_batch_id
      AND (
        bound_operation.root_operation_id = v_execution_operation_id
        OR NULLIF(BTRIM(COALESCE(bound_operation.input_json->>'execution_operation_id', '')), '') = v_execution_operation_id::text
      );

    IF COALESCE(v_settlement_operation_count, 0) = 0 THEN
      IF COALESCE(v_related_settlement_operation_count, 0) = 0
         AND COALESCE(v_bound_settlement_scope_count, 0) = 0 THEN
        -- Compatibility for an older direct settlement route that genuinely has no
        -- child operation or durable settlement-scope rows. The execution operation
        -- remains the deterministic proof binding; no provider evidence is invented.
        v_settlement_operation_id := v_execution_operation_id;
        v_settlement_operation_count := 1;
        v_legacy_direct_settlement := true;
      ELSE
        RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
          'error', 'PAY_SETTLE_RAIL',
          'code', 'SETTLEMENT_OPERATION_PROOF_INCOMPLETE',
          'message', 'Full settlement found a settlement child or settlement scopes, but none formed one complete successful operation bound to the authorised frozen proof.',
          'pay_batch_id', p_pay_batch_id::text,
          'execution_operation_id', v_execution_operation_id::text,
          'auth_request_id', v_operation_auth_request_id::text,
          'related_settlement_operation_count', COALESCE(v_related_settlement_operation_count, 0),
          'bound_settlement_scope_count', COALESCE(v_bound_settlement_scope_count, 0)
        )::text USING ERRCODE = 'P0001';
      END IF;
    ELSIF COALESCE(v_settlement_operation_count, 0) <> 1 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'SETTLEMENT_OPERATION_PROOF_AMBIGUOUS',
        'message', 'Full settlement requires exactly one successful settlement operation bound to the authorised frozen proof.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text,
        'compatible_settlement_operation_count', COALESCE(v_settlement_operation_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF v_settlement_operation_id IS NOT NULL
     AND v_settlement_operation_id <> v_execution_operation_id THEN
    SELECT COUNT(*)::integer
    INTO v_settlement_operation_count
    FROM public.banking_pay_operations AS bound_settlement_operation
    WHERE bound_settlement_operation.id = v_settlement_operation_id
      AND bound_settlement_operation.pay_batch_id = p_pay_batch_id
      AND bound_settlement_operation.operation_type = 'PAYMENT_SETTLEMENT'
      AND bound_settlement_operation.root_operation_id = v_execution_operation_id
      AND JSONB_TYPEOF(bound_settlement_operation.input_json) = 'object'
      AND NULLIF(BTRIM(COALESCE(bound_settlement_operation.input_json->>'execution_operation_id', '')), '') = v_execution_operation_id::text
      AND NULLIF(BTRIM(COALESCE(bound_settlement_operation.input_json->>'auth_request_id', '')), '') = v_operation_auth_request_id::text
      AND CASE
        WHEN UPPER(BTRIM(COALESCE(bound_settlement_operation.input_json->>'settlement_mode', bound_settlement_operation.input_json->>'execution_mode', ''))) IN ('STANDARD_BANK', 'BANK') THEN 'STANDARD_BANK'
        WHEN UPPER(BTRIM(COALESCE(bound_settlement_operation.input_json->>'settlement_mode', bound_settlement_operation.input_json->>'execution_mode', ''))) IN ('CSV', 'CSV_SETTLEMENT') THEN 'CSV_SETTLEMENT'
        WHEN UPPER(BTRIM(COALESCE(bound_settlement_operation.input_json->>'settlement_mode', bound_settlement_operation.input_json->>'execution_mode', ''))) IN ('EXTERNAL', 'EXTERNAL_SETTLEMENT', 'MANUAL_SETTLEMENT') THEN 'EXTERNAL_SETTLEMENT'
        ELSE UPPER(BTRIM(COALESCE(bound_settlement_operation.input_json->>'settlement_mode', bound_settlement_operation.input_json->>'execution_mode', '')))
      END = v_settlement_mode
      AND UPPER(BTRIM(COALESCE(bound_settlement_operation.input_json->>'pay_channel_scope', bound_settlement_operation.input_json->>'scope', ''))) = v_operation_projection_scope
      AND LOWER(BTRIM(COALESCE(bound_settlement_operation.input_json->>'server_owned_payment_projection_proof', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND NULLIF(BTRIM(COALESCE(bound_settlement_operation.input_json->>'paye_net_state_hash', '')), '') = v_expected_paye_net_state_hash
      AND NULLIF(BTRIM(COALESCE(bound_settlement_operation.input_json->>'bank_payment_projection_hash', '')), '') = v_expected_bank_payment_projection_hash
      AND LOWER(BTRIM(COALESCE(bound_settlement_operation.input_json->>'suppress_remittances', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') = v_suppress_remittances;

    IF COALESCE(v_settlement_operation_count, 0) <> 1 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'SETTLEMENT_OPERATION_BINDING_INVALID',
        'message', 'The selected settlement child is not bound to the authorised execution operation, auth request, mode, scope, hashes, and remittance policy.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'settlement_operation_id', v_settlement_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF v_settlement_operation_id IS NOT NULL
     AND v_settlement_operation_id <> v_execution_operation_id THEN
    WITH settled_scope_for_event_repair AS (
      SELECT
        scope_row.id AS settlement_scope_id,
        scope_row.pay_batch_candidate_id,
        scope_row.payload_json
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.operation_id = v_settlement_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
        AND scope_row.settlement_event_id IS NULL
        AND NOT (
          UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
          AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
        )
      FOR UPDATE OF scope_row
    ), direct_scope_transfer AS (
      SELECT
        settled_scope.settlement_scope_id,
        CASE
          WHEN COALESCE(
            NULLIF(BTRIM(COALESCE(settled_scope.payload_json->>'pay_bank_transfer_id', '')), ''),
            NULLIF(BTRIM(COALESCE(settled_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '')
          ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN COALESCE(
              NULLIF(BTRIM(COALESCE(settled_scope.payload_json->>'pay_bank_transfer_id', '')), ''),
              NULLIF(BTRIM(COALESCE(settled_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '')
            )::uuid
          ELSE NULL::uuid
        END AS pay_bank_transfer_id
      FROM settled_scope_for_event_repair AS settled_scope
    ), item_scope_transfer AS (
      SELECT DISTINCT
        settled_scope.settlement_scope_id,
        batch_item.pay_bank_transfer_id
      FROM settled_scope_for_event_repair AS settled_scope
      CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
        CASE
          WHEN JSONB_TYPEOF(settled_scope.payload_json->'pay_batch_item_ids') = 'array'
            THEN settled_scope.payload_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_element(value)
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = CASE
             WHEN (item_element.value #>> '{}')
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN (item_element.value #>> '{}')::uuid
             ELSE NULL::uuid
           END
       AND batch_item.pay_batch_candidate_id = settled_scope.pay_batch_candidate_id
       AND COALESCE(batch_item.is_voided, false) = false
      WHERE batch_item.pay_bank_transfer_id IS NOT NULL
    ), scope_transfer_candidate AS (
      SELECT
        direct_scope_transfer.settlement_scope_id,
        direct_scope_transfer.pay_bank_transfer_id
      FROM direct_scope_transfer
      WHERE direct_scope_transfer.pay_bank_transfer_id IS NOT NULL

      UNION

      SELECT
        item_scope_transfer.settlement_scope_id,
        item_scope_transfer.pay_bank_transfer_id
      FROM item_scope_transfer
      WHERE item_scope_transfer.pay_bank_transfer_id IS NOT NULL
    ), resolved_scope_transfer AS (
      SELECT
        scope_transfer_candidate.settlement_scope_id,
        (ARRAY_AGG(scope_transfer_candidate.pay_bank_transfer_id ORDER BY scope_transfer_candidate.pay_bank_transfer_id))[1] AS pay_bank_transfer_id
      FROM scope_transfer_candidate
      GROUP BY scope_transfer_candidate.settlement_scope_id
      HAVING COUNT(*) = 1
    ), ranked_matching_event AS (
      SELECT
        resolved_scope_transfer.settlement_scope_id,
        matching_event.id AS event_id,
        ROW_NUMBER() OVER (
          PARTITION BY resolved_scope_transfer.settlement_scope_id
          ORDER BY
            matching_event.event_time_utc DESC NULLS LAST,
            matching_event.received_at_utc DESC NULLS LAST,
            matching_event.created_at_utc DESC NULLS LAST,
            matching_event.id DESC
        ) AS event_rank
      FROM resolved_scope_transfer
      JOIN public.pay_bank_transfer_events AS matching_event
        ON matching_event.pay_batch_id = p_pay_batch_id
       AND matching_event.pay_bank_transfer_id = resolved_scope_transfer.pay_bank_transfer_id
      WHERE (
          UPPER(BTRIM(COALESCE(matching_event.normalised_state, ''))) IN ('COMPLETED', 'SETTLED', 'PAID', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
          OR UPPER(BTRIM(COALESCE(matching_event.provider_state, ''))) IN ('COMPLETED', 'SETTLED', 'PAID', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED')
        )
        AND (
          matching_event.idempotency_key LIKE
            'rail-settle:' || v_settlement_operation_id::text || ':' || resolved_scope_transfer.settlement_scope_id::text || ':' || resolved_scope_transfer.pay_bank_transfer_id::text || ':%'
          OR (
            NULLIF(BTRIM(COALESCE(matching_event.raw_payload->>'source_rpc', '')), '') = 'pay_settle_rail'
            AND NULLIF(BTRIM(COALESCE(matching_event.raw_payload->>'operation_id', '')), '') = v_settlement_operation_id::text
            AND NULLIF(BTRIM(COALESCE(matching_event.raw_payload->>'settlement_scope_id', '')), '') = resolved_scope_transfer.settlement_scope_id::text
          )
        )
    )
    UPDATE public.banking_pay_operation_settlement_scope AS scope_repair
    SET settlement_event_id = ranked_matching_event.event_id,
        updated_at_utc = v_now
    FROM ranked_matching_event
    WHERE scope_repair.id = ranked_matching_event.settlement_scope_id
      AND ranked_matching_event.event_rank = 1
      AND scope_repair.operation_id = v_settlement_operation_id
      AND scope_repair.pay_batch_id = p_pay_batch_id
      AND scope_repair.status = 'SETTLED'
      AND scope_repair.settlement_event_id IS NULL;
  END IF;

  IF v_settlement_operation_id IS NOT NULL THEN
    SELECT COALESCE(
      JSONB_AGG(settlement_scope_id_row.id::text ORDER BY settlement_scope_id_row.id),
      '[]'::jsonb
    )
    INTO v_settlement_scope_ids
    FROM public.banking_pay_operation_settlement_scope AS settlement_scope_id_row
    WHERE settlement_scope_id_row.operation_id = v_settlement_operation_id
      AND settlement_scope_id_row.pay_batch_id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(settlement_scope_id_row.status, ''))) IN ('SETTLED', 'SKIPPED');
  END IF;

  IF COALESCE(v_completed_transfer_count, 0) > 0
     AND v_settlement_operation_id IS NOT NULL
     AND v_settlement_operation_id <> v_execution_operation_id THEN
    SELECT
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
          AND NOT (
            UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
            AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
          )
      )::integer,
      COUNT(DISTINCT COALESCE(
        NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_bank_transfer_id', '')), ''),
        NULLIF(BTRIM(COALESCE(scope_row.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '')
      )) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
          AND NOT (
            UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
            AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
          )
      )::integer,
      COUNT(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
          AND NOT (
            UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
            AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
          )
          AND scope_row.settlement_event_id IS NULL
      )::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) NOT IN ('SETTLED', 'SKIPPED'))::integer,
      COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'FAILED')::integer,
      COUNT(*) FILTER (
        WHERE v_settlement_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
          AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
          AND NOT (
            UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
            AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
          )
          AND (
            UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) <> 'LOCAL_MANUAL_TRANSFER'
            OR LOWER(BTRIM(COALESCE(scope_row.payload_json->>'local_manual_settlement_scope', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
            OR NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'execution_operation_id', '')), '') <> v_execution_operation_id::text
            OR NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'settlement_operation_id', '')), '') <> v_settlement_operation_id::text
            OR NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'auth_request_id', '')), '') <> v_operation_auth_request_id::text
            OR NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'global_paye_net_state_hash', scope_row.payload_json->>'paye_net_state_hash', '')), '') <> v_expected_paye_net_state_hash
            OR NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'scoped_paye_net_state_hash', '')), '') <> v_expected_scoped_paye_net_state_hash
            OR NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'global_bank_payment_projection_hash', '')), '') <> v_expected_global_bank_payment_projection_hash
            OR NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'scoped_bank_payment_projection_hash', scope_row.payload_json->>'bank_payment_projection_hash', '')), '') <> v_expected_bank_payment_projection_hash
            OR LOWER(BTRIM(COALESCE(scope_row.payload_json->>'submitted_to_bank', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            OR LOWER(BTRIM(COALESCE(scope_row.payload_json->>'provider_submission_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            OR LOWER(BTRIM(COALESCE(scope_row.payload_json->>'provider_submission_attempted', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          )
      )::integer
    INTO
      v_settlement_positive_scope_count,
      v_settlement_positive_transfer_count,
      v_settlement_missing_event_count,
      v_settlement_nonterminal_scope_count,
      v_settlement_failed_scope_count,
      v_settlement_local_proof_invalid_count
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    WHERE scope_row.operation_id = v_settlement_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id;

    SELECT COUNT(*)::integer
    INTO v_settlement_unmatched_transfer_count
    FROM pg_temp._tmp_settle_external_completed_transfers AS completed_transfer
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_settlement_scope AS scope_row
      WHERE scope_row.operation_id = v_settlement_operation_id
        AND scope_row.pay_batch_id = p_pay_batch_id
        AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
        AND COALESCE(
          NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_bank_transfer_id', '')), ''),
          NULLIF(BTRIM(COALESCE(scope_row.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '')
        ) = completed_transfer.transfer_id::text
    );

    SELECT COUNT(*)::integer
    INTO v_settlement_unmatched_scope_transfer_count
    FROM public.banking_pay_operation_settlement_scope AS scope_row
    WHERE scope_row.operation_id = v_settlement_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SETTLED'
      AND NOT (
        UPPER(BTRIM(COALESCE(scope_row.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
        AND UPPER(BTRIM(COALESCE(scope_row.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM pg_temp._tmp_settle_external_completed_transfers AS completed_transfer
        WHERE completed_transfer.transfer_id::text = COALESCE(
          NULLIF(BTRIM(COALESCE(scope_row.payload_json->>'pay_bank_transfer_id', '')), ''),
          NULLIF(BTRIM(COALESCE(scope_row.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}', '')), '')
        )
      );

    IF COALESCE(v_settlement_positive_scope_count, 0) = 0
       OR COALESCE(v_settlement_positive_transfer_count, 0) <> COALESCE(v_completed_transfer_count, 0)
       OR COALESCE(v_settlement_missing_event_count, 0) <> 0
       OR COALESCE(v_settlement_nonterminal_scope_count, 0) <> 0
       OR COALESCE(v_settlement_failed_scope_count, 0) <> 0
       OR COALESCE(v_settlement_local_proof_invalid_count, 0) <> 0
       OR COALESCE(v_settlement_unmatched_transfer_count, 0) <> 0
       OR COALESCE(v_settlement_unmatched_scope_transfer_count, 0) <> 0 THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'SETTLEMENT_SCOPE_TRANSFER_PROOF_INCOMPLETE',
        'message', 'Completed transfers and terminal settlement scopes do not form one exact, mode-valid, operation-bound proof set.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'settlement_operation_id', v_settlement_operation_id::text,
        'completed_transfer_count', COALESCE(v_completed_transfer_count, 0),
        'settlement_positive_scope_count', COALESCE(v_settlement_positive_scope_count, 0),
        'settlement_positive_transfer_count', COALESCE(v_settlement_positive_transfer_count, 0),
        'missing_settlement_event_count', COALESCE(v_settlement_missing_event_count, 0),
        'nonterminal_scope_count', COALESCE(v_settlement_nonterminal_scope_count, 0),
        'failed_scope_count', COALESCE(v_settlement_failed_scope_count, 0),
        'local_proof_invalid_count', COALESCE(v_settlement_local_proof_invalid_count, 0),
        'unmatched_completed_transfer_count', COALESCE(v_settlement_unmatched_transfer_count, 0),
        'unmatched_scope_transfer_count', COALESCE(v_settlement_unmatched_scope_transfer_count, 0)
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  v_submitted_to_bank := v_settlement_mode = 'STANDARD_BANK' AND COALESCE(v_completed_transfer_count, 0) > 0;
  v_provider_submission_required := v_submitted_to_bank;
  v_provider_submission_attempted := v_submitted_to_bank;
  v_local_settlement_evidence_only := v_settlement_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
    OR v_no_bank_payment_execution_validated;

  IF v_settlement_mode = 'CSV_SETTLEMENT' THEN
    v_bank_confirm_ref := COALESCE(
      NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'csv_bank_confirm_ref', '')), ''),
      NULLIF(BTRIM(COALESCE(v_batch_intent_json->>'csv_bank_confirm_ref', '')), ''),
      NULLIF(BTRIM(COALESCE(v_batch.settlement_confirmation_json->>'bank_confirm_ref', '')), '')
    );
  END IF;

  IF COALESCE(v_completed_transfer_count, 0) > 0
     AND v_settlement_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT') THEN
    IF v_settlement_operation_id IS NULL THEN
      RAISE EXCEPTION 'PAY_SETTLE_RAIL_LOCAL_SETTLEMENT_OPERATION_REQUIRED' USING ERRCODE = 'P0001';
    END IF;

    IF EXISTS (
      SELECT 1
      FROM pg_temp._tmp_settle_external_completed_transfers AS local_completed_transfer
      JOIN public.pay_bank_transfers AS local_transfer
        ON local_transfer.id = local_completed_transfer.transfer_id
       AND local_transfer.pay_batch_id = p_pay_batch_id
      WHERE NULLIF(BTRIM(COALESCE(local_transfer.request_id, '')), '') IS NOT NULL
         OR NULLIF(BTRIM(COALESCE(local_transfer.rail_tx_id, '')), '') IS NOT NULL
         OR EXISTS (
           SELECT 1
           FROM public.pay_bank_transfer_events AS local_event_conflict
           WHERE local_event_conflict.pay_bank_transfer_id = local_transfer.id
             AND (
               NULLIF(BTRIM(COALESCE(local_event_conflict.provider_request_id, '')), '') IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(local_event_conflict.provider_transaction_id, '')), '') IS NOT NULL
               OR local_event_conflict.provider_webhook_receipt_id IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(local_event_conflict.provider_event_transport, '')), '') IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(local_event_conflict.adapter_key, '')), '') IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(local_event_conflict.adapter_version, '')), '') IS NOT NULL
               OR UPPER(BTRIM(COALESCE(local_event_conflict.event_source, ''))) IN (
                 'PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL',
                 'RAIL_PROVIDER', 'PROVIDER', 'PROVIDER_SETTLEMENT'
               )
             )
         )
      LIMIT 1
    ) THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'LOCAL_SETTLEMENT_PROVIDER_EVIDENCE_CONFLICT',
        'message', 'CSV/external settlement cannot be finalised from local proof while provider request, transaction, webhook, transport, or adapter evidence exists.',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_operation_id', v_execution_operation_id::text,
        'settlement_operation_id', v_settlement_operation_id::text,
        'settlement_mode', v_settlement_mode
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_local_positive_commit_ref := CASE
      WHEN v_legacy_direct_settlement
        THEN COALESCE(
          NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), ''),
          CASE
            WHEN v_settlement_mode = 'CSV_SETTLEMENT' THEN 'CSV_SETTLEMENT:'
            ELSE 'EXTERNAL_SETTLEMENT:'
          END || v_settlement_operation_id::text
        )
      ELSE CASE
        WHEN v_settlement_mode = 'CSV_SETTLEMENT' THEN 'CSV_SETTLEMENT:'
        ELSE 'EXTERNAL_SETTLEMENT:'
      END || v_settlement_operation_id::text
    END;

    IF v_legacy_direct_settlement IS NOT TRUE
       AND NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), '') IS NOT NULL
       AND NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), '') <> v_local_positive_commit_ref THEN
      RAISE EXCEPTION '%', JSONB_BUILD_OBJECT(
        'error', 'PAY_SETTLE_RAIL',
        'code', 'LOCAL_SETTLEMENT_COMMIT_REFERENCE_CONFLICT',
        'message', 'The existing execution commit reference conflicts with the deterministic local settlement proof.',
        'pay_batch_id', p_pay_batch_id::text,
        'settlement_operation_id', v_settlement_operation_id::text,
        'existing_execution_commit_ref', v_execution_commit_ref,
        'expected_execution_commit_ref', v_local_positive_commit_ref
      )::text USING ERRCODE = 'P0001';
    END IF;

    v_execution_commit_ref := v_local_positive_commit_ref;

    UPDATE public.pay_bank_transfers AS local_completed_transfer_update
    SET rail_meta_json = JSONB_STRIP_NULLS(
      COALESCE(local_completed_transfer_update.rail_meta_json, '{}'::jsonb)
      || JSONB_BUILD_OBJECT(
        'execution_mode', v_settlement_mode,
        'settlement_mode', v_settlement_mode,
        'execution_operation_id', v_execution_operation_id::text,
        'manual_settlement_operation_id', v_settlement_operation_id::text,
        'auth_request_id', v_operation_auth_request_id::text,
        'bank_confirm_ref', CASE WHEN v_settlement_mode = 'CSV_SETTLEMENT' THEN v_bank_confirm_ref ELSE NULL::text END,
        'external_settlement_comment', CASE
          WHEN v_settlement_mode = 'EXTERNAL_SETTLEMENT'
            THEN NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'external_settlement_comment', '')), '')
          ELSE NULL::text
        END,
        'submitted_to_bank', false,
        'provider_submission_required', false,
        'provider_submission_attempted', false,
        'local_settlement_evidence_only', true
      )
    )
    WHERE local_completed_transfer_update.pay_batch_id = p_pay_batch_id
      AND EXISTS (
        SELECT 1
        FROM pg_temp._tmp_settle_external_completed_transfers AS completed_transfer_marker
        WHERE completed_transfer_marker.transfer_id = local_completed_transfer_update.id
      );
  END IF;


  select count(distinct pbi_chk.pay_bank_transfer_id)::int
  into v_linked_transfer_ct
  from public.pay_batch_items pbi_chk
  join public.pay_batch_candidates pbc_chk
    on pbc_chk.id = pbi_chk.pay_batch_candidate_id
  where pbc_chk.pay_batch_id = p_pay_batch_id
    and pbi_chk.item_type <> 'DEBT_CREATED'
    and pbi_chk.is_voided = false
    and pbi_chk.pay_bank_transfer_id is not null;

  select exists (
    select 1
    from public.pay_batch_candidates pbc_pos
    where pbc_pos.pay_batch_id = p_pay_batch_id
      and coalesce(pbc_pos.net_bank_amount,0) > 0
    limit 1
  )
  into v_has_pos_net;

  if v_linked_transfer_ct = 0 and v_has_pos_net and v_no_bank_scope_authorised is not true then
    raise exception 'STATE_INCONSISTENT: positive net_bank_amount but no transfers';
  end if;

  create temp table if not exists _tmp_newly_settled_candidates (
    candidate_id uuid primary key
  ) on commit drop;

  truncate table _tmp_newly_settled_candidates;

  with cand_transfers as (
    select
      pbc.candidate_id as candidate_id,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.is_voided = false
          and pbi.pay_bank_transfer_id is not null
      ) as total_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.is_voided = false
          and pbi.pay_bank_transfer_id is not null
          and external_completed_candidate_transfer.transfer_id is not null
      ) as completed_transfers
    from public.pay_batch_candidates pbc
    left join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    left join public.pay_bank_transfers pbt
      on pbt.id = pbi.pay_bank_transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    left join pg_temp._tmp_settle_external_completed_transfers as external_completed_candidate_transfer
      on external_completed_candidate_transfer.transfer_id = pbi.pay_bank_transfer_id
    where pbc.pay_batch_id = p_pay_batch_id
    group by pbc.candidate_id
  ),
  eligible as (
    select
      ct.candidate_id
    from cand_transfers ct
    join public.pay_batch_candidates pbc2
      on pbc2.pay_batch_id = p_pay_batch_id
     and pbc2.candidate_id = ct.candidate_id
    where (
        pbc2.settled_at_utc is null
        or (
          upper(btrim(coalesce(v_batch.status, ''))) not in ('SETTLED', 'FAILED')
          and not exists (
            select 1
            from jsonb_array_elements(
              case
                when jsonb_typeof(v_batch.settlement_confirmation_json->'durably_finalised_candidate_ids') = 'array'
                  then v_batch.settlement_confirmation_json->'durably_finalised_candidate_ids'
                else '[]'::jsonb
              end
            ) as finalised_candidate_marker(value)
            where nullif(btrim(coalesce(finalised_candidate_marker.value #>> '{}', '')), '') = pbc2.candidate_id::text
          )
          and (
            (
              v_no_bank_scope_authorised
              and exists (
                select 1
                from pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope as no_bank_candidate_scope
                where no_bank_candidate_scope.pay_batch_candidate_id = pbc2.id
              )
            )
            or (
              coalesce(ct.total_transfers, 0) > 0
              and coalesce(ct.total_transfers, 0) = coalesce(ct.completed_transfers, 0)
              and exists (
                select 1
                from public.banking_pay_operation_settlement_scope as operation_candidate_scope
                join public.banking_pay_operations as operation_candidate_settlement
                  on operation_candidate_settlement.id = operation_candidate_scope.operation_id
                 and operation_candidate_settlement.pay_batch_id = p_pay_batch_id
                where operation_candidate_scope.pay_batch_id = p_pay_batch_id
                  and operation_candidate_scope.pay_batch_candidate_id = pbc2.id
                  and operation_candidate_scope.status = 'SETTLED'
                  and operation_candidate_scope.settlement_event_id is not null
                  and (
                    (
                      operation_candidate_settlement.operation_type = 'PAYMENT_EXECUTE'
                      and operation_candidate_settlement.id = v_execution_operation_id
                    )
                    or (
                      operation_candidate_settlement.operation_type = 'PAYMENT_SETTLEMENT'
                      and operation_candidate_settlement.root_operation_id = v_execution_operation_id
                    )
                  )
                  and (
                    operation_candidate_settlement.id = v_execution_operation_id
                    or nullif(btrim(coalesce(
                      operation_candidate_settlement.input_json->>'auth_request_id',
                      operation_candidate_settlement.input_json->>'authRequestId',
                      operation_candidate_settlement.input_json #>> '{execution_intent_json,auth_request_id}',
                      operation_candidate_settlement.input_json #>> '{execution_intent_json,authRequestId}',
                      ''
                    )), '') = v_operation_auth_request_id::text
                  )
                  and lower(btrim(coalesce(operation_candidate_settlement.progress_json->>'requires_full_batch_finalisation', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
                  and lower(btrim(coalesce(operation_candidate_settlement.progress_json->>'full_batch_finalisation_safe', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
                  and lower(btrim(coalesce(operation_candidate_settlement.progress_json->>'all_scopes_terminal', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
                  and lower(btrim(coalesce(operation_candidate_settlement.progress_json->>'all_scopes_successful', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
              )
            )
          )
        )
      )
      and (
        (
          ct.total_transfers = 0
          and (
            (
              v_no_bank_scope_authorised
              and exists (
                select 1
                from pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope as zero_candidate_scope
                where zero_candidate_scope.pay_batch_candidate_id = pbc2.id
              )
            )
          )
        )
        or (ct.total_transfers > 0 and ct.total_transfers = ct.completed_transfers)
      )
  )
  insert into _tmp_newly_settled_candidates(candidate_id)
  select e.candidate_id
  from eligible e;

  update public.pay_batch_candidates pbc
  set
    settlement_status = 'SETTLED',
    settled_at_utc = coalesce(pbc.settled_at_utc, v_now),
    settled_via = case
      when v_no_bank_scope_authorised
       and exists (
         select 1
         from pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope as no_bank_candidate_scope
         where no_bank_candidate_scope.pay_batch_candidate_id = pbc.id
       )
       and not exists (
         select 1
         from public.pay_batch_items as transfer_backed_candidate_item
         where transfer_backed_candidate_item.pay_batch_candidate_id = pbc.id
           and coalesce(transfer_backed_candidate_item.is_voided, false) = false
           and transfer_backed_candidate_item.pay_bank_transfer_id is not null
       )
       and not exists (
         select 1
         from public.banking_pay_operation_settlement_scope as transfer_backed_candidate_scope
         where transfer_backed_candidate_scope.operation_id = v_no_bank_settlement_operation_id
           and transfer_backed_candidate_scope.pay_batch_id = p_pay_batch_id
           and transfer_backed_candidate_scope.pay_batch_candidate_id = pbc.id
           and transfer_backed_candidate_scope.settlement_event_id is not null
       )
        then 'NO_BANK_PAYMENT'
      else coalesce(
        nullif(btrim(coalesce(pbc.settled_via, '')), ''),
        case
          when v_settlement_mode in ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT') then v_settlement_mode
          else upper(coalesce(v_batch.rail_provider_snapshot, 'RAIL'))
        end
      )
    end,
    settled_note = case
      when v_no_bank_scope_authorised
       and exists (
         select 1
         from pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope as no_bank_candidate_scope
         where no_bank_candidate_scope.pay_batch_candidate_id = pbc.id
       )
       and not exists (
         select 1
         from public.pay_batch_items as transfer_backed_candidate_item
         where transfer_backed_candidate_item.pay_batch_candidate_id = pbc.id
           and coalesce(transfer_backed_candidate_item.is_voided, false) = false
           and transfer_backed_candidate_item.pay_bank_transfer_id is not null
       )
       and not exists (
         select 1
         from public.banking_pay_operation_settlement_scope as transfer_backed_candidate_scope
         where transfer_backed_candidate_scope.operation_id = v_no_bank_settlement_operation_id
           and transfer_backed_candidate_scope.pay_batch_id = p_pay_batch_id
           and transfer_backed_candidate_scope.pay_batch_candidate_id = pbc.id
           and transfer_backed_candidate_scope.settlement_event_id is not null
       )
        then coalesce(pbc.settled_note, v_local_no_bank_commit_ref)
      else pbc.settled_note
    end
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t);

  select coalesce(jsonb_agg(t.candidate_id::text order by t.candidate_id), '[]'::jsonb)
  into v_newly_settled_candidates
  from _tmp_newly_settled_candidates t;

  with existing_finalised_candidate_ids as (
    select distinct finalised_candidate_marker.value #>> '{}' as candidate_id_text
    from jsonb_array_elements(
      case
        when jsonb_typeof(v_batch.settlement_confirmation_json->'durably_finalised_candidate_ids') = 'array'
          then v_batch.settlement_confirmation_json->'durably_finalised_candidate_ids'
        else '[]'::jsonb
      end
    ) as finalised_candidate_marker(value)
    where (finalised_candidate_marker.value #>> '{}')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and exists (
        select 1
        from public.pay_batch_candidates as existing_finalised_candidate
        where existing_finalised_candidate.pay_batch_id = p_pay_batch_id
          and existing_finalised_candidate.candidate_id::text = finalised_candidate_marker.value #>> '{}'
      )
  ), current_finalised_candidate_ids as (
    select newly_finalised_candidate.candidate_id::text as candidate_id_text
    from pg_temp._tmp_newly_settled_candidates as newly_finalised_candidate
  ), combined_finalised_candidate_ids as (
    select existing_finalised_candidate_ids.candidate_id_text
    from existing_finalised_candidate_ids
    union
    select current_finalised_candidate_ids.candidate_id_text
    from current_finalised_candidate_ids
  )
  select coalesce(
    jsonb_agg(combined_finalised_candidate_ids.candidate_id_text order by combined_finalised_candidate_ids.candidate_id_text),
    '[]'::jsonb
  )
  into v_durably_finalised_candidate_ids
  from combined_finalised_candidate_ids;


  create temp table if not exists _tmp_settle_cache_candidates (
    candidate_id uuid primary key
  ) on commit drop;

  truncate table _tmp_settle_cache_candidates;

  with candidate_transfer_state as (
    select
      pbc.candidate_id as candidate_id,
      bool_or(nsc.candidate_id is not null) as newly_settled_in_this_call,
      bool_or(coalesce(pbc.settled_at_utc, null) is not null or upper(coalesce(pbc.settlement_status, '')) = 'SETTLED') as already_settled_before_cache_rebuild,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and coalesce(pbi.is_voided, false) = false
          and pbi.pay_bank_transfer_id is not null
      ) as total_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and coalesce(pbi.is_voided, false) = false
          and pbi.pay_bank_transfer_id is not null
          and external_completed_cache_transfer.transfer_id is not null
      ) as completed_transfers
    from public.pay_batch_candidates pbc
    left join _tmp_newly_settled_candidates nsc
      on nsc.candidate_id = pbc.candidate_id
    left join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    left join pg_temp._tmp_settle_external_completed_transfers as external_completed_cache_transfer
      on external_completed_cache_transfer.transfer_id = pbi.pay_bank_transfer_id
    where pbc.pay_batch_id = p_pay_batch_id
    group by pbc.candidate_id
  ), eligible_cache_candidates as (
    select
      cts.candidate_id
    from candidate_transfer_state cts
    where (cts.newly_settled_in_this_call is true or cts.already_settled_before_cache_rebuild is true)
      and (
        (
          coalesce(cts.total_transfers, 0) = 0
          and (
            (
              v_no_bank_scope_authorised
              and exists (
                select 1
                from pg_temp.tmp_pay_settle_rail_full_eligible_no_bank_scope as zero_cache_scope
                where zero_cache_scope.candidate_id = cts.candidate_id
              )
            )
          )
        )
        or (coalesce(cts.total_transfers, 0) > 0 and coalesce(cts.total_transfers, 0) = coalesce(cts.completed_transfers, 0))
      )
  )
  insert into _tmp_settle_cache_candidates(candidate_id)
  select distinct ecc.candidate_id
  from eligible_cache_candidates ecc
  where ecc.candidate_id is not null;

  SELECT public._pay_manual_adjustment_carry_forward_mark_consumed(
    p_pay_batch_id,
    NULL::uuid[],
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'pay_bank_transfer_ids', COALESCE((
        SELECT jsonb_agg(DISTINCT completed_transfer_rows.transfer_id::text ORDER BY completed_transfer_rows.transfer_id::text)
        FROM pg_temp._tmp_settle_external_completed_transfers AS completed_transfer_rows
      ), '[]'::jsonb),
      'pay_batch_item_ids', COALESCE((
        SELECT jsonb_agg(DISTINCT item_rows.id::text ORDER BY item_rows.id::text)
        FROM public.pay_batch_items AS item_rows
        JOIN public.pay_batch_candidates AS candidate_rows
          ON candidate_rows.id = item_rows.pay_batch_candidate_id
        JOIN pg_temp._tmp_settle_external_completed_transfers AS completed_transfer_rows
          ON completed_transfer_rows.transfer_id = item_rows.pay_bank_transfer_id
        WHERE candidate_rows.pay_batch_id = p_pay_batch_id
      ), '[]'::jsonb)
    ),
    jsonb_build_object('final_paid', true, 'cash_state', 'FINAL_PAID', 'source', 'pay_settle_rail'),
    p_actor_user_id
  )
  INTO v_carry_forward_mark_result;

  v_consumed_carry_forward_count := v_consumed_carry_forward_count + COALESCE(NULLIF(v_carry_forward_mark_result->>'consumed_count', '')::integer, 0);

  with needed_timesheets as (
    select distinct
      pbi.timesheet_id as timesheet_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbc.candidate_id in (select t.candidate_id from _tmp_settle_cache_candidates t)
      and pbi.item_type <> 'DEBT_CREATED'
      and pbi.is_voided = false
      and pbi.timesheet_id is not null
  ),
  have_snap as (
    select distinct
      pbs.timesheet_id as timesheet_id
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_settle_cache_candidates t)
  ),
  missing as (
    select n.timesheet_id
    from needed_timesheets n
    left join have_snap h
      on h.timesheet_id = n.timesheet_id
    where h.timesheet_id is null
  )
  select coalesce(jsonb_agg(m.timesheet_id::text order by m.timesheet_id), '[]'::jsonb)
  into v_missing_timesheets
  from missing m;

  if jsonb_array_length(v_missing_timesheets) > 0 then
    raise exception 'pay_settle_rail: MISSING_FROZEN_SNAPSHOTS for timesheets %', v_missing_timesheets::text;
  end if;

  with snap as (
    select
      pbs.timesheet_id,
      pbs.target_snapshot_json
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_settle_cache_candidates t)
  ),
  ambig as (
    select
      s.timesheet_id
    from snap s
    group by s.timesheet_id
    having count(distinct s.target_snapshot_json) > 1
  )
  select coalesce(jsonb_agg(a.timesheet_id::text order by a.timesheet_id), '[]'::jsonb)
  into v_ambig_timesheets
  from ambig a;

  if jsonb_array_length(v_ambig_timesheets) > 0 then
    raise exception 'pay_settle_rail: AMBIGUOUS_TARGET_SNAPSHOT for timesheets %', v_ambig_timesheets::text;
  end if;

  with chosen as (
    select distinct on (pbs.timesheet_id)
      pbs.timesheet_id,
      pbs.target_snapshot_json,
      pbs.signature
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_settle_cache_candidates t)
    order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
  )
  insert into public.timesheet_pay_state_history(
    timesheet_id,
    pay_batch_id,
    settled_at_utc,
    snapshot_json,
    signature
  )
  select
    c.timesheet_id,
    p_pay_batch_id,
    v_now,
    c.target_snapshot_json,
    c.signature
  from chosen c
  where not exists (
    select 1
    from public.timesheet_pay_state_history existing_history
    where existing_history.timesheet_id = c.timesheet_id
      and existing_history.pay_batch_id = p_pay_batch_id
  );

  with chosen as (
    select distinct on (pbs.timesheet_id)
      pbs.timesheet_id,
      pbs.target_snapshot_json,
      pbs.signature
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_settle_cache_candidates t)
    order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
  ),
  affected_ids as (
    select coalesce(array_agg(distinct chosen.timesheet_id order by chosen.timesheet_id), array[]::uuid[]) as timesheet_ids
    from chosen
  ),
  outstanding_components as (
    select
      oc.timesheet_id,
      oc.truth_ex_vat,
      oc.baseline_ex_vat,
      oc.reserved_ex_vat,
      oc.outstanding_ex_vat
    from affected_ids
    cross join lateral public._pay_outstanding_components(affected_ids.timesheet_ids, p_pay_batch_id) oc
  ),
  summary_calc as (
    select
      c.timesheet_id,
      round(coalesce(sum(coalesce(oc.truth_ex_vat, 0) - coalesce(oc.baseline_ex_vat, 0)), 0), 2)::numeric(12,2) as summary_net_delta_ex_vat,
      round(coalesce(sum(coalesce(oc.reserved_ex_vat, 0)), 0), 2)::numeric(12,2) as summary_reserved_ex_vat,
      round(coalesce(sum(coalesce(oc.outstanding_ex_vat, 0)), 0), 2)::numeric(12,2) as summary_outstanding_ex_vat
    from chosen c
    left join outstanding_components oc
      on oc.timesheet_id = c.timesheet_id
    group by c.timesheet_id
  ),
  summary_display as (
    select
      c.timesheet_id,
      c.target_snapshot_json,
      c.signature,
      coalesce(sc.summary_net_delta_ex_vat, 0)::numeric(12,2) as summary_net_delta_ex_vat,
      coalesce(sc.summary_reserved_ex_vat, 0)::numeric(12,2) as summary_reserved_ex_vat,
      coalesce(sc.summary_outstanding_ex_vat, 0)::numeric(12,2) as summary_outstanding_ex_vat,
      case
        when coalesce(sc.summary_outstanding_ex_vat, 0) > 0 then 'PARTIALLY_PAID'
        when coalesce(sc.summary_reserved_ex_vat, 0) > 0 then 'PROCESSING'
        else 'PAID'
      end as summary_pay_status_code,
      case
        when coalesce(sc.summary_net_delta_ex_vat, 0) < -0.01 then 'RED_COIN'
        when coalesce(sc.summary_outstanding_ex_vat, 0) > 0 then 'HALF_COIN'
        when coalesce(sc.summary_reserved_ex_vat, 0) > 0 then 'CLOCK'
        else 'COIN'
      end as summary_pay_icon_code
    from chosen c
    left join summary_calc sc
      on sc.timesheet_id = c.timesheet_id
  )
  insert into public.timesheet_pay_state(
    timesheet_id,
    last_settled_snapshot_json,
    last_settled_signature,
    last_settled_pay_batch_id,
    last_settled_at_utc,
    summary_pay_status_code,
    summary_pay_icon_code,
    summary_pay_paid_at_utc,
    summary_net_delta_ex_vat
  )
  select
    sd.timesheet_id,
    sd.target_snapshot_json,
    sd.signature,
    p_pay_batch_id,
    v_now,
    sd.summary_pay_status_code,
    sd.summary_pay_icon_code,
    v_now,
    sd.summary_net_delta_ex_vat
  from summary_display sd
  on conflict (timesheet_id) do update
  set
    last_settled_snapshot_json = excluded.last_settled_snapshot_json,
    last_settled_signature = excluded.last_settled_signature,
    last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
    last_settled_at_utc = excluded.last_settled_at_utc,
    summary_pay_status_code = excluded.summary_pay_status_code,
    summary_pay_icon_code = excluded.summary_pay_icon_code,
    summary_pay_paid_at_utc = excluded.summary_pay_paid_at_utc,
    summary_net_delta_ex_vat = excluded.summary_net_delta_ex_vat;

  -- Operation-mode settlement deliberately leaves finance reservations RESERVED
  -- until every frozen settlement scope for the candidate is terminal.  Commit any
  -- such reservations here, immediately before the existing atomic settlement
  -- transition, so component and case balances are finalised from the same frozen
  -- batch artifacts.  Legacy/manual paths that already committed reservations are
  -- unaffected.
  with newly_committed_reservations as (
    update public.pay_advance_reservations par
    set
      status = 'COMMITTED',
      committed_at_utc = coalesce(par.committed_at_utc, v_now),
      updated_by_user_id = p_actor_user_id
    from public.pay_batch_items pbi,
         public.pay_batch_candidates pbc
    where par.pay_batch_id = p_pay_batch_id
      and par.pay_batch_item_id = pbi.id
      and pbc.id = pbi.pay_batch_candidate_id
      and pbc.pay_batch_id = p_pay_batch_id
      and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
      and upper(coalesce(par.status,'')) = 'RESERVED'
      and par.released_at_utc is null
      and par.settled_at_utc is null
    returning
      par.finance_case_id,
      par.finance_component_id,
      par.id as reservation_id,
      par.committed_at_utc
  )
  insert into public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  select
    committed_reservation.finance_case_id,
    committed_reservation.finance_component_id,
    'RESERVATION_COMMITTED',
    v_now,
    p_actor_user_id,
    p_pay_batch_id,
    committed_reservation.reservation_id,
    jsonb_build_object('reservation_status', 'RESERVED'),
    jsonb_build_object(
      'reservation_status', 'COMMITTED',
      'committed_at_utc', committed_reservation.committed_at_utc::text,
      'commit_stage', 'FULL_BATCH_SETTLEMENT_FINALISATION'
    ),
    'rail_settlement_commit',
    null
  from newly_committed_reservations committed_reservation;

  update public.pay_advance_reservations par
  set
    status = 'SETTLED',
    settled_at_utc = coalesce(par.settled_at_utc, v_now),
    updated_by_user_id = p_actor_user_id
  from public.pay_batch_items pbi,
       public.pay_batch_candidates pbc
  where par.pay_batch_id = p_pay_batch_id
    and par.pay_batch_item_id = pbi.id
    and pbc.id = pbi.pay_batch_candidate_id
    and pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    and upper(coalesce(par.status,'')) = 'COMMITTED';

  create temp table if not exists _tmp_component_settle (
    finance_component_id uuid not null primary key,
    finance_case_id uuid null,
    settled_source_amount numeric not null
  ) on commit drop;

  truncate table _tmp_component_settle;

  create temp table if not exists _tmp_component_settle_source (
    finance_component_id uuid null,
    finance_case_id uuid null,
    pay_batch_item_id uuid not null,
    reservation_id uuid null,
    frozen_component_key_type text null,
    frozen_component_key_value text null,
    settled_source_amount numeric not null
  ) on commit drop;

  truncate table _tmp_component_settle_source;

  create temp table if not exists _tmp_component_settle_unresolved (
    pay_batch_id uuid not null,
    pay_batch_item_id uuid not null,
    reservation_id uuid null,
    finance_case_id uuid null,
    frozen_component_key_type text null,
    frozen_component_key_value text null,
    affected_source_amount numeric not null,
    reason_code text not null
  ) on commit drop;

  truncate table _tmp_component_settle_unresolved;

  insert into _tmp_component_settle_source(
    finance_component_id,
    finance_case_id,
    pay_batch_item_id,
    reservation_id,
    frozen_component_key_type,
    frozen_component_key_value,
    settled_source_amount
  )
  select
    coalesce(
      par.finance_component_id,
      pbi.finance_component_id
    ) as finance_component_id,
    coalesce(
      par.finance_case_id,
      pbi.finance_case_id
    ) as finance_case_id,
    pbi.id as pay_batch_item_id,
    par.id as reservation_id,
    nullif(
      btrim(
        coalesce(
          par.frozen_component_key_type,
          pbi.frozen_component_key_type,
          par.frozen_component_snapshot_json->>'component_key_type',
          pbi.frozen_component_snapshot_json->>'component_key_type',
          ''
        )
      ),
      ''
    ) as frozen_component_key_type,
    nullif(
      btrim(
        coalesce(
          par.frozen_component_key_value,
          pbi.frozen_component_key_value,
          par.frozen_component_snapshot_json->>'component_key_value',
          pbi.frozen_component_snapshot_json->>'component_key_value',
          ''
        )
      ),
      ''
    ) as frozen_component_key_value,
    round(
      sum(
        coalesce(
          par.reserved_source_amount,
          pbi.frozen_source_amount,
          abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, par.reserved_amount, 0))
        )
      ),
      2
    ) as settled_source_amount
  from public.pay_advance_reservations par
  join public.pay_batch_items pbi
    on pbi.id = par.pay_batch_item_id
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
   and pbc.pay_batch_id = p_pay_batch_id
  where par.pay_batch_id = p_pay_batch_id
    and upper(coalesce(par.status,'')) = 'SETTLED'
    and par.settled_at_utc = v_now
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
  group by
    coalesce(
      par.finance_component_id,
      pbi.finance_component_id
    ),
    coalesce(
      par.finance_case_id,
      pbi.finance_case_id
    ),
    pbi.id,
    par.id,
    nullif(
      btrim(
        coalesce(
          par.frozen_component_key_type,
          pbi.frozen_component_key_type,
          par.frozen_component_snapshot_json->>'component_key_type',
          pbi.frozen_component_snapshot_json->>'component_key_type',
          ''
        )
      ),
      ''
    ),
    nullif(
      btrim(
        coalesce(
          par.frozen_component_key_value,
          pbi.frozen_component_key_value,
          par.frozen_component_snapshot_json->>'component_key_value',
          pbi.frozen_component_snapshot_json->>'component_key_value',
          ''
        )
      ),
      ''
    )
  having round(
    sum(
      coalesce(
        par.reserved_source_amount,
        pbi.frozen_source_amount,
        abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, par.reserved_amount, 0))
      )
    ),
    2
  ) > 0;

  insert into _tmp_component_settle(finance_component_id, finance_case_id, settled_source_amount)
  select
    component_source.finance_component_id,
    component_source.finance_case_id,
    round(sum(component_source.settled_source_amount), 2) as settled_source_amount
  from _tmp_component_settle_source as component_source
  where component_source.finance_component_id is not null
    and component_source.settled_source_amount > 0
  group by component_source.finance_component_id,
           component_source.finance_case_id
  having round(sum(component_source.settled_source_amount), 2) > 0;

  insert into _tmp_component_settle_unresolved(
    pay_batch_id,
    pay_batch_item_id,
    reservation_id,
    finance_case_id,
    frozen_component_key_type,
    frozen_component_key_value,
    affected_source_amount,
    reason_code
  )
  select
    p_pay_batch_id,
    component_source.pay_batch_item_id,
    component_source.reservation_id,
    component_source.finance_case_id,
    component_source.frozen_component_key_type,
    component_source.frozen_component_key_value,
    component_source.settled_source_amount,
    'SETTLEMENT_COMPONENT_ID_MISSING_FROM_FROZEN_ARTIFACT'
  from _tmp_component_settle_source as component_source
  where component_source.finance_component_id is null
    and component_source.settled_source_amount > 0;

  select
    count(*)::int,
    round(coalesce(sum(component_unresolved.affected_source_amount), 0), 2),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'reason_code', component_unresolved.reason_code,
          'pay_batch_id', component_unresolved.pay_batch_id::text,
          'pay_batch_item_id', component_unresolved.pay_batch_item_id::text,
          'reservation_id', case when component_unresolved.reservation_id is null then null else component_unresolved.reservation_id::text end,
          'finance_case_id', case when component_unresolved.finance_case_id is null then null else component_unresolved.finance_case_id::text end,
          'frozen_component_key_type', component_unresolved.frozen_component_key_type,
          'frozen_component_key_value', component_unresolved.frozen_component_key_value,
          'affected_source_amount', component_unresolved.affected_source_amount
        )
        order by component_unresolved.pay_batch_item_id::text,
                 component_unresolved.reservation_id::text
      ),
      '[]'::jsonb
    )
  into
    v_component_unresolved_count,
    v_component_unresolved_amount,
    v_component_unresolved_json
  from _tmp_component_settle_unresolved as component_unresolved;

  create temp table if not exists _tmp_component_settle_apply (
    finance_component_id uuid not null primary key,
    finance_case_id uuid null,
    classification public.pay_finance_component_classification_enum not null,
    settled_source_amount numeric not null,
    remaining_before numeric not null,
    remaining_after numeric not null
  ) on commit drop;

  truncate table _tmp_component_settle_apply;

  insert into _tmp_component_settle_apply(
    finance_component_id,
    finance_case_id,
    classification,
    settled_source_amount,
    remaining_before,
    remaining_after
  )
  select
    pfc.id,
    pfc.finance_case_id,
    pfc.classification,
    tcs.settled_source_amount,
    round(coalesce(pfc.remaining_source_amount, 0), 2) as remaining_before,
    round(greatest(coalesce(pfc.remaining_source_amount, 0) - coalesce(tcs.settled_source_amount, 0), 0), 2) as remaining_after
  from _tmp_component_settle tcs
  join public.pay_finance_case_components pfc
    on pfc.id = tcs.finance_component_id;

  update public.pay_finance_case_components pfc
  set
    remaining_source_amount = csa.remaining_after,
    resolved_at_utc = case
      when csa.remaining_after <= 0 then coalesce(pfc.resolved_at_utc, v_now)
      else pfc.resolved_at_utc
    end,
    updated_at_utc = v_now
  from _tmp_component_settle_apply csa
  where pfc.id = csa.finance_component_id;

  select
    count(*)::int,
    round(coalesce(sum(csa.settled_source_amount), 0), 2)
  into
    v_component_settled_count,
    v_component_settled_amount
  from _tmp_component_settle_apply csa;

  insert into public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  select
    csa.finance_case_id,
    csa.finance_component_id,
    'COMPONENT_SETTLED',
    v_now,
    p_actor_user_id,
    p_pay_batch_id,
    null::uuid,
    jsonb_build_object(
      'remaining_source_amount', csa.remaining_before
    ),
    jsonb_build_object(
      'remaining_source_amount', csa.remaining_after,
      'settled_source_amount', csa.settled_source_amount,
      'classification', csa.classification::text
    ),
    'rail_settlement',
    null
  from _tmp_component_settle_apply csa;

  insert into public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  select
    par.finance_case_id,
    par.finance_component_id,
    'RESERVATION_SETTLED',
    v_now,
    p_actor_user_id,
    p_pay_batch_id,
    par.id,
    jsonb_build_object('reservation_status', 'COMMITTED'),
    jsonb_build_object('reservation_status', 'SETTLED', 'settled_at_utc', v_now::text),
    'rail_settlement',
    null
  from public.pay_advance_reservations par
  where par.pay_batch_id = p_pay_batch_id
    and upper(coalesce(par.status,'')) = 'SETTLED'
    and par.settled_at_utc = v_now;

  create temp table if not exists _tmp_repay_taken (
    finance_case_id uuid not null,
    repayment_week_start date null,
    item_type text not null,
    taken_amount numeric not null
  ) on commit drop;

  truncate table _tmp_repay_taken;

  insert into _tmp_repay_taken(finance_case_id, repayment_week_start, item_type, taken_amount)
  select
    coalesce(
      pbi.finance_case_id,
      nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid
    ) as finance_case_id,
    pbi.repayment_week_start,
    pbi.item_type,
    round(
      sum(
        abs(
          coalesce(
            pbi.frozen_source_amount,
            pbi.amount_ex_vat,
            pbi.amount_inc_vat,
            0
          )
        )
      ),
      2
    ) as taken_amount
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    and pbi.is_voided = false
    and pbi.item_type in ('LOAN_REPAYMENT','OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','UNDERPAYMENT_PAYMENT')
    and coalesce(
      pbi.finance_case_id,
      nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid
    ) is not null
  group by
    coalesce(
      pbi.finance_case_id,
      nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid
    ),
    pbi.repayment_week_start,
    pbi.item_type
  having round(
    sum(
      abs(
        coalesce(
          pbi.frozen_source_amount,
          pbi.amount_ex_vat,
          pbi.amount_inc_vat,
          0
        )
      )
    ),
    2
  ) > 0;

  for v_adv_id in
    select distinct trt.finance_case_id
    from _tmp_repay_taken trt
    where trt.finance_case_id is not null
  loop
    select
      pa.schedule_json,
      pa.outstanding_amount,
      pa.next_due_week_start,
      pa.case_type,
      pa.minimum_earnings_threshold,
      pa.take_home_floor_override,
      (pa.cleared_at_utc is not null) as was_cleared
    into
      v_old_sched,
      v_old_out,
      v_old_next,
      v_case_type,
      v_min_earnings_threshold,
      v_take_home_floor_override,
      v_was_cleared
    from public.pay_advances pa
    where pa.id = v_adv_id
    for update;

    if v_old_sched is null then
      v_old_sched := '[]'::jsonb;
    end if;

    select round(coalesce(sum(trt.taken_amount),0),2)
    into v_total_taken
    from _tmp_repay_taken trt
    where trt.finance_case_id = v_adv_id;

    v_new_out := round(greatest(coalesce(v_old_out,0) - coalesce(v_total_taken,0), 0), 2);

    /*
      Componentised cases use their open component ledger as the settlement
      authority.  The case-level outstanding value can be a current-run due
      rather than the whole residual, especially after a cross-pay-method
      resolution.  Rebuild the post-settlement case balance from the already
      updated source-side components so a partial recovery leaves the true
      residual instead of clearing the case.
    */
    if exists (
      select 1
      from public.pay_finance_case_components pfc_any
      where pfc_any.finance_case_id = v_adv_id
      limit 1
    ) then
      select round(
        coalesce(
          sum(coalesce(pfc_open.remaining_source_amount, 0))
            filter (where pfc_open.closed_at_utc is null),
          0
        ),
        2
      )
      into v_new_out
      from public.pay_finance_case_components pfc_open
      where pfc_open.finance_case_id = v_adv_id;
    end if;

    if v_case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
       or v_case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum then
      with taken_map as (
        select
          trt.repayment_week_start as week_start,
          round(sum(trt.taken_amount),2) as taken_amount
        from _tmp_repay_taken trt
        where trt.finance_case_id = v_adv_id
        group by trt.repayment_week_start
      ),
      expanded as (
        select
          e.elem as elem,
          nullif(e.elem->>'week_start','')::date as wk,
          coalesce(nullif(e.elem->>'amount','')::numeric,0) as amt
        from jsonb_array_elements(coalesce(v_old_sched,'[]'::jsonb)) e(elem)
      ),
      rewritten as (
        select
          case
            when em.wk is not null
             and tm.week_start is not null
             and em.wk = tm.week_start
             and em.amt < 0
            then jsonb_set(
              em.elem,
              '{amount}',
              to_jsonb(round(em.amt + tm.taken_amount, 2)),
              true
            )
            else em.elem
          end as elem
        from expanded em
        left join taken_map tm
          on tm.week_start = em.wk
      )
      select coalesce(jsonb_agg(r.elem), '[]'::jsonb)
      into v_new_sched
      from rewritten r;

      with expanded2 as (
        select
          nullif(e2.elem->>'week_start','')::date as wk,
          coalesce(nullif(e2.elem->>'amount','')::numeric,0) as amt
        from jsonb_array_elements(coalesce(v_new_sched,'[]'::jsonb)) e2(elem)
      )
      select min(ex2.wk)
      into v_new_next
      from expanded2 ex2
      where ex2.wk is not null
        and ex2.amt < 0;
    else
      v_new_sched := v_old_sched;
      v_new_next := v_old_next;
    end if;

    select exists (
      select 1
      from public.pay_item_snoozes pis
      where v_today_uk is not null
        and pis.cleared_at_utc is null
        and pis.cancelled_at_utc is null
        and pis.source_ref = ('advance:' || v_adv_id::text)
        and (
          pis.snooze_until_date is null
          or pis.snooze_until_date >= v_today_uk
        )
      limit 1
    )
    into v_snooze_active;

    IF COALESCE(v_snooze_active, false) THEN
      v_active_snooze_report_count := COALESCE(v_active_snooze_report_count, 0) + 1;
      IF jsonb_array_length(COALESCE(v_active_snooze_report_sample, '[]'::jsonb)) < 25 THEN
        v_active_snooze_report_sample := COALESCE(v_active_snooze_report_sample, '[]'::jsonb) || jsonb_build_array(
          jsonb_strip_nulls(jsonb_build_object(
            'finance_case_id', v_adv_id::text,
            'source_ref', 'advance:' || v_adv_id::text,
            'finance_case_type', v_case_type::text,
            'active_as_of_london_date', v_today_uk::text,
            'reporting_effect_only', true,
            'frozen_settlement_authority_unchanged', true
          ))
        );
      END IF;
    END IF;

    if v_case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
       or v_case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum then
      insert into public.pay_finance_case_events(
        finance_case_id,
        event_type,
        event_at_utc,
        actor_user_id,
        pay_batch_id,
        reservation_id,
        before_json,
        after_json,
        reason,
        note
      )
      select
        v_adv_id,
        'RECOVERY_SHORTFALL',
        v_now,
        p_actor_user_id,
        p_pay_batch_id,
        null::uuid,
        jsonb_build_object(
          'week_start', exp.week_start::text,
          'scheduled_due_amount', exp.expected_due,
          'taken_amount', coalesce(tkn.taken_amount,0)
        ),
        jsonb_build_object(
          'week_start', exp.week_start::text,
          'scheduled_due_amount', exp.expected_due,
          'taken_amount', coalesce(tkn.taken_amount,0),
          'remaining_due_amount', round(greatest(exp.expected_due - coalesce(tkn.taken_amount,0),0),2)
        ),
        case
          when v_snooze_active then 'SNOOZED'
          when coalesce(v_take_home_floor_override,0) > 0 then 'MIN_TAKE_HOME_FLOOR'
          when coalesce(v_min_earnings_threshold,0) > 0 then 'INSUFFICIENT_EARNINGS'
          else 'INSUFFICIENT_EARNINGS'
        end,
        null
      from (
        select
          nullif(elem->>'week_start','')::date as week_start,
          abs(coalesce(nullif(elem->>'amount','')::numeric,0)) as expected_due
        from jsonb_array_elements(coalesce(v_old_sched,'[]'::jsonb)) elem
        where nullif(elem->>'week_start','') is not null
          and coalesce(nullif(elem->>'amount','')::numeric,0) < 0
      ) exp
      left join (
        select trt.repayment_week_start as week_start, round(sum(trt.taken_amount),2) as taken_amount
        from _tmp_repay_taken trt
        where trt.finance_case_id = v_adv_id
        group by trt.repayment_week_start
      ) tkn
        on tkn.week_start = exp.week_start
      where round(greatest(exp.expected_due - coalesce(tkn.taken_amount,0),0),2) > 0;
    end if;

    update public.pay_advances pa2
    set
      schedule_json = coalesce(v_new_sched,'[]'::jsonb),
      outstanding_amount = v_new_out,
      next_due_week_start = v_new_next,
      status = case
        when v_case_type in ('PAYMENT_ADVANCE'::public.pay_finance_case_type_enum, 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum, 'OVERPAYMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
             and v_new_out <= 0 then 'PAID_OFF'::public.pay_advance_status_enum
        else pa2.status
      end,
      cleared_at_utc = case
        when v_case_type in ('PAYMENT_ADVANCE'::public.pay_finance_case_type_enum, 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum, 'OVERPAYMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
             and v_new_out <= 0 then coalesce(pa2.cleared_at_utc, v_now)
        else pa2.cleared_at_utc
      end,
      cleared_by_user_id = case
        when v_case_type in ('PAYMENT_ADVANCE'::public.pay_finance_case_type_enum, 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum, 'OVERPAYMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
             and v_new_out <= 0 then coalesce(pa2.cleared_by_user_id, p_actor_user_id)
        else pa2.cleared_by_user_id
      end,
      updated_at = v_now
    where pa2.id = v_adv_id;

    insert into public.pay_advance_patches(
      advance_id,
      pay_batch_id,
      old_outstanding_amount,
      new_outstanding_amount,
      old_schedule_json,
      new_schedule_json,
      old_next_due_week_start,
      new_next_due_week_start
    )
    values (
      v_adv_id,
      p_pay_batch_id,
      v_old_out,
      v_new_out,
      v_old_sched,
      v_new_sched,
      v_old_next,
      v_new_next
    );

    insert into public.pay_finance_case_events(
      finance_case_id,
      event_type,
      event_at_utc,
      actor_user_id,
      pay_batch_id,
      reservation_id,
      before_json,
      after_json,
      reason,
      note
    )
    values (
      v_adv_id,
      'RECOVERY_SETTLED',
      v_now,
      p_actor_user_id,
      p_pay_batch_id,
      null::uuid,
      jsonb_build_object(
        'outstanding_amount', v_old_out,
        'next_due_week_start', case when v_old_next is null then null else v_old_next::text end,
        'schedule_json', v_old_sched
      ),
      jsonb_build_object(
        'outstanding_amount', v_new_out,
        'next_due_week_start', case when v_new_next is null then null else v_new_next::text end,
        'schedule_json', v_new_sched,
        'taken_amount', v_total_taken,
        'case_type', v_case_type::text
      ),
      'rail_settlement',
      null
    );

    if v_new_out <= 0 then
      insert into public.pay_finance_case_events(
        finance_case_id,
        event_type,
        event_at_utc,
        actor_user_id,
        pay_batch_id,
        reservation_id,
        before_json,
        after_json,
        reason,
        note
      )
      values (
        v_adv_id,
        'CASE_CLEARED',
        v_now,
        p_actor_user_id,
        p_pay_batch_id,
        null::uuid,
        jsonb_build_object(
          'outstanding_amount', v_old_out,
          'was_cleared', v_was_cleared
        ),
        jsonb_build_object(
          'outstanding_amount', v_new_out,
          'cleared_at_utc', v_now::text,
          'cleared_by_user_id', p_actor_user_id::text
        ),
        'rail_settlement',
        null
      );
    end if;

    if v_case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum then
      v_overpay_patched_ct := v_overpay_patched_ct + 1;
    elsif v_case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum then
      v_payment_advance_recovery_patched_ct := v_payment_advance_recovery_patched_ct + 1;
    elsif v_case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum then
      v_manual_debt_patched_ct := v_manual_debt_patched_ct + 1;
    end if;
  end loop;

  with affected_component_cases as (
    select distinct csa.finance_case_id
    from _tmp_component_settle_apply csa
    where csa.finance_case_id is not null
    union
    select distinct trt.finance_case_id
    from _tmp_repay_taken trt
    where trt.finance_case_id is not null
      and exists (
        select 1
        from public.pay_finance_case_components pfc_exists
        where pfc_exists.finance_case_id = trt.finance_case_id
        limit 1
      )
  ),
  reconciled_component_cases as (
    select
      afc.finance_case_id,
      round(coalesce(pa.outstanding_amount, 0), 2) as case_outstanding_amount,
      round(coalesce(sum(coalesce(pfc.remaining_source_amount, 0)), 0), 2) as open_component_remaining_source_amount,
      count(pfc.id)::int as open_component_count
    from affected_component_cases afc
    join public.pay_advances pa
      on pa.id = afc.finance_case_id
    left join public.pay_finance_case_components pfc
      on pfc.finance_case_id = afc.finance_case_id
     and pfc.closed_at_utc is null
    where exists (
      select 1
      from public.pay_finance_case_components pfc_any
      where pfc_any.finance_case_id = afc.finance_case_id
      limit 1
    )
    group by
      afc.finance_case_id,
      pa.outstanding_amount
  ),
  reconciliation_bad as (
    select
      rcc.finance_case_id,
      rcc.case_outstanding_amount,
      rcc.open_component_remaining_source_amount,
      rcc.open_component_count,
      round(rcc.case_outstanding_amount - rcc.open_component_remaining_source_amount, 2) as mismatch_amount
    from reconciled_component_cases rcc
    where abs(round(rcc.case_outstanding_amount - rcc.open_component_remaining_source_amount, 2)) > 0.01
      and not exists (
        select 1
        from _tmp_component_settle_unresolved component_unresolved
        where component_unresolved.finance_case_id = rcc.finance_case_id
      )
  )
  select
    (select count(*)::int from reconciled_component_cases),
    count(*)::int,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'finance_case_id', reconciliation_bad.finance_case_id::text,
          'case_outstanding_amount', reconciliation_bad.case_outstanding_amount,
          'open_component_remaining_source_amount', reconciliation_bad.open_component_remaining_source_amount,
          'open_component_count', reconciliation_bad.open_component_count,
          'mismatch_amount', reconciliation_bad.mismatch_amount
        )
        order by reconciliation_bad.finance_case_id::text
      ),
      '[]'::jsonb
    )
  into
    v_component_reconciliation_checked_ct,
    v_component_reconciliation_bad_ct,
    v_component_reconciliation_bad
  from reconciliation_bad;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_SETTLE_RAIL:COMPONENT_RECONCILIATION_RESULT',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'checked_count', coalesce(v_component_reconciliation_checked_ct, 0),
        'mismatch_count', coalesce(v_component_reconciliation_bad_ct, 0),
        'mismatches', v_component_reconciliation_bad,
        'component_id_missing_from_frozen_artifact_count', coalesce(v_component_unresolved_count, 0),
        'component_id_missing_from_frozen_artifact_amount', coalesce(v_component_unresolved_amount, 0),
        'component_id_missing_from_frozen_artifact', coalesce(v_component_unresolved_json, '[]'::jsonb)
      ),
      'pay_batches',
      p_pay_batch_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  if coalesce(v_component_reconciliation_bad_ct, 0) > 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_SETTLE_RAIL_COMPONENT_RECONCILIATION_MISMATCH',
      'pay_batch_id', p_pay_batch_id::text,
      'message', 'Finance case outstanding amount does not reconcile to open component remaining_source_amount after settlement.',
      'mismatches', v_component_reconciliation_bad
    )::text;
  end if;


  with payouts as (
    select distinct
      coalesce(
        pbi.finance_case_id,
        nullif(btrim(replace(coalesce(pbi.source_ref,''), 'advance:', '')),'')::uuid
      ) as finance_case_id,
      pbi.pay_bank_transfer_id as transfer_id,
      pbi.item_type
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_bank_transfers pbt
      on pbt.id = pbi.pay_bank_transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    join pg_temp._tmp_settle_external_completed_transfers as external_completed_payout_transfer
      on external_completed_payout_transfer.transfer_id = pbt.id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
      and pbi.is_voided = false
      and pbi.item_type in ('LOAN_PAYOUT','MANUAL_CREDIT_PAYOUT','UNDERPAYMENT_PAYMENT')
      and pbi.pay_bank_transfer_id is not null
      and coalesce(
        pbi.finance_case_id,
        nullif(btrim(replace(coalesce(pbi.source_ref,''), 'advance:', '')),'')::uuid
      ) is not null
  ),
  upd as (
    update public.pay_advances pa
    set
      payout_status = 'PAID'::public.pay_advance_payout_status_enum,
      payout_pay_batch_id = p_pay_batch_id,
      payout_transfer_id = p.transfer_id,
      updated_at = v_now
    from payouts p
    where pa.id = p.finance_case_id
      and pa.case_type in ('PAYMENT_ADVANCE'::public.pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
      and coalesce(pa.payout_status::text,'') <> 'PAID'
    returning pa.id, pa.case_type, p.transfer_id, p.item_type
  )
  select count(*)::int
  into v_payout_cases_marked_paid_ct
  from upd;

  insert into public.pay_finance_case_events(
    finance_case_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  select
    upd.id,
    'PAYOUT_SETTLED',
    v_now,
    p_actor_user_id,
    p_pay_batch_id,
    null::uuid,
    jsonb_build_object('payout_status', 'PENDING_OR_COMMITTED'),
    jsonb_build_object(
      'payout_status', 'PAID',
      'payout_transfer_id', upd.transfer_id::text,
      'item_type', upd.item_type,
      'case_type', upd.case_type::text
    ),
    'rail_settlement',
    null
  from (
    select distinct
      pa.id,
      pa.case_type,
      pbi.pay_bank_transfer_id as transfer_id,
      pbi.item_type
    from public.pay_advances pa
    join public.pay_batch_items pbi
      on pbi.finance_case_id = pa.id
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_bank_transfers pbt
      on pbt.id = pbi.pay_bank_transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    join pg_temp._tmp_settle_external_completed_transfers as external_completed_payout_event_transfer
      on external_completed_payout_event_transfer.transfer_id = pbt.id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
      and pbi.is_voided = false
      and pbi.item_type in ('LOAN_PAYOUT','MANUAL_CREDIT_PAYOUT','UNDERPAYMENT_PAYMENT')
      and pa.case_type in ('PAYMENT_ADVANCE'::public.pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum, 'UNDERPAYMENT'::public.pay_finance_case_type_enum)
  ) upd;

  with payable_transfer_ids as (
    select distinct
      pbi.pay_bank_transfer_id as transfer_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.item_type <> 'DEBT_CREATED'
      and pbi.is_voided = false
      and pbi.pay_bank_transfer_id is not null
  ),
  stats as (
    select
      count(*)::int as total_ct,
      sum(case when upper(coalesce(pbt.status,'')) in ('PENDING','PROCESSING','UNKNOWN') then 1 else 0 end)::int as pending_ct,
      sum(case when external_completed_status_transfer.transfer_id is not null then 1 else 0 end)::int as completed_ct,
      sum(case when external_completed_status_transfer.transfer_id is not null then 1 else 0 end)::int as settled_evidence_ct,
      sum(case when upper(coalesce(pbt.status,'')) in ('FAILED','DECLINED','REJECTED','CANCELLED') then 1 else 0 end)::int as failed_ct,
      sum(case when upper(coalesce(pbt.status,'')) in ('RETURNED','REVERTED') then 1 else 0 end)::int as returned_ct,
      sum(case when upper(coalesce(pbt.status,'')) = 'BLOCKED' then 1 else 0 end)::int as blocked_ct
    from payable_transfer_ids pti
    join public.pay_bank_transfers pbt
      on pbt.id = pti.transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    left join pg_temp._tmp_settle_external_completed_transfers as external_completed_status_transfer
      on external_completed_status_transfer.transfer_id = pbt.id
  )
  select
    case
      when coalesce(s.total_ct, 0) = 0 and v_no_bank_payment_execution_validated then 'SETTLED'
      when coalesce(s.total_ct, 0) = 0 then 'PARTIAL'
      when coalesce(s.pending_ct,0) = 0
       and coalesce(s.failed_ct,0) = 0
       and coalesce(s.blocked_ct,0) = 0
       and coalesce(s.returned_ct,0) = 0
       and coalesce(s.settled_evidence_ct,0) >= coalesce(s.total_ct,0)
        then 'SETTLED'
      when coalesce(s.pending_ct,0) = 0
       and coalesce(s.failed_ct,0) = 0
       and coalesce(s.blocked_ct,0) = 0
       and coalesce(s.returned_ct,0) > 0
       and coalesce(s.settled_evidence_ct,0) > 0
        then 'SETTLED'
      when coalesce(s.pending_ct,0) = 0
       and coalesce(s.failed_ct,0) > 0
       and coalesce(s.returned_ct,0) = 0
        then 'FAILED'
      else 'PARTIAL'
    end
  into v_batch_status
  from stats s;

  v_completed_with_failed_payments := (v_batch_status = 'FAILED');
  v_batch_status_label := CASE
    WHEN v_batch_status = 'FAILED' THEN 'Completed with failed payments'
    WHEN v_batch_status = 'SETTLED' THEN 'Completed'
    WHEN v_batch_status = 'PARTIAL' THEN 'Partially completed'
    ELSE v_batch_status
  END;

  v_settlement_confirmation_json := COALESCE(v_batch.settlement_confirmation_json, '{}'::jsonb);

  IF COALESCE(v_completed_transfer_count, 0) > 0 THEN
    IF v_settlement_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT') THEN
      v_settlement_confirmation_json := v_settlement_confirmation_json
        - 'provider_reference'
        - 'provider_request_id'
        - 'provider_submission_id'
        - 'provider_transaction_id'
        - 'rail_tx_id';
    ELSIF v_no_bank_scope_authorised THEN
      v_settlement_confirmation_json := v_settlement_confirmation_json - 'local_commit_reference';
    END IF;

    v_settlement_confirmation_json := v_settlement_confirmation_json
      || JSONB_STRIP_NULLS(
        JSONB_BUILD_OBJECT(
          'settlement_mode', v_settlement_mode,
          'provisional_scope_settlement', CASE WHEN v_batch_status IN ('SETTLED', 'FAILED') THEN false ELSE NULL::boolean END,
          'full_batch_finalised', (v_batch_status IN ('SETTLED', 'FAILED')),
          'full_batch_finalised_at_utc', CASE
            WHEN v_batch_status IN ('SETTLED', 'FAILED') THEN COALESCE(
              NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'full_batch_finalised_at_utc', '')), ''),
              v_now::text
            )
            ELSE NULL::text
          END,
          'confirmation_mode', CASE
            WHEN v_no_bank_scope_authorised THEN 'MIXED_TRANSFER_AND_NO_BANK_PAYMENT'
            WHEN v_settlement_mode = 'CSV_SETTLEMENT' THEN 'CSV_MANUAL_CONFIRM'
            WHEN v_settlement_mode = 'EXTERNAL_SETTLEMENT' THEN 'EXTERNAL_MANUAL_CONFIRM'
            ELSE 'PROVIDER_TERMINAL_SETTLEMENT'
          END,
          'settled_at_utc', COALESCE(
            NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'settled_at_utc', '')), ''),
            v_now::text
          ),
          'settled_by_user_id', COALESCE(
            NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'settled_by_user_id', '')), ''),
            p_actor_user_id::text
          ),
          'payment_date', CASE WHEN v_effective_payment_date IS NULL THEN NULL::text ELSE v_effective_payment_date::text END,
          'execution_operation_id', v_execution_operation_id::text,
          'settlement_operation_id', CASE WHEN v_settlement_operation_id IS NULL THEN NULL::text ELSE v_settlement_operation_id::text END,
          'auth_request_id', v_operation_auth_request_id::text,
          'projection_scope', v_operation_projection_scope
        )
        || JSONB_BUILD_OBJECT(
          'global_paye_net_state_hash', v_expected_paye_net_state_hash,
          'paye_net_state_hash', v_expected_paye_net_state_hash,
          'scoped_paye_net_state_hash', v_expected_scoped_paye_net_state_hash,
          'global_bank_payment_projection_hash', v_expected_global_bank_payment_projection_hash,
          'scoped_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
          'bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
          'projection_changed_after_authorisation', v_current_projection_changed,
          'positive_transfer_count', COALESCE(v_completed_transfer_count, 0),
          'total_bank_out', ROUND(COALESCE(v_completed_transfer_total, 0), 2),
          'pay_bank_transfer_ids', COALESCE(v_completed_transfer_ids, '[]'::jsonb),
          'pay_bank_transfer_id', CASE
            WHEN JSONB_ARRAY_LENGTH(COALESCE(v_completed_transfer_ids, '[]'::jsonb)) = 1 THEN v_completed_transfer_ids->>0
            ELSE NULL::text
          END,
          'settlement_scope_ids', COALESCE(v_settlement_scope_ids, '[]'::jsonb),
          'settlement_scope_id', CASE
            WHEN JSONB_ARRAY_LENGTH(COALESCE(v_settlement_scope_ids, '[]'::jsonb)) = 1 THEN v_settlement_scope_ids->>0
            ELSE NULL::text
          END
        )
        || JSONB_BUILD_OBJECT(
          'execution_commit_ref', COALESCE(
            NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), ''),
            NULLIF(BTRIM(COALESCE(v_detected_execution_commit_ref, '')), ''),
            NULLIF(BTRIM(COALESCE(v_local_positive_commit_ref, '')), '')
          ),
          'local_commit_reference', CASE
            WHEN v_settlement_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
              THEN COALESCE(NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), ''), v_local_positive_commit_ref)
            ELSE NULL::text
          END,
          'bank_confirm_ref', CASE WHEN v_settlement_mode = 'CSV_SETTLEMENT' THEN v_bank_confirm_ref ELSE NULL::text END,
          'external_comment', CASE
            WHEN v_settlement_mode = 'EXTERNAL_SETTLEMENT'
              THEN NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'external_settlement_comment', '')), '')
            ELSE NULL::text
          END,
          'csv_export_hash', CASE
            WHEN v_settlement_mode = 'CSV_SETTLEMENT'
              THEN NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'bank_csv_export_hash', v_batch_intent_json->>'bank_csv_export_hash', '')), '')
            ELSE NULL::text
          END,
          'csv_uploaded_confirmed', CASE
            WHEN v_settlement_mode = 'CSV_SETTLEMENT'
              THEN LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'csv_uploaded_confirmed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            ELSE NULL::boolean
          END,
          'external_settlement_confirmed', CASE WHEN v_settlement_mode = 'EXTERNAL_SETTLEMENT' THEN true ELSE NULL::boolean END,
          'submitted_to_bank', v_submitted_to_bank,
          'provider_submission_required', v_provider_submission_required,
          'provider_submission_attempted', v_provider_submission_attempted,
          'local_settlement_evidence_only', v_local_settlement_evidence_only
        )
        || JSONB_BUILD_OBJECT(
          'contains_no_bank_payment_scopes', CASE WHEN v_no_bank_scope_authorised THEN true ELSE NULL::boolean END,
          'no_bank_payment_reason', CASE WHEN v_no_bank_scope_authorised THEN 'EXPLICIT_ZERO_PAYE' ELSE NULL::text END,
          'zero_scope_count', CASE WHEN v_no_bank_scope_authorised THEN COALESCE(v_no_bank_total_scope_count, 0) ELSE 0 END,
          'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_marker,
          'no_bank_scope_reference', CASE WHEN v_no_bank_scope_authorised THEN v_local_no_bank_commit_ref ELSE NULL::text END,
          'suppress_remittances', v_suppress_remittances,
          'remittances_suppressed_at_utc', CASE
            WHEN v_suppress_remittances THEN COALESCE(
              NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'remittances_suppressed_at_utc', '')), ''),
              v_now::text
            )
            ELSE NULL::text
          END,
          'no_bank_payment_note', CASE
            WHEN v_no_bank_scope_authorised
              THEN 'Explicit-zero PAYE scopes were settled locally without transfer or provider evidence; the positive-payment commit proof remains authoritative.'
            ELSE NULL::text
          END
        )
      );
  ELSIF v_no_bank_payment_execution_validated AND v_batch_status = 'SETTLED' THEN
    v_settlement_confirmation_json := v_settlement_confirmation_json
      - 'bank_confirm_ref'
      - 'provider_reference'
      - 'provider_request_id'
      - 'provider_submission_id'
      - 'provider_transaction_id'
      - 'rail_tx_id';

    v_settlement_confirmation_json := v_settlement_confirmation_json
      || JSONB_STRIP_NULLS(
        JSONB_BUILD_OBJECT(
          'settlement_mode', v_settlement_mode,
          'provisional_scope_settlement', false,
          'full_batch_finalised', true,
          'full_batch_finalised_at_utc', COALESCE(
            NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'full_batch_finalised_at_utc', '')), ''),
            v_now::text
          ),
          'confirmation_mode', CASE
            WHEN v_settlement_mode = 'CSV_SETTLEMENT' THEN 'NO_BANK_PAYMENT_REVIEW'
            WHEN v_settlement_mode = 'EXTERNAL_SETTLEMENT' THEN 'NO_BANK_PAYMENT_EXTERNAL_CONFIRMATION'
            ELSE 'NO_BANK_PAYMENT_EXECUTION'
          END,
          'scope_kind', 'NO_BANK_PAYMENT',
          'no_bank_payment_reason', 'EXPLICIT_ZERO_PAYE',
          'no_bank_payment_execution', true,
          'contains_no_bank_payment_scopes', true,
          'allow_explicit_zero_no_bank_scopes', true,
          'local_commit_reference', v_local_no_bank_commit_ref,
          'execution_commit_ref', v_local_no_bank_commit_ref,
          'execution_operation_id', v_execution_operation_id::text,
          'settlement_operation_id', CASE WHEN v_settlement_operation_id IS NULL THEN NULL::text ELSE v_settlement_operation_id::text END,
          'auth_request_id', v_operation_auth_request_id::text
        )
        || JSONB_BUILD_OBJECT(
          'global_paye_net_state_hash', v_expected_paye_net_state_hash,
          'paye_net_state_hash', v_expected_paye_net_state_hash,
          'scoped_paye_net_state_hash', v_expected_scoped_paye_net_state_hash,
          'global_bank_payment_projection_hash', v_expected_global_bank_payment_projection_hash,
          'scoped_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
          'bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
          'projection_changed_after_authorisation', v_current_projection_changed,
          'projection_scope', v_operation_projection_scope,
          'zero_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
          'positive_transfer_count', 0,
          'total_bank_out', 0,
          'pay_bank_transfer_ids', '[]'::jsonb,
          'settlement_scope_ids', COALESCE(v_settlement_scope_ids, '[]'::jsonb),
          'settlement_scope_id', CASE
            WHEN JSONB_ARRAY_LENGTH(COALESCE(v_settlement_scope_ids, '[]'::jsonb)) = 1 THEN v_settlement_scope_ids->>0
            ELSE NULL::text
          END
        )
        || JSONB_BUILD_OBJECT(
          'payment_date', CASE WHEN v_effective_payment_date IS NULL THEN NULL::text ELSE v_effective_payment_date::text END,
          'settled_at_utc', COALESCE(
            NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'settled_at_utc', '')), ''),
            v_now::text
          ),
          'settled_by_user_id', COALESCE(
            NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'settled_by_user_id', '')), ''),
            p_actor_user_id::text
          ),
          'csv_uploaded_confirmed', CASE
            WHEN v_settlement_mode = 'CSV_SETTLEMENT'
              THEN LOWER(BTRIM(COALESCE(v_operation_auth_intent_json->>'csv_uploaded_confirmed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
            ELSE NULL::boolean
          END,
          'external_comment', CASE
            WHEN v_settlement_mode = 'EXTERNAL_SETTLEMENT'
              THEN NULLIF(BTRIM(COALESCE(v_operation_auth_intent_json->>'external_settlement_comment', '')), '')
            ELSE NULL::text
          END,
          'submitted_to_bank', false,
          'provider_submission_required', false,
          'provider_submission_attempted', false,
          'local_settlement_evidence_only', true,
          'bank_upload_occurred', false,
          'bank_payment_occurred', false,
          'provider_submission_occurred', false
        )
        || JSONB_BUILD_OBJECT(
          'suppress_remittances', v_suppress_remittances,
          'remittances_suppressed_at_utc', CASE
            WHEN v_suppress_remittances THEN COALESCE(
              NULLIF(BTRIM(COALESCE(v_settlement_confirmation_json->>'remittances_suppressed_at_utc', '')), ''),
              v_now::text
            )
            ELSE NULL::text
          END,
          'no_bank_payment_note', CASE
            WHEN v_settlement_mode = 'CSV_SETTLEMENT'
              THEN 'The current zero-row CloudTMS Bank CSV was reviewed; no bank upload or payment occurred or was required.'
            WHEN v_settlement_mode = 'EXTERNAL_SETTLEMENT'
              THEN 'The authorised external no-bank settlement was completed locally; no bank payment or provider submission occurred or was required.'
            ELSE 'All authorised PAYE bank amounts were explicitly zero; no bank transfer or provider submission occurred or was required.'
          END
        )
      );
  END IF;

  UPDATE public.pay_batches AS pb2
  SET
    status = v_batch_status,
    completed_at_utc = CASE
      WHEN v_batch_status IN ('SETTLED', 'FAILED') THEN COALESCE(pb2.completed_at_utc, v_now)
      ELSE pb2.completed_at_utc
    END,
    last_status_checked_at_utc = v_now,
    total_bank_out = CASE
      WHEN v_batch_status = 'SETTLED' AND COALESCE(v_completed_transfer_count, 0) > 0
        THEN ROUND(COALESCE(v_completed_transfer_total, 0), 2)
      WHEN v_batch_status = 'SETTLED' AND v_no_bank_payment_execution_validated
        THEN 0
      ELSE pb2.total_bank_out
    END,
    execution_commit_state = CASE
      WHEN COALESCE(v_completed_transfer_count, 0) > 0 THEN 'COMMITTED'
      WHEN v_no_bank_payment_execution_validated AND v_batch_status = 'SETTLED' THEN 'COMMITTED'
      ELSE COALESCE(NULLIF(BTRIM(COALESCE(pb2.execution_commit_state, '')), ''), 'NOT_SUBMITTED')
    END,
    execution_commit_ref = CASE
      WHEN COALESCE(v_completed_transfer_count, 0) > 0
           AND v_settlement_mode IN ('CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
        THEN v_local_positive_commit_ref
      WHEN COALESCE(v_completed_transfer_count, 0) > 0 THEN COALESCE(
        NULLIF(BTRIM(COALESCE(pb2.execution_commit_ref, '')), ''),
        NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), ''),
        NULLIF(BTRIM(COALESCE(v_detected_execution_commit_ref, '')), '')
      )
      WHEN v_no_bank_payment_execution_validated AND v_batch_status = 'SETTLED' THEN COALESCE(
        NULLIF(BTRIM(COALESCE(pb2.execution_commit_ref, '')), ''),
        v_local_no_bank_commit_ref
      )
      ELSE pb2.execution_commit_ref
    END,
    execution_committed_at_utc = CASE
      WHEN COALESCE(v_completed_transfer_count, 0) > 0 THEN COALESCE(
        pb2.execution_committed_at_utc,
        v_execution_committed_at_utc,
        v_detected_execution_committed_at_utc,
        v_now
      )
      WHEN v_no_bank_payment_execution_validated AND v_batch_status = 'SETTLED' THEN COALESCE(
        pb2.execution_committed_at_utc,
        v_execution_committed_at_utc,
        v_now
      )
      ELSE pb2.execution_committed_at_utc
    END,
    settlement_confirmation_json = v_settlement_confirmation_json
  WHERE pb2.id = p_pay_batch_id;


  if jsonb_array_length(coalesce(v_durably_finalised_candidate_ids, '[]'::jsonb)) > 0 then
    update public.pay_batches as finalised_candidate_batch_update
    set settlement_confirmation_json = coalesce(finalised_candidate_batch_update.settlement_confirmation_json, '{}'::jsonb)
      || jsonb_build_object(
        'durably_finalised_candidate_ids', v_durably_finalised_candidate_ids
      )
    where finalised_candidate_batch_update.id = p_pay_batch_id;
  end if;

  IF v_no_bank_payment_execution_validated AND v_batch_status = 'SETTLED' THEN
    v_execution_commit_state := 'COMMITTED';
    v_execution_commit_ref := v_local_no_bank_commit_ref;
    v_execution_committed_at_utc := COALESCE(v_execution_committed_at_utc, v_now);
  ELSIF COALESCE(v_completed_transfer_count, 0) > 0 THEN
    v_execution_commit_state := 'COMMITTED';
    v_execution_commit_ref := COALESCE(v_local_positive_commit_ref, v_execution_commit_ref, v_detected_execution_commit_ref);
    v_execution_committed_at_utc := COALESCE(v_execution_committed_at_utc, v_detected_execution_committed_at_utc, v_now);
  END IF;

  select exists (
    select 1
    from public.pay_batch_candidates pbc_unsent
    where pbc_unsent.pay_batch_id = p_pay_batch_id
      and pbc_unsent.remittance_sent_at_utc is null
    limit 1
  )
  into v_catchup_needed;

  if v_suppress_remittances = true then
    v_comm_trigger_status := 'SUPPRESSED_BY_EXECUTION_INTENT';
    v_comm_error := null;
    v_comm_result := jsonb_build_object(
      'ok', true,
      'trigger_status', v_comm_trigger_status,
      'message_kind', case when upper(btrim(coalesce(v_batch.batch_kind_fixed,''))) = 'LOANS' then 'PAYOUT_NOTICE' else 'REMITTANCE' end,
      'catchup_send', false,
      'dispatch_required', false,
      'suppressed_by_execution_intent', true,
      'execution_mode', v_settlement_mode
    );
    v_suppression_audit_json := jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'batch_kind_fixed', upper(coalesce(v_batch.batch_kind_fixed,'')),
      'settlement_mode', v_settlement_mode,
      'suppress_remittances', true,
      'suppressed_at_utc', v_now::text,
      'auth_request_id', nullif(btrim(coalesce(v_execution_intent_json->>'auth_request_id', '')), '')
    );
    begin
      insert into public.audit_events(
        actor_user_id,
        object_type,
        object_id_text,
        action,
        before_json,
        after_json,
        reason
      )
      select
        p_actor_user_id,
        'pay_batch',
        p_pay_batch_id::text,
        'PAY_BATCH_SETTLEMENT_COMMUNICATION_SUPPRESSED',
        null::jsonb,
        v_suppression_audit_json,
        'suppressed_by_execution_intent'
      where not exists (
        select 1
        from public.audit_events as existing_suppression_audit
        where existing_suppression_audit.object_type = 'pay_batch'
          and existing_suppression_audit.object_id_text = p_pay_batch_id::text
          and existing_suppression_audit.action = 'PAY_BATCH_SETTLEMENT_COMMUNICATION_SUPPRESSED'
          and existing_suppression_audit.reason = 'suppressed_by_execution_intent'
      );
    exception when others then
      null;
    end;
  elsif v_catchup_needed then
    v_comm_trigger_status := 'REMITTANCE_READY_DEFERRED_TO_OPERATION';
    v_comm_error := null;
    v_comm_result := jsonb_build_object(
      'ok', true,
      'trigger_status', v_comm_trigger_status,
      'message_kind', case when upper(btrim(coalesce(v_batch.batch_kind_fixed,''))) = 'LOANS' then 'PAYOUT_NOTICE' else 'REMITTANCE' end,
      'catchup_send', false,
      'dispatch_required', true,
      'remittance_ready', true,
      'remittance_queued', false,
      'deferred_to_operation', true,
      'requires_remittance_operation', true,
      'operation_type', 'REMITTANCE_QUEUE',
      'source_rpc', 'pay_settle_rail',
      'message', 'Settlement is remittance-ready. Queueing is handled by the separate scalable remittance operation.'
    );

    begin
      insert into public.audit_events(
        actor_user_id,
        object_type,
        object_id_text,
        action,
        before_json,
        after_json,
        reason
      )
      select
        p_actor_user_id,
        'pay_batch',
        p_pay_batch_id::text,
        'PAY_BATCH_SETTLEMENT_REMITTANCE_DEFERRED_TO_OPERATION',
        null::jsonb,
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'batch_kind_fixed', upper(coalesce(v_batch.batch_kind_fixed,'')),
          'trigger_status', v_comm_trigger_status,
          'result', coalesce(v_comm_result, '{}'::jsonb)
        ),
        'settlement_remittance_deferred_to_operation'
      where not exists (
        select 1
        from public.audit_events as existing_deferred_audit
        where existing_deferred_audit.object_type = 'pay_batch'
          and existing_deferred_audit.object_id_text = p_pay_batch_id::text
          and existing_deferred_audit.action = 'PAY_BATCH_SETTLEMENT_REMITTANCE_DEFERRED_TO_OPERATION'
          and existing_deferred_audit.reason = 'settlement_remittance_deferred_to_operation'
      );
    exception when others then
      null;
    end;
  else
    v_comm_trigger_status := 'NO_CATCHUP_NEEDED';
    v_comm_error := null;
    v_comm_result := jsonb_build_object(
      'ok', true,
      'trigger_status', v_comm_trigger_status,
      'message_kind', case when upper(btrim(coalesce(v_batch.batch_kind_fixed,''))) = 'LOANS' then 'PAYOUT_NOTICE' else 'REMITTANCE' end,
      'catchup_send', false
    );
  end if;

  v_worker_communications := jsonb_build_object(
    'primary_trigger_stage', 'COMMIT',
    'catchup_attempted_at_settlement', (v_catchup_needed and v_suppress_remittances = false),
    'message_kind', coalesce(nullif(btrim(coalesce(v_comm_result->>'message_kind','')), ''), case when upper(btrim(coalesce(v_batch.batch_kind_fixed,''))) = 'LOANS' then 'PAYOUT_NOTICE' else 'REMITTANCE' end),
    'trigger_status', v_comm_trigger_status,
    'error', v_comm_error,
    'result', coalesce(v_comm_result, '{}'::jsonb),
    'remittance_queue_stage_result', coalesce(v_comm_result, '{}'::jsonb),
    'remittance_ready', (v_catchup_needed and v_suppress_remittances = false),
    'remittance_queued', false,
    'requires_remittance_operation', (v_catchup_needed and v_suppress_remittances = false),
    'execution_mode', v_settlement_mode,
    'suppress_remittances', v_suppress_remittances
  );

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_pending_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) in ('PENDING','PROCESSING','UNKNOWN');

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json,
        'failed_reason', pbt.failed_reason
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_failed_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) in ('FAILED','DECLINED','REJECTED','CANCELLED');

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json,
        'completed_at_utc', case when pbt.completed_at_utc is null then null else pbt.completed_at_utc::text end
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_returned_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) in ('RETURNED','REVERTED');

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_blocked_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'BLOCKED';


  for v_changed_channel_audit in
    with batch_item_portions as (
      select
        pbi.id as pay_batch_item_id,
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id as candidate_id,
        pbi.timesheet_id as timesheet_id,
        ts.booking_id as booking_id,
        pbi.pay_bank_transfer_id as pay_bank_transfer_id,
        pbi.item_type as item_type,
        pbi.segment_key as segment_key,
        pbi.source_ref as source_ref,
        pbi.description as description,
        upper(coalesce(nullif(btrim(coalesce(pbi.frozen_source_pay_method,'')), ''), '')) as source_pay_channel,
        upper(coalesce(nullif(btrim(coalesce(pbi.pay_channel,'')), ''), nullif(btrim(coalesce(pbi.frozen_target_pay_method,'')), ''), '')) as settled_pay_channel,
        case when pbi.frozen_component_key_type is null then null else pbi.frozen_component_key_type end as component_key_type,
        pbi.frozen_component_key_value as component_key_value,
        case when pbi.frozen_component_classification is null then null else pbi.frozen_component_classification::text end as component_classification,
        case when pbi.frozen_resolution_mode is null then null else pbi.frozen_resolution_mode::text end as resolution_mode,
        pbi.frozen_resolution_payload_json as resolution_payload_json,
        pbi.frozen_resolution_result_json as resolution_result_json,
        pbi.frozen_source_basis_json as frozen_source_basis_json,
        round(coalesce(pbi.frozen_source_amount, pbi.amount_ex_vat, 0), 2) as item_source_amount_ex_vat,
        round(coalesce(pbi.frozen_target_amount_ex_vat, pbi.amount_ex_vat, 0), 2) as item_settled_amount_ex_vat,
        pbi.amount_ex_vat as item_amount_ex_vat
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      left join public.timesheets ts
        on ts.timesheet_id = pbi.timesheet_id
      where pbc.pay_batch_id = p_pay_batch_id
        and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
        and pbi.timesheet_id is not null
        and pbi.item_type <> 'DEBT_CREATED'
        and coalesce(pbi.is_voided, false) = false
    ),
    portion_rows as (
      select
        bip.pay_batch_item_id,
        bip.pay_batch_candidate_id,
        bip.candidate_id,
        bip.timesheet_id,
        bip.booking_id,
        bip.pay_bank_transfer_id,
        bip.source_pay_channel,
        bip.settled_pay_channel,
        bip.component_key_type,
        bip.component_key_value,
        bip.component_classification,
        bip.resolution_mode,
        bip.resolution_payload_json,
        bip.resolution_result_json,
        bip.segment_key,
        bip.source_ref,
        bip.description,
        pbib.id as pay_batch_item_breakdown_id,
        coalesce(
          nullif(btrim(coalesce(pbib.line_kind,'')), ''),
          case
            when bip.item_type = 'SEGMENT_DELTA' then 'SEGMENT_BUCKET'
            when bip.item_type = 'EXPENSE_DELTA' then 'EXPENSE'
            when bip.item_type = 'MILEAGE_DELTA' then 'MILEAGE'
            when bip.item_type = 'ADJUSTMENT_DELTA' then 'ADJUSTMENT'
            when bip.item_type = 'OVERPAYMENT_RECOVERY' then 'OVERPAYMENT_RECOVERY'
            when bip.item_type = 'LOAN_REPAYMENT' then 'LOAN_REPAYMENT'
            when bip.item_type = 'MANUAL_DEBT_RECOVERY' then 'MANUAL_DEBT_RECOVERY'
            when bip.item_type = 'LOAN_PAYOUT' then 'LOAN_PAYOUT'
            when bip.item_type = 'MANUAL_CREDIT_PAYOUT' then 'MANUAL_CREDIT_PAYOUT'
            when bip.item_type = 'UNDERPAYMENT_PAYMENT' then 'UNDERPAYMENT_PAYMENT'
            else bip.item_type
          end
        ) as line_kind,
        nullif(btrim(coalesce(pbib.bucket_code, bip.frozen_source_basis_json ->> 'bucket_code', '')), '') as bucket_code,
        coalesce(
          nullif(btrim(coalesce(pbib.unit_name, '')), ''),
          nullif(btrim(coalesce(bip.description, '')), ''),
          case
            when bip.item_type = 'SEGMENT_DELTA' then 'Timesheet portion'
            when bip.item_type = 'EXPENSE_DELTA' then 'Expense'
            when bip.item_type = 'MILEAGE_DELTA' then 'Mileage'
            when bip.item_type = 'ADJUSTMENT_DELTA' then 'Adjustment'
            when bip.item_type = 'OVERPAYMENT_RECOVERY' then 'Overpayment recovery'
            when bip.item_type = 'LOAN_REPAYMENT' then 'Loan repayment'
            when bip.item_type = 'MANUAL_DEBT_RECOVERY' then 'Manual debt recovery'
            when bip.item_type = 'LOAN_PAYOUT' then 'Loan payout'
            when bip.item_type = 'MANUAL_CREDIT_PAYOUT' then 'Manual credit payout'
            when bip.item_type = 'UNDERPAYMENT_PAYMENT' then 'Underpayment payment'
            else 'Batch item'
          end
        ) as unit_name,
        case
          when pbib.units is not null then round(pbib.units, 4)
          when bip.frozen_source_basis_json ? 'source_units'
           and nullif(btrim(coalesce(bip.frozen_source_basis_json ->> 'source_units', '')), '') is not null
            then round((bip.frozen_source_basis_json ->> 'source_units')::numeric, 4)
          else null::numeric
        end as units,
        case
          when bip.frozen_source_basis_json ? 'source_rate'
           and nullif(btrim(coalesce(bip.frozen_source_basis_json ->> 'source_rate', '')), '') is not null
            then round((bip.frozen_source_basis_json ->> 'source_rate')::numeric, 6)
          else null::numeric
        end as source_rate,
        case
          when pbib.rate is not null then round(pbib.rate, 6)
          else null::numeric
        end as settled_rate,
        case
          when pbib.units is not null
           and bip.frozen_source_basis_json ? 'source_rate'
           and nullif(btrim(coalesce(bip.frozen_source_basis_json ->> 'source_rate', '')), '') is not null
            then round(pbib.units * round((bip.frozen_source_basis_json ->> 'source_rate')::numeric, 6), 2)
          else bip.item_source_amount_ex_vat
        end as source_amount_ex_vat,
        round(coalesce(pbib.amount_ex_vat, bip.item_settled_amount_ex_vat), 2) as settled_amount_ex_vat,
        md5(concat_ws('|',
          bip.pay_batch_item_id::text,
          coalesce(
            nullif(btrim(coalesce(pbib.line_kind,'')), ''),
            case
              when bip.item_type = 'SEGMENT_DELTA' then 'SEGMENT_BUCKET'
              when bip.item_type = 'EXPENSE_DELTA' then 'EXPENSE'
              when bip.item_type = 'MILEAGE_DELTA' then 'MILEAGE'
              when bip.item_type = 'ADJUSTMENT_DELTA' then 'ADJUSTMENT'
              when bip.item_type = 'OVERPAYMENT_RECOVERY' then 'OVERPAYMENT_RECOVERY'
              when bip.item_type = 'LOAN_REPAYMENT' then 'LOAN_REPAYMENT'
              when bip.item_type = 'MANUAL_DEBT_RECOVERY' then 'MANUAL_DEBT_RECOVERY'
              when bip.item_type = 'LOAN_PAYOUT' then 'LOAN_PAYOUT'
              when bip.item_type = 'MANUAL_CREDIT_PAYOUT' then 'MANUAL_CREDIT_PAYOUT'
              else bip.item_type
            end
          ),
          coalesce(nullif(btrim(coalesce(pbib.bucket_code, bip.frozen_source_basis_json ->> 'bucket_code', '')), ''), ''),
          coalesce(
            case
              when pbib.units is not null then round(pbib.units, 4)::text
              when bip.frozen_source_basis_json ? 'source_units'
               and nullif(btrim(coalesce(bip.frozen_source_basis_json ->> 'source_units', '')), '') is not null
                then round((bip.frozen_source_basis_json ->> 'source_units')::numeric, 4)::text
              else null::text
            end,
            ''
          ),
          coalesce(case when pbib.rate is not null then round(pbib.rate, 6)::text else null::text end, ''),
          round(coalesce(pbib.amount_ex_vat, bip.item_settled_amount_ex_vat), 2)::text
        )) as portion_identity_key
      from batch_item_portions bip
      left join public.pay_batch_item_breakdowns pbib
        on pbib.pay_batch_item_id = bip.pay_batch_item_id
    )
    select
      pr.pay_batch_item_id,
      pr.pay_batch_item_breakdown_id,
      pr.pay_batch_candidate_id,
      pr.candidate_id,
      pr.timesheet_id,
      pr.booking_id,
      pr.pay_bank_transfer_id,
      pr.source_pay_channel,
      pr.settled_pay_channel,
      pr.component_key_type,
      pr.component_key_value,
      pr.component_classification,
      pr.resolution_mode,
      pr.resolution_payload_json,
      pr.resolution_result_json,
      pr.segment_key,
      pr.source_ref,
      pr.description,
      pr.line_kind,
      pr.bucket_code,
      pr.unit_name,
      pr.units,
      pr.source_rate,
      pr.settled_rate,
      pr.source_amount_ex_vat,
      pr.settled_amount_ex_vat,
      pr.portion_identity_key
    from portion_rows pr
    where pr.source_pay_channel <> ''
      and pr.settled_pay_channel <> ''
      and pr.source_pay_channel <> pr.settled_pay_channel
    order by
      pr.timesheet_id,
      pr.pay_batch_item_id,
      pr.line_kind,
      pr.bucket_code nulls first,
      pr.unit_name,
      pr.settled_amount_ex_vat desc
  loop
    v_changed_channel_audit_after_json := jsonb_build_object(
      'event_kind', 'TIMESHEET_CHANNEL_CHANGE_PORTION',
      'audit_effective_state', 'SETTLED_ACTIVE',
      'channel_changed', true,
      'timesheet_id', v_changed_channel_audit.timesheet_id::text,
      'booking_id', case when v_changed_channel_audit.booking_id is null then null else v_changed_channel_audit.booking_id::text end,
      'candidate_id', case when v_changed_channel_audit.candidate_id is null then null else v_changed_channel_audit.candidate_id::text end,
      'pay_batch_id', p_pay_batch_id::text,
      'pay_batch_candidate_id', case when v_changed_channel_audit.pay_batch_candidate_id is null then null else v_changed_channel_audit.pay_batch_candidate_id::text end,
      'pay_batch_item_id', case when v_changed_channel_audit.pay_batch_item_id is null then null else v_changed_channel_audit.pay_batch_item_id::text end,
      'pay_batch_item_breakdown_id', case when v_changed_channel_audit.pay_batch_item_breakdown_id is null then null else v_changed_channel_audit.pay_batch_item_breakdown_id::text end,
      'portion_identity_key', v_changed_channel_audit.portion_identity_key,
      'source_pay_channel', v_changed_channel_audit.source_pay_channel,
      'settled_pay_channel', v_changed_channel_audit.settled_pay_channel,
      'component_key_type', v_changed_channel_audit.component_key_type,
      'component_key_value', v_changed_channel_audit.component_key_value,
      'component_classification', v_changed_channel_audit.component_classification,
      'line_kind', v_changed_channel_audit.line_kind,
      'bucket_code', v_changed_channel_audit.bucket_code,
      'unit_name', v_changed_channel_audit.unit_name,
      'units', case when v_changed_channel_audit.units is null then null else round(v_changed_channel_audit.units, 4) end,
      'source_rate', case when v_changed_channel_audit.source_rate is null then null else round(v_changed_channel_audit.source_rate, 6) end,
      'settled_rate', case when v_changed_channel_audit.settled_rate is null then null else round(v_changed_channel_audit.settled_rate, 6) end,
      'source_amount_ex_vat', round(coalesce(v_changed_channel_audit.source_amount_ex_vat, 0), 2),
      'settled_amount_ex_vat', round(coalesce(v_changed_channel_audit.settled_amount_ex_vat, 0), 2),
      'resolution_mode', v_changed_channel_audit.resolution_mode,
      'resolution_selection_kind', case
        when v_changed_channel_audit.resolution_mode = 'SUGGESTED_EQUIVALENT_BASIS' then 'SUGGESTED'
        when v_changed_channel_audit.resolution_mode in ('MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT') then 'MANUAL'
        else null
      end,
      'pay_bank_transfer_id', case when v_changed_channel_audit.pay_bank_transfer_id is null then null else v_changed_channel_audit.pay_bank_transfer_id::text end,
      'segment_key', v_changed_channel_audit.segment_key,
      'source_ref', v_changed_channel_audit.source_ref,
      'description', v_changed_channel_audit.description,
      'settled_at_utc', v_now
    );

    begin
      perform public._audit_insert(
        'timesheets',
        v_changed_channel_audit.timesheet_id::text,
        'TIMESHEET_CHANNEL_CHANGE_PORTION_SETTLED',
        null,
        v_changed_channel_audit_after_json,
        'rail_settlement',
        p_actor_user_id
      );
      v_changed_channel_settled_ct := v_changed_channel_settled_ct + 1;
    exception when others then
      null;
    end;
  end loop;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_SETTLE_RAIL:COUNTS',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'is_stale', v_is_stale,
        'stale_reasons', v_stale_reasons,
        'newly_settled_candidates_count', jsonb_array_length(coalesce(v_newly_settled_candidates,'[]'::jsonb)),
        'overpayment_patches_applied', v_overpay_patched_ct,
        'payment_advance_recovery_patches_applied', v_payment_advance_recovery_patched_ct,
        'manual_debt_recovery_patches_applied', v_manual_debt_patched_ct,
        'payout_cases_marked_paid', v_payout_cases_marked_paid_ct,
        'component_settlements_applied', v_component_settled_count,
        'component_settlement_amount', v_component_settled_amount,
        'component_id_missing_from_frozen_artifact_count', coalesce(v_component_unresolved_count, 0),
        'component_id_missing_from_frozen_artifact_amount', coalesce(v_component_unresolved_amount, 0),
        'component_id_missing_from_frozen_artifact', coalesce(v_component_unresolved_json, '[]'::jsonb),
        'component_reconciliation_checked_count', coalesce(v_component_reconciliation_checked_ct, 0),
        'component_reconciliation_mismatch_count', coalesce(v_component_reconciliation_bad_ct, 0),
        'component_reconciliation_mismatches', v_component_reconciliation_bad,
        'batch_status', v_batch_status,
        'execution_commit_state', case when coalesce(v_completed_transfer_count, 0) > 0 then 'COMMITTED' else v_execution_commit_state end,
        'execution_commit_ref', coalesce(v_execution_commit_ref, v_detected_execution_commit_ref),
        'execution_committed_at_utc', case when coalesce(v_execution_committed_at_utc, v_detected_execution_committed_at_utc) is null then null else coalesce(v_execution_committed_at_utc, v_detected_execution_committed_at_utc)::text end,
        'worker_communications', v_worker_communications,
        'bank_event_ingest_count', v_bank_event_ingest_count,
        'bank_event_ingest_results', v_bank_event_ingest_results,
        'active_snooze_report_count', COALESCE(v_active_snooze_report_count, 0),
        'active_snooze_report_sample', COALESCE(v_active_snooze_report_sample, '[]'::jsonb),
        'london_current_date', v_today_uk::text
      ),
      'pay_batches',
      p_pay_batch_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  v_live_signal_result := public.banking_pay_batch_signal_touch(
    p_pay_batch_id := p_pay_batch_id,
    p_change_reason := 'PAY_SETTLE_RAIL',
    p_change_source := 'pay_settle_rail',
    p_change_scope_json := jsonb_strip_nulls(jsonb_build_object(
      'execution_mode', v_settlement_mode,
      'batch_status', v_batch_status,
      'batch_status_label', v_batch_status_label,
      'completed_with_failed_payments', v_completed_with_failed_payments,
      'newly_settled_candidates', COALESCE(v_newly_settled_candidates, '[]'::jsonb),
      'completed_transfer_count', COALESCE(v_completed_transfer_count, 0),
      'no_bank_scope_authorised', v_no_bank_scope_authorised,
      'no_bank_payment_execution', v_no_bank_payment_execution_validated,
      'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_marker,
      'no_bank_scope_count', COALESCE(v_no_bank_total_scope_count, 0),
      'local_commit_reference', COALESCE(
        CASE WHEN v_no_bank_scope_authorised THEN v_local_no_bank_commit_ref ELSE NULL::text END,
        v_local_positive_commit_ref
      ),
      'consumed_carry_forward_count', COALESCE(v_consumed_carry_forward_count, 0),
      'component_id_missing_from_frozen_artifact_count', COALESCE(v_component_unresolved_count, 0),
      'bank_event_ingest_count', COALESCE(v_bank_event_ingest_count, 0)
    )),
    p_touch_payment_status := true,
    p_touch_correction_progress := false,
    p_touch_alerts := false,
    p_touch_overview := true
  );

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'batch_status', (select pb3.status from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'batch_status_label', v_batch_status_label,
    'completed_with_failed_payments', v_completed_with_failed_payments,
    'execution_commit_state', (select pb3.execution_commit_state from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'execution_commit_ref', (select pb3.execution_commit_ref from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'execution_committed_at_utc', (select case when pb3.execution_committed_at_utc is null then null else pb3.execution_committed_at_utc::text end from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'total_bank_out', (select pb3.total_bank_out from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'settlement_operation_id', CASE WHEN v_settlement_operation_id IS NULL THEN NULL::text ELSE v_settlement_operation_id::text END,
    'settlement_confirmation_json', (select pb3.settlement_confirmation_json from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'execution_mode', v_settlement_mode,
    'suppress_remittances', v_suppress_remittances,
    'newly_settled_candidates', v_newly_settled_candidates,
    'still_pending_transfers', v_pending_transfers,
    'failed_transfers', v_failed_transfers,
    'returned_transfers', v_returned_transfers,
    'blocked_transfers', v_blocked_transfers,
    'live_snooze_diagnostics', jsonb_build_object(
      'date_authority_available', v_today_uk IS NOT NULL,
      'date_authority_code', CASE WHEN v_today_uk IS NULL THEN COALESCE(v_date_context->>'code', 'PAY_SETTLE_RAIL_LONDON_DATE_UNAVAILABLE') ELSE 'OK' END,
      'active_as_of_london_date', CASE WHEN v_today_uk IS NULL THEN NULL ELSE v_today_uk::text END,
      'active_snooze_report_count', COALESCE(v_active_snooze_report_count, 0),
      'active_snooze_report_sample', COALESCE(v_active_snooze_report_sample, '[]'::jsonb),
      'reporting_only', true,
      'frozen_settlement_authority_unchanged', true
    ),
    'freshness', jsonb_build_object(
      'is_stale', v_is_stale,
      'stale_reasons', v_stale_reasons,
      'diff_sample', v_diff_sample,
      'freshness_validation_status', nullif(v_stored_freshness_status, ''),
      'freshness_result_hash', v_stored_freshness_result_hash,
      'freshness_scope_hash', v_stored_freshness_scope_hash,
      'freshness_operation_id', case when v_stored_freshness_operation_id is null then null else v_stored_freshness_operation_id::text end,
      'source', coalesce(v_fresh->>'source', 'stored_freshness_metadata_non_blocking')
    ),
    'patch_summary', jsonb_build_object(
      'overpayment_patches_applied', v_overpay_patched_ct,
      'payment_advance_recovery_patches_applied', v_payment_advance_recovery_patched_ct,
      'manual_debt_recovery_patches_applied', v_manual_debt_patched_ct,
      'payout_cases_marked_paid', v_payout_cases_marked_paid_ct,
      'component_settlements_applied', v_component_settled_count,
      'component_settlement_amount', v_component_settled_amount,
      'component_id_missing_from_frozen_artifact_count', coalesce(v_component_unresolved_count, 0),
      'component_id_missing_from_frozen_artifact_amount', coalesce(v_component_unresolved_amount, 0),
      'component_id_missing_from_frozen_artifact', coalesce(v_component_unresolved_json, '[]'::jsonb),
      'component_reconciliation_checked_count', coalesce(v_component_reconciliation_checked_ct, 0),
      'component_reconciliation_mismatch_count', coalesce(v_component_reconciliation_bad_ct, 0)
    ),
    'timesheet_channel_change_audit', jsonb_build_object(
      'settled_event_count', v_changed_channel_settled_ct
    ),
    'worker_communications', v_worker_communications,
    'consumed_carry_forward_count', COALESCE(v_consumed_carry_forward_count, 0),
    'carry_forward_mark_consumed_result', COALESCE(v_carry_forward_mark_result, '{}'::jsonb),
    'remittance_ready', (v_catchup_needed and v_suppress_remittances = false),
    'remittance_queued', false,
    'remittance_queue_stage_result', coalesce(v_comm_result, '{}'::jsonb),
    'bank_event_ingest', jsonb_build_object(
      'count', v_bank_event_ingest_count,
      'results', v_bank_event_ingest_results
    ),
    'live_signal', COALESCE(v_live_signal_result, '{}'::jsonb),
    'policy_x_checked', true
  ) || jsonb_strip_nulls(
    jsonb_build_object(
      'proof_validation_outcome', v_proof_validation_outcome,
      'auth_request_id', case when v_operation_auth_request_id is null then null else v_operation_auth_request_id::text end,
      'execution_operation_id', case when v_execution_operation_id is null then null else v_execution_operation_id::text end,
      'settlement_operation_id', CASE
        WHEN v_settlement_operation_id IS NULL THEN NULL::text
        ELSE v_settlement_operation_id::text
      END,
      'authorised_execution_mode', v_authorised_execution_mode,
      'authorised_projection_scope', v_operation_projection_scope,
      'authorised_paye_net_state_hash', v_expected_paye_net_state_hash,
      'current_paye_net_state_hash', v_current_paye_net_state_hash,
      'authorised_global_bank_payment_projection_hash', v_expected_global_bank_payment_projection_hash,
      'current_global_bank_payment_projection_hash', v_all_bank_payment_projection_hash,
      'authorised_scoped_paye_net_state_hash', v_expected_scoped_paye_net_state_hash,
      'current_scoped_paye_net_state_hash', v_current_scoped_paye_net_state_hash,
      'authorised_bank_payment_projection_hash', v_expected_bank_payment_projection_hash,
      'current_bank_payment_projection_hash', v_current_bank_payment_projection_hash,
      'projection_changed_after_authorisation', v_current_projection_changed,
      'projection_diagnostic', v_projection_diagnostic_json,
      'authorised_missing_explicit_paye_input_count', v_expected_missing_count,
      'current_missing_explicit_paye_input_count', v_current_missing_count,
      'authorised_scoped_explicit_zero_count', v_expected_zero_count,
      'current_scoped_explicit_zero_count', v_current_zero_count,
      'authorised_scoped_positive_bank_payment_count', v_expected_positive_count,
      'current_scoped_positive_bank_payment_count', v_current_positive_count,
      'completed_transfer_count', coalesce(v_completed_transfer_count, 0),
      'no_bank_scope_authorised', v_no_bank_scope_authorised,
      'no_bank_payment_execution', v_no_bank_payment_execution_validated,
      'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes_marker,
      'no_bank_scope_count', case when v_no_bank_scope_authorised then coalesce(v_no_bank_total_scope_count, 0) else null::integer end,
      'no_bank_scope_reference', case when v_no_bank_scope_authorised then v_local_no_bank_commit_ref else null::text end,
      'local_commit_reference', case when v_no_bank_payment_execution_validated then v_local_no_bank_commit_ref else null::text end,
      'no_bank_invalid_scope_count', case when v_no_bank_scope_authorised then coalesce(v_no_bank_invalid_scope_count, 0) else null::integer end,
      'no_bank_missing_scope_count', case when v_no_bank_scope_authorised then coalesce(v_no_bank_missing_scope_count, 0) else null::integer end,
      'no_bank_nonterminal_scope_count', case when v_no_bank_scope_authorised then coalesce(v_no_bank_nonterminal_scope_count, 0) else null::integer end,
      'no_bank_failed_scope_count', case when v_no_bank_scope_authorised then coalesce(v_no_bank_failed_scope_count, 0) else null::integer end,
      'no_bank_transfer_scope_count', case when v_no_bank_payment_execution_validated then coalesce(v_no_bank_scope_artifact_count, 0) else null::integer end,
      'no_bank_transfer_count', case when v_no_bank_payment_execution_validated then coalesce(v_no_bank_batch_transfer_count, 0) else null::integer end,
      'no_bank_transfer_event_count', case when v_no_bank_payment_execution_validated then coalesce(v_no_bank_transfer_event_count, 0) else null::integer end,
      'no_bank_provider_attempt_count', case when v_no_bank_payment_execution_validated then coalesce(v_no_bank_provider_attempt_count, 0) else null::integer end
    )
  );
end;
$function$;

ALTER FUNCTION pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb) TO anon;
GRANT EXECUTE ON FUNCTION pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb) TO service_role;
