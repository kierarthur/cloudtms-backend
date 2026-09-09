-- Source-less unpaid cancellation admission: keep restoration truth but admit only complete proved-unpaid scopes.
-- Generated from exact current owners by scripts/generate-banking-pay-unpaid-cancellation-sourceless-v1.mjs.
-- Financial, tax, VAT, payment-channel, provider, settlement and remittance policy remain unchanged.

\set ON_ERROR_STOP on

begin;

-- Final activation is deliberately a zero-active cutover. The request lock is
-- acquired first, matching current request-start order (request, then operation),
-- and both locks are held through the function replacements and COMMIT.
LOCK TABLE public.pay_payment_correction_requests IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.banking_pay_operations IN SHARE ROW EXCLUSIVE MODE;

DO $payment_correction_zero_active_cutover$
DECLARE
  v_nonterminal_request_count integer := 0;
  v_nonterminal_operation_count integer := 0;
BEGIN
  -- Exact request terminal vocabulary from pay_payment_correction_requests_status_chk.
  -- NULL, blank, unexpected, or every admitted nonterminal status fails closed.
  SELECT pg_catalog.count(*)::integer
  INTO v_nonterminal_request_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(request_row.status)) NOT IN (
       'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
       'FAILED', 'REJECTED', 'CANCELLED'
     );

  -- Exact operation terminal vocabulary from the current active-idempotency
  -- authority. REVIEW_REQUIRED is terminal/non-runnable, but remains audit data.
  SELECT pg_catalog.count(*)::integer
  INTO v_nonterminal_operation_count
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND (
      operation_row.status IS NULL
      OR pg_catalog.upper(pg_catalog.btrim(operation_row.status)) NOT IN (
        'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'
      )
    );

  IF v_nonterminal_request_count <> 0 OR v_nonterminal_operation_count <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED',
              'nonterminal_request_count', v_nonterminal_request_count,
              'nonterminal_operation_count', v_nonterminal_operation_count
            )::text;
  END IF;
END;
$payment_correction_zero_active_cutover$;

CREATE OR REPLACE FUNCTION public.pay_payment_cancelability_diagnostic(
  p_pay_batch_id uuid,
  p_selection_json jsonb DEFAULT '{}'::jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_diagnostic_context text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_batch public.pay_batches%ROWTYPE;
  v_selection_json jsonb := '{}'::jsonb;
  v_resolved_scope_json jsonb := '{}'::jsonb;
  v_scope_type text := NULL::text;
  v_scope_is_full boolean := true;
  v_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_provider_evidence_class text := 'NO_PROVIDER_EVIDENCE';
  v_provider_submitted boolean := false;
  v_provider_request_sent boolean := false;
  v_provider_response_present boolean := false;
  v_provider_event_present boolean := false;
  v_provider_external_id_present boolean := false;
  v_local_prepared_only boolean := true;
  v_provider_cash_state text := NULL::text;
  v_provider_blocker_code text := NULL::text;
  v_provider_reason text := NULL::text;
  v_provider_support_details_json jsonb := '{}'::jsonb;
  v_transfer_count integer := 0;
  v_transfer_final_count integer := 0;
  v_transfer_terminal_no_money_count integer := 0;
  v_transfer_pending_count integer := 0;
  v_event_count integer := 0;
  v_event_final_count integer := 0;
  v_event_terminal_no_money_count integer := 0;
  v_event_pending_count integer := 0;
  v_event_unknown_count integer := 0;
  v_provider_webhook_evidence_count integer := 0;
  v_invalid_webhook_evidence_count integer := 0;
  v_failed_webhook_replay_evidence_count integer := 0;
  v_provider_poll_evidence_count integer := 0;
  v_provider_outage_count integer := 0;
  v_pre_bank_applied_count integer := 0;
  v_no_money_applied_count integer := 0;
  v_open_provider_submit_count integer := 0;
  v_manual_adjustment_result jsonb := '{}'::jsonb;
  v_freshness_result jsonb := '{}'::jsonb;
  v_manual_carry_forward_required boolean := false;
  v_can_carry_forward_automatically boolean := true;
  v_manual_carry_forward_blocker_count integer := 0;
  v_source_less_ambiguous_count integer := 0;
  v_source_restoration_investigation_required boolean := false;
  v_freshness_blocker_count integer := 0;
  v_partial_scope_blocker_count integer := 0;
  v_final_evidence_count integer := 0;
  v_terminal_no_money_count integer := 0;
  v_pending_provider_count integer := 0;
  v_unknown_provider_count integer := 0;
  v_has_paid_or_settled boolean := false;
  v_has_provider_unknown boolean := false;
  v_has_provider_pending boolean := false;
  v_has_provider_outage boolean := false;
  v_has_terminal_no_money boolean := false;
  v_is_local_not_sent boolean := false;
  v_lifecycle_state text := 'PROVIDER_OUTCOME_UNKNOWN';
  v_lifecycle_label text := 'Provider outcome unknown — check provider';
  v_recommended_action text := 'CHECK_PROVIDER_STATUS';
  v_can_pre_provider_cancel boolean := false;
  v_can_no_money_unwind boolean := false;
  v_can_recover_overpayment boolean := false;
  v_requires_provider_cancel boolean := false;
  v_requires_bank_check boolean := false;
  v_requires_retry_later boolean := false;
  v_whole_batch_cancel_available boolean := false;
  v_row_cancel_available boolean := false;
  v_retry_same_batch_safe boolean := false;
  v_blockers jsonb := '[]'::jsonb;
  v_warnings jsonb := '[]'::jsonb;
  v_race_or_submission_blockers jsonb := '[]'::jsonb;
  v_provider_evidence_summary_json jsonb := '{}'::jsonb;
  v_blocking_paid_evidence_json jsonb := '{}'::jsonb;
  v_terminal_no_money_evidence_json jsonb := '{}'::jsonb;
  v_pending_provider_evidence_json jsonb := '{}'::jsonb;
  v_provider_outcome_unknown_evidence_json jsonb := '{}'::jsonb;
  v_provider_webhook_evidence_json jsonb := '{}'::jsonb;
  v_finance_scope_json jsonb := '{}'::jsonb;
  v_support_details_json jsonb := '{}'::jsonb;
  v_provider_failure_reason_code text := NULL::text;
  v_provider_failure_reason_group text := NULL::text;
  v_provider_failure_reason_label text := NULL::text;
  v_provider_failure_reason_result jsonb := '{}'::jsonb;
  v_failure_event_provider_key text := NULL::text;
  v_failure_event_state text := NULL::text;
  v_failure_event_reason_code text := NULL::text;
  v_failure_event_reason_text text := NULL::text;
  v_failure_event_payload jsonb := '{}'::jsonb;
  v_alert_candidate_kind text := NULL::text;
  v_alert_candidate_severity text := NULL::text;
  v_alert_candidate_is_success_only boolean := false;
  v_paid_recovery_required boolean := false;
  v_has_actual_recovery_context boolean := false;
  v_status_update_signature text := NULL::text;
  v_result jsonb := '{}'::jsonb;
  v_actor_valid boolean := true;
  v_batch_terminal boolean := false;
  v_diagnostic_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_diagnostic_context, '')), ''), '-', '_'));
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_BATCH_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_BATCH_REQUIRED')::text;
  END IF;


  IF v_diagnostic_context IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_CONTEXT_REQUIRED',
              'message', 'pay_payment_cancelability_diagnostic requires an explicit diagnostic/action context.'
            )::text;
  END IF;

  IF v_diagnostic_context IN (
    'LIST',
    'BATCH_LIST',
    'PAY_BATCHES_LIST',
    'PAY_BATCH_GET',
    'PAY_BATCH_GET_BOOTSTRAP_ONLY',
    'BOOTSTRAP',
    'BATCH_BOOTSTRAP',
    'OVERVIEW',
    'BATCH_OVERVIEW',
    'PREVIEW_OPEN',
    'PREVIEW_PROGRESS',
    'WORKBENCH_OPEN',
    'WORKBENCH_PROGRESS',
    'OPERATION_GET',
    'OPERATION_WORKER',
    'OPERATION_WORKER_PROOF',
    'OPERATION_PROGRESS',
    'PROGRESS_POLLING',
    'LIVE_WATCH',
    'WATCH_SIGNAL',
    'RPC_CHANGES_PING'
  ) THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_CONTEXT_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_CONTEXT_NOT_ALLOWED',
              'context', v_diagnostic_context,
              'message', 'Cancelability diagnostics are blocked from list, bootstrap, Overview, preview, operation progress, operation GET, and worker proof paths.'
            )::text;
  END IF;

  IF v_diagnostic_context NOT IN (
    'CURRENT_PAYMENT_STATUS',
    'CURRENT_PAYMENT_STATUS_TAB',
    'PAYMENT_STATUS_TAB',
    'PAYMENT_ISSUES_TAB',
    'PAYMENT_ISSUE_REVIEW',
    'CANCELLATION_ACTION',
    'CANCEL_PAYMENT_ACTION',
    'CANCEL_WHOLE_BATCH_ACTION',
    'CORRECTION_REVIEW',
    'CORRECTION_ACTION',
    'PAYMENT_CORRECTION_PLAN',
    'USER_TRIGGERED_DIAGNOSTIC',
    'EXPLICIT_DIAGNOSTIC'
  ) THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_CONTEXT_NOT_EXPLICIT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_CONTEXT_NOT_EXPLICIT',
              'context', v_diagnostic_context,
              'message', 'Cancelability diagnostics are only allowed from explicit Current Payment Status, cancellation, correction, or user-triggered diagnostic flows.'
            )::text;
  END IF;

  IF p_selection_json IS NOT NULL AND COALESCE(jsonb_typeof(p_selection_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_SELECTION_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_SELECTION_MUST_BE_OBJECT')::text;
  END IF;

  v_selection_json := COALESCE(p_selection_json, '{}'::jsonb);
  v_paid_recovery_required := (
    lower(NULLIF(btrim(COALESCE(v_selection_json->>'recovery_required', '')), '')) IN ('true','t','yes','y','1','on')
    OR lower(NULLIF(btrim(COALESCE(v_selection_json->>'paid_recovery_required', '')), '')) IN ('true','t','yes','y','1','on')
    OR lower(NULLIF(btrim(COALESCE(v_selection_json->>'payment_recovery_required', '')), '')) IN ('true','t','yes','y','1','on')
    OR upper(NULLIF(btrim(COALESCE(v_selection_json->>'requested_action', '')), '')) = 'AMEND_AND_RECOVER_OVERPAYMENT'
    OR upper(NULLIF(btrim(COALESCE(v_selection_json->>'source_context', '')), '')) = 'PAID_RECOVERY_REQUIRED'
  );

  SELECT pay_batch_row.*
  INTO v_batch
  FROM public.pay_batches AS pay_batch_row
  WHERE pay_batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  v_batch_terminal := upper(btrim(COALESCE(v_batch.status, ''))) IN (
    'COMMITTED', 'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'
  );

  IF p_actor_user_id IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.tms_users AS actor_user
      WHERE actor_user.id = p_actor_user_id
        AND COALESCE(actor_user.is_active, false) = true
    )
    INTO v_actor_valid;

    IF COALESCE(v_actor_valid, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_ACTOR_NOT_ALLOWED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CANCELABILITY_DIAGNOSTIC_ACTOR_NOT_ALLOWED',
                'actor_user_id', p_actor_user_id::text
              )::text;
    END IF;
  END IF;

  v_resolved_scope_json := public._pay_resolve_payment_scope_for_cancel_rewind(
    p_pay_batch_id,
    v_selection_json,
    false,
    p_actor_user_id
  );

  v_scope_type := COALESCE(NULLIF(BTRIM(v_resolved_scope_json ->> 'scope_type'), ''), 'BATCH');
  v_scope_is_full := COALESCE((v_resolved_scope_json ->> 'is_full_scope')::boolean, true);

  SELECT COALESCE(array_agg(parsed_values.value_uuid ORDER BY parsed_values.value_uuid), ARRAY[]::uuid[])
  INTO v_pay_bank_transfer_ids
  FROM (
    SELECT DISTINCT raw_values.raw_value::uuid AS value_uuid
    FROM jsonb_array_elements_text(COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb)) AS raw_values(raw_value)
    WHERE raw_values.raw_value ~ v_uuid_regex
  ) AS parsed_values;

  SELECT COALESCE(array_agg(parsed_values.value_uuid ORDER BY parsed_values.value_uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM (
    SELECT DISTINCT raw_values.raw_value::uuid AS value_uuid
    FROM jsonb_array_elements_text(COALESCE(v_resolved_scope_json -> 'pay_batch_item_ids', '[]'::jsonb)) AS raw_values(raw_value)
    WHERE raw_values.raw_value ~ v_uuid_regex
  ) AS parsed_values;

  SELECT COALESCE(array_agg(parsed_values.value_uuid ORDER BY parsed_values.value_uuid), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM (
    SELECT DISTINCT raw_values.raw_value::uuid AS value_uuid
    FROM jsonb_array_elements_text(COALESCE(v_resolved_scope_json -> 'candidate_ids', '[]'::jsonb)) AS raw_values(raw_value)
    WHERE raw_values.raw_value ~ v_uuid_regex
  ) AS parsed_values;

  SELECT COALESCE(array_agg(parsed_values.value_uuid ORDER BY parsed_values.value_uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_candidate_ids
  FROM (
    SELECT DISTINCT raw_values.raw_value::uuid AS value_uuid
    FROM jsonb_array_elements_text(COALESCE(v_resolved_scope_json -> 'pay_batch_candidate_ids', '[]'::jsonb)) AS raw_values(raw_value)
    WHERE raw_values.raw_value ~ v_uuid_regex
  ) AS parsed_values;

  SELECT provider_evidence.evidence_class,
         provider_evidence.provider_submitted,
         provider_evidence.provider_request_sent,
         provider_evidence.provider_response_present,
         provider_evidence.provider_event_present,
         provider_evidence.provider_external_id_present,
         provider_evidence.local_prepared_only,
         provider_evidence.cash_state,
         provider_evidence.blocker_code,
         provider_evidence.reason,
         provider_evidence.support_details_json
  INTO v_provider_evidence_class,
       v_provider_submitted,
       v_provider_request_sent,
       v_provider_response_present,
       v_provider_event_present,
       v_provider_external_id_present,
       v_local_prepared_only,
       v_provider_cash_state,
       v_provider_blocker_code,
       v_provider_reason,
       v_provider_support_details_json
  FROM public._pay_bank_transfer_provider_evidence_classify(
    p_pay_batch_id,
    NULL::uuid,
    v_resolved_scope_json,
    NULL::uuid
  ) AS provider_evidence
  LIMIT 1;

  v_provider_evidence_class := COALESCE(v_provider_evidence_class, 'NO_PROVIDER_EVIDENCE');
  v_provider_submitted := COALESCE(v_provider_submitted, false);
  v_provider_request_sent := COALESCE(v_provider_request_sent, false);
  v_provider_response_present := COALESCE(v_provider_response_present, false);
  v_provider_event_present := COALESCE(v_provider_event_present, false);
  v_provider_external_id_present := COALESCE(v_provider_external_id_present, false);
  v_local_prepared_only := COALESCE(v_local_prepared_only, true);
  v_provider_support_details_json := COALESCE(v_provider_support_details_json, '{}'::jsonb);

  v_manual_adjustment_result := public._pay_detect_manual_adjustments_for_carry_forward(
    p_pay_batch_id,
    v_resolved_scope_json,
    p_actor_user_id
  );

  v_freshness_result := public._pay_manual_adjustment_carry_forward_freshness_check(
    p_pay_batch_id,
    v_candidate_ids,
    v_pay_batch_item_ids,
    v_resolved_scope_json,
    p_actor_user_id
  );

  v_manual_carry_forward_required := COALESCE((v_manual_adjustment_result ->> 'manual_adjustment_carry_forward_required')::boolean, false);
  v_can_carry_forward_automatically := COALESCE((v_manual_adjustment_result ->> 'can_carry_forward_automatically')::boolean, true);
  v_manual_carry_forward_blocker_count := COALESCE(jsonb_array_length(COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb)), 0);
  v_source_less_ambiguous_count := COALESCE(NULLIF(v_manual_adjustment_result ->> 'source_less_ambiguous_count', '')::integer, 0);
  v_freshness_blocker_count := COALESCE(jsonb_array_length(COALESCE(v_freshness_result -> 'blockers', '[]'::jsonb)), 0);
  v_partial_scope_blocker_count := COALESCE(jsonb_array_length(COALESCE(v_resolved_scope_json -> 'partial_scope_blockers', '[]'::jsonb)), 0);

  WITH scoped_transfers AS (
    SELECT transfer_rows.*
    FROM public.pay_bank_transfers AS transfer_rows
    WHERE transfer_rows.pay_batch_id = p_pay_batch_id
      AND (
        COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0
        OR transfer_rows.id = ANY(v_pay_bank_transfer_ids)
      )
  ), classified_transfers AS (
    SELECT
      scoped_transfers.id AS pay_bank_transfer_id,
      movement_rows.cash_state,
      movement_rows.is_final_money_moved,
      movement_rows.is_terminal_no_money,
      movement_rows.is_pending_non_final
    FROM scoped_transfers
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      scoped_transfers.status,
      scoped_transfers.rail_state,
      COALESCE(scoped_transfers.rail_meta_json, '{}'::jsonb),
      jsonb_build_object(
        'provider_key', scoped_transfers.rail_provider,
        'rail_env', scoped_transfers.rail_env,
        'request_id', scoped_transfers.request_id,
        'rail_tx_id', scoped_transfers.rail_tx_id
      )
    ) AS movement_rows
  )
  SELECT
    count(*)::integer,
    count(*) FILTER (WHERE classified_transfers.is_final_money_moved)::integer,
    count(*) FILTER (WHERE classified_transfers.is_terminal_no_money)::integer,
    count(*) FILTER (WHERE classified_transfers.is_pending_non_final)::integer
  INTO v_transfer_count,
       v_transfer_final_count,
       v_transfer_terminal_no_money_count,
       v_transfer_pending_count
  FROM classified_transfers;

  WITH scoped_events AS (
    SELECT
      event_rows.*,
      receipt_rows.status AS receipt_status,
      receipt_rows.provider_key AS receipt_provider_key,
      receipt_rows.rail_env AS receipt_rail_env,
      receipt_rows.provider_event_key AS receipt_provider_event_key,
      receipt_rows.signature_valid AS receipt_signature_valid
    FROM public.pay_bank_transfer_events AS event_rows
    LEFT JOIN public.bank_provider_webhook_receipts AS receipt_rows
      ON receipt_rows.id = event_rows.provider_webhook_receipt_id
    WHERE event_rows.pay_batch_id = p_pay_batch_id
      AND (
        COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0
        OR event_rows.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
        OR event_rows.pay_bank_transfer_id IS NULL
      )
  ), classified_events AS (
    SELECT
      scoped_events.id AS event_id,
      scoped_events.provider_event_transport,
      scoped_events.provider_signature_valid,
      scoped_events.provider_webhook_receipt_id,
      scoped_events.receipt_status,
      scoped_events.receipt_provider_key,
      scoped_events.receipt_rail_env,
      scoped_events.receipt_provider_event_key,
      scoped_events.receipt_signature_valid,
      scoped_events.provider_failure_reason_group,
      scoped_events.mapping_status,
      scoped_events.normalised_state,
      scoped_events.provider_state,
      movement_rows.cash_state,
      movement_rows.is_final_money_moved,
      movement_rows.is_terminal_no_money,
      movement_rows.is_pending_non_final
    FROM scoped_events
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      scoped_events.normalised_state,
      scoped_events.provider_state,
      COALESCE(scoped_events.raw_payload, '{}'::jsonb),
      jsonb_build_object(
        'provider_key', scoped_events.provider_key,
        'provider_event_type', scoped_events.provider_event_type,
        'provider_event_transport', scoped_events.provider_event_transport,
        'provider_event_key', scoped_events.provider_event_key
      )
    ) AS movement_rows
  )
  SELECT
    count(*)::integer,
    count(*) FILTER (WHERE classified_events.is_final_money_moved)::integer,
    count(*) FILTER (WHERE classified_events.is_terminal_no_money)::integer,
    count(*) FILTER (WHERE classified_events.is_pending_non_final)::integer,
    count(*) FILTER (
      WHERE (
        upper(COALESCE(classified_events.normalised_state, '')) IN ('UNKNOWN', 'OUTCOME_UNKNOWN', 'PROVIDER_UNKNOWN')
        OR upper(COALESCE(classified_events.mapping_status, '')) IN ('UNMATCHED', 'AMBIGUOUS', 'REVIEW_REQUIRED')
        OR upper(COALESCE(classified_events.provider_failure_reason_group, '')) IN ('WEBHOOK_UNMATCHED', 'PROVIDER_UNKNOWN')
      )
      AND upper(COALESCE(classified_events.provider_failure_reason_group, '')) <> 'PROVIDER_OUTAGE'
    )::integer,
    count(*) FILTER (
      WHERE classified_events.provider_event_transport = 'PROVIDER_WEBHOOK'
        AND classified_events.provider_signature_valid IS DISTINCT FROM false
        AND classified_events.provider_webhook_receipt_id IS NOT NULL
        AND classified_events.receipt_signature_valid IS TRUE
        AND upper(COALESCE(classified_events.receipt_status, '')) IN ('VERIFIED', 'NORMALISED', 'NORMALIZED', 'INGESTED', 'FAILED_RETRYABLE', 'UNMATCHED_REVIEW_REQUIRED')
        AND classified_events.receipt_provider_key IS NOT DISTINCT FROM v_batch.rail_provider_snapshot
        AND classified_events.receipt_rail_env IS NOT DISTINCT FROM COALESCE(v_batch.rail_env_snapshot, 'PROD')
        AND NULLIF(btrim(COALESCE(classified_events.receipt_provider_event_key, '')), '') IS NOT NULL
    )::integer,
    count(*) FILTER (
      WHERE classified_events.provider_event_transport = 'PROVIDER_WEBHOOK'
        AND (
          classified_events.provider_signature_valid IS FALSE
          OR classified_events.provider_webhook_receipt_id IS NULL
          OR classified_events.receipt_signature_valid IS DISTINCT FROM true
          OR upper(COALESCE(classified_events.receipt_status, '')) IN ('SIGNATURE_INVALID', 'FAILED_FINAL')
          OR classified_events.receipt_provider_key IS DISTINCT FROM v_batch.rail_provider_snapshot
          OR classified_events.receipt_rail_env IS DISTINCT FROM COALESCE(v_batch.rail_env_snapshot, 'PROD')
          OR NULLIF(btrim(COALESCE(classified_events.receipt_provider_event_key, '')), '') IS NULL
        )
    )::integer,
    count(*) FILTER (
      WHERE classified_events.provider_event_transport = 'FAILED_WEBHOOK_REPLAY'
        AND classified_events.provider_webhook_receipt_id IS NOT NULL
        AND upper(COALESCE(classified_events.receipt_status, '')) NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL')
        AND classified_events.receipt_provider_key IS NOT DISTINCT FROM v_batch.rail_provider_snapshot
        AND classified_events.receipt_rail_env IS NOT DISTINCT FROM COALESCE(v_batch.rail_env_snapshot, 'PROD')
        AND NULLIF(btrim(COALESCE(classified_events.receipt_provider_event_key, '')), '') IS NOT NULL
    )::integer,
    count(*) FILTER (WHERE classified_events.provider_event_transport = 'PROVIDER_POLL')::integer,
    count(*) FILTER (
      WHERE upper(COALESCE(classified_events.provider_failure_reason_group, '')) = 'PROVIDER_OUTAGE'
        AND upper(COALESCE(classified_events.mapping_status, '')) = 'MATCHED'
    )::integer
  INTO v_event_count,
       v_event_final_count,
       v_event_terminal_no_money_count,
       v_event_pending_count,
       v_event_unknown_count,
       v_provider_webhook_evidence_count,
       v_invalid_webhook_evidence_count,
       v_failed_webhook_replay_evidence_count,
       v_provider_poll_evidence_count,
       v_provider_outage_count
  FROM classified_events;

  SELECT event_rows.provider_key,
         COALESCE(NULLIF(BTRIM(event_rows.provider_state), ''), NULLIF(BTRIM(event_rows.normalised_state), '')),
         event_rows.provider_failure_reason_code,
         event_rows.provider_failure_reason_group,
         COALESCE(event_rows.raw_payload, '{}'::jsonb)
  INTO v_failure_event_provider_key,
       v_failure_event_state,
       v_failure_event_reason_code,
       v_provider_failure_reason_group,
       v_failure_event_payload
  FROM public.pay_bank_transfer_events AS event_rows
  CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
    event_rows.normalised_state,
    event_rows.provider_state,
    COALESCE(event_rows.raw_payload, '{}'::jsonb),
    jsonb_build_object(
      'provider_key', event_rows.provider_key,
      'provider_event_type', event_rows.provider_event_type,
      'provider_event_transport', event_rows.provider_event_transport
    )
  ) AS movement_rows
  WHERE event_rows.pay_batch_id = p_pay_batch_id
    AND (
      COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0
      OR event_rows.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
      OR event_rows.pay_bank_transfer_id IS NULL
    )
    AND (
      movement_rows.is_terminal_no_money
      OR event_rows.provider_failure_reason_group IS NOT NULL
      OR event_rows.provider_failure_reason_code IS NOT NULL
    )
  ORDER BY event_rows.received_at_utc DESC, event_rows.id DESC
  LIMIT 1;

  IF v_failure_event_provider_key IS NOT NULL OR v_failure_event_state IS NOT NULL OR v_failure_event_reason_code IS NOT NULL OR v_provider_failure_reason_group IS NOT NULL THEN
    v_provider_failure_reason_result := public._banking_provider_failure_reason_normalise(
      v_failure_event_provider_key,
      v_failure_event_state,
      v_failure_event_reason_code,
      v_provider_failure_reason_group,
      COALESCE(v_failure_event_payload, '{}'::jsonb)
    );
    v_provider_failure_reason_code := COALESCE(v_failure_event_reason_code, v_provider_failure_reason_result ->> 'failure_reason_code');
    v_provider_failure_reason_group := COALESCE(NULLIF(v_provider_failure_reason_group, ''), v_provider_failure_reason_result ->> 'failure_reason_group');
    v_provider_failure_reason_label := v_provider_failure_reason_result ->> 'failure_reason_label';
  END IF;

  SELECT
    count(*) FILTER (WHERE correction_work_items.work_kind = 'PRE_BANK_CANCEL' AND correction_work_items.status = 'APPLIED')::integer,
    count(*) FILTER (WHERE correction_work_items.work_kind = 'NO_MONEY_UNWIND' AND correction_work_items.status = 'APPLIED')::integer
  INTO v_pre_bank_applied_count,
       v_no_money_applied_count
  FROM public.pay_payment_correction_work_items AS correction_work_items
  WHERE correction_work_items.pay_batch_id = p_pay_batch_id
    AND (
      COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0
      OR correction_work_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
      OR correction_work_items.pay_bank_transfer_id IS NULL
    );

  SELECT EXISTS (
           SELECT 1
           FROM public.pay_payment_correction_requests AS recovery_request
           WHERE recovery_request.pay_batch_id = p_pay_batch_id
             AND upper(BTRIM(COALESCE(recovery_request.status::text, ''))) IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING', 'APPLIED', 'COMPLETE', 'COMPLETED')
             AND upper(BTRIM(COALESCE(recovery_request.correction_kind::text, ''))) IN ('PAID_RECOVERY_REQUIRED', 'PAID_SETTLED_RECOVERY_REQUIRED', 'AMEND_AND_RECOVER_OVERPAYMENT', 'OVERPAYMENT_RECOVERY', 'SETTLED_REVERSAL', 'POST_PAYMENT_AMENDMENT')
         )
         OR EXISTS (
           SELECT 1
           FROM public.pay_payment_correction_items AS recovery_item
           WHERE recovery_item.pay_batch_id = p_pay_batch_id
             AND upper(BTRIM(COALESCE(recovery_item.status::text, ''))) IN ('PENDING', 'APPROVED', 'AUTHORISED', 'APPLIED', 'PROCESSING')
             AND upper(BTRIM(COALESCE(recovery_item.correction_item_kind::text, ''))) IN ('SETTLED_REVERSAL', 'OVERPAYMENT_RECOVERY', 'PAID_RECOVERY', 'POST_PAYMENT_AMENDMENT')
         )
         OR EXISTS (
           SELECT 1
           FROM public.pay_batch_items AS recovery_batch_item
           JOIN public.pay_batch_candidates AS recovery_batch_candidate
             ON recovery_batch_candidate.id = recovery_batch_item.pay_batch_candidate_id
           WHERE recovery_batch_candidate.pay_batch_id = p_pay_batch_id
             AND COALESCE(recovery_batch_item.is_voided, false) = false
             AND upper(BTRIM(COALESCE(recovery_batch_item.item_type::text, ''))) = 'OVERPAYMENT_RECOVERY'
             AND COALESCE(recovery_batch_item.amount_inc_vat, recovery_batch_item.amount_ex_vat, 0) <> 0
         )
         OR upper(NULLIF(BTRIM(COALESCE(v_selection_json->>'source_context', '')), '')) IN ('PAID_RECOVERY_REQUIRED', 'POST_PAYMENT_AMENDMENT', 'OVERPAYMENT_RECOVERY')
  INTO v_has_actual_recovery_context;

  v_has_actual_recovery_context := COALESCE(v_has_actual_recovery_context, false);

  SELECT count(*)::integer
  INTO v_open_provider_submit_count
  FROM public.banking_pay_operations AS provider_operations
  WHERE provider_operations.pay_batch_id = p_pay_batch_id
    AND provider_operations.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
    AND provider_operations.status IN ('QUEUED', 'RUNNING', 'PROCESSING', 'CLAIMED', 'IN_PROGRESS')
    AND NOT (
      provider_operations.operation_type = 'PAYMENT_EXECUTE'
      AND provider_operations.phase = 'SCHEDULE_PAYMENT'
      AND provider_operations.resume_reason IN (
        'WAIT_FOR_SCHEDULED_NO_BANK_PAYMENT',
        'WAIT_FOR_SCHEDULED_LOCAL_MANUAL_SETTLEMENT'
      )
    );

  v_transfer_count := COALESCE(v_transfer_count, 0);
  v_transfer_final_count := COALESCE(v_transfer_final_count, 0);
  v_transfer_terminal_no_money_count := COALESCE(v_transfer_terminal_no_money_count, 0);
  v_transfer_pending_count := COALESCE(v_transfer_pending_count, 0);
  v_event_count := COALESCE(v_event_count, 0);
  v_event_final_count := COALESCE(v_event_final_count, 0);
  v_event_terminal_no_money_count := COALESCE(v_event_terminal_no_money_count, 0);
  v_event_pending_count := COALESCE(v_event_pending_count, 0);
  v_event_unknown_count := COALESCE(v_event_unknown_count, 0);
  v_provider_webhook_evidence_count := COALESCE(v_provider_webhook_evidence_count, 0);
  v_invalid_webhook_evidence_count := COALESCE(v_invalid_webhook_evidence_count, 0);
  v_failed_webhook_replay_evidence_count := COALESCE(v_failed_webhook_replay_evidence_count, 0);
  v_provider_poll_evidence_count := COALESCE(v_provider_poll_evidence_count, 0);
  v_provider_outage_count := COALESCE(v_provider_outage_count, 0);
  v_pre_bank_applied_count := COALESCE(v_pre_bank_applied_count, 0);
  v_no_money_applied_count := COALESCE(v_no_money_applied_count, 0);
  v_open_provider_submit_count := COALESCE(v_open_provider_submit_count, 0);

  v_final_evidence_count := v_transfer_final_count + v_event_final_count;
  v_terminal_no_money_count := v_transfer_terminal_no_money_count + v_event_terminal_no_money_count;
  -- A locally prepared transfer is stored as PENDING before any provider call.
  -- Do not turn that local row into provider-pending authority when the central
  -- evidence classifier proves that nothing was submitted or sent. Provider
  -- events and explicit request/submission evidence remain fail-closed.
  v_pending_provider_count :=
    CASE WHEN v_local_prepared_only THEN 0 ELSE v_transfer_pending_count END
    + v_event_pending_count
    + CASE WHEN v_provider_submitted OR v_provider_request_sent THEN 1 ELSE 0 END;
  v_unknown_provider_count := v_event_unknown_count + CASE
    WHEN v_provider_outage_count > 0 OR v_provider_blocker_code = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 0
    WHEN v_provider_evidence_class = 'PROVIDER_OUTCOME_UNKNOWN' OR v_provider_blocker_code = 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 1
    ELSE 0
  END;

  v_has_paid_or_settled := v_final_evidence_count > 0 OR upper(COALESCE(v_provider_cash_state, '')) = 'FINAL_PAID';
  v_has_provider_unknown := v_unknown_provider_count > 0;
  v_has_provider_pending := v_pending_provider_count > 0 AND v_terminal_no_money_count = 0 AND v_final_evidence_count = 0;
  v_has_provider_outage := v_provider_outage_count > 0 OR v_provider_blocker_code = 'PROVIDER_OUTAGE_RETRY_LATER';
  v_has_terminal_no_money := v_terminal_no_money_count > 0 OR upper(COALESCE(v_provider_cash_state, '')) = 'TERMINAL_NO_MONEY';
  v_is_local_not_sent := v_has_paid_or_settled IS NOT TRUE
                          AND v_has_provider_unknown IS NOT TRUE
                          AND v_has_provider_pending IS NOT TRUE
                          AND v_has_terminal_no_money IS NOT TRUE
                          AND v_provider_submitted IS NOT TRUE
                          AND v_provider_request_sent IS NOT TRUE
                          AND v_provider_response_present IS NOT TRUE
                          AND v_provider_event_present IS NOT TRUE
                          AND v_provider_external_id_present IS NOT TRUE;

  IF COALESCE(v_has_paid_or_settled, false) IS TRUE
     AND COALESCE(v_has_terminal_no_money, false) IS TRUE THEN
    v_has_actual_recovery_context := true;
  END IF;

  IF v_partial_scope_blocker_count > 0 THEN
    v_blockers := v_blockers || COALESCE(v_resolved_scope_json -> 'partial_scope_blockers', '[]'::jsonb);
  END IF;

  IF v_freshness_blocker_count > 0 THEN
    v_blockers := v_blockers || COALESCE(v_freshness_result -> 'blockers', '[]'::jsonb);
  END IF;

  -- Source restoration remains truthful: ambiguous rows cannot be reconstructed.
  -- For a complete, proved-unpaid scope this is advisory to financial cancellation;
  -- the immutable correction evidence is surfaced for Banking investigation afterward.
  v_source_restoration_investigation_required :=
    v_source_less_ambiguous_count > 0
    AND v_scope_is_full
    AND v_scope_type IN ('BATCH', 'CANDIDATES')
    AND (v_is_local_not_sent OR v_has_terminal_no_money)
    AND v_has_paid_or_settled IS NOT TRUE
    AND v_has_provider_unknown IS NOT TRUE
    AND v_has_provider_pending IS NOT TRUE
    AND v_open_provider_submit_count = 0
    AND v_partial_scope_blocker_count = 0
    AND v_freshness_blocker_count = 0;

  IF v_manual_carry_forward_blocker_count > 0 OR v_can_carry_forward_automatically IS NOT TRUE THEN
    IF v_source_restoration_investigation_required THEN
      v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
        'code', 'SOURCE_RESTORATION_INVESTIGATION_REQUIRED_AFTER_CANCELLATION',
        'message', 'The unpaid payment can be cancelled, but ambiguous source-less adjustment evidence must be investigated and is not reconstructed.',
        'source_less_ambiguous_count', v_source_less_ambiguous_count,
        'carry_forward_blockers', COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb),
        'policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'
      ));
    ELSE
      v_blockers := v_blockers || COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb);
    END IF;
  END IF;

  IF v_open_provider_submit_count > 0 THEN
    v_race_or_submission_blockers := v_race_or_submission_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PROVIDER_SUBMISSION_IN_PROGRESS',
      'message', 'Provider submission is in progress for this batch.'
    ));
    v_blockers := v_blockers || v_race_or_submission_blockers;
  END IF;

  IF v_batch_terminal THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PAYMENT_CORRECTION_BATCH_TERMINAL',
      'message', 'This payment batch is already complete and cannot be changed.',
      'batch_status', v_batch.status
    ));
  END IF;

  IF v_has_paid_or_settled AND COALESCE(v_has_actual_recovery_context, false) THEN
    v_lifecycle_state := 'PAID_OR_SETTLED';
    v_lifecycle_label := 'Recover overpayment in next pay run';
    v_recommended_action := 'AMEND_AND_RECOVER_OVERPAYMENT';
  ELSIF v_has_provider_unknown THEN
    v_lifecycle_state := 'PROVIDER_OUTCOME_UNKNOWN';
    v_lifecycle_label := 'Provider outcome unknown — check provider';
    v_recommended_action := 'CHECK_PROVIDER_STATUS';
  ELSIF v_has_paid_or_settled THEN
    v_lifecycle_state := 'PAID_OR_SETTLED';
    v_lifecycle_label := CASE WHEN upper(COALESCE(v_batch.status, '')) = 'SETTLED' THEN 'Settled' ELSE 'Paid' END;
    v_recommended_action := 'VIEW_DETAILS';
  ELSIF v_has_provider_pending THEN
    v_lifecycle_state := 'PROVIDER_SUBMITTED_PENDING';
    v_lifecycle_label := 'Provider pending';
    v_recommended_action := 'CHECK_PROVIDER_STATUS';
  ELSIF v_has_provider_outage
        AND COALESCE(v_provider_request_sent, false) IS NOT TRUE
        AND COALESCE(v_provider_submitted, false) IS NOT TRUE
        AND COALESCE(v_provider_external_id_present, false) IS NOT TRUE
        AND COALESCE(jsonb_array_length(v_blockers), 0) = 0 THEN
    v_lifecycle_state := 'PROVIDER_OUTAGE_RETRY_LATER';
    v_lifecycle_label := 'Bank unavailable — unsent payments can be retried';
    v_recommended_action := 'RETRY_PROVIDER_LATER';
  ELSIF v_no_money_applied_count > 0 THEN
    v_lifecycle_state := 'FINANCIALS_REWOUND';
    v_lifecycle_label := 'Financials rewound — amend timesheet and recalculate';
    v_recommended_action := 'VIEW_RECALCULATE_NEXT_STEP';
  ELSIF v_has_terminal_no_money THEN
    IF upper(COALESCE(v_failure_event_state, '')) IN ('CANCELLED', 'CANCELED') THEN
      v_lifecycle_state := 'PROVIDER_CANCELLED_NO_MONEY';
    ELSE
      v_lifecycle_state := 'PROVIDER_FAILED_NO_MONEY';
    END IF;
    v_lifecycle_label := 'Failed — not paid';
    v_recommended_action := 'NO_MONEY_UNWIND_AND_RECALCULATE';
  ELSIF v_pre_bank_applied_count > 0 THEN
    IF v_scope_type = 'BATCH' THEN
      v_lifecycle_state := 'CANCELLED_BEFORE_BANK_SUBMISSION';
      v_lifecycle_label := 'Cancelled before bank submission';
    ELSE
      v_lifecycle_state := 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION';
      v_lifecycle_label := 'Partially cancelled before bank submission';
    END IF;
    v_recommended_action := 'VIEW_RECALCULATE_NEXT_STEP';
  ELSIF v_is_local_not_sent THEN
    IF upper(COALESCE(v_batch.status, '')) = 'SCHEDULED' OR v_batch.scheduled_at_utc IS NOT NULL THEN
      v_lifecycle_state := 'SCHEDULED_LOCAL_NOT_SENT';
      v_lifecycle_label := 'Scheduled — not sent to bank yet';
    ELSE
      v_lifecycle_state := 'LOCAL_PREPARED_NOT_SENT';
      v_lifecycle_label := 'Prepared locally — not sent to bank yet';
    END IF;
    v_recommended_action := 'PRE_PROVIDER_CANCEL_AND_RECALCULATE';
  ELSE
    v_lifecycle_state := 'PROVIDER_OUTCOME_UNKNOWN';
    v_lifecycle_label := 'Provider outcome unknown — check provider';
    v_recommended_action := 'CHECK_PROVIDER_STATUS';
  END IF;

  v_can_pre_provider_cancel := v_lifecycle_state IN ('LOCAL_PREPARED_NOT_SENT', 'SCHEDULED_LOCAL_NOT_SENT')
                               AND v_scope_is_full
                               AND COALESCE(jsonb_array_length(v_blockers), 0) = 0;
  v_can_no_money_unwind := v_lifecycle_state IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY')
                           AND v_scope_is_full
                           AND COALESCE(jsonb_array_length(v_blockers), 0) = 0;
  v_can_recover_overpayment := v_lifecycle_state = 'PAID_OR_SETTLED' AND COALESCE(v_has_actual_recovery_context, false);
  v_requires_provider_cancel := v_provider_submitted OR v_provider_request_sent;
  v_requires_bank_check := v_lifecycle_state IN ('PROVIDER_OUTCOME_UNKNOWN', 'PROVIDER_SUBMITTED_PENDING');
  v_requires_retry_later := v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER';
  v_whole_batch_cancel_available := v_can_pre_provider_cancel AND v_scope_type = 'BATCH';
  v_row_cancel_available := v_can_pre_provider_cancel AND v_scope_type <> 'BATCH';
  v_retry_same_batch_safe := v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER'
                             AND v_provider_request_sent IS NOT TRUE
                             AND v_provider_submitted IS NOT TRUE
                             AND v_provider_external_id_present IS NOT TRUE
                             AND COALESCE(jsonb_array_length(v_blockers), 0) = 0;

  v_provider_evidence_summary_json := jsonb_build_object(
    'evidence_class', v_provider_evidence_class,
    'provider_submitted', v_provider_submitted,
    'provider_request_sent', v_provider_request_sent,
    'provider_response_present', v_provider_response_present,
    'provider_event_present', v_provider_event_present,
    'provider_external_id_present', v_provider_external_id_present,
    'local_prepared_only', v_local_prepared_only,
    'cash_state', v_provider_cash_state,
    'provider_webhook_evidence_count', v_provider_webhook_evidence_count,
    'invalid_webhook_evidence_count', v_invalid_webhook_evidence_count,
    'failed_webhook_replay_evidence_count', v_failed_webhook_replay_evidence_count,
    'provider_poll_evidence_count', v_provider_poll_evidence_count,
    'support_details_json', v_provider_support_details_json
  );

  v_blocking_paid_evidence_json := jsonb_build_object(
    'has_paid_or_settled', v_has_paid_or_settled,
    'final_evidence_count', v_final_evidence_count,
    'transfer_final_count', v_transfer_final_count,
    'event_final_count', v_event_final_count
  );

  v_terminal_no_money_evidence_json := jsonb_build_object(
    'has_terminal_no_money', v_has_terminal_no_money,
    'terminal_no_money_count', v_terminal_no_money_count,
    'transfer_terminal_no_money_count', v_transfer_terminal_no_money_count,
    'event_terminal_no_money_count', v_event_terminal_no_money_count,
    'provider_failure_reason_code', v_provider_failure_reason_code,
    'provider_failure_reason_group', v_provider_failure_reason_group,
    'provider_failure_reason_label', v_provider_failure_reason_label
  );

  v_pending_provider_evidence_json := jsonb_build_object(
    'has_provider_pending', v_has_provider_pending,
    'pending_provider_count', v_pending_provider_count,
    'transfer_pending_count', v_transfer_pending_count,
    'event_pending_count', v_event_pending_count
  );

  v_provider_outcome_unknown_evidence_json := jsonb_build_object(
    'has_provider_unknown', v_has_provider_unknown,
    'unknown_provider_count', v_unknown_provider_count,
    'event_unknown_count', v_event_unknown_count,
    'provider_blocker_code', v_provider_blocker_code,
    'provider_reason', v_provider_reason
  );

  v_provider_webhook_evidence_json := jsonb_build_object(
    'provider_webhook_evidence_count', v_provider_webhook_evidence_count,
    'invalid_webhook_evidence_count', v_invalid_webhook_evidence_count,
    'failed_webhook_replay_evidence_count', v_failed_webhook_replay_evidence_count,
    'provider_poll_evidence_count', v_provider_poll_evidence_count,
    'event_count', v_event_count
  );

  v_finance_scope_json := jsonb_build_object(
    'finance_case_ids', COALESCE(v_resolved_scope_json -> 'finance_case_ids', '[]'::jsonb),
    'finance_component_ids', COALESCE(v_resolved_scope_json -> 'finance_component_ids', '[]'::jsonb),
    'reservation_ids', COALESCE(v_resolved_scope_json -> 'reservation_ids', '[]'::jsonb)
  );

  v_alert_candidate_is_success_only := v_lifecycle_state = 'PAID_OR_SETTLED'
    AND COALESCE(v_has_actual_recovery_context, false) IS NOT TRUE;
  v_alert_candidate_kind := CASE
    WHEN v_alert_candidate_is_success_only THEN NULL::text
    WHEN v_lifecycle_state = 'PAID_OR_SETTLED' AND v_recommended_action = 'AMEND_AND_RECOVER_OVERPAYMENT' THEN 'PAID_SETTLED_RECOVERY_REQUIRED'
    WHEN v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'PROVIDER_OUTAGE_RETRY_LATER'
    WHEN v_lifecycle_state = 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    WHEN v_lifecycle_state IN ('PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY') THEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
    WHEN v_lifecycle_state = 'PROVIDER_SUBMITTED_PENDING' THEN NULL::text
    WHEN v_manual_carry_forward_blocker_count > 0 AND v_source_restoration_investigation_required IS NOT TRUE THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
    ELSE NULL::text
  END;

  v_alert_candidate_severity := CASE
    WHEN v_alert_candidate_kind IN ('PROVIDER_OUTAGE_RETRY_LATER', 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER', 'TERMINAL_NO_MONEY_REWIND_AVAILABLE', 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS', 'PAID_SETTLED_RECOVERY_REQUIRED') THEN 'ACTION_REQUIRED'
    ELSE NULL::text
  END;

  v_status_update_signature := md5(jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'scope_type', v_scope_type,
    'lifecycle_state', v_lifecycle_state,
    'recommended_action', v_recommended_action,
    'final_evidence_count', v_final_evidence_count,
    'terminal_no_money_count', v_terminal_no_money_count,
    'pending_provider_count', v_pending_provider_count,
    'unknown_provider_count', v_unknown_provider_count,
    'pre_bank_applied_count', v_pre_bank_applied_count,
    'no_money_applied_count', v_no_money_applied_count,
    'source_restoration_investigation_required', v_source_restoration_investigation_required,
    'source_less_ambiguous_count', v_source_less_ambiguous_count
  )::text);

  v_support_details_json := jsonb_build_object(
    'batch_status', v_batch.status,
    'scope_type', v_scope_type,
    'scope_is_full', v_scope_is_full,
    'transfer_count', v_transfer_count,
    'event_count', v_event_count,
    'pre_bank_applied_count', v_pre_bank_applied_count,
    'no_money_applied_count', v_no_money_applied_count,
    'open_provider_submit_count', v_open_provider_submit_count,
    'source_restoration_investigation_required', v_source_restoration_investigation_required,
    'source_less_ambiguous_count', v_source_less_ambiguous_count,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
  );

  v_result := jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'selection_json', v_selection_json,
    'scope_type', v_scope_type,
    'payment_lifecycle_state', v_lifecycle_state,
    'payment_lifecycle_label', v_lifecycle_label,
    'recommended_action', v_recommended_action,
    'next_step', CASE WHEN v_lifecycle_state IN ('FINANCIALS_REWOUND', 'CANCELLED_BEFORE_BANK_SUBMISSION', 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION') THEN 'AMEND_TIMESHEET_AND_RECALCULATE' ELSE NULL::text END,
    'next_step_label', CASE WHEN v_lifecycle_state IN ('FINANCIALS_REWOUND', 'CANCELLED_BEFORE_BANK_SUBMISSION', 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION') THEN 'Amend timesheets and recalculate' ELSE NULL::text END,
    'can_pre_provider_cancel', v_can_pre_provider_cancel,
    'can_no_money_unwind', v_can_no_money_unwind,
    'can_recover_overpayment', v_can_recover_overpayment,
    'requires_provider_cancel', v_requires_provider_cancel,
    'requires_bank_check', v_requires_bank_check,
    'requires_retry_later', v_requires_retry_later,
    'whole_batch_cancel_available', v_whole_batch_cancel_available,
    'row_cancel_available', v_row_cancel_available,
    'retry_same_batch_safe', v_retry_same_batch_safe,
    'retry_status_label', CASE WHEN v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Bank unavailable — unsent payments can be retried' ELSE NULL::text END,
    'retry_row_label', CASE WHEN v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Bank unavailable — retry available' ELSE NULL::text END,
    'retry_button_label', CASE WHEN v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Retry unsent payments' ELSE NULL::text END,
    'retry_running_label', CASE WHEN v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Retrying unsent payments' ELSE NULL::text END,
    'retry_disabled_button_label', CASE WHEN v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Retry in progress' ELSE NULL::text END,
    'retry_row_action_label', CASE WHEN v_lifecycle_state = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Open retry summary' ELSE NULL::text END,
    'paid_recovery_label', CASE WHEN v_lifecycle_state = 'PAID_OR_SETTLED' AND COALESCE(v_has_actual_recovery_context, false) THEN 'Recover overpayment in next pay run' ELSE NULL::text END,
    'has_actual_recovery_context', COALESCE(v_has_actual_recovery_context, false)
  );

  v_result := v_result || jsonb_build_object(
    'provider_evidence_summary_json', v_provider_evidence_summary_json,
    'blocking_paid_evidence_json', v_blocking_paid_evidence_json,
    'terminal_no_money_evidence_json', v_terminal_no_money_evidence_json,
    'pending_provider_evidence_json', v_pending_provider_evidence_json,
    'provider_outcome_unknown_evidence_json', v_provider_outcome_unknown_evidence_json,
    'provider_webhook_evidence_json', v_provider_webhook_evidence_json,
    'resolved_full_payment_scope_json', v_resolved_scope_json,
    'finance_scope_json', v_finance_scope_json,
    'manual_adjustment_carry_forward_required', v_manual_carry_forward_required,
    'manual_adjustments_to_carry_forward', COALESCE(v_manual_adjustment_result -> 'manual_adjustments_to_carry_forward', '[]'::jsonb),
    'manual_adjustments_carried_forward_existing', COALESCE(v_manual_adjustment_result -> 'manual_adjustments_carried_forward_existing', '[]'::jsonb),
    'can_carry_forward_automatically', v_can_carry_forward_automatically,
    'source_less_ambiguous_count', v_source_less_ambiguous_count,
    'source_restoration_investigation_required', v_source_restoration_investigation_required,
    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'
  );

  v_result := v_result || jsonb_build_object(
    'carry_forward_blockers', COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb),
    'manual_adjustment_support_details_json', COALESCE(v_manual_adjustment_result -> 'manual_adjustment_support_details_json', '{}'::jsonb),
    'race_or_submission_blockers', v_race_or_submission_blockers,
    'blockers', v_blockers,
    'warnings', v_warnings,
    'support_details_json', v_support_details_json,
    'provider_failure_reason_code', v_provider_failure_reason_code,
    'provider_failure_reason_group', v_provider_failure_reason_group,
    'provider_failure_reason_label', v_provider_failure_reason_label,
    'status_update_signature', v_status_update_signature,
    'verified_webhook_evidence_count', v_provider_webhook_evidence_count,
    'invalid_webhook_evidence_count', v_invalid_webhook_evidence_count,
    'failed_webhook_replay_evidence_count', v_failed_webhook_replay_evidence_count,
    'alert_candidate_kind', v_alert_candidate_kind,
    'alert_candidate_severity', v_alert_candidate_severity,
    'alert_candidate_is_success_only', v_alert_candidate_is_success_only
  );

  RETURN v_result;
END;
$function$;

CREATE OR REPLACE FUNCTION public._pay_payment_movement_classify(
  p_pay_batch_id uuid,
  p_selection_json jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_selection_json jsonb := COALESCE(p_selection_json, '{}'::jsonb);
  v_diagnostic_json jsonb := '{}'::jsonb;
  v_lifecycle text := NULL::text;
  v_recommended_action text := NULL::text;
  v_classification text := NULL::text;
  v_blockers jsonb := '[]'::jsonb;
  v_manual_result jsonb := '{}'::jsonb;
  v_safe_to_auto_apply boolean := false;
  v_source_restoration_investigation_required boolean := false;
  v_diagnostic_context text := NULL::text;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR jsonb_typeof(p_selection_json) <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  v_diagnostic_context := CASE
    WHEN UPPER(BTRIM(COALESCE(v_selection_json->>'diagnostic_context', v_selection_json->>'diagnosticContext', ''))) IN (
      'CURRENT_PAYMENT_STATUS',
      'CURRENT_PAYMENT_STATUS_TAB',
      'PAYMENT_STATUS_TAB',
      'PAYMENT_ISSUES_TAB',
      'PAYMENT_ISSUE_REVIEW',
      'CANCELLATION_ACTION',
      'CANCEL_PAYMENT_ACTION',
      'CANCEL_WHOLE_BATCH_ACTION',
      'CORRECTION_REVIEW',
      'CORRECTION_ACTION',
      'PAYMENT_CORRECTION_PLAN',
      'USER_TRIGGERED_DIAGNOSTIC',
      'EXPLICIT_DIAGNOSTIC'
    ) THEN UPPER(BTRIM(COALESCE(v_selection_json->>'diagnostic_context', v_selection_json->>'diagnosticContext', '')))
    WHEN UPPER(BTRIM(COALESCE(v_selection_json->>'scope_type', v_selection_json->>'scopeType', ''))) IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN 'CANCEL_WHOLE_BATCH_ACTION'
    ELSE 'CANCEL_PAYMENT_ACTION'
  END;

  v_diagnostic_json := public.pay_payment_cancelability_diagnostic(
    p_pay_batch_id,
    v_selection_json,
    NULL::uuid,
    v_diagnostic_context
  );
  v_lifecycle := NULLIF(btrim(COALESCE(v_diagnostic_json->>'payment_lifecycle_state', '')), '');
  v_recommended_action := NULLIF(btrim(COALESCE(v_diagnostic_json->>'recommended_action', '')), '');
  v_source_restoration_investigation_required := COALESCE(NULLIF(v_diagnostic_json->>'source_restoration_investigation_required', '')::boolean, false);
  v_manual_result := jsonb_build_object(
    'manual_adjustment_carry_forward_required', COALESCE(NULLIF(v_diagnostic_json->>'manual_adjustment_carry_forward_required', '')::boolean, false),
    'manual_adjustments_to_carry_forward', COALESCE(v_diagnostic_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
    'manual_adjustments_carried_forward_existing', COALESCE(v_diagnostic_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb),
    'can_carry_forward_automatically', COALESCE(NULLIF(v_diagnostic_json->>'can_carry_forward_automatically', '')::boolean, true),
    'carry_forward_blockers', COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb),
    'manual_adjustment_support_details_json', COALESCE(v_diagnostic_json->'manual_adjustment_support_details_json', '{}'::jsonb),
    'source_less_ambiguous_count', COALESCE(NULLIF(v_diagnostic_json->>'source_less_ambiguous_count', '')::integer, 0),
    'source_restoration_investigation_required', v_source_restoration_investigation_required,
    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'
  );

  v_blockers := COALESCE(v_diagnostic_json->'blockers', '[]'::jsonb);

  IF v_lifecycle = 'PAID_OR_SETTLED' THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_HAS_SETTLEMENT_EVIDENCE',
      'message', 'Payment has paid/settled evidence; use overpayment recovery instead of no-money unwind.',
      'required_action', 'AMEND_AND_RECOVER_OVERPAYMENT'
    ));
  END IF;

  IF v_lifecycle = 'PROVIDER_OUTCOME_UNKNOWN' THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER',
      'message', 'Provider outcome is unknown; check provider before retry or unwind.',
      'required_action', 'CHECK_PROVIDER_STATUS'
    ));
  END IF;

  IF v_lifecycle = 'PROVIDER_SUBMITTED_PENDING' THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PROVIDER_CANCELLATION_REQUIRED_BEFORE_UNWIND',
      'message', 'Provider submission is pending; provider state must be checked before unwind.',
      'required_action', 'CHECK_PROVIDER_STATUS'
    ));
  END IF;

  IF (COALESCE(NULLIF(v_diagnostic_json->>'source_less_ambiguous_count', '')::integer, 0) > 0
     OR COALESCE(jsonb_array_length(COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb)), 0) > 0)
     AND v_source_restoration_investigation_required IS NOT TRUE THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS',
      'message', 'One or more source-less manual adjustments could not be carried forward automatically.',
      'carry_forward_blockers', COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb)
    ));
  END IF;

  v_classification := CASE
    WHEN v_lifecycle = 'LOCAL_PREPARED_NOT_SENT' THEN 'LOCAL_PREPARED_NOT_SENT'
    WHEN v_lifecycle = 'SCHEDULED_LOCAL_NOT_SENT' THEN 'SCHEDULED_LOCAL_NOT_SENT'
    WHEN v_lifecycle = 'PROVIDER_SUBMITTED_PENDING' THEN 'PROVIDER_SUBMITTED_PENDING'
    WHEN v_lifecycle = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'PROVIDER_OUTAGE_RETRY_LATER'
    WHEN v_lifecycle = 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_OUTCOME_UNKNOWN'
    WHEN v_lifecycle = 'PROVIDER_CANCELLED_NO_MONEY' THEN 'PROVIDER_CANCELLED_NO_MONEY'
    WHEN v_lifecycle = 'PROVIDER_FAILED_NO_MONEY' THEN 'PROVIDER_FAILED_NO_MONEY'
    WHEN v_lifecycle = 'PAID_OR_SETTLED' THEN 'PAID_OR_SETTLED'
    WHEN v_lifecycle = 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION' THEN 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION'
    WHEN v_lifecycle = 'CANCELLED_BEFORE_BANK_SUBMISSION' THEN 'CANCELLED_BEFORE_BANK_SUBMISSION'
    WHEN v_lifecycle = 'FINANCIALS_REWOUND' THEN 'FINANCIALS_REWOUND'
    ELSE 'PROVIDER_OUTCOME_UNKNOWN'
  END;

  v_safe_to_auto_apply := v_recommended_action IN ('PRE_PROVIDER_CANCEL_AND_RECALCULATE', 'NO_MONEY_UNWIND_AND_RECALCULATE')
    AND COALESCE(jsonb_array_length(COALESCE(v_blockers, '[]'::jsonb)), 0) = 0;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'diagnostic_context', v_diagnostic_context,
    'classification', v_classification,
    'payment_lifecycle_state', v_lifecycle,
    'recommended_action', v_recommended_action,
    'safe_to_auto_apply', v_safe_to_auto_apply,
    'can_pre_provider_cancel', COALESCE(NULLIF(v_diagnostic_json->>'can_pre_provider_cancel', '')::boolean, false),
    'can_no_money_unwind', COALESCE(NULLIF(v_diagnostic_json->>'can_no_money_unwind', '')::boolean, false),
    'can_recover_overpayment', COALESCE(NULLIF(v_diagnostic_json->>'can_recover_overpayment', '')::boolean, false),
    'requires_provider_cancel', COALESCE(NULLIF(v_diagnostic_json->>'requires_provider_cancel', '')::boolean, false),
    'requires_bank_check', COALESCE(NULLIF(v_diagnostic_json->>'requires_bank_check', '')::boolean, false),
    'requires_retry_later', COALESCE(NULLIF(v_diagnostic_json->>'requires_retry_later', '')::boolean, false),
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'reasons', COALESCE(v_diagnostic_json->'warnings', '[]'::jsonb),
    'evidence', jsonb_build_object(
      'blocking_paid_evidence_json', COALESCE(v_diagnostic_json->'blocking_paid_evidence_json', '{}'::jsonb),
      'terminal_no_money_evidence_json', COALESCE(v_diagnostic_json->'terminal_no_money_evidence_json', '{}'::jsonb),
      'pending_provider_evidence_json', COALESCE(v_diagnostic_json->'pending_provider_evidence_json', '{}'::jsonb),
      'provider_evidence', COALESCE(v_diagnostic_json#>'{support_details_json,provider_evidence}', '{}'::jsonb)
    ),
    'manual_adjustment_carry_forward', v_manual_result,
    'source_restoration_investigation_required', v_source_restoration_investigation_required,
    'source_less_ambiguous_count', COALESCE(NULLIF(v_diagnostic_json->>'source_less_ambiguous_count', '')::integer, 0),
    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION',
    'resolved_full_payment_scope_json', COALESCE(v_diagnostic_json->'resolved_full_payment_scope_json', '{}'::jsonb),
    'finance_scope_json', COALESCE(v_diagnostic_json->'finance_scope_json', '{}'::jsonb),
    'diagnostic_payload', v_diagnostic_json,
    'policy_x_checked', true
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_payment_correction_plan(
  p_pay_batch_id uuid,
  p_selection_json jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_correction_context text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch_id uuid;
  v_batch_status text;
  v_batch_pay_date date;
  v_batch_authoritative_payment_date date;
  v_effective_pay_date date;
  v_batch_created_at_utc timestamptz;
  v_batch_execution_commit_state text;
  v_batch_execution_commit_ref text;
  v_classification_result jsonb := '{}'::jsonb;
  v_classification text := 'AMBIGUOUS_REVIEW_REQUIRED';
  v_recommended_action text := 'REVIEW_BANK_EVIDENCE';
  v_hard_blockers jsonb := '[]'::jsonb;
  v_warnings jsonb := '[]'::jsonb;
  v_selection_summary jsonb := '{}'::jsonb;
  v_affected_candidates jsonb := '[]'::jsonb;
  v_affected_transfers jsonb := '[]'::jsonb;
  v_affected_umbrellas jsonb := '[]'::jsonb;
  v_affected_items jsonb := '[]'::jsonb;
  v_affected_finance_cases jsonb := '[]'::jsonb;
  v_communication_effects jsonb := '{}'::jsonb;
  v_suggested_resolution_required boolean := false;
  v_suggested_resolution jsonb := NULL::jsonb;
  v_amounts jsonb := '{}'::jsonb;
  v_draft_interference jsonb := '[]'::jsonb;
  v_large_correction jsonb := '{}'::jsonb;
  v_work_expansion_plan jsonb := '{}'::jsonb;
  v_can_apply boolean := false;
  v_draft_removal_requested boolean := false;
  v_strict_draft_removal_pre_bank_cancel boolean := false;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_id_count integer := 0;
  v_expected_item_mismatch_count integer := 0;

  v_selected_item_count integer := 0;
  v_selected_candidate_count integer := 0;
  v_selected_transfer_count integer := 0;
  v_selected_umbrella_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_already_corrected_count integer := 0;
  v_voided_count integer := 0;
  v_pay_channel_change_count integer := 0;
  v_umbrella_change_count integer := 0;
  v_timesheet_item_count integer := 0;
  v_net_fixed_finance_item_count integer := 0;
  v_gross_channel_sensitive_item_count integer := 0;
  v_work_item_count integer := 0;
  v_large_correction_threshold integer := 100;
  v_recommended_chunk_size integer := 50;
  v_total_amount_ex_vat numeric := 0;
  v_total_amount_vat numeric := 0;
  v_total_amount_inc_vat numeric := 0;
  v_queued_unsent_count integer := 0;
  v_sent_notice_count integer := 0;
  v_subject_id text;
  v_scope_type text;
  v_selected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_selection_hash text;
  v_selection_filters_applied jsonb := '{}'::jsonb;
  v_suggested_resolution_finance_cases jsonb := '[]'::jsonb;
  v_finance_case_record record;
  v_case_component_ids jsonb := '[]'::jsonb;
  v_case_component_fingerprints jsonb := '{}'::jsonb;
  v_case_suggestion jsonb := NULL::jsonb;
  v_case_suggestion_hash text := NULL::text;
  v_case_generation_error jsonb := NULL::jsonb;
  v_selected_mail_scope_json jsonb := '{}'::jsonb;
  v_mail_legacy_review_count integer := 0;
  v_mail_legacy_queued_review_count integer := 0;
  v_financial_cancellation_communication_v2 boolean := false;

  v_retry_eligible_count integer := 0;
  v_retry_ineligible_count integer := 0;
  v_retry_eligible_scope_json jsonb := '{}'::jsonb;
  v_retry_ineligible_summary_json jsonb := '[]'::jsonb;
  v_retry_in_progress boolean := false;
  v_retry_already_in_progress boolean := false;
  v_retry_operation_id uuid := NULL::uuid;
  v_retry_operation_status text := NULL::text;
  v_retry_button_visible boolean := false;
  v_retry_button_disabled_reason text := NULL::text;
  v_retry_scope_signature text := NULL::text;
  v_retry_status_label text := NULL::text;
  v_retry_button_label text := NULL::text;
  v_retry_running_label text := NULL::text;
  v_recovery_action_label text := NULL::text;
  v_correction_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_correction_context, '')), ''), '-', '_'));
BEGIN
  v_subject_id := COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID');
  v_scope_type := upper(nullif(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''));


  IF v_correction_context IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CORRECTION_PLAN_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CORRECTION_PLAN_CONTEXT_REQUIRED',
              'message', 'pay_payment_correction_plan requires an explicit correction context.'
            )::text;
  END IF;

  IF v_correction_context IN (
    'LIST',
    'BATCH_LIST',
    'PAY_BATCHES_LIST',
    'BOOTSTRAP',
    'BATCH_BOOTSTRAP',
    'PAY_BATCH_GET_BOOTSTRAP_ONLY',
    'OVERVIEW',
    'BATCH_OVERVIEW',
    'PREVIEW_OPEN',
    'OPERATION_GET',
    'OPERATION_PROGRESS',
    'PROGRESS_POLLING',
    'PREVIEW_PROGRESS',
    'LIVE_WATCH',
    'WATCH_SIGNAL',
    'RPC_CHANGES_PING'
  ) THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CORRECTION_PLAN_CONTEXT_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CORRECTION_PLAN_CONTEXT_NOT_ALLOWED',
              'context', v_correction_context,
              'message', 'Payment correction planning is blocked from list, bootstrap, Overview, preview, operation GET/progress, and progress polling paths.'
            )::text;
  END IF;

  IF v_correction_context NOT IN (
    'CORRECTION_REVIEW',
    'CORRECTION_ACTION',
    'PAYMENT_CORRECTION_REVIEW',
    'PAYMENT_CORRECTION_ACTION',
    'USER_TRIGGERED_CORRECTION',
    'CURRENT_PAYMENT_STATUS_CORRECTION_ACTION',
    'PAYMENT_ISSUES_CORRECTION_ACTION',
    'DRAFT_REMOVE_FROM_BATCH'
  ) THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CORRECTION_PLAN_CONTEXT_NOT_EXPLICIT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CORRECTION_PLAN_CONTEXT_NOT_EXPLICIT',
              'context', v_correction_context,
              'message', 'Payment correction planning is only allowed from explicit correction review/action flows.'
            )::text;
  END IF;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PLAN_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'selection_scope_type', CASE WHEN p_selection_json IS NULL THEN NULL ELSE p_selection_json->>'scope_type' END,
      'actor_user_id', p_actor_user_id,
      'selection_json', p_selection_json
    ),
    'pay_payment_correction',
    v_subject_id,
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

  SELECT
    public.pay_batches.id,
    public.pay_batches.status,
    public.pay_batches.pay_date,
    public.pay_batches.authoritative_payment_date,
    public.pay_batches.created_at_utc,
    public.pay_batches.execution_commit_state,
    public.pay_batches.execution_commit_ref
  INTO
    v_batch_id,
    v_batch_status,
    v_batch_pay_date,
    v_batch_authoritative_payment_date,
    v_batch_created_at_utc,
    v_batch_execution_commit_state,
    v_batch_execution_commit_ref
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF v_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_effective_pay_date := COALESCE(v_batch_authoritative_payment_date, v_batch_pay_date);

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_selected;
  CREATE TEMP TABLE _tmp_payment_correction_plan_selected ON COMMIT DROP AS
  SELECT
    selected_rows.pay_batch_id,
    selected_rows.pay_batch_candidate_id,
    selected_rows.candidate_id,
    selected_rows.candidate_display_name,
    selected_rows.candidate_tms_ref,
    selected_rows.pay_batch_item_id,
    selected_rows.item_type,
    selected_rows.timesheet_id,
    selected_rows.pay_bank_transfer_id,
    selected_rows.transfer_status,
    selected_rows.transfer_amount,
    selected_rows.transfer_group_key,
    selected_rows.payee_entity_kind,
    selected_rows.payee_entity_id,
    selected_rows.umbrella_id,
    selected_rows.umbrella_name,
    selected_rows.finance_case_id,
    selected_rows.finance_component_id,
    selected_rows.reservation_id,
    selected_rows.pay_channel,
    selected_rows.frozen_source_pay_method,
    selected_rows.frozen_target_pay_method,
    selected_rows.current_candidate_pay_method,
    selected_rows.economic_key_type,
    selected_rows.economic_key_value,
    selected_rows.source_amount_ex_vat,
    selected_rows.target_amount_ex_vat,
    selected_rows.key_resolution_source,
    selected_rows.key_resolution_failure_reason,
    selected_rows.amount_ex_vat,
    selected_rows.amount_vat,
    selected_rows.amount_inc_vat,
    selected_rows.is_voided,
    selected_rows.already_corrected,
    selected_rows.applied_correction_kinds
  FROM public._pay_payment_correction_selected_items(
    p_pay_batch_id,
    p_selection_json,
    true
  ) AS selected_rows;

  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (pay_batch_item_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (pay_batch_candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (finance_case_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (finance_component_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (reservation_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_selected (timesheet_id, economic_key_type, economic_key_value);

  SELECT
    COALESCE(array_agg(plan_selected.pay_batch_item_id ORDER BY plan_selected.pay_batch_item_id), ARRAY[]::uuid[]),
    md5(COALESCE(string_agg(plan_selected.pay_batch_item_id::text, ',' ORDER BY plan_selected.pay_batch_item_id::text), 'NO_SELECTED_ITEMS'))
  INTO
    v_selected_pay_batch_item_ids,
    v_selected_selection_hash
  FROM pg_temp._tmp_payment_correction_plan_selected AS plan_selected;

  v_selection_filters_applied := jsonb_build_object(
    'scope_type', v_scope_type,
    'pay_batch_item_id_supplied', p_selection_json ? 'pay_batch_item_id',
    'pay_batch_item_ids_supplied', p_selection_json ? 'pay_batch_item_ids',
    'pay_bank_transfer_id_supplied', p_selection_json ? 'pay_bank_transfer_id',
    'pay_bank_transfer_ids_supplied', p_selection_json ? 'pay_bank_transfer_ids',
    'finance_case_id_supplied', p_selection_json ? 'finance_case_id',
    'finance_case_ids_supplied', p_selection_json ? 'finance_case_ids',
    'finance_component_id_supplied', p_selection_json ? 'finance_component_id',
    'finance_component_ids_supplied', p_selection_json ? 'finance_component_ids',
    'reservation_id_supplied', p_selection_json ? 'reservation_id',
    'reservation_ids_supplied', p_selection_json ? 'reservation_ids',
    'item_type_supplied', p_selection_json ? 'item_type',
    'item_types_supplied', p_selection_json ? 'item_types',
    'expected_item_count_supplied', p_selection_json ? 'expected_item_count',
    'expected_pay_batch_item_ids_supplied', p_selection_json ? 'expected_pay_batch_item_ids'
  );

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_detail;
  CREATE TEMP TABLE _tmp_payment_correction_plan_detail ON COMMIT DROP AS
  SELECT
    plan_selected.pay_batch_id,
    plan_selected.pay_batch_candidate_id,
    plan_selected.candidate_id,
    plan_selected.candidate_display_name,
    plan_selected.candidate_tms_ref,
    plan_selected.pay_batch_item_id,
    plan_selected.item_type,
    plan_selected.timesheet_id,
    plan_selected.pay_bank_transfer_id,
    plan_selected.transfer_status,
    plan_selected.transfer_amount,
    plan_selected.transfer_group_key,
    plan_selected.payee_entity_kind,
    plan_selected.payee_entity_id,
    plan_selected.umbrella_id,
    plan_selected.umbrella_name,
    plan_selected.finance_case_id,
    plan_selected.finance_component_id,
    plan_selected.reservation_id,
    plan_selected.pay_channel,
    plan_selected.frozen_source_pay_method,
    plan_selected.frozen_target_pay_method,
    plan_selected.current_candidate_pay_method,
    plan_selected.economic_key_type,
    plan_selected.economic_key_value,
    plan_selected.source_amount_ex_vat,
    plan_selected.target_amount_ex_vat,
    plan_selected.key_resolution_source,
    plan_selected.key_resolution_failure_reason,
    plan_selected.amount_ex_vat,
    plan_selected.amount_vat,
    plan_selected.amount_inc_vat,
    plan_selected.is_voided,
    plan_selected.already_corrected,
    plan_selected.applied_correction_kinds,
    public.candidates.pay_method AS live_candidate_pay_method,
    public.candidates.umbrella_id AS live_candidate_umbrella_id,
    public.pay_batch_items.frozen_component_classification::text AS frozen_component_classification,
    public.pay_batch_items.frozen_resolution_mode::text AS frozen_resolution_mode,
    public.pay_batch_items.frozen_resolution_payload_json AS frozen_resolution_payload_json,
    public.pay_batch_items.frozen_resolution_result_json AS frozen_resolution_result_json,
    public.pay_batch_items.payout_instruction_snapshot_json AS payout_instruction_snapshot_json,
    public.pay_finance_case_components.classification::text AS finance_component_classification,
    public.pay_finance_case_components.source_pay_method AS finance_component_source_pay_method,
    public.pay_finance_case_components.saved_target_pay_method AS finance_component_saved_target_pay_method,
    public.pay_finance_case_components.saved_resolution_mode::text AS finance_component_saved_resolution_mode,
    public.pay_finance_case_components.is_resolution_stale AS finance_component_is_resolution_stale,
    public.pay_finance_case_components.stale_reason AS finance_component_stale_reason,
    public.pay_finance_case_components.closed_at_utc AS finance_component_closed_at_utc,
    public.pay_advances.case_type::text AS finance_case_type,
    public.pay_advances.advance_kind::text AS finance_advance_kind,
    public.pay_advances.status::text AS finance_case_status,
    public.pay_advances.payout_status::text AS finance_payout_status,
    public.pay_advances.taxability::text AS finance_taxability,
    public.pay_advances.routing_kind::text AS finance_routing_kind,
    public.pay_advances.written_off_at_utc AS finance_written_off_at_utc,
    public.pay_advances.cleared_at_utc AS finance_cleared_at_utc
  FROM pg_temp._tmp_payment_correction_plan_selected AS plan_selected
  JOIN public.pay_batch_items
    ON public.pay_batch_items.id = plan_selected.pay_batch_item_id
  JOIN public.candidates
    ON public.candidates.id = plan_selected.candidate_id
  LEFT JOIN public.pay_finance_case_components
    ON public.pay_finance_case_components.id = plan_selected.finance_component_id
  LEFT JOIN public.pay_advances
    ON public.pay_advances.id = plan_selected.finance_case_id;

  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (pay_bank_transfer_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (finance_case_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (finance_component_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (reservation_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_detail (timesheet_id, economic_key_type, economic_key_value);

  v_classification_result := public.pay_payment_cancelability_diagnostic(
    p_pay_batch_id,
    p_selection_json,
    p_actor_user_id,
    'CORRECTION_REVIEW'
  );

  v_classification := COALESCE(
    NULLIF(btrim(v_classification_result->>'payment_lifecycle_state'), ''),
    NULLIF(btrim(v_classification_result->>'classification'), ''),
    'AMBIGUOUS_REVIEW_REQUIRED'
  );

  v_recommended_action := COALESCE(
    NULLIF(btrim(v_classification_result->>'recommended_action'), ''),
    CASE v_classification
      WHEN 'LOCAL_PREPARED_NOT_SENT' THEN 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
      WHEN 'SCHEDULED_LOCAL_NOT_SENT' THEN 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
      WHEN 'CANCELLED_BEFORE_BANK_SUBMISSION' THEN 'VIEW_RECALCULATE_NEXT_STEP'
      WHEN 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION' THEN 'VIEW_RECALCULATE_NEXT_STEP'
      WHEN 'FINANCIALS_REWOUND' THEN 'VIEW_RECALCULATE_NEXT_STEP'
      WHEN 'PROVIDER_CANCELLED_NO_MONEY' THEN 'NO_MONEY_UNWIND_AND_RECALCULATE'
      WHEN 'PROVIDER_FAILED_NO_MONEY' THEN 'NO_MONEY_UNWIND_AND_RECALCULATE'
      WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'RETRY_PROVIDER_LATER'
      WHEN 'PROVIDER_SUBMITTED_PENDING' THEN 'CHECK_PROVIDER_STATUS'
      WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'CHECK_PROVIDER_STATUS'
      WHEN 'PAID_OR_SETTLED' THEN 'AMEND_AND_RECOVER_OVERPAYMENT'
      ELSE 'CHECK_PROVIDER_STATUS'
    END
  );

  v_hard_blockers := COALESCE(v_classification_result->'blockers', '[]'::jsonb);

  IF jsonb_array_length(COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb)) > 0 THEN
    IF COALESCE(NULLIF(v_classification_result->>'source_restoration_investigation_required', '')::boolean, false) THEN
      v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
        'code', 'SOURCE_RESTORATION_INVESTIGATION_REQUIRED_AFTER_CANCELLATION',
        'message', 'Cancellation may continue for this proved-unpaid complete scope; ambiguous source-less rows remain frozen investigation evidence and are not reconstructed.',
        'source_less_ambiguous_count', COALESCE(NULLIF(v_classification_result->>'source_less_ambiguous_count', '')::integer, 0),
        'policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'
      ));
    ELSE
      v_hard_blockers := v_hard_blockers || COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb);
    END IF;
  END IF;

  v_warnings := COALESCE(v_classification_result->'warnings', '[]'::jsonb);

  IF v_classification = 'AMBIGUOUS_REVIEW_REQUIRED'
     AND jsonb_array_length(v_hard_blockers) = 0 THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CLASSIFICATION_AMBIGUOUS_REVIEW_REQUIRED',
      'message', 'Payment correction action cannot be planned until provider/bank state is clarified.'
    ));
  END IF;

  SELECT
    count(*)::integer,
    count(DISTINCT plan_detail.candidate_id)::integer,
    count(DISTINCT plan_detail.pay_bank_transfer_id) FILTER (WHERE plan_detail.pay_bank_transfer_id IS NOT NULL)::integer,
    count(DISTINCT plan_detail.umbrella_id) FILTER (WHERE plan_detail.umbrella_id IS NOT NULL)::integer,
    count(*) FILTER (WHERE plan_detail.key_resolution_failure_reason IS NOT NULL OR plan_detail.economic_key_type IS NULL OR plan_detail.economic_key_value IS NULL)::integer,
    count(*) FILTER (WHERE COALESCE(plan_detail.already_corrected, false))::integer,
    count(*) FILTER (WHERE COALESCE(plan_detail.is_voided, false))::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_detail.item_type, ''))) IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
    )::integer,
    count(*) FILTER (
      WHERE plan_detail.finance_case_id IS NOT NULL
        AND COALESCE(plan_detail.finance_component_classification, plan_detail.frozen_component_classification, '') IN ('REIMBURSEMENT_GROSS_FIXED', 'NET_PAY_FIXED_RECOVERY')
    )::integer,
    count(*) FILTER (
      WHERE (
        COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
        OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
        OR (
          COALESCE(plan_detail.finance_taxability, '') = 'TAXABLE'
          AND plan_detail.finance_case_id IS NOT NULL
          AND (
            COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
            OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
            OR NULLIF(btrim(COALESCE(plan_detail.frozen_resolution_mode, '')), '') IS NOT NULL
            OR NULLIF(btrim(COALESCE(plan_detail.finance_component_saved_resolution_mode, '')), '') IS NOT NULL
          )
        )
      )
    )::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method, ''))) IS DISTINCT FROM upper(btrim(COALESCE(plan_detail.frozen_target_pay_method, plan_detail.frozen_source_pay_method, plan_detail.pay_channel, '')))
        AND NULLIF(btrim(COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method, '')), '') IS NOT NULL
        AND NULLIF(btrim(COALESCE(plan_detail.frozen_target_pay_method, plan_detail.frozen_source_pay_method, plan_detail.pay_channel, '')), '') IS NOT NULL
    )::integer,
    count(*) FILTER (
      WHERE plan_detail.umbrella_id IS NOT NULL
        AND plan_detail.live_candidate_umbrella_id IS DISTINCT FROM plan_detail.umbrella_id
    )::integer,
    round(COALESCE(sum(COALESCE(plan_detail.amount_ex_vat, 0)), 0), 2)::numeric,
    round(COALESCE(sum(COALESCE(plan_detail.amount_vat, 0)), 0), 2)::numeric,
    round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)::numeric
  INTO
    v_selected_item_count,
    v_selected_candidate_count,
    v_selected_transfer_count,
    v_selected_umbrella_count,
    v_key_resolution_failure_count,
    v_already_corrected_count,
    v_voided_count,
    v_timesheet_item_count,
    v_net_fixed_finance_item_count,
    v_gross_channel_sensitive_item_count,
    v_pay_channel_change_count,
    v_umbrella_change_count,
    v_total_amount_ex_vat,
    v_total_amount_vat,
    v_total_amount_inc_vat
  FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail;

  v_draft_removal_requested := COALESCE(NULLIF(btrim(p_selection_json->>'source_context'), ''), '') = 'DRAFT_REMOVE_FROM_BATCH'
    AND COALESCE(NULLIF(btrim(p_selection_json->>'requested_action'), ''), '') = 'CANCEL_PAYMENT_ATTEMPT';

  IF v_draft_removal_requested THEN
    IF v_selected_item_count <= 0 THEN
      v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'DRAFT_REMOVE_SELECTED_ITEM_REQUIRED',
        'message', 'Draft removal requires at least one selected non-voided frozen batch item.',
        'pay_batch_id', p_pay_batch_id::text
      ));
    END IF;

    IF NULLIF(btrim(COALESCE(p_selection_json->>'expected_item_count', '')), '') IS NOT NULL THEN
      IF (p_selection_json->>'expected_item_count') !~ '^\d+$' THEN
        v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
          'code', 'DRAFT_REMOVE_EXPECTED_ITEM_COUNT_INVALID',
          'message', 'Draft removal expected_item_count must be a whole number.',
          'pay_batch_id', p_pay_batch_id::text,
          'expected_item_count', p_selection_json->>'expected_item_count'
        ));
      ELSE
        v_expected_item_count := (p_selection_json->>'expected_item_count')::integer;
        IF v_expected_item_count IS DISTINCT FROM v_selected_item_count THEN
          v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
            'code', 'DRAFT_REMOVE_SELECTION_DRIFT',
            'message', 'Draft removal selection has changed since the UI built the request.',
            'pay_batch_id', p_pay_batch_id::text,
            'expected_item_count', v_expected_item_count,
            'selected_item_count', v_selected_item_count
          ));
        END IF;
      END IF;
    END IF;

    IF p_selection_json ? 'expected_pay_batch_item_ids' THEN
      IF COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
        v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
          'code', 'DRAFT_REMOVE_EXPECTED_ITEM_IDS_INVALID',
          'message', 'Draft removal expected_pay_batch_item_ids must be an array.',
          'pay_batch_id', p_pay_batch_id::text
        ));
      ELSE
        SELECT
          count(*)::integer,
          COALESCE(sum(CASE WHEN NOT EXISTS (
            SELECT 1
            FROM pg_temp._tmp_payment_correction_plan_selected AS selected_item_rows
            WHERE selected_item_rows.pay_batch_item_id::text = expected_item_ids.value
          ) THEN 1 ELSE 0 END), 0)::integer
        INTO
          v_expected_item_id_count,
          v_expected_item_mismatch_count
        FROM jsonb_array_elements_text(p_selection_json->'expected_pay_batch_item_ids') AS expected_item_ids(value);

        IF v_expected_item_id_count IS DISTINCT FROM v_selected_item_count
           OR COALESCE(v_expected_item_mismatch_count, 0) > 0 THEN
          v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
            'code', 'DRAFT_REMOVE_SELECTION_DRIFT',
            'message', 'Draft removal selected item ids have changed since the UI built the request.',
            'pay_batch_id', p_pay_batch_id::text,
            'expected_item_id_count', v_expected_item_id_count,
            'selected_item_count', v_selected_item_count,
            'mismatch_count', v_expected_item_mismatch_count
          ));
        END IF;
      END IF;
    END IF;

    IF v_recommended_action <> 'PRE_PROVIDER_CANCEL_AND_RECALCULATE' THEN
      v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'DRAFT_REMOVE_CLASSIFICATION_MISMATCH',
        'message', 'Draft removal is only allowed while the selected payment is still a strict pre-bank cancel.',
        'pay_batch_id', p_pay_batch_id::text,
        'classification', v_classification
      ));
    END IF;

    IF upper(btrim(COALESCE(v_batch_status, ''))) NOT IN ('DRAFT', 'DRAFT_CREATED')
       OR upper(btrim(COALESCE(v_batch_execution_commit_state, ''))) IN ('SUBMITTED_NOT_COMMITTED', 'COMMITTED', 'SETTLED', 'CANCELLED') THEN
      v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'DRAFT_REMOVE_NO_LONGER_PRE_BANK',
        'message', 'Draft removal is only allowed before the batch is submitted, executed, settled, cancelled, or committed.',
        'pay_batch_id', p_pay_batch_id::text,
        'batch_status', v_batch_status,
        'execution_commit_state', v_batch_execution_commit_state
      ));
    END IF;
  END IF;

  v_strict_draft_removal_pre_bank_cancel := v_draft_removal_requested
    AND v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
    AND v_selected_item_count > 0
    AND upper(btrim(COALESCE(v_batch_status, ''))) IN ('DRAFT', 'DRAFT_CREATED')
    AND upper(btrim(COALESCE(v_batch_execution_commit_state, ''))) NOT IN ('SUBMITTED_NOT_COMMITTED', 'COMMITTED', 'SETTLED', 'CANCELLED')
    AND jsonb_array_length(v_hard_blockers) = 0;

  IF v_gross_channel_sensitive_item_count > 0
     AND NOT COALESCE(v_strict_draft_removal_pre_bank_cancel, false)
     AND v_recommended_action NOT IN ('PRE_PROVIDER_CANCEL_AND_RECALCULATE', 'NO_MONEY_UNWIND_AND_RECALCULATE') THEN
    v_suggested_resolution_required := true;
  END IF;

  SELECT COALESCE(jsonb_agg(candidate_rows.candidate_json ORDER BY candidate_rows.candidate_display_name), '[]'::jsonb)
  INTO v_affected_candidates
  FROM (
    SELECT
      plan_detail.candidate_display_name,
      jsonb_build_object(
        'pay_batch_candidate_id', plan_detail.pay_batch_candidate_id,
        'candidate_id', plan_detail.candidate_id,
        'candidate_display_name', plan_detail.candidate_display_name,
        'candidate_tms_ref', plan_detail.candidate_tms_ref,
        'current_candidate_pay_method', COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method),
        'current_umbrella_id', plan_detail.live_candidate_umbrella_id,
        'item_count', count(*)::integer,
        'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
      ) AS candidate_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    GROUP BY
      plan_detail.pay_batch_candidate_id,
      plan_detail.candidate_id,
      plan_detail.candidate_display_name,
      plan_detail.candidate_tms_ref,
      COALESCE(plan_detail.current_candidate_pay_method, plan_detail.live_candidate_pay_method),
      plan_detail.live_candidate_umbrella_id
  ) AS candidate_rows;

  SELECT COALESCE(jsonb_agg(transfer_rows.transfer_json ORDER BY transfer_rows.transfer_group_key, transfer_rows.pay_bank_transfer_id), '[]'::jsonb)
  INTO v_affected_transfers
  FROM (
    SELECT
      plan_detail.pay_bank_transfer_id,
      plan_detail.transfer_group_key,
      jsonb_build_object(
        'pay_bank_transfer_id', plan_detail.pay_bank_transfer_id,
        'transfer_status', plan_detail.transfer_status,
        'transfer_amount', plan_detail.transfer_amount,
        'transfer_group_key', plan_detail.transfer_group_key,
        'payee_entity_kind', plan_detail.payee_entity_kind,
        'payee_entity_id', plan_detail.payee_entity_id,
        'candidate_count', count(DISTINCT plan_detail.candidate_id)::integer,
        'item_count', count(*)::integer,
        'selected_amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
      ) AS transfer_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
    GROUP BY
      plan_detail.pay_bank_transfer_id,
      plan_detail.transfer_status,
      plan_detail.transfer_amount,
      plan_detail.transfer_group_key,
      plan_detail.payee_entity_kind,
      plan_detail.payee_entity_id
  ) AS transfer_rows;

  SELECT COALESCE(jsonb_agg(umbrella_rows.umbrella_json ORDER BY umbrella_rows.umbrella_name), '[]'::jsonb)
  INTO v_affected_umbrellas
  FROM (
    SELECT
      plan_detail.umbrella_name,
      jsonb_build_object(
        'umbrella_id', plan_detail.umbrella_id,
        'umbrella_name', plan_detail.umbrella_name,
        'candidate_count', count(DISTINCT plan_detail.candidate_id)::integer,
        'transfer_count', count(DISTINCT plan_detail.pay_bank_transfer_id) FILTER (WHERE plan_detail.pay_bank_transfer_id IS NOT NULL)::integer,
        'item_count', count(*)::integer,
        'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
      ) AS umbrella_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.umbrella_id IS NOT NULL
    GROUP BY plan_detail.umbrella_id, plan_detail.umbrella_name
  ) AS umbrella_rows;

  SELECT COALESCE(jsonb_agg(item_rows.item_json ORDER BY item_rows.candidate_display_name, item_rows.pay_batch_item_id), '[]'::jsonb)
  INTO v_affected_items
  FROM (
    SELECT
      plan_detail.candidate_display_name,
      plan_detail.pay_batch_item_id,
      jsonb_build_object(
        'pay_batch_item_id', plan_detail.pay_batch_item_id,
        'pay_batch_candidate_id', plan_detail.pay_batch_candidate_id,
        'candidate_id', plan_detail.candidate_id,
        'candidate_display_name', plan_detail.candidate_display_name,
        'item_type', plan_detail.item_type,
        'timesheet_id', plan_detail.timesheet_id,
        'pay_bank_transfer_id', plan_detail.pay_bank_transfer_id,
        'finance_case_id', plan_detail.finance_case_id,
        'finance_component_id', plan_detail.finance_component_id,
        'reservation_id', plan_detail.reservation_id,
        'economic_key_type', plan_detail.economic_key_type,
        'economic_key_value', plan_detail.economic_key_value,
        'key_resolution_source', plan_detail.key_resolution_source,
        'key_resolution_failure_reason', plan_detail.key_resolution_failure_reason,
        'amount_ex_vat', plan_detail.amount_ex_vat,
        'amount_vat', plan_detail.amount_vat,
        'amount_inc_vat', plan_detail.amount_inc_vat,
        'is_voided', plan_detail.is_voided,
        'already_corrected', plan_detail.already_corrected,
        'applied_correction_kinds', plan_detail.applied_correction_kinds
      ) AS item_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
  ) AS item_rows;

  SELECT COALESCE(jsonb_agg(finance_case_rows.finance_case_json ORDER BY finance_case_rows.finance_case_id), '[]'::jsonb)
  INTO v_affected_finance_cases
  FROM (
    SELECT
      plan_detail.finance_case_id,
      jsonb_build_object(
        'finance_case_id', plan_detail.finance_case_id,
        'candidate_id', min(plan_detail.candidate_id::text),
        'case_type', max(plan_detail.finance_case_type),
        'advance_kind', max(plan_detail.finance_advance_kind),
        'status', max(plan_detail.finance_case_status),
        'payout_status', max(plan_detail.finance_payout_status),
        'taxability', max(plan_detail.finance_taxability),
        'routing_kind', max(plan_detail.finance_routing_kind),
        'written_off_at_utc', max(plan_detail.finance_written_off_at_utc),
        'cleared_at_utc', max(plan_detail.finance_cleared_at_utc),
        'component_ids', COALESCE(jsonb_agg(DISTINCT plan_detail.finance_component_id) FILTER (WHERE plan_detail.finance_component_id IS NOT NULL), '[]'::jsonb),
        'reservation_ids', COALESCE(jsonb_agg(DISTINCT plan_detail.reservation_id) FILTER (WHERE plan_detail.reservation_id IS NOT NULL), '[]'::jsonb),
        'item_count', count(*)::integer,
        'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2),
        'requires_suggested_resolution', bool_or(
          COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR (
            COALESCE(plan_detail.finance_taxability, '') = 'TAXABLE'
            AND (
              NULLIF(btrim(COALESCE(plan_detail.frozen_resolution_mode, '')), '') IS NOT NULL
              OR NULLIF(btrim(COALESCE(plan_detail.finance_component_saved_resolution_mode, '')), '') IS NOT NULL
            )
          )
        )
      ) AS finance_case_json
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_case_id IS NOT NULL
    GROUP BY plan_detail.finance_case_id
  ) AS finance_case_rows;

  v_amounts := jsonb_build_object(
    'amount_ex_vat', v_total_amount_ex_vat,
    'amount_vat', v_total_amount_vat,
    'amount_inc_vat', v_total_amount_inc_vat,
    'by_pay_channel', COALESCE((
      SELECT jsonb_agg(channel_rows.channel_json ORDER BY channel_rows.pay_channel)
      FROM (
        SELECT
          COALESCE(plan_detail.pay_channel, 'UNKNOWN') AS pay_channel,
          jsonb_build_object(
            'pay_channel', COALESCE(plan_detail.pay_channel, 'UNKNOWN'),
            'item_count', count(*)::integer,
            'amount_ex_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_ex_vat, 0)), 0), 2),
            'amount_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_vat, 0)), 0), 2),
            'amount_inc_vat', round(COALESCE(sum(COALESCE(plan_detail.amount_inc_vat, 0)), 0), 2)
          ) AS channel_json
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        GROUP BY COALESCE(plan_detail.pay_channel, 'UNKNOWN')
      ) AS channel_rows
    ), '[]'::jsonb)
  );

  IF v_pay_channel_change_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'PAY_CHANNEL_CHANGED_AFTER_BATCH',
      'message', 'Correction will use the original frozen batch artifact. Any future replacement payment will use the candidate current live pay channel.',
      'affected_item_count', v_pay_channel_change_count
    ));
  END IF;

  IF v_umbrella_change_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'CANDIDATE_UMBRELLA_CHANGED_AFTER_BATCH',
      'message', 'One or more selected items were frozen against a different umbrella/payment group from the candidate current umbrella. Correction uses the frozen batch artifact; future replacement payment uses the current live umbrella routing.',
      'affected_item_count', v_umbrella_change_count
    ));
  END IF;

  IF v_timesheet_item_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'TIMESHEET_CORRECTION_USES_FROZEN_BATCH_METHOD',
      'message', 'Timesheet correction follows the old frozen batch artifact and does not recalculate the old payment from current live timesheet financials.',
      'affected_item_count', v_timesheet_item_count
    ));
  END IF;

  IF v_net_fixed_finance_item_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'NET_FIXED_FINANCE_ITEMS_NO_CHANNEL_CONVERSION',
      'message', 'Net/fixed finance items do not require PAYE/Umbrella amount conversion. Reservation and payout state still need correction handling.',
      'affected_item_count', v_net_fixed_finance_item_count
    ));
  END IF;

  IF v_suggested_resolution_required THEN
    v_suggested_resolution_finance_cases := '[]'::jsonb;

    FOR v_finance_case_record IN
      SELECT
        plan_detail.finance_case_id AS finance_case_id,
        (array_agg(DISTINCT plan_detail.candidate_id ORDER BY plan_detail.candidate_id))[1] AS candidate_id,
        COALESCE(jsonb_agg(DISTINCT plan_detail.finance_component_id) FILTER (WHERE plan_detail.finance_component_id IS NOT NULL), '[]'::jsonb) AS selected_component_ids
      FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
      WHERE plan_detail.finance_case_id IS NOT NULL
        AND (
          COALESCE(plan_detail.finance_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR COALESCE(plan_detail.frozen_component_classification, '') = 'TAXABLE_CHANNEL_SENSITIVE'
          OR (
            COALESCE(plan_detail.finance_taxability, '') = 'TAXABLE'
            AND (
              NULLIF(btrim(COALESCE(plan_detail.frozen_resolution_mode, '')), '') IS NOT NULL
              OR NULLIF(btrim(COALESCE(plan_detail.finance_component_saved_resolution_mode, '')), '') IS NOT NULL
            )
          )
        )
      GROUP BY plan_detail.finance_case_id
      ORDER BY plan_detail.finance_case_id
    LOOP
      SELECT
        COALESCE(jsonb_agg(finance_components.id ORDER BY finance_components.id), '[]'::jsonb),
        COALESCE(jsonb_object_agg(
          finance_components.id::text,
          COALESCE(
            NULLIF(btrim(finance_components.resolution_fingerprint), ''),
            md5(jsonb_build_object(
              'finance_component_id', finance_components.id,
              'finance_case_id', finance_components.finance_case_id,
              'classification', finance_components.classification::text,
              'source_pay_method', finance_components.source_pay_method,
              'source_amount', finance_components.source_amount,
              'remaining_source_amount', finance_components.remaining_source_amount,
              'saved_target_pay_method', finance_components.saved_target_pay_method,
              'saved_resolution_mode', finance_components.saved_resolution_mode::text,
              'saved_resolution_payload_json', finance_components.saved_resolution_payload_json,
              'saved_resolution_result_json', finance_components.saved_resolution_result_json,
              'is_resolution_stale', finance_components.is_resolution_stale,
              'closed_at_utc', finance_components.closed_at_utc,
              'updated_at_utc', finance_components.updated_at_utc
            )::text)
          )
        ), '{}'::jsonb)
      INTO
        v_case_component_ids,
        v_case_component_fingerprints
      FROM public.pay_finance_case_components AS finance_components
      WHERE finance_components.finance_case_id = v_finance_case_record.finance_case_id
        AND finance_components.id IN (
          SELECT DISTINCT plan_detail.finance_component_id
          FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
          WHERE plan_detail.finance_case_id = v_finance_case_record.finance_case_id
            AND plan_detail.finance_component_id IS NOT NULL
        );

      v_case_suggestion := NULL::jsonb;
      v_case_suggestion_hash := NULL::text;
      v_case_generation_error := NULL::jsonb;

      IF p_actor_user_id IS NULL THEN
        v_case_generation_error := jsonb_build_object(
          'code', 'ACTOR_USER_ID_REQUIRED_FOR_SUGGESTED_RESOLUTION',
          'message', 'A real taxable channel restructure suggestion requires an actor user id because the existing suggested-resolution function requires p_actor_user_id.'
        );

        v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
          'code', 'ACTOR_USER_ID_REQUIRED_FOR_SUGGESTED_RESOLUTION',
          'message', 'Gross/taxable/channel-sensitive finance items require a suggested resolution, but no actor user id was supplied to generate it.',
          'finance_case_id', v_finance_case_record.finance_case_id
        ));
      ELSE
        BEGIN
          v_case_suggestion := public.pay_finance_case_taxable_channel_restructure_suggestion(
            p_finance_case_id => v_finance_case_record.finance_case_id,
            p_actor_user_id => p_actor_user_id,
            p_effective_pay_date => v_effective_pay_date,
            p_resolution_path => 'SUGGESTED',
            p_schedule_input_mode => NULL::text,
            p_weeks_total => NULL::integer,
            p_weekly_due => NULL::numeric,
            p_manual_total_remaining => NULL::numeric,
            p_note => 'Generated for payment correction plan ' || p_pay_batch_id::text
          );

          v_case_suggestion_hash := md5(jsonb_build_object(
            'finance_case_id', v_finance_case_record.finance_case_id,
            'candidate_id', v_finance_case_record.candidate_id,
            'component_ids', v_case_component_ids,
            'selected_component_ids', v_finance_case_record.selected_component_ids,
            'component_fingerprints', v_case_component_fingerprints,
            'effective_pay_date', v_effective_pay_date,
            'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure',
            'resolution_path', COALESCE(v_case_suggestion->>'resolution_path', v_case_suggestion#>>'{request,resolution_path}', 'SUGGESTED'),
            'resolution_mode', COALESCE(v_case_suggestion->>'resolution_mode', v_case_suggestion#>>'{result,resolution_mode}', v_case_suggestion#>>'{suggestion,resolution_mode}'),
            'weeks_total', COALESCE(v_case_suggestion->>'weeks_total', v_case_suggestion#>>'{result,weeks_total}', v_case_suggestion#>>'{suggestion,weeks_total}'),
            'weekly_due', COALESCE(v_case_suggestion->>'weekly_due', v_case_suggestion#>>'{result,weekly_due}', v_case_suggestion#>>'{suggestion,weekly_due}'),
            'manual_total_remaining', COALESCE(v_case_suggestion->>'manual_total_remaining', v_case_suggestion#>>'{result,manual_total_remaining}', v_case_suggestion#>>'{suggestion,manual_total_remaining}'),
            'taxable_channel_result', COALESCE(
              v_case_suggestion->'taxable_channel_result',
              v_case_suggestion->'result',
              v_case_suggestion->'suggestion',
              v_case_suggestion
            ) - 'generated_at'
              - 'generated_at_utc'
              - 'created_at'
              - 'created_at_utc'
              - 'updated_at'
              - 'updated_at_utc'
              - 'audit'
              - 'debug'
          )::text);
        EXCEPTION
          WHEN OTHERS THEN
            v_case_generation_error := jsonb_build_object(
              'code', 'SUGGESTED_RESOLUTION_GENERATION_FAILED',
              'sqlstate', SQLSTATE,
              'message', SQLERRM
            );

            v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
              'code', 'SUGGESTED_RESOLUTION_GENERATION_FAILED',
              'message', 'A gross/taxable/channel-sensitive finance suggested resolution could not be generated using the existing suggested-resolution process.',
              'finance_case_id', v_finance_case_record.finance_case_id,
              'sqlstate', SQLSTATE,
              'error_message', SQLERRM
            ));
        END;
      END IF;

      v_suggested_resolution_finance_cases := v_suggested_resolution_finance_cases || jsonb_build_array(jsonb_build_object(
        'finance_case_id', v_finance_case_record.finance_case_id,
        'candidate_id', v_finance_case_record.candidate_id,
        'component_ids', v_case_component_ids,
        'selected_component_ids', v_finance_case_record.selected_component_ids,
        'current_component_fingerprints', v_case_component_fingerprints,
        'suggestion', v_case_suggestion,
        'suggestion_hash', v_case_suggestion_hash,
        'suggestion_hash_basis', jsonb_build_object(
          'finance_case_id', v_finance_case_record.finance_case_id,
          'candidate_id', v_finance_case_record.candidate_id,
          'component_ids', v_case_component_ids,
          'selected_component_ids', v_finance_case_record.selected_component_ids,
          'component_fingerprints', v_case_component_fingerprints,
          'effective_pay_date', v_effective_pay_date,
          'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure'
        ),
        'suggestion_generation_error', v_case_generation_error,
        'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure',
        'effective_pay_date', v_effective_pay_date,
        'accepted_payload_required', true
      ));
    END LOOP;

    v_suggested_resolution := jsonb_build_object(
      'required', true,
      'reason', 'Gross/taxable/channel-sensitive finance items are present and must use the existing suggested-resolution process before correction apply.',
      'must_be_accepted_before_apply', true,
      'must_be_applied_atomically_with_correction', true,
      'apply_surface', 'pay_finance_case_apply_taxable_channel_restructure',
      'finance_cases', v_suggested_resolution_finance_cases
    );

    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SUGGESTED_RESOLUTION_REQUIRED',
      'message', 'Gross/taxable/channel-sensitive finance items require an accepted suggested resolution before this correction request can be started.',
      'affected_item_count', v_gross_channel_sensitive_item_count,
      'suggested_resolution', v_suggested_resolution
    ));
  ELSE
    v_suggested_resolution := NULL::jsonb;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_overlap_items;
  CREATE TEMP TABLE _tmp_payment_correction_plan_overlap_items ON COMMIT DROP AS
  WITH selected_candidate_ids AS (
    SELECT DISTINCT plan_detail.candidate_id
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.candidate_id IS NOT NULL
  ),
  selected_economic_keys AS (
    SELECT DISTINCT
      plan_detail.candidate_id,
      plan_detail.timesheet_id,
      plan_detail.economic_key_type,
      plan_detail.economic_key_value
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.candidate_id IS NOT NULL
      AND plan_detail.timesheet_id IS NOT NULL
      AND plan_detail.economic_key_type IS NOT NULL
      AND plan_detail.economic_key_value IS NOT NULL
  ),
  selected_finance_scope AS (
    SELECT DISTINCT
      plan_detail.finance_case_id,
      plan_detail.finance_component_id,
      plan_detail.reservation_id
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_case_id IS NOT NULL
       OR plan_detail.finance_component_id IS NOT NULL
       OR plan_detail.reservation_id IS NOT NULL
  ),
  filtered_other_items AS (
    SELECT
      public.pay_batches.id AS other_pay_batch_id,
      public.pay_batches.status AS other_batch_status,
      public.pay_batches.created_at_utc AS other_batch_created_at_utc,
      public.pay_batches.execution_commit_state AS other_execution_commit_state,
      public.pay_batches.execution_commit_ref AS other_execution_commit_ref,
      public.pay_batch_candidates.candidate_id AS other_candidate_id,
      public.pay_batch_items.id AS other_pay_batch_item_id,
      public.pay_batch_items.pay_batch_candidate_id AS other_pay_batch_candidate_id,
      public.pay_batch_items.timesheet_id AS other_timesheet_id,
      public.pay_batch_items.finance_case_id AS other_finance_case_id,
      public.pay_batch_items.finance_component_id AS other_finance_component_id,
      public.pay_batch_items.reservation_id AS other_reservation_id,
      public.pay_batch_items.pay_bank_transfer_id AS other_pay_bank_transfer_id,
      other_economic_components.key_type AS other_economic_key_type,
      other_economic_components.key_value AS other_economic_key_value,
      public.pay_batch_candidates.settlement_status AS other_candidate_settlement_status,
      public.pay_batch_candidates.settled_at_utc AS other_candidate_settled_at_utc,
      public.pay_bank_transfers.status AS other_transfer_status,
      public.pay_bank_transfers.completed_at_utc AS other_transfer_completed_at_utc
    FROM public.pay_batches
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.pay_batch_id = public.pay_batches.id
    JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_batch_candidate_id = public.pay_batch_candidates.id
    JOIN selected_candidate_ids
      ON selected_candidate_ids.candidate_id = public.pay_batch_candidates.candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    LEFT JOIN LATERAL public._pay_batch_item_economic_components(NULL::uuid, ARRAY[public.pay_batch_items.id]) AS other_economic_components
      ON true
    WHERE public.pay_batches.id <> p_pay_batch_id
      AND COALESCE(public.pay_batch_items.is_voided, false) = false
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS other_applied_corrections
        WHERE other_applied_corrections.pay_batch_item_id = public.pay_batch_items.id
          AND other_applied_corrections.status = 'APPLIED'
      )
  )
  SELECT
    filtered_other_items.other_pay_batch_id,
    filtered_other_items.other_batch_status,
    filtered_other_items.other_batch_created_at_utc,
    filtered_other_items.other_execution_commit_state,
    filtered_other_items.other_execution_commit_ref,
    filtered_other_items.other_candidate_id,
    filtered_other_items.other_pay_batch_item_id,
    filtered_other_items.other_pay_batch_candidate_id,
    filtered_other_items.other_timesheet_id,
    filtered_other_items.other_finance_case_id,
    filtered_other_items.other_finance_component_id,
    filtered_other_items.other_reservation_id,
    filtered_other_items.other_pay_bank_transfer_id,
    filtered_other_items.other_economic_key_type,
    filtered_other_items.other_economic_key_value,
    filtered_other_items.other_candidate_settlement_status,
    filtered_other_items.other_candidate_settled_at_utc,
    filtered_other_items.other_transfer_status,
    filtered_other_items.other_transfer_completed_at_utc,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM selected_economic_keys
        WHERE selected_economic_keys.candidate_id = filtered_other_items.other_candidate_id
          AND selected_economic_keys.timesheet_id = filtered_other_items.other_timesheet_id
          AND selected_economic_keys.economic_key_type = filtered_other_items.other_economic_key_type
          AND selected_economic_keys.economic_key_value = filtered_other_items.other_economic_key_value
      ) THEN 'ECONOMIC_KEY'
      WHEN EXISTS (
        SELECT 1
        FROM selected_finance_scope
        WHERE selected_finance_scope.finance_case_id IS NOT NULL
          AND selected_finance_scope.finance_case_id = filtered_other_items.other_finance_case_id
      ) THEN 'FINANCE_CASE'
      WHEN EXISTS (
        SELECT 1
        FROM selected_finance_scope
        WHERE selected_finance_scope.finance_component_id IS NOT NULL
          AND selected_finance_scope.finance_component_id = filtered_other_items.other_finance_component_id
      ) THEN 'FINANCE_COMPONENT'
      WHEN EXISTS (
        SELECT 1
        FROM selected_finance_scope
        WHERE selected_finance_scope.reservation_id IS NOT NULL
          AND selected_finance_scope.reservation_id = filtered_other_items.other_reservation_id
      ) THEN 'RESERVATION'
      ELSE NULL
    END AS overlap_kind
  FROM filtered_other_items
  WHERE EXISTS (
      SELECT 1
      FROM selected_economic_keys
      WHERE selected_economic_keys.candidate_id = filtered_other_items.other_candidate_id
        AND selected_economic_keys.timesheet_id = filtered_other_items.other_timesheet_id
        AND selected_economic_keys.economic_key_type = filtered_other_items.other_economic_key_type
        AND selected_economic_keys.economic_key_value = filtered_other_items.other_economic_key_value
    )
    OR EXISTS (
      SELECT 1
      FROM selected_finance_scope
      WHERE selected_finance_scope.finance_case_id IS NOT NULL
        AND selected_finance_scope.finance_case_id = filtered_other_items.other_finance_case_id
    )
    OR EXISTS (
      SELECT 1
      FROM selected_finance_scope
      WHERE selected_finance_scope.finance_component_id IS NOT NULL
        AND selected_finance_scope.finance_component_id = filtered_other_items.other_finance_component_id
    )
    OR EXISTS (
      SELECT 1
      FROM selected_finance_scope
      WHERE selected_finance_scope.reservation_id IS NOT NULL
        AND selected_finance_scope.reservation_id = filtered_other_items.other_reservation_id
    );

  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_overlap_items (other_pay_batch_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_overlap_items (other_candidate_id);
  CREATE INDEX ON pg_temp._tmp_payment_correction_plan_overlap_items (overlap_kind);

  SELECT COALESCE(jsonb_agg(overlap_batches.overlap_json ORDER BY overlap_batches.other_batch_created_at_utc, overlap_batches.other_pay_batch_id), '[]'::jsonb)
  INTO v_draft_interference
  FROM (
    SELECT
      overlap_items.other_pay_batch_id,
      min(overlap_items.other_batch_created_at_utc) AS other_batch_created_at_utc,
      jsonb_build_object(
        'pay_batch_id', overlap_items.other_pay_batch_id,
        'status', max(overlap_items.other_batch_status),
        'execution_commit_state', max(overlap_items.other_execution_commit_state),
        'execution_commit_ref', max(overlap_items.other_execution_commit_ref),
        'overlap_kinds', COALESCE(jsonb_agg(DISTINCT overlap_items.overlap_kind) FILTER (WHERE overlap_items.overlap_kind IS NOT NULL), '[]'::jsonb),
        'candidate_count', count(DISTINCT overlap_items.other_candidate_id)::integer,
        'item_count', count(DISTINCT overlap_items.other_pay_batch_item_id)::integer,
        'message', 'This correction cannot be applied because draft batch ' || overlap_items.other_pay_batch_id::text || ' already reserves affected items under the current pay channel. Delete/cancel draft batch ' || overlap_items.other_pay_batch_id::text || ' first, then retry.'
      ) AS overlap_json
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE public._pay_batch_status_is_active_reservation(overlap_items.other_batch_status)
    GROUP BY overlap_items.other_pay_batch_id
  ) AS overlap_batches;

  IF jsonb_array_length(v_draft_interference) > 0 THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'OVERLAPPING_DRAFT_BATCH_RESERVES_AFFECTED_ITEMS',
      'message', 'This correction cannot be applied because another draft/reserved batch already reserves affected items. Delete/cancel the interfering draft batch first, then retry.',
      'draft_interference', v_draft_interference
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE overlap_items.other_batch_created_at_utc > v_batch_created_at_utc
      AND (
        upper(btrim(COALESCE(overlap_items.other_batch_status, ''))) IN ('SCHEDULED', 'EXECUTING', 'WAITING_BANK_CONFIRM', 'AUTHORISED_FOR_PAYMENT', 'AWAITING_AUTHORISATION')
        OR upper(btrim(COALESCE(overlap_items.other_execution_commit_state, ''))) IN ('COMMITTED', 'SUBMITTED')
        OR NULLIF(btrim(COALESCE(overlap_items.other_execution_commit_ref, '')), '') IS NOT NULL
      )
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'LATER_AUTHORISED_OR_SUBMITTED_BATCH_OVERLAP',
      'message', 'A later authorised/submitted batch overlaps the selected correction scope. Manual review is required before correction can apply.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE overlap_items.other_batch_created_at_utc > v_batch_created_at_utc
      AND (
        upper(btrim(COALESCE(overlap_items.other_batch_status, ''))) IN ('FAILED', 'PARTIAL')
      )
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'LATER_FAILED_OR_AMBIGUOUS_BATCH_OVERLAP',
      'message', 'A later failed or partial batch overlaps the selected correction scope. Manual review is required.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_overlap_items AS overlap_items
    WHERE overlap_items.other_batch_created_at_utc > v_batch_created_at_utc
      AND (
        upper(btrim(COALESCE(overlap_items.other_candidate_settlement_status, ''))) = 'SETTLED'
        OR overlap_items.other_candidate_settled_at_utc IS NOT NULL
        OR upper(btrim(COALESCE(overlap_items.other_transfer_status, ''))) = 'COMPLETED'
        OR overlap_items.other_transfer_completed_at_utc IS NOT NULL
      )
  ) THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'LATER_SETTLED_BATCH_OVERLAP_WARNING',
      'message', 'A later settled batch overlaps the candidate/economic scope. It must remain untouched; this correction only affects the selected old frozen batch artifacts.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_written_off_at_utc IS NOT NULL
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'FINANCE_CASE_WRITTEN_OFF',
      'message', 'One or more selected finance cases have been written off and require manual finance review.'
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
    WHERE plan_detail.finance_component_is_resolution_stale IS TRUE
  ) THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'FINANCE_COMPONENT_RESOLUTION_STALE',
      'message', 'One or more selected finance component resolutions are stale and must be regenerated before correction apply.'
    ));
  END IF;

  SELECT jsonb_build_object(
    'scope_type', v_scope_type,
    'work_unit', COALESCE(NULLIF(btrim(COALESCE(p_selection_json->>'work_unit', '')), ''), v_scope_type, 'UNKNOWN'),
    'pay_batch_id', p_pay_batch_id::text,
    'pay_batch_ids', jsonb_build_array(p_pay_batch_id::text),
    'is_whole_batch', (
      v_scope_type = 'BATCH'
      AND NOT (COALESCE(p_selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_batch_candidate_id',
        'pay_batch_candidate_ids',
        'selected_pay_batch_candidate_ids',
        'candidate_id',
        'candidate_ids',
        'selected_candidate_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'umbrella_id',
        'umbrella_ids',
        'selected_umbrella_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'selected_candidate_scope_complete', (
      v_scope_type = 'CANDIDATES'
      AND NOT (COALESCE(p_selection_json, '{}'::jsonb) ?| ARRAY[
        'pay_batch_item_id',
        'pay_batch_item_ids',
        'selected_pay_batch_item_ids',
        'expected_pay_batch_item_ids',
        'pay_bank_transfer_id',
        'pay_bank_transfer_ids',
        'selected_pay_bank_transfer_ids',
        'finance_case_id',
        'finance_case_ids',
        'selected_finance_case_ids',
        'finance_component_id',
        'finance_component_ids',
        'selected_finance_component_ids',
        'reservation_id',
        'reservation_ids',
        'selected_reservation_ids',
        'payout_transfer_id',
        'payout_transfer_ids',
        'selected_payout_transfer_ids',
        'transfer_group_key',
        'transfer_group_keys',
        'selected_transfer_group_keys'
      ]::text[])
    ),
    'pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_batch_item_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_batch_item_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_batch_candidate_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_batch_candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'candidate_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.candidate_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.candidate_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'umbrella_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.umbrella_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.umbrella_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_case_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.finance_case_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.finance_case_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'finance_component_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.finance_component_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.finance_component_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'reservation_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.reservation_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.reservation_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'payout_transfer_ids', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.pay_bank_transfer_id::text AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE plan_detail.pay_bank_transfer_id IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb),
    'transfer_group_keys', COALESCE((
      SELECT jsonb_agg(selected_values.value_text ORDER BY selected_values.value_text)
      FROM (
        SELECT DISTINCT plan_detail.transfer_group_key AS value_text
        FROM pg_temp._tmp_payment_correction_plan_detail AS plan_detail
        WHERE NULLIF(btrim(COALESCE(plan_detail.transfer_group_key, '')), '') IS NOT NULL
      ) AS selected_values
    ), '[]'::jsonb)
  )
  INTO v_selected_mail_scope_json;

  -- Communication V2 is selected only from the backend's exact, complete,
  -- proved-unpaid cancellation classification. A caller's requested_action is
  -- never authority, and Draft removal deliberately retains the historical path.
  v_financial_cancellation_communication_v2 :=
    v_correction_context IN (
      'CORRECTION_ACTION',
      'PAYMENT_CORRECTION_ACTION',
      'USER_TRIGGERED_CORRECTION',
      'CURRENT_PAYMENT_STATUS_CORRECTION_ACTION',
      'PAYMENT_ISSUES_CORRECTION_ACTION'
    )
    AND v_scope_type IN ('BATCH', 'CANDIDATES')
    AND COALESCE(v_classification_result #>> '{resolved_full_payment_scope_json,scope_type}', '') = v_scope_type
    AND COALESCE(NULLIF(v_classification_result #>> '{resolved_full_payment_scope_json,is_full_scope}', '')::boolean, false)
    AND COALESCE(v_draft_removal_requested, false) IS NOT TRUE
    AND (
      (
        v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
        AND COALESCE(NULLIF(v_classification_result->>'can_pre_provider_cancel', '')::boolean, false)
      )
      OR (
        v_recommended_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'
        AND COALESCE(NULLIF(v_classification_result->>'can_no_money_unwind', '')::boolean, false)
      )
    );

  IF v_financial_cancellation_communication_v2 THEN
    -- Financial cancellation must never depend on mail state. This owner does
    -- not read mail and therefore cannot prove that the original payment notice
    -- was SENT: QUEUED is not SENT, and no follow-up notice is authorised here.
    v_queued_unsent_count := 0;
    v_sent_notice_count := 0;
    v_mail_legacy_review_count := 0;
    v_mail_legacy_queued_review_count := 0;
    v_communication_effects := jsonb_build_object(
      'queued_unsent_to_cancel', '[]'::jsonb,
      'legacy_broad_matches_requiring_review', '[]'::jsonb,
      'sent_to_leave_as_audit', '[]'::jsonb,
      'external_correction_notice', false,
      'admin_notice_required', true,
      'queued_unsent_count', 0,
      'sent_notice_count', 0,
      'legacy_broad_review_count', 0,
      'legacy_broad_queued_review_count', 0,
      'selected_scope_json', v_selected_mail_scope_json,
      'financial_cancellation_independent', true,
      'communication_cleanup_contract_version', 2,
      'mail_outbox_mutation_count', 0,
      'follow_up_cancellation_notice_permitted', false,
      'follow_up_cancellation_notice_requires_proved_original_sent', true
    );
  ELSE
  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_mail;
  CREATE TEMP TABLE _tmp_payment_correction_plan_mail ON COMMIT DROP AS
  WITH candidate_mail AS (
    SELECT
      public.mail_outbox.id,
      public.mail_outbox.type,
      public.mail_outbox."to" AS mail_to,
      public.mail_outbox.subject,
      public.mail_outbox.status::text AS status,
      public.mail_outbox.created_at_utc,
      public.mail_outbox.sent_at,
      public.mail_outbox.failed_at,
      public.mail_outbox.reference,
      public.mail_outbox.recipient_kind,
      public.mail_outbox.recipient_id,
      public.mail_outbox.context_kind,
      public.mail_outbox.context_id,
      public.mail_outbox.email_type,
      COALESCE(public.mail_outbox.payment_scope_json, '{}'::jsonb) AS payment_scope_json
    FROM public.mail_outbox
    WHERE upper(btrim(COALESCE(public.mail_outbox.status::text, ''))) IN ('QUEUED', 'SENT')
      AND lower(concat_ws('|', public.mail_outbox.type, public.mail_outbox.email_type, public.mail_outbox.context_kind, public.mail_outbox.reference, COALESCE(public.mail_outbox.payment_scope_json::text, '{}'))) LIKE ANY (
        ARRAY['%remittance%', '%payout%', '%pay_batch%', '%finance_payout%']
      )
  ), matched_mail AS (
    SELECT
      candidate_mail.id,
      candidate_mail.type,
      candidate_mail.mail_to,
      candidate_mail.subject,
      candidate_mail.status,
      candidate_mail.created_at_utc,
      candidate_mail.sent_at,
      candidate_mail.failed_at,
      candidate_mail.reference,
      candidate_mail.recipient_kind,
      candidate_mail.recipient_id,
      candidate_mail.context_kind,
      candidate_mail.context_id,
      candidate_mail.email_type,
      candidate_mail.payment_scope_json,
      mail_match.match_result
    FROM candidate_mail
    CROSS JOIN LATERAL (
      SELECT public._pay_payment_correction_mail_scope_match(
        candidate_mail.id,
        p_pay_batch_id,
        p_selection_json,
        v_selected_mail_scope_json,
        false
      ) AS match_result
    ) AS mail_match
  )
  SELECT
    matched_mail.id,
    matched_mail.type,
    matched_mail.mail_to,
    matched_mail.subject,
    matched_mail.status,
    matched_mail.created_at_utc,
    matched_mail.sent_at,
    matched_mail.failed_at,
    matched_mail.reference,
    matched_mail.recipient_kind,
    matched_mail.recipient_id,
    matched_mail.context_kind,
    matched_mail.context_id,
    matched_mail.email_type,
    matched_mail.payment_scope_json,
    COALESCE(matched_mail.match_result->>'match_kind', 'NONE') AS scope_match,
    COALESCE(matched_mail.match_result->>'match_confidence', 'NONE') AS match_confidence,
    COALESCE(NULLIF(matched_mail.match_result->>'safe_to_cancel', '')::boolean, false) AS safe_to_cancel,
    COALESCE(NULLIF(matched_mail.match_result->>'requires_review', '')::boolean, false) AS requires_review,
    COALESCE(matched_mail.match_result->>'reason', 'NO_SCOPE_MATCH') AS match_reason,
    matched_mail.match_result AS match_result
  FROM matched_mail
  WHERE COALESCE(NULLIF(matched_mail.match_result->>'matched', '')::boolean, false);

  SELECT
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    )::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'SENT'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    )::integer,
    count(*) FILTER (
      WHERE COALESCE(plan_mail.requires_review, false)
    )::integer,
    count(*) FILTER (
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED'
        AND COALESCE(plan_mail.requires_review, false)
    )::integer
  INTO
    v_queued_unsent_count,
    v_sent_notice_count,
    v_mail_legacy_review_count,
    v_mail_legacy_queued_review_count
  FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail;

  IF v_mail_legacy_queued_review_count > 0 THEN
    v_hard_blockers := v_hard_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'MAIL_SCOPE_LEGACY_BROAD_MATCH_REQUIRES_REVIEW',
      'message', 'One or more queued payment notices only match the selected payment scope by broad legacy candidate, umbrella, recipient, or reference data. Manual review is required before applying this correction.',
      'queued_notice_count', v_mail_legacy_queued_review_count
    ));
  ELSIF v_mail_legacy_review_count > 0 THEN
    v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
      'code', 'MAIL_SCOPE_LEGACY_BROAD_MATCH_REQUIRES_REVIEW',
      'message', 'One or more payment notices only match the selected payment scope by broad legacy candidate, umbrella, recipient, or reference data and require review.',
      'notice_count', v_mail_legacy_review_count
    ));
  END IF;

  v_communication_effects := jsonb_build_object(
    'queued_unsent_to_cancel', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'scope_match', plan_mail.scope_match,
        'match_confidence', plan_mail.match_confidence,
        'safe_to_cancel', plan_mail.safe_to_cancel,
        'requires_review', plan_mail.requires_review,
        'reason', plan_mail.match_reason,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference,
        'payment_scope_json', COALESCE(plan_mail.payment_scope_json, '{}'::jsonb)
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'QUEUED'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    ), '[]'::jsonb),
    'legacy_broad_matches_requiring_review', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'scope_match', plan_mail.scope_match,
        'match_confidence', plan_mail.match_confidence,
        'safe_to_cancel', plan_mail.safe_to_cancel,
        'requires_review', plan_mail.requires_review,
        'reason', plan_mail.match_reason,
        'status', plan_mail.status,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference,
        'payment_scope_json', COALESCE(plan_mail.payment_scope_json, '{}'::jsonb)
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE COALESCE(plan_mail.requires_review, false)
        AND COALESCE(plan_mail.safe_to_cancel, false) = false
    ), '[]'::jsonb),
    'sent_to_leave_as_audit', COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'mail_outbox_id', plan_mail.id,
        'scope_match', plan_mail.scope_match,
        'match_confidence', plan_mail.match_confidence,
        'safe_to_cancel', plan_mail.safe_to_cancel,
        'requires_review', plan_mail.requires_review,
        'reason', plan_mail.match_reason,
        'type', plan_mail.type,
        'to', plan_mail.mail_to,
        'subject', plan_mail.subject,
        'recipient_kind', plan_mail.recipient_kind,
        'recipient_id', plan_mail.recipient_id,
        'context_kind', plan_mail.context_kind,
        'context_id', plan_mail.context_id,
        'reference', plan_mail.reference,
        'payment_scope_json', COALESCE(plan_mail.payment_scope_json, '{}'::jsonb),
        'sent_at', plan_mail.sent_at
      ) ORDER BY plan_mail.created_at_utc, plan_mail.id)
      FROM pg_temp._tmp_payment_correction_plan_mail AS plan_mail
      WHERE upper(btrim(COALESCE(plan_mail.status, ''))) = 'SENT'
        AND COALESCE(plan_mail.safe_to_cancel, false)
        AND (
          plan_mail.match_confidence = 'EXACT'
          OR plan_mail.scope_match = 'WHOLE_BATCH'
        )
    ), '[]'::jsonb),
    'external_correction_notice', false,
    'admin_notice_required', true,
    'queued_unsent_count', v_queued_unsent_count,
    'sent_notice_count', v_sent_notice_count,
    'legacy_broad_review_count', v_mail_legacy_review_count,
    'legacy_broad_queued_review_count', v_mail_legacy_queued_review_count,
    'selected_scope_json', v_selected_mail_scope_json
  );

  END IF;

  v_work_item_count := CASE
    WHEN v_selected_transfer_count > 0
      AND v_recommended_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'
      THEN v_selected_transfer_count
    WHEN v_selected_candidate_count > 0
      THEN v_selected_candidate_count
    ELSE v_selected_item_count
  END;

  v_large_correction := jsonb_build_object(
    'large_correction', COALESCE(v_work_item_count, 0) > v_large_correction_threshold,
    'threshold', v_large_correction_threshold,
    'work_item_count', COALESCE(v_work_item_count, 0),
    'recommended_chunk_size', v_recommended_chunk_size
  );

  v_work_expansion_plan := jsonb_build_object(
    'recommended_action', v_recommended_action,
    'work_kind', NULL::text,
    'work_unit', CASE
      WHEN v_selected_transfer_count > 0 THEN 'TRANSFER'
      WHEN v_selected_candidate_count > 0 THEN 'CANDIDATE'
      ELSE 'ITEM'
    END,
    'work_item_count', COALESCE(v_work_item_count, 0),
    'process_synchronously', COALESCE(v_work_item_count, 0) <= v_large_correction_threshold,
    'recommended_chunk_size', v_recommended_chunk_size,
    'requires_work_queue', COALESCE(v_work_item_count, 0) > v_large_correction_threshold,
    'selected_pay_batch_item_ids', to_jsonb(v_selected_pay_batch_item_ids),
    'selected_selection_hash', v_selected_selection_hash,
    'chunking_plan', jsonb_build_object(
      'default_limit', v_recommended_chunk_size,
      'hard_cap', 100,
      'requires_chunking', COALESCE(v_work_item_count, 0) > v_large_correction_threshold
    )
  );

  v_selection_summary := jsonb_build_object(
    'scope_type', upper(nullif(btrim(COALESCE(p_selection_json->>'scope_type', '')), '')),
    'selected_pay_batch_item_ids', to_jsonb(v_selected_pay_batch_item_ids),
    'selected_selection_hash', v_selected_selection_hash,
    'selection_filters_applied', v_selection_filters_applied,
    'selected_item_count', v_selected_item_count,
    'selected_candidate_count', v_selected_candidate_count,
    'selected_transfer_count', v_selected_transfer_count,
    'selected_umbrella_count', v_selected_umbrella_count,
    'key_resolution_failure_count', v_key_resolution_failure_count,
    'already_corrected_count', v_already_corrected_count,
    'voided_count', v_voided_count,
    'pay_channel_change_count', v_pay_channel_change_count,
    'umbrella_change_count', v_umbrella_change_count,
    'gross_channel_sensitive_item_count', v_gross_channel_sensitive_item_count,
    'net_fixed_finance_item_count', v_net_fixed_finance_item_count
  );

  v_can_apply := (
    v_recommended_action IN ('PRE_PROVIDER_CANCEL_AND_RECALCULATE', 'NO_MONEY_UNWIND_AND_RECALCULATE')
    AND v_classification NOT IN ('AMBIGUOUS_REVIEW_REQUIRED', 'PAID_OR_SETTLED', 'PROVIDER_SUBMITTED_PENDING', 'PROVIDER_OUTCOME_UNKNOWN', 'PROVIDER_OUTAGE_RETRY_LATER')
    AND jsonb_array_length(v_hard_blockers) = 0
    AND v_selected_item_count > 0
    AND NOT v_suggested_resolution_required
  );

  v_retry_scope_signature := md5(jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'selection_hash', COALESCE(v_selected_selection_hash, 'NO_SELECTED_ITEMS'),
    'classification', v_classification,
    'recommended_action', v_recommended_action,
    'pay_channel_scope', COALESCE(p_selection_json->>'pay_channel_scope', p_selection_json->>'pay_channel', 'ALL'),
    'retry_contract', 'RETRY_UNSENT_PAYMENTS'
  )::text);

  v_retry_status_label := 'Bank unavailable — unsent payments can be retried';
  v_retry_button_label := 'Retry unsent payments';
  v_retry_running_label := 'Retrying unsent payments';
  v_recovery_action_label := 'Recover overpayment in next pay run';

  IF v_recommended_action = 'RETRY_PROVIDER_LATER'
     AND v_classification = 'PROVIDER_OUTAGE_RETRY_LATER'
     AND jsonb_array_length(v_hard_blockers) = 0
     AND v_selected_item_count > 0 THEN
    v_retry_eligible_count := CASE
      WHEN COALESCE(v_selected_transfer_count, 0) > 0 THEN v_selected_transfer_count
      WHEN COALESCE(v_selected_candidate_count, 0) > 0 THEN v_selected_candidate_count
      ELSE v_selected_item_count
    END;
    v_retry_ineligible_count := 0;
    v_retry_eligible_scope_json := jsonb_build_object(
      'scope_type', COALESCE(NULLIF(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''), 'BATCH'),
      'pay_batch_id', p_pay_batch_id::text,
      'retry_scope_signature', v_retry_scope_signature,
      'pay_batch_item_ids', COALESCE(v_selected_mail_scope_json->'pay_batch_item_ids', '[]'::jsonb),
      'pay_batch_candidate_ids', COALESCE(v_selected_mail_scope_json->'pay_batch_candidate_ids', '[]'::jsonb),
      'candidate_ids', COALESCE(v_selected_mail_scope_json->'candidate_ids', '[]'::jsonb),
      'pay_bank_transfer_ids', COALESCE(v_selected_mail_scope_json->'pay_bank_transfer_ids', '[]'::jsonb),
      'transfer_group_keys', COALESCE(v_selected_mail_scope_json->'transfer_group_keys', '[]'::jsonb),
      'reason', 'Backend-confirmed provider request was not sent; eligible for Retry unsent payments.'
    );
  ELSE
    v_retry_eligible_count := 0;
    v_retry_ineligible_count := CASE
      WHEN v_recommended_action = 'RETRY_PROVIDER_LATER' THEN GREATEST(COALESCE(v_selected_transfer_count, 0), COALESCE(v_selected_candidate_count, 0), COALESCE(v_selected_item_count, 0))
      ELSE 0
    END;
    v_retry_eligible_scope_json := jsonb_build_object(
      'scope_type', COALESCE(NULLIF(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''), 'BATCH'),
      'pay_batch_id', p_pay_batch_id::text,
      'retry_scope_signature', v_retry_scope_signature,
      'eligible', false
    );
    IF v_recommended_action = 'RETRY_PROVIDER_LATER' THEN
      v_retry_ineligible_summary_json := jsonb_build_array(jsonb_build_object(
        'code', 'RETRY_UNSENT_PAYMENTS_NOT_CURRENTLY_ELIGIBLE',
        'message', 'No selected payment scope is currently confirmed as unsent and retry-safe.',
        'classification', v_classification,
        'recommended_action', v_recommended_action
      ));
    END IF;
  END IF;

  SELECT operation_rows.id,
         operation_rows.status
  INTO v_retry_operation_id,
       v_retry_operation_status
  FROM public.banking_pay_operations AS operation_rows
  WHERE operation_rows.pay_batch_id = p_pay_batch_id
    AND upper(btrim(COALESCE(operation_rows.operation_type, ''))) = 'PAYMENT_RETRY_BLOCKED_FUNDS'
    AND upper(btrim(COALESCE(operation_rows.status, ''))) NOT IN ('COMPLETED', 'COMPLETE', 'SUCCEEDED', 'SUCCESS', 'FAILED', 'FAILED_FINAL', 'CANCELLED', 'CANCELED')
    AND (
      COALESCE(operation_rows.input_json->>'retry_scope_signature', '') = v_retry_scope_signature
      OR COALESCE(operation_rows.progress_json->>'retry_scope_signature', '') = v_retry_scope_signature
      OR COALESCE(operation_rows.result_json->>'retry_scope_signature', '') = v_retry_scope_signature
      OR COALESCE(v_retry_eligible_count, 0) > 0
    )
  ORDER BY operation_rows.created_at_utc DESC NULLS LAST, operation_rows.id DESC
  LIMIT 1;

  v_retry_in_progress := v_retry_operation_id IS NOT NULL;
  v_retry_already_in_progress := v_retry_in_progress;
  v_retry_button_visible := COALESCE(v_retry_eligible_count, 0) > 0 AND COALESCE(v_retry_in_progress, false) IS NOT TRUE;

  IF COALESCE(v_retry_in_progress, false) THEN
    v_retry_button_disabled_reason := 'Retry already in progress';
  ELSIF COALESCE(v_retry_eligible_count, 0) <= 0 AND v_recommended_action = 'RETRY_PROVIDER_LATER' THEN
    v_retry_button_disabled_reason := 'No unsent payments available to retry';
  ELSE
    v_retry_button_disabled_reason := NULL::text;
  END IF;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PLAN_RESULT',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'classification', v_classification,
      'recommended_action', v_recommended_action,
      'can_apply', v_can_apply,
      'selected_item_count', v_selected_item_count,
      'selected_candidate_count', v_selected_candidate_count,
      'selected_transfer_count', v_selected_transfer_count,
      'hard_blocker_count', jsonb_array_length(v_hard_blockers),
      'warning_count', jsonb_array_length(v_warnings),
      'suggested_resolution_required', v_suggested_resolution_required,
      'umbrella_change_count', v_umbrella_change_count,
      'work_item_count', v_work_item_count,
      'large_correction', COALESCE(v_work_item_count, 0) > v_large_correction_threshold,
      'retry_eligible_count', v_retry_eligible_count,
      'retry_in_progress', v_retry_in_progress,
      'retry_operation_id', CASE WHEN v_retry_operation_id IS NULL THEN NULL ELSE v_retry_operation_id::text END
    ),
    'pay_payment_correction',
    v_subject_id,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id,
    'classification', v_classification,
    'recommended_action', v_recommended_action,
    'user_facing_recommended_action', v_recommended_action,
    'next_step', CASE WHEN v_recommended_action = 'VIEW_RECALCULATE_NEXT_STEP' THEN 'AMEND_TIMESHEET_AND_RECALCULATE' ELSE NULL::text END,
    'next_step_label', CASE WHEN v_recommended_action = 'VIEW_RECALCULATE_NEXT_STEP' THEN 'Amend timesheets and recalculate' ELSE NULL::text END,
    'display_only_action', v_recommended_action = 'VIEW_RECALCULATE_NEXT_STEP',
    'action_label', CASE WHEN v_recommended_action = 'AMEND_AND_RECOVER_OVERPAYMENT' THEN v_recovery_action_label WHEN v_recommended_action = 'RETRY_PROVIDER_LATER' THEN v_retry_button_label ELSE NULL::text END,
    'status_label', CASE WHEN v_recommended_action = 'RETRY_PROVIDER_LATER' THEN v_retry_status_label ELSE NULL::text END,
    'button_label', CASE WHEN v_recommended_action = 'RETRY_PROVIDER_LATER' THEN v_retry_button_label ELSE NULL::text END,
    'running_label', CASE WHEN v_recommended_action = 'RETRY_PROVIDER_LATER' THEN v_retry_running_label ELSE NULL::text END,
    'recovery_action_label', CASE WHEN v_recommended_action = 'AMEND_AND_RECOVER_OVERPAYMENT' THEN v_recovery_action_label ELSE NULL::text END,
    'retry_eligible_count', COALESCE(v_retry_eligible_count, 0),
    'retry_ineligible_count', COALESCE(v_retry_ineligible_count, 0),
    'retry_eligible_scope_json', COALESCE(v_retry_eligible_scope_json, '{}'::jsonb),
    'retry_ineligible_summary_json', COALESCE(v_retry_ineligible_summary_json, '[]'::jsonb),
    'retry_in_progress', COALESCE(v_retry_in_progress, false),
    'retry_already_in_progress', COALESCE(v_retry_already_in_progress, false),
    'retry_operation_id', CASE WHEN v_retry_operation_id IS NULL THEN NULL ELSE v_retry_operation_id::text END,
    'retry_operation_status', v_retry_operation_status,
    'retry_button_visible', COALESCE(v_retry_button_visible, false),
    'retry_button_disabled_reason', v_retry_button_disabled_reason,
    'retry_scope_signature', v_retry_scope_signature,
    'payment_lifecycle_state', v_classification,
    'provider_failure_reason_code', COALESCE(NULLIF(btrim(v_classification_result->>'provider_failure_reason_code'), ''), NULLIF(btrim(v_classification_result#>>'{provider_evidence,provider_failure_reason_code}'), '')),
    'provider_failure_reason_group', COALESCE(NULLIF(btrim(v_classification_result->>'provider_failure_reason_group'), ''), NULLIF(btrim(v_classification_result#>>'{provider_evidence,provider_failure_reason_group}'), '')),
    'provider_failure_reason_label', COALESCE(NULLIF(btrim(v_classification_result->>'provider_failure_reason_label'), ''), NULLIF(btrim(v_classification_result#>>'{provider_evidence,provider_failure_reason_label}'), '')),
    'alert_candidate_kind', NULLIF(btrim(COALESCE(v_classification_result->>'alert_candidate_kind', '')), ''),
    'alert_candidate_severity', NULLIF(btrim(COALESCE(v_classification_result->>'alert_candidate_severity', '')), ''),
    'alert_candidate_is_success_only', COALESCE(NULLIF(v_classification_result->>'alert_candidate_is_success_only', '')::boolean, false),
    'live_status_signature', COALESCE(NULLIF(btrim(v_classification_result->>'live_status_signature'), ''), NULLIF(btrim(v_classification_result->>'status_update_signature'), ''), md5(jsonb_build_object('pay_batch_id', p_pay_batch_id::text, 'classification', v_classification, 'recommended_action', v_recommended_action, 'selection_hash', COALESCE(v_selected_selection_hash, ''))::text)),
    'status_update_signature', COALESCE(NULLIF(btrim(v_classification_result->>'status_update_signature'), ''), NULLIF(btrim(v_classification_result->>'live_status_signature'), ''), md5(jsonb_build_object('pay_batch_id', p_pay_batch_id::text, 'classification', v_classification, 'recommended_action', v_recommended_action, 'selection_hash', COALESCE(v_selected_selection_hash, ''))::text)),
    'can_apply', v_can_apply,
    'hard_blockers', v_hard_blockers,
    'warnings', v_warnings,
    'selection', v_selection_summary,
    'affected_candidates', v_affected_candidates,
    'affected_transfers', v_affected_transfers,
    'affected_umbrellas', v_affected_umbrellas,
    'affected_items', v_affected_items,
    'affected_finance_cases', v_affected_finance_cases,
    'communication_effects', v_communication_effects,
    'suggested_resolution_required', v_suggested_resolution_required,
    'suggested_resolution', v_suggested_resolution,
    'amounts', v_amounts
  ) || jsonb_build_object(
    'draft_interference', v_draft_interference,
    'large_correction', v_large_correction,
    'work_expansion_plan', v_work_expansion_plan,
    'chunking_plan', COALESCE(v_work_expansion_plan->'chunking_plan', '{}'::jsonb),
    'full_payment_scope', COALESCE(v_classification_result->'resolved_full_payment_scope_json', v_classification_result->'full_payment_scope', '{}'::jsonb),
    'resolved_full_payment_scope_json', COALESCE(v_classification_result->'resolved_full_payment_scope_json', v_classification_result->'full_payment_scope', '{}'::jsonb),
    'finance_scope', COALESCE(v_classification_result->'finance_scope_json', v_classification_result->'finance_scope', '{}'::jsonb),
    'finance_scope_json', COALESCE(v_classification_result->'finance_scope_json', v_classification_result->'finance_scope', '{}'::jsonb),
    'manual_adjustment_carry_forward_required', COALESCE(NULLIF(v_classification_result->>'manual_adjustment_carry_forward_required', '')::boolean, false),
    'manual_adjustments_to_carry_forward', COALESCE(v_classification_result->'manual_adjustments_to_carry_forward', '[]'::jsonb),
    'safe_carry_forward_items', COALESCE(v_classification_result->'manual_adjustments_to_carry_forward', '[]'::jsonb),
    'manual_adjustments_carried_forward_existing', COALESCE(v_classification_result->'manual_adjustments_carried_forward_existing', '[]'::jsonb),
    'ambiguous_manual_adjustment_blockers', COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb),
    'source_restoration_investigation_required', COALESCE(NULLIF(v_classification_result->>'source_restoration_investigation_required', '')::boolean, false),
    'source_less_ambiguous_count', COALESCE(NULLIF(v_classification_result->>'source_less_ambiguous_count', '')::integer, 0),
    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION',
    'provider_evidence', jsonb_build_object(
      'pending_provider_evidence_json', COALESCE(v_classification_result->'pending_provider_evidence_json', '{}'::jsonb),
      'terminal_no_money_evidence_json', COALESCE(v_classification_result->'terminal_no_money_evidence_json', '{}'::jsonb),
      'provider_outcome_unknown_evidence_json', COALESCE(v_classification_result->'provider_outcome_unknown_evidence_json', '{}'::jsonb),
      'provider_webhook_evidence_json', COALESCE(v_classification_result->'provider_webhook_evidence_json', '{}'::jsonb),
      'provider_failure_reason_code', COALESCE(NULLIF(btrim(v_classification_result->>'provider_failure_reason_code'), ''), NULLIF(btrim(v_classification_result#>>'{provider_evidence,provider_failure_reason_code}'), '')),
      'provider_failure_reason_group', COALESCE(NULLIF(btrim(v_classification_result->>'provider_failure_reason_group'), ''), NULLIF(btrim(v_classification_result#>>'{provider_evidence,provider_failure_reason_group}'), '')),
      'provider_failure_reason_label', COALESCE(NULLIF(btrim(v_classification_result->>'provider_failure_reason_label'), ''), NULLIF(btrim(v_classification_result#>>'{provider_evidence,provider_failure_reason_label}'), ''))
    ),
    'provider_webhook_evidence_json', COALESCE(v_classification_result->'provider_webhook_evidence_json', '{}'::jsonb),
    'provider_outcome_unknown_evidence_json', COALESCE(v_classification_result->'provider_outcome_unknown_evidence_json', '{}'::jsonb),
    'paid_evidence', COALESCE(v_classification_result->'blocking_paid_evidence_json', '{}'::jsonb),
    'race_submission_blockers', COALESCE((
      SELECT jsonb_agg(race_blockers.blocker_value ORDER BY race_blockers.blocker_ordinal)
      FROM jsonb_array_elements(COALESCE(v_classification_result->'blockers', '[]'::jsonb)) WITH ORDINALITY AS race_blockers(blocker_value, blocker_ordinal)
      WHERE COALESCE(race_blockers.blocker_value->>'code', '') IN (
        'PROVIDER_SUBMISSION_ALREADY_CLAIMED',
        'PROVIDER_SUBMISSION_IN_PROGRESS',
        'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
      )
    ), '[]'::jsonb),
    'movement_classification_detail', v_classification_result,
    'live_refresh_scope', jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'classification', v_classification,
      'recommended_action', v_recommended_action,
      'selected_selection_hash', v_selected_selection_hash,
      'pay_batch_item_ids', to_jsonb(v_selected_pay_batch_item_ids),
      'pay_bank_transfer_ids', COALESCE(v_selected_mail_scope_json->'pay_bank_transfer_ids', '[]'::jsonb),
      'pay_batch_candidate_ids', COALESCE(v_selected_mail_scope_json->'pay_batch_candidate_ids', '[]'::jsonb),
      'candidate_ids', COALESCE(v_selected_mail_scope_json->'candidate_ids', '[]'::jsonb),
      'finance_case_ids', COALESCE(v_selected_mail_scope_json->'finance_case_ids', '[]'::jsonb),
      'finance_component_ids', COALESCE(v_selected_mail_scope_json->'finance_component_ids', '[]'::jsonb),
      'reservation_ids', COALESCE(v_selected_mail_scope_json->'reservation_ids', '[]'::jsonb),
      'retry_scope_signature', v_retry_scope_signature,
      'retry_eligible_count', COALESCE(v_retry_eligible_count, 0),
      'retry_in_progress', COALESCE(v_retry_in_progress, false)
    ),
    'batch', jsonb_build_object(
      'status', v_batch_status,
      'pay_date', v_batch_pay_date,
      'authoritative_payment_date', v_batch_authoritative_payment_date,
      'effective_pay_date', v_effective_pay_date,
      'created_at_utc', v_batch_created_at_utc,
      'execution_commit_state', v_batch_execution_commit_state,
      'execution_commit_ref', v_batch_execution_commit_ref
    )
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PLAN_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'selection_json', p_selection_json,
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

ALTER FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text) TO service_role;

ALTER FUNCTION public._pay_payment_movement_classify(uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_payment_movement_classify(uuid,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public._pay_payment_movement_classify(uuid,jsonb) TO service_role;

ALTER FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text) TO service_role;

commit;
