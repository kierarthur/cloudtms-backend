-- Restore the explicit diagnostic contexts required by the Banking alert panel.
-- The July alert lifecycle function pre-dated the mandatory diagnostic-context
-- arguments and therefore raised before it could return or acknowledge alerts.

DO $migration$
DECLARE
  v_function_oid oid;
  v_definition text;
  v_provider_before constant text := $provider_before$
      p_counts_only := false
    ) AS provider_diagnostic(diagnostic_json)
$provider_before$;
  v_provider_after constant text := $provider_after$
      p_counts_only := false,
      p_provider_diagnostic_context := 'PAYMENT_ISSUES_PROVIDER_DIAGNOSTIC'
    ) AS provider_diagnostic(diagnostic_json)
$provider_after$;
  v_cancelability_before constant text := $cancelability_before$
          jsonb_build_object('scope_type', 'BATCH'),
          p_actor_user_id
        ),
$cancelability_before$;
  v_cancelability_after constant text := $cancelability_after$
          jsonb_build_object('scope_type', 'BATCH'),
          p_actor_user_id,
          'PAYMENT_ISSUES_TAB'
        ),
$cancelability_after$;
  v_occurrences integer;
BEGIN
  SELECT function_row.oid
  INTO v_function_oid
  FROM pg_proc AS function_row
  JOIN pg_namespace AS function_schema
    ON function_schema.oid = function_row.pronamespace
  WHERE function_schema.nspname = 'public'
    AND function_row.proname = 'banking_alerts_active_for_user'
    AND pg_get_function_identity_arguments(function_row.oid) =
      'p_actor_user_id uuid, p_entity_kind text, p_entity_id uuid, p_include_acknowledged boolean, p_limit integer, p_alert_context text';

  IF v_function_oid IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERTS_ACTIVE_FOR_USER_FUNCTION_NOT_FOUND';
  END IF;

  v_definition := pg_get_functiondef(v_function_oid);

  IF strpos(v_definition, v_provider_after) = 0 THEN
    v_occurrences := (length(v_definition) - length(replace(v_definition, v_provider_before, ''))) / length(v_provider_before);
    IF v_occurrences <> 1 THEN
      RAISE EXCEPTION 'BANKING_ALERT_PROVIDER_DIAGNOSTIC_CALL_PATCH_COUNT_INVALID: %', v_occurrences;
    END IF;
    v_definition := replace(v_definition, v_provider_before, v_provider_after);
  END IF;

  IF strpos(v_definition, v_cancelability_after) = 0 THEN
    v_occurrences := (length(v_definition) - length(replace(v_definition, v_cancelability_before, ''))) / length(v_cancelability_before);
    IF v_occurrences <> 1 THEN
      RAISE EXCEPTION 'BANKING_ALERT_CANCELABILITY_CALL_PATCH_COUNT_INVALID: %', v_occurrences;
    END IF;
    v_definition := replace(v_definition, v_cancelability_before, v_cancelability_after);
  END IF;

  IF strpos(v_definition, v_provider_after) = 0 OR strpos(v_definition, v_cancelability_after) = 0 THEN
    RAISE EXCEPTION 'BANKING_ALERT_DIAGNOSTIC_CONTEXT_PATCH_NOT_APPLIED';
  END IF;

  EXECUTE v_definition;
END;
$migration$;

-- Make the server-resolved frozen cancellation scope authoritative before the
-- first work expansion. This prevents finance metadata from narrowing a mixed
-- whole-batch draft while retaining the exact frozen-item equality guard.
DO $migration$
DECLARE
  v_function_oid oid;
  v_definition text;
  v_scope_before constant text := $scope_before$
      'scope_type', v_scope_type,
      'pay_batch_id', p_pay_batch_id::text,
$scope_before$;
  v_scope_after constant text := $scope_after$
      'scope_type', v_scope_type,
      'work_unit', CASE
        WHEN v_scope_type IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN 'BATCH'
        WHEN jsonb_array_length(COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb)) > 0 THEN 'TRANSFER'
        ELSE 'CANDIDATE'
      END,
      'pay_batch_id', p_pay_batch_id::text,
$scope_after$;
  v_occurrences integer;
BEGIN
  SELECT function_row.oid
  INTO v_function_oid
  FROM pg_proc AS function_row
  JOIN pg_namespace AS function_schema
    ON function_schema.oid = function_row.pronamespace
  WHERE function_schema.nspname = 'public'
    AND function_row.proname = 'pay_payment_cancel_not_sent_and_recalculate'
    AND pg_get_function_identity_arguments(function_row.oid) =
      'p_pay_batch_id uuid, p_selection_json jsonb, p_actor_user_id uuid, p_reason text, p_idempotency_key text, p_confirmation_json jsonb';

  IF v_function_oid IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_NOT_SENT_FUNCTION_NOT_FOUND';
  END IF;

  v_definition := pg_get_functiondef(v_function_oid);

  IF strpos(v_definition, v_scope_after) = 0 THEN
    v_occurrences := (length(v_definition) - length(replace(v_definition, v_scope_before, ''))) / length(v_scope_before);
    IF v_occurrences <> 1 THEN
      RAISE EXCEPTION 'PAYMENT_CANCEL_FROZEN_SCOPE_PATCH_COUNT_INVALID: %', v_occurrences;
    END IF;
    v_definition := replace(v_definition, v_scope_before, v_scope_after);
  END IF;

  IF strpos(v_definition, v_scope_after) = 0 THEN
    RAISE EXCEPTION 'PAYMENT_CANCEL_FROZEN_SCOPE_PATCH_NOT_APPLIED';
  END IF;

  EXECUTE v_definition;
END;
$migration$;
