-- CloudTMS Banking Pay manual carry-forward preview contract.
-- The cancellation owner creates signed CREDIT and DEBIT carry-forwards and
-- the canonical Workbench owner marks both as Ready.  This exact replacement
-- lets the common preview contract recognise that established non-finance-case
-- identity while retaining every existing negative-entitlement fence.
-- Generated from the historical function by the checked repository generator.

CREATE OR REPLACE FUNCTION public.pay_workbench_preview_line_contract_ok(
  p_line_json jsonb DEFAULT '{}'::jsonb,
  p_economic_key_json jsonb DEFAULT NULL::jsonb,
  p_target_section text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_line_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_line_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_line_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_economic_key_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_economic_key_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_economic_key_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_target_section_input text := NULLIF(BTRIM(COALESCE(p_target_section, '')), '');
  v_target_section text := CASE
    WHEN v_target_section_input IS NULL THEN public.pay_workbench_preview_section_from_line_json(v_line_json)
    ELSE public.pay_workbench_preview_section_from_line_json(v_line_json || jsonb_build_object('target_section', v_target_section_input))
  END;
  v_presentation_section text := UPPER(NULLIF(BTRIM(COALESCE(v_line_json->>'presentation_section', v_line_json->>'readiness_state', '')), ''));
  v_source_kind text := UPPER(NULLIF(BTRIM(COALESCE(v_line_json->>'source_kind', v_line_json->>'source_type', '')), ''));
  v_line_key text := NULLIF(BTRIM(COALESCE(v_line_json->>'line_key', v_line_json->>'row_key', v_line_json->>'preview_row_id', v_line_json->>'line_id', '')), '');
  v_line_type text := UPPER(NULLIF(BTRIM(COALESCE(v_line_json->>'line_type', v_line_json->>'case_type', '')), ''));
  v_case_type text := UPPER(NULLIF(BTRIM(COALESCE(v_line_json->>'case_type', '')), ''));
  v_item_direction text := UPPER(NULLIF(BTRIM(COALESCE(v_line_json->>'item_direction', v_line_json->>'direction', '')), ''));
  v_finance_case_id_text text := NULLIF(BTRIM(COALESCE(v_line_json->>'finance_case_id', '')), '');
  v_finance_case_id_present boolean := false;
  v_is_recognised_finance_deduction boolean := false;
  v_manual_carry_forward_id_text text := NULLIF(BTRIM(COALESCE(v_line_json->>'manual_adjustment_carry_forward_id', '')), '');
  v_is_recognised_manual_carry_forward boolean := false;
  v_key_type text := UPPER(NULLIF(BTRIM(COALESCE(v_economic_key_json->>'key_type', v_line_json#>>'{economic_key,key_type}', v_line_json->>'component_key_type', v_line_json->>'key_type', '')), ''));
  v_key_value text := NULLIF(BTRIM(COALESCE(v_economic_key_json->>'key_value', v_line_json#>>'{economic_key,key_value}', v_line_json->>'component_key_value', v_line_json->>'key_value', '')), '');
  v_amount_text text := NULLIF(BTRIM(COALESCE(v_line_json->>'amount_ex_vat', v_line_json->>'preview_amount_ex_vat', v_line_json->>'section_amount_ex_vat', '')), '');
  v_amount numeric := NULL::numeric;
  v_draftable boolean := false;
  v_is_ready_for_draft boolean := false;
  v_is_excluded boolean := false;
  v_materialisable boolean := false;
  v_selection_allowed boolean := false;
  v_ok boolean := false;
  v_reasons jsonb := '[]'::jsonb;
  v_reason_count integer := 0;
BEGIN
  IF jsonb_typeof(COALESCE(p_line_json, '{}'::jsonb)) <> 'object' THEN
    v_reasons := v_reasons || jsonb_build_array('LINE_JSON_NOT_OBJECT');
  END IF;

  IF v_source_kind IN (
    'TIMESHEET_SNAPSHOT',
    'TIMESHEET_SNAPSHOT_EVIDENCE',
    'RAW_TIMESHEET_SNAPSHOT',
    'INTERNAL_ONLY',
    'NO_DELTA',
    'EXCLUDED'
  ) THEN
    v_reasons := v_reasons || jsonb_build_array('INTERNAL_OR_RAW_SOURCE_KIND');
  END IF;

  IF v_amount_text IS NOT NULL AND v_amount_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
    v_amount := ROUND(v_amount_text::numeric, 2);
  END IF;

  v_finance_case_id_present := COALESCE(v_finance_case_id_text, '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  v_is_recognised_finance_deduction := (
    COALESCE(v_finance_case_id_present, false)
    AND (v_item_direction IS NULL OR v_item_direction = 'DEDUCTION')
    AND (
      v_line_type = 'OVERPAYMENT_RECOVERY'
      OR v_line_type IN ('MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
    )
  );

  -- Cancellation freezes signed manual adjustments independently of finance
  -- cases.  Accept that existing owner only when every carry-forward identity
  -- agrees; this does not classify or calculate the signed amount.
  v_is_recognised_manual_carry_forward := (
    v_line_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
    AND v_case_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
    AND v_item_direction IN ('CREDIT', 'DEBIT')
    AND COALESCE(v_manual_carry_forward_id_text, '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    AND NULLIF(BTRIM(COALESCE(v_line_json->>'source_ref', '')), '')
          = 'carry_forward:' || LOWER(v_manual_carry_forward_id_text)
    AND NULLIF(BTRIM(COALESCE(v_line_json->>'case_key', '')), '')
          = 'carry_forward:' || LOWER(v_manual_carry_forward_id_text)
  );

  v_draftable := LOWER(BTRIM(COALESCE(v_line_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_is_ready_for_draft := LOWER(BTRIM(COALESCE(v_line_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_is_excluded := LOWER(BTRIM(COALESCE(v_line_json->>'is_excluded_from_allocation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF v_target_section = 'canonical_preview_lines' THEN
    v_materialisable := true;

    IF COALESCE(v_draftable, false) IS NOT TRUE THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_NOT_DRAFTABLE');
    END IF;

    IF COALESCE(v_is_ready_for_draft, false) IS NOT TRUE THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_NOT_READY_FOR_DRAFT');
    END IF;

    IF COALESCE(v_is_excluded, false) THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_EXCLUDED_FROM_ALLOCATION');
    END IF;

    IF v_presentation_section IS DISTINCT FROM 'READY_TO_PAY' THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_PRESENTATION_SECTION_INVALID');
    END IF;

    IF v_amount IS NULL THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_AMOUNT_MISSING');
    ELSIF ROUND(COALESCE(v_amount, 0), 2) = 0 THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_AMOUNT_ZERO');
    ELSIF ROUND(COALESCE(v_amount, 0), 2) < 0
       AND COALESCE(v_is_recognised_finance_deduction, false) IS NOT TRUE
       AND COALESCE(v_is_recognised_manual_carry_forward, false) IS NOT TRUE THEN
      v_reasons := v_reasons || jsonb_build_array('NEGATIVE_ENTITLEMENT_MUST_ROUTE_TO_FINANCE_CASE');
    END IF;

    IF v_key_type IS NULL OR v_key_value IS NULL THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_ECONOMIC_KEY_MISSING');
    ELSIF v_key_type = 'TS_DAY' AND v_key_value !~ '^\d{4}-\d{2}-\d{2}$' THEN
      v_reasons := v_reasons || jsonb_build_array('READY_ROW_TS_DAY_KEY_NOT_DATE');
    END IF;

    IF v_line_key LIKE 'timesheet_snapshot:%' THEN
      v_reasons := v_reasons || jsonb_build_array('RAW_TIMESHEET_SNAPSHOT_ROW_KEY_REJECTED');
    END IF;

  ELSIF v_target_section IN ('cases_resolutions', 'blocked_for_pay') THEN
    v_materialisable := true;
    v_selection_allowed := false;

    IF v_presentation_section IS NULL THEN
      v_reasons := v_reasons || jsonb_build_array('DISPLAY_ROW_PRESENTATION_SECTION_MISSING');
    END IF;

    IF COALESCE(v_draftable, false) THEN
      v_reasons := v_reasons || jsonb_build_array('DISPLAY_ROW_MUST_NOT_BE_DRAFTABLE');
    END IF;
  ELSE
    v_materialisable := false;
  END IF;

  SELECT jsonb_array_length(v_reasons)
  INTO v_reason_count;

  v_ok := COALESCE(v_materialisable, false) AND COALESCE(v_reason_count, 0) = 0;

  IF v_target_section = 'canonical_preview_lines' AND COALESCE(v_ok, false) THEN
    v_selection_allowed := true;
  END IF;

  RETURN jsonb_build_object(
    'ok', COALESCE(v_ok, false),
    'materialisable', COALESCE(v_materialisable, false),
    'target_section', v_target_section,
    'presentation_section', v_presentation_section,
    'source_kind', v_source_kind,
    'line_type', v_line_type,
    'case_type', v_case_type,
    'item_direction', v_item_direction,
    'finance_case_id', v_finance_case_id_text,
    'is_recognised_finance_deduction', COALESCE(v_is_recognised_finance_deduction, false),
    'is_recognised_manual_carry_forward', COALESCE(v_is_recognised_manual_carry_forward, false),
    'line_key', v_line_key,
    'draftable', COALESCE(v_draftable, false),
    'is_ready_for_draft', COALESCE(v_is_ready_for_draft, false)
  )
  || jsonb_build_object(
    'is_excluded_from_allocation', COALESCE(v_is_excluded, false),
    'selection_allowed', COALESCE(v_selection_allowed, false),
    'amount_ex_vat', v_amount,
    'key_type', v_key_type,
    'key_value', v_key_value,
    'reason_count', COALESCE(v_reason_count, 0),
    'reasons', COALESCE(v_reasons, '[]'::jsonb),
    'status', CASE WHEN COALESCE(v_ok, false) THEN 'OK' ELSE 'NOT_OK' END
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_preview_line_contract_ok(jsonb, jsonb, text)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_preview_line_contract_ok(jsonb, jsonb, text)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_preview_line_contract_ok(jsonb, jsonb, text)
  TO service_role;
