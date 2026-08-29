-- Pre-Draft PRESENTATION projection of existing certified display fields.
-- It neither computes pay nor allocates a recovery. Current Workbench owners
-- still supply VAT-aware display amounts, recovery caps and selectable identity.
-- Test oracle: unchanged renderPayNewBatchWizard numeric helpers, frozen with
-- source hashes in tests/fixtures/banking-pay-legacy-display-oracle.json.
\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_first_display_number_v2(
  p_values jsonb[], p_skip_invalid boolean DEFAULT false
) RETURNS numeric LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_value jsonb; v_text text;
BEGIN
  FOREACH v_value IN ARRAY p_values LOOP
    IF v_value IS NULL OR v_value = 'null'::jsonb THEN CONTINUE; END IF;
    v_text := BTRIM(v_value #>> '{}');
    IF v_text = '' THEN CONTINUE; END IF;
    IF jsonb_typeof(v_value) IN ('number','string')
       AND v_text ~ '^-?[0-9]{1,16}([.][0-9]{1,12})?$' THEN RETURN v_text::numeric; END IF;
    IF p_skip_invalid THEN CONTINUE; END IF;
    -- A corrupt display scalar must not silently become a zero payment.
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_AMOUNT' USING ERRCODE = '22023';
  END LOOP;
  RETURN NULL;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_first_display_number_v2(jsonb[], boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_first_display_number_v2(jsonb[], boolean) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_display_fields_v2(p_row jsonb, p_keys text[])
RETURNS jsonb[] LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  -- Keys occur in snake/camel pairs. Preserve top-level then nested precedence
  -- for each pair exactly as getLineRowLevelAmount/getLineSectionAmount do.
  WITH nested AS (
    SELECT CASE WHEN jsonb_typeof(p_row->'row_json') = 'object' THEN p_row->'row_json'
                WHEN jsonb_typeof(p_row->'rowJson') = 'object' THEN p_row->'rowJson'
                ELSE '{}'::jsonb END AS row_json
  ), values_in_order AS (
    SELECT k.ord, 0 AS origin, p_row->k.key AS value FROM unnest(p_keys) WITH ORDINALITY AS k(key,ord)
    UNION ALL
    SELECT k.ord, 1 AS origin, nested.row_json->k.key FROM unnest(p_keys) WITH ORDINALITY AS k(key,ord) CROSS JOIN nested
  )
  SELECT array_agg(value ORDER BY (ord-1)/2, origin, ord) FROM values_in_order;
$function$;
ALTER FUNCTION private.pay_workbench_modal_display_fields_v2(jsonb, text[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_display_fields_v2(jsonb, text[]) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_line_display_amount_v2(p_row jsonb)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_nested jsonb; v_summary jsonb; v_headroom jsonb; v_components jsonb;
  v_component jsonb; v_basis jsonb; v_line_type text; v_child boolean;
  v_row_amount numeric; v_section_amount numeric; v_recoverable numeric;
  v_original numeric; v_outstanding numeric; v_component_due numeric;
  v_raw_component_total numeric := 0; v_available_component_total numeric := 0;
  v_component_count integer := 0; v_due_values jsonb[] := ARRAY[]::jsonb[];
  v_scheduled numeric; v_no_headroom boolean;
BEGIN
  IF jsonb_typeof(p_row) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_AMOUNT' USING ERRCODE = '22023';
  END IF;
  v_nested := CASE WHEN jsonb_typeof(p_row->'row_json') = 'object' THEN p_row->'row_json'
                   WHEN jsonb_typeof(p_row->'rowJson') = 'object' THEN p_row->'rowJson' ELSE '{}'::jsonb END;
  v_summary := CASE WHEN jsonb_typeof(p_row->'case_resolution_summary') = 'object' THEN p_row->'case_resolution_summary'
                    WHEN jsonb_typeof(v_nested->'case_resolution_summary') = 'object' THEN v_nested->'case_resolution_summary' ELSE '{}'::jsonb END;
  v_headroom := CASE WHEN jsonb_typeof(p_row->'selection_recovery_headroom_v1') = 'object' THEN p_row->'selection_recovery_headroom_v1'
                     WHEN jsonb_typeof(v_nested->'selection_recovery_headroom_v1') = 'object' THEN v_nested->'selection_recovery_headroom_v1' ELSE '{}'::jsonb END;
  v_line_type := UPPER(BTRIM(COALESCE(NULLIF(p_row->>'line_type',''),NULLIF(p_row->>'lineType',''),
    NULLIF(v_nested->>'line_type',''),v_nested->>'lineType','')));
  v_child := UPPER(BTRIM(COALESCE(NULLIF(p_row->>'presentation_role',''),NULLIF(p_row->>'presentationRole',''),
    NULLIF(v_nested->>'presentation_role',''),v_nested->>'presentationRole',''))) = 'CHILD';
  v_row_amount := private.pay_workbench_modal_first_display_number_v2(private.pay_workbench_modal_display_fields_v2(p_row, ARRAY[
    'amount_display','amountDisplay','amount_ex_vat','amountExVat','preview_amount_ex_vat','previewAmountExVat',
    'ready_preview_amount_ex_vat','readyPreviewAmountExVat','component_amount_ex_vat','componentAmountExVat'
  ]));
  v_section_amount := CASE WHEN v_child THEN v_row_amount ELSE COALESCE(
    private.pay_workbench_modal_first_display_number_v2(private.pay_workbench_modal_display_fields_v2(p_row, ARRAY[
      'section_amount_display','sectionAmountDisplay','section_amount_ex_vat','sectionAmountExVat',
      'section_non_segment_amount_ex_vat','sectionNonSegmentAmountExVat'
    ])), v_row_amount) END;
  IF v_line_type NOT IN ('OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY') THEN
    IF v_section_amount IS NULL THEN RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_AMOUNT' USING ERRCODE = '22023'; END IF;
    RETURN v_section_amount;
  END IF;
  SELECT COALESCE(jsonb_agg(item.value ORDER BY source.ord, item.ord), '[]'::jsonb) INTO v_components
  FROM unnest(ARRAY[p_row->'case_components',p_row->'caseComponents',v_nested->'case_components',v_nested->'caseComponents']) WITH ORDINALITY AS source(value,ord)
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(source.value)='array' THEN source.value ELSE '[]'::jsonb END) WITH ORDINALITY AS item(value,ord)
  WHERE jsonb_typeof(item.value)='object';

  v_recoverable := ABS(ROUND(COALESCE(private.pay_workbench_modal_first_display_number_v2(
    ARRAY[p_row->'recoverable_this_pay_run_ex_vat',p_row->'recoverableThisPayRunExVat',
          v_nested->'recoverable_this_pay_run_ex_vat',v_nested->'recoverableThisPayRunExVat']
    || CASE WHEN v_line_type='OVERPAYMENT_RECOVERY' THEN ARRAY[v_headroom->'recoverable_amount_ex_vat',v_headroom->'recoverableAmountExVat'] ELSE ARRAY[]::jsonb[] END
    || ARRAY[to_jsonb(v_row_amount),to_jsonb(v_section_amount),'0'::jsonb], true),0),2));

  IF v_line_type='MANUAL_DEBT_RECOVERY' THEN
    FOR v_component IN SELECT value FROM jsonb_array_elements(v_components) LOOP
      v_basis := CASE WHEN jsonb_typeof(v_component->'source_basis_json')='object' THEN v_component->'source_basis_json'
                      WHEN jsonb_typeof(v_component->'sourceBasisJson')='object' THEN v_component->'sourceBasisJson' ELSE '{}'::jsonb END;
      v_due_values := v_due_values || ARRAY[v_component->'nominal_due_amount_ex_vat',v_component->'nominalDueAmountExVat',
        v_basis->'nominal_due_amount_ex_vat',v_basis->'nominalDueAmountExVat',v_basis->'weekly_due',v_basis->'weeklyDue'];
    END LOOP;
    v_scheduled := ABS(ROUND(COALESCE(private.pay_workbench_modal_first_display_number_v2(ARRAY[
      p_row->'nominal_due_amount_ex_vat',p_row->'nominalDueAmountExVat',v_nested->'nominal_due_amount_ex_vat',v_nested->'nominalDueAmountExVat',
      v_summary->'nominal_due_amount_ex_vat',v_summary->'nominalDueAmountExVat'] || v_due_values, true),0),2));
    SELECT EXISTS (
      SELECT 1 FROM unnest(ARRAY[p_row->'blocked_reason_codes',p_row->'blockedReasonCodes',v_nested->'blocked_reason_codes',
        v_nested->'blockedReasonCodes',v_summary->'blocked_reason_codes',v_summary->'blockedReasonCodes']) AS source(value)
      CROSS JOIN LATERAL jsonb_array_elements_text(CASE WHEN jsonb_typeof(source.value)='array' THEN source.value ELSE '[]'::jsonb END) AS reason(value)
      WHERE UPPER(BTRIM(reason.value))='NO_PAY_HEADROOM'
      UNION ALL
      SELECT 1 FROM unnest(ARRAY[p_row->>'presentation_reason',p_row->>'presentationReason',v_nested->>'presentation_reason',v_nested->>'presentationReason']) AS reason(value)
      WHERE UPPER(BTRIM(reason.value))='NO_PAY_HEADROOM'
    ) INTO v_no_headroom;
    IF v_scheduled>0 AND v_recoverable=0 AND v_no_headroom THEN RETURN -v_scheduled; END IF;
    IF v_section_amount IS NULL THEN RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_AMOUNT' USING ERRCODE = '22023'; END IF;
    RETURN v_section_amount;
  END IF;

  FOR v_component IN SELECT value FROM jsonb_array_elements(v_components) LOOP
    v_original := ABS(ROUND(private.pay_workbench_modal_first_display_number_v2(ARRAY[
      v_component->'resolved_target_amount_ex_vat',v_component->'resolvedTargetAmountExVat',v_component->'target_pay_ex_vat',v_component->'targetPayExVat',
      v_component->'source_amount',v_component->'sourceAmount',v_component->'source_pay_ex_vat',v_component->'sourcePayExVat',
      v_component->'remaining_source_amount',v_component->'remainingSourceAmount'], true),2));
    v_outstanding := ABS(ROUND(private.pay_workbench_modal_first_display_number_v2(ARRAY[
      v_component->'target_outstanding_ex_vat',v_component->'targetOutstandingExVat',v_component->'remaining_source_amount',v_component->'remainingSourceAmount',to_jsonb(v_original)], true),2));
    v_component_due := ABS(ROUND(COALESCE(private.pay_workbench_modal_first_display_number_v2(ARRAY[
      v_component->'preview_due_amount_ex_vat',v_component->'previewDueAmountExVat',v_component->'allocated_source_due_amount_ex_vat',v_component->'allocatedSourceDueAmountExVat',
      v_component->'target_pay_ex_vat',v_component->'targetPayExVat','0'::jsonb], true),0),2));
    IF v_original IS NULL AND v_outstanding IS NULL THEN CONTINUE; END IF;
    v_component_count := v_component_count + 1;
    v_raw_component_total := v_raw_component_total + GREATEST(v_component_due,0);
    v_available_component_total := v_available_component_total + GREATEST(COALESCE(v_outstanding,v_original,0),0);
  END LOOP;
  -- The old presentation caps each component in order and then sums it.
  -- For existing two-decimal certified facts that sum is min(sum, row cap).
  -- Only the display total is projected; no component allocation is changed.
  IF v_component_count>0 THEN
    v_recoverable := LEAST(v_recoverable, CASE WHEN v_recoverable>0 AND v_raw_component_total<>v_recoverable
      THEN v_available_component_total ELSE v_raw_component_total END);
  END IF;
  RETURN CASE WHEN v_recoverable>0 THEN -v_recoverable ELSE 0::numeric END;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_line_display_amount_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_line_display_amount_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;
