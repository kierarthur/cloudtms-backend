-- Repeatable CloudTMS function/view authority: banking_pay_signed_recovery_classifier_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_batch_signed_non_charge_recovery_evidence_v1(
  p_document jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SET search_path = 'pg_catalog'
AS $function$
DECLARE
  v_item jsonb;
  v_components jsonb;
  v_component jsonb;
  v_matching_count integer := 0;
  v_timesheet_id text;
  v_key_type text;
  v_key_value text;
  v_truth numeric;
  v_baseline numeric;
  v_reserved numeric;
  v_outstanding numeric;
  v_component_amount numeric;
  v_source_pay numeric;
  v_source_charge numeric;
  v_expected_digest text;
BEGIN
  IF jsonb_typeof(p_document) IS DISTINCT FROM 'object' THEN
    RETURN NULL;
  END IF;

  v_item := CASE
    WHEN jsonb_typeof(p_document->'pay_batch_item') = 'object'
      THEN p_document->'pay_batch_item'
    ELSE p_document
  END;
  v_components := v_item#>'{frozen_resolution_payload_json,case_components}';
  IF jsonb_typeof(v_components) IS DISTINCT FROM 'array' THEN
    RETURN NULL;
  END IF;

  v_timesheet_id := NULLIF(BTRIM(COALESCE(v_item->>'timesheet_id', '')), '');
  v_key_type := UPPER(NULLIF(BTRIM(COALESCE(
    v_item->>'frozen_component_key_type',
    v_item#>>'{frozen_component_snapshot_json,component_key_type}',
    ''
  )), ''));
  v_key_value := NULLIF(BTRIM(COALESCE(
    v_item->>'frozen_component_key_value',
    v_item#>>'{frozen_component_snapshot_json,component_key_value}',
    ''
  )), '');

  /* Ordinary frozen resolutions may legitimately contain several rate or
     correction components for one TS_DAY/TS_TOTAL key.  Cardinality is a
     signed-recovery safeguard only after the complete non-charge return
     signature is present; counting ordinary same-key components first makes
     valid historical pay block the pre-Draft Workbench. */
  SELECT COUNT(*)::integer, MIN(component.value::text)::jsonb
  INTO v_matching_count, v_component
  FROM jsonb_array_elements(v_components) AS component(value)
  WHERE UPPER(BTRIM(COALESCE(component.value->>'component_key_type', ''))) = v_key_type
    AND BTRIM(COALESCE(component.value->>'component_key_value', '')) = v_key_value
    AND COALESCE(component.value#>>'{source_basis_json,component_fallback}', '')
      = 'WORKED_TIME_AMOUNT'
    AND COALESCE(component.value->>'authoritative_truth_ex_vat', '')
      ~ '^-?[0-9]+(\.[0-9]+)?$'
    AND COALESCE(component.value->>'authoritative_baseline_ex_vat', '')
      ~ '^-?[0-9]+(\.[0-9]+)?$'
    AND ROUND((component.value->>'authoritative_truth_ex_vat')::numeric, 2) = 0
    AND ROUND((component.value->>'authoritative_baseline_ex_vat')::numeric, 2) < 0;

  IF v_matching_count = 0 THEN
    RETURN NULL;
  END IF;
  IF v_matching_count <> 1 THEN
    RAISE EXCEPTION 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID'
      USING ERRCODE = '23514',
        DETAIL = jsonb_build_object(
          'reason', 'MATCHED_COMPONENT_CARDINALITY',
          'matching_component_count', v_matching_count,
          'timesheet_id', v_timesheet_id,
          'key_type', v_key_type,
          'key_value', v_key_value
        )::text;
  END IF;

  IF COALESCE(v_component#>>'{source_basis_json,component_fallback}', '')
       <> 'WORKED_TIME_AMOUNT'
     OR COALESCE(v_component->>'authoritative_truth_ex_vat', '')
       !~ '^-?[0-9]+(\.[0-9]+)?$'
     OR COALESCE(v_component->>'authoritative_baseline_ex_vat', '')
       !~ '^-?[0-9]+(\.[0-9]+)?$'
     OR ROUND((v_component->>'authoritative_truth_ex_vat')::numeric, 2) <> 0
     OR ROUND((v_component->>'authoritative_baseline_ex_vat')::numeric, 2) >= 0 THEN
    RETURN NULL;
  END IF;

  IF v_timesheet_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR UPPER(BTRIM(COALESCE(v_item->>'item_type', ''))) NOT IN (
       'SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA'
     )
     OR v_key_type NOT IN ('TS_DAY', 'TS_TOTAL')
     OR v_key_value IS NULL
     OR (v_key_type = 'TS_DAY' AND (
       v_key_value !~ '^\d{4}-\d{2}-\d{2}$'
       OR NOT pg_input_is_valid(v_key_value, 'date')
       OR CASE WHEN pg_input_is_valid(v_key_value, 'date')
            THEN v_key_value::date::text <> v_key_value ELSE true END
     ))
     OR COALESCE(v_component->>'component_amount_ex_vat', '')
       !~ '^-?[0-9]+(\.[0-9]+)?$'
     OR COALESCE(v_component->>'source_pay_ex_vat', '')
       !~ '^-?[0-9]+(\.[0-9]+)?$'
     OR COALESCE(v_component->>'source_charge_ex_vat', '')
       !~ '^-?[0-9]+(\.[0-9]+)?$'
     OR COALESCE(v_component->>'authoritative_reserved_ex_vat', '')
       !~ '^-?[0-9]+(\.[0-9]+)?$'
     OR COALESCE(v_component->>'authoritative_outstanding_ex_vat', '')
       !~ '^-?[0-9]+(\.[0-9]+)?$'
     OR NULLIF(BTRIM(COALESCE(v_component->>'physical_bucket_key', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_component->>'physical_bucket_digest', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_component->>'financial_revision_digest', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_component->>'target_authority_digest', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_component->>'conversion_context_digest', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_component->>'sealed_evidence_digest', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_component->>'source_family_key', '')), '')
       IS DISTINCT FROM 'timesheet:' || v_timesheet_id
     OR UPPER(NULLIF(BTRIM(COALESCE(v_component->>'source_pay_method', '')), ''))
       IS DISTINCT FROM UPPER(NULLIF(BTRIM(COALESCE(
         v_component->>'current_target_pay_method', ''
       )), ''))
     OR (v_key_type = 'TS_DAY' AND NULLIF(BTRIM(COALESCE(
       v_component#>>'{source_basis_json,work_date}', ''
     )), '') IS DISTINCT FROM v_key_value) THEN
    RAISE EXCEPTION 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID'
      USING ERRCODE = '23514',
        DETAIL = jsonb_build_object(
          'reason', 'FROZEN_EVIDENCE_SHAPE',
          'timesheet_id', v_timesheet_id,
          'key_type', v_key_type,
          'key_value', v_key_value
        )::text;
  END IF;

  v_truth := ROUND((v_component->>'authoritative_truth_ex_vat')::numeric, 2);
  v_baseline := ROUND((v_component->>'authoritative_baseline_ex_vat')::numeric, 2);
  v_reserved := ROUND((v_component->>'authoritative_reserved_ex_vat')::numeric, 2);
  v_outstanding := ROUND((v_component->>'authoritative_outstanding_ex_vat')::numeric, 2);
  v_component_amount := ROUND((v_component->>'component_amount_ex_vat')::numeric, 2);
  v_source_pay := ROUND((v_component->>'source_pay_ex_vat')::numeric, 2);
  v_source_charge := ROUND((v_component->>'source_charge_ex_vat')::numeric, 2);
  v_expected_digest := md5(jsonb_build_object(
    'sealed_evidence_version', 2,
    'financial_revision_digest', v_component->>'financial_revision_digest',
    'target_authority_digest', v_component->>'target_authority_digest',
    'conversion_context_digest', v_component->>'conversion_context_digest',
    'physical_bucket_digest', v_component->>'physical_bucket_digest',
    'economic_key_type', v_key_type,
    'economic_key_value', v_key_value,
    'truth_ex_vat', v_truth,
    'baseline_ex_vat', v_baseline,
    'reserved_ex_vat', v_reserved
  )::text);

  IF v_truth <> 0
     OR v_baseline >= 0
     OR v_reserved < 0
     OR v_outstanding <= 0
     OR v_outstanding <> ROUND(v_truth - v_baseline - v_reserved, 2)
     OR v_component_amount <> v_outstanding
     OR v_source_pay <> v_outstanding
     OR v_source_charge <> 0
     OR v_component->>'sealed_evidence_digest' IS DISTINCT FROM v_expected_digest THEN
    RAISE EXCEPTION 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID'
      USING ERRCODE = '23514',
        DETAIL = jsonb_build_object(
          'reason', 'FROZEN_EVIDENCE_RECONCILIATION',
          'timesheet_id', v_timesheet_id,
          'key_type', v_key_type,
          'key_value', v_key_value
        )::text;
  END IF;

  RETURN jsonb_build_object(
    'contract', 'SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1',
    'contract_version', 1,
    'timesheet_id', v_timesheet_id,
    'economic_key_type', v_key_type,
    'economic_key_value', v_key_value,
    'truth_ex_vat', v_truth,
    'baseline_ex_vat', v_baseline,
    'reserved_ex_vat', v_reserved,
    'outstanding_ex_vat', v_outstanding,
    'source_charge_ex_vat', v_source_charge,
    'physical_bucket_key', v_component->>'physical_bucket_key',
    'physical_bucket_digest', v_component->>'physical_bucket_digest',
    'sealed_evidence_digest', v_component->>'sealed_evidence_digest',
    'financial_revision_digest', v_component->>'financial_revision_digest',
    'target_authority_digest', v_component->>'target_authority_digest',
    'conversion_context_digest', v_component->>'conversion_context_digest',
    'source_pay_method', UPPER(BTRIM(v_component->>'source_pay_method')),
    'target_pay_method', UPPER(BTRIM(v_component->>'current_target_pay_method')),
    'evidence_digest', md5(jsonb_build_object(
      'contract', 'SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1',
      'contract_version', 1,
      'timesheet_id', v_timesheet_id,
      'economic_key_type', v_key_type,
      'economic_key_value', v_key_value,
      'truth_ex_vat', v_truth,
      'baseline_ex_vat', v_baseline,
      'reserved_ex_vat', v_reserved,
      'outstanding_ex_vat', v_outstanding,
      'source_charge_ex_vat', v_source_charge,
      'physical_bucket_key', v_component->>'physical_bucket_key',
      'physical_bucket_digest', v_component->>'physical_bucket_digest',
      'sealed_evidence_digest', v_component->>'sealed_evidence_digest'
    )::text)
  );
END;
$function$;

ALTER FUNCTION private.pay_batch_signed_non_charge_recovery_evidence_v1(jsonb)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_batch_signed_non_charge_recovery_evidence_v1(jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.pay_batch_signed_non_charge_recovery_evidence_v1(jsonb)
  TO postgres;

commit;
