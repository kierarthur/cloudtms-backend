-- WORKBENCH_SETTLED_CERTIFICATION_V2 / V8 bounded producer.
-- Runtime authority is Miget; the supabase directory name is historical only.
-- This file snapshots Workbench evidence. It owns no payment economics.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_source_row_digest_v8(
  p_preview_row_id uuid
)
RETURNS text
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_digest text;
BEGIN
  SELECT private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(
      pg_catalog.jsonb_build_object(
        'id', preview.id,
        'session_id', preview.session_id,
        'session_version', preview.session_version,
        'candidate_id', preview.candidate_id,
        'timesheet_id', preview.timesheet_id,
        'section', preview.section,
        'row_key', preview.row_key,
        'row_ordinal', preview.row_ordinal,
        'key_type', preview.key_type,
        'key_value', preview.key_value,
        'selected', preview.selected,
        'selection_state', preview.selection_state,
        'status', preview.status,
        'row_json', preview.row_json
      )
    )
  ) INTO v_digest
  FROM public.banking_pay_workbench_preview_rows preview
  WHERE preview.id = p_preview_row_id;

  RETURN v_digest;
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_preview_contract_v8(
  p_preview_row_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_row public.banking_pay_workbench_preview_rows%ROWTYPE;
  v_json jsonb;
  v_overlay jsonb;
  v_preview_contract jsonb;
  v_candidate_id uuid;
  v_timesheet_id uuid;
  v_finance_case_id uuid;
  v_finance_component_id uuid;
  v_pay_channel text;
  v_line_type text;
  v_key_type text;
  v_key_value text;
  v_amount numeric;
  v_selected_headroom numeric;
  v_recoverable numeric;
  v_nominal numeric;
  v_physical_section text;
  v_effective_section text;
  v_overlay_valid boolean := false;
  v_synthetic_total boolean := false;
  v_certified_finance boolean := false;
  v_eligible boolean := false;
BEGIN
  SELECT * INTO v_row
  FROM public.banking_pay_workbench_preview_rows
  WHERE id = p_preview_row_id;
  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object('eligible', false, 'reason', 'PREVIEW_ROW_NOT_FOUND');
  END IF;

  v_json := v_row.row_json;
  v_overlay := CASE WHEN pg_catalog.jsonb_typeof(v_json->'selection_recovery_headroom_v1') = 'object'
    THEN v_json->'selection_recovery_headroom_v1' ELSE '{}'::jsonb END;
  v_preview_contract := CASE WHEN pg_catalog.jsonb_typeof(v_json->'preview_contract') = 'object'
    THEN v_json->'preview_contract' ELSE '{}'::jsonb END;
  v_candidate_id := v_row.candidate_id;
  v_timesheet_id := COALESCE(
    v_row.timesheet_id,
    CASE WHEN COALESCE(v_json#>>'{economic_key,timesheet_id}', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (v_json#>>'{economic_key,timesheet_id}')::uuid END,
    CASE WHEN COALESCE(v_json->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (v_json->>'timesheet_id')::uuid END
  );
  v_finance_case_id := CASE
    WHEN COALESCE(v_json->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (v_json->>'finance_case_id')::uuid
    ELSE NULL
  END;
  v_finance_component_id := CASE
    WHEN COALESCE(v_json->>'finance_component_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN (v_json->>'finance_component_id')::uuid
    ELSE NULL
  END;
  v_pay_channel := UPPER(NULLIF(BTRIM(COALESCE(
    v_json->>'pay_channel', v_json->>'current_pay_method', v_json->>'candidate_pay_method', ''
  )), ''));
  v_line_type := UPPER(NULLIF(BTRIM(COALESCE(v_json->>'line_type', v_json->>'item_type', '')), ''));
  v_key_type := UPPER(NULLIF(BTRIM(COALESCE(
    v_row.key_type, v_json#>>'{economic_key,key_type}', v_json->>'key_type', v_json->>'component_key_type', ''
  )), ''));
  v_key_value := NULLIF(BTRIM(COALESCE(
    v_row.key_value, v_json#>>'{economic_key,key_value}', v_json->>'key_value', v_json->>'component_key_value', ''
  )), '');
  IF COALESCE(v_json->>'amount_ex_vat', v_json->>'preview_amount_ex_vat', v_json->>'amount_display', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
    v_amount := ROUND(COALESCE(v_json->>'amount_ex_vat', v_json->>'preview_amount_ex_vat', v_json->>'amount_display')::numeric, 2);
  END IF;

  v_physical_section := CASE LOWER(BTRIM(COALESCE(v_overlay->>'physical_section', '')))
    WHEN 'canonical_preview_lines' THEN 'canonical_preview_lines'
    WHEN 'ready_to_pay' THEN 'canonical_preview_lines'
    WHEN 'ready' THEN 'canonical_preview_lines'
    WHEN 'blocked_for_pay' THEN 'blocked_for_pay'
    WHEN 'blocked_for_pay_now' THEN 'blocked_for_pay'
    WHEN 'blocked_now' THEN 'blocked_for_pay'
    WHEN 'blocked_preview_lines' THEN 'blocked_for_pay'
    ELSE LOWER(BTRIM(COALESCE(v_overlay->>'physical_section', ''))) END;
  v_effective_section := CASE LOWER(BTRIM(COALESCE(v_overlay->>'effective_section', '')))
    WHEN 'canonical_preview_lines' THEN 'canonical_preview_lines'
    WHEN 'ready_to_pay' THEN 'canonical_preview_lines'
    WHEN 'ready' THEN 'canonical_preview_lines'
    WHEN 'blocked_for_pay' THEN 'blocked_for_pay'
    WHEN 'blocked_for_pay_now' THEN 'blocked_for_pay'
    WHEN 'blocked_now' THEN 'blocked_for_pay'
    WHEN 'blocked_preview_lines' THEN 'blocked_for_pay'
    ELSE LOWER(BTRIM(COALESCE(v_overlay->>'effective_section', ''))) END;

  IF COALESCE(v_overlay->>'selected_positive_headroom_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
     AND COALESCE(v_overlay->>'recoverable_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
     AND COALESCE(v_overlay->>'nominal_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
    v_selected_headroom := ROUND((v_overlay->>'selected_positive_headroom_ex_vat')::numeric, 2);
    v_recoverable := ROUND((v_overlay->>'recoverable_amount_ex_vat')::numeric, 2);
    v_nominal := ROUND((v_overlay->>'nominal_due_amount_ex_vat')::numeric, 2);
  END IF;
  v_overlay_valid := COALESCE(v_overlay->>'contract_version', '') = '1'
    AND COALESCE(v_overlay->>'candidate_id', '') = v_candidate_id::text
    AND v_pay_channel IN ('PAYE','UMBRELLA')
    AND UPPER(COALESCE(v_overlay->>'pay_channel', '')) = v_pay_channel
    AND v_line_type IN ('OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','LOAN_REPAYMENT')
    AND v_physical_section = 'blocked_for_pay'
    AND v_effective_section = 'canonical_preview_lines'
    AND v_selected_headroom > 0
    AND v_recoverable > 0
    AND v_recoverable <= v_selected_headroom
    AND v_nominal >= v_recoverable
    AND v_amount < 0
    AND ABS(v_amount) = v_recoverable
    AND v_overlay->'static_recovery_eligible' = 'true'::jsonb
    AND COALESCE(v_overlay->>'overlay_digest', '') ~* '^[0-9a-f]{32}$'
    AND UPPER(COALESCE(v_overlay->>'policy_x_authority_scope', '')) = 'PRE_DRAFT_LIVE_TRUTH';

  IF v_key_type = 'TS_TOTAL'
     AND UPPER(COALESCE(v_key_value, '')) = 'TOTAL'
     AND LOWER(CONCAT_WS('|',
       v_row.id::text, v_row.row_key, v_json->>'preview_row_id', v_json->>'row_key',
       v_json->>'line_key', v_json->>'source_ref', v_json#>>'{source_basis_json,row_key}',
       v_json#>>'{source_basis_json,line_key}', v_json#>>'{source_basis_json,source_ref}'
     )) LIKE '%:non_segment:total%'
     AND (
       v_json->'resolved_segment_rows_replace_source_total' = 'true'::jsonb
       OR v_json#>'{source_basis_json,resolved_segment_rows_replace_source_total}' = 'true'::jsonb
       OR EXISTS (
         SELECT 1
         FROM public.banking_pay_workbench_preview_rows sibling
         WHERE sibling.session_id = v_row.session_id
           AND sibling.id <> v_row.id
           AND sibling.timesheet_id IS NOT DISTINCT FROM v_timesheet_id
           AND UPPER(COALESCE(sibling.key_type, sibling.row_json#>>'{economic_key,key_type}', '')) = 'TS_DAY'
           AND (sibling.selected OR UPPER(sibling.selection_state) = 'SELECTED' OR UPPER(sibling.status) = 'READY')
       )
     ) THEN
    v_synthetic_total := true;
  END IF;

  v_effective_section := CASE
    WHEN v_overlay_valid THEN 'canonical_preview_lines'
    WHEN LOWER(v_row.section) IN ('canonical_preview_lines','ready_to_pay','ready') THEN 'canonical_preview_lines'
    WHEN LOWER(v_row.section) IN ('blocked_for_pay','blocked_for_pay_now','blocked_now','blocked_preview_lines') THEN 'blocked_for_pay'
    WHEN LOWER(v_row.section) IN ('cases_resolutions','cases_and_resolutions','case_resolution','case_resolutions') THEN 'cases_resolutions'
    ELSE LOWER(v_row.section) END;

  -- Finance rows do not have a Timesheet identity.  Admit only the six
  -- canonical visible aliases emitted by the current Workbench producer and
  -- bind the producer-owned case/component/source identities byte-for-byte.
  -- Hidden/frozen item vocabulary (LOAN_REPAYMENT and MANUAL_CREDIT_PAYOUT)
  -- remains invalid as a visible selected row.  No amount, tax, VAT, channel,
  -- headroom or later Draft policy is calculated here.
  v_certified_finance := v_timesheet_id IS NULL
    AND v_line_type IN (
      'OVERPAYMENT_RECOVERY',
      'MANUAL_DEBT_RECOVERY',
      'PAYMENT_ADVANCE_REPAYMENT',
      'LOAN_PAYOUT',
      'UNDERPAYMENT_PAYMENT',
      'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
    )
    AND v_finance_case_id IS NOT NULL
    AND v_finance_component_id IS NOT NULL
    AND v_key_type IN ('CASE_TOTAL','FINANCE_COMPONENT')
    AND v_key_value IS NOT NULL
    AND COALESCE(v_json->>'source_function', '') = 'pay_workbench_candidate_source_build_chunk'
    AND UPPER(COALESCE(v_json->>'dependency_family_kind', '')) = 'FINANCE_CASE'
    AND COALESCE(v_json->>'dependency_family_key', '') = 'finance:' || v_finance_case_id::text
    AND COALESCE(v_json->>'source_ref', '') = 'advance:' || v_finance_case_id::text
    AND UPPER(COALESCE(v_json->>'policy_x_authority_scope', '')) = 'PRE_DRAFT_LIVE_TRUTH'
    AND pg_catalog.jsonb_typeof(v_json->'case_components') = 'array'
    AND pg_catalog.jsonb_array_length(v_json->'case_components') > 0
    AND (
      SELECT pg_catalog.count(*) = 1
      FROM pg_catalog.jsonb_array_elements(v_json->'case_components') AS component(value)
      WHERE component.value->>'finance_component_id' = v_finance_component_id::text
        AND UPPER(COALESCE(component.value->>'component_key_type', '')) = v_key_type
        AND COALESCE(component.value->>'component_key_value', '') = v_key_value
    );

  v_eligible := v_effective_section = 'canonical_preview_lines'
    AND UPPER(v_row.status) = 'READY'
    AND v_row.selected
    AND UPPER(v_row.selection_state) = 'SELECTED'
    AND v_json->'draftable' = 'true'::jsonb
    AND v_json->'is_ready_for_draft' = 'true'::jsonb
    AND v_json->'is_excluded_from_allocation' = 'false'::jsonb
    AND v_preview_contract->'ok' = 'true'::jsonb
    AND (v_overlay_valid OR v_preview_contract->'selection_allowed' = 'true'::jsonb)
    AND (NOT (v_json ? 'selection_allowed') OR v_json->'selection_allowed' = 'true'::jsonb)
    AND v_candidate_id IS NOT NULL
    AND (
      (
        v_timesheet_id IS NOT NULL
        AND v_key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
      )
      OR v_certified_finance
    )
    AND v_key_value IS NOT NULL
    AND v_pay_channel IN ('PAYE','UMBRELLA')
    AND v_line_type IS NOT NULL
    AND v_amount IS NOT NULL
    AND NOT v_synthetic_total;

  RETURN pg_catalog.jsonb_build_object(
    'eligible', v_eligible,
    'effective_section', v_effective_section,
    'overlay_valid', v_overlay_valid,
    'synthetic_resolved_total', v_synthetic_total,
    'candidate_id', v_candidate_id,
    'timesheet_id', v_timesheet_id,
    'finance_case_id', v_finance_case_id,
    'finance_component_id', v_finance_component_id,
    'certified_finance', v_certified_finance,
    'pay_channel', v_pay_channel,
    'line_type', v_line_type,
    'key_type', v_key_type,
    'key_value', v_key_value,
    'amount_ex_vat', v_amount,
    'recovery_overlay', CASE WHEN v_overlay_valid THEN v_overlay ELSE NULL END
  );
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_constituent_seed_v8(
  p_certificate_uuid uuid,
  p_constituent_ordinal integer
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_member private.banking_pay_workbench_settled_certificate_source_members_v8%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_preview public.banking_pay_workbench_preview_rows%ROWTYPE;
  v_source public.banking_pay_workbench_candidate_source_lines%ROWTYPE;
  v_publication private.banking_pay_workbench_settled_certificate_publications_v8%ROWTYPE;
  v_contract jsonb;
  v_json jsonb;
  v_source_identity_unsigned jsonb;
  v_source_identity_digest text;
  v_prior_treatment text;
  v_prior_paid text;
  v_prior_digest text;
  v_supersession_digest text;
  v_item_digest text;
  v_overlay jsonb;
  v_overlay_digest text;
  v_recovery_contract text;
  v_recovery_nominal text;
  v_recovery_amount text;
  v_recovery_headroom text;
  v_recovery_allocated text;
  v_recovery_result text;
  v_allocation_kind text;
  v_allocation_result text;
  v_allocation_digest text;
  v_reservation_amount text;
  v_reservation_ids jsonb := '[]'::jsonb;
  v_reservation_digest text;
  v_superseded_source_ids jsonb := '[]'::jsonb;
  v_economic_build_id uuid;
  v_entitlement_count integer := 0;
  v_entitlement_truth numeric;
  v_entitlement_baseline numeric;
  v_reservation_count integer := 0;
  v_reservation_missing_id_count integer := 0;
  v_client_id uuid;
  v_timesheet_id uuid;
  v_line_type text;
  v_key_type text;
  v_key_value text;
  v_pay_channel text;
  v_payment_method text;
  v_amount numeric;
BEGIN
  IF p_certificate_uuid IS NULL OR p_constituent_ordinal IS NULL
     OR p_constituent_ordinal < 0 OR p_constituent_ordinal > 49999 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CONSTITUENT_CURSOR_INVALID' USING ERRCODE = '22023';
  END IF;

  SELECT * INTO STRICT v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 header
  WHERE header.certificate_uuid = p_certificate_uuid;
  SELECT * INTO STRICT v_member
  FROM private.banking_pay_workbench_settled_certificate_source_members_v8 member
  WHERE member.certificate_uuid = p_certificate_uuid
    AND member.constituent_ordinal = p_constituent_ordinal;
  SELECT * INTO STRICT v_preview
  FROM public.banking_pay_workbench_preview_rows preview
  WHERE preview.id = v_member.preview_row_id;

  IF v_preview.session_id IS DISTINCT FROM v_header.workbench_session_id
     OR v_preview.session_version IS DISTINCT FROM v_header.session_version
     OR private.pay_workbench_settled_certificate_source_row_digest_v8(v_preview.id)
          IS DISTINCT FROM v_member.source_row_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_ROW_CHANGED' USING ERRCODE = '55000';
  END IF;
  v_contract := private.pay_workbench_settled_certificate_preview_contract_v8(v_preview.id);
  IF v_contract->'eligible' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_ROW_NOT_DRAFT_ELIGIBLE'
      USING ERRCODE = '55000', DETAIL = v_contract::text;
  END IF;
  v_json := v_preview.row_json;

  IF COALESCE(v_json->>'source_line_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_LINE_IDENTITY_MISSING' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO STRICT v_source
  FROM public.banking_pay_workbench_candidate_source_lines source_line
  WHERE source_line.id = (v_json->>'source_line_id')::uuid;
  IF v_source.session_id IS DISTINCT FROM v_header.workbench_session_id
     OR v_source.session_version IS DISTINCT FROM v_header.session_version
     OR v_source.candidate_id IS DISTINCT FROM v_preview.candidate_id
     OR v_source.source_change_seq::text IS DISTINCT FROM COALESCE(v_json->>'source_change_seq', '')
     OR v_source.source_build_run_id::text IS DISTINCT FROM COALESCE(v_json->>'source_build_run_id', '')
     OR v_source.source_publication_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_LINE_IDENTITY_MISMATCH' USING ERRCODE = '55000';
  END IF;
  SELECT * INTO STRICT v_publication
  FROM private.banking_pay_workbench_settled_certificate_publications_v8 publication
  WHERE publication.certificate_uuid = p_certificate_uuid
    AND publication.candidate_id = v_preview.candidate_id;
  IF v_publication.source_change_seq IS DISTINCT FROM v_source.source_change_seq
     OR v_publication.source_build_run_id IS DISTINCT FROM v_source.source_build_run_id
     OR v_publication.source_publication_id IS DISTINCT FROM v_source.source_publication_id THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PUBLICATION_SOURCE_MISMATCH' USING ERRCODE = '55000';
  END IF;
  SELECT CASE
    WHEN COALESCE(scope.certified_preview_publication_attestation_json->>'economic_build_id','')
      ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    THEN (scope.certified_preview_publication_attestation_json->>'economic_build_id')::uuid
    ELSE NULL END
  INTO STRICT v_economic_build_id
  FROM public.banking_pay_workbench_session_scope scope
  WHERE scope.session_id=v_header.workbench_session_id
    AND scope.candidate_id=v_preview.candidate_id
    AND scope.certified_preview_publication_session_version=v_header.session_version
    AND scope.certified_preview_publication_source_change_seq=v_source.source_change_seq
    AND scope.certified_preview_publication_source_build_run_id=v_source.source_build_run_id
    AND scope.certified_preview_publication_source_publication_id=v_source.source_publication_id;
  IF v_economic_build_id IS NULL OR NOT EXISTS (
    SELECT 1
    FROM private.banking_pay_workbench_economic_builds build_row
    WHERE build_row.id=v_economic_build_id
      AND build_row.candidate_id=v_preview.candidate_id
      AND build_row.session_id=v_header.workbench_session_id
      AND build_row.session_version=v_header.session_version
      AND build_row.source_change_seq=v_source.source_change_seq
      AND build_row.source_build_run_id=v_source.source_build_run_id
      AND build_row.status='COMPLETE'
      AND build_row.completed_at_utc IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_ECONOMIC_BUILD_IDENTITY_MISSING' USING ERRCODE='55000';
  END IF;

  v_line_type := UPPER(v_contract->>'line_type');
  v_key_type := UPPER(v_contract->>'key_type');
  v_key_value := v_contract->>'key_value';
  v_pay_channel := UPPER(v_contract->>'pay_channel');
  v_timesheet_id := (v_contract->>'timesheet_id')::uuid;
  v_amount := ROUND((v_contract->>'amount_ex_vat')::numeric, 2);
  IF COALESCE(v_json->>'client_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_client_id := (v_json->>'client_id')::uuid;
  ELSIF NULLIF(BTRIM(COALESCE(v_json->>'client_id', '')), '') IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CLIENT_IDENTITY_INVALID' USING ERRCODE = '22023';
  END IF;
  v_payment_method := UPPER(NULLIF(BTRIM(COALESCE(
    v_json->>'current_target_pay_method', v_json->>'source_pay_method', v_json->>'pay_channel', ''
  )), ''));
  IF v_payment_method IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAYMENT_METHOD_MISSING' USING ERRCODE = '22023';
  END IF;

  v_source_identity_unsigned := pg_catalog.jsonb_build_object(
    'source_kind', 'CERTIFIED_CANDIDATE_SOURCE_LINE',
    'source_line_id', v_source.id::text,
    'source_row_key', v_source.line_key,
    'source_publication_id', v_source.source_publication_id,
    'source_change_seq', v_source.source_change_seq,
    'source_build_run_id', v_source.source_build_run_id
  );
  v_source_identity_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_source_identity_unsigned)
  );

  -- Prior settlement is not reconstructed and never defaults from an absent
  -- preview key.  The installed component owner groups the sealed build facts
  -- by the exact Timesheet/economic key and supplies the active settled
  -- baseline already used by Workbench reconciliation.  A genuinely absent
  -- grouped row is explicit zero evidence from the complete sealed build.
  SELECT COUNT(*)::integer,MAX(component.truth_ex_vat),MAX(component.baseline_ex_vat)
  INTO v_entitlement_count,v_entitlement_truth,v_entitlement_baseline
  FROM private.pay_current_timesheet_entitlement_components_from_build_v1(
    v_economic_build_id,NULL
  ) component
  WHERE component.timesheet_id IS NOT DISTINCT FROM v_timesheet_id
    AND UPPER(component.key_type)=v_key_type
    AND component.key_value=v_key_value;
  IF v_entitlement_count > 1 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PRIOR_PAYMENT_COMPONENT_CARDINALITY_INVALID'
      USING ERRCODE='55000';
  END IF;
  IF v_entitlement_count=1 THEN
    IF v_entitlement_truth IS NULL OR v_entitlement_baseline IS NULL THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PRIOR_PAYMENT_COMPONENT_INCOMPLETE'
        USING ERRCODE='55000';
    END IF;
    v_prior_treatment := 'ACTIVE_SETTLED_COMPONENT_BASELINE_APPLIED';
    v_prior_paid := private.pay_workbench_settled_certificate_money_v8(
      pg_catalog.to_jsonb(ROUND(v_entitlement_baseline,2))
    );
  ELSE
    v_prior_treatment := 'NO_SETTLED_COMPONENT_BASELINE_IN_SEALED_BUILD';
    v_prior_paid := '0.00';
  END IF;
  v_prior_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
      'treatment', v_prior_treatment,
      'prior_paid_amount_ex_vat', v_prior_paid,
      'economic_build_id',v_economic_build_id,
      'authoritative_truth_ex_vat',CASE WHEN v_entitlement_count=1 THEN
        private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(ROUND(v_entitlement_truth,2))) END,
      'authoritative_baseline_ex_vat',CASE WHEN v_entitlement_count=1 THEN v_prior_paid END,
      'source_row_digest_sha256', v_member.source_row_digest_sha256
    ))
  );

  -- Supersession is the exact stable line or certified cancellation replay
  -- lineage inside this session. Candidate-only history is deliberately not
  -- admitted because it could bind an unrelated economic source.
  SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(old_source.id::text)
           ORDER BY old_source.source_change_seq,old_source.source_ordinal,old_source.id),'[]'::jsonb)
  INTO v_superseded_source_ids
  FROM public.banking_pay_workbench_candidate_source_lines old_source
  WHERE old_source.session_id=v_source.session_id
    AND old_source.candidate_id=v_source.candidate_id
    AND old_source.id<>v_source.id
    AND UPPER(BTRIM(old_source.status))='SUPERSEDED'
    AND (
      old_source.line_key=v_source.line_key
      OR (
        old_source.economic_key_json=v_source.economic_key_json
        AND old_source.source_build_run_id::text IN (
          NULLIF(v_source.source_row_json->>'original_source_build_run_id',''),
          NULLIF(v_source.source_row_json->>'replayed_from_source_build_run_id','')
        )
      )
    );
  v_supersession_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
      'treatment',CASE WHEN pg_catalog.jsonb_array_length(v_superseded_source_ids)=0
        THEN 'CURRENT_CERTIFIED_SOURCE_ONLY'
        ELSE 'CURRENT_CERTIFIED_SOURCE_REPLACES_PROVED_LINEAGE' END,
      'ordered_superseded_source_ids',v_superseded_source_ids,
      'source_publication_id', v_source.source_publication_id
    ))
  );
  v_item_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
      'source_identity_digest_sha256', v_source_identity_digest,
      'semantic_kind', v_line_type,
      'canonical_amount_ex_vat', private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(v_amount)),
      'economic_key', pg_catalog.jsonb_build_object(
        'timesheet_id', v_timesheet_id, 'key_type', v_key_type, 'key_value', v_key_value
      )
    ))
  );

  v_overlay := CASE WHEN pg_catalog.jsonb_typeof(v_json->'selection_recovery_headroom_v1') = 'object'
    THEN v_json->'selection_recovery_headroom_v1' ELSE NULL END;
  IF v_line_type IN ('OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','LOAN_REPAYMENT') THEN
    v_recovery_contract := COALESCE(v_overlay->>'contract_version', v_json->>'recovery_residual_contract_version');
    v_recovery_nominal := private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(
      ROUND(COALESCE(NULLIF(v_overlay->>'nominal_due_amount_ex_vat', '')::numeric,
        NULLIF(v_json->>'nominal_due_amount_ex_vat', '')::numeric), 2)
    ));
    v_recovery_amount := private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(
      ROUND(COALESCE(NULLIF(v_overlay->>'recoverable_amount_ex_vat', '')::numeric,
        NULLIF(v_json->>'recoverable_this_pay_run_ex_vat', '')::numeric, ABS(v_amount)), 2)
    ));
    v_recovery_headroom := private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(
      ROUND(COALESCE(NULLIF(v_overlay->>'selected_positive_headroom_ex_vat', '')::numeric,
        NULLIF(v_json->>'retained_positive_headroom_ex_vat', '')::numeric,
        NULLIF(v_json->>'semantic_ordinary_positive_headroom_ex_vat', '')::numeric,
        ABS(v_amount)), 2)
    ));
    v_recovery_allocated := v_recovery_amount;
    v_recovery_result := 'RECOVERY_HEADROOM_ALLOCATED';
    v_allocation_kind := 'WORKBENCH_RECOVERY_HEADROOM_V1';
    v_allocation_result := 'ALLOCATED_WITHIN_CERTIFIED_HEADROOM';
    v_allocation_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
      private.pay_workbench_settled_certificate_stable_stringify_v8(COALESCE(v_overlay, pg_catalog.jsonb_build_object(
        'nominal_due_amount_ex_vat', v_recovery_nominal,
        'recoverable_amount_ex_vat', v_recovery_amount,
        'selected_positive_headroom_ex_vat', v_recovery_headroom
      )))
    );
    v_overlay_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
      private.pay_workbench_settled_certificate_stable_stringify_v8(COALESCE(v_overlay, '{}'::jsonb))
    );
  ELSE
    v_recovery_result := 'NOT_APPLICABLE';
    v_allocation_kind := 'NOT_APPLICABLE';
    v_allocation_result := 'NOT_APPLICABLE';
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_json->>'finance_component_id','')),'') IS NOT NULL
     AND COALESCE(v_json->>'finance_component_id','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FINANCE_COMPONENT_IDENTITY_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT COUNT(*)::integer,
         COUNT(*) FILTER(WHERE fact.reservation_id IS NULL)::integer,
         CASE WHEN COUNT(*)=0 THEN NULL ELSE private.pay_workbench_settled_certificate_money_v8(
           pg_catalog.to_jsonb(ROUND(SUM(fact.reserved_source_amount),2))) END,
         COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(fact.reservation_id::text)
           ORDER BY fact.natural_key,fact.reservation_id)
           FILTER(WHERE fact.reservation_id IS NOT NULL),'[]'::jsonb)
  INTO v_reservation_count,v_reservation_missing_id_count,v_reservation_amount,v_reservation_ids
  FROM private.banking_pay_workbench_economic_build_facts fact
  WHERE fact.build_id=v_economic_build_id
    AND fact.fact_family='RESERVATION_COMPONENT'
    AND fact.candidate_id=v_preview.candidate_id
    AND fact.timesheet_id IS NOT DISTINCT FROM v_timesheet_id
    AND UPPER(fact.economic_key_type)=v_key_type
    AND fact.economic_key_value=v_key_value
    AND (NULLIF(v_json->>'finance_component_id','') IS NULL
      OR fact.finance_component_id=(v_json->>'finance_component_id')::uuid);
  IF v_reservation_missing_id_count<>0
     OR (v_reservation_count<>pg_catalog.jsonb_array_length(v_reservation_ids)) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_RESERVATION_IDENTITY_INCOMPLETE'
      USING ERRCODE='55000';
  END IF;
  v_reservation_digest := CASE WHEN v_reservation_amount IS NULL THEN NULL ELSE
    private.pay_workbench_settled_certificate_sha256_text_v8(
      private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
        'source_reservation_amount_ex_vat', v_reservation_amount,
        'ordered_active_source_reservation_ids', v_reservation_ids
      ))
    ) END;

  RETURN pg_catalog.jsonb_build_object(
    'preview_row_id', v_preview.id,
    'presentation_preview_row_id', COALESCE(NULLIF(BTRIM(v_json->>'preview_row_id'), ''), v_preview.id::text),
    'row_key', v_preview.row_key,
    'line_id', NULLIF(BTRIM(v_json->>'line_id'), ''),
    'source_kind', 'CERTIFIED_CANDIDATE_SOURCE_LINE',
    'source_line_id', v_source.id::text,
    'source_row_key', v_source.line_key,
    'source_publication_id', v_source.source_publication_id,
    'source_change_seq', v_source.source_change_seq,
    'source_build_run_id', v_source.source_build_run_id,
    'source_identity_digest_sha256', v_source_identity_digest,
    'candidate_publication_ordinal', v_publication.scope_ordinal,
    'candidate_id', v_preview.candidate_id,
    'client_id', v_client_id,
    'timesheet_id', v_timesheet_id,
    'resolved_pay_channel', v_pay_channel,
    'resolved_payment_method', v_payment_method,
    'amount_sign', CASE WHEN v_amount < 0 THEN 'NEGATIVE' WHEN v_amount > 0 THEN 'POSITIVE' ELSE 'ZERO' END,
    'semantic_kind', v_line_type,
    'economic_key_type', v_key_type,
    'economic_key_value', v_key_value,
    'canonical_amount_ex_vat', private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(v_amount)),
    'source_reservation_amount_ex_vat', v_reservation_amount,
    'source_reservation_ids', v_reservation_ids,
    'prior_payment_treatment', v_prior_treatment,
    'prior_paid_amount_ex_vat', v_prior_paid,
    'prior_payment_evidence_digest_sha256', v_prior_digest,
    'supersession_treatment', CASE WHEN pg_catalog.jsonb_array_length(v_superseded_source_ids)=0
      THEN 'CURRENT_CERTIFIED_SOURCE_ONLY'
      ELSE 'CURRENT_CERTIFIED_SOURCE_REPLACES_PROVED_LINEAGE' END,
    'supersession_evidence_digest_sha256', v_supersession_digest,
    'superseded_source_ids',v_superseded_source_ids,
    'recovery_contract_version', v_recovery_contract,
    'recovery_nominal_due_amount_ex_vat', v_recovery_nominal,
    'recovery_recoverable_this_pay_run_ex_vat', v_recovery_amount,
    'recovery_headroom_amount_ex_vat', v_recovery_headroom,
    'recovery_allocated_amount_ex_vat', v_recovery_allocated,
    'recovery_result_kind', v_recovery_result,
    'recovery_overlay_digest_sha256', v_overlay_digest,
    'expected_allocation_basis_kind', v_allocation_kind,
    'expected_allocated_recovery_amount_ex_vat', v_recovery_allocated,
    'expected_allocation_result', v_allocation_result,
    'expected_allocation_source_digest_sha256', v_allocation_digest,
    'expected_item_semantic_kind', v_line_type,
    'expected_item_source_identity_digest_sha256', v_source_identity_digest,
    'expected_item_amount_ex_vat', private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(v_amount)),
    'expected_item_source_digest_sha256', v_item_digest,
    'expected_reservation_applicability', CASE WHEN v_reservation_amount IS NULL THEN 'NOT_APPLICABLE' ELSE 'APPLICABLE' END,
    'expected_reservation_amount_ex_vat', v_reservation_amount,
    'expected_reservation_source_digest_sha256', v_reservation_digest,
    'case_components', CASE WHEN pg_catalog.jsonb_typeof(v_json->'case_components') = 'array'
      THEN v_json->'case_components' ELSE '[]'::jsonb END,
    'source_row_digest_sha256', v_member.source_row_digest_sha256
  );
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_component_seed_v8(
  p_component jsonb,
  p_frozen_component_ordinal integer,
  p_economic_key_type text,
  p_economic_key_value text
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_source_basis jsonb;
  v_stable_component_id text;
  v_component_fallback text;
  v_truth text;
  v_baseline text;
  v_reserved text;
  v_outstanding text;
  v_component_amount text;
  v_source_pay text;
  v_source_charge text;
  v_financial_revision text;
  v_target_authority text;
  v_conversion_context text;
  v_bucket_key text;
  v_bucket_digest text;
  v_sealed_evidence text;
  v_source_method text;
  v_target_method text;
  v_full boolean;
  v_decisive boolean;
  v_bound_facts jsonb;
  v_component_key_type text;
  v_component_key_value text;
BEGIN
  IF pg_catalog.jsonb_typeof(p_component) IS DISTINCT FROM 'object'
     OR p_frozen_component_ordinal IS NULL OR p_frozen_component_ordinal < 0
     OR p_economic_key_type NOT IN (
       'TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD',
       'CASE_TOTAL','FINANCE_COMPONENT'
     )
     OR NULLIF(BTRIM(COALESCE(p_economic_key_value, '')), '') IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_COMPONENT_REQUEST_INVALID' USING ERRCODE = '22023';
  END IF;
  v_source_basis := CASE WHEN pg_catalog.jsonb_typeof(p_component->'source_basis_json') = 'object'
    THEN p_component->'source_basis_json' ELSE '{}'::jsonb END;
  v_component_key_type := UPPER(NULLIF(BTRIM(COALESCE(
    p_component->>'component_key_type', v_source_basis->>'component_key_type', ''
  )), ''));
  v_component_key_value := NULLIF(BTRIM(COALESCE(
    p_component->>'component_key_value', v_source_basis->>'component_key_value', ''
  )), '');
  IF v_component_key_type IS DISTINCT FROM p_economic_key_type
     OR v_component_key_value IS DISTINCT FROM p_economic_key_value THEN
    RETURN NULL;
  END IF;

  v_stable_component_id := NULLIF(BTRIM(COALESCE(
    p_component->>'component_fingerprint', p_component->>'source_basis_fingerprint',
    p_component->>'finance_component_id', p_component->>'segment_stable_key',
    p_component->>'segment_id', ''
  )), '');
  IF v_stable_component_id IS NULL THEN
    v_stable_component_id := private.pay_workbench_settled_certificate_sha256_text_v8(
      private.pay_workbench_settled_certificate_stable_stringify_v8(p_component)
    );
  END IF;
  v_component_fallback := UPPER(NULLIF(BTRIM(COALESCE(
    p_component->>'component_fallback', v_source_basis->>'component_fallback', ''
  )), ''));

  v_truth := CASE WHEN COALESCE(p_component->>'authoritative_truth_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN private.pay_workbench_settled_certificate_money_v8(p_component->'authoritative_truth_ex_vat') END;
  v_baseline := CASE WHEN COALESCE(p_component->>'authoritative_baseline_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN private.pay_workbench_settled_certificate_money_v8(p_component->'authoritative_baseline_ex_vat') END;
  v_reserved := CASE WHEN COALESCE(p_component->>'authoritative_reserved_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN private.pay_workbench_settled_certificate_money_v8(p_component->'authoritative_reserved_ex_vat') END;
  v_outstanding := CASE WHEN COALESCE(p_component->>'authoritative_outstanding_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN private.pay_workbench_settled_certificate_money_v8(p_component->'authoritative_outstanding_ex_vat') END;
  v_component_amount := CASE WHEN COALESCE(p_component->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN private.pay_workbench_settled_certificate_money_v8(p_component->'component_amount_ex_vat') END;
  v_source_pay := CASE WHEN COALESCE(p_component->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN private.pay_workbench_settled_certificate_money_v8(p_component->'source_pay_ex_vat') END;
  v_source_charge := CASE WHEN COALESCE(p_component->>'source_charge_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN private.pay_workbench_settled_certificate_money_v8(p_component->'source_charge_ex_vat') END;
  v_financial_revision := NULLIF(BTRIM(p_component->>'financial_revision_digest'), '');
  v_target_authority := NULLIF(BTRIM(p_component->>'target_authority_digest'), '');
  v_conversion_context := NULLIF(BTRIM(p_component->>'conversion_context_digest'), '');
  v_bucket_key := NULLIF(BTRIM(p_component->>'physical_bucket_key'), '');
  v_bucket_digest := NULLIF(BTRIM(p_component->>'physical_bucket_digest'), '');
  v_sealed_evidence := NULLIF(BTRIM(p_component->>'sealed_evidence_digest'), '');
  v_source_method := NULLIF(BTRIM(p_component->>'source_pay_method'), '');
  v_target_method := NULLIF(BTRIM(COALESCE(
    p_component->>'current_target_pay_method', p_component->>'target_pay_method', ''
  )), '');

  -- Cardinality is deliberately applied after this complete signed
  -- pre-signature, so ordinary same-key components never become signed.
  v_full := v_component_fallback = 'WORKED_TIME_AMOUNT'
    AND v_truth = '0.00'
    AND v_baseline IS NOT NULL AND v_baseline::numeric < 0;
  v_decisive := v_full
    AND v_reserved IS NOT NULL AND v_reserved::numeric >= 0
    AND v_outstanding IS NOT NULL AND v_outstanding::numeric > 0
    AND v_outstanding::numeric = v_truth::numeric - v_baseline::numeric - v_reserved::numeric
    AND v_component_amount = v_outstanding
    AND v_source_pay = v_outstanding
    AND v_source_charge = '0.00'
    AND v_financial_revision IS NOT NULL AND v_target_authority IS NOT NULL
    AND v_conversion_context IS NOT NULL AND v_bucket_key IS NOT NULL
    AND v_bucket_digest IS NOT NULL AND v_sealed_evidence IS NOT NULL
    AND v_source_method IS NOT NULL AND v_target_method IS NOT NULL;
  v_bound_facts := pg_catalog.jsonb_build_object(
    'frozen_component_ordinal', p_frozen_component_ordinal,
    'source_component_kind', COALESCE(NULLIF(BTRIM(p_component->>'classification'), ''), 'CASE_COMPONENT'),
    'economic_key_type', v_component_key_type,
    'economic_key_value', v_component_key_value,
    'component_fallback', v_component_fallback,
    'authoritative_truth_ex_vat', v_truth,
    'authoritative_baseline_ex_vat', v_baseline,
    'authoritative_reserved_ex_vat', v_reserved,
    'authoritative_outstanding_ex_vat', v_outstanding,
    'component_amount_ex_vat', v_component_amount,
    'source_pay_ex_vat', v_source_pay,
    'source_charge_ex_vat', v_source_charge,
    'financial_revision_digest', v_financial_revision,
    'target_authority_digest', v_target_authority,
    'conversion_context_digest', v_conversion_context,
    'physical_bucket_key', v_bucket_key,
    'physical_bucket_digest', v_bucket_digest,
    'sealed_evidence_digest', v_sealed_evidence,
    'source_pay_method', v_source_method,
    'target_pay_method', v_target_method
  );
  RETURN pg_catalog.jsonb_build_object(
    'stable_component_id', v_stable_component_id,
    'bound_facts', v_bound_facts,
    'full_signed_pre_signature_match', v_full,
    'decisive_signed_evidence', v_decisive,
    'bound_component_digest_sha256', private.pay_workbench_settled_certificate_sha256_text_v8(
      private.pay_workbench_settled_certificate_stable_stringify_v8(
        pg_catalog.jsonb_build_object('stable_component_id', v_stable_component_id, 'bound_facts', v_bound_facts)
      )
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_build_start_v8(
  p_workbench_session_id uuid,
  p_actor_user_id uuid,
  p_build_idempotency_key text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_existing private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_certificate_uuid uuid := pg_catalog.gen_random_uuid();
  v_build_id uuid := pg_catalog.gen_random_uuid();
  v_selected_count integer;
  v_partition_count integer;
  v_selected_total numeric;
  v_invalid_selected_count integer;
  v_server_id_count integer;
  v_server_distinct_count integer;
  v_server_missing_count integer;
  v_selected_missing_count integer;
  v_filter_candidate_id uuid;
  v_filter_client_id uuid;
  v_filter_candidate_text text;
  v_filter_client_text text;
  v_filter_context jsonb;
  v_filter_digest text;
  v_gate jsonb;
  v_gate_digest text;
  v_empty_digest text;
  v_publication_count integer;
  v_invalid_publication_count integer;
  v_queued_jobs integer;
  v_running_jobs integer;
  v_invalid_pointer_count integer;
  v_last_receipt_sha256 text;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout', '15000', true);
  PERFORM pg_catalog.set_config('lock_timeout', '1500', true);
  IF p_workbench_session_id IS NULL OR p_actor_user_id IS NULL
     OR NULLIF(BTRIM(COALESCE(p_build_idempotency_key, '')), '') IS NULL
     OR pg_catalog.octet_length(p_build_idempotency_key) > 512 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_REQUEST_INVALID' USING ERRCODE = '22023';
  END IF;

  SELECT * INTO v_session
  FROM public.banking_pay_workbench_sessions
  WHERE id = p_workbench_session_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_NOT_FOUND' USING ERRCODE = 'P0001';
  END IF;
  IF v_session.actor_user_id IS DISTINCT FROM p_actor_user_id
     OR UPPER(v_session.status) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL
     OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_INVALID' USING ERRCODE = '55000';
  END IF;
  IF v_session.version < 1 OR v_session.progress_counter_version < 1
     OR v_session.authority_fence_generation < 1
     OR UPPER(v_session.progress_state) <> 'READY'
     OR NOT v_session.scope_seed_complete
     OR v_session.scope_total_count <> v_session.scope_seeded_count
     OR v_session.scope_total_count <> v_session.scope_ready_count
     OR v_session.scope_pending_count <> 0 OR v_session.scope_failed_count <> 0
     OR v_session.line_units_total <> v_session.line_units_ready
     OR v_session.line_units_pending <> 0 OR v_session.line_units_failed <> 0
     OR v_session.scope_change_generation_target <> v_session.scope_change_generation_applied
     OR v_session.scope_change_generation_target <> v_session.scope_change_generation_shadow_checked THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_NOT_SETTLED' USING ERRCODE = '55000';
  END IF;

  SELECT * INTO v_existing
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.workbench_session_id = p_workbench_session_id
    AND certificate.session_version = v_session.version
    AND certificate.progress_counter_version = v_session.progress_counter_version
    AND certificate.authority_fence_generation = v_session.authority_fence_generation
    AND certificate.build_idempotency_key = BTRIM(p_build_idempotency_key)
  LIMIT 1;
  IF FOUND THEN
    SELECT receipt.page_digest_sha256
    INTO v_last_receipt_sha256
    FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
    WHERE receipt.certificate_uuid=v_existing.certificate_uuid
      AND receipt.request_scope_key='CERTIFICATE_BUILD'
      AND receipt.page_kind='BUILD_APPEND'
      AND receipt.pay_channel_scope='ALL'
    ORDER BY receipt.page_sequence DESC
    LIMIT 1;
    RETURN pg_catalog.jsonb_build_object(
      'ok', v_existing.lifecycle IN ('BUILDING','SEALED_CURRENT'),
      'replayed', true,
      'certificate_uuid', v_existing.certificate_uuid,
      'build_id', v_existing.build_id,
      'certification_id', v_existing.certification_id,
      'overall_digest_sha256', v_existing.overall_digest_sha256,
      'lifecycle', v_existing.lifecycle,
      'selected_constituent_count', v_existing.selected_constituent_count,
      'next_after_ordinal',v_existing.build_after_ordinal,
      'last_page_receipt_sha256',v_last_receipt_sha256,
      'append_complete',v_existing.selected_constituents_digest_sha256 IS NOT NULL,
      'build_failure_code',v_existing.build_failure_code,
      'build_failure_message',v_existing.build_failure_message,
      'lease_owner',v_existing.lease_owner,
      'maximum_page_size',256,
      'maximum_requested_page_size',256,
      'maximum_build_emission_rows',64
    );
  END IF;

  -- A settled authority has one producer at a time. Different caller keys may
  -- not manufacture parallel certificates for the same immutable session
  -- generation; the exact current certificate is the canonical replay result.
  SELECT * INTO v_existing
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.workbench_session_id=p_workbench_session_id
    AND certificate.session_version=v_session.version
    AND certificate.progress_counter_version=v_session.progress_counter_version
    AND certificate.authority_fence_generation=v_session.authority_fence_generation
    AND certificate.lifecycle IN ('BUILDING','SEALED_CURRENT')
  ORDER BY CASE certificate.lifecycle WHEN 'SEALED_CURRENT' THEN 0 ELSE 1 END,
           certificate.created_at_utc
  LIMIT 1;
  IF FOUND THEN
    IF v_existing.lifecycle='SEALED_CURRENT' THEN
      SELECT receipt.page_digest_sha256
      INTO v_last_receipt_sha256
      FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
      WHERE receipt.certificate_uuid=v_existing.certificate_uuid
        AND receipt.request_scope_key='CERTIFICATE_BUILD'
        AND receipt.page_kind='BUILD_APPEND'
        AND receipt.pay_channel_scope='ALL'
      ORDER BY receipt.page_sequence DESC
      LIMIT 1;
      RETURN pg_catalog.jsonb_build_object(
        'ok',true,'replayed',true,'certificate_uuid',v_existing.certificate_uuid,
        'build_id',v_existing.build_id,'certification_id',v_existing.certification_id,
        'overall_digest_sha256',v_existing.overall_digest_sha256,
        'lifecycle',v_existing.lifecycle,
        'selected_constituent_count',v_existing.selected_constituent_count,
        'next_after_ordinal',v_existing.build_after_ordinal,
        'last_page_receipt_sha256',v_last_receipt_sha256,
        'append_complete',true,
        'lease_owner',v_existing.lease_owner,
        'maximum_page_size',256,
        'maximum_requested_page_size',256,
        'maximum_build_emission_rows',64
      );
    END IF;
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_ALREADY_IN_PROGRESS'
      USING ERRCODE='55000',DETAIL=pg_catalog.jsonb_build_object(
        'certificate_uuid',v_existing.certificate_uuid,'build_id',v_existing.build_id)::text;
  END IF;

  -- Admission counts at most the supported set plus one sentinel.  Exact row
  -- eligibility, economics, source identity and digests are deliberately
  -- checked by bounded append pages; doing that work here made a legitimate
  -- 50,000-row session exceed the fixed 15-second budget.
  SELECT COUNT(*)::integer
  INTO v_selected_count
  FROM (
    SELECT preview.id
    FROM public.banking_pay_workbench_preview_rows preview
    WHERE preview.session_id = p_workbench_session_id
      AND preview.session_version = v_session.version
      AND preview.selected
      AND UPPER(preview.selection_state) = 'SELECTED'
    LIMIT 50001
  ) bounded_selected;
  IF v_selected_count < 1 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NO_SELECTED_ROWS' USING ERRCODE = '22023';
  END IF;
  IF v_selected_count > 50000 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SELECTED_LIMIT_EXCEEDED' USING ERRCODE = '54000';
  END IF;
  IF v_session.selected_row_count IS DISTINCT FROM v_selected_count THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SELECTED_COUNTER_MISMATCH' USING ERRCODE = '55000';
  END IF;

  IF v_session.server_selected_preview_row_ids_provided IS NOT TRUE
     OR pg_catalog.jsonb_typeof(v_session.server_selected_preview_row_ids) IS DISTINCT FROM 'array'
     OR EXISTS (
       SELECT 1 FROM pg_catalog.jsonb_array_elements(v_session.server_selected_preview_row_ids) element(value)
       WHERE pg_catalog.jsonb_typeof(element.value) <> 'string'
          OR BTRIM(element.value #>> '{}') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     ) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SERVER_SELECTION_INVALID' USING ERRCODE = '22023';
  END IF;
  WITH server_ids AS MATERIALIZED (
    SELECT value::uuid AS preview_row_id
    FROM pg_catalog.jsonb_array_elements_text(v_session.server_selected_preview_row_ids) element(value)
  ), selected_ids AS MATERIALIZED (
    SELECT preview.id AS preview_row_id
    FROM public.banking_pay_workbench_preview_rows preview
    WHERE preview.session_id = p_workbench_session_id
      AND preview.session_version = v_session.version
      AND preview.selected
      AND UPPER(preview.selection_state) = 'SELECTED'
  )
  SELECT (SELECT COUNT(*) FROM server_ids)::integer,
         (SELECT COUNT(DISTINCT preview_row_id) FROM server_ids)::integer,
         (SELECT COUNT(*) FROM server_ids s LEFT JOIN selected_ids e USING (preview_row_id) WHERE e.preview_row_id IS NULL)::integer,
         (SELECT COUNT(*) FROM selected_ids e LEFT JOIN server_ids s USING (preview_row_id) WHERE s.preview_row_id IS NULL)::integer
  INTO v_server_id_count, v_server_distinct_count, v_server_missing_count, v_selected_missing_count;
  IF v_server_id_count <> v_selected_count OR v_server_distinct_count <> v_selected_count
     OR v_server_missing_count <> 0 OR v_selected_missing_count <> 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SERVER_SELECTION_MISMATCH' USING ERRCODE = '22023';
  END IF;
  -- BUILDING headers use harmless non-null placeholders for totals and
  -- partitions.  The final append page atomically replaces them with the exact
  -- values proved from every bounded constituent page before seal can run.
  v_partition_count := 1;
  v_selected_total := 0;

  v_filter_candidate_text := NULLIF(BTRIM(COALESCE(
    v_session.filters_json->>'candidate_filter_id', v_session.filters_json->>'candidateFilterId',
    v_session.filters_json->>'filter_candidate_id', v_session.filters_json->>'candidate_id',
    v_session.filters_json->>'candidateId', ''
  )), '');
  v_filter_client_text := NULLIF(BTRIM(COALESCE(
    v_session.filters_json->>'client_filter_id', v_session.filters_json->>'clientFilterId',
    v_session.filters_json->>'filter_client_id', v_session.filters_json->>'client_id',
    v_session.filters_json->>'clientId', ''
  )), '');
  IF v_filter_candidate_text IS NOT NULL AND v_filter_candidate_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_CONTEXT_INVALID' USING ERRCODE = '22023';
  END IF;
  IF v_filter_client_text IS NOT NULL AND v_filter_client_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_CONTEXT_INVALID' USING ERRCODE = '22023';
  END IF;
  v_filter_candidate_id := v_filter_candidate_text::uuid;
  v_filter_client_id := v_filter_client_text::uuid;
  v_filter_context := pg_catalog.jsonb_build_object(
    'candidate_filter_id', v_filter_candidate_id,
    'client_filter_id', v_filter_client_id,
    'filter_binding_mode', 'EXACT_CERTIFIED_SELECTED_UNIVERSE'
  );
  v_filter_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_filter_context)
  );

  IF v_filter_candidate_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM public.banking_pay_workbench_preview_rows preview
    WHERE preview.session_id = p_workbench_session_id
      AND preview.session_version = v_session.version
      AND preview.selected
      AND UPPER(preview.selection_state) = 'SELECTED'
      AND preview.candidate_id <> v_filter_candidate_id
  ) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_CONTEXT_MISMATCH' USING ERRCODE = '22023';
  END IF;
  IF v_filter_client_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM public.banking_pay_workbench_preview_rows preview
    WHERE preview.session_id = p_workbench_session_id
      AND preview.session_version = v_session.version
      AND preview.selected
      AND UPPER(preview.selection_state) = 'SELECTED'
      AND COALESCE(preview.row_json->>'client_id', '') <> v_filter_client_id::text
  ) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_CONTEXT_MISMATCH' USING ERRCODE = '22023';
  END IF;

  SELECT COUNT(*) FILTER (WHERE status = 'QUEUED')::integer,
         COUNT(*) FILTER (WHERE status = 'RUNNING')::integer
  INTO v_queued_jobs, v_running_jobs
  FROM public.banking_pay_workbench_jobs
  WHERE session_id = p_workbench_session_id
    AND status IN ('QUEUED','RUNNING');
  SELECT COUNT(*)::integer INTO v_invalid_pointer_count
  FROM public.banking_pay_workbench_session_scope scope
  LEFT JOIN public.banking_pay_workbench_jobs job ON job.id = scope.pending_job_id
  WHERE scope.session_id = p_workbench_session_id
    AND (scope.pending_job_id IS NOT NULL OR scope.status <> 'READY'
      OR (scope.pending_job_id IS NOT NULL AND (job.id IS NULL OR job.status NOT IN ('QUEUED','RUNNING'))));
  IF COALESCE(v_queued_jobs, 0) <> 0 OR COALESCE(v_running_jobs, 0) <> 0 OR v_invalid_pointer_count <> 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CURRENT_JOB_AUTHORITY_UNSETTLED' USING ERRCODE = '55000';
  END IF;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE scope.status <> 'READY'
           OR state.status <> 'READY'
           OR scope.certified_preview_publication_parity_ok IS NOT TRUE
           OR scope.certified_preview_publication_session_version IS DISTINCT FROM v_session.version
           OR scope.certified_preview_publication_source_change_seq IS DISTINCT FROM state.source_change_seq
           OR scope.certified_preview_publication_source_build_run_id IS NULL
           OR scope.certified_preview_publication_source_publication_id IS NULL
           OR pg_catalog.jsonb_typeof(scope.certified_preview_publication_attestation_json) IS DISTINCT FROM 'object')::integer
  INTO v_publication_count, v_invalid_publication_count
  FROM public.banking_pay_workbench_session_scope scope
  LEFT JOIN public.banking_pay_workbench_session_candidate_state state
    ON state.session_id = scope.session_id AND state.candidate_id = scope.candidate_id
  WHERE scope.session_id = p_workbench_session_id;
  IF v_publication_count <> v_session.scope_total_count OR v_invalid_publication_count <> 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PUBLICATION_AUTHORITY_INVALID' USING ERRCODE = '55000';
  END IF;

  v_gate := private.pay_workbench_modal_draft_gate_v2(p_workbench_session_id, v_selected_count);
  IF v_gate->'can_create_draft' IS DISTINCT FROM 'true'::jsonb
     OR COALESCE(v_gate->>'session_selected_eligible_ready_row_count', '') !~ '^[0-9]+$'
     OR (v_gate->>'session_selected_eligible_ready_row_count')::integer <> v_selected_count
     OR pg_catalog.jsonb_array_length(v_gate->'blocker_codes') <> 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DRAFT_GATE_REJECTED'
      USING ERRCODE = '55000', DETAIL = v_gate::text;
  END IF;
  v_gate_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_gate)
  );
  v_empty_digest := private.pay_workbench_settled_certificate_sha256_text_v8('[]');

  INSERT INTO private.banking_pay_workbench_settled_certificates_v8(
    certificate_uuid, certificate_contract, workbench_session_id, session_version,
    progress_counter_version, progress_state, source_snapshot_run_id, session_signature,
    pay_date, week_ending_cutoff, filters_digest_sha256,
    scope_change_generation_target, scope_change_generation_applied,
    scope_change_generation_shadow_checked, authority_fence_generation,
    publication_count, publications_digest_sha256,
    scope_total_count, scope_seeded_count, scope_ready_count, scope_pending_count, scope_failed_count,
    line_units_total, line_units_ready, line_units_pending, line_units_failed,
    selected_row_count, selected_page_order, selected_page_size_max, selected_pages_fetched,
    selected_terminal_sentinel_seen, selected_sentinel_overflow, all_selected_rows_loaded,
    server_selected_preview_row_ids_provided, server_selected_ids_equal_materialised_selected_ids,
    ready_action_required_blocked_pairwise_disjoint, active_draft_rows_excluded,
    ineligible_rows_excluded, snoozed_rows_excluded, unloaded_selection_gap_count,
    queued_current_job_count, running_current_job_count, unresolved_current_job_count,
    invalid_current_job_pointer_count, historical_terminal_rows_retained,
    historical_terminal_rows_are_not_current_authority, can_create_draft,
    selected_eligible_ready_row_count, blocking_reason_count, gate_digest_sha256,
    selected_constituent_count, selected_canonical_amount_ex_vat_total,
    selected_partition_count, selected_partitions_ordering,
    policy_contract_version, before_policy_projection_digest_sha256,
    after_policy_projection_digest_sha256, policy_digests_equal,
    execution_recovery_delta_only, forbidden_policy_delta_count, no_payment_policy_change,
    lifecycle, build_id, build_idempotency_key, lease_owner, lease_expires_at_utc,
    created_by_user_id, candidate_filter_id, client_filter_id,
    filter_binding_mode, filter_context_digest_sha256
  ) VALUES (
    v_certificate_uuid, 'WORKBENCH_SETTLED_CERTIFICATION_V2', p_workbench_session_id, v_session.version,
    v_session.progress_counter_version, 'READY', v_session.source_snapshot_run_id, v_session.session_signature,
    v_session.pay_date, v_session.week_ending_cutoff, v_filter_digest,
    v_session.scope_change_generation_target, v_session.scope_change_generation_applied,
    v_session.scope_change_generation_shadow_checked, v_session.authority_fence_generation,
    v_publication_count, v_empty_digest,
    v_session.scope_total_count, v_session.scope_seeded_count, v_session.scope_ready_count, 0, 0,
    v_session.line_units_total, v_session.line_units_ready, 0, 0,
    v_selected_count, 'row_ordinal asc, preview_row_id asc', 256, 1,
    true, false, true, true, true,
    true, true, true, true, 0,
    0, 0, 0, 0, true, true, true,
    v_selected_count, 0, v_gate_digest,
    v_selected_count, private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(v_selected_total)),
    v_partition_count, 'minimum constituent ordinal asc, candidate_id asc, resolved_pay_channel asc',
    'WORKBENCH_PAYMENT_POLICY_PARITY_V2', v_filter_digest, v_filter_digest, true,
    true, 0, true,
    'BUILDING', v_build_id, BTRIM(p_build_idempotency_key), p_actor_user_id::text,
    clock_timestamp() + interval '30 seconds', p_actor_user_id,
    v_filter_candidate_id, v_filter_client_id,
    'EXACT_CERTIFIED_SELECTED_UNIVERSE', v_filter_digest
  );

  INSERT INTO private.banking_pay_workbench_settled_certificate_source_members_v8(
    certificate_uuid, constituent_ordinal, preview_row_id, source_row_ordinal, source_row_digest_sha256
  )
  SELECT v_certificate_uuid,
         (ROW_NUMBER() OVER (ORDER BY preview.row_ordinal, preview.id) - 1)::integer,
         preview.id, preview.row_ordinal, NULL
  FROM public.banking_pay_workbench_preview_rows preview
  WHERE preview.session_id = p_workbench_session_id
    AND preview.session_version = v_session.version
    AND preview.selected
    AND UPPER(preview.selection_state) = 'SELECTED'
  ORDER BY preview.row_ordinal, preview.id;

  INSERT INTO private.banking_pay_workbench_settled_certificate_digest_runs_v8(
    certificate_uuid,stream_kind,pay_channel_scope,hash_contract,status,next_ordinal,
    stream_phase,current_state_json
  )
  SELECT v_certificate_uuid,'SELECTED_CONSTITUENTS',scope,
         'V1_STABLE_STRINGIFY_CONSTITUENTS','BUILDING',0,'ROWS',
         private.pay_workbench_settled_certificate_sha256_update_v8(
           private.pay_workbench_settled_certificate_sha256_init_v8(),'['
         )
  FROM (VALUES ('ALL'),('PAYE'),('UMBRELLA')) scopes(scope);

  INSERT INTO private.banking_pay_workbench_settled_certificate_publications_v8(
    certificate_uuid, scope_ordinal, candidate_id, candidate_state_id, candidate_state_status,
    source_change_seq, source_build_run_id, source_publication_id,
    certified_publication_session_version, publication_attestation_version,
    publication_attestation_digest_sha256, publication_parity_ok, publication_attested_at_utc
  )
  SELECT v_certificate_uuid, scope.scope_ordinal::integer, scope.candidate_id, state.id, state.status,
         scope.certified_preview_publication_source_change_seq,
         scope.certified_preview_publication_source_build_run_id,
         scope.certified_preview_publication_source_publication_id,
         scope.certified_preview_publication_session_version,
         CASE WHEN COALESCE(scope.certified_preview_publication_attestation_json->>'contract_version', '') ~ '^[0-9]+$'
           THEN (scope.certified_preview_publication_attestation_json->>'contract_version')::integer ELSE 1 END,
         private.pay_workbench_settled_certificate_sha256_text_v8(
           private.pay_workbench_settled_certificate_stable_stringify_v8(scope.certified_preview_publication_attestation_json)
         ), true,
         COALESCE(scope.certified_preview_publication_attested_at_utc, clock_timestamp())
  FROM public.banking_pay_workbench_session_scope scope
  JOIN public.banking_pay_workbench_session_candidate_state state
    ON state.session_id = scope.session_id AND state.candidate_id = scope.candidate_id
  WHERE scope.session_id = p_workbench_session_id
  ORDER BY scope.scope_ordinal, scope.candidate_id;

  INSERT INTO private.banking_pay_workbench_settled_certificate_policy_owners_v8(
    certificate_uuid,owner_ordinal,logical_owner_identity
  ) VALUES
    (v_certificate_uuid,0,'WORKBENCH_SELECTION_OWNER'),
    (v_certificate_uuid,1,'DRAFT_FINANCIAL_OWNERS_UNCHANGED');
  INSERT INTO private.banking_pay_workbench_settled_certificate_policy_surfaces_v8(
    certificate_uuid,surface_ordinal,compared_surface
  ) VALUES
    (v_certificate_uuid,0,'eligibility and selected constituents'),
    (v_certificate_uuid,1,'amount, sign, tax, VAT and economic keys'),
    (v_certificate_uuid,2,'pay method, channel, grouping and routing'),
    (v_certificate_uuid,3,'approval, hold, resolution, prior-paid, supersession and recovery/headroom'),
    (v_certificate_uuid,4,'pre-Draft source reservations'),
    (v_certificate_uuid,5,'unchanged Draft financial owner identities without Draft-produced facts'),
    (v_certificate_uuid,6,'provider, settlement, remittance and ultimate payment boundaries');

  INSERT INTO private.banking_pay_workbench_settled_certificate_universes_v8(
    certificate_uuid, universe_kind, row_count, universe_digest_sha256
  )
  SELECT v_certificate_uuid, kind, 0, v_empty_digest
  FROM (VALUES ('READY'),('ACTION_REQUIRED'),('BLOCKED'),('ACTIVE_DRAFT'),('INELIGIBLE'),('SNOOZED')) kinds(kind);
  INSERT INTO private.banking_pay_workbench_settled_certificate_digest_runs_v8(
    certificate_uuid,stream_kind,pay_channel_scope,hash_contract,status,next_ordinal,
    stream_phase,current_state_json
  ) VALUES (
    v_certificate_uuid,'UNIVERSE_CAPTURE','ALL','V2_STABLE_STRINGIFY_OVERALL',
    'BUILDING',0,'ROWS',pg_catalog.jsonb_build_object(
      'after_row_ordinal',NULL,'after_preview_row_id',NULL
    )
  );

  INSERT INTO private.banking_pay_workbench_settled_certificate_lifecycle_events_v8(
    certificate_uuid, event_sequence, event_kind, reason_code, actor_user_id
  ) VALUES (v_certificate_uuid, 0, 'BUILD_STARTED', 'CURRENT_SETTLED_WORKBENCH_AUTHORITY', p_actor_user_id);

  RETURN pg_catalog.jsonb_build_object(
    'ok', true, 'replayed', false,
    'certificate_uuid', v_certificate_uuid,
    'build_id', v_build_id,
    'lifecycle', 'BUILDING',
    'selected_constituent_count', v_selected_count,
    'selected_partition_count', NULL,
    'selected_canonical_amount_ex_vat_total', NULL,
    'classification_pending', true,
    'authority_fence_generation', v_session.authority_fence_generation,
    'next_after_ordinal', NULL,
    'last_page_receipt_sha256',NULL,
    'append_complete',false,
    'lease_owner',p_actor_user_id::text,
    'maximum_page_size', 256,
    'maximum_requested_page_size', 256,
    'maximum_build_emission_rows', 64
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_build_append_page_v8(
  p_certificate_uuid uuid,
  p_after_ordinal integer,
  p_requested_limit integer,
  p_expected_previous_receipt_sha256 text,
  p_lease_owner text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_existing_receipt private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_previous_receipt private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_member private.banking_pay_workbench_settled_certificate_source_members_v8%ROWTYPE;
  v_partition private.banking_pay_workbench_settled_certificate_partitions_v8%ROWTYPE;
  v_seed jsonb;
  v_component jsonb;
  v_component_seed jsonb;
  v_bound jsonb;
  v_all_elements jsonb := '[]'::jsonb;
  v_full_elements jsonb := '[]'::jsonb;
  v_decisive_elements jsonb := '[]'::jsonb;
  v_all_ids jsonb := '[]'::jsonb;
  v_full_ids jsonb := '[]'::jsonb;
  v_decisive_ids jsonb := '[]'::jsonb;
  v_all_digest text;
  v_full_digest text;
  v_decisive_digest text;
  v_constituent_digest text;
  v_empty_digest text := private.pay_workbench_settled_certificate_sha256_text_v8('[]');
  v_request_preimage_digest text;
  v_page_digest text;
  v_page_digests jsonb := '[]'::jsonb;
  v_page_sequence integer;
  v_row_count integer := 0;
  v_bounded_count integer;
  v_page_byte_count integer := 0;
  v_row_byte_count integer;
  v_member_ordinal integer;
  v_reservation_ordinal integer;
  v_evidence_ordinal integer;
  v_all_ordinal integer;
  v_next_after integer;
  v_has_more boolean;
  v_full_count integer;
  v_decisive_count integer;
  v_all_count integer;
  v_error_code text;
  v_error_message text;
  v_digest_run private.banking_pay_workbench_settled_certificate_digest_runs_v8%ROWTYPE;
  v_scope text;
  v_constituent_json text;
  v_state jsonb;
  v_source_row_digest text;
  v_new_partition_ordinal integer;
  v_final_entry_count integer;
  v_final_partition_count integer;
  v_final_selected_total numeric;
  v_partition_is_new boolean;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout', '15000', true);
  PERFORM pg_catalog.set_config('lock_timeout', '1500', true);
  IF p_certificate_uuid IS NULL
     OR p_requested_limit NOT BETWEEN 1 AND 256
     OR (p_after_ordinal IS NOT NULL AND (p_after_ordinal < 0 OR p_after_ordinal > 49999))
     OR NULLIF(BTRIM(COALESCE(p_lease_owner, '')), '') IS NULL
     OR pg_catalog.octet_length(p_lease_owner) > 512
     OR (p_expected_previous_receipt_sha256 IS NOT NULL
       AND p_expected_previous_receipt_sha256 !~ '^[0-9a-f]{64}$') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_PAGE_REQUEST_INVALID' USING ERRCODE = '22023';
  END IF;

  v_request_preimage_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
      'certificate_uuid', p_certificate_uuid,
      'stage_kind', 'BUILD_APPEND',
      'pay_channel_scope', NULL,
      'after_ordinal', p_after_ordinal,
      'requested_limit', p_requested_limit,
      'expected_previous_receipt_sha256', p_expected_previous_receipt_sha256
    ))
  );

  SELECT * INTO v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certificate_uuid = p_certificate_uuid
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE = 'P0001';
  END IF;
  SELECT * INTO v_existing_receipt
  FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
  WHERE receipt.certificate_uuid = p_certificate_uuid
    AND receipt.request_scope_key = 'CERTIFICATE_BUILD'
    AND receipt.page_kind = 'BUILD_APPEND'
    AND receipt.pay_channel_scope = 'ALL'
    AND receipt.after_ordinal IS NOT DISTINCT FROM p_after_ordinal;
  IF FOUND THEN
    IF v_existing_receipt.request_preimage_digest_sha256 IS DISTINCT FROM v_request_preimage_digest THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_PAGE_REPLAY_CONFLICT' USING ERRCODE = '23505';
    END IF;
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'replayed', true, 'certificate_uuid', p_certificate_uuid,
      'page_sequence', v_existing_receipt.page_sequence,
      'row_count', v_existing_receipt.row_count,
      'canonical_byte_count', v_existing_receipt.canonical_byte_count,
      'next_after_ordinal', v_existing_receipt.next_after_ordinal,
      'has_more', v_existing_receipt.has_more,
      'terminal_sentinel_present', v_existing_receipt.terminal_sentinel_present,
      'page_receipt_sha256', v_existing_receipt.page_digest_sha256
    );
  END IF;
  IF v_header.lifecycle <> 'BUILDING' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_NOT_ACTIVE' USING ERRCODE = '55000';
  END IF;
  IF v_header.build_after_ordinal IS DISTINCT FROM p_after_ordinal THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_PAGE_CURSOR_INVALID' USING ERRCODE = '55000';
  END IF;
  IF v_header.lease_owner IS NOT NULL
     AND v_header.lease_owner IS DISTINCT FROM BTRIM(p_lease_owner)
     AND v_header.lease_expires_at_utc > clock_timestamp() THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_LEASE_BUSY' USING ERRCODE = '55P03';
  END IF;
  SELECT * INTO v_previous_receipt
  FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
  WHERE receipt.certificate_uuid = p_certificate_uuid
    AND receipt.request_scope_key = 'CERTIFICATE_BUILD'
    AND receipt.page_kind = 'BUILD_APPEND'
    AND receipt.pay_channel_scope = 'ALL'
  ORDER BY receipt.page_sequence DESC
  LIMIT 1;
  IF (p_after_ordinal IS NULL AND FOUND)
     OR (p_after_ordinal IS NOT NULL AND (
       NOT FOUND
       OR v_previous_receipt.next_after_ordinal IS DISTINCT FROM p_after_ordinal
       OR p_expected_previous_receipt_sha256 IS DISTINCT FROM v_previous_receipt.page_digest_sha256
     )) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_PAGE_PREDECESSOR_INVALID' USING ERRCODE = '55000';
  END IF;
  IF p_after_ordinal IS NULL AND p_expected_previous_receipt_sha256 IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_PAGE_PREDECESSOR_INVALID' USING ERRCODE = '55000';
  END IF;

  SELECT * INTO v_session
  FROM public.banking_pay_workbench_sessions session
  WHERE session.id = v_header.workbench_session_id
  FOR SHARE;
  IF NOT FOUND
     OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.progress_state) <> 'READY'
     OR UPPER(v_session.status) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_AUTHORITY_CHANGED' USING ERRCODE = '55000';
  END IF;
  UPDATE private.banking_pay_workbench_settled_certificates_v8
  SET lease_owner = BTRIM(p_lease_owner), lease_expires_at_utc = clock_timestamp() + interval '30 seconds'
  WHERE certificate_uuid = p_certificate_uuid;

  BEGIN
    SELECT COUNT(*)::integer INTO v_bounded_count
    FROM (
      SELECT member.constituent_ordinal
      FROM private.banking_pay_workbench_settled_certificate_source_members_v8 member
      WHERE member.certificate_uuid = p_certificate_uuid
        AND (p_after_ordinal IS NULL OR member.constituent_ordinal > p_after_ordinal)
      ORDER BY member.constituent_ordinal
       LIMIT LEAST(p_requested_limit,64) + 1
     ) bounded;
    v_has_more := v_bounded_count > LEAST(p_requested_limit,64);

    FOR v_member IN
      SELECT member.*
      FROM private.banking_pay_workbench_settled_certificate_source_members_v8 member
      WHERE member.certificate_uuid = p_certificate_uuid
        AND (p_after_ordinal IS NULL OR member.constituent_ordinal > p_after_ordinal)
      ORDER BY member.constituent_ordinal
      LIMIT LEAST(p_requested_limit,64)
    LOOP
      v_source_row_digest := private.pay_workbench_settled_certificate_source_row_digest_v8(
        v_member.preview_row_id
      );
      IF v_source_row_digest IS NULL THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_ROW_NOT_FOUND' USING ERRCODE = '55000';
      END IF;
      IF v_member.source_row_digest_sha256 IS NOT NULL
         AND v_member.source_row_digest_sha256 IS DISTINCT FROM v_source_row_digest THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_ROW_CHANGED' USING ERRCODE = '55000';
      END IF;
      UPDATE private.banking_pay_workbench_settled_certificate_source_members_v8
      SET source_row_digest_sha256 = v_source_row_digest
      WHERE certificate_uuid = p_certificate_uuid
        AND constituent_ordinal = v_member.constituent_ordinal;

      v_seed := private.pay_workbench_settled_certificate_constituent_seed_v8(
        p_certificate_uuid, v_member.constituent_ordinal
      );
      v_all_elements := '[]'::jsonb;
      v_full_elements := '[]'::jsonb;
      v_decisive_elements := '[]'::jsonb;
      v_all_ids := '[]'::jsonb;
      v_full_ids := '[]'::jsonb;
      v_decisive_ids := '[]'::jsonb;

      v_all_ordinal := 0;
      FOR v_component, v_evidence_ordinal IN
        SELECT component.value, (component.ordinality - 1)::integer
        FROM pg_catalog.jsonb_array_elements(v_seed->'case_components') WITH ORDINALITY component(value, ordinality)
        ORDER BY component.ordinality
      LOOP
        v_component_seed := private.pay_workbench_settled_certificate_component_seed_v8(
          v_component, v_evidence_ordinal, v_seed->>'economic_key_type', v_seed->>'economic_key_value'
        );
        IF v_component_seed IS NULL THEN CONTINUE; END IF;
        IF v_all_ids @> pg_catalog.jsonb_build_array(v_component_seed->'stable_component_id') THEN
          RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_COMPONENT_IDENTITY_DUPLICATE' USING ERRCODE = '22023';
        END IF;
        v_bound := v_component_seed->'bound_facts';
        v_all_ids := v_all_ids || pg_catalog.jsonb_build_array(v_component_seed->'stable_component_id');
        v_all_elements := v_all_elements || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'stable_component_id', v_component_seed->>'stable_component_id', 'bound_facts', v_bound
        ));
        IF v_component_seed->'full_signed_pre_signature_match' = 'true'::jsonb THEN
          v_full_ids := v_full_ids || pg_catalog.jsonb_build_array(v_component_seed->'stable_component_id');
          v_full_elements := v_full_elements || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'stable_component_id', v_component_seed->>'stable_component_id', 'bound_facts', v_bound,
            'signed_pre_signature', pg_catalog.jsonb_build_object(
              'component_fallback', v_bound->'component_fallback',
              'authoritative_truth_ex_vat', v_bound->'authoritative_truth_ex_vat',
              'authoritative_baseline_ex_vat', v_bound->'authoritative_baseline_ex_vat'
            )
          ));
        END IF;
        IF v_component_seed->'decisive_signed_evidence' = 'true'::jsonb THEN
          v_decisive_ids := v_decisive_ids || pg_catalog.jsonb_build_array(v_component_seed->'stable_component_id');
          v_decisive_elements := v_decisive_elements || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'stable_component_id', v_component_seed->>'stable_component_id', 'bound_facts', v_bound,
            'signed_pre_signature', pg_catalog.jsonb_build_object(
              'component_fallback', v_bound->'component_fallback',
              'authoritative_truth_ex_vat', v_bound->'authoritative_truth_ex_vat',
              'authoritative_baseline_ex_vat', v_bound->'authoritative_baseline_ex_vat'
            ),
            'decisive_frozen_evidence', pg_catalog.jsonb_build_object(
              'authoritative_reserved_ex_vat', v_bound->'authoritative_reserved_ex_vat',
              'authoritative_outstanding_ex_vat', v_bound->'authoritative_outstanding_ex_vat',
              'component_amount_ex_vat', v_bound->'component_amount_ex_vat',
              'source_pay_ex_vat', v_bound->'source_pay_ex_vat',
              'source_charge_ex_vat', v_bound->'source_charge_ex_vat',
              'financial_revision_digest', v_bound->'financial_revision_digest',
              'target_authority_digest', v_bound->'target_authority_digest',
              'conversion_context_digest', v_bound->'conversion_context_digest',
              'physical_bucket_key', v_bound->'physical_bucket_key',
              'physical_bucket_digest', v_bound->'physical_bucket_digest',
              'sealed_evidence_digest', v_bound->'sealed_evidence_digest',
              'source_pay_method', v_bound->'source_pay_method',
              'target_pay_method', v_bound->'target_pay_method'
            )
          ));
        END IF;
      END LOOP;
      v_all_count := pg_catalog.jsonb_array_length(v_all_elements);
      v_full_count := pg_catalog.jsonb_array_length(v_full_elements);
      v_decisive_count := pg_catalog.jsonb_array_length(v_decisive_elements);
      IF v_full_count > 1 OR v_decisive_count > 1 OR v_decisive_count > v_full_count THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_COMPONENT_EVIDENCE_CARDINALITY_INVALID' USING ERRCODE = '22023';
      END IF;
      v_all_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(v_all_elements));
      v_full_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(v_full_elements));
      v_decisive_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(v_decisive_elements));

      INSERT INTO private.banking_pay_workbench_settled_certificate_entries_v8(
        certificate_uuid, constituent_ordinal, preview_row_id, materialised_preview_row_id,
        presentation_preview_row_id, row_key, line_id, source_kind, source_line_id,
        source_row_key, source_publication_id, source_change_seq, source_build_run_id,
        source_identity_digest_sha256, candidate_publication_ordinal, candidate_id, client_id,
        timesheet_id, resolved_pay_channel, resolved_payment_method, amount_sign, semantic_kind,
        economic_key_timesheet_id, economic_key_type, economic_key_value, canonical_amount_ex_vat,
        source_reservation_amount_ex_vat, prior_payment_treatment, prior_paid_amount_ex_vat,
        prior_payment_evidence_digest_sha256, supersession_treatment,
        supersession_evidence_digest_sha256, recovery_contract_version,
        recovery_nominal_due_amount_ex_vat, recovery_recoverable_this_pay_run_ex_vat,
        recovery_headroom_amount_ex_vat, recovery_allocated_amount_ex_vat, recovery_result_kind,
        recovery_overlay_digest_sha256, expected_allocation_basis_kind,
        expected_allocated_recovery_amount_ex_vat, expected_allocation_result,
        expected_allocation_source_digest_sha256, expected_item_semantic_kind,
        expected_item_source_identity_digest_sha256, expected_item_amount_ex_vat,
        expected_item_source_digest_sha256, expected_reservation_applicability,
        expected_reservation_amount_ex_vat, expected_reservation_source_digest_sha256,
        all_same_key_count, all_same_key_digest_sha256, signed_match_count,
        signed_match_digest_sha256, decisive_signed_count, decisive_signed_digest_sha256,
        readiness_class, selection_state, selected, draftable, is_ready_for_draft,
        constituent_digest_sha256
      ) VALUES (
        p_certificate_uuid, v_member.constituent_ordinal, (v_seed->>'preview_row_id')::uuid,
        (v_seed->>'preview_row_id')::uuid, v_seed->>'presentation_preview_row_id',
        v_seed->>'row_key', v_seed->>'line_id', v_seed->>'source_kind', v_seed->>'source_line_id',
        v_seed->>'source_row_key', (v_seed->>'source_publication_id')::uuid,
        (v_seed->>'source_change_seq')::bigint, (v_seed->>'source_build_run_id')::uuid,
        v_seed->>'source_identity_digest_sha256', (v_seed->>'candidate_publication_ordinal')::integer,
        (v_seed->>'candidate_id')::uuid, NULLIF(v_seed->>'client_id','')::uuid,
        NULLIF(v_seed->>'timesheet_id','')::uuid, v_seed->>'resolved_pay_channel',
        v_seed->>'resolved_payment_method', v_seed->>'amount_sign', v_seed->>'semantic_kind',
        NULLIF(v_seed->>'timesheet_id','')::uuid, v_seed->>'economic_key_type',
        v_seed->>'economic_key_value', v_seed->>'canonical_amount_ex_vat',
        v_seed->>'source_reservation_amount_ex_vat', v_seed->>'prior_payment_treatment',
        v_seed->>'prior_paid_amount_ex_vat', v_seed->>'prior_payment_evidence_digest_sha256',
        v_seed->>'supersession_treatment', v_seed->>'supersession_evidence_digest_sha256',
        v_seed->>'recovery_contract_version', v_seed->>'recovery_nominal_due_amount_ex_vat',
        v_seed->>'recovery_recoverable_this_pay_run_ex_vat', v_seed->>'recovery_headroom_amount_ex_vat',
        v_seed->>'recovery_allocated_amount_ex_vat', v_seed->>'recovery_result_kind',
        v_seed->>'recovery_overlay_digest_sha256', v_seed->>'expected_allocation_basis_kind',
        v_seed->>'expected_allocated_recovery_amount_ex_vat', v_seed->>'expected_allocation_result',
        v_seed->>'expected_allocation_source_digest_sha256', v_seed->>'expected_item_semantic_kind',
        v_seed->>'expected_item_source_identity_digest_sha256', v_seed->>'expected_item_amount_ex_vat',
        v_seed->>'expected_item_source_digest_sha256', v_seed->>'expected_reservation_applicability',
        v_seed->>'expected_reservation_amount_ex_vat', v_seed->>'expected_reservation_source_digest_sha256',
        v_all_count, v_all_digest, v_full_count, v_full_digest, v_decisive_count, v_decisive_digest,
        'READY', 'SELECTED', true, true, true, v_empty_digest
      );

      v_reservation_ordinal := 0;
      FOR v_component IN SELECT value FROM pg_catalog.jsonb_array_elements(v_seed->'source_reservation_ids')
      LOOP
        INSERT INTO private.banking_pay_workbench_settled_cert_source_reservations_v8(
          certificate_uuid, constituent_ordinal, reservation_ordinal, source_reservation_id
        ) VALUES (p_certificate_uuid, v_member.constituent_ordinal, v_reservation_ordinal, v_component #>> '{}');
        v_reservation_ordinal := v_reservation_ordinal + 1;
      END LOOP;

      v_reservation_ordinal := 0;
      FOR v_component IN SELECT value FROM pg_catalog.jsonb_array_elements(v_seed->'superseded_source_ids')
      LOOP
        INSERT INTO private.banking_pay_workbench_settled_certificate_superseded_sources_v8(
          certificate_uuid,constituent_ordinal,source_ordinal,superseded_source_id
        ) VALUES (
          p_certificate_uuid,v_member.constituent_ordinal,v_reservation_ordinal,v_component #>> '{}'
        );
        v_reservation_ordinal:=v_reservation_ordinal+1;
      END LOOP;

      FOR v_component, v_evidence_ordinal IN
        SELECT component.value, (component.ordinality - 1)::integer
        FROM pg_catalog.jsonb_array_elements(v_seed->'case_components') WITH ORDINALITY component(value, ordinality)
        ORDER BY component.ordinality
      LOOP
        v_component_seed := private.pay_workbench_settled_certificate_component_seed_v8(
          v_component, v_evidence_ordinal, v_seed->>'economic_key_type', v_seed->>'economic_key_value'
        );
        IF v_component_seed IS NULL THEN CONTINUE; END IF;
        v_bound := v_component_seed->'bound_facts';
        INSERT INTO private.banking_pay_workbench_settled_certificate_component_evidence_v8(
          certificate_uuid,constituent_ordinal,evidence_kind,evidence_ordinal,stable_component_id,
          frozen_component_ordinal,source_component_kind,economic_key_type,economic_key_value,
          component_fallback,authoritative_truth_ex_vat,authoritative_baseline_ex_vat,
          authoritative_reserved_ex_vat,authoritative_outstanding_ex_vat,component_amount_ex_vat,
          source_pay_ex_vat,source_charge_ex_vat,financial_revision_digest,target_authority_digest,
          conversion_context_digest,physical_bucket_key,physical_bucket_digest,sealed_evidence_digest,
          source_pay_method,target_pay_method,full_signed_pre_signature_match,
          decisive_signed_evidence,bound_component_digest_sha256
        ) VALUES (
          p_certificate_uuid,v_member.constituent_ordinal,'ALL_SAME_ECONOMIC_KEY',
          v_all_ordinal,
          v_component_seed->>'stable_component_id',(v_bound->>'frozen_component_ordinal')::integer,
          v_bound->>'source_component_kind',v_bound->>'economic_key_type',v_bound->>'economic_key_value',
          v_bound->>'component_fallback',v_bound->>'authoritative_truth_ex_vat',
          v_bound->>'authoritative_baseline_ex_vat',v_bound->>'authoritative_reserved_ex_vat',
          v_bound->>'authoritative_outstanding_ex_vat',v_bound->>'component_amount_ex_vat',
          v_bound->>'source_pay_ex_vat',v_bound->>'source_charge_ex_vat',
          v_bound->>'financial_revision_digest',v_bound->>'target_authority_digest',
          v_bound->>'conversion_context_digest',v_bound->>'physical_bucket_key',
          v_bound->>'physical_bucket_digest',v_bound->>'sealed_evidence_digest',
          v_bound->>'source_pay_method',v_bound->>'target_pay_method',
          (v_component_seed->>'full_signed_pre_signature_match')::boolean,
          (v_component_seed->>'decisive_signed_evidence')::boolean,
          v_component_seed->>'bound_component_digest_sha256'
        );
        v_all_ordinal := v_all_ordinal + 1;
      END LOOP;
      -- Copy the exact qualifying facts
      -- into the FULL and DECISIVE relations. These separate relations are the
      -- machine proof that cardinality was not applied to ordinary matches.
      INSERT INTO private.banking_pay_workbench_settled_certificate_component_evidence_v8
      SELECT certificate_uuid,constituent_ordinal,'FULL_SIGNED_PRE_SIGNATURE',
             (ROW_NUMBER() OVER (ORDER BY frozen_component_ordinal,stable_component_id)-1)::integer,
             stable_component_id,frozen_component_ordinal,source_component_kind,economic_key_type,economic_key_value,
             component_fallback,authoritative_truth_ex_vat,authoritative_baseline_ex_vat,
             authoritative_reserved_ex_vat,authoritative_outstanding_ex_vat,component_amount_ex_vat,
             source_pay_ex_vat,source_charge_ex_vat,financial_revision_digest,target_authority_digest,
             conversion_context_digest,physical_bucket_key,physical_bucket_digest,sealed_evidence_digest,
             source_pay_method,target_pay_method,full_signed_pre_signature_match,decisive_signed_evidence,
             bound_component_digest_sha256
      FROM private.banking_pay_workbench_settled_certificate_component_evidence_v8
      WHERE certificate_uuid=p_certificate_uuid AND constituent_ordinal=v_member.constituent_ordinal
        AND evidence_kind='ALL_SAME_ECONOMIC_KEY' AND full_signed_pre_signature_match
      ORDER BY frozen_component_ordinal,stable_component_id;
      INSERT INTO private.banking_pay_workbench_settled_certificate_component_evidence_v8
      SELECT certificate_uuid,constituent_ordinal,'DECISIVE_SIGNED_EVIDENCE',
             (ROW_NUMBER() OVER (ORDER BY frozen_component_ordinal,stable_component_id)-1)::integer,
             stable_component_id,frozen_component_ordinal,source_component_kind,economic_key_type,economic_key_value,
             component_fallback,authoritative_truth_ex_vat,authoritative_baseline_ex_vat,
             authoritative_reserved_ex_vat,authoritative_outstanding_ex_vat,component_amount_ex_vat,
             source_pay_ex_vat,source_charge_ex_vat,financial_revision_digest,target_authority_digest,
             conversion_context_digest,physical_bucket_key,physical_bucket_digest,sealed_evidence_digest,
             source_pay_method,target_pay_method,full_signed_pre_signature_match,decisive_signed_evidence,
             bound_component_digest_sha256
      FROM private.banking_pay_workbench_settled_certificate_component_evidence_v8
      WHERE certificate_uuid=p_certificate_uuid AND constituent_ordinal=v_member.constituent_ordinal
        AND evidence_kind='ALL_SAME_ECONOMIC_KEY' AND decisive_signed_evidence
      ORDER BY frozen_component_ordinal,stable_component_id;

      v_constituent_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(
          private.pay_workbench_settled_certificate_constituent_unsigned_v8(
            p_certificate_uuid, v_member.constituent_ordinal
          )
        )
      );
      UPDATE private.banking_pay_workbench_settled_certificate_entries_v8
      SET constituent_digest_sha256 = v_constituent_digest
      WHERE certificate_uuid = p_certificate_uuid AND constituent_ordinal = v_member.constituent_ordinal;
      v_constituent_json := private.pay_workbench_settled_certificate_stable_stringify_v8(
        private.pay_workbench_settled_certificate_constituent_json_v8(p_certificate_uuid, v_member.constituent_ordinal)
      );
      v_row_byte_count := pg_catalog.octet_length(v_constituent_json);
      IF v_row_byte_count > 65536 THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CONSTITUENT_BYTE_LIMIT_EXCEEDED' USING ERRCODE = '54000';
      END IF;
      v_page_byte_count := v_page_byte_count + v_row_byte_count;
      IF v_page_byte_count > 524288 THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTE_LIMIT_EXCEEDED' USING ERRCODE = '54000';
      END IF;

      SELECT * INTO v_partition
      FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
      WHERE partition.certificate_uuid = p_certificate_uuid
        AND partition.candidate_id = (v_seed->>'candidate_id')::uuid
        AND partition.resolved_pay_channel = v_seed->>'resolved_pay_channel';
      v_partition_is_new := NOT FOUND;
      IF v_partition_is_new THEN
        SELECT COUNT(*)::integer INTO v_new_partition_ordinal
        FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
        WHERE partition.certificate_uuid = p_certificate_uuid;
        INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
          certificate_uuid,partition_ordinal,candidate_id,resolved_pay_channel,
          constituent_count,canonical_amount_ex_vat_total,partition_digest_sha256
        ) VALUES (
          p_certificate_uuid,v_new_partition_ordinal,(v_seed->>'candidate_id')::uuid,
          v_seed->>'resolved_pay_channel',1,v_seed->>'canonical_amount_ex_vat',v_empty_digest
        ) RETURNING * INTO v_partition;
      END IF;
      v_member_ordinal := CASE WHEN v_partition_is_new THEN 0 ELSE v_partition.constituent_count END;
      INSERT INTO private.banking_pay_workbench_settled_certificate_partition_members_v8(
        certificate_uuid,stream_ordinal,partition_ordinal,member_ordinal,constituent_ordinal,
        stable_identity_digest_sha256
      ) VALUES (
        p_certificate_uuid,v_member.constituent_ordinal,v_partition.partition_ordinal,v_member_ordinal,
        v_member.constituent_ordinal,v_constituent_digest
      );
      IF NOT v_partition_is_new THEN
        UPDATE private.banking_pay_workbench_settled_certificate_partitions_v8
        SET constituent_count = constituent_count + 1,
            canonical_amount_ex_vat_total = private.pay_workbench_settled_certificate_money_v8(
              pg_catalog.to_jsonb(canonical_amount_ex_vat_total::numeric + (v_seed->>'canonical_amount_ex_vat')::numeric)
            )
        WHERE certificate_uuid=p_certificate_uuid
          AND partition_ordinal=v_partition.partition_ordinal;
      END IF;
      FOREACH v_scope IN ARRAY ARRAY['ALL',v_seed->>'resolved_pay_channel'] LOOP
        SELECT * INTO STRICT v_digest_run
        FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
        WHERE digest_run.certificate_uuid=p_certificate_uuid
          AND digest_run.stream_kind='SELECTED_CONSTITUENTS'
          AND digest_run.pay_channel_scope=v_scope
        FOR UPDATE;
        v_state := private.pay_workbench_settled_certificate_sha256_update_v8(
          v_digest_run.current_state_json,
          CASE WHEN v_digest_run.next_ordinal=0 THEN '' ELSE ',' END || v_constituent_json
        );
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
        SET current_state_json=v_state,next_ordinal=next_ordinal+1
        WHERE digest_run_uuid=v_digest_run.digest_run_uuid;
      END LOOP;
      v_page_digests := v_page_digests || pg_catalog.jsonb_build_array(v_constituent_digest);
      v_row_count := v_row_count + 1;
      v_next_after := v_member.constituent_ordinal;
    END LOOP;

    IF v_row_count = 0 THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_PAGE_EMPTY' USING ERRCODE = '55000';
    END IF;
    v_page_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
      private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
        'request_preimage_digest_sha256',v_request_preimage_digest,
        'constituent_digests',v_page_digests,
        'row_count',v_row_count,'canonical_byte_count',v_page_byte_count,
        'next_after_ordinal',v_next_after,'has_more',v_has_more,
        'terminal_sentinel_present',true
      ))
    );
    IF NOT v_has_more THEN
      SELECT COUNT(*)::integer,ROUND(COALESCE(SUM(entry.canonical_amount_ex_vat::numeric),0),2)
      INTO v_final_entry_count,v_final_selected_total
      FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
      WHERE entry.certificate_uuid=p_certificate_uuid;
      SELECT COUNT(*)::integer INTO v_final_partition_count
      FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
      WHERE partition.certificate_uuid=p_certificate_uuid;
      IF v_final_entry_count IS DISTINCT FROM v_header.selected_constituent_count
         OR v_final_partition_count < 1 THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_COMPLETENESS_INVALID' USING ERRCODE='55000';
      END IF;
      UPDATE private.banking_pay_workbench_settled_certificates_v8
      SET selected_partition_count=v_final_partition_count,
          selected_canonical_amount_ex_vat_total=private.pay_workbench_settled_certificate_money_v8(
            pg_catalog.to_jsonb(v_final_selected_total)
          )
      WHERE certificate_uuid=p_certificate_uuid;

      INSERT INTO private.banking_pay_workbench_settled_certificate_digest_runs_v8(
        certificate_uuid,stream_kind,pay_channel_scope,hash_contract,status,next_ordinal,
        stream_phase,current_partition_ordinal,current_member_ordinal,current_state_json
      )
      SELECT p_certificate_uuid,'PARTITION_OBJECT:'||partition.partition_ordinal::text,'ALL',
             'V1_STABLE_STRINGIFY_PARTITIONS','BUILDING',0,'IDENTITY_DIGESTS',
             partition.partition_ordinal,NULL,
             private.pay_workbench_settled_certificate_sha256_update_v8(
               private.pay_workbench_settled_certificate_sha256_init_v8(),
               '{"candidate_id":'||pg_catalog.to_jsonb(partition.candidate_id)::text||
               ',"canonical_amount_ex_vat_total":'||pg_catalog.to_jsonb(partition.canonical_amount_ex_vat_total)::text||
               ',"constituent_count":'||partition.constituent_count::text||
               ',"ordered_constituent_identity_digests":['
             )
      FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
      WHERE partition.certificate_uuid=p_certificate_uuid
      ORDER BY partition.partition_ordinal;

      FOR v_digest_run IN
        SELECT * FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
        WHERE digest_run.certificate_uuid=p_certificate_uuid
          AND digest_run.stream_kind='SELECTED_CONSTITUENTS'
        ORDER BY digest_run.pay_channel_scope
        FOR UPDATE
      LOOP
        v_state := private.pay_workbench_settled_certificate_sha256_update_v8(v_digest_run.current_state_json,']');
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
        SET current_state_json=v_state,status='COMPLETE',stream_phase='COMPLETE',
            final_digest_sha256=private.pay_workbench_settled_certificate_sha256_final_v8(v_state)
        WHERE digest_run_uuid=v_digest_run.digest_run_uuid;
      END LOOP;
      UPDATE private.banking_pay_workbench_settled_certificates_v8 header
      SET selected_constituents_digest_sha256=digest_run.final_digest_sha256
      FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
      WHERE header.certificate_uuid=p_certificate_uuid
        AND digest_run.certificate_uuid=header.certificate_uuid
        AND digest_run.stream_kind='SELECTED_CONSTITUENTS'
        AND digest_run.pay_channel_scope='ALL';
    END IF;
    SELECT COALESCE(MAX(page_sequence),-1)+1 INTO v_page_sequence
    FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8
    WHERE certificate_uuid=p_certificate_uuid AND request_scope_key='CERTIFICATE_BUILD'
      AND page_kind='BUILD_APPEND' AND pay_channel_scope='ALL';
    INSERT INTO private.banking_pay_workbench_settled_certificate_page_receipts_v8(
      certificate_uuid,request_scope_key,page_kind,pay_channel_scope,page_sequence,after_ordinal,requested_limit,
      expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,canonical_byte_count,
      next_after_ordinal,has_more,terminal_sentinel_present,page_digest_sha256
    ) VALUES (
      p_certificate_uuid,'CERTIFICATE_BUILD','BUILD_APPEND','ALL',v_page_sequence,p_after_ordinal,p_requested_limit,
      p_expected_previous_receipt_sha256,v_request_preimage_digest,v_row_count,v_page_byte_count,
      v_next_after,v_has_more,true,v_page_digest
    );
    UPDATE private.banking_pay_workbench_settled_certificates_v8
    SET build_after_ordinal=v_next_after,
        selected_pages_fetched=(SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8
          WHERE certificate_uuid=p_certificate_uuid AND request_scope_key='CERTIFICATE_BUILD'
            AND page_kind='BUILD_APPEND' AND pay_channel_scope='ALL'),
        lease_expires_at_utc=CASE WHEN v_has_more THEN clock_timestamp()+interval '30 seconds' ELSE NULL END,
        lease_owner=CASE WHEN v_has_more THEN BTRIM(p_lease_owner) ELSE NULL END
    WHERE certificate_uuid=p_certificate_uuid;
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'replayed',false,'certificate_uuid',p_certificate_uuid,'page_sequence',v_page_sequence,
      'row_count',v_row_count,'canonical_byte_count',v_page_byte_count,'next_after_ordinal',v_next_after,
      'has_more',v_has_more,'terminal_sentinel_present',true,'page_receipt_sha256',v_page_digest
    );
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_code = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
    UPDATE private.banking_pay_workbench_settled_certificates_v8
    SET lifecycle='BUILD_FAILED',build_failure_code=v_error_code,
        build_failure_message=LEFT(v_error_message,2000),build_failed_at_utc=clock_timestamp(),
        lease_owner=NULL,lease_expires_at_utc=NULL
    WHERE certificate_uuid=p_certificate_uuid AND lifecycle='BUILDING';
    INSERT INTO private.banking_pay_workbench_settled_certificate_lifecycle_events_v8(
      certificate_uuid,event_sequence,event_kind,reason_code
    ) SELECT p_certificate_uuid,COALESCE(MAX(event_sequence),-1)+1,'BUILD_FAILED',v_error_code
      FROM private.banking_pay_workbench_settled_certificate_lifecycle_events_v8
      WHERE certificate_uuid=p_certificate_uuid;
    RETURN pg_catalog.jsonb_build_object('ok',false,'replayed',false,'certificate_uuid',p_certificate_uuid,
      'lifecycle','BUILD_FAILED','error_code',v_error_code,'error_message',LEFT(v_error_message,2000));
  END;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_seal_v8(
  p_certificate_uuid uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_run private.banking_pay_workbench_settled_certificate_digest_runs_v8%ROWTYPE;
  v_partition private.banking_pay_workbench_settled_certificate_partitions_v8%ROWTYPE;
  v_state jsonb;
  v_fragment text := '';
  v_scope text;
  v_kind text;
  v_next_partition integer;
  v_next_member integer;
  v_row_count integer;
  v_bounded_count integer;
  v_has_more boolean;
  v_digest text;
  v_unsigned jsonb;
  v_manifests jsonb;
  v_overall_result jsonb;
  v_event_sequence bigint;
  v_old record;
  v_after_row_ordinal bigint;
  v_after_preview_row_id uuid;
  v_next_row_ordinal bigint;
  v_next_preview_row_id uuid;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout','15000',true);
  PERFORM pg_catalog.set_config('lock_timeout','1500',true);
  IF p_certificate_uuid IS NULL OR p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SEAL_REQUEST_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certificate_uuid=p_certificate_uuid
  FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE='P0001'; END IF;
  IF v_header.lifecycle='SEALED_CURRENT' THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'replayed',true,
      'certificate_uuid',p_certificate_uuid,'certification_id',v_header.certification_id,
      'overall_digest_sha256',v_header.overall_digest_sha256,'lifecycle',v_header.lifecycle);
  END IF;
  IF v_header.lifecycle<>'BUILDING' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_BUILD_NOT_ACTIVE' USING ERRCODE='55000';
  END IF;
  IF v_header.created_by_user_id IS DISTINCT FROM p_actor_user_id THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_ACTOR_MISMATCH' USING ERRCODE='42501';
  END IF;
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions session
  WHERE session.id=v_header.workbench_session_id FOR SHARE;
  IF NOT FOUND OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.progress_state)<>'READY' OR UPPER(v_session.status)<>'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_AUTHORITY_CHANGED' USING ERRCODE='55000';
  END IF;
  IF (SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_entries_v8
      WHERE certificate_uuid=p_certificate_uuid)<>v_header.selected_constituent_count
     OR EXISTS (
       SELECT 1
       FROM private.banking_pay_workbench_settled_certificate_source_members_v8 member
       WHERE member.certificate_uuid=p_certificate_uuid
         AND member.source_row_digest_sha256 IS NULL
     ) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SOURCE_MEMBERSHIP_CHANGED' USING ERRCODE='55000';
  END IF;

  -- Capture the complete Ready / Action Required / Blocked and exclusion
  -- universes in fixed keyset pages.  Every call rechecks the same session
  -- generation above, so a publication or selection change stops the build;
  -- no Candidate row locks or unbounded JSON arrays are used.
  SELECT * INTO v_run
  FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
  WHERE digest_run.certificate_uuid=p_certificate_uuid
    AND digest_run.stream_kind='UNIVERSE_CAPTURE'
    AND digest_run.status='BUILDING'
  LIMIT 1 FOR UPDATE;
  IF FOUND THEN
    v_after_row_ordinal := NULLIF(v_run.current_state_json->>'after_row_ordinal','')::bigint;
    v_after_preview_row_id := NULLIF(v_run.current_state_json->>'after_preview_row_id','')::uuid;
    SELECT COUNT(*)::integer INTO v_bounded_count
    FROM (
      SELECT preview.id
      FROM public.banking_pay_workbench_preview_rows preview
      WHERE preview.session_id=v_header.workbench_session_id
        AND preview.session_version=v_header.session_version
        AND (v_after_row_ordinal IS NULL
          OR (preview.row_ordinal,preview.id)>(v_after_row_ordinal,v_after_preview_row_id))
      ORDER BY preview.row_ordinal,preview.id
      LIMIT 257
    ) bounded;
    v_has_more:=v_bounded_count>256;
    v_row_count:=LEAST(v_bounded_count,256);

    WITH page AS MATERIALIZED (
      SELECT preview.*
      FROM public.banking_pay_workbench_preview_rows preview
      WHERE preview.session_id=v_header.workbench_session_id
        AND preview.session_version=v_header.session_version
        AND (v_after_row_ordinal IS NULL
          OR (preview.row_ordinal,preview.id)>(v_after_row_ordinal,v_after_preview_row_id))
      ORDER BY preview.row_ordinal,preview.id
      LIMIT 256
    ), classified AS MATERIALIZED (
      SELECT page.*,
        private.pay_workbench_settled_certificate_preview_contract_v8(page.id) contract,
        private.pay_workbench_settled_certificate_sha256_text_v8(
          private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
            'preview_row_id',page.id,'row_key',page.row_key,'section',page.section,
            'row_ordinal',page.row_ordinal,'status',page.status,
            'selection_state',page.selection_state,'selected',page.selected,
            'selection_identity_digest',page.row_json->>'selection_identity_digest'
          ))
        ) stable_digest
      FROM page
    ), flagged AS MATERIALIZED (
      SELECT classified.*,
        UPPER(BTRIM(COALESCE(
          NULLIF(row_json#>>'{snooze_state,state}',''),
          CASE WHEN pg_catalog.jsonb_typeof(row_json->'snooze_state')='string'
            THEN NULLIF(row_json->>'snooze_state','') END,
          row_json->>'blocked_snooze_state',''
        ))) NOT IN ('','NONE','NOT_SNOOZED','CLEARED') AS is_snoozed,
        EXISTS (
          SELECT 1 FROM pg_catalog.jsonb_array_elements_text(CASE
            WHEN pg_catalog.jsonb_typeof(row_json->'blocked_reason_codes')='array'
              THEN row_json->'blocked_reason_codes' ELSE '[]'::jsonb END) reason(value)
          WHERE UPPER(reason.value) LIKE '%DRAFT%'
        ) AS is_active_draft,
        COALESCE(
          row_json->'eligible'='false'::jsonb
          OR row_json->'is_eligible'='false'::jsonb
          OR UPPER(COALESCE(row_json->>'eligibility_state','')) IN ('INELIGIBLE','NOT_ELIGIBLE','EXCLUDED'),
          false
        ) AS is_ineligible
      FROM classified
    ), memberships AS (
      SELECT row_ordinal,id,stable_digest,CASE contract->>'effective_section'
        WHEN 'canonical_preview_lines' THEN 'READY'
        WHEN 'cases_resolutions' THEN 'ACTION_REQUIRED'
        ELSE 'BLOCKED' END universe_kind
      FROM flagged WHERE NOT is_snoozed AND NOT is_active_draft AND NOT is_ineligible
      UNION ALL SELECT row_ordinal,id,stable_digest,'SNOOZED' FROM flagged WHERE is_snoozed
      UNION ALL SELECT row_ordinal,id,stable_digest,'ACTIVE_DRAFT' FROM flagged WHERE is_active_draft
      UNION ALL SELECT row_ordinal,id,stable_digest,'INELIGIBLE' FROM flagged WHERE is_ineligible
    ), numbered AS (
      SELECT membership.universe_kind,membership.stable_digest,
        COALESCE((SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 existing
          WHERE existing.certificate_uuid=p_certificate_uuid
            AND existing.universe_kind=membership.universe_kind),0)::integer
        +(ROW_NUMBER() OVER (PARTITION BY membership.universe_kind ORDER BY membership.row_ordinal,membership.id)-1)::integer
          AS member_ordinal
      FROM memberships membership
    )
    INSERT INTO private.banking_pay_workbench_settled_certificate_universe_members_v8(
      certificate_uuid,universe_kind,member_ordinal,stable_identity_digest_sha256
    )
    SELECT p_certificate_uuid,universe_kind,member_ordinal,stable_digest
    FROM numbered
    ORDER BY universe_kind,member_ordinal;

    SELECT page.row_ordinal,page.id
    INTO v_next_row_ordinal,v_next_preview_row_id
    FROM (
      SELECT preview.row_ordinal,preview.id
      FROM public.banking_pay_workbench_preview_rows preview
      WHERE preview.session_id=v_header.workbench_session_id
        AND preview.session_version=v_header.session_version
        AND (v_after_row_ordinal IS NULL
          OR (preview.row_ordinal,preview.id)>(v_after_row_ordinal,v_after_preview_row_id))
      ORDER BY preview.row_ordinal,preview.id
      LIMIT 256
    ) page
    ORDER BY page.row_ordinal DESC,page.id DESC
    LIMIT 1;

    IF NOT v_has_more THEN
      UPDATE private.banking_pay_workbench_settled_certificate_universes_v8 universe
      SET row_count=(SELECT COUNT(*)::integer
        FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
        WHERE member.certificate_uuid=p_certificate_uuid
          AND member.universe_kind=universe.universe_kind)
      WHERE universe.certificate_uuid=p_certificate_uuid;
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
      SET status='COMPLETE',stream_phase='COMPLETE',next_ordinal=next_ordinal+v_row_count,
          current_state_json=pg_catalog.jsonb_build_object(
            'after_row_ordinal',v_next_row_ordinal,'after_preview_row_id',v_next_preview_row_id)
      WHERE digest_run_uuid=v_run.digest_run_uuid;
    ELSE
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
      SET next_ordinal=next_ordinal+v_row_count,
          current_state_json=pg_catalog.jsonb_build_object(
            'after_row_ordinal',v_next_row_ordinal,'after_preview_row_id',v_next_preview_row_id)
      WHERE digest_run_uuid=v_run.digest_run_uuid;
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'sealed',false,'stage','UNIVERSE_CAPTURE',
      'page_row_count',v_row_count,'has_more',v_has_more,'terminal_sentinel_present',true);
  END IF;

  -- Finish each partition's own exact V1 object digest.  Both identity digests
  -- and ordinals are streamed from the normalized member relation in pages of
  -- at most 256; build_start and append never construct a giant partition.
  SELECT * INTO v_run
  FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
  WHERE digest_run.certificate_uuid=p_certificate_uuid
    AND digest_run.stream_kind LIKE 'PARTITION_OBJECT:%'
    AND digest_run.status='BUILDING'
  ORDER BY digest_run.current_partition_ordinal
  LIMIT 1 FOR UPDATE;
  IF FOUND THEN
    SELECT * INTO STRICT v_partition
    FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
    WHERE partition.certificate_uuid=p_certificate_uuid
      AND partition.partition_ordinal=v_run.current_partition_ordinal;
    IF v_run.stream_phase NOT IN ('IDENTITY_DIGESTS','ORDINALS') THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PARTITION_DIGEST_PHASE_INVALID' USING ERRCODE='55000';
    END IF;
    SELECT COUNT(*)::integer INTO v_bounded_count FROM (
      SELECT member.member_ordinal
      FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
      WHERE member.certificate_uuid=p_certificate_uuid
        AND member.partition_ordinal=v_partition.partition_ordinal
        AND (v_run.current_member_ordinal IS NULL OR member.member_ordinal>v_run.current_member_ordinal)
      ORDER BY member.member_ordinal LIMIT 257
    ) bounded;
    v_has_more := v_bounded_count>256;
    SELECT COALESCE(pg_catalog.string_agg(
      CASE WHEN v_run.next_ordinal=0 AND member.member_ordinal=(SELECT MIN(m2.member_ordinal) FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 m2 WHERE m2.certificate_uuid=p_certificate_uuid AND m2.partition_ordinal=v_partition.partition_ordinal AND (v_run.current_member_ordinal IS NULL OR m2.member_ordinal>v_run.current_member_ordinal)) THEN '' ELSE ',' END ||
        CASE WHEN v_run.stream_phase='IDENTITY_DIGESTS'
          THEN pg_catalog.to_jsonb(member.stable_identity_digest_sha256)::text
          ELSE member.constituent_ordinal::text END,
      '' ORDER BY member.member_ordinal),'') ,COUNT(*)::integer,MAX(member.member_ordinal)
    INTO v_fragment,v_row_count,v_next_member
    FROM (
      SELECT member.* FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
      WHERE member.certificate_uuid=p_certificate_uuid AND member.partition_ordinal=v_partition.partition_ordinal
        AND (v_run.current_member_ordinal IS NULL OR member.member_ordinal>v_run.current_member_ordinal)
      ORDER BY member.member_ordinal LIMIT 256
    ) member;
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
    IF NOT v_has_more THEN
      IF v_run.next_ordinal+v_row_count<>v_partition.constituent_count THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PARTITION_MEMBER_COUNT_MISMATCH' USING ERRCODE='55000';
      END IF;
      IF v_run.stream_phase='IDENTITY_DIGESTS' THEN
        v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(
          v_state,'],"ordered_constituent_ordinals":[');
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
        SET current_state_json=v_state,next_ordinal=0,current_member_ordinal=NULL,
            stream_phase='ORDINALS'
        WHERE digest_run_uuid=v_run.digest_run_uuid;
      ELSE
        v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,
          '],"partition_ordinal":'||v_partition.partition_ordinal::text||
          ',"resolved_pay_channel":'||pg_catalog.to_jsonb(v_partition.resolved_pay_channel)::text||'}');
        v_digest:=private.pay_workbench_settled_certificate_sha256_final_v8(v_state);
        UPDATE private.banking_pay_workbench_settled_certificate_partitions_v8
        SET partition_digest_sha256=v_digest
        WHERE certificate_uuid=p_certificate_uuid AND partition_ordinal=v_partition.partition_ordinal;
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
        SET current_state_json=v_state,next_ordinal=next_ordinal+v_row_count,
            current_member_ordinal=v_next_member,status='COMPLETE',stream_phase='COMPLETE',
            final_digest_sha256=v_digest
        WHERE digest_run_uuid=v_run.digest_run_uuid;
      END IF;
    ELSE
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
      SET current_state_json=v_state,next_ordinal=next_ordinal+v_row_count,
          current_member_ordinal=v_next_member
      WHERE digest_run_uuid=v_run.digest_run_uuid;
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'sealed',false,'stage','PARTITION_DIGESTS',
      'partition_ordinal',v_partition.partition_ordinal,'page_row_count',v_row_count,'has_more',true);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8
    WHERE certificate_uuid=p_certificate_uuid AND stream_kind='SELECTED_PARTITIONS'
  ) THEN
    INSERT INTO private.banking_pay_workbench_settled_certificate_digest_runs_v8(
      certificate_uuid,stream_kind,pay_channel_scope,hash_contract,status,next_ordinal,
      stream_phase,current_state_json
    ) SELECT p_certificate_uuid,'SELECTED_PARTITIONS',scope,'V1_STABLE_STRINGIFY_PARTITIONS',
      'BUILDING',0,'PARTITION_START',private.pay_workbench_settled_certificate_sha256_update_v8(
        private.pay_workbench_settled_certificate_sha256_init_v8(),'[')
      FROM (VALUES('ALL'),('PAYE'),('UMBRELLA')) scopes(scope);
  END IF;
  SELECT * INTO v_run FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
  WHERE digest_run.certificate_uuid=p_certificate_uuid AND digest_run.stream_kind='SELECTED_PARTITIONS'
    AND digest_run.status='BUILDING'
  ORDER BY CASE digest_run.pay_channel_scope WHEN 'ALL' THEN 0 WHEN 'PAYE' THEN 1 ELSE 2 END
  LIMIT 1 FOR UPDATE;
  IF FOUND THEN
    IF v_run.stream_phase='PARTITION_START' THEN
      SELECT MIN(partition.partition_ordinal) INTO v_next_partition
      FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
      WHERE partition.certificate_uuid=p_certificate_uuid
        AND (v_run.pay_channel_scope='ALL' OR partition.resolved_pay_channel=v_run.pay_channel_scope)
        AND (v_run.current_partition_ordinal IS NULL OR partition.partition_ordinal>v_run.current_partition_ordinal);
      IF v_next_partition IS NULL THEN
        v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,']');
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
        SET current_state_json=v_state,status='COMPLETE',stream_phase='COMPLETE',
            final_digest_sha256=private.pay_workbench_settled_certificate_sha256_final_v8(v_state)
        WHERE digest_run_uuid=v_run.digest_run_uuid;
      ELSE
        SELECT * INTO STRICT v_partition FROM private.banking_pay_workbench_settled_certificate_partitions_v8
        WHERE certificate_uuid=p_certificate_uuid AND partition_ordinal=v_next_partition;
        v_fragment:=CASE WHEN v_run.next_ordinal=0 THEN '' ELSE ',' END||
          '{"candidate_id":'||pg_catalog.to_jsonb(v_partition.candidate_id)::text||
          ',"canonical_amount_ex_vat_total":'||pg_catalog.to_jsonb(v_partition.canonical_amount_ex_vat_total)::text||
          ',"constituent_count":'||v_partition.constituent_count::text||
          ',"ordered_constituent_identity_digests":[';
        v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
        SET current_state_json=v_state,stream_phase='PARTITION_IDENTITIES',
            current_partition_ordinal=v_next_partition,current_member_ordinal=NULL
        WHERE digest_run_uuid=v_run.digest_run_uuid;
      END IF;
    ELSIF v_run.stream_phase IN ('PARTITION_IDENTITIES','PARTITION_ORDINALS') THEN
      SELECT * INTO STRICT v_partition FROM private.banking_pay_workbench_settled_certificate_partitions_v8
      WHERE certificate_uuid=p_certificate_uuid AND partition_ordinal=v_run.current_partition_ordinal;
      SELECT COUNT(*)::integer INTO v_bounded_count FROM (
        SELECT member.member_ordinal FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
        WHERE member.certificate_uuid=p_certificate_uuid AND member.partition_ordinal=v_partition.partition_ordinal
          AND (v_run.current_member_ordinal IS NULL OR member.member_ordinal>v_run.current_member_ordinal)
        ORDER BY member.member_ordinal LIMIT 257
      ) bounded;
      v_has_more:=v_bounded_count>256;
      IF v_run.stream_phase='PARTITION_IDENTITIES' THEN
        SELECT COALESCE(pg_catalog.string_agg(
          CASE WHEN v_run.current_member_ordinal IS NULL AND member.member_ordinal=0 THEN '' ELSE ',' END||pg_catalog.to_jsonb(member.stable_identity_digest_sha256)::text,
          '' ORDER BY member.member_ordinal),''),COUNT(*)::integer,MAX(member.member_ordinal)
        INTO v_fragment,v_row_count,v_next_member FROM (
          SELECT member.* FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
          WHERE member.certificate_uuid=p_certificate_uuid AND member.partition_ordinal=v_partition.partition_ordinal
            AND (v_run.current_member_ordinal IS NULL OR member.member_ordinal>v_run.current_member_ordinal)
          ORDER BY member.member_ordinal LIMIT 256
        ) member;
      ELSE
        SELECT COALESCE(pg_catalog.string_agg(
          CASE WHEN v_run.current_member_ordinal IS NULL AND member.member_ordinal=0 THEN '' ELSE ',' END||member.constituent_ordinal::text,
          '' ORDER BY member.member_ordinal),''),COUNT(*)::integer,MAX(member.member_ordinal)
        INTO v_fragment,v_row_count,v_next_member FROM (
          SELECT member.* FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
          WHERE member.certificate_uuid=p_certificate_uuid AND member.partition_ordinal=v_partition.partition_ordinal
            AND (v_run.current_member_ordinal IS NULL OR member.member_ordinal>v_run.current_member_ordinal)
          ORDER BY member.member_ordinal LIMIT 256
        ) member;
      END IF;
      v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
      IF NOT v_has_more THEN
        IF v_run.stream_phase='PARTITION_IDENTITIES' THEN
          v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,'],"ordered_constituent_ordinals":[');
          UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
          SET current_state_json=v_state,stream_phase='PARTITION_ORDINALS',current_member_ordinal=NULL
          WHERE digest_run_uuid=v_run.digest_run_uuid;
        ELSE
          v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,
            '],"partition_digest_sha256":'||pg_catalog.to_jsonb(v_partition.partition_digest_sha256)::text||
            ',"partition_ordinal":'||v_partition.partition_ordinal::text||
            ',"resolved_pay_channel":'||pg_catalog.to_jsonb(v_partition.resolved_pay_channel)::text||'}');
          UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
          SET current_state_json=v_state,stream_phase='PARTITION_START',current_member_ordinal=NULL,
              next_ordinal=next_ordinal+1
          WHERE digest_run_uuid=v_run.digest_run_uuid;
        END IF;
      ELSE
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
        SET current_state_json=v_state,current_member_ordinal=v_next_member
        WHERE digest_run_uuid=v_run.digest_run_uuid;
      END IF;
    ELSE
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PARTITION_STREAM_PHASE_INVALID' USING ERRCODE='55000';
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'sealed',false,'stage','SELECTED_PARTITION_STREAMS',
      'pay_channel_scope',v_run.pay_channel_scope,'has_more',true);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8
    WHERE certificate_uuid=p_certificate_uuid AND stream_kind='PUBLICATIONS'
  ) THEN
    INSERT INTO private.banking_pay_workbench_settled_certificate_digest_runs_v8(
      certificate_uuid,stream_kind,pay_channel_scope,hash_contract,status,next_ordinal,
      stream_phase,current_state_json
    ) VALUES (p_certificate_uuid,'PUBLICATIONS','ALL','V2_STABLE_STRINGIFY_OVERALL','BUILDING',0,'ROWS',
      private.pay_workbench_settled_certificate_sha256_update_v8(private.pay_workbench_settled_certificate_sha256_init_v8(),'['));
    INSERT INTO private.banking_pay_workbench_settled_certificate_digest_runs_v8(
      certificate_uuid,stream_kind,pay_channel_scope,hash_contract,status,next_ordinal,
      stream_phase,current_state_json
    ) SELECT p_certificate_uuid,'UNIVERSE:'||kind,'ALL','V2_STABLE_STRINGIFY_OVERALL','BUILDING',0,'ROWS',
      private.pay_workbench_settled_certificate_sha256_update_v8(private.pay_workbench_settled_certificate_sha256_init_v8(),'[')
      FROM (VALUES('READY'),('ACTION_REQUIRED'),('BLOCKED'),('ACTIVE_DRAFT'),('INELIGIBLE'),('SNOOZED')) kinds(kind);
  END IF;
  SELECT * INTO v_run FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
  WHERE digest_run.certificate_uuid=p_certificate_uuid AND digest_run.status='BUILDING'
    AND (digest_run.stream_kind='PUBLICATIONS' OR digest_run.stream_kind LIKE 'UNIVERSE:%')
  ORDER BY digest_run.stream_kind LIMIT 1 FOR UPDATE;
  IF FOUND THEN
    v_fragment:='';v_row_count:=0;v_next_member:=NULL;
    IF v_run.stream_kind='PUBLICATIONS' THEN
      SELECT COUNT(*)::integer INTO v_bounded_count FROM (
        SELECT publication.scope_ordinal FROM private.banking_pay_workbench_settled_certificate_publications_v8 publication
        WHERE publication.certificate_uuid=p_certificate_uuid AND publication.scope_ordinal>=v_run.next_ordinal
        ORDER BY publication.scope_ordinal LIMIT 257
      ) bounded;
      v_has_more:=v_bounded_count>256;
      SELECT COALESCE(pg_catalog.string_agg(
        CASE WHEN v_run.next_ordinal=0 AND publication.scope_ordinal=0 THEN '' ELSE ',' END||
        private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
          'scope_ordinal',publication.scope_ordinal,'candidate_id',publication.candidate_id,
          'candidate_state_id',publication.candidate_state_id,'candidate_state_status',publication.candidate_state_status,
          'source_change_seq',publication.source_change_seq,'source_build_run_id',publication.source_build_run_id,
          'source_publication_id',publication.source_publication_id,
          'certified_publication_session_version',publication.certified_publication_session_version,
          'publication_attestation_version',publication.publication_attestation_version,
          'publication_attestation_digest_sha256',publication.publication_attestation_digest_sha256,
          'publication_parity_ok',publication.publication_parity_ok,
          'publication_attested_at_utc',publication.publication_attested_at_utc)),
        '' ORDER BY publication.scope_ordinal),''),COUNT(*)::integer,MAX(publication.scope_ordinal)
      INTO v_fragment,v_row_count,v_next_member FROM (
        SELECT publication.* FROM private.banking_pay_workbench_settled_certificate_publications_v8 publication
        WHERE publication.certificate_uuid=p_certificate_uuid AND publication.scope_ordinal>=v_run.next_ordinal
        ORDER BY publication.scope_ordinal LIMIT 256
      ) publication;
    ELSE
      v_kind:=SUBSTRING(v_run.stream_kind FROM 10);
      SELECT COUNT(*)::integer INTO v_bounded_count FROM (
        SELECT member.member_ordinal FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
        WHERE member.certificate_uuid=p_certificate_uuid AND member.universe_kind=v_kind
          AND member.member_ordinal>=v_run.next_ordinal ORDER BY member.member_ordinal LIMIT 257
      ) bounded;
      v_has_more:=v_bounded_count>256;
      SELECT COALESCE(pg_catalog.string_agg(
        CASE WHEN v_run.next_ordinal=0 AND member.member_ordinal=0 THEN '' ELSE ',' END||
          pg_catalog.to_jsonb(member.stable_identity_digest_sha256)::text,
        '' ORDER BY member.member_ordinal),''),COUNT(*)::integer,MAX(member.member_ordinal)
      INTO v_fragment,v_row_count,v_next_member FROM (
        SELECT member.* FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
        WHERE member.certificate_uuid=p_certificate_uuid AND member.universe_kind=v_kind
          AND member.member_ordinal>=v_run.next_ordinal ORDER BY member.member_ordinal LIMIT 256
      ) member;
    END IF;
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
    IF NOT v_has_more THEN
      v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,']');
      v_digest:=private.pay_workbench_settled_certificate_sha256_final_v8(v_state);
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
      SET current_state_json=v_state,status='COMPLETE',stream_phase='COMPLETE',
          next_ordinal=next_ordinal+v_row_count,final_digest_sha256=v_digest
      WHERE digest_run_uuid=v_run.digest_run_uuid;
      IF v_run.stream_kind='PUBLICATIONS' THEN
        UPDATE private.banking_pay_workbench_settled_certificates_v8 SET publications_digest_sha256=v_digest
        WHERE certificate_uuid=p_certificate_uuid;
      ELSE
        UPDATE private.banking_pay_workbench_settled_certificate_universes_v8
        SET universe_digest_sha256=v_digest WHERE certificate_uuid=p_certificate_uuid AND universe_kind=v_kind;
      END IF;
    ELSE
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8
      SET current_state_json=v_state,next_ordinal=next_ordinal+v_row_count,current_member_ordinal=v_next_member
      WHERE digest_run_uuid=v_run.digest_run_uuid;
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'sealed',false,'stage','AUTHORITY_STREAMS',
      'stream_kind',v_run.stream_kind,'page_row_count',v_row_count,'has_more',true);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 WHERE certificate_uuid=p_certificate_uuid) THEN
    FOREACH v_scope IN ARRAY ARRAY['ALL','PAYE','UMBRELLA'] LOOP
      SELECT pg_catalog.jsonb_build_object(
        'pay_channel_scope',v_scope,
        'constituent_count',(SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry WHERE entry.certificate_uuid=p_certificate_uuid AND (v_scope='ALL' OR entry.resolved_pay_channel=v_scope)),
        'partition_count',(SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition WHERE partition.certificate_uuid=p_certificate_uuid AND (v_scope='ALL' OR partition.resolved_pay_channel=v_scope)),
        'canonical_amount_ex_vat_total',private.pay_workbench_settled_certificate_money_v8(pg_catalog.to_jsonb(COALESCE((SELECT SUM(entry.canonical_amount_ex_vat::numeric) FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry WHERE entry.certificate_uuid=p_certificate_uuid AND (v_scope='ALL' OR entry.resolved_pay_channel=v_scope)),0))),
        'selected_constituents_digest_sha256',(SELECT final_digest_sha256 FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 WHERE certificate_uuid=p_certificate_uuid AND stream_kind='SELECTED_CONSTITUENTS' AND pay_channel_scope=v_scope),
        'selected_partitions_digest_sha256',(SELECT final_digest_sha256 FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 WHERE certificate_uuid=p_certificate_uuid AND stream_kind='SELECTED_PARTITIONS' AND pay_channel_scope=v_scope)
      ) INTO v_unsigned;
      INSERT INTO private.banking_pay_workbench_settled_certificate_channel_manifests_v8(
        certificate_uuid,pay_channel_scope,constituent_count,partition_count,
        canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
        selected_partitions_digest_sha256,manifest_digest_sha256
      ) VALUES (p_certificate_uuid,v_scope,(v_unsigned->>'constituent_count')::integer,
        (v_unsigned->>'partition_count')::integer,v_unsigned->>'canonical_amount_ex_vat_total',
        v_unsigned->>'selected_constituents_digest_sha256',v_unsigned->>'selected_partitions_digest_sha256',
        private.pay_workbench_settled_certificate_sha256_text_v8(
          private.pay_workbench_settled_certificate_stable_stringify_v8(v_unsigned)));
    END LOOP;
    SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'pay_channel_scope',manifest.pay_channel_scope,'constituent_count',manifest.constituent_count,
      'partition_count',manifest.partition_count,'canonical_amount_ex_vat_total',manifest.canonical_amount_ex_vat_total,
      'selected_constituents_digest_sha256',manifest.selected_constituents_digest_sha256,
      'selected_partitions_digest_sha256',manifest.selected_partitions_digest_sha256,
      'manifest_digest_sha256',manifest.manifest_digest_sha256)
      ORDER BY CASE manifest.pay_channel_scope WHEN 'ALL' THEN 0 WHEN 'PAYE' THEN 1 ELSE 2 END)
    INTO v_manifests FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
    WHERE manifest.certificate_uuid=p_certificate_uuid;
    UPDATE private.banking_pay_workbench_settled_certificates_v8 header
    SET selected_partitions_digest_sha256=(SELECT final_digest_sha256 FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 WHERE certificate_uuid=p_certificate_uuid AND stream_kind='SELECTED_PARTITIONS' AND pay_channel_scope='ALL'),
        manifests_digest_sha256=private.pay_workbench_settled_certificate_sha256_text_v8(
          private.pay_workbench_settled_certificate_stable_stringify_v8(v_manifests))
    WHERE header.certificate_uuid=p_certificate_uuid;
    SELECT pg_catalog.jsonb_build_object(
      'candidate_filter_id',v_header.candidate_filter_id,'client_filter_id',v_header.client_filter_id,
      'filter_binding_mode','EXACT_CERTIFIED_SELECTED_UNIVERSE',
      'filter_context_digest_sha256',v_header.filter_context_digest_sha256,
      'constituent_count',manifest.constituent_count,
      'selected_constituents_digest_sha256',manifest.selected_constituents_digest_sha256,
      'partition_count',manifest.partition_count,
      'selected_partitions_digest_sha256',manifest.selected_partitions_digest_sha256,
      'canonical_amount_ex_vat_total',manifest.canonical_amount_ex_vat_total
    ) INTO v_unsigned FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
    WHERE manifest.certificate_uuid=p_certificate_uuid AND manifest.pay_channel_scope='ALL';
    INSERT INTO private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8(
      certificate_uuid,candidate_filter_id,client_filter_id,filter_binding_mode,
      filter_context_digest_sha256,constituent_count,selected_constituents_digest_sha256,
      partition_count,selected_partitions_digest_sha256,canonical_amount_ex_vat_total,
      manifest_digest_sha256
    ) VALUES (p_certificate_uuid,v_header.candidate_filter_id,v_header.client_filter_id,
      'EXACT_CERTIFIED_SELECTED_UNIVERSE',v_header.filter_context_digest_sha256,
      (v_unsigned->>'constituent_count')::integer,v_unsigned->>'selected_constituents_digest_sha256',
      (v_unsigned->>'partition_count')::integer,v_unsigned->>'selected_partitions_digest_sha256',
      v_unsigned->>'canonical_amount_ex_vat_total',private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(v_unsigned)));
    RETURN pg_catalog.jsonb_build_object('ok',true,'sealed',false,'stage','MANIFESTS','has_more',true);
  END IF;

  -- Pin the certification instant once. It is part of the exact V2 overall
  -- stableStringify payload, so retries must reuse it byte-for-byte.
  UPDATE private.banking_pay_workbench_settled_certificates_v8
  SET certified_at_utc=COALESCE(certified_at_utc,clock_timestamp())
  WHERE certificate_uuid=p_certificate_uuid AND lifecycle='BUILDING';

  v_overall_result:=private.pay_workbench_settled_certificate_overall_digest_advance_v8(
    p_certificate_uuid,256
  );
  IF COALESCE((v_overall_result->>'complete')::boolean,false) IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'sealed',false,'stage','OVERALL_DIGEST',
      'digest_stage',v_overall_result->>'stage','has_more',true
    ) || v_overall_result;
  END IF;
  v_digest:=v_overall_result->>'overall_digest_sha256';
  IF COALESCE(v_digest,'') !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERALL_DIGEST_INVALID' USING ERRCODE='55000';
  END IF;

  -- Fail closed on every compact manifest/header reconciliation before the
  -- certificate becomes visible to a Draft consumer.
  IF NOT EXISTS (
    SELECT 1
    FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
    WHERE manifest.certificate_uuid=p_certificate_uuid
      AND manifest.pay_channel_scope='ALL'
      AND manifest.constituent_count=v_header.selected_constituent_count
      AND manifest.partition_count=v_header.selected_partition_count
      AND manifest.canonical_amount_ex_vat_total=v_header.selected_canonical_amount_ex_vat_total
      AND manifest.selected_constituents_digest_sha256=v_header.selected_constituents_digest_sha256
      AND manifest.selected_partitions_digest_sha256=v_header.selected_partitions_digest_sha256
  ) OR (SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8
        WHERE certificate_uuid=p_certificate_uuid)<>3
     OR (SELECT COUNT(*) FROM private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8
         WHERE certificate_uuid=p_certificate_uuid)<>1 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FINAL_MANIFEST_RECONCILIATION_FAILED' USING ERRCODE='55000';
  END IF;

  SELECT manifest.manifest_digest_sha256
  INTO STRICT v_scope
  FROM private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8 manifest
  WHERE manifest.certificate_uuid=p_certificate_uuid;

  UPDATE private.banking_pay_workbench_settled_certificates_v8
  SET overall_digest_sha256=v_digest,
      certification_id='WORKBENCH_SETTLED_CERTIFICATION_V2:'||v_digest,
      filter_scope_manifest_digest_sha256=v_scope,
      lifecycle='SEALED_CURRENT',sealed_at_utc=clock_timestamp(),
      lease_owner=NULL,lease_expires_at_utc=NULL
  WHERE certificate_uuid=p_certificate_uuid AND lifecycle='BUILDING';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FINAL_SEAL_CONFLICT' USING ERRCODE='55000';
  END IF;

  -- A genuinely replaced Workbench session stops being admissible, but its
  -- certificate and every linked/FROZEN operation remain immutable audit
  -- evidence. This is lifecycle metadata only; it does not alter Draft facts.
  FOR v_old IN
    SELECT old_certificate.certificate_uuid
    FROM private.banking_pay_workbench_settled_certificates_v8 old_certificate
    JOIN public.banking_pay_workbench_sessions old_session
      ON old_session.id=old_certificate.workbench_session_id
    WHERE old_certificate.lifecycle='SEALED_CURRENT'
      AND old_certificate.certificate_uuid<>p_certificate_uuid
      AND old_session.replacement_session_id=v_header.workbench_session_id
    ORDER BY old_certificate.certificate_uuid
  LOOP
    UPDATE private.banking_pay_workbench_settled_certificates_v8
    SET lifecycle='SUPERSEDED_BY_NEW_SESSION',superseded_by_certificate_uuid=p_certificate_uuid
    WHERE certificate_uuid=v_old.certificate_uuid AND lifecycle='SEALED_CURRENT';
    IF FOUND THEN
      INSERT INTO private.banking_pay_workbench_settled_certificate_lifecycle_events_v8(
        certificate_uuid,event_sequence,event_kind,reason_code,actor_user_id,related_certificate_uuid
      ) SELECT v_old.certificate_uuid,COALESCE(MAX(event_sequence),-1)+1,
          'CERTIFICATE_SUPERSEDED','NEW_SETTLED_WORKBENCH_SESSION',p_actor_user_id,p_certificate_uuid
        FROM private.banking_pay_workbench_settled_certificate_lifecycle_events_v8
        WHERE certificate_uuid=v_old.certificate_uuid;
    END IF;
  END LOOP;

  SELECT COALESCE(MAX(event_sequence),-1)+1 INTO v_event_sequence
  FROM private.banking_pay_workbench_settled_certificate_lifecycle_events_v8
  WHERE certificate_uuid=p_certificate_uuid;
  INSERT INTO private.banking_pay_workbench_settled_certificate_lifecycle_events_v8(
    certificate_uuid,event_sequence,event_kind,reason_code,actor_user_id
  ) VALUES (
    p_certificate_uuid,v_event_sequence,'CERTIFICATE_SEALED',
    'CURRENT_SETTLED_WORKBENCH_AUTHORITY',p_actor_user_id
  );

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'sealed',true,'replayed',false,'has_more',false,
    'certificate_uuid',p_certificate_uuid,
    'certification_id','WORKBENCH_SETTLED_CERTIFICATION_V2:'||v_digest,
    'overall_digest_sha256',v_digest,'lifecycle','SEALED_CURRENT'
  );
END;
$function$;

-- Read the certificate half of Create Draft readiness from the same current
-- Workbench generation that produced the visible preview.  This is a bounded,
-- service-only status read: it takes no Candidate locks, returns no constituent
-- payload and never changes certificate or payment state.
CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_status_v8(
  p_workbench_session_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_status record;
  v_lifecycle text;
  v_session_current boolean;
  v_certificate_ready boolean;
  v_continue_polling boolean;
  v_recovery_required boolean;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout', '6000', true);
  PERFORM pg_catalog.set_config('lock_timeout', '1000', true);
  IF p_workbench_session_id IS NULL OR p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STATUS_SCOPE_INVALID' USING ERRCODE = '22023';
  END IF;

  SELECT session.id AS session_id,
         session.version AS session_version,
         session.progress_counter_version,
         session.authority_fence_generation,
         session.status AS session_status,
         session.progress_state,
         session.discarded_at_utc,
         session.replacement_session_id,
         session.selected_row_count,
         certificate.certificate_uuid,
         certificate.certificate_contract,
         certificate.certification_id,
         certificate.overall_digest_sha256,
         certificate.lifecycle,
         certificate.build_failure_code,
         certificate.build_failure_message
  INTO v_status
  FROM public.banking_pay_workbench_sessions session
  LEFT JOIN LATERAL (
    SELECT candidate.certificate_uuid,
           candidate.certificate_contract,
           candidate.certification_id,
           candidate.overall_digest_sha256,
           candidate.lifecycle,
           candidate.build_failure_code,
           candidate.build_failure_message,
           candidate.created_at_utc
    FROM private.banking_pay_workbench_settled_certificates_v8 candidate
    WHERE candidate.workbench_session_id = session.id
      AND candidate.session_version = session.version
      AND candidate.progress_counter_version = session.progress_counter_version
      AND candidate.authority_fence_generation = session.authority_fence_generation
    ORDER BY CASE candidate.lifecycle
               WHEN 'SEALED_CURRENT' THEN 0
               WHEN 'BUILDING' THEN 1
               WHEN 'BUILD_FAILED' THEN 2
               WHEN 'REVOKED_CORRUPT_OR_SECURITY' THEN 3
               WHEN 'SUPERSEDED_BY_NEW_SESSION' THEN 4
               ELSE 5
             END,
             candidate.created_at_utc DESC,
             candidate.certificate_uuid
    LIMIT 1
  ) certificate ON true
  WHERE session.id = p_workbench_session_id
    AND session.actor_user_id = p_actor_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STATUS_SESSION_NOT_FOUND' USING ERRCODE = 'P0001';
  END IF;

  v_session_current := UPPER(v_status.session_status) = 'OPEN'
    AND UPPER(v_status.progress_state) = 'READY'
    AND v_status.discarded_at_utc IS NULL
    AND v_status.replacement_session_id IS NULL
    AND v_status.session_version >= 1
    AND v_status.progress_counter_version >= 1
    AND v_status.authority_fence_generation >= 1
    AND v_status.selected_row_count BETWEEN 1 AND 50000;
  v_lifecycle := CASE
    WHEN NOT v_session_current THEN 'SESSION_NOT_CURRENT_READY'
    ELSE COALESCE(v_status.lifecycle, 'MISSING')
  END;
  v_certificate_ready := v_session_current
    AND v_lifecycle = 'SEALED_CURRENT'
    AND v_status.certificate_contract = 'WORKBENCH_SETTLED_CERTIFICATION_V2'
    AND COALESCE(v_status.certification_id, '') ~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
    AND COALESCE(v_status.overall_digest_sha256, '') ~ '^[0-9a-f]{64}$';
  v_continue_polling := v_session_current AND v_lifecycle IN ('MISSING', 'BUILDING');
  v_recovery_required := v_session_current AND v_lifecycle IN (
    'BUILD_FAILED', 'REVOKED_CORRUPT_OR_SECURITY', 'SUPERSEDED_BY_NEW_SESSION'
  ) OR v_lifecycle = 'SESSION_NOT_CURRENT_READY';

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'workbench_session_id', v_status.session_id,
    'session_version', v_status.session_version,
    'progress_counter_version', v_status.progress_counter_version,
    'authority_fence_generation', v_status.authority_fence_generation,
    'selected_constituent_count', v_status.selected_row_count,
    'session_current_ready', v_session_current,
    'certificate_lifecycle', v_lifecycle,
    'certificate_uuid', v_status.certificate_uuid,
    'certificate_contract', v_status.certificate_contract,
    'certification_id', CASE WHEN v_certificate_ready THEN v_status.certification_id ELSE NULL END,
    'overall_digest_sha256', CASE WHEN v_certificate_ready THEN v_status.overall_digest_sha256 ELSE NULL END,
    'certificate_ready_for_draft', v_certificate_ready,
    'certificate_build_pending', v_continue_polling,
    'certificate_continue_polling', v_continue_polling,
    'certificate_recovery_required', v_recovery_required,
    'certificate_error_code', CASE WHEN v_lifecycle = 'BUILD_FAILED' THEN v_status.build_failure_code ELSE NULL END,
    'certificate_error_message', CASE WHEN v_lifecycle = 'BUILD_FAILED' THEN LEFT(v_status.build_failure_message, 2000) ELSE NULL END,
    'maximum_selected_constituents', 50000,
    'no_candidate_locks', true,
    'read_only', true
  );
END;
$function$;

-- Select and lease only certificate work that can make progress.  The sweep
-- deliberately resumes an incomplete BUILDING certificate before admitting a
-- new one, ignores an exact current SEALED certificate, and does not retry an
-- exact failed/revoked/superseded generation.  A genuine session authority
-- change has a different version/progress/fence identity and is therefore
-- eligible as new work.  No Candidate or publication row is locked.
CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_due_claim_v8(
  p_limit integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_due record;
  v_start jsonb;
  v_certificate_uuid uuid;
  v_lease_owner text;
  v_claims jsonb := '[]'::jsonb;
  v_processed integer := 0;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout', '15000', true);
  PERFORM pg_catalog.set_config('lock_timeout', '1500', true);
  IF p_limit NOT BETWEEN 1 AND 2 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SWEEP_LIMIT_INVALID' USING ERRCODE = '22023';
  END IF;

  FOR v_due IN
    SELECT session.id AS session_id,
           session.actor_user_id,
           active.certificate_uuid AS active_certificate_uuid,
           active.lifecycle AS active_lifecycle
    FROM public.banking_pay_workbench_sessions session
    LEFT JOIN LATERAL (
      SELECT certificate.certificate_uuid,
             certificate.lifecycle,
             certificate.lease_expires_at_utc
      FROM private.banking_pay_workbench_settled_certificates_v8 certificate
      WHERE certificate.workbench_session_id = session.id
        AND certificate.session_version = session.version
        AND certificate.progress_counter_version = session.progress_counter_version
        AND certificate.authority_fence_generation = session.authority_fence_generation
        AND certificate.lifecycle IN ('BUILDING','SEALED_CURRENT')
      ORDER BY CASE certificate.lifecycle WHEN 'BUILDING' THEN 0 ELSE 1 END,
               certificate.created_at_utc,
               certificate.certificate_uuid
      LIMIT 1
    ) active ON true
    WHERE UPPER(session.status) = 'OPEN'
      AND UPPER(session.progress_state) = 'READY'
      AND session.discarded_at_utc IS NULL
      AND session.replacement_session_id IS NULL
      AND session.selected_row_count BETWEEN 1 AND 50000
      AND (
        (
          active.lifecycle = 'BUILDING'
          AND (active.lease_expires_at_utc IS NULL OR active.lease_expires_at_utc <= clock_timestamp())
        )
        OR (
          active.certificate_uuid IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM private.banking_pay_workbench_settled_certificates_v8 historical
            WHERE historical.workbench_session_id = session.id
              AND historical.session_version = session.version
              AND historical.progress_counter_version = session.progress_counter_version
              AND historical.authority_fence_generation = session.authority_fence_generation
          )
        )
      )
    ORDER BY CASE WHEN active.lifecycle = 'BUILDING' THEN 0 ELSE 1 END,
             session.updated_at_utc,
             session.id
    LIMIT p_limit
    FOR UPDATE OF session SKIP LOCKED
  LOOP
    v_start := public.pay_workbench_settled_certificate_build_start_v8(
      v_due.session_id,
      v_due.actor_user_id,
      'WORKBENCH_SETTLED_CERTIFICATE_V8:' || v_due.session_id::text
    );
    IF v_start->'ok' IS DISTINCT FROM 'true'::jsonb
       OR v_start->>'lifecycle' <> 'BUILDING'
       OR COALESCE(v_start->>'certificate_uuid', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SWEEP_CLAIM_INVALID' USING ERRCODE = '55000';
    END IF;
    v_certificate_uuid := (v_start->>'certificate_uuid')::uuid;
    IF v_due.active_certificate_uuid IS NOT NULL
       AND v_due.active_certificate_uuid IS DISTINCT FROM v_certificate_uuid THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SWEEP_IDENTITY_CHANGED' USING ERRCODE = '55000';
    END IF;
    v_lease_owner := 'WORKBENCH_CERTIFICATE_SWEEP_V8:' || pg_catalog.gen_random_uuid()::text;
    UPDATE private.banking_pay_workbench_settled_certificates_v8 certificate
    SET lease_owner = v_lease_owner,
        lease_expires_at_utc = clock_timestamp() + interval '30 seconds'
    WHERE certificate.certificate_uuid = v_certificate_uuid
      AND certificate.lifecycle = 'BUILDING';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SWEEP_LEASE_CONFLICT' USING ERRCODE = '55P03';
    END IF;

    v_claims := v_claims || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'session_id', v_due.session_id,
      'actor_user_id', v_due.actor_user_id,
      'certificate_uuid', v_certificate_uuid,
      'lease_owner', v_lease_owner,
      'lifecycle', 'BUILDING',
      'replayed', v_start->'replayed'
    ));
    v_processed := v_processed + 1;
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'processed', v_processed,
    'claims', v_claims,
    'maximum_claims', 2,
    'claim_lease_seconds', 30,
    'building_priority', true,
    'exact_current_sealed_skipped', true,
    'exact_historical_terminal_skipped', true
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_lifecycle_v8(
  p_certification_id text,
  p_action text,
  p_reason_code text,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_replacement private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_event_sequence bigint;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout','6000',true);
  PERFORM pg_catalog.set_config('lock_timeout','1000',true);
  IF COALESCE(p_certification_id,'') !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
     OR p_action NOT IN ('SUPERSEDE_BY_NEW_SESSION','REVOKE_CORRUPT_OR_SECURITY')
     OR NULLIF(BTRIM(COALESCE(p_reason_code,'')),'') IS NULL
     OR pg_catalog.octet_length(p_reason_code)>512 OR p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_LIFECYCLE_REQUEST_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT certificate.* INTO v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certification_id=p_certification_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE='P0001'; END IF;
  IF p_action='REVOKE_CORRUPT_OR_SECURITY' THEN
    IF v_header.lifecycle='REVOKED_CORRUPT_OR_SECURITY' THEN
      IF v_header.revocation_code IS DISTINCT FROM BTRIM(p_reason_code) THEN
        RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_LIFECYCLE_REPLAY_CONFLICT' USING ERRCODE='23505';
      END IF;
      RETURN pg_catalog.jsonb_build_object('ok',true,'replayed',true,
        'certification_id',p_certification_id,'lifecycle',v_header.lifecycle,
        'revocation_code',v_header.revocation_code);
    END IF;
    IF v_header.lifecycle NOT IN ('SEALED_CURRENT','SUPERSEDED_BY_NEW_SESSION') THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_LIFECYCLE_TRANSITION_INVALID' USING ERRCODE='55000';
    END IF;
    UPDATE private.banking_pay_workbench_settled_certificates_v8
    SET lifecycle='REVOKED_CORRUPT_OR_SECURITY',revocation_code=BTRIM(p_reason_code)
    WHERE certificate_uuid=v_header.certificate_uuid;
  ELSE
    IF v_header.lifecycle='SUPERSEDED_BY_NEW_SESSION' THEN
      RETURN pg_catalog.jsonb_build_object('ok',true,'replayed',true,
        'certification_id',p_certification_id,'lifecycle',v_header.lifecycle,
        'superseded_by_certificate_uuid',v_header.superseded_by_certificate_uuid);
    END IF;
    IF v_header.lifecycle<>'SEALED_CURRENT' THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_LIFECYCLE_TRANSITION_INVALID' USING ERRCODE='55000';
    END IF;
    SELECT session.* INTO STRICT v_session FROM public.banking_pay_workbench_sessions session
    WHERE session.id=v_header.workbench_session_id FOR SHARE;
    IF v_session.replacement_session_id IS NULL THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REPLACEMENT_SESSION_MISSING' USING ERRCODE='55000';
    END IF;
    SELECT certificate.* INTO v_replacement
    FROM private.banking_pay_workbench_settled_certificates_v8 certificate
    WHERE certificate.workbench_session_id=v_session.replacement_session_id
      AND certificate.lifecycle='SEALED_CURRENT'
    ORDER BY certificate.sealed_at_utc DESC LIMIT 1;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REPLACEMENT_NOT_SEALED' USING ERRCODE='55000';
    END IF;
    UPDATE private.banking_pay_workbench_settled_certificates_v8
    SET lifecycle='SUPERSEDED_BY_NEW_SESSION',superseded_by_certificate_uuid=v_replacement.certificate_uuid
    WHERE certificate_uuid=v_header.certificate_uuid;
  END IF;
  SELECT COALESCE(MAX(event_sequence),-1)+1 INTO v_event_sequence
  FROM private.banking_pay_workbench_settled_certificate_lifecycle_events_v8
  WHERE certificate_uuid=v_header.certificate_uuid;
  INSERT INTO private.banking_pay_workbench_settled_certificate_lifecycle_events_v8(
    certificate_uuid,event_sequence,event_kind,reason_code,actor_user_id,related_certificate_uuid
  ) VALUES (
    v_header.certificate_uuid,v_event_sequence,
    CASE p_action WHEN 'SUPERSEDE_BY_NEW_SESSION' THEN 'CERTIFICATE_SUPERSEDED' ELSE 'CERTIFICATE_REVOKED' END,
    BTRIM(p_reason_code),p_actor_user_id,
    CASE WHEN p_action='SUPERSEDE_BY_NEW_SESSION' THEN v_replacement.certificate_uuid ELSE NULL END
  );
  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'replayed',false,'certification_id',p_certification_id,
    'lifecycle',CASE p_action WHEN 'SUPERSEDE_BY_NEW_SESSION' THEN 'SUPERSEDED_BY_NEW_SESSION' ELSE 'REVOKED_CORRUPT_OR_SECURITY' END,
    'superseded_by_certificate_uuid',CASE WHEN p_action='SUPERSEDE_BY_NEW_SESSION' THEN v_replacement.certificate_uuid ELSE NULL END,
    'revocation_code',CASE WHEN p_action='REVOKE_CORRUPT_OR_SECURITY' THEN BTRIM(p_reason_code) ELSE NULL END
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_settled_certificate_source_row_digest_v8(uuid) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_preview_contract_v8(uuid) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_constituent_seed_v8(uuid,integer) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_component_seed_v8(jsonb,integer,text,text) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_build_start_v8(uuid,uuid,text) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_build_append_page_v8(uuid,integer,integer,text,text) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_seal_v8(uuid,uuid) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_status_v8(uuid,uuid) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_due_claim_v8(integer) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_lifecycle_v8(text,text,text,uuid) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_source_row_digest_v8(uuid) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_preview_contract_v8(uuid) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_constituent_seed_v8(uuid,integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_component_seed_v8(jsonb,integer,text,text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_build_start_v8(uuid,uuid,text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_build_append_page_v8(uuid,integer,integer,text,text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_seal_v8(uuid,uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_status_v8(uuid,uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_due_claim_v8(integer) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_lifecycle_v8(text,text,text,uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_source_row_digest_v8(uuid) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_preview_contract_v8(uuid) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_constituent_seed_v8(uuid,integer) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_component_seed_v8(jsonb,integer,text,text) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_build_start_v8(uuid,uuid,text) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_build_append_page_v8(uuid,integer,integer,text,text) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_seal_v8(uuid,uuid) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_status_v8(uuid,uuid) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_due_claim_v8(integer) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_lifecycle_v8(text,text,text,uuid) TO postgres, service_role;

NOTIFY pgrst, 'reload schema';

commit;
