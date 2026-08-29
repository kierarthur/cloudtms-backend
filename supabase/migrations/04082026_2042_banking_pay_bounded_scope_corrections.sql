-- Banking Pay bounded-scope post-audit corrections (TEST only).
-- Policy X is unchanged: these objects carry pre-draft orchestration authority only.
BEGIN;

ALTER TABLE private.banking_pay_workbench_economic_build_facts
  DROP CONSTRAINT bpay_wb_economic_build_facts_family_chk,
  ADD CONSTRAINT bpay_wb_economic_build_facts_family_chk CHECK (fact_family IN (
    'DEPENDENCY_EDGE','FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT',
    'ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT','PAY_STATE_FALLBACK',
    'FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY','PROTECTION_EVIDENCE',
    'PAYEE_BASELINE_INPUT','ALLOCATION_INPUT','CANONICAL_INPUT','EXPECTED_FINANCE_EFFECT'
  ));

ALTER TABLE private.banking_pay_workbench_economic_build_facts
  DROP CONSTRAINT bpay_wb_economic_build_facts_unit_chk,
  ADD CONSTRAINT bpay_wb_economic_build_facts_unit_chk CHECK (
    (fact_family = 'DEPENDENCY_EDGE' AND (
      dependency_unit_key IS NULL
      OR (dependency_unit_key <> 'GLOBAL' AND btrim(dependency_unit_key) <> '')
    ))
    OR (fact_family IN (
      'RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY',
      'PROTECTION_EVIDENCE','ALLOCATION_INPUT','EXPECTED_FINANCE_EFFECT'
    ) AND dependency_unit_key = 'GLOBAL')
    OR (fact_family IN (
      'FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
      'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT'
    ) AND NULLIF(btrim(dependency_unit_key),'') IS NOT NULL AND dependency_unit_key <> 'GLOBAL')
  );

ALTER TABLE private.banking_pay_workbench_economic_build_facts
  DROP CONSTRAINT bpay_wb_economic_build_facts_authority_chk,
  ADD CONSTRAINT bpay_wb_economic_build_facts_authority_chk CHECK (
    (fact_family <> 'FROZEN_SETTLED_COMPONENT' OR (economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL)))
    AND (fact_family <> 'ENTITLEMENT_COMPONENT' OR (economic_key_type IS NOT NULL AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL) AND (baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)))
    AND (fact_family <> 'RESERVATION_COMPONENT' OR (reservation_id IS NOT NULL AND economic_key_type IS NOT NULL AND reserved_source_amount IS NOT NULL))
    AND (fact_family <> 'FINANCE_CASE_IDENTITY' OR finance_case_id IS NOT NULL)
    AND (fact_family <> 'FINANCE_COMPONENT_IDENTITY' OR (finance_case_id IS NOT NULL AND finance_component_id IS NOT NULL))
    AND (fact_family <> 'PROTECTION_EVIDENCE' OR (finance_case_id IS NOT NULL OR finance_component_id IS NOT NULL OR reservation_id IS NOT NULL OR source_id IS NOT NULL))
    AND (fact_family <> 'PAYEE_BASELINE_INPUT' OR (economic_key_type IS NOT NULL AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)))
    AND (fact_family <> 'ALLOCATION_INPUT' OR (economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL OR reserved_source_amount IS NOT NULL)))
    AND (fact_family <> 'CANONICAL_INPUT' OR (economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL OR truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)))
    AND (fact_family <> 'EXPECTED_FINANCE_EFFECT' OR (
      source_id IS NOT NULL
      AND source_relation IN ('pay_advances','pay_finance_case_components','pay_finance_case_events')
      AND source_payload_json ? 'operation'
      AND source_payload_json ? 'expected_before_digest'
      AND source_payload_json ? 'expected_after_digest'
    ))
  );

COMMIT;
