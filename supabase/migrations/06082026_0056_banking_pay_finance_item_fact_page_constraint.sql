-- Banking Pay bounded-scope Version 1.2.16 fact-page authority correction.
--
-- FINANCE_ITEM_AUTHORITY was introduced as the first build-global physical
-- fact stream. Its fact-row constraints were updated in Version 1.2.8, but
-- the same family was omitted from the fact-page unit constraint. Replace the
-- existing named constraint in place; this adds no object and changes no
-- public contract or economic rule.

ALTER TABLE private.banking_pay_workbench_economic_build_fact_pages
  DROP CONSTRAINT IF EXISTS bpay_wb_economic_build_fact_pages_unit_chk;

ALTER TABLE private.banking_pay_workbench_economic_build_fact_pages
  ADD CONSTRAINT bpay_wb_economic_build_fact_pages_unit_chk CHECK (
    (fact_family IN (
      'FINANCE_ITEM_AUTHORITY','RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY',
      'FINANCE_COMPONENT_IDENTITY','PROTECTION_EVIDENCE','ALLOCATION_INPUT'
    ) AND dependency_unit_key = 'GLOBAL')
    OR (fact_family IN (
      'FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
      'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT'
    ) AND btrim(dependency_unit_key) <> '' AND dependency_unit_key <> 'GLOBAL')
  );
