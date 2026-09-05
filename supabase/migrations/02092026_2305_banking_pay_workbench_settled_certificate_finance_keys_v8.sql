-- Add the two canonical finance economic-key kinds consumed by the settled
-- certificate producer.  This is deliberately additive: the immutable 2300
-- migration remains byte-identical to its release lock, and no amount, channel,
-- tax, VAT, eligibility, allocation or materialisation policy is changed here.

ALTER TABLE private.banking_pay_workbench_settled_certificate_entries_v8
  DROP CONSTRAINT banking_pay_workbench_settled_certifica_economic_key_type_check,
  ADD CONSTRAINT banking_pay_workbench_settled_certifica_economic_key_type_check
    CHECK (economic_key_type IN (
      'TS_DAY',
      'TS_TOTAL',
      'ADDITIONAL_CODE',
      'ADJUSTMENT_CODE',
      'EXPENSE_CODE',
      'MANUAL_CARRY_FORWARD',
      'CASE_TOTAL',
      'FINANCE_COMPONENT'
    ));

ALTER TABLE private.banking_pay_workbench_settled_certificate_component_evidence_v8
  DROP CONSTRAINT banking_pay_workbench_settled_certific_economic_key_type_check1,
  ADD CONSTRAINT banking_pay_workbench_settled_certific_economic_key_type_check1
    CHECK (economic_key_type IN (
      'TS_DAY',
      'TS_TOTAL',
      'ADDITIONAL_CODE',
      'ADJUSTMENT_CODE',
      'EXPENSE_CODE',
      'MANUAL_CARRY_FORWARD',
      'CASE_TOTAL',
      'FINANCE_COMPONENT'
    ));
