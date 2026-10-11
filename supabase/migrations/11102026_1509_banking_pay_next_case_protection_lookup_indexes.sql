-- One-time CloudTMS schema/data migration: banking_pay_next_case_protection_lookup_indexes
-- FC-16 native accepted-component seek; additive index only, no business rows.

\set ON_ERROR_STOP on

begin;

create index bpay_next_run_choice_case_protection_idx
  on private.bpay_next_run_choice_member(case_component_id,run_id,candidate_id)
  where member_kind='CASE_COMPONENT' and selected;

-- Original allocated-hold receipt is retained; distinguish unknown accepted
-- input protection without rewriting its non-null allocated amount field.
alter table private.bpay_next_write_off_request
  drop constraint bpay_next_write_off_request_issue_code_check,
  add constraint bpay_next_write_off_request_issue_code_check check(issue_code in (
    'BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY','BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE',
    'BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED','BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND',
    'BPAY_NEXT_WRITE_OFF_NO_BALANCE','BPAY_NEXT_WRITE_OFF_INPUT_PENDING'));

commit;
