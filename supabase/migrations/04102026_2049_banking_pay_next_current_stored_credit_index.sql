-- L22 factual unbound original-credit page. No legacy data rewrite/bootstrap.
-- Literal enum predicates match the reader exactly (no enum::text index cast).

\set ON_ERROR_STOP on

begin;

create index bpay_next_current_stored_credit_candidate_id
  on public.pay_advances(candidate_id,id)
  where case_type='MANUAL_CREDIT_ADJUSTMENT'
    and taxability='NON_TAXABLE' and routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT'
    and oneoff_bank_details_required is true and status='ACTIVE' and payout_status='PENDING';

-- The legacy component table has no finance_case_id uniqueness. An indexed
-- first-two probe below refuses ambiguity, without multiplying returned rows.
create index bpay_next_stored_credit_component_case_id
  on public.pay_finance_case_components(finance_case_id,id);

commit;
