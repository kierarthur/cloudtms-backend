-- XD01 / owner A3e: indexes for this current masked reader's own sort keys.
-- No producer, trigger, business row or uncapped beneficiary constraint changes.

\set ON_ERROR_STOP on

begin;

create index bpay_next_stored_credit_created_cursor_v1
  on public.pay_advances(candidate_id,created_at,id)
  where case_type='MANUAL_CREDIT_ADJUSTMENT' and taxability='NON_TAXABLE'
    and routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT' and oneoff_bank_details_required is true
    and status='ACTIVE' and payout_status='PENDING';

create index bpay_next_stored_credit_amount_cursor_v1
  on public.pay_advances(candidate_id,original_amount,id)
  where case_type='MANUAL_CREDIT_ADJUSTMENT' and taxability='NON_TAXABLE'
    and routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT' and oneoff_bank_details_required is true
    and status='ACTIVE' and payout_status='PENDING';

-- A full-name B-tree would reject legal long names. The prefix is only an
-- access key; the reader compares/orders the complete unchanged C-name too.
-- Physical visits within a shared-prefix tie group are not claimed bounded.
create index bpay_next_stored_credit_payee_prefix_cursor_v1
  on public.pay_finance_case_oneoff_payout_bank_details
    (candidate_id,(pg_catalog.left(beneficiary_name,128) collate "C"),finance_case_id);

commit;
