-- CASE_PAYOUT loans/credits are not earnings. Return/reissue retains their
-- exact EXCLUDED0 contribution and real original/cash references. The existing
-- accepted-projection trigger proves the zero-gross/NULL-net payout identity;
-- returned payroll still requires bound B1 or explicit UNBOUND. No data change.
\set ON_ERROR_STOP on
begin;

do $non_earnings_return$
declare
  v_constraint name;
  v_expected text:=pg_catalog.regexp_replace($audited_check$
    ((payment_state <> ALL (ARRAY['RETURNED_OWED'::text, 'REISSUED_PAID'::text]))
     OR ((returned_cash_id IS NOT NULL) AND (original_transfer_id IS NOT NULL)
       AND ((return_floor_binding_ref IS NOT NULL) OR (eligibility_state = 'UNBOUND'::text))))
  $audited_check$,'[[:space:]()]','','g');
begin
  select c.conname into strict v_constraint from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_worker_week_contribution'::regclass and c.contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=v_expected;
  execute pg_catalog.format('alter table private.bpay_next_worker_week_contribution drop constraint %I',v_constraint);
end
$non_earnings_return$;
alter table private.bpay_next_worker_week_contribution
  add constraint bpay_next_week_return_earnings_scope_ck check (
    payment_state not in ('RETURNED_OWED','REISSUED_PAID') or
    (returned_cash_id is not null and original_transfer_id is not null and
      (return_floor_binding_ref is not null or eligibility_state='UNBOUND' or
        (eligibility_state='EXCLUDED' and eligible_arranged_amount=0
          and basis_kind='GROSS_FALLBACK' and original_gross_amount=0
          and original_payroll_net is null and accepted_projection_id is not null
          and accepted_bank_cash is not null))));

commit;
