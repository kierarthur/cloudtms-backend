-- L22: own-destination weekly basis is distinct from accepted total bank cash.
-- Replace only the exact audited 0380 shape CHECK. The existing 0390 projection
-- trigger is the authoritative exact proof: ordinary = full cash, genuine
-- sealed split = own cash. This range is NOT arbitrary partial-bank authority.
-- No business update, new money column, account/channel rewrite or grant.

\set ON_ERROR_STOP on

begin;

do $audited_predecessor$
declare v_actual text;v_expected text;
begin
  select pg_catalog.pg_get_expr(c.conbin,c.conrelid) into strict v_actual
    from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_worker_week_contribution'::regclass
      and c.conname='bpay_next_week_eligible_bank_basis' and c.contype='c' and c.convalidated;
  v_expected:=$check$
    ((eligibility_state='UNBOUND'::text AND eligible_arranged_amount IS NULL)
    OR (eligibility_state='EXCLUDED'::text AND eligible_arranged_amount IS NOT NULL AND eligible_arranged_amount=0)
    OR (eligibility_state='ELIGIBLE'::text AND eligible_arranged_amount IS NOT NULL
      AND eligible_arranged_amount=CASE WHEN accepted_projection_id IS NOT NULL
        THEN accepted_bank_cash ELSE original_gross_amount END))
  $check$;
  -- pg_get_expr exposes the penny-domain's harmless numeric coercions. Strip
  -- only those casts and presentation parentheses/space, not other semantics.
  v_actual:=pg_catalog.lower(pg_catalog.regexp_replace(
    pg_catalog.regexp_replace(v_actual,'::numeric','','g'),'[[:space:]()]','','g'));
  v_expected:=pg_catalog.lower(pg_catalog.regexp_replace(v_expected,'[[:space:]()]','','g'));
  if v_actual is distinct from v_expected then
    raise exception using errcode='55000',message='BPAY_NEXT_SPLIT_WEEK_PREDECESSOR_CHECK_MISMATCH';
  end if;
end $audited_predecessor$;

alter table private.bpay_next_worker_week_contribution
  drop constraint bpay_next_week_eligible_bank_basis;
alter table private.bpay_next_worker_week_contribution
  add constraint bpay_next_week_eligible_bank_basis check (
    (eligibility_state='UNBOUND' and eligible_arranged_amount is null)
    or (eligibility_state='EXCLUDED' and eligible_arranged_amount is not null and eligible_arranged_amount=0)
    or (eligibility_state='ELIGIBLE' and eligible_arranged_amount is not null
      and ((accepted_projection_id is null and eligible_arranged_amount=original_gross_amount)
        or (accepted_projection_id is not null and eligible_arranged_amount>=0
          and eligible_arranged_amount<=accepted_bank_cash))));

-- Install current 0390 before any application use. Its existing always-active
-- projection guard certifies the exact unique sealed destination fact; absent
-- that fact it continues to require full accepted cash, not merely this range.

commit;
