-- PS-C01 stored Umbrella bridge. Exact predecessor assertion, no backfill.
-- Only private NEXT schema; PAYE and CASE_PAYOUT cash clauses preserved.
\set ON_ERROR_STOP on
begin;
do $predecessor$
begin
  if (select pg_catalog.pg_get_constraintdef(c.oid) from pg_catalog.pg_constraint c
      where c.conrelid='private.bpay_next_net_projection'::pg_catalog.regclass
        and c.conname='bpay_next_projection_case_cash') is distinct from
    $expected$CHECK (((input_kind = 'UMBRELLA'::text) OR ((input_kind = ANY (ARRAY['PAYE_MANUAL'::text, 'PAYE_IMPORT'::text])) AND (cash_amount = ((entered_paye_net + (accepted_net_additions)::numeric) - accepted_recoveries))) OR ((input_kind = 'CASE_PAYOUT'::text) AND (gross_ex_vat = (0)::numeric) AND (gross_vat = (0)::numeric) AND (gross_inc_vat = (0)::numeric) AND ((accepted_gross_additions)::numeric = (0)::numeric) AND ((accepted_gross_deductions)::numeric = (0)::numeric) AND (accepted_recoveries = (0)::numeric) AND (cash_amount = (accepted_net_additions)::numeric))))$expected$ then
    raise exception using errcode='23514',message='BPAY_NEXT_PS_C01_PROJECTION_PREDECESSOR_MISMATCH';
  end if;
end
$predecessor$;

alter table private.bpay_next_net_projection
  add column payment_case_target_inc_vat private.bpay_next_penny_amount not null default 0
    check(payment_case_target_inc_vat>=0),
  add column deduction_case_target_inc_vat private.bpay_next_penny_amount not null default 0
    check(deduction_case_target_inc_vat>=0),
  drop constraint bpay_next_projection_case_cash,
  add constraint bpay_next_projection_case_cash check (
    (input_kind='UMBRELLA' and entered_paye_net is null
      and cash_amount=gross_inc_vat+payment_case_target_inc_vat-deduction_case_target_inc_vat)
    or (input_kind in ('PAYE_MANUAL','PAYE_IMPORT') and cash_amount=entered_paye_net+accepted_net_additions-accepted_recoveries)
    or (input_kind='CASE_PAYOUT' and gross_ex_vat=0 and gross_vat=0 and gross_inc_vat=0
      and accepted_gross_additions=0 and accepted_gross_deductions=0 and accepted_recoveries=0 and cash_amount=accepted_net_additions));

-- Retain the separate F06 ordinary-WORK shape constraint. CM-13a owns
-- its later supported Case successor and population through0410.
-- Whole-row0390 projection immutability already includes the new columns.
alter table private.bpay_next_net_projection
  add constraint bpay_next_projection_umbrella_bridge_key unique
    (id,run_worker_id,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,
     payment_case_target_inc_vat,deduction_case_target_inc_vat,cash_amount);

alter table private.bpay_next_transfer
  add column umbrella_bridge_kind text,
  add column umbrella_gross_ex_vat private.bpay_next_penny_amount,
  add column umbrella_gross_vat private.bpay_next_penny_amount,
  add column umbrella_gross_inc_vat private.bpay_next_penny_amount,
  add column umbrella_payment_case_target_inc_vat private.bpay_next_penny_amount,
  add column umbrella_deduction_case_target_inc_vat private.bpay_next_penny_amount,
  add constraint bpay_next_transfer_umbrella_bridge_shape check (
    (umbrella_bridge_kind is null and umbrella_gross_ex_vat is null
      and umbrella_gross_vat is null and umbrella_gross_inc_vat is null
      and umbrella_payment_case_target_inc_vat is null
      and umbrella_deduction_case_target_inc_vat is null)
    or (umbrella_bridge_kind is not null and umbrella_bridge_kind='UMBRELLA'
      and projection_id is not null and original_transfer_id is null
      and beneficiary_kind='UMBRELLA'
      and umbrella_gross_ex_vat is not null and umbrella_gross_ex_vat>=0
      and umbrella_gross_vat is not null and umbrella_gross_vat>=0
      and umbrella_gross_inc_vat is not null and umbrella_gross_inc_vat>=0
      and umbrella_payment_case_target_inc_vat is not null and umbrella_payment_case_target_inc_vat>=0
      and umbrella_deduction_case_target_inc_vat is not null and umbrella_deduction_case_target_inc_vat>=0
      and umbrella_gross_inc_vat=umbrella_gross_ex_vat+umbrella_gross_vat
      and cash_amount=umbrella_gross_inc_vat+umbrella_payment_case_target_inc_vat-umbrella_deduction_case_target_inc_vat)),
  add constraint bpay_next_transfer_umbrella_bridge_required check (
    beneficiary_kind<>'UMBRELLA' or original_transfer_id is not null
    or status in ('BUILDING','CANCELLED') or umbrella_bridge_kind is not null),
  add constraint bpay_next_transfer_umbrella_projection_bridge_fk foreign key
    (projection_id,run_worker_id,umbrella_bridge_kind,umbrella_gross_ex_vat,
     umbrella_gross_vat,umbrella_gross_inc_vat,umbrella_payment_case_target_inc_vat,
     umbrella_deduction_case_target_inc_vat,cash_amount)
    references private.bpay_next_net_projection
      (id,run_worker_id,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,
       payment_case_target_inc_vat,deduction_case_target_inc_vat,cash_amount)
    on delete restrict;
-- Existing0145 pins projection identity; exact FK to0390 immutable projection
-- pins the complete bridge without an additional trigger or owner rewrite.
create index bpay_next_transfer_umbrella_projection_bridge_idx
  on private.bpay_next_transfer(projection_id,run_worker_id,umbrella_bridge_kind,
    umbrella_gross_ex_vat,umbrella_gross_vat,umbrella_gross_inc_vat,
    umbrella_payment_case_target_inc_vat,umbrella_deduction_case_target_inc_vat,cash_amount)
  where umbrella_bridge_kind is not null;
commit;
