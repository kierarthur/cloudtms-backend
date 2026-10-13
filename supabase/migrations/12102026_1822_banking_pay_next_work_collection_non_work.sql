-- FC-15: classified PAYE automatic origins; owner12October manual retention.
-- No seed/backfill/tax reclassification, new liability or public trigger.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_work_collection
  drop constraint bpay_next_work_collection_check1,
  drop constraint bpay_next_work_collection_check2,
  add constraint bpay_next_work_collection_check1 check (case_id is null or
    (original_tax_treatment is not null and original_tax_treatment in ('TAXABLE','NON_TAXABLE'))),
  add constraint bpay_next_work_collection_check2 check (not paid_basis_qualified or
    (original_tax_treatment is not null and original_tax_treatment in ('TAXABLE','NON_TAXABLE'))),
  add column collection_mode text not null default 'AUTOMATIC'
    check (collection_mode in ('AUTOMATIC','MANUAL')),
  add column manual_outstanding_source_ex_vat private.bpay_next_penny_amount
    check (manual_outstanding_source_ex_vat>=0),
  add column manual_reason_code text check (manual_reason_code in
    ('AUTOMATIC_COLLECTION_UNAVAILABLE','AMOUNT_UNBOUND'));

-- Owner coverage amendment is manual only. Automatic origins remain PAYE;
-- original channel is still the actual immutable paid run-line channel.
alter table private.bpay_next_work_collection
  drop constraint bpay_next_work_collection_original_source_pay_channel_check,
  add constraint bpay_next_work_collection_original_source_pay_channel_check
    check (original_source_pay_channel in ('PAYE','UMBRELLA') and
      (collection_mode='MANUAL' or original_source_pay_channel='PAYE')),
  add constraint bpay_next_work_collection_manual_observation_check check (
    (collection_mode='AUTOMATIC' and manual_reason_code is null and manual_outstanding_source_ex_vat is null)
    or (collection_mode='MANUAL' and manual_reason_code is not null and
      ((manual_reason_code='AMOUNT_UNBOUND' and manual_outstanding_source_ex_vat is null)
       or (manual_reason_code='AUTOMATIC_COLLECTION_UNAVAILABLE' and manual_outstanding_source_ex_vat is not null))));

-- A single deterministic paid anchor, never a re-summed payment history.
create index bpay_next_effect_collection_paid_anchor_idx
  on private.bpay_next_financial_effect(work_id,component_key,occurred_at_utc,id)
  where effect_kind='PAYROLL_SETTLED' and source_disposition_ex_vat>0;

comment on column private.bpay_next_work_collection.manual_outstanding_source_ex_vat is
  'Owner12October: known native unreserved difference; NULL means retained unbound amount, never zero or a second debt.';

commit;
