-- WB08 additive closure after the immutable1252 schema lock. No business data.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_workbench_group add column certified_zero boolean not null default false;
alter table private.bpay_next_workbench_item add column written_off_source_ex_vat private.bpay_next_penny_amount,
  add constraint bpay_next_workbench_writeoff_known_shape check(
    (amount_known and written_off_source_ex_vat is not null and written_off_source_ex_vat>=0
      and (portion_kind<>'ZERO' or written_off_source_ex_vat=0))
    or (not amount_known and written_off_source_ex_vat is null));

commit;
