-- DR13 immutable original-basis observations only. No data/backfill or public trigger.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_run_case_instruction
 add column captured_component_reserved_source_ex_vat private.bpay_next_penny_amount,
 add column captured_case_reserved_source_ex_vat private.bpay_next_penny_amount,
 add constraint bpay_next_case_review_reserved_pair check (
  (captured_component_reserved_source_ex_vat is null)=(captured_case_reserved_source_ex_vat is null)
  and (captured_component_reserved_source_ex_vat is null or
   (captured_component_reserved_source_ex_vat>=0 and captured_case_reserved_source_ex_vat>=0
    and captured_component_outstanding_source_ex_vat is not null and captured_case_outstanding_source_ex_vat is not null)));
alter table private.bpay_next_case_selection_item
 add column review_observed_component_principal_source_ex_vat private.bpay_next_penny_amount,
 add column review_observed_component_reserved_source_ex_vat private.bpay_next_penny_amount,
 add column review_observed_unreserved_source_ex_vat private.bpay_next_penny_amount,
 add constraint bpay_next_case_review_excluded_reservations check (
  (review_observed_component_principal_source_ex_vat is null and review_observed_component_reserved_source_ex_vat is null and review_observed_unreserved_source_ex_vat is null)
  or (review_observed_component_principal_source_ex_vat is not null and review_observed_component_reserved_source_ex_vat is not null and review_observed_unreserved_source_ex_vat is not null
   and review_observed_component_principal_source_ex_vat>=0 and review_observed_component_reserved_source_ex_vat>=0 and review_observed_unreserved_source_ex_vat>=0
   and review_observed_at_utc is not null));
commit;
