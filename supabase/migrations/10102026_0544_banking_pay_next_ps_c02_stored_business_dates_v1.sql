-- PS-C02: a persisted business date of the immutable occurrence fact.
-- Approved PSP15/18: outcome occurrence owns paid/return date, not ingestion
-- or later economic-posting/request-completion time. No producer/API rewrite.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_transfer_outcome
  add column business_date date generated always as
    ((occurred_at_utc at time zone 'Europe/London')::date) stored not null,
  add constraint bpay_next_outcome_business_date_finite check(pg_catalog.isfinite(business_date));
alter table private.bpay_next_internal_receipt
  add column business_date date generated always as
    ((occurred_at_utc at time zone 'Europe/London')::date) stored not null,
  add constraint bpay_next_internal_business_date_finite check(pg_catalog.isfinite(business_date));
comment on column private.bpay_next_transfer_outcome.business_date is
  'UK business date of the original reported occurrence; received_at_utc is ingestion. Paid only for an actual SETTLED outcome, return date only for RETURNED.';
comment on column private.bpay_next_internal_receipt.business_date is
  'UK date of the genuine zero-cash accounting receipt occurrence; never a bank payment date.';
-- Existing0200 outcome immutable trigger and0490 internal receipt guard
-- freeze these generated fields along with their original occurrence clocks.
-- Existing producer defaults retain server-owned received_at_utc.
commit;
