-- DR06: private scheduling metadata only; no financial revision or public trigger.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_pay_run add column preparation_wake_at_utc timestamptz
  check(preparation_wake_at_utc is null or pg_catalog.isfinite(preparation_wake_at_utc));
commit;
