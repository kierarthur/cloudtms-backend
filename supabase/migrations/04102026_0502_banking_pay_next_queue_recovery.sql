-- Durable financial jobs remain the authority; queue messages are wake-ups.
-- One small indexed recovery time, independent of financial availability and
-- leases. No revaluation, history copy, agency population sweep or activation.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_job add column queue_wake_after_utc timestamptz
  not null default pg_catalog.transaction_timestamp();
alter table private.bpay_next_job add constraint bpay_next_job_queue_wake_finite
  check (pg_catalog.isfinite(queue_wake_after_utc));
create index bpay_next_job_queue_ready_idx
  on private.bpay_next_job(queue_wake_after_utc,command_sequence,id)
  where status in ('READY','BLOCKED');
create index bpay_next_job_queue_expired_idx
  on private.bpay_next_job((greatest(queue_wake_after_utc,lease_until_utc)),command_sequence,id)
  where status='LEASED';
commit;
