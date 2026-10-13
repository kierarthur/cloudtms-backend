-- DR-04: stored native preparation buckets and recoverable enrollment evidence.
-- Private additive metadata only; no public-table trigger or new financial status.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_pay_run
  add column preparing_candidate_count bigint not null default 0 check(preparing_candidate_count>=0),
  add column ready_candidate_count bigint not null default 0 check(ready_candidate_count>=0),
  add column review_candidate_count bigint not null default 0 check(review_candidate_count>=0);
-- Preserve any already installed native attempts; this is catalogue-owned
-- metadata reconciliation, not carry-over of historic financial obligations.
update private.bpay_next_pay_run r set
  preparing_candidate_count=(select count(*) from private.bpay_next_run_worker w where w.run_id=r.id and w.status='PREPARING'),
  ready_candidate_count=(select count(*) from private.bpay_next_run_worker w where w.run_id=r.id and w.status='READY'),
  review_candidate_count=(select count(*) from private.bpay_next_run_worker w where w.run_id=r.id and w.status='REVIEW');
alter table private.bpay_next_command
  add column enrollment_error_code text check(enrollment_error_code ~ '^[0-9A-Z]{5}$'),
  add column enrollment_error_at_utc timestamptz,
  add constraint bpay_next_enrollment_error_pair check((enrollment_error_code is null)=(enrollment_error_at_utc is null));
commit;
