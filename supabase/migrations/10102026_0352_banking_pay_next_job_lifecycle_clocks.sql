-- One-time CloudTMS schema/data migration: banking_pay_next_job_lifecycle_clocks
-- PF-06 D01: exact missing lifecycle clocks on the existing private job row.
-- Historical times remain unknown; no business rows or owners are rewritten.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_job
  add column created_at_utc timestamptz,
  add column started_at_utc timestamptz,
  add column finished_at_utc timestamptz;

comment on column private.bpay_next_job.created_at_utc is
  'PF06 actual job insertion time; NULL for jobs predating lifecycle capture.';
comment on column private.bpay_next_job.started_at_utc is
  'PF06 first actual LEASED transition for jobs with captured creation; historical first starts remain unknown.';
comment on column private.bpay_next_job.finished_at_utc is
  'PF06 first actual DONE or FAILED transition; BLOCKED is resumable. Historical terminal times remain unknown.';

commit;
