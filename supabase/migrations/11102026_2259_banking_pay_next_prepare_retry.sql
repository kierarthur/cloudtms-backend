-- PF08 / BD124: additive operational recovery on original PREPARE jobs.
-- No business rows are created or rewritten. Existing lease and money keys stay.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_job
 add column retry_count integer not null default 0 check(retry_count between 0 and 5),
 add column next_retry_at_utc timestamptz,
 add column needs_attention boolean not null default false,
 add column last_retry_owner_epoch bigint check(last_retry_owner_epoch>0),
 add column retry_run_id uuid,
 add column retry_prior_issue_code text check(octet_length(retry_prior_issue_code)<=256),
 add column retry_prior_issue_work_id uuid,
 add column retry_execution_revision bigint check(retry_execution_revision>1),
 add column retry_request_id uuid,
 add constraint bpay_next_job_retry_attention_ck check(not needs_attention or
   (job_kind='PREPARE' and retry_count=5 and status='READY' and lease_nonce is null and lease_until_utc is null)),
 add constraint bpay_next_job_retry_execution_ck check((retry_execution_revision is null)=(retry_request_id is null));
alter table private.bpay_next_workbench_command_receipt
 alter column work_id drop not null,
 add column retry_run_worker_id uuid,
 add column retry_input_job_id uuid,
 add column retry_captured_revision bigint,
 add column retry_expected_revision bigint,
 add column retry_target_revision bigint,
 add constraint bpay_next_workbench_retry_execution_ck check(
   (retry_run_worker_id is null and retry_input_job_id is null and retry_captured_revision is null
     and retry_expected_revision is null and retry_target_revision is null)
   or (num_nonnulls(retry_run_worker_id,retry_input_job_id,retry_captured_revision,retry_expected_revision,retry_target_revision)=5 and retry_captured_revision>0
     and retry_expected_revision>0 and retry_target_revision::numeric=retry_expected_revision::numeric+1
     and retry_captured_revision<=retry_expected_revision)),
 add constraint bpay_next_workbench_retry_work_ck check(work_id is not null or result->>'retry_kind'='PREPARE');
-- IDs in immutable receipts deliberately do not prevent native terminal cleanup,
-- matching the existing receipt.job_id contract. Qualification is a PK read.
alter table private.bpay_next_run_worker drop constraint bpay_next_worker_issue_scope_ck;
alter table private.bpay_next_run_worker add constraint bpay_next_worker_issue_scope_ck check(
 (review_issue_code is null and review_issue_work_id is null)
 or (review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
  'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT','PF08_NEEDS_ATTENTION')
  and review_issue_work_id is null)
 or (review_issue_code is not null and review_issue_code not in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED',
  'CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT','PF08_NEEDS_ATTENTION')
  and review_issue_work_id is not null));
create index bpay_next_job_retry_due_idx on private.bpay_next_job
 (greatest(queue_wake_after_utc,next_retry_at_utc),command_sequence,id)
 where not needs_attention and status in ('READY','BLOCKED');
create index bpay_next_job_retry_expired_idx on private.bpay_next_job
 (greatest(queue_wake_after_utc,lease_until_utc,next_retry_at_utc),command_sequence,id)
 where not needs_attention and status='LEASED';
create index bpay_next_prepare_attention_candidate_idx on private.bpay_next_job(retry_run_id,candidate_id,id)
 where job_kind='PREPARE' and needs_attention;

commit;
