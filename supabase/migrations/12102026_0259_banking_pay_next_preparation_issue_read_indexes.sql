-- DR15 read-only native membership/seek support under owner SDP34.
-- Existing private sources only; no data, table, trigger or money owner.

\set ON_ERROR_STOP on

begin;

create index bpay_next_issue_selected_work_label_idx
  on private.bpay_next_run_choice_member(run_id,candidate_id,member_no)
  include(work_id,expected_revision_id,skip_code) where member_kind='WORK' and selected;
create index bpay_next_issue_completed_release_idx
  on private.bpay_next_worker_reprepare_request(run_id,candidate_id,run_worker_id)
  include(original_issue_work_id,original_issue_code,target_preparation_revision,initiator_kind)
  where action='RELEASE' and status='COMPLETE';
create index bpay_next_issue_worker_gross_idx
  on private.bpay_next_run_worker(run_id,gross_ex_vat,candidate_id,id);
create index bpay_next_issue_worker_state_idx
  on private.bpay_next_run_worker(run_id,status,candidate_id,id);

commit;
