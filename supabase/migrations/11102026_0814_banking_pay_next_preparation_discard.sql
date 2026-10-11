-- DR09 separately typed whole-run discard; additive private bindings only.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_preparation_discard_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  run_id uuid not null unique references private.bpay_next_pay_run(id) on delete restrict,
  original_prepare_command_id uuid references private.bpay_next_run_command(command_id) on delete restrict,
  captured_selection_state text not null check(captured_selection_state in ('OPEN','SEALED')),
  reason text check(pg_catalog.octet_length(reason) between 1 and 500),
  original_removed_count bigint not null check(original_removed_count>=0),
  removed_candidate_count bigint not null default 0 check(removed_candidate_count>=0),
  already_removed_candidate_count bigint not null default 0 check(already_removed_candidate_count>=0),
  blocked_candidate_count bigint not null default 0 check(blocked_candidate_count>=0),
  selection_count bigint not null check (selection_count>=0),
  selection_page_count bigint not null check (selection_page_count>=0),
  selection_review_revision bigint not null check (selection_review_revision>=0),
  expected_candidate_count bigint not null check (expected_candidate_count>=0),
  enrolled_candidate_count bigint not null default 0 check (enrolled_candidate_count>=0),
  completed_candidate_count bigint not null default 0 check (completed_candidate_count>=0),
  initiator_kind text not null check (initiator_kind in ('OFFICE')),
  actor_user_id uuid references public.tms_users(id) on delete restrict,
  accepted_at_utc timestamptz not null check (pg_catalog.isfinite(accepted_at_utc)),
  status text not null check (status in ('CANCELLING','DISCARDED','PARTIAL')),
  finished_at_utc timestamptz,
  unique (command_id,run_id),
  check ((initiator_kind='OFFICE')=(actor_user_id is not null)),
  check (completed_candidate_count<=enrolled_candidate_count and enrolled_candidate_count<=expected_candidate_count
    and completed_candidate_count=removed_candidate_count+already_removed_candidate_count+blocked_candidate_count
    and original_removed_count<=expected_candidate_count),
  check(status<>'DISCARDED' or blocked_candidate_count=0),
  check(status<>'PARTIAL' or blocked_candidate_count>0),
  check ((status in ('DISCARDED','PARTIAL'))=(finished_at_utc is not null)),
  check (finished_at_utc is null or (pg_catalog.isfinite(finished_at_utc) and finished_at_utc>=accepted_at_utc
    and completed_candidate_count=expected_candidate_count and enrolled_candidate_count=expected_candidate_count))
);
create table private.bpay_next_preparation_discard_worker (
  command_id uuid not null,
  candidate_id uuid not null,
  run_id uuid not null,
  member_no bigint not null check (member_no>0),
  job_id uuid not null unique references private.bpay_next_job(id) on delete restrict,
  run_worker_id uuid,
  expected_execution_revision bigint check(expected_execution_revision>0),
  already_worker_released boolean not null default false,
  outcome text check(outcome in ('REMOVED','ALREADY_REMOVED','BLOCKED')),
  blocked_code text check(blocked_code in ('PAYMENT_ALREADY_PROTECTED','PREPARATION_STATE_PROTECTED')),
  projection_work_cursor bigint check(projection_work_cursor>0),
  projection_case_cursor bigint check(projection_case_cursor>0),
  pending_work_ids uuid[] check(pending_work_ids is null or(cardinality(pending_work_ids) between 1 and 100
    and array_ndims(pending_work_ids)=1 and array_position(pending_work_ids,null::uuid) is null
    and pg_column_size(pending_work_ids)<=16384)),
  original_job_id uuid references private.bpay_next_job(id) on delete restrict,
  original_job_status text,
  original_job_phase text check (pg_catalog.octet_length(original_job_phase)<=64),
  original_job_cursor text check (pg_catalog.octet_length(original_job_cursor)<=512),
  original_position_work_cursor uuid,
  original_position_component_cursor text check (pg_catalog.octet_length(original_position_component_cursor)<=512),
  original_applied_line_count bigint check (original_applied_line_count>=0),
  original_worker_status text,
  original_review_issue_code text check (pg_catalog.octet_length(original_review_issue_code)<=256),
  original_review_issue_work_id uuid references private.bpay_next_work(id) on delete restrict,
  preparation_revision bigint check (preparation_revision>0),
  selection_revision bigint check (selection_revision>=0),
  draft_state_id uuid,
  expected_work_count bigint not null check (expected_work_count>=0),
  expected_case_hold_count bigint not null check (expected_case_hold_count>=0),
  released_work_count bigint not null default 0 check (released_work_count>=0),
  released_case_hold_count bigint not null default 0 check (released_case_hold_count>=0),
  work_cursor bigint check (work_cursor>0),
  case_hold_cursor uuid,
  checkpoint bigint not null default 0 check (checkpoint>=0),
  status text not null check (status in ('READY','RELEASING','DISCARDED','BLOCKED')),
  stage text not null check (stage in ('NEW','WORK','CASE','PROJECT_WORK','PROJECT_CASE','FINAL','COMPLETE')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  started_at_utc timestamptz,
  finished_at_utc timestamptz,
  primary key (command_id,candidate_id),
  unique (command_id,member_no),
  foreign key (command_id,run_id) references private.bpay_next_preparation_discard_request(command_id,run_id) on delete restrict,
  foreign key (command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  foreign key (run_id,candidate_id) references private.bpay_next_selection_candidate(run_id,candidate_id) on delete restrict,
  foreign key (run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (draft_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
  check (released_work_count<=expected_work_count and released_case_hold_count<=expected_case_hold_count),
  check ((original_job_id is null and original_job_status is null and original_job_phase is null
      and original_job_cursor is null and original_position_work_cursor is null and original_position_component_cursor is null
      and original_applied_line_count is null)
    or (original_job_id is not null and original_job_status is not null and original_job_phase is not null and original_applied_line_count is not null)),
  check ((run_worker_id is null and original_worker_status is null and preparation_revision is null and selection_revision is null
    and draft_state_id is null and expected_execution_revision is null and not already_worker_released
    and expected_work_count=0 and expected_case_hold_count=0)
    or(run_worker_id is not null and original_worker_status is not null and preparation_revision>0
      and selection_revision>=0 and expected_execution_revision>=preparation_revision)),
  check ((status='READY' and stage='NEW' and checkpoint=0 and started_at_utc is null and finished_at_utc is null
      and outcome is null and blocked_code is null and pending_work_ids is null)
    or(status='RELEASING' and stage in ('WORK','CASE','PROJECT_WORK','PROJECT_CASE','FINAL') and started_at_utc is not null
      and finished_at_utc is null and outcome is null and blocked_code is null)
    or(status='DISCARDED' and stage='COMPLETE' and started_at_utc is not null and finished_at_utc is not null
      and outcome in ('REMOVED','ALREADY_REMOVED') and blocked_code is null and pending_work_ids is null
      and released_work_count=expected_work_count and released_case_hold_count=expected_case_hold_count)
    or(status='BLOCKED' and stage='COMPLETE' and started_at_utc is not null and finished_at_utc is not null
      and outcome='BLOCKED' and blocked_code is not null and pending_work_ids is null
      and released_work_count=0 and released_case_hold_count=0)),
  check (pg_catalog.isfinite(created_at_utc) and (started_at_utc is null or pg_catalog.isfinite(started_at_utc))
    and (finished_at_utc is null or pg_catalog.isfinite(finished_at_utc)))
);
create index bpay_next_preparation_discard_pending_idx on private.bpay_next_preparation_discard_worker(command_id,member_no) where stage<>'COMPLETE';
create index bpay_next_preparation_discard_worker_idx on private.bpay_next_preparation_discard_worker(run_worker_id,command_id);
alter table private.bpay_next_preparation_discard_request owner to postgres;
alter table private.bpay_next_preparation_discard_worker owner to postgres;
alter table private.bpay_next_preparation_discard_request enable row level security;
alter table private.bpay_next_preparation_discard_worker enable row level security;
revoke all on table private.bpay_next_preparation_discard_request,private.bpay_next_preparation_discard_worker
  from public,anon,authenticated,service_role;
-- DR09 running WORKER_RELEASE retains only its bounded native ORIGIN work and original manifest seeks.
alter table private.bpay_next_worker_reprepare_request add column pending_work_ids uuid[],
 add column projection_work_cursor bigint check(projection_work_cursor is null or projection_work_cursor>0),
 add column projection_case_cursor bigint check(projection_case_cursor is null or projection_case_cursor>0),
 add constraint bpay_next_worker_release_pending_bound check(pending_work_ids is null or
  (cardinality(pending_work_ids) between 1 and 100 and array_ndims(pending_work_ids)=1 and array_position(pending_work_ids,null) is null
    and pg_column_size(pending_work_ids)<=16384)),
 add constraint bpay_next_worker_release_complete_projection check(status<>'COMPLETE' or pending_work_ids is null);
commit;
