-- Unconfirmed preparation expiry only. No legacy deadline adoption, payment
-- cancellation, principal correction or automatic NOW-based backfill.
\set ON_ERROR_STOP on
begin;

alter table private.bpay_next_pay_run add column preparation_expires_at_utc timestamptz,
  add constraint bpay_next_preparation_deadline_ck check (preparation_expires_at_utc is null
    or (pg_catalog.isfinite(created_at_utc) and pg_catalog.isfinite(preparation_expires_at_utc)
      and preparation_expires_at_utc=created_at_utc+interval '72 hours'));
-- Match the entire audited predicate, not a guessed generated CHECK name.
do $run_status$
declare v_name name;
begin
  select c.conname into strict v_name from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_pay_run'::regclass and c.contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=
        'status=ANYARRAY[''PREPARING''::text,''REVIEW''::text,''DRAFT''::text,''CANCELLED''::text,''EXECUTING''::text,''COMPLETE''::text]';
  execute pg_catalog.format('alter table private.bpay_next_pay_run drop constraint %I',v_name);
end
$run_status$;
alter table private.bpay_next_pay_run add constraint bpay_next_run_status_ck
  check (status in ('PREPARING','REVIEW','DRAFT','CANCELLING','CANCELLED','EXECUTING','COMPLETE'));
create index bpay_next_preparation_due_idx on private.bpay_next_pay_run(preparation_expires_at_utc,id)
  where confirmed_at_utc is null and status in ('PREPARING','REVIEW') and preparation_expires_at_utc is not null;

create table private.bpay_next_preparation_expiry_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  run_id uuid not null unique references private.bpay_next_pay_run(id) on delete restrict,
  original_prepare_command_id uuid references private.bpay_next_run_command(command_id) on delete restrict,
  captured_deadline timestamptz not null check (pg_catalog.isfinite(captured_deadline)),
  selection_count bigint not null check (selection_count>=0),
  selection_page_count bigint not null check (selection_page_count>=0),
  selection_review_revision bigint not null check (selection_review_revision>=0),
  expected_candidate_count bigint not null check (expected_candidate_count>=0),
  enrolled_candidate_count bigint not null default 0 check (enrolled_candidate_count>=0),
  completed_candidate_count bigint not null default 0 check (completed_candidate_count>=0),
  initiator_kind text not null check (initiator_kind in ('OFFICE','SYSTEM')),
  actor_user_id uuid references public.tms_users(id) on delete restrict,
  accepted_at_utc timestamptz not null check (pg_catalog.isfinite(accepted_at_utc)),
  status text not null check (status in ('CANCELLING','EXPIRED')),
  finished_at_utc timestamptz,
  unique (command_id,run_id),
  check ((initiator_kind='OFFICE')=(actor_user_id is not null)),
  check (accepted_at_utc>=captured_deadline),
  check (completed_candidate_count<=enrolled_candidate_count and enrolled_candidate_count<=expected_candidate_count),
  check ((status='EXPIRED')=(finished_at_utc is not null)),
  check (finished_at_utc is null or (pg_catalog.isfinite(finished_at_utc) and finished_at_utc>=accepted_at_utc
    and completed_candidate_count=expected_candidate_count and enrolled_candidate_count=expected_candidate_count))
);
create table private.bpay_next_preparation_expiry_worker (
  command_id uuid not null,
  candidate_id uuid not null,
  run_id uuid not null,
  member_no bigint not null check (member_no>0),
  job_id uuid not null unique references private.bpay_next_job(id) on delete restrict,
  run_worker_id uuid,
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
  status text not null check (status in ('READY','RELEASING','EXPIRED')),
  stage text not null check (stage in ('NEW','WORK','CASE','FINAL','COMPLETE')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  started_at_utc timestamptz,
  finished_at_utc timestamptz,
  primary key (command_id,candidate_id),
  unique (command_id,member_no),
  foreign key (command_id,run_id) references private.bpay_next_preparation_expiry_request(command_id,run_id) on delete restrict,
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
      and draft_state_id is null and expected_work_count=0 and expected_case_hold_count=0 and original_review_issue_code is null
      and original_review_issue_work_id is null)
    or (run_worker_id is not null and original_worker_status in ('PREPARING','REVIEW','READY')
      and preparation_revision is not null and selection_revision is not null)),
  check ((status='READY' and stage='NEW' and checkpoint=0 and started_at_utc is null and finished_at_utc is null)
    or (status='RELEASING' and stage in ('WORK','CASE','FINAL') and started_at_utc is not null and finished_at_utc is null)
    or (status='EXPIRED' and stage='COMPLETE' and started_at_utc is not null and finished_at_utc is not null
      and released_work_count=expected_work_count and released_case_hold_count=expected_case_hold_count)),
  check (pg_catalog.isfinite(created_at_utc) and (started_at_utc is null or pg_catalog.isfinite(started_at_utc))
    and (finished_at_utc is null or pg_catalog.isfinite(finished_at_utc)))
);
create index bpay_next_preparation_expiry_worker_idx on private.bpay_next_preparation_expiry_worker(run_worker_id,command_id);
alter table private.bpay_next_preparation_expiry_request owner to postgres;
alter table private.bpay_next_preparation_expiry_worker owner to postgres;
alter table private.bpay_next_preparation_expiry_request enable row level security;
alter table private.bpay_next_preparation_expiry_worker enable row level security;
revoke all on table private.bpay_next_preparation_expiry_request,private.bpay_next_preparation_expiry_worker
  from public,anon,authenticated,service_role;
commit;
