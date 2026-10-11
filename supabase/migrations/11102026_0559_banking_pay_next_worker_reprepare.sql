-- DR05 additive native worker action. Frozen inputs/reservations are retained.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_run_worker
  add column captured_preparation_revision bigint not null default 1
    check (captured_preparation_revision>0 and captured_preparation_revision<=preparation_revision),
  add column input_prepare_job_id uuid references private.bpay_next_job(id) on delete restrict,
  add column active_prepare_job_id uuid references private.bpay_next_job(id) on delete restrict;

alter table private.bpay_next_job
  add column retained_prepare_phase text check (retained_prepare_phase in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE')),
  add column retained_prepare_cursor text,
  add column retained_prepare_work_cursor uuid,
  add column retained_prepare_component_cursor text,
  add constraint bpay_next_retained_prepare_cursor_pair check
    ((retained_prepare_work_cursor is null)=(retained_prepare_component_cursor is null));
alter table private.bpay_next_case_allocation_state
  add column retained_prepare_stage text check (retained_prepare_stage in ('WEEK_CAPTURE','INSTRUCTION_CAPTURE','ALLOCATE')),
  add column retained_weekly_binding_state text check (retained_weekly_binding_state in ('UNBOUND','BOUND','NOT_AFFECTED')),
  add constraint bpay_next_retained_case_stage_pair check
    ((retained_prepare_stage is null)=(retained_weekly_binding_state is null));

create table private.bpay_next_worker_reprepare_request (
  command_id uuid not null references private.bpay_next_command(id) on delete restrict,
  run_worker_id uuid not null,
  run_id uuid not null,
  candidate_id uuid not null,
  action text not null check (action in ('REPREPARE','RELEASE')),
  expected_preparation_revision bigint not null check (expected_preparation_revision>0),
  target_preparation_revision bigint not null check (target_preparation_revision=expected_preparation_revision+1),
  captured_preparation_revision bigint not null check (captured_preparation_revision>0
    and captured_preparation_revision<=expected_preparation_revision),
  original_prepare_command_id uuid not null references private.bpay_next_command(id) on delete restrict,
  input_prepare_job_id uuid not null references private.bpay_next_job(id) on delete restrict,
  previous_job_id uuid not null references private.bpay_next_job(id) on delete restrict,
  job_id uuid references private.bpay_next_job(id) on delete restrict,
  module_epoch bigint not null check (module_epoch>0),
  initiator_kind text not null check (initiator_kind in ('OFFICE','SYSTEM')),
  actor_user_id uuid references public.tms_users(id) on delete restrict,
  original_worker_status text not null check (original_worker_status in ('PREPARING','REVIEW','READY')),
  original_issue_code text check (pg_catalog.octet_length(original_issue_code) between 1 and 256),
  original_issue_work_id uuid references private.bpay_next_work(id) on delete restrict,
  accepted_review_revision bigint not null check (accepted_review_revision>0),
  resume_phase text check (resume_phase in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE')),
  resume_cursor text,
  resume_work_cursor uuid,
  resume_component_cursor text,
  selection_revision bigint not null check (selection_revision>=0),
  draft_state_id uuid,
  expected_work_count bigint not null check (expected_work_count>=0),
  expected_case_hold_count bigint not null check (expected_case_hold_count>=0),
  released_work_count bigint not null default 0 check (released_work_count>=0),
  released_case_hold_count bigint not null default 0 check (released_case_hold_count>=0),
  work_cursor bigint check (work_cursor>0),
  case_hold_cursor uuid,
  checkpoint bigint not null default 0 check (checkpoint>=0),
  status text not null check (status in ('ACCEPTED','RUNNING','COMPLETE')),
  stage text not null check (stage in ('NEW','WORK','CASE','FINAL','PROJECT','COMPLETE')),
  accepted_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check (pg_catalog.isfinite(accepted_at_utc)),
  finished_at_utc timestamptz,
  primary key (command_id,run_worker_id,target_preparation_revision),
  unique (job_id,target_preparation_revision),
  unique (run_worker_id,target_preparation_revision),
  foreign key (run_worker_id,run_id,candidate_id) references private.bpay_next_run_worker(id,run_id,candidate_id) on delete restrict,
  foreign key (run_id,candidate_id) references private.bpay_next_selection_candidate(run_id,candidate_id) on delete restrict,
  foreign key (draft_state_id,run_worker_id,candidate_id,captured_preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
  check ((initiator_kind='OFFICE')=(actor_user_id is not null)),
  check (initiator_kind<>'SYSTEM' or (action='RELEASE' and original_issue_code is not null and original_issue_code in
    ('SELECTED_WORK_NOT_APPROVED','SELECTED_WORK_REVISION_CHANGED','SELECTED_WORK_POSITION_PENDING',
     'SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE','CASE_NO_PAYABLE_AMOUNT'))),
  check ((resume_work_cursor is null)=(resume_component_cursor is null)),
  check ((action='REPREPARE')=(resume_phase is not null)),
  check (released_work_count<=expected_work_count and released_case_hold_count<=expected_case_hold_count),
  check ((status='COMPLETE')=(stage='COMPLETE' and finished_at_utc is not null)),
  check (finished_at_utc is null or (pg_catalog.isfinite(finished_at_utc) and finished_at_utc>=accepted_at_utc))
);
create index bpay_next_worker_action_pending_run_idx on private.bpay_next_worker_reprepare_request(run_id,action)
  where status<>'COMPLETE';
create index bpay_next_worker_action_original_idx on private.bpay_next_worker_reprepare_request(original_prepare_command_id,run_worker_id);
alter table private.bpay_next_worker_reprepare_request enable row level security;
revoke all on private.bpay_next_worker_reprepare_request from public,anon,authenticated,service_role;

-- Economic state FK remains original; expiry separately fences execution.
alter table private.bpay_next_preparation_expiry_worker
  add column expected_execution_revision bigint check (expected_execution_revision>0),
  add column already_worker_released boolean not null default false;

-- Replace only the original audited worker-shape predicate, regardless of its
-- provider-generated constraint name. All other expiry checks/FKs remain.
do $audit$
declare v_constraint text;v_count integer;
begin
  select min(conname),count(*) into v_constraint,v_count from pg_catalog.pg_constraint
    where conrelid='private.bpay_next_preparation_expiry_worker'::regclass and contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(conbin,conrelid),'\s','','g')=
        pg_catalog.regexp_replace($shape$(((run_worker_id IS NULL) AND (original_worker_status IS NULL) AND (preparation_revision IS NULL) AND (selection_revision IS NULL) AND (draft_state_id IS NULL) AND (expected_work_count = 0) AND (expected_case_hold_count = 0) AND (original_review_issue_code IS NULL) AND (original_review_issue_work_id IS NULL)) OR ((run_worker_id IS NOT NULL) AND (original_worker_status = ANY (ARRAY['PREPARING'::text, 'REVIEW'::text, 'READY'::text])) AND (preparation_revision IS NOT NULL) AND (selection_revision IS NOT NULL)))$shape$,'\s','','g');
  if v_count<>1 then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ORIGINAL_WORKER_SHAPE_UNRECOGNISED';end if;
  execute pg_catalog.format('alter table private.bpay_next_preparation_expiry_worker drop constraint %I',v_constraint);
end
$audit$;
alter table private.bpay_next_preparation_expiry_worker add constraint bpay_next_expiry_worker_shape_v2_ck check (
  (run_worker_id is null and original_worker_status is null and preparation_revision is null and selection_revision is null
    and draft_state_id is null and expected_work_count=0 and expected_case_hold_count=0 and original_review_issue_code is null
    and original_review_issue_work_id is null and expected_execution_revision is null and not already_worker_released)
  or (run_worker_id is not null and preparation_revision is not null and selection_revision is not null
    and expected_execution_revision>=preparation_revision
    and ((not already_worker_released and original_worker_status in ('PREPARING','REVIEW','READY'))
      or (already_worker_released and original_worker_status='CANCELLED' and expected_work_count=0 and expected_case_hold_count=0)))
);

commit;
