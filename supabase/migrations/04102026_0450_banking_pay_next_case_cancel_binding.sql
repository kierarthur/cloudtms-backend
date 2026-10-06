-- Ordered whole-worker cancellation identity/checkpoint. No history or paid
-- backfill; public SIMPLE_CANCEL shapes and job phases remain unchanged.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_case_cancel_binding (
  command_id uuid primary key references private.bpay_next_cancel_request(command_id) on delete restrict,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  draft_state_id uuid not null,
  preparation_revision bigint not null check (preparation_revision>0),
  selection_revision bigint not null check (selection_revision>0),
  status text not null check (status in ('REQUESTED','CANCELLING','CANCELLED','BLOCKED')),
  stage text not null check (stage in ('INTENT','WORK','CASE','FINAL','COMPLETE','BLOCKED')),
  checkpoint bigint not null default 0 check (checkpoint>=0),
  expected_work_count bigint not null check (expected_work_count>=0),
  released_work_count bigint not null default 0 check (released_work_count between 0 and expected_work_count),
  work_cursor bigint check (work_cursor>0),
  expected_case_hold_count bigint check (expected_case_hold_count>=0),
  released_case_hold_count bigint not null default 0 check (released_case_hold_count>=0),
  case_hold_cursor uuid,
  accepted_projection_revision bigint check (accepted_projection_revision>=0),
  projection_id uuid,
  net_state_id uuid,
  transfer_id uuid,
  blocked_code text check (pg_catalog.octet_length(blocked_code) between 1 and 256),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  started_at_utc timestamptz,
  finished_at_utc timestamptz,
  foreign key (run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (draft_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
  foreign key (net_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
  foreign key (projection_id,run_worker_id) references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  foreign key (transfer_id,run_worker_id) references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  check (expected_case_hold_count is null or released_case_hold_count<=expected_case_hold_count),
  check ((status='REQUESTED' and stage='INTENT' and checkpoint=0 and started_at_utc is null and finished_at_utc is null
      and expected_case_hold_count is null and accepted_projection_revision is null and projection_id is null and net_state_id is null and transfer_id is null)
    or (status='CANCELLING' and stage in ('WORK','CASE','FINAL') and started_at_utc is not null and finished_at_utc is null
      and expected_case_hold_count is not null and accepted_projection_revision is not null)
    or (status='CANCELLED' and stage='COMPLETE' and started_at_utc is not null and finished_at_utc is not null
      and expected_case_hold_count is not null and accepted_projection_revision is not null
      and released_work_count=expected_work_count and released_case_hold_count=expected_case_hold_count)
    or (status='BLOCKED' and stage='BLOCKED' and finished_at_utc is not null and released_work_count=0 and released_case_hold_count=0)),
  check ((accepted_projection_revision is null and projection_id is null and net_state_id is null)
    or (accepted_projection_revision=0 and projection_id is null and net_state_id is null)
    or (accepted_projection_revision>0 and projection_id is not null and net_state_id is not null)),
  check ((status='BLOCKED')=(blocked_code is not null)),
  check (pg_catalog.isfinite(created_at_utc) and (started_at_utc is null or pg_catalog.isfinite(started_at_utc))
    and (finished_at_utc is null or pg_catalog.isfinite(finished_at_utc)))
);
create index bpay_next_case_cancel_worker_idx on private.bpay_next_case_cancel_binding(run_worker_id,command_id);
-- Dropping released rows from this index avoids walking every retired NET
-- reservation on each current cancellation page.
create index bpay_next_case_hold_active_worker_page_idx on private.bpay_next_case_hold(run_worker_id,id) where status='ACTIVE';
alter table private.bpay_next_case_cancel_binding owner to postgres;
alter table private.bpay_next_case_cancel_binding enable row level security;
revoke all on table private.bpay_next_case_cancel_binding from public,anon,authenticated,service_role;
commit;
