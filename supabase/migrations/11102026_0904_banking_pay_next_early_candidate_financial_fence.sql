-- Early DR37 D14 only: real immutable NET request/job/state identities; no acquisition.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_candidate_week_commit_fence (
 candidate_id uuid not null,
 pay_week_start date not null check(pg_catalog.isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
 command_id uuid not null references private.bpay_next_paye_net_request(command_id) on delete restrict,
 run_worker_id uuid not null,
 original_job_id uuid not null,
 module_epoch bigint not null check(module_epoch>0),
 allocation_state_id uuid not null references private.bpay_next_case_allocation_state(id) on delete restrict,
 stage text not null check(stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING','COMPLETE','REFUSED','CANCELLED')),
 created_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check(pg_catalog.isfinite(created_at_utc)),
 updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check(pg_catalog.isfinite(updated_at_utc)),
 primary key(candidate_id,pay_week_start),
 foreign key(run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
 foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 foreign key(original_job_id,command_id,candidate_id,module_epoch) references private.bpay_next_job(id,command_id,candidate_id,module_epoch) on delete restrict,
 foreign key(candidate_id,pay_week_start) references private.bpay_next_worker_week(candidate_id,pay_week_start) on delete restrict,
 check(updated_at_utc>=created_at_utc)
);
create unique index bpay_next_candidate_financial_fence_active_idx
 on private.bpay_next_candidate_week_commit_fence(candidate_id)
 where stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING');
alter table private.bpay_next_candidate_week_commit_fence owner to postgres;
alter table private.bpay_next_candidate_week_commit_fence enable row level security;
alter table private.bpay_next_candidate_week_commit_fence force row level security;
create policy bpay_next_candidate_financial_fence_owner on private.bpay_next_candidate_week_commit_fence
 to current_user using(true) with check(true);
revoke all on table private.bpay_next_candidate_week_commit_fence from public,anon,authenticated,service_role;
commit;
