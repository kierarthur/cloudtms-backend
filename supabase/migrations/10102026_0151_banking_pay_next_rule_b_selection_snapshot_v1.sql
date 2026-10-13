-- F-25 owner rule B: exact selected WORK revisions and raw Create Draft gates.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_run_work_query_snapshot (
 run_id uuid not null,
 work_id uuid not null,
 expected_revision_id uuid not null,
 physical_timesheet_id uuid not null,
 gate_result jsonb not null,
 captured_at_utc timestamptz not null default transaction_timestamp(),
 primary key(run_id,work_id),
 foreign key(run_id,work_id) references private.bpay_next_run_selection(run_id,work_id) on delete restrict,
 foreign key(work_id,expected_revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict
);
alter table private.bpay_next_run_work_query_snapshot owner to postgres;
alter table private.bpay_next_run_work_query_snapshot enable row level security;
revoke all on private.bpay_next_run_work_query_snapshot from public,anon,authenticated,service_role;
alter table private.bpay_next_selection_candidate
 add column target_pay_channel text,
 add column target_umbrella_id uuid references public.umbrellas(id) on delete restrict,
 add column target_umbrella_vat_chargeable boolean,
 add column min_take_home_wtd numeric(12,2);

commit;
