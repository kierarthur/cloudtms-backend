-- WB44 finite current-projection continuation; no financial command or history.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_workbench_revision
  add column pending_task_count bigint not null default 0 check(pending_task_count>=0);
create table private.bpay_next_workbench_snooze_due (
  snooze_id uuid primary key references private.bpay_next_snooze(id),
  work_id uuid references private.bpay_next_work(id),
  case_id uuid references private.bpay_next_finance_case(id),
  snooze_version bigint not null check(snooze_version>0),
  until_date date,
  pending boolean not null,
  check(not pending or until_date is not null),
  check((work_id is null)=(case_id is not null))
);
create index bpay_next_workbench_snooze_expiry_idx on private.bpay_next_workbench_snooze_due(until_date,snooze_id)
  where pending;
create index bpay_next_workbench_snooze_seed_idx on private.bpay_next_snooze(id) where active;
alter table private.bpay_next_workbench_snooze_due owner to postgres;
alter table private.bpay_next_workbench_snooze_due enable row level security;
alter table private.bpay_next_workbench_snooze_due force row level security;
revoke all on private.bpay_next_workbench_snooze_due from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_snooze_due
  for all to CURRENT_USER using(true) with check(true);
alter table private.bpay_next_workbench_candidate_metadata
  add column target_pay_channel text check(target_pay_channel in ('PAYE','UMBRELLA')),
  add column target_umbrella_id uuid references public.umbrellas(id),
  add column target_umbrella_vat_chargeable boolean,
  add column target_umbrella_enabled boolean;
create index bpay_next_workbench_metadata_umbrella_idx
  on private.bpay_next_workbench_candidate_metadata(target_umbrella_id,candidate_id)
  where target_pay_channel='UMBRELLA';

create table private.bpay_next_workbench_task (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  task_key text not null unique check(pg_catalog.octet_length(task_key)<=64),
  task_type text not null check(task_type in ('SEED','METADATA','ORIGIN','UMBRELLA')),
  candidate_id uuid references public.candidates(id),
  work_id uuid references private.bpay_next_work(id),
  umbrella_id uuid references public.umbrellas(id),
  generation bigint not null default 1 check(generation>0),
  module_epoch bigint not null check(module_epoch>0),
  owner_epoch bigint not null default 1 check(owner_epoch>0),
  status text not null default 'READY' check(status in ('READY','LEASED','DONE','ERROR')),
  phase text not null check(phase in ('SEED_WORK','SEED_CASE','SEED_GROUP','SEED_SNOOZE','SEED_METADATA','SEED_DRAIN',
    'METADATA_GROUP','METADATA_SUMMARY','ORIGIN_CASE','ORIGIN_WORK','UMBRELLA_CANDIDATE','DONE')),
  cursor_key text check(pg_catalog.octet_length(cursor_key)<=64),
  through_work_id uuid,through_case_id uuid,through_candidate_id uuid,through_snooze_id uuid,through_group_key text,through_summary_key text,
  seed_fenced boolean not null default false,
  financial_view_revision bigint check(financial_view_revision>=0),
  metadata_version bigint check(metadata_version>0),
  expected_revision_id uuid,
  expected_applied_revision_id uuid,
  expected_approval_state text check(expected_approval_state in ('PENDING','APPROVED','WITHDRAWN')),
  expected_wait_state boolean,
  bank_warning_changed boolean not null default false,
  control_changed boolean not null default false,
  step_no bigint not null default 0 check(step_no>=0),
  rows_visited bigint not null default 0 check(rows_visited>=0),
  lease_nonce uuid,lease_until_utc timestamptz,
  wake_after_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  available_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  attempt_count bigint not null default 0 check(attempt_count>=0),
  last_error_code text check(pg_catalog.octet_length(last_error_code)<=256),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id),
  foreign key(work_id,expected_revision_id) references private.bpay_next_work_revision(work_id,id),
  foreign key(work_id,expected_applied_revision_id) references private.bpay_next_work_revision(work_id,id),
  check(case when status='LEASED' then lease_nonce is not null and lease_until_utc is not null
    else lease_nonce is null and lease_until_utc is null end),
  check((status='DONE')=(phase='DONE')),
  check((task_type='SEED' and candidate_id is null and work_id is null and umbrella_id is null and task_key='SEED')
    or(task_type='METADATA' and candidate_id is not null and work_id is null and umbrella_id is null and task_key='METADATA:'||candidate_id::text)
    or(task_type='ORIGIN' and candidate_id is not null and work_id is not null and umbrella_id is null and task_key='ORIGIN:'||work_id::text)
    or(task_type='UMBRELLA' and candidate_id is null and work_id is null and umbrella_id is not null and task_key='UMBRELLA:'||umbrella_id::text))
);
create index bpay_next_workbench_task_wake_idx on private.bpay_next_workbench_task(wake_after_utc,id)
  where status in ('READY','ERROR');
create index bpay_next_workbench_task_expired_idx on private.bpay_next_workbench_task(lease_until_utc,id)
  where status='LEASED';
create index bpay_next_workbench_task_candidate_pending_idx on private.bpay_next_workbench_task(candidate_id,id)
  where status<>'DONE';
create index bpay_next_workbench_seed_work_idx on private.bpay_next_work(id)
  where approval_state in ('APPROVED','WITHDRAWN') and (current_revision_id is not null or applied_revision_id is not null);
create index bpay_next_workbench_seed_case_idx on private.bpay_next_finance_case(id)
  where status in ('OPEN','PAUSED');
create index bpay_next_workbench_origin_case_page_idx on private.bpay_next_work_collection(work_id,case_id)
  where case_id is not null;
create index bpay_next_workbench_candidate_metadata_page_idx
  on private.bpay_next_workbench_candidate_summary(candidate_id,((client_scope||':'||pay_channel||':'||section) collate "C"));

alter table private.bpay_next_workbench_task owner to postgres;
alter table private.bpay_next_workbench_task enable row level security;
alter table private.bpay_next_workbench_task force row level security;
revoke all on private.bpay_next_workbench_task from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_task
  for all to CURRENT_USER using(true) with check(true);
insert into private.bpay_next_workbench_task(task_key,task_type,module_epoch,phase)
  select 'SEED','SEED',owner_epoch,'SEED_WORK' from private.bpay_next_module_control where id=1;
update private.bpay_next_workbench_revision set pending_task_count=1 where id=1;

commit;
