-- DR18 / BD138: exact confirmed Work/Case refresh membership on the existing WB08 task.
-- Private UUID scope only. No money, live public trigger, financial command or queue.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_workbench_task
  add column confirm_run_id uuid references private.bpay_next_pay_run(id),
  add column confirm_review_revision bigint check(confirm_review_revision>0),
  add constraint bpay_next_workbench_task_id_type_key unique(id,task_type);
alter table private.bpay_next_workbench_task
  drop constraint bpay_next_workbench_task_task_type_check,
  drop constraint bpay_next_workbench_task_phase_check,
  drop constraint bpay_next_workbench_task_check2;
alter table private.bpay_next_workbench_task
  add constraint bpay_next_workbench_task_task_type_check
    check(task_type in ('SEED','METADATA','ORIGIN','UMBRELLA','CONFIRM')),
  add constraint bpay_next_workbench_task_phase_check
    check(phase in ('SEED_WORK','SEED_CASE','SEED_GROUP','SEED_SNOOZE','SEED_METADATA','SEED_DRAIN',
      'METADATA_GROUP','METADATA_SUMMARY','ORIGIN_CASE','ORIGIN_WORK','UMBRELLA_CANDIDATE',
      'CONFIRM_WORK','CONFIRM_CASE','DONE')),
  add constraint bpay_next_workbench_task_scope_check check((
    (task_type<>'CONFIRM' and confirm_run_id is null and confirm_review_revision is null and (
      (task_type='SEED' and candidate_id is null and work_id is null and umbrella_id is null and task_key='SEED')
      or(task_type='METADATA' and candidate_id is not null and work_id is null and umbrella_id is null and task_key='METADATA:'||candidate_id::text)
      or(task_type='ORIGIN' and candidate_id is not null and work_id is not null and umbrella_id is null and task_key='ORIGIN:'||work_id::text)
      or(task_type='UMBRELLA' and candidate_id is null and work_id is null and umbrella_id is not null and task_key='UMBRELLA:'||umbrella_id::text)))
    or(task_type='CONFIRM' and candidate_id is null and work_id is null and umbrella_id is null
      and confirm_run_id is not null and confirm_review_revision is not null
      and task_key='CONFIRM:'||confirm_run_id::text and phase in ('CONFIRM_WORK','CONFIRM_CASE','DONE'))
  ) is true);
create unique index bpay_next_workbench_confirm_run_idx on private.bpay_next_workbench_task(confirm_run_id)
  where task_type='CONFIRM';

create table private.bpay_next_confirm_refresh_member (
  task_id uuid not null,
  task_type text not null default 'CONFIRM' check(task_type='CONFIRM'),
  member_kind text not null check(member_kind in ('WORK','CASE')),
  native_id uuid not null,
  work_id uuid references private.bpay_next_work(id),
  case_id uuid references private.bpay_next_finance_case(id),
  primary key(task_id,member_kind,native_id),
  foreign key(task_id,task_type) references private.bpay_next_workbench_task(id,task_type),
  check(((member_kind='WORK' and work_id=native_id and case_id is null)
    or(member_kind='CASE' and case_id=native_id and work_id is null)) is true)
);
alter table private.bpay_next_confirm_refresh_member owner to postgres;
alter table private.bpay_next_confirm_refresh_member enable row level security;
alter table private.bpay_next_confirm_refresh_member force row level security;
revoke all on private.bpay_next_confirm_refresh_member from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_confirm_refresh_member
  for all to CURRENT_USER using(true) with check(true);
create trigger bpay_next_confirm_refresh_member_immutable before update or delete
  on private.bpay_next_confirm_refresh_member for each row execute function private.bpay_next_effect_immutable_v1();
create trigger bpay_next_confirm_refresh_member_no_truncate before truncate
  on private.bpay_next_confirm_refresh_member for each statement execute function private.bpay_next_effect_immutable_v1();

commit;
