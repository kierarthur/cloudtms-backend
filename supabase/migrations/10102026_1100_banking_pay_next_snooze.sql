-- WB-36: exact ordered worker-financial snooze metadata. No money changes.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_snooze (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_id uuid not null references public.candidates(id), work_id uuid references private.bpay_next_work(id),
  expected_revision_id uuid references private.bpay_next_work_revision(id),
  physical_timesheet_id uuid references public.timesheets(timesheet_id),
  scope_kind text not null check(scope_kind in ('WHOLE_TIMESHEET','SEGMENT','EXPENSE_LINE','EXPENSES_ALL','CASE')),
  scope_identity text not null check(length(scope_identity) between 1 and 500), component_key text, segment_key text,
  approved_segment_id uuid references private.bpay_next_approved_segment(id),
  expense_approved_line_id uuid references private.bpay_next_approved_line(id),
  case_id uuid references private.bpay_next_finance_case(id), case_component_id uuid references private.bpay_next_case_component(id),
  case_kind text,
  snooze_kind text not null check(snooze_kind in ('DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT',
    'OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY')),
  worker_pay_basis bytea not null check(octet_length(worker_pay_basis)=32),
  until_date date, note text not null default '' check(length(note)<=500), version bigint not null default 1 check(version>0),
  control_sequence bigint not null check(control_sequence>0), active boolean not null default true,
  created_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  created_by uuid not null references public.tms_users(id), updated_by uuid not null references public.tms_users(id),
  last_request_id uuid not null,
  check((scope_kind='CASE')=(case_id is not null)), check((scope_kind='CASE')=(case_component_id is not null)),
  check((scope_kind='CASE')=(work_id is null)),
  check(scope_kind='CASE' or (expected_revision_id is not null and physical_timesheet_id is not null)),
  check((scope_kind='SEGMENT')=(segment_key is not null)), check((scope_kind='SEGMENT')=(approved_segment_id is not null)),
  check((scope_kind='EXPENSE_LINE')=(expense_approved_line_id is not null)),
  check(scope_kind not in ('SEGMENT','EXPENSE_LINE') or component_key is not null),
  foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id),
  foreign key(work_id,expected_revision_id) references private.bpay_next_work_revision(work_id,id),
  foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id),
  check(scope_kind='CASE' or case_kind is null),
  check(scope_kind<>'CASE' or (case_kind is not null and expected_revision_id is null and physical_timesheet_id is null)),
  check(scope_kind in ('SEGMENT','EXPENSE_LINE') or component_key is null),
  check((scope_kind='CASE' and ((snooze_kind='OVERPAYMENT_RECOVERY' and case_kind='OVERPAYMENT')
      or (snooze_kind='PAYMENT_ADVANCE_REPAYMENT' and case_kind in ('LOAN','ADVANCE'))
      or (snooze_kind='MANUAL_DEBT_RECOVERY' and case_kind='MANUAL_DEBT')))
    or (scope_kind<>'CASE' and snooze_kind in ('DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT'))),
  check(scope_kind not in ('EXPENSE_LINE','EXPENSES_ALL') or snooze_kind='DO_NOT_PAY')
);
create unique index bpay_next_snooze_active_scope_idx
  on private.bpay_next_snooze(candidate_id,scope_kind,scope_identity) where active;
create index bpay_next_snooze_active_work_idx
  on private.bpay_next_snooze(work_id,scope_kind,segment_key,component_key) where active;
create index bpay_next_snooze_scope_history_idx on private.bpay_next_snooze(work_id,scope_kind,scope_identity,id);
create index bpay_next_snooze_due_idx on private.bpay_next_snooze(until_date,id) where active and until_date is not null;
create table private.bpay_next_snooze_event (
  id uuid primary key default pg_catalog.gen_random_uuid(), snooze_id uuid not null references private.bpay_next_snooze(id),
  candidate_id uuid not null references public.candidates(id),
  event_kind text not null check(event_kind in ('SET','AMENDED','CLEARED','CANCELLED','WOKEN')),
  version bigint not null check(version>0), control_sequence bigint not null check(control_sequence>0),
  actor_id uuid references public.tms_users(id), request_id uuid not null, reason text not null check(length(reason)<=500),
  before_state jsonb, after_state jsonb not null, occurred_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  unique(snooze_id,version)
);
create index bpay_next_snooze_event_cut_idx on private.bpay_next_snooze_event(snooze_id,control_sequence desc);
alter table private.bpay_next_selected_segment add constraint bpay_next_selected_segment_snooze_event_fk
  foreign key(snooze_id,snooze_version) references private.bpay_next_snooze_event(snooze_id,version);
alter table private.bpay_next_run_position add constraint bpay_next_run_position_snooze_event_fk
  foreign key(snooze_id,snooze_version) references private.bpay_next_snooze_event(snooze_id,version);
create table private.bpay_next_snooze_warning_ack (
  token uuid primary key default pg_catalog.gen_random_uuid(), request_id uuid not null unique,
  actor_id uuid not null references public.tms_users(id), candidate_id uuid not null references public.candidates(id),
  module_epoch bigint not null check(module_epoch>0),
  scope_kind text not null check(scope_kind in ('WHOLE_TIMESHEET','SEGMENT','EXPENSE_LINE','EXPENSES_ALL','CASE')),
  scope_identity text not null check(length(scope_identity) between 1 and 500),
  expected_revision_id uuid, expected_version bigint not null check(expected_version>=0),
  worker_pay_basis bytea not null check(octet_length(worker_pay_basis)=32), warning_context jsonb not null,
  issued_at_utc timestamptz not null, expires_at_utc timestamptz not null,
  consumed_by_request_id uuid unique, consumed_at_utc timestamptz,
  check(expires_at_utc=issued_at_utc+interval '30 minutes'),
  check((consumed_by_request_id is null)=(consumed_at_utc is null))
);
-- Exact metadata replay; CHECK never writes. WB-19 owns the one public W2 route.
create table private.bpay_next_snooze_request (
  request_id uuid primary key references private.bpay_next_command(id), actor_id uuid not null references public.tms_users(id),
  action text not null check(action in ('SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR')),
  request_body jsonb not null, result_body jsonb not null,
  created_at_utc timestamptz not null default pg_catalog.clock_timestamp()
);
do $security$
declare v_table text;
begin
  foreach v_table in array array['bpay_next_snooze','bpay_next_snooze_event','bpay_next_snooze_warning_ack','bpay_next_snooze_request'] loop
    execute pg_catalog.format('alter table private.%I enable row level security',v_table);
    execute pg_catalog.format('revoke all on private.%I from public,anon,authenticated,service_role',v_table);
  end loop;
end
$security$;
alter table private.bpay_next_snooze owner to postgres;
alter table private.bpay_next_snooze_event owner to postgres;
alter table private.bpay_next_snooze_warning_ack owner to postgres;
alter table private.bpay_next_snooze_request owner to postgres;
create policy service_owner on private.bpay_next_snooze to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_snooze_event to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_snooze_warning_ack to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_snooze_request to CURRENT_USER using(true) with check(true);

commit;
