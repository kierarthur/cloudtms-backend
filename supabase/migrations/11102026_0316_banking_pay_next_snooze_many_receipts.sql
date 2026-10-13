-- WB-38 only: frozen native expense/mileage scope and bounded immutable page receipts.
-- No public table, claim, financial fact, producer or existing snooze guard changes.

\set ON_ERROR_STOP on

begin;

create index bpay_next_work_revision_snooze_physical_idx on private.bpay_next_work_revision
  (physical_timesheet_id,physical_timesheet_version,work_id,id) where sealed_at_utc is not null;

create index bpay_next_snooze_event_scope_cut_idx on private.bpay_next_snooze_event
  ((after_state->>'work_id'),(after_state->>'scope_kind'),(after_state->>'scope_identity'),control_sequence desc,id desc);

create table private.bpay_next_snooze_many_request (
  request_id uuid primary key references private.bpay_next_command(id),
  actor_id uuid not null references public.tms_users(id),
  candidate_id uuid not null references public.candidates(id),
  work_id uuid not null references private.bpay_next_work(id),
  revision_id uuid not null references private.bpay_next_work_revision(id),
  physical_timesheet_id uuid not null references public.timesheets(timesheet_id),
  module_epoch bigint not null check(module_epoch>0),
  action text not null check(action in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY')),
  request_body jsonb not null check(jsonb_typeof(request_body)='object'),
  control_cut bigint not null check(control_cut>=0),
  worker_pay_basis bytea not null check(octet_length(worker_pay_basis)=32),
  warning_context jsonb not null check(jsonb_typeof(warning_context)='object'),
  aggregate_control jsonb,
  whole_control jsonb,
  aggregate_ack_request_id uuid not null unique default gen_random_uuid(),
  aggregate_mutation_request_id uuid not null unique default gen_random_uuid(),
  aggregate_result jsonb,
  total_count bigint not null check(total_count>0),
  successful_count bigint not null default 0 check(successful_count>=0),
  failed_count bigint not null default 0 check(failed_count>=0),
  next_after_line_no integer default 0 check(next_after_line_no>=0),
  next_page_no bigint not null default 1 check(next_page_no>0),
  check(successful_count+failed_count<=total_count),
  created_at_utc timestamptz not null default clock_timestamp()
);
create table private.bpay_next_snooze_many_line (
  request_id uuid not null references private.bpay_next_snooze_many_request(request_id),
  line_no integer not null check(line_no>0),
  approved_line_id uuid not null references private.bpay_next_approved_line(id),
  component_key text not null,
  label text not null,
  amount text not null,
  worker_pay_basis bytea not null check(octet_length(worker_pay_basis)=32),
  initial_control jsonb,
  ack_request_id uuid not null unique default gen_random_uuid(),
  mutation_request_id uuid not null unique default gen_random_uuid(),
  successful boolean not null default false,
  last_result jsonb,
  primary key(request_id,line_no),
  unique(request_id,approved_line_id)
);
create table private.bpay_next_snooze_many_page (
  request_id uuid not null references private.bpay_next_snooze_many_request(request_id),
  page_no bigint not null check(page_no>0),
  request_body jsonb not null check(jsonb_typeof(request_body)='object'),
  result_body jsonb not null check(coalesce(jsonb_typeof(result_body)='object' and result_body->'received'='true'::jsonb
    and jsonb_typeof(result_body#>'{result,rows}')='array' and jsonb_array_length(result_body#>'{result,rows}')<=100,false)),
  created_at_utc timestamptz not null default clock_timestamp(),
  primary key(request_id,page_no)
);
alter table private.bpay_next_snooze_many_request owner to postgres;
alter table private.bpay_next_snooze_many_line owner to postgres;
alter table private.bpay_next_snooze_many_page owner to postgres;
alter table private.bpay_next_snooze_many_request enable row level security;
alter table private.bpay_next_snooze_many_line enable row level security;
alter table private.bpay_next_snooze_many_page enable row level security;
revoke all on private.bpay_next_snooze_many_request,private.bpay_next_snooze_many_line,private.bpay_next_snooze_many_page from public,anon,authenticated,service_role;

commit;
