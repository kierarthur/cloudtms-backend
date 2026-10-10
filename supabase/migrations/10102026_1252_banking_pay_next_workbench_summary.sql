-- WB-08 typed maintained projection. No earnings, allocation or seed/backfill.
-- Native day/Case parents remain financial authority. WB-44 alone enables reads.
\set ON_ERROR_STOP on
begin;

create table private.bpay_next_workbench_revision (
  id smallint primary key check(id=1),
  workbench_revision bigint not null default 0 check(workbench_revision>=0),
  adoption_revision bigint not null default 0 check(adoption_revision>=0),
  adoption_state text not null default 'UNSEEDED' check(adoption_state in ('UNSEEDED','SEEDING','CURRENT','ERROR')),
  dirty_candidate_count bigint not null default 0 check(dirty_candidate_count>=0),
  dirty_group_count bigint not null default 0 check(dirty_group_count>=0),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
insert into private.bpay_next_workbench_revision(id) values(1);

create table private.bpay_next_workbench_candidate_metadata (
  candidate_id uuid primary key references public.candidates(id),
  candidate_sort_name text not null check(octet_length(candidate_sort_name)<=8192),
  tms_ref text check(octet_length(tms_ref)<=8192),
  bank_warning_code text check(bank_warning_code in ('BANK_DETAILS_MISSING','BANK_DETAILS_CHANGED')),
  metadata_version bigint not null default 1 check(metadata_version>0),
  continuation_pending boolean not null default false,
  adopted_metadata_version bigint not null default 1 check(adopted_metadata_version>0),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check(adopted_metadata_version<=metadata_version),
  check(continuation_pending or adopted_metadata_version=metadata_version)
);
create index bpay_next_workbench_metadata_pending_idx on private.bpay_next_workbench_candidate_metadata(candidate_id)
  where continuation_pending;

create table private.bpay_next_workbench_group (
  group_key text primary key check(octet_length(group_key)<=64),
  group_kind text not null check(group_kind in ('WORK','CASE')),
  work_id uuid,case_id uuid,candidate_id uuid not null,
  client_id uuid references public.clients(id),contract_id uuid references public.contracts(id),
  physical_timesheet_id uuid references public.timesheets(timesheet_id),
  current_revision_id uuid,
  source_pay_channel text check(source_pay_channel in ('PAYE','UMBRELLA')),
  target_pay_channel text check(target_pay_channel in ('PAYE','UMBRELLA')),
  week_ending_date date,
  complete boolean not null,
  dirty boolean not null default false,
  projection_revision bigint not null check(projection_revision>0),
  financial_view_revision bigint not null check(financial_view_revision>=0),
  metadata_version bigint not null check(metadata_version>0),
  ready_count bigint not null default 0 check(ready_count>=0),
  action_count bigint not null default 0 check(action_count>=0),
  blocked_count bigint not null default 0 check(blocked_count>=0),
  waiting_count bigint not null default 0 check(waiting_count>=0),
  stale_snooze_count bigint not null default 0 check(stale_snooze_count>=0),
  stale_snooze_code text check(stale_snooze_code='SNOOZE_SEGMENT_STALE'),
  progress_state text not null check(progress_state in ('CURRENT','PROCESSING','UNAVAILABLE')),
  error_code text check(octet_length(error_code)<=256),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique(group_key,candidate_id),
  unique(work_id),unique(case_id),
  foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id),
  foreign key(work_id,current_revision_id) references private.bpay_next_work_revision(work_id,id),
  foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id),
  check((group_kind='WORK' and work_id is not null and case_id is null and group_key='WORK:'||work_id::text)
    or (group_kind='CASE' and case_id is not null and work_id is null and group_key='CASE:'||case_id::text))
);
create index bpay_next_workbench_group_candidate_idx on private.bpay_next_workbench_group(candidate_id,group_key);
create index bpay_next_workbench_group_scope_idx on private.bpay_next_workbench_group(client_id,target_pay_channel,candidate_id,group_key);

create table private.bpay_next_workbench_item (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  group_key text not null,candidate_id uuid not null,
  work_id uuid,revision_id uuid,approved_line_id uuid,approved_segment_id uuid,
  case_id uuid,case_component_id uuid,rule_id uuid,
  member_kind text not null check(member_kind in ('WORK_COMPONENT','WORK_SEGMENT','CASE_COMPONENT','GROUP_EVIDENCE')),
  component_key text not null check(octet_length(component_key)<=256),
  member_key text not null check(octet_length(member_key) between 1 and 256),
  segment_key text check(octet_length(segment_key) between 1 and 245),
  portion_kind text not null check(portion_kind in ('AVAILABLE','RESERVED','RECOVERY','ZERO','EVIDENCE')),
  client_id uuid references public.clients(id),contract_id uuid references public.contracts(id),
  physical_timesheet_id uuid references public.timesheets(timesheet_id),
  work_date date,week_ending_date date,
  candidate_display_name text check(octet_length(candidate_display_name)<=8192),
  candidate_reference text check(octet_length(candidate_reference)<=8192),
  client_display_name text check(octet_length(client_display_name)<=8192),
  timesheet_reference text check(octet_length(timesheet_reference)<=8192),
  component_kind text,tax_treatment text,
  source_pay_channel text check(source_pay_channel in ('PAYE','UMBRELLA')),
  target_pay_channel text check(target_pay_channel in ('PAYE','UMBRELLA')),
  direction text not null check(direction in ('PAYMENT','DEDUCTION','INFORMATIONAL')),
  payroll_stage text check(payroll_stage in ('GROSS_ADD','GROSS_DEDUCT','NET_ADD','NET_DEDUCT')),
  approved_quantity numeric,unit_label text check(octet_length(unit_label)<=256),
  nominal_source_ex_vat private.bpay_next_penny_amount,
  approved_source_ex_vat private.bpay_next_penny_amount,
  realised_source_ex_vat private.bpay_next_penny_amount,
  active_reserved_source_ex_vat private.bpay_next_penny_amount,
  residual_source_ex_vat private.bpay_next_penny_amount,
  gross_due_source_ex_vat private.bpay_next_penny_amount,
  included_source_ex_vat private.bpay_next_penny_amount,
  excluded_source_ex_vat private.bpay_next_penny_amount,
  recovery_source_ex_vat private.bpay_next_penny_amount,
  held_overhang_source_ex_vat private.bpay_next_penny_amount,
  deduction_balance_source_ex_vat private.bpay_next_penny_amount,
  selected_source_ex_vat private.bpay_next_penny_amount,
  amount_known boolean not null,
  selected_amount_known boolean not null default false,
  section text not null check(section in ('READY','ACTION','BLOCKED','WAITING')),
  reason_code text not null check(octet_length(reason_code) between 1 and 256),
  reason_priority smallint not null check(reason_priority between 1 and 200),
  selectable boolean not null,
  is_selected boolean not null default false,
  eligible_from_date date,
  snooze_id uuid references private.bpay_next_snooze(id),snooze_version bigint,
  snooze_scope text,snooze_until_date date,
  stale_snooze_code text check(stale_snooze_code in ('SNOOZE_SEGMENT_STALE','SNOOZE_BASIS_STALE')),
  query_state text not null default 'NOT_APPLICABLE' check(query_state in ('NOT_APPLICABLE','UNAVAILABLE','CLEAR','OPEN')),
  query_qualified boolean not null default false,
  pay_hold boolean,pay_hold_reason text check(octet_length(pay_hold_reason)<=8192),
  channel_note_code text check(channel_note_code in ('CHANNEL_RESOLVER','MOVE_CHANNEL_RESOLVER')),
  accepted_run_id uuid references private.bpay_next_pay_run(id),
  accepted_command_id uuid references private.bpay_next_command(id),
  accepted_module_epoch bigint,
  financial_view_revision bigint not null check(financial_view_revision>=0),
  projection_revision bigint not null check(projection_revision>0),
  metadata_version bigint not null check(metadata_version>0),
  progress_state text not null check(progress_state in ('CURRENT','PROCESSING','UNAVAILABLE')),
  error_code text check(octet_length(error_code)<=256),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique(group_key,component_key,member_kind,member_key,portion_kind),
  foreign key(group_key,candidate_id) references private.bpay_next_workbench_group(group_key,candidate_id),
  foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id),
  foreign key(work_id,revision_id) references private.bpay_next_work_revision(work_id,id),
  foreign key(revision_id,approved_line_id,component_key) references private.bpay_next_approved_line(revision_id,id,component_key),
  foreign key(revision_id,approved_segment_id,component_key,segment_key)
    references private.bpay_next_approved_segment(revision_id,id,component_key,segment_key),
  foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id),
  foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id),
  foreign key(rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id),
  check(group_key=case when work_id is not null then 'WORK:'||work_id::text else 'CASE:'||case_id::text end),
  check((member_kind in ('WORK_COMPONENT','WORK_SEGMENT') and work_id is not null and case_id is null
       and case_component_id is null and rule_id is null and component_key<>''
       and ((member_kind='WORK_SEGMENT' and segment_key is not null and member_key=segment_key)
         or (member_kind='WORK_COMPONENT' and segment_key is null and approved_segment_id is null and member_key=component_key)))
    or (member_kind='CASE_COMPONENT' and case_id is not null and case_component_id is not null and rule_id is not null
       and work_id is null and revision_id is null and approved_line_id is null and approved_segment_id is null
       and segment_key is null and member_key=case_component_id::text and component_key<>'')
    or (member_kind='GROUP_EVIDENCE' and portion_kind='EVIDENCE' and component_key='' and segment_key is null
       and case_component_id is null and approved_line_id is null and approved_segment_id is null
       and not selectable and member_key=coalesce(work_id,case_id)::text and ((work_id is null)<>(case_id is null)))),
  check(not selectable or (section='READY' and amount_known and portion_kind in ('AVAILABLE','ZERO')
    and member_kind<>'GROUP_EVIDENCE')),
  check((snooze_id is null)=(snooze_version is null)),
  check(not is_selected or accepted_run_id is not null),
  check((accepted_run_id is null)=(accepted_command_id is null)),
  check(not selected_amount_known or is_selected),
  check(not selected_amount_known or selected_source_ex_vat is not null),
  check(not amount_known or nominal_source_ex_vat is not null),
  check(not amount_known or direction<>'DEDUCTION' or deduction_balance_source_ex_vat is not null),
  check(amount_known or (nominal_source_ex_vat is null and approved_source_ex_vat is null
    and realised_source_ex_vat is null and active_reserved_source_ex_vat is null and residual_source_ex_vat is null
    and gross_due_source_ex_vat is null and included_source_ex_vat is null and excluded_source_ex_vat is null
    and recovery_source_ex_vat is null and held_overhang_source_ex_vat is null and deduction_balance_source_ex_vat is null)),
  check(not amount_known or (gross_due_source_ex_vat=included_source_ex_vat+excluded_source_ex_vat
    and included_source_ex_vat>=0 and excluded_source_ex_vat>=0 and active_reserved_source_ex_vat>=0
    and recovery_source_ex_vat>=0 and held_overhang_source_ex_vat>=0)),
  check(portion_kind<>'RESERVED' or (amount_known and not selectable and active_reserved_source_ex_vat>0
    and included_source_ex_vat=0 and excluded_source_ex_vat=0)),
  check(portion_kind<>'AVAILABLE' or (amount_known and nominal_source_ex_vat>0)),
  check(portion_kind<>'ZERO' or (amount_known and member_kind in ('WORK_COMPONENT','WORK_SEGMENT')
    and nominal_source_ex_vat=0 and approved_source_ex_vat=0 and realised_source_ex_vat=0
    and active_reserved_source_ex_vat=0 and residual_source_ex_vat=0 and gross_due_source_ex_vat=0
    and included_source_ex_vat=0 and excluded_source_ex_vat=0 and recovery_source_ex_vat=0 and held_overhang_source_ex_vat=0)),
  check(portion_kind<>'RECOVERY' or (amount_known and direction='INFORMATIONAL' and not selectable and recovery_source_ex_vat>0)),
  check(portion_kind<>'EVIDENCE' or (not amount_known and not selectable)),
  check(not amount_known or (approved_source_ex_vat is not null and realised_source_ex_vat is not null
    and active_reserved_source_ex_vat is not null and residual_source_ex_vat is not null
    and gross_due_source_ex_vat is not null and included_source_ex_vat is not null and excluded_source_ex_vat is not null
    and recovery_source_ex_vat is not null and held_overhang_source_ex_vat is not null))
);
create index bpay_next_workbench_item_candidate_idx on private.bpay_next_workbench_item(candidate_id,section,group_key,component_key,member_kind,member_key,portion_kind);
create index bpay_next_workbench_item_work_idx on private.bpay_next_workbench_item(work_id,component_key,segment_key);
create index bpay_next_workbench_item_case_idx on private.bpay_next_workbench_item(case_id,case_component_id);
create index bpay_next_workbench_item_future_idx on private.bpay_next_workbench_item(eligible_from_date,id) where reason_code='FUTURE_WEEK';
create index bpay_next_workbench_item_scope_idx on private.bpay_next_workbench_item(client_id,target_pay_channel,section,candidate_id,id);

create table private.bpay_next_workbench_candidate_summary (
  candidate_id uuid not null references public.candidates(id),
  client_id uuid references public.clients(id),
  client_scope text not null check(client_scope=case when client_id is null then 'ALL' else client_id::text end),
  pay_channel text not null check(pay_channel in ('PAYE','UMBRELLA','ALL')),
  section text not null check(section in ('READY','ACTION','BLOCKED','WAITING','ALL')),
  item_count bigint not null default 0 check(item_count>=0),
  contribution_count bigint not null default 0 check(contribution_count>=0),
  segment_count bigint not null default 0 check(segment_count>=0),
  payable_count bigint not null default 0 check(payable_count>=0),
  selected_count bigint not null default 0 check(selected_count>=0),
  snoozed_count bigint not null default 0 check(snoozed_count>=0),
  held_count bigint not null default 0 check(held_count>=0),
  queried_count bigint not null default 0 check(queried_count>=0),
  blocked_count bigint not null default 0 check(blocked_count>=0),
  deduction_item_count bigint not null default 0 check(deduction_item_count>=0),
  unknown_amount_count bigint not null default 0 check(unknown_amount_count>=0),
  unknown_selected_amount_count bigint not null default 0 check(unknown_selected_amount_count>=0),
  amount private.bpay_next_penny_amount not null default 0,
  waiting_amount private.bpay_next_penny_amount not null default 0,
  ready_net_add_amount private.bpay_next_penny_amount not null default 0,
  waiting_net_add_amount private.bpay_next_penny_amount not null default 0,
  gross_deduction_balance private.bpay_next_penny_amount not null default 0,
  net_deduction_balance private.bpay_next_penny_amount not null default 0,
  approved_amount private.bpay_next_penny_amount not null default 0,
  paid_credit_amount private.bpay_next_penny_amount not null default 0,
  reserved_amount private.bpay_next_penny_amount not null default 0,
  excluded_amount private.bpay_next_penny_amount not null default 0,
  recovery_amount private.bpay_next_penny_amount not null default 0,
  selected_amount private.bpay_next_penny_amount not null default 0,
  bank_warning_code text check(bank_warning_code in ('BANK_DETAILS_MISSING','BANK_DETAILS_CHANGED')),
  candidate_sort_name text not null check(octet_length(candidate_sort_name)<=8192),
  tms_ref text check(octet_length(tms_ref)<=8192),
  metadata_version bigint not null check(metadata_version>0),
  projection_revision bigint not null check(projection_revision>0),
  primary key(candidate_id,client_scope,pay_channel,section)
);
create index bpay_next_workbench_candidate_sort_idx on private.bpay_next_workbench_candidate_summary(section,client_scope,pay_channel,candidate_sort_name,candidate_id);

create table private.bpay_next_workbench_scope_summary (
  client_id uuid references public.clients(id),
  client_scope text not null check(client_scope=case when client_id is null then 'ALL' else client_id::text end),
  pay_channel text not null check(pay_channel in ('PAYE','UMBRELLA','ALL')),
  section text not null check(section in ('READY','ACTION','BLOCKED','WAITING','ALL')),
  candidate_count bigint not null default 0 check(candidate_count>=0),
  item_count bigint not null default 0 check(item_count>=0),
  contribution_count bigint not null default 0 check(contribution_count>=0),
  segment_count bigint not null default 0 check(segment_count>=0),
  payable_count bigint not null default 0 check(payable_count>=0),
  selected_count bigint not null default 0 check(selected_count>=0),
  snoozed_count bigint not null default 0 check(snoozed_count>=0),
  held_count bigint not null default 0 check(held_count>=0),
  queried_count bigint not null default 0 check(queried_count>=0),
  blocked_count bigint not null default 0 check(blocked_count>=0),
  deduction_item_count bigint not null default 0 check(deduction_item_count>=0),
  unknown_amount_count bigint not null default 0 check(unknown_amount_count>=0),
  unknown_selected_amount_count bigint not null default 0 check(unknown_selected_amount_count>=0),
  amount private.bpay_next_penny_amount not null default 0,
  waiting_amount private.bpay_next_penny_amount not null default 0,
  ready_net_add_amount private.bpay_next_penny_amount not null default 0,
  waiting_net_add_amount private.bpay_next_penny_amount not null default 0,
  gross_deduction_balance private.bpay_next_penny_amount not null default 0,
  net_deduction_balance private.bpay_next_penny_amount not null default 0,
  approved_amount private.bpay_next_penny_amount not null default 0,
  paid_credit_amount private.bpay_next_penny_amount not null default 0,
  reserved_amount private.bpay_next_penny_amount not null default 0,
  excluded_amount private.bpay_next_penny_amount not null default 0,
  recovery_amount private.bpay_next_penny_amount not null default 0,
  selected_amount private.bpay_next_penny_amount not null default 0,
  projection_revision bigint not null check(projection_revision>0),
  primary key(client_scope,pay_channel,section)
);

create type private.bpay_next_workbench_delta as (
  candidate_id uuid,client_id uuid,client_scope text,pay_channel text,section text,
  item_count bigint,
  contribution_count bigint,
  segment_count bigint,
  payable_count bigint,
  selected_count bigint,
  snoozed_count bigint,
  held_count bigint,
  queried_count bigint,
  blocked_count bigint,
  deduction_item_count bigint,
  unknown_amount_count bigint,
  unknown_selected_amount_count bigint,
  amount numeric,
  waiting_amount numeric,
  ready_net_add_amount numeric,
  waiting_net_add_amount numeric,
  gross_deduction_balance numeric,
  net_deduction_balance numeric,
  approved_amount numeric,
  paid_credit_amount numeric,
  reserved_amount numeric,
  excluded_amount numeric,
  recovery_amount numeric,
  selected_amount numeric
);
alter type private.bpay_next_workbench_delta owner to postgres;
revoke all on type private.bpay_next_workbench_delta from public,anon,authenticated,service_role;

alter table private.bpay_next_workbench_revision owner to postgres;
alter table private.bpay_next_workbench_revision enable row level security;
alter table private.bpay_next_workbench_revision force row level security;
revoke all on private.bpay_next_workbench_revision from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_revision
  for all to CURRENT_USER using(true) with check(true);

alter table private.bpay_next_workbench_candidate_metadata owner to postgres;
alter table private.bpay_next_workbench_candidate_metadata enable row level security;
alter table private.bpay_next_workbench_candidate_metadata force row level security;
revoke all on private.bpay_next_workbench_candidate_metadata from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_candidate_metadata
  for all to CURRENT_USER using(true) with check(true);

alter table private.bpay_next_workbench_group owner to postgres;
alter table private.bpay_next_workbench_group enable row level security;
alter table private.bpay_next_workbench_group force row level security;
revoke all on private.bpay_next_workbench_group from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_group
  for all to CURRENT_USER using(true) with check(true);

alter table private.bpay_next_workbench_item owner to postgres;
alter table private.bpay_next_workbench_item enable row level security;
alter table private.bpay_next_workbench_item force row level security;
revoke all on private.bpay_next_workbench_item from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_item
  for all to CURRENT_USER using(true) with check(true);

alter table private.bpay_next_workbench_candidate_summary owner to postgres;
alter table private.bpay_next_workbench_candidate_summary enable row level security;
alter table private.bpay_next_workbench_candidate_summary force row level security;
revoke all on private.bpay_next_workbench_candidate_summary from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_candidate_summary
  for all to CURRENT_USER using(true) with check(true);

alter table private.bpay_next_workbench_scope_summary owner to postgres;
alter table private.bpay_next_workbench_scope_summary enable row level security;
alter table private.bpay_next_workbench_scope_summary force row level security;
revoke all on private.bpay_next_workbench_scope_summary from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_scope_summary
  for all to CURRENT_USER using(true) with check(true);

commit;
