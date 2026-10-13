-- WB15/owner A3e: indexed own-reader display only; native identities and existing projection values.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_workbench_group_display(
  presentation_key text not null check(presentation_key=presentation_kind||':'||presentation_id::text),
  presentation_kind text not null check(presentation_kind in ('WORK','CASE','ROW')),presentation_id uuid not null,
  parent_group_key text not null,candidate_id uuid not null,case_component_id uuid references private.bpay_next_case_component(id),
  section text not null check(section in ('READY','WAITING','ACTION','BLOCKED','ALL')),
  client_id uuid references public.clients(id),week_ending_date date,client_display_name text check(octet_length(client_display_name)<=8192),
  payment_label text not null check(octet_length(payment_label)<=256),timesheet_reference text check(octet_length(timesheet_reference)<=8192),
  target_pay_channel text check(target_pay_channel in ('PAYE','UMBRELLA')),has_deduction boolean not null,selectable boolean not null,reason_code text not null check(reason_code~'^[A-Z][A-Z0-9_]{0,255}$'),
  reason_priority smallint not null check(reason_priority between 1 and 200),
  group_amount numeric,ready_net_add_amount numeric,gross_deduction_balance numeric,net_deduction_balance numeric,waiting_amount numeric,waiting_net_add_amount numeric,
  accepted_run_id uuid references private.bpay_next_pay_run(id),accepted_command_id uuid references private.bpay_next_command(id),accepted_module_epoch bigint,
  snooze_until_date date,eligible_from_date date,pay_hold_reason text check(octet_length(pay_hold_reason)<=8192),
  primary key(presentation_key,section),foreign key(parent_group_key,candidate_id) references private.bpay_next_workbench_group(group_key,candidate_id),
  check((presentation_kind='ROW')=(case_component_id is not null)),
  check(presentation_kind<>'ROW' or presentation_id=case_component_id),
  check(presentation_kind='ROW' or presentation_key=parent_group_key),
  check((accepted_run_id is null)=(accepted_command_id is null) and (accepted_run_id is null)=(accepted_module_epoch is null)),
  check((group_amount is null)=(ready_net_add_amount is null) and (group_amount is null)=(gross_deduction_balance is null) and (group_amount is null)=(net_deduction_balance is null) and (group_amount is null)=(waiting_amount is null) and (group_amount is null)=(waiting_net_add_amount is null))
);
create index bpay_next_wb15_display_parent_idx on private.bpay_next_workbench_group_display(parent_group_key);
create index bpay_next_wb15_payment_sort_idx on private.bpay_next_workbench_group_display(candidate_id,section,(week_ending_date is null),coalesce(week_ending_date,date '0001-01-01'),presentation_kind collate "C",presentation_id);
create index bpay_next_wb15_client_sort_idx on private.bpay_next_workbench_group_display(candidate_id,section,(client_display_name is null),coalesce(client_display_name,'') collate "C",presentation_kind collate "C",presentation_id);
create index bpay_next_wb15_deduction_sort_idx on private.bpay_next_workbench_group_display(candidate_id,section,has_deduction,presentation_kind collate "C",presentation_id);
create index bpay_next_wb15_amount_sort_idx on private.bpay_next_workbench_group_display(candidate_id,section,(group_amount is null),coalesce(group_amount,0),presentation_kind collate "C",presentation_id);
create index bpay_next_wb15_reason_sort_idx on private.bpay_next_workbench_group_display(candidate_id,section,reason_priority,reason_code collate "C",presentation_kind collate "C",presentation_id);
create index bpay_next_wb15_channel_sort_idx on private.bpay_next_workbench_group_display(candidate_id,section,(target_pay_channel is null),coalesce(target_pay_channel,'') collate "C",presentation_kind collate "C",presentation_id);
alter table private.bpay_next_workbench_group_display owner to postgres;
alter table private.bpay_next_workbench_group_display enable row level security;
alter table private.bpay_next_workbench_group_display force row level security;
revoke all on private.bpay_next_workbench_group_display from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_workbench_group_display for all to CURRENT_USER using(true) with check(true);

-- Additive initialization from existing maintained rows only, identical to the writer helper.
-- WB15 own-list display projection. Values never enter a financial command.
insert into private.bpay_next_workbench_group_display(
  presentation_key,presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,
  section,client_id,week_ending_date,client_display_name,payment_label,timesheet_reference,
  target_pay_channel,has_deduction,selectable,reason_code,reason_priority,group_amount,ready_net_add_amount,
  gross_deduction_balance,net_deduction_balance,waiting_amount,waiting_net_add_amount,
  accepted_run_id,accepted_command_id,accepted_module_epoch,snooze_until_date,eligible_from_date,pay_hold_reason
)
with members as (
  select g.group_key parent_group_key,g.candidate_id,g.client_id,g.week_ending_date,g.target_pay_channel,g.certified_zero,
    case when g.group_kind='WORK' then 'WORK' when fc.case_kind='OVERPAYMENT' or i.case_component_id is null then 'CASE' else 'ROW' end presentation_kind,
    case when g.group_kind='WORK' then g.work_id when fc.case_kind='OVERPAYMENT' or i.case_component_id is null then g.case_id else i.case_component_id end presentation_id,
    case when g.group_kind='CASE' and fc.case_kind<>'OVERPAYMENT' then i.case_component_id end case_component_id,
    case when g.group_kind='WORK' then i.client_display_name else cl.name end client_display_name,
    case when g.group_kind='WORK' then 'Timesheet' when fc.case_kind='OVERPAYMENT' then 'Overpayment recovery'
      when cc.case_subtype in ('LOAN','PAYMENT_ADVANCE') then case when cc.direction='DEDUCTION' then 'Payment Advance repayment' else 'Payment Advance' end
      when cc.case_subtype='MANUAL_CREDIT' then 'Manual Credit' when cc.case_subtype='UNDERPAYMENT' then 'Underpayment'
      when cc.case_subtype='MANUAL_DEBT' then 'Manual Debt' else 'Payment needs attention' end payment_label,
    i.id,i.section item_section,i.direction,i.payroll_stage,i.portion_kind,i.member_kind,i.reason_code,i.reason_priority,
    i.is_selected,i.selectable,i.amount_known,i.selected_amount_known,i.timesheet_reference,i.included_source_ex_vat,
    i.gross_due_source_ex_vat,i.nominal_source_ex_vat,i.deduction_balance_source_ex_vat,
    i.active_reserved_source_ex_vat,i.selected_source_ex_vat,i.accepted_run_id,i.accepted_command_id,i.accepted_module_epoch,
    i.snooze_until_date,i.eligible_from_date,i.pay_hold_reason,
    (not i.amount_known and not(g.certified_zero and i.member_kind='GROUP_EVIDENCE')
      or i.is_selected and not i.selected_amount_known) amount_unavailable,
    case when i.is_selected then i.selected_source_ex_vat
      when i.portion_kind='RESERVED' then i.active_reserved_source_ex_vat
      when i.direction='PAYMENT' and i.section='READY' then i.included_source_ex_vat
      when i.direction='PAYMENT' and i.section='WAITING' and i.payroll_stage='GROSS_ADD' then i.gross_due_source_ex_vat
      else i.nominal_source_ex_vat end display_value,
    case when i.is_selected and i.accepted_run_id is not null and i.accepted_command_id is not null and i.accepted_module_epoch is not null
      and exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command cmd on cmd.id=rc.command_id
        join private.bpay_next_pay_run pr on pr.id=rc.run_id where rc.run_id=i.accepted_run_id and rc.command_id=i.accepted_command_id
          and cmd.command_kind='PREPARE' and cmd.module_epoch=i.accepted_module_epoch and pr.selection_state='SEALED'
          and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING'))
      then pg_catalog.jsonb_build_array(i.accepted_run_id::text,i.accepted_command_id::text,i.accepted_module_epoch::text) end accepted_pair
  from private.bpay_next_workbench_group g join private.bpay_next_workbench_item i on i.group_key=g.group_key
  left join private.bpay_next_finance_case fc on fc.id=g.case_id
  left join private.bpay_next_case_component cc on cc.id=i.case_component_id and cc.case_id=g.case_id
  left join public.clients cl on cl.id=g.client_id
  where /*GROUP_FILTER*/true
), scoped as (
  select m.*,sec.section from members m cross join lateral(values(m.item_section),('ALL'::text)) sec(section)
), reduced as (
  select presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,section,client_id,week_ending_date,target_pay_channel,
    min(client_display_name::text) client_display_name,min(payment_label::text) payment_label,min(timesheet_reference::text) timesheet_reference,
    bool_or(direction='DEDUCTION') has_deduction,bool_or(selectable) selectable,
    (array_agg(reason_code order by reason_priority,id))[1] reason_code,min(reason_priority::smallint) reason_priority,
    bool_or(amount_unavailable) amount_unavailable,
    coalesce(sum(case when direction='DEDUCTION' then -display_value when direction='PAYMENT' then display_value else 0 end),0) group_amount,
    coalesce(sum(included_source_ex_vat) filter(where item_section='READY' and direction='PAYMENT' and payroll_stage='NET_ADD'),0) ready_net_add_amount,
    coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='GROSS_DEDUCT'),0) gross_deduction_balance,
    coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='NET_DEDUCT'),0) net_deduction_balance,
    coalesce(sum(gross_due_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='GROSS_ADD' and portion_kind<>'RESERVED'),0) waiting_amount,
    coalesce(sum(nominal_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='NET_ADD' and portion_kind<>'RESERVED'),0) waiting_net_add_amount,
    array_agg(distinct accepted_pair) filter(where accepted_pair is not null) accepted_pairs,
    min(snooze_until_date::date) snooze_until_date,min(eligible_from_date::date) eligible_from_date,min(pay_hold_reason::text) pay_hold_reason
  from scoped group by presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,section,client_id,week_ending_date,target_pay_channel
)
select presentation_kind||':'||presentation_id::text,presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,
  section,client_id,week_ending_date,client_display_name,payment_label,timesheet_reference,
  target_pay_channel,has_deduction,selectable,reason_code,reason_priority,
  case when not amount_unavailable then group_amount end,case when not amount_unavailable then ready_net_add_amount end,
  case when not amount_unavailable then gross_deduction_balance end,case when not amount_unavailable then net_deduction_balance end,
  case when not amount_unavailable then waiting_amount end,case when not amount_unavailable then waiting_net_add_amount end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>0)::uuid end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>1)::uuid end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>2)::bigint end,
  snooze_until_date,eligible_from_date,pay_hold_reason
from reduced;

commit;
