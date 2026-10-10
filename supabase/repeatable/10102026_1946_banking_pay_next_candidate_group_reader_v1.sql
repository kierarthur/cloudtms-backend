-- WB15 native display-sort maintenance and own bounded reader; original financial owners unchanged.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_workbench_refresh_group_display_v1(p_group_key text)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_group_key is null then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_GROUP_INVALID';end if;
  delete from private.bpay_next_workbench_group_display where parent_group_key=p_group_key;
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
  where g.group_key=p_group_key
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

end
$function$;

create or replace function private.bpay_next_workbench_refresh_work_core_v1(p_work_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_member record;v_accepted record;v_totals record;v_candidate uuid;v_client uuid;v_target text;v_financial bigint;v_projection bigint;
  v_group text:='WORK:'||p_work_id::text;v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  v_sunday date;v_sequence bigint;v_reason text;v_priority smallint;v_section text;v_complete boolean;
  v_query_gate jsonb;v_query_scope jsonb;v_query_loaded boolean:=false;v_query_state text:='UNAVAILABLE';
  v_wait_before boolean;v_wait_after boolean;v_case_id uuid;v_withdrawn_complete boolean;v_was_dirty boolean;
  v_positive numeric;v_recovery numeric;v_overhang numeric;v_snooze jsonb;v_count bigint:=0;v_portion text;v_zero boolean;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id for update;
  -- Read stored WAIT before replacing it; the withdrawal owner already changed work.
  select exists(select 1 from private.bpay_next_workbench_item i where i.group_key=v_group
    and i.reason_code='AWAITING_REAUTHORISATION') into v_wait_before;
  v_wait_after:=private.bpay_next_work_wait_state_v1(v_work.id);
  select exists(select 1 from private.bpay_next_workbench_group g where g.group_key=v_group and g.dirty) into v_was_dirty;
  -- Reauthorisation becomes effective only after final position application.
  -- Keep its stored WAIT marker during Processing for the final boundary hook.
  if v_wait_before and v_work.approval_state='APPROVED'
    and v_work.current_revision_id is distinct from v_work.applied_revision_id then
    update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
      dirty_group_count=dirty_group_count+case when v_was_dirty then 0 else 1 end,
      updated_at_utc=transaction_timestamp() where id=1 returning workbench_revision into v_projection;
    update private.bpay_next_workbench_group set complete=false,dirty=true,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    update private.bpay_next_workbench_item set selectable=false,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    return;
  end if;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;
  select r.* into v_revision from private.bpay_next_work_revision r
    where r.id=case when v_work.approval_state='WITHDRAWN' then v_work.applied_revision_id else v_work.current_revision_id end
      and r.work_id=v_work.id;
  v_complete:=v_work.approval_state='APPROVED' and v_revision.id is not null and v_revision.sealed_at_utc is not null
    and v_work.applied_revision_id=v_work.current_revision_id
    and (select count(*) from private.bpay_next_approved_line a where a.revision_id=v_revision.id)=v_revision.expected_line_count;
  v_withdrawn_complete:=v_work.approval_state='WITHDRAWN' and v_revision.id is not null
    and v_revision.sealed_at_utc is not null and not exists(select 1 from private.bpay_next_position p
      where p.work_id=v_work.id and p.applied_revision_id is distinct from v_work.applied_revision_id
        and (p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
          or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
          or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0));
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_group_count=dirty_group_count-case when v_was_dirty then 1 else 0 end,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,work_id,candidate_id,client_id,contract_id,
    physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,
    complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'WORK',v_work.id,v_candidate,v_client,v_work.contract_id,v_revision.physical_timesheet_id,v_work.current_revision_id,
    v_revision.source_pay_channel,v_target,v_work.week_ending_date,coalesce(v_complete,false),v_projection,v_financial,v_metadata.metadata_version,
    case when v_complete then 'CURRENT' when v_work.approval_state='APPROVED' then 'PROCESSING' else 'UNAVAILABLE' end)
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    physical_timesheet_id=excluded.physical_timesheet_id,current_revision_id=excluded.current_revision_id,
    source_pay_channel=excluded.source_pay_channel,target_pay_channel=excluded.target_pay_channel,week_ending_date=excluded.week_ending_date,
    complete=excluded.complete,dirty=false,projection_revision=excluded.projection_revision,financial_view_revision=excluded.financial_view_revision,
    metadata_version=excluded.metadata_version,progress_state=excluded.progress_state,error_code=null,updated_at_utc=transaction_timestamp();
  v_sunday:=v_today+(7-extract(isodow from v_today)::integer);
  select last_sequence into strict v_sequence from private.bpay_next_command_clock where id=1;
  if v_work.approval_state='WITHDRAWN' then
    -- Native retained positions supply A/C/H directly. No zero revision, job,
    -- debt, current residual offer, segment repricing or Case mutation here.
    for v_member in select p.component_key,p.approved_source_ex_vat approved_amount,
      p.realised_source_ex_vat realised_amount,p.held_source_ex_vat held_amount,p.source_basis_channel,
      a.id approved_line_id,a.component_kind,a.tax_treatment,a.work_date,a.approved_quantity,a.unit_label
      from private.bpay_next_position p left join private.bpay_next_approved_line a
        on a.revision_id=v_revision.id and a.component_key=p.component_key
      where p.work_id=v_work.id and v_withdrawn_complete and (p.applied_revision_id=v_work.applied_revision_id
        or p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
        or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
        or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0) order by p.component_key loop
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
      v_item.component_key:=v_member.component_key;v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_member.component_key;
      v_item.portion_kind:='WITHDRAWN';v_item.direction:='INFORMATIONAL';v_item.payroll_stage:='GROSS_ADD';
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_member.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_member.component_kind;v_item.tax_treatment:=v_member.tax_treatment;
      v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
      v_item.source_pay_channel:=v_member.source_basis_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_member.approved_amount;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=v_member.held_amount;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.reason_priority:=case when v_wait_after then 70 else 20 end;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      insert into private.bpay_next_workbench_item select (v_item).*;
    end loop;
    update private.bpay_next_workbench_group set complete=v_count>0,certified_zero=false,
      progress_state=case when v_withdrawn_complete then 'CURRENT' when v_work.applied_revision_id is not null then 'PROCESSING' else 'UNAVAILABLE' end
      where group_key=v_group;
  else
  for v_member in select m.component_key,m.approved_line_id,m.approved_segment_id,m.segment_key,m.component_kind,m.tax_treatment,m.work_date,m.approved_quantity,m.unit_label,m.approved_amount,m.realised_amount,m.held_amount,m.source_pay_channel,m.parent_complete,m.segment_attribution_complete from private.bpay_next_workbench_work_members_v1(v_work.id,v_revision.id) m order by m.component_key,m.segment_key nulls first loop
    v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
    v_item.approved_segment_id:=v_member.approved_segment_id;v_item.segment_key:=v_member.segment_key;
    v_item.component_key:=v_member.component_key;v_item.member_kind:=case when v_member.segment_key is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end;
    v_item.member_key:=coalesce(v_member.segment_key,v_member.component_key);v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;
    v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;v_item.work_date:=v_member.work_date;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;
    v_item.candidate_reference:=v_metadata.tms_ref;v_item.client_display_name:=v_revision.client_display_name;
    v_item.timesheet_reference:=v_revision.timesheet_reference;v_item.component_kind:=v_member.component_kind;
    v_item.tax_treatment:=v_member.tax_treatment;v_item.source_pay_channel:=v_member.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:='PAYMENT';v_item.payroll_stage:='GROSS_ADD';v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
    v_item.amount_known:=coalesce(v_complete,false) and v_member.parent_complete and v_member.segment_attribution_complete;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_complete then 'CURRENT' else 'PROCESSING' end;v_item.updated_at_utc:=transaction_timestamp();
    v_reason:='READY';v_priority:=200;v_section:='READY';
    if v_revision.id is null or v_target is null or v_work.week_ending_date is null
      or extract(isodow from v_work.week_ending_date)<>7 then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif v_work.approval_state<>'APPROVED' then v_reason:='MISSING_APPROVAL';v_priority:=21;v_section:='BLOCKED';
    elsif not v_complete then v_reason:='APPLY_PENDING';v_priority:=30;v_section:='BLOCKED';
    elsif not v_member.parent_complete then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif not v_member.segment_attribution_complete then v_reason:='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE';v_priority:=12;v_section:='BLOCKED';end if;
    if v_item.amount_known then
      v_positive:=greatest(v_member.approved_amount-v_member.realised_amount-v_member.held_amount,0);
      v_recovery:=greatest(v_member.realised_amount-v_member.approved_amount,0);
      v_overhang:=greatest(v_member.held_amount-greatest(v_member.approved_amount-v_member.realised_amount,0),0);
      v_item.nominal_source_ex_vat:=v_positive;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=0;
      v_item.written_off_source_ex_vat:=0;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=v_positive;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=v_recovery;v_item.held_overhang_source_ex_vat:=v_overhang;
    else v_positive:=null;v_recovery:=null;v_overhang:=null;end if;
    select a.run_id,a.command_id,a.module_epoch,a.selected_source_ex_vat into v_accepted from private.bpay_next_workbench_accepted_work_v1(v_work.id,v_member.component_key,v_member.segment_key) a;
    if found then
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.selected_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.selected_source_ex_vat is not null;
      if v_priority>50 then v_reason:='IN_PREPARATION';v_priority:=50;v_section:='WAITING';end if;
    end if;
    v_snooze:=private.bpay_next_snooze_at_cut_v1(v_work.id,v_revision.id,v_member.segment_key,v_member.component_key,v_sequence,v_today);
    if v_snooze is not null then
      v_item.snooze_id:=(v_snooze->>'id')::uuid;v_item.snooze_version:=(v_snooze->>'version')::bigint;
      v_item.snooze_scope:=v_snooze->>'scope_kind';v_item.snooze_until_date:=(v_snooze->>'until_date')::date;
      if v_positive>0 then v_item.excluded_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.work_id=v_work.id and s.active
      and (s.until_date is null or s.until_date>=v_today) and ((s.scope_kind='SEGMENT' and s.segment_key=v_member.segment_key)
        or s.scope_kind='WHOLE_TIMESHEET' or (s.scope_kind='EXPENSE_LINE' and s.component_key=v_member.component_key)
        or (s.scope_kind='EXPENSES_ALL' and v_member.component_kind in ('EXPENSE','MILEAGE')))) then
      v_item.stale_snooze_code:=case when v_member.segment_key is not null and v_member.approved_segment_id is null
        then 'SNOOZE_SEGMENT_STALE' else 'SNOOZE_BASIS_STALE' end;
    end if;
    if v_priority>100 and v_work.week_ending_date>v_sunday then
      v_reason:='FUTURE_WEEK';v_priority:=100;v_section:='WAITING';v_item.eligible_from_date:=v_work.week_ending_date-6;
    elsif v_priority>110 and v_snooze is not null and v_positive>0 then v_reason:='SNOOZED';v_priority:=110;v_section:='WAITING';end if;
    v_item.query_qualified:=coalesce(v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED')
      and v_member.component_kind in ('WORK','PROTECTED_WORK') and v_revision.source_event_id is not null
      and v_revision.physical_timesheet_id is not null and v_item.amount_known and v_item.included_source_ex_vat>0,false);
    if v_item.query_qualified then v_item.query_state:='UNAVAILABLE';end if;
    v_item.section:=v_section;v_item.reason_code:=v_reason;v_item.reason_priority:=v_priority;
    v_item.selectable:=v_section='READY' and v_item.amount_known;v_base:=v_item;
    if not v_item.amount_known or v_positive>0 or (v_member.approved_amount=0 and v_member.realised_amount=0
      and v_member.held_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null) then
      v_item.portion_kind:=case when not v_item.amount_known then 'EVIDENCE' when v_positive=0 then 'ZERO' else 'AVAILABLE' end;
      -- WB-07: query classification belongs only to this new positive portion.
      -- The original pre-query v_base/v_priority remain unchanged for H/R.
      if v_item.query_qualified and v_item.portion_kind='AVAILABLE' then
        if not v_query_loaded then
          v_query_gate:=null;v_query_state:='UNAVAILABLE';
          begin
            v_query_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
          exception when others then
            v_query_gate:=null;
          end;
          v_query_loaded:=true;
          -- Guard object types separately: malformed scalar replies cannot
          -- reach the object-minus-keys operators or escape the unavailable path.
          if jsonb_typeof(v_query_gate)='object' then
            if (v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
                and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
                and v_query_gate->'ok'='true'::jsonb and v_query_gate->>'code'='OK'
                and jsonb_typeof(v_query_gate->'code')='string'
                and jsonb_typeof(v_query_gate->'blocked')='boolean'
                and jsonb_typeof(v_query_gate->'query_state_sha256')='string'
                and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is true then
              v_query_scope:=v_query_gate->'scope';
              if jsonb_typeof(v_query_scope)='object' then
                if (v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
                    'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
                    and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
                      'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
                    and v_query_scope->'root_timesheet_id'=to_jsonb(v_revision.physical_timesheet_id::text)
                    and v_query_scope->'family_booking_id'=to_jsonb(v_work.booking_id)
                    and v_query_scope->'candidate_id'=to_jsonb(v_work.candidate_id::text)
                    and v_query_scope->'client_id'=to_jsonb(v_client::text)
                    and v_query_scope->'contract_id'=to_jsonb(v_work.contract_id::text)
                    and v_query_scope->'week_ending_date'=to_jsonb(v_work.week_ending_date::text)
                    and v_query_scope->'root_version'=to_jsonb(v_revision.physical_timesheet_version::text)
                    and ((v_query_scope->'target_family_id'='null'::jsonb
                        and v_query_scope->'family_bound_version'='null'::jsonb)
                      or (jsonb_typeof(v_query_scope->'target_family_id')='string'
                        and v_query_scope->>'target_family_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        and jsonb_typeof(v_query_scope->'family_bound_version')='string'
                        and v_query_scope->>'family_bound_version' ~ '^[1-9][0-9]{0,18}$'
                        and (char_length(v_query_scope->>'family_bound_version')<19
                          or (v_query_scope->>'family_bound_version') collate "C"<='9223372036854775807' collate "C")))) is true then
                  v_query_state:=case when v_query_gate->'blocked'='true'::jsonb then 'OPEN' else 'CLEAR' end;
                end if;
              end if;
            end if;
          end if;
        end if;
        v_item.query_state:=v_query_state;
        if v_query_state='UNAVAILABLE' and v_item.reason_priority>60 then
          v_item.reason_code:='PAY_QUERY_UNAVAILABLE';v_item.reason_priority:=60;v_item.section:='BLOCKED';
        elsif v_query_state='OPEN' and v_item.reason_priority>61 then
          v_item.reason_code:='PAY_QUERY_OPEN';v_item.reason_priority:=61;v_item.section:='ACTION';
        end if;
        v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    elsif v_recovery>0 then
      v_item.portion_kind:='RECOVERY';v_item.direction:='INFORMATIONAL';v_item.selectable:=false;
      if v_priority>83 then v_item.reason_code:='RECOVERY_MANAGED';v_item.reason_priority:=83;v_item.section:='ACTION';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_item.amount_known and v_member.held_amount>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.active_reserved_source_ex_vat:=v_member.held_amount;v_item.nominal_source_ex_vat:=v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      if v_positive>0 or v_recovery>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_priority>111 then v_item.reason_code:='RESERVED';v_item.reason_priority:=111;v_item.section:='WAITING';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_base.amount_known and (v_positive>0 or v_recovery>0 or v_member.held_amount>0
      or (v_member.approved_amount=0 and v_member.realised_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null)) then
      select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
        sum(i.residual_source_ex_vat) residual,
        sum(i.included_source_ex_vat+i.excluded_source_ex_vat-i.recovery_source_ex_vat-i.held_overhang_source_ex_vat) split_residual
        into v_totals from private.bpay_next_workbench_item i where i.group_key=v_group
          and i.component_key=v_member.component_key and i.member_kind=v_base.member_kind and i.member_key=v_base.member_key;
      if (v_totals.a,v_totals.c,v_totals.h,v_totals.residual,v_totals.split_residual)
        is distinct from (v_member.approved_amount,v_member.realised_amount,v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_NATIVE_MEMBER_NOT_CONSERVED';end if;
    end if;
  end loop;
  end if;
  if v_count=0 then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.work_id:=v_work.id;
    v_item.revision_id:=v_revision.id;v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_work.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;v_item.selected_amount_known:=false;v_item.is_selected:=false;v_item.selectable:=false;
    v_item.section:='BLOCKED';v_item.reason_code:=case when v_work.approval_state<>'APPROVED' then 'MISSING_APPROVAL' else 'DATA_UNAVAILABLE' end;
    if v_work.approval_state='WITHDRAWN' then
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
    end if;
    v_item.reason_priority:=case when v_work.approval_state='WITHDRAWN' then case when v_wait_after then 70 else 20 end
      when v_work.approval_state<>'APPROVED' then 21 else 10 end;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_work.approval_state='WITHDRAWN' and v_work.applied_revision_id is not null
      and not v_withdrawn_complete then 'PROCESSING' else 'UNAVAILABLE' end;
    v_item.updated_at_utc:=transaction_timestamp();v_item.target_pay_channel:=v_target;
    v_zero:=coalesce(v_complete,false) and v_revision.certified_zero and v_revision.approved_source_ex_vat=0
      and v_revision.expected_line_count=0 and v_target is not null and v_work.week_ending_date is not null
      and extract(isodow from v_work.week_ending_date)=7
      and not exists(select 1 from private.bpay_next_position p where p.work_id=v_work.id);
    if v_zero then
      v_item.section:='READY';v_item.reason_code:='CERTIFIED_ZERO';v_item.reason_priority:=200;v_item.progress_state:='CURRENT';
      if v_work.week_ending_date>v_sunday then
        v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_work.week_ending_date-6;
      end if;
    end if;
    insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=g.complete and x.unknown=0,certified_zero=coalesce(v_complete,false) and coalesce(v_revision.certified_zero,false)
    from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,
      count(*) filter(where not amount_known and not (member_kind='GROUP_EVIDENCE' and coalesce(v_zero,false))) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  update private.bpay_next_workbench_group g set stale_snooze_count=x.n,
    stale_snooze_code=case when x.n>0 then 'SNOOZE_SEGMENT_STALE' end
    from(select count(*) n from private.bpay_next_snooze s where s.work_id=v_work.id and s.scope_kind='SEGMENT'
      and s.active and (s.until_date is null or s.until_date>=v_today) and not exists(select 1
        from private.bpay_next_approved_segment a where a.revision_id=v_revision.id and a.segment_key=s.segment_key)) x
    where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;
create or replace function private.bpay_next_workbench_refresh_case_v1(p_case_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_snooze private.bpay_next_snooze%rowtype;v_accepted record;v_totals record;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_group text:='CASE:'||p_case_id::text;v_candidate uuid;v_client uuid;v_contract uuid;v_target text;
  v_financial bigint;v_projection bigint;v_count integer:=0;v_accepted_count integer;v_capacity numeric;
  v_available numeric;v_reserved numeric;v_realised numeric;v_balance numeric;v_due date;v_basis bytea;
  v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select c.candidate_id into strict v_candidate from private.bpay_next_finance_case c where c.id=p_case_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select c.* into strict v_case from private.bpay_next_finance_case c where c.id=p_case_id for update;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select w.contract_id,co.client_id into v_contract,v_client from private.bpay_next_work_collection wc
    join private.bpay_next_work w on w.id=wc.work_id left join public.contracts co on co.id=w.contract_id where wc.case_id=p_case_id;
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,case_id,candidate_id,client_id,contract_id,
    target_pay_channel,complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'CASE',v_case.id,v_candidate,v_client,v_contract,v_target,true,v_projection,v_financial,v_metadata.metadata_version,'CURRENT')
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    target_pay_channel=excluded.target_pay_channel,complete=true,projection_revision=excluded.projection_revision,
    financial_view_revision=excluded.financial_view_revision,metadata_version=excluded.metadata_version,
    progress_state='CURRENT',error_code=null,updated_at_utc=transaction_timestamp();
  for v_component in select c.* from private.bpay_next_case_component c where c.case_id=v_case.id order by c.component_ordinal,c.id loop
    v_count:=v_count+1;
    select r.* into strict v_rule from private.bpay_next_case_rule r where r.id=v_component.rule_id
      and r.case_id=v_case.id and r.candidate_id=v_candidate;
    if v_component.direction='DEDUCTION' then
      v_capacity:=case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat else v_component.approved_source_ex_vat end;
      v_realised:=v_component.recovered_source_ex_vat;v_reserved:=v_component.active_recovery_source_ex_vat;
      v_balance:=v_capacity-v_component.written_off_source_ex_vat-v_realised;
      v_available:=v_balance-v_reserved;
    else
      v_capacity:=v_component.approved_source_ex_vat;
      v_realised:=v_component.funded_source_ex_vat+v_component.paid_credit_source_ex_vat;
      v_reserved:=v_component.active_payout_source_ex_vat;
      v_balance:=v_capacity-v_component.written_off_source_ex_vat-v_realised;
      v_available:=v_balance-v_reserved;
    end if;
    if v_available<0 or v_balance<0 then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CASE_CAPACITY_INCONSISTENT';end if;
    if v_available=0 and v_reserved=0 then continue;end if;
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.case_id:=v_case.id;v_item.case_component_id:=v_component.id;v_item.rule_id:=v_rule.id;
    v_item.member_kind:='CASE_COMPONENT';v_item.member_key:=v_component.id::text;v_item.component_key:=v_component.component_key;
    v_item.client_id:=v_client;v_item.contract_id:=v_contract;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.component_kind:=v_component.case_kind;v_item.tax_treatment:=v_component.tax_treatment;
    v_item.source_pay_channel:=v_component.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:=v_component.direction;v_item.payroll_stage:=v_component.payroll_stage;
    v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_available;v_item.approved_source_ex_vat:=v_capacity;
    v_item.realised_source_ex_vat:=v_realised;v_item.active_reserved_source_ex_vat:=0;
    v_item.written_off_source_ex_vat:=v_component.written_off_source_ex_vat;
    v_item.residual_source_ex_vat:=v_available;v_item.gross_due_source_ex_vat:=case when v_component.direction='PAYMENT' then v_available else 0 end;
    v_item.included_source_ex_vat:=case when v_component.direction='PAYMENT' then v_available else 0 end;
    v_item.excluded_source_ex_vat:=0;v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
    v_item.deduction_balance_source_ex_vat:=case when v_component.direction='DEDUCTION' then v_balance else 0 end;
    v_item.section:='READY';v_item.reason_code:='READY';v_item.reason_priority:=200;v_item.selectable:=true;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
    if v_target is null or v_case.current_rule_id is null then
      v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    end if;
    v_accepted_count:=0;
    for v_accepted in
      select r.id run_id,cmd.id command_id,cmd.module_epoch,ci.nominal_source_ex_vat
      from private.bpay_next_case_selection_item si
      join private.bpay_next_case_selection s on (s.run_id,s.candidate_id,s.selection_revision)=(si.run_id,si.candidate_id,si.selection_revision) and s.status='SEALED'
      join private.bpay_next_pay_run r on r.id=si.run_id and r.selection_state='SEALED' and r.status in ('PREPARING','REVIEW','DRAFT','EXECUTING')
      join private.bpay_next_run_command rc on rc.run_id=r.id join private.bpay_next_command cmd on cmd.id=rc.command_id and cmd.command_kind='PREPARE'
      left join private.bpay_next_run_worker rw on rw.run_id=r.id and rw.candidate_id=si.candidate_id
      left join private.bpay_next_run_case_instruction ci on ci.run_worker_id=rw.id and ci.preparation_revision=rw.preparation_revision
        and ci.case_component_id=si.case_component_id
      where si.case_component_id=v_component.id and si.is_selected
        and ((rw.id is not null and rw.status in ('PREPARING','REVIEW','READY','DRAFT','CANCELLING','ISSUED')
          and si.selection_revision=case when rw.case_selection_revision>0 then rw.case_selection_revision
            else (select max(cs.selection_revision::numeric)::bigint from private.bpay_next_case_selection cs
              where cs.run_id=r.id and cs.candidate_id=si.candidate_id and cs.status='SEALED') end)
          or (rw.id is null and si.selection_revision=(select max(cs.selection_revision::numeric)::bigint from private.bpay_next_case_selection cs
            where cs.run_id=r.id and cs.candidate_id=si.candidate_id and cs.status='SEALED')))
    loop
      v_accepted_count:=v_accepted_count+1;
      if v_accepted_count>1 then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_ACCEPTED_CASE_COLLISION';end if;
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.nominal_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.nominal_source_ex_vat is not null;
      if v_item.reason_priority>50 then v_item.section:='WAITING';v_item.reason_code:='IN_PREPARATION';v_item.reason_priority:=50;end if;
    end loop;
    if v_item.reason_priority>82 and v_component.resolution_state='REVIEW' then
      v_item.section:='ACTION';v_item.reason_code:='CASE_REVIEW_REQUIRED';v_item.reason_priority:=82;
    elsif v_item.reason_priority>90 and v_case.status='PAUSED' then
      v_item.section:='WAITING';v_item.reason_code:='CASE_PAUSED';v_item.reason_priority:=90;
    elsif v_case.status in ('CLOSED','WRITTEN_OFF') and v_available>0 then
      v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    end if;
    v_due:=case when v_component.direction='DEDUCTION' then coalesce(v_rule.next_due_monday,v_case.due_date) else v_case.due_date end;
    if v_item.reason_priority>100 and v_due>v_today then
      v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_due;
    end if;
    v_basis:=sha256(convert_to(jsonb_build_array(v_candidate,v_component.case_id,v_component.id,
      v_component.case_kind,v_component.case_subtype,v_component.tax_treatment,v_component.instruction_kind,v_component.direction,
      v_component.payroll_stage,v_component.source_pay_channel,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,
      v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,v_rule.weekly_due_source_ex_vat,
      v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)::text,'UTF8'));
    select s.* into v_snooze from private.bpay_next_snooze s where s.case_component_id=v_component.id and s.scope_kind='CASE'
      and s.active and (s.until_date is null or s.until_date>=v_today) and s.worker_pay_basis=v_basis;
    if found then
      v_item.snooze_id:=v_snooze.id;v_item.snooze_version:=v_snooze.version;v_item.snooze_scope:=v_snooze.scope_kind;v_item.snooze_until_date:=v_snooze.until_date;
      if v_item.reason_priority>110 then v_item.section:='WAITING';v_item.reason_code:='SNOOZED';v_item.reason_priority:=110;end if;
      if v_component.direction='PAYMENT' then v_item.excluded_source_ex_vat:=v_item.included_source_ex_vat;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.case_component_id=v_component.id and s.scope_kind='CASE'
      and s.active and (s.until_date is null or s.until_date>=v_today)) then v_item.stale_snooze_code:='SNOOZE_BASIS_STALE';end if;
    v_item.selectable:=v_item.section='READY';v_base:=v_item;
    if v_available>0 then v_item.portion_kind:='AVAILABLE';insert into private.bpay_next_workbench_item select (v_item).*;end if;
    if v_reserved>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.nominal_source_ex_vat:=v_reserved;v_item.active_reserved_source_ex_vat:=v_reserved;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;
      if v_available>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;v_item.deduction_balance_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_item.reason_priority>111 then v_item.section:='WAITING';v_item.reason_code:='RESERVED';v_item.reason_priority:=111;end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
      sum(i.written_off_source_ex_vat) w,sum(i.residual_source_ex_vat) residual,
      sum(i.deduction_balance_source_ex_vat) debt into v_totals from private.bpay_next_workbench_item i
      where i.group_key=v_group and i.case_component_id=v_component.id;
    if (v_totals.a,v_totals.c,v_totals.h,v_totals.w,v_totals.residual,v_totals.debt)
      is distinct from (v_capacity,v_realised,v_reserved,v_component.written_off_source_ex_vat,v_available,
        case when v_component.direction='DEDUCTION' then v_balance else 0::numeric end) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CASE_MEMBER_NOT_CONSERVED';end if;
  end loop;
  if v_count=0 and v_case.status in ('OPEN','PAUSED') then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.case_id:=v_case.id;
    v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_case.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_contract;v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selectable:=false;v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    v_item.target_pay_channel:=v_target;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:='UNAVAILABLE';v_item.updated_at_utc:=transaction_timestamp();insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=x.unknown=0 from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,count(*) filter(where not amount_known) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;
create or replace function private.bpay_next_workbench_kind_page_v1(
  p_kind text,p_scope jsonb,p_after jsonb,p_limit integer
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare v_sql text;v_where text;v_order text;v_tuple text;v_seek text;v_key text;v_projection text;
  v_row record;v_rows jsonb:='[]'::jsonb;v_last jsonb;v_n integer:=0;v_complete boolean:=true;v_member boolean;v_affected bigint;v_anchor_page boolean:=false;
  v_client_scope text:=coalesce(p_scope->>'client_id','ALL');v_channel text:=p_scope->>'channel';
  v_section text:=p_scope->>'section';v_candidate uuid:=(p_scope->>'candidate_id')::uuid;
  v_client uuid:=(p_scope->>'client_id')::uuid;v_search text:=p_scope->>'search';
  v_group text:=p_scope->>'group_key';v_item uuid:=(p_scope->>'item_id')::uuid;v_reason text:=p_scope->>'reason_code';
  v_sort text:=p_scope->>'sort';v_desc boolean;v_op text;
  v_candidate_count bigint;v_members bigint;v_selected_candidates bigint;v_selected_members bigint;
  v_count_selection jsonb:=coalesce(p_scope->'count_selection','null'::jsonb);
begin
  if p_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS') or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_desc:=v_sort like '%_DESC';v_op:=case when v_desc then '<' else '>' end;
  if p_kind='HEADER' then
    -- Exact natural summary lookup; missing CURRENT scope has no contributions.
    select pg_catalog.jsonb_build_object('candidate_count',coalesce(s.candidate_count,0)::text,'item_count',coalesce(s.item_count,0)::text,'contribution_count',coalesce(s.contribution_count,0)::text,'segment_count',coalesce(s.segment_count,0)::text,'payable_count',coalesce(s.payable_count,0)::text,'selected_count',coalesce(s.selected_count,0)::text,'snoozed_count',coalesce(s.snoozed_count,0)::text,'held_count',coalesce(s.held_count,0)::text,'queried_count',coalesce(s.queried_count,0)::text,'blocked_count',coalesce(s.blocked_count,0)::text,'deduction_item_count',coalesce(s.deduction_item_count,0)::text,'unknown_amount_count',coalesce(s.unknown_amount_count,0)::text,'unknown_selected_amount_count',coalesce(s.unknown_selected_amount_count,0)::text,'amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.amount,0)::text else null end,'waiting_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_amount,0)::text else null end,'ready_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.ready_net_add_amount,0)::text else null end,'waiting_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_net_add_amount,0)::text else null end,'gross_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.gross_deduction_balance,0)::text else null end,'net_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.net_deduction_balance,0)::text else null end,'approved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.approved_amount,0)::text else null end,'paid_credit_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.paid_credit_amount,0)::text else null end,'reserved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.reserved_amount,0)::text else null end,'excluded_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.excluded_amount,0)::text else null end,'recovery_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.recovery_amount,0)::text else null end,'selected_amount',case when coalesce(s.unknown_selected_amount_count,0)=0 then coalesce(s.selected_amount,0)::text else null end,'has_deduction',coalesce(s.deduction_item_count,0)>0) into v_rows
      from (values(1)) anchor(n) left join private.bpay_next_workbench_scope_summary s
        on s.client_scope=v_client_scope and s.pay_channel=v_channel and s.section=v_section;
    if v_candidate is not null then
      select pg_catalog.jsonb_build_object('candidate_count',case when s.candidate_id is null then '0' else '1' end,'item_count',coalesce(s.item_count,0)::text,'contribution_count',coalesce(s.contribution_count,0)::text,'segment_count',coalesce(s.segment_count,0)::text,'payable_count',coalesce(s.payable_count,0)::text,'selected_count',coalesce(s.selected_count,0)::text,'snoozed_count',coalesce(s.snoozed_count,0)::text,'held_count',coalesce(s.held_count,0)::text,'queried_count',coalesce(s.queried_count,0)::text,'blocked_count',coalesce(s.blocked_count,0)::text,'deduction_item_count',coalesce(s.deduction_item_count,0)::text,'unknown_amount_count',coalesce(s.unknown_amount_count,0)::text,'unknown_selected_amount_count',coalesce(s.unknown_selected_amount_count,0)::text,'amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.amount,0)::text else null end,'waiting_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_amount,0)::text else null end,'ready_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.ready_net_add_amount,0)::text else null end,'waiting_net_add_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.waiting_net_add_amount,0)::text else null end,'gross_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.gross_deduction_balance,0)::text else null end,'net_deduction_balance',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.net_deduction_balance,0)::text else null end,'approved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.approved_amount,0)::text else null end,'paid_credit_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.paid_credit_amount,0)::text else null end,'reserved_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.reserved_amount,0)::text else null end,'excluded_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.excluded_amount,0)::text else null end,'recovery_amount',case when coalesce(s.unknown_amount_count,0)=0 then coalesce(s.recovery_amount,0)::text else null end,'selected_amount',case when coalesce(s.unknown_selected_amount_count,0)=0 then coalesce(s.selected_amount,0)::text else null end,'has_deduction',coalesce(s.deduction_item_count,0)>0) into v_rows
        from (values(1)) anchor(n) left join private.bpay_next_workbench_candidate_summary s
          on s.candidate_id=v_candidate and s.client_scope=v_client_scope and s.pay_channel=v_channel and s.section=v_section;
    end if;
    -- Owner E6: deductions-only remains visible, but does not enter Create counts.
    -- Native ALL/client/channel summary universe; no work/publisher/money reads.
    if v_section='READY' then
      select count(*),coalesce(sum(s.payable_count+s.deduction_item_count),0)::bigint,
        count(*) filter(where v_count_selection<>'null'::jsonb and
          ((v_count_selection->>'mode'='ALL') <> ((v_count_selection->'candidate_ids') ? s.candidate_id::text))),
        coalesce(sum(s.payable_count+s.deduction_item_count) filter(where v_count_selection<>'null'::jsonb and
          ((v_count_selection->>'mode'='ALL') <> ((v_count_selection->'candidate_ids') ? s.candidate_id::text))),0)::bigint
      into v_candidate_count,v_members,v_selected_candidates,v_selected_members
      from private.bpay_next_workbench_candidate_summary s
      where s.section='READY' and s.client_scope=v_client_scope and s.pay_channel=v_channel
        and s.payable_count>0 and (v_candidate is null or s.candidate_id=v_candidate);
      v_rows:=v_rows||jsonb_build_object('candidate_count',v_candidate_count::text);
    end if;
    v_rows:=v_rows||jsonb_build_object('selectable_member_count',v_members::text,
      'selected_candidate_count',case when v_count_selection<>'null'::jsonb then v_selected_candidates::text else null end,
      'selected_member_count',case when v_count_selection<>'null'::jsonb then v_selected_members::text else null end);
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_rows),'complete',true,'last_key',null);
  elsif p_kind in ('CANDIDATES','PICK_CANDIDATES') then
    v_where:=$query$s.client_scope=$1 and s.pay_channel=$2 and s.section=$3$query$;
    if p_kind='CANDIDATES' and v_section='READY' then
      v_where:=v_where||' and (s.payable_count>0 or s.deduction_item_count>0)';end if;
    if v_candidate is not null then v_where:=v_where||' and s.candidate_id=$4';end if;
    if v_search<>'' then v_where:=v_where||' and (s.candidate_sort_name like $6 escape ''!'' or s.tms_ref like $6 escape ''!'')';end if;
    v_projection:=case p_kind when 'CANDIDATES' then $query$pg_catalog.jsonb_build_object('candidate_id',s.candidate_id,
      'candidate_sort_name',s.candidate_sort_name,
      'tms_ref',s.tms_ref,
      'bank_warning_code',s.bank_warning_code,
      'metadata_version',(s.metadata_version)::text,
      'projection_revision',(s.projection_revision)::text,
      'item_count',(s.item_count)::text,
      'contribution_count',(s.contribution_count)::text,
      'segment_count',(s.segment_count)::text,
      'payable_count',(s.payable_count)::text,
      'selected_count',(s.selected_count)::text,
      'snoozed_count',(s.snoozed_count)::text,
      'held_count',(s.held_count)::text,
      'queried_count',(s.queried_count)::text,
      'blocked_count',(s.blocked_count)::text,
      'deduction_item_count',(s.deduction_item_count)::text,
      'unknown_amount_count',(s.unknown_amount_count)::text,
      'unknown_selected_amount_count',(s.unknown_selected_amount_count)::text,
      'amount',(case when s.unknown_amount_count=0 then s.amount else null end)::text,
      'waiting_amount',(case when s.unknown_amount_count=0 then s.waiting_amount else null end)::text,
      'ready_net_add_amount',(case when s.unknown_amount_count=0 then s.ready_net_add_amount else null end)::text,
      'waiting_net_add_amount',(case when s.unknown_amount_count=0 then s.waiting_net_add_amount else null end)::text,
      'gross_deduction_balance',(case when s.unknown_amount_count=0 then s.gross_deduction_balance else null end)::text,
      'net_deduction_balance',(case when s.unknown_amount_count=0 then s.net_deduction_balance else null end)::text,
      'approved_amount',(case when s.unknown_amount_count=0 then s.approved_amount else null end)::text,
      'paid_credit_amount',(case when s.unknown_amount_count=0 then s.paid_credit_amount else null end)::text,
      'reserved_amount',(case when s.unknown_amount_count=0 then s.reserved_amount else null end)::text,
      'excluded_amount',(case when s.unknown_amount_count=0 then s.excluded_amount else null end)::text,
      'recovery_amount',(case when s.unknown_amount_count=0 then s.recovery_amount else null end)::text,
      'selected_amount',(case when s.unknown_selected_amount_count=0 then s.selected_amount else null end)::text,
      'has_deduction',s.deduction_item_count>0)$query$ else $query$pg_catalog.jsonb_build_object('candidate_id',s.candidate_id,
      'candidate_sort_name',s.candidate_sort_name,
      'tms_ref',s.tms_ref)$query$ end;
    if v_sort like 'AMOUNT_%' then
      v_tuple:='((s.unknown_amount_count>0),(case when s.unknown_amount_count=0 then s.amount else 0 end),s.candidate_id)';
      v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2)::uuid)';v_key:='pg_catalog.jsonb_build_array(s.unknown_amount_count>0,(case when s.unknown_amount_count=0 then s.amount else 0 end)::text,s.candidate_id)';
    elsif v_sort like 'STATUS_%' then
      v_tuple:='((s.payable_count=0),(s.bank_warning_code is null),coalesce(s.bank_warning_code,'''') collate "C",s.candidate_sort_name collate "C",s.candidate_id)';
      v_seek:='(($10->>0)::boolean,($10->>1)::boolean,($10->>2) collate "C",($10->>3) collate "C",($10->>4)::uuid)';
      v_key:='pg_catalog.jsonb_build_array(s.payable_count=0,s.bank_warning_code is null,coalesce(s.bank_warning_code,''''),s.candidate_sort_name,s.candidate_id)';
    elsif v_sort like 'DEDUCTIONS_%' then
      v_tuple:='((s.deduction_item_count>0),s.candidate_sort_name collate "C",(s.tms_ref is null),coalesce(s.tms_ref,'''') collate "C",s.candidate_id)';
      v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::boolean,($10->>3) collate "C",($10->>4)::uuid)';v_key:='pg_catalog.jsonb_build_array(s.deduction_item_count>0,s.candidate_sort_name,s.tms_ref is null,coalesce(s.tms_ref,''''),s.candidate_id)';
    else
      v_tuple:='(s.candidate_sort_name collate "C",(s.tms_ref is null),coalesce(s.tms_ref,'''') collate "C",s.candidate_id)';
      v_seek:='(($10->>0) collate "C",($10->>1)::boolean,($10->>2) collate "C",($10->>3)::uuid)';v_key:='pg_catalog.jsonb_build_array(s.candidate_sort_name,s.tms_ref is null,coalesce(s.tms_ref,''''),s.candidate_id)';end if;
    v_sql:=' from private.bpay_next_workbench_candidate_summary s where ';
  elsif p_kind='PICK_CLIENTS' then
    v_where:='s.client_id is not null and s.pay_channel=$2 and s.section=$3 and s.item_count>0';
    if v_client is not null then v_where:=v_where||' and s.client_id=$5';end if;
    if v_candidate is not null then v_where:=v_where||' and exists(select 1 from private.bpay_next_workbench_candidate_summary cs where cs.candidate_id=$4 and cs.client_scope=s.client_scope and cs.pay_channel=$2 and cs.section=$3 and cs.item_count>0)';end if;
    if v_search<>'' then v_where:=v_where||' and exists(select 1 from public.clients c where c.id=s.client_id and c.name like $6 escape ''!'')';end if;
    -- Client IDs seed the exact maintained scope before one factual name lookup.
    v_projection:=$query$pg_catalog.jsonb_build_object('client_id',s.client_id,
      'client_display_name',c.name)$query$;
    v_tuple:='(s.client_scope collate "C")';v_seek:='(($10->>0) collate "C")';v_key:='pg_catalog.jsonb_build_array(s.client_scope)';
    v_sql:=' from private.bpay_next_workbench_scope_summary s where ';
  elsif p_kind='CANDIDATE_GROUPS' then
    v_where:='s.candidate_id=$4 and s.section=$3';
    if v_client is not null then v_where:=v_where||' and s.client_id=$5';end if;
    if v_channel<>'ALL' then v_where:=v_where||' and s.target_pay_channel=$2';end if;
    if v_section='READY' then v_where:=v_where||' and g.complete and not g.dirty and g.progress_state=''CURRENT'' and s.selectable';end if;
    v_projection:=$query$pg_catalog.jsonb_build_object('group_key',s.presentation_key,
      'group_kind',s.presentation_kind,
      'candidate_id',g.candidate_id,
      'work_id',g.work_id,
      'case_id',g.case_id,
      'client_id',g.client_id,
      'contract_id',g.contract_id,
      'physical_timesheet_id',g.physical_timesheet_id,
      'current_revision_id',g.current_revision_id,
      'source_pay_channel',g.source_pay_channel,
      'target_pay_channel',g.target_pay_channel,
      'week_ending_date',g.week_ending_date,
      'complete',g.complete,
      'dirty',g.dirty,
      'certified_zero',g.certified_zero,
      'ready_count',(g.ready_count)::text,
      'action_count',(g.action_count)::text,
      'blocked_count',(g.blocked_count)::text,
      'waiting_count',(g.waiting_count)::text,
      'stale_snooze_count',(g.stale_snooze_count)::text,
      'financial_view_revision',(g.financial_view_revision)::text,
      'projection_revision',(g.projection_revision)::text,
      'metadata_version',(g.metadata_version)::text,
      'stale_snooze_code',g.stale_snooze_code,
      'error_code',g.error_code,
      'progress_state',g.progress_state,
      'client_display_name',s.client_display_name,
      'bank_warning_code',m.bank_warning_code,
      'section',$3,
      'reason_code',s.reason_code,
      'has_deduction',s.has_deduction,
      'selectable',g.complete and not g.dirty and g.progress_state='CURRENT' and s.selectable,
      'group_amount',s.group_amount::text,
      'ready_net_add_amount',s.ready_net_add_amount::text,
      'gross_deduction_balance',s.gross_deduction_balance::text,
      'net_deduction_balance',s.net_deduction_balance::text,
      'presentation_id',s.presentation_id,'parent_group_key',s.parent_group_key,'case_component_id',s.case_component_id,
      'payment_label',s.payment_label,'timesheet_reference',s.timesheet_reference,
      'waiting_amount',s.waiting_amount::text,'waiting_net_add_amount',s.waiting_net_add_amount::text,
      'accepted_run_id',s.accepted_run_id,'accepted_command_id',s.accepted_command_id,'accepted_module_epoch',s.accepted_module_epoch::text,
      'snooze_until_date',s.snooze_until_date,'eligible_from_date',s.eligible_from_date,'pay_hold_reason',s.pay_hold_reason,
      'reason_priority',s.reason_priority)$query$;
    if v_sort like 'PAYMENT_%' or v_sort='NAME_ASC' then
      v_tuple:='((s.week_ending_date is null),coalesce(s.week_ending_date,date ''0001-01-01''),s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,coalesce(nullif($10->>1,'''')::date,date ''0001-01-01''),($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.week_ending_date is null,coalesce(s.week_ending_date::text,''''),s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'CLIENT_%' then
      v_tuple:='((s.client_display_name is null),coalesce(s.client_display_name,'''') collate "C",s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.client_display_name is null,coalesce(s.client_display_name,''''),s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'CHANNEL_%' then
      v_tuple:='((s.target_pay_channel is null),coalesce(s.target_pay_channel,'''') collate "C",s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.target_pay_channel is null,coalesce(s.target_pay_channel,''''),s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'DEDUCTIONS_%' then
      v_tuple:='(s.has_deduction,s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::uuid)';v_key:='jsonb_build_array(s.has_deduction,s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'AMOUNT_%' then
      v_tuple:='((s.group_amount is null),coalesce(s.group_amount,0),s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.group_amount is null,coalesce(s.group_amount,0)::text,s.presentation_kind,s.presentation_id)';
    elsif v_sort like 'REASON_%' then
      v_tuple:='(s.reason_priority,s.reason_code collate "C",s.presentation_kind collate "C",s.presentation_id)';v_seek:='(($10->>0)::smallint,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid)';v_key:='jsonb_build_array(s.reason_priority,s.reason_code,s.presentation_kind,s.presentation_id)';
    else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    v_sql:=' from private.bpay_next_workbench_group_display s join private.bpay_next_workbench_group g on g.group_key=s.parent_group_key and g.candidate_id=s.candidate_id where ';
  else
    v_where:='true';
    if v_candidate is not null then v_where:=v_where||' and i.candidate_id=$4';end if;
    if v_client is not null then v_where:=v_where||' and i.client_id=$5';end if;
    if v_channel<>'ALL' then v_where:=v_where||' and i.target_pay_channel=$2';end if;
    if v_section<>'ALL' then v_where:=v_where||' and i.section=$3';end if;
    if v_group is not null then
      if p_kind='GROUP_LINES' and v_group like 'ROW:%' then
        v_where:=v_where||' and i.case_component_id=substring($7 from 5)::uuid and exists(select 1 from private.bpay_next_workbench_group_display d where d.presentation_key=$7 and d.parent_group_key=i.group_key and d.candidate_id=i.candidate_id)';
      else v_where:=v_where||' and i.group_key=$7';end if;
    end if;
    if v_item is not null then v_where:=v_where||' and i.id=$8';end if;
    if v_reason is not null then v_where:=v_where||' and i.reason_code=$9';end if;
    if v_search<>'' then v_where:=v_where||' and (i.candidate_display_name like $6 escape ''!'' or i.candidate_reference like $6 escape ''!'' or i.timesheet_reference like $6 escape ''!'')';end if;
    v_projection:=case p_kind when 'ACTION_REQUIRED' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'client_id',i.client_id,
      'physical_timesheet_id',i.physical_timesheet_id,
      'revision_id',i.revision_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'timesheet_reference',i.timesheet_reference,
      'week_ending_date',i.week_ending_date,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'section',i.section,
      'query_state',i.query_state,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'pay_hold_reason',i.pay_hold_reason,
      'eligible_from_date',i.eligible_from_date,
      'snooze_id',i.snooze_id,
      'snooze_version',(i.snooze_version)::text,
      'snooze_until_date',i.snooze_until_date,
      'stale_snooze_code',i.stale_snooze_code,
      'progress_state',i.progress_state,
      'error_code',i.error_code,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text)||pg_catalog.jsonb_build_object(
      'direction',i.direction,'payroll_stage',i.payroll_stage,
      'approved_source_ex_vat',i.approved_source_ex_vat::text,
      'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat::text,
      'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat::text,
      'recovery_source_ex_vat',i.recovery_source_ex_vat::text,
      'included_source_ex_vat',i.included_source_ex_vat::text,
      'gross_due_source_ex_vat',i.gross_due_source_ex_vat::text)$query$ when 'BLOCKED' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'client_id',i.client_id,
      'physical_timesheet_id',i.physical_timesheet_id,
      'revision_id',i.revision_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'timesheet_reference',i.timesheet_reference,
      'week_ending_date',i.week_ending_date,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'section',i.section,
      'query_state',i.query_state,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'pay_hold_reason',i.pay_hold_reason,
      'eligible_from_date',i.eligible_from_date,
      'snooze_id',i.snooze_id,
      'snooze_version',(i.snooze_version)::text,
      'snooze_until_date',i.snooze_until_date,
      'stale_snooze_code',i.stale_snooze_code,
      'progress_state',i.progress_state,
      'error_code',i.error_code,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text)||pg_catalog.jsonb_build_object(
      'direction',i.direction,'payroll_stage',i.payroll_stage,
      'approved_source_ex_vat',i.approved_source_ex_vat::text,
      'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat::text,
      'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat::text,
      'recovery_source_ex_vat',i.recovery_source_ex_vat::text,
      'included_source_ex_vat',i.included_source_ex_vat::text,
      'gross_due_source_ex_vat',i.gross_due_source_ex_vat::text)$query$ when 'ISSUE_DETAIL' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'client_id',i.client_id,
      'physical_timesheet_id',i.physical_timesheet_id,
      'revision_id',i.revision_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'timesheet_reference',i.timesheet_reference,
      'week_ending_date',i.week_ending_date,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'section',i.section,
      'query_state',i.query_state,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'pay_hold_reason',i.pay_hold_reason,
      'eligible_from_date',i.eligible_from_date,
      'snooze_id',i.snooze_id,
      'snooze_version',(i.snooze_version)::text,
      'snooze_until_date',i.snooze_until_date,
      'stale_snooze_code',i.stale_snooze_code,
      'progress_state',i.progress_state,
      'error_code',i.error_code,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text)||pg_catalog.jsonb_build_object(
      'direction',i.direction,'payroll_stage',i.payroll_stage,
      'approved_source_ex_vat',i.approved_source_ex_vat::text,
      'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat::text,
      'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat::text,
      'recovery_source_ex_vat',i.recovery_source_ex_vat::text,
      'included_source_ex_vat',i.included_source_ex_vat::text,
      'gross_due_source_ex_vat',i.gross_due_source_ex_vat::text)$query$ when 'GROUP_LINES' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',coalesce($7,i.group_key),
      'component_key',i.component_key,
      'member_key',i.member_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'revision_id',i.revision_id,
      'approved_line_id',i.approved_line_id,
      'approved_segment_id',i.approved_segment_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'rule_id',i.rule_id,
      'client_id',i.client_id,
      'contract_id',i.contract_id,
      'physical_timesheet_id',i.physical_timesheet_id,
      'snooze_id',i.snooze_id,
      'accepted_run_id',i.accepted_run_id,
      'accepted_command_id',i.accepted_command_id,
      'segment_key',i.segment_key,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'client_display_name',i.client_display_name,
      'timesheet_reference',i.timesheet_reference,
      'component_kind',i.component_kind,
      'tax_treatment',i.tax_treatment,
      'unit_label',i.unit_label,
      'pay_hold_reason',i.pay_hold_reason,
      'channel_note_code',i.channel_note_code,
      'snooze_scope',i.snooze_scope,
      'stale_snooze_code',i.stale_snooze_code,
      'error_code',i.error_code,
      'member_kind',i.member_kind,
      'portion_kind',i.portion_kind,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'direction',i.direction,
      'payroll_stage',i.payroll_stage,
      'work_date',i.work_date,
      'week_ending_date',i.week_ending_date,
      'eligible_from_date',i.eligible_from_date)||pg_catalog.jsonb_build_object('snooze_until_date',i.snooze_until_date,
      'approved_quantity',(i.approved_quantity)::text,
      'amount_known',i.amount_known,
      'selected_amount_known',i.selected_amount_known,
      'selectable',i.selectable,
      'is_selected',i.is_selected,
      'query_qualified',i.query_qualified,
      'pay_hold',i.pay_hold,
      'section',i.section,
      'reason_code',i.reason_code,
      'reason_priority',i.reason_priority,
      'query_state',i.query_state,
      'progress_state',i.progress_state,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text,
      'metadata_version',(i.metadata_version)::text,
      'snooze_version',(i.snooze_version)::text,
      'accepted_module_epoch',(i.accepted_module_epoch)::text,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'approved_source_ex_vat',(i.approved_source_ex_vat)::text,
      'realised_source_ex_vat',(i.realised_source_ex_vat)::text,
      'active_reserved_source_ex_vat',(i.active_reserved_source_ex_vat)::text,
      'residual_source_ex_vat',(i.residual_source_ex_vat)::text,
      'gross_due_source_ex_vat',(i.gross_due_source_ex_vat)::text,
      'included_source_ex_vat',(i.included_source_ex_vat)::text,
      'excluded_source_ex_vat',(i.excluded_source_ex_vat)::text,
      'recovery_source_ex_vat',(i.recovery_source_ex_vat)::text,
      'held_overhang_source_ex_vat',(i.held_overhang_source_ex_vat)::text,
      'deduction_balance_source_ex_vat',(i.deduction_balance_source_ex_vat)::text,
      'selected_source_ex_vat',(i.selected_source_ex_vat)::text,
      'written_off_source_ex_vat',(i.written_off_source_ex_vat)::text)$query$ when 'EXPORT_ROWS' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
      'group_key',i.group_key,
      'candidate_id',i.candidate_id,
      'work_id',i.work_id,
      'case_id',i.case_id,
      'case_component_id',i.case_component_id,
      'revision_id',i.revision_id,
      'client_id',i.client_id,
      'component_key',i.component_key,
      'member_kind',i.member_kind,
      'member_key',i.member_key,
      'portion_kind',i.portion_kind,
      'candidate_display_name',i.candidate_display_name,
      'candidate_reference',i.candidate_reference,
      'client_display_name',i.client_display_name,
      'timesheet_reference',i.timesheet_reference,
      'work_date',i.work_date,
      'week_ending_date',i.week_ending_date,
      'component_kind',i.component_kind,
      'tax_treatment',i.tax_treatment,
      'source_pay_channel',i.source_pay_channel,
      'target_pay_channel',i.target_pay_channel,
      'direction',i.direction,
      'payroll_stage',i.payroll_stage,
      'approved_quantity',(i.approved_quantity)::text,
      'unit_label',i.unit_label,
      'nominal_source_ex_vat',(i.nominal_source_ex_vat)::text,
      'approved_source_ex_vat',(i.approved_source_ex_vat)::text,
      'realised_source_ex_vat',(i.realised_source_ex_vat)::text,
      'active_reserved_source_ex_vat',(i.active_reserved_source_ex_vat)::text,
      'residual_source_ex_vat',(i.residual_source_ex_vat)::text,
      'included_source_ex_vat',(i.included_source_ex_vat)::text,
      'excluded_source_ex_vat',(i.excluded_source_ex_vat)::text,
      'deduction_balance_source_ex_vat',(i.deduction_balance_source_ex_vat)::text,
      'amount_known',i.amount_known,
      'section',i.section,
      'reason_code',i.reason_code,
      'selectable',i.selectable,
      'financial_view_revision',(i.financial_view_revision)::text,
      'projection_revision',(i.projection_revision)::text)||pg_catalog.jsonb_build_object('metadata_version',(i.metadata_version)::text)$query$ end;
    -- Non-null native UUID closes every issue sort including CASE portions.
    if p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then
      if v_sort like 'PAYMENT_%' then
        v_tuple:='(coalesce(i.timesheet_reference,'''') collate "C",i.id)';
        v_seek:='(($10->>0) collate "C",($10->>1)::uuid)';v_key:='jsonb_build_array(coalesce(i.timesheet_reference,''''),i.id)';
      elsif v_sort like 'REASON_%' then
        v_tuple:='(i.reason_priority,i.reason_code collate "C",i.id)';
        v_seek:='(($10->>0)::smallint,($10->>1) collate "C",($10->>2)::uuid)';
        v_key:='jsonb_build_array(i.reason_priority,i.reason_code,i.id)';
      elsif v_sort like 'AMOUNT_%' then
        v_tuple:='((i.nominal_source_ex_vat is null),coalesce(i.nominal_source_ex_vat,0),i.id)';
        v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2)::uuid)';v_key:='jsonb_build_array(i.nominal_source_ex_vat is null,coalesce(i.nominal_source_ex_vat,0)::text,i.id)';
      else
        v_tuple:='(coalesce(i.candidate_display_name,'''') collate "C",i.candidate_id,i.id)';
        v_seek:='(($10->>0) collate "C",($10->>1)::uuid,($10->>2)::uuid)';v_key:='jsonb_build_array(coalesce(i.candidate_display_name,''''),i.candidate_id,i.id)';end if;
    else v_tuple:='(i.id)';v_seek:='(($10->>0)::uuid)';v_key:='pg_catalog.jsonb_build_array(i.id)';end if;
    v_sql:=' from private.bpay_next_workbench_item i where ';
  end if;
  -- Explicit scalar ORDER BY; never ORDER BY a composite row (which prevents
  -- ordinary multicolumn index use). Commas inside COALESCE stay intact.
  v_order:=case
    when p_kind in ('CANDIDATES','PICK_CANDIDATES') and v_sort like 'AMOUNT_%' then '(s.unknown_amount_count>0) @,(case when s.unknown_amount_count=0 then s.amount else 0 end) @,s.candidate_id @'
    when p_kind='CANDIDATES' and v_sort like 'STATUS_%' then '(s.payable_count=0) @,(s.bank_warning_code is null) @,coalesce(s.bank_warning_code,'''') collate "C" @,s.candidate_sort_name collate "C" @,s.candidate_id @'
    when p_kind in ('CANDIDATES','PICK_CANDIDATES') and v_sort like 'DEDUCTIONS_%' then '(s.deduction_item_count>0) @,s.candidate_sort_name collate "C" @,(s.tms_ref is null) @,coalesce(s.tms_ref,'''') collate "C" @,s.candidate_id @'
    when p_kind in ('CANDIDATES','PICK_CANDIDATES') then 's.candidate_sort_name collate "C" @,(s.tms_ref is null) @,coalesce(s.tms_ref,'''') collate "C" @,s.candidate_id @'
    when p_kind='PICK_CLIENTS' then 's.client_scope collate "C" @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'PAYMENT_%' or v_sort='NAME_ASC') then '(s.week_ending_date is null) @,coalesce(s.week_ending_date,date ''0001-01-01'') @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'CLIENT_%') then '(s.client_display_name is null) @,coalesce(s.client_display_name,'''') collate "C" @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'CHANNEL_%') then '(s.target_pay_channel is null) @,coalesce(s.target_pay_channel,'''') collate "C" @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'DEDUCTIONS_%') then 's.has_deduction @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'AMOUNT_%') then '(s.group_amount is null) @,coalesce(s.group_amount,0) @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind='CANDIDATE_GROUPS' and (v_sort like 'REASON_%') then 's.reason_priority @,s.reason_code collate "C" @,s.presentation_kind collate "C" @,s.presentation_id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'PAYMENT_%' then 'coalesce(i.timesheet_reference,'''') collate "C" @,i.id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'REASON_%' then 'i.reason_priority @,i.reason_code collate "C" @,i.id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'AMOUNT_%' then '(i.nominal_source_ex_vat is null) @,coalesce(i.nominal_source_ex_vat,0) @,i.id @'
    when p_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then 'coalesce(i.candidate_display_name,'''') collate "C" @,i.candidate_id @,i.id @'
    else 'i.id @' end;
  v_order:=replace(v_order,'@',case when v_desc then 'desc' else 'asc' end);
  v_search:=replace(replace(replace(v_search,'!','!!'),'%','!%'),'_','!_')||'%';
  if p_kind='ISSUE_DETAIL' then
    execute 'select count(distinct i.candidate_id)'||v_sql||v_where into v_affected
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
  end if;
  -- A refresh anchor is a native identity, resolved to today's exact sort key here.
  -- It is never a browser-retagged historical cursor.
  if p_kind='CANDIDATE_GROUPS' and v_group is not null and p_after is null then
    execute 'select '||v_key||v_sql||v_where||' and s.presentation_key=$7' into p_after
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if p_after is null then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_anchor_page:=true;
  end if;
  if p_after is not null then
    execute 'select exists(select 1'||v_sql||v_where||' and '||v_tuple||'='||v_seek||')' into v_member
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if not v_member then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_where:=v_where||' and '||v_tuple||case when v_anchor_page then case when v_desc then '<=' else '>=' end else v_op end||v_seek;
  end if;
  if p_kind='PICK_CLIENTS' then
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.client_id,s.client_scope'||v_sql||v_where||' order by '||v_order||' limit $11) s join public.clients c on c.id=s.client_id order by '||v_order;
  elsif p_kind='CANDIDATE_GROUPS' then
    -- Ten indexed native presentation identities before only PK factual enrichment.
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.presentation_key,s.presentation_kind,s.presentation_id,s.parent_group_key,s.candidate_id,s.case_component_id,s.section,s.client_id,s.week_ending_date,s.client_display_name,s.payment_label,s.timesheet_reference,s.target_pay_channel,s.has_deduction,s.selectable,s.reason_code,s.reason_priority,s.group_amount,s.ready_net_add_amount,s.gross_deduction_balance,s.net_deduction_balance,s.waiting_amount,s.waiting_net_add_amount,s.accepted_run_id,s.accepted_command_id,s.accepted_module_epoch,s.snooze_until_date,s.eligible_from_date,s.pay_hold_reason'||v_sql||v_where||' order by '||v_order||' limit $11) s join private.bpay_next_workbench_group g on g.group_key=s.parent_group_key and g.candidate_id=s.candidate_id left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=s.candidate_id order by '||v_order;
  else
    -- No joins or arbitrary row transport: the page is the indexed typed seed.
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key'||v_sql||v_where||' order by '||v_order||' limit $11';
  end if;
  for v_row in execute v_sql using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1 loop
    if v_n=p_limit then v_complete:=false;exit;end if;
    if pg_catalog.octet_length(v_row.row_value::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_last:=v_row.row_key;v_n:=v_n+1;
    if pg_catalog.octet_length(v_rows::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  end loop;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete,'last_key',v_last,
    'affected_candidate_count',case when p_kind='ISSUE_DETAIL' then v_affected::text else null end);
end
$function$;
create or replace function public.bpay_next_workbench_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
  v_actor uuid;v_kind text;v_scope jsonb;v_context jsonb;v_after jsonb;v_cursor jsonb;
  v_page jsonb;v_response jsonb;v_revision private.bpay_next_workbench_revision%rowtype;
  v_epoch bigint;v_agency uuid;v_date_context jsonb;v_limit integer;v_error text;
  v_key_count integer;v_key jsonb;v_sort text;v_text text;v_keys text[];v_count_selection jsonb;
begin
  -- STABLE reads share a single statement snapshot. No volatile authority or
  -- FOR SHARE helper; these are the existing service/active-admin predicates.
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  p_request:=p_request||jsonb_build_object('count_selection',coalesce(p_request->'count_selection','null'::jsonb));
  v_keys:=array['actor_user_id','kind','candidate_id','client_id','channel','section',
    'search','sort','group_key','item_id','reason_code','after','limit','count_selection'];
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>32768
    or not(p_request ?& v_keys) or p_request-v_keys<>'{}'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if jsonb_typeof(p_request->'actor_user_id')<>'string'
    or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select r.* into strict v_revision from private.bpay_next_workbench_revision r where r.id=1;
  select d.installation_id into strict v_agency from private.cloudtms_database_identity d where d.singleton;
  v_kind:=p_request->>'kind';v_sort:=p_request->>'sort';
  if jsonb_typeof(p_request->'kind')<>'string'
    or v_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL',
      'CANDIDATE_GROUPS','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS')
    or jsonb_typeof(p_request->'channel')<>'string' or p_request->>'channel' not in ('ALL','PAYE','UMBRELLA')
    or jsonb_typeof(p_request->'section')<>'string' or p_request->>'section' not in ('ALL','READY','ACTION','BLOCKED','WAITING')
    or jsonb_typeof(p_request->'sort')<>'string' or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort<>'NAME_ASC')
    or (v_sort like 'CLIENT_%' or v_sort like 'CHANNEL_%') and v_kind<>'CANDIDATE_GROUPS'
    or (v_kind='CANDIDATE_GROUPS' and v_sort='NAME_DESC')
    or jsonb_typeof(p_request->'search')<>'string' or length(p_request->>'search')>200
    or octet_length(p_request->>'search')>800
    or btrim(p_request->>'search')<>p_request->>'search'
    or jsonb_typeof(p_request->'limit')<>'number' or p_request->>'limit'!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit not between 1 and 100 or (v_kind='CANDIDATE_GROUPS' and v_limit>10) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  foreach v_text in array array['candidate_id','client_id','item_id'] loop
    if p_request->v_text<>'null'::jsonb and (jsonb_typeof(p_request->v_text)<>'string'
      or p_request->>v_text!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end loop;
  if p_request->'group_key'<>'null'::jsonb and (jsonb_typeof(p_request->'group_key')<>'string'
    or p_request->>'group_key'!~'^(WORK|CASE|ROW):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->>'group_key' like 'ROW:%' and v_kind not in ('GROUP_LINES','CANDIDATE_GROUPS') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->'reason_code'<>'null'::jsonb and (jsonb_typeof(p_request->'reason_code')<>'string'
    or p_request->>'reason_code'!~'^[A-Z][A-Z0-9_]{0,255}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if (v_kind in ('CANDIDATE_GROUPS','GROUP_LINES') and p_request->'candidate_id'='null'::jsonb)
    or (v_kind in ('GROUP_LINES','ISSUE_DETAIL') and p_request->'group_key'='null'::jsonb)
    or (v_kind='ISSUE_DETAIL' and p_request->'reason_code'='null'::jsonb)
    or (v_kind not in ('GROUP_LINES','ISSUE_DETAIL','CANDIDATE_GROUPS') and (p_request->'group_key'<>'null'::jsonb or p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)
    or (v_kind='CANDIDATE_GROUPS' and (p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)))
    or (v_kind not in ('CANDIDATES','PICK_CANDIDATES','PICK_CLIENTS','ACTION_REQUIRED','BLOCKED','EXPORT_ROWS') and p_request->>'search'<>'')
    or (v_kind='HEADER' and p_request->'after'<>'null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_count_selection:=p_request->'count_selection';
  if v_count_selection<>'null'::jsonb then
    if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
      or not(v_count_selection ?& array['mode','candidate_ids'])
      or v_count_selection-array['mode','candidate_ids']<>'{}'::jsonb
      or jsonb_typeof(v_count_selection->'mode')<>'string' or v_count_selection->>'mode' not in ('ALL','NONE')
      or jsonb_typeof(v_count_selection->'candidate_ids')<>'array' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if exists(select 1 from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)
      where jsonb_typeof(a.value)<>'string' or (a.value#>>'{}')!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (select count(*)<>count(distinct a.value) from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)) then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end if;
  v_scope:=p_request-array['actor_user_id','kind','after','limit'];
  -- Fixed effective sections, explicit normalization; client cannot silently
  -- continue a cursor issued for a different list.
  if v_kind='CANDIDATES' then v_scope:=jsonb_set(v_scope,'{section}','"READY"'::jsonb);
  elsif v_kind='ACTION_REQUIRED' then v_scope:=jsonb_set(v_scope,'{section}','"ACTION"'::jsonb);
  elsif v_kind='BLOCKED' then v_scope:=jsonb_set(v_scope,'{section}','"BLOCKED"'::jsonb);end if;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok'<>'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
  v_context:=jsonb_build_object('kind',v_kind,'agency_id',v_agency,'actor_user_id',v_actor,
    'owner_epoch',v_epoch::text,'official_pay_date',v_date_context->>'current_official_pay_date',
    'workbench_revision',v_revision.workbench_revision::text,'adoption_revision',v_revision.adoption_revision::text,
    'scope',v_scope);
  if v_revision.adoption_state<>'CURRENT' or v_revision.dirty_candidate_count<>0
    or v_revision.dirty_group_count<>0 or v_revision.pending_task_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT';end if;
  v_cursor:=p_request->'after';
  if v_cursor<>'null'::jsonb then
    if jsonb_typeof(v_cursor)<>'object' or not(v_cursor ?& array['context','key'])
      or v_cursor-array['context','key']<>'{}'::jsonb or v_cursor->'context' is distinct from v_context then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_after:=v_cursor->'key';
    v_key_count:=case when v_kind in ('CANDIDATES','PICK_CANDIDATES') then case when v_sort like 'DEDUCTIONS_%' or v_sort like 'STATUS_%' then 5 when v_sort like 'AMOUNT_%' then 3 else 4 end
      when v_kind='CANDIDATE_GROUPS' then case when v_sort like 'DEDUCTIONS_%' then 3 else 4 end when v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then case when v_sort like 'PAYMENT_%' then 2 else 3 end else 1 end;
    if jsonb_typeof(v_after)<>'array' or jsonb_array_length(v_after)<>v_key_count then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_kind in ('CANDIDATES','PICK_CANDIDATES') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string'
        or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'DEDUCTIONS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or jsonb_typeof(v_after->2)<>'boolean' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'STATUS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string')) then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind='CANDIDATE_GROUPS' then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(v_after->(v_key_count-2))<>'string' or v_after->>(v_key_count-2) not in ('WORK','CASE','ROW')
        or (v_sort like 'DEDUCTIONS_%' and jsonb_typeof(v_after->0)<>'boolean')
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort not like 'DEDUCTIONS_%' and v_sort not like 'REASON_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string'))
        or ((v_sort='NAME_ASC' or v_sort like 'PAYMENT_%') and ((v_after->>0='true' and v_after->>1<>'') or (v_after->>0='false' and v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')))
        or (v_sort like 'AMOUNT_%' and v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^(0|[1-9][0-9]*)$'
          or (v_after->>0)::numeric>32767 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort like 'PAYMENT_%' and jsonb_typeof(v_after->0)<>'string') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif jsonb_typeof(v_after->0)<>'string' or (v_kind='CANDIDATE_GROUPS'
      and v_after->>0!~'^(WORK|CASE):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (v_kind<>'CANDIDATE_GROUPS' and v_after->>0!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end if;
  case v_kind
    when 'HEADER' then v_page:=private.bpay_next_workbench_header_page_v1(v_scope,v_after,v_limit);
    when 'CANDIDATES' then v_page:=private.bpay_next_workbench_candidates_page_v1(v_scope,v_after,v_limit);
    when 'ACTION_REQUIRED' then v_page:=private.bpay_next_workbench_action_required_page_v1(v_scope,v_after,v_limit);
    when 'BLOCKED' then v_page:=private.bpay_next_workbench_blocked_page_v1(v_scope,v_after,v_limit);
    when 'ISSUE_DETAIL' then v_page:=private.bpay_next_workbench_issue_detail_page_v1(v_scope,v_after,v_limit);
    when 'CANDIDATE_GROUPS' then v_page:=private.bpay_next_workbench_candidate_groups_page_v1(v_scope,v_after,v_limit);
    when 'GROUP_LINES' then v_page:=private.bpay_next_workbench_group_lines_page_v1(v_scope,v_after,v_limit);
    when 'PICK_CANDIDATES' then v_page:=private.bpay_next_workbench_pick_candidates_page_v1(v_scope,v_after,v_limit);
    when 'PICK_CLIENTS' then v_page:=private.bpay_next_workbench_pick_clients_page_v1(v_scope,v_after,v_limit);
    when 'EXPORT_ROWS' then v_page:=private.bpay_next_workbench_export_rows_page_v1(v_scope,v_after,v_limit);
  end case;
  v_response:=jsonb_build_object('ok',true,'kind',v_kind,'workbench_revision',v_revision.workbench_revision::text,
    'complete',v_page->'complete','context',v_context,'rows',v_page->'rows',
    'affected_candidate_count',v_page->'affected_candidate_count',
    'next_cursor',case when v_page->>'complete'='true' then null else jsonb_build_object('context',v_context,'key',v_page->'last_key') end,
    'page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_page->'last_key',
      'row_count',jsonb_array_length(v_page->'rows')::text));
  if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  return v_response;
exception when others then
  v_error:=case
    when sqlstate='42501' then 'BPAY_NEXT_WORKBENCH_FORBIDDEN'
    when sqlstate in ('22023','22P02','22003','22007','22008') then 'BPAY_NEXT_WORKBENCH_REQUEST_INVALID'
    when sqlstate='54000' then 'BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE'
    when sqlerrm in ('BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_WORKBENCH_CURSOR_STALE',
      'BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT','BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE') then sqlerrm
    else 'BPAY_NEXT_WORKBENCH_UNAVAILABLE' end;
  return jsonb_build_object('ok',false,'error_code',v_error,'retryable',v_error in
    ('BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT'),
    'reload_required',v_error='BPAY_NEXT_WORKBENCH_CURSOR_STALE',
    'workbench_revision',v_revision.workbench_revision::text,'complete',false,
    'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
    'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
end
$function$;
alter function private.bpay_next_workbench_refresh_group_display_v1(text) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_group_display_v1(text) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_work_core_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_work_core_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_case_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_case_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
