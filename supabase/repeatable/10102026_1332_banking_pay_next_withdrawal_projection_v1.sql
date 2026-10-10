-- WB02 exact three-way successors; existing public Source and native money owners retained.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_work_wait_state_v1(p_work_id uuid)
returns boolean language plpgsql stable security definer set search_path=pg_catalog,private
as $function$
declare v_state text;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.approval_state into strict v_state from private.bpay_next_work w where w.id=p_work_id;
  return v_state='WITHDRAWN' and coalesce((select sum(p.realised_source_ex_vat)
    from private.bpay_next_position p where p.work_id=p_work_id),0)>0;
end
$function$;

create or replace function private.bpay_next_workbench_refresh_work_v1(p_work_id uuid)
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
  if v_wait_before is distinct from v_wait_after then
    -- Same Candidate is already locked. Exact origin Cases precede metadata/revision.
    for v_case_id in select wc.case_id from private.bpay_next_work_collection wc
      where wc.work_id=v_work.id and wc.case_id is not null order by wc.case_id loop
      perform 1 from private.bpay_next_finance_case c where c.id=v_case_id for update;
    end loop;
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
  if v_wait_before is distinct from v_wait_after then
    for v_case_id in select wc.case_id from private.bpay_next_work_collection wc
      where wc.work_id=v_work.id and wc.case_id is not null order by wc.case_id loop
      perform private.bpay_next_workbench_refresh_case_v1(v_case_id);
    end loop;
  end if;
end
$function$;


create or replace function private.bpay_next_ordinary_authorisation_trigger_v1()
returns trigger
language plpgsql security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_new_authorisation boolean;
  v_removed_authorisation boolean;
  v_route text;
  v_booking_id text;
  v_contract_id uuid;
  v_candidate_id uuid;
  v_work private.bpay_next_work%rowtype;
  v_physical_timesheet_id uuid;
begin
  if tg_op='INSERT' then
    v_new_authorisation:=new.authorised_at_utc is not null;
    v_removed_authorisation:=false;
  else
    v_new_authorisation:=old.authorised_at_utc is null
      and new.authorised_at_utc is not null;
    v_removed_authorisation:=old.authorised_at_utc is not null
      and new.authorised_at_utc is null;
  end if;
  if not (v_new_authorisation or v_removed_authorisation)
     or (select active_owner from private.bpay_next_module_control
          where id=1)<>'NEXT' then
    return new;
  end if;
  if v_new_authorisation then
    v_route:=private.bpay_next_approval_route_v1(new.timesheet_id);
    if v_route in ('ORDINARY_HOURS','EXPENSE_CARRIER') then
      perform private.bpay_next_publish_ordinary_from_financial_v1(new.id);
    end if;
  else
    -- Unauthorisation changes what may be offered next; it does not erase a
    -- revision already captured into a frozen offer or rewrite paid history.
    select t.booking_id,t.contract_id,c.candidate_id
      into strict v_booking_id,v_contract_id,v_candidate_id
      from public.timesheets t
      join public.contracts c on c.id=t.contract_id
      where t.timesheet_id=new.timesheet_id;
    insert into private.bpay_next_worker_control(candidate_id)
      values (v_candidate_id) on conflict(candidate_id) do nothing;
    perform 1 from private.bpay_next_worker_control
      where candidate_id=v_candidate_id for update;
    select * into v_work from private.bpay_next_work
      where booking_id=v_booking_id for update;
    -- The installed unauthorisation owner stamps revoked_at BEFORE clearing
    -- the financial row. Its approval route rightly rejects revoked rows;
    -- use the already-published exact work identity for this transition.
    if not found or v_work.work_kind='SOURCE' then
      return new;
    end if;
    select r.physical_timesheet_id into strict v_physical_timesheet_id
      from private.bpay_next_work_revision r
      where r.id=v_work.current_revision_id and r.work_id=v_work.id;
    if v_work.candidate_id<>v_candidate_id
       or v_work.contract_id<>v_contract_id
       or v_work.work_kind not in ('EXPENSE','ORDINARY')
       or v_work.approval_state<>'APPROVED'
       or v_physical_timesheet_id<>new.timesheet_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_ORDINARY_UNAUTHORISE_WORK_MISMATCH';
    end if;
    update private.bpay_next_work
      set approval_state='WITHDRAWN',current_revision_id=null,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_work.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_candidate_id;
    perform private.bpay_next_workbench_refresh_work_v1(v_work.id);
  end if;
  return new;
end
$function$;


create or replace function private.bpay_next_source_withdrawal_guard_v1()
returns trigger
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_booking_id text;
  v_contract_id uuid;
  v_candidate_id uuid;
  v_work_id uuid;
  v_work private.bpay_next_work%rowtype;
begin
  if old.withdrawn_at_utc is not null or new.withdrawn_at_utc is null
     or (select active_owner from private.bpay_next_module_control
          where id=1 for share)<>'NEXT' then
    return new;
  end if;
  select t.booking_id,t.contract_id,c.candidate_id
    into strict v_booking_id,v_contract_id,v_candidate_id
    from public.timesheets t
    join public.contracts c on c.id=t.contract_id
    where t.timesheet_id=new.root_timesheet_id;
  -- Preserve the publication/worker lock order: Candidate control first,
  -- exact work second. These are the only new rows this event protects.
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select id into v_work_id from private.bpay_next_work
    where booking_id=v_booking_id;
  if v_work_id is null then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_WITHDRAW_WORK_MISSING';
  end if;
  select * into strict v_work from private.bpay_next_work
    where id=v_work_id for update;
  if v_work.work_kind<>'SOURCE'
     or v_work.candidate_id<>v_candidate_id
     or v_work.contract_id<>v_contract_id
     or v_work.approval_state<>'APPROVED' then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_WITHDRAW_WORK_MISMATCH';
  end if;
  -- A later withdrawal changes CURRENT eligibility, not captured finances.
  -- Never release/reprice frozen selections or active holds here. Retain the
  -- separate Source paid-work restriction: one indexed exact-work EXISTS,
  -- no traversal or reconstruction of old Timesheet versions.
  if exists(select 1 from private.bpay_next_financial_effect e
      where e.work_id=v_work_id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SOURCE_WITHDRAW_FINANCIAL_DEPENDENCY';
  end if;
  update private.bpay_next_work
    set approval_state='WITHDRAWN',current_revision_id=null,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_work_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate_id;
  perform private.bpay_next_workbench_refresh_work_v1(v_work_id);
  return new;
end
$function$;

alter function private.bpay_next_work_wait_state_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_work_wait_state_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_refresh_work_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_work_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_ordinary_authorisation_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_ordinary_authorisation_trigger_v1() from public,anon,authenticated,service_role;

alter function private.bpay_next_source_withdrawal_guard_v1() owner to postgres;
revoke all on function private.bpay_next_source_withdrawal_guard_v1() from public,anon,authenticated,service_role;

create or replace function private.bpay_next_workbench_group_contribution_v1(p_group_key text)
returns setof private.bpay_next_workbench_delta
language plpgsql stable security definer set search_path=pg_catalog,private
as $function$
begin
  perform private.bpay_next_workbench_require_next_v1();
  return query
    with scoped as (
      select g.candidate_id,cs.client_id,cs.client_scope,ch.pay_channel,sec.section,
        i.group_key,i.component_key,i.member_kind,i.member_key,i.portion_kind,i.section item_section,
        i.selectable,i.is_selected,i.direction,i.payroll_stage,i.amount_known,i.selected_amount_known,
        (g.complete and g.certified_zero and i.member_kind='GROUP_EVIDENCE') certified_zero_evidence,
        i.reason_code,i.gross_due_source_ex_vat,i.nominal_source_ex_vat,i.approved_source_ex_vat,i.realised_source_ex_vat,
        i.active_reserved_source_ex_vat,i.included_source_ex_vat,i.excluded_source_ex_vat,
        i.recovery_source_ex_vat,i.deduction_balance_source_ex_vat,i.selected_source_ex_vat
      from private.bpay_next_workbench_item i
      join private.bpay_next_workbench_group g on g.group_key=i.group_key
      cross join lateral(select distinct v.client_id,v.client_scope from (values(g.client_id,g.client_id::text),(null::uuid,'ALL'::text)) v(client_id,client_scope)
        where v.client_scope is not null) cs
      cross join lateral(select distinct v.pay_channel from (values(g.target_pay_channel),('ALL'::text)) v(pay_channel)
        where v.pay_channel is not null) ch
      cross join lateral(select distinct v.section from (values(i.section),('ALL'::text)) v(section)) sec
      where i.group_key=p_group_key
    )
    select candidate_id,client_id,client_scope,pay_channel,section,
      count(distinct group_key)::bigint,
      count(*) filter(where member_kind<>'GROUP_EVIDENCE')::bigint,
      count(distinct (component_key,member_kind,member_key)) filter(where member_kind<>'GROUP_EVIDENCE')::bigint,
      count(*) filter(where selectable and direction='PAYMENT')::bigint,
      count(distinct (component_key,member_kind,member_key)) filter(where is_selected)::bigint,
      count(*) filter(where reason_code='SNOOZED')::bigint,
      count(*) filter(where portion_kind='RESERVED' or (portion_kind='WITHDRAWN' and active_reserved_source_ex_vat>0))::bigint,
      count(*) filter(where reason_code in ('PAY_QUERY_OPEN','PAY_QUERY_UNAVAILABLE'))::bigint,
      count(*) filter(where item_section='BLOCKED')::bigint,
      count(distinct (component_key,member_kind,member_key)) filter(where selectable and direction='DEDUCTION')::bigint,
      count(*) filter(where not amount_known and not certified_zero_evidence)::bigint,
      count(*) filter(where is_selected and not selected_amount_known)::bigint,
      coalesce(sum(included_source_ex_vat) filter(where item_section='READY' and direction='PAYMENT' and payroll_stage='GROSS_ADD'),0)::numeric,
      coalesce(sum(gross_due_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='GROSS_ADD' and portion_kind<>'RESERVED'),0)::numeric,
      coalesce(sum(included_source_ex_vat) filter(where item_section='READY' and direction='PAYMENT' and payroll_stage='NET_ADD'),0)::numeric,
      coalesce(sum(nominal_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='NET_ADD' and portion_kind<>'RESERVED'),0)::numeric,
      coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='GROSS_DEDUCT'),0)::numeric,
      coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='NET_DEDUCT'),0)::numeric,
      coalesce(sum(approved_source_ex_vat),0)::numeric,
      coalesce(sum(realised_source_ex_vat),0)::numeric,
      coalesce(sum(active_reserved_source_ex_vat),0)::numeric,
      coalesce(sum(excluded_source_ex_vat),0)::numeric,
      coalesce(sum(recovery_source_ex_vat),0)::numeric,
      coalesce(sum(selected_source_ex_vat) filter(where is_selected and selected_amount_known),0)::numeric
    from scoped group by candidate_id,client_id,client_scope,pay_channel,section;
end
$function$;


alter function private.bpay_next_workbench_group_contribution_v1(text) owner to postgres;
revoke all on function private.bpay_next_workbench_group_contribution_v1(text) from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';
commit;
