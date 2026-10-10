-- WB14 current1743 successors: issue reason keyset and whole exact-group count.
-- WB12 HEADER/Status and all other read bodies/5s/ACLs retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_workbench_kind_page_v1(
  p_kind text,p_scope jsonb,p_after jsonb,p_limit integer
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare v_sql text;v_where text;v_order text;v_tuple text;v_seek text;v_key text;v_projection text;
  v_row record;v_rows jsonb:='[]'::jsonb;v_last jsonb;v_n integer:=0;v_complete boolean:=true;v_member boolean;v_affected bigint;
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
    v_where:='g.candidate_id=$4';
    if v_client is not null then v_where:=v_where||' and g.client_id=$5';end if;
    if v_channel<>'ALL' then v_where:=v_where||' and g.target_pay_channel=$2';end if;
    if v_section='READY' then v_where:=v_where||' and g.complete and not g.dirty and g.progress_state=''CURRENT''';end if;
    if v_section<>'ALL' then v_where:=v_where||' and exists(select 1 from private.bpay_next_workbench_item i where i.group_key=g.group_key and i.section=$3'||case when v_section='READY' then ' and i.selectable' else '' end||')';end if;
    v_projection:=$query$pg_catalog.jsonb_build_object('group_key',g.group_key,
      'group_kind',g.group_kind,
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
      'client_display_name',a.client_display_name,
      'bank_warning_code',m.bank_warning_code,
      'section',$3,
      'reason_code',a.reason_code,
      'has_deduction',a.has_deduction,
      'selectable',g.complete and not g.dirty and g.progress_state='CURRENT' and a.selectable,
      'group_amount',case when a.unknown_count=0 then a.group_amount::text else null end,
      'ready_net_add_amount',case when a.unknown_count=0 then a.ready_net_add_amount::text else null end,
      'gross_deduction_balance',case when a.unknown_count=0 then a.gross_deduction_balance::text else null end,
      'net_deduction_balance',case when a.unknown_count=0 then a.net_deduction_balance::text else null end)$query$;
    v_tuple:='((g.week_ending_date is null),coalesce(g.week_ending_date,date ''0001-01-01''),g.group_kind collate "C",coalesce(g.work_id,g.case_id))';
    v_seek:='(($10->>0)::boolean,coalesce(nullif($10->>1,'''')::date,date ''0001-01-01''),($10->>2) collate "C",($10->>3)::uuid)';
    v_key:='pg_catalog.jsonb_build_array(g.week_ending_date is null,coalesce(g.week_ending_date::text,''''),g.group_kind,coalesce(g.work_id,g.case_id))';
    v_sql:=' from private.bpay_next_workbench_group g where ';
  else
    v_where:='true';
    if v_candidate is not null then v_where:=v_where||' and i.candidate_id=$4';end if;
    if v_client is not null then v_where:=v_where||' and i.client_id=$5';end if;
    if v_channel<>'ALL' then v_where:=v_where||' and i.target_pay_channel=$2';end if;
    if v_section<>'ALL' then v_where:=v_where||' and i.section=$3';end if;
    if v_group is not null then v_where:=v_where||' and i.group_key=$7';end if;
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
      'group_key',i.group_key,
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
    when p_kind='CANDIDATE_GROUPS' then '(g.week_ending_date is null) @,coalesce(g.week_ending_date,date ''0001-01-01'') @,g.group_kind collate "C" @,coalesce(g.work_id,g.case_id) @'
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
  if p_after is not null then
    execute 'select exists(select 1'||v_sql||v_where||' and '||v_tuple||'='||v_seek||')' into v_member
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if not v_member then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_where:=v_where||' and '||v_tuple||v_op||v_seek;
  end if;
  if p_kind='PICK_CLIENTS' then
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.client_id,s.client_scope'||v_sql||v_where||' order by '||v_order||' limit $11) s join public.clients c on c.id=s.client_id order by '||v_order;
  elsif p_kind='CANDIDATE_GROUPS' then
    -- <=10 exact parents before indexed current-member enrichment. WB10 owns
    -- later maintained/index performance changes; no history reconstruction.
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select g.group_key,g.group_kind,g.candidate_id,g.work_id,g.case_id,g.client_id,g.contract_id,g.physical_timesheet_id,g.current_revision_id,g.source_pay_channel,g.target_pay_channel,g.week_ending_date,g.complete,g.dirty,g.certified_zero,g.ready_count,g.action_count,g.blocked_count,g.waiting_count,g.stale_snooze_count,g.financial_view_revision,g.projection_revision,g.metadata_version,g.stale_snooze_code,g.error_code,g.progress_state'||v_sql||v_where||' order by '||v_order||' limit $11) g left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=g.candidate_id cross join lateral (
      select min(i.client_display_name) client_display_name,
        (select ir.reason_code from private.bpay_next_workbench_item ir where ir.group_key=g.group_key and ($3=''ALL'' or ir.section=$3) order by ir.reason_priority,ir.id limit 1) reason_code,
        coalesce(bool_or(i.direction=''DEDUCTION''),false) has_deduction,
        coalesce(bool_or(i.selectable),false) selectable,
        count(*) filter(where not i.amount_known and not(g.certified_zero and i.member_kind=''GROUP_EVIDENCE'')) unknown_count,
        coalesce(sum(i.included_source_ex_vat) filter(where i.section=''READY'' and i.direction=''PAYMENT'' and i.payroll_stage=''GROSS_ADD''),0) group_amount,
        coalesce(sum(i.included_source_ex_vat) filter(where i.section=''READY'' and i.direction=''PAYMENT'' and i.payroll_stage=''NET_ADD''),0) ready_net_add_amount,
        coalesce(sum(i.deduction_balance_source_ex_vat) filter(where i.direction=''DEDUCTION'' and i.payroll_stage=''GROSS_DEDUCT''),0) gross_deduction_balance,
        coalesce(sum(i.deduction_balance_source_ex_vat) filter(where i.direction=''DEDUCTION'' and i.payroll_stage=''NET_DEDUCT''),0) net_deduction_balance
      from private.bpay_next_workbench_item i where i.group_key=g.group_key and ($3=''ALL'' or i.section=$3)) a order by '||v_order;
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
    or jsonb_typeof(p_request->'sort')<>'string' or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort<>'NAME_ASC')
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
    or p_request->>'group_key'!~'^(WORK|CASE):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->'reason_code'<>'null'::jsonb and (jsonb_typeof(p_request->'reason_code')<>'string'
    or p_request->>'reason_code'!~'^[A-Z][A-Z0-9_]{0,255}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if (v_kind in ('CANDIDATE_GROUPS','GROUP_LINES') and p_request->'candidate_id'='null'::jsonb)
    or (v_kind in ('GROUP_LINES','ISSUE_DETAIL') and p_request->'group_key'='null'::jsonb)
    or (v_kind='ISSUE_DETAIL' and p_request->'reason_code'='null'::jsonb)
    or (v_kind not in ('GROUP_LINES','ISSUE_DETAIL') and (p_request->'group_key'<>'null'::jsonb or p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb))
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
      when v_kind='CANDIDATE_GROUPS' then 4 when v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then case when v_sort like 'PAYMENT_%' then 2 else 3 end else 1 end;
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
      if jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string'
        or (v_after->>0='true' and v_after->>1<>'') or (v_after->>0='false' and v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
        or jsonb_typeof(v_after->2)<>'string' or v_after->>2 not in ('WORK','CASE')
        or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
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

alter function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
