-- WB34 same-parent completion anchors. Full current0117/0132 bodies; ordinary cursor semantics unchanged.
\set ON_ERROR_STOP on
begin;
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
  v_baseline uuid:=(p_scope->'_selection_memory'->>'run_id')::uuid;v_memory jsonb:=p_scope->'_selection_memory';
  v_mode text;v_intent jsonb;v_value jsonb;v_remembered jsonb;
  v_date_context jsonb;v_pay_date date;v_native record;v_quote jsonb;v_cache_entry jsonb;
  v_quote_cache jsonb[]:=array[]::jsonb[];v_cache_bytes bigint:=0;v_cached boolean;
  v_keys text[];v_source numeric;v_chosen_component numeric;v_chosen_leaf numeric;
  v_prefix_count bigint;v_badge text;v_refresh_key jsonb;v_refresh_live jsonb;v_refresh_identity text;v_refresh_before boolean:=false;

begin
  if p_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS') or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_scope ? '_refresh_anchor_key' and (p_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES') or p_after is not null) then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_desc:=v_sort like '%_DESC';v_op:=case when v_desc then '<' else '>' end;
  if p_kind in ('CANDIDATE_GROUPS','GROUP_LINES') then
    v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
    if v_date_context->>'ok' is distinct from 'true' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
    v_pay_date:=(v_date_context->>'current_official_pay_date')::date;
    if p_scope ? '_official_pay_date' and (p_scope->>'_official_pay_date')::date is distinct from v_pay_date then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  end if;
  if p_kind='EXPORT_ROWS' then
    -- WB18: native list sort only, exact bounded item seed before immutable facts.
    -- Export controls supply the exact normalized filter, not free text.
    if v_search<>'' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_sort in ('C_NAME_ASC','C_NAME_DESC') then
      v_tuple:='(coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C",(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C",i.candidate_id,i.id)';v_seek:='(($10->>0) collate "C",($10->>1)::boolean,($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C" @,(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null) @,coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C" @,i.candidate_id @,i.id @';v_key:='jsonb_build_array(coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,''''),(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,''''),i.candidate_id,i.id)';
    elsif v_sort in ('C_AMOUNT_ASC','C_AMOUNT_DESC') then
      v_tuple:='((coalesce(cs.unknown_amount_count,0)>0),(case when coalesce(cs.unknown_amount_count,0)=0 then coalesce(cs.amount,0) else 0 end),i.candidate_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2)::uuid,($10->>3)::uuid)';v_order:='(coalesce(cs.unknown_amount_count,0)>0) @,(case when coalesce(cs.unknown_amount_count,0)=0 then coalesce(cs.amount,0) else 0 end) @,i.candidate_id @,i.id @';v_key:='jsonb_build_array((coalesce(cs.unknown_amount_count,0)>0),(case when coalesce(cs.unknown_amount_count,0)=0 then coalesce(cs.amount,0) else 0 end)::text,i.candidate_id,i.id)';
    elsif v_sort in ('C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC') then
      v_tuple:='((coalesce(cs.deduction_item_count,0)>0),coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C",(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C",i.candidate_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::boolean,($10->>3) collate "C",($10->>4)::uuid,($10->>5)::uuid)';v_order:='(coalesce(cs.deduction_item_count,0)>0) @,coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C" @,(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null) @,coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,'''') collate "C" @,i.candidate_id @,i.id @';v_key:='jsonb_build_array((coalesce(cs.deduction_item_count,0)>0),coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,''''),(coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference) is null),coalesce(cs.tms_ref,m.tms_ref,i.candidate_reference,''''),i.candidate_id,i.id)';
    elsif v_sort in ('C_STATUS_ASC','C_STATUS_DESC') then
      v_tuple:='((coalesce(cs.payable_count,0)=0),(coalesce(cs.bank_warning_code,m.bank_warning_code) is null),coalesce(cs.bank_warning_code,m.bank_warning_code,'''') collate "C",coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C",i.candidate_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::boolean,($10->>2) collate "C",($10->>3) collate "C",($10->>4)::uuid,($10->>5)::uuid)';v_order:='(coalesce(cs.payable_count,0)=0) @,(coalesce(cs.bank_warning_code,m.bank_warning_code) is null) @,coalesce(cs.bank_warning_code,m.bank_warning_code,'''') collate "C" @,coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,'''') collate "C" @,i.candidate_id @,i.id @';v_key:='jsonb_build_array((coalesce(cs.payable_count,0)=0),(coalesce(cs.bank_warning_code,m.bank_warning_code) is null),coalesce(cs.bank_warning_code,m.bank_warning_code,''''),coalesce(cs.candidate_sort_name,m.candidate_sort_name,i.candidate_display_name,''''),i.candidate_id,i.id)';
    elsif v_sort in ('G_PAYMENT_ASC','G_PAYMENT_DESC') then
      v_tuple:='((gd.week_ending_date is null),coalesce(gd.week_ending_date,date ''0001-01-01''),gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::date,($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.week_ending_date is null) @,coalesce(gd.week_ending_date,date ''0001-01-01'') @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.week_ending_date is null),coalesce(gd.week_ending_date,date ''0001-01-01'')::text,gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_CLIENT_ASC','G_CLIENT_DESC') then
      v_tuple:='((gd.client_display_name is null),coalesce(gd.client_display_name,'''') collate "C",gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.client_display_name is null) @,coalesce(gd.client_display_name,'''') collate "C" @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.client_display_name is null),coalesce(gd.client_display_name,''''),gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_CHANNEL_ASC','G_CHANNEL_DESC') then
      v_tuple:='((gd.target_pay_channel is null),coalesce(gd.target_pay_channel,'''') collate "C",gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.target_pay_channel is null) @,coalesce(gd.target_pay_channel,'''') collate "C" @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.target_pay_channel is null),coalesce(gd.target_pay_channel,''''),gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_AMOUNT_ASC','G_AMOUNT_DESC') then
      v_tuple:='((gd.group_amount is null),coalesce(gd.group_amount,0),gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1)::numeric,($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='(gd.group_amount is null) @,coalesce(gd.group_amount,0) @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array((gd.group_amount is null),coalesce(gd.group_amount,0)::text,gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC') then
      v_tuple:='(gd.has_deduction,gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::boolean,($10->>1) collate "C",($10->>2)::uuid,($10->>3)::uuid)';v_order:='gd.has_deduction @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array(gd.has_deduction,gd.presentation_kind,gd.presentation_id,i.id)';
    elsif v_sort in ('G_REASON_ASC','G_REASON_DESC') then
      v_tuple:='(gd.reason_priority,gd.reason_code collate "C",gd.presentation_kind collate "C",gd.presentation_id,i.id)';v_seek:='(($10->>0)::smallint,($10->>1) collate "C",($10->>2) collate "C",($10->>3)::uuid,($10->>4)::uuid)';v_order:='gd.reason_priority @,gd.reason_code collate "C" @,gd.presentation_kind collate "C" @,gd.presentation_id @,i.id @';v_key:='jsonb_build_array(gd.reason_priority,gd.reason_code,gd.presentation_kind,gd.presentation_id,i.id)';
    else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    v_order:=replace(v_order,'@',case when v_desc then 'desc' else 'asc' end);
    v_sql:=' from private.bpay_next_workbench_item i
      join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
      join private.bpay_next_workbench_group_display gd on gd.parent_group_key=i.group_key and gd.candidate_id=i.candidate_id
        and gd.section=''ALL'' and (gd.presentation_kind<>''ROW'' or gd.case_component_id=i.case_component_id)
      left join private.bpay_next_workbench_candidate_summary cs on cs.candidate_id=i.candidate_id
        and cs.client_scope=$1 and cs.pay_channel=$2 and cs.section=''READY''
      left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=i.candidate_id where ';
    v_where:='($4 is null or i.candidate_id=$4) and ($5 is null or i.client_id=$5)
      and ($2=''ALL'' or i.target_pay_channel=$2) and ($3=''ALL'' or i.section=$3)';
    if p_after is not null then
      execute 'select exists(select 1'||v_sql||v_where||' and '||v_tuple||'='||v_seek||')' into v_member
        using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
      if not v_member then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
      v_where:=v_where||' and '||v_tuple||v_op||v_seek;
    end if;
    v_sql:='with seed as materialized(select i.id,'||v_key||' row_key,gd.payment_label,
        (coalesce(cs.payable_count,0)>0) candidate_has_pay'||v_sql||v_where||' order by '||v_order||' limit $11)
      select '||'pg_catalog.jsonb_build_object(''item_id'',i.id,
      ''group_key'',i.group_key,
      ''candidate_id'',i.candidate_id,
      ''work_id'',i.work_id,
      ''case_id'',i.case_id,
      ''case_component_id'',i.case_component_id,
      ''revision_id'',i.revision_id,
      ''client_id'',i.client_id,
      ''component_key'',i.component_key,
      ''member_kind'',i.member_kind,
      ''member_key'',i.member_key,
      ''portion_kind'',i.portion_kind,
      ''candidate_display_name'',i.candidate_display_name,
      ''candidate_reference'',i.candidate_reference,
      ''client_display_name'',i.client_display_name,
      ''timesheet_reference'',i.timesheet_reference,
      ''work_date'',i.work_date,
      ''week_ending_date'',i.week_ending_date,
      ''component_kind'',i.component_kind,
      ''tax_treatment'',i.tax_treatment,
      ''source_pay_channel'',i.source_pay_channel,
      ''target_pay_channel'',i.target_pay_channel,
      ''direction'',i.direction,
      ''payroll_stage'',i.payroll_stage,
      ''approved_quantity'',(i.approved_quantity)::text,
      ''unit_label'',i.unit_label,
      ''nominal_source_ex_vat'',(i.nominal_source_ex_vat)::text,
      ''approved_source_ex_vat'',(i.approved_source_ex_vat)::text,
      ''realised_source_ex_vat'',(i.realised_source_ex_vat)::text,
      ''active_reserved_source_ex_vat'',(i.active_reserved_source_ex_vat)::text,
      ''residual_source_ex_vat'',(i.residual_source_ex_vat)::text,
      ''included_source_ex_vat'',(i.included_source_ex_vat)::text,
      ''excluded_source_ex_vat'',(i.excluded_source_ex_vat)::text,
      ''deduction_balance_source_ex_vat'',(i.deduction_balance_source_ex_vat)::text,
      ''amount_known'',i.amount_known,
      ''section'',i.section,
      ''reason_code'',i.reason_code,
      ''selectable'',i.selectable,
      ''financial_view_revision'',(i.financial_view_revision)::text,
      ''projection_revision'',(i.projection_revision)::text)||pg_catalog.jsonb_build_object(''metadata_version'',(i.metadata_version)::text)||jsonb_build_object(
      ''export_key'',seed.row_key,''approved_line_id'',i.approved_line_id,
      ''physical_timesheet_id'',r.physical_timesheet_id,''job_title'',r.job_title,''band_label'',r.band_label,
      ''shift_start'',coalesce(sd.shift_start_local,to_char(sd.shift_start_at at time zone ''Europe/London'',''HH24:MI'')),
      ''shift_finish'',coalesce(sd.shift_end_local,to_char(sd.shift_end_at at time zone ''Europe/London'',''HH24:MI'')),
      ''detail_label'',coalesce(sd.detail_label,l.evidence_ref),
      ''candidate_has_pay'',seed.candidate_has_pay,
      ''export_amount'',(case when not i.amount_known then null
        when i.is_selected then case when i.selected_amount_known then i.selected_source_ex_vat end
        when i.portion_kind=''RESERVED'' then i.active_reserved_source_ex_vat
        when i.direction=''PAYMENT'' and i.section=''READY'' then i.included_source_ex_vat
        when i.direction=''PAYMENT'' and i.section=''WAITING'' and i.payroll_stage=''GROSS_ADD'' then i.gross_due_source_ex_vat
        else i.nominal_source_ex_vat end)::text,
      ''export_label'',seed.payment_label,
      ''choice_eligible'',i.selectable and i.section=''READY'' and not i.is_selected and g.complete and not g.dirty and g.progress_state=''CURRENT'')'||' as row_value,seed.row_key from seed
      join private.bpay_next_workbench_item i on i.id=seed.id
      join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
      left join private.bpay_next_work_revision r on r.id=i.revision_id and r.work_id=i.work_id
      left join private.bpay_next_approved_line l on l.id=i.approved_line_id and l.revision_id=i.revision_id and l.component_key=i.component_key
      left join private.bpay_next_approved_segment sg on sg.id=i.approved_segment_id and sg.revision_id=i.revision_id
        and sg.approved_line_id=i.approved_line_id and sg.component_key=i.component_key and sg.segment_key=i.segment_key
      left join private.bpay_next_shift_detail sd on sd.id=sg.shift_detail_id and sd.approved_line_id=i.approved_line_id
        and sd.segment_key=i.segment_key order by '||replace(v_seek,'$10','seed.row_key')||case when v_desc then ' desc' else ' asc' end;
    -- Only the <=101 seeded identities reach this typed final presentation sort.
    -- No unbounded ranking, JSON numeric-string ordering or factual scan.
    for v_row in execute v_sql using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1 loop
      if v_n=p_limit then v_complete:=false;exit;end if;
      v_value:=private.bpay_next_selection_memory_row_v1('GROUP_LINES',v_row.row_value,p_scope,v_baseline);
      -- One Candidate's native selected-pay fact, never a browser inventory.
      v_value:=v_value||jsonb_build_object('candidate_default_pay_selected',exists(
       select 1 from private.bpay_next_selection_memory_choices_v1(
        jsonb_set(p_scope,'{candidate_id}',v_value->'candidate_id'),v_baseline,null) selected
       where selected.selected and selected.direction='PAYMENT'));
      v_rows:=v_rows||jsonb_build_array(v_value);v_last:=v_row.row_key;v_n:=v_n+1;
      if octet_length(v_rows::text)>110000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    end loop;
    return jsonb_build_object('rows',v_rows,'complete',v_complete,'last_key',v_last);
  end if;
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
    select m.mode,m.raw_intent into v_mode,v_remembered from private.bpay_next_selection_mode_memory m where m.run_id=v_baseline and m.row_kind='HEADER';
    if v_count_selection ? 'intent' then
     v_intent:=v_count_selection->'intent';
     with choices as materialized(select * from private.bpay_next_selection_memory_choices_v1(p_scope,v_baseline,v_intent)),
     payable as (select distinct candidate_id from choices where selected and direction='PAYMENT')
     select (select count(*) from payable),count(*) into v_selected_candidates,v_selected_members
      from private.bpay_next_workbench_item i join choices l on l.approved_line_id=i.approved_line_id or l.case_component_id=i.case_component_id
      where i.section='READY' and i.selectable and i.amount_known and not i.is_selected and i.portion_kind in ('AVAILABLE','ZERO')
       and l.selected and exists(select 1 from payable p where p.candidate_id=l.candidate_id);
     v_rows:=v_rows||jsonb_build_object('selected_candidate_count',v_selected_candidates::text,'selected_member_count',v_selected_members::text);
    end if;
    v_rows:=v_rows||jsonb_build_object('selection_memory',jsonb_build_object('run_id',v_baseline,'scope',v_memory->'scope'),
     'selection_mode',coalesce(v_mode,'ALL_MATCHING'),'remembered_intent',v_remembered);
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
      'case_state',fc.status,
      'client_id',g.client_id,
      'contract_id',g.contract_id,
      'physical_timesheet_id',r.physical_timesheet_id,
      'physical_revision_id',r.id,'physical_timesheet_version',r.physical_timesheet_version::text,
      'source_kind',r.source_kind,'source_head_id',r.source_head_id,
      'source_root_timesheet_id',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_id else h.root_timesheet_id end end,
      'source_root_timesheet_version',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_version else h.root_timesheet_version end end::text,
      'source_week_ending_date',case when r.source_kind in ('SOURCE','PROTECTED') then r.week_ending_date end,

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
      'physical_timesheet_id',r.physical_timesheet_id,
      'physical_revision_id',r.id,'physical_timesheet_version',r.physical_timesheet_version::text,
      'source_kind',r.source_kind,'source_head_id',r.source_head_id,
      'source_root_timesheet_id',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_id else h.root_timesheet_id end end,
      'source_root_timesheet_version',case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_version else h.root_timesheet_version end end::text,
      'source_week_ending_date',case when r.source_kind in ('SOURCE','PROTECTED') then r.week_ending_date end,

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
      'written_off_source_ex_vat',(i.written_off_source_ex_vat)::text,
      'approved_line_no',l.line_no::text,'shift_detail_id',sd.id,'shift_detail_no',sd.detail_no::text,
      'job_title',r.job_title,'band_label',r.band_label,
      'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
      'is_adjustment',ts.is_adjustment,
      'component_amount_known',a.amount_known,'component_ready_amount',a.ready_amount::text,
      'component_waiting_amount',a.waiting_amount::text,'component_reserved_amount',a.reserved_amount::text)$query$ when 'EXPORT_ROWS' then $query$pg_catalog.jsonb_build_object('item_id',i.id,
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
  if p_scope ? '_refresh_anchor_key' then
    if p_after is not null or p_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    v_refresh_before:=coalesce((p_scope->>'_refresh_anchor_before')::boolean,false);v_refresh_key:=p_scope->'_refresh_anchor_key';p_after:=v_refresh_key;v_anchor_page:=true;
    v_refresh_identity:=case when p_kind='CANDIDATES' then 's.candidate_id=($10->>-1)::uuid'
      when p_kind='CANDIDATE_GROUPS' then 's.presentation_key=($10->>-2)||'':''||($10->>-1)'
      else 'i.id=($10->>-1)::uuid' end;
    execute 'select '||v_key||v_sql||v_where||' and '||v_refresh_identity into v_refresh_live
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if v_refresh_live is not null then p_after:=v_refresh_live;end if;
    if not v_refresh_before then
    execute 'select exists(select 1'||v_sql||v_where||' and '||v_tuple||case when v_desc then '<=' else '>=' end||v_seek||')' into v_member
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if not v_member then
      -- Vanished terminal anchor: bounded reverse seek to the nearest remaining predecessor.
      execute 'select '||v_key||v_sql||v_where||' and '||v_tuple||case when v_desc then '>' else '<' end||v_seek||' order by '||
        replace(v_order,case when v_desc then ' desc' else ' asc' end,case when v_desc then ' asc' else ' desc' end)||' limit 1' into v_refresh_live
        using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
      if v_refresh_live is null then return jsonb_build_object('rows','[]'::jsonb,'complete',true,'last_key',null,'affected_candidate_count',case when p_kind='ISSUE_DETAIL' then v_affected::text else null end);end if;
      p_after:=v_refresh_live;
    end if;
    end if;
  elsif p_kind='CANDIDATE_GROUPS' and v_group is not null and p_after is null then
    execute 'select '||v_key||v_sql||v_where||' and s.presentation_key=$7' into p_after
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if p_after is null then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_anchor_page:=true;
  end if;
  if p_after is not null then
    execute 'select exists(select 1'||v_sql||v_where||' and '||v_tuple||'='||v_seek||')' into v_member
      using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1;
    if not v_member and not(p_scope ? '_refresh_anchor_key') then raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_where:=v_where||' and '||v_tuple||case when v_refresh_before then case when v_desc then '>' else '<' end when v_anchor_page then case when v_desc then '<=' else '>=' end else v_op end||v_seek;
  end if;
  if v_refresh_before then
    v_order:=replace(v_order,case when v_desc then ' desc' else ' asc' end,case when v_desc then ' asc' else ' desc' end);
  end if;
  if p_kind='PICK_CLIENTS' then
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.client_id,s.client_scope'||v_sql||v_where||' order by '||v_order||' limit $11) s join public.clients c on c.id=s.client_id order by '||v_order;
  elsif p_kind='CANDIDATE_GROUPS' then
    -- Ten indexed native presentation identities before only PK factual enrichment.
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.presentation_key,s.presentation_kind,s.presentation_id,s.parent_group_key,s.candidate_id,s.case_component_id,s.section,s.client_id,s.week_ending_date,s.client_display_name,s.payment_label,s.timesheet_reference,s.target_pay_channel,s.has_deduction,s.selectable,s.reason_code,s.reason_priority,s.group_amount,s.ready_net_add_amount,s.gross_deduction_balance,s.net_deduction_balance,s.waiting_amount,s.waiting_net_add_amount,s.accepted_run_id,s.accepted_command_id,s.accepted_module_epoch,s.snooze_until_date,s.eligible_from_date,s.pay_hold_reason'||v_sql||v_where||' order by '||v_order||' limit $11) s join private.bpay_next_workbench_group g on g.group_key=s.parent_group_key and g.candidate_id=s.candidate_id left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=s.candidate_id left join private.bpay_next_finance_case fc on fc.id=g.case_id and fc.candidate_id=g.candidate_id left join private.bpay_next_work w on w.id=g.work_id and w.candidate_id=g.candidate_id left join private.bpay_next_work_revision r on r.work_id=w.id and r.id=case when w.approval_state=''WITHDRAWN'' then w.applied_revision_id else w.current_revision_id end left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id and h.candidate_id=w.candidate_id and h.contract_id=w.contract_id and h.week_ending_date=r.week_ending_date order by '||v_order;
  elsif p_kind='ISSUE_DETAIL' then
    -- WB17: native issue identities first, exact Work current/withdrawn-applied
    -- revision only; no first-booking fallback or current Source pointer.
    v_sql:='with seed as materialized(select i.id,'||v_key||' row_key'||v_sql||v_where||' order by '||v_order||' limit $11)
      select '||v_projection||' as row_value,seed.row_key from seed
      join private.bpay_next_workbench_item i on i.id=seed.id
      left join private.bpay_next_work w on w.id=i.work_id and w.candidate_id=i.candidate_id
      left join private.bpay_next_work_revision r on r.work_id=w.id
        and r.id=case when w.approval_state=''WITHDRAWN'' then w.applied_revision_id else w.current_revision_id end
      left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id
        and h.candidate_id=w.candidate_id and h.contract_id=w.contract_id and h.week_ending_date=r.week_ending_date
      order by '||v_order;
  elsif p_kind='GROUP_LINES' then
    -- WB16: limit the original scoped UUID identities first. Deduplicate only
    -- their exact component keys, then enrich from current indexed facts.
    -- No history, quote, producer or browser aggregation on opening.
    v_sql:='with seed as materialized(select i.id,'||v_key||' row_key'||v_sql||v_where||' order by '||v_order||' limit $11),
      component_keys as materialized(select distinct i.group_key,i.candidate_id,i.work_id,i.revision_id,i.approved_line_id,i.component_key
        from seed join private.bpay_next_workbench_item i on i.id=seed.id where i.work_id is not null and i.approved_line_id is not null),
      component_amounts as materialized(select k.group_key,k.candidate_id,k.work_id,k.revision_id,k.approved_line_id,k.component_key,
        count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id) amount_known,
        case when count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id)
          then coalesce(sum(case when m.section=''READY'' then case when m.is_selected then m.selected_source_ex_vat else m.included_source_ex_vat end else 0 end),0) end ready_amount,
        case when count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id)
          then coalesce(sum(m.gross_due_source_ex_vat) filter(where m.section=''WAITING'' and m.portion_kind<>''RESERVED''),0) end waiting_amount,
        case when count(*)>0 and bool_and(m.amount_known and (not m.is_selected or m.selected_amount_known)
          and m.included_source_ex_vat is not null and m.gross_due_source_ex_vat is not null
          and m.active_reserved_source_ex_vat is not null and g.complete and not g.dirty and g.current_revision_id=m.revision_id)
          then coalesce(sum(m.active_reserved_source_ex_vat) filter(where m.portion_kind=''RESERVED''),0) end reserved_amount
        from component_keys k join private.bpay_next_workbench_item m on m.group_key=k.group_key and m.candidate_id=k.candidate_id
          and m.work_id=k.work_id and m.revision_id=k.revision_id and m.approved_line_id=k.approved_line_id and m.component_key=k.component_key
          and m.member_kind in (''WORK_COMPONENT'',''WORK_SEGMENT'')
        join private.bpay_next_workbench_group g on g.group_key=k.group_key and g.candidate_id=k.candidate_id
        group by k.group_key,k.candidate_id,k.work_id,k.revision_id,k.approved_line_id,k.component_key)
      select '||v_projection||' as row_value,seed.row_key from seed join private.bpay_next_workbench_item i on i.id=seed.id
      left join private.bpay_next_approved_line l on l.id=i.approved_line_id and l.revision_id=i.revision_id and l.component_key=i.component_key
      left join private.bpay_next_work_revision r on r.id=i.revision_id and r.work_id=i.work_id
      left join private.bpay_next_work w on w.id=i.work_id and w.candidate_id=i.candidate_id
      left join private.bpay_next_approved_segment sg on sg.id=i.approved_segment_id and sg.revision_id=i.revision_id
        and sg.approved_line_id=i.approved_line_id and sg.component_key=i.component_key and sg.segment_key=i.segment_key
      left join private.bpay_next_shift_detail sd on sd.id=sg.shift_detail_id and sd.approved_line_id=i.approved_line_id and sd.segment_key=i.segment_key
      left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
      left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
        and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=i.candidate_id)
      left join component_amounts a on a.group_key=i.group_key and a.candidate_id=i.candidate_id and a.work_id=i.work_id
        and a.revision_id=i.revision_id and a.approved_line_id=i.approved_line_id and a.component_key=i.component_key
      order by '||v_order;
  else
    -- No joins or arbitrary row transport: the page is the indexed typed seed.
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key'||v_sql||v_where||' order by '||v_order||' limit $11';
  end if;
  for v_row in execute v_sql using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit+1 loop
    if v_n=p_limit then v_complete:=false;exit;end if;
    if pg_catalog.octet_length(v_row.row_value::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    v_value:=v_row.row_value;
    -- Output-only chosen money. Original Source fields and native page keys stay literal.
    if p_kind='CANDIDATE_GROUPS' then
      v_badge:=null;
      if v_value->>'group_kind'='WORK' and v_value->>'section'='READY' and v_value->>'group_amount' is not null then
        select g.target_pay_channel into v_badge
        from private.bpay_next_workbench_group g
        join private.bpay_next_work w on w.id=g.work_id and w.candidate_id=g.candidate_id
        join private.bpay_next_work_revision r on r.id=w.current_revision_id and r.work_id=w.id
        join public.candidates c on c.id=w.candidate_id
        left join public.umbrellas u on u.id=c.umbrella_id
        join private.bpay_next_workbench_candidate_metadata md on md.candidate_id=c.id
        join private.bpay_next_worker_control wc on wc.candidate_id=c.id
        where g.group_key=v_value->>'parent_group_key' and g.candidate_id=(v_value->>'candidate_id')::uuid
          and g.complete and not g.dirty and g.progress_state='CURRENT'
          and g.current_revision_id=r.id and w.applied_revision_id=r.id and w.approval_state='APPROVED'
          and g.chosen_display_pay_date=v_pay_date and g.target_pay_channel=upper(c.pay_method)
          and c.active and (g.target_pay_channel='PAYE' or g.target_pay_channel='UMBRELLA' and u.enabled and u.vat_chargeable is not null)
          and g.metadata_version=md.metadata_version and not md.continuation_pending and md.adopted_metadata_version=md.metadata_version
          and g.financial_view_revision=wc.financial_view_revision
          and exists(select 1 from private.bpay_next_workbench_item i
            join private.bpay_next_approved_line a on a.id=i.approved_line_id and a.revision_id=r.id and a.component_key=i.component_key
            join private.bpay_next_component_resolution d on d.work_id=w.id and d.state='ACTIVE'
              and (d.based_on_revision_id=r.id or d.published_revision_id=r.id)
              and (case when d.based_on_revision_id=r.id then d.approved_line_id else d.published_line_id end)=a.id
            cross join lateral private.bpay_next_component_resolution_active_v1(w.id,r.id,a.id,d.rate_family,d.rate_code,
              g.target_pay_channel,case when g.target_pay_channel='UMBRELLA' then u.id end,
              case when g.target_pay_channel='UMBRELLA' then u.vat_chargeable end,r.valuation_policy_id,v_pay_date) active
            where i.group_key=g.group_key and i.candidate_id=g.candidate_id and i.work_id=w.id and i.revision_id=r.id
              and a.tax_treatment='TAXABLE' and i.tax_treatment='TAXABLE' and i.section='READY' and i.selectable
              and i.amount_known and not i.is_selected and i.direction='PAYMENT' and i.portion_kind in ('AVAILABLE','ZERO')
              and i.progress_state='CURRENT' and i.projection_revision=g.projection_revision
              and i.financial_view_revision=wc.financial_view_revision and i.metadata_version=md.metadata_version
              and (d.candidate_id,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,d.valuation_policy_id,d.valuation_date)
                is not distinct from(c.id,g.target_pay_channel,case when g.target_pay_channel='UMBRELLA' then u.id end,
                  case when g.target_pay_channel='UMBRELLA' then u.vat_chargeable end,r.valuation_policy_id,v_pay_date)
              and active.id=d.id and active.decision_version=d.decision_version);
      end if;
      v_value:=v_value||jsonb_build_object('rate_set_pay_channel',v_badge);
    elsif p_kind='GROUP_LINES' then
      v_chosen_component:=null;v_chosen_leaf:=null;v_badge:=null;
      if v_value->>'section'='READY' and v_value->>'tax_treatment'='TAXABLE' and v_value->>'direction'='PAYMENT'
        and v_value->>'selectable'='true' and v_value->>'is_selected'='false' and v_value->>'amount_known'='true'
        and v_value->>'portion_kind' in ('AVAILABLE','ZERO') and v_value->>'work_id' is not null then
        select i.*,g.complete as group_complete,g.dirty as group_dirty,g.progress_state as group_progress,
          g.current_revision_id as group_revision,g.chosen_display_pay_date,g.chosen_display_date_sensitive,
          w.current_revision_id,w.applied_revision_id,w.approval_state,a.tax_treatment as native_tax
          into v_native from private.bpay_next_workbench_item i
          join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id and g.work_id=i.work_id
          join private.bpay_next_work w on w.id=i.work_id and w.candidate_id=i.candidate_id
          join private.bpay_next_approved_line a on a.id=i.approved_line_id and a.revision_id=i.revision_id and a.component_key=i.component_key
          where i.id=(v_value->>'item_id')::uuid and i.group_key=v_value->>'group_key'
            and i.work_id=(v_value->>'work_id')::uuid and i.revision_id=(v_value->>'revision_id')::uuid
            and i.approved_line_id=(v_value->>'approved_line_id')::uuid;
        if not found or not v_native.group_complete or v_native.group_dirty or v_native.group_progress<>'CURRENT'
          or v_native.group_revision is distinct from v_native.revision_id or v_native.current_revision_id is distinct from v_native.revision_id
          or v_native.applied_revision_id is distinct from v_native.revision_id or v_native.approval_state<>'APPROVED'
          or v_native.native_tax<>'TAXABLE'
          or v_native.chosen_display_date_sensitive and v_native.chosen_display_pay_date is distinct from v_pay_date then
          raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
        if exists(select 1 from private.bpay_next_component_resolution d where d.work_id=v_native.work_id and d.state='ACTIVE'
          and (d.based_on_revision_id=v_native.revision_id or d.published_revision_id=v_native.revision_id)
          and (case when d.based_on_revision_id=v_native.revision_id then d.approved_line_id else d.published_line_id end)=v_native.approved_line_id) then
          select true,x into v_cached,v_cache_entry from unnest(v_quote_cache) x
            where x->>'work_id'=v_native.work_id::text and x->>'revision_id'=v_native.revision_id::text
              and x->>'approved_line_id'=v_native.approved_line_id::text;
          if v_cached is not true then
            with native as materialized(select i.* from private.bpay_next_workbench_item i
              where i.group_key=v_native.group_key and i.candidate_id=v_native.candidate_id and i.work_id=v_native.work_id
                and i.revision_id=v_native.revision_id and i.approved_line_id=v_native.approved_line_id and i.component_key=v_native.component_key
                and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
                and i.direction='PAYMENT' and i.portion_kind in ('AVAILABLE','ZERO')
              order by i.member_kind,i.member_key,i.id limit 10001)
            select array_agg('WORK:'||i.work_id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key
              order by i.member_kind,i.member_key,i.id),sum(i.included_source_ex_vat) into v_keys,v_source from native i;
            if cardinality(v_keys)>10000 then
              raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
            v_quote:=private.bpay_next_current_component_resolution_quote_v1(v_native.work_id,v_native.revision_id,
              v_native.approved_line_id,v_pay_date,v_keys,null);
            v_cache_entry:=jsonb_build_object('work_id',v_native.work_id,'revision_id',v_native.revision_id,
              'approved_line_id',v_native.approved_line_id,'source',v_source,'quote',v_quote);
            v_cache_bytes:=v_cache_bytes+octet_length(v_cache_entry::text)+2;
            if v_cache_bytes>8388608 or cardinality(v_quote_cache)>=p_limit then
              raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
            v_quote_cache:=array_append(v_quote_cache,v_cache_entry);
          end if;
          v_quote:=nullif(v_cache_entry->'quote','null'::jsonb);v_source:=(v_cache_entry->>'source')::numeric;
          if v_quote is not null then
            if v_value->>'component_amount_known' is distinct from 'true' or v_value->>'component_ready_amount' is null
              or v_source is null or v_quote->'component_quote'->>'ex' is null then
              raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
            v_chosen_component:=(v_value->>'component_ready_amount')::numeric+(v_quote->'component_quote'->>'ex')::numeric-v_source;
            if v_native.member_kind='WORK_COMPONENT' then v_chosen_leaf:=(v_quote->'component_quote'->>'ex')::numeric;
            elsif v_native.included_source_ex_vat=0 then v_chosen_leaf:=0;
            else
              -- LAG sees the complete native prefix, including predecessors off this page.
              with prefixes as materialized(select x,ordinality,
                (x->'quote'->>'ex')::numeric-coalesce(lag((x->'quote'->>'ex')::numeric) over(order by ordinality),0) ex
                from jsonb_array_elements(v_quote->'prefix_targets') with ordinality p(x,ordinality))
              select count(*),min(ex) into v_prefix_count,v_chosen_leaf from prefixes
                where x->>'approved_segment_id'=v_native.approved_segment_id::text and x->>'segment_key'=v_native.segment_key;
              if v_prefix_count<>1 or v_chosen_leaf is null then
                raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
            end if;
            v_badge:=v_native.target_pay_channel;
          end if;
        end if;
      end if;
      v_value:=v_value||jsonb_build_object('chosen_component_ready_amount',v_chosen_component::text,
        'chosen_leaf_amount',v_chosen_leaf::text,'rate_set_pay_channel',v_badge);
    end if;
    if pg_catalog.octet_length(v_value::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    if p_kind in ('CANDIDATES','CANDIDATE_GROUPS','GROUP_LINES') then
     v_value:=private.bpay_next_selection_memory_row_v1(p_kind,v_value,p_scope,v_baseline);end if;
    if v_refresh_before then v_rows:=pg_catalog.jsonb_build_array(v_value)||v_rows;if v_last is null then v_last:=v_row.row_key;end if;
    else v_rows:=v_rows||pg_catalog.jsonb_build_array(v_value);v_last:=v_row.row_key;end if;v_n:=v_n+1;
    if pg_catalog.octet_length(v_rows::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  end loop;
  if v_refresh_before and v_n>0 then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete,'last_key',v_last,
    'affected_candidate_count',case when p_kind='ISSUE_DETAIL' then v_affected::text else null end);
end
$function$;

alter function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer) from public,anon,authenticated,service_role;
create or replace function public.bpay_next_workbench_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
  v_source_receipt private.weekly_source_entitlement_publication_receipts%rowtype;v_source_member record;v_source_no integer;v_source_count integer;
  v_initial_a public.weekly_source_root_authorisations%rowtype;v_initial_t public.timesheets%rowtype;
  v_initial_f public.timesheets_financials%rowtype;v_initial_w private.bpay_next_work%rowtype;
  v_initial_r private.bpay_next_work_revision%rowtype;v_initial_p private.bpay_next_publication%rowtype;
  v_initial_c public.contracts%rowtype;v_initial_line private.bpay_next_approved_line%rowtype;
  v_initial_inventory jsonb;v_initial_basis jsonb;v_initial_origin jsonb;v_initial_details jsonb;
  v_initial_cert jsonb;v_initial_component jsonb;v_initial_n integer;
  v_est jsonb;v_descriptor jsonb;v_est_intent jsonb;v_est_scope jsonb;v_versions jsonb;v_est_leaves jsonb;v_est_instructions jsonb:='[]';
  v_est_plan jsonb;v_est_rows jsonb:='[]';v_est_row jsonb;v_est_key jsonb;v_est_last jsonb;v_est_bound jsonb;
  v_est_candidate public.candidates%rowtype;v_est_position private.bpay_next_position%rowtype;
  v_est_week private.bpay_next_worker_week%rowtype;v_est_binding private.bpay_next_source_component_binding_version%rowtype;
  v_est_work private.bpay_next_work%rowtype;v_est_revision private.bpay_next_work_revision%rowtype;
  v_est_line private.bpay_next_approved_line%rowtype;v_est_original_revision uuid;v_est_original_line uuid;
  v_est_choice jsonb;v_est_choice_versions jsonb[]:=array[]::jsonb[];v_est_vat boolean;
  v_est_window uuid;v_est_vat_pct numeric;v_est_window_count bigint;v_est_amounts jsonb;
  v_est_record record;v_est_value record;v_est_source numeric:=0;v_est_gross numeric:=0;v_est_h numeric:=0;
  v_est_gross_add numeric:=0;v_est_gross_deduct numeric:=0;v_est_net_add numeric:=0;v_est_net_deduct numeric:=0;
  v_est_financial bigint;v_est_week_start date;v_est_count bigint;v_est_seen integer:=0;
  v_est_work_ids uuid[];v_est_components uuid[];v_est_wait_components uuid[];v_est_all_overpayment boolean;v_est_state text:='ESTIMATE';
  v_actor uuid;v_kind text;v_scope jsonb;v_context jsonb;v_after jsonb;v_cursor jsonb;
  v_page jsonb;v_response jsonb;v_read_scope jsonb;v_ns jsonb;v_memory jsonb;v_baseline uuid;v_revision private.bpay_next_workbench_revision%rowtype;
  v_epoch bigint;v_agency uuid;v_date_context jsonb;v_limit integer;v_error text;
  v_refresh boolean:=false;v_refresh_before boolean:=false;v_key_count integer;v_key jsonb;v_sort text;v_text text;v_keys text[];v_count_selection jsonb;v_progress jsonb;
  v_wait_row jsonb;v_wait_rows jsonb;v_wait_origin record;
begin
  -- STABLE reads share a single statement snapshot. No volatile authority or
  -- FOR SHARE helper; these are the existing service/active-admin predicates.
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  -- WB29 true read: finite native context and explicit incoming Preview only.
  if p_request->>'kind'='RESOLUTION_CONTEXT' then
    if jsonb_typeof(p_request) is distinct from 'object' or octet_length(p_request::text)>32768
      or not(p_request?&array['actor_user_id','kind','resolution'])
      or p_request-array['actor_user_id','kind','resolution']<>'{}'::jsonb
      or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
      or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_REQUEST_INVALID';end if;
    return private.bpay_next_resolution_context_v1((p_request->>'actor_user_id')::uuid,p_request->'resolution');
  end if;
  -- WB-39 read-only native snooze management; original financial owners retained.
  if p_request->>'kind'='SNOOZES' then
    if jsonb_typeof(p_request)<>'object' or not(p_request?&array['actor_user_id','kind'])
      or jsonb_typeof(p_request->'actor_user_id')<>'string'
      or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    return private.bpay_next_snooze_management_page_v1((p_request->>'actor_user_id')::uuid,p_request-array['actor_user_id','kind']);
  end if;
  -- WB-38 true read: context only; acknowledgement issuance remains W2.
  if p_request->>'kind'='SNOOZE_CHECK' then
    if jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>8192
      or not(p_request ?& array['actor_user_id','kind','snooze'])
      or p_request-array['actor_user_id','kind','snooze']<>'{}'::jsonb
      or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
      or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';end if;
    v_actor:=(p_request->>'actor_user_id')::uuid;
    return jsonb_build_object('ok',true,'kind','SNOOZE_CHECK',
      'context',private.bpay_next_snooze_context_v1(v_actor,p_request->'snooze'));
  end if;
  p_request:=p_request||jsonb_build_object('count_selection',coalesce(p_request->'count_selection','null'::jsonb),
    'refresh_anchor',coalesce(p_request->'refresh_anchor','null'::jsonb),'progress_after',coalesce(p_request->'progress_after','null'::jsonb),'selection_memory',coalesce(p_request->'selection_memory','null'::jsonb),'estimate',coalesce(p_request->'estimate','null'::jsonb));
  v_keys:=array['actor_user_id','kind','candidate_id','client_id','channel','section',
    'search','sort','group_key','item_id','reason_code','after','refresh_anchor','limit','count_selection','progress_after','selection_memory','estimate'];
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
      'CANDIDATE_GROUPS','GROUP_LINES','CANDIDATE_ESTIMATE','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS')
    or jsonb_typeof(p_request->'channel')<>'string' or p_request->>'channel' not in ('ALL','PAYE','UMBRELLA')
    or jsonb_typeof(p_request->'section')<>'string' or p_request->>'section' not in ('ALL','READY','ACTION','BLOCKED','WAITING')
    or jsonb_typeof(p_request->'sort')<>'string' or (v_kind='EXPORT_ROWS' and v_sort not in ('C_NAME_ASC','C_NAME_DESC','C_AMOUNT_ASC','C_AMOUNT_DESC','C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC','C_STATUS_ASC','C_STATUS_DESC','G_PAYMENT_ASC','G_PAYMENT_DESC','G_CLIENT_ASC','G_CLIENT_DESC','G_CHANNEL_ASC','G_CHANNEL_DESC','G_AMOUNT_ASC','G_AMOUNT_DESC','G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC','G_REASON_ASC','G_REASON_DESC'))
    or (v_kind<>'EXPORT_ROWS' and (false or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES') and v_sort<>'NAME_ASC')
    or (v_sort like 'CLIENT_%' or v_sort like 'CHANNEL_%') and v_kind<>'CANDIDATE_GROUPS'
    or (v_kind='CANDIDATE_GROUPS' and v_sort='NAME_DESC')
))
    or jsonb_typeof(p_request->'search')<>'string' or length(p_request->>'search')>200
    or octet_length(p_request->>'search')>800
    or btrim(p_request->>'search')<>p_request->>'search'
    or jsonb_typeof(p_request->'limit')<>'number' or p_request->>'limit'!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->'refresh_anchor'<>'null'::jsonb and (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES') or p_request->'after'<>'null'::jsonb) then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
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
  if (v_kind in ('CANDIDATE_GROUPS','GROUP_LINES','CANDIDATE_ESTIMATE') and p_request->'candidate_id'='null'::jsonb)
    or (v_kind in ('GROUP_LINES','ISSUE_DETAIL') and p_request->'group_key'='null'::jsonb)
    or (v_kind='ISSUE_DETAIL' and p_request->'reason_code'='null'::jsonb)
    or (v_kind not in ('GROUP_LINES','ISSUE_DETAIL','CANDIDATE_GROUPS') and (p_request->'group_key'<>'null'::jsonb or p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)
    or (v_kind='CANDIDATE_GROUPS' and (p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)))
    or (v_kind not in ('CANDIDATES','PICK_CANDIDATES','PICK_CLIENTS','ACTION_REQUIRED','BLOCKED','EXPORT_ROWS') and p_request->>'search'<>'')
    or (v_kind='HEADER' and p_request->'after'<>'null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_count_selection:=p_request->'count_selection';
  if v_count_selection ? 'intent' then
   if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
    or not(v_count_selection ?& array['intent','memory_baseline_run_id'])
    or v_count_selection-array['intent','memory_baseline_run_id']<>'{}'::jsonb
    or not private.bpay_next_selection_memory_intent_valid_v1(v_count_selection->'intent') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  else
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
  end if;
  if p_request->'progress_after'<>'null'::jsonb and v_kind<>'HEADER' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_scope:=p_request-array['actor_user_id','kind','after','refresh_anchor','limit','progress_after','estimate'];
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
  if v_kind='HEADER' then
    v_progress:=private.bpay_next_workbench_job_progress_v1(v_context,p_request->'progress_after',v_limit);
  end if;
  if v_revision.adoption_state<>'CURRENT' or v_revision.dirty_candidate_count<>0
    or v_revision.dirty_group_count<>0 or v_revision.pending_task_count<>0
    or exists(select 1 from private.bpay_next_workbench_group g where g.group_kind='WORK' and g.chosen_display_date_sensitive and g.complete and not g.dirty
      and g.progress_state='CURRENT' and (g.ready_count>0 or g.action_count>0)
      and g.chosen_display_pay_date is null limit 1)
    or exists(select 1 from private.bpay_next_workbench_group g where g.group_kind='WORK' and g.chosen_display_date_sensitive and g.complete and not g.dirty
      and g.progress_state='CURRENT' and (g.ready_count>0 or g.action_count>0)
      and g.chosen_display_pay_date<(v_date_context->>'current_official_pay_date')::date limit 1)
    or exists(select 1 from private.bpay_next_workbench_group g where g.group_kind='WORK' and g.chosen_display_date_sensitive and g.complete and not g.dirty
      and g.progress_state='CURRENT' and (g.ready_count>0 or g.action_count>0)
      and g.chosen_display_pay_date>(v_date_context->>'current_official_pay_date')::date limit 1) then
    v_response:=jsonb_build_object('ok',false,'error_code','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT',
      'retryable',true,'reload_required',false,'workbench_revision',v_revision.workbench_revision::text,
      'complete',false,'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
      'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
    if v_kind='HEADER' then v_response:=v_response||jsonb_build_object('job_progress',v_progress);end if;
    if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    return v_response;
  end if;
  v_ns:=jsonb_build_object('agency_id',v_agency,'owner_epoch',v_epoch::text,'actor_user_id',v_actor,
   'official_pay_date',v_date_context->>'current_official_pay_date','scope',jsonb_build_object(
    'candidate_id',v_scope->'candidate_id','client_id',v_scope->'client_id','channel',v_scope->'channel'));
  v_baseline:=private.bpay_next_selection_memory_resolve_v1(v_ns,p_request->'selection_memory');
  v_memory:=jsonb_build_object('run_id',v_baseline,'scope',coalesce(nullif(p_request->'selection_memory'->'scope','null'::jsonb),v_ns->'scope'));
  if v_count_selection ? 'intent' and v_count_selection->'memory_baseline_run_id' is distinct from v_memory->'run_id' then
   raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  if v_count_selection ? 'intent' then
   -- Same native identities as the current memory-aware Create owner.
   -- Component-only edits inherit defaults; supplied Work parents must match.
   if exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'candidates') chosen
     where not exists(select 1 from public.candidates candidate where candidate.id=(chosen->>'candidate_id')::uuid))
    or exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'components') c(value)
     where case when exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'works') work
       where work->>'work_id'=c.value->>'work_id') then not exists(
        select 1 from jsonb_array_elements(v_count_selection->'intent'->'works') work
         where work->>'work_id'=c.value->>'work_id' and work->>'candidate_id'=c.value->>'candidate_id'
          and work->>'expected_revision_id'=c.value->>'expected_revision_id')
      else not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision r on r.work_id=w.id
       join private.bpay_next_workbench_item i on i.work_id=w.id and i.revision_id=r.id
       join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
       where w.id=(c.value->>'work_id')::uuid and w.candidate_id=(c.value->>'candidate_id')::uuid
        and r.id=(c.value->>'expected_revision_id')::uuid and w.current_revision_id=r.id and w.applied_revision_id=r.id
        and i.approved_line_id=(c.value->>'approved_line_id')::uuid and i.candidate_id=w.candidate_id
        and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_memory->'scope'->>'candidate_id' is null or i.candidate_id=(v_memory->'scope'->>'candidate_id')::uuid)
        and (v_memory->'scope'->>'client_id' is null or i.client_id=(v_memory->'scope'->>'client_id')::uuid)
        and (v_memory->'scope'->>'channel'='ALL' or i.target_pay_channel=v_memory->'scope'->>'channel')) end) then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
   if exists(select 1 from unnest(array['works','components']) kind
    cross join lateral jsonb_array_elements(v_count_selection->'intent'->kind) chosen
    where not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision wr on wr.work_id=w.id
     where w.id=(chosen->>'work_id')::uuid and w.candidate_id=(chosen->>'candidate_id')::uuid
      and wr.id=(chosen->>'expected_revision_id')::uuid and w.current_revision_id=wr.id and w.applied_revision_id=wr.id
      and (kind='works' or exists(select 1 from private.bpay_next_approved_line l where l.id=(chosen->>'approved_line_id')::uuid and l.revision_id=wr.id))))
    or exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'cases') chosen
     where not exists(select 1 from private.bpay_next_case_component cc where cc.id=(chosen->>'case_component_id')::uuid and cc.candidate_id=(chosen->>'candidate_id')::uuid)) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  end if;
  v_context:=v_context||jsonb_build_object('selection_memory',v_memory);
  if v_kind='CANDIDATE_ESTIMATE' then
    v_est:=p_request->'estimate';v_descriptor:=v_est->'descriptor';
    if p_request->>'section'<>'READY' or p_request->'count_selection'<>'null'::jsonb
      or jsonb_typeof(v_est) is distinct from 'object'
      or not(v_est ?& array['descriptor','expected_financial_view_revision','versions','verify_only'])
      or v_est-array['descriptor','expected_financial_view_revision','versions','verify_only']<>'{}'::jsonb
      or jsonb_typeof(v_est->'verify_only') is distinct from 'boolean'
      or (v_est->'versions'='null'::jsonb and (v_est->'verify_only'='true'::jsonb or p_request->'after'<>'null'::jsonb))
      or jsonb_typeof(v_est->'expected_financial_view_revision') is distinct from 'string'
      or v_est->>'expected_financial_view_revision'!~'^(0|[1-9][0-9]{0,18})$'
      or (v_est->'versions'<>'null'::jsonb and (jsonb_typeof(v_est->'versions')<>'array' or jsonb_array_length(v_est->'versions')>8000))
      or jsonb_typeof(v_descriptor) is distinct from 'object'
      or not(v_descriptor ?& array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id'])
      or v_descriptor-array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id']<>'{}'::jsonb
      or not private.bpay_next_selection_memory_intent_valid_v1(v_descriptor->'intent')
      or jsonb_typeof(v_descriptor->'revision') is distinct from 'number'
      or v_descriptor->>'revision'!~'^(0|[1-9][0-9]{0,15})$' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_descriptor->'namespace' is distinct from jsonb_set(v_ns,'{scope}',v_memory->'scope')
      or v_descriptor->'memory_baseline_run_id' is distinct from v_memory->'run_id' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    select c.* into strict v_est_candidate from public.candidates c where c.id=(p_request->>'candidate_id')::uuid;
    select c.financial_view_revision into strict v_est_financial from private.bpay_next_worker_control c where c.candidate_id=v_est_candidate.id;
    v_est_week_start:=(v_date_context->>'current_official_pay_date')::date-
      (extract(isodow from (v_date_context->>'current_official_pay_date')::date)::integer-1);
    if v_descriptor->>'workbench_revision' is distinct from v_revision.workbench_revision::text
      or v_descriptor->>'adoption_revision' is distinct from v_revision.adoption_revision::text
      or v_est->>'expected_financial_view_revision' is distinct from v_est_financial::text then v_est_state:='OUTDATED';end if;
    -- Bound the current indexed Candidate membership BEFORE choices materialises LEAF.
    select count(*) into v_est_count from (select 1 from private.bpay_next_workbench_item i
      join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
      where i.candidate_id=v_est_candidate.id and
        ((i.section='READY' and i.selectable) or
          (i.section='ACTION' and i.reason_code='ORIGIN_UNAUTHORISED_WAIT' and i.member_kind='CASE_COMPONENT'))
        and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_scope->>'client_id' is null or i.client_id=(v_scope->>'client_id')::uuid)
        and (v_scope->>'channel'='ALL' or i.target_pay_channel=v_scope->>'channel') limit 1001) bounded;
    if v_est_count>1000 then v_est_state:='PENDING';end if;
    if v_est_state='ESTIMATE' then
      begin
        -- Only this Candidate's native overrides can affect their estimate.
        v_est_intent:=jsonb_build_object('mode',v_descriptor->'intent'->'mode');
        for v_est_record in select unnest(array['candidates','works','components','cases']) kind loop
          v_est_intent:=v_est_intent||jsonb_build_object(v_est_record.kind,(select coalesce(jsonb_agg(x.value),'[]'::jsonb)
            from jsonb_array_elements(v_descriptor->'intent'->v_est_record.kind) x(value) where x.value->>'candidate_id'=v_est_candidate.id::text));
        end loop;
        with chosen as materialized(select * from private.bpay_next_selection_memory_choices_v1(
          v_scope,v_baseline,v_est_intent) where selected),
        leaves as materialized(select * from private.bpay_next_selection_memory_leaves_v1(v_scope))
        select coalesce(jsonb_agg(jsonb_build_object('leaf_key',l.leaf_key,'candidate_id',l.candidate_id,'work_id',l.work_id,'revision_id',l.revision_id,'approved_line_id',l.approved_line_id,'case_component_id',l.case_component_id,'basis',l.basis) order by l.leaf_key),'[]'::jsonb) into v_est_leaves from leaves l
          join chosen c on c.candidate_id=l.candidate_id and
            (c.work_id=l.work_id and c.revision_id=l.revision_id and c.approved_line_id=l.approved_line_id
              or c.case_component_id=l.case_component_id);
        if jsonb_array_length(v_est_leaves)>1000 then
          raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        select coalesce(array_agg(distinct (l->>'work_id')::uuid) filter(where l->>'work_id' is not null),'{}'),
          coalesce(array_agg(distinct (l->>'case_component_id')::uuid) filter(where l->>'case_component_id' is not null),'{}')
          into v_est_work_ids,v_est_components from jsonb_array_elements(v_est_leaves) l;
        -- WB41 information only: paused recoveries are not choices or LEAF
        -- admission. Their original native versions join the existing estimate
        -- verification and plan, so Outstanding and zero recoverable stay distinct.
        select coalesce(array_agg(bounded.case_component_id),'{}') into v_est_wait_components from (
          select i.case_component_id from private.bpay_next_workbench_item i
            join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
            where i.candidate_id=v_est_candidate.id and i.section='ACTION'
              and i.reason_code='ORIGIN_UNAUTHORISED_WAIT' and i.member_kind='CASE_COMPONENT'
              and i.portion_kind='AVAILABLE' and i.amount_known and not i.is_selected
              and i.accepted_run_id is null and i.case_component_id is not null
              and g.complete and not g.dirty and g.progress_state='CURRENT' and i.progress_state='CURRENT'
              and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
              and (v_scope->>'client_id' is null or i.client_id=(v_scope->>'client_id')::uuid)
              and (v_scope->>'channel'='ALL' or i.target_pay_channel=v_scope->>'channel')
          limit 1001) bounded;
        select coalesce(array_agg(distinct id),'{}') into v_est_components
          from unnest(v_est_components||v_est_wait_components) id;
        if cardinality(v_est_work_ids)+cardinality(v_est_components)>1000 then
          raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        select count(*) into v_est_count from (select 1 from private.bpay_next_worker_week_contribution x
          where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
            and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date limit 1001) bounded;
        if v_est_count>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        if exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'works') chosen
          where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_work w
            where w.id=(chosen->>'work_id')::uuid and w.candidate_id=v_est_candidate.id
              and w.current_revision_id=(chosen->>'expected_revision_id')::uuid and w.applied_revision_id=w.current_revision_id))
          or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'components') chosen
            where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_work w
              join private.bpay_next_approved_line l on l.revision_id=w.current_revision_id
              where w.id=(chosen->>'work_id')::uuid and w.candidate_id=v_est_candidate.id
                and l.id=(chosen->>'approved_line_id')::uuid and w.current_revision_id=(chosen->>'expected_revision_id')::uuid
                and w.applied_revision_id=w.current_revision_id))
          or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'cases') chosen
            where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_case_component cc
              where cc.id=(chosen->>'case_component_id')::uuid and cc.candidate_id=v_est_candidate.id)) then
          raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        select count(*) into v_est_count from (select 1 from private.bpay_next_approved_line l
          join private.bpay_next_work w on w.current_revision_id=l.revision_id where w.id=any(v_est_work_ids) limit 1001) bounded;
        if v_est_count>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        if exists(select 1 from private.bpay_next_valuation_policy_window pw
          where pw.policy_id in (select r.valuation_policy_id from private.bpay_next_work_revision r join private.bpay_next_work w on w.current_revision_id=r.id where w.id=any(v_est_work_ids)
            union select o.valuation_policy_id from private.bpay_next_case_create_request o join private.bpay_next_case_component cc on cc.case_id=o.case_id where cc.id=any(v_est_components)
            union select o.original_valuation_policy_id from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components))
          and (v_date_context->>'current_official_pay_date')::date>=pw.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(pw.date_to,'infinity'::date)
          and pw.date_from=(select max(winner.date_from::date) from private.bpay_next_valuation_policy_window winner where winner.policy_id=pw.policy_id and (v_date_context->>'current_official_pay_date')::date>=winner.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(winner.date_to,'infinity'::date))
          group by pw.policy_id having count(*)>1) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        -- Native identities and versions only; no monetary carry is returned to the browser.
        if exists(select 1 from private.bpay_next_source_component_binding b
          join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
          where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
            and cardinality(receipt.head_ids) not between 1 and 2) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        with source_receipts as materialized(select distinct receipt.*,receipt.xmin::text as receipt_version
          from private.bpay_next_source_component_binding b join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
          where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)),
        source_members as materialized(select point.* from source_receipts receipt cross join lateral (
    select h.id,h.xmin::text as head_version,a.id as auth_id,a.xmin::text as auth_version,w.xmin::text as work_version,r.xmin::text as revision_version,p.xmin::text as publication_version,h.prior_head_id,w.id as work_id,r.id as revision_id,
           p.command_id,coalesce(p.predecessor_revision_id,
             case when p.command_id is null and w.current_revision_id is distinct from r.id
               then w.current_revision_id end) as predecessor_revision_id 
      from generate_subscripts(receipt.head_ids,1) member(member_no) join public.weekly_source_entitlement_heads h on h.id=receipt.head_ids[member.member_no]
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      join private.bpay_next_work w on w.original_timesheet_id=h.root_timesheet_id
        and w.booking_id=h.root_family_booking_id and w.candidate_id=h.candidate_id
        and w.contract_id=h.contract_id and w.week_ending_date=h.week_ending_date and w.work_kind='SOURCE'
      join private.bpay_next_work_revision r on r.work_id=w.id and r.source_head_id=h.id and r.source_event_id=h.id
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.source_kind in ('SOURCE','PROTECTED')
      left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
        and p.candidate_id=w.candidate_id
      where h.id=any(receipt.head_ids) and h.state='COMMITTED_CURRENT'
        and h.candidate_id=receipt.candidate_id and h.root_timesheet_id=receipt.member_root_ids[member.member_no]
        and h.root_family_booking_id=receipt.member_family_booking_ids[member.member_no]
        and h.root_timesheet_version=receipt.member_root_versions[member.member_no]
        and h.decision_bundle_id=receipt.decision_bundle_id and h.bundle_revision=receipt.bundle_revision
        and h.publication_receipt_digest=receipt.request_digest
        and h.scope_change_tx_token=receipt.scope_change_tx_token limit 3
        ) point),native as (
          select jsonb_build_array('C',c.id,c.xmin::text) value from public.candidates c where c.id=v_est_candidate.id
          union all select jsonb_build_array('U',u.id,u.xmin::text) from public.umbrellas u where u.id=v_est_candidate.umbrella_id
          union all select jsonb_build_array('W',w.id,w.xmin::text) from private.bpay_next_work w where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('R',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_work w on w.current_revision_id=r.id where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('P',p.work_id,p.component_key,p.xmin::text) from private.bpay_next_position p
            where p.work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l
              where l->>'work_id'=p.work_id::text and l->'basis'->>'component_key'=p.component_key)
          union all select jsonb_build_array('I',i.id,i.xmin::text) from private.bpay_next_workbench_item i
            where i.candidate_id=v_est_candidate.id and exists(select 1 from jsonb_array_elements(v_est_leaves) l
              where l->>'leaf_key'=case when i.work_id is not null then 'WORK:'||i.work_id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key
                else 'CASE:'||i.case_component_id::text end) and i.portion_kind in ('AVAILABLE','ZERO')
          union all select jsonb_build_array('FC',c.id,c.xmin::text) from private.bpay_next_finance_case c
            join private.bpay_next_case_component cc on cc.case_id=c.id where cc.id=any(v_est_components)
          union all select jsonb_build_array('CC',cc.id,cc.xmin::text) from private.bpay_next_case_component cc where cc.id=any(v_est_components)
          union all select jsonb_build_array('CR',r.id,r.xmin::text) from private.bpay_next_case_rule r
            join private.bpay_next_finance_case c on c.current_rule_id=r.id
            join private.bpay_next_case_component cc on cc.case_id=c.id where cc.id=any(v_est_components)
          union all select jsonb_build_array('CP',p.case_component_id,p.pay_week_start,p.xmin::text) from private.bpay_next_case_period p
            where p.case_component_id=any(v_est_components) and p.pay_week_start=v_est_week_start
          union all select jsonb_build_array('CO',o.id,o.xmin::text) from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OP',p.work_id,p.component_key,p.xmin::text) from private.bpay_next_position p
            join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OW',w.id,w.xmin::text) from private.bpay_next_work w
            join private.bpay_next_work_collection o on o.work_id=w.id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('SN',sn.id,sn.xmin::text) from private.bpay_next_snooze sn
            where sn.candidate_id=v_est_candidate.id and (sn.work_id=any(v_est_work_ids) or sn.case_component_id=any(v_est_components))
              and sn.control_sequence=(select max(s.control_sequence::bigint) from private.bpay_next_snooze s
                where (sn.work_id is not null and s.work_id=sn.work_id or sn.case_component_id is not null and s.case_component_id=sn.case_component_id))
          union all select jsonb_build_array('SB',b.canonical_work_id,b.canonical_component_key,b.xmin::text)
            from private.bpay_next_source_component_binding b where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('SV',bv.id,bv.xmin::text) from private.bpay_next_source_component_binding_version bv
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('PL',p.command_id,p.xmin::text) from private.bpay_next_publication p
            join private.bpay_next_source_component_binding_version bv on p.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BC',c.id,c.xmin::text) from private.bpay_next_command c
            join private.bpay_next_source_component_binding_version bv on c.id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BJ',j.id,j.xmin::text) from private.bpay_next_job j
            join private.bpay_next_source_component_binding_version bv on j.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BM',m.command_id,m.candidate_id,m.xmin::text) from private.bpay_next_command_member m
            join private.bpay_next_source_component_binding_version bv on m.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('EB',d.decision_bundle_id,d.xmin::text) from public.weekly_source_entitlement_decision_bundles d
            join private.bpay_next_source_component_binding_version bv on bv.decision_bundle_id=d.decision_bundle_id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('RA',a.id,a.xmin::text) from public.weekly_source_root_authorisations a
            join private.bpay_next_source_component_binding_version bv on bv.initial_root_authorisation_id=a.id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('TS',t.timesheet_id,t.xmin::text) from public.timesheets t
            join private.bpay_next_source_component_binding_version bv on bv.initial_root_timesheet_id=t.timesheet_id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('TF',f.id,f.xmin::text) from public.timesheets_financials f
            join private.bpay_next_source_component_binding_version bv on bv.initial_financial_snapshot_id=f.id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('CT',c.id,c.xmin::text) from public.contracts c
            join private.bpay_next_work w on w.contract_id=c.id where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('SR',receipt.id,receipt.receipt_version) from source_receipts receipt
          union all select jsonb_build_array('SH',m.id,m.head_version) from source_members m
          union all select jsonb_build_array('SA',m.auth_id,m.auth_version) from source_members m
          union all select jsonb_build_array('SW',m.work_id,m.work_version) from source_members m
          union all select jsonb_build_array('SVR',m.revision_id,m.revision_version) from source_members m
          union all select jsonb_build_array('SP',m.command_id,m.publication_version) from source_members m
          union all select jsonb_build_array('OR',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_work_collection o on r.id=o.original_revision_id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('AR',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_position p on p.applied_revision_id=r.id join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OL',l.id,l.xmin::text) from private.bpay_next_approved_line l
            join private.bpay_next_work_collection o on o.original_approved_line_id=l.id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('AL',l.id,l.xmin::text) from private.bpay_next_approved_line l
            join private.bpay_next_position p on p.applied_revision_id=l.revision_id and p.component_key=l.component_key join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('CQ',o.command_id,o.xmin::text) from private.bpay_next_case_create_request o
            join private.bpay_next_case_component cc on cc.case_id=o.case_id where cc.id=any(v_est_components)
          union all select jsonb_build_array('PW',pw.policy_id,pw.source_window_id,pw.xmin::text) from private.bpay_next_valuation_policy_window pw
            where (v_date_context->>'current_official_pay_date')::date>=pw.date_from
              and (v_date_context->>'current_official_pay_date')::date<=coalesce(pw.date_to,'infinity'::date)
              and pw.date_from=(select max(winner.date_from::date) from private.bpay_next_valuation_policy_window winner where winner.policy_id=pw.policy_id and (v_date_context->>'current_official_pay_date')::date>=winner.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(winner.date_to,'infinity'::date))
              and (exists(select 1 from private.bpay_next_work_revision r join private.bpay_next_work w on w.current_revision_id=r.id
                where w.id=any(v_est_work_ids) and r.valuation_policy_id=pw.policy_id)
                or exists(select 1 from private.bpay_next_case_create_request o join private.bpay_next_case_component cc on cc.case_id=o.case_id
                  where cc.id=any(v_est_components) and o.valuation_policy_id=pw.policy_id)
                or exists(select 1 from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components)
                  and o.original_valuation_policy_id=pw.policy_id))
          union all select jsonb_build_array('WK',w.candidate_id,w.pay_week_start,w.xmin::text) from private.bpay_next_worker_week w
            where w.candidate_id=v_est_candidate.id and w.pay_week_start=v_est_week_start
          union all select jsonb_build_array('WC',x.id,x.xmin::text) from private.bpay_next_worker_week_contribution x
            where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
              and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date
        ) select coalesce(jsonb_agg(value order by value::text),'[]'::jsonb) into v_versions from (select distinct value from native) exact_versions;
        if v_est->'versions'<>'null'::jsonb and
          (select coalesce(jsonb_agg(x.value order by x.value::text),'[]') from jsonb_array_elements(v_est->'versions') x
            where x.value->>0 is distinct from 'RATE_DECISION') is distinct from v_versions then
          v_est_state:='OUTDATED';
        else
          for v_est_record in select (l->>'work_id')::uuid work_id,(l->>'revision_id')::uuid revision_id,
              l->'basis'->>'component_key' component_key,(l->>'approved_line_id')::uuid approved_line_id,
              sum((l->'basis'->>'included_source_ex_vat')::numeric) included_source,
              array_agg(l->>'leaf_key' order by l->>'leaf_key') selected_leaf_keys,
              count(*) segment_count,bool_and(coalesce((l->'basis'->>'pay_hold')::boolean,false)=false) not_held
            from jsonb_array_elements(v_est_leaves) l where l->>'work_id' is not null
            group by l->>'work_id',l->>'revision_id',l->'basis'->>'component_key',l->>'approved_line_id' loop
            select p.* into strict v_est_position from private.bpay_next_position p
              join private.bpay_next_work w on w.id=p.work_id and w.current_revision_id=v_est_record.revision_id
                and w.applied_revision_id=v_est_record.revision_id and w.approval_state='APPROVED'
              where p.work_id=v_est_record.work_id and p.component_key=v_est_record.component_key
                and p.applied_revision_id=v_est_record.revision_id;
            if private.bpay_next_work_wait_state_v1(v_est_record.work_id) or not v_est_record.not_held
              or v_est_record.included_source is null or v_est_record.included_source<0
              or exists(select 1 from private.bpay_next_workbench_item i
                join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
                where i.work_id=v_est_record.work_id and i.component_key=v_est_record.component_key
                  and i.portion_kind in ('AVAILABLE','ZERO') and (i.financial_view_revision<>v_est_financial
                    or g.financial_view_revision<>v_est_financial or not g.complete or g.dirty or i.progress_state<>'CURRENT'))
              or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'works') w
                where w->>'work_id'=v_est_record.work_id::text and w->>'expected_revision_id'<>v_est_record.revision_id::text)
              or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'components') c
                where c->>'work_id'=v_est_record.work_id::text and c->>'expected_revision_id'<>v_est_record.revision_id::text) then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            select x.* into strict v_est_work from private.bpay_next_work x where x.id=v_est_record.work_id;
            select x.* into strict v_est_revision from private.bpay_next_work_revision x
              where x.id=v_est_record.revision_id and x.work_id=v_est_work.id;
            select x.* into strict v_est_line from private.bpay_next_approved_line x
              where x.id=v_est_record.approved_line_id and x.revision_id=v_est_revision.id
                and x.component_key=v_est_record.component_key;
            v_est_original_revision:=private.bpay_next_treatment_approval_revision_v1(v_est_revision.id);
            select x.id into strict v_est_original_line from private.bpay_next_approved_line x
              where x.revision_id=v_est_original_revision and x.component_key=v_est_line.component_key;
            if v_est_revision.approved_at_utc is null or v_est_revision.sealed_at_utc is null
              or v_est_position.approved_source_ex_vat is distinct from v_est_line.source_pay_ex_vat then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            if v_est_work.work_kind='SOURCE' then
              select bv.* into strict v_est_binding from private.bpay_next_source_component_binding b
                join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
                where b.canonical_work_id=v_est_record.work_id and b.canonical_component_key=v_est_record.component_key
                  and bv.approval_work_id=v_est_record.work_id and bv.approval_revision_id=v_est_original_revision
                  and bv.approved_line_id=v_est_original_line and bv.component_state='PRESENT'
                  and bv.candidate_id=v_est_candidate.id and bv.approval_component_key=v_est_record.component_key;
              -- Exact existing bound snapshot admission, without locks or a new certificate owner.
              if v_est_binding.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
  select x.* into strict v_initial_a from public.weekly_source_root_authorisations x where x.id=v_est_binding.initial_root_authorisation_id;
  select x.* into strict v_initial_t from public.timesheets x where x.timesheet_id=v_initial_a.root_timesheet_id;
  select x.* into strict v_initial_c from public.contracts x where x.id=v_initial_t.contract_id;
  select x.* into strict v_initial_r from private.bpay_next_work_revision x where x.source_event_id=v_initial_a.id;
  select x.* into strict v_initial_w from private.bpay_next_work x where x.id=v_initial_r.work_id;
  select x.* into strict v_initial_f from public.timesheets_financials x where x.id=v_initial_r.financial_snapshot_id;
  select x.* into strict v_initial_p from private.bpay_next_publication x
    where x.command_id=private.bpay_next_source_command_id_v1(v_initial_a.id);
  if v_initial_a.withdrawn_at_utc is not null or v_initial_a.current_entitlement_head_id is not null
     or exists(select 1 from public.weekly_source_entitlement_heads h where btrim(h.root_family_booking_id)=btrim(v_initial_t.booking_id))
     or v_initial_t.is_current is distinct from true or v_initial_t.authorised_at_server is null
     or v_initial_t.revoked_at is not null or v_initial_t.archived_at_utc is not null
     or (v_initial_a.family_booking_id,v_initial_a.timesheet_version) is distinct from (v_initial_t.booking_id,v_initial_t.version)
     or (v_initial_w.work_kind,v_initial_w.candidate_id,v_initial_w.contract_id,v_initial_w.original_timesheet_id,v_initial_w.booking_id,v_initial_w.week_ending_date,private.bpay_next_treatment_approval_revision_v1(v_initial_w.current_revision_id))
       is distinct from ('SOURCE'::text,v_initial_c.candidate_id,v_initial_c.id,v_initial_t.timesheet_id,v_initial_t.booking_id,v_initial_t.week_ending_date,v_initial_r.id)
     or v_initial_w.approval_state is distinct from 'APPROVED'
     or (v_initial_r.source_kind,v_initial_r.source_head_id,v_initial_r.source_event_id,v_initial_r.physical_timesheet_id,v_initial_r.physical_timesheet_version,v_initial_r.week_ending_date)
       is distinct from ('SOURCE'::text,null::uuid,v_initial_a.id,v_initial_t.timesheet_id,v_initial_t.version,v_initial_t.week_ending_date)
     or v_initial_r.approved_at_utc is null or v_initial_r.sealed_at_utc is null
     or v_initial_f.is_current is distinct from true or v_initial_f.is_stale is distinct from false or v_initial_f.authorised_at_utc is null
     or (v_initial_f.timesheet_id,v_initial_f.timesheet_version,v_initial_f.candidate_id,v_initial_f.client_id)
       is distinct from (v_initial_t.timesheet_id,v_initial_t.version,v_initial_w.candidate_id,v_initial_c.client_id)
     or (v_initial_p.work_id,v_initial_p.revision_id,v_initial_p.candidate_id,v_initial_p.revision_no)
       is distinct from (v_initial_w.id,v_initial_r.id,v_initial_w.candidate_id,v_initial_r.revision_no)
     or not exists(select 1 from private.bpay_next_command command
       join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=v_initial_w.candidate_id
       where command.id=v_initial_p.command_id and command.command_kind='POSITION_APPLY'
         and command.module_epoch=v_epoch and command.status in ('SEALED','COMPLETE')
         and command.sealed_at_utc is not null and command.expected_member_count=1 and member.member_no=1
         -- INITIAL_METADATA_PHASE_V2_BEGIN: no fabricated enrollment/job.
         and ((command.status='SEALED' and command.enrolled_member_count=0
           and v_initial_p.status='QUEUED' and v_initial_p.phase='NEW' and v_initial_p.applied_at_utc is null
           and not exists(select 1 from private.bpay_next_job job where job.command_id=command.id))
          or (command.status='COMPLETE' and command.enrolled_member_count=1
           and v_initial_p.status='APPLIED' and v_initial_p.phase='DONE' and v_initial_p.applied_at_utc is not null
           and (select count(*) from private.bpay_next_job job where job.command_id=command.id)=1
           and exists(select 1 from private.bpay_next_job job
             join private.bpay_next_worker_control ctl on ctl.candidate_id=job.candidate_id
             where job.command_id=command.id and job.candidate_id=v_initial_w.candidate_id
               and job.job_kind='POSITION_APPLY' and job.command_sequence=command.agency_sequence
               and job.module_epoch=command.module_epoch and job.status='DONE' and job.phase='DONE'
               and job.owner_epoch>0 and job.owner_epoch<=ctl.active_owner_epoch
               and job.lease_nonce is null and job.lease_until_utc is null)))
         -- INITIAL_METADATA_PHASE_V2_END
       )
  then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;

  select x.* into strict v_initial_line from private.bpay_next_approved_line x where x.id=v_est_binding.approved_line_id;
  v_initial_details:=jsonb_build_object('segments',v_initial_f.invoice_breakdown_json->'segments','additional_units',v_initial_f.additional_units_json,
    'actual_schedule',v_initial_f.actual_schedule_json,'rate_source_refs',v_initial_f.rate_source_refs_json);
  if (v_est_binding.candidate_id,v_est_binding.canonical_work_id,v_est_binding.approval_work_id,v_est_binding.approval_revision_id,
      v_est_binding.initial_root_authorisation_id,v_est_binding.initial_financial_snapshot_id,v_est_binding.initial_root_timesheet_id,
      v_est_binding.initial_root_version,v_est_binding.initial_authorisation_generation,v_est_binding.initial_authorised_row_signature)
    is distinct from (v_initial_w.candidate_id,v_initial_w.id,v_initial_w.id,v_initial_r.id,v_initial_a.id,v_initial_f.id,
      v_initial_t.timesheet_id,v_initial_t.version,v_initial_a.authorisation_generation,v_initial_a.authorised_row_signature)
    or v_initial_r.source_inventory_digest is distinct from v_est_binding.initial_inventory_digest
    or (v_est_binding.approved_line_id,v_est_binding.classification_line_id,v_est_binding.canonical_component_key,v_est_binding.approval_component_key,
        v_est_binding.approval_source_channel,v_est_binding.tax_treatment,v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
      is distinct from (v_initial_line.id,v_initial_line.id,v_initial_line.component_key,v_initial_line.component_key,v_initial_r.source_pay_channel,
        v_initial_line.tax_treatment,v_initial_p.command_id,v_initial_w.id,v_initial_r.id)
    or (v_initial_line.revision_id,v_initial_line.source_component_id,v_initial_line.component_key,v_initial_line.evidence_ref)
      is distinct from (v_initial_r.id,v_est_binding.source_component_id,'SOURCE:'||v_est_binding.source_component_id::text,
        'source-component:'||v_est_binding.source_component_id::text||'#'||encode(v_est_binding.component_sha256,'hex'))
    or v_est_binding.initial_detail_digest is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',v_initial_details)
    or v_est_binding.initial_policy_digest is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',v_initial_f.policy_snapshot_json)
    or v_est_binding.binding_digest is distinct from sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v_est_binding.canonical_work_id,
      v_est_binding.canonical_component_key,v_est_binding.approval_revision_id,v_est_binding.approved_line_id,v_est_binding.initial_root_authorisation_id,
      encode(v_est_binding.initial_certificate_digest,'hex'),v_est_binding.publication_command_id_1::text,encode(v_est_binding.component_sha256,'hex'))::text,'UTF8'))
  then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;

              else
                select x.* into strict v_source_receipt from private.weekly_source_entitlement_publication_receipts x where x.id=v_est_binding.source_receipt_id;
                if cardinality(v_source_receipt.head_ids) not between 1 and 2 or not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
                  where b.decision_bundle_id=v_source_receipt.decision_bundle_id and b.bundle_revision=v_source_receipt.bundle_revision
                    and b.candidate_id=v_source_receipt.candidate_id and b.state='COMMITTED' and b.request_digest=v_source_receipt.request_digest
                    and b.decision_id=v_source_receipt.decision_id and b.decided_by_user_id=v_source_receipt.decided_by_user_id
                    and b.decision_bundle_id=v_est_binding.decision_bundle_id and b.bundle_revision=v_est_binding.bundle_revision) then
                  raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
                v_source_count:=0;
                for v_source_no in 1..cardinality(v_source_receipt.head_ids) loop
    select h.id,h.prior_head_id,w.id as work_id,r.id as revision_id,
           p.command_id,coalesce(p.predecessor_revision_id,
             case when p.command_id is null and w.current_revision_id is distinct from r.id
               then w.current_revision_id end) as predecessor_revision_id into strict v_source_member
      from public.weekly_source_entitlement_heads h
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      join private.bpay_next_work w on w.original_timesheet_id=h.root_timesheet_id
        and w.booking_id=h.root_family_booking_id and w.candidate_id=h.candidate_id
        and w.contract_id=h.contract_id and w.week_ending_date=h.week_ending_date and w.work_kind='SOURCE'
      join private.bpay_next_work_revision r on r.work_id=w.id and r.source_head_id=h.id and r.source_event_id=h.id
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.source_kind in ('SOURCE','PROTECTED')
      left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
        and p.candidate_id=w.candidate_id
      where h.id=v_source_receipt.head_ids[v_source_no] and h.state='COMMITTED_CURRENT'
        and h.candidate_id=v_source_receipt.candidate_id and h.root_timesheet_id=v_source_receipt.member_root_ids[v_source_no]
        and h.root_family_booking_id=v_source_receipt.member_family_booking_ids[v_source_no]
        and h.root_timesheet_version=v_source_receipt.member_root_versions[v_source_no]
        and h.decision_bundle_id=v_source_receipt.decision_bundle_id and h.bundle_revision=v_source_receipt.bundle_revision
        and h.publication_receipt_digest=v_source_receipt.request_digest
        and h.scope_change_tx_token=v_source_receipt.scope_change_tx_token;

                  if (v_source_member.command_id,v_source_member.work_id,v_source_member.revision_id) is not distinct from
                    (v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
                    or (v_est_binding.publication_command_id_2 is not null and (v_source_member.command_id,v_source_member.work_id,v_source_member.revision_id) is not distinct from
                      (v_est_binding.publication_command_id_2,v_est_binding.publication_work_id_2,v_est_binding.publication_revision_id_2)) then v_source_count:=v_source_count+1;end if;
                end loop;
                if v_source_count<>(case when v_est_binding.publication_command_id_2 is null then 1 else 2 end) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
              end if;
              if (select count(*) from private.bpay_next_publication p
                join private.bpay_next_work w on w.id=p.work_id
                where p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
                  and w.current_revision_id is not distinct from w.applied_revision_id
                  and private.bpay_next_treatment_approval_revision_v1(w.current_revision_id)=p.revision_id
                  and private.bpay_next_treatment_approval_revision_v1(w.applied_revision_id)=p.revision_id
                  and ((p.command_id,p.work_id,p.revision_id) is not distinct from
                    (v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
                    or (v_est_binding.publication_command_id_2 is not null and (p.command_id,p.work_id,p.revision_id) is not distinct from
                      (v_est_binding.publication_command_id_2,v_est_binding.publication_work_id_2,v_est_binding.publication_revision_id_2))))
                is distinct from (case when v_est_binding.publication_command_id_2 is null then 1 else 2 end)::bigint then
                raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
              if v_est_position.source_book_state is distinct from 'READY' or coalesce(v_est_position.source_book_revision,0)<1
                or v_est_position.source_basis_channel is distinct from v_est_position.source_book_seed_channel
                or v_est_binding.approval_source_channel is distinct from v_est_position.source_basis_channel
                or v_est_position.approved_source_ex_vat is distinct from (select l.source_pay_ex_vat from private.bpay_next_approved_line l where l.id=v_est_record.approved_line_id)
                or (v_est_position.native_realised_paye,v_est_position.native_held_paye,v_est_position.native_realised_umbrella,v_est_position.native_held_umbrella) is distinct from
                  (case when v_est_position.source_basis_channel='PAYE' then v_est_position.realised_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='PAYE' then v_est_position.held_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='UMBRELLA' then v_est_position.realised_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='UMBRELLA' then v_est_position.held_source_ex_vat else 0::numeric end)
                or v_est_binding.tax_treatment is distinct from v_est_line.tax_treatment or v_est_binding.classification_line_id is null
                or not exists(select 1 from private.bpay_next_approved_line original_class
                  where original_class.id=v_est_binding.classification_line_id and original_class.revision_id=v_est_original_revision
                    and original_class.component_key=v_est_line.component_key
                    and original_class.tax_treatment=v_est_line.tax_treatment and original_class.component_kind=v_est_line.component_kind)
                or (v_est_position.source_basis_channel='PAYE' and (v_est_position.native_realised_paye,v_est_position.native_held_paye)
                  is distinct from (v_est_position.realised_source_ex_vat,v_est_position.held_source_ex_vat))
                or (v_est_position.source_basis_channel='UMBRELLA' and (v_est_position.native_realised_umbrella,v_est_position.native_held_umbrella)
                  is distinct from (v_est_position.realised_source_ex_vat,v_est_position.held_source_ex_vat)) then
                raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            end if;
            v_est_vat:=case when upper(v_est_candidate.pay_method)='UMBRELLA' then
              (select u.vat_chargeable from public.umbrellas u where u.id=v_est_candidate.umbrella_id and u.enabled) end;
            if upper(v_est_candidate.pay_method) not in ('PAYE','UMBRELLA') or v_est_candidate.pay_method is null
              or (upper(v_est_candidate.pay_method)='UMBRELLA' and v_est_vat is null) then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            v_est_choice:=null;
            if (v_est_work.work_kind in ('ORDINARY','SOURCE')
                or (v_est_work.work_kind='EXPENSE' and v_est_revision.source_kind='EXPENSE'
                  and v_est_revision.financial_snapshot_id is not null))
              and v_est_line.component_kind in ('EXPENSE','MILEAGE') and v_est_line.tax_treatment='NON_TAXABLE' then
              -- DR38 native fixed reimbursement: Source ex once, no channel conversion.
              v_est_window:=null;v_est_vat_pct:=null;v_est_window_count:=null;
              select pw.source_window_id,pw.vat_rate_pct,count(*) over(partition by pw.date_from)
                into v_est_window,v_est_vat_pct,v_est_window_count from private.bpay_next_valuation_policy_window pw
                where pw.policy_id=v_est_revision.valuation_policy_id
                  and (v_date_context->>'current_official_pay_date')::date>=pw.date_from
                  and (v_date_context->>'current_official_pay_date')::date<=coalesce(pw.date_to,'infinity'::date)
                order by pw.date_from desc,pw.source_window_id desc limit 1;
              if v_est_window is null or v_est_window_count<>1 then
                raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
              if upper(v_est_candidate.pay_method)='UMBRELLA' then
                v_est_amounts:=public._pay_umbrella_vat_calc(v_est_record.included_source,v_est_vat_pct,v_est_vat);
                select v_est_window as policy_window_id,v_est_record.included_source as target_ex_vat,
                  (v_est_amounts->>'vat')::numeric as target_vat,(v_est_amounts->>'inc')::numeric as target_inc_vat into v_est_value;
              else
                select v_est_window as policy_window_id,v_est_record.included_source as target_ex_vat,
                  0::numeric as target_vat,v_est_record.included_source as target_inc_vat into v_est_value;
              end if;
            elsif v_est_line.tax_treatment='TAXABLE' and v_est_line.component_kind in ('WORK','PROTECTED_WORK','ADDITIONAL','ADJUSTMENT') then
              v_est_choice:=private.bpay_next_current_component_resolution_quote_v1(v_est_work.id,v_est_revision.id,v_est_line.id,
                (v_date_context->>'current_official_pay_date')::date,v_est_record.selected_leaf_keys,null);
              if v_est_choice is null then
            select x.* into strict v_est_value from private.bpay_next_work_revision r
              cross join lateral private.bpay_next_value_source_for_target_v1(v_est_record.included_source,r.source_pay_channel,
                v_est_candidate.pay_method::text,case when v_est_candidate.pay_method='UMBRELLA' then
                  (select u.vat_chargeable from public.umbrellas u where u.id=v_est_candidate.umbrella_id) else false end,
                r.valuation_policy_id,(v_date_context->>'current_official_pay_date')::date) x where r.id=v_est_record.revision_id;
              else
                if (v_est_choice->>'included_source_component_ex_vat')::numeric is distinct from v_est_record.included_source
                  or (v_est_choice->>'valuation_policy_id')::uuid is distinct from v_est_revision.valuation_policy_id then
                  raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
                select (v_est_choice->>'valuation_window_id')::uuid as policy_window_id,
                  (v_est_choice#>>'{component_quote,ex}')::numeric as target_ex_vat,
                  (v_est_choice#>>'{component_quote,vat}')::numeric as target_vat,
                  (v_est_choice#>>'{component_quote,inc}')::numeric as target_inc_vat into v_est_value;
                select array_cat(v_est_choice_versions,coalesce(array_agg(x.value),array[]::jsonb[]))
                  into v_est_choice_versions from jsonb_array_elements(v_est_choice->'current_decision_versions') x;
                if cardinality(v_est_choice_versions)>8000 then
                  raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
              end if;
            else
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';
            end if;
            v_est_source:=v_est_source+v_est_record.included_source;v_est_gross:=v_est_gross+v_est_value.target_ex_vat;
          end loop;
          if jsonb_array_length(v_versions)+cardinality(v_est_choice_versions)>8000 then
            raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
          select coalesce(jsonb_agg(value order by value::text),'[]') into v_versions
            from (select distinct value from (select x.value from jsonb_array_elements(v_versions) x
              union all select unnest(v_est_choice_versions)) combined) native_versions;
          if v_est->'versions'<>'null'::jsonb and v_est->'versions' is distinct from v_versions then
            v_est_state:='OUTDATED';
          else
          select cardinality(v_est_components)>0 and coalesce(bool_and((cc.case_kind,cc.instruction_kind,cc.payroll_stage)
            is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false) into v_est_all_overpayment
            from private.bpay_next_case_component cc where cc.id=any(v_est_components);
          v_est_h:=v_est_gross;
          select w.* into v_est_week from private.bpay_next_worker_week w where w.candidate_id=v_est_candidate.id and w.pay_week_start=v_est_week_start;
          if cardinality(v_est_components)>0 and not v_est_all_overpayment then
            if coalesce(v_est_week.unresolved_contribution_count,0)<>0 or exists(select 1 from private.bpay_next_run_worker w
              join private.bpay_next_pay_run r on r.id=w.run_id where w.candidate_id=v_est_candidate.id
                and w.status in ('READY','DRAFT','ISSUED','COMPLETE') and r.pay_date>=v_est_week_start
                and r.pay_date<=(v_date_context->>'current_official_pay_date')::date
                and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_WEEK_UNBOUND';end if;
            v_est_seen:=0;
            for v_est_record in select x.* from private.bpay_next_worker_week_contribution x
              where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
                and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date
              order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit 1001 loop
              v_est_seen:=v_est_seen+1;
              if v_est_seen>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
              if v_est_record.eligibility_state='UNBOUND' then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_WEEK_UNBOUND';end if;
              v_est_h:=v_est_h+v_est_record.eligible_arranged_amount;
            end loop;
          end if;
          for v_est_record in select cc.* from private.bpay_next_case_component cc where cc.id=any(v_est_components)
            order by cc.case_id,cc.component_ordinal,cc.id loop
            v_est_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
              'candidate_id',v_est_candidate.id,'case_id',v_est_record.case_id,'case_component_id',v_est_record.id,
              'pay_date',v_date_context->>'current_official_pay_date','pay_week_start',v_est_week_start,
              'target_pay_channel',v_est_candidate.pay_method::text,'captured_default_floor',v_est_candidate.min_take_home_wtd,
              'captured_default_floor_revision',coalesce(v_est_week.default_floor_revision,1),
              'weekly_binding_state',case when v_est_all_overpayment then 'NOT_AFFECTED' else 'BOUND' end,'accepted_run_id',null));
            if v_est_record.id=any(v_est_wait_components) and (
                not exists(select 1 from private.bpay_next_work_collection origin
                  join private.bpay_next_work work on work.id=origin.work_id and work.candidate_id=origin.candidate_id
                  where origin.case_id=v_est_record.case_id and origin.case_component_id=v_est_record.id
                    and origin.candidate_id=v_est_candidate.id and origin.paid_basis_qualified
                    and origin.qualification_state='READY' and private.bpay_next_work_wait_state_v1(work.id))
                or (v_est_plan->'instruction'->>'case_kind',v_est_plan->'instruction'->>'instruction_kind',
                    v_est_plan->'instruction'->>'direction',v_est_plan->'instruction'->>'payroll_stage',
                    v_est_plan->'instruction'->>'hold_purpose')
                  is distinct from ('OVERPAYMENT','RECOVERY','DEDUCTION','GROSS_DEDUCT','GROSS_RECOVERY')
                or v_est_plan->'instruction'->>'explanation' is distinct from 'ORIGIN_UNAUTHORISED_WAIT'
                or v_est_plan->'instruction'->>'work_collection_id' is null
                or (v_est_plan->'instruction'->>'nominal_source_ex_vat')::numeric is distinct from 0::numeric
                or (v_est_plan->'instruction'->>'capacity_source_ex_vat')::numeric is distinct from 0::numeric
                or (v_est_plan->'instruction'->>'nominal_target_ex_vat')::numeric is distinct from 0::numeric
                or (v_est_plan->'instruction'->>'capacity_target_ex_vat')::numeric is distinct from 0::numeric) then
              -- Maintained WAIT may survive an approved-but-unapplied pointer.
              -- It must never admit a positive informational recovery.
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            v_est_instructions:=v_est_instructions||jsonb_build_array(v_est_plan->'instruction');
          end loop;
          v_est_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
            'instructions',v_est_instructions,'channel_headroom',v_est_gross,'worker_take_home',v_est_h));
          v_est_scope:=jsonb_build_object('descriptor',v_descriptor,'expected_financial_view_revision',v_est_financial::text,'versions',v_versions);
          v_context:=v_context||jsonb_build_object('estimate_scope',v_est_scope);
          v_after:=null;v_est_last:=null;v_est_seen:=0;
          if p_request->'after'<>'null'::jsonb then
            if jsonb_typeof(p_request->'after')<>'object' or not(p_request->'after' ?& array['context','key'])
              or (p_request->'after')-array['context','key']<>'{}'::jsonb or p_request->'after'->'context' is distinct from v_context
              or (p_request->'after'->'key'<>'null'::jsonb and (jsonb_typeof(p_request->'after'->'key')<>'array' or jsonb_array_length(p_request->'after'->'key')<>4))
              or (p_request->'after'->'key'='null'::jsonb and (jsonb_array_length(v_est_plan->'rows')<>0 or v_est->'verify_only'<>'true'::jsonb)) then
              raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
            v_after:=nullif(p_request->'after'->'key','null'::jsonb);
            if v_after is not null and not exists(select 1 from jsonb_array_elements(v_est_plan->'rows') r where
              jsonb_build_array(r->'instruction'->>'age_key',r->'instruction'->>'case_id',r->'instruction'->>'component_ordinal',r->'instruction'->>'case_component_id')=v_after) then
              raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
          end if;
          for v_est_row in select value from jsonb_array_elements(v_est_plan->'rows') loop
            if v_est_row->'instruction'->>'payroll_stage'='GROSS_ADD' then v_est_gross_add:=v_est_gross_add+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='GROSS_DEDUCT' then v_est_gross_deduct:=v_est_gross_deduct+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='NET_ADD' then v_est_net_add:=v_est_net_add+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='NET_DEDUCT' then v_est_net_deduct:=v_est_net_deduct+(v_est_row->>'taken_amount')::numeric;end if;
            v_est_key:=jsonb_build_array(v_est_row->'instruction'->>'age_key',v_est_row->'instruction'->>'case_id',
              v_est_row->'instruction'->>'component_ordinal',v_est_row->'instruction'->>'case_component_id');
            if v_after is not null and ((v_est_key->>0)::bigint,(v_est_key->>1)::uuid,(v_est_key->>2)::bigint,(v_est_key->>3)::uuid)
              <=((v_after->>0)::bigint,(v_after->>1)::uuid,(v_after->>2)::bigint,(v_after->>3)::uuid) then continue;end if;
            v_est_seen:=v_est_seen+1;
            if v_est_seen<=v_limit and v_est->'verify_only'='false'::jsonb then
              v_est_last:=v_est_key;
              v_est_rows:=v_est_rows||jsonb_build_array(jsonb_build_object('case_id',v_est_row->'instruction'->'case_id',
                'case_component_id',v_est_row->'instruction'->'case_component_id','age_key',v_est_key->>0,
                'component_ordinal',v_est_key->>2,'recoverable_this_run',v_est_row->>'taken_amount',
                'cap_reason',v_est_row->'cap_reason','affordability_reason',v_est_row->'affordability_reason',
                'capacity_reason',v_est_row->'capacity_reason','payroll_stage',v_est_row->'instruction'->'payroll_stage'));
            end if;
          end loop;
          if v_est->'verify_only'='true'::jsonb and v_est_seen<>0 then
            raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
          v_est_bound:=jsonb_build_object('context',v_context,'key',coalesce(v_est_last,v_after));
          -- Conservative URL encoding bound checked again exactly by Worker/Office.
          if octet_length((p_request||jsonb_build_object('after',v_est_bound,'estimate',v_est||jsonb_build_object('versions',v_versions)))::text)*3>32768 then
            raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_SCOPE_TOO_LARGE';end if;
          v_response:=jsonb_build_object('ok',true,'kind',v_kind,'state','ESTIMATE','context',v_context,
            'workbench_revision',v_revision.workbench_revision::text,'complete',v_est_seen<=v_limit,'rows',v_est_rows,
            'next_cursor',case when v_est_seen>v_limit then v_est_bound else null end,
            'verification_cursor',jsonb_build_object('context',v_context,'key',coalesce(v_est_last,v_after)),
            'estimated_gross_source_ex_vat',v_est_source::text,'estimated_gross_target_ex_vat',(v_est_gross+v_est_gross_add-v_est_gross_deduct)::text,
            'estimated_gross_deductions',v_est_gross_deduct::text,'estimated_net_additions',v_est_net_add::text,'estimated_net_deductions',v_est_net_deduct::text,
            'financial_view_revision_before',v_est_financial::text,'financial_view_revision_after',v_est_financial::text,
            'affected_candidate_count','1','page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_est_last,'row_count',jsonb_array_length(v_est_rows)::text));
          if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_SCOPE_TOO_LARGE';end if;
          return v_response;
          end if;
        end if;
      exception when check_violation or no_data_found or too_many_rows or program_limit_exceeded then
        v_est_state:='PENDING';
      when object_not_in_prerequisite_state then
        if sqlerrm='BPAY_NEXT_COMPARISON_NOT_INSTALLED' then v_est_state:='PENDING';else raise;end if;
      end;
    end if;
    v_context:=v_context-'estimate_scope';
    return jsonb_build_object('ok',true,'kind',v_kind,'state',v_est_state,'context',v_context,
      'workbench_revision',v_revision.workbench_revision::text,'complete',true,'rows','[]'::jsonb,
      'next_cursor',null,'verification_cursor',null,'estimated_gross_source_ex_vat',null,'estimated_gross_target_ex_vat',null,
      'estimated_gross_deductions',null,'estimated_net_additions',null,'estimated_net_deductions',null,
      'financial_view_revision_before',v_est_financial::text,'financial_view_revision_after',v_est_financial::text,
      'affected_candidate_count','1','page_receipt',jsonb_build_object('context',v_context,'start_key',null,'end_key',null,'row_count','0'));
  elsif p_request->'estimate'<>'null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';
  end if;

  v_read_scope:=v_scope||jsonb_build_object('_selection_memory',v_memory,
    '_official_pay_date',v_date_context->>'current_official_pay_date');
  v_cursor:=p_request->'after';
  if p_request->'refresh_anchor'<>'null'::jsonb then
    if v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES') or v_cursor<>'null'::jsonb then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    v_cursor:=p_request->'refresh_anchor';v_refresh:=true;v_refresh_before:=coalesce((v_cursor->>'before')::boolean,false);
  end if;
  if v_cursor<>'null'::jsonb then
    if jsonb_typeof(v_cursor)<>'object' or not(v_cursor ?& array['context','key'])
      or (v_cursor-case when v_refresh then array['context','key','before'] else array['context','key'] end)<>'{}'::jsonb
      or (v_cursor ? 'before' and (not v_refresh or jsonb_typeof(v_cursor->'before') is distinct from 'boolean'))
      or (not v_refresh and v_cursor->'context' is distinct from v_context)
      or (v_refresh and ((v_cursor->'context')-array['workbench_revision','adoption_revision'] is distinct from v_context-array['workbench_revision','adoption_revision']
        or jsonb_typeof(v_cursor#>'{context,workbench_revision}') is distinct from 'string'
        or jsonb_typeof(v_cursor#>'{context,adoption_revision}') is distinct from 'string'
        or v_cursor#>>'{context,workbench_revision}'!~'^(0|[1-9][0-9]{0,18})$'
        or v_cursor#>>'{context,adoption_revision}'!~'^(0|[1-9][0-9]{0,18})$')) then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    if v_refresh and ((v_cursor#>>'{context,workbench_revision}')::numeric>(v_context->>'workbench_revision')::numeric
      or (v_cursor#>>'{context,adoption_revision}')::numeric>(v_context->>'adoption_revision')::numeric) then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_after:=v_cursor->'key';
    if v_kind='EXPORT_ROWS' then
      if jsonb_typeof(v_after)<>'array' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      if v_sort in ('C_NAME_ASC','C_NAME_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'string' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_AMOUNT_ASC','C_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'boolean' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_STATUS_ASC','C_STATUS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_PAYMENT_ASC','G_PAYMENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CLIENT_ASC','G_CLIENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CHANNEL_ASC','G_CHANNEL_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_AMOUNT_ASC','G_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_REASON_ASC','G_REASON_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    else
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
  end if;
  if v_refresh then
    v_read_scope:=v_read_scope||jsonb_build_object('_refresh_anchor_key',v_after,'_refresh_anchor_before',v_refresh_before);v_after:=null;
  end if;
  case v_kind
    when 'HEADER' then v_page:=private.bpay_next_workbench_header_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATES' then v_page:=private.bpay_next_workbench_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'ACTION_REQUIRED' then v_page:=private.bpay_next_workbench_action_required_page_v1(v_read_scope,v_after,v_limit);
    when 'BLOCKED' then v_page:=private.bpay_next_workbench_blocked_page_v1(v_read_scope,v_after,v_limit);
    when 'ISSUE_DETAIL' then v_page:=private.bpay_next_workbench_issue_detail_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATE_GROUPS' then v_page:=private.bpay_next_workbench_candidate_groups_page_v1(v_read_scope,v_after,v_limit);
    when 'GROUP_LINES' then v_page:=private.bpay_next_workbench_group_lines_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CANDIDATES' then v_page:=private.bpay_next_workbench_pick_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CLIENTS' then v_page:=private.bpay_next_workbench_pick_clients_page_v1(v_read_scope,v_after,v_limit);
    when 'EXPORT_ROWS' then v_page:=private.bpay_next_workbench_export_rows_page_v1(v_read_scope,v_after,v_limit);
  end case;
  -- WB41 navigation only: exact immutable paid origin, never a Case selection
  -- operand or a changed seek key. Keep every indexed reference/week/key intact.
  if v_kind in ('ACTION_REQUIRED','ISSUE_DETAIL','CANDIDATE_GROUPS') then
    v_wait_rows:='[]'::jsonb;
    for v_wait_row in select value from jsonb_array_elements(v_page->'rows') loop
      if v_wait_row->>'reason_code'='ORIGIN_UNAUTHORISED_WAIT'
        and v_wait_row->>'section'='ACTION'
        and (v_kind<>'CANDIDATE_GROUPS' or v_wait_row->>'selectable'='false')
        and v_wait_row->>'work_id' is null
        and (v_wait_row->>'case_component_id' is not null or v_kind='CANDIDATE_GROUPS' and v_wait_row->>'group_kind'='CASE' and v_wait_row->>'accepted_run_id' is null) then
        select r.id,r.physical_timesheet_id,r.physical_timesheet_version,r.source_kind,r.source_head_id,
          case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_id else h.root_timesheet_id end end as root_id,
          case when r.source_kind in ('SOURCE','PROTECTED') then case when r.source_head_id is null and r.sealed_at_utc is not null then r.physical_timesheet_version else h.root_timesheet_version end end as root_version,
          case when r.source_kind in ('SOURCE','PROTECTED') then r.week_ending_date end as source_week
          into v_wait_origin
        from private.bpay_next_work_collection origin
        join private.bpay_next_case_component component on component.id=origin.case_component_id and component.case_id=origin.case_id and component.candidate_id=origin.candidate_id
        join private.bpay_next_work work on work.id=origin.work_id and work.candidate_id=origin.candidate_id
        join private.bpay_next_work_revision r on r.id=origin.original_revision_id and r.work_id=work.id
        left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id and h.candidate_id=work.candidate_id and h.contract_id=work.contract_id and h.week_ending_date=r.week_ending_date
        where origin.case_id=(v_wait_row->>'case_id')::uuid and (v_wait_row->>'case_component_id' is null or origin.case_component_id=(v_wait_row->>'case_component_id')::uuid)
          and origin.candidate_id=(v_wait_row->>'candidate_id')::uuid and origin.paid_basis_qualified and origin.qualification_state='READY'
          and (component.case_kind,component.case_subtype,component.instruction_kind,component.direction,component.payroll_stage)
            is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','DEDUCTION','GROSS_DEDUCT')
          and exists(select 1 from private.bpay_next_workbench_item info
            where info.case_id=origin.case_id and info.case_component_id=origin.case_component_id
              and info.candidate_id=origin.candidate_id and info.work_id is null
              and info.portion_kind='AVAILABLE' and info.section='ACTION'
              and info.reason_code='ORIGIN_UNAUTHORISED_WAIT' and not info.is_selected and not info.selectable
              and (v_kind='CANDIDATE_GROUPS' or info.id=(v_wait_row->>'item_id')::uuid))
          and private.bpay_next_work_wait_state_v1(work.id);
        if found then
          v_wait_row:=v_wait_row||jsonb_build_object('physical_timesheet_id',v_wait_origin.physical_timesheet_id);
          if v_kind in ('ISSUE_DETAIL','CANDIDATE_GROUPS') then
            v_wait_row:=v_wait_row||jsonb_build_object('physical_revision_id',v_wait_origin.id,
              'physical_timesheet_version',v_wait_origin.physical_timesheet_version::text,'source_kind',v_wait_origin.source_kind,
              'source_head_id',v_wait_origin.source_head_id,'source_root_timesheet_id',v_wait_origin.root_id,
              'source_root_timesheet_version',v_wait_origin.root_version::text,'source_week_ending_date',v_wait_origin.source_week);
          end if;
        end if;
      end if;
      v_wait_rows:=v_wait_rows||jsonb_build_array(v_wait_row);
    end loop;
    v_page:=jsonb_set(v_page,'{rows}',v_wait_rows);
  end if;
  v_response:=jsonb_build_object('ok',true,'kind',v_kind,'workbench_revision',v_revision.workbench_revision::text,
    'complete',v_page->'complete','context',v_context,'rows',v_page->'rows',
    'affected_candidate_count',v_page->'affected_candidate_count',
    'next_cursor',case when v_page->>'complete'='true' then null else jsonb_build_object('context',v_context,'key',v_page->'last_key') end,
    'page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_page->'last_key',
      'row_count',jsonb_array_length(v_page->'rows')::text));
  if v_kind='HEADER' then
    v_response:=v_response||jsonb_build_object('job_progress',v_progress);
    v_response:=jsonb_set(v_response,'{rows,0}',(v_response#>'{rows,0}')||
      jsonb_build_object('oldest_pending_age_seconds',v_progress->'oldest_pending_age_seconds','can_create_draft',true));
  end if;
  if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  return v_response;
exception when others then
  if p_request->>'kind' in ('SNOOZE_CHECK','RESOLUTION_CONTEXT') then raise;end if;
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
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
