-- WB17 exact current/applied physical Timesheet and immutable Source root/week.
-- Three-way successor of2026; other kinds and WB16 GROUP_LINES unchanged.
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
begin
  if p_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS') or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_desc:=v_sort like '%_DESC';v_op:=case when v_desc then '<' else '>' end;
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
      v_rows:=v_rows||jsonb_build_array(v_row.row_value);v_last:=v_row.row_key;v_n:=v_n+1;
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
    v_sql:='select '||v_projection||' as row_value,'||v_key||' as row_key from (select s.presentation_key,s.presentation_kind,s.presentation_id,s.parent_group_key,s.candidate_id,s.case_component_id,s.section,s.client_id,s.week_ending_date,s.client_display_name,s.payment_label,s.timesheet_reference,s.target_pay_channel,s.has_deduction,s.selectable,s.reason_code,s.reason_priority,s.group_amount,s.ready_net_add_amount,s.gross_deduction_balance,s.net_deduction_balance,s.waiting_amount,s.waiting_net_add_amount,s.accepted_run_id,s.accepted_command_id,s.accepted_module_epoch,s.snooze_until_date,s.eligible_from_date,s.pay_hold_reason'||v_sql||v_where||' order by '||v_order||' limit $11) s join private.bpay_next_workbench_group g on g.group_key=s.parent_group_key and g.candidate_id=s.candidate_id left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=s.candidate_id left join private.bpay_next_work w on w.id=g.work_id and w.candidate_id=g.candidate_id left join private.bpay_next_work_revision r on r.work_id=w.id and r.id=case when w.approval_state=''WITHDRAWN'' then w.applied_revision_id else w.current_revision_id end left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id and h.candidate_id=w.candidate_id and h.contract_id=w.contract_id and h.week_ending_date=r.week_ending_date order by '||v_order;
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
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_last:=v_row.row_key;v_n:=v_n+1;
    if pg_catalog.octet_length(v_rows::text)>110000 then
      raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  end loop;
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
    or jsonb_typeof(p_request->'sort')<>'string' or (v_kind='EXPORT_ROWS' and v_sort not in ('C_NAME_ASC','C_NAME_DESC','C_AMOUNT_ASC','C_AMOUNT_DESC','C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC','C_STATUS_ASC','C_STATUS_DESC','G_PAYMENT_ASC','G_PAYMENT_DESC','G_CLIENT_ASC','G_CLIENT_DESC','G_CHANNEL_ASC','G_CHANNEL_DESC','G_AMOUNT_ASC','G_AMOUNT_DESC','G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC','G_REASON_ASC','G_REASON_DESC'))
    or (v_kind<>'EXPORT_ROWS' and (false or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort<>'NAME_ASC')
    or (v_sort like 'CLIENT_%' or v_sort like 'CHANNEL_%') and v_kind<>'CANDIDATE_GROUPS'
    or (v_kind='CANDIDATE_GROUPS' and v_sort='NAME_DESC')
))
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
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
