-- DR13 frozen balance and native reason display; no live Case read.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_run_review_cases_v1(p_actor uuid,p_run uuid,p_worker uuid,p_sort text,p_after jsonb,p_limit integer)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_context jsonb;v_reply jsonb;v_final jsonb;v_rows jsonb:='[]';v_row jsonb;
 v_worker private.bpay_next_run_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;v_selection private.bpay_next_case_selection%rowtype;
 v_desc boolean:=right(p_sort,4)='DESC';v_numeric boolean:=split_part(p_sort,'_',1) in ('DUE','THIS','OUTSTANDING');
 v_complete boolean;v_n integer:=0;
begin
 if p_sort not in ('DEDUCTION_ASC','DEDUCTION_DESC','DUE_ASC','DUE_DESC','THIS_DRAFT_ASC','THIS_DRAFT_DESC','OUTSTANDING_ASC','OUTSTANDING_DESC','NOTE_ASC','NOTE_DESC')
  or p_limit is null or p_limit not between 1 and 100 or p_after is null then
  raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 v_context:=private.bpay_next_run_review_context_v1(p_actor,p_run,p_worker,p_sort);
 select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=p_worker and w.run_id=p_run;
 select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run;
 -- A Case detail page is a completed frozen offer, never a view of capture
 -- checkpoints whose operands can change without a financial transition.
 if not exists(select 1 from private.bpay_next_run_channel_contribution_v1(v_worker) c where c.frozen_offer_complete) then
  raise exception using errcode='55000',message='BPAY_NEXT_CASE_REVIEW_NOT_READY';end if;
 if p_after<>'null'::jsonb and (jsonb_typeof(p_after) is distinct from 'object' or p_after-array['context','key']<>'{}'::jsonb
  or not(p_after?&array['context','key']) or p_after->'context' is distinct from v_context
  or jsonb_typeof(p_after->'key') is distinct from 'array' or jsonb_array_length(p_after->'key')<>3
  or jsonb_typeof(p_after#>'{key,0}') is distinct from 'boolean' or jsonb_typeof(p_after#>'{key,2}') is distinct from 'string'
  or p_after#>>'{key,2}'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or ((p_after#>>'{key,0}')::boolean and p_after#>'{key,1}'<>'null'::jsonb)
  or (not(p_after#>>'{key,0}')::boolean and jsonb_typeof(p_after#>'{key,1}') is distinct from 'string')
  or (v_numeric and p_after#>>'{key,1}' is not null and p_after#>>'{key,1}'!~'^[0-9]{1,16}([.][0-9]{1,2})?$')
  or octet_length(p_after::text)>8192) then raise exception using errcode='55000',message='BPAY_NEXT_REVIEW_CONTEXT_CHANGED';end if;
 select s.* into v_selection from private.bpay_next_case_selection s
  where s.run_id=p_run and s.candidate_id=v_worker.candidate_id and s.selection_revision=v_worker.case_selection_revision and s.status='SEALED';
 if v_worker.case_selection_revision>0 and not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_UNSEALED';end if;
 with original as (
  select s.*,memory.basis,memory.captured_at_utc choice_captured_at,i.id instruction_id,i.case_subtype,i.direction,i.payroll_stage,
   i.source_pay_channel,i.nominal_source_ex_vat,i.nominal_target_ex_vat,i.captured_at_utc,i.explanation,
   i.captured_component_outstanding_source_ex_vat,i.captured_case_outstanding_source_ex_vat,
   i.captured_component_reserved_source_ex_vat,i.captured_case_reserved_source_ex_vat,
   i.other_run_capacity_reduction_target_ex_vat,i.other_run_capacity_reason,
   result.allocated_source_ex_vat,result.allocated_target_ex_vat,result.cap_reason,result.affordability_reason,result.capacity_reason,
   case when net.id is not null and net.completed_at_utc is not null and v_worker.net_projection_revision>0
      and i.payroll_stage='NET_DEDUCT' then 'NET_ACCEPTED' else 'DRAFT' end result_basis
  from private.bpay_next_case_selection_item s
  left join private.bpay_next_selection_exclusion_memory exclusion on exclusion.run_id=s.run_id and exclusion.intent_kind='cases'
   and exclusion.native_id=s.case_component_id and not s.is_selected and not exclusion.selected
  left join private.bpay_next_selection_mode_memory memory on memory.run_id=coalesce(exclusion.basis_ref_run_id,s.run_id)
   and memory.row_kind='LEAF' and memory.leaf_key='CASE:'||s.case_component_id::text and memory.candidate_id=s.candidate_id
  left join private.bpay_next_run_case_instruction i on i.run_worker_id=p_worker and i.preparation_revision=v_worker.captured_preparation_revision
   and i.selection_revision=s.selection_revision and i.case_component_id=s.case_component_id
  left join private.bpay_next_case_allocation_state draft on draft.run_worker_id=p_worker
   and draft.preparation_revision=v_worker.captured_preparation_revision and draft.selection_revision=s.selection_revision
   and draft.pass_kind='DRAFT' and draft.projection_no=0 and draft.completed_at_utc is not null
  left join private.bpay_next_case_allocation_state net on net.run_worker_id=p_worker
   and net.preparation_revision=v_worker.captured_preparation_revision and net.selection_revision=s.selection_revision
   and net.pass_kind='NET' and net.projection_no=v_worker.net_projection_revision and net.completed_at_utc is not null
  left join private.bpay_next_case_allocation_result result on result.instruction_id=i.id
   and result.state_id=case when i.payroll_stage='NET_DEDUCT' and net.id is not null then net.id else draft.id end
  where s.run_id=p_run and s.candidate_id=v_worker.candidate_id and s.selection_revision=v_worker.case_selection_revision
 ), figures as (
  select o.*,coalesce(o.case_subtype,o.basis->>'case_subtype') subtype,
   coalesce(o.direction,o.basis->>'direction') saved_direction,coalesce(o.payroll_stage,o.basis->>'payroll_stage') saved_stage,
   coalesce(o.source_pay_channel,o.basis->>'source_pay_channel') saved_channel,
   case when is_selected then case when explanation='ORIGIN_UNAUTHORISED_WAIT'
     then least(captured_component_outstanding_source_ex_vat,captured_case_outstanding_source_ex_vat) else nominal_source_ex_vat end
    else case when review_observed_at_utc is not null then review_observed_due_source_ex_vat
     else case when basis->>'case_state'<>'OPEN' or (basis->>'case_due_date')::date>v_run.pay_date then 0
      when basis->>'case_instruction_kind'<>'RECOVERY' then null
      when basis->>'case_weekly_due' is not null then case when (v_run.pay_date-extract(isodow from v_run.pay_date)::integer+1)>=(basis->>'case_next_due')::date
        then least((basis->>'case_weekly_due')::numeric,(basis->>'deduction_balance_source_ex_vat')::numeric) else 0 end
      when basis->>'case_kind'='OVERPAYMENT' and basis->>'rule_id'=case_id::text and case_id=case_component_id
        and exists(select 1 from private.bpay_next_work_collection c where c.id=o.case_id and c.case_id=o.case_id and c.case_component_id=o.case_component_id)
        then (basis->>'deduction_balance_source_ex_vat')::numeric else null end end end due,
   case when is_selected then allocated_target_ex_vat else null end this_draft,
   case when is_selected and captured_component_outstanding_source_ex_vat is not null and allocated_source_ex_vat is not null
      then least(captured_component_outstanding_source_ex_vat,captured_case_outstanding_source_ex_vat)-allocated_source_ex_vat
    when not is_selected then case when review_observed_at_utc is not null then review_observed_outstanding_source_ex_vat
      when basis->>'direction'='DEDUCTION' then (basis->>'deduction_balance_source_ex_vat')::numeric end end outstanding_after
  from original o
 ), balances as (
  select f.*,case when is_selected then captured_component_outstanding_source_ex_vat else review_observed_component_principal_source_ex_vat end principal_outstanding,
   case when is_selected then captured_component_reserved_source_ex_vat else review_observed_component_reserved_source_ex_vat end active_reserved_before,
   case when is_selected and saved_direction='DEDUCTION' then allocated_target_ex_vat end recoverable_this_run,
   case when is_selected and captured_component_reserved_source_ex_vat is not null and allocated_source_ex_vat is not null then
    least(greatest(captured_component_outstanding_source_ex_vat-captured_component_reserved_source_ex_vat-allocated_source_ex_vat,0),
      greatest(captured_case_outstanding_source_ex_vat-captured_case_reserved_source_ex_vat-allocated_source_ex_vat,0))
    when not is_selected then review_observed_unreserved_source_ex_vat end remaining_unreserved,
   case when is_selected and captured_component_reserved_source_ex_vat is not null and
     greatest(captured_case_outstanding_source_ex_vat-captured_case_reserved_source_ex_vat,0)<greatest(captured_component_outstanding_source_ex_vat-captured_component_reserved_source_ex_vat,0)
     or not is_selected and review_observed_unreserved_source_ex_vat<greatest(review_observed_component_principal_source_ex_vat-review_observed_component_reserved_source_ex_vat,0)
    then 'Shared balance limits this amount' end unreserved_limit_note
  from figures f
 ), labelled as (
  select f.*,case subtype when 'LOAN' then case when saved_direction='DEDUCTION' then 'Payment Advance repayment' else 'Payment Advance' end
    when 'PAYMENT_ADVANCE' then case when saved_direction='DEDUCTION' then 'Payment Advance repayment' else 'Payment Advance' end
    when 'OVERPAYMENT' then 'Overpayment recovery' when 'MANUAL_DEBT' then 'Manual Debt'
    when 'UNDERPAYMENT' then 'Underpayment' when 'MANUAL_CREDIT' then 'Manual Credit' else 'Payment' end label,
   case when not is_selected then 'UNTICKED' when instruction_id is null or allocated_source_ex_vat is null then 'NOT_PREPARED'
    when explanation='ORIGIN_UNAUTHORISED_WAIT' then 'ORIGIN_UNAUTHORISED_WAIT'
    when other_run_capacity_reason='SAME_WEEK_INSTALMENT' and other_run_capacity_reduction_target_ex_vat>0 and allocated_source_ex_vat=0
      and affordability_reason is null and capacity_reason='CASE_CAPACITY' then 'SAME_WEEK_INSTALMENT'
    when other_run_capacity_reason='CASE_BALANCE' and other_run_capacity_reduction_target_ex_vat>0 and capacity_reason='CASE_CAPACITY' then 'CASE_BALANCE'
    when cap_reason is not null then cap_reason
    when saved_stage='NET_DEDUCT' and result_basis='DRAFT' then 'PAYE_NET_NOT_ENTERED' end reason_code
  from balances f
 ), noted as (
  select l.*,case reason_code when 'UNTICKED' then 'Not selected'
   when 'NOT_PREPARED' then 'Not available'
   when 'ORIGIN_UNAUTHORISED_WAIT' then 'Recovery paused — waiting for re-authorisation'
   when 'SAME_WEEK_INSTALMENT' then 'This week’s instalment is already in another Draft'
   when 'CASE_BALANCE' then 'Some of the balance is already in another Draft'
   when 'PAYE_NET_NOT_ENTERED' then 'PAYE net not entered'
   when 'CASE_CAPACITY' then 'Limited to the remaining balance'
   when 'EARNINGS_THRESHOLD' then 'Below the minimum earnings threshold'
   when 'TAKE_HOME_FLOOR' then case when this_draft=0 then 'Not enough pay to protect take-home pay' else 'Limited to protect take-home pay' end
   when 'PAY_HEADROOM' then case when this_draft=0 then 'Not enough pay' else 'Limited to available pay' end
   else case when reason_code is not null then 'Limited to available pay' else '' end end note from labelled l
 ), scoped as (
  select l.*,case split_part(p_sort,'_',1) when 'DEDUCTION' then label when 'DUE' then due::text
   when 'THIS' then this_draft::text when 'OUTSTANDING' then outstanding_after::text
   when 'NOTE' then note end sort_value from noted l
 ), tail as (select sort_value from scoped where case_component_id=(p_after#>>'{key,2}')::uuid), bounded as (
  select n.* from scoped n where p_after='null'::jsonb or
   (n.sort_value is null and not(p_after#>>'{key,0}')::boolean) or
   ((n.sort_value is null)=(p_after#>>'{key,0}')::boolean and case when v_numeric then
    case when v_desc then (coalesce(n.sort_value::numeric,0),n.case_component_id)<(coalesce((p_after#>>'{key,1}')::numeric,0),(p_after#>>'{key,2}')::uuid)
      else (coalesce(n.sort_value::numeric,0),n.case_component_id)>(coalesce((p_after#>>'{key,1}')::numeric,0),(p_after#>>'{key,2}')::uuid) end
    else case when v_desc then (coalesce(n.sort_value,'') collate "C",n.case_component_id)<(coalesce(p_after#>>'{key,1}','') collate "C",(p_after#>>'{key,2}')::uuid)
      else (coalesce(n.sort_value,'') collate "C",n.case_component_id)>(coalesce(p_after#>>'{key,1}','') collate "C",(p_after#>>'{key,2}')::uuid) end end)
  order by n.sort_value is null asc,
   case when v_numeric and not v_desc then n.sort_value::numeric end asc,case when v_numeric and v_desc then n.sort_value::numeric end desc,
   case when not v_numeric and not v_desc then n.sort_value end collate "C" asc,case when not v_numeric and v_desc then n.sort_value end collate "C" desc,
   case when not v_desc then n.case_component_id end asc,case when v_desc then n.case_component_id end desc limit p_limit+1
 ) select jsonb_build_object('tail_matches',(select count(*) from tail where sort_value is not distinct from p_after#>>'{key,1}')::text,
  'rows',coalesce((select jsonb_agg(jsonb_build_object('case_id',n.case_id,'case_component_id',n.case_component_id,
   'instruction_id',n.instruction_id,'selected',n.is_selected,'case_subtype',n.subtype,'direction',n.saved_direction,'payroll_stage',n.saved_stage,
   'source_pay_channel',n.saved_channel,'label',n.label,'due',n.due::text,'this_draft',n.this_draft::text,
   'outstanding_after',n.outstanding_after::text,
   'principal_outstanding',n.principal_outstanding::text,'active_reserved_before',n.active_reserved_before::text,
   'recoverable_this_run',n.recoverable_this_run::text,'remaining_unreserved',n.remaining_unreserved::text,'unreserved_limit_note',n.unreserved_limit_note,'result_basis',case when n.instruction_id is null then null else n.result_basis end,
   'reason_code',n.reason_code,'note',n.note,'captured_at_utc',coalesce(n.captured_at_utc,n.review_observed_at_utc,n.choice_captured_at),
   'other_run_capacity_reduction',n.other_run_capacity_reduction_target_ex_vat::text,'other_run_capacity_reason',n.other_run_capacity_reason,
   'sort_key',jsonb_build_array(n.sort_value is null,n.sort_value,n.case_component_id)) order by n.sort_value is null asc,
   case when v_numeric and not v_desc then n.sort_value::numeric end asc,case when v_numeric and v_desc then n.sort_value::numeric end desc,
   case when not v_numeric and not v_desc then n.sort_value end collate "C" asc,case when not v_numeric and v_desc then n.sort_value end collate "C" desc,
   case when not v_desc then n.case_component_id end asc,case when v_desc then n.case_component_id end desc) from bounded n),'[]'::jsonb)) into v_reply;
 if p_after<>'null'::jsonb and v_reply->>'tail_matches'<>'1' then raise exception using errcode='55000',message='BPAY_NEXT_REVIEW_CONTEXT_CHANGED';end if;
 v_complete:=jsonb_array_length(v_reply->'rows')<=p_limit;
 for v_row in select value from jsonb_array_elements(v_reply->'rows') limit p_limit loop
  if (v_row->>'outstanding_after')::numeric<0 then raise exception using errcode='23514',message='BPAY_NEXT_CASE_REVIEW_OUTSTANDING_INVALID';end if;
  v_final:=jsonb_build_object('context',v_context,'rows',v_rows||jsonb_build_array(v_row),'complete',false,
   'next_cursor',jsonb_build_object('context',v_context,'key',v_row->'sort_key'),
   'counts',jsonb_build_object('selected',coalesce(v_selection.selected_count,0)::text,'excluded',coalesce(v_selection.excluded_count,0)::text,'scope_count',coalesce(v_selection.item_count,0)::text));
  if octet_length(v_final::text)>120000 then
   if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
   v_complete:=false;exit;end if;
  v_rows:=v_rows||jsonb_build_array(v_row);v_n:=v_n+1;
 end loop;
 v_final:=jsonb_build_object('context',v_context,'rows',v_rows,'complete',v_complete,
  'next_cursor',case when v_complete then null else jsonb_build_object('context',v_context,'key',v_rows->-1->'sort_key') end,
  'counts',jsonb_build_object('selected',coalesce(v_selection.selected_count,0)::text,'excluded',coalesce(v_selection.excluded_count,0)::text,'scope_count',coalesce(v_selection.item_count,0)::text));
 if octet_length(v_final::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
 return v_final;
end
$function$;
alter function private.bpay_next_run_review_cases_v1(uuid,uuid,uuid,text,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_run_review_cases_v1(uuid,uuid,uuid,text,jsonb,integer) from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;
