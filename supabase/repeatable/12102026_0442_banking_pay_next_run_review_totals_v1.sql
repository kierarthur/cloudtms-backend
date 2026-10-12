-- DR-16 bounded frozen review. Native money owners remain unchanged.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_run_review_context_v1(p_actor uuid,p_run uuid,p_worker uuid,p_sort text)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v jsonb;v_epoch bigint;v_original bigint;
begin
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT';
 if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select coalesce(c.module_epoch,(r.accepted_choice_descriptor#>>'{namespace,owner_epoch}')::bigint) into v_original
  from private.bpay_next_pay_run r left join private.bpay_next_run_command rc on rc.run_id=r.id
  left join private.bpay_next_command c on c.id=rc.command_id where r.id=p_run;
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_PREPARATION_RUN_MISSING';end if;
 if v_original is distinct from v_epoch then raise exception using errcode='55000',message='BPAY_NEXT_REVIEW_CONTEXT_CHANGED';end if;
 select jsonb_build_object('run_id',r.id,'actor_user_id',p_actor,'owner_epoch',v_epoch::text,
  'review_revision',r.review_revision::text,'run_status',r.status,'selection_state',r.selection_state,
  'paye_totals_revision',a.totals_revision::text,'umbrella_totals_revision',b.totals_revision::text,
  'run_worker_id',w.id,'candidate_id',w.candidate_id,'worker_status',w.status,
  'preparation_revision',w.preparation_revision::text,'captured_preparation_revision',w.captured_preparation_revision::text,
  'case_selection_revision',w.case_selection_revision::text,'net_request_revision',w.net_request_revision::text,
  'net_projection_revision',w.net_projection_revision::text,'sort',p_sort)
 into v from private.bpay_next_pay_run r
 join private.bpay_next_run_channel_totals a on a.run_id=r.id and a.pay_channel='PAYE'
 join private.bpay_next_run_channel_totals b on b.run_id=r.id and b.pay_channel='UMBRELLA'
 left join private.bpay_next_run_worker w on w.run_id=r.id and w.id=p_worker where r.id=p_run;
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_REVIEW_TOTALS_NOT_INITIALISED';end if;
 if p_worker is not null and v->>'run_worker_id' is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';end if;
 return v;
end
$function$;
alter function private.bpay_next_run_review_context_v1(uuid,uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_run_review_context_v1(uuid,uuid,uuid,text) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_run_review_totals_v1(p_actor uuid,p_run uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_context jsonb;v_rows jsonb;v_whole jsonb;v_cut timestamptz;
begin
 v_context:=private.bpay_next_run_review_context_v1(p_actor,p_run,null,null);
 select jsonb_agg(jsonb_build_object('pay_channel',t.pay_channel,'candidate_count',t.candidate_count::text,'ready_count',t.ready_count::text,'gross_ex_vat',t.gross_ex_vat::text,'gross_vat',t.gross_vat::text,'gross_inc_vat',t.gross_inc_vat::text,'gross_stage_deductions',t.gross_stage_deductions::text,'net_recovery_reserved',t.net_recovery_reserved::text,'cash_known',t.cash_known::text,'net_missing_count',t.net_missing_count::text,'applicable_net_missing_count',t.applicable_net_missing_count::text,'net_replacement_pending_count',t.net_replacement_pending_count::text,'net_refused_code',t.net_refused_code,
  'totals_revision',t.totals_revision::text,'updated_at_utc',t.updated_at_utc) order by t.pay_channel collate "C"),
  jsonb_build_object('candidate_count',sum(t.candidate_count::bigint)::text,'ready_count',sum(t.ready_count::bigint)::text,'gross_ex_vat',sum(t.gross_ex_vat::numeric)::text,'gross_vat',sum(t.gross_vat::numeric)::text,'gross_inc_vat',sum(t.gross_inc_vat::numeric)::text,'gross_stage_deductions',sum(t.gross_stage_deductions::numeric)::text,'net_recovery_reserved',sum(t.net_recovery_reserved::numeric)::text,'cash_known',sum(t.cash_known::numeric)::text,'net_missing_count',sum(t.net_missing_count::bigint)::text,'applicable_net_missing_count',sum(t.applicable_net_missing_count::bigint)::text,'net_replacement_pending_count',sum(t.net_replacement_pending_count::bigint)::text,'net_refused_code',min((t.net_refused_code collate "C")::text)) into v_rows,v_whole
 from private.bpay_next_run_channel_totals t where t.run_id=p_run;
 select created_at_utc into strict v_cut from private.bpay_next_pay_run where id=p_run;
 return jsonb_build_object('context',v_context,'whole',v_whole,'channels',v_rows,'captured_at_utc',v_cut,'observed_at_utc',statement_timestamp());
end
$function$;
alter function private.bpay_next_run_review_totals_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_run_review_totals_v1(uuid,uuid) from public,anon,authenticated,service_role;

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
  from figures f
 ), noted as (
  select l.*,case reason_code when 'UNTICKED' then 'Not selected'
   when 'NOT_PREPARED' then 'Not available'
   when 'ORIGIN_UNAUTHORISED_WAIT' then 'Recovery paused — waiting for re-authorisation'
   when 'SAME_WEEK_INSTALMENT' then 'This week’s instalment is already in another Draft'
   when 'CASE_BALANCE' then 'Some of the balance is already in another Draft'
   when 'PAYE_NET_NOT_ENTERED' then 'PAYE net not entered'
   when 'CASE_CAPACITY' then 'Limited to the remaining balance'
   when 'EARNINGS_THRESHOLD' then 'Earnings below the threshold'
   when 'TAKE_HOME_FLOOR' then 'Limited to protect take-home pay'
   when 'PAY_HEADROOM' then 'Limited to available pay'
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
   'outstanding_after',n.outstanding_after::text,'result_basis',case when n.instruction_id is null then null else n.result_basis end,
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
-- Exact reader parents0414 and0639 retained; only named review additions.
create or replace function private.bpay_next_worker_status_page_v1(
  p_run_id uuid,p_worker_id uuid,p_after_transfer_no integer,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_header jsonb;v_row record;v_rows jsonb:='[]'::jsonb;
  v_bytes integer;v_count integer:=0;v_more boolean:=false;
  v_query text;
begin
  if p_run_id is null or p_worker_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_transfer_no is not null and p_after_transfer_no<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- One run, one worker, one accepted-projection unique lookup. During a net
  -- replacement show the accepted projection AND an explicit pending flag;
  -- never portray the new request as already accepted. Original CSV advice
  -- uses the transfer's own projection, not this current progress pointer.
  select pg_catalog.jsonb_build_object(
    'run_id',r.id,'run_status',r.status,'selection_state',r.selection_state,
    'review_revision',r.review_revision::text,'run_worker_id',w.id,
    'candidate_id',w.candidate_id,'worker_status',w.status,
    'target_pay_channel',w.target_pay_channel,'umbrella_id',w.target_umbrella_id,
    'preparation_revision',w.preparation_revision::text,
    'case_selection_revision',w.case_selection_revision::text,
    'net_request_revision',w.net_request_revision::text,
    'net_projection_revision',w.net_projection_revision::text,
    'net_pending',w.net_request_revision<>w.net_projection_revision,
    'review_issue_code',w.review_issue_code,'review_issue_work_id',w.review_issue_work_id,
    'frozen_gross_ex_vat',w.gross_ex_vat::text,'frozen_gross_vat',w.gross_vat::text,
    'frozen_gross_inc_vat',w.gross_inc_vat::text,'gross_stage_deductions',contribution.gross_stage_deductions::text,'net_recovery_reserved',contribution.net_recovery_reserved::text,'frozen_offer_complete',contribution.frozen_offer_complete,
    'projection_id',p.id,'projection_input_kind',p.input_kind,
    'entered_paye_net',p.entered_paye_net::text,
    'accepted_recoveries',p.accepted_recoveries::text,
    'accepted_net_additions',p.accepted_net_additions::text,
    'cash_amount',p.cash_amount::text,
    'reservation_mode',rv.reservation_mode,'reservation_projection_id',rv.reservation_projection_id,
    'remaining_reservation_cash',rv.remaining_reservation_cash::text,'reservation_internal_zero',rv.reservation_internal_zero,
    'reservation_holds_state',rv.reservation_holds_state
  ) into v_header
  from private.bpay_next_pay_run r
  join private.bpay_next_run_worker w on w.run_id=r.id and w.id=p_worker_id
  cross join lateral private.bpay_next_run_channel_contribution_v1(w) contribution
  left join private.bpay_next_net_projection p
    on p.run_worker_id=w.id and p.projection_no=w.net_projection_revision and p.retired_at_utc is null
  cross join lateral private.bpay_next_original_reservation_v1(w.id,null) rv
  where r.id=p_run_id;
  if not found then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';end if;
  v_bytes:=512+pg_catalog.octet_length(v_header::text);
  v_query:=$query$select pg_catalog.to_jsonb(page) as row_value from (
      select t.id as transfer_id,t.transfer_no::text,t.projection_id,t.execution_kind,
        t.status,t.cash_amount::text,t.original_transfer_id,
        rv.reservation_state,rv.remaining_reservation_cash,rv.reservation_holds_state
      from private.bpay_next_transfer t
      cross join lateral private.bpay_next_original_reservation_v1(t.run_worker_id,t.id) rv where t.run_worker_id=$1$query$;
  if p_after_transfer_no is not null then
    v_query:=v_query||' and t.transfer_no>$2';
  end if;
  v_query:=v_query||' order by t.transfer_no,t.id limit $3) page';
  for v_row in execute v_query using p_worker_id,p_after_transfer_no,p_limit
  loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return pg_catalog.jsonb_build_object('header',v_header,'rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;
alter function private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) owner to postgres;
revoke all on function private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) from public,anon,authenticated,service_role;
create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
  v_issue_run uuid;v_issue_epoch bigint;v_issue_original_epoch bigint;v_issue_wb bigint;v_issue_adoption bigint;
  v_issue_run_row private.bpay_next_pay_run%rowtype;v_issue_context jsonb;v_issue_after jsonb;v_issue_reply jsonb;v_issue_sort text;v_issue_desc boolean;
  v_issue_reason_labels jsonb;v_issue_fresh_context jsonb;v_issue_final jsonb;
begin
  if p_kind in ('REVIEW_TOTALS','REVIEW_CASES') then
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or jsonb_typeof(p_args->'run_id') is distinct from 'string' or p_args->>'run_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    if p_kind='REVIEW_TOTALS' then
      if p_args-array['run_id']<>'{}'::jsonb or not(p_args?'run_id') then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
      return private.bpay_next_run_review_totals_v1(p_actor_user_id,(p_args->>'run_id')::uuid);
    end if;
    if p_args-array['run_id','run_worker_id','sort','after','limit']<>'{}'::jsonb or not(p_args?&array['run_id','run_worker_id','sort','after','limit'])
      or jsonb_typeof(p_args->'run_worker_id') is distinct from 'string' or p_args->>'run_worker_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or jsonb_typeof(p_args->'sort') is distinct from 'string' or jsonb_typeof(p_args->'limit') is distinct from 'number'
      or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100 then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    return private.bpay_next_run_review_cases_v1(p_actor_user_id,(p_args->>'run_id')::uuid,(p_args->>'run_worker_id')::uuid,p_args->>'sort',p_args->'after',(p_args->>'limit')::integer);
  end if;
  if p_kind='PREPARE_ISSUES' then
    -- DR15 read-only original selection/diagnostic facts; no economic authority.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    select owner_epoch into v_issue_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT';
    if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
    if jsonb_typeof(p_args) is distinct from 'object' or pg_catalog.octet_length(p_args::text)>32768 or p_args-array['run_id','view','sort','after','limit','row_key','waiting_only']<>'{}'::jsonb
      or not(p_args ?& array['run_id','view','sort','after','limit','row_key','waiting_only'])
      or jsonb_typeof(p_args->'waiting_only') is distinct from 'boolean' or (p_args->'waiting_only'='true'::jsonb and p_args->>'view'<>'CANDIDATES')
      or jsonb_typeof(p_args->'run_id') is distinct from 'string' or p_args->>'run_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->>'view' not in ('CANDIDATES','NOT_IN_DRAFT') or jsonb_typeof(p_args->'view') is distinct from 'string'
      or p_args->>'sort' not in ('CANDIDATE_ASC','CANDIDATE_DESC','PAYMENT_ASC','PAYMENT_DESC','REASON_ASC','REASON_DESC','GROSS_ASC','GROSS_DESC','STATE_ASC','STATE_DESC','CHANNEL_ASC','CHANNEL_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC')
      or jsonb_typeof(p_args->'sort') is distinct from 'string' or jsonb_typeof(p_args->'limit') is distinct from 'number'
      or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100
      or (p_args->'row_key'<>'null'::jsonb and (jsonb_typeof(p_args->'row_key') is distinct from 'string' or length(p_args->>'row_key') not between 1 and 160)) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    v_issue_run:=(p_args->>'run_id')::uuid;v_limit:=(p_args->>'limit')::integer;v_issue_sort:=p_args->>'sort';v_issue_desc:=right(v_issue_sort,4)='DESC';
    select r.* into v_issue_run_row from private.bpay_next_pay_run r where r.id=v_issue_run;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_PREPARATION_RUN_MISSING';end if;
    select c.module_epoch into v_issue_original_epoch from private.bpay_next_run_command rc join private.bpay_next_command c on c.id=rc.command_id where rc.run_id=v_issue_run;
    v_issue_original_epoch:=coalesce(v_issue_original_epoch,(v_issue_run_row.accepted_choice_descriptor#>>'{namespace,owner_epoch}')::bigint);
    if v_issue_original_epoch is distinct from v_issue_epoch then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    select workbench_revision,adoption_revision into v_issue_wb,v_issue_adoption from private.bpay_next_workbench_revision where id=1;
    v_issue_context:=jsonb_build_object('run_id',v_issue_run,'actor_user_id',p_actor_user_id,'owner_epoch',v_issue_epoch::text,
      'review_revision',v_issue_run_row.review_revision::text,'run_status',v_issue_run_row.status,'selection_state',v_issue_run_row.selection_state,
      'selection_pages',v_issue_run_row.selection_page_count::text,'selected_candidates',v_issue_run_row.selected_candidate_count::text,
      'preparing',v_issue_run_row.preparing_candidate_count::text,'ready',v_issue_run_row.ready_candidate_count::text,
      'review',v_issue_run_row.review_candidate_count::text,'removed',v_issue_run_row.cancelled_candidate_count::text,
      'workbench_revision',v_issue_wb::text,'adoption_revision',v_issue_adoption::text,'view',p_args->>'view','sort',v_issue_sort,'row_key',p_args->'row_key','waiting_only',p_args->'waiting_only','paye_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='PAYE'),'umbrella_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='UMBRELLA'));
    v_issue_after:=p_args->'after';
    if v_issue_after<>'null'::jsonb then
      if jsonb_typeof(v_issue_after) is distinct from 'object' or pg_catalog.octet_length(v_issue_after::text)>8192 or v_issue_after-array['context','key']<>'{}'::jsonb or not(v_issue_after ?& array['context','key'])
        or jsonb_typeof(v_issue_after->'key') is distinct from 'array' or jsonb_array_length(v_issue_after->'key')<>3
        or jsonb_typeof(v_issue_after#>'{key,0}') is distinct from 'boolean'
        or jsonb_typeof(v_issue_after#>'{key,2}') is distinct from 'string' or length(v_issue_after#>>'{key,2}') not between 1 and 160
        or (v_issue_after#>'{key,1}'<>'null'::jsonb and (jsonb_typeof(v_issue_after#>'{key,1}') is distinct from 'string' or length(v_issue_after#>>'{key,1}')>8192))
        or ((v_issue_after#>>'{key,0}')::boolean is distinct from (v_issue_after#>'{key,1}'='null'::jsonb))
        or (split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and v_issue_after#>'{key,1}'<>'null'::jsonb and v_issue_after#>>'{key,1}'!~'^-?(0|[1-9][0-9]{0,15})(\.[0-9]{1,6})?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
      if v_issue_after->'context' is distinct from v_issue_context then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    end if;
    v_issue_reason_labels:=case when p_args->>'view'='NOT_IN_DRAFT' then '{"SOURCE_PAY_QUERY_OPEN":"Pay query open","SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE":"Could not check the pay query","SELECTED_WORK_NOT_APPROVED":"Changed after Create Draft — next Draft","SELECTED_WORK_REVISION_CHANGED":"Changed after Create Draft — next Draft","SELECTED_WORK_POSITION_PENDING":"Changed after Create Draft — next Draft","CAPTURE_POSITION_CHANGED":"The authorised Timesheet could not be prepared; try again.","POSITION_COMPONENT_EVIDENCE_MISSING":"Part of the authorised Timesheet could not be checked; try again.","POSITION_APPROVAL_AMOUNT_MISMATCH":"The authorised Timesheet amount could not be checked; try again.","UMBRELLA_WORK_EXECUTION_UNSUPPORTED":"This Timesheet cannot be paid this way; check the Timesheet.","SOURCE_BOOK_APPROVAL_UNBOUND":"The authorised Source Timesheet could not be checked; try again.","FINANCIAL_RESOLUTION_REQUIRED":"The payment needs a decision before it can be prepared.","TARGET_UMBRELLA_MISSING":"Choose the Candidate’s Umbrella before trying again.","SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE":"The Timesheet correction could not be checked; contact support.","CASE_SELECTION_REQUIRED":"The selected payments could not be checked; contact support.","CASE_TARGET_UNSUPPORTED":"The selected payment cannot be paid this way; contact support.","CASE_INPUT_UNBOUND":"The selected payment details could not be checked; contact support.","CASE_WEEK_BASIS_UNBOUND":"The payment week could not be checked; contact support.","CASE_RETURN_FLOOR_UNBOUND":"The returned payment details could not be checked; contact support.","CASE_NO_PAYABLE_AMOUNT":"Nothing to pay — deductions stay outstanding.","PF08_NEEDS_ATTENTION":"Could not prepare this Candidate after several tries.","BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE":"Not yet payable","BPAY_NEXT_SELECTION_ON_HOLD":"Pay on hold","BPAY_NEXT_SELECTION_SNOOZED":"Snoozed","BPAY_NEXT_SELECTION_WORK_NOT_APPROVED":"No longer authorised","BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT":"Changed after Create Draft — next Draft","BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","BPAY_NEXT_SIGNED_COMPONENT_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","PAYE_EXCLUDED_BY_SAME_WEEK_GUARD":"A PAYE Draft already exists for this pay week","ORIGIN_UNAUTHORISED_WAIT":"Recovery paused — waiting for re-authorisation","DRAFT_CANDIDATE_REMOVED":"Removed from Draft — payments are back in Banking Pay.","PREPARATION_ENROLMENT_FAILED":"Could not prepare this Candidate; contact support.","PREPARATION_EXPIRED":"The Draft expired — payments are back in Banking Pay.","DRAFT_DISCARDED":"The Draft was discarded — payments are back in Banking Pay."}'::jsonb else '{"SOURCE_PAY_QUERY_OPEN":"Pay Query open on this Timesheet — resolve it in Weekly Source Office Checks","SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE":"Pay Query evidence could not be confirmed — check the Timesheet''s queries","SELECTED_WORK_NOT_APPROVED":"The Timesheet is no longer authorised.","SELECTED_WORK_REVISION_CHANGED":"The authorised Timesheet changed after Create Draft.","SELECTED_WORK_POSITION_PENDING":"The authorised Timesheet changed after Create Draft.","CAPTURE_POSITION_CHANGED":"The authorised Timesheet could not be prepared; try again.","POSITION_COMPONENT_EVIDENCE_MISSING":"Part of the authorised Timesheet could not be checked; try again.","POSITION_APPROVAL_AMOUNT_MISMATCH":"The authorised Timesheet amount could not be checked; try again.","UMBRELLA_WORK_EXECUTION_UNSUPPORTED":"This Timesheet cannot be paid this way; check the Timesheet.","SOURCE_BOOK_APPROVAL_UNBOUND":"The authorised Source Timesheet could not be checked; try again.","FINANCIAL_RESOLUTION_REQUIRED":"The payment needs a decision before it can be prepared.","TARGET_UMBRELLA_MISSING":"Choose the Candidate’s Umbrella before trying again.","SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE":"The Timesheet correction could not be checked; contact support.","CASE_SELECTION_REQUIRED":"The selected payments could not be checked; contact support.","CASE_TARGET_UNSUPPORTED":"The selected payment cannot be paid this way; contact support.","CASE_INPUT_UNBOUND":"The selected payment details could not be checked; contact support.","CASE_WEEK_BASIS_UNBOUND":"The payment week could not be checked; contact support.","CASE_RETURN_FLOOR_UNBOUND":"The returned payment details could not be checked; contact support.","CASE_NO_PAYABLE_AMOUNT":"Nothing to pay — deductions stay outstanding.","PF08_NEEDS_ATTENTION":"Could not prepare this Candidate after several tries.","BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE":"Not yet payable","BPAY_NEXT_SELECTION_ON_HOLD":"Pay on hold","BPAY_NEXT_SELECTION_SNOOZED":"Snoozed","BPAY_NEXT_SELECTION_WORK_NOT_APPROVED":"No longer authorised","BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT":"Changed after Create Draft — next Draft","BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","BPAY_NEXT_SIGNED_COMPONENT_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","PAYE_EXCLUDED_BY_SAME_WEEK_GUARD":"A PAYE Draft already exists for this pay week","ORIGIN_UNAUTHORISED_WAIT":"Recovery paused — waiting for re-authorisation","DRAFT_CANDIDATE_REMOVED":"Removed from Draft — payments are back in Banking Pay.","PREPARATION_ENROLMENT_FAILED":"Could not prepare this Candidate; contact support.","PREPARATION_EXPIRED":"The Draft expired — payments are back in Banking Pay.","DRAFT_DISCARDED":"The Draft was discarded — payments are back in Banking Pay."}'::jsonb end;
    with choice_skips as (
      select 'SKIP:'||m.work_id::text row_key,'SKIPPED_WORK'::text origin,m.candidate_id,null::uuid worker_id,m.work_id,m.expected_revision_id revision_id,
        'NOT_IN_DRAFT'::text state,m.skip_code code,null::numeric gross,null::bigint preparation_revision
      from private.bpay_next_run_choice_member m where m.run_id=v_issue_run and m.member_kind='WORK' and m.selected and m.skip_code is not null
    ), page_skips as (
      select distinct on (w.id) 'PAGE:'||p.page_no::text||':'||(s.value->>'work_id') row_key,'SKIPPED_WORK'::text origin,w.candidate_id,null::uuid worker_id,w.id work_id,
        (ch.value->>'expected_revision_id')::uuid revision_id,'NOT_IN_DRAFT'::text state,s.value->>'code' code,null::numeric gross,null::bigint preparation_revision
      from private.bpay_next_selection_page p cross join lateral jsonb_array_elements(p.skipped_json) s(value)
      join private.bpay_next_work w on w.id=(s.value->>'work_id')::uuid
      left join lateral (select value from jsonb_array_elements(coalesce(p.choices_request_json,'[]'::jsonb)) where value->>'work_id'=w.id::text limit 1) ch on true
      where p.run_id=v_issue_run and not exists(select 1 from private.bpay_next_run_choice_member m where m.run_id=v_issue_run and m.member_kind='WORK' and m.work_id=w.id and m.selected and m.skip_code is not null)
      order by w.id,p.page_no
    ), releases as (
      select 'RELEASE:'||q.run_worker_id::text row_key,case when q.initiator_kind='SYSTEM' then 'SYSTEM_RELEASE' else 'OFFICE_RELEASE' end origin,
        q.candidate_id,q.run_worker_id worker_id,q.original_issue_work_id work_id,coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,
        'NOT_IN_DRAFT'::text state,case when q.initiator_kind='SYSTEM' then q.original_issue_code else 'DRAFT_CANDIDATE_REMOVED' end code,
        null::numeric gross,q.target_preparation_revision preparation_revision
      from private.bpay_next_worker_reprepare_request q
      left join private.bpay_next_run_work rw on rw.run_worker_id=q.run_worker_id and rw.work_id=q.original_issue_work_id and rw.candidate_id=q.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=q.original_issue_work_id and cm.candidate_id=q.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=q.original_issue_work_id
      where q.run_id=v_issue_run and q.action='RELEASE' and q.status='COMPLETE' and q.stage='COMPLETE' and q.finished_at_utc is not null
    ), expired as (
      select 'EXPIRED:'||x.candidate_id::text row_key,'EXPIRED'::text origin,x.candidate_id,x.run_worker_id worker_id,x.original_review_issue_work_id work_id,
        coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,'NOT_IN_DRAFT'::text state,'PREPARATION_EXPIRED'::text code,null::numeric gross,x.preparation_revision
      from private.bpay_next_preparation_expiry_worker x
      left join private.bpay_next_run_work rw on rw.run_worker_id=x.run_worker_id and rw.work_id=x.original_review_issue_work_id and rw.candidate_id=x.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=x.original_review_issue_work_id and cm.candidate_id=x.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=x.original_review_issue_work_id
      where x.run_id=v_issue_run and x.status='EXPIRED' and x.stage='COMPLETE' and x.finished_at_utc is not null
        and not exists(select 1 from releases q where q.candidate_id=x.candidate_id)
    ), discarded as (
      select 'DISCARDED:'||x.candidate_id::text row_key,'DISCARDED'::text origin,x.candidate_id,x.run_worker_id worker_id,x.original_review_issue_work_id work_id,
        coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,'NOT_IN_DRAFT'::text state,'DRAFT_DISCARDED'::text code,null::numeric gross,x.preparation_revision
      from private.bpay_next_preparation_discard_worker x
      left join private.bpay_next_run_work rw on rw.run_worker_id=x.run_worker_id and rw.work_id=x.original_review_issue_work_id and rw.candidate_id=x.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=x.original_review_issue_work_id and cm.candidate_id=x.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=x.original_review_issue_work_id
      where x.run_id=v_issue_run and x.status='DISCARDED' and x.stage='COMPLETE' and x.finished_at_utc is not null and x.outcome in ('REMOVED','ALREADY_REMOVED')
        and not exists(select 1 from releases q where q.candidate_id=x.candidate_id)
    ), candidates as (
      select 'CANDIDATE:'||sc.candidate_id::text row_key,'CANDIDATE'::text origin,sc.candidate_id,rw.id worker_id,rw.review_issue_work_id work_id,
        coalesce(cw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,
        coalesce(rw.status,case when cmd.status='FAILED' then 'FAILED' else 'PENDING' end) state,coalesce(rw.review_issue_code,case when rw.id is null and cmd.status='FAILED' then 'PREPARATION_ENROLMENT_FAILED' end) code,
        case when rw.status in ('READY','DRAFT','ISSUED','COMPLETE') or (rw.status='REVIEW' and exists(select 1 from private.bpay_next_case_allocation_state completed
        where completed.run_worker_id=rw.id and completed.preparation_revision=rw.captured_preparation_revision and completed.selection_revision=rw.case_selection_revision
          and completed.pass_kind='DRAFT' and completed.projection_no=0 and completed.completed_at_utc is not null)) then rw.gross_ex_vat end gross,rw.preparation_revision
      from private.bpay_next_selection_candidate sc
      left join private.bpay_next_run_worker rw on rw.run_id=sc.run_id and rw.candidate_id=sc.candidate_id
      left join private.bpay_next_run_work cw on cw.run_worker_id=rw.id and cw.work_id=rw.review_issue_work_id and cw.candidate_id=sc.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=sc.run_id and cm.member_kind='WORK' and cm.work_id=rw.review_issue_work_id and cm.candidate_id=sc.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=sc.run_id and wc.work_id=rw.review_issue_work_id
      left join private.bpay_next_run_command rc on rc.run_id=sc.run_id left join private.bpay_next_command cmd on cmd.id=rc.command_id
      where sc.run_id=v_issue_run
    ), facts as (
      select * from choice_skips union all select * from page_skips union all select * from releases union all select * from expired union all select * from discarded union all select * from candidates
    ), labelled as (
      select f.*,
        case when f.origin='CANDIDATE' then coalesce(review_worker.target_pay_channel,selected_candidate.target_pay_channel) end pay_channel,
        case when f.origin='CANDIDATE' and contribution.frozen_offer_complete then contribution.gross_stage_deductions+contribution.net_recovery_reserved end deduction_total,
        f.origin='CANDIDATE' and coalesce(contribution.frozen_offer_complete,false) frozen_offer_complete,
        case when f.origin='CANDIDATE' then coalesce(case_selection.item_count,0) end case_count,
        wr.physical_timesheet_id,wr.physical_timesheet_version,wr.timesheet_reference,wr.week_ending_date,
        coalesce(wr.candidate_display_name,original_label.candidate_display_name,case when f.work_id is null then coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),'')) end) candidate_name,
        case when wr.id is not null then wr.candidate_reference when original_label.id is not null then original_label.candidate_reference else case when f.work_id is null then c.tms_ref end end candidate_reference,
        case when wr.id is not null or original_label.id is not null then 'CAPTURED_WORK' when f.work_id is null then 'CURRENT_CANDIDATE' else 'UNKNOWN_ORIGINAL' end label_origin,
        x.outcome discard_outcome,x.blocked_code discard_blocked_code,
        wait.waiting_run_id is not null waiting_on_other_run,wait.waiting_run_id,wait.waiting_actor_display_name,
        f.origin='CANDIDATE' and exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=f.worker_id
          and i.preparation_revision=(select own.captured_preparation_revision from private.bpay_next_run_worker own where own.id=f.worker_id)
          and i.other_run_capacity_reduction_target_ex_vat>0) capacity_reduced_by_other_run
      from facts f
      left join private.bpay_next_run_worker review_worker on review_worker.id=f.worker_id and review_worker.run_id=v_issue_run
      left join private.bpay_next_selection_candidate selected_candidate on selected_candidate.run_id=v_issue_run and selected_candidate.candidate_id=f.candidate_id
      left join private.bpay_next_case_selection case_selection on case_selection.run_id=v_issue_run and case_selection.candidate_id=f.candidate_id
        and case_selection.selection_revision=review_worker.case_selection_revision and case_selection.status='SEALED'
      left join lateral private.bpay_next_run_channel_contribution_v1(review_worker) contribution on f.origin='CANDIDATE'
      left join lateral private.bpay_next_preparation_wait_owner_v1(v_issue_run,f.candidate_id) wait on f.origin='CANDIDATE'
      left join private.bpay_next_work w on w.id=f.work_id and w.candidate_id=f.candidate_id
      left join private.bpay_next_work_revision wr on wr.work_id=w.id and wr.id=f.revision_id
      left join lateral (
        select r.id,r.candidate_display_name,r.candidate_reference from private.bpay_next_run_choice_member m
        join private.bpay_next_work_revision r on r.work_id=m.work_id and r.id=m.expected_revision_id
        where m.run_id=v_issue_run and m.candidate_id=f.candidate_id and m.member_kind='WORK' and m.selected order by m.member_no limit 1
      ) original_label on f.work_id is null and wr.id is null
      left join public.candidates c on c.id=f.candidate_id
      left join private.bpay_next_preparation_discard_worker x on x.run_id=v_issue_run and x.candidate_id=f.candidate_id and f.origin='CANDIDATE'
    ), scoped as (
      select l.*,case split_part(v_issue_sort,'_',1) when 'CANDIDATE' then lower(l.candidate_name) when 'PAYMENT' then case when l.timesheet_reference is not null then 'Timesheet '||l.timesheet_reference when l.work_id is null then 'Payments' else 'Timesheet' end
        when 'REASON' then case when l.waiting_on_other_run then 'Waiting for another Draft'||coalesce(' started by '||l.waiting_actor_display_name,'')||'.' else coalesce(v_issue_reason_labels->>l.code,case when l.state='PENDING' then 'Waiting to prepare' when l.state in ('PREPARING','CANCELLING') then 'Preparing' else 'Could not prepare this Candidate; contact support.' end) end
        when 'STATE' then case when l.waiting_on_other_run then '1:Waiting' else (case l.state when 'REVIEW' then '0' when 'FAILED' then '0' when 'PREPARING' then '1' when 'PENDING' then '2' when 'READY' then '3' when 'DRAFT' then '4' when 'ISSUED' then '5' when 'COMPLETE' then '6' when 'CANCELLING' then '7' else '8' end)||':'||case l.state when 'FAILED' then 'To review' when 'DRAFT' then 'Confirmed' when 'ISSUED' then 'Issued' when 'COMPLETE' then 'Complete' when 'NOT_IN_DRAFT' then 'Not in this Draft' when 'PENDING' then 'Waiting to prepare' when 'PREPARING' then 'Preparing' when 'REVIEW' then 'To review' when 'READY' then 'Ready' when 'CANCELLING' then 'Removing' when 'CANCELLED' then 'Not in this Draft' else l.state end end
        when 'GROSS' then l.gross::text when 'CHANNEL' then case l.pay_channel when 'UMBRELLA' then 'Umbrella' else l.pay_channel end when 'DEDUCTIONS' then l.deduction_total::text end sort_value
      from labelled l where ((p_args->>'view'='NOT_IN_DRAFT' and l.origin<>'CANDIDATE') or (p_args->>'view'='CANDIDATES' and l.origin='CANDIDATE')) and (p_args->'waiting_only'='false'::jsonb or l.waiting_on_other_run)
    ), tail as (
      select sort_value from scoped where row_key=v_issue_after#>>'{key,2}'
    ), counts as (
      select (select count(distinct work_id) from facts where origin='SKIPPED_WORK') skipped_timesheets,
        (select count(distinct candidate_id) from facts where origin in ('SYSTEM_RELEASE','OFFICE_RELEASE','EXPIRED','DISCARDED')) removed_candidates,
        (select count(*) from scoped) scope_count,
        (select count(*) from tail where sort_value is not distinct from v_issue_after#>>'{key,1}') tail_matches
    ), bounded as (
      select n.* from scoped n where (p_args->'row_key'='null'::jsonb or n.row_key=p_args->>'row_key')
        and (v_issue_after='null'::jsonb or case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') then
          case when v_issue_desc then (n.sort_value is null,coalesce(n.sort_value::numeric,0),n.row_key collate "C")<((v_issue_after#>>'{key,0}')::boolean,coalesce((v_issue_after#>>'{key,1}')::numeric,0),(v_issue_after#>>'{key,2}') collate "C")
            else (n.sort_value is null,coalesce(n.sort_value::numeric,0),n.row_key collate "C")>((v_issue_after#>>'{key,0}')::boolean,coalesce((v_issue_after#>>'{key,1}')::numeric,0),(v_issue_after#>>'{key,2}') collate "C") end
          else case when v_issue_desc then (n.sort_value is null,coalesce(n.sort_value,'') collate "C",n.row_key collate "C")<((v_issue_after#>>'{key,0}')::boolean,coalesce(v_issue_after#>>'{key,1}','') collate "C",(v_issue_after#>>'{key,2}') collate "C")
            else (n.sort_value is null,coalesce(n.sort_value,'') collate "C",n.row_key collate "C")>((v_issue_after#>>'{key,0}')::boolean,coalesce(v_issue_after#>>'{key,1}','') collate "C",(v_issue_after#>>'{key,2}') collate "C") end end)
      order by case when not v_issue_desc then n.sort_value is null end asc,case when v_issue_desc then n.sort_value is null end desc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and not v_issue_desc then n.sort_value::numeric end asc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and v_issue_desc then n.sort_value::numeric end desc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and not v_issue_desc then n.sort_value end collate "C" asc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and v_issue_desc then n.sort_value end collate "C" desc,
        case when not v_issue_desc then n.row_key end collate "C" asc,case when v_issue_desc then n.row_key end collate "C" desc limit v_limit+1
    ) select jsonb_build_object('skipped_timesheets',counts.skipped_timesheets::text,'removed_candidates',counts.removed_candidates::text,'scope_count',counts.scope_count::text,'tail_matches',counts.tail_matches::text,
      'rows',coalesce((select jsonb_agg(jsonb_build_object('row_key',b.row_key,'origin',b.origin,'candidate_id',b.candidate_id,'run_worker_id',b.worker_id,'worker_status',b.state,'issue_code',b.code,
        'work_id',b.work_id,'expected_revision_id',b.revision_id,'physical_timesheet_id',b.physical_timesheet_id,'physical_timesheet_version',b.physical_timesheet_version::text,
        'candidate_display_name',b.candidate_name,'candidate_reference',b.candidate_reference,'label_origin',b.label_origin,'timesheet_reference',b.timesheet_reference,'week_ending_date',b.week_ending_date,
        'frozen_ex_vat',b.gross::text,'pay_channel',b.pay_channel,'deduction_total',b.deduction_total::text,'frozen_offer_complete',b.frozen_offer_complete,'case_count',b.case_count::text,'preparation_revision',b.preparation_revision::text,'discard_outcome',b.discard_outcome,'discard_blocked_code',b.discard_blocked_code,
        'waiting_on_other_run',b.waiting_on_other_run,'waiting_run_id',b.waiting_run_id,'waiting_actor_display_name',b.waiting_actor_display_name,'capacity_reduced_by_other_run',b.capacity_reduced_by_other_run,
        'sort_key',jsonb_build_array(b.sort_value is null,b.sort_value,b.row_key)) order by
        case when not v_issue_desc then b.sort_value is null end asc,case when v_issue_desc then b.sort_value is null end desc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and not v_issue_desc then b.sort_value::numeric end asc,
        case when split_part(v_issue_sort,'_',1) in ('GROSS','DEDUCTIONS') and v_issue_desc then b.sort_value::numeric end desc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and not v_issue_desc then b.sort_value end collate "C" asc,
        case when split_part(v_issue_sort,'_',1) not in ('GROSS','DEDUCTIONS') and v_issue_desc then b.sort_value end collate "C" desc,
        case when not v_issue_desc then b.row_key end collate "C" asc,case when v_issue_desc then b.row_key end collate "C" desc) from bounded b),'[]'::jsonb)) into v_issue_reply from counts;
    if v_issue_after<>'null'::jsonb and v_issue_reply->>'tail_matches'<>'1' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    v_complete:=jsonb_array_length(v_issue_reply->'rows')<=v_limit;v_rows:='[]'::jsonb;v_n:=0;
    for v_row in select value from jsonb_array_elements(v_issue_reply->'rows') limit v_limit loop
      if exists(select 1 from jsonb_each_text(v_row) e where e.key in ('candidate_display_name','candidate_reference','timesheet_reference') and pg_catalog.octet_length(e.value)>8192) then
        raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_issue_final:=jsonb_build_object('context',v_issue_context,'rows',v_rows||jsonb_build_array(v_row),'complete',false,
        'next_cursor',jsonb_build_object('context',v_issue_context,'key',v_row->'sort_key'),'counts',v_issue_reply-array['rows','tail_matches']);
      if pg_catalog.octet_length(v_issue_final::text)>120000 then
        if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
        v_complete:=false;exit;end if;
      v_rows:=v_rows||jsonb_build_array(v_row);v_n:=v_n+1;
    end loop;
    -- The same native context closes a first-read/concurrent-commit gap.
    select jsonb_build_object('run_id',v_issue_run,'actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text,
      'review_revision',r.review_revision::text,'run_status',r.status,'selection_state',r.selection_state,
      'selection_pages',r.selection_page_count::text,'selected_candidates',r.selected_candidate_count::text,
      'preparing',r.preparing_candidate_count::text,'ready',r.ready_candidate_count::text,
      'review',r.review_candidate_count::text,'removed',r.cancelled_candidate_count::text,
      'workbench_revision',b.workbench_revision::text,'adoption_revision',b.adoption_revision::text,'view',p_args->>'view','sort',v_issue_sort,'row_key',p_args->'row_key','waiting_only',p_args->'waiting_only','paye_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='PAYE'),'umbrella_totals_revision',(select totals_revision::text from private.bpay_next_run_channel_totals where run_id=v_issue_run and pay_channel='UMBRELLA')) into v_issue_fresh_context
      from private.bpay_next_pay_run r cross join private.bpay_next_module_control m cross join private.bpay_next_workbench_revision b
      where r.id=v_issue_run and m.id=1 and m.active_owner='NEXT' and b.id=1;
    if v_issue_fresh_context is distinct from v_issue_context then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    v_issue_final:=jsonb_build_object('context',v_issue_context,'rows',v_rows,'complete',v_complete,
      'next_cursor',case when v_complete then null else jsonb_build_object('context',v_issue_context,'key',v_rows->-1->'sort_key') end,
      'counts',v_issue_reply-array['rows','tail_matches']);
    if pg_catalog.octet_length(v_issue_final::text)>120000 or (not v_complete and pg_catalog.octet_length((v_issue_final->'next_cursor')::text)>8192) then
      raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
    return v_issue_final;
  end if;
  if p_kind in ('CASE_AUDIT','CANDIDATE_CASE_REPORT') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_finance_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind in ('CASE_LIST','CASE_SUMMARY') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_list_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind='CASE_PREVIEW' then
    -- FC-03 genuinely read-only: no authority helper, receipt, business lock or wake.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if p_args ? 'case_id' or p_args ? 'expected_case_revision' then
      if jsonb_typeof(p_args) is distinct from 'object' or p_args-array['input','case_id','expected_case_revision']<>'{}'::jsonb
        or not(p_args ?& array['input','case_id','expected_case_revision'])
        or jsonb_typeof(p_args->'case_id') is distinct from 'string'
        or p_args->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(p_args->'expected_case_revision') is distinct from 'string'
        or p_args->>'expected_case_revision'!~'^[1-9][0-9]{0,18}$'
        or (p_args->>'expected_case_revision')::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      return jsonb_build_object('rows',jsonb_build_array(private.bpay_next_case_edit_preview_v1(p_actor_user_id,
        (p_args->>'case_id')::uuid,(p_args->>'expected_case_revision')::bigint,p_args->'input')),'complete',true);
    end if;
    if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object' or (p_args-'input') is distinct from '{}'::jsonb
      or not(p_args ? 'input') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_case_create_preview_v1(p_actor_user_id,p_args->'input')),'complete',true);
  end if;
  if p_kind='GUARD' then
   -- Read-only guard takes no business locks or mutating authority helper.
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   v_keys:=array['run_id']||case when p_args ? 'descriptor' then array['descriptor'] else array[]::text[] end;
   perform private.bpay_next_preparation_args_v1(p_args,v_keys);
   return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_paye_guard_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,p_args->'descriptor')),'complete',true);
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'PROGRESS' then array['run_id']
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit']||case when p_args ? 'member_kind' then array['member_kind'] else array[]::text[] end else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']||case when p_args ? 'attention_only' then array['attention_only'] else array[]::text[] end
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
      ||case when p_args ? 'edit_command_id' then array['edit_command_id'] else array[]::text[] end
      ||case when p_args ? 'pause_command_id' then array['pause_command_id'] else array[]::text[] end
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(case when p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then p_args-'member_kind' else p_args end,array_remove(v_keys,'member_kind'));
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    if p_args->'accepted_inputs' is distinct from 'true'::jsonb or (p_args ? 'member_kind' and (pg_catalog.jsonb_typeof(p_args->'member_kind') is distinct from 'string' or p_args->>'member_kind' not in ('WORK','CASE_COMPONENT'))) then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer,coalesce(p_args->>'member_kind','WORK'));
  end if;
  if p_kind='WORKERS' and p_args ? 'attention_only' then
    if p_args->'attention_only' is distinct from 'true'::jsonb then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    return private.bpay_next_prepare_attention_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,(p_args->>'limit')::integer);
  end if;
  if p_kind='PROGRESS' then
    v_progress:=private.bpay_next_preparation_progress_v1((p_args->>'run_id')::uuid);
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_progress),'complete',true);
  end if;
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='CHANNEL_DRAFTS' then
    return private.bpay_next_channel_drafts_page_v1((p_args->>'run_id')::uuid);
  elsif p_kind='CHANNEL_WORKERS' then
    return private.bpay_next_channel_worker_page_v1((p_args->>'run_id')::uuid,
      p_args->>'pay_channel',(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_STATUS' then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    return private.bpay_next_channel_status_page_v1((p_args->>'run_id')::uuid,p_args->>'pay_channel',
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  -- Only the two exact factual0580 owners; no competing reader or financial
  -- history/calculation path. CURRENT_WORK does not certify payability.
  if p_kind='CURRENT_WORK' then
    return private.bpay_next_current_work_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_week')::date,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CURRENT_CASES' then
    return private.bpay_next_current_case_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_case_id')::uuid,v_limit);
  elsif p_kind='CURRENT_STORED_CREDITS' then
    -- Factual, exact-Candidate possible origins only; binding revalidates the
    -- retained creator/component/account facts. This is not pay eligibility.
    -- Closed raw reply includes scan_cursor from the bounded raw window;
    -- filtering can produce an empty nonterminal page without restarting it.
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,v_limit);
  elsif p_kind='CASE_COMPONENTS' then
    v_progress:=private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
    if p_args ? 'edit_command_id' then
      if jsonb_typeof(p_args->'edit_command_id') is distinct from 'string'
        or p_args->>'edit_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_edit_receipt_v1((p_args->>'edit_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('edit_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if p_args ? 'pause_command_id' then
      if jsonb_typeof(p_args->'pause_command_id') is distinct from 'string'
         or p_args->>'pause_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_PAUSE_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_pause_receipt_v1((p_args->>'pause_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('pause_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if pg_catalog.octet_length(v_progress::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
    return v_progress;
  elsif p_kind in ('STATUS','RETURN_CASH') then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    if p_kind='RETURN_CASH' then
      return private.bpay_next_return_cash_page_v1((p_args->>'run_id')::uuid,
        (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
    end if;
    return private.bpay_next_worker_status_page_v1((p_args->>'run_id')::uuid,
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  if p_kind in ('COMPONENTS','SHIFTS','BREAKS','RATES') then
    if not exists(select 1 from private.bpay_next_work w
      join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
      where w.id=v_work and w.approval_state='APPROVED' and w.current_revision_id=v_revision
        and w.applied_revision_id=v_revision and r.sealed_at_utc is not null and r.approved_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if p_kind<>'COMPONENTS' and not exists(select 1 from private.bpay_next_approved_line l where l.id=v_line and l.revision_id=v_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    if p_kind='BREAKS' and not exists(select 1 from private.bpay_next_shift_detail s where s.id=v_shift and s.approved_line_id=v_line) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
  elsif p_kind in ('WORK_CHOICES','SELECTED_COMPONENTS') then
    perform 1 from private.bpay_next_pay_run r where r.id=(p_args->>'run_id')::uuid;
    if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    if p_kind='SELECTED_COMPONENTS' then
      perform 1 from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work;
      if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    end if;
  end if;
  -- Each branch is a <=100 indexed relational page. A byte boundary ends
  -- before an entire row, never truncates facts; callers resume its last key.
  -- CASE evaluates only its chosen subquery, so unrelated review owners are
  -- never called with NULL scope. Each subquery orders/limits BEFORE packing.
  select case p_kind
    when 'COMPONENTS' then array(
      select pg_catalog.jsonb_build_object(
        'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind,
        'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text,
        'unit_label',l.unit_label,'approved_quantity',l.approved_quantity::text,
        'approved_unit_rate',l.approved_unit_rate::text,
        'job_title',r.job_title,'band_label',r.band_label,
        'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
        'is_adjustment',ts.is_adjustment,'correction_kind',ts.correction_kind,
        'associated_rate_schedule_id',rs.id,'associated_rate_family',rs.rate_family,'associated_rate_code',rs.rate_code,
        'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text,
        'standard_rate_schedule',case when l.component_kind in ('WORK','PROTECTED_WORK') then
          (select coalesce(jsonb_agg(snapshot.value order by snapshot.bucket_order),'[]'::jsonb) from (
            select jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code,
              'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text,
              'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text) value,
              case sr.rate_code when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 end bucket_order
            from private.bpay_next_rate_schedule sr where sr.revision_id=v_revision and sr.rate_family='STANDARD'
              and sr.rate_code in ('DAY','NIGHT','SAT','SUN','BH') order by bucket_order limit 5
          ) snapshot) else '[]'::jsonb end,
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
        join private.bpay_next_work w on w.id=v_work and w.id=r.work_id
        left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
        left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
          and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=w.candidate_id)
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and ((l.component_kind='ADDITIONAL' and l.component_key like 'ADDITIONAL:%'
            and rs.rate_family='ADDITIONAL' and rs.rate_code=substring(l.component_key from 12))
            or (l.component_kind='MILEAGE' and l.component_key='MILEAGE:MILE' and rs.rate_family='MILEAGE' and rs.rate_code='MILE'))
        left join private.bpay_next_position p on p.work_id=v_work and p.component_key=l.component_key
        where l.revision_id=v_revision and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
        order by l.line_no,l.id limit v_limit)
    when 'WORK_CHOICES' then array(
      select pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id,
        'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)
        from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid
          and ((p_args->>'after_work_id') is null or c.work_id>(p_args->>'after_work_id')::uuid)
        order by c.work_id limit v_limit)
    when 'SELECTED_COMPONENTS' then array(
      select pg_catalog.jsonb_build_object('selection_no',c.selection_no::text,
        'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)
        from private.bpay_next_selected_component c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work
          and c.selection_no>coalesce((p_args->>'after_selection_no')::bigint,0)
        order by c.selection_no limit v_limit)
    when 'WORKERS' then array(
      select pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id,
        'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text,
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code,
        'discard_outcome',x.outcome,'discard_blocked_code',x.blocked_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
        left join private.bpay_next_preparation_discard_worker x on x.run_id=(p_args->>'run_id')::uuid and x.candidate_id=w.candidate_id
        order by w.candidate_id limit v_limit)
    when 'REVISION_COMPARISON' then array(
      select pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id,
        'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)
        from private.bpay_next_simple_revision_comparison_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit) w
        order by w.work_id limit v_limit)
    when 'LINES' then array(
      select pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text,
        'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
        'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)
        from private.bpay_next_simple_review_line_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_line_no')::bigint,v_limit) l
        order by l.line_no limit v_limit)
    when 'SHIFTS' then array(
      select pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date,
        'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local,
        'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text,
        'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded,
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text,
        'segment_key',s.segment_key,'approved_segment_id',sg.id,'job_title',wr.job_title,'band_label',wr.band_label)
        from private.bpay_next_shift_detail s
        join private.bpay_next_approved_line al on al.id=s.approved_line_id and al.revision_id=v_revision
        join private.bpay_next_work_revision wr on wr.id=al.revision_id and wr.work_id=v_work
        left join private.bpay_next_approved_segment sg on sg.shift_detail_id=s.id
          and sg.revision_id=v_revision and sg.approved_line_id=v_line and sg.segment_key=s.segment_key
        where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text,
        'rate_schedule_id',rs.id,'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text)
        from private.bpay_next_rate_detail r
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and rs.rate_family='STANDARD' and rs.rate_code=r.bucket
        where r.approved_line_id=v_line
          and ((p_args->>'after_bucket') is null or r.bucket>p_args->>'after_bucket')
        order by r.bucket limit v_limit)
  end into v_page;
  foreach v_row in array v_page
  loop
    if pg_catalog.octet_length((v_rows||pg_catalog.jsonb_build_array(v_row))::text)>120000 then
      if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_complete:=false;exit;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);v_n:=v_n+1;
  end loop;
  if v_n=v_limit then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete);
end
$function$;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
notify pgrst, 'reload schema';
commit;
