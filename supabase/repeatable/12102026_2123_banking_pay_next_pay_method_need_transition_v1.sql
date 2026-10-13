-- WB-29 latest explicit rate/gross scope must be reclassified on the next EX08 transition.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_pay_method_change_scope_v1(p_candidate_id uuid,p_context jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare v_result jsonb:='[]'::jsonb;v_row jsonb;v record;v_q record;v_class text;v_quote jsonb;v_amount numeric;
begin
 if(select count(*) from(select group_key from private.bpay_next_workbench_group where candidate_id=p_candidate_id limit 1001) bounded)>1000 then
  raise exception using errcode='54000',message='BPAY_NEXT_PAY_METHOD_SCOPE_TOO_LARGE';end if;
 if exists(select 1 from private.bpay_next_workbench_group where candidate_id=p_candidate_id
  and(dirty or not complete or progress_state<>'CURRENT')) then
  raise exception using errcode='55000',message='BPAY_NEXT_PAY_METHOD_CONTEXT_UNAVAILABLE';end if;
 for v in
 with case_bindings as materialized (
  select b.binding_kind,b.run_id,b.case_component_id,b.native_id,b.channel,b.protected_amount
  from private.bpay_next_workbench_group g cross join lateral private.bpay_next_case_protected_bindings_v1(g.case_id,null,1001) b
  where g.candidate_id=p_candidate_id and g.group_kind='CASE' limit 1001
 ), future as (
  select 'F:'||i.group_key||':'||i.member_kind||':'||i.member_key||':'||i.portion_kind as native_key,
   case when i.work_id is not null then 'WORK' else 'CASE' end as member_kind,
   i.work_id,i.revision_id,i.approved_line_id,i.approved_segment_id,i.case_id,i.case_component_id,
   cc.component_revision as case_component_revision,null::uuid as transfer_id,null::uuid as run_id,
   i.source_pay_channel,wr.source_umbrella_id,
   coalesce(wr.valuation_policy_id,cc.valuation_policy_id) as valuation_policy_id,
   i.tax_treatment,i.component_kind,
   case when i.amount_known then i.nominal_source_ex_vat::numeric end as source_amount,
   left(coalesce(nullif(i.timesheet_reference,''),case when i.component_kind in ('LOAN','ADVANCE') then 'Payment Advance repayment'
     when i.case_id is not null then 'Case payment' else 'Timesheet' end),256) as label,
   left(coalesce(i.client_display_name,''),256) as detail,
   i.section as original_section,i.reason_code
  from private.bpay_next_workbench_group g join private.bpay_next_workbench_item i on i.group_key=g.group_key and i.candidate_id=g.candidate_id
   left join private.bpay_next_work_revision wr on wr.id=i.revision_id
   left join private.bpay_next_case_component cc on cc.id=i.case_component_id
  where g.candidate_id=p_candidate_id and i.member_kind<>'GROUP_EVIDENCE' and i.portion_kind in ('AVAILABLE','ZERO')
   and i.accepted_run_id is null and not i.is_selected
   -- Accepted Create precedes projection. Its native choices protect the exact
   -- selected lines now, while unselected lines of a SUBSET remain future work.
   and not exists(select 1 from private.bpay_next_run_choice_member m
    join private.bpay_next_pay_run pr on pr.id=m.run_id and pr.accepted_choice_descriptor is not null
    left join private.bpay_next_run_worker rw on rw.run_id=m.run_id and rw.candidate_id=m.candidate_id
    where m.candidate_id=i.candidate_id and m.work_id=i.work_id and m.expected_revision_id=i.revision_id
     and m.member_kind='WORK' and m.selected and m.skip_code is null
     and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING') and(rw.id is null or rw.status not in ('CANCELLED','COMPLETE'))
     and(m.selection_mode='ALL'
      or exists(select 1 from private.bpay_next_selected_component s where s.run_id=m.run_id and s.work_id=m.work_id and s.approved_line_id=i.approved_line_id)
      or exists(select 1 from jsonb_array_elements(pr.accepted_choice_descriptor#>'{intent,components}') line
        where line->>'work_id'=i.work_id::text and line->>'approved_line_id'=i.approved_line_id::text and line->'selected'='true'::jsonb)))
   and not exists(select 1 from case_bindings b where b.case_component_id=i.case_component_id and b.binding_kind='PENDING')
  order by i.group_key,i.member_kind,i.member_key,i.portion_kind limit 1001
 ), protected_work as (
  select distinct 'PW:'||m.run_id::text||':'||m.work_id::text as native_key,'PROTECTED_WORK'::text as member_kind,
   m.work_id,m.expected_revision_id as revision_id,null::uuid as approved_line_id,null::uuid as approved_segment_id,
   null::uuid as case_id,null::uuid as case_component_id,null::bigint as case_component_revision,null::uuid as transfer_id,m.run_id,
   coalesce(sc.target_pay_channel,rw.target_pay_channel) as source_pay_channel,
   coalesce(sc.target_umbrella_id,rw.target_umbrella_id) as source_umbrella_id,
   null::uuid as valuation_policy_id,null::text as tax_treatment,null::text as component_kind,
   null::numeric as source_amount,
   'Draft'::text as label,'Keeps its captured pay channel and destination'::text as detail,'WAITING'::text as original_section,null::text as reason_code
  from private.bpay_next_run_choice_member m join private.bpay_next_pay_run pr on pr.id=m.run_id
   left join private.bpay_next_selection_candidate sc on sc.run_id=m.run_id and sc.candidate_id=m.candidate_id
   left join private.bpay_next_run_worker rw on rw.run_id=m.run_id and rw.candidate_id=m.candidate_id
  where m.candidate_id=p_candidate_id and m.member_kind='WORK' and m.selected and m.skip_code is null
   and pr.accepted_choice_descriptor is not null and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING')
   and(rw.id is null or rw.status not in ('CANCELLED','COMPLETE'))
  order by native_key limit 1001
 ), protected_case as (
  select distinct 'PC:'||b.run_id::text||':'||b.case_component_id::text||':'||b.binding_kind||':'||b.native_id::text as native_key,'PROTECTED_CASE'::text as member_kind,
   null::uuid as work_id,null::uuid as revision_id,null::uuid as approved_line_id,null::uuid as approved_segment_id,
   c.case_id,c.id as case_component_id,c.component_revision as case_component_revision,null::uuid as transfer_id,b.run_id,
   b.channel as source_pay_channel,null::uuid as source_umbrella_id,null::uuid as valuation_policy_id,
   c.tax_treatment,c.case_kind as component_kind,b.protected_amount as source_amount,
   'Case payment'::text as label,'Keeps its captured pay channel and destination'::text as detail,'WAITING'::text as original_section,null::text as reason_code
  from case_bindings b join private.bpay_next_case_component c on c.id=b.case_component_id
  where c.candidate_id=p_candidate_id order by native_key limit 1001
 ), transfers as (
  select 'T:'||t.id::text as native_key,'TRANSFER'::text as member_kind,
   null::uuid as work_id,null::uuid as revision_id,null::uuid as approved_line_id,null::uuid as approved_segment_id,
   null::uuid as case_id,null::uuid as case_component_id,null::bigint as case_component_revision,t.id as transfer_id,w.run_id,
   w.target_pay_channel as source_pay_channel,w.target_umbrella_id as source_umbrella_id,null::uuid as valuation_policy_id,
   null::text as tax_treatment,null::text as component_kind,t.cash_amount as source_amount,
   case when t.status='RETURNED' then 'Returned payment' else 'Payment' end as label,
   'Keeps its original destination'::text as detail,'WAITING'::text as original_section,null::text as reason_code
  from private.bpay_next_transfer t join private.bpay_next_run_worker w on w.id=t.run_worker_id
  where t.candidate_id=p_candidate_id and(t.status in ('DRAFT','SCHEDULED','ISSUED_CSV','SUBMITTED','UNKNOWN')
    or t.status='RETURNED' and exists(select 1 from private.bpay_next_return_cash rc where rc.original_transfer_id=t.id
      and rc.amount_owed>rc.amount_reissued_paid)) order by native_key limit 1001
 ) select * from future union all select * from protected_work union all select * from protected_case union all select * from transfers
 order by native_key limit 1001
 loop
  if jsonb_array_length(v_result)>=1000 then raise exception using errcode='54000',message='BPAY_NEXT_PAY_METHOD_SCOPE_TOO_LARGE';end if;
  v_quote:=null;v_amount:=v.source_amount;
  if v.member_kind in ('PROTECTED_WORK','PROTECTED_CASE','TRANSFER') then v_class:='PROTECTED';
  elsif v.original_section<>'READY' and v.reason_code not in ('CHANNEL_RESOLUTION_REQUIRED','RATE_RESOLUTION_REQUIRED','GROSS_RESOLUTION_REQUIRED','CASE_CHANNEL_REVIEW') then v_class:='UNCHANGED';
  elsif v.source_amount is null then v_class:='NEEDS_DECISION';
  elsif v.component_kind in ('EXPENSE','MILEAGE') or v.tax_treatment in ('NON_TAXABLE','EXPENSE_CODE','REIMBURSEMENT_GROSS_FIXED','NOT_APPLICABLE') then
   -- Fixed reimbursement/principal basis; VAT and deductions belong to Draft.
   v_class:='UNCHANGED';
  elsif v.tax_treatment is distinct from 'TAXABLE' then v_class:='NEEDS_DECISION';
  else
   begin
    select * into strict v_q from private.bpay_next_value_source_for_target_v1(v.source_amount,v.source_pay_channel,
     p_context->>'target_method',(p_context->>'target_vat_chargeable')::boolean,v.valuation_policy_id,(p_context->>'official_pay_date')::date);
    v_class:='COMPATIBLE';
    v_quote:=jsonb_build_object('policy_id',v.valuation_policy_id,'policy_window_id',v_q.policy_window_id,
     'source_channel',v.source_pay_channel,'target_channel',p_context->>'target_method','source_ex_vat',v.source_amount::text,
     'target_ex_vat',v_q.target_ex_vat::text,'target_vat',v_q.target_vat::text,'target_inc_vat',v_q.target_inc_vat::text,'treatment',v.tax_treatment);
   exception when sqlstate '22023' or sqlstate '23514' or no_data_found then v_class:='NEEDS_DECISION';v_quote:=null;end;
  end if;
  v_row:=jsonb_build_object('key',v.native_key,'member_kind',v.member_kind,'work_id',v.work_id,'revision_id',v.revision_id,
   'approved_line_id',v.approved_line_id,'approved_segment_id',v.approved_segment_id,'case_id',v.case_id,'case_component_id',v.case_component_id,
   'case_component_revision',v.case_component_revision::text,'transfer_id',v.transfer_id,'run_id',v.run_id,
   'source_channel',v.source_pay_channel,'source_umbrella_id',v.source_umbrella_id,'classification',v_class,
   'source_amount',v_amount::text,'quote',v_quote,'label',v.label,'detail',v.detail,'original_section',v.original_section);
  v_result:=v_result||jsonb_build_array(v_row);
  if octet_length(v_result::text)>1048576 then raise exception using errcode='54000',message='BPAY_NEXT_PAY_METHOD_SCOPE_TOO_LARGE';end if;
 end loop;
 return v_result;
end
$function$;


alter function private.bpay_next_pay_method_change_scope_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_pay_method_change_scope_v1(uuid,jsonb) from public,anon,authenticated,service_role;
commit;
