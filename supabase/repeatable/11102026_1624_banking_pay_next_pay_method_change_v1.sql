-- EX-08 / BD114: future current-fact change; immutable exact accepted decision.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_pay_method_change_context_v1(
 p_actor_user_id uuid,p_candidate_id uuid,p_new_method text,p_umbrella_id uuid)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare v_context jsonb;v_date jsonb;
begin
 if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
  or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active and role::text='admin') then
  raise exception using errcode='42501',message='BPAY_NEXT_PAY_METHOD_FORBIDDEN';end if;
 if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if p_new_method is null or p_new_method not in ('PAYE','UMBRELLA')
  or(p_new_method='PAYE' and p_umbrella_id is not null)
  or(p_new_method='UMBRELLA' and not exists(select 1 from public.umbrellas where id=p_umbrella_id and enabled and vat_chargeable is not null)) then
  raise exception using errcode='22023',message='BPAY_NEXT_PAY_METHOD_TARGET_INVALID';end if;
 v_date:=public.pay_banking_official_date_context_v1(null::timestamptz);
 if v_date->>'ok' is distinct from 'true' or v_date->>'current_official_pay_date' is null then
  raise exception using errcode='55000',message='BPAY_NEXT_PAY_METHOD_CONTEXT_UNAVAILABLE';end if;
 select jsonb_build_object('agency_id',d.installation_id,'actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text,
  'candidate_id',c.id,'source_method',c.pay_method,'source_umbrella_id',case when c.pay_method='UMBRELLA' then c.umbrella_id end,
  'target_method',p_new_method,'target_umbrella_id',p_umbrella_id,'target_vat_chargeable',case when p_new_method='UMBRELLA' then u.vat_chargeable end,
  'target_umbrella_enabled',case when p_new_method='UMBRELLA' then u.enabled end,
  'financial_view_revision',coalesce(w.financial_view_revision,0)::text,'metadata_version',coalesce(cm.metadata_version,0)::text,
  'workbench_revision',r.workbench_revision::text,'adoption_revision',r.adoption_revision::text,'last_command_sequence',clock.last_sequence::text,
  'valuation_policy_id',p.current_policy_id,'official_pay_date',v_date->>'current_official_pay_date')
 into v_context from public.candidates c cross join private.cloudtms_database_identity d
 cross join private.bpay_next_module_control m cross join private.bpay_next_valuation_policy_control p
 cross join private.bpay_next_workbench_revision r cross join private.bpay_next_command_clock clock
 left join public.umbrellas u on u.id=p_umbrella_id
 left join private.bpay_next_worker_control w on w.candidate_id=c.id
 left join private.bpay_next_workbench_candidate_metadata cm on cm.candidate_id=c.id
 where c.id=p_candidate_id and c.active and c.pay_method in ('PAYE','UMBRELLA')
  and d.singleton and m.id=1 and p.id=1 and r.id=1 and clock.id=1 and r.adoption_state='CURRENT' and p.current_policy_id is not null
  and not coalesce(cm.continuation_pending,false);
 if v_context is null or octet_length(v_context::text)>4096 then
  raise exception using errcode='55000',message='BPAY_NEXT_PAY_METHOD_CONTEXT_UNAVAILABLE';end if;
 return v_context;
end
$function$;

-- Maintained current scope only, each candidate-indexed input capped at 1001.
-- No Contract-move comparison binding currently exists (CM-02/CM-19 own it).
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
  elsif v.original_section<>'READY' and v.reason_code not in ('CHANNEL_RESOLUTION_REQUIRED','CASE_CHANNEL_REVIEW') then v_class:='UNCHANGED';
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

create or replace function public.bpay_next_pay_method_change_preview_v1(
 p_actor_user_id uuid,p_candidate_id uuid,p_new_method text,p_umbrella_id uuid,p_after jsonb,p_limit integer)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,private,public
as $function$
declare v_context jsonb;v_scope jsonb;v_rows jsonb:='[]'::jsonb;v_row jsonb;v_cursor jsonb;v_after text;v_last text;v_complete boolean:=true;
 v_name text;v_ref text;v_company text;v_n integer:=0;
begin
 if p_limit is null or p_limit not between 1 and 20 then raise exception using errcode='22023',message='BPAY_NEXT_PAY_METHOD_PAGE_INVALID';end if;
 v_context:=private.bpay_next_pay_method_change_context_v1(p_actor_user_id,p_candidate_id,p_new_method,p_umbrella_id);
 if p_after is not null then
  if jsonb_typeof(p_after) is distinct from 'object' or octet_length(p_after::text)>8192
   or(select count(*) from jsonb_object_keys(p_after))<>2 or not(p_after ?& array['context','last_key'])
   or jsonb_typeof(p_after->'last_key') is distinct from 'string' or octet_length(p_after->>'last_key') not between 1 and 1024 then
   raise exception using errcode='22023',message='BPAY_NEXT_PAY_METHOD_PAGE_INVALID';end if;
  if p_after->'context' is distinct from v_context then raise exception using errcode='40001',message='BPAY_NEXT_PAY_METHOD_PREVIEW_CHANGED';end if;
  v_after:=p_after->>'last_key';
 end if;
 v_scope:=private.bpay_next_pay_method_change_scope_v1(p_candidate_id,v_context);
 if v_context is distinct from private.bpay_next_pay_method_change_context_v1(p_actor_user_id,p_candidate_id,p_new_method,p_umbrella_id) then
  raise exception using errcode='40001',message='BPAY_NEXT_PAY_METHOD_PREVIEW_CHANGED';end if;
 for v_row in select value from jsonb_array_elements(v_scope) where v_after is null or value->>'key'>v_after order by value->>'key' loop
  if v_n=p_limit then v_complete:=false;exit;end if;
  v_last:=v_row->>'key';v_rows:=v_rows||jsonb_build_array(v_row);v_n:=v_n+1;
 end loop;
 select left(coalesce(nullif(c.display_name,''),nullif(concat_ws(' ',c.first_name,c.last_name),''),'Candidate'),256),left(c.tms_ref,256)
  into v_name,v_ref from public.candidates c where c.id=p_candidate_id;
 select left(u.name,256) into v_company from public.umbrellas u where u.id=p_umbrella_id;
 if not v_complete then v_cursor:=jsonb_build_object('context',v_context,'last_key',v_last);end if;
 v_row:=jsonb_build_object('context',v_context,'candidate_name',v_name,'candidate_reference',v_ref,'umbrella_name',v_company,
  'rows',v_rows,'complete',v_complete,'next_cursor',v_cursor,'total_member_count',jsonb_array_length(v_scope)::text,
  'category_counts',(select jsonb_build_object(
   'stays_payable',count(*) filter(where value->>'classification'='COMPATIBLE' or value->>'classification'='UNCHANGED' and value->>'original_section'='READY')::text,
   'needs_decision',count(*) filter(where value->>'classification'='NEEDS_DECISION')::text,
   'not_changed',count(*) filter(where value->>'classification'='PROTECTED' or value->>'classification'='UNCHANGED' and value->>'original_section'<>'READY')::text)
   from jsonb_array_elements(v_scope)));
 if octet_length(v_row::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_PAY_METHOD_RESPONSE_TOO_LARGE';end if;
 return v_row;
end
$function$;

create or replace function public.bpay_next_pay_method_change_apply_v1(p_actor_user_id uuid,p_candidate_id uuid,p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,private,public
as $function$
declare v_operation uuid;v_target text;v_umbrella uuid;v_context jsonb;v_scope jsonb;v_member jsonb;v_result jsonb;
 v_saved record;v_metadata bigint;v_task record;v_epoch bigint;
begin
 if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
  or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active and role::text='admin') then
  raise exception using errcode='42501',message='BPAY_NEXT_PAY_METHOD_FORBIDDEN';end if;
 if p_request is null or jsonb_typeof(p_request) is distinct from 'object' or octet_length(p_request::text)>8192
  or(select count(*) from jsonb_object_keys(p_request))<>6
  or not(p_request ?& array['operation_id','new_method','expected_old_method','destination_patch','reason','preview_context'])
  or jsonb_typeof(p_request->'operation_id') is distinct from 'string'
  or(p_request->>'operation_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or jsonb_typeof(p_request->'new_method') is distinct from 'string' or p_request->>'new_method' not in ('PAYE','UMBRELLA')
  or jsonb_typeof(p_request->'expected_old_method') is distinct from 'string' or p_request->>'expected_old_method' not in ('PAYE','UMBRELLA')
  or jsonb_typeof(p_request->'destination_patch') is distinct from 'object'
  or(p_request->'reason'<>'null'::jsonb and(jsonb_typeof(p_request->'reason') is distinct from 'string' or octet_length(p_request->>'reason')>2048))
  or jsonb_typeof(p_request->'preview_context') is distinct from 'object' or octet_length((p_request->'preview_context')::text)>4096 then
  raise exception using errcode='22023',message='BPAY_NEXT_PAY_METHOD_INPUT_INVALID';end if;
 v_operation:=(p_request->>'operation_id')::uuid;v_target:=p_request->>'new_method';
 if(v_target='PAYE' and p_request->'destination_patch'<>'{}'::jsonb)
  or(v_target='UMBRELLA' and((select count(*) from jsonb_object_keys(p_request->'destination_patch'))<>1
   or jsonb_typeof(p_request#>'{destination_patch,umbrella_id}') is distinct from 'string'
   or coalesce(p_request#>>'{destination_patch,umbrella_id}','')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
  raise exception using errcode='22023',message='BPAY_NEXT_PAY_METHOD_PROTECTED_REWRITE_FORBIDDEN';end if;
 v_umbrella:=(p_request#>>'{destination_patch,umbrella_id}')::uuid;
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 perform 1 from public.candidates where id=p_candidate_id for update;
 if not found then raise exception using errcode='22023',message='BPAY_NEXT_PAY_METHOD_CANDIDATE_MISSING';end if;
 insert into private.bpay_next_worker_control(candidate_id) values(p_candidate_id) on conflict do nothing;
 perform 1 from private.bpay_next_worker_control where candidate_id=p_candidate_id for update;
 -- Replay before mutable currentness, including reason/context and original actor.
 select candidate_id,actor_user_id,owner_epoch,request,result into v_saved from private.bpay_next_pay_method_change_receipt where operation_id=v_operation;
 if found then
  if(v_saved.candidate_id,v_saved.actor_user_id,v_saved.owner_epoch,v_saved.request)
   is distinct from(p_candidate_id,p_actor_user_id,v_epoch,p_request) then
   raise exception using errcode='23514',message='BPAY_NEXT_PAY_METHOD_OPERATION_CONFLICT';end if;
  return v_saved.result;
 end if;
 -- Create preholds Candidate before its clock. Other intakes can hold the
 -- clock first: NOWAIT prevents an inverted wait in that existing order.
 begin perform 1 from private.bpay_next_command_clock where id=1 for share nowait;
 exception when lock_not_available then
  raise exception using errcode='55000',message='BPAY_NEXT_PAY_METHOD_CONTEXT_UNAVAILABLE';end;
 perform private.bpay_next_current_valuation_policy_id_v1();
 if v_umbrella is not null then perform 1 from public.umbrellas where id=v_umbrella for share;end if;
 v_context:=private.bpay_next_pay_method_change_context_v1(p_actor_user_id,p_candidate_id,v_target,v_umbrella);
 if p_request->'preview_context' is distinct from v_context or p_request->>'expected_old_method' is distinct from v_context->>'source_method' then
  raise exception using errcode='40001',message='BPAY_NEXT_PAY_METHOD_PREVIEW_CHANGED';end if;
 if(v_context->>'source_method',(v_context->>'source_umbrella_id')::uuid) is not distinct from(v_target,v_umbrella) then
  raise exception using errcode='22023',message='BPAY_NEXT_PAY_METHOD_NO_CHANGE';end if;
 v_scope:=private.bpay_next_pay_method_change_scope_v1(p_candidate_id,v_context);
 -- Same Candidate/control -> all Works -> all Cases -> their components order.
 perform 1 from private.bpay_next_work where id in(select (value->>'work_id')::uuid from jsonb_array_elements(v_scope)) order by id for share;
 perform 1 from private.bpay_next_finance_case where id in(select (value->>'case_id')::uuid from jsonb_array_elements(v_scope)) order by id for share;
 perform 1 from private.bpay_next_case_component where id in(select (value->>'case_component_id')::uuid from jsonb_array_elements(v_scope)) order by id for share;
 if v_context is distinct from private.bpay_next_pay_method_change_context_v1(p_actor_user_id,p_candidate_id,v_target,v_umbrella)
  or v_scope is distinct from private.bpay_next_pay_method_change_scope_v1(p_candidate_id,v_context) then
  raise exception using errcode='40001',message='BPAY_NEXT_PAY_METHOD_PREVIEW_CHANGED';end if;
 -- Existing trigger advances metadata and schedules the existing finite task.
 update public.candidates set pay_method=v_target,umbrella_id=v_umbrella where id=p_candidate_id;
 select metadata_version into strict v_metadata from private.bpay_next_workbench_candidate_metadata where candidate_id=p_candidate_id;
 select id,generation,metadata_version,module_epoch,control_changed,status into strict v_task from private.bpay_next_workbench_task where task_key='METADATA:'||p_candidate_id::text;
 if v_metadata<=(v_context->>'metadata_version')::bigint or v_task.metadata_version<>v_metadata
  or v_task.module_epoch<>v_epoch or not v_task.control_changed or v_task.status<>'READY' then
  raise exception using errcode='23514',message='BPAY_NEXT_PAY_METHOD_PROJECTION_MISSING';end if;
 v_result:=jsonb_build_object('accepted',true,'operation_id',v_operation,'candidate_id',p_candidate_id,
  'pay_method',v_target,'umbrella_id',v_umbrella,'metadata_version',v_metadata::text,
  'task_id',v_task.id,'task_generation',v_task.generation::text,'affected_member_count',jsonb_array_length(v_scope)::text,'projection_pending',true);
 insert into private.bpay_next_pay_method_change_receipt(operation_id,candidate_id,actor_user_id,owner_epoch,request,accepted_context,
  source_pay_channel,source_umbrella_id,target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,
  post_metadata_version,task_id,task_generation,affected_member_count,result)
 values(v_operation,p_candidate_id,p_actor_user_id,v_epoch,p_request,v_context,v_context->>'source_method',(v_context->>'source_umbrella_id')::uuid,
  v_target,v_umbrella,(v_context->>'target_vat_chargeable')::boolean,v_metadata,v_task.id,v_task.generation,jsonb_array_length(v_scope),v_result);
 for v_member in select value from jsonb_array_elements(v_scope) loop
  insert into private.bpay_next_pay_method_change_member(operation_id,candidate_id,member_key,member_kind,work_id,revision_id,
   approved_line_id,approved_segment_id,case_id,case_component_id,case_component_revision,transfer_id,run_id,
   source_pay_channel,source_umbrella_id,classification,source_amount,quote)
  values(v_operation,p_candidate_id,v_member->>'key',v_member->>'member_kind',(v_member->>'work_id')::uuid,(v_member->>'revision_id')::uuid,
   (v_member->>'approved_line_id')::uuid,(v_member->>'approved_segment_id')::uuid,(v_member->>'case_id')::uuid,(v_member->>'case_component_id')::uuid,
   (v_member->>'case_component_revision')::bigint,(v_member->>'transfer_id')::uuid,(v_member->>'run_id')::uuid,
   v_member->>'source_channel',(v_member->>'source_umbrella_id')::uuid,v_member->>'classification',(v_member->>'source_amount')::numeric,nullif(v_member->'quote','null'::jsonb));
 end loop;
 return v_result;
exception when unique_violation then
 raise exception using errcode='23514',message='BPAY_NEXT_PAY_METHOD_OPERATION_CONFLICT';
end
$function$;

alter function private.bpay_next_pay_method_change_context_v1(uuid,uuid,text,uuid) owner to postgres;
alter function private.bpay_next_pay_method_change_scope_v1(uuid,jsonb) owner to postgres;
alter function public.bpay_next_pay_method_change_preview_v1(uuid,uuid,text,uuid,jsonb,integer) owner to postgres;
alter function public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_pay_method_change_context_v1(uuid,uuid,text,uuid),private.bpay_next_pay_method_change_scope_v1(uuid,jsonb),
 public.bpay_next_pay_method_change_preview_v1(uuid,uuid,text,uuid,jsonb,integer),public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb)
 from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_pay_method_change_preview_v1(uuid,uuid,text,uuid,jsonb,integer),public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb) to service_role;
notify pgrst,'reload schema';

commit;
