-- WB-29: three-way successor of exact EX08 apply1624; all native guards retained.
\set ON_ERROR_STOP on
begin;

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
 -- WB-29: exact explicit unresolved scope, original receipt/member/actor.
 perform private.bpay_next_component_resolution_need_publish_v1(v_operation,null);
 return v_result;
exception when unique_violation then
 raise exception using errcode='23514',message='BPAY_NEXT_PAY_METHOD_OPERATION_CONFLICT';
end
$function$;


alter function public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb) to service_role;
notify pgrst,'reload schema';
commit;
