-- WB34: complete installed0926 W2 successor, unchanged body and ACL.
-- Final RPC root must reload the PostgREST schema cache. Applied0926 stays unchanged.
\set ON_ERROR_STOP on
begin;
create or replace function public.bpay_next_workbench_command_v1(p_request jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_actor uuid;v_epoch bigint;v_receipt private.bpay_next_snooze_request%rowtype;v_result jsonb;v_request uuid;v_stale text;v_changed jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>32768
    or (select count(*) from jsonb_object_keys(p_request))<>4
    or exists(select 1 from jsonb_object_keys(p_request) k where k not in ('actor_user_id','action','args','readback'))
    or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
    or (p_request->>'actor_user_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
    or p_request->>'action' is null or p_request->>'action' not in ('DECISIONS_CLEAR','RESOLUTION_SAVE','RESOLUTION_CLEAR','RETRY_WORK_JOB','SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR','SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY')
    or jsonb_typeof(p_request->'readback') is distinct from 'boolean' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
  if p_request->>'action'='DECISIONS_CLEAR' then
    return private.bpay_next_decisions_clear_command_v1(v_actor,p_request->'args',(p_request->>'readback')::boolean);
  end if;
  if p_request->>'action' in ('RESOLUTION_SAVE','RESOLUTION_CLEAR') then
    begin
      return private.bpay_next_resolution_command_v1(v_actor,p_request->>'action',p_request->'args',(p_request->>'readback')::boolean);
    exception when check_violation then
      get stacked diagnostics v_stale=MESSAGE_TEXT;
      if v_stale not in ('BPAY_NEXT_RESOLUTION_STALE','BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED','BPAY_NEXT_RESOLUTION_BASIS_CHANGED') then raise;end if;
      v_changed:=private.bpay_next_resolution_changed_evidence_v1(v_actor,p_request->'args');
      raise exception using errcode='23514',message=v_stale,detail=v_changed::text;
    end;
  end if;
  if p_request->>'action'<>'RETRY_WORK_JOB' then
    if p_request->>'action' in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY') then
      return private.bpay_next_snooze_many_v1(v_actor,p_request->>'action',p_request->'args',(p_request->>'readback')::boolean);
    end if;
    if jsonb_typeof(p_request->'args') is distinct from 'object' or octet_length((p_request->'args')::text)>4096
      or p_request#>>'{args,request_id}' is null
      or p_request#>>'{args,request_id}'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';end if;
    select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
    if v_epoch is null then
      raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
    v_request:=(p_request#>>'{args,request_id}')::uuid;
    select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
    if found and not exists(select 1 from private.bpay_next_command c where c.id=v_request and c.module_epoch=v_epoch
      and c.command_kind=p_request->>'action') then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_FINANCIAL_VERSION_STALE';end if;
    if (p_request->>'readback')::boolean then
      select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
      if found and (v_receipt.actor_id,v_receipt.action,v_receipt.request_body)
        is distinct from (v_actor,p_request->>'action',p_request->'args') then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';end if;
      v_result:=v_receipt.result_body;
    else
      v_result:=private.bpay_next_snooze_action_v1(v_actor,p_request->>'action',p_request->'args');
    end if;
    if v_result is not null then
      if v_result ? 'version' then v_result:=jsonb_set(v_result,'{version}',to_jsonb(v_result->>'version'));end if;
      if v_result ? 'control_sequence' then v_result:=jsonb_set(v_result,'{control_sequence}',to_jsonb(v_result->>'control_sequence'));end if;
    end if;
    return jsonb_build_object('request_id',v_request,'candidate_id',p_request#>>'{args,candidate_id}',
      'received',v_result is not null,'replay',(p_request->>'readback')::boolean and v_result is not null,'result',v_result);
  end if;
  if octet_length(p_request::text)>4096 then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  if (p_request->>'readback')::boolean then
    return private.bpay_next_retry_work_job_v1(v_actor,p_request->'args',true);
  end if;
  return public.bpay_next_financial_intake_v1(v_actor,'RETRY_WORK_JOB',p_request->'args')->'result';
end
$function$;
alter function public.bpay_next_workbench_command_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_command_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_workbench_command_v1(jsonb) to service_role;

notify pgrst,'reload schema';
commit;
