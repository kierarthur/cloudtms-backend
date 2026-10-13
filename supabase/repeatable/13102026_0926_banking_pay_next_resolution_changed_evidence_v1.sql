-- WB-34 D06: native read-only changed facts attached to known stale refusals.
-- Complete public W2 successor of 0721; private 0537 money owner unchanged.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_resolution_changed_evidence_v1(p_actor_id uuid,p_args jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare v_unavailable jsonb;v_response jsonb;v_facts jsonb;v_scope jsonb;v_maps jsonb:='[]'::jsonb;v_works jsonb:='[]'::jsonb;
 v_work jsonb;v_row jsonb;v_rows jsonb;v_old private.bpay_next_work_revision%rowtype;v_current private.bpay_next_work_revision%rowtype;
 w private.bpay_next_work%rowtype;l private.bpay_next_approved_line%rowtype;n private.bpay_next_approved_line%rowtype;
 v_agency uuid;v_epoch bigint;v_basis jsonb;
begin
 v_unavailable:=jsonb_build_object('request_id',p_args->>'request_id','candidate_id',p_args->>'candidate_id','state','UNAVAILABLE',
   'mappings',null,'facts',null,'unavailable_code','BPAY_NEXT_RESOLUTION_CONTEXT_UNAVAILABLE');
 -- This refusal can occur before the writer's context comparison. Bind independently.
 if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
  or not exists(select 1 from public.tms_users u where u.id=p_actor_id and u.is_active and u.role::text='admin') then return v_unavailable;end if;
 select d.installation_id into v_agency from private.cloudtms_database_identity d where d.singleton;
 select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
 if v_agency is null or v_epoch is null or (p_args#>>'{context,agency_id}',p_args#>>'{context,actor_user_id}',p_args#>>'{context,candidate_id}',p_args#>>'{context,owner_epoch}')
  is distinct from (v_agency::text,p_actor_id::text,p_args->>'candidate_id',v_epoch::text) then return v_unavailable;end if;
 v_scope:=jsonb_build_object('candidate_id',p_args->>'candidate_id','reason',null,'works',
  (select jsonb_agg(jsonb_build_object('work_id',x.value->'work_id','expected_revision_id',x.value->'expected_revision_id',
   'rows',(select jsonb_agg(y.value-array['expected_decision_id','expected_decision_version']||jsonb_build_object('intent',null) order by y.ordinality)
    from jsonb_array_elements(x.value->'rows') with ordinality y(value,ordinality))) order by x.ordinality)
   from jsonb_array_elements(p_args->'works') with ordinality x(value,ordinality)));
 perform private.bpay_next_resolution_scope_validate_v1(v_scope,'READ');
 for v_work in select value from jsonb_array_elements(p_args->'works') loop
  select x.* into w from private.bpay_next_work x where x.id=(v_work->>'work_id')::uuid and x.candidate_id=(p_args->>'candidate_id')::uuid;
  select x.* into v_old from private.bpay_next_work_revision x where x.id=(v_work->>'expected_revision_id')::uuid and x.work_id=w.id;
  select x.* into v_current from private.bpay_next_work_revision x where x.id=w.current_revision_id and x.work_id=w.id;
  if w.id is null or v_old.id is null or v_old.approved_at_utc is null or v_old.sealed_at_utc is null
   or v_current.id is null or v_current.approved_at_utc is null or v_current.sealed_at_utc is null or w.approval_state is distinct from 'APPROVED'
   or w.applied_revision_id is distinct from v_current.id or w.current_revision_no is distinct from v_current.revision_no then return v_unavailable;end if;
  v_rows:='[]'::jsonb;
  for v_row in select value from jsonb_array_elements(v_work->'rows') loop
   select x.* into l from private.bpay_next_approved_line x where x.id=(v_row->>'approved_line_id')::uuid and x.revision_id=v_old.id;
   select x.* into n from private.bpay_next_approved_line x where x.revision_id=v_current.id and x.component_key=l.component_key;
   if l.id is null or n.id is null then return v_unavailable;end if;
   -- Qualify both exact original and current requested buckets; no Source-ID/date fallback.
   v_basis:=private.bpay_next_component_resolution_basis_v1(v_old.id,l.id,v_row->>'rate_family',v_row->>'rate_code');
   if v_basis->>'component_key' is distinct from l.component_key then return v_unavailable;end if;
   v_basis:=private.bpay_next_component_resolution_basis_v1(v_current.id,n.id,v_row->>'rate_family',v_row->>'rate_code');
   if v_basis->>'component_key' is distinct from l.component_key then return v_unavailable;end if;
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('approved_line_id',n.id,'rate_family',v_row->>'rate_family','rate_code',v_row->>'rate_code','intent',null));
   v_maps:=v_maps||jsonb_build_array(jsonb_build_object('work_id',w.id,'based_on_revision_id',v_old.id,'approved_line_id',l.id,
    'rate_family',v_row->>'rate_family','rate_code',v_row->>'rate_code','component_key',l.component_key,
    'current_revision_id',v_current.id,'current_approved_line_id',n.id));
  end loop;
  v_works:=v_works||jsonb_build_array(jsonb_build_object('work_id',w.id,'expected_revision_id',v_current.id,'rows',v_rows));
 end loop;
 v_scope:=jsonb_build_object('candidate_id',p_args->>'candidate_id','works',v_works,'reason',null);
 perform private.bpay_next_resolution_scope_validate_v1(v_scope,'READ');
 v_facts:=private.bpay_next_resolution_context_v1(p_actor_id,v_scope);
 if (v_facts#>>'{context,agency_id}',v_facts#>>'{context,actor_user_id}',v_facts#>>'{context,candidate_id}',v_facts#>>'{context,owner_epoch}')
  is distinct from (v_agency::text,p_actor_id::text,p_args->>'candidate_id',v_epoch::text) then return v_unavailable;end if;
 v_response:=jsonb_build_object('request_id',p_args->>'request_id','candidate_id',p_args->>'candidate_id','state','CURRENT',
  'mappings',v_maps,'facts',v_facts,'unavailable_code',null);
 -- PostgREST escapes DETAIL as a string. Reserve envelope space without truncating facts.
 if octet_length(to_jsonb(v_response::text)::text)>120000 or octet_length(v_response::text)>120000 then return v_unavailable;end if;
 return v_response;
exception when others then
 -- Read-only unavailable facts must never replace the original known stale refusal.
 return v_unavailable;
end
$function$;
alter function private.bpay_next_resolution_changed_evidence_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_resolution_changed_evidence_v1(uuid,jsonb) from public,anon,authenticated,service_role;

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

commit;
