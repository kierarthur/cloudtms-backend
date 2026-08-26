begin;

create or replace function public.candidate_no_work_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_contract_week_id uuid,
  p_expected_row_signature text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_context jsonb;
  v_candidate_id uuid;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_system_actor uuid;
  v_preview jsonb;
  v_result jsonb;
  v_response jsonb;
  v_signature text;
  v_signature_payload jsonb;
  v_route_authority jsonb;
  v_current_timesheet_id uuid;
  v_timesheet_ids uuid[]:=array[]::uuid[];
  v_contract_week_ids uuid[]:=array[]::uuid[];
  v_nhsp_shift_ids uuid[]:=array[]::uuid[];
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null or p_contract_week_id is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_NO_WORK_PAYLOAD_INVALID' using errcode='22023';
  end if;

  select ae.after_json into v_response
  from public.audit_events ae
  where ae.object_type='contract_week'
    and ae.object_id_text=p_contract_week_id::text
    and ae.action='CANDIDATE_NO_WORK'
    and ae.correlation_id=p_idempotency_key
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    return coalesce(v_response,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;

  select * into v_week from public.contract_weeks where id=p_contract_week_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=v_candidate_id for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000'; end if;

  v_route_authority:=private._candidate_route_family_v1(v_week.timesheet_id,v_week.id);
  if not coalesce((v_route_authority->>'candidate_no_work_allowed')::boolean,false) then
    raise exception 'CANDIDATE_NO_WORK_NOT_ALLOWED' using errcode='55000',detail=v_route_authority::text;
  end if;

  if v_week.timesheet_id is null then
    if v_week.status not in ('PLANNED','OPEN') then raise exception 'CANDIDATE_NO_WORK_NOT_ALLOWED' using errcode='55000'; end if;
    select to_jsonb(x) into v_result from public.contract_week_delete_planned(v_week.id,v_system_actor) x;
    v_response:=jsonb_build_object('ok',true,'action','DELETE_PLANNED','contract_week_id',v_week.id,
      'result',v_result,'candidate_hidden',true,'idempotency_key',p_idempotency_key,'idempotent_replay',false);
    perform private._candidate_audit_v1('contract_week',v_week.id::text,'CANDIDATE_NO_WORK',
      jsonb_build_object('timesheet_id',null),v_response,null,v_system_actor,p_idempotency_key,p_now_utc);
    return v_response;
  end if;

  if coalesce(v_week.additional_seq,0)>0 or coalesce(v_week.is_adjustment,false) then
    raise exception 'CANDIDATE_NO_WORK_NOT_ALLOWED' using errcode='55000';
  end if;

  v_preview:=public.timesheet_weekly_chain_delete_preview(v_week.timesheet_id,v_system_actor);
  v_current_timesheet_id:=nullif(v_preview->>'current_timesheet_id','')::uuid;
  if v_current_timesheet_id is null then
    raise exception 'CANDIDATE_NO_WORK_BLOCKED' using errcode='55000',detail=v_preview::text;
  end if;

  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
    into v_timesheet_ids
  from jsonb_array_elements_text(coalesce(v_preview->'timesheet_ids','[]'::jsonb)) ids(value);
  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
    into v_contract_week_ids
  from jsonb_array_elements_text(coalesce(v_preview->'contract_week_ids','[]'::jsonb)) ids(value);
  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
    into v_nhsp_shift_ids
  from jsonb_array_elements_text(coalesce(v_preview->'nhsp_shift_ids','[]'::jsonb)) ids(value);

  v_signature_payload:=public.timesheet_lifecycle_guard_signature_v1(
    v_current_timesheet_id,v_week.id,false
  );
  v_signature:=coalesce(
    nullif(btrim(v_signature_payload->>'backend_row_signature'),''),
    nullif(btrim(v_signature_payload->>'row_signature'),''),
    nullif(btrim(v_signature_payload->>'signature'),'')
  );
  if nullif(btrim(coalesce(p_expected_row_signature,'')),'') is not null
     and btrim(p_expected_row_signature) is distinct from v_signature then
    raise exception 'CANDIDATE_ROW_SIGNATURE_CHANGED' using errcode='40001';
  end if;
  if v_signature is null then raise exception 'CANDIDATE_ROW_SIGNATURE_REQUIRED' using errcode='22023'; end if;

  if v_preview->>'decision'='PERMANENT_DELETE' then
    v_result:=public.timesheet_weekly_chain_delete_apply(
      v_current_timesheet_id,v_system_actor,v_timesheet_ids,v_contract_week_ids,v_nhsp_shift_ids,v_signature
    );
  elsif v_preview->>'decision'='ARCHIVE_REQUIRED' then
    v_result:=public.timesheet_archive_transition_v1(
      v_current_timesheet_id,'ARCHIVE','WEEKLY_CHAIN_DELETE_PARENT',v_system_actor,
      v_current_timesheet_id,v_signature,p_now_utc
    );
  else
    raise exception 'CANDIDATE_NO_WORK_BLOCKED' using errcode='55000',detail=v_preview::text;
  end if;

  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'CANDIDATE_NO_WORK_BLOCKED' using errcode='55000',detail=v_result::text;
  end if;
  v_response:=jsonb_build_object(
    'ok',true,'contract_week_id',v_week.id,'timesheet_id',v_current_timesheet_id,
    'decision',v_preview->>'decision','result',v_result,'candidate_hidden',true,
    'idempotency_key',p_idempotency_key,'idempotent_replay',false
  );
  perform private._candidate_audit_v1('contract_week',v_week.id::text,'CANDIDATE_NO_WORK',
    jsonb_build_object('timesheet_id',v_current_timesheet_id),v_response,null,v_system_actor,p_idempotency_key,p_now_utc);
  return v_response;
end;
$function$;

revoke all on function public.candidate_no_work_atomic_v1(uuid,text,uuid,text,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_no_work_atomic_v1(uuid,text,uuid,text,text,timestamptz) to service_role;

notify pgrst, 'reload schema';

commit;
