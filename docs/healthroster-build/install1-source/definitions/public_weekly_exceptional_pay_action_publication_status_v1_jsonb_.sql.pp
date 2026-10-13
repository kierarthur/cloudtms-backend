create or replace function public.weekly_exceptional_pay_action_publication_status_v1(
  p_request jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id'
  ];
  v_actor uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_publication_count integer;
begin
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_ACTION_PUBLICATION_STATUS_V1' then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID' using errcode='22023';
  end;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id;
  if v_run.requested_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
  end if;

  select count(*) into v_publication_count
  from public.weekly_exceptional_c1_publication_requests request
  where request.family_id=v_family.id
    and request.orchestration_run_id=v_run.id;
  if v_publication_count>1 then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_publication_count=0 then
    if not private.weekly_source_pay_actor_allowed_v1(v_actor) then
      raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'staged',false,'family_id',v_family.id,
      'orchestration_run_id',v_run.id,'run_state',v_run.state
    );
  end if;
  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.family_id=v_family.id and request.orchestration_run_id=v_run.id;

  select approval.* into strict v_approval
  from public.weekly_exceptional_payment_approvals approval
  where approval.creation_orchestration_run_id=v_run.id
    and approval.pay_target_family_id=v_family.id;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_approval.source_cycle_id;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id;
  perform private.weekly_source_office_authority_v1(
    v_actor,'APPROVE_PROTECTED_PAY',v_group.id,v_approval.client_id,
    v_cycle.finalisation_week_ending
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'staged',true,'family_id',v_family.id,
    'orchestration_run_id',v_run.id,'run_state',v_run.state,
    'publication_request_id',v_publication.id,
    'request_sha256',pg_catalog.encode(v_publication.request_sha256,'hex'),
    'publication_state',v_publication.state
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
end;
$function$;
