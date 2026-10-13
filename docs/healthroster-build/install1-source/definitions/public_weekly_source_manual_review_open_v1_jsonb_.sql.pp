create or replace function public.weekly_source_manual_review_open_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_row_id uuid; v_command_id uuid; v_reason text; v_hash bytea;
  v_prior private.weekly_source_manual_review_commands%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_source jsonb; v_route jsonb; v_result jsonb; v_review uuid; v_existing boolean;
  v_family_id uuid; v_sequence bigint; v_count integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object' or exists(select 1 from jsonb_object_keys(p_request) k
    where k not in ('actor_user_id','source_row_id','reason','command_id')) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid; v_row_id:=(p_request->>'source_row_id')::uuid;
    v_command_id:=(p_request->>'command_id')::uuid;
  exception when others then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end;
  v_reason:=btrim(coalesce(p_request->>'reason',''));
  if v_actor is null or v_row_id is null or v_command_id is null or char_length(v_reason) not between 1 and 1000 then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_MANUAL_REVIEW_OPEN_V2',p_request);
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'OPEN' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'OPEN_PAY_QUERY',v_cycle.source_group_id,
      v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND:'||v_command_id::text,0));
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'OPEN' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'OPEN_PAY_QUERY',v_cycle.source_group_id,
      v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  -- The global try-key serialises participating source writers. Re-read the
  -- actual source and current authority after admission; never use a stale UI tuple.
  v_source:=private.weekly_source_manual_review_source_v2(v_row_id);
  if v_source is null then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_SOURCE_STALE' using errcode='40001'; end if;
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=(v_source->>'source_cycle_id')::uuid;
  perform private.weekly_source_office_authority_v1(v_actor,'OPEN_PAY_QUERY',v_cycle.source_group_id,
    (v_source->>'client_id')::uuid,v_cycle.finalisation_week_ending);
  v_route:=private.weekly_source_office_route_key_v1(v_cycle.id,(v_source->>'candidate_id')::uuid,
    (v_source->>'client_id')::uuid,(v_source->>'contract_id')::uuid,(v_source->>'work_date')::date);
  if v_route->>'authority_mode' is distinct from 'SOURCE_AUTHORITY' then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='22023'; end if;
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_QUERY_FAMILY:'||
    (v_source->>'contract_id')||':'||(v_source->>'week_ending_date'),0));
  select r.id into v_review from private.weekly_source_manual_reviews r
    where r.source_group_id=v_cycle.source_group_id and r.work_event_id=(v_source->>'work_event_id')::uuid
      and r.state='OPEN' for update;
  v_existing:=v_review is not null;
  if v_existing and not exists(select 1 from private.weekly_source_manual_reviews r where r.id=v_review
    and r.candidate_id=(v_source->>'candidate_id')::uuid and r.client_id=(v_source->>'client_id')::uuid
    and r.contract_id=(v_source->>'contract_id')::uuid and r.work_date=(v_source->>'work_date')::date) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='55000'; end if;
  if not v_existing then
    select count(*),(array_agg(f.id))[1] into v_count,v_family_id
      from public.weekly_exceptional_pay_target_families f
      where f.contract_id=(v_source->>'contract_id')::uuid and f.candidate_id=(v_source->>'candidate_id')::uuid
        and f.week_ending_date=(v_source->>'week_ending_date')::date;
    if v_count>1 then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_AMBIGUOUS' using errcode='55000'; end if;
    select coalesce(max(e.event_sequence),0) into v_sequence from public.weekly_exceptional_pay_family_events e
      where e.family_id=v_family_id and e.durable_work_event_id=(v_source->>'work_event_id')::uuid;
    insert into private.weekly_source_manual_reviews(source_group_id,source_cycle_id,upload_row_id,
      row_resolution_id,source_row_hash,work_event_id,candidate_id,client_id,contract_id,work_date,
      state,office_reason,opened_by_user_id)
    values(v_cycle.source_group_id,v_cycle.id,v_row_id,(v_source->>'row_resolution_id')::uuid,
      decode(v_source->>'source_row_hash','hex'),(v_source->>'work_event_id')::uuid,
      (v_source->>'candidate_id')::uuid,(v_source->>'client_id')::uuid,(v_source->>'contract_id')::uuid,
      (v_source->>'work_date')::date,'OPEN',v_reason,v_actor) returning id into v_review;
    perform public._audit_insert('weekly_source_manual_review',v_review::text,'WEEKLY_SOURCE_MANUAL_REVIEW_OPENED',null,
      jsonb_build_object('source_row_id',v_row_id,'work_event_id',v_source->'work_event_id','reason',v_reason,
        'opening_final_revision_id',v_source->'final_revision_id',
        'opening_projection_publication_id',v_source->'projection_publication_id'),
      'Office sent imported shift to Pay Queries',v_actor);
  end if;
  v_result:=jsonb_build_object('ok',true,'review_id',v_review,'already_open',v_existing,
    'problem','Manually queried','idempotent_replay',false);
  insert into private.weekly_source_manual_review_commands(command_id,command_kind,actor_user_id,request_sha256,
    review_id,source_cycle_id,client_id,week_ending_date,opening_final_revision_id,opening_projection_publication_id,
    opening_target_family_id,opening_event_sequence,result_json)
  values(v_command_id,'OPEN',v_actor,v_hash,v_review,v_cycle.id,(v_source->>'client_id')::uuid,
    (v_source->>'week_ending_date')::date,case when not v_existing then (v_source->>'final_revision_id')::uuid end,
    case when not v_existing then (v_source->>'projection_publication_id')::uuid end,
    case when not v_existing then v_family_id end,case when not v_existing then v_sequence end,v_result);
  return v_result;
end;
$function$;
