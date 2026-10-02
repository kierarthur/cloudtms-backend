-- Office-only review of one already-resolved imported work identity. No source,
-- Timesheet, financial or notification row is created by opening a review.
create or replace function public.weekly_source_manual_review_open_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_row_id uuid; v_reason text;
  v_row public.weekly_source_upload_rows%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_route jsonb; v_existing uuid; v_review uuid;
  v_week date;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
      where key not in ('actor_user_id','source_row_id','reason')) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_row_id:=(p_request->>'source_row_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023';
  end;
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  if v_actor is null or v_row_id is null or pg_catalog.char_length(v_reason) not between 1 and 1000 then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023';
  end if;
  select * into v_row from public.weekly_source_upload_rows where id=v_row_id;
  if not found then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_ROW_NOT_FOUND' using errcode='22023'; end if;
  select * into strict v_upload from public.weekly_source_uploads where id=v_row.upload_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_upload.source_cycle_id;
  select * into v_resolution from public.weekly_source_row_resolutions
    where upload_row_id=v_row.id order by generation desc,id desc limit 1;
  if not found or v_resolution.mapping_state<>'RESOLVED' or v_resolution.work_event_id is null
    or v_row.row_finalisation_state not in ('NOT_APPLICABLE','SOURCE_WORKED')
    or v_upload.state not in ('CURRENT','SUPERSEDED')
    or not (
      exists(select 1 from public.weekly_source_client_manifests manifest
        join public.weekly_source_final_revisions revision on revision.id=manifest.final_revision_id
        where manifest.source_cycle_id=v_cycle.id and manifest.client_id=v_resolution.client_id
          and revision.upload_id=v_upload.id and revision.state='CURRENT')
      or (not exists(select 1 from public.weekly_source_client_manifests manifest
            join public.weekly_source_final_revisions revision on revision.id=manifest.final_revision_id
            where manifest.source_cycle_id=v_cycle.id and manifest.client_id=v_resolution.client_id
              and revision.state='CURRENT')
        and exists(select 1 from public.weekly_source_projection_publications publication
          where publication.source_cycle_id=v_cycle.id and publication.upload_id=v_upload.id
            and publication.state='CURRENT'))
    ) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_SOURCE_STALE' using errcode='40001';
  end if;
  v_route:=private.weekly_source_office_route_key_v1(v_cycle.id,v_resolution.candidate_id,
    v_resolution.client_id,v_resolution.contract_id,v_row.work_date);
  if v_route->>'authority_mode'<>'SOURCE_AUTHORITY' then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',
    v_cycle.source_group_id,v_resolution.client_id,v_cycle.finalisation_week_ending);
  v_week:=(date_trunc('week',v_row.work_date)::date+6);
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_QUERY_FAMILY:'||v_resolution.contract_id::text||':'||v_week::text,0));
  if exists(select 1 from public.timesheets timesheet
      where timesheet.contract_id=v_resolution.contract_id
        and timesheet.week_ending_date=v_week and timesheet.is_current
        and timesheet.authorised_at_server is not null) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_ALREADY_AUTHORISED' using errcode='55000';
  end if;
  select id into v_existing from private.weekly_source_manual_reviews
    where source_group_id=v_cycle.source_group_id and work_event_id=v_resolution.work_event_id
      and state='OPEN' for update;
  if v_existing is not null then
    return pg_catalog.jsonb_build_object('ok',true,'review_id',v_existing,
      'already_open',true,'problem','Manually queried');
  end if;
  insert into private.weekly_source_manual_reviews(
    source_group_id,source_cycle_id,upload_row_id,row_resolution_id,source_row_hash,
    work_event_id,candidate_id,client_id,contract_id,work_date,state,
    office_reason,opened_by_user_id)
  values(v_cycle.source_group_id,v_cycle.id,v_row.id,v_resolution.id,v_row.normalised_row_hash,
    v_resolution.work_event_id,v_resolution.candidate_id,v_resolution.client_id,
    v_resolution.contract_id,v_row.work_date,'OPEN',v_reason,v_actor)
  returning id into v_review;
  perform public._audit_insert('weekly_source_manual_review',v_review::text,
    'WEEKLY_SOURCE_MANUAL_REVIEW_OPENED',null,
    pg_catalog.jsonb_build_object('source_row_id',v_row.id,'work_event_id',v_resolution.work_event_id,
      'reason',v_reason),'Office sent imported shift back to Queries',v_actor);
  return pg_catalog.jsonb_build_object('ok',true,'review_id',v_review,
    'already_open',false,'problem','Manually queried');
end;
$function$;
alter function public.weekly_source_manual_review_open_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_open_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_manual_review_open_v1(jsonb) to service_role;

create or replace function public.weekly_source_manual_review_resolve_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_review_id uuid; v_kind text;
  v_review private.weekly_source_manual_reviews%rowtype;
  v_current_row public.weekly_source_upload_rows%rowtype;
  v_expected_hash text;
  v_week date; v_scope_date date;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
      where key not in ('actor_user_id','review_id','resolution_kind','expected_current_row_hash')) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_review_id:=(p_request->>'review_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023';
  end;
  v_kind:=p_request->>'resolution_kind';
  if v_actor is null or v_review_id is null or v_kind not in ('OFFICE_ACCEPTED_SOURCE','PROTECTED_PAY') then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023';
  end if;
  select * into v_review from private.weekly_source_manual_reviews where id=v_review_id;
  if not found or v_review.state<>'OPEN' then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_NOT_OPEN' using errcode='40001';
  end if;
  select finalisation_week_ending into strict v_scope_date
  from public.weekly_source_cycles where id=v_review.source_cycle_id;
  perform private.weekly_source_office_authority_v1(v_actor,
    case when v_kind='PROTECTED_PAY' then 'APPROVE_PROTECTED_PAY' else 'ACCEPT_SYSTEM_HOURS' end,
    v_review.source_group_id,v_review.client_id,v_scope_date);
  v_week:=(date_trunc('week',v_review.work_date)::date+6);
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_QUERY_FAMILY:'||v_review.contract_id::text||':'||v_week::text,0));
  select * into strict v_review from private.weekly_source_manual_reviews
    where id=v_review_id and state='OPEN' for update;
  if v_kind='OFFICE_ACCEPTED_SOURCE' then
    v_expected_hash:=p_request->>'expected_current_row_hash';
    if v_expected_hash is null or v_expected_hash!~'^[0-9a-f]{64}$' then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_CURRENT_SOURCE_REQUIRED' using errcode='22023';
    end if;
    -- A later import may replace the original row's hours. Accept only the
    -- currently displayed source tuple for the same durable work identity.
    select candidate_row.* into v_current_row
    from public.weekly_source_upload_rows candidate_row
    join public.weekly_source_uploads candidate_upload on candidate_upload.id=candidate_row.upload_id
    join lateral (
      select mapping.work_event_id,mapping.mapping_state
      from public.weekly_source_row_resolutions mapping
      where mapping.upload_row_id=candidate_row.id
      order by mapping.generation desc,mapping.id desc limit 1
    ) current_mapping on current_mapping.work_event_id=v_review.work_event_id
      and current_mapping.mapping_state='RESOLVED'
    where candidate_upload.source_cycle_id=v_review.source_cycle_id
      and candidate_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED')
      and (
        exists(select 1 from public.weekly_source_client_manifests manifest
          join public.weekly_source_final_revisions revision on revision.id=manifest.final_revision_id
          where manifest.source_cycle_id=v_review.source_cycle_id
            and manifest.client_id=v_review.client_id
            and revision.state='CURRENT' and revision.upload_id=candidate_upload.id)
        or (not exists(select 1 from public.weekly_source_client_manifests manifest
            join public.weekly_source_final_revisions revision on revision.id=manifest.final_revision_id
            where manifest.source_cycle_id=v_review.source_cycle_id
              and manifest.client_id=v_review.client_id and revision.state='CURRENT')
          and exists(select 1 from public.weekly_source_projection_publications publication
            where publication.source_cycle_id=v_review.source_cycle_id
              and publication.upload_id=candidate_upload.id and publication.state='CURRENT'))
      )
    order by candidate_row.created_at_utc desc,candidate_row.id desc limit 1;
    if not found or pg_catalog.encode(v_current_row.normalised_row_hash,'hex')<>v_expected_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_SOURCE_STALE' using errcode='40001';
    end if;
  elsif not exists (
    select 1 from public.weekly_exceptional_pay_family_events protected_event
    join public.weekly_exceptional_pay_target_families family
      on family.id=protected_event.family_id
    where family.contract_id=v_review.contract_id
      and family.candidate_id=v_review.candidate_id
      and family.week_ending_date=v_week
      and protected_event.durable_work_event_id=v_review.work_event_id
      and protected_event.state='WAIT'
      and protected_event.event_sequence=(select max(latest.event_sequence)
        from public.weekly_exceptional_pay_family_events latest
        where latest.family_id=protected_event.family_id
          and latest.durable_work_event_id=protected_event.durable_work_event_id)
  ) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_PROTECTED_PAY_REQUIRED' using errcode='55000';
  end if;
  update private.weekly_source_manual_reviews
    set state='RESOLVED',resolved_by_user_id=v_actor,
      resolved_at_utc=pg_catalog.transaction_timestamp(),resolution_kind=v_kind
    where id=v_review.id;
  perform public._audit_insert('weekly_source_manual_review',v_review.id::text,
    'WEEKLY_SOURCE_MANUAL_REVIEW_RESOLVED',null,
    pg_catalog.jsonb_build_object('resolution_kind',v_kind,'work_event_id',v_review.work_event_id,
      'accepted_source_row_id',v_current_row.id),
    'Office resolved imported-shift review',v_actor);
  return pg_catalog.jsonb_build_object('ok',true,'review_id',v_review.id,
    'resolution_kind',v_kind);
end;
$function$;
alter function public.weekly_source_manual_review_resolve_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_resolve_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_manual_review_resolve_v1(jsonb) to service_role;
notify pgrst, 'reload schema';
