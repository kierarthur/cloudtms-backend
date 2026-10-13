create or replace function public.weekly_source_office_notifications_list_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_limit integer;
  v_open_only boolean;
  v_rows jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','limit','open_only')) then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,100);
    v_open_only:=coalesce((p_request->>'open_only')::boolean,true);
  exception when others then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_limit<1 or v_limit>500 or not exists(
    select 1 from public.tms_users u where u.id=v_actor and u.is_active
      and (pg_catalog.lower(pg_catalog.btrim(u.role)) in ('admin','user')
        or private.weekly_source_pay_actor_allowed_v1(v_actor))
  ) then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_FORBIDDEN' using errcode='42501';
  end if;
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data) order by row_data.created_at_utc desc,row_data.id),'[]'::jsonb)
  into v_rows
  from (
    select notice.id,notice.event_kind,notice.issue_id,notice.issue_generation,
      notice.payload_json,notice.operational_state,notice.read_at_utc,notice.created_at_utc
    from public.office_action_notifications notice
    where notice.recipient_user_id=v_actor
      and (not v_open_only or notice.operational_state='OPEN')
    order by notice.created_at_utc desc,notice.id
    limit v_limit
  ) row_data;
  return pg_catalog.jsonb_build_object('ok',true,'notifications',v_rows);
end;
$function$;
