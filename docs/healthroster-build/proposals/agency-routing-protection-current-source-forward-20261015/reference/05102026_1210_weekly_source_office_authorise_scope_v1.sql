-- Repeatable CloudTMS function/view authority: weekly_source_office_authorise_scope_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Routing only: retain the existing Weekly Source applicability and Office
-- permission boundary without reading paid/processing information. The real
-- first-authorisation owner still makes every source/pay/financial decision.
create or replace function public.weekly_source_office_authorise_scope_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_id uuid; v_group uuid; v_count integer;
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object'
     or not p_request ?& array['actor_user_id','timesheet_id']
     or p_request-array['actor_user_id','timesheet_id']<>'{}'::jsonb
     or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
     or jsonb_typeof(p_request->'timesheet_id') is distinct from 'string' then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_id:=(p_request->>'timesheet_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_id is null then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',null,null,current_date);
  select t.* into v_root from public.timesheets t where t.timesheet_id=v_id
    and t.is_current and t.revoked_at is null and t.archived_at_utc is null;
  if not found then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_NOT_FOUND' using errcode='22023';
  end if;
  if v_root.sheet_scope is distinct from 'WEEKLY'
     or v_root.line_type is distinct from 'HOURS' or v_root.contract_id is null then
    return jsonb_build_object('contract','WEEKLY_SOURCE_AUTHORISE_SCOPE_V1','applicable',false);
  end if;
  select c.* into strict v_contract from public.contracts c where c.id=v_root.contract_id;
  select count(*),(array_agg(g.id))[1] into v_count,v_group
    from public.weekly_source_group_clients membership
    join public.weekly_source_groups g on g.id=membership.source_group_id
    where membership.client_id=v_contract.client_id and g.active
      and v_root.week_ending_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
  if v_count=0 then
    return jsonb_build_object('contract','WEEKLY_SOURCE_AUTHORISE_SCOPE_V1','applicable',false);
  elsif v_count<>1 then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_AMBIGUOUS' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',
    v_group,v_contract.client_id,v_root.week_ending_date);
  return jsonb_build_object('contract','WEEKLY_SOURCE_AUTHORISE_SCOPE_V1','applicable',true);
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function public.weekly_source_office_authorise_scope_v1(jsonb) owner to current_user;
revoke all on function public.weekly_source_office_authorise_scope_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_office_authorise_scope_v1(jsonb) to service_role;
notify pgrst,'reload schema';

commit;
