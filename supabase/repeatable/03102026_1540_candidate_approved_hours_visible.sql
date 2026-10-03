-- Candidate list and detail must use the same committed approved-hours head.
-- A financial total or a Candidate submission is not evidence of approval.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_candidate_list_approval_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_entitlement jsonb;
  v_total numeric;
begin
  if p_timesheet_id is null then
    return pg_catalog.jsonb_build_object('state','NOT_PROCESSED','total_hours',null);
  end if;
  v_context:=private.weekly_source_candidate_week_context_v1(p_timesheet_id);
  if v_context is null then
    return pg_catalog.jsonb_build_object('state','NOT_PROCESSED','total_hours',null);
  end if;
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(v_context);
  if v_entitlement->>'state'='NO_APPROVED_ENTITLEMENT' then
    return pg_catalog.jsonb_build_object('state','NOT_PROCESSED','total_hours',null);
  end if;
  if v_entitlement->>'state' is distinct from 'AVAILABLE' then
    return pg_catalog.jsonb_build_object('state','UNAVAILABLE','total_hours',null);
  end if;
  -- SOURCE_AUTHORITY approval always has a committed head whose numeric total
  -- is verified against its resolved component times by the resolver.
  if v_entitlement->>'authority' is distinct from 'HEAD'
     or v_entitlement->>'total_hours' is null then
    return pg_catalog.jsonb_build_object('state','UNAVAILABLE','total_hours',null);
  end if;
  v_total:=(v_entitlement->>'total_hours')::numeric;
  if v_total<0 then
    return pg_catalog.jsonb_build_object('state','UNAVAILABLE','total_hours',null);
  end if;
  return pg_catalog.jsonb_build_object('state','AVAILABLE','total_hours',v_total);
end;
$function$;

-- Keep the ten-member CandidateWeeklySourceView shape intact.  The additive
-- detail members carry the approval state/total while the view always carries
-- the approved rows, including when they happen to equal submitted rows.
create or replace function private.weekly_source_candidate_view_merge_v1(
  p_timesheet_id uuid,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_view jsonb;
  v_approval jsonb;
  v_entitlement jsonb;
  v_context jsonb;
begin
  v_view:=private.weekly_source_candidate_view_v1(p_timesheet_id,p_now_utc);
  if v_view is null then return '{}'::jsonb; end if;
  v_context:=private.weekly_source_candidate_week_context_v1(p_timesheet_id);
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(v_context);
  if v_entitlement->>'state'='NO_APPROVED_ENTITLEMENT' then
    v_approval:=pg_catalog.jsonb_build_object('state','NOT_PROCESSED','total_hours',null);
  elsif v_entitlement->>'state'='AVAILABLE'
    and v_entitlement->>'authority'='HEAD'
    and v_entitlement->>'total_hours' is not null
    and (v_entitlement->>'total_hours')::numeric>=0 then
    v_approval:=pg_catalog.jsonb_build_object('state','AVAILABLE',
      'total_hours',(v_entitlement->>'total_hours')::numeric);
  else
    v_approval:=pg_catalog.jsonb_build_object('state','UNAVAILABLE','total_hours',null);
  end if;
  if v_approval->>'state'='AVAILABLE' then
    v_view:=pg_catalog.jsonb_set(v_view,'{approved_hours_to_be_paid}',
      coalesce(v_entitlement->'rows','[]'::jsonb));
  end if;
  return pg_catalog.jsonb_build_object(
    'weekly_source_candidate_view',v_view,
    'weekly_source_approved_hours_state',v_approval->'state',
    'weekly_source_approved_total_hours',v_approval->'total_hours');
end;
$function$;

-- Paginated card projection: preserve the existing page and item order, adding
-- the same certified approval to import-authoritative cards only.
create or replace function public.candidate_app_timesheet_page_v2(
  p_session_id uuid,
  p_environment text,
  p_view text default 'CURRENT',
  p_cursor text default null,
  p_limit integer default 50,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_page jsonb;
  v_item jsonb;
  v_items jsonb:='[]'::jsonb;
  v_approval jsonb;
begin
  v_page:=public.candidate_app_timesheet_page_v1(
    p_session_id,p_environment,p_view,p_cursor,p_limit,p_now_utc);
  for v_item in select value from pg_catalog.jsonb_array_elements(
    coalesce(v_page->'items','[]'::jsonb)) loop
    if v_item->>'route_family'='IMPORT_AUTHORITATIVE' then
      if coalesce((v_item->>'authorised')::boolean,false) then
        v_approval:=private.weekly_source_candidate_list_approval_v1(
          nullif(v_item->>'timesheet_id','')::uuid);
      else
        v_approval:=pg_catalog.jsonb_build_object(
          'state','NOT_PROCESSED','total_hours',null);
      end if;
      v_item:=v_item||pg_catalog.jsonb_build_object(
        'approved_hours_state',v_approval->'state',
        'approved_total_hours',v_approval->'total_hours');
    end if;
    v_items:=v_items||pg_catalog.jsonb_build_array(v_item);
  end loop;
  return pg_catalog.jsonb_set(v_page,'{items}',v_items);
end;
$function$;

alter function private.weekly_source_candidate_list_approval_v1(uuid) owner to postgres;
alter function private.weekly_source_candidate_view_merge_v1(uuid,timestamptz) owner to postgres;
alter function public.candidate_app_timesheet_page_v2(uuid,text,text,text,integer,timestamptz) owner to postgres;
revoke all on function private.weekly_source_candidate_list_approval_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_view_merge_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_app_timesheet_page_v2(uuid,text,text,text,integer,timestamptz)
  from public,anon,authenticated;
grant execute on function public.candidate_app_timesheet_page_v2(uuid,text,text,text,integer,timestamptz)
  to service_role;

notify pgrst, 'reload schema';

commit;
