-- Source Office summary: one canonical paged read and its category metadata
-- share a STABLE statement snapshot. Fixed row signatures are unchanged.
\set ON_ERROR_STOP on
begin;

create or replace function public.weekly_source_office_summary_rows_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_filters jsonb;
  v_row jsonb;
  v_root public.timesheets%rowtype;
  v_category jsonb;
  v_rows jsonb:='[]'::jsonb;
  v_authority_checked boolean:=false;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,array['actor_user_id','p_filters'])
     or not (p_request ?& array['actor_user_id','p_filters'])
     or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
     or jsonb_typeof(p_request->'p_filters') is distinct from 'object' then
    raise exception 'WEEKLY_SOURCE_SUMMARY_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_filters:=p_request->'p_filters';
  -- A page reader, not a second membership/count/financial owner. Normal
  -- Office pages and targeted patches already supply this explicit bound.
  if v_actor is null or coalesce(v_filters->>'limit','') !~ '^[0-9]{1,3}$'
     or (v_filters->>'limit')::integer not between 1 and 200
     or lower(coalesce(v_filters->>'disable_paging',v_filters->>'disablePaging',
         v_filters->>'no_paging',v_filters->>'noPaging','')) in ('true','t','yes','y','1')
     or lower(coalesce(v_filters->>'apply_paging',v_filters->>'applyPaging','')) in ('false','f','no','n','0')
     or lower(coalesce(v_filters->>'purpose','')) in
         ('membership','memberships','ids','totals','total','count','counts') then
    raise exception 'WEEKLY_SOURCE_SUMMARY_PAGE_REQUIRED' using errcode='22023';
  end if;
  -- Exactly one call with the ORIGINAL filters. Do not re-sort, re-page,
  -- introduce membership predicates or drop contract-week-only rows.
  for v_row in select to_jsonb(r) from public.timesheet_summary_lightweight_rows_v1(v_filters) r loop
    if v_row->>'timesheet_id' is not null and v_row->>'sheet_scope'='WEEKLY'
       and (v_row->>'route_type'='WEEKLY_NHSP'
         or (v_row->>'route_type'='WEEKLY_HEALTHROSTER'
             and v_row->'client_no_timesheet_required'='true'::jsonb)
         -- A cold protected-only root may retain the ordinary route display
         -- until Source arrives. Its actual TARGET ownership, not that label,
         -- qualifies Source metadata. Never match just Candidate/week.
         or exists(select 1 from public.timesheets t
           join public.contracts c on c.id=t.contract_id
           join public.weekly_exceptional_pay_target_families f
             on f.root_family_booking_id=t.booking_id
               and f.root_timesheet_id=t.timesheet_id
               and f.candidate_id=c.candidate_id and f.contract_id=t.contract_id
               and f.week_ending_date=t.week_ending_date
           where t.timesheet_id=(v_row->>'timesheet_id')::uuid
             and t.is_adjustment=false
             and f.ownership_state='TARGET_MANAGED')) then
      if not v_authority_checked then
        perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS');
        v_authority_checked:=true;
      end if;
      select t.* into strict v_root from public.timesheets t
        where t.timesheet_id=(v_row->>'timesheet_id')::uuid;
      v_category:=private.weekly_source_timesheet_category_v2(v_root.timesheet_id);
      if v_category is not null and
         (v_category#>>'{category_basis,facts,root_timesheet_id}' is distinct from v_root.timesheet_id::text
          or v_category#>'{category_basis,facts,root_version}' is distinct from to_jsonb(v_root.version)
          or (v_category->>'presentation_category' is not null
            and v_category->>'presentation_category' is distinct from v_row->>'tools_stage')) then
        raise exception 'WEEKLY_SOURCE_SUMMARY_CATEGORY_CHANGED' using errcode='40001';
      end if;
      v_row:=v_row||jsonb_build_object('weekly_source_root_version',v_root.version,
        'weekly_source_operational_category',v_category,
        'weekly_source_processing_reason',v_category->'processing_reason');
    end if;
    v_rows:=v_rows||jsonb_build_array(v_row);
  end loop;
  return jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_SUMMARY_ROWS_V1','rows',v_rows);
exception when invalid_text_representation then
  raise exception 'WEEKLY_SOURCE_SUMMARY_REQUEST_INVALID' using errcode='22023';
end;
$function$;
alter function public.weekly_source_office_summary_rows_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_summary_rows_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_office_summary_rows_v1(jsonb) to service_role;
notify pgrst, 'reload schema';
commit;
