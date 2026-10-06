-- Repeatable CloudTMS function/view authority: weekly_source_candidate_initial_approval_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- The initial Source authorisation can be a genuine authorised TSFIN without
-- a HEAD. Read only its positively certified I1 basis; never a proposed TSFIN,
-- candidate submission or guessed aggregate. No money enters Candidate rows.
create or replace function private.weekly_source_candidate_initial_approval_v2(
  p_root_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_inventory jsonb;
  v_basis jsonb;
  v_fin public.timesheets_financials%rowtype;
  v_component jsonb;
  v_segment jsonb;
  v_clock jsonb;
  v_matches integer;
  v_date date;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_break integer;
  v_component_hours numeric;
  v_total numeric:=0;
  v_rows jsonb:='[]'::jsonb;
  v_unavailable constant jsonb:=jsonb_build_object('state','UNAVAILABLE',
    'reason','INITIAL_APPROVED_HOURS_NOT_DERIVABLE','authority',null,'head_id',null,
    'total_hours',null,'rows','[]'::jsonb);
begin
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  v_basis:=v_inventory->'approval_basis';
  if (v_inventory->>'ok') is distinct from 'true'
     or (v_basis->>'coverage_complete') is distinct from 'true'
     or v_basis#>>'{origin,kind}' is distinct from 'INITIAL_AUTHORISED_TSFIN_V1'
     or v_basis#>>'{scope,root_timesheet_id}' is distinct from p_root_timesheet_id::text
     or v_inventory->>'authority' is distinct from 'TSFIN' then return v_unavailable; end if;
  select * into v_fin from public.timesheets_financials f
    where f.id=(v_basis#>>'{origin,financial_snapshot_id}')::uuid
      and f.timesheet_id=p_root_timesheet_id and f.is_current
      and f.authorised_at_utc is not null;
  if not found or jsonb_typeof(v_fin.invoice_breakdown_json->'segments') is distinct from 'array' then
    return v_unavailable;
  end if;
  for v_component in select value from jsonb_array_elements(v_inventory->'components')
    where value->>'component_kind'='WORKED_TIME'
      and value->'exclude_from_pay'='false'::jsonb
    order by value->>'work_date',value->>'component_id' loop
    select count(*),jsonb_agg(s.value)->0 into v_matches,v_segment
      from jsonb_array_elements(v_fin.invoice_breakdown_json->'segments') s(value)
      where s.value->>'segment_id'=v_component->>'segment_id'
        and private.weekly_source_entitlement_component_id_v1('WORKED_TIME','SEGMENT',
          s.value->>'segment_id',coalesce(s.value#>>'{weekly_source,work_event_id}',s.value->>'segment_id'))::text
              =v_component->>'component_id';
    if v_matches<>1
       or v_segment->>'date' is distinct from v_component->>'work_date'
       then return v_unavailable; end if;
    v_date:=(v_component->>'work_date')::date;
    if v_date not between (v_basis#>>'{scope,week_ending_date}')::date-6
                         and (v_basis#>>'{scope,week_ending_date}')::date then return v_unavailable; end if;
    v_clock:=private.weekly_source_candidate_approved_clock_row_v2(
      v_segment,v_component,'approved-'||(v_component->>'component_id'));
    if v_clock is null then return v_unavailable; end if;
    v_component_hours:=(v_clock->>'hours')::numeric;
    v_rows:=v_rows||jsonb_build_array(v_clock->'row');
    v_total:=v_total+v_component_hours;
  end loop;
  if v_total is distinct from v_fin.total_hours then return v_unavailable; end if;
  select coalesce(jsonb_agg(r.value order by r.value->>'date',
    r.value->>'start' collate "C",r.value->>'row_key' collate "C"),'[]'::jsonb)
    into v_rows from jsonb_array_elements(v_rows) r(value);
  return jsonb_build_object('state','AVAILABLE','reason',null,
    'authority','INITIAL_AUTHORISED_TSFIN_V1','head_id',null,
    'certified_zero',(v_basis->>'component_count')::integer=0,'total_hours',v_total,'rows',v_rows);
end;
$function$;

alter function private.weekly_source_candidate_initial_approval_v2(uuid) owner to postgres;
revoke all on function private.weekly_source_candidate_initial_approval_v2(uuid)
  from public,anon,authenticated,service_role;

commit;
