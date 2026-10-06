-- Repeatable CloudTMS function/view authority: weekly_source_candidate_head_hours_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Approved clocks come from the exact chosen detail of the current committed
-- HEAD. A newer import/protected event with the same duration is not a source
-- of those clocks. This reader exposes hours only, never rates or money.
create or replace function private.weekly_source_candidate_head_hours_v2(
  p_root_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_inventory jsonb;
  v_basis jsonb;
  v_component jsonb;
  v_detail jsonb;
  v_clock jsonb;
  v_matches integer;
  v_date date;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_break integer;
  v_hours numeric;
  v_total numeric:=0;
  v_rows jsonb:='[]'::jsonb;
  v_unavailable constant jsonb:=jsonb_build_object('state','UNAVAILABLE',
    'reason','APPROVED_HEAD_DETAIL_NOT_DERIVABLE','authority',null,'head_id',null,
    'total_hours',null,'rows','[]'::jsonb);
begin
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  v_basis:=v_inventory->'approval_basis';
  if v_inventory->>'ok' is distinct from 'true'
     or v_basis->>'coverage_complete' is distinct from 'true'
     or v_basis#>>'{origin,kind}' is distinct from 'COMMITTED_SOURCE_HEAD_V1'
     or v_basis#>>'{scope,root_timesheet_id}' is distinct from p_root_timesheet_id::text
     or v_inventory->>'authority' is distinct from 'HEAD'
     or v_inventory->>'head_id' is distinct from v_basis#>>'{origin,head_id}'
     or to_regclass('private.bpay_next_source_chosen_detail') is null then return v_unavailable; end if;
  for v_component in select value from jsonb_array_elements(v_inventory->'components')
    where value->>'component_kind'='WORKED_TIME'
      and value->'exclude_from_pay'='false'::jsonb
    order by value->>'work_date',value->>'component_id' loop
    -- The I1 certificate already validates this exact record's component and
    -- detail hashes. Re-read the same closed identity in this statement snapshot.
    execute 'select count(*),jsonb_agg(d.detail_json)->0
      from private.bpay_next_source_chosen_detail d
      where d.head_id=$1 and d.component_id=$2
        and d.decision_bundle_id=$3 and d.bundle_revision=$4
        and d.component_sha256=$5
        and d.detail_sha256=pg_catalog.sha256(pg_catalog.convert_to(d.detail_json::text,''UTF8''))'
      into v_matches,v_detail using (v_basis#>>'{origin,head_id}')::uuid,
        (v_component->>'component_id')::uuid,(v_basis#>>'{origin,decision_bundle_id}')::uuid,
        (v_basis#>>'{origin,bundle_revision}')::bigint,decode(v_component->>'component_sha256','hex');
    if v_matches<>1 or v_detail->>'work_date' is distinct from v_component->>'work_date'
       then return v_unavailable; end if;
    v_date:=(v_component->>'work_date')::date;
    if v_date not between (v_basis#>>'{scope,week_ending_date}')::date-6
                         and (v_basis#>>'{scope,week_ending_date}')::date then return v_unavailable; end if;
    v_clock:=private.weekly_source_candidate_approved_clock_row_v2(
      v_detail,v_component,'approved-'||(v_component->>'component_id'));
    if v_clock is null then return v_unavailable; end if;
    v_hours:=(v_clock->>'hours')::numeric;
    v_rows:=v_rows||jsonb_build_array(v_clock->'row');
    v_total:=v_total+v_hours;
  end loop;
  select coalesce(jsonb_agg(r.value order by r.value->>'date',
    r.value->>'start' collate "C",r.value->>'row_key' collate "C"),'[]'::jsonb)
    into v_rows from jsonb_array_elements(v_rows) r(value);
  return jsonb_build_object('state','AVAILABLE','reason',null,'authority','HEAD',
    'head_id',v_basis#>'{origin,head_id}','head_revision',v_basis#>'{origin,head_revision}',
    'certified_zero',(v_basis->>'component_count')::integer=0,
    'component_count',(v_basis->>'component_count')::integer,'total_hours',v_total,'rows',v_rows);
end;
$function$;

alter function private.weekly_source_candidate_head_hours_v2(uuid) owner to postgres;
revoke all on function private.weekly_source_candidate_head_hours_v2(uuid)
  from public,anon,authenticated,service_role;

commit;
