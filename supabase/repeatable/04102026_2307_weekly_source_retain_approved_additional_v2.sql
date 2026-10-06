-- Repeatable CloudTMS function/view authority: weekly_source_retain_approved_additional_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Later Source hours replace worked/source-expense components, not separately
-- approved Additional earnings. Read the complete actual approved inventory;
-- unavailable evidence is not permission to drop those earnings or reprice.
create or replace function private.weekly_source_retain_approved_additional_v2(
  p_root_timesheet_id uuid,p_components jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_inventory jsonb;
  v_basis jsonb;
  v_result jsonb:='[]'::jsonb;
  v_component jsonb;
  v_ordinal integer:=0;
begin
  if p_root_timesheet_id is null or jsonb_typeof(p_components) is distinct from 'array' then
    raise exception 'WEEKLY_SOURCE_ADDITIONAL_RETENTION_INVALID' using errcode='22023';
  end if;
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  v_basis:=v_inventory->'approval_basis';
  if v_inventory->>'ok' is distinct from 'true'
     or v_basis->>'coverage_complete' is distinct from 'true'
     or v_basis#>>'{scope,root_timesheet_id}' is distinct from p_root_timesheet_id::text
     or v_basis#>>'{origin,kind}' not in ('INITIAL_AUTHORISED_TSFIN_V1','COMMITTED_SOURCE_HEAD_V1')
     or v_basis#>>'{origin,kind}' is null then
    raise exception 'WEEKLY_SOURCE_APPROVED_ADDITIONAL_UNAVAILABLE' using errcode='55000';
  end if;
  for v_component in select value from jsonb_array_elements(p_components) loop
    v_ordinal:=v_ordinal+1;
    if v_component->>'component_kind'='ADDITIONAL_UNIT'
       or (v_component->>'component_ordinal')::integer is distinct from v_ordinal then
      raise exception 'WEEKLY_SOURCE_ADDITIONAL_RETENTION_INVALID' using errcode='22023';
    end if;
    v_result:=v_result||jsonb_build_array(
      private.weekly_source_publication_component_canonical_v1(v_component,'retained.base'));
  end loop;
  for v_component in select c.value-'component_sha256'
    from jsonb_array_elements(v_inventory->'components') c(value)
    where c.value->>'component_kind'='ADDITIONAL_UNIT'
    order by (c.value->>'component_ordinal')::integer loop
    v_ordinal:=v_ordinal+1;
    -- Preserve the already approved UUID, stable family/code identity, units,
    -- rates, amounts and exclusions. Only its position in the new vector moves.
    v_component:=jsonb_set(v_component,'{component_ordinal}',to_jsonb(v_ordinal));
    v_result:=v_result||jsonb_build_array(
      private.weekly_source_publication_component_canonical_v1(v_component,'retained.additional'));
  end loop;
  if exists(select 1 from jsonb_array_elements(v_result) c(value)
    group by c.value->>'component_id' having count(*)<>1) then
    raise exception 'WEEKLY_SOURCE_ADDITIONAL_RETENTION_INVALID' using errcode='22023';
  end if;
  return v_result;
end;
$function$;

alter function private.weekly_source_retain_approved_additional_v2(uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_retain_approved_additional_v2(uuid,jsonb)
  from public,anon,authenticated,service_role;

commit;
