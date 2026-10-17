create or replace function private._candidate_manager_authoriser_domain_array_v2(p_value jsonb)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path=''
as $function$
declare
  v_result jsonb;
  v_count integer;
begin
  if p_value is not null and pg_catalog.jsonb_typeof(p_value)<>'array' then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_APPROVED_DOMAINS_INVALID';
  end if;
  select pg_catalog.count(*),coalesce(pg_catalog.jsonb_agg(v.domain order by v.domain),'[]'::jsonb)
    into v_count,v_result
  from (
    select distinct private._candidate_normalize_domain_v1(e.value) as domain
    from pg_catalog.jsonb_array_elements_text(coalesce(p_value,'[]'::jsonb)) e
  ) v
  where v.domain<>'';
  if v_count>50 or exists (
    select 1
    from pg_catalog.jsonb_array_elements_text(v_result) e
    where pg_catalog.char_length(e.value)>253
       or e.value!~'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$'
  ) then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_APPROVED_DOMAIN_INVALID';
  end if;
  return v_result;
end;
$function$;