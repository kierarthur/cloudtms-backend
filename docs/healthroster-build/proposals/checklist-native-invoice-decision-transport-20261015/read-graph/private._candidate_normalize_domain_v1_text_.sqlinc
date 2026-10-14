create or replace function private._candidate_normalize_domain_v1(p_domain text)
returns text
language sql
immutable
parallel safe
set search_path = pg_catalog
as $function$
  select lower(regexp_replace(btrim(coalesce(p_domain,'')), '^@', ''));
$function$;