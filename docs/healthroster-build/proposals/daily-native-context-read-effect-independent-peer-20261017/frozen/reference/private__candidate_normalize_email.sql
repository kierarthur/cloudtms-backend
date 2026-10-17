create or replace function private._candidate_normalize_email(p_email text)
returns text
language sql
immutable
parallel safe
set search_path = pg_catalog
as $function$
  select lower(btrim(coalesce(p_email,'')));
$function$;