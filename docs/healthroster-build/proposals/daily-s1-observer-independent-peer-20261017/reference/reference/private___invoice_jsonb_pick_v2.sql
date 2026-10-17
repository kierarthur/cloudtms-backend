create or replace function private._invoice_jsonb_pick_v2(
  p_value jsonb,
  p_keys text[]
) returns jsonb
language sql
immutable
security invoker
set search_path to 'public','private','extensions','pg_temp'
as $function$
  select coalesce(
    jsonb_object_agg(e.key, e.value order by e.key),
    '{}'::jsonb
  )
  from jsonb_each(coalesce(p_value, '{}'::jsonb)) e
  where e.key = any(coalesce(p_keys, array[]::text[]));
$function$;