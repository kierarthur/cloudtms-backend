create or replace function private._candidate_feature_enabled_current_v1(
  p_feature_key text
)
returns boolean
language sql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select coalesce((sd.candidate_app_feature_flags_json->>btrim(coalesce(p_feature_key,'')))::boolean,false)
  from public.settings_defaults sd
  where sd.id=1;
$function$;