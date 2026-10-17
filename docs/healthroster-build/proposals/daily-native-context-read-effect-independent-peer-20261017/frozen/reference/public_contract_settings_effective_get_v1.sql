create or replace function public.contract_settings_effective_get_v1(
  p_client_id uuid,
  p_contract_id uuid,
  p_relevant_date date,
  p_workflow text,
  p_timesheet_id uuid default null
)
returns jsonb
language sql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
  select private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,p_relevant_date,p_workflow,p_timesheet_id
  )
$function$;