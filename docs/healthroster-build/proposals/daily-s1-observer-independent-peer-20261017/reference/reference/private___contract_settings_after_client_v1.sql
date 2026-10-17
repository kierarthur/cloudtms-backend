create or replace function private._contract_settings_after_client_v1()
returns trigger language plpgsql volatile security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
begin
  if tg_op='DELETE' then
    perform private._contract_settings_refresh_planned_for_client_v1(old.client_id);
    return old;
  end if;
  perform private._contract_settings_refresh_planned_for_client_v1(new.client_id);
  return new;
end
$function$;