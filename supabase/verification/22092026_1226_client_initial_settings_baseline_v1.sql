\set ON_ERROR_STOP on

begin;

do $verify$
declare
  v_actor uuid := '99220922-1226-4000-8000-000000000001';
  v_client uuid := '99220922-1226-4000-8000-000000000002';
  v_result jsonb;
begin
  insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
  values(v_actor,'initial-client-settings-proof@example.invalid','admin','not-a-password','Initial settings proof',true);

  v_result:=public.client_create_with_settings_v1(
    v_client,
    '{"name":"Initial settings baseline proof"}'::jsonb,
    v_actor,
    '{"effective_from":"2026-09-22"}'::jsonb
  );
  if v_result->'client_settings'->>'effective_from'<>'1900-01-01' then
    raise exception 'CLIENT_INITIAL_SETTINGS_BASELINE_NOT_1900';
  end if;

  v_result:=public.client_create_with_settings_v1(
    v_client,
    '{"name":"Initial settings baseline proof"}'::jsonb,
    v_actor,
    '{"effective_from":"2026-09-22"}'::jsonb
  );
  if v_result->>'replay'<>'true' then
    raise exception 'CLIENT_INITIAL_SETTINGS_REPLAY_FAILED';
  end if;

  perform public.contract_settings_effective_get_v1(
    v_client,null,'2026-09-14','WEEKLY',null
  );

  update public.client_settings
  set effective_from=DATE '2026-09-25'
  where client_id=v_client;
  if not exists(
    select 1 from public.client_settings
    where client_id=v_client and effective_from=DATE '2026-09-25'
  ) then
    raise exception 'LATER_SETTINGS_REVISION_DATE_NOT_PRESERVED';
  end if;

  if has_function_privilege('anon','public.client_create_with_settings_v1(uuid,jsonb,uuid,jsonb,timestamptz)','EXECUTE')
     or has_function_privilege('authenticated','public.client_create_with_settings_v1(uuid,jsonb,uuid,jsonb,timestamptz)','EXECUTE')
     or not has_function_privilege('service_role','public.client_create_with_settings_v1(uuid,jsonb,uuid,jsonb,timestamptz)','EXECUTE') then
    raise exception 'CLIENT_INITIAL_SETTINGS_FUNCTION_ACL_INVALID';
  end if;
end
$verify$;

rollback;
