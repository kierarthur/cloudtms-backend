do $verification$
declare
  v_signature regprocedure := pg_catalog.to_regprocedure(
    'private._candidate_manager_email_claim_route_current_v1(uuid,uuid,bigint,text,uuid,uuid,integer)'
  );
begin
  if v_signature is null then
    raise exception using
      errcode='ZX999',
      message='MIGET_PRIVATE_HELPER_MISSING';
  end if;

  if not pg_catalog.has_schema_privilege('service_role','private','USAGE')
     or not pg_catalog.has_function_privilege(
       'service_role',v_signature,'EXECUTE'
     ) then
    raise exception using
      errcode='ZX999',
      message='MIGET_PRIVATE_HELPER_SERVICE_ACCESS_MISSING';
  end if;

  if pg_catalog.has_function_privilege('anon',v_signature,'EXECUTE')
     or pg_catalog.has_function_privilege(
       'authenticated',v_signature,'EXECUTE'
     ) then
    raise exception using
      errcode='ZX999',
      message='MIGET_PRIVATE_HELPER_BROWSER_ACCESS_PRESENT';
  end if;
end
$verification$;

select 'PASS'::text as miget_private_helper_service_acl_verification;
