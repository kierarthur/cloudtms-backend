-- Verifies the two Daily Validation compatibility authorities without reading
-- or mutating application rows. The general RPC fingerprint deliberately
-- excludes these named functions so this focused contract owns their posture.

do $daily_validation_compatibility_security_verification$
declare
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_owner_or_shape_mismatch integer;
begin
  with expected(signature,expected_config) as (
    values
      (
        'timesheet_break_entry_effective_get_v1(uuid,uuid,date)',
        array['search_path=public, pg_temp']::text[]
      ),
      (
        'daily_zero_shifts_review_create_v1(uuid,date,date,uuid,text)',
        array['search_path=public, extensions, pg_temp']::text[]
      )
  ), targets as (
    select
      e.signature,
      p.oid,
      p.prosecdef,
      p.proconfig,
      p.proowner as owner_oid,
      language_row.lanname as language_name,
      pg_catalog.pg_get_function_result(p.oid) as result_type,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as service_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as authenticated_execute,
      e.expected_config
    from expected e
    left join pg_catalog.pg_proc p
      on p.oid=pg_catalog.to_regprocedure('public.'||e.signature)
    left join pg_catalog.pg_roles owner_role on owner_role.oid=p.proowner
    left join pg_catalog.pg_language language_row on language_row.oid=p.prolang
  )
  select
    pg_catalog.count(*) filter (where oid is not null),
    pg_catalog.count(*) filter (where oid is null or not service_execute),
    pg_catalog.count(*) filter (
      where oid is not null and (anon_execute or authenticated_execute)
    ),
    pg_catalog.count(*) filter (
      where oid is null
         or not prosecdef
         or owner_oid<>(select oid from pg_catalog.pg_roles where rolname=current_user)
         or language_name<>'plpgsql'
         or result_type<>'jsonb'
         or proconfig is distinct from expected_config
    )
  into v_count,v_service_missing,v_browser_executable,v_owner_or_shape_mismatch
  from targets;

  if v_count<>2
     or v_service_missing<>0
     or v_browser_executable<>0
     or v_owner_or_shape_mismatch<>0 then
    raise exception
      'DAILY_VALIDATION_COMPATIBILITY_SECURITY_VERIFICATION_FAILED:count=% service_missing=% browser_executable=% shape_mismatch=%',
      v_count,v_service_missing,v_browser_executable,v_owner_or_shape_mismatch;
  end if;
end
$daily_validation_compatibility_security_verification$;
