-- Final provider-neutral browser isolation authority for public SECURITY
-- DEFINER functions. Application and Office Workers use service_role; browser
-- roles must never inherit direct PostgREST execution through PUBLIC defaults.

\set ON_ERROR_STOP on

begin;

do $browser_isolation$
declare
  v_signature regprocedure;
begin
  for v_signature in
    select p.oid::regprocedure
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.prosecdef
      and p.proname <> 'cloudtms_data_api_mfa_gate'
  loop
    execute pg_catalog.format(
      'revoke all privileges on function %s from PUBLIC, anon, authenticated',
      v_signature
    );
  end loop;

  if exists (
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.prosecdef
      and p.proname <> 'cloudtms_data_api_mfa_gate'
      and (
        pg_catalog.has_function_privilege('anon', p.oid, 'EXECUTE')
        or pg_catalog.has_function_privilege('authenticated', p.oid, 'EXECUTE')
      )
  ) then
    raise exception using
      errcode = 'ZX999',
      message = 'GENERAL_SECURITY_DEFINER_BROWSER_ISOLATION_FAILED';
  end if;
end
$browser_isolation$;

commit;
