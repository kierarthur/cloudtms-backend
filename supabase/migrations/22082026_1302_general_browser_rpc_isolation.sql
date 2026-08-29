-- TEST-first CloudTMS browser isolation for existing non-Candidate/MyTMS
-- SECURITY DEFINER functions.
--
-- Exact runtime signatures are selected only after the complete ordered manifest
-- matches the audited TEST snapshot. Function bodies, owners, search paths,
-- volatility, and business semantics are not changed. The PostgREST pre-request
-- MFA gate is explicitly excluded so the Data API authentication contract remains
-- operational.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $rpc_isolation$
declare
  v_count integer;
  v_service_missing integer;
  v_hash text;
  v_browser_executable integer;
  v_apply_rpcs boolean := false;
  v_target record;
begin
  with targets as (
    select
      p.oid,
      p.oid::regprocedure::text as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
      and (
        pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
        or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
      )
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not svc_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text,E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_hash
  from targets;

  if v_count=403 and v_service_missing=0
     and v_hash='4f3e49bb874e7bc7299c95ecdce61b9a' then
    v_apply_rpcs := true;
  elsif v_count=0 then
    with targets as (
      select
        p.oid::regprocedure::text as signature,
        pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
        pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
        pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
      from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid=p.pronamespace
      where n.nspname='public'
        and p.prosecdef
        and p.proname<>'cloudtms_data_api_mfa_gate'
        and p.proname not ilike '%candidate%'
    )
    select
      pg_catalog.count(*),
      pg_catalog.count(*) filter (where not svc_execute),
      pg_catalog.count(*) filter (where anon_execute or auth_execute),
      pg_catalog.md5(coalesce(pg_catalog.string_agg(
        signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
        E'\n' order by signature
      ),''))
    into v_count,v_service_missing,v_browser_executable,v_hash
    from targets;

    if v_count<>639 or v_service_missing<>72 or v_browser_executable<>0
       or v_hash<>'c699635d63bc90d181a3e00b145eead3' then
      raise exception 'GENERAL_RPC_POST_MANIFEST_DRIFT:count=% service_missing=% browser_executable=% hash=%',
        v_count,v_service_missing,v_browser_executable,v_hash;
    end if;
  else
    raise exception 'GENERAL_RPC_MANIFEST_DRIFT:count=% service_missing=% hash=%',
      v_count,v_service_missing,v_hash;
  end if;

  if v_apply_rpcs then
    for v_target in
    select p.oid::regprocedure as signature
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
      and (
        pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
        or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
      )
    order by p.oid::regprocedure::text
    loop
      execute pg_catalog.format(
        'revoke all privileges on function %s from PUBLIC, anon, authenticated',
        v_target.signature
      );
      execute pg_catalog.format(
        'grant execute on function %s to service_role',v_target.signature
      );
    end loop;
  end if;
end
$rpc_isolation$;
