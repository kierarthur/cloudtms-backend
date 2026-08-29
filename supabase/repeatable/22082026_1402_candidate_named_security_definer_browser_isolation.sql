-- Repeatable final ACL boundary for Candidate-named SECURITY DEFINER functions.
--
-- Earlier authoritative repeatables can recreate these functions and their
-- historical browser grants. This later ordered repeatable reasserts the Worker-
-- only boundary after every source replacement. It changes no function body,
-- owner, search_path, volatility, business meaning, financial rule, or Policy X
-- authority and preserves every existing service_role EXECUTE grant.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $candidate_named_rpc_isolation$
declare
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_hash text;
  v_target record;
begin
  with targets as (
    select
      p.oid::regprocedure::text as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public' and p.prosecdef and p.proname ilike '%candidate%'
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

  if v_count=85 and v_service_missing=7 and v_browser_executable=34
     and v_hash='92b080451840f5ab3940fb540907d466' then
    for v_target in
      select p.oid::regprocedure as signature
      from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid=p.pronamespace
      where n.nspname='public' and p.prosecdef and p.proname ilike '%candidate%'
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
    end loop;
  elsif not (
    v_count=85 and v_service_missing=7 and v_browser_executable=0
    and v_hash='1058d64351f6e5cbbe572564d7c89b28'
  ) then
    raise exception 'CANDIDATE_NAMED_RPC_MANIFEST_DRIFT:count=% service_missing=% browser_executable=% hash=%',
      v_count,v_service_missing,v_browser_executable,v_hash;
  end if;
end
$candidate_named_rpc_isolation$;
