-- Provider-neutral LEGACY_UPGRADE replacement for the protected Candidate-
-- named SECURITY DEFINER browser boundary. The original protected repeatable
-- remains byte-identical; its historical pre-state fingerprint includes the
-- Supabase grantor/ACL history. This replacement accepts only the exact Miget
-- transition manifest, preserves the service_role EXECUTE matrix byte-for-
-- byte, and removes browser execution without changing a function body.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $legacy_candidate_named_rpc_isolation$
declare
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_identity_hash text;
  v_service_hash text;
  v_acl_hash text;
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
    pg_catalog.md5(coalesce(pg_catalog.string_agg(signature,E'\n' order by signature),'')),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text,E'\n' order by signature
    ),'')),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_browser_executable,v_identity_hash,v_service_hash,v_acl_hash
  from targets;

  if v_count<>85 or v_service_missing<>13
     or v_identity_hash<>'9a750d0555772cb2902c02ec73d56711'
     or v_service_hash<>'1358ce0e91782fffa75f9199067f6bdd' then
    raise exception 'LEGACY_CANDIDATE_NAMED_RPC_MANIFEST_DRIFT:count=% service_missing=% identity_hash=% service_hash=%',
      v_count,v_service_missing,v_identity_hash,v_service_hash;
  end if;

  if v_browser_executable=30 and v_acl_hash='8f676bd1721798c756a8f0126469f661' then
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
  elsif not (v_browser_executable=0 and v_acl_hash='d74699ad8c6938055b2d83e883feeee9') then
    raise exception 'LEGACY_CANDIDATE_NAMED_RPC_ACL_MANIFEST_DRIFT:browser=% acl_hash=%',
      v_browser_executable,v_acl_hash;
  end if;

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
    pg_catalog.count(*) filter (where anon_execute or auth_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text,E'\n' order by signature
    ),'')),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_browser_executable,v_service_hash,v_acl_hash
  from targets;

  if v_browser_executable<>0
     or v_service_hash<>'1358ce0e91782fffa75f9199067f6bdd'
     or v_acl_hash<>'d74699ad8c6938055b2d83e883feeee9' then
    raise exception 'LEGACY_CANDIDATE_NAMED_RPC_ISOLATION_FAILED:browser=% service_hash=% acl_hash=%',
      v_browser_executable,v_service_hash,v_acl_hash;
  end if;
end
$legacy_candidate_named_rpc_isolation$;
