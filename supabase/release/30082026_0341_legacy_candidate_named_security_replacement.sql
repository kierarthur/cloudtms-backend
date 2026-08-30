-- Provider-neutral LEGACY_UPGRADE replacement for the protected Candidate-
-- named SECURITY DEFINER browser boundary. The original protected repeatable
-- remains byte-identical; its historical pre-state fingerprint includes the
-- Supabase grantor/ACL history. This replacement accepts only the exact Miget
-- transition manifest, preserves the service_role EXECUTE matrix byte-for-
-- byte, and removes browser execution without changing a function body. Its
-- identity is built from explicit catalogue schemas/names, never from the
-- search_path-dependent regprocedure display form.

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
  v_expected_service_hash text;
  v_expected_acl_before text;
  v_expected_acl_after text;
  v_target record;
begin
  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
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

  if v_count<>85 or v_identity_hash<>'cc2fdf2c039a8fe660d7f916211d5b48' then
    raise exception 'LEGACY_CANDIDATE_NAMED_RPC_MANIFEST_DRIFT:count=% service_missing=% browser=% identity_hash=% service_hash=% acl_hash=%',
      v_count,v_service_missing,v_browser_executable,v_identity_hash,v_service_hash,v_acl_hash;
  end if;

  if v_service_missing=8 and v_service_hash='a2385961fc412bc27af963ffa1d8b1d5' then
    -- The exact current repository ordering converges both a fresh legacy
    -- checkpoint and the hosted interrupted checkpoint to these five already
    -- installed repository-authorised service grants before this boundary.
    v_expected_service_hash := 'a2385961fc412bc27af963ffa1d8b1d5';
    v_expected_acl_before := '49848ad832ddbfcdf8c42851e04a83b7';
    v_expected_acl_after := 'e7dd4bf723616c232bf09f07df1253f3';
  else
    raise exception 'LEGACY_CANDIDATE_NAMED_RPC_SERVICE_MANIFEST_DRIFT:service_missing=% service_hash=%',
      v_service_missing,v_service_hash;
  end if;

  if v_browser_executable=30 and v_acl_hash=v_expected_acl_before then
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
  elsif not (v_browser_executable=0 and v_acl_hash=v_expected_acl_after) then
    raise exception 'LEGACY_CANDIDATE_NAMED_RPC_ACL_MANIFEST_DRIFT:browser=% acl_hash=%',
      v_browser_executable,v_acl_hash;
  end if;

  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
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
     or v_service_hash<>v_expected_service_hash
     or v_acl_hash<>v_expected_acl_after then
    raise exception 'LEGACY_CANDIDATE_NAMED_RPC_ISOLATION_FAILED:browser=% service_hash=% acl_hash=%',
      v_browser_executable,v_service_hash,v_acl_hash;
  end if;
end
$legacy_candidate_named_rpc_isolation$;
