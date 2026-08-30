-- LEGACY_UPGRADE-only provider-neutral replacement for the immutable general
-- browser RPC-isolation migration. The original migration is pinned to one
-- exact Supabase TEST ACL snapshot and uses search_path-dependent regprocedure
-- display text. This replacement accepts only the exact structural routine
-- identity manifest reconstructed from the sealed LIVE clone, preserves the
-- effective service_role EXECUTE matrix, and removes browser execution.

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $legacy_general_rpc_isolation$
declare
  v_count integer;
  v_identity_hash text;
  v_service_hash_before text;
  v_service_hash_after text;
  v_browser_executable integer;
  v_target record;
begin
  with targets as (
    select
      p.oid,
      n.nspname||'.'||p.proname||'('||coalesce((
        select string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
  )
  select
    count(*)::integer,
    md5(coalesce(string_agg(signature,E'\n' order by signature),'')),
    md5(coalesce(string_agg(
      signature||'|'||svc_execute::text,E'\n' order by signature
    ),''))
  into v_count,v_identity_hash,v_service_hash_before
  from targets;

  if v_count<>354 or v_identity_hash<>'29c373707fca29a303b91fb0144b7d78' then
    raise exception 'LEGACY_GENERAL_RPC_IDENTITY_MANIFEST_DRIFT:count=% hash=%',
      v_count,v_identity_hash;
  end if;

  for v_target in
    select
      p.oid::regprocedure as executable_signature,
      has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
    order by p.oid
  loop
    execute format(
      'revoke all privileges on function %s from PUBLIC, anon, authenticated',
      v_target.executable_signature
    );
    if v_target.svc_execute then
      execute format(
        'grant execute on function %s to service_role',
        v_target.executable_signature
      );
    end if;
  end loop;

  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
  )
  select
    md5(coalesce(string_agg(
      signature||'|'||svc_execute::text,E'\n' order by signature
    ),'')),
    count(*) filter (where anon_execute or auth_execute)::integer
  into v_service_hash_after,v_browser_executable
  from targets;

  if v_service_hash_after<>v_service_hash_before then
    raise exception 'LEGACY_GENERAL_RPC_SERVICE_ACL_CHANGED';
  end if;
  if v_browser_executable<>0 then
    raise exception 'LEGACY_GENERAL_RPC_BROWSER_EXECUTE_REMAINS:count=%',
      v_browser_executable;
  end if;
end
$legacy_general_rpc_isolation$;
