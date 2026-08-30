-- LEGACY_UPGRADE-only provider-neutral replacement for the immutable
-- Supabase migration that hard-codes the database name `postgres`.
-- Miget assigns each service a generated database name. Preserve the exact
-- operational settings on the connected target database without touching
-- application rows, function bodies, extension membership, or server-wide
-- configuration.

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $apply_current_database_defaults$
declare
  v_database name := current_database();
begin
  execute format('alter database %I set "plpgsql_check.mode" to %L',v_database,'disabled');
  execute format('alter database %I set "plpgsql_check.profiler" to %L',v_database,'off');
  execute format('alter database %I set "plpgsql_check.tracer" to %L',v_database,'off');
  execute format('alter database %I set "plpgsql_check.constants_tracing" to %L',v_database,'off');
  execute format('alter database %I set "plpgsql_check.cursors_leaks" to %L',v_database,'off');
  execute format('alter database %I set "plpgsql_check.strict_cursors_leaks" to %L',v_database,'off');
  execute format('alter database %I set "plpgsql_check.fatal_errors" to %L',v_database,'off');
end
$apply_current_database_defaults$;

do $verify_current_database_defaults$
declare
  v_settings text[];
  v_expected constant text[] := array[
    'plpgsql_check.mode=disabled',
    'plpgsql_check.profiler=off',
    'plpgsql_check.tracer=off',
    'plpgsql_check.constants_tracing=off',
    'plpgsql_check.cursors_leaks=off',
    'plpgsql_check.strict_cursors_leaks=off',
    'plpgsql_check.fatal_errors=off'
  ];
begin
  select s.setconfig
  into v_settings
  from pg_catalog.pg_db_role_setting s
  join pg_catalog.pg_database d on d.oid=s.setdatabase
  where d.datname=current_database()
    and s.setrole=0;

  if v_settings is null or not v_settings @> v_expected then
    raise exception 'LEGACY_PLPGSQL_CHECK_DATABASE_DEFAULTS_NOT_APPLIED';
  end if;
end
$verify_current_database_defaults$;
