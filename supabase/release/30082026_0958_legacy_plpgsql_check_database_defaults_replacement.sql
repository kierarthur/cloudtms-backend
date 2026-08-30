-- LEGACY_UPGRADE-only provider-neutral replacement for the immutable
-- Supabase migration that hard-codes the database name `postgres`.
-- Miget assigns each service a generated database name. Preserve the exact
-- operational settings on the connected target database when the provider
-- exposes them. Miget does not install or expose plpgsql_check and rejects its
-- superuser-only database GUCs; that exact wholly-absent state is verified as
-- safely non-applicable. No application row, function body, extension
-- membership, or server-wide configuration is changed.

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $apply_current_database_defaults$
declare
  v_database name := current_database();
  v_extension_installed boolean;
  v_expected_setting_count integer;
  v_any_setting_count integer;
  v_persisted_override_count integer;
  v_expected_names constant text[] := array[
    'plpgsql_check.mode',
    'plpgsql_check.profiler',
    'plpgsql_check.tracer',
    'plpgsql_check.constants_tracing',
    'plpgsql_check.cursors_leaks',
    'plpgsql_check.strict_cursors_leaks',
    'plpgsql_check.fatal_errors'
  ];
begin
  select exists (
    select 1 from pg_catalog.pg_extension where extname='plpgsql_check'
  ) into v_extension_installed;

  select
    pg_catalog.count(*) filter (where name=any(v_expected_names))::integer,
    pg_catalog.count(*)::integer
  into v_expected_setting_count,v_any_setting_count
  from pg_catalog.pg_settings
  where name like 'plpgsql_check.%';

  select pg_catalog.count(*)::integer
  into v_persisted_override_count
  from pg_catalog.pg_db_role_setting s
  join pg_catalog.pg_database d on d.oid=s.setdatabase
  cross join lateral pg_catalog.unnest(s.setconfig) as configured(setting)
  where d.datname=current_database()
    and s.setrole=0
    and configured.setting like 'plpgsql_check.%';

  if not v_extension_installed
     and v_any_setting_count=0
     and v_persisted_override_count=0 then
    raise notice 'LEGACY_PLPGSQL_CHECK_NOT_AVAILABLE_ON_PROVIDER';
    return;
  end if;

  if v_expected_setting_count<>pg_catalog.array_length(v_expected_names,1) then
    raise exception 'LEGACY_PLPGSQL_CHECK_EXPECTED_SETTINGS_MISSING:extension=% expected=% any=% overrides=%',
      v_extension_installed,v_expected_setting_count,v_any_setting_count,v_persisted_override_count;
  end if;

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
  v_extension_installed boolean;
  v_expected_setting_count integer;
  v_any_setting_count integer;
  v_persisted_override_count integer;
  v_expected_names constant text[] := array[
    'plpgsql_check.mode',
    'plpgsql_check.profiler',
    'plpgsql_check.tracer',
    'plpgsql_check.constants_tracing',
    'plpgsql_check.cursors_leaks',
    'plpgsql_check.strict_cursors_leaks',
    'plpgsql_check.fatal_errors'
  ];
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
  select exists (
    select 1 from pg_catalog.pg_extension where extname='plpgsql_check'
  ) into v_extension_installed;

  select
    pg_catalog.count(*) filter (where name=any(v_expected_names))::integer,
    pg_catalog.count(*)::integer
  into v_expected_setting_count,v_any_setting_count
  from pg_catalog.pg_settings
  where name like 'plpgsql_check.%';

  select pg_catalog.count(*)::integer
  into v_persisted_override_count
  from pg_catalog.pg_db_role_setting s
  join pg_catalog.pg_database d on d.oid=s.setdatabase
  cross join lateral pg_catalog.unnest(s.setconfig) as configured(setting)
  where d.datname=current_database()
    and s.setrole=0
    and configured.setting like 'plpgsql_check.%';

  if not v_extension_installed and v_any_setting_count=0 then
    if v_persisted_override_count<>0 then
      raise exception 'LEGACY_PLPGSQL_CHECK_UNAVAILABLE_OVERRIDE_DRIFT:count=%',
        v_persisted_override_count;
    end if;
    return;
  end if;

  if v_expected_setting_count<>pg_catalog.array_length(v_expected_names,1) then
    raise exception 'LEGACY_PLPGSQL_CHECK_EXPECTED_SETTINGS_NOT_VERIFIABLE:extension=% expected=% any=%',
      v_extension_installed,v_expected_setting_count,v_any_setting_count;
  end if;

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
