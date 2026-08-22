-- TEST-first CloudTMS browser isolation for non-Candidate/MyTMS application views.
-- View definitions and projected columns are not changed.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $view_isolation$
declare
  v_count integer;
  v_hash text;
  v_apply_views boolean := false;
  v_target record;
begin
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      c.relname||'|'||
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT')::text,
      E'\n' order by c.relname
    ),''))
  into v_count,v_hash
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v'
    and not ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[])))
    and (
      pg_catalog.has_table_privilege('anon',c.oid,'SELECT')
      or pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT')
    )
    and c.relname not ilike '%candidate%';

  if v_count=20 and v_hash='757bde8ef24111608d17b480741ee041' then
    v_apply_views := true;
  elsif v_count=0 then
    select
      pg_catalog.count(*),
      pg_catalog.md5(coalesce(pg_catalog.string_agg(
        c.relname||'|'||
        ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[])))::text||'|'||
        pg_catalog.has_table_privilege('service_role',c.oid,'SELECT')::text||'|'||
        pg_catalog.has_table_privilege('anon',c.oid,'SELECT')::text||'|'||
        pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT')::text,
        E'\n' order by c.relname
      ),''))
    into v_count,v_hash
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='v'
      and c.relname in (
        'timesheets_hr_view','v_contract_weeks_enriched','v_finance_cases_register',
        'v_legacy_contract_rate_lines_flat','v_mailshot_resolution_graph',
        'v_mailshot_src_client','v_mailshot_src_contract','v_mailshot_src_invoice',
        'v_mailshot_src_system','v_mailshot_src_timesheet','v_mailshot_src_umbrella',
        'v_outbox_unified','v_rates_client_defaults_enabled',
        'v_timesheets_daily_match','v_timesheets_details','v_timesheets_funnel',
        'v_timesheets_summary','v_timesheets_summary_base','v_ts_invoice_precheck',
        'vw_picker_clients'
      );

    if v_count<>20 or v_hash<>'f7b3b9ccf07dd052c65b98932af9a76c' then
      raise exception 'GENERAL_VIEW_POST_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
    end if;
  else
    raise exception 'GENERAL_VIEW_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
  end if;

  if v_apply_views then
    for v_target in
    select c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='v'
      and not ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[])))
      and (
        pg_catalog.has_table_privilege('anon',c.oid,'SELECT')
        or pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT')
      )
      and c.relname not ilike '%candidate%'
    order by c.relname
    loop
      execute pg_catalog.format(
        'alter view %I.%I set (security_invoker=true)','public',v_target.relname
      );
      execute pg_catalog.format(
        'revoke all privileges on table %I.%I from PUBLIC, anon, authenticated',
        'public',v_target.relname
      );
      execute pg_catalog.format(
        'grant select on table %I.%I to service_role','public',v_target.relname
      );
    end loop;
  end if;
end
$view_isolation$;
