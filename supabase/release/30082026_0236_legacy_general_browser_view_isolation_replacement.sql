-- LEGACY_UPGRADE-only provider-neutral replacement for the immutable general
-- browser view-isolation migration. The original post-state fingerprint includes
-- provider ACL history. This replacement accepts only the exact audited view-name
-- manifest, makes every target a security-invoker view, grants service SELECT,
-- and removes all browser privileges without changing a view definition.

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $legacy_general_view_isolation$
declare
  v_count integer;
  v_hash text;
  v_target record;
begin
  select
    count(*)::integer,
    md5(coalesce(string_agg(c.relname,E'\n' order by c.relname),''))
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

  if v_count<>20 or v_hash<>'2140eb627004e18241adb94c085b57bf' then
    raise exception 'LEGACY_GENERAL_VIEW_NAME_MANIFEST_DRIFT:count=% hash=%',
      v_count,v_hash;
  end if;

  for v_target in
    select c.relname
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
      )
    order by c.relname
  loop
    execute format(
      'alter view %I.%I set (security_invoker=true)',
      'public',v_target.relname
    );
    execute format(
      'revoke all privileges on table %I.%I from PUBLIC, anon, authenticated',
      'public',v_target.relname
    );
    execute format(
      'grant select on table %I.%I to service_role',
      'public',v_target.relname
    );
  end loop;

  select
    count(*)::integer,
    md5(coalesce(string_agg(
      c.relname||'|'||
      ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[])))::text||'|'||
      has_table_privilege('service_role',c.oid,'SELECT')::text||'|'||
      has_table_privilege('anon',c.oid,'SELECT')::text||'|'||
      has_table_privilege('authenticated',c.oid,'SELECT')::text,
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
    raise exception 'LEGACY_GENERAL_VIEW_ISOLATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;
end
$legacy_general_view_isolation$;
