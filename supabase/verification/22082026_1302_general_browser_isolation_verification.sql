-- Verifies the installed non-Candidate/MyTMS browser-isolation phase without
-- reading or mutating application rows.

do $general_browser_isolation_verification$
declare
  v_count integer;
  v_hash text;
  v_service_missing integer;
  v_browser_executable integer;
begin
  with protected_tables(name) as (
    values
      ('candidate_job_titles'),('candidates'),('client_settings'),('clients'),
      ('contract_weeks'),('contracts'),('mail_outbox'),('settings_defaults'),
      ('timesheet_evidence'),('timesheets'),('timesheets_financials'),
      ('candidate_app_accounts'),('candidate_app_sessions'),
      ('candidate_approval_requests'),('candidate_auth_challenges'),
      ('candidate_daily_availability_days'),
      ('candidate_daily_command_receipts'),('candidate_daily_rota_days'),
      ('candidate_daily_rota_generations'),
      ('candidate_daily_sheet_projection_outbox'),('candidate_notifications'),
      ('candidate_submission_components'),('candidate_submission_workflows'),
      ('invoice_document_versions')
  ), targets as (
    select
      c.relname,
      c.relrowsecurity,
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_table_privilege('service_role',c.oid,'INSERT') as svc_insert,
      pg_catalog.has_table_privilege('service_role',c.oid,'UPDATE') as svc_update,
      pg_catalog.has_table_privilege('service_role',c.oid,'DELETE') as svc_delete,
      pg_catalog.has_table_privilege('service_role',c.oid,'TRUNCATE') as svc_truncate,
      pg_catalog.has_table_privilege('service_role',c.oid,'REFERENCES') as svc_references,
      pg_catalog.has_table_privilege('service_role',c.oid,'TRIGGER') as svc_trigger,
      pg_catalog.has_table_privilege('anon',c.oid,'SELECT,INSERT,UPDATE,DELETE') as anon_access,
      pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT,INSERT,UPDATE,DELETE') as auth_access
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public'
      and c.relkind in ('r','p')
      and c.relname not ilike '%candidate%'
      and not exists (select 1 from protected_tables p where p.name=c.relname)
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      relname||'|'||relrowsecurity::text||'|'||
      svc_select::text||svc_insert::text||svc_update::text||svc_delete::text||
      svc_truncate::text||svc_references::text||svc_trigger::text||'|'||
      anon_access::text||'|'||auth_access::text,
      E'\n' order by relname
    ),''))
  into v_count,v_hash
  from targets;

  if v_count<>126 or v_hash<>'d916546f6203aa1baee7fde8d2c08885' then
    raise exception 'GENERAL_RELATION_ISOLATION_VERIFICATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;

  with targets as (
    select
      c.relname,
      ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[]))) as invoker,
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_table_privilege('anon',c.oid,'SELECT') as anon_select,
      pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT') as auth_select
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
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      relname||'|'||invoker::text||'|'||svc_select::text||'|'||
      anon_select::text||'|'||auth_select::text,
      E'\n' order by relname
    ),''))
  into v_count,v_hash
  from targets;

  if v_count<>20 or v_hash<>'f7b3b9ccf07dd052c65b98932af9a76c' then
    raise exception 'GENERAL_VIEW_ISOLATION_VERIFICATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;

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
    raise exception 'GENERAL_RPC_ISOLATION_VERIFICATION_FAILED:count=% service_missing=% browser_executable=% hash=%',
      v_count,v_service_missing,v_browser_executable,v_hash;
  end if;

  with targets as (
    select
      c.relname,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'USAGE') as svc_usage,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'UPDATE') as svc_update,
      pg_catalog.has_sequence_privilege('anon',c.oid,'USAGE,SELECT,UPDATE') as anon_access,
      pg_catalog.has_sequence_privilege('authenticated',c.oid,'USAGE,SELECT,UPDATE') as auth_access
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='S'
      and c.relname not ilike '%candidate%'
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      relname||'|'||svc_usage::text||svc_select::text||svc_update::text||'|'||
      anon_access::text||'|'||auth_access::text,
      E'\n' order by relname
    ),''))
  into v_count,v_hash
  from targets;

  if v_count<>8 or v_hash<>'27bb33e5336a5c596eaa1b210a14b3dc' then
    raise exception 'GENERAL_SEQUENCE_ISOLATION_VERIFICATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;

  with default_entries as (
    select
      r.rolname as owner_name,
      d.defaclobjtype::text as object_type,
      case when x.grantee=0 then 'PUBLIC' else gr.rolname end as grantee,
      x.privilege_type,
      x.is_grantable
    from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    join pg_catalog.pg_roles r on r.oid=d.defaclrole
    cross join lateral pg_catalog.aclexplode(d.defaclacl) x
    left join pg_catalog.pg_roles gr on gr.oid=x.grantee
    where n.nspname='public'
      and d.defaclobjtype in ('r','f','S')
      and r.rolname='postgres'
      and (x.grantee=0 or gr.rolname in ('anon','authenticated','service_role'))
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      owner_name||'|'||object_type||'|'||grantee||'|'||privilege_type||'|'||is_grantable::text,
      E'\n' order by owner_name,object_type,grantee,privilege_type,is_grantable
    ),''))
  into v_count,v_hash
  from default_entries;

  if v_count<>12 or v_hash<>'ed611d0ad2b3902eea77d3215b18374e' then
    raise exception 'GENERAL_DEFAULT_ACL_VERIFICATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;

  if not pg_catalog.has_function_privilege(
    'anon','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) or not pg_catalog.has_function_privilege(
    'authenticated','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) then
    raise exception 'GENERAL_MFA_PRE_REQUEST_EXECUTE_CONTRACT_CHANGED';
  end if;
end
$general_browser_isolation_verification$;
