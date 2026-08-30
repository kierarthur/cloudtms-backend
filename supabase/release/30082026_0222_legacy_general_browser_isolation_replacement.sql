-- LEGACY_UPGRADE-only provider-neutral replacement for the immutable general
-- browser-isolation migration. The original migration is pinned to one exact
-- Supabase TEST ACL/RLS snapshot. This replacement accepts only the exact
-- reconstructed LIVE relation/sequence name manifest, preserves service_role
-- privileges, removes browser privileges, enables RLS, and installs the same
-- owner default-ACL boundary. Final release verifiers and the canonical contract
-- still run after all migrations and repeatables.

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $legacy_general_isolation$
declare
  v_count integer;
  v_hash text;
  v_service_hash_before text;
  v_service_hash_after text;
  v_target record;
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
    select c.oid,c.relname,c.relrowsecurity,
      has_table_privilege('service_role',c.oid,'SELECT') svc_select,
      has_table_privilege('service_role',c.oid,'INSERT') svc_insert,
      has_table_privilege('service_role',c.oid,'UPDATE') svc_update,
      has_table_privilege('service_role',c.oid,'DELETE') svc_delete,
      has_table_privilege('service_role',c.oid,'TRUNCATE') svc_truncate,
      has_table_privilege('service_role',c.oid,'REFERENCES') svc_references,
      has_table_privilege('service_role',c.oid,'TRIGGER') svc_trigger
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p')
      and c.relname not ilike '%candidate%'
      and not exists(select 1 from protected_tables p where p.name=c.relname)
  )
  select count(*)::integer,
    md5(coalesce(string_agg(relname,E'\n' order by relname),'')),
    md5(coalesce(string_agg(
      relname||'|'||svc_select::text||svc_insert::text||svc_update::text||
      svc_delete::text||svc_truncate::text||svc_references::text||svc_trigger::text,
      E'\n' order by relname),''))
  into v_count,v_hash,v_service_hash_before
  from targets;

  if v_count<>126 or v_hash<>'a4713d3e744b0e2fa6c82a317948ab69' then
    raise exception 'LEGACY_GENERAL_RELATION_NAME_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
  end if;

  for v_target in
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
    )
    select c.relname,
      has_table_privilege('service_role',c.oid,'SELECT') svc_select,
      has_table_privilege('service_role',c.oid,'INSERT') svc_insert,
      has_table_privilege('service_role',c.oid,'UPDATE') svc_update,
      has_table_privilege('service_role',c.oid,'DELETE') svc_delete,
      has_table_privilege('service_role',c.oid,'TRUNCATE') svc_truncate,
      has_table_privilege('service_role',c.oid,'REFERENCES') svc_references,
      has_table_privilege('service_role',c.oid,'TRIGGER') svc_trigger
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p')
      and c.relname not ilike '%candidate%'
      and not exists(select 1 from protected_tables p where p.name=c.relname)
    order by c.relname
  loop
    execute format('alter table %I.%I enable row level security','public',v_target.relname);
    execute format('revoke all privileges on table %I.%I from PUBLIC, anon, authenticated','public',v_target.relname);
    if v_target.svc_select then execute format('grant select on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_insert then execute format('grant insert on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_update then execute format('grant update on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_delete then execute format('grant delete on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_truncate then execute format('grant truncate on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_references then execute format('grant references on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_trigger then execute format('grant trigger on table %I.%I to service_role','public',v_target.relname); end if;
  end loop;

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
    select c.oid,c.relname,c.relrowsecurity,
      has_table_privilege('service_role',c.oid,'SELECT') svc_select,
      has_table_privilege('service_role',c.oid,'INSERT') svc_insert,
      has_table_privilege('service_role',c.oid,'UPDATE') svc_update,
      has_table_privilege('service_role',c.oid,'DELETE') svc_delete,
      has_table_privilege('service_role',c.oid,'TRUNCATE') svc_truncate,
      has_table_privilege('service_role',c.oid,'REFERENCES') svc_references,
      has_table_privilege('service_role',c.oid,'TRIGGER') svc_trigger
    from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p')
      and c.relname not ilike '%candidate%'
      and not exists(select 1 from protected_tables p where p.name=c.relname)
  )
  select md5(coalesce(string_agg(
    relname||'|'||svc_select::text||svc_insert::text||svc_update::text||
    svc_delete::text||svc_truncate::text||svc_references::text||svc_trigger::text,
    E'\n' order by relname),''))
  into v_service_hash_after from targets;

  if v_service_hash_after<>v_service_hash_before or exists(
    with protected_tables(name) as (
      values
        ('candidate_job_titles'),('candidates'),('client_settings'),('clients'),
        ('contract_weeks'),('contracts'),('mail_outbox'),('settings_defaults'),
        ('timesheet_evidence'),('timesheets'),('timesheets_financials'),
        ('candidate_app_accounts'),('candidate_app_sessions'),
        ('candidate_approval_requests'),('candidate_auth_challenges'),
        ('candidate_daily_availability_days'),('candidate_daily_command_receipts'),
        ('candidate_daily_rota_days'),('candidate_daily_rota_generations'),
        ('candidate_daily_sheet_projection_outbox'),('candidate_notifications'),
        ('candidate_submission_components'),('candidate_submission_workflows'),
        ('invoice_document_versions')
    )
    select 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p') and c.relname not ilike '%candidate%'
      and not exists(select 1 from protected_tables p where p.name=c.relname)
      and (not c.relrowsecurity
        or has_table_privilege('anon',c.oid,'SELECT') or has_table_privilege('anon',c.oid,'INSERT')
        or has_table_privilege('anon',c.oid,'UPDATE') or has_table_privilege('anon',c.oid,'DELETE')
        or has_table_privilege('anon',c.oid,'TRUNCATE') or has_table_privilege('anon',c.oid,'REFERENCES')
        or has_table_privilege('anon',c.oid,'TRIGGER')
        or has_table_privilege('authenticated',c.oid,'SELECT') or has_table_privilege('authenticated',c.oid,'INSERT')
        or has_table_privilege('authenticated',c.oid,'UPDATE') or has_table_privilege('authenticated',c.oid,'DELETE')
        or has_table_privilege('authenticated',c.oid,'TRUNCATE') or has_table_privilege('authenticated',c.oid,'REFERENCES')
        or has_table_privilege('authenticated',c.oid,'TRIGGER'))
  ) then
    raise exception 'LEGACY_GENERAL_RELATION_ISOLATION_FAILED';
  end if;

  select count(*)::integer,md5(coalesce(string_agg(c.relname,E'\n' order by c.relname),''))
  into v_count,v_hash
  from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='S' and c.relname not ilike '%candidate%';
  if v_count<>8 or v_hash<>'581acf649985f7457facbd1f9c1bda9f' then
    raise exception 'LEGACY_GENERAL_SEQUENCE_NAME_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
  end if;

  select md5(coalesce(string_agg(c.relname||'|'||
    has_sequence_privilege('service_role',c.oid,'USAGE')::text||
    has_sequence_privilege('service_role',c.oid,'SELECT')::text||
    has_sequence_privilege('service_role',c.oid,'UPDATE')::text,
    E'\n' order by c.relname),'')) into v_service_hash_before
  from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='S' and c.relname not ilike '%candidate%';

  for v_target in
    select c.relname,
      has_sequence_privilege('service_role',c.oid,'USAGE') svc_usage,
      has_sequence_privilege('service_role',c.oid,'SELECT') svc_select,
      has_sequence_privilege('service_role',c.oid,'UPDATE') svc_update
    from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='S' and c.relname not ilike '%candidate%'
    order by c.relname
  loop
    execute format('revoke all privileges on sequence %I.%I from PUBLIC, anon, authenticated','public',v_target.relname);
    if v_target.svc_usage then execute format('grant usage on sequence %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_select then execute format('grant select on sequence %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_update then execute format('grant update on sequence %I.%I to service_role','public',v_target.relname); end if;
  end loop;

  select md5(coalesce(string_agg(c.relname||'|'||
    has_sequence_privilege('service_role',c.oid,'USAGE')::text||
    has_sequence_privilege('service_role',c.oid,'SELECT')::text||
    has_sequence_privilege('service_role',c.oid,'UPDATE')::text,
    E'\n' order by c.relname),'')) into v_service_hash_after
  from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='S' and c.relname not ilike '%candidate%';
  if v_service_hash_after<>v_service_hash_before then
    raise exception 'LEGACY_GENERAL_SEQUENCE_SERVICE_ACL_CHANGED';
  end if;

  if exists(
    select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='S' and c.relname not ilike '%candidate%'
      and (
        has_sequence_privilege('anon',c.oid,'USAGE')
        or has_sequence_privilege('anon',c.oid,'SELECT')
        or has_sequence_privilege('anon',c.oid,'UPDATE')
        or has_sequence_privilege('authenticated',c.oid,'USAGE')
        or has_sequence_privilege('authenticated',c.oid,'SELECT')
        or has_sequence_privilege('authenticated',c.oid,'UPDATE')
      )
  ) then
    raise exception 'LEGACY_GENERAL_SEQUENCE_ISOLATION_FAILED';
  end if;

  alter default privileges for role current_user in schema public revoke all on tables from PUBLIC, anon, authenticated;
  alter default privileges for role current_user in schema public grant all on tables to service_role;
  alter default privileges for role current_user in schema public revoke all on sequences from PUBLIC, anon, authenticated;
  alter default privileges for role current_user in schema public grant all on sequences to service_role;
  alter default privileges for role current_user in schema public revoke all on functions from PUBLIC, anon, authenticated;
  alter default privileges for role current_user in schema public grant all on functions to service_role;

  if exists(
    select 1 from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    cross join lateral pg_catalog.aclexplode(d.defaclacl) x
    left join pg_catalog.pg_roles gr on gr.oid=x.grantee
    where n.nspname='public' and d.defaclrole=(select oid from pg_catalog.pg_roles where rolname=current_user)
      and d.defaclobjtype in('r','S','f')
      and (x.grantee=0 or gr.rolname in('anon','authenticated'))
  ) then
    raise exception 'LEGACY_GENERAL_DEFAULT_BROWSER_ACL_REMAINS';
  end if;
end
$legacy_general_isolation$;
