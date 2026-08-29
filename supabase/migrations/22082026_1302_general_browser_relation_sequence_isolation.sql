-- TEST-first CloudTMS browser isolation: non-Candidate/MyTMS tables, sequences,
-- and future default privileges.
--
-- The Candidate/MyTMS boundary installed by
-- 22082026_0951_candidate_mytms_browser_isolation.sql is deliberately excluded.
-- This migration changes no rows, policies, functions, views, financial logic,
-- Banking Pay semantics, or Policy X authority.
--
-- Only the application migration owner (`postgres`) is changed here. Managed
-- Supabase-owned defaults belong to `supabase_admin` and cannot be altered by the
-- project migration role; they remain a separately monitored platform boundary.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $relation_isolation$
declare
  v_count integer;
  v_rls_off integer;
  v_hash text;
  v_default_count integer;
  v_default_hash text;
  v_apply_relations boolean := false;
  v_apply_sequences boolean := false;
  v_apply_defaults boolean := false;
  v_target record;
  v_owner record;
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
      pg_catalog.has_table_privilege('service_role',c.oid,'TRIGGER') as svc_trigger
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public'
      and c.relkind in ('r','p')
      and c.relname not ilike '%candidate%'
      and not exists (select 1 from protected_tables p where p.name=c.relname)
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not relrowsecurity),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      relname||'|'||relrowsecurity::text||'|'||
      svc_select::text||svc_insert::text||svc_update::text||svc_delete::text||
      svc_truncate::text||svc_references::text||svc_trigger::text,
      E'\n' order by relname
    ),''))
  into v_count,v_rls_off,v_hash
  from targets;

  if v_count=126 and v_rls_off=72
     and v_hash='9d03056e7bf7a74405ade6a587650c29' then
    v_apply_relations := true;
  elsif v_count=126 and v_rls_off=0
     and v_hash='c89e90608b90bcb7e03cff499e9bc869' then
    v_apply_relations := false;
  else
    raise exception 'GENERAL_RELATION_MANIFEST_DRIFT:count=% rls_off=% hash=%',
      v_count,v_rls_off,v_hash;
  end if;

  if v_apply_relations then
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
    select
      c.relname,
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_table_privilege('service_role',c.oid,'INSERT') as svc_insert,
      pg_catalog.has_table_privilege('service_role',c.oid,'UPDATE') as svc_update,
      pg_catalog.has_table_privilege('service_role',c.oid,'DELETE') as svc_delete,
      pg_catalog.has_table_privilege('service_role',c.oid,'TRUNCATE') as svc_truncate,
      pg_catalog.has_table_privilege('service_role',c.oid,'REFERENCES') as svc_references,
      pg_catalog.has_table_privilege('service_role',c.oid,'TRIGGER') as svc_trigger
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public'
      and c.relkind in ('r','p')
      and c.relname not ilike '%candidate%'
      and not exists (select 1 from protected_tables p where p.name=c.relname)
    order by c.relname
    loop
      execute pg_catalog.format(
        'alter table %I.%I enable row level security','public',v_target.relname
      );
      execute pg_catalog.format(
        'revoke all privileges on table %I.%I from PUBLIC, anon, authenticated',
        'public',v_target.relname
      );

      if v_target.svc_select then
        execute pg_catalog.format('grant select on table %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_insert then
        execute pg_catalog.format('grant insert on table %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_update then
        execute pg_catalog.format('grant update on table %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_delete then
        execute pg_catalog.format('grant delete on table %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_truncate then
        execute pg_catalog.format('grant truncate on table %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_references then
        execute pg_catalog.format('grant references on table %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_trigger then
        execute pg_catalog.format('grant trigger on table %I.%I to service_role','public',v_target.relname);
      end if;
    end loop;
  end if;

  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      c.relname||'|'||
      pg_catalog.has_sequence_privilege('service_role',c.oid,'USAGE')::text||
      pg_catalog.has_sequence_privilege('service_role',c.oid,'SELECT')::text||
      pg_catalog.has_sequence_privilege('service_role',c.oid,'UPDATE')::text,
      E'\n' order by c.relname
    ),''))
  into v_count,v_hash
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='S'
    and c.relname not ilike '%candidate%';

  if v_count=8 and v_hash='18d216f7cdbe6f1086e98cfc8fe30347' then
    v_apply_sequences := true;
  elsif v_count=8 and v_hash='27bb33e5336a5c596eaa1b210a14b3dc' then
    v_apply_sequences := false;
  else
    raise exception 'GENERAL_SEQUENCE_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
  end if;

  if v_apply_sequences then
    for v_target in
    select
      c.relname,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'USAGE') as svc_usage,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'UPDATE') as svc_update
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='S'
      and c.relname not ilike '%candidate%'
    order by c.relname
    loop
      execute pg_catalog.format(
        'revoke all privileges on sequence %I.%I from PUBLIC, anon, authenticated',
        'public',v_target.relname
      );
      if v_target.svc_usage then
        execute pg_catalog.format('grant usage on sequence %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_select then
        execute pg_catalog.format('grant select on sequence %I.%I to service_role','public',v_target.relname);
      end if;
      if v_target.svc_update then
        execute pg_catalog.format('grant update on sequence %I.%I to service_role','public',v_target.relname);
      end if;
    end loop;
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
      and r.rolname='postgres'
      and d.defaclobjtype in ('r','f','S')
      and (x.grantee=0 or gr.rolname in ('anon','authenticated','service_role'))
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      owner_name||'|'||object_type||'|'||grantee||'|'||privilege_type||'|'||is_grantable::text,
      E'\n' order by owner_name,object_type,grantee,privilege_type,is_grantable
    ),''))
  into v_default_count,v_default_hash
  from default_entries;

  if v_default_count=36
     and v_default_hash='be6a509853c8ddf2f9ab1b7cecde8565' then
    v_apply_defaults := true;
  elsif v_default_count=12
     and v_default_hash='ed611d0ad2b3902eea77d3215b18374e' then
    v_apply_defaults := false;
  else
    raise exception 'GENERAL_DEFAULT_ACL_MANIFEST_DRIFT:count=% hash=%',
      v_default_count,v_default_hash;
  end if;

  if v_apply_defaults then
    for v_owner in
    select r.rolname
    from pg_catalog.pg_roles r
    where r.rolname='postgres'
    order by r.rolname
    loop
      execute pg_catalog.format(
        'alter default privileges for role %I in schema public revoke all on tables from PUBLIC, anon, authenticated',
        v_owner.rolname
      );
      execute pg_catalog.format(
        'alter default privileges for role %I in schema public grant all on tables to service_role',
        v_owner.rolname
      );
      execute pg_catalog.format(
        'alter default privileges for role %I in schema public revoke all on sequences from PUBLIC, anon, authenticated',
        v_owner.rolname
      );
      execute pg_catalog.format(
        'alter default privileges for role %I in schema public grant all on sequences to service_role',
        v_owner.rolname
      );
      execute pg_catalog.format(
        'alter default privileges for role %I in schema public revoke all on functions from PUBLIC, anon, authenticated',
        v_owner.rolname
      );
      execute pg_catalog.format(
        'alter default privileges for role %I in schema public grant all on functions to service_role',
        v_owner.rolname
      );
    end loop;
  end if;
end
$relation_isolation$;
