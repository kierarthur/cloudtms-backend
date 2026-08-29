-- Provider-neutral application-owner defaults for Miget and future PostgreSQL
-- hosts. This changes no application rows. It installs the same closed
-- browser-role posture for whichever authenticated role owns the database.

\set ON_ERROR_STOP on

begin;

-- A pg_dump/pg_restore transfer cannot reproduce source-role default ACL
-- semantics when the destination database owner has a provider-generated
-- name. Re-establish the audited CloudTMS boundary for existing objects before
-- installing defaults for future objects.
\ir ../baseline/22082026_1505_cloudtms_test_acl_baseline.sql

do $existing_function_acl$
declare
  v_signature regprocedure;
begin
  for v_signature in
    select p.oid::regprocedure
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
    order by p.oid::regprocedure::text
  loop
    execute pg_catalog.format(
      'revoke all privileges on function %s from PUBLIC, anon, authenticated',
      v_signature
    );
  end loop;
end
$existing_function_acl$;

revoke all privileges on all functions in schema private
  from public, anon, authenticated;

revoke all privileges on table public.candidate_manager_email_route_receipts
  from public, anon, authenticated, service_role;
revoke all privileges on table public.candidate_manager_email_template_versions
  from public, anon, authenticated, service_role;

revoke all privileges on function public.cloudtms_data_api_mfa_gate()
  from public, anon, authenticated, service_role;
grant execute on function public.cloudtms_data_api_mfa_gate()
  to anon, authenticated, service_role;

alter default privileges for role current_user in schema public
  revoke all on tables from public, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all on tables to service_role;

alter default privileges for role current_user in schema public
  revoke all on sequences from public, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all on sequences to service_role;

alter default privileges for role current_user in schema public
  revoke all on functions from public, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all on functions to service_role;

do $verification$
begin
  if exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and (
        pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
        or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
      )
  ) then
    raise exception using errcode='ZX999',message='PROVIDER_OWNER_PUBLIC_SECURITY_DEFINER_EXPOSURE';
  end if;

  if exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private'
      and (
        pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
        or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
      )
  ) then
    raise exception using errcode='ZX999',message='PROVIDER_OWNER_PRIVATE_FUNCTION_EXPOSURE';
  end if;

  if exists(
    select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    cross join lateral pg_catalog.aclexplode(
      coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))
    ) acl
    where n.nspname='public'
      and c.relname in (
        'candidate_manager_email_route_receipts',
        'candidate_manager_email_template_versions'
      )
      and acl.privilege_type in ('SELECT','INSERT','UPDATE','DELETE')
      and (acl.grantee=0 or acl.grantee in (
        select oid from pg_catalog.pg_roles
        where rolname in ('anon','authenticated','service_role')
      ))
  ) then
    raise exception using errcode='ZX999',message='PROVIDER_OWNER_MANAGER_EMAIL_TABLE_EXPOSURE';
  end if;

  if not pg_catalog.has_function_privilege(
    'anon','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) or not pg_catalog.has_function_privilege(
    'authenticated','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) then
    raise exception using errcode='ZX999',message='PROVIDER_OWNER_MFA_GATE_UNAVAILABLE';
  end if;

  if 3<>(
    select count(*)
    from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    where n.nspname='public'
      and pg_catalog.pg_get_userbyid(d.defaclrole)=current_user
      and d.defaclobjtype in ('r','S','f')
  ) then
    raise exception using errcode='ZX999',message='PROVIDER_OWNER_DEFAULT_ACL_MISSING';
  end if;

  if exists(
    select 1
    from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    cross join lateral pg_catalog.aclexplode(d.defaclacl) acl
    left join pg_catalog.pg_roles grantee on grantee.oid=acl.grantee
    where n.nspname='public'
      and pg_catalog.pg_get_userbyid(d.defaclrole)=current_user
      and d.defaclobjtype in ('r','S','f')
      and (acl.grantee=0 or grantee.rolname in ('anon','authenticated'))
  ) then
    raise exception using errcode='ZX999',message='PROVIDER_OWNER_BROWSER_DEFAULT_ACL_PRESENT';
  end if;

  if 3<>(
    select count(distinct d.defaclobjtype)
    from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    cross join lateral pg_catalog.aclexplode(d.defaclacl) acl
    join pg_catalog.pg_roles grantee on grantee.oid=acl.grantee
    where n.nspname='public'
      and pg_catalog.pg_get_userbyid(d.defaclrole)=current_user
      and d.defaclobjtype in ('r','S','f')
      and grantee.rolname='service_role'
  ) then
    raise exception using errcode='ZX999',message='PROVIDER_OWNER_SERVICE_DEFAULT_ACL_MISSING';
  end if;
end
$verification$;

commit;
