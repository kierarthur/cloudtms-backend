-- Restore the Supabase service-role RLS bypass contract on provider-neutral
-- PostgreSQL. PostgREST's service_role is deliberately not a PostgreSQL
-- superuser on Miget, so every RLS-enabled public table needs an explicit
-- policy. SQL grants remain the separate authority for which tables the role
-- may actually read or mutate. This migration changes no application rows.

\set ON_ERROR_STOP on

begin;

do $install$
declare
  v_target record;
  v_existing record;
begin
  if not exists(
    select 1
    from pg_catalog.pg_roles
    where rolname = 'service_role'
  ) then
    raise exception using
      errcode = 'ZX999',
      message = 'MIGET_SERVICE_ROLE_MISSING';
  end if;

  for v_target in
    select n.nspname, c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relkind in ('r', 'p')
      and c.relrowsecurity
    order by c.relname
  loop
    select p.cmd, p.roles, p.qual, p.with_check
      into v_existing
    from pg_catalog.pg_policies p
    where p.schemaname = v_target.nspname
      and p.tablename = v_target.relname
      and p.policyname = 'cloudtms_miget_service_owner_all';

    if found then
      if v_existing.cmd <> 'ALL'
        or not ('service_role'::name = any(v_existing.roles))
        or not (current_user::name = any(v_existing.roles))
        or v_existing.qual <> 'true'
        or v_existing.with_check <> 'true'
      then
        raise exception using
          errcode = 'ZX999',
          message = pg_catalog.format(
            'MIGET_SERVICE_ROLE_POLICY_MISMATCH:%I.%I',
            v_target.nspname,
            v_target.relname
          );
      end if;
    else
      execute pg_catalog.format(
        'create policy cloudtms_miget_service_owner_all on %I.%I for all to %I, service_role using (true) with check (true)',
        v_target.nspname,
        v_target.relname,
        current_user
      );
    end if;
  end loop;
end
$install$;

do $verify$
begin
  if exists(
    select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_policies p
      on p.schemaname = n.nspname
     and p.tablename = c.relname
     and p.policyname = 'cloudtms_miget_service_owner_all'
    where n.nspname = 'public'
      and c.relkind in ('r', 'p')
      and c.relrowsecurity
      and (
        p.policyname is null
        or p.cmd <> 'ALL'
        or not ('service_role'::name = any(p.roles))
        or not (current_user::name = any(p.roles))
        or p.qual <> 'true'
        or p.with_check <> 'true'
      )
  ) then
    raise exception using
      errcode = 'ZX999',
      message = 'MIGET_SERVICE_ROLE_POLICY_VERIFICATION_FAILED';
  end if;
end
$verify$;

commit;
