-- Reconcile the seven private Candidate Daily service-owner RLS policies that
-- already exist on the managed Miget TEST database with reproducible NEW
-- database source authority. This changes no application rows and grants no
-- table privilege: SQL grants remain the independent access boundary.

\set ON_ERROR_STOP on

begin;

do $install$
declare
  v_table text;
  v_existing record;
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'service_role'
  ) then
    raise exception using
      errcode = 'ZX999',
      message = 'MIGET_SERVICE_ROLE_MISSING';
  end if;

  foreach v_table in array array[
    'candidate_daily_authority_scopes',
    'candidate_daily_authority_transitions',
    'candidate_daily_batch_receipts',
    'candidate_daily_entitlements',
    'candidate_daily_external_effect_receipts',
    'candidate_daily_source_links',
    'candidate_daily_sync_state'
  ]::text[]
  loop
    if not exists (
      select 1
      from pg_catalog.pg_class c
      join pg_catalog.pg_namespace n on n.oid = c.relnamespace
      where n.nspname = 'private'
        and c.relname = v_table
        and c.relkind in ('r', 'p')
        and c.relrowsecurity
    ) then
      raise exception using
        errcode = 'ZX999',
        message = pg_catalog.format(
          'PRIVATE_DAILY_RLS_TABLE_MISSING_OR_DISABLED:%I.%I',
          'private',
          v_table
        );
    end if;

    select p.cmd, p.roles, p.qual, p.with_check
      into v_existing
    from pg_catalog.pg_policies p
    where p.schemaname = 'private'
      and p.tablename = v_table
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
            'PRIVATE_DAILY_SERVICE_POLICY_MISMATCH:%I.%I',
            'private',
            v_table
          );
      end if;
    else
      execute pg_catalog.format(
        'create policy cloudtms_miget_service_owner_all on private.%I for all to %I, service_role using (true) with check (true)',
        v_table,
        current_user
      );
    end if;
  end loop;
end
$install$;

do $verify$
declare
  v_table text;
begin
  foreach v_table in array array[
    'candidate_daily_authority_scopes',
    'candidate_daily_authority_transitions',
    'candidate_daily_batch_receipts',
    'candidate_daily_entitlements',
    'candidate_daily_external_effect_receipts',
    'candidate_daily_source_links',
    'candidate_daily_sync_state'
  ]::text[]
  loop
    if not exists (
      select 1
      from pg_catalog.pg_policies p
      where p.schemaname = 'private'
        and p.tablename = v_table
        and p.policyname = 'cloudtms_miget_service_owner_all'
        and p.cmd = 'ALL'
        and 'service_role'::name = any(p.roles)
        and current_user::name = any(p.roles)
        and p.qual = 'true'
        and p.with_check = 'true'
    ) then
      raise exception using
        errcode = 'ZX999',
        message = pg_catalog.format(
          'PRIVATE_DAILY_SERVICE_POLICY_VERIFICATION_FAILED:%I.%I',
          'private',
          v_table
        );
    end if;
  end loop;
end
$verify$;

commit;
