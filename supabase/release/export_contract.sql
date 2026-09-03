\set ON_ERROR_STOP on
\pset tuples_only on
\pset format unaligned

-- Canonical schema/security contract. It deliberately contains no table rows,
-- sequence values, credentials, Vault values, or other business data.
-- pg_get_constraintdef() shortens referenced relation names according to the
-- caller's search_path.  Pin one provider-neutral path before catalog reads so
-- the same physical foreign key exports identically from a clean database and
-- a managed database whose login role has a different default search_path.
set search_path = pg_catalog, public;

with
app_namespaces as (
  select oid, nspname, nspowner, nspacl
  from pg_catalog.pg_namespace
  where nspname in ('public', 'private')
),
contract_role_names as (
  select oid, case when rolname=current_user then 'postgres' else rolname end as logical_name
  from pg_catalog.pg_roles
),
extensions_contract as (
  select jsonb_agg(
    jsonb_build_object('name', e.extname, 'schema', n.nspname)
    order by e.extname collate "C"
  ) as value
  from pg_catalog.pg_extension e
  join pg_catalog.pg_namespace n on n.oid = e.extnamespace
  where e.extname in ('btree_gist', 'pgcrypto', 'pg_trgm', 'uuid-ossp')
),
enum_contract as (
  select jsonb_agg(
    jsonb_build_object(
      'schema', n.nspname,
      'name', t.typname,
      'labels', (
        select jsonb_agg(e.enumlabel order by e.enumsortorder)
        from pg_catalog.pg_enum e
        where e.enumtypid = t.oid
      )
    ) order by n.nspname collate "C", t.typname collate "C"
  ) as value
  from pg_catalog.pg_type t
  join app_namespaces n on n.oid = t.typnamespace
  where t.typtype = 'e'
),
relation_contract as (
  select jsonb_agg(
    jsonb_build_object(
      'schema', n.nspname,
      'name', c.relname,
      'kind', c.relkind,
      'owner', (select logical_name from contract_role_names where oid=c.relowner),
      'rls', c.relrowsecurity,
      'force_rls', c.relforcerowsecurity,
      'options', coalesce(to_jsonb(c.reloptions), '[]'::jsonb),
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', expanded_acl.grantee,
            'privilege', expanded_acl.privilege,
            'grantable', expanded_acl.grantable
          ) order by expanded_acl.grantee collate "C",
                     expanded_acl.privilege collate "C", expanded_acl.grantable
        )
        from (
          select distinct
            case when a.grantee = 0 then 'PUBLIC'
              else (select logical_name from contract_role_names where oid=a.grantee) end as grantee,
            a.privilege_type as privilege,
            a.is_grantable as grantable
          from pg_catalog.aclexplode(coalesce(
            c.relacl,
            pg_catalog.acldefault(
              case when c.relkind = 'S' then 'S'::"char" else 'r'::"char" end,
              c.relowner
            )
          )) a
        ) expanded_acl
      ), '[]'::jsonb),
      'columns', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'name', a.attname,
            'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
            'not_null', a.attnotnull,
            'identity', a.attidentity,
            'generated', a.attgenerated,
            'default', pg_catalog.pg_get_expr(d.adbin, d.adrelid)
          ) order by a.attname collate "C"
        )
        from pg_catalog.pg_attribute a
        left join pg_catalog.pg_attrdef d
          on d.adrelid = a.attrelid and d.adnum = a.attnum
        where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
      ), '[]'::jsonb),
      'constraints', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'name', con.conname,
            'type', con.contype,
            'definition', pg_catalog.pg_get_constraintdef(con.oid, true),
            'deferrable', con.condeferrable,
            'deferred', con.condeferred,
            'validated', con.convalidated
          ) order by con.conname collate "C"
        )
        from pg_catalog.pg_constraint con
        where con.conrelid = c.oid
          -- PostgreSQL 18 also represents an ordinary column NOT NULL
          -- specification as a relation pg_constraint row (contype = 'n').
          -- The portable contract already seals that enforceable authority in
          -- columns[].not_null; it deliberately does not treat the generated
          -- constraint name as separate application authority.  Exclude only
          -- the exact ordinary duplicate shape.  Any future or malformed
          -- NOT NULL constraint metadata remains visible and therefore causes
          -- contract drift rather than being silently normalised away.
          and not (
            con.contype = 'n'
            and con.contypid = 0::oid
            and con.conindid = 0::oid
            and con.conparentid = 0::oid
            and con.confrelid = 0::oid
            and not con.condeferrable
            and not con.condeferred
            and con.convalidated
            and con.conislocal
            and con.coninhcount = 0
            and not con.connoinherit
            and con.conkey is not null
            and pg_catalog.cardinality(con.conkey) = 1
            and con.confkey is null
            and con.conpfeqop is null
            and con.conppeqop is null
            and con.conffeqop is null
            and con.conexclop is null
            and con.conbin is null
            and exists (
              select 1
              from pg_catalog.pg_attribute not_null_column
              where not_null_column.attrelid = con.conrelid
                and not_null_column.attnum = con.conkey[1]
                and not_null_column.attnum > 0
                and not not_null_column.attisdropped
                and not_null_column.attnotnull
            )
          )
      ), '[]'::jsonb),
      'indexes', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'name', i.relname,
            'definition', pg_catalog.pg_get_indexdef(i.oid)
          ) order by i.relname collate "C"
        )
        from pg_catalog.pg_index ix
        join pg_catalog.pg_class i on i.oid = ix.indexrelid
        where ix.indrelid = c.oid
      ), '[]'::jsonb),
      'view_definition_sha256', case when c.relkind = 'v' then
        pg_catalog.encode(
          extensions.digest(
            pg_catalog.convert_to(pg_catalog.pg_get_viewdef(c.oid, true), 'UTF8'),
            'sha256'
          ), 'hex'
        )
      else null end
    ) order by n.nspname collate "C", c.relkind, c.relname collate "C"
  ) as value
  from pg_catalog.pg_class c
  join app_namespaces n on n.oid = c.relnamespace
  where c.relkind in ('r', 'p', 'v', 'S')
    and not (
      n.nspname = 'private'
      and c.relname in (
        'cloudtms_database_identity',
        'cloudtms_database_releases',
        'cloudtms_migration_ledger',
        'cloudtms_repeatable_ledger'
      )
    )
),
routine_contract as (
  select jsonb_agg(
    jsonb_build_object(
      'schema', n.nspname,
      'identity', p.proname || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')',
      'owner', (select logical_name from contract_role_names where oid=p.proowner),
      'security_definer', p.prosecdef,
      'volatility', p.provolatile,
      'parallel', p.proparallel,
      -- plpgsql_check is development instrumentation and is not installed by
      -- every PostgreSQL provider. Its routine-local settings are excluded
      -- from the provider-neutral application contract; search_path and every
      -- other routine configuration remain authoritative.
      'config', coalesce((
        select jsonb_agg(config_value order by config_value collate "C")
        from pg_catalog.unnest(coalesce(p.proconfig, array[]::text[])) config_value
        where config_value !~ '^plpgsql_check[.]'
      ), '[]'::jsonb),
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', expanded_acl.grantee,
            'privilege', expanded_acl.privilege,
            'grantable', expanded_acl.grantable
          ) order by expanded_acl.grantee collate "C",
                     expanded_acl.privilege collate "C", expanded_acl.grantable
        )
        from (
          select distinct
            case when a.grantee = 0 then 'PUBLIC'
              else (select logical_name from contract_role_names where oid=a.grantee) end as grantee,
            a.privilege_type as privilege,
            a.is_grantable as grantable
          from pg_catalog.aclexplode(coalesce(
            p.proacl,
            pg_catalog.acldefault('f'::"char", p.proowner)
          )) a
        ) expanded_acl
        where not (
          p.proname = 'cloudtms_data_api_mfa_gate'
          and expanded_acl.grantee = 'authenticator'
        )
      ), '[]'::jsonb),
      'definition_sha256', pg_catalog.encode(
        extensions.digest(
          pg_catalog.convert_to(
            pg_catalog.replace(
              pg_catalog.regexp_replace(
                pg_catalog.pg_get_functiondef(p.oid),
                E'\n SET "plpgsql_check\\.[^"]+" TO ''[^'']*''',
                '',
                'g'
              ),
              E'\r\n',
              E'\n'
            ),
            'UTF8'
          ),
          'sha256'
        ), 'hex'
      )
    ) order by n.nspname collate "C", p.proname collate "C",
               pg_catalog.pg_get_function_identity_arguments(p.oid) collate "C"
  ) as value
  from pg_catalog.pg_proc p
  join app_namespaces n on n.oid = p.pronamespace
  where not exists (
    select 1
    from pg_catalog.pg_depend d
    where d.classid = 'pg_proc'::regclass
      and d.objid = p.oid
      and d.deptype = 'e'
  )
),
trigger_contract as (
  select jsonb_agg(
    jsonb_build_object(
      'schema', n.nspname,
      'table', c.relname,
      'name', t.tgname,
      'definition', pg_catalog.pg_get_triggerdef(t.oid, true),
      'enabled', t.tgenabled
    ) order by n.nspname collate "C", c.relname collate "C", t.tgname collate "C"
  ) as value
  from pg_catalog.pg_trigger t
  join pg_catalog.pg_class c on c.oid = t.tgrelid
  join app_namespaces n on n.oid = c.relnamespace
  where not t.tgisinternal
),
policy_contract as (
  select jsonb_agg(
    jsonb_build_object(
      'schema', schemaname,
      'table', tablename,
      'name', policyname,
      'permissive', permissive,
      'roles', coalesce((
        select jsonb_agg(
          case when role_name::text=current_user then 'postgres' else role_name::text end
          order by (case when role_name::text=current_user then 'postgres' else role_name::text end) collate "C"
        )
        from pg_catalog.unnest(roles) with ordinality as policy_role(role_name,ordinality)
      ), '[]'::jsonb),
      'command', cmd,
      'using', qual,
      'check', with_check
    ) order by schemaname collate "C", tablename collate "C", policyname collate "C"
  ) as value
  from pg_catalog.pg_policies
  where schemaname in ('public', 'private')
),
schema_contract as (
  select jsonb_agg(
    jsonb_build_object(
      'schema', n.nspname,
      'owner', (select logical_name from contract_role_names where oid=n.nspowner),
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', expanded_acl.grantee,
            'privilege', expanded_acl.privilege,
            'grantable', expanded_acl.grantable
          ) order by expanded_acl.grantee collate "C",
                     expanded_acl.privilege collate "C", expanded_acl.grantable
        )
        from (
          select distinct
            case when a.grantee = 0 then 'PUBLIC'
              else (select logical_name from contract_role_names where oid=a.grantee) end as grantee,
            a.privilege_type as privilege,
            a.is_grantable as grantable
          from pg_catalog.aclexplode(coalesce(
            n.nspacl,
            pg_catalog.acldefault('n'::"char", n.nspowner)
          )) a
        ) expanded_acl
      ), '[]'::jsonb)
    ) order by n.nspname collate "C"
  ) as value
  from app_namespaces n
),
default_acl_rows as (
  select
    owner_role.logical_name as owner_name,
    n.nspname as schema_name,
    d.defaclobjtype as object_kind,
    jsonb_build_object(
      'owner', owner_role.logical_name,
      'schema', n.nspname,
      'kind', d.defaclobjtype,
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', expanded_acl.grantee,
            'privilege', expanded_acl.privilege,
            'grantable', expanded_acl.grantable
          ) order by expanded_acl.grantee collate "C",
                     expanded_acl.privilege collate "C", expanded_acl.grantable
        )
        from (
          select distinct
            case when a.grantee = 0 then 'PUBLIC'
              else (select logical_name from contract_role_names where oid=a.grantee) end as grantee,
            a.privilege_type as privilege,
            a.is_grantable as grantable
          from pg_catalog.aclexplode(coalesce(
            d.defaclacl,
            pg_catalog.acldefault(d.defaclobjtype, d.defaclrole)
          )) a
        ) expanded_acl
      ), '[]'::jsonb)
    ) as contract_row
  from pg_catalog.pg_default_acl d
  join app_namespaces n on n.oid = d.defaclnamespace
  join contract_role_names owner_role on owner_role.oid=d.defaclrole
  -- Default privileges are installation authority only for the exact role
  -- running this release.  A provider image can also contain a physical role
  -- literally named `postgres`; when CURRENT_USER is the generated service
  -- owner, importing that bootstrap role's defaults would merge two different
  -- security postures under one logical owner.
  where d.defaclrole = (
    select role_row.oid
    from pg_catalog.pg_roles role_row
    where role_row.rolname = current_user
  )
),
default_acl_contract as (
  -- Collapse byte-identical rows defensively while preserving every distinct
  -- default privilege owned by the exact installation role above.
  select jsonb_agg(
    contract_row order by owner_name collate "C",
                          schema_name collate "C", object_kind
  ) as value
  from (
    select distinct owner_name, schema_name, object_kind, contract_row
    from default_acl_rows
  ) canonical_default_acl_rows
)
select jsonb_build_object(
  'contract_version', 1,
  'extensions', coalesce((select value from extensions_contract), '[]'::jsonb),
  'schemas', coalesce((select value from schema_contract), '[]'::jsonb),
  'enums', coalesce((select value from enum_contract), '[]'::jsonb),
  'relations', coalesce((select value from relation_contract), '[]'::jsonb),
  'routines', coalesce((select value from routine_contract), '[]'::jsonb),
  'triggers', coalesce((select value from trigger_contract), '[]'::jsonb),
  'policies', coalesce((select value from policy_contract), '[]'::jsonb),
  'default_acls', coalesce((select value from default_acl_contract), '[]'::jsonb)
)::text;
