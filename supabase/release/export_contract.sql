\set ON_ERROR_STOP on
\pset tuples_only on
\pset format unaligned

-- Canonical schema/security contract. It deliberately contains no table rows,
-- sequence values, credentials, Vault values, or other business data.
with
app_namespaces as (
  select oid, nspname, nspowner, nspacl
  from pg_catalog.pg_namespace
  where nspname in ('public', 'private')
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
      'owner', pg_catalog.pg_get_userbyid(c.relowner),
      'rls', c.relrowsecurity,
      'force_rls', c.relforcerowsecurity,
      'options', coalesce(to_jsonb(c.reloptions), '[]'::jsonb),
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end,
            'privilege', a.privilege_type,
            'grantable', a.is_grantable
          ) order by (case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end) collate "C",
                     a.privilege_type collate "C", a.is_grantable
        )
        from pg_catalog.aclexplode(coalesce(c.relacl, '{}'::aclitem[])) a
      ), '[]'::jsonb),
      'columns', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'position', a.attnum,
            'name', a.attname,
            'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
            'not_null', a.attnotnull,
            'identity', a.attidentity,
            'generated', a.attgenerated,
            'default', pg_catalog.pg_get_expr(d.adbin, d.adrelid)
          ) order by a.attnum
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
      'owner', pg_catalog.pg_get_userbyid(p.proowner),
      'security_definer', p.prosecdef,
      'volatility', p.provolatile,
      'parallel', p.proparallel,
      'config', coalesce(to_jsonb(p.proconfig), '[]'::jsonb),
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end,
            'privilege', a.privilege_type,
            'grantable', a.is_grantable
          ) order by (case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end) collate "C",
                     a.privilege_type collate "C", a.is_grantable
        )
        from pg_catalog.aclexplode(coalesce(p.proacl, '{}'::aclitem[])) a
      ), '[]'::jsonb),
      'definition_sha256', pg_catalog.encode(
        extensions.digest(
          pg_catalog.convert_to(
            pg_catalog.replace(pg_catalog.pg_get_functiondef(p.oid), E'\r\n', E'\n'),
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
      'roles', to_jsonb(roles),
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
      'owner', pg_catalog.pg_get_userbyid(n.nspowner),
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end,
            'privilege', a.privilege_type,
            'grantable', a.is_grantable
          ) order by (case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end) collate "C",
                     a.privilege_type collate "C", a.is_grantable
        )
        from pg_catalog.aclexplode(coalesce(n.nspacl, '{}'::aclitem[])) a
      ), '[]'::jsonb)
    ) order by n.nspname collate "C"
  ) as value
  from app_namespaces n
),
default_acl_contract as (
  select jsonb_agg(
    jsonb_build_object(
      'owner', pg_catalog.pg_get_userbyid(d.defaclrole),
      'schema', n.nspname,
      'kind', d.defaclobjtype,
      'acl', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'grantee', case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end,
            'privilege', a.privilege_type,
            'grantable', a.is_grantable
          ) order by (case when a.grantee = 0 then 'PUBLIC' else pg_catalog.pg_get_userbyid(a.grantee) end) collate "C",
                     a.privilege_type collate "C", a.is_grantable
        )
        from pg_catalog.aclexplode(coalesce(d.defaclacl, '{}'::aclitem[])) a
      ), '[]'::jsonb)
    ) order by pg_catalog.pg_get_userbyid(d.defaclrole) collate "C",
               n.nspname collate "C", d.defaclobjtype
  ) as value
  from pg_catalog.pg_default_acl d
  join app_namespaces n on n.oid = d.defaclnamespace
  where pg_catalog.pg_get_userbyid(d.defaclrole) = 'postgres'
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
