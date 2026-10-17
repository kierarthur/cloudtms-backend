set local search_path = pg_catalog, public;
with identity_proof as (select jsonb_build_object('physicalDatabase',current_database(),'identityTest',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null),
 'customerKeyNull',exists(select 1 from private.cloudtms_database_identity where singleton and customer_key is null),
 'latestRelease',(select jsonb_build_object('status',status,'commit',git_commit,'releaseId',release_id,'completedAt',completed_at_utc,
  'repositoryContract',repository_contract_sha256,'installedContract',installed_contract_sha256) from private.cloudtms_database_releases order by started_at_utc desc,release_id desc limit 1),
 'module',(select active_owner||':'||owner_epoch::text from private.bpay_next_module_control where id=1),
 'applyingCount',(select count(*) from private.cloudtms_database_releases where status='APPLYING'),'markerAbsent',to_regclass('private.hr_install2_11111111111111111111111111111111') is null)::text), business as (select 'private.candidate_daily_batch_receipts' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "private"."candidate_daily_batch_receipts" t) hashed_rows
union all
select 'public.audit_events' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."audit_events" t) hashed_rows
union all
select 'public.settings_defaults' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."settings_defaults" t) hashed_rows
union all
select 'public.tms_users' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."tms_users" t) hashed_rows
union all
select 'public.ts_financials_outbox' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."ts_financials_outbox" t) hashed_rows
union all
select 'public.weekly_candidate_cohorts' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."weekly_candidate_cohorts" t) hashed_rows
union all
select 'public.weekly_candidate_outreach_generations' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."weekly_candidate_outreach_generations" t) hashed_rows
union all
select 'public.weekly_manager_recipient_generations' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."weekly_manager_recipient_generations" t) hashed_rows
union all
select 'public.weekly_message_dispatch_commands' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."weekly_message_dispatch_commands" t) hashed_rows
union all
select 'public.weekly_source_global_settings' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."weekly_source_global_settings" t) hashed_rows
union all
select 'public.weekly_timesheet_submission_request_memberships' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."weekly_timesheet_submission_request_memberships" t) hashed_rows
union all
select 'public.weekly_timesheet_submission_requests' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint
   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from "public"."weekly_timesheet_submission_requests" t) hashed_rows),
 ledger as (select jsonb_build_object('identity',(select coalesce(jsonb_agg(to_jsonb(l)),'[]'::jsonb) from private.cloudtms_database_identity l),
 'migrations',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_migration_ledger l),
 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),
 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),
 catalogue as (with
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
              case when c.relkind = 'S' then 's'::"char" else 'r'::"char" end,
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
  'contract_version', 5,
  'extensions', coalesce((select value from extensions_contract), '[]'::jsonb),
  'schemas', coalesce((select value from schema_contract), '[]'::jsonb),
  'enums', coalesce((select value from enum_contract), '[]'::jsonb),
  'relations', coalesce((select value from relation_contract), '[]'::jsonb),
  'routines', coalesce((select value from routine_contract), '[]'::jsonb),
  'triggers', coalesce((select value from trigger_contract), '[]'::jsonb),
  'policies', coalesce((select value from policy_contract), '[]'::jsonb),
  'default_acls', coalesce((select value from default_acl_contract), '[]'::jsonb)
)::text)
 select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(
 'ledgerFingerprint',encode(pg_catalog.sha256(pg_catalog.convert_to((select value from ledger)::text,'UTF8')),'hex'),
 'catalogueFingerprint',encode(pg_catalog.sha256(pg_catalog.convert_to((select to_jsonb(c) from catalogue c)::text,'UTF8')),'hex'),
 'businessCountsFingerprint',encode(pg_catalog.sha256(pg_catalog.convert_to((select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count) order by relation),'[]'::jsonb) from business)::text,'UTF8')),'hex'),
 'businessRowsFingerprint',encode(pg_catalog.sha256(pg_catalog.convert_to((select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business)::text,'UTF8')),'hex'),
 'businessRelations',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business),
 'relationCensus','d213a5d24a20317095d431c95689c9656a96aace1dd4f084489bd4e0bd4116b2',
 'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),
  'backgroundSnapshotXmax',pg_snapshot_xmax(pg_current_snapshot())::text,
  'backgroundSnapshotEpochZero',pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296,
  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(pg_current_snapshot()) x),
  'backgroundRows',jsonb_build_object('private.candidate_daily_batch_receipts',(select coalesce(jsonb_agg(jsonb_build_object('keyHash',encode(pg_catalog.sha256(pg_catalog.convert_to((r.batch_receipt_id)::text,'UTF8')),'hex'),
   'rowHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r))::text,'UTF8')),'hex'),'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,
   'nativeClaimProved',coalesce((r.environment='TEST' and r.actor_class='SIGNED_SYSTEM' and r.operation_class='PROJECTION_CLAIM'
  and r.claim_target='MASTER_AVAILABILITY_SHEET' and r.claim_limit between 1 and 100
  and r.claim_request_id is not null and r.item_count=1
  and r.item_keys_json=jsonb_build_array('claim:'||r.claim_request_id::text)
  and r.state='COMPLETED' and r.terminal_http_status=200
  and r.terminal_response_body->>'batch_receipt_id'=r.batch_receipt_id::text
  and r.terminal_response_body->>'claim_request_id'=r.claim_request_id::text
  and r.terminal_response_body->'items'=r.claimed_items_json
  and (case when pg_input_is_valid(r.terminal_response_body->>'lease_set_expires_at','timestamptz')
    then (r.terminal_response_body->>'lease_set_expires_at')::timestamptz end)=r.lease_expires_at_utc
  and extract(epoch from r.lease_expires_at_utc-r.created_at_utc) between 30 and 600
  and extract(epoch from r.lease_expires_at_utc-r.created_at_utc)=trunc(extract(epoch from r.lease_expires_at_utc-r.created_at_utc))
  and r.request_hash=private._candidate_daily_json_sha256_v1(jsonb_build_object('operation','PROJECTION_CLAIM',
    'claim_request_id',r.claim_request_id,'target',r.claim_target,'claimant',r.lease_owner,
    'max_items',r.claim_limit,'lease_seconds',extract(epoch from r.lease_expires_at_utc-r.created_at_utc)::integer))
  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)
   order by r.batch_receipt_id),'[]'::jsonb) from private.candidate_daily_batch_receipts r),'public.ts_financials_outbox',(select coalesce(jsonb_agg(jsonb_build_object('keyHash',encode(pg_catalog.sha256(pg_catalog.convert_to((r.id)::text,'UTF8')),'hex'),'rowHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r))::text,'UTF8')),'hex'),
   'stableHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r)-array['attempt_count','next_attempt_at'])::text,'UTF8')),'hex'),'xmin',r.xmin::text,
   'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,
   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)
   order by r.id),'[]'::jsonb) from public.ts_financials_outbox r)),
  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)
    from catalogue c cross join lateral jsonb_array_elements(c.jsonb_build_object::jsonb->'routines') r
    where r->>'identity' ~ '^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\('),
 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and pid <> pg_backend_pid() and not (pid = 101 and datid = 17) and (state <> 'idle' or xact_start is not null))))::text from identity_proof;