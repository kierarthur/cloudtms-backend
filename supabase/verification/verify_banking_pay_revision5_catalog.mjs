#!/usr/bin/env node
// Fail CI unless the final installed Banking Pay catalogue matches Revision 5.

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const manifestPath = path.join(here, 'banking_pay_revision5_catalog_manifest.json');
const databaseUrl = process.env.DB_URL;
if (!databaseUrl) {
  console.error('DB_URL is not set');
  process.exit(1);
}

const sqlLiteral = (value) => `'${String(value).replaceAll("'", "''")}'`;
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const expected = Array.isArray(manifest.functions) ? manifest.functions : [];
const tuples = expected.map((item) => `(${sqlLiteral(item.schema)},${sqlLiteral(item.name)})`).join(',');
const query = `
with catalogue as (
  select
    n.nspname as schema,
    p.proname as name,
    pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_catalog.encode(
      extensions.digest(
        pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid), 'UTF8'),
        'sha256'
      ),
      'hex'
    ) as definition_sha256,
    pg_catalog.pg_get_userbyid(p.proowner) as owner,
    p.prosecdef as security_definer,
    coalesce(to_jsonb(p.proconfig), '[]'::jsonb) as proconfig,
    coalesce(acl.expanded_acl, '[]'::jsonb) as expanded_acl
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid = p.pronamespace
  left join lateral (
    select jsonb_agg(
      jsonb_build_object(
        'grantee', case when privileges.grantee = 0 then 'PUBLIC' else grantee_role.rolname end,
        'grantor', grantor_role.rolname,
        'privilege_type', privileges.privilege_type,
        'is_grantable', privileges.is_grantable
      )
      order by case when privileges.grantee = 0 then 'PUBLIC' else grantee_role.rolname end,
               privileges.privilege_type,
               privileges.is_grantable
    ) as expanded_acl
    from pg_catalog.aclexplode(
      coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))
    ) privileges
    left join pg_catalog.pg_roles grantee_role on grantee_role.oid = privileges.grantee
    left join pg_catalog.pg_roles grantor_role on grantor_role.oid = privileges.grantor
  ) acl on true
  where (n.nspname, p.proname) in (${tuples})
)
select coalesce(
  jsonb_agg(to_jsonb(catalogue) order by schema, name, identity_arguments),
  '[]'::jsonb
)
from catalogue;`;

const result = spawnSync(
  'psql',
  [databaseUrl, '-v', 'ON_ERROR_STOP=1', '-X', '-tA', '-c', query],
  { encoding: 'utf8' },
);
if (result.status !== 0) {
  console.error('Unable to read final PostgreSQL function catalogue');
  process.exit(1);
}

const actual = JSON.parse(String(result.stdout || '').trim() || '[]');
const keyOf = (item) => `${item.schema}\u0000${item.name}\u0000${item.identity_arguments}`;
const expectedByKey = new Map(expected.map((item) => [keyOf(item), item]));
const actualByKey = new Map(actual.map((item) => [keyOf(item), item]));
const problems = [];

if (expectedByKey.size !== manifest.function_count) {
  problems.push('manifest function count is inconsistent');
}
for (const [key, item] of expectedByKey) {
  if (!actualByKey.has(key)) problems.push(`missing identity: ${item.schema}.${item.name}`);
}
for (const [key, item] of actualByKey) {
  if (!expectedByKey.has(key)) problems.push(`unexpected overload: ${item.schema}.${item.name}`);
}

const fields = ['definition_sha256', 'owner', 'security_definer', 'proconfig', 'expanded_acl'];
for (const [key, expectedItem] of expectedByKey) {
  const actualItem = actualByKey.get(key);
  if (!actualItem) continue;
  const changed = fields.filter((field) => JSON.stringify(expectedItem[field]) !== JSON.stringify(actualItem[field]));
  if (changed.length) problems.push(`${expectedItem.schema}.${expectedItem.name} differs in: ${changed.join(', ')}`);
}

if (problems.length) {
  console.error('Revision 5 final-catalogue verification failed:');
  for (const problem of problems) console.error(`- ${problem}`);
  process.exit(1);
}
console.log(`Revision 5 final catalogue verified: ${actualByKey.size} functions`);
