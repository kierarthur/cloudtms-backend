#!/usr/bin/env node
// Fail CI unless the installed certified-source preview owners match GitHub.

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(fs.readFileSync(
  path.join(here, 'banking_pay_workbench_certified_source_preview_catalog_manifest.json'),
  'utf8',
));
const databaseUrl = process.env.DB_URL;
if (!databaseUrl) {
  console.error('DB_URL is not set');
  process.exit(1);
}

const sqlLiteral = (value) => `'${String(value).replaceAll("'", "''")}'`;
const expected = Array.isArray(manifest.functions) ? manifest.functions : [];
const names = expected.map((item) => `(${sqlLiteral(item.schema)},${sqlLiteral(item.name)})`).join(',');
const query = `
with wanted(schema_name,function_name) as (values ${names}), catalogue as (
  select n.nspname as schema, p.proname as name,
    pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_catalog.encode(extensions.digest(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8'),'sha256'),'hex') as definition_sha256,
    pg_catalog.pg_get_userbyid(p.proowner) as owner,
    p.prosecdef as security_definer,
    coalesce(to_jsonb(p.proconfig),'[]'::jsonb) as proconfig,
    coalesce(acl.expanded_acl,'[]'::jsonb) as expanded_acl
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  join wanted w on w.schema_name=n.nspname and w.function_name=p.proname
  left join lateral (
    select jsonb_agg(jsonb_build_object(
      'grantee',case when x.grantee=0 then 'PUBLIC' else grantee_role.rolname end,
      'grantor',grantor_role.rolname,'privilege_type',x.privilege_type,'is_grantable',x.is_grantable
    ) order by case when x.grantee=0 then 'PUBLIC' else grantee_role.rolname end,x.privilege_type,x.is_grantable) as expanded_acl
    from pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) x
    left join pg_catalog.pg_roles grantee_role on grantee_role.oid=x.grantee
    left join pg_catalog.pg_roles grantor_role on grantor_role.oid=x.grantor
  ) acl on true
)
select coalesce(jsonb_agg(to_jsonb(catalogue) order by schema,name,identity_arguments),'[]'::jsonb)
from catalogue;`;

const result = spawnSync('psql', [databaseUrl, '-v', 'ON_ERROR_STOP=1', '-X', '-tA', '-c', query], {
  encoding: 'utf8',
});
if (result.status !== 0) {
  console.error('Unable to read the certified-source preview function catalogue');
  process.exit(1);
}

const actual = JSON.parse(String(result.stdout || '').trim() || '[]');
const keyOf = (item) => `${item.schema}\u0000${item.name}\u0000${item.identity_arguments}`;
const expectedByKey = new Map(expected.map((item) => [keyOf(item), item]));
const actualByKey = new Map(actual.map((item) => [keyOf(item), item]));
const problems = [];
if (expected.length !== manifest.function_count || expectedByKey.size !== manifest.function_count) {
  problems.push('manifest function count or identity uniqueness is inconsistent');
}
for (const [key, item] of expectedByKey) {
  const current = actualByKey.get(key);
  if (!current) {
    problems.push(`missing identity: ${item.schema}.${item.name}`);
    continue;
  }
  for (const field of ['definition_sha256', 'owner', 'security_definer', 'proconfig', 'expanded_acl']) {
    if (JSON.stringify(item[field]) !== JSON.stringify(current[field])) {
      problems.push(`${item.schema}.${item.name} differs in: ${field}`);
    }
  }
}
for (const [key, item] of actualByKey) {
  if (!expectedByKey.has(key)) problems.push(`unexpected overload: ${item.schema}.${item.name}`);
}
if (problems.length) {
  console.error('Certified-source preview catalogue verification failed:');
  for (const problem of problems) console.error(`- ${problem}`);
  process.exit(1);
}
console.log(`Certified-source preview catalogue verified: ${manifest.function_count} functions`);
