#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const manifest = JSON.parse(fs.readFileSync(
  path.join(here, 'banking_pay_workbench_settled_certificate_v8_catalog_manifest.json'),
  'utf8',
));
const databaseUrl = process.env.DB_URL;
if (!databaseUrl) {
  console.error('DB_URL is not set');
  process.exit(1);
}

const sqlLiteral = value => `'${String(value).replaceAll("'", "''")}'`;
const expected = Array.isArray(manifest.functions) ? manifest.functions : [];
const names = [...new Set(expected.map(item => `${item.schema}\u0000${item.name}`))]
  .map(value => {
    const [schema, name] = value.split('\u0000');
    return `(${sqlLiteral(schema)},${sqlLiteral(name)})`;
  }).join(',');
const query = `
with wanted(schema_name,function_name) as (values ${names}), catalogue as (
  select n.nspname as schema,p.proname as name,
    pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_catalog.encode(extensions.digest(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8'),'sha256'),'hex') as definition_sha256,
    pg_catalog.pg_get_userbyid(p.proowner) as owner,p.prosecdef as security_definer,
    p.provolatile as volatility,p.proparallel as parallel,
    coalesce(pg_catalog.to_jsonb(p.proconfig),'[]'::jsonb) as proconfig,
    coalesce(acl.expanded_acl,'[]'::jsonb) as expanded_acl
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  join wanted w on w.schema_name=n.nspname and w.function_name=p.proname
  left join lateral (
    select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'grantee',case when x.grantee=0 then 'PUBLIC' else grantee_role.rolname end,
      'grantor',grantor_role.rolname,'privilege_type',x.privilege_type,'is_grantable',x.is_grantable
    ) order by case when x.grantee=0 then 'PUBLIC' else grantee_role.rolname end,x.privilege_type,x.is_grantable) as expanded_acl
    from pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) x
    left join pg_catalog.pg_roles grantee_role on grantee_role.oid=x.grantee
    left join pg_catalog.pg_roles grantor_role on grantor_role.oid=x.grantor
  ) acl on true
)
select pg_catalog.jsonb_build_object(
  'current_user',current_user,
  'functions',coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(catalogue)
    order by schema,name,identity_arguments),'[]'::jsonb)
)
from catalogue;`;

const psql = process.env.PSQL || 'psql';
const result = spawnSync(psql, [databaseUrl, '-v', 'ON_ERROR_STOP=1', '-X', '-tA', '-c', query], {
  encoding: 'utf8',
});
if (result.status !== 0) {
  console.error('Unable to read the H1 V8 function catalogue');
  process.exit(1);
}

const response = JSON.parse(String(result.stdout || '').trim() || '{}');
const physicalOwner = String(response.current_user || '');
const logicalOwner = owner => owner === physicalOwner ? 'postgres' : owner;
const canonicalActual = (response.functions || []).map(item => ({
  ...item,
  owner: logicalOwner(item.owner),
  proconfig: [...item.proconfig].sort(),
  expanded_acl: item.expanded_acl.map(acl => ({
    ...acl,
    grantee: logicalOwner(acl.grantee),
    grantor: logicalOwner(acl.grantor),
  })),
}));
const canonicalJson = value => {
  if (Array.isArray(value)) return value.map(canonicalJson);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonicalJson(value[key])]));
};
const jsonEqual = (left, right) => JSON.stringify(canonicalJson(left)) === JSON.stringify(canonicalJson(right));
const keyOf = item => `${item.schema}\u0000${item.name}\u0000${item.identity_arguments}`;
const expectedByKey = new Map(expected.map(item => [keyOf(item), item]));
const actualByKey = new Map(canonicalActual.map(item => [keyOf(item), item]));
const problems = [];

if (manifest.project_ref !== 'provider-neutral') problems.push('manifest is not provider-neutral');
if (expected.length !== 39 || manifest.function_count !== 39 || expectedByKey.size !== 39) {
  problems.push('manifest function count or identity uniqueness is inconsistent');
}
for (const item of expected) {
  if (!Array.isArray(item.source_files) || item.source_files.length < 1) {
    problems.push(`${item.schema}.${item.name} has no source owner`);
    continue;
  }
  for (const sourceFile of item.source_files) {
    const absolute = path.resolve(root, sourceFile);
    if (!absolute.startsWith(path.resolve(root, 'supabase', 'repeatable') + path.sep)
        || !fs.existsSync(absolute)) {
      problems.push(`${item.schema}.${item.name} has an invalid source owner: ${sourceFile}`);
    }
  }
}
for (const [key, item] of expectedByKey) {
  const actual = actualByKey.get(key);
  if (!actual) {
    problems.push(`missing identity: ${item.schema}.${item.name}(${item.identity_arguments})`);
    continue;
  }
  for (const field of [
    'definition_sha256','owner','security_definer','volatility','parallel','proconfig','expanded_acl',
  ]) {
    if (!jsonEqual(item[field], actual[field])) {
      problems.push(`${item.schema}.${item.name} differs in: ${field}`);
    }
  }
}
for (const [key, item] of actualByKey) {
  if (!expectedByKey.has(key)) problems.push(`unexpected overload: ${item.schema}.${item.name}`);
}

if (problems.length) {
  console.error('H1 V8 catalogue verification failed:');
  for (const problem of problems) console.error(`- ${problem}`);
  process.exit(1);
}
console.log(`H1 V8 catalogue verified: ${manifest.function_count} functions`);
