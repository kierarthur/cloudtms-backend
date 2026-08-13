#!/usr/bin/env node
// Fail CI unless the installed targeted fast-route and certified-reuse owners match GitHub.

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(fs.readFileSync(
  path.join(here, 'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json'),
  'utf8',
));
const databaseUrl = process.env.DB_URL;
if (!databaseUrl) {
  console.error('DB_URL is not set');
  process.exit(1);
}

const sqlLiteral = (value) => `'${String(value).replaceAll("'", "''")}'`;
const expected = Array.isArray(manifest.functions) ? manifest.functions : [];
const repoRoot = path.resolve(here, '..', '..');
const jamesSerializerPath = 'supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql';
const jamesHelperPath = 'supabase/repeatable/13082026_1912_pay_workbench_sealed_rate_component_projection_v1.sql';
const jamesSynchronizerPath = 'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql';
const jamesSerializer = fs.readFileSync(path.join(repoRoot, jamesSerializerPath), 'utf8');
const jamesHelper = fs.readFileSync(path.join(repoRoot, jamesHelperPath), 'utf8');
const jamesSynchronizer = fs.readFileSync(path.join(repoRoot, jamesSynchronizerPath), 'utf8');
const names = expected.map((item) => `(${sqlLiteral(item.schema)},${sqlLiteral(item.name)})`).join(',');
const query = `
with wanted(schema_name,function_name) as (values ${names}), catalogue as (
  select n.nspname as schema, p.proname as name,
    pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_arguments,
    pg_catalog.encode(extensions.digest(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8'),'sha256'),'hex') as definition_sha256,
    pg_catalog.pg_get_userbyid(p.proowner) as owner,
    p.prosecdef as security_definer, p.provolatile as volatility, p.proparallel as parallel,
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
  console.error('Unable to read the targeted fast-route and certified-reuse function catalogue');
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

const requiredJamesOwners = [
  ['private', 'pay_workbench_unit_economic_occurrence_page_v1', jamesSerializerPath],
  ['private', 'pay_workbench_sealed_rate_component_projection_v1', jamesHelperPath],
  ['private', 'pay_sync_overpayments_from_workbench_workspace_v1', jamesSynchronizerPath],
];
for (const [schema, name, sourceFile] of requiredJamesOwners) {
  const matches = expected.filter((item) => item.schema === schema && item.name === name);
  if (matches.length !== 1 || JSON.stringify(matches[0].source_files) !== JSON.stringify([sourceFile])) {
    problems.push(`James authority owner or source file is not exact: ${schema}.${name}`);
  }
}

const helperManifest = expected.find((item) =>
  item.schema === 'private' && item.name === 'pay_workbench_sealed_rate_component_projection_v1');
const helperActual = helperManifest ? actualByKey.get(keyOf(helperManifest)) : undefined;
if (!helperActual || helperActual.volatility !== 's' || helperActual.parallel !== 'u'
    || helperActual.security_definer !== true
    || JSON.stringify(helperActual.proconfig) !== JSON.stringify(['search_path=""'])
    || JSON.stringify(helperActual.expanded_acl) !== JSON.stringify([{
      grantee: 'postgres', grantor: 'postgres', is_grantable: false, privilege_type: 'EXECUTE',
    }])) {
  problems.push('sealed rate helper metadata or ACL is not exact');
}

const helperCalls = jamesSynchronizer.match(
  /private\.pay_workbench_sealed_rate_component_projection_v1\s*\(/g,
) || [];
if (helperCalls.length !== 1) problems.push('bounded synchronizer must invoke the sealed rate helper exactly once');
if (/'source_pay_method'\s*,\s*v_scope|'target_pay_method'\s*,\s*v_scope/.test(jamesSynchronizer)) {
  problems.push('bounded synchronizer uses v_scope as component rate-method authority');
}
if (/preliminary_outstanding_allocation|preliminary_allocations|final_allocations|preview_truth_weight_total/i
  .test(jamesSynchronizer)) {
  problems.push('bounded synchronizer contains prohibited proportional allocation ownership');
}
for (const required of [
  'tmp_sync_builder_physical_components',
  'PAY_SYNC_OVERPAYMENTS_RATE_ECONOMIC_FENCE_MISMATCH',
  'PAY_WORKBENCH_CANONICAL_PHYSICAL_COMPONENT_MISMATCH',
]) {
  if (!jamesSynchronizer.includes(required)) problems.push(`bounded physical fence is missing: ${required}`);
}
for (const required of [
  'rate_authority_version', 'physical_bucket_key', 'physical_bucket_digest',
  'builder_comparison_digest', 'financial_digest_version',
]) {
  if (!jamesSerializer.includes(required)) problems.push(`serializer rate contract is missing: ${required}`);
}
if (/public\.(timesheets|timesheets_financials|candidates|umbrellas|settings_finance_windows)/i
  .test(jamesHelper)) {
  problems.push('sealed rate helper contains a live authority relation');
}
if (problems.length) {
  console.error('Targeted fast-route and certified-reuse catalogue verification failed:');
  for (const problem of problems) console.error(`- ${problem}`);
  process.exit(1);
}
console.log(`Targeted fast-route and certified-reuse catalogue verified: ${manifest.function_count} functions`);
