#!/usr/bin/env node
// Fail CI unless the installed semantic Ready-to-Pay and cancellation-reversion
// authorities match their sole GitHub catalogue owner.

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const manifest = JSON.parse(fs.readFileSync(
  path.join(here, 'banking_pay_semantic_ready_cancellation_reversion_catalog_manifest.json'),
  'utf8',
));
const expected = Array.isArray(manifest.functions) ? manifest.functions : [];
const residualPath = 'supabase/repeatable/14082026_1254_pay_workbench_execution_residual_identity_proof_page_v1.sql';
const parentPath = 'supabase/repeatable/13082026_1245_pay_workbench_execution_refresh_owner_proof_page_v1.sql';
const residual = fs.readFileSync(path.join(repoRoot, residualPath), 'utf8');
const parent = fs.readFileSync(path.join(repoRoot, parentPath), 'utf8');
const databaseUrl = process.env.DB_URL;
if (!databaseUrl) {
  console.error('DB_URL is not set');
  process.exit(1);
}

const sqlLiteral = (value) => `'${String(value).replaceAll("'", "''")}'`;
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
  console.error('Unable to read the semantic Ready-to-Pay and cancellation-reversion catalogue');
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
for (const item of expected) {
  for (const sourceFile of item.source_files || []) {
    if (!fs.existsSync(path.join(repoRoot, sourceFile))) {
      problems.push(`${item.schema}.${item.name} has a missing saved source file: ${sourceFile}`);
    }
  }
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
for (const [schema, name, sourceFile] of [
  ['private', 'pay_workbench_execution_residual_identity_proof_page_v1', residualPath],
  ['private', 'pay_workbench_execution_refresh_owner_proof_page_v1', parentPath],
]) {
  const matches = expected.filter((item) => item.schema === schema && item.name === name);
  if (matches.length !== 1 || JSON.stringify(matches[0].source_files) !== JSON.stringify([sourceFile])) {
    problems.push(`residual authority owner or source file is not exact: ${schema}.${name}`);
  }
}
for (const required of [
  'referenced_scopes', 'validated_scope_attestations', 'referenced_scope_set_digest',
  'common_publication_attestation_digest', 'frozen_scope_ordinal',
  'ready_rows_raw', 'ready_rows_validated', 'ready_identity_invalid_count',
  "row_json->>'source_ordinal'", "row_json->>'source_line_id'",
  'EXECUTION_RESIDUAL_COMMON_PUBLICATION_AUTHORITY_CONFLICT',
]) {
  if (!residual.includes(required)) problems.push(`complete V2 residual authority is missing: ${required}`);
}
if (!/ready\.row_ordinal::numeric IS DISTINCT FROM\s*\(v_frozen_scope_ordinal::numeric\*1000000::numeric\s*\+ready\.source_ordinal::numeric\)/
  .test(residual) || /%\s*1000000/.test(residual)) {
  problems.push('READY identity is not bound to the explicit frozen source ordinal equation');
}
const requiredV3Match = residual.match(
  /\]\s*::text\[\]\s+AS\s+allowed_keys,\s*ARRAY\[([\s\S]*?)\]\s*::text\[\]\s+AS\s+required_keys/i,
);
if (!requiredV3Match) {
  problems.push('invariant V3 attestation key contract is missing');
} else {
  for (const key of [
    'source_publication_id', 'source_identity_digest', 'preview_identity_digest',
    'semantic_proof_digest', 'scope_ordinal',
  ]) {
    if (!new RegExp(`'${key}'`).test(requiredV3Match[1])) {
      problems.push(`invariant V3 attestation key is missing: ${key}`);
    }
  }
  for (const key of [
    'certification_version', 'certification_digest', 'admission_seal_version',
    'admission_seal_digest', 'projection_fingerprint', 'original_semantic_proof_digest',
    'selection_recovery_headroom_v1', 'cancellation_request_id',
  ]) {
    if (new RegExp(`'${key}'`).test(requiredV3Match[1])) {
      problems.push(`optional V3 attestation key was made unconditional: ${key}`);
    }
  }
}
for (const requiredPattern of [
  /optional_digest\.value IS NOT NULL\s+AND optional_digest\.value!~'\^\[0-9a-f\]\{32\}\$'/,
  /attestation \? 'selection_recovery_headroom_v1'/,
  /attestation \? 'certification_version'/,
  /attestation \? 'admission_seal_version'/,
  /scope_ordinal',''\)!~'\^\[1-9\]\[0-9\]\{0,17\}\$'/,
  /source_ordinal',''\)~'\^\[1-9\]\[0-9\]\{0,17\}\$'/,
]) {
  if (!requiredPattern.test(residual)) {
    problems.push(`optional V3 evidence or bigint-safe ordinal guard is missing: ${requiredPattern}`);
  }
}
if (!/frozen_scope_ordinal',''\)~'\^\[1-9\]\[0-9\]\{0,17\}\$'/.test(parent)) {
  problems.push('V2 parent frozen scope ordinal is not bigint-safe');
}
for (const required of [
  'residual_candidate_result_count', 'referenced_scope_set_digest',
  'common_publication_attestation_digest', 'frozen_scope_ordinal',
]) {
  if (!parent.includes(required)) problems.push(`V2 residual parent binding is missing: ${required}`);
}
if (!/v_options_version='1'[\s\S]*LIMIT 1/.test(parent)) {
  problems.push('legacy V1 scope selection is not explicitly isolated from V2 authority');
}
if (problems.length) {
  console.error('Semantic Ready-to-Pay and cancellation-reversion catalogue verification failed:');
  for (const problem of problems) console.error(`- ${problem}`);
  process.exit(1);
}
console.log(`Semantic Ready-to-Pay and cancellation-reversion catalogue verified: ${manifest.function_count} functions`);
