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
const jamesSourceBuildPath = 'supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql';
const jamesSourceBuildAuthorityPath = 'supabase/repeatable/08082026_0322_pay_workbench_candidate_source_build_chunk_authority.sql';
const clonePath = 'supabase/repeatable/04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql';
const cursorPreservePath = 'supabase/repeatable/05082026_1348_pay_workbench_fact_cursor_preserve_v2.sql';
const cursorTransitionPath = 'supabase/repeatable/05082026_1539_pay_workbench_fact_cursor_transition_v3.sql';
const claimStartPath = 'supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql';
const financeCancellationAuthorityPath = 'supabase/repeatable/17082026_2052_pay_finance_resolution_cancel_authority.sql';
const jamesSerializer = fs.readFileSync(path.join(repoRoot, jamesSerializerPath), 'utf8');
const jamesHelper = fs.readFileSync(path.join(repoRoot, jamesHelperPath), 'utf8');
const jamesSynchronizer = fs.readFileSync(path.join(repoRoot, jamesSynchronizerPath), 'utf8');
const jamesSourceBuild = fs.readFileSync(path.join(repoRoot, jamesSourceBuildPath), 'utf8');
const clone = fs.readFileSync(path.join(repoRoot, clonePath), 'utf8');
const cursorPreserve = fs.readFileSync(path.join(repoRoot, cursorPreservePath), 'utf8');
const cursorTransition = fs.readFileSync(path.join(repoRoot, cursorTransitionPath), 'utf8');
const claimStart = fs.readFileSync(path.join(repoRoot, claimStartPath), 'utf8');
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
  ['private', 'pay_workbench_unit_economic_occurrence_page_v1', [jamesSerializerPath]],
  ['private', 'pay_workbench_sealed_rate_component_projection_v1', [jamesHelperPath]],
  ['private', 'pay_sync_overpayments_from_workbench_workspace_v1', [jamesSynchronizerPath]],
  ['private', 'pay_workbench_candidate_source_build_chunk_legacy_v1',
    [jamesSourceBuildPath, jamesSourceBuildAuthorityPath]],
  ['public', 'pay_workbench_session_clone_eligible_rows_v1', [clonePath]],
  ['private', 'pay_workbench_fact_cursor_preserve_v2', [cursorPreservePath]],
  ['private', 'pay_workbench_fact_cursor_transition_v3', [cursorTransitionPath]],
  ['public', 'pay_workbench_source_build_attempt_claim_start_v1', [claimStartPath]],
];
for (const [schema, name, sourceFiles] of requiredJamesOwners) {
  const matches = expected.filter((item) => item.schema === schema && item.name === name);
  if (matches.length !== 1 || JSON.stringify(matches[0].source_files) !== JSON.stringify(sourceFiles)) {
    problems.push(`James authority owner or source file is not exact: ${schema}.${name}`);
  }
}

const requiredFinanceCancellationOwners = [
  ['public', 'pay_preview_candidate_build_finance_case_baseline'],
  ['public', 'pay_preview_candidate_build_canonical_lines'],
  ['public', 'pay_workbench_session_clear_case_resolution'],
];
for (const [schema, name] of requiredFinanceCancellationOwners) {
  const matches = expected.filter((item) => item.schema === schema && item.name === name);
  if (matches.length !== 1
      || JSON.stringify(matches[0].source_files) !== JSON.stringify([financeCancellationAuthorityPath])) {
    problems.push(`finance cancellation owner or source file is not exact: ${schema}.${name}`);
  }
}
if (!fs.existsSync(path.join(repoRoot, financeCancellationAuthorityPath))) {
  problems.push('finance cancellation authority source file is missing');
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
  'PAY_SYNC_OVERPAYMENTS_RATE_PHYSICAL_FENCE_MISMATCH',
]) {
  if (!jamesSynchronizer.includes(required)) problems.push(`bounded physical fence is missing: ${required}`);
}
for (const required of [
  'rate_authority_version', 'physical_bucket_key', 'physical_bucket_digest',
  'builder_comparison_digest', 'financial_digest_version',
]) {
  if (!jamesSerializer.includes(required)) problems.push(`serializer rate contract is missing: ${required}`);
}
if (!/WHEN input\.source_pay_method IS NULL\s+OR input\.source_pay_method NOT IN \('PAYE','UMBRELLA'\)/
  .test(jamesSerializer)
  || !/WHEN input\.target_pay_method IS NULL\s+OR input\.target_pay_method NOT IN \('PAYE','UMBRELLA'\)/
    .test(jamesSerializer)) {
  problems.push('serializer nullable pay-method failures are not typed before materialisation');
}
if (!jamesSerializer.includes('FROM jsonb_each(CASE')
    || !jamesSerializer.includes("WHEN 'ADDITIONAL' THEN 'additional:'||UPPER")) {
  problems.push('serializer does not preserve every arbitrary additional-rate identity');
}
for (const required of [
  'nested_evidence_raw', 'nested_evidence_normalized', 'exact_allocation_matched',
  'sealed_physical_amount_attribution', 'truth_residual_sources',
  'sealed_finance_case_authority', 'FINANCE_CASE_IDENTITY',
  'synthetic_component_authorities',
  'TOP_LEVEL_SOURCE_BASIS', 'CASE_BUCKET_RESOLUTION',
  'RATE_AUTHORITY_NESTED_AMOUNT_OVERCONSUMED',
  'RATE_AUTHORITY_PARENT_COMPONENT_RECONCILIATION_MISMATCH',
]) {
  if (!jamesHelper.includes(required)) {
    problems.push(`sealed physical baseline/reservation attribution is missing: ${required}`);
  }
}
if (/synthetic_bucket_attributed AS MATERIALIZED \([\s\S]*?FROM synthetic_component_sources source\s+LEFT JOIN LATERAL jsonb_array_elements_text[\s\S]{0,180}\sGROUP BY source\.timesheet_id/
  .test(jamesHelper)) {
  problems.push('synthetic authority metadata still multiplies financial component rows');
}
for (const required of [
  'ACTIVE_ITEM_RESERVATION:', 'pay_batch_items_active_reservation',
  'RESERVATION_ECONOMIC_KEY_MISSING', 'RESERVATION_ECONOMIC_KEY_CONFLICT',
  'RESERVATION_DUPLICATE_LOGICAL_OWNER',
]) {
  if (!jamesSourceBuild.includes(required)) {
    problems.push(`sealed active-item reservation authority is missing: ${required}`);
  }
}
if (!/COALESCE\(item\.reservation_id,item\.pay_batch_item_id\)/.test(jamesSourceBuild)) {
  problems.push('active item reservation does not bind its deterministic fact authority identity');
}
for (const required of [
  'tmp_sync_sealed_reservation_items', 'tmp_sync_reserved_batch_items_replacement',
  'tmp_sync_reserved_additional_by_code_replacement', 'component_fallback',
  'WORKED_TIME_AMOUNT',
]) {
  if (!jamesSynchronizer.includes(required)) {
    problems.push(`sealed synchronizer reconstruction is missing: ${required}`);
  }
}
if (!jamesSynchronizer.includes("SELECT 21,'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'")
    || !jamesSynchronizer.includes("SELECT 22,'RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING'")) {
  problems.push('synchronizer nullable pay-method failures are not materialised before constraints');
}
if (!/GROUP BY raw\.timesheet_id,raw\.component_kind,raw\.component_member_identity,\s*raw\.economic_key_type,raw\.economic_key_value,raw\.bucket_code/
  .test(jamesSynchronizer)) {
  problems.push('multiple-rate fence does not use the full physical component identity');
}
const additionalCodeExpression = /UPPER\(NULLIF\(BTRIM\(COALESCE\(\s*breakdown\.value->>'bucket_code',\s*CASE WHEN sealed\.component_key_type='ADDITIONAL_CODE'\s*THEN sealed\.component_key_value END\)\),''\)\)/g;
if ((jamesSynchronizer.match(additionalCodeExpression) || []).length !== 2) {
  problems.push('reservation additional-code SELECT and GROUP BY expressions differ');
}
if (/public\.(timesheets|timesheets_financials|candidates|umbrellas|settings_finance_windows)/i
  .test(jamesHelper)) {
  problems.push('sealed rate helper contains a live authority relation');
}
for (const required of [
  'source_method_evidence', 'source_method_authority_tier',
  'source_method_authority_summary', 'source_method_authority',
  'selected_authority_priority',
  'distinct_supported_source_method_count', 'complete_evidence_digest',
  'RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT',
]) {
  if (!jamesHelper.includes(required)) problems.push(`exact economic-key source-method proof is missing: ${required}`);
}
if (/allocation\.economic_key_type IN \('TS_DAY','TS_TOTAL'\)[\s\S]{0,160}allocation\.matched_count=0[\s\S]{0,160}THEN 0/
  .test(jamesHelper)) {
  problems.push('complete baseline-only nested evidence is still discarded without a live bucket');
}
if (!/GROUP BY evidence\.timesheet_id,evidence\.economic_key_type,evidence\.economic_key_value/
  .test(jamesHelper) || /MIN\((?:source\.)?source_pay_method\)/i.test(jamesHelper)) {
  problems.push('sealed rate helper does not preserve exact economic-key source-method cardinality');
}
if (!/CASE WHEN COALESCE\(bucket\.validated_failure,economic\.failure_code\) IS NOT NULL\s+THEN 'FAILED'/
  .test(jamesHelper)) {
  problems.push('failed economic-key rows are not typed FAILED before component admission');
}
if (!jamesSynchronizer.includes('complete_component_method_digest')
    || !jamesSynchronizer.includes('component_method_sample')
    || /COALESCE\(component\.source_pay_method,(?:candidate_pay_method|current_target_pay_method)\)/i
      .test(jamesSynchronizer)
    || /MIN\(sealed\.source_pay_method\)/i.test(jamesSynchronizer)) {
  problems.push('synchronizer source-method proof or target-as-source prohibition is incomplete');
}
for (const required of [
  'WORKBENCH_SOURCE_OWNER_V3', 'WORKBENCH_SOURCE_OWNER_V2',
  'authority_fingerprint_version', 'authority_fingerprint',
  'source_publication_baseline_required', 'required_physical_publication_contract_version',
]) {
  if (!clone.includes(required)) problems.push(`clone fingerprint parity is missing: ${required}`);
}
for (const required of [
  'RESERVATION_COMPONENT_SOURCE_KEY_C_V1', 'reservation_source_key_order_contract',
  'PAY_WORKBENCH_RESERVATION_ORDER_CONTRACT_OBSOLETE',
]) {
  if (!cursorPreserve.includes(required) || !jamesSourceBuild.includes(required)) {
    problems.push(`reservation cursor cutover is missing: ${required}`);
  }
}
if (!cursorTransition.includes('RESERVATION_COMPONENT_SOURCE_KEY_C_V1')
    || !/source_key COLLATE "C"/.test(jamesSourceBuild)
    || !/v_last_source_key COLLATE "C"/.test(jamesSourceBuild)) {
  problems.push('reservation cursor transition or C-order paging contract is incomplete');
}
for (const required of [
  'pay_workbench_repair_orphaned_pending_source_build',
  'PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN',
  'REBOUND_ACTIVE_SUCCESSOR', 'RECONCILED_SUCCESSFUL_BUILD', 'FAILED_CLOSED_MAX_ATTEMPTS',
  'v_terminal_candidate_state_present:=FOUND',
]) {
  if (!claimStart.includes(required)) problems.push(`terminal claim convergence is missing: ${required}`);
}
if (problems.length) {
  console.error('Targeted fast-route and certified-reuse catalogue verification failed:');
  for (const problem of problems) console.error(`- ${problem}`);
  process.exit(1);
}
console.log(`Targeted fast-route and certified-reuse catalogue verified: ${manifest.function_count} functions`);
