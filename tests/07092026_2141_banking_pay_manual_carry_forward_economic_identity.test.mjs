import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourcePath = path.join(root, 'supabase', 'repeatable', '04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql');
const ownerPath = path.join(root, 'supabase', 'repeatable', '07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql');
const generatorPath = path.join(root, 'scripts', 'generate-banking-pay-manual-carry-forward-economic-identity-v1.mjs');
const verifierPath = path.join(root, 'supabase', 'verification', '07092026_2142_banking_pay_manual_carry_forward_economic_identity_verification.sql');
const source = readFileSync(sourcePath, 'utf8');
const owner = readFileSync(ownerPath, 'utf8');
const verifier = readFileSync(verifierPath, 'utf8');

const count = (value, needle) => value.split(needle).length - 1;
const sha256 = (value) => createHash('sha256').update(value).digest('hex');

const addedIdentity =
  "              'component_key_type', 'MANUAL_CARRY_FORWARD',\n" +
  "              'component_key_value', cf_lines.manual_adjustment_carry_forward_id::text,\n" +
  "              'key_type', 'MANUAL_CARRY_FORWARD',\n" +
  "              'key_value', cf_lines.manual_adjustment_carry_forward_id::text,\n" +
  "              'economic_key', jsonb_strip_nulls(jsonb_build_object(\n" +
  "                'timesheet_id', CASE WHEN cf_lines.timesheet_id IS NULL THEN NULL ELSE cf_lines.timesheet_id::text END,\n" +
  "                'key_type', 'MANUAL_CARRY_FORWARD',\n" +
  "                'key_value', cf_lines.manual_adjustment_carry_forward_id::text\n" +
  "              )),\n";

test('generated carry-forward owner is current and has a one-block intended-only diff', () => {
  const check = spawnSync(process.execPath, [generatorPath, '--check'], {
    cwd: root,
    encoding: 'utf8'
  });
  assert.equal(check.status, 0, check.stderr || check.stdout);
  assert.equal(sha256(source), '28069c242c44e77333c15d599cbac1526ba966af274aad00ab2afda55018f01a');
  assert.equal(sha256(owner), 'b4ec1a479c0608f997daf0de9d2816803b9aa3af6f3a09edfd411c71572c3e4f');
  assert.equal(count(owner, addedIdentity), 1);
  assert.equal(owner.replace(addedIdentity, ''), source);
});

test('replacement adds only the stable producer-owned carry-forward identity', () => {
  assert.equal(count(source, "'key_type', 'MANUAL_CARRY_FORWARD'"), 0);
  assert.equal(count(owner, "'key_type', 'MANUAL_CARRY_FORWARD'"), 2);
  assert.equal(count(owner, "'key_value', cf_lines.manual_adjustment_carry_forward_id::text"), 2);
  assert.equal(count(owner, "'component_key_type', 'MANUAL_CARRY_FORWARD'"), 1);
  assert.equal(count(owner, "'component_key_value', cf_lines.manual_adjustment_carry_forward_id::text"), 1);
  assert.match(owner, /'timesheet_id', CASE WHEN cf_lines\.timesheet_id IS NULL THEN NULL ELSE cf_lines\.timesheet_id::text END,/);
  assert.match(owner, /'source_ref', \('carry_forward:' \|\| cf_lines\.manual_adjustment_carry_forward_id::text\),/);
  assert.match(owner, /'operation_source_key', cf_lines\.operation_source_key,/);
});

test('all existing function identity, metadata and privilege restoration remain unchanged', () => {
  assert.equal(count(owner, 'CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_canonical_lines('), 1);
  assert.equal(count(owner, 'CREATE OR REPLACE FUNCTION '), 1);
  assert.equal(count(owner, 'RETURNS jsonb'), 1);
  assert.equal(count(owner, 'SECURITY DEFINER'), 1);
  assert.match(owner, /ALTER FUNCTION public\.pay_preview_candidate_build_canonical_lines\(jsonb,uuid\)\s+OWNER TO postgres;/);
  assert.match(owner, /ALTER FUNCTION public\.pay_preview_candidate_build_canonical_lines\(jsonb,uuid\)\s+SET search_path TO 'public';/);
  assert.match(owner, /REVOKE ALL ON FUNCTION public\.pay_preview_candidate_build_canonical_lines\(jsonb,uuid\)\s+FROM PUBLIC,anon,authenticated,service_role;/);
  assert.match(owner, /GRANT EXECUTE ON FUNCTION public\.pay_preview_candidate_build_canonical_lines\(jsonb,uuid\)\s+TO postgres,service_role;/);
});

test('runtime verifier accepts only canonical or provider-neutral diagnostic metadata', () => {
  assert.match(verifier, /function_row\.proconfig = ARRAY\['search_path=public'\]::text\[\]/);
  assert.match(verifier, /COALESCE\(function_row\.proconfig, ARRAY\[\]::text\[\]\) @> ARRAY\[/);
  assert.match(verifier, /COALESCE\(function_row\.proconfig, ARRAY\[\]::text\[\]\) <@ ARRAY\[/);
  assert.match(verifier, /pg_catalog\.cardinality\(function_row\.proconfig\) = 8/);
  for (const config of [
    'search_path=public',
    'plpgsql_check.mode=disabled',
    'plpgsql_check.profiler=off',
    'plpgsql_check.tracer=off',
    'plpgsql_check.constants_tracing=off',
    'plpgsql_check.cursors_leaks=off',
    'plpgsql_check.strict_cursors_leaks=off',
    'plpgsql_check.fatal_errors=off',
  ]) {
    assert.ok(count(verifier, `'${config}'`) >= 1, config);
  }
  assert.doesNotMatch(verifier, /plpgsql_check\.(?:mode|profiler|tracer|constants_tracing|cursors_leaks|strict_cursors_leaks|fatal_errors)=(?!disabled|off)/);
});

test('upstream and downstream vocabulary is an exact identity bridge, not an economic rule', () => {
  const sourceBuild = readFileSync(path.join(root, 'supabase', 'repeatable', '07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql'), 'utf8');
  const publisher = readFileSync(path.join(root, 'supabase', 'repeatable', '07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql'), 'utf8');
  const allocation = readFileSync(path.join(root, 'supabase', 'repeatable', '04092026_1360_banking_pay_manual_carry_forward_allocation_seed_v8.sql'), 'utf8');
  assert.match(sourceBuild, /CASE WHEN jsonb_typeof\(line\.line_json->'economic_key'\)='object'[\s\S]+THEN line\.line_json->'economic_key' ELSE '\{\}'::jsonb END/);
  assert.match(publisher, /prepared_row\.key_type IS NULL[\s\S]+OR prepared_row\.key_value IS NULL/);
  assert.match(allocation, /'MANUAL_CARRY_FORWARD'/);
  assert.doesNotMatch(addedIdentity, /amount|vat|gross|net|headroom|tax|paye_treatment|pay_channel/i);
});

test('mutation guards reject removing or changing every required identity field', () => {
  const mutations = [
    addedIdentity.replace("              'component_key_type', 'MANUAL_CARRY_FORWARD',\n", ''),
    addedIdentity.replace("              'component_key_value', cf_lines.manual_adjustment_carry_forward_id::text,\n", ''),
    addedIdentity.replace("              'key_type', 'MANUAL_CARRY_FORWARD',\n", ''),
    addedIdentity.replace("              'key_value', cf_lines.manual_adjustment_carry_forward_id::text,\n", ''),
    addedIdentity.replace("              'economic_key', jsonb_strip_nulls(jsonb_build_object(\n", "              'economic_key', jsonb_build_object(\n"),
    addedIdentity.replace("'MANUAL_CARRY_FORWARD'", "'TS_TOTAL'"),
    addedIdentity.replace('cf_lines.manual_adjustment_carry_forward_id::text', "'TOTAL'")
  ];
  for (const mutation of mutations) {
    assert.notEqual(mutation, addedIdentity);
    const mutatedOwner = owner.replace(addedIdentity, mutation);
    assert.notEqual(mutatedOwner, owner);
    assert.throws(() => {
      assert.equal(count(mutatedOwner, addedIdentity), 1);
      assert.equal(mutatedOwner.replace(addedIdentity, ''), source);
    });
  }
});
