import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const dir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(dir, '../../../..');
const rel = path.relative(root, dir).replaceAll('\\', '/');
const sha = b => createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json')));
function run(args) {
  const r = spawnSync(process.execPath, args, { cwd: root, encoding: 'utf8', maxBuffer: 2e6 });
  assert.equal(r.status, 0, r.stderr + r.stdout); return r.stdout;
}
const output = run(['--test', rel + '/all-owner.test.mjs']);
assert.match(output, /tests 15/); assert.match(output, /pass 15/); assert.match(output, /fail 0/);
const inherited = run(['--test', 'docs/healthroster-build/proposals/rekey-source-alias-current-native-successor-20261014/source-alias.test.mjs']);
assert.match(inherited, /tests 9/); assert.match(inherited, /pass 9/); assert.match(inherited, /fail 0/);
const files = [];
function walk(prefix) {
  for (const entry of fs.readdirSync(path.join(dir, prefix), { withFileTypes: true })) {
    const file = prefix ? prefix + '/' + entry.name : entry.name;
    if (entry.isDirectory()) walk(file); else files.push(file);
  }
}
walk(''); const scripts = files.filter(f => f.endsWith('.mjs'));
for (const file of scripts) run(['--check', rel + '/' + file]);
fs.writeFileSync(path.join(dir, 'SOURCE_CHECK_RESULT.json'), JSON.stringify({
  source_only: true, effect_free_all_owner_cases: 15, passed: 15, failed: 0,
  inherited_predecessor_cases_passed: 9, syntax_passed: scripts.length,
  complete_owner_reversals: 1, unique_hunks: 4,
  inherited_complete_three_owner_reversals: 3, inherited_unique_hunks: 4,
  predecessor_artifacts_preserved: 15,
  foreign_candidate_and_client_refusal: 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT/55000',
  multiple_owner_refusal: 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS/55000',
  actual_discovery_and_projection_source_branches_checked: true,
  new_signatures_beyond_frozen15: 0, new_stores: 0, event_original_key_rewritten: false,
  independent_review: 'PENDING', canonical_mounted: false, sql_compiled: false,
  native_proof: false, full_paid_overlap_complete: false, full_primary_complete: false,
  scratch_created: false, real_database_or_lab_calls: 0, cleanup_retried: false,
  remaining_gates: 'REFUSAL_ABI_AND_BOUNDARY.md'
}, null, 2) + '\n');
files.push('SOURCE_CHECK_RESULT.json');
const artifacts = [...new Set(files)].sort().map(file => {
  const b = fs.readFileSync(path.join(dir, file)); return { file, bytes: b.length, sha256: sha(b) };
});
fs.writeFileSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json'), JSON.stringify({
  schema_version: 'REKEY_SOURCE_ALIAS_ALL_OWNER_SOURCE_SUCCESSOR_FREEZE_V1',
  source_only: true, independent_review: 'PENDING', canonical_mounted: false, native_proof: false,
  artifact_count: artifacts.length, artifacts
}, null, 2) + '\n');
console.log(JSON.stringify({ artifacts: artifacts.length, manifest_sha256: sha(fs.readFileSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json'))), check_sha256: sha(fs.readFileSync(path.join(dir, 'SOURCE_CHECK_RESULT.json'))), new_cases: 15, inherited_cases: 9, syntax_passed: scripts.length }));
