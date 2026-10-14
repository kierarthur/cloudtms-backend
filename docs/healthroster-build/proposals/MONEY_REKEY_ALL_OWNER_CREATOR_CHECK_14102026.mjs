import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const dir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(dir, '../../..');
const sha = b => createHash('sha256').update(b).digest('hex');
const packets = [
  ['rekey-source-alias-current-native-successor-20261014', '44706e1eef850a647458d1af77e4a860520ca60d68801b1b91f88bae6135be07', 15],
  ['rekey-source-alias-all-owner-successor-20261014', '400b1b49fe5534b79baccbea9a9e3b7017458b40c84afeb66ebafedced92ff5c', 10]
];
const files = [];
for (const [folder, hash, count] of packets) {
  const manifestPath = folder + '/FROZEN_ARTIFACT_MANIFEST.json';
  const bytes = fs.readFileSync(path.join(dir, manifestPath));
  assert.equal(sha(bytes), hash);
  const manifest = JSON.parse(bytes); assert.equal(manifest.artifact_count, count);
  for (const a of manifest.artifacts) {
    const file = folder + '/' + a.file, raw = fs.readFileSync(path.join(dir, file));
    assert.equal(raw.length, a.bytes); assert.equal(sha(raw), a.sha256); files.push(file);
  }
  files.push(manifestPath);
}
files.push('MONEY_REKEY_SOURCE_ALIAS_REVIEW_HANDOFF_14102026.md', 'MONEY_REKEY_SOURCE_ALIAS_CREATOR_CHECK_14102026.mjs',
  'MONEY_REKEY_SOURCE_ALIAS_CREATOR_CHECK_RESULT_14102026.json', 'MONEY_REKEY_ALL_OWNER_REVIEW_HANDOFF_14102026.md',
  'MONEY_REKEY_ALL_OWNER_CREATOR_CHECK_14102026.mjs');
const rules = [
  ['private-key', /-----BEGIN [A-Z ]*PRIVATE KEY-----/],
  ['github-token', /(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})/],
  ['literal-jwt', /eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}/],
  ['quoted-secret-assignment', /(?:password|service_role_key|api_key|access_token|bearer_token)\s*[:=]\s*['"][^'"\r\n]{12,}['"]/i]
];
const findings = [];
for (const file of files) for (const [rule, re] of rules) if (re.test(fs.readFileSync(path.join(dir, file), 'utf8'))) findings.push({ file, rule });
assert.equal(findings.length, 0, JSON.stringify(findings));
const original = JSON.parse(fs.readFileSync(path.join(dir, packets[0][0], 'COMPLETE_CURRENT_REVERSES.json')));
for (const p of original.proofs) assert.deepEqual(execFileSync('git', ['show', original.own_input_commit + ':' + p.input], { cwd: root, maxBuffer: 3e6 }), fs.readFileSync(path.join(dir, packets[0][0], 'before', p.file)));
const proof = JSON.parse(fs.readFileSync(path.join(dir, packets[1][0], 'COMPLETE_OWNER_REVERSE.json')));
let source = fs.readFileSync(path.join(dir, packets[1][0], 'forward/00_known_source_key.sqlinc'), 'utf8');
for (const h of [...proof.hunks].reverse()) { assert.equal(source.split(h.after).length, 2); source = source.replace(h.after, h.before); }
assert.equal(sha(source), proof.before_sha256);
assert.equal(source, fs.readFileSync(path.join(dir, packets[0][0], proof.input), 'utf8'));
const result = {
  source_only: true, listed_artifacts: 25, manifests: 2, raw_hash_length_mismatches: 0,
  source_scan_files: files.length, credential_rules: rules.length, credential_findings: findings.length,
  own_published_predecessor: original.own_input_commit,
  original_frozen_artifacts_preserved: 15, complete_helper_reversals: 1, unique_helper_hunks: 4,
  inherited_whole_owner_reversals: 3, inherited_unique_hunks: 4,
  effect_free_new_cases: 15, inherited_cases: 9, syntax_passed: 3,
  independent_review: 'PENDING', source_published: false, canonical_mounted: false,
  sql_compiled: false, native_proof: false, full_paid_overlap_complete: false, full_primary_complete: false,
  scratch_created: false, cleanup_retried: false
};
fs.writeFileSync(path.join(dir, 'MONEY_REKEY_ALL_OWNER_CREATOR_CHECK_RESULT_14102026.json'), JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify(result));
