import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// Source-only committed-object reader. This never connects to a database or
// modifies the root worktree, its Git state, or the frozen predecessor packet.
const out = path.dirname(fileURLToPath(import.meta.url));
const own = path.resolve(out, '../../../..');
const root = path.resolve(own, '../healthroster-v2-backend-13102026');
const before = 'b65df0997ac69188eadf4de2a88f70226fa99532';
const after = 'cdaf300c62a0d94b0fd0f5a57b41a42d1c7f2b83';
const frozen = path.resolve(out, '../protected-source-qualification-operational-gate');
const frozenManifestSha = '66b5b97d5628be25a0555c20c3a5755bf9561bd760afcaaaaed1b9b09f71b700';
const frozenBackup = 'adec5f8b8a3955ac725c0da15ee344ac891b7674';
const verifier = 'supabase/verification/13102026_2206_weekly_source_automatic_native_delta_v1.sql';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const git = args => execFileSync('git', ['-c', 'maintenance.auto=false', '-c', 'gc.auto=0', ...args], {
  cwd: root, maxBuffer: 5_000_000
});
const show = (commit, file) => git(['show', `${commit}:${file}`]);
assert.equal(sha(fs.readFileSync(path.join(frozen, 'REVIEW_MANIFEST.json'))), frozenManifestSha);
const manifest = JSON.parse(fs.readFileSync(path.join(frozen, 'REVIEW_MANIFEST.json')));
for (const file of manifest.files) {
  const bytes = fs.readFileSync(path.join(own, file.file));
  assert.equal(bytes.length, file.bytes, file.file);
  assert.equal(sha(bytes), file.sha256, file.file);
}
assert.equal(manifest.files.length, 44);
assert.equal(fs.readdirSync(frozen).length, 45);

const expected = [
  ['M', 'docs/healthroster-build/PROGRESS.md'],
  ['A', 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE.sql'],
  ['A', 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_FRESH.sql'],
  ['A', 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_PLAN.json'],
  ['A', 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_1.json'],
  ['A', 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_1_SQL_CAPSULE.json'],
  ['A', 'docs/healthroster-build/checkpoint13102026/prepare_automatic_short_compile.mjs'],
  ['M', verifier]
];
const actual = git(['diff', '--name-status', before, after]).toString('utf8').trim().split('\n').map(line => line.split('\t'));
assert.deepEqual(actual, expected);
assert.deepEqual(actual.filter(([, file]) => !file.startsWith('docs/')), [['M', verifier]]);

const oldVerifier = show(before, verifier);
const newVerifier = show(after, verifier);
const oldAnchor = "if not exists(select 1 from pg_catalog.pg_proc p where p.oid=pg_catalog.to_regprocedure(r.signature)\n      and pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')=r.body_sha256) then";
const newAnchor = "if not exists(select 1 from pg_catalog.pg_proc retained_proc where retained_proc.oid=pg_catalog.to_regprocedure(r.signature)\n      and pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(retained_proc.prosrc,'UTF8')),'hex')=r.body_sha256) then";
assert.equal(oldVerifier.toString('utf8').split(oldAnchor).length - 1, 1);
assert.equal(newVerifier.toString('utf8').split(newAnchor).length - 1, 1);
assert.equal(newVerifier.toString('utf8').replace(newAnchor, () => oldAnchor), oldVerifier.toString('utf8'));

const census = JSON.parse(fs.readFileSync(path.join(frozen, 'COMPLETE_CLOSURE_CENSUS.json')));
assert.equal(census.root_source_commit, before);
assert.equal(census.base_private_count, 232);
assert.equal(census.new_distinct_private_count, 26);
assert.equal(census.planned_private_count, 258);
const updatedPins = census.root_2205_release_acl_local_receipt_verifier_pins.map(pin => {
  const file = pin.path.replace('../healthroster-v2-backend-13102026/', '');
  const prior = show(before, file), current = show(after, file);
  assert.equal(sha(prior), pin.sha256, file);
  assert.equal(prior.length, pin.bytes, file);
  assert.equal(file === verifier || sha(prior) === sha(current), true, file);
  return {file, prior_sha256: sha(prior), current_sha256: sha(current), bytes: current.length,
    byte_identical: prior.equals(current)};
});
assert.equal(updatedPins.length, 16);
assert.equal(updatedPins.filter(pin => !pin.byte_identical).length, 1);

const fullDiff = git(['diff', '--binary', '--full-index', '--no-ext-diff', before, after]);
const runtimeDiff = git(['diff', '--binary', '--full-index', '--no-ext-diff', before, after, '--', verifier]);
fs.writeFileSync(path.join(out, 'ROOT_B65_TO_CDAF_FULL_DIFF.patch'), fullDiff);
fs.writeFileSync(path.join(out, 'VERIFIER_2206_ALIAS_FORWARD.patch'), runtimeDiff);
const changed = expected.map(([status, file]) => {
  const current = show(after, file), prior = status === 'M' ? show(before, file) : null;
  return {status, file, before_sha256: prior ? sha(prior) : null, after_sha256: sha(current), bytes: current.length,
    role: file === verifier ? 'DATA_FREE_VERIFIER_ALIAS_REPAIR' : 'ROOT_PROGRESS_OR_FAILED_TRIAL_EVIDENCE'};
});
const record = {
  schema_version: 'PROTECTED_SOURCE_STRICT_GATE_ROOT_PIN_FORWARD_V1',
  status: 'FROZEN_SOURCE_PIN_FORWARD_NATIVE_OPERATIONAL_ACCEPTANCE_UNEXECUTED',
  root_before: before, root_after: after, frozen_package_backup_commit: frozenBackup,
  frozen_package_manifest_sha256: frozenManifestSha, frozen_package_files_verified_unchanged: 45,
  changed, full_diff: {file: 'ROOT_B65_TO_CDAF_FULL_DIFF.patch', sha256: sha(fullDiff), bytes: fullDiff.length},
  runtime_diff: {file: 'VERIFIER_2206_ALIAS_FORWARD.patch', sha256: sha(runtimeDiff), bytes: runtimeDiff.length},
  verifier_reverse_alias_matches_entire_previous_file: true, runtime_changes_exactly_one_verifier_alias: true,
  updated_root_input_pins: updatedPins, preserved_root_2205_release_acl_local_receipt_pins: 15,
  root_private_signatures_preserved: 232, planned_complete_private_union: 258,
  new_signatures_from_this_forward: 0, new_service_grants: 0, tables_preserved: 105, service_signatures_preserved: 142,
  operational_prerequisite_v2: false, actual_activation: false, actual_api_ui_phone: false,
  lane_database_calls: 0,
  evidence_rule: 'The historical b65 short-trial failure remains an original b65 execution receipt. This verifier change is selected execution inventory, so it is not same-source execution equivalence and must not relabel old R1 receipts. A new trial or qualification retains its actual new execution identity.',
  integration_rule: 'Use cdaf300c as the root input pin and the current2206 hash here. Preserve frozen45, all232 root private signatures, current2205 Local/receipt/helper/ACL bodies and current release arrays. Compose the full26 additions from the frozen literal census and measure the actual catalogue; do not copy an old central registry.'
};
fs.writeFileSync(path.join(out, 'ROOT_PIN_FORWARD.json'), JSON.stringify(record, null, 2) + '\n');
console.log(JSON.stringify({source_only: true, frozen45_unchanged: true, exact_alias_reverse_proof: true,
  root_input_pins: updatedPins.length, changed_input_pins: 1, new_private_signatures: 0, planned_union: 258,
  verifier_sha256: sha(newVerifier), root_after: after}));
