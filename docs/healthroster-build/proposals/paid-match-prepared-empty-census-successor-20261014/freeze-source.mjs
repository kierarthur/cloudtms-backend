import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const dir = path.dirname(fileURLToPath(import.meta.url)), root = path.resolve(dir, '../../../..');
const rel = path.relative(root, dir).replaceAll('\\', '/'), read = f => fs.readFileSync(path.join(dir, f));
const sha = b => createHash('sha256').update(b).digest('hex');
const write = (f, x) => fs.writeFileSync(path.join(dir, f), JSON.stringify(x, null, 2) + '\n');
assert.ok(!fs.existsSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json')));
const prior = 'docs/healthroster-build/proposals/paid-match-prepared-pay-consumer-construction-20261014/';
const rawManifest = fs.readFileSync(path.join(root, prior, 'FROZEN_ARTIFACT_MANIFEST.json'));
assert.deepEqual(rawManifest, execFileSync('git', ['show', '76a5dbd0721ae2c41edb049312aa81d5df5a06be:' + prior + 'FROZEN_ARTIFACT_MANIFEST.json'], { cwd: root, maxBuffer: 3e6 }));
const priorManifest = JSON.parse(rawManifest);
for (const a of priorManifest.artifacts) { const b = fs.readFileSync(path.join(root, prior, a.file)); assert.equal(sha(b), a.sha256); assert.equal(b.length, a.bytes); }
const proof = JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));
for (const p of proof.native_input_pins) { const b = execFileSync('git', ['show', p.commit + ':' + p.file], { cwd: root, maxBuffer: 3e6 }); assert.equal(sha(b), p.sha256); assert.deepEqual(b, read(p.saved)); }
write('SELECTED_SOURCE_RECIPE.json', { source_only: true, independent_review: 'PENDING', containing_prior_checkpoint: '76a5dbd0721ae2c41edb049312aa81d5df5a06be', prerequisite_packet: prior, prerequisite_manifest_sha256: sha(rawManifest), selected_complete_source_order: ['forward/01_prepared_paid_choice_census.sqlinc', 'forward/02_complete_prepared_context.sqlinc', 'forward/04_complete_apply_census_recheck.sqlinc'], replaced_complete_leaves: proof.records.map(r => r.source), existing_signatures_and_ACL_retained: true, new_private_signatures: [], new_public_signatures: [], new_store_column_calculator: 0, optional_empty_DTO_shape_retained: true, NHSP_empty_behavior_retained: true, genuine_CorrectFinal_paid_choice_writer_added: false, legal_late_choice_interleaving_proved: false, source_39_recorder_9_13_three_receipt_exceptions_and_Policy_X_unchanged: true, SQL_compiled: false, native_proof: false, canonical_mounted: false, whole_primary_complete: false });
function run(args) { const r = spawnSync(process.execPath, args, { cwd: root, encoding: 'utf8', maxBuffer: 2e6 }); assert.equal(r.status, 0, r.stdout + r.stderr); return r.stdout; }
const tests = run(['--test', rel + '/empty-census-source.test.mjs']);
assert.match(tests, /tests 8/); assert.match(tests, /pass 8/); assert.match(tests, /fail 0/);
const files = [];
function walk(prefix = '') { for (const e of fs.readdirSync(path.join(dir, prefix), { withFileTypes: true })) { const f = prefix ? prefix + '/' + e.name : e.name; if (e.isDirectory()) walk(f); else files.push(f); } }
walk();
for (const f of files.filter(f => f.endsWith('.mjs'))) run(['--check', rel + '/' + f]);
const credentials = [/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/, /\bgh[pousr]_[A-Za-z0-9]{20,}\b/, /\bAKIA[0-9A-Z]{16}\b/, /\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/];
const whitespace = [];
for (const f of files) { const s = read(f).toString(); assert.ok(!credentials.some(r => r.test(s)), 'credential ' + f); s.split(/\r?\n/).forEach((line, i) => { if (/[ \t]+$/.test(line)) whitespace.push({ file: f, line: i + 1 }); }); if (/(?:\r?\n){2,}$/.test(s)) whitespace.push({ file: f, kind: 'PRESERVED EOF BLANK LINES' }); }
write('SOURCE_CHECK_RESULT.json', { source_only: true, independent_review: 'PENDING', cases: 8, pass: 8, fail: 0, syntax_pass: 3, whole_owner_reversals: 3, unique_hunks: 5, native_input_pins: proof.native_input_pins.length, preserved_Prepared17_artifacts: priorManifest.artifact_count, credential_findings: 0, whitespace_disposition: whitespace.length ? 'EXACT SOURCE EXCEPTIONS RETAINED; NO BLANKET PASS' : 'PASS', whitespace_exceptions: whitespace, legal_late_choice_interleaving_proved: false, SQL_compiled: false, native_calls: 0, canonical_mounted: false, whole_primary_complete: false, scratch_created: false, cleanup_retried: false });
files.push('SOURCE_CHECK_RESULT.json');
const artifacts = [...new Set(files)].sort().map(file => { const b = read(file); return { file, bytes: b.length, sha256: sha(b) }; });
write('FROZEN_ARTIFACT_MANIFEST.json', { schema_version: 'PAID_MATCH_PREPARED_EMPTY_CENSUS_SUCCESSOR_SOURCE_FREEZE_V1', source_only: true, independent_review: 'PENDING', native_proof: false, canonical_mounted: false, whole_primary_complete: false, artifact_count: artifacts.length, artifacts });
console.log(JSON.stringify({ artifacts: artifacts.length, manifest_sha256: sha(read('FROZEN_ARTIFACT_MANIFEST.json')), check_sha256: sha(read('SOURCE_CHECK_RESULT.json')), recipe_sha256: sha(read('SELECTED_SOURCE_RECIPE.json')), cases: 8, syntax: 3, whole_owners: 3, unique_hunks: 5 }));
