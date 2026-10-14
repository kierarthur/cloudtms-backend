import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const git = (...a) => execFileSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false', ...a], { cwd: repo, maxBuffer: 64e6 });
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
const patterns = [/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/, /\b(?:ghp_|github_pat_)[A-Za-z0-9_]{30,}\b/, /\bAKIA[A-Z0-9]{16}\b/, /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b/];
const packets = [
  ['agency-mode-a-preset-policy-source-successor-20261014', '0bb9866c364a66fbaca2858f071f9c3728e88c11aa76a170b7d94d65fe1ee0d8', 31, 'construction/mode-a-policy.test.mjs', 33],
  ['agency-issued-credit-dependency-source-successor-20261014', 'f849596fa983ce92804013f42cd3d7bcc8ffc3a2db629431d9d73b0e83fca2b7', 12, 'construction/issued-credit-dependency.test.mjs', 16],
];
const files = []; let scans = 0, tests = 0;
for (const [name, hash, count, test, expected] of packets) {
  const rel = 'docs/healthroster-build/proposals/' + name;
  const m = fs.readFileSync(path.join(repo, rel, 'REVIEW_MANIFEST.json'));
  assert.equal(sha(m), hash);
  const entries = JSON.parse(m).entries;
  assert.equal(entries.length + 1, count);
  for (const e of [...entries, { path: 'REVIEW_MANIFEST.json', bytes: m.length, sha256: sha(m) }]) {
    const p = rel + '/' + e.path, b = fs.readFileSync(path.join(repo, p));
    assert.equal(b.length, e.bytes); assert.equal(sha(b), e.sha256);
    assert.ok(patterns.every(pattern => !pattern.test(b.toString())), 'Credential-like material: ' + p);
    files.push(p); scans++;
  }
  const run = spawnSync(process.execPath, ['--test', path.join(repo, rel, test)], { encoding: 'utf8' });
  assert.equal(run.status, 0, run.stdout + run.stderr);
  assert.equal(Number(/pass (\d+)/.exec(run.stdout)[1]), expected); tests += expected;
}
let preserved = 0;
for (const name of ['agency-native-whole-week-validation-runtime-forward-20261014', 'agency-validation-marker-override-source-successor-20261014',
  'ppv2-report-d10-query-population-source-forward-20261014', 'agency-mode-a-source-support-issue-construction-20261014']) {
  const rel = 'docs/healthroster-build/proposals/' + name;
  const m = fs.readFileSync(path.join(repo, rel, 'REVIEW_MANIFEST.json'));
  for (const e of [...JSON.parse(m).entries, { path: 'REVIEW_MANIFEST.json', bytes: m.length, sha256: sha(m) }]) {
    const p = rel + '/' + e.path, b = fs.readFileSync(path.join(repo, p));
    assert.equal(sha(b), e.sha256); assert.ok(b.equals(git('show', 'f44a95b5501d964e17fe6175fa4e1e1654bf622c:' + p))); preserved++;
  }
}
for (const p of ['docs/healthroster-build/AGENCY_MODE_A_CREDIT_SOURCE_CHECKPOINT_14102026.md',
  'docs/healthroster-build/check-agency-mode-a-credit-source-backup.mjs']) {
  const b = fs.readFileSync(path.join(repo, p)); assert.ok(patterns.every(pattern => !pattern.test(b.toString())));
  files.push(p); scans++;
}
if (process.argv.includes('--staged')) {
  const staged = git('diff', '--cached', '--name-only', '-z').toString().split('\0').filter(Boolean).sort();
  assert.deepEqual(staged, [...files].sort());
  for (const p of files) assert.ok(fs.readFileSync(path.join(repo, p)).equals(git('show', ':' + p)));
}
if (process.argv.includes('--committed')) for (const p of files) assert.ok(fs.readFileSync(path.join(repo, p)).equals(git('show', 'HEAD:' + p)));
console.log(JSON.stringify({ source_only: true, native: false, files: files.length, saved_mismatches: 0, credential_scans: scans,
  credential_findings: 0, tests, predecessor37_13_32_45_files_unchanged: preserved,
  staged_exact: process.argv.includes('--staged'), committed_exact: process.argv.includes('--committed'), native_seal: null }));
