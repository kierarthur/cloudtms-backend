import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { composeOwnerSource, sourceSha256 } from './healthroster-owner-source-compose.mjs';

// Fixed, independently source-reviewed inputs. Creates an unmounted proposal;
// never installs SQL, edits another lane or rewrites a frozen source packet.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const peers = path.dirname(root);
const report = { repo: path.join(peers, 'healthroster-v2-report-build-13102026'),
  commit: '8f08aeac806454498c040925cf98b196970a3a2d',
  folder: 'docs/healthroster-build/proposals/ppv2-report-d10-query-population-source-forward-20261014',
  manifest: 'REVIEW_MANIFEST.json', manifestSha: 'df75699198b4292a6cce0ace87d51da515e6f2df5101052442561fddd955da3b' };
const money = { repo: path.join(peers, 'healthroster-v2-money-build-13102026'),
  commit: '30a30f16b29d0b61c11fc91656fdc8abccb5ab64',
  folder: 'docs/healthroster-build/proposals/paid-match-office-confirm-consumer-construction-20261014',
  manifest: 'FROZEN_ARTIFACT_MANIFEST.json', manifestSha: '3d0560bef03c6f8ef0b01024a45581d7d163b32bccc121401e4f0cfc8052e69d' };
const git = (lane, file) => execFileSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false',
  'show', `${lane.commit}:${lane.folder}/${file}`], { cwd: lane.repo, windowsHide: true, maxBuffer: 12_000_000 });
function readPinned(lane, file, expected) {
  const raw = git(lane, file), saved = fs.readFileSync(path.join(lane.repo, lane.folder, file));
  if (!raw.equals(saved) || (expected && sourceSha256(raw) !== expected))
    throw new Error(`PUBLISHED_SAVED_SOURCE_MISMATCH: ${file}`);
  return raw;
}
function checkPacket(lane) {
  const manifest = JSON.parse(readPinned(lane, lane.manifest, lane.manifestSha));
  const entries = manifest.entries ?? manifest.artifacts;
  if (!Array.isArray(entries) || !entries.length) throw new Error('SOURCE_MANIFEST_ENTRIES_REQUIRED');
  const paths = new Set();
  for (const entry of entries) {
    const file = entry.path ?? entry.file;
    if (typeof file !== 'string' || file.startsWith('/') || file.includes('..') || paths.has(file))
      throw new Error('SOURCE_MANIFEST_PATH_INVALID');
    paths.add(file);
    const raw = readPinned(lane, file, entry.sha256);
    if (raw.length !== entry.bytes) throw new Error(`SOURCE_MANIFEST_SIZE_MISMATCH: ${file}`);
  }
  return { commit: lane.commit, folder: lane.folder, manifest: lane.manifest,
    manifest_sha256: lane.manifestSha, checked_entries: entries.length, saved_git_mismatches: 0 };
}
const provenance = [checkPacket(report), checkPacket(money)];
const reportSource = readPinned(report, 'construction/workspace.report-d10-population-forward.sqlinc').toString('utf8');
const record = JSON.parse(readPinned(money, 'COMPLETE_CURRENT_REVERSES.json')).records
  .find(value => value.file === '02_complete_office_workspace.sqlinc');
if (record?.signature !== 'public.weekly_source_office_workspace_v1(jsonb)' || record.hunks.length !== 6)
  throw new Error('PAID_MATCH_WHOLE_OWNER_RECORD_MISMATCH');
const result = composeOwnerSource({ source: reportSource,
  expectedSha256: 'e46d0fc92985d330fcf01d4464a327f169c98b33d99da8965443a52bc23fd9ef', hunks: record.hunks });
// Prove the shared untouched base still is the reviewed current root owner.
const rootBefore = readPinned(report, 'construction/workspace.current-before.sqlinc');
if (sourceSha256(rootBefore) !== record.before_sha256) throw new Error('COMMON_CURRENT_OWNER_MISMATCH');
const outputFolder = path.join(root, 'docs/healthroster-build/proposals/office-report-d10-paid-match-union-20261014');
if (fs.existsSync(outputFolder)) throw new Error('FROZEN_UNION_FOLDER_ALREADY_EXISTS');
const files = new Map([
  ['forward/workspace.report-d10-paid-match.sqlinc', result.output],
  ['forward/scopes.report-d10.sqlinc', readPinned(report, 'construction/scopes.report-d10-population-forward.sqlinc')],
  ['forward/paid-match-question.sqlinc', readPinned(money, 'forward/01_paid_match_question.sqlinc')],
  ['forward/workspace-acl.sqlinc', readPinned(money, 'forward/03_office_workspace_acl.sqlinc')],
  ['reference/workspace.report-d10-before.sqlinc', reportSource],
  ['reference/workspace.current-root-before.sqlinc', rootBefore],
  ['COMPLETE_CURRENT_REVERSE.json', JSON.stringify(result.reverse, null, 2) + '\n'],
  ['SOURCE_INPUTS.json', JSON.stringify({ source_only: true, native: false, provenance,
    common_root_commit: record.commit, signature: record.signature, independent_union_review_pending: true,
    canonical_mounted: false, native_proof: false, source_only_reversal_proved: true }, null, 2) + '\n'],
  ['README.md', '# Report, D10 and paid-match Office workspace union\n\n'
    + 'Unmounted source composition of the independently reviewed Report32 and Money28 packets. '
    + 'Applies the six exact paid-match edits to the complete Report/D10 workspace. '
    + 'Its whole reverse reproduces the Report input exactly; both packets match their immutable Git commits and saved files.\n\n'
    + 'Install the question helper before the whole workspace and reassert its existing service-only ACL after it. '
    + 'The preserved scopes owner depends on the current native D10 group ABI. '
    + 'No new store, column, public signature, financial classifier or permission is introduced by this union.\n\n'
    + 'Source reversal is not PostgreSQL compilation, native first-use, final financial/security/UI acceptance or Part B sign-off. '
    + 'Original source reviews remain bounded; the union needs independent review and final native composition.\n']
]);
// Recheck both complete packets immediately before writing the new proposal.
checkPacket(report); checkPacket(money);
fs.mkdirSync(outputFolder, { recursive: true });
for (const [file, raw] of files) {
  const destination = path.join(outputFolder, file);
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.writeFileSync(destination, raw, { flag: 'wx' });
}
const manifest = { schema: 'HEALTHROSTER_OFFICE_WORKSPACE_UNION_SOURCE_MANIFEST_V1', source_only: true,
  native: false, independent_review_pending: true, entries: [...files].map(([file, raw]) => ({
    path: file, bytes: Buffer.byteLength(raw), sha256: sourceSha256(raw) })) };
fs.writeFileSync(path.join(outputFolder, 'SOURCE_MANIFEST.json'), JSON.stringify(manifest, null, 2) + '\n', { flag: 'wx' });
console.log(JSON.stringify({ outputFolder, source_only: true, native: false, files: files.size + 1,
  whole_owner_sha256: result.reverse.after_sha256, whole_reverse_verified: true,
  source_packets: provenance, SQL_executed: false }));
