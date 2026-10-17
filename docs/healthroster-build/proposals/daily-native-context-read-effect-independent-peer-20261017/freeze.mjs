import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
const here = path.dirname(fileURLToPath(import.meta.url));
assert(fs.realpathSync.native(here).replaceAll('\\', '/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'));
assert(!fs.existsSync(here + '/MANIFEST.json'), 'IMMUTABLE_FREEZE_EXISTS');
const sha = value => createHash('sha256').update(value).digest('hex');
const files = [];
function collect(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    assert(!entry.isSymbolicLink());
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) collect(target);
    else {
      const file = path.relative(here, target).replaceAll('\\', '/');
      assert(!file.endsWith('.pyc'));
      const raw = fs.readFileSync(target);
      files.push({ file, bytes: raw.length, sha256: sha(raw) });
    }
  }
}
collect(here);
files.sort((a, b) => a.file.localeCompare(b.file));
const manifest = { version: 'DAILY_CONTEXT_READ_EFFECT_INDEPENDENT_PEER_FREEZE_V1', requiredModel: 'gpt-6.1-sol/xhigh', candidateCommit: '28457faeab3269ab65dbd2df92f6b04ee074cb41', candidateManifestSha256: '0977b1340702d8a1ec0a752be064801fee2af368b138a8a4b2381d793e252487', disposition: 'ACCEPT_BOUNDED_SOURCE_PLAN_WITH_REUSABLE_CLASSIFIER_LIMITATIONS', originalTests: 20, independentTests: 27, runtimeTargets: [], rootWrites: 0, native: 'NOT_RUN', bankBodiesRead: 0, files };
const raw = Buffer.from(JSON.stringify(manifest, null, 2) + '\n');
fs.writeFileSync(here + '/MANIFEST.json', raw);
console.log(JSON.stringify({ files: files.length + 1, manifestSha256: sha(raw), reviewProofSha256: sha(fs.readFileSync(here + '/REVIEW_PROOF.json')), inversePinsSha256: sha(fs.readFileSync(here + '/INVERSE_PINS.json')) }));
