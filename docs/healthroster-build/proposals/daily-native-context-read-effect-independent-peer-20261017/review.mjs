// Finite read-only source peer. No database, network, SQL execution or root write.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const own = 'D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026';
const root = 'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const money = 'D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026';
const commit = '28457faeab3269ab65dbd2df92f6b04ee074cb41';
const prefix = 'docs/healthroster-build/proposals/daily-native-context-read-effect-closure-20261017';
const manifestPin = '0977b1340702d8a1ec0a752be064801fee2af368b138a8a4b2381d793e252487';
const sha = value => createHash('sha256').update(value).digest('hex');
const json = file => JSON.parse(fs.readFileSync(file));
const git = (where, ...args) => execFileSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false', '-C', where, ...args], { maxBuffer: 32e6 });
const normal = value => value.replaceAll('\\', '/');
assert(normal(fs.realpathSync.native(here)).startsWith(own + '/'), 'OWN_PHYSICAL_D_REQUIRED');
for (const directory of [own, root, money]) {
  assert.equal(normal(fs.realpathSync.native(directory)), directory);
  for (const value of git(directory, 'rev-parse', '--show-toplevel', '--absolute-git-dir', '--git-common-dir').toString().trim().split(/\r?\n/)) {
    assert(normal(path.resolve(directory, value)).startsWith('D:/Development/GitHub/'), 'PHYSICAL_GIT_D_REQUIRED');
  }
}
const record = process.argv.includes('--record');
assert(process.argv.slice(2).every(value => value === '--record'), 'NO_EXTRA_RUNNER_ARGUMENTS');
if (record) assert(!fs.existsSync(here + '/MANIFEST.json'), 'NO_FROZEN_EVIDENCE_OVERWRITE');
if (fs.existsSync(here + '/MANIFEST.json')) {
  for (const row of json(here + '/MANIFEST.json').files) {
    const raw = fs.readFileSync(here + '/' + row.file);
    assert.equal(raw.length, row.bytes, row.file);
    assert.equal(sha(raw), row.sha256, row.file);
  }
}

const frozen = here + '/frozen';
const originalManifest = fs.readFileSync(frozen + '/MANIFEST.json');
assert.equal(sha(originalManifest), manifestPin);
const candidateRows = [...JSON.parse(originalManifest).files, { file: 'MANIFEST.json', bytes: originalManifest.length, sha256: manifestPin }];
for (const row of candidateRows) {
  const raw = fs.readFileSync(frozen + '/' + row.file);
  assert.equal(raw.length, row.bytes, row.file);
  assert.equal(sha(raw), row.sha256, row.file);
  assert(raw.equals(git(money, 'show', commit + ':' + prefix + '/' + row.file)), 'EXACT_COMMITTED_CANDIDATE:' + row.file);
  assert(raw.equals(fs.readFileSync(money + '/' + prefix + '/' + row.file)), 'MONEY_SAVED_IMMUTABLE:' + row.file);
  assert(raw.equals(fs.readFileSync(root + '/' + prefix + '/' + row.file)), 'ROOT_COPIED_IMMUTABLE:' + row.file);
}
const selection = json(frozen + '/SOURCE_SELECTION.json');
const inversePins = [];
for (const row of selection.captures) {
  const parent = fs.readFileSync(root + '/' + row.file);
  assert.equal(sha(parent), row.fileSha256);
  for (const revision of [selection.rootHead + ':' + row.file, 'HEAD:' + row.file, ':' + row.file]) {
    assert(parent.equals(git(root, 'show', revision)), 'COMPLETE_CURRENT_PARENT:' + row.identity);
  }
  const text = parent.toString();
  const capture = fs.readFileSync(frozen + '/' + row.capture);
  assert(Buffer.from(text.slice(row.start, row.end)).equals(capture));
  assert(Buffer.from(text.slice(0, row.start) + capture.toString() + text.slice(row.end)).equals(parent), 'LITERAL_IDENTITY_RECONSTRUCTION');
  if (row.bodyRawSha256) {
    const body = capture.toString().match(/\bas\s+(\$[a-zA-Z_0-9]*\$)([\s\S]*?)\1\s*;?\s*$/i);
    assert(body);
    assert.equal(sha(body[2]), row.bodyRawSha256);
    assert.equal(Buffer.byteLength(body[2]), row.bodyRawBytes);
  }
  inversePins.push({ identity: row.identity, file: row.file, completeParentSha256: row.fileSha256, rawCharacterRange: [row.start, row.end], completeStatementSha256: row.statementSha256, completeBodyRawSha256: row.bodyRawSha256 ?? null, capture: row.capture, transformation: 'NONE', reconstructedWholeParentSha256: sha(parent) });
}
for (const row of json(frozen + '/EXTERNAL_EVIDENCE_PINS.json').rows) {
  const raw = fs.readFileSync(frozen + '/' + row.to);
  assert.equal(raw.length, row.bytes);
  assert.equal(sha(raw), row.sha256);
  assert(raw.equals(git(row.root, 'show', row.commit + ':' + row.file)));
}
const documentary = json(frozen + '/DOCUMENTARY_CONTRACT_METADATA.json').source;
const contract = fs.readFileSync(root + '/' + documentary.file);
assert.equal(contract.length, documentary.bytes);
assert.equal(sha(contract), documentary.sha256);
assert(contract.equals(git(root, 'show', selection.rootHead + ':' + documentary.file)));

// Actual current managed loader, including uppercase .SQL. Declaration headers,
// raw owner ranges and hashes only outside the 18 expressly selected bodies.
const helperFiles = ['scripts/cloudtms-db-release-lib.mjs', 'scripts/cloudtms-managed-release-sql.mjs', 'scripts/bpay-next-final-definers.mjs'];
const helperPins = helperFiles.map(file => ({ file, sha256: sha(fs.readFileSync(root + '/' + file)) }));
const release = await import(pathToFileURL(root + '/' + helperFiles[0]));
const managed = await import(pathToFileURL(root + '/' + helperFiles[1]));
const definers = await import(pathToFileURL(root + '/' + helperFiles[2]));
const inventory = release.inventory();
assert.equal(inventory.migrations.length, 426);
assert.equal(inventory.repeatables.length, 1096);
const uppercase = inventory.migrations.filter(row => row.path.endsWith('.SQL')).map(row => row.path).sort();
assert(uppercase.some(file => file.endsWith('13072026_1648_MIGRATION.SQL')));
assert(uppercase.some(file => file.endsWith('26052026_1525_INDEX_OUTBOX.SQL')));
const selectedNames = new Set(selection.captures.map(row => row.identity));
const final = new Map();
const visited = new Set();
function scan(file, releaseRoot = file, ancestors = []) {
  assert(!ancestors.includes(file), 'INCLUDE_CYCLE');
  assert(file.startsWith('supabase/') && !file.includes('..'), 'BOUNDED_SQL_PATH');
  visited.add(file);
  const raw = fs.readFileSync(root + '/' + file), rawText = raw.toString(), source = rawText.replaceAll('\r\n', '\n');
  const offsets = [];
  for (let i = 0; i < rawText.length; i++) { offsets.push(i); if (rawText[i] === '\r' && rawText[i + 1] === '\n') i++; }
  offsets.push(rawText.length);
  const parsed = managed.scanManagedSql(source);
  const events = [...parsed.statements.map(row => ({ ...row, kind: 'statement' })), ...parsed.meta.filter(row => /^\\ir\s/i.test(row.text)).map(row => ({ ...row, kind: 'include' }))].sort((a, b) => a.start - b.start);
  for (const event of events) {
    if (event.kind === 'include') {
      const match = event.text.match(/^\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/i);
      assert(match);
      scan(path.posix.normalize(path.posix.join(path.posix.dirname(file), match[1] ?? match[2] ?? match[3])), releaseRoot, [...ancestors, file]);
      continue;
    }
    const text = source.slice(event.start, event.end), start = offsets[event.start], end = offsets[event.end];
    for (const identity of definers.routineDefinitions(text, file)) {
      if (selectedNames.has(identity)) final.set(identity, { file, releaseRoot, start, end, statementSha256: sha(rawText.slice(start, end)), fileSha256: sha(raw) });
    }
    const tokens = event.tokens, at = tokens.indexOf('VIEW');
    if (tokens[0] === 'CREATE' && at >= 1 && at <= 4) {
      let index = at + 1;
      if (tokens[index] === 'IF') index += 3;
      const identity = tokens.slice(index, index + 3).join('').toLowerCase();
      if (selectedNames.has(identity)) final.set(identity, { file, releaseRoot, start, end, statementSha256: sha(rawText.slice(start, end)), fileSha256: sha(raw) });
    }
  }
}
for (const filename of fs.readdirSync(root + '/supabase/baseline').filter(file => /\.sql$/i.test(file)).sort()) scan('supabase/baseline/' + filename);
for (const row of [...inventory.migrations, ...inventory.repeatables]) scan(row.path);
assert.equal(final.size, 18);
for (const row of selection.captures) {
  assert.deepEqual(final.get(row.identity), Object.fromEntries(['file', 'releaseRoot', 'start', 'end', 'statementSha256', 'fileSha256'].map(key => [key, row[key]])), 'ACTUAL_FINAL_MANAGED_OWNER:' + row.identity);
}

const output = execFileSync('python', [here + '/review.py'], { encoding: 'utf8', maxBuffer: 8e6, env: { ...process.env, PYTHONDONTWRITEBYTECODE: '1' } });
const results = JSON.parse(output);
const proof = { version: 'DAILY_CONTEXT_INDEPENDENT_RAW_REVIEW_V1', candidateCommit: commit, candidateManifestSha256: manifestPin, candidateFiles: candidateRows.length, selectedWholeRawOwners: 18, selectedRootCommit: selection.rootHead, selectedSourceSavedIndexCurrentHeadEqual: true, literalWholeParentIdentityReconstructions: 18, currentManagedInventory: { migrations: 426, repeatables: 1096, uppercaseMigrationPaths: uppercase, declarationFilesVisited: visited.size, finalSelectedOwners: final.size }, helperPins, originalProgrammesReplayed: 3, originalTests: 20, independentTests: results.independentTests, bankBodiesRead: 0, rootWrites: 0, runtimeTargets: [], native: 'NOT_RUN' };
const inverses = { version: 'DAILY_CONTEXT_SOURCE_IDENTITY_INVERSE_PINS_V1', runtimeTransformations: 0, runtimeReverse: 'NOT_APPLICABLE_NO_PRODUCT_OWNER_CHANGED', selectedRootCommit: selection.rootHead, rows: inversePins };
if (record) {
  for (const [file, value] of [['RESULTS.json', results], ['REVIEW_PROOF.json', proof], ['INVERSE_PINS.json', inverses]]) fs.writeFileSync(here + '/' + file, JSON.stringify(value, null, 2) + '\n');
} else {
  for (const [file, value] of [['RESULTS.json', results], ['REVIEW_PROOF.json', proof], ['INVERSE_PINS.json', inverses]]) assert.deepEqual(json(here + '/' + file), value, 'FROZEN_REPLAY:' + file);
}
console.log(JSON.stringify({ disposition: results.disposition, candidateFiles: candidateRows.length, rawOwners: 18, fragments: 283, actualCallSites: results.parser.callSites, originalTests: 20, independentTests: results.independentTests, managedMigrations: 426, managedRepeatables: 1096, reusableGuardCounterexamples: results.reusableGuardLimitations.length, rootObservedHead: git(root, 'rev-parse', 'HEAD').toString().trim(), native: 'NOT_RUN' }));
