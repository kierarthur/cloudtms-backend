const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const monolithName = '26052026_2100HRS_NEW_FUNCTIONS.sql';
const reassertName = '08082026_0902_reassert_authorities_after_legacy_monolith.sql';
const monolith = fs.readFileSync(path.join(repeatableDir, monolithName), 'utf8');
const reassert = fs.readFileSync(path.join(repeatableDir, reassertName), 'utf8');
const targetedManifest = JSON.parse(fs.readFileSync(
  path.join(root, 'supabase', 'verification', 'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json'),
  'utf8',
));
const criticalAuthorityFiles = [
  '07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
];

const dateKey = (name) => {
  const match = name.match(/^(\d{2})(\d{2})(\d{4})[_-]?(\d{2})?(\d{2})?/);
  if (!match) return '999999999999';
  return `${match[3]}${match[2]}${match[1]}${match[4] || '00'}${match[5] || '00'}`;
};

const createdFunctions = (source) => [...source.matchAll(
  /CREATE\s+OR\s+REPLACE\s+FUNCTION\s+(?:(public|private)\.)?([a-zA-Z0-9_]+)\s*\(/gi,
)].map((match) => `${(match[1] || 'public').toLowerCase()}.${match[2].toLowerCase()}`);

const touchedFunctions = (source) => [...source.matchAll(
  /(?:CREATE\s+OR\s+REPLACE|ALTER|DROP)\s+FUNCTION(?:\s+IF\s+EXISTS)?\s+(?:(public|private)\.)?([a-zA-Z0-9_]+)\s*\(/gi,
)].map((match) => `${(match[1] || 'public').toLowerCase()}.${match[2].toLowerCase()}`);

test('legacy monolith changes force a complete later-authority replay', () => {
  const monolithHash = crypto.createHash('sha256').update(monolith).digest('hex');
  assert.match(reassert, new RegExp(`legacy_monolith_sha256:\\s*\\n-- ${monolithHash}`));

  const replayedFunctions = new Set(createdFunctions(monolith));
  const laterFiles = fs.readdirSync(repeatableDir)
    .filter((name) => name.endsWith('.sql'))
    .filter((name) => name !== monolithName && name !== reassertName)
    .filter((name) => dateKey(name) > dateKey(monolithName))
    .sort((left, right) => dateKey(left).localeCompare(dateKey(right)) || left.localeCompare(right));

  // Follow the complete authority chain, not only direct overlaps with the
  // legacy omnibus. A replayed intermediate file can define another function
  // whose newer focused authority must then also be replayed.
  const expectedFiles = [];
  for (const name of laterFiles) {
    const source = fs.readFileSync(path.join(repeatableDir, name), 'utf8');
    const identities = touchedFunctions(source);
    if (!identities.some((identity) => replayedFunctions.has(identity))) continue;

    expectedFiles.push(name);
    for (const identity of identities) replayedFunctions.add(identity);
  }
  for (const name of criticalAuthorityFiles) {
    assert.ok(laterFiles.includes(name), 'missing critical authority file: ' + name);
    if (!expectedFiles.includes(name)) expectedFiles.push(name);
  }
  expectedFiles.sort((left, right) => dateKey(left).localeCompare(dateKey(right)) || left.localeCompare(right));

  const includedFiles = [...reassert.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  assert.deepEqual(includedFiles, expectedFiles);
  assert.equal(new Set(includedFiles).size, includedFiles.length);
  assert.doesNotMatch(reassert, /\\ir\s+26052026_2100HRS_NEW_FUNCTIONS\.sql/i);
});


test('reconciliation authorities are explicitly replayed and catalogued', () => {
  const includedFiles = [...reassert.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  assert.ok(includedFiles.includes(criticalAuthorityFiles[0]));
  assert.equal(targetedManifest.function_count, targetedManifest.functions.length);

  const mainOwner = targetedManifest.functions.find((entry) =>
    entry.schema === 'private' &&
    entry.name === 'pay_sync_overpayments_from_workbench_workspace_v1');
  assert.ok(mainOwner);
  assert.equal(mainOwner.security_definer, false);
  assert.deepEqual(mainOwner.proconfig, ['search_path=""', 'plpgsql_check.mode=disabled']);
  assert.deepEqual(mainOwner.source_files, [
    'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
  ]);

  const residualOwner = targetedManifest.functions.find((entry) =>
    entry.schema === 'public' &&
    entry.name === '_ctms_candidate_correction_residuals_v1');
  assert.ok(residualOwner);
  assert.equal(residualOwner.identity_arguments,
    'p_session_id uuid, p_candidate_id uuid, p_exclude_pay_batch_id uuid, p_context text');
  assert.equal(residualOwner.security_definer, true);
  assert.deepEqual(residualOwner.proconfig, [
    'search_path=public, extensions, pg_temp',
    'plpgsql_check.mode=disabled',
  ]);
  assert.deepEqual(residualOwner.expanded_acl, [{
    grantee: 'postgres',
    grantor: 'postgres',
    is_grantable: false,
    privilege_type: 'EXECUTE',
  }]);
  assert.deepEqual(residualOwner.source_files, [
    'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
  ]);
});
