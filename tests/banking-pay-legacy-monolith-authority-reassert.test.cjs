const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const monolithName = '26052026_2100HRS_NEW_FUNCTIONS.sql';
const reassertName = '08082026_0902_reassert_authorities_after_legacy_monolith.sql';
const currentClosureName = '29082026_0326_banking_pay_release_authority_repair_v1.sql';
const normalizeLf = (value) => String(value || '').replace(/\r\n/g, '\n');
const monolith = normalizeLf(fs.readFileSync(path.join(repeatableDir, monolithName), 'utf8'));
const reassert = normalizeLf(fs.readFileSync(path.join(repeatableDir, reassertName), 'utf8'));
const currentClosure = normalizeLf(fs.readFileSync(path.join(repeatableDir, currentClosureName), 'utf8'));
const targetedManifest = JSON.parse(fs.readFileSync(
  path.join(root, 'supabase', 'verification', 'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json'),
  'utf8',
));
const criticalAuthorityFiles = [
  '07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
];
const finalAuthorityFiles = [
  '19072026_1816_cancel_refresh_supersede_finance_dirty.sql',
];
const incrementalReplayDependencies = [
  '21072026_1235_00b_import_correction_runtime_guards.sql',
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
    .filter((name) => name !== monolithName && name !== reassertName && name !== currentClosureName)
    .filter((name) => dateKey(name) > dateKey(monolithName))
    .sort((left, right) => dateKey(left).localeCompare(dateKey(right)) || left.localeCompare(right));

  // Follow the complete authority chain, not only direct overlaps with the
  // legacy omnibus. A replayed intermediate file can define another function
  // whose newer focused authority must then also be replayed.
  const expectedFiles = [];
  for (const name of laterFiles) {
    const source = normalizeLf(fs.readFileSync(path.join(repeatableDir, name), 'utf8'));
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
  for (const name of finalAuthorityFiles) {
    assert.ok(laterFiles.includes(name), 'missing final authority file: ' + name);
    const existingIndex = expectedFiles.indexOf(name);
    if (existingIndex >= 0) expectedFiles.splice(existingIndex, 1);
    expectedFiles.push(name);
  }

  const historicalIncludedFiles = [...reassert.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  const currentIncludedFiles = [...currentClosure.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  const completeIncludedFiles = new Set([...historicalIncludedFiles, ...currentIncludedFiles]);

  assert.deepEqual(
    expectedFiles.filter((name) => !completeIncludedFiles.has(name)),
    [],
    'the immutable historical replay plus current closure must cover every later authority',
  );
  assert.equal(new Set(historicalIncludedFiles).size, historicalIncludedFiles.length);
  assert.equal(new Set(currentIncludedFiles).size, currentIncludedFiles.length);
  assert.doesNotMatch(reassert, /\\ir\s+26052026_2100HRS_NEW_FUNCTIONS\.sql/i);
  assert.equal(
    crypto.createHash('sha256').update(reassert).digest('hex'),
    '3483e69bbc1ca13ba151b75b59e7b8e192f96f9df2627b829340b4d4e50d62c5',
    'the historical compatibility replay must remain byte-for-byte immutable',
  );
  for (const required of [
    '04082026_1219_pay_workbench_mark_finance_case_dirty.sql',
    '07082026_2224_candidate_app_weekly_office_replacements_v1.sql',
    '14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql',
    '27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql',
    '28082026_1424_banking_pay_modal_selection_owner_bridge.sql',
  ]) assert.ok(currentIncludedFiles.includes(required), `missing current authority closure: ${required}`);
  for (const forbidden of [
    '20072026_0302_disable_plpgsql_check_for_reservation_finalizer.sql',
    '23072026_1402_disable_plpgsql_check_for_banking_cancel.sql',
    '26052026_2100HRS_NEW_FUNCTIONS.sql',
  ]) assert.ok(!currentIncludedFiles.includes(forbidden), `unsafe historical replay in current closure: ${forbidden}`);
});

test('changed earlier overlapping authorities force the final reassertion to replay', () => {
  for (const name of incrementalReplayDependencies) {
    const source = normalizeLf(fs.readFileSync(path.join(repeatableDir, name), 'utf8'));
    const sourceHash = crypto.createHash('sha256').update(source).digest('hex');
    const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    assert.match(
      reassert,
      new RegExp(`authority_dependency_sha256:\\s*${escapedName}\\s*\\n-- ${sourceHash}`),
    );
  }
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
