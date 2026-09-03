import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(here, '..');
const evidencePath = path.join(
  repositoryRoot,
  'codex_outputs/h1-workbench-recovery-causal-v1/SHARED_BASELINE_RECONCILIATION_14dd89af.json'
);
const evidence = JSON.parse(fs.readFileSync(evidencePath, 'utf8'));
const installedSnapshotPath = path.join(
  repositoryRoot,
  'codex_outputs/h1-workbench-recovery-causal-v1/MIGET_INSTALLED_IDENTITY_SNAPSHOT_12ecbb56.json'
);
const installedSnapshot = JSON.parse(fs.readFileSync(installedSnapshotPath, 'utf8'));
const rollbackManifestPath = path.join(
  repositoryRoot,
  'codex_outputs/h1-workbench-recovery-causal-v1/ROLLBACK_SOURCE_MANIFEST_14dd89af.json'
);
const rollbackManifest = JSON.parse(fs.readFileSync(rollbackManifestPath, 'utf8'));
const candidateManagerEmailWorkflow = fs.readFileSync(
  path.join(repositoryRoot, '.github/workflows/candidate-manager-email-verify.yml'),
  'utf8'
);
const databaseSourceWorkflow = fs.readFileSync(
  path.join(repositoryRoot, '.github/workflows/supabase-migrate.yml'),
  'utf8'
);

function git(args, encoding = 'utf8') {
  return execFileSync('git', args, {
    cwd: repositoryRoot,
    encoding,
    maxBuffer: 4 * 1024 * 1024
  });
}

function contractAt(commit) {
  return JSON.parse(git(['show', `${commit}:supabase/release/current-contract.json`]));
}

function releaseAt(commit) {
  return JSON.parse(git(['show', `${commit}:supabase/release/current-release.json`]));
}

test('Candidate source verification fetches the historical commits required by this proof', () => {
  assert.match(
    candidateManagerEmailWorkflow,
    /uses:\s*actions\/checkout@v6\s*\n\s*with:\s*\n\s*fetch-depth:\s*0\b/
  );
});

test('database source verification fetches the historical commits required by this proof', () => {
  assert.match(
    databaseSourceWorkflow,
    /uses:\s*actions\/checkout@v6\s*\n\s*with:\s*(?:\n\s*#[^\n]*)*\n\s*fetch-depth:\s*0\b/
  );
});

test('shared successor has the exact proved ancestry and tree', () => {
  const format = git([
    'show', '-s', '--format=%H%n%T%n%P', evidence.shared_successor_commit
  ]).trim().split(/\r?\n/);
  assert.equal(format[0], evidence.shared_successor_commit);
  assert.equal(format[1], evidence.shared_successor_tree);
  assert.deepEqual(format.slice(2), evidence.shared_successor_parents);
  assert.deepEqual(evidence.shared_successor_parents, ['7811288c04fd0fc1427ab9d98e4f844e1c66079e']);
  for (const ancestor of evidence.proved_ancestry.slice(0, -1)) {
    assert.doesNotThrow(() => git(['merge-base', '--is-ancestor', ancestor, evidence.shared_successor_commit]));
  }
});

test('latest shared successor has the exact direct and cumulative Candidate-only path deltas', () => {
  const direct = git([
    'diff', '--name-only', `${evidence.previous_sealed_shared_source_commit}..${evidence.shared_successor_commit}`
  ]).trim().split(/\r?\n/).filter(Boolean);
  assert.deepEqual(direct, evidence.changes_from_previous_sealed_shared_source);
  const cumulative = git([
    'diff', '--name-only', `${evidence.previous_shared_database_commit}..${evidence.shared_successor_commit}`
  ]).trim().split(/\r?\n/).filter(Boolean);
  assert.deepEqual(cumulative, evidence.changes_from_database_installed_source);
  for (const item of evidence.direct_changed_source_objects) {
    const objectSpec = `${evidence.shared_successor_commit}:${item.path}`;
    assert.equal(git(['rev-parse', objectSpec]).trim(), item.git_blob_sha1);
    const body = git(['show', objectSpec], null);
    assert.equal(body.length, item.bytes);
    assert.equal(crypto.createHash('sha256').update(body).digest('hex'), item.content_sha256);
  }
});

test('latest Candidate successor has no H1, SQL, contract, Banking, finance or Draft overlap', () => {
  assert.deepEqual(evidence.h1_path_intersection_from_781_to_14dd, []);
  assert.deepEqual(evidence.h1_path_intersection_from_12ec_to_14dd, []);
  assert.deepEqual(evidence.sql_contract_banking_finance_draft_path_intersection_from_781_to_14dd, []);
  assert.deepEqual(evidence.sql_contract_banking_finance_draft_path_intersection_from_12ec_to_14dd, []);
  assert.equal(evidence.h1_held_paths_byte_identical_from_781_to_14dd, true);
  assert.equal(evidence.h1_held_paths_byte_identical_from_12ec_to_14dd, true);
});

test('the three H1 function entries are byte-semantically unchanged at 14dd', () => {
  const base = contractAt(evidence.previous_shared_database_commit);
  const successor = contractAt(evidence.shared_successor_commit);
  const expected = evidence.unchanged_h1_routine_hashes_at_shared_successor;
  for (const [identity, hash] of Object.entries(expected)) {
    const before = base.routines.find((entry) => `${entry.schema}.${entry.identity}` === identity);
    const after = successor.routines.find((entry) => `${entry.schema}.${entry.identity}` === identity);
    assert.ok(before, `missing base function ${identity}`);
    assert.ok(after, `missing successor function ${identity}`);
    assert.deepEqual(after, before);
    assert.equal(after.definition_sha256, hash);
  }
});

test('the shared contract still contains the preceding Candidate session-family authority', () => {
  const base = contractAt(evidence.h1_original_base_commit);
  const successor = contractAt(evidence.shared_successor_commit);
  const addedFunction = successor.routines.find((entry) => entry.identity.startsWith('candidate_app_federated_session_project_v2('));
  assert.ok(addedFunction);
  assert.equal(addedFunction.schema, 'public');
  assert.equal(addedFunction.security_definer, true);

  const baseSessions = base.relations.find((entry) => entry.schema === 'public' && entry.name === 'candidate_app_sessions');
  const successorSessions = successor.relations.find((entry) => entry.schema === 'public' && entry.name === 'candidate_app_sessions');
  assert.ok(baseSessions);
  assert.ok(successorSessions);
  assert.equal(baseSessions.columns.some(({ name }) => name === 'global_session_family_identity_hmac'), false);
  assert.equal(successorSessions.columns.some(({ name }) => name === 'global_session_family_identity_hmac'), true);
});

test('shared Candidate and H1 release entries remain disjoint', () => {
  const base = releaseAt(evidence.h1_original_base_commit);
  const successor = releaseAt(evidence.shared_successor_commit);
  const candidateVerifier = 'supabase/verification/01092026_2032_candidate_federated_session_family_projection_verification.sql';
  const h1Verifier = 'supabase/verification/01092026_1927_banking_pay_workbench_causal_recovery_verification.sql';
  for (const key of ['verificationFiles', 'newVerificationFiles']) {
    assert.equal(base[key].includes(candidateVerifier), false);
    assert.equal(successor[key].filter((entry) => entry === candidateVerifier).length, 1);
    assert.equal(successor[key].includes(h1Verifier), false);
  }
  assert.match(evidence.future_combined_candidate_rule, /apply H1 and accepted H2 deltas entry-by-entry/i);
});

test('recorded shared contract hashes match the exact fetched commit', () => {
  const raw = git([
    'show', `${evidence.shared_successor_commit}:supabase/release/current-contract.json`
  ], null);
  const fileHash = crypto.createHash('sha256').update(raw).digest('hex');
  const semanticHash = crypto.createHash('sha256').update(JSON.stringify(JSON.parse(raw))).digest('hex');
  assert.equal(fileHash, evidence.shared_source_contract_file_sha256);
  assert.equal(semanticHash, evidence.shared_source_contract_semantic_sha256);
});

test('fresh Miget baseline binds the exact database-installed 12ec release and unchanged contract', () => {
  assert.equal(installedSnapshot.label, 'CURRENT SHARED TEST BASELINE / NO H1 INSTALL / READ ONLY');
  assert.equal(installedSnapshot.authority.environment, 'TEST');
  assert.equal(installedSnapshot.authority.database_name, 'cloudtms_test_clone');
  assert.match(installedSnapshot.authority.postgres_server_version, /^17[.]/);
  assert.equal(installedSnapshot.authority.pg_show_plans_enabled, 'off');
  assert.equal(installedSnapshot.latest_verified_release.git_commit, evidence.database_installed_source_commit);
  assert.equal(installedSnapshot.latest_verified_release.status, 'VERIFIED');
  assert.equal(
    installedSnapshot.latest_verified_release.repository_contract_sha256,
    evidence.shared_source_contract_semantic_sha256
  );
  assert.equal(
    installedSnapshot.latest_verified_release.installed_contract_sha256,
    evidence.shared_source_contract_semantic_sha256
  );
});

test('fresh installed H1 routine identities remain pre-H1 and differ from the local candidate', () => {
  const expected = evidence.unchanged_h1_routine_hashes_at_shared_successor;
  assert.equal(installedSnapshot.h1_owned_routines.length, Object.keys(expected).length);
  for (const routine of installedSnapshot.h1_owned_routines) {
    assert.equal(routine.h1_installed, false);
    assert.equal(routine.canonical_definition_sha256, expected[routine.identity]);
    assert.notEqual(routine.h1_local_candidate_definition_sha256, routine.canonical_definition_sha256);
    assert.equal(routine.logical_contract_owner, 'postgres');
    assert.equal(routine.security_definer, true);
    assert.deepEqual(routine.provider_normalized_contract_acl, [
      { grantee: 'postgres', privilege: 'EXECUTE' }
    ]);
  }
});

test('rollback source manifest resolves every backend object to exact 14dd bytes', () => {
  assert.equal(rollbackManifest.source_commit, evidence.shared_successor_commit);
  assert.equal(rollbackManifest.source_tree, evidence.shared_successor_tree);
  for (const item of rollbackManifest.backend_source_objects) {
    const objectSpec = `${rollbackManifest.source_commit}:${item.path}`;
    assert.equal(git(['rev-parse', objectSpec]).trim(), item.git_blob_sha1);
    const body = git(['show', objectSpec], null);
    assert.equal(body.length, item.bytes);
    assert.equal(crypto.createHash('sha256').update(body).digest('hex'), item.content_sha256);
  }
  for (const item of rollbackManifest.preserved_non_h1_successor_source_objects) {
    const objectSpec = `${rollbackManifest.source_commit}:${item.path}`;
    assert.equal(git(['rev-parse', objectSpec]).trim(), item.git_blob_sha1);
    const body = git(['show', objectSpec], null);
    assert.equal(body.length, item.bytes);
    assert.equal(crypto.createHash('sha256').update(body).digest('hex'), item.content_sha256);
  }
});

test('reconciliation is ready for source review while runtime conclusions remain open', () => {
  assert.equal(evidence.artifact_status, 'LOCAL_SOURCE_HANDOVER_NOT_INSTALLED_NOT_DEPLOYED_NOT_FINAL_RUNTIME_CERTIFICATE');
  assert.equal(evidence.reconciliation_status, 'SOURCE_PROVED_NON_OVERLAP_INDEPENDENT_HANDOVER_READY');
  assert.match(evidence.independent_handover_rule, /exact shared source 14dd89af/i);
  assert.match(evidence.future_combined_candidate_rule, /exact 14dd89af/i);
  assert.ok(evidence.open_gates.includes('independent H1 source/local verifier verdict'));
});
