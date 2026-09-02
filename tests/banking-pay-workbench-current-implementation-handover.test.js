import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = relative => fs.readFileSync(path.join(repoRoot, relative), 'utf8');
const readJson = relative => JSON.parse(read(relative));
const sha256 = buffer => crypto.createHash('sha256').update(buffer).digest('hex');
const runGit = args => execFileSync('git', args, {
  cwd: repoRoot,
  encoding: 'utf8',
  maxBuffer: 32 * 1024 * 1024,
}).trim();

const packRoot = 'codex_outputs/h1-workbench-recovery-current-test-source';
const inventory = readJson(`${packRoot}/CURRENT_SOURCE_INVENTORY.json`);
const evidence = readJson(`${packRoot}/CURRENT_TEST_EVIDENCE.json`);
const miget = readJson(`${packRoot}/CURRENT_MIGET_READ_ONLY_SNAPSHOT.json`);
const rollback = readJson(`${packRoot}/CURRENT_ROLLBACK_MANIFEST.json`);
const checksums = readJson(`${packRoot}/CURRENT_CANDIDATE_CHECKSUMS.json`);
const handover = read(`${packRoot}/CURRENT_IMPLEMENTATION_HANDOVER.md`);

test('current H1 handover is anchored to an ancestor of the source under review', () => {
  assert.equal(inventory.base_commit, rollback.base_commit);
  assert.doesNotThrow(() => execFileSync('git', [
    'merge-base', '--is-ancestor', inventory.base_commit, 'HEAD',
  ], { cwd: repoRoot }));
  assert.equal(runGit(['rev-parse', `${inventory.base_commit}^{tree}`]), inventory.base_tree);
  assert.match(handover, /SOURCE IMPLEMENTATION COMPLETE/);
  assert.match(handover, /REBASED TEST RELEASE CANDIDATE \/ NOT PUSHED \/ NOT INSTALLED \/ NOT DEPLOYED/);
  assert.match(handover, /not a fault-free runtime verdict/i);
});

test('source inventory hashes every declared current implementation file', () => {
  for (const item of inventory.files) {
    const absolute = path.join(repoRoot, item.path);
    assert.ok(fs.existsSync(absolute), `missing inventory path: ${item.path}`);
    const bytes = fs.readFileSync(absolute);
    assert.equal(bytes.length, item.bytes, `byte count drift: ${item.path}`);
    assert.equal(sha256(bytes), item.sha256, `SHA-256 drift: ${item.path}`);
  }
  assert.equal(checksums.schema_version, 'H1_CURRENT_TEST_SOURCE_CHECKSUMS_V1');
  assert.equal(checksums.base_commit, inventory.base_commit);
  for (const item of checksums.artifacts) {
    const absolute = path.join(repoRoot, item.path);
    assert.ok(fs.existsSync(absolute), `missing checksum path: ${item.path}`);
    const bytes = fs.readFileSync(absolute);
    assert.equal(bytes.length, item.bytes, `checksum byte count drift: ${item.path}`);
    assert.equal(sha256(bytes), item.sha256, `checksum SHA-256 drift: ${item.path}`);
  }
});

test('generated contract changes exactly the three declared H1 definitions', () => {
  const candidate = readJson('supabase/release/current-contract.json');
  const base = JSON.parse(runGit(['show', `${inventory.base_commit}:supabase/release/current-contract.json`]));
  const canonical = value => sha256(Buffer.from(JSON.stringify(value)));
  assert.equal(canonical(base), inventory.base_contract_semantic_sha256);
  assert.equal(canonical(candidate), inventory.candidate_contract_semantic_sha256);
  assert.equal(candidate.routines.length, base.routines.length);

  const byIdentity = values => new Map(values.map(value => [`${value.schema}.${value.identity}`, value]));
  const before = byIdentity(base.routines);
  const after = byIdentity(candidate.routines);
  assert.deepEqual([...after.keys()], [...before.keys()]);

  const changed = [];
  for (const [identity, current] of after) {
    const prior = before.get(identity);
    if (JSON.stringify(current) === JSON.stringify(prior)) continue;
    const currentWithoutHash = { ...current };
    const priorWithoutHash = { ...prior };
    delete currentWithoutHash.definition_sha256;
    delete priorWithoutHash.definition_sha256;
    assert.deepEqual(currentWithoutHash, priorWithoutHash, `non-definition contract drift: ${identity}`);
    changed.push([identity, current.definition_sha256]);
  }
  assert.deepEqual(changed, [
    [
      'public.pay_workbench_fail_job(p_job_id uuid, p_error_json jsonb, p_retry_after_seconds integer)',
      inventory.contract_semantic_delta.routine_hashes.pay_workbench_fail_job,
    ],
    [
      'public.pay_workbench_repair_orphaned_pending_source_build(p_session_id uuid, p_candidate_id uuid, p_limit integer, p_now_utc timestamp with time zone, p_reason text)',
      inventory.contract_semantic_delta.routine_hashes.pay_workbench_repair_orphaned_pending_source_build,
    ],
    [
      'public.pay_workbench_source_build_attempt_claim_start_v1(p_worker_id text, p_lane_identity text, p_lease_seconds integer, p_now_utc timestamp with time zone, p_session_id uuid, p_candidate_id uuid)',
      inventory.contract_semantic_delta.routine_hashes.pay_workbench_source_build_attempt_claim_start_v1,
    ],
  ]);
});

test('causal verifier is mandatory in both release verification lists', () => {
  const release = readJson('supabase/release/current-release.json');
  const verifier = 'supabase/verification/01092026_1927_banking_pay_workbench_causal_recovery_verification.sql';
  assert.equal(release.verificationFiles.filter(value => value === verifier).length, 1);
  assert.equal(release.newVerificationFiles.filter(value => value === verifier).length, 1);
});

test('H1 implementation is policy-neutral and independent of Create Draft', () => {
  assert.deepEqual(evidence.policy_parity, {
    changes_policy: false,
    changes_create_draft_contract: false,
    changes_eligibility_or_amounts: false,
    changes_tax_or_vat: false,
    changes_pay_channel_or_method: false,
    changes_provider_or_settlement: false,
    changes_selection_authority: false,
    changes_workbench_recovery_execution: true,
    changes_final_bulk_authorise_behavior: false,
    changes_candidate_runtime_or_security_boundary: false,
  });
  const forbiddenProductionPaths = [
    'broker/src/index.js',
    'supabase/repeatable/21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql',
    'supabase/repeatable/02092026_1030_banking_pay_draft_scope_finance_constituent_handoff_v1.sql',
    'supabase/repeatable/02092026_1040_banking_pay_draft_insert_items_finance_handoff_v1.sql',
  ];
  const declared = new Set(inventory.files.map(item => item.path));
  for (const file of forbiddenProductionPaths) assert.equal(declared.has(file), false);
  assert.match(handover, /changes no Draft input, Draft output or Draft-produced fact/);
  assert.match(handover, /preserves Policy X/);
});

test('test evidence keeps green H1 scope separate from open external and environment gates', () => {
  assert.equal(evidence.results.focused_h1.pass, 93);
  assert.equal(evidence.results.focused_h1.fail, 0);
  assert.equal(evidence.results.current_handover_validator.pass, 8);
  assert.equal(evidence.results.current_handover_validator.fail, 0);
  assert.equal(evidence.results.complete_javascript.pass, 1170);
  assert.equal(evidence.results.complete_javascript.fail, 0);
  assert.equal(evidence.results.complete_cjs_matrix.h1_recovery_failures, 0);
  assert.equal(evidence.results.complete_cjs_matrix.fail, 0);
  assert.equal(evidence.results.complete_cjs_matrix.pre_existing_external_failures.length, 0);
  assert.equal(evidence.results.complete_cjs_matrix.companion_reconciliations.length, 2);
  assert.equal(evidence.results.fresh_dual_engine_execution.pass, false);
  assert.equal(evidence.results.fresh_dual_engine_execution.not_counted_as_pass, true);
  assert.equal(evidence.verdict, 'READY_FOR_INDEPENDENT_SOURCE_REVIEW_NOT_YET_A_FAULT_FREE_RUNTIME_VERDICT');
});

test('fresh Miget snapshot proves current authority and that H1 remains uninstalled', () => {
  assert.equal(miget.authority.environment, 'TEST');
  assert.equal(miget.authority.database_name, 'cloudtms_test_clone');
  assert.match(miget.authority.postgres_server_version, /^17\.11\b/);
  assert.equal(
    miget.latest_verified_release.repository_contract_sha256,
    miget.latest_verified_release.installed_contract_sha256,
  );
  assert.equal(miget.h1_owned_routines.length, 3);
  for (const routine of miget.h1_owned_routines) {
    assert.equal(routine.h1_installed, false);
    assert.notEqual(routine.installed_canonical_definition_sha256, routine.candidate_definition_sha256);
  }
});

test('rollback is evidence-preserving and never deletes database history', () => {
  const restored = new Set(rollback.modified_files.map(item => item.path));
  const omitted = new Set(rollback.new_files_to_omit_on_source_reversal);
  for (const item of inventory.files) {
    if (item.status.startsWith('modified')) {
      assert.ok(restored.has(item.path), `modified inventory path missing rollback blob: ${item.path}`);
    } else if (item.status === 'new') {
      assert.ok(omitted.has(item.path), `new inventory path missing rollback omission: ${item.path}`);
    }
  }
  assert.equal(rollback.database_reversal_if_later_installed.destructive_table_or_data_removal, false);
  assert.equal(
    rollback.database_reversal_if_later_installed.retain_historical_jobs_attempts_dead_rows_and_release_records,
    true,
  );
  assert.equal(rollback.worker_frontend_rollback, 'not applicable; this implementation changes no Worker or frontend runtime file');
  assert.equal(rollback.create_draft_rollback, 'not applicable; this implementation changes no Create Draft owner or contract');
});
