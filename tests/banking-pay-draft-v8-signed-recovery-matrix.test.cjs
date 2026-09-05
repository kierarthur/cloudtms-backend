const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const readJson = relativePath => JSON.parse(read(relativePath));
const sha256 = text => crypto.createHash('sha256').update(text).digest('hex');
const contract = JSON.parse(read('tests/fixtures/banking-pay-draft-v8-signed-recovery-cases-v1.json'));
const results = readJson('tests/fixtures/banking-pay-draft-v8-signed-recovery-results-v1.json');
const runner = read('scripts/verify-banking-pay-draft-v8-signed-recovery-matrix.mjs');
const clone = value => JSON.parse(JSON.stringify(value));

function validateResults(candidate) {
  assert.equal(candidate.contract, 'BANKING_PAY_DRAFT_V8_SIGNED_RECOVERY_RESULTS_V1');
  assert.equal(candidate.status, 'CURRENT_V8_POLICY_OWNER_PASS_V1_PARITY_OPEN');
  assert.match(candidate.v1_v8_typed_parity_status, /^OPEN_/);
  for (const [sourcePath, expectedHash] of Object.entries(candidate.source_sha256)) {
    assert.equal(sha256(read(sourcePath)), expectedHash, `${sourcePath} changed without result review`);
  }
  assert.deepEqual(candidate.engines.map(row => row.engine_id), ['PG17_11','PG18_6']);
  for (const engine of candidate.engines) {
    assert.equal(engine.case_count, 5);
    assert.equal(engine.pass_count, 5);
    assert.equal(engine.fail_count, 0);
    assert.ok(engine.elapsed_ms > 0 && engine.elapsed_ms < 15000);
  }
  assert.deepEqual(candidate.class_results.map(row => row.class_id), contract.cases.map(row => row.class_id));
  assert.deepEqual(candidate.class_results.map(row => row.marker), contract.cases.map(row => row.expected_marker));
  assert.deepEqual(candidate.class_results.map(row => row.proof_kind), [
    'FULL_TYPED_CURRENT_V8_POLICY_OWNER','FULL_TYPED_CURRENT_V8_POLICY_OWNER',
    'FULL_TYPED_CURRENT_V8_POLICY_OWNER','TYPED_FAIL_CLOSED_ZERO_WRITE','TYPED_FAIL_CLOSED_ZERO_WRITE'
  ]);
  assert.deepEqual(candidate.summary, {
    p5_classes: 5,full_typed_current_v8_policy_owner_pass: 3,
    typed_fail_closed_zero_write_pass: 2,full_typed_v1_v8_parity_pass: 0
  });
  assert.equal(candidate.execution_contract.transaction_outcome, 'ROLLBACK');
  assert.equal(candidate.execution_contract.external_payment_actions, 0);
  assert.equal(candidate.execution_contract.materialised_50000_rows, false);
  return true;
}

test('P5 declares five distinct sealed signed-recovery outcomes without claiming V1 parity', () => {
  assert.equal(contract.contract, 'BANKING_PAY_DRAFT_V8_SIGNED_RECOVERY_CASES_V1');
  assert.equal(contract.status, 'RUNTIME_PROOF_PENDING');
  assert.match(contract.v1_v8_typed_parity_status, /^OPEN_/);
  assert.deepEqual(contract.cases.map(row => row.class_id), [
    'signed_positive_return','signed_negative_recovery','signed_mixed_ordinary_same_key',
    'signed_two_decisive_matches','signed_tampered_or_incomplete'
  ]);
  assert.equal(new Set(contract.cases.map(row => row.expected_marker)).size, 5);
  assert.equal(contract.execution_contract.materialised_50000_rows, false);
  assert.equal(contract.execution_contract.statement_timeout_ms, 15000);
  assert.equal(contract.execution_contract.lock_timeout_ms, 1500);
  assert.equal(contract.execution_contract.idle_in_transaction_session_timeout_ms, 30000);
});

test('P5 composes the established finalizer and classifier fixtures with fail-closed mutations', () => {
  for (const source of contract.source_fixtures) assert.ok(fs.existsSync(path.join(root, source)), source);
  for (const marker of [
    'H2_V8_SIGNED_RECOVERY_PASS=signed_positive_return',
    'H2_V8_SIGNED_RECOVERY_PASS=signed_negative_recovery',
    'H2_V8_SIGNED_RECOVERY_PASS=signed_mixed_ordinary_same_key',
    'H2_V8_SIGNED_RECOVERY_REJECT=signed_two_decisive_matches',
    'H2_V8_SIGNED_RECOVERY_REJECT=signed_tampered_or_incomplete',
    "exception when sqlstate '23514'",
    'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',
    'H2_P5_REJECTION_MUTATED_DRAFT',
    'H2_P5_BUDGET_RELAXATION',
    'EXACT_LOCAL_DATABASE_GUARD_SUBSTITUTION_ONLY'
  ]) assert.ok(runner.includes(marker), marker);
  assert.doesNotMatch(runner, /generate_series\s*\(\s*1\s*,\s*50000\s*\)/i);
  assert.doesNotMatch(runner, /v1_v8_typed_parity_status:\s*'PARITY_PASS'/);
});

test('P5 records exact dual-engine results and rejects false parity or weakened negatives', () => {
  assert.equal(validateResults(results), true);

  const falseParity = clone(results);
  falseParity.v1_v8_typed_parity_status = 'PARITY_PASS';
  falseParity.summary.full_typed_v1_v8_parity_pass = 5;
  assert.throws(() => validateResults(falseParity));

  const weakenedNegative = clone(results);
  weakenedNegative.class_results[3].proof_kind = 'FULL_TYPED_CURRENT_V8_POLICY_OWNER';
  assert.throws(() => validateResults(weakenedNegative));

  const omitted = clone(results);
  omitted.class_results.pop();
  assert.throws(() => validateResults(omitted));
});
