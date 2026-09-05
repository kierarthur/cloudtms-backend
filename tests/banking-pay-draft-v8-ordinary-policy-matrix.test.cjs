const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const readText = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const readJson = relativePath => JSON.parse(readText(relativePath));
const sha256 = text => crypto.createHash('sha256').update(text).digest('hex');
const casesPath = 'tests/fixtures/banking-pay-draft-v8-ordinary-policy-cases-v1.json';
const resultsPath = 'tests/fixtures/banking-pay-draft-v8-ordinary-policy-results-v1.json';
const cases = readJson(casesPath);
const results = readJson(resultsPath);

const clone = value => JSON.parse(JSON.stringify(value));

function validate(caseCandidate, resultCandidate) {
  assert.equal(caseCandidate.contract, 'BANKING_PAY_DRAFT_V8_ORDINARY_POLICY_CASES_V1');
  assert.equal(caseCandidate.evidence_scope, 'FULL_TYPED_CURRENT_V8_POLICY_OWNER');
  assert.match(caseCandidate.v1_v8_typed_parity_status, /^OPEN_/);
  assert.equal(caseCandidate.cases.length, 8);
  const caseIds = caseCandidate.cases.map(row => row.case_id);
  assert.equal(new Set(caseIds).size, 8);
  const classIds = caseCandidate.cases.flatMap(row => row.class_ids);
  assert.equal(classIds.length, 18);
  assert.equal(new Set(classIds).size, 18);
  assert.deepEqual(caseCandidate.explicit_open_classes.map(row => row.class_id).sort(), [
    'saved_payment_method_resolution','saved_rate_resolution'
  ]);
  assert.equal(caseCandidate.materialised_row_ceiling, 5000);
  assert.equal(caseCandidate.configured_scalar_ceiling, 50000);

  assert.equal(resultCandidate.contract, 'BANKING_PAY_DRAFT_V8_ORDINARY_POLICY_RESULTS_V1');
  assert.equal(resultCandidate.evidence_tier, 'FULL_TYPED_CURRENT_V8_POLICY_OWNER');
  assert.match(resultCandidate.v1_v8_typed_parity_status, /^OPEN_/);
  for (const [sourcePath, expectedHash] of Object.entries(resultCandidate.source_sha256)) {
    assert.equal(sha256(readText(sourcePath)), expectedHash, `${sourcePath} changed without result review`);
  }
  assert.deepEqual(resultCandidate.passed_class_ids, classIds);
  assert.deepEqual(resultCandidate.open_class_ids.sort(), caseCandidate.explicit_open_classes.map(row => row.class_id).sort());
  assert.deepEqual(resultCandidate.summary, {
    p1_classes: 20,
    full_typed_current_v8_policy_owner_pass: 18,
    open_exact_resolution_owner_fixture: 2,
    full_typed_v1_v8_parity_pass: 0
  });
  assert.equal(resultCandidate.engines.length, 2);
  assert.deepEqual(resultCandidate.engines.map(row => row.engine_id).sort(), ['PG17_11','PG18_6']);
  for (const engine of resultCandidate.engines) {
    assert.equal(engine.case_count, 8);
    assert.equal(engine.pass_count, 8);
    assert.equal(engine.fail_count, 0);
    assert.equal(engine.class_pass_count, 18);
    assert.deepEqual(Object.keys(engine.elapsed_ms_by_case), caseIds);
    assert.ok(Object.values(engine.elapsed_ms_by_case).every(value => value > 0 && value < 15000));
  }
  assert.deepEqual(resultCandidate.execution_contract, {
    transaction_outcome: 'ROLLBACK',
    external_payment_actions: 0,
    provider_actions: 0,
    settlement_actions: 0,
    remittance_actions: 0,
    statement_timeout_ms: 15000,
    lock_timeout_ms: 1500,
    idle_in_transaction_session_timeout_ms: 30000,
    jit: 'OFF',
    harness_isolation: 'EXACT_LOCAL_DATABASE_GUARD_SUBSTITUTION_ONLY',
    committed_fixture_unchanged: true,
    pre_insert_collision_guard_preserved: true,
    maximum_materialised_row_count: 11,
    prohibited_materialised_row_count: 50000
  });
  const runner = readText('scripts/verify-banking-pay-draft-v8-ordinary-policy-matrix.mjs');
  for (const marker of [
    'H2_P1_TYPED_DRAFT_OUTPUT_MISMATCH',
    'H2_P1_CHANNEL_VAT_POLICY_MISMATCH',
    'H2_V8_ORDINARY_POLICY_PASS=',
    'FULL_TYPED_CURRENT_V8_POLICY_OWNER',
    'EXACT_LOCAL_DATABASE_GUARD_SUBSTITUTION_ONLY'
  ]) assert.ok(runner.includes(marker), marker);
  assert.doesNotMatch(runner, /generate_series\s*\(\s*1\s*,\s*50000\s*\)/i);
}

test('P1 binds 18 exact current-V8 Draft outputs on PG17 and PG18 while leaving two saved-resolution gates open', () => {
  validate(cases, results);
});

test('P1 rejects omitted classes, false V1 parity and prohibited 50,000-row materialisation', () => {
  const omitted = clone(cases);
  omitted.cases[5].class_ids.pop();
  assert.throws(() => validate(omitted, results));

  const falseParity = clone(results);
  falseParity.v1_v8_typed_parity_status = 'PARITY_PASS';
  falseParity.summary.full_typed_v1_v8_parity_pass = 18;
  assert.throws(() => validate(cases, falseParity));

  const materialised50k = clone(results);
  materialised50k.execution_contract.maximum_materialised_row_count = 50000;
  assert.throws(() => validate(cases, materialised50k));
});
