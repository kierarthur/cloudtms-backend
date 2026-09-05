const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const readJson = relativePath => JSON.parse(fs.readFileSync(path.join(root, relativePath), 'utf8'));
const matrixPath = 'tests/fixtures/banking-pay-create-draft-v8-policy-parity-matrix-v1.json';
const matrix = readJson(matrixPath);
const policy = readJson('codex_outputs/banking-pay-create-draft-policy-v1/BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json');

test('the generated parity matrix is current and maps all 88 finite policy classes exactly once', () => {
  const generated = spawnSync(process.execPath, [
    path.join(root, 'scripts/generate-banking-pay-draft-v8-policy-parity-matrix.mjs'),
    '--check'
  ], { cwd: root, encoding: 'utf8' });
  assert.equal(generated.status, 0, generated.stderr || generated.stdout);
  assert.equal(matrix.source_policy_contract.payment_family_count, 15);
  assert.equal(matrix.source_policy_contract.finite_equivalence_class_count, 88);
  assert.equal(matrix.totals.mapped_classes, 88);
  assert.equal(matrix.totals.duplicate_assignments, 0);
  assert.equal(matrix.totals.missing_assignments, 0);
  assert.equal(matrix.totals.unknown_assignments, 0);
  const policyIds = policy.finite_equivalence_classes.map(row => row.class_id).sort();
  const matrixIds = matrix.class_groups.flatMap(group => group.class_ids).sort();
  assert.deepEqual(matrixIds, policyIds);
});

test('each class group is executable, source-bound and never totals-only evidence', () => {
  for (const group of matrix.class_groups) {
    assert.ok(group.class_count > 0, group.group_id);
    assert.match(group.execution_kind, /DIRECT|BOUNDED/);
    assert.ok(group.evidence_files.length >= 3, group.group_id);
    for (const evidence of group.evidence_files) {
      const absolutePath = path.join(root, evidence.path);
      assert.equal(fs.existsSync(absolutePath), true, evidence.path);
      assert.match(evidence.sha256, /^[0-9a-f]{64}$/);
    }
  }
  assert.doesNotMatch(matrix.policy_rule, /totals alone|totals-only/i);
  assert.match(matrix.policy_rule, /amount, sign, gross\/net, tax, VAT, channel/);
});

test('load proof stops at 5,000 rows while retaining lightweight 50,000 and 50,001 contract guards', () => {
  const scale = matrix.execution_efficiency;
  assert.deepEqual(scale.load_dataset_rows, [101, 1001, 5000]);
  assert.equal(scale.maximum_materialised_load_rows, 5000);
  assert.equal(scale.supported_configured_ceiling, 50000);
  assert.equal(scale.configured_ceiling_proof, 'SCALAR_SOURCE_AND_CONTRACT_ONLY_NO_50000_ROW_DATASET');
  assert.equal(scale.first_rejected_count, 50001);
  assert.equal(scale.first_rejected_count_proof, 'SCALAR_SOURCE_AND_CONTRACT_ONLY_ZERO_WRITES');
  assert.equal(scale.prohibited_test, 'DO_NOT_GENERATE_OR_EXECUTE_A_50000_ROW_LOAD_TEST');
});

test('paired Timesheets and all policy-sensitive categories have deliberate executable ownership', () => {
  const paired = matrix.class_groups.find(group => group.group_id === 'P2_PAIRED_TIMESHEET_LIFECYCLE');
  assert.ok(paired.class_ids.includes('paired_reversal_replacement_paye'));
  assert.ok(paired.class_ids.includes('paired_reversal_only_umbrella'));
  assert.ok(paired.class_ids.includes('paired_draft_response_loss_replay'));
  const finance = matrix.class_groups.find(group => group.group_id === 'P4_FINANCE_CATEGORIES_GROSS_NET_VAT_HEADROOM');
  for (const classId of [
    'advance_payout_paye', 'advance_repayment_paye',
    'overpayment_taxable_paye', 'overpayment_nontaxable_paye',
    'underpayment_taxable_paye', 'manual_credit_umbrella',
    'manual_debt_nontaxable_paye', 'mixed_recoveries_deterministic_order'
  ]) assert.ok(finance.class_ids.includes(classId), classId);
  assert.match(finance.purpose, /PAYE gross\/net, Umbrella VAT/);
});

test('bounded property and mutation coverage is finite, recorded and fail closed', () => {
  assert.equal(new Set(matrix.recorded_property_seeds).size, matrix.recorded_property_seeds.length);
  assert.ok(matrix.recorded_property_seeds.length >= 4);
  assert.ok(matrix.bounded_property_dimensions.length >= 8);
  assert.ok(matrix.mutation_operators.length >= 14);
  assert.deepEqual(matrix.generated_property_cases, {
    accepted: 48,
    rejected: 48,
    total: 96,
    execution_test: 'tests/banking-pay-draft-v8-bounded-property.test.cjs'
  });
  assert.deepEqual(matrix.mutation_evidence, {
    execution_test: 'tests/banking-pay-draft-v8-mutation.test.cjs',
    operators_total: 14,
    killed: 14,
    surviving: 0,
    status: 'LOCAL_EXECUTION_REQUIRED_BEFORE_RELEASE'
  });
  for (const operator of [
    'CHANGE_AMOUNT_BY_ONE_PENNY', 'CHANGE_VAT_BY_ONE_PENNY',
    'CHANGE_ECONOMIC_KEY', 'CHANGE_SCOPE_GENERATION',
    'SUBSTITUTE_HIDDEN_FINANCE_ALIAS', 'DROP_PAIRED_TIMESHEET_LEG'
  ]) assert.ok(matrix.mutation_operators.includes(operator), operator);
});

test('downstream parity includes execution preparation, status, cancellation and frozen lineage without real payment', () => {
  assert.equal(matrix.downstream_contract_groups.length, 3);
  assert.equal(matrix.release_gate.no_real_payment_or_provider_action, true);
  const paths = matrix.downstream_contract_groups.flatMap(group => group.evidence_files.map(file => file.path));
  for (const required of [
    'tests/banking-pay-current-status-draft-amount.test.cjs',
    'tests/banking-pay-pre-provider-retry-recovery.test.cjs',
    'tests/banking-pay-cancellation-frozen-scope-v2.test.js',
    'tests/banking-pay-semantic-ready-cancellation-reversion.test.cjs'
  ]) assert.ok(paths.includes(required), required);
  const cancellation = matrix.downstream_contract_groups.find(group => group.group_id === 'D3_CANCEL_REVERT_RETRY_LINEAGE');
  for (const scenario of [
    'WHOLE_BATCH_DRAFT_CANCEL',
    'ONE_WHOLE_CANDIDATE_DRAFT_CANCEL_UNRELATED_CANDIDATES_UNCHANGED',
    'ONE_WHOLE_CANDIDATE_FUTURE_DATED_SCHEDULED_LOCAL_NOT_SENT_CANCEL',
    'PROVIDER_SUBMITTED_OR_SETTLED_PRE_BANK_CANCEL_REJECT',
    'CANDIDATE_CANCEL_REPLAY_RESPONSE_LOSS_IDEMPOTENT'
  ]) assert.ok(cancellation.required_scenarios.includes(scenario), scenario);
  assert.ok(paths.includes('tests/banking-pay-create-draft-execute-policy-contract.test.cjs'));
  assert.ok(matrix.release_gate.pass_requires.some(rule => /V1-versus-V8 durable Draft/.test(rule)));
  assert.ok(matrix.release_gate.pass_requires.some(rule => /no skipped acceptance case/.test(rule)));
});
