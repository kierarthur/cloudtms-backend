const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const fixturePath = path.join(root, 'tests', 'fixtures', 'banking-pay-draft-v8-downstream-contract-bindings-v1.json');
const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));

const requiredGateIds = [
  'DRAFT_TERMINAL_OPERATION_RESPONSE',
  'PAYE_WORKSHEET_NET_ENTRY_AND_SAVED_SCALAR',
  'UMBRELLA_PAYEE_VAT_AND_CHANNEL',
  'CURRENT_PAYMENT_STATUS_ROWS_AMOUNTS_ACTIONS',
  'OVERVIEW_BENEFICIARY_AND_PAYMENT_TOTALS',
  'EXECUTE_PAYMENT_ELIGIBILITY_AND_PREVIEW',
  'IMMEDIATE_EXECUTION_LOCAL_PREPARATION',
  'SCHEDULED_EXECUTION_LOCAL_PREPARATION',
  'PROVIDER_PREPARATION_AND_SUBMISSION_AUTHORITY',
  'BANK_TRANSFER_PREPARATION',
  'CSV_SETTLEMENT_PROJECTION',
  'EXTERNAL_SETTLEMENT_PROJECTION',
  'REMITTANCE_GENERATION_AND_SUPPRESSION_PROJECTION',
  'WHOLE_BATCH_DRAFT_CANCELLATION',
  'WHOLE_CANDIDATE_UNTOUCHED_DRAFT_CANCELLATION',
  'WHOLE_CANDIDATE_FUTURE_DATED_EXECUTED_NOT_PAID_CANCELLATION',
  'CERTIFIED_REVERSION_CORRECTION_RETRY_AND_BLOCKED_FUNDS',
  'FRONTEND_POLLING_AND_TERMINAL_INTERPRETATION'
];

const openStatus = 'OPEN_EXACT_V1_V8_DOWNSTREAM_PARITY_REQUIRED';
const allowedEvidenceLevels = new Set([
  'SOURCE_GUARD_ONLY',
  'EXECUTABLE_COMPONENT_ONLY',
  'ROLLBACK_RUNTIME_COMPONENT_ONLY',
  'EXACT_V1_V8_DOWNSTREAM_PARITY'
]);

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function validate(doc) {
  assert.equal(doc.contract, 'BANKING_PAY_DRAFT_V8_DOWNSTREAM_CONTRACT_BINDINGS_V1');
  assert.equal(doc.policy_boundary.allowed_change, 'Performance and orchestration only.');
  assert.equal(doc.policy_boundary.no_real_action, true, 'REAL_ACTION_PROHIBITION_REMOVED');
  assert.match(doc.policy_boundary.policy_x, /Post-Draft.*frozen batch artifacts/);
  assert.match(doc.policy_boundary.route_transparency, /No downstream owner may branch/);
  assert.ok(doc.policy_boundary.forbidden_changes.includes('gross or net treatment'));
  assert.ok(doc.policy_boundary.forbidden_changes.includes('cancellation, correction or reversion outcome'));

  assert.equal(doc.gates.length, requiredGateIds.length);
  const actualIds = doc.gates.map(row => row.gate_id);
  assert.equal(new Set(actualIds).size, actualIds.length, 'DUPLICATE_DOWNSTREAM_GATE');
  assert.deepEqual([...actualIds].sort(), [...requiredGateIds].sort(), 'DOWNSTREAM_GATE_CENSUS_CHANGED');

  for (const gate of doc.gates) {
    assert.ok(allowedEvidenceLevels.has(gate.current_evidence_level), `${gate.gate_id}:UNKNOWN_EVIDENCE_LEVEL`);
    assert.ok(gate.owner && gate.owner.length > 5, `${gate.gate_id}:OWNER_REQUIRED`);
    assert.ok(Array.isArray(gate.proved_now ? [gate.proved_now] : []) && gate.proved_now.length > 20, `${gate.gate_id}:PROVED_SCOPE_REQUIRED`);
    assert.ok(Array.isArray(gate.still_missing), `${gate.gate_id}:MISSING_PROOF_LIST_REQUIRED`);

    for (const relative of [...gate.source_paths, ...gate.evidence_paths]) {
      assert.equal(path.isAbsolute(relative), false, `${gate.gate_id}:ABSOLUTE_EVIDENCE_PATH_PROHIBITED`);
      assert.equal(fs.existsSync(path.join(root, relative)), true, `${gate.gate_id}:MISSING_EVIDENCE:${relative}`);
    }

    const claimsPass = gate.status === 'PASS' || gate.status === 'EXACT_V1_V8_DOWNSTREAM_PARITY_PASS';
    if (claimsPass) {
      assert.equal(gate.current_evidence_level, 'EXACT_V1_V8_DOWNSTREAM_PARITY', `${gate.gate_id}:PARITY_PASS_WITHOUT_EXACT_RUNTIME`);
      assert.ok(gate.result_manifest && typeof gate.result_manifest.path === 'string', `${gate.gate_id}:RESULT_MANIFEST_REQUIRED`);
      assert.match(gate.result_manifest.sha256, /^[0-9a-f]{64}$/, `${gate.gate_id}:RESULT_MANIFEST_HASH_REQUIRED`);
      const resultPath = path.join(root, gate.result_manifest.path);
      assert.equal(fs.existsSync(resultPath), true, `${gate.gate_id}:RESULT_MANIFEST_MISSING`);
      assert.equal(sha256(fs.readFileSync(resultPath)), gate.result_manifest.sha256, `${gate.gate_id}:RESULT_MANIFEST_STALE`);
      assert.equal(gate.still_missing.length, 0, `${gate.gate_id}:PASS_GATE_STILL_HAS_MISSING_PROOF`);
    } else {
      assert.equal(gate.status, openStatus, `${gate.gate_id}:UNRECOGNISED_OPEN_STATUS`);
      assert.ok(gate.still_missing.length > 0, `${gate.gate_id}:OPEN_GATE_MISSING_PROOF_LIST_REQUIRED`);
    }
  }

  const levelCounts = Object.fromEntries([...allowedEvidenceLevels].map(level => [level, 0]));
  for (const gate of doc.gates) levelCounts[gate.current_evidence_level] += 1;
  assert.deepEqual(doc.totals, {
    gates: doc.gates.length,
    source_guard_only: levelCounts.SOURCE_GUARD_ONLY,
    executable_component_only: levelCounts.EXECUTABLE_COMPONENT_ONLY,
    rollback_runtime_component_only: levelCounts.ROLLBACK_RUNTIME_COMPONENT_ONLY,
    exact_v1_v8_downstream_parity_pass: levelCounts.EXACT_V1_V8_DOWNSTREAM_PARITY,
    open: doc.gates.filter(row => row.status === openStatus).length
  });

  assert.equal(doc.cancellation_invariants.whole_batch_and_whole_candidate_are_distinct, true, 'BATCH_CANDIDATE_SCOPE_MERGED');
  assert.equal(doc.cancellation_invariants.unrelated_candidates_must_remain_byte_for_byte_unchanged, true, 'UNRELATED_CANDIDATE_GUARD_REMOVED');
  assert.equal(doc.cancellation_invariants.untouched_draft_action, 'DRAFT_CANCEL');
  assert.deepEqual(doc.cancellation_invariants.future_dated_executed_not_paid_actions, ['LOCAL_PREPARED_NOT_SENT', 'SCHEDULED_LOCAL_NOT_SENT']);
  assert.equal(doc.cancellation_invariants.failed_payment_return_invariants.immediate_and_scheduled_failures_are_separate_required_cases, true);
  assert.equal(doc.cancellation_invariants.failed_payment_return_invariants.user_action, 'RELEASE_FAILED_PAYMENT');
  assert.deepEqual(doc.cancellation_invariants.failed_payment_return_invariants.accepted_no_money_classifications, ['PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY']);
  assert.equal(doc.cancellation_invariants.failed_payment_return_invariants.provider_ambiguity_or_money_movement_must_fail_closed, true);
  assert.equal(doc.cancellation_invariants.failed_payment_return_invariants.released_constituents_must_reappear_once_in_current_workbench, true);
  assert.equal(doc.cancellation_invariants.failed_payment_return_invariants.remaining_draft_and_unrelated_candidates_must_remain_unchanged, true);
  assert.equal(doc.cancellation_invariants.provider_submitted_or_settled_must_not_use_pre_bank_cancel, true);
  assert.equal(doc.cancellation_invariants.current_complete_runtime_proof, false);
  assert.match(doc.release_gate, /cannot be enabled/);
}

test('the downstream ledger is complete, owner-bound and honest about its open runtime parity', () => {
  validate(fixture);
  assert.equal(fixture.gates.filter(gate => gate.status === openStatus).length, 11);
  assert.equal(fixture.totals.exact_v1_v8_downstream_parity_pass, 7);
});

test('source guards preserve PAYE, Umbrella, status, execution and cancellation decision boundaries', () => {
  const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
  const paye = read('supabase/repeatable/21072026_1235_48_pay_set_paye_net_manual.sql');
  const status = read('supabase/repeatable/04082026_1146_pay_batch_payment_status_page_v1.sql');
  const transfer = read('supabase/repeatable/20072026_1215_align_transfer_scope_with_bank_projection.sql');
  const batchCancel = read('supabase/repeatable/04082026_1206_pay_batch_cancel.sql');
  const selectedItems = read('supabase/repeatable/09082026_1403_pay_payment_correction_selected_items_draft_scope.sql');
  const preBank = read('supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql');
  const failedPaymentRelease = read('supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql');

  assert.match(paye, /pay_batch_paye_net_inputs/);
  assert.match(paye, /GROSS_DEDUCT/);
  assert.match(paye, /NET_DEDUCT/);
  assert.match(status, /reviewed_payment_amount/);
  assert.match(status, /amount_inc_vat/);
  assert.match(transfer, /pay_batch_paye_net_inputs/);
  assert.match(batchCancel, /'scope_type',\s*'BATCH'/);
  assert.doesNotMatch(batchCancel, /'scope_type',\s*'CANDIDATES'/);
  assert.match(selectedItems, /v_scope_type\s*=\s*'CANDIDATES'/);
  assert.match(preBank, /v_classification IN \('LOCAL_PREPARED_NOT_SENT', 'SCHEDULED_LOCAL_NOT_SENT'\)/);
  assert.match(preBank, /provider_evidence\.provider_submitted/);
  assert.match(preBank, /Pre-bank cancellation cannot apply because bank\/provider submission evidence exists/);
  assert.match(failedPaymentRelease, /PROVIDER_CANCELLED_NO_MONEY/);
  assert.match(failedPaymentRelease, /PROVIDER_FAILED_NO_MONEY/);
  assert.match(failedPaymentRelease, /UNWIND_FAILED_PAYMENT/);
});

test('a source-only or isolated component result cannot be promoted to downstream PASS', () => {
  for (const gateId of ['EXECUTE_PAYMENT_ELIGIBILITY_AND_PREVIEW', 'WHOLE_BATCH_DRAFT_CANCELLATION']) {
    const mutated = clone(fixture);
    mutated.gates.find(row => row.gate_id === gateId).status = 'PASS';
    assert.throws(() => validate(mutated), /PARITY_PASS_WITHOUT_EXACT_RUNTIME/);
  }
});

test('cancellation mutations cannot merge batch and Candidate scope or affect another Candidate', () => {
  const unrelated = clone(fixture);
  unrelated.cancellation_invariants.unrelated_candidates_must_remain_byte_for_byte_unchanged = false;
  assert.throws(() => validate(unrelated), /UNRELATED_CANDIDATE_GUARD_REMOVED/);

  const merged = clone(fixture);
  merged.cancellation_invariants.whole_batch_and_whole_candidate_are_distinct = false;
  assert.throws(() => validate(merged), /BATCH_CANDIDATE_SCOPE_MERGED/);

  const wrongFutureState = clone(fixture);
  wrongFutureState.cancellation_invariants.future_dated_executed_not_paid_actions = ['SCHEDULED'];
  assert.throws(() => validate(wrongFutureState));
});

test('real payment actions and downstream policy drift remain prohibited', () => {
  const actionMutation = clone(fixture);
  actionMutation.policy_boundary.no_real_action = false;
  assert.throws(() => validate(actionMutation), /REAL_ACTION_PROHIBITION_REMOVED/);

  const policyMutation = clone(fixture);
  policyMutation.policy_boundary.allowed_change = 'Change payment routing';
  assert.throws(() => validate(policyMutation));
});
