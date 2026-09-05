const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const json = relativePath => JSON.parse(read(relativePath));
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');

const historicalPath = 'supabase/repeatable/04082026_1154_pay_batches_claim_due_scheduled.sql';
const transferPreparePath = 'supabase/repeatable/12082026_1446_pay_execute_bank_transfer_chunk_prepare_voided_overlay.sql';
const classifierPath = 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql';
const ownerPath = 'supabase/repeatable/04092026_2330_banking_pay_due_schedule_local_prepare_evidence_v1.sql';
const runtimePath = 'tests/04092026_2340_banking_pay_scheduled_due_local_prepare_runtime.sql';
const runnerPath = 'scripts/verify-banking-pay-scheduled-due-local-prepare-v1.mjs';
const resultPath = 'codex_outputs/h2-draft-parity/P10_SCHEDULED_DUE_LOCAL_PREPARE_RESULTS_V1.json';

test('P10 preserves the exact pre-change scheduled-payment divergence as audit evidence', () => {
  const result = json(resultPath);
  assert.equal(result.classification, 'SCHEDULED_EXECUTION_ORCHESTRATION_DEFECT_POLICY_DELTA_ZERO');
  assert.equal(result.first_divergent_boundary, 'pay_batches_claim_due_scheduled post-lock provider-evidence guard');
  assert.equal(sha256(read(historicalPath)), result.source_history.historical_due_owner_sha256);
  assert.equal(sha256(read(transferPreparePath)), result.source_history.local_transfer_prepare_owner_sha256);
  assert.equal(sha256(read(classifierPath)), result.source_history.canonical_classifier_owner_sha256);
  assert.equal(result.pre_change.due_claim_code, 'NO_DUE_BATCH');
  assert.equal(result.pre_change.due_skip_code, 'PROVIDER_EVIDENCE_PRESENT');
  assert.equal(result.pre_change.canonical_classification, 'SCHEDULED_LOCAL_NOT_SENT');
  assert.equal(result.pre_change.canonical_is_unattempted_submit_eligible, true);
  assert.equal(result.pre_change.provider_submission_evidence, false);
  assert.equal(result.pre_change.provider_event_count, 0);
  assert.equal(result.pre_change.provider_attempt_count, 0);
  assert.equal(result.pre_change.persistent_write_delta, 0);
});

test('P10 correction is one exact owner and does not change payment policy or timeout budgets', () => {
  const owner = read(ownerPath);
  const historical = read(historicalPath);
  const result = json(resultPath);
  assert.equal((owner.match(/CREATE\s+OR\s+REPLACE\s+FUNCTION/gi) || []).length, 1);
  assert.equal(sha256(owner), result.local_correction.owner_sha256);
  assert.equal(result.local_correction.policy_or_economic_change_count, 0);
  assert.equal(result.local_correction.statement_timeout_changed, false);
  assert.equal(result.local_correction.lock_timeout_changed, false);
  assert.match(owner, /SET statement_timeout TO '6000ms'/);
  assert.match(owner, /SET lock_timeout TO '1000ms'/);
  assert.match(owner, /SECURITY DEFINER/);
  assert.match(owner, /SET search_path TO pg_catalog, private, extensions, pg_temp/);
  assert.match(owner, /pay_bank_transfer_execution_classify\([\s\S]*?'SCHEDULE_ACTION'/);
  assert.match(owner, /auth_request\.execution_intent_json->>'operation_id'/);
  assert.match(owner, /v_authorised_operation_id/);
  assert.doesNotMatch(owner, /transfer_row\.request_id IS NOT NULL/);
  assert.match(historical, /transfer_row\.request_id IS NOT NULL/);
  assert.doesNotMatch(owner, /pay_settle_rail\s*\(|pay_bank_transfers_claim_provider_submit_chunk\s*\(|remittance_generate\s*\(|amount\s*=|net_amount|vat_amount/i);
});

test('P10 classifier gate retains every unsafe provider-evidence category and idempotent local eligibility', () => {
  const owner = read(ownerPath);
  for (const field of [
    'has_provider_submission_evidence',
    'has_provider_event_evidence',
    'has_provider_attempt_without_external_id',
    'has_operation_submit_attempt',
    'has_ambiguous_external_evidence',
    'is_terminal_or_completed',
    'has_provider_submit_blocker',
    'is_unattempted_submit_eligible'
  ]) {
    assert.match(owner, new RegExp(`transfer_evidence\\.${field}`));
  }
  assert.match(owner, /'PROVIDER_EVIDENCE_PRESENT'/);
  assert.match(owner, /'payment-execute:scheduled:batch:'/);
  assert.match(owner, /'DUE_OPERATION_STARTED'/);
  assert.match(owner, /FOR UPDATE SKIP LOCKED/);
});

test('P10 runtime fixture is rollback-only and exercises the complete local prepare-to-due sequence', () => {
  const sql = read(runtimePath);
  const result = json(resultPath);
  assert.equal(sha256(sql), result.local_correction.runtime_fixture_sha256);
  assert.match(sql, /^BEGIN;$/m);
  assert.match(sql, /^SET LOCAL statement_timeout = '15s';$/m);
  assert.match(sql, /^SET LOCAL lock_timeout = '1500ms';$/m);
  assert.match(sql, /pay_execute_bank_transfer_chunk_prepare\s*\(/i);
  assert.match(sql, /pay_batch_prepare\s*\(/i);
  assert.match(sql, /pay_batch_auth_start\s*\(/i);
  assert.match(sql, /pay_batch_schedule\s*\(/i);
  assert.match(sql, /banking_pay_operation_claim_next\s*\([\s\S]*?p_operation_types := ARRAY\['PAYMENT_EXECUTE'\]/i);
  assert.match(sql, /banking_pay_operation_release_lease\s*\([\s\S]*?p_release_state := 'COMPLETE'/i);
  assert.doesNotMatch(sql, /banking_pay_operation_finish\s*\(/i);
  assert.match(sql, /pay_bank_transfer_execution_classify\s*\(/i);
  assert.match(sql, /batch_kind_fixed = 'PAYE'[\s\S]*?pay_set_paye_net_manual\s*\(/i);
  assert.match(sql, /SELECT DISTINCT ON \([\s\S]*?transfer\.payee_entity_kind[\s\S]*?transfer\.bank_details_hash_snapshot/i);
  assert.equal((sql.match(/pay_batches_claim_due_scheduled\s*\(/gi) || []).length, 2);
  assert.equal((sql.match(/^ROLLBACK;$/gm) || []).length, 1);
  assert.equal((sql.match(/^COMMIT;$/gm) || []).length, 0);
  assert.doesNotMatch(sql, /pay_settle_rail\s*\(|pay_bank_transfers_claim_provider_submit_chunk\s*\(|remittance_generate\s*\(/i);
  assert.ok(read(runnerPath).includes('assertPreChange'));
  assert.ok(read(runnerPath).includes('assertCorrected'));
});

test('P10 dual-engine evidence remains local-only and all final gates remain explicit', () => {
  const result = json(resultPath);
  assert.equal(result.status, 'EXACT_V1_V8_SCHEDULED_LOCAL_PREPARATION_PARITY_PASS_NOT_INSTALLED');
  assert.equal(result.local_results.pg17.status, 'PASS_ROLLBACK_ONLY');
  assert.equal(result.local_results.pg18.status, 'PASS_ROLLBACK_ONLY');
  assert.equal(result.local_results.pg17.due_claimed_count, 1);
  assert.equal(result.local_results.pg18.due_claimed_count, 1);
  assert.equal(result.local_results.pg17.replay_claimed_count, 0);
  assert.equal(result.local_results.pg18.replay_claimed_count, 0);
  assert.equal(result.local_results.pg17.unsafe_provider_evidence_cases_blocked, 5);
  assert.equal(result.local_results.pg18.unsafe_provider_evidence_cases_blocked, 5);
  assert.equal(result.blocking_gates.length, 6);
  assert.ok(!result.blocking_gates.includes('COMPLETE_PROVIDER_EVIDENCE_NEGATIVE_MATRIX'));
  assert.ok(result.blocking_gates.includes('MIGET_TEST_INSTALL_AND_READBACK'));
  assert.ok(result.blocking_gates.includes('FRESH_COMPLETE_POST_CORRECTION_AUDIT'));
});

test('P10 mutation guards reject policy drift, provider-evidence weakening and false finality', () => {
  const original = json(resultPath);
  const mutations = [
    value => { value.status = 'PASS'; },
    value => { value.classification = 'PAYMENT_POLICY_CHANGE'; },
    value => { value.pre_change.provider_submission_evidence = true; },
    value => { value.local_correction.policy_or_economic_change_count = 1; },
    value => { value.local_correction.statement_timeout_changed = true; },
    value => { value.local_results.pg17.replay_claimed_count = 1; },
    value => { value.local_results.pg18.unsafe_provider_evidence_cases_blocked = 4; },
    value => { value.blocking_gates = []; }
  ];
  const validate = value => {
    assert.equal(value.status, 'EXACT_V1_V8_SCHEDULED_LOCAL_PREPARATION_PARITY_PASS_NOT_INSTALLED');
    assert.equal(value.classification, 'SCHEDULED_EXECUTION_ORCHESTRATION_DEFECT_POLICY_DELTA_ZERO');
    assert.equal(value.pre_change.provider_submission_evidence, false);
    assert.equal(value.local_correction.policy_or_economic_change_count, 0);
    assert.equal(value.local_correction.statement_timeout_changed, false);
    assert.equal(value.local_results.pg17.replay_claimed_count, 0);
    assert.equal(value.local_results.pg18.unsafe_provider_evidence_cases_blocked, 5);
    assert.ok(value.blocking_gates.length > 0);
  };
  validate(structuredClone(original));
  for (const mutate of mutations) {
    const value = structuredClone(original);
    mutate(value);
    assert.throws(() => validate(value));
  }
});
