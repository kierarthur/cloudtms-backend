const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const worker = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');

function functionBody(name) {
  const markers = [`function ${name}`, `async function ${name}`];
  const start = markers.map((marker) => worker.indexOf(marker)).filter((value) => value >= 0).sort((a, b) => a - b)[0];
  assert.ok(Number.isInteger(start) && start >= 0, `${name} missing`);
  const boundaries = [worker.indexOf('\nfunction ', start + 10), worker.indexOf('\nasync function ', start + 10)].filter((value) => value > start);
  const end = boundaries.length ? Math.min(...boundaries) : worker.length;
  return worker.slice(start, end);
}

test('canonical correction proof uses the exact frozen field set and rejects extras', () => {
  const helperStart = worker.indexOf('const BANKING_PAY_CORRECTION_UUID_RE');
  const helperEnd = worker.indexOf('\nasync function requireBankingPayCancellationActor', helperStart);
  const source = `${functionBody('stableBankingPayContinuationJson')}\n${worker.slice(helperStart, helperEnd)}\nthis.canonicalise = canonicaliseBankingPayCorrectionProofPayload;`;
  const context = { TextEncoder, WeakSet, Set, Object, Array, String, Number, Error, JSON };
  vm.runInNewContext(source, context);
  const payload = {
    version: 1,
    correction_request_id: '11111111-1111-4111-8111-111111111111',
    pay_batch_id: '22222222-2222-4222-8222-222222222222',
    actor_user_id: '33333333-3333-4333-8333-333333333333',
    session_hash: '1'.repeat(64),
    plan_hash: '2'.repeat(64),
    selection_hash: '3'.repeat(64),
    requested_action: 'PRE_BANK_CANCEL',
    selected_candidate_count: 2,
    selected_active_item_count: 3,
    selected_amount_pence: 1234,
    reason_hash: '4'.repeat(64),
    evidence_hash: null,
    outcome_hash: null,
    nonce: 'A'.repeat(43),
    issued_at_epoch_seconds: 100,
    expires_at_epoch_seconds: 200
  };
  const canonical = context.canonicalise(payload);
  assert.equal(canonical, JSON.stringify(Object.fromEntries(Object.entries(payload).sort(([a], [b]) => a.localeCompare(b)))));
  assert.throws(() => context.canonicalise({ ...payload, extra: true }), (error) => error?.code === 'REAUTH_PROOF_PAYLOAD_FIELDS_INVALID');
  assert.throws(() => context.canonicalise({ ...payload, evidence_hash: undefined }), (error) => error?.code === 'REAUTH_PROOF_HASH_INVALID');
});

test('proof signing and verification canonicalise exact bytes', () => {
  const signing = functionBody('signBankingPayCorrectionProof');
  const verification = functionBody('verifyBankingPayCorrectionProof');
  assert.match(signing, /canonicaliseBankingPayCorrectionProofPayload/);
  assert.match(verification, /canonicaliseBankingPayCorrectionProofPayload/);
  assert.match(verification, /canonical !== new TextDecoder\(\)\.decode\(payloadBytes\)/);
  assert.match(functionBody('createBankingPayCorrectionReauthProof'), /expiresAt = Math\.min\(issuedAt \+ 600, requestedAt \+ 86400\)/);
  assert.match(functionBody('verifyPaymentReversalReauth'), /crypto\.subtle|verifyBankingPayCorrectionProof/);
});

test('payment status page validates bounded cursor, sort and fixed page sizes before one RPC', () => {
  const body = functionBody('handleBankingPayPaymentStatusPageV1');
  assert.match(body, /byteLength > 4096/);
  assert.match(body, /\['STATUS', 'CANDIDATE', 'AMOUNT'\]/);
  assert.match(body, /\[25, 50, 75, 100\]/);
  assert.equal((body.match(/sbRpc\(/g) || []).length, 1);
  assert.match(body, /pay_batch_payment_status_page_v1/);
});

test('planning is bounded and freezes exact selection without accepting a proof', () => {
  const body = functionBody('handleBankingPayCorrectionPlanV1');
  const normalise = functionBody('normalizeBankingPayCorrectionSelection');
  assert.match(body, /maxBytes: 524288/);
  assert.match(body, /PAYMENT_CORRECTION_PROOF_NOT_ACCEPTED_AT_PLANNING/);
  assert.match(normalise, /10000/);
  assert.match(normalise, /context: 'CURRENT_PAYMENT_STATUS'/);
  assert.match(normalise, /snapshot_token/);
  assert.match(normalise, /idempotency_key/);
  assert.equal((body.match(/sbRpc\(/g) || []).length, 1);
});

test('installed Stage 1 reason authority is preserved for planning and start replay', () => {
  const plan = functionBody('handleBankingPayCorrectionPlanV1');
  const start = functionBody('handleBankingPayCorrectionStartPreparedV1');
  assert.match(plan, /FAILED_PAYMENT_RELEASE_CONFIRMED_NOT_PAID/);
  assert.match(plan, /DRAFT_PAYMENT_CANCELLED_BY_USER/);
  assert.match(start, /REAUTH_REASON_MISMATCH/);
  assert.match(start, /p_reason: reason/);
});

test('maker checker does not accept requester proof and cancellation route forces CANCEL', () => {
  const auth = functionBody('handleBankingPayCorrectionAuthActionV1');
  const cancel = functionBody('handleBankingPayCorrectionCancelV1');
  assert.match(auth, /REQUESTER_PROOF_NOT_ACCEPTED_FOR_AUTHORISATION/);
  assert.match(auth, /PAYMENT_AUTHORISER_REQUIRED/);
  assert.match(auth, /PAYMENT_GOLDEN_KEY_REQUIRED/);
  assert.doesNotMatch(auth, /verifyPaymentReversalReauth\(/);
  assert.match(cancel, /'CANCEL'/);
  assert.match(worker, /return handleBankingPayCorrectionCancelV1\(env, req, user, m\.id\)/);
});

test('whole draft cancellation owns its audit reason and uses the existing ceremony', () => {
  const body = functionBody('handleBankingPayBatchCancelV1');
  assert.match(body, /DRAFT_PAYMENT_CANCELLED_BY_USER/);
  assert.match(body, /verifyPaymentReversalReauth/);
  assert.match(body, /p_correction_request_id: null/);
  assert.doesNotMatch(body, /body\?\.reason/);
  assert.equal((body.match(/sbRpc\(/g) || []).length, 1);
});

test('manual not-paid resolution is exact evidence only and cannot auto-release', () => {
  const body = functionBody('handleBankingPayPaymentStatusResolveV1');
  assert.match(body, /instruction_scope_ids/);
  assert.match(body, /manual_ambiguity_resolution: true/);
  assert.match(body, /suppress_auto_unwind: true/);
  assert.match(body, /MANUAL_RESOLUTION_AUTO_RELEASE_PROHIBITED/);
  assert.match(body, /reauthProofHash/);
  assert.doesNotMatch(body, /pay_payment_correction_request_start/);
  assert.doesNotMatch(body, /\.\.\.\(body\?\.event_json/);
  assert.equal((body.match(/sbRpc\(/g) || []).length, 1);
});

test('paid-after-release acknowledgement is a separate exact Finance route', () => {
  const body = functionBody('handleBankingPayPaidAfterReleaseReviewV1');
  assert.match(body, /ACKNOWLEDGE_PAID_AND_KEEP_NONPAYABLE/);
  assert.match(body, /PAID_AFTER_RELEASE_EVIDENCE_MISMATCH/);
  assert.match(body, /review_status: 'ACKNOWLEDGED'/);
  assert.match(body, /pay_bank_event_ingest/);
  assert.doesNotMatch(body, /pay_payment_correction_request_start|pay_settle_rail/);
  assert.match(worker, /return handleBankingPayPaidAfterReleaseReviewV1\(env, req, user, m\.id\)/);
});

test('integrity checker is read-only TEST support only and rejects repair', () => {
  const body = functionBody('handleBankingPayCorrectionIntegrityV1');
  assert.match(body, /yakevhtttcsljosbdpov\.supabase\.co/);
  assert.match(body, /INTEGRITY_CHECK_REPAIR_PROHIBITED/);
  assert.match(body, /Number\.isInteger\(maxCandidates\)/);
  assert.equal((body.match(/sbRpc\(/g) || []).length, 1);
  assert.match(body, /pay_payment_correction_integrity_check_v1/);
});

test('retired process and confirm-no-money routes cannot advance or unwind directly', () => {
  assert.match(worker, /PAYMENT_CORRECTION_PROCESS_ROUTE_RETIRED/);
  assert.match(worker, /CONFIRM_NO_MONEY_UNWIND_ROUTE_RETIRED/);
  assert.doesNotMatch(worker, /sbRpc\(env, 'pay_payment_confirm_no_money_and_unwind'/);
  assert.doesNotMatch(worker, /sbRpc\(env, 'pay_payment_cancel_not_sent_and_recalculate/);
});

test('cancellation producers preserve the typed SQL descriptor and report enqueue separately', () => {
  for (const name of [
    'handleBankingPayCorrectionPlanV1',
    'handleBankingPayCorrectionStartPreparedV1',
    'handleBankingPayCorrectionAuthActionV1',
    'handleBankingPayBatchCancelV1'
  ]) {
    const body = functionBody(name);
    assert.match(body, /continuation_enqueue/);
    assert.doesNotMatch(body, /\{ \.\.\.result, continuation \}/);
  }
  const eventWake = functionBody('enqueueBankingPayEventResultContinuations');
  assert.match(eventWake, /slice\(0, 4\)/);
  assert.doesNotMatch(eventWake, /Object\.values|recursive/i);
});
