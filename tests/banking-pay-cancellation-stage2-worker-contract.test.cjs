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
  assert.match(normalise, /CURRENT_PAYMENT_STATUS/);
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

test('public version route carries the deployed Stage 2 source marker', () => {
  const body = functionBody('handleVersion');
  assert.match(body, /banking_pay_cancellation_stage2/);
  assert.match(body, /revision:\s*"5E"/);
  assert.match(body, /implementation_commit:\s*"3f143361"/);
});

test('selection canonicalisation rejects conflicting membership semantics before planning', () => {
  const constantsStart = worker.indexOf('const BANKING_PAY_CORRECTION_UUID_RE');
  const constantsEnd = worker.indexOf('\nasync function parseBankingPayCancellationJsonBody', constantsStart);
  const source = [
    functionBody('stableBankingPayContinuationJson'),
    worker.slice(constantsStart, constantsEnd),
    functionBody('bankingPayCorrectionCanonicalUuidArray'),
    functionBody('validateBankingPayPaymentStatusFilter'),
    functionBody('canonicaliseBankingPayCorrectionSelectionFilter'),
    functionBody('normalizeBankingPayCorrectionSelection'),
    'this.normalizeSelection = normalizeBankingPayCorrectionSelection;'
  ].join('\n');
  const context = { TextEncoder, Set, Object, Array, String, Number, Error, JSON };
  vm.runInNewContext(source, context);
  const candidateA = '11111111-1111-4111-8111-111111111111';
  const candidateB = '22222222-2222-4222-8222-222222222222';
  const base = { snapshot_token: 'snapshot', idempotency_key: 'request-key' };

  assert.throws(() => context.normalizeSelection({ ...base, mode: 'ALL_MATCHING', explicit_candidate_tokens: [candidateA] }, 'PRE_BANK_CANCEL'), (error) => error?.code === 'PAYMENT_CORRECTION_SELECTION_MODE_CONFLICT');
  assert.throws(() => context.normalizeSelection({ ...base, mode: 'ALL_MATCHING', filter_json: { included_candidate_tokens: [candidateA] } }, 'PRE_BANK_CANCEL'), (error) => error?.code === 'PAYMENT_CORRECTION_SELECTION_MODE_CONFLICT');
  assert.throws(() => context.normalizeSelection({ ...base, mode: 'EXPLICIT', explicit_candidate_tokens: [candidateA], exclusions: [candidateB] }, 'PRE_BANK_CANCEL'), (error) => error?.code === 'PAYMENT_CORRECTION_SELECTION_MODE_CONFLICT');
  assert.throws(() => context.normalizeSelection({ ...base, mode: 'ALL_MATCHING', filter_json: { actionable_only: 'true' } }, 'PRE_BANK_CANCEL'), (error) => error?.code === 'FILTER_INVALID');
  assert.throws(() => context.normalizeSelection({ ...base, mode: 'ALL_MATCHING', filter_json: { action: 'RELEASE_FAILED_PAYMENT', actionable_only: true } }, 'PRE_BANK_CANCEL'), (error) => error?.code === 'PAYMENT_CORRECTION_ACTION_FILTER_MISMATCH');

  const canonicalAll = context.normalizeSelection({
    ...base,
    mode: 'ALL_MATCHING',
    exclusions: [candidateB],
    filter_json: { action: 'CANCEL_PAYMENT', actionable_only: true, search: '  Example  ', excluded_candidate_tokens: [candidateA] }
  }, 'PRE_BANK_CANCEL');
  assert.deepEqual(Array.from(canonicalAll.exclusions), [candidateA, candidateB]);
  assert.equal(JSON.stringify(canonicalAll.filter_json), JSON.stringify({ action: 'CANCEL_PAYMENT', search: 'Example' }));

  const canonicalExplicit = context.normalizeSelection({ ...base, mode: 'EXPLICIT', explicit_candidate_tokens: [candidateB, candidateA] }, 'PRE_BANK_CANCEL');
  assert.deepEqual(Array.from(canonicalExplicit.explicit_candidate_tokens), [candidateA, candidateB]);
  assert.equal(JSON.stringify(canonicalExplicit.filter_json), '{}');
});

test('selection canonicalisation fails closed for falsy, unknown and conflicting authorities', () => {
  const constantsStart = worker.indexOf('const BANKING_PAY_CORRECTION_UUID_RE');
  const constantsEnd = worker.indexOf('\nasync function parseBankingPayCancellationJsonBody', constantsStart);
  const source = [
    functionBody('stableBankingPayContinuationJson'),
    worker.slice(constantsStart, constantsEnd),
    functionBody('bankingPayCorrectionCanonicalUuidArray'),
    functionBody('validateBankingPayPaymentStatusFilter'),
    functionBody('canonicaliseBankingPayCorrectionSelectionFilter'),
    functionBody('normalizeBankingPayCorrectionSelection'),
    'this.normalizeSelection = normalizeBankingPayCorrectionSelection;'
  ].join('\n');
  const context = { TextEncoder, Set, Object, Array, String, Number, Error, JSON };
  vm.runInNewContext(source, context);
  const candidateA = '11111111-1111-4111-8111-111111111111';
  const base = { mode: 'ALL_MATCHING', snapshot_token: 'snapshot', idempotency_key: 'request-key' };

  for (const filter_json of [null, false, 0, undefined]) {
    assert.throws(() => context.normalizeSelection({ ...base, filter_json }, 'PRE_BANK_CANCEL'));
  }
  for (const selection_json of [null, false, 0, undefined]) {
    assert.throws(() => context.normalizeSelection({ ...base, selection_json }, 'PRE_BANK_CANCEL'));
  }
  for (const exclusions of [null, false, 0, undefined]) {
    assert.throws(() => context.normalizeSelection({ ...base, exclusions }, 'PRE_BANK_CANCEL'));
  }

  assert.throws(() => context.normalizeSelection({ ...base, unsupported: true }, 'PRE_BANK_CANCEL'));
  assert.throws(() => context.normalizeSelection({ ...base, selection_json: { unsupported: true } }, 'PRE_BANK_CANCEL'));
  assert.throws(() => context.normalizeSelection({ ...base, included_candidate_tokens: [candidateA] }, 'PRE_BANK_CANCEL'));
  assert.throws(() => context.normalizeSelection({ ...base, filter_json: { search: 'A' }, selection_json: { filter_json: { search: 'B' } } }, 'PRE_BANK_CANCEL'), (error) => error?.code === 'PAYMENT_CORRECTION_SELECTION_ALIAS_CONFLICT');
  assert.throws(() => context.normalizeSelection({ ...base, exclusions: [candidateA], selection_json: { exclusions: [] } }, 'PRE_BANK_CANCEL'), (error) => error?.code === 'PAYMENT_CORRECTION_SELECTION_ALIAS_CONFLICT');

  const equalAliases = context.normalizeSelection({
    ...base,
    filter_json: { search: ' Example ' },
    selection_json: { filterJson: { search: 'Example' } },
    exclusions: [candidateA]
  }, 'PRE_BANK_CANCEL');
  assert.equal(equalAliases.filter_json.search, 'Example');
  assert.deepEqual(Array.from(equalAliases.exclusions), [candidateA]);
});

test('invalid selection descriptors are rejected before the planning RPC', async () => {
  let rpcCalls = 0;
  const normalizerStart = worker.indexOf('const BANKING_PAY_CORRECTION_UUID_RE');
  const normalizerEnd = worker.indexOf('\nasync function parseBankingPayCancellationJsonBody', normalizerStart);
  const normalizerContext = { TextEncoder, Set, Object, Array, String, Number, Error, JSON };
  vm.runInNewContext([
    functionBody('stableBankingPayContinuationJson'),
    worker.slice(normalizerStart, normalizerEnd),
    functionBody('bankingPayCorrectionCanonicalUuidArray'),
    functionBody('validateBankingPayPaymentStatusFilter'),
    functionBody('canonicaliseBankingPayCorrectionSelectionFilter'),
    functionBody('normalizeBankingPayCorrectionSelection'),
    'this.normalizeSelection = normalizeBankingPayCorrectionSelection;'
  ].join('\n'), normalizerContext);

  const handlerContext = {
    String,
    requireBankingPayCancellationActor: async () => ({ ok: true, actorUserId: '11111111-1111-4111-8111-111111111111' }),
    bankingPayCorrectionUuid: () => true,
    parseBankingPayCancellationJsonBody: async () => ({
      requested_action: 'PRE_BANK_CANCEL',
      snapshot_token: 'snapshot',
      idempotency_key: 'request-key',
      filter_json: false
    }),
    normalizeBankingPayCorrectionSelection: normalizerContext.normalizeSelection,
    bankingPayCorrectionBoundedText: () => 'reason',
    bankingPayCorrectionBodyErrorResponse: (_env, _req, error) => ({ status: 400, code: error.code }),
    sbRpc: async () => { rpcCalls += 1; return {}; }
  };
  vm.runInNewContext(`${functionBody('handleBankingPayCorrectionPlanV1')}\nthis.handle = handleBankingPayCorrectionPlanV1;`, handlerContext);
  const result = await handlerContext.handle({}, {}, {}, '22222222-2222-4222-8222-222222222222');
  assert.equal(result.status, 400);
  assert.equal(rpcCalls, 0);
});

test('post-commit enqueue failure preserves accepted cancellation and event results', async () => {
  const context = {
    readBankingPayContinuationFlag: () => true,
    enqueueBankingPayOperationContinuations: async () => { throw Object.assign(new Error('queue unavailable'), { code: 'QUEUE_UNAVAILABLE' }); }
  };
  vm.runInNewContext(`${functionBody('enqueueBankingPayCancellationResult')}\n${functionBody('enqueueBankingPayEventResultContinuations')}\nthis.enqueueCancellation = enqueueBankingPayCancellationResult; this.enqueueEvents = enqueueBankingPayEventResultContinuations;`, context);
  const descriptor = { required: true, terminal: false, requires_user_action: false, operation_id: '11111111-1111-4111-8111-111111111111' };
  const cancellation = await context.enqueueCancellation({}, { ok: true, request_id: 'request', continuation: descriptor }, 'PAYMENT_CORRECTION_PLAN');
  assert.equal(cancellation.ok, false);
  assert.equal(cancellation.background_start_delayed, true);
  assert.equal(cancellation.code, 'BANKING_PAY_CONTINUATION_ENQUEUE_DELAYED');
  const event = await context.enqueueEvents({}, { ok: true, continuations: [descriptor] }, 'PAYMENT_STATUS_EVENT_WAKE');
  assert.equal(event.ok, false);
  assert.equal(event.background_start_delayed, true);
  assert.equal(event.enqueued_count, 0);
  for (const name of ['handleBankingPayCorrectionPlanV1', 'handleBankingPayCorrectionStartPreparedV1', 'handleBankingPayCorrectionAuthActionV1', 'handleBankingPayBatchCancelV1', 'handleBankingPayPaymentStatusResolveV1', 'handleBankingPayPaidAfterReleaseReviewV1']) {
    assert.match(functionBody(name), /background_start_delayed/);
  }
});

test('requester proof requires a trusted lower-case tenant UUID and binds it to access tokens', async () => {
  const { webcrypto } = require('node:crypto');
  const sessionContext = { crypto: webcrypto, TextEncoder, DataView, Uint8Array, Array, String, Number, Math, Object, Error, BANKING_PAY_CORRECTION_UUID_RE: /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i };
  vm.runInNewContext(`${functionBody('bankingPayCorrectionSessionHash')}\nthis.sessionHash = bankingPayCorrectionSessionHash;`, sessionContext);
  const baseUser = { id: '11111111-1111-4111-8111-111111111111', session_id: 'session-id', session_issued_at_epoch_seconds: 123456 };
  await assert.rejects(() => sessionContext.sessionHash(null, baseUser), (error) => error?.code === 'REAUTH_SESSION_TENANT_REQUIRED');
  await assert.rejects(() => sessionContext.sessionHash(null, { ...baseUser, tenant_id: 'NOT-A-UUID' }), (error) => error?.code === 'REAUTH_SESSION_TENANT_REQUIRED');
  const hashA = await sessionContext.sessionHash(null, { ...baseUser, tenant_id: '22222222-2222-4222-8222-222222222222' });
  const hashB = await sessionContext.sessionHash(null, { ...baseUser, tenant_id: '33333333-3333-4333-8333-333333333333' });
  assert.match(hashA, /^[0-9a-f]{64}$/);
  assert.notEqual(hashA, hashB);

  let signedPayload = null;
  const tokenContext = {
    String, Object, Error, Math,
    accessTtl: () => 900,
    sessionSecret: () => 'test-secret',
    createToken: async (_secret, payload) => { signedPayload = payload; return 'signed-token'; }
  };
  vm.runInNewContext(`${functionBody('bankingPayCorrectionTenantAuthority')}\n${functionBody('mintAccessToken')}\nthis.mint = mintAccessToken;`, tokenContext);
  await tokenContext.mint({ BANKING_PAY_CORRECTION_TENANT_ID_V1: '8467e881-9691-4181-a738-f9834922d747' }, { user_id: baseUser.id, email: 'test@example.invalid', role: 'admin', sv: 1, sid: 'session-id' });
  assert.equal(signedPayload.tenant_id, '8467e881-9691-4181-a738-f9834922d747');
  const requireUserBody = functionBody('requireUser');
  assert.match(requireUserBody, /p\.tenant_id/);
  assert.doesNotMatch(requireUserBody, /request.*tenant|body.*tenant/i);
});

test('no-progress fuse counts the first stalled delivery and trips exactly on five', () => {
  const context = { Number, Math, String };
  vm.runInNewContext(`${functionBody('calculateBankingPayContinuationNoProgress')}\nthis.calculate = calculateBankingPayContinuationNoProgress;`, context);
  let count = 0;
  for (let delivery = 1; delivery <= 5; delivery += 1) {
    const result = context.calculate({ previous_count: count, pre_witness: 'same', post_witness: 'same', immediate_more_work: true, legitimate_future_wait: false });
    count = result.count;
    assert.equal(count, delivery);
    assert.equal(result.current_delivery_no_progress, true);
  }
  assert.equal(count, 5);
  assert.equal(context.calculate({ previous_count: 4, pre_witness: 'before', post_witness: 'after', immediate_more_work: true }).count, 0);
  assert.equal(context.calculate({ previous_count: 3, pre_witness: 'before', post_witness: 'after', immediate_more_work: false, legitimate_future_wait: true }).count, 3);
  assert.match(functionBody('claimAndAdvanceOneBankingPayOperation'), /continuationNoProgressCount >= 5/);
});

test('TEST configuration supplies one immutable tenant authority for correction proof binding', () => {
  const wrangler = fs.readFileSync(path.resolve(__dirname, '../wrangler.toml'), 'utf8');
  const matches = wrangler.match(/BANKING_PAY_CORRECTION_TENANT_ID_V1\s*=\s*"8467e881-9691-4181-a738-f9834922d747"/g) || [];
  assert.equal(matches.length, 2);
});
