import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');

function functionBody(name) {
  const markers = [`function ${name}`, `async function ${name}`];
  const start = markers.map((marker) => worker.indexOf(marker)).filter((value) => value >= 0).sort((a, b) => a - b)[0];
  assert.ok(Number.isInteger(start) && start >= 0, `${name} missing`);
  const boundaries = [worker.indexOf('\nfunction ', start + 10), worker.indexOf('\nasync function ', start + 10)].filter((value) => value > start);
  const end = boundaries.length ? Math.min(...boundaries) : worker.length;
  return worker.slice(start, end);
}

const IDS = Object.freeze({
  request: '11111111-1111-4111-8111-111111111111',
  batch: '22222222-2222-4222-8222-222222222222',
  actor: '33333333-3333-4333-8333-333333333333',
  operation: '44444444-4444-4444-8444-444444444444'
});

function executableStartPreparedHarness({ status = 'APPLIED', actorUserId = IDS.actor } = {}) {
  const calls = [];
  const requestRow = {
    id: IDS.request,
    pay_batch_id: IDS.batch,
    requested_by_user_id: IDS.actor,
    reason: 'user-requested cancellation',
    status,
    required_quantity: 1,
    approved_count: 0,
    plan_hash: 'a'.repeat(64),
    selection_hash: 'b'.repeat(64),
    source_bank_event_id: null,
    accepted_resolution_json: null,
    reauth_expires_at_utc: new Date(200_000).toISOString(),
    plan_json: {
      requested_action: 'PRE_BANK_CANCEL',
      selected_candidate_count: 1,
      selected_active_item_count: 1,
      selected_amount_pence: 500,
      reason_hash: 'c'.repeat(64),
      evidence_hash: null,
      outcome_hash: null
    }
  };
  const context = {
    Set,
    String,
    Number,
    Date,
    Object,
    Array,
    requireBankingPayCancellationActor: async () => {
      calls.push('actor');
      return { ok: true, actorUserId };
    },
    parseBankingPayCancellationJsonBody: async (request) => request.body,
    bankingPayCorrectionUuid: (value) => String(value || '').toLowerCase(),
    bankingPayCancellationResponse: (_env, _req, statusCode, payload) => ({ status: statusCode, payload }),
    bankingPayCorrectionBodyErrorResponse: (_env, _req, error) => ({ status: 400, payload: { code: error.code || error.message } }),
    bankingPayCorrectionError: (_env, _req, error) => ({ status: 500, payload: { code: error.code || error.message } }),
    bankingPayCorrectionBoundedText: (value) => String(value).trim(),
    readBankingPayCorrectionRequestForWorker: async () => requestRow,
    verifyConsumedBankingPayCorrectionProofReplayV1: async () => {
      calls.push('consumed-proof');
      return { ok: true, proof_hash: 'd'.repeat(64), verified_payload: {}, consumed_replay: true };
    },
    verifyPaymentReversalReauth: async () => {
      calls.push('strict-proof');
      return { ok: true, proof_hash: 'd'.repeat(64), verified_payload: { expires_at_epoch_seconds: 200 } };
    },
    sbRpc: async (_env, rpc, args) => {
      calls.push({ rpc, args });
      const legacyResume = ['REQUESTED', 'AWAITING_AUTHORISATION'].includes(status)
        && requestRow.required_quantity === 1
        && requestRow.approved_count === 0;
      return {
        ok: true,
        is_existing: status !== 'PLANNED' && !legacyResume,
        code: legacyResume
          ? 'PAYMENT_CORRECTION_AUTHORISED'
          : (status === 'PLANNED' ? 'REQUEST_STARTED' : 'REQUEST_ALREADY_STARTED'),
        correction_request_id: IDS.request,
        operation_id: IDS.operation
      };
    },
    unwrapBankingPayCancellationRpc: (value) => value,
    enqueueBankingPayCancellationResult: async () => ({ ok: true, enqueued_count: 0 }),
    bankingPayCorrectionRpcHttpStatus: (_result, fallback) => fallback
  };
  vm.runInNewContext(
    `${functionBody('handleBankingPayCorrectionStartPreparedV1')}\nthis.startPrepared = handleBankingPayCorrectionStartPreparedV1;`,
    context
  );
  return { context, calls, requestRow };
}

test('generic START_PREPARED distinguishes a fresh start from an exact consumed-proof replay', () => {
  const body = functionBody('handleBankingPayCorrectionStartPreparedV1');
  const statusRead = body.indexOf('const requestStatus');
  const strictProof = body.indexOf('verifyPaymentReversalReauth', statusRead);
  const consumedProof = body.indexOf('verifyConsumedBankingPayCorrectionProofReplayV1', statusRead);
  assert.ok(statusRead > 0 && strictProof > statusRead && consumedProof > statusRead);
  for (const status of ['REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING','APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED']) {
    assert.match(body, new RegExp(`'${status}'`));
  }
  assert.match(body, /REQUEST_OWNER_REQUIRED/);
  assert.match(body, /REAUTH_REASON_MISMATCH/);
  assert.match(body, /command: 'START_PREPARED'/);
});

test('consumed proof replay verifies signature, exact stored hash, bindings and original consumption window', async () => {
  const helper = functionBody('verifyConsumedBankingPayCorrectionProofReplayV1');
  const token = 'signed-token';
  const expectedHash = crypto.createHash('sha256').update(token).digest('hex');
  const payload = {
    version: 1,
    correction_request_id: '11111111-1111-4111-8111-111111111111',
    pay_batch_id: '22222222-2222-4222-8222-222222222222',
    actor_user_id: '33333333-3333-4333-8333-333333333333',
    session_hash: 'e'.repeat(64),
    plan_hash: 'a'.repeat(64),
    selection_hash: 'b'.repeat(64),
    requested_action: 'PRE_BANK_CANCEL',
    selected_candidate_count: 1,
    selected_active_item_count: 2,
    selected_amount_pence: 500,
    reason_hash: 'c'.repeat(64),
    evidence_hash: null,
    outcome_hash: null,
    nonce: 'replay-proof-nonce',
    issued_at_epoch_seconds: 100,
    expires_at_epoch_seconds: 200
  };
  const context = {
    TextEncoder,
    String,
    Number,
    Date,
    Object,
    Array,
    BANKING_PAY_CORRECTION_SHA256_RE: /^[0-9a-f]{64}$/,
    BANKING_PAY_CORRECTION_PROOF_FIELDS: Object.freeze(Object.keys(payload)),
    verifyBankingPayCorrectionProof: async () => payload,
    sha256BankingPayRawText: async (value) => crypto.createHash('sha256').update(value).digest('hex'),
    isBankingPayCorrectionPlainObject: (value) => value !== null && typeof value === 'object' && !Array.isArray(value)
  };
  vm.runInNewContext(`${helper}\nthis.verifyReplay = verifyConsumedBankingPayCorrectionProofReplayV1;`, context);
  const request = {
    reauth_proof_hash: expectedHash,
    reauth_consumed_at_utc: new Date(150_000).toISOString(),
    reauth_expires_at_utc: new Date(200_000).toISOString()
  };
  const user = { id: payload.actor_user_id };
  const exact = await context.verifyReplay({}, user, token, request, payload);
  assert.equal(exact.ok, true);
  assert.equal(exact.consumed_replay, true);

  const wrongHash = await context.verifyReplay({}, user, `${token}-different`, request, payload);
  assert.equal(wrongHash.ok, false);
  assert.equal(wrongHash.code, 'REAUTH_PROOF_NOT_CONSUMED');

  const consumedLate = await context.verifyReplay({}, user, token, { ...request, reauth_consumed_at_utc: new Date(201_000).toISOString() }, payload);
  assert.equal(consumedLate.ok, false);
  assert.equal(consumedLate.code, 'REAUTH_PROOF_CONSUMPTION_INVALID');

  const changedBinding = await context.verifyReplay({}, user, token, request, { ...payload, pay_batch_id: '44444444-4444-4444-8444-444444444444' });
  assert.equal(changedBinding.ok, false);
  assert.equal(changedBinding.code, 'REAUTH_PROOF_BINDING_MISMATCH');

  const wrongActor = await context.verifyReplay({}, { id: '55555555-5555-4555-8555-555555555555' }, token, request, payload);
  assert.equal(wrongActor.ok, false);
  assert.equal(wrongActor.code, 'REAUTH_PROOF_ACTOR_MISMATCH');

  const unconsumed = await context.verifyReplay({}, user, token, { ...request, reauth_consumed_at_utc: null }, payload);
  assert.equal(unconsumed.ok, false);
  assert.equal(unconsumed.code, 'REAUTH_PROOF_NOT_CONSUMED');

  context.verifyBankingPayCorrectionProof = async () => null;
  const invalidHmac = await context.verifyReplay({}, user, token, request, payload);
  assert.equal(invalidHmac.ok, false);
  assert.equal(invalidHmac.code, 'REAUTH_PROOF_INVALID');
});

test('consumed replay does not relax the strict PLANNED proof path', () => {
  const body = functionBody('handleBankingPayCorrectionStartPreparedV1');
  assert.match(body, /requestStatus !== 'PLANNED' && !alreadyStarted/);
  assert.match(body, /alreadyStarted\s*\? await verifyConsumedBankingPayCorrectionProofReplayV1/);
  assert.match(body, /: await verifyPaymentReversalReauth\(env, user, user, token, expectedProof\)/);
  assert.match(body, /!alreadyStarted && \(!Number\.isFinite\(boundExpiry\)/);
});

test('already-started exact replay remains executable after proof expiry but only after actor/payment admission', async () => {
  const { context, calls } = executableStartPreparedHarness({ status: 'APPLIED' });
  const result = await context.startPrepared(
    {},
    { body: { correction_request_id: IDS.request, reauth_proof_token: 'already-consumed-token' } },
    { id: IDS.actor },
    IDS.batch
  );
  assert.equal(result.status, 200);
  assert.equal(result.payload.code, 'REQUEST_ALREADY_STARTED');
  assert.equal(calls[0], 'actor');
  assert.equal(calls[1], 'consumed-proof');
  assert.equal(calls.includes('strict-proof'), false);
  const rpc = calls.find((entry) => entry && typeof entry === 'object' && entry.rpc);
  assert.equal(rpc.rpc, 'pay_payment_correction_request_start');
  assert.equal(rpc.args.p_selection_json.command, 'START_PREPARED');
  assert.equal(rpc.args.p_selection_json.correction_request_id, IDS.request);
});

test('already-started replay fails closed for owner, reason, proof and unsupported-status mismatches', async () => {
  {
    const { context, calls } = executableStartPreparedHarness({ status: 'APPLIED', actorUserId: '55555555-5555-4555-8555-555555555555' });
    const result = await context.startPrepared({}, { body: { correction_request_id: IDS.request, reauth_proof_token: 'token' } }, { id: IDS.actor }, IDS.batch);
    assert.equal(result.status, 403);
    assert.equal(result.payload.code, 'REQUEST_OWNER_REQUIRED');
    assert.deepEqual(calls, ['actor']);
  }
  {
    const { context, calls } = executableStartPreparedHarness({ status: 'APPLIED' });
    const result = await context.startPrepared({}, { body: { correction_request_id: IDS.request, reauth_proof_token: 'token', reason: 'different' } }, { id: IDS.actor }, IDS.batch);
    assert.equal(result.status, 409);
    assert.equal(result.payload.code, 'REAUTH_REASON_MISMATCH');
    assert.deepEqual(calls, ['actor']);
  }
  {
    const { context, calls } = executableStartPreparedHarness({ status: 'APPLIED' });
    context.verifyConsumedBankingPayCorrectionProofReplayV1 = async () => {
      calls.push('consumed-proof');
      return { ok: false, code: 'REAUTH_PROOF_NOT_CONSUMED' };
    };
    const result = await context.startPrepared({}, { body: { correction_request_id: IDS.request, reauth_proof_token: 'token' } }, { id: IDS.actor }, IDS.batch);
    assert.equal(result.status, 403);
    assert.equal(result.payload.code, 'REAUTH_PROOF_INVALID');
    assert.equal(calls.some((entry) => entry && typeof entry === 'object' && entry.rpc), false);
  }
  {
    const { context, calls } = executableStartPreparedHarness({ status: 'PLANNING' });
    const result = await context.startPrepared({}, { body: { correction_request_id: IDS.request, reauth_proof_token: 'token' } }, { id: IDS.actor }, IDS.batch);
    assert.equal(result.status, 409);
    assert.equal(result.payload.code, 'REQUEST_NOT_READY_TO_START');
    assert.deepEqual(calls, ['actor']);
  }
});

test('fresh PLANNED start retains strict live proof and bound-expiry checks', async () => {
  const { context, calls } = executableStartPreparedHarness({ status: 'PLANNED' });
  const result = await context.startPrepared(
    {},
    { body: { correction_request_id: IDS.request, reauth_proof_token: 'fresh-token' } },
    { id: IDS.actor },
    IDS.batch
  );
  assert.equal(result.status, 202);
  assert.equal(calls[0], 'actor');
  assert.equal(calls[1], 'strict-proof');
  assert.equal(calls.includes('consumed-proof'), false);

  const rejected = executableStartPreparedHarness({ status: 'PLANNED' });
  rejected.context.verifyPaymentReversalReauth = async () => ({
    ok: true,
    proof_hash: 'd'.repeat(64),
    verified_payload: { expires_at_epoch_seconds: 201 }
  });
  const mismatch = await rejected.context.startPrepared(
    {},
    { body: { correction_request_id: IDS.request, reauth_proof_token: 'fresh-token' } },
    { id: IDS.actor },
    IDS.batch
  );
  assert.equal(mismatch.status, 403);
  assert.equal(mismatch.payload.code, 'REAUTH_PROOF_INVALID');
});

test('legacy one-authoriser REQUESTED/AWAITING shape reaches the unchanged SQL resume transition', async () => {
  for (const status of ['REQUESTED', 'AWAITING_AUTHORISATION']) {
    const { context, calls } = executableStartPreparedHarness({ status });
    const result = await context.startPrepared(
      {},
      { body: { correction_request_id: IDS.request, reauth_proof_token: 'already-consumed-token' } },
      { id: IDS.actor },
      IDS.batch
    );
    assert.equal(result.status, 202);
    assert.equal(result.payload.code, 'PAYMENT_CORRECTION_AUTHORISED');
    assert.equal(calls[0], 'actor');
    assert.equal(calls[1], 'consumed-proof');
    const rpc = calls.find((entry) => entry && typeof entry === 'object' && entry.rpc);
    assert.equal(rpc.rpc, 'pay_payment_correction_request_start');
  }
});

test('whole-Draft exact replay remains a separate fixed-shape guard', () => {
  const readReplay = functionBody('readExactDraftCancellationReplayV1');
  const draftHandler = functionBody('handleBankingPayBatchCancelV1');
  assert.match(readReplay, /DRAFT_CANCELLATION_REPLAY_AMBIGUOUS/);
  assert.match(readReplay, /source_context/);
  assert.match(readReplay, /pay_batch_cancel/);
  assert.match(draftHandler, /const exactReplay = await readExactDraftCancellationReplayV1/);
});
