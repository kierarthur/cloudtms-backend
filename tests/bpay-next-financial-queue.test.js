import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { handleBpayNextFinancialQueueMessage, BPAY_NEXT_FINANCIAL_QUEUE_LIMITS } from '../broker/src/bpay-next-financial-queue.js';

const JOB = '00000000-0000-4000-8000-000000000001';
const NONCE = '00000000-0000-4000-8000-000000000002';
const EFFECT = '00000000-0000-4000-8000-000000000003';
const EPOCH = '9007199254740993';
const CURSOR = '9007199254740995';
const NEXT = '9007199254740997';
const UNTIL = '2026-10-04T14:15:16.123456Z';
const KINDS = ['PAYE_NET_ENTRY', 'TRANSFER_BUILD', 'CSV_SETTLEMENT', 'CSV_RETURN', 'CASH_REISSUE', 'SIMPLE_CANCEL', 'REISSUE_CANCEL', 'INTERNAL_SETTLEMENT', 'CASE_CREATE'];
const PAGED = ['TRANSFER_BUILD', 'CSV_SETTLEMENT', 'SIMPLE_CANCEL', 'INTERNAL_SETTLEMENT'];

test('case NET CLAIM and each bounded page retain exact authority/cursor; REVIEW terminates locally', async () => {
  const claimed = { ...receipt('PAYE_NET_ENTRY', 'CLAIM'), phase: 'FINANCE_ALLOCATE', cursor: CURSOR };
  const initial = dependencies(claimed), first = message(envelope('PAYE_NET_ENTRY', 'CLAIM'));
  assert.equal((await handleBpayNextFinancialQueueMessage(first, initial)).reason, 'CONTINUATION_PERSISTED');
  assert.deepEqual(initial.sent, [{ version: 1, action: 'STEP', job_id: JOB, job_kind: 'PAYE_NET_ENTRY',
    lease_nonce: NONCE, owner_epoch: EPOCH, expected_phase: 'FINANCE_ALLOCATE', expected_cursor: CURSOR, limit: 100 }]);
  const call = { ...initial.sent[0], limit: 17 };
  const payload = { ...receipt('PAYE_NET_ENTRY'), phase: 'FINANCE_ALLOCATE', done: false,
    lease_until_utc: UNTIL, projection_id: null, cursor: NEXT, rows_visited: '17', run_worker_id: EFFECT, issue_code: null };
  const events = [], progress = dependencies(payload, events), page = message(call, events);
  assert.equal((await handleBpayNextFinancialQueueMessage(page, progress)).reason, 'CONTINUATION_PERSISTED');
  assert.deepEqual(events, ['rpc', 'send', 'durable', 'ack']);
  assert.deepEqual(progress.sent, [{ ...call, expected_cursor: NEXT }]);
  assert.equal(progress.calls.length, 1);
  const stagnant = dependencies({ ...payload, cursor: CURSOR, rows_visited: '0', replay: true });
  assert.equal((await handleBpayNextFinancialQueueMessage(message(call), stagnant)).disposition, 'RETRY');
  assert.equal(stagnant.sent.length, 0);
  const review = dependencies({ ...payload, phase: 'REVIEW', done: true, lease_until_utc: null,
    cursor: null, issue_code: 'CASE_RETURN_FLOOR_UNBOUND' });
  assert.equal((await handleBpayNextFinancialQueueMessage(message(call), review)).reason, 'DONE');
  assert.equal(review.sent.length, 0);
});

function envelope(kind = 'TRANSFER_BUILD', action = 'STEP') {
  return { version: 1, action, job_id: JOB, job_kind: kind,
    ...(action === 'CLAIM' ? { lease_seconds: 120 } : { lease_nonce: NONCE, owner_epoch: EPOCH,
      ...(PAGED.includes(kind) ? { expected_cursor: null, limit: 100 } : {}) }) };
}
function receipt(kind = 'TRANSFER_BUILD', action = 'STEP', done = false) {
  if (action === 'CLAIM') return { job_id: JOB, job_kind: kind,
    phase: ['PAYE_NET_ENTRY','SIMPLE_CANCEL','REISSUE_CANCEL','CASE_CREATE'].includes(kind) ? 'NEW' : kind === 'TRANSFER_BUILD' ? 'WORK' : 'MEMBERS',
    lease_nonce: NONCE, owner_epoch: EPOCH, lease_until_utc: UNTIL, cursor: null };
  const finished = done || !PAGED.includes(kind);
  const base = { job_id: JOB, job_kind: kind, owner_epoch: EPOCH,
    lease_until_utc: finished ? null : UNTIL, done: finished, replay: false };
  switch (kind) {
    case 'PAYE_NET_ENTRY': return { ...base, phase: 'PROJECTED', projection_id: EFFECT };
    case 'TRANSFER_BUILD': return { ...base, phase: finished ? 'MEMBERS_READY' : 'BUILDING',
      transfer_id: EFFECT, transfer_status: finished ? 'MEMBERS_READY' : 'BUILDING', cursor: CURSOR,
      member_count: CURSOR, rows_visited: '100' };
    case 'INTERNAL_SETTLEMENT':
    case 'CSV_SETTLEMENT': return { ...base, phase: finished ? 'COMPLETE' : 'MEMBERS', cursor: CURSOR,
      posted_members: CURSOR, rows_visited: '100' };
    case 'CSV_RETURN': return { ...base, phase: 'COMPLETE', return_cash_id: EFFECT };
    case 'CASH_REISSUE': return { ...base, phase: 'COMPLETE', transfer_id: EFFECT };
    case 'CASE_CREATE': return { ...base, phase: 'CASE_CREATED', case_id: EFFECT };
    case 'REISSUE_CANCEL': return { ...base, phase: 'CANCELLED', transfer_id: EFFECT, blocked_code: null };
    default: return { ...base, phase: finished ? 'CANCELLED' : 'CANCELLING', run_worker_id: EFFECT,
      cursor: CURSOR, released_line_count: CURSOR, rows_visited: '100', holds_released: '99', blocked_code: null };
  }
}
function message(body = envelope(), events = []) {
  return { id: 'queue-message-1', attempts: 1, body, events,
    ack() { assert.equal(this.body, body); this.events.push('ack'); },
    retry(options) { assert.equal(this.body, body); this.events.push(['retry', options]); } };
}
function dependencies(payload = receipt(), events = []) {
  const calls = [], sent = [], failures = [];
  return { calls, sent, failures,
    async financialJobRpc(action, jobId, jobKind, args, options) {
      calls.push({ action, jobId, jobKind, args, options }); events.push('rpc');
      return Response.json(payload);
    },
    async sendContinuation(body) {
      events.push('send'); sent.push(structuredClone(body)); events.push('durable');
    },
    async onFailure(summary) { failures.push(summary); events.push('failure'); return 'RETRY'; }
  };
}
const dbError = (code, message) => Response.json({ code, message }, { status: 400 });

for (const kind of KINDS) {
  test(`CLAIM ${kind} persists the exact new server token before ack; never executes STEP inline`, async () => {
    const events = [], value = envelope(kind, 'CLAIM'), msg = message(value, events);
    const deps = dependencies(receipt(kind, 'CLAIM'), events);
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'ACK', reason: 'CONTINUATION_PERSISTED' });
    assert.deepEqual(events, ['rpc', 'send', 'durable', 'ack']);
    assert.equal(deps.calls.length, 1); assert.equal(deps.calls[0].action, 'CLAIM');
    assert.deepEqual(deps.calls[0].args, { lease_seconds: 120 });
    assert.deepEqual(deps.sent, [envelope(kind)]);
    assert.deepEqual(msg.body, value); assert.equal(deps.failures.length, 0);
    assert.equal(deps.calls[0].options.timeoutMs, 8000);
    assert.equal(deps.calls[0].options.maxResponseBytes, 128 * 1024);
  });

  test(`DONE ${kind} acknowledges exactly once without another send/claim/step`, async () => {
    const events = [], msg = message(envelope(kind), events), deps = dependencies(receipt(kind, 'STEP', true), events);
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'ACK', reason: 'DONE' });
    assert.deepEqual(events, ['rpc', 'ack']);
    assert.equal(deps.calls.length, 1); assert.equal(deps.sent.length, 0); assert.equal(deps.failures.length, 0);
  });
}

for (const kind of PAGED) {
  test(`one unfinished ${kind} page publishes next cursor with original nonce/epoch/limit before ack`, async () => {
    const events = [], value = { ...envelope(kind), expected_cursor: CURSOR, limit: 17 };
    const payload = { ...receipt(kind), cursor: NEXT, rows_visited: '17',
      ...(kind === 'SIMPLE_CANCEL' ? { holds_released: '16' } : {}) };
    const deps = dependencies(payload, events), msg = message(value, events);
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'ACK', reason: 'CONTINUATION_PERSISTED' });
    assert.deepEqual(deps.sent, [{ ...value, expected_cursor: NEXT }]);
    assert.deepEqual(events, ['rpc', 'send', 'durable', 'ack']);
    assert.equal(deps.calls.length, 1); assert.equal(msg.body.expected_cursor, CURSOR);
    assert.equal(deps.sent[0].owner_epoch, EPOCH); assert.equal(deps.sent[0].lease_nonce, NONCE);
  });
}

test('CASE_CREATE durable token publication precedes ack; lost STEP retries original token then terminal replay acks', async () => {
  const events = [], deps = dependencies(receipt('CASE_CREATE', 'CLAIM'), events);
  const claim = message(envelope('CASE_CREATE', 'CLAIM'), events);
  assert.equal((await handleBpayNextFinancialQueueMessage(claim, deps)).reason, 'CONTINUATION_PERSISTED');
  assert.deepEqual(events, ['rpc', 'send', 'durable', 'ack']);
  const original = structuredClone(deps.sent[0]);
  assert.deepEqual(original, envelope('CASE_CREATE'));
  let approved = false, effects = 0;
  deps.financialJobRpc = async (action, jobId, jobKind, args) => {
    deps.calls.push({ action, jobId, jobKind, args });
    assert.equal(action, 'STEP'); assert.equal(jobKind, 'CASE_CREATE');
    assert.deepEqual(args, { lease_nonce: NONCE, owner_epoch: EPOCH });
    if (!approved) { approved = true; effects++; throw new Error('reply lost after real owner acceptance'); }
    return Response.json({ ...receipt('CASE_CREATE'), replay: true });
  };
  const lost = message(original);
  assert.equal((await handleBpayNextFinancialQueueMessage(lost, deps)).recovery, 'RETRY_SAME_STEP');
  assert.deepEqual(lost.events, [['retry', { delaySeconds: 30 }]]);
  assert.deepEqual(lost.body, original);
  for (let duplicate = 0; duplicate < 2; duplicate++) {
    const replay = message(original);
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(replay, deps), { disposition: 'ACK', reason: 'DONE' });
    assert.deepEqual(replay.events, ['ack']);
  }
  assert.equal(effects, 1); assert.equal(deps.sent.length, 1);
  assert.deepEqual(deps.calls[1], deps.calls[2]); assert.deepEqual(deps.calls[2], deps.calls[3]);
});

test('CASE_CREATE lost CLAIM publication and busy/predecessor/retry classifications never adopt an active nonce', async () => {
  for (const code of [null, 'BPAY_NEXT_JOB_LEASE_BUSY', 'BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR', 'BPAY_NEXT_JOB_CLAIM_RETRY']) {
    const original = envelope('CASE_CREATE', 'CLAIM'), msg = message(original), deps = dependencies();
    deps.financialJobRpc = async (...args) => {
      deps.calls.push(args); if (code === null) throw new Error('CLAIM response lost'); return dbError('55000', code);
    };
    assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).disposition, 'RETRY');
    assert.deepEqual(msg.body, original); assert.equal(deps.calls.length, 1);
    assert.equal(deps.sent.length, 0); assert.equal(deps.failures.length, 0);
    assert.equal(Object.hasOwn(msg.body, 'lease_nonce'), false);
  }
  const original = envelope('CASE_CREATE', 'CLAIM'), msg = message(original), deps = dependencies(receipt('CASE_CREATE', 'CLAIM'));
  deps.sendContinuation = async (next) => { deps.sent.push(next); throw new Error('durable send receipt lost'); };
  assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).recovery, 'WAIT_FOR_LEASE_EXPIRY');
  assert.deepEqual(msg.body, original); assert.deepEqual(msg.events, [['retry', { delaySeconds: 120 }]]);
  assert.equal(deps.calls.length, 1); assert.deepEqual(deps.sent, [envelope('CASE_CREATE')]);
});

test('CASE_CREATE rejects builder/approval fields and routes permanent lease failure through token-free recording', async () => {
  for (const extra of [{ expected_phase: 'NEW' }, { expected_cursor: null }, { limit: 100 },
    { case_id: EFFECT }, { actor_user_id: EFFECT }, { principal: '10.00' }]) {
    const deps = dependencies(), msg = message({ ...envelope('CASE_CREATE'), ...extra });
    assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).disposition, 'RETRY');
    assert.equal(deps.calls.length, 0); assert.equal(deps.sent.length, 0);
  }
  const deps = dependencies(), msg = message(envelope('CASE_CREATE'));
  deps.financialJobRpc = async () => dbError('55000', 'BPAY_NEXT_CASE_CREATE_LEASE_STALE');
  deps.onFailure = async (summary) => {
    deps.failures.push(summary);
    assert.equal(summary.job_kind, 'CASE_CREATE'); assert.equal(summary.error_code, 'BPAY_NEXT_JOB_NOT_ELIGIBLE');
    assert.equal(JSON.stringify(summary).includes(NONCE), false); return 'ACK_RECORDED';
  };
  assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).reason, 'FAILURE_RECORDED');
  assert.deepEqual(msg.events, ['ack']); assert.equal(deps.sent.length, 0);
});

test('a reclaimed CLAIM checkpoint is preserved as bigint text, not restarted from null', async () => {
  const payload = { ...receipt('SIMPLE_CANCEL', 'CLAIM'), phase: 'RELEASE', cursor: CURSOR, owner_epoch: '9223372036854775807' };
  const deps = dependencies(payload), msg = message(envelope('SIMPLE_CANCEL', 'CLAIM'));
  await handleBpayNextFinancialQueueMessage(msg, deps);
  assert.equal(deps.sent[0].expected_cursor, CURSOR);
  assert.equal(deps.sent[0].owner_epoch, '9223372036854775807');
  assert.equal(deps.sent[0].limit, 100);
  assert.deepEqual(Object.keys(deps.sent[0]).sort(), ['action', 'expected_cursor', 'job_id', 'job_kind', 'lease_nonce', 'limit', 'owner_epoch', 'version']);
});

test('ack remains absent while durable publication is pending', async () => {
  let releaseSend, enteredSend;
  const entered = new Promise((resolve) => { enteredSend = resolve; });
  const durable = new Promise((resolve) => { releaseSend = resolve; });
  const msg = message(envelope('PAYE_NET_ENTRY', 'CLAIM'));
  const deps = dependencies(receipt('PAYE_NET_ENTRY', 'CLAIM'));
  deps.sendContinuation = async (next) => { deps.sent.push(next); enteredSend(); await durable; };
  const pending = handleBpayNextFinancialQueueMessage(msg, deps);
  await entered;
  assert.deepEqual(msg.events, []); assert.equal(deps.calls.length, 1);
  releaseSend(); await pending;
  assert.deepEqual(msg.events, ['ack']);
});

test('definite DONE CLAIM is terminal: ack rather than indefinitely retry or adopt an old nonce', async () => {
  for (const kind of KINDS) {
    const deps = dependencies();
    deps.financialJobRpc = async () => dbError('55000', 'BPAY_NEXT_JOB_TERMINAL');
    const msg = message(envelope(kind, 'CLAIM'));
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'ACK', reason: 'TERMINAL' });
    assert.deepEqual(msg.events, ['ack']); assert.deepEqual(deps.sent, []); assert.deepEqual(deps.failures, []);
  }
});

test('busy, predecessor and contention retry classifications defer the same CLAIM without token adoption', async () => {
  for (const [code, recovery, delay] of [
    ['BPAY_NEXT_JOB_LEASE_BUSY', 'WAIT_FOR_LEASE_EXPIRY', 120],
    ['BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR', 'WAIT_FOR_PREDECESSOR', 30],
    ['BPAY_NEXT_JOB_CLAIM_RETRY', 'RETRY_CALL', 30]
  ]) {
    const original = envelope('TRANSFER_BUILD', 'CLAIM'), msg = message(original), deps = dependencies();
    deps.financialJobRpc = async (...args) => {
      deps.calls.push(args); return dbError('55000', code);
    };
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'RETRY', error_code: code, outcome_unknown: false, recovery });
    assert.deepEqual(msg.events, [['retry', { delaySeconds: delay }]]);
    assert.equal(deps.calls.length, 1); assert.deepEqual(deps.sent, []); assert.deepEqual(deps.failures, []);
    assert.deepEqual(msg.body, original); assert.equal(Object.hasOwn(msg.body, 'lease_nonce'), false);
  }
});

test('lost CLAIM result and malformed claim receipt retry the unchanged CLAIM, never a recovered/adopted STEP', async () => {
  for (const reply of [() => { throw new Error('secret transport details'); },
    () => Response.json({ ...receipt('TRANSFER_BUILD', 'CLAIM'), owner_epoch: 9007199254740993 })]) {
    const msg = message(envelope('TRANSFER_BUILD', 'CLAIM')), deps = dependencies();
    deps.financialJobRpc = async (...args) => { deps.calls.push(args); return reply(); };
    const result = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(result.disposition, 'RETRY'); assert.equal(result.outcome_unknown, true);
    assert.equal(result.recovery, 'WAIT_FOR_LEASE_EXPIRY');
    assert.deepEqual(msg.events, [['retry', { delaySeconds: 120 }]]);
    assert.equal(deps.calls.length, 1); assert.equal(deps.sent.length, 0);
    assert.equal(JSON.stringify(result).includes('secret'), false);
  }
});

test('lost STEP response retries EXACT nonce/epoch/cursor/limit; owner replay publishes its current checkpoint once', async () => {
  const value = { ...envelope(), expected_cursor: CURSOR, limit: 17 }, deps = dependencies();
  let advanced = false;
  deps.financialJobRpc = async (action, jobId, jobKind, args) => {
    deps.calls.push({ action, jobId, jobKind, args });
    if (!advanced) { advanced = true; throw new Error('response lost after page commit'); }
    return Response.json({ ...receipt(), cursor: NEXT, rows_visited: '0', replay: true });
  };
  const first = message(value);
  assert.equal((await handleBpayNextFinancialQueueMessage(first, deps)).recovery, 'RETRY_SAME_STEP');
  assert.deepEqual(first.events, [['retry', { delaySeconds: 30 }]]);
  assert.equal(deps.sent.length, 0);
  const second = message(value);
  await handleBpayNextFinancialQueueMessage(second, deps);
  assert.deepEqual(deps.calls[0], deps.calls[1]);
  assert.deepEqual(deps.sent, [{ ...value, expected_cursor: NEXT }]);
  assert.deepEqual(second.events, ['ack']); assert.equal(advanced, true);
});

test('failed continuation send after CLAIM retries CLAIM, while failed send after STEP retries its original page', async () => {
  for (const action of ['CLAIM', 'STEP']) {
    const value = envelope('TRANSFER_BUILD', action), msg = message(value), deps = dependencies(receipt('TRANSFER_BUILD', action));
    deps.sendContinuation = async (next) => { deps.sent.push(next); throw new Error('publication receipt unknown'); };
    const outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.deepEqual(outcome, { disposition: 'RETRY', error_code: 'BPAY_NEXT_QUEUE_CONTINUATION_SEND_UNKNOWN', outcome_unknown: true,
      recovery: action === 'CLAIM' ? 'WAIT_FOR_LEASE_EXPIRY' : 'RETRY_SAME_STEP' });
    assert.deepEqual(msg.events, [['retry', { delaySeconds: action === 'CLAIM' ? 120 : 30 }]]);
    assert.deepEqual(msg.body, value); assert.equal(deps.calls.length, 1); assert.equal(deps.sent.length, 1);
  }
});

test('publication succeeded but receipt was lost: duplicate CLAIM/STEP deliveries rely only on owner authority', async () => {
  const durable = [], deps = dependencies();
  let leased = false, done = false, effects = 0, loseSend = true;
  deps.financialJobRpc = async (action, jobId, jobKind, args) => {
    deps.calls.push({ action, jobId, jobKind, args });
    if (action === 'CLAIM') {
      if (done) return dbError('55000', 'BPAY_NEXT_JOB_TERMINAL');
      if (leased) return dbError('55000', 'BPAY_NEXT_JOB_LEASE_BUSY');
      leased = true; return Response.json(receipt('CSV_RETURN', 'CLAIM'));
    }
    assert.equal(args.lease_nonce, NONCE); assert.equal(args.owner_epoch, EPOCH);
    const replay = done;
    if (!done) { effects++; done = true; }
    return Response.json({ ...receipt('CSV_RETURN', 'STEP', true), replay });
  };
  deps.sendContinuation = async (next) => {
    durable.push(structuredClone(next));
    if (loseSend) { loseSend = false; throw new Error('ack lost after durable queue write'); }
  };
  const claim = envelope('CSV_RETURN', 'CLAIM');
  assert.equal((await handleBpayNextFinancialQueueMessage(message(claim), deps)).disposition, 'RETRY');
  assert.equal((await handleBpayNextFinancialQueueMessage(message(claim), deps)).recovery, 'WAIT_FOR_LEASE_EXPIRY');
  assert.equal(durable.length, 1);
  for (let delivery = 0; delivery < 2; delivery++) {
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(message(durable[0]), deps), { disposition: 'ACK', reason: 'DONE' });
  }
  assert.deepEqual(await handleBpayNextFinancialQueueMessage(message(claim), deps), { disposition: 'ACK', reason: 'TERMINAL' });
  assert.equal(effects, 1); assert.equal(deps.calls.length, 5);
});

test('no client timestamp or local expiry inference: even a past successful CLAIM receipt retains its exact authority', async () => {
  const deps = dependencies({ ...receipt('TRANSFER_BUILD', 'CLAIM'), lease_until_utc: '2001-01-01T00:00:00Z' });
  const msg = message(envelope('TRANSFER_BUILD', 'CLAIM'));
  await handleBpayNextFinancialQueueMessage(msg, deps);
  assert.deepEqual(deps.sent, [envelope()]);
  assert.equal(deps.calls.length, 1);
});

test('permanent missing/kind/scope/lease errors require an explicit durable failure disposition before ack', async () => {
  for (const [code, dbMessage, safeCode] of [
    ['P0002', 'BPAY_NEXT_JOB_NOT_FOUND', 'BPAY_NEXT_JOB_NOT_FOUND'],
    ['23514', 'BPAY_NEXT_JOB_KIND_MISMATCH', 'BPAY_NEXT_JOB_KIND_MISMATCH'],
    ['42501', 'private details', 'BPAY_NEXT_JOB_FORBIDDEN'],
    ['55000', 'BPAY_NEXT_CANCEL_PAGE_LEASE_INVALID', 'BPAY_NEXT_JOB_NOT_ELIGIBLE']
  ]) {
    const events = [], deps = dependencies(undefined, events), msg = message(envelope(), events);
    deps.financialJobRpc = async () => { events.push('rpc'); return dbError(code, dbMessage); };
    deps.onFailure = async (summary) => {
      events.push('failure'); deps.failures.push(summary);
      assert.equal(Object.isFrozen(summary), true);
      assert.deepEqual(Object.keys(summary).sort(), ['action', 'attempts', 'error_code', 'job_id', 'job_kind', 'message_id', 'outcome_unknown', 'recovery', 'retryable']);
      assert.equal(JSON.stringify(summary).includes(NONCE), false);
      assert.equal(summary.error_code, safeCode); assert.equal(summary.outcome_unknown, false);
      events.push('failure-durable'); return 'ACK_RECORDED';
    };
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'ACK', reason: 'FAILURE_RECORDED', error_code: safeCode });
    assert.deepEqual(events, ['rpc', 'failure', 'failure-durable', 'ack']);
    assert.equal(deps.sent.length, 0);
  }
});

test('failure callback rejecting, opting to retry, or giving an unrecognized receipt never acknowledges', async () => {
  for (const disposition of ['RETRY', undefined, 'ACK', { disposition: 'ACK_RECORDED' }, new Error('secret failure details')]) {
    const msg = message(envelope()), deps = dependencies();
    deps.financialJobRpc = async () => dbError('P0002', 'BPAY_NEXT_JOB_NOT_FOUND');
    deps.onFailure = async () => { if (disposition instanceof Error) throw disposition; return disposition; };
    const outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(outcome.disposition, 'RETRY'); assert.equal(outcome.outcome_unknown, false);
    assert.deepEqual(msg.events, [['retry', { delaySeconds: 30 }]]);
    assert.equal(JSON.stringify(outcome).includes('secret'), false);
  }
});

test('missing fixed RPC, durable publisher or failure recorder refuses financial work and retries original', async () => {
  for (const missing of ['financialJobRpc', 'sendContinuation', 'onFailure']) {
    const deps = dependencies(receipt('TRANSFER_BUILD', 'CLAIM')), msg = message(envelope('TRANSFER_BUILD', 'CLAIM'));
    delete deps[missing];
    const outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(outcome.error_code, 'BPAY_NEXT_QUEUE_DEPENDENCY_UNAVAILABLE');
    assert.deepEqual(msg.events, [['retry', { delaySeconds: 30 }]]);
    assert.equal(deps.calls.length, 0); assert.equal(deps.sent.length, 0);
  }
});

test('closed bounded envelopes reject caller actors/functions/money/discovery and malformed lease/page input before RPC', async () => {
  for (const body of [null, [], {}, envelope('PREPARE_RUN'), envelope('POSITION_REBUILD'),
    { ...envelope(), version: 2 }, { ...envelope(), actor_user_id: EFFECT },
    { ...envelope(), function: 'claim_anything' }, { ...envelope(), amount: '180.00' },
    { ...envelope(), candidate_id: EFFECT }, { ...envelope(), history: {} },
    { ...envelope(), owner_epoch: 9007199254740993 }, { ...envelope(), owner_epoch: '1e3' },
    { ...envelope(), expected_cursor: CURSOR, limit: 101 }, { ...envelope(), limit: '100' },
    { ...envelope('CSV_RETURN'), limit: 100 }, { ...envelope('CSV_RETURN'), expected_cursor: null },
    { ...envelope('TRANSFER_BUILD', 'CLAIM'), lease_nonce: NONCE },
    { ...envelope('TRANSFER_BUILD', 'CLAIM'), lease_seconds: 121 },
    { ...envelope(), job_id: 'a'.repeat(2100) }]) {
    const deps = dependencies(), msg = message(body);
    const outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(outcome.disposition, 'RETRY'); assert.equal(outcome.outcome_unknown, false);
    assert.match(outcome.error_code, /^BPAY_NEXT_(QUEUE_ENVELOPE|JOB_REQUEST)_INVALID$/);
    assert.equal(deps.calls.length, 0); assert.equal(deps.sent.length, 0); assert.equal(deps.failures.length, 1);
    assert.equal(deps.failures[0].job_kind === 'PREPARE_RUN', false);
    assert.equal(JSON.stringify(deps.failures[0]).includes(NONCE), false);
  }
});

test('malformed envelope without a failure recorder retries; exact schema forbids hidden/accessor/custom-object content', async () => {
  let getterCalls = 0;
  const accessor = { ...envelope() };
  Object.defineProperty(accessor, 'lease_nonce', { get() { getterCalls++; return NONCE; }, enumerable: true });
  const versionAccessor = { ...envelope() };
  Object.defineProperty(versionAccessor, 'version', { get() { getterCalls++; return 1; }, enumerable: true });
  const hidden = { ...envelope() }; Object.defineProperty(hidden, 'actor_user_id', { value: EFFECT });
  const symbol = { ...envelope(), [Symbol('actor')]: EFFECT };
  const custom = Object.assign(Object.create({}), envelope());
  for (const body of [accessor, versionAccessor, hidden, symbol, custom, new Date(), { version: 1, action: 'STEP', job_id: JOB, job_kind: 'TRANSFER_BUILD', lease_nonce: {} }]) {
    const deps = dependencies(), msg = message(body);
    delete deps.onFailure;
    assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).disposition, 'RETRY');
    assert.deepEqual(msg.events, [['retry', { delaySeconds: 30 }]]); assert.equal(deps.calls.length, 0);
  }
  assert.equal(getterCalls, 0);
});

test('unfinished unchanged/backward/null cursor never creates a same-page continuation loop', async () => {
  for (const cursor of [CURSOR, '1', null]) {
    const value = { ...envelope(), expected_cursor: CURSOR }, deps = dependencies({ ...receipt(), cursor, rows_visited: '0', replay: true });
    const msg = message(value), outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(outcome.error_code, 'BPAY_NEXT_QUEUE_CONTINUATION_INVALID'); assert.equal(outcome.outcome_unknown, true);
    assert.deepEqual(msg.events, [['retry', { delaySeconds: 30 }]]);
    assert.equal(deps.calls.length, 1); assert.equal(deps.sent.length, 0); assert.deepEqual(msg.body, value);
  }
});

test('DONE replay retains owner authority after cleared lease/epoch and does not create another checkpoint', async () => {
  const deps = dependencies({ ...receipt('TRANSFER_BUILD', 'STEP', true), owner_epoch: '1', replay: true, rows_visited: '0' });
  const msg = message(envelope());
  assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'ACK', reason: 'DONE' });
  assert.deepEqual(msg.events, ['ack']); assert.equal(deps.sent.length, 0);
});

test('missing Message disposition methods throws rather than permit implicit batch acknowledgment', async () => {
  await assert.rejects(() => handleBpayNextFinancialQueueMessage({ body: envelope() }, dependencies()), /BPAY_NEXT_QUEUE_MESSAGE_INTERFACE_INVALID/);
});

test('implementation is one bounded adapter call, no batch drain, client clock, arbitrary SQL or browser route', () => {
  const source = readFileSync(new URL('../broker/src/bpay-next-financial-queue.js', import.meta.url), 'utf8');
  assert.equal((source.match(/await stepBpayNextFinancialJob\(/g) || []).length, 1);
  assert.match(source, /await dependencies\.sendContinuation\(next\)/);
  assert.doesNotMatch(source, /\b(fetch\s*\(|while\s*\(|setInterval\s*\(|Date\.now\s*\(|ackAll\s*\(|retryAll\s*\(|waitUntil\s*\(|requireOfficeUser\s*\()/);
  assert.deepEqual(BPAY_NEXT_FINANCIAL_QUEUE_LIMITS, { envelopeBytes: 2048, pageRows: 100, retrySeconds: 30, claimWaitSeconds: 120 });
});

const COMPONENT = 'TS_DAY:2026-10-04:retained Ω component';
function positionEnvelope(phase = 'NEW', cursor = null) {
  return { ...envelope('POSITION_APPLY'), expected_phase: phase, expected_cursor: cursor, limit: 100 };
}
function positionPayload(phase = 'NEW', cursor = COMPONENT) {
  return { job_id: JOB, job_kind: 'POSITION_APPLY', owner_epoch: EPOCH,
    lease_until_utc: phase === 'DONE' ? null : UNTIL, done: phase === 'DONE', phase, cursor, rows_visited: '0' };
}
function prepareEnvelope(phase = 'NEW') {
  return { ...envelope('PREPARE'), expected_phase: phase,
    ...(['NEW', 'PINNING'].includes(phase) ? { expected_cursor: null }
      : ['RESERVE', 'ALLOCATE'].includes(phase) ? { expected_work_cursor: null, expected_component_cursor: null, limit: 100 }
        : phase === 'FINANCE_ALLOCATE' ? { expected_cursor: null, limit: 100 } : {}) };
}
function preparePayload(phase = 'PINNING') {
  const done = ['READY', 'REVIEW'].includes(phase);
  return { job_id: JOB, job_kind: 'PREPARE', owner_epoch: EPOCH, lease_until_utc: done ? null : UNTIL,
    done, replay: false, phase, cursor: phase === 'PINNING' ? CURSOR : null,
    work_cursor: null, component_cursor: null, run_worker_id: EFFECT,
    worker_status: phase === 'REVIEW' ? 'REVIEW' : done ? 'READY' : 'PREPARING',
    issue_code: phase === 'REVIEW' ? 'FINANCIAL_RESOLUTION_REQUIRED' : null,
    issue_work_id: phase === 'REVIEW' ? EFFECT : null, rows_visited: phase === 'PINNING' ? '1' : null };
}

test('POSITION CLAIM persists phase plus exact Unicode component cursor, not an assumed bigint', async () => {
  const component = '😀'.repeat(256);
  const payload = { job_id: JOB, job_kind: 'POSITION_APPLY', phase: 'REMOVED', lease_nonce: NONCE,
    owner_epoch: EPOCH, lease_until_utc: UNTIL, cursor: component };
  const events = [], deps = dependencies(payload, events), msg = message(envelope('POSITION_APPLY', 'CLAIM'), events);
  await handleBpayNextFinancialQueueMessage(msg, deps);
  assert.deepEqual(deps.sent, [positionEnvelope('REMOVED', component)]);
  assert.deepEqual(events, ['rpc', 'send', 'durable', 'ack']); assert.equal(deps.calls.length, 1);
});

test('PREPARE CLAIM persists the exact one next-phase input, preserving selection/UUID/component identity', async () => {
  for (const phase of ['NEW', 'PINNING', 'RESERVE', 'ALLOCATE', 'FINANCE_ALLOCATE']) {
    const payload = { job_id: JOB, job_kind: 'PREPARE', phase, lease_nonce: NONCE, owner_epoch: EPOCH,
      lease_until_utc: UNTIL, cursor: phase === 'PINNING' ? CURSOR : null,
      work_cursor: ['RESERVE', 'ALLOCATE'].includes(phase) ? EFFECT : null,
      component_cursor: ['RESERVE', 'ALLOCATE'].includes(phase) ? COMPONENT : null };
    const deps = dependencies(payload), msg = message(envelope('PREPARE', 'CLAIM'));
    await handleBpayNextFinancialQueueMessage(msg, deps);
    const expected = prepareEnvelope(phase);
    if (phase === 'PINNING') expected.expected_cursor = CURSOR;
    if (['RESERVE', 'ALLOCATE'].includes(phase)) {
      expected.expected_work_cursor = EFFECT; expected.expected_component_cursor = COMPONENT;
    }
    assert.deepEqual(deps.sent, [expected]); assert.deepEqual(msg.events, ['ack']); assert.equal(deps.calls.length, 1);
  }
});

test('POSITION one page or phase transition replaces only the owner-returned checkpoint; final replay acks', async () => {
  for (const [call, payload, expected] of [
    [positionEnvelope(), positionPayload('NEW', COMPONENT), positionEnvelope('NEW', COMPONENT)],
    [positionEnvelope('NEW', COMPONENT), positionPayload('REMOVED', null), positionEnvelope('REMOVED', null)],
    [positionEnvelope('REMOVED', COMPONENT), positionPayload('DONE', null), null]
  ]) {
    const deps = dependencies(payload), msg = message(call);
    const outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(outcome.disposition, 'ACK'); assert.equal(deps.calls.length, 1);
    assert.deepEqual(deps.sent, expected ? [expected] : []); assert.deepEqual(msg.events, ['ack']);
  }
});

test('PREPARE finite phase journey carries exact state, resets tuples only on owner transitions, and never drains inline', async () => {
  const reserve = { ...prepareEnvelope('RESERVE'), expected_work_cursor: EFFECT, expected_component_cursor: COMPONENT };
  const allocate = { ...prepareEnvelope('ALLOCATE'), expected_work_cursor: EFFECT, expected_component_cursor: COMPONENT };
  const vectors = [
    [prepareEnvelope('NEW'), preparePayload('PINNING'), { ...prepareEnvelope('PINNING'), expected_cursor: CURSOR }],
    [{ ...prepareEnvelope('PINNING'), expected_cursor: CURSOR }, preparePayload('RESERVE'), prepareEnvelope('RESERVE')],
    [prepareEnvelope('RESERVE'), { ...preparePayload('RESERVE'), work_cursor: EFFECT, component_cursor: COMPONENT }, reserve],
    [reserve, preparePayload('ALLOCATE'), prepareEnvelope('ALLOCATE')],
    [prepareEnvelope('ALLOCATE'), { ...preparePayload('ALLOCATE'), work_cursor: EFFECT, component_cursor: COMPONENT }, allocate],
    [allocate, preparePayload('FINANCE_ALLOCATE'), prepareEnvelope('FINANCE_ALLOCATE')],
    [prepareEnvelope('FINANCE_ALLOCATE'), preparePayload('READY'), null]
  ];
  for (const [value, payload, next] of vectors) {
    const events = [], deps = dependencies(payload, events), msg = message(value, events);
    assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).disposition, 'ACK');
    assert.equal(deps.calls.length, 1); assert.deepEqual(deps.sent, next ? [next] : []);
    assert.deepEqual(events, next ? ['rpc', 'send', 'durable', 'ack'] : ['rpc', 'ack']);
    assert.deepEqual(msg.body, value);
  }
});

test('same PREPARE pair page retains its original limit rather than reset to a whole Candidate drain', async () => {
  const value = { ...prepareEnvelope('RESERVE'), expected_work_cursor: EFFECT, expected_component_cursor: COMPONENT, limit: 17 };
  const nextComponent = `${COMPONENT}:next`;
  const deps = dependencies({ ...preparePayload('RESERVE'), work_cursor: EFFECT, component_cursor: nextComponent, rows_visited: '17' });
  await handleBpayNextFinancialQueueMessage(message(value), deps);
  assert.deepEqual(deps.sent, [{ ...value, expected_component_cursor: nextComponent }]);
  assert.equal(deps.calls.length, 1);
});

test('PREPARE READY/REVIEW and POSITION final replay acknowledge only the authoritative terminal owner result', async () => {
  for (const [value, payload] of [
    [{ ...positionEnvelope(), owner_epoch: '1' }, positionPayload('DONE', null)],
    [{ ...prepareEnvelope('RESERVE'), owner_epoch: '1' }, { ...preparePayload('READY'), worker_status: 'CANCELLED', replay: true, rows_visited: '0' }],
    [prepareEnvelope('ALLOCATE'), { ...preparePayload('REVIEW'), replay: true, rows_visited: null }]
  ]) {
    const deps = dependencies(payload), msg = message(value);
    assert.deepEqual(await handleBpayNextFinancialQueueMessage(msg, deps), { disposition: 'ACK', reason: 'DONE' });
    assert.deepEqual(msg.events, ['ack']); assert.equal(deps.calls.length, 1); assert.equal(deps.sent.length, 0);
  }
});

test('new kinds unknown transport/send outcome preserves the EXACT original phase and cursor tuple for retry', async () => {
  for (const value of [positionEnvelope('REMOVED', COMPONENT),
    { ...prepareEnvelope('RESERVE'), expected_work_cursor: EFFECT, expected_component_cursor: COMPONENT }]) {
    for (const failedStage of ['rpc', 'send']) {
      const payload = value.job_kind === 'POSITION_APPLY' ? positionPayload('REMOVED', `${COMPONENT}:next`)
        : { ...preparePayload('RESERVE'), work_cursor: EFFECT, component_cursor: `${COMPONENT}:next` };
      const deps = dependencies(payload), original = structuredClone(value), msg = message(value);
      if (failedStage === 'rpc') deps.financialJobRpc = async (...args) => { deps.calls.push(args); throw new Error('unknown commit'); };
      else deps.sendContinuation = async (next) => { deps.sent.push(next); throw new Error('unknown publication'); };
      const result = await handleBpayNextFinancialQueueMessage(msg, deps);
      assert.equal(result.recovery, 'RETRY_SAME_STEP'); assert.equal(result.outcome_unknown, true);
      assert.deepEqual(msg.body, original); assert.deepEqual(msg.events, [['retry', { delaySeconds: 30 }]]);
      assert.equal(deps.calls.length, 1);
    }
  }
});

test('new kind CLAIM unknown/busy/predecessor retries remain CLAIM, never manufacture a phase or active nonce', async () => {
  for (const kind of ['POSITION_APPLY', 'PREPARE']) for (const code of [null, 'BPAY_NEXT_JOB_LEASE_BUSY', 'BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR']) {
    const deps = dependencies(), value = envelope(kind, 'CLAIM'), msg = message(value);
    deps.financialJobRpc = async (...args) => {
      deps.calls.push(args);
      if (code === null) throw new Error('lost CLAIM reply');
      return dbError('55000', code);
    };
    const result = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(result.disposition, 'RETRY'); assert.equal(deps.calls.length, 1); assert.equal(deps.sent.length, 0);
    assert.deepEqual(msg.body, value); assert.equal(Object.hasOwn(msg.body, 'lease_nonce'), false);
    assert.equal(Object.hasOwn(msg.body, 'expected_phase'), false);
  }
});

test('a retained unsupported-owner response keeps its definite failure handoff; no fake next READY', async () => {
  const deps = dependencies(), value = prepareEnvelope('FINANCE_ALLOCATE'), msg = message(value);
  deps.financialJobRpc = async (...args) => { deps.calls.push(args); return dbError('55000', 'BPAY_NEXT_CASE_ALLOCATION_REQUIRED'); };
  const outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
  assert.equal(outcome.error_code, 'BPAY_NEXT_JOB_CASE_ALLOCATION_UNSUPPORTED');
  assert.equal(outcome.disposition, 'RETRY'); assert.equal(outcome.outcome_unknown, false);
  assert.equal(deps.calls.length, 1); assert.deepEqual(deps.sent, []);
  assert.equal(deps.failures[0].job_kind, 'PREPARE'); assert.deepEqual(msg.body, value);
});

test('new kind malformed phase/partial cursor/case or money fields do not reach any owner', async () => {
  for (const body of [
    { ...positionEnvelope(), expected_phase: 'DONE' },
    { ...positionEnvelope(), expected_cursor: '😀'.repeat(257) },
    { ...prepareEnvelope('RESERVE'), expected_work_cursor: EFFECT },
    { ...prepareEnvelope('RESERVE'), expected_component_cursor: COMPONENT },
    { ...prepareEnvelope('NEW'), expected_cursor: 9007199254740995 },
    { ...prepareEnvelope('NEW'), expected_phase: 'CASE_ALLOCATE' },
    { ...prepareEnvelope('FINANCE_ALLOCATE'), expected_cursor: 1 },
    { ...prepareEnvelope('FINANCE_ALLOCATE'), amount: '180.00' },
    { ...prepareEnvelope('FINANCE_ALLOCATE'), case_id: EFFECT }
  ]) {
    const deps = dependencies(), msg = message(body);
    assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).disposition, 'RETRY');
    assert.equal(deps.calls.length, 0); assert.equal(deps.sent.length, 0);
  }
});

test('case finance pages continue once at a strictly later checkpoint, retain limit, and finish Candidate REVIEW', async () => {
  const value = { ...prepareEnvelope('FINANCE_ALLOCATE'), expected_cursor: CURSOR, limit: 17 };
  const nextCursor = String(BigInt(CURSOR) + 1n);
  const deps = dependencies({ ...preparePayload('FINANCE_ALLOCATE'), cursor: nextCursor, rows_visited: '17' });
  const msg = message(value);
  assert.equal((await handleBpayNextFinancialQueueMessage(msg, deps)).disposition, 'ACK');
  assert.deepEqual(deps.sent, [{ ...value, expected_cursor: nextCursor }]);
  assert.equal(deps.calls.length, 1);
  for (const cursor of [null, CURSOR, String(BigInt(CURSOR) - 1n)]) {
    const stalled = dependencies({ ...preparePayload('FINANCE_ALLOCATE'), cursor, rows_visited: '0' });
    const outcome = await handleBpayNextFinancialQueueMessage(message(value), stalled);
    assert.equal(outcome.error_code, 'BPAY_NEXT_QUEUE_CONTINUATION_INVALID');
    assert.equal(stalled.sent.length, 0);
  }
  const review = dependencies({ ...preparePayload('REVIEW'), issue_code: 'CASE_SELECTION_REQUIRED', issue_work_id: null, rows_visited: '0' });
  assert.deepEqual(await handleBpayNextFinancialQueueMessage(message(value), review), { disposition: 'ACK', reason: 'DONE' });
  assert.equal(review.sent.length, 0); assert.equal(review.failures.length, 0);
});

test('an unchanged new-kind phase/cursor cannot create an indefinite same-state continuation loop', async () => {
  for (const [value, payload] of [
    [positionEnvelope('NEW', COMPONENT), positionPayload('NEW', COMPONENT)],
    [{ ...prepareEnvelope('PINNING'), expected_cursor: CURSOR }, { ...preparePayload('PINNING'), replay: true, rows_visited: '0' }],
    [prepareEnvelope('RESERVE'), preparePayload('RESERVE')],
    [prepareEnvelope('FINANCE_ALLOCATE'), preparePayload('FINANCE_ALLOCATE')]
  ]) {
    const deps = dependencies(payload), msg = message(value), outcome = await handleBpayNextFinancialQueueMessage(msg, deps);
    assert.equal(outcome.error_code, 'BPAY_NEXT_QUEUE_CONTINUATION_INVALID'); assert.equal(outcome.outcome_unknown, true);
    assert.equal(deps.calls.length, 1); assert.equal(deps.sent.length, 0);
    assert.deepEqual(msg.events, [['retry', { delaySeconds: 30 }]]);
  }
});
