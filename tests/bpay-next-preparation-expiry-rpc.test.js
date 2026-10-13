import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import {
  BPAY_NEXT_PREPARATION_EXPIRY_LIMITS,
  createBpayNextPreparationExpiryRpc,
  readBpayNextPreparationExpiryDue,
  receiveBpayNextPreparationExpiry
} from '../broker/src/bpay-next-preparation-expiry-rpc.js';

// Synthetic unsigned JWT shape only. Every fetch in this file is injected;
// no test reaches PostgREST, a database, a hosted Worker or another network.
const jwt = (claims = { role: 'service_role' }) =>
  `e30.${Buffer.from(JSON.stringify(claims)).toString('base64url')}.c3ludGhldGlj`;
const serviceHeaders = () => ({ Authorization: `Bearer ${jwt()}` });
const RUN = 'a1000000-0000-0000-0000-000000000001';
const OTHER = 'a1000000-0000-0000-0000-000000000002';
const COMMAND = '70000000-0000-0000-0000-000000000001';
const TIME = '2026-10-01T12:13:14.000001Z';
const dueInput = (changes = {}) => ({ after_deadline: null, after_run_id: null, limit: 100, ...changes });
const item = (run_id = RUN, expected_deadline = TIME) => ({ run_id, expected_deadline });
const page = (items = [item()]) => ({ items,
  cursor_deadline: items.at(-1)?.expected_deadline ?? null,
  cursor_run_id: items.at(-1)?.run_id ?? null });
const receipt = (changes = {}) => ({ command_id: COMMAND, run_id: RUN, sequence: '9007199254740993',
  phase: 'CANCELLING', expected_candidates: '2', completed_candidates: '1', replay: false, ...changes });
const json = (payload, status = 200, headers = {}) => new Response(JSON.stringify(payload), {
  status, headers: { 'content-type': 'application/json', ...headers }
});
const textResponse = (text, headers = {}, status = 200) => new Response(text, {
  status, headers: { 'content-type': 'application/json', ...headers }
});
const deps = (raw, extra = {}) => ({ preparationExpiryRpc: raw, ...extra });
const rawResult = (payload, extra) => deps(async () => json(payload), extra);
function failure(result, code, unknown = false, retryable = false, recovery = 'NONE') {
  assert.deepEqual(result, { ok: false, error_code: `BPAY_NEXT_EXPIRY_${code}`,
    retryable, outcome_unknown: unknown, recovery });
  assert.equal(Object.isFrozen(result), true);
}
function streamResponse(chunks, { closed = true, cancel = () => {}, headers = {}, status = 200 } = {}) {
  let controller;
  const stream = new ReadableStream({ start(value) {
    controller = value;
    for (const chunk of chunks) value.enqueue(typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk);
    if (closed) value.close();
  }, cancel });
  return { response: new Response(stream, { status, headers: { 'content-type': 'application/json', ...headers } }), controller };
}

test('raw transport fixes both public RPCs, exact args, signal and redirect policy', async () => {
  const calls = [];
  const originalHeaders = new Headers(serviceHeaders());
  originalHeaders.set('x-task', 'original');
  const raw = createBpayNextPreparationExpiryRpc({ baseUrl: 'http://127.0.0.1:54321/', headers: originalHeaders,
    fetchImpl: async (url, options) => { calls.push({ url, options }); return json(page()); } });
  originalHeaders.set('authorization', 'Bearer changed-after-construction');
  originalHeaders.set('x-task', 'changed');
  const signal = new AbortController().signal;
  const due = { p_after_deadline: TIME, p_after_run_id: RUN, p_limit: 1 };
  assert.ok(await raw('DUE', due, { signal }) instanceof Response);
  calls[0].options.headers.set('x-task', 'changed-one-request');
  const receive = { p_run_id: RUN };
  await raw('RECEIVE', receive, { signal });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].url, 'http://127.0.0.1:54321/rest/v1/rpc/bpay_next_preparation_expiry_due_v1');
  assert.equal(calls[1].url, 'http://127.0.0.1:54321/rest/v1/rpc/bpay_next_preparation_expiry_receive_v1');
  for (const { options } of calls) {
    assert.equal(options.method, 'POST'); assert.equal(options.redirect, 'error');
    assert.equal(options.signal, signal); assert.equal(options.headers.get('content-type'), 'application/json');
    assert.equal(options.headers.get('accept'), 'application/json');
    assert.equal(options.headers.get('authorization'), serviceHeaders().Authorization);
  }
  assert.equal(calls[1].options.headers.get('x-task'), 'original');
  assert.equal(calls[0].options.headers.get('X-CloudTMS-Banking-Pay-Route-Class'), 'DISPLAY');
  assert.equal(calls[1].options.headers.get('X-CloudTMS-Banking-Pay-Route-Class'), 'OPERATION_NUDGE');
  assert.deepEqual(JSON.parse(calls[0].options.body), due);
  assert.deepEqual(JSON.parse(calls[1].options.body), receive);
});

test('raw factory accepts HTTPS or local HTTP and requires a service JWT shape', () => {
  for (const baseUrl of ['https://example.invalid', 'http://localhost:54321', 'http://[::1]:54321']) {
    assert.equal(typeof createBpayNextPreparationExpiryRpc({ baseUrl, headers: serviceHeaders(), fetchImpl: async () => {} }), 'function');
  }
  for (const baseUrl of ['', 'not-url', 'ftp://example.invalid', 'http://example.invalid',
    'https://user:password@example.invalid', 'https://example.invalid?key=token', 'https://example.invalid#fragment']) {
    assert.throws(() => createBpayNextPreparationExpiryRpc({ baseUrl, headers: serviceHeaders() }), /BPAY_NEXT_EXPIRY_TRANSPORT_INVALID/);
  }
  for (const headers of [{}, { Authorization: 'Bearer not-jwt' },
    { Authorization: `Bearer ${jwt({ role: 'authenticated' })}` },
    { Authorization: `Bearer ${jwt({ role: 'anon' })}` },
    { Authorization: `Bearer ${jwt({})}` }, { Authorization: 'Bearer e30.e30.c2ln' }]) {
    assert.throws(() => createBpayNextPreparationExpiryRpc({ baseUrl: 'https://example.invalid', headers }), /BPAY_NEXT_EXPIRY_TRANSPORT_INVALID/);
  }
  for (const fetchImpl of [null, false, 'fetch']) {
    assert.throws(() => createBpayNextPreparationExpiryRpc({ baseUrl: 'https://example.invalid', headers: serviceHeaders(), fetchImpl }), /BPAY_NEXT_EXPIRY_TRANSPORT_INVALID/);
  }
});

test('raw transport denies arbitrary RPC, private clock, actors and extra fields before network', async () => {
  let network = 0;
  const raw = createBpayNextPreparationExpiryRpc({ baseUrl: 'https://example.invalid', headers: serviceHeaders(),
    fetchImpl: async () => { network++; return json(receipt()); } });
  const signal = new AbortController().signal;
  for (const [action, args, options] of [
    ['private.bpay_next_accept_preparation_expiry_v1', { p_run_id: RUN }, { signal }],
    ['CLAIM', { p_run_id: RUN }, { signal }], ['STEP', { p_run_id: RUN }, { signal }],
    [{ toString: () => 'DUE' }, { p_run_id: RUN }, { signal }],
    ['RECEIVE', { p_run_id: RUN, p_clock: TIME }, { signal }],
    ['RECEIVE', { p_run_id: RUN, p_actor_user_id: OTHER }, { signal }],
    ['RECEIVE', { p_run_id: RUN, p_command_id: COMMAND }, { signal }],
    ['RECEIVE', { p_run_id: RUN }, { signal, actorUserId: OTHER }],
    ['RECEIVE', { p_run_id: RUN }, { signal: {} }], ['RECEIVE', { p_run_id: RUN }, undefined],
    ['DUE', { p_after_deadline: TIME, p_after_run_id: null, p_limit: 1 }, { signal }],
    ['DUE', { p_after_deadline: null, p_after_run_id: null, p_limit: 101 }, { signal }]
  ]) await assert.rejects(raw(action, args, options), /BPAY_NEXT_EXPIRY_TRANSPORT_INVALID/);
  await assert.rejects(raw('RECEIVE', { p_run_id: RUN }, { signal }, { actor: OTHER }), /BPAY_NEXT_EXPIRY_TRANSPORT_INVALID/);
  assert.equal(network, 0);
});

test('due adapter preserves all six microsecond digits and exact cursor, with one call only', async () => {
  const payload = page([item(RUN, TIME), item(OTHER, '2026-10-01T12:13:14.000002Z')]);
  let calls = 0;
  const result = await readBpayNextPreparationExpiryDue(dueInput({ limit: 2 }), deps(async (action, args, options) => {
    calls++; assert.equal(action, 'DUE');
    assert.deepEqual(args, { p_after_deadline: null, p_after_run_id: null, p_limit: 2 });
    assert.equal(Object.isFrozen(args), true); assert.deepEqual(Object.keys(options), ['signal']);
    assert.ok(options.signal instanceof AbortSignal);
    return json(payload);
  }));
  assert.deepEqual(result, { ok: true, action: 'DUE', result: payload });
  assert.equal(calls, 1); assert.equal(Object.isFrozen(result.result.items), true);
  assert.equal(Object.isFrozen(result.result.items[0]), true);
  assert.notEqual(result.result.items[0].expected_deadline, result.result.items[1].expected_deadline);
});

test('due input calendar validates UTC without rounding or timezone conversion', async () => {
  for (const timestamp of ['2024-02-29T00:00:00.123456Z', '2000-02-29T23:59:59Z',
    '2026-10-01T12:13:14.1Z', '0001-01-01T00:00:00.000001Z']) {
    const result = await readBpayNextPreparationExpiryDue(dueInput({ after_deadline: timestamp, after_run_id: RUN }),
      deps(async (_action, args) => { assert.equal(args.p_after_deadline, timestamp); return json(page([])); }));
    assert.equal(result.ok, true);
  }
  let calls = 0;
  const noCall = deps(async () => { calls++; return json(page()); });
  for (const timestamp of ['2026-02-29T00:00:00Z', '1900-02-29T00:00:00Z', '2026-04-31T00:00:00Z',
    '0000-01-01T00:00:00Z', '2026-13-01T00:00:00Z', '2026-10-00T00:00:00Z',
    '2026-10-01T24:00:00Z', '2026-10-01T12:13:60Z', '2026-10-01T12:13:14.1234567Z',
    '2026-10-01T12:13:14+00:00', '2026-10-01', 'infinity', new Date()]) {
    failure(await readBpayNextPreparationExpiryDue(dueInput({ after_deadline: timestamp, after_run_id: RUN }), noCall), 'REQUEST_INVALID');
  }
  assert.equal(calls, 0);
});

test('closed due/receive inputs and dependency options refuse before any action', async () => {
  let calls = 0;
  const noCall = deps(async () => { calls++; return json(page()); });
  for (const input of [null, [], {}, dueInput({ limit: 0 }), dueInput({ limit: 101 }),
    dueInput({ limit: 1.5 }), dueInput({ limit: '1' }), dueInput({ limit: null }),
    dueInput({ after_deadline: TIME }), dueInput({ after_run_id: RUN }),
    dueInput({ after_deadline: TIME, after_run_id: 'not-uuid' }),
    dueInput({ actor_user_id: OTHER }), dueInput({ clock: TIME }), dueInput({ money: '220.00' }),
    Object.create(dueInput()), Object.assign(dueInput(), { [Symbol('hidden')]: true })]) {
    failure(await readBpayNextPreparationExpiryDue(input, noCall), 'REQUEST_INVALID');
  }
  const getter = dueInput(); Object.defineProperty(getter, 'limit', { enumerable: true, get() { throw new Error('must not execute'); } });
  failure(await readBpayNextPreparationExpiryDue(getter, noCall), 'REQUEST_INVALID');
  for (const input of [null, [], {}, { run_id: null }, { run_id: 'bad' }, { run_id: RUN, actor: OTHER },
    { run_id: RUN, clock: TIME }, { run_id: RUN, command_id: COMMAND }, { run_id: RUN, expected_deadline: TIME }]) {
    failure(await receiveBpayNextPreparationExpiry(input, noCall), 'REQUEST_INVALID');
  }
  for (const timeoutMs of [0, 8001, -1, 1.5, '10', null]) {
    failure(await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(noCall.preparationExpiryRpc, { timeoutMs })), 'REQUEST_INVALID');
  }
  failure(await receiveBpayNextPreparationExpiry({ run_id: RUN }, {}), 'DEPENDENCY_UNAVAILABLE', false, true, 'RETRY_SAME_RUN');
  failure(await readBpayNextPreparationExpiryDue(dueInput(), { ...noCall, actorUserId: OTHER }), 'DEPENDENCY_UNAVAILABLE', false, true, 'RETRY_CALL');
  failure(await receiveBpayNextPreparationExpiry({ run_id: RUN }, noCall, { clock: TIME }), 'REQUEST_INVALID');
  failure(await readBpayNextPreparationExpiryDue(dueInput(), noCall, { actor: OTHER }), 'REQUEST_INVALID');
  assert.equal(calls, 0);
});

test('due tuple uses time first and exact UUID ordering second, including prior cursor', async () => {
  const equalTime = page([item(RUN), item(OTHER)]);
  assert.equal((await readBpayNextPreparationExpiryDue(dueInput(), rawResult(equalTime))).ok, true);
  const lowerUuidLaterTime = page([item(OTHER, TIME), item(RUN, '2026-10-01T12:13:14.000002Z')]);
  assert.equal((await readBpayNextPreparationExpiryDue(dueInput(), rawResult(lowerUuidLaterTime))).ok, true);
  const prior = dueInput({ after_deadline: TIME, after_run_id: RUN });
  assert.equal((await readBpayNextPreparationExpiryDue(prior, rawResult(page([item(OTHER)])))).ok, true);
  assert.equal((await readBpayNextPreparationExpiryDue(prior, rawResult(page([item(RUN, '2026-10-01T12:13:14.000002Z')])))).ok, true);
  for (const payload of [page([item(OTHER), item(RUN)]), page([item(RUN), item(RUN.toUpperCase())]),
    page([item(OTHER, '2026-10-01T12:13:14.000002Z'), item(RUN, TIME)])]) {
    failure(await readBpayNextPreparationExpiryDue(dueInput(), rawResult(payload)), 'RESPONSE_INVALID', false, true, 'RETRY_CALL');
  }
  for (const payload of [page([item(RUN)]), page([item(OTHER, '2026-10-01T12:13:14.000000Z')])]) {
    failure(await readBpayNextPreparationExpiryDue(prior, rawResult(payload)), 'RESPONSE_INVALID', false, true, 'RETRY_CALL');
  }
});

test('due exact page and item shapes cannot carry financial data or inaccurate cursors', async () => {
  for (const payload of [null, [], [page()], { ...page(), money: '220.00' },
    { items: [], cursor_deadline: TIME, cursor_run_id: RUN },
    { ...page(), cursor_run_id: null }, { ...page(), cursor_deadline: null },
    { ...page(), cursor_run_id: OTHER }, { ...page(), cursor_deadline: '2026-10-01T12:13:14.000002Z' },
    page([{ run_id: RUN }]), page([{ ...item(), amount: '220.00' }]),
    page([item('not-uuid')]), page([item(RUN, '2026-02-29T00:00:00.000001Z')])]) {
    failure(await readBpayNextPreparationExpiryDue(dueInput(), rawResult(payload)), 'RESPONSE_INVALID', false, true, 'RETRY_CALL');
  }
  assert.deepEqual(await readBpayNextPreparationExpiryDue(dueInput(), rawResult(page([]))), { ok: true, action: 'DUE', result: page([]) });
});

test('due has a hard 100 row bound and honors the smaller requested page size', async () => {
  const rows = Array.from({ length: 101 }, (_, index) => item(`10000000-0000-0000-0000-${String(index + 1).padStart(12, '0')}`));
  assert.equal((await readBpayNextPreparationExpiryDue(dueInput(), rawResult(page(rows.slice(0, 100))))).ok, true);
  failure(await readBpayNextPreparationExpiryDue(dueInput(), rawResult(page(rows))), 'RESPONSE_INVALID', false, true, 'RETRY_CALL');
  failure(await readBpayNextPreparationExpiryDue(dueInput({ limit: 1 }), rawResult(page(rows.slice(0, 2)))), 'RESPONSE_INVALID', false, true, 'RETRY_CALL');
});

test('RECEIVE retains lossless bigint text, real SQL receipt UUID and exact run identity', async () => {
  let calls = 0;
  const payload = receipt({ sequence: '9223372036854775807', expected_candidates: '9007199254740993', completed_candidates: '9007199254740992' });
  const result = await receiveBpayNextPreparationExpiry({ run_id: RUN.toUpperCase() }, deps(async (action, args, options) => {
    calls++; assert.equal(action, 'RECEIVE'); assert.deepEqual(args, { p_run_id: RUN.toUpperCase() });
    assert.equal(Object.isFrozen(args), true); assert.deepEqual(Object.keys(options), ['signal']);
    return json(payload);
  }));
  assert.deepEqual(result, { ok: true, action: 'RECEIVE', result: payload });
  assert.equal(Object.isFrozen(result.result), true); assert.equal(calls, 1);
  for (const payload of [receipt({ phase: 'EXPIRED', expected_candidates: '0', completed_candidates: '0' }),
    receipt({ phase: 'EXPIRED', expected_candidates: '2', completed_candidates: '2', replay: true })]) {
    assert.equal((await receiveBpayNextPreparationExpiry({ run_id: RUN }, rawResult(payload))).ok, true);
  }
});

test('parsed adapters compose with the real fixed raw transport, without service discovery or follow-up calls', async () => {
  const calls = [];
  const raw = createBpayNextPreparationExpiryRpc({ baseUrl: 'http://localhost:54321', headers: serviceHeaders(),
    fetchImpl: async (url, options) => {
      calls.push({ url, args: JSON.parse(options.body) });
      return json(url.endsWith('_due_v1') ? page([item(RUN, TIME)]) : receipt({ replay: true }));
    } });
  const discovery = await readBpayNextPreparationExpiryDue(dueInput({ limit: 1 }), deps(raw));
  assert.equal(discovery.ok, true); assert.equal(calls.length, 1);
  const intake = await receiveBpayNextPreparationExpiry({ run_id: discovery.result.items[0].run_id }, deps(raw));
  assert.equal(intake.ok, true); assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].args, { p_after_deadline: null, p_after_run_id: null, p_limit: 1 });
  assert.deepEqual(calls[1].args, { p_run_id: RUN });
  assert.equal(intake.result.replay, true);
});

test('RECEIVE refuses wrong identity, phase, counters, missing/extra fields as unknown outcome', async () => {
  const noSequence = receipt(); delete noSequence.sequence;
  for (const payload of [null, [], [receipt()], noSequence, receipt({ actor_user_id: OTHER }),
    receipt({ command_id: 'bad' }), receipt({ run_id: OTHER }), receipt({ phase: 'DONE' }),
    receipt({ phase: 'EXPIRED' }), receipt({ completed_candidates: '3' }),
    receipt({ expected_candidates: '0', completed_candidates: '0' }), receipt({ completed_candidates: '2' }),
    receipt({ sequence: 9007199254740993 }), receipt({ sequence: '0' }), receipt({ sequence: '01' }),
    receipt({ sequence: '9223372036854775808' }), receipt({ expected_candidates: '-1' }),
    receipt({ expected_candidates: null }), receipt({ completed_candidates: 1 }),
    receipt({ completed_candidates: '1.0' }), receipt({ replay: 'false' })]) {
    let calls = 0;
    const result = await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async () => { calls++; return json(payload); }));
    failure(result, 'RESPONSE_INVALID', true, true, 'RETRY_SAME_RUN'); assert.equal(calls, 1);
  }
});

test('streamed replies are decoded incrementally and returned only after EOF', async () => {
  const text = JSON.stringify(page());
  const { response } = streamResponse([text.slice(0, 7), text.slice(7, 20), text.slice(20)]);
  assert.equal((await readBpayNextPreparationExpiryDue(dueInput(), deps(async () => response))).ok, true);
  assert.equal(response.body.locked, false);
});

test('32 KiB counts actual bytes, not characters, and cancels an oversized unread tail', async () => {
  let cancelled = 0;
  const { response } = streamResponse(['é'.repeat(17000)], { closed: false, cancel() { cancelled++; } });
  const result = await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async () => response));
  failure(result, 'RESPONSE_TOO_LARGE', true, true, 'RETRY_SAME_RUN');
  assert.equal(cancelled, 1); assert.equal(response.body.locked, false);
});

test('declared length over cap or malformed is refused without reading the response', async () => {
  for (const length of ['32769', '-1', '1.5', 'not-a-length']) {
    let cancelled = 0;
    const { response } = streamResponse([], { closed: false, cancel() { cancelled++; }, headers: { 'content-length': length } });
    failure(await readBpayNextPreparationExpiryDue(dueInput(), deps(async () => response)), 'RESPONSE_TOO_LARGE', false, true, 'RETRY_CALL');
    assert.equal(cancelled, 1);
  }
  const result = await readBpayNextPreparationExpiryDue(dueInput(), deps(async () => textResponse(JSON.stringify(page()), { 'content-length': '1' })));
  assert.equal(result.ok, true); // Never trust a small declared length over real byte counting.
});

test('exact byte cap is accepted; cap+1 is refused even when advertised smaller', async () => {
  const payload = JSON.stringify(page([]));
  const padded = `${payload}${' '.repeat(BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.responseBytes - Buffer.byteLength(payload))}`;
  assert.equal((await readBpayNextPreparationExpiryDue(dueInput(), deps(async () => textResponse(padded)))).ok, true);
  failure(await readBpayNextPreparationExpiryDue(dueInput(), deps(async () => textResponse(`${padded} `, { 'content-length': '1' }))),
    'RESPONSE_TOO_LARGE', false, true, 'RETRY_CALL');
});

test('duplicate and escaped duplicate keys cannot masquerade as exact replies', async () => {
  for (const text of [JSON.stringify(receipt()).replace('"sequence":', '"sequence":"2","sequence":'),
    JSON.stringify(receipt()).replace('"run_id":', '"run_id":"' + OTHER + '","run\\u005fid":'),
    JSON.stringify(page()).replace('"run_id":', '"run_id":"' + OTHER + '","run_id":')]) {
    const isDue = text.includes('"items"');
    const result = isDue
      ? await readBpayNextPreparationExpiryDue(dueInput(), deps(async () => textResponse(text)))
      : await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async () => textResponse(text)));
    failure(result, 'RESPONSE_INVALID', !isDue, true, isDue ? 'RETRY_CALL' : 'RETRY_SAME_RUN');
  }
  const tooDeep = `${'['.repeat(9)}0${']'.repeat(9)}`;
  failure(await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async () => textResponse(tooDeep))), 'RESPONSE_INVALID', true, true, 'RETRY_SAME_RUN');
});

test('malformed JSON, non-JSON, bodyless responses and invalid UTF-8 never trigger a second action', async () => {
  const responses = [textResponse('{'), textResponse('null'), textResponse('not-json', { 'content-type': 'text/plain' }),
    new Response(null, { status: 204 }), new Response(new Uint8Array([0xc3, 0x28]), { headers: { 'content-type': 'application/json' } })];
  for (const response of responses) {
    let calls = 0;
    const result = await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async () => { calls++; return response; }));
    assert.equal(result.ok, false); assert.equal(result.outcome_unknown, true); assert.equal(result.recovery, 'RETRY_SAME_RUN');
    assert.equal(calls, 1);
  }
});

test('lost RECEIVE transport is unknown and never automatically retried; DUE is read-only', async () => {
  for (const method of [receiveBpayNextPreparationExpiry, readBpayNextPreparationExpiryDue]) {
    let calls = 0;
    const result = await method(method === receiveBpayNextPreparationExpiry ? { run_id: RUN } : dueInput(),
      deps(async () => { calls++; throw new Error('JWT token secret provider body must not escape'); }));
    failure(result, 'TRANSPORT_FAILED', method === receiveBpayNextPreparationExpiry, true,
      method === receiveBpayNextPreparationExpiry ? 'RETRY_SAME_RUN' : 'RETRY_CALL');
    assert.equal(calls, 1); assert.equal(JSON.stringify(result).includes('secret'), false);
  }
});

test('known SQL refusals are token-free definite failures, not unknown financial receipts', async () => {
  const vectors = [
    ['42501', 'BPAY_NEXT_EXPIRY_SERVICE_FORBIDDEN', 403, 'FORBIDDEN', false],
    ['22023', 'BPAY_NEXT_EXPIRY_SERVICE_INPUT_INVALID', 400, 'REQUEST_INVALID', false],
    ['23514', 'BPAY_NEXT_EXPIRY_REPLAY_CONFLICT', 400, 'CONFLICT', false],
    ['P0002', 'no rows', 400, 'SCOPE_NOT_FOUND', false],
    ['55000', 'BPAY_NEXT_EXPIRY_NOT_DUE', 400, 'NOT_DUE', false],
    ['55000', 'BPAY_NEXT_MODULE_NOT_ACTIVE', 400, 'MODULE_NOT_ACTIVE', false],
    ['55000', 'BPAY_NEXT_EXPIRY_NOT_UNCONFIRMED', 400, 'NOT_ELIGIBLE', false],
    ['55000', 'BPAY_NEXT_EXPIRY_DEADLINE_UNBOUND', 400, 'STATE_INVALID', false],
    ['55000', 'future unexpected state failure', 400, 'STATE_INVALID', false],
    ['40001', 'secret serialization details', 500, 'TRANSACTION_RETRY', true],
    ['40P01', 'secret deadlock details', 500, 'TRANSACTION_RETRY', true],
    ['55P03', 'lock unavailable', 500, 'TRANSACTION_RETRY', true],
    ['57014', 'statement timeout', 500, 'TRANSACTION_RETRY', true],
    ['PGRST202', 'schema dependency absent', 404, 'DEPENDENCY_UNAVAILABLE', true]
  ];
  for (const [code, message, status, expected, retryable] of vectors) {
    let calls = 0;
    const result = await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async () => {
      calls++; return json({ code, message, details: 'secret SQL details', hint: null }, status);
    }));
    failure(result, expected, false, retryable, retryable ? 'RETRY_SAME_RUN' : 'NONE');
    assert.equal(calls, 1); assert.equal(JSON.stringify(result).includes('secret'), false);
  }
});

test('unknown or malformed HTTP failure cannot claim RECEIVE did not commit', async () => {
  for (const payload of [{ error: 'secret' }, { code: 'toString', message: 'secret', details: null, hint: null },
    { code: 'XX000', message: 'secret', details: null, hint: null },
    { code: '23514', message: 'secret', details: null, hint: null, untrusted: true }]) {
    failure(await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async () => json(payload, 500))),
      'TRANSPORT_FAILED', true, true, 'RETRY_SAME_RUN');
  }
});

test('deadline covers an incomplete response body, aborts and cancels without hanging on cleanup', async () => {
  let cancelled = 0; let calls = 0; let signal;
  const { response } = streamResponse([JSON.stringify(receipt())], { closed: false,
    cancel() { cancelled++; return new Promise(() => {}); } });
  const result = await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async (_action, _args, options) => {
    calls++; signal = options.signal; return response;
  }, { timeoutMs: 15 }));
  failure(result, 'TIMED_OUT', true, true, 'RETRY_SAME_RUN');
  assert.equal(signal.aborted, true); assert.equal(cancelled, 1); assert.equal(calls, 1);
  assert.equal(response.body.locked, false);
});

test('late raw response after ignored abort is cancelled, never parsed or adopted', async () => {
  let resolveRaw; let cancelled = 0; let calls = 0; let signal;
  const { response } = streamResponse([JSON.stringify(receipt())], { closed: false, cancel() { cancelled++; } });
  const result = await receiveBpayNextPreparationExpiry({ run_id: RUN }, deps(async (_action, _args, options) => {
    calls++; signal = options.signal; return new Promise((resolve) => { resolveRaw = resolve; });
  }, { timeoutMs: 15 }));
  failure(result, 'TIMED_OUT', true, true, 'RETRY_SAME_RUN'); assert.equal(signal.aborted, true);
  resolveRaw(response);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(cancelled, 1); assert.equal(calls, 1); assert.equal(response.body.locked, false);
});

test('fresh calls never reuse receipt, cursor, headers or an uncertain result from a previous call', async () => {
  let calls = 0;
  const dependency = deps(async () => {
    calls++;
    if (calls === 1) throw new Error('lost reply');
    return json(receipt({ replay: true }));
  });
  failure(await receiveBpayNextPreparationExpiry({ run_id: RUN }, dependency), 'TRANSPORT_FAILED', true, true, 'RETRY_SAME_RUN');
  assert.deepEqual(await receiveBpayNextPreparationExpiry({ run_id: RUN }, dependency),
    { ok: true, action: 'RECEIVE', result: receipt({ replay: true }) });
  assert.equal(calls, 2); // Explicit caller retry only.
});

test('adapter is an isolated fixed service seam, not an actor/clock/job/browser API', async () => {
  const source = await readFile(new URL('../broker/src/bpay-next-preparation-expiry-rpc.js', import.meta.url), 'utf8');
  const rpcNames = [...new Set(source.match(/bpay_next_[a-z_]+_v1/g))].sort();
  assert.deepEqual(rpcNames, ['bpay_next_preparation_expiry_due_v1', 'bpay_next_preparation_expiry_receive_v1']);
  assert.doesNotMatch(source, /p_actor_user_id|p_clock|p_command_id|Date\.(?:parse|now)|new Date\(/);
  assert.doesNotMatch(source, /env\.[A-Z_]+|\.ack\(|\.retry\(|\.send\(|setInterval\(/);
  assert.equal(BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.responseBytes, 32768);
  assert.equal(BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.pageRows, 100);
});
