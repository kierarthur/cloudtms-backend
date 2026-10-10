import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import {
  BPAY_NEXT_RUNTIME_LIMITS, BPAY_NEXT_RUNTIME_JOB_KINDS,
  createBpayNextRuntimeRpc, enrollBpayNextOne, readBpayNextJobWakePage, readBpayNextExpiryDispatchPage, cleanupBpayNextTerminalRows
} from '../broker/src/bpay-next-runtime-rpc.js';

// Unsigned synthetic JWT shape only. Every fetch is injected; no network,
// database, Worker binding, queue message or financial owner is executed.
const jwt = (claims = { role: 'service_role' }) =>
  `e30.${Buffer.from(JSON.stringify(claims)).toString('base64url')}.c3ludGhldGlj`;
const headers = () => ({ Authorization: `Bearer ${jwt()}` });
const JOB = 'a1000000-0000-0000-0000-000000000001';
const OTHER = 'a1000000-0000-0000-0000-000000000002';
const enrollment = (changes = {}) => ({ progressed: true, last_enrolled_sequence: '9007199254740993',
  job_id: JOB, job_kind: 'POSITION_APPLY', job_status: 'READY', ...changes });
const empty = (progressed = false, sequence = '0') => enrollment({ progressed,
  last_enrolled_sequence: sequence, job_id: null, job_kind: null, job_status: null });
const item = (changes = {}) => ({ job_id: JOB, job_kind: 'POSITION_APPLY', command_sequence: '9007199254740993', ...changes });
const page = (items = [item()], lane = 'READY') => ({ lane, items });
const input = (changes = {}) => ({ lane: 'READY', limit: 100, ...changes });
const TIME = '2026-10-01T12:13:14.000001Z';
const dueItem = (run_id = JOB, expected_deadline = TIME) => ({ run_id, expected_deadline });
const expiryPage = (items = [dueItem()]) => ({ items,
  cursor_deadline: items.at(-1)?.expected_deadline ?? null, cursor_run_id: items.at(-1)?.run_id ?? null });
const json = (value, status = 200, extra = {}) => new Response(JSON.stringify(value), {
  status, headers: { 'content-type': 'application/json', ...extra }
});
const text = (value, extra = {}, status = 200) => new Response(value, {
  status, headers: { 'content-type': 'application/json', ...extra }
});
const deps = (runtimeRpc, extra = {}) => ({ runtimeRpc, ...extra });
function stream(chunks, { close = true, cancel = () => {}, extra = {} } = {}) {
  return new Response(new ReadableStream({ start(controller) {
    for (const chunk of chunks) controller.enqueue(typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk);
    if (close) controller.close();
  }, cancel }), { headers: { 'content-type': 'application/json', ...extra } });
}
function failure(value, suffix, unknown = false, retryable = false, recovery = 'NONE') {
  assert.deepEqual(value, { ok: false, error_code: `BPAY_NEXT_RUNTIME_${suffix}`,
    retryable, outcome_unknown: unknown, recovery });
  assert.equal(Object.isFrozen(value), true);
}

test('raw metadata transport fixes enrollment/wake/expiry/cleanup endpoints and exact service args', async () => {
  const calls = [];
  const original = new Headers(headers()); original.set('x-task', 'original');
  const raw = createBpayNextRuntimeRpc({ baseUrl: 'http://127.0.0.1:54321/', headers: original,
    fetchImpl: async (url, options) => { calls.push({ url, options }); return json(empty()); } });
  original.set('authorization', 'Bearer changed'); original.set('x-task', 'changed');
  const signal = new AbortController().signal;
  assert.ok(await raw('ENROLL', {}, { signal }) instanceof Response);
  calls[0].options.headers.set('x-task', 'changed-request');
  await raw('WAKE_PAGE', { p_lane: 'LEASE_EXPIRED', p_limit: 1 }, { signal });
  await raw('EXPIRY_PAGE', { p_limit: 1 }, { signal });
  await raw('CLEANUP', { p_limit: 1 }, { signal });
  assert.equal(calls.length, 4);
  assert.equal(calls[0].url, 'http://127.0.0.1:54321/rest/v1/rpc/bpay_next_enroll_service_v1');
  assert.equal(calls[1].url, 'http://127.0.0.1:54321/rest/v1/rpc/bpay_next_job_wake_page_v1');
  assert.equal(calls[2].url, 'http://127.0.0.1:54321/rest/v1/rpc/bpay_next_expiry_dispatch_page_v1');
  assert.equal(calls[3].url, 'http://127.0.0.1:54321/rest/v1/rpc/bpay_next_terminal_cleanup_service_v1');
  assert.deepEqual(JSON.parse(calls[0].options.body), {});
  assert.deepEqual(JSON.parse(calls[1].options.body), { p_lane: 'LEASE_EXPIRED', p_limit: 1 });
  assert.deepEqual(JSON.parse(calls[2].options.body), { p_limit: 1 });
  assert.deepEqual(JSON.parse(calls[3].options.body), { p_limit: 1 });
  for (const { options } of calls) {
    assert.equal(options.method, 'POST'); assert.equal(options.redirect, 'error'); assert.equal(options.signal, signal);
    assert.equal(options.headers.get('authorization'), headers().Authorization);
    assert.equal(options.headers.get('accept'), 'application/json'); assert.equal(options.headers.get('content-type'), 'application/json');
    assert.equal(options.headers.get('X-CloudTMS-Banking-Pay-Route-Class'), 'OPERATION_NUDGE');
  }
  assert.equal(calls[1].options.headers.get('x-task'), 'original');
});

test('raw factory requires service JWT shape, HTTPS/local HTTP and safe fixed transport configuration', () => {
  for (const baseUrl of ['https://example.invalid', 'http://localhost:54321', 'http://[::1]:54321']) {
    assert.equal(typeof createBpayNextRuntimeRpc({ baseUrl, headers: headers(), fetchImpl: async () => {} }), 'function');
  }
  for (const baseUrl of ['', 'not-url', 'http://example.invalid', 'ftp://example.invalid',
    'https://user:password@example.invalid', 'https://example.invalid?token=value', 'https://example.invalid#fragment']) {
    assert.throws(() => createBpayNextRuntimeRpc({ baseUrl, headers: headers() }), /BPAY_NEXT_RUNTIME_TRANSPORT_INVALID/);
  }
  for (const authorization of [null, 'Bearer missing', `Bearer ${jwt({ role: 'anon' })}`,
    `Bearer ${jwt({ role: 'authenticated' })}`, `Bearer ${jwt({})}`, 'Bearer e30.e30.c2ln']) {
    assert.throws(() => createBpayNextRuntimeRpc({ baseUrl: 'https://example.invalid',
      headers: authorization === null ? {} : { Authorization: authorization } }), /BPAY_NEXT_RUNTIME_TRANSPORT_INVALID/);
  }
  for (const fetchImpl of [null, false, 'fetch']) {
    assert.throws(() => createBpayNextRuntimeRpc({ baseUrl: 'https://example.invalid', headers: headers(), fetchImpl }), /BPAY_NEXT_RUNTIME_TRANSPORT_INVALID/);
  }
});

test('raw caller cannot select private RPC/clock, financial data, actor, lease token or arbitrary lane', async () => {
  let network = 0;
  const raw = createBpayNextRuntimeRpc({ baseUrl: 'https://example.invalid', headers: headers(),
    fetchImpl: async () => { network++; return json(empty()); } });
  const signal = new AbortController().signal;
  for (const [action, args, options] of [
    ['CLAIM', {}, { signal }], ['STEP', {}, { signal }], ['private.bpay_next_enroll_one_v1', {}, { signal }],
    [{ toString: () => 'ENROLL' }, {}, { signal }],
    ['ENROLL', { p_candidate_id: JOB }, { signal }], ['ENROLL', { p_clock: '2026-10-01T00:00:00Z' }, { signal }],
    ['ENROLL', { p_actor_user_id: JOB }, { signal }], ['ENROLL', { money: '220.00' }, { signal }],
    ['ENROLL', {}, { signal, actorUserId: JOB }], ['ENROLL', {}, { signal: {} }], ['ENROLL', {}, undefined],
    ['WAKE_PAGE', { p_lane: 'ALL', p_limit: 100 }, { signal }],
    ['WAKE_PAGE', { p_lane: 'READY', p_limit: 101 }, { signal }],
    ['WAKE_PAGE', { p_lane: 'READY', p_limit: 1, lease_nonce: JOB }, { signal }],
    ['WAKE_PAGE', { p_lane: 'LEASE_EXPIRED', p_limit: 1, cursor: JOB }, { signal }],
    ['EXPIRY_PAGE', { p_limit: 0 }, { signal }],
    ['EXPIRY_PAGE', { p_limit: 1, after_deadline: TIME, after_run_id: JOB }, { signal }],
    ['EXPIRY_PAGE', { p_limit: 1, sweep_through_utc: TIME }, { signal }]
  ]) await assert.rejects(raw(action, args, options), /BPAY_NEXT_RUNTIME_TRANSPORT_INVALID/);
  await assert.rejects(raw('ENROLL', {}, { signal }, { actor: JOB }), /BPAY_NEXT_RUNTIME_TRANSPORT_INVALID/);
  assert.equal(network, 0);
});

test('ENROLL performs exactly one raw metadata call; partial enrollment may keep prior sequence zero', async () => {
  let calls = 0;
  const payload = enrollment({ last_enrolled_sequence: '0' });
  const result = await enrollBpayNextOne(deps(async (action, args, options) => {
    calls++; assert.equal(action, 'ENROLL'); assert.deepEqual(args, {});
    assert.equal(Object.isFrozen(args), true); assert.deepEqual(Object.keys(options), ['signal']);
    assert.ok(options.signal instanceof AbortSignal); return json(payload);
  }));
  assert.deepEqual(result, { ok: true, action: 'ENROLL', result: payload });
  assert.equal(Object.isFrozen(result.result), true); assert.equal(calls, 1);
});

test('empty expiry lane advancement remains true even with no job or worker', async () => {
  for (const payload of [empty(false, '0'), empty(false, '9007199254740993'),
    empty(true, '9007199254740993'), empty(true, '9223372036854775807')]) {
    assert.deepEqual(await enrollBpayNextOne(deps(async () => json(payload))), { ok: true, action: 'ENROLL', result: payload });
  }
});

test('all thirteen actual job kinds are typed explicitly, including CASE_WRITE_OFF and retired PREPARE metadata', async () => {
  assert.equal(BPAY_NEXT_RUNTIME_JOB_KINDS.length, 13); assert.equal(Object.isFrozen(BPAY_NEXT_RUNTIME_JOB_KINDS), true);
  for (const job_kind of BPAY_NEXT_RUNTIME_JOB_KINDS) {
    const payload = enrollment({ job_kind });
    assert.equal((await enrollBpayNextOne(deps(async () => json(payload)))).ok, true);
    assert.equal((await readBpayNextJobWakePage(input(), deps(async () => json(page([item({ job_kind })]))))).ok, true);
  }
  const completed = enrollment({ job_kind: 'PREPARE', job_status: 'DONE' });
  assert.deepEqual(await enrollBpayNextOne(deps(async () => json(completed))), { ok: true, action: 'ENROLL', result: completed });
});

test('closed enrollment receipt requires exact complete null/job tuple and lossless nonnegative sequence', async () => {
  const missing = enrollment(); delete missing.job_kind;
  for (const payload of [null, [], [enrollment()], missing, enrollment({ extra: true }),
    enrollment({ progressed: false }), enrollment({ progressed: 'true' }),
    enrollment({ job_id: null }), empty(false, '-1'), empty(true, '0'), enrollment({ job_kind: 'UNKNOWN' }),
    enrollment({ job_id: 'bad' }), enrollment({ job_status: 'LEASED' }), enrollment({ job_status: 'FAILED' }),
    enrollment({ job_status: null }), enrollment({ job_kind: null }),
    enrollment({ last_enrolled_sequence: 9007199254740993 }), enrollment({ last_enrolled_sequence: '01' }),
    enrollment({ last_enrolled_sequence: '1.0' }), enrollment({ last_enrolled_sequence: '9223372036854775808' }),
    enrollment({ lease_nonce: JOB })]) {
    let calls = 0;
    failure(await enrollBpayNextOne(deps(async () => { calls++; return json(payload); })),
      'RESPONSE_INVALID', true, true, 'READ_JOB_RECOVERY');
    assert.equal(calls, 1);
  }
});

test('WAKE_PAGE validates exact lane and one immutable bounded page, never claims a lease', async () => {
  for (const lane of ['READY', 'LEASE_EXPIRED']) {
    let calls = 0;
    const payload = page([item()], lane);
    const result = await readBpayNextJobWakePage(input({ lane, limit: 1 }), deps(async (action, args, options) => {
      calls++; assert.equal(action, 'WAKE_PAGE'); assert.deepEqual(args, { p_lane: lane, p_limit: 1 });
      assert.equal(Object.isFrozen(args), true); assert.deepEqual(Object.keys(options), ['signal']); return json(payload);
    }));
    assert.deepEqual(result, { ok: true, action: 'WAKE_PAGE', result: payload }); assert.equal(calls, 1);
    assert.equal(Object.isFrozen(result.result.items), true); assert.equal(Object.isFrozen(result.result.items[0]), true);
  }
});

test('wake time leads SQL order: decreasing or equal command sequences must remain legitimate', async () => {
  const payload = page([item({ command_sequence: '9223372036854775807' }),
    item({ job_id: OTHER, job_kind: 'CASE_CREATE', command_sequence: '1' }),
    item({ job_id: 'a1000000-0000-0000-0000-000000000003', command_sequence: '1' })]);
  assert.deepEqual(await readBpayNextJobWakePage(input(), deps(async () => json(payload))),
    { ok: true, action: 'WAKE_PAGE', result: payload });
});

test('wake page requires unique job membership, exact three fields and positive bigint strings', async () => {
  for (const payload of [null, [], [page()], { ...page(), cursor: JOB }, page([], 'LEASE_EXPIRED'),
    page([item(), item({ job_id: JOB.toUpperCase(), job_kind: 'CASE_CREATE' })]),
    page([item({ job_id: 'bad' })]), page([item({ job_kind: 'UNKNOWN' })]),
    page([item({ command_sequence: '0' })]), page([item({ command_sequence: '01' })]),
    page([item({ command_sequence: 9007199254740993 })]), page([item({ command_sequence: '9223372036854775808' })]),
    page([item({ lease_nonce: JOB })]), page([item({ owner_epoch: '1' })]), page([item({ amount: '220.00' })])]) {
    failure(await readBpayNextJobWakePage(input(), deps(async () => json(payload))),
      'RESPONSE_INVALID', true, true, 'WAIT_FOR_WAKE_RECOVERY');
  }
  assert.deepEqual(await readBpayNextJobWakePage(input(), deps(async () => json(page([])))),
    { ok: true, action: 'WAKE_PAGE', result: page([]) });
});

test('wake response cannot exceed requested limit or hard 100 rows', async () => {
  const rows = Array.from({ length: 101 }, (_, index) => item({ job_id: `10000000-0000-0000-0000-${String(index + 1).padStart(12, '0')}` }));
  assert.equal((await readBpayNextJobWakePage(input(), deps(async () => json(page(rows.slice(0, 100)))))).ok, true);
  failure(await readBpayNextJobWakePage(input(), deps(async () => json(page(rows)))), 'RESPONSE_INVALID', true, true, 'WAIT_FOR_WAKE_RECOVERY');
  failure(await readBpayNextJobWakePage(input({ limit: 1 }), deps(async () => json(page(rows.slice(0, 2))))),
    'RESPONSE_INVALID', true, true, 'WAIT_FOR_WAKE_RECOVERY');
});

test('EXPIRY_PAGE sends only a limit and preserves all six UTC microsecond digits', async () => {
  let calls = 0;
  const payload = expiryPage([dueItem(JOB, TIME), dueItem(OTHER, '2026-10-01T12:13:14.000002Z')]);
  const result = await readBpayNextExpiryDispatchPage({ limit: 2 }, deps(async (action, args, options) => {
    calls++; assert.equal(action, 'EXPIRY_PAGE'); assert.deepEqual(args, { p_limit: 2 });
    assert.equal(Object.isFrozen(args), true); assert.deepEqual(Object.keys(options), ['signal']); return json(payload);
  }));
  assert.deepEqual(result, { ok: true, action: 'EXPIRY_PAGE', result: payload });
  assert.equal(calls, 1); assert.equal(Object.isFrozen(result.result.items), true);
  assert.equal(Object.isFrozen(result.result.items[0]), true);
  assert.notEqual(result.result.items[0].expected_deadline, result.result.items[1].expected_deadline);
});

test('expiry page is strictly ordered by deadline/UUID and exact cursor tuple, not caller state', async () => {
  assert.equal((await readBpayNextExpiryDispatchPage({ limit: 100 }, deps(async () => json(expiryPage([dueItem(JOB), dueItem(OTHER)]))))).ok, true);
  assert.equal((await readBpayNextExpiryDispatchPage({ limit: 100 }, deps(async () => json(expiryPage([
    dueItem(OTHER), dueItem(JOB, '2026-10-01T12:13:14.000002Z')]))))).ok, true);
  for (const payload of [expiryPage([dueItem(OTHER), dueItem(JOB)]),
    expiryPage([dueItem(JOB), dueItem(JOB.toUpperCase())]),
    expiryPage([dueItem(OTHER, '2026-10-01T12:13:14.000002Z'), dueItem(JOB, TIME)]),
    { ...expiryPage(), cursor_run_id: OTHER }, { ...expiryPage(), cursor_deadline: null },
    { ...expiryPage(), cursor_deadline: '2026-10-01T12:13:14.000002Z' },
    { items: [], cursor_run_id: JOB, cursor_deadline: TIME }]) {
    failure(await readBpayNextExpiryDispatchPage({ limit: 100 }, deps(async () => json(payload))),
      'RESPONSE_INVALID', true, true, 'WAIT_FOR_SWEEP_RECOVERY');
  }
  assert.deepEqual(await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(async () => json(expiryPage([])))),
    { ok: true, action: 'EXPIRY_PAGE', result: expiryPage([]) });
});

test('expiry shape/calendar/bounds reject invalid or financial facts before another action', async () => {
  const rows = Array.from({ length: 101 }, (_, index) => dueItem(`10000000-0000-0000-0000-${String(index + 1).padStart(12, '0')}`));
  assert.equal((await readBpayNextExpiryDispatchPage({ limit: 100 }, deps(async () => json(expiryPage(rows.slice(0, 100)))))).ok, true);
  for (const payload of [expiryPage(rows), { ...expiryPage(), sweep_through_utc: TIME },
    expiryPage([{ ...dueItem(), amount: '220.00' }]), expiryPage([dueItem('bad')]),
    ...['2026-02-29T00:00:00Z', '1900-02-29T00:00:00Z', '2026-04-31T00:00:00Z',
      '0000-01-01T00:00:00Z', '2026-10-01T24:00:00Z', '2026-10-01T12:13:14.1234567Z',
      '2026-10-01T12:13:14+00:00', 'infinity'].map((time) => expiryPage([dueItem(JOB, time)]))]) {
    let calls = 0;
    failure(await readBpayNextExpiryDispatchPage({ limit: 100 }, deps(async () => { calls++; return json(payload); })),
      'RESPONSE_INVALID', true, true, 'WAIT_FOR_SWEEP_RECOVERY'); assert.equal(calls, 1);
  }
  failure(await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(async () => json(expiryPage(rows.slice(0, 2))))),
    'RESPONSE_INVALID', true, true, 'WAIT_FOR_SWEEP_RECOVERY');
  assert.equal((await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(async () => json(expiryPage([dueItem(JOB, '2000-02-29T00:00:00.123456Z')]))))).ok, true);
});

test('expiry sweep restart may return older times on a fresh call; no client cursor is retained', async () => {
  let calls = 0;
  const results = [expiryPage([dueItem(JOB, '2026-10-02T00:00:00.000001Z')]), expiryPage([]), expiryPage([dueItem(JOB, TIME)])];
  const dependency = deps(async () => json(results[calls++]));
  for (const payload of results) {
    assert.deepEqual(await readBpayNextExpiryDispatchPage({ limit: 1 }, dependency),
      { ok: true, action: 'EXPIRY_PAGE', result: payload });
  }
  assert.equal(calls, 3); // Synthetic owner replies, not a native watermark proof.
});

test('closed inputs/dependencies deny financial/actor/clock/lease data before any RPC', async () => {
  let calls = 0;
  const noCall = deps(async () => { calls++; return json(page()); });
  for (const value of [null, [], {}, input({ lane: 'ALL' }), input({ lane: null }), input({ limit: 0 }),
    input({ limit: 101 }), input({ limit: 1.5 }), input({ limit: '1' }), input({ limit: null }),
    input({ actor_user_id: JOB }), input({ clock: '2026-10-01' }), input({ lease_nonce: JOB }),
    input({ amount: '220.00' }), input({ cursor: JOB }), Object.create(input()),
    Object.assign(input(), { [Symbol('extra')]: true })]) {
    failure(await readBpayNextJobWakePage(value, noCall), 'REQUEST_INVALID');
  }
  const accessor = input(); Object.defineProperty(accessor, 'limit', { enumerable: true, get() { throw new Error('must not run'); } });
  failure(await readBpayNextJobWakePage(accessor, noCall), 'REQUEST_INVALID');
  for (const timeoutMs of [0, 8001, null, 1.5, '10']) {
    failure(await enrollBpayNextOne({ ...noCall, timeoutMs }), 'REQUEST_INVALID');
  }
  failure(await enrollBpayNextOne(noCall, { actor: JOB }), 'REQUEST_INVALID');
  failure(await readBpayNextJobWakePage(input(), noCall, { clock: '2026-10-01' }), 'REQUEST_INVALID');
  failure(await enrollBpayNextOne({ ...noCall, actor: JOB }), 'DEPENDENCY_UNAVAILABLE', false, true, 'RETRY_CALL');
  failure(await enrollBpayNextOne({}), 'DEPENDENCY_UNAVAILABLE', false, true, 'RETRY_CALL');
  for (const value of [null, {}, { limit: 0 }, { limit: 101 }, { limit: '1' }, { limit: 1.5 },
    { limit: 1, after_deadline: TIME, after_run_id: JOB }, { limit: 1, sweep_through_utc: TIME },
    { limit: 1, actor: JOB }, { limit: 1, clock: TIME }]) {
    failure(await readBpayNextExpiryDispatchPage(value, noCall), 'REQUEST_INVALID');
  }
  failure(await readBpayNextExpiryDispatchPage({ limit: 1 }, noCall, { clock: TIME }), 'REQUEST_INVALID');
  assert.equal(calls, 0);
});

test('parsed callers compose with the fixed raw transport but do not send or execute returned jobs', async () => {
  const calls = [];
  const raw = createBpayNextRuntimeRpc({ baseUrl: 'http://localhost:54321', headers: headers(),
    fetchImpl: async (url, options) => {
      calls.push({ url, args: JSON.parse(options.body) });
      return json(url.endsWith('_enroll_service_v1') ? enrollment({ job_kind: 'CASE_CREATE' })
        : url.endsWith('_job_wake_page_v1') ? page([item({ job_kind: 'CASE_CREATE' })]) : expiryPage());
    } });
  assert.equal((await enrollBpayNextOne(deps(raw))).ok, true); assert.equal(calls.length, 1);
  assert.equal((await readBpayNextJobWakePage(input({ limit: 1 }), deps(raw))).ok, true); assert.equal(calls.length, 2);
  assert.equal((await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(raw))).ok, true); assert.equal(calls.length, 3);
  assert.deepEqual(calls[0].args, {}); assert.deepEqual(calls[1].args, { p_lane: 'READY', p_limit: 1 });
  assert.deepEqual(calls[2].args, { p_limit: 1 });
});

test('response reader waits for EOF, decodes chunks and releases its stream lock', async () => {
  const payload = JSON.stringify(enrollment());
  const response = stream([payload.slice(0, 10), payload.slice(10, 50), payload.slice(50)]);
  assert.equal((await enrollBpayNextOne(deps(async () => response))).ok, true);
  assert.equal(response.body.locked, false);
});

test('byte cap applies to real streamed bytes and oversized cleanup cannot prolong the call', async () => {
  let cancelled = 0;
  const response = stream(['é'.repeat(17000)], { close: false, cancel() { cancelled++; return new Promise(() => {}); } });
  failure(await readBpayNextJobWakePage(input(), deps(async () => response)), 'RESPONSE_TOO_LARGE', true, true, 'WAIT_FOR_WAKE_RECOVERY');
  assert.equal(cancelled, 1); assert.equal(response.body.locked, false);
});

test('declared oversize cancels unread response; exact 32KiB fits and dishonest smaller header cannot evade bound', async () => {
  for (const length of ['32769', '-1', '1.5', 'bad']) {
    let cancelled = 0;
    const response = stream([], { close: false, cancel() { cancelled++; }, extra: { 'content-length': length } });
    failure(await enrollBpayNextOne(deps(async () => response)), 'RESPONSE_TOO_LARGE', true, true, 'READ_JOB_RECOVERY');
    assert.equal(cancelled, 1);
  }
  const payload = JSON.stringify(empty());
  const padded = payload + ' '.repeat(BPAY_NEXT_RUNTIME_LIMITS.responseBytes - Buffer.byteLength(payload));
  assert.equal((await enrollBpayNextOne(deps(async () => text(padded)))).ok, true);
  failure(await enrollBpayNextOne(deps(async () => text(`${padded} `, { 'content-length': '1' }))),
    'RESPONSE_TOO_LARGE', true, true, 'READ_JOB_RECOVERY');
});

test('duplicate JSON keys, escaped duplicate identity and excessive nesting refuse the entire page', async () => {
  for (const payload of [JSON.stringify(enrollment()).replace('"progressed":', '"progressed":false,"progressed":'),
    JSON.stringify(enrollment()).replace('"job_id":', `"job_id":"${OTHER}","job\\u005fid":`),
    `${'['.repeat(9)}0${']'.repeat(9)}`]) {
    failure(await enrollBpayNextOne(deps(async () => text(payload))), 'RESPONSE_INVALID', true, true, 'READ_JOB_RECOVERY');
  }
  const duplicate = JSON.stringify(page()).replace('"job_kind":', '"job_kind":"UNKNOWN","job_kind":');
  failure(await readBpayNextJobWakePage(input(), deps(async () => text(duplicate))), 'RESPONSE_INVALID', true, true, 'WAIT_FOR_WAKE_RECOVERY');
});

test('malformed/non-raw/invalid UTF-8 responses cannot publish or claim another job', async () => {
  for (const response of [enrollment(), text('{'), text('not-json', { 'content-type': 'text/plain' }),
    new Response(null, { status: 204 }), new Response(new Uint8Array([0xc3, 0x28]), { headers: { 'content-type': 'application/json' } })]) {
    let calls = 0;
    const result = await enrollBpayNextOne(deps(async () => { calls++; return response; }));
    assert.equal(result.ok, false); assert.equal(result.outcome_unknown, true); assert.equal(result.recovery, 'READ_JOB_RECOVERY');
    assert.equal(calls, 1);
  }
});

test('lost metadata replies are uncertain, with distinct recovery labels and no silent retries', async () => {
  for (const action of ['ENROLL', 'WAKE_PAGE', 'EXPIRY_PAGE']) {
    let calls = 0;
    const dependency = deps(async () => { calls++; throw new Error('JWT token secret SQL body must not escape'); });
    const result = action === 'ENROLL' ? await enrollBpayNextOne(dependency) : action === 'WAKE_PAGE'
      ? await readBpayNextJobWakePage(input(), dependency) : await readBpayNextExpiryDispatchPage({ limit: 1 }, dependency);
    failure(result, 'TRANSPORT_FAILED', true, true, action === 'ENROLL' ? 'READ_JOB_RECOVERY'
      : action === 'WAKE_PAGE' ? 'WAIT_FOR_WAKE_RECOVERY' : 'WAIT_FOR_SWEEP_RECOVERY');
    assert.equal(calls, 1); assert.equal(JSON.stringify(result).includes('secret'), false);
  }
});

test('known SQL aborts remain definite, closed and token-free', async () => {
  for (const [code, message, expected, retryable] of [
    ['42501', 'BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN', 'FORBIDDEN', false],
    ['22023', 'BPAY_NEXT_RUNTIME_SERVICE_INPUT_INVALID', 'REQUEST_INVALID', false],
    ['23514', 'BPAY_NEXT_ENROLLMENT_COMMAND_INVALID', 'CONFLICT', false],
    ['P0002', 'no data', 'SCOPE_NOT_FOUND', false], ['55000', 'BPAY_NEXT_MODULE_NOT_ACTIVE', 'MODULE_NOT_ACTIVE', false],
    ['55000', 'BPAY_NEXT_EXPIRY_DEADLINE_UNBOUND', 'STATE_INVALID', false],
    ['55000', 'unknown broken metadata state', 'STATE_INVALID', false],
    ['40P01', 'secret deadlock graph', 'TRANSACTION_RETRY', true],
    ['40001', 'secret serialization body', 'TRANSACTION_RETRY', true],
    ['55P03', 'busy', 'TRANSACTION_RETRY', true], ['57014', 'timeout', 'TRANSACTION_RETRY', true],
    ['PGRST202', 'missing dependency', 'DEPENDENCY_UNAVAILABLE', true]
  ]) {
    const error = { code, message, details: 'secret private data', hint: null };
    failure(await enrollBpayNextOne(deps(async () => json(error, 400))), expected, false, retryable, retryable ? 'RETRY_CALL' : 'NONE');
    failure(await readBpayNextJobWakePage(input(), deps(async () => json(error, 400))), expected, false, retryable, retryable ? 'RETRY_CALL' : 'NONE');
    failure(await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(async () => json(error, 400))), expected, false, retryable, retryable ? 'RETRY_CALL' : 'NONE');
  }
});

test('unknown error code/prototype names/extra fields never turn a lost reply into definite rejection', async () => {
  for (const payload of [{ error: 'secret' }, { code: 'toString', message: 'secret', details: null, hint: null },
    { code: 'XX000', message: 'secret', details: null, hint: null },
    { code: '23514', message: 'secret', details: null, hint: null, extra: true }]) {
    failure(await enrollBpayNextOne(deps(async () => json(payload, 500))), 'TRANSPORT_FAILED', true, true, 'READ_JOB_RECOVERY');
  }
});

test('deadline includes a complete JSON body without EOF and does not await hanging cancellation', async () => {
  let signal; let cancelled = 0; let calls = 0;
  const response = stream([JSON.stringify(page())], { close: false, cancel() { cancelled++; return new Promise(() => {}); } });
  failure(await readBpayNextJobWakePage(input(), deps(async (_action, _args, options) => {
    signal = options.signal; calls++; return response;
  }, { timeoutMs: 15 })), 'TIMED_OUT', true, true, 'WAIT_FOR_WAKE_RECOVERY');
  assert.equal(signal.aborted, true); assert.equal(cancelled, 1); assert.equal(calls, 1); assert.equal(response.body.locked, false);
});

test('late ignored-abort raw response is cancelled and cannot become a successful enrollment', async () => {
  let resolveRaw; let signal; let cancelled = 0; let calls = 0;
  const response = stream([JSON.stringify(enrollment())], { close: false, cancel() { cancelled++; } });
  failure(await enrollBpayNextOne(deps(async (_action, _args, options) => {
    signal = options.signal; calls++; return new Promise((resolve) => { resolveRaw = resolve; });
  }, { timeoutMs: 15 })), 'TIMED_OUT', true, true, 'READ_JOB_RECOVERY');
  assert.equal(signal.aborted, true); resolveRaw(response);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(cancelled, 1); assert.equal(calls, 1); assert.equal(response.body.locked, false);
});

test('expiry dispatch incomplete/oversized/malformed body is uncertain and cannot auto-receive a run', async () => {
  let cancelled = 0; let calls = 0;
  const response = stream([JSON.stringify(expiryPage())], { close: false, cancel() { cancelled++; } });
  failure(await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(async () => { calls++; return response; }, { timeoutMs: 15 })),
    'TIMED_OUT', true, true, 'WAIT_FOR_SWEEP_RECOVERY');
  assert.equal(cancelled, 1); assert.equal(calls, 1);
  failure(await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(async () => text(' '.repeat(32769)))),
    'RESPONSE_TOO_LARGE', true, true, 'WAIT_FOR_SWEEP_RECOVERY');
  const duplicate = JSON.stringify(expiryPage()).replace('"run_id":', `"run_id":"${OTHER}","run_id":`);
  failure(await readBpayNextExpiryDispatchPage({ limit: 1 }, deps(async () => text(duplicate))),
    'RESPONSE_INVALID', true, true, 'WAIT_FOR_SWEEP_RECOVERY');
});

test('fresh explicit calls use only fresh owner results, never cached missing/progress/unknown receipts', async () => {
  let calls = 0;
  const dependency = deps(async () => {
    calls++; if (calls === 1) throw new Error('lost receipt');
    return json(calls === 2 ? empty(true, '1') : enrollment({ last_enrolled_sequence: '2' }));
  });
  failure(await enrollBpayNextOne(dependency), 'TRANSPORT_FAILED', true, true, 'READ_JOB_RECOVERY');
  assert.equal((await enrollBpayNextOne(dependency)).result.job_id, null);
  assert.equal((await enrollBpayNextOne(dependency)).result.job_id, JOB); assert.equal(calls, 3);
});

test('cleanup sends one bounded metadata call and accepts only truthful closed counters', async () => {
  const receipt = { lane: 'JOB', rows_visited: '100', rows_deleted: '1', rows_retained: '99', busy: false };
  let calls = 0;
  const dependency = deps(async (action, args) => {
    calls++; assert.equal(action, 'CLEANUP'); assert.deepEqual(args, { p_limit: 100 }); return json(receipt);
  });
  assert.deepEqual(await cleanupBpayNextTerminalRows({ limit: 100 }, dependency),
    { ok: true, action: 'CLEANUP', result: receipt });
  assert.equal(calls, 1);
  const busy = { lane: null, rows_visited: '0', rows_deleted: '0', rows_retained: '0', busy: true };
  assert.equal((await cleanupBpayNextTerminalRows({ limit: 1 }, deps(async () => json(busy)))).ok, true);
  for (const payload of [{ ...receipt, rows_retained: '98' }, { ...receipt, rows_visited: '101' },
    { ...receipt, rows_deleted: 1 }, { ...receipt, lane: 'ALL' }, { ...receipt, amount: '1.00' },
    { ...busy, lane: 'JOB' }, { ...busy, rows_visited: '1', rows_retained: '1' }]) {
    failure(await cleanupBpayNextTerminalRows({ limit: 100 }, deps(async () => json(payload))),
      'RESPONSE_INVALID', true, true, 'WAIT_FOR_SWEEP_RECOVERY');
  }
  let invalidCalls = 0;
  for (const input of [{ limit: 101 }, { limit: 1, cursor: JOB }]) {
    failure(await cleanupBpayNextTerminalRows(input, deps(async () => { invalidCalls++; })), 'REQUEST_INVALID');
  }
  assert.equal(invalidCalls, 0);
  let unknownCalls = 0;
  failure(await cleanupBpayNextTerminalRows({ limit: 1 }, deps(async () => { unknownCalls++; throw Error('lost'); })),
    'TRANSPORT_FAILED', true, true, 'WAIT_FOR_SWEEP_RECOVERY');
  assert.equal(unknownCalls, 1);
});

test('module exposes exactly four metadata RPCs and no browser actor/private clock/lease/queue bindings', async () => {
  const source = await readFile(new URL('../broker/src/bpay-next-runtime-rpc.js', import.meta.url), 'utf8');
  assert.deepEqual([...new Set(source.match(/bpay_next_[a-z_]+_v1/g))].sort(),
    ['bpay_next_enroll_service_v1', 'bpay_next_expiry_dispatch_page_v1', 'bpay_next_job_wake_page_v1', 'bpay_next_terminal_cleanup_service_v1']);
  assert.doesNotMatch(source, /p_actor_user_id|p_clock|lease_nonce|lease_until_utc|owner_epoch|env\.[A-Z_]+|\.send\(|\.ack\(|\.retry\(|setInterval\(/);
  assert.doesNotMatch(source, /Date\.(?:parse|now)|new Date\(/);
  assert.equal(BPAY_NEXT_RUNTIME_LIMITS.responseBytes, 32768); assert.equal(BPAY_NEXT_RUNTIME_LIMITS.pageRows, 100);
});
