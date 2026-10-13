import assert from 'node:assert/strict';
import test from 'node:test';

import {
  handleBpayNextFrozenRead,
  BPAY_NEXT_FROZEN_READ_LIMITS
} from '../broker/src/bpay-next-frozen-read.js';

const ACTOR = '00000000-0000-4000-8000-000000000001';
const LINE = '00000000-0000-4000-8000-000000000002';
const WORK = '00000000-0000-4000-8000-000000000003';
const SHIFT = '00000000-0000-4000-8000-000000000004';
const DETAIL = '00000000-0000-4000-8000-000000000005';

function request(body, options = {}) {
  return new Request('https://broker.test.invalid/frozen-page', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    ...options
  });
}

function shifts(detailNo = 1) {
  return {
    shift_detail_id: SHIFT, detail_no: detailNo, work_date: '2026-10-03',
    shift_start_at: '2026-10-03T09:00:00+01:00',
    shift_end_at: '2026-10-03T17:00:00+01:00',
    shift_start_local: null, shift_end_local: null, shift_overnight: false,
    submitted_minutes: 480, approved_minutes: 450, approved_hours: '7.500000',
    detail_label: 'Frozen approved shift',
    segment_pay_ex_vat: null, pay_excluded: null,
    hours_day: null, hours_night: null, hours_sat: null, hours_sun: null, hours_bh: null
  };
}

function deps(rows = []) {
  const calls = [];
  return {
    calls,
    async requireOfficeUser(req, roles) {
      assert.ok(req instanceof Request);
      assert.deepEqual(roles, ['admin']);
      return { id: ACTOR, role: 'admin' };
    },
    async frozenPageRpc(kind, args, options) {
      calls.push({ kind, args, options });
      return Response.json({ rows, complete: Array.isArray(rows) && rows.length < args.p_limit });
    }
  };
}

async function error(body, dependencies, code, status = 400) {
  const response = await handleBpayNextFrozenRead(request(body), dependencies);
  assert.equal(response.status, status);
  const result = await response.json();
  assert.equal(result.error_code, code);
  return result;
}

test('one frozen shift page uses the validated Office actor and exact SQL arguments', async () => {
  const dependencies = deps([shifts(3)]);
  const response = await handleBpayNextFrozenRead(request({
    kind: 'SHIFTS', run_line_id: LINE, after_detail_no: 2, limit: 1
  }), dependencies);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await response.json(), {
    ok: true, kind: 'SHIFTS', rows: [shifts(3)], complete: false,
    next_cursor: { after_detail_no: 3 }
  });
  assert.equal(dependencies.calls.length, 1);
  const call = dependencies.calls[0];
  assert.equal(call.kind, 'SHIFTS');
  assert.deepEqual(call.args, { p_limit: 1, p_run_line_id: LINE, p_after_detail_no: 2 });
  assert.equal(call.options.actorUserId, ACTOR);
  assert.ok(call.options.signal instanceof AbortSignal);
  assert.equal(call.options.timeoutMs, 8000);
  assert.equal(call.options.maxResponseBytes, BPAY_NEXT_FROZEN_READ_LIMITS.responseBytes);
});

test('ordinary day shift detail retains exact money, exclusions and bucket hours without unused rates', async () => {
  const row = { ...shifts(), segment_pay_ex_vat: '9999999999999999.99',
    pay_excluded: false, hours_day: '7.500000', hours_night: '0.000000',
    hours_sat: '0.000000', hours_sun: '0.000000', hours_bh: '0.000000' };
  const response = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), deps([row]));
  assert.equal(response.status, 200);
  assert.deepEqual((await response.json()).rows, [row]);
  const excluded = { ...row, segment_pay_ex_vat: '0.00', pay_excluded: true };
  const excludedResponse = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), deps([excluded]));
  assert.equal(excludedResponse.status, 200);
  assert.deepEqual((await excludedResponse.json()).rows, [excluded]);
  for (const invalid of [
    { ...excluded, segment_pay_ex_vat: '0.01' },
    { ...row, segment_pay_ex_vat: 120 },
    { ...row, segment_pay_ex_vat: '1.001' },
    { ...row, hours_day: null },
    { ...row, hours_day: '-0.000001' },
    { ...row, pay_day: '16.00' }
  ]) await error({ kind: 'SHIFTS', run_line_id: LINE }, deps([invalid]), 'BPAY_NEXT_FROZEN_RESPONSE_INVALID', 503);
});

test('breaks retain duration-only evidence and cannot omit the frozen shift identity', async () => {
  const row = {
    break_detail_id: DETAIL, break_no: 2,
    break_start_at: null, break_end_at: null,
    break_start_local: null, break_end_local: null, break_minutes: 30
  };
  const dependencies = deps([row]);
  const response = await handleBpayNextFrozenRead(request({
    kind: 'BREAKS', run_line_id: LINE, shift_detail_id: SHIFT,
    after_break_no: 1, limit: 2
  }), dependencies);
  assert.deepEqual(await response.json(), {
    ok: true, kind: 'BREAKS', rows: [row], complete: true, next_cursor: null
  });
  assert.deepEqual(dependencies.calls[0].args, {
    p_limit: 2, p_run_line_id: LINE, p_after_break_no: 1, p_shift_detail_id: SHIFT
  });
  await error({ kind: 'BREAKS', run_line_id: LINE }, deps(), 'BPAY_NEXT_FROZEN_REQUEST_INVALID');
});

test('available rate options are read once per run_work with the complete text tuple cursor', async () => {
  const row = {
    rate_schedule_id: DETAIL, rate_family: 'ADDITIONAL', rate_code: 'β-code',
    unit_label: 'approved unit', paye_rate: '999999999999.123456',
    umbrella_rate: '0.000000', charge_rate: null
  };
  const dependencies = deps([row]);
  const response = await handleBpayNextFrozenRead(request({
    kind: 'RATE_SCHEDULE', run_work_id: WORK,
    after_rate_family: 'ADDITIONAL', after_rate_code: 'A', limit: 1
  }), dependencies);
  assert.deepEqual(await response.json(), {
    ok: true, kind: 'RATE_SCHEDULE', rows: [row], complete: false,
    next_cursor: { after_rate_family: 'ADDITIONAL', after_rate_code: 'β-code' }
  });
  assert.deepEqual(dependencies.calls[0].args, {
    p_limit: 1, p_run_work_id: WORK, p_after_rate_family: 'ADDITIONAL', p_after_rate_code: 'A'
  });
});

test('Source used-rate detail keeps unknown rates null and exact signed decimal hours', async () => {
  const row = { rate_detail_id: DETAIL, bucket: 'NIGHT', approved_hours: '-0.000001', source_pay_rate: null };
  const dependencies = deps([row]);
  const response = await handleBpayNextFrozenRead(request({
    kind: 'RATE_DETAILS', run_line_id: LINE, after_bucket: 'DAY', limit: 1
  }), dependencies);
  assert.deepEqual(await response.json(), {
    ok: true, kind: 'RATE_DETAILS', rows: [row], complete: false,
    next_cursor: { after_bucket: 'NIGHT' }
  });
  assert.deepEqual(dependencies.calls[0].args, {
    p_limit: 1, p_run_line_id: LINE, p_after_bucket: 'DAY'
  });
});

test('empty pages end traversal without totals, scans or automatic next-page calls', async () => {
  const dependencies = deps();
  const response = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), dependencies);
  assert.deepEqual(await response.json(), { ok: true, kind: 'SHIFTS', rows: [], complete: true, next_cursor: null });
  assert.equal(dependencies.calls.length, 1);
  assert.equal(dependencies.calls[0].args.p_limit, 50);
});

test('a byte-limited short page retains the explicit continuation cursor', async () => {
  const dependencies = deps();
  dependencies.frozenPageRpc = async () => Response.json({ rows: [shifts(7)], complete: false });
  const response = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE, limit: 50 }), dependencies);
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    ok: true, kind: 'SHIFTS', rows: [shifts(7)], complete: false,
    next_cursor: { after_detail_no: 7 }
  });
});

test('caller actor, candidate, live Timesheet and arbitrary function fields cannot reach the RPC', async () => {
  for (const extra of [
    { actor_user_id: WORK }, { p_actor_user_id: WORK }, { user: { id: WORK, role: 'admin' } },
    { candidate_id: WORK }, { timesheet_id: WORK }, { function_name: 'pay_batch_get' },
    { include_history: true }, { include_current_rates: true }
  ]) {
    const dependencies = deps();
    await error({ kind: 'SHIFTS', run_line_id: LINE, ...extra }, dependencies, 'BPAY_NEXT_FROZEN_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('authentication precedes body parsing and rejects inactive or non-Office identities', async () => {
  for (const [user, status, code] of [
    [null, 401, 'BPAY_NEXT_FROZEN_UNAUTHORIZED'],
    [{ id: 'invalid', role: 'admin' }, 401, 'BPAY_NEXT_FROZEN_UNAUTHORIZED'],
    [{ id: ACTOR, role: 'candidate' }, 403, 'BPAY_NEXT_FROZEN_FORBIDDEN']
  ]) {
    const dependencies = deps();
    dependencies.requireOfficeUser = async () => user;
    const response = await handleBpayNextFrozenRead(request(null, { body: 'not json' }), dependencies);
    assert.equal(response.status, status);
    assert.equal((await response.json()).error_code, code);
    assert.equal(dependencies.calls.length, 0);
  }
});

test('missing public RPC wiring stays a retryable dependency error', async () => {
  const dependencies = deps();
  delete dependencies.frozenPageRpc;
  const result = await error({ kind: 'SHIFTS', run_line_id: LINE }, dependencies,
    'BPAY_NEXT_FROZEN_DEPENDENCY_UNAVAILABLE', 503);
  assert.equal(result.retryable, true);
});

test('bad bounds, half tuple cursors and wrong page kinds are rejected before dispatch', async () => {
  for (const body of [
    { kind: 'SHIFTS', run_line_id: LINE, limit: 0 },
    { kind: 'SHIFTS', run_line_id: LINE, limit: 101 },
    { kind: 'SHIFTS', run_line_id: LINE, limit: '10' },
    { kind: 'SHIFTS', run_line_id: LINE, after_detail_no: -1 },
    { kind: 'SHIFTS', run_line_id: LINE, after_detail_no: 2147483648 },
    { kind: 'SHIFTS', run_line_id: 'bad' },
    { kind: 'RATE_DETAILS', run_line_id: LINE, after_bucket: 'TODAY' },
    { kind: 'RATE_SCHEDULE', run_work_id: WORK, after_rate_family: 'STANDARD' },
    { kind: 'RATE_SCHEDULE', run_work_id: WORK, after_rate_code: 'DAY' },
    { kind: 'CURRENT_TIMESHEET', run_line_id: LINE }
  ]) {
    const dependencies = deps();
    await error(body, dependencies, 'BPAY_NEXT_FROZEN_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('local overnight clocks remain local without fabricated UTC dates', async () => {
  const row = { ...shifts(), shift_start_at: null, shift_end_at: null,
    shift_start_local: '21:00', shift_end_local: '07:30', shift_overnight: true };
  const response = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), deps([row]));
  assert.equal(response.status, 200);
  assert.deepEqual((await response.json()).rows, [row]);
});

test('PostgreSQL microsecond timestamp detail is preserved without JS millisecond rounding', async () => {
  const row = { ...shifts(), shift_start_at: '2026-10-03T09:00:00.000001Z',
    shift_end_at: '2026-10-03T09:00:00.000002Z' };
  const response = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), deps([row]));
  assert.equal(response.status, 200);
  assert.deepEqual((await response.json()).rows, [row]);
});

test('no live fallback or fabricated zero conceals malformed frozen rows', async () => {
  for (const row of [
    { ...shifts(), approved_hours: 7.5 }, // JSON numeric already lost the exact transport contract.
    { ...shifts(), approved_hours: '1e2' },
    { ...shifts(), approved_hours: '0.0000001' },
    { ...shifts(), detail_label: 'é'.repeat(4097) },
    { ...shifts(), work_date: '2026-02-30' },
    { ...shifts(), shift_end_at: null },
    { ...shifts(), submitted_minutes: -1 },
    { ...shifts(), current_candidate_name: 'mutable value' },
    { ...shifts(), approved_minutes: null, approved_hours: null,
      shift_start_at: null, shift_end_at: null, shift_start_local: null, shift_end_local: null }
  ]) {
    const dependencies = deps([row]);
    await error({ kind: 'SHIFTS', run_line_id: LINE }, dependencies, 'BPAY_NEXT_FROZEN_RESPONSE_INVALID', 503);
    assert.equal(dependencies.calls.length, 1);
  }
  const missing = shifts();
  delete missing.approved_hours;
  await error({ kind: 'SHIFTS', run_line_id: LINE }, deps([missing]), 'BPAY_NEXT_FROZEN_RESPONSE_INVALID', 503);
});

test('oversized, duplicate and stale page results cannot produce a looping cursor', async () => {
  for (const rows of [[shifts(1), shifts(2)], [shifts(3), shifts(3)], [shifts(3), shifts(2)], [shifts(1)]]) {
    await error({ kind: 'SHIFTS', run_line_id: LINE, limit: rows.length === 2 ? 1 : 2, after_detail_no: 1 },
      deps(rows), 'BPAY_NEXT_FROZEN_RESPONSE_INVALID', 503);
  }
  await error({ kind: 'SHIFTS', run_line_id: LINE, limit: 2 }, deps([shifts(3), shifts(3)]),
    'BPAY_NEXT_FROZEN_RESPONSE_INVALID', 503);
  await error({ kind: 'SHIFTS', run_line_id: LINE, limit: 2 }, deps([shifts(3), shifts(2)]),
    'BPAY_NEXT_FROZEN_RESPONSE_INVALID', 503);
});

test('frozen detail survives changes in unrelated mutable data without reading that data', async () => {
  const frozen = shifts();
  let currentTimesheet = { approved_hours: '7.500000', client: 'Original client' };
  const dependencies = deps([frozen]);
  Object.defineProperty(dependencies, 'loadCurrentTimesheet', { get() { throw new Error('live read forbidden'); } });
  const first = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), dependencies);
  const original = await first.text();
  currentTimesheet = { approved_hours: '8.000000', client: 'Changed client' };
  assert.equal(currentTimesheet.approved_hours, '8.000000');
  const later = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), dependencies);
  assert.equal(await later.text(), original);
  assert.equal(dependencies.calls.length, 2);
});

test('unknown dependency bodies and forged internal-looking errors do not leak values', async () => {
  for (const failure of [
    new Error('sensitive upstream detail'),
    Object.assign(new Error('sensitive upstream detail'), { code: 'BPAY_NEXT_FROZEN_sensitive_value' }),
    Response.json({ code: 'XX000', message: 'sensitive upstream detail', row: { bank: 'sensitive value' } }, { status: 500 })
  ]) {
    const dependencies = deps();
    dependencies.frozenPageRpc = async () => {
      if (failure instanceof Response) return failure;
      throw failure;
    };
    const result = await error({ kind: 'SHIFTS', run_line_id: LINE }, dependencies,
      'BPAY_NEXT_FROZEN_DEPENDENCY_UNAVAILABLE', 503);
    assert.equal(JSON.stringify(result).includes('sensitive'), false);
    assert.equal(result.retryable, true);
  }
});

test('PostgREST cache miss, denied scope and unavailable immutable detail remain distinct', async () => {
  for (const [payload, status, expectedStatus, code, retryable] of [
    [{ code: 'PGRST202', message: 'missing wrapper' }, 404, 503, 'BPAY_NEXT_FROZEN_DEPENDENCY_UNAVAILABLE', true],
    [{ code: '42501', message: 'denied' }, 403, 403, 'BPAY_NEXT_FROZEN_FORBIDDEN', false],
    [{ code: 'P0002', message: 'no frozen line' }, 400, 404, 'BPAY_NEXT_FROZEN_DETAIL_NOT_FOUND', false],
    [{ code: '55000', message: 'BPAY_NEXT_BREAK_SHIFT_NOT_IN_FROZEN_LINE' }, 400, 409, 'BPAY_NEXT_FROZEN_DETAIL_NOT_AVAILABLE', false],
    [{ code: '55000', message: 'BPAY_NEXT_FROZEN_DETAIL_NOT_AVAILABLE' }, 400, 409, 'BPAY_NEXT_FROZEN_DETAIL_NOT_AVAILABLE', false]
  ]) {
    const dependencies = deps();
    dependencies.frozenPageRpc = async () => Response.json(payload, { status });
    const result = await error({ kind: 'SHIFTS', run_line_id: LINE }, dependencies, code, expectedStatus);
    assert.equal(result.retryable, retryable);
  }
});

test('streamed response bytes are capped before parse and the stream is cancelled', async () => {
  const dependencies = deps();
  let cancellations = 0;
  let emitted = 0;
  dependencies.frozenPageRpc = async () => new Response(new ReadableStream({
    pull(controller) {
      emitted += 1;
      controller.enqueue(new Uint8Array(16384).fill(32));
    },
    cancel() { cancellations += 1; }
  }));
  await error({ kind: 'SHIFTS', run_line_id: LINE }, dependencies, 'BPAY_NEXT_FROZEN_PAGE_TOO_LARGE', 503);
  assert.equal(cancellations, 1);
  assert.ok(emitted <= 10);
});

test('declared and actual request byte limits reject before any frozen RPC', async () => {
  const dependencies = deps();
  let response = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }, {
    headers: { 'content-length': String(BPAY_NEXT_FROZEN_READ_LIMITS.requestBytes + 1) }
  }), dependencies);
  assert.equal(response.status, 413);
  response = await handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE,
    actor_user_id: 'x'.repeat(BPAY_NEXT_FROZEN_READ_LIMITS.requestBytes) }), dependencies);
  assert.equal(response.status, 413);
  assert.equal(dependencies.calls.length, 0);
});

test('malformed JSON and unsupported HTTP methods never call the frozen dependency', async () => {
  const dependencies = deps();
  let response = await handleBpayNextFrozenRead(request(null, { body: '{' }), dependencies);
  assert.equal(response.status, 400);
  response = await handleBpayNextFrozenRead(new Request('https://broker.test.invalid/frozen-page'), dependencies);
  assert.equal(response.status, 405);
  assert.equal(dependencies.calls.length, 0);
});

test('the dependency deadline aborts and cancels a response body without leaking its partial content', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const dependencies = deps();
  let started;
  const rpcStarted = new Promise((resolve) => { started = resolve; });
  let signal;
  let cancellations = 0;
  dependencies.frozenPageRpc = async (kind, args, options) => {
    signal = options.signal;
    const response = new Response(new ReadableStream({
      start(controller) { controller.enqueue(new TextEncoder().encode('[{"sensitive_partial":')); },
      cancel() { cancellations += 1; }
    }));
    started();
    return response;
  };
  const pending = handleBpayNextFrozenRead(request({ kind: 'SHIFTS', run_line_id: LINE }), dependencies);
  await rpcStarted;
  t.mock.timers.tick(BPAY_NEXT_FROZEN_READ_LIMITS.timeoutMs);
  const response = await pending;
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), {
    ok: false, error_code: 'BPAY_NEXT_FROZEN_DEPENDENCY_TIMEOUT', retryable: true
  });
  assert.equal(signal.aborted, true);
  assert.equal(cancellations, 1);
});

test('the final JSON envelope is bounded as well as the raw RPC array', async () => {
  const rows = Array.from({ length: 16 }, (_, index) => ({ ...shifts(index + 1), detail_label: '' }));
  const maximum = BPAY_NEXT_FROZEN_READ_LIMITS.responseBytes;
  const targetArrayBytes = maximum - 10;
  let remaining = targetArrayBytes - Buffer.byteLength(JSON.stringify(rows));
  for (const row of rows) {
    const length = Math.min(8192, remaining);
    row.detail_label = 'x'.repeat(length);
    remaining -= length;
  }
  assert.equal(remaining, 0);
  assert.equal(Buffer.byteLength(JSON.stringify(rows)), targetArrayBytes);
  await error({ kind: 'SHIFTS', run_line_id: LINE, limit: 16 }, deps(rows),
    'BPAY_NEXT_FROZEN_PAGE_TOO_LARGE', 503);
});
