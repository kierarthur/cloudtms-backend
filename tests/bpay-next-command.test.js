import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

import { handleBpayNextCommand, BPAY_NEXT_COMMAND_LIMITS } from '../broker/src/bpay-next-command.js';

const ACTOR = '00000000-0000-4000-8000-000000000001';
const COMMAND = '00000000-0000-4000-8000-000000000002';
const WORKER = '00000000-0000-4000-8000-000000000003';
const TRANSFER = '00000000-0000-4000-8000-000000000004';
const INSTRUCTION = '00000000-0000-4000-8000-000000000005';
const OUTCOME = '00000000-0000-4000-8000-000000000006';
const CASH = '00000000-0000-4000-8000-000000000007';
const OTHER = '00000000-0000-4000-8000-000000000008';
const SEQUENCE = '9007199254740993';
const TIME = '2026-10-04T11:00:00.123456+01:00';
const CSV = 'Payment reference,Payee name,Sort code,Bank account number,Amount\nBP123,"TEST PAYEE",123456,12345678,180.00\n';
const INPUTS = {
  BIND_STORED_CREDIT: { command_id: COMMAND, legacy_case_id: OTHER },
  PAYE_NET: { command_id: COMMAND, run_worker_id: WORKER, entered_paye_net: '180.00' },
  TRANSFER_BEGIN: { command_id: COMMAND, run_worker_id: WORKER },
  CANCEL_REQUEST: { command_id: COMMAND, run_worker_id: WORKER },
  CASH_REISSUE: { command_id: COMMAND, return_cash_id: CASH },
  REISSUE_CANCEL: { command_id: COMMAND, transfer_id: TRANSFER },
  CSV_ISSUE: { instruction_id: INSTRUCTION, transfer_id: TRANSFER },
  INTERNAL_SETTLE: { command_id: COMMAND, transfer_id: TRANSFER },
  PREPARATION_EXPIRE: { command_id: COMMAND, run_id: OTHER, expected_deadline: TIME },
  CSV_SETTLEMENT: { command_id: COMMAND, transfer_id: TRANSFER, receipt_id: 'TEST-ONLY-SETTLEMENT', amount: '180.00', occurred_at: TIME },
  CSV_RETURN: { command_id: COMMAND, transfer_id: TRANSFER, receipt_id: 'TEST-ONLY-RETURN', amount: '180.00', occurred_at: TIME }
};
test('reissue cancellation accepts only whole transfer identity and coherent blocked receipts',async()=>{
  const body={action:'REISSUE_CANCEL',...INPUTS.REISSUE_CANCEL};
  const blocked={command_id:COMMAND,transfer_id:TRANSFER,sequence:SEQUENCE,phase:'BLOCKED',
    blocked_code:'BPAY_NEXT_REISSUE_CANCEL_INSTRUCTION_PROTECTED',replay:true};
  const dependencies=deps(blocked);
  const response=await handleBpayNextCommand(request(body),dependencies);
  assert.equal(response.status,200);assert.deepEqual((await response.json()).result,blocked);
  assert.equal(dependencies.calls.length,1);
  for(const payload of [{...blocked,blocked_code:null},{...blocked,phase:'CANCELLED'},
    {...blocked,transfer_id:OTHER}]) {
    const bad=await handleBpayNextCommand(request(body),deps(payload));
    assert.equal(bad.status,503);assert.equal((await bad.json()).outcome_unknown,true);
  }
  for(const extra of [{amount:'10.00'},{component_id:OTHER},{return_cash_id:CASH},{actor_user_id:ACTOR}]) {
    const dependencies=deps(blocked);
    const bad=await handleBpayNextCommand(request({...body,...extra}),dependencies);
    assert.equal(bad.status,400);assert.equal(dependencies.calls.length,0);
  }
});
test('preparation expiry has no caller clock or SYSTEM authority and checks exact completion receipt',async()=>{
  const body={action:'PREPARATION_EXPIRE',...INPUTS.PREPARATION_EXPIRE};
  const valid=result('PREPARATION_EXPIRE');
  assert.equal((await handleBpayNextCommand(request(body),deps(valid))).status,200);
  const completed={...valid,phase:'EXPIRED',completed_candidates:'2',replay:true};
  assert.equal((await handleBpayNextCommand(request(body),deps(completed))).status,200);
  for(const extra of [{observed_now:TIME},{initiator_kind:'SYSTEM'},{actor_user_id:ACTOR},{duration_hours:72}]) {
    const dependencies=deps(valid);
    const bad=await handleBpayNextCommand(request({...body,...extra}),dependencies);
    assert.equal(bad.status,400);assert.equal(dependencies.calls.length,0);
  }
  for(const payload of [{...valid,run_id:WORKER},{...valid,completed_candidates:'3'},
    {...valid,phase:'EXPIRED'},{...valid,expected_candidates:2},
    {...valid,deadline:TIME}]) {
    const bad=await handleBpayNextCommand(request(body),deps(payload));
    assert.equal(bad.status,503);assert.equal((await bad.json()).outcome_unknown,true);
  }
  const sql=readFileSync(new URL('../supabase/repeatable/04102026_0270_banking_pay_next_financial_intake_service_v1.sql',import.meta.url),'utf8');
  assert.match(sql,/private\.bpay_next_accept_preparation_expiry_v1\(\s*v_command_id,v_run_id,p_actor_user_id,v_occurred_at,null\)/);
});
function result(action = 'PAYE_NET') {
  switch (action) {
    case 'BIND_STORED_CREDIT': return { case_id: COMMAND, command_id: COMMAND, sequence: SEQUENCE, phase: 'ACCEPTED_PENDING_CASE', replay: false };
    case 'PAYE_NET': return { command_id: COMMAND, run_worker_id: WORKER, sequence: SEQUENCE, request_no: '1', phase: 'ACCEPTED_PENDING_PROJECTION', replay: false };
    case 'TRANSFER_BEGIN': return { command_id: COMMAND, run_worker_id: WORKER, sequence: SEQUENCE, transfer_id: TRANSFER, phase: 'BUILDING', replay: false };
    case 'CANCEL_REQUEST': return { command_id: COMMAND, run_worker_id: WORKER, sequence: SEQUENCE, phase: 'REQUESTED', cursor: null, released_line_count: '0', blocked_code: null, replay: false };
    case 'CASH_REISSUE': return { command_id: COMMAND, return_cash_id: CASH, sequence: SEQUENCE, transfer_id: null, phase: 'ACCEPTED_PENDING_BUILD', replay: false };
    case 'REISSUE_CANCEL': return { command_id: COMMAND, transfer_id: TRANSFER, sequence: SEQUENCE, phase: 'REQUESTED', blocked_code: null, replay: false };
    case 'CSV_ISSUE': return { instruction_id: INSTRUCTION, transfer_id: TRANSFER, file_name: `bpay-next-${INSTRUCTION}.csv`, csv_sha256: createHash('sha256').update(CSV).digest('hex'), csv_text: CSV, phase: 'ISSUED_CSV', payment_recorded: false, replay: false };
    case 'INTERNAL_SETTLE': return { command_id: COMMAND, transfer_id: TRANSFER, internal_receipt_id: OUTCOME, sequence: SEQUENCE, posting_complete: false, phase: 'ACCEPTED_PENDING_POSTING', replay: false };
    case 'PREPARATION_EXPIRE': return { command_id: COMMAND, run_id: OTHER, sequence: SEQUENCE, phase: 'CANCELLING', expected_candidates: '2', completed_candidates: '0', replay: false };
    default: return { command_id: COMMAND, transfer_id: TRANSFER, outcome_id: OUTCOME, sequence: SEQUENCE, posting_complete: false, phase: 'ACCEPTED_PENDING_POSTING', replay: false };
  }
}
function request(body, options = {}) {
  return new Request('https://broker.test.invalid/financial-intake', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), ...options
  });
}

test('zero-cash settlement accepts only execution identity and a server-owned typed receipt', async () => {
  const body = { action: 'INTERNAL_SETTLE', ...INPUTS.INTERNAL_SETTLE };
  for (const payload of [result('INTERNAL_SETTLE'),
    { ...result('INTERNAL_SETTLE'), posting_complete: true, phase: 'POSTED', replay: true }]) {
    const dependencies = deps(payload);
    const response = await handleBpayNextCommand(request(body), dependencies);
    assert.equal(response.status, 200);
    assert.deepEqual((await response.json()).result, payload);
    assert.equal(dependencies.calls.length, 1);
  }
  for (const extra of [{ amount: '0.00' }, { occurred_at: TIME }, { receipt_id: 'caller' },
    { internal_receipt_id: OUTCOME }, { outcome_id: OUTCOME }, { actor_user_id: ACTOR }]) {
    const dependencies = deps();
    await failed({ ...body, ...extra }, dependencies, 'BPAY_NEXT_COMMAND_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
  for (const payload of [{ ...result('INTERNAL_SETTLE'), command_id: OTHER, replay: true },
    { ...result('INTERNAL_SETTLE'), transfer_id: OTHER },
    { ...result('INTERNAL_SETTLE'), internal_receipt_id: null },
    { ...result('INTERNAL_SETTLE'), posting_complete: true },
    { ...result('INTERNAL_SETTLE'), outcome_id: OUTCOME }]) {
    await failed(body, deps(payload), 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true);
  }
});
function deps(payload = result()) {
  const calls = [];
  return {
    calls,
    async requireOfficeUser(req, roles) {
      assert.ok(req instanceof Request);
      assert.deepEqual(roles, ['admin']);
      return { id: ACTOR, role: 'admin' };
    },
    async financialCommandRpc(action, args, options) {
      calls.push({ action, args, options });
      return Response.json(payload);
    }
  };
}
async function failed(body, dependencies, code, status = 400, unknown = false) {
  const response = await handleBpayNextCommand(request(body), dependencies);
  assert.equal(response.status, status);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal(response.headers.get('x-content-type-options'), 'nosniff');
  const value = await response.json();
  assert.equal(value.error_code, code);
  assert.equal(value.outcome_unknown, unknown);
  return value;
}

for (const action of Object.keys(INPUTS)) {
  test(`${action} calls one closed raw dependency with verified actor and typed args`, async () => {
    const payload = result(action);
    const dependencies = deps(payload);
    const response = await handleBpayNextCommand(request({ action, ...INPUTS[action] }), dependencies);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { ok: true, action, result: payload });
    assert.equal(dependencies.calls.length, 1);
    const call = dependencies.calls[0];
    assert.equal(call.action, action);
    assert.deepEqual(call.args, INPUTS[action]);
    assert.equal(Object.hasOwn(call.args, 'actor_user_id'), false);
    assert.equal(Object.hasOwn(call.args, 'action'), false);
    assert.equal(Object.isFrozen(call.args), true);
    assert.equal(call.options.actorUserId, ACTOR);
    assert.ok(call.options.signal instanceof AbortSignal);
    assert.equal(call.options.signal.aborted, false);
    assert.equal(call.options.timeoutMs, 8000);
    assert.equal(call.options.maxResponseBytes, 128 * 1024);
    assert.equal(call.options.routeClass, 'OPERATION_NUDGE');
  });
}

test('money inputs and bigint receipts retain all digits, including zero PAYE net', async () => {
  for (const amount of ['9999999999999999.99', '0', '0.00']) {
    const dependencies = deps();
    const response = await handleBpayNextCommand(request({ action: 'PAYE_NET', ...INPUTS.PAYE_NET, entered_paye_net: amount }), dependencies);
    assert.equal(response.status, 200);
    assert.equal(dependencies.calls[0].args.entered_paye_net, amount);
    assert.equal((await response.json()).result.sequence, SEQUENCE);
  }
});

test('frozen case PAYE resaves bind exact prior revision; payout-only has no invented payroll net', async () => {
  for (const body of [
    { action: 'PAYE_NET', ...INPUTS.PAYE_NET, expected_projection_revision: '9007199254740993' },
    { action: 'CASE_PAYOUT', command_id: COMMAND, run_worker_id: WORKER, expected_projection_revision: '0' }
  ]) {
    const dependencies = deps(result());
    const response = await handleBpayNextCommand(request(body), dependencies);
    assert.equal(response.status, 200);
    assert.equal(dependencies.calls.length, 1);
    assert.equal(dependencies.calls[0].args.expected_projection_revision, body.expected_projection_revision);
    assert.equal(Object.hasOwn(dependencies.calls[0].args, 'entered_paye_net'), body.action === 'PAYE_NET');
    assert.equal(dependencies.calls[0].options.actorUserId, ACTOR);
  }
  for (const expected_projection_revision of [0, null, '-1', '01', '1e2', '9223372036854775807']) {
    const dependencies = deps();
    await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET, expected_projection_revision }, dependencies,
      'BPAY_NEXT_COMMAND_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
  const dependencies = deps();
  await failed({ action: 'CASE_PAYOUT', command_id: COMMAND, run_worker_id: WORKER,
    expected_projection_revision: '0', entered_paye_net: '0' }, dependencies, 'BPAY_NEXT_COMMAND_REQUEST_INVALID');
  assert.equal(dependencies.calls.length, 0);
});

test('receipt timestamps retain exact offset/microseconds and UTF-8 evidence identity', async () => {
  const dependencies = deps(result('CSV_RETURN'));
  const receipt = 'é'.repeat(128);
  const response = await handleBpayNextCommand(request({ action: 'CSV_RETURN', ...INPUTS.CSV_RETURN, receipt_id: receipt }), dependencies);
  assert.equal(response.status, 200);
  assert.equal(dependencies.calls[0].args.occurred_at, TIME);
  assert.equal(dependencies.calls[0].args.receipt_id, receipt);
});

test('CSV replay returns exact bytes and does not claim download is settlement', async () => {
  const payload = { ...result('CSV_ISSUE'), replay: true };
  const dependencies = deps(payload);
  const response = await handleBpayNextCommand(request({ action: 'CSV_ISSUE', ...INPUTS.CSV_ISSUE }), dependencies);
  const value = await response.json();
  assert.equal(response.status, 200);
  assert.equal(value.result.csv_text, CSV);
  assert.equal(value.result.csv_sha256, createHash('sha256').update(CSV).digest('hex'));
  assert.equal(value.result.payment_recorded, false);
  assert.equal(dependencies.calls.length, 1);
});

test('cancelled/blocked intake and created reissue replays retain closed progress', async () => {
  const variants = [
    ['CANCEL_REQUEST', { ...result('CANCEL_REQUEST'), phase: 'CANCELLED', cursor: SEQUENCE, released_line_count: SEQUENCE, replay: true }],
    ['CANCEL_REQUEST', { ...result('CANCEL_REQUEST'), phase: 'BLOCKED', blocked_code: 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED', replay: true }],
    ['CASH_REISSUE', { ...result('CASH_REISSUE'), transfer_id: TRANSFER, phase: 'TRANSFER_CREATED', replay: true }],
    ['TRANSFER_BEGIN', { ...result('TRANSFER_BEGIN'), phase: 'CANCELLED', replay: true }]
  ];
  for (const [action, payload] of variants) {
    const response = await handleBpayNextCommand(request({ action, ...INPUTS[action] }), deps(payload));
    assert.equal(response.status, 200);
    assert.deepEqual((await response.json()).result, payload);
  }
});

test('outcome receipt replay returns its original command identity, not a new one', async () => {
  const payload = { ...result('CSV_SETTLEMENT'), command_id: OTHER, replay: true, posting_complete: true, phase: 'POSTED' };
  const response = await handleBpayNextCommand(request({ action: 'CSV_SETTLEMENT', ...INPUTS.CSV_SETTLEMENT }), deps(payload));
  assert.equal(response.status, 200);
  assert.equal((await response.json()).result.command_id, OTHER);
  await failed({ action: 'CSV_SETTLEMENT', ...INPUTS.CSV_SETTLEMENT }, deps({ ...payload, replay: false }), 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true);
});

test('caller actors, money overrides, function names, candidate/line subsets and job claims are rejected', async () => {
  for (const extra of [
    { actor_user_id: ACTOR }, { p_actor_user_id: ACTOR }, { user_id: ACTOR },
    { role: 'admin' }, { rpc: 'private.bpay_next_receive_command_v1' },
    { function: 'pay_execute_bank' }, { sql: 'select 1' }, { cash_amount: '1.00' },
    { entered_paye_net: '1.00' }, { candidate_id: OTHER }, { run_line_ids: [OTHER] },
    { job_id: OTHER }, { lease_nonce: OTHER }, { owner_epoch: '1' }, { args: {} }
  ]) {
    const dependencies = deps(result('CANCEL_REQUEST'));
    await failed({ action: 'CANCEL_REQUEST', ...INPUTS.CANCEL_REQUEST, ...extra }, dependencies, 'BPAY_NEXT_COMMAND_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('only the closed actions and complete exact required keys are accepted', async () => {
  for (const body of [null, [], {}, { action: 'constructor' }, { action: '__proto__' },
    { action: 'JOB_CLAIM' }, { action: 'paye_net', ...INPUTS.PAYE_NET },
    { action: 'PAYE_NET', command_id: COMMAND, run_worker_id: WORKER },
    { action: 'TRANSFER_BEGIN', ...INPUTS.TRANSFER_BEGIN, amount: '1' },
    { action: 'CANCEL_REQUEST', command_id: COMMAND, run_line_id: WORKER }]) {
    const dependencies = deps();
    await failed(body, dependencies, 'BPAY_NEXT_COMMAND_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('non-string, exponent, negative, overprecision and out-of-range money never reach RPC', async () => {
  for (const entered_paye_net of [180, null, false, {}, '', 'NaN', 'Infinity', '1e2', '-1', '-0', '+1', ' 1', '1 ', '.1', '1.', '01', '1.001', '10000000000000000.00']) {
    const dependencies = deps();
    await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET, entered_paye_net }, dependencies, 'BPAY_NEXT_COMMAND_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
  for (const amount of ['0', '0.0', '0.00']) {
    await failed({ action: 'CSV_SETTLEMENT', ...INPUTS.CSV_SETTLEMENT, amount }, deps(), 'BPAY_NEXT_COMMAND_REQUEST_INVALID');
  }
});

test('invalid identifiers, impossible/unzoned dates and oversized evidence are rejected', async () => {
  for (const changes of [
    { command_id: 123 }, { transfer_id: 'not-a-uuid' }, { receipt_id: '' },
    { receipt_id: 'é'.repeat(129) }, { receipt_id: 'receipt\0value' },
    { occurred_at: '2026-02-30T11:00:00Z' }, { occurred_at: '0000-01-01T00:00:00Z' },
    { occurred_at: '2026-10-04T11:00:00' }, { occurred_at: '2026-10-04T25:00:00Z' },
    { occurred_at: '2026-10-04T11:00:00.1234567Z' }, { occurred_at: 'infinity' }
  ]) {
    const dependencies = deps();
    await failed({ action: 'CSV_RETURN', ...INPUTS.CSV_RETURN, ...changes }, dependencies, 'BPAY_NEXT_COMMAND_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('existing admin auth runs before body parsing and unauthorised requests never invoke RPC', async () => {
  let called = false;
  const dependencies = { async requireOfficeUser(req, roles) { assert.deepEqual(roles, ['admin']); return null; }, async financialCommandRpc() { called = true; } };
  const response = await handleBpayNextCommand(new Request('https://broker.test.invalid', { method: 'POST', body: 'not JSON' }), dependencies);
  assert.equal(response.status, 401);
  assert.equal((await response.json()).error_code, 'BPAY_NEXT_COMMAND_UNAUTHORIZED');
  assert.equal(called, false);
  for (const user of [{ id: ACTOR, role: 'user' }, { id: 'not-uuid', role: 'admin' }]) {
    await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, { ...deps(), async requireOfficeUser() { return user; } },
      user.role === 'admin' ? 'BPAY_NEXT_COMMAND_UNAUTHORIZED' : 'BPAY_NEXT_COMMAND_FORBIDDEN', user.role === 'admin' ? 401 : 403);
  }
});

test('non-POST and missing dependencies cannot perform an operation', async () => {
  const dependencies = deps();
  const response = await handleBpayNextCommand(new Request('https://broker.test.invalid', { method: 'GET' }), dependencies);
  assert.equal(response.status, 405);
  assert.equal(dependencies.calls.length, 0);
  for (const missing of [{}, { requireOfficeUser: dependencies.requireOfficeUser }]) {
    const value = await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, missing, 'BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503);
    assert.equal(value.retryable, true);
  }
});

test('request bytes are bounded before parsing, including dishonest/missing content-length', async () => {
  for (const headers of [{}, { 'content-length': '1' }, { 'content-length': '999999' }]) {
    const dependencies = deps();
    const response = await handleBpayNextCommand(new Request('https://broker.test.invalid', { method: 'POST', headers, body: ' '.repeat(BPAY_NEXT_COMMAND_LIMITS.requestBytes + 1) }), dependencies);
    assert.equal(response.status, 413);
    assert.equal((await response.json()).outcome_unknown, false);
    assert.equal(dependencies.calls.length, 0);
  }
});

test('responses are closed typed receipts, not arbitrary JSON/number/candidate trees', async () => {
  for (const payload of [null, [], {}, { ...result(), sequence: 9007199254740993 },
    { ...result(), request_no: 1 }, { ...result(), sequence: '9223372036854775808' },
    { ...result(), sequence: '0' }, { ...result(), phase: 'COMPLETE' },
    { ...result(), history: [] }, { ...result(), cash_amount: '1.00' },
    { ...result(), actor_user_id: ACTOR }, { ...result(), run_worker_id: OTHER }]) {
    await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, deps(payload), 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true);
  }
  await failed({ action: 'CANCEL_REQUEST', ...INPUTS.CANCEL_REQUEST }, deps({ ...result('CANCEL_REQUEST'), released_line_count: 1 }), 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true);
  await failed({ action: 'CASH_REISSUE', ...INPUTS.CASH_REISSUE }, deps({ ...result('CASH_REISSUE'), phase: 'TRANSFER_CREATED' }), 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true);
  await failed({ action: 'CSV_RETURN', ...INPUTS.CSV_RETURN }, deps({ ...result('CSV_RETURN'), posting_complete: true }), 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true);
});

test('known DB transaction rejections are safe and distinguish permission/conflict/missing interface', async () => {
  for (const [code, message, expected, status, retryable] of [
    ['42501', 'secret actor details', 'BPAY_NEXT_COMMAND_FORBIDDEN', 403, false],
    ['23514', 'secret row details', 'BPAY_NEXT_COMMAND_CONFLICT', 409, false],
    ['23505', 'secret constraint details', 'BPAY_NEXT_COMMAND_CONFLICT', 409, false],
    ['22023', 'secret invalid details', 'BPAY_NEXT_COMMAND_REQUEST_INVALID', 400, false],
    ['22008', 'secret date details', 'BPAY_NEXT_COMMAND_REQUEST_INVALID', 400, false],
    ['P0002', 'secret row details', 'BPAY_NEXT_COMMAND_SCOPE_NOT_FOUND', 404, false],
    ['PGRST202', 'secret schema details', 'BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503, true],
    ['55000', 'BPAY_NEXT_MODULE_NOT_ACTIVE', 'BPAY_NEXT_COMMAND_MODULE_NOT_ACTIVE', 503, false],
    ['55000', 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED', 'BPAY_NEXT_COMMAND_NOT_ELIGIBLE', 409, false],
    ['55000', 'BPAY_NEXT_CSV_CONFIGURED_FORMAT_NOT_YET_SUPPORTED', 'BPAY_NEXT_COMMAND_CSV_CONFIGURATION_UNSUPPORTED', 409, false],
    ['54000', 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, false]
  ]) {
    const dependencies = { ...deps(), async financialCommandRpc() { return Response.json({ code, message, details: 'secret' }, { status: 400 }); } };
    const value = await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, dependencies, expected, status);
    assert.equal(value.retryable, retryable);
    assert.equal(JSON.stringify(value).includes('secret'), false);
  }
});

test('unexpected dependency errors expose no message and report an unknown attempted outcome', async () => {
  for (const failure of [new Error('secret transport URL'), { code: 'BPAY_NEXT_COMMAND_SECRET_VALUE', message: 'secret' }]) {
    const dependencies = { ...deps(), async financialCommandRpc() { throw failure; } };
    const value = await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, dependencies, 'BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503, true);
    assert.equal(JSON.stringify(value).includes('secret'), false);
    assert.equal(value.retryable, true);
  }
});

test('non-raw/corrupt/invalid UTF-8 responses are rejected with exact-ID retry semantics', async () => {
  for (const response of [result(), new Response('not JSON'),
    new Response(new Uint8Array([0xc3, 0x28]))]) {
    await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, { ...deps(), async financialCommandRpc() { return response; } }, 'BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true);
  }
});

test('response cap applies to declared and streamed bytes and cancels oversized streams', async () => {
  let cancelled = false;
  const response = new Response(new ReadableStream({
    start(controller) { controller.enqueue(new Uint8Array(BPAY_NEXT_COMMAND_LIMITS.responseBytes + 1)); },
    cancel() { cancelled = true; }
  }));
  await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, { ...deps(), async financialCommandRpc() { return response; } }, 'BPAY_NEXT_COMMAND_RESPONSE_TOO_LARGE', 503, true);
  assert.equal(cancelled, true);
  const declared = new Response('{}', { headers: { 'content-length': String(BPAY_NEXT_COMMAND_LIMITS.responseBytes + 1) } });
  await failed({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }, { ...deps(), async financialCommandRpc() { return declared; } }, 'BPAY_NEXT_COMMAND_RESPONSE_TOO_LARGE', 503, true);
});

test('deadline aborts a pending response stream without automatic re-dispatch', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let signal;
  let cancelled = false;
  let calls = 0;
  const dependencies = { ...deps(), async financialCommandRpc(action, args, options) {
    calls += 1;
    signal = options.signal;
    return new Response(new ReadableStream({ cancel() { cancelled = true; } }));
  } };
  const pending = handleBpayNextCommand(request({ action: 'CANCEL_REQUEST', ...INPUTS.CANCEL_REQUEST }), dependencies);
  for (let turn = 0; turn < 20 && !signal; turn++) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(signal);
  t.mock.timers.tick(BPAY_NEXT_COMMAND_LIMITS.timeoutMs);
  const response = await pending;
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { ok: false, error_code: 'BPAY_NEXT_COMMAND_DEPENDENCY_TIMEOUT', retryable: true, outcome_unknown: true });
  assert.equal(signal.aborted, true);
  assert.equal(cancelled, true);
  assert.equal(calls, 1);
});

test('service wrapper source closes actions, independently authorises actor and stringifies numeric outputs', () => {
  // Source contract only; parent task owns actual local PostgreSQL first-use.
  const sql = readFileSync(new URL('../supabase/repeatable/04102026_0270_banking_pay_next_financial_intake_service_v1.sql', import.meta.url), 'utf8');
  assert.match(sql, /public\.bpay_next_financial_intake_v1\(\s*p_actor_user_id uuid,p_action text,p_args jsonb/);
  assert.match(sql, /request\.jwt\.claim\.role/);
  assert.match(sql, /u\.is_active is true[\s\S]*u\.role::text='admin'/);
  assert.match(sql, /jsonb_typeof\(p_args->v_key\) is distinct from 'string'/);
  assert.match(sql, /c\.agency_sequence::text/);
  assert.match(sql, /'released_line_count',v_result->>'released_line_count'/);
  assert.match(sql, /from public,anon,authenticated,service_role/);
  assert.match(sql, /grant execute on function public\.bpay_next_financial_intake_v1\(uuid,text,jsonb\) to service_role/);
  assert.match(sql, /private\.bpay_next_issue_simple_csv_v1\(v_instruction_id,v_transfer_id,p_actor_user_id\)/);
  assert.match(sql, /private\.bpay_next_receive_simple_reissue_v1\(v_command_id,v_return_cash_id,p_actor_user_id\)/);
  const csvBranch = sql.slice(sql.indexOf("when 'CSV_ISSUE' then\n      --"));
  assert.ok(csvBranch.indexOf('where id=v_run_id for update;') >= 0);
  assert.ok(csvBranch.indexOf('where id=v_run_id for update;')
    < csvBranch.indexOf('if not exists(select 1 from private.bpay_next_csv_instruction'));
  assert.doesNotMatch(sql, /\bexecute\s+(?:format|v_sql)|return\s+v_result\s*;/i);
});

test('a raw response arriving after timeout is cancelled, never consumed or re-dispatched', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let finish;
  let signal;
  let cancelled = false;
  let calls = 0;
  const dependencies = { ...deps(), async financialCommandRpc(action, args, options) {
    calls += 1;
    signal = options.signal;
    return new Promise((resolve) => { finish = resolve; });
  } };
  const pending = handleBpayNextCommand(request({ action: 'PAYE_NET', ...INPUTS.PAYE_NET }), dependencies);
  for (let turn = 0; turn < 20 && !finish; turn++) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(finish);
  t.mock.timers.tick(BPAY_NEXT_COMMAND_LIMITS.timeoutMs);
  const response = await pending;
  assert.equal(response.status, 503);
  assert.equal((await response.json()).outcome_unknown, true);
  assert.equal(signal.aborted, true);
  finish(new Response(new ReadableStream({ cancel() { cancelled = true; } })));
  for (let turn = 0; turn < 20 && !cancelled; turn++) await new Promise((resolve) => setImmediate(resolve));
  assert.equal(cancelled, true);
  assert.equal(calls, 1);
});

test('original stored credit bind accepts only two identities and coherent same-ID replay, never caller economic facts', async () => {
  const body={action:'BIND_STORED_CREDIT',...INPUTS.BIND_STORED_CREDIT};
  for(const replay of [false,true]) {
    const response=await handleBpayNextCommand(request(body),deps({...result('BIND_STORED_CREDIT'),replay}));
    assert.equal(response.status,200);assert.equal((await response.json()).result.case_id,COMMAND);
  }
  for(const extra of [{amount:'40.00'},{bank_details_hash:'caller'},{actor_user_id:ACTOR},{candidate_id:WORKER},
    {original_source_pay_channel:'UMBRELLA'},{rpc:'arbitrary'}]) {
    const dependencies=deps(result('BIND_STORED_CREDIT'));
    await failed({...body,...extra},dependencies,'BPAY_NEXT_COMMAND_REQUEST_INVALID');
    assert.equal(dependencies.calls.length,0);
  }
  for(const output of [{...result('BIND_STORED_CREDIT'),case_id:OTHER,replay:true},
    {...result('BIND_STORED_CREDIT'),command_id:OTHER}, {...result('BIND_STORED_CREDIT'),sequence:9007199254740992},
    {...result('BIND_STORED_CREDIT'),legacy_case_id:OTHER}, {...result('BIND_STORED_CREDIT'),phase:'CASE_CREATED'}]) {
    await failed(body,deps(output),'BPAY_NEXT_COMMAND_RESPONSE_INVALID',503,true);
  }
});

test('lost original-credit reply preserves unknown outcome and identical caller-owned IDs without automatic retry', async () => {
  const body={action:'BIND_STORED_CREDIT',...INPUTS.BIND_STORED_CREDIT};
  const calls=[];
  const dependencies={...deps(),async financialCommandRpc(action,args,options){
    calls.push({action,args,actor:options.actorUserId});
    if(calls.length===1)throw new TypeError('simulated after-commit reply loss');
    return Response.json({...result('BIND_STORED_CREDIT'),replay:true});
  }};
  const lost=await failed(body,dependencies,'BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE',503,true);
  assert.equal(lost.retryable,true);assert.equal(calls.length,1);
  const recovered=await handleBpayNextCommand(request(body),dependencies);
  assert.equal(recovered.status,200);assert.equal((await recovered.json()).result.replay,true);
  assert.deepEqual(calls[1],calls[0]);assert.equal(calls.length,2);
});
