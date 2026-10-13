import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

import { stepBpayNextFinancialJob, BPAY_NEXT_JOB_LIMITS } from '../broker/src/bpay-next-job-step.js';

const JOB = '00000000-0000-4000-8000-000000000001';
const NONCE = '00000000-0000-4000-8000-000000000002';
const EFFECT = '00000000-0000-4000-8000-000000000003';
const OTHER = '00000000-0000-4000-8000-000000000004';
const EPOCH = '9007199254740993';
const CURSOR = '9007199254740995';
const UNTIL = '2026-10-04T14:15:16.123456Z';
const KINDS = ['PAYE_NET_ENTRY', 'TRANSFER_BUILD', 'CSV_SETTLEMENT', 'CSV_RETURN', 'CASH_REISSUE', 'SIMPLE_CANCEL', 'REISSUE_CANCEL', 'INTERNAL_SETTLEMENT', 'CASE_CREATE'];
const PAGED = ['TRANSFER_BUILD', 'CSV_SETTLEMENT', 'SIMPLE_CANCEL', 'INTERNAL_SETTLEMENT'];

test('PF06 observes one completed bounded read with visited rows and preserves unknown counts', async () => {
  for (const kind of ['TRANSFER_BUILD', 'CSV_RETURN']) {
    const payload = result(kind), seen = [], dependencies = deps(payload);
    dependencies.onStepMeasurement = (line, busy) => seen.push({ line, busy });
    const outcome = await stepBpayNextFinancialJob(input(kind), dependencies);
    assert.equal(outcome.ok, true);
    assert.equal(dependencies.calls.length, 1);
    assert.equal(seen.length, 1);
    assert.deepEqual(Object.keys(seen[0].line).sort(), ['bytes','job_id','kind','ms','rows','step']);
    assert.equal(seen[0].line.job_id, JOB);
    assert.equal(seen[0].line.kind, kind);
    assert.equal(seen[0].line.step, 'STEP');
    assert.equal(seen[0].line.rows, kind === 'TRANSFER_BUILD' ? '100' : null);
    assert.equal(seen[0].line.bytes, new TextEncoder().encode(JSON.stringify(payload)).byteLength);
    assert.equal(seen[0].busy, false);
    assert.ok(Number.isFinite(seen[0].line.ms) && seen[0].line.ms >= 0);
    assert.equal(JSON.stringify(seen).includes(NONCE), false);
    assert.equal(JSON.stringify(seen).includes(CURSOR), false);
  }
  const seen = [], value = prepareStep('PINNING');
  const dependencies = deps(prepareReceipt('RESERVE'));
  dependencies.onStepMeasurement = line => seen.push(line);
  assert.equal((await stepBpayNextFinancialJob(value, dependencies)).ok, true);
  assert.equal(seen[0].step, 'STEP:PINNING');
});

test('PF06 exact Pay Query contention is labeled without changing errors or observer failure disposition', async () => {
  for (const message of ['WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY', 'SOME_OTHER_LOCK_BUSY']) {
    const seen = [];
    const dependencies = { financialJobRpc: async () => Response.json({code:'55P03',message}, {status:409}),
      onStepMeasurement: (line, busy) => { seen.push({line,busy}); throw Error('observer failed'); } };
    const outcome = await stepBpayNextFinancialJob(input(), dependencies);
    assert.equal(outcome.error_code, 'BPAY_NEXT_JOB_TRANSACTION_RETRY');
    assert.equal(outcome.recovery, 'RETRY_CALL');
    assert.equal(outcome.outcome_unknown, false);
    assert.equal(seen.length, 1);
    assert.equal(seen[0].busy, message === 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY');
    assert.equal(seen[0].line.rows, null);
  }
  const dependencies = deps();
  dependencies.onStepMeasurement = () => { throw Error('logging failed'); };
  assert.equal((await stepBpayNextFinancialJob(input(), dependencies)).ok, true);
});

test('case NET uses one bounded page and exposes a projection only at completion', async () => {
  const call = { ...input('PAYE_NET_ENTRY'), expected_phase: 'FINANCE_ALLOCATE', expected_cursor: null, limit: 17 };
  const unfinished = { ...result('PAYE_NET_ENTRY'), phase: 'FINANCE_ALLOCATE', done: false,
    lease_until_utc: UNTIL, projection_id: null, cursor: '1', rows_visited: '17', run_worker_id: OTHER, issue_code: null };
  for (const payload of [unfinished,
    { ...unfinished, phase: 'PROJECTED', done: true, lease_until_utc: null, projection_id: EFFECT, cursor: null },
    { ...unfinished, phase: 'REVIEW', done: true, lease_until_utc: null, cursor: null, issue_code: 'CASE_WEEK_BASIS_UNBOUND' }
  ]) {
    const dependencies = deps(payload);
    assert.equal((await stepBpayNextFinancialJob(call, dependencies)).ok, true);
    assert.equal(dependencies.calls.length, 1);
    assert.equal(dependencies.calls[0].args.limit, 17);
  }
  for (const payload of [{ ...unfinished, projection_id: EFFECT }, { ...unfinished, rows_visited: '18' },
    { ...unfinished, issue_code: 'CASE_INPUT_UNBOUND' }, { ...unfinished, cursor: null },
    { ...unfinished, replay: true }, { ...unfinished, rows_visited: 17 }]) {
    await failed(call, deps(payload), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  }
  const dependencies = deps(unfinished);
  await failed({ ...call, expected_phase: 'NEW' }, dependencies, 'BPAY_NEXT_JOB_REQUEST_INVALID');
  assert.equal(dependencies.calls.length, 0);
});
test('reissue cancel terminal receipt never implies releasing original payroll or a later cash hold',async()=>{
  const call=input('REISSUE_CANCEL');
  const retained={...result('REISSUE_CANCEL'),phase:'BLOCKED',
    blocked_code:'BPAY_NEXT_REISSUE_CANCEL_INSTRUCTION_PROTECTED',replay:true};
  const dependencies=deps(retained);
  const output=await stepBpayNextFinancialJob(call,dependencies);
  assert.equal(output.ok,true);assert.deepEqual(output.result,retained);assert.equal(dependencies.calls.length,1);
  for(const bad of [{...retained,blocked_code:null},{...retained,phase:'CANCELLED'},
    {...retained,done:false,lease_until_utc:UNTIL},{...retained,amount_released:'180.00'}]) {
    const output=await stepBpayNextFinancialJob(call,deps(bad));
    assert.equal(output.error_code,'BPAY_NEXT_JOB_RESPONSE_INVALID');assert.equal(output.outcome_unknown,true);
  }
  const unused=deps(retained);
  assert.equal((await stepBpayNextFinancialJob({...call,amount:'10.00'},unused)).error_code,'BPAY_NEXT_JOB_REQUEST_INVALID');
  assert.equal(unused.calls.length,0);
});

function input(kind = 'TRANSFER_BUILD', action = 'STEP') {
  const base = { action, job_id: JOB, job_kind: kind };
  return action === 'CLAIM' ? { ...base, lease_seconds: 120 }
    : { ...base, lease_nonce: NONCE, owner_epoch: EPOCH,
      ...(PAGED.includes(kind) ? { expected_cursor: null, limit: 100 } : {}) };
}
function result(kind = 'TRANSFER_BUILD', action = 'STEP') {
  if (action === 'CLAIM') return { job_id: JOB, job_kind: kind,
    phase: ['PAYE_NET_ENTRY','SIMPLE_CANCEL','REISSUE_CANCEL','CASE_CREATE'].includes(kind) ? 'NEW' : kind === 'TRANSFER_BUILD' ? 'WORK' : 'MEMBERS',
    lease_nonce: NONCE, owner_epoch: EPOCH, lease_until_utc: UNTIL, cursor: null };
  const base = { job_id: JOB, job_kind: kind, owner_epoch: EPOCH,
    lease_until_utc: PAGED.includes(kind) ? UNTIL : null, done: !PAGED.includes(kind), replay: false };
  switch (kind) {
    case 'PAYE_NET_ENTRY': return { ...base, phase: 'PROJECTED', projection_id: EFFECT };
    case 'TRANSFER_BUILD': return { ...base, phase: 'BUILDING', transfer_id: EFFECT, transfer_status: 'BUILDING', cursor: CURSOR, member_count: CURSOR, rows_visited: '100' };
    case 'INTERNAL_SETTLEMENT':
    case 'CSV_SETTLEMENT': return { ...base, phase: 'MEMBERS', cursor: CURSOR, posted_members: CURSOR, rows_visited: '100' };
    case 'CSV_RETURN': return { ...base, phase: 'COMPLETE', return_cash_id: EFFECT };
    case 'CASH_REISSUE': return { ...base, phase: 'COMPLETE', transfer_id: EFFECT };
    case 'CASE_CREATE': return { ...base, phase: 'CASE_CREATED', case_id: EFFECT };
    case 'REISSUE_CANCEL': return { ...base, phase: 'CANCELLED', transfer_id: EFFECT, blocked_code: null };
    default: return { ...base, phase: 'CANCELLING', run_worker_id: EFFECT, cursor: CURSOR, released_line_count: CURSOR, rows_visited: '100', holds_released: '99', blocked_code: null };
  }
}
function deps(payload = result()) {
  const calls = [];
  return { calls, async financialJobRpc(action, jobId, jobKind, args, options) {
    calls.push({ action, jobId, jobKind, args, options });
    return Response.json(payload);
  } };
}
async function failed(value, dependencies, code, unknown = false, recovery = 'NONE') {
  const answer = await stepBpayNextFinancialJob(value, dependencies);
  assert.equal(answer.ok, false);
  assert.equal(answer.error_code, code);
  assert.equal(answer.outcome_unknown, unknown);
  assert.equal(answer.recovery, recovery);
  assert.deepEqual(Object.keys(answer).sort(), ['error_code', 'ok', 'outcome_unknown', 'recovery', 'retryable']);
  return answer;
}

for (const kind of KINDS) {
  for (const action of ['CLAIM', 'STEP']) {
    test(`${action} ${kind} performs exactly one closed raw call`, async () => {
      const payload = result(kind, action);
      const dependencies = deps(payload);
      const value = input(kind, action);
      assert.deepEqual(await stepBpayNextFinancialJob(value, dependencies), { ok: true, action, job_kind: kind, result: payload });
      assert.equal(dependencies.calls.length, 1);
      const call = dependencies.calls[0];
      assert.equal(call.action, action);
      assert.equal(call.jobId, JOB);
      assert.equal(call.jobKind, kind);
      const { action: unusedAction, job_id: unusedId, job_kind: unusedKind, ...args } = value;
      assert.deepEqual(call.args, args);
      assert.equal(Object.isFrozen(call.args), true);
      assert.ok(call.options.signal instanceof AbortSignal);
      assert.equal(call.options.timeoutMs, 8000);
      assert.equal(call.options.maxResponseBytes, 128 * 1024);
      assert.equal(call.options.routeClass, 'OPERATION_NUDGE');
      assert.deepEqual(Object.keys(call.options).sort(), ['maxResponseBytes', 'routeClass', 'signal', 'timeoutMs']);
    });
  }
}

test('opaque lease epoch/cursors/counts retain all bigint digits, including reclamation checkpoint', async () => {
  const payload = { ...result('SIMPLE_CANCEL', 'CLAIM'), phase: 'RELEASE', cursor: CURSOR, owner_epoch: '9223372036854775807' };
  const claimed = await stepBpayNextFinancialJob(input('SIMPLE_CANCEL', 'CLAIM'), deps(payload));
  assert.deepEqual(claimed.result, payload);
  const dependencies = deps({ ...result(), owner_epoch: '9223372036854775807', rows_visited: '1' });
  assert.equal((await stepBpayNextFinancialJob({ ...input(), owner_epoch: '9223372036854775807', expected_cursor: CURSOR, limit: 1 }, dependencies)).ok, true);
  assert.equal(dependencies.calls[0].args.expected_cursor, CURSOR);
  assert.equal(dependencies.calls[0].args.owner_epoch, '9223372036854775807');
});

test('CASE_CREATE is one terminal lease-only owner call with retained DONE replay, not a case builder input', async () => {
  const value = input('CASE_CREATE');
  for (const replay of [false, true]) {
    const payload = { ...result('CASE_CREATE'), replay };
    const dependencies = deps(payload);
    assert.deepEqual(await stepBpayNextFinancialJob({ ...value, owner_epoch: replay ? '1' : EPOCH }, dependencies),
      { ok: true, action: 'STEP', job_kind: 'CASE_CREATE', result: payload });
    assert.equal(dependencies.calls.length, 1);
    assert.deepEqual(dependencies.calls[0].args, { lease_nonce: NONCE, owner_epoch: replay ? '1' : EPOCH });
  }
  for (const extra of [{ expected_phase: 'NEW' }, { expected_cursor: null }, { limit: 1 },
    { case_id: EFFECT }, { actor_user_id: OTHER }, { principal: '10.00' }, { candidate_id: OTHER }]) {
    const dependencies = deps(result('CASE_CREATE'));
    await failed({ ...value, ...extra }, dependencies, 'BPAY_NEXT_JOB_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
  for (const payload of [{ ...result('CASE_CREATE'), phase: 'DONE' },
    { ...result('CASE_CREATE'), done: false, lease_until_utc: UNTIL },
    { ...result('CASE_CREATE'), case_id: null }, { ...result('CASE_CREATE'), case_id: 'not-uuid' },
    { ...result('CASE_CREATE'), approved_principal: '10.00' }, { ...result('CASE_CREATE'), cursor: null },
    { ...result('CASE_CREATE'), replay: 'true' }]) {
    await failed(value, deps(payload), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  }
  for (const payload of [{ ...result('CASE_CREATE', 'CLAIM'), phase: 'DONE' },
    { ...result('CASE_CREATE', 'CLAIM'), cursor: '1' }]) {
    await failed(input('CASE_CREATE', 'CLAIM'), deps(payload), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'WAIT_FOR_LEASE_EXPIRY');
  }
});

test('CASE_CREATE lost CLAIM and STEP use exact established recovery; stale lease remains a definite refusal', async () => {
  for (const action of ['CLAIM', 'STEP']) {
    let calls = 0;
    await failed(input('CASE_CREATE', action), { async financialJobRpc() { calls++; throw new Error('lost committed reply'); } },
      'BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', true, action === 'CLAIM' ? 'WAIT_FOR_LEASE_EXPIRY' : 'RETRY_SAME_STEP');
    assert.equal(calls, 1);
  }
  const value = await failed(input('CASE_CREATE'), { async financialJobRpc() {
    return Response.json({ code: '55000', message: 'BPAY_NEXT_CASE_CREATE_LEASE_STALE' }, { status: 400 });
  } }, 'BPAY_NEXT_JOB_NOT_ELIGIBLE');
  assert.equal(value.retryable, false);
});

test('CASE_CREATE SQL dispatch preserves exact request/phase race gates and terminal owner evidence', () => {
  const sql = readFileSync(new URL('../supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql', import.meta.url), 'utf8');
  assert.match(sql, /when 'CASE_CREATE' then\s+select c\.\* into strict v_claim from private\.bpay_next_claim_case_create_job_v1\(p_job_id,v_lease_seconds\) c/);
  assert.match(sql, /BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE/);
  assert.match(sql, /r\.command_id=j\.command_id and r\.candidate_id=j\.candidate_id\) as case_create_request_present/);
  assert.match(sql, /when 'CASE_CREATE' then array\['NEW'\]/);
  assert.match(sql, /p_job_kind='CASE_CREATE' and \(not v_claim_state\.case_create_request_present[\s\S]*?v_claim_state\.cursor_key is not null/);
  assert.ok(sql.indexOf('not v_claim_state.case_create_request_present') < sql.indexOf("message='BPAY_NEXT_JOB_CLAIM_RETRY'"));
  assert.match(sql, /v_result:=private\.bpay_next_apply_case_create_v1\(p_job_id,v_nonce,v_owner_epoch\)/);
  assert.match(sql, /p_job_kind='CASE_CREATE' and \(v_job\.status<>'DONE' or v_job\.phase<>'DONE'[\s\S]*?'CASE_CREATED'[\s\S]*?r\.case_id::text=v_result->>'case_id'/);
  assert.equal((sql.match(/private\.bpay_next_apply_case_create_v1\(/g) ?? []).length, 1);
  assert.doesNotMatch(sql.replace(/--[^\n]*/g, ''), /bpay_next_accept_case_create|insert into|update private\.bpay_next_finance_case|grant[^;]*private\./);
});

test('closed input rejects Office actors, arbitrary functions, money, enrollment and unregistered job kinds', async () => {
  for (const value of [null, [], {}, input('PREPARE_RUN'), input('POSITION_REBUILD'), input('CSV_ISSUE'),
    { ...input(), action: 'DRAIN' }, { ...input(), job_id: 'not-uuid' },
    { ...input(), actor_user_id: OTHER }, { ...input(), function: 'anything' },
    { ...input(), amount: '180.00' }, { ...input(), command_id: OTHER },
    { ...input(), candidate_id: OTHER }, { ...input(), expected_line_count: '99' },
    { ...input(), lease_seconds: 120 }, { ...input('CSV_RETURN'), limit: 100 },
    { ...input('PAYE_NET_ENTRY'), expected_cursor: null },
    { ...input('TRANSFER_BUILD', 'CLAIM'), lease_nonce: NONCE },
    { ...input('TRANSFER_BUILD', 'CLAIM'), owner_epoch: EPOCH }]) {
    const dependencies = deps();
    await failed(value, dependencies, 'BPAY_NEXT_JOB_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('only bounded integer claim duration and page size are accepted', async () => {
  for (const seconds of [0, 121, -1, 1.5, '120', null, Infinity]) {
    const dependencies = deps();
    await failed({ ...input('TRANSFER_BUILD', 'CLAIM'), lease_seconds: seconds }, dependencies, 'BPAY_NEXT_JOB_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
  for (const limit of [0, 101, -1, 1.5, '100', null, Infinity]) {
    await failed({ ...input(), limit }, deps(), 'BPAY_NEXT_JOB_REQUEST_INVALID');
  }
  assert.equal((await stepBpayNextFinancialJob({ ...input('TRANSFER_BUILD', 'CLAIM'), lease_seconds: 1 }, deps(result('TRANSFER_BUILD', 'CLAIM')))).ok, true);
});

test('lease nonce/epoch and page cursor are exact typed inputs, not JS numbers or exponent strings', async () => {
  for (const epoch of [1, '0', '-1', '01', '1e3', '1.0', '9223372036854775808', null]) {
    await failed({ ...input(), owner_epoch: epoch }, deps(), 'BPAY_NEXT_JOB_REQUEST_INVALID');
  }
  for (const cursor of [1, '0', '-1', '01', '1e3', '9223372036854775808', undefined]) {
    await failed({ ...input(), expected_cursor: cursor }, deps(), 'BPAY_NEXT_JOB_REQUEST_INVALID');
  }
  await failed({ ...input(), lease_nonce: null }, deps(), 'BPAY_NEXT_JOB_REQUEST_INVALID');
});

test('missing fixed raw dependency is definite and performs no mutation', async () => {
  const value = await failed(input(), {}, 'BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', false, 'RETRY_CALL');
  assert.equal(value.retryable, true);
});

test('closed typed response rejects arbitrary result trees, precision loss and wrong scope', async () => {
  for (const payload of [null, [], {}, { ...result(), job_id: OTHER },
    { ...result(), job_kind: 'CSV_SETTLEMENT' }, { ...result(), candidate: {} },
    { ...result(), cash_amount: '180.00' }, { ...result(), lease_nonce: NONCE },
    { ...result(), owner_epoch: 9007199254740993 }, { ...result(), cursor: 9007199254740995 },
    { ...result(), member_count: 3 }, { ...result(), rows_visited: 100 },
    { ...result(), member_count: '9223372036854775808' }, { ...result(), rows_visited: '101' },
    { ...result(), owner_epoch: '1' }, { ...result(), phase: 'COMPLETE' },
    { ...result(), done: true }, { ...result(), lease_until_utc: null },
    { ...result(), lease_until_utc: '2026-02-30T00:00:00Z' }]) {
    await failed(input(), deps(payload), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  }
});

test('claim response rejects incompatible phase/current-token adoption shapes', async () => {
  for (const payload of [{ ...result('PAYE_NET_ENTRY', 'CLAIM'), phase: 'MEMBERS' },
    { ...result('PAYE_NET_ENTRY', 'CLAIM'), cursor: '1' },
    { ...result('PAYE_NET_ENTRY', 'CLAIM'), replay: true },
    { ...result('PAYE_NET_ENTRY', 'CLAIM'), lease_until_utc: null },
    { ...result('PAYE_NET_ENTRY', 'CLAIM'), owner_epoch: '0' }]) {
    await failed(input('PAYE_NET_ENTRY', 'CLAIM'), deps(payload), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'WAIT_FOR_LEASE_EXPIRY');
  }
});

test('stale cursor replay reports retained checkpoint with zero work, never starts a second page', async () => {
  for (const kind of PAGED) {
    const payload = { ...result(kind), replay: true, rows_visited: '0', ...(kind === 'SIMPLE_CANCEL' ? { holds_released: '0' } : {}) };
    const dependencies = deps(payload);
    const value = await stepBpayNextFinancialJob({ ...input(kind), expected_cursor: '1', limit: 1 }, dependencies);
    assert.equal(value.ok, true);
    assert.equal(value.result.cursor, CURSOR);
    assert.equal(dependencies.calls.length, 1);
  }
  await failed(input(), deps({ ...result(), replay: true }), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
});

test('DONE STEP replay retains original result even after instruction/cancellation and lease clearing', async () => {
  const original = { ...input(), owner_epoch: '1', expected_cursor: '1' };
  const payload = { ...result(), phase: 'MEMBERS_READY', transfer_status: 'CANCELLED',
    done: true, lease_until_utc: null, replay: true, rows_visited: '0' };
  const dependencies = deps(payload);
  assert.equal((await stepBpayNextFinancialJob(original, dependencies)).ok, true);
  assert.deepEqual(dependencies.calls[0].args, { lease_nonce: NONCE, owner_epoch: '1', expected_cursor: '1', limit: 100 });
  for (const kind of ['PAYE_NET_ENTRY', 'CSV_RETURN', 'CASH_REISSUE']) {
    assert.equal((await stepBpayNextFinancialJob({ ...input(kind), owner_epoch: '1' }, deps({ ...result(kind), replay: true }))).ok, true);
  }
});

test('completed settlement and blocked/cancelled results are explicit terminal progress', async () => {
  assert.equal((await stepBpayNextFinancialJob(input('CSV_SETTLEMENT'), deps({ ...result('CSV_SETTLEMENT'), phase: 'COMPLETE', done: true, lease_until_utc: null }))).ok, true);
  for (const phase of ['CANCELLED', 'BLOCKED']) {
    const payload = { ...result('SIMPLE_CANCEL'), phase, done: true, lease_until_utc: null,
      rows_visited: '0', holds_released: '0', blocked_code: phase === 'BLOCKED' ? 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED' : null };
    assert.equal((await stepBpayNextFinancialJob(input('SIMPLE_CANCEL'), deps(payload))).ok, true);
  }
  await failed(input('SIMPLE_CANCEL'), deps({ ...result('SIMPLE_CANCEL'), blocked_code: 'BPAY_NEXT_WRONG' }), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  await failed(input('SIMPLE_CANCEL'), deps({ ...result('SIMPLE_CANCEL'), holds_released: '100', rows_visited: '99' }), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  await failed({ ...input(), limit: 1 }, deps(result()), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
});

test('group cancellation precheck remains REQUESTED with exact cursor and no released money', async () => {
  const call = { ...input('SIMPLE_CANCEL'), limit: 1 };
  const checking = { ...result('SIMPLE_CANCEL'), phase: 'REQUESTED', cursor: '1',
    released_line_count: '0', rows_visited: '1', holds_released: '0' };
  const dependencies = deps(checking);
  assert.deepEqual(await stepBpayNextFinancialJob(call, dependencies), {
    ok: true, action: 'STEP', job_kind: 'SIMPLE_CANCEL', result: checking
  });
  assert.equal(dependencies.calls.length, 1);
  assert.deepEqual(dependencies.calls[0].args, {
    lease_nonce: NONCE, owner_epoch: EPOCH, expected_cursor: null, limit: 1
  });
  const replay = { ...checking, replay: true, rows_visited: '0' };
  assert.equal((await stepBpayNextFinancialJob(call, deps(replay))).ok, true);
  for (const bad of [{ ...checking, released_line_count: '1' }, { ...checking, holds_released: '1' },
    { ...checking, done: true, lease_until_utc: null }, { ...checking, blocked_code: 'BPAY_NEXT_CANCEL_TRANSFER_PROTECTED' },
    { ...checking, rows_visited: '2' }, { ...checking, phase: 'CHECKING' }]) {
    await failed(call, deps(bad), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  }
});

test('SQL transaction errors distinguish busy CLAIM, terminal CLAIM, scope and retryable transaction aborts', async () => {
  for (const [code, message, expected, retryable, recovery] of [
    ['42501', 'secret actor', 'BPAY_NEXT_JOB_FORBIDDEN', false, 'NONE'],
    ['P0002', 'secret job', 'BPAY_NEXT_JOB_NOT_FOUND', false, 'NONE'],
    ['23514', 'BPAY_NEXT_JOB_KIND_MISMATCH', 'BPAY_NEXT_JOB_KIND_MISMATCH', false, 'NONE'],
    ['23514', 'secret scope', 'BPAY_NEXT_JOB_CONFLICT', false, 'NONE'],
    ['22023', 'secret input', 'BPAY_NEXT_JOB_REQUEST_INVALID', false, 'NONE'],
    ['55000', 'BPAY_NEXT_JOB_LEASE_BUSY', 'BPAY_NEXT_JOB_LEASE_BUSY', true, 'WAIT_FOR_LEASE_EXPIRY'],
    ['55000', 'BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR', 'BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR', true, 'WAIT_FOR_PREDECESSOR'],
    ['55000', 'BPAY_NEXT_JOB_CLAIM_RETRY', 'BPAY_NEXT_JOB_CLAIM_RETRY', true, 'RETRY_CALL'],
    ['55000', 'BPAY_NEXT_JOB_TERMINAL', 'BPAY_NEXT_JOB_TERMINAL', false, 'NONE'],
    ['55000', 'BPAY_NEXT_MODULE_NOT_ACTIVE', 'BPAY_NEXT_JOB_MODULE_NOT_ACTIVE', false, 'NONE'],
    ['55000', 'BPAY_NEXT_CANCEL_PAGE_LEASE_INVALID', 'BPAY_NEXT_JOB_NOT_ELIGIBLE', false, 'NONE'],
    ['40001', 'secret serialization', 'BPAY_NEXT_JOB_TRANSACTION_RETRY', true, 'RETRY_CALL'],
    ['40P01', 'secret deadlock', 'BPAY_NEXT_JOB_TRANSACTION_RETRY', true, 'RETRY_CALL'],
    ['57014', 'secret cancellation', 'BPAY_NEXT_JOB_TRANSACTION_RETRY', true, 'RETRY_CALL'],
    ['PGRST202', 'secret cache', 'BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', true, 'RETRY_CALL'],
    ['54000', 'BPAY_NEXT_JOB_RESPONSE_INVALID', 'BPAY_NEXT_JOB_RESPONSE_INVALID', false, 'NONE']
  ]) {
    const dependencies = { async financialJobRpc() { return Response.json({ code, message, details: 'secret' }, { status: 400 }); } };
    const value = await failed(input('TRANSFER_BUILD', 'CLAIM'), dependencies, expected, false, recovery);
    assert.equal(value.retryable, retryable);
    assert.equal(JSON.stringify(value).includes('secret'), false);
  }
});

test('racing CLAIM loser and ordered predecessor deferral remain retryable without adopting/repeating', async () => {
  for (const kind of KINDS) {
    for (const [message, recovery] of [['BPAY_NEXT_JOB_LEASE_BUSY', 'WAIT_FOR_LEASE_EXPIRY'],
      ['BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR', 'WAIT_FOR_PREDECESSOR'],
      ['BPAY_NEXT_JOB_CLAIM_RETRY', 'RETRY_CALL']]) {
      const dependencies = deps();
      dependencies.financialJobRpc = async (action, jobId, jobKind, args, options) => {
        dependencies.calls.push({ action, jobId, jobKind, args, options });
        return Response.json({ code: '55000', message }, { status: 400 });
      };
      const value = await failed(input(kind, 'CLAIM'), dependencies, message, false, recovery);
      assert.equal(value.retryable, true);
      assert.equal(dependencies.calls.length, 1);
      assert.deepEqual(dependencies.calls[0].args, { lease_seconds: 120 });
      assert.equal(Object.hasOwn(value, 'lease_nonce'), false);
    }
  }
});

test('unknown transport outcome separates lost CLAIM from exact STEP retry and never retries itself', async () => {
  for (const action of ['CLAIM', 'STEP']) {
    let calls = 0;
    const dependencies = { async financialJobRpc() { calls++; throw new Error('secret transport URL'); } };
    const answer = await failed(input('TRANSFER_BUILD', action), dependencies, 'BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', true,
      action === 'CLAIM' ? 'WAIT_FOR_LEASE_EXPIRY' : 'RETRY_SAME_STEP');
    assert.equal(answer.retryable, true);
    assert.equal(calls, 1);
    assert.equal(JSON.stringify(answer).includes('secret'), false);
  }
});

test('unclassified permanent owner claim failures do not become generic retries', async () => {
  for (const message of ['BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE', 'BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE',
    'BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE', 'BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE', 'BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE', 'BPAY_NEXT_OTHER_FAILURE']) {
    let calls = 0;
    const answer = await failed(input('SIMPLE_CANCEL', 'CLAIM'), { async financialJobRpc() {
      calls++;
      return Response.json({ code: '55000', message }, { status: 400 });
    } }, 'BPAY_NEXT_JOB_NOT_ELIGIBLE');
    assert.equal(answer.retryable, false);
    assert.equal(calls, 1);
  }
});

test('raw transport required; malformed JSON and invalid UTF-8 fail with unknown STEP outcome', async () => {
  for (const response of [result(), new Response('not JSON'), new Response(new Uint8Array([0xc3, 0x28])), new Response(null)]) {
    await failed(input(), { async financialJobRpc() { return response; } }, 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  }
});

test('128KiB response cap applies before parsing even with no/lying/oversized headers', async () => {
  for (const headers of [{}, { 'content-length': '1' }, { 'content-length': '999999' }]) {
    let cancelled = false;
    const response = new Response(new ReadableStream({
      start(controller) { controller.enqueue(new Uint8Array(BPAY_NEXT_JOB_LIMITS.responseBytes + 1)); },
      cancel() { cancelled = true; }
    }), { headers });
    await failed(input(), { async financialJobRpc() { return response; } }, 'BPAY_NEXT_JOB_RESPONSE_TOO_LARGE', true, 'RETRY_SAME_STEP');
    assert.equal(cancelled, true);
  }
});

test('8s deadline aborts and cancels stalled response stream, no automatic repeated STEP', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let signal, cancelled = false, calls = 0;
  const pending = stepBpayNextFinancialJob(input(), { async financialJobRpc(action, job, kind, args, options) {
    calls++;
    signal = options.signal;
    return new Response(new ReadableStream({ cancel() { cancelled = true; } }));
  } });
  for (let turn = 0; turn < 10 && !cancelled; turn++) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(signal);
  t.mock.timers.tick(8000);
  assert.deepEqual(await pending, { ok: false, error_code: 'BPAY_NEXT_JOB_DEPENDENCY_TIMEOUT', retryable: true, outcome_unknown: true, recovery: 'RETRY_SAME_STEP' });
  assert.equal(signal.aborted, true);
  assert.equal(cancelled, true);
  assert.equal(calls, 1);
});

test('late raw CLAIM response is cancelled without learning/adopting its token', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let deliver, signal, cancelled = false, calls = 0;
  const pending = stepBpayNextFinancialJob(input('TRANSFER_BUILD', 'CLAIM'), { financialJobRpc(action, job, kind, args, options) {
    signal = options.signal;
    calls++;
    return new Promise((resolve) => { deliver = resolve; });
  } });
  t.mock.timers.tick(8000);
  const answer = await pending;
  assert.equal(answer.recovery, 'WAIT_FOR_LEASE_EXPIRY');
  assert.equal(answer.outcome_unknown, true);
  deliver(new Response(new ReadableStream({ cancel() { cancelled = true; } })));
  for (let turn = 0; turn < 10 && !cancelled; turn++) await new Promise((resolve) => setImmediate(resolve));
  assert.equal(signal.aborted, true);
  assert.equal(cancelled, true);
  assert.equal(calls, 1);
  assert.equal(Object.hasOwn(answer, 'lease_nonce'), false);
});

test('public SQL service boundary retains owner dispatch, decimal text and lock order', () => {
  // Source-only proof; parent task owns actual PostgreSQL first-use/API proof.
  const sql = readFileSync(new URL('../supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql', import.meta.url), 'utf8');
  assert.match(sql, /public\.bpay_next_financial_job_v1\(\s*p_action text,p_job_id uuid,p_job_kind text,p_args jsonb/);
  assert.match(sql, /request\.jwt\.claim\.role/);
  assert.match(sql, /active_owner='NEXT' for share/);
  assert.match(sql, /v_job\.job_kind<>p_job_kind/);
  assert.ok(sql.indexOf('BPAY_NEXT_JOB_KIND_MISMATCH') < sql.indexOf('select c.* into strict v_claim'));
  assert.match(sql, /v_job\.status='DONE'[\s\S]*BPAY_NEXT_JOB_TERMINAL/);
  assert.match(sql, /v_job\.status='LEASED'[\s\S]*BPAY_NEXT_JOB_LEASE_BUSY/);
  const catchStart = sql.indexOf("exception when sqlstate '55000' then");
  const catchEnd = sql.indexOf('-- Only the owner-returned newly acquired token', catchStart);
  assert.ok(catchStart > sql.indexOf('select c.* into strict v_claim'));
  const claimCatch = sql.slice(catchStart, catchEnd);
  for (const message of ['BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE', 'BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE',
    'BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE', 'BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE']) assert.ok(claimCatch.includes(message));
  assert.match(claimCatch, /select j\.\*,[\s\S]*into strict v_claim_state[\s\S]*v_claim_state\.module_epoch is distinct from/);
  assert.match(claimCatch, /earlier\.candidate_id=j\.candidate_id[\s\S]*earlier\.command_sequence<j\.command_sequence[\s\S]*earlier\.status<>'DONE'/);
  assert.match(claimCatch, /v_claim_state\.phase=any\(case p_job_kind[\s\S]*v_claim_state\.available_at_utc>[\s\S]*not v_claim_state\.cancel_request_pending/);
  assert.match(claimCatch, /r\.command_id=j\.command_id and r\.candidate_id=j\.candidate_id[\s\S]*r\.status in \('REQUESTED','CANCELLING'\)/);
  assert.match(claimCatch, /BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR/);
  assert.match(claimCatch, /not v_claim_state\.predecessor_pending[\s\S]*v_claim_state\.lease_until_utc<=[\s\S]*BPAY_NEXT_JOB_CLAIM_RETRY/);
  assert.ok(claimCatch.indexOf('v_claim_state.phase=any') < claimCatch.indexOf('BPAY_NEXT_JOB_CLAIM_RETRY'));
  assert.ok(claimCatch.indexOf('not v_claim_state.cancel_request_pending') < claimCatch.indexOf('BPAY_NEXT_JOB_CLAIM_RETRY'));
  assert.doesNotMatch(claimCatch, /when others|bpay_next_claim_simple_|jsonb_build_object|lease_nonce/i);
  assert.match(sql, /'lease_nonce',v_claim\.lease_nonce/);
  assert.doesNotMatch(sql, /'lease_nonce',v_job\.lease_nonce/);
  assert.match(sql, /'owner_epoch',v_claim\.owner_epoch::text/);
  assert.match(sql, /'owner_epoch',v_job\.owner_epoch::text/);
  assert.match(sql, /'member_count',v_result->>'member_count'/);
  assert.match(sql, /'posted_members',v_result->>'posted_members'/);
  assert.match(sql, /'released_line_count',v_result->>'released_line_count'/);
  assert.match(sql, /v_limit not between 1 and 100/);
  assert.match(sql, /from public,anon,authenticated,service_role/);
  assert.match(sql, /grant execute on function public\.bpay_next_financial_job_v1\(text,uuid,text,jsonb\) to service_role/);
  assert.match(sql, /notify pgrst,'reload schema'/);
  for (const owner of ['claim_simple_paye_net_job', 'claim_simple_transfer_job', 'claim_simple_outcome_job', 'claim_simple_cancel_job',
    'apply_simple_paye_net', 'build_simple_transfer_page', 'post_simple_outcome_page', 'post_simple_return', 'build_simple_reissue', 'cancel_simple_worker_page',
    'claim_position_job', 'apply_position_page', 'claim_prepare_job', 'pin_selected_work', 'capture_position_page', 'hold_fresh_position_page', 'complete_no_case_worker']) {
    assert.match(sql, new RegExp(`private\\.bpay_next_${owner}_v1\\(`));
  }
  const executable = sql.replace(/--[^\n]*/g, '');
  assert.doesNotMatch(executable, /\bfor update\b|\bexecute\s+(?:format|v_sql)|return\s+v_result\s*;|pg_catalog\.(coalesce|nullif|greatest|least)\(/i);
  assert.doesNotMatch(executable, /\b(?:create|alter) table\b|grant[^;]*private\.|prepare_run|enroll|csv_instruction|Timesheets/i);
});

test('module is internal single-call code with no Office HTTP route or stateful drain', () => {
  const source = readFileSync(new URL('../broker/src/bpay-next-job-step.js', import.meta.url), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
  assert.doesNotMatch(source, /requireOfficeUser|new Request\(|fetch\(|waitUntil|setInterval|current_timesheet|histor(?:y|ies)/i);
  assert.equal((source.match(/await dependencies\.financialJobRpc\(/g) ?? []).length, 1);
  assert.match(source, /export async function stepBpayNextFinancialJob/);
});

const COMPONENT = 'TS_DAY:2026-10-04:retained component';
function positionStep(phase = 'NEW', cursor = null) {
  return { action: 'STEP', job_id: JOB, job_kind: 'POSITION_APPLY', lease_nonce: NONCE,
    owner_epoch: EPOCH, expected_phase: phase, expected_cursor: cursor, limit: 100 };
}
function positionReceipt(phase = 'NEW', cursor = COMPONENT) {
  return { job_id: JOB, job_kind: 'POSITION_APPLY', owner_epoch: EPOCH,
    lease_until_utc: phase === 'DONE' ? null : UNTIL, done: phase === 'DONE',
    phase, cursor, rows_visited: '0' };
}
function prepareStep(phase = 'NEW') {
  return { action: 'STEP', job_id: JOB, job_kind: 'PREPARE', lease_nonce: NONCE, owner_epoch: EPOCH,
    expected_phase: phase, ...(['NEW', 'PINNING'].includes(phase) ? { expected_cursor: null }
      : ['RESERVE', 'ALLOCATE'].includes(phase) ? { expected_work_cursor: null, expected_component_cursor: null, limit: 100 }
        : phase === 'FINANCE_ALLOCATE' ? { expected_cursor: null, limit: 100 } : {}) };
}
function prepareReceipt(phase = 'PINNING') {
  const done = ['READY', 'REVIEW'].includes(phase);
  return { job_id: JOB, job_kind: 'PREPARE', owner_epoch: EPOCH, lease_until_utc: done ? null : UNTIL,
    done, replay: false, phase, cursor: phase === 'PINNING' ? CURSOR : null,
    work_cursor: null, component_cursor: null, run_worker_id: EFFECT,
    worker_status: phase === 'REVIEW' ? 'REVIEW' : done ? 'READY' : 'PREPARING',
    issue_code: phase === 'REVIEW' ? 'FINANCIAL_RESOLUTION_REQUIRED' : null,
    issue_work_id: phase === 'REVIEW' ? OTHER : null,
    rows_visited: phase === 'PINNING' ? '1' : null };
}
function prepareClaim(phase = 'NEW') {
  return { job_id: JOB, job_kind: 'PREPARE', phase, lease_nonce: NONCE, owner_epoch: EPOCH,
    lease_until_utc: UNTIL, cursor: phase === 'PINNING' ? CURSOR : null,
    work_cursor: ['RESERVE', 'ALLOCATE'].includes(phase) ? OTHER : null,
    component_cursor: ['RESERVE', 'ALLOCATE'].includes(phase) ? COMPONENT : null };
}

test('POSITION CLAIM and each STEP preserve exact text component/phase with no invented replay flag', async () => {
  const claim = { ...prepareClaim(), job_kind: 'POSITION_APPLY', phase: 'REMOVED', cursor: COMPONENT };
  delete claim.work_cursor; delete claim.component_cursor;
  assert.equal((await stepBpayNextFinancialJob(input('POSITION_APPLY', 'CLAIM'), deps(claim))).ok, true);
  for (const phase of ['NEW', 'REMOVED', 'DONE']) {
    const payload = positionReceipt(phase, phase === 'DONE' ? null : COMPONENT);
    const dependencies = deps(payload);
    assert.deepEqual(await stepBpayNextFinancialJob(positionStep(), dependencies),
      { ok: true, action: 'STEP', job_kind: 'POSITION_APPLY', result: payload });
    assert.equal(dependencies.calls.length, 1);
    assert.equal(Object.hasOwn(payload, 'replay'), false);
  }
  await failed(positionStep(), deps({ ...positionReceipt(), replay: true }), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
});

test('component cursor bound is 256 Unicode characters, not bytes/code units or a fabricated key grammar', async () => {
  for (const component of [COMPONENT, 'arbitrary retained key\nwith quotes " and Ω', '😀'.repeat(256)]) {
    const dependencies = deps(positionReceipt('NEW', component));
    assert.equal((await stepBpayNextFinancialJob(positionStep('NEW', component), dependencies)).ok, true);
    assert.equal(dependencies.calls[0].args.expected_cursor, component);
    const prepared = { ...prepareStep('RESERVE'), expected_work_cursor: OTHER, expected_component_cursor: component };
    assert.equal((await stepBpayNextFinancialJob(prepared, deps({ ...prepareReceipt('RESERVE'), work_cursor: OTHER, component_cursor: component }))).ok, true);
  }
  for (const component of ['', '😀'.repeat(257), '\u0000', '\ud800', 1, undefined]) {
    const dependencies = deps();
    await failed({ ...positionStep(), expected_cursor: component }, dependencies, 'BPAY_NEXT_JOB_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('PREPARE CLAIM retains the complete phase-specific checkpoint for all five supported phases', async () => {
  for (const phase of ['NEW', 'PINNING', 'RESERVE', 'ALLOCATE', 'FINANCE_ALLOCATE']) {
    const payload = prepareClaim(phase), dependencies = deps(payload);
    assert.deepEqual(await stepBpayNextFinancialJob(input('PREPARE', 'CLAIM'), dependencies),
      { ok: true, action: 'CLAIM', job_kind: 'PREPARE', result: payload });
    assert.equal(dependencies.calls.length, 1); assert.deepEqual(dependencies.calls[0].args, { lease_seconds: 120 });
  }
});

test('PREPARE STEP dispatch input is one closed phase-specific shape, including pin selection bigint text', async () => {
  const next = { NEW: 'PINNING', PINNING: 'RESERVE', RESERVE: 'ALLOCATE', ALLOCATE: 'FINANCE_ALLOCATE', FINANCE_ALLOCATE: 'READY' };
  for (const phase of Object.keys(next)) {
    const value = prepareStep(phase), payload = prepareReceipt(next[phase]), dependencies = deps(payload);
    if (phase === 'PINNING') value.expected_cursor = CURSOR;
    const answer = await stepBpayNextFinancialJob(value, dependencies);
    assert.equal(answer.ok, true); assert.equal(dependencies.calls.length, 1);
    const { action, job_id, job_kind, ...args } = value;
    assert.deepEqual(dependencies.calls[0].args, args);
    if (phase === 'PINNING') assert.equal(dependencies.calls[0].args.expected_cursor, CURSOR);
  }
});

test('PREPARE forbids partial UUID/component tuples, wrong phase cursors, numeric identity loss and unsupported phase', async () => {
  for (const value of [
    { ...prepareStep('RESERVE'), expected_work_cursor: OTHER },
    { ...prepareStep('RESERVE'), expected_component_cursor: COMPONENT },
    { ...prepareStep('RESERVE'), expected_work_cursor: 'not-uuid', expected_component_cursor: COMPONENT },
    { ...prepareStep('RESERVE'), expected_work_cursor: OTHER, expected_component_cursor: 1 },
    { ...prepareStep('RESERVE'), expected_cursor: CURSOR },
    { ...prepareStep('NEW'), expected_work_cursor: null },
    { ...prepareStep('PINNING'), expected_cursor: 9007199254740995 },
    { ...prepareStep('NEW'), limit: 100 },
    { ...prepareStep('FINANCE_ALLOCATE'), expected_cursor: 1 },
    { ...prepareStep('FINANCE_ALLOCATE'), limit: 101 },
    { ...prepareStep('FINANCE_ALLOCATE'), expected_cursor: '0' },
    { ...prepareStep('FINANCE_ALLOCATE'), case_id: OTHER },
    { ...prepareStep('FINANCE_ALLOCATE'), expected_phase: 'READY' },
    { ...prepareStep('FINANCE_ALLOCATE'), expected_phase: 'CASE_ALLOCATE' },
    { ...positionStep(), expected_phase: 'DONE' }, { ...positionStep(), expected_work_cursor: OTHER }
  ]) {
    const dependencies = deps();
    await failed(value, dependencies, 'BPAY_NEXT_JOB_REQUEST_INVALID');
    assert.equal(dependencies.calls.length, 0);
  }
});

test('PREPARE phase completion is not job DONE; counts remain nullable when exact owner did not report them', async () => {
  for (const phase of ['RESERVE', 'ALLOCATE', 'FINANCE_ALLOCATE']) {
    const payload = prepareReceipt(phase);
    assert.equal((await stepBpayNextFinancialJob(prepareStep('NEW'), deps(payload))).ok, true);
    assert.equal(payload.done, false); assert.equal(payload.rows_visited, null);
    await failed(prepareStep('NEW'), deps({ ...payload, done: true }), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  }
  for (const phase of ['READY', 'REVIEW']) {
    const payload = prepareReceipt(phase);
    assert.equal((await stepBpayNextFinancialJob(prepareStep('RESERVE'), deps(payload))).ok, true);
  }
});

test('changed chosen revision is a definite worker REVIEW, not a stale transport retry or implicit remap', async () => {
  const payload = { ...prepareReceipt('REVIEW'), issue_code: 'SELECTED_WORK_REVISION_CHANGED' };
  const dependencies = deps(payload);
  const answer = await stepBpayNextFinancialJob(prepareStep('NEW'), dependencies);
  assert.equal(answer.ok, true);
  assert.equal(answer.result.phase, 'REVIEW');
  assert.equal(answer.result.issue_work_id, payload.issue_work_id);
  assert.equal(dependencies.calls.length, 1);
  await failed(prepareStep('NEW'), deps({ ...payload, issue_work_id: null }),
    'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
});

test('PREPARE typed responses reject wrong pair/issue/worker state/count and arbitrary captured trees', async () => {
  for (const payload of [
    { ...prepareClaim('RESERVE'), work_cursor: null },
    { ...prepareClaim('PINNING'), work_cursor: OTHER, component_cursor: COMPONENT },
    { ...prepareClaim('FINANCE_ALLOCATE'), work_cursor: OTHER, component_cursor: COMPONENT }
  ]) await failed(input('PREPARE', 'CLAIM'), deps(payload), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'WAIT_FOR_LEASE_EXPIRY');
  for (const payload of [
    { ...prepareReceipt('RESERVE'), work_cursor: OTHER },
    { ...prepareReceipt('PINNING'), cursor: 9007199254740995 },
    { ...prepareReceipt('PINNING'), rows_visited: '2' },
    { ...prepareReceipt('PINNING'), rows_visited: '1', replay: true },
    { ...prepareReceipt('PINNING'), worker_status: 'READY' },
    { ...prepareReceipt('READY'), issue_code: 'FINANCIAL_RESOLUTION_REQUIRED' },
    { ...prepareReceipt('REVIEW'), issue_work_id: null },
    { ...prepareReceipt('REVIEW'), issue_code: 'UNKNOWN_CASE_ISSUE' },
    { ...prepareReceipt('READY'), worker_status: 'REVIEW' },
    { ...prepareReceipt('READY'), phase: 'CASE_ALLOCATE' },
    { ...prepareReceipt('READY'), captured_lines: [] }
  ]) await failed(prepareStep('NEW'), deps(payload), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
});

test('a retained unsupported-owner response is a definite refusal, never fake READY or repeated execution', async () => {
  const dependencies = deps();
  dependencies.financialJobRpc = async (...args) => {
    dependencies.calls.push(args);
    return Response.json({ code: '55000', message: 'BPAY_NEXT_CASE_ALLOCATION_REQUIRED' }, { status: 400 });
  };
  const answer = await failed(prepareStep('FINANCE_ALLOCATE'), dependencies, 'BPAY_NEXT_JOB_CASE_ALLOCATION_UNSUPPORTED');
  assert.equal(answer.retryable, false); assert.equal(dependencies.calls.length, 1);
});

test('POSITION and PREPARE completed old STEP replay uses exact original evidence, even after Candidate cancellation', async () => {
  assert.equal((await stepBpayNextFinancialJob({ ...positionStep(), owner_epoch: '1' },
    deps(positionReceipt('DONE', null)))).ok, true);
  for (const phase of ['READY', 'REVIEW']) {
    const payload = { ...prepareReceipt(phase), replay: true, rows_visited: '0',
      ...(phase === 'READY' ? { worker_status: 'CANCELLED' } : {}) };
    assert.equal((await stepBpayNextFinancialJob({ ...prepareStep('RESERVE'), owner_epoch: '1' }, deps(payload))).ok, true);
  }
});

test('new kind unknown CLAIM/STEP transport outcomes retain existing exact recovery and one call', async () => {
  for (const kind of ['POSITION_APPLY', 'PREPARE']) for (const action of ['CLAIM', 'STEP']) {
    const value = action === 'CLAIM' ? input(kind, action) : kind === 'PREPARE' ? prepareStep('RESERVE') : positionStep();
    let calls = 0;
    const answer = await failed(value, { async financialJobRpc() { calls++; throw new Error('lost reply'); } },
      'BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', true, action === 'CLAIM' ? 'WAIT_FOR_LEASE_EXPIRY' : 'RETRY_SAME_STEP');
    assert.equal(answer.retryable, true); assert.equal(calls, 1);
  }
});

test('new SQL phase routes are finite, owner-authoritative and case terminal replay uses retained state', () => {
  const sql = readFileSync(new URL('../supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql', import.meta.url), 'utf8');
  assert.match(sql, /pg_catalog\.char_length\(p_args->>'expected_component_cursor'\) not between 1 and 256/);
  assert.match(sql, /v_job\.status='DONE' and v_job\.phase='READY'[\s\S]*private\.bpay_next_complete_no_case_worker_v1/);
  assert.match(sql, /v_job\.status='DONE' and v_job\.phase='DONE'[\s\S]*private\.bpay_next_pin_selected_work_v1/);
  assert.match(sql, /elsif v_phase='FINANCE_ALLOCATE'[\s\S]*private\.bpay_next_prepare_case_page_v1/);
  assert.match(sql, /v_job\.status='DONE' and \(exists\(select 1 from private\.bpay_next_case_allocation_state[\s\S]*private\.bpay_next_prepare_case_page_v1/);
  assert.match(sql, /w\.review_issue_code in \('CASE_SELECTION_REQUIRED'[\s\S]*'CASE_NO_PAYABLE_AMOUNT'/);
  assert.match(sql, /v_job\.status='DONE' and \(exists\([\s\S]*?v_result:=private\.bpay_next_prepare_case_page_v1\(p_job_id,v_nonce,v_owner_epoch,v_cursor,coalesce\(v_limit,1\)\);/);
  assert.match(sql, /p_job_kind<>'POSITION_APPLY' then[\s\S]*'replay',v_result->'replay'/);
  assert.match(sql, /prepare_admissible[\s\S]*p_job_kind='PREPARE' and not v_claim_state\.prepare_admissible/);
  assert.doesNotMatch(sql.replace(/--[^\n]*/g, ''), /bpay_next_case_allocation_row|bpay_next_build_case|\bupdate private\.bpay_next_/);
});

test('case finance pages keep exact bigint text and Candidate issues carry no invented Work identity', async () => {
  const value = { ...prepareStep('FINANCE_ALLOCATE'), expected_cursor: CURSOR, limit: 17 };
  const payload = { ...prepareReceipt('FINANCE_ALLOCATE'), cursor: String(BigInt(CURSOR) + 1n), rows_visited: '17' };
  const dependencies = deps(payload);
  assert.equal((await stepBpayNextFinancialJob(value, dependencies)).ok, true);
  assert.equal(dependencies.calls[0].args.expected_cursor, CURSOR);
  assert.equal((await stepBpayNextFinancialJob(input('PREPARE','CLAIM'), deps({ ...prepareClaim('FINANCE_ALLOCATE'), cursor: CURSOR }))).ok,true);
  for (const issue_code of ['CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
    'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT']) {
    const review = { ...prepareReceipt('REVIEW'), issue_code, issue_work_id: null, rows_visited: '0' };
    assert.equal((await stepBpayNextFinancialJob(value, deps(review))).ok, true);
    await failed(value, deps({ ...review, issue_work_id: OTHER }), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
  }
  await failed(value, deps({ ...payload, rows_visited: '18' }), 'BPAY_NEXT_JOB_RESPONSE_INVALID', true, 'RETRY_SAME_STEP');
});
