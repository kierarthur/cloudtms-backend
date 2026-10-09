import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import {
  BPAY_NEXT_OWNER_STATUS_RPC, BPAY_NEXT_OWNER_STATUS_LIMITS,
  normalizeBpayNextOwnerStatus, createBpayNextOwnerStatusRpc,
  createBpayNextLegacyFence, isBpayNextLegacyFinancialRoute
} from '../broker/src/bpay-next-legacy-fence.js';

// Unsigned, synthetic shape only: never used with any external transport.
const jwt = (claims = { role: 'service_role' }) =>
  `e30.${Buffer.from(JSON.stringify(claims)).toString('base64url')}.c3ludGhldGlj`;
const headers = () => ({ Authorization: `Bearer ${jwt()}` });
const status = (owner = 'LEGACY', epoch = '1') => ({ active_owner: owner, owner_epoch: epoch });
const code = (expected) => (error) => {
  assert.equal(error.code, expected);
  assert.equal(error.message, expected);
  return true;
};

test('owner status accepts only exact finite owners and lossless positive PG bigint strings', () => {
  for (const owner of ['LEGACY', 'NEXT', 'DISABLED']) {
    for (const epoch of ['1', '9007199254740993', '9223372036854775807']) {
      const parsed = normalizeBpayNextOwnerStatus(status(owner, epoch));
      assert.deepEqual(parsed, status(owner, epoch));
      assert.equal(Object.isFrozen(parsed), true);
    }
  }
  for (const value of [null, [], {}, 'LEGACY', [status()],
    { ...status(), extra: true }, status('UNKNOWN'), status('legacy'),
    status('LEGACY', 1), status('LEGACY', null), status('LEGACY', '0'),
    status('LEGACY', '01'), status('LEGACY', '-1'), status('LEGACY', '1.0'),
    status('LEGACY', '9223372036854775808'), status('LEGACY', '1'.repeat(500)),
    Object.create(status()), Object.assign(status(), { [Symbol('extra')]: true })]) {
    assert.throws(() => normalizeBpayNextOwnerStatus(value), code('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'));
  }
});

test('fresh guard never runs financial/provider/queue action for NEXT, DISABLED or unknown status', async () => {
  for (const owner of ['NEXT', 'DISABLED', 'UNKNOWN']) {
    let actions = 0;
    let checks = 0;
    const fence = createBpayNextLegacyFence({ ownerStatusRpc: async () => { checks++; return status(owner); } });
    const expected = owner === 'NEXT' ? 'BPAY_NEXT_LEGACY_WRITER_RETIRED'
      : owner === 'DISABLED' ? 'BPAY_NEXT_LEGACY_WRITER_DISABLED' : 'BPAY_NEXT_OWNER_STATUS_UNAVAILABLE';
    await assert.rejects(fence.runLegacyWriter(async () => { actions++; }), code(expected));
    assert.equal(actions, 0); assert.equal(checks, 1);
  }
  for (const rpc of [async () => { throw new Error('dependency body must not escape'); },
    async () => ({ active_owner: 'LEGACY' }), async () => null]) {
    let externalNetwork = 0;
    await assert.rejects(createBpayNextLegacyFence({ ownerStatusRpc: rpc }).runLegacyWriter(() => {
      externalNetwork++; assert.fail('must not send provider/email/queue request');
    }), code('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'));
    assert.equal(externalNetwork, 0);
  }
});

test('LEGACY allows exactly the callback after fresh check, with no mutable cached owner', async () => {
  let owner = 'LEGACY'; let checks = 0; let actions = 0;
  const fence = createBpayNextLegacyFence({ ownerStatusRpc: async () => { checks++; return status(owner, String(checks)); } });
  assert.equal(await fence.runLegacyWriter((receipt) => {
    actions++; assert.deepEqual(receipt, status('LEGACY', '1')); return 'allowed';
  }), 'allowed');
  owner = 'NEXT';
  await assert.rejects(fence.runLegacyWriter(() => { actions++; }), (error) => {
    assert.equal(error.code, 'BPAY_NEXT_LEGACY_WRITER_RETIRED');
    assert.equal(error.status, 410); assert.equal(error.retryable, false); return true;
  });
  assert.equal(checks, 2); assert.equal(actions, 1);
});

test('business callback errors remain the callback result, not an unknown owner status', async () => {
  const error = new Error('legacy business refusal');
  const fence = createBpayNextLegacyFence({ ownerStatusRpc: async () => status() });
  await assert.rejects(fence.runLegacyWriter(() => { throw error; }), (actual) => actual === error);
  await assert.rejects(fence.runLegacyWriter(null), TypeError);
});

test('fixed raw adapter sends one zero-arg RPC with service JWT and parses exact response', async () => {
  let calls = 0;
  const rpc = createBpayNextOwnerStatusRpc({ baseUrl: 'https://agency-test.invalid/', headers: headers(),
    fetchImpl: async (url, options) => {
      calls++;
      assert.equal(url, `https://agency-test.invalid/rest/v1/rpc/${BPAY_NEXT_OWNER_STATUS_RPC}`);
      assert.equal(options.method, 'POST'); assert.equal(options.body, '{}');
      assert.equal(options.redirect, 'error'); assert.ok(options.signal instanceof AbortSignal);
      assert.equal(options.headers.get('authorization'), headers().Authorization);
      assert.equal(options.headers.get('content-type'), 'application/json');
      assert.equal(options.headers.get('X-CloudTMS-Banking-Pay-Route-Class'), 'DISPLAY');
      return Response.json(status('NEXT', '9007199254740993'));
    } });
  assert.deepEqual(await rpc(), status('NEXT', '9007199254740993'));
  await assert.rejects(rpc('caller_selected_rpc'), TypeError);
  assert.equal(calls, 1);
});

test('invalid adapter target/header/service claim fails before any network', () => {
  const valid = { baseUrl: 'https://agency-test.invalid', headers: headers(), fetchImpl: () => assert.fail('must not fetch') };
  for (const options of [
    { baseUrl: 'http://agency-test.invalid' }, { baseUrl: 'https://user:secret@agency-test.invalid' },
    { baseUrl: 'https://agency-test.invalid/?rpc=anything' }, { baseUrl: 'https://agency-test.invalid/#fragment' },
    { headers: {} }, { headers: { Authorization: 'Bearer not-a-jwt' } },
    { headers: { Authorization: `Bearer ${jwt({ role: 'authenticated' })}` } },
    { headers: { Authorization: 'Bearer e30.bm90LWpzb24.c3ludGhldGlj' } },
    { timeoutMs: 0 }, { timeoutMs: 8001 }, { timeoutMs: 1.5 }, { fetchImpl: null }
  ]) assert.throws(() => createBpayNextOwnerStatusRpc({ ...valid, ...options }), TypeError);
  assert.doesNotThrow(() => createBpayNextOwnerStatusRpc({ ...valid, baseUrl: 'http://127.0.0.1:3000' }));
});

test('raw dependency errors and malformed/scalar/extra-field results are safe unavailable refusals', async () => {
  for (const make of [
    () => Response.json({ message: 'sensitive dependency body' }, { status: 403 }),
    () => new Response('not JSON', { headers: { 'content-type': 'application/json' } }),
    () => new Response('{"active_owner":"NEXT","active_owner":"LEGACY","owner_epoch":"1"}', { headers: { 'content-type': 'application/json' } }),
    () => new Response('{"active_owner":"NEXT","active_\\u006fwner":"LEGACY","owner_epoch":"1"}', { headers: { 'content-type': 'application/json' } }),
    () => Response.json({ ...status(), extra: 'unsafe' }), () => Response.json([status()]),
    () => new Response(JSON.stringify(status()), { headers: { 'content-type': 'text/plain' } }),
    () => new Response(null, { status: 204 }), () => { throw new Error('raw secret-like error'); }
  ]) {
    const rpc = createBpayNextOwnerStatusRpc({ baseUrl: 'https://agency-test.invalid', headers: headers(), fetchImpl: make });
    await assert.rejects(rpc(), code('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'));
  }
});

test('encoded response bytes, content length and UTF-8 are bounded before parsing', async () => {
  for (const make of [
    () => new Response('x'.repeat(BPAY_NEXT_OWNER_STATUS_LIMITS.responseBytes + 1), { headers: { 'content-type': 'application/json' } }),
    () => new Response('{}', { headers: { 'content-type': 'application/json', 'content-length': '1025' } }),
    () => new Response('{}', { headers: { 'content-type': 'application/json', 'content-length': 'invalid' } }),
    () => new Response(new Uint8Array([0xc3, 0x28]), { headers: { 'content-type': 'application/json' } })
  ]) {
    await assert.rejects(createBpayNextOwnerStatusRpc({ baseUrl: 'https://agency-test.invalid', headers: headers(), fetchImpl: make })(),
      code('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'));
  }
});

test('timeout aborts/cancels a pending body without starting legacy work', async () => {
  let cancelled = 0; let signal; let actions = 0;
  const rpc = createBpayNextOwnerStatusRpc({ baseUrl: 'https://agency-test.invalid', headers: headers(), timeoutMs: 10,
    fetchImpl: async (_, options) => {
      signal = options.signal;
      return new Response(new ReadableStream({ cancel() { cancelled++; } }), { headers: { 'content-type': 'application/json' } });
    } });
  await assert.rejects(createBpayNextLegacyFence({ ownerStatusRpc: rpc }).runLegacyWriter(() => { actions++; }),
    code('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'));
  assert.equal(signal.aborted, true); assert.equal(cancelled, 1); assert.equal(actions, 0);
});

test('late fetch response after timeout is cancelled and never used to authorize an action', async () => {
  let resolve; let cancelled = 0; let actions = 0;
  const rpc = createBpayNextOwnerStatusRpc({ baseUrl: 'https://agency-test.invalid', headers: headers(), timeoutMs: 10,
    fetchImpl: () => new Promise((done) => { resolve = done; }) });
  await assert.rejects(createBpayNextLegacyFence({ ownerStatusRpc: rpc }).runLegacyWriter(() => { actions++; }),
    code('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'));
  resolve(new Response(new ReadableStream({ cancel() { cancelled++; } }), { headers: { 'content-type': 'application/json' } }));
  await new Promise((done) => setTimeout(done, 0));
  assert.equal(cancelled, 1); assert.equal(actions, 0);
});

test('timeout cancellation cannot turn a complete-but-unclosed body into a LEGACY authorization', async () => {
  let cancelled = 0; let actions = 0;
  const rpc = createBpayNextOwnerStatusRpc({ baseUrl: 'https://agency-test.invalid', headers: headers(), timeoutMs: 10,
    fetchImpl: async () => new Response(new ReadableStream({
      start(controller) { controller.enqueue(new TextEncoder().encode(JSON.stringify(status()))); },
      cancel() { cancelled++; }
    }), { headers: { 'content-type': 'application/json' } }) });
  await assert.rejects(createBpayNextLegacyFence({ ownerStatusRpc: rpc }).runLegacyWriter(() => { actions++; }),
    code('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE'));
  assert.equal(cancelled, 1); assert.equal(actions, 0);
});

test('known NEXT routes and factual banking-id/Source/ordinary routes remain outside the legacy matcher', () => {
  for (const [method, path] of [['POST', '/api/banking/pay/next/command'], ['POST', '/api/banking/pay/next/frozen-detail'],
    ['POST', '/api/banking/pay/next/history-page'], ['POST', '/api//banking/pay/next/history-page/'],
    ['POST', '/api/banking/pay/next/preparation'], ['GET', '/api/banking/pay/next/preparation-page'], ['GET','/api//banking/pay/next/owner-status/'],
    ['GET', '/api/banking/pay/next/advice/11111111-2222-4333-8444-555555555555'],
    ['GET', '/api/banking/id/preview'], ['POST', '/api/banking/id/execute'],
    ['POST','/api/banking/pay/bank-name-check/override'],
    ['POST','/api/banking/pay/bank-name-check/clear-override'],
    ['POST', '/api/timesheets/one/authorise'], ['POST', '/api/timesheets/import'],
    ['PATCH', '/api/candidates/one'], ['POST', '/api/weekly-source/publish'],
    ['POST','/auth/login'],['POST','/auth/refresh']]) {
    assert.equal(isBpayNextLegacyFinancialRoute(method, path), false, path);
  }
});

test('old financial routes and unknown NEXT paths do not escape pre-network guard', () => {
  for (const [method, path] of [['GET', '/api/banking/pay/workbench'], ['POST', '/api/banking/pay/execute'],
    ['POST', '/api/banking/pay/provider-webhook/provider/id'], ['GET', '/api/banking/finance/register'],
    ['POST', '/api/banking/pay/next/unknown'], ['POST', '/api/banking/pay/next/command/extra'],
    ['GET', '/api/banking/pay/next/command'], ['POST','/api/banking/pay/next/owner-status'], ['GET','/api/banking/pay/next/owner-status/extra'], ['GET', '/api/banking/pay/next/frozen-detail'],
    ['GET', '/api/banking/pay/next/history-page'], ['POST', '/api/banking/pay/next/history-page/extra'],
    ['GET', '/api/banking/pay/next/preparation'], ['POST', '/api/banking/pay/next/preparation-page'],
    ['POST', '/api/banking/pay/next/advice/11111111-2222-4333-8444-555555555555'],
    ['GET', '/api/banking/pay/next/advice/not-a-uuid'],
    ['GET', '/api/banking/pay/next/advice/11111111-2222-4333-8444-555555555555/extra'],
    ['POST', '/api/banking/pay/next/preparation/extra'], ['GET', '/api/banking/pay/next/preparation-page/extra'],
    ['POST', '/api/timesheets/one/advance-payment'], ['POST', '/api/timesheets/one/unadvance-payment'],
    ['POST', '/api/timesheets/one/snooze-payment'], ['POST', '/api/timesheets/one/snooze-payment/clear'],
    ['POST', '/api/timesheets/one/pay-adjustment'], ['POST', '/api/remittances/send'],
    ['POST', '/api/remittances/email-for-candidate'], ['POST', '/api/candidates/one/pay-method-change'],
    ['GET', '/api/candidates/one/pay-method-change-preview'],
    ['POST','/auth/reauth/start'],['POST','/auth/reauth/verify'],
    ['POST','/api/banking//pay/batch/test/cancel'],
    ['POST','/api//timesheets/test/advance-payment'],
    ['POST','/api//candidates/test/pay-method-change'],
    ['POST','/api//banking/pay/provider-webhook/test/id']]) {
    assert.equal(isBpayNextLegacyFinancialRoute(method, path), true, path);
  }
});

const tables = [
  'pay_advance_patches','pay_advance_reservations','pay_advances','pay_bank_transfer_events','pay_bank_transfers',
  'pay_batch_auth_actions','pay_batch_auth_requests','pay_batch_auth_tokens','pay_batch_candidates','pay_batch_display_summary',
  'pay_batch_item_breakdowns','pay_batch_items','pay_batch_paye_net_inputs','pay_batch_timesheet_snapshots','pay_batches',
  'pay_finance_case_components','pay_finance_case_events','pay_finance_case_oneoff_payout_bank_details','pay_item_snoozes',
  'pay_manual_adjustment_carry_forwards','pay_payment_correction_actions','pay_payment_correction_items',
  'pay_payment_correction_request_candidates','pay_payment_correction_requests','pay_payment_correction_work_items',
  'pay_payment_return_notice_groups','pay_snooze_warning_acknowledgements','timesheet_pay_state','timesheet_pay_state_history',
  'timesheet_payment_overrides','ts_pay_adjustments','banking_pay_operations','banking_pay_workbench_jobs'
];

test('saved SQL contains exact33 financial scopes and retires only its own factual-token fences', async () => {
  const source = await readFile(new URL('../supabase/repeatable/04102026_0550_banking_pay_next_legacy_writer_fence_v1.sql', import.meta.url), 'utf8');
  const inventories = [...source.matchAll(/-- BEGIN EXACT LEGACY RELATIONS([\s\S]*?)-- END EXACT LEGACY RELATIONS/g)];
  assert.equal(inventories.length, 2);
  for (const inventory of inventories) assert.deepEqual([...inventory[1].matchAll(/'([^']+)'/g)].map((match) => match[1]), tables);
  assert.equal(new Set(tables).size, 33);
  assert.match(source,/tgrelid='public\.banking_pay_scope_change_transactions'::regclass/);
  assert.match(source,/drop trigger %I on public\.banking_pay_scope_change_transactions/);
  assert.match(source,/v_trigger\.tgfoid<>'private\.bpay_next_legacy_writer_fence_v1\(\)'::regprocedure/);
  assert.match(source, /where m\.id=1 for share/);
  assert.match(source, /before insert or update or delete[^\n]+for each row/);
  assert.match(source, /before truncate[^\n]+for each statement/);
  assert.equal((source.match(/enable always trigger/g) || []).length, 2);
  assert.match(source, /grant execute on function public\.bpay_next_owner_status_v1\(\) to service_role/);
  assert.match(source, /v_claim<>\s*'service_role'/);
  assert.match(source, /notify pgrst,'reload schema'/);
  assert.doesNotMatch(source, /update\s+private\.bpay_next_module_control|session_replication_role|pg_trigger_depth|current_user\s*=|current_user\s+in/i);
});
