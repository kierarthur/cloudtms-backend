import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeSourceNativeComposedResult, decodeSourceUnprotectRunPrepared,
  sourceNativeContinuationFromRpcError, sourcePayDecisionReadAction } from '../../broker/src/weekly-source/source-native-contract.mjs';
import { orchestrateWeeklyProtectedAction, WeeklyProtectedActionError } from '../../broker/src/weekly-source/protected-action-orchestrator.mjs';

const id = (n) => `91000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const hash = 'a'.repeat(64);
const composed = (route = 'NEXT') => ({ version: 'SOURCE_NATIVE_COMPOSED_RESULT_V1',
  application_id: id(1), decision_id: id(2), request_id: id(3), review_id: id(4), member_key: 'shift:real',
  preparation_id: id(5), route, origin: 'APPLIED_IN_THIS_TRANSACTION', control_status: 'WITHDRAWN',
  native_receipt: route === 'NEXT' ? { family_id: id(6), orchestration_run_id: id(7), approval_id: id(8),
    generation_id: id(9), work_id: id(10), revision_id: id(11), new_family_bound_version: '9007199254740993',
    approved_pay_ex_vat: '-27.12', source_manifest_sha256: hash } : {
    family_id: id(6), orchestration_run_id: id(7), approval_id: id(8), generation_id: id(9),
    publication_request_id: id(12), new_family_bound_version: '9007199254740993', local_state: 'PENDING_FREEZE',
    authority: 'COMMON_ENTITLEMENT_HEAD', requires_first_authorisation: false, timesheet_financials_id: null,
    entitlement: { kind: 'PENDING_BUNDLE', pending_bundle_id: id(13), decision_bundle_id: id(14), bundle_revision: '1' },
    local_receipt_sha256: hash, native_result_sha256: 'b'.repeat(64) }, failure_code: null });
const detail = { version: 'SOURCE_NATIVE_CONTINUATION_V1', decision_id: id(2), request_id: id(3), reason: 'APPLICATION_INCOMPLETE' };
const rpcError = (fn = 'weekly_exceptional_pay_next_publication_status_v1') => Object.assign(new Error('RPC failed'),
  { fn, status: 400, body: 'untrusted transport text', json: { code: 'P0001',
    message: 'SOURCE_PAY_DECISION_RESULT_REQUIRED', details: JSON.stringify(detail) } });

for (const route of ['NEXT', 'SOURCE_LOCAL']) {
  test(`composed ${route} preserves its genuine private receipt and large integer strings`, () => {
    const value = composed(route);
    assert.equal(decodeSourceNativeComposedResult(value), value);
  });
  for (const key of Object.keys(composed(route))) {
    test(`${route} requires composed key ${key}`, () => {
      const value = composed(route); delete value[key];
      assert.throws(() => decodeSourceNativeComposedResult(value), { code: 'SOURCE_NATIVE_RESULT_INVALID' });
    });
  }
  for (const key of Object.keys(composed(route).native_receipt)) {
    test(`${route} requires control key ${key}`, () => {
      const value = composed(route); delete value.native_receipt[key];
      assert.throws(() => decodeSourceNativeComposedResult(value));
    });
  }
}
test('private NEXT cannot pretend to have a pre-publication command or state', () => {
  for (const extra of ['command_id', 'agency_sequence', 'state']) {
    const value = composed(); value.native_receipt[extra] = id(15);
    assert.throws(() => decodeSourceNativeComposedResult(value));
  }
});
test('pending local bundle never accepts a committed head or publication receipt', () => {
  for (const key of ['source_head_id', 'publication_receipt_id']) {
    const value = composed('SOURCE_LOCAL'); value.native_receipt.entitlement[key] = id(15);
    assert.throws(() => decodeSourceNativeComposedResult(value));
  }
});
test('genuine Local pending control withdrawal stays WITHDRAWN while finance is pending', () => {
  assert.equal(decodeSourceNativeComposedResult(composed('SOURCE_LOCAL')).control_status, 'WITHDRAWN');
  const value = composed('SOURCE_LOCAL'); value.control_status = 'NOT_APPLIED';
  assert.throws(() => decodeSourceNativeComposedResult(value));
});
test('Local unauthorised TSFIN and committed head are correlated alternatives', () => {
  const value = composed('SOURCE_LOCAL'); const receipt = value.native_receipt;
  Object.assign(receipt, { local_state: 'COMPLETE', authority: 'UNAUTHORISED_TSFIN',
    requires_first_authorisation: true, timesheet_financials_id: id(20), entitlement: null });
  decodeSourceNativeComposedResult(value);
  receipt.requires_first_authorisation = false;
  assert.throws(() => decodeSourceNativeComposedResult(value));
  Object.assign(receipt, { authority: 'COMMON_ENTITLEMENT_HEAD', timesheet_financials_id: null,
    entitlement: { kind: 'COMMITTED_HEAD', decision_bundle_id: id(14), bundle_revision: '2',
      source_head_id: id(15), publication_receipt_id: id(16) } });
  decodeSourceNativeComposedResult(value);
});
test('dependency rollback has no applied receipt and must retain a permitted failure', () => {
  const value = { ...composed(), origin: 'NOT_APPLIED_DEPENDENCY', control_status: 'NOT_APPLIED',
    native_receipt: null, failure_code: 'DEPENDENCY_UNAVAILABLE' };
  decodeSourceNativeComposedResult(value);
  value.native_receipt = composed().native_receipt;
  assert.throws(() => decodeSourceNativeComposedResult(value));
});
test('composed identity must match the exact request/member/application, never latest', () => {
  for (const key of ['application_id', 'decision_id', 'request_id', 'review_id', 'member_key', 'preparation_id', 'route', 'family_id', 'orchestration_run_id']) {
    assert.throws(() => decodeSourceNativeComposedResult(composed(), { [key]: id(99) }));
  }
});
test('bigint counters reject coercion, leading zero, negative, and overflow', () => {
  for (const token of [1, '01', '0', '-1', '9223372036854775808', '1e3']) {
    const value = composed(); value.native_receipt.new_family_bound_version = token;
    assert.throws(() => decodeSourceNativeComposedResult(value));
  }
});
test('only exact current sbRpc P0001 transport is a Source continuation', () => {
  const error = rpcError();
  assert.deepEqual(sourceNativeContinuationFromRpcError(error, error.fn), detail);
  for (const mutate of [e => { e.fn = 'other'; }, e => { e.json.code = '55000'; },
    e => { e.json.message = 'other'; }, e => { e.json.details = '{'; },
    e => { e.json.details = JSON.stringify({ ...detail, url: 'https://bad.example' }); },
    e => { e.json.details = JSON.stringify({ ...detail, decision_id: 'bad' }); },
    e => { e.json.details = JSON.stringify({ ...detail, reason: 'LATEST' }); },
    e => { delete e.json; e.code = 'SOURCE_PAY_DECISION_RESULT_REQUIRED'; }]) {
    const bad = rpcError(); mutate(bad);
    assert.equal(sourceNativeContinuationFromRpcError(bad, error.fn), null);
  }
});
test('saved result action uses the unchanged seven-key action union and server URL', () => {
  assert.deepEqual(sourcePayDecisionReadAction(id(2), id(3)), { kind: 'READ_RESULT', target_type: 'SOURCE_PAY_DECISION',
    target_id: id(2), url: `/api/weekly-source/pay-decisions/${id(2)}?request_id=${id(3)}`,
    caption: 'Check saved result', enabled: true, disabled_reason: null });
});
test('normal native orchestrator stops before context, calculation, stage or publish', async () => {
  const calls = [];
  await assert.rejects(orchestrateWeeklyProtectedAction({ action: 'WITHDRAW_PROTECTED_HOURS', request: {
    actor_user_id: id(21), source_cycle_id: id(22), work_event_id: id(23), family_id: id(6),
    expected_family_bound_version: '1', reason: 'Confirmed decision', idempotency_key: 'confirmed-native-ref-0001' },
  dependencies: { calculateWeeklySnapshot: () => { throw new Error('must not calculate'); },
    dataRpc: async (name) => { calls.push(name);
      if (name === 'weekly_exceptional_pay_prepare_action_v1') return { family_id: id(6), orchestration_run_id: id(7), request_kind: 'WITHDRAW' };
      throw rpcError(name);
    } } }), (error) => error instanceof WeeklyProtectedActionError
      && error.code === 'SOURCE_PAY_DECISION_RESULT_REQUIRED' && error.status === 409
      && error.details.retryable === false && error.details.next_action.kind === 'READ_RESULT');
  assert.deepEqual(calls, ['weekly_exceptional_pay_prepare_action_v1', 'weekly_exceptional_pay_next_publication_status_v1']);
});

test('atomic native-birth decoder accepts exact 20-key projection and retains stub state', () => {
  const prepared = { ok: true, outcome: 'PREPARED', family_id: id(6), orchestration_run_id: id(7),
    source_cycle_id: id(20), source_group_id: id(21), agency_id: id(22), candidate_id: id(23),
    client_id: id(24), contract_id: id(25), root_timesheet_id: id(26), work_event_id: id(27),
    week_start_date: '2026-10-12', week_ending_date: '2026-10-18', source_mode: 'HEALTHROSTER_WEEKLY',
    family_bound_version: '9007199254740993', request_kind: 'WITHDRAW', protected_schedule: {
      work_date: '2026-10-12', start_at_local: '2026-10-12 09:00:00', end_at_local: '2026-10-12 17:00:00', break_minutes: 30 },
    run_state: 'RUNNING', idempotent_replay: false };
  const value = { version: 'SOURCE_UNPROTECT_RUN_PREPARED_V1', preparation_id: id(5), decision_id: id(2),
    member_key: 'shift:real', review_id: id(4), accept_or_resume_request_id: id(3), route: 'NEXT',
    preparation_state: 'RUN_PREPARED', expires_at_utc: '2026-10-16T19:30:00Z', prepared };
  assert.equal(decodeSourceUnprotectRunPrepared(value), value);
  value.prepared.contract_week_id = id(40);
  assert.throws(() => decodeSourceUnprotectRunPrepared(value));
});
