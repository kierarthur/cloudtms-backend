import assert from 'node:assert/strict';
import test from 'node:test';
import { prepareSourcePayDecisionConfirmation } from '../../broker/src/weekly-source/source-pay-decision-preparation.mjs';
import { decodeSourcePayDecisionConfirmationPrepared, sourceNativeContinuationFromRpcError } from '../../broker/src/weekly-source/source-native-contract.mjs';
const id = (n) => `91000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const hash = 'a'.repeat(64);
const request = () => ({ version: 'SOURCE_PAY_DECISION_V1', action: 'ACCEPT', decision_id: id(1), request_id: id(2),
  review_id: id(3), review_digest: hash, decision: 'EXCLUDE_FUTURE_WORK_PAY', supersedes_decision_id: null,
  expected_prior_dispositions: [], expected_prior_dispositions_sha256: hash, invoice_policy: 'KEEP_SOURCE_INVOICE',
  contact_policy: 'DO_NOT_QUERY_SELECTED_ISSUES', reason: 'Office confirmation', evidence_refs: [],
  expected_pay_cut_hash: hash, expected_source_invoice_hash: hash, expected_issue_set_hash: hash });
function member(n) { return { version: 'SOURCE_UNPROTECT_RUN_PREPARED_V1', preparation_id: id(10 + n),
  decision_id: id(1), member_key: `real-source-member:${n}`, review_id: id(3), accept_or_resume_request_id: id(2),
  route: 'NEXT', preparation_state: 'RUN_PREPARED', expires_at_utc: '2099-10-16T19:30:00Z',
  prepared: { ok: true, outcome: 'PREPARED', family_id: id(20 + n), orchestration_run_id: id(30 + n),
    source_cycle_id: id(40), source_group_id: id(41), agency_id: id(42), candidate_id: id(50 + n), client_id: id(43),
    contract_id: id(60 + n), root_timesheet_id: id(70 + n), work_event_id: id(80 + n), week_start_date: '2026-10-12',
    week_ending_date: '2026-10-18', source_mode: 'HEALTHROSTER_WEEKLY', family_bound_version: '9007199254740993',
    request_kind: 'WITHDRAW', protected_schedule: { work_date: '2026-10-16', start_at_local: '2026-10-16 09:00:00',
      end_at_local: '2026-10-16 17:00:00', break_minutes: 30 }, run_state: 'RUNNING', idempotent_replay: false } }; }
const result = () => ({ version: 'SOURCE_PAY_DECISION_CONFIRMATION_PREPARED_V1', decision_id: id(1), review_id: id(3),
  request_id: id(2), members: [member(1), member(2)] });
test('fresh confirmation calls one server composition with unchanged nested browser body', async () => {
  const body = request();const calls = [];
  const saved = await prepareSourcePayDecisionConfirmation({ request: body, actorUserId: id(99), dataRpc: async (...args) => {
    calls.push(args);return result();
  } });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], ['weekly_source_pay_decision_confirmation_prepare_v1', { p_request: {
    version: 'SOURCE_PAY_DECISION_CONFIRMATION_PREPARE_V1', actor_user_id: id(99), confirmation_request: body } }]);
  assert.deepEqual(saved.members.map((m) => m.member_key), ['real-source-member:1', 'real-source-member:2']);
  assert.equal(saved.members[0].prepared.family_bound_version, '9007199254740993');
});
test('source-only/no-protection/positive composition can return the exact empty set', async () => {
  const body = request();body.decision = 'ACCEPT_FINAL_SOURCE_PAY';const empty = result();empty.members = [];
  const saved = await prepareSourcePayDecisionConfirmation({ request: body, actorUserId: id(99), dataRpc: async () => empty });
  assert.deepEqual(saved.members, []);
  await assert.rejects(prepareSourcePayDecisionConfirmation({ request: body, actorUserId: id(99), dataRpc: async () => result() }),
    { code: 'SOURCE_NATIVE_RESULT_INVALID' });
});
for (const mutate of [r => { r.members.push({ ...r.members[0] }); },
  r => { r.members[1].prepared.orchestration_run_id = r.members[0].prepared.orchestration_run_id; },
  r => { r.members[1].preparation_id = r.members[0].preparation_id; },
  r => { r.members[1].review_id = id(98); }, r => { r.application_id = id(98); }]) {
  test('batch decoder rejects cross-owned, duplicate or extra internal results', () => {
    const value = result();mutate(value);assert.throws(() => decodeSourcePayDecisionConfirmationPrepared(value));
  });
}
test('consumed, stale and expired metadata stop the Worker before calculation', async () => {
  for (const [state, code] of [['CONSUMED', 'SOURCE_PAY_DECISION_RESULT_REQUIRED'], ['STALE', 'SOURCE_PAY_DECISION_REVIEW_CHANGED'],
    ['RUN_PREPARED', 'SOURCE_PAY_DECISION_REVIEW_CHANGED']]) {
    const value = result();value.members[0].preparation_state = state;
    if (state === 'RUN_PREPARED') value.members[0].expires_at_utc = '2000-01-01T00:00:00Z';
    await assert.rejects(prepareSourcePayDecisionConfirmation({ request: request(), actorUserId: id(99), dataRpc: async () => value }),
      (error) => error.code === code && error.status === 409 && error.details.retryable === false);
  }
});
test('exact committed preparation request redirects to the real original Source result', async () => {
  const fn = 'weekly_source_pay_decision_confirmation_prepare_v1';
  const error = Object.assign(new Error('RPC failed'), { fn, status: 400, json: { code: 'P0001',
    message: 'SOURCE_PAY_DECISION_RESULT_REQUIRED', details: JSON.stringify({ version: 'SOURCE_NATIVE_CONTINUATION_V1',
      decision_id: id(1), request_id: id(2), reason: 'APPLICATION_INCOMPLETE' }) } });
  // Extending Source's internal composition does not widen the four normal
  // native functions recognised by the existing protected action adapter.
  assert.equal(sourceNativeContinuationFromRpcError(error, fn), null);
  await assert.rejects(prepareSourcePayDecisionConfirmation({ request: request(), actorUserId: id(99), dataRpc: async () => { throw error; } }),
    (e) => e.code === 'SOURCE_PAY_DECISION_RESULT_REQUIRED' && e.details.next_action.url === `/api/weekly-source/pay-decisions/${id(1)}?request_id=${id(2)}`);
  error.json.details = JSON.stringify({ version: 'SOURCE_NATIVE_CONTINUATION_V1', decision_id: id(1), request_id: id(2),
    reason: 'APPLICATION_INCOMPLETE', url: 'https://untrusted.example' });
  await assert.rejects(prepareSourcePayDecisionConfirmation({ request: request(), actorUserId: id(99), dataRpc: async () => { throw error; } }),
    (e) => e === error);
});
test('browser cannot choose required members, actor, preparation or assembly', async () => {
  for (const key of ['member_key', 'actor_user_id', 'preparation_id', 'assembly_id']) {
    let called = false;
    await assert.rejects(prepareSourcePayDecisionConfirmation({ request: { ...request(), [key]: id(90) }, actorUserId: id(99),
      dataRpc: async () => { called = true;return result(); } }));assert.equal(called, false);
  }
});
