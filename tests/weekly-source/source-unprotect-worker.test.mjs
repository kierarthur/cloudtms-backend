import assert from 'node:assert/strict';
import test from 'node:test';
import { calculateSourceUnprotectPreviewQuote, calculateSourceUnprotectAssemblyQuote, prepareSourcePayDecisionServiceRequest } from '../../broker/src/weekly-source/source-unprotect-worker.mjs';
import { hashSourceNativeJson } from '../../broker/src/weekly-source/source-native-economic.mjs';
const id = n => `94000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const hash = 'a'.repeat(64);
const expires = '2099-10-16T19:30:00Z';
const schedule = { work_date: '2026-10-12', start_at_local: '2026-10-12 09:00:00', end_at_local: '2026-10-12 17:00:00', break_minutes: 30 };
const body = () => ({ version: 'SOURCE_PAY_DECISION_V1', action: 'ACCEPT', decision_id: id(1), request_id: id(2), review_id: id(3),
  review_digest: hash, decision: 'EXCLUDE_FUTURE_WORK_PAY', supersedes_decision_id: null, expected_prior_dispositions: [],
  expected_prior_dispositions_sha256: hash, invoice_policy: 'KEEP_SOURCE_INVOICE', contact_policy: 'DO_NOT_QUERY_SELECTED_ISSUES',
  reason: 'Confirmed by Office', evidence_refs: [], expected_pay_cut_hash: hash, expected_source_invoice_hash: hash, expected_issue_set_hash: hash });
const prepared = () => ({ ok: true, outcome: 'PREPARED', family_id: id(4), orchestration_run_id: id(5), source_cycle_id: id(6),
  source_group_id: id(7), agency_id: id(8), candidate_id: id(9), client_id: id(10), contract_id: id(11), root_timesheet_id: id(12),
  work_event_id: id(13), week_start_date: '2026-10-12', week_ending_date: '2026-10-18', source_mode: 'HEALTHROSTER_WEEKLY',
  family_bound_version: '9007199254740993', request_kind: 'WITHDRAW', protected_schedule: schedule, run_state: 'RUNNING', idempotent_replay: false });
const born = () => ({ version: 'SOURCE_UNPROTECT_RUN_PREPARED_V1', preparation_id: id(14), decision_id: id(1), member_key: 'real:source-member',
  review_id: id(3), accept_or_resume_request_id: id(2), route: 'NEXT', preparation_state: 'RUN_PREPARED', expires_at_utc: expires, prepared: prepared() });
function context() {
  const p = prepared();const timesheet = { timesheet_id: id(12), contract_id: id(11), version: 1, is_current: true, archived_at_utc: null };
  const contract = { id: id(11), candidate_id: id(9), client_id: id(10) };const financial = { id: id(15) };
  return { action: 'WITHDRAW', family_id: p.family_id, family_bound_version: p.family_bound_version, current_target_vector_sha256: hash,
    source_cycle_id: p.source_cycle_id, source_group_id: p.source_group_id, agency_id: p.agency_id, candidate_id: p.candidate_id,
    client_id: p.client_id, contract_id: p.contract_id, contract_week_id: id(16), root_timesheet_id: p.root_timesheet_id,
    work_event_id: p.work_event_id, week_ending_date: p.week_ending_date, source_mode: p.source_mode, policy: {}, timesheet,
    contract_week: { id: id(16) }, contract_record: contract, current_financial: financial, approved_component_identity_components: null,
    requires_zero_financial: false, protected_schedule: schedule, source_segments: [], protected_decisions: [{ work_event_id: id(13), state: 'ACCEPTED_SOURCE', fixed_schedule: null }],
    source_proposal: { selected_work_event_id: id(13), source_present: false, source_minutes: 0, source_revision: null, source_hash: null, source_segments: [] },
    client_sources: [], source_expenses: [], candidate_submission: null, current_comparison_revision_id: null, current_final_revision_id: null,
    root_financial: {}, provider: {}, expected_head_revision: '0', request_sequence: '1', zero_rate_source_refs: {}, next_source_segments: [],
    protected_current_placement: { version: 'PROTECTED_CURRENT_PLACEMENT_V1', moved: false, family_id: id(4), family_root_id: id(12), member_key: id(12),
      original_root_timesheet_id: id(12), original_contract_id: id(11), current_timesheet_id: id(12), current_contract_id: id(11), current_financial_id: id(15),
      current_timesheet_version: 1, current_approval_work_id: null, original_canonical_work_id: null, move_operation_id: null, placement_revision: '0',
      immutable_original_evidence_sha256: hash, source_invoice_certificate_sha256: hash, placement_certificate_sha256: hash },
    current_entitlement: { timesheet, contract_record: contract, current_financial: financial, current_approval_work_id: null } };
}
function calculator(calls) { return async args => {
  calls.push(['calculator', args]);return { ok: true, snapshot: { timesheet_id: args.context.root_timesheet_id, timesheet_version: 1,
    candidate_id: id(9), client_id: id(10), policy_snapshot_json: {}, rate_source_refs_json: {},
    additional_units_json: {}, expenses_pay_ex_vat: 0, invoice_breakdown_json: { mode: 'SEGMENTS', segments: [] } } };
}; }
function owner(calls, mutate = () => {}) { return async (fn, args) => {
  calls.push([fn, args]);
  if (fn === 'weekly_source_pay_decision_confirmation_prepare_v1') {
    const batch = { version: 'SOURCE_PAY_DECISION_CONFIRMATION_PREPARED_V1', decision_id: id(1), review_id: id(3), request_id: id(2),
      confirmation_reason: body().reason, members: [born()] };mutate(batch);return batch;
  }
  if (fn === 'weekly_exceptional_pay_action_context_v1') return { ...context(), ok: true, contract: 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1', orchestration_run_id: id(5) };
  if (fn === 'weekly_source_pay_decision_unprotect_quote_context_v1') {
    const reviewed = operands();
    const quote = await calculateSourceUnprotectPreviewQuote({ reviewContext: reviewed.review_context,
      quantityPartition: reviewed.quantity_partition, priorCommitmentCutSha256: reviewed.prior_commitment_cut_sha256,
      calculateWeeklySnapshot: calculator([]) });
    return { version: 'SOURCE_UNPROTECT_QUOTE_CONTEXT_RESULT_V1', preparation_id: id(14), decision_id: id(1), review_id: id(3),
      member_key: born().member_key, accept_or_resume_request_id: id(2), orchestration_run_id: id(5), context_sha256: hash,
      source_observation_sha256: hash, economic_target_sha256: quote.economic_target_sha256,
      quantity_partition: reviewed.quantity_partition, prior_commitment_cut_sha256: hash };
  }
  if (fn === 'weekly_source_pay_decision_unprotect_prepare_v1') return { version: 'SOURCE_UNPROTECT_PREPARATION_RESULT_V1', preparation_id: id(14),
    decision_id: id(1), member_key: born().member_key, review_id: id(3), accept_or_resume_request_id: id(2), orchestration_run_id: id(5),
    payload_sha256: await hashSourceNativeJson('SOURCE_UNPROTECT_NATIVE_PAYLOAD_V1', args.p_request.native_payload), expires_at_utc: expires };
  assert.fail(`Unexpected financial RPC ${fn}`);
}; }
const savedContext = () => ({ version: 'SOURCE_PAY_DECISION_CONFIRMATION_CONTEXT_RESULT_V1', decision_id: id(1), review_id: id(3),
  request_id: id(2), confirmation_reason: body().reason, required_members: [{ member_key: born().member_key,
    family_id: prepared().family_id, work_event_id: prepared().work_event_id }] });
const rpcError = (fn, code, message) => Object.assign(new Error('RPC failed'), { fn, status: 400, json: { code, message }, body: '{}' });
const timeout = fn => Object.assign(new Error('RPC timed out'), { fn, status: 408, json: null, body: '' });

test('failed atomic birth rederives the exact required set and emits no invented native identity or financial call', async () => {
  const fn = 'weekly_source_pay_decision_confirmation_prepare_v1';
  for (const [error, failure] of [[timeout(fn), 'RESULT_UNKNOWN'], [rpcError(fn, '55P03', 'lock timeout'), 'DEPENDENCY_UNAVAILABLE']]) {
    const calls = [];const service = await prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20),
      calculateWeeklySnapshot: calculator(calls), dataRpc: async (name, args) => {
        calls.push([name, args]);if (name === fn) throw error;
        assert.equal(name, 'weekly_source_pay_decision_confirmation_context_v1');
        assert.deepEqual(args.p_request.confirmation_request, body());return savedContext();
      } });
    assert.deepEqual(calls.map(([name]) => name), [fn, 'weekly_source_pay_decision_confirmation_context_v1']);
    assert.deepEqual(service.unprotect_preparations, [{ member_key: born().member_key, status: 'NOT_PREPARED', failure_code: failure }]);
    assert.equal(Object.hasOwn(service.unprotect_preparations[0], 'preparation_id'), false);
  }
});

test('a concrete metadata registration dependency retains the actual run fence and never invokes a native financial writer', async () => {
  const fn = 'weekly_source_pay_decision_unprotect_prepare_v1';
  for (const [error, failure] of [[timeout(fn), 'RESULT_UNKNOWN'],
    [rpcError(fn, '55000', 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_OWNER_NOT_READY'), 'DEPENDENCY_UNAVAILABLE'],
    [rpcError(fn, '55000', 'SOURCE_NATIVE_PREREQUISITE_NOT_PREPARED'), 'DEPENDENCY_UNAVAILABLE']]) {
    const calls = [];const actual = owner(calls);
    const service = await prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20), calculateWeeklySnapshot: calculator(calls),
      dataRpc: async (name, args) => { if (name === fn) { calls.push([name, args]);throw error; }return actual(name, args); } });
    assert.equal(calls.at(-1)[0], fn);
    assert.deepEqual(service.unprotect_preparations, [{ member_key: born().member_key, status: 'NOT_PREPARED', failure_code: failure }]);
  }
});

test('permission/stale/integrity/generic transport/malformed/wrong-function failures never become NOT_PREPARED', async () => {
  const fn = 'weekly_source_pay_decision_confirmation_prepare_v1';
  for (const error of [rpcError(fn, '42501', 'denied'), rpcError(fn, '40001', 'changed'), rpcError(fn, '23514', 'invalid'),
    rpcError(fn, '55000', 'WEEKLY_PROTECTED_ACTION_SCOPE_INVALID'), Object.assign(new Error('unavailable'), { status: 503 }),
    { ...timeout(fn), body: 'not empty' }, { ...timeout(fn), json: {} }, timeout('weekly_exceptional_pay_publish_next_v1'),
    new TypeError('fetch failed')]) {
    const calls = [];
    await assert.rejects(prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20), calculateWeeklySnapshot: calculator(calls),
      dataRpc: async name => { calls.push(name);throw error; } }), e => e === error);
    assert.deepEqual(calls, [fn]);
  }
});

test('one unavailable member retains its truthful status while another genuine member completes payload preparation', async () => {
  const calls = [];const failed = { ...born(), member_key: 'actual:other-member', preparation_id: id(41),
    prepared: { ...prepared(), orchestration_run_id: id(42), work_event_id: id(43) } };
  const actual = owner(calls, batch => { batch.members.unshift(failed); });
  const service = await prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20), calculateWeeklySnapshot: calculator(calls),
    dataRpc: async (name, args) => {
      if (name === 'weekly_source_pay_decision_unprotect_quote_context_v1' && args.p_request.member_key === failed.member_key) {
        calls.push([name, args]);throw rpcError(name, '55P03', 'lock timeout');
      }
      return actual(name, args);
    } });
  assert.deepEqual(service.unprotect_preparations.map(ref => [ref.member_key, ref.status]), [[failed.member_key, 'NOT_PREPARED'], [born().member_key, 'READY']]);
  assert.equal(service.unprotect_preparations[1].preparation_id, born().preparation_id);
  assert.equal(calls.filter(([name]) => name === 'weekly_source_pay_decision_unprotect_prepare_v1').length, 1);
});

test('failed birth recovery still refuses changed/consumed saved context before Source acceptance', async () => {
  for (const changed of ['member', 'review', 'committed', 'permission']) {
    const calls = [];const birth = 'weekly_source_pay_decision_confirmation_prepare_v1';
    await assert.rejects(prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20), calculateWeeklySnapshot: calculator(calls),
      dataRpc: async name => {
        calls.push(name);if (name === birth) throw timeout(name);
        if (changed === 'permission') throw rpcError(name, '42501', 'denied');
        if (changed === 'committed') throw rpcError(name, 'P0001', 'SOURCE_PAY_DECISION_RESULT_REQUIRED');
        const result = savedContext();if (changed === 'member') result.required_members[0].from_browser = true;
        else result.review_id = id(40);return result;
      } }));
    assert.deepEqual(calls, [birth, 'weekly_source_pay_decision_confirmation_context_v1']);
  }
});
test('Run-free preview calls actual calculator without allocating a run/publication identity or invoking financial RPC', async () => {
  const calls = [];const quote = await calculateSourceUnprotectPreviewQuote({ reviewContext: {
    version: 'SOURCE_UNPROTECT_REVIEW_CONTEXT_RESULT_V1', assembly_id: id(17), decision_id: id(1), member_key: born().member_key,
    source_phase: 'NO_FINAL_REPORT_YET', source_observation_sha256: hash, context_sha256: hash, context: context() },
  quantityPartition: [], priorCommitmentCutSha256: hash, calculateWeeklySnapshot: calculator(calls) });
  assert.deepEqual(calls.map(r => r[0]), ['calculator']);assert.deepEqual(quote.economic_target.component_economics, []);
  assert.equal(quote.calculation_owner, 'buildWeeklyScheduleSegmentsSnapshot');
  assert.equal(Object.hasOwn(quote.economic_target, 'orchestration_run_id'), false);
});
test('Confirmed composition uses genuine run context, actual pure native builder and full payload registration before SQL acceptance', async () => {
  const calls = [];const service = await prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20),
    dataRpc: owner(calls), calculateWeeklySnapshot: calculator(calls) });
  assert.deepEqual(calls.map(r => r[0]), ['weekly_source_pay_decision_confirmation_prepare_v1', 'weekly_source_pay_decision_unprotect_quote_context_v1',
    'weekly_exceptional_pay_action_context_v1',
    'calculator', 'weekly_source_pay_decision_unprotect_prepare_v1']);
  const payload = calls.at(-1)[1].p_request.native_payload;
  assert.deepEqual(Object.keys(payload), ['route', 'prepare_request', 'publish_request']);
  assert.equal(payload.prepare_request.protected_schedule, null);assert.equal(payload.prepare_request.action, 'WITHDRAW');
  assert.equal(payload.prepare_request.reason, body().reason);assert.equal(payload.publish_request.reason, body().reason);
  assert.equal(payload.publish_request.idempotency_key, `${payload.prepare_request.idempotency_key}:next`);
  assert.deepEqual(payload.publish_request.target_snapshot, { schema_version: 'WEEKLY_PROTECTED_NEXT_TARGET_V1', components: [] });
  assert.equal(service.preview_assembly_ref, null);assert.equal(service.unprotect_preparations[0].status, 'READY');
  assert.equal(Object.hasOwn(body(), 'actor_user_id'), false);
});
test('Changed full post-confirmation snapshot or cross-request reviewed quote stops before payload registration', async () => {
  for (const mutation of ['snapshot', 'request', 'economic_hash', 'quantity']) {
    const calls = [];const original = owner(calls);
    await assert.rejects(prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20),
      dataRpc: async (fn, args) => {
        const result = await original(fn, args);
        if (fn === 'weekly_source_pay_decision_unprotect_quote_context_v1') {
          if (mutation === 'request') result.accept_or_resume_request_id = id(40);
          if (mutation === 'economic_hash') result.economic_target_sha256 = 'b'.repeat(64);
          if (mutation === 'quantity') result.quantity_partition[0].excluded_quantity_json = { kind: 'WHOLE_SHIFT', changed: true };
        }
        return result;
      }, calculateWeeklySnapshot: async args => {
        const result = await calculator(calls)(args);
        if (mutation === 'snapshot') result.snapshot.tax_or_other_complete_native_field = 'changed';
        return result;
      } }), error => ['SOURCE_NATIVE_RESULT_INVALID','SOURCE_PAY_DECISION_REVIEW_CHANGED'].includes(error.code));
    assert.equal(calls.some(([fn]) => fn === 'weekly_source_pay_decision_unprotect_prepare_v1'), false);
  }
});
test('Consumed run stops before context/calculation, and positive requires an actually empty preparation set', async () => {
  const calls = [];
  await assert.rejects(prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20),
    dataRpc: owner(calls, batch => { batch.members[0].preparation_state = 'CONSUMED'; }), calculateWeeklySnapshot: calculator(calls) }),
  { code: 'SOURCE_PAY_DECISION_RESULT_REQUIRED' });
  assert.equal(calls.length, 1);
  const positive = { ...body(), decision: 'ACCEPT_FINAL_SOURCE_PAY' };const emptyCalls = [];
  const service = await prepareSourcePayDecisionServiceRequest({ request: positive, actorUserId: id(20),
    dataRpc: owner(emptyCalls, batch => { batch.members = []; }), calculateWeeklySnapshot: calculator(emptyCalls) });
  assert.deepEqual(service.unprotect_preparations, []);assert.equal(emptyCalls.length, 1);
});
test('Cross-request registration/hash is rejected without attempting a stage, complete or publication', async () => {
  const calls = [];const original = owner(calls);
  await assert.rejects(prepareSourcePayDecisionServiceRequest({ request: body(), actorUserId: id(20), calculateWeeklySnapshot: calculator(calls),
    dataRpc: async (fn, args) => { const result = await original(fn, args);return fn.endsWith('unprotect_prepare_v1')
      ? { ...result, payload_sha256: 'b'.repeat(64) } : result; } }), { code: 'SOURCE_NATIVE_RESULT_INVALID' });
  assert.equal(calls.at(-1)[0], 'weekly_source_pay_decision_unprotect_prepare_v1');
});
function operands() {
  return { version: 'SOURCE_PAY_DECISION_PREVIEW_QUOTE_CONTEXT_RESULT_V1', prior_commitment_cut_sha256: hash,
    review_context: { version: 'SOURCE_UNPROTECT_REVIEW_CONTEXT_RESULT_V1', assembly_id: id(17), decision_id: id(1),
      member_key: born().member_key, source_phase: 'NO_FINAL_REPORT_YET', source_observation_sha256: hash, context_sha256: hash, context: context() },
    quantity_partition: [{ cut_id: id(18), cut_member_no: '1', member_sha256: hash, family_root_id: id(12), source_row_id: id(19),
      source_work_event_id: id(13), canonical_work_id: null, canonical_component_key: null, scope_kind: 'SOURCE_ROW', scope_mode: 'WHOLE_SHIFT',
      work_dates: ['2026-10-12'], supported_quantity_json: null, excluded_quantity_json: { kind: 'WHOLE_SHIFT' } }] };
}
test('Assembly quote reads exact immutable server cut operands and preserves genuine prefinancial nulls', async () => {
  const calls = [];const quote = await calculateSourceUnprotectAssemblyQuote({ assemblyId: id(17), decisionId: id(1), memberKey: born().member_key,
    actorUserId: id(20), cutId: id(18), expectedPayCutHash: hash, calculateWeeklySnapshot: calculator(calls),
    dataRpc: async (fn, args) => { calls.push([fn, args]);assert.equal(fn, 'weekly_source_pay_decision_preview_quote_context_v1');return operands(); } });
  assert.deepEqual(calls.map(r => r[0]), ['weekly_source_pay_decision_preview_quote_context_v1', 'calculator']);
  assert.deepEqual(quote.economic_target.quantity_partition, operands().quantity_partition);
  assert.equal(quote.economic_target.quantity_partition[0].canonical_work_id, null);
  assert.equal(quote.economic_target.quantity_partition[0].supported_quantity_json, null);
});
test('Cross-assembly, member, cut, digest, duplicate and extra quote operands stop before calculator', async () => {
  for (const mutate of [o => { o.review_context.assembly_id = id(30); }, o => { o.review_context.member_key = 'another'; },
    o => { o.quantity_partition[0].cut_id = id(30); }, o => { o.prior_commitment_cut_sha256 = 'b'.repeat(64); },
    o => { o.quantity_partition.push({ ...o.quantity_partition[0] }); }, o => { o.quantity_partition[0].amount = '100.00'; },
    o => { o.quantity_partition[0].cut_member_no = 1; }, o => { o.browser_amount = 100; }]) {
    const calls = [];const value = operands();mutate(value);
    await assert.rejects(calculateSourceUnprotectAssemblyQuote({ assemblyId: id(17), decisionId: id(1), memberKey: born().member_key,
      actorUserId: id(20), cutId: id(18), expectedPayCutHash: hash, calculateWeeklySnapshot: calculator(calls), dataRpc: async () => value }),
    { code: 'SOURCE_NATIVE_RESULT_INVALID' });
    assert.deepEqual(calls, []);
  }
});
