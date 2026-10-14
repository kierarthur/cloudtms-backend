import test from 'node:test';
import assert from 'node:assert/strict';
import {
  createTimesheetCorrectionOrchestrator,
  cloneFrozenCorrectionReversal,
  CorrectionOrchestrationError
} from '../broker/src/timesheet-correction-orchestrator.mjs';

const id = n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const I = { request: id(1), actor: id(2), original: id(3), p0: id(4), r0: id(5), candidate: id(6),
  client: id(7), root: id(8), originalFin: id(9), pFin: id(10), rFin: id(11), correction: `awc:${id(8)}:${id(1)}`, invoice: id(13), deleteOp: id(14) };
const H = 'a'.repeat(64), FH = 'b'.repeat(64);
const copy = x => JSON.parse(JSON.stringify(x));
const action = (rev = 1) => ({ request_id: I.request, expected_state_revision: rev });
function finance(ts = I.original, version = 7) {
  return {
    id: I.originalFin, is_current: true, timesheet_id: ts, timesheet_version: version,
    candidate_id: I.candidate, client_id: I.client, is_stale: false, has_rate_issue: false, has_pay_channel_issue: false,
    processing_status: 'READY_FOR_INVOICE', basis: 'SELF_REPORTED', pay_method: 'PAYE', role: 'Nurse', band: '6',
    occupant_key_norm: 'historical-occupant', policy_snapshot_json: { revision: 9, erni_pct: 15.05, no_timesheet_required: false },
    rate_source_refs_json: { kind: 'CONTRACT', override_id: id(15), historic_version: 12 },
    pay_day: 21.35, charge_day: 32.8, pay_night: 27.11, charge_night: 44.03,
    pay_sat: 29, charge_sat: 45, pay_sun: 33, charge_sun: 49, pay_bh: 41, charge_bh: 59,
    hours_day: '6.2500', hours_night: 1.5, hours_sat: 0, hours_sun: null, hours_bh: '0.00', total_hours: '7.7500',
    total_pay_ex_vat: '177.1400', total_charge_ex_vat: '271.0450', margin_ex_vat: '67.2444',
    pay_vat_rate_pct_snapshot: '20.00', pay_vat_amount_snapshot: '35.4280', pay_total_inc_vat_snapshot: '212.5680',
    pay_wtr_rate_pct_snapshot: '12.07',
    expenses_pay_ex_vat: '2.01', expenses_charge_ex_vat: '3.02', travel_pay_ex_vat: '4.03', travel_charge_ex_vat: '5.04',
    accommodation_pay_ex_vat: '6.05', accommodation_charge_ex_vat: '7.06', other_pay_ex_vat: '8.07', other_charge_ex_vat: '9.08',
    mileage_units: '11.2500', mileage_pay_rate: '0.4500', mileage_charge_rate: '0.5500',
    mileage_pay_ex_vat: '5.0625', mileage_charge_ex_vat: '6.1875',
    additional_pay_ex_vat: '12.3500', additional_charge_ex_vat: '14.0000', additional_margin_ex_vat: '-0.2087',
    additional_units_json: { ONCALL: { code: 'ONCALL', bucket_name: 'On call', unit_name: 'visits', unit_count: '2.50',
      pay_rate: '4.9400', charge_rate: '5.6000', pay_ex_vat: '12.3500', charge_ex_vat: '14.0000',
      margin_ex_vat: '-0.2087', pay_cost_ex_vat_including_erni: '14.2087', days: { '2026-10-12': '2.50' } } },
    invoice_breakdown_json: { mode: 'AGGREGATE', base_hours: {
      day: '6.2500', night: 1.5, sat: 0, sun: null, bh: '0.00',
      pay_rates: { day: 21.35, night: 27.11, sat: 29, sun: 33, bh: 41 },
      charge_rates: { day: 32.8, night: 44.03, sat: 45, sun: 49, bh: 59 },
      pay_ex_vat: '164.7900', charge_ex_vat: '257.0450'
    }, additional: { units: {}, pay_ex_vat: '12.3500', charge_ex_vat: '14.0000', margin_ex_vat: '-0.2087' },
    totals: { total_pay_ex_vat: '177.1400', total_charge_ex_vat: '271.0450', margin_ex_vat: '67.2444', vat_amount: '54.2090', total_inc_vat: '325.2540' } },
    worked_start_iso: '2026-10-12T20:00:00+01:00', worked_end_iso: '2026-10-13T04:00:00+01:00', break_minutes: 15,
    authorised_at_utc: '2026-10-12T10:00:00Z', authorised_by_user_id: I.actor,
    paid_at_utc: '2026-10-13T10:00:00Z', paid_by_user_id: I.actor, payment_reference: 'historic-payment',
    locked_by_invoice_id: I.invoice, locked_at_utc: '2026-10-13T12:00:00Z',
    created_at: '2026-10-12T10:00:00Z', computed_at_utc: '2026-10-12T10:00:00Z'
  };
}
function prepareFixture() {
  return {
    ok: true, contract_version: 'TIMESHEET_CORRECTION_PREPARE_V1', prepare_fingerprint: H,
    candidate_id: I.candidate, client_id: I.client, original_materialisation_required: false, original_authorisation_required: false,
    authority: { request_id: I.request, state_revision: 1, target_revision: H, proposal_revision: 1,
      root_timesheet_id: I.root, origin: 'SIGNED_TIMESHEET_CORRECTION', original_timesheet_id: I.original,
      original_timesheet_version: 7, original_financial_id: I.originalFin, original_financial_authority_hash: FH,
      replacement_timesheet_id: I.p0, replacement_version: 3, replacement_financial_id: null,
      replacement_financial_authority_hash: null, replacement_schedule: [{ work_date: '2026-10-12', start_time: '20:00', end_time: '04:00' }],
      replacement_signed_witness: { workflow_id: id(16), workflow_generation: 2, approval_request_id: id(17),
        hours_component_id: id(18), candidate_component_id: id(19), manager_component_id: id(20),
        candidate_sha256: H, manager_sha256: FH, manager_approved_at_utc: '2026-10-13T11:20:00Z', document_version_id: null },
      planned_reversal_timesheet_id: I.r0, planned_reversal_week_id: null, original_invoice_id: I.invoice, return_invoice_id: null },
    frozen_original: { timesheet: { timesheet_id: I.original, version: 7, booking_id: 'CANDIDATE-WEEKLY-original' },
      financial_authority_hash: FH, financial_snapshot: finance(), planned_reversal_timesheet_id: I.r0 },
    original_current_snapshot: finance(), replacement_current_snapshot: null,
    replacement_timesheet: { timesheet_id: I.p0, version: 3, parent_timesheet_id: I.root, booking_id: 'bk_awc_exact_p0',
      correction_id: I.correction, correction_kind: 'WHOLE_TIMESHEET_REPLACEMENT', is_current: true, archived_at_utc: null, authorised_at_server: null }
  };
}
function replacementFixture() {
  const f = finance(I.p0, 3);
  for (const k of ['id', 'authorised_at_utc', 'authorised_by_user_id', 'paid_at_utc', 'paid_by_user_id', 'payment_reference', 'locked_by_invoice_id', 'locked_at_utc']) delete f[k];
  return f;
}
function appliedFixture(overrides = {}) {
  const members = [I.r0, I.p0];
  return { ok: true, state: 'APPLIED', request_id: I.request, correction_id: I.correction,
    reversed_timesheet_id: I.original, reversal_timesheet_id: I.r0, replacement_timesheet_id: I.p0,
    state_revision: 2, prepare_fingerprint: H, original_authorisation_required: false,
    original_timesheet_authority_hash: H, reversal_timesheet_authority_hash: H, replacement_timesheet_authority_hash: H,
    original_financial_id: I.originalFin, original_financial_authority_hash: FH,
    reversal_financial_id: I.rFin, reversal_financial_authority_hash: H, replacement_financial_id: I.pFin, replacement_financial_authority_hash: H,
    ordinary_notification_timesheet_ids: [I.p0],
    authorisation: { ok: true, all_success: true, success_count: 2, failure_count: 0,
      results: members.map(current_timesheet_id => ({ current_timesheet_id, action: 'AUTHORISE', success: true })) }, ...overrides };
}
function cancelledFixture(overrides = {}) {
  return { ok: true, state: 'CANCELLED', request_id: I.request, state_revision: 2, replacement_timesheet_id: I.p0,
    delete_operation_id: I.deleteOp, retained_immutable_component_asset_count: 3,
    lawful_removal: { ok: true, decision: 'PERMANENT_DELETE', apply_performed: true, deleted_timesheet_ids: [I.p0],
      r2_cleanup_keys: ['exact/p0/mutable.pdf'] }, ...overrides };
}
function harness(options = {}) {
  const calls = [], prepared = options.prepare ?? prepareFixture(), applied = options.applied ?? appliedFixture();
  const dependencies = {
    assertActor: async (_, action) => { calls.push(['actor', action]); return { actor_user_id: I.actor, environment: 'TEST' }; },
    rpc: async (name, args) => {
      calls.push([name, copy(args)]);
      if (options.rpc) return options.rpc(name, args);
      if (name === 'timesheet_correction_prepare_v1') return copy(prepared);
      if (name === 'timesheet_correction_authorise_atomic_v1') return copy(applied);
      if (name === 'timesheet_correction_cancel_atomic_v1') return copy(options.cancelled ?? cancelledFixture());
      if (name === 'timesheet_correction_return_to_draft_v1') return copy(options.returned ?? { ok: true, not_required: true });
      if (name === 'timesheet_correction_send_v1') return { ok: true, request_id: I.request, state: 'WAITING', state_revision: 1,
        replacement_timesheet_id: I.p0, replacement_booking_id: 'bk_awc_exact_p0', target_revision: H, proposal_revision: 1 };
      assert.fail(`Unexpected RPC ${name}`);
    },
    buildReplacementSnapshot: async ({ prepare }) => { calls.push(['canonical', prepare.authority.replacement_timesheet_id]); return replacementFixture(); },
    readRequestAuthority: async () => {
      calls.push(['request-authority']);
      return copy(options.authority ?? { request_id: I.request, state: 'WAITING', state_revision: 1, replacement_timesheet_id: I.p0, return_invoice_id: null });
    },
    deleteCommittedTimesheetR2Keys: async (result, opts) => {
      calls.push(['native-cleanup', copy(result), copy(opts)]);
      return { ok: true, delete_operation_id: opts.deleteOperationId, requested_timesheet_id: opts.requestedTimesheetId,
        deleted_timesheet_ids: result.deleted_timesheet_ids, requested_key_count: result.r2_cleanup_keys.length,
        attempted_key_count: result.r2_cleanup_keys.length, deleted_key_count: result.r2_cleanup_keys.length,
        already_missing_key_count: 0, deferred_key_count: 0, durable_retry_recorded: true,
        r2_cleanup_complete: true, deleted_keys: result.r2_cleanup_keys, deferred_keys: [] };
    },
    resolveLifecycleItem: async ({ timesheetId }) => { calls.push(['resolve', timesheetId]); return null; },
    runOrdinaryLifecycle: async ({ items, action }) => {
      calls.push(['ordinary', copy(items), action]);
      return { ok: true, all_success: true, failure_count: 0, results: items.map(i => ({ success: true, current_timesheet_id: i.timesheet_id })) };
    },
    ...options.dependencies
  };
  return { calls, dependencies, adapter: createTimesheetCorrectionOrchestrator(dependencies) };
}
const rejects = (fn, code) => assert.rejects(fn, e => e instanceof CorrectionOrchestrationError && e.code === code);

test('AGGREGATE reversal negates exact decimals, all original money/quantities, ERNI and VAT; preserves rates/identity', () => {
  const source = finance(), before = copy(source);
  const r = cloneFrozenCorrectionReversal(source, { timesheetId: I.r0, version: 1, candidateId: I.candidate, clientId: I.client });
  assert.deepEqual(source, before);
  assert.equal(r.timesheet_id, I.r0); assert.equal(r.timesheet_version, 1);
  assert.equal(r.invoice_breakdown_json.base_hours.day, '-6.2500');
  assert.equal(r.invoice_breakdown_json.base_hours.night, -1.5);
  assert.equal(r.invoice_breakdown_json.base_hours.sun, null);
  assert.equal(r.invoice_breakdown_json.base_hours.pay_rates.day, source.pay_day);
  assert.equal(r.invoice_breakdown_json.totals.vat_amount, '-54.2090');
  assert.equal(r.invoice_breakdown_json.totals.total_inc_vat, '-325.2540');
  assert.equal(r.additional_units_json.ONCALL.pay_cost_ex_vat_including_erni, '-14.2087');
  assert.equal(r.additional_units_json.ONCALL.days['2026-10-12'], '-2.50');
  assert.equal(r.additional_units_json.ONCALL.margin_ex_vat, '0.2087');
  assert.equal(r.mileage_units, '-11.2500'); assert.equal(r.mileage_pay_rate, source.mileage_pay_rate);
  for (const k of ['expenses_pay_ex_vat', 'travel_charge_ex_vat', 'accommodation_pay_ex_vat', 'other_charge_ex_vat', 'pay_vat_amount_snapshot']) assert.equal(r[k], `-${source[k]}`);
  assert.deepEqual(copy(r.policy_snapshot_json), source.policy_snapshot_json);
  assert.deepEqual(copy(r.rate_source_refs_json), source.rate_source_refs_json);
  assert.equal(r.worked_start_iso, source.worked_start_iso); assert.equal(r.break_minutes, source.break_minutes);
  for (const k of ['id', 'is_current', 'paid_at_utc', 'payment_reference', 'authorised_at_utc', 'locked_by_invoice_id']) assert.equal(Object.hasOwn(r, k), false);
});

test('SEGMENTS reversal preserves exact component order, IDs, historical rates and refs while clearing only new-leg locks', () => {
  const s = finance();
  s.invoice_breakdown_json = { mode: 'SEGMENTS', segments: [
    { segment_id: 'part-b', row_key: 'stable-b', ref_num: '00199', work_date: '2026-10-12', rate_version: 5,
      pay_day: '22.9500', charge_day: '35.0000', hours_day: '4.2500', pay_ex_vat: '97.5375', charge_ex_vat: '148.7500',
      pay_cost_ex_vat_including_erni: '112.2169', vat_amount: '29.7500', total_inc_vat: '178.5000',
      invoice_locked_invoice_id: I.invoice, pay_locked: true, pay_locked_at_utc: '2026-10-13T12:00:00Z', is_reversal: false },
    { segment_id: 'part-a', work_date: '2026-10-13', pay_day: '22.95', hours_day: -1, pay_amount: '-22.95', total_hours: -1 }
  ] };
  const r = cloneFrozenCorrectionReversal(s, { timesheetId: I.r0, version: 1, candidateId: I.candidate, clientId: I.client });
  assert.deepEqual(r.invoice_breakdown_json.segments.map(x => x.segment_id), ['part-b', 'part-a']);
  const b = r.invoice_breakdown_json.segments[0];
  assert.equal(b.ref_num, '00199'); assert.equal(b.pay_day, '22.9500'); assert.equal(b.rate_version, 5);
  assert.equal(b.pay_cost_ex_vat_including_erni, '-112.2169'); assert.equal(b.vat_amount, '-29.7500');
  assert.equal(b.is_reversal, true); assert.equal(Object.hasOwn(b, 'pay_locked'), false);
  assert.equal(r.invoice_breakdown_json.segments[1].hours_day, 1);
  assert.equal(r.invoice_breakdown_json.segments[1].is_reversal, true);
});

test('decimal negation preserves exponent precision and rejects unsafe or future unsupported quantity facts', () => {
  const s = finance(); s.hours_day = '1.234500e+02';
  assert.equal(cloneFrozenCorrectionReversal(s, { timesheetId: I.r0, version: 1, candidateId: I.candidate, clientId: I.client }).hours_day, '-1.234500e+02');
  s.invoice_breakdown_json.segments = [{ newly_added_pay_amount: '1.00' }];
  assert.throws(() => cloneFrozenCorrectionReversal(s, { timesheetId: I.r0, version: 1, candidateId: I.candidate, clientId: I.client }), /CORRECTION_REVERSAL_UNSUPPORTED_FACT/);
  s.hours_day = Number.MAX_SAFE_INTEGER + 1;
  assert.throws(() => cloneFrozenCorrectionReversal(s, { timesheetId: I.r0, version: 1, candidateId: I.candidate, clientId: I.client }), /CORRECTION_FINANCIAL_NUMBER_UNSAFE/);
});

test('authorise invokes actor, read-only PREPARE, existing canonical P0 producer then one native atomic complete-unit call', async () => {
  const { adapter, calls } = harness();
  const receipt = await adapter.authorise(action(), {});
  assert.deepEqual(calls.map(x => x[0]), ['actor', 'timesheet_correction_prepare_v1', 'canonical', 'timesheet_correction_authorise_atomic_v1']);
  const args = calls.at(-1)[1];
  assert.deepEqual(Object.keys(args).sort(), ['p_actor_user_id', 'p_expected_state_revision', 'p_prepared', 'p_request_id']);
  assert.deepEqual(Object.keys(args.p_prepared).sort(), ['prepare_fingerprint', 'replacement_snapshot', 'reversal_snapshot']);
  assert.equal(args.p_prepared.reversal_snapshot.mileage_units, '-11.2500');
  assert.equal(args.p_prepared.reversal_snapshot.timesheet_id, I.r0);
  assert.deepEqual(receipt.ordinary_notification_timesheet_ids, [I.p0]);
  assert.equal(calls.some(x => /enqueue|write|bulk|notification|cleanup/.test(x[0])), false);
});

test('A0 with existing finance remains a genuine three-member native approval, including exact original identity', async () => {
  const p = prepareFixture(); p.original_authorisation_required = true;
  const applied = appliedFixture({ original_authorisation_required: true });
  applied.authorisation.success_count = 3;
  applied.authorisation.results.push({ current_timesheet_id: I.original, action: 'AUTHORISE', success: true });
  await harness({ prepare: p, applied }).adapter.authorise(action(), {});
});

test('missing A0 finance holds before any producer or mutation; calculator output is never assumed to equal stored native output', async () => {
  const p = prepareFixture(); p.original_materialisation_required = true; p.original_authorisation_required = true;
  p.original_current_snapshot = null; p.authority.original_financial_id = null; p.authority.original_financial_authority_hash = null;
  const { adapter, calls } = harness({ prepare: p });
  await rejects(() => adapter.authorise(action(), {}), 'CORRECTION_ORIGINAL_NORMALISATION_UNPROVED');
  assert.deepEqual(calls.map(x => x[0]), ['actor', 'timesheet_correction_prepare_v1']);
});

test('user financial/origin/actor/derived-target injection refuses before auth or any RPC across action payloads', async () => {
  for (const field of ['p_prepared', 'original_snapshot', 'replacement_snapshot', 'reversal_snapshot', 'origin', 'actor_user_id', 'replacement_timesheet_id', 'return_invoice_id']) {
    const { adapter, calls } = harness();
    for (const method of ['authorise', 'prepare', 'cancel', 'returnToDraft']) {
      await rejects(() => adapter[method]({ ...action(), [field]: 'injected' }, {}), 'CORRECTION_PAYLOAD_FIELD_NOT_ALLOWED');
    }
    assert.equal(calls.length, 0);
  }
});

test('SEND accepts actual closed Candidate breaks and non-UUID business booking receipt; preserves immutable proposal exactly', async () => {
  const { adapter, calls } = harness();
  const p = { timesheet_id: I.original, expected_version: 7, request_key: 'send-key', reason: 'Correct signed hours',
    proposal: { schedule: [
      { segment_id: 'one', work_date: '2026-10-12', start_time: '20:00', end_time: '04:00', break_entry: { kind: 'NO_BREAK', no_break: true, break_minutes: 0 } },
      { segment_id: 'two', work_date: '2026-10-13', break_entry: { kind: 'DURATION_MINUTES', break_minutes: 15 } },
      { segment_id: 'three', work_date: '2026-10-14', break_entry: { kind: 'START_END_TIMES', break_start: '23:50', break_end: '00:20', calculated_break_minutes: 30 } }
    ], changed_dates: ['2026-10-12'] } };
  await adapter.send(p, {});
  assert.deepEqual(calls.at(-1)[1].p_request, p);
  await rejects(() => adapter.send({ ...p, origin: 'AGENCY_INVOICE_CORRECTION' }, {}), 'CORRECTION_PAYLOAD_FIELD_NOT_ALLOWED');
  const invalid = copy(p); invalid.proposal.schedule[0].pay_rate = 25;
  await rejects(() => adapter.send(invalid, {}), 'CORRECTION_PAYLOAD_FIELD_NOT_ALLOWED');
});

test('SEND terminal replay does not require fields absent from native replay or produce finance/notifications', async () => {
  const { adapter, calls } = harness({ rpc: () => ({ ok: true, request_id: I.request, state: 'APPLIED', state_revision: 2, replacement_timesheet_id: I.p0, replayed: true }) });
  const result = await adapter.send({ timesheet_id: I.original, expected_version: 7, request_key: 'same-key', reason: 'Same', proposal: { schedule: [] } }, {});
  assert.equal(result.replayed, true); assert.equal(calls.length, 2); assert.equal(result.ordinary_notification_timesheet_ids, undefined);
});

test('actor denial and environment mismatch have no service/financial side effects', async () => {
  const { adapter, calls } = harness({ dependencies: { assertActor: async () => ({ actor_user_id: I.actor, environment: 'LIVE' }) } });
  await rejects(() => adapter.authorise(action(), {}), 'CORRECTION_ENVIRONMENT_NOT_ALLOWED');
  assert.equal(calls.length, 0);
});

test('stale CAS, replacement wrong identity and malformed prepare identity stop before the atomic RPC', async () => {
  const h = harness();
  await rejects(() => h.adapter.authorise(action(9), {}), 'CORRECTION_REQUEST_CAS_STALE');
  assert.equal(h.calls.some(x => x[0] === 'canonical'), false);
  const wrong = harness({ dependencies: { buildReplacementSnapshot: async () => finance(I.original, 7) } });
  await rejects(() => wrong.adapter.authorise(action(), {}), 'CORRECTION_CANONICAL_SNAPSHOT_INVALID');
  assert.equal(wrong.calls.some(x => x[0] === 'timesheet_correction_authorise_atomic_v1'), false);
  const p = prepareFixture(); p.replacement_timesheet.booking_id = p.frozen_original.timesheet.booking_id;
  await rejects(() => harness({ prepare: p }).adapter.authorise(action(), {}), 'CORRECTION_PREPARE_AUTHORITY_INVALID');
});

test('existing original is immutable even to the injected producer callback', async () => {
  const h = harness({ dependencies: { buildReplacementSnapshot: async ({ prepare }) => {
    assert.throws(() => { prepare.original_current_snapshot.total_pay_ex_vat = '1.00'; }, TypeError);
    return replacementFixture();
  } } });
  await h.adapter.authorise(action(), {});
});

test('returned owner failure preserves its exact bounded code and invokes no follow-up', async () => {
  const h = harness({ applied: { ok: false, error_code: 'CORRECTION_COMPLETE_UNIT_AUTHORISATION_FAILED' } });
  await rejects(() => h.adapter.authorise(action(), {}), 'CORRECTION_COMPLETE_UNIT_AUTHORISATION_FAILED');
  assert.equal(h.calls.length, 4);
});

test('committed receipt must bind exact original/reversal/P0, fingerprint, hashes, P0-only notifications and complete member identities', async () => {
  const mutations = [
    r => { r.reversal_timesheet_id = id(90); }, r => { r.original_financial_authority_hash = H; },
    r => { r.prepare_fingerprint = FH; }, r => { r.ordinary_notification_timesheet_ids = [I.original, I.p0]; },
    r => { r.authorisation.results[1].current_timesheet_id = I.original; }, r => { r.authorisation.results[0].success = false; }
  ];
  for (const mutate of mutations) {
    const r = appliedFixture(); mutate(r);
    await assert.rejects(() => harness({ applied: r }).adapter.authorise(action(), {}), CorrectionOrchestrationError);
  }
});

test('APPLIED replay uses the durable saved receipt without rebuilding or reauthorising; contradictory Cancel cannot masquerade as approval', async () => {
  const replay = appliedFixture({ replayed: true }); delete replay.authorisation;
  const h = harness({ prepare: replay });
  await h.adapter.authorise(action(), {});
  assert.deepEqual(h.calls.map(x => x[0]), ['actor', 'timesheet_correction_prepare_v1']);
  await rejects(() => harness({ prepare: cancelledFixture({ replayed: true }) }).adapter.authorise(action(), {}), 'CORRECTION_REQUEST_NOT_APPLIED');
});

test('pending signed Cancel reads only narrow request authority, performs native cancel, then delegates exact filtered committed keys', async () => {
  const h = harness(); const result = await h.adapter.cancel(action(), {});
  assert.deepEqual(h.calls.map(x => x[0]), ['actor', 'request-authority', 'timesheet_correction_cancel_atomic_v1', 'native-cleanup']);
  const cleanup = h.calls.at(-1);
  assert.deepEqual(cleanup[1].r2_cleanup_keys, ['exact/p0/mutable.pdf']);
  assert.deepEqual(cleanup[2], { deleteOperationId: I.deleteOp, requestedTimesheetId: I.p0 });
  assert.equal(result.state, 'CANCELLED');
});

test('Cancel wrong state, wrong target, returned failure, incomplete removal, foreign deleted member or unverified keys never invokes cleanup', async () => {
  const variants = [
    cancelledFixture({ replacement_timesheet_id: I.original }),
    { ok: false, error_code: 'CORRECTION_CANCEL_COMPLETE_UNIT_FAILED' },
    cancelledFixture({ lawful_removal: { ...cancelledFixture().lawful_removal, apply_performed: false } }),
    cancelledFixture({ lawful_removal: { ...cancelledFixture().lawful_removal, deleted_timesheet_ids: [I.p0, I.original] } }),
    cancelledFixture({ lawful_removal: { ...cancelledFixture().lawful_removal, r2_cleanup_keys: [null] } }),
    appliedFixture({ replayed: true })
  ];
  for (const cancelled of variants) {
    const h = harness({ cancelled });
    await assert.rejects(() => h.adapter.cancel(action(), {}), CorrectionOrchestrationError);
    assert.equal(h.calls.some(x => x[0] === 'native-cleanup'), false);
  }
  const applied = harness({ authority: { request_id: I.request, state: 'APPLIED', state_revision: 2, replacement_timesheet_id: I.p0, return_invoice_id: null } });
  await rejects(() => applied.adapter.cancel(action(), {}), 'CORRECTION_REQUEST_NOT_WAITING');
  assert.equal(applied.calls.some(x => x[0] === 'timesheet_correction_cancel_atomic_v1'), false);
});

test('lawful archive Cancel validates exact archived P0 and does not call R2 cleanup', async () => {
  const h = harness({ cancelled: cancelledFixture({ lawful_removal: { ok: true, action: 'ARCHIVE', decision: 'ARCHIVED', archived_count: 1, timesheet_ids: [I.p0] } }) });
  await h.adapter.cancel(action(), {});
  assert.equal(h.calls.some(x => x[0] === 'native-cleanup'), false);
});

test('Cancel replay may resume only the existing cleanup owner with same committed delete-operation identity', async () => {
  const h = harness({ authority: { request_id: I.request, state: 'CANCELLED', state_revision: 2, replacement_timesheet_id: I.p0, return_invoice_id: null },
    cancelled: cancelledFixture({ replayed: true }) });
  await h.adapter.cancel(action(), {});
  assert.equal(h.calls.at(-1)[2].deleteOperationId, I.deleteOp);
});

test('cleanup runtime failure reports committed cancellation with pending follow-up; missing owner holds before SQL mutation', async () => {
  const h = harness({ dependencies: { deleteCommittedTimesheetR2Keys: async () => { throw new Error('storage unavailable'); } } });
  const r = await h.adapter.cancel(action(), {});
  assert.equal(r.state, 'CANCELLED'); assert.equal(r.cleanup.cleanup_pending, true);
  const missing = harness({ dependencies: { deleteCommittedTimesheetR2Keys: undefined } });
  await rejects(() => missing.adapter.cancel(action(), {}), 'CORRECTION_COMMITTED_CLEANUP_OWNER_UNAVAILABLE');
  assert.equal(missing.calls.some(x => x[0] === 'timesheet_correction_cancel_atomic_v1'), false);
});

test('missing narrow authority holds Cancel and Return without substituting finalized Prepare', async () => {
  const h = harness({ dependencies: { readRequestAuthority: undefined } });
  await rejects(() => h.adapter.cancel(action(), {}), 'CORRECTION_REQUEST_AUTHORITY_UNPROVED');
  await rejects(() => h.adapter.returnToDraft(action(2), {}), 'CORRECTION_REQUEST_AUTHORITY_UNPROVED');
  assert.equal(h.calls.some(x => /timesheet_correction/.test(x[0])), false);
});

test('actual service request-read wire decimal CAS admits pending Cancel and exact applied Return', async () => {
  const wire = { contract_version: 'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1', request_id: I.request,
    state: 'WAITING', state_revision: '1', replacement_timesheet_id: I.p0, return_invoice_id: null };
  const cancel = harness({ authority: wire });
  assert.equal((await cancel.adapter.cancel(action(), {})).state, 'CANCELLED');
  assert.equal(cancel.calls.find(x => x[0] === 'timesheet_correction_cancel_atomic_v1')[1].p_expected_state_revision, 1);
  const returned = harness({ authority: { ...wire, state: 'APPLIED', state_revision: '2' } });
  assert.deepEqual(await returned.adapter.returnToDraft(action(2), {}), { ok: true, not_required: true });
});

test('request-read wire refuses unknown versions, noncanonical CAS and unsafe integers before mutation', async () => {
  const wire = { contract_version: 'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1', request_id: I.request,
    state: 'WAITING', state_revision: '1', replacement_timesheet_id: I.p0, return_invoice_id: null };
  const changes = [{ contract_version: 'UNKNOWN' }, { state_revision: 1 }, { state_revision: '01' },
    { state_revision: '1.0' }, { state_revision: '-1' }, { state_revision: '9007199254740992' },
    { original_snapshot: {} }];
  for (const change of changes) {
    const h = harness({ authority: { ...wire, ...change } });
    await rejects(() => h.adapter.cancel(action(), {}), 'CORRECTION_REQUEST_AUTHORITY_UNPROVED');
    assert.equal(h.calls.some(x => x[0] === 'timesheet_correction_cancel_atomic_v1'), false);
  }
});

test('Return binds exact terminal request CAS and same invoice, retaining truthful pending and empty/no-op receipts', async () => {
  const authority = { request_id: I.request, state: 'APPLIED', state_revision: 2, replacement_timesheet_id: I.p0, return_invoice_id: I.invoice };
  for (const returned of [
    { ok: true, request_id: I.request, action: 'RETURN', return_invoice_id: I.invoice, return_pending: false },
    { ok: true, return_invoice_id: I.invoice, empty_replacement: true, return_pending: false },
    { ok: true, return_invoice_id: I.invoice, replayed: true, return_pending: false },
    { ok: true, return_invoice_id: I.invoice, return_pending: true, blocker_code: 'AGENCY_INVOICE_TIMESHEET_RETURNING' }
  ]) {
    const h = harness({ authority, returned });
    assert.deepEqual(await h.adapter.returnToDraft(action(2), {}), returned);
    assert.deepEqual(h.calls.map(x => x[0]), ['actor', 'request-authority', 'timesheet_correction_return_to_draft_v1']);
  }
  await rejects(() => harness({ authority }).adapter.returnToDraft(action(), {}), 'CORRECTION_REQUEST_CAS_STALE');
  await rejects(() => harness({ authority, returned: { ok: true, return_invoice_id: id(90), return_pending: false, replayed: true } }).adapter.returnToDraft(action(2), {}), 'CORRECTION_RETURN_RECEIPT_INVALID');
  const none = { ...authority, return_invoice_id: null };
  assert.deepEqual(await harness({ authority: none }).adapter.returnToDraft(action(2), {}), { ok: true, not_required: true });
});

test('mixed lifecycle resolves all entries before mutation, de-duplicates request/P0 aliases and preserves ordinary native owner', async () => {
  const h = harness({ dependencies: { resolveLifecycleItem: async ({ timesheetId }) => {
    h.calls.push(['resolve', timesheetId]);
    return timesheetId === id(99) ? null : { kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId,
      request_id: I.request, state: 'WAITING', state_revision: 1, replacement_timesheet_id: I.p0, reversal_timesheet_id: null };
  } } });
  const r = await h.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.p0 }, { row_key: `timesheet:${I.p0}` }, { timesheet_id: id(99), expected_timesheet_id: id(99) }] }, {});
  assert.equal(r.all_success, true); assert.equal(r.results.length, 2);
  assert.equal(h.calls.filter(x => x[0] === 'timesheet_correction_authorise_atomic_v1').length, 1);
  assert.ok(h.calls.findIndex(x => x[0] === 'canonical') > h.calls.findLastIndex(x => x[0] === 'resolve'));
  assert.deepEqual(h.calls.find(x => x[0] === 'ordinary')[1], [{ timesheet_id: id(99), expected_timesheet_id: id(99) }]);
  assert.deepEqual(r.ordinary_notification_timesheet_ids, [I.p0]);
});

test('APPLIED R0/P0 lifecycle aliases expand once using native complete transition items and preserve expected-version fences', async () => {
  const transition_items = [I.r0, I.p0].map(timesheet_id => ({ timesheet_id, expected_timesheet_id: timesheet_id,
    expected_version: timesheet_id === I.r0 ? 1 : 3, lifecycle_group_id: `correction:${I.correction}`, lifecycle_group_size: 2 }));
  const h = harness({ dependencies: { resolveLifecycleItem: async ({ timesheetId }) => ({ kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId,
    request_id: I.request, state: 'APPLIED', state_revision: 2, replacement_timesheet_id: I.p0, reversal_timesheet_id: I.r0,
    expected_member_count: 2, transition_items }) } });
  await h.adapter.runLifecycle({ action: 'UNAUTHORISE', items: [{ timesheet_id: I.r0 }, { timesheet_id: I.p0 }] }, {});
  assert.deepEqual(h.calls.find(x => x[0] === 'ordinary')[1], transition_items);
  assert.equal(h.calls.some(x => x[0] === 'timesheet_correction_prepare_v1'), false);
});

test('resolver contradictions and client group/finance injection refuse before mutations', async () => {
  const h = harness();
  await rejects(() => h.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.p0, current_timesheet_id: I.original }] }, {}), 'CANDIDATE_CONTEXT_STALE');
  await rejects(() => h.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.p0, lifecycle_group_size: 3 }] }, {}), 'CORRECTION_PAYLOAD_FIELD_NOT_ALLOWED');
  assert.equal(h.calls.length, 0);
  const conflicting = harness({ dependencies: { resolveLifecycleItem: async ({ timesheetId }) => ({ kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId,
    request_id: I.request, state: 'WAITING', state_revision: timesheetId === I.p0 ? 1 : 2, replacement_timesheet_id: I.p0, reversal_timesheet_id: I.r0 }) } });
  await rejects(() => conflicting.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.p0 }, { timesheet_id: I.r0 }] }, {}), 'CORRECTION_REQUEST_CAS_STALE');
  assert.equal(conflicting.calls.some(x => x[0].startsWith('timesheet_correction_')), false);
});

test('mixed native returned failure is per-unit and never erases already committed correction receipt or claims global atomicity', async () => {
  const h = harness({ dependencies: {
    resolveLifecycleItem: async ({ timesheetId }) => timesheetId === I.p0 ? { kind: 'CORRECTION_REQUEST', selected_timesheet_id: I.p0,
      request_id: I.request, state: 'WAITING', state_revision: 1, replacement_timesheet_id: I.p0 } : null,
    runOrdinaryLifecycle: async () => ({ ok: false, all_success: false, failure_count: 1, results: [{ success: false, error_code: 'LOCK_TIMEOUT' }] })
  } });
  const r = await h.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.p0 }, { timesheet_id: id(99) }] }, {});
  assert.equal(r.all_success, false); assert.equal(r.results[0].success, true);
  assert.equal(r.results[0].receipt.state, 'APPLIED'); assert.equal(r.results[1].success, false);
  assert.deepEqual(r.ordinary_notification_timesheet_ids, [I.p0]);
});

test('separate repeated correction requests sharing a root remain distinct units; one failure does not erase or skip the other', async () => {
  const second = { request: id(21), p0: id(22), r0: id(23), original: I.p0, originalFin: I.pFin, pFin: id(24), rFin: id(25), correction: `awc:${I.root}:${id(21)}` };
  const transform = (input, mapping) => {
    const walk = v => Array.isArray(v) ? v.map(walk) : v && typeof v === 'object'
      ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, walk(x)]))
      : typeof v === 'string' && Object.hasOwn(mapping, v) ? mapping[v] : v;
    return walk(input);
  };
  const mapping = Object.fromEntries(Object.entries(second).map(([k, v]) => [I[k], v]));
  const p2 = transform(prepareFixture(), mapping), r2 = transform(appliedFixture(), mapping);
  let firstFails = false;
  const h = harness({
    rpc: (name, args) => {
      const isSecond = args.p_request_id === second.request;
      if (name === 'timesheet_correction_prepare_v1') {
        if (!isSecond && firstFails) throw new Error('CORRECTION_ORIGINAL_SIGNATURE_CHANGED');
        return copy(isSecond ? p2 : prepareFixture());
      }
      if (name === 'timesheet_correction_authorise_atomic_v1') return copy(isSecond ? r2 : appliedFixture());
      assert.fail(name);
    }, dependencies: {
      resolveLifecycleItem: async ({ timesheetId }) => ({ kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId,
        request_id: timesheetId === second.p0 ? second.request : I.request, root_timesheet_id: I.root,
        state: 'WAITING', state_revision: 1, replacement_timesheet_id: timesheetId }),
      buildReplacementSnapshot: async ({ prepare }) => {
        const s = replacementFixture(); s.timesheet_id = prepare.authority.replacement_timesheet_id;
        return s;
      }
    }
  });
  const payload = { action: 'AUTHORISE', items: [{ timesheet_id: I.p0 }, { timesheet_id: second.p0 }] };
  const result = await h.adapter.runLifecycle(payload, {});
  assert.equal(result.results.length, 2); assert.equal(result.all_success, true);
  assert.deepEqual(result.ordinary_notification_timesheet_ids, [I.p0, second.p0]);
  assert.equal(h.calls.filter(x => x[0] === 'timesheet_correction_authorise_atomic_v1').length, 2);
  firstFails = true;
  const partial = await h.adapter.runLifecycle(payload, {});
  assert.equal(partial.results[0].success, false); assert.equal(partial.results[0].code, 'CORRECTION_ORIGINAL_SIGNATURE_CHANGED');
  assert.equal(partial.results[1].receipt.state, 'APPLIED');
  assert.deepEqual(partial.ordinary_notification_timesheet_ids, [second.p0]);
});

test('unverified or foreign cleanup summary cannot claim cleanup complete after a committed Cancel', async () => {
  const h = harness({ dependencies: { deleteCommittedTimesheetR2Keys: async () => ({ ok: true, delete_operation_id: id(90), r2_cleanup_complete: true }) } });
  const r = await h.adapter.cancel(action(), {});
  assert.equal(r.state, 'CANCELLED'); assert.equal(r.cleanup.cleanup_pending, true);
  assert.equal(r.cleanup.r2_cleanup_complete, undefined);
});

test('original A0 selected with exact request P0 de-duplicates into native three-member unit; original alone cannot apply correction', async () => {
  const p = prepareFixture(); p.original_authorisation_required = true;
  const r = appliedFixture({ original_authorisation_required: true });
  r.authorisation.success_count = 3;
  r.authorisation.results.push({ current_timesheet_id: I.original, success: true, action: 'AUTHORISE' });
  const h = harness({ prepare: p, applied: r, dependencies: { resolveLifecycleItem: async ({ timesheetId, selectedTimesheetIds }) => {
    assert.deepEqual(selectedTimesheetIds, [I.original, I.p0]);
    return { kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId, request_id: I.request,
      state: 'WAITING', state_revision: 1, original_timesheet_id: I.original,
      original_authorisation_required: true, replacement_timesheet_id: I.p0, reversal_timesheet_id: null };
  } } });
  const outcome = await h.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.original }, { timesheet_id: I.p0 }] }, {});
  assert.equal(outcome.results.length, 1); assert.equal(outcome.results[0].receipt.authorisation.success_count, 3);
  assert.equal(h.calls.some(x => x[0] === 'ordinary'), false);
  const originalOnly = harness({ dependencies: { resolveLifecycleItem: async ({ timesheetId }) => ({
    kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId, request_id: I.request,
    state: 'WAITING', state_revision: 1, original_timesheet_id: I.original, original_authorisation_required: true,
    replacement_timesheet_id: I.p0, reversal_timesheet_id: null
  }) } });
  await rejects(() => originalOnly.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.original }] }, {}), 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
  assert.equal(originalOnly.calls.some(x => x[0] === 'timesheet_correction_prepare_v1'), false);
});

test('returned actor, narrow-reader or resolver failure cannot be overwritten by plausible identity fields', async () => {
  const authority = { request_id: I.request, state: 'WAITING', state_revision: 1, replacement_timesheet_id: I.p0, return_invoice_id: null };
  const actorDenied = harness({ dependencies: { assertActor: async () => ({ actor_user_id: I.actor, environment: 'TEST', ok: false, error_code: 'CORRECTION_ACTOR_NOT_ALLOWED' }) } });
  await rejects(() => actorDenied.adapter.authorise(action(), {}), 'CORRECTION_ACTOR_NOT_ALLOWED');
  assert.equal(actorDenied.calls.length, 0);
  const readFailed = harness({ dependencies: { readRequestAuthority: async () => ({ ...authority, ok: false, error_code: 'CORRECTION_REQUEST_CAS_STALE' }) } });
  await rejects(() => readFailed.adapter.cancel(action(), {}), 'CORRECTION_REQUEST_CAS_STALE');
  assert.equal(readFailed.calls.some(x => x[0] === 'timesheet_correction_cancel_atomic_v1'), false);
  const resolveFailed = harness({ dependencies: { resolveLifecycleItem: async ({ timesheetId }) => ({ ...authority,
    kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId, ok: false, error_code: 'CORRECTION_LATEST_POSITIVE_REQUIRED' }) } });
  await rejects(() => resolveFailed.adapter.runLifecycle({ action: 'AUTHORISE', items: [{ timesheet_id: I.p0 }] }, {}), 'CORRECTION_LATEST_POSITIVE_REQUIRED');
  assert.equal(resolveFailed.calls.some(x => x[0] === 'timesheet_correction_prepare_v1'), false);
});

test('native digest target and request-bound awc text work for the unsigned agency origin without manager evidence invention', async () => {
  const p = prepareFixture(); p.authority.origin = 'AGENCY_INVOICE_CORRECTION'; p.authority.replacement_signed_witness = null;
  const h = harness({ prepare: p });
  const receipt = await h.adapter.authorise(action(), {});
  assert.equal(receipt.correction_id, I.correction);
  assert.equal(h.calls.filter(x => x[0] === 'timesheet_correction_authorise_atomic_v1').length, 1);
});

test('numeric, malformed, coercible and unknown-proposal witnesses refuse before canonical production or mutation', async () => {
  for (const value of [2, '2', H.toUpperCase(), H.slice(1), null, {}, [H]]) {
    const p = prepareFixture(); p.authority.target_revision = value;
    const h = harness({ prepare: p });
    await rejects(() => h.adapter.authorise(action(), {}), 'CORRECTION_PREPARE_AUTHORITY_INVALID');
    assert.deepEqual(h.calls.map(x => x[0]), ['actor','timesheet_correction_prepare_v1']);
  }
  for (const value of [0, 2, '1']) {
    const p = prepareFixture(); p.authority.proposal_revision = value;
    await rejects(() => harness({ prepare: p }).adapter.authorise(action(), {}), 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  }
});

test('prepared P0 correction identity must name this request and exact immutable root', async () => {
  for (const correction_id of [id(12), 'awc:' + I.root + ':' + id(90), 'awc:' + id(90) + ':' + I.request]) {
    const p = prepareFixture(); p.replacement_timesheet.correction_id = correction_id;
    const h = harness({ prepare: p });
    await rejects(() => h.adapter.authorise(action(), {}), 'CORRECTION_RECEIPT_IDENTITY_MISMATCH');
    assert.deepEqual(h.calls.map(x => x[0]), ['actor','timesheet_correction_prepare_v1']);
  }
});

test('APPLIED replay rejects a fabricated UUID or another request awc identity without rebuilding finance', async () => {
  for (const correction_id of [id(12), 'awc:' + I.root + ':' + id(90)]) {
    const r = appliedFixture({ replayed: true, correction_id }); delete r.authorisation;
    const h = harness({ prepare: r });
    await rejects(() => h.adapter.authorise(action(), {}), 'CORRECTION_RECEIPT_IDENTITY_MISMATCH');
    assert.deepEqual(h.calls.map(x => x[0]), ['actor','timesheet_correction_prepare_v1']);
  }
});

test('SEND rejects fabricated numeric target and changed immutable proposal revision, including supplied terminal replay witnesses', async () => {
  const payload = { timesheet_id: I.original, expected_version: 7, request_key: 'native-shape-send', reason: 'Correct hours', proposal: { schedule: [] } };
  for (const fields of [{ target_revision: 2 }, { target_revision: [H] }, { proposal_revision: 2 },
    { replayed: true, state: 'APPLIED', state_revision: 2, target_revision: 2 },
    { replayed: true, state: 'APPLIED', state_revision: 2, proposal_revision: 2 }]) {
    const h = harness({ rpc: () => ({ ok:true, request_id:I.request, state:'WAITING', state_revision:1,
      replacement_timesheet_id:I.p0, replacement_booking_id:'bk_awc_exact_p0', target_revision:H, proposal_revision:1, ...fields }) });
    await rejects(() => h.adapter.send(payload, {}), 'CORRECTION_SEND_RECEIPT_INVALID');
    assert.equal(h.calls.length, 2);
  }
});
