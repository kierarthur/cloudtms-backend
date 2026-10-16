import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { sourceNativeJsonbText, hashSourceNativeJson, sourceProtectedCalculationContext,
  buildSourceUnprotectEconomicTarget } from '../../broker/src/weekly-source/source-native-economic.mjs';
import { projectWeeklySourceC1ComponentEconomics, buildWeeklySourceC1Components }
  from '../../broker/src/banking-pay/weekly-source-c1-components.mjs';
import { decodeSourceUnprotectReviewContext, normalizeSourceNativeCalculationContext }
  from '../../broker/src/weekly-source/source-native-contract.mjs';

const id = n => `93000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const hash = 'a'.repeat(64);
function fixture(moved = false) {
  const current = moved ? id(4) : id(2); const contract = moved ? id(5) : id(3);
  const timesheet = { timesheet_id: current, version: 1, contract_id: contract, is_current: true, archived_at_utc: null,
    settings_authority_json: { values: { calculation: 'native', tax: 'PAYE', resolved_at_utc: '2026-10-16T01:02:03Z' } } };
  const currentContract = { id: contract, candidate_id: id(10), client_id: id(11), rates_json: { pay_day: '12.00' } };
  const context = { action: 'WITHDRAW', family_id: id(1), root_timesheet_id: id(2), contract_id: id(3), candidate_id: id(10), client_id: id(11),
    protected_current_placement: { version: 'PROTECTED_CURRENT_PLACEMENT_V1', moved, family_id: id(1), family_root_id: id(2), member_key: id(2),
      original_root_timesheet_id: id(2), original_contract_id: id(3), current_timesheet_id: current, current_contract_id: contract,
      current_financial_id: id(6), current_timesheet_version: 1, current_approval_work_id: null, original_canonical_work_id: null,
      move_operation_id: moved ? id(7) : null, placement_revision: moved ? '9007199254740993' : '0',
      immutable_original_evidence_sha256: hash, source_invoice_certificate_sha256: 'b'.repeat(64), placement_certificate_sha256: 'c'.repeat(64) },
    current_entitlement: { timesheet, contract_record: currentContract, current_financial: { id: id(6) }, current_approval_work_id: null },
    requires_zero_financial: false, policy: { calculation: 'native', tax: 'PAYE' }, work_event_id: id(12),
    provider: { source_pay_method: 'PAYE', target_pay_method: 'PAYE', source_document_sha256: hash },
    source_expenses: [], next_source_segments: null };
  const schedule = [{ date: '2026-10-12', start: '09:00', end: '17:00', break_mins: 30, work_event_id: id(12), protected_target_state: 'WAIT' }];
  const snapshot = { timesheet_id: current, timesheet_version: 1, candidate_id: id(10), client_id: id(11), pay_method: 'PAYE',
    policy_snapshot_json: { calculation: 'native', tax: 'PAYE' }, rate_source_refs_json: { contract_id: contract },
    hours_day: 7.5, pay_day: 12, charge_day: 20, total_pay_ex_vat: 90, total_charge_ex_vat: 150,
    additional_units_json: { BONUS: { unit_count: '2.000000', pay_rate: '3.000000', charge_rate: '5.000000', pay_ex_vat: '6.00', charge_ex_vat: '10.00' } },
    mileage_units: '0.000000', mileage_evidence_manifest: { immutable_document: id(15) }, expenses_pay_ex_vat: 0,
    invoice_breakdown_json: { mode: 'SEGMENTS', segments: [{ date: '2026-10-12', segment_id: 'native-existing-segment',
      segment_key: 'native-key', segment_stable_key: id(12), hours_day: 7.5, hours_night: 0, hours_sat: 0, hours_sun: 0, hours_bh: 0,
      pay_amount: 90, charge_amount: 150, exclude_from_pay: false, weekly_protected_component_identity: { source_component_id: id(16) } }] } };
  return { context, schedule, calculation: { ok: true, snapshot }, sourceObservationSha256: hash,
    quantityPartition: [{ member_key: id(2), supported: { unit: 'MINUTE', quantity: '0' }, excluded: { unit: 'MINUTE', quantity: '450' } }],
    priorCommitmentCutSha256: 'd'.repeat(64) };
}
test('Native JSON hashing uses the existing PostgreSQL domain separator and jsonb byte ordering', async () => {
  const value = { zz: 1e21, a: 1e-7, 'é': '🧑', b: ['quote"', null, -0, true] };
  const expected = '{"a": 0.0000001, "b": ["quote\\\"", null, 0, true], "zz": 1000000000000000000000, "é": "🧑"}';
  assert.equal(sourceNativeJsonbText(value), expected);
  assert.equal(await hashSourceNativeJson('DOMAIN', value), createHash('sha256').update(`DOMAIN\x1f${expected}`).digest('hex'));
  assert.notEqual(await hashSourceNativeJson('OTHER_DOMAIN', value), await hashSourceNativeJson('DOMAIN', value));
});
test('Native JSON hashing rejects unsupported values instead of silently dropping economic data', () => {
  for (const value of [NaN, Infinity, undefined, 1n, { a: undefined }, '\0', '\uD800', new Date(), [, 1]]) {
    assert.throws(() => sourceNativeJsonbText(value));
  }
  const cyclic = {}; cyclic.self = cyclic; assert.throws(() => sourceNativeJsonbText(cyclic));
});
test('Newly authored numeric tokens do not normalize existing native receipt bytes or decimal money strings', async () => {
  const one = JSON.parse('{"value":1}');const scaled = JSON.parse('{"value":1.00}');
  assert.equal(await hashSourceNativeJson('SOURCE_UNPROTECT_CALCULATION_POLICY_V1', one),
    await hashSourceNativeJson('SOURCE_UNPROTECT_CALCULATION_POLICY_V1', scaled));
  assert.notEqual(await hashSourceNativeJson('SOURCE_UNPROTECT_CALCULATION_POLICY_V1', { value: '1' }),
    await hashSourceNativeJson('SOURCE_UNPROTECT_CALCULATION_POLICY_V1', { value: '1.00' }));
  // Exact known native JSONB textual bytes stay distinct. This is a byte
  // fixture, not a claim that PostgreSQL was executed by this unit test.
  for (const domain of ['WEEKLY_PROTECTED_LOCAL_RESULT_V1','SOURCE_NATIVE_COMPOSED_RESULT_V1','SOURCE_NATIVE_LOCAL_RECEIPT_ROW_V1']) {
    assert.notEqual(createHash('sha256').update(`${domain}\x1f{"value": 1}`).digest('hex'),
      createHash('sha256').update(`${domain}\x1f{"value": 1.00}`).digest('hex'));
  }
});
test('Unallocated economics equal the actual native C1 component population without invented IDs', () => {
  const f = fixture(); const input = { root_timesheet_id: id(2), financial_snapshot: f.calculation.snapshot, expenses: [] };
  const normal = buildWeeklySourceC1Components({ ...input, component_id_for_key: () => id(19) });
  const unallocated = projectWeeklySourceC1ComponentEconomics(input);
  assert.equal(normal.length, unallocated.length);
  assert.deepEqual(unallocated, normal.map(({ component, authority }) => {
    const { component_id, component_ordinal, source_ordinal, ...rest } = component;
    void component_id; void component_ordinal; void source_ordinal; return { component: rest, authority };
  }));
  assert.ok(unallocated.every(r => !Object.hasOwn(r.component, 'component_id')));
});
test('Moved native calculation takes actual B inputs while preserving original A/evidence context', () => {
  const f = fixture(true); const before = structuredClone(f.context);
  const current = sourceProtectedCalculationContext(f.context);
  assert.equal(current.root_timesheet_id, id(4)); assert.equal(current.contract_id, id(5));
  assert.equal(current.current_financial.id, id(6)); assert.equal(current.policy.resolved_at_utc, undefined);
  assert.deepEqual(f.context, before);
  assert.throws(() => sourceProtectedCalculationContext({ ...f.context, current_entitlement: { ...f.context.current_entitlement,
    timesheet: { ...f.context.current_entitlement.timesheet, timesheet_id: id(20) } } }));
});
test('Reviewed economics retain the complete native snapshot and all stable evidence/component identities', async () => {
  const f = fixture(true); const result = await buildSourceUnprotectEconomicTarget(f);
  assert.deepEqual(result.economic_target.tsfin_snapshot_json, f.calculation.snapshot);
  assert.equal(result.economic_target.root_timesheet_id, id(2));
  assert.equal(result.economic_target.tsfin_snapshot_json.timesheet_id, id(4));
  assert.equal(result.economic_target.accepted_placement_sha256, 'c'.repeat(64));
  assert.equal(result.economic_target.tsfin_snapshot_json.invoice_breakdown_json.segments[0].weekly_protected_component_identity.source_component_id, id(16));
  for (const mutate of [v => { v.calculation.snapshot.pay_day = 12.01; }, v => { v.calculation.snapshot.additional_units_json.BONUS.unit_count = '3.000000'; },
    v => { v.calculation.snapshot.invoice_breakdown_json.segments[0].charge_amount = 151; }, v => { v.calculation.snapshot.mileage_evidence_manifest.immutable_document = id(18); },
    v => { v.quantityPartition[0].excluded.quantity = '449'; }, v => { v.context.provider.target_pay_method = 'UMBRELLA'; },
    v => { v.context.current_entitlement.contract_record.rates_json.pay_day = '13.00'; }, v => { v.priorCommitmentCutSha256 = 'e'.repeat(64); }]) {
    const changed = fixture(true); mutate(changed);
    assert.notEqual((await buildSourceUnprotectEconomicTarget(changed)).economic_target_sha256, result.economic_target_sha256);
  }
});
test('New run/request transport identity does not change reviewed economics', async () => {
  const one = fixture(); const two = fixture();
  one.context.orchestration_run_id = id(20); two.context.orchestration_run_id = id(21);
  assert.equal((await buildSourceUnprotectEconomicTarget(one)).economic_target_sha256, (await buildSourceUnprotectEconomicTarget(two)).economic_target_sha256);
});
test('Missing prerequisite and an original-A snapshot cannot silently become a moved B target', async () => {
  const f = fixture(true); f.calculation.snapshot.timesheet_id = id(2);
  await assert.rejects(buildSourceUnprotectEconomicTarget(f), { code: 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID' });
  const missing = fixture(); missing.context.requires_zero_financial = true;
  await assert.rejects(buildSourceUnprotectEconomicTarget(missing), { code: 'SOURCE_NATIVE_PREREQUISITE_NOT_PREPARED' });
});

function review() {
  const f = fixture(); const context = { ...f.context, family_bound_version: '9007199254740993', current_target_vector_sha256: hash,
    source_cycle_id: id(22), source_group_id: id(23), agency_id: id(24), contract_week_id: id(25), week_ending_date: '2026-10-18',
    source_mode: 'HEALTHROSTER_WEEKLY', timesheet: f.context.current_entitlement.timesheet, contract_week: { id: id(25) },
    contract_record: f.context.current_entitlement.contract_record, current_financial: f.context.current_entitlement.current_financial,
    approved_component_identity_components: [], protected_schedule: {}, source_segments: [], protected_decisions: [],
    source_proposal: { source_revision: null, source_hash: null }, client_sources: [], candidate_submission: null,
    current_comparison_revision_id: null, current_final_revision_id: null, root_financial: null,
    expected_head_revision: '0', request_sequence: '1', zero_rate_source_refs: {} };
  return { version: 'SOURCE_UNPROTECT_REVIEW_CONTEXT_RESULT_V1', assembly_id: id(26), decision_id: id(27), member_key: id(2),
    source_phase: 'NO_FINAL_REPORT_YET', source_observation_sha256: hash, context_sha256: hash, context };
}
test('Review context is genuinely run-free and normal run context strips only its three response fields', () => {
  const r = review(); assert.equal(decodeSourceUnprotectReviewContext(r), r);
  assert.deepEqual(normalizeSourceNativeCalculationContext({ ...r.context, ok: true, contract: 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1',
    orchestration_run_id: id(28) }), r.context);
  for (const key of ['orchestration_run_id', 'ok', 'arbitrary_economics']) {
    const invalid = review(); invalid.context[key] = id(28); assert.throws(() => decodeSourceUnprotectReviewContext(invalid));
  }
  const fake = review(); fake.context.source_proposal.source_revision = id(29); assert.throws(() => decodeSourceUnprotectReviewContext(fake));
  const final = review(); final.source_phase = 'FINAL_SOURCE_OBSERVED'; assert.throws(() => decodeSourceUnprotectReviewContext(final));
});
