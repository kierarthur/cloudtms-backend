import assert from 'node:assert/strict';
import test from 'node:test';
import { projectWeeklyProtectedComponentIdentity as project,
  PROTECTED_COMPONENT_IDENTITY_VERSION } from '../../broker/src/weekly-source/protected-component-identity.mjs';

const a = '11111111-1111-4111-8111-111111111111';
const b = '22222222-2222-4222-8222-222222222222';
function fixture({ id = a, end = '17:00', root = a, state = 'WAIT' } = {}) {
  const schedule = [{ date: '2026-09-01', start: '09:00', end, break_mins: 30,
    work_event_id: id, protected_target_state: state }];
  const calculation = { ok: true, snapshot: { timesheet_id: root,
    total_pay_ex_vat: 75, total_charge_ex_vat: 150, policy_snapshot_json: { immutable: true },
    invoice_breakdown_json: { mode: 'SEGMENTS', totals: { pay: 75, charge: 150 },
      segments: [{ segment_id: `ts:${root}:${end}`, date: '2026-09-01', start: '09:00',
        end, overnight: false, break_mins: 30, hours_day: 2, hours_night: 1,
        hours_sat: 1, hours_sun: 1, hours_bh: 2.5, pay_amount: 75, charge_amount: 150,
        exclude_from_pay: false, breaks: [], ref_num: 'frozen-ref' }] } } };
  return { calculation, schedule, approvedComponents: [] };
}
const segments = value => value.snapshot.invoice_breakdown_json.segments;
const canonical = id => ({ component_kind: 'WORKED_TIME', economic_key_type: 'SEGMENT',
  economic_key_value: `weekly-source-event:${id}`, component_member_identity: id });
const refuses = (input, code) => assert.throws(() => project(input), { code });

test('fresh projection changes identity only; never invents imported Source or money evidence', () => {
  const input = fixture(); const before = structuredClone(input);
  const output = project(input);
  const { segment_id, weekly_protected_component_identity, ...unchanged } = segments(output)[0];
  const { segment_id: old, ...original } = segments(input.calculation)[0];
  assert.deepEqual(unchanged, original);
  assert.equal(segment_id, `weekly-source-event:${a}`);
  assert.deepEqual(weekly_protected_component_identity,
    { schema_version: PROTECTED_COMPONENT_IDENTITY_VERSION, work_event_id: a });
  assert.equal(Object.hasOwn(segments(output)[0], 'weekly_source'), false);
  assert.deepEqual(input, before);
  assert.deepEqual({ ...output.snapshot, invoice_breakdown_json: undefined },
    { ...input.calculation.snapshot, invoice_breakdown_json: undefined });
  assert.deepEqual(output.snapshot.invoice_breakdown_json.totals,
    input.calculation.snapshot.invoice_breakdown_json.totals);
});
test('same event retains identity through end/break/rate-bucket changes and physical root reuse', () => {
  const initial = project(fixture());
  const amend = fixture({ end: '16:00', root: b });
  amend.approvedComponents = [canonical(a)];
  amend.schedule[0].break_mins = 0;
  Object.assign(segments(amend.calculation)[0], { break_mins: 0, hours_day: 0,
    hours_night: 7, pay_amount: 65, charge_amount: 130 });
  const changed = project(amend);
  assert.equal(segments(changed)[0].segment_id, segments(initial)[0].segment_id);
  assert.equal(segments(changed)[0].pay_amount, 65);
  assert.equal(segments(changed)[0].hours_night, 7);
});
test('distinct same-clock events keep separate identities; five buckets remain one event segment', () => {
  const input = fixture(); const other = fixture({ id: b });
  input.schedule.push(other.schedule[0]);
  segments(input.calculation).push(segments(other.calculation)[0]);
  const output = segments(project(input));
  assert.equal(output.length, 2);
  assert.notEqual(output[0].segment_id, output[1].segment_id);
  assert.equal(output[0].hours_bh, 2.5);
});
test('identity projection introduces no new per-root manifest-size limit', () => {
  const input = fixture(); input.schedule = []; segments(input.calculation).length = 0;
  for (let ordinal = 1; ordinal <= 101; ordinal++) {
    const id = `${ordinal.toString(16).padStart(8, '0')}-3333-4333-8333-333333333333`;
    const item = fixture({ id });
    input.schedule.push(item.schedule[0]);
    segments(input.calculation).push(segments(item.calculation)[0]);
  }
  assert.equal(segments(project(input)).length, 101);
});
test('genuine zero then same event reintroduction needs no historical-key lookup', () => {
  const empty = fixture(); empty.schedule = []; segments(empty.calculation).length = 0;
  empty.approvedComponents = [canonical(a)];
  assert.deepEqual(segments(project(empty)), []);
  assert.equal(segments(project(fixture()))[0].segment_id, `weekly-source-event:${a}`);
});
test('canonical prior tuple and genuine existing Source metadata are retained', () => {
  const input = fixture({ state: 'SOURCE' }); input.approvedComponents = [canonical(a)];
  segments(input.calculation)[0].weekly_source = { work_event_id: a,
    pay_vector: { rates: { day: '10.000000' } }, actual_import_provenance: 'retained' };
  const source = structuredClone(segments(input.calculation)[0].weekly_source);
  assert.deepEqual(segments(project(input))[0].weekly_source, source);
});
test('noncanonical already-used identity is refused, never renamed or aggregated', () => {
  for (const patch of [{ economic_key_value: 'ts:old:clock-hash' },
    { component_member_identity: 'ts:old:clock-hash' }, { economic_key_type: 'DAY' }]) {
    const input = fixture(); input.approvedComponents = [{ ...canonical(a), ...patch }];
    refuses(input, 'WEEKLY_PROTECTED_COMPONENT_LEGACY_IDENTITY_UNAVAILABLE');
  }
});
test('unknown/incomplete manifests and repeated prior/event identities are refused', () => {
  const unknown = fixture(); delete unknown.approvedComponents;
  refuses(unknown, 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_INVALID');
  const missing = fixture(); segments(missing.calculation).length = 0;
  refuses(missing, 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_NOT_EXACT');
  const duplicate = fixture(); duplicate.schedule.push({ ...duplicate.schedule[0] });
  segments(duplicate.calculation).push({ ...segments(duplicate.calculation)[0] });
  refuses(duplicate, 'WEEKLY_PROTECTED_COMPONENT_EVENT_DUPLICATE');
  const repeatedPrior = fixture(); repeatedPrior.approvedComponents = [canonical(a), canonical(a)];
  refuses(repeatedPrior, 'WEEKLY_PROTECTED_COMPONENT_EVENT_DUPLICATE');
});
test('output association must match exact accepted invocation ordinal; no matching fallback', () => {
  for (const patch of [{ date: '2026-09-02' }, { end: '16:00' }, { break_mins: 0 }]) {
    const input = fixture(); Object.assign(segments(input.calculation)[0], patch);
    refuses(input, 'WEEKLY_PROTECTED_COMPONENT_OUTPUT_NOT_EXACT');
  }
});
test('conflicting Source metadata or a second projection is refused', () => {
  const conflicting = fixture(); segments(conflicting.calculation)[0].weekly_source = { work_event_id: b };
  refuses(conflicting, 'WEEKLY_PROTECTED_COMPONENT_SOURCE_IDENTITY_CONFLICT');
  const duplicate = fixture(); duplicate.calculation = project(duplicate);
  refuses(duplicate, 'WEEKLY_PROTECTED_COMPONENT_ALREADY_PROJECTED');
});
