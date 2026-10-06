// Executes the saved calculator without network, database or financial writes.
// The fixture is input only; financial output is produced by the actual owner.
import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import Module from 'node:module';
import { build } from 'esbuild';
import { projectWeeklyProtectedComponentIdentity as project } from '../../broker/src/weekly-source/protected-component-identity.mjs';

const root = path.resolve(import.meta.dirname, '../..');
const source = await readFile(path.join(root, 'broker/src/index.js'), 'utf8');
const bundled = await build({ stdin: {
  contents: `${source}\nexport { buildWeeklyScheduleSegmentsSnapshot };`,
  resolveDir: path.join(root, 'broker/src'), sourcefile: 'identity-calculator-owner.js',
}, bundle: true, write: false, platform: 'node', format: 'cjs', logLevel: 'error' });
const owner = new Module(path.join(root, 'tests/.identity-calculator-owner.cjs'));
owner.filename = path.join(root, 'tests/.identity-calculator-owner.cjs');
owner.paths = Module._nodeModulePaths(root);
owner._compile(bundled.outputFiles[0].text, owner.filename);
const calculate = owner.exports.buildWeeklyScheduleSegmentsSnapshot;
const id = ordinal => `${String(ordinal).padStart(8, '0')}-1111-4111-8111-111111111111`;
const contract = { id: id(90), candidate_id: id(91), client_id: id(92),
  pay_method_snapshot: 'PAYE', rates_json: {
    paye_day: 10, paye_night: 12, paye_sat: 14, paye_sun: 16, paye_bh: 18,
    charge_day: 20, charge_night: 24, charge_sat: 28, charge_sun: 32, charge_bh: 36,
  }, additional_rates_json: {} };
const policy = { timezone_id: 'Europe/London', day_start: '06:00:00', day_end: '20:00:00',
  night_start: '20:00:00', night_end: '06:00:00', sat_start: '00:00:00', sat_end: '00:00:00',
  sun_start: '00:00:00', sun_end: '00:00:00', bh_start: '00:00:00', bh_end: '00:00:00',
  bh_list: ['2026-09-07'], weekly_rate_classification_method: 'SPLIT_RATE_WINDOWS' };
const shift = (ordinal, date, start, end, break_mins = 0) => ({ date, start, end, break_mins,
  work_event_id: id(ordinal), protected_target_state: 'WAIT' });

test('actual calculator preserves exact schedule ordinal through overnight and rate buckets', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => { throw new Error('NETWORK_FORBIDDEN_IN_CALCULATOR_PROOF'); };
  try {
    // Deliberately not date-sorted: proves invocation order, not clock matching.
    const schedule = [shift(1, '2026-09-09', '09:00', '17:00', 30),
      shift(2, '2026-09-11', '19:00', '07:00', 30),
      shift(3, '2026-09-07', '09:00', '17:00'),
      shift(4, '2026-09-13', '09:00', '17:00'),
      shift(5, '2026-09-12', '09:00', '17:00')];
    const ts = { timesheet_id: id(93), version: 1, actual_schedule_json: schedule };
    const raw = await calculate({}, ts, { week_ending_date: '2026-09-13' }, contract, {},
      { write_now: false, policy_override: policy, ignore_locked_segments_for_preview: true });
    assert.equal(raw.ok, true);
    const output = project({ calculation: raw, schedule, approvedComponents: [] });
    const segments = output.snapshot.invoice_breakdown_json.segments;
    assert.equal(segments.length, schedule.length);
    const hours = segment => ['day', 'night', 'sat', 'sun', 'bh']
      .reduce((total, bucket) => total + Number(segment[`hours_${bucket}`] || 0), 0);
    assert.equal(hours(segments[0]), 7.5, 'the actual calculator deducts the 30-minute duration break');
    assert.equal(segments[0].pay_amount, 75, 'approved day pay uses the net 7.5 hours, not the 8-hour span');
    assert.equal(hours(segments[1]), 11.5, 'an overnight duration break is deducted once across rate buckets');
    for (let ordinal = 0; ordinal < schedule.length; ordinal++) {
      const { segment_id, weekly_protected_component_identity, ...actual } = segments[ordinal];
      const { segment_id: unused, ...original } = raw.snapshot.invoice_breakdown_json.segments[ordinal];
      assert.deepEqual(actual, original, 'no calculator output other than identity is changed');
      assert.equal(segment_id, `weekly-source-event:${schedule[ordinal].work_event_id}`);
      assert.equal(weekly_protected_component_identity.work_event_id, schedule[ordinal].work_event_id);
    }
    assert.equal(segments[1].overnight, true);
    assert(segments[1].hours_day > 0 && segments[1].hours_night > 0 && segments[1].hours_sat > 0,
      'one overnight work event keeps multiple genuine calculator buckets');
    assert(segments[2].hours_bh > 0);
    assert(segments[3].hours_sun > 0);
    assert(segments[4].hours_sat > 0);
    assert.deepEqual(output.snapshot.invoice_breakdown_json.totals, raw.snapshot.invoice_breakdown_json.totals);
  } finally { globalThis.fetch = originalFetch; }
});
