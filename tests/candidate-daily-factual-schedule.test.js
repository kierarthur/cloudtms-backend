import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { mapCanonicalDailyScheduleToIso } from '../broker/src/daily-schedule-authority.js';

const source = fs.readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const factual = source.slice(source.indexOf('function hasExplicitCandidateZeroBreak('), source.indexOf('async function deterministicCandidateUuid('));
const time = source.slice(source.indexOf('function ukLocalToUtcISO('), source.indexOf('function isBSTLocalDate('));
// Execute the actual production adapter and UK-time owner, not a mocked mapper.
const build = new Function('mapCanonicalDailyScheduleToIso', `${time}\n${factual}\nreturn buildCandidateDailySubmissionThroughCanonicalAuthority;`)(mapCanonicalDailyScheduleToIso);
const workflow = { id: '00000000-0000-4000-8000-000000000962', generation: 1, work_date: '2026-08-28' };
const segment = { date: workflow.work_date, work_date: workflow.work_date, start_time: '07:30', end_time: '20:00', break_start: '13:00', break_end: '13:30', break_minutes: 30 };
const submit = (row = segment, date = workflow.work_date) => build({ workflow: { ...workflow, work_date: date }, factualSubmission: { actual_schedule_json: [row], timesheet_patch_json: { actual_schedule_json: [row] } } });

test('actual app Daily schedule reaches the existing factual adapter in UK time', () => {
  const frozen = JSON.stringify(segment);
  const result = submit();
  assert.equal(result.contract_version, 'CANDIDATE_DAILY_FACTUAL_SUBMISSION_V1');
  assert.deepEqual(result.timesheet_patch_json, {
    worked_start_iso: '2026-08-28T06:30:00.000Z', worked_end_iso: '2026-08-28T19:00:00.000Z',
    break_start_iso: '2026-08-28T12:00:00.000Z', break_end_iso: '2026-08-28T12:30:00.000Z', break_minutes: 30,
    actual_schedule_json: [segment], additional_units_week: {}, additional_units_per_day: {}
  });
  assert.equal(JSON.stringify(segment), frozen);
});

test('Daily mapping handles midnight, winter and explicit no-break without inventing break times', () => {
  const date = '2026-12-12';
  const result = submit({ date, work_date: date, start_time: '20:00', end_time: '08:00', no_break: true, break_minutes: 0 }, date).timesheet_patch_json;
  assert.equal(result.worked_start_iso, '2026-12-12T20:00:00.000Z');
  assert.equal(result.worked_end_iso, '2026-12-13T08:00:00.000Z');
  assert.equal(result.break_minutes, 0);
  assert.equal(result.break_start_iso, null);
  assert.equal(result.break_end_iso, null);
});

test('Daily mapping fails closed for wrong work date, several entries and invalid breaks', () => {
  assert.throws(() => submit({ ...segment, date: '2026-08-29', work_date: '2026-08-29' }), /CANDIDATE_DAILY_SCHEDULE_DATE_MISMATCH/);
  assert.throws(() => build({ workflow, factualSubmission: { actual_schedule_json: [segment, segment] } }), /CANDIDATE_DAILY_SINGLE_INTERVAL_REQUIRED/);
  assert.throws(() => submit({ ...segment, break_start: '06:00', break_end: '06:30' }), /Break window must be fully within/);
  assert.throws(() => submit({ ...segment, start_time: '25:00' }), /must both be valid HH:MM/);
});

test('Daily mapping rejects nonexistent and ambiguous UK clock-change intervals', () => {
  for (const date of ['2026-03-29', '2026-10-25']) {
    assert.throws(() => submit({ date, work_date: date, start_time: '01:30', end_time: '03:00', no_break: true, break_minutes: 0 }, date), /Non-existent local|Ambiguous local/);
  }
});

test('existing exact ISO factual submissions retain their original contract', () => {
  const patch = { worked_start_iso: '2026-08-28T06:30:00.000Z', worked_end_iso: '2026-08-28T19:00:00.000Z', break_minutes: 0, no_break: true, actual_schedule_json: [segment] };
  const result = build({ workflow, factualSubmission: { timesheet_patch_json: patch } });
  assert.equal(result.timesheet_patch_json.worked_start_iso, patch.worked_start_iso);
  assert.equal(result.timesheet_patch_json.break_start_iso, null);
});
