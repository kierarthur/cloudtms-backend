import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const sql = fs.readFileSync(new URL(
  '../supabase/repeatable/31082026_0317_timesheet_cross_record_overlap_guard_v1.sql',
  import.meta.url
), 'utf8');
const verification = fs.readFileSync(new URL(
  '../supabase/verification/31082026_0322_timesheet_cross_record_overlap_guard_verification.sql',
  import.meta.url
), 'utf8');
const candidateBackend = fs.readFileSync(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
const officeBackend = fs.readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');

test('one private indexed guard owns Candidate and Office overlap admission', () => {
  assert.match(sql, /trg_timesheet_submission_workflow_overlap_biu/i);
  assert.match(sql, /trg_timesheet_authorisation_overlap_biu/i);
  assert.match(sql, /pg_catalog\.pg_advisory_xact_lock\s*\(\s*pg_catalog\.hashtextextended/i);
  assert.match(sql, /idx_tsfin_candidate_current|timesheets_financials\s+f[\s\S]*f\.candidate_id=p_candidate_id/i);
  assert.match(sql, /candidate_submission_workflows[\s\S]*w\.candidate_id=p_candidate_id/i);
});

test('strict interval comparison allows touching boundaries and covers all schedule shapes', () => {
  assert.match(sql, /proposed\.work_start_utc<existing\.work_end_utc[\s\S]*existing\.work_start_utc<proposed\.work_end_utc/i);
  assert.doesNotMatch(sql, /proposed\.work_start_utc\s*<=\s*existing\.work_end_utc/i);
  for (const field of [
    'actual_schedule_json', 'start_iso', 'end_iso', 'worked_start_iso', 'worked_end_iso',
    'start_utc', 'end_utc', 'start_time', 'end_time', 'Europe/London'
  ]) assert.match(sql, new RegExp(field.replace('/', '\\/'), 'i'));
});

test('same logical booking is excluded while refused and cancelled work does not block', () => {
  assert.match(sql, /linked\.booking_id=p_exclude_booking_id/i);
  assert.match(sql, /t\.booking_id<>p_exclude_booking_id/i);
  assert.match(sql, /w\.id is distinct from p_exclude_workflow_id/i);
  assert.match(sql, /WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT/);
  assert.match(sql, /READY_TO_FINALISE/);
  assert.doesNotMatch(sql.match(/w\.state in \([\s\S]*?\)/i)?.[0] ?? '', /REFUSED|REJECTED|CANCELLED|EXPIRED|SUPERSEDED/);
});

test('guard is fail-closed and browser-inaccessible', () => {
  assert.match(sql, /message='TIMESHEET_WORK_INTERVAL_OVERLAP'/);
  assert.match(sql, /message='TIMESHEET_CANDIDATE_IDENTITY_REQUIRED_FOR_OVERLAP_CHECK'/);
  assert.match(sql, /revoke all on function private\._timesheet_cross_record_overlap_assert_v1/i);
  assert.match(sql, /from anon/i);
  assert.match(sql, /from authenticated/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('rollback-only first-use proof covers Weekly, Daily, Office, replacement and boundary cases', () => {
  for (const marker of [
    'OVERLAPPING_WEEKLY_SUBMISSION_WAS_ACCEPTED',
    'OVERLAPPING_DAILY_SUBMISSION_WAS_ACCEPTED',
    'OVERLAPPING_WEEKLY_DAILY_SUBMISSION_WAS_ACCEPTED',
    'OVERLAPPING_OFFICE_AUTHORISATION_WAS_ACCEPTED',
    'OVERLAPPING_WEEKLY_ROLLBACK_NOT_PROVEN',
    'OVERLAPPING_DAILY_ROLLBACK_NOT_PROVEN',
    'TIMESHEET_OVERLAP_PRIVATE_HELPER_BROWSER_EXECUTABLE'
  ]) assert.match(verification, new RegExp(marker));
  assert.match(verification, /v_schedule_touching/);
  assert.match(verification, /v_workflow_same_family/);
  assert.match(verification, /rollback;\s*$/i);
});

test('Candidate and Office return the exact professional conflict message', () => {
  const message = 'You have already submitted a timesheet containing these hours. This timesheet cannot be accepted.';
  assert.match(candidateBackend, /TIMESHEET_WORK_INTERVAL_OVERLAP/);
  assert.ok(candidateBackend.includes(message));
  assert.match(officeBackend, /TIMESHEET_WORK_INTERVAL_OVERLAP/);
  assert.ok(officeBackend.includes(message));
});
