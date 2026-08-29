import assert from 'node:assert/strict';
import test from 'node:test';

import { candidateOfficeSummaryInternals } from '../broker/src/index.js';

const uuid = (number) => `00000000-0000-4000-8000-${String(number).padStart(12, '0')}`;
const rawCid = 'cid1-68s48hhp6x1k8ba488vk2b9m6gsm8ba168w34ba68n0kcga26x24cc2284mq71';

function dailyReceiptSource(timesheetId, occupantKey = rawCid) {
  return {
    timesheet_id: timesheetId,
    occupant_key_norm: occupantKey,
    sheet_scope: 'DAILY',
    submission_mode: 'MANUAL',
    candidate_submission_route_intent: 'ELECTRONIC',
    is_current: true,
    archived_at_utc: null
  };
}

function fixtureFetch({ timesheets = [], candidates = [], workflows = [] } = {}, calls = []) {
  return async (_env, url) => {
    calls.push(url);
    if (url.includes('/rest/v1/timesheets?')) return { rows: timesheets };
    if (url.includes('/rest/v1/candidates?')) return { rows: candidates };
    if (url.includes('/rest/v1/candidate_submission_workflows?')) return { rows: workflows };
    throw new Error(`Unexpected read route: ${url}`);
  };
}

test('Daily first receipt is presented as the Candidate and intended electronic lifecycle', async () => {
  const timesheetId = uuid(1);
  const result = await candidateOfficeSummaryInternals.attachCandidateDailyOfficePresentation(
    { SUPABASE_URL: 'https://miget.test' },
    [{
      timesheet_id: timesheetId,
      sheet_scope: 'DAILY',
      candidate_name: rawCid,
      route_type: 'DAILY_MANUAL',
      route_display: 'Daily Manual',
      submission_mode: 'MANUAL',
      processing_status_display: 'Processing Delayed'
    }],
    fixtureFetch({
      timesheets: [dailyReceiptSource(timesheetId)],
      candidates: [{
        id: uuid(500),
        key_norm: rawCid,
        display_name: 'Kier Arthur',
        first_name: 'Kier',
        last_name: 'Arthur'
      }],
      workflows: [{
        id: uuid(600),
        target_timesheet_id: timesheetId,
        anchor_timesheet_id: timesheetId,
        state: 'AWAITING_MANAGER_APPROVAL',
        route: 'PHONE',
        generation: 1,
        updated_at_utc: '2026-08-29T15:57:53Z'
      }]
    })
  );

  assert.equal(result.length, 1);
  assert.equal(result[0].candidate_name, 'Kier Arthur');
  assert.equal(result[0].candidate_display_name, 'Kier Arthur');
  assert.equal(result[0].candidate_id, uuid(500));
  assert.equal(result[0].route_type, 'DAILY_ELECTRONIC');
  assert.equal(result[0].route_display, 'Daily Electronic');
  assert.equal(result[0].submission_mode, 'ELECTRONIC');
  assert.equal(result[0].processing_status_display, 'Processing Delayed');
  assert.equal(result[0].candidate_manager_approval_status, 'AWAITING_MANAGER_APPROVAL');
  assert.equal(result[0].candidate_manager_approval_status_display, 'Awaiting manager approval');
  assert.equal(JSON.stringify(result).includes(rawCid), false, 'the Global CID never leaves as display text');
});

test('Daily presentation never exposes an opaque Candidate identity when enrichment is unavailable', async () => {
  const timesheetId = uuid(2);
  const result = await candidateOfficeSummaryInternals.attachCandidateDailyOfficePresentation(
    { SUPABASE_URL: 'https://miget.test' },
    [{
      timesheet_id: timesheetId,
      sheet_scope: 'DAILY',
      candidate_name: rawCid,
      candidate_display_name: rawCid,
      route_type: 'DAILY_MANUAL'
    }],
    fixtureFetch({ timesheets: [], candidates: [], workflows: [] })
  );

  assert.equal(result[0].candidate_name, 'Candidate not yet resolved');
  assert.equal(result[0].candidate_display_name, 'Candidate not yet resolved');
  assert.equal(JSON.stringify(result).includes(rawCid), false);
});

test('genuine Office Daily Manual rows retain their route and status', async () => {
  const timesheetId = uuid(3);
  const result = await candidateOfficeSummaryInternals.attachCandidateDailyOfficePresentation(
    { SUPABASE_URL: 'https://miget.test' },
    [{
      timesheet_id: timesheetId,
      sheet_scope: 'DAILY',
      candidate_name: 'Office Manual Candidate',
      route_type: 'DAILY_MANUAL',
      route_display: 'Daily Manual',
      submission_mode: 'MANUAL',
      processing_status_display: 'Processed'
    }],
    fixtureFetch({
      timesheets: [{
        ...dailyReceiptSource(timesheetId, 'office-manual-candidate'),
        candidate_submission_route_intent: null
      }],
      candidates: []
    })
  );

  assert.equal(result[0].route_type, 'DAILY_MANUAL');
  assert.equal(result[0].route_display, 'Daily Manual');
  assert.equal(result[0].submission_mode, 'MANUAL');
  assert.equal(result[0].processing_status_display, 'Processed');
});

test('Daily presentation batches all reads and performs no mutation request', async () => {
  const rows = Array.from({ length: 101 }, (_, index) => ({
    timesheet_id: uuid(index + 1),
    sheet_scope: 'DAILY',
    candidate_name: `Candidate ${index + 1}`,
    route_type: 'DAILY_MANUAL'
  }));
  const timesheets = rows.map((row, index) => dailyReceiptSource(row.timesheet_id, `candidate-key-${index + 1}`));
  const candidates = rows.map((row, index) => ({
    id: uuid(index + 1001),
    key_norm: `candidate-key-${index + 1}`,
    display_name: `Candidate ${index + 1}`
  }));
  const workflows = rows.map((row, index) => ({
    id: uuid(index + 2001),
    target_timesheet_id: row.timesheet_id,
    anchor_timesheet_id: row.timesheet_id,
    state: index % 2 ? 'AWAITING_MANAGER_APPROVAL' : 'MANAGER_APPROVED',
    updated_at_utc: '2026-08-29T12:00:00Z'
  }));
  const calls = [];

  const result = await candidateOfficeSummaryInternals.attachCandidateDailyOfficePresentation(
    { SUPABASE_URL: 'https://miget.test' },
    rows,
    fixtureFetch({ timesheets, candidates, workflows }, calls)
  );

  assert.equal(result.length, 101);
  assert.equal(calls.filter((url) => url.includes('/timesheets?')).length, 3);
  assert.equal(calls.filter((url) => url.includes('/candidates?')).length, 3);
  assert.equal(calls.filter((url) => url.includes('/candidate_submission_workflows?')).length, 3);
  assert.ok(calls.every((url) => url.startsWith('https://miget.test/rest/v1/')));
  assert.ok(calls.every((url) => !/[?&](?:insert|update|delete|upsert)=/i.test(url)));
  assert.ok(result.every((row) => row.route_type === 'DAILY_ELECTRONIC'));
});

test('newest workflow state wins without calling a withdrawn claim manager approval required', async () => {
  const timesheetId = uuid(4);
  const result = await candidateOfficeSummaryInternals.attachCandidateDailyOfficePresentation(
    { SUPABASE_URL: 'https://miget.test' },
    [{ timesheet_id: timesheetId, sheet_scope: 'DAILY', candidate_name: 'Kier Arthur', route_type: 'DAILY_MANUAL' }],
    fixtureFetch({
      timesheets: [dailyReceiptSource(timesheetId)],
      candidates: [{ id: uuid(500), key_norm: rawCid, display_name: 'Kier Arthur' }],
      workflows: [
        {
          id: uuid(700), target_timesheet_id: timesheetId, state: 'CANCELLED',
          updated_at_utc: '2026-08-29T16:00:00Z'
        },
        {
          id: uuid(701), target_timesheet_id: timesheetId, state: 'AWAITING_MANAGER_APPROVAL',
          updated_at_utc: '2026-08-29T15:00:00Z'
        }
      ]
    })
  );

  assert.equal(result[0].candidate_manager_approval_status, 'CANCELLED');
  assert.equal(result[0].candidate_manager_approval_status_display, 'Candidate submission cancelled');
});

test('a signed submission with no active manager route is manager approval required', () => {
  assert.deepEqual(
    candidateOfficeSummaryInternals.candidateDailyOfficeWorkflowPresentation('READY_FOR_MANAGER_APPROVAL'),
    {
      candidate_manager_approval_status: 'REQUIRED',
      candidate_manager_approval_status_display: 'Manager approval required'
    }
  );
});

test('Daily receipt details use immutable Candidate hours and remain view-only', async () => {
  const timesheetId = uuid(8);
  const result = await candidateOfficeSummaryInternals.attachCandidateDailyOfficeDetailPresentation(
    { SUPABASE_URL: 'https://miget.test' },
    {
      processing_status_display: 'Processing Delayed',
      timesheet: {
        timesheet_id: timesheetId,
        sheet_scope: 'DAILY',
        submission_mode: 'MANUAL',
        candidate_submission_route_intent: 'ELECTRONIC',
        occupant_key_norm: rawCid
      },
      action_flags: { can_edit: true, can_authorise: true }
    },
    fixtureFetch({
      workflows: [{
        id: uuid(88),
        target_timesheet_id: timesheetId,
        anchor_timesheet_id: timesheetId,
        state: 'AWAITING_MANAGER_APPROVAL',
        route: 'PHONE',
        generation: 1,
        work_date: '2026-08-29',
        updated_at_utc: '2026-08-29T15:57:53Z',
        immutable_submission_json: {
          hours_submission: {
            timesheet_patch_json: {
              worked_start_iso: '2026-08-29T06:30:00Z',
              worked_end_iso: '2026-08-29T19:00:00Z',
              break_start_iso: '2026-08-29T12:00:00Z',
              break_end_iso: '2026-08-29T13:00:00Z',
              break_minutes: 60
            }
          }
        }
      }]
    })
  );

  assert.equal(result.processing_status_display, 'Processing Delayed');
  assert.equal(result.route_display, 'Daily Electronic');
  assert.equal(result.timesheet.submission_mode, 'ELECTRONIC');
  assert.equal(result.timesheet.total_hours, 11.5);
  assert.equal(result.timesheet.worked_minutes, 690);
  assert.deepEqual(result.actual_schedule_json, [{
    date: '2026-08-29',
    work_date: '2026-08-29',
    start_time: '07:30',
    end_time: '20:00',
    break_start: '13:00',
    break_end: '14:00',
    break_minutes: 60
  }]);
  assert.equal(result.timesheet.occupant_key_norm, undefined);
  assert.equal(result.action_flags.can_edit, false);
  assert.equal(result.action_flags.can_authorise, false);
  assert.equal(result.action_flags.candidate_manager_approval_pending, true);
  assert.equal(result.candidate_manager_approval_status, 'AWAITING_MANAGER_APPROVAL');
  assert.equal(result.office_candidate_receipt_view_only, true);
  assert.equal(JSON.stringify(result).includes(rawCid), false);
});

test('genuine Daily Manual details are not rewritten', async () => {
  const input = {
    timesheet: {
      timesheet_id: uuid(9),
      sheet_scope: 'DAILY',
      submission_mode: 'MANUAL',
      candidate_submission_route_intent: null
    },
    action_flags: { can_edit: true }
  };
  const result = await candidateOfficeSummaryInternals.attachCandidateDailyOfficeDetailPresentation(
    { SUPABASE_URL: 'https://miget.test' },
    input,
    async () => { throw new Error('Daily Manual must not query Candidate workflow data'); }
  );
  assert.deepEqual(result, input);
});
