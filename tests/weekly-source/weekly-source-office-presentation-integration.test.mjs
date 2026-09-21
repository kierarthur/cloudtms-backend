import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { weeklySourceOfficePresentationInternals } from '../../broker/src/index.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const ACTOR_ID = '81000000-0000-4000-8000-000000000001';
const TIMESHEET_ID = '81000000-0000-4000-8000-000000000002';

test('attaches only the explicit current Weekly Source presentation contract', async () => {
  const calls = [];
  const presentation = {
    contract: 'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1',
    scope: 'WEEKLY',
    record_version: 'revision-1',
    freshness: 'CURRENT',
    route: 'NHSP',
    action_state: { authorise_allowed: true },
  };
  const result = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
    {},
    { timesheet: { timesheet_id: TIMESHEET_ID, sheet_scope: 'WEEKLY' } },
    ACTOR_ID,
    async (...args) => {
      calls.push(args);
      return { weekly_source_office_timesheet_presentation_v1: presentation };
    },
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0][1], 'weekly_source_office_timesheet_presentation_v1');
  assert.deepEqual(calls[0][2], {
    p_request: { actor_user_id: ACTOR_ID, timesheet_id: TIMESHEET_ID },
  });
  assert.deepEqual(result.weekly_source_presentation, presentation);
});

test('Daily and non-applicable records remain on their existing presentation path', async () => {
  let calls = 0;
  const daily = { timesheet: { timesheet_id: TIMESHEET_ID, sheet_scope: 'DAILY' } };
  const dailyResult = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
    {}, daily, ACTOR_ID, async () => { calls += 1; },
  );
  assert.equal(calls, 0);
  assert.deepEqual(dailyResult, daily);

  const ordinary = { timesheet: { timesheet_id: TIMESHEET_ID, sheet_scope: 'WEEKLY' } };
  const ordinaryResult = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
    {}, ordinary, ACTOR_ID, async () => ({ applicable: false }),
  );
  assert.equal(ordinaryResult.weekly_source_presentation, undefined);
});

test('a source-applicable projection failure blocks authorisation instead of falling back to legacy UI', async () => {
  const result = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
    {},
    {
      current_timesheet_id: TIMESHEET_ID,
      sheet_scope: 'WEEKLY',
      is_import_authoritative: true,
    },
    ACTOR_ID,
    async () => { throw new Error('projection unavailable'); },
  );

  assert.equal(result.weekly_source_presentation.contract, 'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1');
  assert.equal(result.weekly_source_presentation.freshness, 'UNAVAILABLE');
  assert.equal(result.weekly_source_presentation.action_state.authorise_allowed, false);
});

test('Simple and Bulk Authorise both attach the same server-owned projection', () => {
  const worker = fs.readFileSync(path.join(ROOT, 'broker/src/index.js'), 'utf8');
  assert.match(worker, /attachWeeklySourceOfficeTimesheetPresentation\(\s*env,\s*candidatePresentedDetailsPayload,\s*user\.id/);
  const bulkStart = worker.indexOf('async function handleTimesheetBulkAuthoriseContext');
  const bulkEnd = worker.indexOf('\nasync function ', bulkStart + 50);
  const bulkSource = worker.slice(bulkStart, bulkEnd > bulkStart ? bulkEnd : undefined);
  assert.match(bulkSource, /attachWeeklySourceOfficeTimesheetPresentation\(env, payload, user\.id\)/);
  assert.doesNotMatch(bulkSource, /pay_workbench|pay_batch_create|banking\/pay/);
});
