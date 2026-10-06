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

test('additive Source root must match the independently requested physical Timesheet before decoration', async () => {
  for (const root of [TIMESHEET_ID, ACTOR_ID, null, { id: TIMESHEET_ID }]) {
    const supplied = { contract: 'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1', scope: 'WEEKLY',
      root_timesheet_id: root, record_version: 'revision-root', freshness: 'CURRENT',
      action_state: { authorise_allowed: true } };
    const result = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
      {}, { timesheet_id: TIMESHEET_ID, sheet_scope: 'WEEKLY', is_import_authoritative: true },
      ACTOR_ID, async () => supplied);
    if (root === TIMESHEET_ID) assert.deepEqual(result.weekly_source_presentation, supplied);
    else {
      assert.equal(result.weekly_source_presentation.freshness, 'UNAVAILABLE');
      assert.equal(result.weekly_source_presentation.action_state.authorise_allowed, false);
    }
  }
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

test('canonical invoice-delay category reaches all existing detail shapes without archive or pay inference', async () => {
  const category = {
    presentation_category: 'PROCESSING_DELAYED',
    processing_reason: 'Awaiting a valid import for invoicing',
    is_archived: false,
    category_basis: { sha256: 'a'.repeat(64) }
  };
  const payload = { current_timesheet_id: TIMESHEET_ID, sheet_scope: 'WEEKLY',
    timesheet: { timesheet_id: TIMESHEET_ID, archived_at_utc: null },
    row: { ready_to_pay: true }, effective: { pay_due: 75 } };
  const result = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
    {}, payload, ACTOR_ID, async () => ({ contract: 'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1',
      scope: 'WEEKLY', operational_category: category })
  );
  for (const value of [result, result.timesheet, result.row, result.effective]) {
    assert.equal(value.tools_stage, 'PROCESSING_DELAYED');
    assert.equal(value.processing_status_display, 'Processing Delayed');
    assert.deepEqual(value.weekly_source_operational_category, category);
  }
  assert.equal(result.timesheet.archived_at_utc, null);
  assert.equal(result.row.ready_to_pay, true);
  assert.equal(result.effective.pay_due, 75);
  assert.equal(payload.timesheet.tools_stage, undefined, 'input is not mutated');
});

test('no category override preserves the existing Stage and actions', async () => {
  const payload = { current_timesheet_id: TIMESHEET_ID, sheet_scope: 'WEEKLY',
    tools_stage: 'INVOICED', timesheet: { timesheet_id: TIMESHEET_ID, is_archived: false } };
  const result = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
    {}, payload, ACTOR_ID, async () => ({ contract: 'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1',
      scope: 'WEEKLY', operational_category: { presentation_category: null } })
  );
  assert.equal(result.tools_stage, 'INVOICED');
  assert.deepEqual(result.timesheet, { ...payload.timesheet,
    weekly_source_operational_category: null, weekly_source_processing_reason: null });
});

test('explicit no override clears stale category hints in every detail shape, not financial facts', async () => {
  const stale = { weekly_source_operational_category: { presentation_category: 'PROCESSING_DELAYED' },
    weekly_source_processing_reason: 'Awaiting a valid import for invoicing' };
  const payload = { ...stale, current_timesheet_id: TIMESHEET_ID, sheet_scope: 'WEEKLY',
    tools_stage: 'AUTHORISED_FOR_INVOICING', timesheet: { ...stale, pay_on_hold: false },
    row: { ...stale, total_hours: 7.5 }, effective: { ...stale, total_pay_ex_vat: 75 } };
  const result = await weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation(
    {}, payload, ACTOR_ID, async () => ({ contract: 'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1',
      scope: 'WEEKLY', operational_category: { presentation_category: null } }));
  for (const value of [result, result.timesheet, result.row, result.effective]) {
    assert.equal(value.weekly_source_operational_category, null);
    assert.equal(value.weekly_source_processing_reason, null);
  }
  assert.equal(result.tools_stage, 'AUTHORISED_FOR_INVOICING');
  assert.equal(result.row.total_hours, 7.5);
  assert.equal(result.effective.total_pay_ex_vat, 75);
  assert.equal(result.timesheet.pay_on_hold, false);
  assert.notEqual(payload.weekly_source_operational_category, null);
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

test('normal summary gets one exact server snapshot with filters and row order unchanged', async () => {
  const filters = { limit: 100, offset: 17, tools_stage: 'PROCESSING_DELAYED', order_dir: 'desc' };
  const rows = Array.from({ length: 100 }, (_, index) => ({ timesheet_id: 'root-'+index,
    tools_stage: 'PROCESSING_DELAYED', total_hours: index,
    weekly_source_operational_category: { category_basis: { facts: { root_version: index+1 } } } }));
  const calls=[];
  const result=await weeklySourceOfficePresentationInternals.readWeeklySourceOfficeSummaryRows(
    {}, ACTOR_ID, filters, async (...args) => {
      calls.push(args);
      return [{ weekly_source_office_summary_rows_v1:
        { contract: 'WEEKLY_SOURCE_OFFICE_SUMMARY_ROWS_V1', rows } }];
    });
  assert.equal(calls.length,1,'not one RPC per Timesheet');
  assert.equal(calls[0][1],'weekly_source_office_summary_rows_v1');
  assert.deepEqual(calls[0][2],{p_request:{actor_user_id:ACTOR_ID,p_filters:filters}});
  assert.equal(result,rows,'no JavaScript repaging/category/action/financial rewrite');
});

test('summary contract failure cannot silently fall back to old display evidence', async () => {
  for (const bad of [{rows:[]}, {contract:'WEEKLY_SOURCE_OFFICE_SUMMARY_ROWS_V1',rows:null},
    {contract:'WEEKLY_SOURCE_OFFICE_SUMMARY_ROWS_V1',rows:Array(201).fill({})}]) {
    await assert.rejects(weeklySourceOfficePresentationInternals.readWeeklySourceOfficeSummaryRows(
      {}, ACTOR_ID,{limit:100},async()=>bad),/WEEKLY_SOURCE_SUMMARY_CONTRACT_INVALID/);
  }
});

test('normal rows and patches share paired reader, frozen pay-batch stays on its owner', () => {
  const worker=fs.readFileSync(path.join(ROOT,'broker/src/index.js'),'utf8');
  const summary=worker.slice(worker.indexOf('async function handleTimesheetsSummary('),
    worker.indexOf('\nasync function ',worker.indexOf('async function handleTimesheetsSummary(')+50));
  assert.match(summary,/readWeeklySourceOfficeSummaryRows\(env, user\.id, scanFilters\)/);
  assert.match(summary,/readWeeklySourceOfficeSummaryRows\(env, user\.id, rowFilters\)/);
  assert.match(summary,/sbRpc\(env, 'pay_batch_timesheet_summary_lightweight_v1'/);
  assert.doesNotMatch(summary,/sbRpc\(env, 'timesheet_summary_lightweight_rows_v1'/);
});
