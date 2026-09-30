import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const sqlUrl = new URL('../../supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql', import.meta.url);

test('requested-week exception is enforced at selection, rendering and render assertion', async () => {
  const sql = await readFile(sqlUrl, 'utf8');
  const helper = sql.slice(sql.indexOf('create or replace function private.weekly_source_manager_due_event_covers_v1'), sql.indexOf('create or replace function private.weekly_source_query_require_service_v1'));
  for (const condition of [
    'incident.candidate_id=cohort.candidate_id',
    'incident.source_cycle_id=cohort.source_cycle_id',
    "requested.state='SUBMITTED_WITH_ISSUES'",
    'requested.client_id=incident.client_id',
    'requested.contract_id=comparison.contract_id',
    'work_event.work_date>requested.week_ending-7',
    'work_event.work_date<=requested.week_ending',
    "incident.candidate_action_state='NOT_REQUIRED'",
  ]) assert.ok(helper.includes(condition), `missing scope condition: ${condition}`);
  assert.equal(sql.split('private.weekly_source_manager_due_event_covers_v1(event.id,incident.id)').length - 1, 4);
});

test('missing-week requests and hours questions retain separate current pointers', async () => {
  const sql = await readFile(sqlUrl, 'utf8');
  assert.match(sql, /generation\.request_kind='SUBMIT_TIMESHEET'\s+then cohort\.current_submission_generation_id else cohort\.current_generation_id end/);
  const migration = await readFile(new URL('../../supabase/migrations/30092026_2059_weekly_source_independent_candidate_requests.sql', import.meta.url), 'utf8');
  assert.match(migration, /candidate_cohort_id,request_kind/);
  assert.match(migration, /current_submission_generation_id/);
});
