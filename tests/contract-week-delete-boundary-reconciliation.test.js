import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const historicalPlannedDelete = fs.readFileSync(
  new URL('../supabase/repeatable/16122025_timesheet_qr_refuse_restore_planned_delete_audit_rpc.sql', import.meta.url),
  'utf8'
);
const historicalWeeklyDelete = fs.readFileSync(
  new URL('../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql', import.meta.url),
  'utf8'
);
const weeklyDelete = fs.readFileSync(
  new URL('../supabase/repeatable/29082026_1914_contract_week_delete_boundary_reconciliation.sql', import.meta.url),
  'utf8'
);
const plannedDelete = weeklyDelete;
const calendarFeed = fs.readFileSync(
  new URL('../supabase/repeatable/14122025_calendar_rpc_feeds.sql', import.meta.url),
  'utf8'
);

test('contract calendar feed includes an exact unassigned contract', () => {
  assert.match(calendarFeed, /where c\.candidate_id is not distinct from \$1/i);
  assert.match(calendarFeed, /where f\.contract_id = \$1/i);
});

for (const [label, sql] of [
  ['planned Contract-week deletion', plannedDelete],
  ['weekly Timesheet-chain deletion', weeklyDelete]
]) {
  test(`${label} reconciles only a removed boundary week`, () => {
    assert.match(sql, /for update/i);
    assert.match(sql, /min\(cw\.week_ending_date\), max\(cw\.week_ending_date\)/i);
    assert.match(sql, /v_first_remaining_week > v_start_week/i);
    assert.match(sql, /v_last_remaining_week < v_end_week/i);
    assert.match(sql, /jsonb_array_elements\(coalesce\(remaining\.planned_schedule_json, '\[\]'::jsonb\)\)/i);
    assert.match(sql, /coalesce\(v_first_remaining_planned_date, v_first_remaining_week - 6\)/i);
    assert.match(sql, /coalesce\(v_last_remaining_planned_date, v_last_remaining_week\)/i);
    assert.match(sql, /CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE/i);
  });
}

test('weekly deletion returns the reconciled Contract dates to the caller', () => {
  assert.doesNotMatch(historicalPlannedDelete, /CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE/i);
  assert.doesNotMatch(historicalWeeklyDelete, /CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE/i);
  assert.match(weeklyDelete, /private\.contract_week_delete_planned_base_v1/i);
  assert.match(weeklyDelete, /set schema private/i);
  assert.match(weeklyDelete, /private\.timesheet_weekly_chain_delete_apply_base_v1/i);
  assert.match(weeklyDelete, /'contract_dates', jsonb_build_object\(/i);
  assert.match(weeklyDelete, /'changed', v_new_start is distinct from v_contract_start\s+or v_new_end is distinct from v_contract_end/i);
});
