const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const sql = fs.readFileSync(
  path.join(__dirname, '../supabase/migrations/23092026_0345_weekly_source_stage8_initial_policy_test_repair.sql'),
  'utf8',
);

test('Stage 8 repair is exact-client TEST-only and fails closed on history', () => {
  assert.match(sql, /v_client_id constant uuid := '7ead2058-a6b1-417c-aa71-d05c8b56cbd2'/);
  assert.match(sql, /v_group_id constant uuid := '81829cdc-f7e2-4f72-b65c-895502f5f207'/);
  assert.match(sql, /source_group\.environment='TEST'/);
  assert.match(sql, /source_group\.source_family='NHSP'/);
  assert.match(sql, /v_membership_count<>1 or v_policy_count<>1/);
  assert.match(sql, /WEEKLY_SOURCE_STAGE8_BASELINE_REPAIR_HISTORY_UNSAFE/);
  assert.match(sql, /v_membership\.created_at_utc<>v_policy\.created_at_utc/);
});

test('Stage 8 repair changes only the two first-policy date boundaries', () => {
  assert.match(sql, /update public\.weekly_source_group_clients\s+set valid_from=date '1900-01-01'/);
  assert.match(sql, /update public\.weekly_source_client_policies\s+set effective_from=date '1900-01-01'/);
  assert.doesNotMatch(sql, /delete\s+from/i);
  assert.doesNotMatch(sql, /update public\.timesheets/i);
  assert.doesNotMatch(sql, /update public\.timesheets_financials/i);
});
