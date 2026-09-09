import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const source = fs.readFileSync(
  new URL('../supabase/repeatable/30082026_0125_candidate_submitted_weekly_card_linkage.sql', import.meta.url),
  'utf8'
).toLowerCase();

test('a materialised expense carrier uses its exact workflow owner before the week fallback', () => {
  assert.match(source, /workflow\.target_timesheet_id=carrier\.timesheet_id/);
  assert.match(source, /workflow\.target_timesheet_id is null/);
  assert.match(source, /from public\.candidate_submission_workflows exact_owner/);
  assert.match(source, /exact_owner\.target_timesheet_id=carrier\.timesheet_id/);
  assert.doesNotMatch(
    source,
    /workflow\.target_timesheet_id=carrier\.timesheet_id or workflow\.contract_week_id=carrier\.id/
  );
});
