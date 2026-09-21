import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const helperPath = new URL('../../supabase/repeatable/15092026_1534_01_weekly_source_summary_delay_presentation_v1.sql', import.meta.url);
const summaryPath = new URL('../../supabase/repeatable/19012026_extras.sql', import.meta.url);
const lower = (value) => value.toLowerCase();

test('weekly validation delay is presentation-only and cannot mutate pay or invoices', async () => {
  const source = lower(await readFile(helperPath, 'utf8'));
  assert.match(source, /create or replace function private\.weekly_source_summary_pay_delayed_v1/);
  assert.match(source, /language sql[\s\S]*?stable/);
  assert.match(source, /source_authority/);
  assert.match(source, /timesheet_authority/);
  assert.doesNotMatch(source, /(insert into|update|delete from) public\.(timesheets|timesheets_financials|invoices|invoice_lines|pay_workbench|banking_pay)/);
  assert.match(source, /revoke all on function private\.weekly_source_summary_pay_delayed_v1[\s\S]*?authenticated/);
});

test('Timesheet Summary preserves its status and adds one exact reason plus one filter', async () => {
  const source = lower(await readFile(summaryPath, 'utf8'));
  assert.match(source, /private\.weekly_source_summary_pay_delayed_v1/);
  assert.match(source, /candidate payment is waiting for final weekly source validation\./);
  assert.match(source, /v_issues_filter = 'weekly_source_pay_waiting'/);
  assert.match(source, /not in \('paid','partially_paid','processing','advanced','overpaid'\)/);
  assert.match(
    source,
    /weekly_source_summary_pay_delayed_v1\(\s*source_rows\.timesheet_id,\s*coalesce\(timesheet_row\.contract_id, contract_week_row\.contract_id\),\s*source_rows\.client_id,\s*source_rows\.week_ending_date\s*\)/,
  );
  assert.doesNotMatch(source, /weekly_source_summary_pay_delayed_v1\(\s*source_rows\.timesheet_id,\s*source_rows\.contract_id,/);
});
