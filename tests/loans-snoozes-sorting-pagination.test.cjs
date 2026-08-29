const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const sql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/20072026_0936_loans_snoozes_sort_page_size.sql'),
  'utf8'
);

function sliceBetween(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.ok(start >= 0, `${startMarker} must exist`);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(end > start, `${endMarker} must follow ${startMarker}`);
  return source.slice(start, end);
}

test('Loans / Snoozes endpoint validates and forwards both independent sort contracts', () => {
  const handler = sliceBetween(
    workerSource,
    'async function handleBankingFinanceLoansSnoozesList',
    'async function runInteractiveWorkbenchCandidateFastLane'
  );

  assert.match(handler, /Math\.min\(n, 50\)/);
  assert.match(handler, /return \{ value: 10 \}/);
  assert.match(handler, /allowedFinanceSortKeys = new Set\(\['created_at', 'candidate', 'case_type'\]\)/);
  assert.match(handler, /allowedTimesheetSortKeys = new Set\(\['created_at', 'candidate', 'timesheet'\]\)/);
  assert.match(handler, /sbRpc\(env, 'pay_loans_snoozes_page_v2'/);
  assert.match(handler, /p_timesheet_sort_key: timesheetSortKeyRaw/);
  assert.match(handler, /p_timesheet_sort_dir: timesheetSortDirRaw/);
});

test('database function sorts each full dataset before applying its independent page', () => {
  assert.match(sql, /p_finance_page_size integer default 10/);
  assert.match(sql, /p_timesheet_page_size integer default 10/);
  assert.match(sql, /least\(greatest\(coalesce\(p_finance_page_size, 10\), 1\), 50\)/);
  assert.match(sql, /least\(greatest\(coalesce\(p_timesheet_page_size, 10\), 1\), 50\)/);

  const financeOrder = sql.indexOf("v_finance_sort_key = 'case_type'");
  const financePageCut = sql.indexOf('orr.sort_position > ((v_finance_page - 1) * v_finance_page_size)');
  const timesheetOrder = sql.indexOf("v_timesheet_sort_key = 'timesheet'");
  const timesheetPageCut = sql.indexOf('orr.sort_position > ((v_timesheet_page - 1) * v_timesheet_page_size)');

  assert.ok(financeOrder >= 0 && financePageCut > financeOrder);
  assert.ok(timesheetOrder >= 0 && timesheetPageCut > timesheetOrder);
  assert.match(sql, /'sort_key', v_timesheet_sort_key/);
  assert.match(sql, /'sort_dir', v_timesheet_sort_dir/);
});

test('sortable pagination wrapper remains read/display-only and Policy X safe', () => {
  assert.match(sql, /v_base := public\.pay_loans_snoozes_list/);
  assert.match(sql, /Finance economics, eligibility,[\s\S]*settlement behaviour remain untouched/);
  assert.doesNotMatch(sql, /pay_batch_execute|pay_batch_settle|provider_submission|pay_workbench_prepare_draft/i);
  assert.doesNotMatch(sql, /\b(?:insert\s+into|update\s+public\.|delete\s+from|alter\s+table|drop\s+table)\b/i);
  assert.match(sql, /revoke execute on function public\.pay_loans_snoozes_page_v2[\s\S]*from public, anon, authenticated/);
  assert.match(sql, /grant execute on function public\.pay_loans_snoozes_page_v2[\s\S]*to service_role/);
});
