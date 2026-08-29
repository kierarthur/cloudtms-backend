import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const worker = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8');
const applySql = readFileSync(
  new URL('../../supabase/repeatable/20260221_01_add_tms_ref_num_and_admin_rpcs.sql', import.meta.url),
  'utf8'
);

function functionBody(name) {
  const startMarker = `async function ${name}(`;
  const start = worker.indexOf(startMarker);
  assert.notEqual(start, -1, `${name} must exist`);
  const next = worker.indexOf('\nasync function ', start + startMarker.length);
  return worker.slice(start, next === -1 ? worker.length : next);
}

test('pay-method preview accepts only retained-finance-aware canonical coverage', () => {
  const body = functionBody('handleCandidatePayMethodChangePreview');
  assert.match(body, /CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY/);
  assert.doesNotMatch(body, /CANONICAL_CURRENT_TIMESHEETS/);
  assert.match(body, /retained_finance_timesheet_ids/);
  assert.match(body, /retained_finance_timesheet_count/);
  assert.match(body, /\.\.\.retainedFinanceTimesheetIds/);
});

test('pay-method apply proof keeps retained financial authority in the exact target union', () => {
  const body = functionBody('handleCandidatePayMethodChange');
  assert.match(body, /CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY/);
  assert.doesNotMatch(body, /CANONICAL_CURRENT_TIMESHEETS/);
  assert.match(body, /retained_finance_timesheet_ids/);
  assert.match(body, /\.\.\.retainedFinanceIds/);
});

test('database apply contract returns and replays retained-finance coverage proof', () => {
  assert.match(
    applySql,
    /coverage_basis'[\s\S]*CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY/
  );
  assert.ok(
    (applySql.match(/'retained_finance_timesheet_ids'/g) || []).length >= 2,
    'new and replay results must both include retained finance IDs'
  );
  assert.ok(
    (applySql.match(/'retained_finance_timesheet_count'/g) || []).length >= 2,
    'new and replay results must both include retained finance counts'
  );
});
