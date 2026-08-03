const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const read = (relativePath) => fs.readFileSync(path.resolve(__dirname, '..', relativePath), 'utf8');
const source = read('supabase/repeatable/02052026_1528_fast_timesheet_reading.sql');
const summary = read('supabase/repeatable/19012026_extras.sql');
const totals = read('supabase/repeatable/15012026_summarytotals.sql');
const payCache = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
const worker = read('broker/src/index.js');

test('usable expense and mileage evidence is classified in one set-based source pass', () => {
  assert.match(source, /evidence_agg AS MATERIALIZED/i);
  assert.match(source, /te0\.storage_key/i);
  assert.match(source, /ARRAY\['Expenses evidence'::text\]/i);
  assert.match(source, /ARRAY\['Mileage evidence'::text\]/i);
});

test('reference and pair issues are classified before filtering and paging', () => {
  const pairCte = summary.indexOf('correction_pair_issue_timesheets AS MATERIALIZED');
  const filterCte = summary.indexOf('filtered AS MATERIALIZED', pairCte);
  const paging = summary.indexOf('LIMIT v_limit', filterCte);
  assert.ok(pairCte > 0 && filterCte > pairCte && paging > filterCte);
  assert.match(summary, /client_reference_settings AS MATERIALIZED/i);
  assert.match(summary, /reference_number_required_to_issue_invoice/i);
  assert.match(summary, /ARRAY\['Refs missing'::text\]/i);
  assert.match(summary, /ARRAY\['Paired needs invoicing'::text\]/i);
  assert.doesNotMatch(worker, /enrichCorrectionPairPlacementIssues/i);
});

test('overpaid means positive liability exceeded by money actually paid', () => {
  assert.match(summary, /paid_to_date_ex_vat[\s\S]*> 0\.01[\s\S]*paid_to_date_ex_vat[\s\S]*net_delta_ex_vat[\s\S]*> 0\.01[\s\S]*net_delta_ex_vat[\s\S]*< -0\.01/i);
  assert.match(summary, /WHEN enriched_base\.genuine_overpaid THEN ARRAY\['__PAY_BADGE_OVERPAID__'::text\]/i);
  assert.match(summary, /v_issues_filter = 'overpaid'[\s\S]*enriched_row\.genuine_overpaid/i);
});

test('coin and half-coin payment illustrations remain unchanged', () => {
  assert.match(payCache, /WHEN 'PARTIALLY_PAID' THEN 'HALF_COIN'/i);
  assert.match(payCache, /WHEN 'PAID' THEN 'COIN'/i);
  assert.match(payCache, /WHEN 'PROCESSING' THEN 'CLOCK'/i);
});

test('totals and selected IDs delegate issue membership to the row source', () => {
  assert.ok((totals.match(/single issue-classification authority/g) || []).length >= 2);
  assert.match(totals, /purpose', 'totals'/i);
  assert.match(totals, /purpose', 'membership'/i);
});
