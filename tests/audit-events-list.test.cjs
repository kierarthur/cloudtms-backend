const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repeatablePath = path.resolve(
  __dirname,
  '../supabase/repeatable/19072026_1722_audit_events_list.sql'
);
const sql = fs.readFileSync(repeatablePath, 'utf8');

test('audit list replacement follows the repeatable naming convention', () => {
  assert.match(path.basename(repeatablePath), /^\d{8}_\d{4}_[a-z0-9_]+\.sql$/);
  assert.match(sql, /CREATE OR REPLACE FUNCTION public\.audit_events_list\(/);
});

test('filtered rows, page rows, count and aggregation share one statement', () => {
  assert.match(sql, /WITH filtered AS \([\s\S]*ordered_rows AS \([\s\S]*page_rows AS \([\s\S]*page_aggregate AS \(/);
  assert.match(sql, /SELECT page_aggregate\.items,[\s\S]*SELECT COUNT\(\*\)::bigint FROM filtered/);
  assert.doesNotMatch(sql, /FROM page_rows AS pr;[\s\S]*RETURN/);
});

test('pagination and sorting are deterministic across the filtered dataset', () => {
  assert.match(sql, /ROW_NUMBER\(\) OVER/);
  assert.match(sql, /filtered_row\.ts_utc DESC,[\s\S]*filtered_row\.id DESC/);
  assert.match(sql, /sort_ordinal > v_offset/);
  assert.match(sql, /sort_ordinal <= v_offset \+ v_limit/);
  assert.match(sql, /ORDER BY page_row\.sort_ordinal/);
});

test('security-definer execution is restricted to the backend service role', () => {
  assert.match(sql, /SET search_path TO 'public', 'pg_temp'/);
  assert.match(sql, /REVOKE ALL ON FUNCTION public\.audit_events_list[\s\S]*FROM PUBLIC/);
  assert.match(sql, /GRANT EXECUTE ON FUNCTION public\.audit_events_list[\s\S]*TO service_role/);
});
