const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');
const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8').replaceAll('\r\n', '\n');
const actual = read('supabase/repeatable/28082026_1650_banking_pay_modal_source_progress_facts.sql');
const source = read('supabase/repeatable/07082026_2155_pay_workbench_session_recompute_progress_counters.sql');
function between(value, start, end) {
  const from = value.indexOf(start), to = value.indexOf(end, from);
  assert.ok(from >= 0 && to > from);
  return value.slice(from, to);
}
test('source-scope publication and pending-owner predicates are byte-equivalent to the current owner', () => {
  assert.equal(between(actual, '  WITH classified_scope AS (', '),\n  source_pending AS ('),
    between(source, '  WITH classified_scope AS (', '), counted_scope AS ('));
  assert.equal(between(actual, '  source_pending AS (', '),\n  facts AS ('),
    between(source, '  WITH source_pending AS (', '), ownership_summary AS (').replace('  WITH source_pending AS (', '  source_pending AS ('));
});
test('progress projection is private, read-only, complete and contains no financial or sample-derived authority', () => {
  assert.doesNotMatch(actual, /\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|LIMIT 10\b|LIMIT 25\b)/i);
  assert.doesNotMatch(actual, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(actual, /STABLE SECURITY INVOKER SET search_path TO ''/);
  assert.match(actual, /FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(actual, /BANKING_PAY_V2_STALE_REVISION/);
});
test('generated progress projection is reproducible against its current owner', () => {
  const result = spawnSync(process.execPath, ['scripts/generate-banking-pay-source-progress-facts.mjs', '--check'], { cwd: root, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr || result.stdout);
});
test('rollback-contained source progress first-use and all-scope parity', { skip: !process.env.BANKING_MODAL_LOCAL_PSQL }, () => {
  const result = spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL, ['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_1652_banking_pay_source_progress_runtime.sql'], { cwd: root, encoding: 'utf8', timeout: 60000 });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  assert.match(result.stderr, /PASS: 26 progress-owner comparisons across 105/);
});
