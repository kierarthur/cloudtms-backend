const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);
const worker = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);

function extractFunction(name, nextFunctionName) {
  const start = sql.indexOf(`CREATE OR REPLACE FUNCTION public.${name}(`);
  const end = sql.indexOf(
    `CREATE OR REPLACE FUNCTION public.${nextFunctionName}(`,
    start + 1
  );
  assert.ok(start >= 0 && end > start, `${name} must be present`);
  return sql.slice(start, end);
}

test('shared Banking Pay open serialises globally and retires every competing OPEN session', () => {
  const source = extractFunction(
    'pay_workbench_session_open_shared_v2',
    'timesheet_weekly_chain_delete_apply'
  );

  assert.match(
    source,
    /pg_catalog\.hashtext\('public\.pay_workbench_session_open_shared_v2\.global_open'\)/
  );

  const inventoryStart = source.indexOf(
    'CREATE TEMP TABLE _bpay_open_shared_duplicate_inventory'
  );
  const inventoryEnd = source.indexOf(
    'SELECT COUNT(*)::integer,',
    inventoryStart
  );
  assert.ok(inventoryStart >= 0 && inventoryEnd > inventoryStart);
  const inventory = source.slice(inventoryStart, inventoryEnd);

  assert.match(inventory, /duplicate_session\.id IS DISTINCT FROM v_session_row\.id/);
  assert.match(inventory, /duplicate_session\.status = 'OPEN'/);
  assert.match(inventory, /duplicate_session\.discarded_at_utc IS NULL/);
  assert.doesNotMatch(inventory, /duplicate_session\.actor_user_id = p_actor_user_id/);
  assert.doesNotMatch(inventory, /duplicate_session\.pay_date = p_pay_date/);
  assert.doesNotMatch(inventory, /v_signature_candidate_filter_id/);
  assert.doesNotMatch(inventory, /v_signature_client_filter_id/);
  assert.doesNotMatch(inventory, /v_signature_candidate_ids/);
});

test('competing sessions are linked to the authoritative replacement before commit', () => {
  const source = extractFunction(
    'pay_workbench_session_open_shared_v2',
    'timesheet_weekly_chain_delete_apply'
  );

  assert.match(source, /SET status = 'DISCARDED'/);
  assert.match(source, /replacement_session_id = v_session_row\.id/);
  assert.match(source, /progress_state = 'DISCARDED'/);
  assert.match(source, /discarded_by_function', 'pay_workbench_session_open_shared_v2'/);
});

test('Worker retries an idempotent workbench open within a strict three-attempt transient-timeout bound', () => {
  const start = worker.indexOf('async function handleBankingPayWorkbenchSessionOpen');
  const end = worker.indexOf('\nfunction bankingPayWorkbenchLogsEnabled', start);
  assert.ok(start >= 0 && end > start, 'workbench open handler must be present');
  const source = worker.slice(start, end);

  assert.match(source, /const maxOpenRpcAttempts = 3;/);
  assert.match(source, /openAttempt <= maxOpenRpcAttempts/);
  assert.match(source, /rpcStatus === 408/);
  assert.match(source, /rpcStatus === 504/);
  assert.match(source, /rpcStatus === 524/);
  assert.match(source, /PAY_WORKBENCH_SESSION_OPEN_SHARED_V2_TRANSIENT_RETRY/);
  assert.match(source, /openAttempt < maxOpenRpcAttempts/);
  assert.match(source, /await new Promise\(\(resolve\) => setTimeout\(resolve, 250\)\)/);
});
