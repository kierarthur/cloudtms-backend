const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const repeatableSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);

function sqlFunctionBody(name) {
  const marker = `CREATE OR REPLACE FUNCTION public.${name}`;
  const start = repeatableSql.indexOf(marker);
  assert.ok(start >= 0, `${name} must exist`);
  const end = repeatableSql.indexOf('CREATE OR REPLACE FUNCTION public.', start + marker.length);
  return repeatableSql.slice(start, end > start ? end : repeatableSql.length);
}

function workerFunctionBody(name, nextName) {
  const start = workerSource.indexOf(`async function ${name}`);
  assert.ok(start >= 0, `${name} must exist`);
  const end = workerSource.indexOf(`function ${nextName}`, start + 1);
  assert.ok(end > start, `${nextName} must follow ${name}`);
  return workerSource.slice(start, end);
}

test('clear-all decisions persists an explicit empty selection and resets the counter', () => {
  const body = sqlFunctionBody('pay_workbench_session_clear_all_decisions');

  assert.match(body, /server_selected_preview_row_ids\s*=\s*'\[\]'::jsonb/);
  assert.match(body, /server_selected_preview_row_ids_provided\s*=\s*true/);
  assert.match(body, /selected_row_count\s*=\s*0/);
  assert.match(body, /'server_selected_preview_row_ids_provided',\s*true/);
  assert.doesNotMatch(body, /'server_selected_preview_row_ids_provided',\s*false/);
});

test('clear-all Worker response cannot reintroduce stale or implicit row selection', () => {
  const body = workerFunctionBody(
    'handleBankingPayWorkbenchSessionClearAllDecisions',
    'openAssignmentBandMappingsModal'
  );

  assert.match(body, /const canonicalSelectedPreviewRowIds = \[\]/);
  assert.match(body, /const canonicalSelectedPreviewRowIdsProvided = true/);
  assert.match(body, /server_selected_preview_row_ids: cloneJson\(canonicalSelectedPreviewRowIds\) \|\| \[\]/);
  assert.match(body, /server_selected_preview_row_ids_provided: canonicalSelectedPreviewRowIdsProvided/);
});
