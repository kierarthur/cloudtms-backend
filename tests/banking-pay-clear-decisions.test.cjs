const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const repeatableSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/11082026_1552_pay_workbench_session_clear_all_decisions.sql'),
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

test('clear-all decisions selects all eligible current rows and rebuilds only changed candidates', () => {
  const body = sqlFunctionBody('pay_workbench_session_clear_all_decisions');

  assert.match(body, /pay_workbench_session_set_selected_rows/);
  assert.match(body, /'global_selection_action',\s*'SELECT_ALL_SECTION'/);
  assert.match(body, /'selection_intent_mode',\s*'IMPLICIT_ALL'/);
  assert.match(body, /v_affected_candidate_ids/);
  assert.match(body, /decision_changed_candidate_only/);
  assert.match(body, /'force_refresh',\s*true/);
  assert.match(body, /'no_change_candidate_rebuild_count',\s*0/);
  assert.doesNotMatch(body, /version\s*=\s*\w+\.version\s*\+\s*1/);
  assert.match(body, /'server_selected_preview_row_ids_provided',\s*true/);
  assert.doesNotMatch(body, /'server_selected_preview_row_ids_provided',\s*false/);
});

test('clear-all Worker preserves the server-owned implicit-all selection', () => {
  const body = workerFunctionBody(
    'handleBankingPayWorkbenchSessionClearAllDecisions',
    'openAssignmentBandMappingsModal'
  );

  assert.match(body, /clearObj\.server_selected_preview_row_ids/);
  assert.match(body, /const canonicalSelectionIntentMode/);
  assert.match(body, /canonicalSelectionIntentMode !== 'IMPLICIT_ALL'/);
  assert.match(body, /server_selected_preview_row_ids: cloneJson\(canonicalSelectedPreviewRowIds\) \|\| \[\]/);
  assert.match(body, /server_selected_preview_row_ids_provided: canonicalSelectedPreviewRowIdsProvided/);
});
