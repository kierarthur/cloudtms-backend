const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const guardSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/19072026_2344_banking_pay_shared_selection_guard.sql'),
  'utf8'
);
const previewRevisionSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/20072026_0117_banking_pay_preview_selection_revision.sql'),
  'utf8'
);

function sliceBetween(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.ok(start >= 0, startMarker + ' must exist');
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(end > start, endMarker + ' must follow ' + startMarker);
  return source.slice(start, end);
}

test('Create Draft rejects a changed global selection before starting an operation', () => {
  const createBody = sliceBetween(
    workerSource,
    'async function handleBankingPayCreateDraft',
    'async function handleTimesheetAdvancePayment'
  );

  const currentSelectionRead = createBody.indexOf('fetchCurrentSessionSelectionForCreateDraft');
  const exactSetGuard = createBody.indexOf('const reviewedSelectionStillCurrent');
  const operationInput = createBody.indexOf('const buildDraftCreateOperationInputFromSessionSelection');

  assert.ok(currentSelectionRead >= 0);
  assert.ok(exactSetGuard > currentSelectionRead, 'exact set guard must follow the authoritative read');
  assert.ok(operationInput > exactSetGuard, 'exact set guard must run before operation input is built');
  assert.match(createBody, /WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED/);
  assert.match(createBody, /reviewedGlobalSelectedIds\.length === authoritativeGlobalSelectedIds\.length/);
  assert.match(createBody, /no_operation_started: true/);
  assert.match(createBody, /no_batch_created: true/);
});

test('Create Draft persists the exact reviewed set and revision for the database lock guard', () => {
  const createBody = sliceBetween(
    workerSource,
    'async function handleBankingPayCreateDraft',
    'async function handleTimesheetAdvancePayment'
  );

  assert.match(createBody, /expected_workbench_selected_preview_row_ids: reviewedGlobalSelectedIds/);
  assert.match(createBody, /expected_workbench_progress_counter_version: postSyncProgressCounterVersion/);
  assert.match(createBody, /selection_review_contract_version: 1/);
  assert.match(createBody, /selection_reviewed_by_user_id: actorUserId/);
});

test('prepare-draft repeatable enforces selection and revision under the session lock', () => {
  assert.equal(
    (guardSql.match(/CREATE OR REPLACE FUNCTION public\.pay_workbench_prepare_draft/g) || []).length,
    1
  );
  assert.match(guardSql, /FROM public\.banking_pay_workbench_sessions AS session_row[\s\S]*FOR UPDATE;/);
  assert.match(guardSql, /expected_workbench_progress_counter_version/);
  assert.match(guardSql, /expected_workbench_selected_preview_row_ids/);
  assert.match(guardSql, /v_current_selected_preview_row_ids IS DISTINCT FROM v_expected_selected_preview_row_ids/);
  assert.match(guardSql, /WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED/);
  assert.doesNotMatch(guardSql, /PAY_WORKBENCH_PREPARE_DRAFT_SESSION_ACTOR_MISMATCH/);
  assert.match(guardSql, /PAY_WORKBENCH_PREPARE_DRAFT_OPERATION_ACTOR_MISMATCH/);
});

test('prepare-draft remains service-role only and preserves the Policy X boundary', () => {
  assert.match(guardSql, /REVOKE ALL ON FUNCTION public\.pay_workbench_prepare_draft[\s\S]*FROM PUBLIC;/);
  assert.match(guardSql, /REVOKE ALL ON FUNCTION public\.pay_workbench_prepare_draft[\s\S]*FROM anon;/);
  assert.match(guardSql, /REVOKE ALL ON FUNCTION public\.pay_workbench_prepare_draft[\s\S]*FROM authenticated;/);
  assert.match(guardSql, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_prepare_draft[\s\S]*TO service_role;/);
  assert.match(guardSql, /Policy X: this validates live workbench truth only before draft freeze/);
  assert.doesNotMatch(guardSql, /pay_batch_execute|pay_batch_settle|provider_submission/i);
});

test('preview page exposes the authoritative shared selection revision', () => {
  assert.equal(
    (previewRevisionSql.match(/CREATE OR REPLACE FUNCTION public\.pay_workbench_session_get_preview_page/g) || []).length,
    1
  );
  assert.match(previewRevisionSql, /'session_version', v_session_row\.version/);
  assert.match(previewRevisionSql, /'progress_counter_version', COALESCE\(v_session_row\.progress_counter_version, 0\)/);
  assert.match(previewRevisionSql, /'selected_row_count', COALESCE\(v_session_row\.selected_row_count, 0\)/);
  assert.doesNotMatch(previewRevisionSql, /pay_batch_execute|pay_batch_settle|provider_submission/i);
});
