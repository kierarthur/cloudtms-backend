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
const bankingFunctionsSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
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

test('Create Draft idempotency advances after a corrected workbench recompute', () => {
  const createBody = sliceBetween(
    workerSource,
    'async function handleBankingPayCreateDraft',
    'async function handleTimesheetAdvancePayment'
  );

  assert.match(
    createBody,
    /const idempotencyHash = await sha256Hex\(stableStringify\(\{[\s\S]*progress_counter_version: postSyncProgressCounterVersion/,
    'a failed draft must not be replayed after the reviewed workbench truth has materially advanced'
  );
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

test('prepare-draft selection review uses the certified effective recovery section end to end', () => {
  assert.match(
    guardSql,
    /LOWER\(private\.pay_workbench_preview_effective_section_v1\([\s\S]{0,160}?preview_row\.section,[\s\S]{0,160}?preview_row\.row_json[\s\S]{0,80}?\)\) = 'canonical_preview_lines'/,
    'the final reviewed-set guard must count a strictly promoted recovery exactly once'
  );
  assert.match(
    guardSql,
    /private\.pay_workbench_preview_effective_section_v1\([\s\S]{0,160}?historical_preview_row\.section,[\s\S]{0,160}?historical_preview_row\.row_json[\s\S]{0,80}?\) AS section/,
    'historical row rotation must compare the logical certified section'
  );
  assert.equal(
    (
      guardSql.match(
        /private\.pay_workbench_preview_effective_section_v1\(\s*current_preview_row\.section,\s*current_preview_row\.row_json\s*\)/g
      ) || []
    ).length,
    2,
    'economic-key and frozen-contract row rotation must use the same effective section'
  );
  assert.match(
    guardSql,
    /private\.pay_workbench_preview_effective_section_v1\(\s*preview_row\.section,\s*preview_row\.row_json\s*\) AS section/,
    'the final line contract must validate the effective section rather than the immutable physical partition'
  );
  assert.match(
    guardSql,
    /p_target_section => COALESCE\(NULLIF\(BTRIM\(selected_rows\.section\), ''\), 'canonical_preview_lines'\)/
  );
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
  assert.match(
    previewRevisionSql,
    /'selected_row_count', CASE[\s\S]*WHEN v_resolved_section = 'canonical_preview_lines'[\s\S]*THEN COALESCE\(v_selected_eligible_count, 0\)/
  );
  assert.doesNotMatch(previewRevisionSql, /pay_batch_execute|pay_batch_settle|provider_submission/i);
});
test('draft scope lock mismatch retains the selection-review failure contract', () => {
  const advanceBody = sliceBetween(
    workerSource,
    'async function advanceBankingPayDraftCreateOperation',
    'async function handleBankingPaySnoozeValidate'
  );

  assert.match(advanceBody, /const extractWorkbenchSelectionChangedDraftFailure =/);
  assert.match(advanceBody, /candidate\.toUpperCase\(\)\.includes\('WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'\)/);
  assert.match(advanceBody, /operation_created: true/);
  assert.match(advanceBody, /operation_started: true/);
  assert.match(advanceBody, /no_operation_started: false/);
  assert.match(advanceBody, /no_batch_created: true/);
  const selectionCatch = advanceBody.lastIndexOf('const selectionChangedFailure = extractWorkbenchSelectionChangedDraftFailure(e)');
  const genericCatch = advanceBody.lastIndexOf("code: 'DRAFT_CREATE_OPERATION_FAILED'");
  assert.ok(selectionCatch >= 0);
  assert.ok(genericCatch > selectionCatch, 'selection-review failure must be handled before the generic draft failure');
});

test('selection updates reconcile draft readiness without rebuilding candidate economics', () => {
  const setSelectedRowsBody = sliceBetween(
    bankingFunctionsSql,
    'CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_selected_rows',
    'DROP FUNCTION IF EXISTS public.pay_workbench_session_recompute_candidate'
  );

  assert.match(setSelectedRowsBody, /'selected_eligible_ready_row_count', COALESCE\(v_current_selected_count, 0\)/);
  assert.match(setSelectedRowsBody, /'selected_rows_available', COALESCE\(v_current_selected_count, 0\) > 0/);
  assert.match(setSelectedRowsBody, /'ready_for_draft', v_session_ready AND COALESCE\(v_current_selected_count, 0\) > 0/);
  assert.match(setSelectedRowsBody, /'can_create_draft', v_session_ready AND COALESCE\(v_current_selected_count, 0\) > 0/);
  assert.match(setSelectedRowsBody, /WHERE UPPER\(BTRIM\(blocker_code\.value\)\) <> 'NO_SELECTED_ROWS'/);
  assert.match(setSelectedRowsBody, /IF v_session_ready AND COALESCE\(v_current_selected_count, 0\) = 0 THEN/);
  assert.doesNotMatch(setSelectedRowsBody, /pay_workbench_enqueue_session_candidate_refresh/);
  assert.doesNotMatch(setSelectedRowsBody, /pay_workbench_candidate_source_build_chunk/);
  assert.doesNotMatch(setSelectedRowsBody, /pay_batch_apply_finance_adjustments/);
});
