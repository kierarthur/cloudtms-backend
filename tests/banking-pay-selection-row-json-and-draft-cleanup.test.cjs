const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const canonicalSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);
const overpaymentSyncSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_40_pay_sync_overpayments_from_preview.sql'),
  'utf8'
);

function sliceBetween(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.ok(start >= 0, startMarker + ' must exist');
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(end > start, endMarker + ' must follow ' + startMarker);
  return source.slice(start, end);
}

function sqlFunctionBody(source, name) {
  const functionMarker = `CREATE OR REPLACE FUNCTION public.${name}`;
  const start = source.indexOf(functionMarker);
  assert.ok(start >= 0, `${name} must exist`);
  const end = source.indexOf('CREATE OR REPLACE FUNCTION public.', start + functionMarker.length);
  return source.slice(start, end > start ? end : source.length);
}

test('selection writes keep row_json selected state in lockstep with table columns', () => {
  const selectionFunction = sqlFunctionBody(canonicalSql, 'pay_workbench_session_set_selected_rows');

  assert.equal(
    (selectionFunction.match(/row_json\s*=\s*COALESCE\(preview_row\.row_json, '\{\}'::jsonb\)/g) || []).length,
    4,
    'all four selection update paths must update row_json'
  );
  assert.match(selectionFunction, /'selected', true,[\s\S]*?'selection_state', 'SELECTED'/);
  assert.match(selectionFunction, /'selected', false,[\s\S]*?'selection_state', 'UNSELECTED'/);
  assert.match(selectionFunction, /WHEN update_actions\.action = 'REJECT_SELECT' THEN 'NOT_SELECTABLE'/);
});

test('authoritative overpayment components are not adjusted by finance movements twice', () => {
  assert.match(
    overpaymentSyncSql,
    /when upper\(btrim\(coalesce\(\s*tec\.component_json->>'overpayment_component_authority',\s*''\s*\)\)\) = 'PRE_DRAFT_LIVE_TRUTH'[\s\S]*?then round\(coalesce\(tec\.component_amount_ex, 0\), 2\)::numeric\(12,2\)/i,
    'authoritative pre-draft components must use their canonical outstanding amount directly'
  );

  assert.match(
    overpaymentSyncSql,
    /else round\(\s*coalesce\(tec\.component_amount_ex, 0\)\s*\+ coalesce\(fcm\.settled_overpayment_recovery_ex, 0\)[\s\S]*?- coalesce\(fcm\.reserved_underpayment_payment_ex, 0\),\s*2\s*\)::numeric\(12,2\)/i,
    'legacy/non-authoritative component paths must retain their finance-movement adjustment'
  );

  assert.match(
    overpaymentSyncSql,
    /'remaining_source_amount',\s*CASE[\s\S]*?overpayment_component_authority[\s\S]*?= 'PRE_DRAFT_LIVE_TRUTH'[\s\S]*?THEN abs\(tec\.signed_component_amount_ex\)/i,
    'authoritative components must pass their canonical outstanding amount explicitly to component sync'
  );

  assert.match(
    overpaymentSyncSql,
    /'overpayment_component_authority',\s*NULLIF\(UPPER\(BTRIM\(COALESCE\([\s\S]*?tec\.component_json->>'overpayment_component_authority'/i,
    'the authority marker must survive component normalization so case sync uses outstanding semantics'
  );

  assert.match(
    overpaymentSyncSql,
    /IF COALESCE\(v_target_amount_is_authoritative_outstanding, false\) THEN[\s\S]*?v_new_outstanding_amount := GREATEST\([\s\S]*?v_target_case_amount_ex[\s\S]*?v_effective_case_amount_ex := ROUND\([\s\S]*?v_existing_recovered_amount[\s\S]*?v_new_outstanding_amount/i,
    'authoritative case sync must preserve historic consumption without deducting it from current outstanding again'
  );

  assert.match(
    overpaymentSyncSql,
    /ELSE[\s\S]*?v_effective_case_amount_ex := GREATEST\([\s\S]*?v_new_outstanding_amount := greatest\(v_effective_case_amount_ex - v_existing_recovered_amount/i,
    'legacy case sync must retain its existing recovered-amount calculation'
  );
});

test('failed-draft cleanup ignores historical batches mentioned by input or validation', () => {
  const discovery = sliceBetween(
    workerSource,
    'async function discoverCreatedDraftBatchIds()',
    'const cancelCreatedDraftBatchesForFailure'
  );

  assert.doesNotMatch(discovery, /collectPayBatchIdsFromValue\(progressJson/);
  assert.doesNotMatch(discovery, /collectPayBatchIdsFromValue\(inputJson/);
  assert.doesNotMatch(discovery, /chunk\?\.error_json/);
  assert.match(discovery, /progressJson\?\.batch_shells/);
  assert.match(discovery, /fetchCandidateScopes/);
  assert.match(discovery, /fetchOperationChunkRowsForCleanup/);
  assert.match(discovery, /includeSingularIds \? \(source\.pay_batch_id \|\| \[\]\) : \[\]/);
});
