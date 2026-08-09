const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repeatablePath = path.resolve(
  __dirname,
  '../supabase/repeatable/19072026_1659_cancel_batch_audit_and_full_candidate_refresh.sql'
);
const refreshRepeatablePath = path.resolve(
  __dirname,
  '../supabase/repeatable/19072026_1816_cancel_refresh_supersede_finance_dirty.sql'
);
const repeatableSql = fs.readFileSync(repeatablePath, 'utf8');
const refreshRepeatableSql = fs.readFileSync(refreshRepeatablePath, 'utf8');
const workerSource = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');

function sliceBetween(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(start >= 0, `missing start marker: ${startMarker}`);
  assert.ok(end > start, `missing end marker: ${endMarker}`);
  return source.slice(start, end);
}

test('repeatable follows the SQL function naming and placement convention', () => {
  assert.match(path.basename(repeatablePath), /^\d{8}_\d{4}_[a-z0-9_]+\.sql$/);
  assert.match(path.basename(refreshRepeatablePath), /^\d{8}_\d{4}_[a-z0-9_]+\.sql$/);
  assert.match(repeatableSql, /pay_payment_cancel_finalise_metadata_v1/);
  assert.match(repeatableSql, /pay_payment_cancel_not_sent_and_recalculate_complete_v1/);
  assert.doesNotMatch(repeatableSql, /CREATE OR REPLACE FUNCTION public\.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1/);
  assert.match(refreshRepeatableSql, /CREATE OR REPLACE FUNCTION public\.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1/);
});

test('cancellation completes all frozen work before it can report success', () => {
  assert.match(repeatableSql, /WHILE v_complete IS NOT TRUE AND v_iteration < 100 LOOP/);
  assert.match(repeatableSql, /pay_payment_correction_process_chunk\(/);
  assert.match(repeatableSql, /PAYMENT_CANCEL_COMPLETE_LIMIT_EXCEEDED/);
  assert.match(repeatableSql, /no partial cancellation was committed/);
  assert.match(repeatableSql, /'is_complete', true/);
});

test('terminal cancellation metadata and actor audit are fail-closed and idempotent', () => {
  assert.match(repeatableSql, /v_request\.correction_kind, ''\)\)\) <> 'PRE_BANK_CANCEL'/);
  assert.match(repeatableSql, /v_request\.status, ''\)\)\) <> 'APPLIED'/);
  assert.match(repeatableSql, /execution_commit_state, 'NOT_SUBMITTED'/);
  assert.match(repeatableSql, /PAYMENT_CANCEL_FINALISE_ACTIVE_ITEMS_REMAIN/);
  assert.match(repeatableSql, /cancelled_by_user_id = COALESCE/);
  assert.match(repeatableSql, /'PAY_BATCH_CANCELLED'/);
  assert.match(repeatableSql, /cancelled_by_display/);
  assert.match(repeatableSql, /existing_audit\.correlation_id = p_correction_request_id::text/);
});

test('post-cancel complex candidates enter the canonical current-authority ladder with no targeted ids', () => {
  assert.match(refreshRepeatableSql, /WORKBENCH_JOB_SUPERSEDED_BY_CANCEL_FULL_CANDIDATE_REFRESH/);
  assert.match(refreshRepeatableSql, /'defer_complex_enqueue',true/);
  assert.match(refreshRepeatableSql, /'targeted_timesheet_ids', '\[\]'::jsonb/);
  assert.match(refreshRepeatableSql, /'linked_timesheet_ids', '\[\]'::jsonb/);
  assert.match(refreshRepeatableSql, /'refresh_scope_kind', 'CANDIDATE_FULL_LIVE'/);
  assert.match(refreshRepeatableSql, /'canonical_route_ladder_required', true/);
  assert.match(refreshRepeatableSql, /'fallback_reason', 'CERTIFIED_CANCELLATION_REVERSION_REJECTED'/);
  assert.doesNotMatch(refreshRepeatableSql, /'force_legacy', true/);
  assert.match(refreshRepeatableSql, /'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'/);
  assert.doesNotMatch(refreshRepeatableSql, /selection_state\s*=\s*'READY'/);
});

test('Worker cancellation route binds the verified ceremony to the same bounded correction request', () => {
  const handler = sliceBetween(
    workerSource,
    'async function handleBankingPayBatchCancelV1',
    'async function handleBankingPayPaymentStatusResolveV1'
  );
  const bridge = sliceBetween(
    workerSource,
    'async function startVerifiedDraftCancellationAfterPlanningV1',
    'async function handleBankingPayBatchCancelV1'
  );

  assert.match(handler, /sbRpc\(env, 'pay_batch_cancel'/);
  assert.match(handler, /startVerifiedDraftCancellationAfterPlanningV1/);
  assert.match(bridge, /claimAndAdvanceOneBankingPayOperation/);
  assert.match(bridge, /pay_payment_correction_reauth_bind_v1/);
  assert.match(bridge, /pay_payment_correction_request_start/);
  assert.match(bridge, /enqueueBankingPayCancellationResult/);
  assert.doesNotMatch(handler, /pay_payment_cancel_not_sent_and_recalculate/);
  assert.doesNotMatch(handler, /pay_workbench_/);
  assert.doesNotMatch(bridge, /pay_workbench_/);
});

test('Policy X remains frozen during cancel and switches to live truth only after cancel', () => {
  assert.match(repeatableSql, /cancellation itself continues to operate only on frozen batch artifacts/);
  assert.match(repeatableSql, /PRE_DRAFT_LIVE_TRUTH/);
  assert.doesNotMatch(repeatableSql, /post[_ -]draft.*live.*fallback/i);
});
