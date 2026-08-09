const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repeatablePath = path.resolve(
  __dirname,
  '../supabase/repeatable/19072026_1816_cancel_refresh_supersede_finance_dirty.sql'
);
const sql = fs.readFileSync(repeatablePath, 'utf8');

test('cancellation finance-dirty race fix follows the repeatable SQL convention', () => {
  assert.match(path.basename(repeatablePath), /^\d{8}_\d{4}_[a-z0-9_]+\.sql$/);
  assert.match(sql, /CREATE OR REPLACE FUNCTION public\.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1/);
});

test('the cancelled frozen batch is the authority for affected finance cases', () => {
  assert.match(sql, /FROM public\.pay_batch_items AS batch_item/);
  assert.match(sql, /JOIN public\.pay_batch_candidates AS batch_candidate/);
  assert.match(sql, /batch_candidate\.pay_batch_id = p_pay_batch_id/);
  assert.match(sql, /batch_item\.finance_case_id IS NOT NULL/);
});

test('only queued finance dirty jobs for matching finance cases are superseded', () => {
  assert.match(sql, /job_type, ''\)\)\) = 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY'/);
  assert.match(sql, /status, ''\)\)\) = 'QUEUED'/);
  assert.match(sql, /unnest\(v_changed_finance_case_ids\)/);
  assert.match(sql, /WORKBENCH_FINANCE_DIRTY_SUPERSEDED_BY_CANCEL_FULL_CANDIDATE_REFRESH/);
  assert.doesNotMatch(sql, /status, ''\)\)\) IN \('QUEUED', 'RUNNING'\)/);
});

test('cancellation supersession is successful terminal work with durable audit metadata', () => {
  assert.match(sql, /WITH superseded_finance_dirty_jobs AS \([\s\S]*SET status = 'SUCCEEDED'/);
  assert.match(sql, /WITH superseded_targeted_jobs AS \([\s\S]*SET status = 'SUCCEEDED'/);
  assert.match(sql, /completed_at_utc = v_now/);
  assert.match(sql, /failed_at_utc = NULL::timestamptz/);
  assert.match(sql, /last_error_json = NULL::jsonb/);
  assert.match(sql, /economic_build_id = NULL::uuid/);
  assert.match(sql, /private_stage = NULL::text/);
  assert.match(sql, /private_cursor_kind = NULL::text/);
  assert.match(sql, /private_cursor_json = '\{\}'::jsonb/);
  assert.match(sql, /private_stage_version = NULL::integer/);
  assert.match(sql, /'completion_code', 'WORKBENCH_FINANCE_DIRTY_SUPERSEDED_BY_CANCEL_FULL_CANDIDATE_REFRESH'/);
  assert.match(sql, /'completion_code', 'WORKBENCH_JOB_SUPERSEDED_BY_CANCEL_FULL_CANDIDATE_REFRESH'/);
  assert.match(sql, /'superseding_session_id', p_session_id::text/);
  assert.match(sql, /'superseding_pay_batch_id', p_pay_batch_id::text/);
});

test('finance race guard runs before the canonical current-authority ladder is invoked', () => {
  const supersedeAt = sql.indexOf('WITH superseded_finance_dirty_jobs AS');
  const enqueueAt = sql.indexOf('v_enqueue_result := public.pay_workbench_enqueue_candidate_refresh');
  assert.ok(supersedeAt >= 0);
  assert.ok(enqueueAt > supersedeAt);
  assert.match(sql, /'refresh_scope_kind', 'CANDIDATE_FULL_LIVE'/);
  assert.match(sql, /'canonical_route_ladder_required', true/);
  assert.match(sql, /'fallback_reason', 'CERTIFIED_CANCELLATION_REVERSION_REJECTED'/);
  assert.doesNotMatch(sql, /'force_legacy', true/);
});

test('race fix changes orchestration only and preserves the Policy X boundary', () => {
  assert.match(sql, /PRE_DRAFT_LIVE_TRUTH/);
  assert.doesNotMatch(sql, /UPDATE public\.pay_finance_case_components/);
  assert.doesNotMatch(sql, /UPDATE public\.pay_advances/);
  assert.doesNotMatch(sql, /UPDATE public\.pay_batch_items/);
  assert.doesNotMatch(sql, /INSERT INTO public\.pay_batch/);
});

test('cancelled rows are durably returned unselected before refresh jobs run', () => {
  const unselectAt = sql.indexOf('UPDATE public.banking_pay_workbench_preview_rows AS cancelled_preview_row');
  const sessionSelectionAt = sql.indexOf('UPDATE public.banking_pay_workbench_sessions AS selection_session');
  const enqueueAt = sql.indexOf('v_enqueue_result := public.pay_workbench_enqueue_candidate_refresh');
  assert.ok(unselectAt >= 0);
  assert.ok(sessionSelectionAt > unselectAt);
  assert.ok(enqueueAt > sessionSelectionAt);
  assert.match(sql, /WHEN UPPER\(BTRIM\(COALESCE\(cancelled_preview_row\.status, ''\)\)\) = 'READY' THEN 'UNSELECTED'/);
  assert.match(sql, /server_selected_preview_row_ids_provided = true/);
  assert.match(sql, /'mode', 'EXPLICIT_INCLUDE'/);
  assert.match(sql, /'source_selection_action', 'RETURN_CANCELLED_ROWS_UNSELECTED'/);
});

test('post-cancel selection reconciliation preserves unrelated selected rows only', () => {
  assert.match(sql, /jsonb_agg\(to_jsonb\(selected_preview_row\.id::text\)/);
  assert.match(sql, /selected_preview_row\.selected, false\) IS TRUE/);
  assert.match(sql, /selected_preview_row\.selection_state, ''\)\)\) = 'SELECTED'/);
  assert.match(sql, /'server_selected_preview_row_ids', COALESCE\(v_preserved_selected_preview_row_ids, '\[\]'::jsonb\)/);
  assert.match(sql, /'cancelled_row_unselected_count', COALESCE\(v_cancelled_row_unselected_count, 0\)/);
});
