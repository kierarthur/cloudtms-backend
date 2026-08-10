const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8');

const context = read('supabase', 'repeatable',
  '10082026_2345_banking_pay_correction_owned_dirty_context.sql');
const setting = read('supabase', 'migrations',
  '10082026_2346_banking_pay_correction_owned_dirty_control.sql');
const summaryTrigger = read('supabase', 'repeatable',
  '04082026_1219_pay_timesheet_summary_pay_state_refresh_trigger.sql');
const financeTrigger = read('supabase', 'repeatable',
  '04082026_1202_pay_workbench_financial_scope_dirty_transition_v1.sql');
const dirtyRuntime = read('supabase', 'repeatable',
  '07082026_1016_banking_pay_targeted_delta_runtime.sql');
const semanticHelpers = read('supabase', 'repeatable',
  '09082026_0712_banking_pay_semantic_ready_helpers.sql');

test('clean repeatable order installs causal context before legacy callers', () => {
  assert.match(
    financeTrigger,
    /\\ir 10082026_2345_banking_pay_correction_owned_dirty_context\.sql/,
  );
});

test('causal context is transaction-local, bounded, and has no durable business table', () => {
  assert.match(context, /CREATE TEMP TABLE IF NOT EXISTS pg_temp\._bpay_wb_correction_dirty_context_v1/i);
  assert.match(context, /ON COMMIT DROP/i);
  assert.match(context, /CORRECTION_OWNED_DIRTY_CAUSAL_V1/g);
  assert.match(context, /REQUEST_PREPARE.*REQUEST_START.*FINANCIAL_PAGE_START/s);
  assert.match(context, /FINANCIAL_PAGE_APPLIED.*FINANCIAL_TERMINAL/s);
  assert.match(context, /SECURITY INVOKER/i);
  assert.match(context, /REVOKE ALL[\s\S]*PUBLIC,anon,authenticated,service_role/i);
  assert.doesNotMatch(context, /CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?!IF NOT EXISTS pg_temp)/i);
});

test('request lifecycle causality is derived from the exact transition relation', () => {
  assert.match(summaryTrigger, /TG_TABLE_NAME='pay_payment_correction_requests'/);
  assert.match(summaryTrigger, /FROM new_rows AS request_transition/);
  assert.match(summaryTrigger, /FROM new_rows AS new_request[\s\S]*JOIN old_rows AS old_request/);
  assert.match(summaryTrigger, /pay_workbench_correction_dirty_context_set_v1/);
  assert.match(summaryTrigger, /THEN 'REQUEST_START'[\s\S]*ELSE 'REQUEST_PREPARE'/);
  assert.match(summaryTrigger, /draft_overlay_fast_pre_request_authorities/);
});

test('summary and finance triggers retain invalidation while stamping the exact scope token', () => {
  for (const source of [summaryTrigger, financeTrigger]) {
    assert.match(source, /correction_dirty_contexts/);
    assert.match(source, /request_owned_scope_change_tx_token/);
    assert.match(source, /pay_workbench_scope_change_tx_token_v1\(\)/);
    assert.match(source, /pay_workbench_scope_invalidate_v1/);
  }
  assert.match(summaryTrigger, /mixed statement must never suppress an unrelated economic change/i);
  assert.doesNotMatch(summaryTrigger, /WHERE v_correction_dirty_contexts \? invalidation_pair\.candidate_id::text/);
  assert.match(financeTrigger, /v_correction_context_count=v_impacted_candidate_count/);
});

test('candidate dirty owner defers only the latest exactly correlated token', () => {
  assert.match(setting, /banking_pay_correction_request_dirty_deferral_v1_enabled[\s\S]*DEFAULT false/i);
  assert.match(dirtyRuntime, /banking_pay_correction_request_dirty_deferral_v1_enabled/);
  assert.match(dirtyRuntime, /scope_change_tx_token'[\s\S]*request_owned_scope_change_tx_token/s);
  assert.match(dirtyRuntime, /CORRECTION_OWNED_DIRTY_CAUSAL_V1/);
  assert.match(dirtyRuntime, /extensions\.digest[\s\S]*sha256/s);
  assert.match(dirtyRuntime, /correction_operation\.phase<>'COMPLETE'/);
  assert.match(dirtyRuntime, /WAITING_AUTHORISATION[\s\S]*interval '30 seconds'/s);
  assert.match(dirtyRuntime, /REQUEST_OWNED_POLICY_X_DIRTY_WAITING_FOR_FINANCIAL_BOUNDARY/);
  assert.match(dirtyRuntime, /source_build_enqueue_skipped_by_request_boundary', true/);
});

test('financial page establishes context in the exact mutation transaction', () => {
  assert.match(semanticHelpers, /CREATE OR REPLACE FUNCTION private\.pay_pre_bank_cancel_apply_work_page_v1/);
  assert.match(semanticHelpers, /pay_workbench_correction_dirty_context_set_v1/);
  assert.match(semanticHelpers, /p_lifecycle_phase:='FINANCIAL_PAGE_START'/);
  assert.match(semanticHelpers, /owner','pay_pre_bank_cancel_apply_work_page_v1'/);
});

test('Policy X and economic owners are not changed by the causality patch', () => {
  const combined = [context, summaryTrigger, financeTrigger,
    dirtyRuntime, semanticHelpers].join('\n');
  assert.doesNotMatch(context, /pay_sync_overpayments|settlement|remittance|provider_submission/i);
  assert.doesNotMatch(setting, /timeout|lease|lane|parallel/i);
  assert.match(combined, /POST_DRAFT_FROZEN_EVIDENCE/);
  assert.match(combined, /PRE_DRAFT_LIVE_TRUTH/);
});
