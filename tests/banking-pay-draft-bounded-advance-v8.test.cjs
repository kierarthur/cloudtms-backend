const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const ownerPath = path.join(
  root,
  'supabase',
  'repeatable',
  '02092026_2330_banking_pay_draft_bounded_advance_v8.sql'
);
const rejectedAtomicPath = path.join(root, 'codex_outputs', 'h2-draft-parity', 'F010_REJECTED_ATOMIC_EXECUTOR_EVIDENCE.md');
const owner = fs.readFileSync(ownerPath, 'utf8');
const stageOwnerPath = path.join(
  root,
  'supabase',
  'repeatable',
  '03092026_1200_banking_pay_draft_certificate_stage_advance_v8.sql'
);
const stageOwner = fs.readFileSync(stageOwnerPath, 'utf8');
const terminalPath = path.join(
  root,
  'supabase',
  'repeatable',
  '02092026_2331_banking_pay_draft_terminal_finish_v8.sql'
);
const terminalOwner = fs.readFileSync(terminalPath, 'utf8');
const rejectedAtomic = fs.readFileSync(rejectedAtomicPath, 'utf8');
const runtimePath = path.join(root, 'tests', '02092026_2332_banking_pay_draft_bounded_advance_v8_runtime.sql');
const runtime = fs.readFileSync(runtimePath, 'utf8');

const certificateStageOwners = [
  'pay_workbench_draft_certificate_final_freeze_v8'
];

const delegatedBusinessOwners = [
  'banking_pay_draft_phase_units_seed_v8',
  'pay_batch_shell_ensure_from_operation',
  'pay_workbench_prepare_draft_allocation_rows_seed',
  'pay_batch_insert_candidates_from_preview',
  'pay_batch_insert_items_from_preview',
  'pay_batch_apply_finance_adjustments',
  'pay_batch_finalize_reservations_and_markers',
  'pay_batch_populate_candidate_summaries',
  'pay_batch_create_timesheet_snapshots',
  'pay_batch_build_item_breakdowns',
  'pay_batch_assert_integrity',
  'pay_workbench_draft_constituent_parity_page_v8',
  'pay_workbench_patch_preview_after_batch_mutation'
];

test('bounded V8 entry is service-only and the earlier whole-route design is not the release owner', () => {
  assert.match(owner, /CREATE OR REPLACE FUNCTION public\.banking_pay_draft_advance_bounded_v8\(/);
  assert.match(owner, /REVOKE ALL ON FUNCTION public\.banking_pay_draft_advance_bounded_v8\(uuid,text,text\)\s+FROM PUBLIC, anon, authenticated/);
  assert.match(owner, /GRANT EXECUTE ON FUNCTION public\.banking_pay_draft_advance_bounded_v8\(uuid,text,text\)\s+TO service_role/);
  assert.match(rejectedAtomic, /C5D3D9026C5AB9BCA227A2A56CF5E41030E31930907E46B779BA1E0FCDB413C0/);
  assert.match(rejectedAtomic, /must never be restored to `supabase\/repeatable`/);
  assert.equal(fs.existsSync(path.join(root, 'supabase', 'repeatable', '02092026_2314_banking_pay_draft_atomic_execute_v8.sql')), false);
  assert.doesNotMatch(owner, /banking_pay_draft_execute_atomic_v8/);
});

test('one invocation performs at most one existing-owner business page', () => {
  assert.match(owner, /performs at most one\s+-- existing-owner business page/);
  assert.match(owner, /one business owner page and returns immediately/);
  assert.match(owner, /'business_owner_call_count', 1/);
  assert.doesNotMatch(owner, /FOREACH v_phase IN ARRAY/);
  assert.doesNotMatch(owner, /IF v_owner_iteration > 500/);
  assert.doesNotMatch(owner, /maximum chunks per call|p_request_budget_ms/i);
});

test('bounded pages retain the 50,000 ceiling without a global 100 cap', () => {
  assert.match(owner, /WHEN v_phase IN \('SEED_ALLOCATION_ROWS','APPLY_FINANCE_ADJUSTMENTS'\) THEN 50/);
  assert.match(owner, /WHEN v_phase = 'FINALISE_RESERVATIONS' THEN 1/);
  assert.match(owner, /ELSE 100/);
  assert.match(owner, /banking_pay_draft_phase_units_seed_v8\([\s\S]*?256/);
  assert.match(owner, /pay_workbench_draft_constituent_parity_page_v8\([\s\S]*?256/);
  assert.doesNotMatch(owner, /selected_preview_row_ids_json|selected_canonical_preview_lines_json/);
  assert.doesNotMatch(owner, /LIMIT\s+50000/i);
});

test('current timeout and lock budgets remain active and cannot be weakened', () => {
  assert.match(owner, /banking_pay_hot_path_budget_apply\('WORKBENCH_CHUNK'\)/);
  assert.doesNotMatch(owner, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
  assert.doesNotMatch(owner, /set_config\([^\n]*(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
  assert.doesNotMatch(owner, /p_request_budget_ms|timeoutMs|no_timeout|disable.*timeout/i);
});

test('every business decision remains delegated to the existing owner', () => {
  for (const name of certificateStageOwners) assert.match(stageOwner, new RegExp(`public\\.${name}\\b`));
  for (const name of delegatedBusinessOwners) assert.match(owner, new RegExp(`public\\.${name}\\b`));
  for (const name of certificateStageOwners) assert.doesNotMatch(owner, new RegExp(`public\\.${name}\\b`));
  assert.doesNotMatch(owner, /GROSS_(?:ADD|DEDUCT)|NET_(?:ADD|DEDUCT)|PAYMENT_ADVANCE_REPAYMENT|LOAN_REPAYMENT|VAT_RATE|vat_percent/i);
  assert.doesNotMatch(owner, /UPDATE\s+(?:public\.)?(?:finance_cases|payment_advances|timesheets)\b/i);
  assert.doesNotMatch(owner, /INSERT\s+INTO\s+(?:public\.)?pay_batch_items\b/i);
});

test('phase progress, response loss and finalizer continuation fail closed', () => {
  assert.match(owner, /DRAFT_PHASE_UNIT_REPLAY_CONFLICT/);
  assert.match(owner, /DRAFT_BOUNDED_ADVANCE_LEASE_OWNER_MISMATCH/);
  assert.match(owner, /DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID/);
  assert.match(owner, /DRAFT_BOUNDED_ADVANCE_PARITY_(?:FAILED|INCOMPLETE)/);
  assert.match(owner, /unit_state = CASE WHEN COALESCE\(p_owner_has_more, false\)[\s\S]*?'WAITING_CONTINUATION'/);
  assert.match(owner, /immediate_continue/);
  assert.doesNotMatch(owner, /^\s*(?:COMMIT|ROLLBACK)\s*;/im);
  assert.match(owner, /v_operation\.frozen_selected_row_count/);
  assert.doesNotMatch(owner, /v_operation\.frozen_selected_count\b/);
  assert.match(owner, /PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED/);
  assert.match(owner, /EMPTY_RESERVED_BATCH_CLEANUP_REQUIRED/);
  assert.match(owner, /existing Worker cancelSkippedEmptyReservedDraftBatches/);
});

test('the bounded owner durably patches the preview but stops before terminal publication and Worker wake', () => {
  assert.match(owner, /'work_kind', 'READY_FOR_TERMINAL_FINISH'/);
  assert.match(owner, /'business_artifacts_complete', true/);
  assert.match(owner, /'constituent_parity_complete', true/);
  assert.match(owner, /'post_create_patch_complete', true/);
  assert.match(owner, /'worker_wake_required_after_terminal', true/);
  assert.match(owner, /post_refresh_state = 'APPLIED'/);
  assert.match(owner, /POST_CREATE_REFRESH_RETRY_REQUIRED/);
  assert.doesNotMatch(owner, /UPDATE\s+public\.banking_pay_operations[\s\S]{0,800}status\s*=\s*'COMPLETE'/i);
  assert.doesNotMatch(owner, /INSERT INTO private\.banking_pay_draft_operation_terminal_results_v8/i);
  assert.doesNotMatch(owner, /scheduleBankingPayWorkbenchDrainWithDurableWake/);
});

test('the Draft-only terminal adapter preserves the public result shape without reopening legacy selected arrays', () => {
  assert.match(terminalOwner, /CREATE OR REPLACE FUNCTION public\.banking_pay_draft_operation_finish_v8\(/);
  assert.match(terminalOwner, /RETURNS TABLE \([\s\S]*?finished boolean,[\s\S]*?result_json jsonb,[\s\S]*?failed_at_utc timestamptz/);
  assert.match(terminalOwner, /post_draft_authority_contract_version','POST_DRAFT_LIVE_AUTHORITY_V2'/);
  assert.match(terminalOwner, /policy_x_authority','FROZEN_PRE_DRAFT_SOURCE_PLUS_POST_DRAFT_LIVE_FENCE'/);
  assert.match(terminalOwner, /pay_workbench_scope_blocker_state_v1/);
  assert.match(terminalOwner, /banking_pay_draft_operation_terminal_results_v8/);
  assert.match(terminalOwner, /link_state='TERMINAL_COMPLETE'/);
  assert.match(terminalOwner, /freeze_state='TERMINAL_COMPLETE'/);
  assert.doesNotMatch(terminalOwner, /jsonb_array_elements_text\([\s\S]{0,120}selected_preview_row_ids_json/);
  assert.doesNotMatch(terminalOwner, /banking_pay_operation_chunks/);
  assert.doesNotMatch(terminalOwner, /UPDATE\s+(?:public\.)?(?:finance_cases|payment_advances|timesheets)\b/i);
  assert.doesNotMatch(terminalOwner, /INSERT\s+INTO\s+(?:public\.)?pay_batch_items\b/i);
});

test('the terminal RPC is bounded and cannot widen the established time or lock budget', () => {
  assert.match(terminalOwner, /set_config\('statement_timeout', '15000', true\)/);
  assert.match(terminalOwner, /set_config\('lock_timeout', '1500', true\)/);
  assert.doesNotMatch(terminalOwner, /statement_timeout', '(?:1[6-9]|[2-9][0-9])\d{3}/);
  assert.doesNotMatch(terminalOwner, /lock_timeout', '(?:1[6-9]\d\d|[2-9]\d{3,})'/);
  assert.doesNotMatch(terminalOwner, /FOR UPDATE[\s\S]{0,100}(?:pay_candidates|candidate_publications|banking_pay_workbench_preview_rows)/i);
});

test('rollback runtime fixture drives every current business phase with one owner call per advance', () => {
  for (const name of [
    'SEED_ALLOCATION_ROWS','CREATE_BATCH_SHELLS','INSERT_CANDIDATES','INSERT_ITEMS',
    'APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS','POPULATE_CANDIDATE_SUMMARIES',
    'CREATE_TIMESHEET_SNAPSHOTS','BUILD_ITEM_BREAKDOWNS','ASSERT_INTEGRITY','CONSTITUENT_PARITY',
    'POST_CREATE_REFRESH'
  ]) assert.match(runtime, new RegExp(name));
  assert.match(runtime, /business_owner_call_count[^\n]+BETWEEN 0 AND 1/);
  assert.match(runtime, /expected exactly 13 bounded owner calls including one continuation and one durable post-Draft patch/);
  assert.match(runtime, /'has_more',v_prior_calls=0/);
  assert.match(runtime, /EMPTY_RESERVED_BATCH_CLEANUP_REQUIRED/);
  assert.match(runtime, /valid channel did not continue after empty-shell cleanup/);
  assert.match(runtime, /all-empty operation did not preserve established failure/);
  assert.match(runtime, /banking_pay_draft_operation_finish_v8/);
  assert.match(runtime, /post_draft_authority_contract_version/);
  assert.match(runtime, /terminal replay was not idempotent/);
  assert.match(runtime, /terminal mismatch did not fail atomically/);
  assert.match(runtime, /Compare the row-backed terminal adapter with the currently accepted legacy/);
  assert.match(runtime, /V1\/V8 terminal policy projection differs/);
  assert.match(runtime, /public\.banking_pay_operation_finish\(/);
  assert.match(runtime, /ROLLBACK;/);
  assert.doesNotMatch(runtime, /(?:provider|settlement|remittance).*\(/i);
});
