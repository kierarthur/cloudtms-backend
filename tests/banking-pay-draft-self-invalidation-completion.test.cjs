const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8');

const controls = read('supabase','migrations','10082026_0023_banking_pay_draft_self_invalidation_controls.sql');
const helpers = read('supabase','repeatable','10082026_0024_banking_pay_draft_expected_effects_v1.sql');
const trigger = read('supabase','repeatable','04082026_1202_pay_workbench_financial_scope_dirty_transition_v1.sql');
const claim = read('supabase','repeatable','07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql');
const patch = read('supabase','repeatable','09082026_0825_pay_workbench_patch_preview_after_batch_mutation.sql');
const builder = read('supabase','repeatable','05082026_1545_pay_preview_candidate_build_canonical_lines.sql');
const summary = read('supabase','repeatable','09082026_0826_pay_preview_candidate_build_summary_fragment.sql');
const semantic = read('supabase','repeatable','09082026_0712_banking_pay_semantic_ready_helpers.sql');
const expansion = read('supabase','repeatable','04082026_1208_pay_payment_correction_expand_work.sql');
const cancelSafe = read('supabase','repeatable','19072026_1816_cancel_refresh_supersede_finance_dirty.sql');
const financeAdjustments = read('supabase','repeatable','21072026_1235_49_pay_batch_apply_finance_adjustments.sql');

const draftWriters = [
  read('supabase','repeatable','26052026_2100HRS_NEW_FUNCTIONS.sql'),
  read('supabase','repeatable','21072026_1235_49_pay_batch_apply_finance_adjustments.sql'),
  read('supabase','repeatable','31072026_1720_pay_batch_nested_resolution_breakdowns.sql'),
].join('\n');

test('Draft self-invalidation controls are independent and disabled by default', () => {
  for (const setting of [
    'banking_pay_draft_expected_effects_v1_enabled',
    'banking_pay_draft_self_invalidation_claim_deferral_v1_enabled',
    'banking_pay_draft_create_adoption_v1_enabled',
  ]) {
    assert.match(controls, new RegExp(`${setting} boolean NOT NULL DEFAULT false`));
  }
});

test('Draft expected effects are transaction-local, operation-bound and fail closed', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_draft_expected_effects_v1/);
  assert.match(helpers, /pg_backend_pid\(\)/);
  assert.match(helpers, /txid_current\(\)/);
  assert.match(helpers, /operation_type,''\)\)\) <> 'DRAFT_CREATE'/);
  assert.match(helpers, /banking_pay_operation_candidate_scope/);
  assert.match(helpers, /ASSERT_COMPLETE/);
  assert.match(helpers, /DRAFT_EXPECTED_EFFECT_MISMATCH/);
  assert.match(helpers, /REVOKE ALL ON FUNCTION private\.pay_workbench_draft_expected_effects_v1[\s\S]*FROM PUBLIC,anon,authenticated,service_role/);
});

test('all Draft writers register and assert their exact mutation phase', () => {
  for (const phase of [
    'INSERT_ITEMS',
    'APPLY_FINANCE_ADJUSTMENTS',
    'FINALISE_RESERVATIONS',
    'CREATE_TIMESHEET_SNAPSHOTS',
    'BUILD_ITEM_BREAKDOWNS',
  ]) {
    assert.match(draftWriters, new RegExp(`'${phase}'`));
  }
  assert.ok((draftWriters.match(/private\.pay_workbench_draft_expected_effects_v1/g) || []).length >= 10);
});

test('finance adjustments register expected effects on the normal path before any retry cleanup or materialisation', () => {
  const registerNeedle = "p_operation_id,'APPLY_FINANCE_ADJUSTMENTS','REGISTER'";
  const partialRetryNeedle = 'IF coalesce(v_operation_allocation_done, 0) > 0';
  const firstFinanceInsertNeedle = 'insert into public.pay_batch_items';
  const assertNeedle = "p_operation_id,'APPLY_FINANCE_ADJUSTMENTS','ASSERT_COMPLETE'";

  const registerIndex = financeAdjustments.indexOf(registerNeedle);
  const partialRetryIndex = financeAdjustments.indexOf(partialRetryNeedle);
  const firstFinanceInsertIndex = financeAdjustments.indexOf(firstFinanceInsertNeedle);
  const assertIndex = financeAdjustments.indexOf(assertNeedle);

  assert.ok(registerIndex > -1);
  assert.ok(partialRetryIndex > registerIndex);
  assert.ok(firstFinanceInsertIndex > registerIndex);
  assert.ok(assertIndex > firstFinanceInsertIndex);
  assert.equal(financeAdjustments.split(registerNeedle).length - 1, 1);
});

test('financial dirty trigger consumes only exact same-transaction Draft effects', () => {
  assert.match(trigger, /_bpay_wb_draft_expected_effect_context_v1/);
  assert.match(trigger, /backend_pid=pg_catalog\.pg_backend_pid\(\)/);
  assert.match(trigger, /transaction_id=pg_catalog\.txid_current\(\)/);
  assert.match(trigger, /operation_scope\.operation_id=context_row\.operation_id/);
  assert.match(trigger, /operation_scope\.candidate_id=impact\.candidate_id/);
  assert.match(trigger, /expected\.value->>'relation_name'/);
  assert.match(trigger, /expected\.value->>'operation'/);
  assert.match(trigger, /DELETE FROM pg_temp\._bpay_wb_transition_impacts_v1/);
  assert.match(trigger, /observed\.matched/);
});

test('claim deferral is exact, bounded and reuses the same queued job', () => {
  assert.match(claim, /DRAFT_CREATE_SELF_INVALIDATION_DEFERRED/);
  assert.match(claim, /v_draft_deferral_count<12/);
  assert.match(claim, /WHEN v_draft_deferral_count=0 THEN 2/);
  assert.match(claim, /WHEN v_draft_deferral_count=1 THEN 3/);
  assert.match(claim, /ELSE 5/);
  assert.match(claim, /draft_operation_id/);
  assert.match(claim, /draft_context_token/);
  assert.match(claim, /draft_phase/);
  assert.doesNotMatch(claim, /INSERT INTO public\.banking_pay_workbench_jobs[\s\S]{0,800}?DRAFT_CREATE_SELF_INVALIDATION_DEFERRED/);
});

test('post-create adoption is strict V3 current authority and never rewrites economics', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_draft_create_adoption_finalize_v1/);
  assert.match(helpers, /v_operation\.phase,''\)\)\)<>'POST_CREATE_REFRESH'/);
  assert.match(helpers, /v_operation\.status,''\)\)\)='WAITING'/);
  assert.match(helpers, /v_operation\.runner_state,''\)\)\)='RUNNABLE'/);
  assert.match(helpers, /operation_batch_scope\.pay_batch_id=p_pay_batch_id/);
  assert.doesNotMatch(helpers, /v_operation\.pay_batch_id IS DISTINCT FROM p_pay_batch_id/);
  assert.match(helpers, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3/);
  assert.match(helpers, /READY_TO_PAY_SEMANTIC_V2/);
  assert.match(helpers, /pending_job_id IS NOT NULL/);
  assert.match(helpers, /private\.pay_workbench_semantic_ready_proof_page_v1/);
  assert.match(patch, /private\.pay_workbench_draft_create_adoption_finalize_v1/);
  assert.doesNotMatch(helpers, /UPDATE public\.banking_pay_workbench_candidate_source_lines/);
});

test('negative ordinary parents cannot remain Ready and are separately reported', () => {
  assert.match(builder, /NEGATIVE_ORDINARY_PRESENTATION_ONLY/);
  assert.match(builder, /'presentation_section', 'BLOCKED_FOR_PAY'/);
  assert.match(builder, /case_needs_resolution/);
  assert.match(summary, /negative_ordinary_blocked_count/);
  assert.match(summary, /negative_ordinary_cases_count/);
  assert.match(summary, /invalid_ready_negative_parent_count/);
  assert.match(semantic, /invalid_ready_negative_parent_count = 0/);
  assert.match(semantic, /presentation_section_digest/);
});

test('untouched Draft routing is derived from authority rather than requested label', () => {
  assert.match(expansion, /DRAFT_OVERLAY_FAST/);
  assert.doesNotMatch(expansion, /v_requested_action\s*=\s*'DRAFT_CANCEL'[\s\S]{0,300}?DRAFT_OVERLAY_FAST/);
});

test('changed cancellation candidates enter canonical election without pre-forced full mode', () => {
  assert.match(cancelSafe, /defer_complex_enqueue/);
  assert.match(cancelSafe, /TARGETED_TIMESHEETS/);
  assert.doesNotMatch(cancelSafe, /'force_legacy',\s*true/);
  assert.doesNotMatch(cancelSafe, /'projection_mode',\s*'LEGACY'/);
  assert.doesNotMatch(cancelSafe, /'source_build_required',\s*true/);
  assert.doesNotMatch(cancelSafe, /SET status\s*=\s*'SUCCEEDED'[\s\S]{0,500}?FINANCE_DIRTY/);
});
