const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8');

const migration = read('supabase', 'migrations',
  '09082026_0713_banking_pay_semantic_ready_v3_and_cancellation_reversion.sql');
const helpers = read('supabase', 'repeatable',
  '09082026_0712_banking_pay_semantic_ready_helpers.sql');
const builder = read('supabase', 'repeatable',
  '05082026_1545_pay_preview_candidate_build_canonical_lines.sql');
const summary = read('supabase', 'repeatable',
  '09082026_0826_pay_preview_candidate_build_summary_fragment.sql');
const publisher = read('supabase', 'repeatable',
  '07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql');
const complete = read('supabase', 'repeatable',
  '04082026_1219_pay_workbench_complete_job.sql');
const progress = read('supabase', 'repeatable',
  '07082026_2155_pay_workbench_session_recompute_progress_counters.sql');
const scopeSeed = read('supabase', 'repeatable',
  '21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql');
const allocationSeed = read('supabase', 'repeatable',
  '08082026_0717_pay_workbench_prepare_draft_allocation_rows_seed_sort_order.sql');
const expand = read('supabase', 'repeatable',
  '04082026_1208_pay_payment_correction_expand_work.sql');
const processChunk = read('supabase', 'repeatable',
  '04082026_1209_pay_payment_correction_process_chunk.sql');
const patch = read('supabase', 'repeatable',
  '09082026_0825_pay_workbench_patch_preview_after_batch_mutation.sql');
const cancelSafe = read('supabase', 'repeatable',
  '19072026_1816_cancel_refresh_supersede_finance_dirty.sql');
const enqueue = read('supabase', 'repeatable',
  '07082026_1017_pay_workbench_enqueue_candidate_refresh.sql');
const delta = read('supabase', 'repeatable',
  '07082026_1011_banking_pay_targeted_delta_helpers.sql');
const clone = read('supabase', 'repeatable',
  '04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql');
const reconciliation = read('supabase', 'repeatable',
  '07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql');
const workflow = read('.github', 'workflows', 'supabase-migrate.yml');
const semanticVerifier = read('supabase', 'verification',
  'verify_banking_pay_semantic_ready_cancellation_reversion_catalog.mjs');
const manifestNames = [
  'banking_pay_revision5_catalog_manifest.json',
  'banking_pay_workbench_certified_source_preview_catalog_manifest.json',
  'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json',
  'banking_pay_semantic_ready_cancellation_reversion_catalog_manifest.json',
];
const manifests = manifestNames.map((name) => JSON.parse(read('supabase', 'verification', name)));
const reassert = read('supabase', 'repeatable',
  '08082026_0902_reassert_authorities_after_legacy_monolith.sql');
const operationFinish = read('supabase', 'repeatable',
  '09082026_1128_banking_pay_operation_finish_post_draft_authority.sql');
const correctionStart = read('supabase', 'repeatable',
  '04082026_1207_pay_payment_correction_request_start.sql');
const recoveryRevalidation = read('supabase', 'repeatable',
  '19072026_1405_revalidate_recovery_headroom_after_materialisation.sql');
const previewPage = read('supabase', 'repeatable',
  '20072026_0117_banking_pay_preview_selection_revision.sql');
const semanticSelection = read('supabase', 'repeatable',
  '09082026_1727_pay_workbench_session_set_selected_rows_semantic_overlay.sql');
const selectionCarry = read('supabase', 'repeatable',
  '25072026_2153_banking_pay_selection_carry_runtime.sql');
const broker = read('broker', 'src', 'index.js');
const legacyFunctions = read('supabase', 'repeatable', '26052026_2100HRS_NEW_FUNCTIONS.sql');

test('semantic V3 and cancellation controls install disabled with a fail-closed dependency', () => {
  for (const setting of [
    'banking_pay_workbench_semantic_ready_observe_v2_enabled',
    'banking_pay_workbench_semantic_ready_publication_v3_enabled',
    'banking_pay_workbench_semantic_ready_draft_guard_v2_enabled',
    'banking_pay_cancellation_reversion_observe_v1_enabled',
    'banking_pay_cancellation_reversion_publish_v1_enabled',
    'banking_pay_cancellation_reversion_exact_empty_v1_enabled',
    'banking_pay_draft_overlay_fast_cancel_v1_enabled',
  ]) {
    assert.match(migration, new RegExp(`${setting} boolean NOT NULL DEFAULT false`));
    assert.match(migration, new RegExp(`${setting} = false`));
  }
  assert.match(migration, /settings_bpay_semantic_cancellation_dependency_ck/);
  assert.match(migration, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3/);
  assert.match(migration, /READY_TO_PAY_SEMANTIC_V2/);
  assert.match(migration, /CERTIFIED_CANCELLATION_REVERSION/);
});

test('canonical rows split presentation parents from exact positive allocation components only under V3', () => {
  assert.match(builder, /timesheet_allocation_component_lines/);
  assert.match(builder, /'presentation_role', 'ALLOCATION_COMPONENT'/);
  assert.match(builder, /component_key_type in \('TS_DAY','EXPENSE_CODE','ADDITIONAL_CODE','ADJUSTMENT_CODE'\)/i);
  assert.match(builder, /component_amount_ex_vat > 0/);
  assert.match(builder, /requires_resolution is not true/i);
  assert.match(builder, /where v_semantic_ready_publication_enabled/i);
  assert.match(builder, /CASE WHEN v_semantic_ready_publication_enabled THEN false ELSE/i);
  assert.match(builder, /'is_excluded_from_allocation', false/);
  assert.match(builder, /'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'/);
});

test('reconciliation keeps its presentation fence while validating V3 allocation children independently', () => {
  assert.match(reconciliation, /semantic_non_selectable_parent/);
  assert.match(reconciliation, /presentation_role','PARENT'/);
  assert.match(reconciliation, /<> 'ALLOCATION_COMPONENT'/);
  assert.match(reconciliation, /PAY_WORKBENCH_CANONICAL_PRESENTATION_ALLOCATION_MISMATCH/);
  assert.match(reconciliation, /NEGATIVE_ORDINARY_PRESENTATION_ONLY/);
  assert.match(reconciliation, /actual\.semantic_negative_parent/);
  assert.match(reconciliation, /actual\.presentation_section='BLOCKED_FOR_PAY'/);
  assert.match(reconciliation, /expected\.presentation_section='READY_TO_PAY'/);
  assert.match(reconciliation, /PAY_WORKBENCH_CANONICAL_FACT_COMPONENT_MISMATCH/);
});

test('V3 caps recognised recoveries to exact same-candidate allocation headroom', () => {
  assert.match(builder, /create temporary table semantic_finance_case_lines/i);
  assert.match(builder, /ordinary_positive_headroom/i);
  assert.match(builder, /prior_semantic_recovery_amount/i);
  assert.match(builder, /semantic_recovery_sort_at_utc nulls last/i);
  assert.match(builder, /least\([\s\S]{0,220}?ordinary_positive_headroom-ranked\.prior_semantic_recovery_amount/i);
  assert.match(builder, /'semantic_recovery_headroom_capped'/i);
  assert.match(builder, /'NO_PAY_HEADROOM'/i);
  assert.match(builder, /'proposed_semantic_ready_amount'/i);
});

test('V3 recovery headroom follows selected positive rows without changing certified identities', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_recovery_selection_overlay_apply_v1/);
  assert.match(helpers, /positive_row\.selected IS TRUE/);
  assert.match(helpers, /positive_row\.selection_state[\s\S]{0,100}?= 'SELECTED'/);
  assert.match(helpers, /presentation_role'[\s\S]{0,100}?ALLOCATION_COMPONENT/);
  assert.match(helpers, /PARTITION BY recovery_base\.pay_channel/);
  assert.match(
    helpers,
    /WHEN recovery_base\.static_recovery_eligible[\s\S]{0,120}?THEN recovery_base\.nominal_due_amount_ex_vat/,
  );
  assert.match(helpers, /selected_positive_headroom_ex_vat[\s\S]{0,300}?prior_nominal_due_amount_ex_vat/);
  assert.match(helpers, /PAY_WORKBENCH_RECOVERY_SELECTION_OVERLAY_DUPLICATE_IDENTITY/);
  assert.match(helpers, /'effective_section'[\s\S]{0,160}?overlay_row\.effective_section/);
  assert.match(helpers, /'ready_for_draft',[\s\S]{0,120}?v_selected_row_count/);
  assert.match(helpers, /'draft_blocker_codes',[\s\S]{0,120}?v_draft_blocker_codes/);
  assert.match(helpers, /'physical_sections_changed', false/);
  assert.match(helpers, /'source_rows_changed', false/);
  assert.match(helpers, /REVOKE ALL ON FUNCTION private\.pay_workbench_recovery_selection_overlay_apply_v1[\s\S]*FROM PUBLIC, anon, authenticated, service_role/);
});

test('V3 selection revalidation uses the overlay and skips legacy physical section movement', () => {
  assert.match(recoveryRevalidation, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3/);
  assert.match(recoveryRevalidation, /private\.pay_workbench_recovery_selection_overlay_apply_v1/);
  assert.match(recoveryRevalidation, /legacy_physical_section_revalidation_skipped/);
  assert.match(recoveryRevalidation, /certified_source_preview_identity_preserved/);
});

test('newly eligible recovery defaults selected while explicit user deselection remains authoritative', () => {
  assert.match(semanticSelection, /'selection_user_override', 'SELECTED'/);
  assert.match(semanticSelection, /'selection_user_override', 'UNSELECTED'/);
  assert.match(semanticSelection, /'selection_origin'[\s\S]{0,180}?THEN 'USER_EXPLICIT_SELECT'/);
  assert.match(semanticSelection, /'selection_origin'[\s\S]{0,220}?THEN 'USER_EXPLICIT_DESELECT'/);
  assert.match(
    recoveryRevalidation,
    /base_row_json->>'selection_user_override'[\s\S]{0,180}?existing_ready\.row_json->>'selection_user_override'/,
  );
  assert.match(
    recoveryRevalidation,
    /selection_user_override'[\s\S]{0,120}?= 'UNSELECTED'[\s\S]{0,80}?THEN false[\s\S]{0,50}?ELSE true/,
  );
  assert.match(recoveryRevalidation, /SERVER_DEFAULT_NEWLY_ELIGIBLE/);
  assert.match(helpers, /AS explicitly_user_unselected/);
  assert.match(
    helpers,
    /effective_section = 'canonical_preview_lines'[\s\S]{0,120}?explicitly_user_unselected IS NOT TRUE/,
  );
  assert.match(
    helpers,
    /explicitly_user_unselected IS TRUE THEN 'UNSELECTED'[\s\S]{0,80}?ELSE 'SELECTED'/,
  );
  assert.match(publisher, /WHEN v_contract_version = 3 THEN/);
  assert.match(publisher, /WHEN prepared_row\.existing_preview_id IS NULL THEN true/);
  assert.match(publisher, /existing_selection_user_override/);
  assert.match(publisher, /SERVER_DEFAULT_NEW_READY_LINE/);
  assert.match(
    selectionCarry,
    /selection_user_override'[\s\S]{0,180}?= upper\(btrim\(COALESCE\(source_preview\.selection_state/,
  );
  assert.match(selectionCarry, /'selection_origin', 'SESSION_REPLACEMENT_CARRY'/);
});

test('preview paging exposes each recovery through its single effective section', () => {
  assert.match(previewPage, /selection_recovery_headroom_v1,contract_version/);
  assert.match(previewPage, /selection_recovery_headroom_v1,effective_section/);
  assert.match(
    previewPage,
    /preview_count_row\.row_json#>>'\{selection_recovery_headroom_v1,effective_section\}'[\s\S]{0,180}?= v_resolved_section/,
  );
  assert.match(
    previewPage,
    /preview_row\.row_json#>>'\{selection_recovery_headroom_v1,effective_section\}'[\s\S]{0,180}?AS section/,
  );
});

test('selection mutation uses the same effective section without moving certified rows', () => {
  assert.match(
    semanticSelection,
    /CREATE OR REPLACE FUNCTION private\.pay_workbench_preview_effective_section_v1/,
  );
  assert.match(
    semanticSelection,
    /selection_recovery_headroom_v1,effective_section/,
  );
  assert.match(
    semanticSelection,
    /p_target_section\s*=>\s*private\.pay_workbench_preview_effective_section_v1/,
  );
  assert.match(
    semanticSelection,
    /lower\(private\.pay_workbench_preview_effective_section_v1\(preview_row\.section, preview_row\.row_json\)\)\s*=\s*'canonical_preview_lines'/,
  );
  assert.equal(
    (
      semanticSelection.match(
        /LOWER\(private\.pay_workbench_preview_effective_section_v1\(selected_row\.section, selected_row\.row_json\)\)\s*=\s*'canonical_preview_lines'/g,
      ) || []
    ).length,
    2,
    'global and row-patch responses must both return the authoritative selected-row set',
  );
  assert.doesNotMatch(
    semanticSelection,
    /LOWER\(private\.pay_workbench_preview_effective_section_v1\(selected_row\.section, selected_row\.row_json\)\)\s*=\s*'CANONICAL_PREVIEW_LINES'/,
    'a lower-cased section cannot be compared with an upper-cased literal',
  );
  assert.match(
    semanticSelection,
    /pay_workbench_revalidate_zero_retained_recovery_headroom_v1[\s\S]*private\.pay_workbench_preview_effective_section_v1/,
  );
  assert.doesNotMatch(
    semanticSelection,
    /SET\s+section\s*=/i,
  );
});

test('row-backed summaries exclude presentation parents and retain legacy behavior while disabled', () => {
  assert.match(summary, /v_semantic_ready_publication_enabled boolean := false/);
  assert.match(summary, /is_excluded_from_allocation/);
  assert.match(summary, /selection_allowed/);
  assert.match(summary, /COALESCE\(v_semantic_ready_publication_enabled,false\) IS NOT TRUE/);
  assert.match(summary, /semantic_ready_publication_enabled/);
});

test('semantic proof is bounded, same-candidate and postgres-only', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_semantic_ready_proof_page_v1/);
  assert.match(
    helpers,
    /selection_recovery_headroom_v1,effective_section[\s\S]{0,500}?AS line_contract/,
    'semantic proof must validate a recovery against its effective overlay section, not its immutable physical section',
  );
  assert.match(helpers, /v_candidate_count < 1 OR v_candidate_count > 100/);
  assert.match(helpers, /ordinary_positive_amount \+ rollup\.recognised_deduction_amount >= 0/);
  assert.match(helpers, /-rollup\.recognised_deduction_amount <= rollup\.ordinary_positive_amount/);
  assert.match(helpers, /'cross_candidate_headroom_used', false/);
  assert.match(helpers, /REVOKE ALL ON FUNCTION private\.pay_workbench_semantic_ready_proof_page_v1[\s\S]*FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(helpers, /GRANT EXECUTE ON FUNCTION private\.pay_workbench_semantic_ready_proof_page_v1[\s\S]*TO postgres/);
});

test('Draft scope and allocation insertion both reject recovery-only or negative candidate sets', () => {
  for (const source of [scopeSeed, allocationSeed]) {
    assert.match(source, /PAY_WORKBENCH_DRAFT_RECOVERY_WITHOUT_POSITIVE_HEADROOM/);
    assert.match(source, /PAY_WORKBENCH_DRAFT_DEDUCTION_EXCEEDS_SELECTED_HEADROOM/);
    assert.match(source, /PAY_WORKBENCH_DRAFT_CANDIDATE_RESULT_NEGATIVE/);
    assert.match(source, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3/);
    assert.match(source, /READY_TO_PAY_SEMANTIC_V2/);
  }
  assert.match(scopeSeed, /'source_publication_attestation'/);
  assert.match(scopeSeed, /'semantic_proof_digest'/);
});

test('Draft allocation expansion drops zero-value finance component rows before item insertion', () => {
  assert.match(
    allocationSeed,
    /FROM\s+allocation_expanded_rows\s+WHERE\s+ROUND\(COALESCE\(allocation_expanded_rows\.allocated_amount,\s*0\),\s*2\)\s*<>\s*0/i
  );
});

test('Draft allocation expansion caps finance components sequentially to the selected parent amount', () => {
  assert.match(
    allocationSeed,
    /ROWS\s+BETWEEN\s+UNBOUNDED\s+PRECEDING\s+AND\s+1\s+PRECEDING[\s\S]*AS\s+preceding_component_abs_amount/i,
  );
  assert.match(
    allocationSeed,
    /LEAST\([\s\S]*raw_component_abs_amount[\s\S]*ABS\(COALESCE\(finance_component_source_rows\.allocated_amount,\s*0\)\)[\s\S]*preceding_component_abs_amount/i,
  );
});

test('V3 publication composes structural parity with semantic parity without widening V1', () => {
  assert.match(publisher, /v_contract_version NOT IN \(1,2,3\)/);
  assert.match(
    publisher,
    /v_contract_version\s*=\s*1[\s\S]{0,500}?v_authority_kind\s*<>\s*'BOUNDED_FULL_SOURCE_BUILD'/i,
  );
  assert.match(publisher, /private\.pay_workbench_semantic_ready_proof_page_v1/);
  assert.match(
    publisher,
    /private\.pay_workbench_recovery_selection_overlay_apply_v1[\s\S]*private\.pay_workbench_semantic_ready_proof_page_v1/,
  );
  assert.match(publisher, /CERTIFIED_SOURCE_PREVIEW_SEMANTIC_PARITY_FAILED/);
  assert.match(publisher, /'attestation_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'/);
  assert.match(publisher, /'section_counts',v_section_counts\s*\)\s*\|\|\s*jsonb_build_object\(/);
  assert.match(publisher, /'semantic_proof_digest',v_semantic_proof_digest/);
  assert.match(complete, /banking_pay_workbench_semantic_ready_publication_v3_enabled/);
  assert.match(progress, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3/);
  assert.match(clone, /banking_pay_workbench_semantic_ready_publication_v3_enabled/);
  assert.match(delta, /banking_pay_workbench_semantic_ready_publication_v3_enabled/);
});

test('canonical enqueue promotes legacy current authority after V3 publication is enabled', () => {
  assert.match(enqueue, /v_semantic_ready_publication_enabled boolean := false/);
  assert.match(enqueue, /banking_pay_workbench_semantic_ready_publication_v3_enabled/);
  assert.match(
    enqueue,
    /v_semantic_ready_publication_enabled IS NOT TRUE[\s\S]*CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3/,
  );
  assert.match(enqueue, /READY_TO_PAY_SEMANTIC_V2/);
});

test('cancellation reversion uses deterministic candidate locks, immutable lineage and one page publication', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_cancel_reversion_admission_page_v1/);
  assert.match(helpers, /LEGACY_OR_SEMANTICALLY_UNCERTIFIED_SOURCE/);
  assert.match(helpers, /CURRENT_ECONOMIC_AUTHORITY_CHANGED/);
  assert.match(helpers, /ORIGINAL_SOURCE_DIGEST_MISMATCH/);
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_publish_certified_source_preview_page_v1/);
  assert.match(helpers, /pay_workbench_candidate_serial_key/);
  assert.match(helpers, /ORDER BY descriptor\.value->>'candidate_id'/);
  assert.match(helpers, /original_source_build_run_id/);
  assert.match(helpers, /cancellation_reversion_run_id/);
  assert.match(helpers, /private\.pay_workbench_publish_certified_source_preview_v1/);
  assert.match(helpers, /pay_workbench_session_recompute_progress_counters/);
  assert.match(processChunk, /private\.pay_workbench_cancel_reversion_admission_page_v1/);
  assert.match(processChunk, /private\.pay_workbench_publish_certified_source_preview_page_v1/);
  assert.match(processChunk, /CERTIFIED_CANCELLATION_REVERSION_COMPLETE/);
});

test('eligible no-provider financial reversions use one set-based page with a strict compatibility fallback', () => {
  assert.match(helpers, /CREATE TEMP TABLE pg_temp\._bpay_pre_cancel_fast_work/);
  assert.match(helpers, /CREATE TEMP TABLE pg_temp\._bpay_pre_cancel_fast_items/);
  assert.match(helpers, /provider_shape_count=0/);
  assert.match(helpers, /public\.pay_bank_transfer_events AS transfer_event/);
  assert.match(helpers, /public\.banking_pay_operation_transfer_scope AS transfer_scope/);
  assert.match(helpers, /INSERT INTO public\.pay_payment_correction_items\(/);
  assert.match(helpers, /UPDATE public\.pay_batch_items AS item_to_void/);
  assert.match(helpers, /CREATE TEMP TABLE pg_temp\._bpay_pre_cancel_fast_reservations/);
  assert.match(helpers, /INSERT INTO public\.pay_finance_case_events\(/);
  assert.match(helpers, /'set_based_work_item_count',v_fast_count/);
  assert.match(helpers, /v_result := public\.pay_pre_bank_cancel_apply_work_item\(v_work\.id,p_actor_user_id\)/);
  assert.match(helpers, /NOT EXISTS \([\s\S]*_bpay_pre_cancel_fast_work AS fast_work[\s\S]*fast_work\.work_item_id=work_row\.id/);
});

test('mixed cancellation failures fall through the canonical safe ladder without a forced legacy route', () => {
  assert.match(patch, /v_defer_complex_enqueue/);
  assert.match(patch, /IF NOT v_defer_complex_enqueue THEN/);
  assert.match(cancelSafe, /'defer_complex_enqueue',true/);
  assert.doesNotMatch(cancelSafe, /'force_legacy',\s*true/);
  assert.match(cancelSafe, /pay_workbench_enqueue_candidate_refresh/);
  assert.match(enqueue, /CERTIFIED_CANCELLATION_REVERSION/);
  assert.match(delta, /CERTIFIED_CANCELLATION_REVERSION/);
});

test('untouched Draft cancellation avoids financial work items, source builds and reconciliation', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_draft_overlay_remove_page_v1/);
  assert.match(helpers, /DRAFT_OVERLAY_FAST_NOT_UNTOUCHED/);
  assert.match(helpers, /execution_commit_state/);
  assert.match(helpers, /'financial_work_item_count',0/);
  assert.match(helpers, /'full_build_count',0,'reconciliation_count',0/);
  assert.match(expand, /private\.pay_workbench_draft_overlay_remove_page_v1/);
  assert.match(expand, /DRAFT_OVERLAY_FAST_COMPLETE/);
});

test('Draft completion freezes the exact post-Draft authority before fast cancellation is admitted', () => {
  assert.match(operationFinish, /POST_DRAFT_LIVE_AUTHORITY_V1/);
  assert.match(operationFinish, /'post_draft_authority'/);
  assert.match(operationFinish, /source_change_seq/);
  assert.match(operationFinish, /dirty_generation/);
  assert.match(helpers, /operation_scope_link\.operation_id=operation_row\.id/);
  assert.match(helpers, /operation_scope_link\.candidate_id=batch_candidate\.candidate_id/);
  assert.match(helpers, /operation_scope_link\.pay_batch_id=request_row\.pay_batch_id/);
  assert.match(helpers, /DRAFT_OVERLAY_PREFLIGHT/);
  assert.match(helpers, /CURRENT_ECONOMIC_AUTHORITY_CHANGED/);
  assert.match(helpers, /PRE_FINANCIAL/);
  assert.match(helpers, /POST_FINANCIAL/);
});

test('untouched Draft admission separates pre-request economic truth from cancellation-owned dirtying', () => {
  assert.match(correctionStart, /DRAFT_OVERLAY_FAST_PRE_REQUEST_AUTHORITY_V1/);
  assert.match(correctionStart, /draft_overlay_fast_pre_request_authorities/);
  assert.match(
    correctionStart,
    /v_selection\s*-\s*'command'\s*-\s*'draft_overlay_fast_pre_request_authorities'/,
  );
  assert.match(correctionStart, /ECONOMIC_AUTHORITY_CHANGED_BEFORE_CANCELLATION_START/);
  assert.match(correctionStart, /DRAFT_OVERLAY_FAST_START_AUTHORITY_V1/);
  assert.match(correctionStart, /draft_overlay_fast_start_authorities/);
  assert.match(correctionStart, /request_owned_dirty_job_id/);
  assert.match(correctionStart, /dirty_job\.session_id=v_batch\.source_workbench_session_id/);
  assert.match(correctionStart, /policy_x_dirtying_only/);
  assert.match(correctionStart, /economic_truth_mutation_allowed/);
  assert.match(correctionStart, /DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT/);
  assert.match(correctionStart, /DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE/);
  assert.match(correctionStart, /pay_workbench_candidate_serial_key/);
  assert.match(helpers, /PRE_REQUEST_ECONOMIC_AUTHORITY_NOT_CURRENT/);
  assert.match(helpers, /CANCELLATION_START_AUTHORITY_MISSING_OR_MISMATCH/);
  assert.match(helpers, /current_request_owned_dirty_job_id/);
  assert.match(helpers, /dirty_job\.session_id=p_session_id/);
  assert.match(helpers, /DIRTY_TRIGGER:PAY_BATCHES:UPDATE/);
  assert.match(helpers, /WORKBENCH_AUTHORITY_NOT_FROZEN_DRAFT_BASELINE/);
  assert.match(
    helpers,
    /draft_overlay_start_authority->>'source_change_seq'[\s\S]*CURRENT_ECONOMIC_AUTHORITY_CHANGED/,
  );
});

test('focused modern authorities are replayed after the historical omnibus', () => {
  assert.match(reassert, /\\ir 09082026_0825_pay_workbench_patch_preview_after_batch_mutation\.sql/);
  assert.match(reassert, /\\ir 09082026_0826_pay_preview_candidate_build_summary_fragment\.sql/);
  assert.match(reassert, /\\ir 09082026_1128_banking_pay_operation_finish_post_draft_authority\.sql/);
});

test('semantic and cancellation authorities have one exact catalogue owner and workflow verifier', () => {
  const semanticManifest = manifests.at(-1);
  assert.equal(semanticManifest.function_count, 27);
  assert.equal(semanticManifest.functions.length, 27);
  for (const identity of [
    'private.pay_workbench_draft_expected_effects_v1',
    'private.pay_workbench_draft_create_adoption_finalize_v1',
    'private.pay_workbench_financial_scope_dirty_transition_v1',
    'public.pay_batch_insert_items_from_preview',
    'public.pay_batch_apply_finance_adjustments',
    'public.pay_batch_finalize_reservations_and_markers',
    'public.pay_batch_create_timesheet_snapshots',
    'public.pay_batch_build_item_breakdowns',
    'public.pay_workbench_session_carry_forward_preview_selections_v1',
    'public.trg_banking_pay_preview_selection_carry_apply',
  ]) {
    const [schema, name] = identity.split('.');
    assert.ok(
      semanticManifest.functions.some((entry) => entry.schema === schema && entry.name === name),
      `${identity} must have one semantic-cancellation catalogue owner`,
    );
  }
  assert.match(semanticVerifier, /definition_sha256/);
  assert.match(semanticVerifier, /unexpected overload/);
  assert.match(semanticVerifier, /missing saved source file/);
  assert.match(workflow, /verify_banking_pay_semantic_ready_cancellation_reversion_catalog\.mjs/);

  const identities = manifests.flatMap((manifest) => manifest.functions.map((entry) =>
    `${entry.schema}.${entry.name}(${entry.identity_arguments})`));
  assert.equal(new Set(identities).size, identities.length);
});

test('changes heartbeat preserves each open batch watermark instead of forcing every signal changed', () => {
  const start = broker.indexOf('async function handleChangesPing(');
  const end = broker.indexOf('async function handleRolesGlobal', start);
  assert.ok(start >= 0);
  assert.ok(end > start);
  const body = broker.slice(start, end);

  assert.match(body, /watched_pay_batches/);
  assert.match(body, /watchedBatchDescriptorById/);
  assert.match(body, /watchedBatchDescriptors\.length >= 20/);
  assert.match(body, /p_known_version: descriptor\.known_version/);
  assert.match(body, /p_known_payment_status_version: descriptor\.known_payment_status_version/);
  assert.match(body, /p_known_correction_progress_version: descriptor\.known_correction_progress_version/);
  assert.match(body, /p_known_alert_version: descriptor\.known_alert_version/);
  assert.match(body, /p_known_overview_version: descriptor\.known_overview_version/);
  assert.doesNotMatch(body, /p_known_correction_progress_version:\s*null/);
});

test('whole-batch cancellation progress is one correction-request alert, not one alert per candidate', () => {
  assert.match(
    legacyFunctions,
    /WHEN v_alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'[\s\S]{0,260}?v_cancellation_operation_id/,
  );
  assert.match(
    legacyFunctions,
    /p_pay_batch_id::text \|\| ':' \|\| v_alert_kind \|\| ':' \|\| COALESCE\(\(SELECT cs\.correction_request_id::text FROM correction_scope AS cs\)/,
  );
  assert.match(
    legacyFunctions,
    /'cancellation_operation_id'[\s\S]{0,220}?SELECT cs\.correction_request_id/,
  );
  assert.doesNotMatch(
    legacyFunctions.slice(legacyFunctions.indexOf("WHEN v_alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'"), legacyFunctions.indexOf("WHEN v_alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'")),
    /candidate_id|work_item_id/,
  );
});
