import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..');
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8');

const canonical = read('supabase', 'repeatable', '05082026_1545_pay_preview_candidate_build_canonical_lines.sql');
const recovery = read('supabase', 'repeatable', '09082026_0712_banking_pay_semantic_ready_helpers.sql');
const semanticSelection = read('supabase', 'repeatable', '09082026_1727_pay_workbench_session_set_selected_rows_semantic_overlay.sql');
const refresh = read('supabase', 'repeatable', '11082026_1557_pay_workbench_session_refresh_current_authority_v1.sql');
const clearResolution = read('supabase', 'repeatable', '21072026_1235_42_pay_workbench_session_clear_case_resolution.sql');
const candidatePreview = read('supabase', 'repeatable', '16082026_2035_pay_workbench_candidate_preview_effective_section_v1.sql');
const sessionPreviewPage = read('supabase', 'repeatable', '20072026_0117_banking_pay_preview_selection_revision.sql');
const replay = read('supabase', 'repeatable', '08082026_0902_reassert_authorities_after_legacy_monolith.sql');

test('resolved timesheet allocation children retain exact segment and rate-resolution authority', () => {
  for (const token of [
    'PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_MISSING',
    'PAY_WORKBENCH_ALLOCATION_SEGMENT_IDENTITY_AMBIGUOUS',
    "segment_row.segment_stable_key is not distinct from component_rows.stable_component_identity",
    "'source_basis_fingerprint', eligible_components.component_json->'source_basis_fingerprint'",
    "'source_basis_json', eligible_components.component_json->'source_basis_json'",
    "'source_units', eligible_components.component_json->'source_units'",
    "'source_rate', eligible_components.component_json->'source_rate'",
    "'source_charge_rate', eligible_components.component_json->'source_charge_rate'",
    "'resolved_rate_clear_payload_json'",
    "'case_resolution_summary_json'",
    "'role', eligible_components.matched_segment_json->'role'",
    "'band', eligible_components.matched_segment_json->'band'",
    "'start', eligible_components.matched_segment_json->'start'",
    "'finish', eligible_components.matched_segment_json->'finish'"
  ]) assert.match(canonical, new RegExp(token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'));

  assert.match(canonical, /not exists\s*\([\s\S]*exact_segment[\s\S]*segment_row\.segment_date::text is not distinct from component_rows\.component_key_value/i);
  assert.doesNotMatch(canonical, /component_rows\.component_key_type <> 'TS_DAY'[\s\S]{0,400}ready_segment\.segment_stable_key[\s\S]{0,180}\bor\b[\s\S]{0,180}ready_segment\.segment_date/i);
});

test('recovery overlay allocates canonical child components exactly and fails closed', () => {
  for (const failure of [
    'PAY_WORKBENCH_RECOVERY_COMPONENT_IDENTITY_INCOMPLETE',
    'PAY_WORKBENCH_RECOVERY_COMPONENT_IDENTITY_DUPLICATE',
    'PAY_WORKBENCH_RECOVERY_COMPONENT_AUTHORITY_MISSING',
    'PAY_WORKBENCH_RECOVERY_COMPONENT_CAPACITY_INSUFFICIENT',
    'PAY_WORKBENCH_RECOVERY_COMPONENT_PENNY_RECONCILIATION_FAILED'
  ]) assert.match(recovery, new RegExp(failure));

  assert.match(recovery, /recoverable_amount_ex_vat[\s\S]*prior_component_capacity_ex_vat[\s\S]*penny_residual_ex_vat[\s\S]*component_recoverable_ex_vat/i);
  assert.match(recovery, /'preview_due_amount_ex_vat', recovery_component_final\.component_recoverable_ex_vat/i);
  assert.match(recovery, /'allocated_source_due_amount_ex_vat', recovery_component_final\.component_recoverable_ex_vat/i);
  assert.match(recovery, /'case_components', overlay_row\.allocated_case_components/i);
  assert.match(recovery, /WHEN allocated_recovery\.actionable_restructure\s+THEN 'cases_resolutions'/i);
  assert.match(recovery, /WHEN allocated_recovery\.static_recovery_eligible[\s\S]*recoverable_amount_ex_vat > 0[\s\S]*THEN 'canonical_preview_lines'/i);
  assert.match(recovery, /ELSE 'blocked_for_pay'/i);
  assert.match(recovery, /'physical_sections_changed', false/i);
  assert.doesNotMatch(recovery, /SET\s+section\s*=/i);
});

test('same-session rebase is version-only, parity-fenced and preserves physical authority', () => {
  assert.match(refresh, /CREATE OR REPLACE FUNCTION private\.pay_workbench_candidate_session_version_rebase_v1/i);
  assert.match(refresh, /p_from_session_version bigint[\s\S]*p_to_session_version bigint[\s\S]*p_actor_user_id uuid/i);
  assert.match(refresh, /v_from_version := p_from_session_version/i);
  assert.match(refresh, /evaluated_generation IS DISTINCT FROM v_registry\.dirty_generation/i);
  assert.match(refresh, /VERSION_REBASE_PARITY_NOT_EXACT/i);
  assert.match(refresh, /v_session\.server_selected_preview_row_ids_provided IS NOT TRUE\s*OR v_selected_preview_count = v_selected_session_count/i);
  assert.match(refresh, /VERSION_REBASE_SELECTION_NOT_EXACT/i);
  assert.match(refresh, /source_row_json, old_source\.economic_key_json/i);
  assert.match(refresh, /old_source\.section, old_source\.source_row_json/i);
  assert.match(refresh, /physical_section_preserved', true/i);
  assert.match(refresh, /WORKBENCH_SESSION_VERSION_REBASE_POST_FENCE_FAILED/i);
  assert.match(refresh, /private\.pay_workbench_candidate_physical_currentness_page_v1/i);
  assert.match(refresh, /public\.pay_workbench_delta_update_candidate_state_v1/i);
  assert.match(refresh, /'route', 'SESSION_VERSION_REBASE'/i);
  assert.match(refresh, /VERSION_REBASE_TRANSACTION_ROLLED_BACK/i);
  assert.match(refresh, /'post_draft_artifacts_touched', false/i);
});

test('every mutating Cancel Resolution branch converges the new session version after the affected owner is proven', () => {
  const versionAdvanceCount = (clearResolution.match(/SET version = session_update\.version \+ 1/gi) || []).length;
  const refreshOwnerCount = (clearResolution.match(/public\.pay_workbench_session_refresh_current_authority_v1\(/gi) || []).length;

  assert.equal(versionAdvanceCount, 3);
  assert.equal(refreshOwnerCount, versionAdvanceCount);
  assert.equal((clearResolution.match(/WORKBENCH_SESSION_VERSION_REFRESH_NOT_PROVEN/gi) || []).length, versionAdvanceCount * 2);
  assert.equal((clearResolution.match(/WORKBENCH_SESSION_VERSION_REFRESH_CURSOR_INVALID/gi) || []).length, versionAdvanceCount * 2);
  assert.equal((clearResolution.match(/'session_version_refresh_pages', v_session_refresh_pages/gi) || []).length, versionAdvanceCount);
  assert.equal((clearResolution.match(/'session_version_refresh_page_count', v_session_refresh_page_count/gi) || []).length, versionAdvanceCount);

  for (const mutationBranch of clearResolution.split(/SET version = session_update\.version \+ 1/i).slice(1)) {
    const enqueueAt = mutationBranch.indexOf('pay_workbench_enqueue_session_candidate_refresh');
    const proveOwnerAt = mutationBranch.indexOf('IF v_job_id IS NULL');
    const convergeAt = mutationBranch.indexOf('pay_workbench_session_refresh_current_authority_v1');
    assert.ok(enqueueAt >= 0 && proveOwnerAt > enqueueAt && convergeAt > proveOwnerAt);
  }
});

test('candidate preview pages by effective section and exposes physical section only as a diagnostic', () => {
  assert.match(candidatePreview, /private\.pay_workbench_preview_effective_section_v1\(/i);
  assert.match(candidatePreview, /ORDER BY eligible_rows\.effective_section, eligible_rows\.row_ordinal, eligible_rows\.id/i);
  assert.match(candidatePreview, /'section', limited_rows\.effective_section/i);
  assert.match(candidatePreview, /'effective_section', limited_rows\.effective_section/i);
  assert.match(candidatePreview, /'physical_section', limited_rows\.physical_section/i);
  assert.match(candidatePreview, /'cursor_scheme', 'effective_section_row_ordinal_id'/i);
  assert.match(candidatePreview, /SECURITY DEFINER[\s\S]*SET search_path TO 'public'/i);
  assert.match(candidatePreview, /GRANT EXECUTE[\s\S]*TO service_role/i);
  assert.match(candidatePreview, /actionable_sibling[\s\S]*finance_case_id[\s\S]*cases_resolutions/i);
  assert.match(candidatePreview, /NEGATIVE_ORDINARY_PRESENTATION_ONLY[\s\S]*recovery_sibling[\s\S]*recovery_sibling\.timesheet_id = preview_row\.timesheet_id[\s\S]*OVERPAYMENT_RECOVERY/i);
  assert.match(candidatePreview, /'presentation_section', CASE limited_rows\.effective_section/i);
  assert.match(candidatePreview, /'selection_allowed', CASE[\s\S]*ELSE false/i);
  const previewReplayAt = replay.indexOf('\\ir 16082026_2035_pay_workbench_candidate_preview_effective_section_v1.sql');
  const financeReplayAt = replay.indexOf('\\ir 17082026_2052_pay_finance_resolution_cancel_authority.sql');
  const finalReplayAt = replay.indexOf('\\ir 19072026_1816_cancel_refresh_supersede_finance_dirty.sql');
  assert.ok(previewReplayAt >= 0 && financeReplayAt > previewReplayAt && finalReplayAt > financeReplayAt,
    'preview, finance and final cancellation authorities must retain their relative replay order');
});

test('normal session preview page uses the same effective routing and suppresses dominated recovery carriers', () => {
  assert.match(sessionPreviewPage, /private\.pay_workbench_preview_effective_section_v1\(\s*preview_count_row\.section, preview_count_row\.row_json/i);
  assert.match(sessionPreviewPage, /private\.pay_workbench_preview_effective_section_v1\(\s*preview_row\.section, preview_row\.row_json/i);
  assert.match(sessionPreviewPage, /actionable_sibling[\s\S]*finance_case_id[\s\S]*cases_resolutions/i);
  assert.match(sessionPreviewPage, /NEGATIVE_ORDINARY_PRESENTATION_ONLY[\s\S]*recovery_sibling[\s\S]*recovery_sibling\.timesheet_id = preview_count_row\.timesheet_id[\s\S]*OVERPAYMENT_RECOVERY/i);
  assert.match(sessionPreviewPage, /NEGATIVE_ORDINARY_PRESENTATION_ONLY[\s\S]*recovery_sibling[\s\S]*recovery_sibling\.timesheet_id = preview_row\.timesheet_id[\s\S]*OVERPAYMENT_RECOVERY/i);
  assert.match(sessionPreviewPage, /'effective_section', limited_rows\.section/i);
  assert.match(sessionPreviewPage, /'physical_section', limited_rows\.physical_section/i);
  assert.match(sessionPreviewPage, /WHEN limited_rows\.section <> 'canonical_preview_lines' THEN false/i);
});

test('actionable taxable restructure is a read-time Cases authority even before a headroom overlay is refreshed', () => {
  assert.match(semanticSelection, /case_needs_resolution/i);
  assert.match(semanticSelection, /TAXABLE_CHANNEL_RESTRUCTURE/i);
  assert.match(semanticSelection, /taxable_channel_restructure,can_apply/i);
  assert.match(semanticSelection, /THEN 'cases_resolutions'/i);
});

test('the bounded fix does not redefine frozen James or post-Draft economic owners', () => {
  const combined = [canonical, recovery, semanticSelection, refresh, candidatePreview, sessionPreviewPage].join('\n');
  for (const frozenOwner of [
    'pay_workbench_unit_economic_occurrence_page_v1',
    'pay_workbench_sealed_rate_component_projection_v1',
    'pay_sync_overpayments_from_workbench_workspace_v1',
    'pay_workbench_session_apply_case_resolution',
    'banking_pay_draft_create_step_v1',
    'pay_payment_execute',
    'pay_payment_settle'
  ]) {
    assert.doesNotMatch(combined, new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+(?:public|private)\\.${frozenOwner}\\b`, 'i'));
  }
});
