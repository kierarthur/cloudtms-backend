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
const refresh = read('supabase', 'repeatable', '11082026_1557_pay_workbench_session_refresh_current_authority_v1.sql');
const candidatePreview = read('supabase', 'repeatable', '16082026_2035_pay_workbench_candidate_preview_effective_section_v1.sql');
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

test('candidate preview pages by effective section and exposes physical section only as a diagnostic', () => {
  assert.match(candidatePreview, /private\.pay_workbench_preview_effective_section_v1\(/i);
  assert.match(candidatePreview, /ORDER BY eligible_rows\.effective_section, eligible_rows\.row_ordinal, eligible_rows\.id/i);
  assert.match(candidatePreview, /'section', limited_rows\.effective_section/i);
  assert.match(candidatePreview, /'effective_section', limited_rows\.effective_section/i);
  assert.match(candidatePreview, /'physical_section', limited_rows\.physical_section/i);
  assert.match(candidatePreview, /'cursor_scheme', 'effective_section_row_ordinal_id'/i);
  assert.match(candidatePreview, /SECURITY DEFINER[\s\S]*SET search_path TO 'public'/i);
  assert.match(candidatePreview, /GRANT EXECUTE[\s\S]*TO service_role/i);
  assert.match(replay, /\\ir 16082026_2035_pay_workbench_candidate_preview_effective_section_v1\.sql\s+\\ir 19072026_1816_cancel_refresh_supersede_finance_dirty\.sql/i);
});

test('the bounded fix does not redefine frozen James or post-Draft economic owners', () => {
  const combined = [canonical, recovery, refresh, candidatePreview].join('\n');
  for (const frozenOwner of [
    'pay_workbench_unit_economic_occurrence_page_v1',
    'pay_workbench_sealed_rate_component_projection_v1',
    'pay_sync_overpayments_from_workbench_workspace_v1',
    'pay_workbench_session_apply_case_resolution',
    'pay_workbench_session_clear_case_resolution',
    'banking_pay_draft_create_step_v1',
    'pay_payment_execute',
    'pay_payment_settle'
  ]) {
    assert.doesNotMatch(combined, new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+(?:public|private)\\.${frozenOwner}\\b`, 'i'));
  }
});
