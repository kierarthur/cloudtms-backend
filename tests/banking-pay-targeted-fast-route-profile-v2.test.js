import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

const migration = read('supabase/migrations/07082026_1011_banking_pay_targeted_fast_route_profile_v2.sql');
const helpers = read('supabase/repeatable/07082026_1011_banking_pay_targeted_delta_helpers.sql');
const claim = read('supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql');
const chunk = read('supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql');
const execute = read('supabase/repeatable/07082026_1014_pay_workbench_source_build_attempt_execute_v1.sql');
const reconcile = read('supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql');
const runtime = read('supabase/repeatable/07082026_1016_banking_pay_targeted_delta_runtime.sql');
const enqueue = read('supabase/repeatable/07082026_1017_pay_workbench_enqueue_candidate_refresh.sql');
const entitlementCache = read('supabase/repeatable/07082026_1357_pay_workbench_current_entitlement_components_cached_v1.sql');
const correctionResidual = read('supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql');
const effectNormaliser = read('supabase/repeatable/04082026_2315_pay_workbench_finance_effect_normalise_row_v1.sql');
const cloneBoundedCertification = read('supabase/repeatable/07082026_1430_pay_workbench_session_clone_bounded_certification_v1.sql');
const cloneEligibleRows = read('supabase/repeatable/04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql');

test('schema rollout is disabled and profile 1 by default', () => {
  for (const flag of [
    'banking_pay_workbench_delta_enable_simple_authorise',
    'banking_pay_workbench_delta_enable_simple_unauthorise',
    'banking_pay_workbench_delta_enable_exact_import_family'
  ]) {
    assert.match(migration, new RegExp(`${flag}\\s+boolean\\s+NOT NULL\\s+DEFAULT false`, 'i'));
  }
  assert.match(migration, /banking_pay_workbench_source_build_execution_profile_version\s+integer\s+NOT NULL\s+DEFAULT 1/i);
  assert.match(migration, /CHECK\s*\(banking_pay_workbench_source_build_execution_profile_version IN \(1,\s*2\)\)/i);
  assert.match(migration, /admission_seal_digest[^;]+admission_sealed_at_utc/is);
});

test('new authority helpers are private, fail closed and inaccessible to application roles', () => {
  for (const name of [
    'pay_workbench_session_candidate_scope_ensure_v1',
    'pay_workbench_targeted_delta_admission_v1',
    'pay_workbench_targeted_delta_scope_finalize_v1'
  ]) {
    assert.match(helpers, new RegExp(`CREATE OR REPLACE FUNCTION private\\.${name}\\(`));
    assert.match(helpers, new RegExp(`REVOKE ALL ON FUNCTION private\\.${name}[\\s\\S]+FROM PUBLIC,\\s*anon,\\s*authenticated,\\s*service_role`, 'i'));
  }
  assert.match(helpers, /SECURITY DEFINER\s+SET search_path = ''/i);
  assert.match(helpers, /TARGETED_DELTA_CONNECTED_AUTHORITY_PRESENT/);
  assert.match(helpers, /TARGETED_DELTA_SETTLED_AUTHORITY_PRESENT/);
  assert.match(helpers, /TARGETED_DELTA_FINANCE_AUTHORITY_PRESENT/);
  assert.match(helpers, /source_row\.status = 'DIRTY'[\s\S]+source_build_run_id IS DISTINCT FROM p_projection_run_id/);
  assert.match(helpers, /source_build\.status='COMPLETE'[\s\S]+source_delta\.status='COMPLETED'/);
  assert.match(helpers, /accepted_source_run_digest/);
  assert.doesNotMatch(helpers, /v_registry\.current_build_id IS NULL THEN/);
});

test('claim freezes one immutable execution profile and execute enforces it', () => {
  assert.match(claim, /'execution_profile_version',v_execution_profile_version/);
  assert.match(claim, /prior_attempt\.execution_profile_version IS DISTINCT FROM v_execution_profile_version/);
  assert.match(claim, /captured_candidate_generation,captured_source_change_seq,execution_profile_version/);
  assert.match(execute, /v_attempt\.execution_profile_version IS DISTINCT FROM v_execution_profile_version/);
  assert.match(execute, /set_config\(\s*'cloudtms\.pay_workbench_execution_profile_version'/);
  assert.match(execute, /v_execution_profile_version=1/);
});

test('profile 1 remains the exact legacy owner and profile 2 packs bounded logical work', () => {
  assert.match(chunk, /FUNCTION private\.pay_workbench_candidate_source_build_chunk_legacy_v1/);
  assert.match(chunk, /v_result := private\.pay_workbench_candidate_source_build_chunk_legacy_v1\([\s\S]+IF v_profile=1/);
  assert.match(chunk, /IF v_profile=1\s+OR v_stage <> 'WORKSPACE_FACT'/);
  assert.match(chunk, /WHILE v_step < 4/);
  assert.match(execute, /v_microsteps>=16/);
  assert.match(execute, /v_fact_page_records>=64/);
  assert.match(execute, /v_physical_fact_rows>=400/);
  assert.match(execute, /lease_expires_at_utc-interval '10 seconds'/);
  assert.doesNotMatch(execute, /LOOP[\s\S]{0,300}pay_workbench_source_build_attempt_execute_v1/);
});

test('profile 2 reconciliation reuses set-owned immutable workspace without changing effect attestation', () => {
  assert.match(reconcile, /tmp_sync_entitlement_components_v2/);
  assert.match(reconcile, /tmp_sync_settled_basis_by_timesheet_v2/);
  assert.match(reconcile, /v_bounded_scope_ids\s*\) current_fingerprint/);
  assert.match(reconcile, /v_effect_capture_mode/);
  assert.match(reconcile, /PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH/);
  assert.match(reconcile, /execution_profile_version/);
  assert.match(reconcile, /total_reconcile_ms/);
  assert.match(reconcile, /reconcile_execute_timing_json/);
  assert.match(reconcile, /attestation_json=COALESCE\(attestation_json,'\{\}'::jsonb\)\|\|jsonb_build_object/);
  assert.match(reconcile, /execution lineage, not component economic[\s\S]+source_basis_json',jsonb_build_object\(\s*'linked_timesheet_id'/i);
  assert.match(reconcile, /source_basis_json',jsonb_build_object\('build_id',p_build_id::text,[\s\S]+economic_authority','SEALED_ECONOMIC_BUILD_FACTS'/);
  assert.match(chunk, /reconcile_capture_timing_json/);
  assert.doesNotMatch(reconcile, /\bfact\.id\b/);
});

test('correction residuals reuse only exact entitlement inputs inside one valid profile-2 build transaction', () => {
  assert.match(entitlementCache, /private\.pay_workbench_current_entitlement_components_cached_v1/);
  assert.match(entitlementCache, /ARRAY_AGG\(DISTINCT input_id ORDER BY input_id\)/);
  assert.match(entitlementCache, /build_row\.build_token = v_sync_token[\s\S]+build_row\.status = 'RECONCILING'/);
  assert.match(entitlementCache, /public\._pay_current_timesheet_entitlement_components\(\s*v_timesheet_ids/);
  assert.match(entitlementCache, /PRIMARY KEY\(sync_token, cache_key, timesheet_id, key_type, key_value\)/);
  assert.match(entitlementCache, /input_timesheet_ids = v_timesheet_ids/);
  assert.match(entitlementCache, /REVOKE ALL ON FUNCTION[\s\S]+FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(correctionResidual, /private\.pay_workbench_current_entitlement_components_cached_v1\(\s*v_member_ids/);
  assert.doesNotMatch(entitlementCache, /_pay_current_timesheet_entitlement_components_from_build_v1/);
});

test('generated event parent UUIDs are removed only from digests while parent mapping remains exact', () => {
  assert.match(effectNormaliser, /v_relation='pay_finance_case_events'/);
  assert.match(effectNormaliser, /v_operation='INSERT'/);
  assert.match(effectNormaliser, /before_json[\s\S]+-'id'-'finance_case_id'-'finance_component_id'/);
  assert.match(effectNormaliser, /after_json[\s\S]+-'id'-'finance_case_id'-'finance_component_id'/);
  assert.match(effectNormaliser, /closed_at_utc','cleared_at_utc','written_off_at_utc/);
  assert.match(effectNormaliser, /v_before_value IS NULL[\s\S]+__GENERATED_NON_NULL__/);
  assert.match(effectNormaliser, /Existing\/manual timestamps, removals[\s\S]+remain exact/i);
  assert.match(effectNormaliser, /all economic, classification and lifecycle fields remain exact/i);
  assert.match(reconcile, /PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH/);
});

test('simple lifecycle routing shares one proof and preserves full fallback', () => {
  assert.ok((runtime.match(/private\.pay_workbench_targeted_delta_admission_v1\(/g) || []).length >= 3);
  assert.match(runtime, /banking_pay_workbench_delta_enable_simple_authorise/);
  assert.match(runtime, /banking_pay_workbench_delta_enable_simple_unauthorise/);
  assert.match(runtime, /new_scope_baseline_required/);
  assert.match(runtime, /WORKBENCH_CANDIDATE_SOURCE_BUILD/);
  assert.match(helpers, /TARGETED_DELTA_GENERATION_STALE/);
});

test('newly eligible candidates use authoritative direct scope discovery then first full baseline', () => {
  assert.match(helpers, /pay_preview_build_context/);
  assert.match(helpers, /ON CONFLICT \(session_id,candidate_id\)/);
  assert.match(runtime, /pay_workbench_session_candidate_scope_ensure_v1/);
  assert.match(runtime, /v_new_scope_baseline_required/);
  assert.match(runtime, /'new_scope_baseline_required',v_new_scope_baseline_required/);
});

test('bounded-source clone reuse requires one complete immutable publication proof', () => {
  assert.match(cloneBoundedCertification, /CREATE OR REPLACE FUNCTION private\.pay_workbench_session_clone_bounded_certification_v1/);
  assert.match(cloneBoundedCertification, /SECURITY DEFINER\s+SET search_path = ''/i);
  assert.match(cloneBoundedCertification, /status = 'COMPLETE'[\s\S]+private_stage = 'COMPLETE'/);
  assert.match(cloneBoundedCertification, /canonical_count = v_source_count[\s\S]+canonical_digest = v_source_digest/);
  assert.match(cloneBoundedCertification, /captured_input_fingerprint IS DISTINCT FROM timesheet_state\.current_input_fingerprint/);
  assert.match(cloneBoundedCertification, /SOURCE_CHANGE_SEQUENCE_STALE/);
  assert.match(cloneBoundedCertification, /CONTROLLED_CLONE_REBASE_NOT_AUTHORISED/);
  assert.match(cloneBoundedCertification, /v_option_source_session_id = p_source_session_id/);
  assert.match(cloneBoundedCertification, /v_option_target_session_id = p_target_session_id/);
  assert.match(cloneBoundedCertification, /CLONE_REBASING/);
  assert.match(cloneBoundedCertification, /TIME_OR_SESSION_SENSITIVE_AUTHORITY_REQUIRES_FULL_BUILD/);
  assert.match(cloneBoundedCertification, /PAYE_UMBRELLA_OR_BANK_ROUTING_CHANGED/);
  assert.match(cloneBoundedCertification, /PAYEE_READINESS_FINGERPRINT_CHANGED/);
  assert.match(cloneBoundedCertification, /REVOKE ALL ON FUNCTION[\s\S]+FROM PUBLIC, anon, authenticated, service_role/);
  assert.doesNotMatch(cloneBoundedCertification, /_pay_current_timesheet_entitlement_components\s*\(/);
  assert.doesNotMatch(cloneBoundedCertification, /_pay_outstanding_components\s*\(/);
});

test('clone execution preserves bounded canonical bytes and build identity while retaining full fallback', () => {
  assert.match(cloneEligibleRows, /pay_workbench_session_clone_eligibility_v1\([\s\S]+IS NOT TRUE[\s\S]+private\.pay_workbench_session_clone_bounded_certification_v1\(/);
  assert.match(cloneEligibleRows, /WHEN v_bounded_build_clone IS TRUE THEN source_line\.source_build_run_id/);
  assert.match(cloneEligibleRows, /WHEN v_bounded_build_clone IS TRUE THEN source_line\.source_row_json/);
  assert.match(cloneEligibleRows, /v_clone_projection_run_id[\s\S]+pay_workbench_delta_update_candidate_state_v1\(/);
  assert.match(cloneEligibleRows, /'bounded_build_certified', v_bounded_build_clone/);
  assert.match(cloneEligibleRows, /'session_id', p_target_session_id::text[\s\S]+'session_version', v_target_session\.version/);
  assert.match(cloneEligibleRows, /WORKBENCH_CANDIDATE_SOURCE_BUILD/);
  assert.match(cloneEligibleRows, /legacy_refresh_enqueued_count/);
});

test('sealed lifecycle output stages before one final proof and atomic promotion', () => {
  assert.match(runtime, /admission_seal_version = 1/);
  assert.match(runtime, /v_sealed_targeted_delta/);
  assert.match(runtime, /THEN 'DIRTY' ELSE 'CURRENT'/);
  assert.match(runtime, /'targeted_delta_stage'/);
  assert.match(runtime, /pay_workbench_targeted_delta_scope_finalize_v1/);
  assert.match(helpers, /SET status='SUPERSEDED'[\s\S]+SET status='CURRENT'/);
  assert.match(helpers, /PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_PROOF_STALE/);
});

test('public signatures and defaults remain unchanged', () => {
  assert.match(chunk, /pay_workbench_candidate_source_build_chunk\(\s*p_session_id uuid,\s*p_candidate_id uuid,\s*p_cursor_json jsonb DEFAULT NULL::jsonb,\s*p_payload_json jsonb DEFAULT '\{\}'::jsonb,\s*p_limit integer DEFAULT 100/s);
  assert.match(runtime, /pay_workbench_candidate_delta_refresh_chunk\(p_session_id uuid, p_candidate_id uuid, p_payload_json jsonb DEFAULT '\{\}'::jsonb, p_cursor_json jsonb DEFAULT '\{\}'::jsonb, p_limit integer DEFAULT 25\)/);
  assert.match(enqueue, /pay_workbench_enqueue_candidate_refresh\(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_payload_json jsonb DEFAULT '\{\}'::jsonb\)/);
});
