import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

const schema = read('supabase/migrations/08082026_0010_banking_pay_certified_reuse_v2.sql');
const cursorSchema = read('supabase/migrations/07082026_2354_banking_pay_dirty_session_cursor_v1.sql');
const publisher = read('supabase/repeatable/07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql');
const progress = read('supabase/repeatable/07082026_2155_pay_workbench_session_recompute_progress_counters.sql');
const runtime = read('supabase/repeatable/07082026_1016_banking_pay_targeted_delta_runtime.sql');
const helpers = read('supabase/repeatable/07082026_1011_banking_pay_targeted_delta_helpers.sql');
const cloneOwner = read('supabase/repeatable/04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql');
const cloneCert = read('supabase/repeatable/07082026_1430_pay_workbench_session_clone_bounded_certification_v1.sql');
const reuseHelpers = read('supabase/repeatable/08082026_0020_banking_pay_certified_reuse_helpers.sql');
const dirtyLane = read('supabase/repeatable/08082026_0054_banking_pay_dirty_lane_clone_eligibility.sql');
const exactImport = read('supabase/repeatable/08082026_0021_banking_pay_exact_import_family_admission_v1.sql');
const reconcile = read('supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql');
const claim = read('supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql');
const acl = read('supabase/repeatable/07082026_2358_banking_pay_targeted_fast_route_acl.sql');
const catalogue = JSON.parse(read('supabase/verification/banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json'));
const verifier = read('supabase/verification/verify_banking_pay_targeted_fast_route_certified_reuse_catalog.mjs');
const workflow = read('.github/workflows/supabase-migrate.yml');

test('V2 schema accepts only certified clone or targeted delta terminal parity and remains disabled', () => {
  assert.match(schema, /DROP CONSTRAINT bpay_wb_scope_certified_preview_attestation_ck/i);
  assert.match(schema, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1[\s\S]+BOUNDED_FULL_SOURCE_BUILD/);
  assert.match(schema, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2[\s\S]+CERTIFIED_CLONE[\s\S]+READY','SOURCE_EMPTY/);
  assert.match(schema, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2[\s\S]+TARGETED_DELTA[\s\S]+READY','SOURCE_EMPTY/);
  for (const name of [
    'banking_pay_workbench_clone_bounded_reuse_v2_enabled',
    'banking_pay_workbench_clone_source_empty_reuse_enabled'
  ]) assert.match(schema, new RegExp(name + '\\s+boolean\\s+NOT NULL\\s+DEFAULT false', 'i'));
  assert.match(schema, /banking_pay_workbench_reconciliation_optimization_version integer NOT NULL DEFAULT 0/i);
  assert.match(schema, /banking_pay_workbench_delta_enable_simple_authorise=false/);
  assert.match(schema, /banking_pay_workbench_delta_enable_simple_unauthorise=false/);
  assert.match(schema, /banking_pay_workbench_delta_enable_exact_import_family=false/);
});

test('publisher keeps V1 strict and exposes only certified V2 clone or delta branches', () => {
  assert.match(publisher, /v_contract_version integer := CASE/);
  assert.match(publisher, /v_contract_version=1[\s\S]+v_authority_kind\s*<>\s*'BOUNDED_FULL_SOURCE_BUILD'/);
  assert.match(publisher, /v_authority_kind NOT IN \('CERTIFIED_CLONE','TARGETED_DELTA'\)/);
  assert.match(publisher, /v_final_state NOT IN \('READY','SOURCE_EMPTY','PENDING_DELTA'\)/);
  assert.doesNotMatch(publisher, /READINESS_PATCH/);
  assert.match(publisher, /\(v_scope_ordinal \* 1000000\) \+ ready_row\.source_ordinal/);
  assert.match(publisher, /source_minus_preview/);
  assert.match(publisher, /preview_minus_source/);
  assert.match(publisher, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2/);
});

test('progress accepts exact V1 or terminal V2 authority and blocks pending publication', () => {
  assert.match(progress, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1[\s\S]+BOUNDED_FULL_SOURCE_BUILD/);
  assert.match(progress, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2[\s\S]+authority_kind' IN \('CERTIFIED_CLONE','TARGETED_DELTA'\)/);
  assert.match(progress, /final_state' IN \('READY','SOURCE_EMPTY'\)/);
  assert.match(progress, /CURRENT_SOURCE_PREVIEW_PUBLICATION_INCOMPLETE/);
  assert.doesNotMatch(progress, /PENDING_DELTA'\)/);
});

test('dirty lane leaves candidate lock ownership to processor and processor owns composite cursor', () => {
  assert.match(dirtyLane, /claim_job\.job_type <> 'WORKBENCH_CANDIDATE_DIRTY_APPLY'[\s\S]+candidate_serial_required/);
  const lockAt = runtime.indexOf('v_candidate_lock_acquired := pg_catalog.pg_try_advisory_xact_lock');
  const cursorAt = runtime.indexOf("private_cursor_kind <> 'DIRTY_SESSION_SCAN_V1'");
  assert.ok(lockAt >= 0 && cursorAt > lockAt, 'candidate lock must precede private cursor read');
  assert.match(runtime, /candidate_serial_delayed',\s*true/);
  assert.match(runtime, /next_cursor_json', COALESCE\(v_job\.private_cursor_json/);
  assert.match(runtime, /upper_created_at_utc/);
  assert.match(runtime, /upper_session_id/);
  assert.match(runtime, /last_created_at_utc/);
  assert.match(runtime, /last_session_id/);
  assert.match(cursorSchema, /DIRTY_SESSION_SCAN_V1/);
});

test('historical reuse is bounded and target-date eligibility remains authoritative', () => {
  assert.match(reuseHelpers, /LIMIT 16/);
  assert.match(reuseHelpers, /status='COMPLETE'/);
  assert.match(reuseHelpers, /private_stage='COMPLETE'/);
  assert.match(reuseHelpers, /banking_pay_workbench_clone_bounded_reuse_v2_enabled/);
  assert.match(reuseHelpers, /banking_pay_workbench_clone_source_empty_reuse_enabled/);
  assert.match(cloneCert, /pay_workbench_session_clone_eligibility_v1\(/);
  assert.match(cloneCert, /target_date_eligible/);
  assert.match(cloneCert, /CERTIFIED_PUBLICATION_ATTESTATION_DRIFT/);
  assert.match(cloneCert, /source_identity_digest/);
  assert.match(cloneCert, /preview_identity_digest/);
  assert.match(cloneCert, /'source_selection_authorised',true/);
  assert.match(cloneCert, /target_authority_scope_digest/);
  assert.match(cloneCert, /PAYEE_READINESS_FINGERPRINT_CHANGED/);
  assert.match(dirtyLane, /source_selection_authorised/);
  assert.match(cloneOwner, /source_session\.status,''\)\)\) IN \('DISCARDED','REPLACED'\)/);
});

test('retired preview history is excluded only from the V2 target-date probe', () => {
  assert.match(
    dirtyLane,
    /v_certified_reuse_v2_probe IS NOT TRUE[\s\S]+preview_row\.status, ''\)\)\) <> 'SUPERSEDED'/
  );
  assert.match(
    dirtyLane,
    /COUNT\(\*\) FILTER \(WHERE UPPER\(BTRIM\(COALESCE\(all_preview\.status, ''\)\)\) <> 'READY'\)/
  );
  assert.match(dirtyLane, /'clone_eligible', CASE WHEN v_certified_reuse_v2_probe IS TRUE THEN false ELSE true END/);
  assert.match(dirtyLane, /'target_date_eligible', CASE WHEN v_certified_reuse_v2_probe IS TRUE THEN true ELSE NULL::boolean END/);
});

test('clone owner locks deterministic session union, fences, then uses V2 publisher', () => {
  assert.match(cloneOwner, /ORDER BY candidate_lock\.id/);
  assert.match(cloneOwner, /ORDER BY session_lock\.id/);
  assert.match(cloneOwner, /ORDER BY scope_lock\.session_id,scope_lock\.candidate_id/);
  assert.match(cloneOwner, /pay_workbench_session_clone_publication_fence_v1/);
  assert.match(cloneOwner, /'contract_version',2/);
  assert.match(cloneOwner, /'authority_kind','CERTIFIED_CLONE'/);
  assert.match(reuseHelpers, /certification_digest/);
});

test('sealed delta remains non-ready until one atomic V2 finaliser proves parity', () => {
  assert.match(runtime, /THEN 'DIRTY' ELSE 'CURRENT'/);
  assert.match(runtime, /TARGETED_DELTA_OUTPUT_NOT_READY/);
  assert.match(runtime, /pay_workbench_targeted_delta_scope_finalize_v1/);
  assert.match(helpers, /PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_PROOF_STALE/);
  assert.match(helpers, /'contract_version',2/);
  assert.match(helpers, /'authority_kind','TARGETED_DELTA'/);
  assert.match(helpers, /certified_preview_publication_parity_ok/);
  assert.match(helpers, /SET status='SUPERSEDED'[\s\S]+SET status='CURRENT'/);
});

test('reconciliation version is frozen per build and optimisation remains pass-local', () => {
  assert.match(claim, /'reconciliation_optimization_version',v_reconciliation_optimization_version/);
  assert.match(claim, /v_reconciliation_optimization_version integer:=0/);
  assert.match(claim, /IF v_execution_profile_version=2 THEN/);
  assert.match(claim, /v_build\.attestation_json->>'reconciliation_optimization_version'/);
  assert.match(reconcile, /cloudtms\.pay_workbench_reconciliation_optimization_version/);
  assert.match(reconcile, /CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp\./);
  assert.match(reconcile, /ON COMMIT DROP/);
  assert.match(reconcile, /v_reconciliation_optimization_version=1/);
  assert.doesNotMatch(reconcile, /capture[_ ]derived[_ ]residual/i);
});

test('exact import-family admission is weekly, complete, occurrence-aware and postgres-only', () => {
  assert.match(exactImport, /IN \('NHSP','HEALTHROSTER'\)/);
  assert.match(exactImport, /NHSP_WEEKLY/);
  assert.match(exactImport, /HEALTHROSTER_WEEKLY/);
  assert.doesNotMatch(exactImport, /HEALTHROSTER_DAILY/);
  assert.match(exactImport, /count\(DISTINCT family_member\.timesheet_id\)/);
  assert.match(exactImport, /EXACT_IMPORT_OCCURRENCE_EVIDENCE_INCOMPLETE/);
  assert.match(exactImport, /v_occurrence_count<=0/);
  assert.match(exactImport, /FROM PUBLIC,anon,authenticated,service_role/);
  assert.match(exactImport, /TO postgres/);
});

test('ACL and catalogue verifier preserve least privilege and one manifest owner', () => {
  assert.match(acl, /pay_workbench_session_clone_eligible_rows_v1[\s\S]+FROM PUBLIC,anon,authenticated,service_role[\s\S]+TO service_role/);
  assert.match(acl, /pay_workbench_candidate_delta_refresh_chunk[\s\S]+FROM PUBLIC,anon,authenticated,service_role[\s\S]+TO service_role/);
  assert.match(acl, /pay_workbench_delta_write_compatible_rows_v1[\s\S]+FROM PUBLIC,anon,authenticated,service_role/);
  assert.doesNotMatch(acl.match(/pay_workbench_delta_write_compatible_rows_v1[\s\S]*$/)?.[0] || '', /TO service_role/);
  assert.equal(catalogue.function_count, 17);
  assert.equal(catalogue.functions.length, 17);
  assert.equal(new Set(catalogue.functions.map((entry) => entry.schema + '.' + entry.name + '(' + entry.identity_arguments + ')')).size, 17);
  assert.ok(catalogue.functions.some((entry) => entry.name === 'pay_workbench_enqueue_stage_continuation'));
  assert.ok(catalogue.functions.some((entry) => entry.name === 'pay_workbench_fail_job'));
  assert.match(verifier, /definition_sha256/);
  assert.match(verifier, /unexpected overload/);
  assert.match(workflow, /verify_banking_pay_targeted_fast_route_certified_reuse_catalog\.mjs/);
});
