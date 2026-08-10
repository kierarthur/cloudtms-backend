import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');

const migration = read('supabase/migrations/10082026_1015_banking_pay_source_publication_and_draft_fast_path.sql');
const helpers = read('supabase/repeatable/10082026_1025_banking_pay_source_publication_identity_and_draft_step.sql');
const publisher = read('supabase/repeatable/07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql');
const fullSource = read('supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql');
const clone = read('supabase/repeatable/04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql');
const delta = read('supabase/repeatable/07082026_1011_banking_pay_targeted_delta_helpers.sql');
const admission = read('supabase/repeatable/09082026_0712_banking_pay_semantic_ready_helpers.sql');
const draftScope = read('supabase/repeatable/21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql');
const draftAllocation = read('supabase/repeatable/08082026_0717_pay_workbench_prepare_draft_allocation_rows_seed_sort_order.sql');
const requestStart = read('supabase/repeatable/04082026_1207_pay_payment_correction_request_start.sql');
const cancelSafe = read('supabase/repeatable/19072026_1816_cancel_refresh_supersede_finance_dirty.sql');
const claimStart = read('supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql');
const worker = read('broker/src/index.js');
const semanticManifest = JSON.parse(read('supabase/verification/banking_pay_semantic_ready_cancellation_reversion_catalog_manifest.json'));

test('physical source publication identity is schema-backed and independently feature controlled', () => {
  assert.match(migration, /ADD COLUMN IF NOT EXISTS source_publication_id uuid/);
  assert.match(migration, /certified_preview_publication_source_publication_id uuid/);
  assert.match(migration, /authority_fingerprint_version smallint/);
  assert.match(migration, /authority_fingerprint text/);
  for (const setting of [
    'banking_pay_source_publication_identity_write_v1_enabled',
    'banking_pay_source_publication_identity_enforce_v1_enabled',
    'banking_pay_same_authority_build_election_v1_enabled',
    'banking_pay_draft_step_rpc_v1_enabled',
    'banking_pay_selection_intent_identity_v1_enabled',
    'banking_pay_pre_bank_cancel_set_page_v1_enabled'
  ]) assert.match(migration, new RegExp(setting));
  assert.match(migration, /UNIQUE[\s\S]*source_publication_id[\s\S]*source_ordinal/i);
});

test('one deterministic publication identity follows full, clone and targeted-delta physical sets', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_source_publication_identity_v1/);
  assert.match(helpers, /SOURCE_PUBLICATION_V1/);
  assert.match(fullSource, /source_publication_id[\s\S]*pay_workbench_source_publication_identity_v1/);
  assert.match(clone, /source_publication_id[\s\S]*pay_workbench_source_publication_identity_v1/);
  assert.match(delta, /publication[\s\S]*identity describes the complete physical CURRENT set/);
  assert.match(delta, /SET source_change_seq=v_run\.source_change_seq,[\s\S]*source_publication_id=v_source_publication_id/);
  assert.match(publisher, /certified_preview_publication_source_publication_id=v_source_publication_id/);
  assert.match(publisher, /SOURCE_SET_AMBIGUOUS/);
});

test('Draft freeze and cancellation admission bind one exact physical publication', () => {
  assert.match(draftScope, /'source_publication_id'[\s\S]*certified_preview_publication_source_publication_id/);
  assert.match(draftAllocation, /PAY_WORKBENCH_DRAFT_SOURCE_SEMANTIC_CERTIFICATION_REQUIRED/);
  assert.match(requestStart, /original_source_publication_id/);
  assert.match(requestStart, /banking_pay_source_publication_identity_enforce_v1_enabled/);
  assert.match(admission, /original_source_publication_id/);
  assert.match(admission, /source_publication_id[\s\S]*v_original_source_publication_id/);
  assert.match(admission, /ORIGINAL_SOURCE_PUBLICATION_ID_MISSING/);
});

test('same-authority build uniqueness is frozen when the economic build is inserted', () => {
  assert.match(claimStart, /banking_pay_same_authority_build_election_v1_enabled/);
  assert.match(claimStart, /authority_fingerprint_version/);
  assert.match(claimStart, /authority_fingerprint/);
  assert.match(claimStart, /PAY_WORKBENCH_SOURCE_BUILD_AUTHORITY_FINGERPRINT_REQUIRED/);
});

test('cancelled rows remain eligible but explicitly unticked across publication', () => {
  assert.match(cancelSafe, /selection_user_override'[\s\S]*'UNSELECTED'/);
  assert.match(cancelSafe, /POST_CANCEL_RETURN_UNSELECTED/);
  assert.match(publisher, /selection_user_override/);
  assert.match(publisher, /v_selection_intent_identity_enabled/);
  assert.match(publisher, /existing_selection_user_override/);
});

test('Draft one-step RPC preserves existing business owners while removing HTTP fanout', () => {
  assert.match(helpers, /CREATE OR REPLACE FUNCTION public\.banking_pay_draft_create_step_v1/);
  assert.match(helpers, /pay_workbench_prepare_draft_allocation_rows_seed/);
  assert.match(helpers, /pay_batch_insert_items_from_preview/);
  assert.match(helpers, /pay_batch_apply_finance_adjustments/);
  assert.match(helpers, /pay_batch_finalize_reservations_and_markers/);
  assert.match(helpers, /banking_pay_operation_finish_chunk/);
  assert.match(helpers, /banking_pay_operation_save_progress/);
  assert.match(worker, /tryDraftCreateStepRpc/);
  assert.match(worker, /banking_pay_draft_create_step_v1/);
  assert.match(worker, /if \(stepResult\) return stepResult/);

  const draftFunctionStart = worker.indexOf('async function advanceBankingPayDraftCreateOperation');
  const nextFunctionStart = worker.indexOf('\nasync function ', draftFunctionStart + 1);
  const helperStart = worker.indexOf('const tryDraftCreateStepRpc');
  assert.ok(draftFunctionStart >= 0 && helperStart > draftFunctionStart);
  assert.ok(nextFunctionStart < 0 || helperStart < nextFunctionStart);
  assert.equal(worker.indexOf('const tryDraftCreateStepRpc', helperStart + 1), -1);
});

test('new helpers are catalogue-owned once and remain service/private only', () => {
  assert.equal(semanticManifest.function_count, semanticManifest.functions.length);
  const sourceIdentity = semanticManifest.functions.filter((entry) => entry.name === 'pay_workbench_source_publication_identity_v1');
  const draftStep = semanticManifest.functions.filter((entry) => entry.name === 'banking_pay_draft_create_step_v1');
  assert.equal(sourceIdentity.length, 1);
  assert.equal(draftStep.length, 1);
  assert.deepEqual(sourceIdentity[0].expanded_acl.map((acl) => acl.grantee), ['postgres']);
  assert.deepEqual(draftStep[0].expanded_acl.map((acl) => acl.grantee), ['postgres', 'service_role']);
  assert.equal(draftStep[0].security_definer, true);
});

test('the orchestration patch does not change Policy X economics or provider authority', () => {
  for (const sql of [helpers, publisher, admission, requestStart, cancelSafe]) {
    assert.doesNotMatch(sql, /CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER/i);
  }
  assert.doesNotMatch(helpers, /provider[_ ]submission|bank[_ ]transfer[_ ]execute|settlement[_ ]execute/i);
});
