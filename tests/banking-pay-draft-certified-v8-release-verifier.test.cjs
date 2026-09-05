const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const verifierPath = path.join(root, 'supabase', 'verification', '03092026_1944_banking_pay_draft_certified_v8_verification.sql');
const releasePath = path.join(root, 'supabase', 'release', 'current-release.json');
const workerPath = path.join(root, 'broker', 'src', 'index.js');
const helperPath = path.join(root, 'broker', 'src', 'banking-pay-draft-certified-v8.js');

const verifier = fs.readFileSync(verifierPath, 'utf8').replace(/\r\n/g, '\n');
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));
const worker = fs.readFileSync(workerPath, 'utf8').replace(/\r\n/g, '\n');
const helper = fs.readFileSync(helperPath, 'utf8').replace(/\r\n/g, '\n');
const releaseEntry = 'supabase/verification/03092026_1944_banking_pay_draft_certified_v8_verification.sql';

test('certified Draft V8 release verifier covers all private relations and service entry points', () => {
  const relations = [
    'banking_pay_draft_frozen_certificate_scopes_v8',
    'banking_pay_draft_frozen_constituent_refs_v8',
    'banking_pay_draft_frozen_constituent_payloads_v8',
    'banking_pay_draft_frozen_partition_refs_v8',
    'banking_pay_draft_frozen_candidate_inputs_v8',
    'banking_pay_draft_frozen_candidate_scopes_v8',
    'banking_pay_draft_frozen_candidate_scope_members_v8',
    'banking_pay_draft_frozen_stage_receipts_v8',
    'banking_pay_draft_phase_units_v1',
    'banking_pay_draft_owner_receipts_v1',
    'banking_pay_draft_operation_created_batches_v8',
    'banking_pay_draft_operation_terminal_results_v8',
    'banking_pay_draft_operation_provenance_events_v8',
    'banking_pay_draft_finalizer_iterations_v8',
    'banking_pay_draft_constituent_parity_results_v8'
  ];
  for (const relation of relations) assert.match(verifier, new RegExp(`private\\.${relation}`));

  const functions = [
    'banking_pay_draft_certified_operation_start_v8',
    'pay_workbench_draft_certificate_constituent_ref_page_v8',
    'pay_workbench_draft_certificate_partition_ref_page_v8',
    'pay_workbench_draft_certificate_final_freeze_v8',
    'pay_workbench_settled_certificate_reference_validate_v8',
    'banking_pay_draft_phase_units_seed_v8',
    'pay_workbench_prepare_draft_scope_from_frozen_page_v8',
    'pay_workbench_draft_constituent_parity_page_v8',
    'banking_pay_draft_advance_bounded_v8',
    'banking_pay_draft_operation_finish_v8',
    'banking_pay_draft_certificate_stage_advance_v8',
    'banking_pay_draft_readiness_page_v8',
    'pay_workbench_prepare_draft_allocation_rows_seed',
    'pay_batch_stage_operation_candidate_chunk_context',
    'pay_batch_insert_items_from_preview',
    'pay_batch_apply_finance_adjustments',
    'pay_batch_assert_integrity'
  ];
  for (const fn of functions) assert.match(verifier, new RegExp(`public\\.${fn}\\(`));
  assert.match(verifier, /has_table_privilege\('service_role'/);
  assert.match(verifier, /has_function_privilege\('service_role'/);
});

test('release verifier freezes existing budgets and uses scalar-only ceiling proof', () => {
  assert.match(verifier, /statement_timeout=6000ms/);
  assert.match(verifier, /lock_timeout=1000ms/);
  assert.match(verifier, /statement_timeout'',\s*\\s\*''15000/);
  assert.match(verifier, /lock_timeout'',\s*\\s\*''1500/);
  assert.match(verifier, /constituent_count\\s\+NOT\\s\+BETWEEN/);
  assert.match(verifier, /50000/);
  assert.doesNotMatch(verifier, /generate_series\s*\([^)]*50000/i);
  assert.doesNotMatch(verifier, /INSERT\s+INTO/i);
  assert.doesNotMatch(verifier, /UPDATE\s+(?!bytes)/i);
  assert.doesNotMatch(verifier, /DELETE\s+FROM/i);
});

test('release manifest runs the Draft verifier for UPGRADE and NEW', () => {
  assert.equal(release.verificationFiles.filter((entry) => entry === releaseEntry).length, 1);
  assert.equal(release.newVerificationFiles.filter((entry) => entry === releaseEntry).length, 1);
});

test('certified route is selected before legacy expanded arrays and keeps a 50,000 scalar ceiling', () => {
  const certifiedIndex = worker.indexOf('if (body.certified_draft_v8 === true)');
  const legacyIndex = worker.indexOf('const rawSelectedPreviewRowIds =');
  assert.ok(certifiedIndex >= 0 && legacyIndex > certifiedIndex);
  assert.match(worker, /maximum_supported_constituents:\s*50000/);
  assert.match(helper, /integerInRange\(candidateCount,\s*1,\s*50000\)/);
  assert.doesNotMatch(helper, /50001\s*[,)]/);
});

test('verification SQL rejects illegal qualified PostgreSQL conditionals', () => {
  assert.match(verifier, /pg_catalog\\\.\(coalesce\|nullif\|least\|greatest\)/);
  assert.doesNotMatch(verifier, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
