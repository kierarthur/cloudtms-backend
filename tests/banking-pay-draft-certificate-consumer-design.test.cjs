const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const designPath = path.join(root, 'codex_outputs', 'h2-draft-parity', 'DRAFT_CERTIFICATE_CONSUMER_V1.design.json');
const design = JSON.parse(fs.readFileSync(designPath, 'utf8'));
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const scopeOwner = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql'), 'utf8');
const allocationOwner = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '08082026_0717_pay_workbench_prepare_draft_allocation_rows_seed_sort_order.sql'), 'utf8');
const financeOwner = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '21072026_1235_49_pay_batch_apply_finance_adjustments.sql'), 'utf8');
const finalizerOwner = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '01092026_1459_banking_pay_signed_recovery_draft_v1.sql'), 'utf8');
const monolith = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '26052026_2100HRS_NEW_FUNCTIONS.sql'), 'utf8');

test('design binds the accepted H1 V8 producer and zero-drift policy identities', () => {
  assert.equal(design.contract, 'DRAFT_CERTIFICATE_CONSUMER_V1');
  assert.equal(design.runtime_authority, 'MIGET_TEST');
  assert.equal(design.upstream_certificate.manifest_sha256, '7e2880ca4719b5e50e02c67cf359891006e6f7df3a7c49431e0ea3c5dac839e7');
  assert.equal(design.zero_drift_authority.sha256, '3952c019426334a6c04b568226d019fc915f635d2411b7a295229c188beef42c');
  assert.equal(design.limits.selected_constituent_max, 50000);
  assert.equal(design.limits.selected_constituent_reject, 50001);
  assert.equal(design.limits.certificate_read_page_max, 256);
  assert.equal(design.limits.candidate_scope_rpc_page_max, 256);
  assert.equal(design.limits.owner_iteration_row_max, 100);
  assert.deepEqual(design.limits.page_size_review.certificate_and_scope_staging_sizes_to_measure, [100, 128, 256]);
  assert.equal(design.limits.statement_timeout_change_permitted, false);
  assert.equal(design.limits.lock_timeout_change_permitted, false);
});

test('design keeps policy owners and orchestration corrections separate', () => {
  assert.match(design.policy_separation.f013b, /CLOSED_NOT_DEFECT/);
  assert.match(design.policy_separation.f013b, /PAYMENT_ADVANCE_REPAYMENT/);
  assert.match(design.policy_separation.f013b, /LOAN_REPAYMENT/);
  assert.ok(design.policy_separation.forbidden.includes('new amount equation'));
  assert.ok(design.policy_separation.forbidden.includes('raising a timeout'));
  assert.ok(design.policy_separation.forbidden.includes('hiding or capping selected rows'));
});

test('current source proves the pre-chunk global array handoff and phase-unit collision boundary', () => {
  assert.match(worker, /fetchCurrentSessionSelectionForCreateDraft/);
  assert.match(worker, /p_units_json:\s*allScopeIds/);
  assert.match(worker, /p_units_json:\s*scopeIds/);
  assert.match(monolith, /CREATE OR REPLACE FUNCTION public\.banking_pay_operation_seed_chunks/);
  assert.match(monolith, /DRAFT_CREATE_CHUNK_UNITS_TOO_LARGE/);
  assert.match(monolith, /\(\(\(seed_unit\.unit_ordinal - 1\) \/ v_chunk_size\) \+ 1\)::integer AS sequence_no/);
  assert.match(monolith, /CHUNK_SCOPE_MISMATCH/);
});

test('current source proves the scope and allocation owners are capped or truncated at 100', () => {
  assert.match(scopeOwner, /selected preview row input exceeds the 100 row cap/i);
  assert.ok((scopeOwner.match(/LIMIT 100/gi) || []).length >= 2);
  assert.match(allocationOwner, /candidate scope id array exceeds the 100 row cap/i);
  assert.match(allocationOwner, /LIMIT 100/i);
  assert.match(allocationOwner, /selected_canonical_preview_lines_json/);
});

test('current source proves owner continuation exists but the Worker completes after one call', () => {
  assert.match(finalizerOwner, /'has_more'/);
  assert.match(monolith, /CREATE OR REPLACE FUNCTION public\.pay_batch_create_timesheet_snapshots/);
  assert.match(monolith, /CREATE OR REPLACE FUNCTION public\.pay_batch_build_item_breakdowns/);
  assert.ok((monolith.match(/'has_more'/g) || []).length > 2);
  assert.match(worker, /await finishChunk\(chunk\.chunk_id, 'COMPLETE'/);
  assert.doesNotMatch(worker, /while\s*\([^)]*has_more/);
});

test('finance owner has no continuation output and must not be silently treated as paged', () => {
  assert.match(financeOwner, /CREATE OR REPLACE FUNCTION public\.pay_batch_apply_finance_adjustments/);
  assert.match(financeOwner, /private\.pay_workbench_draft_finance_item_plan_v1/);
  assert.doesNotMatch(financeOwner, /'has_more'/);
  assert.match(financeOwner, /PAYMENT_ADVANCE_REPAYMENT/);
  assert.match(financeOwner, /LOAN_REPAYMENT/);
});

test('row-backed design has exact source rows, appendable units, receipts and parity links', () => {
  const names = new Set(design.relations.map((relation) => relation.name));
  assert.ok(names.has('private.banking_pay_draft_frozen_certificate_scopes_v8'));
  assert.ok(names.has('private.banking_pay_draft_frozen_constituent_refs_v8'));
  assert.ok(names.has('private.banking_pay_draft_frozen_partition_refs_v8'));
  assert.ok(names.has('private.banking_pay_draft_frozen_candidate_scopes_v8'));
  assert.ok(names.has('private.banking_pay_draft_frozen_candidate_scope_members_v8'));
  assert.ok(names.has('private.banking_pay_draft_phase_units_v1'));
  assert.ok(names.has('private.banking_pay_draft_owner_receipts_v1'));
  assert.ok(names.has('private.banking_pay_draft_constituent_parity_results_v8'));
  assert.ok(names.has('private.banking_pay_draft_operation_terminal_results_v8'));
  assert.equal(design.candidate_scope_contract.one_shell_per_candidate_channel, true);
  assert.equal(design.candidate_scope_contract.normalized_membership, true);
  assert.ok(design.candidate_scope_contract.preserved_small_owner_inputs.includes('effective_payees_json'));
  assert.ok(design.candidate_scope_contract.prohibited_giant_inputs.includes('selected_canonical_preview_lines_json'));
});

test('Worker completion contract forbids unbounded in-request loops and premature completion', () => {
  assert.equal(design.worker_contract.no_in_request_loop, true);
  assert.equal(design.worker_contract.no_unbounded_response, true);
  assert.equal(design.worker_contract.no_timeout_change, true);
  assert.match(design.worker_contract.completion_rule, /Never finish a chunk, phase or operation/);
  assert.match(design.worker_contract.old_operation_rule, /no implicit fallback/i);
});

test('source-proved defects stay distinct from latent risks and bounded-design constraints', () => {
  const findings = new Map(design.proved_current_failures.map((finding) => [finding.code, finding]));
  assert.equal(findings.get('F010C_PRE_CHUNK_ARRAY_DUPLICATION').classification, 'PROVED_CREATE_DRAFT_TRANSPORT_DEFECT');
  assert.equal(findings.get('F010A_SCOPE_SEED_100_CAP_AND_TRUNCATION').classification, 'PROVED_CREATE_DRAFT_SCOPE_DEFECT');
  assert.equal(findings.get('F010_OWNER_CONTINUATION_IGNORED').classification, 'SOURCE_PROVED_LATENT_HANDSHAKE_RISK_NOT_OBSERVED_ROOT');
  assert.equal(findings.get('F010_FINANCE_OWNER_NOT_PAGED').classification, 'SOURCE_PROVED_BOUNDED_ORCHESTRATION_DESIGN_CONSTRAINT_NOT_POLICY_DEFECT');
});

test('frontend transport removes repeated constituent arrays without changing terminal business output', () => {
  const transport = design.frontend_transport_contract;
  assert.match(transport.current_source_evidence, /repeats them across request fields/i);
  assert.match(transport.new_submission_rule, /never carry selected constituent arrays or constituent contracts/i);
  assert.match(transport.selection_authority_rule, /server-certified selection/i);
  assert.match(transport.result_rule, /terminal operation result/i);
  assert.match(transport.result_rule, /retain their current meaning and type/i);
  assert.match(transport.legacy_rule, /no.*fall back/i);
  assert.equal(transport.implementation_status, 'SOURCE_MAPPED_NOT_YET_CONNECTED_TO_STABLE_H1_ISSUER');
});
