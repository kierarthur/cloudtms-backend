const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

const worker = read('broker/src/index.js');
const transport = read('broker/src/banking-pay-draft-certified-v8.js');
const wrangler = read('wrangler.toml');
const frozenSchema = read(
  'supabase/migrations/02092026_2310_banking_pay_draft_frozen_certificate_v8.sql'
);
const rowBackedScope = read(
  'supabase/repeatable/02092026_2313_banking_pay_draft_candidate_scope_row_backed_v8.sql'
);
const certificateAdvance = read(
  'supabase/repeatable/03092026_1200_banking_pay_draft_certificate_stage_advance_v8.sql'
);
const readinessPage = read(
  'supabase/repeatable/03092026_1210_banking_pay_draft_readiness_page_v8.sql'
);
const boundedAdvance = read(
  'supabase/repeatable/02092026_2330_banking_pay_draft_bounded_advance_v8.sql'
);
const terminalFinish = read(
  'supabase/repeatable/02092026_2331_banking_pay_draft_terminal_finish_v8.sql'
);
const parity = read(
  'supabase/repeatable/02092026_2315_banking_pay_draft_constituent_parity_v8.sql'
);
const financePolicyMatrix = read(
  'scripts/verify-banking-pay-draft-v8-finance-policy-matrix.mjs'
);
const audit = JSON.parse(read(
  'codex_outputs/h2-draft-parity/H2_CREATE_DRAFT_LIFECYCLE_CROSS_AUDIT_V1.json'
));

const gate = (id) => audit.gates.find((row) => row.id === id);

test('the lifecycle audit is finite, policy-neutral and blocks every incomplete gate', () => {
  assert.equal(audit.contract, 'H2_CREATE_DRAFT_LIFECYCLE_CROSS_AUDIT_V1');
  assert.equal(audit.runtime_authority, 'MIGET_TEST');
  assert.equal(audit.gates.length, 16);
  assert.deepEqual(audit.blocking_gate_ids, audit.gates.map((row) => row.id));
  assert.equal(audit.publication_authorised, false);
  assert.equal(audit.real_draft_or_payment_action_authorised, false);
  assert.match(audit.policy_rule, /Only transport and orchestration may change/);
});

test('current local source activates only the exact compact certified V8 Worker route', () => {
  assert.equal(gate('H2-L02').status, 'LOCAL_SOURCE_ROUTE_PRESENT_FULL_RUNTIME_OPEN');
  assert.match(worker, /from\s+['"]\.\/banking-pay-draft-certified-v8\.js['"]/);
  assert.match(worker, /validateCurrentCertificateIssuerEnvelopeV8/);
  assert.match(transport, /export function validateCurrentCertificateIssuerEnvelopeV8/);
  assert.match(transport, /export function buildCertifiedDraftOperationInputV8/);
  assert.match(worker, /validateCertifiedDraftOperationProjectionV8/);
  assert.match(worker, /banking_pay_draft_certificate_stage_advance_v8/);
});

test('current local source restores both readiness phases through bounded frozen-row pages', () => {
  assert.equal(gate('H2-L05').status, 'LOCAL_SOURCE_AND_BOUNDED_PAGE_PROOF_PRESENT_FULL_RUNTIME_OPEN');
  assert.equal(gate('H2-L06').status, 'LOCAL_SOURCE_AND_BOUNDED_PAGE_PROOF_PRESENT_FULL_RUNTIME_OPEN');
  assert.match(certificateAdvance, /SET phase = 'DRAIN_TSFIN'/);
  assert.match(readinessPage, /banking_pay_draft_readiness_page_v8/);
  assert.match(readinessPage, /private\.banking_pay_draft_frozen_constituent_payloads_v8/);
  assert.match(worker, /if \(phase === 'DRAIN_TSFIN'\)/);
  assert.match(worker, /if \(phase === 'ENSURE_PAYEE_READINESS'\)/);
  assert.match(worker, /tsfinBestEffortMakeReadyForDraft/);
  assert.match(worker, /revolutEnsurePayeesReadyFromPreview/);
});

test('the V8 readiness route ignores the deliberately empty legacy Timesheet array and uses the bounded pager', () => {
  assert.equal(gate('H2-L05').status, 'LOCAL_SOURCE_AND_BOUNDED_PAGE_PROOF_PRESENT_FULL_RUNTIME_OPEN');
  assert.match(rowBackedScope, /'\[\]'::jsonb, '\[\]'::jsonb, '\[\]'::jsonb/);
  assert.match(rowBackedScope, /selected_timesheet_ids_json <> '\[\]'::jsonb/);
  assert.match(frozenSchema, /timesheet_id uuid NULL/);
  assert.match(worker, /tsfinBestEffortMakeReadyForDraft\(env, inputJson\.selected_timesheet_ids \|\| inputJson\.selectedTimesheetIds \|\| \[\]/);
  assert.match(worker, /banking_pay_draft_readiness_page_v8/);
  assert.match(worker, /timesheet_ids/);
});

test('current compact operation input preserves the exact accepted same-week PAYE proof', () => {
  assert.equal(gate('H2-L03').status, 'LOCAL_SOURCE_AND_FOCUSED_RUNTIME_PASS_FINAL_INTEGRATION_OPEN');
  assert.match(transport, /const SAME_WEEK_OVERRIDE_KEYS/);
  const projectionStart = transport.indexOf('export function validateCertifiedDraftOperationProjectionV8');
  const projectionEnd = transport.indexOf('export function isCertifiedDraftOperationProjectionV8');
  const projection = transport.slice(projectionStart, projectionEnd);
  assert.match(projection, /same_week_paye_override/);
  assert.match(projection, /SAME_WEEK_OVERRIDE_KEYS/);
  assert.match(worker, /inputJson\.same_week_paye_override/);
  assert.match(worker, /pay_channel_scope/);
});

test('current local source consumes cleanup and terminal signals through existing owners', () => {
  assert.equal(gate('H2-L11').status, 'LOCAL_SOURCE_AND_DUAL_ENGINE_SEQUENCE_PASS_FULL_RUNTIME_OPEN');
  assert.equal(gate('H2-L12').status, 'LOCAL_SOURCE_AND_DUAL_ENGINE_SEQUENCE_PASS_FULL_PARITY_OPEN');
  for (const signal of [
    'EMPTY_RESERVED_BATCH_CLEANUP_REQUIRED',
    'TERMINAL_FAILURE_REQUIRED',
    'READY_FOR_TERMINAL_FINISH'
  ]) assert.match(boundedAdvance, new RegExp(signal));
  assert.match(worker, /EMPTY_RESERVED_BATCH_CLEANUP_REQUIRED|TERMINAL_FAILURE_REQUIRED|READY_FOR_TERMINAL_FINISH/);
  assert.match(worker, /pay_batch_abort_failed_draft_create_partial/);
  assert.match(worker, /banking_pay_batch_signal_touch/);
  assert.match(worker, /finishFailedWithCleanup/);
  assert.match(worker, /cancelSkippedEmptyReservedDraftBatches/);
});

test('the finance policy matrix is rollback-only, dual-engine and covers every supported variant', () => {
  assert.equal(gate('H2-L08').status,
    'LOCAL_DUAL_ENGINE_20_VARIANT_POLICY_MATRIX_PASS_COMPLETE_RELEASE_PARITY_OPEN');
  assert.match(financePolicyMatrix, /for \(let ordinal = 1; ordinal <= 20; ordinal \+= 1\)/);
  assert.match(financePolicyMatrix, /PostgreSQL \(17\\\.11\|18\\\.6\)/);
  assert.match(financePolicyMatrix, /POST_CREATE_REFRESH/);
  assert.match(financePolicyMatrix, /pay_batch_item_id IS NULL/);
  assert.match(financePolicyMatrix, /external_payment_actions: 0/);
  assert.match(financePolicyMatrix, /transaction_outcome: 'ROLLBACK'/);
  assert.doesNotMatch(financePolicyMatrix, /generate_series\([^\n]*(?:49999|50000)/i);
});

test('the terminal owner and V8 Worker preserve the full accepted result contract', () => {
  assert.equal(gate('H2-L12').status, 'LOCAL_SOURCE_AND_DUAL_ENGINE_SEQUENCE_PASS_FULL_PARITY_OPEN');
  for (const field of [
    'pay_batch_ids',
    'created_pay_batch_ids',
    'primary_pay_batch_id',
    'pay_batch_id',
    'created_batches',
    'paye_pay_batch_id',
    'umbrella_pay_batch_id',
    'created_batch_count',
    'candidate_count'
  ]) assert.match(terminalFinish, new RegExp(field));
  for (const acceptedResultField of [
    'reservation_availability',
    'post_create_refresh',
    'source_session_discarded',
    'replacement_session_id'
  ]) assert.match(worker, new RegExp(acceptedResultField));
  assert.match(worker, /banking_pay_draft_operation_finish_v8/);
  assert.match(worker, /buildCertifiedDraftTerminalResultV8/);
});

test('the existing TEST durable continuation queue carries the V8 route without timeout changes', () => {
  assert.equal(gate('H2-L13').status,
    'LOCAL_DUAL_ENGINE_5000_ROW_BOUNDED_TRANSPORT_PASS_QUEUE_AND_INSTALLED_RUNTIME_OPEN');
  assert.match(wrangler, /BANKING_PAY_CONTINUATION_ENABLED = "true"/);
  assert.match(wrangler, /binding = "BANKING_PAY_CONTINUATION_QUEUE"/);
  assert.match(wrangler, /queue = "test-cloudtms-banking-pay-continuation"/);
  assert.match(wrangler, /dead_letter_queue = "test-cloudtms-banking-pay-continuation-dlq"/);
  assert.match(worker, /async function processBankingPayContinuationMessage/);
  assert.match(worker, /draftCreateMaxPhaseUnits: 20/);
  assert.match(worker, /draftCreateMaxChunksPerCall: 10/);
  assert.match(worker, /validateCertifiedDraftOperationProjectionV8/);
  assert.match(worker, /banking_pay_draft_advance_bounded_v8/);
});

test('the parity routine enforces certified source facts without becoming a payment-policy owner', () => {
  assert.equal(gate('H2-L10').status, 'LOCAL_SOURCE_AND_DUAL_ENGINE_EXPECTED_FACT_PASS_FULL_V1_V8_PARITY_OPEN');

  for (const field of [
    'expected_allocation_basis_kind',
    'expected_allocation_result',
    'expected_allocation_source_digest_sha256',
    'expected_item_semantic_kind',
    'expected_item_source_identity_digest_sha256',
    'expected_item_source_digest_sha256',
    'expected_reservation_applicability',
    'expected_reservation_source_digest_sha256'
  ]) assert.match(parity, new RegExp(field));

  const mismatchStart = parity.indexOf('IF v_entry.expected_item_amount_ex_vat::numeric');
  const mismatchEnd = parity.indexOf('IF v_mismatch IS NOT NULL', mismatchStart);
  const mismatchChain = parity.slice(mismatchStart, mismatchEnd);
  assert.ok(mismatchStart >= 0 && mismatchEnd > mismatchStart);
  assert.match(mismatchChain, /DRAFT_PARITY_CERTIFICATE_BINDING_MISMATCH/);
  assert.match(mismatchChain, /DRAFT_PARITY_ALLOCATION_SOURCE_EVIDENCE_MISMATCH/);
  assert.match(mismatchChain, /DRAFT_PARITY_ITEM_SOURCE_EVIDENCE_MISMATCH/);
  assert.match(mismatchChain, /DRAFT_PARITY_SOURCE_RESERVATION_EVIDENCE_MISMATCH/);
  assert.doesNotMatch(mismatchChain, /allocation_row\.finance_case_id IS NOT NULL[\s\S]*v_reservation_count = 0/);
  assert.match(mismatchChain, /expected_reservation_applicability = 'APPLICABLE'/);
  assert.match(mismatchChain, /expected_reservation_applicability = 'NOT_APPLICABLE'/);
  assert.match(parity, /selection_recovery_headroom_v1/);
  assert.match(parity, /workbench_settled_certificate_binding_v8/);
});

test('wrong-chat presentation text is absent from the active H2 audit contract', () => {
  const raw = JSON.stringify(audit);
  assert.doesNotMatch(raw, /I did not work this shift|- confirm/i);
  assert.equal(audit.wrong_chat_requirements_retained, false);
});
