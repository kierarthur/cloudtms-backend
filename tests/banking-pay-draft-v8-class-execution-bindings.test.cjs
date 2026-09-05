const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const ledgerPath = 'tests/fixtures/banking-pay-create-draft-v8-class-execution-bindings-v1.json';
const readText = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const readJson = relativePath => JSON.parse(readText(relativePath));
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const ledger = readJson(ledgerPath);

const expectedCurrentV8FinanceSelectors = {
  advance_payout_paye: [11],
  advance_payout_umbrella: [12],
  advance_repayment_paye: [9],
  advance_repayment_umbrella: [10],
  overpayment_taxable_paye: [3],
  overpayment_nontaxable_paye: [1],
  overpayment_umbrella: [2, 4],
  underpayment_taxable_paye: [15],
  underpayment_nontaxable_paye: [13],
  underpayment_umbrella: [14, 16],
  manual_credit_taxable_paye: [19],
  manual_credit_nontaxable_paye: [17],
  manual_credit_umbrella: [18, 20],
  manual_debt_taxable_paye: [7],
  manual_debt_nontaxable_paye: [5],
  manual_debt_umbrella: [6, 8]
};
const expectedCurrentV8OrdinarySelectors = {
  ordinary_one_segment_paye: ['SIMPLE_PAYE'],
  ordinary_one_segment_umbrella: ['SIMPLE_UMBRELLA'],
  ordinary_multi_segment_paye: ['MULTI_TIMESHEET_PAYE'],
  ordinary_multi_segment_umbrella: ['MULTI_TIMESHEET_UMBRELLA'],
  ordinary_multiple_rate_families: ['COMPONENT_MIX_PAYE'],
  ordinary_same_key_multiple_components: ['COMPONENT_MIX_PAYE'],
  expense_expenses_paye: ['FALLBACK_EXPENSES_PAYE'],
  expense_travel_paye: ['COMPONENT_MIX_PAYE'],
  expense_accommodation_paye: ['COMPONENT_MIX_PAYE'],
  expense_other_paye: ['COMPONENT_MIX_PAYE'],
  mileage_paye: ['COMPONENT_MIX_PAYE'],
  expense_vat_umbrella: ['COMPONENT_MIX_UMBRELLA_VAT'],
  expense_non_vat_umbrella: ['COMPONENT_MIX_UMBRELLA_NON_VAT'],
  mileage_umbrella: ['COMPONENT_MIX_UMBRELLA_VAT'],
  additional_code_component: ['COMPONENT_MIX_PAYE'],
  adjustment_code_component: ['COMPONENT_MIX_PAYE'],
  forced_advance_paye: ['COMPONENT_MIX_PAYE'],
  forced_advance_umbrella: ['COMPONENT_MIX_UMBRELLA_VAT']
};
const expectedCurrentV8SignedOutputSelectors = {
  signed_positive_return: ['signed_positive_return'],
  signed_negative_recovery: ['signed_negative_recovery'],
  signed_mixed_ordinary_same_key: ['signed_mixed_ordinary_same_key']
};
const expectedCurrentV8SignedRejectionSelectors = {
  signed_two_decisive_matches: ['signed_two_decisive_matches'],
  signed_tampered_or_incomplete: ['signed_tampered_or_incomplete']
};
const expectedCurrentV8SavedResolutionSelectors = {
  saved_rate_resolution: ['SAVED_RATE_RESOLUTION_UMBRELLA_TO_PAYE'],
  saved_payment_method_resolution: ['SAVED_PAYMENT_METHOD_RESOLUTION_PAYE_TO_UMBRELLA']
};
const expectedCurrentV8PriorOutputSelectors = {
  part_paid_residual: ['part_paid_residual'],
  active_reservation_residual: ['active_reservation_residual'],
  superseded_absent: ['superseded_absent'],
  cancelled_untouched_reappears_once: ['cancelled_untouched_reappears_once'],
  snooze_expiry_reappears_once: ['snooze_expiry_reappears_once']
};
const expectedCurrentV8PriorRejectionSelectors = {
  fully_settled_absent: ['fully_settled_absent'],
  timesheet_snoozed_excluded: ['timesheet_snoozed_excluded'],
  segment_snooze_isolation: ['segment_snooze_isolation'],
  finance_case_snoozed_excluded: ['finance_case_snoozed_excluded'],
  action_required_excluded: ['action_required_excluded'],
  blocked_excluded: ['blocked_excluded'],
  active_draft_excluded: ['active_draft_excluded']
};
const expectedCurrentV8PairedOutputSelectors = {
  paired_reversal_replacement_paye: ['paired_reversal_replacement_paye'],
  paired_reversal_replacement_umbrella: ['paired_reversal_replacement_umbrella'],
  paired_cross_channel_resolution: ['paired_cross_channel_resolution'],
  paired_transition_replay: ['paired_transition_replay'],
  paired_draft_response_loss_replay: ['paired_draft_response_loss_replay']
};
const expectedCurrentV8PairedRejectionSelectors = {
  paired_broken_or_duplicate_leg: ['paired_broken_or_duplicate_leg'],
  paired_stale_fingerprint: ['paired_stale_fingerprint'],
  paired_mixed_candidate_or_client: ['paired_mixed_candidate_or_client'],
  paired_paid_or_invoiced_conflict: ['paired_paid_or_invoiced_conflict'],
  paired_reversal_only_paye: ['paired_reversal_only_paye'],
  paired_reversal_only_umbrella: ['paired_reversal_only_umbrella']
};
const expectedCurrentV8P4OutputSelectors = {
  advance_part_repaid_residual: ['advance_part_repaid_residual'],
  advance_voided_reappears_once: ['advance_voided_reappears_once'],
  overpayment_exact_headroom: ['overpayment_exact_headroom'],
  overpayment_partial_headroom: ['overpayment_partial_headroom'],
  manual_debt_partial_headroom: ['manual_debt_partial_headroom'],
  mixed_recoveries_deterministic_order: ['mixed_recoveries_deterministic_order'],
  carry_forward_credit: ['carry_forward_credit'],
  carry_forward_debit: ['carry_forward_debit']
};
const expectedCurrentV8P4RejectionSelectors = {
  advance_paid_off_absent: ['advance_paid_off_absent'],
  advance_cancelled_absent: ['advance_cancelled_absent'],
  overpayment_zero_headroom: ['overpayment_zero_headroom'],
  manual_debt_zero_headroom: ['manual_debt_zero_headroom']
};
const expectedCurrentV8P6OutputSelectors = {
  mixed_candidates_independent: ['DISTINCT_TIMESHEETS_101'],
  mixed_channel_partitions: ['MIXED_PAYE_UMBRELLA_ORACLE_8', 'DISTINCT_TIMESHEETS_101'],
  over_100_distinct_timesheets: ['DISTINCT_TIMESHEETS_101'],
  multi_segment_over_100: ['MULTI_SEGMENT_101'],
  pagination_1001: ['PAGINATION_1001'],
  duplicate_delivery_replay: ['DISTINCT_TIMESHEETS_101'],
  response_loss_replay: ['DISTINCT_TIMESHEETS_101'],
  concurrent_same_selection: ['DISTINCT_TIMESHEETS_101']
};
const expectedCurrentV8P6RejectionSelectors = {
  boundary_50001_reject: ['boundary_50001_reject'],
  stale_selection_revision: ['stale_selection_revision']
};
const expectedCurrentV8P6ScalarSelectors = {
  boundary_50000: ['boundary_50000']
};
const expectedP6TransportSubchecks = {
  mixed_candidates_independent: 'PASS_FOUR_INTERLEAVED_CANDIDATES_AND_FULL_DRAFT_OUTPUT',
  mixed_channel_partitions: 'PASS_PAYE_UMBRELLA_PARTITIONS_AND_FULL_DRAFT_OUTPUT',
  over_100_distinct_timesheets: 'PASS_101_DISTINCT_TIMESHEETS_FULL_DRAFT_OUTPUT',
  multi_segment_over_100: 'PASS_101_SEGMENTS_ACROSS_16_TIMESHEETS_FULL_DRAFT_OUTPUT',
  pagination_1001: 'PASS_1001_ROW_PAGED_TRANSPORT_AND_FULL_DRAFT_OUTPUT',
  boundary_50000: 'PASS_SCALAR_CONFIGURED_CEILING_ONLY',
  boundary_50001_reject: 'PASS_WORKER_TYPED_PRE_ADMISSION_AND_DUAL_ENGINE_SERVER_ZERO_WRITE_REJECTION',
  duplicate_delivery_replay: 'PASS_FULL_DRAFT_DUPLICATE_DELIVERY_REPLAY',
  response_loss_replay: 'PASS_FULL_DRAFT_RESPONSE_LOSS_REPLAY',
  concurrent_same_selection: 'PASS_DIFFERENT_OPERATION_KEYS_SAME_SELECTION_ONE_ACTIVE_OPERATION_THEN_FULL_DRAFT_OUTPUT'
};

const clone = value => JSON.parse(JSON.stringify(value));

function validateLedger(candidate) {
  assert.equal(candidate.contract, 'BANKING_PAY_CREATE_DRAFT_V8_CLASS_EXECUTION_BINDINGS_V1');
  assert.equal(candidate.status, 'OPEN_EXECUTABLE_PARITY_GATES');

  for (const source of [candidate.source_policy_contract, candidate.source_parity_matrix]) {
    assert.match(source.sha256, /^[0-9a-f]{64}$/, source.path);
    assert.equal(sha256(readText(source.path)), source.sha256, `${source.path} changed without ledger review`);
  }

  const policy = readJson(candidate.source_policy_contract.path);
  const matrix = readJson(candidate.source_parity_matrix.path);
  const policyIds = policy.finite_equivalence_classes.map(row => row.class_id).sort();
  const matrixAssignments = new Map();
  for (const group of matrix.class_groups) {
    for (const classId of group.class_ids) {
      assert.equal(matrixAssignments.has(classId), false, `duplicate source-matrix assignment: ${classId}`);
      matrixAssignments.set(classId, group.group_id);
    }
  }

  const bindings = candidate.class_execution_bindings;
  assert.equal(bindings.length, 88);
  const bindingIds = bindings.map(binding => binding.class_id);
  assert.equal(new Set(bindingIds).size, bindingIds.length, 'duplicate class execution binding');
  assert.deepEqual([...bindingIds].sort(), policyIds, 'binding ledger omitted or invented a policy class');
  assert.deepEqual([...matrixAssignments.keys()].sort(), policyIds, 'source matrix omitted or invented a policy class');

  const evidenceDefinitions = candidate.evidence_definitions;
  const structuralEvidence = evidenceDefinitions.STRUCTURAL_MATRIX_CLASS_ID_ONLY;
  const structuralText = readText(structuralEvidence.path);
  assert.equal(structuralEvidence.evidence_kind, 'SOURCE_ONLY_GROUP_ASSOCIATION');
  assert.match(structuralEvidence.does_not_prove, /No class-specific Draft output/);
  assert.ok(structuralEvidence.marker_template.includes('{class_id}'));
  for (const classId of policyIds) {
    const marker = structuralEvidence.marker_template.replace('{class_id}', classId);
    assert.ok(structuralText.includes(marker), `${classId} structural marker not found`);
  }

  const financeEvidence = evidenceDefinitions.FINANCE_V8_20_VARIANT_MATRIX;
  const financeFixture = readText(financeEvidence.runtime_fixture_path);
  const canonicalFinanceFixture = readText(financeEvidence.canonical_fixture_path);
  const financeRunner = readText(financeEvidence.runner_path);
  const financeResults = readJson(financeEvidence.result_path);
  assert.equal(sha256(financeFixture), financeEvidence.runtime_fixture_sha256);
  assert.equal(sha256(canonicalFinanceFixture), financeEvidence.canonical_fixture_sha256);
  assert.equal(sha256(financeRunner), financeEvidence.runner_sha256);
  assert.equal(sha256(readText(financeEvidence.result_path)), financeEvidence.result_sha256);
  assert.equal(financeEvidence.evidence_kind, 'CURRENT_V8_DRAFT_OUTPUT_ONLY');
  assert.match(financeEvidence.does_not_prove, /does not execute V1/);
  assert.equal(financeResults.engines.pg17.pass_count, 20);
  assert.equal(financeResults.engines.pg18.pass_count, 20);
  assert.equal(financeResults.payment_policy_or_economic_change_count, 0);
  assert.equal(financeResults.provider_payment_settlement_remittance_actions, 0);
  for (const marker of financeEvidence.required_literal_markers) {
    assert.ok(financeRunner.includes(marker), `unknown finance evidence marker: ${marker}`);
  }
  for (const visibleAlias of [
    'OVERPAYMENT_RECOVERY',
    'MANUAL_DEBT_RECOVERY',
    'PAYMENT_ADVANCE_REPAYMENT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
  ]) assert.ok(canonicalFinanceFixture.includes(`'${visibleAlias}'`), visibleAlias);

  const ordinaryEvidence = evidenceDefinitions.ORDINARY_V8_8_CASE_MATRIX;
  const ordinaryCaseContract = readJson(ordinaryEvidence.case_contract_path);
  const ordinaryRunner = readText(ordinaryEvidence.runner_path);
  const ordinaryResults = readJson(ordinaryEvidence.result_path);
  assert.equal(sha256(readText(ordinaryEvidence.case_contract_path)), ordinaryEvidence.case_contract_sha256);
  assert.equal(sha256(ordinaryRunner), ordinaryEvidence.runner_sha256);
  assert.equal(sha256(readText(ordinaryEvidence.result_path)), ordinaryEvidence.result_sha256);
  assert.equal(ordinaryEvidence.evidence_kind, 'CURRENT_V8_DRAFT_OUTPUT_ONLY');
  assert.match(ordinaryEvidence.does_not_prove, /does not execute historical V1/);
  assert.equal(ordinaryResults.summary.full_typed_current_v8_policy_owner_pass, 18);
  assert.equal(ordinaryResults.summary.full_typed_v1_v8_parity_pass, 0);
  assert.deepEqual(ordinaryResults.open_class_ids.sort(), ['saved_payment_method_resolution','saved_rate_resolution']);
  for (const marker of ordinaryEvidence.required_literal_markers) {
    assert.ok(ordinaryRunner.includes(marker), `unknown ordinary evidence marker: ${marker}`);
  }
  const ordinaryCasesById = new Map(ordinaryCaseContract.cases.map(row => [row.case_id,row]));

  const signedEvidence = evidenceDefinitions.SIGNED_RECOVERY_V8_5_CASE_MATRIX;
  const signedCases = readJson(signedEvidence.case_contract_path);
  const signedRunner = readText(signedEvidence.runner_path);
  const signedResults = readJson(signedEvidence.result_path);
  assert.equal(sha256(readText(signedEvidence.case_contract_path)), signedEvidence.case_contract_sha256);
  assert.equal(sha256(signedRunner), signedEvidence.runner_sha256);
  assert.equal(sha256(readText(signedEvidence.result_path)), signedEvidence.result_sha256);
  assert.equal(signedEvidence.evidence_kind, 'CURRENT_V8_DRAFT_OUTPUT_AND_TYPED_REJECTION_ONLY');
  assert.match(signedEvidence.does_not_prove, /does not execute historical V1/);
  assert.deepEqual(signedResults.summary, {
    p5_classes: 5,full_typed_current_v8_policy_owner_pass: 3,
    typed_fail_closed_zero_write_pass: 2,full_typed_v1_v8_parity_pass: 0
  });
  for (const marker of signedEvidence.required_literal_markers) {
    assert.ok(signedRunner.includes(marker), `unknown signed-recovery evidence marker: ${marker}`);
  }
  const signedCasesById = new Map(signedCases.cases.map(row => [row.class_id,row]));

  const savedEvidence = evidenceDefinitions.SAVED_RESOLUTION_V8_2_CASE_MATRIX;
  const savedCases = readJson(savedEvidence.case_contract_path);
  const savedRunner = readText(savedEvidence.runner_path);
  const savedResults = readJson(savedEvidence.result_path);
  assert.equal(sha256(readText(savedEvidence.case_contract_path)), savedEvidence.case_contract_sha256);
  assert.equal(sha256(savedRunner), savedEvidence.runner_sha256);
  assert.equal(sha256(readText(savedEvidence.result_path)), savedEvidence.result_sha256);
  assert.equal(savedEvidence.evidence_kind, 'CURRENT_V8_DRAFT_OUTPUT_ONLY');
  assert.match(savedEvidence.does_not_prove, /does not execute historical V1/);
  assert.equal(savedResults.status, 'CURRENT_V8_LOCAL_DUAL_ENGINE_PASS_HISTORICAL_V1_PARITY_OPEN');
  assert.equal(savedResults.policy_result.policy_or_economic_delta_count, 0);
  assert.equal(savedResults.policy_result.provider_payment_settlement_remittance_actions, 0);
  assert.equal(savedResults.case_results.length, 2);
  assert.equal(savedResults.case_results.every(row => row.status === 'CURRENT_V8_LOCAL_FULL_DRAFT_OUTPUT_PASS'), true);
  for (const marker of savedEvidence.required_literal_markers) {
    assert.ok(savedRunner.includes(marker), `unknown saved-resolution evidence marker: ${marker}`);
  }
  const savedCasesByClass = new Map(savedCases.cases.map(row => [row.class_id,row]));

  const priorEvidence = evidenceDefinitions.PRIOR_EXCLUSION_V8_12_CLASS_MATRIX;
  const priorCases = readJson(priorEvidence.case_contract_path);
  const priorRuntime = readText(priorEvidence.runtime_fixture_path);
  const priorTest = readText(priorEvidence.source_test_path);
  const priorResults = readJson(priorEvidence.result_path);
  assert.equal(sha256(readText(priorEvidence.case_contract_path)), priorEvidence.case_contract_sha256);
  assert.equal(sha256(priorRuntime), priorEvidence.runtime_fixture_sha256);
  assert.equal(sha256(priorTest), priorEvidence.source_test_sha256);
  assert.equal(sha256(readText(priorEvidence.result_path)), priorEvidence.result_sha256);
  assert.equal(priorEvidence.evidence_kind, 'CURRENT_V8_DRAFT_OUTPUT_AND_TYPED_REJECTION_ONLY');
  assert.equal(priorResults.counts.classes_current_v8_runtime_pass, 12);
  assert.equal(priorResults.counts.channel_case_executions_total, 48);
  assert.equal(priorResults.counts.v1_v8_parity_pass, 0);
  assert.equal(priorResults.counts.policy_or_economic_changes, 0);
  assert.equal(priorResults.counts.provider_payment_settlement_remittance_actions, 0);
  assert.equal(priorResults.open_findings.length, 0);
  for (const marker of priorEvidence.required_literal_markers) {
    assert.ok(`${priorRuntime}\n${priorTest}\n${JSON.stringify(priorResults)}`.includes(marker), `unknown prior/exclusion evidence marker: ${marker}`);
  }
  const priorCasesByClass = new Map(priorCases.classes.map(row => [row.class_id,row]));

  const pairedEvidence = evidenceDefinitions.PAIRED_V8_11_CLASS_MATRIX;
  const pairedCases = readJson(pairedEvidence.case_contract_path);
  const pairedRuntime = readText(pairedEvidence.runtime_fixture_path);
  const pairedTest = readText(pairedEvidence.source_test_path);
  const pairedResults = readJson(pairedEvidence.result_path);
  assert.equal(sha256(readText(pairedEvidence.case_contract_path)), pairedEvidence.case_contract_sha256);
  assert.equal(sha256(pairedRuntime), pairedEvidence.runtime_fixture_sha256);
  assert.equal(sha256(pairedTest), pairedEvidence.source_test_sha256);
  assert.equal(sha256(readText(pairedEvidence.result_path)), pairedEvidence.result_sha256);
  assert.equal(pairedResults.counts.classes_runtime_pass, 11);
  assert.equal(pairedResults.counts.policy_or_economic_changes, 0);
  assert.equal(pairedResults.counts.provider_payment_settlement_remittance_actions, 0);
  const pairedCasesByClass = new Map(pairedCases.classes.map(row => [row.class_id,row]));

  const p4Evidence = evidenceDefinitions.FINANCE_LIFECYCLE_HEADROOM_V8_12_CLASS_MATRIX;
  const p4Cases = readJson(p4Evidence.case_contract_path);
  const p4Runtime = readText(p4Evidence.runtime_fixture_path);
  const p4Test = readText(p4Evidence.source_test_path);
  const p4Results = readJson(p4Evidence.result_path);
  assert.equal(sha256(readText(p4Evidence.case_contract_path)), p4Evidence.case_contract_sha256);
  assert.equal(sha256(p4Runtime), p4Evidence.runtime_fixture_sha256);
  assert.equal(sha256(p4Test), p4Evidence.source_test_sha256);
  assert.equal(sha256(readText(p4Evidence.result_path)), p4Evidence.result_sha256);
  assert.equal(p4Results.counts.runtime_cases_pass_total, 64);
  assert.equal(p4Results.counts.runtime_cases_fail_total, 0);
  assert.equal(p4Results.counts.policy_or_economic_changes, 0);
  assert.equal(p4Results.counts.provider_payment_settlement_remittance_actions, 0);
  const p4CasesByClass = new Map(p4Cases.classes.map(row => [row.class_id,row]));

  const p6Evidence = evidenceDefinitions.P6_BOUNDED_SCALE_REPLAY_CONCURRENCY;
  const p6ScaleFixture = readText(p6Evidence.scale_fixture_path);
  const p6FullDraftRunner = readText(p6Evidence.full_draft_runner_path);
  const p6FullDraftCases = readJson(p6Evidence.full_draft_case_contract_path);
  const p6PropertyTest = readText(p6Evidence.property_test_path);
  const p6ConcurrencyTest = readText(p6Evidence.concurrency_test_path);
  const p6AtomicMultibatchFixture = readText(p6Evidence.atomic_multibatch_fixture_path);
  const p6AtomicMultibatchWorkerTest = readText(p6Evidence.atomic_multibatch_worker_test_path);
  const p6TransportTest = readText(p6Evidence.transport_test_path);
  const p6ScalarBoundaryFixture = readText(p6Evidence.scalar_boundary_fixture_path);
  const p6StaleSelectionFixture = readText(p6Evidence.stale_selection_fixture_path);
  const p6Results = readJson(p6Evidence.result_path);
  assert.equal(sha256(p6ScaleFixture), p6Evidence.scale_fixture_sha256);
  assert.equal(sha256(p6FullDraftRunner), p6Evidence.full_draft_runner_sha256);
  assert.equal(sha256(readText(p6Evidence.full_draft_case_contract_path)), p6Evidence.full_draft_case_contract_sha256);
  assert.equal(sha256(p6PropertyTest), p6Evidence.property_test_sha256);
  assert.equal(sha256(p6ConcurrencyTest), p6Evidence.concurrency_test_sha256);
  assert.equal(sha256(p6AtomicMultibatchFixture), p6Evidence.atomic_multibatch_fixture_sha256);
  assert.equal(sha256(p6AtomicMultibatchWorkerTest), p6Evidence.atomic_multibatch_worker_test_sha256);
  assert.equal(sha256(p6TransportTest), p6Evidence.transport_test_sha256);
  assert.equal(sha256(p6ScalarBoundaryFixture), p6Evidence.scalar_boundary_fixture_sha256);
  assert.equal(sha256(p6StaleSelectionFixture), p6Evidence.stale_selection_fixture_sha256);
  assert.equal(sha256(readText(p6Evidence.result_path)), p6Evidence.result_sha256);
  assert.equal(p6Evidence.evidence_kind, 'CURRENT_V8_BOUNDED_TRANSPORT_AND_FULL_DRAFT_OUTPUT');
  assert.match(p6Evidence.does_not_prove, /does not materialise 50,000 rows/);
  assert.deepEqual(p6Results.materialised_boundary.tested_row_counts, [101, 1001, 5000]);
  assert.equal(p6Results.materialised_boundary.materialised_50000_test_prohibited, true);
  assert.equal(p6Results.materialised_boundary.first_rejected_count, 50001);
  assert.equal(p6Results.bounded_generated_property.cases, 96);
  assert.deepEqual(p6Results.mutation, {operators_total: 14, killed: 14, surviving: 0});
  assert.equal(p6Results.policy_result.economic_or_payment_policy_change_count, 0);
  assert.equal(p6Results.policy_result.timeout_or_lock_budget_change_count, 0);
  assert.equal(p6Results.policy_result.expanded_selection_arrays, false);
  assert.equal(p6Results.full_current_v8_draft_output.status, 'LOCAL_DUAL_ENGINE_PASS');
  assert.equal(p6Results.full_current_v8_draft_output.limits.statement_timeout_ms, 15000);
  assert.equal(p6Results.full_current_v8_draft_output.limits.timeout_increase_or_bypass, false);
  assert.equal(p6Results.full_current_v8_draft_output.cases.PAGINATION_5000.selected_constituents, 5000);
  assert.deepEqual(
    p6Results.full_current_v8_draft_output.cases.PAGINATION_5000.tested_distinct_timesheet_counts,
    [500, 1000]
  );
  for (const shape of Object.values(
    p6Results.full_current_v8_draft_output.cases.PAGINATION_5000.tested_shapes
  )) {
    assert.ok(shape.pg17.maximum_production_call_ms < 15000);
    assert.ok(shape.pg18.maximum_production_call_ms < 15000);
  }
  assert.equal(p6Results.actual_worker_postgrest_handoff.expanded_selection_arrays, false);
  assert.equal(p6Results.actual_worker_postgrest_handoff.response_loss_replay, true);
  assert.equal(p6Results.stale_selection_final_freeze.status, 'LOCAL_DUAL_ENGINE_TYPED_REJECTION_PASS');
  assert.equal(p6Results.stale_selection_final_freeze.typed_error, 'WORKBENCH_CERTIFICATE_FINAL_FREEZE_STALE');
  assert.equal(p6Results.stale_selection_final_freeze.pre_freeze_result, 'STAGING_PRESERVED_WITH_NO_PARTIAL_FREEZE');
  assert.equal(p6Results.stale_selection_final_freeze.valid_freeze_and_response_loss_replay, true);
  assert.equal(p6Results.stale_selection_final_freeze.provider_payment_settlement_remittance_actions, 0);
  assert.equal(p6Results.stale_selection_final_freeze.transaction_outcome, 'ROLLBACK_ZERO_FIXTURE_ROWS');
  for (const marker of [
    'WORKBENCH_CERTIFICATE_FINAL_FREEZE_STALE',
    'stale final freeze changed state',
    'once frozen, a later Workbench refresh cannot poison the Draft',
    'rollback did not leave zero fixture writes'
  ]) assert.ok(p6StaleSelectionFixture.includes(marker), `unknown stale-selection evidence marker: ${marker}`);
  assert.equal(p6Results.scalar_50001_rejection.status, 'LOCAL_DUAL_ENGINE_LAYERED_REJECTION_PASS');
  assert.equal(p6Results.scalar_50001_rejection.materialised_rows, 0);
  assert.equal(p6Results.scalar_50001_rejection.worker_typed_error, 'BANKING_PAY_DRAFT_CERTIFIED_SELECTED_COUNT_INVALID');
  assert.equal(p6Results.scalar_50001_rejection.worker_operation_start_called, false);
  assert.equal(p6Results.scalar_50001_rejection.provider_payment_settlement_remittance_actions, 0);
  assert.equal(p6Results.scalar_50000_configured_ceiling.status, 'LOCAL_DUAL_ENGINE_SCALAR_CONTRACT_PASS');
  assert.equal(p6Results.scalar_50000_configured_ceiling.materialised_rows, 0);
  assert.equal(p6Results.scalar_50000_configured_ceiling.worker_envelope_accepts_exact_count, 50000);
  assert.equal(p6Results.scalar_50000_configured_ceiling.database_channel_manifest_accepts_exact_count, 50000);
  assert.equal(p6Results.engines.pg17.same_selection_competing_operations, 'PASS_ONE_ACTIVE_OPERATION_SECOND_LOCK_TIMEOUT_THEN_REPLAY_RETURNS_EXISTING_OPERATION');
  assert.equal(p6Results.engines.pg18.same_selection_competing_operations, 'PASS_ONE_ACTIVE_OPERATION_SECOND_LOCK_TIMEOUT_THEN_REPLAY_RETURNS_EXISTING_OPERATION');
  assert.equal(p6Results.atomic_multibatch_failure.status, 'LOCAL_DUAL_ENGINE_AND_WORKER_COMPENSATED_FAILURE_PASS');
  assert.equal(p6Results.atomic_multibatch_failure.scenario_id, 'ATOMIC_PAYE_UMBRELLA_MULTIBATCH_FAILURE');
  assert.equal(p6Results.atomic_multibatch_failure.paye_batch_final_status, 'CANCELLED');
  assert.equal(p6Results.atomic_multibatch_failure.umbrella_batch_final_status, 'CANCELLED');
  assert.equal(p6Results.atomic_multibatch_failure.active_financial_or_reservation_state_remaining, 0);
  assert.equal(p6Results.atomic_multibatch_failure.provider_payment_settlement_remittance_actions, 0);
  assert.equal(p6Results.atomic_multibatch_failure.classification, 'COMPENSATED_FAIL_CLOSED_NOT_LITERAL_ZERO_WRITE');
  for (const marker of [
    'H2_V8_PAYE_PARTIAL_ABORT_FAILED',
    'H2_V8_UMBRELLA_PARTIAL_ABORT_FAILED',
    "status='CANCELLED'",
    'abort_failed_draft_create_partial',
    'created_pay_batch_ids: [PAYE_BATCH_ID, UMBRELLA_BATCH_ID]'
  ]) assert.ok(`${p6AtomicMultibatchFixture}\n${p6AtomicMultibatchWorkerTest}`.includes(marker), `unknown atomic multibatch evidence marker: ${marker}`);
  for (const marker of [
    '50,001 selected constituents fail before operation admission',
    'BANKING_PAY_DRAFT_CERTIFIED_SELECTED_COUNT_INVALID',
    '50,001 scalar manifest unexpectedly succeeded',
    '50,001 scalar rejection created a Draft operation'
  ]) assert.ok(`${p6TransportTest}\n${p6ScalarBoundaryFixture}`.includes(marker), `unknown 50,001 evidence marker: ${marker}`);
  for (const marker of p6Evidence.required_literal_markers) {
    assert.ok(`${p6ScaleFixture}\n${p6FullDraftRunner}\n${p6PropertyTest}\n${p6ConcurrencyTest}\n${p6TransportTest}\n${p6ScalarBoundaryFixture}\n${p6StaleSelectionFixture}\n${JSON.stringify(p6Results)}`.includes(marker), `unknown P6 evidence marker: ${marker}`);
  }
  const p6CasesByClass = new Map();
  const p6CasesById = new Map();
  for (const testCase of p6FullDraftCases.cases) {
    p6CasesById.set(testCase.case_id, testCase);
    for (const classId of testCase.class_ids) p6CasesByClass.set(classId, testCase.case_id);
  }

  const currentV8Bindings = [];
  const currentV8RejectionBindings = [];
  const currentV8ScalarBindings = [];
  const currentV8CompensatedFailureBindings = [];
  const createDraftPolicyConformanceBindings = [];
  const openBindings = [];
  const parityPassBindings = [];
  const observedFinanceOrdinals = [];
  for (const binding of bindings) {
    assert.equal(binding.group_id, matrixAssignments.get(binding.class_id), `${binding.class_id} group drift`);
    assert.ok(candidate.assertion_profiles[binding.assertion_profile_id], `${binding.class_id} unknown assertion profile`);
    assert.ok(Array.isArray(binding.evidence_refs) && binding.evidence_refs.length > 0, binding.class_id);
    for (const evidenceRef of binding.evidence_refs) {
      assert.ok(evidenceDefinitions[evidenceRef], `${binding.class_id} unknown evidence ref ${evidenceRef}`);
    }
    assert.equal(
      binding.create_draft_policy_contract_conformance_status,
      'PASS_CURRENT_V8_EXACT_POLICY_OUTCOME',
      `${binding.class_id} lacks exact current-V8 Create Draft policy conformance`
    );
    createDraftPolicyConformanceBindings.push(binding);

    if (binding.evidence_refs.includes('STRUCTURAL_MATRIX_CLASS_ID_ONLY')) {
      const marker = structuralEvidence.marker_template.replace('{class_id}', binding.class_id);
      assert.ok(structuralText.includes(marker), `${binding.class_id} structural marker not found`);
    }

    if (binding.current_v8_status === 'CURRENT_V8_ONLY') {
      currentV8Bindings.push(binding);
      assert.equal(binding.evidence_tier, 'CURRENT_V8_DRAFT_OUTPUT');
      assert.equal(binding.current_v8_case_selectors.length, 1);
      const selector = binding.current_v8_case_selectors[0];
      if (binding.evidence_refs[0] === 'FINANCE_V8_20_VARIANT_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['FINANCE_V8_20_VARIANT_MATRIX']);
        assert.equal(selector.kind, 'VARIANT_ORDINAL');
        assert.deepEqual(selector.ordinals, expectedCurrentV8FinanceSelectors[binding.class_id], binding.class_id);
        observedFinanceOrdinals.push(...selector.ordinals);
      } else if (binding.evidence_refs[0] === 'ORDINARY_V8_8_CASE_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['ORDINARY_V8_8_CASE_MATRIX']);
        assert.equal(selector.kind, 'CASE_ID');
        assert.deepEqual(selector.case_ids, expectedCurrentV8OrdinarySelectors[binding.class_id], binding.class_id);
        for (const caseId of selector.case_ids) {
          assert.ok(ordinaryCasesById.get(caseId)?.class_ids.includes(binding.class_id), `${binding.class_id} is not bound by ${caseId}`);
        }
      } else if (binding.evidence_refs[0] === 'SAVED_RESOLUTION_V8_2_CASE_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['SAVED_RESOLUTION_V8_2_CASE_MATRIX']);
        assert.equal(selector.kind, 'CASE_ID');
        assert.deepEqual(selector.case_ids, expectedCurrentV8SavedResolutionSelectors[binding.class_id], binding.class_id);
        assert.equal(savedCasesByClass.get(binding.class_id)?.case_id, selector.case_ids[0]);
      } else if (binding.evidence_refs[0] === 'PRIOR_EXCLUSION_V8_12_CLASS_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['PRIOR_EXCLUSION_V8_12_CLASS_MATRIX']);
        assert.equal(selector.kind, 'CASE_ID');
        assert.deepEqual(selector.case_ids, expectedCurrentV8PriorOutputSelectors[binding.class_id], binding.class_id);
        assert.equal(priorCasesByClass.get(binding.class_id)?.expected?.target_selected, 1);
      } else if (binding.evidence_refs[0] === 'PAIRED_V8_11_CLASS_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['PAIRED_V8_11_CLASS_MATRIX']);
        assert.equal(selector.kind, 'CASE_ID');
        assert.deepEqual(selector.case_ids, expectedCurrentV8PairedOutputSelectors[binding.class_id], binding.class_id);
        assert.ok(pairedCasesByClass.has(binding.class_id));
      } else if (binding.evidence_refs[0] === 'FINANCE_LIFECYCLE_HEADROOM_V8_12_CLASS_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['FINANCE_LIFECYCLE_HEADROOM_V8_12_CLASS_MATRIX']);
        assert.equal(selector.kind, 'CASE_ID');
        assert.deepEqual(selector.case_ids, expectedCurrentV8P4OutputSelectors[binding.class_id], binding.class_id);
        assert.ok(p4CasesByClass.has(binding.class_id));
      } else if (binding.evidence_refs[0] === 'P6_BOUNDED_SCALE_REPLAY_CONCURRENCY') {
        assert.deepEqual(binding.evidence_refs, ['P6_BOUNDED_SCALE_REPLAY_CONCURRENCY']);
        assert.equal(selector.kind, 'CASE_ID');
        assert.deepEqual(selector.case_ids, expectedCurrentV8P6OutputSelectors[binding.class_id], binding.class_id);
        if (binding.class_id === 'concurrent_same_selection') {
          assert.ok(p6CasesById.has(selector.case_ids[0]), 'concurrency result lacks its complete winning-operation Draft-output case');
          assert.equal(p6Results.engines.pg17.same_selection_competing_operations, 'PASS_ONE_ACTIVE_OPERATION_SECOND_LOCK_TIMEOUT_THEN_REPLAY_RETURNS_EXISTING_OPERATION');
          assert.equal(p6Results.engines.pg18.same_selection_competing_operations, 'PASS_ONE_ACTIVE_OPERATION_SECOND_LOCK_TIMEOUT_THEN_REPLAY_RETURNS_EXISTING_OPERATION');
        } else if (binding.class_id === 'mixed_channel_partitions') {
          assert.equal(p6CasesByClass.get(binding.class_id), 'DISTINCT_TIMESHEETS_101');
          assert.equal(selector.case_ids[0], 'MIXED_PAYE_UMBRELLA_ORACLE_8');
        } else {
          assert.equal(p6CasesByClass.get(binding.class_id), selector.case_ids[0]);
        }
      } else {
        assert.deepEqual(binding.evidence_refs, ['SIGNED_RECOVERY_V8_5_CASE_MATRIX']);
        assert.equal(selector.kind, 'CASE_ID');
        assert.deepEqual(selector.case_ids, expectedCurrentV8SignedOutputSelectors[binding.class_id], binding.class_id);
        assert.equal(signedCasesById.get(binding.class_id)?.class_id, binding.class_id);
      }
    } else if (binding.current_v8_status === 'CURRENT_V8_TYPED_REJECTION_ONLY') {
      currentV8RejectionBindings.push(binding);
      assert.equal(binding.evidence_tier, 'CURRENT_V8_TYPED_REJECTION');
      assert.equal(binding.assertion_profile_id, 'FAIL_CLOSED_ZERO_WRITE');
      assert.equal(binding.current_v8_case_selectors.length, 1);
      const selector = binding.current_v8_case_selectors[0];
      assert.equal(selector.kind, 'CASE_ID');
      if (binding.evidence_refs[0] === 'PRIOR_EXCLUSION_V8_12_CLASS_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['PRIOR_EXCLUSION_V8_12_CLASS_MATRIX']);
        assert.deepEqual(selector.case_ids, expectedCurrentV8PriorRejectionSelectors[binding.class_id], binding.class_id);
        assert.equal(priorCasesByClass.get(binding.class_id)?.expected?.target_selected, 0);
      } else if (binding.evidence_refs[0] === 'PAIRED_V8_11_CLASS_MATRIX') {
        if (binding.class_id.startsWith('paired_reversal_only_')) {
          assert.equal(binding.source_lifecycle_status, 'VALID_GENUINE_REVERSAL_ONLY');
          assert.equal(binding.draft_zero_headroom_outcome, 'RECOVERY_RETAINED_NOT_SELECTED');
          assert.deepEqual(binding.evidence_refs, ['PAIRED_V8_11_CLASS_MATRIX','FINANCE_LIFECYCLE_HEADROOM_V8_12_CLASS_MATRIX']);
          assert.deepEqual(binding.cross_class_covering_case_ids, ['overpayment_zero_headroom']);
          assert.ok(p4CasesByClass.has('overpayment_zero_headroom'));
          assert.equal(pairedResults.reversal_only_recovery_completion.classification, 'DELIBERATE_CROSS_CLASS_EXECUTABLE_POLICY_PROOF');
          assert.ok(pairedResults.reversal_only_recovery_completion.covering_p4_classes.includes('overpayment_zero_headroom'));
          assert.equal(pairedResults.reversal_only_recovery_completion.policy_or_economic_change, false);
        } else {
          assert.deepEqual(binding.evidence_refs, ['PAIRED_V8_11_CLASS_MATRIX']);
          assert.equal(Object.hasOwn(binding, 'cross_class_covering_case_ids'), false);
        }
        assert.deepEqual(selector.case_ids, expectedCurrentV8PairedRejectionSelectors[binding.class_id], binding.class_id);
        assert.ok(pairedCasesByClass.has(binding.class_id));
      } else if (binding.evidence_refs[0] === 'FINANCE_LIFECYCLE_HEADROOM_V8_12_CLASS_MATRIX') {
        assert.deepEqual(binding.evidence_refs, ['FINANCE_LIFECYCLE_HEADROOM_V8_12_CLASS_MATRIX']);
        assert.deepEqual(selector.case_ids, expectedCurrentV8P4RejectionSelectors[binding.class_id], binding.class_id);
        assert.ok(p4CasesByClass.has(binding.class_id));
      } else if (binding.evidence_refs[0] === 'P6_BOUNDED_SCALE_REPLAY_CONCURRENCY') {
        assert.deepEqual(binding.evidence_refs, ['P6_BOUNDED_SCALE_REPLAY_CONCURRENCY']);
        assert.deepEqual(selector.case_ids, expectedCurrentV8P6RejectionSelectors[binding.class_id], binding.class_id);
        if (binding.class_id === 'stale_selection_revision') {
          assert.equal(p6Results.stale_selection_final_freeze.status, 'LOCAL_DUAL_ENGINE_TYPED_REJECTION_PASS');
        } else {
          assert.equal(binding.class_id, 'boundary_50001_reject');
          assert.equal(p6Results.scalar_50001_rejection.status, 'LOCAL_DUAL_ENGINE_LAYERED_REJECTION_PASS');
        }
      } else {
        assert.deepEqual(binding.evidence_refs, ['SIGNED_RECOVERY_V8_5_CASE_MATRIX']);
        assert.deepEqual(selector.case_ids, expectedCurrentV8SignedRejectionSelectors[binding.class_id], binding.class_id);
        assert.equal(signedCasesById.get(binding.class_id)?.class_id, binding.class_id);
      }
    } else if (binding.current_v8_status === 'CURRENT_V8_COMPENSATED_FAILURE_PASS') {
      currentV8CompensatedFailureBindings.push(binding);
      assert.equal(binding.evidence_tier, 'CURRENT_V8_COMPENSATED_FAILURE');
      assert.equal(binding.assertion_profile_id, 'FAIL_CLOSED_COMPENSATED_NO_ACTIVE_FINANCIAL_STATE');
      assert.deepEqual(binding.evidence_refs, ['P6_BOUNDED_SCALE_REPLAY_CONCURRENCY']);
      assert.deepEqual(binding.current_v8_case_selectors, [{kind: 'SCENARIO_ID',scenario_ids: ['ATOMIC_PAYE_UMBRELLA_MULTIBATCH_FAILURE']}]);
      assert.equal(p6Results.atomic_multibatch_failure.status, 'LOCAL_DUAL_ENGINE_AND_WORKER_COMPENSATED_FAILURE_PASS');
    } else if (binding.current_v8_status === 'CURRENT_V8_SCALAR_CONTRACT_PASS') {
      currentV8ScalarBindings.push(binding);
      assert.equal(binding.evidence_tier, 'SCALAR_CONTRACT_GUARD_ONLY');
      assert.equal(binding.assertion_profile_id, 'BOUNDED_TRANSPORT_ONLY');
      assert.deepEqual(binding.evidence_refs, ['P6_BOUNDED_SCALE_REPLAY_CONCURRENCY']);
      assert.equal(binding.current_v8_case_selectors.length, 1);
      const selector = binding.current_v8_case_selectors[0];
      assert.equal(selector.kind, 'CASE_ID');
      assert.deepEqual(selector.case_ids, expectedCurrentV8P6ScalarSelectors[binding.class_id], binding.class_id);
      assert.equal(p6Results.scalar_50000_configured_ceiling.status, 'LOCAL_DUAL_ENGINE_SCALAR_CONTRACT_PASS');
    } else {
      openBindings.push(binding);
      assert.match(binding.current_v8_status, /^OPEN_/);
      assert.deepEqual(binding.current_v8_case_selectors, []);
      assert.ok(binding.evidence_refs.includes('STRUCTURAL_MATRIX_CLASS_ID_ONLY'));
    }

    if (binding.v1_v8_typed_parity_status === 'PARITY_PASS') {
      parityPassBindings.push(binding);
      assert.deepEqual(binding.route_coverage, ['V1', 'V8'], `${binding.class_id} lacks exact V1+V8 routes`);
      assert.equal(binding.assertion_profile_id, 'FULL_TYPED_DRAFT_V1_V8');
      assert.ok(binding.result_manifest && typeof binding.result_manifest === 'object', `${binding.class_id} lacks typed result manifest`);
      assert.match(binding.result_manifest.path, /\S/);
      assert.match(binding.result_manifest.sha256, /^[0-9a-f]{64}$/);
      assert.equal(sha256(readText(binding.result_manifest.path)), binding.result_manifest.sha256);
    } else {
      assert.equal(binding.v1_v8_typed_parity_status, 'OPEN', binding.class_id);
      assert.equal(binding.result_manifest, null, `${binding.class_id} must not carry an unaccepted result manifest`);
    }

    if (Object.hasOwn(expectedP6TransportSubchecks, binding.class_id)) {
      assert.ok(binding.evidence_refs.includes('P6_BOUNDED_SCALE_REPLAY_CONCURRENCY'), `${binding.class_id} lacks bounded transport evidence`);
      assert.equal(binding.transport_subcheck_status, expectedP6TransportSubchecks[binding.class_id]);
      if (Object.hasOwn(expectedCurrentV8P6OutputSelectors, binding.class_id)) {
        assert.equal(binding.current_v8_status, 'CURRENT_V8_ONLY', `${binding.class_id} lacks full current-V8 Draft output status`);
      } else if (Object.hasOwn(expectedCurrentV8P6RejectionSelectors, binding.class_id)) {
        assert.equal(binding.current_v8_status, 'CURRENT_V8_TYPED_REJECTION_ONLY', `${binding.class_id} lacks typed rejection status`);
      } else if (Object.hasOwn(expectedCurrentV8P6ScalarSelectors, binding.class_id)) {
        assert.equal(binding.current_v8_status, 'CURRENT_V8_SCALAR_CONTRACT_PASS', `${binding.class_id} lacks scalar contract status`);
      }
    }
  }

  assert.deepEqual(
    currentV8Bindings.map(binding => binding.class_id).sort(),
    [
      ...Object.keys(expectedCurrentV8FinanceSelectors),...Object.keys(expectedCurrentV8OrdinarySelectors),
      ...Object.keys(expectedCurrentV8SignedOutputSelectors),...Object.keys(expectedCurrentV8SavedResolutionSelectors),
      ...Object.keys(expectedCurrentV8PriorOutputSelectors),...Object.keys(expectedCurrentV8PairedOutputSelectors),
      ...Object.keys(expectedCurrentV8P4OutputSelectors),...Object.keys(expectedCurrentV8P6OutputSelectors)
    ].sort()
  );
  assert.deepEqual(
    currentV8RejectionBindings.map(binding => binding.class_id).sort(),
    [...Object.keys(expectedCurrentV8SignedRejectionSelectors),...Object.keys(expectedCurrentV8PriorRejectionSelectors),
      ...Object.keys(expectedCurrentV8PairedRejectionSelectors),...Object.keys(expectedCurrentV8P4RejectionSelectors),
      ...Object.keys(expectedCurrentV8P6RejectionSelectors)].sort()
  );
  assert.deepEqual(observedFinanceOrdinals.sort((a, b) => a - b), Array.from({ length: 20 }, (_, index) => index + 1));
  assert.equal(currentV8Bindings.length, 65);
  assert.equal(currentV8RejectionBindings.length, 21);
  assert.equal(currentV8ScalarBindings.length, 1);
  assert.equal(currentV8CompensatedFailureBindings.length, 1);
  assert.deepEqual(currentV8CompensatedFailureBindings.map(binding => binding.class_id), ['atomic_multibatch_failure']);
  assert.equal(openBindings.length, 0);
  assert.equal(createDraftPolicyConformanceBindings.length, 88);
  assert.deepEqual(parityPassBindings.map(binding => binding.class_id), ['mixed_channel_partitions']);

  assert.deepEqual(candidate.create_draft_policy_contract_conformance, {
    status: 'PASS_ALL_88_CURRENT_V8_CREATE_DRAFT_OUTCOMES',
    scope: 'Exact current-V8 Draft output, typed zero-write rejection, compensated failure or scalar ceiling outcome for every frozen policy class. Downstream Execute Payment and cancellation gates remain separately measured and this status does not claim historical V1 executed all 88 classes.',
    source_policy_contract_sha256: candidate.source_policy_contract.sha256,
    class_count: 88,
    pass_count: 88,
    policy_or_economic_delta_count: 0,
    direct_historical_v1_execution_claim: false
  });

  assert.deepEqual(candidate.execution_boundary.materialised_load_rows, [101, 1001, 5000]);
  assert.equal(candidate.execution_boundary.maximum_materialised_load_rows, 5000);
  assert.equal(candidate.execution_boundary.configured_ceiling, 50000);
  assert.equal(candidate.execution_boundary.configured_ceiling_evidence_kind, 'SCALAR_SOURCE_AND_CONTRACT_ONLY');
  assert.equal(candidate.execution_boundary.first_rejected_count, 50001);
  assert.equal(candidate.execution_boundary.prohibited_materialised_row_count, 50000);
  assert.equal(candidate.execution_boundary.materialised_load_rows.includes(50000), false);
  assert.match(candidate.execution_boundary.unchanged_timeout_rule, /may be increased, removed or bypassed/);

  assert.deepEqual(candidate.totals, {
    classes: 88,
    current_v8_exact_draft_output_only: 65,
    current_v8_typed_rejection_only: 21,
    current_v8_compensated_failure_only: 1,
    current_v8_scalar_contract_only: 1,
    open_class_specific_draft_output_or_rejection: 0,
    current_v8_create_draft_policy_contract_conformance_pass: 88,
    v1_v8_full_typed_parity_pass: 1,
    downstream_execution_gates: 11,
    downstream_execution_gates_pass: 0
  });
  assert.equal(candidate.downstream_execution_gates.length, 11);
  assert.ok(candidate.downstream_execution_gates.every(gate => gate.status === 'OPEN_EXECUTABLE_V1_V8_PARITY_REQUIRED'));
  for (const requiredGate of [
    'PAYE_WORKSHEET_BEFORE_AND_AFTER_MANUAL_NET',
    'IMMEDIATE_EXECUTE_ELIGIBILITY_PREVIEW_LOCAL_PREPARATION',
    'SCHEDULED_EXECUTE_ELIGIBILITY_PREVIEW_LOCAL_PREPARATION',
    'CURRENT_PAYMENT_STATUS_AND_OVERVIEW',
    'WHOLE_CANDIDATE_UNTOUCHED_DRAFT_CANCEL',
    'WHOLE_CANDIDATE_FUTURE_DATED_EXECUTED_NOT_PAID_CANCEL',
    'CERTIFIED_REVERSION_OR_EXISTING_SAFE_FALLBACK'
  ]) assert.ok(candidate.downstream_execution_gates.some(gate => gate.gate_id === requiredGate), requiredGate);

  return true;
}

test('the execution ledger binds all 88 classes without overstating current V8 or V1-versus-V8 proof', () => {
  assert.equal(validateLedger(ledger), true);
});

test('the ledger validator rejects an omitted or duplicate class binding', () => {
  const omitted = clone(ledger);
  omitted.class_execution_bindings.pop();
  assert.throws(() => validateLedger(omitted));

  const duplicated = clone(ledger);
  duplicated.class_execution_bindings[1].class_id = duplicated.class_execution_bindings[0].class_id;
  assert.throws(() => validateLedger(duplicated));
});

test('the ledger validator rejects unknown evidence markers and evidence references', () => {
  const unknownMarker = clone(ledger);
  unknownMarker.evidence_definitions.STRUCTURAL_MATRIX_CLASS_ID_ONLY.marker_template = '__UNKNOWN_{class_id}_MARKER__';
  assert.throws(() => validateLedger(unknownMarker));

  const unknownReference = clone(ledger);
  unknownReference.class_execution_bindings[0].evidence_refs = ['NOT_A_REAL_EVIDENCE_DEFINITION'];
  assert.throws(() => validateLedger(unknownReference));
});

test('the ledger validator rejects false current-V8 and V1-versus-V8 PASS claims', () => {
  const falseCurrentV8 = clone(ledger);
  const compensatedIndex = falseCurrentV8.class_execution_bindings.findIndex(binding => binding.class_id === 'atomic_multibatch_failure');
  falseCurrentV8.class_execution_bindings[compensatedIndex].current_v8_status = 'CURRENT_V8_ONLY';
  assert.throws(() => validateLedger(falseCurrentV8));

  const falseParity = clone(ledger);
  falseParity.class_execution_bindings[0].v1_v8_typed_parity_status = 'PARITY_PASS';
  falseParity.class_execution_bindings[0].route_coverage = ['V8'];
  assert.throws(() => validateLedger(falseParity));

  const noManifest = clone(ledger);
  noManifest.class_execution_bindings[0].v1_v8_typed_parity_status = 'PARITY_PASS';
  noManifest.class_execution_bindings[0].route_coverage = ['V1', 'V8'];
  assert.throws(() => validateLedger(noManifest));

  const falsePolicyConformance = clone(ledger);
  falsePolicyConformance.class_execution_bindings[0].create_draft_policy_contract_conformance_status = 'OPEN';
  assert.throws(() => validateLedger(falsePolicyConformance));

  const falsePolicyTotal = clone(ledger);
  falsePolicyTotal.create_draft_policy_contract_conformance.pass_count = 87;
  assert.throws(() => validateLedger(falsePolicyTotal));
});

test('the ledger validator rejects any materialised 50,000-row test', () => {
  const prohibitedLoad = clone(ledger);
  prohibitedLoad.execution_boundary.materialised_load_rows.push(50000);
  assert.throws(() => validateLedger(prohibitedLoad));
});
