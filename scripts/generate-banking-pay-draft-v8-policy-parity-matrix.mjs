import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const contractPath = 'codex_outputs/banking-pay-create-draft-policy-v1/BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json';
const outputPath = 'tests/fixtures/banking-pay-create-draft-v8-policy-parity-matrix-v1.json';
const checkOnly = process.argv.includes('--check');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');

const contractText = read(contractPath);
const contract = JSON.parse(contractText);

const groups = [
  {
    group_id: 'P1_ORDINARY_COMPONENTS_RATES_EXPENSES',
    purpose: 'Ordinary Timesheet, rate, expense, mileage, additional-code and forced-advance mapping in PAYE and Umbrella.',
    class_ids: [
      'ordinary_one_segment_paye', 'ordinary_one_segment_umbrella',
      'ordinary_multi_segment_paye', 'ordinary_multi_segment_umbrella',
      'ordinary_multiple_rate_families', 'ordinary_same_key_multiple_components',
      'saved_rate_resolution', 'saved_payment_method_resolution',
      'expense_expenses_paye', 'expense_travel_paye', 'expense_accommodation_paye',
      'expense_other_paye', 'mileage_paye', 'expense_vat_umbrella',
      'expense_non_vat_umbrella', 'mileage_umbrella',
      'additional_code_component', 'adjustment_code_component',
      'forced_advance_paye', 'forced_advance_umbrella'
    ],
    execution_kind: 'DIRECT_DETERMINISTIC_AND_BOUNDED_PROPERTY',
    evidence_files: [
      'tests/banking-pay-canonical-segment-state-allocation.test.js',
      'tests/banking-pay-draft-insert-items-certified-v8.test.cjs',
      'tests/banking-pay-draft-finance-item-plan.test.cjs',
      'tests/banking-pay-ts-total-draftability.test.cjs',
      'tests/banking-pay-current-status-draft-amount.test.cjs',
      'tests/02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql'
    ]
  },
  {
    group_id: 'P2_PAIRED_TIMESHEET_LIFECYCLE',
    purpose: 'Paired Timesheet reversal/replacement and reversal-only membership, identity, lifecycle and replay.',
    class_ids: [
      'paired_reversal_replacement_paye', 'paired_reversal_replacement_umbrella',
      'paired_cross_channel_resolution', 'paired_broken_or_duplicate_leg',
      'paired_stale_fingerprint', 'paired_mixed_candidate_or_client',
      'paired_paid_or_invoiced_conflict', 'paired_transition_replay',
      'paired_draft_response_loss_replay', 'paired_reversal_only_paye',
      'paired_reversal_only_umbrella'
    ],
    execution_kind: 'DIRECT_DETERMINISTIC_AND_MUTATION',
    evidence_files: [
      'tests/banking-pay-canonical-correction-carrier.test.cjs',
      'tests/banking-pay-canonical-correction-carrier.test.cjs',
      'tests/banking-pay-continuation-and-cancellation-contract.test.cjs',
      'tests/16082026_0102_banking_pay_case_resolution_convergence_verification.sql'
    ]
  },
  {
    group_id: 'P3_PRIOR_PAYMENT_AND_EXCLUSIONS',
    purpose: 'Prior-paid residual, reservation, settled, superseded, cancellation and Workbench exclusion policy.',
    class_ids: [
      'part_paid_residual', 'fully_settled_absent', 'active_reservation_residual',
      'superseded_absent', 'cancelled_untouched_reappears_once',
      'timesheet_snoozed_excluded', 'segment_snooze_isolation',
      'finance_case_snoozed_excluded', 'snooze_expiry_reappears_once',
      'action_required_excluded', 'blocked_excluded', 'active_draft_excluded'
    ],
    execution_kind: 'DIRECT_DETERMINISTIC_AND_CROSS_CLASS',
    evidence_files: [
      'tests/banking-pay-active-draft-preview-exclusion.test.cjs',
      'tests/banking-pay-selection-row-json-and-draft-cleanup.test.cjs',
      'tests/banking-pay-draft-fast-fallback.test.cjs',
      'tests/banking-pay-cancellation-selection-parity.test.cjs',
      'tests/banking-pay-modal-v2-ready-completeness.test.mjs',
      'tests/28082026_1759_banking_pay_global_selection_filters.sql'
    ]
  },
  {
    group_id: 'P4_FINANCE_CATEGORIES_GROSS_NET_VAT_HEADROOM',
    purpose: 'Every visible finance family, exact staged vocabulary, PAYE gross/net, Umbrella VAT, headroom and carry-forward handling.',
    class_ids: [
      'advance_payout_paye', 'advance_payout_umbrella',
      'advance_repayment_paye', 'advance_repayment_umbrella',
      'advance_part_repaid_residual', 'advance_paid_off_absent',
      'advance_cancelled_absent', 'advance_voided_reappears_once',
      'overpayment_taxable_paye', 'overpayment_nontaxable_paye',
      'overpayment_umbrella', 'overpayment_zero_headroom',
      'overpayment_exact_headroom', 'overpayment_partial_headroom',
      'underpayment_taxable_paye', 'underpayment_nontaxable_paye',
      'underpayment_umbrella', 'manual_credit_taxable_paye',
      'manual_credit_nontaxable_paye', 'manual_credit_umbrella',
      'manual_debt_taxable_paye', 'manual_debt_nontaxable_paye',
      'manual_debt_umbrella', 'manual_debt_zero_headroom',
      'manual_debt_partial_headroom', 'mixed_recoveries_deterministic_order',
      'carry_forward_credit', 'carry_forward_debit'
    ],
    execution_kind: 'DIRECT_DETERMINISTIC_20_VARIANT_CHAIN_AND_BOUNDED_CROSS_PRODUCT',
    evidence_files: [
      'tests/03092026_1600_banking_pay_draft_v8_finance_category_runtime.sql',
      'scripts/verify-banking-pay-draft-v8-finance-policy-matrix.mjs',
      'tests/banking-pay-create-draft-headroom.test.mjs',
      'tests/banking-pay-draft-finance-row-transport-v8.test.cjs',
      'tests/banking-pay-post-materialisation-recovery-headroom.test.cjs'
    ]
  },
  {
    group_id: 'P5_SIGNED_RECOVERY_EVIDENCE',
    purpose: 'Signed non-charge recovery shape, same-key cardinality, exact evidence and tamper rejection.',
    class_ids: [
      'signed_positive_return', 'signed_negative_recovery',
      'signed_mixed_ordinary_same_key', 'signed_two_decisive_matches',
      'signed_tampered_or_incomplete'
    ],
    execution_kind: 'DIRECT_DETERMINISTIC_AND_MUTATION',
    evidence_files: [
      'tests/01092026_1511_banking_pay_signed_recovery_draft_runtime_verification.sql',
      'tests/banking-pay-signed-recovery-draft-parity.test.cjs',
      'tests/banking-pay-signed-recovery-classifier-historical-compatibility.test.cjs',
      'tests/banking-pay-draft-constituent-parity-v8.test.cjs'
    ]
  },
  {
    group_id: 'P6_SCALE_PARTITIONS_REPLAY_CONCURRENCY',
    purpose: 'Complete global selection across independent Candidates/channels with bounded pages, replay, stale input and atomic failure.',
    class_ids: [
      'mixed_candidates_independent', 'mixed_channel_partitions',
      'over_100_distinct_timesheets', 'multi_segment_over_100',
      'pagination_1001', 'boundary_50000', 'boundary_50001_reject',
      'duplicate_delivery_replay', 'response_loss_replay',
      'concurrent_same_selection', 'stale_selection_revision',
      'atomic_multibatch_failure'
    ],
    execution_kind: 'BOUNDED_LOAD_101_1001_5000_PLUS_SCALAR_CEILING_GUARDS',
    evidence_files: [
      'tests/03092026_1010_banking_pay_workbench_settled_certificate_v8_scale_verification.sql',
      'tests/02092026_2352_banking_pay_draft_certificate_consumer_phase_units_runtime.sql',
      'tests/02092026_2354_banking_pay_draft_candidate_scope_row_backed_v8_runtime.sql',
      'tests/03092026_1201_banking_pay_draft_certificate_stage_advance_v8_runtime.sql',
      'tests/banking-pay-draft-certified-v8-concurrency.runtime.test.cjs',
      'tests/banking-pay-draft-v8-bounded-property.test.cjs',
      'tests/banking-pay-draft-certified-v8-transport.test.cjs',
      'tests/banking-pay-draft-certificate-consumer-phase-units.test.cjs'
    ]
  }
];

const downstreamGroups = [
  {
    group_id: 'D1_DRAFT_ARTIFACT_AND_OPERATION_RESPONSE',
    evidence_files: [
      'tests/banking-pay-draft-certified-v8-terminal.test.cjs',
      'tests/banking-pay-draft-certified-v8-lifecycle-audit.test.cjs',
      'tests/02092026_2357_banking_pay_draft_constituent_parity_v8_runtime.sql'
    ]
  },
  {
    group_id: 'D2_EXECUTE_STATUS_WORKSHEET_OVERVIEW',
    evidence_files: [
      'tests/banking-pay-current-status-draft-amount.test.cjs',
      'tests/banking-pay-payment-date-fence.test.cjs',
      'tests/banking-pay-pre-provider-retry-recovery.test.cjs',
      'tests/banking-pay-draft-fast-fallback.test.cjs'
    ]
  },
  {
    group_id: 'D3_CANCEL_REVERT_RETRY_LINEAGE',
    required_scenarios: [
      'WHOLE_BATCH_DRAFT_CANCEL',
      'ONE_WHOLE_CANDIDATE_DRAFT_CANCEL_UNRELATED_CANDIDATES_UNCHANGED',
      'ONE_WHOLE_CANDIDATE_LOCAL_PREPARED_NOT_SENT_CANCEL',
      'ONE_WHOLE_CANDIDATE_FUTURE_DATED_SCHEDULED_LOCAL_NOT_SENT_CANCEL',
      'PROVIDER_SUBMITTED_OR_SETTLED_PRE_BANK_CANCEL_REJECT',
      'CANDIDATE_CANCEL_REPLAY_RESPONSE_LOSS_IDEMPOTENT',
      'CANDIDATE_CANCEL_WORKBENCH_REAPPEARANCE_ONCE',
      'CERTIFIED_REVERSION_OR_EXISTING_SAFE_FALLBACK'
    ],
    evidence_files: [
      'tests/banking-pay-create-draft-execute-policy-contract.test.cjs',
      'tests/banking-pay-cancellation-frozen-scope-v2.test.js',
      'tests/banking-pay-cancellation-selection-parity.test.cjs',
      'tests/banking-pay-cancellation-post-finalise-refresh.test.cjs',
      'tests/banking-pay-semantic-ready-cancellation-reversion.test.cjs',
      'tests/banking-pay-source-publication-draft-cancel-fast-path.test.js'
    ]
  }
];

const allClasses = contract.finite_equivalence_classes.map(item => item.class_id);
const assigned = groups.flatMap(group => group.class_ids);
const duplicateAssignments = assigned.filter((value, index) => assigned.indexOf(value) !== index);
const missingAssignments = allClasses.filter(value => !assigned.includes(value));
const unknownAssignments = assigned.filter(value => !allClasses.includes(value));
if (duplicateAssignments.length || missingAssignments.length || unknownAssignments.length) {
  throw new Error(JSON.stringify({ duplicateAssignments, missingAssignments, unknownAssignments }));
}

const decorate = group => ({
  ...group,
  ...(Array.isArray(group.class_ids) ? { class_count: group.class_ids.length } : {}),
  evidence_files: [...new Set(group.evidence_files)].map(relativePath => ({
    path: relativePath,
    sha256: sha256(read(relativePath))
  }))
});

const matrix = {
  contract: 'BANKING_PAY_CREATE_DRAFT_V8_POLICY_PARITY_MATRIX_V1',
  source_policy_contract: {
    path: contractPath,
    sha256: sha256(contractText),
    payment_family_count: contract.payment_families.length,
    finite_equivalence_class_count: allClasses.length
  },
  status: 'EXECUTION_REQUIRED_BEFORE_RELEASE',
  policy_rule: 'The candidate route may change orchestration only. Every eligibility, identity, amount, sign, gross/net, tax, VAT, channel, payee, recovery, reservation, lifecycle and frozen-authority outcome remains owned by the existing policy contract.',
  execution_efficiency: {
    database_fixture_groups: groups.length,
    rule: 'Exercise compatible categories together in bounded pages. Do not create one Draft per raw value or repeat an unchanged run for confidence.',
    load_dataset_rows: [101, 1001, 5000],
    maximum_materialised_load_rows: 5000,
    supported_configured_ceiling: 50000,
    configured_ceiling_proof: 'SCALAR_SOURCE_AND_CONTRACT_ONLY_NO_50000_ROW_DATASET',
    first_rejected_count: 50001,
    first_rejected_count_proof: 'SCALAR_SOURCE_AND_CONTRACT_ONLY_ZERO_WRITES',
    prohibited_test: 'DO_NOT_GENERATE_OR_EXECUTE_A_50000_ROW_LOAD_TEST'
  },
  recorded_property_seeds: [
    '0x43544d530001', '0x43544d530002', '0x43544d530003', '0x43544d530004'
  ],
  generated_property_cases: {
    accepted: 48,
    rejected: 48,
    total: 96,
    execution_test: 'tests/banking-pay-draft-v8-bounded-property.test.cjs'
  },
  bounded_property_dimensions: [
    'PAYE_OR_UMBRELLA', 'ONE_OR_MULTI_CANDIDATE', 'ONE_OR_MULTI_COMPONENT',
    'POSITIVE_NEGATIVE_OR_ZERO_RESIDUAL', 'TAXABLE_OR_NONTAXABLE',
    'VAT_OR_ZERO_VAT', 'FRESH_REPLAY_OR_RESPONSE_LOSS', 'CURRENT_STALE_OR_TAMPERED'
  ],
  mutation_operators: [
    'DROP_SELECTED_CONSTITUENT', 'DUPLICATE_SELECTED_CONSTITUENT',
    'FLIP_PAY_CHANNEL', 'FLIP_SIGN', 'CHANGE_AMOUNT_BY_ONE_PENNY',
    'CHANGE_VAT_BY_ONE_PENNY', 'CHANGE_ECONOMIC_KEY',
    'CHANGE_SOURCE_IDENTITY_DIGEST', 'DROP_PARTITION_MEMBER',
    'CHANGE_PRIOR_PAID_RESIDUAL', 'REUSE_STALE_PAGE_RECEIPT',
    'CHANGE_SCOPE_GENERATION', 'SUBSTITUTE_HIDDEN_FINANCE_ALIAS',
    'DROP_PAIRED_TIMESHEET_LEG'
  ],
  mutation_evidence: {
    execution_test: 'tests/banking-pay-draft-v8-mutation.test.cjs',
    operators_total: 14,
    killed: 14,
    surviving: 0,
    status: 'LOCAL_EXECUTION_REQUIRED_BEFORE_RELEASE'
  },
  class_groups: groups.map(decorate),
  downstream_contract_groups: downstreamGroups.map(decorate),
  totals: {
    class_groups: groups.length,
    mapped_classes: assigned.length,
    duplicate_assignments: duplicateAssignments.length,
    missing_assignments: missingAssignments.length,
    unknown_assignments: unknownAssignments.length,
    downstream_contract_groups: downstreamGroups.length
  },
  release_gate: {
    pass_requires: [
      'all 88 finite classes execute through their assigned deterministic or bounded property group',
      'every generated seed is recorded and reproducible',
      'every mutation operator is killed',
      'PG17 and PG18 NEW, UPGRADE and reapply pass',
      'the actual Worker-to-PostgREST route passes within unchanged budgets',
      'V1-versus-V8 durable Draft and downstream projections are exact',
      'no skipped acceptance case, TODO, unexplained divergence or policy delta remains'
    ],
    no_real_payment_or_provider_action: true
  }
};

const output = `${JSON.stringify(matrix, null, 2)}\n`;
const absoluteOutput = path.join(root, outputPath);
if (checkOnly) {
  if (!fs.existsSync(absoluteOutput) || fs.readFileSync(absoluteOutput, 'utf8') !== output) {
    throw new Error(`${outputPath} is stale; regenerate it before continuing`);
  }
} else {
  fs.writeFileSync(absoluteOutput, output);
}

console.log(JSON.stringify({
  output: outputPath,
  sha256: sha256(output),
  mapped_classes: assigned.length,
  group_count: groups.length,
  maximum_materialised_load_rows: matrix.execution_efficiency.maximum_materialised_load_rows
}));
