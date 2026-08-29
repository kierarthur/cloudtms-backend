// Synthetic canonical display inputs. These are not production payment data.
// Materialisation/frozen-overlap and mutation proofs are separate SQL fixtures;
// this catalogue cannot, by itself, prove those earlier lifecycle steps.
let sequence = 100;
const row = (amount, extra = {}) => ({
  preview_row_id: `00000000-0000-4000-8000-${String(++sequence).padStart(12, '0')}`,
  candidate_id: '00000000-0000-4000-8000-000000000002',
  presentation_section: 'READY_TO_PAY', section: 'canonical_preview_lines',
  presentation_role: 'CHILD', line_type: 'TIMESHEET_PAYMENT', source_kind: 'TIMESHEET',
  status: 'READY', selected: true, selection_state: 'SELECTED',
  selection_allowed: true, draftable: true, is_ready_for_draft: true,
  is_excluded_from_allocation: false, is_recognised_finance_deduction: false,
  key_type: 'SOURCE_REF', key_value: 'synthetic-fixture', pay_channel: 'PAYE',
  amount_display: amount, section_amount_display: amount, amount_ex_vat: amount,
  ...extra
});
const context = extra => ({ selection_allowed: false, draftable: false,
  is_ready_for_draft: false, is_excluded_from_allocation: true, ...extra });
module.exports = [
  { id: 'AMT-001', label: 'Ordinary PAYE, no post-Draft net calculation', amount: '500.00', rows: [row('500.00', { net_pay: '391.12' })] },
  { id: 'AMT-002', label: 'Umbrella current VAT-aware display once', amount: '120.00', rows: [row('120.00', { pay_channel: 'UMBRELLA', amount_ex_vat: '100.00', vat_amount: '20.00' })] },
  { id: 'AMT-003', label: 'Context parent XOR promoted children', amount: '100.00', rows: [
    row('100.00', context({ presentation_role: 'PARENT' })), row('40.00'), row('60.00')
  ] },
  { id: 'AMT-004', label: 'Date-bucketed child ignores parent section amount', amount: '77.32', rows: [row('77.32', { key_type: 'TS_DAY', key_value: '2026-08-24', section_amount_display: '177.32' })] },
  { id: 'AMT-005', label: 'Promoted expense', amount: '15.75', rows: [row('15.75', { line_type: 'EXPENSE_DELTA', key_type: 'EXPENSE_CODE', key_value: 'TRAVEL' })] },
  { id: 'AMT-006', label: 'Additional payment', amount: '25.00', rows: [row('25.00', { line_type: 'ADDITIONAL_PAYMENT', key_type: 'ADDITIONAL_CODE' })] },
  { id: 'AMT-007', label: 'Manual debt uses current recovery, not balance or due', amount: '-30.00', rows: [row('-30.00', { line_type: 'MANUAL_DEBT_RECOVERY', nominal_due_amount_ex_vat: '50.00', outstanding_amount: '1000.00', recoverable_this_pay_run_ex_vat: '30.00', is_recognised_finance_deduction: true })] },
  { id: 'AMT-008', label: 'Full overpayment recovery', amount: '-40.00', rows: [row('-40.00', { line_type: 'OVERPAYMENT_RECOVERY', recoverable_this_pay_run_ex_vat: '40.00', case_components: [{ source_amount: '200.00', target_outstanding_ex_vat: '200.00', preview_due_amount_ex_vat: '40.00' }], is_recognised_finance_deduction: true })] },
  { id: 'AMT-009', label: 'Partial recovery uses certified cap, not residual', amount: '-20.00', rows: [row('-20.00', { line_type: 'OVERPAYMENT_RECOVERY', recoverable_this_pay_run_ex_vat: '20.00', nominal_due_amount_ex_vat: '100.00', case_components: [{ source_amount: '100.00', target_outstanding_ex_vat: '100.00', preview_due_amount_ex_vat: '80.00' }], is_recognised_finance_deduction: true })] },
  { id: 'AMT-010', label: 'Loan repayment, not loan balance', amount: '-15.00', rows: [row('-15.00', { line_type: 'LOAN_REPAYMENT', balance: '500.00', is_recognised_finance_deduction: true })] },
  { id: 'AMT-011', label: 'Advance repayment, not advance balance', amount: '-20.00', rows: [row('-20.00', { line_type: 'PAYMENT_ADVANCE_REPAYMENT', balance: '400.00', is_recognised_finance_deduction: true })] },
  { id: 'AMT-012', label: 'Standalone positive finance adjustment', amount: '7.50', rows: [row('7.50', { line_type: 'FINANCE_ADJUSTMENT' })] },
  { id: 'AMT-013', label: 'Current resolved Ready wins over historical readiness label', amount: '88.00', rows: [row('88.00', { readiness_state: 'CASES_RESOLUTIONS', case_resolution_summary: { saved: true } })] },
  { id: 'AMT-014', label: 'Carry-forward current display precedence', amount: '66.00', rows: [row('66.00', { key_type: 'MANUAL_CARRY_FORWARD', amount_ex_vat: '55.00', vat_amount: '11.00', pay_channel: 'UMBRELLA' })] },
  { id: 'AMT-015', label: 'Correction carrier and superseded context', amount: '45.00', rows: [row('45.00', { canonical_correction_key: 'synthetic-current-carrier' }), row('95.00', context({ status: 'SUPERSEDED', selection_state: 'SUPERSEDED' }))] },
  { id: 'AMT-016', label: 'Synthetic total excluded in favour of resolved day', amount: '55.00', rows: [row('55.00', context({ key_type: 'TS_TOTAL', key_value: 'TOTAL', row_key: 'fixture:non_segment:total', resolved_segment_rows_replace_source_total: true })), row('55.00', { key_type: 'TS_DAY', key_value: '2026-08-25' })] },
  { id: 'AMT-017', label: 'Negative presentation parent is not another recovery', amount: '-10.00', rows: [row('-10.00', context({ presentation_role: 'PARENT', presentation_reason: 'NEGATIVE_ORDINARY_PRESENTATION_ONLY' })), row('-10.00', { line_type: 'OVERPAYMENT_RECOVERY', recoverable_this_pay_run_ex_vat: '10.00', is_recognised_finance_deduction: true })] },
  { id: 'AMT-018', label: 'Frozen post-Draft canonical unavailable row', amount: '0.00', rows: [row('150.00', context({ post_draft_unavailable: true, presentation_section: 'DRAFTED', selection_state: 'NOT_SELECTABLE' }))] },
  { id: 'AMT-019', label: 'Zero-current-value unselectable identity', amount: '0.00', rows: [row('0.00', context({ selection_state: 'NOT_SELECTABLE' }))] }
];
