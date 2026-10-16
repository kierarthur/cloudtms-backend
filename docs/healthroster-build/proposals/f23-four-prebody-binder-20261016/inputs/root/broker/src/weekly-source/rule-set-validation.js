// HealthRoster v2 settings: closed source contract, independent of financial identity.
// Existing client/Contract settings retain their existing owners. These values are
// immutable version inputs, never additions to policy_sha256 or NO_CHANGE.
export const RULE_SET_CHOICES = Object.freeze({
  checking_profile_kind: ['HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1', 'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1', 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'],
  self_bill_source: ['SAME_FILE', 'SEPARATE_FINAL_REPORT'],
  final_report_profile_kind: ['NONE', 'ACACIUM_SELF_BILL_REPORT_V1'],
  invoice_membership: ['FINALISED_IN_CYCLE_WINDOW', 'FOLLOW_FINAL_REPORT_LINES'],
  pay_membership: ['ALL_FINALISED_IN_FILE', 'ON_ACCEPTED_REPORT_OR_PROTECTED'],
  invoice_amount_basis: ['CONTRACT_CALCULATION', 'SOURCE_AMOUNT'],
  source_amount_basis: ['NONE', 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY', 'REPORT_LINE_NET'],
  line_vat_method: ['ROUND_EACH_LINE_FROM_NET', 'LINE_GROSS_MINUS_NET_FROM_SOURCE', 'LINE_VAT_FROM_REPORT', 'ROUND_TOTAL_FROM_EXACT_NET'],
  invoice_vat_method: ['SUM_OF_LINES', 'ONCE_ON_TOTAL_TRUNCATE', 'ONCE_ON_TOTAL_HALF_UP'],
  rate_difference_policy: ['NOT_APPLICABLE', 'FLAG_ALLOW_IGNORE'],
  negative_adjustment_policy: ['NOT_ALLOWED', 'NEGATIVE_ADJUSTMENT_LINE'],
  lookback_warning: ['OFF', 'WARN_IF_AFTER_OLDEST_PROTECTED'],
  identical_rekey: ['COMPLETE_AUTOMATICALLY'],
  authorised_late_addition: ['COMPLETE_AUTOMATICALLY'],
  equal_protected_hours: ['CLEAR_AUTOMATICALLY'],
  signed_comparison: ['ON_EVERY_PUBLICATION'],
  unfinalised_action: ['ASK_CLIENT_TO_FINALISE'],
  unfinalised_chase: ['ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT'],
  changed_query_clocks: ['REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED'],
  invoice_sender: ['CLIENT_SELF_BILL', 'AGENCY_INVOICE'],
});
for (const choices of Object.values(RULE_SET_CHOICES)) Object.freeze(choices);

export const RULE_SET_DEFAULTS = Object.freeze({
  self_bill_source: 'SAME_FILE', final_report_profile_kind: 'NONE',
  invoice_membership: 'FINALISED_IN_CYCLE_WINDOW', pay_membership: 'ALL_FINALISED_IN_FILE',
  invoice_amount_basis: 'CONTRACT_CALCULATION', source_amount_basis: 'NONE',
  line_vat_method: 'ROUND_EACH_LINE_FROM_NET', invoice_vat_method: 'SUM_OF_LINES',
  rate_difference_policy: 'NOT_APPLICABLE', negative_adjustment_policy: 'NEGATIVE_ADJUSTMENT_LINE',
  lookback_warning: 'WARN_IF_AFTER_OLDEST_PROTECTED', identical_rekey: 'COMPLETE_AUTOMATICALLY',
  authorised_late_addition: 'COMPLETE_AUTOMATICALLY', equal_protected_hours: 'CLEAR_AUTOMATICALLY',
  signed_comparison: 'ON_EVERY_PUBLICATION', unfinalised_action: 'ASK_CLIENT_TO_FINALISE',
  unfinalised_chase: 'ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT',
  changed_query_clocks: 'REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED', invoice_sender: 'CLIENT_SELF_BILL',
  earliest_final_weekday: 4, earliest_final_local_time: '00:00', attach_healthroster_section: true,
});

const CLOCK = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
const KEYS = new Set([...Object.keys(RULE_SET_CHOICES), 'earliest_final_weekday', 'earliest_final_local_time', 'attach_healthroster_section']);
function refuse(code, message) {
  const error = new Error(message); error.code = code; error.status = 400; throw error;
}

/** Scope/capability comes only from a server-resolved group/installation witness.
 * Never pass browser-supplied membership, cutoff or NEXT readiness as context.
 * Persisted database owners repeat all these checks under their own locks.
 */
export function validateRuleSetValues(input, context = {}) {
  if (!input || typeof input !== 'object' || Array.isArray(input)
      || Object.keys(input).some((key) => !KEYS.has(key))) {
    refuse('WEEKLY_SOURCE_RULE_VALUES_INVALID', 'Choose only the available source rules.');
  }
  const values = { ...RULE_SET_DEFAULTS, ...input };
  const agencyInvoice = values.invoice_sender === 'AGENCY_INVOICE';
  if (agencyInvoice) {
    // This timesheet-led preset has no cutoff/final-upload gate (R-197/R-242).
    if (!Object.hasOwn(input, 'earliest_final_weekday')) values.earliest_final_weekday = null;
    if (!Object.hasOwn(input, 'earliest_final_local_time')) values.earliest_final_local_time = null;
  }
  for (const [key, choices] of Object.entries(RULE_SET_CHOICES)) {
    if (!choices.includes(values[key])) refuse('WEEKLY_SOURCE_RULE_VALUE_INVALID', 'Choose an available value for each source rule.');
  }
  if (typeof values.attach_healthroster_section !== 'boolean') {
    refuse('WEEKLY_SOURCE_RULE_VALUE_INVALID', 'Choose whether to attach the HealthRoster section.');
  }
  const summary = values.checking_profile_kind === 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1';
  const earliestUnused = values.earliest_final_weekday === null && values.earliest_final_local_time === null;
  if (!earliestUnused && (!Number.isInteger(values.earliest_final_weekday)
      || values.earliest_final_weekday < 0 || values.earliest_final_weekday > 6
      || !CLOCK.test(values.earliest_final_local_time))) {
    refuse('WEEKLY_SOURCE_RULE_FINAL_CLOCK_INVALID', 'Choose a valid earliest final-upload day and time.');
  }
  if ((summary || agencyInvoice) !== earliestUnused) {
    refuse('WEEKLY_SOURCE_RULE_R9', summary
      ? 'Weekly summary files finalise from the group cut-off; choose Not used for earliest final upload.'
      : agencyInvoice ? 'This agency-invoice preset has no final-upload cut-off; choose Not used.'
      : 'Choose an earliest final-upload time for this file layout.');
  }
  const separate = values.self_bill_source === 'SEPARATE_FINAL_REPORT';
  if (!separate && (values.final_report_profile_kind !== 'NONE'
      || values.invoice_membership === 'FOLLOW_FINAL_REPORT_LINES'
      || values.pay_membership === 'ON_ACCEPTED_REPORT_OR_PROTECTED')) {
    refuse('WEEKLY_SOURCE_RULE_R1', 'These rules need a separate final report.');
  }
  if ((values.invoice_membership === 'FOLLOW_FINAL_REPORT_LINES'
      || values.pay_membership === 'ON_ACCEPTED_REPORT_OR_PROTECTED')
      && (!separate || values.final_report_profile_kind === 'NONE')) {
    refuse('WEEKLY_SOURCE_RULE_R2', 'Choose the separate final-report layout first.');
  }
  if ((values.invoice_amount_basis === 'SOURCE_AMOUNT' && values.source_amount_basis === 'NONE')
      || (values.source_amount_basis === 'REPORT_LINE_NET' && !separate)
      || (values.source_amount_basis === 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' && summary)) {
    refuse('WEEKLY_SOURCE_RULE_R3', 'The selected file must contain the chosen invoice amount.');
  }
  if ((values.line_vat_method === 'LINE_VAT_FROM_REPORT' && values.final_report_profile_kind === 'NONE')
      || (values.line_vat_method === 'LINE_GROSS_MINUS_NET_FROM_SOURCE'
        && values.source_amount_basis !== 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY')
      || (values.line_vat_method === 'ROUND_TOTAL_FROM_EXACT_NET' && values.invoice_amount_basis === 'SOURCE_AMOUNT')) {
    refuse('WEEKLY_SOURCE_RULE_R4', 'The selected VAT method needs amounts supplied by this file or the Contract calculation.');
  }
  if ((values.rate_difference_policy === 'FLAG_ALLOW_IGNORE') !== (values.invoice_amount_basis === 'SOURCE_AMOUNT')) {
    refuse('WEEKLY_SOURCE_RULE_R6', 'Contract-rate differences apply only when the invoice uses source amounts.');
  }
  if (context.sourceFixedExpenses === true && (!summary || context.publishedSourceExpensesReady !== true)) {
    refuse('WEEKLY_SOURCE_RULE_R7', !summary
      ? 'Source expenses are available only for the weekly summary file.'
      : 'Source expenses are waiting for compatible Banking Pay support.');
  }
  if (context.sourceFamily === 'NHSP') refuse('WEEKLY_SOURCE_RULE_R10', 'NHSP keeps its existing source rules.');
  if (!summary && !agencyInvoice && context.cutoffWeekday !== undefined) {
    if (!Number.isInteger(context.cutoffWeekday) || context.cutoffWeekday < 0 || context.cutoffWeekday > 6
        || !CLOCK.test(context.cutoffLocalTime)) refuse('WEEKLY_SOURCE_RULE_CUTOFF_INVALID', 'The group cut-off is not ready.');
    // PostgreSQL weekday convention: Sunday=0. Compare a Monday-start week.
    const ordinal = (day) => (day + 6) % 7;
    const minute = (clock) => Number(clock.slice(0, 2)) * 60 + Number(clock.slice(3));
    if (ordinal(values.earliest_final_weekday) * 1440 + minute(values.earliest_final_local_time)
        < ordinal(context.cutoffWeekday) * 1440 + minute(context.cutoffLocalTime)) {
      refuse('WEEKLY_SOURCE_RULE_R9', 'The final upload cannot open before the group cut-off.');
    }
  }
  if (values.invoice_sender === 'AGENCY_INVOICE') {
    const fixed = { ...RULE_SET_DEFAULTS, checking_profile_kind: 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
      invoice_sender: 'AGENCY_INVOICE', earliest_final_weekday: null, earliest_final_local_time: null };
    if (Object.keys(fixed).some((key) => key !== 'attach_healthroster_section' && values[key] !== fixed[key])) {
      refuse('WEEKLY_SOURCE_RULE_R13', 'This agency-invoice preset has one setting: attach the HealthRoster section.');
    }
    if (separate || values.invoice_membership === 'FOLLOW_FINAL_REPORT_LINES'
        || values.invoice_amount_basis === 'SOURCE_AMOUNT' || values.source_amount_basis !== 'NONE'
        || values.rate_difference_policy !== 'NOT_APPLICABLE' || context.sourceFixedExpenses === true) {
      refuse('WEEKLY_SOURCE_RULE_R13', 'A normal agency invoice cannot use self-bill amount or report rules.');
    }
    if (context.signedTimesheetGroup === true || context.memberSelfBill === true
        || context.memberNoTimesheetRequired === true || context.memberNhsp === true
        || context.selfBillNoInvoicesSent === true) {
      refuse('WEEKLY_SOURCE_RULE_R14', 'This client cannot use the unsigned agency-invoice preset.');
    }
  } else if (context.memberSelfBillEnabled === false) {
    refuse('WEEKLY_SOURCE_RULE_R15', 'The client must have self-bill enabled for these rules.');
  }
  return Object.freeze(values);
}

/** Called on the already-detected report layout before any staging writes. */
export function assertRuleSetReportLayout(values, detectedLayout) {
  if ((detectedLayout === 'A' && values.invoice_vat_method === 'ONCE_ON_TOTAL_TRUNCATE')
      || (detectedLayout === 'B' && values.invoice_vat_method === 'ONCE_ON_TOTAL_HALF_UP')) {
    refuse('WEEKLY_SOURCE_RULE_R11', 'The report layout does not match this invoice VAT rule.');
  }
}
