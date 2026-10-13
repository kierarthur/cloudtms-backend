import test from 'node:test';
import assert from 'node:assert/strict';
import { validateRuleSetValues, assertRuleSetReportLayout } from '../../broker/src/weekly-source/rule-set-validation.js';
const basic = { checking_profile_kind: 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' };
const context = { sourceFamily: 'ROSTER', cutoffWeekday: 3, cutoffLocalTime: '23:59' };
const acacium = { checking_profile_kind: 'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1', self_bill_source: 'SEPARATE_FINAL_REPORT', final_report_profile_kind: 'ACACIUM_SELF_BILL_REPORT_V1', invoice_membership: 'FOLLOW_FINAL_REPORT_LINES', pay_membership: 'ON_ACCEPTED_REPORT_OR_PROTECTED', invoice_amount_basis: 'SOURCE_AMOUNT', source_amount_basis: 'REPORT_LINE_NET', line_vat_method: 'LINE_VAT_FROM_REPORT', invoice_vat_method: 'ONCE_ON_TOTAL_TRUNCATE', rate_difference_policy: 'FLAG_ALLOW_IGNORE' };
function rejects(values, code, extra = {}) { assert.throws(() => validateRuleSetValues(values, { ...context, ...extra }), (e) => e.code === code); }
test('Surrey uses source money/gross-net VAT and the exact cut-off', () => {
  const v = validateRuleSetValues({ ...basic, invoice_amount_basis: 'SOURCE_AMOUNT', source_amount_basis: 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY', line_vat_method: 'LINE_GROSS_MINUS_NET_FROM_SOURCE', rate_difference_policy: 'FLAG_ALLOW_IGNORE' }, context);
  assert.equal(v.earliest_final_local_time, '00:00'); assert.equal(v.earliest_final_weekday, 4);
  assert.equal(v.pay_membership, 'ALL_FINALISED_IN_FILE'); assert.ok(Object.isFrozen(v));
});
test('Acacium membership is report-controlled and Layout B VAT is accepted', () => {
  const v = validateRuleSetValues(acacium, { ...context, cutoffWeekday: 1 });
  assertRuleSetReportLayout(v, 'B'); assert.equal(v.pay_membership, 'ON_ACCEPTED_REPORT_OR_PROTECTED');
  assert.throws(() => assertRuleSetReportLayout(v, 'A'), (e) => e.code === 'WEEKLY_SOURCE_RULE_R11');
  assert.throws(() => assertRuleSetReportLayout({ ...v, invoice_vat_method: 'ONCE_ON_TOTAL_HALF_UP' }, 'B'), (e) => e.code === 'WEEKLY_SOURCE_RULE_R11');
});
test('Magnit has no earliest upload and needs published source-expense support', () => {
  const v = { checking_profile_kind: 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1', earliest_final_weekday: null, earliest_final_local_time: null };
  assert.equal(validateRuleSetValues(v, context).invoice_amount_basis, 'CONTRACT_CALCULATION');
  rejects(v, 'WEEKLY_SOURCE_RULE_R7', { sourceFixedExpenses: true });
  validateRuleSetValues(v, { ...context, sourceFixedExpenses: true, publishedSourceExpensesReady: true });
  rejects({ ...v, source_amount_basis: 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' }, 'WEEKLY_SOURCE_RULE_R3');
});
test('every supplied field is closed and mandatory recheck cannot be disabled', () => {
  rejects({ ...basic, signed_comparison: 'OFF' }, 'WEEKLY_SOURCE_RULE_VALUE_INVALID');
  rejects({ ...basic, made_up_rule: true }, 'WEEKLY_SOURCE_RULE_VALUES_INVALID');
  rejects({ ...basic, attach_healthroster_section: 'false' }, 'WEEKLY_SOURCE_RULE_VALUE_INVALID');
});
test('report-only rules never run without the declared separate report', () => {
  rejects({ ...basic, pay_membership: 'ON_ACCEPTED_REPORT_OR_PROTECTED' }, 'WEEKLY_SOURCE_RULE_R1');
  rejects({ ...acacium, final_report_profile_kind: 'NONE' }, 'WEEKLY_SOURCE_RULE_R2');
});
test('amount and VAT sources are actual layout capabilities', () => {
  rejects({ ...basic, invoice_amount_basis: 'SOURCE_AMOUNT' }, 'WEEKLY_SOURCE_RULE_R3');
  rejects({ ...basic, source_amount_basis: 'REPORT_LINE_NET' }, 'WEEKLY_SOURCE_RULE_R3');
  rejects({ ...basic, line_vat_method: 'LINE_VAT_FROM_REPORT' }, 'WEEKLY_SOURCE_RULE_R4');
  rejects({ ...acacium, line_vat_method: 'ROUND_TOTAL_FROM_EXACT_NET' }, 'WEEKLY_SOURCE_RULE_R4');
  rejects({ ...basic, rate_difference_policy: 'FLAG_ALLOW_IGNORE' }, 'WEEKLY_SOURCE_RULE_R6');
});
test('week clocks compare Monday through Sunday and cannot precede cut-off', () => {
  rejects({ ...basic, earliest_final_weekday: 3, earliest_final_local_time: '23:58' }, 'WEEKLY_SOURCE_RULE_R9');
  validateRuleSetValues({ ...basic, earliest_final_weekday: 0, earliest_final_local_time: '00:00' }, context);
  rejects({ ...basic, earliest_final_local_time: '24:00' }, 'WEEKLY_SOURCE_RULE_FINAL_CLOCK_INVALID');
  rejects({ ...basic, earliest_final_weekday: null, earliest_final_local_time: null }, 'WEEKLY_SOURCE_RULE_R9');
});
test('NHSP stays outside the new rules and source expense scope is narrow', () => {
  rejects(basic, 'WEEKLY_SOURCE_RULE_R10', { sourceFamily: 'NHSP' });
  rejects(basic, 'WEEKLY_SOURCE_RULE_R7', { sourceFixedExpenses: true, publishedSourceExpensesReady: true });
});
test('unsigned agency invoice allows only its fixed policy and one attachment setting', () => {
  const v = { ...basic, invoice_sender: 'AGENCY_INVOICE', attach_healthroster_section: false };
  assert.equal(validateRuleSetValues(v, context).attach_healthroster_section, false);
  assert.equal(validateRuleSetValues(v, context).earliest_final_weekday, null);
  rejects({ ...v, earliest_final_weekday: 4, earliest_final_local_time: '00:00' }, 'WEEKLY_SOURCE_RULE_R9');
  for (const key of ['signedTimesheetGroup', 'memberSelfBill', 'memberNoTimesheetRequired', 'memberNhsp', 'selfBillNoInvoicesSent']) rejects(v, 'WEEKLY_SOURCE_RULE_R14', { [key]: true });
  rejects({ ...acacium, invoice_sender: 'AGENCY_INVOICE' }, 'WEEKLY_SOURCE_RULE_R13');
  for(const [key,value] of [['lookback_warning','OFF'],['negative_adjustment_policy','NOT_ALLOWED'],['invoice_vat_method','ONCE_ON_TOTAL_TRUNCATE'],['checking_profile_kind','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1']]) {
    rejects({...v,[key]:value},'WEEKLY_SOURCE_RULE_R13');
  }
  rejects(basic, 'WEEKLY_SOURCE_RULE_R15', { memberSelfBillEnabled: false });
});
