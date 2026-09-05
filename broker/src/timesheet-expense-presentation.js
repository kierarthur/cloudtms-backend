const EXPENSE_LINE_TYPES = new Set(['EXPENSES', 'MILEAGE']);

function upper(value) {
  return String(value == null ? '' : value).trim().toUpperCase();
}

function finiteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function finiteNumberOrNull(value) {
  if (value == null || String(value).trim() === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function numbersMatch(left, right) {
  return left !== null && right !== null && Math.abs(left - right) <= 0.000001;
}

function legacyFinancialsProveExpenseOnly(input = {}) {
  if (upper(input.line_type ?? input.lineType) !== 'HOURS') return false;
  const expenseValues = input.expense_values ?? input.expenseValues ?? {};
  const totalPay = finiteNumberOrNull(input.total_pay_ex_vat ?? input.totalPayExVat);
  const totalCharge = finiteNumberOrNull(input.total_charge_ex_vat ?? input.totalChargeExVat);
  const expensePay = finiteNumberOrNull(expenseValues.expenses_pay_ex_vat ?? expenseValues.expensesPayExVat);
  const mileagePay = finiteNumberOrNull(expenseValues.mileage_pay_ex_vat ?? expenseValues.mileagePayExVat);
  const expenseCharge = finiteNumberOrNull(expenseValues.expenses_charge_ex_vat ?? expenseValues.expensesChargeExVat);
  const mileageCharge = finiteNumberOrNull(expenseValues.mileage_charge_ex_vat ?? expenseValues.mileageChargeExVat);
  if ([totalPay, totalCharge, expensePay, mileagePay, expenseCharge, mileageCharge]
    .some((value) => value === null)) return false;
  const provedExpensePay = expensePay + mileagePay;
  const provedExpenseCharge = expenseCharge + mileageCharge;
  return Math.abs(provedExpensePay) + Math.abs(provedExpenseCharge) > 0
    && numbersMatch(totalPay, provedExpensePay)
    && numbersMatch(totalCharge, provedExpenseCharge);
}

function numericJsonAbsoluteSum(value) {
  if (value == null) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? Math.abs(value) : 0;
  if (typeof value === 'string') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? Math.abs(parsed) : 0;
  }
  if (Array.isArray(value)) return value.reduce((sum, item) => sum + numericJsonAbsoluteSum(item), 0);
  if (typeof value === 'object') return Object.values(value)
    .reduce((sum, item) => sum + numericJsonAbsoluteSum(item), 0);
  return 0;
}

function scheduleHasWork(value) {
  if (Array.isArray(value)) return value.length > 0;
  if (!value || typeof value !== 'object') return false;
  return Object.keys(value).length > 0;
}

function routePresentation(input = {}) {
  const { workflowRoute, routeFamily, explicitOfficeCreated } = input;
  const workflow = upper(workflowRoute);
  const family = upper(routeFamily);
  if (workflow === 'PAPER' || family === 'QR' || family === 'PAPER') {
    return { expense_route_kind: 'QR', display_route_label: 'QR Expense' };
  }
  if (['PHONE', 'EMAIL', 'ELECTRONIC'].includes(workflow)
    || family === 'ELECTRONIC'
    || upper(input.submission_mode ?? input.submissionMode) === 'ELECTRONIC'
    || input.import_source_proved === true
    || input.importSourceProved === true) {
    return { expense_route_kind: 'ELECTRONIC', display_route_label: 'Electronic Expense' };
  }
  if (explicitOfficeCreated === true || isExplicitOfficeCreatedExpenseRecord({
    ...input,
    expense_only_proved: true
  })) {
    return { expense_route_kind: 'MANUAL', display_route_label: 'Manual Expense' };
  }
  return { expense_route_kind: 'UNKNOWN', display_route_label: 'Expense' };
}

export function isExplicitOfficeCreatedExpenseRecord(input = {}) {
  return (EXPENSE_LINE_TYPES.has(upper(input.line_type ?? input.lineType))
      || input.expense_only_proved === true
      || input.expenseOnlyProved === true)
    && upper(input.submission_mode ?? input.submissionMode) === 'MANUAL'
    && !String(input.candidate_workflow_id ?? input.candidateWorkflowId ?? '').trim()
    && !String(input.workflowRoute ?? '').trim();
}

/**
 * Presentation-only classification. It deliberately fails closed: a stored
 * Manual Adjustment is never renamed unless its own current record proves an
 * explicit expense/mileage line, a real non-zero claim, zero work, and no
 * worked schedule or additional work units.
 */
export function classifyExpenseTimesheetPresentation(input = {}) {
  const lineType = upper(input.line_type ?? input.lineType);
  const totalHours = finiteNumberOrNull(input.total_hours ?? input.totalHours);
  const expenseValues = input.expense_values ?? input.expenseValues ?? {};
  const expenseValue = Object.values(expenseValues)
    .reduce((sum, value) => sum + Math.abs(finiteNumber(value)), 0);
  const additionalUnits = numericJsonAbsoluteSum(input.additional_units_week ?? input.additionalUnitsWeek)
    + numericJsonAbsoluteSum(input.additional_units_per_day ?? input.additionalUnitsPerDay)
    + numericJsonAbsoluteSum(input.additional_units_json ?? input.additionalUnitsJson);
  const workedSchedule = scheduleHasWork(input.actual_schedule_json ?? input.actualScheduleJson)
    || scheduleHasWork(input.financial_schedule_json ?? input.financialScheduleJson)
    || Boolean(String(input.worked_start_iso ?? input.workedStartIso ?? '').trim())
    || Boolean(String(input.worked_end_iso ?? input.workedEndIso ?? '').trim());

  const isExpenseOnly = (EXPENSE_LINE_TYPES.has(lineType) || legacyFinancialsProveExpenseOnly(input))
    && totalHours === 0
    && additionalUnits === 0
    && !workedSchedule
    && expenseValue > 0;

  if (!isExpenseOnly) {
    return {
      is_expense_only: false,
      expense_route_kind: 'UNKNOWN',
      display_route_label: input.fallback_display_label ?? input.fallbackDisplayLabel ?? null
    };
  }
  return { is_expense_only: true, ...routePresentation(input) };
}

export const expenseTimesheetPresentationInternals = Object.freeze({
  numericJsonAbsoluteSum,
  scheduleHasWork,
  legacyFinancialsProveExpenseOnly,
  routePresentation,
  isExplicitOfficeCreatedExpenseRecord
});
