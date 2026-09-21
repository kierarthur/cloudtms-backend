const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/i;

const TSFIN_KEYS = Object.freeze([
  'additional_charge_ex_vat', 'additional_margin_ex_vat', 'additional_pay_ex_vat',
  'additional_units_json', 'band', 'basis', 'candidate_assignment', 'candidate_id',
  'charge_bh', 'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'client_id',
  'expenses_charge_ex_vat', 'expenses_description', 'expenses_evidence_manifest',
  'expenses_evidence_r2_key', 'expenses_pay_ex_vat', 'hours_bh', 'hours_day',
  'hours_night', 'hours_sat', 'hours_sun', 'invoice_breakdown_json', 'margin_ex_vat',
  'mileage_charge_ex_vat', 'mileage_charge_rate', 'mileage_evidence_manifest',
  'mileage_evidence_r2_key', 'mileage_pay_ex_vat', 'mileage_pay_rate', 'mileage_units',
  'pay_bh', 'pay_day', 'pay_method', 'pay_night', 'pay_sat', 'pay_sun',
  'policy_snapshot_json', 'processing_status', 'rate_source_refs_json', 'role',
  'timesheet_id', 'timesheet_version', 'total_charge_ex_vat', 'total_hours',
  'total_pay_ex_vat',
]);

const ECONOMIC_SEGMENT_FIELDS = Object.freeze([
  'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh',
  'pay_amount', 'charge_amount',
]);

const DISPLAY_RATE_FIELDS = Object.freeze({
  pay_day: ['pay_vector', 'rates', 'day'],
  pay_night: ['pay_vector', 'rates', 'night'],
  pay_sat: ['pay_vector', 'rates', 'sat'],
  pay_sun: ['pay_vector', 'rates', 'sun'],
  pay_bh: ['pay_vector', 'rates', 'bh'],
  charge_day: ['charge_vector', 'rates', 'day'],
  charge_night: ['charge_vector', 'rates', 'night'],
  charge_sat: ['charge_vector', 'rates', 'sat'],
  charge_sun: ['charge_vector', 'rates', 'sun'],
  charge_bh: ['charge_vector', 'rates', 'bh'],
});

export class WeeklyCorrectFinalSnapshotError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'WeeklyCorrectFinalSnapshotError';
    this.code = code;
    this.status = 502;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, message, details = {}) {
  throw new WeeklyCorrectFinalSnapshotError(code, message, details);
}

function object(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', `${label} is invalid.`);
  }
  return value;
}

function uuid(value, label) {
  const result = String(value ?? '').trim().toLowerCase();
  if (!UUID_PATTERN.test(result)) {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', `${label} is invalid.`);
  }
  return result;
}

function clone(value, label) {
  try {
    const encoded = JSON.stringify(value);
    if (encoded == null) throw new TypeError('not JSON');
    return JSON.parse(encoded);
  } catch {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', `${label} is invalid.`);
  }
}

function numeric(value, label) {
  if (value == null || value === '') return 0;
  const result = Number(value);
  if (!Number.isFinite(result)) {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH', `${label} is invalid.`);
  }
  return result;
}

function numericOrNull(value, label) {
  if (value == null || value === '') return null;
  return numeric(value, label);
}

function round2(value) {
  const amount = numeric(value, 'Calculated amount');
  const scaled = amount * 100;
  const rounded = scaled < 0
    ? -Math.round(Math.abs(scaled) + Number.EPSILON)
    : Math.round(scaled + Number.EPSILON);
  return rounded / 100;
}

function sameNumber(left, right) {
  return Math.abs(numeric(left, 'Calculated value') - numeric(right, 'Prepared value')) < 0.000001;
}

function canonicalText(value) {
  return value == null ? null : String(value);
}

function sameJson(left, right) {
  if (left === right) return true;
  if (left == null || right == null) return left == null && right == null;
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    return left.every((entry, index) => sameJson(entry, right[index]));
  }
  if (typeof left === 'object' || typeof right === 'object') {
    if (typeof left !== 'object' || typeof right !== 'object') return false;
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    return sameJson(leftKeys, rightKeys)
      && leftKeys.every((key) => sameJson(left[key], right[key]));
  }
  return left === right;
}

function nested(value, path) {
  let current = value;
  for (const key of path) current = current?.[key];
  return current;
}

function expectedDisplayRates(expectedSegments, calculatedSnapshot) {
  const first = expectedSegments[0] ?? null;
  const weeklySource = first?.weekly_source;
  const result = {};
  for (const [field, path] of Object.entries(DISPLAY_RATE_FIELDS)) {
    const expected = weeklySource ? nested(weeklySource, path) : null;
    if (first && expected == null) {
      fail(
        'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
        'The prepared source rate authority is incomplete.',
        { field },
      );
    }
    if (first && !sameNumber(calculatedSnapshot[field], expected)) {
      fail(
        'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
        'The ordinary calculator does not match the prepared source rates.',
        { field },
      );
    }
    result[field] = first ? numericOrNull(expected, field) : numericOrNull(calculatedSnapshot[field], field);
  }
  return result;
}

function compareSegments(expectedSegments, calculatedSegments) {
  if (!Array.isArray(expectedSegments) || !Array.isArray(calculatedSegments)
      || expectedSegments.length !== calculatedSegments.length) {
    fail(
      'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
      'The ordinary calculator produced a different shift count.',
    );
  }
  expectedSegments.forEach((expectedValue, index) => {
    const expected = object(expectedValue, `Prepared shift ${index + 1}`);
    const calculated = object(calculatedSegments[index], `Calculated shift ${index + 1}`);
    for (const field of ['date', 'start', 'end', 'ref_num']) {
      if (canonicalText(calculated[field]) !== canonicalText(expected[field])) {
        fail(
          'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
          'The ordinary calculator produced different shift details.',
          { index, field },
        );
      }
    }
    if (Boolean(calculated.overnight) !== Boolean(expected.overnight)
        || Number(calculated.break_mins ?? 0) !== Number(expected.break_mins ?? 0)
        || !sameJson(calculated.breaks ?? [], expected.breaks ?? [])
        || Boolean(calculated.is_reversal) !== Boolean(expected.is_reversal)
        || Boolean(calculated.exclude_from_pay) !== Boolean(expected.exclude_from_pay)) {
      fail(
        'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
        'The ordinary calculator produced different shift details.',
        { index },
      );
    }
    for (const field of ECONOMIC_SEGMENT_FIELDS) {
      if (!sameNumber(calculated[field], expected[field])) {
        fail(
          'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
          'The ordinary calculator produced different shift values.',
          { index, field },
        );
      }
    }
  });
}

function frozenPolicy(timesheetContext) {
  const authority = object(
    timesheetContext.settings_authority_json,
    'Frozen Timesheet settings',
  );
  const values = object(authority.values, 'Frozen Timesheet settings values');
  const policy = clone(values, 'Frozen Timesheet settings values');
  delete policy.resolved_at_utc;
  return policy;
}

function sourceExpenseProjection(expectedSourceExpenses, expectedRateSourceRefs, currentFinancial) {
  if (!Array.isArray(expectedSourceExpenses)) {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', 'Prepared source expenses are invalid.');
  }
  if (expectedSourceExpenses.length) {
    const manifestHash = String(expectedRateSourceRefs.source_expense_manifest_hash ?? '').trim().toLowerCase();
    if (!SHA256_PATTERN.test(manifestHash)) {
      fail(
        'WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID',
        'The server source-expense proof is invalid.',
      );
    }
    let pence = 0;
    let zeroAuthorityCount = 0;
    for (const [index, value] of expectedSourceExpenses.entries()) {
      const expense = object(value, `Prepared source expense ${index + 1}`);
      const sourcePence = Number(expense.source_expense_pence);
      if (!Number.isSafeInteger(sourcePence) || sourcePence < 0
          || !sameNumber(expense.candidate_reimbursement_ex_vat, sourcePence / 100)
          || !sameNumber(expense.client_charge_ex_vat, sourcePence / 100)) {
        fail(
          'WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID',
          'A prepared source expense is invalid.',
          { index },
        );
      }
      if (sourcePence === 0) zeroAuthorityCount += 1;
      pence += sourcePence;
      if (!Number.isSafeInteger(pence)) {
        fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', 'Prepared source expenses are too large.');
      }
    }
    if (zeroAuthorityCount > 0 && zeroAuthorityCount < expectedSourceExpenses.length) {
      fail(
        'WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID',
        'Prepared source expenses mix zero tombstones with payable authorities.',
      );
    }
    if (zeroAuthorityCount === 0) {
      const amount = pence / 100;
      return Object.freeze({
        pay: amount,
        charge: amount,
        description: 'Source-approved expenses',
        evidenceKey: null,
        evidenceManifest: Object.freeze({
          schema_version: 'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1',
          authorities: clone(expectedSourceExpenses, 'Prepared source expenses'),
          manifest_hash: manifestHash,
        }),
      });
    }
  }

  const currentManifest = currentFinancial.expenses_evidence_manifest;
  const currentWasSourceExpense = currentManifest
    && typeof currentManifest === 'object'
    && !Array.isArray(currentManifest)
    && currentManifest.schema_version === 'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1';
  if (currentWasSourceExpense) {
    return Object.freeze({
      pay: 0,
      charge: 0,
      description: null,
      evidenceKey: null,
      evidenceManifest: null,
    });
  }
  return Object.freeze({
    pay: numeric(currentFinancial.expenses_pay_ex_vat, 'Current expense pay'),
    charge: numeric(currentFinancial.expenses_charge_ex_vat, 'Current expense charge'),
    description: currentFinancial.expenses_description ?? null,
    evidenceKey: currentFinancial.expenses_evidence_r2_key ?? null,
    evidenceManifest: clone(currentManifest ?? null, 'Current expense evidence'),
  });
}

function totals(expectedSegments, additional, expenses, mileage, policy, payMethod) {
  const sums = expectedSegments.reduce((result, segment) => ({
    hours_day: result.hours_day + numeric(segment.hours_day, 'Day hours'),
    hours_night: result.hours_night + numeric(segment.hours_night, 'Night hours'),
    hours_sat: result.hours_sat + numeric(segment.hours_sat, 'Saturday hours'),
    hours_sun: result.hours_sun + numeric(segment.hours_sun, 'Sunday hours'),
    hours_bh: result.hours_bh + numeric(segment.hours_bh, 'Bank Holiday hours'),
    pay: result.pay + numeric(segment.pay_amount, 'Shift pay'),
    charge: result.charge + numeric(segment.charge_amount, 'Shift charge'),
  }), {
    hours_day: 0,
    hours_night: 0,
    hours_sat: 0,
    hours_sun: 0,
    hours_bh: 0,
    pay: 0,
    charge: 0,
  });
  const totalPay = round2(sums.pay + additional.pay + expenses.pay + mileage.pay);
  const totalCharge = round2(sums.charge + additional.charge + expenses.charge + mileage.charge);
  const wagePay = round2(sums.pay + additional.pay);
  const reimbursementPay = round2(expenses.pay + mileage.pay);
  const erniPct = numeric(policy.erni_pct, 'Employer contribution percentage');
  const erniMultiplier = erniPct > 0 ? 1 + (erniPct > 1 ? erniPct / 100 : erniPct) : 1;
  const applyErniTo = String(policy.apply_erni_to ?? 'PAYE_ONLY').trim().toUpperCase();
  const erniApplies = String(payMethod ?? '').trim().toUpperCase() === 'PAYE'
    && ['ALL', 'PAYE_ONLY'].includes(applyErniTo);
  const wageCost = erniApplies ? round2(wagePay * erniMultiplier) : wagePay;
  return Object.freeze({
    ...Object.fromEntries(Object.entries(sums)
      .filter(([key]) => key.startsWith('hours_'))
      .map(([key, value]) => [key, round2(value)])),
    total_hours: round2(
      sums.hours_day + sums.hours_night + sums.hours_sat + sums.hours_sun + sums.hours_bh,
    ),
    total_pay_ex_vat: totalPay,
    total_charge_ex_vat: totalCharge,
    margin_ex_vat: round2(totalCharge - (wageCost + reimbursementPay)),
  });
}

/**
 * Convert one calculation made by the existing ordinary Weekly calculator
 * into the exact closed service snapshot accepted by the database owner.  The
 * prepared database context remains the identity/lineage authority; this
 * adapter only accepts the calculator result after every shift value agrees.
 */
export function adaptWeeklyCorrectFinalServiceSnapshot(input = {}) {
  const root = object(input.rootContext, 'Prepared root');
  const timesheetContext = object(input.timesheetContext, 'Timesheet context');
  const weeklyContext = object(input.weeklyContext, 'Weekly context');
  const currentFinancial = object(timesheetContext.out_cur_fin, 'Current financial record');
  const timesheet = object(timesheetContext.out_timesheet, 'Current Timesheet');
  const contract = object(weeklyContext.out_contract, 'Current Contract');
  object(weeklyContext.out_cw, 'Current Contract week');
  const calculation = object(input.calculation?.snapshot ?? input.calculation, 'Ordinary calculation');

  const rootId = uuid(root.root_timesheet_id, 'Prepared root Timesheet');
  if (uuid(timesheetContext.effective_timesheet_id, 'Effective Timesheet') !== rootId
      || uuid(timesheet.timesheet_id, 'Current Timesheet') !== rootId
      || uuid(weeklyContext.timesheet_id, 'Weekly Timesheet') !== rootId
      || uuid(calculation.timesheet_id, 'Calculated Timesheet') !== rootId
      || String(timesheet.sheet_scope ?? '').toUpperCase() !== 'WEEKLY'
      || timesheet.is_current !== true
      || uuid(contract.id, 'Current Contract') !== uuid(timesheet.contract_id, 'Timesheet Contract')) {
    fail(
      'WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID',
      'The ordinary calculation belongs to another Timesheet or Contract.',
      { root_timesheet_id: rootId },
    );
  }
  const sourceMode = String(root.source_mode ?? '').trim().toUpperCase();
  if (!['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY'].includes(sourceMode)
      || !Array.isArray(root.expected_segments)
      || !Array.isArray(root.expected_actual_schedule)) {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', 'The prepared source context is incomplete.');
  }
  const expectedRateSourceRefs = object(root.expected_rate_source_refs, 'Prepared rate authority');
  if (uuid(expectedRateSourceRefs.root_timesheet_id, 'Prepared rate Timesheet') !== rootId) {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', 'The prepared rate authority changed.');
  }
  const calculatedBreakdown = object(calculation.invoice_breakdown_json, 'Calculated shift breakdown');
  compareSegments(root.expected_segments, calculatedBreakdown.segments);
  const displayRates = expectedDisplayRates(root.expected_segments, calculation);
  const policy = frozenPolicy(timesheet);

  const additional = Object.freeze({
    units: clone(currentFinancial.additional_units_json ?? {}, 'Current additional units'),
    pay: numeric(currentFinancial.additional_pay_ex_vat, 'Current additional pay'),
    charge: numeric(currentFinancial.additional_charge_ex_vat, 'Current additional charge'),
    margin: numeric(currentFinancial.additional_margin_ex_vat, 'Current additional margin'),
  });
  const expenses = sourceExpenseProjection(
    root.expected_source_expenses,
    expectedRateSourceRefs,
    currentFinancial,
  );
  const mileage = Object.freeze({
    units: numeric(currentFinancial.mileage_units, 'Current mileage units'),
    pay: numeric(currentFinancial.mileage_pay_ex_vat, 'Current mileage pay'),
    charge: numeric(currentFinancial.mileage_charge_ex_vat, 'Current mileage charge'),
    payRate: numericOrNull(currentFinancial.mileage_pay_rate, 'Current mileage pay rate'),
    chargeRate: numericOrNull(currentFinancial.mileage_charge_rate, 'Current mileage charge rate'),
    evidenceKey: currentFinancial.mileage_evidence_r2_key ?? null,
    evidenceManifest: clone(
      currentFinancial.mileage_evidence_manifest ?? null,
      'Current mileage evidence',
    ),
  });
  const economicTotals = totals(
    root.expected_segments,
    additional,
    expenses,
    mileage,
    policy,
    contract.pay_method_snapshot,
  );

  const invoiceBreakdown = Object.freeze({
    mode: 'SEGMENTS',
    segments: clone(root.expected_segments, 'Prepared source segments'),
    additional: Object.freeze({
      units: clone(additional.units, 'Current additional units'),
      pay_ex_vat: additional.pay,
      charge_ex_vat: additional.charge,
      margin_ex_vat: additional.margin,
    }),
    totals: Object.freeze({
      total_pay_ex_vat: economicTotals.total_pay_ex_vat,
      total_charge_ex_vat: economicTotals.total_charge_ex_vat,
      margin_ex_vat: economicTotals.margin_ex_vat,
    }),
  });

  const tsfin = {
    timesheet_id: rootId,
    timesheet_version: Number(timesheet.version),
    basis: sourceMode === 'NHSP_WEEKLY' ? 'NHSP' : 'HEALTHROSTER_SELF_BILL',
    candidate_id: contract.candidate_id,
    client_id: contract.client_id,
    role: contract.role,
    band: contract.band,
    pay_method: contract.pay_method_snapshot,
    policy_snapshot_json: policy,
    rate_source_refs_json: clone(expectedRateSourceRefs, 'Prepared rate authority'),
    hours_day: economicTotals.hours_day,
    hours_night: economicTotals.hours_night,
    hours_sat: economicTotals.hours_sat,
    hours_sun: economicTotals.hours_sun,
    hours_bh: economicTotals.hours_bh,
    total_hours: economicTotals.total_hours,
    ...displayRates,
    additional_units_json: clone(additional.units, 'Current additional units'),
    additional_pay_ex_vat: additional.pay,
    additional_charge_ex_vat: additional.charge,
    additional_margin_ex_vat: additional.margin,
    expenses_pay_ex_vat: expenses.pay,
    expenses_charge_ex_vat: expenses.charge,
    expenses_description: expenses.description,
    expenses_evidence_r2_key: expenses.evidenceKey,
    expenses_evidence_manifest: clone(expenses.evidenceManifest, 'Expense evidence'),
    mileage_units: mileage.units,
    mileage_pay_ex_vat: mileage.pay,
    mileage_charge_ex_vat: mileage.charge,
    mileage_pay_rate: mileage.payRate,
    mileage_charge_rate: mileage.chargeRate,
    mileage_evidence_r2_key: mileage.evidenceKey,
    mileage_evidence_manifest: clone(mileage.evidenceManifest, 'Mileage evidence'),
    total_pay_ex_vat: economicTotals.total_pay_ex_vat,
    total_charge_ex_vat: economicTotals.total_charge_ex_vat,
    margin_ex_vat: economicTotals.margin_ex_vat,
    candidate_assignment: 'ASSIGNED',
    processing_status: 'PENDING_AUTH',
    invoice_breakdown_json: invoiceBreakdown,
  };
  const actualKeys = Object.keys(tsfin).sort();
  if (!sameJson(actualKeys, [...TSFIN_KEYS].sort())) {
    fail('WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID', 'The service snapshot key set changed.');
  }
  return Object.freeze({
    schema_version: 'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1',
    calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
    source_actual_schedule_json: Object.freeze(clone(
      root.expected_actual_schedule,
      'Prepared actual schedule',
    )),
    tsfin_snapshot_json: Object.freeze(tsfin),
  });
}

export const WEEKLY_CORRECT_FINAL_SNAPSHOT_ADAPTER_CONTRACT = Object.freeze({
  version: 'WEEKLY_CORRECT_FINAL_SNAPSHOT_ADAPTER_V1',
  calculatorOwner: 'buildWeeklyScheduleSegmentsSnapshot',
  preserves: Object.freeze(['ADDITIONAL_UNITS', 'MILEAGE', 'NON_SOURCE_EXPENSES']),
  sourceExpenseEvidenceOwner: 'DATABASE_PREPARED_MANIFEST_HASH',
  browserEconomicsAccepted: false,
});
