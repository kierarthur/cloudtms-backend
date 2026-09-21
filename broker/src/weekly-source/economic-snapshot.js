import { WEEKLY_SHIFT_CALCULATOR_VERSION } from './weekly-rate-owner.js';

const BUCKETS = Object.freeze(['day', 'night', 'sat', 'sun', 'bh']);
const SOURCE_MODES = new Set(['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY']);
const RATE_METHODS = new Set(['SPLIT_RATE_WINDOWS', 'WHOLE_SHIFT_START_DAY']);
const PUBLISHABLE_NHSP_RESULTS = new Set([
  'EXACT',
  'SOURCE_ROUNDING_EQUIVALENT',
  'MISMATCH',
  'ZERO_SOURCE_CHARGE',
]);
const PENCE_LIMIT = 999999999999n;

export const WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_SCHEMA_VERSION =
  'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

const upper = (value) => String(value ?? '').trim().toUpperCase();

function requireObject(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(code, `${label} is unavailable.`);
  }
  return value;
}

function wholeMinutes(value, code, label, { positive = false } = {}) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0 || (positive && parsed === 0)) {
    fail(code, `${label} must be ${positive ? 'a positive' : 'a non-negative'} whole number of minutes.`);
  }
  return parsed;
}

function normaliseSignedNumber(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    fail('WEEKLY_SOURCE_ECONOMIC_NUMBER_INVALID', 'The canonical calculation contains an invalid number.');
  }
  return Object.is(parsed, -0) ? 0 : parsed;
}

function integerPence(value, code, label) {
  const text = String(value ?? '').trim();
  if (!/^-?\d+$/.test(text)) fail(code, `${label} is unavailable.`);
  const parsed = BigInt(text);
  if (parsed < -PENCE_LIMIT || parsed > PENCE_LIMIT) {
    fail(code, `${label} is outside the supported range.`);
  }
  return parsed;
}

function round2(value) {
  return Math.round((Number(value) || 0) * 100) / 100;
}

function strictRateTable(value, label) {
  const rates = requireObject(value, 'WEEKLY_SOURCE_ECONOMIC_RATES_UNAVAILABLE', `${label} rates`);
  const result = {};
  for (const bucket of BUCKETS) {
    const rate = Number(rates[bucket]);
    if (!Number.isFinite(rate) || rate <= 0) {
      fail('WEEKLY_SOURCE_ECONOMIC_RATE_INVALID', `${label} ${bucket} rate is invalid.`, { bucket });
    }
    result[bucket] = rate;
  }
  return Object.freeze(result);
}

function canonicalBuckets(calculation) {
  const rawMinutes = requireObject(
    calculation.bucketMinutes,
    'WEEKLY_SOURCE_ECONOMIC_BUCKETS_INVALID',
    'Canonical bucket minutes',
  );
  const rawHours = requireObject(
    calculation.hours,
    'WEEKLY_SOURCE_ECONOMIC_HOURS_INVALID',
    'Canonical hours',
  );
  const sign = Number(calculation.sign);
  if (sign !== 1 && sign !== -1) {
    fail('WEEKLY_SOURCE_ECONOMIC_SIGN_INVALID', 'The canonical shift sign is invalid.');
  }

  const bucketMinutes = {};
  const hours = {};
  for (const bucket of BUCKETS) {
    const minutes = wholeMinutes(
      rawMinutes[bucket],
      'WEEKLY_SOURCE_ECONOMIC_BUCKETS_INVALID',
      `${bucket} time`,
    );
    const expectedHours = normaliseSignedNumber(round2((minutes / 60) * sign));
    const actualHours = normaliseSignedNumber(rawHours[bucket]);
    if (actualHours !== expectedHours) {
      fail('WEEKLY_SOURCE_ECONOMIC_HOURS_MISMATCH', 'Canonical hours do not match canonical minutes.', {
        bucket,
        expectedHours,
        actualHours,
      });
    }
    bucketMinutes[bucket] = minutes;
    hours[bucket] = expectedHours;
  }
  return {
    sign,
    bucketMinutes: Object.freeze(bucketMinutes),
    hours: Object.freeze(hours),
  };
}

function expectedPence(hours, rates, sign) {
  const unsignedAmount = round2(BUCKETS.reduce(
    (sum, bucket) => sum + (Math.abs(hours[bucket]) * rates[bucket]),
    0,
  ));
  const pence = Math.round(round2(unsignedAmount * sign) * 100);
  if (!Number.isSafeInteger(pence)) {
    fail('WEEKLY_SOURCE_ECONOMIC_AMOUNT_OUT_OF_RANGE', 'The canonical shift amount is outside the supported range.');
  }
  const value = BigInt(pence);
  if (value < -PENCE_LIMIT || value > PENCE_LIMIT) {
    fail('WEEKLY_SOURCE_ECONOMIC_AMOUNT_OUT_OF_RANGE', 'The canonical shift amount is outside the supported range.');
  }
  return value;
}

function validateBreakAllocation(value, breakMinutes) {
  if (value == null) return null;
  const allocation = requireObject(
    value,
    'WEEKLY_SOURCE_ECONOMIC_BREAK_ALLOCATION_INVALID',
    'Canonical break allocation',
  );
  const bucket = String(allocation.bucket ?? '').trim().toLowerCase();
  const portionMinutes = wholeMinutes(
    allocation.portionMinutes,
    'WEEKLY_SOURCE_ECONOMIC_BREAK_ALLOCATION_INVALID',
    'Break rate portion',
    { positive: true },
  );
  const deductedMinutes = wholeMinutes(
    allocation.deductedMinutes,
    'WEEKLY_SOURCE_ECONOMIC_BREAK_ALLOCATION_INVALID',
    'Allocated break',
    { positive: true },
  );
  const tieRule = upper(allocation.tieRule);
  if (
    !BUCKETS.includes(bucket)
    || !['EARLIEST_LONGEST_PORTION', 'LATEST_LONGEST_PORTION'].includes(tieRule)
    || deductedMinutes !== breakMinutes
    || deductedMinutes > portionMinutes
  ) {
    fail('WEEKLY_SOURCE_ECONOMIC_BREAK_ALLOCATION_INVALID', 'The canonical break allocation is inconsistent.');
  }
  const startMs = new Date(allocation.startInstant).getTime();
  const endMs = new Date(allocation.endInstant).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    fail('WEEKLY_SOURCE_ECONOMIC_BREAK_ALLOCATION_INVALID', 'The canonical break allocation interval is invalid.');
  }
  return Object.freeze({
    bucket,
    startInstant: new Date(startMs).toISOString(),
    endInstant: new Date(endMs).toISOString(),
    portionMinutes,
    deductedMinutes,
    tieRule,
  });
}

function validatePriceComparison(comparison, sourceMode, calculation) {
  if (comparison == null) {
    if (sourceMode === 'NHSP_WEEKLY') {
      fail(
        'WEEKLY_SOURCE_NHSP_PRICE_COMPARISON_REQUIRED',
        'NHSP economic publication requires a passing source-price comparison.',
      );
    }
    return;
  }
  requireObject(comparison, 'WEEKLY_SOURCE_PRICE_COMPARISON_INVALID', 'Source-price comparison');
  if (comparison.accepted !== true || comparison.calculation !== calculation) {
    fail('WEEKLY_SOURCE_PRICE_COMPARISON_INVALID', 'The source-price comparison is not a passing canonical result.');
  }

  const comparisonPay = integerPence(
    comparison.canonicalPayPence,
    'WEEKLY_SOURCE_PRICE_COMPARISON_INVALID',
    'Compared candidate pay',
  );
  const comparisonCharge = integerPence(
    comparison.calculatedComparisonChargePence,
    'WEEKLY_SOURCE_PRICE_COMPARISON_INVALID',
    'Compared calculated charge',
  );
  if (
    comparisonPay !== integerPence(calculation.payPence, 'WEEKLY_SOURCE_ECONOMIC_TOTAL_INVALID', 'Canonical pay')
    || comparisonCharge !== integerPence(
      calculation.chargePence,
      'WEEKLY_SOURCE_ECONOMIC_TOTAL_INVALID',
      'Canonical charge',
    )
  ) {
    fail('WEEKLY_SOURCE_PRICE_COMPARISON_INVALID', 'The source-price comparison does not contain this calculation.');
  }

  if (sourceMode === 'NHSP_WEEKLY') {
    if (
      !PUBLISHABLE_NHSP_RESULTS.has(upper(comparison.result))
      || comparison.amountAuthority !== 'VALIDATED_SOURCE_PENCE'
      || comparison.invoicePresentationChargePence == null
    ) {
      fail('WEEKLY_SOURCE_PRICE_COMPARISON_INVALID', 'The NHSP source-price comparison is not publishable.');
    }
    integerPence(
      comparison.invoicePresentationChargePence,
      'WEEKLY_SOURCE_PRICE_COMPARISON_INVALID',
      'NHSP invoice presentation charge',
    );
    const needsAcceptance = ['MISMATCH', 'ZERO_SOURCE_CHARGE'].includes(upper(comparison.result));
    if ((comparison.requiresOfficeAcceptance === true) !== needsAcceptance) {
      fail('WEEKLY_SOURCE_PRICE_COMPARISON_INVALID', 'The NHSP source-price warning state is invalid.');
    }
    return;
  }

  if (
    upper(comparison.result) !== 'NOT_APPLICABLE'
    || comparison.amountAuthority !== 'CLOUDTMS_CALCULATION'
    || integerPence(
      comparison.invoicePresentationChargePence,
      'WEEKLY_SOURCE_PRICE_COMPARISON_INVALID',
      'Roster invoice presentation charge',
    ) !== comparisonCharge
  ) {
    fail('WEEKLY_SOURCE_PRICE_COMPARISON_INVALID', 'The roster source-price comparison is invalid.');
  }
}

/**
 * Build the only browser-to-projection economic payload for a resolved Weekly
 * source row. `resolvedFacts` must be assembled by server-owned Contract and
 * policy lookups; source files and browser requests are not rate authorities.
 *
 * A caller may supply the output of compareWeeklySourceShiftPrice as
 * `priceComparison`, or a direct canonicalWeeklyShiftFinancialSegment output
 * as `calculation`. NHSP always requires the former so pricing validation
 * cannot be bypassed. Configurable roster sources, including the no-receipt
 * whole-shift profile, remain HEALTHROSTER_WEEKLY at this boundary.
 */
export function buildWeeklySourceCanonicalEconomicSnapshot(input = {}) {
  const resolvedFacts = requireObject(
    input.resolvedFacts,
    'WEEKLY_SOURCE_ECONOMIC_FACTS_REQUIRED',
    'Server-resolved economic facts',
  );
  const sourceMode = upper(resolvedFacts.sourceMode);
  if (!SOURCE_MODES.has(sourceMode)) {
    fail('WEEKLY_SOURCE_MODE_INVALID', 'The server-resolved Weekly source mode is invalid.');
  }
  const rateMethod = upper(resolvedFacts.rateMethod);
  if (!RATE_METHODS.has(rateMethod)) {
    fail('WEEKLY_SOURCE_RATE_METHOD_INVALID', 'The server-resolved Weekly rate method is invalid.');
  }

  const hasComparison = input.priceComparison != null;
  const hasCalculation = input.calculation != null;
  if (hasComparison === hasCalculation) {
    fail(
      'WEEKLY_SOURCE_ECONOMIC_INPUT_CARDINALITY_INVALID',
      'Supply exactly one canonical calculation or source-price comparison.',
    );
  }
  const priceComparison = hasComparison
    ? requireObject(input.priceComparison, 'WEEKLY_SOURCE_PRICE_COMPARISON_INVALID', 'Source-price comparison')
    : null;
  const calculation = requireObject(
    priceComparison?.calculation ?? input.calculation,
    'WEEKLY_SOURCE_ECONOMIC_CALCULATION_REQUIRED',
    'Canonical Weekly shift calculation',
  );

  if (
    calculation.calculatorVersion !== WEEKLY_SHIFT_CALCULATOR_VERSION
    || upper(calculation.mode) !== rateMethod
  ) {
    fail('WEEKLY_SOURCE_ECONOMIC_CALCULATION_INVALID', 'The canonical calculation does not match resolved policy.');
  }

  const paidMinutes = wholeMinutes(
    calculation.paidMinutes,
    'WEEKLY_SOURCE_ECONOMIC_PAID_MINUTES_INVALID',
    'Paid time',
    { positive: true },
  );
  const breakMinutes = wholeMinutes(
    calculation.breakMinutes,
    'WEEKLY_SOURCE_ECONOMIC_BREAK_MINUTES_INVALID',
    'Break',
  );
  const { sign, bucketMinutes, hours } = canonicalBuckets(calculation);
  if (BUCKETS.reduce((sum, bucket) => sum + bucketMinutes[bucket], 0) !== paidMinutes) {
    fail('WEEKLY_SOURCE_ECONOMIC_MINUTES_MISMATCH', 'Canonical bucket minutes do not equal paid time.');
  }

  const payRates = strictRateTable(resolvedFacts.payRates, 'Pay');
  const chargeRates = strictRateTable(resolvedFacts.chargeRates, 'Charge');
  const payPence = integerPence(calculation.payPence, 'WEEKLY_SOURCE_ECONOMIC_TOTAL_INVALID', 'Canonical pay');
  const chargePence = integerPence(
    calculation.chargePence,
    'WEEKLY_SOURCE_ECONOMIC_TOTAL_INVALID',
    'Canonical charge',
  );
  if (
    payPence !== expectedPence(hours, payRates, sign)
    || chargePence !== expectedPence(hours, chargeRates, sign)
  ) {
    fail('WEEKLY_SOURCE_ECONOMIC_TOTAL_MISMATCH', 'Canonical totals do not match server-resolved rates.');
  }
  validatePriceComparison(priceComparison, sourceMode, calculation);

  const breakAllocation = validateBreakAllocation(calculation.breakAllocation, breakMinutes);
  const snapshot = {
    schema_version: WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_SCHEMA_VERSION,
    calculator_version: WEEKLY_SHIFT_CALCULATOR_VERSION,
    source_mode: sourceMode,
    rate_method: rateMethod,
    sign,
    paid_minutes: paidMinutes,
    break_minutes: breakMinutes,
    bucket_minutes: bucketMinutes,
    hours,
    pay_rates: payRates,
    charge_rates: chargeRates,
    total_pay_pence: payPence.toString(),
    calculated_charge_pence: chargePence.toString(),
  };
  if (breakAllocation) snapshot.break_allocation = breakAllocation;
  return Object.freeze(snapshot);
}
