import assert from 'node:assert/strict';
import test from 'node:test';

import {
  WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_SCHEMA_VERSION,
  buildWeeklySourceCanonicalEconomicSnapshot,
} from '../../../broker/src/weekly-source/economic-snapshot.js';
import { compareWeeklySourceShiftPrice } from '../../../broker/src/weekly-source/source-price-comparator.js';
import { canonicalWeeklyShiftFinancialSegment } from '../../../broker/src/weekly-source/weekly-rate-owner.js';

const payRates = Object.freeze({ day: 10, night: 11, sat: 12, sun: 13, bh: 14 });
const chargeRates = Object.freeze({ day: 20, night: 21, sat: 22, sun: 23, bh: 24 });
const splitInput = Object.freeze({
  mode: 'SPLIT_RATE_WINDOWS',
  bucketMinutes: { day: 450, night: 0, sat: 0, sun: 0, bh: 0 },
  breakMinutes: 30,
  payRates,
  chargeRates,
});

function facts(overrides = {}) {
  return {
    sourceMode: 'HEALTHROSTER_WEEKLY',
    rateMethod: 'SPLIT_RATE_WINDOWS',
    payRates,
    chargeRates,
    ...overrides,
  };
}

test('builds the exact SQL economic_snapshot contract from a passing NHSP comparison', () => {
  const priceComparison = compareWeeklySourceShiftPrice({
    sourceMode: 'NHSP_WEEKLY',
    sourceChargePence: '15000',
    financialInput: splitInput,
  });
  const snapshot = buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison,
    resolvedFacts: facts({ sourceMode: 'NHSP_WEEKLY' }),
  });
  assert.deepEqual(snapshot, {
    schema_version: WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_SCHEMA_VERSION,
    calculator_version: 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
    source_mode: 'NHSP_WEEKLY',
    rate_method: 'SPLIT_RATE_WINDOWS',
    sign: 1,
    paid_minutes: 450,
    break_minutes: 30,
    bucket_minutes: { day: 450, night: 0, sat: 0, sun: 0, bh: 0 },
    hours: { day: 7.5, night: 0, sat: 0, sun: 0, bh: 0 },
    pay_rates: payRates,
    charge_rates: chargeRates,
    total_pay_pence: '7500',
    calculated_charge_pence: '15000',
  });
  assert.deepEqual(Object.keys(snapshot), [
    'schema_version', 'calculator_version', 'source_mode', 'rate_method', 'sign',
    'paid_minutes', 'break_minutes', 'bucket_minutes', 'hours', 'pay_rates',
    'charge_rates', 'total_pay_pence', 'calculated_charge_pence',
  ]);
});

test('NHSP source rounding remains invoice evidence and never replaces the calculated snapshot charge', () => {
  const priceComparison = compareWeeklySourceShiftPrice({
    sourceMode: 'NHSP_WEEKLY',
    sourceChargePence: '15001',
    financialInput: splitInput,
  });
  const snapshot = buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison,
    resolvedFacts: facts({ sourceMode: 'NHSP_WEEKLY' }),
  });
  assert.equal(priceComparison.invoicePresentationChargePence, '15001');
  assert.equal(snapshot.calculated_charge_pence, '15000');
  assert.equal('invoice_presentation_charge_pence' in snapshot, false);
});

test('a physical NHSP reversal preserves signed hours and totals without negative zero values', () => {
  const priceComparison = compareWeeklySourceShiftPrice({
    sourceMode: 'NHSP_WEEKLY',
    sourceChargePence: '-15000',
    financialInput: { ...splitInput, sign: -1 },
  });
  const snapshot = buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison,
    resolvedFacts: facts({ sourceMode: 'NHSP_WEEKLY' }),
  });
  assert.equal(snapshot.sign, -1);
  assert.deepEqual(snapshot.hours, { day: -7.5, night: 0, sat: 0, sun: 0, bh: 0 });
  assert.equal(snapshot.total_pay_pence, '-7500');
  assert.equal(snapshot.calculated_charge_pence, '-15000');
  assert.equal(Object.values(snapshot.hours).some((value) => Object.is(value, -0)), false);
});

test('configurable no-receipt whole-shift roster remains HEALTHROSTER_WEEKLY with no Magnit identity', () => {
  const priceComparison = compareWeeklySourceShiftPrice({
    sourceMode: 'HEALTHROSTER_WEEKLY',
    financialInput: {
      mode: 'WHOLE_SHIFT_START_DAY',
      startInstant: '2026-09-07T08:00:00.000Z',
      endInstant: '2026-09-07T16:00:00.000Z',
      timeZone: 'Europe/London',
      breakEvidence: { durationMinutes: 30 },
      bankHolidayDates: [],
      payRates,
      chargeRates,
    },
  });
  const snapshot = buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison,
    resolvedFacts: facts({
      sourceMode: 'HEALTHROSTER_WEEKLY',
      rateMethod: 'WHOLE_SHIFT_START_DAY',
      sourceProfile: 'CONFIGURABLE_NO_RECEIPT_WHOLE_SHIFT',
    }),
  });
  assert.equal(snapshot.source_mode, 'HEALTHROSTER_WEEKLY');
  assert.equal(snapshot.rate_method, 'WHOLE_SHIFT_START_DAY');
  assert.equal(JSON.stringify(snapshot).includes('MAGNIT'), false);
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison,
    resolvedFacts: facts({ sourceMode: 'MAGNIT_WEEKLY', rateMethod: 'WHOLE_SHIFT_START_DAY' }),
  }), { code: 'WEEKLY_SOURCE_MODE_INVALID' });
});

test('direct canonical calculation is accepted for roster but cannot bypass NHSP price validation', () => {
  const calculation = canonicalWeeklyShiftFinancialSegment({ ...splitInput, strictRates: true });
  const snapshot = buildWeeklySourceCanonicalEconomicSnapshot({
    calculation,
    resolvedFacts: facts(),
  });
  assert.equal(snapshot.source_mode, 'HEALTHROSTER_WEEKLY');
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    calculation,
    resolvedFacts: facts({ sourceMode: 'NHSP_WEEKLY' }),
  }), { code: 'WEEKLY_SOURCE_NHSP_PRICE_COMPARISON_REQUIRED' });
});

test('a price warning publishes canonical economics but a false warning state fails closed', () => {
  const mismatch = compareWeeklySourceShiftPrice({
    sourceMode: 'NHSP_WEEKLY',
    sourceChargePence: '14990',
    financialInput: splitInput,
  });
  const warningSnapshot = buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison: mismatch,
    resolvedFacts: facts({ sourceMode: 'NHSP_WEEKLY' }),
  });
  assert.equal(warningSnapshot.calculated_charge_pence, '15000');
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison: { ...mismatch, requiresOfficeAcceptance: false },
    resolvedFacts: facts({ sourceMode: 'NHSP_WEEKLY' }),
  }), { code: 'WEEKLY_SOURCE_PRICE_COMPARISON_INVALID' });
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    priceComparison: compareWeeklySourceShiftPrice({
      sourceMode: 'HEALTHROSTER_WEEKLY',
      financialInput: splitInput,
    }),
    calculation: canonicalWeeklyShiftFinancialSegment({ ...splitInput, strictRates: true }),
    resolvedFacts: facts(),
  }), { code: 'WEEKLY_SOURCE_ECONOMIC_INPUT_CARDINALITY_INVALID' });
});

test('resolved rate and method drift are rejected before projection', () => {
  const calculation = canonicalWeeklyShiftFinancialSegment({ ...splitInput, strictRates: true });
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    calculation,
    resolvedFacts: facts({ chargeRates: { ...chargeRates, day: 20.01 } }),
  }), { code: 'WEEKLY_SOURCE_ECONOMIC_TOTAL_MISMATCH' });
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    calculation,
    resolvedFacts: facts({ payRates: { ...payRates, bh: 0 } }),
  }), { code: 'WEEKLY_SOURCE_ECONOMIC_RATE_INVALID' });
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    calculation,
    resolvedFacts: facts({ rateMethod: 'WHOLE_SHIFT_START_DAY' }),
  }), { code: 'WEEKLY_SOURCE_ECONOMIC_CALCULATION_INVALID' });
});

test('tampered canonical hours, totals and paid-minute cardinality are rejected', () => {
  const calculation = canonicalWeeklyShiftFinancialSegment({ ...splitInput, strictRates: true });
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    calculation: { ...calculation, hours: { ...calculation.hours, day: 7.49 } },
    resolvedFacts: facts(),
  }), { code: 'WEEKLY_SOURCE_ECONOMIC_HOURS_MISMATCH' });
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    calculation: { ...calculation, payPence: '7501' },
    resolvedFacts: facts(),
  }), { code: 'WEEKLY_SOURCE_ECONOMIC_TOTAL_MISMATCH' });
  assert.throws(() => buildWeeklySourceCanonicalEconomicSnapshot({
    calculation: { ...calculation, paidMinutes: 451 },
    resolvedFacts: facts(),
  }), { code: 'WEEKLY_SOURCE_ECONOMIC_MINUTES_MISMATCH' });
});

test('duration-only split allocation is included only when the canonical calculator supplies it', () => {
  const calculation = canonicalWeeklyShiftFinancialSegment({
    mode: 'SPLIT_RATE_WINDOWS',
    ratePortions: [
      { bucket: 'night', startInstant: '2026-09-07T04:00:00Z', endInstant: '2026-09-07T05:00:00Z' },
      { bucket: 'day', startInstant: '2026-09-07T05:00:00Z', endInstant: '2026-09-07T12:00:00Z' },
    ],
    breakMinutes: 30,
    durationBreakTieRule: 'EARLIEST_LONGEST_PORTION',
    payRates,
    chargeRates,
    strictRates: true,
  });
  const snapshot = buildWeeklySourceCanonicalEconomicSnapshot({ calculation, resolvedFacts: facts() });
  assert.deepEqual(snapshot.break_allocation, {
    bucket: 'day',
    startInstant: '2026-09-07T05:00:00.000Z',
    endInstant: '2026-09-07T12:00:00.000Z',
    portionMinutes: 420,
    deductedMinutes: 30,
    tieRule: 'EARLIEST_LONGEST_PORTION',
  });

  const withoutAllocation = buildWeeklySourceCanonicalEconomicSnapshot({
    calculation: canonicalWeeklyShiftFinancialSegment({ ...splitInput, strictRates: true }),
    resolvedFacts: facts(),
  });
  assert.equal('break_allocation' in withoutAllocation, false);
});
