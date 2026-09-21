import assert from 'node:assert/strict';
import test from 'node:test';

import {
  WEEKLY_SHIFT_CALCULATOR_VERSION,
  canonicalWeeklyShiftFinancialSegment,
} from '../../../broker/src/weekly-source/weekly-rate-owner.js';

const payRates = { day: 10, night: 11, sat: 12, sun: 13, bh: 14 };
const chargeRates = { day: 20, night: 21, sat: 22, sun: 23, bh: 24 };

function whole(overrides = {}) {
  return canonicalWeeklyShiftFinancialSegment({
    mode: 'WHOLE_SHIFT_START_DAY',
    startInstant: '2026-09-07T08:00:00.000Z',
    endInstant: '2026-09-07T16:00:00.000Z',
    timeZone: 'Europe/London',
    breakEvidence: { durationMinutes: 30 },
    bankHolidayDates: [],
    payRates,
    chargeRates,
    ...overrides,
  });
}

test('split mode preserves the existing per-bucket rounding order', () => {
  const value = canonicalWeeklyShiftFinancialSegment({
    mode: 'SPLIT_RATE_WINDOWS',
    bucketMinutes: { day: 451, night: 29, sat: 0, sun: 0, bh: 0 },
    breakMinutes: 30,
    payRates,
    chargeRates,
  });
  assert.deepEqual(value.hours, { day: 7.52, night: 0.48, sat: 0, sun: 0, bh: 0 });
  assert.equal(value.payAmount, 80.48);
  assert.equal(value.chargeAmount, 160.48);
  assert.equal(value.calculatorVersion, WEEKLY_SHIFT_CALCULATOR_VERSION);
});

test('whole-shift precedence is bank holiday, Sunday, Saturday, crossed weekday night, same-day weekday day', () => {
  assert.equal(whole({ bankHolidayDates: ['2026-09-07'] }).category, 'bh');
  assert.equal(whole({ startInstant: '2026-09-06T08:00:00Z', endInstant: '2026-09-06T16:00:00Z' }).category, 'sun');
  assert.equal(whole({ startInstant: '2026-09-05T08:00:00Z', endInstant: '2026-09-05T16:00:00Z' }).category, 'sat');
  assert.equal(whole({ startInstant: '2026-09-07T21:00:00Z', endInstant: '2026-09-08T05:00:00Z' }).category, 'night');
  assert.equal(whole().category, 'day');
});

test('whole-shift deducts exact and duration-only breaks once and returns identical amounts', () => {
  const duration = whole();
  const exact = whole({
    breakEvidence: {
      exactIntervals: [{ startInstant: '2026-09-07T12:00:00Z', endInstant: '2026-09-07T12:30:00Z' }],
    },
  });
  assert.equal(duration.paidMinutes, 450);
  assert.equal(exact.paidMinutes, 450);
  assert.equal(duration.payPence, exact.payPence);
  assert.equal(duration.chargePence, exact.chargePence);
});

test('whole-shift supports source paid-minutes evidence without inventing a break position', () => {
  const value = whole({ breakEvidence: { paidMinutes: 450 } });
  assert.equal(value.breakMinutes, 30);
  assert.equal(value.paidMinutes, 450);
  assert.deepEqual(value.bucketMinutes, { day: 450, night: 0, sat: 0, sun: 0, bh: 0 });
});

test('negative physical movement keeps the category and reverses hours and money', () => {
  const value = whole({ sign: -1 });
  assert.equal(value.category, 'day');
  assert.equal(value.hours.day, -7.5);
  assert.equal(value.payPence, '-7500');
  assert.equal(value.chargePence, '-15000');
});

test('DST uses real elapsed minutes and refuses invalid intervals and breaks', () => {
  const spring = whole({
    startInstant: '2026-03-29T00:00:00Z',
    endInstant: '2026-03-29T03:00:00Z',
    breakEvidence: { durationMinutes: 0 },
  });
  assert.equal(spring.category, 'sun');
  assert.equal(spring.paidMinutes, 180);
  assert.throws(() => whole({ endInstant: '2026-09-07T07:00:00Z' }), { code: 'WEEKLY_SHIFT_INTERVAL_INVALID' });
  assert.throws(() => whole({ breakEvidence: { durationMinutes: 481 } }), { code: 'WEEKLY_SHIFT_BREAK_INVALID' });
  assert.throws(() => whole({
    breakEvidence: { exactIntervals: [
      { startInstant: '2026-09-07T10:00:00Z', endInstant: '2026-09-07T11:00:00Z' },
      { startInstant: '2026-09-07T10:30:00Z', endInstant: '2026-09-07T11:30:00Z' },
    ] },
  }), { code: 'WEEKLY_SHIFT_BREAKS_OVERLAP' });
});

test('whole-shift fails closed on unavailable or zero rates while ordinary split compatibility may preserve zero', () => {
  assert.throws(() => whole({ chargeRates: { ...chargeRates, day: 0 } }), { code: 'WEEKLY_SHIFT_RATE_INVALID' });
  const split = canonicalWeeklyShiftFinancialSegment({
    mode: 'SPLIT_RATE_WINDOWS',
    bucketMinutes: { day: 60 },
    payRates,
    chargeRates: { ...chargeRates, day: 0 },
  });
  assert.equal(split.chargePence, '0');
});

test('ordinary split extraction preserves missing-rate coercion while strict source comparison refuses it', () => {
  const compatible = canonicalWeeklyShiftFinancialSegment({
    mode: 'SPLIT_RATE_WINDOWS',
    bucketMinutes: { day: 60 },
    payRates: { day: 10 },
    chargeRates: { day: 20 },
  });
  assert.equal(compatible.payPence, '1000');
  assert.equal(compatible.chargePence, '2000');
  assert.throws(() => canonicalWeeklyShiftFinancialSegment({
    mode: 'SPLIT_RATE_WINDOWS',
    bucketMinutes: { day: 60 },
    payRates: { day: 10 },
    chargeRates: { day: 20 },
    strictRates: true,
  }), { code: 'WEEKLY_SHIFT_RATES_UNAVAILABLE' });
});

test('split duration-only break is deducted wholly from the longest contiguous rate portion', () => {
  const value = canonicalWeeklyShiftFinancialSegment({
    mode: 'SPLIT_RATE_WINDOWS',
    ratePortions: [
      { bucket: 'night', startInstant: '2026-09-07T04:00:00Z', endInstant: '2026-09-07T05:00:00Z' },
      { bucket: 'day', startInstant: '2026-09-07T05:00:00Z', endInstant: '2026-09-07T12:00:00Z' },
    ],
    breakMinutes: 30,
    durationBreakTieRule: 'EARLIEST_LONGEST_PORTION',
    payRates,
    chargeRates,
  });
  assert.deepEqual(value.bucketMinutes, { day: 390, night: 60, sat: 0, sun: 0, bh: 0 });
  assert.equal(value.breakAllocation.bucket, 'day');
  assert.equal(value.breakAllocation.deductedMinutes, 30);
});

test('split duration-only break tie follows the explicit first or last policy without hidden bucket precedence', () => {
  const input = {
    mode: 'SPLIT_RATE_WINDOWS',
    ratePortions: [
      { bucket: 'night', startInstant: '2026-09-07T05:00:00Z', endInstant: '2026-09-07T06:00:00Z' },
      { bucket: 'day', startInstant: '2026-09-07T06:00:00Z', endInstant: '2026-09-07T07:00:00Z' },
    ],
    breakMinutes: 30,
    payRates,
    chargeRates,
  };
  const first = canonicalWeeklyShiftFinancialSegment({ ...input, durationBreakTieRule: 'EARLIEST_LONGEST_PORTION' });
  const last = canonicalWeeklyShiftFinancialSegment({ ...input, durationBreakTieRule: 'LATEST_LONGEST_PORTION' });
  assert.deepEqual(first.bucketMinutes, { day: 60, night: 30, sat: 0, sun: 0, bh: 0 });
  assert.deepEqual(last.bucketMinutes, { day: 30, night: 60, sat: 0, sun: 0, bh: 0 });
  assert.equal(first.breakAllocation.bucket, 'night');
  assert.equal(last.breakAllocation.bucket, 'day');
});

test('split duration-only break never spills beyond its chosen longest contiguous portion', () => {
  assert.throws(() => canonicalWeeklyShiftFinancialSegment({
    mode: 'SPLIT_RATE_WINDOWS',
    ratePortions: [
      { bucket: 'night', startInstant: '2026-09-07T05:00:00Z', endInstant: '2026-09-07T05:20:00Z' },
      { bucket: 'day', startInstant: '2026-09-07T05:20:00Z', endInstant: '2026-09-07T05:40:00Z' },
    ],
    breakMinutes: 30,
    durationBreakTieRule: 'EARLIEST_LONGEST_PORTION',
    payRates,
    chargeRates,
  }), { code: 'WEEKLY_SHIFT_BREAK_EXCEEDS_LONGEST_PORTION' });
});

test('split extraction is byte-equivalent to the established writer formula across deterministic cases', () => {
  let seed = 0x5eeda11;
  const random = () => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return seed / 0x100000000;
  };
  const establishedNumber = (value) => (value == null || Number.isNaN(Number(value))) ? 0 : Number(value);
  const establishedRound2 = (value) => Math.round((Number(value) || 0) * 100) / 100;
  for (let index = 0; index < 2000; index += 1) {
    const bucketMinutes = Object.fromEntries(['day', 'night', 'sat', 'sun', 'bh'].map((bucket) => [bucket, Math.floor(random() * 721)]));
    const pay = Object.fromEntries(['day', 'night', 'sat', 'sun', 'bh'].map((bucket) => [bucket, index % 19 === 0 ? undefined : (random() * 99).toFixed(4)]));
    const charge = Object.fromEntries(['day', 'night', 'sat', 'sun', 'bh'].map((bucket) => [bucket, index % 23 === 0 ? null : (random() * 199).toFixed(4)]));
    const sign = index % 7 === 0 ? -1 : 1;
    const rawHours = Object.fromEntries(Object.entries(bucketMinutes).map(([bucket, minutes]) => [bucket, +(establishedNumber(minutes) / 60).toFixed(2)]));
    const expectedPay = establishedRound2(establishedRound2(
      rawHours.day * establishedNumber(pay.day)
      + rawHours.night * establishedNumber(pay.night)
      + rawHours.sat * establishedNumber(pay.sat)
      + rawHours.sun * establishedNumber(pay.sun)
      + rawHours.bh * establishedNumber(pay.bh)
    ) * sign);
    const expectedCharge = establishedRound2(establishedRound2(
      rawHours.day * establishedNumber(charge.day)
      + rawHours.night * establishedNumber(charge.night)
      + rawHours.sat * establishedNumber(charge.sat)
      + rawHours.sun * establishedNumber(charge.sun)
      + rawHours.bh * establishedNumber(charge.bh)
    ) * sign);
    const actual = canonicalWeeklyShiftFinancialSegment({ mode: 'SPLIT_RATE_WINDOWS', bucketMinutes, payRates: pay, chargeRates: charge, sign });
    assert.equal(actual.payAmount, expectedPay, `pay case ${index}`);
    assert.equal(actual.chargeAmount, expectedCharge, `charge case ${index}`);
    assert.deepEqual(actual.hours, Object.fromEntries(Object.entries(rawHours).map(([bucket, hours]) => [bucket, establishedRound2(hours * sign)])), `hours case ${index}`);
  }
});
