import assert from 'node:assert/strict';
import test from 'node:test';

import { compareWeeklySourceShiftPrice } from '../../../broker/src/weekly-source/source-price-comparator.js';

const financialInput = {
  mode: 'SPLIT_RATE_WINDOWS',
  bucketMinutes: { day: 450, night: 0, sat: 0, sun: 0, bh: 0 },
  payRates: { day: 10, night: 11, sat: 12, sun: 13, bh: 14 },
  chargeRates: { day: 20, night: 21, sat: 22, sun: 23, bh: 24 },
};

const negativeFinancialInput = { ...financialInput, sign: -1 };

const nhsp = (sourceChargePence, input = financialInput) => compareWeeklySourceShiftPrice({
  sourceMode: 'NHSP_WEEKLY',
  sourceChargePence,
  financialInput: input,
});

test('NHSP exact charge freezes source pence for invoice and keeps calculated pay', () => {
  const result = nhsp('15000');
  assert.equal(result.accepted, true);
  assert.equal(result.result, 'EXACT');
  assert.equal(result.invoicePresentationChargePence, '15000');
  assert.equal(result.canonicalPayPence, '7500');
  assert.equal(result.amountAuthority, 'VALIDATED_SOURCE_PENCE');
  assert.equal(result.signedDifferencePence, '0');
});

test('NHSP exact full-negative charge freezes the signed source pence', () => {
  const result = nhsp('-15000', negativeFinancialInput);
  assert.equal(result.accepted, true);
  assert.equal(result.result, 'EXACT');
  assert.equal(result.invoicePresentationChargePence, '-15000');
  assert.equal(result.canonicalPayPence, '-7500');
  assert.equal(result.signedDifferencePence, '0');
});

// Plan 6.2 `25 §6`, `24 §13`, `14 §4.3` items 1 and 5, NHSP-BR-013, PRC-006,
// PRC-007, PRC-020 and NHSBR-019: the one-penny profile is symmetric under the
// same non-zero sign. The Plan 6 directional band is superseded.
test('NHSP accepts a same-sign one-penny difference in either arithmetic direction', () => {
  const positiveAbove = nhsp('15001');
  assert.equal(positiveAbove.accepted, true);
  assert.equal(positiveAbove.result, 'SOURCE_ROUNDING_EQUIVALENT');
  assert.equal(positiveAbove.invoicePresentationChargePence, '15001');
  assert.equal(positiveAbove.signedDifferencePence, '1');
  assert.equal(positiveAbove.canonicalPayPence, '7500');

  const positiveBelow = nhsp('14999');
  assert.equal(positiveBelow.accepted, true);
  assert.equal(positiveBelow.result, 'SOURCE_ROUNDING_EQUIVALENT');
  assert.equal(positiveBelow.invoicePresentationChargePence, '14999');
  assert.equal(positiveBelow.signedDifferencePence, '-1');
  assert.equal(positiveBelow.canonicalPayPence, '7500');

  const negativeMore = nhsp('-15001', negativeFinancialInput);
  assert.equal(negativeMore.accepted, true);
  assert.equal(negativeMore.result, 'SOURCE_ROUNDING_EQUIVALENT');
  assert.equal(negativeMore.invoicePresentationChargePence, '-15001');
  assert.equal(negativeMore.signedDifferencePence, '-1');
  assert.equal(negativeMore.canonicalPayPence, '-7500');

  const negativeLess = nhsp('-14999', negativeFinancialInput);
  assert.equal(negativeLess.accepted, true);
  assert.equal(negativeLess.result, 'SOURCE_ROUNDING_EQUIVALENT');
  assert.equal(negativeLess.invoicePresentationChargePence, '-14999');
  assert.equal(negativeLess.signedDifferencePence, '1');
  assert.equal(negativeLess.canonicalPayPence, '-7500');
});

test('Candidate pay never inherits the accepted invoice penny', () => {
  for (const [source, input] of [
    ['15001', financialInput],
    ['14999', financialInput],
    ['-15001', negativeFinancialInput],
    ['-14999', negativeFinancialInput],
  ]) {
    const result = nhsp(source, input);
    assert.equal(result.calculatedComparisonChargePence, input === financialInput ? '15000' : '-15000');
    assert.equal(result.canonicalPayPence, input === financialInput ? '7500' : '-7500');
    assert.notEqual(result.invoicePresentationChargePence, result.calculatedComparisonChargePence);
  }
});

test('a genuine disparity is a bound Office-warning candidate, not a Contract or pay blocker', () => {
  for (const [source, input] of [
    ['15002', financialInput],
    ['14998', financialInput],
    ['-15002', negativeFinancialInput],
    ['-14998', negativeFinancialInput],
  ]) {
    const result = nhsp(source, input);
    assert.equal(result.accepted, true, `${source} must be safe for warning review`);
    assert.equal(result.result, 'MISMATCH');
    assert.equal(result.requiresOfficeAcceptance, true);
    assert.equal(result.acceptanceKind, 'ACCEPTED_DISPARITY');
    assert.equal(result.blockerCode, null);
    assert.equal(result.invoicePresentationChargePence, source);
    assert.equal(result.amountAuthority, 'VALIDATED_SOURCE_PENCE');
  }
});

test('a conflicting sign is unsafe and cannot be accepted by Office', () => {
  assert.throws(() => nhsp('-15000'), { code: 'NHSP_SOURCE_CHARGE_SIGN_INVALID' });
  assert.throws(() => nhsp('15000', negativeFinancialInput), { code: 'NHSP_SOURCE_CHARGE_SIGN_INVALID' });
});

test('a zero source is classified separately for the NHSP rate-card warning', () => {
  const zeroCalculation = () => compareWeeklySourceShiftPrice({
    sourceMode: 'NHSP_WEEKLY',
    sourceChargePence: '-1',
    financialInput: {
      ...financialInput,
      bucketMinutes: { day: 0, night: 0, sat: 0, sun: 0, bh: 0 },
    },
  });
  assert.throws(zeroCalculation, { code: 'NHSP_CALCULATED_CHARGE_ZERO_INVALID' });

  const zeroCalculationPositiveSource = () => compareWeeklySourceShiftPrice({
    sourceMode: 'NHSP_WEEKLY',
    sourceChargePence: '1',
    financialInput: {
      ...financialInput,
      bucketMinutes: { day: 0, night: 0, sat: 0, sun: 0, bh: 0 },
    },
  });
  assert.throws(zeroCalculationPositiveSource, { code: 'NHSP_CALCULATED_CHARGE_ZERO_INVALID' });

  const zeroSource = nhsp('0');
  assert.equal(zeroSource.accepted, true);
  assert.equal(zeroSource.result, 'ZERO_SOURCE_CHARGE');
  assert.equal(zeroSource.requiresOfficeAcceptance, true);
  assert.equal(zeroSource.acceptanceKind, 'ACCEPTED_ZERO');
  assert.equal(zeroSource.invoicePresentationChargePence, '0');
});

test('NHSP disparity preserves exact invoice authority without altering calculated candidate pay', () => {
  const result = nhsp('14990');
  assert.equal(result.accepted, true);
  assert.equal(result.requiresOfficeAcceptance, true);
  assert.equal(result.blockerCode, null);
  assert.equal(result.invoicePresentationChargePence, '14990');
  assert.equal(result.canonicalPayPence, '7500');
});

test('generic roster remains HEALTHROSTER_WEEKLY and uses CloudTMS calculated charge', () => {
  const result = compareWeeklySourceShiftPrice({ sourceMode: 'HEALTHROSTER_WEEKLY', financialInput });
  assert.equal(result.accepted, true);
  assert.equal(result.result, 'NOT_APPLICABLE');
  assert.equal(result.invoicePresentationChargePence, '15000');
  assert.equal(result.amountAuthority, 'CLOUDTMS_CALCULATION');
  assert.equal(result.sourceValidationChargePence, null);
});

test('source comparator refuses missing or zero applicable rates', () => {
  assert.throws(() => compareWeeklySourceShiftPrice({
    sourceMode: 'HEALTHROSTER_WEEKLY',
    financialInput: { ...financialInput, chargeRates: { ...financialInput.chargeRates, day: 0 } },
  }), { code: 'WEEKLY_SHIFT_RATE_INVALID' });
});
