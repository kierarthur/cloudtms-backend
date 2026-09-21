import { canonicalWeeklyShiftFinancialSegment } from './weekly-rate-owner.js';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function integerPence(value, code, label) {
  const text = String(value ?? '').trim();
  if (!/^-?\d+$/.test(text)) fail(code, `${label} is unavailable.`);
  const parsed = BigInt(text);
  const limit = 999999999999n;
  if (parsed < -limit || parsed > limit) fail(code, `${label} is outside the supported range.`);
  return parsed;
}

/**
 * The one Weekly source-price decision. Candidate pay always remains the
 * canonical CloudTMS calculation. NHSP invoice presentation may use the exact
 * signed source pence after this independent comparison is either an ordinary
 * pass or an explicitly accepted Office warning.  This function never records
 * that Office decision; it only classifies the current immutable facts.
 */
export function compareWeeklySourceShiftPrice(input = {}) {
  const sourceMode = String(input.sourceMode ?? '').trim().toUpperCase();
  if (!['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY'].includes(sourceMode)) {
    fail('WEEKLY_SOURCE_MODE_INVALID', 'The weekly source mode is invalid.');
  }

  const calculation = canonicalWeeklyShiftFinancialSegment({
    ...(input.financialInput ?? {}),
    strictRates: true,
  });
  const calculated = integerPence(calculation.chargePence, 'WEEKLY_CALCULATED_CHARGE_INVALID', 'Calculated charge');

  if (sourceMode !== 'NHSP_WEEKLY') {
    return Object.freeze({
      accepted: true,
      result: 'NOT_APPLICABLE',
      blockerCode: null,
      calculatedComparisonChargePence: calculated.toString(),
      sourceValidationChargePence: null,
      invoicePresentationChargePence: calculated.toString(),
      amountAuthority: 'CLOUDTMS_CALCULATION',
      canonicalPayPence: calculation.payPence,
      calculation,
    });
  }

  const source = integerPence(input.sourceChargePence, 'NHSP_SOURCE_CHARGE_INVALID', 'NHSP source charge');
  if (calculated === 0n && source !== 0n) {
    fail('NHSP_CALCULATED_CHARGE_ZERO_INVALID', 'The calculated NHSP charge is zero for a non-zero source charge.');
  }
  if ((source > 0n && calculated < 0n) || (source < 0n && calculated > 0n)) {
    fail('NHSP_SOURCE_CHARGE_SIGN_INVALID', 'The NHSP source charge and calculated charge have conflicting signs.');
  }
  const difference = source - calculated;
  // Plan 6.2 supersedes the Plan 6 directional band. The sealed
  // NHSP_TWO_COMPONENT_PENCE_V1 exception is symmetric: the source and the
  // calculated pence must carry the same non-zero sign, and the absolute
  // difference may be exactly one penny in either arithmetic direction.
  // No directional constraint is permitted.
  // Authority: pack 25 §6; 24 §13; 14 §4.3 items 1 and 5; NHSP-BR-013;
  // acceptance PRC-006, PRC-007, PRC-020, NHSBR-019, NHSBR-020.
  // A difference greater than one penny remains a known disparity. Conflicting
  // signs and a non-zero source beside a zero calculation are unsafe facts and
  // fail above; they are never converted into an Office-acceptance warning.
  const sameNonZeroSign = (source > 0n && calculated > 0n)
    || (source < 0n && calculated < 0n);
  // Gate 13 finding F7. `difference === 0n` alone also holds for source 0 and
  // calculated 0, so an all-zero row returned EXACT with amountAuthority
  // VALIDATED_SOURCE_PENCE, while the server re-derives MISMATCH for a zero on
  // either side (`weekly_source_projection_build_v1.sql`: "when
  // v_charge_source_pence=0 or v_charge_calculated_pence=0 then 'MISMATCH'").
  // The server then refused the caller's whole verdict with
  // WEEKLY_SOURCE_QUALIFICATION_RESULT_NOT_REDERIVED (22023) instead of
  // blocking by the name the Office expects. The non-zero-sign test is
  // therefore part of EXACT as well, exactly as `14 §4.3.5` says: "an all-zero
  // row … block".
  const zeroSource = source === 0n && calculated !== 0n;
  const exact = sameNonZeroSign && difference === 0n;
  const onePennyEquivalent = !exact
    && sameNonZeroSign
    && (difference === 1n || difference === -1n);
  if (!exact && !onePennyEquivalent) {
    const result = zeroSource ? 'ZERO_SOURCE_CHARGE' : 'MISMATCH';
    return Object.freeze({
      // The canonical calculation and source pence are both safe to publish to
      // the server-owned preview.  Finalisation still requires the separately
      // persisted, fingerprint-bound Office acceptance.
      accepted: true,
      result,
      requiresOfficeAcceptance: true,
      acceptanceKind: zeroSource ? 'ACCEPTED_ZERO' : 'ACCEPTED_DISPARITY',
      blockerCode: null,
      calculatedComparisonChargePence: calculated.toString(),
      sourceValidationChargePence: source.toString(),
      invoicePresentationChargePence: source.toString(),
      amountAuthority: 'VALIDATED_SOURCE_PENCE',
      canonicalPayPence: calculation.payPence,
      signedDifferencePence: difference.toString(),
      calculation,
    });
  }

  return Object.freeze({
    accepted: true,
    result: exact ? 'EXACT' : 'SOURCE_ROUNDING_EQUIVALENT',
    requiresOfficeAcceptance: false,
    acceptanceKind: null,
    blockerCode: null,
    calculatedComparisonChargePence: calculated.toString(),
    sourceValidationChargePence: source.toString(),
    invoicePresentationChargePence: source.toString(),
    amountAuthority: 'VALIDATED_SOURCE_PENCE',
    canonicalPayPence: calculation.payPence,
    signedDifferencePence: difference.toString(),
    calculation,
  });
}
