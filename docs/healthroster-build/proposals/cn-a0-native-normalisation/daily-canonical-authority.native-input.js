/*
 * Canonical DAILY financial snapshot authority.
 *
 * Every DAILY route supplies canonical CloudTMS context and rate rows to this
 * one calculation body. Callers may authenticate, normalise request shapes,
 * queue work and format responses; they must not reproduce these economics.
 */

const round2 = (value) => Math.round((Number(value) || 0) * 100) / 100;
const numberOrZero = (value) => (value == null ? 0 : Number(value) || 0);
const boolish = (value) => {
  if (value === true) return true;
  if (value === false || value == null) return false;
  return ['true', '1', 'yes', 'y', 'on'].includes(String(value).trim().toLowerCase());
};

const missingUsedRates = (hours, pay, charge) => {
  const used = (value) => Number(value || 0) > 0;
  return (
    (used(hours.hours_day) && (pay.day == null || charge.day == null)) ||
    (used(hours.hours_night) && (pay.night == null || charge.night == null)) ||
    (used(hours.hours_sat) && (pay.sat == null || charge.sat == null)) ||
    (used(hours.hours_sun) && (pay.sun == null || charge.sun == null)) ||
    (used(hours.hours_bh) && (pay.bh == null || charge.bh == null))
  );
};

export function deriveCanonicalDailyFinancialContext({
  effectiveTimesheetId,
  context,
  timesheetOverride = null,
  toLocalParts,
  parseCandidateRoleCodes,
  normaliseRole,
  normaliseBand
}) {
  const ctx = context || {};
  const timesheet = {
    ...(ctx.out_timesheet || {}),
    ...(timesheetOverride || {})
  };
  const candidate = ctx.out_candidate || null;
  const candidateId = candidate?.id || null;
  const candidateAssignment = candidateId ? 'ASSIGNED' : 'UNASSIGNED';
  const clientId = ctx.out_client_id || null;
  const workedDateYmd = timesheet?.worked_start_iso && typeof toLocalParts === 'function'
    ? (toLocalParts(timesheet.worked_start_iso, null)?.ymd || null)
    : null;
  const policy = ctx.out_policy || {};

  const candidateRoleCodes = typeof parseCandidateRoleCodes === 'function'
    ? parseCandidateRoleCodes(candidate?.roles)
    : [];
  const rawRole = String(timesheet?.job_title_norm || '').trim();
  const timesheetRole = rawRole && typeof normaliseRole === 'function'
    ? normaliseRole(rawRole)
    : (rawRole || null);
  const timesheetBand = typeof normaliseBand === 'function'
    ? normaliseBand(timesheet?.band)
    : (timesheet?.band ?? null);

  let roleForRates = null;
  if (candidateId) {
    roleForRates = timesheetRole && candidateRoleCodes.includes(timesheetRole)
      ? timesheetRole
      : null;
  } else {
    roleForRates = timesheetRole;
  }

  let rateType = 'UMBRELLA';
  const payMethod = String(candidate?.pay_method || '').toUpperCase();
  if (payMethod === 'PAYE' || payMethod === 'UMBRELLA') rateType = payMethod;

  const correctionKind = String(timesheet?.correction_kind || '').trim().toUpperCase();
  const adjustmentOrigin = String(timesheet?.adjustment_origin || '').trim().toUpperCase();
  const key = String(effectiveTimesheetId || timesheet?.timesheet_id || '').trim();
  if (!key) throw new Error('DAILY_CANONICAL_TIMESHEET_REQUIRED');

  return {
    key,
    timesheet,
    currentFinancials: ctx.out_cur_fin || null,
    effectiveFlags: ctx.out_effective_flags || null,
    policy,
    candidate,
    candidateId,
    candidateAssignment,
    clientId,
    workedDateYmd,
    roleForRates,
    bandForRates: timesheetBand || null,
    rateType,
    isCorrectionTimesheet: Boolean(correctionKind) ||
      adjustmentOrigin === 'IMPORT_CORRECTION' ||
      adjustmentOrigin === 'IMPORT_CANCELLATION',
    rateRequest: {
      k: key,
      candidate_id: candidateId,
      client_id: clientId,
      role: roleForRates,
      band: timesheetBand || null,
      date: workedDateYmd,
      rate_type: rateType
    }
  };
}

export function buildCanonicalDailyFinancialSnapshot({
  env,
  timesheet,
  currentFinancials = null,
  policy = {},
  candidate = null,
  umbrella = null,
  candidateId = null,
  candidateAssignment = null,
  clientId = null,
  role = null,
  band = null,
  rateType = null,
  ratesRow = null,
  effectiveFlags = null,
  preserveUnprocessed = false,
  subtractBreak,
  classifyMinutes,
  resolveEffectivePayChannel
}) {
  if (!timesheet?.timesheet_id) throw new Error('DAILY_CANONICAL_TIMESHEET_REQUIRED');
  if (typeof subtractBreak !== 'function' || typeof classifyMinutes !== 'function') {
    throw new Error('DAILY_CANONICAL_TIME_AUTHORITY_REQUIRED');
  }

  const rates = ratesRow || {};
  const pay = {
    day: rates.pay_day ?? null,
    night: rates.pay_night ?? null,
    sat: rates.pay_sat ?? null,
    sun: rates.pay_sun ?? null,
    bh: rates.pay_bh ?? null
  };
  const charge = {
    day: rates.charge_day ?? null,
    night: rates.charge_night ?? null,
    sat: rates.charge_sat ?? null,
    sun: rates.charge_sun ?? null,
    bh: rates.charge_bh ?? null
  };

  let segments = [];
  if (timesheet.worked_start_iso && timesheet.worked_end_iso) {
    segments.push([timesheet.worked_start_iso, timesheet.worked_end_iso]);
  }
  segments = subtractBreak(
    segments,
    timesheet.break_start_iso || null,
    timesheet.break_end_iso || null,
    timesheet.break_minutes ?? null
  );
  const hours = classifyMinutes(env, policy || {}, segments);
  const missingRates = missingUsedRates(hours, pay, charge);

  // Preserve the pre-extraction TSFIN worker rule exactly: candidate PAYE or
  // UMBRELLA wins; otherwise retain the factual timesheet pay_method value.
  // Do not introduce a current-TSFIN fallback because that could change ERNI
  // treatment merely because an older financial snapshot exists.
  const candidatePayMethod = String(candidate?.pay_method || '').toUpperCase();
  const payMethod = candidatePayMethod === 'PAYE'
    ? 'PAYE'
    : candidatePayMethod === 'UMBRELLA'
      ? 'UMBRELLA'
      : (timesheet.pay_method || null);
  let payChannelBad = false;
  try {
    if (candidateId && clientId && typeof resolveEffectivePayChannel === 'function') {
      const channel = resolveEffectivePayChannel({
        pay_method: payMethod || candidate?.pay_method || null,
        candidate,
        umbrella
      });
      payChannelBad = !channel || channel.ok === false;
    }
  } catch {
    payChannelBad = true;
  }

  const validationRequired = boolish(effectiveFlags?.hr_validation_required_for_invoice);
  const validationStatus = String(effectiveFlags?.validation_status || '').toUpperCase();
  const validationOk = validationStatus === 'VALIDATION_OK' || validationStatus === 'OVERRIDDEN';
  const mustHoldForHrValidation = validationRequired && !validationOk;

  const qrStatus = String(timesheet.qr_status || '').toUpperCase();
  const qrToken = String(timesheet.qr_token || '').trim();
  const hasQrIssuedProof = Boolean(
    (qrToken && timesheet.qr_generated_at) || String(timesheet.qr_last_sent_hash || '').trim()
  );
  const qrAwaitingSignature = qrStatus === 'PENDING' && hasQrIssuedProof && !timesheet.qr_scanned_at;
  const isAuthorised = Boolean(timesheet.authorised_at_server);

  let processingStatus;
  let hasRateIssue = false;
  let hasPayChannelIssue = false;
  if (!candidateId) {
    processingStatus = 'UNASSIGNED';
  } else if (!clientId) {
    processingStatus = 'CLIENT_UNRESOLVED';
  } else if (missingRates) {
    processingStatus = preserveUnprocessed ? 'UNPROCESSED' : 'RATE_MISSING';
    hasRateIssue = true;
    hasPayChannelIssue = payChannelBad;
  } else if (payChannelBad) {
    processingStatus = preserveUnprocessed ? 'UNPROCESSED' : 'PAY_CHANNEL_MISSING';
    hasPayChannelIssue = true;
  } else if (preserveUnprocessed) {
    processingStatus = 'UNPROCESSED';
  } else if (qrAwaitingSignature) {
    processingStatus = 'AWAITING_MANUAL_SIGNATURE';
  } else if (!isAuthorised) {
    processingStatus = 'PENDING_AUTH';
  } else if (mustHoldForHrValidation) {
    processingStatus = 'READY_FOR_HR';
  } else {
    processingStatus = 'READY_FOR_INVOICE';
  }

  const totalPayExVat = round2(
    hours.hours_day * numberOrZero(pay.day) +
    hours.hours_night * numberOrZero(pay.night) +
    hours.hours_sat * numberOrZero(pay.sat) +
    hours.hours_sun * numberOrZero(pay.sun) +
    hours.hours_bh * numberOrZero(pay.bh)
  );
  const totalChargeExVat = round2(
    hours.hours_day * numberOrZero(charge.day) +
    hours.hours_night * numberOrZero(charge.night) +
    hours.hours_sat * numberOrZero(charge.sat) +
    hours.hours_sun * numberOrZero(charge.sun) +
    hours.hours_bh * numberOrZero(charge.bh)
  );

  const applyErniTo = String(policy?.apply_erni_to || 'PAYE_ONLY').toUpperCase();
  const erniPctRaw = Number(policy?.erni_pct ?? 0);
  const erniFraction = Number.isFinite(erniPctRaw) && erniPctRaw > 0
    ? (erniPctRaw > 1 ? erniPctRaw / 100 : erniPctRaw)
    : 0;
  const erniApplies = applyErniTo === 'ALL' || (applyErniTo === 'PAYE_ONLY' && payMethod === 'PAYE');
  const payCostExVat = round2(erniApplies ? totalPayExVat * (1 + erniFraction) : totalPayExVat);
  const marginExVat = round2(totalChargeExVat - payCostExVat);

  const invoiceBreakdown = {
    mode: 'AGGREGATE',
    base_hours: {
      day: hours.hours_day,
      night: hours.hours_night,
      sat: hours.hours_sat,
      sun: hours.hours_sun,
      bh: hours.hours_bh,
      pay_rates: pay,
      charge_rates: charge,
      pay_ex_vat: totalPayExVat,
      charge_ex_vat: totalChargeExVat
    },
    additional: { units: {}, pay_ex_vat: 0, charge_ex_vat: 0, margin_ex_vat: 0 },
    totals: {
      total_pay_ex_vat: totalPayExVat,
      total_charge_ex_vat: totalChargeExVat,
      margin_ex_vat: marginExVat
    }
  };

  return {
    timesheet_id: timesheet.timesheet_id,
    timesheet_version: timesheet.version || 1,
    basis: 'SELF_REPORTED',
    occupant_key_norm: timesheet.occupant_key_norm || null,
    worked_start_iso: timesheet.worked_start_iso || null,
    worked_end_iso: timesheet.worked_end_iso || null,
    break_start_iso: timesheet.break_start_iso || null,
    break_end_iso: timesheet.break_end_iso || null,
    break_minutes: timesheet.break_minutes ?? null,
    candidate_id: candidateId,
    client_id: clientId || null,
    role: role || null,
    band: band || null,
    pay_method: payMethod,
    policy_snapshot_json: policy || {},
    rate_source_refs_json: {
      kind: rates.source_kind || 'NONE',
      override_id: rates.override_id || null,
      default_id: rates.default_id || null,
      rate_type: rates.rate_type || rateType || null
    },
    hours_day: hours.hours_day,
    hours_night: hours.hours_night,
    hours_sat: hours.hours_sat,
    hours_sun: hours.hours_sun,
    hours_bh: hours.hours_bh,
    pay_day: pay.day,
    pay_night: pay.night,
    pay_sat: pay.sat,
    pay_sun: pay.sun,
    pay_bh: pay.bh,
    charge_day: charge.day,
    charge_night: charge.night,
    charge_sat: charge.sat,
    charge_sun: charge.sun,
    charge_bh: charge.bh,
    total_hours: round2(
      hours.hours_day + hours.hours_night + hours.hours_sat + hours.hours_sun + hours.hours_bh
    ),
    total_pay_ex_vat: totalPayExVat,
    total_charge_ex_vat: totalChargeExVat,
    margin_ex_vat: marginExVat,
    additional_units_json: {},
    additional_pay_ex_vat: 0,
    additional_charge_ex_vat: 0,
    additional_margin_ex_vat: 0,
    pay_wtr_rate_pct_snapshot: null,
    candidate_assignment: candidateAssignment || (candidateId ? 'ASSIGNED' : 'UNASSIGNED'),
    processing_status: processingStatus,
    has_rate_issue: hasRateIssue,
    has_pay_channel_issue: hasPayChannelIssue,
    invoice_breakdown_json: invoiceBreakdown
  };
}
