const BUCKETS = Object.freeze(['day', 'night', 'sat', 'sun', 'bh']);

export const WEEKLY_SHIFT_CALCULATOR_VERSION = 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function finiteNumber(value, code, label) {
  if (value == null || value === '' || !Number.isFinite(Number(value))) {
    fail(code, `${label} is unavailable.`);
  }
  return Number(value);
}

function integerMinutes(value, code, label) {
  const minutes = finiteNumber(value, code, label);
  if (!Number.isSafeInteger(minutes) || minutes < 0) {
    fail(code, `${label} must be a non-negative whole number of minutes.`);
  }
  return minutes;
}

function round2(value) {
  return Math.round((Number(value) || 0) * 100) / 100;
}

function moneyPence(hours, rate, sign) {
  const unsignedAmount = round2(hours * rate);
  const signedAmount = round2(unsignedAmount * sign);
  const pence = Math.round(signedAmount * 100);
  if (!Number.isSafeInteger(pence)) {
    fail('WEEKLY_SHIFT_AMOUNT_OUT_OF_RANGE', 'The calculated shift amount is outside the supported range.');
  }
  return { amount: signedAmount, pence };
}

function canonicalRateTable(rates, label, { strictPositive = false } = {}) {
  if (!rates || typeof rates !== 'object' || Array.isArray(rates)) {
    fail('WEEKLY_SHIFT_RATES_UNAVAILABLE', `${label} rates are unavailable.`);
  }
  const result = {};
  for (const bucket of BUCKETS) {
    // Compatibility mode deliberately mirrors the established Weekly writer's
    // Number(value) || 0 coercion. Strict source qualification never uses it.
    const value = strictPositive
      ? finiteNumber(rates[bucket], 'WEEKLY_SHIFT_RATES_UNAVAILABLE', `${label} ${bucket} rate`)
      : (Number.isFinite(Number(rates[bucket])) ? (Number(rates[bucket]) || 0) : 0);
    if (strictPositive && value <= 0) {
      fail('WEEKLY_SHIFT_RATE_INVALID', `${label} ${bucket} rate is invalid.`, { bucket });
    }
    result[bucket] = value;
  }
  return result;
}

function normaliseSign(value) {
  const sign = Number(value ?? 1);
  if (sign !== 1 && sign !== -1) {
    fail('WEEKLY_SHIFT_SIGN_INVALID', 'The shift sign must be positive or negative.');
  }
  return sign;
}

function localDateParts(instant, timeZone) {
  const parsed = new Date(instant);
  if (!Number.isFinite(parsed.getTime())) {
    fail('WEEKLY_SHIFT_INSTANT_INVALID', 'The shift contains an invalid time.');
  }
  let parts;
  try {
    parts = new Intl.DateTimeFormat('en-GB', {
      timeZone,
      weekday: 'short',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hourCycle: 'h23',
    }).formatToParts(parsed);
  } catch {
    fail('WEEKLY_SHIFT_TIMEZONE_INVALID', 'The weekly shift time zone is invalid.');
  }
  const value = (type) => parts.find((part) => part.type === type)?.value ?? '';
  return {
    date: `${value('year')}-${value('month')}-${value('day')}`,
    weekday: value('weekday').toUpperCase(),
    localTime: `${value('hour') === '24' ? '00' : value('hour')}:${value('minute')}`,
  };
}

function validateInstants(startInstant, endInstant, timeZone) {
  const startMs = new Date(startInstant).getTime();
  const endMs = new Date(endInstant).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    fail('WEEKLY_SHIFT_INTERVAL_INVALID', 'The shift start and finish must form a valid forward interval.');
  }
  const elapsed = (endMs - startMs) / 60000;
  if (!Number.isSafeInteger(elapsed) || elapsed <= 0) {
    fail('WEEKLY_SHIFT_INTERVAL_NOT_WHOLE_MINUTES', 'The shift interval must resolve to whole real minutes.');
  }
  return {
    startMs,
    endMs,
    elapsedMinutes: elapsed,
    startLocal: localDateParts(startInstant, timeZone),
    endLocal: localDateParts(endInstant, timeZone),
  };
}

function resolvePaidMinutes(interval, breakEvidence = {}) {
  const exact = Array.isArray(breakEvidence.exactIntervals) ? breakEvidence.exactIntervals : [];
  const hasDuration = breakEvidence.durationMinutes != null && breakEvidence.durationMinutes !== '';
  const hasPaid = breakEvidence.paidMinutes != null && breakEvidence.paidMinutes !== '';
  if (exact.length && (hasDuration || hasPaid)) {
    fail('WEEKLY_SHIFT_BREAK_AMBIGUOUS', 'The shift contains more than one break authority.');
  }
  if (hasDuration && hasPaid) {
    fail('WEEKLY_SHIFT_BREAK_AMBIGUOUS', 'The shift contains more than one break authority.');
  }

  let breakMinutes = 0;
  if (exact.length) {
    const ordered = exact.map((item) => {
      const startMs = new Date(item?.startInstant).getTime();
      const endMs = new Date(item?.endInstant).getTime();
      if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
        fail('WEEKLY_SHIFT_BREAK_INTERVAL_INVALID', 'A break interval is invalid.');
      }
      if (startMs < interval.startMs || endMs > interval.endMs) {
        fail('WEEKLY_SHIFT_BREAK_OUTSIDE_SHIFT', 'A break must sit wholly inside its shift.');
      }
      const minutes = (endMs - startMs) / 60000;
      if (!Number.isSafeInteger(minutes)) {
        fail('WEEKLY_SHIFT_BREAK_NOT_WHOLE_MINUTES', 'A break must resolve to whole real minutes.');
      }
      return { startMs, endMs, minutes };
    }).sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs);
    for (let index = 1; index < ordered.length; index += 1) {
      if (ordered[index].startMs < ordered[index - 1].endMs) {
        fail('WEEKLY_SHIFT_BREAKS_OVERLAP', 'Break intervals must not overlap.');
      }
    }
    breakMinutes = ordered.reduce((sum, item) => sum + item.minutes, 0);
  } else if (hasDuration) {
    breakMinutes = integerMinutes(breakEvidence.durationMinutes, 'WEEKLY_SHIFT_BREAK_INVALID', 'Break');
  } else if (hasPaid) {
    const paid = integerMinutes(breakEvidence.paidMinutes, 'WEEKLY_SHIFT_PAID_MINUTES_INVALID', 'Paid time');
    if (paid > interval.elapsedMinutes) {
      fail('WEEKLY_SHIFT_PAID_MINUTES_INVALID', 'Paid time cannot exceed the real shift interval.');
    }
    breakMinutes = interval.elapsedMinutes - paid;
  }
  if (breakMinutes > interval.elapsedMinutes) {
    fail('WEEKLY_SHIFT_BREAK_INVALID', 'Break time cannot exceed the shift interval.');
  }
  return { paidMinutes: interval.elapsedMinutes - breakMinutes, breakMinutes };
}

function wholeShiftBucket(interval, bankHolidayDates) {
  const holidays = new Set(Array.isArray(bankHolidayDates) ? bankHolidayDates.map(String) : []);
  if (holidays.has(interval.startLocal.date)) return 'bh';
  if (interval.startLocal.weekday === 'SUN') return 'sun';
  if (interval.startLocal.weekday === 'SAT') return 'sat';
  if (interval.endLocal.date > interval.startLocal.date) return 'night';
  return 'day';
}

function splitDurationBreakBuckets(ratePortions, breakMinutes, tieRule, payRates, chargeRates) {
  if (!Array.isArray(ratePortions) || ratePortions.length === 0) {
    fail('WEEKLY_SHIFT_RATE_PORTIONS_REQUIRED', 'Duration-only break calculation requires the shift rate portions.');
  }
  const tie = String(tieRule ?? 'EARLIEST_LONGEST_PORTION').trim().toUpperCase();
  if (!['EARLIEST_LONGEST_PORTION', 'LATEST_LONGEST_PORTION'].includes(tie)) {
    fail('WEEKLY_SHIFT_BREAK_TIE_RULE_INVALID', 'The duration-only break tie rule is invalid.');
  }
  const ordered = ratePortions.map((portion, index) => {
    const bucket = String(portion?.bucket ?? '').trim().toLowerCase();
    if (!BUCKETS.includes(bucket)) {
      fail('WEEKLY_SHIFT_RATE_PORTION_INVALID', 'A split-rate portion has an invalid rate category.', { index });
    }
    const startMs = new Date(portion?.startInstant).getTime();
    const endMs = new Date(portion?.endInstant).getTime();
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      fail('WEEKLY_SHIFT_RATE_PORTION_INVALID', 'A split-rate portion has an invalid interval.', { index });
    }
    const minutes = (endMs - startMs) / 60000;
    if (!Number.isSafeInteger(minutes) || minutes <= 0) {
      fail('WEEKLY_SHIFT_RATE_PORTION_INVALID', 'A split-rate portion must contain whole positive minutes.', { index });
    }
    return {
      bucket,
      startMs,
      endMs,
      minutes,
      payRate: payRates[bucket],
      chargeRate: chargeRates[bucket],
    };
  }).sort((a, b) => a.startMs - b.startMs || a.endMs - b.endMs || a.bucket.localeCompare(b.bucket));

  const merged = [];
  for (const portion of ordered) {
    const previous = merged.at(-1);
    if (previous && portion.startMs < previous.endMs) {
      fail('WEEKLY_SHIFT_RATE_PORTIONS_OVERLAP', 'Split-rate portions must not overlap.');
    }
    if (
      previous
      && portion.startMs === previous.endMs
      && portion.bucket === previous.bucket
      && portion.payRate === previous.payRate
      && portion.chargeRate === previous.chargeRate
    ) {
      previous.endMs = portion.endMs;
      previous.minutes += portion.minutes;
    } else {
      merged.push({ ...portion });
    }
  }

  const requestedBreak = integerMinutes(breakMinutes, 'WEEKLY_SHIFT_BREAK_INVALID', 'Break');
  let selected = null;
  if (requestedBreak > 0) {
    const longest = Math.max(...merged.map((portion) => portion.minutes));
    const tied = merged.filter((portion) => portion.minutes === longest);
    selected = tie === 'LATEST_LONGEST_PORTION' ? tied.at(-1) : tied[0];
    if (requestedBreak > selected.minutes) {
      fail(
        'WEEKLY_SHIFT_BREAK_EXCEEDS_LONGEST_PORTION',
        'The break is longer than the selected contiguous rate portion.',
        { breakMinutes: requestedBreak, longestPortionMinutes: selected.minutes },
      );
    }
  }

  const bucketMinutes = Object.fromEntries(BUCKETS.map((bucket) => [bucket, 0]));
  for (const portion of merged) bucketMinutes[portion.bucket] += portion.minutes;
  if (selected) bucketMinutes[selected.bucket] -= requestedBreak;

  return {
    bucketMinutes,
    breakAllocation: selected ? Object.freeze({
      bucket: selected.bucket,
      startInstant: new Date(selected.startMs).toISOString(),
      endInstant: new Date(selected.endMs).toISOString(),
      portionMinutes: selected.minutes,
      deductedMinutes: requestedBreak,
      tieRule: tie,
    }) : null,
  };
}

function financialResult(bucketMinutes, payRates, chargeRates, sign, mode, category, breakMinutes, fingerprints) {
  const hours = {};
  const unsignedHours = {};
  for (const bucket of BUCKETS) {
    const unsignedMinutes = integerMinutes(bucketMinutes[bucket] ?? 0, 'WEEKLY_SHIFT_BUCKET_MINUTES_INVALID', `${bucket} time`);
    unsignedHours[bucket] = Number((unsignedMinutes / 60).toFixed(2));
    hours[bucket] = round2(unsignedHours[bucket] * sign);
  }
  // Keep the exact established Weekly order: add the five unsigned bucket
  // products, round the shift once, and only then apply a reversal sign.
  const payUnsigned = round2(
    unsignedHours.day * payRates.day
    + unsignedHours.night * payRates.night
    + unsignedHours.sat * payRates.sat
    + unsignedHours.sun * payRates.sun
    + unsignedHours.bh * payRates.bh
  );
  const chargeUnsigned = round2(
    unsignedHours.day * chargeRates.day
    + unsignedHours.night * chargeRates.night
    + unsignedHours.sat * chargeRates.sat
    + unsignedHours.sun * chargeRates.sun
    + unsignedHours.bh * chargeRates.bh
  );
  const payAmount = round2(payUnsigned * sign);
  const chargeAmount = round2(chargeUnsigned * sign);
  const payPence = Math.round(payAmount * 100);
  const chargePence = Math.round(chargeAmount * 100);
  if (!Number.isSafeInteger(payPence) || !Number.isSafeInteger(chargePence)) {
    fail('WEEKLY_SHIFT_AMOUNT_OUT_OF_RANGE', 'The calculated shift amount is outside the supported range.');
  }
  return Object.freeze({
    ok: true,
    calculatorVersion: WEEKLY_SHIFT_CALCULATOR_VERSION,
    mode,
    category,
    sign,
    paidMinutes: Object.values(bucketMinutes).reduce((sum, value) => sum + Number(value || 0), 0),
    breakMinutes,
    bucketMinutes: Object.freeze({ ...bucketMinutes }),
    hours: Object.freeze(hours),
    payAmount,
    chargeAmount,
    payPence: String(payPence),
    chargePence: String(chargePence),
    fingerprints: Object.freeze({ ...(fingerprints ?? {}) }),
  });
}

export function canonicalWeeklyShiftFinancialSegment(input = {}) {
  const mode = String(input.mode ?? 'SPLIT_RATE_WINDOWS').toUpperCase();
  if (!['SPLIT_RATE_WINDOWS', 'WHOLE_SHIFT_START_DAY'].includes(mode)) {
    fail('WEEKLY_SHIFT_RATE_METHOD_UNSUPPORTED', 'The weekly rate calculation method is unsupported.');
  }
  const strictRates = input.strictRates === true || mode === 'WHOLE_SHIFT_START_DAY';
  const payRates = canonicalRateTable(input.payRates, 'Pay', { strictPositive: strictRates });
  const chargeRates = canonicalRateTable(input.chargeRates, 'Charge', { strictPositive: strictRates });
  const sign = normaliseSign(input.sign);

  if (mode === 'SPLIT_RATE_WINDOWS') {
    if (Array.isArray(input.ratePortions)) {
      const breakMinutes = integerMinutes(input.breakMinutes ?? 0, 'WEEKLY_SHIFT_BREAK_INVALID', 'Break');
      const allocated = splitDurationBreakBuckets(
        input.ratePortions,
        breakMinutes,
        input.durationBreakTieRule,
        payRates,
        chargeRates,
      );
      const result = financialResult(
        allocated.bucketMinutes,
        payRates,
        chargeRates,
        sign,
        mode,
        null,
        breakMinutes,
        input.fingerprints,
      );
      return Object.freeze({ ...result, breakAllocation: allocated.breakAllocation });
    }
    if (!input.bucketMinutes || typeof input.bucketMinutes !== 'object' || Array.isArray(input.bucketMinutes)) {
      fail('WEEKLY_SHIFT_BUCKET_MINUTES_REQUIRED', 'Split-rate calculation requires canonical bucket minutes.');
    }
    const bucketMinutes = Object.fromEntries(BUCKETS.map((bucket) => [
      bucket,
      integerMinutes(input.bucketMinutes[bucket] ?? 0, 'WEEKLY_SHIFT_BUCKET_MINUTES_INVALID', `${bucket} time`),
    ]));
    return financialResult(
      bucketMinutes,
      payRates,
      chargeRates,
      sign,
      mode,
      null,
      integerMinutes(input.breakMinutes ?? 0, 'WEEKLY_SHIFT_BREAK_INVALID', 'Break'),
      input.fingerprints,
    );
  }

  const timeZone = String(input.timeZone ?? 'Europe/London').trim() || 'Europe/London';
  const interval = validateInstants(input.startInstant, input.endInstant, timeZone);
  const { paidMinutes, breakMinutes } = resolvePaidMinutes(interval, input.breakEvidence);
  const category = wholeShiftBucket(interval, input.bankHolidayDates);
  const bucketMinutes = Object.fromEntries(BUCKETS.map((bucket) => [bucket, bucket === category ? paidMinutes : 0]));
  return financialResult(bucketMinutes, payRates, chargeRates, sign, mode, category, breakMinutes, {
    ...(input.fingerprints ?? {}),
    timeZone,
    startLocalDate: interval.startLocal.date,
    endLocalDate: interval.endLocal.date,
  });
}
