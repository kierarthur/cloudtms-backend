/*
 * Canonical Europe/London DAILY schedule normalisation.
 *
 * Office create/edit and Candidate finalisation must all call this module.
 * A zero-minute break with no break start/end is an explicit supported state.
 */

const trim = (value) => String(value == null ? '' : value).trim();

const parseYmd = (value) => {
  const match = trim(value).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const y = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(y, month - 1, day));
  if (
    date.getUTCFullYear() !== y ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) return null;
  return { y, month, day };
};

const addDaysYmd = (ymd, days) => {
  const parsed = parseYmd(ymd);
  if (!parsed) return null;
  const date = new Date(Date.UTC(parsed.y, parsed.month - 1, parsed.day));
  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, '0'),
    String(date.getUTCDate()).padStart(2, '0')
  ].join('-');
};

const normaliseHhmm = (value) => {
  let raw = trim(value);
  if (!raw) return '';
  if (/^\d{1,2}:\d{2}$/.test(raw)) {
    const [hour, minute] = raw.split(':').map(Number);
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return '';
    return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
  }
  raw = raw.replace(':', '');
  if (!/^\d{1,4}$/.test(raw)) return '';
  const hour = raw.length <= 2 ? Number(raw) : Number(raw.slice(0, -2));
  const minute = raw.length <= 2 ? 0 : Number(raw.slice(-2));
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return '';
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
};

const hhmmToMinutes = (value) => {
  const match = trim(value).match(/^(\d{2}):(\d{2})$/);
  if (!match) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return hour * 60 + minute;
};

const uniqueUtcCandidates = (ymd, hhmm, ukLocalToUtcISO) => {
  const byIso = new Map();
  for (const prefer of ['earlier', 'later']) {
    const iso = ukLocalToUtcISO(ymd, hhmm, { prefer });
    const milliseconds = iso ? new Date(iso).getTime() : NaN;
    if (Number.isFinite(milliseconds)) {
      const canonical = new Date(milliseconds).toISOString();
      byIso.set(canonical, { iso: canonical, milliseconds });
    }
  }
  return [...byIso.values()].sort((a, b) => a.milliseconds - b.milliseconds);
};

const resolveWindow = (startYmd, startHhmm, endYmd, endHhmm, label, ukLocalToUtcISO) => {
  const starts = uniqueUtcCandidates(startYmd, startHhmm, ukLocalToUtcISO);
  const ends = uniqueUtcCandidates(endYmd, endHhmm, ukLocalToUtcISO);
  if (!starts.length) return { error: `Non-existent local start time on ${label}` };
  if (!ends.length) return { error: `Non-existent local end time on ${label}` };

  const pairs = new Map();
  const addPair = (startIso, endIso) => {
    const startMs = new Date(startIso).getTime();
    const endMs = new Date(endIso).getTime();
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return;
    pairs.set(`${startMs}|${endMs}`, {
      start_iso: new Date(startMs).toISOString(),
      end_iso: new Date(endMs).toISOString(),
      start_ms: startMs,
      end_ms: endMs
    });
  };

  for (const start of starts) {
    const endAfter = ukLocalToUtcISO(endYmd, endHhmm, { afterIso: start.iso, prefer: 'later' });
    if (endAfter) addPair(start.iso, endAfter);
    for (const end of ends) addPair(start.iso, end.iso);
  }

  const resolved = [...pairs.values()].sort((a, b) => a.start_ms - b.start_ms || a.end_ms - b.end_ms);
  if (!resolved.length) return { error: `Invalid local time range on ${label}` };
  if (resolved.length > 1) return { error: `Ambiguous local time range across DST change on ${label}` };
  return resolved[0];
};

export function mapCanonicalDailyScheduleToIso(scheduleJson, {
  workedDateFallback = null,
  ukLocalToUtcISO
} = {}) {
  if (typeof ukLocalToUtcISO !== 'function') {
    throw new Error('DAILY_UK_TIME_AUTHORITY_REQUIRED');
  }

  let segment = null;
  if (Array.isArray(scheduleJson)) {
    const usable = scheduleJson.filter((item) => (
      item && typeof item === 'object' &&
      parseYmd(item.date || item.work_date || item.ymd || workedDateFallback || '')
    ));
    if (!usable.length) return { error: 'schedule_json must contain one usable date entry.' };
    const dates = new Set(usable.map((item) => trim(item.date || item.work_date || item.ymd || workedDateFallback)));
    if (dates.size !== 1) return { error: 'DAILY schedule_json must contain exactly one date entry.' };
    segment = usable[0];
  } else if (scheduleJson && typeof scheduleJson === 'object') {
    segment = scheduleJson;
  } else {
    return { error: 'schedule_json must be an object or array.' };
  }

  const ymd = trim(segment.date || segment.work_date || segment.ymd || workedDateFallback);
  if (!parseYmd(ymd)) return { error: 'schedule_json.date must be a valid YYYY-MM-DD.' };

  const startHhmm = normaliseHhmm(segment.start || segment.worked_start || '');
  const endHhmm = normaliseHhmm(segment.end || segment.worked_end || '');
  if (!startHhmm || !endHhmm) {
    return { error: 'schedule_json.start and schedule_json.end must both be valid HH:MM values.' };
  }

  const startMinute = hhmmToMinutes(startHhmm);
  const endMinute = hhmmToMinutes(endHhmm);
  if (startMinute === endMinute) return { error: 'schedule_json.start cannot equal schedule_json.end.' };
  const adjustedEndMinute = endMinute > startMinute ? endMinute : endMinute + 1440;
  const endYmd = addDaysYmd(ymd, Math.floor(adjustedEndMinute / 1440));
  const endLocalMinute = adjustedEndMinute % 1440;
  const endLocal = `${String(Math.floor(endLocalMinute / 60)).padStart(2, '0')}:${String(endLocalMinute % 60).padStart(2, '0')}`;
  const worked = resolveWindow(ymd, startHhmm, endYmd, endLocal, 'worked shift', ukLocalToUtcISO);
  if (worked.error) return worked;

  const workedMinutes = Math.round((worked.end_ms - worked.start_ms) / 60000);
  if (workedMinutes <= 0 || workedMinutes > 36 * 60) {
    return { error: 'Worked shift duration must be between 1 minute and 36 hours.' };
  }

  const meaningfulBreaks = Array.isArray(segment.breaks)
    ? segment.breaks.filter((item) => item && typeof item === 'object' && (trim(item.start) || trim(item.end)))
    : [];
  if (meaningfulBreaks.length > 1) {
    return { error: 'DAILY schedule_json supports only one break window.' };
  }

  const breakStartHhmm = normaliseHhmm(segment.break_start || meaningfulBreaks[0]?.start || '');
  const breakEndHhmm = normaliseHhmm(segment.break_end || meaningfulBreaks[0]?.end || '');
  if (Boolean(breakStartHhmm) !== Boolean(breakEndHhmm)) {
    return { error: 'schedule_json.break_start and schedule_json.break_end must both be present or both blank.' };
  }

  let breakStartIso = null;
  let breakEndIso = null;
  let breakMinutes = null;
  const rawBreakMinutes = segment.break_minutes === '' || segment.break_minutes == null
    ? null
    : Number(segment.break_minutes);

  if (breakStartHhmm && breakEndHhmm) {
    const breakStartMinute = hhmmToMinutes(breakStartHhmm);
    const breakEndMinute = hhmmToMinutes(breakEndHhmm);
    if (breakStartMinute === breakEndMinute) {
      return { error: 'schedule_json.break_start cannot equal schedule_json.break_end.' };
    }
    let adjustedBreakStart = breakStartMinute;
    while (adjustedBreakStart < startMinute) adjustedBreakStart += 1440;
    let adjustedBreakEnd = breakEndMinute;
    while (adjustedBreakEnd <= adjustedBreakStart) adjustedBreakEnd += 1440;
    if (adjustedBreakStart < startMinute || adjustedBreakEnd > adjustedEndMinute) {
      return { error: 'Break window must be fully within the worked shift span.' };
    }

    const breakStartYmd = addDaysYmd(ymd, Math.floor(adjustedBreakStart / 1440));
    const breakEndYmd = addDaysYmd(ymd, Math.floor(adjustedBreakEnd / 1440));
    const startOfBreak = adjustedBreakStart % 1440;
    const endOfBreak = adjustedBreakEnd % 1440;
    const breakStartLocal = `${String(Math.floor(startOfBreak / 60)).padStart(2, '0')}:${String(startOfBreak % 60).padStart(2, '0')}`;
    const breakEndLocal = `${String(Math.floor(endOfBreak / 60)).padStart(2, '0')}:${String(endOfBreak % 60).padStart(2, '0')}`;
    const resolvedBreak = resolveWindow(
      breakStartYmd,
      breakStartLocal,
      breakEndYmd,
      breakEndLocal,
      'break window',
      ukLocalToUtcISO
    );
    if (resolvedBreak.error) return resolvedBreak;
    breakStartIso = resolvedBreak.start_iso;
    breakEndIso = resolvedBreak.end_iso;
    breakMinutes = Math.round((resolvedBreak.end_ms - resolvedBreak.start_ms) / 60000);
    if (rawBreakMinutes != null && Math.abs(Number(rawBreakMinutes) - breakMinutes) > 1) {
      return { error: 'schedule_json.break_minutes does not match the break start/end.' };
    }
  } else if (rawBreakMinutes != null) {
    if (!Number.isFinite(rawBreakMinutes) || rawBreakMinutes < 0) {
      return { error: 'schedule_json.break_minutes must be a non-negative number.' };
    }
    breakMinutes = Math.round(rawBreakMinutes);
  }

  if (breakMinutes != null && breakMinutes > workedMinutes) {
    return { error: 'Break duration cannot exceed the worked shift duration.' };
  }

  return {
    ymd,
    worked_start_iso: worked.start_iso,
    worked_end_iso: worked.end_iso,
    break_start_iso: breakStartIso,
    break_end_iso: breakEndIso,
    break_minutes: breakMinutes,
    worked_minutes: workedMinutes,
    net_worked_minutes: workedMinutes - (breakMinutes || 0),
    normalized_schedule_json: {
      date: ymd,
      start: startHhmm,
      end: endHhmm,
      break_start: breakStartHhmm || '',
      break_end: breakEndHhmm || '',
      break_minutes: breakStartHhmm ? '' : (breakMinutes == null ? '' : breakMinutes),
      no_break: breakMinutes === 0 && !breakStartIso && !breakEndIso
    }
  };
}

export function buildCandidateDailyPatchFromFrozenInput(canonicalSaveInput) {
  const contract = canonicalSaveInput && typeof canonicalSaveInput === 'object' && !Array.isArray(canonicalSaveInput)
    ? canonicalSaveInput
    : null;
  if (!contract || contract.contract_version !== 'CANDIDATE_DAILY_CANONICAL_SAVE_V1') {
    throw new Error('CANDIDATE_DAILY_CANONICAL_SAVE_CONTRACT_INVALID');
  }
  const source = contract.timesheet_patch_json;
  if (!source || typeof source !== 'object' || Array.isArray(source)) {
    throw new Error('CANDIDATE_DAILY_SAVE_PAYLOAD_INVALID');
  }
  const workedStartIso = source.worked_start_iso == null ? null : new Date(String(source.worked_start_iso)).toISOString();
  const workedEndIso = source.worked_end_iso == null ? null : new Date(String(source.worked_end_iso)).toISOString();
  if (!workedStartIso || !workedEndIso) throw new Error('CANDIDATE_DAILY_WORKED_TIME_REQUIRED');
  const workedStartMs = new Date(workedStartIso).getTime();
  const workedEndMs = new Date(workedEndIso).getTime();
  const workedMinutes = Math.round((workedEndMs - workedStartMs) / 60000);
  if (!Number.isFinite(workedMinutes) || workedMinutes <= 0 || workedMinutes > 36 * 60) {
    throw new Error('CANDIDATE_DAILY_WORKED_TIME_INVALID');
  }

  const hasBreakStart = source.break_start_iso != null && trim(source.break_start_iso) !== '';
  const hasBreakEnd = source.break_end_iso != null && trim(source.break_end_iso) !== '';
  if (hasBreakStart !== hasBreakEnd) throw new Error('CANDIDATE_DAILY_BREAK_INVALID');
  let breakStartIso = null;
  let breakEndIso = null;
  let breakMinutes;
  if (hasBreakStart) {
    breakStartIso = new Date(String(source.break_start_iso)).toISOString();
    breakEndIso = new Date(String(source.break_end_iso)).toISOString();
    const breakStartMs = new Date(breakStartIso).getTime();
    const breakEndMs = new Date(breakEndIso).getTime();
    breakMinutes = Math.round((breakEndMs - breakStartMs) / 60000);
    if (!Number.isFinite(breakMinutes) || breakMinutes <= 0 || breakStartMs < workedStartMs || breakEndMs > workedEndMs) {
      throw new Error('CANDIDATE_DAILY_BREAK_INVALID');
    }
    if (source.break_minutes != null && Number(source.break_minutes) !== breakMinutes) {
      throw new Error('CANDIDATE_DAILY_BREAK_MISMATCH');
    }
  } else {
    breakMinutes = source.break_minutes == null || source.break_minutes === ''
      ? null
      : Number(source.break_minutes);
    if (breakMinutes !== 0) {
      throw new Error('CANDIDATE_DAILY_BREAK_TIMES_REQUIRED');
    }
  }

  const actualScheduleJson = source.actual_schedule_json;
  if (actualScheduleJson == null || (typeof actualScheduleJson !== 'object')) {
    throw new Error('CANDIDATE_DAILY_ACTUAL_SCHEDULE_REQUIRED');
  }
  return {
    worked_start_iso: workedStartIso,
    worked_end_iso: workedEndIso,
    worked_minutes: workedMinutes,
    break_start_iso: breakStartIso,
    break_end_iso: breakEndIso,
    break_minutes: breakMinutes,
    actual_schedule_json: actualScheduleJson
  };
}
export function buildCanonicalDailyScheduleFromState({
  workedStartIso,
  workedEndIso,
  breakStartIso,
  breakEndIso,
  breakMinutes,
  ymdHint = null,
  toLocalParts
}) {
  if (typeof toLocalParts !== 'function' || !workedStartIso || !workedEndIso) return null;
  const start = toLocalParts(workedStartIso, 'Europe/London');
  const end = toLocalParts(workedEndIso, 'Europe/London');
  if (!start?.ymd || !start?.hhmm || !end?.hhmm) return null;
  const breakStart = breakStartIso ? toLocalParts(breakStartIso, 'Europe/London') : null;
  const breakEnd = breakEndIso ? toLocalParts(breakEndIso, 'Europe/London') : null;
  const hasBreakWindow = Boolean(breakStart?.hhmm && breakEnd?.hhmm);
  const minutes = breakMinutes == null || !Number.isFinite(Number(breakMinutes))
    ? null
    : Math.round(Number(breakMinutes));
  return {
    date: parseYmd(ymdHint) ? ymdHint : start.ymd,
    start: start.hhmm,
    end: end.hhmm,
    break_start: hasBreakWindow ? breakStart.hhmm : '',
    break_end: hasBreakWindow ? breakEnd.hhmm : '',
    break_minutes: hasBreakWindow ? '' : (minutes == null ? '' : minutes),
    no_break: minutes === 0 && !hasBreakWindow
  };
}
