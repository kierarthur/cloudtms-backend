import * as XLSX from 'xlsx';

export const WEEKLY_SOURCE_PARSER_VERSION = 'WEEKLY_SOURCE_STRICT_V1';

export function normaliseHeader(value) {
  return String(value ?? '').trim().replace(/\s+/g, ' ').toLocaleLowerCase('en-GB');
}

export function normaliseText(value) {
  return String(value ?? '').trim().replace(/\s+/g, ' ');
}

export function isBlankCell(cell) {
  return !cell || cell.kind === 'BLANK' || normaliseText(cell.text) === '';
}

export function cellText(cell) {
  if (!cell || cell.kind === 'BLANK') return '';
  return normaliseText(cell.text);
}

export function serialiseIssue(code, physicalRow, column, message, extra = {}) {
  return {
    code,
    physicalRow,
    ...(column ? { column } : {}),
    message,
    ...extra,
  };
}

export function boundedCellEvidence(cell, sourceKind, maxLength = 512) {
  if (!cell) return null;
  const original = String(cell.rawToken ?? '');
  const decoded = String(cell.text ?? '');
  return {
    coordinate: cell.coordinate,
    sourceKind,
    cellKind: cell.kind,
    formulaPresent: cell.kind === 'FORMULA',
    originalToken: original.slice(0, maxLength),
    decodedToken: decoded.slice(0, maxLength),
    tokenTruncated: original.length > maxLength || decoded.length > maxLength,
  };
}

function pad2(value) {
  return String(value).padStart(2, '0');
}

function expandTwoDigitYear(year) {
  return year >= 70 ? 1900 + year : 2000 + year;
}

function validIsoDate(year, month, day) {
  const candidate = new Date(Date.UTC(year, month - 1, day));
  if (
    candidate.getUTCFullYear() !== year
    || candidate.getUTCMonth() !== month - 1
    || candidate.getUTCDate() !== day
  ) return null;
  return `${String(year).padStart(4, '0')}-${pad2(month)}-${pad2(day)}`;
}

export function parseDateCell(cell, { date1904 = false, slashOrder = 'DMY' } = {}) {
  if (isBlankCell(cell)) return { ok: false, code: 'DATE_BLANK' };
  if (['FORMULA', 'ERROR', 'BOOLEAN'].includes(cell.kind)) return { ok: false, code: 'DATE_UNVERIFIABLE' };

  if ((cell.kind === 'NUMBER' || cell.kind === 'DATE') && typeof cell.numericValue === 'number' && Number.isFinite(cell.numericValue)) {
    const parsed = XLSX.SSF.parse_date_code(cell.numericValue, { date1904 });
    if (!parsed || !parsed.y || !parsed.m || !parsed.d) return { ok: false, code: 'DATE_INVALID' };
    const value = validIsoDate(parsed.y, parsed.m, parsed.d);
    return value ? { ok: true, value } : { ok: false, code: 'DATE_INVALID' };
  }

  const source = cellText(cell);
  let match = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(source);
  if (match) {
    const value = validIsoDate(Number(match[1]), Number(match[2]), Number(match[3]));
    return value ? { ok: true, value } : { ok: false, code: 'DATE_INVALID' };
  }

  match = /^(\d{1,2})[\/-](\d{1,2})[\/-](\d{2}|\d{4})$/.exec(source);
  if (match) {
    const year = match[3].length === 2 ? expandTwoDigitYear(Number(match[3])) : Number(match[3]);
    const month = slashOrder === 'MDY' ? Number(match[1]) : Number(match[2]);
    const day = slashOrder === 'MDY' ? Number(match[2]) : Number(match[1]);
    const value = validIsoDate(year, month, day);
    return value ? { ok: true, value } : { ok: false, code: 'DATE_INVALID' };
  }

  match = /^(\d{1,2})-([A-Za-z]{3})-(\d{2}|\d{4})$/.exec(source);
  if (match) {
    const months = new Map([
      ['jan', 1], ['feb', 2], ['mar', 3], ['apr', 4], ['may', 5], ['jun', 6],
      ['jul', 7], ['aug', 8], ['sep', 9], ['oct', 10], ['nov', 11], ['dec', 12],
    ]);
    const month = months.get(match[2].toLowerCase());
    const year = match[3].length === 2 ? expandTwoDigitYear(Number(match[3])) : Number(match[3]);
    const value = month ? validIsoDate(year, month, Number(match[1])) : null;
    return value ? { ok: true, value } : { ok: false, code: 'DATE_INVALID' };
  }

  return { ok: false, code: 'DATE_INVALID' };
}

export function parseTimeCell(cell) {
  if (isBlankCell(cell)) return { ok: false, code: 'TIME_BLANK' };
  if (['FORMULA', 'ERROR', 'BOOLEAN'].includes(cell.kind)) return { ok: false, code: 'TIME_UNVERIFIABLE' };
  const source = cellText(cell);
  const match = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(source);
  if (!match) return { ok: false, code: 'TIME_INVALID' };
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  const second = Number(match[3] ?? 0);
  if (hour > 23 || minute > 59 || second !== 0) return { ok: false, code: 'TIME_INVALID' };
  return { ok: true, value: `${pad2(hour)}:${pad2(minute)}`, minutes: (hour * 60) + minute };
}

export function parseIntegerMinutesCell(cell, { allowZero = true } = {}) {
  if (isBlankCell(cell)) return { ok: false, code: 'MINUTES_BLANK' };
  if (['FORMULA', 'ERROR', 'BOOLEAN', 'DATE'].includes(cell.kind)) return { ok: false, code: 'MINUTES_UNVERIFIABLE' };
  let value;
  if (cell.kind === 'NUMBER') value = cell.numericValue;
  else if (/^\d+$/.test(cellText(cell))) value = Number(cellText(cell));
  if (!Number.isSafeInteger(value) || value < 0 || (!allowZero && value === 0)) {
    return { ok: false, code: 'MINUTES_INVALID' };
  }
  return { ok: true, value };
}

export function parseDurationCell(cell, { allowZero = false } = {}) {
  if (isBlankCell(cell)) return { ok: false, code: 'DURATION_BLANK' };
  if (['FORMULA', 'ERROR', 'BOOLEAN'].includes(cell.kind)) return { ok: false, code: 'DURATION_UNVERIFIABLE' };
  if (allowZero && cell.kind === 'NUMBER' && cell.numericValue === 0) return { ok: true, value: 0 };
  const source = cellText(cell);
  const match = /^(\d+):(\d{2})$/.exec(source);
  if (!match || Number(match[2]) > 59) return { ok: false, code: 'DURATION_INVALID' };
  const minutes = (Number(match[1]) * 60) + Number(match[2]);
  if (!Number.isSafeInteger(minutes) || minutes < 0 || (!allowZero && minutes === 0)) {
    return { ok: false, code: 'DURATION_INVALID' };
  }
  return { ok: true, value: minutes };
}

export function validateActualTuple({ startCell, endCell, breakCell, totalCell, allowExplicitZero = false }) {
  const blankStates = [startCell, endCell, breakCell, totalCell].map(isBlankCell);
  if (blankStates.every(Boolean)) return { ok: false, kind: 'ABSENT', code: 'ACTUAL_TUPLE_ABSENT' };

  const total = parseDurationCell(totalCell, { allowZero: allowExplicitZero });
  const start = parseTimeCell(startCell);
  const end = parseTimeCell(endCell);
  const breakResult = parseIntegerMinutesCell(breakCell);

  if (allowExplicitZero && total.ok && total.value === 0) {
    if (start.ok || end.ok || breakResult.ok) {
      return { ok: false, kind: 'INVALID', code: 'ZERO_ACTUAL_WITH_SHIFT_FIELDS' };
    }
    return { ok: true, kind: 'ZERO', totalMinutes: 0 };
  }

  if (!start.ok || !end.ok || !breakResult.ok || !total.ok) {
    return { ok: false, kind: 'INVALID', code: 'ACTUAL_TUPLE_INCOMPLETE_OR_INVALID' };
  }
  if (start.minutes === end.minutes) return { ok: false, kind: 'INVALID', code: 'ACTUAL_START_EQUALS_END' };

  const elapsedMinutes = end.minutes > start.minutes
    ? end.minutes - start.minutes
    : (24 * 60) - start.minutes + end.minutes;
  const calculatedMinutes = elapsedMinutes - breakResult.value;
  if (calculatedMinutes <= 0 || calculatedMinutes !== total.value) {
    return {
      ok: false,
      kind: 'INVALID',
      code: 'ACTUAL_INTERVAL_TOTAL_MISMATCH',
      calculatedMinutes,
      statedMinutes: total.value,
    };
  }
  return {
    ok: true,
    kind: 'WORKED',
    start: start.value,
    end: end.value,
    breakMinutes: breakResult.value,
    totalMinutes: total.value,
    overnight: end.minutes < start.minutes,
  };
}

export async function sha256Hex(bytes) {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const digest = await crypto.subtle.digest('SHA-256', view);
  return Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, '0')).join('');
}

export function stableSortByPhysicalRow(rows) {
  return [...rows].sort((left, right) => {
    if (left.worksheetIndex !== right.worksheetIndex) return left.worksheetIndex - right.worksheetIndex;
    return left.physicalRow - right.physicalRow;
  });
}
