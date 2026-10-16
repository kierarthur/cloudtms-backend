import * as XLSX from 'xlsx';
import { boundedCellEvidence, isBlankCell } from './shared.js';

/** Export Finalised Date is a Europe/London wall clock. Never substitute an
 * upload time, Entered Date or report/request time for this physical cell. */
export function parseSourceFinalisedTimeCell(cell, { date1904 = false } = {}) {
  const evidence = boundedCellEvidence(cell, 'XLSX');
  const invalid = (code) => ({ ok: false, code, evidence });
  if (isBlankCell(cell)) return invalid('HEALTHROSTER_FINALISED_DATE_REQUIRED');
  if (['FORMULA', 'ERROR', 'BOOLEAN'].includes(cell.kind)) return invalid('HEALTHROSTER_FINALISED_DATE_UNVERIFIABLE');
  let parts;
  if (['NUMBER', 'DATE'].includes(cell.kind) && Number.isFinite(cell.numericValue)) {
    const parsed = XLSX.SSF.parse_date_code(cell.numericValue, { date1904 });
    if (parsed) parts = [parsed.y, parsed.m, parsed.d, parsed.H, parsed.M, parsed.S];
  } else {
    const token = String(cell.text ?? '').trim();
    let match = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(token);
    if (match) parts = match.slice(1).map((value) => Number(value ?? 0));
    else {
      match = /^(\d{1,2})[/-](\d{1,2})[/-](\d{4})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(token);
      if (match) parts = [match[3], match[2], match[1], match[4], match[5], match[6] ?? 0].map(Number);
    }
  }
  if (!parts || parts.some((part) => !Number.isInteger(part))) return invalid('HEALTHROSTER_FINALISED_DATE_INVALID');
  const [year, month, day, hour, minute, second] = parts;
  const wallClock = Date.UTC(year, month - 1, day, hour, minute, second);
  const check = new Date(wallClock);
  if (year < 1900 || year > 9999 || month < 1 || month > 12 || day < 1
      || check.getUTCFullYear() !== year || check.getUTCMonth() !== month - 1 || check.getUTCDate() !== day
      || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return invalid('HEALTHROSTER_FINALISED_DATE_INVALID');
  }
  const formatter = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London',
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
  const matches = [wallClock, wallClock - 3_600_000].filter((candidate) => {
    const actual = Object.fromEntries(formatter.formatToParts(new Date(candidate)).map((p) => [p.type, p.value]));
    return [actual.year, actual.month, actual.day, actual.hour, actual.minute, actual.second].map(Number)
      .every((value, index) => value === parts[index]);
  });
  if (matches.length !== 1) return invalid(matches.length ? 'HEALTHROSTER_FINALISED_DATE_AMBIGUOUS' : 'HEALTHROSTER_FINALISED_DATE_NONEXISTENT');
  return { ok: true, finalisedAtUtc: new Date(matches[0]).toISOString(), evidence };
}
