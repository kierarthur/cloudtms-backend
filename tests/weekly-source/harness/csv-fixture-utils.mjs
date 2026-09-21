import { createHash } from 'node:crypto';

function escapeCsv(value) {
  const text = value === null || value === undefined ? '' : String(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function writeCsvArtifact({ profile, fileName, headers, rows }) {
  const lines = [headers, ...rows].map((row) => row.map(escapeCsv).join(','));
  const bytes = Buffer.from(`${lines.join('\r\n')}\r\n`, 'utf8');
  return {
    profile,
    fileName,
    mediaType: 'text/csv',
    sheetName: null,
    bytes,
    byteCount: bytes.byteLength,
    sha256: createHash('sha256').update(bytes).digest('hex')
  };
}

export function poundsTextFromPence(pence) {
  const value = BigInt(pence);
  const absolute = value < 0n ? -value : value;
  return `${value < 0n ? '-' : ''}${absolute / 100n}.${String(absolute % 100n).padStart(2, '0')}`;
}

export function decimalHoursText(minutes) {
  if (!Number.isInteger(minutes) || minutes < 0 || minutes > 2880) throw new TypeError('minutes must be a bounded integer');
  return (minutes / 60).toFixed(2);
}
