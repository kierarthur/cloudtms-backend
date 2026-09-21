import { createHash } from 'node:crypto';
import * as XLSX from 'xlsx';

const EXCEL_EPOCH_DAYS = 25569;
const MILLIS_PER_DAY = 86_400_000;
const FIXED_WORKBOOK_TIME = new Date('2000-01-01T00:00:00.000Z');

export class SourceFixtureError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'SourceFixtureError';
    this.code = code;
  }
}

export function excelDateSerial(isoDate) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(isoDate));
  if (!match) throw new SourceFixtureError('FIXTURE_DATE_INVALID', 'Fixture dates must use YYYY-MM-DD');
  const millis = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  return millis / MILLIS_PER_DAY + EXCEL_EPOCH_DAYS;
}

export function excelDateTimeSerial(isoDate, time = '12:00') {
  return excelDateSerial(isoDate) + excelTimeSerial(time);
}

export function excelTimeSerial(value) {
  const match = /^(?:([01]\d|2[0-3])):([0-5]\d)$/.exec(String(value));
  if (!match) throw new SourceFixtureError('FIXTURE_TIME_INVALID', 'Fixture times must use HH:mm');
  return (Number(match[1]) * 60 + Number(match[2])) / 1440;
}

export function excelDurationSerial(minutes) {
  if (!Number.isInteger(minutes) || minutes < 0 || minutes > 2880) {
    throw new SourceFixtureError('FIXTURE_DURATION_INVALID', 'Fixture duration must be a whole number of minutes');
  }
  return minutes / 1440;
}

export function decimalPoundsFromPence(pence) {
  if (!/^-?(?:0|[1-9]\d{0,14})$/.test(String(pence))) {
    throw new SourceFixtureError('FIXTURE_PENCE_INVALID', 'Fixture money must be declared as integer pence text');
  }
  const value = BigInt(pence);
  const absolute = value < 0n ? -value : value;
  const numeric = Number(`${value < 0n ? '-' : ''}${absolute / 100n}.${String(absolute % 100n).padStart(2, '0')}`);
  if (!Number.isFinite(numeric)) throw new SourceFixtureError('FIXTURE_PENCE_UNSAFE', 'Fixture money is outside the supported range');
  return numeric;
}

export function stableWorkbook(sheetName, worksheet) {
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);
  workbook.Props = {
    Title: 'CloudTMS deterministic Weekly Source test fixture',
    Subject: 'TEST-only source fixture',
    Author: 'CloudTMS test harness',
    Creator: 'CloudTMS test harness',
    CreatedDate: FIXED_WORKBOOK_TIME,
    ModifiedDate: FIXED_WORKBOOK_TIME
  };
  return workbook;
}

export function writeWorkbookArtifact({ profile, fileName, sheetName, worksheet }) {
  const workbook = stableWorkbook(sheetName, worksheet);
  const bytes = Buffer.from(XLSX.write(workbook, {
    type: 'buffer',
    bookType: 'xlsx',
    compression: false,
    bookSST: false,
    cellDates: false
  }));
  return {
    profile,
    fileName,
    mediaType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    sheetName,
    bytes,
    byteCount: bytes.byteLength,
    sha256: createHash('sha256').update(bytes).digest('hex')
  };
}

export function makeWorksheet(rows) {
  return XLSX.utils.aoa_to_sheet(rows, { cellDates: false });
}

export function setCellFormat(worksheet, address, format) {
  if (worksheet[address]) worksheet[address].z = format;
}

export function requireScenarioUpload(scenario, uploadOrKey) {
  if (!scenario?.foundation || !Array.isArray(scenario.sourceUploads)) {
    throw new SourceFixtureError('FIXTURE_SCENARIO_REQUIRED', 'A validated Weekly Source scenario is required');
  }
  const upload = typeof uploadOrKey === 'string'
    ? scenario.sourceUploads.find((item) => item.key === uploadOrKey)
    : uploadOrKey;
  if (!upload || !scenario.sourceUploads.includes(upload)) {
    throw new SourceFixtureError('FIXTURE_UPLOAD_UNKNOWN', 'The requested source upload is not part of the scenario');
  }
  const client = scenario.foundation.clients.find((item) => item.key === upload.clientKey);
  if (!client) throw new SourceFixtureError('FIXTURE_CLIENT_UNKNOWN', `Upload ${upload.key} references an unknown Client`);
  const candidates = new Map(scenario.foundation.candidates.map((item) => [item.key, item]));
  const contracts = new Map(scenario.foundation.contracts.map((item) => [item.key, item]));
  for (const row of upload.physicalRows) {
    if (!candidates.has(row.candidateKey)) {
      throw new SourceFixtureError('FIXTURE_CANDIDATE_UNKNOWN', `Source row ${row.key} references an unknown Candidate`);
    }
    if (row.contractKey && !contracts.has(row.contractKey)) {
      throw new SourceFixtureError('FIXTURE_CONTRACT_UNKNOWN', `Source row ${row.key} references an unknown Contract`);
    }
  }
  return { upload, client, candidates, contracts };
}

export function setWorkbookColumnWidths(worksheet, widths) {
  worksheet['!cols'] = widths.map((wch) => ({ wch }));
}

