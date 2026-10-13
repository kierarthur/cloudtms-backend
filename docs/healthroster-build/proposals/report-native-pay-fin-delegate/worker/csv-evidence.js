import { failParser } from './errors.js';
import { sha256Hex } from './shared.js';

const MAX_FILE_BYTES = 20 * 1024 * 1024;
const MAX_ROWS = 100_000;
const MAX_COLUMNS = 512;
const MAX_CELLS = 1_000_000;

function asBytes(input) {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  failParser('SOURCE_BYTES_REQUIRED', 'Weekly source parsing requires file bytes.');
}

function parseCsvRecords(text) {
  const records = [];
  let record = [];
  let field = '';
  let inQuotes = false;
  let afterQuote = false;

  const finishField = () => {
    record.push(field);
    field = '';
    afterQuote = false;
  };
  const finishRecord = () => {
    finishField();
    records.push(record);
    record = [];
    if (records.length > MAX_ROWS) failParser('CSV_ROW_LIMIT_EXCEEDED', 'The CSV file exceeds the permitted row count.');
  };

  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    if (inQuotes) {
      if (character === '"') {
        if (text[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = false;
          afterQuote = true;
        }
      } else field += character;
      continue;
    }
    if (afterQuote && ![',', '\r', '\n'].includes(character)) {
      failParser('CSV_QUOTE_STRUCTURE_INVALID', 'The CSV file contains characters after a closing quote.');
    }
    if (character === '"') {
      if (field !== '') failParser('CSV_QUOTE_STRUCTURE_INVALID', 'The CSV file contains a quote inside an unquoted value.');
      inQuotes = true;
    } else if (character === ',') finishField();
    else if (character === '\n') finishRecord();
    else if (character === '\r') {
      if (text[index + 1] === '\n') index += 1;
      finishRecord();
    } else field += character;
  }
  if (inQuotes) failParser('CSV_QUOTE_STRUCTURE_INVALID', 'The CSV file has an unterminated quoted value.');
  if (field !== '' || record.length > 0) finishRecord();
  return records;
}

export async function readCsvEvidence(input) {
  const bytes = asBytes(input);
  if (bytes.byteLength === 0) failParser('SOURCE_FILE_EMPTY', 'The weekly source file is empty.');
  if (bytes.byteLength > MAX_FILE_BYTES) failParser('SOURCE_FILE_TOO_LARGE', 'The weekly source file exceeds the permitted size.');
  let text;
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(bytes).replace(/^\uFEFF/, '');
  } catch {
    failParser('CSV_UTF8_REQUIRED', 'The CSV source must be valid UTF-8.');
  }
  const records = parseCsvRecords(text);
  if (records.length === 0) failParser('CSV_HEADER_REQUIRED', 'The CSV source has no header row.');
  const width = records[0].length;
  if (width === 0 || width > MAX_COLUMNS) failParser('CSV_COLUMN_COUNT_INVALID', 'The CSV header has an invalid column count.');

  const rows = [];
  let ignoredBlankRows = 0;
  for (let recordIndex = 0; recordIndex < records.length; recordIndex += 1) {
    const record = records[recordIndex];
    if (record.every((value) => value === '') && recordIndex > 0) {
      ignoredBlankRows += 1;
      continue;
    }
    if (record.length !== width) {
      failParser('CSV_ROW_WIDTH_MISMATCH', 'A CSV row does not have the same number of fields as the header.', {
        physicalRow: recordIndex + 1,
        expectedColumns: width,
        actualColumns: record.length,
      });
    }
    const cells = record.map((value, columnIndex) => ({
      worksheetIndex: 0,
      coordinate: `R${recordIndex + 1}C${columnIndex + 1}`,
      physicalRow: recordIndex + 1,
      columnIndex: columnIndex + 1,
      columnName: String(columnIndex + 1),
      kind: value === '' ? 'BLANK' : 'STRING',
      text: value,
      rawToken: value,
      numericValue: null,
    }));
    rows.push({ physicalRow: recordIndex + 1, cells });
  }
  const cellCount = rows.length * width;
  if (cellCount > MAX_CELLS) failParser('CSV_CELL_LIMIT_EXCEEDED', 'The CSV source exceeds the permitted cell count.');
  const lookup = new Map(rows.flatMap((row) => row.cells.map((cell) => [`${row.physicalRow}:${cell.columnIndex}`, cell])));
  return {
    sourceKind: 'CSV',
    sourceFileSha256: await sha256Hex(bytes),
    fileByteLength: bytes.byteLength,
    date1904: false,
    ignoredBlankRows,
    sheets: [{
      name: 'CSV',
      index: 0,
      partPath: null,
      physicalRowCount: rows.length,
      physicalCellCount: cellCount,
      firstPhysicalRow: rows[0]?.physicalRow ?? null,
      lastPhysicalRow: rows.at(-1)?.physicalRow ?? null,
      rows,
      getCell(row, oneBasedColumn) {
        return lookup.get(`${row}:${oneBasedColumn}`) ?? null;
      },
      getCellByCoordinate(coordinate) {
        const match = /^R(\d+)C(\d+)$/.exec(String(coordinate));
        return match ? lookup.get(`${Number(match[1])}:${Number(match[2])}`) ?? null : null;
      },
    }],
  };
}

