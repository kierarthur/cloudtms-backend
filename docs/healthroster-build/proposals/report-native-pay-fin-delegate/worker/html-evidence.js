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

function decodeEntities(value) {
  return String(value ?? '')
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, decimal) => String.fromCodePoint(Number(decimal)))
    .replace(/&nbsp;/gi, '\u00a0')
    .replace(/&pound;/gi, '£')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&apos;/gi, "'")
    .replace(/&amp;/gi, '&');
}

function cellText(fragment) {
  return decodeEntities(fragment
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, ''));
}

function spanValue(attributes, name) {
  const match = new RegExp(`\\b${name}\\s*=\\s*["']?(\\d+)`, 'i').exec(attributes);
  if (!match) return 1;
  const value = Number(match[1]);
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_COLUMNS) {
    failParser('HTML_CELL_SPAN_INVALID', 'The HTML table contains an invalid cell span.');
  }
  return value;
}

export async function readHtmlEvidence(input) {
  const bytes = asBytes(input);
  if (bytes.byteLength === 0) failParser('SOURCE_FILE_EMPTY', 'The weekly source file is empty.');
  if (bytes.byteLength > MAX_FILE_BYTES) failParser('SOURCE_FILE_TOO_LARGE', 'The weekly source file exceeds the permitted size.');
  const html = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
  if (/<\s*(?:frameset|frame|iframe|object|embed|link)\b/i.test(html)) {
    failParser('HTML_WRAPPER_NOT_SUPPORTED', 'HTML wrapper and frame exports are not accepted as source evidence.');
  }
  if (/<\s*(?:script|img|link)\b[^>]*(?:src|href)\s*=\s*["']?\s*(?:https?:)?\/\//i.test(html)) {
    failParser('HTML_REMOTE_REFERENCE_NOT_SUPPORTED', 'HTML source evidence must be self-contained.');
  }
  const tableOpeningCount = Array.from(html.matchAll(/<table\b/gi)).length;
  const tables = Array.from(html.matchAll(/<table\b[^>]*>([\s\S]*?)<\/table>/gi));
  if (tableOpeningCount !== 1 || tables.length !== 1) {
    failParser('HTML_SINGLE_TABLE_REQUIRED', 'HTML source evidence must contain exactly one data table.', { tableCount: tableOpeningCount });
  }

  const occupied = new Map();
  const rows = [];
  let cellCount = 0;
  const trMatches = Array.from(tables[0][1].matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi));
  if (trMatches.length === 0 || trMatches.length > MAX_ROWS) {
    failParser('HTML_ROW_COUNT_INVALID', 'The HTML table has an invalid row count.');
  }
  for (let rowIndex = 0; rowIndex < trMatches.length; rowIndex += 1) {
    const physicalRow = rowIndex + 1;
    const cells = [];
    let columnIndex = 1;
    const fragments = Array.from(trMatches[rowIndex][1].matchAll(/<(td|th)\b([^>]*)>([\s\S]*?)<\/\1>/gi));
    for (const fragment of fragments) {
      while ((occupied.get(`${physicalRow}:${columnIndex}`) ?? 0) > 0) columnIndex += 1;
      const colspan = spanValue(fragment[2], 'colspan');
      const rowspan = spanValue(fragment[2], 'rowspan');
      if (columnIndex + colspan - 1 > MAX_COLUMNS) failParser('HTML_COLUMN_LIMIT_EXCEEDED', 'The HTML table exceeds the permitted column count.');
      const text = cellText(fragment[3]);
      for (let columnOffset = 0; columnOffset < colspan; columnOffset += 1) {
        const column = columnIndex + columnOffset;
        const coordinate = `${column}:${physicalRow}`;
        const cell = {
          worksheetIndex: 0,
          coordinate,
          physicalRow,
          columnIndex: column,
          columnName: String(column),
          kind: columnOffset === 0 && text !== '' ? 'STRING' : 'BLANK',
          text: columnOffset === 0 ? text : '',
          rawToken: columnOffset === 0 ? text : '',
          numericValue: null,
          htmlHeaderCell: fragment[1].toLowerCase() === 'th',
          ...(columnOffset > 0 ? { mergedFromColumn: columnIndex } : {}),
        };
        cells.push(cell);
        for (let rowOffset = 1; rowOffset < rowspan; rowOffset += 1) {
          occupied.set(`${physicalRow + rowOffset}:${column}`, 1);
        }
        cellCount += 1;
        if (cellCount > MAX_CELLS) failParser('HTML_CELL_LIMIT_EXCEEDED', 'The HTML table exceeds the permitted cell count.');
      }
      columnIndex += colspan;
    }
    cells.sort((left, right) => left.columnIndex - right.columnIndex);
    rows.push({ physicalRow, cells });
  }

  const lookup = new Map(rows.flatMap((row) => row.cells.map((cell) => [`${row.physicalRow}:${cell.columnIndex}`, cell])));
  return {
    sourceKind: 'HTML',
    sourceFileSha256: await sha256Hex(bytes),
    fileByteLength: bytes.byteLength,
    date1904: false,
    sheets: [{
      name: 'HTML_TABLE',
      index: 0,
      partPath: null,
      physicalRowCount: rows.length,
      physicalCellCount: cellCount,
      firstPhysicalRow: 1,
      lastPhysicalRow: rows.length,
      rows,
      getCell(row, oneBasedColumn) {
        return lookup.get(`${row}:${oneBasedColumn}`) ?? null;
      },
      getCellByCoordinate(coordinate) {
        const match = /^(\d+):(\d+)$/.exec(String(coordinate));
        return match ? lookup.get(`${Number(match[2])}:${Number(match[1])}`) ?? null : null;
      },
    }],
  };
}
