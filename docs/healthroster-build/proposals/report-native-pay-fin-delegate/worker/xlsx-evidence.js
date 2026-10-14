import * as XLSX from 'xlsx';
import { failParser } from './errors.js';
import { sha256Hex } from './shared.js';

const MAX_FILE_BYTES = 20 * 1024 * 1024;
const MAX_PHYSICAL_ROWS = 100_000;
const MAX_PHYSICAL_COLUMNS = 512;
const MAX_PHYSICAL_CELLS = 1_000_000;

function asBytes(input) {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  failParser('SOURCE_BYTES_REQUIRED', 'Weekly source parsing requires file bytes.');
}

function decodeXmlEntities(value) {
  return String(value ?? '')
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, decimal) => String.fromCodePoint(Number(decimal)))
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, '&');
}

function parseAttributes(raw) {
  const attributes = {};
  const expression = /([\w:.-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g;
  for (const match of raw.matchAll(expression)) attributes[match[1]] = decodeXmlEntities(match[2] ?? match[3] ?? '');
  return attributes;
}

function normaliseZipPath(value) {
  let result = String(value ?? '').replaceAll('\\', '/').replace(/^Root Entry\//i, '').replace(/^\/+/, '');
  const parts = [];
  for (const part of result.split('/')) {
    if (!part || part === '.') continue;
    if (part === '..') parts.pop();
    else parts.push(part);
  }
  return parts.join('/');
}

function joinZipPath(base, target) {
  if (target.startsWith('/')) return normaliseZipPath(target);
  return normaliseZipPath(`${base}/${target}`);
}

function cfbEntries(bytes) {
  let cfb;
  try {
    cfb = XLSX.CFB.read(bytes, { type: 'array' });
  } catch {
    failParser('XLSX_CONTAINER_INVALID', 'The supplied file is not a readable OOXML workbook.');
  }
  const entries = new Map();
  for (let index = 0; index < cfb.FullPaths.length; index += 1) {
    const path = normaliseZipPath(cfb.FullPaths[index]);
    const content = cfb.FileIndex[index]?.content;
    if (!path || !content) continue;
    entries.set(path.toLowerCase(), content instanceof Uint8Array ? content : new Uint8Array(content));
  }
  return entries;
}

function readXml(entries, path, { required = true } = {}) {
  const content = entries.get(normaliseZipPath(path).toLowerCase());
  if (!content) {
    if (!required) return null;
    failParser('XLSX_PART_MISSING', 'A required OOXML workbook part is missing.', { part: path });
  }
  return new TextDecoder('utf-8', { fatal: false }).decode(content);
}

function sharedStringsFromXml(xml) {
  if (!xml) return [];
  const strings = [];
  for (const match of xml.matchAll(/<si\b[^>]*>([\s\S]*?)<\/si>/gi)) {
    const fragments = Array.from(match[1].matchAll(/<t\b[^>]*>([\s\S]*?)<\/t>/gi), (part) => decodeXmlEntities(part[1]));
    strings.push(fragments.join(''));
  }
  return strings;
}

function workbookSheetParts(entries) {
  const workbookXml = readXml(entries, 'xl/workbook.xml');
  const relationshipsXml = readXml(entries, 'xl/_rels/workbook.xml.rels');
  const relationships = new Map();
  for (const match of relationshipsXml.matchAll(/<Relationship\b([^>]*)\/?\s*>/gi)) {
    const attributes = parseAttributes(match[1]);
    if (attributes.Id && attributes.Target) relationships.set(attributes.Id, joinZipPath('xl', attributes.Target));
  }

  const sheets = [];
  for (const match of workbookXml.matchAll(/<sheet\b([^>]*)\/?\s*>/gi)) {
    const attributes = parseAttributes(match[1]);
    const relationId = attributes['r:id'];
    const path = relationships.get(relationId);
    if (!attributes.name || !relationId || !path) {
      failParser('XLSX_WORKSHEET_RELATION_INVALID', 'A worksheet relationship is incomplete.');
    }
    sheets.push({ name: attributes.name, path });
  }
  if (sheets.length === 0) failParser('XLSX_NO_WORKSHEETS', 'The workbook contains no worksheets.');
  return sheets;
}

function inlineString(body) {
  return Array.from(body.matchAll(/<t\b[^>]*>([\s\S]*?)<\/t>/gi), (match) => decodeXmlEntities(match[1])).join('');
}

function parseWorksheetCells(xml, sharedStrings, sheetJsWorksheet, worksheetIndex) {
  const cells = [];
  const coordinates = new Set();
  const expression = /<c\b([^>]*?)(?:\/\s*>|>([\s\S]*?)<\/c>)/gi;
  for (const match of xml.matchAll(expression)) {
    const attributes = parseAttributes(match[1]);
    const coordinate = attributes.r;
    if (!coordinate || !/^[A-Z]+[1-9]\d*$/.test(coordinate)) {
      failParser('XLSX_CELL_COORDINATE_INVALID', 'A worksheet cell does not have an explicit valid coordinate.', { worksheetIndex });
    }
    if (coordinates.has(coordinate)) {
      failParser('XLSX_DUPLICATE_CELL_COORDINATE', 'A worksheet contains the same physical cell coordinate more than once.', {
        worksheetIndex,
        coordinate,
      });
    }
    coordinates.add(coordinate);
    const decoded = XLSX.utils.decode_cell(coordinate);
    if (decoded.r + 1 > MAX_PHYSICAL_ROWS || decoded.c + 1 > MAX_PHYSICAL_COLUMNS) {
      failParser('XLSX_PHYSICAL_BOUNDS_EXCEEDED', 'The worksheet exceeds the permitted physical bounds.', {
        worksheetIndex,
        coordinate,
      });
    }

    const body = match[2] ?? '';
    const formula = /<f\b[^>]*>[\s\S]*?<\/f>|<f\b[^>]*\/\s*>/i.test(body);
    const valueMatch = /<v\b[^>]*>([\s\S]*?)<\/v>/i.exec(body);
    const rawToken = valueMatch ? decodeXmlEntities(valueMatch[1]) : '';
    const type = attributes.t ?? 'n';
    const sheetJsCell = sheetJsWorksheet?.[coordinate];
    let kind = 'BLANK';
    let text = '';
    let numericValue = null;

    if (formula || sheetJsCell?.f !== undefined) {
      kind = 'FORMULA';
      text = sheetJsCell?.w ?? rawToken;
    } else if (type === 's') {
      const sharedIndex = Number(rawToken);
      if (!Number.isSafeInteger(sharedIndex) || sharedIndex < 0 || sharedIndex >= sharedStrings.length) {
        failParser('XLSX_SHARED_STRING_INVALID', 'A worksheet references an invalid shared string.', { worksheetIndex, coordinate });
      }
      kind = 'STRING';
      text = sharedStrings[sharedIndex];
    } else if (type === 'inlineStr') {
      kind = 'STRING';
      text = inlineString(body);
    } else if (type === 'str') {
      kind = 'STRING';
      text = rawToken;
    } else if (type === 'b') {
      kind = 'BOOLEAN';
      text = rawToken;
    } else if (type === 'e') {
      kind = 'ERROR';
      text = rawToken;
    } else if (type === 'd') {
      kind = 'DATE';
      text = sheetJsCell?.w ?? rawToken;
    } else if (rawToken !== '') {
      numericValue = Number(rawToken);
      if (!Number.isFinite(numericValue)) {
        failParser('XLSX_NUMERIC_TOKEN_INVALID', 'A worksheet contains an invalid numeric token.', { worksheetIndex, coordinate });
      }
      const dateFormatted = Boolean(sheetJsCell?.z && XLSX.SSF.is_date(sheetJsCell.z));
      kind = dateFormatted ? 'DATE' : 'NUMBER';
      text = sheetJsCell?.w ?? rawToken;
    }

    cells.push({
      worksheetIndex,
      coordinate,
      physicalRow: decoded.r + 1,
      columnIndex: decoded.c + 1,
      columnName: XLSX.utils.encode_col(decoded.c),
      kind,
      text: String(text ?? ''),
      rawToken,
      numericValue,
    });
    if (cells.length > MAX_PHYSICAL_CELLS) {
      failParser('XLSX_CELL_LIMIT_EXCEEDED', 'The workbook exceeds the permitted physical cell count.');
    }
  }
  cells.sort((left, right) => left.physicalRow - right.physicalRow || left.columnIndex - right.columnIndex);
  return cells;
}

function parseWorksheetRowNumbers(xml, worksheetIndex) {
  const rowNumbers = [];
  const seen = new Set();
  for (const match of xml.matchAll(/<row\b([^>]*?)(?:\/\s*>|>[\s\S]*?<\/row>)/gi)) {
    const attributes = parseAttributes(match[1]);
    if (!attributes.r || !/^[1-9]\d*$/.test(attributes.r)) {
      failParser('XLSX_ROW_ORDINAL_INVALID', 'A worksheet row does not have an explicit valid physical ordinal.', { worksheetIndex });
    }
    const physicalRow = Number(attributes.r);
    if (!Number.isSafeInteger(physicalRow) || physicalRow > MAX_PHYSICAL_ROWS) {
      failParser('XLSX_PHYSICAL_BOUNDS_EXCEEDED', 'The worksheet exceeds the permitted physical row bounds.', { worksheetIndex });
    }
    if (seen.has(physicalRow)) {
      failParser('XLSX_DUPLICATE_ROW_ORDINAL', 'A worksheet contains the same physical row ordinal more than once.', {
        worksheetIndex,
        physicalRow,
      });
    }
    seen.add(physicalRow);
    rowNumbers.push(physicalRow);
  }
  rowNumbers.sort((left, right) => left - right);
  return rowNumbers;
}

function buildSheet(name, index, path, xml, sharedStrings, sheetJsWorksheet) {
  const cells = parseWorksheetCells(xml, sharedStrings, sheetJsWorksheet, index);
  const physicalRowNumbers = parseWorksheetRowNumbers(xml, index);
  const rows = new Map(physicalRowNumbers.map((physicalRow) => [physicalRow, []]));
  const lookup = new Map();
  for (const cell of cells) {
    if (!rows.has(cell.physicalRow)) {
      failParser('XLSX_CELL_OUTSIDE_PHYSICAL_ROW', 'A worksheet cell is not contained by a declared physical row.', {
        worksheetIndex: index,
        coordinate: cell.coordinate,
      });
    }
    lookup.set(cell.coordinate, cell);
    rows.get(cell.physicalRow).push(cell);
  }
  const physicalRows = Array.from(rows, ([physicalRow, rowCells]) => ({ physicalRow, cells: rowCells }))
    .sort((left, right) => left.physicalRow - right.physicalRow);
  return {
    name,
    index,
    partPath: path,
    physicalRowCount: physicalRows.length,
    physicalCellCount: cells.length,
    firstPhysicalRow: physicalRows[0]?.physicalRow ?? null,
    lastPhysicalRow: physicalRows.at(-1)?.physicalRow ?? null,
    rows: physicalRows,
    getCellByCoordinate(coordinate) {
      return lookup.get(coordinate) ?? null;
    },
    getCell(row, oneBasedColumn) {
      return lookup.get(`${XLSX.utils.encode_col(oneBasedColumn - 1)}${row}`) ?? null;
    },
  };
}

export async function readXlsxEvidence(input) {
  const bytes = asBytes(input);
  if (bytes.byteLength === 0) failParser('SOURCE_FILE_EMPTY', 'The weekly source file is empty.');
  if (bytes.byteLength > MAX_FILE_BYTES) failParser('SOURCE_FILE_TOO_LARGE', 'The weekly source file exceeds the permitted size.');
  if (!(bytes[0] === 0x50 && bytes[1] === 0x4b)) {
    failParser('XLSX_OOXML_REQUIRED', 'The approved XLSX profiles require an OOXML workbook.');
  }

  let workbook;
  try {
    workbook = XLSX.read(bytes, {
      type: 'array',
      raw: true,
      cellNF: true,
      cellFormula: true,
      cellDates: false,
      dense: false,
    });
  } catch {
    failParser('XLSX_WORKBOOK_INVALID', 'The supplied OOXML workbook could not be read.');
  }

  const entries = cfbEntries(bytes);
  const sheetParts = workbookSheetParts(entries);
  const sharedStrings = sharedStringsFromXml(readXml(entries, 'xl/sharedStrings.xml', { required: false }));
  const sheets = sheetParts.map((sheetPart, index) => buildSheet(
    sheetPart.name,
    index,
    sheetPart.path,
    readXml(entries, sheetPart.path),
    sharedStrings,
    workbook.Sheets[sheetPart.name],
  ));
  const totalCells = sheets.reduce((sum, sheet) => sum + sheet.physicalCellCount, 0);
  if (totalCells > MAX_PHYSICAL_CELLS) failParser('XLSX_CELL_LIMIT_EXCEEDED', 'The workbook exceeds the permitted physical cell count.');

  return {
    sourceKind: 'XLSX',
    sourceFileSha256: await sha256Hex(bytes),
    fileByteLength: bytes.byteLength,
    date1904: Boolean(workbook.Workbook?.WBProps?.date1904),
    sheets,
  };
}
