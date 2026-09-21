import { failParser } from './errors.js';
import { readCsvEvidence } from './csv-evidence.js';
import { readHtmlEvidence } from './html-evidence.js';
import { normaliseHealthRosterEvidence } from './healthroster-normalise.js';
import { normaliseNhspEvidence } from './nhsp-normalise.js';
import { normaliseRosterSummaryEvidence } from './roster-summary-normalise.js';
import { publicColumnMap, resolveWeeklySourceProfile, WEEKLY_SOURCE_PROFILE_IDS } from './profiles.js';
import { WEEKLY_SOURCE_PARSER_VERSION } from './shared.js';
import { sha256Hex } from './shared.js';
import { readXlsxEvidence } from './xlsx-evidence.js';

function asBytes(input) {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  failParser('SOURCE_BYTES_REQUIRED', 'Weekly source parsing requires file bytes.');
}

function inferSourceKind(bytes) {
  if (bytes[0] === 0x50 && bytes[1] === 0x4b) return 'XLSX';
  const prefix = new TextDecoder('utf-8', { fatal: false }).decode(bytes.subarray(0, Math.min(bytes.byteLength, 512)))
    .replace(/^\uFEFF/, '')
    .trimStart();
  if (prefix.startsWith('<')) return 'HTML';
  if (prefix.includes(',') && /(?:\r?\n|$)/.test(prefix)) return 'CSV';
  failParser('SOURCE_FILE_KIND_NOT_RECOGNISED', 'The source bytes are neither an OOXML workbook nor a self-contained HTML table.');
}

function boundedPhysicalRows(sheet) {
  const saved = new Map((sheet.rows ?? []).map((row) => [row.physicalRow, row]));
  const last = Number(sheet.lastPhysicalRow ?? 0);
  const result = [];
  for (let physicalRow = 1; physicalRow <= last; physicalRow += 1) {
    const cells = saved.get(physicalRow)?.cells ?? [];
    const boundedRawCells = {};
    for (const cell of cells) {
      const original = String(cell.rawToken ?? '');
      const decoded = String(cell.text ?? '');
      boundedRawCells[cell.columnName] = {
        coordinate: cell.coordinate,
        kind: cell.kind,
        originalToken: original.slice(0, 512),
        decodedToken: decoded.slice(0, 512),
        tokenTruncated: original.length > 512 || decoded.length > 512,
      };
    }
    result.push({ physicalRow, boundedRawCells });
  }
  return result;
}

async function selectedPartFingerprint(sourceFileSha256, sheet) {
  if (!sheet.partPath) return null;
  const payload = JSON.stringify({
    authority: 'WEEKLY_SOURCE_WORKBOOK_PART_AND_SHEET_V1',
    sourceFileSha256,
    partPath: sheet.partPath,
    sheetName: sheet.name,
    sheetIndex: sheet.index,
    physicalRowCount: sheet.physicalRowCount,
    physicalCellCount: sheet.physicalCellCount,
    firstPhysicalRow: sheet.firstPhysicalRow,
    lastPhysicalRow: sheet.lastPhysicalRow,
  });
  return sha256Hex(new TextEncoder().encode(payload));
}

export async function parseWeeklySourceFile(input, options = {}) {
  const bytes = asBytes(input);
  const sourceKind = options.sourceKind ?? inferSourceKind(bytes);
  if (!['XLSX', 'HTML', 'CSV'].includes(sourceKind)) {
    failParser('SOURCE_KIND_NOT_SUPPORTED', 'Only XLSX, self-contained HTML and approved CSV source evidence are supported.', { sourceKind });
  }
  const evidence = sourceKind === 'XLSX'
    ? await readXlsxEvidence(bytes)
    : sourceKind === 'HTML'
      ? await readHtmlEvidence(bytes)
      : await readCsvEvidence(bytes);
  const profile = resolveWeeklySourceProfile(evidence, options);
  const nhspProfile = [
    WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
    WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
  ].includes(profile.profileId);
  const normalised = nhspProfile
    ? normaliseNhspEvidence(evidence, profile, options)
    : profile.profileId === WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1
      ? normaliseRosterSummaryEvidence(evidence, profile, options)
      : normaliseHealthRosterEvidence(evidence, profile, options);
  const workbookPartAndSheetFingerprint = await selectedPartFingerprint(
    evidence.sourceFileSha256,
    profile.sheet,
  );

  return {
    parserVersion: WEEKLY_SOURCE_PARSER_VERSION,
    profileId: profile.profileId,
    profileVersion: profile.profileVersion,
    sourceKind: evidence.sourceKind,
    sourceFileSha256: evidence.sourceFileSha256,
    fileFacts: {
      byteLength: evidence.fileByteLength,
      worksheetCount: evidence.sheets.length,
    },
    selectedWorksheet: {
      name: profile.sheet.name,
      index: profile.sheet.index,
      partPath: profile.sheet.partPath,
      physicalRowCount: profile.sheet.physicalRowCount,
      physicalOrdinalSpan: profile.sheet.lastPhysicalRow ?? 0,
      physicalCellCount: profile.sheet.physicalCellCount,
      firstPhysicalRow: profile.sheet.firstPhysicalRow,
      lastPhysicalRow: profile.sheet.lastPhysicalRow,
      workbookPartAndSheetFingerprint,
    },
    headerRow: profile.headerRow,
    resolvedColumnMap: publicColumnMap(profile.columns),
    physicalRows: boundedPhysicalRows(profile.sheet),
    ...normalised,
    ok: normalised.fatalErrors.length === 0,
  };
}

export { WeeklySourceParserError } from './errors.js';
export {
  XLSX_BINARY64_SAME_VALUE_PENCE_V1,
  parseSourceMoneyCell,
  classifyNhspMoneyComponents,
  parseSourceFixedExpenseCell,
} from './money.js';
export { ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS, WEEKLY_SOURCE_PROFILE_IDS } from './profiles.js';
export { WEEKLY_SHIFT_CALCULATOR_VERSION, canonicalWeeklyShiftFinancialSegment } from './weekly-rate-owner.js';
export { compareWeeklySourceShiftPrice } from './source-price-comparator.js';
export {
  WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_SCHEMA_VERSION,
  buildWeeklySourceCanonicalEconomicSnapshot,
} from './economic-snapshot.js';
export {
  WEEKLY_PROTECTED_TARGET_SCHEDULE_VERSION,
  composeWeeklyProtectedTargetSchedule,
} from './protected-target-schedule.js';
export { WEEKLY_MANAGER_EMAIL_POLICY, renderWeeklyManagerQueryEmail } from './manager-email.js';
export { qualifyWeeklySourceContract } from './contract-qualification.js';
