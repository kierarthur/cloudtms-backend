import { WeeklySourceParserError } from './errors.js';
import { classifyNhspMoneyComponents, parseSourceMoneyCell } from './money.js';
import {
  boundedCellEvidence,
  cellText,
  isBlankCell,
  parseDateCell,
  serialiseIssue,
  stableSortByPhysicalRow,
  validateActualTuple,
} from './shared.js';

function moneyCellEvidence(evidence, cell, parsed, parseIssue = null) {
  return {
    ...boundedCellEvidence(cell, evidence.sourceKind, 128),
    parseState: parsed && !parseIssue ? 'PARSED' : 'UNVERIFIABLE',
    parsedPence: parsed?.pence ?? null,
    ...(parsed?.normalisation ? { normalisation: parsed.normalisation } : {}),
    ...(parseIssue ? { parseIssue } : {}),
  };
}

function rowCells(sheet, physicalRow) {
  return sheet.rows.find((row) => row.physicalRow === physicalRow)?.cells ?? [];
}

function rowHasContent(sheet, physicalRow) {
  return rowCells(sheet, physicalRow).some((cell) => !isBlankCell(cell));
}

function contentColumns(sheet, physicalRow) {
  return rowCells(sheet, physicalRow).filter((cell) => !isBlankCell(cell)).map((cell) => cell.columnIndex);
}

function requiredLiteralText(cell, label) {
  if (!cell || isBlankCell(cell)) throw new WeeklySourceParserError('NHSP_REQUIRED_VALUE_BLANK', `${label} is blank.`, { coordinate: cell?.coordinate ?? null });
  if (['FORMULA', 'ERROR', 'BOOLEAN'].includes(cell.kind)) {
    throw new WeeklySourceParserError('NHSP_REQUIRED_VALUE_UNVERIFIABLE', `${label} is not a literal value.`, { coordinate: cell.coordinate });
  }
  return cell.kind === 'NUMBER' ? cell.rawToken : cellText(cell);
}

function exactEconomicFingerprint(row) {
  return JSON.stringify([
    row.date,
    row.referenceNumber,
    row.workerName,
    row.workerUniqueId,
    row.trust,
    row.ward,
    row.assignment,
    row.actual.start,
    row.actual.end,
    row.actual.breakMinutes,
    row.actual.totalMinutes,
    row.pricingEvidence.commissionPence,
    row.pricingEvidence.totalCostPence,
    row.pricingEvidence.sourceTotalPence,
    row.physicalSign,
  ]);
}

function parseEconomicRow(evidence, profile, physicalRow, final) {
  const { sheet, columns } = profile;
  const actualCells = {
    start: sheet.getCell(physicalRow, columns.actualStart),
    end: sheet.getCell(physicalRow, columns.actualEnd),
    break: sheet.getCell(physicalRow, columns.actualBreak),
    total: sheet.getCell(physicalRow, columns.actualTotal),
  };
  const dateResult = parseDateCell(sheet.getCell(physicalRow, columns.date), {
    date1904: evidence.date1904,
    // The NHSP browser export is a UK HTML table and writes literal dates as
    // dd/mm/yyyy. The OOXML export uses workbook date cells; retain the
    // established MDY fallback there for any literal legacy value.
    slashOrder: evidence.sourceKind === 'HTML' ? 'DMY' : 'MDY',
  });
  if (!dateResult.ok) {
    throw new WeeklySourceParserError('NHSP_DATE_INVALID', 'NHSP shift Date is invalid.', {
      coordinate: sheet.getCell(physicalRow, columns.date)?.coordinate ?? null,
    });
  }
  const referenceNumber = isBlankCell(sheet.getCell(physicalRow, columns.referenceNumber))
    ? null
    : requiredLiteralText(sheet.getCell(physicalRow, columns.referenceNumber), 'NHSP Ref Num');
  const actual = validateActualTuple({
    startCell: actualCells.start,
    endCell: actualCells.end,
    breakCell: actualCells.break,
    totalCell: actualCells.total,
  });
  if (!actual.ok || actual.kind !== 'WORKED') {
    throw new WeeklySourceParserError('NHSP_ACTUAL_TUPLE_INVALID', 'NHSP Actual Start, End, Break and Total are incomplete or inconsistent.', {
      physicalRow,
      actualCode: actual.code,
    });
  }

  let pricingEvidence;
  let pricingIssue = null;
  const commissionCell = sheet.getCell(physicalRow, columns.commission);
  const totalCostCell = sheet.getCell(physicalRow, columns.totalCost);
  try {
    pricingEvidence = classifyNhspMoneyComponents(
      commissionCell,
      totalCostCell,
    );
  } catch (error) {
    if (final) throw error;
    pricingIssue = error instanceof WeeklySourceParserError ? error.code : 'NHSP_PREFINAL_PRICING_UNVERIFIABLE';
    const parsedComponents = {};
    try { parsedComponents.commission = parseSourceMoneyCell(commissionCell); } catch { /* retained as bounded invalid evidence */ }
    try { parsedComponents.totalCost = parseSourceMoneyCell(totalCostCell); } catch { /* retained as bounded invalid evidence */ }
    pricingEvidence = {
      commissionPence: null,
      totalCostPence: null,
      sourceTotalPence: null,
      physicalSign: null,
      normalisations: [],
      parsedComponents,
    };
  }
  if (final) {
    const fmcCell = sheet.getCell(physicalRow, columns.fmc);
    const fmc = parseSourceMoneyCell(fmcCell);
    if (BigInt(fmc.pence) !== 0n) {
      throw new WeeklySourceParserError('NHSP_FMC_MUST_BE_ZERO', 'NHSP FMC must be exactly zero.', {
        coordinate: sheet.getCell(physicalRow, columns.fmc)?.coordinate ?? null,
      });
      }
    pricingEvidence.parsedComponents.fmc = fmc;
    pricingEvidence.fmcCell = fmcCell;
  }

  const row = {
    worksheetIndex: sheet.index,
    physicalRow,
    sourceRowId: `worksheet:${sheet.index}:row:${physicalRow}`,
    rowKind: final
      ? (pricingEvidence.physicalSign === 'FULL_NEGATIVE' ? 'FULL_REVERSAL' : 'POSITIVE_SOURCE_SHIFT')
      : 'PREFINAL_CHECK_SHIFT',
    date: dateResult.value,
    referenceNumber,
    workerName: requiredLiteralText(sheet.getCell(physicalRow, columns.workerName), 'Agency Worker Name'),
    workerUniqueId: isBlankCell(sheet.getCell(physicalRow, columns.workerUniqueId))
      ? null
      : requiredLiteralText(sheet.getCell(physicalRow, columns.workerUniqueId), 'Agency Worker Unique Id'),
    trust: requiredLiteralText(sheet.getCell(physicalRow, columns.trust), 'Trust'),
    ward: cellText(sheet.getCell(physicalRow, columns.ward)) || null,
    assignment: requiredLiteralText(sheet.getCell(physicalRow, columns.assignment), 'Assignment'),
    actual: {
      start: actual.start,
      end: actual.end,
      breakMinutes: actual.breakMinutes,
      totalMinutes: actual.totalMinutes,
      overnight: actual.overnight,
      sourceEvidence: Object.fromEntries(Object.entries(actualCells).map(([name, cell]) => [name, boundedCellEvidence(cell, evidence.sourceKind)])),
    },
    physicalSign: pricingEvidence.physicalSign,
    pricingEvidence: {
      commissionPence: pricingEvidence.commissionPence,
      totalCostPence: pricingEvidence.totalCostPence,
      sourceTotalPence: pricingEvidence.sourceTotalPence,
      normalisations: pricingEvidence.normalisations,
      components: {
        commission: moneyCellEvidence(evidence, commissionCell, pricingEvidence.parsedComponents.commission, pricingIssue),
        totalCost: moneyCellEvidence(evidence, totalCostCell, pricingEvidence.parsedComponents.totalCost, pricingIssue),
        ...(final ? {
          fmc: moneyCellEvidence(evidence, pricingEvidence.fmcCell, pricingEvidence.parsedComponents.fmc),
        } : {}),
      },
      authority: final ? 'FINAL_SOURCE_INVOICE_EVIDENCE' : 'PREFINAL_CHECK_EVIDENCE_ONLY',
      state: pricingIssue ? 'UNVERIFIABLE' : 'PARSED',
      ...(pricingIssue ? { issueCode: pricingIssue } : {}),
    },
    ...(pricingIssue ? { pricingIssue } : {}),
  };
  return row;
}

export function normaliseNhspEvidence(evidence, profile, options = {}) {
  const final = profile.profileId === 'NHSP_FINAL_BACKING_V1';
  const { sheet } = profile;
  const economicRows = [];
  const presentationRows = [];
  const fatalErrors = [];
  const warnings = [];
  let frameworkTrailer = null;
  let totalTrailer = null;
  let mostRecentEconomicRow = null;

  for (let physicalRow = profile.headerRow + 1; physicalRow <= (sheet.lastPhysicalRow ?? profile.headerRow); physicalRow += 1) {
    if (!rowHasContent(sheet, physicalRow)) continue;
    const first = cellText(sheet.getCell(physicalRow, 1));
    const columnsWithContent = contentColumns(sheet, physicalRow);

    if (final && first === 'FrameWork Management Charge') {
      if (frameworkTrailer) {
        fatalErrors.push(serialiseIssue('NHSP_DUPLICATE_FMC_TRAILER', physicalRow, 'A', 'The final report has more than one framework charge trailer.'));
        continue;
      }
      if (columnsWithContent.some((column) => ![1, profile.columns.totalCost].includes(column))) {
        fatalErrors.push(serialiseIssue('NHSP_FMC_TRAILER_SHAPE_INVALID', physicalRow, 'A', 'The framework charge trailer has unexpected populated cells.'));
        continue;
      }
      try {
        const amount = parseSourceMoneyCell(sheet.getCell(physicalRow, profile.columns.totalCost));
        if (BigInt(amount.pence) !== 0n) throw new WeeklySourceParserError('NHSP_FMC_TRAILER_NOT_ZERO', 'The framework charge trailer is not zero.');
        frameworkTrailer = {
          physicalRow,
          pence: amount.pence,
          evidence: moneyCellEvidence(evidence, sheet.getCell(physicalRow, profile.columns.totalCost), amount),
        };
      } catch (error) {
        fatalErrors.push(serialiseIssue(error.code ?? 'NHSP_FMC_TRAILER_INVALID', physicalRow, 'R', error.message));
      }
      continue;
    }
    if (final && first === 'Total') {
      if (totalTrailer) {
        fatalErrors.push(serialiseIssue('NHSP_DUPLICATE_TOTAL_TRAILER', physicalRow, 'A', 'The final report has more than one Total trailer.'));
        continue;
      }
      if (columnsWithContent.some((column) => ![1, profile.columns.totalCost].includes(column))) {
        fatalErrors.push(serialiseIssue('NHSP_TOTAL_TRAILER_SHAPE_INVALID', physicalRow, 'A', 'The Total trailer has unexpected populated cells.'));
        continue;
      }
      try {
        const amount = parseSourceMoneyCell(sheet.getCell(physicalRow, profile.columns.totalCost));
        totalTrailer = {
          physicalRow,
          totalCostPence: amount.pence,
          evidence: moneyCellEvidence(evidence, sheet.getCell(physicalRow, profile.columns.totalCost), amount),
        };
      } catch (error) {
        fatalErrors.push(serialiseIssue(error.code ?? 'NHSP_TOTAL_TRAILER_INVALID', physicalRow, 'R', error.message));
      }
      continue;
    }

    if (columnsWithContent.length === 1 && columnsWithContent[0] === profile.columns.ward) {
      if (!mostRecentEconomicRow) {
        fatalErrors.push(serialiseIssue('NHSP_ORPHAN_WARD_CONTINUATION', physicalRow, 'F', 'A ward continuation row has no preceding shift row.'));
      } else {
        try {
          presentationRows.push({
            worksheetIndex: sheet.index,
            physicalRow,
            rowKind: 'WARD_CONTINUATION',
            followsPhysicalRow: mostRecentEconomicRow,
            wardContinuation: requiredLiteralText(sheet.getCell(physicalRow, profile.columns.ward), 'NHSP ward continuation'),
          });
        } catch (error) {
          fatalErrors.push(serialiseIssue(
            error instanceof WeeklySourceParserError ? error.code : 'NHSP_WARD_CONTINUATION_INVALID',
            physicalRow,
            'F',
            error instanceof Error ? error.message : 'The NHSP ward continuation is invalid.',
          ));
          mostRecentEconomicRow = null;
        }
      }
      continue;
    }

    if (final && totalTrailer) {
      fatalErrors.push(serialiseIssue('NHSP_CONTENT_AFTER_TOTAL_TRAILER', physicalRow, null, 'The final report contains content after its Total trailer.'));
      continue;
    }
    // A continuation may only follow the immediately preceding economic row
    // (with any number of continuation rows). A malformed economic-looking row
    // must never cause a later ward-only row to attach to an older valid shift.
    mostRecentEconomicRow = null;
    try {
      const parsed = parseEconomicRow(evidence, profile, physicalRow, final);
      economicRows.push(parsed);
      if (parsed.pricingIssue) {
        warnings.push(serialiseIssue(parsed.pricingIssue, physicalRow, null, 'Pre-final pricing evidence could not be verified; hours checking remains available.'));
      }
      mostRecentEconomicRow = physicalRow;
    } catch (error) {
      fatalErrors.push(serialiseIssue(
        error instanceof WeeklySourceParserError ? error.code : 'NHSP_ROW_INVALID',
        physicalRow,
        error?.details?.coordinate?.replace(/\d+$/, '') ?? null,
        error instanceof Error ? error.message : 'The NHSP row is invalid.',
      ));
    }
  }

  if (economicRows.length === 0) fatalErrors.push(serialiseIssue('NHSP_NO_ECONOMIC_ROWS', null, null, 'The NHSP source contains no valid shift rows.'));
  const exactRows = new Map();
  for (const row of economicRows) {
    const fingerprint = exactEconomicFingerprint(row);
    if (exactRows.has(fingerprint)) {
      fatalErrors.push(serialiseIssue('NHSP_EXACT_DUPLICATE_ECONOMIC_ROW', row.physicalRow, null, 'The NHSP source contains an exact duplicate economic row.', {
        firstPhysicalRow: exactRows.get(fingerprint),
      }));
    } else exactRows.set(fingerprint, row.physicalRow);
  }

  const trusts = [...new Set(economicRows.map((row) => row.trust))];
  if (final && trusts.length !== 1) {
    fatalErrors.push(serialiseIssue('NHSP_SINGLE_TRUST_REQUIRED', null, null, 'An NHSP source file must contain exactly one Trust.', { trustCount: trusts.length }));
  }
  if (final && options.expectedTrust && trusts.length === 1 && trusts[0] !== options.expectedTrust.trim()) {
    fatalErrors.push(serialiseIssue('NHSP_TRUST_SCOPE_MISMATCH', null, null, 'The NHSP source Trust does not match the selected source group.'));
  }

  if (final) {
    if (!frameworkTrailer) fatalErrors.push(serialiseIssue('NHSP_FMC_TRAILER_REQUIRED', null, null, 'The final report is missing its framework charge trailer.'));
    if (!totalTrailer) fatalErrors.push(serialiseIssue('NHSP_TOTAL_TRAILER_REQUIRED', null, null, 'The final report is missing its Total trailer.'));
    if (frameworkTrailer && totalTrailer && frameworkTrailer.physicalRow >= totalTrailer.physicalRow) {
      fatalErrors.push(serialiseIssue('NHSP_TRAILER_ORDER_INVALID', totalTrailer.physicalRow, null, 'The final report trailers are in the wrong order.'));
    }
    if (totalTrailer) {
      const calculatedTotalCost = economicRows.reduce((sum, row) => sum + BigInt(row.pricingEvidence.totalCostPence), 0n);
      if (calculatedTotalCost.toString() !== totalTrailer.totalCostPence) {
        fatalErrors.push(serialiseIssue('NHSP_TOTAL_COST_TRAILER_MISMATCH', totalTrailer.physicalRow, 'R', 'The Total trailer does not equal the signed sum of row Total Cost values.'));
      }
    }
  }

  return {
    rows: stableSortByPhysicalRow(economicRows),
    presentationRows: stableSortByPhysicalRow(presentationRows),
    scope: {
      ...(final ? { trust: trusts.length === 1 ? trusts[0] : null } : { trusts }),
      ...(final ? {
        backingReportNumber: profile.metadata.reportNumber,
        reportHeadingName: profile.metadata.reportHeadingName,
      } : {}),
    },
    trailers: final ? { frameworkManagementCharge: frameworkTrailer, total: totalTrailer } : null,
    rowCounts: {
      economic: economicRows.length,
      fullReversals: economicRows.filter((row) => row.rowKind === 'FULL_REVERSAL').length,
      wardContinuations: presentationRows.length,
    },
    warnings,
    fatalErrors,
  };
}
