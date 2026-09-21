import { boundedCellEvidence, cellText, isBlankCell, parseDateCell, serialiseIssue, stableSortByPhysicalRow, validateActualTuple } from './shared.js';

function rowHasContent(sheet, physicalRow) {
  return (sheet.rows.find((row) => row.physicalRow === physicalRow)?.cells ?? []).some((cell) => !isBlankCell(cell));
}

function safeLiteralText(cell) {
  if (!cell || isBlankCell(cell) || ['FORMULA', 'ERROR', 'BOOLEAN'].includes(cell.kind)) return null;
  return cell.kind === 'NUMBER' ? cell.rawToken : cellText(cell);
}

function finalisationState(profile, physicalRow) {
  const { sheet, columns, profileId } = profile;
  const finaliserCell = sheet.getCell(physicalRow, columns.finalisedBy);
  if (finaliserCell && !isBlankCell(finaliserCell) && ['FORMULA', 'ERROR', 'BOOLEAN'].includes(finaliserCell.kind)) {
    return { kind: 'BLOCKED', code: 'FINALISATION_INDICATOR_UNVERIFIABLE' };
  }
  const finaliser = safeLiteralText(finaliserCell);
  if (profileId === 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1') {
    const finalisedDateCell = sheet.getCell(physicalRow, columns.finalisedDate);
    if (finalisedDateCell && !isBlankCell(finalisedDateCell) && ['FORMULA', 'ERROR', 'BOOLEAN'].includes(finalisedDateCell.kind)) {
      return { kind: 'BLOCKED', code: 'FINALISATION_INDICATOR_UNVERIFIABLE' };
    }
    const finalisedDate = safeLiteralText(finalisedDateCell);
    if (Boolean(finalisedDate) !== Boolean(finaliser)) return { kind: 'BLOCKED', code: 'FINALISATION_INDICATORS_DISAGREE' };
    return finalisedDate && finaliser ? { kind: 'FINALISED' } : { kind: 'UNFINALISED' };
  }
  const statusCell = sheet.getCell(physicalRow, columns.status);
  if (statusCell && !isBlankCell(statusCell) && ['FORMULA', 'ERROR', 'BOOLEAN'].includes(statusCell.kind)) {
    return { kind: 'BLOCKED', code: 'FINALISATION_INDICATOR_UNVERIFIABLE' };
  }
  const status = safeLiteralText(statusCell);
  const statusFinalised = status?.toLocaleLowerCase('en-GB') === 'timesheet finalised';
  if (statusFinalised !== Boolean(finaliser)) return { kind: 'BLOCKED', code: 'FINALISATION_INDICATORS_DISAGREE' };
  return statusFinalised ? { kind: 'FINALISED' } : { kind: 'UNFINALISED' };
}

export function normaliseHealthRosterEvidence(evidence, profile, options = {}) {
  const { sheet, columns } = profile;
  const rows = [];
  const fatalErrors = [];
  const warnings = [];
  const requestIds = new Map();

  for (let physicalRow = profile.headerRow + 1; physicalRow <= (sheet.lastPhysicalRow ?? profile.headerRow); physicalRow += 1) {
    if (!rowHasContent(sheet, physicalRow)) continue;
    const requestId = safeLiteralText(sheet.getCell(physicalRow, columns.requestId));
    const workerName = safeLiteralText(sheet.getCell(physicalRow, columns.workerName));
    const dateResult = parseDateCell(sheet.getCell(physicalRow, columns.date), { date1904: evidence.date1904 });
    if (!requestId) fatalErrors.push(serialiseIssue('HEALTHROSTER_REQUEST_ID_REQUIRED', physicalRow, null, 'Request Id is required on every data row.'));
    else if (requestIds.has(requestId)) {
      fatalErrors.push(serialiseIssue('HEALTHROSTER_DUPLICATE_REQUEST_ID', physicalRow, null, 'Request Id must be unique within the upload.', {
        firstPhysicalRow: requestIds.get(requestId),
      }));
    } else requestIds.set(requestId, physicalRow);
    if (!workerName) fatalErrors.push(serialiseIssue('HEALTHROSTER_STAFF_REQUIRED', physicalRow, null, 'Staff is required on every data row.'));
    if (!dateResult.ok) fatalErrors.push(serialiseIssue('HEALTHROSTER_DATE_INVALID', physicalRow, null, 'Date is missing or invalid.'));

    const finalisation = finalisationState(profile, physicalRow);
    const actualCells = {
      start: sheet.getCell(physicalRow, columns.actualStart),
      end: sheet.getCell(physicalRow, columns.actualEnd),
      break: sheet.getCell(physicalRow, columns.actualBreak),
      total: sheet.getCell(physicalRow, columns.actualTotal),
    };
    const actual = validateActualTuple({
      startCell: actualCells.start,
      endCell: actualCells.end,
      breakCell: actualCells.break,
      totalCell: actualCells.total,
      allowExplicitZero: true,
    });

    let rowKind;
    if (finalisation.kind === 'BLOCKED') {
      rowKind = 'BLOCKED';
      fatalErrors.push(serialiseIssue(finalisation.code, physicalRow, null, 'HealthRoster finalisation indicators disagree.'));
    } else if (finalisation.kind === 'UNFINALISED') {
      rowKind = 'UNFINALISED';
    } else if (!actual.ok || actual.kind !== 'WORKED') {
      rowKind = 'BLOCKED';
      fatalErrors.push(serialiseIssue('HEALTHROSTER_FINALISED_ACTUAL_INVALID', physicalRow, null, 'A finalised row has missing or inconsistent Actual hours.', {
        actualCode: actual.kind === 'ZERO' ? 'ACTUAL_ZERO_MARKED_FINALISED' : actual.code,
      }));
    } else rowKind = 'FINALISED_WORKED';

    const sourcePosition = rowKind === 'FINALISED_WORKED'
      ? 'WORKED'
      : finalisation.kind === 'UNFINALISED' && actual.ok && actual.kind === 'ZERO'
        ? 'EXPLICIT_ZERO'
        : finalisation.kind === 'UNFINALISED'
          ? 'NOT_FINALISED'
          : 'BLOCKED';

    rows.push({
      worksheetIndex: sheet.index,
      physicalRow,
      sourceRowId: `worksheet:${sheet.index}:row:${physicalRow}`,
      rowKind,
      sourcePosition,
      requestId,
      workerName,
      finalisedBy: safeLiteralText(sheet.getCell(physicalRow, columns.finalisedBy)),
      date: dateResult.ok ? dateResult.value : null,
      ...(rowKind === 'FINALISED_WORKED' ? {
        actual: {
          start: actual.start,
          end: actual.end,
          breakMinutes: actual.breakMinutes,
          totalMinutes: actual.totalMinutes,
          overnight: actual.overnight,
          sourceEvidence: Object.fromEntries(Object.entries(actualCells).map(([name, cell]) => [name, boundedCellEvidence(cell, evidence.sourceKind)])),
        },
      } : {}),
      ...(sourcePosition === 'EXPLICIT_ZERO' ? {
        zeroEvidence: {
          totalMinutes: 0,
          sourceEvidence: Object.fromEntries(Object.entries(actualCells).map(([name, cell]) => [name, boundedCellEvidence(cell, evidence.sourceKind)])),
        },
      } : {}),
    });
  }
  if (rows.length === 0) fatalErrors.push(serialiseIssue('HEALTHROSTER_NO_DATA_ROWS', null, null, 'The HealthRoster source contains no data rows.'));

  return {
    rows: stableSortByPhysicalRow(rows),
    presentationRows: [],
    scope: { client: options.expectedClient?.trim() || null },
    trailers: null,
    rowCounts: {
      total: rows.length,
      finalisedWorked: rows.filter((row) => row.rowKind === 'FINALISED_WORKED').length,
      unfinalised: rows.filter((row) => row.rowKind === 'UNFINALISED').length,
      explicitZero: rows.filter((row) => row.sourcePosition === 'EXPLICIT_ZERO').length,
      blocked: rows.filter((row) => row.rowKind === 'BLOCKED').length,
    },
    warnings,
    fatalErrors,
  };
}
