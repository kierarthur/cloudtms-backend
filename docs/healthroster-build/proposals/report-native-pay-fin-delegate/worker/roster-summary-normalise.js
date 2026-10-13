import { parseSourceFixedExpenseCell } from './money.js';
import { boundedCellEvidence, cellText, isBlankCell, serialiseIssue, stableSortByPhysicalRow } from './shared.js';

function requiredText(cell) {
  if (!cell || isBlankCell(cell)) return null;
  return cellText(cell);
}

function parseDecimalHours(cell) {
  const token = requiredText(cell);
  if (!token || !/^\d+(?:\.\d+)?$/.test(token)) return { ok: false, code: 'TOTAL_HOURS_INVALID' };
  const [whole, fraction = ''] = token.split('.');
  const scale = 10n ** BigInt(fraction.length);
  const numerator = ((BigInt(whole) * scale) + BigInt(fraction || '0')) * 60n;
  if (numerator % scale !== 0n) return { ok: false, code: 'TOTAL_HOURS_NOT_WHOLE_MINUTES' };
  const minutes = numerator / scale;
  if (minutes > 7n * 24n * 60n) return { ok: false, code: 'TOTAL_HOURS_OUT_OF_RANGE' };
  return { ok: true, minutes: Number(minutes) };
}

function parseLocalTimestamp(cell) {
  const token = requiredText(cell);
  const match = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/.exec(token ?? '');
  if (!match || Number(match[4]) > 23 || Number(match[5]) > 59 || Number(match[6]) > 59) return null;
  const values = match.slice(1).map(Number);
  const pseudoUtc = Date.UTC(values[0], values[1] - 1, values[2], values[3], values[4], values[5]);
  const date = new Date(pseudoUtc);
  if (
    date.getUTCFullYear() !== values[0] || date.getUTCMonth() !== values[1] - 1 || date.getUTCDate() !== values[2]
    || date.getUTCHours() !== values[3] || date.getUTCMinutes() !== values[4] || date.getUTCSeconds() !== values[5]
  ) return null;
  return {
    localDateTime: `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}`,
    workDate: `${match[1]}-${match[2]}-${match[3]}`,
    orderingValue: pseudoUtc,
  };
}

export function normaliseRosterSummaryEvidence(evidence, profile) {
  const { sheet, columns } = profile;
  const rows = [];
  const fatalErrors = [];
  const warnings = [];
  const lineIds = new Map();
  for (let physicalRow = 2; physicalRow <= (sheet.lastPhysicalRow ?? 1); physicalRow += 1) {
    const lineId = requiredText(sheet.getCell(physicalRow, columns.lineId));
    const candidate = requiredText(sheet.getCell(physicalRow, columns.candidate));
    const businessUnitId = requiredText(sheet.getCell(physicalRow, columns.businessUnitId));
    const start = parseLocalTimestamp(sheet.getCell(physicalRow, columns.bookingStart));
    const end = parseLocalTimestamp(sheet.getCell(physicalRow, columns.bookingEnd));
    const hours = parseDecimalHours(sheet.getCell(physicalRow, columns.totalHours));
    const elapsedMinutes = start && end ? (end.orderingValue - start.orderingValue) / 60000 : null;
    if (!lineId) fatalErrors.push(serialiseIssue('ROSTER_SUMMARY_LINE_ID_REQUIRED', physicalRow, null, 'Line ID is required.'));
    else if (lineIds.has(lineId)) fatalErrors.push(serialiseIssue('ROSTER_SUMMARY_DUPLICATE_LINE_ID', physicalRow, null, 'Line ID must be unique within the source file.', { firstPhysicalRow: lineIds.get(lineId) }));
    else lineIds.set(lineId, physicalRow);
    if (!candidate) fatalErrors.push(serialiseIssue('ROSTER_SUMMARY_CANDIDATE_REQUIRED', physicalRow, null, 'Candidate is required.'));
    if (!businessUnitId) fatalErrors.push(serialiseIssue('ROSTER_SUMMARY_BUSINESS_UNIT_REQUIRED', physicalRow, null, 'Business Unit ID is required.'));
    if (!start || !end || end.orderingValue <= start?.orderingValue) {
      fatalErrors.push(serialiseIssue('ROSTER_SUMMARY_BOOKING_INTERVAL_INVALID', physicalRow, null, 'Booking Start and Booking End must form a valid forward local interval.'));
    }
    if (!hours.ok) fatalErrors.push(serialiseIssue(`ROSTER_SUMMARY_${hours.code}`, physicalRow, null, 'Total Hours must be a non-negative whole-minute value.'));
    if (
      start && end && hours.ok
      && (!Number.isSafeInteger(elapsedMinutes) || hours.minutes > elapsedMinutes)
    ) {
      fatalErrors.push(serialiseIssue(
        'ROSTER_SUMMARY_WORKED_DURATION_INVALID',
        physicalRow,
        null,
        'Total Hours cannot exceed the whole-minute Booking interval.',
      ));
    }
    let expense = null;
    try {
      expense = parseSourceFixedExpenseCell(sheet.getCell(physicalRow, columns.expenses));
    } catch (error) {
      fatalErrors.push(serialiseIssue(error.code ?? 'SOURCE_EXPENSE_INVALID', physicalRow, null, error.message));
    }
    rows.push({
      worksheetIndex: 0,
      physicalRow,
      sourceRowId: `worksheet:0:row:${physicalRow}`,
      rowKind: hours.ok && hours.minutes === 0 ? 'SOURCE_ZERO' : 'SOURCE_SHIFT',
      lineId,
      candidate,
      candidateUid: requiredText(sheet.getCell(physicalRow, columns.candidateUid)),
      candidateId: requiredText(sheet.getCell(physicalRow, columns.candidateId)),
      payrollNumber: requiredText(sheet.getCell(physicalRow, columns.payrollNumber)),
      bookingId: requiredText(sheet.getCell(physicalRow, columns.bookingId)),
      timesheetId: requiredText(sheet.getCell(physicalRow, columns.timesheetId)),
      clientScope: {
        businessUnitId,
        grandParentBusinessUnitName: requiredText(sheet.getCell(physicalRow, columns.grandParentBusinessUnitName)),
        parentBusinessUnitName: requiredText(sheet.getCell(physicalRow, columns.parentBusinessUnitName)),
        businessUnitName: requiredText(sheet.getCell(physicalRow, columns.businessUnitName)),
      },
      workDate: start?.workDate ?? null,
      wholeShiftInputs: {
        bookingStartLocal: start?.localDateTime ?? null,
        bookingEndLocal: end?.localDateTime ?? null,
        paidMinutes: hours.ok ? hours.minutes : null,
        breakMinutes: Number.isSafeInteger(elapsedMinutes) && hours.ok
          ? elapsedMinutes - hours.minutes
          : null,
        sourceEvidence: {
          bookingStart: boundedCellEvidence(sheet.getCell(physicalRow, columns.bookingStart), evidence.sourceKind),
          bookingEnd: boundedCellEvidence(sheet.getCell(physicalRow, columns.bookingEnd), evidence.sourceKind),
          totalHours: boundedCellEvidence(sheet.getCell(physicalRow, columns.totalHours), evidence.sourceKind),
        },
      },
      sourceFixedExpense: {
        ...(expense ?? { pence: null, state: 'INVALID' }),
        sourceEvidence: boundedCellEvidence(sheet.getCell(physicalRow, columns.expenses), evidence.sourceKind, 128),
      },
      bookingReference: requiredText(sheet.getCell(physicalRow, columns.bookingReference)),
      jobId: requiredText(sheet.getCell(physicalRow, columns.jobId)),
      jobType: requiredText(sheet.getCell(physicalRow, columns.jobType)),
      supplyType: requiredText(sheet.getCell(physicalRow, columns.supplyType)),
    });
  }
  if (rows.length === 0) fatalErrors.push(serialiseIssue('ROSTER_SUMMARY_NO_DATA_ROWS', null, null, 'The weekly source contains no data rows.'));
  return {
    rows: stableSortByPhysicalRow(rows),
    presentationRows: [],
    scope: { businessUnitIds: [...new Set(rows.map((row) => row.clientScope.businessUnitId).filter(Boolean))] },
    trailers: null,
    rowCounts: {
      total: rows.length,
      sourceShifts: rows.filter((row) => row.rowKind === 'SOURCE_SHIFT').length,
      sourceZero: rows.filter((row) => row.rowKind === 'SOURCE_ZERO').length,
      sourceExpensePresent: rows.filter((row) => row.sourceFixedExpense.state === 'PRESENT').length,
      sourceExpenseOmittedZero: rows.filter((row) => row.sourceFixedExpense.state === 'OMITTED_ZERO').length,
    },
    warnings,
    fatalErrors,
  };
}
