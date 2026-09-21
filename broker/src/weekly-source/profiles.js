import { failParser } from './errors.js';
import { cellText, normaliseHeader } from './shared.js';

export const WEEKLY_SOURCE_PROFILE_IDS = Object.freeze({
  NHSP_PREFINAL_RELEASED_V1: 'NHSP_PREFINAL_RELEASED_V1',
  NHSP_FINAL_BACKING_V1: 'NHSP_FINAL_BACKING_V1',
  HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1: 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
  HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1: 'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
  ROSTER_WEEKLY_SUMMARY_ACTUAL_V1: 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',
});

const PROFILE_VERSION = '1';

const HEALTHROSTER_LAYOUT_A_HEADERS = Object.freeze([
  'Request Id', 'Staff', 'Date', 'From', 'To', 'Break', 'Start', 'End', 'Actual Break', 'Hours',
  'Finalised Date', 'Timesheet Finalised By',
]);

const HEALTHROSTER_LAYOUT_B_HEADERS = Object.freeze([
  'Request Id', 'Status', 'Staff', 'Date', 'Start', 'End', 'Actual Start', 'Actual End',
  'Actual Break', 'Actual Hours', 'Timesheet Finalised By',
]);

export const ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS = Object.freeze([
  'grand_parent_business_unit_name', 'parent_business_unit_name', 'business_unit_name', 'Billing Group Name',
  'Client Weekend Date', 'Weekend Date', 'Custom Code 1', 'Custom Code 2', 'Custom Code 3', 'Job Category',
  'Vat Option', 'Permanent Equivalent Rates', 'Candidate', 'Payroll Number', 'Vacancy Id', 'Booking Id',
  'Timesheet Id', 'Agency', 'Agency Location', 'Candidate Uid', 'Candidate Id', 'TNA Reference', 'Approved By',
  'Approved Date', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'Bonus',
  'Bonus NI', 'Expenses', 'Total Hours', 'Total Cost', 'Line ID', 'STD Hours', 'STD Unit Cost', 'STD Sub Cost',
  'OT Hours', 'OT Unit Cost', 'OT Sub Cost', 'SAT Hours', 'SAT Unit Cost', 'SAT Sub Cost', 'SUN Hours',
  'SUN Unit Cost', 'SUN Sub Cost', 'BH Hours', 'BH Unit Cost', 'BH Sub Cost', 'Nmw-Midweek-Adj/sat Hours',
  'Nmw-Midweek-Adj/sat Unit Cost', 'Nmw-Midweek-Adj/sat Sub Cost', 'Booking Reference', 'Booking Reason',
  'Supply Type', 'Type', 'Business Unit ID', 'Job ID', 'Job Type', 'Booking Start', 'Booking End', 'Invoice ID',
]);

function rosterSummaryColumns(sheet) {
  const headerCells = sheet.rows.find((row) => row.physicalRow === 1)?.cells ?? [];
  if (headerCells.length !== ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.length) return null;
  if (!ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.every((header, index) => {
    const cell = sheet.getCell(1, index + 1);
    return cell?.kind === 'STRING' && cellText(cell) === header;
  })) return null;
  return Object.freeze({
    grandParentBusinessUnitName: 1,
    parentBusinessUnitName: 2,
    businessUnitName: 3,
    clientWeekendDate: 5,
    weekendDate: 6,
    candidate: 13,
    payrollNumber: 14,
    bookingId: 16,
    timesheetId: 17,
    candidateUid: 20,
    candidateId: 21,
    expenses: 34,
    totalHours: 35,
    lineId: 37,
    bookingReference: 56,
    supplyType: 58,
    businessUnitId: 60,
    jobId: 61,
    jobType: 62,
    bookingStart: 63,
    bookingEnd: 64,
  });
}

function uniqueHeaderColumn(sheet, row, expected) {
  const matches = (sheet.rows.find((candidate) => candidate.physicalRow === row)?.cells ?? [])
    .filter((cell) => cell.kind === 'STRING' && normaliseHeader(cellText(cell)) === normaliseHeader(expected))
    .map((cell) => cell.columnIndex);
  return matches.length === 1 ? matches[0] : null;
}

function nhspHeaderColumns(sheet, final) {
  const scalarHeaders = final
    ? {
      date: 'Date', referenceNumber: 'Ref Num', workerName: 'Agency Worker Name',
      workerUniqueId: 'Agency Worker Unique Id', trust: 'Trust', ward: 'Ward', assignment: 'Assignment',
      commission: 'Commission', fmc: 'FMC', totalCost: 'Total Cost', rate: 'Rate',
    }
    : {
      date: 'Date', referenceNumber: 'Ref Num', workerName: 'Agency Worker Name',
      workerUniqueId: 'Agency Worker Unique Id', trust: 'Trust', ward: 'Ward', assignment: 'Assignment',
      commission: 'Commission', totalCost: 'Total Cost',
    };
  const columns = {};
  for (const [name, header] of Object.entries(scalarHeaders)) {
    columns[name] = uniqueHeaderColumn(sheet, 2, header);
    if (!columns[name]) return null;
  }
  const contractGroup = uniqueHeaderColumn(sheet, 2, 'Contract');
  const actualGroup = uniqueHeaderColumn(sheet, 2, 'Actual');
  if (!contractGroup || !actualGroup || contractGroup === actualGroup) return null;
  const sequence = ['Start', 'End', 'Break In Minutes', 'Total'];
  if (!sequence.every((header, offset) => {
    const cell = sheet.getCell(3, contractGroup + offset);
    return cell?.kind === 'STRING' && normaliseHeader(cellText(cell)) === normaliseHeader(header);
  })) return null;
  if (!sequence.every((header, offset) => {
    const cell = sheet.getCell(3, actualGroup + offset);
    return cell?.kind === 'STRING' && normaliseHeader(cellText(cell)) === normaliseHeader(header);
  })) return null;
  Object.assign(columns, {
    contractStart: contractGroup,
    contractEnd: contractGroup + 1,
    contractBreak: contractGroup + 2,
    contractTotal: contractGroup + 3,
    actualStart: actualGroup,
    actualEnd: actualGroup + 1,
    actualBreak: actualGroup + 2,
    actualTotal: actualGroup + 3,
  });
  if (new Set(Object.values(columns)).size !== Object.values(columns).length) return null;
  return Object.freeze(columns);
}

function parseNhspFinalTitle(value) {
  const title = String(value ?? '').replace(/^[ \t\u00a0]+|[ \t\u00a0]+$/g, '');
  const match = /^Agency Backing Report ([1-9]\d{0,18}) for (.+) Agency$/.exec(title);
  if (!match || !match[2] || Array.from(match[2]).length > 200 || /[\u0000-\u001f\u007f]/u.test(match[2])) return null;
  return { reportNumber: match[1], reportHeadingName: match[2] };
}

function trimNhspOuterWhitespace(value) {
  return String(value ?? '').replace(/^[ \t\u00a0]+|[ \t\u00a0]+$/g, '');
}

function headerMapForRow(sheet, row) {
  const map = new Map();
  const physical = sheet.rows.find((candidate) => candidate.physicalRow === row);
  for (const cell of physical?.cells ?? []) {
    if (cell.kind !== 'STRING') continue;
    const key = normaliseHeader(cellText(cell));
    if (!key) continue;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(cell.columnIndex);
  }
  return map;
}

function exactUniqueColumns(headerMap, requiredHeaders) {
  const resolved = {};
  for (const header of requiredHeaders) {
    const candidates = headerMap.get(normaliseHeader(header)) ?? [];
    if (candidates.length !== 1) return null;
    resolved[header] = candidates[0];
  }
  return resolved;
}

function findHealthRosterCandidates(sheet, requiredHeaders, sequence) {
  const results = [];
  for (const row of sheet.rows) {
    const headerMap = headerMapForRow(sheet, row.physicalRow);
    if (!headerMap.has(normaliseHeader('Request Id'))) continue;
    const columns = exactUniqueColumns(headerMap, requiredHeaders);
    if (!columns) continue;
    if (sequence.length > 0) {
      const first = columns[sequence[0]];
      if (!sequence.every((header, index) => columns[header] === first + index)) continue;
    }
    results.push({ headerRow: row.physicalRow, columns });
  }
  return results;
}

function mapHealthRosterColumns(layoutId, exactColumns) {
  if (layoutId === WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1) {
    return {
      requestId: exactColumns['Request Id'],
      workerName: exactColumns.Staff,
      date: exactColumns.Date,
      plannedStart: exactColumns.From,
      plannedEnd: exactColumns.To,
      plannedBreak: exactColumns.Break,
      actualStart: exactColumns.Start,
      actualEnd: exactColumns.End,
      actualBreak: exactColumns['Actual Break'],
      actualTotal: exactColumns.Hours,
      finalisedDate: exactColumns['Finalised Date'],
      finalisedBy: exactColumns['Timesheet Finalised By'],
    };
  }
  return {
    requestId: exactColumns['Request Id'],
    status: exactColumns.Status,
    workerName: exactColumns.Staff,
    date: exactColumns.Date,
    plannedStart: exactColumns.Start,
    plannedEnd: exactColumns.End,
    actualStart: exactColumns['Actual Start'],
    actualEnd: exactColumns['Actual End'],
    actualBreak: exactColumns['Actual Break'],
    actualTotal: exactColumns['Actual Hours'],
    finalisedBy: exactColumns['Timesheet Finalised By'],
  };
}

function allProfileCandidates(evidence) {
  const candidates = [];
  for (const sheet of evidence.sheets) {
    const a1Cell = sheet.getCell(1, 1);
    const a1 = a1Cell?.kind === 'STRING' ? trimNhspOuterWhitespace(a1Cell.text) : '';
    const prefinalColumns = nhspHeaderColumns(sheet, false);
    if (a1 === 'Timesheets Previously Released' && prefinalColumns) {
      candidates.push({
        profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
        sheet,
        headerRow: 3,
        columns: prefinalColumns,
        metadata: {},
      });
    }
    const finalTitle = parseNhspFinalTitle(a1);
    const finalColumns = nhspHeaderColumns(sheet, true);
    if (finalTitle && finalColumns) {
      candidates.push({
        profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
        sheet,
        headerRow: 3,
        columns: finalColumns,
        metadata: finalTitle,
      });
    }

    for (const candidate of findHealthRosterCandidates(
      sheet,
      HEALTHROSTER_LAYOUT_A_HEADERS,
      ['From', 'To', 'Break', 'Start', 'End', 'Actual Break', 'Hours'],
    )) {
      candidates.push({
        profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
        sheet,
        headerRow: candidate.headerRow,
        columns: mapHealthRosterColumns(WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1, candidate.columns),
        metadata: {},
      });
    }
    for (const candidate of findHealthRosterCandidates(
      sheet,
      HEALTHROSTER_LAYOUT_B_HEADERS,
      [],
    )) {
      candidates.push({
        profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1,
        sheet,
        headerRow: candidate.headerRow,
        columns: mapHealthRosterColumns(WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1, candidate.columns),
        metadata: {},
      });
    }
    if (evidence.sourceKind === 'CSV') {
      const columns = rosterSummaryColumns(sheet);
      if (columns) {
        candidates.push({
          profileId: WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1,
          sheet,
          headerRow: 1,
          columns,
          metadata: {},
        });
      }
    }
  }
  return candidates;
}

export function resolveWeeklySourceProfile(evidence, options = {}) {
  const profileId = options.profileId;
  if (!Object.values(WEEKLY_SOURCE_PROFILE_IDS).includes(profileId)) {
    failParser('APPROVED_PROFILE_REQUIRED', 'An exact approved weekly source profile is required.', { profileId: profileId ?? null });
  }
  const candidates = allProfileCandidates(evidence);
  if (candidates.length === 0) {
    failParser('SOURCE_PROFILE_NOT_RECOGNISED', 'No exact approved worksheet and header profile matched the source file.', { profileId });
  }
  if (candidates.length !== 1) {
    failParser('SOURCE_PROFILE_AMBIGUOUS', 'The source file contains more than one qualifying worksheet or profile.', {
      candidateCount: candidates.length,
      candidateProfiles: candidates.map((candidate) => candidate.profileId),
    });
  }
  const selected = candidates[0];
  if (selected.profileId !== profileId) {
    failParser('SOURCE_PROFILE_MISMATCH', 'The supplied file does not match the explicitly approved source profile.', {
      expectedProfileId: profileId,
      detectedProfileId: selected.profileId,
    });
  }
  if (profileId === WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1) {
    const expectedHeading = trimNhspOuterWhitespace(options.configuredNhspReportHeadingName);
    if (!expectedHeading) {
      failParser('NHSP_REPORT_HEADING_REQUIRED', 'The configured NHSP report heading name is required for a final backing report.');
    }
    if (selected.metadata.reportHeadingName !== expectedHeading) {
      failParser('NHSP_REPORT_HEADING_MISMATCH', 'The final backing report heading does not match the configured source group.', {
        reportNumber: selected.metadata.reportNumber,
      });
    }
  }
  return {
    ...selected,
    profileVersion: PROFILE_VERSION,
  };
}

export function publicColumnMap(columns) {
  return Object.fromEntries(Object.entries(columns).map(([name, index]) => [name, {
    index,
    column: XLSX_COLUMN_NAME(index),
  }]));
}

function XLSX_COLUMN_NAME(oneBasedIndex) {
  let current = oneBasedIndex;
  let result = '';
  while (current > 0) {
    const remainder = (current - 1) % 26;
    result = String.fromCharCode(65 + remainder) + result;
    current = Math.floor((current - 1) / 26);
  }
  return result;
}
