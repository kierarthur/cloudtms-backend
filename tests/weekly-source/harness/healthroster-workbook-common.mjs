import {
  excelDateSerial,
  excelDateTimeSerial,
  excelDurationSerial,
  excelTimeSerial,
  makeWorksheet,
  setCellFormat,
  setWorkbookColumnWidths,
  SourceFixtureError,
  writeWorkbookArtifact
} from './workbook-fixture-utils.mjs';
import { HEALTHROSTER_LAYOUT_A_HEADERS, HEALTHROSTER_LAYOUT_B_HEADERS } from './source-profile-layouts.mjs';

function blankRow(headers) {
  return Array(headers.length).fill(null);
}

function set(row, headers, name, value) {
  const index = headers.indexOf(name);
  if (index < 0) throw new SourceFixtureError('HEALTHROSTER_COLUMN_MISSING', `Fixture layout is missing ${name}`);
  row[index] = value;
}

function weekday(date) {
  const [year, month, day] = date.split('-').map(Number);
  return ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][new Date(Date.UTC(year, month - 1, day)).getUTCDay()];
}

function requestId(row, ordinal) {
  return row.requestId || `9${String(ordinal + 1).padStart(9, '0')}`;
}

function populatedActual(value, converter) {
  return value === null || value === undefined ? null : converter(value);
}

function layoutARow({ row, candidate, client, ordinal, mutations }) {
  const headers = HEALTHROSTER_LAYOUT_A_HEADERS;
  const output = blankRow(headers);
  set(output, headers, 'Request Id', requestId(row, ordinal));
  set(output, headers, 'Staff', candidate.displayName);
  set(output, headers, 'Agency', 'Scenario Agency');
  set(output, headers, 'Year', Number(row.workDate.slice(0, 4)));
  set(output, headers, 'Date', excelDateSerial(row.workDate));
  set(output, headers, 'Unit', mutations.has('CROSS_CLIENT') ? `${client.name} other` : client.name);
  set(output, headers, 'Location Name', `Location ${ordinal + 1}`);
  set(output, headers, 'Grade', row.band || 'Band 5');
  set(output, headers, 'Assignment Id', row.contractKey || `assignment_${ordinal + 1}`);
  // Planned values are deliberate decoys. Only Start/End/Actual Break/Hours
  // are actual facts in this locked layout.
  set(output, headers, 'From', excelTimeSerial('06:00'));
  set(output, headers, 'To', excelTimeSerial('14:00'));
  set(output, headers, 'Break', 60);
  set(output, headers, 'Start', populatedActual(row.actualStart, excelTimeSerial));
  set(output, headers, 'End', populatedActual(row.actualEnd, excelTimeSerial));
  set(output, headers, 'Actual Break', row.actualBreakMinutes ?? null);
  set(output, headers, 'Hours', populatedActual(row.actualWorkedMinutes, excelDurationSerial));
  set(output, headers, 'Original Shift Duration', excelDurationSerial(420));
  set(output, headers, 'Staff Group', row.role || 'Nursing');
  // Financial exports are populated with obvious decoys to prove that no
  // HealthRoster monetary column is authoritative.
  set(output, headers, 'Estimated Cost', 999.91);
  set(output, headers, 'Indicative Cost', 999.92);
  set(output, headers, 'Actual Cost', 999.93);
  set(output, headers, 'Agency Worker Pay', 999.94);
  const finalised = row.finalisation === 'FINALISED';
  const inconsistent = row.finalisation === 'INCONSISTENT' || mutations.has('MIXED_FINALISATION');
  if (finalised || inconsistent) set(output, headers, 'Finalised Date', excelDateTimeSerial(row.workDate, '12:00'));
  if (finalised && !inconsistent) set(output, headers, 'Timesheet Finalised By', row.finalisedBy || 'Scenario Finaliser');
  return output;
}

function layoutBRow({ row, candidate, client, ordinal, mutations }) {
  const headers = HEALTHROSTER_LAYOUT_B_HEADERS;
  const output = blankRow(headers);
  set(output, headers, 'Request Id', requestId(row, ordinal));
  const finalised = row.finalisation === 'FINALISED';
  const inconsistent = row.finalisation === 'INCONSISTENT' || mutations.has('MIXED_FINALISATION');
  set(output, headers, 'Status', finalised || inconsistent ? 'Timesheet Finalised' : (row.statusText || 'Informed Agency'));
  set(output, headers, 'Date', excelDateSerial(row.workDate));
  set(output, headers, 'Day', weekday(row.workDate));
  set(output, headers, 'Shift', `Shift ${ordinal + 1}`);
  // These early Start/End values are planned and deliberately disagree with
  // the actual fields selected by the locked profile.
  set(output, headers, 'Start', excelTimeSerial('06:00'));
  set(output, headers, 'End', excelTimeSerial('14:00'));
  set(output, headers, 'Trust', mutations.has('CROSS_CLIENT') ? `${client.name} other` : client.name);
  set(output, headers, 'Unit', `Unit ${ordinal + 1}`);
  set(output, headers, 'Unit Description', `Unit description ${ordinal + 1}`);
  set(output, headers, 'Location Name', `Location ${ordinal + 1}`);
  set(output, headers, 'Staff Group', row.role || 'Nursing');
  set(output, headers, 'Booked Grade', row.band || 'Band 5');
  set(output, headers, 'Request Grade', row.band || 'Band 5');
  set(output, headers, 'Agency', 'Scenario Agency');
  set(output, headers, 'Staff', candidate.displayName);
  set(output, headers, 'Assignment Number', row.contractKey || `assignment_${ordinal + 1}`);
  set(output, headers, 'Booked Hours', excelDurationSerial(420));
  set(output, headers, 'Indicative Cost', 999.91);
  set(output, headers, 'Actual Cost', 999.92);
  set(output, headers, 'Estimated Cost', 999.93);
  set(output, headers, 'Actual Start', populatedActual(row.actualStart, excelTimeSerial));
  set(output, headers, 'Actual End', populatedActual(row.actualEnd, excelTimeSerial));
  set(output, headers, 'Actual Break', row.actualBreakMinutes ?? null);
  set(output, headers, 'Original Grade', row.band || 'Band 5');
  set(output, headers, 'Actual Hours', populatedActual(row.actualWorkedMinutes, excelDurationSerial));
  set(output, headers, 'Agency Worker Pay', 999.94);
  set(output, headers, 'Year', Number(row.workDate.slice(0, 4)));
  if (finalised && !inconsistent) set(output, headers, 'Timesheet Finalised By', row.finalisedBy || 'Scenario Finaliser');
  return output;
}

function applyRequestIdMutations(rows, headers, mutationSet) {
  const requestIndex = headers.indexOf('Request Id');
  if (mutationSet.has('BLANK_REQUEST_ID') && rows[0]) rows[0][requestIndex] = null;
  if (mutationSet.has('DUPLICATE_REQUEST_ID') && rows[0]) {
    if (rows[1]) rows[1][requestIndex] = rows[0][requestIndex];
    else rows.push([...rows[0]]);
  }
}

export function buildHealthRosterWorkbook({ scenario, upload, client, candidates, layout }) {
  const mutationSet = new Set(upload.mutations || []);
  const headers = [...(layout === 'A' ? HEALTHROSTER_LAYOUT_A_HEADERS : HEALTHROSTER_LAYOUT_B_HEADERS)];
  if (mutationSet.has('WRONG_HEADER')) {
    const actualIndex = headers.indexOf(layout === 'A' ? 'Actual Break' : 'Actual Hours');
    headers[actualIndex] = `Unexpected ${headers[actualIndex]}`;
  }
  const canonicalHeaders = layout === 'A' ? HEALTHROSTER_LAYOUT_A_HEADERS : HEALTHROSTER_LAYOUT_B_HEADERS;
  const dataRows = upload.physicalRows.map((row, ordinal) => (layout === 'A' ? layoutARow : layoutBRow)({
    row,
    candidate: candidates.get(row.candidateKey),
    client,
    ordinal,
    mutations: mutationSet
  }));
  applyRequestIdMutations(dataRows, canonicalHeaders, mutationSet);
  const worksheet = makeWorksheet([headers, ...dataRows]);
  setWorkbookColumnWidths(worksheet, headers.map((header) => Math.min(32, Math.max(10, String(header).length + 2))));
  for (let ordinal = 0; ordinal < dataRows.length; ordinal += 1) {
    const excelRow = ordinal + 2;
    const dateColumn = XLSXColumn(canonicalHeaders.indexOf('Date'));
    setCellFormat(worksheet, `${dateColumn}${excelRow}`, 'dd-mm-yyyy');
    const timeNames = layout === 'A'
      ? ['From', 'To', 'Start', 'End', 'Hours', 'Original Shift Duration']
      : ['Start', 'End', 'Booked Hours', 'Actual Start', 'Actual End', 'Actual Hours'];
    for (const name of timeNames) {
      const index = canonicalHeaders.indexOf(name);
      if (index >= 0) setCellFormat(worksheet, `${XLSXColumn(index)}${excelRow}`, 'h:mm');
    }
    if (layout === 'A') {
      setCellFormat(worksheet, `${XLSXColumn(canonicalHeaders.indexOf('Finalised Date'))}${excelRow}`, 'dd-mm-yyyy hh:mm');
    }
  }
  const sheetName = mutationSet.has('WRONG_SHEET') ? 'Unexpected' : 'Export';
  return writeWorkbookArtifact({
    profile: upload.profile,
    fileName: `${upload.key}.xlsx`,
    sheetName,
    worksheet
  });
}

function XLSXColumn(index) {
  let value = index + 1;
  let output = '';
  while (value > 0) {
    const remainder = (value - 1) % 26;
    output = String.fromCharCode(65 + remainder) + output;
    value = Math.floor((value - 1) / 26);
  }
  return output;
}
