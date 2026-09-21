import { DeterministicIdentityRegistry } from './deterministic-identities.mjs';
import {
  decimalPoundsFromPence,
  excelDateSerial,
  excelDurationSerial,
  excelTimeSerial,
  makeWorksheet,
  setCellFormat,
  setWorkbookColumnWidths,
  SourceFixtureError,
  writeWorkbookArtifact
} from './workbook-fixture-utils.mjs';
import { NHSP_FINAL_HEADERS, NHSP_PREFINAL_HEADERS, NHSP_SUBHEADERS } from './source-profile-layouts.mjs';

function requireActual(row) {
  if (!row.actualStart || !row.actualEnd || row.actualBreakMinutes === null || row.actualWorkedMinutes === null) {
    throw new SourceFixtureError('NHSP_ACTUAL_FACTS_REQUIRED', `NHSP row ${row.key} is missing an Actual worked-time fact`);
  }
}

function signedPence(row, mutationSet) {
  if (row.commissionPence === null || row.totalCostPence === null) {
    throw new SourceFixtureError('NHSP_MONEY_REQUIRED', `NHSP row ${row.key} must declare Commission and Total Cost pence`);
  }
  let commission = BigInt(row.commissionPence);
  let totalCost = BigInt(row.totalCostPence);
  if (mutationSet.has('MIXED_SIGN')) {
    commission = commission === 0n ? -1n : -absBigInt(commission);
    totalCost = totalCost === 0n ? 1n : absBigInt(totalCost);
  }
  if (mutationSet.has('PRICE_MISMATCH')) totalCost += totalCost < 0n ? -2n : 2n;
  if (!mutationSet.has('MIXED_SIGN') && !mutationSet.has('NEGATIVE_ZERO')) {
    const sum = commission + totalCost;
    if (row.sign === 'POSITIVE' && (commission < 0n || totalCost < 0n || sum <= 0n)) {
      throw new SourceFixtureError('NHSP_SIGN_INCONSISTENT', `Positive NHSP row ${row.key} has non-positive money`);
    }
    if (row.sign === 'FULL_NEGATIVE' && (commission > 0n || totalCost > 0n || sum >= 0n)) {
      throw new SourceFixtureError('NHSP_SIGN_INCONSISTENT', `Full-negative NHSP row ${row.key} has non-negative money`);
    }
    if (row.sign === 'ZERO' && (commission !== 0n || totalCost !== 0n)) {
      throw new SourceFixtureError('NHSP_SIGN_INCONSISTENT', `Zero NHSP row ${row.key} has non-zero money`);
    }
  }
  return { commission, totalCost };
}

function absBigInt(value) {
  return value < 0n ? -value : value;
}

function sourceReference(registry, row, ordinal) {
  if (row.requestId) return row.requestId;
  return registry.externalKey('nhsp-reference', ordinal, { prefix: 'NHSP', maxLength: 40 });
}

function makeDataRows({ scenario, upload, client, candidates, final }) {
  const mutationSet = new Set(upload.mutations || []);
  const registry = new DeterministicIdentityRegistry(scenario.scenarioId);
  const rows = [];
  const totals = [];
  upload.physicalRows.forEach((row, ordinal) => {
    requireActual(row);
    const candidate = candidates.get(row.candidateKey);
    const money = signedPence(row, mutationSet);
    totals.push(money.totalCost);
    const contractStart = excelTimeSerial('06:00');
    const contractEnd = excelTimeSerial('14:00');
    const contractBreak = 60;
    const contractTotal = excelDurationSerial(420);
    const actualStart = excelTimeSerial(row.actualStart);
    const actualEnd = excelTimeSerial(row.actualEnd);
    const actualTotal = excelDurationSerial(row.actualWorkedMinutes);
    const commissionValue = mutationSet.has('NEGATIVE_ZERO') && ordinal === 0
      ? '-0.00'
      : decimalPoundsFromPence(money.commission.toString());
    const base = [
      excelDateSerial(row.workDate),
      sourceReference(registry, row, ordinal),
      candidate.displayName,
      candidate.tmsRef,
      mutationSet.has('CROSS_CLIENT') ? `${client.name} - other Trust` : (upload.trustName || client.name),
      `Ward ${ordinal + 1}`,
      row.band || row.role || 'Test assignment',
      contractStart,
      contractEnd,
      contractBreak,
      contractTotal,
      actualStart,
      actualEnd,
      row.actualBreakMinutes,
      actualTotal,
      commissionValue
    ];
    if (final) base.push(0);
    base.push(decimalPoundsFromPence(money.totalCost.toString()));
    if (final) base.push('Basic');
    rows.push(base);
    rows.push(['', '', '', '', '', `Service ${ordinal + 1}`]);
  });
  return { rows, totalCostPence: totals.reduce((sum, value) => sum + value, 0n) };
}

function mergesFor(final, physicalRowCount) {
  const lastColumn = final ? 18 : 16;
  const merges = [
    { s: { r: 0, c: 0 }, e: { r: 0, c: lastColumn } },
    { s: { r: 1, c: 7 }, e: { r: 1, c: 10 } },
    { s: { r: 1, c: 11 }, e: { r: 1, c: 14 } }
  ];
  for (let c = 0; c <= lastColumn; c += 1) {
    if (c < 7 || c > 14) merges.push({ s: { r: 1, c }, e: { r: 2, c } });
  }
  for (let ordinal = 0; ordinal < physicalRowCount; ordinal += 1) {
    const first = 3 + ordinal * 2;
    for (let c = 0; c <= lastColumn; c += 1) {
      if (c !== 5) merges.push({ s: { r: first, c }, e: { r: first + 1, c } });
    }
  }
  return merges;
}

export function buildNhspWorkbook({ scenario, upload, client, candidates, final, reportHeadingName = null }) {
  const mutationSet = new Set(upload.mutations || []);
  if (final) {
    if (!upload.trustName) throw new SourceFixtureError('NHSP_TRUST_REQUIRED', 'A final NHSP report must declare one Trust');
    if (!/^[1-9]\d*$/.test(String(upload.reportNumber || ''))) {
      throw new SourceFixtureError('NHSP_REPORT_NUMBER_INVALID', 'A final NHSP report number must contain canonical positive digits');
    }
    if (!reportHeadingName || String(reportHeadingName).trim() !== reportHeadingName || reportHeadingName.length > 200) {
      throw new SourceFixtureError('NHSP_REPORT_HEADING_NAME_REQUIRED', 'A final NHSP fixture requires the configured report-heading Agency name');
    }
  }
  const heading = final
    ? `Agency Backing Report ${upload.reportNumber} for ${reportHeadingName} Agency`
    : 'Timesheets Previously Released';
  const headers = [...(final ? NHSP_FINAL_HEADERS : NHSP_PREFINAL_HEADERS)];
  if (mutationSet.has('WRONG_HEADER')) headers[0] = 'Worked Date';
  const subheaders = [...NHSP_SUBHEADERS];
  while (subheaders.length < headers.length) subheaders.push('');
  const data = makeDataRows({ scenario, upload, client, candidates, final });
  const rows = [[heading], headers, subheaders, ...data.rows];
  if (final) {
    const management = Array(headers.length).fill('');
    management[0] = 'FrameWork Management Charge';
    management[17] = 0;
    const total = Array(headers.length).fill('');
    total[0] = 'Total';
    // The real report footer checks Total Cost only. Commission is deliberately
    // excluded even though Commission + Total Cost is the signed line value.
    total[17] = decimalPoundsFromPence(data.totalCostPence.toString());
    rows.push(management, total);
  }
  const worksheet = makeWorksheet(rows);
  worksheet['!merges'] = mergesFor(final, upload.physicalRows.length);
  setWorkbookColumnWidths(worksheet, final
    ? [12, 18, 28, 22, 35, 24, 20, 10, 10, 18, 10, 10, 10, 18, 10, 14, 10, 14, 12]
    : [12, 18, 28, 22, 35, 24, 20, 10, 10, 18, 10, 10, 10, 18, 10, 14, 14]);
  for (let ordinal = 0; ordinal < upload.physicalRows.length; ordinal += 1) {
    const excelRow = 4 + ordinal * 2;
    setCellFormat(worksheet, `A${excelRow}`, 'm/d/yy');
    for (const column of ['H', 'I', 'K', 'L', 'M', 'O']) setCellFormat(worksheet, `${column}${excelRow}`, 'h:mm');
    for (const column of final ? ['P', 'Q', 'R'] : ['P', 'Q']) setCellFormat(worksheet, `${column}${excelRow}`, '0.00');
  }
  const sheetName = mutationSet.has('WRONG_SHEET') ? 'Unexpected' : 'Export';
  return writeWorkbookArtifact({
    profile: upload.profile,
    fileName: `${upload.key}.xlsx`,
    sheetName,
    worksheet
  });
}
