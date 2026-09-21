import assert from 'node:assert/strict';
import test from 'node:test';
import * as XLSX from 'xlsx';

import { parseWeeklySourceFile, WEEKLY_SOURCE_PROFILE_IDS } from '../../../broker/src/weekly-source/index.js';

function workbookBytes(sheets) {
  const workbook = XLSX.utils.book_new();
  for (const [name, rows] of sheets) XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet(rows), name);
  return XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
}

function nhspHeaders(final) {
  return [
    final
      ? ['Agency Backing Report 123 for Exact Heading Agency']
      : ['Timesheets Previously Released'],
    final
      ? ['Date', 'Ref Num', 'Agency Worker Name', 'Agency Worker Unique Id', 'Trust', 'Ward', 'Assignment', 'Contract', null, null, null, 'Actual', null, null, null, 'Commission', 'FMC', 'Total Cost', 'Rate']
      : ['Date', 'Ref Num', 'Agency Worker Name', 'Agency Worker Unique Id', 'Trust', 'Ward', 'Assignment', 'Contract', null, null, null, 'Actual', null, null, null, 'Commission', 'Total Cost'],
    [null, null, null, null, null, null, null, 'Start', 'End', 'Break In Minutes', 'Total', 'Start', 'End', 'Break In Minutes', 'Total'],
  ];
}

test('NHSP pre-final parses Actual fields, ignores Contract fields and recognises ward continuations', async () => {
  const rows = nhspHeaders(false);
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', 'not-a-time', 'ignored', 'ignored', 'ignored', '09:00', '17:00', 30, '7:30', 52.5, 209.85]);
  rows.push([null, null, null, null, null, 'Presentation continuation']);
  rows.push(['2026-09-02', 1002, 'Worker Two', 'W2', 'Trust Two', 'Ward Two', 'ROLE', null, null, null, null, '20:00', '08:00', 30, '11:30', 10, 100]);
  const result = await parseWeeklySourceFile(workbookBytes([['Released', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  });
  assert.equal(result.ok, true);
  assert.equal(result.rows.length, 2);
  assert.equal(result.rows[0].actual.totalMinutes, 450);
  assert.equal(result.rows[1].actual.overnight, true);
  assert.deepEqual(result.scope.trusts, ['Trust One', 'Trust Two']);
  assert.equal(result.presentationRows[0].rowKind, 'WARD_CONTINUATION');
  assert.equal(result.rows[0].pricingEvidence.authority, 'PREFINAL_CHECK_EVIDENCE_ONLY');
});

test('NHSP pre-final keeps hours checking available when pricing evidence is invalid', async () => {
  const rows = nhspHeaders(false);
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', -1, 2]);
  const result = await parseWeeklySourceFile(workbookBytes([['Released', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  });
  assert.equal(result.ok, true);
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].actual.totalMinutes, 450);
  assert.equal(result.rows[0].pricingEvidence.state, 'UNVERIFIABLE');
  assert.ok(result.warnings.some((issue) => issue.code === 'NHSP_MIXED_SIGN_ROW'));
});

test('NHSP ward continuation cannot skip over a malformed economic-looking row', async () => {
  const rows = nhspHeaders(false);
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', 52.5, 209.85]);
  rows.push(['not-a-date', 1002, 'Worker Two', 'W2', 'Trust One', 'Ward Two', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', 52.5, 209.85]);
  rows.push([null, null, null, null, null, 'Must not attach to Worker One']);
  const result = await parseWeeklySourceFile(workbookBytes([['Released', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  });
  assert.equal(result.ok, false);
  assert.ok(result.fatalErrors.some((issue) => issue.code === 'NHSP_DATE_INVALID'));
  assert.ok(result.fatalErrors.some((issue) => issue.code === 'NHSP_ORPHAN_WARD_CONTINUATION'));
  assert.equal(result.presentationRows.length, 0);
});

test('NHSP two-row header map resolves reordered money columns instead of trusting letters', async () => {
  const rows = nhspHeaders(false);
  [rows[1][15], rows[1][16]] = [rows[1][16], rows[1][15]];
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', 209.85, 52.5]);
  const result = await parseWeeklySourceFile(workbookBytes([['Released', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  });
  assert.equal(result.ok, true);
  assert.equal(result.resolvedColumnMap.commission.column, 'Q');
  assert.equal(result.resolvedColumnMap.totalCost.column, 'P');
  assert.equal(result.rows[0].pricingEvidence.sourceTotalPence, '26235');
});

test('NHSP final preserves positive and full-negative physical movements and reconciles the Total Cost trailer', async () => {
  const rows = nhspHeaders(true);
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', 52.5, 0, 209.85, 'Basic']);
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', -10, 0, -100, 'Basic']);
  rows.push(['FrameWork Management Charge', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 0]);
  rows.push(['Total', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 109.85]);
  const result = await parseWeeklySourceFile(workbookBytes([['Backing', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
    configuredNhspReportHeadingName: 'Exact Heading',
    expectedTrust: 'Trust One',
  });
  assert.equal(result.ok, true);
  assert.deepEqual(result.rows.map((row) => row.rowKind), ['POSITIVE_SOURCE_SHIFT', 'FULL_REVERSAL']);
  assert.deepEqual(result.rows.map((row) => row.pricingEvidence.sourceTotalPence), ['26235', '-11000']);
  assert.equal(result.scope.backingReportNumber, '123');
  assert.equal(result.trailers.total.totalCostPence, '10985');
});

test('NHSP final fails closed for mixed signs', async () => {
  const rows = nhspHeaders(true);
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', -10, 0, 100, 'Basic']);
  rows.push(['FrameWork Management Charge', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 0]);
  rows.push(['Total', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 100]);
  const result = await parseWeeklySourceFile(workbookBytes([['Backing', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
    configuredNhspReportHeadingName: 'Exact Heading',
  });
  assert.equal(result.ok, false);
  assert.ok(result.fatalErrors.some((issue) => issue.code === 'NHSP_MIXED_SIGN_ROW'));
});

test('NHSP final rejects a formula even when the workbook caches a numeric money result', async () => {
  const rows = nhspHeaders(true);
  rows.push(['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward One', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', 52.5, 0, 209.85, 'Basic']);
  rows.push(['FrameWork Management Charge', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 0]);
  rows.push(['Total', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 209.85]);
  const workbook = XLSX.utils.book_new();
  const worksheet = XLSX.utils.aoa_to_sheet(rows);
  worksheet.P4 = { t: 'n', f: '50+2.5', v: 52.5 };
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Backing');
  const result = await parseWeeklySourceFile(XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' }), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
    configuredNhspReportHeadingName: 'Exact Heading',
  });
  assert.equal(result.ok, false);
  assert.ok(result.fatalErrors.some((issue) => issue.code === 'SOURCE_MONEY_UNVERIFIABLE_CELL_TYPE'));
});

test('HealthRoster layout A uses only the second Start/End block and keeps unfinalised rows as non-worked evidence', async () => {
  const header = ['Request Id', 'Staff', 'Date', 'From', 'To', 'Break', 'Start', 'End', 'Actual Break', 'Hours', 'Finalised Date', 'Timesheet Finalised By'];
  const rows = [
    header,
    ['A1', 'Worker One', '2026-09-01', '00:00', '00:01', 0, '20:00', '08:00', 30, '11:30', '2026-09-02', 'Manager'],
    ['A2', 'Worker Two', '2026-09-02', '09:00', '17:00', 30, null, null, null, null, null, null],
  ];
  const result = await parseWeeklySourceFile(workbookBytes([['Export', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
    expectedClient: 'Client A',
  });
  assert.equal(result.ok, true);
  assert.equal(result.rows[0].rowKind, 'FINALISED_WORKED');
  assert.equal(result.rows[0].actual.start, '20:00');
  assert.equal(result.rows[0].actual.totalMinutes, 690);
  assert.equal(result.rows[1].rowKind, 'UNFINALISED');
  assert.equal(result.rows[1].actual, undefined);
});

test('HealthRoster distinguishes an unfinalised explicit zero and blocks a zero row marked finalised', async () => {
  const header = ['Request Id', 'Staff', 'Date', 'From', 'To', 'Break', 'Start', 'End', 'Actual Break', 'Hours', 'Finalised Date', 'Timesheet Finalised By'];
  const result = await parseWeeklySourceFile(workbookBytes([['Export', [
    header,
    ['Z1', 'Worker One', '2026-09-01', '09:00', '17:00', 30, null, null, null, '0:00', null, null],
    ['Z2', 'Worker Two', '2026-09-02', '09:00', '17:00', 30, null, null, null, '0:00', '2026-09-03', 'Manager'],
  ]]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
  });
  assert.equal(result.ok, false);
  assert.equal(result.rows[0].rowKind, 'UNFINALISED');
  assert.equal(result.rows[0].sourcePosition, 'EXPLICIT_ZERO');
  assert.equal(result.rows[1].rowKind, 'BLOCKED');
  assert.ok(result.fatalErrors.some((issue) => issue.actualCode === 'ACTUAL_ZERO_MARKED_FINALISED'));
});

test('HealthRoster layout B uses Actual fields and requires status/finaliser agreement', async () => {
  const header = ['Request Id', 'Status', 'Staff', 'Date', 'Start', 'End', 'Actual Start', 'Actual End', 'Actual Break', 'Actual Hours', 'Timesheet Finalised By'];
  const rows = [
    header,
    ['B1', 'Timesheet Finalised', 'Worker One', '2026-09-01', '01:00', '02:00', '09:00', '17:00', 30, '7:30', 'Manager'],
    ['B2', 'Informed Agency', 'Worker Two', '2026-09-02', '09:00', '17:00', null, null, null, null, null],
    ['B3', 'Timesheet Finalised', 'Worker Three', '2026-09-03', '09:00', '17:00', null, null, null, null, null],
  ];
  const result = await parseWeeklySourceFile(workbookBytes([['Export', rows]]), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1,
  });
  assert.equal(result.ok, false);
  assert.equal(result.rows[0].actual.start, '09:00');
  assert.equal(result.rows[1].rowKind, 'UNFINALISED');
  assert.equal(result.rows[2].rowKind, 'BLOCKED');
  assert.ok(result.fatalErrors.some((issue) => issue.code === 'FINALISATION_INDICATORS_DISAGREE'));
});

test('profile selection refuses workbooks with more than one qualifying source table', async () => {
  const layoutA = [['Request Id', 'Staff', 'Date', 'From', 'To', 'Break', 'Start', 'End', 'Actual Break', 'Hours', 'Finalised Date', 'Timesheet Finalised By']];
  const layoutB = [['Request Id', 'Status', 'Staff', 'Date', 'Start', 'End', 'Actual Start', 'Actual End', 'Actual Break', 'Actual Hours', 'Timesheet Finalised By']];
  await assert.rejects(
    parseWeeklySourceFile(workbookBytes([['A', layoutA], ['B', layoutB]]), {
      profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
    }),
    { code: 'SOURCE_PROFILE_AMBIGUOUS' },
  );
});

test('HTML frame and bundle wrappers are refused before profile parsing', async () => {
  const bytes = new TextEncoder().encode('<html><frameset><frame src="sheet001.htm"></frameset></html>');
  await assert.rejects(parseWeeklySourceFile(bytes, {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  }), { code: 'HTML_WRAPPER_NOT_SUPPORTED' });
});

test('a self-contained NHSP HTML table is decoded without fetching sidecars', async () => {
  const tableRows = [
    ['Timesheets Previously Released'],
    ['Date', 'Ref Num', 'Agency Worker Name', 'Agency Worker Unique Id', 'Trust', 'Ward', 'Assignment', 'Contract', '', '', '', 'Actual', '', '', '', 'Commission', 'Total Cost'],
    ['', '', '', '', '', '', '', 'Start', 'End', 'Break In Minutes', 'Total', 'Start', 'End', 'Break In Minutes', 'Total', '', ''],
    ['1/9/26', '1001', 'Worker One', '', 'Trust One', 'Ward One', 'ROLE', 'ignored', 'ignored', 'ignored', 'ignored', '09:00', '17:00', '30', '7:30', '&pound;52.50', '209.85'],
  ];
  const html = `<html><body><table>${tableRows.map((row) => `<tr>${row.map((value) => `<td>${value}</td>`).join('')}</tr>`).join('')}</table></body></html>`;
  const result = await parseWeeklySourceFile(new TextEncoder().encode(html), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  });
  assert.equal(result.ok, true);
  assert.equal(result.sourceKind, 'HTML');
  assert.equal(result.rows[0].date, '2026-09-01');
  assert.equal(result.rows[0].pricingEvidence.commissionPence, '5250');
});
