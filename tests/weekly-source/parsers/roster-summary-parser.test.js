import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS,
  parseWeeklySourceFile,
  WEEKLY_SOURCE_PROFILE_IDS,
} from '../../../broker/src/weekly-source/index.js';

function csvEscape(value) {
  const text = String(value ?? '');
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function sourceBytes(overrides = {}) {
  const values = Object.fromEntries(ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map((header) => [header, '']));
  Object.assign(values, {
    grand_parent_business_unit_name: 'Group',
    parent_business_unit_name: 'Parent',
    business_unit_name: 'Unit',
    Candidate: 'Worker One',
    'Candidate Uid': 'UID1',
    'Candidate Id': 'CID1',
    'Booking Id': 'B1',
    'Timesheet Id': 'T1',
    Expenses: '25.40',
    'Total Hours': '7.50',
    'Line ID': 'L1',
    'Business Unit ID': 'BU1',
    'Booking Start': '2026-09-01 09:00:00',
    'Booking End': '2026-09-01 17:00:00',
    ...overrides,
  });
  const csv = `${ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map(csvEscape).join(',')}\r\n${ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map((header) => csvEscape(values[header])).join(',')}\r\n`;
  return new TextEncoder().encode(csv);
}

test('configurable source-fixed-expense CSV uses exact hours, booking interval and non-negative expense pence', async () => {
  const result = await parseWeeklySourceFile(sourceBytes(), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1,
  });
  assert.equal(result.ok, true);
  assert.equal(result.rows[0].wholeShiftInputs.bookingStartLocal, '2026-09-01T09:00:00');
  assert.equal(result.rows[0].wholeShiftInputs.paidMinutes, 450);
  assert.equal(result.rows[0].sourceFixedExpense.pence, '2540');
  assert.equal(result.rows[0].sourceFixedExpense.state, 'PRESENT');
  assert.equal(result.rows[0].sourceFixedExpense.sourceEvidence.decodedToken, '25.40');
  assert.equal(result.rows[0].rowKind, 'SOURCE_SHIFT');
  assert.equal(Object.hasOwn(result, 'sourceMode'), false);
  assert.equal(Object.hasOwn(result, 'bankingMode'), false);
  assert.equal(JSON.stringify(result).includes('MAGNIT'), false);
});

test('blank source expense is explicit omitted zero and zero hours remain a source-zero position', async () => {
  const result = await parseWeeklySourceFile(sourceBytes({ Expenses: '', 'Total Hours': '0.00' }), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1,
  });
  assert.equal(result.ok, true);
  assert.equal(result.rows[0].rowKind, 'SOURCE_ZERO');
  assert.equal(result.rows[0].sourceFixedExpense.pence, '0');
  assert.equal(result.rows[0].sourceFixedExpense.state, 'OMITTED_ZERO');
});

test('negative expense and fractional-minute hours fail closed', async () => {
  const expense = await parseWeeklySourceFile(sourceBytes({ Expenses: '-1.00' }), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1,
  });
  assert.equal(expense.ok, false);
  assert.ok(expense.fatalErrors.some((issue) => issue.code === 'SOURCE_EXPENSE_INVALID_LEXEME'));

  const hours = await parseWeeklySourceFile(sourceBytes({ 'Total Hours': '7.333' }), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1,
  });
  assert.equal(hours.ok, false);
  assert.ok(hours.fatalErrors.some((issue) => issue.code === 'ROSTER_SUMMARY_TOTAL_HOURS_NOT_WHOLE_MINUTES'));
});

test('CSV profile is selected by exact headers, never by a client or filename hint', async () => {
  const wrongHeaders = [...ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS];
  [wrongHeaders[0], wrongHeaders[1]] = [wrongHeaders[1], wrongHeaders[0]];
  const bytes = new TextEncoder().encode(`${wrongHeaders.join(',')}\r\n${wrongHeaders.map(() => '').join(',')}\r\n`);
  await assert.rejects(parseWeeklySourceFile(bytes, {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1,
  }), { code: 'SOURCE_PROFILE_NOT_RECOGNISED' });
});
