import { DeterministicIdentityRegistry } from './deterministic-identities.mjs';
import { decimalHoursText, poundsTextFromPence, writeCsvArtifact } from './csv-fixture-utils.mjs';
import { SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS } from './source-profile-layouts.mjs';
import { requireScenarioUpload, SourceFixtureError } from './workbook-fixture-utils.mjs';

const RATE_FAMILIES = Object.freeze({
  STD: ['STD Hours', 'STD Unit Cost', 'STD Sub Cost'],
  OT: ['OT Hours', 'OT Unit Cost', 'OT Sub Cost'],
  SAT: ['SAT Hours', 'SAT Unit Cost', 'SAT Sub Cost'],
  SUN: ['SUN Hours', 'SUN Unit Cost', 'SUN Sub Cost'],
  BH: ['BH Hours', 'BH Unit Cost', 'BH Sub Cost'],
  NMW_MIDWEEK_ADJ_SAT: ['Nmw-Midweek-Adj/sat Hours', 'Nmw-Midweek-Adj/sat Unit Cost', 'Nmw-Midweek-Adj/sat Sub Cost']
});

function relatedTimesheets(scenario, clientKey) {
  const contractKeys = new Set(scenario.foundation.contracts.filter((item) => item.clientKey === clientKey).map((item) => item.key));
  const weekKeys = new Set(scenario.foundation.weeks.filter((item) => contractKeys.has(item.contractKey)).map((item) => item.key));
  return scenario.foundation.timesheets.filter((item) => weekKeys.has(item.weekKey));
}

function validateProfile(context, scenario, profileDefinition) {
  if (context.upload.profile !== 'GENERIC_WEEKLY_COMPLETE_V1') {
    throw new SourceFixtureError('SOURCE_FIXED_PROFILE_REQUIRED', 'Source-fixed expense fixtures use the generic complete source envelope');
  }
  if (profileDefinition?.profileId !== 'SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_V1') {
    throw new SourceFixtureError('SOURCE_FIXED_PROFILE_NOT_CONFIGURED', 'The source-fixed expense writer requires its exact versioned profile');
  }
  if (context.client.settings.sourceSuppliedExpenses !== true || context.client.settings.calculationMode !== 'WHOLE_SHIFT') {
    throw new SourceFixtureError('SOURCE_FIXED_CLIENT_POLICY_REQUIRED', 'Source-fixed expenses require both source-supplied expenses and whole-shift calculation policy');
  }
  if (relatedTimesheets(scenario, context.client.key).some((item) => item.expenses.mode === 'ORDINARY_EVIDENCE')) {
    throw new SourceFixtureError('ORDINARY_EXPENSES_PROTECTED', 'Ordinary evidence expenses cannot be converted into source-fixed expenses');
  }
  for (const [rowKey, family] of Object.entries(profileDefinition.rateFamilyByRowKey || {})) {
    if (!context.upload.physicalRows.some((row) => row.key === rowKey) || !Object.hasOwn(RATE_FAMILIES, family)) {
      throw new SourceFixtureError('WHOLE_SHIFT_CLASSIFICATION_INVALID', 'Every declared whole-shift rate family must name an upload row and supported source column family');
    }
  }
}

function emptyRecord() {
  return Object.fromEntries(SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS.map((header) => [header, '']));
}

function weekdayHeader(workDate) {
  const [year, month, day] = workDate.split('-').map(Number);
  return ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'][new Date(Date.UTC(year, month - 1, day)).getUTCDay()];
}

function nextDateWhenOvernight(workDate, start, end) {
  const [startHour, startMinute] = start.split(':').map(Number);
  const [endHour, endMinute] = end.split(':').map(Number);
  if (endHour * 60 + endMinute > startHour * 60 + startMinute) return workDate;
  const [year, month, day] = workDate.split('-').map(Number);
  return new Date(Date.UTC(year, month - 1, day + 1)).toISOString().slice(0, 10);
}

export function buildSourceFixedExpenseWholeShiftCsv(scenario, uploadOrKey, profileDefinition) {
  const context = requireScenarioUpload(scenario, uploadOrKey);
  validateProfile(context, scenario, profileDefinition);
  const mutations = new Set(context.upload.mutations || []);
  const registry = new DeterministicIdentityRegistry(scenario.scenarioId);
  const headers = [...SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS];
  if (mutations.has('WRONG_HEADER')) headers[33] = 'Unexpected Expenses';
  const rows = context.upload.physicalRows.map((row, ordinal) => {
    if (row.sourceExpensePence === null) {
      throw new SourceFixtureError('SOURCE_EXPENSE_REQUIRED', `Source-fixed row ${row.key} must declare its expense pence, including zero`);
    }
    if (row.actualWorkedMinutes === null || !row.actualStart || !row.actualEnd) {
      throw new SourceFixtureError('SOURCE_FIXED_ACTUAL_REQUIRED', `Source-fixed row ${row.key} requires whole-shift Actual hours`);
    }
    const candidate = context.candidates.get(row.candidateKey);
    const record = emptyRecord();
    const rowId = row.requestId || registry.externalKey('fixed-expense-source-row', ordinal, { prefix: 'FX' });
    const hours = decimalHoursText(row.actualWorkedMinutes);
    const family = profileDefinition.rateFamilyByRowKey?.[row.key] || profileDefinition.defaultRateFamily;
    if (!Object.hasOwn(RATE_FAMILIES, family)) {
      throw new SourceFixtureError('WHOLE_SHIFT_CLASSIFICATION_REQUIRED', `Source-fixed row ${row.key} requires an explicit whole-shift rate family`);
    }
    const [hoursColumn, unitCostColumn, subCostColumn] = RATE_FAMILIES[family];
    // This choice only reproduces the physical source column family. It is not
    // the CloudTMS rate classifier and is never an expected pay/charge oracle.
    for (const dayColumn of ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']) record[dayColumn] = '0.00';
    for (const [unusedHours, unusedUnitCost, unusedSubCost] of Object.values(RATE_FAMILIES)) {
      record[unusedHours] = '0.00';
      record[unusedUnitCost] = '';
      record[unusedSubCost] = '0.00';
    }
    record.grand_parent_business_unit_name = profileDefinition.businessHierarchy?.grandParent || 'Scenario group';
    record.parent_business_unit_name = profileDefinition.businessHierarchy?.parent || context.client.name;
    record.business_unit_name = profileDefinition.businessHierarchy?.unit || context.client.name;
    record['Billing Group Name'] = context.client.name;
    record['Client Weekend Date'] = context.upload.coverageEnd || row.workDate;
    record['Weekend Date'] = context.upload.coverageEnd || row.workDate;
    record['Job Category'] = row.role || 'Scenario role';
    record['Vat Option'] = context.client.settings.sourceExpenseVatChargeable ? 'VAT' : 'NO VAT';
    record.Candidate = candidate.displayName;
    record['Payroll Number'] = candidate.tmsRef;
    record['Booking Id'] = rowId;
    record['Timesheet Id'] = registry.externalKey('fixed-expense-timesheet', ordinal, { prefix: 'TS' });
    record.Agency = profileDefinition.agencyDisplayName || 'Scenario Agency';
    record['Candidate Uid'] = candidate.tmsRef;
    record['Candidate Id'] = candidate.tmsRef;
    record['TNA Reference'] = row.requestId || '';
    record['Approved By'] = 'Scenario Approver';
    record['Approved Date'] = row.workDate;
    record[weekdayHeader(row.workDate)] = hours;
    record.Expenses = poundsTextFromPence(row.sourceExpensePence);
    record['Total Hours'] = hours;
    record.Bonus = '0.00';
    record['Bonus NI'] = '0.00';
    record['Total Cost'] = row.totalCostPence === null ? '0.00' : poundsTextFromPence(row.totalCostPence);
    record['Line ID'] = rowId;
    record[hoursColumn] = hours;
    record[unitCostColumn] = profileDefinition.unitCostByRateFamilyPence?.[family] === undefined
      ? ''
      : poundsTextFromPence(profileDefinition.unitCostByRateFamilyPence[family]);
    record[subCostColumn] = row.totalCostPence === null ? '0.00' : poundsTextFromPence(row.totalCostPence);
    record['Booking Reference'] = row.requestId || rowId;
    record['Booking Reason'] = 'Scenario source fixture';
    record['Supply Type'] = 'Temporary';
    record.Type = 'Weekly';
    record['Business Unit ID'] = registry.externalKey('fixed-expense-business-unit', 0, { prefix: 'BU' });
    record['Job ID'] = row.contractKey || '';
    record['Job Type'] = row.band || row.role || 'Scenario role';
    record['Booking Start'] = `${row.workDate} ${row.actualStart}:00`;
    record['Booking End'] = `${nextDateWhenOvernight(row.workDate, row.actualStart, row.actualEnd)} ${row.actualEnd}:00`;
    record['Invoice ID'] = context.upload.key;
    return SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS.map((header) => record[header]);
  });
  return writeCsvArtifact({
    profile: profileDefinition.profileId,
    fileName: `${context.upload.key}.csv`,
    headers,
    rows
  });
}

export { RATE_FAMILIES as SOURCE_FIXED_WHOLE_SHIFT_RATE_FAMILIES };
