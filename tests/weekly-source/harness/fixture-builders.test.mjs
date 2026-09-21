import assert from 'node:assert/strict';
import test from 'node:test';
import * as XLSX from 'xlsx';
import { canonicalDigest, cloneJson } from './canonical-json.mjs';
import { buildExpectedSourceModel } from './expected-source-model.mjs';
import { auditFoundationRecordPlan } from './foundation-record-audit.mjs';
import { buildFoundationRecordPlan, FoundationBuilderError } from './foundation-record-builder.mjs';
import { buildHealthRosterLayoutAWorkbook } from './healthroster-layout-a-builder.mjs';
import { buildHealthRosterLayoutBWorkbook } from './healthroster-layout-b-builder.mjs';
import { buildNhspBackingReport } from './nhsp-backing-report-builder.mjs';
import { buildNhspPrefinalWorkbook } from './nhsp-prefinal-workbook-builder.mjs';
import { buildSourceFile } from './source-file-builder.mjs';
import {
  HEALTHROSTER_LAYOUT_A_HEADERS,
  HEALTHROSTER_LAYOUT_B_HEADERS,
  NHSP_FINAL_HEADERS,
  NHSP_PREFINAL_HEADERS,
  NHSP_SUBHEADERS,
  SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS
} from './source-profile-layouts.mjs';
import { SourceFixtureError } from './workbook-fixture-utils.mjs';
import { loadScenarioSchema, validateScenarioObject } from './scenario-loader.mjs';

const rateVector = { day: '1000', night: '1200', saturday: '1300', sunday: '1400', bankHoliday: '1500' };

function settings(overrides = {}) {
  return {
    selfBill: false,
    nhsp: false,
    noTimesheetRequired: false,
    requiresHealthRoster: false,
    referenceRequiredBeforePay: false,
    autoAuthorise: false,
    candidateQueriesEnabled: true,
    managerQueriesEnabled: true,
    sourceSuppliedExpenses: false,
    sourceExpenseVatChargeable: false,
    breakEntryMode: 'DURATION_MINUTES',
    calculationMode: 'SPLIT_RATE',
    cutoffWeekday: 3,
    cutoffLocalTime: '15:00',
    ...overrides
  };
}

function client(key, sourceAuthority, overrides = {}) {
  return {
    key,
    name: `Scenario ${key}`,
    sourceAuthority,
    managerEmail: `${key}@example.test`,
    settings: settings(overrides)
  };
}

function contract(key, clientKey, candidateKey = 'candidate_1', payMethod = 'PAYE') {
  return {
    key,
    candidateKey,
    clientKey,
    startDate: '2026-08-01',
    endDate: '2026-12-31',
    role: 'Nurse',
    band: 'Band 5',
    payMethod,
    rates: { pay: rateVector, charge: rateVector },
    additionalUnits: [{ code: 'BONUS', payPence: '100', chargePence: '200' }]
  };
}

function row(key, candidateKey, contractKey, overrides = {}) {
  return {
    key,
    candidateKey,
    contractKey,
    requestId: `REQ-${key.toUpperCase()}`,
    workDate: '2026-09-14',
    actualStart: '20:00',
    actualEnd: '08:00',
    actualBreakMinutes: 60,
    actualWorkedMinutes: 660,
    sign: 'POSITIVE',
    finalisation: 'FINALISED',
    statusText: 'Timesheet Finalised',
    finalisedBy: 'Scenario Finaliser',
    commissionPence: null,
    totalCostPence: null,
    sourceExpensePence: null,
    role: 'Nurse',
    band: 'Band 5',
    ...overrides
  };
}

function upload(key, profile, stage, clientKey, physicalRows, overrides = {}) {
  return {
    key,
    profile,
    stage,
    clientKey,
    trustName: null,
    reportNumber: null,
    cycleUtc: '2026-09-16T14:00:00Z',
    coverageStart: '2026-09-14',
    coverageEnd: '2026-09-20',
    complete: true,
    physicalRows,
    mutations: [],
    ...overrides
  };
}

function scenarioFacts() {
  const clients = [
    client('client_nhsp', 'SOURCE', { selfBill: true, nhsp: true, noTimesheetRequired: true }),
    client('client_hr_a', 'SOURCE', { selfBill: true, noTimesheetRequired: true, requiresHealthRoster: true }),
    client('client_hr_b', 'SIGNED_TIMESHEET', { requiresHealthRoster: true, autoAuthorise: true }),
    client('client_generic', 'SOURCE', { selfBill: true, noTimesheetRequired: true }),
    client('client_fixed', 'SOURCE', {
      selfBill: true,
      noTimesheetRequired: true,
      sourceSuppliedExpenses: true,
      sourceExpenseVatChargeable: true,
      calculationMode: 'WHOLE_SHIFT'
    }),
    client('client_ordinary', 'ORDINARY')
  ];
  const contracts = [
    contract('contract_nhsp', 'client_nhsp'),
    contract('contract_hr_a', 'client_hr_a'),
    contract('contract_hr_b', 'client_hr_b'),
    contract('contract_generic', 'client_generic'),
    contract('contract_fixed', 'client_fixed'),
    contract('contract_ordinary', 'client_ordinary', 'candidate_2', 'UMBRELLA')
  ];
  return {
    schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
    scenarioId: 'WS-SOURCE-BUILDER-001',
    title: 'Deterministic source and foundation builders',
    fixedSeed: '0000000000000000000000000000000000000000000000000000000000000002',
    requirementIds: ['SRC-REQ-005', 'SRC-REQ-006', 'SRC-REQ-007', 'SRC-REQ-008', 'SRC-REQ-009', 'SRC-REQ-010'],
    protectedIds: ['PROT-SEC-001', 'PROT-INV-001', 'PROT-AUDIT-001'],
    tags: ['HARNESS', 'SOURCE_BUILDERS'],
    clock: { initialUtc: '2026-09-15T09:00:00Z', timezone: 'Europe/London' },
    foundation: {
      agency: {
        key: 'agency_1',
        globalSettings: { candidateDeadlineMinutes: 720, candidateReminderMinutes: 360, managerBatchMinutes: 360 }
      },
      users: [{ key: 'office_1', role: 'OFFICE', active: true }],
      candidates: [
        { key: 'candidate_1', displayName: 'Test Candidate One', tmsRef: 'CAN-TEST-1', payMethod: 'PAYE', umbrellaKey: null, active: true, activeDeviceCount: 1 },
        { key: 'candidate_2', displayName: 'Test Candidate Two', tmsRef: 'CAN-TEST-2', payMethod: 'UMBRELLA', umbrellaKey: 'provider_1', active: true, activeDeviceCount: 0 }
      ],
      clients,
      contracts,
      weeks: [
        { key: 'week_fixed', contractKey: 'contract_fixed', weekEndingDate: '2026-09-20', additionalSequence: 0, status: 'SUBMITTED' },
        { key: 'week_ordinary', contractKey: 'contract_ordinary', weekEndingDate: '2026-09-20', additionalSequence: 0, status: 'SUBMITTED' }
      ],
      timesheets: [
        {
          key: 'timesheet_fixed', weekKey: 'week_fixed', submissionState: 'SUBMITTED', candidateSigned: true, managerSigned: false,
          shifts: [{ key: 'fixed_shift', workDate: '2026-09-14', start: '20:00', end: '08:00', breakMinutes: 60, breakStart: null, breakEnd: null, workedMinutes: 660, reference: null, additionalUnits: [] }],
          expenses: { mode: 'SOURCE_FIXED', amountPence: '1234', evidenceCount: 0 }
        },
        {
          key: 'timesheet_ordinary', weekKey: 'week_ordinary', submissionState: 'SUBMITTED', candidateSigned: true, managerSigned: true,
          shifts: [{ key: 'ordinary_shift', workDate: '2026-09-14', start: '09:00', end: '17:00', breakMinutes: 30, breakStart: null, breakEnd: null, workedMinutes: 450, reference: null, additionalUnits: [] }],
          expenses: { mode: 'ORDINARY_EVIDENCE', amountPence: '2500', evidenceCount: 2 }
        }
      ]
    },
    sourceUploads: [
      upload('nhsp_pre', 'NHSP_PREFINAL_RELEASED_V1', 'PREFINAL', 'client_nhsp', [
        row('nhsp_pre_1', 'candidate_1', 'contract_nhsp', { commissionPence: '5250', totalCostPence: '20985' })
      ], { trustName: 'Scenario Trust' }),
      upload('nhsp_final', 'NHSP_FINAL_BACKING_V1', 'FINAL', 'client_nhsp', [
        row('nhsp_positive', 'candidate_1', 'contract_nhsp', { commissionPence: '5250', totalCostPence: '20985' }),
        row('nhsp_negative', 'candidate_1', 'contract_nhsp', { sign: 'FULL_NEGATIVE', commissionPence: '-5250', totalCostPence: '-20985' })
      ], { trustName: 'Scenario Trust', reportNumber: '1740430' }),
      upload('hr_a', 'HEALTHROSTER_SELF_BILL_LAYOUT_A_V1', 'FINAL', 'client_hr_a', [
        row('hr_a_1', 'candidate_1', 'contract_hr_a', { requestId: '1025516016' })
      ]),
      upload('hr_b', 'HEALTHROSTER_TIMESHEET_AUTHORITY_LAYOUT_B_V1', 'COMPARISON_ONLY', 'client_hr_b', [
        row('hr_b_1', 'candidate_1', 'contract_hr_b', { requestId: '0926641430' }),
        row('hr_b_unfilled', 'candidate_1', 'contract_hr_b', {
          requestId: '0926641485', actualStart: null, actualEnd: null, actualBreakMinutes: null,
          actualWorkedMinutes: null, finalisation: 'NOT_FINALISED', statusText: 'Informed Agency', finalisedBy: null
        })
      ]),
      upload('generic', 'GENERIC_WEEKLY_COMPLETE_V1', 'FINAL', 'client_generic', [
        row('generic_1', 'candidate_1', 'contract_generic')
      ]),
      upload('fixed_expense', 'GENERIC_WEEKLY_COMPLETE_V1', 'FINAL', 'client_fixed', [
        row('fixed_1', 'candidate_1', 'contract_fixed', { sourceExpensePence: '1234', totalCostPence: '9900' })
      ])
    ],
    actions: [],
    expected: {
      outcome: 'NO_CHANGE', sourceMovements: [], invoiceLines: [], approvedHours: [], communications: [],
      c1PublicationCategory: 'NONE', visibleAssertions: [], forbiddenAssertions: []
    }
  };
}

function workbook(artifact) {
  return XLSX.read(artifact.bytes, { type: 'buffer', raw: true, cellDates: false });
}

function csvRows(artifact) {
  return artifact.bytes.toString('utf8').trimEnd().split(/\r\n/).map((line) => line.split(','));
}

const genericProfile = {
  profileId: 'GENERIC_WEEKLY_COMPLETE_TEST_V1',
  fileType: 'CSV',
  headers: ['Row ID', 'Candidate Ref', 'Client', 'Work Date', 'Start', 'End', 'Break Minutes', 'Worked Minutes', 'Reference'],
  columns: {
    identity: 'Row ID', candidateReference: 'Candidate Ref', client: 'Client', workDate: 'Work Date',
    actualStart: 'Start', actualEnd: 'End', actualBreakMinutes: 'Break Minutes',
    actualWorkedMinutes: 'Worked Minutes', reference: 'Reference'
  }
};

const fixedExpenseProfile = {
  profileId: 'SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_V1',
  defaultRateFamily: 'STD',
  unitCostByRateFamilyPence: { STD: '900' },
  businessHierarchy: { grandParent: 'Scenario group', parent: 'Scenario parent', unit: 'Scenario unit' },
  agencyDisplayName: 'Scenario Agency'
};

test('TH-005/TH-006 foundation plan is deterministic, dependency ordered and audited before database action', async () => {
  const scenario = scenarioFacts();
  validateScenarioObject(scenario, await loadScenarioSchema());
  const first = buildFoundationRecordPlan(scenario);
  const second = buildFoundationRecordPlan(cloneJson(scenario));
  assert.deepEqual(first, second);
  assert.deepEqual(first.stages.map((stage) => stage.sequence), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
  assert.equal(first.mode, 'NON_DATABASE_INPUT_PLAN');
  assert.equal(first.stages[1].records[0].recordKind, 'UMBRELLA_PROVIDER_PREREQUISITE');
  const fixedEvidence = first.stages[8].records.find((record) => record.key === 'timesheet_fixed');
  const ordinaryEvidence = first.stages[8].records.find((record) => record.key === 'timesheet_ordinary');
  assert.equal(fixedEvidence.expenses.mode, 'SOURCE_FIXED');
  assert.equal(fixedEvidence.evidence.length, 0);
  assert.equal(ordinaryEvidence.expenses.mode, 'ORDINARY_EVIDENCE');
  assert.equal(ordinaryEvidence.evidence.length, 2);
  const audit = auditFoundationRecordPlan(scenario, first);
  assert.equal(audit.planDigest, first.planDigest);
  const changed = cloneJson(first);
  changed.stages[4].records[0].settings.autoAuthorise = !changed.stages[4].records[0].settings.autoAuthorise;
  assert.throws(() => auditFoundationRecordPlan(scenario, changed), (error) => error instanceof FoundationBuilderError && error.code === 'FOUNDATION_AUDIT_MISMATCH');
});

test('TH-005 foundation plan refuses missing and cross-owner prerequisites', () => {
  const missing = scenarioFacts();
  missing.foundation.contracts[0].candidateKey = 'candidate_missing';
  assert.throws(() => buildFoundationRecordPlan(missing), (error) => error.code === 'FOUNDATION_CANDIDATE_DEPENDENCY_MISSING');
  const crossed = scenarioFacts();
  crossed.sourceUploads[0].physicalRows[0].contractKey = 'contract_hr_a';
  assert.throws(() => buildFoundationRecordPlan(crossed), (error) => error.code === 'FOUNDATION_CROSS_AGENCY_OR_OWNER_LINK');
});

test('TH-008 NHSP pre-final writer is exact, deterministic, two-row and supports an empty workbook', () => {
  assert.equal(canonicalDigest(NHSP_PREFINAL_HEADERS), '7a4630a05c3db54a25ab41278a914a7ef5fd17c1f22f73fe91501e54769b7afe');
  assert.equal(canonicalDigest(NHSP_SUBHEADERS), '1e08e3af6062369864bbd12c58a19e763c16563265c33ae06d1db46cbfd72524');
  const scenario = scenarioFacts();
  const first = buildNhspPrefinalWorkbook(scenario, 'nhsp_pre');
  const second = buildNhspPrefinalWorkbook(scenario, 'nhsp_pre');
  assert.equal(first.sha256, second.sha256);
  assert.deepEqual(first.bytes, second.bytes);
  const wb = workbook(first);
  const ws = wb.Sheets.Export;
  assert.equal(ws.A1.v, 'Timesheets Previously Released');
  assert.equal(ws.A2.v, 'Date');
  assert.equal(ws.H2.v, 'Contract');
  assert.equal(ws.L2.v, 'Actual');
  assert.equal(ws.L4.v, 20 / 24);
  assert.equal(ws.N4.v, 60);
  assert.equal(ws.P4.v, 52.5);
  assert.equal(ws.Q4.v, 209.85);
  assert.equal(ws.F5.v, 'Service 1');
  assert(ws['!merges'].some((range) => range.s.r === 3 && range.e.r === 4 && range.s.c === 0));
  const empty = scenarioFacts();
  empty.sourceUploads.find((item) => item.key === 'nhsp_pre').physicalRows = [];
  assert.equal(workbook(buildNhspPrefinalWorkbook(empty, 'nhsp_pre')).Sheets.Export['!ref'], 'A1:Q3');
});

test('TH-009 NHSP final writer preserves signed physical rows, Actual hours and Total Cost-only footer', () => {
  assert.equal(canonicalDigest(NHSP_FINAL_HEADERS), '856a1438c922f489bc9623f6dd5470c3597f60987bfe8c30c2a7d34c73d89fef');
  const scenario = scenarioFacts();
  const artifact = buildNhspBackingReport(scenario, 'nhsp_final', { reportHeadingName: 'Scenario Agency Name' });
  const repeat = buildNhspBackingReport(scenario, 'nhsp_final', { reportHeadingName: 'Scenario Agency Name' });
  assert.equal(artifact.sha256, repeat.sha256);
  const ws = workbook(artifact).Sheets.Export;
  assert.equal(ws.A1.v, 'Agency Backing Report 1740430 for Scenario Agency Name Agency');
  assert.equal(ws.L4.v, 20 / 24);
  assert.equal(ws.O4.v, 660 / 1440);
  assert.equal(ws.P4.v, 52.5);
  assert.equal(ws.R4.v, 209.85);
  assert.equal(ws.P6.v, -52.5);
  assert.equal(ws.R6.v, -209.85);
  assert.equal(ws.R9.v, 0);
  const expected = buildExpectedSourceModel(scenario, 'nhsp_final');
  assert.equal(expected.rows[0].nhspSignedInvoiceExVatPence, '26235');
  assert.equal(expected.rows[1].nhspSignedInvoiceExVatPence, '-26235');
  assert(!Object.hasOwn(expected.rows[0], 'calculatedInvoiceExVatPence'));
});

test('TH-009 generated NHSP mutation corpus contains mixed-sign, lexical negative-zero, wrong-header and wrong-sheet cases', () => {
  const mixed = scenarioFacts();
  mixed.sourceUploads.find((item) => item.key === 'nhsp_final').mutations = ['MIXED_SIGN'];
  const mixedSheet = workbook(buildNhspBackingReport(mixed, 'nhsp_final', { reportHeadingName: 'Scenario Agency Name' })).Sheets.Export;
  assert(mixedSheet.P4.v < 0);
  assert(mixedSheet.R4.v > 0);

  const negativeZero = scenarioFacts();
  negativeZero.sourceUploads.find((item) => item.key === 'nhsp_final').mutations = ['NEGATIVE_ZERO'];
  const negativeZeroSheet = workbook(buildNhspBackingReport(negativeZero, 'nhsp_final', { reportHeadingName: 'Scenario Agency Name' })).Sheets.Export;
  assert.equal(negativeZeroSheet.P4.t, 's');
  assert.equal(negativeZeroSheet.P4.v, '-0.00');

  const wrongHeader = scenarioFacts();
  wrongHeader.sourceUploads.find((item) => item.key === 'nhsp_final').mutations = ['WRONG_HEADER'];
  assert.equal(workbook(buildNhspBackingReport(wrongHeader, 'nhsp_final', { reportHeadingName: 'Scenario Agency Name' })).Sheets.Export.A2.v, 'Worked Date');

  const wrongSheet = scenarioFacts();
  wrongSheet.sourceUploads.find((item) => item.key === 'nhsp_final').mutations = ['WRONG_SHEET'];
  assert.deepEqual(workbook(buildNhspBackingReport(wrongSheet, 'nhsp_final', { reportHeadingName: 'Scenario Agency Name' })).SheetNames, ['Unexpected']);
});

test('TH-010 HealthRoster Layout A reproduces the locked header and keeps planned times separate from Actual facts', () => {
  assert.equal(canonicalDigest(HEALTHROSTER_LAYOUT_A_HEADERS), '6951f0a3da7e073b3464592601b679af718ea0b6357173ff42a40b5069418aa8');
  const scenario = scenarioFacts();
  const artifact = buildHealthRosterLayoutAWorkbook(scenario, 'hr_a');
  assert.equal(artifact.sha256, buildHealthRosterLayoutAWorkbook(scenario, 'hr_a').sha256);
  const ws = workbook(artifact).Sheets.Export;
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true, defval: null });
  assert.deepEqual(rows[0], [...HEALTHROSTER_LAYOUT_A_HEADERS]);
  const header = rows[0];
  assert.equal(rows[1][header.indexOf('From')], 6 / 24);
  assert.equal(rows[1][header.indexOf('Start')], 20 / 24);
  assert.equal(rows[1][header.indexOf('Actual Break')], 60);
  assert.equal(rows[1][header.indexOf('Hours')], 660 / 1440);
  assert(rows[1][header.indexOf('Finalised Date')] > 0);
  assert.equal(rows[1][header.indexOf('Timesheet Finalised By')], 'Scenario Finaliser');
  assert.equal(rows[1][header.indexOf('Actual Cost')], 999.93);
});

test('TH-010 Layout A emits blank/duplicate Request Id and inconsistent finalisation variants without inference', () => {
  const scenario = scenarioFacts();
  const source = scenario.sourceUploads.find((item) => item.key === 'hr_a');
  source.mutations = ['BLANK_REQUEST_ID', 'DUPLICATE_REQUEST_ID', 'MIXED_FINALISATION'];
  const rows = XLSX.utils.sheet_to_json(workbook(buildHealthRosterLayoutAWorkbook(scenario, 'hr_a')).Sheets.Export, { header: 1, raw: true, defval: null });
  const header = rows[0];
  assert.equal(rows[1][header.indexOf('Request Id')], null);
  assert.equal(rows[2][header.indexOf('Request Id')], null);
  assert(rows[1][header.indexOf('Finalised Date')] > 0);
  assert.equal(rows[1][header.indexOf('Timesheet Finalised By')], null);
});

test('TH-011 HealthRoster Layout B reproduces the locked header and leaves an unfinalised/unfilled row blank', () => {
  assert.equal(canonicalDigest(HEALTHROSTER_LAYOUT_B_HEADERS), '2cee75a3ea40dd58ee19da519c032897ead594b91d27347f2ed2edf562eb28ec');
  const scenario = scenarioFacts();
  const artifact = buildHealthRosterLayoutBWorkbook(scenario, 'hr_b');
  const rows = XLSX.utils.sheet_to_json(workbook(artifact).Sheets.Export, { header: 1, raw: true, defval: null });
  assert.deepEqual(rows[0], [...HEALTHROSTER_LAYOUT_B_HEADERS]);
  const header = rows[0];
  assert.equal(rows[1][header.indexOf('Start')], 6 / 24);
  assert.equal(rows[1][header.indexOf('Actual Start')], 20 / 24);
  assert.equal(rows[1][header.indexOf('Status')], 'Timesheet Finalised');
  assert.equal(rows[1][header.indexOf('Timesheet Finalised By')], 'Scenario Finaliser');
  assert.equal(rows[2][header.indexOf('Status')], 'Informed Agency');
  assert.equal(rows[2][header.indexOf('Actual Start')], null);
  assert.equal(rows[2][header.indexOf('Actual Hours')], null);
  assert.equal(rows[2][header.indexOf('Timesheet Finalised By')], null);
});

test('TH-007/TH-012 dispatch is fail-closed for generic and no-source profiles', () => {
  const scenario = scenarioFacts();
  assert.throws(() => buildSourceFile(scenario, 'generic'), (error) => error instanceof SourceFixtureError && error.code === 'GENERIC_PROFILE_NOT_CONFIGURED');
  const generic = buildSourceFile(scenario, 'generic', { genericProfile });
  assert.deepEqual(csvRows(generic)[0], genericProfile.headers);
  const empty = scenarioFacts();
  empty.sourceUploads.find((item) => item.key === 'generic').physicalRows = [];
  assert.equal(csvRows(buildSourceFile(empty, 'generic', { genericProfile })).length, 1);
  const unknown = scenarioFacts();
  unknown.sourceUploads.find((item) => item.key === 'generic').profile = 'UNREGISTERED_PROFILE';
  assert.throws(() => buildSourceFile(unknown, 'generic', { genericProfile }), (error) => error.code === 'SOURCE_PROFILE_UNSUPPORTED');

  const none = scenarioFacts();
  none.sourceUploads.push(upload('none', 'NONE_ORDINARY_WEEKLY', 'NONE', 'client_ordinary', [], {
    cycleUtc: null, coverageStart: null, coverageEnd: null, complete: false
  }));
  const noFile = buildSourceFile(none, 'none');
  assert.equal(noFile.byteCount, 0);
  assert.equal(noFile.bytes, null);
});

test('TH-012 configurable source-fixed expense/whole-shift CSV matches evidence headers and cannot consume ordinary evidence expenses', () => {
  const scenario = scenarioFacts();
  const artifact = buildSourceFile(scenario, 'fixed_expense', { genericProfile: fixedExpenseProfile });
  const rows = csvRows(artifact);
  assert.deepEqual(rows[0], [...SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS]);
  assert.equal(canonicalDigest(SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS), 'ace7c49ef4cf1308c4fdc55220e018562e2f0c2e46f9f148687ea116eb2d342e');
  const header = rows[0];
  assert.equal(rows[1][header.indexOf('Expenses')], '12.34');
  assert.equal(rows[1][header.indexOf('Total Hours')], '11.00');
  assert.equal(rows[1][header.indexOf('STD Hours')], '11.00');
  assert.equal(rows[1][header.indexOf('STD Unit Cost')], '9.00');
  assert.equal(rows[1][header.indexOf('Booking Start')], '2026-09-14 20:00:00');
  assert.equal(rows[1][header.indexOf('Booking End')], '2026-09-15 08:00:00');
  assert.equal(rows[1][header.indexOf('Billing Group Name')], scenario.foundation.clients.find((item) => item.key === 'client_fixed').name);

  const protectedOrdinary = scenarioFacts();
  const fixedUpload = protectedOrdinary.sourceUploads.find((item) => item.key === 'fixed_expense');
  fixedUpload.clientKey = 'client_ordinary';
  fixedUpload.physicalRows[0].contractKey = 'contract_ordinary';
  const ordinaryClient = protectedOrdinary.foundation.clients.find((item) => item.key === 'client_ordinary');
  ordinaryClient.settings.sourceSuppliedExpenses = true;
  ordinaryClient.settings.calculationMode = 'WHOLE_SHIFT';
  assert.throws(
    () => buildSourceFile(protectedOrdinary, 'fixed_expense', { genericProfile: fixedExpenseProfile }),
    (error) => error.code === 'ORDINARY_EXPENSES_PROTECTED'
  );
});
