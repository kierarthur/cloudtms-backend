import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { canonicalDigest } from './canonical-json.mjs';
import {
  loadScenarioFile,
  loadScenarioFiles,
  loadScenarioSchema,
  loadScenariosFromDirectory,
  ScenarioContractError,
  validateScenarioObject
} from './scenario-loader.mjs';
import { cleanupScenarioWorkspace, createScenarioWorkspace } from './scenario-cleanup.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const fixtureDirectory = path.resolve(here, '../../fixtures/weekly-source/scenarios');
const fixturePath = path.join(fixtureDirectory, 'WS-HARNESS-FOUNDATION-001.json');

async function fixtureClone() {
  return JSON.parse(await readFile(fixturePath, 'utf8'));
}

async function expectContractCode(promise, code) {
  await assert.rejects(promise, (error) => error instanceof ScenarioContractError && error.code === code);
}

test('TH-001 checked schema and foundation scenario validate with a stable contract digest', async () => {
  const schema = await loadScenarioSchema();
  assert.equal(schema.$id, 'https://cloudtms.test/contracts/weekly-source-test-scenario-v1.schema.json');
  assert.equal(canonicalDigest(schema), 'f7f6193c9135bf62e85aeb7a9237aabb7ab25bc318a6071efd245a49abb2aff5');
  const scenario = await loadScenarioFile(fixturePath, { schema });
  assert.equal(scenario.scenarioId, 'WS-HARNESS-FOUNDATION-001');
  assert(Object.isFrozen(scenario));
});

test('TH-002 directory loading is sorted and reproducible', async () => {
  const first = await loadScenariosFromDirectory(fixtureDirectory);
  const second = await loadScenariosFromDirectory(fixtureDirectory);
  assert.equal(first.digest, second.digest);
  assert.deepEqual(first.scenarios.map((scenario) => scenario.scenarioId), [
    'WS-HARNESS-FOUNDATION-001',
    'WS-REAL-WORLD-NHSP-001',
    'WS-REAL-WORLD-ROSTER-001',
  ]);
});

test('TH-002 refuses an empty scenario set', async () => {
  await expectContractCode(loadScenarioFiles([]), 'SCENARIO_SET_EMPTY');
});

test('TH-001 validates the complete nested fact vocabulary without running product logic', async () => {
  const schema = await loadScenarioSchema();
  const scenario = await fixtureClone();
  scenario.foundation.candidates.push({
    key: 'candidate_1', displayName: 'Test Candidate', tmsRef: 'CAN-TEST-1', payMethod: 'PAYE', umbrellaKey: null, active: true, activeDeviceCount: 1
  });
  scenario.foundation.clients.push({
    key: 'client_1',
    name: 'Test Trust',
    sourceAuthority: 'SOURCE',
    managerEmail: 'manager@example.test',
    settings: {
      selfBill: true,
      nhsp: true,
      noTimesheetRequired: true,
      requiresHealthRoster: false,
      referenceRequiredBeforePay: false,
      autoAuthorise: true,
      candidateQueriesEnabled: true,
      managerQueriesEnabled: true,
      sourceSuppliedExpenses: false,
      sourceExpenseVatChargeable: false,
      breakEntryMode: 'DURATION_MINUTES',
      calculationMode: 'SPLIT_RATE',
      cutoffWeekday: 3,
      cutoffLocalTime: '15:00'
    }
  });
  const rates = { day: '1000', night: '1200', saturday: '1300', sunday: '1400', bankHoliday: '1500' };
  scenario.foundation.contracts.push({
    key: 'contract_1', candidateKey: 'candidate_1', clientKey: 'client_1', startDate: '2026-09-01', endDate: '2026-12-31',
    role: 'Nurse', band: 'Band 5', payMethod: 'PAYE', rates: { pay: rates, charge: rates },
    additionalUnits: [{ code: 'BONUS', payPence: '100', chargePence: '100' }]
  });
  scenario.foundation.weeks.push({ key: 'week_1', contractKey: 'contract_1', weekEndingDate: '2026-09-20', additionalSequence: 0, status: 'SUBMITTED' });
  const shift = {
    key: 'shift_1', workDate: '2026-09-15', start: '09:00', end: '17:00', breakMinutes: 30,
    breakStart: null, breakEnd: null, workedMinutes: 450, reference: 'REQ-1', additionalUnits: [{ code: 'BONUS', quantity: '1' }]
  };
  scenario.foundation.timesheets.push({
    key: 'timesheet_1', weekKey: 'week_1', submissionState: 'SUBMITTED', candidateSigned: true, managerSigned: false,
    shifts: [shift], expenses: { mode: 'NONE', amountPence: '0', evidenceCount: 0 }
  });
  scenario.sourceUploads.push({
    key: 'upload_1', profile: 'NHSP_FINAL_BACKING_V1', stage: 'FINAL', clientKey: 'client_1', trustName: 'Test Trust',
    reportNumber: 'BACKING-1', cycleUtc: '2026-09-16T14:00:00Z', coverageStart: '2026-09-15', coverageEnd: '2026-09-15', complete: true,
    physicalRows: [{
      key: 'source_row_1', candidateKey: 'candidate_1', contractKey: 'contract_1', requestId: 'REQ-1', workDate: '2026-09-15',
      actualStart: '09:00', actualEnd: '17:00', actualBreakMinutes: 30, actualWorkedMinutes: 450,
      sign: 'POSITIVE', finalisation: 'FINALISED', statusText: 'Finalised', finalisedBy: 'Test Manager',
      commissionPence: '5250', totalCostPence: '20985', sourceExpensePence: null, role: 'Nurse', band: 'Band 5'
    }]
  });
  scenario.actions.push({ kind: 'UPLOAD_SOURCE', atUtc: '2026-09-16T14:00:00Z', actorKey: 'office_1', uploadKey: 'upload_1' });
  scenario.expected = {
    outcome: 'SUCCESS',
    sourceMovements: [{ sourceRowKey: 'source_row_1', kind: 'PHYSICAL_POSITIVE', workedMinutes: 450, invoiceExVatPence: '26235' }],
    invoiceLines: [{ sourceRowKey: 'source_row_1', clientKey: 'client_1', cycleKey: 'cycle_1', reportNumber: 'BACKING-1', exVatPence: '26235' }],
    approvedHours: [{ candidateKey: 'candidate_1', contractKey: 'contract_1', weekKey: 'week_1', shifts: [shift] }],
    communications: [{ audience: 'OFFICE', kind: 'IMPORT_READY', count: 1, groupKey: 'upload_1' }],
    c1PublicationCategory: 'NONE', visibleAssertions: ['Ready to finalise'], forbiddenAssertions: ['Password']
  };
  assert.doesNotThrow(() => validateScenarioObject(scenario, schema));
});

test('TH-001 rejects unknown properties, invalid pence and unapproved source profiles', async () => {
  const workspace = await createScenarioWorkspace('WS-HARNESS-LOADER-NEGATIVE');
  try {
    const unknown = await fixtureClone();
    unknown.unapproved = true;
    const unknownPath = path.join(workspace.path, 'unknown.json');
    await writeFile(unknownPath, JSON.stringify(unknown));
    await expectContractCode(loadScenarioFile(unknownPath), 'SCENARIO_SCHEMA_INVALID');

    const invalidPence = await fixtureClone();
    invalidPence.expected.invoiceLines.push({ sourceRowKey: 'row_1', clientKey: 'client_1', cycleKey: 'cycle_1', exVatPence: '01' });
    const pencePath = path.join(workspace.path, 'pence.json');
    await writeFile(pencePath, JSON.stringify(invalidPence));
    await expectContractCode(loadScenarioFile(pencePath), 'SCENARIO_SCHEMA_INVALID');

    const invalidProfile = await fixtureClone();
    invalidProfile.sourceUploads.push({ key: 'upload_1', profile: 'BEST_EFFORT', stage: 'FINAL', clientKey: 'client_1', physicalRows: [] });
    const profilePath = path.join(workspace.path, 'profile.json');
    await writeFile(profilePath, JSON.stringify(invalidProfile));
    await expectContractCode(loadScenarioFile(profilePath), 'SCENARIO_SCHEMA_INVALID');
  } finally {
    await cleanupScenarioWorkspace(workspace);
  }
});

test('TH-002 refuses duplicate immutable scenario IDs even when file ordering changes', async () => {
  const workspace = await createScenarioWorkspace('WS-HARNESS-DUPLICATE-ID');
  try {
    const first = await fixtureClone();
    const second = await fixtureClone();
    second.title = 'Same immutable identity from another file';
    const firstPath = path.join(workspace.path, 'z.json');
    const secondPath = path.join(workspace.path, 'a.json');
    await writeFile(firstPath, JSON.stringify(first));
    await writeFile(secondPath, JSON.stringify(second));
    await expectContractCode(loadScenarioFiles([firstPath, secondPath]), 'SCENARIO_ID_DUPLICATE');
    await expectContractCode(loadScenarioFiles([secondPath, firstPath]), 'SCENARIO_ID_DUPLICATE');
  } finally {
    await cleanupScenarioWorkspace(workspace);
  }
});

test('TH-001 refuses connection material, HTTP endpoints and SQL disguised as scenario text', async () => {
  const workspace = await createScenarioWorkspace('WS-HARNESS-PROHIBITED-CONTENT');
  try {
    const cases = [
      ['database-url.json', 'postgresql://example.invalid/db', 'SCENARIO_DATABASE_URL_FORBIDDEN'],
      ['endpoint.json', 'https://example.invalid/production', 'SCENARIO_ENDPOINT_FORBIDDEN'],
      ['sql.json', 'SELECT value FROM restricted_table', 'SCENARIO_SQL_FORBIDDEN']
    ];
    for (const [name, title, code] of cases) {
      const scenario = await fixtureClone();
      scenario.title = title;
      const target = path.join(workspace.path, name);
      await writeFile(target, JSON.stringify(scenario));
      await expectContractCode(loadScenarioFile(target), code);
    }
  } finally {
    await cleanupScenarioWorkspace(workspace);
  }
});
