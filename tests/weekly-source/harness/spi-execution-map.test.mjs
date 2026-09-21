import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assertSpiLedgerMatchesExecutionMap,
  spiIdsForEvidenceGroup,
  verifySpiExecutionEvidence,
  WEEKLY_SOURCE_SPI_EXECUTION_MAP,
} from './spi-execution-map.mjs';

const rows = WEEKLY_SOURCE_SPI_EXECUTION_MAP.map(({ scenarioId }) => ({ scenario_id: scenarioId }));
const exactOutcome = (scenarioId) => ({
  scenarioId,
  localResult: 'PASS',
  observations: {
    executedActions: ['EXECUTED_OWNER'],
    databaseReadbacks: ['READ_BACK'],
    prohibitedOutcomeChecks: ['ABSENT'],
    actualInvoiceResult: 'Observed invoice result',
    actualOfficeAuditResult: 'Observed Office/audit result',
  },
});
const envelope = (scenarioId, spiIds = [], { exact = false } = {}) => ({
  schemaVersion: 'WEEKLY_SOURCE_TEST_RESULT_V1',
  scenarioId,
  status: 'PASS',
  spiIds,
  actual: { spiOutcomes: exact ? spiIds.map(exactOutcome) : [] },
});

test('all 106 SPI rows have an explicit local/external ownership split', () => {
  assert.equal(WEEKLY_SOURCE_SPI_EXECUTION_MAP.length, 106);
  assert.equal(new Set(WEEKLY_SOURCE_SPI_EXECUTION_MAP.map((row) => row.scenarioId)).size, 106);
  assert.equal(assertSpiLedgerMatchesExecutionMap(rows), true);
  assert.equal(WEEKLY_SOURCE_SPI_EXECUTION_MAP.filter((row) => row.owner === 'WEEKLY_SOURCE').length, 47);
  assert.equal(WEEKLY_SOURCE_SPI_EXECUTION_MAP.filter((row) => row.owner === 'SHARED_WEEKLY_SOURCE_AND_HANDOVER2').length, 59);
  assert.equal(WEEKLY_SOURCE_SPI_EXECUTION_MAP.filter((row) => row.owner === 'HANDOVER2').length, 0);
  assert.deepEqual(spiIdsForEvidenceGroup('protected-differential'), ['SPI-046', 'SPI-047']);
  assert.deepEqual(spiIdsForEvidenceGroup('mytms-candidate'), [
    'SPI-061', 'SPI-062', 'SPI-063', 'SPI-064', 'SPI-092',
  ]);
});

test('an SPI row passes only when it is claimed and every required executable envelope passed', () => {
  const result = verifySpiExecutionEvidence(rows, [
    envelope('WS-SERVICE-PARSER-PROFILES-001', ['SPI-019']),
  ], { allowExternalHandover2: true });
  const row = result.rows.find((item) => item.scenarioId === 'SPI-019');
  assert.equal(row.result, 'MISSING');
  assert.deepEqual(row.missingEvidenceScenarioIds, [
    'WS-DATABASE-COMPONENT-NEW-PG17-001',
    'WS-DATABASE-COMPONENT-UPGRADE-PG17-001',
  ]);
});

test('listing an SPI id without its own observed outcome never claims it', () => {
  const result = verifySpiExecutionEvidence(rows, [
    envelope('WS-SERVICE-PARSER-PROFILES-001', ['SPI-001']),
  ], { allowExternalHandover2: true });
  const row = result.rows.find((item) => item.scenarioId === 'SPI-001');
  assert.equal(row.localResult, 'MISSING');
  assert.equal(row.claimed, false);
});

test('superseded SPI-029 becomes not applicable only after the PHD-014 replacement route is executed', () => {
  const result = verifySpiExecutionEvidence(rows, [
    envelope('WS-SERVICE-PARSER-PROFILES-001', ['SPI-029'], { exact: true }),
    envelope('WS-DATABASE-COMPONENT-NEW-PG17-001', ['SPI-029']),
    envelope('WS-DATABASE-COMPONENT-UPGRADE-PG17-001', ['SPI-029']),
  ], { allowExternalHandover2: true });
  const row = result.rows.find((item) => item.scenarioId === 'SPI-029');
  assert.equal(row.result, 'NOT_APPLICABLE');
  assert.equal(row.supersededBy, 'PHD-014');
});

test('SPI readiness proves the local limbs but never substitutes for the controlling completion matrix', () => {
  const localIds = WEEKLY_SOURCE_SPI_EXECUTION_MAP
    .filter((row) => row.owner !== 'HANDOVER2')
    .map((row) => row.scenarioId);
  const result = verifySpiExecutionEvidence(rows, [
    envelope('WS-SPI-LOCAL-EXACT-R28-001', localIds, { exact: true }),
    envelope('WS-SERVICE-PARSER-PROFILES-001', localIds),
    envelope('WS-DATABASE-COMPONENT-NEW-PG17-001', localIds),
    envelope('WS-DATABASE-COMPONENT-UPGRADE-PG17-001', localIds),
    envelope('WS-DIFFERENTIAL-PROTECTED-29-001', localIds),
    envelope('WS-MYTMS-CANDIDATE-MANAGER-EXACT-001', localIds),
    envelope('WS-BROWSER-OFFICE-COMPLETE-R26-001', localIds),
  ], { allowExternalHandover2: true });
  assert.equal(result.weeklySourceSpiLocalReady, true);
  assert.equal(result.weeklySourcePluginReady, false);
  assert.equal(result.pluginReadinessAuthority, 'COMBINED_IMPLEMENTATION_COMPLETION_MATRIX');
  assert.equal(result.weeklySourcePassCount, 105);
  assert.equal(result.weeklySourceNotApplicableCount, 1);
  assert.equal(result.externalPendingCount, 59);
  assert.equal(result.complete, false);
});
