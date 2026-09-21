import assert from 'node:assert/strict';
import { access, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { canonicalDigest } from './canonical-json.mjs';
import { buildExpectedOutcomeOracle, compareActualWithOracle } from './expected-outcome-oracle.mjs';
import { createResultEnvelope, writeResultEnvelope } from './result-envelope.mjs';
import {
  cleanupScenarioWorkspace,
  createScenarioWorkspace,
  ScenarioCleanupRegistry
} from './scenario-cleanup.mjs';
import { loadScenarioFile } from './scenario-loader.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const fixturePath = path.resolve(here, '../../fixtures/weekly-source/scenarios/WS-HARNESS-FOUNDATION-001.json');

test('TH-014 oracle expands only declared expected facts and detects a deliberate mutation', async () => {
  const scenario = await loadScenarioFile(fixturePath);
  const oracle = buildExpectedOutcomeOracle(scenario);
  assert.equal(oracle.arithmetic.invoiceExVatPence, '0');
  assert.equal(oracle.arithmetic.sourceMovementInvoiceExVatPence, '0');
  assert(compareActualWithOracle(scenario.expected, oracle).pass);

  const mutated = structuredClone(scenario.expected);
  mutated.outcome = 'WAITING';
  const comparison = compareActualWithOracle(mutated, oracle);
  assert.equal(comparison.pass, false);
  assert.deepEqual(comparison.firstDivergence, { path: '$.outcome', expected: 'NO_CHANGE', actual: 'WAITING' });
});

test('TH-029 result envelope is deterministic, bounded and contains cleanup proof', async () => {
  const scenario = await loadScenarioFile(fixturePath);
  const oracle = buildExpectedOutcomeOracle(scenario);
  const comparison = compareActualWithOracle(scenario.expected, oracle);
  const input = {
    scenario,
    repositories: [{ name: 'cloudtms-backend', commit: '44bf8b7bd6307df6c0fba9bdaedc7c4cdf978efc', dirtyOwnershipDigest: 'b'.repeat(64) }],
    database: { mode: 'NOT_RUN_FOUNDATION_SELF_TEST', supportedVersion: '17.11' },
    generatedSources: [],
    parser: { mode: 'NOT_RUN_FOUNDATION_SELF_TEST', normalFormDigest: null },
    clockValuesUtc: ['2026-09-15T09:00:00.000Z'],
    executedOwners: [{ kind: 'HARNESS', name: 'declared-outcome-oracle-v1' }],
    oracle,
    actual: scenario.expected,
    comparison,
    c1: { category: 'NONE', requestDigest: null },
    outbox: { email: 0, push: 0, provider: 0 },
    projectionDigests: [{ name: 'foundation', digest: 'c'.repeat(64) }],
    acceptanceIds: ['AT-HARNESS-001'],
    cleanup: { complete: true, freshProbe: 'ABSENT' }
  };
  const first = createResultEnvelope(input);
  const second = createResultEnvelope(input);
  assert.deepEqual(first, second);
  assert.match(first.evidenceDigest, /^[a-f0-9]{64}$/);
  assert.equal(first.status, 'PASS');

  const workspace = await createScenarioWorkspace('WS-HARNESS-RESULT-ENVELOPE');
  try {
    const target = path.join(workspace.path, 'result.json');
    await writeResultEnvelope(target, first);
    const saved = JSON.parse(await readFile(target, 'utf8'));
    assert.equal(canonicalDigest(saved), canonicalDigest(first));
  } finally {
    await cleanupScenarioWorkspace(workspace);
  }
});

test('TH-020 cleanup refuses a mismatched marker and proves an owned workspace absent', async () => {
  const workspace = await createScenarioWorkspace('WS-HARNESS-CLEANUP-BOUNDARY');
  await writeFile(path.join(workspace.path, 'scenario-output.txt'), 'temporary test output');
  const markerPath = path.join(workspace.path, '.cloudtms-weekly-source-harness.json');
  await writeFile(markerPath, JSON.stringify({ schemaVersion: 'WEEKLY_SOURCE_TEMP_WORKSPACE_V1', scenarioId: 'WS-WRONG-OWNER' }));
  await assert.rejects(() => cleanupScenarioWorkspace(workspace), /marker does not match/);
  await writeFile(markerPath, JSON.stringify({ schemaVersion: 'WEEKLY_SOURCE_TEMP_WORKSPACE_V1', scenarioId: workspace.scenarioId }));
  const proof = await cleanupScenarioWorkspace(workspace);
  assert.deepEqual(proof, {
    scenarioId: 'WS-HARNESS-CLEANUP-BOUNDARY',
    removed: true,
    freshProbe: 'ABSENT'
  });
  await assert.rejects(() => access(workspace.path));
});

test('TH-020 cleanup registry executes exact owners in reverse order and requires fresh proof', async () => {
  const events = [];
  const registry = new ScenarioCleanupRegistry('WS-HARNESS-CLEANUP-REGISTRY');
  registry.register('FIRST_RESOURCE', async () => events.push('cleanup:first'), async () => {
    events.push('probe:first');
    return { cleaned: true, residueCount: 0 };
  });
  registry.register('SECOND_RESOURCE', async () => events.push('cleanup:second'), async () => {
    events.push('probe:second');
    return { cleaned: true, residueCount: 0 };
  });
  const proof = await registry.run();
  assert.deepEqual(events, ['cleanup:second', 'probe:second', 'cleanup:first', 'probe:first']);
  assert.equal(proof.complete, true);
});

test('TH-020 cleanup registry continues other cleanup after one owner fails', async () => {
  const events = [];
  const registry = new ScenarioCleanupRegistry('WS-HARNESS-CLEANUP-FAILURE');
  registry.register('SURVIVING_RESOURCE', async () => events.push('cleanup:surviving'), async () => ({ cleaned: true }));
  registry.register('FAILED_RESOURCE', async () => {
    events.push('cleanup:failed');
    throw new Error('bounded failure');
  }, async () => ({ cleaned: false }));
  await assert.rejects(() => registry.run(), (error) => {
    assert.equal(error.code, 'WEEKLY_SOURCE_CLEANUP_INCOMPLETE');
    return true;
  });
  assert.deepEqual(events, ['cleanup:failed', 'cleanup:surviving']);
});
