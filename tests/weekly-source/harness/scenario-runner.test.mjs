import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { canonicalDigest } from './canonical-json.mjs';
import { runWeeklySourceScenario } from './scenario-runner.mjs';
import { loadScenarioFile } from './scenario-loader.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const fixturePath = path.resolve(here, '../../fixtures/weekly-source/scenarios/WS-HARNESS-FOUNDATION-001.json');

function dependencies({ seededOutcome = false } = {}) {
  let plan = null;
  return {
    foundation: {
      async applyPrerequisites(input) { plan = input.plan; },
      async auditPrerequisites() {
        return {
          passed: true,
          scenarioId: plan.scenarioId,
          planDigest: plan.planDigest,
          preconditionDigest: canonicalDigest(plan),
        };
      },
    },
    async parseSource() { throw new Error('No-source fixture must not invoke the parser'); },
    async executeProductAction() {
      return { owner: 'TEST_PRODUCT_OWNER', seededOutcome, result: { ok: true } };
    },
    async collectObservedState({ scenario }) { return structuredClone(scenario.expected); },
    async collectProjections() { return [{ name: 'foundation', text: 'No visible change' }]; },
    async collectCoverage() {
      return { acceptanceIds: ['SRC-001'], protectedIds: ['PROT-SEC-001'], modelIds: [] };
    },
    async repositoryEvidence() {
      return [{ name: 'cloudtms-backend', commit: 'a'.repeat(40), dirtyOwnershipDigest: 'b'.repeat(64) }];
    },
    async databaseEvidence() { return { mode: 'LOCAL_SCENARIO_ADAPTER', supportedVersion: '17.11' }; },
    async cleanupScenario() { return { complete: true, freshProbe: 'ABSENT' }; },
  };
}

test('TH-018/019 runner uses injected owners, blocks network and emits PASS only after cleanup', async () => {
  const scenario = await loadScenarioFile(fixturePath);
  const envelope = await runWeeklySourceScenario({ scenario, dependencies: dependencies() });
  assert.equal(envelope.status, 'PASS');
  assert.equal(envelope.cleanup.complete, true);
  assert.deepEqual(envelope.acceptanceIds, ['SRC-001']);
  assert.deepEqual(envelope.protectedIds, ['PROT-SEC-001']);
  assert.match(envelope.evidenceDigest, /^[a-f0-9]{64}$/);
  assert.equal(envelope.outbox.email, 0);
});

test('TH-018 runner refuses an injected action adapter that claims direct outcome seeding', async () => {
  const raw = JSON.parse(await readFile(fixturePath, 'utf8'));
  raw.actions = [{ kind: 'READ_PROJECTION', atUtc: '2026-09-15T10:00:00Z' }];
  await assert.rejects(
    () => runWeeklySourceScenario({ scenario: raw, dependencies: dependencies({ seededOutcome: true }) }),
    (error) => error.code === 'SCENARIO_PRODUCT_OUTCOME_SEED_FORBIDDEN',
  );
});
