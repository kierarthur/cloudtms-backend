import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { loadScenariosFromDirectory } from '../tests/weekly-source/harness/scenario-loader.mjs';
import { buildRealWorldActionExecutionPlan } from '../tests/weekly-source/harness/real-world-action-contract.mjs';
import { canonicalDigest } from '../tests/weekly-source/harness/canonical-json.mjs';

function value(flag) {
  const index = process.argv.indexOf(flag);
  if (index < 0 || !process.argv[index + 1]) throw new Error(`${flag} is required`);
  return path.resolve(process.argv[index + 1]);
}

const scenarioDirectory = value('--scenario-directory');
const output = value('--output');
const loaded = await loadScenariosFromDirectory(scenarioDirectory);
const scenarios = loaded.scenarios.filter((scenario) => scenario.tags?.includes('REAL_WORLD'));
if (scenarios.length !== 2) throw new Error(`Expected exactly two real-world scenarios, received ${scenarios.length}`);

const plans = scenarios.map((scenario) => {
  const execution = buildRealWorldActionExecutionPlan(scenario);
  return {
    scenarioId: scenario.scenarioId,
    scenarioInputDigest: canonicalDigest(scenario),
    expectedDigest: canonicalDigest(scenario.expected),
    modes: execution.modes.map((mode) => ({ mode, status: 'PENDING_STAGE2_EXECUTION' })),
    requiredObservations: execution.requiredObservations,
    actions: execution.actions.map((action) => ({
      actionNumber: action.actionNumber,
      kind: action.kind,
      atUtc: action.atUtc,
      owners: action.owners,
      observes: action.observes,
    })),
  };
});

const payload = {
  schemaVersion: 'WEEKLY_SOURCE_REAL_WORLD_EXECUTION_PLAN_V1',
  generatedAt: '2026-09-21',
  status: 'READY_FOR_STAGE2_NEW_AND_UPGRADE_EXECUTION',
  rule: 'No scenario may pass from parser output, copied expected data, or a broad suite result. Every action and every required observation must be returned by the named product owner on NEW and UPGRADE PostgreSQL 17.11.',
  executionGate: {
    databaseAdapter: 'tests/weekly-source/adapters/database-scenario-adapter.mjs',
    scenarioExecutor: 'tests/weekly-source/adapters/real-world-database-execution.mjs',
    productAdapterExport: 'createRealWorldScenarioDependencies',
    environmentInput: 'CLOUDTMS_WEEKLY_SOURCE_REAL_WORLD_DB_ADAPTER',
    invokedAutomaticallyBy: 'scripts/run-weekly-source-harness.mjs db:new|db:upgrade',
    failClosed: true,
  },
  plans,
};
payload.evidenceDigest = createHash('sha256').update(JSON.stringify(payload)).digest('hex');
await mkdir(path.dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(payload, null, 2)}\n`);
process.stdout.write(`${JSON.stringify({ status: payload.status, scenarios: plans.length, actions: plans.reduce((sum, plan) => sum + plan.actions.length, 0) })}\n`);
