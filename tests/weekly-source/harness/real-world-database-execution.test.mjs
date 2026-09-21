import assert from 'node:assert/strict';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { executeRealWorldDatabaseJourneys } from '../adapters/real-world-database-execution.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const scenarioDirectory = path.resolve(here, '../../fixtures/weekly-source/scenarios');

test('real-world NEW/UPGRADE execution refuses a missing product/database adapter before any scenario can pass', async () => {
  await assert.rejects(
    executeRealWorldDatabaseJourneys({
      mode: 'NEW',
      scenarioDirectory,
      resultDirectory: path.join(here, '.not-created'),
      createScenarioDependencies: null,
      database: {},
    }),
    (error) => error.code === 'WEEKLY_SOURCE_REAL_WORLD_PRODUCT_ADAPTER_REQUIRED',
  );
});

test('real-world execution accepts only the two controlled PostgreSQL modes', async () => {
  await assert.rejects(
    executeRealWorldDatabaseJourneys({
      mode: 'COMPONENT',
      scenarioDirectory,
      resultDirectory: path.join(here, '.not-created'),
      createScenarioDependencies: () => ({}),
      database: {},
    }),
    (error) => error.code === 'WEEKLY_SOURCE_REAL_WORLD_MODE_INVALID',
  );
});
