#!/usr/bin/env node
import path from 'node:path';
import { lstat, mkdir, readdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { canonicalJsonPretty } from '../tests/weekly-source/harness/canonical-json.mjs';
import { runWeeklySourceComponentPostgresHarness } from '../tests/weekly-source/harness/component-postgres-cluster.mjs';
import { executeWeeklySourceComponentDatabaseScenarios } from '../tests/weekly-source/adapters/database-scenario-adapter.mjs';
import { createRealWorldScenarioDependencies } from '../tests/weekly-source/adapters/real-world-product-database-adapter.mjs';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

async function requireEmptyDirectory(value) {
  const resolved = path.resolve(value);
  const stat = await lstat(resolved).catch(() => null);
  if (stat) {
    if (!stat.isDirectory() || stat.isSymbolicLink() || (await readdir(resolved)).length !== 0) {
      throw new Error('CLOUDTMS_WEEKLY_SOURCE_COMPONENT_RESULT_DIR must be a new or empty regular directory');
    }
  } else await mkdir(resolved, { recursive: true });
  return resolved;
}

function safe(error) {
  return String(error?.message ?? error)
    .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
    .replace(/\b(password|passphrase|secret|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]');
}

try {
  if (process.env.CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB !== '1') {
    throw new Error('CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB=1 is required');
  }
  const resultDirectory = await requireEmptyDirectory(required('CLOUDTMS_WEEKLY_SOURCE_COMPONENT_RESULT_DIR'));
  const mode = String(process.env.CLOUDTMS_WEEKLY_SOURCE_COMPONENT_MODE ?? 'UPGRADE').toUpperCase();
  if (!['NEW', 'UPGRADE'].includes(mode)) {
    throw new Error('CLOUDTMS_WEEKLY_SOURCE_COMPONENT_MODE must be NEW or UPGRADE');
  }
  const result = await runWeeklySourceComponentPostgresHarness({
    mode,
    runId: required('CLOUDTMS_WEEKLY_SOURCE_COMPONENT_RUN_ID'),
    repoRoot,
    snapshotPath: mode === 'UPGRADE' ? required('CLOUDTMS_WEEKLY_SOURCE_UPGRADE_SNAPSHOT') : null,
    snapshotSha256: mode === 'UPGRADE' ? required('CLOUDTMS_WEEKLY_SOURCE_UPGRADE_SNAPSHOT_SHA256') : null,
    resultDirectory,
    psqlBin: process.env.PSQL_BIN,
    pgRestoreBin: process.env.PG_RESTORE_BIN,
    preserveOnFailure: process.env.CLOUDTMS_WEEKLY_SOURCE_PRESERVE_DB_ON_FAILURE === '1',
    executeScenarios: (input) => executeWeeklySourceComponentDatabaseScenarios({
      ...input,
      scenarioDirectory: path.join(repoRoot, 'tests', 'fixtures', 'weekly-source', 'scenarios'),
      createRealWorldScenarioDependencies,
    }),
  });
  await writeFile(path.join(resultDirectory, 'weekly-source-component-postgres-result.json'), canonicalJsonPretty(result), { encoding: 'utf8', flag: 'wx' });
  console.log(`Weekly Source ${mode} component PostgreSQL proof passed: ${result.migrationCount} migrations, ${result.repeatableCount} repeatables, ${result.scenarioEvidence.groupCount} verifier groups.`);
} catch (error) {
  console.error(`${String(error?.code ?? 'WEEKLY_SOURCE_COMPONENT_FAILED')}: ${safe(error)}`);
  process.exitCode = 1;
}
