#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { lstat, mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { canonicalJsonPretty } from '../tests/weekly-source/harness/canonical-json.mjs';
import {
  loadAndVerifyControllingLedgers,
  readResultEnvelopes,
  verifyExecutedCoverage,
  WEEKLY_SOURCE_CONTROL_SETS,
} from '../tests/weekly-source/harness/evidence-coverage-ledger.mjs';
import { WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT } from '../tests/weekly-source/harness/differential-protection-contract.mjs';
import { createPhaseController, WEEKLY_SOURCE_PHASE_ORDER } from '../tests/weekly-source/harness/phase-controller.mjs';
import { runPostgresHarness } from '../tests/weekly-source/harness/postgres-cluster.mjs';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const harnessRoot = path.join(repoRoot, 'tests', 'weekly-source', 'harness');
const weeklyTestRoot = path.join(repoRoot, 'tests', 'weekly-source');
const command = process.argv[2];

function fail(code, message) {
  const error = new Error(message);
  error.code = code;
  throw error;
}

function safeError(error) {
  const message = String(error?.message ?? 'Weekly Source harness failed')
    .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
    .replace(/\bBearer\s+\S+/gi, 'Bearer [redacted]')
    .replace(/\b(password|passphrase|secret|api[_-]?key|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]');
  return {
    code: String(error?.code ?? error?.name ?? 'WEEKLY_SOURCE_HARNESS_FAILED'),
    message,
  };
}

async function harnessSourceDigest() {
  const hash = createHash('sha256');
  const visit = async (directory) => {
    const entries = await readdir(directory, { withFileTypes: true });
    for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name, 'en'))) {
      const target = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(target);
      else if (entry.isFile()) {
        hash.update(path.relative(repoRoot, target).replaceAll('\\', '/')).update('\0');
        hash.update(await readFile(target)).update('\0');
      }
    }
  };
  await visit(harnessRoot);
  hash.update('scripts/run-weekly-source-harness.mjs\0').update(await readFile(fileURLToPath(import.meta.url)));
  return hash.digest('hex');
}

function runNodeTests(files, label) {
  const result = spawnSync(process.execPath, ['--test', ...files], {
    cwd: repoRoot,
    env: { ...process.env, NODE_ENV: 'test' },
    encoding: 'utf8',
    timeout: 600_000,
    maxBuffer: 128 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.status !== 0) {
    const bounded = String(result.stderr || result.stdout || result.error?.message || `exit ${result.status}`).slice(-4000);
    const error = new Error(`${label} failed.\n${bounded}`);
    error.code = 'WEEKLY_SOURCE_NODE_TESTS_FAILED';
    throw error;
  }
  const output = String(result.stdout ?? '');
  return {
    label,
    fileCount: files.length,
    outputSha256: createHash('sha256').update(output).digest('hex'),
  };
}

async function allHarnessTestFiles() {
  const entries = await readdir(harnessRoot, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith('.test.mjs'))
    .map((entry) => path.relative(repoRoot, path.join(harnessRoot, entry.name)))
    .sort((left, right) => left.localeCompare(right, 'en'));
}

async function buildersPhase(context) {
  const preferred = [
    'scenario-loader.test.mjs',
    'deterministic-identities.test.mjs',
    'fixture-builders.test.mjs',
    'oracle-result-cleanup.test.mjs',
    'external-effect-fakes.test.mjs',
    'test-only-reachability.test.mjs',
  ].map((name) => path.relative(repoRoot, path.join(harnessRoot, name)));
  const evidence = [runNodeTests(preferred, 'Weekly Source builder self-tests')];
  if (context.packRoot) {
    const ledgers = await loadAndVerifyControllingLedgers(context.packRoot);
    evidence.push({
      label: 'Plan 6.2 controlling ledgers',
      packVersion: ledgers.packVersion,
      controlSetCount: WEEKLY_SOURCE_CONTROL_SETS.length,
      controlSetRows: Object.fromEntries(WEEKLY_SOURCE_CONTROL_SETS.map((set) => [set.key, ledgers[set.key].length])),
      workItems: ledgers.workItems.length,
      // The Plan 6.2 rows that the generated-case ledger does not yet mark COVERED.
      // They are named here so a run record cannot describe them as already proved.
      pendingExecutionCount: ledgers.pendingExecution.length,
      reExecutionRequiredCount: ledgers.reExecutionRequired.length,
      reExecutionRequired: ledgers.reExecutionRequired,
      acceptanceWithoutAtomicRequirementCount: ledgers.acceptanceWithoutAtomicRequirement.length,
      digest: ledgers.digest,
    });
  }
  return { executed: true, pass: true, evidence, sourceDigest: context.sourceDigest };
}

async function unitPhase(context) {
  const files = await allHarnessTestFiles();
  return {
    executed: true,
    pass: true,
    evidence: [runNodeTests(files, 'Weekly Source pure harness tests')],
    sourceDigest: context.sourceDigest,
  };
}

async function importTestAdapter(filePath, requiredExport) {
  if (!filePath) fail('WEEKLY_SOURCE_PHASE_ADAPTER_REQUIRED', `${requiredExport} adapter path is required.`);
  const resolved = path.resolve(filePath);
  const relative = path.relative(weeklyTestRoot, resolved);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    fail('WEEKLY_SOURCE_PHASE_ADAPTER_REFUSED', 'Harness adapters must be inside tests/weekly-source.');
  }
  const stat = await lstat(resolved).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) fail('WEEKLY_SOURCE_PHASE_ADAPTER_INVALID', 'Harness adapter must be a regular, non-symbolic-link file.');
  const module = await import(`${pathToFileURL(resolved).href}?source=${encodeURIComponent((await harnessSourceDigest()).slice(0, 16))}`);
  if (typeof module[requiredExport] !== 'function') fail('WEEKLY_SOURCE_PHASE_ADAPTER_INVALID', `Harness adapter must export ${requiredExport}.`);
  return module[requiredExport];
}

async function databasePhase(mode, context) {
  if (process.env.CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB !== '1') {
    fail('WEEKLY_SOURCE_LOCAL_DB_OPT_IN_REQUIRED', 'Local task-owned database execution requires CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB=1.');
  }
  const adapter = await importTestAdapter(
    process.env.CLOUDTMS_WEEKLY_SOURCE_DB_ADAPTER,
    'executeWeeklySourceDatabaseScenarios',
  );
  const createRealWorldScenarioDependencies = await importTestAdapter(
    process.env.CLOUDTMS_WEEKLY_SOURCE_REAL_WORLD_DB_ADAPTER,
    'createRealWorldScenarioDependencies',
  );
  const baseRunId = process.env.CLOUDTMS_WEEKLY_SOURCE_DB_RUN_ID;
  if (!baseRunId) fail('WEEKLY_SOURCE_DB_RUN_ID_REQUIRED', 'CLOUDTMS_WEEKLY_SOURCE_DB_RUN_ID is required.');
  const result = await runPostgresHarness({
    mode,
    runId: `${baseRunId}-${mode.toLowerCase()}`,
    repoRoot,
    psqlBin: process.env.PSQL_BIN,
    pgRestoreBin: process.env.PG_RESTORE_BIN,
    allowImagePull: process.env.CLOUDTMS_WEEKLY_SOURCE_ALLOW_IMAGE_PULL === '1',
    reuseOwnedResources: process.env.CLOUDTMS_WEEKLY_SOURCE_REUSE_OWNED_DB === '1',
    preserveOnFailure: process.env.CLOUDTMS_WEEKLY_SOURCE_PRESERVE_DB_ON_FAILURE === '1',
    upgradeSnapshotPath: mode === 'UPGRADE' ? process.env.CLOUDTMS_WEEKLY_SOURCE_UPGRADE_SNAPSHOT : null,
    upgradeSnapshotSha256: mode === 'UPGRADE' ? process.env.CLOUDTMS_WEEKLY_SOURCE_UPGRADE_SNAPSHOT_SHA256 : null,
    upgradeExpectedEnvironment: mode === 'UPGRADE' ? process.env.CLOUDTMS_WEEKLY_SOURCE_UPGRADE_EXPECTED_ENVIRONMENT : null,
    upgradeExpectedCustomerKey: mode === 'UPGRADE' ? process.env.CLOUDTMS_WEEKLY_SOURCE_UPGRADE_EXPECTED_CUSTOMER_KEY : null,
    executeScenarios: (database) => adapter({
      ...database,
      repoRoot,
      scenarioDirectory: path.join(repoRoot, 'tests', 'fixtures', 'weekly-source', 'scenarios'),
      resultDirectory: context.resultDirectory,
      createRealWorldScenarioDependencies,
    }),
  });
  return {
    executed: true,
    pass: true,
    evidence: [{
      mode,
      database: result.database,
      serverVersion: result.serverVersion,
      releaseOutputDigest: result.releaseOutputDigest,
      scenarioEvidenceDigest: createHash('sha256').update(JSON.stringify(result.scenarioEvidence)).digest('hex'),
    }],
    cleanup: result.cleanup,
    sourceDigest: context.sourceDigest,
  };
}

async function adapterPhase(phase, environmentName, context) {
  const adapter = await importTestAdapter(process.env[environmentName], 'runWeeklySourceHarnessPhase');
  const result = await adapter({
    phase,
    repoRoot,
    packRoot: context.packRoot,
    scenarioDirectory: path.join(repoRoot, 'tests', 'fixtures', 'weekly-source', 'scenarios'),
    resultDirectory: context.resultDirectory,
    // The protected-function differential compares two task-owned local
    // PostgreSQL builds. Keep the target explicit and adapter-scoped; service
    // and browser adapters simply ignore this optional value.
    connectionUrl: process.env.CLOUDTMS_WEEKLY_SOURCE_DB_URL ?? null,
  });
  return { ...result, sourceDigest: context.sourceDigest };
}

async function resultFiles(directory) {
  if (!directory) fail('WEEKLY_SOURCE_RESULT_DIRECTORY_REQUIRED', 'The executed scenario result directory is required.');
  const stat = await lstat(directory).catch(() => null);
  if (!stat?.isDirectory() || stat.isSymbolicLink()) fail('WEEKLY_SOURCE_RESULT_DIRECTORY_INVALID', 'The scenario result directory is unavailable.');
  const entries = await readdir(directory, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith('.json'))
    .map((entry) => path.join(directory, entry.name));
}

async function modelPhase(context) {
  if (!context.packRoot) fail('WEEKLY_SOURCE_PACK_ROOT_REQUIRED', 'The controlling Plan 6.2 pack root is required for model coverage.');
  const ledgers = await loadAndVerifyControllingLedgers(context.packRoot);
  const files = await resultFiles(context.resultDirectory);
  const envelopes = await readResultEnvelopes(files);
  const coverage = verifyExecutedCoverage(ledgers, envelopes, { requireComplete: true });
  return {
    executed: true,
    pass: true,
    evidence: [{
      packVersion: ledgers.packVersion,
      resultCount: coverage.resultCount,
      controlSetCount: coverage.controlSetCount,
      covered: coverage.coveredCounts,
      required: Object.fromEntries(WEEKLY_SOURCE_CONTROL_SETS.map((set) => [set.key, set.requiredCount])),
      coveredControllingRequirements: coverage.coveredControllingRequirements,
      // Retained Plan 6 keys so an existing evidence reader keeps working.
      acceptance: coverage.coveredAcceptance,
      requirements: coverage.coveredRequirements,
      protected: coverage.coveredProtected,
      models: coverage.coveredModels,
      differentialAdapter: {
        status: WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.status,
        populated: WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.populated,
        baselinesCaptured: WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.baselinesCaptured,
      },
      evidenceDigest: coverage.evidenceDigest,
    }],
    sourceDigest: context.sourceDigest,
  };
}

function executors() {
  return {
    builders: buildersPhase,
    unit: unitPhase,
    'db:new': (context) => databasePhase('NEW', context),
    'db:upgrade': (context) => databasePhase('UPGRADE', context),
    service: (context) => adapterPhase('service', 'CLOUDTMS_WEEKLY_SOURCE_SERVICE_ADAPTER', context),
    browser: (context) => adapterPhase('browser', 'CLOUDTMS_WEEKLY_SOURCE_BROWSER_ADAPTER', context),
    differential: (context) => adapterPhase('differential', 'CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER', context),
    model: modelPhase,
  };
}

async function prepareContext(isAll) {
  const packRoot = process.env.CLOUDTMS_WEEKLY_SOURCE_PACK_ROOT ? path.resolve(process.env.CLOUDTMS_WEEKLY_SOURCE_PACK_ROOT) : null;
  const resultDirectory = process.env.CLOUDTMS_WEEKLY_SOURCE_RESULT_DIR
    ? path.resolve(process.env.CLOUDTMS_WEEKLY_SOURCE_RESULT_DIR)
    : null;
  const evidenceDirectory = process.env.CLOUDTMS_WEEKLY_SOURCE_EVIDENCE_DIR
    ? path.resolve(process.env.CLOUDTMS_WEEKLY_SOURCE_EVIDENCE_DIR)
    : null;
  if (isAll) {
    if (!packRoot || !resultDirectory || !evidenceDirectory) {
      fail('WEEKLY_SOURCE_ALL_PATHS_REQUIRED', 'Complete harness execution requires pack, result and evidence directories.');
    }
    const requiredInputs = [
      'CLOUDTMS_WEEKLY_SOURCE_DB_RUN_ID',
      'CLOUDTMS_WEEKLY_SOURCE_DB_ADAPTER',
      'CLOUDTMS_WEEKLY_SOURCE_REAL_WORLD_DB_ADAPTER',
      'CLOUDTMS_WEEKLY_SOURCE_SERVICE_ADAPTER',
      'CLOUDTMS_WEEKLY_SOURCE_BROWSER_ADAPTER',
      'CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER',
      'CLOUDTMS_WEEKLY_SOURCE_UPGRADE_SNAPSHOT',
      'CLOUDTMS_WEEKLY_SOURCE_UPGRADE_SNAPSHOT_SHA256',
      'CLOUDTMS_WEEKLY_SOURCE_UPGRADE_EXPECTED_ENVIRONMENT',
      'CLOUDTMS_WEEKLY_SOURCE_UPGRADE_EXPECTED_CUSTOMER_KEY',
    ].filter((name) => !process.env[name]);
    if (process.env.CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB !== '1') {
      requiredInputs.unshift('CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB=1');
    }
    if (requiredInputs.length) {
      fail('WEEKLY_SOURCE_ALL_INPUTS_REQUIRED', `Complete harness inputs are missing: ${requiredInputs.join(', ')}.`);
    }
    const relativeEvidence = path.relative(resultDirectory, evidenceDirectory);
    const relativeResults = path.relative(evidenceDirectory, resultDirectory);
    if (
      relativeEvidence === ''
      || (!relativeEvidence.startsWith('..') && !path.isAbsolute(relativeEvidence))
      || (!relativeResults.startsWith('..') && !path.isAbsolute(relativeResults))
    ) {
      fail('WEEKLY_SOURCE_OUTPUT_DIRECTORIES_OVERLAP', 'Result and phase-evidence directories must be separate, non-nested locations.');
    }
    for (const [directory, code, label] of [
      [resultDirectory, 'WEEKLY_SOURCE_RESULT_DIRECTORY_NOT_EMPTY', 'result'],
      [evidenceDirectory, 'WEEKLY_SOURCE_EVIDENCE_DIRECTORY_NOT_EMPTY', 'evidence'],
    ]) {
      const stat = await lstat(directory).catch(() => null);
      if (stat) {
        if (!stat.isDirectory() || stat.isSymbolicLink() || (await readdir(directory)).length !== 0) {
          fail(code, `Complete harness ${label} directory must be a new or empty regular directory.`);
        }
      } else await mkdir(directory, { recursive: true });
    }
  }
  return {
    packRoot,
    resultDirectory,
    evidenceDirectory,
    sourceDigest: await harnessSourceDigest(),
  };
}

async function writePhaseEvidence(directory, result) {
  if (!directory) return;
  const target = path.join(directory, `${result.phase.replace(':', '-')}.json`);
  await writeFile(target, canonicalJsonPretty(result), { encoding: 'utf8', flag: 'wx' });
}

async function main() {
  if (![...WEEKLY_SOURCE_PHASE_ORDER, 'all'].includes(command)) {
    fail('WEEKLY_SOURCE_COMMAND_INVALID', `Command must be ${[...WEEKLY_SOURCE_PHASE_ORDER, 'all'].join(', ')}.`);
  }
  const isAll = command === 'all';
  const context = await prepareContext(isAll);
  const controller = createPhaseController({ executors: executors() });
  if (isAll) {
    const result = await controller.runAll(context);
    for (const phase of result.results) await writePhaseEvidence(context.evidenceDirectory, phase);
    await writeFile(path.join(context.evidenceDirectory, 'all.json'), canonicalJsonPretty(result), { encoding: 'utf8', flag: 'wx' });
    console.log(`Weekly Source complete harness passed ${result.results.length} phases (${result.evidenceDigest}).`);
    return;
  }
  const result = await controller.runPhase(command, context);
  await writePhaseEvidence(context.evidenceDirectory, result);
  console.log(`Weekly Source ${command} passed (${result.evidenceDigest}).`);
}

try {
  await main();
} catch (error) {
  const safe = safeError(error);
  console.error(`${safe.code}: ${safe.message}`);
  process.exitCode = 1;
}
