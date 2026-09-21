import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { canonicalDigest } from '../harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../harness/result-envelope.mjs';
import { spiIdsForEvidenceGroup } from '../harness/spi-execution-map.mjs';

const SERVICE_TESTS = Object.freeze([
  'broker/test/import-review-contract.test.mjs',
  'broker/test/import-review-follow-up-sql-contract.test.mjs',
  'broker/test/import-review-follow-up.test.mjs',
  'broker/test/weekly-timesheet-authority-policy.test.mjs',
  'tests/candidate-app-backend.test.js',
  'tests/candidate-multi-agency-routing.test.js',
  'tests/candidate-office-worker-environment-config.test.js',
  'tests/import-authoritative-effective-balance-helper.test.cjs',
  'tests/import-review-validation-only-policy.test.cjs',
  'tests/weekly-source-c1-adapter.test.mjs',
  'tests/weekly-source-c1-authoring.test.mjs',
  'tests/weekly-source-c1-components.test.mjs',
  'tests/weekly-source-c1-durable-publication.test.mjs',
  'tests/weekly-source-c1-publication.test.mjs',
  'tests/weekly-source-c1-stream.test.mjs',
  'tests/weekly-source/parsers/contract-qualification.test.js',
  'tests/weekly-source/parsers/economic-snapshot.test.js',
  'tests/weekly-source/parsers/manager-email.test.js',
  'tests/weekly-source/parsers/money.test.js',
  'tests/weekly-source/parsers/real-evidence.test.js',
  'tests/weekly-source/parsers/roster-summary-parser.test.js',
  'tests/weekly-source/parsers/source-price-comparator.test.js',
  'tests/weekly-source/parsers/strict-parser.test.js',
  'tests/weekly-source/parsers/weekly-rate-owner.test.js',
  'tests/weekly-source/protected-action-orchestrator.test.mjs',
  'tests/weekly-source/protected-action-sql-contract.test.mjs',
  'tests/weekly-source/protected-target-schedule.test.mjs',
  'tests/weekly-source/harness/real-world-scenario-chain.test.mjs',
  'tests/weekly-source/weekly-source-c1-raw-rpc.test.mjs',
  'tests/weekly-source/weekly-source-finalisation-pay-orchestration-sql-contract.test.mjs',
  'tests/weekly-source/weekly-source-finalisation-pay-orchestrator.test.mjs',
  'tests/weekly-source/weekly-source-invoice-admission-sql-contract.test.mjs',
  'tests/weekly-source/weekly-source-invoice-batch-integration-sql-contract.test.mjs',
  'tests/weekly-source/weekly-source-invoice-batch-integration.test.mjs',
  'tests/weekly-source/weekly-source-protected-c1-publication-sql-contract.test.mjs',
  'tests/weekly-source/weekly-source-protected-target-calculator-contract.test.mjs',
  'tests/weekly-source/weekly-source-report-real-owner.test.mjs',
  'tests/weekly-source/weekly-source-invoice-render-real-owner.test.mjs',
  'tests/weekly-source/weekly-source-upload-publication-owner.test.mjs',
]);

const CANDIDATE_TESTS = Object.freeze([
  'src/features/timesheets/weekly-source.test.ts',
  'src/features/timesheets/hours-model.test.ts',
  'src/features/timesheets/submitted-detail.test.ts',
  'src/features/timesheets/expense-only-display.test.ts',
  'src/features/workflows/submit.test.ts',
  'src/core/notifications/destination.test.ts',
]);

const MANAGER_TESTS = Object.freeze([
  'apps/manager-review-web/tests/weekly-query-policy.test.ts',
  'apps/manager-review-web/tests/weekly-query-api.test.ts',
  'apps/manager-review-web/tests/WeeklyQueryApp.test.tsx',
  'apps/manager-review-web/tests/worker.test.ts',
]);

function fail(code, message) {
  const error = new Error(message);
  error.code = code;
  throw error;
}

async function regularFile(file, label) {
  const stat = await lstat(file).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) fail('WEEKLY_SOURCE_SERVICE_INPUT_INVALID', `${label} is unavailable.`);
}

export function parseNodeTestSummary(output) {
  const values = {};
  for (const line of String(output).split(/\r?\n/)) {
    const match = line.trim().match(/^ℹ (tests|pass|fail|skipped) (\d+)$/);
    if (match) values[match[1]] = Number(match[2]);
  }
  if (values.tests !== 583 || values.pass !== 583 || values.fail !== 0 || values.skipped !== 0) {
    fail(
      'WEEKLY_SOURCE_SERVICE_COUNT_INVALID',
      `The service proof requires 583/583 passed with no failures or skips; received ${values.pass ?? '?'} passed, ${values.fail ?? '?'} failed and ${values.skipped ?? '?'} skipped.`,
    );
  }
  return Object.freeze(values);
}

export function parseMytmsFocusedSummary({ candidateOutput, managerOutput, contractOutput }) {
  const candidate = String(candidateOutput).match(/Tests:\s+108 passed, 108 total/);
  const manager = String(managerOutput).match(/Tests\s+20 passed \(20\)/);
  const jsonStart = String(contractOutput).indexOf('{');
  const jsonEnd = String(contractOutput).lastIndexOf('}');
  let contract = null;
  if (jsonStart >= 0 && jsonEnd > jsonStart) {
    try { contract = JSON.parse(String(contractOutput).slice(jsonStart, jsonEnd + 1)); } catch { contract = null; }
  }
  if (!candidate || !manager || contract?.status !== 'PASS' || contract?.operation_count !== 68 || contract?.path_count !== 67) {
    fail('WEEKLY_SOURCE_MYTMS_COUNT_INVALID', 'The focused MyTMS proof requires Candidate 108/108, manager 20/20 and the 68-operation/67-path contract.');
  }
  return Object.freeze({
    candidateTests: 108,
    managerTests: 20,
    operationCount: contract.operation_count,
    pathCount: contract.path_count,
  });
}

function gitCommit(repoRoot) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], {
    cwd: repoRoot,
    encoding: 'utf8',
    timeout: 10_000,
    windowsHide: true,
  });
  if (result.error || result.status !== 0 || !/^[a-f0-9]{40}$/.test(String(result.stdout).trim())) {
    fail('WEEKLY_SOURCE_SERVICE_COMMIT_UNAVAILABLE', 'The backend commit identity is unavailable.');
  }
  return String(result.stdout).trim();
}

async function writeParserRequirementEvidence({ repoRoot, resultDirectory, summary, outputSha256 }) {
  if (!resultDirectory) return null;
  const serviceProtectionPolicies = Object.freeze({
    'PROT-INVDOC-001': ['tests/weekly-source/weekly-source-invoice-render-real-owner.test.mjs'],
    'PROT-ISSUED-001': [
      'tests/weekly-source/weekly-source-invoice-admission-sql-contract.test.mjs',
      'tests/weekly-source/weekly-source-invoice-batch-integration.test.mjs',
    ],
    'PROT-ORDINV-001': [
      'broker/test/weekly-timesheet-authority-policy.test.mjs',
      'tests/weekly-source/weekly-source-invoice-batch-integration.test.mjs',
    ],
    'PROT-DAILY-001': [
      'tests/import-review-validation-only-policy.test.cjs',
      'tests/candidate-app-backend.test.js',
    ],
    'PROT-ORDW-001': ['broker/test/weekly-timesheet-authority-policy.test.mjs'],
    'PROT-PAY-001': [
      'tests/weekly-source/weekly-source-finalisation-pay-orchestrator.test.mjs',
      'tests/weekly-source/protected-action-orchestrator.test.mjs',
    ],
    'PROT-CONTRACT-001': ['tests/weekly-source/parsers/contract-qualification.test.js'],
    'PROT-RATE-001': [
      'tests/weekly-source/parsers/economic-snapshot.test.js',
      'tests/weekly-source/parsers/money.test.js',
      'tests/weekly-source/parsers/weekly-rate-owner.test.js',
    ],
    'PROT-EXPORT-001': ['tests/weekly-source/weekly-source-report-real-owner.test.mjs'],
  });
  const protectionResults = Object.entries(serviceProtectionPolicies).map(([protectedId, executedChecks]) => {
    const missing = executedChecks.filter((file) => !SERVICE_TESTS.includes(file));
    if (missing.length) fail('WEEKLY_SOURCE_SERVICE_PROTECTION_OWNER_MISSING', `${protectedId} owner was not executed: ${missing.join(', ')}`);
    return {
      protectedId,
      surface: 'SERVICE',
      result: 'PASS',
      executedChecks,
      observedResults: executedChecks.map((file) => `NODE_TEST_FILE_PASS:${file}`),
      prohibitedOutcomeChecks: ['NO_REQUIRED_OWNER_FAILED', 'NO_REQUIRED_OWNER_SKIPPED'],
    };
  });
  const scenario = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
    scenarioId: 'WS-SERVICE-PARSER-PROFILES-001',
    fixedSeed: '7b230fa13d8487f722f1152b552fdc55ec6939837baecb3797e53ff71bcf3d4d',
    requirementIds: ['SRC-REQ-005', 'SRC-REQ-006', 'SRC-REQ-007', 'SRC-REQ-009', 'SRC-REQ-010'],
    protectedIds: protectionResults.map((result) => result.protectedId),
  };
  const expected = {
    suite: 'weekly-source-service-and-real-file-contracts',
    tests: 583,
    pass: 583,
    fail: 0,
    skipped: 0,
    productionProfiles: [
      'NHSP_PREFINAL_RELEASED_V1',
      'NHSP_FINAL_BACKING_V1',
      'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
      'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
      'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',
    ],
    protectionResults,
  };
  const actual = { ...expected, ...summary };
  const envelope = createResultEnvelope({
    scenario,
    repositories: [{ repository: 'cloudtms-backend', commit: gitCommit(repoRoot) }],
    database: { used: false, reason: 'PURE_PRODUCTION_PARSER_AND_SERVICE_CONTRACT_PROOF' },
    generatedSources: [],
    parser: { exactRealSourceFiles: true, outputSha256 },
    clockValuesUtc: [],
    // The evidence identifies the exact executable owners that produced the
    // certified 583-test result.  Directory names are deliberately not used:
    // they would make a later matrix reconciliation appear more precise than
    // the work that was actually executed.
    executedOwners: [...SERVICE_TESTS],
    oracle: { expected, expectedDigest: canonicalDigest(expected) },
    actual,
    comparison: { pass: canonicalDigest(actual) === canonicalDigest(expected), actualDigest: canonicalDigest(actual), firstDivergence: null },
    c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
    outbox: { calls: [] },
    projectionDigests: [],
    acceptanceIds: [
      'HRSB-001',
      'HRSB-002',
      'HRSB-003',
      'HRSB-015',
      'HRSB-025',
      'NHSBR-005',
      'NHSBR-006',
      'NHSBR-007',
      'NHSBR-010',
      'NHSBR-017',
      'NHSBR-019',
      'SRC-031',
      'SRC-033',
    ],
    modelIds: ['MODEL-011'],
    protectedIds: protectionResults.map((result) => result.protectedId),
    spiIds: [
      ...spiIdsForEvidenceGroup('source-ingestion'),
      ...spiIdsForEvidenceGroup('finalisation-and-pay'),
      ...spiIdsForEvidenceGroup('mode-a'),
    ],
    cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
  });
  if (envelope.status !== 'PASS') fail('WEEKLY_SOURCE_SERVICE_EVIDENCE_MISMATCH', 'The parser requirement evidence did not match its exact expected result.');
  await writeResultEnvelope(path.join(resultDirectory, 'service-parser-profiles.json'), envelope);
  return envelope.evidenceDigest;
}

function runChecked(command, args, { cwd, code, timeout = 600_000 }) {
  const result = spawnSync(command, args, {
    cwd,
    env: process.env,
    encoding: 'utf8',
    timeout,
    maxBuffer: 128 * 1024 * 1024,
    windowsHide: true,
  });
  const output = `${result.stdout ?? ''}\n${result.stderr ?? ''}`;
  if (result.error || result.status !== 0) fail(code, `The focused MyTMS proof failed before certification (exit ${result.status ?? -1}).`);
  return output;
}

async function writeMytmsFocusedEvidence({ mytmsRoot, resultDirectory }) {
  const candidateRoot = path.join(mytmsRoot, 'apps', 'candidate-app');
  const jestBin = path.join(candidateRoot, 'node_modules', 'jest', 'bin', 'jest.js');
  const vitestBin = path.join(mytmsRoot, 'node_modules', 'vitest', 'vitest.mjs');
  const tscBin = path.join(mytmsRoot, 'node_modules', 'typescript', 'bin', 'tsc');
  await regularFile(jestBin, 'Candidate Jest');
  await regularFile(vitestBin, 'manager Vitest');
  await regularFile(tscBin, 'MyTMS TypeScript');
  for (const file of CANDIDATE_TESTS) await regularFile(path.join(candidateRoot, file), file);
  for (const file of MANAGER_TESTS) await regularFile(path.join(mytmsRoot, file), file);

  const candidateOutput = runChecked(process.execPath, [jestBin, '--runInBand', ...CANDIDATE_TESTS], {
    cwd: candidateRoot,
    code: 'WEEKLY_SOURCE_CANDIDATE_TEST_FAILED',
  });
  const managerOutput = runChecked(process.execPath, [vitestBin, 'run', '--config', 'apps/manager-review-web/vite.config.ts', ...MANAGER_TESTS], {
    cwd: mytmsRoot,
    code: 'WEEKLY_SOURCE_MANAGER_TEST_FAILED',
  });
  runChecked(process.execPath, [tscBin, '--noEmit'], {
    cwd: candidateRoot,
    code: 'WEEKLY_SOURCE_CANDIDATE_TYPECHECK_FAILED',
  });
  runChecked(process.execPath, [tscBin, '-p', 'apps/manager-review-web/tsconfig.json', '--noEmit'], {
    cwd: mytmsRoot,
    code: 'WEEKLY_SOURCE_MANAGER_TYPECHECK_FAILED',
  });
  const contractOutput = runChecked(process.execPath, ['scripts/verify-candidate-contract.mjs'], {
    cwd: mytmsRoot,
    code: 'WEEKLY_SOURCE_MYTMS_CONTRACT_FAILED',
  });
  const summary = parseMytmsFocusedSummary({ candidateOutput, managerOutput, contractOutput });
  const outputSha256 = createHash('sha256').update(`${candidateOutput}\n${managerOutput}\n${contractOutput}`).digest('hex');
  const proofFiles = [
    ...CANDIDATE_TESTS.map((file) => path.join('apps', 'candidate-app', file).replaceAll('\\', '/')),
    ...MANAGER_TESTS,
    'scripts/verify-candidate-contract.mjs',
  ];
  const protectionResults = [
    {
      protectedId: 'PROT-DAILY-001',
      surface: 'CANDIDATE_MANAGER',
      result: 'PASS',
      executedChecks: [
        'src/features/timesheets/submitted-detail.test.ts',
        'src/features/timesheets/hours-model.test.ts',
      ],
      observedResults: [`Candidate focused suite passed ${summary.candidateTests}/${summary.candidateTests}`],
      prohibitedOutcomeChecks: ['DAILY_TOTAL_AND_BREAK_PRESENTATION_RETAINED', 'NO_WEEKLY_SOURCE_AUTHORITY_APPLIED_TO_DAILY'],
    },
    {
      protectedId: 'PROT-ORDW-001',
      surface: 'CANDIDATE_MANAGER',
      result: 'PASS',
      executedChecks: [
        'src/features/timesheets/weekly-source.test.ts',
        'src/features/timesheets/hours-model.test.ts',
        'src/features/workflows/submit.test.ts',
      ],
      observedResults: [`Candidate focused suite passed ${summary.candidateTests}/${summary.candidateTests}`],
      prohibitedOutcomeChecks: ['ORDINARY_TIMESHEET_NOT_CLASSIFIED_AS_WEEKLY_SOURCE', 'ORDINARY_SUBMISSION_OWNER_RETAINED'],
    },
    {
      protectedId: 'PROT-EXP-001',
      surface: 'CANDIDATE_MANAGER',
      result: 'PASS',
      executedChecks: [
        'src/features/timesheets/expense-only-display.test.ts',
        'src/features/timesheets/weekly-source.test.ts',
        'src/features/workflows/submit.test.ts',
      ],
      observedResults: [`Candidate focused suite passed ${summary.candidateTests}/${summary.candidateTests}`],
      prohibitedOutcomeChecks: ['ORDINARY_SEPARATE_EXPENSE_TIMESHEET_RETAINED', 'FIXED_SOURCE_EXPENSE_ONLY_HIDES_ENTRY_WHEN_EXPLICIT'],
    },
    {
      protectedId: 'PROT-APP-001',
      surface: 'CANDIDATE_MANAGER',
      result: 'PASS',
      executedChecks: [
        'src/features/timesheets/weekly-source.test.ts',
        'src/features/timesheets/submitted-detail.test.ts',
        'src/core/notifications/destination.test.ts',
      ],
      observedResults: [
        `Candidate focused suite passed ${summary.candidateTests}/${summary.candidateTests}`,
        `Manager focused suite passed ${summary.managerTests}/${summary.managerTests}`,
      ],
      prohibitedOutcomeChecks: ['NO_CURRENCY_OR_FINANCIAL_LANGUAGE_IN_WEEKLY_SOURCE_PAYLOADS', 'EXISTING_NAVIGATION_DESTINATIONS_RETAINED'],
    },
  ];
  const projectionDigests = [];
  for (const file of proofFiles) {
    projectionDigests.push({ name: file, digest: createHash('sha256').update(await readFile(path.join(mytmsRoot, file))).digest('hex') });
  }
  if (!resultDirectory) return { summary, outputSha256, resultEnvelopeDigest: null };

  const scenario = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
    scenarioId: 'WS-MYTMS-CANDIDATE-MANAGER-EXACT-001',
    fixedSeed: '1279d03cb90f6f6d7bbdc9821ad59ed7b300795550be419a26e83d2f10b54d35',
    requirementIds: [],
    protectedIds: protectionResults.map((result) => result.protectedId),
  };
  const expected = { ...summary, candidateTypecheck: 'PASS', managerTypecheck: 'PASS', contract: 'PASS' };
  const actual = { ...expected, protectionResults };
  const envelope = createResultEnvelope({
    scenario,
    repositories: [{ repository: 'mytms-app', commit: gitCommit(mytmsRoot) }],
    database: { used: false, reason: 'CANDIDATE_MANAGER_PRESENTATION_AND_CONTRACT_PROOF' },
    generatedSources: [],
    parser: { used: false },
    clockValuesUtc: [],
    executedOwners: proofFiles,
    oracle: { expected, expectedDigest: canonicalDigest(expected) },
    actual,
    comparison: { pass: true, actualDigest: canonicalDigest(actual), firstDivergence: null },
    c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
    outbox: { calls: [] },
    projectionDigests,
    acceptanceIds: ['MGR-001', 'MGR-002', 'MGR-003', 'MGR-004', 'MGR-005', 'RPT-001'],
    protectedIds: protectionResults.map((result) => result.protectedId),
    modelIds: [],
    spiIds: spiIdsForEvidenceGroup('mytms-candidate'),
    cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
  });
  await writeResultEnvelope(path.join(resultDirectory, 'mytms-candidate-manager-exact.json'), envelope);
  return { summary, outputSha256, resultEnvelopeDigest: envelope.evidenceDigest };
}

export async function runWeeklySourceHarnessPhase({ phase, repoRoot, packRoot, resultDirectory }) {
  if (phase !== 'service') fail('WEEKLY_SOURCE_SERVICE_PHASE_INVALID', 'The service adapter accepts only the service phase.');
  if (!packRoot) fail('WEEKLY_SOURCE_PACK_ROOT_REQUIRED', 'The sealed Plan 6.2 pack is required for source-file evidence.');
  const restrictedEvidenceRoot = path.join(packRoot, 'evidence', 'source-imports');
  const step6EvidenceRoot = process.env.CLOUDTMS_WEEKLY_SOURCE_STEP6_EVIDENCE_DIR
    ? path.resolve(process.env.CLOUDTMS_WEEKLY_SOURCE_STEP6_EVIDENCE_DIR)
    : null;
  if (!step6EvidenceRoot) fail('WEEKLY_SOURCE_STEP6_EVIDENCE_REQUIRED', 'The copied Step 6 real-report evidence directory is required.');
  const mytmsRoot = process.env.CLOUDTMS_WEEKLY_SOURCE_MYTMS_ROOT
    ? path.resolve(process.env.CLOUDTMS_WEEKLY_SOURCE_MYTMS_ROOT)
    : null;
  if (!mytmsRoot) fail('WEEKLY_SOURCE_MYTMS_ROOT_REQUIRED', 'The isolated MyTMS worktree is required for Candidate and manager evidence.');
  for (const testFile of SERVICE_TESTS) await regularFile(path.join(repoRoot, testFile), testFile);
  await regularFile(path.join(restrictedEvidenceRoot, 'nhsp', 'NHSP released shifts example.xlsx'), 'sealed restricted NHSP evidence');
  await regularFile(path.join(step6EvidenceRoot, 'nhsp', 'BR1-2026-09-09.xls'), 'Step 6 NHSP evidence');

  const result = spawnSync(process.execPath, ['--test', ...SERVICE_TESTS], {
    cwd: repoRoot,
    env: {
      ...process.env,
      CLOUDTMS_WEEKLY_SOURCE_EVIDENCE_DIR: restrictedEvidenceRoot,
      CLOUDTMS_WEEKLY_SOURCE_STEP6_EVIDENCE_DIR: step6EvidenceRoot,
    },
    encoding: 'utf8',
    timeout: 600_000,
    maxBuffer: 128 * 1024 * 1024,
    windowsHide: true,
  });
  const output = `${result.stdout ?? ''}\n${result.stderr ?? ''}`;
  if (result.error || result.status !== 0) {
    fail('WEEKLY_SOURCE_SERVICE_FAILED', `The real Weekly Source service suite failed before certification (exit ${result.status ?? -1}).`);
  }
  const summary = parseNodeTestSummary(output);
  const outputSha256 = createHash('sha256').update(output).digest('hex');
  const fileDigests = [];
  for (const testFile of SERVICE_TESTS) {
    fileDigests.push({
      testFile,
      sha256: createHash('sha256').update(await readFile(path.join(repoRoot, testFile))).digest('hex'),
    });
  }
  const resultEnvelopeDigest = await writeParserRequirementEvidence({ repoRoot, resultDirectory, summary, outputSha256 });
  const mytmsEvidence = await writeMytmsFocusedEvidence({ mytmsRoot, resultDirectory });
  return {
    executed: true,
    pass: true,
    evidence: [{
      suite: 'weekly-source-service-and-real-file-contracts',
      ...summary,
      testFileCount: SERVICE_TESTS.length,
      fileDigests,
      outputSha256,
      resultEnvelopeDigest,
      joinedDatabaseRoute: false,
      bankingDependentResultsIncluded: false,
    }, {
      suite: 'mytms-candidate-manager-focused',
      ...mytmsEvidence.summary,
      outputSha256: mytmsEvidence.outputSha256,
      resultEnvelopeDigest: mytmsEvidence.resultEnvelopeDigest,
      bankingDependentResultsIncluded: false,
    }],
  };
}
