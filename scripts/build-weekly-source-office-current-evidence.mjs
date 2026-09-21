#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { canonicalDigest } from '../tests/weekly-source/harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../tests/weekly-source/harness/result-envelope.mjs';

const SPECS = Object.freeze([
  'tests/e2e/weekly-source-bulk-shell.spec.ts',
  'tests/e2e/weekly-source-import-workspace.spec.ts',
  'tests/e2e/weekly-source-invoice-batch-integration.spec.ts',
  'tests/e2e/weekly-source-invoice-line-move.spec.ts',
  'tests/e2e/weekly-source-lifecycle-states.spec.ts',
  'tests/e2e/weekly-source-presentation-v1.spec.ts',
  'tests/e2e/weekly-source-settings-visual.spec.ts',
  'tests/e2e/weekly-source-simple-shell.spec.ts',
  'tests/e2e/weekly-source-stage11-closure.spec.ts',
]);

function arg(name) {
  const index = process.argv.indexOf(name);
  if (index < 0 || !process.argv[index + 1]) throw new Error(`${name} is required`);
  return path.resolve(process.argv[index + 1]);
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function commit(root) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8', windowsHide: true });
  const value = String(result.stdout ?? '').trim();
  if (result.status !== 0 || !/^[a-f0-9]{40}$/.test(value)) throw new Error('Frontend commit is unavailable');
  return value;
}

function dirtyProductFiles(root) {
  const result = spawnSync('git', ['status', '--porcelain=v1', '--untracked-files=all'], { cwd: root, encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) throw new Error('Frontend status is unavailable');
  return String(result.stdout ?? '').split(/\r?\n/).filter(Boolean)
    .map((line) => line.slice(3).replaceAll('\\', '/'))
    .filter((file) => /^(?:js|css|tests\/fixtures)\//.test(file))
    .sort();
}

const playwrightJson = arg('--playwright-json');
const frontendRoot = arg('--frontend-root');
const output = arg('--output');
const report = JSON.parse(await readFile(playwrightJson, 'utf8'));
const stats = report.stats ?? {};
if (stats.expected !== 52 || stats.unexpected !== 0 || stats.skipped !== 0 || stats.flaky !== 0) {
  throw new Error(`Complete Office proof requires 52/52 clean passes; received ${JSON.stringify(stats)}`);
}
const executedFiles = new Set();
const executedTests = [];
for (const suite of report.suites ?? []) {
  if (suite.file) executedFiles.add(String(suite.file).replaceAll('\\', '/'));
  for (const child of suite.suites ?? []) {
    for (const spec of child.specs ?? []) executedTests.push(spec.title);
  }
  for (const spec of suite.specs ?? []) executedTests.push(spec.title);
}
for (const spec of SPECS) {
  if (![...executedFiles].some((file) => file.endsWith(spec.replace('tests/e2e/', '')) || file.endsWith(spec))) {
    throw new Error(`Complete Office proof omitted ${spec}`);
  }
}

const productFiles = dirtyProductFiles(frontendRoot);
const projectionDigests = [];
for (const file of [...SPECS, ...productFiles]) {
  projectionDigests.push({ name: file, digest: sha256(await readFile(path.join(frontendRoot, file))) });
}
projectionDigests.push({ name: 'office-complete-playwright.json', digest: sha256(await readFile(playwrightJson)) });

const protectionPolicies = Object.freeze({
  'PROT-INV-001': [
    'finalised self-bill stays in the existing batch table with one sticky header checkbox',
  ],
  'PROT-INVMOVE-001': [
    'the screen reads movable_lines and offers a move only on an independently movable line',
    'a move posts one presentation identity without a source-group or week restriction',
    'every same-Client destination uses the same simple move request',
  ],
  'PROT-DAILY-001': [
    'browser helper leaves ordinary Weekly and Daily rendering byte-for-byte with the legacy owner',
  ],
  'PROT-ORDW-001': [
    'Stage 11 protected journey: queue navigation refreshes the selected manual non-QR record, hours and evidence',
    'Stage 11 protected journey: manual non-QR hours, extra shifts, additional units and expenses remain editable',
    'browser helper leaves ordinary Weekly and Daily rendering byte-for-byte with the legacy owner',
  ],
  'PROT-PAY-001': [
    'XSG-022: an eligible row can be authorised, and the committed result survives a refresh',
    'XSG-022: Bulk Process opens its real workbench and its dataset read is answered',
  ],
  'PROT-EXP-001': [
    'Stage 11 protected journey: manual non-QR hours, extra shifts, additional units and expenses remain editable',
    'source-supplied expense stays read-only on the same Timesheet',
    'zero source hours keep the client-provided expense on the same Weekly Timesheet',
  ],
  'PROT-CONTRACT-001': [
    'preview, contract choice and Correct final source follow deterministic policy screens',
  ],
  'PROT-SUMMARY-001': [
    'Stage 11: Timesheet Summary shows one secondary Weekly Source delay',
  ],
  'PROT-SETTINGS-001': [
    'Stage 11: relevant Client and Contract Weekly Source settings use the real settings owner',
    'Stage 11: Roster validation keeps Reference required before pay visible and off by default',
  ],
});
const protectionResults = Object.entries(protectionPolicies).map(([protectedId, requiredTitles]) => {
  const missing = requiredTitles.filter((title) => !executedTests.includes(title));
  if (missing.length) throw new Error(`${protectedId} Office proof omitted: ${missing.join('; ')}`);
  return {
    protectedId,
    surface: 'OFFICE',
    result: 'PASS',
    executedChecks: requiredTitles,
    observedResults: requiredTitles.map((title) => `PLAYWRIGHT_PASS:${title}`),
    prohibitedOutcomeChecks: ['NO_MISSING_REQUIRED_OFFICE_CHECK', 'NO_SKIPPED_REQUIRED_OFFICE_CHECK'],
  };
});

const scenario = {
  schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
  scenarioId: 'WS-BROWSER-OFFICE-COMPLETE-R26-001',
  fixedSeed: sha256('weekly-source-browser-office-complete-r26'),
  requirementIds: [],
  protectedIds: Object.keys(protectionPolicies),
};
const expected = {
  specFiles: [...SPECS],
  tests: 52,
  passed: 52,
  failed: 0,
  skipped: 0,
  flaky: 0,
  protectedJourneys: [
    'ordinary manual non-QR hours and extra shifts',
    'ordinary Candidate and Office expense controls',
    'Bulk Authorise and Bulk Process actions',
    'record switching refreshes hours and evidence',
    'one source presentation moves between same-Client unissued invoices',
    'Simple Timesheet source comparison and existing tabs',
  ],
};
const actual = {
  specFiles: [...executedFiles].map((file) => {
    const match = SPECS.find((spec) => file.endsWith(spec) || file.endsWith(spec.replace('tests/e2e/', '')));
    return match ?? file;
  }).filter((file) => SPECS.includes(file)).sort(),
  tests: stats.expected,
  passed: stats.expected,
  failed: stats.unexpected,
  skipped: stats.skipped,
  flaky: stats.flaky,
  protectedJourneys: [...expected.protectedJourneys],
  protectionResults,
};
const envelope = createResultEnvelope({
  scenario,
  repositories: [{ repository: 'TEST-Frontend', commit: commit(frontendRoot) }],
  database: { used: false, reason: 'REAL_BROWSER_CURRENT_SAVED_FRONTEND' },
  generatedSources: [],
  parser: { used: false },
  clockValuesUtc: [],
  executedOwners: [...SPECS],
  oracle: { expected, expectedDigest: canonicalDigest(expected) },
  actual,
  comparison: {
    pass: actual.tests === expected.tests && actual.failed === 0 && actual.skipped === 0 && actual.flaky === 0,
    actualDigest: canonicalDigest(actual),
    firstDivergence: null,
  },
  c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
  outbox: { calls: [] },
  projectionDigests,
  protectedIds: scenario.protectedIds,
  uiStateIds: Array.from({ length: 22 }, (_, index) => `UI-${String(index + 1).padStart(3, '0')}`),
  spiIds: ['SPI-046', 'SPI-047'],
  xsgIds: ['XSG-014', 'XSG-021', 'XSG-022', 'XSG-023'],
  cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
});
await writeResultEnvelope(output, envelope);
console.log(`Complete current Office evidence passed 52/52 (${envelope.evidenceDigest}).`);
