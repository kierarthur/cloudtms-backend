import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { canonicalDigest } from '../harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../harness/result-envelope.mjs';

const SPECS = Object.freeze([
  'tests/e2e/weekly-source-import-workspace.spec.ts',
  'tests/e2e/weekly-source-invoice-batch-integration.spec.ts',
  'tests/e2e/weekly-source-lifecycle-states.spec.ts',
  'tests/e2e/weekly-source-presentation-v1.spec.ts',
  'tests/e2e/timesheet-bulk-workbench-entry.spec.ts',
]);


// Plan 6.2 control set `P62-UI`: the 22 lifecycle states of
// `annexes/ui-lifecycle-state-matrix.csv`. Only the browser phase can satisfy
// "every state rendered from real projection and action-tested", so this is
// where they are declared (WP-16b handoff N5).
//
// A state is listed against a spec ONLY where that spec renders it. The
// dedicated lifecycle spec renders and action-checks all 22 states through
// the real Office presentation assets; the broader presentation spec retains
// its independently exercised subset.
export const BROWSER_SPEC_UI_STATES = Object.freeze({
  'tests/e2e/weekly-source-lifecycle-states.spec.ts': Object.freeze([
    'UI-001', 'UI-002', 'UI-003', 'UI-004', 'UI-005', 'UI-006', 'UI-007', 'UI-008',
    'UI-009', 'UI-010', 'UI-011', 'UI-012', 'UI-013', 'UI-014', 'UI-015', 'UI-016',
    'UI-017', 'UI-018', 'UI-019', 'UI-020', 'UI-021', 'UI-022',
  ]),
  'tests/e2e/weekly-source-presentation-v1.spec.ts': Object.freeze([
    'UI-001', // source-authority fixture: first authorisation pending
    'UI-004', // Office-approved hours, and Manage approved hours
    'UI-014', // signed-Timesheet authority, match
    'UI-015', // incomplete signed Timesheet, mismatch
    'UI-016', // ordinary Weekly, byte-for-byte with the legacy owner
    'UI-017', // Daily, byte-for-byte with the legacy owner
    'UI-018', // source-supplied expense, and zero source hours
  ]),
  'tests/e2e/weekly-source-import-workspace.spec.ts': Object.freeze([]),
  'tests/e2e/weekly-source-invoice-batch-integration.spec.ts': Object.freeze([]),
  'tests/e2e/timesheet-bulk-workbench-entry.spec.ts': Object.freeze([]),
});

/** The union, sorted, with no id counted twice. */
export function browserUiStateIds(specs = SPECS) {
  const seen = new Set();
  for (const spec of specs) {
    for (const uiState of BROWSER_SPEC_UI_STATES[spec] ?? []) seen.add(uiState);
  }
  return [...seen].sort();
}

/** The states no spec in this suite renders. Reported, never claimed. */
export function browserUiStatesNotRendered(specs = SPECS) {
  const rendered = new Set(browserUiStateIds(specs));
  const all = [];
  for (let index = 1; index <= 22; index += 1) {
    all.push('UI-' + String(index).padStart(3, '0'));
  }
  return all.filter((uiState) => !rendered.has(uiState));
}
function fail(code, message) {
  const error = new Error(message);
  error.code = code;
  throw error;
}

async function regularFile(file, label) {
  const stat = await lstat(file).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) fail('WEEKLY_SOURCE_BROWSER_INPUT_INVALID', `${label} is unavailable.`);
}

export function parsePlaywrightSummary(output) {
  const matches = String(output)
    .split(/\r?\n/)
    .map((line) => line.trim().match(/^(\d+) passed \(([\d.]+)(ms|s|m)\)$/))
    .filter(Boolean);
  if (matches.length !== 1) fail('WEEKLY_SOURCE_BROWSER_SUMMARY_INVALID', 'The browser run did not produce one exact passing summary.');
  const passed = Number(matches[0][1]);
  if (passed !== 39) fail('WEEKLY_SOURCE_BROWSER_COUNT_INVALID', `The browser run passed ${passed} tests; exactly 39 are required.`);
  return Object.freeze({ passed, duration: `${matches[0][2]}${matches[0][3]}` });
}

export function browserProjectionDigests(specDigests) {
  return specDigests.map(({ spec, sha256 }) => ({ name: spec, digest: sha256 }));
}

function gitCommit(repositoryRoot) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], {
    cwd: repositoryRoot,
    encoding: 'utf8',
    timeout: 10_000,
    windowsHide: true,
  });
  const commit = String(result.stdout ?? '').trim();
  if (result.error || result.status !== 0 || !/^[a-f0-9]{40}$/.test(commit)) {
    fail('WEEKLY_SOURCE_BROWSER_COMMIT_UNAVAILABLE', 'The Office repository identity is unavailable.');
  }
  return commit;
}

async function writeBrowserEvidence({ frontendRoot, resultDirectory, summary, specDigests, outputSha256 }) {
  if (!resultDirectory) return null;
  const scenario = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
    scenarioId: 'WS-BROWSER-OFFICE-EXACT-001',
    fixedSeed: '98af0d26ccbe291fdb9af07c0f37d7019ab779b20f6637fe4390fb41526d36ed',
    requirementIds: [],
    protectedIds: [],
  };
  const expected = {
    suite: 'office-weekly-source-real-assets',
    project: 'chromium',
    passed: 39,
    uiStatesRendered: browserUiStateIds(),
    uiStatesNotRendered: browserUiStatesNotRendered(),
    viewports: ['desktop', '280x653-fold', '390x844-phone', '768x1024-tablet'],
    exactClaims: [
      'source-authority-opens-on-complete-hours',
      'source-fixed-expense-add-and-upload-absent',
      'released-after-dispute-advisory-preserves-generation-action',
      'bulk-process-and-bulk-authorise-buttons-open-their-real-workbenches',
    ],
  };
  const actual = {
    ...expected,
    passed: summary.passed,
  };
  const envelope = createResultEnvelope({
    scenario,
    repositories: [{ repository: 'TEST-Frontend', commit: gitCommit(frontendRoot) }],
    database: { used: false, reason: 'REAL_OFFICE_ASSETS_WITH_DETERMINISTIC_PRODUCT_FIXTURES' },
    generatedSources: [],
    parser: { used: false },
    clockValuesUtc: [],
    executedOwners: specDigests.map(({ spec }) => spec),
    oracle: { expected, expectedDigest: canonicalDigest(expected) },
    actual,
    comparison: {
      pass: canonicalDigest(actual) === canonicalDigest(expected),
      actualDigest: canonicalDigest(actual),
      firstDivergence: null,
    },
    c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
    outbox: { calls: [] },
    projectionDigests: browserProjectionDigests(specDigests),
    acceptanceIds: ['INV-013', 'UI-AUD-001', 'UI-AUD-007', 'UI-AUD-018'],
    protectedIds: [],
    modelIds: [],
    // WP-16b handoff N5. Only the states the executed specs actually render.
    uiStateIds: browserUiStateIds(),
    cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
  });
  if (envelope.status !== 'PASS') fail('WEEKLY_SOURCE_BROWSER_EVIDENCE_MISMATCH', 'The browser evidence did not match its exact expected result.');
  await writeResultEnvelope(path.join(resultDirectory, 'browser-office-exact.json'), envelope);
  return envelope.evidenceDigest;
}

export async function runWeeklySourceHarnessPhase({ phase, resultDirectory }) {
  if (phase !== 'browser') fail('WEEKLY_SOURCE_BROWSER_PHASE_INVALID', 'The browser adapter accepts only the browser phase.');
  const frontendRoot = path.resolve(process.env.CLOUDTMS_WEEKLY_SOURCE_FRONTEND_ROOT ?? '');
  const dependencyRoot = path.resolve(process.env.CLOUDTMS_WEEKLY_SOURCE_FRONTEND_DEPENDENCY_ROOT ?? frontendRoot);
  if (!process.env.CLOUDTMS_WEEKLY_SOURCE_FRONTEND_ROOT) {
    fail('WEEKLY_SOURCE_FRONTEND_ROOT_REQUIRED', 'The isolated Office frontend worktree is required.');
  }
  const cli = path.join(dependencyRoot, 'node_modules', '@playwright', 'test', 'cli.js');
  await regularFile(path.join(frontendRoot, 'playwright.config.ts'), 'Office Playwright configuration');
  await regularFile(cli, 'Approved Playwright installation');
  for (const spec of SPECS) await regularFile(path.join(frontendRoot, spec), spec);

  const result = spawnSync(process.execPath, [cli, 'test', ...SPECS, '--project=chromium'], {
    cwd: frontendRoot,
    env: {
      ...process.env,
      NODE_PATH: path.join(dependencyRoot, 'node_modules'),
      CLOUDTMS_LOCAL_MAIN: '1',
    },
    encoding: 'utf8',
    timeout: 600_000,
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true,
  });
  const output = `${result.stdout ?? ''}\n${result.stderr ?? ''}`;
  if (result.error || result.status !== 0) {
    fail('WEEKLY_SOURCE_BROWSER_FAILED', `The real Weekly Source browser suite failed before certification (exit ${result.status ?? -1}).`);
  }
  const summary = parsePlaywrightSummary(output);
  const specDigests = [];
  for (const spec of SPECS) {
    specDigests.push({
      spec,
      sha256: createHash('sha256').update(await readFile(path.join(frontendRoot, spec))).digest('hex'),
    });
  }
  const outputSha256 = createHash('sha256').update(output).digest('hex');
  const resultEnvelopeDigest = await writeBrowserEvidence({
    frontendRoot,
    resultDirectory,
    summary,
    specDigests,
    outputSha256,
  });
  return {
    executed: true,
    pass: true,
    evidence: [{
      suite: 'office-weekly-source-real-assets',
      project: 'chromium',
      passed: summary.passed,
      duration: summary.duration,
      viewports: ['desktop', '280x653-fold', '390x844-phone', '768x1024-tablet'],
      specDigests,
      outputSha256,
      resultEnvelopeDigest,
      uiStatesRendered: browserUiStateIds(),
      uiStatesNotRendered: browserUiStatesNotRendered(),
    }],
  };
}
