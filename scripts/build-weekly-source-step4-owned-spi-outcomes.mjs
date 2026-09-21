#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

import { canonicalDigest } from '../tests/weekly-source/harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../tests/weekly-source/harness/result-envelope.mjs';

function values(flag) {
  const output = [];
  for (let index = 2; index < process.argv.length; index += 1) {
    if (process.argv[index] === flag && process.argv[index + 1]) output.push(path.resolve(process.argv[index + 1]));
  }
  return output;
}

function value(flag) {
  const output = values(flag);
  if (output.length !== 1) throw new Error(`${flag} is required exactly once`);
  return output[0];
}

function sha256(bytes) { return createHash('sha256').update(bytes).digest('hex'); }

function commit(root) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8', windowsHide: true });
  const output = String(result.stdout ?? '').trim();
  if (result.status !== 0 || !/^[a-f0-9]{40}$/.test(output)) throw new Error(`Git commit is unavailable for ${root}`);
  return output;
}

const db = (scenarioId, file, marker) => ({ scenarioId, kind: 'DATABASE', file, marker });
const app = (scenarioId, file, marker, databaseFile, databaseMarker) => ({
  scenarioId, kind: 'MYTMS', file, marker, databaseFile, databaseMarker,
});
const differential = (scenarioId, protectedId, file, marker) => ({
  scenarioId, kind: 'DIFFERENTIAL', protectedId, file, marker,
});

// Every entry names a concrete assertion that ran on both PostgreSQL modes, or
// a concrete protected-area/app assertion plus its database producer. A broad
// suite PASS is never enough on its own.
const registry = Object.freeze([
  db('SPI-019', '15092026_1534_weekly_source_finalisation_v1.sql', 'NHSP work events must never be keyed on the Reference Number'),
  db('SPI-020', '15092026_1534_weekly_source_projection_build_v1.sql', 'AUTO_UNIQUE_WITH_TWO_QUALIFYING_CONTRACTS_WAS_ACCEPTED'),
  db('SPI-021', '15092026_1534_weekly_source_projection_build_v1.sql', 'OFFICE_SELECTED_WITH_ONE_QUALIFYING_CONTRACT_WAS_ACCEPTED'),
  db('SPI-022', '15092026_1534_weekly_source_projection_build_v1.sql', 'EXACT_QUALIFICATION_OBSERVATION_NOT_STORED_FROM_SERVER_FACTS'),
  db('SPI-023', '15092026_1534_weekly_source_projection_build_v1.sql', 'WEEKLY_SOURCE_AUTO_UNIQUE_NOT_UNIQUE'),
  db('SPI-024', '15092026_1534_weekly_source_projection_build_v1.sql', 'WEEKLY_SOURCE_QUALIFICATION_RESULT_NOT_REDERIVED'),
  db('SPI-025', '15092026_1534_weekly_source_projection_build_v1.sql', 'CALLER_CLAIMED_A_FALSE_QUALIFICATION_VERDICT'),
  db('SPI-026', '15092026_1534_weekly_source_projection_build_v1.sql', 'EXACT_CHARGE_CHECK_NOT_STORED_FROM_SERVER_FACTS'),
  db('SPI-027', '15092026_1534_weekly_source_projection_build_v1.sql', 'SYMMETRIC_ONE_PENNY_ROW_REFUSED_BY_THE_OWNER'),
  db('SPI-028', '15092026_1534_weekly_source_projection_build_v1.sql', 'SYMMETRIC_ONE_PENNY_ROW_REFUSED_BY_AN_UNEXPECTED_CONSTRAINT'),
  db('SPI-029', '15092026_1534_weekly_source_finalisation_v1.sql', 'zero source charge was not accepted through the Office owner'),
  db('SPI-030', '15092026_1534_weekly_source_finalisation_v1.sql', 'disabled source-fixed expenses must not contaminate any expense or invoice route'),
  db('SPI-031', '15092026_1534_weekly_source_finalisation_v1.sql', 'generic first appearance must be ADD'),
  db('SPI-032', '15092026_1534_weekly_source_finalisation_v1.sql', 'NO_CHANGE must emit no billing movement'),
  db('SPI-033', '15092026_1534_weekly_source_finalisation_v1.sql', 'AMEND must emit paired full reversal and replacement'),
  db('SPI-034', '15092026_1534_weekly_source_finalisation_v1.sql', 'SOURCE_ABSENT_ZERO must emit only the full reversal'),
  db('SPI-035', '15092026_1534_weekly_source_finalisation_v1.sql', 'HealthRoster unfinalised evidence must emit one full reversal only'),
  db('SPI-036', '15092026_1534_weekly_source_finalisation_v1.sql', 'latest work-event state across disjoint coverage must drive the later cancellation'),
  db('SPI-037', '15092026_1534_weekly_source_finalisation_v1.sql', 'moving complete-coverage window must not erase an older still-current'),
  db('SPI-038', '15092026_1534_weekly_source_finalisation_v1.sql', 'worked source hours and their source-fixed expense must share one ordinary Weekly HOURS root'),
  db('SPI-039', '15092026_1534_weekly_source_finalisation_v1.sql', 'complete-coverage omission must clear the latest expense-only authority'),
  db('SPI-040', '15092026_1534_weekly_source_finalisation_v1.sql', 'source-fixed expense must materialise once with equal pay and charge and configured VAT'),
  db('SPI-041', '17092026_0800_weekly_source_mode_a_dispatch_v1.sql', 'MODE_A_CLEAN_REFERENCE_NOT_WRITTEN_BY_ESTABLISHED_OWNER'),
  db('SPI-042', '17092026_0800_weekly_source_mode_a_dispatch_v1.sql', 'MODE_A_HELD_MANAGER_CORRECTION_EMAIL_ACTION_ABSENT'),
  db('SPI-043', '17092026_0800_weekly_source_mode_a_dispatch_v1.sql', 'MODE_A_UNSIGNED_EVIDENCE_PRODUCED_A_COMPARISON'),
  db('SPI-044', '17092026_0800_weekly_source_mode_a_dispatch_v1.sql', 'MODE_A_CLEAN_AUTO_AUTHORISATION_NOT_RECORDED'),
  db('SPI-045', '17092026_0800_weekly_source_mode_a_dispatch_v1.sql', 'MODE_A_HELD_REFERENCE_WAS_WRITTEN_DESPITE_A_MISMATCH'),
  differential('SPI-046', 'PROT-DAILY-001', 'src/features/workflows/submit.test.ts', 'never invents a DAILY approval method'),
  differential('SPI-047', 'PROT-ORDW-001', 'src/features/timesheets/weekly-source.test.ts', 'an ordinary Timesheet is never treated as a Weekly source record'),
  db('SPI-048', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'selecting one line moved more than one line'),
  db('SPI-049', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'same-Client cross-source-group move did not carry its companion'),
  db('SPI-050', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'the validator admitted an empty source invoice'),
  db('SPI-051', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'source issue validator refused a sealed self-bill'),
  db('SPI-052', '02092026_1833_weekly_source_invoice_issue_validator_v1.sql', 'real asynchronous validator blocked a source expense invoice'),
  db('SPI-053', '02092026_1833_weekly_source_invoice_issue_validator_v1.sql', 'a mixed source invoice was admitted'),
  db('SPI-054', '02092026_1833_weekly_source_invoice_issue_validator_v1.sql', 'batch issue classifier rejected a legal later reversal'),
  db('SPI-055', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'invoice-discounting proof has no positive source invoice'),
  db('SPI-056', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'invoice-discounting proof has no negative-only source invoice'),
  db('SPI-057', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'invoice-discounting proof has no mixed signed source invoice'),
  db('SPI-058', '15092026_1534_weekly_source_invoice_admission_v1.sql', 'moving one source presentation did not produce both invoice-discounting deltas'),
  db('SPI-059', '15092026_1534_weekly_source_correct_final_source_v1.sql', 'APPLY must atomically swap all current authority pointers'),
  db('SPI-060', '15092026_1534_weekly_source_correct_final_source_v1.sql', 'active Banking Draft was accepted'),
  app('SPI-061', 'src/features/timesheets/weekly-source.test.ts', 'UI-019, UI-020 and UI-021 are decided from the produced payload alone', '17092026_1100_weekly_source_candidate_view_producer_v1.sql', 'UI-019 no approved card'),
  app('SPI-062', 'src/features/timesheets/submitted-detail.test.ts', 'keeps the Candidate-submitted Timesheet separate from different approved hours', '17092026_1100_weekly_source_candidate_view_producer_v1.sql', 'UI-020 the Candidate submission is still shown untouched'),
  app('SPI-063', 'src/features/timesheets/weekly-source.test.ts', 'separate additional expense Timesheet is offered only under SEPARATE_TIMESHEET', '17092026_1100_weekly_source_candidate_view_producer_v1.sql', 'source-fixed expense hides Candidate expense entry'),
  app('SPI-064', 'src/features/workflows/submit.test.ts', 'keeps electronic Weekly and PHONE Daily creation unchanged', '17092026_1100_weekly_source_candidate_view_producer_v1.sql', 'ordinary Timesheet still produces NULL'),
  app('SPI-092', 'src/features/timesheets/weekly-source.test.ts', 'UI-021 is the empty-submission, non-empty-approved pair and nothing else', '17092026_1100_weekly_source_candidate_view_producer_v1.sql', 'UI-021 the submitted fact stays empty'),
]);

const backendRoot = value('--backend-root');
const mytmsRoot = value('--mytms-root');
const newComponentPath = value('--new-component');
const upgradeComponentPath = value('--upgrade-component');
const mytmsEvidencePath = value('--mytms-evidence');
const differentialEvidencePath = value('--differential-evidence');
const outputPath = value('--output');

const loadJson = async (file) => JSON.parse(await readFile(file, 'utf8'));
const newComponent = await loadJson(newComponentPath);
const upgradeComponent = await loadJson(upgradeComponentPath);
const mytmsEvidence = await loadJson(mytmsEvidencePath);
const differentialEvidence = await loadJson(differentialEvidencePath);
for (const [label, result, mode] of [
  ['NEW component', newComponent, 'NEW'], ['UPGRADE component', upgradeComponent, 'UPGRADE'],
]) {
  if (result?.schemaVersion !== 'WEEKLY_SOURCE_COMPONENT_POSTGRES_RESULT_V1'
      || result?.status !== 'PASS_WITH_HANDOVER2_PENDING'
      || result?.mode !== mode
      || !String(result?.postgresVersion ?? '').startsWith('170011|17.11')) {
    throw new Error(`${label} is not the required PostgreSQL 17.11 component result.`);
  }
}
if (mytmsEvidence?.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || mytmsEvidence?.status !== 'PASS'
    || mytmsEvidence?.actual?.candidateTests !== 108 || mytmsEvidence?.actual?.managerTests !== 20) {
  throw new Error('MyTMS evidence is not the focused passing 108/20 result.');
}
if (differentialEvidence?.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || differentialEvidence?.status !== 'PASS') {
  throw new Error('Protected-area differential evidence is not passing.');
}

function componentFiles(result) {
  return new Map((result?.scenarioEvidence?.groups ?? []).flatMap((group) => group.files ?? []).map((file) => [file.file, file]));
}
const newFiles = componentFiles(newComponent);
const upgradeFiles = componentFiles(upgradeComponent);
const sourceCache = new Map();
async function sourceProof(file, marker) {
  const full = path.join(backendRoot, 'supabase', 'verification', file);
  let bytes = sourceCache.get(full);
  if (!bytes) { bytes = await readFile(full); sourceCache.set(full, bytes); }
  const text = bytes.toString('utf8');
  if (!text.includes(marker)) throw new Error(`${file} no longer contains the exact marker for ${marker}`);
  const currentSha = sha256(bytes);
  const newResult = newFiles.get(file);
  const upgradeResult = upgradeFiles.get(file);
  if (!newResult || !upgradeResult || newResult.fileSha256 !== currentSha || upgradeResult.fileSha256 !== currentSha
      || !/^[a-f0-9]{64}$/.test(newResult.outputSha256 ?? '')
      || !/^[a-f0-9]{64}$/.test(upgradeResult.outputSha256 ?? '')) {
    throw new Error(`${file} was not executed from the current bytes in both PostgreSQL modes.`);
  }
  return { full, currentSha, newResult, upgradeResult };
}

async function appProof(file, marker) {
  const full = path.join(mytmsRoot, 'apps', 'candidate-app', file);
  const bytes = await readFile(full);
  if (!bytes.toString('utf8').includes(marker)) throw new Error(`${file} no longer contains the exact marker for ${marker}`);
  return { full, sha: sha256(bytes) };
}

const outcomes = [];
const projectionDigests = [];
for (const item of registry) {
  if (item.kind === 'DATABASE') {
    const proof = await sourceProof(item.file, item.marker);
    outcomes.push({
      scenarioId: item.scenarioId,
      localResult: 'PASS',
      observations: {
        executedActions: [`Executed ${item.file} on fresh and upgraded PostgreSQL 17.11 databases.`],
        databaseReadbacks: [
          `fileSha256=${proof.currentSha}`,
          `newOutputSha256=${proof.newResult.outputSha256}`,
          `upgradeOutputSha256=${proof.upgradeResult.outputSha256}`,
          `assertionMarker=${item.marker}`,
        ],
        prohibitedOutcomeChecks: ['NO_CREDIT_FROM_GROUP_PASS_ALONE', 'CURRENT_ASSERTION_MARKER_REQUIRED', 'BOTH_DATABASE_MODES_REQUIRED'],
        actualInvoiceResult: `The current database verifier completed in both modes with the scenario assertion: ${item.marker}.`,
        actualOfficeAuditResult: `The scenario-specific assertion remained present in the executed current verifier ${item.file}.`,
      },
    });
    projectionDigests.push({ name: `${item.scenarioId}:${item.file}`, digest: proof.currentSha });
    continue;
  }
  const appResult = await appProof(item.file, item.marker);
  if (item.kind === 'MYTMS') {
    const databaseResult = await sourceProof(item.databaseFile, item.databaseMarker);
    outcomes.push({
      scenarioId: item.scenarioId,
      localResult: 'PASS',
      observations: {
        executedActions: [`Executed focused MyTMS suite containing ${item.file}.`, `Executed ${item.databaseFile} in both PostgreSQL modes.`],
        databaseReadbacks: [
          `databaseAssertion=${item.databaseMarker}`,
          `newOutputSha256=${databaseResult.newResult.outputSha256}`,
          `upgradeOutputSha256=${databaseResult.upgradeResult.outputSha256}`,
          `mytmsFileSha256=${appResult.sha}`,
          `candidateTests=${mytmsEvidence.actual.candidateTests};managerTests=${mytmsEvidence.actual.managerTests}`,
        ],
        prohibitedOutcomeChecks: ['NO_MONEY_LANGUAGE_IN_CANDIDATE_VIEW', 'NO_ORDINARY_TIMESHEET_ROUTE_CHANGE', 'CURRENT_APP_ASSERTION_REQUIRED'],
        actualInvoiceResult: 'The Candidate presentation changed no invoice authority; the database producer and app presentation assertions both passed.',
        actualOfficeAuditResult: `The current app assertion passed: ${item.marker}.`,
      },
    });
    projectionDigests.push({ name: `${item.scenarioId}:${item.file}`, digest: appResult.sha });
    continue;
  }
  const protectedResult = (differentialEvidence.actual?.protectionResults ?? [])
    .find((result) => result.protectedId === item.protectedId && result.result === 'PASS' && result.surface === 'DATABASE');
  if (!protectedResult) throw new Error(`${item.scenarioId} lacks the ${item.protectedId} database differential.`);
  outcomes.push({
    scenarioId: item.scenarioId,
    localResult: 'PASS',
    observations: {
      executedActions: [`Executed ${item.protectedId} before/after database differential.`, `Executed focused app assertion in ${item.file}.`],
      databaseReadbacks: [...protectedResult.observedResults, `mytmsFileSha256=${appResult.sha}`],
      prohibitedOutcomeChecks: [...protectedResult.prohibitedOutcomeChecks, 'CURRENT_APP_ASSERTION_REQUIRED'],
      actualInvoiceResult: `${item.protectedId} completed with no unauthorised protected-area change.`,
      actualOfficeAuditResult: `The unchanged journey assertion passed: ${item.marker}.`,
    },
  });
  projectionDigests.push({ name: `${item.scenarioId}:${item.file}`, digest: appResult.sha });
}

const ids = registry.map((item) => item.scenarioId);
if (ids.length !== 47 || new Set(ids).size !== 47) throw new Error(`Expected 47 unique Weekly Source-owned scenarios, received ${ids.length}.`);
const expected = { count: 47, scenarioIds: [...ids].sort() };
const actual = { count: outcomes.length, scenarioIds: outcomes.map((item) => item.scenarioId).sort(), spiOutcomes: outcomes };
const scenario = {
  schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
  scenarioId: 'WS-SPI-WEEKLY-SOURCE-OWNED-EXACT-STEP4-001',
  fixedSeed: sha256('weekly-source-step4-owned-spi-outcomes-v1'),
  requirementIds: [], protectedIds: [],
};
const envelope = createResultEnvelope({
  scenario,
  repositories: [
    { repository: 'cloudtms-backend', commit: commit(backendRoot) },
    { repository: 'mytms-app', commit: commit(mytmsRoot) },
  ],
  database: { used: true, engine: 'PostgreSQL', version: '17.11', modes: ['NEW', 'UPGRADE'], rowsReadBack: true },
  generatedSources: [],
  parser: { used: true, inheritedPassingEvidence: true },
  clockValuesUtc: [],
  executedOwners: ['CURRENT_SQL_VERIFIERS', 'CURRENT_MYTMS_FOCUSED_TESTS', 'PROTECTED_AREA_DIFFERENTIAL'],
  oracle: { expected, expectedDigest: canonicalDigest(expected) },
  actual,
  comparison: {
    pass: canonicalDigest({ count: actual.count, scenarioIds: actual.scenarioIds }) === canonicalDigest(expected),
    actualDigest: canonicalDigest(actual), firstDivergence: null,
  },
  c1: { category: 'NOT_APPLICABLE_TO_OWNED_SCENARIOS', emulator: false, releaseEvidenceEligible: false },
  outbox: { calls: [] }, projectionDigests, spiIds: actual.scenarioIds, protectedIds: [],
  cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
});
await writeResultEnvelope(outputPath, envelope);
console.log(`Exact Step 4 Weekly Source-owned SPI outcomes passed ${outcomes.length}/47 (${envelope.evidenceDigest}).`);
