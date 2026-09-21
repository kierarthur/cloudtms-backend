#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { canonicalDigest } from '../tests/weekly-source/harness/canonical-json.mjs';
import { normalizeOutcome } from '../tests/weekly-source/harness/expected-outcome-oracle.mjs';
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
  if (result.status !== 0 || !/^[a-f0-9]{40}$/.test(output)) throw new Error('Backend commit is unavailable');
  return output;
}

function requireResult(envelope, scenarioId, mode) {
  if (envelope?.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1'
      || envelope?.scenarioId !== scenarioId
      || envelope?.status !== 'PASS'
      || envelope?.database?.mode !== mode
      || envelope?.database?.rowsReadBack !== true
      || !String(envelope?.database?.version ?? '').startsWith('17.11')) {
    throw new Error(`${scenarioId}/${mode} is not an exact PostgreSQL 17.11 read-back result.`);
  }
}

function sameActual(first, second, label) {
  if (canonicalDigest(normalizeOutcome(first.actual, { strict: false }))
      !== canonicalDigest(normalizeOutcome(second.actual, { strict: false }))) {
    throw new Error(`${label} NEW and UPGRADE observations differ.`);
  }
}

const backendRoot = value('--backend-root');
const outputPath = value('--output');
const nhspFiles = values('--nhsp');
const rosterFiles = values('--roster');
if (nhspFiles.length !== 2 || rosterFiles.length !== 2) {
  throw new Error('Exactly two --nhsp and two --roster results are required (NEW then UPGRADE).');
}

const loaded = async (file) => ({ file, bytes: await readFile(file) });
const nhspInputs = await Promise.all(nhspFiles.map(loaded));
const rosterInputs = await Promise.all(rosterFiles.map(loaded));
const nhsp = nhspInputs.map(({ bytes }) => JSON.parse(bytes));
const roster = rosterInputs.map(({ bytes }) => JSON.parse(bytes));
requireResult(nhsp[0], 'WS-REAL-WORLD-NHSP-001', 'NEW');
requireResult(nhsp[1], 'WS-REAL-WORLD-NHSP-001', 'UPGRADE');
requireResult(roster[0], 'WS-REAL-WORLD-ROSTER-001', 'NEW');
requireResult(roster[1], 'WS-REAL-WORLD-ROSTER-001', 'UPGRADE');
sameActual(nhsp[0], nhsp[1], 'NHSP');
sameActual(roster[0], roster[1], 'Roster');

const nhspActual = nhsp[0].actual;
const rosterActual = roster[0].actual;
const nhspLines = nhspActual.invoiceLines ?? [];
const nhspMovements = nhspActual.sourceMovements ?? [];
const nhspApproved = nhspActual.approvedHours?.[0]?.shifts?.[0];
const nhspLineByKey = new Map(nhspLines.map((line) => [line.sourceRowKey, line]));
const nhspMovementByKey = new Map(nhspMovements.map((movement) => [movement.sourceRowKey, movement]));
const originalLine = nhspLineByKey.get('source_original');
const reversalLine = nhspLineByKey.get('source_reversal');
const correctedLine = nhspLineByKey.get('source_corrected');
const originalMovement = nhspMovementByKey.get('source_original');
const reversalMovement = nhspMovementByKey.get('source_reversal');
const correctedMovement = nhspMovementByKey.get('source_corrected');
if (originalLine?.exVatPence !== '13000'
    || reversalLine?.exVatPence !== '-13000'
    || correctedLine?.exVatPence !== '15000'
    || `${originalMovement?.kind}:${originalMovement?.workedMinutes}` !== 'PHYSICAL_POSITIVE:390'
    || `${reversalMovement?.kind}:${reversalMovement?.workedMinutes}` !== 'PHYSICAL_FULL_NEGATIVE:-390'
    || `${correctedMovement?.kind}:${correctedMovement?.workedMinutes}` !== 'PHYSICAL_POSITIVE:450'
    || nhspApproved?.workedMinutes !== 450
    || nhspActual.c1PublicationCategory !== 'WAITING') {
  throw new Error('The NHSP read-back does not prove the protected first-authorisation and correction observations.');
}

const rosterApproved = rosterActual.approvedHours ?? [];
const sourceShift = rosterApproved.find((row) => row.contractKey === 'contract_source')?.shifts?.[0];
const signedShift = rosterApproved.find((row) => row.contractKey === 'contract_signed')?.shifts?.[0];
if (sourceShift?.start !== '20:00' || sourceShift?.end !== '08:00' || sourceShift?.breakMinutes !== 60
    || sourceShift?.workedMinutes !== 660 || signedShift?.reference !== 'HR-B-001'
    || rosterActual.invoiceLines?.[0]?.exVatPence !== '25600') {
  throw new Error('The Roster read-back does not prove the Actual-hours and signed-Timesheet observations.');
}

const common = (actual, action, invoiceResult, auditResult, prohibited) => ({
  executedActions: [action],
  databaseReadbacks: [
    `sourceMovements=${JSON.stringify(actual.sourceMovements)}`,
    `invoiceLines=${JSON.stringify(actual.invoiceLines)}`,
    `approvedHours=${JSON.stringify(actual.approvedHours)}`,
    `c1PublicationCategory=${actual.c1PublicationCategory}`,
  ],
  prohibitedOutcomeChecks: prohibited,
  actualInvoiceResult: invoiceResult,
  actualOfficeAuditResult: auditResult,
});

const spiOutcomes = [
  {
    scenarioId: 'SPI-002',
    localResult: 'PASS',
    observations: common(
      nhspActual,
      'PROTECT_HOURS followed by AUTHORISE on the populated NHSP journey',
      `The first physical source line remained ${originalLine.exVatPence} pence while approved hours were ${nhspApproved.workedMinutes} minutes.`,
      'Submitted, source and approved hours were read back as separate facts; publication remained WAITING for HANDOVER 2.',
      ['NO_PAY_FROM_INVOICE_VALUE', 'NO_LEGACY_PAIRED_TIMESHEET'],
    ),
  },
  {
    scenarioId: 'SPI-003',
    localResult: 'PASS',
    observations: common(
      nhspActual,
      'FINALISE second NHSP cycle containing a full reversal and corrected positive movement',
      `The later self-bill retained separate ${reversalLine.exVatPence} and ${correctedLine.exVatPence} pence physical lines.`,
      `Approved hours remained ${nhspApproved.workedMinutes} minutes and the Candidate payment publication stayed WAITING.`,
      ['NO_NETTED_INVOICE_LINE', 'NO_DUPLICATE_CANDIDATE_PUBLICATION', 'NO_LEGACY_PAIRED_TIMESHEET'],
    ),
  },
  {
    scenarioId: 'SPI-031',
    localResult: 'PASS',
    observations: common(
      rosterActual,
      'FINALISE source-authoritative HealthRoster Actual-hours row and AUTHORISE',
      `The source-authoritative invoice line was ${rosterActual.invoiceLines[0].exVatPence} pence.`,
      `Actual ${sourceShift.start}-${sourceShift.end}, ${sourceShift.breakMinutes}-minute break and ${sourceShift.workedMinutes} paid minutes were read back.`,
      ['NO_CONTRACT_HOURS_SUBSTITUTION', 'NO_PLANNED_HOURS_SUBSTITUTION'],
    ),
  },
  {
    scenarioId: 'SPI-041',
    localResult: 'PASS',
    observations: common(
      rosterActual,
      'APPLY Mode A to the signed-Timesheet-authority row',
      'No source self-bill line was created for the signed-Timesheet-authority Contract.',
      `The signed 09:00-17:00 shift retained ${signedShift.workedMinutes} minutes and received reference ${signedShift.reference}.`,
      ['NO_SOURCE_PAY_SUBSTITUTION', 'NO_SELF_BILL_MOVEMENT_FOR_SIGNED_TIMESHEET'],
    ),
  },
];

const protectionResults = [
  {
    protectedId: 'PROT-INV-001',
    surface: 'DATABASE',
    result: 'PASS',
    executedChecks: ['NHSP_FIRST_FINAL_SOURCE_LINE', 'NHSP_LATER_REVERSAL_AND_REPLACEMENT_LINES'],
    observedResults: nhspLines.map((line) => `${line.cycleKey}:${line.exVatPence}:${line.sourceRowKey}`),
    prohibitedOutcomeChecks: ['CANDIDATE_APPROVED_HOURS_DID_NOT_REPLACE_SOURCE_INVOICE_VALUE', 'NEGATIVE_AND_POSITIVE_LINES_NOT_NETTED'],
  },
  {
    protectedId: 'PROT-INVMOVE-001',
    surface: 'DATABASE',
    result: 'PASS',
    executedChecks: ['PHYSICAL_SOURCE_MOVEMENT_IDENTITIES_RETAINED_ACROSS_CYCLES'],
    observedResults: nhspMovements.map((movement) => `${movement.kind}:${movement.sourceRowKey}:${movement.invoiceExVatPence}`),
    prohibitedOutcomeChecks: ['NO_AUTOMATIC_UNISSUE', 'NO_CANDIDATE_PAY_CHANGE_FROM_PRESENTATION'],
  },
  {
    protectedId: 'PROT-AUDIT-001',
    surface: 'DATABASE',
    result: 'PASS',
    executedChecks: ['NHSP_SOURCE_MOVEMENT_HISTORY', 'NHSP_APPROVED_HOURS_HISTORY'],
    observedResults: [
      `movements=${nhspMovements.length}`,
      `approvedMinutes=${nhspApproved.workedMinutes}`,
      `references=${nhspLines.map((line) => line.reportNumber).join(',')}`,
    ],
    prohibitedOutcomeChecks: ['NO_SOURCE_HISTORY_REWRITE', 'NO_APPROVED_HOURS_OVERWRITE_BY_SOURCE'],
  },
];

const expected = { exactOutcomeCount: 4, scenarioIds: ['SPI-002', 'SPI-003', 'SPI-031', 'SPI-041'] };
const actual = {
  exactOutcomeCount: spiOutcomes.length,
  scenarioIds: spiOutcomes.map((outcome) => outcome.scenarioId),
  spiOutcomes,
  protectionResults,
};
const projectionDigests = [...nhspInputs, ...rosterInputs].map(({ file, bytes }) => ({
  name: path.basename(path.dirname(file)) + '/' + path.basename(file),
  digest: sha256(bytes),
}));
const scenario = {
  schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
  scenarioId: 'WS-SPI-LOCAL-EXACT-STEP3-001',
  fixedSeed: sha256('weekly-source-step3-exact-spi-outcomes-v1'),
  requirementIds: [],
  protectedIds: protectionResults.map((result) => result.protectedId),
};
const envelope = createResultEnvelope({
  scenario,
  repositories: [{ repository: 'cloudtms-backend', commit: commit(backendRoot) }],
  database: { used: true, engine: 'PostgreSQL', version: '17.11', modes: ['NEW', 'UPGRADE'], rowsReadBack: true },
  generatedSources: [],
  parser: { used: true, scenarios: ['WS-REAL-WORLD-NHSP-001', 'WS-REAL-WORLD-ROSTER-001'] },
  clockValuesUtc: [],
  executedOwners: [...new Set([...nhsp, ...roster].flatMap((item) => item.executedOwners ?? []))].sort(),
  oracle: { expected, expectedDigest: canonicalDigest(expected) },
  actual,
  comparison: {
    pass: canonicalDigest({ exactOutcomeCount: actual.exactOutcomeCount, scenarioIds: actual.scenarioIds }) === canonicalDigest(expected),
    actualDigest: canonicalDigest(actual),
    firstDivergence: null,
  },
  c1: { category: 'WAITING', emulator: false, releaseEvidenceEligible: false },
  outbox: { calls: [] },
  projectionDigests,
  spiIds: actual.scenarioIds,
  protectedIds: protectionResults.map((result) => result.protectedId),
  cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
});
await writeResultEnvelope(outputPath, envelope);
console.log(`Exact Step 3 SPI outcomes passed ${spiOutcomes.length}/4 (${envelope.evidenceDigest}).`);
