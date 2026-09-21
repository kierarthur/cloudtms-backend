#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { canonicalDigest } from '../tests/weekly-source/harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../tests/weekly-source/harness/result-envelope.mjs';
import { WEEKLY_SOURCE_SPI_EXECUTION_MAP } from '../tests/weekly-source/harness/spi-execution-map.mjs';

function values(name) {
  const output = [];
  for (let index = 0; index < process.argv.length; index += 1) {
    if (process.argv[index] === name && process.argv[index + 1]) output.push(path.resolve(process.argv[index + 1]));
  }
  return output;
}

function value(name) {
  const result = values(name);
  if (result.length !== 1) throw new Error(`${name} is required exactly once`);
  return result[0];
}

function sha256(bytes) { return createHash('sha256').update(bytes).digest('hex'); }

function commit(root) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8', windowsHide: true });
  const output = String(result.stdout ?? '').trim();
  if (result.status !== 0 || !/^[a-f0-9]{40}$/.test(output)) throw new Error('Backend commit is unavailable');
  return output;
}

function parseCsv(text) {
  const rows = [];
  let row = [], field = '', quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (quoted) {
      if (char === '"' && text[index + 1] === '"') { field += '"'; index += 1; }
      else if (char === '"') quoted = false;
      else field += char;
    } else if (char === '"') quoted = true;
    else if (char === ',') { row.push(field); field = ''; }
    else if (char === '\n') { row.push(field.replace(/\r$/, '')); rows.push(row); row = []; field = ''; }
    else field += char;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  const headers = rows.shift();
  return rows.filter((item) => item.some(Boolean)).map((item) => Object.fromEntries(headers.map((header, index) => [header, item[index] ?? ''])));
}

const backendRoot = value('--backend-root');
const ledgerPath = value('--spi-ledger');
const outputPath = value('--output');
const evidencePaths = values('--evidence');
if (evidencePaths.length === 0) throw new Error('At least one exact child evidence file is required');
const scenarioPaths = values('--scenario');
if (scenarioPaths.length < 2) throw new Error('The populated NHSP and Roster scenarios are required');

const children = [];
const projectionDigests = [];
for (const file of evidencePaths) {
  const bytes = await readFile(file);
  const envelope = JSON.parse(bytes);
  if (envelope.status !== 'PASS' || !envelope.evidenceDigest) throw new Error(`Child evidence is not a PASS envelope: ${file}`);
  children.push(envelope);
  projectionDigests.push({ name: `child/${envelope.scenarioId}`, digest: sha256(bytes) });
}

const generatedSources = [];
for (const file of scenarioPaths) {
  const bytes = await readFile(file);
  const scenario = JSON.parse(bytes);
  if (!scenario.tags?.includes('REAL_WORLD') || !scenario.actions?.length || !scenario.sourceUploads?.length) {
    throw new Error(`Scenario is not a populated real-world contract: ${file}`);
  }
  generatedSources.push({ fileName: path.basename(file), sha256: sha256(bytes), scenarioId: scenario.scenarioId });
}

const sharedIds = WEEKLY_SOURCE_SPI_EXECUTION_MAP
  .filter((row) => row.owner === 'SHARED_WEEKLY_SOURCE_AND_HANDOVER2')
  .map((row) => row.scenarioId);
if (sharedIds.length !== 59) throw new Error(`Expected 59 shared SPI rows, received ${sharedIds.length}`);
const ledgerRows = parseCsv(await readFile(ledgerPath, 'utf8'));
const ledgerById = new Map(ledgerRows.map((row) => [row.scenario_id, row]));
const outcomes = sharedIds.map((scenarioId) => {
  const row = ledgerById.get(scenarioId);
  if (!row) throw new Error(`SPI ledger row is missing: ${scenarioId}`);
  const claims = children.flatMap((child) => child?.actual?.spiOutcomes ?? [])
    .filter((outcome) => outcome?.scenarioId === scenarioId && outcome?.localResult === 'PASS');
  if (claims.length !== 1) {
    throw new Error(`${scenarioId} requires exactly one observed local outcome; received ${claims.length}.`);
  }
  const claim = claims[0];
  const observations = claim.observations;
  if (!observations || !Array.isArray(observations.executedActions) || !observations.executedActions.length
      || !Array.isArray(observations.databaseReadbacks) || !observations.databaseReadbacks.length
      || !Array.isArray(observations.prohibitedOutcomeChecks) || !observations.prohibitedOutcomeChecks.length
      || typeof observations.actualInvoiceResult !== 'string' || !observations.actualInvoiceResult
      || typeof observations.actualOfficeAuditResult !== 'string' || !observations.actualOfficeAuditResult) {
    throw new Error(`${scenarioId} has no complete observed local result.`);
  }
  return structuredClone(claim);
});

const expected = {
  proofKind: 'PER_SPI_EXECUTED_LOCAL_OUTCOMES_WITH_EXTERNAL_SETTLEMENT_PENDING',
  sharedScenarioCount: 59,
  localPassCount: 59,
  externalPendingCount: 59,
};
const actual = {
  proofKind: 'PER_SPI_EXECUTED_LOCAL_OUTCOMES_WITH_EXTERNAL_SETTLEMENT_PENDING',
  sharedScenarioCount: outcomes.length,
  localPassCount: outcomes.filter((outcome) => outcome.localResult === 'PASS').length,
  externalPendingCount: outcomes.length,
  spiOutcomes: outcomes,
};
const scenario = {
  schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
  scenarioId: 'WS-SHARED-SPI-LOCAL-LIMBS-R28-001',
  fixedSeed: sha256('weekly-source-shared-spi-local-limbs-r28'),
  requirementIds: [],
  protectedIds: [],
};
const executedOwners = [...new Set(children.flatMap((child) => child.executedOwners ?? []))].sort();
const envelope = createResultEnvelope({
  scenario,
  repositories: [{ repository: 'cloudtms-backend', commit: commit(backendRoot) }],
  database: {
    used: true,
    engine: 'PostgreSQL',
    modes: ['COMPONENT_NEW', 'COMPONENT_UPGRADE'],
    note: 'Exact child envelopes were executed on PostgreSQL 17.11; settlement output remains external.',
  },
  generatedSources,
  parser: { used: true, realWorldScenarioContracts: generatedSources.map((item) => item.scenarioId) },
  clockValuesUtc: [],
  executedOwners,
  oracle: { expected, expectedDigest: canonicalDigest(expected) },
  actual,
  comparison: {
    pass: actual.sharedScenarioCount === 59 && actual.localPassCount === 59,
    actualDigest: canonicalDigest(actual),
    firstDivergence: null,
  },
  c1: { category: 'WAITING', emulator: false, releaseEvidenceEligible: false },
  outbox: { calls: [] },
  projectionDigests,
  spiIds: sharedIds,
  cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
});
await writeResultEnvelope(outputPath, envelope);
console.log(`Shared SPI local limbs passed ${sharedIds.length}/59; HANDOVER 2 settlement limbs remain pending (${envelope.evidenceDigest}).`);
