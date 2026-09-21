import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import {
  WEEKLY_SOURCE_SPI_EXECUTION_MAP,
  verifySpiExecutionEvidence,
} from '../tests/weekly-source/harness/spi-execution-map.mjs';

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

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

const ledgerFile = value('--spi-ledger');
const outputFile = value('--output');
const evidenceFiles = values('--evidence');
if (evidenceFiles.length === 0) throw new Error('At least one exact result envelope is required');

const ledgerRows = parseCsv(await readFile(ledgerFile, 'utf8'));
const envelopes = [];
const evidence = [];
for (const file of evidenceFiles) {
  const bytes = await readFile(file);
  const envelope = JSON.parse(bytes);
  if (envelope.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || envelope.status !== 'PASS') {
    throw new Error(`Evidence is not an exact passing result envelope: ${file}`);
  }
  envelopes.push(envelope);
  evidence.push({
    scenarioId: envelope.scenarioId,
    path: file,
    sha256: sha256(bytes),
    evidenceDigest: envelope.evidenceDigest,
    claimedSpiIds: [...new Set(envelope.spiIds ?? [])].sort(),
  });
}

// The verification helper requires one ledger row for every mapped SPI id. It
// accepts the sealed ledger as data and only credits an SPI id when a passing
// child envelope explicitly names that id. No expected text is copied to an
// observed result here.
const adjudication = verifySpiExecutionEvidence(ledgerRows, envelopes, { allowExternalHandover2: true });
const rows = adjudication.rows.map((row) => ({
  scenarioId: row.scenarioId,
  owner: row.owner,
  configuredStatus: row.status,
  localResult: row.localResult ?? (row.owner === 'HANDOVER2' ? 'NOT_APPLICABLE' : 'MISSING'),
  externalResult: row.externalResult ?? (row.owner === 'HANDOVER2' ? row.result : 'NOT_APPLICABLE'),
  combinedResult: row.result,
  requiredEvidenceScenarioIds: row.requiredEvidenceScenarioIds ?? [],
  missingEvidenceScenarioIds: row.missingEvidenceScenarioIds ?? [],
  exactClaimingEvidenceScenarioIds: evidence
    .filter((item) => (row.exactClaimingEvidenceScenarioIds ?? []).includes(item.scenarioId))
    .map((item) => item.scenarioId),
  supersededBy: row.supersededBy ?? null,
}));

const payload = {
  schemaVersion: 'WEEKLY_SOURCE_SPI_EXACT_ADJUDICATION_V1',
  proofId: 'WS-SPI-EXACT-ADJUDICATION-R27',
  status: adjudication.weeklySourceSpiLocalReady
    ? 'LOCAL_SPI_EXACT_PASS_HANDOVER2_PENDING'
    : 'LOCAL_SPI_EXACT_INCOMPLETE_HANDOVER2_PENDING',
  weeklySourceSpiLocalReady: adjudication.weeklySourceSpiLocalReady,
  weeklySourcePluginReady: false,
  total: WEEKLY_SOURCE_SPI_EXECUTION_MAP.length,
  localPassCount: rows.filter((row) => row.localResult === 'PASS').length,
  localNotApplicableCount: rows.filter((row) => row.localResult === 'NOT_APPLICABLE').length,
  localMissingCount: rows.filter((row) => row.localResult === 'MISSING').length,
  externalPendingCount: rows.filter((row) => row.externalResult === 'EXTERNAL_HANDOVER2_PENDING').length,
  evidence,
  rows,
};
payload.evidenceDigest = sha256(JSON.stringify(payload));
await mkdir(path.dirname(outputFile), { recursive: true });
await writeFile(outputFile, `${JSON.stringify(payload, null, 2)}\n`);
process.stdout.write(`${JSON.stringify({
  status: payload.status,
  localPassCount: payload.localPassCount,
  localNotApplicableCount: payload.localNotApplicableCount,
  localMissingCount: payload.localMissingCount,
  externalPendingCount: payload.externalPendingCount,
})}\n`);
