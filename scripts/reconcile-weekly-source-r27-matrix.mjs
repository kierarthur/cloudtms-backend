import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

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

function csvValue(value) {
  const text = String(value ?? '');
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function writeCsv(rows, headers) {
  return `${[headers, ...rows.map((row) => headers.map((header) => row[header] ?? ''))]
    .map((items) => items.map(csvValue).join(','))
    .join('\r\n')}\r\n`;
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

const idFields = [
  'requirementIds', 'acceptanceIds', 'protectedIds', 'modelIds', 'spiIds',
  'issIds', 'uiStateIds', 'ftiIds', 'xsgIds', 'h2Ids', 'proofIds',
];

const matrixFile = value('--matrix');
const acceptanceFile = value('--acceptance');
const outputDirectory = value('--output-directory');
const spiFile = value('--spi-adjudication');
const protectedFile = value('--protected-adjudication');
const evidenceFiles = values('--evidence');
if (evidenceFiles.length === 0) throw new Error('At least one exact result envelope is required');

const matrixRows = parseCsv((await readFile(matrixFile, 'utf8')).replace(/^\uFEFF/, ''));
const headers = Object.keys(matrixRows[0]);
const acceptanceRows = parseCsv((await readFile(acceptanceFile, 'utf8')).replace(/^\uFEFF/, ''));
const acceptance = new Map(acceptanceRows.map((row) => [row.id, row]));

const evidence = [];
for (const file of evidenceFiles) {
  const bytes = await readFile(file);
  const envelope = JSON.parse(bytes);
  if (envelope.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || envelope.status !== 'PASS') {
    throw new Error(`Evidence is not an exact passing result envelope: ${file}`);
  }
  const ids = new Set(idFields.flatMap((field) => envelope[field] ?? []));
  evidence.push({
    scenarioId: envelope.scenarioId,
    path: file,
    sha256: sha256(bytes),
    evidenceDigest: envelope.evidenceDigest,
    ids,
    executedOwners: envelope.executedOwners ?? [],
  });
}

const spiPayload = JSON.parse(await readFile(spiFile, 'utf8'));
if (spiPayload.schemaVersion !== 'WEEKLY_SOURCE_SPI_EXACT_ADJUDICATION_V1') {
  throw new Error('The exact SPI adjudication is required');
}
const spiRows = new Map(spiPayload.rows.map((row) => [row.scenarioId, row]));

const protectedPayload = JSON.parse(await readFile(protectedFile, 'utf8'));
if (protectedPayload.schemaVersion !== 'WEEKLY_SOURCE_PROTECTED_EXACT_ADJUDICATION_V1') {
  throw new Error('The exact protected adjudication is required');
}
const protectedRows = new Map(protectedPayload.rows.map((row) => [row.protectedId, row]));

function exactEvidenceFor(controlId) {
  return evidence.filter((item) => item.ids.has(controlId));
}

function isExactHandover2(row) {
  return row.control_set === 'handover2'
    || /^HANDOVER\s*2$/i.test(row.implementation_owner.trim())
    || /^BANKING_PAY_OTHER_CHAT$/i.test(row.implementation_owner.trim())
    || /^WORKBENCH_COMPATIBILITY$/i.test(row.implementation_owner.trim());
}

function needsDeployedTest(row) {
  return /authorised TEST|deployed|Miget|real TEST/i.test(row.verification_environment);
}

const checkedAt = '2026-09-21';
const outcomes = [];
for (const row of matrixRows) {
  const acceptanceRow = acceptance.get(row.source_control_id) ?? null;
  const exact = exactEvidenceFor(row.source_control_id);
  const deployed = needsDeployedTest(row);
  const exactHandover2 = isExactHandover2(row);
  const spi = row.control_set === 'source_pay_invoice' ? spiRows.get(row.source_control_id) : null;
  const protection = row.control_set === 'protected' ? protectedRows.get(row.source_control_id) : null;

  let status = 'IN_PROGRESS';
  let actualOutcome = 'No current passing result envelope explicitly names this exact control.';
  let dependency = '';

  if (spi) {
    if (spi.localResult === 'NOT_APPLICABLE') {
      status = 'NOT_APPLICABLE';
      actualOutcome = `The exact SPI adjudication records this scenario as superseded by ${spi.supersededBy}.`;
    } else if (spi.localResult === 'PASS') {
      if (spi.externalResult === 'EXTERNAL_HANDOVER2_PENDING') {
        status = 'EXTERNAL_HANDOVER2_PENDING';
        dependency = 'HANDOVER 2 installed Banking Pay/Workbench result';
        actualOutcome = `The exact named Weekly Source result passed; the separately owned HANDOVER 2 result remains pending. Exact evidence: ${spi.exactClaimingEvidenceScenarioIds.join(', ')}.`;
      } else if (deployed) {
        status = 'IN_PROGRESS';
        dependency = 'Stage 16 authorised TEST deployment and post-deployment proof';
        actualOutcome = `The exact local result passed; the required deployed TEST result remains pending. Exact evidence: ${spi.exactClaimingEvidenceScenarioIds.join(', ')}.`;
      } else {
        status = 'PASS';
        actualOutcome = `The exact named Weekly Source result passed. Exact evidence: ${spi.exactClaimingEvidenceScenarioIds.join(', ')}.`;
      }
    } else {
      dependency = spi.externalResult === 'EXTERNAL_HANDOVER2_PENDING'
        ? 'Weekly Source local execution plus HANDOVER 2 installed result'
        : '';
      actualOutcome = `The exact SPI adjudication found no executed Weekly Source result for this scenario. Missing evidence: ${spi.missingEvidenceScenarioIds.join(', ') || 'an envelope explicitly naming the SPI id'}.`;
    }
  } else if (protection) {
    status = protection.status === 'STAGE16_PENDING' ? 'IN_PROGRESS' : protection.status;
    dependency = protection.status === 'EXTERNAL_HANDOVER2_PENDING'
      ? 'HANDOVER 2 installed Banking Pay/Workbench result'
      : protection.status === 'STAGE16_PENDING'
        ? 'Stage 16 authorised TEST deployment and post-deployment proof'
        : '';
    actualOutcome = protection.reason;
  } else if (exactHandover2) {
    status = 'EXTERNAL_HANDOVER2_PENDING';
    dependency = 'HANDOVER 2 installed Banking Pay/Workbench implementation and integrated proof';
    actualOutcome = 'This exact control is owned by HANDOVER 2 and has not been claimed by Weekly Source.';
  } else if (deployed) {
    status = 'IN_PROGRESS';
    dependency = 'Stage 16 authorised TEST deployment and post-deployment proof';
    actualOutcome = exact.length > 0
      ? `The exact local result passed in ${exact.map((item) => item.scenarioId).join(', ')}; the required deployed TEST result remains pending.`
      : 'The required deployed TEST result has not run; no local evidence is promoted as a substitute.';
  } else if (exact.length > 0) {
    status = 'PASS';
    actualOutcome = `Current passing result envelope(s) explicitly name this control: ${exact.map((item) => item.scenarioId).join(', ')}.`;
  }

  const exactPaths = exact.map((item) => path.relative(path.dirname(matrixFile), item.path).replaceAll('\\', '/'));
  const exactHashes = exact.map((item) => item.sha256);
  outcomes.push({
    matrixRowId: row.matrix_row_id,
    controlSet: row.control_set,
    sourceControlId: row.source_control_id,
    setup: acceptanceRow?.setup ?? row.journeys,
    action: acceptanceRow?.action ?? row.verification_kind,
    expectedOutcome: row.required_outcome,
    prohibitedOutcome: row.prohibited_outcome,
    actualOutcome,
    exactEvidenceScenarioIds: exact.map((item) => item.scenarioId),
    exactEvidencePaths: exactPaths,
    status,
    externalDependency: dependency,
  });

  row.current_status = status;
  row.last_checked_at = checkedAt;
  row.evidence_paths = exactPaths.join(';');
  row.evidence_sha256 = exactHashes.join(';');
  row.evidence_executed_at = exact.length > 0 ? checkedAt : '';
  row.external_dependency = dependency;
  row.status_reason = actualOutcome;
}

await mkdir(outputDirectory, { recursive: true });
const outcomesFile = path.join(outputDirectory, 'exact-control-outcomes.json');
const outcomesPayload = {
  schemaVersion: 'WEEKLY_SOURCE_EXACT_CONTROL_OUTCOMES_V1',
  generatedAt: checkedAt,
  count: outcomes.length,
  outcomes,
};
outcomesPayload.evidenceDigest = sha256(JSON.stringify(outcomesPayload));
await writeFile(outcomesFile, `${JSON.stringify(outcomesPayload, null, 2)}\n`);

// The row-specific outcome file is supporting traceability, never the reason a
// row passes. PASS was decided above only from an exact child result id.
const outcomesRelative = path.relative(path.dirname(matrixFile), outcomesFile).replaceAll('\\', '/');
const outcomesHash = sha256(await readFile(outcomesFile));
for (const row of matrixRows) {
  row.evidence_paths = [row.evidence_paths, outcomesRelative].filter(Boolean).join(';');
  row.evidence_sha256 = [row.evidence_sha256, outcomesHash].filter(Boolean).join(';');
}
await writeFile(matrixFile, writeCsv(matrixRows, headers));

const statuses = ['PASS', 'IN_PROGRESS', 'EXTERNAL_HANDOVER2_PENDING', 'NOT_APPLICABLE'];
const summary = {
  schemaVersion: 'WEEKLY_SOURCE_R27_MATRIX_RECONCILIATION_V1',
  rows: matrixRows.length,
  counts: Object.fromEntries(statuses.map((status) => [status, matrixRows.filter((row) => row.current_status === status).length])),
  exactEvidenceScenarioCount: evidence.length,
  exactControlOutcomeSha256: outcomesHash,
  matrixSha256: sha256(await readFile(matrixFile)),
};
await writeFile(path.join(outputDirectory, 'stage9-matrix-reconciliation.json'), `${JSON.stringify(summary, null, 2)}\n`);
process.stdout.write(`${JSON.stringify(summary)}\n`);
