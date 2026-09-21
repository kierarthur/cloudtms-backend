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

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

const outputFile = value('--output');
const registerFile = value('--protected-register');
const evidenceFiles = values('--evidence');
if (evidenceFiles.length === 0) throw new Error('At least one exact child envelope is required');

const registerText = await readFile(registerFile, 'utf8');
const protectedIds = [...new Set([...registerText.matchAll(/PROT-[A-Z]+-\d{3}/g)].map((match) => match[0]))].sort();
if (protectedIds.length !== 29) throw new Error(`Expected 29 protected ids, received ${protectedIds.length}`);

const children = [];
for (const file of evidenceFiles) {
  const bytes = await readFile(file);
  const envelope = JSON.parse(bytes);
  if (envelope.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || envelope.status !== 'PASS') {
    throw new Error(`Protected child is not an exact passing result envelope: ${file}`);
  }
  const protectionResults = Array.isArray(envelope?.actual?.protectionResults)
    ? envelope.actual.protectionResults.filter((result) => result?.result === 'PASS')
    : [];
  children.push({
    scenarioId: envelope.scenarioId,
    path: file,
    sha256: sha256(bytes),
    evidenceDigest: envelope.evidenceDigest,
    protectedIds: [...new Set(envelope.protectedIds ?? [])].sort(),
    protectionResults,
    surface: (() => {
      const id = String(envelope.scenarioId ?? '').toUpperCase();
      const fileName = file.toUpperCase();
      if (id.includes('DATABASE') || fileName.includes('DATABASE-')) return 'DATABASE';
      if (id.includes('OFFICE') || fileName.includes('OFFICE-')) return 'OFFICE';
      if (id.includes('MYTMS') || id.includes('CANDIDATE') || id.includes('MANAGER')) return 'CANDIDATE_MANAGER';
      if (id.includes('SERVICE') || id.includes('PARSER')) return 'SERVICE';
      if (id.includes('ANDROID') || id.includes('DEVICE')) return 'DEVICE';
      if (id.includes('DEPLOY') || id.includes('TEST-SMOKE')) return 'DEPLOYED_TEST';
      return 'UNCLASSIFIED';
    })(),
  });
}

const handover2Ids = new Set([
  'PROT-WB-001', 'PROT-DRAFT-001', 'PROT-BP-001',
  'PROT-CANCEL-001', 'PROT-REM-001', 'PROT-BANKALERT-001',
]);
const deployedIds = new Set(['PROT-NOTIFY-001', 'PROT-INFRA-001']);

// A Protection ID is not complete merely because one broad suite mentions it.
// The list below is the minimum set of independently observed surfaces implied
// by the sealed matrix's "Required differential proof" column.  It deliberately
// errs on the side of leaving a row open.  HANDOVER 2 and deployed-only rows are
// dealt with separately below and can never be promoted by a local child.
const requiredSurfacesById = Object.freeze({
  'PROT-INV-001': ['DATABASE', 'OFFICE'],
  'PROT-INVDOC-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-ISSUED-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-INVMOVE-001': ['DATABASE', 'OFFICE'],
  'PROT-ORDINV-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-DAILY-001': ['DATABASE', 'SERVICE', 'OFFICE', 'CANDIDATE_MANAGER'],
  'PROT-ORDW-001': ['DATABASE', 'SERVICE', 'OFFICE', 'CANDIDATE_MANAGER'],
  'PROT-PAY-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-ADV-001': ['DATABASE', 'SERVICE'],
  'PROT-EXP-001': ['DATABASE', 'SERVICE', 'OFFICE', 'CANDIDATE_MANAGER'],
  'PROT-CONTRACT-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-RATE-001': ['DATABASE', 'SERVICE'],
  'PROT-APP-001': ['CANDIDATE_MANAGER', 'DEVICE'],
  'PROT-SUMMARY-001': ['OFFICE'],
  'PROT-EXPORT-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-SETTINGS-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-SEC-001': ['DATABASE', 'SERVICE'],
  'PROT-AUDIT-001': ['DATABASE', 'SERVICE', 'OFFICE'],
  'PROT-LEGACY-001': ['DATABASE', 'SERVICE'],
  'PROT-ROTATION-001': ['DATABASE'],
  'PROT-UNAUTH-001': ['DATABASE'],
});
const rows = protectedIds.map((protectedId) => {
  const exactClaims = children.flatMap((child) => child.protectionResults.filter((result) => (
    result.protectedId === protectedId
      && Array.isArray(result.executedChecks)
      && result.executedChecks.length > 0
      && Array.isArray(result.observedResults)
      && result.observedResults.length > 0
      && Array.isArray(result.prohibitedOutcomeChecks)
      && result.prohibitedOutcomeChecks.length > 0
  )).map((result) => ({ child, result })));
  const requiredSurfaces = requiredSurfacesById[protectedId] ?? [];
  const provedSurfaces = [...new Set(exactClaims.map(({ result }) => result.surface))].sort();
  const missingSurfaces = requiredSurfaces.filter((surface) => !provedSurfaces.includes(surface));
  let status = requiredSurfaces.length > 0 && missingSurfaces.length === 0 ? 'PASS' : 'IN_PROGRESS';
  if (handover2Ids.has(protectedId)) status = 'EXTERNAL_HANDOVER2_PENDING';
  else if (deployedIds.has(protectedId)) status = 'STAGE16_PENDING';
  return {
    protectedId,
    status,
    requiredSurfaces,
    provedSurfaces,
    missingSurfaces,
    exactEvidenceScenarioIds: [...new Set(exactClaims.map(({ child }) => child.scenarioId))],
    evidencePaths: [...new Set(exactClaims.map(({ child }) => child.path))],
    reason: status === 'PASS'
      ? `Every required surface is named by current passing exact evidence: ${requiredSurfaces.join(', ')}.`
      : status === 'EXTERNAL_HANDOVER2_PENDING'
        ? 'The installed Banking Pay or Workbench result is owned by HANDOVER 2.'
        : status === 'STAGE16_PENDING'
          ? 'This protection requires a deployed TEST proof.'
          : `Exact protection evidence is incomplete. Missing surfaces: ${missingSurfaces.join(', ') || 'the sealed matrix has no executable surface mapping yet'}.`,
  };
});

const payload = {
  schemaVersion: 'WEEKLY_SOURCE_PROTECTED_EXACT_ADJUDICATION_V1',
  proofId: 'WS-PROTECTED-EXACT-ADJUDICATION-R27',
  status: rows.some((row) => row.status === 'IN_PROGRESS')
    ? 'LOCAL_PROTECTIONS_INCOMPLETE'
    : 'LOCAL_PROTECTIONS_EXACT_PASS_STAGE16_AND_HANDOVER2_PENDING',
  counts: Object.fromEntries(['PASS', 'IN_PROGRESS', 'EXTERNAL_HANDOVER2_PENDING', 'STAGE16_PENDING']
    .map((status) => [status, rows.filter((row) => row.status === status).length])),
  children,
  rows,
};
payload.evidenceDigest = sha256(JSON.stringify(payload));
await mkdir(path.dirname(outputFile), { recursive: true });
await writeFile(outputFile, `${JSON.stringify(payload, null, 2)}\n`);
process.stdout.write(`${JSON.stringify({ status: payload.status, counts: payload.counts })}\n`);
