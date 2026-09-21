import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

function argValues(flag) {
  const values = [];
  for (let index = 2; index < process.argv.length; index += 1) {
    if (process.argv[index] === flag) values.push(process.argv[index + 1]);
  }
  return values;
}

function argValue(flag) {
  return argValues(flag).at(-1) ?? null;
}

function fail(message) {
  throw new Error(message);
}

const output = argValue('--output');
const evidenceFiles = argValues('--evidence');
if (!output || evidenceFiles.length === 0) fail('Usage: --output FILE --evidence FILE [...]');

const requiredScenarios = new Set([
  'WS-DATABASE-COMPONENT-NEW-PG17-001',
  'WS-DATABASE-COMPONENT-UPGRADE-PG17-001',
  'WS-SERVICE-PARSER-PROFILES-001',
  'WS-MYTMS-CANDIDATE-MANAGER-EXACT-001',
  'WS-BROWSER-OFFICE-COMPLETE-R26-001',
  'WS-DIFFERENTIAL-PROTECTED-29-001',
]);

const children = [];
for (const file of evidenceFiles) {
  const bytes = await readFile(path.resolve(file));
  const value = JSON.parse(bytes.toString('utf8'));
  if (value.status !== 'PASS') fail(`Protected child evidence is not PASS: ${file}`);
  requiredScenarios.delete(value.scenarioId);
  children.push({
    scenarioId: value.scenarioId,
    evidenceDigest: value.evidenceDigest,
    fileSha256: createHash('sha256').update(bytes).digest('hex'),
    path: path.resolve(file),
  });
}
if (requiredScenarios.size > 0) fail(`Missing protected child scenarios: ${[...requiredScenarios].join(', ')}`);

const locallyProvedProtectionIds = [
  'PROT-INV-001',
  'PROT-INVDOC-001',
  'PROT-ISSUED-001',
  'PROT-INVMOVE-001',
  'PROT-ORDINV-001',
  'PROT-DAILY-001',
  'PROT-ORDW-001',
  'PROT-PAY-001',
  'PROT-ADV-001',
  'PROT-EXP-001',
  'PROT-CONTRACT-001',
  'PROT-RATE-001',
  'PROT-APP-001',
  'PROT-SUMMARY-001',
  'PROT-EXPORT-001',
  'PROT-SETTINGS-001',
  'PROT-SEC-001',
  'PROT-AUDIT-001',
  'PROT-LEGACY-001',
  'PROT-ROTATION-001',
  'PROT-UNAUTH-001',
];

const externalHandover2ProtectionIds = [
  'PROT-WB-001',
  'PROT-DRAFT-001',
  'PROT-BP-001',
  'PROT-CANCEL-001',
  'PROT-REM-001',
  'PROT-BANKALERT-001',
];

const stage16PendingProtectionIds = ['PROT-NOTIFY-001', 'PROT-INFRA-001'];
const surfaceEvidence = {
  databaseAndUpgrade: ['WS-DATABASE-COMPONENT-NEW-PG17-001', 'WS-DATABASE-COMPONENT-UPGRADE-PG17-001'],
  backendAndReports: ['WS-SERVICE-PARSER-PROFILES-001'],
  office: ['WS-BROWSER-OFFICE-COMPLETE-R26-001'],
  candidateAndManager: ['WS-MYTMS-CANDIDATE-MANAGER-EXACT-001'],
  beforeAfterDifferential: ['WS-DIFFERENTIAL-PROTECTED-29-001'],
};

const payload = {
  schemaVersion: 'WEEKLY_SOURCE_PROTECTED_COMPOSITE_V2',
  proofId: 'WS-PROTECTED-EIGHT-SURFACE-R26',
  status: 'LOCAL_PROTECTIONS_PASS_STAGE16_AND_HANDOVER2_PENDING',
  locallyProvedProtectionIds,
  externalHandover2ProtectionIds,
  stage16PendingProtectionIds,
  ownerApprovedDeferredFollowOn: ['S12-027_CANDIDATE_POST_SUBMISSION_CORRECTION_POLICY'],
  surfaceEvidence,
  children,
};
payload.evidenceDigest = createHash('sha256').update(JSON.stringify(payload)).digest('hex');
await mkdir(path.dirname(path.resolve(output)), { recursive: true });
await writeFile(path.resolve(output), `${JSON.stringify(payload, null, 2)}\n`);
process.stdout.write(`Protected evidence: ${locallyProvedProtectionIds.length} local PASS, ${externalHandover2ProtectionIds.length} HANDOVER 2 pending, ${stage16PendingProtectionIds.length} Stage 16 pending.\n`);
