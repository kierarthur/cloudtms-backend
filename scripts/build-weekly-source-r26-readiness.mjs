import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

function values(flag) {
  const result = [];
  for (let index = 2; index < process.argv.length; index += 1) {
    if (process.argv[index] === flag) result.push(process.argv[index + 1]);
  }
  return result;
}

const output = values('--output').at(-1);
const files = values('--evidence');
if (!output || files.length === 0) throw new Error('Usage: --output FILE --evidence FILE [...]');

const required = new Set([
  'WS-DATABASE-COMPONENT-NEW-PG17-001',
  'WS-DATABASE-COMPONENT-UPGRADE-PG17-001',
  'WS-SERVICE-PARSER-PROFILES-001',
  'WS-MYTMS-CANDIDATE-MANAGER-EXACT-001',
  'WS-BROWSER-OFFICE-COMPLETE-R26-001',
  'WS-SHARED-SPI-LOCAL-LIMBS-R26-001',
]);
const children = [];
for (const file of files) {
  const bytes = await readFile(path.resolve(file));
  const value = JSON.parse(bytes.toString('utf8'));
  if (value.status !== 'PASS') throw new Error(`Readiness child is not PASS: ${file}`);
  required.delete(value.scenarioId);
  children.push({
    scenarioId: value.scenarioId,
    evidenceDigest: value.evidenceDigest,
    sha256: createHash('sha256').update(bytes).digest('hex'),
    path: path.resolve(file),
  });
}
if (required.size > 0) throw new Error(`Missing readiness children: ${[...required].join(', ')}`);

const shared = JSON.parse(await readFile(path.resolve(files.find((file) => file.includes('shared-spi-local-limbs'))), 'utf8'));
const proof = {
  schemaVersion: 'WEEKLY_SOURCE_LOCAL_SPI_READINESS_V2',
  status: 'LOCAL_SPI_LIMBS_PASS_HANDOVER2_PENDING',
  weeklySourceSpiLocalReady: true,
  weeklySourcePluginReady: false,
  localSharedScenarioPassCount: shared.actual.localPassCount,
  externalHandover2PendingCount: shared.actual.externalPendingCount,
  controllingMatrixIsFinalReadinessAuthority: true,
  children,
};
proof.evidenceDigest = createHash('sha256').update(JSON.stringify(proof)).digest('hex');
await mkdir(path.dirname(path.resolve(output)), { recursive: true });
await writeFile(path.resolve(output), `${JSON.stringify(proof, null, 2)}\n`);
process.stdout.write(`${proof.status}; local shared scenarios ${proof.localSharedScenarioPassCount}, HANDOVER 2 pending ${proof.externalHandover2PendingCount}.\n`);
