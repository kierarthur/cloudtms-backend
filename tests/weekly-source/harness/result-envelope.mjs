import { mkdir, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { canonicalDigest, canonicalJsonPretty, cloneJson, deepFreeze } from './canonical-json.mjs';

const SHA256_PATTERN = /^[a-f0-9]{64}$/;

function requireDigest(value, label) {
  if (!SHA256_PATTERN.test(value || '')) throw new TypeError(`${label} must be a lower-case SHA-256 digest`);
  return value;
}

function requireArray(value, label) {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  return cloneJson(value);
}

function assertSafeEvidence(value, location = '$') {
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertSafeEvidence(item, `${location}[${index}]`));
    return;
  }
  if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (/password|passphrase|secret|bearer|authorization|api[_-]?key|connection[_-]?string|database[_-]?url/i.test(key)) {
        throw new TypeError(`${location}.${key} is prohibited in a result envelope`);
      }
      assertSafeEvidence(item, `${location}.${key}`);
    }
    return;
  }
  if (typeof value === 'string' && /(?:postgres(?:ql)?:\/\/|https?:\/\/|\bBearer\s+[A-Za-z0-9._~+\/-]{12,})/i.test(value)) {
    throw new TypeError(`${location} contains connection or credential material`);
  }
}

export function createResultEnvelope({
  scenario,
  repositories,
  database,
  generatedSources = [],
  parser,
  clockValuesUtc,
  executedOwners,
  oracle,
  actual,
  comparison,
  c1,
  outbox,
  projectionDigests = [],
  acceptanceIds = [],
  protectedIds = scenario?.protectedIds ?? [],
  modelIds = [],
  // Plan 6.2 control sets. Every envelope carries all of them, empty where the run proves
  // nothing in that set, so the coverage gate reads an explicit zero rather than a silence.
  spiIds = [],
  issIds = [],
  uiStateIds = [],
  ftiIds = [],
  xsgIds = [],
  h2Ids = [],
  proofIds = [],
  controllingRequirementIds = [],
  foundation = null,
  cleanup
}) {
  if (!scenario?.scenarioId || !scenario?.fixedSeed || !scenario?.schemaVersion) throw new TypeError('Validated scenario identity is required');
  requireDigest(scenario.fixedSeed, 'scenario.fixedSeed');
  if (!oracle?.expectedDigest) throw new TypeError('Declared expected-outcome oracle is required');
  if (!comparison || typeof comparison.pass !== 'boolean') throw new TypeError('Expected/actual comparison is required');
  const body = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_RESULT_V1',
    scenarioContractVersion: scenario.schemaVersion,
    scenarioId: scenario.scenarioId,
    fixedSeed: scenario.fixedSeed,
    repositories: requireArray(repositories, 'repositories'),
    database: cloneJson(database),
    scenarioInputDigest: canonicalDigest(scenario),
    generatedSources: requireArray(generatedSources, 'generatedSources'),
    parser: cloneJson(parser),
    clockValuesUtc: requireArray(clockValuesUtc, 'clockValuesUtc'),
    executedOwners: requireArray(executedOwners, 'executedOwners'),
    expected: cloneJson(oracle.expected),
    actual: cloneJson(actual),
    expectedDigest: oracle.expectedDigest,
    actualDigest: comparison.actualDigest,
    c1: cloneJson(c1),
    outbox: cloneJson(outbox),
    projectionDigests: requireArray(projectionDigests, 'projectionDigests'),
    requirementIds: [...scenario.requirementIds].sort(),
    acceptanceIds: [...acceptanceIds].sort(),
    protectedIds: [...protectedIds].sort(),
    modelIds: [...modelIds].sort(),
    spiIds: [...spiIds].sort(),
    issIds: [...issIds].sort(),
    uiStateIds: [...uiStateIds].sort(),
    ftiIds: [...ftiIds].sort(),
    xsgIds: [...xsgIds].sort(),
    h2Ids: [...h2Ids].sort(),
    proofIds: [...proofIds].sort(),
    controllingRequirementIds: [...controllingRequirementIds].sort(),
    foundation: cloneJson(foundation),
    cleanup: cloneJson(cleanup),
    status: comparison.pass ? 'PASS' : 'FAIL',
    firstDivergence: cloneJson(comparison.firstDivergence)
  };
  for (const item of body.generatedSources) {
    if (item.fileName === null && item.sha256 === null) continue;
    requireDigest(item.sha256, 'generatedSources[].sha256');
  }
  for (const item of body.projectionDigests) requireDigest(item.digest, 'projectionDigests[].digest');
  assertSafeEvidence(body);
  const evidenceDigest = canonicalDigest(body);
  return deepFreeze({ ...body, evidenceDigest });
}

export async function writeResultEnvelope(filePath, envelope) {
  if (path.extname(filePath).toLowerCase() !== '.json') throw new TypeError('Result envelopes must be written as .json files');
  const absolutePath = path.resolve(filePath);
  await mkdir(path.dirname(absolutePath), { recursive: true });
  const temporaryPath = `${absolutePath}.${process.pid}.tmp`;
  await writeFile(temporaryPath, canonicalJsonPretty(envelope), { encoding: 'utf8', flag: 'wx' });
  await rename(temporaryPath, absolutePath);
  return { path: absolutePath, sha256: canonicalDigest(envelope) };
}
