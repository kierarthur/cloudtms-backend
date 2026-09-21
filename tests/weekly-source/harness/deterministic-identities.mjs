import { createHash } from 'node:crypto';

const ID_NAMESPACE = 'cloudtms.weekly-source.test.scenario.v1';
const SCENARIO_PATTERN = /^WS-[A-Z0-9][A-Z0-9_-]{2,79}$/;
const ROLE_PATTERN = /^[a-z][a-z0-9_.:-]{0,79}$/;

function requireTuple(scenarioId, entityRole, ordinal) {
  if (!SCENARIO_PATTERN.test(scenarioId)) throw new TypeError('scenarioId does not satisfy the Weekly Source scenario ID contract');
  if (!ROLE_PATTERN.test(entityRole)) throw new TypeError('entityRole must be a stable lower-case role name');
  if (!Number.isSafeInteger(ordinal) || ordinal < 0 || ordinal > 999999) throw new TypeError('ordinal must be an integer between 0 and 999999');
}

function defaultDigest(parts) {
  const hash = createHash('sha256');
  for (const part of parts) hash.update(String(part), 'utf8').update('\0', 'utf8');
  return hash.digest();
}

function uuidFromDigest(digest) {
  if (!(digest instanceof Uint8Array) || digest.byteLength < 16) throw new TypeError('Identity digest must contain at least 16 bytes');
  const bytes = Buffer.from(digest.subarray(0, 16));
  bytes[6] = (bytes[6] & 0x0f) | 0x80;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function digestForTuple(scenarioId, entityRole, ordinal, digestFunction = defaultDigest) {
  requireTuple(scenarioId, entityRole, ordinal);
  return digestFunction([ID_NAMESPACE, scenarioId, entityRole, ordinal]);
}

export function deriveDeterministicUuid(scenarioId, entityRole, ordinal = 0) {
  return uuidFromDigest(digestForTuple(scenarioId, entityRole, ordinal));
}

function externalSourceKeyFromDigest(scenarioId, entityRole, ordinal, { prefix, maxLength }, digestFunction) {
  requireTuple(scenarioId, entityRole, ordinal);
  const safePrefix = String(prefix).toUpperCase();
  if (!/^[A-Z][A-Z0-9_-]{0,19}$/.test(safePrefix)) throw new TypeError('prefix must be a short source-profile-safe value');
  if (!Number.isInteger(maxLength) || maxLength < safePrefix.length + 10 || maxLength > 160) {
    throw new TypeError('maxLength cannot safely hold a deterministic external key');
  }
  const rolePart = entityRole.toUpperCase().replace(/[^A-Z0-9]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 24);
  const digest = Buffer.from(digestForTuple(scenarioId, `external:${entityRole}`, ordinal, digestFunction)).toString('hex').slice(0, 24).toUpperCase();
  return `${safePrefix}-${rolePart}-${digest}`.slice(0, maxLength).replace(/[-_]$/, '');
}

export function deriveExternalSourceKey(scenarioId, entityRole, ordinal = 0, { prefix = 'WS', maxLength = 64 } = {}) {
  return externalSourceKeyFromDigest(scenarioId, entityRole, ordinal, { prefix, maxLength }, defaultDigest);
}

export class DeterministicIdentityRegistry {
  #scenarioId;
  #digestFunction;
  #uuidOwners = new Map();
  #externalOwners = new Map();

  constructor(scenarioId, { digestFunction = defaultDigest } = {}) {
    requireTuple(scenarioId, 'registry', 0);
    if (typeof digestFunction !== 'function') throw new TypeError('digestFunction must be callable');
    this.#scenarioId = scenarioId;
    this.#digestFunction = digestFunction;
  }

  uuid(entityRole, ordinal = 0) {
    const owner = `${entityRole}:${ordinal}`;
    const uuid = uuidFromDigest(digestForTuple(this.#scenarioId, entityRole, ordinal, this.#digestFunction));
    const previous = this.#uuidOwners.get(uuid);
    if (previous && previous !== owner) throw new Error(`Deterministic UUID collision between ${previous} and ${owner}`);
    this.#uuidOwners.set(uuid, owner);
    return uuid;
  }

  externalKey(entityRole, ordinal = 0, options = {}) {
    const owner = `${entityRole}:${ordinal}`;
    const key = externalSourceKeyFromDigest(
      this.#scenarioId,
      entityRole,
      ordinal,
      { prefix: options.prefix || 'WS', maxLength: options.maxLength || 64 },
      this.#digestFunction
    );
    const collisionGroup = options.declaredCollisionGroup || null;
    if (collisionGroup && !/^[A-Z][A-Z0-9_-]{1,79}$/.test(collisionGroup)) {
      throw new TypeError('declaredCollisionGroup must be a stable upper-case scenario declaration');
    }
    const previous = this.#externalOwners.get(key);
    if (previous?.owner === owner && previous.collisionGroup !== collisionGroup) {
      throw new Error(`External source-key collision declaration changed for ${owner}`);
    }
    if (previous && previous.owner !== owner) {
      const declared = collisionGroup && previous.collisionGroup === collisionGroup;
      if (!declared) throw new Error(`External source-key collision between ${previous.owner} and ${owner}`);
    }
    this.#externalOwners.set(key, { owner, collisionGroup });
    return key;
  }
}

export { ID_NAMESPACE };
