import { canonicalDigest, cloneJson, deepFreeze, sha256Hex } from './canonical-json.mjs';

const DELIVERY_OUTCOMES = new Set(['ACCEPTED', 'RETRYABLE', 'FINAL']);

function requireJsonObject(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError(`${label} must be a JSON object`);
  return cloneJson(value);
}

function normalizeOutcome(value, channel) {
  const outcome = typeof value === 'string' ? { outcome: value } : requireJsonObject(value, `${channel} outcome`);
  if (!DELIVERY_OUTCOMES.has(outcome.outcome)) {
    throw new TypeError(`${channel} outcome must be ACCEPTED, RETRYABLE or FINAL`);
  }
  return deepFreeze(outcome);
}

class CapturedDeliveryAdapter {
  #channel;
  #calls = [];
  #outcomes;
  #strictQueue;

  constructor(channel, outcomes) {
    this.#channel = channel;
    this.#strictQueue = outcomes !== undefined;
    this.#outcomes = (outcomes || []).map((outcome) => normalizeOutcome(outcome, channel));
  }

  async send(request) {
    const captured = requireJsonObject(request, `${this.#channel} request`);
    const sequence = this.#calls.length + 1;
    this.#calls.push(deepFreeze({ sequence, request: captured }));
    if (this.#strictQueue && this.#outcomes.length === 0) {
      throw new Error(`No declared ${this.#channel} fake outcome remains for call ${sequence}`);
    }
    return cloneJson(this.#outcomes.shift() || { outcome: 'ACCEPTED' });
  }

  calls() {
    return cloneJson(this.#calls);
  }

  summary() {
    return deepFreeze({
      channel: this.#channel,
      count: this.#calls.length,
      requestDigest: canonicalDigest(this.#calls.map((call) => call.request))
    });
  }

  assertAllDeclaredOutcomesUsed() {
    if (this.#outcomes.length !== 0) throw new Error(`${this.#channel} has ${this.#outcomes.length} unused declared fake outcomes`);
  }
}

class CapturedR2Adapter {
  #objects = new Map();
  #calls = [];

  async put(key, value, options = {}) {
    if (typeof key !== 'string' || key.length === 0 || key.length > 1024) throw new TypeError('R2 fake key is invalid');
    const bytes = Buffer.isBuffer(value) ? Buffer.from(value) : Buffer.from(value instanceof Uint8Array ? value : String(value), 'utf8');
    const metadata = options && typeof options === 'object' ? cloneJson(options) : {};
    this.#objects.set(key, { bytes, metadata });
    this.#calls.push(deepFreeze({ operation: 'PUT', key, byteLength: bytes.byteLength, sha256: sha256Hex(bytes), metadata }));
    return { key, size: bytes.byteLength, sha256: sha256Hex(bytes) };
  }

  async get(key) {
    this.#calls.push(deepFreeze({ operation: 'GET', key }));
    const found = this.#objects.get(key);
    if (!found) return null;
    return { bytes: Buffer.from(found.bytes), metadata: cloneJson(found.metadata) };
  }

  async delete(key) {
    const existed = this.#objects.delete(key);
    this.#calls.push(deepFreeze({ operation: 'DELETE', key, existed }));
    return { existed };
  }

  calls() {
    return cloneJson(this.#calls);
  }

  summary() {
    return deepFreeze({ channel: 'r2', count: this.#calls.length, requestDigest: canonicalDigest(this.#calls) });
  }
}

export class UnexpectedExternalNetworkError extends Error {
  constructor() {
    super('Unexpected external network request was blocked by the Weekly Source harness');
    this.name = 'UnexpectedExternalNetworkError';
    this.code = 'WEEKLY_SOURCE_UNEXPECTED_NETWORK';
  }
}

function exactLoopbackOrigin(value) {
  try {
    const parsed = new URL(String(value));
    if (parsed.protocol !== 'http:' || parsed.hostname !== '127.0.0.1' || !parsed.port) return null;
    return parsed.origin;
  } catch {
    return null;
  }
}

export async function withExternalNetworkDenied(callback, options = {}) {
  if (typeof callback !== 'function') throw new TypeError('callback must be callable');
  const priorFetch = globalThis.fetch;
  let attempts = 0;
  const allowedOrigins = new Set((options.allowedLoopbackOrigins || []).map(exactLoopbackOrigin));
  if (allowedOrigins.has(null)) throw new TypeError('allowedLoopbackOrigins accepts only explicit 127.0.0.1 HTTP origins');
  const blockedFetch = async (input, init) => {
    const url = exactLoopbackOrigin(input instanceof Request ? input.url : input);
    if (url && allowedOrigins.has(url)) return priorFetch(input, init);
    attempts += 1;
    throw new UnexpectedExternalNetworkError();
  };
  globalThis.fetch = blockedFetch;
  try {
    return await callback({ networkAttempts: () => attempts });
  } finally {
    if (globalThis.fetch !== blockedFetch) throw new Error('Global fetch changed while the Weekly Source network guard was installed');
    globalThis.fetch = priorFetch;
  }
}

export function createExternalEffectFakes({ emailOutcomes, pushOutcomes, providerOutcomes } = {}) {
  const email = new CapturedDeliveryAdapter('email', emailOutcomes);
  const push = new CapturedDeliveryAdapter('push', pushOutcomes);
  const provider = new CapturedDeliveryAdapter('provider', providerOutcomes);
  const r2 = new CapturedR2Adapter();
  return Object.freeze({
    email,
    push,
    provider,
    r2,
    summary() {
      return deepFreeze({ email: email.summary(), push: push.summary(), provider: provider.summary(), r2: r2.summary() });
    },
    assertAllDeclaredOutcomesUsed() {
      email.assertAllDeclaredOutcomesUsed();
      push.assertAllDeclaredOutcomesUsed();
      provider.assertAllDeclaredOutcomesUsed();
    }
  });
}
