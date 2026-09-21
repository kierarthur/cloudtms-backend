import { createHash } from 'node:crypto';

function assertJsonValue(value, path = '$') {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError(`${path} must be a finite JSON number`);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertJsonValue(item, `${path}[${index}]`));
    return;
  }
  if (typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype) {
    for (const [key, item] of Object.entries(value)) {
      if (item === undefined) throw new TypeError(`${path}.${key} cannot be undefined`);
      assertJsonValue(item, `${path}.${key}`);
    }
    return;
  }
  throw new TypeError(`${path} is not JSON-safe`);
}

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, canonicalize(value[key])])
    );
  }
  return value;
}

export function canonicalJson(value) {
  assertJsonValue(value);
  return JSON.stringify(canonicalize(value));
}

export function canonicalJsonPretty(value) {
  assertJsonValue(value);
  return `${JSON.stringify(canonicalize(value), null, 2)}\n`;
}

export function sha256Hex(value) {
  const input = Buffer.isBuffer(value) || value instanceof Uint8Array
    ? value
    : Buffer.from(String(value), 'utf8');
  return createHash('sha256').update(input).digest('hex');
}

export function canonicalDigest(value) {
  return sha256Hex(canonicalJson(value));
}

export function cloneJson(value) {
  assertJsonValue(value);
  return JSON.parse(JSON.stringify(value));
}

export function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  Object.freeze(value);
  for (const item of Object.values(value)) deepFreeze(item);
  return value;
}

export function jsonValuesEqual(left, right) {
  return canonicalJson(left) === canonicalJson(right);
}

