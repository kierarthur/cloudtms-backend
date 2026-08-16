import {
  CANDIDATE_DAILY_HMAC_VERSION,
  findCandidateDailyRoute,
  isCandidateDailySystemPath,
  isValidCorrelationId,
  sha256Hex,
  validateDailyIdempotency
} from './candidate-daily-contract-v1.js';

const encoder = new TextEncoder();
const fatalDecoder = new TextDecoder('utf-8', { fatal: true });
const UNRESERVED_RE = /^[A-Za-z0-9._~-]$/;
const HEADER_NAME_RE = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
const KEY_ID_RE = /^[A-Za-z0-9._-]{1,64}$/;
const NONCE_RE = /^[A-Za-z0-9_-]{22,64}$/;
const LOWER_HEX_64_RE = /^[0-9a-f]{64}$/;
const TIMESTAMP_RE = /^[0-9]{10}$/;
const MAX_CLOCK_SKEW_SECONDS = 300;
const NONCE_RETENTION_SECONDS = 10 * 60;

const SECURITY_HEADERS = Object.freeze([
  'x-cloudtms-key-id',
  'x-cloudtms-signature-version',
  'x-cloudtms-timestamp',
  'x-cloudtms-nonce',
  'x-cloudtms-content-sha256',
  'x-cloudtms-signature',
  'x-correlation-id',
  'idempotency-key'
]);

function bytesToHex(bytes) {
  return Array.from(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes))
    .map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function hexToBytes(value) {
  const source = String(value || '');
  if (!LOWER_HEX_64_RE.test(source)) return null;
  const output = new Uint8Array(source.length / 2);
  for (let index = 0; index < output.length; index += 1) {
    output[index] = Number.parseInt(source.slice(index * 2, index * 2 + 2), 16);
  }
  return output;
}

function constantTimeHexEqual(left, right) {
  const leftBytes = hexToBytes(left);
  const rightBytes = hexToBytes(right);
  if (!leftBytes || !rightBytes || leftBytes.byteLength !== rightBytes.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < leftBytes.byteLength; index += 1) {
    difference |= leftBytes[index] ^ rightBytes[index];
  }
  return difference === 0;
}

function concatBytes(left, right) {
  const output = new Uint8Array(left.byteLength + right.byteLength);
  output.set(left, 0);
  output.set(right, left.byteLength);
  return output;
}

function decodeComponent(raw, encodingError, rejectSeparators = false) {
  const bytes = [];
  for (let index = 0; index < raw.length;) {
    if (raw[index] === '%') {
      if (index + 2 >= raw.length || !/^[0-9A-Fa-f]{2}$/.test(raw.slice(index + 1, index + 3))) {
        throw new Error(encodingError);
      }
      const byte = Number.parseInt(raw.slice(index + 1, index + 3), 16);
      if (rejectSeparators && [0x2f, 0x5c, 0x3f, 0x23].includes(byte)) {
        throw new Error('PATH_SEPARATOR_ENCODED');
      }
      bytes.push(byte);
      index += 3;
      continue;
    }
    bytes.push(...encoder.encode(raw[index]));
    index += 1;
  }
  const buffer = new Uint8Array(bytes);
  let decoded;
  try {
    decoded = fatalDecoder.decode(buffer);
  } catch {
    throw new Error(encodingError);
  }
  return { buffer, text: decoded };
}

function rejectControls(text, errorCode) {
  for (const character of text) {
    const code = character.codePointAt(0);
    if (code < 0x20 || code === 0x7f) throw new Error(errorCode);
  }
}

function percentEncode(bytes) {
  let output = '';
  for (const byte of bytes) {
    const character = String.fromCharCode(byte);
    output += UNRESERVED_RE.test(character)
      ? character
      : `%${byte.toString(16).toUpperCase().padStart(2, '0')}`;
  }
  return output;
}

function asciiCompare(left, right) {
  const maximum = Math.min(left.length, right.length);
  for (let index = 0; index < maximum; index += 1) {
    const difference = left.charCodeAt(index) - right.charCodeAt(index);
    if (difference) return difference;
  }
  return left.length - right.length;
}

export function normalizeCandidateDailyQuery(raw) {
  if (raw === '') return '';
  const pairs = [];
  for (const item of raw.split('&')) {
    if (!item.includes('=') || item.includes('+') || item.includes(';')) throw new Error('QUERY_INVALID');
    const separator = item.indexOf('=');
    const nameRaw = item.slice(0, separator);
    const valueRaw = item.slice(separator + 1);
    if (!nameRaw) throw new Error('QUERY_INVALID');
    const name = decodeComponent(nameRaw, 'QUERY_ENCODING_INVALID');
    const value = decodeComponent(valueRaw, 'QUERY_ENCODING_INVALID');
    rejectControls(name.text, 'QUERY_CONTROL_INVALID');
    rejectControls(value.text, 'QUERY_CONTROL_INVALID');
    pairs.push([percentEncode(name.buffer), percentEncode(value.buffer)]);
  }
  pairs.sort((left, right) => asciiCompare(left[0], right[0]) || asciiCompare(left[1], right[1]));
  return pairs.map(([name, value]) => `${name}=${value}`).join('&');
}

export function parseCandidateDailyRawTarget(rawTarget) {
  if (!rawTarget.startsWith('/') || rawTarget.includes('#') || rawTarget.includes('\\')) {
    throw new Error('PATH_NORMALIZATION_INVALID');
  }
  if ((rawTarget.match(/\?/g) || []).length > 1) throw new Error('PATH_NORMALIZATION_INVALID');
  const separator = rawTarget.indexOf('?');
  const rawPath = separator < 0 ? rawTarget : rawTarget.slice(0, separator);
  const rawQuery = separator < 0 ? '' : rawTarget.slice(separator + 1);
  if (rawPath.includes('//')) throw new Error('PATH_NORMALIZATION_INVALID');
  const segments = rawPath.split('/').map((segment) => {
    const decoded = decodeComponent(segment, 'PATH_ENCODING_INVALID', true);
    rejectControls(decoded.text, 'PATH_CONTROL_INVALID');
    if (decoded.text === '.' || decoded.text === '..') throw new Error('PATH_NORMALIZATION_INVALID');
    return percentEncode(decoded.buffer);
  });
  const normalizedPath = segments.join('/');
  if (!normalizedPath.startsWith('/')) throw new Error('PATH_NORMALIZATION_INVALID');
  return {
    normalizedPath,
    normalizedQuery: separator < 0 ? '' : normalizeCandidateDailyQuery(rawQuery)
  };
}

export function validateCandidateDailyRawHeaders(headerPairs) {
  const seen = new Map();
  for (const pair of headerPairs) {
    if (!Array.isArray(pair) || pair.length !== 2) throw new Error('HEADER_FORMAT_INVALID');
    const [name, value] = pair;
    if (!HEADER_NAME_RE.test(name) || value !== value.trim()) throw new Error('HEADER_FORMAT_INVALID');
    rejectControls(value, 'HEADER_FORMAT_INVALID');
    const lower = name.toLowerCase();
    if (seen.has(lower)) throw new Error('AMBIGUOUS_HEADER');
    seen.set(lower, value);
  }
  if (seen.has('transfer-encoding')) throw new Error('TRANSFER_AMBIGUOUS');
  if (!seen.has('content-length') || !/^(?:0|[1-9][0-9]*)$/.test(seen.get('content-length'))) {
    throw new Error('HEADER_FORMAT_INVALID');
  }
  return seen;
}

function canonicalPrefix({ method, normalizedPath, normalizedQuery, timestamp, nonce, contentSha256, idempotencyKey, correlationId }) {
  return encoder.encode(
    `CLOUDTMS-HMAC-V1\n${method}\n${normalizedPath}\n${normalizedQuery}\n`
    + `${timestamp}\n${nonce}\n${contentSha256}\n${idempotencyKey}\n${correlationId}\n\n`
  );
}

export function candidateDailySignedMessageBytes(fields, rawBody) {
  return concatBytes(canonicalPrefix(fields), rawBody);
}

function safeHeader(request, name) {
  const value = request.headers.get(name);
  if (value == null) return '';
  if (value !== value.trim()) throw new Error('HEADER_FORMAT_INVALID');
  rejectControls(value, 'HEADER_FORMAT_INVALID');
  return value;
}

function validateRequestFraming(request, bodyLength) {
  if (request.headers.has('transfer-encoding')) throw new Error('TRANSFER_AMBIGUOUS');
  const contentLength = safeHeader(request, 'content-length');
  if (!/^(?:0|[1-9][0-9]*)$/.test(contentLength) || Number(contentLength) !== bodyLength) {
    throw new Error('HEADER_FORMAT_INVALID');
  }
  const contentEncoding = safeHeader(request, 'content-encoding');
  if (contentEncoding) throw new Error('BODY_ENCODING_INVALID');
  const contentType = safeHeader(request, 'content-type').toLowerCase();
  if (!/^application\/json(?:;[ \t]*charset=utf-8)?$/.test(contentType)) {
    throw new Error('HEADER_FORMAT_INVALID');
  }
}

function configuredKeys(env) {
  const candidates = [
    {
      id: String(env.CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID || '').trim(),
      secret: String(env.CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_SECRET || '')
    },
    {
      id: String(env.CANDIDATE_DAILY_GOOGLE_HMAC_OVERLAP_KEY_ID || '').trim(),
      secret: String(env.CANDIDATE_DAILY_GOOGLE_HMAC_OVERLAP_SECRET || '')
    }
  ];
  for (const entry of candidates) {
    if (Boolean(entry.id) !== Boolean(entry.secret)) throw new Error('DEPENDENCY_UNAVAILABLE');
    if (entry.id && !KEY_ID_RE.test(entry.id)) throw new Error('DEPENDENCY_UNAVAILABLE');
  }
  const configured = candidates.filter((entry) => entry.id);
  if (!configured.length || new Set(configured.map((entry) => entry.id)).size !== configured.length) {
    throw new Error('DEPENDENCY_UNAVAILABLE');
  }
  return configured;
}

async function importHmacKey(secret) {
  return crypto.subtle.importKey('raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
}

async function consumeNonce(
  env,
  fields,
  requestMessageHash,
  nonceStore = env.R2,
  consumedAtSeconds = Math.floor(Date.now() / 1000)
) {
  if (!nonceStore || typeof nonceStore.put !== 'function') throw new Error('DEPENDENCY_UNAVAILABLE');
  const environment = String(env.CANDIDATE_APP_ENVIRONMENT || '').trim().toLowerCase();
  if (!['test', 'live'].includes(environment)) throw new Error('DEPENDENCY_UNAVAILABLE');
  const key = `candidate-daily-google-nonces/v1/${environment}/${fields.keyId}/${fields.timestamp}/${fields.nonce}`;
  try {
    const result = await nonceStore.put(key, new Uint8Array(), {
      onlyIf: { etagDoesNotMatch: '*' },
      customMetadata: {
        purpose: 'candidate-daily-google-hmac-v1',
        signed_request_epoch: fields.timestamp,
        consumed_epoch: String(consumedAtSeconds),
        expires_epoch: String(consumedAtSeconds + NONCE_RETENTION_SECONDS),
        request_message_sha256: requestMessageHash
      }
    });
    return Boolean(result);
  } catch {
    throw new Error('DEPENDENCY_UNAVAILABLE');
  }
}

function validationFailure(correlationId = '') {
  return { ok: false, status: 400, errorCode: 'VALIDATION_FAILED', retryClass: 'DO_NOT_RETRY', correlationId };
}

function authenticationFailure(correlationId = '') {
  return { ok: false, status: 401, errorCode: 'SYSTEM_AUTH_FAILED', retryClass: 'DO_NOT_RETRY', correlationId };
}

function dependencyFailure(correlationId = '') {
  return { ok: false, status: 503, errorCode: 'DEPENDENCY_UNAVAILABLE', retryClass: 'RETRY_AFTER', correlationId };
}

export async function verifyCandidateDailySystemRequest(request, env, options = {}) {
  let correlationId = '';
  try {
    const url = new URL(request.url);
    const parsedTarget = parseCandidateDailyRawTarget(`${url.pathname}${url.search}`);
    if (!isCandidateDailySystemPath(parsedTarget.normalizedPath)) return validationFailure();
    const route = findCandidateDailyRoute(request.method, parsedTarget.normalizedPath);
    if (!route?.signedSystem) return validationFailure();
    if (request.method !== request.method.toUpperCase()) return validationFailure();
    const body = new Uint8Array(await request.clone().arrayBuffer());
    if (body.byteLength > route.maxBodyBytes) return validationFailure();
    validateRequestFraming(request, body.byteLength);
    if (body.byteLength >= 3 && body[0] === 0xef && body[1] === 0xbb && body[2] === 0xbf) {
      return validationFailure();
    }

    const keyId = safeHeader(request, 'x-cloudtms-key-id');
    const signatureVersion = safeHeader(request, 'x-cloudtms-signature-version');
    const timestamp = safeHeader(request, 'x-cloudtms-timestamp');
    const nonce = safeHeader(request, 'x-cloudtms-nonce');
    const suppliedContentSha256 = safeHeader(request, 'x-cloudtms-content-sha256');
    const suppliedSignature = safeHeader(request, 'x-cloudtms-signature');
    correlationId = safeHeader(request, 'x-correlation-id');
    if (!KEY_ID_RE.test(keyId) || signatureVersion !== CANDIDATE_DAILY_HMAC_VERSION
        || !TIMESTAMP_RE.test(timestamp) || !NONCE_RE.test(nonce)
        || !LOWER_HEX_64_RE.test(suppliedContentSha256)
        || !LOWER_HEX_64_RE.test(suppliedSignature) || !isValidCorrelationId(correlationId)) {
      return validationFailure(correlationId);
    }

    const idempotency = validateDailyIdempotency(route, request);
    if (!idempotency.ok) return validationFailure(correlationId);

    let keys;
    try {
      keys = configuredKeys(env);
    } catch {
      return dependencyFailure(correlationId);
    }
    const selectedKey = keys.find((entry) => entry.id === keyId);
    if (!selectedKey) return authenticationFailure(correlationId);
    const nowSeconds = Number(options.nowSeconds ?? Math.floor(Date.now() / 1000));
    if (Math.abs(nowSeconds - Number(timestamp)) > MAX_CLOCK_SKEW_SECONDS) {
      return authenticationFailure(correlationId);
    }
    const actualContentSha256 = await sha256Hex(body);
    if (!constantTimeHexEqual(actualContentSha256, suppliedContentSha256)) {
      return authenticationFailure(correlationId);
    }
    const fields = {
      method: request.method,
      normalizedPath: parsedTarget.normalizedPath,
      normalizedQuery: parsedTarget.normalizedQuery,
      timestamp,
      nonce,
      contentSha256: actualContentSha256,
      idempotencyKey: idempotency.idempotencyKey,
      correlationId,
      keyId
    };
    const message = candidateDailySignedMessageBytes(fields, body);
    const signature = hexToBytes(suppliedSignature);
    if (!signature || !await crypto.subtle.verify(
      'HMAC', await importHmacKey(selectedKey.secret), signature, message
    )) return authenticationFailure(correlationId);
    const messageHash = await sha256Hex(message);
    let nonceConsumed;
    try {
      nonceConsumed = await consumeNonce(env, fields, messageHash, options.nonceStore, nowSeconds);
    } catch {
      return dependencyFailure(correlationId);
    }
    if (!nonceConsumed) return authenticationFailure(correlationId);
    let parsedBody;
    try {
      parsedBody = JSON.parse(fatalDecoder.decode(body));
      if (!parsedBody || typeof parsedBody !== 'object' || Array.isArray(parsedBody)) throw new Error('object required');
    } catch {
      return validationFailure(correlationId);
    }
    if (parsedTarget.normalizedQuery) return validationFailure(correlationId);
    const bodyIdempotency = validateDailyIdempotency(route, request, parsedBody);
    if (!bodyIdempotency.ok) return validationFailure(correlationId);
    return {
      ok: true,
      route,
      body: parsedBody,
      rawBody: body,
      correlationId,
      idempotencyKey: idempotency.idempotencyKey,
      keyId,
      nonce,
      timestamp,
      requestMessageSha256: messageHash
    };
  } catch (error) {
    if (error?.message === 'DEPENDENCY_UNAVAILABLE') return dependencyFailure(correlationId);
    return validationFailure(correlationId);
  }
}

export async function purgeCandidateDailySystemNonces(env, nowSeconds = Math.floor(Date.now() / 1000)) {
  if (!env.R2?.list || !env.R2?.delete) return;
  let cursor;
  do {
    const page = await env.R2.list({
      prefix: 'candidate-daily-google-nonces/v1/', cursor, limit: 1000, include: ['customMetadata']
    });
    const expired = (page.objects || []).filter((object) => {
      const uploadedEpoch = object.uploaded instanceof Date
        ? Math.floor(object.uploaded.getTime() / 1000)
        : Number(object.customMetadata?.consumed_epoch);
      return Number.isSafeInteger(uploadedEpoch)
        && uploadedEpoch <= nowSeconds - NONCE_RETENTION_SECONDS;
    }).map((object) => object.key);
    if (expired.length) await env.R2.delete(expired);
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
}

export const candidateDailyHmacInternals = Object.freeze({
  SECURITY_HEADERS,
  MAX_CLOCK_SKEW_SECONDS,
  NONCE_RETENTION_SECONDS,
  asciiCompare,
  canonicalPrefix,
  configuredKeys,
  constantTimeHexEqual,
  consumeNonce,
  hexToBytes
});
