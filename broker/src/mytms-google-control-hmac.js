import { sha256Hex } from './candidate-daily-contract-v1.js';
import {
  candidateDailySignedMessageBytes,
  parseCandidateDailyRawTarget
} from './candidate-daily-hmac-v1.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });
const PREFIX = '/private/google-control/v1';
const KEY_ID_RE = /^[A-Za-z0-9._-]{1,64}$/;
const NONCE_RE = /^[A-Za-z0-9_-]{22,64}$/;
const HEX_RE = /^[a-f0-9]{64}$/;
const TIMESTAMP_RE = /^\d{10}$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_BODY_BYTES = 128 * 1024;
const MAX_SKEW_SECONDS = 300;
const NONCE_RETENTION_SECONDS = 10 * 60;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function routeFor(method, path) {
  const exact = {
    [`POST ${PREFIX}/integrations/heartbeat`]: 'INTEGRATION_HEARTBEAT',
    [`POST ${PREFIX}/candidates/provisioning/preflight`]: 'PROVISIONING_PREFLIGHT',
    [`POST ${PREFIX}/candidates/provisioning/commit`]: 'PROVISIONING_COMMIT',
    [`POST ${PREFIX}/target-switches`]: 'TARGET_SWITCH_PREPARE'
  }[`${method} ${path}`];
  if (exact) return exact;
  if (method === 'GET' && new RegExp(`^${PREFIX}/candidates/provisioning/[0-9a-f-]{36}$`, 'i').test(path)) {
    return 'PROVISIONING_STATUS';
  }
  if (method === 'POST' && new RegExp(`^${PREFIX}/target-switches/[0-9a-f-]{36}/attest$`, 'i').test(path)) {
    return 'TARGET_SWITCH_ATTEST';
  }
  if (method === 'POST' && new RegExp(`^${PREFIX}/target-switches/[0-9a-f-]{36}/(?:COMMIT|ABORT)$`, 'i').test(path)) {
    return 'TARGET_SWITCH_TRANSITION';
  }
  if (method === 'GET' && new RegExp(`^${PREFIX}/target-switches/[0-9a-f-]{36}$`, 'i').test(path)) {
    return 'TARGET_SWITCH_STATUS';
  }
  return null;
}

function configuredKeys(env) {
  const keys = [
    [env.CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID, env.CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_SECRET],
    [env.CANDIDATE_DAILY_GOOGLE_HMAC_OVERLAP_KEY_ID, env.CANDIDATE_DAILY_GOOGLE_HMAC_OVERLAP_SECRET]
  ].map(([id, secret]) => ({ id: text(id), secret: String(secret || '') }));
  if (keys.some(entry => Boolean(entry.id) !== Boolean(entry.secret))) throw new Error('CONFIGURATION');
  const active = keys.filter(entry => entry.id);
  if (!active.length || active.some(entry => !KEY_ID_RE.test(entry.id))
      || new Set(active.map(entry => entry.id)).size !== active.length) throw new Error('CONFIGURATION');
  return active;
}

function safeHeader(request, name) {
  const value = request.headers.get(name);
  if (value == null) return '';
  if (value !== value.trim() || /[\u0000-\u001f\u007f]/.test(value)) throw new Error('FRAMING');
  return value;
}

function hexBytes(value) {
  if (!HEX_RE.test(value)) return null;
  const output = new Uint8Array(32);
  for (let index = 0; index < 32; index += 1) output[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  return output;
}

async function importKey(secret) {
  return crypto.subtle.importKey(
    'raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']
  );
}

function googleContextFromRequest(body, query) {
  const source = body?.google_context && typeof body.google_context === 'object'
    ? body.google_context : Object.fromEntries(query.entries());
  const context = {
    integration_key: text(source.integration_key).toLowerCase(),
    project_identity_hmac: text(source.project_identity_hmac).toLowerCase(),
    principal_fingerprint: text(source.principal_fingerprint).toLowerCase(),
    actor_identity_hmac: text(source.actor_identity_hmac).toLowerCase(),
    source_revision: text(source.source_revision)
  };
  if (!/^[a-z0-9][a-z0-9_-]{2,99}$/.test(context.integration_key)
      || !HEX_RE.test(context.project_identity_hmac)
      || !HEX_RE.test(context.principal_fingerprint)
      || !HEX_RE.test(context.actor_identity_hmac)
      || context.source_revision.length < 1 || context.source_revision.length > 200) {
    throw new Error('CONTEXT');
  }
  if (body?.google_context?.reservation_token) {
    context.reservation_token = text(body.google_context.reservation_token);
  }
  return context;
}

async function consumeNonce(env, keyId, timestamp, nonce, messageHash, nowSeconds) {
  if (!env.R2 || typeof env.R2.put !== 'function') throw new Error('DEPENDENCY');
  const environment = text(env.CANDIDATE_APP_ENVIRONMENT).toLowerCase();
  if (!['test', 'live'].includes(environment)) throw new Error('DEPENDENCY');
  const key = `mytms-google-control-nonces/v1/${environment}/${keyId}/${timestamp}/${nonce}`;
  const result = await env.R2.put(key, new Uint8Array(), {
    onlyIf: { etagDoesNotMatch: '*' },
    customMetadata: {
      purpose: 'mytms-google-control-hmac-v1',
      consumed_epoch: String(nowSeconds),
      expires_epoch: String(nowSeconds + NONCE_RETENTION_SECONDS),
      request_message_sha256: messageHash
    }
  });
  return Boolean(result);
}

function failure(status, errorCode, correlationId = '') {
  return { ok: false, status, errorCode, correlationId };
}

export async function verifyMyTmsGoogleControlRequest(request, env, options = {}) {
  let correlationId = '';
  try {
    const url = new URL(request.url);
    const parsedTarget = parseCandidateDailyRawTarget(`${url.pathname}${url.search}`);
    const route = routeFor(request.method, parsedTarget.normalizedPath);
    if (!route || request.method !== request.method.toUpperCase()) return failure(400, 'VALIDATION_FAILED');
    const bodyBytes = new Uint8Array(await request.clone().arrayBuffer());
    if (bodyBytes.byteLength > MAX_BODY_BYTES || request.headers.has('transfer-encoding')
        || safeHeader(request, 'content-encoding')) return failure(400, 'VALIDATION_FAILED');
    const declared = safeHeader(request, 'content-length');
    const implicitEmptyGet = request.method === 'GET' && declared === '' && bodyBytes.byteLength === 0;
    if (!implicitEmptyGet
        && (!/^\d+$/.test(declared) || Number(declared) !== bodyBytes.byteLength)) {
      return failure(400, 'VALIDATION_FAILED');
    }
    if (request.method === 'POST'
        && !/^application\/json(?:;[ \t]*charset=utf-8)?$/i.test(safeHeader(request, 'content-type'))) {
      return failure(400, 'VALIDATION_FAILED');
    }
    if (request.method === 'GET' && bodyBytes.byteLength !== 0) return failure(400, 'VALIDATION_FAILED');

    const keyId = safeHeader(request, 'x-cloudtms-key-id');
    const version = safeHeader(request, 'x-cloudtms-signature-version');
    const timestamp = safeHeader(request, 'x-cloudtms-timestamp');
    const nonce = safeHeader(request, 'x-cloudtms-nonce');
    const suppliedHash = safeHeader(request, 'x-cloudtms-content-sha256');
    const suppliedSignature = safeHeader(request, 'x-cloudtms-signature');
    correlationId = safeHeader(request, 'x-correlation-id');
    const idempotencyKey = safeHeader(request, 'idempotency-key');
    if (!KEY_ID_RE.test(keyId) || version !== 'v1' || !TIMESTAMP_RE.test(timestamp)
        || !NONCE_RE.test(nonce) || !HEX_RE.test(suppliedHash) || !HEX_RE.test(suppliedSignature)
        || correlationId.length < 16 || correlationId.length > 128
        || (request.method === 'POST' && (idempotencyKey.length < 16 || idempotencyKey.length > 200))
        || (request.method === 'GET' && idempotencyKey)) return failure(400, 'VALIDATION_FAILED', correlationId);

    const keys = configuredKeys(env);
    const selected = keys.find(entry => entry.id === keyId);
    if (!selected) return failure(401, 'SYSTEM_AUTH_FAILED', correlationId);
    const nowSeconds = Number(options.nowSeconds ?? Math.floor(Date.now() / 1000));
    if (Math.abs(nowSeconds - Number(timestamp)) > MAX_SKEW_SECONDS) return failure(401, 'SYSTEM_AUTH_FAILED', correlationId);
    const actualHash = await sha256Hex(bodyBytes);
    if (actualHash !== suppliedHash) return failure(401, 'SYSTEM_AUTH_FAILED', correlationId);
    const fields = {
      method: request.method, normalizedPath: parsedTarget.normalizedPath,
      normalizedQuery: parsedTarget.normalizedQuery, timestamp, nonce,
      contentSha256: actualHash, idempotencyKey, correlationId, keyId
    };
    const message = candidateDailySignedMessageBytes(fields, bodyBytes);
    const signature = hexBytes(suppliedSignature);
    if (!signature || !await crypto.subtle.verify(
      'HMAC', await importKey(selected.secret), signature, message
    )) return failure(401, 'SYSTEM_AUTH_FAILED', correlationId);
    const messageHash = await sha256Hex(message);
    if (!await consumeNonce(env, keyId, timestamp, nonce, messageHash, nowSeconds)) {
      return failure(401, 'SYSTEM_AUTH_FAILED', correlationId);
    }
    let body = {};
    if (request.method === 'POST') {
      try {
        body = JSON.parse(decoder.decode(bodyBytes));
        if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('BODY');
      } catch {
        return failure(400, 'VALIDATION_FAILED', correlationId);
      }
      if (parsedTarget.normalizedQuery) return failure(400, 'VALIDATION_FAILED', correlationId);
    }
    const googleContext = googleContextFromRequest(body, url.searchParams);
    return {
      ok: true, route, path: parsedTarget.normalizedPath, body, googleContext,
      correlationId, idempotencyKey, keyId, requestHash: messageHash
    };
  } catch (error) {
    if (['CONFIGURATION', 'DEPENDENCY'].includes(error?.message)) {
      return failure(503, 'DEPENDENCY_UNAVAILABLE', correlationId);
    }
    return failure(400, 'VALIDATION_FAILED', correlationId);
  }
}

export async function purgeMyTmsGoogleControlNonces(env, nowSeconds = Math.floor(Date.now() / 1000)) {
  if (!env.R2?.list || !env.R2?.delete) return;
  let cursor;
  do {
    const page = await env.R2.list({
      prefix: 'mytms-google-control-nonces/v1/', cursor, limit: 1000, include: ['customMetadata']
    });
    const expired = (page.objects || []).filter(object => {
      const consumed = Number(object.customMetadata?.consumed_epoch);
      return Number.isSafeInteger(consumed) && consumed <= nowSeconds - NONCE_RETENTION_SECONDS;
    }).map(object => object.key);
    if (expired.length) await env.R2.delete(expired);
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
}

export const myTmsGoogleControlHmacInternals = Object.freeze({ googleContextFromRequest, routeFor });
