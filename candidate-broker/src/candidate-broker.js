import { signCandidatePrivateRequest } from '../../broker/src/candidate-service-auth.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const PUBLIC_CANDIDATE_PREFIX = '/candidate-app/v1';
const PUBLIC_MANAGER_PREFIX = '/candidate-manager/v1';
const PRIVATE_CANDIDATE_PREFIX = '/private/candidate-app/v1';
const PRIVATE_MANAGER_PREFIX = '/private/candidate-manager/v1';
const MANAGER_ACTION_METHODS = Object.freeze({
  start: 'GET',
  progress: 'POST',
  approve: 'POST',
  refuse: 'POST'
});
const MAX_PUBLIC_JSON_BYTES = 1024 * 1024;
const MAX_PUBLIC_UPLOAD_BYTES = 15 * 1024 * 1024;
const PUBLIC_ERROR_BYTES = 64 * 1024;
const ENUMERATION_SAFE_MINIMUM_MS = 250;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

class CandidateBrokerError extends Error {
  constructor(status, code) {
    super(code);
    this.status = status;
    this.code = code;
  }
}

function text(value) {
  return String(value == null ? '' : value).trim();
}

function upper(value) {
  return text(value).toUpperCase();
}

function isObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function environmentName(env) {
  const value = upper(env.CANDIDATE_APP_ENVIRONMENT);
  if (!['TEST', 'LIVE'].includes(value)) throw new CandidateBrokerError(503, 'CANDIDATE_ENVIRONMENT_UNAVAILABLE');
  return value;
}

function requestId(request) {
  const supplied = text(request.headers.get('x-request-id'));
  return supplied && supplied.length <= 120 ? supplied : crypto.randomUUID();
}

function jsonResponse(status, body, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
      ...headers
    }
  });
}

function errorResponse(error, id) {
  const status = error instanceof CandidateBrokerError ? error.status : 500;
  const code = error instanceof CandidateBrokerError ? error.code : 'CANDIDATE_BROKER_REQUEST_FAILED';
  return jsonResponse(status, { ok: false, error_code: code, request_id: id });
}

function allowedOrigins(env) {
  const values = text(env.CANDIDATE_ALLOWED_ORIGINS).split(',').map((value) => value.trim()).filter(Boolean);
  if (!values.length || values.some((value) => value === '*')) {
    throw new CandidateBrokerError(503, 'CANDIDATE_ORIGIN_POLICY_UNAVAILABLE');
  }
  for (const value of values) {
    try {
      const parsed = new URL(value);
      if (parsed.protocol !== 'https:' || parsed.origin !== value || parsed.username || parsed.password) {
        throw new Error('invalid origin');
      }
    } catch {
      throw new CandidateBrokerError(503, 'CANDIDATE_ORIGIN_POLICY_UNAVAILABLE');
    }
  }
  return new Set(values);
}

function requestOriginContext(request, env, managerRoute) {
  const origin = text(request.headers.get('origin'));
  if (origin) {
    if (!allowedOrigins(env).has(origin)) throw new CandidateBrokerError(403, 'CANDIDATE_ORIGIN_NOT_ALLOWED');
    return { origin, client: 'browser' };
  }
  if (managerRoute) throw new CandidateBrokerError(403, 'MANAGER_BROWSER_ORIGIN_REQUIRED');
  const client = text(request.headers.get('x-cloudtms-client')).toLowerCase();
  if (upper(env.CANDIDATE_ALLOW_NATIVE_CLIENTS) !== 'TRUE' || !['ios', 'android'].includes(client)) {
    throw new CandidateBrokerError(403, 'CANDIDATE_CLIENT_ORIGIN_REQUIRED');
  }
  return { origin: '', client };
}

function withCors(response, origin) {
  const headers = new Headers(response.headers);
  headers.set('cache-control', 'no-store');
  headers.set('x-content-type-options', 'nosniff');
  headers.set('referrer-policy', 'no-referrer');
  headers.set('x-frame-options', 'DENY');
  if (origin) {
    headers.set('access-control-allow-origin', origin);
    headers.append('vary', 'Origin');
  }
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function preflight(request, env) {
  const origin = text(request.headers.get('origin'));
  if (!origin || !allowedOrigins(env).has(origin)) throw new CandidateBrokerError(403, 'CANDIDATE_ORIGIN_NOT_ALLOWED');
  const method = upper(request.headers.get('access-control-request-method'));
  const allowedMethods = new Set(['GET', 'POST', 'PATCH', 'PUT', 'OPTIONS']);
  if (!allowedMethods.has(method)) throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
  const allowedHeaders = new Set([
    'authorization', 'content-type', 'idempotency-key', 'x-request-id', 'x-cloudtms-client',
    'x-candidate-session-token', 'x-candidate-device-id'
  ]);
  const requestedHeaders = text(request.headers.get('access-control-request-headers'))
    .toLowerCase().split(',').map((value) => value.trim()).filter(Boolean);
  if (requestedHeaders.some((header) => !allowedHeaders.has(header))) {
    throw new CandidateBrokerError(403, 'CANDIDATE_HEADER_NOT_ALLOWED');
  }
  return new Response(null, {
    status: 204,
    headers: {
      'access-control-allow-origin': origin,
      'access-control-allow-methods': Array.from(allowedMethods).join(', '),
      'access-control-allow-headers': Array.from(allowedHeaders).join(', '),
      'access-control-max-age': '600',
      'cache-control': 'no-store',
      vary: 'Origin'
    }
  });
}

function bytesToHex(bytes) {
  return Array.from(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes))
    .map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function hexToBytes(value) {
  const source = text(value).toLowerCase();
  if (!/^[0-9a-f]+$/.test(source) || source.length % 2 !== 0) return null;
  const output = new Uint8Array(source.length / 2);
  for (let index = 0; index < output.length; index += 1) {
    output[index] = Number.parseInt(source.slice(index * 2, index * 2 + 2), 16);
  }
  return output;
}

function base64UrlEncode(bytes) {
  let binary = '';
  for (const byte of bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function base64UrlDecode(value) {
  const source = text(value).replace(/-/g, '+').replace(/_/g, '/');
  const binary = atob(source + '='.repeat((4 - source.length % 4) % 4));
  const output = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) output[index] = binary.charCodeAt(index);
  return output;
}

async function sha256Bytes(value) {
  const bytes = value instanceof Uint8Array
    ? value
    : value instanceof ArrayBuffer
      ? new Uint8Array(value)
      : encoder.encode(String(value == null ? '' : value));
  return new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
}

async function sha256Hex(value) {
  return bytesToHex(await sha256Bytes(value));
}

async function envelopeKey(secret, purpose) {
  if (!text(secret)) throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_TOKEN_SECRET_UNAVAILABLE');
  return crypto.subtle.importKey(
    'raw', await sha256Bytes(`${purpose}:${secret}`), { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']
  );
}

async function sealEnvelope(secret, purpose, payload) {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv, additionalData: encoder.encode(purpose) },
    await envelopeKey(secret, purpose),
    encoder.encode(JSON.stringify(payload))
  ));
  return `v1.${base64UrlEncode(iv)}.${base64UrlEncode(ciphertext)}`;
}

async function openEnvelope(secret, purpose, token) {
  const [version, ivValue, ciphertextValue] = text(token).split('.');
  if (version !== 'v1' || !ivValue || !ciphertextValue) return null;
  try {
    const plaintext = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv: base64UrlDecode(ivValue), additionalData: encoder.encode(purpose) },
      await envelopeKey(secret, purpose),
      base64UrlDecode(ciphertextValue)
    );
    const payload = JSON.parse(decoder.decode(plaintext));
    return isObject(payload) ? payload : null;
  } catch {
    return null;
  }
}

function bearerToken(request) {
  const match = /^Bearer\s+(.+)$/i.exec(text(request.headers.get('authorization')));
  return match ? text(match[1]) : '';
}

async function openPublicAccess(request, env) {
  return openPublicAccessToken(bearerToken(request), env);
}

async function openPublicAccessToken(token, env) {
  const payload = await openEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    token
  );
  const now = Math.floor(Date.now() / 1000);
  if (!payload || payload.typ !== 'candidate_broker_access' || payload.aud !== 'cloudtms-candidate-public'
      || payload.env !== environmentName(env) || Number(payload.exp) <= now
      || !text(payload.internal_access_token) || !text(payload.public_session_id)) {
    throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
  }
  return payload;
}

async function openPublicRefresh(body, env) {
  const payload = await openEnvelope(
    env.CANDIDATE_BROKER_REFRESH_TOKEN_SECRET,
    'candidate-broker-refresh-v1',
    text(body.refresh_token)
  );
  const now = Math.floor(Date.now() / 1000);
  if (!payload || payload.typ !== 'candidate_broker_refresh' || payload.aud !== 'cloudtms-candidate-refresh'
      || payload.env !== environmentName(env) || Number(payload.exp) <= now
      || !text(payload.internal_refresh_token) || !text(payload.internal_session_id)
      || !text(payload.public_session_id)
      || (body.session_id && text(body.session_id) !== payload.public_session_id)) {
    throw new CandidateBrokerError(401, 'CANDIDATE_SESSION_INVALID');
  }
  return payload;
}

async function boundedJson(request, maximumBytes = MAX_PUBLIC_JSON_BYTES) {
  const declared = Number(request.headers.get('content-length') || 0);
  if (declared > maximumBytes) throw new CandidateBrokerError(413, 'REQUEST_BODY_TOO_LARGE');
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > maximumBytes) throw new CandidateBrokerError(413, 'REQUEST_BODY_TOO_LARGE');
  if (!bytes.byteLength) return {};
  try {
    const value = JSON.parse(decoder.decode(bytes));
    if (!isObject(value)) throw new Error('object required');
    return value;
  } catch {
    throw new CandidateBrokerError(400, 'INVALID_JSON');
  }
}

async function boundedBodyBytes(request, maximumBytes) {
  const declared = Number(request.headers.get('content-length') || 0);
  if (declared > maximumBytes) throw new CandidateBrokerError(413, 'REQUEST_BODY_TOO_LARGE');
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > maximumBytes) throw new CandidateBrokerError(413, 'REQUEST_BODY_TOO_LARGE');
  return bytes;
}

function privatePath(publicPath) {
  if (publicPath.startsWith(PUBLIC_CANDIDATE_PREFIX)) {
    return `${PRIVATE_CANDIDATE_PREFIX}${publicPath.slice(PUBLIC_CANDIDATE_PREFIX.length)}`;
  }
  if (publicPath.startsWith(PUBLIC_MANAGER_PREFIX)) {
    return `${PRIVATE_MANAGER_PREFIX}${publicPath.slice(PUBLIC_MANAGER_PREFIX.length)}`;
  }
  throw new CandidateBrokerError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
}

function enforceManagerMethod(path, method) {
  const actionMatch = /^\/candidate-manager\/v1\/workflows\/[0-9a-f-]+\/(start|progress|approve|refuse)$/i.exec(path);
  if (actionMatch) {
    const expected = MANAGER_ACTION_METHODS[actionMatch[1].toLowerCase()];
    if (method !== expected) throw new CandidateBrokerError(405, 'METHOD_NOT_ALLOWED');
    return;
  }
  const documentMatch = /^\/candidate-manager\/v1\/workflows\/[0-9a-f-]+\/components\/[0-9a-f-]+\/document$/i.test(path);
  const signatureMatch = /^\/candidate-manager\/v1\/workflows\/[0-9a-f-]+\/signature\/prepare$/i.test(path);
  if ((documentMatch && method !== 'GET') || (signatureMatch && method !== 'POST')) {
    throw new CandidateBrokerError(405, 'METHOD_NOT_ALLOWED');
  }
}

function privateRequestHeaders(request, authorization) {
  const headers = new Headers();
  for (const name of ['content-type', 'idempotency-key', 'x-request-id']) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  if (authorization) headers.set('authorization', authorization);
  headers.set('x-cloudtms-public-client', text(request.headers.get('x-cloudtms-client')) || 'browser');
  return headers;
}

async function forwardPrivate(request, env, { authorization = '', body = undefined } = {}) {
  if (!env.CLOUDTMS_PRIVATE || typeof env.CLOUDTMS_PRIVATE.fetch !== 'function') {
    throw new CandidateBrokerError(503, 'CANDIDATE_PRIVATE_API_UNAVAILABLE');
  }
  const path = privatePath(new URL(request.url).pathname);
  const url = new URL(request.url);
  url.protocol = 'https:';
  url.hostname = 'cloudtms-candidate-private.internal';
  url.port = '';
  url.pathname = path;
  const hasBody = !['GET', 'HEAD'].includes(request.method);
  const maximumBytes = path.includes('/uploads/') ? MAX_PUBLIC_UPLOAD_BYTES : MAX_PUBLIC_JSON_BYTES;
  const bodyValue = body === undefined
    ? (hasBody ? await boundedBodyBytes(request, maximumBytes) : undefined)
    : JSON.stringify(body);
  const headers = privateRequestHeaders(request, authorization);
  if (body !== undefined) headers.set('content-type', 'application/json');
  const unsigned = new Request(url.toString(), {
    method: request.method,
    headers,
    body: bodyValue,
    redirect: 'manual'
  });
  return env.CLOUDTMS_PRIVATE.fetch(await signCandidatePrivateRequest(unsigned, env));
}

async function responseJson(response, maximumBytes = MAX_PUBLIC_JSON_BYTES) {
  const declared = Number(response.headers.get('content-length') || 0);
  if (declared > maximumBytes) throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_RESPONSE_INVALID');
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > maximumBytes) throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_RESPONSE_INVALID');
  try {
    const value = JSON.parse(decoder.decode(bytes));
    if (!isObject(value)) throw new Error('object required');
    return value;
  } catch {
    throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_RESPONSE_INVALID');
  }
}

async function publicSafePrivateResponse(response) {
  if (response.status < 400) {
    const headers = new Headers();
    for (const name of ['content-type', 'content-length', 'content-disposition', 'cache-control']) {
      const value = response.headers.get(name);
      if (value) headers.set(name, value);
    }
    return new Response(response.body, { status: response.status, headers });
  }
  const declared = Number(response.headers.get('content-length') || 0);
  if (declared > PUBLIC_ERROR_BYTES) return jsonResponse(502, { ok: false, error_code: 'CANDIDATE_PRIVATE_RESPONSE_INVALID' });
  let source = {};
  try {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength <= PUBLIC_ERROR_BYTES) source = JSON.parse(decoder.decode(bytes));
  } catch {
    source = {};
  }
  const status = response.status >= 500 ? 502 : response.status;
  return jsonResponse(status, {
    ok: false,
    error_code: response.status >= 500
      ? 'CANDIDATE_PRIVATE_API_UNAVAILABLE'
      : text(source.error_code) || 'CANDIDATE_REQUEST_FAILED',
    request_id: text(source.request_id) || undefined
  });
}

async function wrapPrivateSession(response, env, publicSessionId = crypto.randomUUID()) {
  if (!response.ok) return publicSafePrivateResponse(response);
  const source = await responseJson(response);
  const now = Math.floor(Date.now() / 1000);
  const accessSeconds = Number(source.access_expires_in_seconds || 900);
  const absoluteExpiry = Date.parse(source.absolute_expires_at_utc || '');
  if (!text(source.access_token) || !text(source.refresh_token) || !text(source.session_id)
      || !Number.isFinite(accessSeconds) || accessSeconds < 60
      || !Number.isFinite(absoluteExpiry) || absoluteExpiry <= Date.now()) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_SESSION_INVALID');
  }
  const accessToken = await sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: environmentName(env),
      public_session_id: publicSessionId, internal_access_token: source.access_token,
      iat: now, exp: now + accessSeconds
    }
  );
  const refreshToken = await sealEnvelope(
    env.CANDIDATE_BROKER_REFRESH_TOKEN_SECRET,
    'candidate-broker-refresh-v1',
    {
      typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: environmentName(env),
      public_session_id: publicSessionId, internal_session_id: source.session_id,
      internal_refresh_token: source.refresh_token,
      iat: now, exp: Math.floor(absoluteExpiry / 1000)
    }
  );
  return jsonResponse(response.status, {
    ...source,
    access_token: accessToken,
    refresh_token: refreshToken,
    session_id: publicSessionId
  });
}

async function wrapSelectedCandidateAccess(response, env, existingAccess) {
  if (!response.ok) return publicSafePrivateResponse(response);
  const source = await responseJson(response);
  if (!text(source.access_token)) throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_SESSION_INVALID');
  const now = Math.floor(Date.now() / 1000);
  const seconds = Number(source.access_expires_in_seconds || 900);
  const accessToken = await sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: environmentName(env),
      public_session_id: existingAccess.public_session_id,
      internal_access_token: source.access_token,
      iat: now, exp: now + seconds
    }
  );
  const safe = { ...source, access_token: accessToken, session_id: existingAccess.public_session_id };
  delete safe.internal_session_id;
  delete safe.refresh_token;
  return jsonResponse(response.status, safe);
}

async function wrapPhoneHandoff(response, env, access, request) {
  if (!response.ok) return publicSafePrivateResponse(response);
  const source = await responseJson(response);
  const internalToken = text(source.manager_handoff_token);
  if (!internalToken || !text(source.workflow_id) || !text(source.approval_request_id)) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PHONE_HANDOFF_INVALID');
  }
  const now = Math.floor(Date.now() / 1000);
  const expiresAt = Date.parse(source.expires_at_utc || '');
  if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PHONE_HANDOFF_INVALID');
  }
  const deviceId = text(request.headers.get('x-candidate-device-id'));
  const token = await sealEnvelope(
    env.CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET,
    'candidate-broker-phone-handoff-v1',
    {
      typ: 'candidate_phone_handoff', aud: 'cloudtms-manager-phone', env: environmentName(env),
      workflow_id: source.workflow_id, approval_request_id: source.approval_request_id,
      public_session_id: access.public_session_id,
      device_id_sha256: deviceId ? await sha256Hex(deviceId) : null,
      internal_manager_token: internalToken,
      iat: now, exp: Math.floor(expiresAt / 1000)
    }
  );
  const safe = { ...source, manager_handoff_token: token };
  return jsonResponse(response.status, safe);
}

async function managerAuthorization(request, env) {
  const supplied = bearerToken(request);
  const handoff = await openEnvelope(
    env.CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET,
    'candidate-broker-phone-handoff-v1',
    supplied
  );
  if (!handoff) return `Bearer ${supplied}`;
  const now = Math.floor(Date.now() / 1000);
  if (handoff.typ !== 'candidate_phone_handoff' || handoff.aud !== 'cloudtms-manager-phone'
      || handoff.env !== environmentName(env) || Number(handoff.exp) <= now
      || !text(handoff.internal_manager_token) || !text(handoff.public_session_id)) {
    throw new CandidateBrokerError(401, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
  }
  const access = await openPublicAccessToken(text(request.headers.get('x-candidate-session-token')), env);
  if (access.public_session_id !== handoff.public_session_id) {
    throw new CandidateBrokerError(401, 'MANAGER_PHONE_HANDOFF_SESSION_MISMATCH');
  }
  const expectedDevice = text(handoff.device_id_sha256);
  const deviceId = text(request.headers.get('x-candidate-device-id'));
  if (expectedDevice && (!deviceId || await sha256Hex(deviceId) !== expectedDevice)) {
    throw new CandidateBrokerError(401, 'MANAGER_PHONE_HANDOFF_DEVICE_MISMATCH');
  }
  return `Bearer ${handoff.internal_manager_token}`;
}

async function encryptDeviceToken(env, value) {
  const token = text(value);
  if (!token || token.length > 8192) throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  const purpose = 'candidate-broker-device-token-v1';
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = new Uint8Array(await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv, additionalData: encoder.encode(purpose) },
    await envelopeKey(env.CANDIDATE_BROKER_DEVICE_TOKEN_SECRET, purpose),
    encoder.encode(token)
  ));
  const packed = new Uint8Array(iv.length + encrypted.length);
  packed.set(iv, 0);
  packed.set(encrypted, iv.length);
  return bytesToHex(packed);
}

async function decryptDeviceToken(env, ciphertextHex) {
  const packed = hexToBytes(ciphertextHex);
  if (!packed || packed.length <= 28) throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  try {
    const plaintext = await crypto.subtle.decrypt(
      {
        name: 'AES-GCM', iv: packed.slice(0, 12),
        additionalData: encoder.encode('candidate-broker-device-token-v1')
      },
      await envelopeKey(
        env.CANDIDATE_BROKER_DEVICE_TOKEN_SECRET,
        'candidate-broker-device-token-v1'
      ),
      packed.slice(12)
    );
    return decoder.decode(plaintext);
  } catch {
    throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  }
}

async function applyRateLimit(env, bindingName, keys) {
  const binding = env[bindingName];
  if (!binding || typeof binding.limit !== 'function') {
    throw new CandidateBrokerError(503, 'CANDIDATE_RATE_LIMIT_UNAVAILABLE');
  }
  for (const key of keys) {
    const result = await binding.limit({ key: await sha256Hex(`${environmentName(env)}:${bindingName}:${key}`) });
    if (!result?.success) throw new CandidateBrokerError(429, 'CANDIDATE_RATE_LIMITED');
  }
}

async function rateLimitRequest(request, env, path, managerRoute) {
  const ip = text(request.headers.get('cf-connecting-ip')) || 'unknown-ip';
  await applyRateLimit(env, 'CANDIDATE_GENERAL_RATE_LIMIT', [`ip:${ip}`, `path:${ip}:${path}`]);
  if (path.includes('/auth/')) {
    let subject = 'unknown-subject';
    try {
      const body = await boundedJson(request.clone());
      subject = text(body.email || body.session_id || body.refresh_token).toLowerCase() || subject;
    } catch {
      // The authoritative request parser will return the stable validation error.
    }
    await applyRateLimit(env, 'CANDIDATE_AUTH_RATE_LIMIT', [`ip:${ip}`, `subject:${subject}`]);
  } else if (managerRoute) {
    await applyRateLimit(env, 'CANDIDATE_MANAGER_RATE_LIMIT', [`ip:${ip}`, `token:${bearerToken(request)}`]);
  } else if (path.includes('/uploads/') || path.endsWith('/components/prepare')) {
    await applyRateLimit(env, 'CANDIDATE_UPLOAD_RATE_LIMIT', [`ip:${ip}`, `token:${bearerToken(request)}`]);
  }
}

function isSessionCreationPath(path) {
  return path === `${PUBLIC_CANDIDATE_PREFIX}/auth/login`
    || path === `${PUBLIC_CANDIDATE_PREFIX}/auth/password/complete`;
}

function isChallengeStartPath(path) {
  return path === `${PUBLIC_CANDIDATE_PREFIX}/auth/challenge/start`
    || path === `${PUBLIC_CANDIDATE_PREFIX}/auth/challenge/resend`;
}

async function enumerationSafeChallenge(request, env, path) {
  const started = Date.now();
  const body = await boundedJson(request.clone());
  const email = text(body.email).toLowerCase();
  const purpose = upper(body.purpose || 'ACTIVATE');
  if (!EMAIL_RE.test(email) || !['ACTIVATE', 'RESET', 'RECOVERY'].includes(purpose)
      || (path.endsWith('/resend') && !UUID_RE.test(text(body.challenge_id)))) {
    throw new CandidateBrokerError(400, 'CANDIDATE_CHALLENGE_REQUEST_INVALID');
  }
  const response = await forwardPrivate(request, env, { body: { ...body, email, purpose } });
  if (response.status >= 500 || response.status === 429) return publicSafePrivateResponse(response);
  const remaining = ENUMERATION_SAFE_MINIMUM_MS - (Date.now() - started);
  if (remaining > 0) await new Promise(resolve => setTimeout(resolve, remaining));
  return jsonResponse(202, { ok: true, accepted: true });
}

function isPublicAuthPath(path) {
  return path.startsWith(`${PUBLIC_CANDIDATE_PREFIX}/auth/`);
}

export async function handleCandidateBrokerRequest(request, env, ctx = {}) {
  const id = requestId(request);
  const url = new URL(request.url);
  const path = url.pathname;
  if (path === '/healthz') {
    try {
      return jsonResponse(200, { ok: true, service: 'candidate-broker', environment: environmentName(env) });
    } catch (error) {
      return errorResponse(error, id);
    }
  }
  if (path === '/readyz') {
    try {
      const probe = new Request('https://cloudtms-candidate-private.internal/private/candidate-app/v1/health', {
        headers: { 'x-request-id': id }
      });
      const response = await env.CLOUDTMS_PRIVATE.fetch(await signCandidatePrivateRequest(probe, env));
      return jsonResponse(response.ok ? 200 : 503, { ok: response.ok, service: 'candidate-broker' });
    } catch {
      return jsonResponse(503, { ok: false, error_code: 'CANDIDATE_PRIVATE_API_UNAVAILABLE' });
    }
  }
  const candidateRoute = path.startsWith(PUBLIC_CANDIDATE_PREFIX);
  const managerRoute = path.startsWith(PUBLIC_MANAGER_PREFIX);
  if (!candidateRoute && !managerRoute) return jsonResponse(404, { ok: false, error_code: 'CANDIDATE_ROUTE_NOT_FOUND', request_id: id });
  let origin = '';
  try {
    if (request.method === 'OPTIONS') return preflight(request, env);
    origin = requestOriginContext(request, env, managerRoute).origin;
    const declared = Number(request.headers.get('content-length') || 0);
    if (Number.isFinite(declared) && declared > (path.includes('/uploads/') ? MAX_PUBLIC_UPLOAD_BYTES : MAX_PUBLIC_JSON_BYTES)) {
      throw new CandidateBrokerError(413, 'REQUEST_BODY_TOO_LARGE');
    }
    await rateLimitRequest(request, env, path, managerRoute);

    if (candidateRoute && isChallengeStartPath(path)) {
      return withCors(await enumerationSafeChallenge(request, env, path), origin);
    }

    if (candidateRoute && path === `${PUBLIC_CANDIDATE_PREFIX}/auth/refresh`) {
      const body = await boundedJson(request.clone());
      const refresh = await openPublicRefresh(body, env);
      const response = await forwardPrivate(request, env, {
        body: {
          ...body,
          refresh_token: refresh.internal_refresh_token,
          session_id: refresh.internal_session_id
        }
      });
      return withCors(await wrapPrivateSession(response, env, refresh.public_session_id), origin);
    }

    if (candidateRoute && isSessionCreationPath(path)) {
      return withCors(await wrapPrivateSession(await forwardPrivate(request, env), env), origin);
    }

    if (candidateRoute && isPublicAuthPath(path)) {
      return withCors(await publicSafePrivateResponse(await forwardPrivate(request, env)), origin);
    }

    if (managerRoute) {
      enforceManagerMethod(path, request.method);
      return withCors(await publicSafePrivateResponse(await forwardPrivate(request, env, {
        authorization: await managerAuthorization(request, env)
      })), origin);
    }

    let access = null;
    let authorization = '';
    try {
      access = await openPublicAccess(request, env);
      authorization = `Bearer ${access.internal_access_token}`;
    } catch (error) {
      if (!path.includes('/uploads/')) throw error;
      authorization = await managerAuthorization(request, env);
    }

    if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/push-token`) {
      const body = await boundedJson(request.clone());
      const provider = upper(body.push_provider);
      if (!['APNS', 'FCM', 'WEB_PUSH'].includes(provider)) {
        throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_PROVIDER_INVALID');
      }
      const response = await forwardPrivate(request, env, {
        authorization,
        body: {
          ...body,
          push_provider: provider,
          push_token_ciphertext_hex: await encryptDeviceToken(env, body.push_token),
          push_key_version: 1,
          push_token: undefined
        }
      });
      return withCors(await publicSafePrivateResponse(response), origin);
    }

    const response = await forwardPrivate(request, env, { authorization });
    if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/select-candidate`) {
      return withCors(await wrapSelectedCandidateAccess(response, env, access), origin);
    }
    if (/\/workflows\/[0-9a-f-]+\/actions\/select-phone-approval$/i.test(path)) {
      return withCors(await wrapPhoneHandoff(response, env, access, request), origin);
    }
    return withCors(await publicSafePrivateResponse(response), origin);
  } catch (error) {
    const response = errorResponse(error, id);
    if (error instanceof CandidateBrokerError && error.status === 429) response.headers.set('retry-after', '60');
    return withCors(response, origin);
  }
}

export const candidateBrokerInternals = Object.freeze({
  allowedOrigins,
  boundedBodyBytes,
  decryptDeviceToken,
  encryptDeviceToken,
  environmentName,
  enforceManagerMethod,
  managerActionMethods: MANAGER_ACTION_METHODS,
  enumerationSafeChallenge,
  openEnvelope,
  openPublicAccess,
  openPublicRefresh,
  privatePath,
  requestOriginContext,
  sealEnvelope,
  sha256Hex,
  wrapPrivateSession
});
