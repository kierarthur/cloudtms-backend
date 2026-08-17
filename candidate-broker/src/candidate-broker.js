import { signCandidatePrivateRequest } from '../../broker/src/candidate-service-auth.js';
import {
  CANDIDATE_DAILY_BOOTSTRAP_ROUTE,
  boundedBodyLength,
  candidateCorrelationId,
  createCorrelationId,
  dailyErrorResponse,
  findCandidateDailyRoute,
  isCandidateDailyPath,
  isCandidateDailySystemPath,
  isValidCorrelationId,
  readBoundedDailyJson,
  rebuildCandidateDailyErrorBody,
  rebuildCandidateDailySuccessBody,
  validateDailyIdempotency,
  requestWithCandidateCorrelation
} from '../../broker/src/candidate-daily-contract-v1.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const PUBLIC_CANDIDATE_PREFIX = '/candidate-app/v1';
const PUBLIC_MANAGER_PREFIX = '/candidate-manager/v1';
const PRIVATE_CANDIDATE_PREFIX = '/private/candidate-app/v1';
const PRIVATE_MANAGER_PREFIX = '/private/candidate-manager/v1';
const PRIVATE_SYSTEM_PREFIX = '/private/candidate-system/v1';
const UNAUTHENTICATED_PUBLIC_AUTH_PATHS = new Set([
  `${PUBLIC_CANDIDATE_PREFIX}/auth/challenge/start`,
  `${PUBLIC_CANDIDATE_PREFIX}/auth/challenge/resend`,
  `${PUBLIC_CANDIDATE_PREFIX}/auth/challenge/verify`,
  `${PUBLIC_CANDIDATE_PREFIX}/auth/password/complete`,
  `${PUBLIC_CANDIDATE_PREFIX}/auth/login`,
  `${PUBLIC_CANDIDATE_PREFIX}/auth/refresh`
]);
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
const SHA256_RE = /^[0-9a-f]{64}$/i;
const MAX_IDEMPOTENCY_KEY_BYTES = 200;
const PUBLIC_CREDENTIAL_VERSION_CONTRACT = 'CANDIDATE_PUBLIC_CREDENTIAL_VERSIONS_V1';
const PUBLIC_PHONE_BINDING_CONTRACT = 'CANDIDATE_PUBLIC_PHONE_BINDING_V1';
const DEVICE_CIPHERTEXT_MAGIC = new Uint8Array([0x43, 0x54, 0x50]);
const CREDENTIAL_AUTHORITIES = Object.freeze({
  access: Object.freeze({
    secret: 'CANDIDATE_BROKER_ACCESS_TOKEN_SECRET',
    version: 'CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION',
    readers: 'CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS'
  }),
  refresh: Object.freeze({
    secret: 'CANDIDATE_BROKER_REFRESH_TOKEN_SECRET',
    version: 'CANDIDATE_BROKER_REFRESH_TOKEN_KEY_VERSION',
    readers: 'CANDIDATE_BROKER_REFRESH_TOKEN_READ_KEY_VERSIONS'
  }),
  manager: Object.freeze({
    secret: 'CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET',
    version: 'CANDIDATE_BROKER_MANAGER_HANDOFF_KEY_VERSION',
    readers: 'CANDIDATE_BROKER_MANAGER_HANDOFF_READ_KEY_VERSIONS'
  }),
  publicSession: Object.freeze({
    secret: 'CANDIDATE_BROKER_PUBLIC_SESSION_ID_SECRET',
    version: 'CANDIDATE_BROKER_PUBLIC_SESSION_ID_KEY_VERSION',
    readers: 'CANDIDATE_BROKER_PUBLIC_SESSION_ID_READ_KEY_VERSIONS'
  }),
  deviceEncryption: Object.freeze({
    secret: 'CANDIDATE_BROKER_DEVICE_TOKEN_SECRET',
    version: 'CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_KEY_VERSION',
    readers: 'CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_READ_KEY_VERSIONS',
    maximumVersion: 32767
  }),
  deviceIdentity: Object.freeze({
    secret: 'CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_SECRET',
    version: 'CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_KEY_VERSION',
    readers: 'CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_READ_KEY_VERSIONS'
  })
});

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

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (isObject(value)) {
    return `{${Object.keys(value).sort().map((key) => (
      `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(value === undefined ? null : value);
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

async function hmacSha256Bytes(secret, purpose, value) {
  if (!text(secret)) throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_TOKEN_SECRET_UNAVAILABLE');
  const material = await sha256Bytes(`${purpose}:${secret}`);
  const key = await crypto.subtle.importKey(
    'raw', material, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']
  );
  const bytes = value instanceof Uint8Array ? value : encoder.encode(String(value == null ? '' : value));
  return new Uint8Array(await crypto.subtle.sign('HMAC', key, bytes));
}

function configuredKeyVersion(env, authority) {
  const value = Number(env[authority.version] == null ? 1 : env[authority.version]);
  const maximumVersion = Number(authority.maximumVersion || 65535);
  if (!Number.isSafeInteger(value) || value < 1 || value > maximumVersion) {
    throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE');
  }
  return value;
}

function authoritySecretForVersion(env, authority, version) {
  const keyVersion = Number(version);
  const maximumVersion = Number(authority.maximumVersion || 65535);
  if (!Number.isSafeInteger(keyVersion) || keyVersion < 1 || keyVersion > maximumVersion) {
    throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE');
  }
  const versioned = text(env[`${authority.secret}_V${keyVersion}`]);
  if (versioned) return versioned;
  if (keyVersion === 1 && text(env[authority.secret])) return text(env[authority.secret]);
  throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE');
}

function authorityHasAliasedSecrets(env, authority) {
  const effective = new Map();
  const versionOne = text(env[`${authority.secret}_V1`]) || text(env[authority.secret]);
  if (versionOne) effective.set(1, versionOne);
  const prefix = `${authority.secret}_V`;
  for (const name of Object.keys(env || {})) {
    if (!name.startsWith(prefix)) continue;
    const rawVersion = name.slice(prefix.length);
    if (!/^[1-9][0-9]{0,4}$/.test(rawVersion)) continue;
    const version = Number(rawVersion);
    if (version > Number(authority.maximumVersion || 65535)) continue;
    const secret = text(env[name]);
    if (secret) effective.set(version, secret);
  }
  const observed = new Set();
  for (const secret of effective.values()) {
    if (observed.has(secret)) return true;
    observed.add(secret);
  }
  return false;
}

function assertAuthoritySecretSeparation(env, authority) {
  if (authorityHasAliasedSecrets(env, authority)) {
    throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE');
  }
}

function authorityReadVersions(env, authority) {
  const values = new Set([configuredKeyVersion(env, authority)]);
  const maximumVersion = Number(authority.maximumVersion || 65535);
  for (const raw of text(env[authority.readers]).split(',')) {
    if (!raw) continue;
    const parsed = Number(raw);
    if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximumVersion) {
      throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE');
    }
    values.add(parsed);
  }
  return Array.from(values);
}

function currentPublicCredentialVersions(env) {
  return {
    contract_version: PUBLIC_CREDENTIAL_VERSION_CONTRACT,
    access_key_version: configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.access),
    refresh_key_version: configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.refresh),
    public_session_key_version: configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.publicSession)
  };
}

function assertPublicCredentialSecrets(env, versions) {
  assertAuthoritySecretSeparation(env, CREDENTIAL_AUTHORITIES.access);
  assertAuthoritySecretSeparation(env, CREDENTIAL_AUTHORITIES.refresh);
  assertAuthoritySecretSeparation(env, CREDENTIAL_AUTHORITIES.publicSession);
  authoritySecretForVersion(env, CREDENTIAL_AUTHORITIES.access, versions.access_key_version);
  authoritySecretForVersion(env, CREDENTIAL_AUTHORITIES.refresh, versions.refresh_key_version);
  authoritySecretForVersion(
    env, CREDENTIAL_AUTHORITIES.publicSession, versions.public_session_key_version
  );
  return versions;
}

function publicCredentialVersions(value, env) {
  const source = isObject(value) ? value : currentPublicCredentialVersions(env);
  const contract = text(source.contract_version || PUBLIC_CREDENTIAL_VERSION_CONTRACT);
  const output = {
    contract_version: contract,
    access_key_version: Number(source.access_key_version),
    refresh_key_version: Number(source.refresh_key_version),
    public_session_key_version: Number(source.public_session_key_version)
  };
  if (contract !== PUBLIC_CREDENTIAL_VERSION_CONTRACT
      || [output.access_key_version, output.refresh_key_version, output.public_session_key_version]
        .some(version => !Number.isSafeInteger(version) || version < 1 || version > 65535)) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_CREDENTIAL_VERSION_INVALID');
  }
  return output;
}

async function deterministicEnvelopeKey(secret, purpose, identity) {
  const material = await hmacSha256Bytes(
    secret, `candidate-broker-envelope-v2-key:${purpose}`, identity
  );
  return crypto.subtle.importKey('raw', material, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
}

async function sealEnvelope(secret, purpose, payload, keyVersion = null) {
  const plaintext = encoder.encode(canonicalJson(payload));
  const envelopeVersion = keyVersion == null ? 'v2' : 'v3';
  const identityPurpose = `candidate-broker-envelope-${envelopeVersion}-identity:${purpose}`;
  const identity = await hmacSha256Bytes(secret, identityPurpose, plaintext);
  const identityEncoded = base64UrlEncode(identity);
  // V2 derives a separate AES key for every HMAC-identified canonical plaintext.
  // The all-zero IV therefore never encrypts distinct inputs under one key;
  // an exact replay intentionally recomputes the identical authenticated result.
  // V1 random-IV envelopes remain readable in openEnvelope during rollout.
  const iv = new Uint8Array(12);
  const additionalData = encoder.encode(`candidate-broker-envelope-${envelopeVersion}:${purpose}:${identityEncoded}`);
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv, additionalData },
    await deterministicEnvelopeKey(secret, keyVersion == null ? purpose : `${purpose}:v3`, identity),
    plaintext
  ));
  const ciphertextEncoded = base64UrlEncode(ciphertext);
  return keyVersion == null
    ? `v2.${identityEncoded}.${ciphertextEncoded}`
    : `v3.${Number(keyVersion)}.${identityEncoded}.${ciphertextEncoded}`;
}

async function openEnvelope(secret, purpose, token) {
  const parts = text(token).split('.');
  if (parts.length !== 3 && parts.length !== 4) return null;
  const version = parts[0];
  const versioned = version === 'v3';
  if (!['v1', 'v2', 'v3'].includes(version)) return null;
  if ((versioned && parts.length !== 4) || (!versioned && parts.length !== 3)) return null;
  const identityOrIvValue = parts[versioned ? 2 : 1];
  const ciphertextValue = parts[versioned ? 3 : 2];
  if (versioned && (!/^[1-9][0-9]{0,4}$/.test(parts[1]) || Number(parts[1]) > 65535)) return null;
  if (!identityOrIvValue || !ciphertextValue) return null;
  try {
    if (version === 'v1') {
      const plaintext = await crypto.subtle.decrypt(
        { name: 'AES-GCM', iv: base64UrlDecode(identityOrIvValue), additionalData: encoder.encode(purpose) },
        await envelopeKey(secret, purpose),
        base64UrlDecode(ciphertextValue)
      );
      const payload = JSON.parse(decoder.decode(plaintext));
      return isObject(payload) ? payload : null;
    }
    const identity = base64UrlDecode(identityOrIvValue);
    if (identity.length !== 32) return null;
    const envelopeVersion = versioned ? 'v3' : 'v2';
    const plaintext = await crypto.subtle.decrypt(
      {
        name: 'AES-GCM', iv: new Uint8Array(12),
        additionalData: encoder.encode(`candidate-broker-envelope-${envelopeVersion}:${purpose}:${identityOrIvValue}`)
      },
      await deterministicEnvelopeKey(secret, versioned ? `${purpose}:v3` : purpose, identity),
      base64UrlDecode(ciphertextValue)
    );
    const expectedIdentity = await hmacSha256Bytes(
      secret, `candidate-broker-envelope-${envelopeVersion}-identity:${purpose}`, new Uint8Array(plaintext)
    );
    if (base64UrlEncode(expectedIdentity) !== identityOrIvValue) return null;
    const payload = JSON.parse(decoder.decode(plaintext));
    return isObject(payload) ? payload : null;
  } catch {
    return null;
  }
}

async function sealVersionedEnvelope(env, authority, purpose, payload, requestedVersion = null) {
  assertAuthoritySecretSeparation(env, authority);
  const keyVersion = requestedVersion == null
    ? configuredKeyVersion(env, authority) : Number(requestedVersion);
  const secret = authoritySecretForVersion(env, authority, keyVersion);
  const plaintext = encoder.encode(canonicalJson(payload));
  const identity = await hmacSha256Bytes(
    secret, `candidate-broker-envelope-v4-identity:${keyVersion}:${purpose}`, plaintext
  );
  const identityEncoded = base64UrlEncode(identity);
  const iv = new Uint8Array(12);
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
    {
      name: 'AES-GCM', iv,
      additionalData: encoder.encode(
        `candidate-broker-envelope-v4:${keyVersion}:${purpose}:${identityEncoded}`
      )
    },
    await deterministicEnvelopeKey(secret, `${purpose}:v4:key-version:${keyVersion}`, identity),
    plaintext
  ));
  return `v4.${keyVersion}.${identityEncoded}.${base64UrlEncode(ciphertext)}`;
}

async function openVersionedEnvelope(env, authority, purpose, token) {
  if (authorityHasAliasedSecrets(env, authority)) return null;
  const parts = text(token).split('.');
  if (parts[0] === 'v4') {
    if (parts.length !== 4 || !/^[1-9][0-9]{0,4}$/.test(parts[1])) return null;
    const keyVersion = Number(parts[1]);
    if (keyVersion > 65535 || !authorityReadVersions(env, authority).includes(keyVersion)) return null;
    let secret;
    try {
      secret = authoritySecretForVersion(env, authority, keyVersion);
    } catch {
      return null;
    }
    const identityEncoded = parts[2];
    const ciphertextEncoded = parts[3];
    try {
      const identity = base64UrlDecode(identityEncoded);
      if (identity.length !== 32 || !ciphertextEncoded) return null;
      const plaintext = await crypto.subtle.decrypt(
        {
          name: 'AES-GCM', iv: new Uint8Array(12),
          additionalData: encoder.encode(
            `candidate-broker-envelope-v4:${keyVersion}:${purpose}:${identityEncoded}`
          )
        },
        await deterministicEnvelopeKey(
          secret, `${purpose}:v4:key-version:${keyVersion}`, identity
        ),
        base64UrlDecode(ciphertextEncoded)
      );
      const expectedIdentity = await hmacSha256Bytes(
        secret, `candidate-broker-envelope-v4-identity:${keyVersion}:${purpose}`,
        new Uint8Array(plaintext)
      );
      if (base64UrlEncode(expectedIdentity) !== identityEncoded) return null;
      const payload = JSON.parse(decoder.decode(plaintext));
      return isObject(payload)
        ? { payload, key_version: keyVersion, envelope_version: 'v4' }
        : null;
    } catch {
      return null;
    }
  }
  if (parts[0] === 'v3') {
    if (parts.length !== 4 || !/^[1-9][0-9]{0,4}$/.test(parts[1])) return null;
    const keyVersion = Number(parts[1]);
    if (keyVersion > 65535) return null;
    if (!authorityReadVersions(env, authority).includes(keyVersion)) return null;
    let secret;
    try {
      secret = authoritySecretForVersion(env, authority, keyVersion);
    } catch {
      return null;
    }
    const payload = await openEnvelope(secret, purpose, token);
    return payload ? { payload, key_version: keyVersion, envelope_version: 'v3' } : null;
  }
  if (!['v1', 'v2'].includes(parts[0]) || parts.length !== 3) return null;
  for (const keyVersion of authorityReadVersions(env, authority)) {
    let secret;
    try {
      secret = authoritySecretForVersion(env, authority, keyVersion);
    } catch {
      continue;
    }
    const payload = await openEnvelope(secret, purpose, token);
    if (payload) return { payload, key_version: keyVersion, envelope_version: parts[0] };
  }
  return null;
}

function uuidFromBytes(source) {
  const bytes = new Uint8Array(source.slice(0, 16));
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const value = bytesToHex(bytes);
  return `${value.slice(0, 8)}-${value.slice(8, 12)}-${value.slice(12, 16)}-${value.slice(16, 20)}-${value.slice(20)}`;
}

async function publicSessionIdForPrivate(env, internalSessionId, keyVersion = null) {
  if (!UUID_RE.test(text(internalSessionId))) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_SESSION_INVALID');
  }
  const version = keyVersion == null
    ? configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.publicSession) : Number(keyVersion);
  return uuidFromBytes(await hmacSha256Bytes(
    authoritySecretForVersion(env, CREDENTIAL_AUTHORITIES.publicSession, version),
    `candidate-broker-public-session-id-v1:key-${version}`,
    `${environmentName(env)}:${text(internalSessionId).toLowerCase()}`
  ));
}

function bearerToken(request) {
  const match = /^Bearer\s+(.+)$/i.exec(text(request.headers.get('authorization')));
  return match ? text(match[1]) : '';
}

async function openPublicAccess(request, env) {
  return openPublicAccessToken(bearerToken(request), env);
}

async function openPublicAccessToken(token, env) {
  const opened = await openVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.access,
    'candidate-broker-access-v1',
    token
  );
  const payload = opened?.payload;
  const now = Math.floor(Date.now() / 1000);
  if (!payload || payload.typ !== 'candidate_broker_access' || payload.aud !== 'cloudtms-candidate-public'
      || payload.env !== environmentName(env) || Number(payload.exp) <= now
      || !text(payload.internal_access_token) || !text(payload.public_session_id)) {
    throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
  }
  return { ...payload, _broker_access_key_version: opened.key_version };
}

async function openPublicRefresh(body, env) {
  const opened = await openVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.refresh,
    'candidate-broker-refresh-v1',
    text(body.refresh_token)
  );
  const payload = opened?.payload;
  const now = Math.floor(Date.now() / 1000);
  if (!payload || payload.typ !== 'candidate_broker_refresh' || payload.aud !== 'cloudtms-candidate-refresh'
      || payload.env !== environmentName(env) || Number(payload.exp) <= now
      || !text(payload.internal_refresh_token) || !text(payload.internal_session_id)
      || !text(payload.public_session_id)
      || (body.session_id && text(body.session_id) !== payload.public_session_id)) {
    throw new CandidateBrokerError(401, 'CANDIDATE_SESSION_INVALID');
  }
  return { ...payload, _broker_refresh_key_version: opened.key_version };
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
  if (publicPath.startsWith('/candidate-system/v1')) {
    return `${PRIVATE_SYSTEM_PREFIX}${publicPath.slice('/candidate-system/v1'.length)}`;
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
  for (const name of ['content-type', 'idempotency-key', 'x-request-id', 'x-correlation-id']) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  if (authorization) headers.set('authorization', authorization);
  headers.set('x-cloudtms-public-client', text(request.headers.get('x-cloudtms-client')) || 'browser');
  return headers;
}

function systemRequestHeaders(request) {
  const headers = new Headers();
  for (const name of [
    'content-type', 'content-length', 'idempotency-key', 'x-correlation-id',
    'x-cloudtms-key-id', 'x-cloudtms-signature-version', 'x-cloudtms-timestamp',
    'x-cloudtms-nonce', 'x-cloudtms-content-sha256', 'x-cloudtms-signature'
  ]) {
    const value = request.headers.get(name);
    if (value != null) headers.set(name, value);
  }
  headers.set('x-cloudtms-public-client', 'signed-google-system');
  return headers;
}

function candidateDailyPublicSystemKeyIds(env) {
  const values = [
    ...String(env.CANDIDATE_DAILY_GOOGLE_HMAC_ACCEPTED_KEY_IDS || '').split(','),
    env.CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID,
    env.CANDIDATE_DAILY_GOOGLE_HMAC_OVERLAP_KEY_ID
  ].map((value) => text(value)).filter((value) => /^[A-Za-z0-9._-]{1,64}$/.test(value));
  return new Set(values);
}

function candidateDailySystemRateKeys(request, env) {
  const ip = text(request.headers.get('cf-connecting-ip')) || 'unknown-ip';
  const keyId = text(request.headers.get('x-cloudtms-key-id'));
  const keyBucket = candidateDailyPublicSystemKeyIds(env).has(keyId) ? keyId : 'invalid-key';
  return [`preauth-ip:${ip}`, `key:${keyBucket}`];
}

async function forwardPrivateSystem(request, env, routeDefinition = null) {
  if (!env.CLOUDTMS_PRIVATE || typeof env.CLOUDTMS_PRIVATE.fetch !== 'function') {
    throw new CandidateBrokerError(503, 'DEPENDENCY_UNAVAILABLE');
  }
  if (request.headers.has('origin') || request.headers.has('cookie') || request.headers.has('authorization')) {
    throw new CandidateBrokerError(401, 'SYSTEM_AUTH_FAILED');
  }
  if (request.headers.has('transfer-encoding') || request.headers.has('content-encoding')) {
    throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
  }
  const url = new URL(request.url);
  const route = routeDefinition || findCandidateDailyRoute(request.method, url.pathname);
  if (!route?.signedSystem) throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
  const body = await boundedBodyBytes(request, route.maxBodyBytes);
  const declared = request.headers.get('content-length');
  if (declared == null || !/^(?:0|[1-9][0-9]*)$/.test(declared) || Number(declared) !== body.byteLength) {
    throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
  }
  await applyRateLimit(env, 'CANDIDATE_DAILY_SYSTEM_RATE_LIMIT', candidateDailySystemRateKeys(request, env));
  url.protocol = 'https:';
  url.hostname = 'cloudtms-candidate-private.internal';
  url.port = '';
  url.pathname = privatePath(url.pathname);
  const unsigned = new Request(url.toString(), {
    method: request.method,
    headers: systemRequestHeaders(request),
    body,
    redirect: 'manual',
    signal: AbortSignal.timeout(route.deadlineMs)
  });
  return env.CLOUDTMS_PRIVATE.fetch(await signCandidatePrivateRequest(unsigned, env));
}

async function forwardPrivate(request, env, { authorization = '', body = undefined, timeoutMs = null } = {}) {
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
    redirect: 'manual',
    ...(Number.isSafeInteger(timeoutMs) && timeoutMs > 0
      ? { signal: AbortSignal.timeout(timeoutMs) }
      : {})
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
    for (const name of ['content-type', 'content-length', 'content-disposition', 'cache-control', 'x-correlation-id']) {
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
  const headers = {};
  const retryAfter = response.headers.get('retry-after');
  if (retryAfter && /^\d{1,9}$/.test(retryAfter)) headers['retry-after'] = retryAfter;
  const errorCode = response.status >= 500
    ? 'CANDIDATE_PRIVATE_API_UNAVAILABLE'
    : text(source.error_code) || 'CANDIDATE_REQUEST_FAILED';
  const body = {
    ok: false,
    error_code: errorCode,
    request_id: text(source.request_id) || undefined
  };
  if (['CANDIDATE_CHALLENGE_RESEND_TOO_SOON', 'CANDIDATE_CHALLENGE_RESEND_LIMIT'].includes(errorCode)
      && isObject(source.details)) {
    const retryAfterSeconds = Number(source.details.retry_after_seconds || 0);
    body.details = {
      ...(Number.isSafeInteger(retryAfterSeconds) && retryAfterSeconds > 0
        ? { retry_after_seconds: retryAfterSeconds } : {}),
      terminal: source.details.terminal === true
    };
  }
  return jsonResponse(status, body, headers);
}

function dailyDependencyResponse(routeDefinition, correlationId) {
  const candidateRoute = String(routeDefinition?.routeClass || '').startsWith('CANDIDATE_DAILY_');
  return dailyErrorResponse(
    503,
    candidateRoute ? 'CANDIDATE_DAILY_NOT_READY' : 'DEPENDENCY_UNAVAILABLE',
    candidateRoute ? 'STATUS_CHECK' : 'RETRY_AFTER',
    correlationId
  );
}

async function publicSafeDailyResponse(response, correlationId, routeDefinition) {
  const declared = Number(response.headers.get('content-length') || 0);
  if (Number.isFinite(declared) && declared > MAX_PUBLIC_JSON_BYTES) {
    return dailyDependencyResponse(routeDefinition, correlationId);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_PUBLIC_JSON_BYTES) {
    return dailyDependencyResponse(routeDefinition, correlationId);
  }
  let body;
  try {
    body = JSON.parse(decoder.decode(bytes));
  } catch {
    return dailyDependencyResponse(routeDefinition, correlationId);
  }
  if (!isObject(body) || body.correlation_id !== correlationId || response.headers.get('x-correlation-id') !== correlationId) {
    return dailyDependencyResponse(routeDefinition, correlationId);
  }
  const replay = response.headers.get('x-idempotent-replay');
  const success = rebuildCandidateDailySuccessBody(routeDefinition, response.status, body, correlationId);
  if (success) {
    const headers = new Headers({
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-correlation-id': correlationId
    });
    if (replay === 'true' || replay === 'false') headers.set('x-idempotent-replay', replay);
    return new Response(JSON.stringify(success), { status: 200, headers });
  }
  const rebuilt = rebuildCandidateDailyErrorBody(routeDefinition, response.status, body, correlationId);
  if (!rebuilt) return dailyDependencyResponse(routeDefinition, correlationId);
  const safe = dailyErrorResponse(
    response.status, rebuilt.error_code, rebuilt.retry_class, correlationId, rebuilt.details
  );
  if (replay === 'true' || replay === 'false') safe.headers.set('x-idempotent-replay', replay);
  const retryAfter = response.headers.get('retry-after');
  if (retryAfter && /^\d{1,9}$/.test(retryAfter)) safe.headers.set('retry-after', retryAfter);
  return safe;
}

function dailyBrokerError(error, correlationId, { systemRoute = false, routeDefinition = null, bootstrap = false } = {}) {
  const status = error instanceof CandidateBrokerError ? error.status : 500;
  const code = error instanceof CandidateBrokerError ? error.code : 'INTERNAL_ERROR';
  if (status === 429) return dailyErrorResponse(429, 'RATE_LIMITED', 'RETRY_AFTER', correlationId);
  if (status === 400 || status === 405 || status === 413) {
    return dailyErrorResponse(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY', correlationId);
  }
  if (status === 401 && systemRoute) return dailyErrorResponse(401, 'SYSTEM_AUTH_FAILED', 'DO_NOT_RETRY', correlationId);
  if (status === 401) return dailyErrorResponse(401, 'UNAUTHENTICATED', 'REAUTHENTICATE', correlationId);
  if (status === 403) return dailyErrorResponse(403, 'FORBIDDEN', 'DO_NOT_RETRY', correlationId);
  if (code === 'DEPENDENCY_UNAVAILABLE' || code.endsWith('_UNAVAILABLE')) {
    return bootstrap
      ? dailyErrorResponse(503, 'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER', correlationId)
      : dailyDependencyResponse(routeDefinition, correlationId);
  }
  const command = String(routeDefinition?.routeClass || '').endsWith('_COMMAND');
  return dailyErrorResponse(500, 'INTERNAL_ERROR', command ? 'STATUS_CHECK' : 'RETRY_AFTER', correlationId);
}

async function validateCandidateDailyTransport(request, route) {
  if (request.headers.has('transfer-encoding') || request.headers.has('content-encoding')) {
    throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
  }
  if (request.method === 'GET' || request.method === 'HEAD') {
    const declared = boundedBodyLength(request, route.maxBodyBytes);
    if (!declared.ok) {
      throw new CandidateBrokerError(declared.tooLarge ? 413 : 400,
        declared.tooLarge ? 'PAYLOAD_TOO_LARGE' : 'VALIDATION_FAILED');
    }
    if (declared.declared != null && declared.declared !== 0) {
      throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
    }
    if (!validateDailyIdempotency(route, request).ok) {
      throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
    }
    return;
  }
  const contentType = text(request.headers.get('content-type')).toLowerCase();
  if (!/^application\/json(?:;[ \t]*charset=utf-8)?$/.test(contentType)) {
    throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
  }
  const parsed = await readBoundedDailyJson(request.clone(), route.maxBodyBytes);
  if (!parsed.ok) throw new CandidateBrokerError(parsed.status, parsed.errorCode);
  if (!validateDailyIdempotency(route, request, parsed.body).ok) {
    throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
  }
}

async function wrapPrivateSession(response, env, publicSessionId = null) {
  if (!response.ok) return publicSafePrivateResponse(response);
  const source = await responseJson(response);
  const issuedAt = Date.parse(source.issued_at_utc || '');
  const accessSeconds = Number(source.access_expires_in_seconds || 900);
  const sessionExpiry = Date.parse(source.expires_at_utc || '');
  const absoluteExpiry = Date.parse(source.absolute_expires_at_utc || '');
  if (!text(source.access_token) || !text(source.refresh_token) || !text(source.session_id)
      || !Number.isFinite(accessSeconds) || accessSeconds < 60
      || !Number.isFinite(issuedAt)
      || !Number.isFinite(sessionExpiry) || sessionExpiry <= issuedAt
      || !Number.isFinite(absoluteExpiry) || absoluteExpiry < sessionExpiry
      || absoluteExpiry <= Date.now()) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_SESSION_INVALID');
  }
  const versions = publicCredentialVersions(source.public_credential_versions, env);
  assertPublicCredentialSecrets(env, versions);
  const stablePublicSessionId = publicSessionId
    ? text(publicSessionId)
    : await publicSessionIdForPrivate(env, source.session_id, versions.public_session_key_version);
  if (!UUID_RE.test(stablePublicSessionId)) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_SESSION_INVALID');
  }
  const issuedAtSeconds = Math.floor(issuedAt / 1000);
  const accessToken = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.access,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: environmentName(env),
      public_session_id: stablePublicSessionId, internal_access_token: source.access_token,
      public_session_key_version: versions.public_session_key_version,
      iat: issuedAtSeconds, exp: issuedAtSeconds + accessSeconds
    },
    versions.access_key_version
  );
  const refreshToken = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.refresh,
    'candidate-broker-refresh-v1',
    {
      typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: environmentName(env),
      public_session_id: stablePublicSessionId, internal_session_id: source.session_id,
      internal_refresh_token: source.refresh_token,
      public_session_key_version: versions.public_session_key_version,
      iat: issuedAtSeconds, exp: Math.floor(absoluteExpiry / 1000)
    },
    versions.refresh_key_version
  );
  const safe = {
    ...source,
    access_token: accessToken,
    refresh_token: refreshToken,
    session_id: stablePublicSessionId
  };
  delete safe.public_credential_versions;
  return jsonResponse(response.status, safe);
}

async function wrapSelectedCandidateAccess(response, env, existingAccess) {
  if (!response.ok) return publicSafePrivateResponse(response);
  const source = await responseJson(response);
  if (!text(source.access_token)) throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_SESSION_INVALID');
  const issuedAt = Date.parse(source.issued_at_utc || '');
  const seconds = Number(source.access_expires_in_seconds || 900);
  if (!Number.isFinite(issuedAt) || !Number.isFinite(seconds) || seconds < 60) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PRIVATE_SESSION_INVALID');
  }
  const issuedAtSeconds = Math.floor(issuedAt / 1000);
  const accessKeyVersion = Number(existingAccess._broker_access_key_version)
    || configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.access);
  const accessToken = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.access,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: environmentName(env),
      public_session_id: existingAccess.public_session_id,
      internal_access_token: source.access_token,
      public_session_key_version: Number(existingAccess.public_session_key_version) || 1,
      iat: issuedAtSeconds, exp: issuedAtSeconds + seconds
    },
    accessKeyVersion
  );
  const safe = { ...source, access_token: accessToken, session_id: existingAccess.public_session_id };
  delete safe.internal_session_id;
  delete safe.refresh_token;
  return jsonResponse(response.status, safe);
}

async function publicPhoneBinding(env, access, request) {
  const deviceId = text(request.headers.get('x-candidate-device-id'));
  return {
    contract_version: PUBLIC_PHONE_BINDING_CONTRACT,
    public_session_binding_sha256: await sha256Hex(canonicalJson({
      environment: environmentName(env), public_session_id: access.public_session_id
    })),
    device_binding_sha256: deviceId ? await sha256Hex(canonicalJson({
      environment: environmentName(env), device_id: deviceId
    })) : null
  };
}

function samePublicPhoneBinding(left, right) {
  return isObject(left) && isObject(right)
    && left.contract_version === PUBLIC_PHONE_BINDING_CONTRACT
    && right.contract_version === PUBLIC_PHONE_BINDING_CONTRACT
    && text(left.public_session_binding_sha256) === text(right.public_session_binding_sha256)
    && (left.device_binding_sha256 == null ? null : text(left.device_binding_sha256))
      === (right.device_binding_sha256 == null ? null : text(right.device_binding_sha256));
}

async function wrapPhoneHandoff(response, env, access, request, expectedBinding = null) {
  if (!response.ok) return publicSafePrivateResponse(response);
  const source = await responseJson(response);
  const internalToken = text(source.manager_handoff_token);
  if (!internalToken || !text(source.workflow_id) || !text(source.approval_request_id)) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PHONE_HANDOFF_INVALID');
  }
  const issuedAt = Date.parse(source.issued_at_utc || '');
  const expiresAt = Date.parse(source.expires_at_utc || '');
  if (!Number.isFinite(issuedAt) || !Number.isFinite(expiresAt)
      || expiresAt <= issuedAt || expiresAt <= Date.now()) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PHONE_HANDOFF_INVALID');
  }
  const binding = expectedBinding || await publicPhoneBinding(env, access, request);
  if (!samePublicPhoneBinding(source.public_broker_binding, binding)) {
    throw new CandidateBrokerError(502, 'CANDIDATE_PHONE_HANDOFF_BINDING_INVALID');
  }
  const handoffKeyVersion = Number(source.broker_handoff_key_version)
    || configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.manager);
  const token = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.manager,
    'candidate-broker-phone-handoff-v1',
    {
      typ: 'candidate_phone_handoff', aud: 'cloudtms-manager-phone', env: environmentName(env),
      workflow_id: source.workflow_id, approval_request_id: source.approval_request_id,
      public_session_id: access.public_session_id,
      device_id_sha256: binding.device_binding_sha256,
      internal_manager_token: internalToken,
      iat: Math.floor(issuedAt / 1000), exp: Math.floor(expiresAt / 1000)
    },
    handoffKeyVersion
  );
  const safe = { ...source, manager_handoff_token: token };
  delete safe.public_broker_binding;
  delete safe.broker_handoff_key_version;
  return jsonResponse(response.status, safe);
}

async function managerAuthorization(request, env) {
  const supplied = bearerToken(request);
  const opened = await openVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.manager,
    'candidate-broker-phone-handoff-v1',
    supplied
  );
  const handoff = opened?.payload;
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
  const actualDevice = deviceId ? await sha256Hex(canonicalJson({
    environment: environmentName(env), device_id: deviceId
  })) : '';
  if (expectedDevice && (!actualDevice || actualDevice !== expectedDevice)) {
    throw new CandidateBrokerError(401, 'MANAGER_PHONE_HANDOFF_DEVICE_MISMATCH');
  }
  return `Bearer ${handoff.internal_manager_token}`;
}

async function encryptDeviceToken(env, value) {
  const token = text(value);
  if (!token || token.length > 8192) throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  const purpose = 'candidate-broker-device-token-v1';
  const keyVersion = configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.deviceEncryption);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = new Uint8Array(await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv, additionalData: encoder.encode(purpose) },
    await envelopeKey(
      authoritySecretForVersion(env, CREDENTIAL_AUTHORITIES.deviceEncryption, keyVersion), purpose
    ),
    encoder.encode(token)
  ));
  const packed = new Uint8Array(DEVICE_CIPHERTEXT_MAGIC.length + 2 + iv.length + encrypted.length);
  packed.set(DEVICE_CIPHERTEXT_MAGIC, 0);
  packed[3] = (keyVersion >> 8) & 0xff;
  packed[4] = keyVersion & 0xff;
  packed.set(iv, 5);
  packed.set(encrypted, 17);
  return bytesToHex(packed);
}

async function deviceTokenIdentity(
  env, provider, value, publicSessionId = '', requestedVersion = null
) {
  const token = text(value);
  if (!token || token.length > 8192) throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  const keyVersion = requestedVersion == null
    ? configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.deviceIdentity)
    : Number(requestedVersion);
  return bytesToHex(await hmacSha256Bytes(
    authoritySecretForVersion(env, CREDENTIAL_AUTHORITIES.deviceIdentity, keyVersion),
    `candidate-broker-device-token-identity-v1:key-${keyVersion}`,
    canonicalJson({
      environment: environmentName(env), provider: upper(provider),
      public_session_id: text(publicSessionId), token
    })
  ));
}

async function deviceTokenIdentityProofs(env, provider, value, publicSessionId = '') {
  const versions = authorityReadVersions(env, CREDENTIAL_AUTHORITIES.deviceIdentity);
  const proofs = [];
  for (const keyVersion of versions) {
    proofs.push({
      key_version: keyVersion,
      identity_hmac: await deviceTokenIdentity(
        env, provider, value, publicSessionId, keyVersion
      )
    });
  }
  return proofs;
}

async function decryptDeviceToken(env, ciphertextHex, recordedKeyVersion = null) {
  const packed = hexToBytes(ciphertextHex);
  if (!packed || packed.length <= 28) throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  const versioned = packed.length > 33
    && DEVICE_CIPHERTEXT_MAGIC.every((value, index) => packed[index] === value);
  const keyVersion = versioned ? (packed[3] << 8) + packed[4] : 1;
  if (recordedKeyVersion != null && Number(recordedKeyVersion) !== keyVersion) {
    throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  }
  const ivOffset = versioned ? 5 : 0;
  const ciphertextOffset = ivOffset + 12;
  try {
    const plaintext = await crypto.subtle.decrypt(
      {
        name: 'AES-GCM', iv: packed.slice(ivOffset, ciphertextOffset),
        additionalData: encoder.encode('candidate-broker-device-token-v1')
      },
      await envelopeKey(
        authoritySecretForVersion(env, CREDENTIAL_AUTHORITIES.deviceEncryption, keyVersion),
        'candidate-broker-device-token-v1'
      ),
      packed.slice(ciphertextOffset)
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
  const idempotencyKey = text(body.idempotency_key);
  if (!EMAIL_RE.test(email) || !['ACTIVATE', 'RESET', 'RECOVERY'].includes(purpose)
      || (path.endsWith('/resend') && !UUID_RE.test(text(body.challenge_id)))) {
    throw new CandidateBrokerError(400, 'CANDIDATE_CHALLENGE_REQUEST_INVALID');
  }
  if (!idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  const response = await forwardPrivate(request, env, {
    body: { ...body, email, purpose, idempotency_key: idempotencyKey }
  });
  const delay = async () => {
    const remaining = ENUMERATION_SAFE_MINIMUM_MS - (Date.now() - started);
    if (remaining > 0) await new Promise(resolve => setTimeout(resolve, remaining));
  };
  if (response.status >= 500 || response.status === 429) {
    await delay();
    return publicSafePrivateResponse(response);
  }
  if (response.status >= 400) {
    let errorCode = '';
    try {
      errorCode = text((await response.clone().json()).error_code);
    } catch {
      errorCode = '';
    }
    if (['CANDIDATE_IDEMPOTENCY_KEY_REQUIRED', 'CANDIDATE_IDEMPOTENCY_CONFLICT'].includes(errorCode)) {
      await delay();
      return publicSafePrivateResponse(response);
    }
  }
  await delay();
  return jsonResponse(202, { ok: true, accepted: true });
}

function isUnauthenticatedPublicAuthPath(path) {
  return UNAUTHENTICATED_PUBLIC_AUTH_PATHS.has(path);
}

export async function handleCandidateBrokerRequest(request, env, ctx = {}) {
  const id = requestId(request);
  const url = new URL(request.url);
  const path = url.pathname;
  const systemRoute = isCandidateDailySystemPath(path);
  if (systemRoute) {
    const suppliedCorrelationId = text(request.headers.get('x-correlation-id'));
    const correlationId = isValidCorrelationId(suppliedCorrelationId)
      ? suppliedCorrelationId : createCorrelationId();
    const routeDefinition = findCandidateDailyRoute(request.method, path);
    if (!isValidCorrelationId(suppliedCorrelationId) || !routeDefinition?.signedSystem) {
      return dailyErrorResponse(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY', correlationId);
    }
    try {
      return await publicSafeDailyResponse(
        await forwardPrivateSystem(request, env, routeDefinition), correlationId, routeDefinition
      );
    } catch (error) {
      return dailyBrokerError(error, correlationId, { systemRoute: true, routeDefinition });
    }
  }
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
  const dailyCandidateRoute = isCandidateDailyPath(path);
  const dailyBootstrap = path === `${PUBLIC_CANDIDATE_PREFIX}/bootstrap`;
  let dailyCorrelationId = '';
  let dailyRouteDefinition = null;
  try {
    if (dailyCandidateRoute || dailyBootstrap) {
      dailyCorrelationId = candidateCorrelationId(request);
      request = requestWithCandidateCorrelation(request, dailyCorrelationId);
      if (dailyBootstrap) dailyRouteDefinition = CANDIDATE_DAILY_BOOTSTRAP_ROUTE;
    }
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
      const credentialVersions = currentPublicCredentialVersions(env);
      credentialVersions.public_session_key_version = Number(
        refresh.public_session_key_version || credentialVersions.public_session_key_version
      );
      assertPublicCredentialSecrets(env, credentialVersions);
      const response = await forwardPrivate(request, env, {
        body: {
          ...body,
          refresh_token: refresh.internal_refresh_token,
          session_id: refresh.internal_session_id,
          public_credential_versions: credentialVersions
        }
      });
      return withCors(await wrapPrivateSession(response, env, refresh.public_session_id), origin);
    }

    if (candidateRoute && isSessionCreationPath(path)) {
      const body = await boundedJson(request.clone());
      const credentialVersions = assertPublicCredentialSecrets(
        env, currentPublicCredentialVersions(env)
      );
      return withCors(await wrapPrivateSession(await forwardPrivate(request, env, {
        body: { ...body, public_credential_versions: credentialVersions }
      }), env), origin);
    }

    if (candidateRoute && isUnauthenticatedPublicAuthPath(path)) {
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

    if (dailyCandidateRoute) {
      const route = findCandidateDailyRoute(request.method, path);
      if (!route || route.signedSystem) throw new CandidateBrokerError(400, 'VALIDATION_FAILED');
      dailyRouteDefinition = route;
      await validateCandidateDailyTransport(request, route);
      const bindingName = route.externalEffect
        ? 'CANDIDATE_DAILY_EFFECT_RATE_LIMIT'
        : route.routeClass === 'CANDIDATE_DAILY_COMMAND'
          ? 'CANDIDATE_DAILY_COMMAND_RATE_LIMIT'
          : 'CANDIDATE_DAILY_READ_RATE_LIMIT';
      await applyRateLimit(env, bindingName, [`candidate:${access.public_session_id}`]);
      const response = await forwardPrivate(request, env, {
        authorization,
        timeoutMs: route.deadlineMs
      });
      return withCors(await publicSafeDailyResponse(response, dailyCorrelationId, route), origin);
    }

    if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/push-token`) {
      const body = await boundedJson(request.clone());
      const provider = upper(body.push_provider);
      if (!['APNS', 'FCM', 'WEB_PUSH'].includes(provider)) {
        throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_PROVIDER_INVALID');
      }
      const pushEncryptionKeyVersion = configuredKeyVersion(
        env, CREDENTIAL_AUTHORITIES.deviceEncryption
      );
      const pushIdentityKeyVersion = configuredKeyVersion(
        env, CREDENTIAL_AUTHORITIES.deviceIdentity
      );
      const pushIdentityProofs = await deviceTokenIdentityProofs(
        env, provider, body.push_token, access.public_session_id
      );
      const currentIdentityProof = pushIdentityProofs.find(
        proof => proof.key_version === pushIdentityKeyVersion
      );
      if (!currentIdentityProof) {
        throw new CandidateBrokerError(503, 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE');
      }
      const response = await forwardPrivate(request, env, {
        authorization,
        body: {
          ...body,
          push_provider: provider,
          push_token_ciphertext_hex: await encryptDeviceToken(env, body.push_token),
          push_key_version: pushEncryptionKeyVersion,
          push_token_identity_hmac: currentIdentityProof.identity_hmac,
          push_token_identity_key_version: pushIdentityKeyVersion,
          push_token_identity_proofs: pushIdentityProofs,
          push_token: undefined
        }
      });
      return withCors(await publicSafePrivateResponse(response), origin);
    }

    const phoneAction = /\/workflows\/[0-9a-f-]+\/actions\/select-phone-approval$/i.test(path);
    let phoneBinding = null;
    let response;
    if (phoneAction) {
      const body = await boundedJson(request.clone());
      phoneBinding = await publicPhoneBinding(env, access, request);
      const brokerHandoffKeyVersion = configuredKeyVersion(
        env, CREDENTIAL_AUTHORITIES.manager
      );
      authoritySecretForVersion(env, CREDENTIAL_AUTHORITIES.manager, brokerHandoffKeyVersion);
      response = await forwardPrivate(request, env, {
        authorization,
        body: {
          ...body,
          payload: {
            ...(isObject(body.payload) ? body.payload : {}),
            public_broker_binding: phoneBinding,
            broker_handoff_key_version: brokerHandoffKeyVersion
          }
        }
      });
    } else {
      response = await forwardPrivate(request, env, { authorization });
    }
    if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/select-candidate`) {
      return withCors(await wrapSelectedCandidateAccess(response, env, access), origin);
    }
    if (phoneAction) {
      return withCors(await wrapPhoneHandoff(response, env, access, request, phoneBinding), origin);
    }
    if (dailyBootstrap && !response.ok) {
      return withCors(await publicSafeDailyResponse(
        response, dailyCorrelationId, CANDIDATE_DAILY_BOOTSTRAP_ROUTE
      ), origin);
    }
    return withCors(await publicSafePrivateResponse(response), origin);
  } catch (error) {
    if (dailyCandidateRoute || dailyBootstrap) {
      return withCors(dailyBrokerError(error, dailyCorrelationId, {
        routeDefinition: dailyRouteDefinition,
        bootstrap: dailyBootstrap
      }), origin);
    }
    const response = errorResponse(error, id);
    if (error instanceof CandidateBrokerError && error.status === 429) response.headers.set('retry-after', '60');
    return withCors(response, origin);
  }
}

export const candidateBrokerInternals = Object.freeze({
  allowedOrigins,
  assertPublicCredentialSecrets,
  authorityReadVersions,
  authoritySecretForVersion,
  boundedBodyBytes,
  credentialAuthorities: CREDENTIAL_AUTHORITIES,
  currentPublicCredentialVersions,
  decryptDeviceToken,
  deviceTokenIdentity,
  deviceTokenIdentityProofs,
  encryptDeviceToken,
  environmentName,
  enforceManagerMethod,
  managerActionMethods: MANAGER_ACTION_METHODS,
  enumerationSafeChallenge,
  openEnvelope,
  openVersionedEnvelope,
  openPublicAccess,
  openPublicRefresh,
  publicSessionIdForPrivate,
  privatePath,
  requestOriginContext,
  sealEnvelope,
  sealVersionedEnvelope,
  sha256Hex,
  forwardPrivateSystem,
  candidateDailySystemRateKeys,
  publicSafeDailyResponse,
  validateCandidateDailyTransport,
  wrapPrivateSession
});
