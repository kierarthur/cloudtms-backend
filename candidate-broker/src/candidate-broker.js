import { signCandidatePrivateRequest } from '../../broker/src/candidate-service-auth.js';
import { signCandidateRouteContext } from '../../broker/src/candidate-route-context.js';
import {
  candidateDataPlaneRegistryEntry
} from './candidate-data-plane-registry.generated.js';
import { candidateOperationForRequest } from './candidate-operation-policy.js';
import {
  APP_READY_TWO_PLANE_PROOF_PATH,
  handleAppReadyTwoPlaneProof,
  handleManagerEmailTwoPlaneProof,
  MANAGER_EMAIL_TWO_PLANE_PROOF_PATH
} from './app-ready-two-plane-proof.js';
import {
  handleMyTmsGoogleControlRequest,
  isMyTmsGoogleControlPath
} from './mytms-google-control.js';
import {
  CandidateControlPlaneError,
  controlPlaneEnabled,
  controlPlaneRpc,
  globalAuthCutoverEnabled
} from './control-plane-client.js';
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
const GLOBAL_ACCESS_TTL_SECONDS = 15 * 60;
const GLOBAL_REFRESH_TTL_DAYS = 30;
const GLOBAL_REFRESH_ABSOLUTE_TTL_DAYS = 90;
const GLOBAL_CHALLENGE_TTL_MINUTES = 30;
const GLOBAL_VERIFICATION_RECEIPT_TTL_MINUTES = 30;
const AGENCY_CHOICE_TTL_SECONDS = 5 * 60;
const ROUTE_CONTEXT_TTL_SECONDS = 5 * 60;
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
  }),
  agencyChoice: Object.freeze({
    secret: 'MYTMS_AGENCY_CHOICE_TOKEN_SECRET',
    version: 'MYTMS_AGENCY_CHOICE_TOKEN_KEY_VERSION',
    readers: 'MYTMS_AGENCY_CHOICE_TOKEN_READ_KEY_VERSIONS'
  }),
  globalChallenge: Object.freeze({
    secret: 'MYTMS_GLOBAL_CHALLENGE_TOKEN_SECRET',
    version: 'MYTMS_GLOBAL_CHALLENGE_TOKEN_KEY_VERSION',
    readers: 'MYTMS_GLOBAL_CHALLENGE_TOKEN_READ_KEY_VERSIONS'
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
    headers.set('access-control-expose-headers', 'x-cloudtms-content-sha256, x-request-id');
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
    'authorization', 'cache-control', 'content-type', 'idempotency-key', 'x-request-id', 'x-cloudtms-client',
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

async function managerEmailCredentialHmac(env, credential) {
  const secret = text(env.MYTMS_MANAGER_ROUTE_HMAC_SECRET);
  if (secret.length < 32) {
    throw new CandidateBrokerError(503, 'MANAGER_ROUTE_CONFIGURATION_UNAVAILABLE');
  }
  const key = await crypto.subtle.importKey(
    'raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  return bytesToHex(await crypto.subtle.sign(
    'HMAC', key,
    encoder.encode(`manager-email-credential-v1\u001f${canonicalJson(credential)}`)
  ));
}

function randomOpaqueToken(byteLength = 32) {
  if (!Number.isSafeInteger(byteLength) || byteLength < 16 || byteLength > 128) {
    throw new CandidateBrokerError(500, 'CANDIDATE_RANDOM_TOKEN_INVALID');
  }
  return base64UrlEncode(crypto.getRandomValues(new Uint8Array(byteLength)));
}

async function deterministicControlToken(env, purpose, identity, byteLength = 32) {
  const bytes = await hmacSha256Bytes(
    env.MYTMS_CONTROL_PLANE_TOKEN_DERIVATION_SECRET,
    `mytms-control-plane-token-v1:${purpose}`,
    `${environmentName(env)}:${identity}`
  );
  return base64UrlEncode(bytes.slice(0, byteLength));
}

async function controlPlaneActorIdentityHmac(env, value) {
  return bytesToHex(await hmacSha256Bytes(
    env.MYTMS_CONTROL_PLANE_ACTOR_IDENTITY_SECRET,
    'mytms-control-plane-actor-identity-v1',
    `${environmentName(env)}:${text(value).toLowerCase()}`
  ));
}

async function deriveGlobalPasswordDigest(password, metadata) {
  const secret = String(password == null ? '' : password);
  const parameters = isObject(metadata?.parameters) ? metadata.parameters : {};
  const iterations = Number(parameters.iterations);
  const lengthBytes = Number(parameters.length_bytes);
  const salt = hexToBytes(metadata?.password_salt_hex);
  if (secret.length < 12 || secret.length > 512
      || text(metadata?.credential_scheme) !== 'PBKDF2-HMAC-SHA256'
      || Number(metadata?.scheme_version) !== 1
      || upper(parameters.hash) !== 'SHA-256'
      || iterations !== 100000 || lengthBytes !== 32
      || !salt || salt.length < 16 || salt.length > 128
      || !SHA256_RE.test(text(metadata?.credential_authority_sha256_hex))) {
    throw new CandidateBrokerError(401, 'CANDIDATE_LOGIN_INVALID');
  }
  const imported = await crypto.subtle.importKey(
    'raw', encoder.encode(secret), 'PBKDF2', false, ['deriveBits']
  );
  return bytesToHex(await crypto.subtle.deriveBits(
    { name: 'PBKDF2', hash: 'SHA-256', salt, iterations }, imported, lengthBytes * 8
  ));
}

function globalSessionTimes(now = new Date(), absoluteExpiry = null) {
  const absolute = absoluteExpiry ? new Date(absoluteExpiry) : new Date(
    now.getTime() + GLOBAL_REFRESH_ABSOLUTE_TTL_DAYS * 86400000
  );
  const refreshExpiry = new Date(Math.min(
    now.getTime() + GLOBAL_REFRESH_TTL_DAYS * 86400000,
    absolute.getTime()
  ));
  if (!Number.isFinite(absolute.getTime()) || absolute <= now || refreshExpiry <= now) {
    throw new CandidateBrokerError(401, 'CANDIDATE_SESSION_INVALID');
  }
  return {
    issued_at_utc: now.toISOString(),
    expires_at_utc: refreshExpiry.toISOString(),
    absolute_expires_at_utc: absolute.toISOString()
  };
}

function globalSessionContext(access, env) {
  if (access?.authority !== 'CONTROL_PLANE'
      || !UUID_RE.test(text(access.global_account_id))
      || !UUID_RE.test(text(access.global_session_id))
      || !Number.isSafeInteger(Number(access.session_epoch))
      || Number(access.session_epoch) < 1) {
    throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
  }
  return {
    account_id: text(access.global_account_id).toLowerCase(),
    session_id: text(access.global_session_id).toLowerCase(),
    session_epoch: Number(access.session_epoch),
    actor_identity_hmac: null,
    environment: environmentName(env)
  };
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
  const localAuthority = text(payload?.internal_access_token) && text(payload?.public_session_id);
  const controlPlaneAuthority = payload?.authority === 'CONTROL_PLANE'
    && UUID_RE.test(text(payload.global_account_id))
    && UUID_RE.test(text(payload.global_session_id))
    && UUID_RE.test(text(payload.public_session_id))
    && Number.isSafeInteger(Number(payload.session_epoch)) && Number(payload.session_epoch) > 0
    && Number.isFinite(Date.parse(payload.absolute_expires_at_utc || ''))
    && Date.parse(payload.absolute_expires_at_utc) > Date.now();
  if (!payload || payload.typ !== 'candidate_broker_access' || payload.aud !== 'cloudtms-candidate-public'
      || payload.env !== environmentName(env) || Number(payload.exp) <= now
      || (!localAuthority && !controlPlaneAuthority)) {
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
  const localAuthority = text(payload?.internal_refresh_token) && UUID_RE.test(text(payload?.internal_session_id));
  const controlPlaneAuthority = payload?.authority === 'CONTROL_PLANE'
    && text(payload?.control_plane_refresh_token)
    && UUID_RE.test(text(payload?.global_account_id))
    && UUID_RE.test(text(payload?.global_session_id))
    && UUID_RE.test(text(payload?.public_session_id))
    && Number.isSafeInteger(Number(payload?.session_epoch)) && Number(payload.session_epoch) > 0
    && Number.isSafeInteger(Number(payload?.rotation)) && Number(payload.rotation) >= 0
    && Number.isFinite(Date.parse(payload?.absolute_expires_at_utc || ''))
    && Date.parse(payload.absolute_expires_at_utc) > Date.now();
  if (!payload || payload.typ !== 'candidate_broker_refresh' || payload.aud !== 'cloudtms-candidate-refresh'
      || payload.env !== environmentName(env) || Number(payload.exp) <= now
      || (!localAuthority && !controlPlaneAuthority)
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

function hasExactKeys(value, required, optional = []) {
  if (!isObject(value)) return false;
  const allowed = new Set([...required, ...optional]);
  const keys = Object.keys(value);
  return required.every(name => Object.prototype.hasOwnProperty.call(value, name))
    && keys.every(name => allowed.has(name));
}

function validateAdaptiveBreakEntry(value) {
  if (!isObject(value) || !hasExactKeys(value, ['kind'], [
    'break_start', 'break_end', 'calculated_break_minutes', 'break_minutes', 'no_break'
  ])) throw new CandidateBrokerError(400, 'CANDIDATE_BREAK_ENTRY_INVALID');
  const kind = upper(value.kind);
  const permitted = kind === 'START_END_TIMES'
    ? typeof value.break_start === 'string' && typeof value.break_end === 'string'
      && Number.isSafeInteger(value.calculated_break_minutes)
      && !Object.prototype.hasOwnProperty.call(value, 'break_minutes')
      && !Object.prototype.hasOwnProperty.call(value, 'no_break')
    : kind === 'DURATION_MINUTES'
      ? Number.isSafeInteger(value.break_minutes)
        && !Object.prototype.hasOwnProperty.call(value, 'break_start')
        && !Object.prototype.hasOwnProperty.call(value, 'break_end')
        && !Object.prototype.hasOwnProperty.call(value, 'calculated_break_minutes')
        && !Object.prototype.hasOwnProperty.call(value, 'no_break')
      : kind === 'NO_BREAK'
        ? value.no_break === true && value.break_minutes === 0
          && !Object.prototype.hasOwnProperty.call(value, 'break_start')
          && !Object.prototype.hasOwnProperty.call(value, 'break_end')
          && !Object.prototype.hasOwnProperty.call(value, 'calculated_break_minutes')
        : false;
  if (!permitted) throw new CandidateBrokerError(400, 'CANDIDATE_BREAK_ENTRY_INVALID');
}

function validateCandidateFinalisationBody(path, body) {
  if (!isObject(body)) throw new CandidateBrokerError(400, 'INVALID_JSON');
  if (/^\/candidate-app\/v1\/workflows\/[0-9a-f-]+\/components\/prepare$/i.test(path)) {
    if (upper(body.component_kind) !== 'SIGNED_RETURN') {
      if (Object.prototype.hasOwnProperty.call(body, 'signed_return_proof')) {
        throw new CandidateBrokerError(400, 'CANDIDATE_PAPER_QR_PROOF_FORBIDDEN');
      }
      return body;
    }
    const proof = body.signed_return_proof;
    if (!hasExactKeys(proof, [
      'proof_contract_version', 'paper_return_manifest_sha256',
      'paper_return_page_key', 'detected_qr_count'
    ], ['qr_text']) || proof.proof_contract_version !== 'CANDIDATE_PAPER_RETURN_PROOF_V1'
        || !/^[0-9a-f]{64}$/i.test(text(proof.paper_return_manifest_sha256))
        || text(proof.paper_return_page_key) !== text(body.paper_return_page_key)
        || !Number.isSafeInteger(proof.detected_qr_count)
        || proof.detected_qr_count < 0 || proof.detected_qr_count > 1
        || (proof.detected_qr_count === 1
          ? text(proof.qr_text).length < 20 || text(proof.qr_text).length > 4096
          : Object.prototype.hasOwnProperty.call(proof, 'qr_text'))) {
      throw new CandidateBrokerError(400, 'CANDIDATE_PAPER_QR_PROOF_INVALID');
    }
    return body;
  }
  if (!/^\/candidate-app\/v1\/workflows\/[0-9a-f-]+\/actions\/worker-submit$/i.test(path)) {
    return body;
  }
  const immutable = body.immutable_submission;
  if (!isObject(immutable)) return body;
  if (Object.prototype.hasOwnProperty.call(immutable, 'break_entry_context')) {
    const context = immutable.break_entry_context;
    if (!hasExactKeys(context, ['context_version', 'context_token', 'mode'])
        || context.context_version !== 'CANDIDATE_BREAK_ENTRY_V1'
        || !/^[0-9a-f]{64}$/i.test(text(context.context_token))
        || !['START_END_TIMES', 'DURATION_MINUTES'].includes(upper(context.mode))) {
      throw new CandidateBrokerError(400, 'CANDIDATE_BREAK_ENTRY_CONTEXT_INVALID');
    }
  }
  const owners = [immutable, immutable.hours_submission, immutable.timesheet_patch_json,
    immutable.hours_submission?.timesheet_patch_json].filter(isObject);
  for (const owner of owners) {
    if (Object.prototype.hasOwnProperty.call(owner, 'break_entry')) {
      validateAdaptiveBreakEntry(owner.break_entry);
    }
    for (const key of ['actual_schedule_json', 'schedule_json']) {
      if (Array.isArray(owner[key])) {
        for (const segment of owner[key]) {
          if (isObject(segment) && Object.prototype.hasOwnProperty.call(segment, 'break_entry')) {
            validateAdaptiveBreakEntry(segment.break_entry);
          }
        }
      }
    }
  }
  return body;
}

async function candidateFinalisationTransportBody(request, path) {
  const applies = /^\/candidate-app\/v1\/workflows\/[0-9a-f-]+\/components\/prepare$/i.test(path)
    || /^\/candidate-app\/v1\/workflows\/[0-9a-f-]+\/actions\/worker-submit$/i.test(path);
  if (!applies || request.method !== 'POST') return null;
  return validateCandidateFinalisationBody(path, await boundedJson(request.clone()));
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

async function forwardPrivate(request, env, {
  authorization = '', body = undefined, timeoutMs = null, federated = null
} = {}) {
  const destination = federated?.route?.registryEntry?.binding || env.CLOUDTMS_PRIVATE;
  if (!destination || typeof destination.fetch !== 'function') {
    throw new CandidateBrokerError(503, 'CANDIDATE_PRIVATE_API_UNAVAILABLE');
  }
  const path = privatePath(new URL(request.url).pathname);
  const operation = candidateOperationForRequest(request.method, new URL(request.url).pathname);
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
  if (federated) {
    if (!operation?.data_plane_dispatch_required) {
      throw new CandidateBrokerError(500, 'CANDIDATE_OPERATION_POLICY_INVALID');
    }
    const signedRoute = await routeContextForPrivate(
      federated.access, federated.route, operation.operation_id, env,
      new Date(), federated.authorityKind || 'CANDIDATE_SESSION'
    );
    headers.set('x-cloudtms-route-context', signedRoute.envelope);
    headers.set('x-cloudtms-route-context-sha256', signedRoute.sha256);
    if (federated.projectSession !== false) headers.delete('authorization');
  }
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
  return destination.fetch(await signCandidatePrivateRequest(unsigned, env));
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
    for (const name of [
      'content-type', 'content-length', 'content-disposition', 'cache-control',
      'x-correlation-id', 'x-cloudtms-content-sha256'
    ]) {
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
  if (response.status >= 500) {
    const candidate = text(source.error_code).toUpperCase();
    console.error('[candidate-broker] private request failed', {
      status: response.status,
      error_code: /^[A-Z][A-Z0-9_]{2,100}$/.test(candidate)
        ? candidate : 'CANDIDATE_PRIVATE_API_UNAVAILABLE'
    });
  }
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

async function candidateControlPlaneRpc(env, schema, functionName, args, options = {}) {
  try {
    return await controlPlaneRpc(env, schema, functionName, args, options);
  } catch (error) {
    if (error instanceof CandidateControlPlaneError) {
      throw new CandidateBrokerError(error.status, error.code);
    }
    throw new CandidateBrokerError(503, 'DEPENDENCY_UNAVAILABLE');
  }
}

async function deterministicControlUuid(env, purpose, identity) {
  return uuidFromBytes(await hmacSha256Bytes(
    env.MYTMS_CONTROL_PLANE_TOKEN_DERIVATION_SECRET,
    `mytms-control-plane-uuid-v1:${purpose}`,
    `${environmentName(env)}:${identity}`
  ));
}

function controlPlaneResultError(result, fallback = 'CONTROL_PLANE_REQUEST_REJECTED') {
  if (result?.ok !== false) return null;
  const code = text(result.error_code) || fallback;
  const status = code.includes('THROTTLE') || code.includes('LIMIT') ? 429
    : code.includes('CONFLICT') || code.includes('STALE') ? 409
      : code.includes('INVALID') || code.includes('REUSE') || code.includes('REVOKED') ? 401
        : 400;
  return new CandidateBrokerError(status, code);
}

async function wrapGlobalSession(result, env, refreshToken, route = null) {
  const accountId = text(result?.account_id).toLowerCase();
  const globalSessionId = text(result?.session_id).toLowerCase();
  const issuedAt = Date.parse(result?.issued_at_utc || '');
  const expiresAt = Date.parse(result?.expires_at_utc || '');
  const absoluteExpiresAt = Date.parse(result?.absolute_expires_at_utc || '');
  const rotation = Number(result?.rotation);
  const sessionEpoch = Number(result?.session_epoch);
  if (!UUID_RE.test(accountId) || !UUID_RE.test(globalSessionId)
      || !Number.isFinite(issuedAt) || !Number.isFinite(expiresAt)
      || !Number.isFinite(absoluteExpiresAt) || expiresAt <= issuedAt
      || absoluteExpiresAt < expiresAt || absoluteExpiresAt <= Date.now()
      || !Number.isSafeInteger(rotation) || rotation < 0
      || !Number.isSafeInteger(sessionEpoch) || sessionEpoch < 1
      || !text(refreshToken)) {
    throw new CandidateBrokerError(502, 'CONTROL_PLANE_SESSION_INVALID');
  }
  const versions = assertPublicCredentialSecrets(env, currentPublicCredentialVersions(env));
  const publicSessionId = await publicSessionIdForPrivate(
    env, globalSessionId, versions.public_session_key_version
  );
  const issuedAtSeconds = Math.floor(issuedAt / 1000);
  const common = {
    authority: 'CONTROL_PLANE',
    global_account_id: accountId,
    global_session_id: globalSessionId,
    session_epoch: sessionEpoch,
    absolute_expires_at_utc: new Date(absoluteExpiresAt).toISOString(),
    public_session_id: publicSessionId,
    public_session_key_version: versions.public_session_key_version
  };
  const accessToken = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.access, 'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: environmentName(env),
      ...common, rotation, iat: issuedAtSeconds, exp: issuedAtSeconds + GLOBAL_ACCESS_TTL_SECONDS
    }, versions.access_key_version
  );
  const publicRefreshToken = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.refresh, 'candidate-broker-refresh-v1',
    {
      typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: environmentName(env),
      ...common, rotation, control_plane_refresh_token: refreshToken,
      iat: issuedAtSeconds, exp: Math.floor(absoluteExpiresAt / 1000)
    }, versions.refresh_key_version
  );
  const safe = {
    ok: true,
    access_token: accessToken,
    access_token_type: 'Bearer',
    refresh_token: publicRefreshToken,
    session_id: publicSessionId,
    rotation,
    access_expires_in_seconds: GLOBAL_ACCESS_TTL_SECONDS,
    issued_at_utc: new Date(issuedAt).toISOString(),
    expires_at_utc: new Date(expiresAt).toISOString(),
    absolute_expires_at_utc: new Date(absoluteExpiresAt).toISOString(),
    selection_required: route ? false : Boolean(result.selection_required),
    selected_candidate_id: route ? text(route.local_candidate_id).toLowerCase() : null
  };
  if (route) {
    safe.agency_context = {
      display_name: text(route.agency_display_name),
      environment_label: text(route.environment_label),
      membership_generation: Number(route.membership_generation),
      session_epoch: sessionEpoch
    };
  }
  return jsonResponse(200, safe);
}

async function resolveControlPlaneRoute(access, env, correlationId) {
  const context = globalSessionContext(access, env);
  const result = await candidateControlPlaneRpc(
    env, 'control', 'agency_route_context_resolve_v1',
    { p_global_session_context: context, p_correlation_id: correlationId }
  );
  const resultError = controlPlaneResultError(result, 'AGENCY_CONTEXT_STALE');
  if (resultError) throw resultError;
  if (upper(result.environment_label) !== environmentName(env)
      || text(result.global_account_id).toLowerCase() !== context.account_id
      || text(result.global_session_id).toLowerCase() !== context.session_id
      || Number(result.session_epoch) !== context.session_epoch) {
    throw new CandidateBrokerError(409, 'AGENCY_CONTEXT_STALE');
  }
  const entry = candidateDataPlaneRegistryEntry(result.registry_binding_key, env);
  if (!entry || entry.environment !== environmentName(env)) {
    throw new CandidateBrokerError(503, 'AGENCY_ROUTE_UNAVAILABLE');
  }
  return { ...result, registryEntry: entry };
}

async function resolveManagerEmailRoute(credential, operationId, env) {
  if (!controlPlaneEnabled(env)) {
    throw new CandidateBrokerError(503, 'MANAGER_ROUTE_UNAVAILABLE');
  }
  let result;
  try {
    result = await candidateControlPlaneRpc(
      env, 'control', 'manager_email_route_resolve_v1', {
        p_resolution: {
          environment_label: environmentName(env),
          credential_hmac_hex: await managerEmailCredentialHmac(env, credential),
          credential_key_version: 1,
          operation_id: operationId
        }
      }
    );
  } catch (error) {
    if (['DEPENDENCY_UNAVAILABLE', 'CONTROL_PLANE_DISABLED',
      'CONTROL_PLANE_CONFIGURATION_UNAVAILABLE', 'MANAGER_ROUTE_NOT_CALLABLE'
    ].includes(error?.code)) {
      throw new CandidateBrokerError(503, 'MANAGER_ROUTE_UNAVAILABLE');
    }
    throw new CandidateBrokerError(401, 'MANAGER_SECURE_LINK_INVALID');
  }
  const resultError = controlPlaneResultError(result, 'MANAGER_SECURE_LINK_INVALID');
  if (resultError) throw resultError;
  const entry = candidateDataPlaneRegistryEntry(result.registry_binding_key, env);
  if (result.authority_kind !== 'MANAGER_EMAIL'
      || upper(result.environment_label) !== environmentName(env)
      || !UUID_RE.test(text(result.agency_id))
      || !UUID_RE.test(text(result.data_plane_id))
      || !UUID_RE.test(text(result.route_version_id))
      || !UUID_RE.test(text(result.manager_route_ticket_id))
      || !SHA256_RE.test(text(result.workflow_route_hmac_hex))
      || !SHA256_RE.test(text(result.approval_request_route_hmac_hex))
      || !Number.isSafeInteger(Number(result.route_version)) || Number(result.route_version) < 1
      || !Number.isSafeInteger(Number(result.binding_manifest_generation))
      || Number(result.binding_manifest_generation) < 1
      || !Number.isSafeInteger(Number(result.route_revision)) || Number(result.route_revision) < 1
      || !Number.isSafeInteger(Number(result.request_generation))
      || Number(result.request_generation) < 1
      || !Number.isSafeInteger(Number(result.credential_generation))
      || Number(result.credential_generation) < 1
      || !entry || entry.environment !== environmentName(env)) {
    throw new CandidateBrokerError(503, 'MANAGER_ROUTE_UNAVAILABLE');
  }
  return { ...result, registryEntry: entry };
}

async function routeContextForPrivate(
  access, route, operationId, env, now = new Date(), authorityKind = 'CANDIDATE_SESSION'
) {
  if (authorityKind === 'MANAGER_EMAIL') {
    const routeExpiresAt = Date.parse(text(route.expires_at_utc));
    const expiresAt = new Date(Math.min(
      routeExpiresAt,
      now.getTime() + ROUTE_CONTEXT_TTL_SECONDS * 1000
    ));
    if (!Number.isFinite(routeExpiresAt) || expiresAt <= now) {
      throw new CandidateBrokerError(401, 'MANAGER_SECURE_LINK_INVALID');
    }
    return signCandidateRouteContext({
      v: 2,
      typ: 'cloudtms-route-context-v2',
      aud: 'candidate-private-api',
      authority_kind: 'MANAGER_EMAIL',
      operation_id: operationId,
      environment: environmentName(env),
      agency_id: route.agency_id,
      data_plane_id: route.data_plane_id,
      route_version_id: route.route_version_id,
      route_version: Number(route.route_version),
      binding_manifest_generation: Number(route.binding_manifest_generation),
      manager_route_ticket_id: route.manager_route_ticket_id,
      route_revision: Number(route.route_revision),
      workflow_route_hmac: text(route.workflow_route_hmac_hex).toLowerCase(),
      approval_request_route_hmac: text(route.approval_request_route_hmac_hex).toLowerCase(),
      request_generation: Number(route.request_generation),
      credential_generation: Number(route.credential_generation),
      issued_at_utc: now.toISOString(),
      expires_at_utc: expiresAt.toISOString(),
      nonce: crypto.randomUUID(),
      key_version: route.registryEntry.keyVersion
    }, {
      secret: route.registryEntry.routeContextSecret,
      keyVersion: route.registryEntry.keyVersion,
      nowMilliseconds: now.getTime()
    });
  }
  if (authorityKind === 'MANAGER_PHONE') {
    const expiresAt = new Date(Math.min(
      Number(access.exp) * 1000,
      now.getTime() + ROUTE_CONTEXT_TTL_SECONDS * 1000
    ));
    if (expiresAt <= now) throw new CandidateBrokerError(401, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
    return signCandidateRouteContext({
      v: 2,
      typ: 'cloudtms-route-context-v2',
      aud: 'candidate-private-api',
      authority_kind: 'MANAGER_PHONE',
      operation_id: operationId,
      environment: environmentName(env),
      global_account_id: access.global_account_id,
      global_session_id: access.global_session_id,
      membership_id: route.membership_id,
      membership_generation: Number(route.membership_generation),
      agency_id: route.agency_id,
      agency_candidate_id: route.local_candidate_id,
      data_plane_id: route.data_plane_id,
      route_version: Number(route.route_version),
      session_epoch: Number(route.session_epoch),
      issued_at_utc: now.toISOString(),
      expires_at_utc: expiresAt.toISOString(),
      nonce: crypto.randomUUID(),
      key_version: route.registryEntry.keyVersion
    }, {
      secret: route.registryEntry.routeContextSecret,
      keyVersion: route.registryEntry.keyVersion,
      nowMilliseconds: now.getTime()
    });
  }
  const expiresAt = new Date(Math.min(
    Number(access.exp) * 1000,
    now.getTime() + ROUTE_CONTEXT_TTL_SECONDS * 1000
  ));
  if (expiresAt <= now) throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
  return signCandidateRouteContext({
    v: 1,
    aud: 'candidate-private-api',
    operation_id: operationId,
    environment: environmentName(env),
    global_account_id: access.global_account_id,
    global_session_id: access.global_session_id,
    membership_id: route.membership_id,
    membership_generation: Number(route.membership_generation),
    agency_id: route.agency_id,
    agency_candidate_id: route.local_candidate_id,
    data_plane_id: route.data_plane_id,
    route_version: Number(route.route_version),
    session_epoch: Number(route.session_epoch),
    issued_at_utc: now.toISOString(),
    expires_at_utc: expiresAt.toISOString(),
    key_version: route.registryEntry.keyVersion
  }, {
    secret: route.registryEntry.routeContextSecret,
    keyVersion: route.registryEntry.keyVersion,
    nowMilliseconds: now.getTime()
  });
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

async function wrapPhoneHandoff(
  response, env, access, request, expectedBinding = null, federatedRoute = null
) {
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
      ...(federatedRoute ? { federated_route: {
        global_account_id: access.global_account_id,
        global_session_id: access.global_session_id,
        membership_id: federatedRoute.membership_id,
        membership_generation: Number(federatedRoute.membership_generation),
        agency_id: federatedRoute.agency_id,
        local_candidate_id: federatedRoute.local_candidate_id,
        data_plane_id: federatedRoute.data_plane_id,
        route_version: Number(federatedRoute.route_version),
        session_epoch: Number(federatedRoute.session_epoch),
        registry_binding_key: federatedRoute.registry_binding_key
      } } : {}),
      iat: Math.floor(issuedAt / 1000), exp: Math.floor(expiresAt / 1000)
    },
    handoffKeyVersion
  );
  const safe = { ...source, manager_handoff_token: token };
  delete safe.public_broker_binding;
  delete safe.broker_handoff_key_version;
  return jsonResponse(response.status, safe);
}

async function managerForwardContext(request, env, correlationId) {
  const authorization = await managerAuthorization(request, env);
  const supplied = bearerToken(request);
  const operation = candidateOperationForRequest(request.method, new URL(request.url).pathname);
  if (!operation?.data_plane_dispatch_required) {
    throw new CandidateBrokerError(400, 'CANDIDATE_OPERATION_POLICY_INVALID');
  }
  const opened = await openVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.manager, 'candidate-broker-phone-handoff-v1', supplied
  );
  const handoffRoute = opened?.payload?.federated_route;
  if (!opened?.payload) {
    const route = await resolveManagerEmailRoute(supplied, operation.operation_id, env);
    return {
      authorization,
      federated: { access: null, route, projectSession: false, authorityKind: 'MANAGER_EMAIL' }
    };
  }
  if (!handoffRoute) {
    if (globalAuthCutoverEnabled(env)) {
      throw new CandidateBrokerError(401, 'MANAGER_PHONE_HANDOFF_ROUTE_MISMATCH');
    }
    return { authorization, federated: null };
  }
  if (!globalAuthCutoverEnabled(env)) {
    throw new CandidateBrokerError(401, 'MANAGER_PHONE_HANDOFF_SESSION_MISMATCH');
  }
  const access = await openPublicAccessToken(
    text(request.headers.get('x-candidate-session-token')), env
  );
  const route = await resolveControlPlaneRoute(access, env, correlationId);
  const exact = [
    'global_account_id','global_session_id','membership_id','agency_id',
    'local_candidate_id','data_plane_id','registry_binding_key'
  ].every((name) => text(handoffRoute[name]).toLowerCase() === text(
    name === 'global_account_id' ? access.global_account_id
      : name === 'global_session_id' ? access.global_session_id
        : route[name]
  ).toLowerCase())
    && Number(handoffRoute.membership_generation) === Number(route.membership_generation)
    && Number(handoffRoute.route_version) === Number(route.route_version)
    && Number(handoffRoute.session_epoch) === Number(route.session_epoch);
  if (!exact) throw new CandidateBrokerError(401, 'MANAGER_PHONE_HANDOFF_ROUTE_MISMATCH');
  return {
    authorization,
    federated: { access, route, projectSession: false, authorityKind: 'MANAGER_PHONE' }
  };
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
  return jsonResponse(202, { ok: true, accepted: true, next_step: 'CHECK_EMAIL' });
}

function isUnauthenticatedPublicAuthPath(path) {
  return UNAUTHENTICATED_PUBLIC_AUTH_PATHS.has(path);
}

async function controlPlaneAgenciesInternal(access, env, correlationId) {
  const result = await candidateControlPlaneRpc(
    env, 'control', 'account_agencies_get_v1',
    {
      p_global_session_context: globalSessionContext(access, env),
      p_correlation_id: correlationId
    }
  );
  const resultError = controlPlaneResultError(result, 'AGENCY_CONTEXT_INVALID');
  if (resultError) throw resultError;
  const memberships = Array.isArray(result.memberships_internal) ? result.memberships_internal : [];
  const pendingInvitations = Array.isArray(result.pending_invitations_internal)
    ? result.pending_invitations_internal : [];
  return {
    ...result,
    memberships_internal: memberships,
    pending_invitations_internal: pendingInvitations
  };
}

async function agencyChoiceToken(access, membership, env, now = new Date()) {
  const keyVersion = configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.agencyChoice);
  const expiresAt = new Date(now.getTime() + AGENCY_CHOICE_TTL_SECONDS * 1000);
  const token = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.agencyChoice, 'mytms-agency-choice-v1',
    {
      typ: 'mytms_agency_choice', aud: 'cloudtms-candidate-orchestrator',
      env: environmentName(env), global_account_id: access.global_account_id,
      global_session_id: access.global_session_id, session_epoch: Number(access.session_epoch),
      membership_id: membership.membership_id,
      membership_generation: Number(membership.membership_generation),
      agency_id: membership.agency_id, data_plane_id: membership.data_plane_id,
      route_version_id: membership.route_version_id, route_version: Number(membership.route_version),
      iat: Math.floor(now.getTime() / 1000), exp: Math.floor(expiresAt.getTime() / 1000)
    }, keyVersion
  );
  return { token, expires_at_utc: expiresAt.toISOString() };
}

async function publicAgencyChoices(access, agencyResult, env) {
  const agencies = [];
  for (const membership of agencyResult.memberships_internal) {
    const choice = await agencyChoiceToken(access, membership, env);
    agencies.push({
      agency_choice_token: choice.token,
      display_name: text(membership.display_name),
      environment_label: text(membership.environment_label),
      membership_generation: Number(membership.membership_generation),
      expires_at_utc: choice.expires_at_utc
    });
  }
  const pendingInvitations = [];
  for (const invitation of agencyResult.pending_invitations_internal || []) {
    const now = new Date();
    const configuredExpiry = new Date(now.getTime() + AGENCY_CHOICE_TTL_SECONDS * 1000);
    const invitationExpiry = new Date(text(invitation.expires_at_utc));
    const expiresAt = text(invitation.state) === 'PENDING_ACCEPTANCE'
      && Number.isFinite(invitationExpiry.getTime()) && invitationExpiry < configuredExpiry
      ? invitationExpiry : configuredExpiry;
    const keyVersion = configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.agencyChoice);
    const token = await sealVersionedEnvelope(
      env, CREDENTIAL_AUTHORITIES.agencyChoice, 'mytms-pending-invitation-offer-v1',
      {
        typ: 'mytms_pending_invitation_offer', aud: 'cloudtms-candidate-orchestrator',
        env: environmentName(env), global_account_id: access.global_account_id,
        global_session_id: access.global_session_id, session_epoch: Number(access.session_epoch),
        invitation_id: invitation.invitation_id,
        invitation_generation: Number(invitation.invitation_generation),
        membership_id: invitation.membership_id,
        membership_generation: Number(invitation.membership_generation),
        agency_id: invitation.agency_id, state: text(invitation.state),
        iat: Math.floor(now.getTime() / 1000), exp: Math.floor(expiresAt.getTime() / 1000)
      }, keyVersion
    );
    pendingInvitations.push({
      invitation_offer_token: token,
      display_name: text(invitation.display_name),
      environment_label: text(invitation.environment_label),
      membership_generation: Number(invitation.membership_generation),
      state: text(invitation.state),
      expires_at_utc: expiresAt.toISOString()
    });
  }
  return {
    ok: true,
    context_version: 2,
    selection_required: pendingInvitations.length > 0 || agencies.length !== 1,
    auto_select: pendingInvitations.length === 0 && agencies.length === 1,
    agencies,
    pending_invitations: pendingInvitations
  };
}

async function openPendingInvitationOffer(token, access, env) {
  const opened = await openVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.agencyChoice, 'mytms-pending-invitation-offer-v1', token
  );
  const offer = opened?.payload;
  const now = Math.floor(Date.now() / 1000);
  if (!offer || offer.typ !== 'mytms_pending_invitation_offer'
      || offer.aud !== 'cloudtms-candidate-orchestrator'
      || offer.env !== environmentName(env) || Number(offer.exp) <= now
      || offer.global_account_id !== access.global_account_id
      || offer.global_session_id !== access.global_session_id
      || Number(offer.session_epoch) !== Number(access.session_epoch)
      || !UUID_RE.test(text(offer.invitation_id))
      || !UUID_RE.test(text(offer.membership_id))
      || !UUID_RE.test(text(offer.agency_id))
      || !Number.isSafeInteger(Number(offer.invitation_generation))
      || Number(offer.invitation_generation) < 1
      || !Number.isSafeInteger(Number(offer.membership_generation))
      || Number(offer.membership_generation) < 1
      || !['PENDING_ACCEPTANCE','ACCEPTED_WAITING_ROUTE'].includes(text(offer.state))) {
    throw new CandidateBrokerError(403, 'INVITATION_CONFLICT');
  }
  return offer;
}

async function openAgencyChoice(token, access, env) {
  const opened = await openVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.agencyChoice, 'mytms-agency-choice-v1', token
  );
  const choice = opened?.payload;
  const now = Math.floor(Date.now() / 1000);
  if (!choice || choice.typ !== 'mytms_agency_choice'
      || choice.aud !== 'cloudtms-candidate-orchestrator'
      || choice.env !== environmentName(env) || Number(choice.exp) <= now
      || choice.global_account_id !== access.global_account_id
      || choice.global_session_id !== access.global_session_id
      || Number(choice.session_epoch) !== Number(access.session_epoch)
      || !UUID_RE.test(text(choice.membership_id))
      || !UUID_RE.test(text(choice.data_plane_id))
      || !UUID_RE.test(text(choice.route_version_id))
      || !Number.isSafeInteger(Number(choice.membership_generation))
      || Number(choice.membership_generation) < 1) {
    throw new CandidateBrokerError(403, 'AGENCY_NOT_PERMITTED');
  }
  return choice;
}

async function issueControlPlaneAgencySession(access, choiceToken, idempotencyKey, env, correlationId) {
  if (!UUID_RE.test(text(idempotencyKey))) {
    throw new CandidateBrokerError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  const choice = await openAgencyChoice(choiceToken, access, env);
  const identity = `${access.global_session_id}:${choice.membership_id}:${idempotencyKey}`;
  const now = new Date();
  const times = globalSessionTimes(now, access.absolute_expires_at_utc);
  const refreshToken = await deterministicControlToken(env, 'agency-session-refresh', identity);
  const newSessionId = await deterministicControlUuid(env, 'agency-session', identity);
  const context = {
    ...globalSessionContext(access, env),
    actor_identity_hmac: await controlPlaneActorIdentityHmac(env, access.global_account_id),
    selected_membership_id: choice.membership_id,
    selected_membership_generation: Number(choice.membership_generation),
    selected_data_plane_id: choice.data_plane_id,
    selected_route_version_id: choice.route_version_id,
    new_session_id: newSessionId,
    new_refresh_token_hash_hex: await sha256Hex(refreshToken),
    new_rotation: Number(access.rotation) + 1,
    issued_at_utc: times.issued_at_utc,
    expires_at_utc: times.expires_at_utc,
    choice_token_sha256_hex: await sha256Hex(choiceToken),
    choice_expires_at_utc: new Date(Number(choice.exp) * 1000).toISOString()
  };
  const result = await candidateControlPlaneRpc(
    env, 'control', 'agency_session_issue_v1',
    {
      p_global_session_context: context,
      p_choice_token: choiceToken,
      p_idempotency_key: text(idempotencyKey).toLowerCase(),
      p_correlation_id: correlationId
    }
  );
  const resultError = controlPlaneResultError(result, 'AGENCY_CONTEXT_INVALID');
  if (resultError) throw resultError;
  const route = { ...result };
  const entry = candidateDataPlaneRegistryEntry(route.registry_binding_key, env);
  if (!entry || entry.environment !== environmentName(env)) {
    throw new CandidateBrokerError(503, 'AGENCY_ROUTE_UNAVAILABLE');
  }
  route.registryEntry = entry;
  return {
    result: {
      ...result,
      account_id: access.global_account_id,
      rotation: Number(access.rotation) + 1,
      absolute_expires_at_utc: times.absolute_expires_at_utc,
      selection_required: false
    },
    refreshToken,
    route
  };
}

async function finalizeControlPlaneSession(
  result, refreshToken, requestedCandidateId, replayIdentity, env, correlationId
) {
  const access = {
    authority: 'CONTROL_PLANE', global_account_id: result.account_id,
    global_session_id: result.session_id, session_epoch: Number(result.session_epoch),
    rotation: Number(result.rotation), absolute_expires_at_utc: result.absolute_expires_at_utc,
    exp: Math.floor(Date.parse(result.issued_at_utc) / 1000) + GLOBAL_ACCESS_TTL_SECONDS
  };
  const agencyResult = await controlPlaneAgenciesInternal(access, env, correlationId);
  let selectedMembership = null;
  if (requestedCandidateId) {
    const matches = agencyResult.memberships_internal.filter(
      membership => text(membership.local_candidate_id).toLowerCase() === requestedCandidateId
    );
    if (matches.length !== 1) {
      throw new CandidateBrokerError(403, 'CANDIDATE_SELECTION_NOT_ALLOWED');
    }
    selectedMembership = matches[0];
  } else if (result.invitation_acceptance_internal?.membership_id_internal) {
    selectedMembership = agencyResult.memberships_internal.find(
      membership => text(membership.membership_id).toLowerCase()
        === text(result.invitation_acceptance_internal.membership_id_internal).toLowerCase()
    ) || null;
  } else if (agencyResult.pending_invitations_internal.length === 0
      && agencyResult.memberships_internal.length === 1) {
    selectedMembership = agencyResult.memberships_internal[0];
  }
  if (selectedMembership) {
    const choice = await agencyChoiceToken(access, selectedMembership, env);
    const selectionKey = await deterministicControlUuid(
      env, 'session-auto-selection', replayIdentity
    );
    const selected = await issueControlPlaneAgencySession(
      access, choice.token, selectionKey, env, correlationId
    );
    return wrapGlobalSession(selected.result, env, selected.refreshToken, selected.route);
  }
  return wrapGlobalSession({
    ...result,
    selection_required: agencyResult.pending_invitations_internal.length > 0
      || agencyResult.memberships_internal.length !== 1
  }, env, refreshToken);
}

async function globalPasswordProof(env, password, identity) {
  const keyVersion = Number(env.MYTMS_GLOBAL_PASSWORD_KEY_VERSION || 1);
  if (!Number.isSafeInteger(keyVersion) || keyVersion < 1 || keyVersion > 65535) {
    throw new CandidateBrokerError(503, 'CONTROL_PLANE_CONFIGURATION_UNAVAILABLE');
  }
  const saltBytes = (await hmacSha256Bytes(
    env.MYTMS_CONTROL_PLANE_TOKEN_DERIVATION_SECRET,
    'mytms-global-password-salt-v1',
    `${environmentName(env)}:${identity}`
  )).slice(0, 16);
  const passwordSaltHex = bytesToHex(saltBytes);
  const parameters = { hash: 'SHA-256', iterations: 100000, length_bytes: 32 };
  const passwordDigestHex = await deriveGlobalPasswordDigest(password, {
    credential_scheme: 'PBKDF2-HMAC-SHA256', scheme_version: 1,
    password_salt_hex: passwordSaltHex, parameters,
    credential_authority_sha256_hex: '0'.repeat(64)
  });
  return {
    credential_scheme: 'PBKDF2-HMAC-SHA256', scheme_version: 1,
    password_salt_hex: passwordSaltHex, password_digest_hex: passwordDigestHex,
    parameters, key_version: keyVersion
  };
}

async function deliverControlPlaneChallenge(
  request, result, email, purpose, token, deterministicOutboxKey, env, correlationId
) {
  const binding = env.MYTMS_IDENTITY_DELIVERY;
  if (!binding || typeof binding.fetch !== 'function') {
    throw new CandidateBrokerError(503, 'CANDIDATE_IDENTITY_DELIVERY_UNAVAILABLE');
  }
  const deliveryRequest = new Request(
    'https://cloudtms-identity-delivery.internal/private/mytms-control/v1/auth/challenge-delivery',
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'x-request-id': correlationId,
        'x-forwarded-public-origin': text(request.headers.get('origin'))
      },
      body: JSON.stringify({
        challenge_id: text(result.challenge_id).toLowerCase(), email, purpose, token,
        expires_at_utc: result.expires_at_utc,
        deterministic_outbox_key: deterministicOutboxKey
      })
    }
  );
  const response = await binding.fetch(await signCandidatePrivateRequest(deliveryRequest, env));
  if (![200, 202].includes(response.status)) {
    throw new CandidateBrokerError(503, 'CANDIDATE_IDENTITY_DELIVERY_UNAVAILABLE');
  }
}

async function handleControlPlaneChallenge(request, path, env, correlationId) {
  const started = Date.now();
  const body = await boundedJson(request.clone());
  const email = text(body.email).toLowerCase();
  const invitationToken = text(body.invitation_token);
  const purpose = upper(body.purpose || 'ACTIVATE');
  const idempotencyKey = text(body.idempotency_key);
  const isResend = path.endsWith('/resend');
  const directInvitation = !isResend && invitationToken.length >= 32;
  if ((!directInvitation && !EMAIL_RE.test(email))
      || (directInvitation && (invitationToken.length > 2048 || purpose !== 'ACTIVATE'))
      || !['ACTIVATE', 'RESET', 'RECOVERY'].includes(purpose)
      || (isResend && (!UUID_RE.test(text(body.challenge_id)) || invitationToken))) {
    throw new CandidateBrokerError(400, 'CANDIDATE_CHALLENGE_REQUEST_INVALID');
  }
  if (!idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  const invitationTokenHash = directInvitation ? await sha256Hex(invitationToken) : '';
  const semanticHash = await sha256Hex(canonicalJson({
    email: directInvitation ? null : email,
    invitation_token_sha256: directInvitation ? invitationTokenHash : null,
    purpose, challenge_id: isResend ? text(body.challenge_id).toLowerCase() : null
  }));
  const replayIdentity = `${isResend ? 'resend' : 'start'}:${idempotencyKey}`;
  const challengeId = isResend
    ? text(body.challenge_id).toLowerCase()
    : await deterministicControlUuid(env, 'global-challenge', replayIdentity);
  const now = new Date();
  const expiresAt = new Date(now.getTime() + GLOBAL_CHALLENGE_TTL_MINUTES * 60_000);
  const tokenKeyVersion = configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.globalChallenge);
  const token = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.globalChallenge, 'mytms-global-challenge-v1',
    {
      typ: 'mytms_global_challenge', aud: 'cloudtms-global-identity',
      env: environmentName(env), challenge_id: challengeId,
      email: directInvitation ? null : email, purpose,
      iat: Math.floor(now.getTime() / 1000), exp: Math.floor(expiresAt.getTime() / 1000)
    }, tokenKeyVersion
  );
  const deterministicOutboxKey = `${directInvitation ? 'MYTMS_INVITE_SETUP' : 'MYTMS_AUTH'}_${(
    await sha256Hex(`${environmentName(env)}:${purpose}:${challengeId}:${idempotencyKey}`)
  ).slice(0, 40)}`;
  let result;
  try {
    if (directInvitation) {
      const verificationReceipt = await deterministicControlToken(
        env, 'global-verification-receipt', challengeId
      );
      const receiptExpiresAt = new Date(
        now.getTime() + GLOBAL_VERIFICATION_RECEIPT_TTL_MINUTES * 60_000
      );
      result = await candidateControlPlaneRpc(
        env, 'identity', 'global_invitation_challenge_start_v1',
        {
          p_internal_context: {
            invitation_token_hash_hex: invitationTokenHash,
            challenge_id: challengeId,
            token_hash_hex: await sha256Hex(token), token_key_version: tokenKeyVersion,
            deterministic_outbox_key: deterministicOutboxKey,
            request_semantic_hash_hex: semanticHash,
            verification_receipt_hash_hex: await sha256Hex(verificationReceipt),
            verification_receipt_expires_at_utc: receiptExpiresAt.toISOString(),
            actor_identity_hmac: await controlPlaneActorIdentityHmac(env, invitationTokenHash),
            expires_at_utc: receiptExpiresAt.toISOString()
          },
          p_idempotency_key: await deterministicControlUuid(
            env, 'invitation-challenge-idempotency', idempotencyKey
          ),
          p_correlation_id: correlationId
        }
      );
    } else result = await candidateControlPlaneRpc(
      env, 'identity', isResend ? 'global_challenge_resend_v1' : 'global_challenge_start_v1',
      isResend ? {
        p_internal_context: {
          token_hash_hex: await sha256Hex(token), token_key_version: tokenKeyVersion,
          deterministic_outbox_key: deterministicOutboxKey,
          request_semantic_hash_hex: semanticHash,
          actor_identity_hmac: await controlPlaneActorIdentityHmac(env, email),
          expected_email: email, expected_purpose: purpose,
          expires_at_utc: expiresAt.toISOString(), minimum_interval_seconds: 60,
          maximum_resends: 5
        },
        p_challenge_id: challengeId, p_idempotency_key: idempotencyKey,
        p_correlation_id: correlationId, p_now_utc: now.toISOString()
      } : {
        p_internal_context: {
          challenge_id: challengeId, token_hash_hex: await sha256Hex(token),
          token_key_version: tokenKeyVersion,
          deterministic_outbox_key: deterministicOutboxKey,
          request_semantic_hash_hex: semanticHash,
          actor_identity_hmac: await controlPlaneActorIdentityHmac(env, email),
          expires_at_utc: expiresAt.toISOString()
        },
        p_email: email, p_purpose: purpose,
        p_idempotency_key: await deterministicControlUuid(
          env, 'global-challenge-idempotency', idempotencyKey
        ),
        p_correlation_id: correlationId
      }
    );
  } catch (error) {
    if (error instanceof CandidateBrokerError && error.code === 'IDEMPOTENCY_CONFLICT') {
      throw new CandidateBrokerError(409, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    }
    throw error;
  }
  if (result?.ok === false) {
    const mappings = {
      GLOBAL_CHALLENGE_RESEND_TOO_SOON: 'CANDIDATE_CHALLENGE_RESEND_TOO_SOON',
      GLOBAL_CHALLENGE_RESEND_LIMIT: 'CANDIDATE_CHALLENGE_RESEND_LIMIT'
    };
    const mapped = mappings[text(result.error_code)];
    if (mapped) {
      const retryAfter = Number(result.retry_after_seconds || 0);
      const headers = Number.isSafeInteger(retryAfter) && retryAfter > 0
        ? { 'retry-after': String(retryAfter) } : {};
      return jsonResponse(429, {
        ok: false, error_code: mapped,
        details: {
          ...(Number.isSafeInteger(retryAfter) && retryAfter > 0
            ? { retry_after_seconds: retryAfter } : {}),
          terminal: result.terminal === true
        }
      }, headers);
    }
    throw new CandidateBrokerError(401, 'CANDIDATE_CHALLENGE_INVALID');
  }
  if (directInvitation) {
    if (result?.direct_setup !== true || result?.deliver_email !== false
        || text(result.challenge_id).toLowerCase() !== challengeId
        || !text(result.agency_display_name)) {
      throw new CandidateBrokerError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
    }
    return jsonResponse(202, {
      ok: true, accepted: true, next_step: 'SET_PASSWORD',
      challenge_id: challengeId,
      agency_display_name: text(result.agency_display_name),
      expires_at_utc: text(result.expires_at_utc)
    });
  }
  if (result?.deliver_email === true) {
    if (text(result.challenge_id).toLowerCase() !== challengeId) {
      throw new CandidateBrokerError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
    }
    await deliverControlPlaneChallenge(
      request, result, email, purpose, token, deterministicOutboxKey, env, correlationId
    );
  }
  const remaining = ENUMERATION_SAFE_MINIMUM_MS - (Date.now() - started);
  if (remaining > 0) await new Promise(resolve => setTimeout(resolve, remaining));
  return jsonResponse(202, { ok: true, accepted: true, next_step: 'CHECK_EMAIL' });
}

async function handleControlPlaneChallengeVerify(request, env, correlationId) {
  const body = await boundedJson(request.clone());
  const email = text(body.email).toLowerCase();
  const purpose = upper(body.purpose || 'ACTIVATE');
  const token = text(body.token);
  const idempotencyKey = text(body.idempotency_key);
  if (!EMAIL_RE.test(email) || !['ACTIVATE', 'RESET', 'RECOVERY'].includes(purpose)
      || !token || token.length > 4096
      || !idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_CHALLENGE_REQUEST_INVALID');
  }
  const opened = await openVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.globalChallenge, 'mytms-global-challenge-v1', token
  );
  const challenge = opened?.payload;
  const now = new Date();
  if (!challenge || challenge.typ !== 'mytms_global_challenge'
      || challenge.aud !== 'cloudtms-global-identity'
      || challenge.env !== environmentName(env)
      || challenge.email !== email || challenge.purpose !== purpose
      || !UUID_RE.test(text(challenge.challenge_id))
      || Number(challenge.exp) <= Math.floor(now.getTime() / 1000)
      || (body.challenge_id
        && text(body.challenge_id).toLowerCase() !== text(challenge.challenge_id).toLowerCase())) {
    throw new CandidateBrokerError(401, 'CANDIDATE_CHALLENGE_INVALID');
  }
  const challengeId = text(challenge.challenge_id).toLowerCase();
  const verificationReceipt = await deterministicControlToken(
    env, 'global-verification-receipt', challengeId
  );
  const receiptExpiresAt = new Date(
    now.getTime() + GLOBAL_VERIFICATION_RECEIPT_TTL_MINUTES * 60_000
  );
  const result = await candidateControlPlaneRpc(
    env, 'identity', 'global_challenge_verify_v1',
    {
      p_internal_context: {
        verification_receipt_hash_hex: await sha256Hex(verificationReceipt),
        verification_receipt_expires_at_utc: receiptExpiresAt.toISOString(),
        actor_identity_hmac: await controlPlaneActorIdentityHmac(env, email)
      },
      p_challenge_id: challengeId, p_token: token,
      p_idempotency_key: idempotencyKey, p_correlation_id: correlationId,
      p_now_utc: now.toISOString()
    }
  );
  if (result?.ok !== true) {
    const status = text(result?.error_code) === 'GLOBAL_CHALLENGE_ATTEMPT_LIMIT' ? 429 : 401;
    throw new CandidateBrokerError(status, 'CANDIDATE_CHALLENGE_INVALID');
  }
  return jsonResponse(200, {
    ok: true, challenge_id: challengeId, purpose,
    expires_at_utc: text(result.expires_at_utc)
  });
}

async function handleControlPlanePasswordComplete(request, env, correlationId) {
  const body = await boundedJson(request.clone());
  const challengeId = text(body.challenge_id).toLowerCase();
  const idempotencyKey = text(body.idempotency_key);
  const invitationToken = text(body.invitation_token);
  const requestedCandidateId = body.selected_candidate_id
    ? text(body.selected_candidate_id).toLowerCase() : '';
  if (!UUID_RE.test(challengeId)
      || (requestedCandidateId && !UUID_RE.test(requestedCandidateId))
      || (invitationToken && (invitationToken.length < 32 || invitationToken.length > 2048))
      || !idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_PASSWORD_REQUEST_INVALID');
  }
  const passwordProof = await globalPasswordProof(
    env, body.password, `complete:${challengeId}:${idempotencyKey}`
  );
  const semanticHash = await sha256Hex(canonicalJson({
    challenge_id: challengeId, selected_candidate_id: requestedCandidateId || null,
    invitation_token_sha256: invitationToken ? await sha256Hex(invitationToken) : null,
    password_digest_hex: passwordProof.password_digest_hex,
    device_id: body.device_id ? await sha256Hex(text(body.device_id)) : null,
    platform: text(body.platform).slice(0, 80) || null
  }));
  const replayIdentity = `${challengeId}:${idempotencyKey}:${semanticHash}`;
  const now = new Date();
  const times = globalSessionTimes(now);
  const refreshToken = await deterministicControlToken(
    env, 'password-complete-refresh', replayIdentity
  );
  const verificationReceipt = await deterministicControlToken(
    env, 'global-verification-receipt', challengeId
  );
  let result;
  try {
    result = await candidateControlPlaneRpc(
      env, 'identity', 'global_password_complete_v1',
      {
      p_internal_context: {
        family_id: await deterministicControlUuid(env, 'password-complete-family', replayIdentity),
        session_id: await deterministicControlUuid(env, 'password-complete-session', replayIdentity),
        refresh_token_hash_hex: await sha256Hex(refreshToken),
        actor_identity_hmac: await controlPlaneActorIdentityHmac(env, challengeId),
        ...(invitationToken ? {
          accept_invitation_token_hash_hex: await sha256Hex(invitationToken),
          invitation_accept_idempotency_key: await deterministicControlUuid(
            env, 'password-complete-invitation-accept', `${challengeId}:${idempotencyKey}`
          )
        } : {}),
        ...times
      },
      p_challenge_id: challengeId, p_verification_receipt: verificationReceipt,
      p_password_proof: passwordProof, p_idempotency_key: idempotencyKey,
        p_correlation_id: correlationId, p_now_utc: now.toISOString()
      }
    );
  } catch (error) {
    if (error instanceof CandidateBrokerError && error.code === 'INVITATION_REQUIRED') {
      throw new CandidateBrokerError(403, 'CANDIDATE_INVITATION_REQUIRED');
    }
    throw error;
  }
  const resultError = controlPlaneResultError(result, 'CANDIDATE_PASSWORD_REQUEST_INVALID');
  if (resultError) throw resultError;
  return finalizeControlPlaneSession(
    result, refreshToken, requestedCandidateId, replayIdentity, env, correlationId
  );
}

async function handleControlPlanePasswordChange(request, access, env, correlationId) {
  const body = await boundedJson(request.clone());
  const idempotencyKey = text(body.idempotency_key);
  if (!idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  const context = globalSessionContext(access, env);
  const metadata = await candidateControlPlaneRpc(
    env, 'identity', 'global_session_metadata_v1',
    { p_global_session_context: context, p_correlation_id: correlationId }
  );
  const currentDigest = await deriveGlobalPasswordDigest(body.current_password, metadata);
  const newProof = await globalPasswordProof(
    env, body.password, `change:${access.global_account_id}:${idempotencyKey}`
  );
  const result = await candidateControlPlaneRpc(
    env, 'identity', 'global_password_change_v1',
    {
      p_global_session_context: {
        ...context,
        actor_identity_hmac: await controlPlaneActorIdentityHmac(env, access.global_account_id)
      },
      p_current_password_proof: {
        presented_digest_hex: currentDigest,
        credential_authority_sha256_hex: metadata.credential_authority_sha256_hex
      },
      p_new_password_proof: newProof, p_idempotency_key: idempotencyKey,
      p_correlation_id: correlationId, p_now_utc: new Date().toISOString()
    }
  );
  const resultError = controlPlaneResultError(result, 'CURRENT_PASSWORD_INVALID');
  if (resultError) throw resultError;
  return jsonResponse(200, {
    ok: true, account_id: text(result.account_id).toLowerCase(),
    session_version: Number(result.session_version)
  });
}

async function handleControlPlaneLogin(request, env, correlationId) {
  const body = await boundedJson(request.clone());
  const email = text(body.email).toLowerCase();
  const idempotencyKey = text(body.idempotency_key);
  const requestedCandidateId = body.selected_candidate_id
    ? text(body.selected_candidate_id).toLowerCase() : '';
  if (!EMAIL_RE.test(email) || !idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_LOGIN_INVALID');
  }
  if (requestedCandidateId && !UUID_RE.test(requestedCandidateId)) {
    throw new CandidateBrokerError(400, 'CANDIDATE_LOGIN_INVALID');
  }
  let metadata = await candidateControlPlaneRpc(
    env, 'identity', 'global_login_metadata_v1',
    { p_email: email, p_correlation_id: correlationId }
  );
  if (metadata.found !== true) {
    const dummySalt = (await sha256Hex(`mytms-global-login-dummy:${email}`)).slice(0, 32);
    metadata = {
      credential_scheme: 'PBKDF2-HMAC-SHA256', scheme_version: 1,
      password_salt_hex: dummySalt,
      parameters: { hash: 'SHA-256', iterations: 100000, length_bytes: 32 },
      credential_authority_sha256_hex: await sha256Hex(`mytms-global-login-dummy-authority:${email}`)
    };
  }
  const presentedDigest = await deriveGlobalPasswordDigest(body.password, metadata);
  const semanticHash = await sha256Hex(canonicalJson({
    email, presented_digest_hex: presentedDigest,
    credential_authority_sha256_hex: metadata.credential_authority_sha256_hex
  }));
  const identity = `${email}:${idempotencyKey}:${semanticHash}`;
  const now = new Date();
  const times = globalSessionTimes(now);
  const refreshToken = await deterministicControlToken(env, 'login-refresh', identity);
  const internalContext = {
    family_id: await deterministicControlUuid(env, 'login-family', identity),
    session_id: await deterministicControlUuid(env, 'login-session', identity),
    refresh_token_hash_hex: await sha256Hex(refreshToken),
    request_semantic_hash_hex: semanticHash,
    actor_identity_hmac: await controlPlaneActorIdentityHmac(env, email),
    ...times
  };
  const result = await candidateControlPlaneRpc(
    env, 'identity', 'global_login_v1',
    {
      p_internal_context: internalContext,
      p_email: email,
      p_password_proof: {
        presented_digest_hex: presentedDigest,
        credential_authority_sha256_hex: metadata.credential_authority_sha256_hex
      },
      p_device_context: {},
      p_idempotency_key: idempotencyKey,
      p_correlation_id: correlationId,
      p_now_utc: times.issued_at_utc
    }
  );
  const resultError = controlPlaneResultError(result, 'CANDIDATE_LOGIN_INVALID');
  if (resultError) throw new CandidateBrokerError(401, 'CANDIDATE_LOGIN_INVALID');
  return finalizeControlPlaneSession(
    result, refreshToken, requestedCandidateId, identity, env, correlationId
  );
}

async function handleControlPlaneRefresh(request, body, refresh, env, correlationId) {
  const idempotencyKey = text(body.idempotency_key);
  if (!idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  const identity = `${refresh.global_session_id}:${refresh.rotation}:${idempotencyKey}`;
  const now = new Date();
  const times = globalSessionTimes(now, refresh.absolute_expires_at_utc);
  const newRefreshToken = await deterministicControlToken(env, 'refresh-rotation', identity);
  const result = await candidateControlPlaneRpc(
    env, 'identity', 'global_refresh_v1',
    {
      p_internal_context: {
        new_session_id: await deterministicControlUuid(env, 'refresh-session', identity),
        new_refresh_token_hash_hex: await sha256Hex(newRefreshToken),
        expires_at_utc: times.expires_at_utc,
        actor_identity_hmac: await controlPlaneActorIdentityHmac(env, refresh.global_account_id)
      },
      p_refresh_token_hash: `\\x${await sha256Hex(refresh.control_plane_refresh_token)}`,
      p_presented_rotation: Number(refresh.rotation),
      p_idempotency_key: idempotencyKey,
      p_correlation_id: correlationId,
      p_now_utc: times.issued_at_utc
    }
  );
  const resultError = controlPlaneResultError(result, 'CANDIDATE_SESSION_INVALID');
  if (resultError) throw resultError;
  const access = {
    authority: 'CONTROL_PLANE', global_account_id: result.account_id,
    global_session_id: result.session_id, session_epoch: Number(result.session_epoch),
    rotation: Number(result.rotation), absolute_expires_at_utc: result.absolute_expires_at_utc,
    exp: Math.floor(Date.parse(result.issued_at_utc) / 1000) + GLOBAL_ACCESS_TTL_SECONDS
  };
  let route = null;
  if (result.selected_membership_id) route = await resolveControlPlaneRoute(access, env, correlationId);
  return wrapGlobalSession(result, env, newRefreshToken, route);
}

async function handleControlPlaneLogout(request, access, env, correlationId) {
  const body = await boundedJson(request.clone());
  const idempotencyKey = text(body.idempotency_key);
  if (!idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  const result = await candidateControlPlaneRpc(
    env, 'identity', 'global_logout_v1',
    {
      p_global_session_context: {
        ...globalSessionContext(access, env),
        actor_identity_hmac: await controlPlaneActorIdentityHmac(env, access.global_account_id)
      },
      p_idempotency_key: idempotencyKey,
      p_correlation_id: correlationId,
      p_now_utc: new Date().toISOString()
    }
  );
  const resultError = controlPlaneResultError(result, 'CANDIDATE_SESSION_INVALID');
  if (resultError) throw resultError;
  return jsonResponse(200, { ok: true, status: 'REVOKED' });
}

async function handleControlPlaneInvitationInspect(request, env, correlationId) {
  const started = Date.now();
  const body = await boundedJson(request.clone());
  const token = text(body.invitation_token);
  if (token.length < 32 || token.length > 4096) {
    throw new CandidateBrokerError(400, 'INVITATION_INVALID');
  }
  const result = await candidateControlPlaneRpc(
    env, 'control', 'invitation_inspect_v1',
    {
      p_token_hash: `\\x${await sha256Hex(token)}`,
      p_now_utc: new Date().toISOString(),
      p_correlation_id: correlationId
    }
  );
  const remaining = ENUMERATION_SAFE_MINIMUM_MS - (Date.now() - started);
  if (remaining > 0) await new Promise((resolve) => setTimeout(resolve, remaining));
  const state = text(result.state);
  const nextStep = text(result.next_step);
  if (!['VALID','EXPIRED','SUPERSEDED','CONSUMED','REVOKED','UNAVAILABLE'].includes(state)
      || !['SIGN_IN','CREATE_ACCOUNT','ACCEPT','NONE'].includes(nextStep)) {
    throw new CandidateBrokerError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
  }
  return jsonResponse(200, {
    ok: true, state, next_step: nextStep,
    ...(['VALID','CONSUMED'].includes(state) && text(result.agency_display_name)
      ? { agency_display_name: text(result.agency_display_name) } : {})
  });
}

async function handleControlPlaneInvitationAccept(request, access, env, correlationId) {
  const body = await boundedJson(request.clone());
  const token = text(body.invitation_token);
  const offerToken = text(body.invitation_offer_token);
  const idempotencyKey = text(body.idempotency_key);
  if (Boolean(token) === Boolean(offerToken)
      || (token && (token.length < 32 || token.length > 4096))
      || (offerToken && (offerToken.length < 32 || offerToken.length > 4096))
      || !UUID_RE.test(idempotencyKey)) {
    throw new CandidateBrokerError(400, 'INVITATION_INVALID');
  }
  const sessionContext = globalSessionContext(access, env);
  const metadata = await candidateControlPlaneRpc(
    env, 'identity', 'global_session_metadata_v1',
    { p_global_session_context: sessionContext, p_correlation_id: correlationId }
  );
  const verifiedEmails = Array.isArray(metadata.verified_emails) ? metadata.verified_emails : [];
  if (!verifiedEmails.length) {
    throw new CandidateBrokerError(403, 'EMAIL_VERIFICATION_REQUIRED');
  }
  const verifiedContext = {
    account_id: access.global_account_id,
    verified_emails: verifiedEmails,
    actor_identity_hmac: await controlPlaneActorIdentityHmac(env, access.global_account_id)
  };
  let result;
  if (offerToken) {
    const offer = await openPendingInvitationOffer(offerToken, access, env);
    result = await candidateControlPlaneRpc(
      env, 'control', 'invitation_offer_accept_v1',
      {
        p_verified_context: verifiedContext,
        p_invitation_id: text(offer.invitation_id).toLowerCase(),
        p_membership_id: text(offer.membership_id).toLowerCase(),
        p_expected_membership_generation: Number(offer.membership_generation),
        p_idempotency_key: idempotencyKey.toLowerCase(),
        p_correlation_id: correlationId
      }
    );
  } else {
    result = await candidateControlPlaneRpc(
      env, 'control', 'invitation_accept_with_context_v2',
      {
        p_verified_context: verifiedContext,
        p_token_hash: `\\x${await sha256Hex(token)}`,
        p_idempotency_key: idempotencyKey.toLowerCase(),
        p_correlation_id: correlationId
      }
    );
  }
  const resultError = controlPlaneResultError(result, 'INVITATION_CONFLICT');
  if (resultError) throw resultError;
  const state = text(result.state);
  if (!['ACTIVE','ALREADY_ACTIVE','PENDING_ADMIN_REVIEW'].includes(state)) {
    throw new CandidateBrokerError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
  }
  let publicChoice;
  if (['ACTIVE','ALREADY_ACTIVE'].includes(state) && result.membership_id_internal) {
    const agencies = await controlPlaneAgenciesInternal(access, env, correlationId);
    const membership = agencies.memberships_internal.find(
      entry => text(entry.membership_id).toLowerCase()
        === text(result.membership_id_internal).toLowerCase()
    );
    if (!membership) throw new CandidateBrokerError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
    const choice = await agencyChoiceToken(access, membership, env);
    publicChoice = {
      agency_choice_token: choice.token,
      display_name: text(membership.display_name),
      environment_label: text(membership.environment_label),
      membership_generation: Number(membership.membership_generation),
      expires_at_utc: choice.expires_at_utc
    };
  }
  return jsonResponse(200, {
    ok: true, state,
    membership_generation: Number(result.membership_generation),
    idempotent_replay: result.idempotent_replay === true,
    ...(publicChoice ? { agency_choice: publicChoice } : {})
  });
}

async function controlPlaneDeviceCiphertext(env, provider, token, keyVersion) {
  const envelope = await sealVersionedEnvelope(
    env, CREDENTIAL_AUTHORITIES.deviceEncryption,
    'mytms-control-plane-device-token-v1',
    { provider, token }, keyVersion
  );
  return bytesToHex(encoder.encode(envelope));
}

async function handleControlPlaneDeviceRegistration(body, access, env, correlationId) {
  const provider = upper(body.push_provider);
  const idempotencyKey = text(body.idempotency_key);
  if (!['APNS', 'FCM', 'WEB_PUSH'].includes(provider)) {
    throw new CandidateBrokerError(400, 'CANDIDATE_PUSH_PROVIDER_INVALID');
  }
  if (!idempotencyKey || idempotencyKey.length > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new CandidateBrokerError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  const encryptionKeyVersion = configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.deviceEncryption);
  const identityKeyVersion = configuredKeyVersion(env, CREDENTIAL_AUTHORITIES.deviceIdentity);
  const identityHmac = await deviceTokenIdentity(
    env, provider, body.push_token, '', identityKeyVersion
  );
  const ciphertext = await controlPlaneDeviceCiphertext(
    env, provider, text(body.push_token), encryptionKeyVersion
  );
  const result = await candidateControlPlaneRpc(
    env, 'identity', 'device_registration_upsert_v1',
    {
      p_global_session_context: {
        ...globalSessionContext(access, env),
        actor_identity_hmac: await controlPlaneActorIdentityHmac(env, access.global_account_id)
      },
      p_provider: provider,
      p_token_identity_hmac: identityHmac,
      p_token_ciphertext: `\\x${ciphertext}`,
      p_encryption_key_version: encryptionKeyVersion,
      p_identity_key_version: identityKeyVersion,
      p_idempotency_key: idempotencyKey,
      p_correlation_id: correlationId,
      p_now_utc: new Date().toISOString()
    }
  );
  const resultError = controlPlaneResultError(result, 'DEVICE_REGISTRATION_INVALID');
  if (resultError) throw resultError;
  return jsonResponse(200, { ok: true, status: text(result.status), provider });
}

export async function handleCandidateBrokerRequest(request, env, ctx = {}) {
  const id = requestId(request);
  const url = new URL(request.url);
  const path = url.pathname;
  if (path === APP_READY_TWO_PLANE_PROOF_PATH) {
    try {
      const ip = text(request.headers.get('cf-connecting-ip')) || 'unknown-ip';
      await applyRateLimit(env, 'CANDIDATE_APP_READY_PROOF_RATE_LIMIT', [
        `ip:${ip}`,
        `page:${ip}:${url.searchParams.get('page') || '1'}`
      ]);
      return handleAppReadyTwoPlaneProof(request, env);
    } catch (error) {
      return errorResponse(error, id);
    }
  }
  if (path === MANAGER_EMAIL_TWO_PLANE_PROOF_PATH) {
    try {
      const ip = text(request.headers.get('cf-connecting-ip')) || 'unknown-ip';
      await applyRateLimit(env, 'CANDIDATE_APP_READY_PROOF_RATE_LIMIT', [
        `ip:${ip}`,
        `manager-email:${ip}`
      ]);
      return handleManagerEmailTwoPlaneProof(request, env);
    } catch (error) {
      return errorResponse(error, id);
    }
  }
  if (isMyTmsGoogleControlPath(path)) {
    return handleMyTmsGoogleControlRequest(request, env);
  }
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

    if (candidateRoute && controlPlaneEnabled(env)
        && path === `${PUBLIC_CANDIDATE_PREFIX}/invitations/inspect`) {
      if (request.method !== 'POST') throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
      return withCors(await handleControlPlaneInvitationInspect(request, env, id), origin);
    }

    if (candidateRoute && controlPlaneEnabled(env)
        && path === `${PUBLIC_CANDIDATE_PREFIX}/invitations/accept`) {
      if (request.method !== 'POST') throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
      if (!globalAuthCutoverEnabled(env)) {
        throw new CandidateBrokerError(503, 'CONTROL_PLANE_AUTH_OPERATION_UNAVAILABLE');
      }
      const access = await openPublicAccess(request, env);
      if (access.authority !== 'CONTROL_PLANE') {
        throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
      }
      return withCors(
        await handleControlPlaneInvitationAccept(request, access, env, id), origin
      );
    }

    if (candidateRoute && globalAuthCutoverEnabled(env)
        && path === `${PUBLIC_CANDIDATE_PREFIX}/auth/login`) {
      return withCors(await handleControlPlaneLogin(request, env, id), origin);
    }

    if (candidateRoute && controlPlaneEnabled(env)
        && path === `${PUBLIC_CANDIDATE_PREFIX}/account/agencies`) {
      if (request.method !== 'GET') throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
      const access = await openPublicAccess(request, env);
      if (access.authority !== 'CONTROL_PLANE') {
        throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
      }
      const agencies = await controlPlaneAgenciesInternal(access, env, id);
      return withCors(jsonResponse(200, await publicAgencyChoices(access, agencies, env)), origin);
    }

    if (candidateRoute && controlPlaneEnabled(env)
        && path === `${PUBLIC_CANDIDATE_PREFIX}/account/agency-session`) {
      if (request.method !== 'POST') throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
      const access = await openPublicAccess(request, env);
      if (access.authority !== 'CONTROL_PLANE') {
        throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
      }
      const body = await boundedJson(request.clone());
      const selected = await issueControlPlaneAgencySession(
        access, text(body.agency_choice_token), text(body.idempotency_key), env, id
      );
      return withCors(
        await wrapGlobalSession(selected.result, env, selected.refreshToken, selected.route), origin
      );
    }

    if (candidateRoute && isChallengeStartPath(path)) {
      if (globalAuthCutoverEnabled(env)) {
        if (request.method !== 'POST') {
          throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
        }
        return withCors(
          await handleControlPlaneChallenge(request, path, env, id), origin
        );
      }
      return withCors(await enumerationSafeChallenge(request, env, path), origin);
    }

    if (candidateRoute && globalAuthCutoverEnabled(env)
        && path === `${PUBLIC_CANDIDATE_PREFIX}/auth/challenge/verify`) {
      if (request.method !== 'POST') {
        throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
      }
      return withCors(
        await handleControlPlaneChallengeVerify(request, env, id), origin
      );
    }

    if (candidateRoute && globalAuthCutoverEnabled(env)
        && path === `${PUBLIC_CANDIDATE_PREFIX}/auth/password/complete`) {
      if (request.method !== 'POST') {
        throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
      }
      return withCors(
        await handleControlPlanePasswordComplete(request, env, id), origin
      );
    }

    if (candidateRoute && path === `${PUBLIC_CANDIDATE_PREFIX}/auth/refresh`) {
      const body = await boundedJson(request.clone());
      const refresh = await openPublicRefresh(body, env);
      if (globalAuthCutoverEnabled(env) && refresh.authority === 'CONTROL_PLANE') {
        return withCors(
          await handleControlPlaneRefresh(request, body, refresh, env, id), origin
        );
      }
      if (globalAuthCutoverEnabled(env) || refresh.authority === 'CONTROL_PLANE') {
        throw new CandidateBrokerError(401, 'CANDIDATE_SESSION_INVALID');
      }
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
      if (globalAuthCutoverEnabled(env)) {
        throw new CandidateBrokerError(503, 'CONTROL_PLANE_AUTH_OPERATION_UNAVAILABLE');
      }
      const body = await boundedJson(request.clone());
      const credentialVersions = assertPublicCredentialSecrets(
        env, currentPublicCredentialVersions(env)
      );
      return withCors(await wrapPrivateSession(await forwardPrivate(request, env, {
        body: { ...body, public_credential_versions: credentialVersions }
      }), env), origin);
    }

    if (candidateRoute && isUnauthenticatedPublicAuthPath(path)) {
      if (globalAuthCutoverEnabled(env)) {
        throw new CandidateBrokerError(503, 'CONTROL_PLANE_AUTH_OPERATION_UNAVAILABLE');
      }
      return withCors(await publicSafePrivateResponse(await forwardPrivate(request, env)), origin);
    }

    if (managerRoute) {
      enforceManagerMethod(path, request.method);
      const managerContext = await managerForwardContext(request, env, id);
      return withCors(await publicSafePrivateResponse(await forwardPrivate(request, env, {
        authorization: managerContext.authorization,
        federated: managerContext.federated
      })), origin);
    }

    let access = null;
    let authorization = '';
    let federated = null;
    try {
      access = await openPublicAccess(request, env);
      if (access.authority === 'CONTROL_PLANE') {
        if (!globalAuthCutoverEnabled(env)) {
          throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
        }
        if (path === `${PUBLIC_CANDIDATE_PREFIX}/auth/logout`) {
          return withCors(await handleControlPlaneLogout(request, access, env, id), origin);
        }
        if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/password`) {
          if (request.method !== 'POST') {
            throw new CandidateBrokerError(405, 'CANDIDATE_METHOD_NOT_ALLOWED');
          }
          return withCors(
            await handleControlPlanePasswordChange(request, access, env, id), origin
          );
        }
        const route = await resolveControlPlaneRoute(access, env, id);
        federated = { access, route };
        if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/select-candidate`) {
          const body = await boundedJson(request.clone());
          const selectedCandidateId = text(body.selected_candidate_id || body.candidate_id).toLowerCase();
          if (!UUID_RE.test(selectedCandidateId)
              || selectedCandidateId !== text(route.local_candidate_id).toLowerCase()) {
            throw new CandidateBrokerError(403, 'CANDIDATE_SELECTION_NOT_ALLOWED');
          }
          return withCors(jsonResponse(200, {
            ok: true,
            access_token: bearerToken(request),
            session_id: access.public_session_id,
            selected_candidate_id: selectedCandidateId,
            access_expires_in_seconds: Math.max(0, Number(access.exp) - Math.floor(Date.now() / 1000)),
            issued_at_utc: new Date(Number(access.iat) * 1000).toISOString(),
            idempotent_replay: true
          }), origin);
        }
      } else {
        if (globalAuthCutoverEnabled(env)) {
          throw new CandidateBrokerError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
        }
        authorization = `Bearer ${access.internal_access_token}`;
      }
    } catch (error) {
      if (!path.includes('/uploads/')) throw error;
      const managerContext = await managerForwardContext(request, env, id);
      authorization = managerContext.authorization;
      federated = managerContext.federated;
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
        federated,
        timeoutMs: route.deadlineMs
      });
      return withCors(await publicSafeDailyResponse(response, dailyCorrelationId, route), origin);
    }

    if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/push-token`) {
      const body = await boundedJson(request.clone());
      if (federated) {
        return withCors(
          await handleControlPlaneDeviceRegistration(body, access, env, id), origin
        );
      }
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
        federated,
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

    const finalisationBody = await candidateFinalisationTransportBody(request, path);
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
        federated,
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
      response = await forwardPrivate(request, env, {
        authorization, federated, ...(finalisationBody ? { body: finalisationBody } : {})
      });
    }
    if (path === `${PUBLIC_CANDIDATE_PREFIX}/account/select-candidate`) {
      return withCors(await wrapSelectedCandidateAccess(response, env, access), origin);
    }
    if (phoneAction) {
      return withCors(await wrapPhoneHandoff(
        response, env, access, request, phoneBinding, federated?.route || null
      ), origin);
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
  withCors,
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
  publicSafePrivateResponse,
  publicSafeDailyResponse,
  validateCandidateFinalisationBody,
  validateCandidateDailyTransport,
  wrapPrivateSession
});
