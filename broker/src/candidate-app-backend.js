import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import {
  buildOfficialWeekPeriod,
  officialTimesheetNumber,
  renderOfficialTimesheetPdfBytes
} from './timesheet-official-pdf.js';
import { validateFrozenTimesheetPresentationModel } from './invoice-presentation-contract.js';
import {
  candidateBootstrapCorrelation,
  composeCandidateBootstrapPhase1b,
  handleCandidateDailyPhase1bRequest
} from './candidate-daily-phase1b.js';
import { isCandidateDailyPath } from './candidate-daily-contract-v1.js';
import { controlPlaneRpc } from '../../candidate-broker/src/control-plane-client.js';
import { verifyCandidatePaperQrViaAdapter } from './mytms-manager-control-adapter.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SHA256_RE = /^[0-9a-f]{64}$/i;
const CANDIDATE_PREFIX = '/candidate-app/v1';
const MANAGER_PREFIX = '/candidate-manager/v1';
const MAX_JSON_BYTES = 1024 * 1024;
const MAX_COMPONENT_BYTES = 15 * 1024 * 1024;
const MAX_IMAGE_DIMENSION = 10000;
const MAX_IMAGE_PIXELS = 40_000_000;
const ACCESS_TTL_SECONDS = 15 * 60;
const REFRESH_TTL_DAYS = 30;
const REFRESH_ABSOLUTE_TTL_DAYS = 90;
const PASSWORD_ITERATIONS = 100000;
const PASSWORD_SCHEME = 'PBKDF2-HMAC-SHA256';
const PASSWORD_SCHEME_VERSION = 1;
const RENDERER_CONTRACT_VERSION = 'CANDIDATE_REVIEW_DOCUMENTS_V1';
const DOCUMENT_BRANDING_CONTRACT_VERSION = 'CANDIDATE_DOCUMENT_BRANDING_V1';
const MANAGER_ACTION_METHODS = Object.freeze({
  start: 'GET',
  progress: 'POST',
  approve: 'POST',
  refuse: 'POST'
});

const AUTH_ERROR_CODES = new Set([
  'OFFICE_AUTH_REQUIRED',
  'CANDIDATE_ACCESS_TOKEN_INVALID',
  'CANDIDATE_ACCESS_TOKEN_EXPIRED',
  'CANDIDATE_SESSION_INVALID',
  'CANDIDATE_SESSION_EXPIRED',
  'CANDIDATE_LOGIN_INVALID',
  'CANDIDATE_CHALLENGE_INVALID',
  'CANDIDATE_CHALLENGE_EXPIRED',
  'CANDIDATE_CHALLENGE_ALREADY_USED',
  'CANDIDATE_VERIFIED_CHALLENGE_INVALID',
  'MANAGER_APPROVAL_REQUEST_NOT_READY',
  'MANAGER_APPROVAL_REQUEST_EXPIRED'
]);

const CONFLICT_ERROR_CODES = new Set([
  'CANDIDATE_CONTEXT_STALE',
  'CANDIDATE_TIMESHEET_MOVED',
  'CANDIDATE_REQUEST_GENERATION_STALE',
  'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS',
  'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT',
  'CANDIDATE_REJECTION_SCOPE_CONFLICT',
  'CANDIDATE_IDEMPOTENCY_CONFLICT',
  'CANDIDATE_ACTION_NOT_ELIGIBLE',
  'CANDIDATE_REQUIRES_UNAUTHORISE',
  'CANDIDATE_PROTECTED_FINANCIAL_HISTORY',
  'CANDIDATE_IMPORT_AUTHORITATIVE',
  'CANDIDATE_TOO_MANY_AFFECTED_WORKFLOWS',
  'IDEMPOTENCY_CONFLICT',
  'WORKFLOW_VERSION_MISMATCH',
  'WORKFLOW_GENERATION_CONFLICT',
  'ROW_SIGNATURE_MISMATCH',
  'TIMESHEET_MOVED',
  'ROUTE_CHANGE_CONTEXT_CHANGED',
  'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE',
  'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED',
  'CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT',
  'CANDIDATE_COMPONENT_PREPARE_CONTRACT_MISMATCH',
  'CANDIDATE_COMPONENT_PREPARE_GENERATION_CONFLICT',
  'CANDIDATE_COMPONENT_PREPARE_STATE_CONFLICT',
  'CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT',
  'CANDIDATE_PAPER_RETURN_PAGE_DUPLICATE',
  'CANDIDATE_PAPER_WORKFLOW_CONFLICT',
  'CANDIDATE_PAPER_OUTBOX_CONFLICT',
  'CANDIDATE_BREAK_ENTRY_CONTEXT_STALE',
  'CANDIDATE_PAPER_QR_PROOF_STALE',
  'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
  'MANAGER_REVIEW_MANIFEST_MISMATCH'
]);

const NOT_FOUND_ERROR_CODES = new Set([
  'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND',
  'CANDIDATE_ROUTE_NOT_FOUND',
  'CANDIDATE_WORKFLOW_NOT_FOUND',
  'CANDIDATE_DAILY_SHIFT_NOT_FOUND',
  'CANDIDATE_WORKFLOW_CONTRACT_NOT_FOUND',
  'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND',
  'CANDIDATE_REMINDER_BATCH_NOT_FOUND',
  'TIMESHEET_NOT_FOUND'
]);

const COMPONENT_MEDIA_TYPES = Object.freeze({
  CANDIDATE_SIGNATURE: ['image/png', 'image/jpeg'],
  MANAGER_SIGNATURE: ['image/png', 'image/jpeg'],
  EXPENSE_EVIDENCE: ['image/png', 'image/jpeg', 'application/pdf'],
  MILEAGE_FORM: ['image/png', 'image/jpeg', 'application/pdf'],
  SIGNED_RETURN: ['image/png', 'image/jpeg', 'application/pdf']
});
const PAPER_RETURN_PROOF_VERSION = 'CANDIDATE_PAPER_RETURN_PROOF_V1';
const BREAK_ENTRY_CONTEXT_VERSION = 'CANDIDATE_BREAK_ENTRY_V1';

const CANDIDATE_WORKFLOW_ACTIONS = new Set([
  'AMEND', 'WORKER_SUBMIT', 'SELECT_APPROVAL_METHOD', 'SELECT_PHONE_APPROVAL',
  'CREATE_EMAIL_APPROVAL_REQUEST', 'PAPER_PREPARE', 'PAPER_RETURN', 'REMIND',
  'RENEW', 'CANCEL', 'SUPERSEDE', 'CANCEL_MANAGER_HANDOFF', 'RETRY_FINALISATION',
  'MILEAGE_FORM_PREPARE', 'MILEAGE_FORM_EMAIL'
]);

const ROUTE_INTERVENTION_REASONS = new Set([
  'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',
  'CANDIDATE_REPORTED_HOURS_INCORRECT',
  'HIRING_MANAGER_REPORTED_HOURS_INCORRECT',
  'ELECTRONIC_SUBMISSION_TECHNICAL_FAILURE',
  'OTHER_EXCEPTIONAL_OFFICE_INTERVENTION'
]);

const OFFICE_CONTRACT_VERSION = 'CLOUDTMS_OFFICE_CANDIDATE_API_V1';
const MAX_OFFICE_REMINDER_BATCH_ROWS = 1000;
const MAX_OFFICE_REMINDER_SOURCE_ROWS = 10000;
const OFFICE_REMINDER_PAGE_SIZE = 25;
const MAX_OFFICE_REMINDER_PAGE_SIZE = 100;
const OFFICE_PROJECTION_SURFACES = new Set([
  'SIMPLE_TIMESHEET', 'TIMESHEET_SUMMARY', 'BULK_PROCESS', 'BULK_AUTHORISE',
  'INVOICE_GENERATOR', 'INVOICE_ISSUER'
]);
const OFFICE_MANAGER_ACTIONS = Object.freeze({
  remind: 'REMIND',
  renew: 'RENEW',
  cancel: 'MANAGER_REQUEST_CANCEL',
  'cancel-manager-handoff': 'CANCEL_MANAGER_HANDOFF',
  'phone-review': 'BEGIN_MANAGER_REVIEW',
  'phone-progress': 'RECORD_REVIEW_PROGRESS',
  'phone-approve': 'PHONE_APPROVE',
  'phone-refuse': 'MANAGER_REFUSE'
});
const OFFICE_ADMIN_PERMISSIONS = new Set([
  'view_candidate_state', 'change_route', 'reject_submission', 'resubmit_rejected',
  'send_manager_reminder', 'send_manager_reminder_batch', 'renew_manager_request',
  'cancel_manager_request', 'manage_phone_approval', 'manage_paper',
  'retry_finalisation', 'mark_no_work'
]);
const OFFICE_WORKFLOW_ACTION_PERMISSIONS = Object.freeze({
  remind: 'send_manager_reminder',
  renew: 'renew_manager_request',
  cancel: 'cancel_manager_request',
  'cancel-manager-handoff': 'manage_phone_approval',
  'phone-review': 'manage_phone_approval',
  'phone-progress': 'manage_phone_approval',
  'phone-approve': 'manage_phone_approval',
  'phone-refuse': 'manage_phone_approval',
  'retry-finalisation': 'retry_finalisation'
});

class CandidateHttpError extends Error {
  constructor(status, code, message = code, details = null) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

function jsonResponse(status, body, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', ...headers }
  });
}

function requestId(request) {
  const supplied = String(request.headers.get('x-request-id') || '').trim();
  return supplied && supplied.length <= 120 ? supplied : crypto.randomUUID();
}

function text(value) {
  return String(value == null ? '' : value).trim();
}

function upper(value) {
  return text(value).toUpperCase();
}

function isObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function requireUuid(value, code = 'INVALID_UUID') {
  const out = text(value);
  if (!UUID_RE.test(out)) throw new CandidateHttpError(400, code);
  return out;
}

function requireSha256(value, code = 'INVALID_SHA256') {
  const out = text(value).replace(/^\\x/i, '').toLowerCase();
  if (!SHA256_RE.test(out)) throw new CandidateHttpError(400, code);
  return out;
}

function requireCandidateIdempotency(value) {
  const key = text(value);
  if (!key || key.length > 200) {
    throw new CandidateHttpError(400, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  return key;
}

function publicCredentialVersions(value) {
  if (!isObject(value)) return null;
  const contractVersion = text(value.contract_version);
  const output = {
    contract_version: contractVersion,
    access_key_version: Number(value.access_key_version),
    refresh_key_version: Number(value.refresh_key_version),
    public_session_key_version: Number(value.public_session_key_version)
  };
  if (contractVersion !== 'CANDIDATE_PUBLIC_CREDENTIAL_VERSIONS_V1'
      || [output.access_key_version, output.refresh_key_version, output.public_session_key_version]
        .some(version => !Number.isSafeInteger(version) || version < 1 || version > 65535)) {
    throw new CandidateHttpError(400, 'CANDIDATE_BROKER_CREDENTIAL_VERSION_INVALID');
  }
  return output;
}

function publicPhoneBinding(value) {
  if (!isObject(value)
      || text(value.contract_version) !== 'CANDIDATE_PUBLIC_PHONE_BINDING_V1'
      || !SHA256_RE.test(text(value.public_session_binding_sha256))
      || (value.device_binding_sha256 != null
        && !SHA256_RE.test(text(value.device_binding_sha256)))) {
    throw new CandidateHttpError(400, 'CANDIDATE_PHONE_HANDOFF_BINDING_INVALID');
  }
  return {
    contract_version: 'CANDIDATE_PUBLIC_PHONE_BINDING_V1',
    public_session_binding_sha256: text(value.public_session_binding_sha256).toLowerCase(),
    device_binding_sha256: value.device_binding_sha256 == null
      ? null : text(value.device_binding_sha256).toLowerCase()
  };
}

function publicPushIdentityProofs(value, currentHmac, currentVersion) {
  const source = Array.isArray(value) && value.length
    ? value
    : [{ key_version: currentVersion, identity_hmac: currentHmac }];
  if (source.length > 32) {
    throw new CandidateHttpError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  }
  const seen = new Set();
  const proofs = source.map((proof) => {
    const keyVersion = Number(proof?.key_version);
    const identityHmac = text(proof?.identity_hmac).toLowerCase();
    if (!Number.isSafeInteger(keyVersion) || keyVersion < 1 || keyVersion > 65535
        || !SHA256_RE.test(identityHmac) || seen.has(keyVersion)) {
      throw new CandidateHttpError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
    }
    seen.add(keyVersion);
    return { key_version: keyVersion, identity_hmac: identityHmac };
  });
  if (!proofs.some(proof => (
    proof.key_version === currentVersion && proof.identity_hmac === currentHmac
  ))) {
    throw new CandidateHttpError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  }
  return proofs;
}

function requireInteger(value, code = 'INVALID_INTEGER', minimum = 0) {
  const out = Number(value);
  if (!Number.isSafeInteger(out) || out < minimum) throw new CandidateHttpError(400, code);
  return out;
}

function normaliseEmail(value) {
  const out = text(value).toLowerCase();
  if (!EMAIL_RE.test(out)) throw new CandidateHttpError(400, 'CANDIDATE_EMAIL_INVALID');
  return out;
}

function normaliseMediaType(value) {
  return text(value).split(';')[0].trim().toLowerCase();
}

const CANDIDATE_NOTIFICATION_PREFERENCE_KEYS = Object.freeze([
  'push',
  'manager_approval_updates',
  'timesheet_expense_attention',
  'authorisation',
  'payment',
  'approval_reminders',
  'resubmission_required'
]);

function safeCandidateNotificationPreferences(value) {
  const source = isObject(value) ? value : {};
  const valueOrDefault = (key, legacyKeys = []) => {
    if (typeof source[key] === 'boolean') return source[key];
    const legacy = legacyKeys.map(name => source[name]).filter(item => typeof item === 'boolean');
    return legacy.length ? legacy.every(Boolean) : true;
  };
  return {
    push: valueOrDefault('push'),
    manager_approval_updates: valueOrDefault(
      'manager_approval_updates', ['manager_approval', 'manager_refusal']
    ),
    timesheet_expense_attention: valueOrDefault(
      'timesheet_expense_attention', ['office_rejection']
    ),
    authorisation: valueOrDefault('authorisation'),
    payment: valueOrDefault('payment'),
    approval_reminders: valueOrDefault('approval_reminders'),
    resubmission_required: valueOrDefault('resubmission_required')
  };
}

function requireCandidateNotificationPreferences(value) {
  if (!isObject(value)
      || Object.keys(value).length !== CANDIDATE_NOTIFICATION_PREFERENCE_KEYS.length
      || !CANDIDATE_NOTIFICATION_PREFERENCE_KEYS.every(key => typeof value[key] === 'boolean')
      || !Object.keys(value).every(key => CANDIDATE_NOTIFICATION_PREFERENCE_KEYS.includes(key))) {
    throw new CandidateHttpError(400, 'CANDIDATE_NOTIFICATION_PREFERENCES_INVALID');
  }
  return Object.fromEntries(
    CANDIDATE_NOTIFICATION_PREFERENCE_KEYS.map(key => [key, value[key]])
  );
}

function hex(bytes) {
  return Array.from(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes))
    .map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function bytesFromHex(value) {
  const input = text(value).replace(/^\\x/i, '');
  if (!/^[0-9a-f]*$/i.test(input) || input.length % 2 !== 0) return new Uint8Array();
  const out = new Uint8Array(input.length / 2);
  for (let index = 0; index < out.length; index += 1) out[index] = parseInt(input.slice(index * 2, index * 2 + 2), 16);
  return out;
}

function base64UrlEncode(bytes) {
  let binary = '';
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  for (const byte of source) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function base64UrlDecode(value) {
  const source = text(value).replace(/-/g, '+').replace(/_/g, '/');
  const padded = source + '='.repeat((4 - source.length % 4) % 4);
  const binary = atob(padded);
  const out = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) out[index] = binary.charCodeAt(index);
  return out;
}

function randomToken(byteCount = 32) {
  return base64UrlEncode(crypto.getRandomValues(new Uint8Array(byteCount)));
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
  return hex(await sha256Bytes(value));
}

async function importHmacKey(secret) {
  if (!text(secret)) throw new CandidateHttpError(503, 'CANDIDATE_TOKEN_SECRET_UNAVAILABLE');
  return crypto.subtle.importKey('raw', encoder.encode(String(secret)), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']);
}

async function signCompact(secret, payload) {
  const encoded = base64UrlEncode(encoder.encode(JSON.stringify(payload)));
  const signature = await crypto.subtle.sign('HMAC', await importHmacKey(secret), encoder.encode(encoded));
  return `${encoded}.${base64UrlEncode(signature)}`;
}

async function deterministicOpaqueToken(secret, namespace, ...parts) {
  const signature = await crypto.subtle.sign(
    'HMAC',
    await importHmacKey(secret),
    encoder.encode([namespace, ...parts.map(value => String(value == null ? '' : value))].join('\u001f'))
  );
  return base64UrlEncode(signature);
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (isObject(value)) {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value === undefined ? null : value);
}

function configuredKeyVersion(env, name) {
  const version = Number(env[name] || 1);
  if (!Number.isSafeInteger(version) || version < 1 || version > 32) {
    throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_KEY_VERSION_INVALID');
  }
  return version;
}

function versionedSecret(env, prefix, version, fallback) {
  const configured = text(env[`${prefix}_V${version}`]);
  if (configured) return configured;
  if (Number(version) === 1 && text(fallback)) return text(fallback);
  throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_SECRET_VERSION_UNAVAILABLE');
}

function authReplayKeyVersion(env) {
  return configuredKeyVersion(env, 'CANDIDATE_AUTH_REPLAY_KEY_VERSION');
}

function authReplayReadVersions(env) {
  const values = new Set([authReplayKeyVersion(env)]);
  for (const raw of text(env.CANDIDATE_AUTH_REPLAY_READ_KEY_VERSIONS).split(',')) {
    if (!raw) continue;
    const parsed = Number(raw);
    if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 32) {
      throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_KEY_VERSION_INVALID');
    }
    values.add(parsed);
  }
  return Array.from(values);
}

function authReplaySecret(env, version) {
  if (!authReplayReadVersions(env).includes(Number(version))) {
    throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_SECRET_VERSION_UNAVAILABLE');
  }
  return versionedSecret(env, 'CANDIDATE_AUTH_REPLAY_SECRET', version,
    env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET);
}

function challengeKeyVersion(env) {
  return configuredKeyVersion(env, 'CANDIDATE_CHALLENGE_TOKEN_KEY_VERSION');
}

function challengeReadVersions(env) {
  const values = new Set([challengeKeyVersion(env)]);
  for (const raw of text(env.CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS).split(',')) {
    if (!raw) continue;
    const parsed = Number(raw);
    if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 32) {
      throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_KEY_VERSION_INVALID');
    }
    values.add(parsed);
  }
  return Array.from(values);
}

function challengeSecretForVersion(env, version) {
  return versionedSecret(env, 'CANDIDATE_CHALLENGE_TOKEN_SECRET', version,
    env.CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET);
}

function managerTokenKeyVersion(env) {
  return configuredKeyVersion(env, 'CANDIDATE_MANAGER_TOKEN_KEY_VERSION');
}

function managerTokenSecret(env, version) {
  return versionedSecret(env, 'CANDIDATE_MANAGER_TOKEN_SECRET', version,
    env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET);
}

async function requestHmacSha256(secret, namespace, value) {
  const signature = await crypto.subtle.sign(
    'HMAC', await importHmacKey(secret),
    encoder.encode(`${namespace}\u001f${canonicalJson(value)}`)
  );
  return hex(signature);
}

async function authSecretProof(env, version, purpose, secretValue) {
  return requestHmacSha256(
    authReplaySecret(env, version), `candidate-auth-secret-proof-v1:${purpose}`,
    String(secretValue == null ? '' : secretValue)
  );
}

async function authRequestSha256(env, version, action, identity) {
  return requestHmacSha256(
    authReplaySecret(env, version), 'candidate-auth-mutation-request-v1',
    { contract_version: 'CANDIDATE_AUTH_MUTATION_REQUEST_V1', action: upper(action), ...identity }
  );
}

async function deterministicRefreshToken(env, version, action, sessionId, idempotencyKey) {
  return deterministicOpaqueToken(
    authReplaySecret(env, version), 'candidate-refresh-token-v1',
    environmentName(env), upper(action), requireUuid(sessionId), idempotencyKey
  );
}

async function verifyCompact(secret, token) {
  const [encoded, signature] = text(token).split('.');
  if (!encoded || !signature) return null;
  let valid = false;
  try {
    valid = await crypto.subtle.verify('HMAC', await importHmacKey(secret), base64UrlDecode(signature), encoder.encode(encoded));
  } catch {
    return null;
  }
  if (!valid) return null;
  try {
    const payload = JSON.parse(decoder.decode(base64UrlDecode(encoded)));
    if (!isObject(payload)) return null;
    return payload;
  } catch {
    return null;
  }
}

async function importEnvelopeKey(secret, purpose) {
  if (!text(secret)) throw new CandidateHttpError(503, 'CANDIDATE_TOKEN_SECRET_UNAVAILABLE');
  const material = await sha256Bytes(`${purpose}:${String(secret)}`);
  return crypto.subtle.importKey('raw', material, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
}

async function sealEnvelope(secret, purpose, payload) {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv, additionalData: encoder.encode(purpose) },
    await importEnvelopeKey(secret, purpose),
    encoder.encode(JSON.stringify(payload))
  ));
  return `v1.${base64UrlEncode(iv)}.${base64UrlEncode(ciphertext)}`;
}

async function openEnvelope(secret, purpose, value) {
  const [version, ivEncoded, ciphertextEncoded] = text(value).split('.');
  if (version !== 'v1' || !ivEncoded || !ciphertextEncoded) return null;
  try {
    const plain = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv: base64UrlDecode(ivEncoded), additionalData: encoder.encode(purpose) },
      await importEnvelopeKey(secret, purpose),
      base64UrlDecode(ciphertextEncoded)
    );
    const payload = JSON.parse(decoder.decode(plain));
    return isObject(payload) ? payload : null;
  } catch {
    return null;
  }
}

function environmentName(env) {
  const value = upper(env.CANDIDATE_APP_ENVIRONMENT);
  if (!['TEST', 'LIVE'].includes(value)) throw new CandidateHttpError(503, 'CANDIDATE_ENVIRONMENT_INVALID');
  return value;
}

function tokenSecret(env) {
  const value = text(env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET);
  if (!value) throw new CandidateHttpError(503, 'CANDIDATE_TOKEN_SECRET_UNAVAILABLE');
  return value;
}

function challengeTokenSecret(env) {
  const value = text(env.CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET);
  if (!value) throw new CandidateHttpError(503, 'CANDIDATE_CHALLENGE_SECRET_UNAVAILABLE');
  return value;
}

function serviceHeaders(env, extras = {}) {
  const key = env.SUPABASE_SERVICE_ROLE_KEY;
  if (!text(env.SUPABASE_URL) || !text(key)) throw new CandidateHttpError(503, 'CANDIDATE_DATABASE_UNAVAILABLE');
  return { apikey: key, Authorization: `Bearer ${key}`, 'content-type': 'application/json', ...extras };
}

async function readJson(request, maxBytes = MAX_JSON_BYTES) {
  const declared = Number(request.headers.get('content-length') || 0);
  if (Number.isFinite(declared) && declared > maxBytes) throw new CandidateHttpError(413, 'REQUEST_BODY_TOO_LARGE');
  const bodyText = await request.text();
  if (encoder.encode(bodyText).byteLength > maxBytes) throw new CandidateHttpError(413, 'REQUEST_BODY_TOO_LARGE');
  if (!bodyText) return {};
  try {
    const parsed = JSON.parse(bodyText);
    if (!isObject(parsed)) throw new Error('object required');
    return parsed;
  } catch {
    throw new CandidateHttpError(400, 'INVALID_JSON');
  }
}

async function restRows(env, table, query) {
  const response = await fetch(`${env.SUPABASE_URL}/rest/v1/${table}?${query}`, { headers: serviceHeaders(env) });
  if (!response.ok) throw new Error(`CANDIDATE_DATABASE_READ_FAILED:${response.status}`);
  const value = await response.json().catch(() => []);
  return Array.isArray(value) ? value : [];
}

async function restRowsPaged(env, table, query, { pageSize = 1000, maxRows = MAX_OFFICE_REMINDER_BATCH_ROWS } = {}) {
  const rows = [];
  let offset = 0;
  while (true) {
    const page = await restRows(env, table, `${query}${query ? '&' : ''}limit=${pageSize}&offset=${offset}`);
    rows.push(...page);
    if (rows.length > maxRows) throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_CATALOGUE_TOO_LARGE');
    if (page.length < pageSize) return rows;
    offset += page.length;
  }
}

async function restOne(env, table, query) {
  const rows = await restRows(env, table, `${query}${query.includes('limit=') ? '' : '&limit=1'}`);
  return rows[0] || null;
}

async function restWrite(env, table, method, query, body, prefer = 'return=representation') {
  const suffix = query ? `?${query}` : '';
  const response = await fetch(`${env.SUPABASE_URL}/rest/v1/${table}${suffix}`, {
    method,
    headers: serviceHeaders(env, { Prefer: prefer }),
    body: JSON.stringify(body)
  });
  const responseText = await response.text().catch(() => '');
  if (!response.ok) throw new Error(`CANDIDATE_DATABASE_WRITE_FAILED:${response.status}`);
  if (!responseText) return null;
  try {
    const value = JSON.parse(responseText);
    return Array.isArray(value) ? value[0] || null : value;
  } catch {
    return null;
  }
}

function unwrapRpc(value, name = '') {
  let out = value;
  if (out && isObject(out) && Object.prototype.hasOwnProperty.call(out, 'data')) out = out.data;
  if (Array.isArray(out) && out.length === 1) out = out[0];
  if (out && isObject(out) && name && Object.prototype.hasOwnProperty.call(out, name)) out = out[name];
  if (Array.isArray(out) && out.length === 1) out = out[0];
  return out;
}

function knownErrorCode(error) {
  if (error instanceof CandidateHttpError) return error.code;
  const preferredFrom = (source) => (text(source).toUpperCase()
    .match(/[A-Z][A-Z0-9_]{2,}/g) || []).find((value) => (
    value.startsWith('CANDIDATE_') || value.startsWith('MANAGER_') ||
    value.startsWith('WORKFLOW_') || value.startsWith('ROUTE_') ||
    value.startsWith('TIMESHEET_') || value === 'IDEMPOTENCY_CONFLICT' ||
    value === 'ROW_SIGNATURE_MISMATCH' || value === 'RATE_ISSUE' ||
    value === 'PAY_CHANNEL_ISSUE' || value === 'PAY_METHOD_MISSING' ||
    value === 'EXPENSE_INVOICE_EMAIL_REQUIRED'
  ));
  // PostgREST supplies the database's closed error in its structured JSON.
  // Prefer that authority over an RPC function name embedded earlier in the
  // transport error string (for example candidate_app_bootstrap_v1).
  for (const source of [
    error?.json?.message,
    error?.json?.details,
    error?.json?.hint,
    error?.code
  ]) {
    const preferred = preferredFrom(source);
    if (preferred) return preferred;
  }
  return preferredFrom(error?.message) || 'CANDIDATE_REQUEST_FAILED';
}

const OFFICE_ERROR_ALIASES = Object.freeze({
  IDEMPOTENCY_CONFLICT: 'CANDIDATE_IDEMPOTENCY_CONFLICT',
  ROW_SIGNATURE_MISMATCH: 'CANDIDATE_CONTEXT_STALE',
  ROUTE_CHANGE_CONTEXT_CHANGED: 'CANDIDATE_CONTEXT_STALE',
  TIMESHEET_MOVED: 'CANDIDATE_TIMESHEET_MOVED',
  CANDIDATE_MANAGER_MAIL_DELIVERY_IN_PROGRESS: 'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS',
  CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS: 'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS',
  CANDIDATE_CANCELLATION_REASON_REQUIRED: 'CANDIDATE_REASON_REQUIRED',
  MANAGER_REFUSAL_REASON_REQUIRED: 'CANDIDATE_REASON_REQUIRED',
  ROUTE_INTERVENTION_REASON_REQUIRED: 'CANDIDATE_REASON_REQUIRED',
  CANDIDATE_CANCELLATION_REASON_INVALID: 'CANDIDATE_REASON_INVALID',
  MANAGER_REFUSAL_REASON_INVALID: 'CANDIDATE_REASON_INVALID',
  ROUTE_CHANGE_REASON_INVALID: 'CANDIDATE_REASON_INVALID',
  CANDIDATE_REJECT_REQUIRES_UNAUTHORISE: 'CANDIDATE_REQUIRES_UNAUTHORISE',
  ROUTE_CHANGE_REQUIRES_UNAUTHORISE: 'CANDIDATE_REQUIRES_UNAUTHORISE',
  CANDIDATE_REJECT_PROTECTED_HISTORY: 'CANDIDATE_PROTECTED_FINANCIAL_HISTORY',
  ROUTE_CHANGE_IMPORT_AUTHORITATIVE_BLOCK: 'CANDIDATE_IMPORT_AUTHORITATIVE'
});

function officeErrorCode(error) {
  const source = knownErrorCode(error);
  return OFFICE_ERROR_ALIASES[source] || source;
}

function errorResponse(error, correlationId, office = false) {
  const code = office ? officeErrorCode(error) : knownErrorCode(error);
  let status = error instanceof CandidateHttpError ? error.status : 400;
  if (AUTH_ERROR_CODES.has(code)) status = 401;
  else if (CONFLICT_ERROR_CODES.has(code)) status = 409;
  else if (NOT_FOUND_ERROR_CODES.has(code)) status = 404;
  else if (status !== 405 && (code.endsWith('_DISABLED') || code.includes('NOT_ALLOWED') || code.includes('FORBIDDEN'))) status = 403;
  else if (code === 'CANDIDATE_REQUEST_FAILED') status = 500;
  const professionalMessages = {
    CANDIDATE_CONTEXT_STALE: 'This timesheet changed. Refresh it before trying again.',
    CANDIDATE_REQUEST_GENERATION_STALE: 'This manager request changed. Refresh it before trying again.',
    CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED: 'The selected timesheets changed. Review the current selection before sending reminders.',
    CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS: 'A provider handoff is in progress. Try again when it has completed.',
    CANDIDATE_TIMESHEET_MOVED: 'This timesheet has moved to a newer version. Refresh it before trying again.',
    CANDIDATE_REASON_REQUIRED: 'Enter the required reason before continuing.',
    CANDIDATE_REASON_INVALID: 'Choose a valid reason before continuing.',
    CANDIDATE_REQUIRES_UNAUTHORISE: 'Unauthorise this timesheet before continuing.',
    CANDIDATE_PROTECTED_FINANCIAL_HISTORY: 'Protected financial history prevents this Candidate action.',
    CANDIDATE_IMPORT_AUTHORITATIVE: 'The imported record is authoritative and cannot be changed here.',
    CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT: 'Another Candidate PAPER workflow must be resolved before continuing.',
    CANDIDATE_REJECTION_SCOPE_CONFLICT: 'CloudTMS could not establish one safe rejection scope.',
    CANDIDATE_IDEMPOTENCY_CONFLICT: 'This operation key has already been used for a different request.',
    CANDIDATE_BREAK_ENTRY_CONTEXT_STALE: 'The break-entry setting changed. Refresh the timesheet before submitting it.',
    CANDIDATE_BREAK_ENTRY_MODE_MISMATCH: 'Use the break-entry format shown for this timesheet.',
    CANDIDATE_BREAK_ENTRY_NOT_APPLICABLE: 'Break entry is not available for this timesheet route.',
    CANDIDATE_BREAK_ENTRY_REQUIRED: 'Enter the break for each worked period, or confirm that no break was taken.',
    CANDIDATE_PAPER_QR_UNREADABLE: 'The timesheet QR code could not be read. Take a clearer photograph of the full page.',
    CANDIDATE_PAPER_QR_PROOF_MISMATCH: 'The photographed timesheet does not match this submission.',
    CANDIDATE_PAPER_QR_PROOF_FORBIDDEN: 'A QR code is not expected on this supporting page.',
    CANDIDATE_PAPER_QR_PROOF_STALE: 'The returned-paper upload changed. Start the upload again.',
    METHOD_NOT_ALLOWED: 'This operation does not support that HTTP method.'
  };
  const body = {
    ok: false,
    error_code: code,
    message: professionalMessages[code] || 'CloudTMS could not complete this Candidate operation.',
    retryable: ['CANDIDATE_CONTEXT_STALE', 'CANDIDATE_TIMESHEET_MOVED', 'CANDIDATE_REQUEST_GENERATION_STALE',
      'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED', 'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS'].includes(code),
    request_id: correlationId
  };
  if (error instanceof CandidateHttpError && error.details != null) body.details = error.details;
  const headers = {};
  const retryAfterSeconds = Number(error instanceof CandidateHttpError
    ? error.details?.retry_after_seconds : 0);
  if (status === 429 && Number.isSafeInteger(retryAfterSeconds) && retryAfterSeconds > 0) {
    headers['retry-after'] = String(retryAfterSeconds);
  }
  return jsonResponse(status, body, headers);
}

async function createAccessToken(env, session) {
  const issuedAt = Date.parse(text(session?.issued_at_utc));
  const now = Number.isFinite(issuedAt)
    ? Math.floor(issuedAt / 1000) : Math.floor(Date.now() / 1000);
  return signCompact(tokenSecret(env), {
    typ: 'candidate_access', aud: 'cloudtms-candidate-app',
    env: environmentName(env), sid: session.session_id, rot: Number(session.rotation || 0),
    iat: now, exp: now + ACCESS_TTL_SECONDS
  });
}

function bearerToken(request) {
  const match = /^Bearer\s+(.+)$/i.exec(text(request.headers.get('authorization')));
  return match ? text(match[1]) : '';
}

async function candidateAccessClaims(request, env) {
  const payload = await verifyCompact(tokenSecret(env), bearerToken(request));
  const now = Math.floor(Date.now() / 1000);
  if (!payload || payload.typ !== 'candidate_access' || payload.aud !== 'cloudtms-candidate-app'
      || payload.env !== environmentName(env) || !UUID_RE.test(text(payload.sid))) {
    throw new CandidateHttpError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
  }
  if (!Number.isFinite(Number(payload.exp)) || Number(payload.exp) <= now) {
    throw new CandidateHttpError(401, 'CANDIDATE_ACCESS_TOKEN_EXPIRED');
  }
  return payload;
}

async function verifyCandidateAccess(request, env) {
  const payload = await candidateAccessClaims(request, env);
  const session = await restOne(env, 'candidate_app_sessions',
    `id=eq.${encodeURIComponent(payload.sid)}&select=id,account_id,environment,selected_candidate_id,status,rotation,expires_at_utc,absolute_expires_at_utc`);
  if (!session || session.environment !== payload.env || session.status !== 'ACTIVE'
      || Number(session.rotation) !== Number(payload.rot)
      || Date.parse(session.expires_at_utc) <= Date.now() || Date.parse(session.absolute_expires_at_utc) <= Date.now()) {
    throw new CandidateHttpError(401, 'CANDIDATE_SESSION_INVALID');
  }
  return {
    session_id: session.id,
    account_id: session.account_id,
    selected_candidate_id: session.selected_candidate_id,
    rotation: Number(session.rotation),
    environment: session.environment
  };
}

async function derivePasswordVerifier(password, salt = null, iterations = PASSWORD_ITERATIONS) {
  const secret = String(password == null ? '' : password);
  if (secret.length < 12 || secret.length > 512) throw new CandidateHttpError(400, 'CANDIDATE_PASSWORD_POLICY_FAILED');
  const saltBytes = salt || crypto.getRandomValues(new Uint8Array(16));
  const imported = await crypto.subtle.importKey('raw', encoder.encode(secret), 'PBKDF2', false, ['deriveBits']);
  const digest = new Uint8Array(await crypto.subtle.deriveBits(
    { name: 'PBKDF2', hash: 'SHA-256', salt: saltBytes, iterations }, imported, 256
  ));
  return {
    salt_hex: hex(saltBytes), digest_hex: hex(digest),
    scheme: PASSWORD_SCHEME, scheme_version: PASSWORD_SCHEME_VERSION,
    params: { hash: 'SHA-256', iterations, length_bytes: 32 }
  };
}

async function passwordVerificationProof(password, account) {
  const scheme = upper(account?.password_scheme);
  const schemeVersion = Number(account?.password_scheme_version);
  const params = isObject(account?.password_params_json) ? account.password_params_json : {};
  const iterations = Number(params.iterations || PASSWORD_ITERATIONS);
  const lengthBytes = Number(params.length_bytes || 32);
  const hashName = upper(params.hash || 'SHA-256');
  const salt = bytesFromHex(account?.password_salt);
  const expected = bytesFromHex(account?.password_digest);
  const accountId = text(account?.id).toLowerCase();
  const verifierValid = scheme === PASSWORD_SCHEME
    && schemeVersion === PASSWORD_SCHEME_VERSION
    && Number.isSafeInteger(iterations) && iterations >= 50000 && iterations <= PASSWORD_ITERATIONS
    && lengthBytes === 32 && hashName === 'SHA-256'
    && salt.length >= 16 && expected.length === 32;
  if (!verifierValid) {
    return {
      matches: false,
      presented_password_digest_hex: null,
      expected_password_authority_sha256: null
    };
  }
  const presented = bytesFromHex(
    (await derivePasswordVerifier(password, salt, iterations)).digest_hex
  );
  let difference = presented.length ^ expected.length;
  for (let index = 0; index < expected.length; index += 1) {
    difference |= (presented[index] || 0) ^ expected[index];
  }
  const canonicalAuthority = UUID_RE.test(accountId) ? [
    'CANDIDATE_PASSWORD_AUTHORITY_V1', accountId, scheme, String(schemeVersion),
    hex(salt), hex(expected), hashName, String(iterations), String(lengthBytes)
  ].join('\n') : null;
  return {
    matches: difference === 0,
    presented_password_digest_hex: hex(presented),
    expected_password_authority_sha256: canonicalAuthority
      ? await sha256Hex(canonicalAuthority) : null
  };
}

async function verifyPassword(password, account) {
  return (await passwordVerificationProof(password, account)).matches;
}

function sessionExpiries(now = new Date()) {
  return {
    expires_at_utc: new Date(now.getTime() + REFRESH_TTL_DAYS * 86400000).toISOString(),
    absolute_expires_at_utc: new Date(now.getTime() + REFRESH_ABSOLUTE_TTL_DAYS * 86400000).toISOString()
  };
}

function safeSessionResponse(sessionResult, accessToken, refreshToken) {
  return {
    ok: true,
    access_token: accessToken,
    access_token_type: 'Bearer',
    access_expires_in_seconds: ACCESS_TTL_SECONDS,
    refresh_token: refreshToken,
    session_id: sessionResult.session_id,
    rotation: Number(sessionResult.rotation || 0),
    selected_candidate_id: sessionResult.selected_candidate_id || null,
    selection_required: sessionResult.selection_required === true,
    candidate_ids: Array.isArray(sessionResult.candidate_ids) ? sessionResult.candidate_ids : undefined,
    issued_at_utc: sessionResult.issued_at_utc,
    expires_at_utc: sessionResult.expires_at_utc,
    absolute_expires_at_utc: sessionResult.absolute_expires_at_utc,
    public_credential_versions: isObject(sessionResult.public_credential_versions)
      ? sessionResult.public_credential_versions : undefined
  };
}

async function selectedCandidateSessionResponse(env, claims, sessionId, rotation, result) {
  const issuedAtUtc = new Date(Number(claims.iat) * 1000).toISOString();
  const accessToken = await createAccessToken(env, {
    session_id: sessionId,
    rotation: Number(rotation || 0),
    issued_at_utc: issuedAtUtc
  });
  return {
    ...result,
    access_token: accessToken,
    access_expires_in_seconds: ACCESS_TTL_SECONDS,
    issued_at_utc: issuedAtUtc
  };
}

async function rpcCall(deps, name, args, options = undefined) {
  const result = await deps.rpc(name, args, options);
  return unwrapRpc(result, name);
}

function publicAppBase(request, env) {
  const configured = text(env.CANDIDATE_APP_PUBLIC_URL).replace(/\/$/, '');
  if (!configured || !/^https:\/\//i.test(configured)) {
    throw new CandidateHttpError(503, 'CANDIDATE_PUBLIC_URL_UNAVAILABLE');
  }
  return configured;
}

function deferBackground(ctx, promise, label, details = {}) {
  const guarded = Promise.resolve(promise).catch((error) => {
    console.error('[candidate-app] background task failed', {
      label,
      error_code: knownErrorCode(error),
      ...details
    });
    return { ok: false, error_code: knownErrorCode(error) };
  });
  if (ctx?.waitUntil) {
    ctx.waitUntil(guarded);
    return true;
  }
  return guarded;
}

async function finaliseReceivedPaperReturn(result, finalise) {
  try {
    const finalisation = await finalise();
    return {
      status: 200,
      body: {
        ...result,
        finalisation,
        finalisation_pending: finalisation?.finalisation_pending === true,
        canonical_processing_attempted: true
      }
    };
  } catch (error) {
    return {
      status: 202,
      body: {
        ...result,
        finalisation_pending: true,
        canonical_processing_attempted: true,
        retry_required: true,
        retry_error_code: knownErrorCode(error)
      }
    };
  }
}

async function queueChallengeMail(env, request, result, purpose, email, token) {
  const purposeText = purpose === 'ACTIVATE' ? 'activate your Candidate App account' : 'reset your Candidate App password';
  const route = purpose === 'ACTIVATE' ? 'activate' : 'reset-password';
  const link = `${publicAppBase(request, env)}/candidate/${route}#token=${encodeURIComponent(token)}&challenge=${encodeURIComponent(result.challenge_id)}`;
  const htmlLink = link.replaceAll('&', '&amp;');
  const subject = purpose === 'ACTIVATE' ? 'Activate your CloudTMS Candidate App account' : 'Reset your CloudTMS Candidate App password';
  const bodyText = `Use the secure link below to ${purposeText}.\n\n${link}\n\nThis link expires at ${result.expires_at_utc}. If you did not request this, no action is required.`;
  const deterministicKey = `CANDIDATE_AUTH_${purpose}:${result.challenge_id}`;
  return restWrite(env, 'mail_outbox', 'POST', 'on_conflict=deterministic_outbox_key', {
    type: 'TIMESHEET_GENERAL', to: email, subject,
    body_html: `<p>Use the secure link below to ${purposeText}.</p><p><a href="${htmlLink}">${subject}</a></p><p>This link expires at ${result.expires_at_utc}. If you did not request this, no action is required.</p>`,
    body_text: bodyText, attachments: [], status: 'QUEUED',
    reference: `candidate-auth:${purpose.toLowerCase()}:${result.challenge_id}`,
    recipient_kind: 'CANDIDATE', context_kind: 'CANDIDATE_AUTH', context_id: null,
    email_type: 'CANDIDATE_APP_TRANSACTIONAL', scheduled_for_utc: new Date().toISOString(),
    next_attempt_at_utc: new Date().toISOString(), deterministic_outbox_key: deterministicKey,
    payment_scope_json: {}
  }, 'resolution=ignore-duplicates,return=representation');
}

async function candidateAuthReceiptMetadata(
  deps, env, action, idempotencyKey, identities = {}, reservationMetadata = {}
) {
  const proposedKeyVersion = authReplayKeyVersion(env);
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: action,
    p_environment: environmentName(env),
    p_account_id: identities.account_id || null,
    p_email_normalized: identities.email_normalized || null,
    p_session_id: identities.session_id || null,
    p_selected_candidate_id: identities.selected_candidate_id || null,
    p_payload: {
      replay_probe_only: true,
      reserve_request_key_version: true,
      idempotency_key_version: proposedKeyVersion,
      ...reservationMetadata
    },
    p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  });
  if (result?.request_version_reserved !== true) {
    throw new CandidateHttpError(503, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
  }
  const keyVersion = requireInteger(
    result?.request_key_version || proposedKeyVersion,
    'CANDIDATE_REPLAY_KEY_VERSION_INVALID', 1
  );
  // A reservation remains usable during a rolling writer change only while
  // its exact version is intentionally retained by the private Worker.
  authReplaySecret(env, keyVersion);
  return { ...result, request_key_version: keyVersion };
}

async function candidateAuthExactReplay(
  deps, env, action, idempotencyKey, requestSha256, keyVersion, identities = {}
) {
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: action,
    p_environment: environmentName(env),
    p_account_id: identities.account_id || null,
    p_email_normalized: identities.email_normalized || null,
    p_session_id: identities.session_id || null,
    p_selected_candidate_id: identities.selected_candidate_id || null,
    p_payload: {
      replay_probe_only: true,
      idempotency_request_sha256: requestSha256,
      idempotency_key_version: keyVersion
    },
    p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  });
  return result?.idempotent_replay === true ? result : null;
}

async function candidateAuthReplayAfterPreconditionFailure(
  deps, env, action, idempotencyKey, requestSha256, keyVersion, identities, error
) {
  const errorCode = knownErrorCode(error);
  if (![
    'CANDIDATE_SESSION_INVALID', 'CANDIDATE_SESSION_EXPIRED',
    'CANDIDATE_ACCESS_TOKEN_INVALID', 'CANDIDATE_ACCESS_TOKEN_EXPIRED',
    'CANDIDATE_LOGIN_INVALID'
  ].includes(errorCode)) throw error;
  const replay = await candidateAuthExactReplay(
    deps, env, action, idempotencyKey, requestSha256, keyVersion, identities
  );
  if (replay) return replay;
  throw error;
}

async function refreshTokenForSessionResult(env, fallbackKeyVersion, action, result, idempotencyKey) {
  const resultKeyVersion = requireInteger(
    result?.token_key_version || fallbackKeyVersion,
    'CANDIDATE_REPLAY_KEY_VERSION_INVALID', 1
  );
  return deterministicRefreshToken(
    env, resultKeyVersion, action, result?.session_id, idempotencyKey
  );
}

async function challengeTokenForReceipt(
  env, purpose, email, challengeId, idempotencyKey, expectedHashHex, recordedKeyVersion
) {
  const keyVersion = requireInteger(
    recordedKeyVersion, 'CANDIDATE_REPLAY_KEY_VERSION_INVALID', 1
  );
  if (!challengeReadVersions(env).includes(keyVersion)) {
    throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_SECRET_VERSION_UNAVAILABLE');
  }
  const token = await deterministicOpaqueToken(
    challengeSecretForVersion(env, keyVersion), 'candidate-auth-challenge-v1',
    environmentName(env), purpose, email, challengeId || '', idempotencyKey
  );
  if (await sha256Hex(token) !== expectedHashHex) {
    throw new CandidateHttpError(503, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
  }
  return token;
}

async function phoneTokenForWorkflowResult(
  env, result, workflowId, expectedGeneration, idempotencyKey
) {
  const expectedHashHex = text(result?.approval_token_hash_hex).toLowerCase();
  if (!SHA256_RE.test(expectedHashHex)) {
    throw new CandidateHttpError(503, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
  }
  const keyVersion = requireInteger(
    result?.handoff_token_key_version,
    'CANDIDATE_REPLAY_KEY_VERSION_INVALID', 1
  );
  if (keyVersion > 32) {
    throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_KEY_VERSION_INVALID');
  }
  const token = await deterministicOpaqueToken(
    managerTokenSecret(env, keyVersion),
    'candidate-phone-handoff-v1', workflowId, expectedGeneration, idempotencyKey
  );
  if (await sha256Hex(token) !== expectedHashHex) {
    throw new CandidateHttpError(503, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
  }
  const { approval_token_hash_hex: _internalTokenHash, ...publicResult } = result;
  return { ...publicResult, manager_handoff_token: token };
}

async function handleChallengeStart(request, env, deps, isResend = false) {
  const body = await readJson(request);
  const email = normaliseEmail(body.email);
  const purpose = upper(body.purpose || 'ACTIVATE');
  if (!['ACTIVATE', 'RESET', 'RECOVERY'].includes(purpose)) throw new CandidateHttpError(400, 'CANDIDATE_CHALLENGE_PURPOSE_INVALID');
  const idempotencyKey = requireCandidateIdempotency(body.idempotency_key);
  const challengeId = isResend
    ? requireUuid(body.challenge_id, 'CANDIDATE_CHALLENGE_INVALID') : null;
  const receipt = await rpcCall(deps, 'candidate_auth_challenge_transition_v1', {
    p_action: isResend ? 'RESEND' : 'START',
    p_environment: environmentName(env),
    p_email_normalized: email,
    p_purpose: purpose,
    p_challenge_id: challengeId,
    p_token_hash: null,
    p_token_key_version: null,
    p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  });
  const replayTokenHash = receipt?.replay_receipt_found === true
    ? text(receipt.token_hash_hex).toLowerCase() : '';
  if (receipt?.replay_receipt_found === true && !SHA256_RE.test(replayTokenHash)) {
    throw new CandidateHttpError(503, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
  }
  const replayTokenKeyVersion = receipt?.replay_receipt_found === true
    ? requireInteger(receipt.token_key_version, 'CANDIDATE_REPLAY_KEY_VERSION_INVALID', 1)
    : null;
  const tokenKeyVersion = replayTokenKeyVersion || challengeKeyVersion(env);
  const challengeToken = receipt?.replay_receipt_found === true
    ? null
    : await deterministicOpaqueToken(
      challengeSecretForVersion(env, tokenKeyVersion),
      'candidate-auth-challenge-v1',
      environmentName(env), purpose, email, challengeId || '', idempotencyKey
    );
  const tokenHashHex = replayTokenHash || hex(await sha256Bytes(challengeToken));
  const args = {
    p_action: isResend ? 'RESEND' : 'START', p_environment: environmentName(env),
    p_email_normalized: email, p_purpose: purpose,
    p_challenge_id: challengeId,
    p_token_hash: `\\x${tokenHashHex}`, p_idempotency_key: idempotencyKey,
    p_token_key_version: tokenKeyVersion,
    p_now_utc: new Date().toISOString()
  };
  const result = await rpcCall(deps, 'candidate_auth_challenge_transition_v1', args);
  if (result?.ok !== true) {
    const code = text(result?.error_code) || 'CANDIDATE_CHALLENGE_INVALID';
    if (['CANDIDATE_CHALLENGE_RESEND_TOO_SOON', 'CANDIDATE_CHALLENGE_RESEND_LIMIT'].includes(code)) {
      const retryAfter = Number(result?.retry_after_seconds || 0);
      throw new CandidateHttpError(429, code, code, {
        ...(Number.isSafeInteger(retryAfter) && retryAfter > 0
          ? { retry_after_seconds: retryAfter } : {}),
        terminal: result?.terminal === true
      });
    }
    throw new CandidateHttpError(400, code);
  }
  // The database transition and mail insert are deliberately separate durable authorities.
  // On a retry, the challenge RPC returns the same challenge with deliver_email=false;
  // the create-only deterministic outbox key makes this safe to retry after a
  // prior mail-insert failure without resetting queued or sent delivery truth.
  if (result?.challenge_id && result?.expires_at_utc) {
    const winningTokenHash = text(result.token_hash_hex).toLowerCase();
    if (!SHA256_RE.test(winningTokenHash)) {
      throw new CandidateHttpError(503, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
    }
    const winningTokenKeyVersion = requireInteger(
      result.token_key_version, 'CANDIDATE_REPLAY_KEY_VERSION_INVALID', 1
    );
    const deliveryToken = await challengeTokenForReceipt(
      env, purpose, email, challengeId, idempotencyKey,
      winningTokenHash, winningTokenKeyVersion
    );
    await queueChallengeMail(env, request, result, purpose, email, deliveryToken);
  }
  return jsonResponse(202, { ok: true, accepted: true });
}

async function handleChallengeVerify(request, env, deps) {
  const body = await readJson(request);
  const purpose = upper(body.purpose || 'ACTIVATE');
  const token = text(body.token);
  if (!token) throw new CandidateHttpError(400, 'CANDIDATE_CHALLENGE_INVALID');
  const idempotencyKey = requireCandidateIdempotency(body.idempotency_key);
  const result = await rpcCall(deps, 'candidate_auth_challenge_transition_v1', {
    p_action: 'VERIFY', p_environment: environmentName(env),
    p_email_normalized: normaliseEmail(body.email), p_purpose: purpose,
    p_challenge_id: body.challenge_id ? requireUuid(body.challenge_id) : null,
    p_token_hash: `\\x${await sha256Hex(token)}`, p_idempotency_key: idempotencyKey,
    p_token_key_version: null,
    p_now_utc: new Date().toISOString()
  });
  if (result?.ok !== true) throw new CandidateHttpError(401, result?.error_code || 'CANDIDATE_CHALLENGE_INVALID');
  return jsonResponse(200, {
    ok: true, challenge_id: result.challenge_id, purpose: result.purpose,
    expires_at_utc: result.expires_at_utc
  });
}

async function handlePasswordComplete(request, env, deps) {
  const body = await readJson(request);
  const credentialVersions = publicCredentialVersions(body.public_credential_versions);
  const idempotencyKey = requireCandidateIdempotency(body.idempotency_key);
  const challengeId = requireUuid(body.challenge_id, 'CANDIDATE_VERIFIED_CHALLENGE_REQUIRED');
  const selectedCandidateId = body.selected_candidate_id
    ? requireUuid(body.selected_candidate_id) : null;
  const deviceHash = body.device_id ? await sha256Hex(text(body.device_id)) : null;
  const metadata = await candidateAuthReceiptMetadata(
    deps, env, 'ACTIVATE_PASSWORD', idempotencyKey,
    { selected_candidate_id: selectedCandidateId }
  );
  const keyVersion = metadata.request_key_version;
  const requestSha256 = await authRequestSha256(env, keyVersion, 'ACTIVATE_PASSWORD', {
    challenge_id: challengeId,
    selected_candidate_id: selectedCandidateId,
    password_proof: await authSecretProof(env, keyVersion, 'new-password', body.password),
    device_id_hash_hex: deviceHash,
    platform: text(body.platform).slice(0, 80) || null
  });
  if (metadata?.replay_receipt_found === true) {
    const replay = await candidateAuthExactReplay(
      deps, env, 'ACTIVATE_PASSWORD', idempotencyKey, requestSha256, keyVersion,
      { selected_candidate_id: selectedCandidateId }
    );
    if (!replay) throw new CandidateHttpError(409, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    const refreshToken = await deterministicRefreshToken(
      env, keyVersion, 'ACTIVATE_PASSWORD', replay.session_id, idempotencyKey
    );
    return jsonResponse(200, safeSessionResponse(
      replay, await createAccessToken(env, replay), refreshToken
    ));
  }
  const verifier = await derivePasswordVerifier(body.password);
  const sessionId = crypto.randomUUID();
  const refreshToken = await deterministicRefreshToken(
    env, keyVersion, 'ACTIVATE_PASSWORD', sessionId, idempotencyKey
  );
  const refreshHash = await sha256Hex(refreshToken);
  const now = new Date();
  const expiries = sessionExpiries(now);
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: 'ACTIVATE_PASSWORD', p_environment: environmentName(env),
    p_account_id: null, p_email_normalized: null, p_session_id: sessionId,
    p_selected_candidate_id: selectedCandidateId,
    p_payload: {
      challenge_id: challengeId,
      password_scheme: verifier.scheme, password_scheme_version: verifier.scheme_version,
      password_salt_hex: verifier.salt_hex, password_digest_hex: verifier.digest_hex,
      password_params: verifier.params, refresh_token_hash_hex: refreshHash,
      ...expiries, ...(deviceHash ? { device_id_hash_hex: deviceHash } : {}),
      platform: text(body.platform).slice(0, 80) || null,
      ...(credentialVersions ? { public_credential_versions: credentialVersions } : {}),
      idempotency_request_sha256: requestSha256,
      idempotency_key_version: keyVersion
    },
    p_idempotency_key: idempotencyKey, p_now_utc: now.toISOString()
  });
  const winningRefreshToken = await refreshTokenForSessionResult(
    env, keyVersion, 'ACTIVATE_PASSWORD', result, idempotencyKey
  );
  const accessToken = await createAccessToken(env, result);
  return jsonResponse(200, safeSessionResponse(result, accessToken, winningRefreshToken));
}

async function handleLogin(request, env, deps) {
  const body = await readJson(request);
  const credentialVersions = publicCredentialVersions(body.public_credential_versions);
  const email = normaliseEmail(body.email);
  const idempotencyKey = requireCandidateIdempotency(body.idempotency_key);
  const selectedCandidateId = body.selected_candidate_id
    ? requireUuid(body.selected_candidate_id) : null;
  const deviceHash = body.device_id ? await sha256Hex(text(body.device_id)) : null;
  const metadata = await candidateAuthReceiptMetadata(
    deps, env, 'LOGIN_SUCCESS', idempotencyKey,
    { email_normalized: email, selected_candidate_id: selectedCandidateId }
  );
  const keyVersion = metadata.request_key_version;
  const requestSha256 = await authRequestSha256(env, keyVersion, 'LOGIN_SUCCESS', {
    email_normalized: email,
    selected_candidate_id: selectedCandidateId,
    password_proof: await authSecretProof(env, keyVersion, 'login-password', body.password),
    device_id_hash_hex: deviceHash,
    platform: text(body.platform).slice(0, 80) || null
  });
  if (metadata?.replay_receipt_found === true) {
    const replay = await candidateAuthExactReplay(
      deps, env, 'LOGIN_SUCCESS', idempotencyKey, requestSha256, keyVersion,
      { email_normalized: email, selected_candidate_id: selectedCandidateId }
    );
    if (!replay) throw new CandidateHttpError(409, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    if (replay.ok !== true) {
      throw new CandidateHttpError(401, replay.error_code || 'CANDIDATE_LOGIN_INVALID');
    }
    const refreshToken = await deterministicRefreshToken(
      env, keyVersion, 'LOGIN_SUCCESS', replay.session_id, idempotencyKey
    );
    return jsonResponse(200, safeSessionResponse(
      replay, await createAccessToken(env, replay), refreshToken
    ));
  }
  const account = await restOne(env, 'candidate_app_accounts',
    `environment=eq.${encodeURIComponent(environmentName(env))}&email_normalized=eq.${encodeURIComponent(email)}` +
    '&select=id,environment,status,password_scheme,password_scheme_version,password_salt,password_digest,password_params_json,locked_until_utc');
  const passwordProof = await passwordVerificationProof(body.password, account || {
    password_scheme: PASSWORD_SCHEME,
    password_scheme_version: PASSWORD_SCHEME_VERSION,
    password_salt: '00'.repeat(16),
    password_digest: '00'.repeat(32),
    password_params_json: { hash: 'SHA-256', iterations: PASSWORD_ITERATIONS, length_bytes: 32 }
  });
  const passwordAuthorityPayload = account && passwordProof.expected_password_authority_sha256
    ? {
        presented_password_digest_hex: passwordProof.presented_password_digest_hex,
        expected_password_authority_sha256: passwordProof.expected_password_authority_sha256
      }
    : {};
  const passwordOk = passwordProof.matches;
  if (!passwordOk) {
    const failed = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
      p_action: 'LOGIN_SUCCESS', p_environment: environmentName(env), p_account_id: account?.id || null,
      p_email_normalized: email, p_session_id: null, p_selected_candidate_id: null,
      p_payload: {
        login_failed: true,
        ...passwordAuthorityPayload,
        ...(credentialVersions ? { public_credential_versions: credentialVersions } : {}),
        idempotency_request_sha256: requestSha256,
        idempotency_key_version: keyVersion
      }, p_idempotency_key: idempotencyKey, p_now_utc: new Date().toISOString()
    });
    if (failed?.ok !== false) {
      throw new CandidateHttpError(503, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
    }
    throw new CandidateHttpError(401, 'CANDIDATE_LOGIN_INVALID');
  }
  const sessionId = crypto.randomUUID();
  const refreshToken = await deterministicRefreshToken(
    env, keyVersion, 'LOGIN_SUCCESS', sessionId, idempotencyKey
  );
  const now = new Date();
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: 'LOGIN_SUCCESS', p_environment: environmentName(env), p_account_id: account.id,
    p_email_normalized: email, p_session_id: sessionId,
    p_selected_candidate_id: selectedCandidateId,
    p_payload: {
      refresh_token_hash_hex: await sha256Hex(refreshToken), ...sessionExpiries(now),
      ...passwordAuthorityPayload,
      ...(deviceHash ? { device_id_hash_hex: deviceHash } : {}),
      platform: text(body.platform).slice(0, 80) || null,
      ...(credentialVersions ? { public_credential_versions: credentialVersions } : {}),
      idempotency_request_sha256: requestSha256,
      idempotency_key_version: keyVersion
    },
    p_idempotency_key: idempotencyKey, p_now_utc: now.toISOString()
  });
  if (result?.ok !== true) {
    throw new CandidateHttpError(401, result?.error_code || 'CANDIDATE_LOGIN_INVALID');
  }
  const winningRefreshToken = await refreshTokenForSessionResult(
    env, keyVersion, 'LOGIN_SUCCESS', result, idempotencyKey
  );
  return jsonResponse(200, safeSessionResponse(
    result, await createAccessToken(env, result), winningRefreshToken
  ));
}

async function handleRefresh(request, env, deps) {
  const body = await readJson(request);
  const credentialVersions = publicCredentialVersions(body.public_credential_versions);
  const oldRefresh = text(body.refresh_token);
  if (!oldRefresh) throw new CandidateHttpError(401, 'CANDIDATE_SESSION_INVALID');
  const oldSessionId = requireUuid(body.session_id, 'CANDIDATE_SESSION_INVALID');
  const idempotencyKey = requireCandidateIdempotency(body.idempotency_key);
  const metadata = await candidateAuthReceiptMetadata(
    deps, env, 'REFRESH_SESSION', idempotencyKey, { session_id: oldSessionId }
  );
  const keyVersion = metadata.request_key_version;
  const requestSha256 = await authRequestSha256(env, keyVersion, 'REFRESH_SESSION', {
    session_id: oldSessionId,
    presented_refresh_token_proof: await authSecretProof(
      env, keyVersion, 'presented-refresh-token', oldRefresh
    )
  });
  if (metadata?.replay_receipt_found === true) {
    const replay = await candidateAuthExactReplay(
      deps, env, 'REFRESH_SESSION', idempotencyKey, requestSha256, keyVersion,
      { session_id: oldSessionId }
    );
    if (!replay) throw new CandidateHttpError(409, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    if (replay?.ok !== true) {
      throw new CandidateHttpError(401, replay?.error_code || 'CANDIDATE_SESSION_INVALID');
    }
    const replayRefresh = await deterministicRefreshToken(
      env, keyVersion, 'REFRESH_SESSION', replay.session_id, idempotencyKey
    );
    return jsonResponse(200, safeSessionResponse(
      replay, await createAccessToken(env, replay), replayRefresh
    ));
  }
  const newSessionId = crypto.randomUUID();
  const newRefresh = await deterministicRefreshToken(
    env, keyVersion, 'REFRESH_SESSION', newSessionId, idempotencyKey
  );
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: 'REFRESH_SESSION', p_environment: environmentName(env),
    p_account_id: null, p_email_normalized: null,
    p_session_id: oldSessionId, p_selected_candidate_id: null,
    p_payload: {
      presented_refresh_token_hash_hex: await sha256Hex(oldRefresh),
      new_refresh_token_hash_hex: await sha256Hex(newRefresh), new_session_id: newSessionId,
      ...(credentialVersions ? { public_credential_versions: credentialVersions } : {}),
      idempotency_request_sha256: requestSha256,
      idempotency_key_version: keyVersion
    }, p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  });
  if (result?.ok !== true) throw new CandidateHttpError(401, result?.error_code || 'CANDIDATE_SESSION_INVALID');
  const winningRefreshToken = await refreshTokenForSessionResult(
    env, keyVersion, 'REFRESH_SESSION', result, idempotencyKey
  );
  return jsonResponse(200, safeSessionResponse(
    result, await createAccessToken(env, result), winningRefreshToken
  ));
}

async function handleAccountAction(request, env, deps, action, routeIdentity = {}) {
  const body = request.method === 'GET' ? {} : await readJson(request);
  const idempotencyKey = requireCandidateIdempotency(body.idempotency_key);
  const claims = await candidateAccessClaims(request, env);
  let payload = {};
  let selectedCandidateId = null;
  let pushTokenCiphertext = null;
  let pushEncryptionKeyVersion = null;
  let pushIdentityKeyVersion = null;
  let pushIdentityProofCatalog = null;
  let requestIdentity = { session_id: claims.sid };
  if (action === 'SELECT_TEST_CANDIDATE') {
    selectedCandidateId = requireUuid(body.selected_candidate_id, 'CANDIDATE_SELECTION_NOT_ALLOWED');
    requestIdentity = { ...requestIdentity, selected_candidate_id: selectedCandidateId };
  } else if (action === 'SET_NOTIFICATION_PREFERENCES') {
    body.notification_preferences = requireCandidateNotificationPreferences(
      body.notification_preferences
    );
    requestIdentity = { ...requestIdentity, notification_preferences: body.notification_preferences };
  } else if (action === 'MARK_NOTIFICATION_READ') {
    const notificationId = requireUuid(
      routeIdentity.notification_id,
      'CANDIDATE_NOTIFICATION_NOT_FOUND'
    );
    requestIdentity = { ...requestIdentity, notification_id: notificationId };
  } else if (action === 'REGISTER_PUSH_TOKEN') {
    const ciphertext = text(body.push_token_ciphertext_hex);
    const tokenIdentityHmac = text(body.push_token_identity_hmac).toLowerCase();
    const tokenIdentityKeyVersion = Number(body.push_token_identity_key_version);
    const provider = upper(body.push_provider);
    const keyVersion = Number(body.push_key_version);
    if (!/^[0-9a-f]{58,32768}$/i.test(ciphertext) || ciphertext.length % 2 !== 0
        || !SHA256_RE.test(tokenIdentityHmac)
        || !Number.isSafeInteger(tokenIdentityKeyVersion) || tokenIdentityKeyVersion < 1
        || !['APNS', 'FCM', 'WEB_PUSH'].includes(provider)
        || !Number.isSafeInteger(keyVersion) || keyVersion < 1 || keyVersion > 32767) {
      throw new CandidateHttpError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
    }
    pushTokenCiphertext = ciphertext;
    pushEncryptionKeyVersion = keyVersion;
    pushIdentityKeyVersion = tokenIdentityKeyVersion;
    pushIdentityProofCatalog = publicPushIdentityProofs(
      body.push_token_identity_proofs, tokenIdentityHmac, tokenIdentityKeyVersion
    );
    requestIdentity = {
      ...requestIdentity,
      push_provider: provider
    };
  } else if (action === 'CHANGE_PASSWORD') {
    requestIdentity = { ...requestIdentity, password_change: true };
  }
  const metadata = await candidateAuthReceiptMetadata(
    deps, env, action, idempotencyKey,
    { session_id: claims.sid, selected_candidate_id: selectedCandidateId },
    action === 'REGISTER_PUSH_TOKEN'
      ? { push_token_identity_key_version: pushIdentityKeyVersion }
      : {}
  );
  const keyVersion = metadata.request_key_version;
  if (action === 'REGISTER_PUSH_TOKEN') {
    const semanticIdentityVersion = Number(
      metadata.push_token_identity_key_version || pushIdentityKeyVersion
    );
    const semanticIdentity = pushIdentityProofCatalog.find(
      proof => proof.key_version === semanticIdentityVersion
    );
    if (!semanticIdentity) {
      throw new CandidateHttpError(503, 'CANDIDATE_REPLAY_SECRET_VERSION_UNAVAILABLE');
    }
    requestIdentity = {
      ...requestIdentity,
      push_token_identity_hmac: semanticIdentity.identity_hmac,
      push_token_identity_key_version: semanticIdentity.key_version
    };
  }
  if (action === 'CHANGE_PASSWORD') {
    requestIdentity = {
      ...requestIdentity,
      current_password_proof: await authSecretProof(
        env, keyVersion, 'current-password', body.current_password
      ),
      new_password_proof: await authSecretProof(
        env, keyVersion, 'new-password', body.password
      )
    };
  }
  const requestSha256 = await authRequestSha256(env, keyVersion, action, requestIdentity);
  if (metadata?.replay_receipt_found === true) {
    const replay = await candidateAuthExactReplay(
      deps, env, action, idempotencyKey, requestSha256, keyVersion,
      { session_id: claims.sid, selected_candidate_id: selectedCandidateId }
    );
    if (!replay) throw new CandidateHttpError(409, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    if (action === 'SELECT_TEST_CANDIDATE') {
      return jsonResponse(200, await selectedCandidateSessionResponse(
        env, claims, claims.sid, claims.rot, replay
      ));
    }
    if (action === 'CHANGE_PASSWORD' && replay?.ok !== true) {
      throw new CandidateHttpError(401, replay?.error_code || 'CANDIDATE_LOGIN_INVALID');
    }
    return jsonResponse(200, replay);
  }

  let access;
  try {
    access = await verifyCandidateAccess(request, env);
  } catch (error) {
    const replay = await candidateAuthReplayAfterPreconditionFailure(
      deps, env, action, idempotencyKey, requestSha256, keyVersion,
      { session_id: claims.sid, selected_candidate_id: selectedCandidateId }, error
    );
    if (action === 'SELECT_TEST_CANDIDATE') {
      return jsonResponse(200, await selectedCandidateSessionResponse(
        env, claims, claims.sid, claims.rot, replay
      ));
    }
    return jsonResponse(200, replay);
  }
  if (action === 'SET_NOTIFICATION_PREFERENCES') {
    payload = { notification_preferences: body.notification_preferences };
  } else if (action === 'MARK_NOTIFICATION_READ') {
    payload = { notification_id: requestIdentity.notification_id };
  } else if (action === 'REGISTER_PUSH_TOKEN') {
    payload = {
      push_provider: requestIdentity.push_provider,
      push_token_ciphertext_hex: pushTokenCiphertext,
      push_key_version: pushEncryptionKeyVersion,
      push_token_identity_hmac: requestIdentity.push_token_identity_hmac,
      push_token_identity_key_version: requestIdentity.push_token_identity_key_version
    };
  } else if (action === 'CHANGE_PASSWORD') {
    const account = await restOne(env, 'candidate_app_accounts',
      `id=eq.${encodeURIComponent(access.account_id)}` +
      '&select=id,password_scheme,password_scheme_version,password_salt,password_digest,password_params_json');
    const currentPasswordProof = await passwordVerificationProof(body.current_password, account);
    const verifier = await derivePasswordVerifier(body.password);
    payload = {
      password_scheme: verifier.scheme, password_scheme_version: verifier.scheme_version,
      password_salt_hex: verifier.salt_hex, password_digest_hex: verifier.digest_hex,
      password_params: verifier.params,
      presented_password_digest_hex: currentPasswordProof.presented_password_digest_hex,
      expected_password_authority_sha256: currentPasswordProof.expected_password_authority_sha256
    };
  }
  payload = {
    ...payload,
    idempotency_request_sha256: requestSha256,
    idempotency_key_version: keyVersion
  };
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: action, p_environment: access.environment, p_account_id: access.account_id,
    p_email_normalized: null, p_session_id: access.session_id,
    p_selected_candidate_id: selectedCandidateId, p_payload: payload,
    p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  });
  if (action === 'CHANGE_PASSWORD' && result?.ok !== true) {
    throw new CandidateHttpError(401, result?.error_code || 'CANDIDATE_LOGIN_INVALID');
  }
  if (action === 'SELECT_TEST_CANDIDATE') {
    return jsonResponse(200, await selectedCandidateSessionResponse(
      env, claims, access.session_id, access.rotation, result
    ));
  }
  return jsonResponse(200, result);
}

function componentMediaTypes(kind) {
  const allowed = COMPONENT_MEDIA_TYPES[upper(kind)];
  if (!allowed) throw new CandidateHttpError(400, 'CANDIDATE_COMPONENT_KIND_INVALID');
  return allowed;
}

function positiveLimit(value, fallback, minimum, maximum) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= minimum && parsed <= maximum ? parsed : fallback;
}

function uploadLimits(env) {
  return {
    bytes: positiveLimit(env.CANDIDATE_MAX_UPLOAD_BYTES, MAX_COMPONENT_BYTES, 1024, MAX_COMPONENT_BYTES),
    dimension: positiveLimit(env.CANDIDATE_MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION, 256, 30000),
    pixels: positiveLimit(env.CANDIDATE_MAX_IMAGE_PIXELS, MAX_IMAGE_PIXELS, 65_536, 120_000_000)
  };
}

function containsAscii(bytes, value) {
  const pattern = encoder.encode(value);
  outer: for (let index = 0; index <= bytes.length - pattern.length; index += 1) {
    for (let offset = 0; offset < pattern.length; offset += 1) {
      if (bytes[index + offset] !== pattern[offset]) continue outer;
    }
    return true;
  }
  return false;
}

async function validateComponentBytes(bytes, mediaType, env = {}) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const type = normaliseMediaType(mediaType);
  const limits = uploadLimits(env);
  if (!source.byteLength || source.byteLength > limits.bytes) {
    throw new CandidateHttpError(413, 'CANDIDATE_COMPONENT_SIZE_INVALID');
  }
  if (type === 'application/pdf') {
    const header = decoder.decode(source.slice(0, Math.min(1024, source.byteLength)));
    if (!header.includes('%PDF-') || containsAscii(source, '/Encrypt')) {
      throw new CandidateHttpError(415, 'CANDIDATE_SOURCE_PDF_INVALID');
    }
    let document;
    try {
      document = await PDFDocument.load(source, { ignoreEncryption: false, updateMetadata: false });
    } catch {
      throw new CandidateHttpError(415, 'CANDIDATE_SOURCE_PDF_INVALID');
    }
    if (document.getPageCount() !== 1) {
      throw new CandidateHttpError(400, 'CANDIDATE_SOURCE_PDF_ONE_PAGE_REQUIRED');
    }
    return { media_type: type, page_count: 1, width: null, height: null };
  }
  if (type === 'image/png') {
    const signature = [137, 80, 78, 71, 13, 10, 26, 10];
    if (source.length < signature.length || signature.some((value, index) => source[index] !== value)) {
      throw new CandidateHttpError(415, 'CANDIDATE_SOURCE_IMAGE_INVALID');
    }
  } else if (type === 'image/jpeg') {
    if (source.length < 4 || source[0] !== 0xff || source[1] !== 0xd8
        || source[source.length - 2] !== 0xff || source[source.length - 1] !== 0xd9) {
      throw new CandidateHttpError(415, 'CANDIDATE_SOURCE_IMAGE_INVALID');
    }
  } else {
    throw new CandidateHttpError(415, 'CANDIDATE_COMPONENT_MEDIA_TYPE_INVALID');
  }
  try {
    const document = await PDFDocument.create({ updateMetadata: false });
    const image = type === 'image/png' ? await document.embedPng(source) : await document.embedJpg(source);
    const width = Number(image.width);
    const height = Number(image.height);
    if (!Number.isFinite(width) || !Number.isFinite(height) || width < 1 || height < 1
        || width > limits.dimension || height > limits.dimension || width * height > limits.pixels) {
      throw new CandidateHttpError(413, 'CANDIDATE_SOURCE_IMAGE_DIMENSIONS_INVALID');
    }
    return { media_type: type, page_count: 1, width, height };
  } catch (error) {
    if (error instanceof CandidateHttpError) throw error;
    throw new CandidateHttpError(415, 'CANDIDATE_SOURCE_IMAGE_INVALID');
  }
}

function extensionForMedia(mediaType) {
  const extension = {
    'image/png': 'png', 'image/jpeg': 'jpg', 'application/pdf': 'pdf'
  }[normaliseMediaType(mediaType)];
  if (!extension) throw new CandidateHttpError(415, 'CANDIDATE_COMPONENT_MEDIA_TYPE_INVALID');
  return extension;
}

function componentStorageKey(environment, workflowId, generation, componentKind, mediaType) {
  const extension = extensionForMedia(mediaType);
  return `candidate-app/${environment.toLowerCase()}/${workflowId}/${generation}/source/${upper(componentKind).toLowerCase()}-${crypto.randomUUID()}.${extension}`;
}

function preparedUploadContract(result, expected) {
  const contract = {
    component_id: requireUuid(result?.component_id, 'CANDIDATE_COMPONENT_PREPARE_FAILED'),
    storage_key: text(result?.storage_key),
    media_type: normaliseMediaType(result?.media_type),
    byte_size: Number(result?.byte_size),
    component_kind: upper(result?.component_kind),
    document_role: upper(result?.document_role),
    expense_category: result?.expense_category == null ? null : upper(result.expense_category),
    paper_return_page_key: result?.paper_return_page_key == null ? null : text(result.paper_return_page_key),
    workflow_generation: Number(result?.workflow_generation),
    state: upper(result?.state)
  };
  if (!contract.storage_key
      || contract.media_type !== expected.media_type
      || contract.byte_size !== expected.byte_size
      || contract.component_kind !== expected.component_kind
      || contract.document_role !== expected.document_role
      || contract.expense_category !== expected.expense_category
      || contract.paper_return_page_key !== expected.paper_return_page_key
      || !Number.isSafeInteger(contract.workflow_generation)
      || contract.workflow_generation !== expected.workflow_generation
      || !['PENDING', 'IMMUTABLE'].includes(contract.state)) {
    throw new CandidateHttpError(409, 'CANDIDATE_COMPONENT_PREPARE_CONTRACT_MISMATCH');
  }
  return contract;
}

async function uploadTicket(env, payload) {
  const now = Math.floor(Date.now() / 1000);
  return sealEnvelope(env.CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET, 'candidate-component-upload-v2', {
    typ: 'candidate_component_upload', aud: 'cloudtms-component-upload-v2',
    iat: now, exp: now + 10 * 60, nonce: crypto.randomUUID(), ...payload
  });
}

async function verifyUploadTicket(env, value) {
  const payload = await openEnvelope(
    env.CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET,
    'candidate-component-upload-v2',
    value
  );
  if (!payload || payload.typ !== 'candidate_component_upload'
      || payload.aud !== 'cloudtms-component-upload-v2'
      || !['CANDIDATE_SESSION', 'MANAGER_EMAIL', 'MANAGER_PHONE'].includes(payload.authority_kind)
      || Number(payload.exp) <= Math.floor(Date.now() / 1000)) {
    throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_INVALID');
  }
  return payload;
}

function managerRouteAuthority(request) {
  const authorityKind = upper(request.headers.get('x-cloudtms-manager-route-authority'));
  if (!['MANAGER_EMAIL', 'MANAGER_PHONE'].includes(authorityKind)) {
    throw new CandidateHttpError(401, 'MANAGER_ROUTE_CONTEXT_INVALID');
  }
  if (authorityKind === 'MANAGER_PHONE') return { authority_kind: authorityKind };
  const authority = {
    authority_kind: authorityKind,
    manager_route_ticket_id: text(request.headers.get('x-cloudtms-manager-route-ticket')).toLowerCase(),
    route_revision: Number(request.headers.get('x-cloudtms-manager-route-revision')),
    workflow_route_hmac: text(request.headers.get('x-cloudtms-manager-route-workflow-hmac')).toLowerCase(),
    approval_request_route_hmac: text(request.headers.get('x-cloudtms-manager-route-request-hmac')).toLowerCase(),
    request_generation: Number(request.headers.get('x-cloudtms-manager-route-request-generation')),
    credential_generation: Number(request.headers.get('x-cloudtms-manager-route-credential-generation'))
  };
  if (!UUID_RE.test(authority.manager_route_ticket_id)
      || !Number.isSafeInteger(authority.route_revision) || authority.route_revision < 1
      || !SHA256_RE.test(authority.workflow_route_hmac)
      || !SHA256_RE.test(authority.approval_request_route_hmac)
      || !Number.isSafeInteger(authority.request_generation) || authority.request_generation < 1
      || !Number.isSafeInteger(authority.credential_generation)
      || authority.credential_generation < 1) {
    throw new CandidateHttpError(401, 'MANAGER_ROUTE_CONTEXT_INVALID');
  }
  return authority;
}

async function assertManagerRouteWorkflow(env, workflowId, authority) {
  if (authority.authority_kind !== 'MANAGER_EMAIL') return;
  const expected = await requestHmacSha256(
    managerRouteHmacSecret(env), 'manager-email-workflow-v1', workflowId
  );
  if (expected !== authority.workflow_route_hmac) {
    throw new CandidateHttpError(401, 'MANAGER_ROUTE_CONTEXT_INVALID');
  }
}

async function assertManagerRouteResult(env, result, authority) {
  if (authority?.authority_kind !== 'MANAGER_EMAIL') return;
  const requestId = requireUuid(
    result?.approval_request_id, 'MANAGER_ROUTE_CONTEXT_INVALID'
  );
  const expected = await requestHmacSha256(
    managerRouteHmacSecret(env), 'manager-email-request-v1', requestId
  );
  if (expected !== authority.approval_request_route_hmac
      || Number(result?.approval_request_generation || result?.request_generation)
        !== authority.request_generation) {
    throw new CandidateHttpError(401, 'MANAGER_ROUTE_CONTEXT_INVALID');
  }
}

function routedManagerControlRpc(env, deps, schema, functionName, args) {
  return typeof deps?.controlPlaneRpc === 'function'
    ? deps.controlPlaneRpc(schema, functionName, args)
    : controlPlaneRpc(env, schema, functionName, args);
}

function completeManagerEmailRoute(env, deps, authority, ctx) {
  if (authority?.authority_kind !== 'MANAGER_EMAIL') return;
  const completion = routedManagerControlRpc(
    env, deps, 'control', 'manager_email_route_transition_v1', {
    p_transition: {
      manager_route_ticket_id: authority.manager_route_ticket_id,
      expected_route_revision: authority.route_revision,
      target_state: 'COMPLETED'
    }
  });
  if (ctx && typeof ctx.waitUntil === 'function') {
    ctx.waitUntil(completion.catch(() => null));
  } else {
    completion.catch(() => null);
  }
}

async function currentManagerEmailRouteTickets(env, workflowId) {
  try {
    const rows = await restRows(env, 'candidate_manager_email_route_receipts',
      `workflow_id=eq.${encodeURIComponent(requireUuid(workflowId))}&state=eq.CURRENT`
      + '&select=manager_route_ticket_id,route_revision&limit=100');
    return rows.filter(row => UUID_RE.test(text(row.manager_route_ticket_id))
      && Number.isSafeInteger(Number(row.route_revision)) && Number(row.route_revision) >= 1);
  } catch {
    return [];
  }
}

function retireManagerEmailRoutes(env, deps, routes, ctx) {
  if (!Array.isArray(routes) || !routes.length) return;
  const retirement = Promise.allSettled(routes.map(route => routedManagerControlRpc(
    env, deps, 'control', 'manager_email_route_transition_v1', {
      p_transition: {
        manager_route_ticket_id: route.manager_route_ticket_id,
        expected_route_revision: Number(route.route_revision), target_state: 'RETIRED'
      }
    }
  )));
  if (ctx && typeof ctx.waitUntil === 'function') ctx.waitUntil(retirement);
  else retirement.catch(() => null);
}

function exactKeys(value, required, optional = []) {
  if (!isObject(value)) return false;
  const permitted = new Set([...required, ...optional]);
  const keys = Object.keys(value);
  return required.every((key) => Object.prototype.hasOwnProperty.call(value, key))
    && keys.every((key) => permitted.has(key));
}

async function validateCandidatePaperReturnProof(env, deps, access, workflowId, generation, body) {
  const proof = body.signed_return_proof;
  if (!exactKeys(proof, [
    'proof_contract_version', 'paper_return_manifest_sha256',
    'paper_return_page_key', 'detected_qr_count'
  ], ['qr_text'])) {
    throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_PROOF_REQUIRED');
  }
  if (proof.proof_contract_version !== PAPER_RETURN_PROOF_VERSION
      || !SHA256_RE.test(text(proof.paper_return_manifest_sha256))
      || text(proof.paper_return_page_key) !== text(body.paper_return_page_key)
      || !Number.isSafeInteger(proof.detected_qr_count)
      || proof.detected_qr_count < 0 || proof.detected_qr_count > 1) {
    throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_PROOF_INVALID');
  }
  let decodedToken = null;
  if (proof.detected_qr_count === 1) {
    if (!text(proof.qr_text)) {
      throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_UNREADABLE');
    }
    try {
      decodedToken = (await verifyCandidatePaperQrViaAdapter(env, proof.qr_text)).tok;
    } catch (error) {
      const code = text(error?.code || error?.message);
      if (code === 'TSQ1_SIGNING_SECRET_MISSING') {
        throw new CandidateHttpError(503, 'CANDIDATE_PAPER_QR_CONFIGURATION_UNAVAILABLE');
      }
      throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_UNREADABLE');
    }
  } else if (Object.prototype.hasOwnProperty.call(proof, 'qr_text')) {
    throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_PROOF_INVALID');
  }
  const result = await rpcCall(deps, 'candidate_paper_return_proof_validate_v1', {
    p_session_id: access.session_id,
    p_environment: access.environment || environmentName(env),
    p_workflow_id: workflowId,
    p_expected_generation: generation,
    p_manifest_sha256_hex: text(proof.paper_return_manifest_sha256).toLowerCase(),
    p_page_key: text(proof.paper_return_page_key),
    p_qr_token: decodedToken,
    p_qr_token_sha256_hex: null,
    p_now_utc: new Date().toISOString()
  });
  if (result?.proof_contract_version !== PAPER_RETURN_PROOF_VERSION
      || result?.workflow_id !== workflowId
      || Number(result?.workflow_generation) !== generation
      || result?.paper_return_page_key !== text(proof.paper_return_page_key)
      || !SHA256_RE.test(text(result?.proof_receipt_sha256))
      || (result?.qr_required === true && !SHA256_RE.test(text(result?.qr_token_sha256)))) {
    throw new CandidateHttpError(503, 'CANDIDATE_PAPER_QR_PROOF_UNAVAILABLE');
  }
  if (result.qr_required === true && proof.detected_qr_count !== 1) {
    throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_UNREADABLE');
  }
  if (result.qr_required !== true && proof.detected_qr_count !== 0) {
    throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_PROOF_FORBIDDEN');
  }
  return result;
}

async function revalidateCandidatePaperReturnProof(env, deps, ticket, sessionId) {
  if (!ticket.paper_return_proof) return;
  const proof = ticket.paper_return_proof;
  const result = await rpcCall(deps, 'candidate_paper_return_proof_validate_v1', {
    p_session_id: sessionId,
    p_environment: ticket.env,
    p_workflow_id: ticket.workflow_id,
    p_expected_generation: Number(ticket.generation),
    p_manifest_sha256_hex: proof.paper_return_manifest_sha256,
    p_page_key: proof.paper_return_page_key,
    p_qr_token: null,
    p_qr_token_sha256_hex: proof.qr_token_sha256 || null,
    p_now_utc: new Date().toISOString()
  });
  if (result?.proof_receipt_sha256 !== proof.proof_receipt_sha256
      || result?.paper_return_page_key !== proof.paper_return_page_key
      || Boolean(result?.qr_required) !== Boolean(proof.qr_required)) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_QR_PROOF_STALE');
  }
}

async function handleComponentPrepare(request, env, deps, workflowId, owner = 'candidate') {
  const body = await readJson(request);
  const generation = requireInteger(body.generation, 'WORKFLOW_VERSION_MISMATCH', 1);
  const componentKind = upper(body.component_kind || (owner === 'office' ? 'MANAGER_SIGNATURE' : ''));
  const mediaType = normaliseMediaType(body.media_type);
  const allowed = componentMediaTypes(componentKind);
  if (!allowed.includes(mediaType)) throw new CandidateHttpError(415, 'CANDIDATE_COMPONENT_MEDIA_TYPE_INVALID');
  const byteSize = requireInteger(body.byte_size, 'CANDIDATE_COMPONENT_SIZE_INVALID', 1);
  if (byteSize > uploadLimits(env).bytes) throw new CandidateHttpError(413, 'CANDIDATE_COMPONENT_SIZE_INVALID');
  const environment = environmentName(env);
  let sessionId = null;
  let approvalTokenHash = null;
  let ownerId = null;
  let authority = null;
  let captureMethod = null;
  let expectedContentSha256 = null;
  let candidateAccess = null;
  let paperReturnProof = null;
  if (owner === 'candidate') {
    const access = await verifyCandidateAccess(request, env);
    candidateAccess = access;
    sessionId = access.session_id;
    ownerId = access.session_id;
  } else if (owner === 'manager') {
    authority = managerRouteAuthority(request);
    await assertManagerRouteWorkflow(env, workflowId, authority);
    const managerToken = bearerToken(request);
    if (!managerToken) throw new CandidateHttpError(401, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
    approvalTokenHash = await sha256Hex(managerToken);
    ownerId = approvalTokenHash;
    captureMethod = upper(body.capture_method);
    if (!['DRAW', 'UPLOAD'].includes(captureMethod)
        || (authority.authority_kind === 'MANAGER_PHONE' && captureMethod !== 'DRAW')) {
      throw new CandidateHttpError(400, 'MANAGER_SIGNATURE_CAPTURE_METHOD_INVALID');
    }
    expectedContentSha256 = text(body.content_sha256).toLowerCase();
    if (!SHA256_RE.test(expectedContentSha256)) {
      throw new CandidateHttpError(400, 'CANDIDATE_COMPONENT_DIGEST_INVALID');
    }
  } else if (owner === 'office') {
    const user = await deps.requireOfficeUser(request, ['admin']);
    if (!user) throw new CandidateHttpError(401, 'OFFICE_AUTH_REQUIRED');
    ownerId = requireUuid(user.id, 'OFFICE_AUTH_REQUIRED');
    authority = { authority_kind: 'MANAGER_PHONE' };
    captureMethod = 'DRAW';
  }
  if (owner === 'candidate' && componentKind === 'SIGNED_RETURN') {
    paperReturnProof = await validateCandidatePaperReturnProof(
      env, deps, candidateAccess, workflowId, generation, body
    );
  } else if (Object.prototype.hasOwnProperty.call(body, 'signed_return_proof')) {
    throw new CandidateHttpError(400, 'CANDIDATE_PAPER_QR_PROOF_FORBIDDEN');
  }
  let approvalRequestId = body.approval_request_id ? requireUuid(body.approval_request_id) : null;
  if (owner === 'office') {
    const workflow = await workflowRow(env, workflowId);
    if (Number(workflow.generation) !== generation) {
      throw new CandidateHttpError(409, 'WORKFLOW_GENERATION_CONFLICT');
    }
    const approval = await exactOfficeApproval(
      env, workflowId, generation, approvalRequestId,
      requireInteger(body.approval_request_generation, 'CANDIDATE_REQUEST_GENERATION_STALE', 1)
    );
    if (upper(approval.method) !== 'PHONE' || upper(approval.state) !== 'PENDING') {
      throw new CandidateHttpError(409, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
    }
    approvalRequestId = approval.id;
  }
  const storageKey = componentStorageKey(environment, workflowId, generation, componentKind, mediaType);
  const payload = {
    component_kind: componentKind,
    document_role: upper(body.document_role || (owner === 'office' ? 'MANAGER_SIGNATURE' : '')),
    expense_category: body.expense_category == null ? null : upper(body.expense_category),
    paper_return_page_key: body.paper_return_page_key == null ? null : text(body.paper_return_page_key),
    storage_key: storageKey, media_type: mediaType, byte_size: byteSize,
    approval_request_id: approvalRequestId,
    ...(owner === 'office' ? { service_phone_approval: true, actor_user_id: ownerId } : {}),
    ...(approvalTokenHash ? {
      approval_token_hash_hex: approvalTokenHash
    } : {}),
    ...(captureMethod ? { manager_signature_capture_method: captureMethod } : {}),
    ...(expectedContentSha256 ? { expected_source_content_sha256_hex: expectedContentSha256 } : {})
    ,...(paperReturnProof ? {
      paper_return_proof_receipt_sha256: paperReturnProof.proof_receipt_sha256
    } : {})
  };
  const idempotencyKey = owner === 'office'
    ? requireOfficeIdempotency(body.idempotency_key)
    : requireCandidateIdempotency(body.idempotency_key);
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', {
    p_session_id: sessionId, p_environment: environment, p_workflow_id: workflowId,
    p_action: 'COMPONENT_PREPARE', p_expected_generation: generation, p_payload: payload,
    p_idempotency_key: idempotencyKey, p_now_utc: new Date().toISOString()
  });
  if (authority?.authority_kind === 'MANAGER_EMAIL') {
    await assertManagerRouteResult(env, result, authority);
  }
  const authoritative = preparedUploadContract(result, {
    media_type: mediaType, byte_size: byteSize, component_kind: componentKind,
    document_role: payload.document_role, expense_category: payload.expense_category,
    paper_return_page_key: payload.paper_return_page_key,
    workflow_generation: generation
  });
  const componentId = authoritative.component_id;
  const ticket = await uploadTicket(env, {
    env: environment,
    authority_kind: authority?.authority_kind || 'CANDIDATE_SESSION',
    owner, owner_id: ownerId, workflow_id: workflowId,
    candidate_session_id: null,
    generation: authoritative.workflow_generation, component_id: componentId, component_kind: authoritative.component_kind,
    key: authoritative.storage_key, media_type: authoritative.media_type, byte_size: authoritative.byte_size,
    completion_idempotency_key: `${idempotencyKey}:complete`,
    ...(expectedContentSha256 ? { expected_content_sha256: expectedContentSha256 } : {}),
    ...(captureMethod ? { capture_method: captureMethod } : {}),
    ...(paperReturnProof ? { paper_return_proof: {
      proof_contract_version: PAPER_RETURN_PROOF_VERSION,
      paper_return_manifest_sha256: paperReturnProof.paper_return_manifest_sha256,
      paper_return_page_key: paperReturnProof.paper_return_page_key,
      qr_required: paperReturnProof.qr_required === true,
      qr_token_sha256: paperReturnProof.qr_token_sha256 || null,
      proof_receipt_sha256: paperReturnProof.proof_receipt_sha256
    } } : {}),
    ...(authority?.authority_kind === 'MANAGER_EMAIL' ? {
      manager_route_ticket_id: authority.manager_route_ticket_id,
      route_revision: authority.route_revision,
      workflow_route_hmac: authority.workflow_route_hmac,
      approval_request_route_hmac: authority.approval_request_route_hmac,
      request_generation: authority.request_generation,
      credential_generation: authority.credential_generation
    } : {})
  });
  return jsonResponse(result.idempotent_replay === true ? 200 : 201, {
    ok: true, workflow_id: workflowId, generation: authoritative.workflow_generation, component_id: componentId,
    idempotent_replay: result.idempotent_replay === true,
    upload: {
      method: 'PUT', url: `${owner === 'office' ? '/api/candidate-app' : CANDIDATE_PREFIX}/uploads/${encodeURIComponent(ticket)}`,
      media_type: authoritative.media_type, byte_size: authoritative.byte_size, expires_in_seconds: 600,
      ...(expectedContentSha256 ? { expected_content_sha256: expectedContentSha256 } : {})
    }
  });
}

async function authenticateUploadOwner(request, env, deps, ticket) {
  if (ticket.owner === 'candidate') {
    if (ticket.authority_kind !== 'CANDIDATE_SESSION'
        || request.headers.has('x-cloudtms-manager-route-authority')) {
      throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_INVALID');
    }
    const access = await verifyCandidateAccess(request, env);
    if (access.session_id !== ticket.owner_id) throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_INVALID');
    return { session_id: access.session_id, approval_token_hash_hex: null };
  }
  if (ticket.owner === 'manager') {
    const authority = managerRouteAuthority(request);
    if (authority.authority_kind !== ticket.authority_kind) {
      throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_AUDIENCE_MISMATCH');
    }
    if (authority.authority_kind === 'MANAGER_EMAIL') {
      const exact = [
        'manager_route_ticket_id', 'route_revision', 'workflow_route_hmac',
        'approval_request_route_hmac', 'request_generation', 'credential_generation'
      ].every((name) => String(authority[name]) === String(ticket[name]));
      if (!exact) throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_AUDIENCE_MISMATCH');
      await assertManagerRouteWorkflow(env, ticket.workflow_id, authority);
    }
    const managerToken = bearerToken(request);
    const digest = managerToken ? await sha256Hex(managerToken) : '';
    if (!digest || digest !== ticket.owner_id) throw new CandidateHttpError(401, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
    return { session_id: null, approval_token_hash_hex: digest };
  }
  if (ticket.owner === 'office') {
    if (ticket.authority_kind !== 'MANAGER_PHONE') {
      throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_AUDIENCE_MISMATCH');
    }
    const user = await deps.requireOfficeUser(request, ['admin']);
    if (!user || user.id !== ticket.owner_id) throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_INVALID');
    return { session_id: ticket.candidate_session_id || null, approval_token_hash_hex: null };
  }
  throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_INVALID');
}

async function handleComponentUpload(request, env, deps, encodedTicket) {
  const ticket = await verifyUploadTicket(env, decodeURIComponent(encodedTicket));
  const owner = await authenticateUploadOwner(request, env, deps, ticket);
  const contentType = normaliseMediaType(request.headers.get('content-type'));
  if (contentType !== ticket.media_type) throw new CandidateHttpError(415, 'CANDIDATE_COMPONENT_MEDIA_TYPE_MISMATCH');
  const declared = Number(request.headers.get('content-length') || 0);
  if (declared && declared !== Number(ticket.byte_size)) throw new CandidateHttpError(400, 'CANDIDATE_COMPONENT_SIZE_MISMATCH');
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength !== Number(ticket.byte_size) || bytes.byteLength < 1
      || bytes.byteLength > uploadLimits(env).bytes) {
    throw new CandidateHttpError(400, 'CANDIDATE_COMPONENT_SIZE_MISMATCH');
  }
  const validated = await validateComponentBytes(bytes, contentType, env);
  const bucket = env.R2;
  if (!bucket || typeof bucket.put !== 'function') throw new CandidateHttpError(503, 'CANDIDATE_STORAGE_UNAVAILABLE');
  const digest = await sha256Hex(bytes);
  if (ticket.expected_content_sha256 && digest !== ticket.expected_content_sha256) {
    throw new CandidateHttpError(400, 'CANDIDATE_COMPONENT_DIGEST_MISMATCH');
  }
  await revalidateCandidatePaperReturnProof(env, deps, ticket, owner.session_id);
  const stored = await bucket.put(ticket.key, bytes, {
    onlyIf: { etagDoesNotMatch: '*' },
    httpMetadata: { contentType }, customMetadata: {
      purpose: 'candidate-component', workflow_id: ticket.workflow_id,
      component_id: ticket.component_id, media_type: contentType,
      byte_size: String(bytes.byteLength), sha256: digest,
      authority_kind: ticket.authority_kind,
      capture_method: ticket.capture_method || '',
      paper_return_proof_receipt_sha256: ticket.paper_return_proof?.proof_receipt_sha256 || ''
    }
  });
  if (!stored) {
    const existing = await bucket.head(ticket.key);
    const metadata = existing?.customMetadata || {};
    if (!existing || text(metadata.sha256).toLowerCase() !== digest
        || text(metadata.workflow_id) !== ticket.workflow_id
        || text(metadata.component_id) !== ticket.component_id
        || text(metadata.media_type).toLowerCase() !== contentType
        || Number(metadata.byte_size) !== bytes.byteLength) {
      throw new CandidateHttpError(409, 'CANDIDATE_UPLOAD_TICKET_ALREADY_USED');
    }
  }
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', {
    p_session_id: owner.session_id, p_environment: ticket.env,
    p_workflow_id: ticket.workflow_id, p_action: 'COMPONENT_COMPLETE',
    p_expected_generation: Number(ticket.generation),
    p_payload: {
      component_id: ticket.component_id, source_content_sha256_hex: digest,
        verified_byte_size: bytes.byteLength, verified_media_type: contentType,
        ...(ticket.capture_method ? { manager_signature_capture_method: ticket.capture_method } : {}),
        ...(validated.width ? { verified_image_width: validated.width } : {}),
        ...(validated.height ? { verified_image_height: validated.height } : {}),
        ...(ticket.owner === 'office' ? { service_phone_approval: true, actor_user_id: ticket.owner_id } : {}),
      ...(owner.approval_token_hash_hex ? { approval_token_hash_hex: owner.approval_token_hash_hex } : {})
    }, p_idempotency_key: ticket.completion_idempotency_key,
    p_now_utc: new Date().toISOString()
  });
  return jsonResponse(200, {
    ok: true, workflow_id: ticket.workflow_id, generation: Number(ticket.generation),
    component_id: ticket.component_id, state: result.state, media_type: contentType,
    byte_size: bytes.byteLength, content_sha256: digest,
    idempotent_replay: !stored || result.idempotent_replay === true,
    page_count: validated.page_count,
    image_width: validated.width,
    image_height: validated.height
  });
}

async function r2Bytes(env, key, expectedHash = null) {
  const canonical = text(key).replace(/^\/+/, '');
  if (!canonical) throw new CandidateHttpError(404, 'CANDIDATE_DOCUMENT_NOT_FOUND');
  const object = await env.R2?.get(canonical);
  if (!object) throw new CandidateHttpError(404, 'CANDIDATE_DOCUMENT_NOT_FOUND');
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (expectedHash && (await sha256Hex(bytes)) !== expectedHash.toLowerCase()) {
    throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_DIGEST_MISMATCH');
  }
  return { bytes, media_type: normaliseMediaType(object.httpMetadata?.contentType || 'application/octet-stream') };
}

function dataUrl(bytes, mediaType) {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return `data:${mediaType};base64,${btoa(binary)}`;
}

async function immutablePut(env, key, bytes, mediaType, metadata = {}) {
  const bucket = env.R2;
  if (!bucket) throw new CandidateHttpError(503, 'CANDIDATE_STORAGE_UNAVAILABLE');
  const digest = await sha256Hex(bytes);
  const stored = await bucket.put(key, bytes, {
    onlyIf: { etagDoesNotMatch: '*' },
    httpMetadata: { contentType: mediaType },
    customMetadata: { ...metadata, sha256: digest, media_type: mediaType, byte_size: String(bytes.byteLength) }
  });
  if (stored) return { created: true, sha256: digest };
  const existing = await bucket.head(key);
  if (!existing || text(existing.customMetadata?.sha256).toLowerCase() !== digest
      || text(existing.customMetadata?.media_type).toLowerCase() !== mediaType
      || Number(existing.customMetadata?.byte_size) !== bytes.byteLength) {
    throw new CandidateHttpError(409, 'CANDIDATE_RENDER_IDEMPOTENCY_CONFLICT');
  }
  return { created: false, sha256: digest };
}

function parseJson(value, fallback = null) {
  if (value == null) return fallback;
  if (typeof value === 'string') {
    try { return JSON.parse(value); } catch { return fallback; }
  }
  return value;
}

function ymdAdd(dateText, days) {
  const value = new Date(`${dateText}T12:00:00Z`);
  if (Number.isNaN(value.getTime())) return null;
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

function hhmm(value) {
  const source = text(value);
  const match = /(?:T|^)(\d{2}):(\d{2})/.exec(source);
  if (!match) return '';
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  return hours <= 23 && minutes <= 59 ? `${match[1]}:${match[2]}` : '';
}

function minuteOfDay(value) {
  const valueText = hhmm(value);
  if (!valueText) return null;
  const [hours, minutes] = valueText.split(':').map(Number);
  return hours * 60 + minutes;
}

function intervalMinutes(start, end) {
  const startMinute = minuteOfDay(start);
  const endMinute = minuteOfDay(end);
  if (startMinute == null || endMinute == null || startMinute === endMinute) return 0;
  return endMinute > startMinute ? endMinute - startMinute : endMinute + 1440 - startMinute;
}

function explicitNoBreak(value) {
  if (!isObject(value)) return false;
  if (value.no_break === true || value.noBreak === true) return true;
  const hasMinutes = Object.prototype.hasOwnProperty.call(value, 'break_minutes')
    || Object.prototype.hasOwnProperty.call(value, 'break_mins');
  if (!hasMinutes) return false;
  const raw = value.break_minutes ?? value.break_mins;
  return raw !== null && raw !== '' && Number(raw) === 0;
}

function segmentBreak(segment) {
  const windows = Array.isArray(segment?.breaks) ? segment.breaks : [];
  const primaryStart = text(segment?.break_start || segment?.breakStart || segment?.break_start_time);
  const primaryEnd = text(segment?.break_end || segment?.breakEnd || segment?.break_end_time);
  const firstWindow = windows.find((entry) => hhmm(entry?.start) && hhmm(entry?.end));
  const breakStart = hhmm(primaryStart || firstWindow?.start);
  const breakEnd = hhmm(primaryEnd || firstWindow?.end);
  const explicitValue = segment?.break_minutes ?? segment?.break_mins ?? segment?.unpaid_break_minutes;
  const explicitMinutes = explicitValue === '' || explicitValue == null ? null : Number(explicitValue);
  const minutes = breakStart && breakEnd
    ? intervalMinutes(breakStart, breakEnd)
    : Number.isFinite(explicitMinutes) && explicitMinutes >= 0 ? explicitMinutes : 0;
  return {
    break_start_local: breakStart,
    break_end_local: breakEnd,
    break_minutes: Math.round(minutes),
    break_display_mode: breakStart && breakEnd ? 'EXPLICIT_INTERVAL' : minutes > 0 ? 'MINUTES_ONLY' : 'NONE'
  };
}

function normaliseAdaptiveBreakEntry(entry, expectedMode) {
  if (!isObject(entry) || !exactKeys(entry, ['kind'], [
    'break_start', 'break_end', 'calculated_break_minutes', 'break_minutes', 'no_break'
  ])) {
    throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_INVALID');
  }
  const kind = upper(entry.kind);
  if (kind === 'NO_BREAK') {
    if (entry.no_break !== true || Number(entry.break_minutes) !== 0
        || Object.prototype.hasOwnProperty.call(entry, 'break_start')
        || Object.prototype.hasOwnProperty.call(entry, 'break_end')
        || Object.prototype.hasOwnProperty.call(entry, 'calculated_break_minutes')) {
      throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_INVALID');
    }
    return { no_break: true, break_minutes: 0 };
  }
  if (kind === 'START_END_TIMES' && expectedMode === 'START_END_TIMES') {
    const start = hhmm(entry.break_start);
    const end = hhmm(entry.break_end);
    const calculated = intervalMinutes(start, end);
    if (!start || !end || calculated < 1 || !Number.isSafeInteger(entry.calculated_break_minutes)
        || entry.calculated_break_minutes !== calculated
        || Object.prototype.hasOwnProperty.call(entry, 'break_minutes')
        || Object.prototype.hasOwnProperty.call(entry, 'no_break')) {
      throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_INVALID');
    }
    return { break_start: start, break_end: end, break_minutes: calculated };
  }
  if (kind === 'DURATION_MINUTES' && expectedMode === 'DURATION_MINUTES') {
    if (!Number.isSafeInteger(entry.break_minutes) || entry.break_minutes < 1
        || entry.break_minutes > 24 * 60
        || Object.prototype.hasOwnProperty.call(entry, 'break_start')
        || Object.prototype.hasOwnProperty.call(entry, 'break_end')
        || Object.prototype.hasOwnProperty.call(entry, 'calculated_break_minutes')
        || Object.prototype.hasOwnProperty.call(entry, 'no_break')) {
      throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_INVALID');
    }
    return { break_minutes: entry.break_minutes };
  }
  throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_MODE_MISMATCH');
}

function normaliseCandidateBreakSubmission(factualSubmission, context) {
  const facts = structuredClone(isObject(factualSubmission) ? factualSubmission : {});
  if (context?.applicable !== true) {
    const owners = [facts, facts.hours_submission, facts.timesheet_patch_json,
      facts.hours_submission?.timesheet_patch_json].filter(isObject);
    const hasAdaptiveEntry = owners.some(owner => (
      Object.prototype.hasOwnProperty.call(owner, 'break_entry')
      || ['actual_schedule_json', 'schedule_json'].some(key => (
        Array.isArray(owner[key]) && owner[key].some(segment => (
          isObject(segment) && Object.prototype.hasOwnProperty.call(segment, 'break_entry')
        ))
      ))
    ));
    if (Object.prototype.hasOwnProperty.call(facts, 'break_entry_context') || hasAdaptiveEntry) {
      throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_NOT_APPLICABLE');
    }
    return facts;
  }
  const supplied = facts.break_entry_context;
  if (!exactKeys(supplied, ['context_version', 'context_token', 'mode'])
      || supplied.context_version !== BREAK_ENTRY_CONTEXT_VERSION
      || supplied.context_token !== context.context_token
      || supplied.mode !== context.mode) {
    throw new CandidateHttpError(409, 'CANDIDATE_BREAK_ENTRY_CONTEXT_STALE');
  }
  delete facts.break_entry_context;
  let seen = 0;
  const normaliseArray = (segments) => segments.map((segment) => {
    if (!isObject(segment) || !Object.prototype.hasOwnProperty.call(segment, 'break_entry')) {
      throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_REQUIRED');
    }
    const result = { ...segment, ...normaliseAdaptiveBreakEntry(segment.break_entry, context.mode) };
    delete result.break_entry;
    seen += 1;
    return result;
  });
  const owners = [facts, facts.hours_submission, facts.timesheet_patch_json,
    facts.hours_submission?.timesheet_patch_json].filter(isObject);
  for (const owner of owners) {
    for (const key of ['actual_schedule_json', 'schedule_json']) {
      if (Array.isArray(owner[key])) owner[key] = normaliseArray(owner[key]);
    }
  }
  if (seen === 0 && Object.prototype.hasOwnProperty.call(facts, 'break_entry')) {
    Object.assign(facts, normaliseAdaptiveBreakEntry(facts.break_entry, context.mode));
    delete facts.break_entry;
    seen = 1;
  }
  if (seen === 0) throw new CandidateHttpError(400, 'CANDIDATE_BREAK_ENTRY_REQUIRED');
  return facts;
}

function scheduleFromImmutable(workflow, timesheet) {
  const immutable = parseJson(workflow.immutable_submission_json, {}) || {};
  const hoursSubmission = parseJson(immutable.hours_submission, immutable) || immutable;
  const timesheetPatch = parseJson(hoursSubmission.timesheet_patch_json, {}) || {};
  const candidates = [
    hoursSubmission.actual_schedule_json,
    hoursSubmission.schedule_json,
    timesheetPatch.actual_schedule_json,
    timesheetPatch.schedule_json,
    timesheet?.actual_schedule_json
  ];
  for (const candidate of candidates) {
    const parsed = parseJson(candidate, candidate);
    if (Array.isArray(parsed)) return parsed;
  }
  if (workflow.scope === 'DAILY') {
    const actualStart = timesheetPatch.worked_start_iso || immutable.actual_start || immutable.start || immutable.worked_start_iso || timesheet?.worked_start_iso;
    const actualEnd = timesheetPatch.worked_end_iso || immutable.actual_end || immutable.end || immutable.worked_end_iso || timesheet?.worked_end_iso;
    const noBreak = explicitNoBreak(timesheetPatch) || explicitNoBreak(immutable);
    return [{
      date: workflow.work_date,
      start: hhmm(actualStart), end: hhmm(actualEnd),
      break_start: noBreak ? '' : hhmm(timesheetPatch.break_start_iso || immutable.break_start || immutable.break_start_iso || timesheet?.break_start_iso),
      break_end: noBreak ? '' : hhmm(timesheetPatch.break_end_iso || immutable.break_end || immutable.break_end_iso || timesheet?.break_end_iso),
      break_minutes: noBreak ? 0 : timesheetPatch.break_minutes ?? immutable.break_minutes ?? timesheet?.break_minutes ?? null,
      ref_num: immutable.reference_number || timesheet?.reference_number || null
    }];
  }
  return [];
}

function scheduleLine(segment, index) {
  const date = text(segment?.date || segment?.work_date || segment?.worked_date).slice(0, 10);
  const start = hhmm(segment?.start || segment?.start_time || segment?.actual_start || segment?.worked_start_iso || segment?.start_utc || segment?.start_iso);
  const end = hhmm(segment?.end || segment?.end_time || segment?.actual_end || segment?.worked_end_iso || segment?.end_utc || segment?.end_iso);
  const breakInfo = segmentBreak(segment);
  const duration = intervalMinutes(start, end);
  const paidMinutes = Math.max(0, duration - breakInfo.break_minutes);
  return {
    row_key: text(segment?.row_key || segment?.segment_id) || `segment:${date}:${index + 1}`,
    display_order: index + 1,
    segment_id: text(segment?.segment_id) || `candidate-segment-${index + 1}`,
    date,
    worked_start_utc: text(segment?.start_utc || segment?.worked_start_iso) || null,
    worked_end_utc: text(segment?.end_utc || segment?.worked_end_iso) || null,
    display_start_local: start,
    display_end_local: end,
    ...breakInfo,
    bucket_hours: {
      DAY: Number(segment?.hours_day || 0), NIGHT: Number(segment?.hours_night || 0),
      SAT: Number(segment?.hours_sat || 0), SUN: Number(segment?.hours_sun || 0),
      BH: Number(segment?.hours_bh || 0)
    },
    paid_minutes: paidMinutes,
    band: text(segment?.band) || null,
    booking_reference: text(segment?.ref_num || segment?.reference || segment?.booking_reference) || null,
    reference_required: false,
    reference_source: text(segment?.ref_num || segment?.reference || segment?.booking_reference) ? 'SEGMENT' : null,
    reference_row_key: text(segment?.row_key || segment?.segment_id) || `segment:${date}:${index + 1}`
  };
}

function additionalUnitRows(workflow, timesheet) {
  const immutable = parseJson(workflow.immutable_submission_json, {}) || {};
  const hoursSubmission = parseJson(immutable.hours_submission, immutable) || immutable;
  const timesheetPatch = parseJson(hoursSubmission.timesheet_patch_json, {}) || {};
  const weekly = parseJson(timesheetPatch.additional_units_week ?? hoursSubmission.additional_units_week ?? timesheet?.additional_units_week, {}) || {};
  const perDay = parseJson(timesheetPatch.additional_units_per_day ?? hoursSubmission.additional_units_per_day ?? timesheet?.additional_units_per_day, {}) || {};
  const rows = [];
  const push = (code, value, date = null) => {
    const object = isObject(value) ? value : {};
    const quantity = Number(object.unit_count ?? object.quantity ?? object.units ?? value);
    if (!Number.isFinite(quantity) || quantity === 0) return;
    rows.push({
      row_key: `additional:${upper(code)}:${date || 'weekly'}:${rows.length + 1}`,
      display_order: rows.length + 1,
      code: upper(code), rate_type: text(object.bucket_name || object.rate_type || code),
      date, quantity, unit: text(object.unit_name || object.unit || 'unit'),
      frequency: date ? 'PER_DAY' : 'WEEKLY'
    });
  };
  for (const [code, value] of Object.entries(weekly)) push(code, value, null);
  for (const [date, values] of Object.entries(perDay)) {
    if (!isObject(values)) continue;
    for (const [code, value] of Object.entries(values)) push(code, value, date);
  }
  return rows;
}

function candidateNameParts(candidate) {
  const first = text(candidate?.first_name);
  const last = text(candidate?.last_name || candidate?.surname);
  if (first || last) return { first_name: first || 'Candidate', surname: last || '-' };
  const pieces = text(candidate?.display_name || candidate?.name).split(/\s+/).filter(Boolean);
  return { first_name: pieces.shift() || 'Candidate', surname: pieces.join(' ') || '-' };
}

function officialPresentationFromRows({ timesheet, contractRow, candidate, client, branding, renderer_contract_version } = {}) {
  const worker = candidateNameParts(candidate);
  const clientName = text(client?.name) || 'CloudTMS Client';
  return {
    schema_version: 'OFFICIAL_CANDIDATE_PRESENTATION_V1',
    worker: {
      ...worker,
      job_profile_title: text(contractRow?.role || timesheet?.job_title_norm) || null
    },
    client: {
      name: clientName,
      hospital: text(contractRow?.display_site) || clientName,
      site_ward: text(contractRow?.ward_hint || timesheet?.ward_norm) || '-'
    },
    band: text(contractRow?.band || timesheet?.band) || null,
    renderer_contract_version: text(renderer_contract_version) || RENDERER_CONTRACT_VERSION,
    branding: branding || { agency_name: 'Arthur Rai Medical Services' },
    wording: {
      header: { lines: ['Please review the recorded work and all attached evidence before approving.'] },
      footer: { lines: ['CloudTMS official timesheet'] },
      temporary_worker_declaration: {
        title: 'Temporary Worker Declaration',
        lines: ['I declare that the hours and units recorded are complete and accurate.']
      },
      client_declaration: {
        title: 'Client Declaration',
        lines: ['I confirm that the recorded work was completed and is approved.']
      }
    }
  };
}

async function buildOfficialPresentationSnapshot(env, workflow) {
  const timesheetId = workflow.target_timesheet_id || workflow.anchor_timesheet_id;
  const [timesheet, contractRow, candidate] = await Promise.all([
    timesheetId
      ? restOne(env, 'timesheets', `timesheet_id=eq.${encodeURIComponent(timesheetId)}&is_current=eq.true&select=timesheet_id,ward_norm,job_title_norm,band`)
      : Promise.resolve(null),
    workflow.contract_id
      ? restOne(env, 'contracts', `id=eq.${encodeURIComponent(workflow.contract_id)}&select=id,client_id,role,display_site,ward_hint,band`)
      : Promise.resolve(null),
    restOne(env, 'candidates', `id=eq.${encodeURIComponent(workflow.candidate_id)}&select=id,first_name,last_name,display_name`)
  ]);
  const financials = timesheetId
    ? await restOne(env, 'timesheets_financials', `timesheet_id=eq.${encodeURIComponent(timesheetId)}&is_current=eq.true&select=client_id`)
    : null;
  const clientId = contractRow?.client_id || financials?.client_id;
  const client = clientId
    ? await restOne(env, 'clients', `id=eq.${encodeURIComponent(clientId)}&select=id,name`)
    : null;
  const branding = await candidateDocumentBranding(env);
  return officialPresentationFromRows({
    timesheet, contractRow, candidate, client,
    branding: {
      contract_version: branding.contract_version,
      agency_name: branding.agency_name,
      logo_key: branding.logo_key,
      logo_sha256: branding.logo_sha256,
      logo_media_type: branding.logo_media_type,
      branding_contract_sha256: branding.branding_contract_sha256
    },
    renderer_contract_version: RENDERER_CONTRACT_VERSION
  });
}

async function loadRenderState(env, contract) {
  const workflowId = requireUuid(contract.workflow_id, 'CANDIDATE_RENDER_CONTRACT_INVALID');
  const generation = requireInteger(contract.workflow_generation, 'CANDIDATE_RENDER_CONTRACT_INVALID', 1);
  const workflow = await restOne(env, 'candidate_submission_workflows',
    `id=eq.${encodeURIComponent(workflowId)}&generation=eq.${generation}&select=*`);
  if (!workflow) throw new CandidateHttpError(404, 'CANDIDATE_WORKFLOW_NOT_FOUND');
  const component = await restOne(env, 'candidate_submission_components',
    `id=eq.${encodeURIComponent(contract.component_id)}&workflow_id=eq.${encodeURIComponent(workflowId)}&workflow_generation=eq.${generation}&select=*`);
  if (!component) throw new CandidateHttpError(404, 'CANDIDATE_COMPONENT_NOT_FOUND');
  const timesheetId = workflow.target_timesheet_id || workflow.anchor_timesheet_id;
  const timesheet = timesheetId ? await restOne(env, 'timesheets', `timesheet_id=eq.${encodeURIComponent(timesheetId)}&select=*`) : null;
  const financials = timesheetId ? await restOne(env, 'timesheets_financials',
    `timesheet_id=eq.${encodeURIComponent(timesheetId)}&is_current=eq.true&select=client_id,candidate_id,worked_start_iso,worked_end_iso`) : null;
  const contractRow = workflow.contract_id
    ? await restOne(env, 'contracts', `id=eq.${encodeURIComponent(workflow.contract_id)}&select=*`) : null;
  const candidate = await restOne(env, 'candidates', `id=eq.${encodeURIComponent(workflow.candidate_id)}&select=*`);
  const clientId = contractRow?.client_id || financials?.client_id || timesheet?.client_id;
  const client = clientId ? await restOne(env, 'clients', `id=eq.${encodeURIComponent(clientId)}&select=*`) : null;
  return { workflow, component, timesheet, financials, contract: contractRow, candidate, client };
}

async function signatureAsset(env, componentId, fallbackKey = null, fallbackHash = null) {
  let component = null;
  if (componentId) component = await restOne(env, 'candidate_submission_components', `id=eq.${encodeURIComponent(componentId)}&select=*`);
  const key = component?.storage_key || fallbackKey;
  const expectedHash = component?.source_content_sha256 ? text(component.source_content_sha256).replace(/^\\x/, '') : fallbackHash;
  if (!key) return { identity: {}, data: null };
  const source = await r2Bytes(env, key, expectedHash || null);
  if (!['image/png', 'image/jpeg'].includes(source.media_type)) {
    throw new CandidateHttpError(415, 'CANDIDATE_SIGNATURE_MEDIA_TYPE_UNSUPPORTED');
  }
  return {
    identity: {
      r2_key: key,
      sha256: expectedHash || await sha256Hex(source.bytes),
      media_type: source.media_type,
      size_bytes: source.bytes.byteLength
    },
    data: { data_url: dataUrl(source.bytes, source.media_type) }
  };
}

function officialPeriodWithShiftLines(endDate, lines) {
  const period = buildOfficialWeekPeriod(endDate);
  return {
    ...period,
    days: period.days.map((day) => ({
      ...day,
      shift_lines: lines.filter((line) => line.date === day.date)
        .map((line, index) => ({ ...line, display_order: index + 1 }))
    }))
  };
}

async function buildOfficialCandidateModel(env, contract, state, phase) {
  const { workflow, timesheet, candidate, client, contract: contractRow } = state;
  const frozen = parseJson(workflow.immutable_submission_json, {}) || {};
  const frozenPresentation = parseJson(frozen.official_presentation, {}) || {};
  const frozenHours = parseJson(frozen.hours_submission, frozen) || frozen;
  const frozenTimesheetId = frozenHours?.timesheet_create_json?.timesheet_id;
  const officialTimesheetId = timesheet?.timesheet_id || workflow.target_timesheet_id
    || workflow.anchor_timesheet_id || frozenTimesheetId;
  if (!UUID_RE.test(text(officialTimesheetId))) {
    throw new CandidateHttpError(409, 'CANDIDATE_RENDER_TIMESHEET_ID_INVALID');
  }
  const endDate = text(workflow.week_ending_date || workflow.work_date || timesheet?.week_ending_date).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(endDate)) throw new CandidateHttpError(409, 'CANDIDATE_RENDER_DATE_INVALID');
  const rawSchedule = scheduleFromImmutable(workflow, timesheet);
  const lines = rawSchedule.map(scheduleLine).filter((line) => line.date && line.display_start_local && line.display_end_local);
  lines.sort((a, b) => a.date.localeCompare(b.date) || a.display_start_local.localeCompare(b.display_start_local) || a.segment_id.localeCompare(b.segment_id));
  const period = officialPeriodWithShiftLines(endDate, lines);
  const paidMinutes = lines.reduce((sum, line) => sum + Number(line.paid_minutes || 0), 0);
  const candidateSignatureId = workflow.candidate_signature_component_id;
  const candidateSignature = await signatureAsset(env, candidateSignatureId);
  const managerSignature = phase === 'FINAL'
    ? await signatureAsset(env, workflow.manager_signature_component_id, contract.manager?.signature_storage_key, contract.manager?.signature_sha256)
    : { identity: {}, data: null };
  const additionalRows = additionalUnitRows(workflow, timesheet);
  const worker = isObject(frozenPresentation.worker)
    ? frozenPresentation.worker
    : candidateNameParts(candidate);
  const formVariant = phase === 'FINAL' ? 'ELECTRONIC_SIGNED' : 'ELECTRONIC_MANAGER_REVIEW';
  const signedDate = text(workflow.candidate_signed_at_utc).slice(0, 10) || null;
  const managerDate = text(workflow.manager_approved_at_utc || contract.manager?.approval_date_utc).slice(0, 10) || null;
  const branding = await candidateDocumentBranding(env, workflow);
  const model = {
    schema_version: 'TIMESHEET_RENDER_MODEL_V2',
    template_version: 'timesheet-professional-v2',
    layout_contract_version: 'TIMESHEET_ONE_PAGE_LANDSCAPE_V2',
    timesheet_id: officialTimesheetId,
    document_revision: Number(timesheet?.version || workflow.generation || 1),
    timesheet_number: officialTimesheetNumber(officialTimesheetId),
    sheet_scope: workflow.scope,
    form_variant: formVariant,
    submission_mode: 'ELECTRONIC', locale: 'en-GB', time_zone: 'Europe/London',
    week_period: period,
    worker: {
      ...worker,
      job_profile_title: text(
        frozenPresentation.worker?.job_profile_title || contractRow?.role || contractRow?.job_title ||
        contractRow?.job_profile_title || candidate?.job_title
      ) || null
    },
    client: isObject(frozenPresentation.client) ? frozenPresentation.client : {
      name: text(client?.name || client?.client_name) || 'CloudTMS Client',
      hospital: text(client?.name || client?.client_name) || 'CloudTMS Client',
      site_ward: text(contractRow?.ward || contractRow?.site_ward || timesheet?.ward) || '-'
    },
    band: text(frozenPresentation.band || contractRow?.band || timesheet?.band) || null,
    branding: {
      agency_name: branding.agency_name,
      logo: branding.logo ? {
        r2_key: branding.logo_key,
        sha256: branding.logo_sha256,
        media_type: branding.logo_media_type
      } : {}
    },
    wording: isObject(frozenPresentation.wording) ? frozenPresentation.wording : {
      header: { lines: ['Please review the recorded work and all attached evidence before approving.'] },
      footer: { lines: ['CloudTMS official timesheet'] },
      temporary_worker_declaration: {
        title: 'Temporary Worker Declaration',
        lines: ['I declare that the hours and units recorded are complete and accurate.']
      },
      client_declaration: {
        title: 'Client Declaration',
        lines: ['I confirm that the recorded work was completed and is approved.']
      }
    },
    additional_units_section: {
      schema_version: 'TIMESHEET_ADDITIONAL_UNITS_V1', visible: additionalRows.length > 0,
      title: 'Additional rates / units',
      column_labels: { rate_type: 'Rate Type', date: 'Date', quantity: 'Quantity', unit: 'Unit' },
      minimum_blank_space_rows: 1, rows: additionalRows
    },
    totals: { paid_minutes: paidMinutes },
    authorisation: { authorised: phase === 'FINAL', authorised_at_utc: phase === 'FINAL' ? workflow.manager_approved_at_utc : null },
    signatures: {
      candidate: { ...candidateSignature.identity, signed_date: signedDate },
      authoriser: phase === 'FINAL'
        ? { ...managerSignature.identity, signed_date: managerDate, name: workflow.manager_name, position: workflow.manager_position }
        : {}
    },
    qr: { required: false, signed: false, status: null, payload: null, token: null },
    layout: {
      one_page_required: true, allowed_modes: ['NORMAL', 'COMPACT', 'ULTRA'], second_page_allowed: false,
      minimum_font_size: 5.5, minimum_row_height_mm: 3.45, minimum_signature_height_mm: 7,
      minimum_additional_blank_rows: 1
    }
  };
  validateFrozenTimesheetPresentationModel(model);
  return { model, assets: {
    logo: branding.logo ? { data_url: dataUrl(branding.logo.bytes, branding.logo.media_type) } : null,
    candidate_signature: candidateSignature.data,
    authoriser_signature: managerSignature.data
  } };
}

function expenseClaim(workflow) {
  const immutable = parseJson(workflow.immutable_submission_json, {}) || {};
  const expenseSubmission = parseJson(immutable.expense_submission || immutable.expense_claim || immutable.expenses || immutable, {}) || {};
  return {
    expenseSubmission,
    claim: parseJson(expenseSubmission.canonical_tsfin_snapshot, expenseSubmission) || expenseSubmission
  };
}

function pounds(value) {
  const amount = Number(value);
  return Number.isFinite(amount) ? `£${amount.toFixed(2)}` : null;
}

function expenseSummaryDisplayLines(workflow) {
  const { expenseSubmission, claim } = expenseClaim(workflow);
  const mileage = Number(claim.mileage_units ?? expenseSubmission.mileage_units ?? expenseSubmission.total_mileage);
  const values = [
    ['Accommodation', claim.accommodation_pay_ex_vat ?? expenseSubmission.accommodation_amount],
    ['Travel', claim.travel_pay_ex_vat ?? expenseSubmission.travel_amount],
    ['Other', claim.other_pay_ex_vat ?? expenseSubmission.other_amount]
  ];
  const lines = [];
  if (Number.isFinite(mileage) && mileage !== 0) lines.push(`Mileage: ${mileage} miles`);
  for (const [label, value] of values) {
    const formatted = pounds(value);
    if (formatted && Number(value) !== 0) lines.push(`${label}: ${formatted}`);
  }
  const totalSource = claim.expenses_pay_ex_vat;
  const explicitTotal = totalSource === null || totalSource === undefined || totalSource === ''
    ? Number.NaN : Number(totalSource);
  if (!Number.isFinite(explicitTotal)) {
    throw new CandidateHttpError(409, 'CANDIDATE_EXPENSE_DISPLAY_TOTAL_REQUIRED');
  }
  if (!lines.length && explicitTotal !== 0) lines.push(`Expenses: ${pounds(explicitTotal)}`);
  return {
    lines,
    total: pounds(explicitTotal)
  };
}

function expenseLines(workflow, component) {
  const immutable = parseJson(workflow.immutable_submission_json, {}) || {};
  const presentation = parseJson(immutable.official_presentation, {}) || {};
  const summary = expenseSummaryDisplayLines(workflow);
  return [
    `Candidate: ${text(presentation.worker?.first_name)} ${text(presentation.worker?.surname)}`.trim(),
    `Client: ${text(presentation.client?.name) || '-'}`,
    `Week ending: ${ukDate(workflow.week_ending_date)}`,
    ...summary.lines,
    `Total claim: ${summary.total}`,
    `CloudTMS workflow: ${workflow.id}`,
    `Page identity: ${component.id || workflow.id}`
  ];
}

function frozenBrandingContract(value) {
  if (!value) return null;
  if (value.contract_version || value.branding_contract_sha256) return value;
  const immutable = parseJson(value.immutable_submission_json, {}) || {};
  const presentation = parseJson(immutable.official_presentation, {}) || {};
  return isObject(presentation.branding) ? presentation.branding : null;
}

async function brandingContractSha256(contract) {
  return sha256Hex(JSON.stringify({
    contract_version: contract.contract_version,
    agency_name: contract.agency_name,
    logo_key: contract.logo_key,
    logo_sha256: contract.logo_sha256,
    logo_media_type: contract.logo_media_type
  }));
}

function contentAddressedBrandingLogoKey(digest, mediaType) {
  if (!SHA256_RE.test(text(digest))) {
    throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_BRANDING_CONTRACT_INVALID');
  }
  const extension = mediaType === 'image/png' ? 'png'
    : mediaType === 'image/jpeg' ? 'jpg' : null;
  if (!extension) throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_LOGO_MEDIA_TYPE_INVALID');
  return `candidate-app/branding/${text(digest).toLowerCase()}.${extension}`;
}

async function candidateDocumentBranding(env, frozenSource = null) {
  const frozen = frozenBrandingContract(frozenSource);
  if (frozen) {
    const contract = {
      contract_version: text(frozen.contract_version),
      agency_name: text(frozen.agency_name),
      logo_key: text(frozen.logo_key) || null,
      logo_sha256: text(frozen.logo_sha256).toLowerCase() || null,
      logo_media_type: normaliseMediaType(frozen.logo_media_type || '') || null,
      branding_contract_sha256: text(frozen.branding_contract_sha256).toLowerCase()
    };
    if (contract.contract_version !== DOCUMENT_BRANDING_CONTRACT_VERSION || !contract.agency_name
        || !SHA256_RE.test(contract.branding_contract_sha256)
        || contract.branding_contract_sha256 !== await brandingContractSha256(contract)) {
      throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_BRANDING_CONTRACT_INVALID');
    }
    if (!contract.logo_key) {
      if (contract.logo_sha256 || contract.logo_media_type) {
        throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_BRANDING_CONTRACT_INVALID');
      }
      return { ...contract, logo: null };
    }
    if (!SHA256_RE.test(contract.logo_sha256)
        || !['image/png', 'image/jpeg'].includes(contract.logo_media_type)) {
      throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_LOGO_MEDIA_TYPE_INVALID');
    }
    if (contract.logo_key !== contentAddressedBrandingLogoKey(contract.logo_sha256, contract.logo_media_type)) {
      throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_BRANDING_CONTRACT_INVALID');
    }
    const logo = await r2Bytes(env, contract.logo_key, contract.logo_sha256);
    if (logo.media_type !== contract.logo_media_type) {
      throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_LOGO_MEDIA_TYPE_INVALID');
    }
    return { ...contract, logo };
  }
  const settings = await restOne(env, 'settings_defaults', 'id=eq.1&select=agency_name,agency_logo');
  const contract = {
    contract_version: DOCUMENT_BRANDING_CONTRACT_VERSION,
    agency_name: text(settings?.agency_name) || 'Arthur Rai Medical Services',
    logo_key: text(settings?.agency_logo) || null,
    logo_sha256: null,
    logo_media_type: null
  };
  let logo = null;
  if (contract.logo_key) {
    logo = await r2Bytes(env, contract.logo_key);
    if (!['image/png', 'image/jpeg'].includes(logo.media_type)) {
      throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_LOGO_MEDIA_TYPE_INVALID');
    }
    contract.logo_sha256 = await sha256Hex(logo.bytes);
    contract.logo_media_type = logo.media_type;
    contract.logo_key = contentAddressedBrandingLogoKey(contract.logo_sha256, contract.logo_media_type);
    await immutablePut(env, contract.logo_key, logo.bytes, contract.logo_media_type, {
      purpose: 'candidate-branding-logo',
      logo_sha256: contract.logo_sha256
    });
  }
  contract.branding_contract_sha256 = await brandingContractSha256(contract);
  return { ...contract, logo };
}

async function drawCandidateBranding(pdf, page, branding, { x = 34, y = 800, maxWidth = 140, maxHeight = 32 } = {}) {
  if (branding.logo) {
    const image = branding.logo.media_type === 'image/png'
      ? await pdf.embedPng(branding.logo.bytes) : await pdf.embedJpg(branding.logo.bytes);
    const scale = Math.min(maxWidth / image.width, maxHeight / image.height);
    page.drawImage(image, { x, y, width: image.width * scale, height: image.height * scale });
  }
}

async function embedExpenseSource(pdf, page, env, component, renderInput, contentTop, contentHeight) {
  const sourceId = renderInput?.source_component_id || component.source_component_id;
  if (!sourceId) return false;
  const sourceComponent = await restOne(env, 'candidate_submission_components', `id=eq.${encodeURIComponent(sourceId)}&select=*`);
  if (!sourceComponent?.storage_key) throw new CandidateHttpError(409, 'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED');
  const expected = text(sourceComponent.source_content_sha256).replace(/^\\x/, '') || null;
  const source = await r2Bytes(env, sourceComponent.storage_key, expected);
  const width = page.getWidth() - 72;
  if (source.media_type === 'application/pdf') {
    const input = await PDFDocument.load(source.bytes);
    if (input.getPageCount() !== 1) throw new CandidateHttpError(400, 'CANDIDATE_SOURCE_PDF_ONE_PAGE_REQUIRED');
    const [embedded] = await pdf.embedPdf(source.bytes, [0]);
    const scale = Math.min(width / embedded.width, contentHeight / embedded.height);
    page.drawPage(embedded, {
      x: (page.getWidth() - embedded.width * scale) / 2,
      y: contentTop - embedded.height * scale,
      width: embedded.width * scale, height: embedded.height * scale
    });
    return true;
  }
  const image = source.media_type === 'image/png' ? await pdf.embedPng(source.bytes) : await pdf.embedJpg(source.bytes);
  const scale = Math.min(width / image.width, contentHeight / image.height);
  page.drawImage(image, {
    x: (page.getWidth() - image.width * scale) / 2,
    y: contentTop - image.height * scale,
    width: image.width * scale, height: image.height * scale
  });
  return true;
}

async function renderExpensePage(env, contract, state, phase) {
  const { workflow, component } = state;
  const pdf = await PDFDocument.create({ updateMetadata: false });
  const page = pdf.addPage([595.28, 841.89]);
  const regular = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const branding = await candidateDocumentBranding(env, workflow);
  page.drawRectangle({ x: 0, y: 790, width: page.getWidth(), height: 52, color: rgb(0.04, 0.12, 0.24) });
  await drawCandidateBranding(pdf, page, branding, { x: 420, y: 800, maxWidth: 135, maxHeight: 30 });
  page.drawText(component.component_kind === 'EXPENSE_SUMMARY' ? 'Expense claim approval summary' : 'Expense evidence', {
    x: 36, y: 812, size: 16, font: bold, color: rgb(1, 1, 1)
  });
  page.drawText(`${branding.agency_name} | Page ${component.review_ordinal || contract.review_ordinal} | ${component.expense_category || 'General'}`, {
    x: 36, y: 797, size: 9, font: regular, color: rgb(0.86, 0.9, 0.96)
  });
  const hasSource = await embedExpenseSource(pdf, page, env, component, contract.render_input, 760, 570);
  if (!hasSource) {
    const lines = expenseLines(workflow, component).slice(0, 24);
    if (upper(component.component_kind) === 'EXPENSE_SUMMARY') {
      const identity = lines.slice(0, 3);
      let y = 752;
      for (const line of identity) {
        page.drawText(line.slice(0, 100), { x: 42, y, size: 10, font: regular, color: rgb(0.08, 0.12, 0.2) });
        y -= 20;
      }
      const claimLines = lines.slice(3, -3);
      const totalLine = lines.at(-3) || 'Total claim: £0.00';
      page.drawRectangle({ x: 42, y: 670, width: 511, height: 32, color: rgb(0.9, 0.93, 0.97), borderColor: rgb(0.18, 0.28, 0.4), borderWidth: 1 });
      page.drawText('Claim details', { x: 54, y: 681, size: 11, font: bold, color: rgb(0.07, 0.14, 0.24) });
      y = 632;
      for (const line of claimLines) {
        const colon = line.indexOf(':');
        const label = colon >= 0 ? line.slice(0, colon) : line;
        const value = colon >= 0 ? line.slice(colon + 1).trim() : '';
        page.drawRectangle({ x: 42, y: y - 10, width: 511, height: 38, borderColor: rgb(0.4, 0.47, 0.55), borderWidth: 0.7 });
        page.drawText(label.slice(0, 45), { x: 54, y: y + 3, size: 10, font: regular });
        page.drawText(value.slice(0, 45), { x: 365, y: y + 3, size: 10, font: bold });
        y -= 38;
      }
      page.drawRectangle({ x: 42, y: y - 17, width: 511, height: 45, color: rgb(0.95, 0.97, 0.99), borderColor: rgb(0.18, 0.28, 0.4), borderWidth: 1 });
      page.drawText(totalLine.slice(0, 90), { x: 365, y: y - 1, size: 11, font: bold, color: rgb(0.07, 0.14, 0.24) });
      page.drawText(lines.at(-2)?.slice(0, 100) || '', { x: 42, y: 178, size: 7, font: regular, color: rgb(0.4, 0.45, 0.5) });
      page.drawText(lines.at(-1)?.slice(0, 100) || '', { x: 42, y: 164, size: 7, font: regular, color: rgb(0.4, 0.45, 0.5) });
    } else {
      let y = 750;
      for (const line of lines) {
        page.drawText(line.slice(0, 110), { x: 42, y, size: 10, font: regular, color: rgb(0.08, 0.12, 0.2) });
        y -= 18;
      }
    }
  }
  page.drawRectangle({ x: 36, y: 38, width: page.getWidth() - 72, height: 105, borderColor: rgb(0.15, 0.25, 0.4), borderWidth: 1 });
  if (phase === 'FINAL') {
    page.drawText(`Approved by ${workflow.manager_name || contract.manager?.name || ''}`, { x: 48, y: 116, size: 10, font: bold });
    page.drawText(`Position: ${workflow.manager_position || contract.manager?.position || ''}`, { x: 48, y: 98, size: 9, font: regular });
    page.drawText(`Approval date: ${text(workflow.manager_approved_at_utc || contract.manager?.approval_date_utc).slice(0, 10)}`, { x: 48, y: 80, size: 9, font: regular });
    const signature = await signatureAsset(env, workflow.manager_signature_component_id, contract.manager?.signature_storage_key, contract.manager?.signature_sha256);
    if (!signature.data?.data_url) throw new CandidateHttpError(409, 'MANAGER_SIGNATURE_REQUIRED');
    const decoded = base64UrlDecode(signature.data.data_url.split(',')[1].replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, ''));
    const image = signature.data.data_url.startsWith('data:image/png') ? await pdf.embedPng(decoded) : await pdf.embedJpg(decoded);
    const scale = Math.min(180 / image.width, 55 / image.height);
    page.drawImage(image, { x: 330, y: 70, width: image.width * scale, height: image.height * scale });
  } else {
    page.drawText('Hiring manager review', { x: 48, y: 112, size: 11, font: bold });
    page.drawText('Review this page together with every page in the manifest before approval.', { x: 48, y: 91, size: 9, font: regular });
    page.drawText('Manager signature and approval date intentionally blank.', { x: 48, y: 72, size: 9, font: regular });
  }
  return { pdf_bytes: new Uint8Array(await pdf.save()), page_count: 1 };
}

function candidateRpcArgs(access, env, overrides = {}) {
  return {
    p_session_id: access?.session_id || null,
    p_environment: access?.environment || environmentName(env),
    p_now_utc: new Date().toISOString(),
    ...overrides
  };
}

function workflowActionArgs(access, env, workflowId, action, generation, payload, idempotencyKey) {
  return candidateRpcArgs(access, env, {
    p_workflow_id: requireUuid(workflowId, 'CANDIDATE_WORKFLOW_NOT_FOUND'),
    p_action: upper(action),
    p_expected_generation: generation == null ? null : requireInteger(generation, 'WORKFLOW_GENERATION_CONFLICT', 1),
    p_payload: isObject(payload) ? payload : {},
    p_idempotency_key: requireCandidateIdempotency(idempotencyKey)
  });
}

async function probeWorkflowMutationReplay(
  env, deps, access, workflowId, action, generation, idempotencyKey, semanticPayload
) {
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    access, env, workflowId, action, generation, {
      mutation_replay_probe_only: true,
      mutation_replay_semantic_payload: isObject(semanticPayload) ? semanticPayload : {}
    }, idempotencyKey
  ));
  return result?.idempotent_replay === true ? result : null;
}

async function workflowRow(env, workflowId) {
  const row = await restOne(env, 'candidate_submission_workflows',
    `id=eq.${encodeURIComponent(requireUuid(workflowId, 'CANDIDATE_WORKFLOW_NOT_FOUND'))}&select=*`);
  if (!row) throw new CandidateHttpError(404, 'CANDIDATE_WORKFLOW_NOT_FOUND');
  return row;
}

function forbiddenFinancialKeys(value, path = '') {
  if (!isObject(value) && !Array.isArray(value)) return [];
  const prohibited = /(?:^canonical_tsfin_snapshot$|^rates?$|^rate_source_refs_json$|(?:^|_)(?:pay|charge|margin|vat|erni)(?:_|$)|^invoice_breakdown_json$|^processing_status$|^authorised_at_utc$|^paid_at_utc$)/i;
  const hits = [];
  for (const [key, child] of Object.entries(value)) {
    const current = path ? `${path}.${key}` : key;
    if (prohibited.test(key)) hits.push(current);
    if (isObject(child) || Array.isArray(child)) hits.push(...forbiddenFinancialKeys(child, current));
  }
  return hits;
}

function managerRouteHmacSecret(env) {
  const secret = text(env.MYTMS_MANAGER_ROUTE_HMAC_SECRET);
  if (secret.length < 32) throw new CandidateHttpError(503, 'MANAGER_ROUTE_CONFIGURATION_UNAVAILABLE');
  return secret;
}

function managerAgencyId(env) {
  return requireUuid(env.CANDIDATE_AGENCY_ID || env.MYTMS_OFFICE_AGENCY_ID,
    'MANAGER_ROUTE_CONFIGURATION_UNAVAILABLE');
}

function escapeManagerMailHtml(value) {
  return String(value == null ? '' : value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}

async function candidateManagerMail(env, deps, token, workflowId, managerEmail, kind = 'INITIAL', workflowKind = '') {
  const environment = environmentName(env);
  const agencyId = managerAgencyId(env);
  const [originAuthority, templateAuthority] = await Promise.all([
    routedManagerControlRpc(env, deps, 'control', 'manager_review_origin_resolve_v1', {
      p_agency_id: agencyId, p_environment_label: environment
    }),
    deps.rpc('candidate_manager_email_settings_get_v1', {})
  ]);
  const origin = text(originAuthority?.manager_review_public_origin).replace(/\/$/, '');
  if (!/^https:\/\/[A-Za-z0-9.-]+(?::\d{1,5})?$/.test(origin)) {
    throw new CandidateHttpError(503, 'MANAGER_REVIEW_ORIGIN_UNAVAILABLE');
  }
  const link = `${origin}/manager/timesheet/${encodeURIComponent(workflowId)}#token=${encodeURIComponent(token)}`;
  const submissionType = upper(workflowKind) === 'CONTRACT_EXPENSE' ? 'EXPENSE_CLAIM' : 'TIMESHEET';
  const workflow = await workflowRow(env, workflowId);
  const candidate = await restOne(env, 'candidates',
    `id=eq.${encodeURIComponent(requireUuid(workflow.candidate_id, 'CANDIDATE_WORKFLOW_NOT_FOUND'))}`
    + '&select=id,display_name,first_name,last_name');
  const candidateName = text(candidate?.display_name
    || `${candidate?.first_name || ''} ${candidate?.last_name || ''}`).trim();
  if (!candidateName || candidateName.length > 200) {
    throw new CandidateHttpError(503, 'MANAGER_EMAIL_CANDIDATE_NAME_UNAVAILABLE');
  }
  const template = templateAuthority?.templates?.[submissionType]?.[kind];
  if (!isObject(template) || template.include_link !== true || !text(template.subject)
      || !text(template.body_text) || !text(template.body_html) || !text(template.button_text)
      || /<(?:a|script|style|iframe|form)\b|\b(?:href|src)\s*=|https?:\/\//i.test(template.body_html)
      || template.subject.length > 240 || template.body_text.length > 20_000
      || template.body_html.length > 50_000 || template.button_text.length > 80) {
    throw new CandidateHttpError(503, 'MANAGER_EMAIL_TEMPLATE_UNAVAILABLE');
  }
  const expiryText = 'This secure link expires seven days after it is issued.';
  const safeLink = link.replaceAll('&', '&amp;').replaceAll('"', '&quot;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
  const submissionLabel = submissionType === 'EXPENSE_CLAIM' ? 'expense claim' : 'timesheet';
  const actionText = `${template.button_text} ${candidateName} ${submissionLabel}`;
  return {
    manager_email: normaliseEmail(managerEmail),
    mail: {
      to: normaliseEmail(managerEmail), subject: template.subject,
      body_text: `${template.body_text}\n\n${expiryText}\n\n${actionText}: ${link}`,
      body_html: `${template.body_html}<p>${expiryText}</p><p><a href="${safeLink}" rel="noopener noreferrer">${escapeManagerMailHtml(actionText)}</a></p>`,
      manager_template_version: templateAuthority.version,
      manager_template_sha256: templateAuthority.semantic_sha256_hex,
      manager_origin_version: originAuthority.settings_version,
      manager_origin_sha256: originAuthority.manager_review_origin_semantic_sha256_hex,
      manager_submission_type: submissionType
    }
  };
}

async function candidateManagerTerminalMail(deps, kind, workflowKind = '') {
  const mailKind = upper(kind);
  if (!['WITHDRAWAL', 'CANCELLATION'].includes(mailKind)) {
    throw new CandidateHttpError(500, 'MANAGER_EMAIL_TEMPLATE_UNAVAILABLE');
  }
  const templateAuthority = await deps.rpc('candidate_manager_email_settings_get_v1', {});
  const submissionType = upper(workflowKind) === 'CONTRACT_EXPENSE' ? 'EXPENSE_CLAIM' : 'TIMESHEET';
  const template = templateAuthority?.templates?.[submissionType]?.[mailKind];
  if (!isObject(template) || template.include_link !== false || !text(template.subject)
      || !text(template.body_text) || !text(template.body_html) || template.button_text !== null
      || /<(?:a|img|svg|script|style|iframe|form)\b|\b(?:href|src|on\w+)\s*=|https?:\/\//i.test(template.body_html)
      || template.subject.length > 240 || template.body_text.length > 20_000
      || template.body_html.length > 50_000) {
    throw new CandidateHttpError(503, 'MANAGER_EMAIL_TEMPLATE_UNAVAILABLE');
  }
  return {
    subject: template.subject, body_text: template.body_text, body_html: template.body_html,
    manager_template_version: templateAuthority.version,
    manager_template_sha256: templateAuthority.semantic_sha256_hex,
    manager_submission_type: submissionType
  };
}

async function registerManagerEmailRoute(env, deps, {
  workflowId, approvalRequestId, requestGeneration, credentialGeneration,
  mailOutboxId, managerToken, mailKind, expiresAtUtc, mutationKey
}) {
  const secret = managerRouteHmacSecret(env);
  const environment = environmentName(env);
  const agencyId = managerAgencyId(env);
  const credentialHmac = await requestHmacSha256(secret, 'manager-email-credential-v1', managerToken);
  const workflowHmac = await requestHmacSha256(secret, 'manager-email-workflow-v1', workflowId);
  const requestHmac = await requestHmacSha256(secret, 'manager-email-request-v1', approvalRequestId);
  const idempotencyHmac = await requestHmacSha256(
    secret, 'manager-email-route-idempotency-v1',
    { workflow_id: workflowId, approval_request_id: approvalRequestId, mail_kind: mailKind,
      mutation_key: mutationKey }
  );
  const issuedAtUtc = new Date().toISOString();
  const registrationFacts = {
    contract_version: 'MANAGER_EMAIL_ROUTE_REGISTRATION_V1', environment_label: environment,
    agency_id: agencyId, credential_hmac_hex: credentialHmac,
    workflow_route_hmac_hex: workflowHmac,
    approval_request_route_hmac_hex: requestHmac, request_generation: requestGeneration,
    credential_generation: credentialGeneration, mail_kind: mailKind,
    expires_at_utc: expiresAtUtc
  };
  const semanticSha256 = await sha256Hex(canonicalJson(registrationFacts));
  const route = await routedManagerControlRpc(
    env, deps, 'control', 'manager_email_route_register_v1', {
    p_registration: {
      ...registrationFacts, authority_kind: 'MANAGER_EMAIL',
      credential_key_version: 1, semantic_sha256_hex: semanticSha256,
      idempotency_key_hmac_hex: idempotencyHmac, issued_at_utc: issuedAtUtc
    },
    p_now_utc: issuedAtUtc
  });
  const receipt = await deps.rpc('candidate_manager_email_route_receipt_commit_v1', {
    p_environment: environment, p_workflow_id: workflowId,
    p_approval_request_id: approvalRequestId, p_request_generation: requestGeneration,
    p_credential_generation: credentialGeneration, p_mail_outbox_id: mailOutboxId,
    p_manager_token_hash_hex: await sha256Hex(managerToken),
    p_manager_route_ticket_id: route.manager_route_ticket_id,
    p_route_revision: route.route_revision,
    p_registration_receipt_sha256_hex: route.registration_receipt_sha256_hex,
    p_route_semantic_sha256_hex: semanticSha256, p_mail_kind: mailKind,
    p_idempotency_key: mutationKey, p_now_utc: issuedAtUtc
  });
  return { route, receipt };
}

function renderContracts(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.components)) return value.components;
  return [];
}

function safeQrPackResponse(value) {
  const source = isObject(value) ? value : {};
  return {
    queued: source.queued === true,
    send_state: text(source.send_state) || null,
    document_state: text(source.document_state || source.document_version_status) || null,
    document_operation_id: UUID_RE.test(text(source.document_operation_id)) ? text(source.document_operation_id) : null,
    current_timesheet_id: UUID_RE.test(text(source.current_timesheet_id)) ? text(source.current_timesheet_id) : null,
    timesheet_version: Number.isSafeInteger(Number(source.current_version || source.timesheet_version))
      ? Number(source.current_version || source.timesheet_version) : null,
    recipient_available: source.recipient_available === true
  };
}

function safeCandidateWorkflowPolicy(value) {
  const source = isObject(value) ? value : {};
  const manager = isObject(source.manager_approval_policy)
    ? source.manager_approval_policy : {};
  const textArray = (input) => Array.isArray(input)
    ? input.map(item => text(item).trim()).filter(Boolean) : [];
  return {
    paper_submission_enabled: source.paper_submission_enabled === true,
    allow_daily_manager_authorise_on_phone:
      source.allow_daily_manager_authorise_on_phone === true,
    allow_daily_manager_authorise_by_email:
      source.allow_daily_manager_authorise_by_email === true,
    manager_approval_policy: {
      approved_emails: textArray(manager.approved_emails),
      approved_domains: textArray(manager.approved_domains),
      allow_free_business_email: manager.allow_free_business_email === true
    }
  };
}

function safeExpensePlacement(value) {
  const source = isObject(value) ? value : {};
  const placement = upper(source.placement);
  if (!['BLOCKED', 'SAME_RECORD', 'REUSE_CARRIER', 'CREATE_CARRIER'].includes(placement)) {
    throw new CandidateHttpError(502, 'CANDIDATE_EXPENSE_PLACEMENT_INVALID');
  }
  const anchorContractWeekId = text(source.anchor_contract_week_id);
  if (!UUID_RE.test(anchorContractWeekId)) {
    throw new CandidateHttpError(502, 'CANDIDATE_EXPENSE_PLACEMENT_INVALID');
  }
  const capabilities = isObject(source.capabilities) ? source.capabilities : {};
  const anchorTimesheetId = text(source.anchor_timesheet_id);
  const targetTimesheetId = text(source.target_timesheet_id);
  const targetContractWeekId = text(source.target_contract_week_id);
  const idempotencyKey = text(source.idempotency_key);
  return {
    ok: true,
    placement,
    reason_code: text(source.reason_code) || null,
    anchor_timesheet_id: UUID_RE.test(anchorTimesheetId) ? anchorTimesheetId : null,
    anchor_contract_week_id: anchorContractWeekId,
    target_timesheet_id: UUID_RE.test(targetTimesheetId) ? targetTimesheetId : null,
    target_contract_week_id: UUID_RE.test(targetContractWeekId) ? targetContractWeekId : null,
    target_record_role: text(source.target_record_role) || null,
    capabilities: {
      can_use_same_record: placement === 'SAME_RECORD',
      can_reuse_carrier: placement === 'REUSE_CARRIER',
      can_create_carrier: placement === 'CREATE_CARRIER',
      can_edit_expenses: capabilities.can_edit_expenses === true,
      requires_carrier: capabilities.requires_carrier === true
        || placement === 'REUSE_CARRIER' || placement === 'CREATE_CARRIER'
    },
    ...(typeof source.idempotent_replay === 'boolean'
      ? { idempotent_replay: source.idempotent_replay } : {}),
    ...(UUID_RE.test(idempotencyKey) ? { idempotency_key: idempotencyKey } : {})
  };
}

function normaliseCandidateWorkflowCreatePayload(value) {
  const supplied = isObject(value) ? value : {};
  const workflowKind = upper(supplied.workflow_kind);
  const timesheetId = UUID_RE.test(text(supplied.timesheet_id))
    ? text(supplied.timesheet_id) : null;
  return {
    ...supplied,
    ...(timesheetId && workflowKind === 'DAILY' && !supplied.target_timesheet_id
      ? { target_timesheet_id: timesheetId } : {}),
    ...(timesheetId && workflowKind === 'CONTRACT_EXPENSE' && !supplied.anchor_timesheet_id
      ? { anchor_timesheet_id: timesheetId } : {})
  };
}

function safePaperReturnPages(value) {
  const source = isObject(value) ? value : {};
  const pages = Array.isArray(source.pages) ? source.pages : [];
  return pages.map((page, index) => {
    if (!isObject(page) || !text(page.page_key).trim() || !text(page.component_kind).trim()) {
      throw new CandidateHttpError(409, 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE');
    }
    const componentKind = upper(page.component_kind);
    const pageKey = text(page.page_key).trim();
    const sourceComponentId = page.source_component_id == null
      ? null : requireUuid(page.source_component_id, 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE');
    return {
      ordinal: index + 1,
      page_key: pageKey,
      component_kind: componentKind,
      expense_category: page.expense_category == null
        ? null : upper(page.expense_category),
      source_component_id: sourceComponentId,
      qr_required: componentKind === 'HOURS_TIMESHEET' && pageKey === 'HOURS_TIMESHEET'
    };
  });
}

async function bindCandidatePaperOutbox(env, workflow, timesheetId, pack) {
  if (pack?.recipient_available !== true || pack?.queued !== true) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE');
  }
  const outboxId = requireUuid(pack.mail_outbox_id, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  const row = await restOne(env, 'mail_outbox',
    `id=eq.${encodeURIComponent(outboxId)}`
    + '&select=id,type,context_kind,context_id,status,payment_scope_json,attachments,'
    + 'scheduled_for_utc,next_attempt_at_utc,attempt_lease_token,attempt_lease_expires_at_utc');
  if (!row || row.type !== 'TIMESHEET_QR' || row.context_kind !== 'timesheets'
      || row.context_id !== timesheetId) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_IDENTITY_CONFLICT');
  }
  const binding = {
    candidate_workflow_id: workflow.id,
    candidate_workflow_generation: Number(workflow.generation),
    paper_return_manifest_sha256: text(workflow.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase()
  };
  if (!UUID_RE.test(text(binding.candidate_workflow_id))
      || !Number.isSafeInteger(binding.candidate_workflow_generation)
      || binding.candidate_workflow_generation < 1
      || !SHA256_RE.test(binding.paper_return_manifest_sha256)) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_IDENTITY_CONFLICT');
  }
  const existing = parseJson(row.payment_scope_json, {}) || {};
  for (const [key, value] of Object.entries(binding)) {
    if (String(existing[key] ?? '') !== String(value)) {
      throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_IDENTITY_CONFLICT');
    }
  }
  if (row.status === 'FAILED') throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_FAILED');
  if (['QUEUED', 'SENT'].includes(row.status) && candidateCompletePackAttachmentMatchesScope(row)) {
    return { bound: true, recipient_available: true, mail_outbox_id: outboxId, pack_ready: true };
  }
  if (row.status === 'SENT') throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_ALREADY_SENT');
  const attachments = parseJson(row.attachments, []) || [];
  const held = existing.candidate_paper_pack_ready === false
    && existing.mail_held_until_pdf_rendered === true
    && existing.mail_hold_reason === 'CANDIDATE_PAPER_PACK_PENDING'
    && attachments.length === 0
    && row.status === 'QUEUED'
    && !text(row.attempt_lease_token);
  if (!held) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  }
  return { bound: true, recipient_available: true, mail_outbox_id: outboxId };
}

function withoutInternalRenderContracts(value) {
  if (!isObject(value)) return value;
  const safe = { ...value };
  delete safe.render_contract;
  delete safe.final_render_contract;
  delete safe.daily_materialisation_contract;
  return safe;
}

function safeFinalisationResult(value) {
  const source = isObject(value) ? value : {};
  const safe = {};
  const scalarKeys = [
    'ok', 'workflow_id', 'generation', 'state', 'idempotent_replay',
    'target_timesheet_id', 'hours_timesheet_id', 'expense_timesheet_id',
    'auto_authorised', 'finalisation_pending', 'reason', 'error_code'
  ];
  for (const key of scalarKeys) {
    if (Object.prototype.hasOwnProperty.call(source, key)) safe[key] = source[key];
  }
  if (Array.isArray(source.issue_codes)) safe.issue_codes = source.issue_codes;
  if (Array.isArray(source.auto_authorisation_blockers)) {
    safe.auto_authorisation_blockers = source.auto_authorisation_blockers;
  }
  return safe;
}

async function renderComponentDocument(env, contract, phase, stateOverride = null) {
  const state = stateOverride || await loadRenderState(env, contract);
  if (upper(contract.component_kind) === 'HOURS_TIMESHEET') {
    const { model, assets } = await buildOfficialCandidateModel(env, contract, state, phase);
    const rendered = await renderOfficialTimesheetPdfBytes(model, assets);
    return {
      pdf_bytes: rendered.pdf_bytes,
      page_count: rendered.page_count,
      candidate_signature_sha256: text(state.workflow.candidate_signature_sha256).replace(/^\\x/i, '') || null
    };
  }
  return renderExpensePage(env, contract, state, phase);
}

async function readyGeneratedDocumentReceipt(env, key, expected) {
  const object = await env.R2?.head(key);
  if (!object) return null;
  const metadata = object.customMetadata || {};
  const valid = metadata.purpose === expected.purpose
    && metadata.workflow_id === expected.workflow_id
    && metadata.workflow_generation === String(expected.workflow_generation)
    && metadata.component_id === expected.component_id
    && text(metadata.render_input_sha256).toLowerCase() === expected.render_input_sha256
    && text(metadata.branding_contract_sha256).toLowerCase() === expected.branding_contract_sha256
    && text(metadata.renderer_contract_version) === expected.renderer_contract_version
    && text(metadata.media_type).toLowerCase() === 'application/pdf'
    && SHA256_RE.test(text(metadata.sha256))
    && Number(metadata.byte_size) === Number(object.size)
    && Number(metadata.page_count) > 0;
  if (!valid) throw new CandidateHttpError(409, 'CANDIDATE_RENDER_IDEMPOTENCY_CONFLICT');
  return {
    sha256: text(metadata.sha256).toLowerCase(),
    byte_size: Number(object.size),
    page_count: Number(metadata.page_count)
  };
}

async function renderAndRegister(env, deps, renderContract, phase, officeActorId = null) {
  const contracts = renderContracts(renderContract);
  if (!contracts.length) throw new CandidateHttpError(409, 'MANAGER_REVIEW_DOCUMENT_NOT_READY');
  const results = [];
  for (const contract of contracts) {
    const workflowId = requireUuid(contract.workflow_id, 'CANDIDATE_WORKFLOW_NOT_FOUND');
    const generation = requireInteger(contract.workflow_generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
    const componentId = requireUuid(contract.component_id, 'CANDIDATE_COMPONENT_NOT_FOUND');
    const renderInputHash = text(contract.render_input_sha256).toLowerCase();
    if (!SHA256_RE.test(renderInputHash)) throw new CandidateHttpError(409, 'CANDIDATE_RENDER_CONTRACT_INVALID');
    const state = await loadRenderState(env, contract);
    const frozen = parseJson(state.workflow.immutable_submission_json, {}) || {};
    const brandingHash = text(frozen.official_presentation?.branding?.branding_contract_sha256).toLowerCase();
    const rendererVersion = text(state.workflow.renderer_contract_version || frozen.official_presentation?.renderer_contract_version);
    if (!SHA256_RE.test(brandingHash)) {
      throw new CandidateHttpError(409, 'CANDIDATE_DOCUMENT_BRANDING_CONTRACT_INVALID');
    }
    if (rendererVersion !== RENDERER_CONTRACT_VERSION) {
      throw new CandidateHttpError(409, 'CANDIDATE_RENDERER_CONTRACT_UNSUPPORTED');
    }
    const storageKey = `candidate-app/${environmentName(env).toLowerCase()}/${workflowId}/${generation}/${phase.toLowerCase()}/${String(contract.review_ordinal).padStart(3, '0')}-${componentId}-${renderInputHash}-${rendererVersion}.pdf`;
    const identity = {
      purpose: `candidate-${phase.toLowerCase()}`,
      workflow_id: workflowId,
      workflow_generation: generation,
      component_id: componentId,
      render_input_sha256: renderInputHash,
      branding_contract_sha256: brandingHash,
      renderer_contract_version: rendererVersion
    };
    let durable = await readyGeneratedDocumentReceipt(env, storageKey, identity);
    let rendered = null;
    if (!durable) {
      rendered = await renderComponentDocument(env, contract, phase, state);
      const digest = await sha256Hex(rendered.pdf_bytes);
      await immutablePut(env, storageKey, rendered.pdf_bytes, 'application/pdf', {
        ...identity,
        workflow_generation: String(generation),
        renderer_contract_version: rendererVersion,
        page_count: String(Number(rendered.page_count || 1)),
        sha256: digest
      });
      durable = {
        sha256: digest,
        byte_size: rendered.pdf_bytes.byteLength,
        page_count: Number(rendered.page_count || 1)
      };
    }
    const digest = durable.sha256;
    const receipt = {
      form_variant: upper(contract.form_variant),
      workflow_id: workflowId,
      workflow_generation: generation,
      component_id: componentId,
      component_kind: upper(contract.component_kind),
      document_role: upper(contract.document_role),
      review_ordinal: Number(contract.review_ordinal),
      scope: upper(contract.scope),
      page_count: durable.page_count,
      render_input_sha256: renderInputHash,
      branding_contract_sha256: brandingHash,
      renderer_contract_version: rendererVersion,
      candidate_signature_embedded: contract.candidate_signature_embedded === true,
      manager_signature_embedded: phase === 'FINAL',
      manager_approval_date_embedded: phase === 'FINAL'
    };
    if (phase === 'FINAL') {
      receipt.manager_signature_sha256 = text(contract.manager?.signature_sha256).replace(/^\\x/i, '').toLowerCase();
      receipt.manager_name = text(contract.manager?.name);
      receipt.manager_position = text(contract.manager?.position);
      receipt.manager_approved_at_utc = contract.manager?.approval_date_utc;
      const candidateSignatureSha256 = rendered?.candidate_signature_sha256
        || text(state.workflow.candidate_signature_sha256).replace(/^\\x/i, '').toLowerCase();
      if (candidateSignatureSha256) receipt.candidate_signature_sha256 = candidateSignatureSha256;
    }
    const action = phase === 'FINAL' ? 'REGISTER_FINAL_SIGNED_DOCUMENT' : 'REGISTER_REVIEW_COMPONENT';
    const registrationPayload = {
        component_id: componentId,
        storage_key: storageKey,
        media_type: 'application/pdf',
        byte_size: durable.byte_size,
        page_count: durable.page_count,
        content_sha256_hex: digest,
        render_input_sha256_hex: renderInputHash,
        renderer_contract_version: rendererVersion,
        renderer_receipt: receipt
      };
    const registrationKey = `candidate-render:${phase.toLowerCase()}:${workflowId}:${generation}:${componentId}:${digest}`;
    const result = officeActorId
      ? await officeAdapter(deps, env, officeActorId, 'WORKFLOW_ACTION_EXECUTE', {
        workflow_id: workflowId,
        generation,
        workflow_action: action,
        idempotency_key: registrationKey,
        payload: registrationPayload
      })
      : await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
        null, env, workflowId, action, generation, registrationPayload, registrationKey
      ));
    results.push(result);
  }
  return results;
}

async function resolveWorkflowSession(env, workflow) {
  const session = await restOne(env, 'candidate_app_sessions',
    `account_id=eq.${encodeURIComponent(workflow.account_id)}&environment=eq.${encodeURIComponent(workflow.environment)}` +
    '&status=eq.ACTIVE&expires_at_utc=gt.now()&absolute_expires_at_utc=gt.now()' +
    '&select=id,account_id,environment,selected_candidate_id,rotation,expires_at_utc,absolute_expires_at_utc&order=last_used_at_utc.desc');
  if (!session || session.selected_candidate_id !== workflow.candidate_id) return null;
  return { session_id: session.id, account_id: session.account_id, environment: session.environment, rotation: Number(session.rotation) };
}

async function lifecycleSignature(deps, workflow) {
  const result = await rpcCall(deps, 'timesheet_lifecycle_signature_v1', {
    p_timesheet_id: workflow.target_timesheet_id || workflow.anchor_timesheet_id,
    p_contract_week_id: workflow.contract_week_id || null,
    p_include_payload: false
  });
  return text(result?.backend_row_signature || result?.mutation_row_signature || result?.row_signature || result?.expected_row_signature);
}

async function probeFinalisationReplay(
  env, deps, workflow, generation, idempotencyKey, serviceFinalisation, officeActorId = null
) {
  const finalisationKey = workflow.workflow_kind === 'DAILY'
    ? `${idempotencyKey}:finalise` : idempotencyKey;
  const dailyMaterialisationJson = {
    service_finalisation: { ...serviceFinalisation, replay_probe_only: true }
  };
  const result = officeActorId
    ? await officeAdapter(deps, env, officeActorId, 'FINALISE_REPLAY_LOOKUP', {
      workflow_id: workflow.id,
      generation,
      idempotency_key: finalisationKey,
      daily_materialisation_json: dailyMaterialisationJson
    })
    : await rpcCall(deps, 'candidate_submission_finalize_atomic_v1', {
      p_session_id: null,
      p_environment: environmentName(env),
      p_workflow_id: workflow.id,
      p_expected_generation: generation,
      p_expected_row_signature: null,
      p_idempotency_key: finalisationKey,
      p_now_utc: new Date().toISOString(),
      p_daily_materialisation_json: dailyMaterialisationJson
    });
  return result?.idempotent_replay === true ? safeFinalisationResult(result) : null;
}

async function probeFinalisationReplayByKey(
  env, deps, workflow, generation, idempotencyKey, officeActorId = null
) {
  const finalisationKey = workflow.workflow_kind === 'DAILY'
    ? `${idempotencyKey}:finalise` : idempotencyKey;
  const result = officeActorId
    ? await officeAdapter(deps, env, officeActorId, 'FINALISE_REPLAY_LOOKUP', {
      workflow_id: workflow.id,
      generation,
      idempotency_key: finalisationKey,
      replay_key_probe_only: true
    })
    : await rpcCall(deps, 'candidate_submission_finalize_atomic_v1', {
      p_session_id: null,
      p_environment: environmentName(env),
      p_workflow_id: workflow.id,
      p_expected_generation: generation,
      p_expected_row_signature: null,
      p_idempotency_key: finalisationKey,
      p_now_utc: new Date().toISOString(),
      p_daily_materialisation_json: {
        service_finalisation: {
          contract_version: 'CANDIDATE_MANAGER_FINALISATION_V1',
          workflow_generation: generation,
          replay_key_probe_only: true
        }
      }
    });
  return result?.idempotent_replay === true ? safeFinalisationResult(result) : null;
}

async function finaliseWorkflow(env, deps, workflowId, generation, idempotencyKey, officeActorId = null) {
  idempotencyKey = requireCandidateIdempotency(idempotencyKey);
  const workflow = await workflowRow(env, workflowId);
  const receiptReplay = await probeFinalisationReplayByKey(
    env, deps, workflow, generation, idempotencyKey, officeActorId
  );
  if (receiptReplay) return receiptReplay;
  const approved = upper(workflow.route) === 'PAPER' ? null : await restOne(env, 'candidate_approval_requests',
    `workflow_id=eq.${encodeURIComponent(workflow.id)}&workflow_generation=eq.${encodeURIComponent(generation)}`
    + '&state=eq.APPROVED&select=id,request_generation,method,review_manifest_sha256,approved_at_utc&order=approved_at_utc.desc');
  const finalisationIdentity = {
    contract_version: 'CANDIDATE_FINALISATION_IDENTITY_V1',
    workflow_id: workflow.id,
    workflow_generation: generation,
    approval_method: approved?.method || 'PAPER',
    approval_request_id: approved?.id || null,
    approval_request_generation: approved ? Number(approved.request_generation) : null,
    review_manifest_sha256_hex: text(approved?.review_manifest_sha256).replace(/^\\x/i, '').toLowerCase() || null,
    paper_return_manifest_sha256_hex: upper(workflow.route) === 'PAPER'
      ? text(workflow.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase() : null
  };
  const replayServiceFinalisation = {
    contract_version: 'CANDIDATE_MANAGER_FINALISATION_V1',
    workflow_generation: generation,
    approval_request_id: approved?.id || null,
    approval_request_generation: approved ? Number(approved.request_generation) : null,
    approval_method: approved?.method || 'PAPER',
    review_manifest_sha256_hex: finalisationIdentity.review_manifest_sha256_hex || '',
    paper_return_manifest_sha256_hex: finalisationIdentity.paper_return_manifest_sha256_hex || '',
    finalisation_identity: finalisationIdentity,
    actor_user_id: officeActorId || null
  };
  const replay = await probeFinalisationReplay(
    env, deps, workflow, generation, idempotencyKey, replayServiceFinalisation, officeActorId
  );
  if (replay) return replay;
  if (upper(workflow.route) !== 'PAPER' && !approved) {
    throw new CandidateHttpError(409, 'FINAL_SIGNED_DOCUMENT_NOT_READY');
  }
  const serviceFinalisation = {
    contract_version: 'CANDIDATE_MANAGER_FINALISATION_V1',
    approval_request_id: approved?.id || null,
    approval_request_generation: approved ? Number(approved.request_generation) : null,
    approval_method: approved?.method || 'PAPER',
    workflow_generation: generation,
    review_manifest_sha256_hex: text(approved?.review_manifest_sha256).replace(/^\\x/i, '').toLowerCase(),
    paper_return_manifest_sha256_hex: finalisationIdentity.paper_return_manifest_sha256_hex || '',
    finalisation_identity: finalisationIdentity,
    actor_user_id: officeActorId || null
  };
  if (workflow.workflow_kind === 'DAILY') {
    return safeFinalisationResult(await deps.finaliseDaily({
      sessionId: null,
      environment: environmentName(env),
      workflowId: workflow.id,
      expectedGeneration: generation,
      idempotencyKey,
      nowUtc: new Date().toISOString(),
      serviceFinalisation,
      officeActorId
    }));
  }
  const finalisationPayload = {
    p_session_id: null,
    p_environment: environmentName(env),
    p_workflow_id: workflow.id,
    p_expected_generation: generation,
    p_expected_row_signature: await lifecycleSignature(deps, workflow),
    p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString(),
    p_daily_materialisation_json: { service_finalisation: serviceFinalisation }
  };
  return safeFinalisationResult(officeActorId
    ? await officeAdapter(deps, env, officeActorId, 'FINALISE_EXECUTE', {
      workflow_id: workflow.id,
      generation,
      expected_row_signature: finalisationPayload.p_expected_row_signature,
      idempotency_key: finalisationPayload.p_idempotency_key,
      daily_materialisation_json: finalisationPayload.p_daily_materialisation_json
    })
    : await rpcCall(deps, 'candidate_submission_finalize_atomic_v1', finalisationPayload));
}

async function handleCandidateRead(request, env, deps, kind, params = {}) {
  const access = await verifyCandidateAccess(request, env);
  const url = new URL(request.url);
  if (kind === 'bootstrap') {
    const bootstrap = await rpcCall(deps, 'candidate_app_bootstrap_v1', candidateRpcArgs(access, env, {
      p_expected_rotation: access.rotation
    }));
    const correlationId = candidateBootstrapCorrelation(request);
    const response = composeCandidateBootstrapPhase1b(bootstrap);
    response.notification_preferences = safeCandidateNotificationPreferences(
      response.notification_preferences
    );
    return jsonResponse(200, response, {
      'x-correlation-id': correlationId
    });
  }
  if (kind === 'page') {
    const requestedView = text(url.searchParams.get('view') || 'current').toLowerCase();
    if (!['current', 'history'].includes(requestedView)) {
      throw new CandidateHttpError(400, 'CANDIDATE_VIEW_INVALID');
    }
    const cursor = url.searchParams.get('cursor');
    if (cursor && cursor.length > 2048) {
      throw new CandidateHttpError(400, 'CANDIDATE_CURSOR_INVALID');
    }
    const rawLimit = Number(url.searchParams.get('limit') || 50);
    if (!Number.isSafeInteger(rawLimit) || rawLimit < 1 || rawLimit > 100) {
      throw new CandidateHttpError(400, 'CANDIDATE_PAGE_LIMIT_INVALID');
    }
    return jsonResponse(200, await rpcCall(deps, 'candidate_app_timesheet_page_v1', candidateRpcArgs(access, env, {
      p_view: requestedView.toUpperCase(),
      p_cursor: cursor,
      p_limit: rawLimit
    })));
  }
  if (kind === 'detail') {
    return jsonResponse(200, await rpcCall(deps, 'candidate_app_timesheet_detail_v2', candidateRpcArgs(access, env, {
      p_timesheet_id: params.timesheetId ? requireUuid(params.timesheetId) : null,
      p_contract_week_id: params.contractWeekId ? requireUuid(params.contractWeekId)
        : (url.searchParams.get('contract_week_id') ? requireUuid(url.searchParams.get('contract_week_id')) : null),
      p_workflow_id: params.workflowId ? requireUuid(params.workflowId)
        : (url.searchParams.get('workflow_id') ? requireUuid(url.searchParams.get('workflow_id')) : null)
    })));
  }
  if (kind === 'missing-options') {
    return jsonResponse(200, await rpcCall(deps, 'candidate_missing_week_options_v1', candidateRpcArgs(access, env, {
      p_contract_id: requireUuid(params.contractId),
      p_from: text(url.searchParams.get('from')),
      p_to: text(url.searchParams.get('to'))
    })));
  }
  throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
}

async function handleAddMissingWeek(request, env, deps, contractId) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const result = await rpcCall(deps, 'candidate_contract_week_add_missing_atomic_v1', candidateRpcArgs(access, env, {
    p_contract_id: requireUuid(contractId),
    p_week_ending_date: text(body.week_ending_date),
    p_idempotency_key: requireCandidateIdempotency(body.idempotency_key)
  }));
  return jsonResponse(201, result);
}

async function handleExpensePlacement(request, env, deps, timesheetId, createCarrier = false) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const anchorTimesheetId = requireUuid(timesheetId);
  if (createCarrier) {
    const result = await rpcCall(deps, 'expense_carrier_resolve_or_create_atomic_v1', {
      p_candidate_id: requireUuid(access.selected_candidate_id, 'CANDIDATE_SELECTION_REQUIRED'),
      p_environment: access.environment,
      p_anchor_timesheet_id: anchorTimesheetId,
      p_expected_row_signature: text(body.expected_row_signature) || null,
      p_idempotency_key: requireCandidateIdempotency(body.idempotency_key),
      p_now_utc: new Date().toISOString()
    });
    return jsonResponse(200, safeExpensePlacement(result));
  }
  const proposedClaim = isObject(body.proposed_claim) ? body.proposed_claim : {};
  const forbidden = forbiddenFinancialKeys(proposedClaim);
  if (forbidden.length) {
    throw new CandidateHttpError(400, 'CANDIDATE_FINANCIAL_AUTHORITY_FORBIDDEN', { fields: forbidden.slice(0, 20) });
  }
  const result = await rpcCall(deps, 'expense_placement_resolve_v1', {
    p_candidate_id: requireUuid(access.selected_candidate_id, 'CANDIDATE_SELECTION_REQUIRED'),
    p_environment: access.environment,
    p_anchor_timesheet_id: anchorTimesheetId,
    p_contract_week_id: body.contract_week_id ? requireUuid(body.contract_week_id) : null,
    p_proposed_claim: proposedClaim,
    p_now_utc: new Date().toISOString()
  });
  return jsonResponse(200, safeExpensePlacement(result));
}

async function handleWorkflowCreate(request, env, deps) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const workflowId = body.workflow_id ? requireUuid(body.workflow_id) : crypto.randomUUID();
  const payload = normaliseCandidateWorkflowCreatePayload(
    isObject(body.workflow) ? body.workflow : body
  );
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    access, env, workflowId, 'CREATE', null, payload,
    requireCandidateIdempotency(body.idempotency_key)
  ));
  return jsonResponse(201, {
    ...result,
    policy: safeCandidateWorkflowPolicy(result?.policy)
  });
}

async function handleWorkflowResubmit(request, env, deps, workflowId) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const generation = requireInteger(body.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  const idempotencyKey = requireUuid(body.idempotency_key, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    access, env, workflowId, 'RESUBMIT_REJECTED', generation, {}, idempotencyKey
  ));
  return jsonResponse(201, {
    ...result,
    policy: safeCandidateWorkflowPolicy(result?.policy)
  });
}

async function prepareImmutableSubmission(env, deps, workflow, body, mutationKey) {
  const supplied = isObject(body.immutable_submission) ? structuredClone(body.immutable_submission) : {};
  const forbidden = forbiddenFinancialKeys(supplied);
  if (forbidden.length) throw new CandidateHttpError(400, 'CANDIDATE_FINANCIAL_AUTHORITY_FORBIDDEN', { fields: forbidden.slice(0, 20) });
  const authoritySubmission = workflow.workflow_kind === 'DAILY'
    ? await deps.buildDailySubmission({ workflow, factualSubmission: supplied, mutationKey })
    : await deps.buildWeeklySubmission({ workflow, factualSubmission: supplied, mutationKey });
  return {
    ...authoritySubmission,
    official_presentation: await buildOfficialPresentationSnapshot(env, workflow)
  };
}

function pdfBase64(bytes) {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function escapeCandidateMailHtml(value) {
  return text(value).replace(/[&<>"']/g, (character) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  })[character]);
}

async function candidateMileageFormArtifact(env, workflow, mileageUnits) {
  const presentation = await buildOfficialPresentationSnapshot(env, workflow);
  const formWorkflow = {
    ...workflow,
    immutable_submission_json: {
      expense_claim: { mileage_units: mileageUnits, total_mileage: mileageUnits },
      official_presentation: presentation
    }
  };
  const bytes = await mileageClaimFormBytes(env, formWorkflow, null, presentation);
  const semanticSha256 = await sha256Hex(JSON.stringify({
    contract_version: 'CANDIDATE_MILEAGE_CLAIM_FORM_V1',
    workflow_id: workflow.id,
    workflow_generation: Number(workflow.generation),
    week_ending_date: workflow.week_ending_date,
    mileage_units: mileageUnits,
    branding_contract_sha256: presentation.branding?.branding_contract_sha256
  }));
  const storageKey = `candidate-app/${environmentName(env).toLowerCase()}/${workflow.id}/${workflow.generation}`
    + `/mileage-form/${semanticSha256}.pdf`;
  const stored = await immutablePut(env, storageKey, bytes, 'application/pdf', {
    purpose: 'candidate-mileage-claim-form',
    workflow_id: workflow.id,
    workflow_generation: String(workflow.generation),
    semantic_sha256: semanticSha256,
    page_count: '1'
  });
  return {
    bytes,
    storage_key: storageKey,
    sha256: stored.sha256,
    semantic_sha256: semanticSha256,
    filename: `Mileage_Claim_Form_${text(workflow.week_ending_date).slice(0, 10)}.pdf`,
    candidate_name: `${text(presentation.worker?.first_name)} ${text(presentation.worker?.surname)}`.trim(),
    agency_name: text(presentation.branding?.agency_name) || 'CloudTMS agency',
    idempotent_replay: stored.created !== true
  };
}

async function handleCandidateMileageFormAction(env, workflow, access, body, dbAction) {
  if (workflow.account_id !== access.account_id || workflow.candidate_id !== access.selected_candidate_id) {
    throw new CandidateHttpError(404, 'CANDIDATE_WORKFLOW_NOT_FOUND');
  }
  if (Number(workflow.generation) !== Number(body.generation)) {
    throw new CandidateHttpError(409, 'WORKFLOW_GENERATION_CONFLICT');
  }
  if (!['DRAFT', 'REJECTED'].includes(upper(workflow.state))) {
    throw new CandidateHttpError(409, 'CANDIDATE_WORKFLOW_NOT_MUTABLE');
  }
  if (!['CONTRACT_COMBINED', 'CONTRACT_EXPENSE'].includes(upper(workflow.workflow_kind))) {
    throw new CandidateHttpError(400, 'CANDIDATE_EXPENSE_CLAIM_NOT_ALLOWED');
  }
  const mileageUnits = Number(body.mileage_units ?? body.payload?.mileage_units);
  if (!Number.isFinite(mileageUnits) || mileageUnits <= 0 || mileageUnits > 1_000_000) {
    throw new CandidateHttpError(400, 'CANDIDATE_MILEAGE_UNITS_INVALID');
  }
  const artifact = await candidateMileageFormArtifact(env, workflow, mileageUnits);
  const common = {
    ok: true,
    workflow_id: workflow.id,
    generation: Number(workflow.generation),
    state: workflow.state,
    idempotent_replay: artifact.idempotent_replay,
    mileage_form_state: dbAction === 'MILEAGE_FORM_EMAIL' ? 'EMAIL_QUEUED' : 'PREPARED',
    mileage_form_filename: artifact.filename,
    mileage_form_sha256: artifact.sha256,
    mileage_form_byte_size: artifact.bytes.byteLength
  };
  if (dbAction === 'MILEAGE_FORM_PREPARE') {
    return { ...common, mileage_form_content_base64: pdfBase64(artifact.bytes) };
  }
  const account = await restOne(env, 'candidate_app_accounts',
    `id=eq.${encodeURIComponent(workflow.account_id)}`
    + `&environment=eq.${encodeURIComponent(environmentName(env))}`
    + '&status=eq.ACTIVE&select=id,email_normalized');
  if (!text(account?.email_normalized)) {
    throw new CandidateHttpError(409, 'CANDIDATE_REGISTERED_EMAIL_NOT_AVAILABLE');
  }
  const email = normaliseEmail(account?.email_normalized);
  const safeAgency = escapeCandidateMailHtml(artifact.agency_name);
  const safeCandidate = escapeCandidateMailHtml(artifact.candidate_name || 'Candidate');
  const bodyText = `${artifact.agency_name} has prepared the attached Mileage Claim Form for ${artifact.candidate_name || 'the Candidate'}.\n\n`
    + `Week ending: ${ukDate(workflow.week_ending_date)}\nTotal mileage: ${mileageUnits} miles\n\n`
    + 'Complete the journey details and obtain the required manager signature before returning the form in MyTMS.';
  const deterministicKey = `CANDIDATE_MILEAGE_FORM:${workflow.id}:${workflow.generation}:${artifact.semantic_sha256}`;
  const outbox = await restWrite(env, 'mail_outbox', 'POST', 'on_conflict=deterministic_outbox_key', {
    type: 'TIMESHEET_GENERAL', to: email,
    subject: `Mileage Claim Form for week ending ${ukDate(workflow.week_ending_date)}`,
    body_html: `<p>${safeAgency} has prepared the attached Mileage Claim Form for ${safeCandidate}.</p>`
      + `<p>Week ending: ${escapeCandidateMailHtml(ukDate(workflow.week_ending_date))}<br>`
      + `Total mileage: ${escapeCandidateMailHtml(mileageUnits)} miles</p>`
      + '<p>Complete the journey details and obtain the required manager signature before returning the form in MyTMS.</p>',
    body_text: bodyText,
    attachments: [{
      r2_key: artifact.storage_key,
      filename: artifact.filename,
      content_type: 'application/pdf',
      sha256: artifact.sha256,
      size_bytes: artifact.bytes.byteLength,
      page_count: 1
    }],
    status: 'QUEUED',
    reference: `candidate-mileage-form:${workflow.id}:${workflow.generation}`,
    recipient_kind: 'CANDIDATE',
    context_kind: 'CANDIDATE_WORKFLOW',
    context_id: workflow.id,
    email_type: 'CANDIDATE_APP_TRANSACTIONAL',
    scheduled_for_utc: new Date().toISOString(),
    next_attempt_at_utc: new Date().toISOString(),
    deterministic_outbox_key: deterministicKey,
    payment_scope_json: {
      candidate_mail_authority: 'CANDIDATE_MILEAGE_FORM_V1',
      candidate_workflow_id: workflow.id,
      candidate_workflow_generation: Number(workflow.generation),
      mileage_form_semantic_sha256: artifact.semantic_sha256
    }
  }, 'resolution=ignore-duplicates,return=representation');
  const durable = outbox || await restOne(env, 'mail_outbox',
    `deterministic_outbox_key=eq.${encodeURIComponent(deterministicKey)}&select=id,status`);
  if (!durable?.id || !['QUEUED', 'CLAIMED', 'SENT'].includes(upper(durable.status))) {
    throw new CandidateHttpError(503, 'CANDIDATE_MILEAGE_FORM_EMAIL_NOT_QUEUED');
  }
  return {
    ...common,
    idempotent_replay: artifact.idempotent_replay || !outbox,
    mail_outbox_id: durable.id
  };
}

async function handleWorkflowAction(request, env, deps, workflowId, action, ctx) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const mutationKey = requireCandidateIdempotency(body.idempotency_key);
  body.idempotency_key = mutationKey;
  const generation = requireInteger(body.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  let dbAction = upper(action.replace(/-/g, '_'));
  if (!CANDIDATE_WORKFLOW_ACTIONS.has(dbAction) && dbAction !== 'COMPONENT_SUPERSEDE') {
    throw new CandidateHttpError(400, 'CANDIDATE_WORKFLOW_ACTION_INVALID');
  }
  if (dbAction === 'MILEAGE_FORM_PREPARE' || dbAction === 'MILEAGE_FORM_EMAIL') {
    const workflow = await workflowRow(env, workflowId);
    return jsonResponse(dbAction === 'MILEAGE_FORM_EMAIL' ? 202 : 200,
      await handleCandidateMileageFormAction(env, workflow, access, body, dbAction));
  }
  if (dbAction === 'RETRY_FINALISATION') {
    const workflow = await workflowRow(env, workflowId);
    if (workflow.account_id !== access.account_id || workflow.candidate_id !== access.selected_candidate_id) {
      throw new CandidateHttpError(404, 'CANDIDATE_WORKFLOW_NOT_FOUND');
    }
    return jsonResponse(200, await finaliseWorkflow(
      env, deps, workflowId, generation, mutationKey
    ));
  }
  let payload = isObject(body.payload) ? structuredClone(body.payload) : {};
  let pendingManagerRoute = null;
  let replayResult = null;
  delete payload.mutation_replay_probe_only;
  delete payload.mutation_replay_semantic_payload;
  if (dbAction === 'WORKER_SUBMIT') {
    const candidateSignedAtUtc = text(body.candidate_signed_at_utc || payload.candidate_signed_at_utc).trim();
    if (!candidateSignedAtUtc || !Number.isFinite(Date.parse(candidateSignedAtUtc))) {
      throw new CandidateHttpError(400, 'CANDIDATE_SIGNATURE_TIMESTAMP_REQUIRED');
    }
    const requestedApprovalRoute = text(body.approval_route || payload.approval_route)
      ? upper(body.approval_route || payload.approval_route) : null;
    const signatureComponentId = body.candidate_signature_component_id
      || payload.candidate_signature_component_id || null;
    const submissionFacts = isObject(body.immutable_submission)
      ? structuredClone(body.immutable_submission) : {};
    delete submissionFacts.official_presentation;
    const submissionRequestIdentity = {
      contract_version: 'CANDIDATE_WORKER_SUBMISSION_REQUEST_V1',
      factual_submission: submissionFacts,
      candidate_signature_component_id: signatureComponentId,
      candidate_signed_at_utc: new Date(candidateSignedAtUtc).toISOString(),
      approval_route: requestedApprovalRoute
    };
    const replay = await probeWorkflowMutationReplay(
      env, deps, access, workflowId, dbAction, generation, mutationKey,
      { submission_request_identity: submissionRequestIdentity }
    );
    if (replay) {
      if (replay.render_contract) {
        const work = renderAndRegister(env, deps, replay.render_contract, 'REVIEW');
        const deferred = deferBackground(ctx, work, 'review-render-replay', {
          workflow_id: workflowId, generation
        });
        if (deferred !== true) await deferred;
        return jsonResponse(202, {
          ...withoutInternalRenderContracts(replay),
          review_rendering_accepted: true
        });
      }
      return jsonResponse(200, withoutInternalRenderContracts(replay));
    }
    const workflow = await workflowRow(env, workflowId);
    const breakContext = await rpcCall(deps, 'candidate_break_entry_context_get_v1',
      candidateRpcArgs(access, env, { p_workflow_id: workflowId }));
    const normalisedSubmissionFacts = normaliseCandidateBreakSubmission(
      submissionFacts, breakContext
    );
    const approvalRoute = requestedApprovalRoute || upper(workflow.route);
    payload = {
      ...payload,
      immutable_submission: await prepareImmutableSubmission(
        env, deps, workflow, { ...body, immutable_submission: normalisedSubmissionFacts }, mutationKey
      ),
      submission_request_identity: submissionRequestIdentity,
      break_entry_context: breakContext,
      candidate_signature_component_id: signatureComponentId,
      candidate_signed_at_utc: new Date(candidateSignedAtUtc).toISOString(),
      approval_route: approvalRoute,
      renderer_contract_version: RENDERER_CONTRACT_VERSION
    };
  } else if (dbAction === 'SELECT_PHONE_APPROVAL') {
    const brokerBinding = publicPhoneBinding(payload.public_broker_binding);
    const brokerHandoffKeyVersion = requireInteger(
      payload.broker_handoff_key_version,
      'CANDIDATE_BROKER_CREDENTIAL_VERSION_INVALID', 1
    );
    if (brokerHandoffKeyVersion > 65535) {
      throw new CandidateHttpError(400, 'CANDIDATE_BROKER_CREDENTIAL_VERSION_INVALID');
    }
    payload = {
      ...payload,
      public_broker_binding: brokerBinding,
      broker_handoff_key_version: brokerHandoffKeyVersion
    };
    const replaySemanticPayload = { ...payload };
    delete replaySemanticPayload.approval_token_hash_hex;
    delete replaySemanticPayload.expires_at_utc;
    delete replaySemanticPayload.handoff_token_key_version;
    delete replaySemanticPayload.broker_handoff_key_version;
    const replay = await probeWorkflowMutationReplay(
      env, deps, access, workflowId, dbAction, generation, mutationKey,
      replaySemanticPayload
    );
    if (replay) {
      return jsonResponse(201, await phoneTokenForWorkflowResult(
        env, replay, workflowId, generation, mutationKey
      ));
    }
    const handoffTokenKeyVersion = managerTokenKeyVersion(env);
    const managerToken = await deterministicOpaqueToken(
      managerTokenSecret(env, handoffTokenKeyVersion),
      'candidate-phone-handoff-v1', workflowId, generation, mutationKey
    );
    payload = {
      ...payload,
      approval_token_hash_hex: await sha256Hex(managerToken),
      expires_at_utc: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
      handoff_token_key_version: handoffTokenKeyVersion,
      public_broker_binding: brokerBinding,
      broker_handoff_key_version: brokerHandoffKeyVersion
    };
  } else if (dbAction === 'CREATE_EMAIL_APPROVAL_REQUEST' || dbAction === 'RENEW' || dbAction === 'REMIND') {
    const mailKind = dbAction === 'REMIND' ? 'REMINDER'
      : dbAction === 'RENEW' ? 'RENEWAL' : 'INITIAL';
    let managerEmail = body.manager_email || payload.manager_email;
    let approvalIdentity = 'INITIAL';
    const approvalRequestId = dbAction === 'RENEW' || dbAction === 'REMIND'
      ? requireUuid(body.approval_request_id || payload.approval_request_id,
        'CANDIDATE_REQUEST_GENERATION_STALE') : null;
    const approvalRequestGeneration = dbAction === 'RENEW' || dbAction === 'REMIND'
      ? requireInteger(body.approval_request_generation || payload.approval_request_generation,
        'CANDIDATE_REQUEST_GENERATION_STALE', 1) : null;
    const replaySemanticPayload = {
      ...payload,
      manager_email: managerEmail ? normaliseEmail(managerEmail) : null,
      ...(approvalRequestId ? {
        approval_request_id: approvalRequestId,
        approval_request_generation: approvalRequestGeneration
      } : {})
    };
    delete replaySemanticPayload.mail;
    delete replaySemanticPayload.approval_token_hash_hex;
    if (dbAction === 'REMIND' || dbAction === 'RENEW') {
      delete replaySemanticPayload.manager_email;
    }
    const replay = await probeWorkflowMutationReplay(
      env, deps, access, workflowId, dbAction, generation, mutationKey, replaySemanticPayload
    );
    replayResult = replay;
    if (dbAction === 'RENEW' || dbAction === 'REMIND') {
      const approval = await restOne(env, 'candidate_approval_requests',
        `id=eq.${encodeURIComponent(approvalRequestId)}`
        + `&workflow_id=eq.${encodeURIComponent(workflowId)}&method=eq.EMAIL`
        + `&request_generation=eq.${approvalRequestGeneration}`
        + '&select=id,request_generation,manager_email_normalized');
      if (!approval) throw new CandidateHttpError(409, 'CANDIDATE_REQUEST_GENERATION_STALE');
      if (managerEmail && normaliseEmail(managerEmail) !== approval.manager_email_normalized) {
        throw new CandidateHttpError(409, 'CANDIDATE_REQUEST_GENERATION_STALE');
      }
      managerEmail = approval.manager_email_normalized;
      approvalIdentity = `${approval.id}:${Number(approval.request_generation)}`;
      payload.approval_request_id = approval.id;
      payload.approval_request_generation = Number(approval.request_generation);
    }
    const normalisedManagerEmail = normaliseEmail(managerEmail);
    const managerToken = await deterministicOpaqueToken(
      env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET,
      'candidate-email-handoff-v1', workflowId, generation, dbAction, mutationKey,
      normalisedManagerEmail, approvalIdentity
    );
    if (!replay) {
      const managerWorkflow = await workflowRow(env, workflowId);
      payload = {
        ...payload,
        ...await candidateManagerMail(
          env, deps, managerToken, workflowId, normalisedManagerEmail, mailKind,
          managerWorkflow.workflow_kind
        ),
        approval_token_hash_hex: await sha256Hex(managerToken)
      };
    }
    pendingManagerRoute = { managerToken, mailKind };
  } else if (dbAction === 'CANCEL') {
    const reasonNote = text(
      body.reason_note || body.reason || payload.reason_note || payload.reason
    ).trim();
    if (!reasonNote) throw new CandidateHttpError(400, 'CANDIDATE_CANCELLATION_REASON_REQUIRED');
    if (reasonNote.length > 1000) throw new CandidateHttpError(400, 'CANDIDATE_CANCELLATION_REASON_INVALID');
    const managerWorkflow = await workflowRow(env, workflowId);
    const emailApprovals = await restRows(env, 'candidate_approval_requests',
      `workflow_id=eq.${encodeURIComponent(workflowId)}&method=eq.EMAIL`
      + '&state=in.(PENDING,APPROVED)&select=id&limit=1');
    payload = {
      ...payload,
      reason_note: reasonNote,
      reason_code: text(body.reason_code || payload.reason_code).trim().toUpperCase() || null,
      ...(emailApprovals.length ? {
        manager_terminal_mail: await candidateManagerTerminalMail(
          deps, 'CANCELLATION', managerWorkflow.workflow_kind
        )
      } : {})
    };
  }
  const managerRoutesToRetire = dbAction === 'CANCEL'
    ? await currentManagerEmailRouteTickets(env, workflowId) : [];
  const result = replayResult || await rpcCall(
    deps, 'candidate_workflow_transition_atomic_v1',
    workflowActionArgs(access, env, workflowId, dbAction, generation, payload, mutationKey)
  );
  if (pendingManagerRoute) {
    const approvalRequestId = requireUuid(result?.approval_request_id, 'MANAGER_ROUTE_REGISTRATION_FAILED');
    const mailOutboxId = requireUuid(result?.mail_outbox_id, 'MANAGER_ROUTE_REGISTRATION_FAILED');
    const approval = await restOne(env, 'candidate_approval_requests',
      `id=eq.${encodeURIComponent(approvalRequestId)}&workflow_id=eq.${encodeURIComponent(workflowId)}`
      + '&select=id,request_generation,resend_count,expires_at_utc,token_hash');
    if (!approval) throw new CandidateHttpError(503, 'MANAGER_ROUTE_REGISTRATION_FAILED');
    const credentialGeneration = pendingManagerRoute.mailKind === 'REMINDER'
      ? Number(approval.resend_count) + 1 : 1;
    await registerManagerEmailRoute(env, deps, {
      workflowId, approvalRequestId, requestGeneration: Number(approval.request_generation),
      credentialGeneration, mailOutboxId, managerToken: pendingManagerRoute.managerToken,
      mailKind: pendingManagerRoute.mailKind, expiresAtUtc: approval.expires_at_utc, mutationKey
    });
  }
  if (dbAction === 'CANCEL') retireManagerEmailRoutes(env, deps, managerRoutesToRetire, ctx);
  if (dbAction === 'SELECT_PHONE_APPROVAL') {
    return jsonResponse(201, await phoneTokenForWorkflowResult(
      env, result, workflowId, generation, mutationKey
    ));
  }
  if (dbAction === 'WORKER_SUBMIT' && result?.render_contract) {
    const work = renderAndRegister(env, deps, result.render_contract, 'REVIEW');
    const deferred = deferBackground(ctx, work, 'review-render', { workflow_id: workflowId, generation });
    if (deferred !== true) await deferred;
    return jsonResponse(202, { ...withoutInternalRenderContracts(result), review_rendering_accepted: true });
  }
  if (dbAction === 'PAPER_PREPARE' && result?.state === 'AWAITING_PAPER_RETURN') {
    const workflow = await workflowRow(env, workflowId);
    const timesheetId = workflow.target_timesheet_id || workflow.anchor_timesheet_id;
    if (!timesheetId) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_TIMESHEET_NOT_READY');
    const paperReturnPages = safePaperReturnPages(
      parseJson(workflow.paper_return_manifest_json, {})
    );
    if (!paperReturnPages.length
        || paperReturnPages.length !== Number(result?.paper_return_page_count)) {
      throw new CandidateHttpError(409, 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE');
    }
    const pack = result?.paper_pack;
    const outboxBinding = await bindCandidatePaperOutbox(env, workflow, timesheetId, pack);
    if (deps.nudgeQrPack) await deps.nudgeQrPack({ pack, timesheetId, ctx });
    const { paper_pack: _privatePaperPack, ...publicResult } = result;
    return jsonResponse(202, {
      ...publicResult,
      paper_pack_queued: pack?.queued === true,
      paper_pack: safeQrPackResponse(pack),
      paper_pack_email_bound: outboxBinding.bound,
      paper_return_pages: paperReturnPages
    });
  }
  if (dbAction === 'PAPER_RETURN' && result?.state === 'RECEIVED') {
    const completion = await finaliseReceivedPaperReturn(result, () => finaliseWorkflow(
        env,
        deps,
        workflowId,
        generation,
        `${mutationKey}:paper-finalise`
      ));
    return jsonResponse(completion.status, completion.body);
  }
  return jsonResponse(200, result);
}

async function managerTokenContext(request, env) {
  const token = bearerToken(request);
  if (!token) throw new CandidateHttpError(401, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
  return { token, token_hash_hex: await sha256Hex(token), environment: environmentName(env) };
}

function managerSubmissionType(workflowKind) {
  const kind = upper(workflowKind);
  if (kind === 'CONTRACT_EXPENSE') return 'EXPENSE_CLAIM';
  if (kind === 'CONTRACT_COMBINED') return 'COMBINED';
  return 'TIMESHEET';
}

function managerStartResult(result, routeAuthority) {
  const components = (Array.isArray(result?.ordered_components) ? result.ordered_components : [])
    .map((component) => ({
      component_id: requireUuid(component?.component_id, 'MANAGER_REVIEW_DOCUMENT_NOT_READY'),
      ordinal: requireInteger(component?.ordinal, 'MANAGER_REVIEW_DOCUMENT_NOT_READY', 1),
      component_kind: text(component?.component_kind),
      media_type: normaliseMediaType(component?.media_type),
      byte_size: requireInteger(component?.byte_size, 'MANAGER_REVIEW_DOCUMENT_NOT_READY', 1),
      content_sha256: requireSha256(component?.content_sha256, 'MANAGER_REVIEW_DOCUMENT_NOT_READY'),
      viewed: component?.viewed === true
    }));
  const reviewedCount = Number(result?.reviewed_count || 0);
  if (!components.length || !Number.isSafeInteger(reviewedCount) || reviewedCount < 0
      || reviewedCount > components.length) {
    throw new CandidateHttpError(409, 'MANAGER_REVIEW_DOCUMENT_NOT_READY');
  }
  return {
    ok: true,
    workflow_id: requireUuid(result?.workflow_id, 'MANAGER_REVIEW_DOCUMENT_NOT_READY'),
    workflow_generation: requireInteger(
      result?.workflow_generation, 'MANAGER_REVIEW_DOCUMENT_NOT_READY', 1
    ),
    approval_request_id: requireUuid(
      result?.approval_request_id, 'MANAGER_REVIEW_DOCUMENT_NOT_READY'
    ),
    authority_kind: routeAuthority?.authority_kind || (upper(result?.method) === 'PHONE'
      ? 'MANAGER_PHONE' : 'MANAGER_EMAIL'),
    submission_type: managerSubmissionType(result?.workflow_kind),
    expires_at_utc: new Date(result?.expires_at_utc).toISOString(),
    manifest_sha256: requireSha256(result?.manifest_sha256, 'MANAGER_REVIEW_DOCUMENT_NOT_READY'),
    page_count: components.length,
    ordered_components: components,
    reviewed_count: reviewedCount,
    all_pages_viewed: reviewedCount === components.length,
    can_approve: result?.can_approve === true,
    can_refuse: result?.can_refuse === true
  };
}

function managerProgressResult(result) {
  const requiredCount = requireInteger(result?.required_count, 'MANAGER_REVIEW_PROGRESS_CONFLICT', 1);
  const reviewedCount = requireInteger(result?.reviewed_count, 'MANAGER_REVIEW_PROGRESS_CONFLICT', 1);
  return {
    ok: true,
    workflow_id: requireUuid(result?.workflow_id, 'MANAGER_REVIEW_PROGRESS_CONFLICT'),
    generation: requireInteger(result?.generation, 'MANAGER_REVIEW_PROGRESS_CONFLICT', 1),
    approval_request_id: requireUuid(
      result?.approval_request_id, 'MANAGER_REVIEW_PROGRESS_CONFLICT'
    ),
    component_id: requireUuid(result?.component_id, 'MANAGER_REVIEW_PROGRESS_CONFLICT'),
    reviewed_count: reviewedCount,
    required_count: requiredCount,
    progress_version: requireInteger(
      result?.progress_version, 'MANAGER_REVIEW_PROGRESS_CONFLICT', 1
    ),
    all_pages_viewed: reviewedCount === requiredCount
  };
}

function managerTerminalResult(result, action) {
  const approved = action === 'approve';
  const completedAt = approved ? result?.approved_at_utc : result?.refused_at_utc;
  return {
    ok: true,
    workflow_id: requireUuid(result?.workflow_id, 'MANAGER_APPROVAL_REQUEST_NOT_READY'),
    generation: requireInteger(result?.generation, 'MANAGER_APPROVAL_REQUEST_NOT_READY', 1),
    state: approved ? 'APPROVED' : 'REFUSED',
    completed_at_utc: new Date(completedAt).toISOString(),
    finalisation_state: approved ? 'FINALISATION_PENDING' : 'NOT_APPLICABLE',
    idempotent_replay: result?.idempotent_replay === true
  };
}

async function handleManagerAction(request, env, deps, workflowId, action, ctx) {
  const auth = await managerTokenContext(request, env);
  const routeAuthority = request.headers.has('x-cloudtms-manager-route-authority')
    ? managerRouteAuthority(request) : null;
  if (routeAuthority) await assertManagerRouteWorkflow(env, workflowId, routeAuthority);
  const body = request.method === 'GET' ? {} : await readJson(request);
  const mutationKey = request.method === 'GET'
    ? `manager-start:${workflowId}:${auth.token_hash_hex}`
    : requireCandidateIdempotency(body.idempotency_key);
  const generation = body.generation == null ? null : requireInteger(body.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  let dbAction = {
    start: 'BEGIN_MANAGER_REVIEW', progress: 'RECORD_REVIEW_PROGRESS',
    approve: 'EMAIL_APPROVE', refuse: 'MANAGER_REFUSE'
  }[action];
  if (!dbAction) throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
  if (action === 'approve') {
    const context = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
      null, env, workflowId, 'BEGIN_MANAGER_REVIEW', generation,
      { approval_token_hash_hex: auth.token_hash_hex }, `${mutationKey}:begin-review`
    ));
    dbAction = upper(context?.method) === 'PHONE' ? 'PHONE_APPROVE' : 'EMAIL_APPROVE';
  }
  const payload = { ...(isObject(body.payload) ? body.payload : body), approval_token_hash_hex: auth.token_hash_hex };
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    null, env, workflowId, dbAction, generation, payload, mutationKey
  ));
  if (routeAuthority) await assertManagerRouteResult(env, result, routeAuthority);
  if (['EMAIL_APPROVE', 'PHONE_APPROVE', 'MANAGER_REFUSE'].includes(dbAction)) {
    completeManagerEmailRoute(env, deps, routeAuthority, ctx);
  }
  if (['EMAIL_APPROVE', 'PHONE_APPROVE'].includes(dbAction) && result?.final_render_contract) {
    const work = (async () => {
      await renderAndRegister(env, deps, result.final_render_contract, 'FINAL');
      return finaliseWorkflow(env, deps, workflowId, result.generation, `${mutationKey}:finalise`);
    })();
    const deferred = deferBackground(ctx, work, 'manager-final-render-and-finalise', {
      workflow_id: workflowId,
      generation: result.generation
    });
    if (deferred !== true) await deferred;
    return jsonResponse(202, managerTerminalResult(result, action));
  }
  if (action === 'start') return jsonResponse(200, managerStartResult(result, routeAuthority));
  if (action === 'progress') return jsonResponse(200, managerProgressResult(result));
  if (action === 'approve' || action === 'refuse') {
    return jsonResponse(200, managerTerminalResult(result, action));
  }
  return jsonResponse(200, result);
}

async function handleDocumentStream(request, env, deps, owner, workflowId, componentId) {
  let component;
  if (owner === 'candidate') {
    const access = await verifyCandidateAccess(request, env);
    const workflow = await workflowRow(env, workflowId);
    if (workflow.account_id !== access.account_id || workflow.candidate_id !== access.selected_candidate_id) {
      throw new CandidateHttpError(404, 'CANDIDATE_DOCUMENT_NOT_FOUND');
    }
    component = await restOne(env, 'candidate_submission_components',
      `id=eq.${encodeURIComponent(componentId)}&workflow_id=eq.${encodeURIComponent(workflowId)}&select=*`);
  } else if (owner === 'manager') {
    const auth = await managerTokenContext(request, env);
    const routeAuthority = request.headers.has('x-cloudtms-manager-route-authority')
      ? managerRouteAuthority(request) : null;
    if (routeAuthority) await assertManagerRouteWorkflow(env, workflowId, routeAuthority);
    const manifest = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
      null, env, workflowId, 'BEGIN_MANAGER_REVIEW', null,
      { approval_token_hash_hex: auth.token_hash_hex }, `manager-document:${workflowId}:${componentId}`
    ));
    if (routeAuthority) await assertManagerRouteResult(env, manifest, routeAuthority);
    const allowedIds = (Array.isArray(manifest?.ordered_components) ? manifest.ordered_components : [])
      .map((entry) => text(entry?.component_id || entry?.id));
    if (!allowedIds.includes(componentId)) {
      throw new CandidateHttpError(404, 'CANDIDATE_DOCUMENT_NOT_FOUND');
    }
    component = await restOne(env, 'candidate_submission_components',
      `id=eq.${encodeURIComponent(componentId)}&workflow_id=eq.${encodeURIComponent(workflowId)}&select=*`);
  } else if (owner === 'office') {
    await requireOfficeActor(request, deps, 'manage_paper');
    const generation = requireInteger(
      new URL(request.url).searchParams.get('generation'), 'WORKFLOW_GENERATION_CONFLICT', 1
    );
    const workflow = await workflowRow(env, workflowId);
    if (Number(workflow.generation) !== generation) {
      throw new CandidateHttpError(409, 'WORKFLOW_GENERATION_CONFLICT');
    }
    component = await restOne(env, 'candidate_submission_components',
      `id=eq.${encodeURIComponent(componentId)}&workflow_id=eq.${encodeURIComponent(workflowId)}`
      + `&workflow_generation=eq.${encodeURIComponent(generation)}&state=eq.IMMUTABLE&select=*`);
  } else {
    throw new CandidateHttpError(404, 'CANDIDATE_DOCUMENT_NOT_FOUND');
  }
  if (!component) throw new CandidateHttpError(404, 'CANDIDATE_DOCUMENT_NOT_FOUND');
  const key = owner === 'manager' ? component?.review_storage_key
    : component?.final_signed_storage_key || component?.review_storage_key || component?.storage_key;
  const hash = text(owner === 'manager' ? component?.review_content_sha256
    : component?.final_signed_content_sha256 || component?.review_content_sha256 || component?.source_content_sha256).replace(/^\\x/i, '');
  const stored = await r2Bytes(env, key, hash || null);
  return new Response(stored.bytes, {
    status: 200,
    headers: {
      'content-type': stored.media_type,
      'content-length': String(stored.bytes.byteLength),
      'cache-control': 'private, no-store',
      'content-disposition': 'inline; filename="timesheet-document"',
      'x-content-type-options': 'nosniff',
      ...(owner === 'manager' ? {
        'x-cloudtms-component-id': component.id,
        'x-cloudtms-content-sha256': hash
      } : {})
    }
  });
}

function ukDate(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(text(value));
  return match ? `${match[3]}/${match[2]}/${match[1]}` : '-';
}

async function appendPdfBytes(target, sourceBytes) {
  const source = await PDFDocument.load(sourceBytes);
  const pages = await target.copyPages(source, source.getPageIndices());
  for (const page of pages) target.addPage(page);
}

function mileageJourneyRows(workflow) {
  const { expenseSubmission } = expenseClaim(workflow);
  const source = [expenseSubmission.mileage_journeys, expenseSubmission.journeys, expenseSubmission.mileage_entries]
    .find(Array.isArray) || [];
  const rows = source.slice(0, 10).map((journey) => ({
    post_code_from: text(journey?.post_code_from || journey?.postcode_from || journey?.from_postcode),
    post_code_to: text(journey?.post_code_to || journey?.postcode_to || journey?.to_postcode),
    miles: text(journey?.number_of_miles ?? journey?.miles ?? journey?.mileage_units)
  }));
  while (rows.length < 10) rows.push({ post_code_from: '', post_code_to: '', miles: '' });
  return rows;
}

async function mileageClaimFormBytes(env, workflow, brandingOverride = null, presentationOverride = null) {
  const pdf = await PDFDocument.create({ updateMetadata: false });
  const page = pdf.addPage([595.28, 841.89]);
  const regular = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const branding = brandingOverride || await candidateDocumentBranding(env, workflow);
  page.drawRectangle({ x: 0, y: 785, width: 595.28, height: 56, color: rgb(0.04, 0.12, 0.24) });
  await drawCandidateBranding(pdf, page, branding, { x: 430, y: 800, maxWidth: 125, maxHeight: 30 });
  page.drawText(branding.agency_name.slice(0, 55), { x: 34, y: 817, size: 9, font: bold, color: rgb(0.75, 0.9, 1) });
  page.drawText(`Mileage Claim Form for week ending ${ukDate(workflow.week_ending_date)}`, {
    x: 34, y: 795, size: 16, font: bold, color: rgb(1, 1, 1)
  });
  const immutable = parseJson(workflow.immutable_submission_json, {}) || {};
  const presentation = presentationOverride || parseJson(immutable.official_presentation, {}) || {};
  const { expenseSubmission, claim } = expenseClaim(workflow);
  page.drawText(`Candidate: ${text(presentation.worker?.first_name)} ${text(presentation.worker?.surname)}`.trim(), {
    x: 42, y: 755, size: 10, font: regular, color: rgb(0.07, 0.14, 0.24)
  });
  page.drawText(`Client: ${text(presentation.client?.name) || '-'}`, {
    x: 315, y: 755, size: 10, font: regular, color: rgb(0.07, 0.14, 0.24)
  });
  const columns = [
    { label: 'Post Code from', x: 42, width: 180 },
    { label: 'Post Code To', x: 222, width: 180 },
    { label: 'Number of miles', x: 402, width: 151 }
  ];
  const headerY = 712;
  for (const column of columns) {
    page.drawRectangle({ x: column.x, y: headerY, width: column.width, height: 30, color: rgb(0.9, 0.93, 0.97), borderColor: rgb(0.18, 0.28, 0.4), borderWidth: 1 });
    page.drawText(column.label, { x: column.x + 8, y: headerY + 10, size: 10, font: bold, color: rgb(0.07, 0.14, 0.24) });
  }
  const journeys = mileageJourneyRows(workflow);
  journeys.forEach((journey, index) => {
    const y = headerY - ((index + 1) * 38);
    const values = [journey.post_code_from, journey.post_code_to, journey.miles];
    columns.forEach((column, columnIndex) => {
      page.drawRectangle({ x: column.x, y, width: column.width, height: 38, borderColor: rgb(0.35, 0.42, 0.5), borderWidth: 0.8 });
      if (values[columnIndex]) page.drawText(values[columnIndex].slice(0, 26), { x: column.x + 8, y: y + 13, size: 9, font: regular });
    });
  });
  const totalMileage = text(expenseSubmission.total_mileage ?? claim.mileage_units ?? expenseSubmission.mileage_units) || '0';
  page.drawText(`Total mileage: ${totalMileage}`, { x: 402, y: 278, size: 11, font: bold, color: rgb(0.07, 0.14, 0.24) });
  page.drawRectangle({ x: 42, y: 120, width: 511, height: 110, borderColor: rgb(0.18, 0.28, 0.4), borderWidth: 1 });
  page.drawText('Manager signature', { x: 56, y: 205, size: 10, font: bold });
  page.drawText('Date', { x: 370, y: 205, size: 10, font: bold });
  page.drawLine({ start: { x: 56, y: 150 }, end: { x: 330, y: 150 }, thickness: 0.8, color: rgb(0.35, 0.42, 0.5) });
  page.drawLine({ start: { x: 370, y: 150 }, end: { x: 530, y: 150 }, thickness: 0.8, color: rgb(0.35, 0.42, 0.5) });
  page.drawText('This page forms part of the immutable CloudTMS paper-return manifest.', {
    x: 42, y: 58, size: 8, font: regular, color: rgb(0.35, 0.4, 0.48)
  });
  page.drawText(`Workflow ${workflow.id} | Generation ${workflow.generation}`, {
    x: 42, y: 43, size: 7, font: regular, color: rgb(0.4, 0.45, 0.5)
  });
  return new Uint8Array(await pdf.save());
}

async function paperExpensePageBytes(env, workflow, component, ordinal) {
  if (upper(component.component_kind) === 'MILEAGE_FORM') return mileageClaimFormBytes(env, workflow);
  const rendered = await renderExpensePage(env, {
    review_ordinal: ordinal,
    render_input: upper(component.component_kind) === 'EXPENSE_SUMMARY'
      ? {} : { source_component_id: component.id }
  }, { workflow, component }, 'REVIEW');
  return rendered.pdf_bytes;
}

function paperPackIdentity(env, workflow, timesheet, version) {
  const manifestHash = text(workflow.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase();
  const baseHash = text(version?.sha256).replace(/^\\x/i, '').toLowerCase();
  const immutable = parseJson(workflow.immutable_submission_json, {}) || {};
  const brandingHash = text(immutable.official_presentation?.branding?.branding_contract_sha256).toLowerCase();
  const rendererVersion = text(workflow.renderer_contract_version || immutable.official_presentation?.renderer_contract_version);
  const manifest = parseJson(workflow.paper_return_manifest_json, {}) || {};
  const pageCount = Array.isArray(manifest.pages) ? manifest.pages.length : 0;
  if (!SHA256_RE.test(manifestHash) || !SHA256_RE.test(baseHash) || !SHA256_RE.test(brandingHash)
      || !Number.isSafeInteger(pageCount) || pageCount < 1
      || rendererVersion !== RENDERER_CONTRACT_VERSION) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
  }
  return {
    manifest_hash: manifestHash,
    base_hash: baseHash,
    branding_hash: brandingHash,
    renderer_contract_version: rendererVersion,
    page_count: pageCount,
    key: `candidate-app/${environmentName(env).toLowerCase()}/${workflow.id}/${workflow.generation}/paper-pack/${manifestHash}-${baseHash}-${brandingHash}-${rendererVersion}.pdf`
  };
}

async function activePaperWorkflowsForTimesheet(env, timesheetId) {
  return restRows(env, 'candidate_submission_workflows',
    `environment=eq.${encodeURIComponent(environmentName(env))}&route=eq.PAPER`
    + `&state=eq.AWAITING_PAPER_RETURN&or=(target_timesheet_id.eq.${encodeURIComponent(timesheetId)},anchor_timesheet_id.eq.${encodeURIComponent(timesheetId)})`
    + '&select=*&order=updated_at_utc.asc,id.asc&limit=2');
}

async function readyPaperPackReceipt(env, workflow, timesheet, version) {
  const identity = paperPackIdentity(env, workflow, timesheet, version);
  let object;
  try {
    object = await env.R2?.head(identity.key);
  } catch (error) {
    if (error instanceof CandidateHttpError) throw error;
    throw new CandidateHttpError(503, 'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT');
  }
  if (!object) return { ...identity, ready: false };
  const metadata = object.customMetadata || {};
  const metadataByteSize = Number(metadata.byte_size);
  const metadataPageCount = Number(metadata.page_count);
  const valid = metadata.purpose === 'candidate-complete-paper-pack'
    && metadata.workflow_id === workflow.id
    && Number(metadata.workflow_generation) === Number(workflow.generation)
    && metadata.timesheet_id === timesheet.timesheet_id
    && text(metadata.manifest_sha256).toLowerCase() === identity.manifest_hash
    && text(metadata.base_document_sha256).toLowerCase() === identity.base_hash
    && text(metadata.branding_contract_sha256).toLowerCase() === identity.branding_hash
    && text(metadata.renderer_contract_version) === identity.renderer_contract_version
    && text(metadata.media_type).toLowerCase() === 'application/pdf'
    && SHA256_RE.test(text(metadata.sha256))
    && Number.isSafeInteger(metadataByteSize) && metadataByteSize > 0
    && metadataByteSize === Number(object.size)
    && Number.isSafeInteger(metadataPageCount) && metadataPageCount >= 1
    && metadataPageCount === identity.page_count;
  if (!valid) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT');
  return {
    ...identity,
    ready: true,
    sha256: text(metadata.sha256).toLowerCase(),
    page_count: metadataPageCount,
    byte_size: metadataByteSize
  };
}

function candidateCompletePackAttachmentMatchesScope(row) {
  const scope = parseJson(row?.payment_scope_json, {}) || {};
  const attachments = parseJson(row?.attachments, []) || [];
  if (scope.candidate_paper_pack_ready !== true
      || scope.mail_held_until_pdf_rendered !== false
      || text(scope.mail_hold_reason)) return false;
  if (attachments.length !== 1) return false;
  const attachment = attachments[0] || {};
  return attachment.r2_key === scope.candidate_complete_pack_storage_key
    && text(attachment.sha256).toLowerCase() === text(scope.candidate_complete_pack_sha256).toLowerCase()
    && Number(attachment.size_bytes) === Number(scope.candidate_complete_pack_size_bytes)
    && Number(attachment.page_count) === Number(scope.candidate_complete_pack_page_count)
    && text(attachment.content_type).toLowerCase() === 'application/pdf'
    && Number.isSafeInteger(Number(attachment.size_bytes)) && Number(attachment.size_bytes) > 0
    && Number.isSafeInteger(Number(attachment.page_count)) && Number(attachment.page_count) > 0
    && SHA256_RE.test(text(attachment.sha256))
    && UUID_RE.test(text(attachment.candidate_workflow_id))
    && attachment.candidate_workflow_id === scope.candidate_workflow_id
    && Number(attachment.candidate_workflow_generation) === Number(scope.candidate_workflow_generation)
    && text(attachment.paper_return_manifest_sha256).toLowerCase()
      === text(scope.paper_return_manifest_sha256).toLowerCase();
}

function candidatePaperExecutionState(workflow, outbox = null, timesheet = null, complete = null) {
  const workflowReceipt = parseJson(workflow?.last_mutation_response_json, {}) || {};
  if (workflowReceipt.failure_scope === 'WORKFLOW'
      && workflowReceipt.paper_pack_state === 'FAILED_TERMINAL') {
    return {
      state: 'FAILED_TERMINAL', failure_scope: 'WORKFLOW', retryable: false,
      failure_code: text(workflowReceipt.failure_code)
        || 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED',
      attempt_count: 0, next_retry_at_utc: null, retry_in_progress: false,
      operation_id: workflowReceipt.paper_pack_operation_id || null
    };
  }
  const scope = parseJson(outbox?.payment_scope_json, {}) || {};
  const attemptExpiresAt = Date.parse(text(scope.candidate_paper_pack_attempt_expires_at_utc));
  const common = {
    failure_scope: outbox ? 'OUTBOX' : null,
    failure_code: text(scope.candidate_paper_pack_failure_code) || null,
    attempt_count: Number(scope.candidate_paper_pack_attempt_count || 0),
    next_retry_at_utc: scope.candidate_paper_pack_next_retry_at_utc || null,
    retry_in_progress: !!text(scope.candidate_paper_pack_attempt_token)
      && Number.isFinite(attemptExpiresAt) && attemptExpiresAt > Date.now(),
    operation_id: scope.candidate_paper_pack_operation_id || null
  };
  if (scope.candidate_paper_generation_retired === true) {
    return { ...common, state: 'RETIRED', retryable: false };
  }
  if (complete?.ready === true || candidateCompletePackAttachmentMatchesScope(outbox)) {
    return { ...common, state: 'READY', retryable: false, failure_code: null };
  }
  if (scope.candidate_paper_pack_retryable === true
      || upper(scope.candidate_paper_pack_failure_class) === 'RETRYABLE') {
    const due = Date.parse(text(scope.candidate_paper_pack_next_retry_at_utc));
    return {
      ...common,
      state: Number.isFinite(due) && due > Date.now() ? 'BACKOFF' : 'FAILED_RETRYABLE',
      retryable: true
    };
  }
  if (upper(scope.candidate_paper_pack_failure_class) === 'TERMINAL'
      || upper(outbox?.status) === 'FAILED' || upper(timesheet?.document_state) === 'FAILED') {
    return {
      ...common, state: 'FAILED_TERMINAL', retryable: false,
      failure_code: common.failure_code || 'CANDIDATE_PAPER_DOCUMENT_FAILED'
    };
  }
  return { ...common, state: outbox ? 'PREPARING' : 'STALE', retryable: false };
}

function candidatePaperDeliveryGeneration(workflow) {
  const generation = requireInteger(workflow?.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  return upper(workflow?.state) === 'FINALISED' ? Math.max(generation - 1, 1) : generation;
}

function candidatePaperReceiptRow(row) {
  const scope = parseJson(row?.payment_scope_json, {}) || {};
  if (scope.candidate_paper_generation_retired !== true) return row;
  const retired = parseJson(scope.candidate_retired_delivery_receipt, {}) || {};
  return {
    ...row,
    attachments: Array.isArray(retired.attachments) ? retired.attachments : [],
    payment_scope_json: {
      ...scope,
      candidate_paper_pack_ready: true,
      mail_held_until_pdf_rendered: false,
      mail_hold_reason: null,
      candidate_complete_pack_storage_key: retired.candidate_complete_pack_storage_key
        || scope.candidate_complete_pack_storage_key,
      candidate_complete_pack_sha256: retired.candidate_complete_pack_sha256
        || scope.candidate_complete_pack_sha256,
      candidate_complete_pack_size_bytes: retired.candidate_complete_pack_size_bytes
        || scope.candidate_complete_pack_size_bytes,
      candidate_complete_pack_page_count: retired.candidate_complete_pack_page_count
        || scope.candidate_complete_pack_page_count,
      candidate_complete_pack_media_type: retired.candidate_complete_pack_media_type
        || scope.candidate_complete_pack_media_type
    }
  };
}

function candidatePaperCompleteReceipt(env, workflow, deliveryGeneration, row) {
  const receiptRow = candidatePaperReceiptRow(row);
  const scope = parseJson(receiptRow?.payment_scope_json, {}) || {};
  if (!candidateCompletePackAttachmentMatchesScope(receiptRow)) return null;
  const manifestHash = text(scope.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase();
  const baseHash = text(scope.base_document_sha256).replace(/^\\x/i, '').toLowerCase();
  const brandingHash = text(scope.branding_contract_sha256).replace(/^\\x/i, '').toLowerCase();
  const rendererVersion = text(scope.renderer_contract_version);
  const key = text(scope.candidate_complete_pack_storage_key).replace(/^\/+/, '');
  const sha256 = text(scope.candidate_complete_pack_sha256).toLowerCase();
  const byteSize = Number(scope.candidate_complete_pack_size_bytes);
  const pageCount = Number(scope.candidate_complete_pack_page_count);
  const expectedKey = `candidate-app/${environmentName(env).toLowerCase()}/${workflow.id}/${deliveryGeneration}`
    + `/paper-pack/${manifestHash}-${baseHash}-${brandingHash}-${rendererVersion}.pdf`;
  if (!SHA256_RE.test(manifestHash) || !SHA256_RE.test(baseHash) || !SHA256_RE.test(brandingHash)
      || !SHA256_RE.test(sha256) || rendererVersion !== RENDERER_CONTRACT_VERSION
      || key !== expectedKey || !Number.isSafeInteger(byteSize) || byteSize < 1
      || !Number.isSafeInteger(pageCount) || pageCount < 1) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT');
  }
  return {
    key,
    sha256,
    byte_size: byteSize,
    page_count: pageCount,
    manifest_hash: manifestHash,
    base_hash: baseHash,
    branding_hash: brandingHash,
    renderer_contract_version: rendererVersion,
    delivery_generation: deliveryGeneration,
    retired: scope.candidate_paper_generation_retired === true,
    ready: true
  };
}

async function officeCandidatePaperDelivery(env, workflow) {
  const deliveryGeneration = candidatePaperDeliveryGeneration(workflow);
  const manifestHash = text(workflow?.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase();
  if (!SHA256_RE.test(manifestHash)) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
  }
  const sourceIds = [...new Set([
    text(workflow?.target_timesheet_id), text(workflow?.anchor_timesheet_id)
  ].filter(value => UUID_RE.test(value)))];
  if (!sourceIds.length) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_TIMESHEET_NOT_READY');
  const rows = await restRows(env, 'mail_outbox',
    `type=eq.TIMESHEET_QR&context_kind=eq.timesheets&context_id=in.(${sourceIds.map(encodeURIComponent).join(',')})`
    + '&select=id,context_id,status,payment_scope_json,attachments,attempt_lease_token,'
    + 'attempt_lease_expires_at_utc,created_at_utc&order=created_at_utc.desc&limit=50');
  const exact = rows.filter((row) => {
    const scope = parseJson(row.payment_scope_json, {}) || {};
    return upper(scope.candidate_mail_authority) === 'CANDIDATE_PAPER_V1'
      && scope.candidate_workflow_id === workflow.id
      && Number(scope.candidate_workflow_generation) === deliveryGeneration
      && text(scope.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase() === manifestHash;
  });
  if (exact.length > 1) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_CONFLICT');
  if (exact.length !== 1) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  const outbox = exact[0];
  if (!UUID_RE.test(text(outbox.context_id))) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_TIMESHEET_NOT_READY');
  }
  return {
    delivery_generation: deliveryGeneration,
    source_timesheet_id: outbox.context_id,
    outbox,
    complete: candidatePaperCompleteReceipt(env, workflow, deliveryGeneration, outbox)
  };
}

function completePaperAttachmentMatches(row, complete) {
  const scope = parseJson(row?.payment_scope_json, {}) || {};
  return candidateCompletePackAttachmentMatchesScope(row)
    && scope.candidate_complete_pack_storage_key === complete.key
    && text(scope.candidate_complete_pack_sha256).toLowerCase() === complete.sha256
    && Number(scope.candidate_complete_pack_size_bytes) === complete.byte_size
    && Number(scope.candidate_complete_pack_page_count) === complete.page_count;
}

async function requireCandidatePaperOutbox(env, workflow, timesheet) {
  const manifestHash = text(workflow.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase();
  const outboxRows = await restRows(env, 'mail_outbox',
    `type=eq.TIMESHEET_QR&context_kind=eq.timesheets&context_id=eq.${encodeURIComponent(timesheet.timesheet_id)}`
    + '&select=id,status,payment_scope_json,attachments,attempt_lease_token,attempt_lease_expires_at_utc'
    + '&order=created_at_utc.desc&limit=25');
  const exact = outboxRows.filter((row) => {
    const scope = parseJson(row.payment_scope_json, {}) || {};
    return upper(scope.candidate_mail_authority) === 'CANDIDATE_PAPER_V1'
      && scope.candidate_workflow_id === workflow.id
      && Number(scope.candidate_workflow_generation) === Number(workflow.generation)
      && text(scope.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase() === manifestHash;
  });
  if (exact.length > 1) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_CONFLICT');
  if (exact.length !== 1) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  const row = exact[0];
  if (row.status === 'FAILED') throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_FAILED');
  if (text(row.attempt_lease_token)) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  if (['QUEUED', 'SENT'].includes(row.status) && candidateCompletePackAttachmentMatchesScope(row)) return row;
  const scope = parseJson(row.payment_scope_json, {}) || {};
  const held = row.status === 'QUEUED'
    && scope.candidate_paper_pack_ready === false
    && scope.mail_held_until_pdf_rendered === true
    && scope.mail_hold_reason === 'CANDIDATE_PAPER_PACK_PENDING'
    && (parseJson(row.attachments, []) || []).length === 0;
  if (!held) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  return row;
}

async function claimCandidatePaperPackAttempt(
  env, deps, workflow, outbox, idempotencyKey, officeActorId = null,
  operationId = idempotencyKey
) {
  const manifestHash = text(workflow.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase();
  const attemptToken = await sha256Hex(
    `candidate-paper-pack-attempt-v1:${workflow.id}:${workflow.generation}:${idempotencyKey}`
  );
  const payload = {
    service_paper_pack_attempt: true,
    mail_outbox_id: outbox.id,
    paper_return_manifest_sha256: manifestHash,
    paper_pack_attempt_token: attemptToken,
    paper_pack_operation_id: operationId
  };
  const result = officeActorId
    ? await officeAdapter(deps, env, officeActorId, 'WORKFLOW_ACTION_EXECUTE', {
      workflow_id: workflow.id,
      generation: Number(workflow.generation),
      workflow_action: 'PAPER_PACK_ATTEMPT_CLAIM',
      idempotency_key: idempotencyKey,
      payload
    })
    : await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', {
      p_session_id: null,
      p_environment: environmentName(env),
      p_workflow_id: workflow.id,
      p_action: 'PAPER_PACK_ATTEMPT_CLAIM',
      p_expected_generation: Number(workflow.generation),
      p_payload: payload,
      p_idempotency_key: idempotencyKey,
      p_now_utc: new Date().toISOString()
    });
  return {
    ...result,
    attempt_token: attemptToken,
    claim_acquired_new: result?.paper_pack_attempt_state === 'CLAIMED'
      && result?.idempotent_replay !== true && result?.claim_acquired_new === true
  };
}

async function releaseCandidatePaperPack(
  env, deps, workflow, timesheet, complete, outbox = null, officeActorId = null,
  paperPackAttemptToken = null, paperPackOperationId = null
) {
  const row = outbox || await requireCandidatePaperOutbox(env, workflow, timesheet);
  if (!UUID_RE.test(text(row?.id))) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  }
  for (const digest of [
    complete?.manifest_hash, complete?.sha256, complete?.base_hash, complete?.branding_hash
  ]) {
    if (!SHA256_RE.test(text(digest).toLowerCase())) {
      throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
    }
  }
  if (!text(complete?.key)
      || text(complete?.renderer_contract_version) !== RENDERER_CONTRACT_VERSION
      || !Number.isSafeInteger(Number(complete?.byte_size)) || Number(complete.byte_size) < 1
      || !Number.isSafeInteger(Number(complete?.page_count)) || Number(complete.page_count) < 1) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
  }
  const releasePayload = {
      service_paper_pack_release: true,
      mail_outbox_id: row.id,
      paper_return_manifest_sha256: text(complete.manifest_hash).toLowerCase(),
      complete_pack_storage_key: complete.key,
      complete_pack_sha256: text(complete.sha256).toLowerCase(),
      complete_pack_byte_size: Number(complete.byte_size),
      complete_pack_page_count: Number(complete.page_count),
      complete_pack_media_type: 'application/pdf',
      base_document_sha256: text(complete.base_hash).toLowerCase(),
      branding_contract_sha256: text(complete.branding_hash).toLowerCase(),
      renderer_contract_version: complete.renderer_contract_version,
      paper_pack_attempt_token: paperPackAttemptToken,
      paper_pack_operation_id: paperPackOperationId
    };
  const releaseKey = `paper-pack-release:${workflow.id}:${workflow.generation}:${complete.manifest_hash}:${complete.sha256}`;
  if (officeActorId) {
    return officeAdapter(deps, env, officeActorId, 'WORKFLOW_ACTION_EXECUTE', {
      workflow_id: workflow.id,
      generation: Number(workflow.generation),
      workflow_action: 'PAPER_PACK_RELEASE',
      idempotency_key: releaseKey,
      payload: releasePayload
    });
  }
  return rpcCall(deps, 'candidate_workflow_transition_atomic_v1', {
    p_session_id: null,
    p_environment: environmentName(env),
    p_workflow_id: workflow.id,
    p_action: 'PAPER_PACK_RELEASE',
    p_expected_generation: Number(workflow.generation),
    p_payload: releasePayload,
    p_idempotency_key: releaseKey,
    p_now_utc: new Date().toISOString()
  });
}

async function assembleCandidatePaperPack(env, workflow, timesheet, version) {
  const manifest = parseJson(workflow.paper_return_manifest_json, {}) || {};
  const pages = Array.isArray(manifest.pages) ? manifest.pages : [];
  if (!pages.length || !workflow.paper_return_manifest_sha256) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE');
  }
  let base;
  try {
    base = await r2Bytes(env, version.r2_key, text(version.sha256) || null);
  } catch (error) {
    if (error instanceof CandidateHttpError) throw error;
    throw new CandidateHttpError(503, 'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT');
  }
  if (base.media_type !== 'application/pdf') throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID');
  let components;
  try {
    components = await restRows(env, 'candidate_submission_components',
      `workflow_id=eq.${encodeURIComponent(workflow.id)}&workflow_generation=eq.${encodeURIComponent(workflow.generation)}`
      + '&state=eq.IMMUTABLE&select=*');
  } catch (error) {
    if (error instanceof CandidateHttpError) throw error;
    throw new CandidateHttpError(503, 'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT');
  }
  const byId = new Map(components.map((component) => [text(component.id), component]));
  const combined = await PDFDocument.create({ updateMetadata: false });
  for (let index = 0; index < pages.length; index += 1) {
    const expected = pages[index];
    const kind = upper(expected.component_kind);
    if (kind === 'HOURS_TIMESHEET') {
      await appendPdfBytes(combined, base.bytes);
      continue;
    }
    let component = null;
    if (kind === 'EXPENSE_SUMMARY') {
      component = components.find((entry) => upper(entry.component_kind) === 'EXPENSE_SUMMARY') || {
        id: workflow.id, component_kind: 'EXPENSE_SUMMARY', document_role: 'EXPENSE_APPROVAL_SUMMARY',
        expense_category: null, review_ordinal: index + 1
      };
    } else {
      component = byId.get(text(expected.source_component_id));
    }
    if (!component || upper(component.component_kind) !== kind) {
      throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_COMPONENT_MISSING');
    }
    await appendPdfBytes(combined, await paperExpensePageBytes(env, workflow, component, index + 1));
  }
  if (combined.getPageCount() !== pages.length) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_INCOMPLETE');
  const bytes = new Uint8Array(await combined.save());
  const identity = paperPackIdentity(env, workflow, timesheet, version);
  let receipt;
  try {
    receipt = await immutablePut(env, identity.key, bytes, 'application/pdf', {
      purpose: 'candidate-complete-paper-pack', workflow_id: workflow.id,
      workflow_generation: String(workflow.generation),
      timesheet_id: timesheet.timesheet_id, manifest_sha256: identity.manifest_hash,
      base_document_sha256: identity.base_hash,
      branding_contract_sha256: identity.branding_hash,
      renderer_contract_version: identity.renderer_contract_version,
      page_count: String(combined.getPageCount())
    });
  } catch (error) {
    if (error instanceof CandidateHttpError) throw error;
    throw new CandidateHttpError(503, 'CANDIDATE_PAPER_R2_WRITE_TRANSIENT');
  }
  const complete = {
    ...identity, bytes, sha256: receipt.sha256, byte_size: bytes.byteLength,
    page_count: combined.getPageCount(), ready: true
  };
  return complete;
}

async function candidatePaperPackContext(request, env, deps, timesheetId) {
  const access = await verifyCandidateAccess(request, env);
  const id = requireUuid(timesheetId, 'CANDIDATE_TIMESHEET_NOT_FOUND');

  // Reuse the Candidate-safe detail authority as the ownership/capability gate.
  // The subsequent service reads resolve only the already-authorised current row;
  // neither a raw R2 identity nor a database document identity is exposed.
  await rpcCall(deps, 'candidate_app_timesheet_detail_v1', candidateRpcArgs(access, env, {
    p_timesheet_id: id,
    p_contract_week_id: null,
    p_workflow_id: null
  }));

  const timesheet = await restOne(env, 'timesheets',
    `timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true`
    + '&select=timesheet_id,version,sheet_scope,submission_mode,qr_status,qr_token,'
    + 'document_state,current_document_version_id,manual_pdf_r2_key');
  const qrRoute = text(timesheet?.qr_token)
    && ['PENDING', 'SENT', 'READY'].includes(upper(timesheet?.qr_status));
  if (!timesheet || upper(timesheet.sheet_scope) !== 'WEEKLY' || !qrRoute) {
    throw new CandidateHttpError(404, 'CANDIDATE_PAPER_PACK_NOT_FOUND');
  }
  const workflows = await activePaperWorkflowsForTimesheet(env, id);
  if (workflows.length > 1) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_WORKFLOW_CONFLICT');
  if (!workflows.length) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_WORKFLOW_NOT_READY');

  let version = null;
  if (upper(timesheet.document_state) === 'READY' && UUID_RE.test(text(timesheet.current_document_version_id))) {
    version = await restOne(env, 'invoice_document_versions',
      `id=eq.${encodeURIComponent(timesheet.current_document_version_id)}`
      + `&entity_type=eq.TIMESHEET&entity_id=eq.${encodeURIComponent(id)}`
      + '&purpose=eq.TIMESHEET&status=eq.READY&select=id,r2_key,sha256,status');
  }
  let outbox = null;
  let outboxError = null;
  try {
    outbox = await requireCandidatePaperOutbox(env, workflows[0], timesheet);
  } catch (error) {
    const code = knownErrorCode(error);
    if (!['CANDIDATE_PAPER_OUTBOX_FAILED', 'CANDIDATE_PAPER_OUTBOX_CONFLICT',
      'CANDIDATE_PAPER_OUTBOX_NOT_READY'].includes(code)) throw error;
    outboxError = code;
  }
  let complete = null;
  if (outbox && candidateCompletePackAttachmentMatchesScope(outbox) && version?.r2_key) {
    complete = await readyPaperPackReceipt(env, workflows[0], timesheet, version);
  }
  const execution = candidatePaperExecutionState(workflows[0], outbox, timesheet, complete);
  if (!outbox && outboxError === 'CANDIDATE_PAPER_OUTBOX_CONFLICT') execution.state = 'STALE';
  if (!outbox && outboxError === 'CANDIDATE_PAPER_OUTBOX_FAILED') execution.state = 'FAILED_TERMINAL';
  const ready = execution.state === 'READY' && complete?.ready === true;
  return {
    id, timesheet, version, key: complete?.key || null, ready,
    state: execution.state, execution, complete, outbox
  };
}

async function handlePaperPackStatus(request, env, deps, timesheetId) {
  const context = await candidatePaperPackContext(request, env, deps, timesheetId);
  return jsonResponse(200, {
    ok: true,
    timesheet_id: context.id,
    timesheet_version: Number(context.timesheet.version || 1),
    paper_pack_state: context.state,
    failure_scope: context.execution.failure_scope,
    failure_code: context.execution.failure_code,
    retryable: context.execution.retryable,
    attempt_count: context.execution.attempt_count,
    next_retry_at_utc: context.execution.next_retry_at_utc,
    retry_in_progress: context.execution.retry_in_progress,
    download_available: context.ready,
    page_count: context.complete?.page_count || null
  });
}

async function handlePaperPackDownload(request, env, deps, timesheetId) {
  const context = await candidatePaperPackContext(request, env, deps, timesheetId);
  if (!context.ready) {
    throw new CandidateHttpError(409, context.state.startsWith('FAILED')
      ? 'CANDIDATE_PAPER_PACK_FAILED' : 'CANDIDATE_PAPER_PACK_PREPARING');
  }

  const stored = await r2Bytes(env, context.key, context.complete?.sha256 || null);
  if (stored.media_type !== 'application/pdf') {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID');
  }
  return new Response(stored.bytes, {
    status: 200,
    headers: {
      'content-type': stored.media_type,
      'content-length': String(stored.bytes.byteLength),
      'cache-control': 'private, no-store',
      'content-disposition': `attachment; filename="Timesheet_${context.id}_v${Number(context.timesheet.version || 1)}.pdf"`,
      'x-content-type-options': 'nosniff'
    }
  });
}

const TERMINAL_PAPER_PACK_FAILURES = new Set([
  'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
  'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID',
  'CANDIDATE_PAPER_PACK_COMPONENT_MISSING',
  'CANDIDATE_PAPER_PACK_INCOMPLETE',
  'CANDIDATE_PAPER_PACK_IDENTITY_INVALID',
  'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT',
  'CANDIDATE_PAPER_DOCUMENT_FAILED',
  'CANDIDATE_PAPER_OUTBOX_NOT_READY',
  'CANDIDATE_PAPER_OUTBOX_CONFLICT',
  'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
]);

const RETRYABLE_PAPER_PACK_FAILURES = new Set([
  'CANDIDATE_PAPER_PACK_ASSEMBLY_TRANSIENT',
  'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
  'CANDIDATE_PAPER_R2_WRITE_TRANSIENT'
]);

function canonicalPaperPackFailureCode(error) {
  const code = knownErrorCode(error);
  if (RETRYABLE_PAPER_PACK_FAILURES.has(code) || TERMINAL_PAPER_PACK_FAILURES.has(code)) return code;
  return 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED';
}

async function recordCandidatePaperPackFailure(
  env, deps, workflow, outbox, failureCode, attemptToken = null, officeActorId = null,
  idempotencyKey = null, operationId = null
) {
  const manifestHash = text(workflow.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase();
  const failureKey = idempotencyKey || `paper-pack-failure:${workflow.id}:${workflow.generation}`
    + `:${outbox?.id || 'pre-outbox'}:${attemptToken || 'no-attempt'}:${failureCode}`;
  const payload = {
    service_paper_pack_failure: true,
    mail_outbox_id: outbox?.id || null,
    paper_return_manifest_sha256: manifestHash,
    paper_pack_attempt_token: attemptToken,
    paper_pack_operation_id: operationId,
    error_code: failureCode
  };
  if (officeActorId) {
    return officeAdapter(deps, env, officeActorId, 'WORKFLOW_ACTION_EXECUTE', {
      workflow_id: workflow.id,
      generation: Number(workflow.generation),
      workflow_action: 'PAPER_PACK_MARK_FAILURE',
      idempotency_key: failureKey,
      payload
    });
  }
  return rpcCall(deps, 'candidate_workflow_transition_atomic_v1', {
    p_session_id: null,
    p_environment: environmentName(env),
    p_workflow_id: workflow.id,
    p_action: 'PAPER_PACK_MARK_FAILURE',
    p_expected_generation: Number(workflow.generation),
    p_payload: payload,
    p_idempotency_key: failureKey,
    p_now_utc: new Date().toISOString()
  });
}

export async function processPendingCandidatePaperPacks(env, deps, limit = 10) {
  const workflows = await restRows(env, 'candidate_submission_workflows',
    `environment=eq.${encodeURIComponent(environmentName(env))}&route=eq.PAPER&state=eq.AWAITING_PAPER_RETURN`
    + `&select=*&order=updated_at_utc.asc&limit=${Math.min(25, Math.max(1, Number(limit) || 10))}`);
  const results = [];
  for (const workflow of workflows) {
    const id = text(workflow.target_timesheet_id || workflow.anchor_timesheet_id);
    if (!UUID_RE.test(id)) continue;
    let outbox = null;
    let attemptToken = null;
    let operationId = null;
    try {
      const workflowFailure = parseJson(workflow.last_mutation_response_json, {}) || {};
      if (workflowFailure.failure_scope === 'WORKFLOW'
          && workflowFailure.paper_pack_state === 'FAILED_TERMINAL') {
        results.push({
          workflow_id: workflow.id, timesheet_id: id, ok: false,
          execution_state: 'FAILED_TERMINAL', failure_scope: 'WORKFLOW',
          error_code: text(workflowFailure.failure_code)
            || 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
        });
        continue;
      }
      const matchingWorkflows = await activePaperWorkflowsForTimesheet(env, id);
      if (matchingWorkflows.length > 1) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_WORKFLOW_CONFLICT');
      if (matchingWorkflows.length !== 1 || matchingWorkflows[0].id !== workflow.id) continue;
      const timesheet = await restOne(env, 'timesheets',
        `timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true`
        + '&select=timesheet_id,version,sheet_scope,submission_mode,qr_status,qr_token,'
        + 'document_state,current_document_version_id,manual_pdf_r2_key');
      if (!timesheet) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_TIMESHEET_NOT_READY');
      outbox = await requireCandidatePaperOutbox(env, workflow, timesheet);
      const scope = parseJson(outbox.payment_scope_json, {}) || {};
      if (upper(scope.candidate_paper_pack_failure_class) === 'TERMINAL') {
        results.push({
          workflow_id: workflow.id, timesheet_id: id, ok: false, execution_state: 'FAILED_TERMINAL',
          error_code: text(scope.candidate_paper_pack_failure_code) || 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
        });
        continue;
      }
      const nextRetryAt = Date.parse(text(scope.candidate_paper_pack_next_retry_at_utc));
      if (scope.candidate_paper_pack_retryable === true && Number.isFinite(nextRetryAt)
          && nextRetryAt > Date.now()) {
        results.push({
          workflow_id: workflow.id, timesheet_id: id, ok: false, execution_state: 'BACKOFF',
          error_code: 'CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE',
          next_retry_at_utc: new Date(nextRetryAt).toISOString()
        });
        continue;
      }
      if (upper(timesheet.document_state) !== 'READY'
          || !UUID_RE.test(text(timesheet.current_document_version_id))) {
        if (upper(timesheet.document_state) === 'FAILED') {
          throw new CandidateHttpError(409, 'CANDIDATE_PAPER_DOCUMENT_FAILED');
        }
        results.push({
          workflow_id: workflow.id, timesheet_id: id, ok: false,
          execution_state: 'PREPARING', error_code: 'CANDIDATE_PAPER_DOCUMENT_PENDING'
        });
        continue;
      }
      const version = await restOne(env, 'invoice_document_versions',
        `id=eq.${encodeURIComponent(timesheet.current_document_version_id)}`
        + `&entity_type=eq.TIMESHEET&entity_id=eq.${encodeURIComponent(id)}`
        + '&purpose=eq.TIMESHEET&status=eq.READY&select=id,r2_key,sha256,status');
      if (!version?.r2_key) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_DOCUMENT_FAILED');
      operationId = `paper-pack-scheduler:${workflow.id}:${workflow.generation}:${crypto.randomUUID()}`;
      const claim = await claimCandidatePaperPackAttempt(
        env, deps, workflow, outbox, operationId
      );
      if (claim.paper_pack_attempt_state === 'READY') {
        results.push({ workflow_id: workflow.id, timesheet_id: id, ok: true, already_ready: true });
        continue;
      }
      if (claim.claim_acquired_new !== true) {
        results.push({
          workflow_id: workflow.id, timesheet_id: id, ok: false,
          execution_state: 'IN_PROGRESS', error_code: 'CANDIDATE_PAPER_PACK_ATTEMPT_IN_PROGRESS'
        });
        continue;
      }
      attemptToken = claim.attempt_token;
      let complete = await readyPaperPackReceipt(env, workflow, timesheet, version);
      if (!complete.ready) complete = await assembleCandidatePaperPack(env, workflow, timesheet, version);
      await releaseCandidatePaperPack(
        env, deps, workflow, timesheet, complete, outbox, null, attemptToken, operationId
      );
      results.push({ workflow_id: workflow.id, timesheet_id: id, ok: true, page_count: complete.page_count });
    } catch (error) {
      const observedErrorCode = knownErrorCode(error);
      const failureCode = canonicalPaperPackFailureCode(error);
      let failureReceipt = null;
      let failureReceiptError = null;
      if (!['CANDIDATE_PAPER_PACK_ATTEMPT_IN_PROGRESS',
        'CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE',
        'CANDIDATE_PAPER_PACK_FAILED_TERMINAL'].includes(observedErrorCode)
          && SHA256_RE.test(text(workflow.paper_return_manifest_sha256).replace(/^\\x/i, '').toLowerCase())) {
        try {
          failureReceipt = await recordCandidatePaperPackFailure(
            env, deps, workflow, outbox, failureCode, attemptToken,
            null, null, operationId
          );
        } catch (receiptError) {
          failureReceiptError = knownErrorCode(receiptError);
        }
      }
      results.push({
        workflow_id: workflow.id,
        timesheet_id: id,
        ok: false,
        error_code: observedErrorCode,
        failure_recorded: failureReceipt?.ok === true,
        failure_state: failureReceipt?.paper_pack_state || null,
        failure_receipt_error: failureReceiptError
      });
    }
  }
  return { ok: true, inspected: workflows.length, results };
}

async function handleCandidateNoWork(request, env, deps, contractWeekId) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  return jsonResponse(200, await rpcCall(deps, 'candidate_no_work_atomic_v1', candidateRpcArgs(access, env, {
    p_contract_week_id: requireUuid(contractWeekId),
    p_expected_row_signature: text(body.expected_row_signature),
    p_idempotency_key: requireCandidateIdempotency(body.idempotency_key)
  })));
}

const CANDIDATE_NOTIFICATION_COPY = Object.freeze({
  MANAGER_APPROVED: 'Your submission has been approved by the manager.',
  MANAGER_REFUSED: 'Your submission was refused by the manager. Open it to review what to do next.',
  AUTHORISED: 'Your timesheet has been authorised.',
  SUBMISSION_RECEIVED: 'Your submission has been received.',
  OFFICE_REJECTED: 'Your submission needs changes. Open it to review what to do next.',
  PAPER_PACK_READY: 'Your printed signing documents are ready.',
  RESUBMISSION_REQUIRED: 'A timesheet needs to be submitted again.'
});

function optionalUuid(value) {
  const candidate = text(value);
  return UUID_RE.test(candidate) ? candidate : null;
}

function safeCandidateNotification(row) {
  const source = isObject(row) ? row : {};
  const parameters = isObject(source.template_params) ? source.template_params : {};
  const storedLink = isObject(source.deep_link_json) ? source.deep_link_json : {};
  const eventType = upper(source.event_type);
  const storedType = text(storedLink.type).toLowerCase();
  const workflowId = optionalUuid(source.workflow_id) || optionalUuid(storedLink.workflow_id)
    || optionalUuid(parameters.workflow_id);
  const timesheetId = optionalUuid(source.timesheet_id) || optionalUuid(storedLink.timesheet_id)
    || optionalUuid(parameters.timesheet_id);
  const contractWeekId = optionalUuid(storedLink.contract_week_id)
    || optionalUuid(parameters.contract_week_id);
  let destination = 'HOME';
  if (storedType === 'daily') destination = 'DAILY';
  else if (storedType === 'account') destination = 'ACCOUNT';
  else if (storedType === 'workflow' && workflowId) destination = 'WORKFLOW_DETAIL';
  else if ((storedType === 'timesheet' || storedType === 'paper_pack') && timesheetId) {
    destination = 'TIMESHEET_DETAIL';
  } else if (workflowId) destination = 'WORKFLOW_DETAIL';
  else if (timesheetId || contractWeekId) destination = 'TIMESHEET_DETAIL';

  const payload = {
    state: eventType || 'UPDATE',
    candidate_status_code: eventType || 'UPDATE',
    message: CANDIDATE_NOTIFICATION_COPY[eventType] || 'There is a new update in MyTMS.',
    occurred_at_utc: source.created_at_utc
  };
  if (workflowId) payload.workflow_id = workflowId;
  if (timesheetId) payload.timesheet_id = timesheetId;
  if (contractWeekId) payload.contract_week_id = contractWeekId;

  const deepLink = { destination };
  if (workflowId) deepLink.workflow_id = workflowId;
  if (timesheetId) deepLink.timesheet_id = timesheetId;
  if (contractWeekId) deepLink.contract_week_id = contractWeekId;
  return {
    id: source.id,
    event_type: eventType || 'UPDATE',
    template_key: text(source.template_key) || 'candidate-update-v1',
    payload_json: payload,
    deep_link_json: deepLink,
    state: upper(source.state) || 'UNREAD',
    created_at_utc: source.created_at_utc,
    read_at_utc: source.read_at_utc || null
  };
}

async function handleNotifications(request, env, deps) {
  const access = await verifyCandidateAccess(request, env);
  const candidateId = requireUuid(access.selected_candidate_id, 'CANDIDATE_SELECTION_REQUIRED');
  const url = new URL(request.url);
  const limit = Math.min(100, Math.max(1, Number(url.searchParams.get('limit') || 50)));
  const cursor = text(url.searchParams.get('cursor'));
  let cursorFilter = '';
  if (cursor) {
    const [createdAt, id] = cursor.split('|');
    if (!createdAt || !UUID_RE.test(text(id)) || Number.isNaN(Date.parse(createdAt))) {
      throw new CandidateHttpError(400, 'CANDIDATE_NOTIFICATION_CURSOR_INVALID');
    }
    cursorFilter = `&or=(created_at_utc.lt.${encodeURIComponent(createdAt)},and(created_at_utc.eq.${encodeURIComponent(createdAt)},id.lt.${encodeURIComponent(id)}))`;
  }
  const rows = await restRows(env, 'candidate_notifications',
    `account_id=eq.${encodeURIComponent(access.account_id)}`
    + `&candidate_id=eq.${encodeURIComponent(candidateId)}${cursorFilter}`
    + `&select=id,workflow_id,timesheet_id,event_type,template_key,template_params,deep_link_json,state,created_at_utc,read_at_utc&order=created_at_utc.desc,id.desc&limit=${limit + 1}`);
  const hasMore = rows.length > limit;
  const page = rows.slice(0, limit).map(safeCandidateNotification);
  const tail = page[page.length - 1];
  return jsonResponse(200, {
    ok: true, notifications: page,
    next_cursor: hasMore && tail ? `${tail.created_at_utc}|${tail.id}` : null
  });
}

async function requireOfficeActor(request, deps, permission = 'view_candidate_state') {
  if (!OFFICE_ADMIN_PERMISSIONS.has(permission)) {
    throw new CandidateHttpError(403, 'CANDIDATE_OFFICE_PERMISSION_DENIED');
  }
  const user = await deps.requireOfficeUser(request, ['admin']);
  if (!user) throw new CandidateHttpError(401, 'OFFICE_AUTH_REQUIRED');
  return {
    ...user,
    id: requireUuid(user.id, 'OFFICE_AUTH_REQUIRED'),
    office_permission: permission,
    office_permission_source: 'OFFICE_ADMIN_ROLE_V1'
  };
}

async function officeAdapter(deps, env, actorId, action, payload = {}, observedAtUtc = null) {
  return rpcCall(deps, 'cloudtms_office_candidate_adapter_v1', {
    p_action: upper(action),
    p_actor_user_id: requireUuid(actorId, 'OFFICE_AUTH_REQUIRED'),
    p_environment: environmentName(env),
    p_payload: isObject(payload) ? payload : {},
    p_now_utc: observedAtUtc || new Date().toISOString()
  });
}

function requireOfficeIdempotency(value) {
  return requireUuid(value, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
}

function officeProjectionIdentity(value) {
  const source = isObject(value) ? value : {};
  const timesheetId = text(source.timesheet_id);
  const contractWeekId = text(source.contract_week_id);
  if (timesheetId && !UUID_RE.test(timesheetId)) {
    throw new CandidateHttpError(400, 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID');
  }
  if (contractWeekId && !UUID_RE.test(contractWeekId)) {
    throw new CandidateHttpError(400, 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID');
  }
  if (!timesheetId && !contractWeekId) {
    throw new CandidateHttpError(400, 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID');
  }
  return {
    row_key: text(source.row_key) || timesheetId || contractWeekId,
    timesheet_id: timesheetId || null,
    contract_week_id: contractWeekId || null,
    expected_row_signature: text(source.expected_row_signature) || null
  };
}

async function handleOfficeCapabilities(request, env, deps) {
  const user = await requireOfficeActor(request, deps);
  return jsonResponse(200, await officeAdapter(deps, env, user.id, 'CAPABILITIES'));
}

async function handleOfficeDetail(request, env, deps, timesheetId) {
  const user = await requireOfficeActor(request, deps);
  const url = new URL(request.url);
  return jsonResponse(200, await officeAdapter(deps, env, user.id, 'PROJECT_ONE', {
    timesheet_id: requireUuid(timesheetId, 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND'),
    contract_week_id: text(url.searchParams.get('contract_week_id')) || null,
    row_key: text(url.searchParams.get('row_key')) || null,
    expected_row_signature: text(url.searchParams.get('expected_row_signature')) || null
  }));
}

async function handleOfficeProjectionBatch(request, env, deps) {
  const user = await requireOfficeActor(request, deps);
  const body = await readJson(request);
  const surface = upper(body.surface);
  if (!OFFICE_PROJECTION_SURFACES.has(surface)) {
    throw new CandidateHttpError(400, 'CANDIDATE_OFFICE_PROJECTION_SURFACE_INVALID');
  }
  const supplied = Array.isArray(body.identities) ? body.identities
    : Array.isArray(body.selected_rows) ? body.selected_rows : [];
  if (supplied.length < 1 || supplied.length > 100) {
    throw new CandidateHttpError(400, 'CANDIDATE_OFFICE_PROJECTION_BATCH_INVALID');
  }
  return jsonResponse(200, await officeAdapter(deps, env, user.id, 'PROJECT_BATCH', {
    surface,
    identities: supplied.map(officeProjectionIdentity)
  }));
}

async function handleOfficeRejectPreview(request, env, deps, timesheetId) {
  const user = await requireOfficeActor(request, deps, 'reject_submission');
  return jsonResponse(200, await officeAdapter(deps, env, user.id, 'REJECT_PREVIEW', {
    timesheet_id: requireUuid(timesheetId, 'TIMESHEET_NOT_FOUND')
  }));
}

async function handleOfficeRoute(request, env, deps, action, timesheetId) {
  const user = await requireOfficeActor(request, deps, 'change_route');
  if (action === 'preview') {
    const url = new URL(request.url);
    const routePreview = await rpcCall(deps, 'timesheet_route_version_preview_v1', {
      p_current_timesheet_id: requireUuid(timesheetId), p_target_action: upper(url.searchParams.get('action'))
    });
    let rejectPreview = null;
    try {
      rejectPreview = await officeAdapter(deps, env, user.id, 'REJECT_PREVIEW', {
        timesheet_id: requireUuid(timesheetId)
      });
    } catch (error) {
      const code = knownErrorCode(error);
      if (!['CANDIDATE_ACTION_NOT_ELIGIBLE', 'CANDIDATE_REJECTION_SCOPE_CONFLICT'].includes(code)) throw error;
      rejectPreview = { permitted: false, disabled_reason_code: code, target_workflows: [] };
    }
    const hasCandidateScope = Array.isArray(rejectPreview?.target_workflows)
      && rejectPreview.target_workflows.length > 0;
    const interventionChoice = hasCandidateScope && rejectPreview?.permitted === true ? {
      required: true,
      decision_code: 'REJECT_OR_MANUAL',
      title: 'Does the candidate need to resubmit instead?',
      message: 'Use Reject Candidate Submission where the candidate can correct and resubmit the timesheet themselves.\n\n'
        + 'Convert to Manual only when CloudTMS staff need to enter or process the replacement timesheet on the candidate\'s behalf.',
      reject_available: rejectPreview?.permitted === true,
      reject_disabled_reason_code: rejectPreview?.disabled_reason_code || null,
      reject_disabled_reason: rejectPreview?.disabled_reason || null,
      reject_action: {
        code: 'REJECT_CANDIDATE_SUBMISSION',
        label: 'Use Reject Candidate Submission',
        method: 'GET',
        path: `/api/candidate-app/timesheets/${timesheetId}/reject-preview`
      },
      continue_action: {
        code: 'CONTINUE_ROUTE_CHANGE',
        label: 'Continue to Manual conversion',
        method: 'POST',
        path: `/api/candidate-app/timesheets/${timesheetId}/route-confirm`,
        fixed_body: { action: upper(url.searchParams.get('action')) }
      }
    } : null;
    return jsonResponse(200, {
      ...routePreview,
      office_contract_version: OFFICE_CONTRACT_VERSION,
      intervention_choice: interventionChoice
    });
  }
  const body = await readJson(request);
  if (!ROUTE_INTERVENTION_REASONS.has(upper(body.reason_code)) && body.reason_code != null) {
    throw new CandidateHttpError(400, 'ROUTE_CHANGE_REASON_INVALID');
  }
  return jsonResponse(200, await officeAdapter(deps, env, user.id, 'ROUTE_CONFIRM', {
    current_timesheet_id: requireUuid(timesheetId),
    expected_timesheet_id: requireUuid(body.expected_timesheet_id),
    expected_row_signature: text(body.expected_row_signature),
    expected_context_sha256: text(body.expected_context_sha256),
    target_action: upper(body.action),
    reason_code: body.reason_code == null ? null : upper(body.reason_code),
    reason_note: body.reason_note == null ? null : text(body.reason_note),
    idempotency_key: requireOfficeIdempotency(body.idempotency_key),
    allow_manual_only: body.allow_manual_only === true
  }));
}

async function exactOfficeApproval(env, workflowId, workflowGeneration, requestId, requestGeneration) {
  const approval = await restOne(env, 'candidate_approval_requests',
    `id=eq.${encodeURIComponent(requireUuid(requestId, 'CANDIDATE_REQUEST_GENERATION_STALE'))}`
    + `&workflow_id=eq.${encodeURIComponent(requireUuid(workflowId, 'CANDIDATE_WORKFLOW_NOT_FOUND'))}`
    + `&workflow_generation=eq.${encodeURIComponent(requireInteger(workflowGeneration, 'WORKFLOW_GENERATION_CONFLICT', 1))}`
    + `&request_generation=eq.${encodeURIComponent(requireInteger(requestGeneration, 'CANDIDATE_REQUEST_GENERATION_STALE', 1))}`
    + '&select=*');
  if (!approval) throw new CandidateHttpError(409, 'CANDIDATE_REQUEST_GENERATION_STALE');
  return approval;
}

async function officeManagerMutationPayload(request, env, deps, action, body, workflow, approval, idempotencyKey) {
  if (action === 'REMIND' || action === 'RENEW') {
    if (upper(approval.method) !== 'EMAIL') {
      throw new CandidateHttpError(409, 'MANAGER_APPROVAL_METHOD_MISMATCH');
    }
    const managerToken = await deterministicOpaqueToken(
      env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET,
      'cloudtms-office-manager-action-v1', action, workflow.id, workflow.generation,
      approval.id, approval.request_generation, idempotencyKey
    );
    const result = {
      ...await candidateManagerMail(env, deps, managerToken, workflow.id,
        approval.manager_email_normalized, action === 'REMIND' ? 'REMINDER' : 'RENEWAL',
        workflow.workflow_kind),
      approval_token_hash_hex: await sha256Hex(managerToken)
    };
    Object.defineProperty(result, '__managerRouteToken', { value: managerToken, enumerable: false });
    return result;
  }
  if (action === 'MANAGER_REQUEST_CANCEL') {
    const reason = text(body.reason || body.reason_note);
    if (!reason) throw new CandidateHttpError(400, 'CANDIDATE_CANCELLATION_REASON_REQUIRED');
    if (reason.length > 1000) throw new CandidateHttpError(400, 'CANDIDATE_CANCELLATION_REASON_INVALID');
    return {
      reason_note: reason, reason_code: upper(body.reason_code) || null,
      manager_terminal_mail: await candidateManagerTerminalMail(
        deps, 'WITHDRAWAL', workflow.workflow_kind
      )
    };
  }
  if (action === 'BEGIN_MANAGER_REVIEW' || action === 'CANCEL_MANAGER_HANDOFF') return {};
  if (action === 'RECORD_REVIEW_PROGRESS') {
    const manifestHash = text(body.manifest_sha256_hex).toLowerCase();
    const componentHash = text(body.component_sha256_hex).toLowerCase();
    if (!SHA256_RE.test(manifestHash) || !SHA256_RE.test(componentHash)) {
      throw new CandidateHttpError(400, 'MANAGER_REVIEW_MANIFEST_MISMATCH');
    }
    return {
      manifest_sha256_hex: manifestHash,
      component_id: requireUuid(body.component_id, 'MANAGER_REVIEW_COMPONENT_INVALID'),
      component_sha256_hex: componentHash,
      viewed_receipt: isObject(body.viewed_receipt) ? structuredClone(body.viewed_receipt) : {}
    };
  }
  if (action === 'PHONE_APPROVE') {
    const manifestHash = text(body.manifest_sha256_hex).toLowerCase();
    const managerName = text(body.manager_name);
    const managerPosition = text(body.manager_position);
    if (!SHA256_RE.test(manifestHash)) {
      throw new CandidateHttpError(400, 'MANAGER_REVIEW_MANIFEST_MISMATCH');
    }
    if (!managerName || !managerPosition || managerName.length > 200 || managerPosition.length > 200) {
      throw new CandidateHttpError(400, 'MANAGER_SIGNATURE_REQUIRED');
    }
    return {
      manifest_sha256_hex: manifestHash,
      manager_name: managerName,
      manager_position: managerPosition,
      signature_component_id: requireUuid(body.signature_component_id, 'MANAGER_SIGNATURE_REQUIRED')
    };
  }
  if (action === 'MANAGER_REFUSE') {
    const reason = text(body.reason);
    if (!reason) throw new CandidateHttpError(400, 'MANAGER_REFUSAL_REASON_REQUIRED');
    if (reason.length > 1000) throw new CandidateHttpError(400, 'MANAGER_REFUSAL_REASON_INVALID');
    return { reason };
  }
  throw new CandidateHttpError(400, 'CANDIDATE_WORKFLOW_ACTION_INVALID');
}

async function handleOfficeWorkflowAction(request, env, deps, workflowId, action, ctx) {
  const user = await requireOfficeActor(
    request,deps,OFFICE_WORKFLOW_ACTION_PERMISSIONS[action] || 'view_candidate_state'
  );
  const workflow = await workflowRow(env, workflowId);
  const body = await readJson(request);
  const generation = requireInteger(body.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  const idempotencyKey = requireOfficeIdempotency(body.idempotency_key);
  if (action === 'retry-finalisation') {
    const replayServiceFinalisation = {
      contract_version: 'CANDIDATE_MANAGER_FINALISATION_V1',
      workflow_generation: generation,
      actor_user_id: user.id
    };
    const replay = await probeFinalisationReplay(
      env, deps, workflow, generation, idempotencyKey, replayServiceFinalisation, user.id
    );
    if (replay) return jsonResponse(200, replay);
    if (generation !== Number(workflow.generation)) {
      throw new CandidateHttpError(409, 'WORKFLOW_GENERATION_CONFLICT');
    }
    const contract = workflow.last_mutation_response_json?.final_render_contract;
    if (contract) await renderAndRegister(env, deps, contract, 'FINAL', user.id);
    return jsonResponse(200, await finaliseWorkflow(
      env, deps, workflow.id, generation, idempotencyKey, user.id
    ));
  }
  if (generation !== Number(workflow.generation)) {
    throw new CandidateHttpError(409, 'WORKFLOW_GENERATION_CONFLICT');
  }
  const dbAction = OFFICE_MANAGER_ACTIONS[action];
  if (!dbAction) throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
  const approvalId = requireUuid(body.approval_request_id, 'CANDIDATE_REQUEST_GENERATION_STALE');
  const approvalGeneration = requireInteger(
    body.approval_request_generation, 'CANDIDATE_REQUEST_GENERATION_STALE', 1
  );
  const approval = await exactOfficeApproval(
    env, workflow.id, generation, approvalId, approvalGeneration
  );
  const payload = await officeManagerMutationPayload(
    request, env, deps, dbAction, body, workflow, approval, idempotencyKey
  );
  const managerRoutesToRetire = dbAction === 'MANAGER_REQUEST_CANCEL'
    ? await currentManagerEmailRouteTickets(env, workflow.id) : [];
  const result = await officeAdapter(deps, env, user.id, 'WORKFLOW_ACTION_EXECUTE', {
    workflow_id: workflow.id,
    generation,
    workflow_action: dbAction,
    approval_request_id: approval.id,
    approval_request_generation: Number(approval.request_generation),
    idempotency_key: idempotencyKey,
    payload
  });
  if (payload.__managerRouteToken) {
    const currentApprovalId = requireUuid(result?.approval_request_id, 'MANAGER_ROUTE_REGISTRATION_FAILED');
    const current = await restOne(env, 'candidate_approval_requests',
      `id=eq.${encodeURIComponent(currentApprovalId)}&workflow_id=eq.${encodeURIComponent(workflow.id)}`
      + '&select=id,request_generation,resend_count,expires_at_utc');
    if (!current) throw new CandidateHttpError(503, 'MANAGER_ROUTE_REGISTRATION_FAILED');
    await registerManagerEmailRoute(env, deps, {
      workflowId: workflow.id, approvalRequestId: current.id,
      requestGeneration: Number(current.request_generation),
      credentialGeneration: dbAction === 'REMIND' ? Number(current.resend_count) + 1 : 1,
      mailOutboxId: requireUuid(result?.mail_outbox_id, 'MANAGER_ROUTE_REGISTRATION_FAILED'),
      managerToken: payload.__managerRouteToken,
      mailKind: dbAction === 'REMIND' ? 'REMINDER' : 'RENEWAL',
      expiresAtUtc: current.expires_at_utc, mutationKey: idempotencyKey
    });
  }
  if (dbAction === 'MANAGER_REQUEST_CANCEL') {
    retireManagerEmailRoutes(env, deps, managerRoutesToRetire, ctx);
  }
  if (dbAction === 'PHONE_APPROVE' && result?.final_render_contract) {
    const work = (async () => {
      await renderAndRegister(env, deps, result.final_render_contract, 'FINAL', user.id);
      return finaliseWorkflow(env, deps, workflow.id, result.generation,
        await deterministicOpaqueToken(env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET,
          'cloudtms-office-phone-finalise-v1', idempotencyKey), user.id);
    })();
    const deferred = deferBackground(ctx, work, 'office-final-render-and-finalise', {
      workflow_id: workflow.id,
      generation: result.generation
    });
    if (deferred !== true) await deferred;
    return jsonResponse(202, {
      ...withoutInternalRenderContracts(result),
      final_rendering_accepted: true,
      finalisation_pending: true
    });
  }
  return jsonResponse(200, result);
}

async function handleOfficeReject(request, env, deps, timesheetId) {
  const user = await requireOfficeActor(request, deps, 'reject_submission');
  const body = await readJson(request);
  const reason = text(body.reason);
  if (!reason) throw new CandidateHttpError(400, 'CANDIDATE_REASON_REQUIRED');
  if (reason.length > 1000) throw new CandidateHttpError(400, 'CANDIDATE_REASON_INVALID');
  return jsonResponse(200, await officeAdapter(deps, env, user.id, 'REJECT_CONFIRM', {
    timesheet_id: requireUuid(timesheetId),
    expected_timesheet_id: requireUuid(body.expected_timesheet_id || timesheetId),
    expected_row_signature: text(body.expected_row_signature),
    context_sha256: text(body.context_sha256),
    reason,
    idempotency_key: requireOfficeIdempotency(body.idempotency_key)
  }));
}

function officeReminderChunks(values, size = 100) {
  const output = [];
  for (let index = 0; index < values.length; index += size) output.push(values.slice(index, index + size));
  return output;
}

function officeReminderCandidateName(candidate) {
  const display = text(candidate?.display_name);
  if (display) return display;
  const joined = [text(candidate?.first_name), text(candidate?.last_name)].filter(Boolean).join(' ');
  return joined || 'Candidate';
}

function officeReminderCandidateSurname(candidate) {
  const surname = text(candidate?.last_name);
  if (surname) return surname;
  const display = officeReminderCandidateName(candidate);
  const parts = display.split(/\s+/).filter(Boolean);
  return parts.at(-1) || display;
}

async function loadOfficeManagerReminderCatalogue(env, deps, actorId) {
  const observedAtUtc = new Date().toISOString();
  const approvals = await restRowsPaged(env, 'candidate_approval_requests', [
    'method=eq.EMAIL',
    'state=eq.PENDING',
    'select=id,workflow_id,workflow_generation,request_generation',
    'order=workflow_id.asc,request_generation.desc,id.desc'
  ].join('&'), {
    pageSize: 1000,
    // The product limit applies to rows that are actually reminder-eligible,
    // not merely to pending requests. Keep the source scan independently
    // bounded so a damaged or unexpectedly large queue fails closed before
    // the Worker loads workflow/candidate joins or starts projections.
    maxRows: MAX_OFFICE_REMINDER_SOURCE_ROWS
  });
  if (!approvals.length) {
    const catalogueRevision = await sha256Hex('OFFICE_CANDIDATE_REMINDER_ELIGIBILITY_PAGE_V1:[]');
    return { observed_at_utc: observedAtUtc, catalogue_revision: catalogueRevision, items: [] };
  }

  const workflowIds = [...new Set(approvals.map(row => text(row.workflow_id)).filter(value => UUID_RE.test(value)))];
  const workflows = [];
  for (const ids of officeReminderChunks(workflowIds)) {
    workflows.push(...await restRows(env, 'candidate_submission_workflows', [
      `id=in.(${ids.map(encodeURIComponent).join(',')})`,
      `environment=eq.${encodeURIComponent(environmentName(env))}`,
      'select=id,candidate_id,generation,contract_week_id,anchor_timesheet_id,target_timesheet_id,updated_at_utc'
    ].join('&')));
  }
  const workflowById = new Map(workflows.map(row => [text(row.id), row]));
  const approvalByWorkflow = new Map();
  for (const approval of approvals) {
    const workflow = workflowById.get(text(approval.workflow_id));
    if (!workflow || Number(approval.workflow_generation) !== Number(workflow.generation)) continue;
    const existing = approvalByWorkflow.get(text(workflow.id));
    if (!existing || Number(approval.request_generation) > Number(existing.request_generation)) {
      approvalByWorkflow.set(text(workflow.id), approval);
    }
  }

  const candidateIds = [...new Set(workflows.map(row => text(row.candidate_id)).filter(value => UUID_RE.test(value)))];
  const candidates = [];
  for (const ids of officeReminderChunks(candidateIds)) {
    candidates.push(...await restRows(env, 'candidates', [
      `id=in.(${ids.map(encodeURIComponent).join(',')})`,
      'select=id,display_name,first_name,last_name'
    ].join('&')));
  }
  const candidateById = new Map(candidates.map(row => [text(row.id), row]));

  const currentByRowKey = new Map();
  for (const workflow of workflows) {
    const approval = approvalByWorkflow.get(text(workflow.id));
    if (!approval) continue;
    const timesheetId = text(workflow.target_timesheet_id || workflow.anchor_timesheet_id);
    const contractWeekId = text(workflow.contract_week_id);
    if (!UUID_RE.test(timesheetId) && !UUID_RE.test(contractWeekId)) continue;
    const rowKey = UUID_RE.test(timesheetId) ? timesheetId : contractWeekId;
    const existing = currentByRowKey.get(rowKey);
    if (existing && String(existing.workflow.updated_at_utc || '') >= String(workflow.updated_at_utc || '')) continue;
    currentByRowKey.set(rowKey, {
      workflow,
      approval,
      identity: {
        row_key: rowKey,
        timesheet_id: UUID_RE.test(timesheetId) ? timesheetId : null,
        contract_week_id: UUID_RE.test(contractWeekId) ? contractWeekId : null,
        expected_row_signature: null
      }
    });
  }

  const candidatesForProjection = [...currentByRowKey.values()];
  const projectionsByRowKey = new Map();
  for (const group of officeReminderChunks(candidatesForProjection, 100)) {
    const response = await officeAdapter(deps, env, actorId, 'PROJECT_BATCH', {
      surface: 'TIMESHEET_SUMMARY',
      identities: group.map(item => item.identity)
    }, observedAtUtc);
    for (const row of Array.isArray(response?.results) ? response.results : []) {
      if (row?.ok !== true || !row.projection) {
        throw new CandidateHttpError(503, 'CANDIDATE_REMINDER_CATALOGUE_UNAVAILABLE');
      }
      projectionsByRowKey.set(text(row.correlation_key), row.projection);
    }
  }

  const items = [];
  for (const candidateRow of candidatesForProjection) {
    const projection = projectionsByRowKey.get(candidateRow.identity.row_key);
    if (!projection) throw new CandidateHttpError(503, 'CANDIDATE_REMINDER_CATALOGUE_UNAVAILABLE');
    const action = (Array.isArray(projection.available_actions) ? projection.available_actions : [])
      .find(item => upper(item?.code) === 'SEND_MANAGER_REMINDER' && item?.enabled === true);
    const manager = projection.manager_approval;
    if (!action || text(action?.invocation?.path) !== `/api/candidate-app/workflows/${candidateRow.workflow.id}/actions/remind`
        || text(manager?.request_id) !== text(candidateRow.approval.id)
        || Number(manager?.request_generation) !== Number(candidateRow.approval.request_generation)
        || !manager?.provider_accepted_at_utc) continue;
    const currentIdentity = officeProjectionIdentity(projection.current_identity);
    const candidate = candidateById.get(text(candidateRow.workflow.candidate_id));
    items.push({
      selection_key: candidateRow.approval.id,
      candidate_name: officeReminderCandidateName(candidate),
      candidate_surname: officeReminderCandidateSurname(candidate),
      last_manager_email_at_utc: manager.provider_accepted_at_utc,
      identity: currentIdentity
    });
  }
  if (items.length > MAX_OFFICE_REMINDER_BATCH_ROWS) {
    throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_CATALOGUE_TOO_LARGE');
  }
  items.sort((left, right) => left.candidate_surname.localeCompare(right.candidate_surname, 'en-GB', { sensitivity: 'base' })
    || left.candidate_name.localeCompare(right.candidate_name, 'en-GB', { sensitivity: 'base' })
    || String(left.last_manager_email_at_utc).localeCompare(String(right.last_manager_email_at_utc))
    || left.selection_key.localeCompare(right.selection_key));
  const revisionFacts = items.map(item => ({
    selection_key: item.selection_key,
    candidate_name: item.candidate_name,
    candidate_surname: item.candidate_surname,
    identity: item.identity,
    last_manager_email_at_utc: item.last_manager_email_at_utc
  }));
  const catalogueRevision = await sha256Hex(`OFFICE_CANDIDATE_REMINDER_ELIGIBILITY_PAGE_V1:${JSON.stringify(revisionFacts)}`);
  return { observed_at_utc: observedAtUtc, catalogue_revision: catalogueRevision, items };
}

async function handleOfficeReminderEligibility(request, env, deps) {
  const user = await requireOfficeActor(request, deps, 'send_manager_reminder_batch');
  const url = new URL(request.url);
  const page = Number(url.searchParams.get('page') || 1);
  const pageSize = Number(url.searchParams.get('page_size') || OFFICE_REMINDER_PAGE_SIZE);
  const expectedRevision = text(url.searchParams.get('catalogue_revision')).toLowerCase();
  const surnameQuery = text(url.searchParams.get('surname_query'));
  const sortBy = upper(url.searchParams.get('sort_by') || 'CANDIDATE_SURNAME');
  const sortDirection = upper(url.searchParams.get('sort_direction') || 'ASC');
  if (!Number.isSafeInteger(page) || page < 1 || !Number.isSafeInteger(pageSize)
      || pageSize < 1 || pageSize > MAX_OFFICE_REMINDER_PAGE_SIZE
      || surnameQuery.length > 100
      || !['CANDIDATE_SURNAME', 'LAST_MANAGER_EMAIL'].includes(sortBy)
      || !['ASC', 'DESC'].includes(sortDirection)
      || (expectedRevision && !SHA256_RE.test(expectedRevision))) {
    throw new CandidateHttpError(400, 'CANDIDATE_REMINDER_CATALOGUE_PAGE_INVALID');
  }
  const catalogue = await loadOfficeManagerReminderCatalogue(env, deps, user.id);
  if (expectedRevision && expectedRevision !== catalogue.catalogue_revision) {
    throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
  }
  const foldedSurnameQuery = surnameQuery.toLocaleLowerCase('en-GB');
  const filteredItems = surnameQuery
    ? catalogue.items.filter(item => item.candidate_surname.toLocaleLowerCase('en-GB').includes(foldedSurnameQuery))
    : [...catalogue.items];
  const direction = sortDirection === 'DESC' ? -1 : 1;
  filteredItems.sort((left, right) => direction * (
    sortBy === 'LAST_MANAGER_EMAIL'
      ? String(left.last_manager_email_at_utc).localeCompare(String(right.last_manager_email_at_utc))
        || left.candidate_surname.localeCompare(right.candidate_surname, 'en-GB', { sensitivity: 'base' })
      : left.candidate_surname.localeCompare(right.candidate_surname, 'en-GB', { sensitivity: 'base' })
        || left.candidate_name.localeCompare(right.candidate_name, 'en-GB', { sensitivity: 'base' })
  ) || left.selection_key.localeCompare(right.selection_key));
  const totalItems = filteredItems.length;
  const pageCount = totalItems ? Math.ceil(totalItems / pageSize) : 0;
  if ((totalItems && page > pageCount) || (!totalItems && page !== 1)) {
    throw new CandidateHttpError(400, 'CANDIDATE_REMINDER_CATALOGUE_PAGE_INVALID');
  }
  const offset = (page - 1) * pageSize;
  return jsonResponse(200, {
    ok: true,
    contract_version: 'OFFICE_CANDIDATE_REMINDER_ELIGIBILITY_PAGE_V1',
    observed_at_utc: catalogue.observed_at_utc,
    catalogue_revision: catalogue.catalogue_revision,
    page,
    page_size: pageSize,
    page_count: pageCount,
    total_items: totalItems,
    catalogue_total_items: catalogue.items.length,
    surname_query: surnameQuery,
    sort_by: sortBy,
    sort_direction: sortDirection,
    matching_selection_keys: filteredItems.map(item => item.selection_key),
    items: filteredItems.slice(offset, offset + pageSize)
  });
}

function officeReminderSelection(value) {
  if (!isObject(value)) throw new CandidateHttpError(400, 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID');
  const mode = upper(value.mode);
  const normalizeKeys = (input) => {
    if (!Array.isArray(input) || input.length > MAX_OFFICE_REMINDER_BATCH_ROWS) {
      throw new CandidateHttpError(400, 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID');
    }
    const keys = [...new Set(input.map(text))];
    if (keys.some(key => !UUID_RE.test(key))) throw new CandidateHttpError(400, 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID');
    return keys;
  };
  const included = normalizeKeys(value.included_row_keys || []);
  const excluded = normalizeKeys(value.excluded_row_keys || []);
  if (!['EXPLICIT', 'ALL_ELIGIBLE'].includes(mode)
      || (mode === 'EXPLICIT' && (!included.length || excluded.length))
      || (mode === 'ALL_ELIGIBLE' && included.length)) {
    throw new CandidateHttpError(400, 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID');
  }
  return { mode, included_row_keys: included, excluded_row_keys: excluded };
}

async function resolveOfficeReminderSelection(env, deps, actorId, body) {
  const selection = officeReminderSelection(body.selection);
  const expectedRevision = text(body.catalogue_revision).toLowerCase();
  if (!SHA256_RE.test(expectedRevision)) throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
  const catalogue = await loadOfficeManagerReminderCatalogue(env, deps, actorId);
  if (expectedRevision !== catalogue.catalogue_revision) throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
  const byKey = new Map(catalogue.items.map(item => [item.selection_key, item]));
  const keys = selection.mode === 'ALL_ELIGIBLE'
    ? catalogue.items.map(item => item.selection_key).filter(key => !selection.excluded_row_keys.includes(key))
    : selection.included_row_keys;
  if (!keys.length || keys.length > MAX_OFFICE_REMINDER_BATCH_ROWS || keys.some(key => !byKey.has(key))) {
    throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
  }
  return { catalogue, selection, identities: keys.map(key => byKey.get(key).identity) };
}

async function handleOfficeReminderBatch(request, env, deps, operation, batchId = null) {
  const user = await requireOfficeActor(request, deps, 'send_manager_reminder_batch');
  if (operation === 'status') {
    return jsonResponse(200, await officeAdapter(deps, env, user.id, 'REMINDER_BATCH_STATUS', {
      batch_id: requireUuid(batchId, 'CANDIDATE_REMINDER_BATCH_NOT_FOUND')
    }));
  }
  const body = await readJson(request);
  const selectionRequest = isObject(body.selection);
  let selectionResolution = null;
  let source = Array.isArray(body.identities) ? body.identities
    : Array.isArray(body.selected_rows) ? body.selected_rows : [];
  const maxRows = selectionRequest ? MAX_OFFICE_REMINDER_BATCH_ROWS : 100;
  if (operation === 'preview' && selectionRequest) {
    selectionResolution = await resolveOfficeReminderSelection(env, deps, user.id, body);
    source = selectionResolution.identities;
  }
  if (source.length < 1 || source.length > maxRows) {
    throw new CandidateHttpError(400, 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID');
  }
  const identities = source.map(officeProjectionIdentity);
  if (operation === 'preview') {
    const preview = await officeAdapter(deps, env, user.id, 'REMINDER_BATCH_PREVIEW', {
      identities
    });
    return jsonResponse(200, selectionRequest ? { ...preview, selected_rows: identities } : preview);
  }

  const batchKey = requireOfficeIdempotency(body.idempotency_key || body.batch_id);
  const previewContextHash = text(body.preview_context_hash);
  const selectionFingerprint = text(body.selection_fingerprint);
  if (!SHA256_RE.test(previewContextHash) || !SHA256_RE.test(selectionFingerprint)) {
    throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
  }
  const clientRequest = {
    identities,
    batch_id: batchKey,
    idempotency_key: batchKey,
    preview_context_hash: previewContextHash,
    selection_fingerprint: selectionFingerprint
  };
  const replay = await officeAdapter(deps, env, user.id, 'REMINDER_BATCH_REPLAY', clientRequest);
  if (replay?.found === true) {
    return jsonResponse(202, {
      ...replay,
      retry_after_ms: 1000,
      status_url: `/api/candidate-app/manager-reminder-batches/${batchKey}`
    });
  }
  if (selectionRequest) {
    selectionResolution = await resolveOfficeReminderSelection(env, deps, user.id, body);
    const resolvedIdentities = selectionResolution.identities.map(officeProjectionIdentity);
    if (JSON.stringify(resolvedIdentities) !== JSON.stringify(identities)) {
      throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
    }
  }
  const currentPreview = await officeAdapter(deps, env, user.id, 'REMINDER_BATCH_PREVIEW', { identities });
  if (currentPreview.preview_context_hash !== previewContextHash
      || currentPreview.selection_fingerprint !== selectionFingerprint) {
    throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
  }
  const eligible = (Array.isArray(currentPreview.items) ? currentPreview.items : [])
    .filter(item => item?.eligible === true);
  const approvalIds = eligible.map(item => requireUuid(
    item.approval_request_id, 'CANDIDATE_REQUEST_GENERATION_STALE'
  ));
  const approvals = approvalIds.length ? await restRows(env, 'candidate_approval_requests',
    `id=in.(${approvalIds.map(encodeURIComponent).join(',')})&select=*`) : [];
  const approvalsById = new Map(approvals.map(row => [text(row.id), row]));
  const reminders = [];
  const managerRouteInputs = new Map();
  for (const item of (Array.isArray(currentPreview.items) ? currentPreview.items : [])) {
    if (item?.eligible !== true) {
      reminders.push({ ...item, eligible: false });
      continue;
    }
    const approval = approvalsById.get(text(item.approval_request_id));
    if (!approval
        || Number(approval.workflow_generation) !== Number(item.workflow_generation)
        || Number(approval.request_generation) !== Number(item.approval_request_generation)) {
      throw new CandidateHttpError(409, 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED');
    }
    const managerToken = await deterministicOpaqueToken(
      env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET,
      'cloudtms-office-manager-reminder-batch-v1', batchKey, item.workflow_id,
      item.workflow_generation, approval.id, approval.request_generation
    );
    const managerWorkflow = await workflowRow(env, item.workflow_id);
    managerRouteInputs.set(text(item.workflow_id), {
      managerToken,
      mutationKey: `office-reminder-batch:${batchKey}:${item.workflow_id}:${item.approval_request_generation}`
    });
    reminders.push({
      ...item,
      eligible: true,
      payload: {
        ...await candidateManagerMail(env, deps, managerToken, item.workflow_id,
          approval.manager_email_normalized, 'REMINDER', managerWorkflow.workflow_kind),
        approval_token_hash_hex: await sha256Hex(managerToken)
      }
    });
  }
  const result = await officeAdapter(deps, env, user.id, 'REMINDER_BATCH_EXECUTE', {
    ...clientRequest,
    reminders
  });
  for (const item of (Array.isArray(result?.items) ? result.items : [])) {
    if (upper(item?.outcome) !== 'QUEUED') continue;
    const routeInput = managerRouteInputs.get(text(item.workflow_id));
    const mutation = item?.result;
    if (!routeInput || !isObject(mutation)) {
      throw new CandidateHttpError(503, 'MANAGER_ROUTE_REGISTRATION_FAILED');
    }
    const current = await restOne(env, 'candidate_approval_requests',
      `id=eq.${encodeURIComponent(item.approval_request_id)}`
      + `&workflow_id=eq.${encodeURIComponent(item.workflow_id)}`
      + '&select=id,request_generation,resend_count,expires_at_utc');
    if (!current) throw new CandidateHttpError(503, 'MANAGER_ROUTE_REGISTRATION_FAILED');
    await registerManagerEmailRoute(env, deps, {
      workflowId: item.workflow_id, approvalRequestId: current.id,
      requestGeneration: Number(current.request_generation),
      credentialGeneration: Number(current.resend_count) + 1,
      mailOutboxId: requireUuid(mutation.mail_outbox_id, 'MANAGER_ROUTE_REGISTRATION_FAILED'),
      managerToken: routeInput.managerToken, mailKind: 'REMINDER',
      expiresAtUtc: current.expires_at_utc, mutationKey: routeInput.mutationKey
    });
  }
  return jsonResponse(202, {
    ...result,
    retry_after_ms: 1000,
    status_url: `/api/candidate-app/manager-reminder-batches/${batchKey}`
  });
}

async function officePaperContext(request, env, deps, workflowId, generationValue = null) {
  const user = await requireOfficeActor(request, deps, 'manage_paper');
  const workflow = await workflowRow(env, workflowId);
  const expectedGeneration = requireInteger(generationValue, 'WORKFLOW_GENERATION_CONFLICT', 1);
  if (Number(workflow.generation) !== expectedGeneration || upper(workflow.route) !== 'PAPER') {
    throw new CandidateHttpError(409, 'WORKFLOW_GENERATION_CONFLICT');
  }
  const delivery = await officeCandidatePaperDelivery(env, workflow);
  const timesheetId = delivery.source_timesheet_id;
  const timesheet = await restOne(env, 'timesheets',
    `timesheet_id=eq.${encodeURIComponent(timesheetId)}`
    + '&select=timesheet_id,version,sheet_scope,submission_mode,qr_status,qr_token,'
    + 'document_state,current_document_version_id,manual_pdf_r2_key,is_current,archived_at_utc');
  if (!timesheet) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_TIMESHEET_NOT_READY');
  const livePreparation = upper(workflow.state) === 'AWAITING_PAPER_RETURN'
    && timesheet.is_current === true && !timesheet.archived_at_utc;
  const version = livePreparation && UUID_RE.test(text(timesheet.current_document_version_id))
    ? await restOne(env, 'invoice_document_versions',
      `id=eq.${encodeURIComponent(timesheet.current_document_version_id)}`
      + `&entity_type=eq.TIMESHEET&entity_id=eq.${encodeURIComponent(timesheetId)}`
      + '&purpose=eq.TIMESHEET&status=eq.READY&select=id,r2_key,sha256,status')
    : null;
  return {
    office_actor_id: user.id,
    workflow,
    timesheet,
    version,
    outbox: delivery.outbox,
    complete: delivery.complete,
    delivery_generation: delivery.delivery_generation,
    live_preparation: livePreparation
  };
}

async function handleOfficePaperPack(request, env, deps, workflowId) {
  const url = new URL(request.url);
  const context = await officePaperContext(
    request, env, deps, workflowId, url.searchParams.get('generation')
  );
  if (!context.complete?.ready) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_NOT_READY');
  }
  const stored = await r2Bytes(env, context.complete.key, context.complete.sha256);
  if (stored.media_type !== 'application/pdf') {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID');
  }
  return new Response(stored.bytes, {
    status: 200,
    headers: {
      'content-type': 'application/pdf',
      'content-length': String(stored.bytes.byteLength),
      'cache-control': 'private, no-store',
      'content-disposition': `inline; filename="Candidate_Paper_Pack_${workflowId}.pdf"`,
      'x-content-type-options': 'nosniff'
    }
  });
}

async function handleOfficePaperReturnReview(request, env, deps, workflowId) {
  const url = new URL(request.url);
  const context = await officePaperContext(
    request, env, deps, workflowId, url.searchParams.get('generation')
  );
  if (upper(context.workflow.state) !== 'RECEIVED') {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_RETURN_NOT_RECEIVED');
  }
  const components = await restRows(env, 'candidate_submission_components',
    `workflow_id=eq.${encodeURIComponent(context.workflow.id)}`
    + `&workflow_generation=eq.${encodeURIComponent(context.workflow.generation)}`
    + '&state=eq.IMMUTABLE&select=id,component_kind,document_role,expense_category,'
    + 'paper_return_page_key,review_ordinal,media_type,byte_size,source_content_sha256&order=review_ordinal.asc,id.asc');
  return jsonResponse(200, {
    ok: true,
    contract_version: 'OFFICE_CANDIDATE_PAPER_RETURN_REVIEW_V1',
    workflow_id: context.workflow.id,
    generation: Number(context.workflow.generation),
    state: context.workflow.state,
    components: components.map(component => ({
      component_id: component.id,
      component_kind: component.component_kind,
      document_role: component.document_role,
      expense_category: component.expense_category,
      paper_return_page_key: component.paper_return_page_key,
      review_ordinal: component.review_ordinal,
      media_type: component.media_type,
      byte_size: Number(component.byte_size || 0),
      sha256: text(component.source_content_sha256).replace(/^\\x/i, '').toLowerCase(),
      document: {
        method: 'GET',
        path: `/api/candidate-app/workflows/${context.workflow.id}/components/${component.id}/document?generation=${context.workflow.generation}`
      }
    }))
  });
}

async function handleOfficePaperRetry(request, env, deps, workflowId) {
  const body = await readJson(request);
  const idempotencyKey = requireOfficeIdempotency(body.idempotency_key);
  const generation = requireInteger(body.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  const user = await requireOfficeActor(request, deps, 'manage_paper');
  const operationIdentity = {
    workflow_id: requireUuid(workflowId, 'CANDIDATE_WORKFLOW_NOT_FOUND'),
    generation,
    idempotency_key: idempotencyKey
  };
  const durableReplay = await officeAdapter(
    deps, env, user.id, 'PAPER_RETRY_REPLAY', operationIdentity
  );
  if (durableReplay?.found === true && isObject(durableReplay.result)) {
    return jsonResponse(Number(durableReplay.http_status || 200), {
      ...durableReplay.result,
      idempotent_replay: true
    });
  }
  const durableResult = async (status, result) => {
    const receipt = await officeAdapter(deps, env, user.id, 'PAPER_RETRY_RECORD', {
      ...operationIdentity,
      http_status: status,
      result
    });
    return jsonResponse(Number(receipt?.http_status || status), {
      ...(receipt?.result || result),
      idempotent_replay: receipt?.idempotent_replay === true
    });
  };
  const context = await officePaperContext(request, env, deps, workflowId, body.generation);
  const retryScope = parseJson(context.outbox?.payment_scope_json, {}) || {};
  const nextRetryAt = Date.parse(text(retryScope.candidate_paper_pack_next_retry_at_utc));
  if (retryScope.candidate_paper_pack_retryable === true
      && Number.isFinite(nextRetryAt) && nextRetryAt > Date.now()) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE', {
      next_retry_at_utc: new Date(nextRetryAt).toISOString()
    });
  }
  if (upper(context.workflow.state) !== 'AWAITING_PAPER_RETURN'
      || !context.live_preparation || !context.version?.r2_key) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_RETRY_NOT_READY');
  }
  const outbox = await requireCandidatePaperOutbox(env, context.workflow, context.timesheet);
  if (!context.complete?.ready && retryScope.candidate_paper_pack_retryable !== true) {
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_PACK_RETRY_NOT_READY');
  }
  const currentOperationMatches = text(retryScope.candidate_paper_pack_operation_id) === idempotencyKey;
  const currentOperationState = upper(retryScope.candidate_paper_pack_operation_state);
  const currentLeaseExpiresAt = Date.parse(text(retryScope.candidate_paper_pack_attempt_expires_at_utc));
  if (currentOperationMatches && currentOperationState === 'CLAIMED'
      && Number.isFinite(currentLeaseExpiresAt) && currentLeaseExpiresAt > Date.now()) {
    return jsonResponse(202, {
      ok: true,
      contract_version: 'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
      idempotency_key: idempotencyKey,
      workflow_id: context.workflow.id,
      generation: Number(context.workflow.generation),
      paper_pack_state: 'RETRY_IN_PROGRESS',
      idempotent_replay: true
    });
  }
  const attemptCount = Number.isSafeInteger(Number(retryScope.candidate_paper_pack_attempt_count))
    ? Number(retryScope.candidate_paper_pack_attempt_count) : 0;
  const claimKey = `${idempotencyKey}:attempt:${attemptCount + 1}`;
  const claim = await claimCandidatePaperPackAttempt(
    env, deps, context.workflow, outbox, claimKey, context.office_actor_id, idempotencyKey
  );
  if (claim.paper_pack_attempt_state === 'READY') {
    return durableResult(200, {
      ok: true,
      contract_version: 'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
      idempotency_key: idempotencyKey,
      workflow_id: context.workflow.id,
      generation: Number(context.workflow.generation),
      paper_pack_state: 'READY',
      idempotent_replay: claim.idempotent_replay === true,
      page_count: Number(context.complete?.page_count || 0)
    });
  }
  if (claim.claim_acquired_new !== true) {
    const replayContext = await officePaperContext(request, env, deps, workflowId, body.generation);
    const replayScope = parseJson(replayContext.outbox?.payment_scope_json, {}) || {};
    const operationMatches = text(replayScope.candidate_paper_pack_operation_id) === idempotencyKey;
    if (replayContext.complete?.ready || (operationMatches
        && replayScope.candidate_paper_pack_operation_state === 'READY')) {
      return durableResult(200, {
        ok: true,
        contract_version: 'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
        idempotency_key: idempotencyKey,
        workflow_id: replayContext.workflow.id,
        generation: Number(replayContext.workflow.generation),
        paper_pack_state: 'READY',
        idempotent_replay: true,
        page_count: Number(replayContext.complete?.page_count || 0)
      });
    }
    if (operationMatches && ['FAILED_RETRYABLE', 'FAILED_TERMINAL'].includes(
      upper(replayScope.candidate_paper_pack_operation_state)
    )) {
      const paperPackState = upper(replayScope.candidate_paper_pack_operation_state);
      return durableResult(paperPackState === 'FAILED_RETRYABLE' ? 503 : 409, {
        ok: false,
        contract_version: 'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
        idempotency_key: idempotencyKey,
        workflow_id: replayContext.workflow.id,
        generation: Number(replayContext.workflow.generation),
        paper_pack_state: paperPackState,
        retryable: paperPackState === 'FAILED_RETRYABLE',
        error_code: text(replayScope.candidate_paper_pack_failure_code)
          || 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED',
        next_retry_at_utc: replayScope.candidate_paper_pack_next_retry_at_utc || null,
        idempotent_replay: true
      });
    }
    const leaseExpiresAt = Date.parse(text(replayScope.candidate_paper_pack_attempt_expires_at_utc));
    if (operationMatches && replayScope.candidate_paper_pack_operation_state === 'CLAIMED'
        && Number.isFinite(leaseExpiresAt) && leaseExpiresAt > Date.now()) {
      return jsonResponse(202, {
        ok: true,
        contract_version: 'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
        idempotency_key: idempotencyKey,
        workflow_id: replayContext.workflow.id,
        generation: Number(replayContext.workflow.generation),
        paper_pack_state: 'RETRY_IN_PROGRESS',
        idempotent_replay: true
      });
    }
    throw new CandidateHttpError(409, 'CANDIDATE_PAPER_RETRY_OPERATION_STALE');
  }
  let complete = context.complete;
  try {
    if (!complete?.ready) {
      complete = await assembleCandidatePaperPack(env, context.workflow, context.timesheet, context.version);
    }
    const release = await releaseCandidatePaperPack(
      env, deps, context.workflow, context.timesheet, complete, outbox, context.office_actor_id,
      claim.attempt_token, idempotencyKey
    );
    const atomicReceipt = release?.office_paper_retry_receipt;
    if (atomicReceipt?.found !== true || !isObject(atomicReceipt.result)) {
      throw new CandidateHttpError(503, 'CANDIDATE_PAPER_RETRY_RECEIPT_MISSING');
    }
    return jsonResponse(Number(atomicReceipt.http_status || 200), {
      ...atomicReceipt.result,
      idempotent_replay: atomicReceipt.idempotent_replay === true
    });
  } catch (error) {
    const failureCode = canonicalPaperPackFailureCode(error);
    let failureReceipt = null;
    try {
      failureReceipt = await recordCandidatePaperPackFailure(
        env, deps, context.workflow, outbox, failureCode, claim.attempt_token,
        context.office_actor_id, `${idempotencyKey}:failure:${failureCode}`, idempotencyKey
      );
    } catch {
      // Preserve the canonical execution error; the scheduler can reconcile an expired attempt lease.
    }
    if (!failureReceipt?.ok) throw error;
    const atomicReceipt = failureReceipt.office_paper_retry_receipt;
    if (atomicReceipt?.found !== true || !isObject(atomicReceipt.result)) {
      throw new CandidateHttpError(503, 'CANDIDATE_PAPER_RETRY_RECEIPT_MISSING');
    }
    return jsonResponse(Number(atomicReceipt.http_status || 503), {
      ...atomicReceipt.result,
      idempotent_replay: atomicReceipt.idempotent_replay === true
    });
  }
}

function routeMatch(path, pattern) {
  const actual = path.split('/').filter(Boolean);
  const expected = pattern.split('/').filter(Boolean);
  if (actual.length !== expected.length) return null;
  const params = {};
  for (let index = 0; index < expected.length; index += 1) {
    if (expected[index].startsWith(':')) params[expected[index].slice(1)] = decodeURIComponent(actual[index]);
    else if (actual[index] !== expected[index]) return null;
  }
  return params;
}

export async function handleCandidateAppRequest(request, env, ctx, deps) {
  const path = new URL(request.url).pathname;
  const correlationId = requestId(request);
  const routeAudience = upper(deps?.routeAudience);
  const privateCandidateRoute = path.startsWith(CANDIDATE_PREFIX) || path.startsWith(MANAGER_PREFIX);
  const officeRoute = path.startsWith('/api/candidate-app/');
  if (!privateCandidateRoute && !officeRoute) return null;
  if (privateCandidateRoute && routeAudience !== 'PRIVATE') return null;
  if (officeRoute && routeAudience !== 'OFFICE') return null;
  try {
    if (isCandidateDailyPath(path)) {
      const access = await verifyCandidateAccess(request, env);
      return await handleCandidateDailyPhase1bRequest(request, access, env, deps);
    }
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/auth/challenge/start`) return await handleChallengeStart(request, env, deps);
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/auth/challenge/resend`) return await handleChallengeStart(request, env, deps, true);
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/auth/challenge/verify`) return await handleChallengeVerify(request, env, deps);
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/auth/password/complete`) return await handlePasswordComplete(request, env, deps);
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/auth/login`) return await handleLogin(request, env, deps);
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/auth/refresh`) return await handleRefresh(request, env, deps);
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/auth/logout`) return await handleAccountAction(request, env, deps, 'LOGOUT');
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/account/select-candidate`) return await handleAccountAction(request, env, deps, 'SELECT_TEST_CANDIDATE');
    if (request.method === 'PATCH' && path === `${CANDIDATE_PREFIX}/account/preferences`) return await handleAccountAction(request, env, deps, 'SET_NOTIFICATION_PREFERENCES');
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/account/push-token`) return await handleAccountAction(request, env, deps, 'REGISTER_PUSH_TOKEN');
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/account/password`) return await handleAccountAction(request, env, deps, 'CHANGE_PASSWORD');
    if (request.method === 'GET' && path === `${CANDIDATE_PREFIX}/bootstrap`) return await handleCandidateRead(request, env, deps, 'bootstrap');
    if (request.method === 'GET' && path === `${CANDIDATE_PREFIX}/timesheets`) return await handleCandidateRead(request, env, deps, 'page');
    if (request.method === 'POST' && path === `${CANDIDATE_PREFIX}/workflows`) return await handleWorkflowCreate(request, env, deps);
    if (request.method === 'GET' && path === `${CANDIDATE_PREFIX}/notifications`) return await handleNotifications(request, env, deps);

    let match = routeMatch(path, `${CANDIDATE_PREFIX}/timesheets/:timesheetId`);
    if (match && request.method === 'GET') return await handleCandidateRead(request, env, deps, 'detail', match);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/contract-weeks/:contractWeekId/detail`);
    if (match && request.method === 'GET') return await handleCandidateRead(request, env, deps, 'detail', match);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/workflows/:workflowId/timesheet-detail`);
    if (match && request.method === 'GET') return await handleCandidateRead(request, env, deps, 'detail', match);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/timesheets/:timesheetId/paper-pack/status`);
    if (match && request.method === 'GET') return await handlePaperPackStatus(request, env, deps, match.timesheetId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/timesheets/:timesheetId/paper-pack`);
    if (match && request.method === 'GET') return await handlePaperPackDownload(request, env, deps, match.timesheetId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/timesheets/:timesheetId/expense-placement`);
    if (match && request.method === 'POST') return await handleExpensePlacement(request, env, deps, match.timesheetId, false);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/timesheets/:timesheetId/expense-carrier`);
    if (match && request.method === 'POST') return await handleExpensePlacement(request, env, deps, match.timesheetId, true);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/contracts/:contractId/missing-weeks`);
    if (match && request.method === 'GET') return await handleCandidateRead(request, env, deps, 'missing-options', match);
    if (match && request.method === 'POST') return await handleAddMissingWeek(request, env, deps, match.contractId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/workflows/:workflowId/components/prepare`);
    if (match && request.method === 'POST') return await handleComponentPrepare(request, env, deps, match.workflowId, 'candidate');
    match = routeMatch(path, `${CANDIDATE_PREFIX}/workflows/:workflowId/resubmit`);
    if (match && request.method === 'POST') return await handleWorkflowResubmit(request, env, deps, match.workflowId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/uploads/:ticket`);
    if (match && request.method === 'PUT') return await handleComponentUpload(request, env, deps, match.ticket);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/workflows/:workflowId/actions/:action`);
    if (match && request.method === 'POST') return await handleWorkflowAction(request, env, deps, match.workflowId, match.action, ctx);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/workflows/:workflowId/components/:componentId/document`);
    if (match && request.method === 'GET') return await handleDocumentStream(request, env, deps, 'candidate', match.workflowId, match.componentId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/contract-weeks/:contractWeekId/no-work`);
    if (match && request.method === 'POST') return await handleCandidateNoWork(request, env, deps, match.contractWeekId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/notifications/:notificationId/read`);
    if (match && request.method === 'POST') {
      return await handleAccountAction(request, env, deps, 'MARK_NOTIFICATION_READ', {
        notification_id: match.notificationId
      });
    }

    match = routeMatch(path, `${MANAGER_PREFIX}/workflows/:workflowId/:action`);
    if (match) {
      const expectedMethod = MANAGER_ACTION_METHODS[match.action];
      if (!expectedMethod) throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
      if (request.method !== expectedMethod) throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleManagerAction(request, env, deps, match.workflowId, match.action, ctx);
    }
    match = routeMatch(path, `${MANAGER_PREFIX}/workflows/:workflowId/components/:componentId/document`);
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleDocumentStream(request, env, deps, 'manager', match.workflowId, match.componentId);
    }
    match = routeMatch(path, `${MANAGER_PREFIX}/workflows/:workflowId/signature/prepare`);
    if (match) {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleComponentPrepare(request, env, deps, match.workflowId, 'manager');
    }

    if (path === '/api/candidate-app/office-capabilities') {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeCapabilities(request, env, deps);
    }
    if (path === '/api/candidate-app/timesheets/office-projections') {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeProjectionBatch(request, env, deps);
    }
    if (path === '/api/candidate-app/manager-reminder-eligibility') {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeReminderEligibility(request, env, deps);
    }
    if (path === '/api/candidate-app/manager-reminder-batches/preview') {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeReminderBatch(request, env, deps, 'preview');
    }
    if (path === '/api/candidate-app/manager-reminder-batches') {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeReminderBatch(request, env, deps, 'execute');
    }
    match = routeMatch(path, '/api/candidate-app/manager-reminder-batches/:batchId');
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeReminderBatch(request, env, deps, 'status', match.batchId);
    }
    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/office-detail');
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeDetail(request, env, deps, match.timesheetId);
    }
    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/route-preview');
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeRoute(request, env, deps, 'preview', match.timesheetId);
    }
    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/route-confirm');
    if (match) {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeRoute(request, env, deps, 'confirm', match.timesheetId);
    }
    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/reject-preview');
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeRejectPreview(request, env, deps, match.timesheetId);
    }
    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/reject');
    if (match) {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficeReject(request, env, deps, match.timesheetId);
    }
    match = routeMatch(path, '/api/candidate-app/workflows/:workflowId/paper-pack');
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficePaperPack(request, env, deps, match.workflowId);
    }
    match = routeMatch(path, '/api/candidate-app/workflows/:workflowId/paper-return-review');
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleOfficePaperReturnReview(request, env, deps, match.workflowId);
    }
    match = routeMatch(path, '/api/candidate-app/workflows/:workflowId/components/:componentId/document');
    if (match) {
      if (request.method !== 'GET') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleDocumentStream(request, env, deps, 'office', match.workflowId, match.componentId);
    }
    match = routeMatch(path, '/api/candidate-app/workflows/:workflowId/actions/:action');
    if (match) {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      if (match.action === 'retry-paper-preparation') {
        return await handleOfficePaperRetry(request, env, deps, match.workflowId);
      }
      return await handleOfficeWorkflowAction(request, env, deps, match.workflowId, match.action, ctx);
    }
    match = routeMatch(path, '/api/candidate-app/workflows/:workflowId/signature/prepare');
    if (match) {
      if (request.method !== 'POST') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleComponentPrepare(request, env, deps, match.workflowId, 'office');
    }
    match = routeMatch(path, '/api/candidate-app/uploads/:ticket');
    if (match) {
      if (request.method !== 'PUT') throw new CandidateHttpError(405, 'METHOD_NOT_ALLOWED');
      return await handleComponentUpload(request, env, deps, match.ticket);
    }
    throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
  } catch (error) {
    return errorResponse(error, correlationId, routeAudience === 'OFFICE');
  }
}

export const candidateAppBackendInternals = Object.freeze({
  derivePasswordVerifier,
  passwordVerificationProof,
  deterministicOpaqueToken,
  createAccessToken,
  verifyPassword,
  forbiddenFinancialKeys,
  segmentBreak,
  explicitNoBreak,
  normaliseAdaptiveBreakEntry,
  normaliseCandidateBreakSubmission,
  deferBackground,
  finaliseReceivedPaperReturn,
  buildOfficialPresentationSnapshot,
  officialPresentationFromRows,
  uploadTicket,
  verifyUploadTicket,
  withoutInternalRenderContracts,
  safeFinalisationResult,
  safeQrPackResponse,
  safeCandidateWorkflowPolicy,
  safeExpensePlacement,
  normaliseCandidateWorkflowCreatePayload,
  safeCandidateNotificationPreferences,
  safeCandidateNotification,
  requireCandidateNotificationPreferences,
  safePaperReturnPages,
  immutablePut,
  preparedUploadContract,
  expenseSummaryDisplayLines,
  mileageJourneyRows,
  officialPeriodWithShiftLines,
  paperPackIdentity,
  candidatePaperDeliveryGeneration,
  candidatePaperCompleteReceipt,
  readyPaperPackReceipt,
  readyGeneratedDocumentReceipt,
  releaseCandidatePaperPack,
  requireCandidatePaperOutbox,
  bindCandidatePaperOutbox,
  assembleCandidatePaperPack,
  renderAndRegister,
  candidateDocumentBranding,
  mileageClaimFormBytes,
  renderExpensePage,
  validateComponentBytes,
  renderContracts,
  routeMatch,
  officeErrorCode,
  knownErrorCode,
  managerActionMethods: MANAGER_ACTION_METHODS,
  environmentName
});
