import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import {
  buildOfficialWeekPeriod,
  officialTimesheetNumber,
  renderOfficialTimesheetPdfBytes
} from './timesheet-official-pdf.js';
import { validateFrozenTimesheetPresentationModel } from './invoice-presentation-contract.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SHA256_RE = /^[0-9a-f]{64}$/i;
const CANDIDATE_PREFIX = '/candidate-app/v1';
const MANAGER_PREFIX = '/candidate-manager/v1';
const MAX_JSON_BYTES = 1024 * 1024;
const MAX_COMPONENT_BYTES = 15 * 1024 * 1024;
const ACCESS_TTL_SECONDS = 15 * 60;
const REFRESH_TTL_DAYS = 30;
const REFRESH_ABSOLUTE_TTL_DAYS = 90;
const PASSWORD_ITERATIONS = 100000;
const PASSWORD_SCHEME = 'PBKDF2-HMAC-SHA256';
const PASSWORD_SCHEME_VERSION = 1;
const RENDERER_CONTRACT_VERSION = 'CANDIDATE_REVIEW_DOCUMENTS_V1';

const AUTH_ERROR_CODES = new Set([
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
  'IDEMPOTENCY_CONFLICT',
  'WORKFLOW_VERSION_MISMATCH',
  'WORKFLOW_GENERATION_CONFLICT',
  'ROW_SIGNATURE_MISMATCH',
  'TIMESHEET_MOVED',
  'ROUTE_CHANGE_CONTEXT_CHANGED',
  'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE',
  'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED',
  'CANDIDATE_PAPER_RETURN_PAGE_DUPLICATE',
  'MANAGER_REVIEW_MANIFEST_MISMATCH'
]);

const NOT_FOUND_ERROR_CODES = new Set([
  'CANDIDATE_WORKFLOW_NOT_FOUND',
  'CANDIDATE_DAILY_SHIFT_NOT_FOUND',
  'CANDIDATE_WORKFLOW_CONTRACT_NOT_FOUND',
  'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND',
  'TIMESHEET_NOT_FOUND'
]);

const COMPONENT_MEDIA_TYPES = Object.freeze({
  CANDIDATE_SIGNATURE: ['image/png', 'image/jpeg'],
  MANAGER_SIGNATURE: ['image/png', 'image/jpeg'],
  EXPENSE_EVIDENCE: ['image/png', 'image/jpeg', 'application/pdf'],
  MILEAGE_FORM: ['image/png', 'image/jpeg', 'application/pdf'],
  SIGNED_RETURN: ['image/png', 'image/jpeg', 'application/pdf']
});

const CANDIDATE_WORKFLOW_ACTIONS = new Set([
  'AMEND', 'WORKER_SUBMIT', 'SELECT_APPROVAL_METHOD', 'SELECT_PHONE_APPROVAL',
  'CREATE_EMAIL_APPROVAL_REQUEST', 'PAPER_PREPARE', 'PAPER_RETURN', 'REMIND',
  'RENEW', 'CANCEL', 'SUPERSEDE'
]);

const ROUTE_INTERVENTION_REASONS = new Set([
  'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',
  'CANDIDATE_REPORTED_HOURS_INCORRECT',
  'HIRING_MANAGER_REPORTED_HOURS_INCORRECT',
  'ELECTRONIC_SUBMISSION_TECHNICAL_FAILURE',
  'OTHER_EXCEPTIONAL_OFFICE_INTERVENTION'
]);

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
  const value = upper(env.CANDIDATE_APP_ENVIRONMENT || env.ENVIRONMENT || env.WORKER_ENV || 'TEST');
  if (!['TEST', 'LIVE'].includes(value)) throw new CandidateHttpError(503, 'CANDIDATE_ENVIRONMENT_INVALID');
  return value;
}

function tokenSecret(env) {
  return env.CANDIDATE_SESSION_TOKEN_SECRET || env.SESSION_TOKEN_SECRET;
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
  const source = `${text(error?.code)} ${text(error?.message)}`.toUpperCase();
  const matches = source.match(/[A-Z][A-Z0-9_]{2,}/g) || [];
  const preferred = matches.find((value) => (
    value.startsWith('CANDIDATE_') || value.startsWith('MANAGER_') ||
    value.startsWith('WORKFLOW_') || value.startsWith('ROUTE_') ||
    value.startsWith('TIMESHEET_') || value === 'IDEMPOTENCY_CONFLICT' ||
    value === 'ROW_SIGNATURE_MISMATCH' || value === 'RATE_ISSUE' ||
    value === 'PAY_CHANNEL_ISSUE' || value === 'PAY_METHOD_MISSING' ||
    value === 'EXPENSE_INVOICE_EMAIL_REQUIRED'
  ));
  return preferred || 'CANDIDATE_REQUEST_FAILED';
}

function errorResponse(error, correlationId) {
  const code = knownErrorCode(error);
  let status = error instanceof CandidateHttpError ? error.status : 400;
  if (AUTH_ERROR_CODES.has(code)) status = 401;
  else if (CONFLICT_ERROR_CODES.has(code)) status = 409;
  else if (NOT_FOUND_ERROR_CODES.has(code)) status = 404;
  else if (code.endsWith('_DISABLED') || code.includes('NOT_ALLOWED') || code.includes('FORBIDDEN')) status = 403;
  else if (code === 'CANDIDATE_REQUEST_FAILED') status = 500;
  const body = { ok: false, error_code: code, request_id: correlationId };
  if (error instanceof CandidateHttpError && error.details != null) body.details = error.details;
  return jsonResponse(status, body);
}

async function createAccessToken(env, session) {
  const now = Math.floor(Date.now() / 1000);
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

async function verifyCandidateAccess(request, env) {
  const payload = await verifyCompact(tokenSecret(env), bearerToken(request));
  const now = Math.floor(Date.now() / 1000);
  if (!payload || payload.typ !== 'candidate_access' || payload.aud !== 'cloudtms-candidate-app'
      || payload.env !== environmentName(env) || !UUID_RE.test(text(payload.sid))) {
    throw new CandidateHttpError(401, 'CANDIDATE_ACCESS_TOKEN_INVALID');
  }
  if (!Number.isFinite(Number(payload.exp)) || Number(payload.exp) <= now) {
    throw new CandidateHttpError(401, 'CANDIDATE_ACCESS_TOKEN_EXPIRED');
  }
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

async function verifyPassword(password, account) {
  if (upper(account?.password_scheme) !== PASSWORD_SCHEME
      || Number(account?.password_scheme_version) !== PASSWORD_SCHEME_VERSION) return false;
  const params = isObject(account.password_params_json) ? account.password_params_json : {};
  const iterations = Number(params.iterations || PASSWORD_ITERATIONS);
  if (!Number.isSafeInteger(iterations) || iterations < 50000 || iterations > PASSWORD_ITERATIONS) return false;
  const salt = bytesFromHex(account.password_salt);
  const expected = bytesFromHex(account.password_digest);
  if (salt.length < 16 || expected.length !== 32) return false;
  const actual = bytesFromHex((await derivePasswordVerifier(password, salt, iterations)).digest_hex);
  if (actual.length !== expected.length) return false;
  let difference = 0;
  for (let index = 0; index < actual.length; index += 1) difference |= actual[index] ^ expected[index];
  return difference === 0;
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
    expires_at_utc: sessionResult.expires_at_utc,
    absolute_expires_at_utc: sessionResult.absolute_expires_at_utc
  };
}

async function rpcCall(deps, name, args, options = undefined) {
  const result = await deps.rpc(name, args, options);
  return unwrapRpc(result, name);
}

function publicAppBase(request, env) {
  const configured = text(
    env.CANDIDATE_APP_PUBLIC_URL || env.PUBLIC_APP_BASE_URL ||
    env.PUBLIC_FRONTEND_BASE_URL || env.PUBLIC_SITE_URL
  ).replace(/\/$/, '');
  return configured || new URL(request.url).origin;
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
  const link = `${publicAppBase(request, env)}/candidate/${route}#token=${encodeURIComponent(token)}`;
  const subject = purpose === 'ACTIVATE' ? 'Activate your CloudTMS Candidate App account' : 'Reset your CloudTMS Candidate App password';
  const bodyText = `Use the secure link below to ${purposeText}.\n\n${link}\n\nThis link expires at ${result.expires_at_utc}. If you did not request this, no action is required.`;
  const deterministicKey = `CANDIDATE_AUTH_${purpose}:${result.challenge_id}`;
  return restWrite(env, 'mail_outbox', 'POST', 'on_conflict=deterministic_outbox_key', {
    type: 'TIMESHEET_GENERAL', to: email, subject,
    body_html: `<p>Use the secure link below to ${purposeText}.</p><p><a href="${link}">${subject}</a></p><p>This link expires at ${result.expires_at_utc}. If you did not request this, no action is required.</p>`,
    body_text: bodyText, attachments: [], status: 'QUEUED',
    reference: `candidate-auth:${purpose.toLowerCase()}:${result.challenge_id}`,
    recipient_kind: 'CANDIDATE', context_kind: 'CANDIDATE_AUTH', context_id: null,
    email_type: 'CANDIDATE_APP_TRANSACTIONAL', scheduled_for_utc: new Date().toISOString(),
    next_attempt_at_utc: new Date().toISOString(), deterministic_outbox_key: deterministicKey,
    payment_scope_json: {}
  }, 'resolution=merge-duplicates,return=representation');
}

async function handleChallengeStart(request, env, deps, isResend = false) {
  const body = await readJson(request);
  const email = normaliseEmail(body.email);
  const purpose = upper(body.purpose || 'ACTIVATE');
  if (!['ACTIVATE', 'RESET', 'RECOVERY'].includes(purpose)) throw new CandidateHttpError(400, 'CANDIDATE_CHALLENGE_PURPOSE_INVALID');
  const idempotencyKey = text(body.idempotency_key) || crypto.randomUUID();
  const challengeToken = await deterministicOpaqueToken(
    tokenSecret(env),
    'candidate-auth-challenge-v1',
    environmentName(env), purpose, email, isResend ? text(body.challenge_id) : '', idempotencyKey
  );
  const tokenHash = await sha256Bytes(challengeToken);
  const args = {
    p_action: isResend ? 'RESEND' : 'START', p_environment: environmentName(env),
    p_email_normalized: email, p_purpose: purpose,
    p_challenge_id: isResend ? requireUuid(body.challenge_id, 'CANDIDATE_CHALLENGE_INVALID') : null,
    p_token_hash: `\\x${hex(tokenHash)}`, p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  };
  const result = await rpcCall(deps, 'candidate_auth_challenge_transition_v1', args);
  // The database transition and mail insert are deliberately separate durable authorities.
  // On a retry, the challenge RPC returns the same challenge with deliver_email=false;
  // the deterministic outbox key makes this safe to retry after a prior mail-insert failure.
  if (result?.challenge_id && result?.expires_at_utc) {
    await queueChallengeMail(env, request, result, purpose, email, challengeToken);
  }
  return jsonResponse(202, { ok: true, accepted: true });
}

async function handleChallengeVerify(request, env, deps) {
  const body = await readJson(request);
  const purpose = upper(body.purpose || 'ACTIVATE');
  const token = text(body.token);
  if (!token) throw new CandidateHttpError(400, 'CANDIDATE_CHALLENGE_INVALID');
  const result = await rpcCall(deps, 'candidate_auth_challenge_transition_v1', {
    p_action: 'VERIFY', p_environment: environmentName(env),
    p_email_normalized: normaliseEmail(body.email), p_purpose: purpose,
    p_challenge_id: body.challenge_id ? requireUuid(body.challenge_id) : null,
    p_token_hash: `\\x${await sha256Hex(token)}`, p_idempotency_key: null,
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
  const verifier = await derivePasswordVerifier(body.password);
  const refreshToken = randomToken(32);
  const refreshHash = await sha256Hex(refreshToken);
  const sessionId = crypto.randomUUID();
  const now = new Date();
  const expiries = sessionExpiries(now);
  const deviceHash = body.device_id ? await sha256Hex(text(body.device_id)) : null;
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: 'ACTIVATE_PASSWORD', p_environment: environmentName(env),
    p_account_id: null, p_email_normalized: null, p_session_id: sessionId,
    p_selected_candidate_id: body.selected_candidate_id ? requireUuid(body.selected_candidate_id) : null,
    p_payload: {
      challenge_id: requireUuid(body.challenge_id, 'CANDIDATE_VERIFIED_CHALLENGE_REQUIRED'),
      password_scheme: verifier.scheme, password_scheme_version: verifier.scheme_version,
      password_salt_hex: verifier.salt_hex, password_digest_hex: verifier.digest_hex,
      password_params: verifier.params, refresh_token_hash_hex: refreshHash,
      ...expiries, ...(deviceHash ? { device_id_hash_hex: deviceHash } : {}),
      platform: text(body.platform).slice(0, 80) || null
    },
    p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID(), p_now_utc: now.toISOString()
  });
  const accessToken = await createAccessToken(env, result);
  return jsonResponse(200, safeSessionResponse(result, accessToken, refreshToken));
}

async function handleLogin(request, env, deps) {
  const body = await readJson(request);
  const email = normaliseEmail(body.email);
  const account = await restOne(env, 'candidate_app_accounts',
    `environment=eq.${encodeURIComponent(environmentName(env))}&email_normalized=eq.${encodeURIComponent(email)}` +
    '&select=id,environment,status,password_scheme,password_scheme_version,password_salt,password_digest,password_params_json,locked_until_utc');
  const passwordOk = await verifyPassword(body.password, account || {
    password_scheme: PASSWORD_SCHEME,
    password_scheme_version: PASSWORD_SCHEME_VERSION,
    password_salt: '00'.repeat(16),
    password_digest: '00'.repeat(32),
    password_params_json: { hash: 'SHA-256', iterations: PASSWORD_ITERATIONS, length_bytes: 32 }
  });
  if (!passwordOk) {
    if (account?.id) {
      await rpcCall(deps, 'candidate_auth_account_transition_v1', {
        p_action: 'LOGIN_FAILURE', p_environment: environmentName(env), p_account_id: account.id,
        p_email_normalized: email, p_session_id: null, p_selected_candidate_id: null,
        p_payload: {}, p_idempotency_key: null, p_now_utc: new Date().toISOString()
      }).catch(() => null);
    }
    throw new CandidateHttpError(401, 'CANDIDATE_LOGIN_INVALID');
  }
  const refreshToken = randomToken(32);
  const sessionId = crypto.randomUUID();
  const now = new Date();
  const deviceHash = body.device_id ? await sha256Hex(text(body.device_id)) : null;
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: 'LOGIN_SUCCESS', p_environment: environmentName(env), p_account_id: account.id,
    p_email_normalized: email, p_session_id: sessionId,
    p_selected_candidate_id: body.selected_candidate_id ? requireUuid(body.selected_candidate_id) : null,
    p_payload: {
      refresh_token_hash_hex: await sha256Hex(refreshToken), ...sessionExpiries(now),
      ...(deviceHash ? { device_id_hash_hex: deviceHash } : {}), platform: text(body.platform).slice(0, 80) || null
    },
    p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID(), p_now_utc: now.toISOString()
  });
  return jsonResponse(200, safeSessionResponse(result, await createAccessToken(env, result), refreshToken));
}

async function handleRefresh(request, env, deps) {
  const body = await readJson(request);
  const oldRefresh = text(body.refresh_token);
  if (!oldRefresh) throw new CandidateHttpError(401, 'CANDIDATE_SESSION_INVALID');
  const newRefresh = randomToken(32);
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: 'REFRESH_SESSION', p_environment: environmentName(env),
    p_account_id: null, p_email_normalized: null,
    p_session_id: requireUuid(body.session_id, 'CANDIDATE_SESSION_INVALID'), p_selected_candidate_id: null,
    p_payload: {
      presented_refresh_token_hash_hex: await sha256Hex(oldRefresh),
      new_refresh_token_hash_hex: await sha256Hex(newRefresh), new_session_id: crypto.randomUUID()
    }, p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID(),
    p_now_utc: new Date().toISOString()
  });
  if (result?.ok !== true) throw new CandidateHttpError(401, result?.error_code || 'CANDIDATE_SESSION_INVALID');
  return jsonResponse(200, safeSessionResponse(result, await createAccessToken(env, result), newRefresh));
}

async function handleAccountAction(request, env, deps, action) {
  const access = await verifyCandidateAccess(request, env);
  const body = request.method === 'GET' ? {} : await readJson(request);
  let payload = {};
  let selectedCandidateId = null;
  if (action === 'SELECT_TEST_CANDIDATE') {
    selectedCandidateId = requireUuid(body.selected_candidate_id, 'CANDIDATE_SELECTION_NOT_ALLOWED');
  } else if (action === 'SET_NOTIFICATION_PREFERENCES') {
    if (!isObject(body.notification_preferences)) throw new CandidateHttpError(400, 'CANDIDATE_NOTIFICATION_PREFERENCES_INVALID');
    payload = { notification_preferences: body.notification_preferences };
  } else if (action === 'REGISTER_PUSH_TOKEN') {
    const encrypted = await encryptPushToken(env, text(body.push_token));
    payload = {
      push_provider: upper(body.push_provider),
      push_token_ciphertext_hex: encrypted.ciphertext_hex,
      push_key_version: encrypted.key_version
    };
  } else if (action === 'CHANGE_PASSWORD') {
    const account = await restOne(env, 'candidate_app_accounts',
      `id=eq.${encodeURIComponent(access.account_id)}` +
      '&select=id,password_scheme,password_scheme_version,password_salt,password_digest,password_params_json');
    if (!account || !await verifyPassword(body.current_password, account)) {
      throw new CandidateHttpError(401, 'CANDIDATE_LOGIN_INVALID');
    }
    const verifier = await derivePasswordVerifier(body.password);
    payload = {
      password_scheme: verifier.scheme, password_scheme_version: verifier.scheme_version,
      password_salt_hex: verifier.salt_hex, password_digest_hex: verifier.digest_hex,
      password_params: verifier.params
    };
  }
  const result = await rpcCall(deps, 'candidate_auth_account_transition_v1', {
    p_action: action, p_environment: access.environment, p_account_id: access.account_id,
    p_email_normalized: null, p_session_id: access.session_id,
    p_selected_candidate_id: selectedCandidateId, p_payload: payload,
    p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID(),
    p_now_utc: new Date().toISOString()
  });
  if (action === 'SELECT_TEST_CANDIDATE') {
    const nextAccess = await createAccessToken(env, {
      session_id: access.session_id, rotation: access.rotation
    });
    return jsonResponse(200, { ...result, access_token: nextAccess, access_expires_in_seconds: ACCESS_TTL_SECONDS });
  }
  return jsonResponse(200, result);
}

async function pushEncryptionKey(env) {
  const material = await sha256Bytes(`candidate-push-token-v1:${String(tokenSecret(env) || '')}`);
  return crypto.subtle.importKey('raw', material, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
}

async function encryptPushToken(env, pushToken) {
  if (!pushToken || pushToken.length > 8192) throw new CandidateHttpError(400, 'CANDIDATE_PUSH_TOKEN_INVALID');
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = new Uint8Array(await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv, additionalData: encoder.encode('candidate-push-token-v1') },
    await pushEncryptionKey(env), encoder.encode(pushToken)
  ));
  const packed = new Uint8Array(iv.length + encrypted.length);
  packed.set(iv, 0);
  packed.set(encrypted, iv.length);
  return { ciphertext_hex: hex(packed), key_version: 1 };
}

async function decryptPushToken(env, ciphertext) {
  const packed = bytesFromHex(ciphertext);
  if (packed.length <= 28) throw new Error('CANDIDATE_PUSH_TOKEN_INVALID');
  const plain = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv: packed.slice(0, 12), additionalData: encoder.encode('candidate-push-token-v1') },
    await pushEncryptionKey(env), packed.slice(12)
  );
  return decoder.decode(plain);
}

function componentMediaTypes(kind) {
  const allowed = COMPONENT_MEDIA_TYPES[upper(kind)];
  if (!allowed) throw new CandidateHttpError(400, 'CANDIDATE_COMPONENT_KIND_INVALID');
  return allowed;
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

async function uploadTicket(env, payload) {
  const now = Math.floor(Date.now() / 1000);
  return sealEnvelope(env.UPLOAD_TOKEN_SECRET || tokenSecret(env), 'candidate-component-upload-v1', {
    typ: 'candidate_component_upload', aud: 'cloudtms-candidate-upload',
    iat: now, exp: now + 10 * 60, nonce: crypto.randomUUID(), ...payload
  });
}

async function verifyUploadTicket(env, value) {
  const payload = await openEnvelope(
    env.UPLOAD_TOKEN_SECRET || tokenSecret(env),
    'candidate-component-upload-v1',
    value
  );
  if (!payload || payload.typ !== 'candidate_component_upload' || payload.aud !== 'cloudtms-candidate-upload'
      || Number(payload.exp) <= Math.floor(Date.now() / 1000)) {
    throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_INVALID');
  }
  return payload;
}

async function handleComponentPrepare(request, env, deps, workflowId, owner = 'candidate') {
  const body = await readJson(request);
  const generation = requireInteger(body.generation, 'WORKFLOW_VERSION_MISMATCH', 1);
  const componentKind = upper(body.component_kind);
  const mediaType = normaliseMediaType(body.media_type);
  const allowed = componentMediaTypes(componentKind);
  if (!allowed.includes(mediaType)) throw new CandidateHttpError(415, 'CANDIDATE_COMPONENT_MEDIA_TYPE_INVALID');
  const byteSize = requireInteger(body.byte_size, 'CANDIDATE_COMPONENT_SIZE_INVALID', 1);
  if (byteSize > MAX_COMPONENT_BYTES) throw new CandidateHttpError(413, 'CANDIDATE_COMPONENT_SIZE_INVALID');
  const environment = environmentName(env);
  let sessionId = null;
  let approvalTokenHash = null;
  let ownerId = null;
  if (owner === 'candidate') {
    const access = await verifyCandidateAccess(request, env);
    sessionId = access.session_id;
    ownerId = access.session_id;
  } else if (owner === 'manager') {
    const managerToken = bearerToken(request);
    if (!managerToken) throw new CandidateHttpError(401, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
    approvalTokenHash = await sha256Hex(managerToken);
    ownerId = approvalTokenHash;
  } else if (owner === 'office') {
    const user = await deps.requireOfficeUser(request, ['admin']);
    if (!user) throw new CandidateHttpError(401, 'OFFICE_AUTH_REQUIRED');
    const workflow = await workflowRow(env, workflowId);
    const access = await resolveWorkflowSession(env, workflow);
    if (!access) throw new CandidateHttpError(409, 'CANDIDATE_ACTIVE_SESSION_REQUIRED');
    sessionId = access.session_id;
    ownerId = requireUuid(user.id, 'OFFICE_AUTH_REQUIRED');
  }
  const storageKey = componentStorageKey(environment, workflowId, generation, componentKind, mediaType);
  const payload = {
    component_kind: componentKind,
    document_role: upper(body.document_role),
    expense_category: body.expense_category == null ? null : upper(body.expense_category),
    paper_return_page_key: body.paper_return_page_key == null ? null : text(body.paper_return_page_key),
    storage_key: storageKey, media_type: mediaType, byte_size: byteSize,
    approval_request_id: body.approval_request_id ? requireUuid(body.approval_request_id) : null,
    ...(approvalTokenHash ? {
      approval_token_hash_hex: approvalTokenHash
    } : {})
  };
  const idempotencyKey = text(body.idempotency_key) || crypto.randomUUID();
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', {
    p_session_id: sessionId, p_environment: environment, p_workflow_id: workflowId,
    p_action: 'COMPONENT_PREPARE', p_expected_generation: generation, p_payload: payload,
    p_idempotency_key: idempotencyKey, p_now_utc: new Date().toISOString()
  });
  const componentId = requireUuid(result.component_id, 'CANDIDATE_COMPONENT_PREPARE_FAILED');
  const ticket = await uploadTicket(env, {
    env: environment, owner, owner_id: ownerId, workflow_id: workflowId,
    candidate_session_id: owner === 'office' ? sessionId : null,
    generation, component_id: componentId, component_kind: componentKind,
    key: storageKey, media_type: mediaType, byte_size: byteSize,
    completion_idempotency_key: `${idempotencyKey}:complete`
  });
  return jsonResponse(201, {
    ok: true, workflow_id: workflowId, generation, component_id: componentId,
    upload: {
      method: 'PUT', url: `${CANDIDATE_PREFIX}/uploads/${encodeURIComponent(ticket)}`,
      media_type: mediaType, byte_size: byteSize, expires_in_seconds: 600
    }
  });
}

async function authenticateUploadOwner(request, env, deps, ticket) {
  if (ticket.owner === 'candidate') {
    const access = await verifyCandidateAccess(request, env);
    if (access.session_id !== ticket.owner_id) throw new CandidateHttpError(401, 'CANDIDATE_UPLOAD_TICKET_INVALID');
    return { session_id: access.session_id, approval_token_hash_hex: null };
  }
  if (ticket.owner === 'manager') {
    const managerToken = bearerToken(request);
    const digest = managerToken ? await sha256Hex(managerToken) : '';
    if (!digest || digest !== ticket.owner_id) throw new CandidateHttpError(401, 'MANAGER_APPROVAL_REQUEST_NOT_READY');
    return { session_id: null, approval_token_hash_hex: digest };
  }
  if (ticket.owner === 'office') {
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
  if (bytes.byteLength !== Number(ticket.byte_size) || bytes.byteLength < 1 || bytes.byteLength > MAX_COMPONENT_BYTES) {
    throw new CandidateHttpError(400, 'CANDIDATE_COMPONENT_SIZE_MISMATCH');
  }
  const bucket = env.R2;
  if (!bucket || typeof bucket.put !== 'function') throw new CandidateHttpError(503, 'CANDIDATE_STORAGE_UNAVAILABLE');
  if (typeof bucket.head === 'function' && await bucket.head(ticket.key)) {
    throw new CandidateHttpError(409, 'CANDIDATE_UPLOAD_TICKET_ALREADY_USED');
  }
  await bucket.put(ticket.key, bytes, {
    httpMetadata: { contentType }, customMetadata: {
      purpose: 'candidate-component', workflow_id: ticket.workflow_id,
      component_id: ticket.component_id, sha256: await sha256Hex(bytes)
    }
  });
  const digest = await sha256Hex(bytes);
  try {
    const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', {
      p_session_id: owner.session_id, p_environment: ticket.env,
      p_workflow_id: ticket.workflow_id, p_action: 'COMPONENT_COMPLETE',
      p_expected_generation: Number(ticket.generation),
      p_payload: {
        component_id: ticket.component_id, source_content_sha256_hex: digest,
        verified_byte_size: bytes.byteLength, verified_media_type: contentType,
        ...(owner.approval_token_hash_hex ? { approval_token_hash_hex: owner.approval_token_hash_hex } : {})
      }, p_idempotency_key: ticket.completion_idempotency_key,
      p_now_utc: new Date().toISOString()
    });
    return jsonResponse(200, {
      ok: true, workflow_id: ticket.workflow_id, generation: Number(ticket.generation),
      component_id: ticket.component_id, state: result.state, media_type: contentType,
      byte_size: bytes.byteLength, content_sha256: digest
    });
  } catch (error) {
    await bucket.delete(ticket.key).catch(() => null);
    throw error;
  }
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
  const existing = await bucket.get(key);
  if (existing) {
    const prior = new Uint8Array(await existing.arrayBuffer());
    if ((await sha256Hex(prior)) !== (await sha256Hex(bytes))) {
      throw new CandidateHttpError(409, 'CANDIDATE_RENDER_IDEMPOTENCY_CONFLICT');
    }
    return;
  }
  await bucket.put(key, bytes, { httpMetadata: { contentType: mediaType }, customMetadata: metadata });
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

function officialPresentationFromRows({ timesheet, contractRow, candidate, client } = {}) {
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
    branding: { agency_name: 'Arthur Rai Medical Services' },
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
  return officialPresentationFromRows({ timesheet, contractRow, candidate, client });
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
  const period = buildOfficialWeekPeriod(endDate);
  const rawSchedule = scheduleFromImmutable(workflow, timesheet);
  const lines = rawSchedule.map(scheduleLine).filter((line) => line.date && line.display_start_local && line.display_end_local);
  lines.sort((a, b) => a.date.localeCompare(b.date) || a.display_start_local.localeCompare(b.display_start_local) || a.segment_id.localeCompare(b.segment_id));
  for (const day of period.days) {
    day.shift_lines = lines.filter((line) => line.date === day.date).map((line, index) => ({ ...line, display_order: index + 1 }));
  }
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
    branding: { agency_name: text(frozenPresentation.branding?.agency_name) || 'Arthur Rai Medical Services', logo: {} },
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
    candidate_signature: candidateSignature.data,
    authoriser_signature: managerSignature.data
  } };
}

function expenseLines(workflow, component) {
  const immutable = parseJson(workflow.immutable_submission_json, {}) || {};
  const expenseSubmission = parseJson(immutable.expense_submission || immutable.expense_claim || immutable.expenses || immutable, {}) || {};
  const claim = parseJson(expenseSubmission.canonical_tsfin_snapshot, expenseSubmission) || expenseSubmission;
  const allowed = [
    'mileage_units', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat',
    'travel_pay_ex_vat', 'travel_charge_ex_vat',
    'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat',
    'other_pay_ex_vat', 'other_charge_ex_vat', 'expenses_pay_ex_vat', 'expenses_charge_ex_vat'
  ];
  const rows = allowed.filter((key) => claim[key] != null && Number(claim[key]) !== 0)
    .map((key) => `${key.replace(/_/g, ' ')}: ${String(claim[key])}`);
  return [
    `Workflow: ${workflow.id}`,
    `Week ending: ${workflow.week_ending_date || '-'}`,
    `Category: ${component.expense_category || (component.component_kind === 'MILEAGE_FORM' ? 'MILEAGE' : 'SUMMARY')}`,
    ...rows
  ];
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
  const pdf = await PDFDocument.create();
  const page = pdf.addPage([595.28, 841.89]);
  const regular = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  page.drawRectangle({ x: 0, y: 790, width: page.getWidth(), height: 52, color: rgb(0.04, 0.12, 0.24) });
  page.drawText(component.component_kind === 'EXPENSE_SUMMARY' ? 'Expense claim approval summary' : 'Expense evidence', {
    x: 36, y: 812, size: 16, font: bold, color: rgb(1, 1, 1)
  });
  page.drawText(`Page ${component.review_ordinal || contract.review_ordinal} • ${component.expense_category || 'General'}`, {
    x: 36, y: 797, size: 9, font: regular, color: rgb(0.86, 0.9, 0.96)
  });
  const hasSource = await embedExpenseSource(pdf, page, env, component, contract.render_input, 760, 570);
  if (!hasSource) {
    let y = 750;
    for (const line of expenseLines(workflow, component).slice(0, 24)) {
      page.drawText(line.slice(0, 110), { x: 42, y, size: 10, font: regular, color: rgb(0.08, 0.12, 0.2) });
      y -= 18;
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
    p_idempotency_key: text(idempotencyKey) || crypto.randomUUID()
  });
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

function candidateManagerMail(request, env, token, workflowId, managerEmail, kind = 'INITIAL') {
  const origin = publicAppBase(request, env);
  const link = `${origin}/manager/timesheet/${encodeURIComponent(workflowId)}#token=${encodeURIComponent(token)}`;
  const reminder = kind === 'REMINDER';
  const renewal = kind === 'RENEW';
  const subject = reminder
    ? 'Reminder: timesheet approval required'
    : renewal ? 'Renewed timesheet approval request' : 'Timesheet approval required';
  return {
    manager_email: normaliseEmail(managerEmail),
    mail: {
      to: normaliseEmail(managerEmail), subject,
      body_text: `${reminder ? 'This is a reminder to ' : renewal ? 'Please use this renewed link to ' : 'Please '}review every page and approve or refuse the timesheet.\n\n${link}`,
      body_html: `<p>${reminder ? 'This is a reminder to ' : renewal ? 'Please use this renewed link to ' : 'Please '}review every page and approve or refuse the timesheet.</p><p><a href="${link}">Review timesheet</a></p>`
    }
  };
}

function renderContracts(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.components)) return value.components;
  return [];
}

function safeQrPackResponse(value) {
  const source = isObject(value) ? value : {};
  return {
    queued: source.queued !== false,
    send_state: text(source.send_state) || null,
    document_state: text(source.document_state || source.document_version_status) || null,
    document_operation_id: UUID_RE.test(text(source.document_operation_id)) ? text(source.document_operation_id) : null,
    current_timesheet_id: UUID_RE.test(text(source.current_timesheet_id)) ? text(source.current_timesheet_id) : null,
    timesheet_version: Number.isSafeInteger(Number(source.current_version || source.timesheet_version))
      ? Number(source.current_version || source.timesheet_version) : null,
    recipient_available: source.recipient_available === true
  };
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

async function renderComponentDocument(env, contract, phase) {
  const state = await loadRenderState(env, contract);
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

async function renderAndRegister(env, deps, renderContract, phase) {
  const contracts = renderContracts(renderContract);
  if (!contracts.length) throw new CandidateHttpError(409, 'MANAGER_REVIEW_DOCUMENT_NOT_READY');
  const results = [];
  for (const contract of contracts) {
    const workflowId = requireUuid(contract.workflow_id, 'CANDIDATE_WORKFLOW_NOT_FOUND');
    const generation = requireInteger(contract.workflow_generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
    const componentId = requireUuid(contract.component_id, 'CANDIDATE_COMPONENT_NOT_FOUND');
    const rendered = await renderComponentDocument(env, contract, phase);
    const digest = await sha256Hex(rendered.pdf_bytes);
    const storageKey = `candidate-app/${environmentName(env).toLowerCase()}/${workflowId}/${generation}/${phase.toLowerCase()}/${String(contract.review_ordinal).padStart(3, '0')}-${componentId}.pdf`;
    await immutablePut(env, storageKey, rendered.pdf_bytes, 'application/pdf', {
      purpose: `candidate-${phase.toLowerCase()}`,
      workflow_id: workflowId,
      component_id: componentId,
      sha256: digest
    });
    const receipt = {
      form_variant: upper(contract.form_variant),
      workflow_id: workflowId,
      workflow_generation: generation,
      component_id: componentId,
      component_kind: upper(contract.component_kind),
      document_role: upper(contract.document_role),
      review_ordinal: Number(contract.review_ordinal),
      scope: upper(contract.scope),
      page_count: Number(rendered.page_count || 1),
      render_input_sha256: text(contract.render_input_sha256).toLowerCase(),
      candidate_signature_embedded: contract.candidate_signature_embedded === true,
      manager_signature_embedded: phase === 'FINAL',
      manager_approval_date_embedded: phase === 'FINAL'
    };
    if (phase === 'FINAL') {
      receipt.manager_signature_sha256 = text(contract.manager?.signature_sha256).replace(/^\\x/i, '').toLowerCase();
      receipt.manager_name = text(contract.manager?.name);
      receipt.manager_position = text(contract.manager?.position);
      receipt.manager_approved_at_utc = contract.manager?.approval_date_utc;
      if (rendered.candidate_signature_sha256) receipt.candidate_signature_sha256 = rendered.candidate_signature_sha256;
    }
    const action = phase === 'FINAL' ? 'REGISTER_FINAL_SIGNED_DOCUMENT' : 'REGISTER_REVIEW_COMPONENT';
    const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
      null, env, workflowId, action, generation,
      {
        component_id: componentId,
        storage_key: storageKey,
        media_type: 'application/pdf',
        byte_size: rendered.pdf_bytes.byteLength,
        page_count: Number(rendered.page_count || 1),
        content_sha256_hex: digest,
        render_input_sha256_hex: text(contract.render_input_sha256).toLowerCase(),
        renderer_contract_version: RENDERER_CONTRACT_VERSION,
        renderer_receipt: receipt
      },
      `candidate-render:${phase.toLowerCase()}:${workflowId}:${generation}:${componentId}:${digest}`
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

async function finaliseWorkflow(env, deps, workflowId, generation, idempotencyKey) {
  const workflow = await workflowRow(env, workflowId);
  const access = await resolveWorkflowSession(env, workflow);
  if (!access) {
    return { ok: true, finalisation_pending: true, reason: 'CANDIDATE_ACTIVE_SESSION_REQUIRED', workflow_id: workflow.id, generation: workflow.generation };
  }
  if (workflow.workflow_kind === 'DAILY') {
    return safeFinalisationResult(await deps.finaliseDaily({
      sessionId: access.session_id,
      environment: access.environment,
      workflowId: workflow.id,
      expectedGeneration: generation,
      idempotencyKey: idempotencyKey || crypto.randomUUID(),
      nowUtc: new Date().toISOString()
    }));
  }
  return safeFinalisationResult(await rpcCall(deps, 'candidate_submission_finalize_atomic_v1', candidateRpcArgs(access, env, {
    p_workflow_id: workflow.id,
    p_expected_generation: generation,
    p_expected_row_signature: await lifecycleSignature(deps, workflow),
    p_idempotency_key: text(idempotencyKey) || crypto.randomUUID(),
    p_daily_materialisation_json: null
  })));
}

async function handleCandidateRead(request, env, deps, kind, params = {}) {
  const access = await verifyCandidateAccess(request, env);
  const url = new URL(request.url);
  if (kind === 'bootstrap') {
    return jsonResponse(200, await rpcCall(deps, 'candidate_app_bootstrap_v1', candidateRpcArgs(access, env, {
      p_expected_rotation: access.rotation
    })));
  }
  if (kind === 'page') {
    return jsonResponse(200, await rpcCall(deps, 'candidate_app_timesheet_page_v1', candidateRpcArgs(access, env, {
      p_cursor: url.searchParams.get('cursor'),
      p_limit: Math.min(100, Math.max(1, Number(url.searchParams.get('limit') || 50)))
    })));
  }
  if (kind === 'detail') {
    return jsonResponse(200, await rpcCall(deps, 'candidate_app_timesheet_detail_v1', candidateRpcArgs(access, env, {
      p_timesheet_id: params.timesheetId ? requireUuid(params.timesheetId) : null,
      p_contract_week_id: url.searchParams.get('contract_week_id') ? requireUuid(url.searchParams.get('contract_week_id')) : null,
      p_workflow_id: url.searchParams.get('workflow_id') ? requireUuid(url.searchParams.get('workflow_id')) : null
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
    p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID()
  }));
  return jsonResponse(201, result);
}

async function handleExpensePlacement(request, env, deps, timesheetId, createCarrier = false) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const anchorTimesheetId = requireUuid(timesheetId);
  if (createCarrier) {
    return jsonResponse(200, await rpcCall(deps, 'expense_carrier_resolve_or_create_atomic_v1', {
      p_candidate_id: requireUuid(access.selected_candidate_id, 'CANDIDATE_SELECTION_REQUIRED'),
      p_environment: access.environment,
      p_anchor_timesheet_id: anchorTimesheetId,
      p_expected_row_signature: text(body.expected_row_signature) || null,
      p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID(),
      p_now_utc: new Date().toISOString()
    }));
  }
  const proposedClaim = isObject(body.proposed_claim) ? body.proposed_claim : {};
  const forbidden = forbiddenFinancialKeys(proposedClaim);
  if (forbidden.length) {
    throw new CandidateHttpError(400, 'CANDIDATE_FINANCIAL_AUTHORITY_FORBIDDEN', { fields: forbidden.slice(0, 20) });
  }
  return jsonResponse(200, await rpcCall(deps, 'expense_placement_resolve_v1', {
    p_candidate_id: requireUuid(access.selected_candidate_id, 'CANDIDATE_SELECTION_REQUIRED'),
    p_environment: access.environment,
    p_anchor_timesheet_id: anchorTimesheetId,
    p_contract_week_id: body.contract_week_id ? requireUuid(body.contract_week_id) : null,
    p_proposed_claim: proposedClaim,
    p_now_utc: new Date().toISOString()
  }));
}

async function handleWorkflowCreate(request, env, deps) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const workflowId = body.workflow_id ? requireUuid(body.workflow_id) : crypto.randomUUID();
  const payload = isObject(body.workflow) ? body.workflow : body;
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    access, env, workflowId, 'CREATE', null, payload, body.idempotency_key
  ));
  return jsonResponse(201, result);
}

async function prepareImmutableSubmission(env, deps, workflow, body) {
  const supplied = isObject(body.immutable_submission) ? structuredClone(body.immutable_submission) : {};
  const forbidden = forbiddenFinancialKeys(supplied);
  if (forbidden.length) throw new CandidateHttpError(400, 'CANDIDATE_FINANCIAL_AUTHORITY_FORBIDDEN', { fields: forbidden.slice(0, 20) });
  const authoritySubmission = workflow.workflow_kind === 'DAILY'
    ? await deps.buildDailySubmission({ workflow, factualSubmission: supplied })
    : await deps.buildWeeklySubmission({ workflow, factualSubmission: supplied });
  return {
    ...authoritySubmission,
    official_presentation: await buildOfficialPresentationSnapshot(env, workflow)
  };
}

async function handleWorkflowAction(request, env, deps, workflowId, action, ctx) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  const generation = requireInteger(body.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  let dbAction = upper(action.replace(/-/g, '_'));
  if (!CANDIDATE_WORKFLOW_ACTIONS.has(dbAction) && !['FINALISE', 'COMPONENT_SUPERSEDE'].includes(dbAction)) {
    throw new CandidateHttpError(400, 'CANDIDATE_WORKFLOW_ACTION_INVALID');
  }
  if (dbAction === 'FINALISE') {
    return jsonResponse(200, await finaliseWorkflow(env, deps, workflowId, generation, body.idempotency_key));
  }
  let payload = isObject(body.payload) ? structuredClone(body.payload) : {};
  if (dbAction === 'WORKER_SUBMIT') {
    const workflow = await workflowRow(env, workflowId);
    payload = {
      ...payload,
      immutable_submission: await prepareImmutableSubmission(env, deps, workflow, body),
      candidate_signature_component_id: body.candidate_signature_component_id || payload.candidate_signature_component_id || null,
      candidate_signed_at_utc: body.candidate_signed_at_utc || new Date().toISOString(),
      approval_route: upper(body.approval_route || payload.approval_route || workflow.route),
      renderer_contract_version: RENDERER_CONTRACT_VERSION
    };
  } else if (dbAction === 'CREATE_EMAIL_APPROVAL_REQUEST' || dbAction === 'RENEW' || dbAction === 'REMIND') {
    if (dbAction === 'REMIND') dbAction = 'RENEW';
    const managerToken = randomToken(32);
    const mailKind = upper(action.replace(/-/g, '_')) === 'REMIND' ? 'REMINDER'
      : dbAction === 'RENEW' ? 'RENEW' : 'INITIAL';
    let managerEmail = body.manager_email || payload.manager_email;
    if (!managerEmail && dbAction === 'RENEW') {
      const approval = await restOne(env, 'candidate_approval_requests',
        `workflow_id=eq.${encodeURIComponent(workflowId)}&method=eq.EMAIL&select=manager_email_normalized&order=request_generation.desc`);
      managerEmail = approval?.manager_email_normalized;
    }
    payload = {
      ...payload,
      ...candidateManagerMail(request, env, managerToken, workflowId, managerEmail, mailKind),
      approval_token_hash_hex: await sha256Hex(managerToken)
    };
  }
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    access, env, workflowId, dbAction, generation, payload, body.idempotency_key
  ));
  if (dbAction === 'WORKER_SUBMIT' && result?.render_contract) {
    const work = renderAndRegister(env, deps, result.render_contract, 'REVIEW');
    const deferred = deferBackground(ctx, work, 'review-render', { workflow_id: workflowId, generation });
    if (deferred !== true) await deferred;
    return jsonResponse(202, { ...withoutInternalRenderContracts(result), review_rendering_accepted: true });
  }
  if (dbAction === 'PAPER_PREPARE' && result?.state === 'AWAITING_PAPER_RETURN' && deps.enqueueQrPack) {
    const workflow = await workflowRow(env, workflowId);
    const timesheetId = workflow.target_timesheet_id || workflow.anchor_timesheet_id;
    if (!timesheetId) throw new CandidateHttpError(409, 'CANDIDATE_PAPER_TIMESHEET_NOT_READY');
    const pack = await deps.enqueueQrPack({
      timesheetId,
      expectedTimesheetId: timesheetId,
      idempotencyKey: `${body.idempotency_key || crypto.randomUUID()}:paper-pack`,
      ctx
    });
    return jsonResponse(202, {
      ...result,
      paper_pack_queued: pack?.queued !== false,
      paper_pack: safeQrPackResponse(pack)
    });
  }
  if (dbAction === 'PAPER_RETURN' && result?.state === 'RECEIVED') {
    const completion = await finaliseReceivedPaperReturn(result, () => finaliseWorkflow(
        env,
        deps,
        workflowId,
        generation,
        `${body.idempotency_key || crypto.randomUUID()}:paper-finalise`
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

async function handleManagerAction(request, env, deps, workflowId, action, ctx) {
  const auth = await managerTokenContext(request, env);
  const body = request.method === 'GET' ? {} : await readJson(request);
  const generation = body.generation == null ? null : requireInteger(body.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  const dbAction = {
    start: 'BEGIN_MANAGER_REVIEW', progress: 'RECORD_REVIEW_PROGRESS',
    approve: 'EMAIL_APPROVE', refuse: 'MANAGER_REFUSE'
  }[action];
  if (!dbAction) throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
  const payload = { ...(isObject(body.payload) ? body.payload : body), approval_token_hash_hex: auth.token_hash_hex };
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    null, env, workflowId, dbAction, generation, payload, body.idempotency_key
  ));
  if (dbAction === 'EMAIL_APPROVE' && result?.final_render_contract) {
    const work = (async () => {
      await renderAndRegister(env, deps, result.final_render_contract, 'FINAL');
      return finaliseWorkflow(env, deps, workflowId, result.generation, `${body.idempotency_key || crypto.randomUUID()}:finalise`);
    })();
    const deferred = deferBackground(ctx, work, 'manager-final-render-and-finalise', {
      workflow_id: workflowId,
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
  } else {
    const auth = await managerTokenContext(request, env);
    const manifest = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
      null, env, workflowId, 'BEGIN_MANAGER_REVIEW', null,
      { approval_token_hash_hex: auth.token_hash_hex }, `manager-document:${workflowId}:${componentId}`
    ));
    const allowedIds = (Array.isArray(manifest?.ordered_components) ? manifest.ordered_components : [])
      .map((entry) => text(entry?.component_id || entry?.id));
    if (!allowedIds.includes(componentId)) {
      throw new CandidateHttpError(404, 'CANDIDATE_DOCUMENT_NOT_FOUND');
    }
    component = await restOne(env, 'candidate_submission_components',
      `id=eq.${encodeURIComponent(componentId)}&workflow_id=eq.${encodeURIComponent(workflowId)}&select=*`);
  }
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
      'x-content-type-options': 'nosniff'
    }
  });
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

  let version = null;
  if (upper(timesheet.document_state) === 'READY' && UUID_RE.test(text(timesheet.current_document_version_id))) {
    version = await restOne(env, 'invoice_document_versions',
      `id=eq.${encodeURIComponent(timesheet.current_document_version_id)}`
      + `&entity_type=eq.TIMESHEET&entity_id=eq.${encodeURIComponent(id)}`
      + '&purpose=eq.TIMESHEET&status=eq.READY&select=id,r2_key,sha256,status');
  }
  const key = text(version?.r2_key || '');
  const ready = Boolean(version && key);
  const state = ready ? 'READY'
    : upper(timesheet.document_state) === 'FAILED' ? 'FAILED' : 'PREPARING';
  return { id, timesheet, version, key, ready, state };
}

async function handlePaperPackStatus(request, env, deps, timesheetId) {
  const context = await candidatePaperPackContext(request, env, deps, timesheetId);
  return jsonResponse(200, {
    ok: true,
    timesheet_id: context.id,
    timesheet_version: Number(context.timesheet.version || 1),
    paper_pack_state: context.state,
    download_available: context.ready
  });
}

async function handlePaperPackDownload(request, env, deps, timesheetId) {
  const context = await candidatePaperPackContext(request, env, deps, timesheetId);
  if (!context.ready) {
    throw new CandidateHttpError(409, context.state === 'FAILED'
      ? 'CANDIDATE_PAPER_PACK_FAILED' : 'CANDIDATE_PAPER_PACK_PREPARING');
  }

  const stored = await r2Bytes(env, context.key, text(context.version.sha256) || null);
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

async function handleCandidateNoWork(request, env, deps, contractWeekId) {
  const access = await verifyCandidateAccess(request, env);
  const body = await readJson(request);
  return jsonResponse(200, await rpcCall(deps, 'candidate_no_work_atomic_v1', candidateRpcArgs(access, env, {
    p_contract_week_id: requireUuid(contractWeekId),
    p_expected_row_signature: text(body.expected_row_signature),
    p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID()
  })));
}

async function handleNotifications(request, env, deps, notificationId = null) {
  const access = await verifyCandidateAccess(request, env);
  if (request.method === 'GET') {
    const rows = await restRows(env, 'candidate_notifications',
      `account_id=eq.${encodeURIComponent(access.account_id)}&select=id,event_type,template_key,payload_json,deep_link_json,state,created_at_utc,read_at_utc&order=created_at_utc.desc&limit=100`);
    return jsonResponse(200, { ok: true, notifications: rows });
  }
  const id = requireUuid(notificationId);
  const updated = await restWrite(
    env,
    'candidate_notifications',
    'PATCH',
    `id=eq.${encodeURIComponent(id)}&account_id=eq.${encodeURIComponent(access.account_id)}&state=eq.UNREAD`,
    { state: 'READ', read_at_utc: new Date().toISOString() },
    'return=representation'
  );
  return jsonResponse(200, {
    ok: true,
    notification_id: id,
    state: updated?.state || 'READ',
    idempotent_replay: !updated
  });
}

async function handleOfficeRoute(request, env, deps, action, timesheetId) {
  const user = await deps.requireOfficeUser(request, ['admin']);
  if (!user) throw new CandidateHttpError(401, 'OFFICE_AUTH_REQUIRED');
  if (action === 'preview') {
    const url = new URL(request.url);
    return jsonResponse(200, await rpcCall(deps, 'timesheet_route_version_preview_v1', {
      p_current_timesheet_id: requireUuid(timesheetId), p_target_action: upper(url.searchParams.get('action'))
    }));
  }
  const body = await readJson(request);
  if (!ROUTE_INTERVENTION_REASONS.has(upper(body.reason_code)) && body.reason_code != null) {
    throw new CandidateHttpError(400, 'ROUTE_CHANGE_REASON_INVALID');
  }
  return jsonResponse(200, await rpcCall(deps, 'timesheet_route_version_confirmed_v1', {
    p_current_timesheet_id: requireUuid(timesheetId),
    p_expected_timesheet_id: requireUuid(body.expected_timesheet_id),
    p_expected_row_signature: text(body.expected_row_signature),
    p_expected_context_sha256: text(body.expected_context_sha256),
    p_target_action: upper(body.action),
    p_actor_user_id: requireUuid(user.id),
    p_reason_code: body.reason_code == null ? null : upper(body.reason_code),
    p_reason_note: body.reason_note == null ? null : text(body.reason_note),
    p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID(),
    p_allow_manual_only: body.allow_manual_only === true,
    p_now_utc: new Date().toISOString()
  }));
}

async function handleOfficeWorkflowAction(request, env, deps, workflowId, action, ctx) {
  const user = await deps.requireOfficeUser(request, ['admin']);
  if (!user) throw new CandidateHttpError(401, 'OFFICE_AUTH_REQUIRED');
  const workflow = await workflowRow(env, workflowId);
  const access = await resolveWorkflowSession(env, workflow);
  if (!access) throw new CandidateHttpError(409, 'CANDIDATE_ACTIVE_SESSION_REQUIRED');
  const body = await readJson(request);
  const generation = requireInteger(body.generation || workflow.generation, 'WORKFLOW_GENERATION_CONFLICT', 1);
  if (action === 'retry-finalisation') {
    const contract = workflow.last_mutation_response_json?.final_render_contract;
    if (contract) await renderAndRegister(env, deps, contract, 'FINAL');
    return jsonResponse(200, await finaliseWorkflow(env, deps, workflow.id, generation, body.idempotency_key));
  }
  const dbAction = {
    'phone-review': 'BEGIN_MANAGER_REVIEW',
    'phone-progress': 'RECORD_REVIEW_PROGRESS',
    'phone-approve': 'PHONE_APPROVE',
    'phone-refuse': 'MANAGER_REFUSE'
  }[action];
  if (!dbAction) throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
  const payload = isObject(body.payload) ? body.payload : body;
  const result = await rpcCall(deps, 'candidate_workflow_transition_atomic_v1', workflowActionArgs(
    access, env, workflow.id, dbAction, generation, payload, body.idempotency_key
  ));
  if (dbAction === 'PHONE_APPROVE' && result?.final_render_contract) {
    const work = (async () => {
      await renderAndRegister(env, deps, result.final_render_contract, 'FINAL');
      return finaliseWorkflow(env, deps, workflow.id, result.generation, `${body.idempotency_key || crypto.randomUUID()}:finalise`);
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
  const user = await deps.requireOfficeUser(request, ['admin']);
  if (!user) throw new CandidateHttpError(401, 'OFFICE_AUTH_REQUIRED');
  const body = await readJson(request);
  return jsonResponse(200, await rpcCall(deps, 'candidate_submission_reject_atomic_v1', {
    p_actor_user_id: requireUuid(user.id, 'OFFICE_AUTH_REQUIRED'),
    p_environment: environmentName(env),
    p_timesheet_id: requireUuid(timesheetId),
    p_expected_timesheet_id: requireUuid(body.expected_timesheet_id || timesheetId),
    p_expected_row_signature: text(body.expected_row_signature),
    p_reason: text(body.reason),
    p_idempotency_key: text(body.idempotency_key) || crypto.randomUUID(),
    p_now_utc: new Date().toISOString()
  }));
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
  if (!path.startsWith(CANDIDATE_PREFIX) && !path.startsWith(MANAGER_PREFIX)
      && !path.startsWith('/api/candidate-app/')) return null;
  try {
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
    match = routeMatch(path, `${CANDIDATE_PREFIX}/uploads/:ticket`);
    if (match && request.method === 'PUT') return await handleComponentUpload(request, env, deps, match.ticket);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/workflows/:workflowId/actions/:action`);
    if (match && request.method === 'POST') return await handleWorkflowAction(request, env, deps, match.workflowId, match.action, ctx);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/workflows/:workflowId/components/:componentId/document`);
    if (match && request.method === 'GET') return await handleDocumentStream(request, env, deps, 'candidate', match.workflowId, match.componentId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/contract-weeks/:contractWeekId/no-work`);
    if (match && request.method === 'POST') return await handleCandidateNoWork(request, env, deps, match.contractWeekId);
    match = routeMatch(path, `${CANDIDATE_PREFIX}/notifications/:notificationId/read`);
    if (match && request.method === 'POST') return await handleNotifications(request, env, deps, match.notificationId);

    match = routeMatch(path, `${MANAGER_PREFIX}/workflows/:workflowId/:action`);
    if (match && ['GET', 'POST'].includes(request.method)) return await handleManagerAction(request, env, deps, match.workflowId, match.action, ctx);
    match = routeMatch(path, `${MANAGER_PREFIX}/workflows/:workflowId/components/:componentId/document`);
    if (match && request.method === 'GET') return await handleDocumentStream(request, env, deps, 'manager', match.workflowId, match.componentId);
    match = routeMatch(path, `${MANAGER_PREFIX}/workflows/:workflowId/signature/prepare`);
    if (match && request.method === 'POST') return await handleComponentPrepare(request, env, deps, match.workflowId, 'manager');

    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/route-preview');
    if (match && request.method === 'GET') return await handleOfficeRoute(request, env, deps, 'preview', match.timesheetId);
    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/route-confirm');
    if (match && request.method === 'POST') return await handleOfficeRoute(request, env, deps, 'confirm', match.timesheetId);
    match = routeMatch(path, '/api/candidate-app/timesheets/:timesheetId/reject');
    if (match && request.method === 'POST') return await handleOfficeReject(request, env, deps, match.timesheetId);
    match = routeMatch(path, '/api/candidate-app/workflows/:workflowId/actions/:action');
    if (match && request.method === 'POST') return await handleOfficeWorkflowAction(request, env, deps, match.workflowId, match.action, ctx);
    match = routeMatch(path, '/api/candidate-app/workflows/:workflowId/signature/prepare');
    if (match && request.method === 'POST') return await handleComponentPrepare(request, env, deps, match.workflowId, 'office');
    throw new CandidateHttpError(404, 'CANDIDATE_ROUTE_NOT_FOUND');
  } catch (error) {
    return errorResponse(error, correlationId);
  }
}

export const candidateAppBackendInternals = Object.freeze({
  derivePasswordVerifier,
  deterministicOpaqueToken,
  verifyPassword,
  forbiddenFinancialKeys,
  segmentBreak,
  explicitNoBreak,
  deferBackground,
  finaliseReceivedPaperReturn,
  buildOfficialPresentationSnapshot,
  officialPresentationFromRows,
  uploadTicket,
  verifyUploadTicket,
  withoutInternalRenderContracts,
  safeFinalisationResult,
  safeQrPackResponse,
  renderContracts,
  routeMatch,
  environmentName
});
