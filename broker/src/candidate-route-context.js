const encoder = new TextEncoder();
const decoder = new TextDecoder();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SHA256_RE = /^[0-9a-f]{64}$/;
const OPERATION_ID_RE = /^[A-Za-z][A-Za-z0-9]{2,99}$/;
const MAX_CONTEXT_LIFETIME_SECONDS = 5 * 60;
const MAX_CLOCK_SKEW_SECONDS = 120;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function requiredText(value, code) {
  const output = text(value);
  if (!output) throw new Error(code);
  return output;
}

function bytesToHex(bytes) {
  return Array.from(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes))
    .map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function base64UrlEncode(bytes) {
  let binary = '';
  for (const byte of bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function base64UrlDecode(value) {
  const input = text(value);
  if (!/^[A-Za-z0-9_-]+$/.test(input)) return null;
  const padded = `${input}${'='.repeat((4 - (input.length % 4)) % 4)}`
    .replace(/-/g, '+').replace(/_/g, '/');
  try {
    const binary = atob(padded);
    return Uint8Array.from(binary, (character) => character.charCodeAt(0));
  } catch {
    return null;
  }
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

async function sha256Hex(value) {
  const bytes = value instanceof Uint8Array ? value : encoder.encode(String(value));
  return bytesToHex(await crypto.subtle.digest('SHA-256', bytes));
}

async function importHmacKey(secret, usage) {
  return crypto.subtle.importKey(
    'raw', encoder.encode(requiredText(secret, 'CANDIDATE_ROUTE_CONTEXT_SECRET_UNAVAILABLE')),
    { name: 'HMAC', hash: 'SHA-256' }, false, usage
  );
}

function positiveInteger(value, code) {
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < 1) throw new Error(code);
  return output;
}

function exactUuid(value, code) {
  const output = text(value).toLowerCase();
  if (!UUID_RE.test(output)) throw new Error(code);
  return output;
}

function optionalUuid(value, code) {
  return value == null || text(value) === '' ? null : exactUuid(value, code);
}

function exactOperationId(value, code) {
  const output = text(value);
  if (!OPERATION_ID_RE.test(output)) throw new Error(code);
  return output;
}

function exactSha256(value, code) {
  const output = text(value).toLowerCase();
  if (!SHA256_RE.test(output)) throw new Error(code);
  return output;
}

function normalizedCandidateRouteContext(input) {
  if (!isObject(input)) throw new Error('CANDIDATE_ROUTE_CONTEXT_INVALID');
  const globalSessionFamilyId = optionalUuid(
    input.global_session_family_id, 'CANDIDATE_ROUTE_CONTEXT_SESSION_FAMILY_INVALID'
  );
  return {
    v: positiveInteger(input.v, 'CANDIDATE_ROUTE_CONTEXT_VERSION_INVALID'),
    aud: requiredText(input.aud, 'CANDIDATE_ROUTE_CONTEXT_AUDIENCE_INVALID'),
    operation_id: exactOperationId(input.operation_id, 'CANDIDATE_ROUTE_CONTEXT_OPERATION_INVALID'),
    environment: requiredText(input.environment, 'CANDIDATE_ROUTE_CONTEXT_ENVIRONMENT_INVALID').toUpperCase(),
    global_account_id: exactUuid(input.global_account_id, 'CANDIDATE_ROUTE_CONTEXT_ACCOUNT_INVALID'),
    global_session_id: exactUuid(input.global_session_id, 'CANDIDATE_ROUTE_CONTEXT_SESSION_INVALID'),
    ...(globalSessionFamilyId ? { global_session_family_id: globalSessionFamilyId } : {}),
    membership_id: exactUuid(input.membership_id, 'CANDIDATE_ROUTE_CONTEXT_MEMBERSHIP_INVALID'),
    membership_generation: positiveInteger(
      input.membership_generation, 'CANDIDATE_ROUTE_CONTEXT_MEMBERSHIP_GENERATION_INVALID'
    ),
    agency_id: exactUuid(input.agency_id, 'CANDIDATE_ROUTE_CONTEXT_AGENCY_INVALID'),
    agency_candidate_id: exactUuid(input.agency_candidate_id, 'CANDIDATE_ROUTE_CONTEXT_CANDIDATE_INVALID'),
    data_plane_id: exactUuid(input.data_plane_id, 'CANDIDATE_ROUTE_CONTEXT_DATA_PLANE_INVALID'),
    route_version: positiveInteger(input.route_version, 'CANDIDATE_ROUTE_CONTEXT_ROUTE_VERSION_INVALID'),
    session_epoch: positiveInteger(input.session_epoch, 'CANDIDATE_ROUTE_CONTEXT_SESSION_EPOCH_INVALID'),
    issued_at_utc: requiredText(input.issued_at_utc, 'CANDIDATE_ROUTE_CONTEXT_ISSUED_AT_INVALID'),
    expires_at_utc: requiredText(input.expires_at_utc, 'CANDIDATE_ROUTE_CONTEXT_EXPIRES_AT_INVALID'),
    key_version: positiveInteger(input.key_version, 'CANDIDATE_ROUTE_CONTEXT_KEY_VERSION_INVALID')
  };
}

function normalizedManagerRouteContext(input) {
  if (!isObject(input)) throw new Error('CANDIDATE_ROUTE_CONTEXT_INVALID');
  const authorityKind = requiredText(
    input.authority_kind, 'CANDIDATE_ROUTE_CONTEXT_AUTHORITY_INVALID'
  ).toUpperCase();
  const common = {
    v: positiveInteger(input.v, 'CANDIDATE_ROUTE_CONTEXT_VERSION_INVALID'),
    typ: requiredText(input.typ, 'CANDIDATE_ROUTE_CONTEXT_TYPE_INVALID'),
    aud: requiredText(input.aud, 'CANDIDATE_ROUTE_CONTEXT_AUDIENCE_INVALID'),
    authority_kind: authorityKind,
    operation_id: exactOperationId(input.operation_id, 'CANDIDATE_ROUTE_CONTEXT_OPERATION_INVALID'),
    environment: requiredText(input.environment, 'CANDIDATE_ROUTE_CONTEXT_ENVIRONMENT_INVALID').toUpperCase(),
    agency_id: exactUuid(input.agency_id, 'CANDIDATE_ROUTE_CONTEXT_AGENCY_INVALID'),
    data_plane_id: exactUuid(input.data_plane_id, 'CANDIDATE_ROUTE_CONTEXT_DATA_PLANE_INVALID'),
    route_version: positiveInteger(input.route_version, 'CANDIDATE_ROUTE_CONTEXT_ROUTE_VERSION_INVALID'),
    issued_at_utc: requiredText(input.issued_at_utc, 'CANDIDATE_ROUTE_CONTEXT_ISSUED_AT_INVALID'),
    expires_at_utc: requiredText(input.expires_at_utc, 'CANDIDATE_ROUTE_CONTEXT_EXPIRES_AT_INVALID'),
    nonce: exactUuid(input.nonce, 'CANDIDATE_ROUTE_CONTEXT_NONCE_INVALID'),
    key_version: positiveInteger(input.key_version, 'CANDIDATE_ROUTE_CONTEXT_KEY_VERSION_INVALID')
  };
  if (authorityKind === 'MANAGER_EMAIL') {
    return {
      ...common,
      route_version_id: exactUuid(input.route_version_id, 'CANDIDATE_ROUTE_CONTEXT_ROUTE_VERSION_ID_INVALID'),
      binding_manifest_generation: positiveInteger(
        input.binding_manifest_generation, 'CANDIDATE_ROUTE_CONTEXT_BINDING_GENERATION_INVALID'
      ),
      manager_route_ticket_id: exactUuid(
        input.manager_route_ticket_id, 'CANDIDATE_ROUTE_CONTEXT_MANAGER_TICKET_INVALID'
      ),
      route_revision: positiveInteger(input.route_revision, 'CANDIDATE_ROUTE_CONTEXT_ROUTE_REVISION_INVALID'),
      workflow_route_hmac: exactSha256(
        input.workflow_route_hmac, 'CANDIDATE_ROUTE_CONTEXT_WORKFLOW_HMAC_INVALID'
      ),
      approval_request_route_hmac: exactSha256(
        input.approval_request_route_hmac, 'CANDIDATE_ROUTE_CONTEXT_REQUEST_HMAC_INVALID'
      ),
      request_generation: positiveInteger(
        input.request_generation, 'CANDIDATE_ROUTE_CONTEXT_REQUEST_GENERATION_INVALID'
      ),
      credential_generation: positiveInteger(
        input.credential_generation, 'CANDIDATE_ROUTE_CONTEXT_CREDENTIAL_GENERATION_INVALID'
      )
    };
  }
  if (authorityKind === 'MANAGER_PHONE') {
    const globalSessionFamilyId = optionalUuid(
      input.global_session_family_id, 'CANDIDATE_ROUTE_CONTEXT_SESSION_FAMILY_INVALID'
    );
    return {
      ...common,
      global_account_id: exactUuid(input.global_account_id, 'CANDIDATE_ROUTE_CONTEXT_ACCOUNT_INVALID'),
      global_session_id: exactUuid(input.global_session_id, 'CANDIDATE_ROUTE_CONTEXT_SESSION_INVALID'),
      ...(globalSessionFamilyId ? { global_session_family_id: globalSessionFamilyId } : {}),
      membership_id: exactUuid(input.membership_id, 'CANDIDATE_ROUTE_CONTEXT_MEMBERSHIP_INVALID'),
      membership_generation: positiveInteger(
        input.membership_generation, 'CANDIDATE_ROUTE_CONTEXT_MEMBERSHIP_GENERATION_INVALID'
      ),
      agency_candidate_id: exactUuid(
        input.agency_candidate_id, 'CANDIDATE_ROUTE_CONTEXT_CANDIDATE_INVALID'
      ),
      session_epoch: positiveInteger(input.session_epoch, 'CANDIDATE_ROUTE_CONTEXT_SESSION_EPOCH_INVALID')
    };
  }
  throw new Error('CANDIDATE_ROUTE_CONTEXT_AUTHORITY_INVALID');
}

function normalizedRouteContext(input) {
  return Number(input?.v) === 2
    ? normalizedManagerRouteContext(input)
    : normalizedCandidateRouteContext(input);
}

function validateRouteContextTimes(context, nowMilliseconds) {
  const issuedAt = Date.parse(context.issued_at_utc);
  const expiresAt = Date.parse(context.expires_at_utc);
  if (!Number.isFinite(issuedAt) || !Number.isFinite(expiresAt)
      || new Date(issuedAt).toISOString() !== context.issued_at_utc
      || new Date(expiresAt).toISOString() !== context.expires_at_utc
      || expiresAt <= issuedAt
      || expiresAt - issuedAt > MAX_CONTEXT_LIFETIME_SECONDS * 1000
      || issuedAt > nowMilliseconds + MAX_CLOCK_SKEW_SECONDS * 1000
      || expiresAt <= nowMilliseconds) {
    throw new Error('CANDIDATE_ROUTE_CONTEXT_EXPIRED');
  }
}

function routeContextSecretForVersion(env, keyVersion) {
  const configured = positiveInteger(keyVersion, 'CANDIDATE_ROUTE_CONTEXT_KEY_VERSION_INVALID');
  const name = configured === 1
    ? 'CANDIDATE_ROUTE_CONTEXT_SECRET'
    : `CANDIDATE_ROUTE_CONTEXT_SECRET_V${configured}`;
  return requiredText(env?.[name], 'CANDIDATE_ROUTE_CONTEXT_SECRET_UNAVAILABLE');
}

function acceptedRouteContextVersions(env) {
  const configured = text(env?.CANDIDATE_ROUTE_CONTEXT_READ_KEY_VERSIONS || '1')
    .split(',').map((value) => Number(value.trim()))
    .filter((value) => Number.isSafeInteger(value) && value > 0);
  return new Set(configured.length ? configured : [1]);
}

function routeContextCanonical(payloadBase64Url, version = 1) {
  return `cloudtms-candidate-route-context-v${version}\n${payloadBase64Url}`;
}

export async function signCandidateRouteContext(input, options = {}) {
  const keyVersion = positiveInteger(
    options.keyVersion ?? input?.key_version ?? 1,
    'CANDIDATE_ROUTE_CONTEXT_KEY_VERSION_INVALID'
  );
  const version = Number(input?.v) === 2 ? 2 : 1;
  const context = normalizedRouteContext({ ...input, v: version, key_version: keyVersion });
  if (![1, 2].includes(context.v) || context.aud !== 'candidate-private-api'
      || !['TEST', 'LIVE'].includes(context.environment)
      || (context.v === 2 && context.typ !== 'cloudtms-route-context-v2')) {
    throw new Error('CANDIDATE_ROUTE_CONTEXT_INVALID');
  }
  validateRouteContextTimes(context, options.nowMilliseconds ?? Date.now());
  const payloadBase64Url = base64UrlEncode(encoder.encode(canonicalJson(context)));
  const signature = new Uint8Array(await crypto.subtle.sign(
    'HMAC', await importHmacKey(options.secret, ['sign']),
    encoder.encode(routeContextCanonical(payloadBase64Url, version))
  ));
  const envelope = `v${version}.${payloadBase64Url}.${base64UrlEncode(signature)}`;
  return {
    context,
    envelope,
    sha256: await sha256Hex(envelope)
  };
}

export async function verifyCandidateRouteContext(request, env, nowMilliseconds = Date.now()) {
  try {
    const envelope = text(request.headers.get('x-cloudtms-route-context'));
    const suppliedDigest = text(request.headers.get('x-cloudtms-route-context-sha256')).toLowerCase();
    if (!envelope || !SHA256_RE.test(suppliedDigest) || await sha256Hex(envelope) !== suppliedDigest) return null;
    const parts = envelope.split('.');
    if (parts.length !== 3 || !['v1', 'v2'].includes(parts[0])) return null;
    const version = Number(parts[0].slice(1));
    const payloadBytes = base64UrlDecode(parts[1]);
    const signature = base64UrlDecode(parts[2]);
    if (!payloadBytes || !signature || signature.length !== 32) return null;
    if (base64UrlEncode(payloadBytes) !== parts[1]
        || base64UrlEncode(signature) !== parts[2]) return null;
    const context = normalizedRouteContext(JSON.parse(decoder.decode(payloadBytes)));
    if (canonicalJson(context) !== decoder.decode(payloadBytes)
        || context.v !== version || context.aud !== 'candidate-private-api'
        || (version === 2 && context.typ !== 'cloudtms-route-context-v2')
        || !acceptedRouteContextVersions(env).has(context.key_version)) return null;
    validateRouteContextTimes(context, nowMilliseconds);
    const validSignature = await crypto.subtle.verify(
      'HMAC', await importHmacKey(routeContextSecretForVersion(env, context.key_version), ['verify']),
      signature, encoder.encode(routeContextCanonical(parts[1], version))
    );
    if (!validSignature) return null;
    const expectedEnvironment = requiredText(
      env.CANDIDATE_APP_ENVIRONMENT, 'CANDIDATE_ROUTE_CONTEXT_DEPLOYMENT_INVALID'
    ).toUpperCase();
    const expectedAgencyId = exactUuid(
      env.CANDIDATE_AGENCY_ID, 'CANDIDATE_ROUTE_CONTEXT_DEPLOYMENT_INVALID'
    );
    const expectedDataPlaneId = exactUuid(
      env.CANDIDATE_DATA_PLANE_ID, 'CANDIDATE_ROUTE_CONTEXT_DEPLOYMENT_INVALID'
    );
    const expectedRouteVersion = positiveInteger(
      env.CANDIDATE_ROUTE_VERSION, 'CANDIDATE_ROUTE_CONTEXT_DEPLOYMENT_INVALID'
    );
    if (context.environment !== expectedEnvironment || context.agency_id !== expectedAgencyId
        || context.data_plane_id !== expectedDataPlaneId
        || context.route_version !== expectedRouteVersion) return null;
    return { context, envelope, sha256: suppliedDigest };
  } catch {
    return null;
  }
}

export async function candidateFederatedIdentityHmac(secret, environment, identity) {
  const canonical = [
    'cloudtms-candidate-federated-identity-v1',
    requiredText(environment, 'CANDIDATE_ENVIRONMENT_REQUIRED').toUpperCase(),
    exactUuid(identity, 'CANDIDATE_FEDERATED_IDENTITY_INVALID')
  ].join('\n');
  return bytesToHex(await crypto.subtle.sign(
    'HMAC', await importHmacKey(secret, ['sign']), encoder.encode(canonical)
  ));
}

export const candidateRouteContextInternals = Object.freeze({
  acceptedRouteContextVersions,
  base64UrlDecode,
  base64UrlEncode,
  canonicalJson,
  normalizedRouteContext,
  routeContextCanonical,
  routeContextSecretForVersion,
  sha256Hex,
  validateRouteContextTimes
});
