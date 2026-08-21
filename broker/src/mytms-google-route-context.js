const encoder = new TextEncoder();
const decoder = new TextDecoder();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SHA256_RE = /^[a-f0-9]{64}$/;
const MAX_LIFETIME_MS = 5 * 60 * 1000;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function bytesToHex(bytes) {
  return Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, '0')).join('');
}

function base64UrlEncode(bytes) {
  let binary = '';
  for (const byte of new Uint8Array(bytes)) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/g, '');
}

function base64UrlDecode(value) {
  const source = text(value);
  if (!/^[A-Za-z0-9_-]+$/.test(source)) return null;
  try {
    const binary = atob(`${source}${'='.repeat((4 - source.length % 4) % 4)}`
      .replaceAll('-', '+').replaceAll('_', '/'));
    return Uint8Array.from(binary, character => character.charCodeAt(0));
  } catch {
    return null;
  }
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(
      key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    ).join(',')}}`;
  }
  return JSON.stringify(value);
}

function uuid(value) {
  const output = text(value).toLowerCase();
  if (!UUID_RE.test(output)) throw new Error('MYTMS_GOOGLE_ROUTE_CONTEXT_INVALID');
  return output;
}

function positiveInteger(value) {
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < 1) {
    throw new Error('MYTMS_GOOGLE_ROUTE_CONTEXT_INVALID');
  }
  return output;
}

function normalize(input) {
  const value = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const environment = text(value.environment).toUpperCase();
  if (!['TEST', 'LIVE'].includes(environment)) {
    throw new Error('MYTMS_GOOGLE_ROUTE_CONTEXT_INVALID');
  }
  return {
    v: 1,
    aud: 'mytms-google-data-plane',
    environment,
    integration_id: uuid(value.integration_id),
    agency_id: uuid(value.agency_id),
    data_plane_id: uuid(value.data_plane_id),
    route_version_id: uuid(value.route_version_id),
    route_version: positiveInteger(value.route_version),
    target_generation: Number.isSafeInteger(Number(value.target_generation))
      && Number(value.target_generation) >= 0 ? Number(value.target_generation)
      : (() => { throw new Error('MYTMS_GOOGLE_ROUTE_CONTEXT_INVALID'); })(),
    operation_id: uuid(value.operation_id),
    issued_at_utc: text(value.issued_at_utc),
    expires_at_utc: text(value.expires_at_utc),
    key_version: positiveInteger(value.key_version || 1)
  };
}

function validateTimes(context, nowMs) {
  const issued = Date.parse(context.issued_at_utc);
  const expires = Date.parse(context.expires_at_utc);
  if (!Number.isFinite(issued) || !Number.isFinite(expires)
      || new Date(issued).toISOString() !== context.issued_at_utc
      || new Date(expires).toISOString() !== context.expires_at_utc
      || expires <= issued || expires - issued > MAX_LIFETIME_MS
      || issued > nowMs + 120_000 || expires <= nowMs) {
    throw new Error('MYTMS_GOOGLE_ROUTE_CONTEXT_EXPIRED');
  }
}

async function key(secret, usage) {
  const material = text(secret);
  if (!material) throw new Error('MYTMS_GOOGLE_ROUTE_CONTEXT_SECRET_UNAVAILABLE');
  return crypto.subtle.importKey(
    'raw', encoder.encode(material), { name: 'HMAC', hash: 'SHA-256' }, false, usage
  );
}

async function sha256Hex(value) {
  return bytesToHex(await crypto.subtle.digest('SHA-256', encoder.encode(String(value))));
}

function canonical(payload) {
  return `cloudtms-mytms-google-route-context-v1\n${payload}`;
}

export async function signMyTmsGoogleRouteContext(input, secret, nowMs = Date.now()) {
  const context = normalize(input);
  validateTimes(context, nowMs);
  const payload = base64UrlEncode(encoder.encode(canonicalJson(context)));
  const signature = new Uint8Array(await crypto.subtle.sign(
    'HMAC', await key(secret, ['sign']), encoder.encode(canonical(payload))
  ));
  const envelope = `v1.${payload}.${base64UrlEncode(signature)}`;
  return { envelope, sha256: await sha256Hex(envelope), context };
}

export async function verifyMyTmsGoogleRouteContext(request, env, nowMs = Date.now()) {
  try {
    const envelope = text(request.headers.get('x-cloudtms-google-route-context'));
    const digest = text(request.headers.get('x-cloudtms-google-route-context-sha256')).toLowerCase();
    if (!envelope || !SHA256_RE.test(digest) || await sha256Hex(envelope) !== digest) return null;
    const parts = envelope.split('.');
    if (parts.length !== 3 || parts[0] !== 'v1') return null;
    const payload = base64UrlDecode(parts[1]);
    const signature = base64UrlDecode(parts[2]);
    if (!payload || !signature || signature.byteLength !== 32) return null;
    const raw = decoder.decode(payload);
    const context = normalize(JSON.parse(raw));
    if (canonicalJson(context) !== raw) return null;
    validateTimes(context, nowMs);
    const keyVersion = Number(env.CANDIDATE_GOOGLE_ROUTE_CONTEXT_KEY_VERSION || 1);
    if (context.key_version !== keyVersion) return null;
    if (!await crypto.subtle.verify(
      'HMAC', await key(env.CANDIDATE_ROUTE_CONTEXT_SECRET, ['verify']),
      signature, encoder.encode(canonical(parts[1]))
    )) return null;
    if (context.environment !== text(env.CANDIDATE_APP_ENVIRONMENT).toUpperCase()
        || context.agency_id !== uuid(env.CANDIDATE_AGENCY_ID)
        || context.data_plane_id !== uuid(env.CANDIDATE_DATA_PLANE_ID)
        || context.route_version !== positiveInteger(env.CANDIDATE_ROUTE_VERSION)) return null;
    return { context, envelope, sha256: digest };
  } catch {
    return null;
  }
}

export const myTmsGoogleRouteContextInternals = Object.freeze({ canonicalJson, normalize, validateTimes });
