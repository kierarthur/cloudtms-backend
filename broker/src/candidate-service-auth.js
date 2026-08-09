const encoder = new TextEncoder();
const MAX_CLOCK_SKEW_SECONDS = 120;

function requiredText(value, code) {
  const output = String(value == null ? '' : value).trim();
  if (!output) throw new Error(code);
  return output;
}

function bytesToHex(bytes) {
  return Array.from(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes))
    .map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

async function sha256Hex(value) {
  const bytes = value instanceof ArrayBuffer
    ? new Uint8Array(value)
    : value instanceof Uint8Array
      ? value
      : encoder.encode(String(value == null ? '' : value));
  return bytesToHex(await crypto.subtle.digest('SHA-256', bytes));
}

async function importHmacKey(secret, usage) {
  return crypto.subtle.importKey(
    'raw',
    encoder.encode(requiredText(secret, 'CANDIDATE_PRIVATE_SERVICE_SECRET_UNAVAILABLE')),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    usage
  );
}

function canonicalServiceRequest({ method, requestTarget, timestamp, nonce, environment, bodySha256, authorizationSha256, headersSha256 }) {
  return [
    'cloudtms-candidate-private-v1',
    String(method || '').toUpperCase(),
    requestTarget,
    timestamp,
    nonce,
    environment,
    bodySha256,
    authorizationSha256,
    headersSha256
  ].join('\n');
}

async function signedHeadersSha256(request) {
  return sha256Hex([
    request.headers.get('content-type') || '',
    request.headers.get('idempotency-key') || '',
    request.headers.get('x-request-id') || '',
    request.headers.get('x-cloudtms-public-client') || ''
  ].join('\n'));
}

async function requestBodySha256(request) {
  if (request.method === 'GET' || request.method === 'HEAD') return sha256Hex(new Uint8Array());
  return sha256Hex(await request.clone().arrayBuffer());
}

export async function signCandidatePrivateRequest(request, env) {
  const environment = requiredText(env.CANDIDATE_APP_ENVIRONMENT, 'CANDIDATE_ENVIRONMENT_REQUIRED').toUpperCase();
  if (!['TEST', 'LIVE'].includes(environment)) throw new Error('CANDIDATE_ENVIRONMENT_INVALID');
  const timestamp = String(Math.floor(Date.now() / 1000));
  const nonce = crypto.randomUUID();
  const bodySha256 = await requestBodySha256(request);
  const authorizationSha256 = await sha256Hex(request.headers.get('authorization') || '');
  const headersSha256 = await signedHeadersSha256(request);
  const url = new URL(request.url);
  const canonical = canonicalServiceRequest({
    method: request.method,
    requestTarget: `${url.pathname}${url.search}`,
    timestamp,
    nonce,
    environment,
    bodySha256,
    authorizationSha256,
    headersSha256
  });
  const signature = bytesToHex(await crypto.subtle.sign(
    'HMAC',
    await importHmacKey(env.CANDIDATE_PRIVATE_SERVICE_SECRET, ['sign']),
    encoder.encode(canonical)
  ));
  const headers = new Headers(request.headers);
  headers.set('x-cloudtms-service-version', 'candidate-private-v1');
  headers.set('x-cloudtms-service-environment', environment);
  headers.set('x-cloudtms-service-timestamp', timestamp);
  headers.set('x-cloudtms-service-nonce', nonce);
  headers.set('x-cloudtms-service-body-sha256', bodySha256);
  headers.set('x-cloudtms-service-authorization-sha256', authorizationSha256);
  headers.set('x-cloudtms-service-headers-sha256', headersSha256);
  headers.set('x-cloudtms-service-signature', signature);
  return new Request(request, { headers });
}

function hexToBytes(value) {
  const input = String(value == null ? '' : value).trim().toLowerCase();
  if (!/^[0-9a-f]+$/.test(input) || input.length % 2 !== 0) return null;
  const output = new Uint8Array(input.length / 2);
  for (let index = 0; index < output.length; index += 1) {
    output[index] = Number.parseInt(input.slice(index * 2, index * 2 + 2), 16);
  }
  return output;
}

export async function verifyCandidatePrivateRequest(request, env, nowSeconds = Math.floor(Date.now() / 1000)) {
  try {
    if (request.headers.get('x-cloudtms-service-version') !== 'candidate-private-v1') return false;
    const environment = requiredText(env.CANDIDATE_APP_ENVIRONMENT, 'CANDIDATE_ENVIRONMENT_REQUIRED').toUpperCase();
    if (!['TEST', 'LIVE'].includes(environment)) return false;
    if (request.headers.get('x-cloudtms-service-environment') !== environment) return false;
    const timestamp = request.headers.get('x-cloudtms-service-timestamp') || '';
    const timestampNumber = Number(timestamp);
    if (!Number.isSafeInteger(timestampNumber) || Math.abs(nowSeconds - timestampNumber) > MAX_CLOCK_SKEW_SECONDS) return false;
    const nonce = request.headers.get('x-cloudtms-service-nonce') || '';
    if (!/^[0-9a-f-]{36}$/i.test(nonce)) return false;
    const suppliedBodySha256 = request.headers.get('x-cloudtms-service-body-sha256') || '';
    const actualBodySha256 = await requestBodySha256(request);
    if (suppliedBodySha256 !== actualBodySha256) return false;
    const suppliedAuthorizationSha256 = request.headers.get('x-cloudtms-service-authorization-sha256') || '';
    const actualAuthorizationSha256 = await sha256Hex(request.headers.get('authorization') || '');
    if (suppliedAuthorizationSha256 !== actualAuthorizationSha256) return false;
    const suppliedHeadersSha256 = request.headers.get('x-cloudtms-service-headers-sha256') || '';
    const actualHeadersSha256 = await signedHeadersSha256(request);
    if (suppliedHeadersSha256 !== actualHeadersSha256) return false;
    const signature = hexToBytes(request.headers.get('x-cloudtms-service-signature'));
    if (!signature) return false;
    const url = new URL(request.url);
    const canonical = canonicalServiceRequest({
      method: request.method,
      requestTarget: `${url.pathname}${url.search}`,
      timestamp,
      nonce,
      environment,
      bodySha256: actualBodySha256,
      authorizationSha256: actualAuthorizationSha256,
      headersSha256: actualHeadersSha256
    });
    return crypto.subtle.verify(
      'HMAC',
      await importHmacKey(env.CANDIDATE_PRIVATE_SERVICE_SECRET, ['verify']),
      signature,
      encoder.encode(canonical)
    );
  } catch {
    return false;
  }
}

export const candidateServiceAuthInternals = Object.freeze({
  canonicalServiceRequest,
  requestBodySha256,
  signedHeadersSha256,
  sha256Hex
});
