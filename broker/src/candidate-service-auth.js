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

function canonicalServiceRequest({ version = 'candidate-private-v1', method, requestTarget, timestamp, nonce, environment, bodySha256, authorizationSha256, headersSha256 }) {
  return [
    `cloudtms-${version}`,
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

async function signedHeadersSha256(request, version = 'candidate-private-v1') {
  const values = [
    request.headers.get('content-type') || '',
    request.headers.get('idempotency-key') || '',
    request.headers.get('x-request-id') || '',
    request.headers.get('x-cloudtms-public-client') || ''
  ];
  if (version === 'candidate-private-v2') {
    values.push(
      request.headers.get('x-cloudtms-route-context') || '',
      request.headers.get('x-cloudtms-route-context-sha256') || ''
    );
  } else if (version === 'candidate-private-v3') {
    values.push(
      request.headers.get('x-cloudtms-google-route-context') || '',
      request.headers.get('x-cloudtms-google-route-context-sha256') || ''
    );
  }
  return sha256Hex(values.join('\n'));
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
  const hasRouteContext = request.headers.has('x-cloudtms-route-context');
  const hasRouteContextDigest = request.headers.has('x-cloudtms-route-context-sha256');
  const hasGoogleRouteContext = request.headers.has('x-cloudtms-google-route-context');
  const hasGoogleRouteContextDigest = request.headers.has('x-cloudtms-google-route-context-sha256');
  if (hasRouteContext !== hasRouteContextDigest) {
    throw new Error('CANDIDATE_ROUTE_CONTEXT_HEADERS_INCOMPLETE');
  }
  if (hasGoogleRouteContext !== hasGoogleRouteContextDigest || (hasRouteContext && hasGoogleRouteContext)) {
    throw new Error('CANDIDATE_ROUTE_CONTEXT_HEADERS_INCOMPLETE');
  }
  const version = hasGoogleRouteContext ? 'candidate-private-v3'
    : hasRouteContext ? 'candidate-private-v2' : 'candidate-private-v1';
  const headersSha256 = await signedHeadersSha256(request, version);
  const url = new URL(request.url);
  const canonical = canonicalServiceRequest({
    version,
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
  headers.set('x-cloudtms-service-version', version);
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
    const version = request.headers.get('x-cloudtms-service-version');
    if (!['candidate-private-v1', 'candidate-private-v2', 'candidate-private-v3'].includes(version)) return false;
    const hasRouteContext = request.headers.has('x-cloudtms-route-context');
    const hasRouteContextDigest = request.headers.has('x-cloudtms-route-context-sha256');
    const hasGoogleRouteContext = request.headers.has('x-cloudtms-google-route-context');
    const hasGoogleRouteContextDigest = request.headers.has('x-cloudtms-google-route-context-sha256');
    if (hasRouteContext !== hasRouteContextDigest) return false;
    if (hasGoogleRouteContext !== hasGoogleRouteContextDigest || (hasRouteContext && hasGoogleRouteContext)) return false;
    if ((version === 'candidate-private-v2') !== hasRouteContext
        || (version === 'candidate-private-v3') !== hasGoogleRouteContext) return false;
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
    const actualHeadersSha256 = await signedHeadersSha256(request, version);
    if (suppliedHeadersSha256 !== actualHeadersSha256) return false;
    const signature = hexToBytes(request.headers.get('x-cloudtms-service-signature'));
    if (!signature) return false;
    const url = new URL(request.url);
    const canonical = canonicalServiceRequest({
      version,
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
