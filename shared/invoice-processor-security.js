const encoder = new TextEncoder();

function bytesToHex(bytes) {
  return [...new Uint8Array(bytes)]
    .map(byte => byte.toString(16).padStart(2, '0'))
    .join('');
}

function hexToBytes(value) {
  const text = String(value || '');
  if (!/^[0-9a-f]{64}$/i.test(text)) return null;
  return new Uint8Array(text.match(/.{2}/g).map(pair => Number.parseInt(pair, 16)));
}

function constantTimeHexEqual(left, right) {
  const a = hexToBytes(left);
  const b = hexToBytes(right);
  if (!a || !b || a.byteLength !== b.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < a.byteLength; index += 1) {
    difference |= a[index] ^ b[index];
  }
  return difference === 0;
}

export async function invoiceProcessorSha256Hex(value) {
  const bytes = value instanceof Uint8Array
    ? value
    : encoder.encode(String(value ?? ''));
  return bytesToHex(await crypto.subtle.digest('SHA-256', bytes));
}

export function canonicalInvoiceProcessorRequest(fields) {
  return [
    'INVOICE_DOCUMENT_PROCESSOR_REQUEST_V1',
    String(fields.method || 'POST').toUpperCase(),
    String(fields.path || ''),
    String(fields.timestamp || ''),
    String(fields.nonce || ''),
    String(fields.chunk_id || ''),
    String(fields.fence_token ?? ''),
    String(fields.action || '').toUpperCase(),
    String(fields.plan_generation ?? ''),
    String(fields.processor_policy_version || ''),
    String(fields.body_sha256 || '').toLowerCase()
  ].join('\n');
}

async function importProcessorHmacKey(secret, usage) {
  if (!secret || String(secret).length < 32) {
    throw Object.assign(
      new Error('INVOICE_DOCUMENT_PROCESSOR_SECRET_MISSING'),
      { code: 'INVOICE_DOCUMENT_PROCESSOR_SECRET_MISSING' }
    );
  }
  return crypto.subtle.importKey(
    'raw',
    encoder.encode(String(secret)),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    usage
  );
}

export async function signInvoiceProcessorRequest(secret, fields) {
  const key = await importProcessorHmacKey(secret, ['sign']);
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    encoder.encode(canonicalInvoiceProcessorRequest(fields))
  );
  return bytesToHex(signature);
}

export async function verifyInvoiceProcessorRequest(secret, fields, signature) {
  const expected = await signInvoiceProcessorRequest(secret, fields);
  return constantTimeHexEqual(expected, signature);
}

export function invoiceProcessorIdentityHeaders(fields, signature) {
  return {
    'x-cloudtms-internal-service': 'invoice-document-v1',
    'x-cloudtms-timestamp': String(fields.timestamp),
    'x-cloudtms-nonce': String(fields.nonce),
    'x-cloudtms-chunk-id': String(fields.chunk_id || ''),
    'x-cloudtms-fence-token': String(fields.fence_token ?? ''),
    'x-cloudtms-action': String(fields.action || '').toUpperCase(),
    'x-cloudtms-plan-generation': String(fields.plan_generation ?? ''),
    'x-cloudtms-processor-policy-version': String(fields.processor_policy_version || ''),
    'x-cloudtms-body-sha256': String(fields.body_sha256 || '').toLowerCase(),
    'x-cloudtms-signature': String(signature || '').toLowerCase()
  };
}

export function invoiceProcessorFieldsFromHeaders(request, bodySha256) {
  const url = new URL(request.url);
  return {
    method: request.method,
    path: url.pathname,
    timestamp: request.headers.get('x-cloudtms-timestamp') || '',
    nonce: request.headers.get('x-cloudtms-nonce') || '',
    chunk_id: request.headers.get('x-cloudtms-chunk-id') || '',
    fence_token: request.headers.get('x-cloudtms-fence-token') || '',
    action: request.headers.get('x-cloudtms-action') || '',
    plan_generation: request.headers.get('x-cloudtms-plan-generation') || '',
    processor_policy_version:
      request.headers.get('x-cloudtms-processor-policy-version') || '',
    body_sha256: bodySha256
  };
}

export function validateInvoiceProcessorRequestFields(fields, options = {}) {
  const maximumAgeMs = Number(options.maximumAgeMs || 60_000);
  const timestamp = Number(fields.timestamp);
  if (!Number.isFinite(timestamp) || Math.abs(Date.now() - timestamp) > maximumAgeMs) {
    return { ok: false, code: 'PROCESSOR_REQUEST_TIMESTAMP_INVALID' };
  }
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  if (!uuidPattern.test(String(fields.nonce || ''))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_NONCE_INVALID' };
  }
  if (!/^[0-9a-f]{64}$/i.test(String(fields.body_sha256 || ''))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_BODY_HASH_INVALID' };
  }
  if (fields.method !== 'POST' || !['/process','/ready'].includes(String(fields.path || ''))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_PATH_INVALID' };
  }
  if (String(fields.processor_policy_version || '') !== 'INVOICE_PROCESSOR_LIMITS_V4') {
    return { ok: false, code: 'PROCESSOR_REQUEST_POLICY_INVALID' };
  }
  if (fields.path === '/ready') {
    if (
      fields.action !== 'READY'
      || String(fields.chunk_id || '') !== ''
      || String(fields.fence_token || '') !== ''
      || String(fields.plan_generation || '') !== ''
    ) return { ok: false, code: 'PROCESSOR_READY_IDENTITY_INVALID' };
    return { ok: true };
  }
  if (!uuidPattern.test(String(fields.chunk_id || ''))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_CHUNK_ID_INVALID' };
  }
  if (!/^[1-9][0-9]*$/.test(String(fields.fence_token || ''))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_FENCE_INVALID' };
  }
  if (!/^(0|[1-9][0-9]*)$/.test(String(fields.plan_generation || ''))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_PLAN_GENERATION_INVALID' };
  }
  if (!['ASSET_INSPECT','ASSET_NORMALISE','PDF_MERGE','DOCUMENT_VERIFY'].includes(String(fields.action || ''))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_ACTION_INVALID' };
  }
  return { ok: true };
}
