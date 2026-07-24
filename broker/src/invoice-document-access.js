const encoder = new TextEncoder();
const decoder = new TextDecoder();

function base64UrlEncodeBytes(bytes) {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function base64UrlDecodeBytes(value) {
  const normalised = String(value || '').replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalised + '='.repeat((4 - (normalised.length % 4)) % 4);
  const binary = atob(padded);
  return Uint8Array.from(binary, character => character.charCodeAt(0));
}

function constantTimeEqual(left, right) {
  const a = encoder.encode(String(left || ''));
  const b = encoder.encode(String(right || ''));
  let different = a.length ^ b.length;
  const length = Math.max(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    different |= (a[index] || 0) ^ (b[index] || 0);
  }
  return different === 0;
}

async function importHmacKey(secret) {
  if (!secret || String(secret).length < 32) {
    throw new Error('INVOICE_DOCUMENT_ACCESS_SECRET_MISSING');
  }
  return crypto.subtle.importKey(
    'raw',
    encoder.encode(String(secret)),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify']
  );
}

function validateTokenClaims(claims) {
  if (!claims || typeof claims !== 'object' || Array.isArray(claims)) {
    throw new Error('INVOICE_DOCUMENT_TOKEN_CLAIMS_INVALID');
  }
  const required = ['sub', 'entity_type', 'entity_id', 'document_version_id', 'purpose'];
  for (const key of required) {
    if (!String(claims[key] || '').trim()) {
      throw new Error(`INVOICE_DOCUMENT_TOKEN_${key.toUpperCase()}_MISSING`);
    }
  }
}

export async function createInvoiceDocumentAccessToken(secret, claims, options = {}) {
  validateTokenClaims(claims);
  const nowSeconds = Math.floor((options.nowMs ?? Date.now()) / 1000);
  const ttlSeconds = Math.max(30, Math.min(3600, Number(options.ttlSeconds || 300)));
  const payload = {
    v: 2,
    aud: 'cloudtms-invoice-document',
    sub: claims.sub,
    entity_type: claims.entity_type,
    entity_id: claims.entity_id,
    document_version_id: claims.document_version_id,
    recipient_set_hash: claims.recipient_set_hash || undefined,
    purpose: claims.purpose,
    filename: claims.filename || undefined,
    iat: nowSeconds,
    exp: nowSeconds + ttlSeconds,
    nonce: claims.nonce || crypto.randomUUID()
  };
  const encodedPayload = base64UrlEncodeBytes(encoder.encode(JSON.stringify(payload)));
  const key = await importHmacKey(secret);
  const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(encodedPayload));
  return `${encodedPayload}.${base64UrlEncodeBytes(new Uint8Array(signature))}`;
}

export async function verifyInvoiceDocumentAccessToken(secret, token, options = {}) {
  const parts = String(token || '').split('.');
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    return { ok: false, code: 'INVOICE_DOCUMENT_TOKEN_MALFORMED' };
  }
  try {
    const key = await importHmacKey(secret);
    const expectedSignature = await crypto.subtle.sign('HMAC', key, encoder.encode(parts[0]));
    const expected = base64UrlEncodeBytes(new Uint8Array(expectedSignature));
    if (!constantTimeEqual(expected, parts[1])) {
      return { ok: false, code: 'INVOICE_DOCUMENT_TOKEN_SIGNATURE_INVALID' };
    }

    const claims = JSON.parse(decoder.decode(base64UrlDecodeBytes(parts[0])));
    validateTokenClaims(claims);
    const nowSeconds = Math.floor((options.nowMs ?? Date.now()) / 1000);
    if (claims.v !== 2 || claims.aud !== 'cloudtms-invoice-document') {
      return { ok: false, code: 'INVOICE_DOCUMENT_TOKEN_AUDIENCE_INVALID' };
    }
    if (!Number.isFinite(Number(claims.exp)) || Number(claims.exp) <= nowSeconds) {
      return { ok: false, code: 'INVOICE_DOCUMENT_TOKEN_EXPIRED' };
    }
    if (Number(claims.iat) > nowSeconds + 60) {
      return { ok: false, code: 'INVOICE_DOCUMENT_TOKEN_NOT_YET_VALID' };
    }
    if (options.expectedPurpose && claims.purpose !== options.expectedPurpose) {
      return { ok: false, code: 'INVOICE_DOCUMENT_TOKEN_PURPOSE_MISMATCH' };
    }
    return { ok: true, claims };
  } catch (error) {
    return {
      ok: false,
      code: error?.message === 'INVOICE_DOCUMENT_ACCESS_SECRET_MISSING'
        ? error.message
        : 'INVOICE_DOCUMENT_TOKEN_INVALID'
    };
  }
}

export function buildInvoiceDocumentDownloadUrl(baseUrl, token) {
  const base = String(baseUrl || '').replace(/\/+$/g, '');
  if (!base) throw new Error('INVOICE_DOCUMENT_DOWNLOAD_BASE_URL_MISSING');
  return `${base}/api/invoice-documents/access?token=${encodeURIComponent(token)}`;
}
