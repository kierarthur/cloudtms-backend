const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const encoder = new TextEncoder();

export function parseInvoiceAsyncAllowedUserIds(value) {
  const raw = String(value || '').split(',').map(item => item.trim()).filter(Boolean);
  const invalid = raw.filter(item => !UUID_RE.test(item));
  return Object.freeze({
    ok: invalid.length === 0,
    ids: Object.freeze([...new Set(raw.map(item => item.toLowerCase()))]),
    invalidCount: invalid.length
  });
}

export function parseInvoiceAsyncAccessMode(value) {
  const mode = String(value || 'COHORT').trim().toUpperCase() || 'COHORT';
  return Object.freeze({
    ok: mode === 'COHORT' || mode === 'AUTHENTICATED',
    mode
  });
}

export function isInvoiceAsyncUserAllowed(env, user) {
  const enabled = String(env?.INVOICE_ASYNC_PIPELINE_ENABLED || '').toLowerCase() === 'true';
  if (!enabled) return { allowed: false, code: 'INVOICE_ASYNC_PIPELINE_DISABLED' };
  const access = parseInvoiceAsyncAccessMode(env?.INVOICE_ASYNC_ACCESS_MODE);
  if (!access.ok) return { allowed: false, code: 'INVOICE_ASYNC_ACCESS_MODE_INVALID' };
  if (!user?.id || user.active === false) return { allowed: false, code: 'INVOICE_ASYNC_USER_INACTIVE' };
  if (access.mode === 'AUTHENTICATED') {
    return { allowed: true, code: 'INVOICE_ASYNC_AUTHENTICATED_USER_ALLOWED' };
  }
  const parsed = parseInvoiceAsyncAllowedUserIds(env?.INVOICE_ASYNC_ALLOWED_USER_IDS);
  if (!parsed.ok) return { allowed: false, code: 'INVOICE_ASYNC_ALLOWLIST_INVALID' };
  const roles = Array.isArray(user.roles) ? user.roles : [user.role, user.user_role, user.user_type];
  if (!roles.some(role => String(role || '').toLowerCase() === 'admin')) {
    return { allowed: false, code: 'INVOICE_ASYNC_ADMIN_REQUIRED' };
  }
  return parsed.ids.includes(String(user.id).toLowerCase())
    ? { allowed: true, code: 'INVOICE_ASYNC_USER_ALLOWED' }
    : { allowed: false, code: 'INVOICE_ASYNC_USER_OUTSIDE_COHORT' };
}

export async function sha256Text(value) {
  const digest = await crypto.subtle.digest('SHA-256', encoder.encode(String(value)));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

function hex(bytes) {
  return [...new Uint8Array(bytes)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

function decodeHex(value) {
  const text = String(value || '');
  if (!/^[0-9a-f]{64}$/i.test(text)) return null;
  return new Uint8Array(text.match(/.{2}/g).map(pair => Number.parseInt(pair, 16)));
}

export function canonicalInvoiceDrainMessage(payload) {
  const cursor = payload.reconciliation_cursor;
  return JSON.stringify({
    version: 'INVOICE_QUEUE_DISPATCH_V1',
    timestamp: Number(payload.timestamp),
    nonce: String(payload.nonce || ''),
    depth: Number(payload.depth),
    lanes: [...new Set((payload.lanes || []).map(value => String(value).toUpperCase()))].sort(),
    priority_class: String(payload.priority_class || 'SCHEDULED').toUpperCase(),
    reconciliation_cursor: cursor ? {
      snapshot_at_utc: String(cursor.snapshot_at_utc || ''),
      updated_at_utc: String(cursor.updated_at_utc || ''),
      operation_id: String(cursor.operation_id || '').toLowerCase()
    } : null
  });
}

export async function signInvoiceDrainRequest(secret, payload) {
  if (!secret) throw Object.assign(new Error('INVOICE_QUEUE_DISPATCH_SECRET_MISSING'), { code: 'INVOICE_QUEUE_DISPATCH_SECRET_MISSING' });
  const key = await crypto.subtle.importKey('raw', encoder.encode(String(secret)), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return hex(await crypto.subtle.sign('HMAC', key, encoder.encode(canonicalInvoiceDrainMessage(payload))));
}

export async function verifyInvoiceDrainSignature(secret, payload, signature) {
  const expected = await signInvoiceDrainRequest(secret, payload);
  const actualBytes = decodeHex(signature);
  const expectedBytes = decodeHex(expected);
  if (!actualBytes || !expectedBytes || actualBytes.byteLength !== expectedBytes.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < actualBytes.byteLength; index += 1) {
    difference |= actualBytes[index] ^ expectedBytes[index];
  }
  return difference === 0;
}

