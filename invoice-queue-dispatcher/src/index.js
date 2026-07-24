import { DurableObject } from 'cloudflare:workers';

const encoder = new TextEncoder();
const ALLOWED_LANES = new Set([
  'ALL','DATABASE','DOCUMENT','GENERATION_GROUP','DOCUMENT_PLAN','ISSUE_INVOICE','DELIVERY_PREPARE','RECONCILE',
  'ASSET_INSPECT','ASSET_NORMALISE','SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY'
]);

function json(value, status = 200) {
  return new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' } });
}

function canonicalMessage(payload) {
  const cursor = payload.reconciliation_cursor;
  return JSON.stringify({
    version: 'INVOICE_QUEUE_DISPATCH_V1', timestamp: Number(payload.timestamp), nonce: String(payload.nonce || ''),
    depth: Number(payload.depth), lanes: [...new Set((payload.lanes || []).map(value => String(value).toUpperCase()))].sort(),
    priority_class: String(payload.priority_class || 'SCHEDULED').toUpperCase(),
    reconciliation_cursor: cursor ? {
      snapshot_at_utc: String(cursor.snapshot_at_utc || ''),
      updated_at_utc: String(cursor.updated_at_utc || ''),
      operation_id: String(cursor.operation_id || '').toLowerCase()
    } : null
  });
}

function hex(bytes) {
  return [...new Uint8Array(bytes)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

function decodeHex(value) {
  const text = String(value || '');
  if (!/^[0-9a-f]{64}$/i.test(text)) return null;
  return new Uint8Array(text.match(/.{2}/g).map(pair => Number.parseInt(pair, 16)));
}

async function sha256(value) {
  return hex(await crypto.subtle.digest('SHA-256', encoder.encode(String(value))));
}

async function sign(secret, payload) {
  if (!secret) throw new Error('INVOICE_QUEUE_DISPATCH_SECRET_MISSING');
  const key = await crypto.subtle.importKey('raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return hex(await crypto.subtle.sign('HMAC', key, encoder.encode(canonicalMessage(payload))));
}

async function verify(secret, payload, signature) {
  const expected = decodeHex(await sign(secret, payload));
  const actual = decodeHex(signature);
  if (!expected || !actual || expected.byteLength !== actual.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < expected.byteLength; index += 1) difference |= expected[index] ^ actual[index];
  return difference === 0;
}

function validatePayload(body) {
  const timestamp = Number(body?.timestamp);
  const depth = Number(body?.depth);
  const lanes = [...new Set((Array.isArray(body?.lanes) ? body.lanes : []).map(value => String(value || '').toUpperCase()))].sort();
  if (!Number.isFinite(timestamp) || Math.abs(Date.now() - timestamp) > 60000) return { ok: false, code: 'INVOICE_DISPATCH_TIMESTAMP_INVALID' };
  if (!Number.isInteger(depth) || depth < 0 || depth > 4) return { ok: false, code: 'INVOICE_DISPATCH_DEPTH_INVALID' };
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(body?.nonce || ''))) {
    return { ok: false, code: 'INVOICE_DISPATCH_NONCE_INVALID' };
  }
  if (!lanes.length || lanes.some(lane => !ALLOWED_LANES.has(lane))) return { ok: false, code: 'INVOICE_DISPATCH_LANES_INVALID' };
  let reconciliationCursor = null;
  if (body?.reconciliation_cursor != null) {
    const cursor = body.reconciliation_cursor;
    if (!lanes.includes('RECONCILE')) {
      return { ok: false, code: 'INVOICE_RECONCILIATION_CURSOR_LANE_INVALID' };
    }
    if (
      !Number.isFinite(Date.parse(cursor?.snapshot_at_utc || ''))
      || !Number.isFinite(Date.parse(cursor?.updated_at_utc || ''))
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(cursor?.operation_id || ''))
    ) return { ok: false, code: 'INVOICE_DISPATCH_RECONCILIATION_CURSOR_INVALID' };
    reconciliationCursor = {
      snapshot_at_utc: new Date(cursor.snapshot_at_utc).toISOString(),
      updated_at_utc: new Date(cursor.updated_at_utc).toISOString(),
      operation_id: String(cursor.operation_id).toLowerCase()
    };
  }
  return {
    ok: true,
    payload: {
      timestamp,
      depth,
      lanes,
      nonce: body.nonce,
      priority_class: String(body.priority_class || 'SCHEDULED').toUpperCase(),
      reconciliation_cursor: reconciliationCursor
    }
  };
}

export class InvoiceDispatchNonceGate extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS consumed_nonces (nonce_hash TEXT PRIMARY KEY, expires_at INTEGER NOT NULL)');
  }

  consumeNonce(nonceHash, expiresAt) {
    const now = Date.now();
    this.ctx.storage.sql.exec('DELETE FROM consumed_nonces WHERE expires_at < ?', now);
    const existing = [...this.ctx.storage.sql.exec('SELECT nonce_hash FROM consumed_nonces WHERE nonce_hash = ? LIMIT 1', nonceHash)];
    if (existing.length) return false;
    this.ctx.storage.sql.exec('INSERT INTO consumed_nonces (nonce_hash, expires_at) VALUES (?, ?)', nonceHash, expiresAt);
    return true;
  }

  async fetch(request) {
    if (request.method !== 'POST') return json({ ok: false, code: 'METHOD_NOT_ALLOWED' }, 405);
    const body = await request.json().catch(() => null);
    if (!body || !/^[0-9a-f]{64}$/i.test(String(body.nonce_hash || ''))) return json({ ok: false, code: 'NONCE_GATE_REQUEST_INVALID' }, 400);
    const expiresAt = Number(body.expires_at);
    if (!Number.isFinite(expiresAt) || expiresAt < Date.now() || expiresAt > Date.now() + 120000) return json({ ok: false, code: 'NONCE_GATE_EXPIRY_INVALID' }, 400);
    return this.consumeNonce(String(body.nonce_hash).toLowerCase(), expiresAt)
      ? json({ ok: true, accepted: true })
      : json({ ok: false, accepted: false, code: 'INVOICE_DISPATCH_REPLAY' }, 409);
  }
}

export default {
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    if (request.headers.get('x-cloudtms-internal-service') !== 'invoice-queue-main-v1') return json({ ok: false, code: 'INVOICE_DISPATCH_CALLER_INVALID' }, 403);
    if (request.method === 'GET' && path === '/ready') {
      if (request.headers.get('x-cloudtms-internal-service') !== 'invoice-queue-main-v1') {
        return json({ ok: false, code: 'DISPATCHER_CALLER_INVALID' }, 403);
      }
      return json(
        {
          ok: !!env.INVOICE_DISPATCH_NONCE_GATE && !!env.INVOICE_QUEUE_MAIN,
          nonce_gate: !!env.INVOICE_DISPATCH_NONCE_GATE,
          main_binding: !!env.INVOICE_QUEUE_MAIN
        },
        env.INVOICE_DISPATCH_NONCE_GATE && env.INVOICE_QUEUE_MAIN ? 200 : 503
      );
    }
    if (request.method !== 'POST' || path !== '/dispatch') return json({ ok: false, code: 'NOT_FOUND' }, 404);
    if (Number(request.headers.get('content-length') || 0) > 8192) return json({ ok: false, code: 'INVOICE_DISPATCH_REQUEST_TOO_LARGE' }, 413);
    const body = await request.json().catch(() => null);
    const validated = validatePayload(body);
    if (!validated.ok) return json({ ok: false, code: validated.code }, 400);
    if (!(await verify(env.INVOICE_QUEUE_DISPATCH_SECRET, validated.payload, body.signature))) return json({ ok: false, code: 'INVOICE_DISPATCH_SIGNATURE_INVALID' }, 403);
    const nonceHash = await sha256(validated.payload.nonce);
    const gate = env.INVOICE_DISPATCH_NONCE_GATE.getByName(`invoice-dispatch-nonce-gate:${env.ENVIRONMENT || 'test'}`);
    const gateResponse = await gate.fetch('https://nonce-gate.internal/consume', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ nonce_hash: nonceHash, expires_at: validated.payload.timestamp + 60000 })
    });
    if (!gateResponse.ok) return json({ ok: false, code: gateResponse.status === 409 ? 'INVOICE_DISPATCH_REPLAY' : 'INVOICE_DISPATCH_NONCE_GATE_FAILED' }, gateResponse.status === 409 ? 409 : 503);
    const signature = await sign(env.INVOICE_QUEUE_DISPATCH_SECRET, validated.payload);
    ctx.waitUntil(env.INVOICE_QUEUE_MAIN.fetch('https://invoice-queue-main.internal/internal/invoice-queue/drain', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-cloudtms-internal-service': 'invoice-queue-dispatcher-v1' },
      body: JSON.stringify({ ...validated.payload, signature })
    }).then(response => {
      if (!response.ok) console.warn(JSON.stringify({ event: 'invoice_dispatch_main_rejected', status: response.status, depth: validated.payload.depth }));
    }).catch(error => console.warn(JSON.stringify({ event: 'invoice_dispatch_main_failed', code: String(error?.message || 'DISPATCH_FAILED').slice(0, 120), depth: validated.payload.depth }))));
    return json({ ok: true, accepted: true, depth: validated.payload.depth }, 202);
  }
};
