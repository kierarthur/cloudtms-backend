const textEncoder = new TextEncoder();
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/i;
const PAGE_KEY_DIGEST_PATTERN = /^[0-9a-f]{16}$/i;
const TSQ2_KINDS = new Set(['T', 'S', 'M', 'E']);
const TSQ2_CATEGORIES = new Set(['', 'A', 'M', 'O', 'T']);

function base64Url(bytes) {
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function fromBase64Url(value) {
  const source = String(value || '');
  if (!/^[A-Za-z0-9_-]+$/.test(source)) throw new Error('TSQ1_FORMAT_INVALID');
  const padded = source.replace(/-/g, '+').replace(/_/g, '/')
    + '='.repeat((4 - (source.length % 4)) % 4);
  let binary;
  try { binary = atob(padded); } catch { throw new Error('TSQ1_FORMAT_INVALID'); }
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

export function buildTsq1Payload({ qr_token: qrToken }) {
  const token = String(qrToken || '').trim();
  if (!token) {
    throw Object.assign(new Error('TSQ1_QR_TOKEN_REQUIRED'), {
      code: 'TSQ1_QR_TOKEN_REQUIRED'
    });
  }
  return Object.freeze({
    v: 1,
    tok: token
  });
}

export async function signTsq1(payloadBase64Url, env = {}) {
  const secret = String(env.QR_SIGNING_SECRET || '').trim();
  if (!secret) {
    throw Object.assign(new Error('TSQ1_SIGNING_SECRET_MISSING'), {
      code: 'TSQ1_SIGNING_SECRET_MISSING'
    });
  }
  const key = await crypto.subtle.importKey(
    'raw',
    textEncoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    textEncoder.encode(`TSQ1.${String(payloadBase64Url)}`)
  );
  return base64Url(new Uint8Array(signature));
}

export async function buildTsq1String(payload, env = {}) {
  const normalised = payload && typeof payload === 'object'
    ? payload
    : buildTsq1Payload({ qr_token: payload });
  if (!String(normalised.tok || '').trim()) {
    throw Object.assign(new Error('TSQ1_QR_TOKEN_REQUIRED'), {
      code: 'TSQ1_QR_TOKEN_REQUIRED'
    });
  }
  const payloadBase64Url = base64Url(textEncoder.encode(JSON.stringify(normalised)));
  const signatureBase64Url = await signTsq1(payloadBase64Url, env);
  return `TSQ1.${payloadBase64Url}.${signatureBase64Url}`;
}

export async function verifyTsq1String(value, env = {}) {
  const raw = String(value || '').trim();
  if (raw.length < 20 || raw.length > 4096) {
    throw Object.assign(new Error('TSQ1_FORMAT_INVALID'), { code: 'TSQ1_FORMAT_INVALID' });
  }
  const parts = raw.split('.');
  if (parts.length !== 3 || parts[0] !== 'TSQ1') {
    throw Object.assign(new Error('TSQ1_FORMAT_INVALID'), { code: 'TSQ1_FORMAT_INVALID' });
  }
  const secret = String(env.QR_SIGNING_SECRET || '').trim();
  if (!secret) {
    throw Object.assign(new Error('TSQ1_SIGNING_SECRET_MISSING'), {
      code: 'TSQ1_SIGNING_SECRET_MISSING'
    });
  }
  const key = await crypto.subtle.importKey(
    'raw', textEncoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']
  );
  const valid = await crypto.subtle.verify(
    'HMAC', key, fromBase64Url(parts[2]), textEncoder.encode(`TSQ1.${parts[1]}`)
  );
  if (!valid) {
    throw Object.assign(new Error('TSQ1_SIGNATURE_INVALID'), { code: 'TSQ1_SIGNATURE_INVALID' });
  }
  let payload;
  try { payload = JSON.parse(new TextDecoder().decode(fromBase64Url(parts[1]))); }
  catch { throw Object.assign(new Error('TSQ1_PAYLOAD_INVALID'), { code: 'TSQ1_PAYLOAD_INVALID' }); }
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)
      || Object.keys(payload).sort().join(',') !== 'tok,v'
      || payload.v !== 1 || typeof payload.tok !== 'string'
      || payload.tok.trim() !== payload.tok || payload.tok.length < 16 || payload.tok.length > 512) {
    throw Object.assign(new Error('TSQ1_PAYLOAD_INVALID'), { code: 'TSQ1_PAYLOAD_INVALID' });
  }
  return Object.freeze({ v: 1, tok: payload.tok });
}

async function sha256Hex(value) {
  const digest = await crypto.subtle.digest('SHA-256', textEncoder.encode(String(value || '')));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function tsq2Error(code) {
  return Object.assign(new Error(code), { code });
}

function normaliseTsq2Payload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)
      || Object.keys(payload).sort().join(',') !== 'c,g,k,m,n,o,p,t,v,w'
      || payload.v !== 2
      || !UUID_PATTERN.test(String(payload.w || ''))
      || !UUID_PATTERN.test(String(payload.t || ''))
      || !Number.isSafeInteger(payload.g) || payload.g < 1
      || !SHA256_PATTERN.test(String(payload.m || ''))
      || !Number.isSafeInteger(payload.o) || payload.o < 1 || payload.o > 100
      || !PAGE_KEY_DIGEST_PATTERN.test(String(payload.p || ''))
      || !TSQ2_KINDS.has(String(payload.k || ''))
      || !TSQ2_CATEGORIES.has(String(payload.c == null ? '' : payload.c))
      || !Number.isSafeInteger(payload.n) || payload.n < 1 || payload.n > 100) {
    throw tsq2Error('TSQ2_PAYLOAD_INVALID');
  }
  return Object.freeze({
    v: 2,
    w: String(payload.w).toLowerCase(),
    t: String(payload.t).toLowerCase(),
    g: payload.g,
    m: String(payload.m).toLowerCase(),
    o: payload.o,
    p: String(payload.p).toLowerCase(),
    k: String(payload.k),
    c: String(payload.c == null ? '' : payload.c),
    n: payload.n
  });
}

export async function buildTsq2PagePayload({
  workflow_id: workflowId,
  timesheet_id: timesheetId,
  workflow_generation: generation,
  paper_return_manifest_sha256: manifestSha256,
  ordinal,
  page_key: pageKey,
  page_kind: pageKind,
  category_code: categoryCode = '',
  category_occurrence: categoryOccurrence = 1
} = {}) {
  const normalisedPageKey = String(pageKey || '').trim();
  if (!normalisedPageKey) throw tsq2Error('TSQ2_PAGE_KEY_REQUIRED');
  return normaliseTsq2Payload({
    v: 2,
    w: String(workflowId || '').toLowerCase(),
    t: String(timesheetId || '').toLowerCase(),
    g: Number(generation),
    m: String(manifestSha256 || '').toLowerCase(),
    o: Number(ordinal),
    p: (await sha256Hex(normalisedPageKey)).slice(0, 16),
    k: String(pageKind || ''),
    c: String(categoryCode || ''),
    n: Number(categoryOccurrence)
  });
}

export async function buildTsq2String(payload, env = {}) {
  const normalised = normaliseTsq2Payload(payload);
  const secret = String(env.QR_SIGNING_SECRET || '').trim();
  if (!secret) throw tsq2Error('TSQ2_SIGNING_SECRET_MISSING');
  const payloadBase64Url = base64Url(textEncoder.encode(JSON.stringify(normalised)));
  const key = await crypto.subtle.importKey(
    'raw', textEncoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign(
    'HMAC', key, textEncoder.encode(`TSQ2.${payloadBase64Url}`)
  );
  return `TSQ2.${payloadBase64Url}.${base64Url(new Uint8Array(signature))}`;
}

export async function verifyTsq2String(value, env = {}) {
  const raw = String(value || '').trim();
  if (raw.length < 40 || raw.length > 4096) throw tsq2Error('TSQ2_FORMAT_INVALID');
  const parts = raw.split('.');
  if (parts.length !== 3 || parts[0] !== 'TSQ2') throw tsq2Error('TSQ2_FORMAT_INVALID');
  const secret = String(env.QR_SIGNING_SECRET || '').trim();
  if (!secret) throw tsq2Error('TSQ2_SIGNING_SECRET_MISSING');
  const key = await crypto.subtle.importKey(
    'raw', textEncoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']
  );
  const valid = await crypto.subtle.verify(
    'HMAC', key, fromBase64Url(parts[2]), textEncoder.encode(`TSQ2.${parts[1]}`)
  );
  if (!valid) throw tsq2Error('TSQ2_SIGNATURE_INVALID');
  let payload;
  try { payload = JSON.parse(new TextDecoder().decode(fromBase64Url(parts[1]))); }
  catch { throw tsq2Error('TSQ2_PAYLOAD_INVALID'); }
  return normaliseTsq2Payload(payload);
}
