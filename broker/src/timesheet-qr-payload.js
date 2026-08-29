const textEncoder = new TextEncoder();

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
