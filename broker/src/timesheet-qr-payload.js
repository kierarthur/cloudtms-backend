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
