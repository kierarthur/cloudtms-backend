const encoder = new TextEncoder();
const MAX_SKEW_SECONDS = 120;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function bytesToHex(value) {
  return Array.from(value instanceof Uint8Array ? value : new Uint8Array(value),
    (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function hexToBytes(value) {
  const source = text(value).toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(source)) return null;
  const bytes = new Uint8Array(32);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(source.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

async function digest(value) {
  const bytes = value instanceof ArrayBuffer ? new Uint8Array(value)
    : value instanceof Uint8Array ? value : encoder.encode(String(value ?? ''));
  return bytesToHex(await crypto.subtle.digest('SHA-256', bytes));
}

async function key(env, usage) {
  const secret = text(env.WEEKLY_SOURCE_DELIVERY_SERVICE_SECRET);
  if (secret.length < 32) throw new Error('WEEKLY_SOURCE_DELIVERY_SERVICE_SECRET_UNAVAILABLE');
  return crypto.subtle.importKey(
    'raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, usage
  );
}

function canonical({ method, target, timestamp, nonce, environment, bodyHash }) {
  return [
    'cloudtms-weekly-source-delivery-v1',String(method).toUpperCase(),target,
    timestamp,nonce,environment,bodyHash
  ].join('\n');
}

export async function signWeeklySourceDeliveryRequest(request, env) {
  const environment = text(env.CANDIDATE_APP_ENVIRONMENT || env.WEEKLY_SOURCE_ENVIRONMENT).toUpperCase();
  if (!['TEST','LIVE'].includes(environment)) throw new Error('WEEKLY_SOURCE_ENVIRONMENT_INVALID');
  const timestamp = String(Math.floor(Date.now()/1000));
  const nonce = crypto.randomUUID();
  const bodyHash = await digest(await request.clone().arrayBuffer());
  const url = new URL(request.url);
  const value = canonical({
    method: request.method,target:`${url.pathname}${url.search}`,
    timestamp,nonce,environment,bodyHash
  });
  const signature = bytesToHex(await crypto.subtle.sign(
    'HMAC',await key(env,['sign']),encoder.encode(value)
  ));
  const headers = new Headers(request.headers);
  headers.set('x-cloudtms-weekly-delivery-version','weekly-source-delivery-v1');
  headers.set('x-cloudtms-weekly-delivery-environment',environment);
  headers.set('x-cloudtms-weekly-delivery-timestamp',timestamp);
  headers.set('x-cloudtms-weekly-delivery-nonce',nonce);
  headers.set('x-cloudtms-weekly-delivery-body-sha256',bodyHash);
  headers.set('x-cloudtms-weekly-delivery-signature',signature);
  return new Request(request,{headers});
}

export async function verifyWeeklySourceDeliveryRequest(
  request,env,nowSeconds=Math.floor(Date.now()/1000)
) {
  try {
    if (request.headers.get('x-cloudtms-weekly-delivery-version')!=='weekly-source-delivery-v1') return false;
    const environment = text(env.CANDIDATE_APP_ENVIRONMENT || env.WEEKLY_SOURCE_ENVIRONMENT).toUpperCase();
    if (!['TEST','LIVE'].includes(environment)
        || request.headers.get('x-cloudtms-weekly-delivery-environment')!==environment) return false;
    const timestamp = request.headers.get('x-cloudtms-weekly-delivery-timestamp') || '';
    const instant = Number(timestamp);
    const nonce = request.headers.get('x-cloudtms-weekly-delivery-nonce') || '';
    if (!Number.isSafeInteger(instant) || Math.abs(nowSeconds-instant)>MAX_SKEW_SECONDS
        || !/^[0-9a-f-]{36}$/i.test(nonce)) return false;
    const bodyHash = await digest(await request.clone().arrayBuffer());
    if (request.headers.get('x-cloudtms-weekly-delivery-body-sha256')!==bodyHash) return false;
    const supplied = hexToBytes(request.headers.get('x-cloudtms-weekly-delivery-signature'));
    if (!supplied) return false;
    const url = new URL(request.url);
    return crypto.subtle.verify(
      'HMAC',await key(env,['verify']),supplied,encoder.encode(canonical({
        method:request.method,target:`${url.pathname}${url.search}`,
        timestamp,nonce,environment,bodyHash
      }))
    );
  } catch {
    return false;
  }
}

export const weeklySourceDeliveryAuthInternals = Object.freeze({ canonical,digest });
