// A fresh, fixed, service-only owner check before legacy financial work. This
// does not lock a provider/email request: cutover still needs the reviewed quiet
// window and reconciliation of already admitted legacy work. No status cache.
export const BPAY_NEXT_OWNER_STATUS_RPC = 'bpay_next_owner_status_v1';
export const BPAY_NEXT_OWNER_STATUS_LIMITS = Object.freeze({ responseBytes: 1024, timeoutMs: 8000 });
const OWNERS = new Set(['LEGACY', 'NEXT', 'DISABLED']);
const PG_BIGINT_MAX = 9223372036854775807n;
const OWNER_FIELD = '"active_owner"\\s*:\\s*"(?:LEGACY|NEXT|DISABLED)"';
const EPOCH_FIELD = '"owner_epoch"\\s*:\\s*"[1-9]\\d{0,18}"';
const RAW_STATUS = new RegExp(`^\\s*\\{\\s*(?:${OWNER_FIELD}\\s*,\\s*${EPOCH_FIELD}|${EPOCH_FIELD}\\s*,\\s*${OWNER_FIELD})\\s*\\}\\s*$`);

export class BpayNextLegacyFenceError extends Error {
  constructor(code, status = 503, retryable = true) {
    super(code);
    this.name = 'BpayNextLegacyFenceError';
    this.code = code;
    this.status = status;
    this.retryable = retryable;
  }
}
const unavailable = () => new BpayNextLegacyFenceError('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE');

export function normalizeBpayNextOwnerStatus(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)
      || ![Object.prototype, null].includes(Object.getPrototypeOf(value))
      || Reflect.ownKeys(value).length !== 2
      || !Object.hasOwn(value, 'active_owner') || !Object.hasOwn(value, 'owner_epoch')
      || !OWNERS.has(value.active_owner) || typeof value.owner_epoch !== 'string'
      || !/^[1-9]\d{0,18}$/.test(value.owner_epoch) || BigInt(value.owner_epoch) > PG_BIGINT_MAX) {
    throw unavailable();
  }
  return Object.freeze({ active_owner: value.active_owner, owner_epoch: value.owner_epoch });
}

function serviceHeaders(input) {
  try {
    const headers = new Headers(input);
    const authorization = headers.get('authorization');
    if (typeof authorization !== 'string' || authorization.length > 8192
        || !/^Bearer [A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(authorization)) {
      throw new Error();
    }
    const payload = authorization.slice(7).split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    const bytes = Uint8Array.from(atob(payload), (character) => character.charCodeAt(0));
    const claims = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    // This is only a local shape check. PostgREST verifies the signature and the
    // SQL RPC independently requires service_role; decoded claims are not auth.
    if (claims === null || typeof claims !== 'object' || Array.isArray(claims)
        || claims.role !== 'service_role') throw new Error();
    headers.set('content-type', 'application/json');
    headers.set('accept', 'application/json');
    headers.set('X-CloudTMS-Banking-Pay-Route-Class', 'DISPLAY');
    return headers;
  } catch {
    throw new TypeError('BPAY_NEXT_OWNER_STATUS_TRANSPORT_INVALID');
  }
}

/** Fixed zero-argument PostgREST RPC. No retries, generic RPC, or body fallback. */
export function createBpayNextOwnerStatusRpc({ baseUrl, headers, fetchImpl = fetch,
  timeoutMs = BPAY_NEXT_OWNER_STATUS_LIMITS.timeoutMs }) {
  let endpoint;
  try {
    const base = new URL(baseUrl);
    if (base.username || base.password || base.search || base.hash
        || (base.protocol !== 'https:' && !(base.protocol === 'http:'
          && ['localhost', '127.0.0.1', '[::1]'].includes(base.hostname)))
        || typeof fetchImpl !== 'function' || !Number.isInteger(timeoutMs)
        || timeoutMs < 1 || timeoutMs > BPAY_NEXT_OWNER_STATUS_LIMITS.timeoutMs) throw new Error();
    endpoint = `${base.href.replace(/\/+$/, '')}/rest/v1/rpc/${BPAY_NEXT_OWNER_STATUS_RPC}`;
  } catch {
    throw new TypeError('BPAY_NEXT_OWNER_STATUS_TRANSPORT_INVALID');
  }
  const requestHeaders = serviceHeaders(headers);
  return async (...args) => {
    if (args.length !== 0) throw new TypeError('BPAY_NEXT_OWNER_STATUS_TRANSPORT_INVALID');
    const controller = new AbortController();
    let response;
    let reader;
    let abandoned = false;
    // Best-effort cancellation must not extend the bounded lookup if a broken
    // dependency's cancel promise never resolves. No business work is floated.
    const cancel = () => {
      try {
        const cleanup = reader ? reader.cancel() : response?.body?.cancel();
        if (cleanup) void cleanup.catch(() => {});
      } catch { /* Already locked/closed by abort: nothing else to consume. */ }
    };
    let timer;
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => { abandoned = true; controller.abort(); cancel(); reject(unavailable()); }, timeoutMs);
    });
    try {
      const request = Promise.resolve().then(() => fetchImpl(endpoint, {
        method: 'POST', headers: new Headers(requestHeaders), body: '{}',
        signal: controller.signal, redirect: 'error'
      })).then((raw) => {
        response = raw;
        if (abandoned) { cancel(); throw unavailable(); }
        return raw;
      });
      response = await Promise.race([request, deadline]);
      if (abandoned || controller.signal.aborted || !(response instanceof Response) || response.status !== 200
          || !/^application\/json(?:\s*;|$)/i.test(response.headers.get('content-type') || '')
          || !response.body) throw unavailable();
      const declared = response.headers.get('content-length');
      if (declared !== null && (!/^\d+$/.test(declared)
          || Number(declared) > BPAY_NEXT_OWNER_STATUS_LIMITS.responseBytes)) throw unavailable();
      reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8', { fatal: true });
      let bytes = 0;
      let text = '';
      while (true) {
        const part = await Promise.race([reader.read(), deadline]);
        if (abandoned || controller.signal.aborted) throw unavailable();
        if (part.done) break;
        if (!(part.value instanceof Uint8Array)) throw unavailable();
        bytes += part.value.byteLength;
        if (bytes > BPAY_NEXT_OWNER_STATUS_LIMITS.responseBytes) throw unavailable();
        text += decoder.decode(part.value, { stream: true });
      }
      text += decoder.decode();
      // The actual fixed SQL emits only these two ASCII fields. Reject duplicate
      // or escaped-key ambiguities before JSON.parse can discard them.
      if (abandoned || controller.signal.aborted || !RAW_STATUS.test(text)) throw unavailable();
      return normalizeBpayNextOwnerStatus(JSON.parse(text));
    } catch {
      abandoned = true;
      controller.abort();
      cancel();
      throw unavailable();
    } finally {
      clearTimeout(timer);
      try { reader?.releaseLock(); } catch { /* Pending read is cancelled above. */ }
    }
  };
}

export function createBpayNextLegacyFence({ ownerStatusRpc }) {
  if (typeof ownerStatusRpc !== 'function') throw new TypeError('BPAY_NEXT_OWNER_STATUS_TRANSPORT_INVALID');
  const assertLegacyWriter = async () => {
    let status;
    try { status = normalizeBpayNextOwnerStatus(await ownerStatusRpc()); } catch { throw unavailable(); }
    if (status.active_owner === 'NEXT') {
      throw new BpayNextLegacyFenceError('BPAY_NEXT_LEGACY_WRITER_RETIRED', 410, false);
    }
    if (status.active_owner === 'DISABLED') {
      throw new BpayNextLegacyFenceError('BPAY_NEXT_LEGACY_WRITER_DISABLED', 503, true);
    }
    return status;
  };
  const runLegacyWriter = async (action) => {
    if (typeof action !== 'function') throw new TypeError('BPAY_NEXT_LEGACY_ACTION_INVALID');
    const status = await assertLegacyWriter();
    return action(status);
  };
  return Object.freeze({ assertLegacyWriter, runLegacyWriter });
}

// Only known NEXT contracts escape the old route fence, never arbitrary /next/*.
// Source/ordinary facts, invoice discounting and bank/name setters stay factual.
export function isBpayNextLegacyFinancialRoute(method, pathname) {
  if (typeof method !== 'string' || typeof pathname !== 'string') return false;
  // The retained matchPath router ignores empty slash segments. Use the SAME
  // segmentation here so a doubled-slash alias cannot bypass retirement.
  pathname = '/' + pathname.split('/').filter(Boolean).join('/');
  // EX-01 exact exemptions: actual handlers qualify BPAY_NEXT_ACTION/NEXT.
  if (method === 'POST' && ['/auth/reauth/start', '/auth/reauth/verify'].includes(pathname)) return false;
  if ((method === 'POST' && pathname === '/api/banking/pay/next/command')
      || (method === 'POST' && pathname === '/api/banking/pay/next/reauth-preview')
      || (method === 'POST' && pathname === '/api/banking/pay/next/preparation')
      || (method === 'GET' && pathname === '/api/banking/pay/next/preparation-page')
      || (method === 'GET' && pathname === '/api/banking/pay/next/workbench-page')
      || (method === 'POST' && pathname === '/api/banking/pay/next/workbench-export')
      || (method === 'POST' && pathname === '/api/banking/pay/next/workbench-command')
      || (method === 'GET' && /^\/api\/banking\/pay\/next\/advice\/[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(pathname))
      || (method === 'POST' && pathname === '/api/banking/pay/next/history-page')
      || (method === 'POST' && pathname === '/api/banking/pay/next/frozen-detail')
      || (method === 'GET' && pathname === '/api/banking/pay/next/owner-status')) return false;
  if (method === 'POST' && ['/api/banking/pay/bank-name-check/override',
    '/api/banking/pay/bank-name-check/clear-override'].includes(pathname)) return false;
  if (/^\/api\/banking\/(?:pay|finance)(?:\/|$)/.test(pathname)) return true;
  if (method === 'POST' && /^\/api\/timesheets\/[^/]+\/(?:advance-payment|unadvance-payment|snooze-payment(?:\/clear)?|pay-adjustment)$/.test(pathname)) return true;
  if (method === 'POST' && ['/api/remittances/send', '/api/remittances/email-for-candidate'].includes(pathname)) return true;
  return (method === 'POST' && /^\/api\/candidates\/[^/]+\/pay-method-change$/.test(pathname))
    || (method === 'GET' && /^\/api\/candidates\/[^/]+\/pay-method-change-preview$/.test(pathname));
}
