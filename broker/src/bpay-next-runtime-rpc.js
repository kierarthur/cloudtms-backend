/**
 * INTERNAL one-call metadata enrollment/recovery, not an Office endpoint,
 * scheduler, financial worker, historical job drain or lease-recovery API.
 * Queue messages only wake durable jobs. CLAIM alone decides eligibility and
 * creates a fresh lease; these calls never expose or adopt a stored token.
 * All three SQL calls mutate metadata. Lost replies are uncertain; this adapter
 * reports recovery direction and never silently retries or sends a message.
 */
export const BPAY_NEXT_RUNTIME_LIMITS = Object.freeze({
  requestBytes: 1024, responseBytes: 32 * 1024, timeoutMs: 8000, pageRows: 100
});
export const BPAY_NEXT_RUNTIME_JOB_KINDS = Object.freeze([
  'PAYE_NET_ENTRY', 'TRANSFER_BUILD', 'CSV_SETTLEMENT', 'CSV_RETURN', 'CASH_REISSUE',
  'SIMPLE_CANCEL', 'POSITION_APPLY', 'PREPARE', 'REISSUE_CANCEL', 'INTERNAL_SETTLEMENT',
  'PREPARATION_EXPIRY', 'CASE_CREATE', 'CASE_WRITE_OFF'
]);
const RPCS = Object.freeze({ ENROLL: 'bpay_next_enroll_service_v1', WAKE_PAGE: 'bpay_next_job_wake_page_v1',
  EXPIRY_PAGE: 'bpay_next_expiry_dispatch_page_v1' });
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const INTEGER = /^(?:0|[1-9]\d{0,18})$/;
const BIGINT_MAX = 9223372036854775807n;
const ENCODER = new TextEncoder();
const invalidTransport = () => new TypeError('BPAY_NEXT_RUNTIME_TRANSPORT_INVALID');
function exact(value, keys) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)
      || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) return false;
  const own = Reflect.ownKeys(value);
  return own.length === keys.length && own.every((key) => typeof key === 'string' && keys.includes(key)
    && Object.getOwnPropertyDescriptor(value, key)?.enumerable === true
    && Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'));
}
const uuid = (value) => typeof value === 'string' && UUID.test(value);
function integer(value, positive = false) {
  return typeof value === 'string' && INTEGER.test(value) && BigInt(value) <= BIGINT_MAX
    && (!positive || value !== '0');
}
function pageInput(value) {
  if (!exact(value, ['lane', 'limit']) || !['READY', 'LEASE_EXPIRED'].includes(value.lane)
      || !Number.isInteger(value.limit) || value.limit < 1 || value.limit > 100) {
    throw new Error('BPAY_NEXT_RUNTIME_REQUEST_INVALID');
  }
  return Object.freeze({ lane: value.lane, limit: value.limit });
}
function expiryInput(value) {
  if (!exact(value, ['limit']) || !Number.isInteger(value.limit) || value.limit < 1 || value.limit > 100) {
    throw new Error('BPAY_NEXT_RUNTIME_REQUEST_INVALID');
  }
  return Object.freeze({ limit: value.limit });
}
// PostgreSQL timestamps carry microseconds. Calendar validation/comparison
// must not round through Date's millisecond precision; retain original strings.
function timestampKey(value) {
  if (typeof value !== 'string') return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})T([01]\d|2[0-3]):([0-5]\d):([0-5]\d)(?:\.(\d{1,6}))?Z$/.exec(value);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (year < 1 || month < 1 || month > 12 || day < 1 || day > days[month - 1]) return null;
  return `${value.slice(0, 19)}.${(match[7] || '').padEnd(6, '0')}Z`;
}
function expiryTupleCompare(left, right) {
  const a = timestampKey(left.expected_deadline);
  const b = timestampKey(right.expected_deadline);
  if (a !== b) return a < b ? -1 : 1;
  const leftId = left.run_id.toLowerCase();
  const rightId = right.run_id.toLowerCase();
  return leftId < rightId ? -1 : leftId > rightId ? 1 : 0;
}
function serviceHeaders(input) {
  try {
    const headers = new Headers(input);
    const authorization = headers.get('authorization');
    if (typeof authorization !== 'string' || authorization.length > 8192
        || !/^Bearer [A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(authorization)) throw new Error();
    const payload = authorization.slice(7).split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    const bytes = Uint8Array.from(atob(payload), (character) => character.charCodeAt(0));
    const claims = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    // Local shape only. PostgREST verifies the signature and the exact SQL
    // wrappers independently require service_role; decoded claims are not auth.
    if (claims === null || typeof claims !== 'object' || Array.isArray(claims)
        || claims.role !== 'service_role') throw new Error();
    headers.set('content-type', 'application/json');
    headers.set('accept', 'application/json');
    headers.set('X-CloudTMS-Banking-Pay-Route-Class', 'OPERATION_NUDGE');
    return headers;
  } catch { throw invalidTransport(); }
}

/** Fixed raw service transport; parsed adapters own bytes/deadline/cleanup. */
export function createBpayNextRuntimeRpc(config) {
  let endpoint;
  let headers;
  let fetchImpl;
  try {
    if (!exact(config, ['baseUrl', 'headers', ...(Object.hasOwn(config || {}, 'fetchImpl') ? ['fetchImpl'] : [])])) throw new Error();
    const base = new URL(config.baseUrl);
    fetchImpl = Object.hasOwn(config, 'fetchImpl') ? config.fetchImpl : fetch;
    if (base.username || base.password || base.search || base.hash
        || (base.protocol !== 'https:' && !(base.protocol === 'http:'
          && ['localhost', '127.0.0.1', '[::1]'].includes(base.hostname)))
        || typeof fetchImpl !== 'function') throw new Error();
    endpoint = `${base.href.replace(/\/+$/, '')}/rest/v1/rpc/`;
    headers = serviceHeaders(config.headers);
  } catch { throw invalidTransport(); }
  return async (...values) => {
    const [action, args, options] = values;
    try {
      if (values.length !== 3 || typeof action !== 'string' || !Object.hasOwn(RPCS, action)
          || !exact(options, ['signal']) || !(options.signal instanceof AbortSignal)) throw new Error();
      if (action === 'ENROLL') {
        if (!exact(args, [])) throw new Error();
      } else if (action === 'WAKE_PAGE') {
        if (!exact(args, ['p_lane', 'p_limit'])) throw new Error();
        pageInput({ lane: args.p_lane, limit: args.p_limit });
      } else {
        if (!exact(args, ['p_limit'])) throw new Error();
        expiryInput({ limit: args.p_limit });
      }
    } catch { throw invalidTransport(); }
    const body = JSON.stringify(args);
    if (ENCODER.encode(body).byteLength > BPAY_NEXT_RUNTIME_LIMITS.requestBytes) throw invalidTransport();
    return fetchImpl(`${endpoint}${RPCS[action]}`, { method: 'POST',
      headers: new Headers(headers), body, signal: options.signal, redirect: 'error' });
  };
}

// SQL returns closed JSON. Reject duplicate/escaped duplicate keys before
// JSON.parse drops their evidence; cap nesting separately from byte length.
function parseUniqueJson(text) {
  const stack = [];
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (char === '{' || char === '[') {
      stack.push(char === '{' ? new Set() : null);
      if (stack.length > 8) throw new Error();
    } else if (char === '}' || char === ']') stack.pop();
    else if (char === '"') {
      const start = index;
      index += 1;
      for (; index < text.length; index += 1) {
        if (text[index] === '\\') index += 1;
        else if (text[index] === '"') break;
      }
      const token = JSON.parse(text.slice(start, index + 1));
      let next = index + 1;
      while (next < text.length && /\s/.test(text[next])) next += 1;
      if (text[next] === ':') {
        const keys = stack[stack.length - 1];
        if (!(keys instanceof Set) || keys.has(token)) throw new Error();
        keys.add(token);
      }
    }
  }
  return JSON.parse(text);
}
function enrollmentResult(value) {
  if (!exact(value, ['progressed', 'last_enrolled_sequence', 'job_id', 'job_kind', 'job_status'])
      || typeof value.progressed !== 'boolean' || !integer(value.last_enrolled_sequence)) throw new Error();
  if (value.job_id === null) {
    if (value.job_kind !== null || value.job_status !== null
        || (value.progressed && value.last_enrolled_sequence === '0')) throw new Error();
    // A genuine empty expiry can advance the ordered command lane without a
    // job. Never turn its progressed:true into false or manufacture a worker.
  } else if (!uuid(value.job_id) || !BPAY_NEXT_RUNTIME_JOB_KINDS.includes(value.job_kind)
      || !['READY', 'DONE'].includes(value.job_status) || !value.progressed) throw new Error();
  return Object.freeze({ ...value });
}
function pageResult(value, call) {
  if (!exact(value, ['lane', 'items']) || value.lane !== call.lane || !Array.isArray(value.items)
      || value.items.length > call.limit || value.items.length > 100) throw new Error();
  const seen = new Set();
  const items = value.items.map((item) => {
    if (!exact(item, ['job_id', 'job_kind', 'command_sequence']) || !uuid(item.job_id)
        || !BPAY_NEXT_RUNTIME_JOB_KINDS.includes(item.job_kind) || !integer(item.command_sequence, true)
        || seen.has(item.job_id.toLowerCase())) throw new Error();
    seen.add(item.job_id.toLowerCase());
    return Object.freeze({ ...item });
  });
  // The SQL orders first by its server-owned wake time. That time is not in
  // this receipt, so command_sequence must NOT be assumed globally monotonic.
  return Object.freeze({ lane: value.lane, items: Object.freeze(items) });
}
function expiryResult(value, call) {
  if (!exact(value, ['items', 'cursor_deadline', 'cursor_run_id']) || !Array.isArray(value.items)
      || value.items.length > call.limit || value.items.length > 100) throw new Error();
  let previous = null;
  const seen = new Set();
  const items = value.items.map((item) => {
    if (!exact(item, ['run_id', 'expected_deadline']) || !uuid(item.run_id) || !timestampKey(item.expected_deadline)
        || seen.has(item.run_id.toLowerCase()) || (previous && expiryTupleCompare(previous, item) >= 0)) throw new Error();
    seen.add(item.run_id.toLowerCase()); previous = item;
    return Object.freeze({ ...item });
  });
  const last = items.at(-1);
  if (last ? value.cursor_deadline !== last.expected_deadline || value.cursor_run_id !== last.run_id
    : value.cursor_deadline !== null || value.cursor_run_id !== null) throw new Error();
  // Cursor/watermark persistence is entirely SQL-owned. This receipt's cursor
  // describes its returned page, never permission to replace the stored key.
  return Object.freeze({ items: Object.freeze(items), cursor_deadline: value.cursor_deadline, cursor_run_id: value.cursor_run_id });
}
class RuntimeError extends Error {
  constructor(code, retryable = false, known = false) {
    super(code); this.code = code; this.retryable = retryable; this.known = known;
  }
}
function sqlFailure(value) {
  if (!exact(value, ['code', 'message', 'details', 'hint']) || typeof value.code !== 'string'
      || typeof value.message !== 'string' || (value.details !== null && typeof value.details !== 'string')
      || (value.hint !== null && typeof value.hint !== 'string')) return null;
  const faults = {
    PGRST202: ['DEPENDENCY_UNAVAILABLE', true], '42501': ['FORBIDDEN', false],
    '22023': ['REQUEST_INVALID', false], '22P02': ['REQUEST_INVALID', false],
    '22003': ['REQUEST_INVALID', false], P0002: ['SCOPE_NOT_FOUND', false],
    '23514': ['CONFLICT', false], '23505': ['CONFLICT', false],
    '40001': ['TRANSACTION_RETRY', true], '40P01': ['TRANSACTION_RETRY', true],
    '55P03': ['TRANSACTION_RETRY', true], '57014': ['TRANSACTION_RETRY', true]
  };
  let result = Object.hasOwn(faults, value.code) ? faults[value.code] : null;
  if (value.code === '55000') result = [value.message === 'BPAY_NEXT_MODULE_NOT_ACTIVE' ? 'MODULE_NOT_ACTIVE' : 'STATE_INVALID', false];
  return result ? new RuntimeError(`BPAY_NEXT_RUNTIME_${result[0]}`, result[1], true) : null;
}
async function call(action, input, dependencies) {
  let attempted = false;
  let controller;
  let response;
  let reader;
  let timer;
  let abandoned = false;
  const cancel = () => {
    try {
      const cleanup = reader ? reader.cancel() : response?.body?.cancel();
      if (cleanup) void cleanup.catch(() => {});
    } catch { /* Already closed/locked by abort. */ }
  };
  try {
    let normalized;
    if (action === 'ENROLL') {
      if (!exact(input, [])) throw new RuntimeError('BPAY_NEXT_RUNTIME_REQUEST_INVALID', false, true);
      normalized = Object.freeze({});
    } else normalized = action === 'WAKE_PAGE' ? pageInput(input) : expiryInput(input);
    if (!exact(dependencies, ['runtimeRpc', ...(Object.hasOwn(dependencies || {}, 'timeoutMs') ? ['timeoutMs'] : [])])
        || typeof dependencies.runtimeRpc !== 'function') {
      throw new RuntimeError('BPAY_NEXT_RUNTIME_DEPENDENCY_UNAVAILABLE', true, true);
    }
    const timeoutMs = Object.hasOwn(dependencies, 'timeoutMs') ? dependencies.timeoutMs : BPAY_NEXT_RUNTIME_LIMITS.timeoutMs;
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > BPAY_NEXT_RUNTIME_LIMITS.timeoutMs) {
      throw new RuntimeError('BPAY_NEXT_RUNTIME_REQUEST_INVALID', false, true);
    }
    const args = Object.freeze(action === 'ENROLL' ? {} : action === 'WAKE_PAGE'
      ? { p_lane: normalized.lane, p_limit: normalized.limit } : { p_limit: normalized.limit });
    controller = new AbortController();
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => {
        abandoned = true; controller.abort(); cancel();
        reject(new RuntimeError('BPAY_NEXT_RUNTIME_TIMED_OUT', true));
      }, timeoutMs);
    });
    const request = Promise.resolve().then(() => {
      attempted = true;
      return dependencies.runtimeRpc(action, args, Object.freeze({ signal: controller.signal }));
    }).then((raw) => {
      response = raw;
      if (abandoned) { cancel(); throw new RuntimeError('BPAY_NEXT_RUNTIME_TIMED_OUT', true); }
      return raw;
    });
    response = await Promise.race([request, deadline]);
    if (abandoned || controller.signal.aborted || !(response instanceof Response) || !response.body
        || response.redirected || !/^application\/json(?:\s*;|$)/i.test(response.headers.get('content-type') || '')) {
      throw new RuntimeError('BPAY_NEXT_RUNTIME_RESPONSE_INVALID', true);
    }
    const declared = response.headers.get('content-length');
    if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > BPAY_NEXT_RUNTIME_LIMITS.responseBytes)) {
      throw new RuntimeError('BPAY_NEXT_RUNTIME_RESPONSE_TOO_LARGE', true);
    }
    reader = response.body.getReader();
    const decoder = new TextDecoder('utf-8', { fatal: true });
    let bytes = 0;
    let text = '';
    while (true) {
      const part = await Promise.race([reader.read(), deadline]);
      if (abandoned || controller.signal.aborted) throw new RuntimeError('BPAY_NEXT_RUNTIME_TIMED_OUT', true);
      if (part.done) break;
      if (!(part.value instanceof Uint8Array)) throw new RuntimeError('BPAY_NEXT_RUNTIME_RESPONSE_INVALID', true);
      bytes += part.value.byteLength;
      if (bytes > BPAY_NEXT_RUNTIME_LIMITS.responseBytes) throw new RuntimeError('BPAY_NEXT_RUNTIME_RESPONSE_TOO_LARGE', true);
      text += decoder.decode(part.value, { stream: true });
    }
    text += decoder.decode();
    if (abandoned || controller.signal.aborted) throw new RuntimeError('BPAY_NEXT_RUNTIME_TIMED_OUT', true);
    let payload;
    try { payload = parseUniqueJson(text); } catch { throw new RuntimeError('BPAY_NEXT_RUNTIME_RESPONSE_INVALID', true); }
    if (!response.ok) throw sqlFailure(payload) || new RuntimeError('BPAY_NEXT_RUNTIME_TRANSPORT_FAILED', true);
    if (response.status !== 200) throw new RuntimeError('BPAY_NEXT_RUNTIME_RESPONSE_INVALID', true);
    let result;
    try { result = action === 'ENROLL' ? enrollmentResult(payload) : action === 'WAKE_PAGE'
      ? pageResult(payload, normalized) : expiryResult(payload, normalized); }
    catch { throw new RuntimeError('BPAY_NEXT_RUNTIME_RESPONSE_INVALID', true); }
    return Object.freeze({ ok: true, action, result });
  } catch (error) {
    abandoned = true; controller?.abort(); cancel();
    const failure = error instanceof RuntimeError ? error
      : new RuntimeError(attempted ? 'BPAY_NEXT_RUNTIME_TRANSPORT_FAILED' : 'BPAY_NEXT_RUNTIME_REQUEST_INVALID', attempted, !attempted);
    const unknown = attempted && !failure.known;
    return Object.freeze({ ok: false, error_code: failure.code, retryable: failure.retryable,
      outcome_unknown: unknown, recovery: !failure.retryable ? 'NONE' : unknown
        ? action === 'ENROLL' ? 'READ_JOB_RECOVERY' : action === 'WAKE_PAGE'
          ? 'WAIT_FOR_WAKE_RECOVERY' : 'WAIT_FOR_SWEEP_RECOVERY' : 'RETRY_CALL' });
  } finally {
    clearTimeout(timer);
    try { reader?.releaseLock(); } catch { /* Cancelled pending read. */ }
  }
}

/** Exactly one ordered enrollment; no input, claim, STEP or inline drain. */
export function enrollBpayNextOne(...args) { return call('ENROLL', args.length === 1 ? {} : null, args[0]); }
/** One server-rotated wake page; metadata mutation despite the reader name. */
export function readBpayNextJobWakePage(...args) { return call('WAKE_PAGE', args.length === 2 ? args[0] : null, args[1]); }
/** One fixed-watermark expiry sweep page; no client key/clock or auto-receive. */
export function readBpayNextExpiryDispatchPage(...args) { return call('EXPIRY_PAGE', args.length === 2 ? args[0] : null, args[1]); }
