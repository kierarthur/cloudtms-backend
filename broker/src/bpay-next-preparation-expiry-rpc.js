/**
 * INTERNAL service-only expiry discovery and intake. These are not browser
 * handlers, a scheduler, an enrollment/claim drain, or a private clock adapter.
 * SQL owns the SYSTEM actor, server clock and namespace-derived command UUID.
 * A lost RECEIVE reply is uncertain: retry the SAME run, never invent a new
 * command ID or silently repeat here. No mutable status/cursor cache.
 */
export const BPAY_NEXT_PREPARATION_EXPIRY_LIMITS = Object.freeze({
  requestBytes: 1024, responseBytes: 32 * 1024, timeoutMs: 8000, pageRows: 100
});
const RPCS = Object.freeze({
  DUE: 'bpay_next_preparation_expiry_due_v1',
  RECEIVE: 'bpay_next_preparation_expiry_receive_v1'
});
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const INTEGER = /^(?:0|[1-9]\d{0,18})$/;
const BIGINT_MAX = 9223372036854775807n;
const ENCODER = new TextEncoder();
const invalidTransport = () => new TypeError('BPAY_NEXT_EXPIRY_TRANSPORT_INVALID');

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
// Calendar validation and comparison never use Date: its millisecond precision
// would collapse two different PostgreSQL microsecond keyset positions.
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
function tupleCompare(left, right) {
  const leftTime = timestampKey(left.expected_deadline);
  const rightTime = timestampKey(right.expected_deadline);
  if (leftTime !== rightTime) return leftTime < rightTime ? -1 : 1;
  const a = left.run_id.toLowerCase();
  const b = right.run_id.toLowerCase();
  return a < b ? -1 : a > b ? 1 : 0;
}
function dueInput(value) {
  if (!exact(value, ['after_deadline', 'after_run_id', 'limit'])
      || !Number.isInteger(value.limit) || value.limit < 1 || value.limit > 100
      || (value.after_deadline === null) !== (value.after_run_id === null)
      || (value.after_deadline !== null && (!timestampKey(value.after_deadline) || !uuid(value.after_run_id)))) {
    throw new Error('BPAY_NEXT_EXPIRY_REQUEST_INVALID');
  }
  return Object.freeze({ after_deadline: value.after_deadline, after_run_id: value.after_run_id, limit: value.limit });
}
function receiveInput(value) {
  if (!exact(value, ['run_id']) || !uuid(value.run_id)) throw new Error('BPAY_NEXT_EXPIRY_REQUEST_INVALID');
  return Object.freeze({ run_id: value.run_id });
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
    // Shape only, NOT signature verification or authorization. PostgREST
    // verifies the JWT; both fixed SQL RPCs independently require service_role.
    if (claims === null || typeof claims !== 'object' || Array.isArray(claims)
        || claims.role !== 'service_role') throw new Error();
    headers.set('content-type', 'application/json');
    headers.set('accept', 'application/json');
    return headers;
  } catch { throw invalidTransport(); }
}

/** Fixed raw transport. The parsed adapters below own response/deadline bounds. */
export function createBpayNextPreparationExpiryRpc(config) {
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
      if (action === 'DUE') {
        if (!exact(args, ['p_after_deadline', 'p_after_run_id', 'p_limit'])) throw new Error();
        dueInput({ after_deadline: args.p_after_deadline, after_run_id: args.p_after_run_id, limit: args.p_limit });
      } else {
        if (!exact(args, ['p_run_id'])) throw new Error();
        receiveInput({ run_id: args.p_run_id });
      }
    } catch { throw invalidTransport(); }
    const body = JSON.stringify(args);
    if (ENCODER.encode(body).byteLength > BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.requestBytes) throw invalidTransport();
    const requestHeaders = new Headers(headers);
    requestHeaders.set('X-CloudTMS-Banking-Pay-Route-Class', action === 'DUE' ? 'DISPLAY' : 'OPERATION_NUDGE');
    return fetchImpl(`${endpoint}${RPCS[action]}`, {
      method: 'POST', headers: requestHeaders, body, signal: options.signal, redirect: 'error'
    });
  };
}

// Reject duplicate (including escaped) JSON keys before JSON.parse discards
// them. Depth is bounded independently of bytes; SQL's closed replies need <=3.
function parseUniqueJson(text) {
  const stack = [];
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (char === '{' || char === '[') {
      stack.push(char === '{' ? new Set() : null);
      if (stack.length > 8) throw new Error();
    } else if (char === '}' || char === ']') {
      stack.pop();
    } else if (char === '"') {
      const start = index;
      index += 1;
      for (; index < text.length; index += 1) {
        if (text[index] === '\\') index += 1;
        else if (text[index] === '"') break;
      }
      const token = JSON.parse(text.slice(start, index + 1));
      let next = index + 1;
      while (/\s/.test(text[next] || '') && next < text.length) next += 1;
      if (text[next] === ':') {
        const keys = stack[stack.length - 1];
        if (!(keys instanceof Set) || keys.has(token)) throw new Error();
        keys.add(token);
      }
    }
  }
  return JSON.parse(text);
}
function dueResult(value, call) {
  if (!exact(value, ['items', 'cursor_deadline', 'cursor_run_id']) || !Array.isArray(value.items)
      || value.items.length > call.limit || value.items.length > 100) throw new Error();
  let previous = call.after_deadline === null ? null
    : { expected_deadline: call.after_deadline, run_id: call.after_run_id };
  const seen = new Set();
  const items = value.items.map((item) => {
    if (!exact(item, ['run_id', 'expected_deadline']) || !uuid(item.run_id) || !timestampKey(item.expected_deadline)
        || seen.has(item.run_id.toLowerCase()) || (previous && tupleCompare(previous, item) >= 0)) throw new Error();
    seen.add(item.run_id.toLowerCase());
    previous = item;
    return Object.freeze({ run_id: item.run_id, expected_deadline: item.expected_deadline });
  });
  const last = items.at(-1);
  if ((value.cursor_deadline === null) !== (value.cursor_run_id === null)) throw new Error();
  // SQL visits a finite native due slice before skipping discarded runs. Its
  // cursor includes those skipped rows, so an empty eligible page can advance.
  if (value.cursor_run_id === null) { if (last) throw new Error(); }
  else {
    if (!uuid(value.cursor_run_id) || !timestampKey(value.cursor_deadline)) throw new Error();
    const visited = { expected_deadline: value.cursor_deadline, run_id: value.cursor_run_id };
    if ((last && tupleCompare(last, visited) > 0)
      || (call.after_run_id !== null && tupleCompare({ expected_deadline: call.after_deadline, run_id: call.after_run_id }, visited) >= 0)) throw new Error();
  }
  return Object.freeze({ items: Object.freeze(items), cursor_deadline: value.cursor_deadline, cursor_run_id: value.cursor_run_id });
}
function receiveResult(value, call) {
  if (!exact(value, ['command_id', 'run_id', 'sequence', 'phase', 'expected_candidates', 'completed_candidates', 'replay'])
      || !uuid(value.command_id) || !uuid(value.run_id) || value.run_id.toLowerCase() !== call.run_id.toLowerCase()
      || !integer(value.sequence, true) || !integer(value.expected_candidates) || !integer(value.completed_candidates)
      || !['CANCELLING', 'EXPIRED'].includes(value.phase) || typeof value.replay !== 'boolean') throw new Error();
  const expected = BigInt(value.expected_candidates);
  const completed = BigInt(value.completed_candidates);
  if (completed > expected || (value.phase === 'EXPIRED' && completed !== expected)
      || (value.phase === 'CANCELLING' && completed === expected)) throw new Error();
  // The command namespace is SQL-owned. Validate its type and requested run,
  // retain the real receipt identity, and never substitute a locally made ID.
  return Object.freeze({ ...value });
}
class ExpiryError extends Error {
  constructor(code, retryable = false, known = false) {
    super(code);
    this.code = code;
    this.retryable = retryable;
    this.known = known;
  }
}
function sqlFailure(value) {
  if (!exact(value, ['code', 'message', 'details', 'hint']) || typeof value.code !== 'string'
      || typeof value.message !== 'string' || ![null, 'string'].includes(value.details === null ? null : typeof value.details)
      || ![null, 'string'].includes(value.hint === null ? null : typeof value.hint)) return null;
  const faults = {
    PGRST202: ['DEPENDENCY_UNAVAILABLE', true], '42501': ['FORBIDDEN', false],
    '22023': ['REQUEST_INVALID', false], '22007': ['REQUEST_INVALID', false],
    '22008': ['REQUEST_INVALID', false], '22P02': ['REQUEST_INVALID', false],
    '22003': ['REQUEST_INVALID', false], P0002: ['SCOPE_NOT_FOUND', false],
    '23514': ['CONFLICT', false], '23505': ['CONFLICT', false],
    '40001': ['TRANSACTION_RETRY', true], '40P01': ['TRANSACTION_RETRY', true],
    '55P03': ['TRANSACTION_RETRY', true], '57014': ['TRANSACTION_RETRY', true]
  };
  let result = Object.hasOwn(faults, value.code) ? faults[value.code] : null;
  if (value.code === '55000' && value.message === 'BPAY_NEXT_EXPIRY_WORKER_RELEASE_PENDING') result = ['TRANSACTION_RETRY', true];
  else if (value.code === '55000') result = [value.message === 'BPAY_NEXT_MODULE_NOT_ACTIVE' ? 'MODULE_NOT_ACTIVE'
    : value.message === 'BPAY_NEXT_EXPIRY_NOT_DUE' ? 'NOT_DUE'
      : value.message === 'BPAY_NEXT_EXPIRY_NOT_UNCONFIRMED' ? 'NOT_ELIGIBLE' : 'STATE_INVALID', false];
  return result ? new ExpiryError(`BPAY_NEXT_EXPIRY_${result[0]}`, result[1], true) : null;
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
    const normalized = action === 'DUE' ? dueInput(input) : receiveInput(input);
    if (!exact(dependencies, ['preparationExpiryRpc', ...(Object.hasOwn(dependencies || {}, 'timeoutMs') ? ['timeoutMs'] : [])])
        || typeof dependencies.preparationExpiryRpc !== 'function') {
      throw new ExpiryError('BPAY_NEXT_EXPIRY_DEPENDENCY_UNAVAILABLE', true, true);
    }
    const timeoutMs = Object.hasOwn(dependencies, 'timeoutMs') ? dependencies.timeoutMs : BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.timeoutMs;
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.timeoutMs) {
      throw new ExpiryError('BPAY_NEXT_EXPIRY_REQUEST_INVALID', false, true);
    }
    const args = action === 'DUE' ? Object.freeze({ p_after_deadline: normalized.after_deadline,
      p_after_run_id: normalized.after_run_id, p_limit: normalized.limit }) : Object.freeze({ p_run_id: normalized.run_id });
    controller = new AbortController();
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => {
        abandoned = true;
        controller.abort();
        cancel();
        reject(new ExpiryError('BPAY_NEXT_EXPIRY_TIMED_OUT', true));
      }, timeoutMs);
    });
    const request = Promise.resolve().then(() => {
      attempted = true;
      return dependencies.preparationExpiryRpc(action, args, Object.freeze({ signal: controller.signal }));
    }).then((raw) => {
      response = raw;
      if (abandoned) { cancel(); throw new ExpiryError('BPAY_NEXT_EXPIRY_TIMED_OUT', true); }
      return raw;
    });
    response = await Promise.race([request, deadline]);
    if (abandoned || controller.signal.aborted || !(response instanceof Response) || !response.body
        || response.redirected || !/^application\/json(?:\s*;|$)/i.test(response.headers.get('content-type') || '')) {
      throw new ExpiryError('BPAY_NEXT_EXPIRY_RESPONSE_INVALID', true);
    }
    const declared = response.headers.get('content-length');
    if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.responseBytes)) {
      throw new ExpiryError('BPAY_NEXT_EXPIRY_RESPONSE_TOO_LARGE', true);
    }
    reader = response.body.getReader();
    const decoder = new TextDecoder('utf-8', { fatal: true });
    let bytes = 0;
    let text = '';
    while (true) {
      const part = await Promise.race([reader.read(), deadline]);
      if (abandoned || controller.signal.aborted) throw new ExpiryError('BPAY_NEXT_EXPIRY_TIMED_OUT', true);
      if (part.done) break;
      if (!(part.value instanceof Uint8Array)) throw new ExpiryError('BPAY_NEXT_EXPIRY_RESPONSE_INVALID', true);
      bytes += part.value.byteLength;
      if (bytes > BPAY_NEXT_PREPARATION_EXPIRY_LIMITS.responseBytes) {
        throw new ExpiryError('BPAY_NEXT_EXPIRY_RESPONSE_TOO_LARGE', true);
      }
      text += decoder.decode(part.value, { stream: true });
    }
    text += decoder.decode();
    if (abandoned || controller.signal.aborted) throw new ExpiryError('BPAY_NEXT_EXPIRY_TIMED_OUT', true);
    let payload;
    try { payload = parseUniqueJson(text); } catch { throw new ExpiryError('BPAY_NEXT_EXPIRY_RESPONSE_INVALID', true); }
    if (!response.ok) throw sqlFailure(payload) || new ExpiryError('BPAY_NEXT_EXPIRY_TRANSPORT_FAILED', true);
    if (response.status !== 200) throw new ExpiryError('BPAY_NEXT_EXPIRY_RESPONSE_INVALID', true);
    let result;
    try { result = action === 'DUE' ? dueResult(payload, normalized) : receiveResult(payload, normalized); }
    catch { throw new ExpiryError('BPAY_NEXT_EXPIRY_RESPONSE_INVALID', true); }
    return Object.freeze({ ok: true, action, result });
  } catch (error) {
    abandoned = true;
    controller?.abort();
    cancel();
    const failure = error instanceof ExpiryError ? error
      : new ExpiryError(attempted ? 'BPAY_NEXT_EXPIRY_TRANSPORT_FAILED' : 'BPAY_NEXT_EXPIRY_REQUEST_INVALID', attempted, !attempted);
    const unknown = action === 'RECEIVE' && attempted && !failure.known;
    return Object.freeze({ ok: false, error_code: failure.code, retryable: failure.retryable,
      outcome_unknown: unknown, recovery: failure.retryable ? action === 'RECEIVE' ? 'RETRY_SAME_RUN' : 'RETRY_CALL' : 'NONE' });
  } finally {
    clearTimeout(timer);
    try { reader?.releaseLock(); } catch { /* Cancelled pending read. */ }
  }
}

/** Paired UTC keyset input; exact timestamp strings are retained in the page. */
export function readBpayNextPreparationExpiryDue(...args) { return call('DUE', args.length === 2 ? args[0] : null, args[1]); }
/** Only a run identity: SQL, never the caller, supplies SYSTEM/clock/command. */
export function receiveBpayNextPreparationExpiry(...args) { return call('RECEIVE', args.length === 2 ? args[0] : null, args[1]); }
