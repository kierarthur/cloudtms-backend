const RPC_NAME_PATTERN = /^[a-z][a-z0-9_]{0,126}$/;
const DEFAULT_TIMEOUT_MS = 60_000;
const MAX_REQUEST_BYTES = 8 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 2 * 1024 * 1024;

export class WeeklySourceC1RawRpcInputError extends TypeError {
  constructor(message) {
    super(message);
    this.name = 'WeeklySourceC1RawRpcInputError';
    this.code = 'C1_RAW_RPC_INPUT_INVALID';
  }
}

export class WeeklySourceC1RawRpcKnownOutcomeError extends Error {
  constructor(status, body, rpcName) {
    super(`C1 RPC ${rpcName} returned HTTP ${status}.`);
    this.name = 'WeeklySourceC1RawRpcKnownOutcomeError';
    this.code = 'C1_RAW_RPC_HTTP_ERROR';
    this.status = status;
    this.body = body;
    this.rpcName = rpcName;
    this.outcomeKnown = true;
    this.unknownOutcome = false;
    // Only a finite received DB error identifies retirement; business JSON stays lossless.
    if (typeof body === 'string' && body.length <= 8192) {
      try {
        const refusal = JSON.parse(body);
        if (refusal && !Array.isArray(refusal)
            && ['PT410','55000'].includes(refusal.code)
            && refusal.message === 'BPAY_NEXT_LEGACY_WRITER_RETIRED') {
          this.code = refusal.message; this.status = 410;
        } else if (refusal && !Array.isArray(refusal)
            && refusal.code === 'PT503' && refusal.message === 'BPAY_NEXT_LEGACY_WRITER_DISABLED') {
          this.code = refusal.message; this.status = 503;
        }
      } catch {}
    }
  }
}

export class WeeklySourceC1RawRpcAmbiguousError extends Error {
  constructor(code, message, cause) {
    super(message);
    this.name = 'WeeklySourceC1RawRpcAmbiguousError';
    this.code = code;
    this.outcomeKnown = false;
    this.unknownOutcome = true;
    if (cause !== undefined) Object.defineProperty(this, 'cause', { value: cause });
  }
}

function nonEmptyText(value, label) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new WeeklySourceC1RawRpcInputError(`${label} is required.`);
  }
  return value.trim();
}

function byteLength(value) {
  return new TextEncoder().encode(value).byteLength;
}

function normalizeBaseUrl(value) {
  const text = nonEmptyText(value, 'PostgREST URL').replace(/\/+$/, '');
  let parsed;
  try {
    parsed = new URL(text);
  } catch {
    throw new WeeklySourceC1RawRpcInputError('PostgREST URL is invalid.');
  }
  if (!['https:', 'http:'].includes(parsed.protocol)) {
    throw new WeeklySourceC1RawRpcInputError('PostgREST URL protocol is invalid.');
  }
  return text;
}

function normalizeHeaders(value) {
  if (value instanceof Headers) return Object.fromEntries(value.entries());
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new WeeklySourceC1RawRpcInputError('PostgREST headers are required.');
  }
  return { ...value };
}

/**
 * Build the one raw PostgREST transport used by the protected-hours C1
 * publisher.  It deliberately sends and returns untouched JSON text so
 * signed-bigint values never pass through JSON.parse or JSON.stringify.
 * It admits through one separate DB read, then performs exactly one C1 HTTP attempt.
 * The preflight is not a lease across transactions. A received HTTP response is a known
 * refusal; a timeout or transport loss is ambiguous and must be recovered by
 * the durable C1 owner before anything is replayed.
 */
export function createWeeklySourceC1RawRpc({
  baseUrl,
  headers,
  fetchImpl = globalThis.fetch,
  timeoutMs = DEFAULT_TIMEOUT_MS,
} = {}) {
  const origin = normalizeBaseUrl(baseUrl);
  const requestHeaders = normalizeHeaders(headers);
  if (typeof fetchImpl !== 'function') {
    throw new WeeklySourceC1RawRpcInputError('Fetch transport is unavailable.');
  }
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 120_000) {
    throw new WeeklySourceC1RawRpcInputError('C1 timeout must be between 1 and 120000 milliseconds.');
  }

  return async function weeklySourceC1RawRpc(functionName, parametersJsonText, options = {}) {
    const rpcName = nonEmptyText(functionName, 'C1 RPC name');
    if (!RPC_NAME_PATTERN.test(rpcName)) {
      throw new WeeklySourceC1RawRpcInputError('C1 RPC name is invalid.');
    }
    if (typeof parametersJsonText !== 'string' || parametersJsonText.trim() === '') {
      throw new WeeklySourceC1RawRpcInputError('C1 RPC parameters must be JSON text.');
    }
    if (byteLength(parametersJsonText) > MAX_REQUEST_BYTES) {
      throw new WeeklySourceC1RawRpcInputError(`C1 RPC request exceeds ${MAX_REQUEST_BYTES} bytes.`);
    }
    if (
      options?.automaticRetry !== false ||
      options?.requestBody !== 'LOSSLESS_JSON_TEXT' ||
      options?.responseBody !== 'LOSSLESS_JSON_TEXT'
    ) {
      throw new WeeklySourceC1RawRpcInputError('C1 RPC lossless no-retry contract is required.');
    }

    // DB owns this retired-finance entry. No owner values or business JSON are interpreted here.
    if (rpcName !== 'bpay_next_source_legacy_finance_admit_v1') {
      try {
        const admitted = await weeklySourceC1RawRpc('bpay_next_source_legacy_finance_admit_v1', '{}', options);
        if (admitted.trim() !== 'true') throw new Error('Invalid entry admission.');
      } catch (error) {
        if (['BPAY_NEXT_LEGACY_WRITER_RETIRED','BPAY_NEXT_LEGACY_WRITER_DISABLED'].includes(error?.code)) throw error;
        const unavailable = new WeeklySourceC1RawRpcKnownOutcomeError(503, '', 'bpay_next_source_legacy_finance_admit_v1');
        unavailable.code = 'BPAY_NEXT_OWNER_STATUS_UNAVAILABLE';
        throw unavailable; // The requested external C1 action was never attempted.
      }
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => {
      try { controller.abort(new Error(`C1 RPC timed out after ${timeoutMs}ms.`)); } catch {}
    }, timeoutMs);
    let response;
    try {
      response = await fetchImpl(`${origin}/rest/v1/rpc/${encodeURIComponent(rpcName)}`, {
        method: 'POST',
        headers: {
          ...requestHeaders,
          'content-type': 'application/json',
          accept: 'application/json',
        },
        body: parametersJsonText,
        signal: controller.signal,
      });
    } catch (error) {
      const timedOut = controller.signal.aborted;
      throw new WeeklySourceC1RawRpcAmbiguousError(
        timedOut ? 'C1_RAW_RPC_TIMEOUT' : 'C1_RAW_RPC_TRANSPORT_AMBIGUOUS',
        timedOut
          ? `C1 RPC ${rpcName} timed out with an unknown outcome.`
          : `C1 RPC ${rpcName} lost its response; the outcome is unknown.`,
        error,
      );
    } finally {
      clearTimeout(timeout);
    }

    let responseText;
    try {
      responseText = await response.text();
    } catch (error) {
      throw new WeeklySourceC1RawRpcAmbiguousError(
        'C1_RAW_RPC_RESPONSE_AMBIGUOUS',
        `C1 RPC ${rpcName} returned no readable result; the outcome is unknown.`,
        error,
      );
    }
    if (byteLength(responseText) > MAX_RESPONSE_BYTES) {
      throw new WeeklySourceC1RawRpcAmbiguousError(
        'C1_RAW_RPC_RESPONSE_TOO_LARGE',
        `C1 RPC ${rpcName} returned an oversized result; the outcome is unknown.`,
      );
    }
    if (!response.ok) {
      throw new WeeklySourceC1RawRpcKnownOutcomeError(response.status, responseText, rpcName);
    }
    return responseText;
  };
}

export const WEEKLY_SOURCE_C1_RAW_RPC_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_C1_RAW_RPC_HTTP_V1',
  automaticRetry: false,
  requestBody: 'LOSSLESS_JSON_TEXT',
  responseBody: 'LOSSLESS_JSON_TEXT',
  parsedJson: false,
  ambiguousOutcomeRequiresRecovery: true,
});
