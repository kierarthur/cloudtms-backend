// Banking Pay Stage 2 (H3): the one shared recogniser for the two fail-fast
// Banking Pay busy refusals.
//
// The database raises exactly two busy refusals, both with SQLSTATE 55P03:
//   RAISE EXCEPTION 'BPAY_SOURCE_FAMILY_BUSY'     USING ERRCODE = '55P03';
//   RAISE EXCEPTION 'BPAY_APPROVAL_EVIDENCE_BUSY' USING ERRCODE = '55P03';
// PostgREST has no mapping for SQLSTATE class 55, so both arrive as HTTP 500
// with the structured body {"code":"55P03","message":"<one of the two>"}.
//
// Recognition uses ONLY that structured body (`error.json.code` and
// `error.json.message`, exact match), never free text, and does not depend on
// which RPC raised the refusal. The raising transaction was rolled back, so the
// refused request had no effect; earlier, separately committed steps of a
// multi-request workflow are NOT claimed to have been undone.
//
// This module only classifies and shapes. It performs no retry, and it never
// changes `status`, `json` or `body` on an error object, so existing transient
// retry and background-job logic that reads those fields is unaffected.

export const BPAY_BUSY_SQLSTATE = '55P03';

const BPAY_BUSY_MESSAGES = Object.freeze({
  BPAY_SOURCE_FAMILY_BUSY:
    'Another operation is changing this Timesheet. This request was not completed.',
  BPAY_APPROVAL_EVIDENCE_BUSY:
    'Another approval operation is in progress. This request was not completed.',
});

const RPC_NAME_PATTERN = /^[a-z][a-z0-9_]{0,100}$/;

/**
 * Returns 'BPAY_SOURCE_FAMILY_BUSY' or 'BPAY_APPROVAL_EVIDENCE_BUSY' when the
 * error carries a PostgREST 4xx/5xx status and the exact structured busy
 * signature; otherwise null.
 */
export function bpayBusyCode(error) {
  const status = error?.status;
  if (!Number.isInteger(status) || status < 400 || status > 599) return null;
  const json = error?.json;
  if (!json || typeof json !== 'object' || Array.isArray(json)) return null;
  if (json.code !== BPAY_BUSY_SQLSTATE) return null;
  const message = json.message;
  return typeof message === 'string'
    && Object.prototype.hasOwnProperty.call(BPAY_BUSY_MESSAGES, message)
    ? message
    : null;
}

export function isBpayBusy(error) {
  return bpayBusyCode(error) !== null;
}

/**
 * The fixed transport message used in place of `RPC <fn> failed <status>: <raw
 * body>`. It carries no response body. It starts with the clean sentence, for
 * the routes that still forward `error.message` verbatim, and keeps the
 * SQLSTATE and code tokens that existing message-text classifiers match on
 * (for example /55P03/ and /BUSY/).
 */
export function bpayBusyRpcMessage(functionName, status, code) {
  const name = RPC_NAME_PATTERN.test(String(functionName || '')) ? String(functionName) : 'unknown';
  const sentence = BPAY_BUSY_MESSAGES[code] || BPAY_BUSY_MESSAGES.BPAY_SOURCE_FAMILY_BUSY;
  return `${sentence} (RPC ${name} failed ${Number(status) || 0}: ${BPAY_BUSY_SQLSTATE} ${code})`;
}

/**
 * For call sites that read a PostgREST response with a raw `fetch` instead of
 * `sbRpc`: builds the minimal error-shaped object `bpayBusyEnvelope` reads.
 */
export function bpayBusyErrorFromResponseText(functionName, status, responseText) {
  let json = null;
  try { json = responseText ? JSON.parse(responseText) : null; } catch { json = null; }
  return { fn: String(functionName || ''), status, json };
}

/**
 * The single busy response contract: HTTP 409, a clean message, no automatic
 * retry, and no claim about the wider workflow. Returns null when the error is
 * not one of the two busy refusals.
 */
export function bpayBusyEnvelope(error) {
  const code = bpayBusyCode(error);
  if (!code) return null;
  const functionName = String(error?.fn || '');
  return {
    status: 409,
    body: {
      ok: false,
      error_code: code,
      message: BPAY_BUSY_MESSAGES[code],
      failed_rpc: RPC_NAME_PATTERN.test(functionName) ? functionName : null,
      failed_rpc_outcome: 'ROLLED_BACK',
      automatic_retry: false,
      // Earlier preparation requests may already have committed. This is NOT
      // a declaration that an entire multi-request workflow was rolled back.
      workflow_outcome: 'NOT_ESTABLISHED',
    },
  };
}

/** `bpayBusyEnvelope` as a JSON Response, or null. */
export function bpayBusyResponse(error, headers = { 'content-type': 'application/json; charset=utf-8' }) {
  const busy = bpayBusyEnvelope(error);
  return busy
    ? new Response(JSON.stringify(busy.body), { status: busy.status, headers })
    : null;
}
