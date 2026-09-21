// Weekly Source Plan 6.2 — the durable caller's half of HANDOVER 2 round-5
// ruling A2 / contract decision D13 (WP-23).
//
// The ruling, quoted in contract section 1.2 D13 and repeated in section 15:
//
//   "A guard refusal must occur before any mutation.  Do not insert an audit row
//    in the transaction and then pretend the rolled-back row is durable.  Where a
//    caller receives the refusal, it may record the structured refusal in a
//    separate post-rollback transaction using the same correlation identity.
//    Where no durable caller exists, return the structured refusal to the Office
//    screen and retain ordinary operational logs; do not weaken the zero-write
//    proof merely to manufacture an audit row."
//
// WP-14c built and proved the database owner
// `public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb)` and then
// recorded, as its handoff N1, that no Worker code called it — so every raising
// managed-root guard refusal was still silent in production.  This module is
// that caller.  It changes no owner: the request envelope below is WP-14c's, key
// for key, and nothing here re-derives a refusal from the current database state.
//
// The four rules WP-14c states the caller must honour, and how each is honoured:
//
//   1. A NEW transaction that has not written.  Every call this module makes goes
//      through the ordinary Weekly Source `rpc` dependency, which is one
//      PostgREST request and therefore one fresh transaction of its own.  The
//      module never batches the record onto the refused call.
//   2. After the rollback, never before or during.  `recordGuardRefusalAfterRollback`
//      is only ever reached from a `catch`, after the refusing call has already
//      thrown and its transaction is gone.
//   3. The same correlation identity the attempt carried.  The caller generates
//      the correlation id BEFORE the attempt and passes the same value here.
//   4. A failure of this call must never change the outcome of the refusal.
//      Nothing in this module throws: every path returns a bounded result object,
//      and the caller rethrows the original refusal either way.
//
// Nothing here sends an email, a push or any other outward message, and nothing
// here touches Banking Pay, Draft, execution, cancellation, provider, settlement,
// recovery or remittance behaviour.

const MANAGED_ROOT_REFUSAL_SQLSTATE = '55000';
const MANAGED_ROOT_REFUSAL_MESSAGE = 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED';
const RECORDER_RPC = 'weekly_source_guard_refusal_record_after_rollback_v1';
// WP-14c's envelope: `refusals` is 1..50 under an explicit cardinality check,
// because `public.tsfin_write_snapshots_and_complete` (E26) can return several
// refusals in one call.
const MAX_REFUSALS = 50;
const MAX_CORRELATION_ID = 200;
const MAX_CALLER = 200;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/**
 * PostgREST surfaces a raised refusal as `{code, message, details, hint}` on the
 * thrown error's `json` member (see `sbRpc` in `broker/src/index.js`); a direct
 * PostgreSQL client surfaces the same facts as `code`, `message` and `detail` on
 * the error itself.  Both shapes are read, and neither is invented: a value that
 * is not present is reported absent rather than guessed.
 */
export function readRefusalFacts(error) {
  if (!error || typeof error !== 'object') return null;
  const body = (error.json && typeof error.json === 'object' && !Array.isArray(error.json))
    ? error.json
    : null;
  const sqlstate = String((body?.code ?? error.code) ?? '').trim();
  const message = String((body?.message ?? error.message) ?? '').trim();
  const detail = body
    ? (body.details ?? body.detail ?? null)
    : (error.detail ?? error.details ?? null);
  if (!sqlstate && !message) return null;
  return { sqlstate, message, detail: detail ?? null };
}

/**
 * True only for the managed-root rotation refusal WP-09b's sites raise.  The
 * owner itself checks the same two constants, so a refusal this predicate lets
 * through and the owner then rejects is a refused record, never a wrong one.
 */
export function isManagedRootGuardRefusal(error) {
  const facts = readRefusalFacts(error);
  if (!facts) return false;
  return facts.sqlstate === MANAGED_ROOT_REFUSAL_SQLSTATE
    && facts.message === MANAGED_ROOT_REFUSAL_MESSAGE;
}

function boundedText(value, maximum) {
  const text = String(value ?? '').trim();
  if (!text) return '';
  return text.length > maximum ? text.slice(0, maximum) : text;
}

function refusalEntries(error) {
  const facts = readRefusalFacts(error);
  if (!facts) return [];
  // The DETAIL is passed VERBATIM — as the JSON text PostgreSQL produced, or as
  // the already-parsed object.  WP-14c's owner accepts both and parses it
  // itself; this module never rebuilds it from the current database state.
  return [{
    sqlstate: facts.sqlstate,
    message: facts.message,
    detail: facts.detail,
  }];
}

/**
 * Record a managed-root guard refusal that has already raised and rolled back.
 *
 * Returns a bounded result object and NEVER throws, so a caller can write
 *
 *   catch (error) { await recordGuardRefusalAfterRollback({...}); throw error; }
 *
 * and be certain the refusal's own outcome is unchanged (WP-14c rule 4, proved
 * in its report section 6.4).
 */
export async function recordGuardRefusalAfterRollback({
  rpc,
  error,
  correlationId,
  caller,
  actorUserId = null,
  onRecordFailure = null,
} = {}) {
  if (typeof rpc !== 'function') {
    return { recorded: false, reason: 'RPC_UNAVAILABLE' };
  }
  if (!isManagedRootGuardRefusal(error)) {
    return { recorded: false, reason: 'NOT_A_MANAGED_ROOT_GUARD_REFUSAL' };
  }
  const correlation = boundedText(correlationId, MAX_CORRELATION_ID);
  if (!correlation) {
    return { recorded: false, reason: 'CORRELATION_ID_REQUIRED' };
  }
  const refusals = refusalEntries(error).slice(0, MAX_REFUSALS);
  if (refusals.length === 0) {
    return { recorded: false, reason: 'NO_REFUSAL_FACTS' };
  }
  const actor = String(actorUserId ?? '').trim().toLowerCase();
  const request = {
    correlation_id: correlation,
    caller: boundedText(caller, MAX_CALLER) || 'broker',
    actor_user_id: UUID_PATTERN.test(actor) ? actor : null,
    refusals,
  };
  try {
    const result = await rpc(RECORDER_RPC, { p_request: request }, { timeoutMs: 15_000 });
    return { recorded: true, correlation_id: correlation, result: result ?? null };
  } catch (recordError) {
    // Rule 4: log it and move on.  The refusal is unchanged either way.
    if (typeof onRecordFailure === 'function') {
      try {
        onRecordFailure({
          correlation_id: correlation,
          caller: request.caller,
          error_code: String(recordError?.json?.code ?? recordError?.code ?? '') || 'UNKNOWN',
        });
      } catch { /* a logger must not change the outcome either */ }
    }
    return { recorded: false, reason: 'RECORD_CALL_FAILED', correlation_id: correlation };
  }
}

/**
 * Wrap one attempt so that a managed-root guard refusal is recorded after the
 * rollback and then rethrown unchanged.  The correlation id is generated BEFORE
 * the attempt so the same value can be put into the attempt's own logging, which
 * is WP-14c rule 3.
 */
export async function withGuardRefusalRecording(
  { rpc, caller, actorUserId = null, correlationId = null, onRecordFailure = null },
  attempt,
) {
  const correlation = boundedText(correlationId, MAX_CORRELATION_ID)
    || `ws62:${globalThis.crypto?.randomUUID?.() ?? Date.now().toString(36)}`;
  try {
    return await attempt(correlation);
  } catch (error) {
    await recordGuardRefusalAfterRollback({
      rpc, error, correlationId: correlation, caller, actorUserId, onRecordFailure,
    });
    throw error;
  }
}

export const WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_AFTER_ROLLBACK_V1',
  owner: RECORDER_RPC,
  sqlstate: MANAGED_ROOT_REFUSAL_SQLSTATE,
  message: MANAGED_ROOT_REFUSAL_MESSAGE,
  maxRefusals: MAX_REFUSALS,
  separateTransaction: true,
  changesRefusalOutcome: false,
});
