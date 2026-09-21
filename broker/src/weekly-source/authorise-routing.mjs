// Weekly Source Plan 6.2 — WP-23.
// Gate 13 hostile finance review, finding F2 (CRITICAL).
//
// `public.weekly_source_first_authorise_v1` is the ONLY writer of
// `public.weekly_source_root_authorisations`, and it had no caller: the Office
// Authorise control and Bulk Authorise both posted straight to the ordinary
// `timesheet_authorise_generic_atomic`.  The reviewer executed the consequence —
// no real week ever becomes a managed root, so the publication coordinator's
// pointer update raises `WEEKLY_SOURCE_PUBLICATION_ROOT_AUTHORISATION_POINTER_FAILED`
// for every later-source or protected-hours publication, and the certified
// "£120 entitlement over £100 settled leaves the ordinary £20" scenario is
// unreachable in production.
//
// This module holds the routing DECISION.  The branch that acts on it lives in
// the real Office Authorise route, `handleTimesheetAuthoriseGeneric` in
// `broker/src/index.js`, which Bulk Authorise also reaches through
// `callGoldTimesheetLifecycleActionForBulkItem` — so one branch serves both, and
// no new parallel route is introduced beside the one the Office actually clicks.
//
// WHAT DECIDES.  The database, not the broker.  The Weekly Source Office
// presentation owner reports `applicable`, which is true exactly when the Weekly
// Source lifecycle view model mounts for the week: WEEKLY scope, HOURS line
// type, a Contract, and the Client in an active Weekly Source group valid for
// that week ending date.  None of that rule is reproduced here.
//
// FAIL CLOSED.  Where applicability cannot be established the authorise is
// refused rather than sent to the ordinary owner, because authorising a Weekly
// Source week through the ordinary owner is exactly the silent defect F2
// reports: it succeeds, writes no generation, and leaves the week permanently
// unpublishable.  `SOURCE_CHECK_IN_PROGRESS` is the presentation owner's own
// refusal for a source-authority week with no current publication and is
// surfaced unchanged rather than reinterpreted.
//
// This module changes no database owner, adds no privilege, and decides no
// money value.

export const WEEKLY_SOURCE_AUTHORISE_PROBE_RPC = 'weekly_source_office_timesheet_presentation_v1';
export const WEEKLY_SOURCE_FIRST_AUTHORISE_RPC = 'weekly_source_first_authorise_v1';

/** PostgREST returns a scalar; the broker's callers unwrap the same three shapes. */
export function weeklySourceUnwrapRpc(value, functionName) {
  let payload = value;
  if (Array.isArray(payload) && payload.length === 1) payload = payload[0];
  if (payload && typeof payload === 'object' && !Array.isArray(payload)
      && Object.prototype.hasOwnProperty.call(payload, functionName)) {
    payload = payload[functionName];
  }
  if (Array.isArray(payload) && payload.length === 1) payload = payload[0];
  return payload;
}

function rpcErrorText(error) {
  const body = (error && typeof error === 'object' && error.json && typeof error.json === 'object')
    ? error.json
    : null;
  return `${String(body?.code || error?.code || '')} `
    + `${String(body?.message || error?.message || '')} `
    + `${String(body?.details || body?.detail || '')}`;
}

/**
 * Decide whether an Authorise must go to the Weekly Source first-authorisation
 * wrapper.
 *
 * @param {(fn: string, args: object, options?: object) => Promise<unknown>} rpc
 * @returns {Promise<{bound: boolean, refusal: null|{status:number,error_code:string,message:string}, presentation?: object|null}>}
 */
export async function weeklySourceAuthoriseRouting(rpc, timesheetId, actorUserId) {
  if (typeof rpc !== 'function' || !timesheetId || !actorUserId) {
    return { bound: false, refusal: null };
  }
  let payload;
  try {
    payload = weeklySourceUnwrapRpc(
      await rpc(
        WEEKLY_SOURCE_AUTHORISE_PROBE_RPC,
        { p_request: { actor_user_id: actorUserId, timesheet_id: timesheetId } },
        { timeoutMs: 12000 },
      ),
      WEEKLY_SOURCE_AUTHORISE_PROBE_RPC,
    );
  } catch (error) {
    const text = rpcErrorText(error);
    if (/SOURCE_CHECK_IN_PROGRESS/i.test(text)) {
      return {
        bound: true,
        refusal: {
          status: 409,
          error_code: 'SOURCE_CHECK_IN_PROGRESS',
          message: 'This week is checked against the client system and the check has not '
            + 'completed. It cannot be authorised yet.',
        },
      };
    }
    // Not a current, unrevoked, unarchived Timesheet: the ordinary owner refuses
    // it for its own reasons and no Weekly Source generation can belong to it.
    if (/WEEKLY_SOURCE_TIMESHEET_NOT_FOUND/i.test(text)) return { bound: false, refusal: null };
    return {
      bound: false,
      refusal: {
        status: 503,
        error_code: 'WEEKLY_SOURCE_AUTHORISE_ROUTING_UNAVAILABLE',
        message: 'This timesheet cannot be authorised right now because CloudTMS could not '
          + 'establish whether it is a Weekly source week. Try again shortly.',
      },
    };
  }
  return { bound: payload?.applicable === true, refusal: null, presentation: payload || null };
}

export const WEEKLY_SOURCE_AUTHORISE_ROUTING_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_AUTHORISE_ROUTING_V1',
  probe: WEEKLY_SOURCE_AUTHORISE_PROBE_RPC,
  wrapper: WEEKLY_SOURCE_FIRST_AUTHORISE_RPC,
  ordinaryOwnerUnchanged: true,
  failsClosedWhenUndetermined: true,
});
