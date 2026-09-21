// Weekly Source pending-publication release worker (Gate 5, item G5-7).
//
// `proof/32 §2` "Invocation point": the existing `runWeeklySourceDelivery(...)`
// tick in `broker/src/weekly-source/delivery-runtime.mjs`, **after**
// `weekly_source_query_scheduler_tick_v1` and after every candidate/manager
// delivery step, inside its own `try/catch`; each claimed bundle is applied in
// its own call and its own `try/catch`. A failing bundle records its failure on
// that bundle only and never aborts the tick, the other bundles or the delivery
// work that already ran.
//
// `proof/32 §3`: every minute tick claims one bounded page and re-runs the
// freeze census for each bundle. There is no Banking Pay callback, which is why
// the periodic reconciliation is mandatory.
//
// HANDOVER 2 round-4 ruling 6 point 7: "Any token disagreement, unexpected
// scope, invalidation failure, receipt failure or timeout - including retryable
// `55P03` - rolls back the entire release transaction." The rollback discards
// any counter increment made inside it, so a thrown apply is followed by one
// separate, fresh-transaction call to the failure recorder, on that bundle
// only. That call is itself wrapped, so a failure to record can never abort the
// tick either.
//
// This module decides nothing. Every bound, every clamp, every state
// transition, the census, the locks and the publication live in the database
// owners; the worker only claims a page, applies each bundle once, and records
// a rolled-back transaction.

// HANDOVER 2 round-5 ruling B4.1: "Only genuinely transient technical errors
// consume the retry budget." The database decides which those are. This module
// reports two FACTS about a failed apply and classifies neither of them:
//
//   * `sqlstate`     - the five-character SQLSTATE PostgreSQL returned, if any;
//   * `failure_kind` - what this module observed from outside the database:
//                      TIMEOUT (the request was aborted before an answer came
//                      back), NETWORK (the request never reached an answer),
//                      DATABASE_ERROR (the database answered with an error) or
//                      UNKNOWN.
//
// A client-side timeout carries no SQLSTATE at all and is not proof that the
// server transaction rolled back (WP-08b review F6), so reporting it as a fact
// is what lets the database keep it on the retry budget instead of escalating a
// slow-but-valid release to a human.
const WORKER_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const BOUNDED_CODE_RE = /^[A-Z][A-Z0-9_]{2,119}$/;
const SQLSTATE_RE = /^[0-9A-Za-z]{5}$/;
const FAILURE_KINDS = Object.freeze(['TIMEOUT', 'NETWORK', 'DATABASE_ERROR', 'UNKNOWN']);
const MAX_PAGE = 25;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function unwrap(value, functionName) {
  let result = value;
  if (Array.isArray(result) && result.length === 1) [result] = result;
  if (result && typeof result === 'object' && !Array.isArray(result)
      && Object.prototype.hasOwnProperty.call(result, functionName)) {
    result = result[functionName];
  }
  if (Array.isArray(result) && result.length === 1) [result] = result;
  return result;
}

async function rpc(dependencies, functionName, request, timeoutMs = 30_000) {
  const result = await dependencies.rpc(functionName, { p_request: request }, { timeoutMs });
  return unwrap(result, functionName);
}

// Only a bounded, non-identifying code ever leaves this module. No payload, no
// row, no connection string and no secret is ever carried into the summary.
function boundedErrorCode(error) {
  const candidate = text(error?.message).toUpperCase();
  return BOUNDED_CODE_RE.test(candidate) ? candidate : 'WEEKLY_SOURCE_RELEASE_APPLY_FAILED';
}

// A fact, not a decision: the SQLSTATE the database returned, or null when this
// module never got one. Nothing here maps a SQLSTATE to a meaning.
function boundedSqlState(error) {
  for (const candidate of [error?.code, error?.sqlstate, error?.sqlState,
                           error?.cause?.code, error?.body?.code]) {
    const value = text(candidate);
    if (SQLSTATE_RE.test(value)) return value.toUpperCase();
  }
  return null;
}

// A fact, not a decision: what this module observed. A SQLSTATE means the
// database answered; an abort means the request was cut short; a failure with
// neither means it never completed.
function boundedFailureKind(error) {
  if (boundedSqlState(error)) return 'DATABASE_ERROR';
  const name = text(error?.name).toUpperCase();
  const message = text(error?.message).toUpperCase();
  if (name === 'ABORTERROR' || name === 'TIMEOUTERROR'
      || message.includes('TIMEOUT') || message.includes('ABORT')) {
    return 'TIMEOUT';
  }
  if (name === 'TYPEERROR' || name === 'FETCHERROR'
      || message.includes('FETCH') || message.includes('NETWORK')
      || message.includes('ECONNRESET') || message.includes('ECONNREFUSED')
      || message.includes('SOCKET')) {
    return 'NETWORK';
  }
  return 'UNKNOWN';
}

function outcomeOf(result) {
  if (!result || typeof result !== 'object') return 'UNKNOWN';
  if (result.replayed === true) return 'REPLAYED';
  if (result.released === true) return 'RELEASED';
  if (typeof result.outcome === 'string' && result.outcome) return result.outcome;
  if (result.ok === false && typeof result.code === 'string') return result.code;
  return 'UNKNOWN';
}

async function applyOneBundle(dependencies, workerId, workerRunId, bundle) {
  const pendingBundleId = text(bundle?.pending_bundle_id);
  const record = {
    pending_bundle_id: pendingBundleId,
    outcome: 'UNKNOWN',
    released: false,
    code: null,
    failure_recorded: false,
    // B4.1: reported, never interpreted here.
    sqlstate: null,
    failure_kind: null,
    refusal_disposition: null,
  };
  if (!pendingBundleId) {
    record.outcome = 'WEEKLY_SOURCE_RELEASE_CLAIM_ROW_INVALID';
    record.code = record.outcome;
    return record;
  }
  try {
    const applied = await rpc(
      dependencies,
      'weekly_source_pending_entitlement_release_apply_v1',
      {
        pending_bundle_id: pendingBundleId,
        expected_pending_revision: bundle.pending_revision,
        expected_request_digest: bundle.request_digest,
        worker_id: workerId,
        lease_token: bundle.lease_token,
        worker_run_id: workerRunId,
      },
    );
    record.outcome = outcomeOf(applied);
    record.released = applied?.released === true;
    record.code = typeof applied?.code === 'string' ? applied.code : null;
    // B4.1: the database's own disposition, carried back for the tick summary.
    record.refusal_disposition = typeof applied?.refusal_disposition === 'string'
      ? applied.refusal_disposition : null;
    return record;
  } catch (error) {
    // The release transaction rolled back. Ruling 6 point 7: record it as a
    // failure on this bundle only, in a fresh transaction. Round-5 ruling B4.1:
    // whether that failure spends the retry budget or reaches a human now
    // depends on the SQLSTATE and the observed failure kind, and the DATABASE
    // makes that call -- this module only reports the two facts.
    record.outcome = 'TRANSACTION_ROLLED_BACK';
    record.code = boundedErrorCode(error);
    record.sqlstate = boundedSqlState(error);
    record.failure_kind = boundedFailureKind(error);
    try {
      const recorded = await rpc(
        dependencies,
        'weekly_source_pending_entitlement_release_record_failure_v1',
        {
          pending_bundle_id: pendingBundleId,
          lease_token: bundle.lease_token,
          worker_id: workerId,
          worker_run_id: workerRunId,
          code: record.code,
          detail: 'WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK',
          ...(record.sqlstate ? { sqlstate: record.sqlstate } : {}),
          failure_kind: FAILURE_KINDS.includes(record.failure_kind)
            ? record.failure_kind : 'UNKNOWN',
        },
      );
      record.failure_recorded = recorded?.recorded === true;
      record.refusal_disposition = typeof recorded?.refusal_disposition === 'string'
        ? recorded.refusal_disposition : null;
    } catch {
      // A failure to record is not allowed to abort the tick either. The lease
      // expires and the next tick reclaims the bundle (proof/32 section 10).
      record.failure_recorded = false;
    }
    return record;
  }
}

export async function runPendingEntitlementRelease(dependencies, options = {}) {
  const workerId = text(options.workerId);
  const workerRunId = text(options.workerRunId) || crypto.randomUUID();
  if (!WORKER_ID_RE.test(workerId)) {
    return {
      ok: false,
      error_code: 'WEEKLY_SOURCE_RELEASE_WORKER_ID_INVALID',
      claimed: 0,
      bundles: [],
    };
  }

  // The server clamps the page and the lease; the worker asks for the ordinary
  // values and never assumes its own numbers were honoured (proof/32 section 2).
  const claim = await rpc(
    dependencies,
    'weekly_source_pending_entitlement_release_claim_page_v1',
    {
      worker_id: workerId,
      worker_run_id: workerRunId,
      lease_seconds: Number(options.leaseSeconds ?? 120),
      limit: Number(options.limit ?? MAX_PAGE),
    },
  );

  const claimed = Array.isArray(claim?.bundles) ? claim.bundles : [];
  const bundles = [];
  let released = 0;
  let replayed = 0;
  let frozen = 0;
  let skipped = 0;
  let manualReview = 0;
  let superseded = 0;
  let failed = 0;

  // Each bundle in its own call and its own try/catch. One failing bundle never
  // stops the others.
  for (const bundle of claimed.slice(0, MAX_PAGE)) {
    // eslint-disable-next-line no-await-in-loop
    const record = await applyOneBundle(dependencies, workerId, workerRunId, bundle);
    bundles.push(record);
    if (record.outcome === 'RELEASED') released += 1;
    else if (record.outcome === 'REPLAYED') replayed += 1;
    else if (record.outcome === 'FROZEN') frozen += 1;
    else if (record.outcome === 'SKIPPED_THIS_TICK') skipped += 1;
    else if (record.outcome === 'MANUAL_REVIEW') manualReview += 1;
    else if (record.outcome === 'SUPERSEDED') superseded += 1;
    else failed += 1;
  }

  return {
    ok: true,
    worker_id: workerId,
    worker_run_id: workerRunId,
    lease_seconds: Number(claim?.lease_seconds ?? 0),
    limit: Number(claim?.limit ?? 0),
    claimed: Number(claim?.claimed_count ?? claimed.length),
    released,
    replayed,
    frozen,
    skipped,
    manual_review: manualReview,
    superseded,
    failed,
    bundles,
    // HANDOVER 2 round-5 ruling B4.2: the bounded frozen watch runs inside the
    // claim page's own transaction, so the tick reports its counters without
    // performing or deciding anything. `unchanged_frozen` bundles performed no
    // state transition at all and so wrote no audit row; `escalated_to_claim`
    // ones were handed to the ordinary path and appear in `bundles`.
    watch: {
      polled: Number(claim?.watch?.polled ?? 0),
      unchanged_frozen: Number(claim?.watch?.unchanged_frozen ?? 0),
      escalated_to_claim: Number(claim?.watch?.escalated_to_claim ?? 0),
    },
  };
}

export const pendingEntitlementReleaseWorkerInternals = Object.freeze({
  applyOneBundle,
  boundedErrorCode,
  boundedSqlState,
  boundedFailureKind,
  outcomeOf,
  MAX_PAGE,
  FAILURE_KINDS,
  WORKER_ID_RE,
});
