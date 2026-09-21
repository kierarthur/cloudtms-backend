// WP-08b, item G5-7: the Weekly Source pending-publication release worker and
// its single call site in the delivery tick.
//
// Authority: `P:\proof\32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md`
// sections 2 (invocation point), 3 (wake-up contract), 10 (bounds) and 11
// (forbidden); HANDOVER 2 round-4 ruling 6 point 7. `R26` is the named scenario.

import assert from 'node:assert/strict';
import test from 'node:test';
import {
  runPendingEntitlementRelease,
  pendingEntitlementReleaseWorkerInternals,
} from '../../broker/src/weekly-source/pending-entitlement-release-worker.mjs';
import { runWeeklySourceDelivery } from '../../broker/src/weekly-source/delivery-runtime.mjs';

const DIGEST = 'a'.repeat(64);

function bundle(suffix, overrides = {}) {
  return {
    pending_bundle_id: `d0000000-0000-4000-8000-0000000000${suffix}`,
    decision_bundle_id: `d0000000-0000-4000-8000-0000000000b${suffix.slice(-1)}`,
    bundle_revision: 1,
    candidate_id: 'd0000000-0000-4000-8000-000000000003',
    pending_revision: 2,
    request_digest: DIGEST,
    lease_token: `d0000000-0000-4000-8000-0000000000e${suffix.slice(-1)}`,
    worker_run_id: 'd0000000-0000-4000-8000-00000000cc01',
    ...overrides,
  };
}

// A recording stub in the shape `broker/src/index.js` passes in:
// `rpc(functionName, args, options)` against PostgREST in the public schema.
function stubRpc(handlers) {
  const calls = [];
  return {
    calls,
    dependencies: {
      rpc: async (functionName, args) => {
        calls.push({ functionName, request: args?.p_request });
        const handler = handlers[functionName];
        if (typeof handler === 'function') return handler(args?.p_request, calls);
        if (handler !== undefined) return handler;
        return { ok: true };
      },
    },
  };
}

test('an invalid worker id is refused before the database is touched', async () => {
  const { calls, dependencies } = stubRpc({});
  const result = await runPendingEntitlementRelease(dependencies, { workerId: 'has spaces' });
  assert.equal(result.ok, false);
  assert.equal(result.error_code, 'WEEKLY_SOURCE_RELEASE_WORKER_ID_INVALID');
  assert.equal(calls.length, 0);
});

test('the worker claims one bounded page and applies each bundle exactly once', async () => {
  const applied = [];
  const { calls, dependencies } = stubRpc({
    weekly_source_pending_entitlement_release_claim_page_v1: () => ({
      ok: true,
      claimed_count: 2,
      limit: 25,
      lease_seconds: 120,
      bundles: [bundle('01'), bundle('02')],
    }),
    weekly_source_pending_entitlement_release_apply_v1: (request) => {
      applied.push(request.pending_bundle_id);
      return request.pending_bundle_id.endsWith('01')
        ? { ok: true, released: true, replayed: false, receipt: { id: 'r1' } }
        : { ok: true, released: false, outcome: 'FROZEN' };
    },
  });

  const result = await runPendingEntitlementRelease(dependencies, {
    workerId: 'weekly-source:release',
    workerRunId: 'd0000000-0000-4000-8000-00000000cc01',
  });

  assert.equal(result.ok, true);
  assert.equal(result.claimed, 2);
  assert.equal(result.released, 1);
  assert.equal(result.frozen, 1);
  assert.equal(result.failed, 0);
  assert.deepEqual(applied, [
    'd0000000-0000-4000-8000-000000000001',
    'd0000000-0000-4000-8000-000000000002',
  ]);
  // proof/32 section 2: no actor and no timestamp is ever sent by the caller.
  const applyCalls = calls.filter(
    (call) => call.functionName === 'weekly_source_pending_entitlement_release_apply_v1',
  );
  assert.equal(applyCalls.length, 2);
  for (const call of applyCalls) {
    assert.deepEqual(Object.keys(call.request).sort(), [
      'expected_pending_revision', 'expected_request_digest', 'lease_token',
      'pending_bundle_id', 'worker_id', 'worker_run_id',
    ]);
  }
  // The server owns the clamps; the worker reports back what it was given.
  const claimCall = calls.find(
    (call) => call.functionName === 'weekly_source_pending_entitlement_release_claim_page_v1',
  );
  assert.deepEqual(Object.keys(claimCall.request).sort(), [
    'lease_seconds', 'limit', 'worker_id', 'worker_run_id',
  ]);
  assert.equal(result.limit, 25);
  assert.equal(result.lease_seconds, 120);
});

test('R26: one failing bundle records its own failure and never stops the others', async () => {
  const recorded = [];
  const { dependencies } = stubRpc({
    weekly_source_pending_entitlement_release_claim_page_v1: () => ({
      ok: true,
      claimed_count: 3,
      bundles: [bundle('01'), bundle('02'), bundle('03')],
    }),
    weekly_source_pending_entitlement_release_apply_v1: (request) => {
      if (request.pending_bundle_id.endsWith('02')) {
        // A rolled-back release transaction, exactly as round-4 ruling 6
        // point 7 describes it (a retryable 55P03 lock timeout).
        throw new Error('WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK');
      }
      return { ok: true, released: true, replayed: false };
    },
    weekly_source_pending_entitlement_release_record_failure_v1: (request) => {
      recorded.push(request);
      return { ok: false, recorded: true, technical_failure_count: 1, state: 'PENDING' };
    },
  });

  const result = await runPendingEntitlementRelease(dependencies, {
    workerId: 'weekly-source:release',
    workerRunId: 'd0000000-0000-4000-8000-00000000cc01',
  });

  assert.equal(result.ok, true);
  assert.equal(result.claimed, 3);
  assert.equal(result.released, 2);
  assert.equal(result.failed, 1);
  // Exactly one failure, on exactly the failing bundle, with its own lease.
  assert.equal(recorded.length, 1);
  assert.equal(recorded[0].pending_bundle_id, 'd0000000-0000-4000-8000-000000000002');
  assert.equal(recorded[0].lease_token, 'd0000000-0000-4000-8000-0000000000e2');
  assert.equal(recorded[0].worker_run_id, 'd0000000-0000-4000-8000-00000000cc01');
  assert.equal(recorded[0].code, 'WEEKLY_SOURCE_PENDING_RELEASE_TRANSACTION_ROLLED_BACK');
  const failing = result.bundles.find(
    (row) => row.pending_bundle_id === 'd0000000-0000-4000-8000-000000000002',
  );
  assert.equal(failing.outcome, 'TRANSACTION_ROLLED_BACK');
  assert.equal(failing.failure_recorded, true);
  assert.equal(failing.released, false);
});

test('a failure to record a failure still cannot abort the tick', async () => {
  const { dependencies } = stubRpc({
    weekly_source_pending_entitlement_release_claim_page_v1: () => ({
      ok: true, claimed_count: 2, bundles: [bundle('01'), bundle('02')],
    }),
    weekly_source_pending_entitlement_release_apply_v1: (request) => {
      if (request.pending_bundle_id.endsWith('01')) {
        throw new Error('connect ECONNREFUSED 127.0.0.1:5432 while applying bundle');
      }
      return { ok: true, released: false, outcome: 'FROZEN' };
    },
    weekly_source_pending_entitlement_release_record_failure_v1: () => {
      throw new Error('WEEKLY_SOURCE_RELEASE_FAILURE_RECORD_UNAVAILABLE');
    },
  });

  const result = await runPendingEntitlementRelease(dependencies, {
    workerId: 'weekly-source:release',
  });
  assert.equal(result.ok, true);
  assert.equal(result.frozen, 1);
  assert.equal(result.bundles[0].failure_recorded, false);
  // A message that is not a bounded code never leaks into the summary.
  assert.equal(result.bundles[0].code, 'WEEKLY_SOURCE_RELEASE_APPLY_FAILED');
});

test('every database outcome is counted under its own name, and nothing is guessed', async () => {
  const outcomes = {
    '01': { ok: true, released: true, replayed: false },
    '02': { ok: true, released: true, replayed: true },
    '03': { ok: true, released: false, outcome: 'FROZEN' },
    '04': { ok: false, retryable: true, released: false, outcome: 'SKIPPED_THIS_TICK' },
    '05': { ok: false, released: false, outcome: 'MANUAL_REVIEW' },
    '06': { ok: false, released: false, outcome: 'SUPERSEDED' },
    '07': { ok: false, released: false, code: 'WEEKLY_SOURCE_RELEASE_LEASE_INVALID' },
  };
  const { dependencies } = stubRpc({
    weekly_source_pending_entitlement_release_claim_page_v1: () => ({
      ok: true,
      claimed_count: 7,
      bundles: Object.keys(outcomes).map((suffix) => bundle(suffix)),
    }),
    weekly_source_pending_entitlement_release_apply_v1: (request) =>
      outcomes[request.pending_bundle_id.slice(-2)],
  });
  const result = await runPendingEntitlementRelease(dependencies, {
    workerId: 'weekly-source:release',
  });
  assert.equal(result.released, 1);
  assert.equal(result.replayed, 1);
  assert.equal(result.frozen, 1);
  assert.equal(result.skipped, 1);
  assert.equal(result.manual_review, 1);
  assert.equal(result.superseded, 1);
  assert.equal(result.failed, 1);
  assert.equal(
    result.bundles.at(-1).outcome,
    'WEEKLY_SOURCE_RELEASE_LEASE_INVALID',
  );
});

test('the page the worker will apply is capped at the server maximum', async () => {
  const { dependencies } = stubRpc({
    weekly_source_pending_entitlement_release_claim_page_v1: () => ({
      ok: true,
      claimed_count: 40,
      bundles: Array.from({ length: 40 }, (_, index) =>
        bundle(String(index).padStart(2, '0'), {
          pending_bundle_id: `d0000000-0000-4000-8000-0000000${String(index).padStart(5, '0')}`,
        })),
    }),
    weekly_source_pending_entitlement_release_apply_v1: () => ({
      ok: true, released: false, outcome: 'FROZEN',
    }),
  });
  const result = await runPendingEntitlementRelease(dependencies, {
    workerId: 'weekly-source:release',
  });
  assert.equal(result.bundles.length, pendingEntitlementReleaseWorkerInternals.MAX_PAGE);
  assert.equal(pendingEntitlementReleaseWorkerInternals.MAX_PAGE, 25);
});

test('proof/32 section 2: the release step runs after the delivery work and cannot prevent it',
  async () => {
    const order = [];
    const dependencies = {
      rpc: async (functionName) => {
        order.push(functionName);
        if (functionName === 'weekly_source_pending_entitlement_release_claim_page_v1') {
          throw new Error('WEEKLY_SOURCE_PENDING_RELEASE_UNAVAILABLE');
        }
        return { ok: true };
      },
    };
    const result = await runWeeklySourceDelivery({}, dependencies, {
      workerId: 'weekly-source:tick',
      scheduledStartedAtUtc: '2026-09-17T21:00:00.000Z',
    });

    assert.equal(result.ok, true);
    assert.ok(result.scheduler, 'the query scheduler tick still ran');
    assert.ok(result.delivery, 'the delivery step still ran');
    assert.equal(result.pending_entitlement_release.ok, false);
    assert.equal(
      result.pending_entitlement_release.error_code,
      'WEEKLY_SOURCE_PENDING_RELEASE_UNAVAILABLE',
    );
    // The invocation point: after the scheduler tick and after every delivery
    // step, and exactly once.
    const releaseIndex = order.indexOf('weekly_source_pending_entitlement_release_claim_page_v1');
    assert.ok(releaseIndex > order.indexOf('weekly_source_query_scheduler_tick_v1'));
    assert.ok(releaseIndex > order.indexOf('weekly_source_message_dispatch_target_claim_v1'));
    assert.equal(releaseIndex, order.length - 1);
    assert.equal(
      order.filter((name) => name.startsWith('weekly_source_pending_entitlement_release')).length,
      1,
    );
  });

test('a healthy tick reports the release summary alongside the delivery summary', async () => {
  const dependencies = {
    rpc: async (functionName) => {
      if (functionName === 'weekly_source_pending_entitlement_release_claim_page_v1') {
        return { ok: true, claimed_count: 0, bundles: [], limit: 25, lease_seconds: 120 };
      }
      return { ok: true };
    },
  };
  const result = await runWeeklySourceDelivery({}, dependencies, {
    workerId: 'weekly-source:tick',
    scheduledStartedAtUtc: '2026-09-17T21:00:00.000Z',
  });
  assert.equal(result.pending_entitlement_release.ok, true);
  assert.equal(result.pending_entitlement_release.claimed, 0);
  assert.equal(result.pending_entitlement_release.released, 0);
});

// ---------------------------------------------------------------------------
// WP-08c: HANDOVER 2 round-5 ruling B4
// ---------------------------------------------------------------------------
// B4.1 makes the DATABASE decide whether a refusal is transient. The Worker's
// job is to report two facts - the SQLSTATE and the kind of failure it observed
// from outside the database - and to classify neither. B4.2 gives the tick a
// bounded frozen-watch summary it must surface without acting on it.

test('B4.1: the worker reports the SQLSTATE and the failure kind as facts, and decides nothing',
  async () => {
    const recorded = [];
    const error = Object.assign(new Error('deadlock detected'), { code: '40P01' });
    const { dependencies } = stubRpc({
      weekly_source_pending_entitlement_release_claim_page_v1: () => ({
        ok: true, claimed_count: 1, limit: 25, lease_seconds: 120, bundles: [bundle('01')],
      }),
      weekly_source_pending_entitlement_release_apply_v1: () => { throw error; },
      weekly_source_pending_entitlement_release_record_failure_v1: (request) => {
        recorded.push(request);
        return { ok: true, recorded: true, refusal_disposition: 'TRANSIENT' };
      },
    });
    const result = await runPendingEntitlementRelease(dependencies, { workerId: 'w' });
    assert.equal(recorded.length, 1);
    assert.equal(recorded[0].sqlstate, '40P01');
    assert.equal(recorded[0].failure_kind, 'DATABASE_ERROR');
    assert.equal(result.bundles[0].sqlstate, '40P01');
    assert.equal(result.bundles[0].failure_kind, 'DATABASE_ERROR');
    // The disposition came back FROM the database; the worker never computed it.
    assert.equal(result.bundles[0].refusal_disposition, 'TRANSIENT');
    const source = pendingEntitlementReleaseWorkerInternals.boundedFailureKind.toString()
      + pendingEntitlementReleaseWorkerInternals.boundedSqlState.toString();
    assert.ok(!/TRANSIENT|PERMANENT|MANUAL_REVIEW|40001|55P03|23514/.test(source),
      'the worker must carry no transient/permanent classification of its own');
  });

test('B4.1: a client timeout is reported as TIMEOUT with no SQLSTATE', async () => {
  const recorded = [];
  const abort = Object.assign(new Error('The operation was aborted'), { name: 'AbortError' });
  const { dependencies } = stubRpc({
    weekly_source_pending_entitlement_release_claim_page_v1: () => ({
      ok: true, claimed_count: 1, limit: 25, lease_seconds: 120, bundles: [bundle('01')],
    }),
    weekly_source_pending_entitlement_release_apply_v1: () => { throw abort; },
    weekly_source_pending_entitlement_release_record_failure_v1: (request) => {
      recorded.push(request);
      return { ok: true, recorded: true, refusal_disposition: 'TRANSIENT' };
    },
  });
  await runPendingEntitlementRelease(dependencies, { workerId: 'w' });
  assert.equal(recorded[0].failure_kind, 'TIMEOUT');
  assert.equal('sqlstate' in recorded[0], false);
});

test('B4.1: an error with neither a SQLSTATE nor a recognisable shape is reported UNKNOWN',
  async () => {
    const recorded = [];
    const { dependencies } = stubRpc({
      weekly_source_pending_entitlement_release_claim_page_v1: () => ({
        ok: true, claimed_count: 1, limit: 25, lease_seconds: 120, bundles: [bundle('01')],
      }),
      weekly_source_pending_entitlement_release_apply_v1: () => { throw new Error('WHAT'); },
      weekly_source_pending_entitlement_release_record_failure_v1: (request) => {
        recorded.push(request);
        return { ok: true, recorded: true, refusal_disposition: 'PERMANENT' };
      },
    });
    const result = await runPendingEntitlementRelease(dependencies, { workerId: 'w' });
    assert.equal(recorded[0].failure_kind, 'UNKNOWN');
    assert.equal('sqlstate' in recorded[0], false);
    // The database escalated it; the worker simply carries the answer back.
    assert.equal(result.bundles[0].refusal_disposition, 'PERMANENT');
  });

test('B4.1: only the four declared failure kinds ever leave this module', async () => {
  assert.deepEqual(pendingEntitlementReleaseWorkerInternals.FAILURE_KINDS,
    ['TIMEOUT', 'NETWORK', 'DATABASE_ERROR', 'UNKNOWN']);
  const kinds = [
    Object.assign(new Error('x'), { code: '23514' }),
    Object.assign(new Error('fetch failed'), { name: 'TypeError' }),
    Object.assign(new Error('ECONNRESET'), {}),
    new Error('anything else'),
  ].map(pendingEntitlementReleaseWorkerInternals.boundedFailureKind);
  assert.deepEqual(kinds, ['DATABASE_ERROR', 'NETWORK', 'NETWORK', 'UNKNOWN']);
  assert.equal(pendingEntitlementReleaseWorkerInternals.boundedSqlState(
    Object.assign(new Error('x'), { code: 'not-a-sqlstate' })), null);
});

test('B4.2: the tick reports the bounded frozen-watch counters and acts on none of them',
  async () => {
    const { calls, dependencies } = stubRpc({
      weekly_source_pending_entitlement_release_claim_page_v1: () => ({
        ok: true,
        claimed_count: 0,
        limit: 25,
        lease_seconds: 120,
        bundles: [],
        watch: { ok: true, polled: 3, unchanged_frozen: 2, escalated_to_claim: 1, bundles: [] },
      }),
    });
    const result = await runPendingEntitlementRelease(dependencies, { workerId: 'w' });
    assert.deepEqual(result.watch, { polled: 3, unchanged_frozen: 2, escalated_to_claim: 1 });
    // The watch is the claim page's own work, in its own transaction: the worker
    // makes no extra call for it and applies nothing.
    assert.deepEqual(calls.map((call) => call.functionName),
      ['weekly_source_pending_entitlement_release_claim_page_v1']);
    assert.equal(result.claimed, 0);
    assert.equal(result.released, 0);
  });

test('B4.2: a tick against a database with no watch summary still reports zeroes', async () => {
  const { dependencies } = stubRpc({
    weekly_source_pending_entitlement_release_claim_page_v1: () => ({
      ok: true, claimed_count: 0, limit: 25, lease_seconds: 120, bundles: [],
    }),
  });
  const result = await runPendingEntitlementRelease(dependencies, { workerId: 'w' });
  assert.deepEqual(result.watch, { polled: 0, unchanged_frozen: 0, escalated_to_claim: 0 });
});
