import test from 'node:test';
import assert from 'node:assert/strict';

import {
  createImportReviewPostCommitRunner,
  reconcileTimesheetQueryDeliveryAfterProviderAcceptance
} from '../src/import-review-follow-up.js';

const IMPORT_ID = '20000000-0000-4000-8000-000000000002';
const OPERATION_ID = '30000000-0000-4000-8000-000000000003';
const ACTOR_ID = '10000000-0000-4000-8000-000000000001';
const TIMESHEET_ID = '60000000-0000-4000-8000-000000000006';
const AUTO_AUTHORISE_TIMESHEET_ID = '70000000-0000-4000-8000-000000000007';
const ACTION_ID = 'a'.repeat(64);
const HASH = 'b'.repeat(64);
const COMMITTED_AT_UTC = '2026-07-22T22:00:00.000Z';

function details(applyResult = {}) {
  return {
    importId: IMPORT_ID,
    operationId: OPERATION_ID,
    actorUserId: ACTOR_ID,
    requestHash: HASH,
    applyResult
  };
}

function createScenario(storedResponse, options = {}) {
  const calls = [];
  const tsfinRuns = [];
  let tsfinSummaryCall = 0;
  let authoriseAttempt = 0;
  const sbRpc = async (_env, name, args) => {
    calls.push({ name, args });
    if (name === 'import_review_apply_status_get_v1') {
      return {
        ok: true,
        outcome: 'COMMITTED_WITH_FOLLOW_UP_PENDING',
        follow_up_status: options.followUpStatus || 'PENDING',
        stored_response: storedResponse,
        committed_at_utc: COMMITTED_AT_UTC
      };
    }
    if (name === 'timesheet_query_email_enqueue_v1' && options.emailEnqueueFails) {
      throw new Error('provider detail must not escape');
    }
    if (name === 'tsfin_follow_up_target_summary_v1') {
      const targetCount = Array.isArray(args.p_timesheet_ids) ? args.p_timesheet_ids.length : 0;
      const sequence = Array.isArray(options.tsfinTargetSummaries)
        ? options.tsfinTargetSummaries
        : [
            { settled: false, pendingTotal: 0 },
            { settled: true, pendingTotal: 0 }
          ];
      const selected = sequence[Math.min(tsfinSummaryCall, sequence.length - 1)] || {};
      tsfinSummaryCall += 1;
      const settled = selected.settled === true;
      return {
        ok: true,
        target_count: targetCount,
        current_target_count: targetCount,
        fresh_target_count: settled ? targetCount : 0,
        pending_total: Number(selected.pendingTotal || 0),
        all_targets_fresh: settled,
        all_targets_settled: settled
      };
    }
    if (name === 'timesheet_authorise_bulk_atomic') {
      authoriseAttempt += 1;
      if (options.reauthoriseThrowsOnce && authoriseAttempt === 1) {
        const error = new Error('cannot find parent statement on pldbgapi2 call stack');
        error.status = 500;
        error.code = 'XX000';
        throw error;
      }
      const requestedIds = (Array.isArray(args.p_items) ? args.p_items : [])
        .map((item) => item.timesheet_id);
      if (options.reauthoriseFails) {
        return {
          ok: true,
          all_success: false,
          success_count: 0,
          results: [{ requested_timesheet_id: TIMESHEET_ID, success: false, error_code: 'AUTHORISE_NOT_ALLOWED' }]
        };
      }
      return {
        ok: true,
        all_success: true,
        success_count: requestedIds.length,
        results: requestedIds.map((timesheetId) => ({ requested_timesheet_id: timesheetId, success: true }))
      };
    }
    if (name === 'import_review_follow_up_component_update_v1') {
      return { ok: true, component: args.p_component, component_status: args.p_new_component_status };
    }
    return { ok: true };
  };
  const runTsfinWorkerOnce = async (_env, input) => {
    tsfinRuns.push(input);
    if (options.tsfinThrows) throw new Error('tsfin detail must not escape');
    return {
      picked: options.tsfinPicked ?? 1,
      ok: 1,
      fail: 0,
      completed_timesheet_ids: options.tsfinCompletedTimesheetIds || []
    };
  };
  const runner = createImportReviewPostCommitRunner({
    sbRpc,
    unwrapRpcJsonb: (value) => value,
    runTsfinWorkerOnce,
    wait: async () => {}
  });
  return { runner, calls, tsfinRuns };
}

test('email enqueue failure after commit records only EMAIL as FAILED_RETRYABLE', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [ACTION_ID],
    affected_timesheet_ids: [],
    review_email_follow_up_status: 'PENDING',
    review_tsfin_follow_up_status: 'NOT_REQUIRED'
  }, { emailEnqueueFails: true });

  await assert.rejects(
    current.runner({}, details()),
    /IMPORT_REVIEW_FOLLOW_UP_FAILED_RETRYABLE/
  );

  const update = current.calls.find((call) => (
    call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'EMAIL'
  ));
  assert.equal(update.args.p_expected_component_status, 'PENDING');
  assert.equal(update.args.p_new_component_status, 'FAILED_RETRYABLE');
  assert.equal(update.args.p_error_code, 'EMAIL_ENQUEUE_FAILED');
  assert.equal(update.args.p_error_message, 'Query email enqueue failed after source commit and can be retried safely.');
  assert.equal(current.calls.some((call) => call.name.endsWith('_apply_transactional')), false);
  assert.equal(current.tsfinRuns.length, 0);
});

test('retry resets the failed email component and re-enqueues without source apply', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [ACTION_ID],
    affected_timesheet_ids: [],
    review_email_follow_up_status: 'FAILED_RETRYABLE',
    review_tsfin_follow_up_status: 'NOT_REQUIRED'
  }, { followUpStatus: 'FAILED_RETRYABLE' });

  await current.runner({}, details({
    // Browser/initial-result values are not trusted when committed DB evidence exists.
    post_commit_email_action_ids: ['untrusted'],
    affected_timesheet_ids: ['untrusted']
  }));

  const names = current.calls.map((call) => call.name);
  const reset = current.calls.find((call) => (
    call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'EMAIL'
  ));
  assert.equal(reset.args.p_expected_component_status, 'FAILED_RETRYABLE');
  assert.equal(reset.args.p_new_component_status, 'PENDING');
  assert.equal(current.calls.find((call) => call.name === 'timesheet_query_email_enqueue_v1').args.p_selected_action_ids[0], ACTION_ID);
  assert.equal(names.some((name) => name.endsWith('_apply_transactional')), false);
  assert.equal(names.filter((name) => name === 'timesheet_query_email_enqueue_v1').length, 1);
});

test('email failure does not prevent independent TSFIN completion', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [ACTION_ID],
    affected_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'PENDING',
    review_tsfin_follow_up_status: 'PENDING'
  }, { emailEnqueueFails: true });

  await assert.rejects(current.runner({}, details()), /IMPORT_REVIEW_FOLLOW_UP_FAILED_RETRYABLE/);

  assert.equal(current.tsfinRuns.length, 1);
  const updates = current.calls
    .filter((call) => call.name === 'import_review_follow_up_component_update_v1')
    .map((call) => [call.args.p_component, call.args.p_new_component_status]);
  assert.deepEqual(updates, [
    ['EMAIL', 'FAILED_RETRYABLE'],
    ['TSFIN', 'COMPLETE']
  ]);
  assert.equal(current.calls.some((call) => call.name.endsWith('_apply_transactional')), false);
});

test('both failed components are reset before either side effect is retried', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [ACTION_ID],
    affected_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'FAILED_RETRYABLE',
    review_tsfin_follow_up_status: 'FAILED_RETRYABLE'
  }, { followUpStatus: 'FAILED_RETRYABLE' });

  await current.runner({}, details());

  const sequence = current.calls.map((call) => (
    call.name === 'import_review_follow_up_component_update_v1'
      ? `${call.args.p_component}:${call.args.p_new_component_status}`
      : call.name
  ));
  const emailReset = sequence.indexOf('EMAIL:PENDING');
  const tsfinReset = sequence.indexOf('TSFIN:PENDING');
  const enqueue = sequence.indexOf('timesheet_query_email_enqueue_v1');
  assert.ok(emailReset > 0 && tsfinReset > emailReset && enqueue > tsfinReset);
  assert.ok(sequence.includes('TSFIN:COMPLETE'));
});

test('authorised source timesheet is restored only after TSFIN completes', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID],
    post_commit_reauthorise_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  });

  await current.runner({}, details());

  const names = current.calls.map((call) => call.name);
  const initialSummary = names.indexOf('tsfin_follow_up_target_summary_v1');
  const wake = names.indexOf('enqueue_ts_financials_priority');
  const summaryIndexes = names.map((name, index) => name === 'tsfin_follow_up_target_summary_v1' ? index : -1).filter((index) => index >= 0);
  const authorise = names.indexOf('timesheet_authorise_bulk_atomic');
  const complete = current.calls.findIndex((call) => call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'TSFIN' && call.args.p_new_component_status === 'COMPLETE');
  const preAuthoriseSummary = summaryIndexes.find((index) => index > wake && index < authorise);
  const postAuthoriseSummary = summaryIndexes.find((index) => index > authorise);
  assert.ok(initialSummary >= 0 && wake > initialSummary && preAuthoriseSummary > wake
    && authorise > preAuthoriseSummary && postAuthoriseSummary > authorise && complete > postAuthoriseSummary);
  assert.deepEqual(current.calls[wake].args, {
    _timesheet_ids: [TIMESHEET_ID],
    _reason: 'CONTEXT_CHANGED'
  });
  assert.deepEqual(current.calls[authorise].args.p_items, [{
    timesheet_id: TIMESHEET_ID,
    expected_timesheet_id: TIMESHEET_ID
  }]);
  assert.equal(names.some((name) => name.endsWith('_apply_transactional')), false);
});

test('one transient idempotent authorisation failure is retried without source reapply', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID],
    post_commit_reauthorise_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  }, {
    tsfinTargetSummaries: [{ settled: true, pendingTotal: 0 }],
    reauthoriseThrowsOnce: true
  });

  await current.runner({}, details());

  assert.equal(current.calls.filter((call) => call.name === 'timesheet_authorise_bulk_atomic').length, 2);
  assert.ok(current.calls.some((call) => call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'TSFIN' && call.args.p_new_component_status === 'COMPLETE'));
  assert.equal(current.calls.some((call) => call.name.endsWith('_apply_transactional')), false);
});

test('post-authorisation TSFIN is settled before follow-up completes', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID],
    post_commit_reauthorise_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  }, {
    tsfinTargetSummaries: [
      { settled: true, pendingTotal: 0 },
      { settled: false, pendingTotal: 1 },
      { settled: true, pendingTotal: 0 }
    ]
  });

  await current.runner({}, details());

  assert.equal(current.tsfinRuns.length, 1);
  assert.equal(current.calls.filter((call) => call.name === 'tsfin_follow_up_target_summary_v1').length, 3);
  assert.ok(current.calls.some((call) => call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'TSFIN' && call.args.p_new_component_status === 'COMPLETE'));
});

test('a concurrently leased TSFIN target completes without a false retryable failure', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID],
    post_commit_reauthorise_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  }, {
    tsfinTargetSummaries: [
      { settled: false, pendingTotal: 0 },
      { settled: false, pendingTotal: 1 },
      { settled: true, pendingTotal: 0 }
    ],
    tsfinPicked: 0
  });

  await current.runner({}, details());
  assert.ok(current.calls.some((call) => call.name === 'timesheet_authorise_bulk_atomic'));
  assert.ok(current.calls.some((call) => call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'TSFIN' && call.args.p_new_component_status === 'COMPLETE'));
});

test('a stale or missing TSFIN snapshot remains retryable', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  }, {
    tsfinTargetSummaries: [{ settled: false, pendingTotal: 1 }],
    tsfinPicked: 0
  });

  await assert.rejects(current.runner({}, details()), /IMPORT_REVIEW_FOLLOW_UP_FAILED_RETRYABLE/);
  const failure = current.calls.find((call) => call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'TSFIN');
  assert.equal(failure.args.p_error_code, 'TSFIN_FOLLOW_UP_INCOMPLETE');
  assert.equal(current.calls.some((call) => call.name === 'timesheet_authorise_bulk_atomic'), false);
});

test('already-settled exact targets are authorised without re-enqueueing TSFIN', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID],
    post_commit_reauthorise_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  }, {
    tsfinTargetSummaries: [{ settled: true, pendingTotal: 0 }]
  });

  await current.runner({}, details());

  assert.equal(current.tsfinRuns.length, 0);
  assert.equal(current.calls.some((call) => call.name === 'enqueue_ts_financials_priority'), false);
  assert.ok(current.calls.some((call) => call.name === 'timesheet_authorise_bulk_atomic'));
  assert.ok(current.calls.some((call) => call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'TSFIN' && call.args.p_new_component_status === 'COMPLETE'));
});

test('configured auto-authorise targets and lifecycle restoration targets share one bounded post-TSFIN call', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID, AUTO_AUTHORISE_TIMESHEET_ID],
    post_commit_reauthorise_timesheet_ids: [TIMESHEET_ID],
    auto_authorise_timesheet_ids: [TIMESHEET_ID, AUTO_AUTHORISE_TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  });

  await current.runner({}, details());

  const authorise = current.calls.find((call) => call.name === 'timesheet_authorise_bulk_atomic');
  assert.deepEqual(authorise.args.p_items, [
    { timesheet_id: TIMESHEET_ID, expected_timesheet_id: TIMESHEET_ID },
    { timesheet_id: AUTO_AUTHORISE_TIMESHEET_ID, expected_timesheet_id: AUTO_AUTHORISE_TIMESHEET_ID }
  ]);
  assert.equal(current.calls.some((call) => call.name.endsWith('_apply_transactional')), false);
});

test('reauthorisation failure is retryable and never repeats source apply', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [TIMESHEET_ID],
    post_commit_reauthorise_timesheet_ids: [TIMESHEET_ID],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'PENDING'
  }, { reauthoriseFails: true });

  await assert.rejects(current.runner({}, details()), /IMPORT_REVIEW_FOLLOW_UP_FAILED_RETRYABLE/);
  const failure = current.calls.find((call) => call.name === 'import_review_follow_up_component_update_v1'
    && call.args.p_component === 'TSFIN');
  assert.equal(failure.args.p_new_component_status, 'FAILED_RETRYABLE');
  assert.equal(failure.args.p_error_code, 'TSFIN_AUTHORISE_FAILED');
  assert.equal(current.calls.some((call) => call.name.endsWith('_apply_transactional')), false);
});

test('uncommitted source prevents every follow-up mutation', async () => {
  const calls = [];
  const runner = createImportReviewPostCommitRunner({
    sbRpc: async (_env, name) => {
      calls.push(name);
      return { ok: true, outcome: 'IN_PROGRESS', stored_response: {} };
    },
    unwrapRpcJsonb: (value) => value,
    runTsfinWorkerOnce: async () => { throw new Error('must not run'); }
  });

  await assert.rejects(runner({}, details()), /IMPORT_REVIEW_FOLLOW_UP_SOURCE_NOT_COMMITTED/);
  assert.deepEqual(calls, ['import_review_apply_status_get_v1']);
});

test('terminal components return after status proof without unrelated follow-up work', async () => {
  const current = createScenario({
    post_commit_email_action_ids: [],
    affected_timesheet_ids: [],
    review_email_follow_up_status: 'NOT_REQUIRED',
    review_tsfin_follow_up_status: 'COMPLETE'
  }, { followUpStatus: 'COMPLETE' });

  const result = await current.runner({}, details());
  assert.equal(result.source_committed, true);
  assert.deepEqual(current.calls.map((call) => call.name), ['import_review_apply_status_get_v1']);
  assert.equal(current.tsfinRuns.length, 0);
});

test('provider-accepted marker failure runs reconciliation and exposes no resend path', async () => {
  let markCalls = 0;
  let reconcileCalls = 0;
  const result = await reconcileTimesheetQueryDeliveryAfterProviderAcceptance({
    row: { type: 'TIMESHEET_QUERY', id: OPERATION_ID, status: 'SENT' },
    markDelivery: async () => {
      markCalls += 1;
      throw new Error('history marker unavailable');
    },
    reconcileDelivery: async () => { reconcileCalls += 1; }
  });

  assert.equal(markCalls, 1);
  assert.equal(reconcileCalls, 1);
  assert.deepEqual(result, {
    applicable: true,
    history_marked: false,
    reconcile_attempted: true,
    reconcile_completed: true,
    error_code: 'TIMESHEET_QUERY_DELIVERY_MARK_FAILED'
  });
  assert.equal(Object.keys(result).some((key) => /send|enqueue|provider/i.test(key)), false);
});
