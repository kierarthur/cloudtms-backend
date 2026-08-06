import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const worker = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');

function functionBody(name) {
  const match = new RegExp(`(?:async\\s+)?function\\s+${name}\\s*\\(`).exec(worker);
  const start = match?.index;
  assert.ok(Number.isInteger(start) && start >= 0, `${name} missing`);
  const boundaries = [
    worker.indexOf('\nfunction ', start + 10),
    worker.indexOf('\nasync function ', start + 10)
  ].filter((value) => value > start);
  const end = boundaries.length ? Math.min(...boundaries) : worker.length;
  return worker.slice(start, end);
}

function loadWakeHelper({ throwOnNudge = false } = {}) {
  const calls = [];
  const context = {
    String,
    Number,
    Math,
    Array,
    console: { warn: () => {} },
    logBankingPayWorkbenchDiag: (_env, event, payload) => calls.push({ kind: 'log', event, payload }),
    nudgeBankingPayWorkbenchDrain: (_env, ctx, options) => {
      calls.push({ kind: 'nudge', ctx, options });
      if (throwOnNudge) throw new Error('registration failed');
      return { ok: true, scheduled: true };
    }
  };
  vm.runInNewContext(
    `${functionBody('requestTimesheetLifecycleWorkbenchWake')}\nthis.requestWake = requestTimesheetLifecycleWorkbenchWake;`,
    context
  );
  return { requestWake: context.requestWake, calls };
}

function loadChangedCount() {
  const context = { String, Number, Math, Array, Set };
  vm.runInNewContext(
    `${functionBody('timesheetLifecycleMutationChangedCount')}\nthis.changedCount = timesheetLifecycleMutationChangedCount;`,
    context
  );
  return context.changedCount;
}

function loadCronContinuationClassifier() {
  const context = { String, Number, Math, Array, Set };
  vm.runInNewContext(
    `${functionBody('shouldContinueBankingPayWorkbenchAfterCron')}\nthis.classify = shouldContinueBankingPayWorkbenchAfterCron;`,
    context
  );
  return context.classify;
}

function loadBulkChunk({ results, failBookkeepingAt = -1 } = {}) {
  const queued = (Array.isArray(results) ? results : []).map((result, index) => ({
    id: `item-${index + 1}`,
    action: 'AUTHORISE',
    timesheet_id: `timesheet-${index + 1}`,
    current_timesheet_id: `timesheet-${index + 1}`,
    requested_timesheet_id: `timesheet-${index + 1}`,
    expected_timesheet_id: `timesheet-${index + 1}`,
    expected_row_signature: `signature-${index + 1}`,
    row_key: `timesheet:timesheet-${index + 1}`
  }));
  const pendingResults = [...(results || [])];
  const wakeCalls = [];
  const goldOptions = [];
  let bookkeepingIndex = 0;
  const context = {
    Date,
    Math,
    Number,
    Array,
    String,
    encodeURIComponent,
    fetchTimesheetLifecycleBulkOperation: async () => ({ id: 'operation-1', action: 'AUTHORISE' }),
    patchTimesheetLifecycleBulkOperation: async () => true,
    sbFetch: async () => ({ rows: queued }),
    claimTimesheetLifecycleBulkItem: async (_env, row) => row,
    callGoldTimesheetLifecycleActionForBulkItem: async (_env, _req, _action, _item, _actor, internalOptions) => {
      goldOptions.push(internalOptions);
      return pendingResults.shift();
    },
    completeTimesheetLifecycleBulkItem: async () => {
      const current = bookkeepingIndex++;
      if (current === failBookkeepingAt) throw new Error('bookkeeping failed');
      return true;
    },
    fetchTimesheetLifecycleBulkOperationItems: async () => [],
    summarizeTimesheetLifecycleBulkOperation: () => ({
      ok: true,
      batch_completed: true,
      requested_count: queued.length,
      success_count: results.filter((row) => row?.success === true).length,
      failure_count: results.filter((row) => row?.success !== true).length,
      pending_count: 0,
      stale_count: 0,
      more_due: false,
      all_success: results.every((row) => row?.success === true),
      has_failures: results.some((row) => row?.success !== true)
    }),
    lifecycleBulkNowIso: () => '2026-08-06T12:00:00.000Z',
    requestTimesheetLifecycleWorkbenchWake: (_env, ctx, options) => {
      if (Number(options.lifecycleChangedCount) <= 0) return false;
      wakeCalls.push({ ctx, options });
      return true;
    }
  };
  const symbolMatch = worker.match(/const TIMESHEET_LIFECYCLE_INTERNAL_CALL = Symbol\([^\n]+\);/);
  assert.ok(symbolMatch, 'private lifecycle capability symbol missing');
  vm.runInNewContext(
    `${symbolMatch[0]}\n${functionBody('drainTimesheetLifecycleBulkOperationChunk')}\n` +
      `this.runChunk = drainTimesheetLifecycleBulkOperationChunk;`,
    context
  );
  return { runChunk: context.runChunk, wakeCalls, goldOptions };
}

test('post-commit wake helper requests one bounded global nudge only when lifecycle truth changed', () => {
  const { requestWake, calls } = loadWakeHelper();
  const ctx = { waitUntil() {} };

  assert.equal(requestWake({}, ctx, {
    action: 'AUTHORISE',
    sourceRoute: 'TEST_BULK_CHUNK',
    lifecycleChangedCount: 0
  }), false);
  assert.equal(calls.length, 0);

  assert.equal(requestWake({}, ctx, {
    action: 'AUTHORISE',
    sourceRoute: 'TEST_BULK_CHUNK',
    lifecycleChangedCount: 10,
    actorUserId: 'actor',
    timesheetId: 'timesheet'
  }), true);

  const nudges = calls.filter((entry) => entry.kind === 'nudge');
  assert.equal(nudges.length, 1);
  assert.equal(nudges[0].ctx, ctx);
  assert.equal(nudges[0].options.origin, 'TIMESHEET_AUTHORISE');
  assert.equal(nudges[0].options.budgetProfile, 'NUDGE');
  assert.equal(nudges[0].options.source_route, 'TEST_BULK_CHUNK');
  assert.equal('candidateIds' in nudges[0].options, false);
  assert.equal('timesheetIds' in nudges[0].options, false);
});

test('post-commit wake helper never converts a completed lifecycle mutation into an HTTP failure', () => {
  const { requestWake, calls } = loadWakeHelper({ throwOnNudge: true });
  assert.equal(requestWake({}, null, {
    action: 'UNAUTHORISE',
    sourceRoute: 'TEST_INDIVIDUAL',
    lifecycleChangedCount: 1
  }), false);
  assert.equal(calls.filter((entry) => entry.kind === 'nudge').length, 1);
});

test('mutation change classifier wakes for confirmed and ambiguous successes but not explicit no-op or failure', () => {
  const changedCount = loadChangedCount();
  assert.equal(changedCount({ affected_timesheet_ids: ['a', 'a', 'b'] }), 2);
  assert.equal(changedCount({ success_count: 3 }), 3);
  assert.equal(changedCount({ results: [{ success: true }, { ok: true }, { success: false }] }), 2);
  assert.equal(changedCount({ ok: true }), 1);
  assert.equal(changedCount({ ok: true, no_change: true }), 0);
  assert.equal(changedCount({ ok: false, success_count: 0 }), 0);
});

test('internal bulk suppression is capability-based and cannot be requested through browser-shaped data', () => {
  const symbolMatch = worker.match(/const TIMESHEET_LIFECYCLE_INTERNAL_CALL = Symbol\([^\n]+\);/);
  assert.ok(symbolMatch, 'private lifecycle capability symbol missing');
  const context = {};
  vm.runInNewContext(
    `${symbolMatch[0]}\n${functionBody('timesheetLifecycleWorkbenchWakeSuppressed')}\n` +
      `this.check = timesheetLifecycleWorkbenchWakeSuppressed;\n` +
      `this.trusted = { [TIMESHEET_LIFECYCLE_INTERNAL_CALL]: true, suppressWorkbenchNudge: true };`,
    context
  );
  assert.equal(context.check({ suppressWorkbenchNudge: true, suppress_workbench_nudge: true }), false);
  assert.equal(context.check(context.trusted), true);
});

test('selected bulk paths suppress per-item wakes and own exactly one post-commit wake', () => {
  const gold = functionBody('callGoldTimesheetLifecycleActionForBulkItem');
  assert.match(gold, /handleTimesheetAuthoriseGeneric\(env, subReq, timesheetId, internalOptions\?\.ctx \|\| null, internalOptions\)/);
  assert.match(gold, /handleTimesheetUnauthorise\(env, subReq, timesheetId, internalOptions\?\.ctx \|\| null, internalOptions\)/);
  assert.match(gold, /lifecycle_changed: success && !explicitNoChange/);

  const chunk = functionBody('drainTimesheetLifecycleBulkOperationChunk');
  assert.match(chunk, /\[TIMESHEET_LIFECYCLE_INTERNAL_CALL\]: true/);
  assert.match(chunk, /suppressWorkbenchNudge: true/);
  assert.match(chunk, /if \(result\?\.lifecycle_changed === true\) lifecycleChangedCount \+= 1;/);
  assert.match(chunk, /finally \{[\s\S]*requestTimesheetLifecycleWorkbenchWake/);
  assert.equal((chunk.match(/requestTimesheetLifecycleWorkbenchWake\(/g) || []).length, 1);
  assert.match(chunk, /lifecycle_changed_count: lifecycleChangedCount/);
  assert.match(chunk, /workbench_wake_requested: workbenchWakeRequested/);

  const selected = functionBody('handleTimesheetLifecycleBulkActionRequest');
  assert.match(selected, /TIMESHEET_LIFECYCLE_SELECTED_CORRECTION_PAIR/);
  assert.match(selected, /TIMESHEET_LIFECYCLE_SELECTED_SINGLE/);
  assert.match(selected, /\[TIMESHEET_LIFECYCLE_INTERNAL_CALL\]: true/);
});

test('ten committed bulk items produce one chunk-owned wake and no per-item wake ownership', async () => {
  const ctx = { waitUntil() {} };
  const harness = loadBulkChunk({
    results: Array.from({ length: 10 }, () => ({ ok: true, success: true, lifecycle_changed: true }))
  });
  const summary = await harness.runChunk({}, {}, 'operation-1', { maxItems: 10, ctx, actorUserId: 'actor' });

  assert.equal(summary.lifecycle_changed_count, 10);
  assert.equal(summary.workbench_wake_requested, true);
  assert.equal(harness.wakeCalls.length, 1);
  assert.equal(harness.wakeCalls[0].ctx, ctx);
  assert.equal(harness.wakeCalls[0].options.lifecycleChangedCount, 10);
  assert.equal(harness.goldOptions.length, 10);
  for (const internalOptions of harness.goldOptions) {
    assert.equal(internalOptions.suppressWorkbenchNudge, true);
    assert.equal(internalOptions.ctx, ctx);
    assert.equal(Object.getOwnPropertySymbols(internalOptions).length, 1);
  }
});

test('partial-success chunk wakes once while all-failed chunk does not nudge', async () => {
  const partial = loadBulkChunk({
    results: [
      { ok: true, success: true, lifecycle_changed: true },
      { ok: false, success: false, lifecycle_changed: false },
      { ok: true, success: true, lifecycle_changed: true }
    ]
  });
  const partialSummary = await partial.runChunk({}, {}, 'operation-1', { maxItems: 3 });
  assert.equal(partialSummary.lifecycle_changed_count, 2);
  assert.equal(partial.wakeCalls.length, 1);

  const failed = loadBulkChunk({
    results: [
      { ok: false, success: false, lifecycle_changed: false },
      { ok: false, success: false, lifecycle_changed: false }
    ]
  });
  const failedSummary = await failed.runChunk({}, {}, 'operation-1', { maxItems: 2 });
  assert.equal(failedSummary.lifecycle_changed_count, 0);
  assert.equal(failedSummary.workbench_wake_requested, false);
  assert.equal(failed.wakeCalls.length, 0);
});

test('chunk requests its wake even when operation-item bookkeeping fails after lifecycle commit', async () => {
  const harness = loadBulkChunk({
    results: [{ ok: true, success: true, lifecycle_changed: true }],
    failBookkeepingAt: 0
  });
  await assert.rejects(
    harness.runChunk({}, {}, 'operation-1', { maxItems: 1 }),
    /bookkeeping failed/
  );
  assert.equal(harness.wakeCalls.length, 1);
  assert.equal(harness.wakeCalls[0].options.lifecycleChangedCount, 1);
});

test('individual and correction-pair gold paths preserve one wake unless trusted bulk suppression is present', () => {
  const authorise = functionBody('handleTimesheetAuthoriseGeneric');
  const unauthorise = functionBody('handleTimesheetUnauthorise');

  for (const source of [authorise, unauthorise]) {
    assert.match(source, /internalOptions = null/);
    assert.match(source, /timesheetLifecycleWorkbenchWakeSuppressed\(internalOptions\)/);
    assert.match(source, /requestTimesheetLifecycleWorkbenchWake\(env, ctx/);
  }
  assert.match(authorise, /TIMESHEET_AUTHORISE_CORRECTION_PAIR/);
  assert.match(unauthorise, /TIMESHEET_UNAUTHORISE_CORRECTION_PAIR/);
});

test('router supplies execution context to all lifecycle wake-owning routes', () => {
  assert.match(worker, /handleBulkTimesheetAuthoriseSelected\(env, req, ctx\)/);
  assert.match(worker, /handleBulkTimesheetUnauthoriseSelected\(env, req, ctx\)/);
  assert.match(worker, /handleContractWeekManualAuthorise\(env, req, m\.id, ctx\)/);
  assert.match(functionBody('handleTimesheetLifecycleBulkOperationDrain'), /ctx,[\s\S]*actorUserId: user\?\.id \|\| null/);
});

test('cron requests existing bounded continuation only for safe progress with more due', () => {
  const classify = loadCronContinuationClassifier();
  assert.equal(classify({ ok: true, more_due: true, made_progress: true }), true);
  assert.equal(classify({ ok: true, more_due: true, processed_count: 1 }), true);
  assert.equal(classify({ ok: true, more_due: false, made_progress: true }), false);
  assert.equal(classify({ ok: true, more_due: true, made_progress: false, processed_count: 0 }), false);
  assert.equal(classify({ ok: false, more_due: true, made_progress: true }), false);
  assert.equal(classify({ ok: true, more_due: true, made_progress: true, stop_reason: 'RPC_ERROR' }), false);

  assert.match(worker, /shouldContinueBankingPayWorkbenchAfterCron\(workbenchCronRes\)/);
  assert.match(worker, /origin: 'SCHEDULED_BANKING_PAY_WORKBENCH_CRON_CONTINUATION'/);
  assert.match(worker, /budgetProfile: 'NUDGE'/);
});
