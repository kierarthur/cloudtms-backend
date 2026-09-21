import assert from 'node:assert/strict';
import test from 'node:test';

import {
  orchestrateWeeklySourceFinalisation,
  recoverWeeklySourceFinalisationPayProjection,
  WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT,
  WeeklySourceFinalisationPayError,
} from '../../broker/src/weekly-source/finalisation-pay-orchestrator.mjs';

const ID = Object.freeze({
  actor: 'fa000000-0000-4000-8000-000000000001',
  cycle: 'fa000000-0000-4000-8000-000000000002',
  upload: 'fa000000-0000-4000-8000-000000000003',
  publication: 'fa000000-0000-4000-8000-000000000004',
  revision: 'fa000000-0000-4000-8000-000000000005',
  run: 'fa000000-0000-4000-8000-000000000006',
  taskA: 'fa000000-0000-4000-8000-000000000007',
  taskB: 'fa000000-0000-4000-8000-000000000008',
  rootA: 'fa000000-0000-4000-8000-000000000009',
  rootB: 'fa000000-0000-4000-8000-00000000000a',
  clientManifestA: 'fa000000-0000-4000-8000-00000000000b',
  clientManifestB: 'fa000000-0000-4000-8000-00000000000c',
  receiptA: 'fa000000-0000-4000-8000-00000000000d',
  receiptB: 'fa000000-0000-4000-8000-00000000000e',
});

const HASH = Object.freeze({
  rows: '1'.repeat(64),
  comparison: '2'.repeat(64),
  issues: '3'.repeat(64),
  run: '4'.repeat(64),
  manifest: '5'.repeat(64),
  taskA: '6'.repeat(64),
  taskB: '7'.repeat(64),
  receiptA: '8'.repeat(64),
  receiptB: '9'.repeat(64),
});

const finaliseRequest = (overrides = {}) => ({
  actor_user_id: ID.actor,
  source_cycle_id: ID.cycle,
  authority_scope_kind: 'CYCLE',
  report_scope_id: null,
  upload_id: ID.upload,
  projection_publication_id: ID.publication,
  expected_authority_scope_version: 3,
  expected_row_manifest_hash: HASH.rows,
  expected_comparison_manifest_hash: HASH.comparison,
  expected_issue_set_hash: HASH.issues,
  ...overrides,
});

function rootContext(rootId) {
  return {
    schema_version: 'WEEKLY_SOURCE_FINALISATION_PAY_ROOT_CONTEXT_V1',
    final_revision_id: ID.revision,
    source_cycle_id: ID.cycle,
    client_manifest_id: rootId === ID.rootA ? ID.clientManifestA : ID.clientManifestB,
    client_id: 'fa100000-0000-4000-8000-000000000001',
    root_timesheet_id: rootId,
    source_profile_kind: 'GENERIC_COMPLETE_SNAPSHOT',
    source_mode: 'HEALTHROSTER_WEEKLY',
    expected_segments: [{ work_date: '2026-09-14', hours_day: rootId === ID.rootA ? 7.5 : 8 }],
    expected_actual_schedule: [{ date: '2026-09-14', start: '09:00', end: '17:00', break_minutes: 30 }],
    expected_source_expenses: [],
    expected_rate_source_refs: {
      schema_version: 'WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
      root_timesheet_id: rootId,
      final_revision_id: ID.revision,
    },
  };
}

function task(id, rootId, ordinal) {
  return {
    task_id: id,
    task_ordinal: ordinal,
    client_manifest_id: rootId === ID.rootA ? ID.clientManifestA : ID.clientManifestB,
    root_timesheet_id: rootId,
    projection_idempotency_key: `weekly-source-finalisation-pay:${ID.revision}:${rootId}`,
    prepared_context_hash: rootId === ID.rootA ? HASH.taskA : HASH.taskB,
    root_context: rootContext(rootId),
    state: 'READY',
    attempt_count: 0,
    version: 1,
    projection_receipt_id: null,
    requires_recovery: false,
    action_required: false,
  };
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function refreshRun(run) {
  run.terminal_task_count = run.tasks.filter((entry) => [
    'PREPARED_FOR_AUTHORISATION',
    'PROPOSED',
    'NO_OP_FIRST_NEGATIVE',
    'TARGET_MANAGED_SUPPRESSED',
    'FAILED',
  ].includes(entry.state)).length;
  run.action_required_task_count = run.tasks.filter((entry) => [
    'FAILED',
  ].includes(entry.state)).length;
  if (run.tasks.some((entry) => ['SUBMISSION_STARTED', 'RECOVERY_REQUIRED'].includes(entry.state))) {
    run.state = 'RECOVERY_REQUIRED';
  } else if (run.action_required_task_count > 0) {
    run.state = 'ACTION_REQUIRED';
  } else if (run.terminal_task_count === run.task_count) {
    run.state = 'COMPLETE';
  } else {
    run.state = 'READY';
  }
}

function serviceSnapshot(root) {
  return {
    schema_version: 'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1',
    calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
    source_actual_schedule_json: root.expected_actual_schedule,
    tsfin_snapshot_json: {
      timesheet_id: root.root_timesheet_id,
      rate_source_refs_json: root.expected_rate_source_refs,
      invoice_breakdown_json: { mode: 'SEGMENTS', segments: root.expected_segments },
    },
  };
}

function harness(options = {}) {
  const calls = [];
  const tasks = (options.tasks ?? [task(ID.taskA, ID.rootA, 1)]).map(clone);
  const run = {
    ok: true,
    run_id: ID.run,
    final_revision_id: ID.revision,
    source_cycle_id: ID.cycle,
    state: tasks.length ? 'READY' : 'COMPLETE',
    task_count: tasks.length,
    terminal_task_count: 0,
    action_required_task_count: 0,
    task_manifest_hash: HASH.manifest,
    run_hash: HASH.run,
    tasks,
  };
  refreshRun(run);
  const receipts = new Map();
  const outcomes = options.outcomes ?? new Map();

  const dependencies = {
    calls,
    run,
    receipts,
    buildOrdinaryServiceSnapshot: async (input) => {
      calls.push(['BUILD_SNAPSHOT', clone(input)]);
      if (options.buildError) throw options.buildError;
      return serviceSnapshot(input.root_context);
    },
    dataRpc: async (name, args) => {
      calls.push([name, clone(args)]);
      if (name === 'weekly_source_finalise_atomic_v1') {
        if (options.finaliseError) throw options.finaliseError;
        return {
          ok: true,
          status: 'FINALISED',
          final_revision_id: ID.revision,
          source_cycle_id: ID.cycle,
          idempotent: options.finaliseReplay === true,
        };
      }
      if (name === 'weekly_source_finalisation_pay_open_atomic_v1') {
        if (options.openError) throw options.openError;
        return clone(run);
      }
      if (name === 'weekly_source_finalisation_pay_task_start_atomic_v1') {
        const request = args.p_request;
        const selected = run.tasks.find((entry) => entry.task_id === request.task_id);
        if (options.startError) {
          if (options.startCommittedBeforeError) {
            selected.state = 'SUBMISSION_STARTED';
            selected.attempt_count += 1;
            selected.version += 1;
            refreshRun(run);
          }
          throw options.startError;
        }
        assert.equal(selected.version, request.expected_task_version);
        selected.state = 'SUBMISSION_STARTED';
        selected.attempt_count += 1;
        selected.version += 1;
        run.state = 'RUNNING';
        return {
          ok: true,
          status: 'SUBMISSION_STARTED',
          idempotent_replay: false,
          final_revision_id: ID.revision,
          projection_request: {
            schema_version: 'WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1',
            actor_user_id: ID.actor,
            final_revision_id: ID.revision,
            root_timesheet_id: selected.root_timesheet_id,
            idempotency_key: selected.projection_idempotency_key,
          },
          task: clone(selected),
        };
      }
      if (name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1') {
        const request = args.p_request;
        if (options.projectionError) throw options.projectionError;
        const selected = run.tasks.find((entry) => entry.root_timesheet_id === request.root_timesheet_id);
        const outcome = outcomes.get(selected.task_id) ?? 'PREPARED_FOR_AUTHORISATION';
        const receipt = {
          ok: true,
          outcome,
          error_code: null,
          receipt_id: selected.task_id === ID.taskA ? ID.receiptA : ID.receiptB,
          receipt_hash: selected.task_id === ID.taskA ? HASH.receiptA : HASH.receiptB,
          final_revision_id: ID.revision,
          root_timesheet_id: selected.root_timesheet_id,
        };
        receipts.set(selected.projection_idempotency_key, receipt);
        return clone(receipt);
      }
      if (name === 'weekly_source_finalisation_pay_task_finish_atomic_v1') {
        const request = args.p_request;
        const selected = run.tasks.find((entry) => entry.task_id === request.task_id);
        const receipt = [...receipts.values()].find((entry) => entry.receipt_id === request.projection_receipt_id);
        selected.state = receipt.outcome;
        selected.projection_receipt_id = receipt.receipt_id;
        selected.version += 1;
        refreshRun(run);
        if (options.finishError) throw options.finishError;
        return clone(run);
      }
      if (name === 'weekly_source_finalisation_pay_task_unknown_atomic_v1') {
        const request = args.p_request;
        const selected = run.tasks.find((entry) => entry.task_id === request.task_id);
        if (selected.state === 'SUBMISSION_STARTED') {
          selected.state = 'RECOVERY_REQUIRED';
          selected.version += 1;
        }
        refreshRun(run);
        return clone(run);
      }
      if (name === 'weekly_source_finalisation_pay_task_recover_atomic_v1') {
        const request = args.p_request;
        const selected = run.tasks.find((entry) => entry.task_id === request.task_id);
        const receipt = receipts.get(selected.projection_idempotency_key);
        if (receipt) {
          selected.state = receipt.outcome;
          selected.projection_receipt_id = receipt.receipt_id;
          selected.version += 1;
        } else if (request.confirm_retry) {
          selected.state = 'READY';
          selected.version += 1;
        } else if (selected.state === 'SUBMISSION_STARTED') {
          selected.state = 'RECOVERY_REQUIRED';
          selected.version += 1;
        }
        refreshRun(run);
        return clone(run);
      }
      throw new Error(`Unexpected RPC ${name}`);
    },
  };
  return dependencies;
}

const execute = (dependencies, request = finaliseRequest()) => orchestrateWeeklySourceFinalisation({
  request,
  actor: { id: ID.actor, role: 'admin' },
  env: { marker: 'TEST' },
  ctx: { marker: 'REQUEST' },
  dependencies,
});

test('finalises source first and projects one server-built ordinary Timesheet snapshot exactly once', async () => {
  const dependencies = harness();
  const result = await execute(dependencies);
  assert.equal(result.status, 'FINALISED');
  assert.equal(result.source_finalised, true);
  assert.equal(result.invoice_authority_committed, true);
  assert.deepEqual(dependencies.calls.map(([name]) => name), [
    'weekly_source_finalise_atomic_v1',
    'weekly_source_finalisation_pay_open_atomic_v1',
    'BUILD_SNAPSHOT',
    'weekly_source_finalisation_pay_task_start_atomic_v1',
    'weekly_source_ordinary_pay_projection_apply_atomic_v1',
    'weekly_source_finalisation_pay_task_finish_atomic_v1',
  ]);
  const projection = dependencies.calls.find(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  ))[1].p_request;
  assert.deepEqual(Object.keys(projection).sort(), [
    'actor_user_id',
    'final_revision_id',
    'idempotency_key',
    'root_timesheet_id',
    'schema_version',
    'service_snapshot',
  ].sort());
  assert.equal(projection.idempotency_key, `weekly-source-finalisation-pay:${ID.revision}:${ID.rootA}`);
  assert.equal(result.ordinary_pay_projection.tasks[0].root_context, undefined);
  assert.equal(JSON.stringify(result).includes('expected_rate_source_refs'), false);
});

// Plan 6.2 Gate 2 replaces the superseded assertion this test used to make.
// A locked, paid or Draft-frozen root is no longer REFUSED_LOCKED and no longer
// makes the run ACTION_REQUIRED: the later-change path mutates nothing, so such
// a root simply gets a complete PROPOSED entitlement awaiting an Office
// decision, exactly like any other already-authorised root.  What still has to
// be true is that a mixed run drives every root and finishes cleanly.
test('drives every root when one is already authorised and one is not, and neither is refused', async () => {
  const outcomes = new Map([
    [ID.taskA, 'PROPOSED'],
    [ID.taskB, 'PREPARED_FOR_AUTHORISATION'],
  ]);
  const dependencies = harness({
    tasks: [task(ID.taskA, ID.rootA, 1), task(ID.taskB, ID.rootB, 2)],
    outcomes,
  });
  const result = await execute(dependencies);
  assert.equal(result.status, 'FINALISED');
  assert.equal(result.invoice_authority_committed, true);
  assert.equal(result.ordinary_pay_projection.terminal_task_count, 2);
  assert.equal(result.ordinary_pay_projection.action_required_task_count, 0);
  assert.equal(dependencies.calls.filter(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  )).length, 2);
});

test('rejects a projection receipt that still claims the superseded REFUSED_LOCKED outcome', async () => {
  const outcomes = new Map([[ID.taskA, 'REFUSED_LOCKED']]);
  const dependencies = harness({ outcomes });
  const result = await execute(dependencies);
  // An unrecognised outcome is an unknown projection result, so the run goes to
  // recovery rather than being silently treated as a terminal state.
  assert.equal(result.status, 'FINALISED_PAY_RECOVERY_REQUIRED');
});

test('records an unknown projection result and never retries it during normal replay', async () => {
  const error = Object.assign(new Error('connection ended after submission'), { code: 'UPSTREAM_UNKNOWN' });
  const dependencies = harness({ projectionError: error });
  const first = await execute(dependencies);
  assert.equal(first.status, 'FINALISED_PAY_RECOVERY_REQUIRED');
  assert.equal(dependencies.run.tasks[0].state, 'RECOVERY_REQUIRED');
  assert.equal(dependencies.calls.filter(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  )).length, 1);

  const second = await execute(dependencies);
  assert.equal(second.status, 'FINALISED_PAY_RECOVERY_REQUIRED');
  assert.equal(dependencies.calls.filter(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  )).length, 1);
});

test('explicit recovery consumes an already-durable receipt without reapplying projection', async () => {
  const dependencies = harness();
  const selected = dependencies.run.tasks[0];
  selected.state = 'RECOVERY_REQUIRED';
  selected.attempt_count = 1;
  selected.version = 3;
  refreshRun(dependencies.run);
  dependencies.receipts.set(selected.projection_idempotency_key, {
    ok: true,
    outcome: 'PREPARED_FOR_AUTHORISATION',
    receipt_id: ID.receiptA,
    receipt_hash: HASH.receiptA,
    final_revision_id: ID.revision,
    root_timesheet_id: ID.rootA,
  });

  const result = await recoverWeeklySourceFinalisationPayProjection({
    request: {
      actor_user_id: ID.actor,
      final_revision_id: ID.revision,
      run_id: ID.run,
      task_id: ID.taskA,
      expected_task_version: 3,
      confirm_retry: false,
    },
    actor: { id: ID.actor },
    dependencies,
  });
  assert.equal(result.status, 'FINALISED');
  assert.equal(dependencies.calls.some(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  )), false);
  assert.equal(dependencies.calls.some(([name]) => name === 'BUILD_SNAPSHOT'), false);
});

test('only explicit confirmed recovery rearms and reapplies the original idempotency key once', async () => {
  const dependencies = harness();
  const selected = dependencies.run.tasks[0];
  selected.state = 'RECOVERY_REQUIRED';
  selected.attempt_count = 1;
  selected.version = 4;
  refreshRun(dependencies.run);

  const result = await recoverWeeklySourceFinalisationPayProjection({
    request: {
      actor_user_id: ID.actor,
      final_revision_id: ID.revision,
      run_id: ID.run,
      task_id: ID.taskA,
      expected_task_version: 4,
      confirm_retry: true,
    },
    actor: { id: ID.actor },
    dependencies,
  });
  assert.equal(result.status, 'FINALISED');
  const projectionCalls = dependencies.calls.filter(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  ));
  assert.equal(projectionCalls.length, 1);
  assert.equal(
    projectionCalls[0][1].p_request.idempotency_key,
    `weekly-source-finalisation-pay:${ID.revision}:${ID.rootA}`,
  );
});

test('snapshot preparation failure occurs before submission checkpoint and preserves invoice authority', async () => {
  const dependencies = harness({
    buildError: Object.assign(new Error('calculator unavailable'), { code: 'CALCULATOR_UNAVAILABLE' }),
  });
  const result = await execute(dependencies);
  assert.equal(result.status, 'FINALISED_PAY_PREPARATION_REQUIRED');
  assert.equal(result.invoice_authority_committed, true);
  assert.equal(dependencies.run.tasks[0].state, 'READY');
  assert.equal(dependencies.calls.some(([name]) => (
    name === 'weekly_source_finalisation_pay_task_start_atomic_v1'
  )), false);
  assert.equal(dependencies.calls.some(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  )), false);
});

test('an unknown start outcome is treated as recovery-required and projection is not called', async () => {
  const dependencies = harness({
    startError: Object.assign(new Error('response lost'), { code: 'START_RESPONSE_UNKNOWN' }),
    startCommittedBeforeError: true,
  });
  const result = await execute(dependencies);
  assert.equal(result.status, 'FINALISED_PAY_RECOVERY_REQUIRED');
  assert.equal(result.invoice_authority_committed, true);
  assert.equal(dependencies.calls.some(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  )), false);
});

test('a lost finish response is reconciled from terminal state and does not claim recovery is required', async () => {
  const dependencies = harness({ finishError: Object.assign(new Error('response lost'), { code: 'FINISH_RESPONSE_UNKNOWN' }) });
  const result = await execute(dependencies);
  assert.equal(result.status, 'FINALISED');
  assert.equal(result.ordinary_pay_projection.state, 'COMPLETE');
  assert.equal(dependencies.calls.filter(([name]) => (
    name === 'weekly_source_ordinary_pay_projection_apply_atomic_v1'
  )).length, 1);
});

test('a finalisation call with unknown outcome is not followed by any pay operation', async () => {
  const dependencies = harness({
    finaliseError: Object.assign(new Error('finalisation response unknown'), { code: 'FINALISE_UNKNOWN' }),
  });
  await assert.rejects(() => execute(dependencies), /finalisation response unknown/);
  assert.deepEqual(dependencies.calls.map(([name]) => name), ['weekly_source_finalise_atomic_v1']);
});

test('a final source with no affected roots completes without building or projecting', async () => {
  const dependencies = harness({ tasks: [] });
  const result = await execute(dependencies);
  assert.equal(result.status, 'FINALISED');
  assert.equal(result.ordinary_pay_projection.task_count, 0);
  assert.equal(dependencies.calls.some(([name]) => name === 'BUILD_SNAPSHOT'), false);
});

test('rejects actor drift and browser-supplied extra fields before finalisation', async () => {
  const dependencies = harness();
  await assert.rejects(
    () => orchestrateWeeklySourceFinalisation({
      request: finaliseRequest({ gross_pay: 12000 }),
      actor: { id: ID.actor },
      dependencies,
    }),
    (error) => error instanceof WeeklySourceFinalisationPayError
      && error.code === 'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_FIELD',
  );
  await assert.rejects(
    () => orchestrateWeeklySourceFinalisation({
      request: finaliseRequest(),
      actor: { id: ID.rootA },
      dependencies,
    }),
    (error) => error instanceof WeeklySourceFinalisationPayError
      && error.code === 'WEEKLY_SOURCE_FINALISATION_PAY_ACTOR_MISMATCH',
  );
  assert.equal(dependencies.calls.length, 0);
});

test('published contract explicitly preserves the ordinary lifecycle and forbids hidden retry or bypass', () => {
  assert.equal(WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT.automaticRetryAfterUnknownOutcome, false);
  assert.equal(WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT.invoiceAuthorityDependsOnPayProjection, false);
  assert.equal(WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT.usesExistingOrdinaryTimesheetTsfinLifecycle, true);
  assert.equal(WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT.bypassesWorkbench, false);
  assert.equal(WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT.mutatesWorkbenchOrBankingPay, false);
  assert.equal(WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT.mutatesInvoices, false);
});
