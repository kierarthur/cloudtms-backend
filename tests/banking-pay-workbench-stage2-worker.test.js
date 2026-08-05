import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const worker = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');

function functionBody(name) {
  const markers = [`function ${name}`, `async function ${name}`];
  const start = markers
    .map((marker) => worker.indexOf(marker))
    .filter((value) => value >= 0)
    .sort((a, b) => a - b)[0];
  assert.ok(Number.isInteger(start) && start >= 0, `${name} missing`);
  const boundaries = [
    worker.indexOf('\nfunction ', start + 10),
    worker.indexOf('\nasync function ', start + 10)
  ].filter((value) => value > start);
  const end = boundaries.length ? Math.min(...boundaries) : worker.length;
  return worker.slice(start, end);
}

function loadLaneAttempt() {
  const context = {
    Date,
    Math,
    Number,
    String,
    Boolean,
    Object,
    Array,
    Set,
    JSON,
    Error
  };
  vm.runInNewContext(
    `${functionBody('runBankingPayWorkbenchSourceBuildLaneAttempt')}\nthis.runAttempt = runBankingPayWorkbenchSourceBuildLaneAttempt;`,
    context
  );
  return context.runAttempt;
}

const ids = {
  job: '11111111-1111-4111-8111-111111111111',
  build: '22222222-2222-4222-8222-222222222222',
  candidate: '33333333-3333-4333-8333-333333333333',
  attempt: '44444444-4444-4444-8444-444444444444',
  nonce: '55555555-5555-4555-8555-555555555555'
};

function claim(overrides = {}) {
  return {
    ok: true,
    claimed: true,
    job_id: ids.job,
    build_id: ids.build,
    candidate_id: ids.candidate,
    private_stage: 'WORKSPACE_FACT',
    attempt_id: ids.attempt,
    attempt_number: 2,
    attempt_nonce: ids.nonce,
    attempt_started_at_utc: '2026-08-05T12:00:00.000Z',
    lease_expires_at_utc: '2026-08-05T12:00:25.000Z',
    execution_profile_version: 1,
    ...overrides
  };
}

function execution(overrides = {}) {
  return {
    ok: true,
    processed: true,
    job_id: ids.job,
    build_id: ids.build,
    private_stage: 'WORKSPACE_FACT',
    attempt_number: 2,
    stage_status: 'COMPLETED',
    continuation_enqueued: true,
    has_more: true,
    next_cursor_json: {
      cursor_kind: 'WORKSPACE_FACT',
      dependency_unit_key: 'UNIT:1',
      fact_family: 'ENTITLEMENT_COMPONENT',
      page_number: 2
    },
    result_code: 'FACT_PAGE_COMMITTED',
    elapsed_ms: 25,
    ...overrides
  };
}

function baseOptions(rpcCall, remainingRuntimeMs = () => 20000) {
  return {
    rpcCall,
    remainingRuntimeMs,
    workerId: 'worker:test',
    laneIdentity: 'lane:test',
    leaseSeconds: 25,
    candidateId: ids.candidate,
    claimReserveMs: 1000,
    executeTimeoutMs: 7000,
    rpcSafetyBufferMs: 1000
  };
}

test('one valid claim delivers exactly one exact execute and returns no nonce', async () => {
  const calls = [];
  const runAttempt = loadLaneAttempt();
  const result = await runAttempt({}, baseOptions(async (_env, fn, args, options) => {
    calls.push({ fn, args, options });
    return calls.length === 1 ? claim() : execution();
  }));

  assert.equal(calls.length, 2);
  assert.equal(calls[0].fn, 'pay_workbench_source_build_attempt_claim_start_v1');
  assert.equal(calls[0].options.timeoutMs, 1000);
  assert.equal(calls[1].fn, 'pay_workbench_source_build_attempt_execute_v1');
  assert.equal(calls[1].options.timeoutMs, 7000);
  assert.deepEqual(JSON.parse(JSON.stringify(calls[1].args)), {
    p_job_id: ids.job,
    p_build_id: ids.build,
    p_private_stage: 'WORKSPACE_FACT',
    p_attempt_id: ids.attempt,
    p_attempt_nonce: ids.nonce,
    p_worker_id: 'worker:test',
    p_lane_identity: 'lane:test'
  });
  assert.equal(result.ok, true);
  assert.equal(result.processed, true);
  assert.equal(result.cursor_kind, 'WORKSPACE_FACT');
  assert.equal(result.page_number, 2);
  assert.doesNotMatch(JSON.stringify(result), new RegExp(ids.nonce));
  assert.equal(Object.prototype.hasOwnProperty.call(result, 'next_cursor_json'), false);
});

test('no claim stops after RPC 1', async () => {
  let calls = 0;
  const result = await loadLaneAttempt()({}, baseOptions(async () => {
    calls += 1;
    return { ok: true, claimed: false };
  }));
  assert.equal(calls, 1);
  assert.equal(result.claimed, false);
  assert.equal(result.execute_called, false);
  assert.equal(result.result_code, 'NO_CLAIM');
});

test('uncertain or malformed claim never invokes RPC 2', async () => {
  for (const mode of ['throw', 'malformed']) {
    let calls = 0;
    const result = await loadLaneAttempt()({}, baseOptions(async () => {
      calls += 1;
      if (mode === 'throw') throw new Error('claim transport uncertain');
      return claim({ attempt_nonce: undefined });
    }));
    assert.equal(calls, 1);
    assert.equal(result.execute_called, false);
    assert.equal(result.transport_ok, false);
    assert.match(result.result_code, /CLAIM_(UNCERTAIN|RESPONSE_INVALID)/);
  }
});

test('insufficient preflight budget performs no RPC', async () => {
  let calls = 0;
  const result = await loadLaneAttempt()({}, baseOptions(async () => {
    calls += 1;
    return claim();
  }, () => 10999));
  assert.equal(calls, 0);
  assert.equal(result.skipped, true);
  assert.equal(result.result_code, 'SOURCE_BUILD_TWO_CALL_BUDGET_INSUFFICIENT');
  assert.equal(result.minimum_before_claim_ms, 11000);
});

test('runtime loss after RPC 1 leaves the durable attempt for recovery and skips RPC 2', async () => {
  let calls = 0;
  const budgets = [20000, 0];
  const result = await loadLaneAttempt()({}, baseOptions(async () => {
    calls += 1;
    return claim();
  }, () => budgets.shift() ?? 0));
  assert.equal(calls, 1);
  assert.equal(result.claimed, true);
  assert.equal(result.execute_called, false);
  assert.equal(result.result_code, 'SOURCE_BUILD_ATTEMPT_EXECUTE_BUDGET_DISAPPEARED');
  assert.doesNotMatch(JSON.stringify(result), new RegExp(ids.nonce));
});

test('execute transport loss is not retried and any nonce in an error is redacted', async () => {
  let calls = 0;
  const result = await loadLaneAttempt()({}, baseOptions(async () => {
    calls += 1;
    if (calls === 1) return claim();
    throw new Error(`timeout for ${ids.nonce}`);
  }));
  assert.equal(calls, 2);
  assert.equal(result.execute_called, true);
  assert.equal(result.transport_ok, false);
  assert.equal(result.result_code, 'SOURCE_BUILD_ATTEMPT_EXECUTE_UNCERTAIN');
  assert.match(result.error_message, /\[redacted\]/);
  assert.doesNotMatch(JSON.stringify(result), new RegExp(ids.nonce));
});

test('database-owned stage failure is a valid terminal response and is not retried', async () => {
  let calls = 0;
  const result = await loadLaneAttempt()({}, baseOptions(async () => {
    calls += 1;
    return calls === 1
      ? claim()
      : execution({ ok: false, stage_status: 'FAILED', continuation_enqueued: false, has_more: false, result_code: 'STAGE_ERROR' });
  }));
  assert.equal(calls, 2);
  assert.equal(result.transport_ok, true);
  assert.equal(result.ok, false);
  assert.equal(result.processed, true);
  assert.equal(result.result_code, 'STAGE_ERROR');
});

test('source-build burst uses the two-call helper while preserving parallel settlement and normal fairness', () => {
  const drain = functionBody('drainBankingPayWorkbenchJobs');
  const sourceStart = drain.indexOf('const runSourceBuildParallelBursts');
  const sourceEnd = drain.indexOf('\n  while (stopReason === null)', sourceStart);
  const sourceBurst = drain.slice(sourceStart, sourceEnd);
  assert.match(sourceBurst, /callSourceBuildTwoCall/);
  assert.match(sourceBurst, /Promise\.allSettled/);
  assert.doesNotMatch(sourceBurst, /callDispatcher\(/);
  assert.match(drain, /base\.filter\(\(jobType\) => jobType !== 'WORKBENCH_CANDIDATE_SOURCE_BUILD'\)/);
  assert.doesNotMatch(drain, /!sourceBuildParallelEnabled && allowedJobTypes === null\) return null/);
  assert.match(drain, /NORMAL_DUE_RETRY_SCHEDULED/);
  assert.match(drain, /sourceBuildTwoCallMinimumBudgetMs/);
  assert.match(drain, /sourceBuildAttemptClaimRpcCount/);
  assert.match(drain, /sourceBuildAttemptExecuteRpcCount/);
  assert.match(drain, /queue-scan watermark can advance beyond a blocked prefix/);
  assert.match(drain, /SOURCE_BUILD_LANE:\$\{laneIndex \|\| 0\}/);
  assert.doesNotMatch(drain, /SOURCE_BUILD_LANE:\$\{startedAtMs\}/);
});

test('runtime version advertises the bounded-scope Stage 2 source marker', () => {
  const version = functionBody('handleVersion');
  assert.match(version, /banking_pay_bounded_scope_stage2/);
  assert.match(version, /V1\.2\.11_TWO_CALL_WORKER_20260805/);
  assert.match(version, /7165360304f8ef12b3790078e450ed1d4b128c55/);
});
